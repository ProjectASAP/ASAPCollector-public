//! Cost of building a collector view's states: q01's view (DDSketch α 0.05
//! per series, 5 s tumbling windows) over a synthetic stream, built by
//!
//! - `executor`: `otap::view_executor::ViewExecutor` (ASAPPlanner's
//!   executor, as the `collector_views` processor runs it),
//! - `sketches`: the `asap_sketches` runtime (`PrecomputeImpl` with the same
//!   DDSketch),
//! - `noop`: generating the stream only (the memory baseline).
//!
//! Every batch holds one sample of every series; `RATE` batches per second
//! of data. Each mode observes the batch and then emits the windows it
//! closed, ASAPv1 states included; only that is timed.
//!
//! `asap-views-bench VIEWS.json MODE SERIES RATE SECONDS REPS` prints the
//! nanoseconds per sample of each repetition and their median, and the
//! process's peak RSS (`VmHWM`). Pin it to one core, e.g. `taskset -c 3`.

use std::time::{Duration, Instant};

use asap_precompute_rs::config::{AggregationMode, PrecomputeConfig, WindowSpec};
use asap_precompute_rs::otap::planner_views;
use asap_precompute_rs::otap::view_executor::ViewExecutor;
use asap_precompute_rs::precompute::PrecomputeImpl;
use asap_precompute_rs::sketches::ddsketch::{DDSketchObserver, DDSketchWrapper};
use asap_precompute_rs::{KeyValue, Observation, ObservationValue, Precompute, SketchType};

fn labels(series: usize) -> Vec<Vec<KeyValue>> {
    (0..series)
        .map(|i| {
            vec![
                KeyValue::new("job", format!("j{}", (i / 50) % 10)),
                KeyValue::new("label_0", format!("l{}", i % 50)),
                KeyValue::new("region", format!("r{}", i / 2500)),
                KeyValue::new("service", format!("s{}", (i / 500) % 5)),
            ]
        })
        .collect()
}

/// xorshift64, uniform in [0, 1).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// One sample of every series at `ts_ms`: Pareto(2) times a per-series scale.
fn batch(ts_ms: u64, labels: &[Vec<KeyValue>], rng: &mut Rng) -> Vec<Observation> {
    labels
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let value = (1.0 + (i % 100) as f64) * (1.0 - rng.next()).powf(-0.5);
            Observation::new(
                ts_ms,
                "data",
                Vec::new(),
                l.clone(),
                ObservationValue::float(value),
            )
        })
        .collect()
}

fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

/// One repetition: the time spent in the mode's path and the states it
/// emitted.
fn run(views: &str, mode: &str, series: usize, rate: u64, seconds: u64) -> (Duration, usize) {
    let labels = labels(series);
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let views = planner_views::parse(views).expect("views load");
    // The `sketches` mode builds q01's view: one DDSketch view, 5 s windows.
    let [view] = views.views.as_slice() else {
        panic!("one view");
    };
    assert_eq!(
        (view.sketch, view.tumbling_window_width_ms),
        (asap_sketchlib::SketchAlgorithm::DDSketch, 5_000)
    );
    let mut executor = (mode == "executor").then(|| ViewExecutor::new(&views).expect("compiles"));
    let sketches = (mode == "sketches").then(|| {
        let config = PrecomputeConfig {
            sketch_type: SketchType::DDSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_millis(5_000),
                ..Default::default()
            },
            ..Default::default()
        };
        PrecomputeImpl::new(
            Some(config),
            Some(Box::new(|| Box::new(DDSketchWrapper::new(0.05)))),
            Some(Box::new(DDSketchObserver)),
        )
    });
    let mut busy = Duration::ZERO;
    let mut states = 0;
    for second in 0..seconds {
        for k in 0..rate {
            let ts = 1_000 * second + 1 + k * 1_000 / rate;
            let observations = batch(ts, &labels, &mut rng);
            let start = Instant::now();
            if let Some(executor) = &mut executor {
                for obs in &observations {
                    executor.observe(obs);
                }
                let closed = executor.flush();
                assert!(closed.failures.is_empty(), "{:?}", closed.failures);
                states += closed.states.len();
            } else if let Some(sketches) = &sketches {
                for obs in &observations {
                    sketches.observe(obs).expect("observe");
                }
                states += sketches.take_closed().len();
            }
            busy += start.elapsed();
        }
    }
    (busy, states)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 7 {
        eprintln!(
            "usage: asap-views-bench VIEWS.json executor|sketches|noop SERIES RATE SECONDS REPS"
        );
        std::process::exit(2);
    }
    let views = std::fs::read_to_string(&args[1]).expect("read VIEWS.json");
    let mode = args[2].as_str();
    let [series, rate, seconds, reps] =
        [3, 4, 5, 6].map(|i| args[i].parse::<u64>().expect("a number"));
    let mut ns = Vec::new();
    let mut states = 0;
    for _ in 0..reps {
        let (busy, emitted) = run(&views, mode, series as usize, rate, seconds);
        states = emitted;
        ns.push(busy.as_nanos() as f64 / (series * rate * seconds) as f64);
    }
    let mut sorted = ns.clone();
    sorted.sort_by(f64::total_cmp);
    println!(
        "{mode}: series {series} rate {rate}/s seconds {seconds} states {states} \
         ns/sample {:?} median {:.1} VmHWM {} kB",
        ns.iter()
            .map(|x| (x * 10.0).round() / 10.0)
            .collect::<Vec<_>>(),
        sorted[sorted.len() / 2],
        peak_rss_kb()
    );
}
