//! Runnable end-to-end demo of the `docs/data_model.md` SCHEMA /
//! DICTIONARY / RECORD wire shape, three "processors" wired together
//! in one binary:
//!
//! 1. **Sketch creation processor** (`run_producer`) — feeds synthetic
//!    latency samples into a [`PrecomputeImpl`], closes a window at a
//!    time, and encodes the closed-window envelopes against a
//!    [`SeriesDictionary`] — the SCHEMA/DICTIONARY/RECORD codec from
//!    `otap::dictionary`.
//! 2. **Receive processor** (`run_receiver`) — decodes each
//!    [`SketchStreamBatch`] via a [`SeriesDictionaryDecoder`], merges
//!    the reconstructed envelopes into its own [`PrecomputeImpl`]
//!    (`Precompute::observe_envelope` — bytes merged as sketch state,
//!    never expanded to samples), then *queries* that merged sketch by
//!    draining it with `transmit_sketch = false`: the runtime's
//!    estimate-mode path (`Sketch::estimate`) turns the merged DDSketch
//!    into a p99 gauge.
//! 3. **Prometheus backend** — stands in as a `println!` of the
//!    gauge in Prometheus text exposition format; swap
//!    [`format_prometheus_gauge`]'s call site for a real HTTP
//!    `/metrics` handler to serve it for real.
//!
//! Producer and receiver run as two Tokio tasks connected by an
//! in-process `mpsc` channel carrying [`SketchStreamBatch`] — no
//! network transport, no serialization — so what crosses the "wire"
//! here is literally the same four `RecordBatch`es a real inter-node
//! hop would carry. Watch the printed row counts: window 0 carries
//! `SCHEMA`+`DICTIONARY`+`LABELS` rows (the series is new), every
//! later window carries only `RECORD` rows — that's the whole point of
//! this doc's design, made visible.
//!
//! Run with:
//! ```text
//! cargo run --example sketch_pipeline_demo --features otap
//! ```

use std::time::Duration;

use asap_precompute_rs::envelope::SketchEnvelope;
use asap_precompute_rs::observation::{KeyValue, Observation, ObservationValue};
use asap_precompute_rs::otap::config::{resolve, PluginConfig};
use asap_precompute_rs::otap::{SeriesDictionary, SeriesDictionaryDecoder, SketchStreamBatch};
use asap_precompute_rs::precompute::{Precompute, PrecomputeImpl};
use tokio::sync::mpsc;

/// Both processors run one aggregation plan, so they share this
/// join key — in a real deployment the receiver would learn it out of
/// band (control plane), same as `docs/data_model.md`'s "Open design
/// questions" section describes.
const AGG_ID: u64 = 1;
/// How many synthetic windows the producer closes before shutting
/// down. Each window's samples are drawn from a slowly climbing
/// distribution so the printed p99 visibly moves window to window.
const NUM_WINDOWS: usize = 4;
/// Purely for demo pacing (so the two tasks visibly interleave in the
/// terminal) — window *closing* itself is driven by explicit `drain()`
/// calls below, not wall-clock alignment, so this value doesn't affect
/// correctness.
const PACING: Duration = Duration::from_millis(150);

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::unbounded_channel::<SketchStreamBatch>();

    let producer = tokio::spawn(run_producer(tx));
    let receiver = tokio::spawn(run_receiver(rx));

    let _ = tokio::join!(producer, receiver);
}

/// Sketch creation processor: observes synthetic samples, closes a
/// window at a time, encodes against a [`SeriesDictionary`], sends the
/// result downstream.
async fn run_producer(tx: mpsc::UnboundedSender<SketchStreamBatch>) {
    let plugin_cfg = PluginConfig {
        sketch_type: "ddsketch".into(),
        window_size: Duration::from_secs(10), // never naturally fires — see drain() below.
        output_metric_name: "http_request_duration_ms".into(),
        agg_id: AGG_ID,
        sketch_params: [("relative_accuracy".to_string(), 0.01)]
            .into_iter()
            .collect(),
        ..Default::default()
    };
    let (pcfg, dispatch) = resolve(&plugin_cfg).expect("producer config");
    let precompute = PrecomputeImpl::new(
        Some(pcfg.clone()),
        Some(dispatch.factory),
        Some(dispatch.observer),
    );
    let mut dictionary = SeriesDictionary::new();

    for window_idx in 0..NUM_WINDOWS {
        // One series (path=/api), latency drifting upward window to
        // window so the receiver's printed p99 visibly changes.
        for i in 0..200u64 {
            let base = 10.0 + (window_idx as f64) * 8.0;
            let latency = base + (i % 25) as f64;
            let obs = Observation::new(
                now_ms(),
                "http_request_duration_ms",
                Vec::new(),
                vec![KeyValue::new("path", "/api")],
                ObservationValue::float(latency),
            );
            precompute.observe(&obs).expect("observe");
        }

        // Force this window closed regardless of wall clock — see the
        // module doc's "PACING" note.
        let envelopes = precompute.drain();
        if envelopes.is_empty() {
            continue;
        }
        let batch = dictionary
            .encode(&envelopes, Some(&pcfg))
            .expect("encode window");
        println!(
            "[producer] window {window_idx}: schema={} dictionary={} labels={} record={} row(s)",
            batch.schema.num_rows(),
            batch.dictionary.num_rows(),
            batch.labels.num_rows(),
            batch.record.num_rows(),
        );
        if tx.send(batch).is_err() {
            break; // receiver gone.
        }
        tokio::time::sleep(PACING).await;
    }
    // Dropping `tx` here signals the receiver's `rx.recv()` to return
    // `None` once the channel drains.
}

/// Receive processor: decodes each batch, merges the reconstructed
/// envelopes into its own window, queries the merge by draining in
/// estimate mode, and hands the resulting gauge to
/// [`format_prometheus_gauge`].
async fn run_receiver(mut rx: mpsc::UnboundedReceiver<SketchStreamBatch>) {
    let plugin_cfg = PluginConfig {
        sketch_type: "ddsketch".into(),
        window_size: Duration::from_secs(10), // same "driven by drain(), not wall clock" note as the producer.
        output_metric_name: "http_request_duration_ms_p99".into(),
        agg_id: AGG_ID,
        transmit_sketch: false, // query mode: drain() yields quantile estimates, not sketch bytes.
        quantiles: vec![0.99],
        ..Default::default()
    };
    let (pcfg, dispatch) = resolve(&plugin_cfg).expect("receiver config");
    let precompute =
        PrecomputeImpl::new(Some(pcfg), Some(dispatch.factory), Some(dispatch.observer));
    let mut decoder = SeriesDictionaryDecoder::new();

    while let Some(batch) = rx.recv().await {
        let envelopes = decoder.decode(&batch).expect("decode stream batch");
        println!(
            "[receiver] decoded {} envelope(s) from the batch",
            envelopes.len()
        );
        for env in &envelopes {
            // Merge only — the runtime never expands envelope bytes
            // back into scalar samples (the bandwidth invariant).
            precompute.observe_envelope(env).expect("observe_envelope");
        }

        // Query: force this window's merged sketch to close now, in
        // estimate mode, so the runtime's own estimate machinery
        // (Sketch::estimate) does the quantile math for us.
        for estimate in precompute.drain() {
            print!("{}", format_prometheus_gauge(&estimate));
        }
    }
}

/// Formats one estimate-mode [`SketchEnvelope`] (`payload` empty,
/// `value` set — see `docs/data_model.md`'s `RECORD.value`) as a
/// Prometheus text-exposition gauge sample. Stands in for a real
/// `/metrics` HTTP handler.
fn format_prometheus_gauge(env: &SketchEnvelope) -> String {
    let mut labels: Vec<String> = env
        .labels
        .iter()
        .map(|kv| format!("{}=\"{}\"", kv.key, kv.value))
        .collect();
    labels.sort();
    let label_str = if labels.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", labels.join(","))
    };
    format!(
        "# HELP {name} sketch-derived quantile estimate\n\
         # TYPE {name} gauge\n\
         {name}{label_str} {value} {ts}\n",
        name = env.metric_name,
        value = env.value,
        ts = env.window_end_ms,
    )
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis() as u64
}
