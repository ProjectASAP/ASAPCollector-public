//! Persistent OTAP workers connected by upstream standard OTLP/HTTP nodes.
//! The control listener carries counters/commands only, never telemetry payloads.
#[path = "stream_bench/model.rs"]
mod model;
#[path = "stream_bench/profile.rs"]
mod profile;
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::{
    node::NodeUserConfig,
    observed_state::{ObservedStateSettings, SendPolicy},
    pipeline::PipelineConfig,
    policy::{ChannelCapacityPolicy, TelemetryPolicy},
    DeployedPipelineKey, PipelineGroupId, PipelineId,
};
use otel_arrow_dfe_core_nodes as _;
use otel_arrow_dfe_engine::{
    capability::registry::Capabilities,
    config::{ExporterConfig, ProcessorConfig},
    context::{ControllerContext, PipelineContext},
    control::{
        pipeline_completion_msg_channel, runtime_ctrl_msg_channel, AckMsg, NodeControlMsg,
        RuntimeControlMsg,
    },
    error::{Error, ProcessorErrorKind},
    exporter::ExporterWrapper,
    local::{exporter, processor},
    message::{ExporterInbox, Message},
    node::NodeId,
    processor::ProcessorWrapper,
    terminal_state::TerminalState,
    ConsumerEffectHandlerExtension, ExporterFactory, MessageSourceLocalEffectHandlerExtension,
    ProcessorFactory,
};
use otel_arrow_dfe_otap::{
    pdata::OtapPdata, OTAP_EXPORTER_FACTORIES, OTAP_PIPELINE_FACTORY, OTAP_PROCESSOR_FACTORIES,
};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;
use serde::Serialize;
use std::{
    collections::BTreeMap,
    env, fs,
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::{Duration, Instant},
};

const PROCESSOR: &str = "urn:asap:processor:stream_benchmark";
const BACKEND: &str = "urn:asap:exporter:stream_validation";
static OPTIONS: OnceLock<Options> = OnceLock::new();
static SHARED: OnceLock<Arc<Shared>> = OnceLock::new();
#[derive(Clone)]
struct Options {
    role: String,
    scenario: String,
    points: usize,
    batch: usize,
    source: usize,
    listen: String,
    control: String,
    endpoint: String,
    generator_workers: usize,
    signals_per_second: usize,
}
impl Options {
    fn read() -> Result<Self, String> {
        let get = |key: &str, default: &str| env::var(key).unwrap_or_else(|_| default.into());
        let number = |key: &str, default: &str| {
            get(key, default)
                .parse::<usize>()
                .map_err(|e| e.to_string())
        };
        let o = Self {
            role: get("ASAP_ROLE", "branch"),
            scenario: get("ASAP_SCENARIO", "kll"),
            points: number("ASAP_WINDOW_POINTS", "65536")?,
            batch: number("ASAP_BATCH_SIZE", "1024")?,
            source: number("ASAP_SOURCE", "0")?,
            listen: get("ASAP_LISTEN", "0.0.0.0:4318"),
            control: get("ASAP_CONTROL", "0.0.0.0:4319"),
            endpoint: get("ASAP_ENDPOINT", "http://merge:4318"),
            generator_workers: number("ASAP_GENERATOR_WORKERS", "1")?,
            signals_per_second: number("ASAP_SIGNALS_PER_SECOND", "100000")?,
        };
        if !["raw", "exact", "kll"].contains(&o.scenario.as_str())
            || !["generator", "branch", "merge", "estimate", "backend"].contains(&o.role.as_str())
            || o.points == 0
            || o.batch == 0
            || o.batch > o.points
            || o.source > 1
            || o.generator_workers == 0
            || o.signals_per_second == 0
        {
            return Err("invalid role/scenario/window/batch/source configuration".into());
        }
        Ok(o)
    }
}
#[derive(Default, Serialize, Clone)]
struct Stats {
    received_signals: u64,
    sent_signals: u64,
    sent_windows: u64,
    completed_windows: u64,
    generator_paused: bool,
    otlp_payload_bytes_sent: u64,
    window_latency_ms: BTreeMap<u64, u64>,
}
struct Shared {
    stats: Mutex<Stats>,
    running: AtomicBool,
    target: AtomicU64,
    shutdown: AtomicBool,
}
fn shared() -> &'static Arc<Shared> {
    SHARED.get().unwrap()
}
// All containers share the host's monotonic clock. NTP adjustments must not
// change throughput denominators or end-to-end window latency.
fn now_ns() -> u64 {
    clock_ns(libc::CLOCK_MONOTONIC)
}
fn cpu_ns() -> u64 {
    clock_ns(libc::CLOCK_PROCESS_CPUTIME_ID)
}
fn clock_ns(clock: libc::clockid_t) -> u64 {
    let mut stamp = std::mem::MaybeUninit::<libc::timespec>::uninit();
    assert_eq!(unsafe { libc::clock_gettime(clock, stamp.as_mut_ptr()) }, 0);
    let stamp = unsafe { stamp.assume_init() };
    stamp.tv_sec as u64 * 1_000_000_000 + stamp.tv_nsec as u64
}
fn snapshot() -> serde_json::Value {
    let stats = shared().stats.lock().unwrap().clone();
    let status = fs::read_to_string("/proc/self/status").unwrap();
    let kib = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .and_then(|s| s.split_whitespace().next())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let mut interfaces = BTreeMap::new();
    for line in fs::read_to_string("/proc/net/dev").unwrap().lines().skip(2) {
        if let Some((name, data)) = line.split_once(':') {
            let fields = data.split_whitespace().collect::<Vec<_>>();
            interfaces.insert(name.trim().to_owned(), serde_json::json!({"rx_bytes": fields[0].parse::<u64>().unwrap(), "tx_bytes": fields[8].parse::<u64>().unwrap()}));
        }
    }
    serde_json::json!({"timestamp_ns": now_ns(), "pid": std::process::id(), "cpu_nanoseconds": cpu_ns(),
        "profile_cpu_ns": profile::snapshot(), "rss_kib": kib("VmRSS:"), "process_peak_rss_kib": kib("VmHWM:"), "interfaces": interfaces, "stats": stats})
}
fn start_control() -> Result<(), String> {
    let listener = TcpListener::bind(&OPTIONS.get().unwrap().control).map_err(|e| e.to_string())?;
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = stream.expect("control accept");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            stream
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut line = String::new();
            let mut reader = BufReader::new(&mut stream);
            if reader.read_line(&mut line).is_err() {
                continue;
            }
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                    break;
                }
            }
            let path = line.split_whitespace().nth(1).unwrap_or("");
            let method = line.split_whitespace().next().unwrap_or("");
            let valid = match (method, path) {
                ("GET", "/stats") => true,
                ("POST", "/run") => {
                    shared().target.store(u64::MAX, Ordering::SeqCst);
                    shared().running.store(true, Ordering::SeqCst);
                    true
                }
                ("POST", "/pause") => {
                    shared().running.store(false, Ordering::SeqCst);
                    true
                }
                ("POST", "/shutdown") => {
                    shared().shutdown.store(true, Ordering::SeqCst);
                    true
                }
                ("POST", p) if p.starts_with("/finish/") => {
                    if let Ok(target) = p[8..].parse::<u64>() {
                        shared().target.store(target, Ordering::SeqCst);
                        shared().running.store(true, Ordering::SeqCst);
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            };
            let body = if valid {
                snapshot().to_string()
            } else {
                "{}".into()
            };
            let code = if valid { "200 OK" } else { "400 Bad Request" };
            let _ = write!(stream, "HTTP/1.1 {code}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        }
    });
    Ok(())
}
struct StreamProcessor {
    processor: model::Processor,
    normalizer: Option<model::InputNormalizer>,
}
#[async_trait(?Send)]
impl processor::Processor<OtapPdata> for StreamProcessor {
    async fn process(
        &mut self,
        message: Message<OtapPdata>,
        effects: &mut processor::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if let Message::PData(mut pdata) = message {
            let outputs = if let Some(normalizer) = &mut self.normalizer {
                let count = pdata.num_items();
                {
                    let mut stats = shared().stats.lock().unwrap();
                    stats.received_signals += count as u64;
                    stats.sent_windows =
                        stats.received_signals / OPTIONS.get().unwrap().points as u64;
                }
                normalizer.normalize(count, now_ns()).into_iter().try_fold(
                    Vec::new(),
                    |mut outputs, batch| {
                        outputs.extend(self.processor.process_normalized(batch)?);
                        Ok(outputs)
                    },
                )
            } else {
                self.processor.process(pdata)
            }
            .map_err(|error| Error::ProcessorError {
                processor: effects.processor_id(),
                kind: ProcessorErrorKind::Other,
                error,
                source_detail: String::new(),
            })?;
            for output in outputs {
                effects.send_message_with_source_node(output).await?;
            }
        }
        Ok(())
    }
}
fn create_processor(
    _: PipelineContext,
    node: NodeId,
    config: Arc<NodeUserConfig>,
    runtime: &ProcessorConfig,
    _: &Capabilities,
) -> Result<ProcessorWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let o = OPTIONS.get().unwrap();
    Ok(ProcessorWrapper::local(
        StreamProcessor {
            processor: model::Processor::new(&o.scenario, &o.role, o.points, o.batch),
            normalizer: (o.role == "branch")
                .then(|| model::InputNormalizer::new(o.source, o.points, o.batch)),
        },
        node,
        config,
        runtime,
    ))
}
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static PROCESSOR_FACTORY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: PROCESSOR,
    create: create_processor,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};
struct ValidatingBackend(model::Backend);
#[async_trait(?Send)]
impl exporter::Exporter<OtapPdata> for ValidatingBackend {
    async fn start(
        mut self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effects: exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        loop {
            match inbox.recv().await? {
                Message::PData(pdata) => {
                    // Validation failures terminate the benchmark, never increment goodput.
                    let (count, completed) = self
                        .0
                        .receive(pdata.clone())
                        .expect("backend correctness failure");
                    {
                        let mut stats = shared().stats.lock().unwrap();
                        stats.received_signals += count;
                        if let Some((window, start)) = completed {
                            assert_eq!(window, stats.completed_windows, "backend window gap");
                            stats.completed_windows += 1;
                            let elapsed_ms = now_ns().saturating_sub(start) / 1_000_000;
                            *stats.window_latency_ms.entry(elapsed_ms).or_default() += 1;
                        }
                    }
                    effects.notify_ack(AckMsg::new(pdata)).await?;
                }
                Message::Control(NodeControlMsg::Shutdown { .. }) => break,
                Message::Control(_) => {}
            }
        }
        Ok(TerminalState::default())
    }
}
fn create_backend(
    _: PipelineContext,
    node: NodeId,
    config: Arc<NodeUserConfig>,
    runtime: &ExporterConfig,
    _: &Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let o = OPTIONS.get().unwrap();
    Ok(ExporterWrapper::local(
        ValidatingBackend(model::Backend::new(&o.scenario, o.points)),
        node,
        config,
        runtime,
    ))
}
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static BACKEND_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: BACKEND,
    create: create_backend,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};
fn pipeline_yaml(o: &Options) -> String {
    if o.role == "generator" {
        let rate = o.signals_per_second.div_ceil(o.generator_workers);
        return format!(
            "nodes:\n  source:\n    type: urn:otel:receiver:traffic_generator\n    config:\n      data_source: synthetic\n      generation_strategy: pre_generated\n      resource_attributes:\n        - attrs: {{bench.source: '{}'}}\n      traffic_config:\n        production_mode: smooth\n        signals_per_second: {}\n        max_batch_size: {}\n        metric_weight: 1\n        trace_weight: 0\n        log_weight: 0\n        num_data_points_per_metric: 1\n  sink:\n    type: urn:otel:exporter:otlp_http\n    config:\n      endpoint: {}\n      client_pool_size: 1\n      max_in_flight: 1\n      http: {{timeout: 30s, compression: none}}\nconnections:\n  - {{from: source, to: sink}}\n",
            o.source, rate, o.batch, o.endpoint
        );
    }
    let source = format!("source:\n    type: urn:otel:receiver:otlp\n    config:\n      protocols:\n        http:\n          listening_addr: {}\n          wait_for_result: false\n", o.listen);
    if o.role == "backend" {
        return format!("nodes:\n  {source}  sink: {{type: '{BACKEND}'}}\nconnections:\n  - {{from: source, to: sink}}\n");
    }
    format!("nodes:\n  {source}  processor: {{type: '{PROCESSOR}'}}\n  sink:\n    type: urn:otel:exporter:otlp_http\n    config:\n      endpoint: {}\n      client_pool_size: 1\n      max_in_flight: 1\n      http: {{timeout: 30s, compression: none}}\nconnections:\n  - {{from: source, to: processor}}\n  - {{from: processor, to: sink}}\n", o.endpoint)
}
fn run_pipeline(o: &Options, core_id: usize, num_cores: usize) -> Result<(), String> {
    let config = PipelineConfig::from_yaml(
        "stream-bench".into(),
        o.role.clone().into(),
        &pipeline_yaml(o),
    )
    .map_err(|e| e.to_string())?;
    let telemetry = InternalTelemetrySystem::default();
    let ctx = ControllerContext::new(telemetry.registry()).pipeline_context_with(
        PipelineGroupId::from("stream-bench"),
        PipelineId::from(o.role.clone()),
        core_id,
        num_cores,
        0,
    );
    let entity = ctx.register_pipeline_entity();
    let policy = ChannelCapacityPolicy {
        pdata: 8,
        ..Default::default()
    };
    let runtime = OTAP_PIPELINE_FACTORY
        .build(
            ctx.clone(),
            config,
            policy.clone(),
            TelemetryPolicy::default(),
            None,
            Default::default(),
            None,
            None,
        )
        .map_err(|e| e.to_string())?;
    let (runtime_tx, runtime_rx) = runtime_ctrl_msg_channel(policy.control.pipeline);
    let (completion_tx, completion_rx) = pipeline_completion_msg_channel(policy.control.completion);
    let observed = ObservedStateStore::new(&ObservedStateSettings::default(), telemetry.registry());
    let shutdown_tx = runtime_tx.clone();
    std::thread::spawn(move || {
        while !shared().shutdown.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = shutdown_tx.try_send(RuntimeControlMsg::Shutdown {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "benchmark drained".into(),
        });
    });
    let key = DeployedPipelineKey {
        pipeline_group_id: ctx.pipeline_group_id(),
        pipeline_id: ctx.pipeline_id(),
        core_id,
        deployment_generation: 0,
    };
    let (_, pressure_rx) = tokio::sync::watch::channel(
        otel_arrow_dfe_engine::memory_limiter::MemoryPressureChanged::initial(),
    );
    let _guard = otel_arrow_dfe_engine::entity_context::set_pipeline_entity_key(
        ctx.metrics_registry(),
        entity,
    );
    runtime
        .run_forever(
            key,
            ctx,
            observed.reporter(SendPolicy::default()),
            telemetry.reporter(),
            Duration::from_secs(1),
            pressure_rx,
            runtime_tx,
            runtime_rx,
            completion_tx,
            completion_rx,
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}
fn run_generator(o: &Options) -> Result<(), String> {
    let mut workers = Vec::with_capacity(o.generator_workers);
    for core_id in 0..o.generator_workers {
        let options = o.clone();
        workers.push(std::thread::spawn(move || {
            run_pipeline(&options, core_id, options.generator_workers)
        }));
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| "traffic generator worker panicked".to_owned())??;
    }
    Ok(())
}
fn run() -> Result<(), String> {
    otel_arrow_dfe_otap::crypto::install_crypto_provider()?;
    let options = Options::read()?;
    OPTIONS
        .set(options.clone())
        .map_err(|_| "options initialized")?;
    SHARED
        .set(Arc::new(Shared {
            stats: Mutex::new(Stats {
                generator_paused: true,
                ..Default::default()
            }),
            running: AtomicBool::new(false),
            target: AtomicU64::new(u64::MAX),
            shutdown: AtomicBool::new(false),
        }))
        .map_err(|_| "shared initialized")?;
    start_control()?;
    if options.role == "generator" {
        run_generator(&options)
    } else {
        run_pipeline(&options, 0, 1)
    }
}
fn main() {
    if let Err(error) = run() {
        eprintln!("stream benchmark failed: {error}");
        std::process::exit(1);
    }
}
