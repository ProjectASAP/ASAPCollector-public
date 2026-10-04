//! Persistent real OTAP pipeline: synthetic scalars -> disjoint KLL panes ->
//! tumbling and rolling estimates -> Prometheus OTLP/HTTP.
#[path = "demo/prometheus_export.rs"]
#[allow(dead_code)] // The shared file also supplies the finite demo's file export.
mod prometheus_export;

use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::observation::KeyValue;
use asap_precompute_rs::otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata};
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
    config::{ExporterConfig, ProcessorConfig, ReceiverConfig},
    context::{ControllerContext, PipelineContext},
    control::{
        pipeline_completion_msg_channel, runtime_ctrl_msg_channel, NodeControlMsg,
        RuntimeControlMsg,
    },
    error::{Error, ExporterErrorKind},
    exporter::ExporterWrapper,
    local::{exporter, processor, receiver},
    message::{ExporterInbox, Message},
    node::NodeId,
    processor::ProcessorWrapper,
    receiver::ReceiverWrapper,
    terminal_state::TerminalState,
    ExporterFactory, MessageSourceLocalEffectHandlerExtension, ProcessorFactory, ReceiverFactory,
};
use otel_arrow_dfe_otap::{
    pdata::OtapPdata, OTAP_EXPORTER_FACTORIES, OTAP_PIPELINE_FACTORY, OTAP_PROCESSOR_FACTORIES,
    OTAP_RECEIVER_FACTORIES,
};
use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;
use std::{
    env,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const SOURCE: &str = "urn:asap:receiver:kll_demo_traffic";
const FORK: &str = "urn:asap:processor:kll_demo_fork";
const SINK: &str = "urn:asap:exporter:kll_demo_prometheus";
static OPTIONS: OnceLock<Options> = OnceLock::new();
static FAILURE: Mutex<Option<String>> = Mutex::new(None);

#[derive(Clone)]
struct Options {
    endpoint: String,
    window_seconds: u64,
    slide_seconds: u64,
    run_seconds: Option<u64>,
}
impl Options {
    fn read() -> Result<Self, String> {
        let mut options = Self {
            endpoint: "http://localhost:9090/api/v1/otlp/v1/metrics".into(),
            window_seconds: 60,
            slide_seconds: 5,
            run_seconds: None,
        };
        let args: Vec<String> = env::args().skip(1).collect();
        if args.iter().any(|a| a == "--help") {
            println!("asap-kll-stream-demo [--prometheus-otlp-endpoint URL] [--window-seconds 60] [--slide-seconds 5] [--run-seconds N]\nRuns until Ctrl+C by default. Window must be a multiple of slide.");
            std::process::exit(0);
        }
        for pair in args.chunks(2) {
            if pair.len() != 2 {
                return Err(format!("missing value for {}", pair[0]));
            }
            if pair[0] == "--prometheus-otlp-endpoint" {
                options.endpoint = pair[1].clone();
                continue;
            }
            let value: u64 = pair[1]
                .parse()
                .map_err(|_| "duration must be a positive integer number of seconds")?;
            if value == 0 {
                return Err("duration must be positive".into());
            }
            match pair[0].as_str() {
                "--window-seconds" => options.window_seconds = value,
                "--slide-seconds" => options.slide_seconds = value,
                "--run-seconds" => options.run_seconds = Some(value),
                other => return Err(format!("unknown option {other}")),
            }
        }
        if options.window_seconds < options.slide_seconds
            || !options.window_seconds.is_multiple_of(options.slide_seconds)
        {
            return Err("window must be a positive multiple of slide".into());
        }
        if !(options.endpoint.starts_with("http://") || options.endpoint.starts_with("https://")) {
            return Err("endpoint must be an HTTP(S) URL".into());
        }
        Ok(options)
    }
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock")
        .as_millis() as u64
}

fn traffic(sequence: u64, timestamp: u64) -> OtapPdata {
    let envelopes: Vec<_> = (0..200)
        .map(|offset| {
            let index = sequence * 200 + offset;
            let phase = ((timestamp / 10_000) % 3) as f64;
            let value = if index.is_multiple_of(100) {
                300.0 + 200.0 * phase
            } else {
                10.0 + 20.0 * phase + (index % 97) as f64 * 0.1
            };
            SketchEnvelope {
                schema_version: 1,
                sketch_type: SketchType::Unspecified,
                agg_id: 0,
                resource_labels: vec![KeyValue::new("service.name", "checkout")],
                labels: vec![
                    KeyValue::new("http.request.method", "GET"),
                    KeyValue::new("http.route", "/checkout"),
                    KeyValue::new("http.response.status_code", "200"),
                ],
                window_start_ms: timestamp,
                window_end_ms: timestamp,
                encoding: Encoding::Unspecified,
                payload: vec![],
                hash_spec: None,
                metric_name: "request.duration".into(),
                count: 0,
                aggregation_temporality: 0,
                value,
            }
        })
        .collect();
    encode_envelopes_to_pdata(&envelopes).expect("encode generated metrics")
}
struct TrafficReceiver;
#[async_trait(?Send)]
impl receiver::Receiver<OtapPdata> for TrafficReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl: receiver::ControlChannel<OtapPdata>,
        effects: receiver::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut sequence = 0;
        loop {
            tokio::select! {
                message=ctrl.recv() => match message { Ok(NodeControlMsg::Shutdown {..}) | Err(_) => break, _ => {} },
                _=interval.tick() => {
                    effects.send_message_with_source_node(traffic(sequence,now_ms())).await?;
                    sequence+=1;
                }
            }
        }
        Ok(TerminalState::default())
    }
}
fn create_source(
    _ctx: PipelineContext,
    node: NodeId,
    config: Arc<NodeUserConfig>,
    runtime: &ReceiverConfig,
    _caps: &Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    Ok(ReceiverWrapper::local(
        TrafficReceiver,
        node,
        config,
        runtime,
    ))
}
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
static SOURCE_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SOURCE,
    create: create_source,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

// The pinned engine routes one destination per output port. Explicitly
// duplicate complete panes to two named ports for the comparison demo.
struct PaneFork;
#[async_trait(?Send)]
impl processor::Processor<OtapPdata> for PaneFork {
    async fn process(
        &mut self,
        message: Message<OtapPdata>,
        effects: &mut processor::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if let Message::PData(pdata) = message {
            effects
                .send_message_with_source_node_to("tumbling", pdata.clone())
                .await?;
            effects
                .send_message_with_source_node_to("rolling", pdata)
                .await?;
        }
        Ok(())
    }
}
fn create_fork(
    _ctx: PipelineContext,
    node: NodeId,
    config: Arc<NodeUserConfig>,
    runtime: &ProcessorConfig,
    _caps: &Capabilities,
) -> Result<ProcessorWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    Ok(ProcessorWrapper::local(PaneFork, node, config, runtime))
}
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static FORK_FACTORY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: FORK,
    create: create_fork,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

struct PrometheusExporter {
    node: NodeId,
}
#[async_trait(?Send)]
impl exporter::Exporter<OtapPdata> for PrometheusExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        _effects: exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let endpoint = OPTIONS.get().expect("options").endpoint.clone();
        loop {
            match inbox.recv().await? {
                Message::PData(pdata) => {
                    let error = |message: String| {
                        *FAILURE.lock().expect("export failure lock") = Some(message.clone());
                        Error::ExporterError {
                            exporter: self.node.clone(),
                            kind: ExporterErrorKind::Transport,
                            error: message,
                            source_detail: String::new(),
                        }
                    };
                    let observations = decode_pdata_to_observations(pdata.clone())
                        .map_err(|e| error(e.to_string()))?
                        .observations;
                    let (_, payload) = pdata.into_parts();
                    let encoded =
                        <_ as TryIntoWithOptions<OtlpProtoBytes>>::try_into_with_default(payload)
                            .map_err(|e| error(e.to_string()))?;
                    let OtlpProtoBytes::ExportMetricsRequest(bytes) = encoded else {
                        return Err(error("expected metric gauges".into()));
                    };
                    let url = endpoint.clone();
                    // Curl may block on network I/O; keep it off the local
                    // OTAP executor so ingestion and timers can continue.
                    let result = tokio::task::spawn_blocking(move || {
                        prometheus_export::export_bytes(&bytes, &url, None)
                    })
                    .await
                    .map_err(|e| error(e.to_string()))?;
                    result.map_err(error)?;
                    for o in observations {
                        let quantile = o
                            .labels
                            .iter()
                            .find(|l| l.key == "quantile")
                            .map_or("?", |l| l.value.as_str());
                        println!(
                            "exported window_end_ms={} metric={} quantile={} value={:.3}",
                            o.timestamp_ms, o.metric, quantile, o.value.float
                        );
                    }
                }
                Message::Control(NodeControlMsg::Shutdown { .. }) => break,
                Message::Control(_) => {}
            }
        }
        Ok(TerminalState::default())
    }
}
fn create_sink(
    _ctx: PipelineContext,
    node: NodeId,
    config: Arc<NodeUserConfig>,
    runtime: &ExporterConfig,
    _caps: &Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    Ok(ExporterWrapper::local(
        PrometheusExporter { node: node.clone() },
        node,
        config,
        runtime,
    ))
}
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static SINK_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SINK,
    create: create_sink,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

fn pipeline_yaml(options: &Options) -> String {
    let pane = options.slide_seconds;
    let window = options.window_seconds;
    format!(
        r#"nodes:
  source:
    type: {SOURCE}
  panes:
    type: urn:asap:processor:asap_sketches
    config:
      sketch_type: kll
      window_size: {pane}s
      window_slide: {pane}s
      output_metric_name: request.duration.pane
      agg_id: 7
      sketch_params: {{ k: 400 }}
      transmit_sketch: true
  fork:
    type: {FORK}
  tumbling:
    type: urn:asap:processor:asap_sketches
    config:
      sketch_type: kll
      window_size: {pane}s
      window_slide: {pane}s
      output_metric_name: request.duration.estimate
      agg_id: 7
      sketch_params: {{ k: 400 }}
      transmit_sketch: false
      quantiles: [0.5, 0.99]
  rolling:
    type: urn:asap:processor:asap_sketches
    config:
      sketch_type: kll
      window_size: {window}s
      window_slide: {pane}s
      output_metric_name: request.duration.rolling_estimate
      agg_id: 7
      sketch_params: {{ k: 400 }}
      transmit_sketch: false
      quantiles: [0.5, 0.99]
  sink:
    type: {SINK}
connections:
  - {{ from: source, to: panes }}
  - {{ from: panes, to: fork }}
  - {{ from: 'fork["tumbling"]', to: tumbling }}
  - {{ from: 'fork["rolling"]', to: rolling }}
  - {{ from: tumbling, to: sink }}
  - {{ from: rolling, to: sink }}
"#
    )
}
fn run(options: Options) -> Result<(), String> {
    println!(
        "Persistent OTAP KLL demo: {}s tumbling panes, {}s rolling lookback, {}s emission interval",
        options.slide_seconds, options.window_seconds, options.slide_seconds
    );
    println!(
        "Prometheus endpoint: {}; stop with Ctrl+C",
        options.endpoint
    );
    OPTIONS
        .set(options.clone())
        .map_err(|_| "options already set")?;
    let config = PipelineConfig::from_yaml(
        "asap-kll-stream".into(),
        "demo".into(),
        &pipeline_yaml(&options),
    )
    .map_err(|e| e.to_string())?;
    let telemetry = InternalTelemetrySystem::default();
    let ctx = ControllerContext::new(telemetry.registry()).pipeline_context_with(
        PipelineGroupId::from("asap-kll-stream"),
        PipelineId::from("demo"),
        0,
        1,
        0,
    );
    let entity = ctx.register_pipeline_entity();
    let policy = ChannelCapacityPolicy::default();
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
        let signal_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("signal runtime");
        signal_runtime.block_on(async {
            let lifetime = async {
                if let Some(seconds) = options.run_seconds { tokio::time::sleep(Duration::from_secs(seconds)).await; }
                else { std::future::pending::<()>().await; }
            };
            let failed = async {
                loop {
                    if FAILURE.lock().expect("export failure lock").is_some() { break; }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
            tokio::select! {
                _=failed => {},
                signal=tokio::signal::ctrl_c() => { if let Err(e)=signal { eprintln!("Ctrl+C listener: {e}"); } },
                _=lifetime => {},
            }
        });
        let _ = shutdown_tx.try_send(RuntimeControlMsg::Shutdown {
            deadline: Instant::now() + Duration::from_secs(5),
            reason: "streaming demo stopped".into(),
        });
    });
    let key = DeployedPipelineKey {
        pipeline_group_id: ctx.pipeline_group_id(),
        pipeline_id: ctx.pipeline_id(),
        core_id: 0,
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
        .map_err(|e| e.to_string())?;
    if let Some(error) = FAILURE.lock().expect("export failure lock").take() {
        return Err(error);
    }
    println!("streaming demo stopped");
    Ok(())
}
fn main() {
    if let Err(error) = Options::read().and_then(run) {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
