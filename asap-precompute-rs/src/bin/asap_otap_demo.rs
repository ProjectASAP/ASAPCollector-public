//! Runnable native-OTAP sketch create -> merge -> estimate demonstration.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::observation::KeyValue;
use asap_precompute_rs::otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata};
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_config::observed_state::{ObservedStateSettings, SendPolicy};
use otel_arrow_dfe_config::pipeline::PipelineConfig;
use otel_arrow_dfe_config::policy::{ChannelCapacityPolicy, TelemetryPolicy};
use otel_arrow_dfe_config::{DeployedPipelineKey, PipelineGroupId, PipelineId};
use otel_arrow_dfe_engine::capability::registry::Capabilities;
use otel_arrow_dfe_engine::config::{ExporterConfig, ReceiverConfig};
use otel_arrow_dfe_engine::context::{ControllerContext, PipelineContext};
use otel_arrow_dfe_engine::control::{
    pipeline_completion_msg_channel, runtime_ctrl_msg_channel, NodeControlMsg, RuntimeControlMsg,
};
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::{exporter as local_exporter, receiver as local_receiver};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{
    ExporterFactory, MessageSourceLocalEffectHandlerExtension, ReceiverFactory,
};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::{
    OTAP_EXPORTER_FACTORIES, OTAP_PIPELINE_FACTORY, OTAP_RECEIVER_FACTORIES,
};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;

const SOURCE_URN: &str = "urn:asap:receiver:demo_metrics";
const SINK_URN: &str = "urn:asap:exporter:demo_results";
static CAPTURED: OnceLock<Mutex<Vec<OtapPdata>>> = OnceLock::new();

fn captured() -> &'static Mutex<Vec<OtapPdata>> {
    CAPTURED.get_or_init(|| Mutex::new(Vec::new()))
}

fn scalar_input() -> OtapPdata {
    let observations = (1..=100)
        .map(|value| SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 0,
            resource_labels: vec![],
            labels: vec![KeyValue::new("route", "/checkout")],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::Unspecified,
            payload: vec![],
            hash_spec: None,
            metric_name: "request.duration".to_owned(),
            count: 0,
            aggregation_temporality: 0,
            value: f64::from(value),
        })
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&observations).expect("encode demo input")
}

struct DemoReceiver;

#[async_trait(?Send)]
impl local_receiver::Receiver<OtapPdata> for DemoReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl: local_receiver::ControlChannel<OtapPdata>,
        effect_handler: local_receiver::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, otel_arrow_dfe_engine::error::Error> {
        effect_handler
            .send_message_with_source_node(scalar_input())
            .await?;
        loop {
            match ctrl.recv().await {
                Ok(NodeControlMsg::Shutdown { .. }) | Err(_) => break,
                Ok(_) => {}
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
    _capabilities: &Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    Ok(ReceiverWrapper::local(DemoReceiver, node, config, runtime))
}

#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
static SOURCE_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SOURCE_URN,
    create: create_source,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

struct DemoExporter;

#[async_trait(?Send)]
impl local_exporter::Exporter<OtapPdata> for DemoExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        _effect_handler: local_exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, otel_arrow_dfe_engine::error::Error> {
        loop {
            match inbox.recv().await? {
                Message::PData(pdata) => captured().lock().expect("capture mutex").push(pdata),
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
    _capabilities: &Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    Ok(ExporterWrapper::local(DemoExporter, node, config, runtime))
}

#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static SINK_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SINK_URN,
    create: create_sink,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

fn pipeline_yaml() -> String {
    format!(
        r#"
nodes:
  source:
    type: "{SOURCE_URN}"
  create_sketch:
    type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "ddsketch"
      window_size: "20ms"
      output_metric_name: "request.duration.sketch"
      agg_id: 7
      sketch_params:
        relative_accuracy: 0.01
      transmit_sketch: true
  merge_sketch:
    type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "ddsketch"
      window_size: "20ms"
      output_metric_name: "request.duration.merged_sketch"
      agg_id: 7
      sketch_params:
        relative_accuracy: 0.01
      transmit_sketch: true
  estimate_sketch:
    type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "ddsketch"
      window_size: "20ms"
      output_metric_name: "request.duration.estimate"
      agg_id: 7
      sketch_params:
        relative_accuracy: 0.01
      transmit_sketch: false
      quantiles: [0.5, 0.99]
  sink:
    type: "{SINK_URN}"
connections:
  - from: source
    to: create_sketch
  - from: create_sketch
    to: merge_sketch
  - from: merge_sketch
    to: estimate_sketch
  - from: estimate_sketch
    to: sink
"#
    )
}

fn main() {
    println!("ASAP native OTAP sketch protocol demo");
    println!("input: 100 request.duration values (1..=100), route=/checkout");
    println!("pipeline: scalar -> create DDSketch -> merge sketch -> estimate p50/p99");

    captured().lock().expect("capture mutex").clear();
    let config = PipelineConfig::from_yaml("asap-demo".into(), "sketches".into(), &pipeline_yaml())
        .expect("demo pipeline YAML parses");
    let telemetry = InternalTelemetrySystem::default();
    let pipeline_ctx = ControllerContext::new(telemetry.registry()).pipeline_context_with(
        PipelineGroupId::from("asap-demo"),
        PipelineId::from("sketches"),
        0,
        1,
        0,
    );
    let entity_key = pipeline_ctx.register_pipeline_entity();
    let runtime = OTAP_PIPELINE_FACTORY
        .build(
            pipeline_ctx.clone(),
            config,
            ChannelCapacityPolicy::default(),
            TelemetryPolicy::default(),
            None,
            Default::default(),
            None,
            None,
        )
        .expect("build demo pipeline");
    let channel_policy = ChannelCapacityPolicy::default();
    let (runtime_tx, runtime_rx) = runtime_ctrl_msg_channel(channel_policy.control.pipeline);
    let (completion_tx, completion_rx) =
        pipeline_completion_msg_channel(channel_policy.control.completion);
    let observed = ObservedStateStore::new(&ObservedStateSettings::default(), telemetry.registry());
    let shutdown_tx = runtime_tx.clone();
    let shutdown = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        shutdown_tx
            .try_send(RuntimeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "demo complete".to_owned(),
            })
            .expect("request demo shutdown");
    });
    let pipeline_key = DeployedPipelineKey {
        pipeline_group_id: pipeline_ctx.pipeline_group_id(),
        pipeline_id: pipeline_ctx.pipeline_id(),
        core_id: 0,
        deployment_generation: 0,
    };
    let (_pressure_tx, pressure_rx) = tokio::sync::watch::channel(
        otel_arrow_dfe_engine::memory_limiter::MemoryPressureChanged::initial(),
    );
    let _entity_guard = otel_arrow_dfe_engine::entity_context::set_pipeline_entity_key(
        pipeline_ctx.metrics_registry(),
        entity_key,
    );
    runtime
        .run_forever(
            pipeline_key,
            pipeline_ctx,
            observed.reporter(SendPolicy::default()),
            telemetry.reporter(),
            Duration::from_secs(1),
            pressure_rx,
            runtime_tx,
            runtime_rx,
            completion_tx,
            completion_rx,
        )
        .expect("demo pipeline shuts down cleanly");
    shutdown.join().expect("shutdown thread");

    let outputs = captured().lock().expect("capture mutex");
    let mut decoded = Vec::new();
    for pdata in outputs.iter().cloned() {
        decoded.extend(
            decode_pdata_to_observations(pdata)
                .expect("decode demo output")
                .observations,
        );
    }
    assert!(!decoded.is_empty(), "demo produced no estimates");
    println!("output:");
    for observation in &decoded {
        let quantile = observation
            .labels
            .iter()
            .find(|kv| kv.key == "quantile")
            .map_or("?", |kv| kv.value.as_str());
        let route = observation
            .labels
            .iter()
            .find(|kv| kv.key == "route")
            .map_or("?", |kv| kv.value.as_str());
        println!(
            "  metric={} quantile={} value={:.3} route={} envelope={}",
            observation.metric,
            quantile,
            observation.value.float,
            route,
            observation.value.envelope.is_some()
        );
    }
    println!("success: sketch state crossed two native OtapPdata processor hops");
}
