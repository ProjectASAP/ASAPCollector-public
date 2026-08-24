#![cfg(feature = "otap-engine")]

//! Runs `asap_sketches` inside a real OTAP `RuntimePipeline` built from YAML.

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

const SOURCE_URN: &str = "urn:asap:receiver:one_metric";
const SINK_URN: &str = "urn:asap:exporter:capture";

static CAPTURED: OnceLock<Mutex<Vec<OtapPdata>>> = OnceLock::new();

fn captured() -> &'static Mutex<Vec<OtapPdata>> {
    CAPTURED.get_or_init(|| Mutex::new(Vec::new()))
}

fn scalar_input() -> OtapPdata {
    let envelope = SketchEnvelope {
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
        value: 42.5,
    };
    encode_envelopes_to_pdata(&[envelope]).expect("encode scalar input")
}

struct OneMetricReceiver;

#[async_trait(?Send)]
impl local_receiver::Receiver<OtapPdata> for OneMetricReceiver {
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
    Ok(ReceiverWrapper::local(
        OneMetricReceiver,
        node,
        config,
        runtime,
    ))
}

#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
static SOURCE_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SOURCE_URN,
    create: create_source,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

struct CaptureExporter;

#[async_trait(?Send)]
impl local_exporter::Exporter<OtapPdata> for CaptureExporter {
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
    Ok(ExporterWrapper::local(
        CaptureExporter,
        node,
        config,
        runtime,
    ))
}

#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static SINK_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SINK_URN,
    create: create_sink,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

#[test]
fn yaml_pipeline_ingests_flushes_and_exports_a_sketch() {
    captured().lock().expect("capture mutex").clear();
    let yaml = format!(
        r#"
nodes:
  source:
    type: "{SOURCE_URN}"
  sketches:
    type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "ddsketch"
      window_size: "20ms"
      output_metric_name: "request.duration.p99"
      agg_id: 0
  sink:
    type: "{SINK_URN}"
connections:
  - from: source
    to: sketches
  - from: sketches
    to: sink
"#
    );
    let config = PipelineConfig::from_yaml("asap-e2e".into(), "sketches".into(), &yaml)
        .expect("pipeline YAML parses and validates");

    let telemetry = InternalTelemetrySystem::default();
    let pipeline_ctx = ControllerContext::new(telemetry.registry()).pipeline_context_with(
        PipelineGroupId::from("asap-e2e"),
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
        .expect("pipeline builds from registered factories");

    let channel_policy = ChannelCapacityPolicy::default();
    let (runtime_tx, runtime_rx) = runtime_ctrl_msg_channel(channel_policy.control.pipeline);
    let (completion_tx, completion_rx) =
        pipeline_completion_msg_channel(channel_policy.control.completion);
    let observed = ObservedStateStore::new(&ObservedStateSettings::default(), telemetry.registry());
    let shutdown_tx = runtime_tx.clone();
    let shutdown = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        shutdown_tx
            .try_send(RuntimeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(1),
                reason: "e2e complete".to_owned(),
            })
            .expect("request pipeline shutdown");
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
        .expect("running pipeline shuts down cleanly");
    shutdown.join().expect("shutdown thread");

    let outputs = captured().lock().expect("capture mutex");
    assert!(!outputs.is_empty(), "pipeline exported no sketch window");
    let mut decoded = Vec::new();
    for pdata in outputs.iter().cloned() {
        decoded.extend(
            decode_pdata_to_observations(pdata)
                .expect("decode pipeline output")
                .observations,
        );
    }
    let envelope = decoded
        .iter()
        .find_map(|observation| observation.value.envelope.as_ref())
        .expect("export contains a serialized sketch envelope");
    assert_eq!(envelope.metric_name, "request.duration.p99");
    assert_eq!(envelope.sketch_type, SketchType::DDSketch);
    assert_eq!(envelope.labels, vec![KeyValue::new("route", "/checkout")]);
    assert!(!envelope.payload.is_empty());
}
