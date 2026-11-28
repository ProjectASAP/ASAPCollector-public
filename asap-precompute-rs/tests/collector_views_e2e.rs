#![cfg(feature = "otap-engine")]

//! OTLP in, OTLP out: `collector_views` inside a real OTAP `RuntimePipeline`
//! built from YAML, running the q01 fixture's view (DDSketch α 0.05 per
//! series, 5 s windows over `data`) on ASAPPlanner's executor.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use asap_precompute_rs::otap::codec::decode_each;
use asap_precompute_rs::otap::planner_views;
use asap_precompute_rs::otap::view_executor::ViewExecutor;
use asap_sketchlib::DdSketch;
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
use otel_arrow_dfe_pdata::otlp::OtlpProtoBytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value as AnyValueKind;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    metric::Data, Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::{OtapPayload, TryIntoWithOptions};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;

const SOURCE_URN: &str = "urn:asap:receiver:collector_views_otlp";
const SINK_URN: &str = "urn:asap:exporter:collector_views_capture";
const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/collector-views/q01/collector-views.json"
);
/// 2026-01-01T00:00:00Z, a window end.
const T_MS: u64 = 1_767_225_600_000;

static CAPTURED: OnceLock<Mutex<Vec<OtapPdata>>> = OnceLock::new();

fn captured() -> &'static Mutex<Vec<OtapPdata>> {
    CAPTURED.get_or_init(|| Mutex::new(Vec::new()))
}

fn string(key: &str, value: &str) -> KeyValue {
    KeyValue::new(key, AnyValue::new_string(value))
}

/// Three series of `data` under Resource `service.name=j0`, each 10
/// samples `(series + 1) * 1..=10` in `(T - 5 s, T]`, and a metric `other`
/// no view reads.
fn input() -> MetricsData {
    let point = |series: u64, sample: u64| {
        NumberDataPoint::build()
            .time_unix_nano((T_MS - 5_000 + 500 * (sample + 1)) * 1_000_000)
            .value_double(((series + 1) * (sample + 1)) as f64)
            .attributes(vec![
                string("region", "r0"),
                string("service", &format!("s{series}")),
                string("label_0", "l0"),
            ])
            .finish()
    };
    let points: Vec<_> = (0..10)
        .flat_map(|sample| (0..3).map(move |series| point(series, sample)))
        .collect();
    let data = Metric::build()
        .name("data")
        .unit("s")
        .data_gauge(Gauge::new(points))
        .finish();
    let other = Metric::build()
        .name("other")
        .data_gauge(Gauge::new(vec![point(0, 0)]))
        .finish();
    MetricsData::new(vec![ResourceMetrics::new(
        Resource::build()
            .attributes(vec![string("service.name", "j0")])
            .finish(),
        vec![ScopeMetrics::new(
            InstrumentationScope::build().name("app.meter").finish(),
            vec![data, other],
        )],
    )])
}

fn pdata(data: MetricsData) -> OtapPdata {
    OtapPdata::new_todo_context(
        OtapPayload::try_from(OtlpProtoMessage::Metrics(data)).expect("from OTLP"),
    )
}

fn otlp(pdata: OtapPdata) -> MetricsData {
    let (_, payload) = pdata.into_parts();
    let bytes: OtlpProtoBytes = payload.try_into_with_default().expect("to OTLP");
    let OtlpProtoMessage::Metrics(data) = OtlpProtoMessage::try_from(bytes).expect("decodes")
    else {
        panic!("metrics");
    };
    data
}

struct OtlpReceiver;

#[async_trait(?Send)]
impl local_receiver::Receiver<OtapPdata> for OtlpReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl: local_receiver::ControlChannel<OtapPdata>,
        effect_handler: local_receiver::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, otel_arrow_dfe_engine::error::Error> {
        effect_handler
            .send_message_with_source_node(pdata(input()))
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

#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
static SOURCE_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SOURCE_URN,
    create: |_: PipelineContext,
             node: NodeId,
             config: Arc<NodeUserConfig>,
             runtime: &ReceiverConfig,
             _: &Capabilities| {
        Ok(ReceiverWrapper::local(OtlpReceiver, node, config, runtime))
    },
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

#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static SINK_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SINK_URN,
    create: |_: PipelineContext,
             node: NodeId,
             config: Arc<NodeUserConfig>,
             runtime: &ExporterConfig,
             _: &Capabilities| {
        Ok(ExporterWrapper::local(
            CaptureExporter,
            node,
            config,
            runtime,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

/// Runs `receiver -> collector_views -> capture` for 2 s: the idle timer
/// (every second) closes the window, whose end is long past.
fn run_pipeline() {
    let yaml = format!(
        r#"
nodes:
  source:
    type: "{SOURCE_URN}"
  views:
    type: "urn:asap:processor:collector_views"
    config:
      planner_views: "{FIXTURE}"
  sink:
    type: "{SINK_URN}"
connections:
  - from: source
    to: views
  - from: views
    to: sink
"#
    );
    let config = PipelineConfig::from_yaml("asap-views".into(), "views".into(), &yaml)
        .expect("pipeline YAML parses and validates");
    let telemetry = InternalTelemetrySystem::default();
    let pipeline_ctx = ControllerContext::new(telemetry.registry()).pipeline_context_with(
        PipelineGroupId::from("asap-views"),
        PipelineId::from("views"),
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
        std::thread::sleep(Duration::from_secs(2));
        shutdown_tx
            .try_send(RuntimeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(2),
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
}

fn attributes(kvs: &[KeyValue]) -> Vec<(String, String)> {
    kvs.iter()
        .map(|kv| {
            let value = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(AnyValueKind::StringValue(s)) => s.clone(),
                Some(AnyValueKind::IntValue(i)) => i.to_string(),
                Some(AnyValueKind::BytesValue(_)) => "<bytes>".into(),
                other => format!("{other:?}"),
            };
            (kv.key.clone(), value)
        })
        .collect()
}

fn envelope(point_attributes: &[KeyValue]) -> Vec<u8> {
    point_attributes
        .iter()
        .find(|kv| kv.key == "sketch.envelope")
        .and_then(|kv| match kv.value.as_ref()?.value.as_ref()? {
            AnyValueKind::BytesValue(bytes) => Some(bytes.clone()),
            _ => None,
        })
        .expect("a sketch.envelope")
}

/// The OTLP received is the OTLP sent: one Summary point per series under
/// `data.asap_view.1`, with the received Resource, scope, unit and point
/// attributes, the view identity as metadata, the window `(T - 5 s, T]` and
/// its count; the state is the executor's, and answers within α.
#[test]
fn otlp_in_view_states_out() {
    captured().lock().expect("capture mutex").clear();
    run_pipeline();

    // The same points through the executor directly, by service.
    let views = planner_views::load(std::path::Path::new(FIXTURE)).unwrap();
    let mut executor = ViewExecutor::new(&views).unwrap();
    decode_each(pdata(input()), |obs| executor.observe(obs)).unwrap();
    assert!(executor.flush().states.is_empty(), "the window is open");
    let direct: std::collections::BTreeMap<String, Vec<u8>> = executor
        .drain()
        .states
        .into_iter()
        .map(|s| {
            let service = s.labels.iter().find(|kv| kv.key == "service").unwrap();
            (service.value.clone(), s.payload)
        })
        .collect();
    assert_eq!(direct.len(), 3);

    let outputs: Vec<MetricsData> = captured()
        .lock()
        .expect("capture mutex")
        .iter()
        .cloned()
        .map(otlp)
        .collect();
    let mut points = 0;
    for data in &outputs {
        for resource in &data.resource_metrics {
            assert_eq!(
                attributes(&resource.resource.as_ref().unwrap().attributes),
                [("service.name".to_string(), "j0".to_string())]
            );
            for scope in &resource.scope_metrics {
                assert_eq!(scope.scope.as_ref().unwrap().name, "app.meter");
                for metric in &scope.metrics {
                    assert_eq!(metric.name, "data.asap_view.1", "only the view's states");
                    assert_eq!(metric.unit, "s");
                    let metadata = attributes(&metric.metadata);
                    assert_eq!(metadata[0].0, "asap.plan_id");
                    assert_eq!(metadata[0].1, views.views[0].identity.plan_id);
                    assert_eq!(metadata[1], ("asap.node_id".into(), "1".into()));
                    assert_eq!(
                        metadata[2],
                        (
                            "asap.view_state_key".into(),
                            views.views[0].identity.view_state_key.clone()
                        )
                    );
                    let Some(Data::Summary(summary)) = &metric.data else {
                        panic!("a Summary");
                    };
                    for point in &summary.data_points {
                        points += 1;
                        let attrs = attributes(&point.attributes);
                        assert_eq!(&attrs[0], &("region".into(), "r0".into()));
                        let service = attrs[1].1.clone();
                        assert_eq!(attrs[2], ("label_0".into(), "l0".into()));
                        assert_eq!(point.start_time_unix_nano, (T_MS - 5_000) * 1_000_000);
                        assert_eq!(point.time_unix_nano, T_MS * 1_000_000);
                        assert_eq!(point.count, 10);
                        let bytes = envelope(&point.attributes);
                        assert_eq!(bytes, direct[&service], "{service}");
                        // Series s: samples (s + 1) * 1..=10; p50 is 5 or 6
                        // times (s + 1).
                        let scale: f64 = service[1..].parse::<f64>().unwrap() + 1.0;
                        let p50 = DdSketch::from_asapv1(&bytes)
                            .unwrap()
                            .get_value_at_quantile(0.5)
                            .unwrap();
                        assert!(
                            (p50 / scale - 5.0).abs() / 5.0 <= 0.05
                                || (p50 / scale - 6.0).abs() / 6.0 <= 0.05,
                            "{service}: p50 {p50}"
                        );
                    }
                }
            }
        }
    }
    assert_eq!(points, 3, "one state per series");
}
