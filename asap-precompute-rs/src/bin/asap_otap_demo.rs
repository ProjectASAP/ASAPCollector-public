//! Multi-process OTAP sketch create -> merge -> estimate demonstration.
//! Each ASAP processor runs in its own OS process and OTAP RuntimePipeline.

use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::observation::KeyValue;
use asap_precompute_rs::otap::codec::{decode_pdata_to_observations, encode_envelopes_to_pdata};
use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_config::observed_state::{ObservedStateSettings, SendPolicy};
use otel_arrow_dfe_config::pipeline::PipelineConfig;
use otel_arrow_dfe_config::policy::{ChannelCapacityPolicy, TelemetryPolicy};
use otel_arrow_dfe_config::{DeployedPipelineKey, PipelineGroupId, PipelineId, SignalType};
use otel_arrow_dfe_core_nodes as _;
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
use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs};

const WORKER_ARG: &str = "--df-worker";
const SOURCE_URN: &str = "urn:asap:receiver:otlp_file";
const SINK_URN: &str = "urn:asap:exporter:otlp_file";
static INPUTS: OnceLock<Vec<PathBuf>> = OnceLock::new();
static CAPTURED: OnceLock<(Mutex<Vec<OtapPdata>>, Condvar)> = OnceLock::new();
fn captured() -> &'static (Mutex<Vec<OtapPdata>>, Condvar) {
    CAPTURED.get_or_init(|| (Mutex::new(Vec::new()), Condvar::new()))
}

fn scalar_input(start: i32, end: i32) -> OtapPdata {
    let values = (start..=end)
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
            metric_name: "request.duration".into(),
            count: 0,
            aggregation_temporality: 0,
            value: f64::from(value),
        })
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&values).expect("encode input")
}

fn write_otlp(path: &Path, pdata: OtapPdata) -> Result<(), String> {
    let (_, payload) = pdata.into_parts();
    let encoded = <_ as TryIntoWithOptions<OtlpProtoBytes>>::try_into_with_default(payload)
        .map_err(|e| e.to_string())?;
    match encoded {
        OtlpProtoBytes::ExportMetricsRequest(bytes) => {
            fs::write(path, bytes).map_err(|e| e.to_string())
        }
        _ => Err("non-metrics signal".into()),
    }
}
fn read_otlp(path: &Path) -> Result<OtapPdata, String> {
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    Ok(OtapPdata::new_todo_context(
        OtlpProtoBytes::new_from_bytes(SignalType::Metrics, bytes).into(),
    ))
}

struct FileReceiver;
#[async_trait(?Send)]
impl local_receiver::Receiver<OtapPdata> for FileReceiver {
    async fn start(
        self: Box<Self>,
        mut ctrl: local_receiver::ControlChannel<OtapPdata>,
        effects: local_receiver::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, otel_arrow_dfe_engine::error::Error> {
        for path in INPUTS.get().expect("inputs initialized") {
            effects
                .send_message_with_source_node(read_otlp(path).expect("read OTLP input"))
                .await?;
        }
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
    _caps: &Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    Ok(ReceiverWrapper::local(FileReceiver, node, config, runtime))
}
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
static SOURCE_FACTORY: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SOURCE_URN,
    create: create_source,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

struct FileExporter;
#[async_trait(?Send)]
impl local_exporter::Exporter<OtapPdata> for FileExporter {
    async fn start(
        self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        _effects: local_exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, otel_arrow_dfe_engine::error::Error> {
        loop {
            match inbox.recv().await? {
                Message::PData(pdata) => {
                    let (outputs, ready) = captured();
                    outputs.lock().expect("capture mutex").push(pdata);
                    ready.notify_all();
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
    Ok(ExporterWrapper::local(FileExporter, node, config, runtime))
}
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
static SINK_FACTORY: ExporterFactory<OtapPdata> = ExporterFactory {
    name: SINK_URN,
    create: create_sink,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

fn pipeline_yaml(role: &str, trace: &Path) -> Result<String, String> {
    let (name, transmit, quantiles) = match role {
        "create_a" | "create_b" => ("request.duration.sketch", true, "[]"),
        "merge" => ("request.duration.merged_sketch", true, "[]"),
        "estimate" => ("request.duration.estimate", false, "[0.5, 0.99]"),
        _ => return Err(format!("unknown role {role}")),
    };
    let trace = serde_json::to_string(&trace.to_string_lossy()).unwrap();
    Ok(format!(
        r#"
nodes:
  source: {{ type: "{SOURCE_URN}" }}
  sketch:
    type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "kll"
      encoding: "Msgpack"
      window_size: "20ms"
      output_metric_name: "{name}"
      agg_id: 7
      sketch_params: {{ k: 200 }}
      transmit_sketch: {transmit}
      quantiles: {quantiles}
  debug:
    type: "urn:otel:processor:debug"
    config:
      verbosity: detailed
      mode: batch
      signals: [metrics]
      output: {trace}
  sink: {{ type: "{SINK_URN}" }}
connections:
  - {{ from: source, to: sketch }}
  - {{ from: sketch, to: debug }}
  - {{ from: debug, to: sink }}
"#
    ))
}

fn run_worker(
    role: &str,
    inputs: Vec<PathBuf>,
    output: PathBuf,
    trace: PathBuf,
) -> Result<(), String> {
    INPUTS
        .set(inputs)
        .map_err(|_| "inputs already initialized".to_owned())?;
    captured().0.lock().unwrap().clear();
    let config = PipelineConfig::from_yaml(
        "asap-demo".into(),
        role.to_owned().into(),
        &pipeline_yaml(role, &trace)?,
    )
    .map_err(|e| e.to_string())?;
    let telemetry = InternalTelemetrySystem::default();
    let ctx = ControllerContext::new(telemetry.registry()).pipeline_context_with(
        PipelineGroupId::from("asap-demo"),
        PipelineId::from(role.to_owned()),
        0,
        1,
        0,
    );
    let entity = ctx.register_pipeline_entity();
    let runtime = OTAP_PIPELINE_FACTORY
        .build(
            ctx.clone(),
            config,
            ChannelCapacityPolicy::default(),
            TelemetryPolicy::default(),
            None,
            Default::default(),
            None,
            None,
        )
        .map_err(|e| e.to_string())?;
    let policy = ChannelCapacityPolicy::default();
    let (runtime_tx, runtime_rx) = runtime_ctrl_msg_channel(policy.control.pipeline);
    let (completion_tx, completion_rx) = pipeline_completion_msg_channel(policy.control.completion);
    let observed = ObservedStateStore::new(&ObservedStateSettings::default(), telemetry.registry());
    let shutdown_tx = runtime_tx.clone();
    let shutdown = std::thread::spawn(move || {
        let (outputs, ready) = captured();
        let guard = outputs.lock().expect("capture mutex");
        let (_guard, wait) = ready
            .wait_timeout_while(guard, Duration::from_secs(10), |items| items.is_empty())
            .expect("capture wait");
        shutdown_tx
            .try_send(RuntimeControlMsg::Shutdown {
                deadline: Instant::now() + Duration::from_secs(2),
                reason: if wait.timed_out() {
                    "timed out waiting for pipeline output".into()
                } else {
                    "pipeline output complete".into()
                },
            })
            .expect("shutdown");
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
    shutdown
        .join()
        .map_err(|_| "shutdown thread panic".to_owned())?;
    let mut outputs = captured().0.lock().unwrap();
    if outputs.len() != 1 {
        return Err(format!("expected one output, got {}", outputs.len()));
    }
    write_otlp(&output, outputs.pop().unwrap())
}

fn spawn(exe: &Path, role: &str, inputs: &[&Path], output: &Path, trace: &Path) -> Child {
    let mut cmd = Command::new(exe);
    cmd.arg(WORKER_ARG).arg(role);
    for input in inputs {
        cmd.arg(input);
    }
    cmd.arg(output).arg(trace);
    let child = cmd.spawn().unwrap_or_else(|e| panic!("launch {role}: {e}"));
    println!(
        "launched {role} DF processor pid={} trace={}",
        child.id(),
        trace.display()
    );
    child
}
fn wait(mut child: Child, role: &str) {
    let status = child.wait().unwrap();
    assert!(status.success(), "{role} failed: {status}");
}

fn validate_quantiles(results: &[(String, f64)]) -> Result<(), String> {
    if results.len() != 2 {
        return Err(format!(
            "expected p50 and p99, got {} result(s)",
            results.len()
        ));
    }
    for (quantile, expected) in [("0.5", 100.0), ("0.99", 198.0)] {
        let value = results
            .iter()
            .find_map(|(label, value)| (label == quantile).then_some(*value))
            .ok_or_else(|| format!("missing quantile {quantile}"))?;
        if (value - expected).abs() > 5.0 {
            return Err(format!(
                "quantile {quantile} out of tolerance: got {value}, expected {expected} +/- 5"
            ));
        }
    }
    Ok(())
}

fn run_parent() {
    println!("ASAP OTAP multi-process demo: OTLP Metrics boundaries, ASAPv1 sketches");
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = env::temp_dir().join(format!("asap-otap-demo-{}-{id}", std::process::id()));
    fs::create_dir(&dir).unwrap();
    let p = |name: &str| dir.join(name);
    let ia = p("a.otlp");
    let ib = p("b.otlp");
    let sa = p("sa.otlp");
    let sb = p("sb.otlp");
    let merged = p("merged.otlp");
    let out = p("out.otlp");
    write_otlp(&ia, scalar_input(1, 100)).unwrap();
    write_otlp(&ib, scalar_input(101, 200)).unwrap();
    let exe = env::current_exe().unwrap();
    let a = spawn(&exe, "create_a", &[&ia], &sa, &p("create-a.debug.log"));
    let b = spawn(&exe, "create_b", &[&ib], &sb, &p("create-b.debug.log"));
    wait(a, "create_a");
    wait(b, "create_b");
    let m = spawn(&exe, "merge", &[&sa, &sb], &merged, &p("merge.debug.log"));
    wait(m, "merge");
    let e = spawn(&exe, "estimate", &[&merged], &out, &p("estimate.debug.log"));
    wait(e, "estimate");
    let decoded = decode_pdata_to_observations(read_otlp(&out).unwrap())
        .unwrap()
        .observations;
    let mut quantiles = Vec::with_capacity(decoded.len());
    for o in &decoded {
        let q = o
            .labels
            .iter()
            .find(|kv| kv.key == "quantile")
            .map_or("?", |kv| kv.value.as_str());
        println!(
            "result metric={} quantile={} value={:.3}",
            o.metric, q, o.value.float
        );
        assert_eq!(o.metric, "request.duration.estimate");
        quantiles.push((q.to_owned(), o.value.float));
    }
    validate_quantiles(&quantiles).expect("correct p50/p99 results");
    println!("success: four DF processors ran in four child OS processes");
    println!("official detailed debug traces: {}", dir.display());
}

fn main() {
    let args = env::args_os().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|a| a == WORKER_ARG) {
        if args.len() < 5 {
            eprintln!("usage: {WORKER_ARG} ROLE INPUT... OUTPUT TRACE");
            std::process::exit(2);
        }
        let role = args[1].to_string_lossy().into_owned();
        let paths = args[2..].iter().map(PathBuf::from).collect::<Vec<_>>();
        let trace = paths[paths.len() - 1].clone();
        let output = paths[paths.len() - 2].clone();
        let inputs = paths[..paths.len() - 2].to_vec();
        if let Err(e) = run_worker(&role, inputs, output, trace) {
            eprintln!("[{role}] {e}");
            std::process::exit(1);
        }
    } else {
        run_parent();
    }
}

#[cfg(test)]
mod tests {
    use super::validate_quantiles;

    #[test]
    fn result_validation_requires_correct_p50_and_p99() {
        assert!(validate_quantiles(&[("0.5".into(), 100.0), ("0.99".into(), 198.0)]).is_ok());
        assert!(validate_quantiles(&[("0.5".into(), 12.0), ("0.99".into(), 13.0)]).is_err());
        assert!(validate_quantiles(&[("0.5".into(), 100.0)]).is_err());
    }
}
