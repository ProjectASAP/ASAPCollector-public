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
use otel_arrow_dfe_engine::config::{ExporterConfig, ProcessorConfig, ReceiverConfig};
use otel_arrow_dfe_engine::context::{ControllerContext, PipelineContext};
use otel_arrow_dfe_engine::control::{
    pipeline_completion_msg_channel, runtime_ctrl_msg_channel, NodeControlMsg, RuntimeControlMsg,
};
use otel_arrow_dfe_engine::error::ProcessorErrorKind;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::{
    exporter as local_exporter, processor as local_processor, receiver as local_receiver,
};
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{
    ExporterFactory, MessageSourceLocalEffectHandlerExtension, ProcessorFactory, ReceiverFactory,
};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::{
    OTAP_EXPORTER_FACTORIES, OTAP_PIPELINE_FACTORY, OTAP_PROCESSOR_FACTORIES,
    OTAP_RECEIVER_FACTORIES,
};
use otel_arrow_dfe_pdata::{OtlpProtoBytes, TryIntoWithOptions};
use otel_arrow_dfe_state::store::ObservedStateStore;
use otel_arrow_dfe_telemetry::InternalTelemetrySystem;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs};

const WORKER_ARG: &str = "--df-worker";
const SOURCE_URN: &str = "urn:asap:receiver:otlp_file";
const SINK_URN: &str = "urn:asap:exporter:otlp_file";
const BASELINE_URN: &str = "urn:asap:processor:benchmark_baseline";
static INPUTS: OnceLock<Vec<PathBuf>> = OnceLock::new();
static CAPTURED: OnceLock<(Mutex<Vec<OtapPdata>>, Condvar)> = OnceLock::new();
fn captured() -> &'static (Mutex<Vec<OtapPdata>>, Condvar) {
    CAPTURED.get_or_init(|| (Mutex::new(Vec::new()), Condvar::new()))
}

fn scalar_input(start: u64, end: u64) -> OtapPdata {
    let values = (start..=end)
        .map(|value| SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 0,
            resource_labels: vec![KeyValue::new("service.name", "checkout")],
            labels: vec![
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", "/checkout"),
                KeyValue::new("http.response.status_code", "200"),
            ],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::Unspecified,
            payload: vec![],
            hash_spec: None,
            metric_name: "http.server.request.duration".into(),
            count: 0,
            aggregation_temporality: 0,
            value: value as f64,
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

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BaselineConfig {
    operation: String,
    #[serde(default = "one_input")]
    expected_inputs: usize,
}

const fn one_input() -> usize {
    1
}

struct BaselineProcessor {
    config: BaselineConfig,
    values: Vec<f64>,
    runs: Vec<Vec<f64>>,
    received: usize,
}

fn merge_sorted_runs(left: &[f64], right: &[f64]) -> Vec<f64> {
    let (mut left_index, mut right_index) = (0, 0);
    let mut merged = Vec::with_capacity(left.len() + right.len());
    while left_index < left.len() && right_index < right.len() {
        if left[left_index].total_cmp(&right[right_index]).is_le() {
            merged.push(left[left_index]);
            left_index += 1;
        } else {
            merged.push(right[right_index]);
            right_index += 1;
        }
    }
    merged.extend_from_slice(&left[left_index..]);
    merged.extend_from_slice(&right[right_index..]);
    merged
}

fn values_from_pdata(pdata: OtapPdata) -> Result<Vec<f64>, String> {
    Ok(decode_pdata_to_observations(pdata)
        .map_err(|error| error.to_string())?
        .observations
        .into_iter()
        .map(|observation| observation.value.float)
        .collect())
}

fn values_pdata(values: &[f64], metric_name: &str) -> Result<OtapPdata, String> {
    let envelopes = values
        .iter()
        .copied()
        .map(|value| SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 0,
            resource_labels: vec![KeyValue::new("service.name", "checkout")],
            labels: vec![
                KeyValue::new("http.request.method", "GET"),
                KeyValue::new("http.route", "/checkout"),
                KeyValue::new("http.response.status_code", "200"),
            ],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::Unspecified,
            payload: vec![],
            hash_spec: None,
            metric_name: metric_name.into(),
            count: 0,
            aggregation_temporality: 0,
            value,
        })
        .collect::<Vec<_>>();
    encode_envelopes_to_pdata(&envelopes).map_err(|error| error.to_string())
}

fn exact_quantile_pdata(p50: f64, p99: f64) -> Result<OtapPdata, String> {
    let mut envelopes = Vec::new();
    for (quantile, value) in [("0.5", p50), ("0.99", p99)] {
        let mut envelope = SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 0,
            resource_labels: vec![KeyValue::new("service.name", "checkout")],
            labels: vec![KeyValue::new("http.route", "/checkout")],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::Unspecified,
            payload: vec![],
            hash_spec: None,
            metric_name: "request.duration.exact_estimate".into(),
            count: 0,
            aggregation_temporality: 0,
            value,
        };
        envelope.labels.push(KeyValue::new("quantile", quantile));
        envelopes.push(envelope);
    }
    encode_envelopes_to_pdata(&envelopes).map_err(|error| error.to_string())
}

#[async_trait(?Send)]
impl local_processor::Processor<OtapPdata> for BaselineProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local_processor::EffectHandler<OtapPdata>,
    ) -> Result<(), otel_arrow_dfe_engine::error::Error> {
        if let Message::PData(pdata) = msg {
            if self.config.operation == "pass" && self.config.expected_inputs == 1 {
                effects.send_message_with_source_node(pdata).await?;
                return Ok(());
            }
            let mut values = values_from_pdata(pdata).map_err(|error| {
                otel_arrow_dfe_engine::error::Error::ProcessorError {
                    processor: effects.processor_id(),
                    kind: ProcessorErrorKind::Other,
                    error,
                    source_detail: String::new(),
                }
            })?;
            if self.config.operation == "sort" {
                values.sort_by(f64::total_cmp);
            }
            if self.config.operation == "merge_sorted" {
                self.runs.push(values);
            } else {
                self.values.extend(values);
            }
            self.received += 1;
            if self.received == self.config.expected_inputs {
                if self.config.operation == "merge_sorted" {
                    self.values = merge_sorted_runs(&self.runs[0], &self.runs[1]);
                }
                let output = if self.config.operation == "estimate" {
                    let at =
                        |q: f64| self.values[((self.values.len() - 1) as f64 * q).round() as usize];
                    exact_quantile_pdata(at(0.5), at(0.99))
                } else {
                    values_pdata(&self.values, "request.duration")
                }
                .map_err(|error| {
                    otel_arrow_dfe_engine::error::Error::ProcessorError {
                        processor: effects.processor_id(),
                        kind: ProcessorErrorKind::Other,
                        error,
                        source_detail: String::new(),
                    }
                })?;
                effects.send_message_with_source_node(output).await?;
            }
        }
        Ok(())
    }
}

fn create_baseline(
    _ctx: PipelineContext,
    node: NodeId,
    config: Arc<NodeUserConfig>,
    runtime: &ProcessorConfig,
    _caps: &Capabilities,
) -> Result<ProcessorWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let parsed = serde_json::from_value(config.config.clone()).map_err(|error| {
        otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: error.to_string(),
        }
    })?;
    Ok(ProcessorWrapper::local(
        BaselineProcessor {
            config: parsed,
            values: Vec::new(),
            runs: Vec::new(),
            received: 0,
        },
        node,
        config,
        runtime,
    ))
}

#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static BASELINE_FACTORY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: BASELINE_URN,
    create: create_baseline,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::no_config,
};

fn pipeline_yaml(role: &str, trace: &Path) -> Result<String, String> {
    let processor = match role {
        "create_a" | "create_b" => format!(
            r#"type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "kll"
      encoding: "Msgpack"
      window_size: "20ms"
      output_metric_name: "request.duration.sketch"
      agg_id: 7
      sketch_params: {{ k: 200 }}
      transmit_sketch: true
      quantiles: []"#
        ),
        "merge" | "estimate" => {
            let (name, transmit, quantiles) = if role == "merge" {
                ("request.duration.merged_sketch", true, "[]")
            } else {
                ("request.duration.estimate", false, "[0.5, 0.99]")
            };
            format!(
                r#"type: "urn:asap:processor:asap_sketches"
    config:
      sketch_type: "kll"
      encoding: "Msgpack"
      window_size: "20ms"
      output_metric_name: "{name}"
      agg_id: 7
      sketch_params: {{ k: 200 }}
      transmit_sketch: {transmit}
      quantiles: {quantiles}"#
            )
        }
        "raw_a" | "raw_b" | "raw_final" => {
            format!(
                r#"type: "{BASELINE_URN}"
    config: {{ operation: "pass", expected_inputs: 1 }}"#
            )
        }
        "raw_merge" => format!(
            r#"type: "{BASELINE_URN}"
    config: {{ operation: "pass", expected_inputs: 2 }}"#
        ),
        "exact_a" | "exact_b" => format!(
            r#"type: "{BASELINE_URN}"
    config: {{ operation: "sort", expected_inputs: 1 }}"#
        ),
        "exact_merge" => format!(
            r#"type: "{BASELINE_URN}"
    config: {{ operation: "merge_sorted", expected_inputs: 2 }}"#
        ),
        "exact_estimate" => format!(
            r#"type: "{BASELINE_URN}"
    config: {{ operation: "estimate", expected_inputs: 1 }}"#
        ),
        _ => return Err(format!("unknown role {role}")),
    };
    let trace = serde_json::to_string(&trace.to_string_lossy()).unwrap();
    Ok(format!(
        r#"
nodes:
  source: {{ type: "{SOURCE_URN}" }}
  processor:
    {processor}
  debug:
    type: "urn:otel:processor:debug"
    config:
      verbosity: detailed
      mode: batch
      signals: [metrics]
      output: {trace}
  sink: {{ type: "{SINK_URN}" }}
connections:
  - {{ from: source, to: processor }}
  - {{ from: processor, to: debug }}
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

fn validate_quantiles(results: &[(String, f64)], expected: [(f64, f64); 2]) -> Result<(), String> {
    if results.len() != 2 {
        return Err(format!(
            "expected p50 and p99, got {} result(s)",
            results.len()
        ));
    }
    for (quantile, expected) in expected {
        let quantile = quantile.to_string();
        let value = results
            .iter()
            .find_map(|(label, value)| (label == &quantile).then_some(*value))
            .ok_or_else(|| format!("missing quantile {quantile}"))?;
        let tolerance = (expected * 0.05).max(5.0);
        if (value - expected).abs() > tolerance {
            return Err(format!(
                "quantile {quantile} out of tolerance: got {value}, expected {expected} +/- {tolerance}"
            ));
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct ProcessorRun {
    role: &'static str,
    pid: u32,
}

#[derive(Serialize)]
struct DemoManifest {
    parent_pid: u32,
    points_per_source: u64,
    processors: Vec<ProcessorRun>,
}

struct ParentOptions {
    output_dir: Option<PathBuf>,
    result_manifest: Option<PathBuf>,
    points_per_source: u64,
    scenario: String,
}

fn parent_options(args: &[std::ffi::OsString]) -> Result<ParentOptions, String> {
    let mut options = ParentOptions {
        output_dir: None,
        result_manifest: None,
        points_per_source: 100,
        scenario: "kll".into(),
    };
    let mut index = 0;
    while index < args.len() {
        let flag = args[index].to_string_lossy();
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_ref() {
            "--output-dir" => options.output_dir = Some(PathBuf::from(value)),
            "--result-manifest" => options.result_manifest = Some(PathBuf::from(value)),
            "--points-per-source" => {
                options.points_per_source = value
                    .to_string_lossy()
                    .parse()
                    .map_err(|_| "--points-per-source must be a positive integer".to_owned())?;
                if options.points_per_source == 0 {
                    return Err("--points-per-source must be positive".into());
                }
            }
            "--scenario" => options.scenario = value.to_string_lossy().into_owned(),
            _ => return Err(format!("unknown argument {flag}")),
        }
        index += 2;
    }
    Ok(options)
}

fn run_parent(options: ParentOptions) -> Result<(), String> {
    println!("ASAP OTAP multi-process demo: OTLP Metrics boundaries, ASAPv1 sketches");
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = options.output_dir.unwrap_or_else(|| {
        env::temp_dir().join(format!("asap-otap-demo-{}-{id}", std::process::id()))
    });
    fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let p = |name: &str| dir.join(name);
    let ia = p("a.otlp");
    let ib = p("b.otlp");
    let sa = p("sa.otlp");
    let sb = p("sb.otlp");
    let merged = p("merged.otlp");
    let out = p("out.otlp");
    let n = options.points_per_source;
    write_otlp(&ia, scalar_input(1, n))?;
    write_otlp(&ib, scalar_input(n + 1, n * 2))?;
    let exe = env::current_exe().map_err(|e| e.to_string())?;
    let (role_a, role_b, role_merge, role_estimate) = match options.scenario.as_str() {
        "kll" => ("create_a", "create_b", "merge", "estimate"),
        "raw" => ("raw_a", "raw_b", "raw_merge", "raw_final"),
        "exact" => ("exact_a", "exact_b", "exact_merge", "exact_estimate"),
        other => {
            return Err(format!(
                "unknown scenario {other}; expected raw, exact, or kll"
            ))
        }
    };
    let a = spawn(&exe, role_a, &[&ia], &sa, &p("create-a.debug.log"));
    let b = spawn(&exe, role_b, &[&ib], &sb, &p("create-b.debug.log"));
    let mut processors = vec![
        ProcessorRun {
            role: role_a,
            pid: a.id(),
        },
        ProcessorRun {
            role: role_b,
            pid: b.id(),
        },
    ];
    wait(a, role_a);
    wait(b, role_b);
    let m = spawn(
        &exe,
        role_merge,
        &[&sa, &sb],
        &merged,
        &p("merge.debug.log"),
    );
    processors.push(ProcessorRun {
        role: role_merge,
        pid: m.id(),
    });
    wait(m, role_merge);
    let e = spawn(
        &exe,
        role_estimate,
        &[&merged],
        &out,
        &p("estimate.debug.log"),
    );
    processors.push(ProcessorRun {
        role: role_estimate,
        pid: e.id(),
    });
    wait(e, role_estimate);
    let decoded = decode_pdata_to_observations(read_otlp(&out)?)
        .map_err(|e| e.to_string())?
        .observations;
    if options.scenario == "raw" {
        if decoded.len() != (n * 2) as usize {
            return Err(format!(
                "raw backend received {} of {} signals",
                decoded.len(),
                n * 2
            ));
        }
    }
    let mut quantiles = Vec::with_capacity(decoded.len());
    for o in &decoded {
        if options.scenario == "raw" {
            continue;
        }
        let q = o
            .labels
            .iter()
            .find(|kv| kv.key == "quantile")
            .map_or("?", |kv| kv.value.as_str());
        println!(
            "result metric={} quantile={} value={:.3}",
            o.metric, q, o.value.float
        );
        if options.scenario != "raw" {
            let expected_metric = if options.scenario == "kll" {
                "request.duration.estimate"
            } else {
                "request.duration.exact_estimate"
            };
            assert_eq!(o.metric, expected_metric);
        }
        quantiles.push((q.to_owned(), o.value.float));
    }
    if options.scenario == "kll" {
        validate_quantiles(&quantiles, [(0.5, n as f64), (0.99, n as f64 * 1.98)])?;
    } else if options.scenario == "exact" {
        let exact_at = |q: f64| ((n * 2 - 1) as f64 * q).round() + 1.0;
        let expected = [("0.5", exact_at(0.5)), ("0.99", exact_at(0.99))];
        for (quantile, expected) in expected {
            let actual = quantiles
                .iter()
                .find_map(|(label, value)| (label == quantile).then_some(*value))
                .ok_or_else(|| format!("missing exact quantile {quantile}"))?;
            if actual != expected {
                return Err(format!(
                    "exact quantile {quantile}: got {actual}, expected {expected}"
                ));
            }
        }
    }
    if let Some(path) = options.result_manifest {
        let manifest = DemoManifest {
            parent_pid: std::process::id(),
            points_per_source: n,
            processors,
        };
        fs::write(
            path,
            serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    }
    println!(
        "success: {} ran four DF processors in four child OS processes",
        options.scenario
    );
    println!("official detailed debug traces: {}", dir.display());
    Ok(())
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
        match parent_options(&args).and_then(run_parent) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(2);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validate_quantiles;

    /// Scenario: The backend receives correct, incorrect, and incomplete quantile outputs.
    /// Guarantees: KLL result validation accepts bounded error and rejects bad or missing values.
    #[test]
    fn result_validation_requires_correct_p50_and_p99() {
        let expected = [(0.5, 100.0), (0.99, 198.0)];
        assert!(
            validate_quantiles(&[("0.5".into(), 100.0), ("0.99".into(), 198.0)], expected).is_ok()
        );
        assert!(
            validate_quantiles(&[("0.5".into(), 12.0), ("0.99".into(), 13.0)], expected).is_err()
        );
        assert!(validate_quantiles(&[("0.5".into(), 100.0)], expected).is_err());
    }
}
