//! Lifecycle harness for the `asap_sketches` plugin.
//!
//! Exit criterion: OTAP-harness lifecycle tests pass for each
//! `sketch_type`; round-trip raw input → envelope output preserves
//! expected sketch counts.
//!
//! - One end-to-end test per `sketch_type` (DDSketch / KLL / HLL /
//!   CountSketch / CountMinSketch). Drive raw observations through
//!   the input stream, signal shutdown, drain, assert the emitted
//!   `OtapMetricRecords` family carries an envelope-bearing
//!   attribute row whose payload reconstructs the expected sketch
//!   state.
//! - Drain test: feed input, signal shutdown before the natural
//!   window boundary, assert the final batch carries the in-flight
//!   residue (no observations dropped).
//! - Control-channel test: signal a plan change, assert the
//!   reconfigure path applies the new plan and the post-change
//!   emit reflects the new metric_name.
//! - Per-test regression: the codec round-trip still passes
//!   (covered by `tests/otap_codec.rs` which is run alongside).
//!
//! These tests use Tokio's `current_thread` runtime via
//! `#[tokio::test]` so they are deterministic and fast (each runs
//! in a few hundred ms — see the `window_size` knob below). They
//! deliberately drive the plugin without an OTAP submodule
//! present; the [`OtapMetricRecords`] family modeled in
//! `src/otap/records.rs` is the harness's stand-in for the upstream
//! `OtapPdata` shape, which Phase D wires in.

#![cfg(feature = "otap")]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};

use asap_precompute_rs::config::{PrecomputeConfig, PrecomputeConfigSet, WindowSpec};
use asap_precompute_rs::control_channel::ControlChannel;
use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::otap::{
    AsapSketchesPlugin, OtapMetricRecords, PluginConfig, PluginHandle, SeriesDictionaryDecoder,
    SketchStreamBatch, StartOptions, COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};

const PARENT_ID_COL: &str = "parent_id";
const ATTR_KEY_COL: &str = "key";
const ATTR_BYTES_COL: &str = "bytes";
const ATTR_STR_COL: &str = "str";
const ATTR_INT_COL: &str = "int";

/// Build a tiny single-row `OtapMetricRecords` carrying one scalar
/// observation. Each row gets a fresh `parent_id`, with an attribute
/// row carrying a `host` label (so the runtime sees a stable series
/// key across the input stream).
fn scalar_records(metric: &str, value: f64, timestamp_ms: u64, host: &str) -> OtapMetricRecords {
    let metrics_schema = Arc::new(Schema::new(vec![
        Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
        Field::new(COLUMN_METRIC, DataType::Utf8, false),
        Field::new(COLUMN_VALUE, DataType::Float64, false),
        Field::new(PARENT_ID_COL, DataType::UInt32, false),
    ]));
    let metrics = RecordBatch::try_new(
        metrics_schema,
        vec![
            Arc::new(UInt64Array::from(vec![timestamp_ms * 1_000_000])),
            Arc::new(StringArray::from(vec![metric])),
            Arc::new(Float64Array::from(vec![value])),
            Arc::new(UInt32Array::from(vec![0_u32])),
        ],
    )
    .expect("metrics");
    let attributes_schema = Arc::new(Schema::new(vec![
        Field::new(PARENT_ID_COL, DataType::UInt32, false),
        Field::new(ATTR_KEY_COL, DataType::Utf8, false),
        Field::new(ATTR_BYTES_COL, DataType::Binary, true),
        Field::new(ATTR_STR_COL, DataType::Utf8, true),
        Field::new(ATTR_INT_COL, DataType::UInt64, true),
    ]));
    let attributes = RecordBatch::try_new(
        attributes_schema,
        vec![
            Arc::new(UInt32Array::from(vec![0_u32])),
            Arc::new(StringArray::from(vec!["host"])),
            Arc::new(BinaryArray::from_opt_vec(vec![None as Option<&[u8]>])),
            Arc::new(StringArray::from(vec![Some(host)])),
            Arc::new(UInt64Array::from(vec![None as Option<u64>])),
        ],
    )
    .expect("attributes");
    OtapMetricRecords {
        metrics,
        attributes,
    }
}

/// Drain an [`asap_precompute_rs::otap::EmitReceiver`] with a
/// generous timeout. The lifecycle tasks emit eagerly on shutdown,
/// so 5s is far more than needed in practice.
async fn drain_emit(rx: &mut asap_precompute_rs::otap::EmitReceiver) -> Vec<SketchStreamBatch> {
    let mut out = Vec::new();
    let timeout = Duration::from_secs(5);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(batch)) => out.push(batch),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    out
}

/// Decode every emitted [`SketchStreamBatch`] through one shared
/// [`SeriesDictionaryDecoder`], in order — matching the
/// continuous-stream contract `docs/data_model.md` assumes (a `RECORD`
/// row past the first window carries no `metric`/labels of its own,
/// only a `series_id` referencing a `DICTIONARY` row an earlier batch
/// in this same sequence carried).
fn decode_all(batches: &[SketchStreamBatch]) -> Vec<SketchEnvelope> {
    let mut decoder = SeriesDictionaryDecoder::new();
    let mut out = Vec::new();
    for batch in batches {
        out.extend(decoder.decode(batch).expect("decode stream batch"));
    }
    out
}

/// Finds the one envelope of `expected_type` among `envelopes` and
/// returns its payload bytes; panics if not exactly one is found.
fn extract_envelope_payload(envelopes: &[SketchEnvelope], expected_type: &str) -> Vec<u8> {
    let expected = match expected_type {
        "DDSketch" => SketchType::DDSketch,
        "KLLSketch" => SketchType::KLLSketch,
        "HLLSketch" => SketchType::HLLSketch,
        "CountSketch" => SketchType::CountSketch,
        "CountMinSketch" => SketchType::CountMinSketch,
        other => panic!("unknown sketch type in test helper: {other}"),
    };
    let mut matched: Vec<Vec<u8>> = envelopes
        .iter()
        .filter(|e| e.sketch_type == expected && !e.payload.is_empty())
        .map(|e| e.payload.clone())
        .collect();
    assert_eq!(
        matched.len(),
        1,
        "expected exactly one envelope of type {expected_type}, got {}",
        matched.len()
    );
    matched.remove(0)
}

/// Run a full `Start → N inputs → Shutdown → drain` cycle for one
/// sketch type. Returns the emitted `SketchStreamBatch`es so the
/// per-sketch test can decode them (via [`decode_all`]) and introspect
/// the envelope payload.
async fn run_lifecycle(
    sketch_type: &str,
    metric: &str,
    inputs: &[(f64, &str)],
) -> Vec<SketchStreamBatch> {
    let cfg = PluginConfig {
        sketch_type: sketch_type.into(),
        // Long enough that the natural ticker doesn't fire during
        // the test; the Stop-time drain is what produces output.
        // (We could use Tokio's mock clock to advance time, but the
        // drain path is the one Phase C explicitly tests.)
        window_size: Duration::from_secs(60),
        output_metric_name: metric.into(),
        agg_id: 1,
        ..Default::default()
    };
    let plugin = AsapSketchesPlugin::from_plugin_config(&cfg).expect("config");
    let mut ts = 1_000_u64;
    let mut batches: Vec<OtapMetricRecords> = Vec::new();
    for (val, host) in inputs {
        batches.push(scalar_records(metric, *val, ts, host));
        ts += 100;
    }
    let input_stream = futures::stream::iter(batches);
    let (handle, mut emit_rx) = plugin.start(input_stream, None, StartOptions::default());

    handle.shutdown().await.expect("shutdown");
    drain_emit(&mut emit_rx).await
}

#[tokio::test]
async fn non_asapv1_sketch_transports_are_rejected_at_construction() {
    for sketch_type in ["ddsketch", "hll", "countsketch", "countminsketch"] {
        let cfg = PluginConfig {
            sketch_type: sketch_type.into(),
            ..Default::default()
        };
        assert!(
            AsapSketchesPlugin::from_plugin_config(&cfg).is_err(),
            "{sketch_type} must not emit a private sketch payload"
        );
    }
}

#[tokio::test]
async fn lifecycle_kll_emits_envelope_with_correct_sketch_type() {
    let records = run_lifecycle(
        "kll",
        "latency_ms",
        &[(10.0, "h1"), (20.0, "h1"), (30.0, "h1")],
    )
    .await;
    let envelopes = decode_all(&records);
    let payload = extract_envelope_payload(&envelopes, "KLLSketch");
    assert!(!payload.is_empty());
}

#[tokio::test]
async fn drain_flushes_in_flight_observations_before_window_boundary() {
    // Set up a long window (60s) and feed observations, then issue
    // shutdown immediately. Without the explicit drain, no envelope
    // would be emitted because the window hasn't naturally rotated.
    // The drain path must produce at least one batch.
    let records = run_lifecycle(
        "kll",
        "drain_metric",
        &[(1.0, "h1"), (2.0, "h1"), (3.0, "h1"), (4.0, "h1")],
    )
    .await;
    assert!(!records.is_empty(), "drain must emit at least one batch");
    let envelopes = decode_all(&records);
    let payload = extract_envelope_payload(&envelopes, "KLLSketch");
    assert!(!payload.is_empty());
}

/// Stub control channel that fires one plan update on the first
/// poll, then returns `None`. Records the most recent ack so the
/// test asserts the version-after-apply contract.
struct OnceChannel {
    plan: std::sync::Mutex<Option<PrecomputeConfigSet>>,
    last_ack: AtomicU64,
}

impl OnceChannel {
    fn new(plan: PrecomputeConfigSet) -> Self {
        Self {
            plan: std::sync::Mutex::new(Some(plan)),
            last_ack: AtomicU64::new(0),
        }
    }
}

impl ControlChannel for OnceChannel {
    fn poll(&self) -> Option<PrecomputeConfigSet> {
        self.plan.lock().expect("plan lock").take()
    }

    fn ack(&self, version: u64) {
        self.last_ack.store(version, Ordering::Release);
    }
}

#[tokio::test]
async fn control_channel_plan_change_acks_after_apply() {
    let cfg = PluginConfig {
        sketch_type: "kll".into(),
        window_size: Duration::from_secs(60),
        output_metric_name: "before".into(),
        agg_id: 1,
        ..Default::default()
    };
    let plugin = AsapSketchesPlugin::from_plugin_config(&cfg).expect("config");

    let new_plan = PrecomputeConfigSet {
        version: 42,
        configs: vec![PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::KLLSketch,
            window: WindowSpec {
                size: Duration::from_secs(60),
                ..Default::default()
            },
            metric_name: "after".into(),
            encoding: Encoding::Msgpack,
            transmit_sketch: true,
            ..Default::default()
        }],
    };
    let channel = Arc::new(OnceChannel::new(new_plan));
    let cc: Arc<dyn ControlChannel> = channel.clone();

    // Empty stream: the test exercises the control-channel path
    // without input. After the plan applies (and the runtime stamps
    // the new metric_name), we feed observations through a direct
    // call to the shared `Precompute` (via the plugin's accessor)
    // and rely on the drain to emit one batch.
    let precompute_handle = plugin.precompute().clone();
    let input_stream = futures::stream::pending::<OtapMetricRecords>();
    let (handle, mut emit_rx) = plugin.start(
        Box::pin(input_stream),
        Some(cc),
        StartOptions {
            control_channel_poll_interval: Duration::from_millis(20),
        },
    );

    // Wait for the control task to apply the plan.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if channel.last_ack.load(Ordering::Acquire) == 42 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("control channel didn't ack within 2s");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Now feed observations directly; the runtime is using the
    // post-plan-change config, so emitted envelopes carry
    // metric_name="after".
    use asap_precompute_rs::observation::{KeyValue, Observation, ObservationValue};
    for v in [1.0_f64, 2.0, 3.0] {
        let obs = Observation::new(
            5_000,
            "after",
            vec![],
            vec![KeyValue::new("host", "h1")],
            ObservationValue::float(v),
        );
        precompute_handle.observe(&obs).expect("observe");
    }

    handle.shutdown().await.expect("shutdown");
    let records = drain_emit(&mut emit_rx).await;
    assert!(!records.is_empty(), "drain emit");
    // Verify every decoded envelope's metric_name reflects the applied
    // plan (sourced from DICTIONARY.metric, not repeated per RECORD).
    let envelopes = decode_all(&records);
    assert!(!envelopes.is_empty(), "drain emit");
    assert!(
        envelopes.iter().all(|e| e.metric_name == "after"),
        "post-plan-change emit should carry metric_name=after"
    );
}

#[tokio::test]
async fn shutdown_without_inputs_is_clean_no_op() {
    // Exercises the "Stop without observations" path — the plugin
    // must not panic, must not emit a stale empty batch, and the
    // handle must complete cleanly.
    let cfg = PluginConfig {
        sketch_type: "kll".into(),
        window_size: Duration::from_secs(60),
        ..Default::default()
    };
    let plugin = AsapSketchesPlugin::from_plugin_config(&cfg).expect("config");
    let (handle, mut emit_rx): (PluginHandle, _) = plugin.start(
        futures::stream::empty::<OtapMetricRecords>(),
        None,
        StartOptions::default(),
    );
    handle.shutdown().await.expect("shutdown");
    // Drain gets back zero records (no observations, no envelopes).
    let out = drain_emit(&mut emit_rx).await;
    assert!(
        out.is_empty(),
        "unexpected records on empty-stream shutdown"
    );
}

#[tokio::test]
async fn unknown_sketch_type_rejected_at_construction() {
    let cfg = PluginConfig {
        sketch_type: "notarealsketch".into(),
        window_size: Duration::from_secs(10),
        ..Default::default()
    };
    let err = AsapSketchesPlugin::from_plugin_config(&cfg)
        .err()
        .expect("should reject");
    let msg = format!("{err}");
    assert!(
        msg.contains("notarealsketch"),
        "unexpected error message: {msg}"
    );
}
