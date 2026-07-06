//! Type-level smoke tests for the bootstrap public API.
//!
//! These tests don't exercise the runtime (which is
//! `unimplemented!()` in this PR — see Phase 3 step 2). They lock in
//! the trait surface, the constructor shapes, and the serde
//! round-trip behavior so subsequent migration PRs notice if
//! anything drifts.

use asap_precompute_rs::adapter::Adapter;
use asap_precompute_rs::control_channel::ControlChannel;
use asap_precompute_rs::precompute::{
    BoxedObserver, FrequencyEntry, Precompute, PrecomputeImpl, SketchObserver,
};
use asap_precompute_rs::{
    AggregationMode, Encoding, KeyValue, LabelMatcher, MatchOp, Observation, ObservationValue,
    ObservationValueKind, OnOverflow, PrecomputeConfig, PrecomputeConfigSet, SketchEnvelope,
    SketchType,
};

// ---------------------------------------------------------------
// Constructors

#[test]
fn observation_new_constructs_with_all_four_value_kinds() {
    let labels = vec![KeyValue::new("path", "/api")];

    let o_float = Observation::new(
        1_000,
        "metric",
        vec![],
        labels.clone(),
        ObservationValue::float(1.5),
    );
    assert_eq!(o_float.value.kind, ObservationValueKind::Float);

    let o_hash = Observation::new(
        1_000,
        "metric",
        vec![],
        labels.clone(),
        ObservationValue::hash(0xDEADBEEF),
    );
    assert_eq!(o_hash.value.kind, ObservationValueKind::Hash);

    let o_bytes = Observation::new(
        1_000,
        "metric",
        vec![],
        labels.clone(),
        ObservationValue::bytes(b"opaque-key".to_vec()),
    );
    assert_eq!(o_bytes.value.kind, ObservationValueKind::Bytes);

    let env = SketchEnvelope {
        schema_version: 1,
        sketch_type: SketchType::DDSketch,
        encoding: Encoding::ProtoFull,
        ..Default::default()
    };
    let o_env = Observation::new(
        1_000,
        "metric",
        vec![],
        labels,
        ObservationValue::envelope(env),
    );
    assert_eq!(o_env.value.kind, ObservationValueKind::Envelope);
}

#[test]
fn precompute_config_default_has_sensible_defaults() {
    let cfg = PrecomputeConfig::default();
    assert_eq!(cfg.agg_id, 0);
    assert_eq!(cfg.sketch_type, SketchType::Unspecified);
    assert_eq!(cfg.mode, AggregationMode::Tumbling);
    assert_eq!(cfg.on_overflow, OnOverflow::Drop);
    assert_eq!(cfg.encoding, Encoding::Unspecified);
    assert!(!cfg.delta_transmission);
    assert!(!cfg.global_aggregation);
    assert!(!cfg.omit_resource_attrs);
    assert!(!cfg.emit_window_stats);
    assert_eq!(cfg.matchers.len(), 0);
    assert_eq!(cfg.aggregate_by.len(), 0);
}

#[test]
fn label_matcher_default_op_returns_equal() {
    assert_eq!(LabelMatcher::default_op(), MatchOp::Equal);
    let m = LabelMatcher::default();
    assert_eq!(m.op, MatchOp::Equal);
}

// ---------------------------------------------------------------
// SketchEnvelope serde round-trip

#[test]
fn sketch_envelope_round_trips_through_serde_json() {
    let env = SketchEnvelope {
        schema_version: 1,
        sketch_type: SketchType::CountMinSketch,
        agg_id: 1234,
        resource_labels: vec![KeyValue::new("region", "us-west-2")],
        labels: vec![
            KeyValue::new("status", "200"),
            KeyValue::new("path", "/api"),
        ],
        window_start_ms: 1_700_000_000_000,
        window_end_ms: 1_700_000_060_000,
        encoding: Encoding::ProtoDelta,
        payload: vec![0x01, 0x02, 0x03, 0x04, 0x05],
        hash_spec: None,
        metric_name: "http_request_count".into(),
        count: 4096,
        aggregation_temporality: 1,
    };
    let json = serde_json::to_string(&env).expect("serialize");
    let decoded: SketchEnvelope = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(env, decoded);
}

#[test]
fn precompute_config_set_round_trips_through_serde_json() {
    let cs = PrecomputeConfigSet {
        version: 17,
        configs: vec![
            PrecomputeConfig {
                agg_id: 1,
                sketch_type: SketchType::DDSketch,
                ..Default::default()
            },
            PrecomputeConfig {
                agg_id: 2,
                sketch_type: SketchType::HLLSketch,
                ..Default::default()
            },
        ],
    };
    let json = serde_json::to_string(&cs).expect("serialize");
    let decoded: PrecomputeConfigSet = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(cs, decoded);
    assert_eq!(decoded.find_by_agg_id(2).map(|c| c.agg_id), Some(2));
}

// ---------------------------------------------------------------
// ControlChannel trait can be implemented by a stub

struct NoOpChannel;

impl ControlChannel for NoOpChannel {
    fn poll(&self) -> Option<PrecomputeConfigSet> {
        None
    }
    fn ack(&self, _plan_version: u64) {}
}

#[test]
fn control_channel_trait_implementable_by_stub() {
    let c: Box<dyn ControlChannel> = Box::new(NoOpChannel);
    assert!(c.poll().is_none());
    c.ack(99);
}

// ---------------------------------------------------------------
// Adapter trait can be implemented by a stub

struct NoOpAdapter;

impl Adapter for NoOpAdapter {
    type Event = ();

    fn decode(
        &self,
        _ev: Self::Event,
    ) -> Result<Vec<Observation>, asap_precompute_rs::precompute::PrecomputeError> {
        Ok(vec![])
    }

    fn encode(
        &self,
        _envelopes: &[SketchEnvelope],
    ) -> Result<Self::Event, asap_precompute_rs::precompute::PrecomputeError> {
        Ok(())
    }

    fn schedule_tick(
        &self,
        _period: std::time::Duration,
        _cb: Box<dyn Fn() + Send + Sync>,
    ) -> asap_precompute_rs::adapter::CancelTick {
        Box::new(|| {})
    }

    fn emit_telemetry(&self, _stats: &asap_precompute_rs::precompute::StatsSnapshot) {}
}

#[test]
fn adapter_trait_implementable_by_stub() {
    let a = NoOpAdapter;
    let envs: Vec<SketchEnvelope> = vec![];
    assert!(a.encode(&envs).is_ok());
    assert!(a.decode(()).is_ok());
}

// ---------------------------------------------------------------
// SketchObserver trait can be implemented by a stub (proves the
// trait is dyn-safe and accepts the documented value shapes).

struct DropObserver;

impl SketchObserver for DropObserver {
    fn observe(
        &self,
        _sketch: &mut dyn asap_precompute_rs::Sketch,
        _obs: &Observation,
    ) -> Result<(), asap_precompute_rs::precompute::PrecomputeError> {
        Ok(())
    }
}

#[test]
fn sketch_observer_trait_implementable_by_stub() {
    let _: BoxedObserver = Box::new(DropObserver);
}

// ---------------------------------------------------------------
// PrecomputeImpl constructs without runtime wiring.

#[test]
fn precompute_impl_constructs_with_no_config() {
    let p = PrecomputeImpl::new(None, None, None);
    assert_eq!(p.sketch_type(), SketchType::Unspecified);
    assert!(!p.is_closed());
    p.shutdown().expect("shutdown succeeds");
    assert!(p.is_closed());
}

#[test]
fn precompute_impl_update_config_swaps_active() {
    let p = PrecomputeImpl::new(None, None, None);
    let cs = PrecomputeConfigSet {
        version: 1,
        configs: vec![PrecomputeConfig {
            agg_id: 7,
            sketch_type: SketchType::DDSketch,
            ..Default::default()
        }],
    };
    p.update_config(&cs);
    // stats() returns the empty snapshot — runtime fields
    // remain zero until Phase 3 step 2 wires them up.
    let s = p.stats();
    assert_eq!(s.input_observations, 0);
}

#[test]
fn frequency_entry_default_is_empty() {
    let e = FrequencyEntry::default();
    assert!(e.key.is_empty());
    assert_eq!(e.count, 0.0);
}
