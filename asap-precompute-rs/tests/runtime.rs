//! State-machine integration tests for `asap-precompute-rs`.
//!
//! These tests use a fake `Sketch` so they can exercise the runtime
//! without depending on real sketch wrappers.
//!
//! Coverage:
//!   - `series_key` byte-equivalence (golden literal strings).
//!   - `SnapshotCache::compute_delta` always-refresh policy.
//!   - End-to-end observe → tick cycle.
//!   - `Drain` rotates partial-window state.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use asap_precompute_rs::config::WindowSpec;
use asap_precompute_rs::matchers::{attributes_key, series_key};
use asap_precompute_rs::precompute::{
    DeltaResult, Precompute, PrecomputeError, PrecomputeImpl, Sketch, SketchObserver,
};
use asap_precompute_rs::snapshot_cache::SnapshotCache;
use asap_precompute_rs::{
    AggregationMode, Encoding, KeyValue, Observation, ObservationValue, PrecomputeConfig,
    PrecomputeConfigSet, SketchType,
};

// ----------------------------------------------------------------
// Fake Sketch + SketchObserver test doubles. The byte-level test
// fixtures (delta:"<state>" prefix) are pinned so they stay identical.

#[derive(Clone, Default)]
struct FakeSketchInner {
    state: Vec<u8>,
    delta_force: bool,
}

#[derive(Clone, Default)]
struct FakeSketch {
    inner: Arc<Mutex<FakeSketchInner>>,
}

impl FakeSketch {
    fn new() -> Self {
        Self::default()
    }

    fn with_state(state: &[u8]) -> Self {
        Self {
            inner: Arc::new(Mutex::new(FakeSketchInner {
                state: state.to_vec(),
                delta_force: false,
            })),
        }
    }

    fn set_state(&self, state: &[u8]) {
        self.inner.lock().unwrap().state = state.to_vec();
    }

    fn force_full_next(&self) {
        self.inner.lock().unwrap().delta_force = true;
    }
}

impl Sketch for FakeSketch {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        Ok(self.inner.lock().unwrap().state.clone())
    }

    fn compute_delta_against(
        &self,
        prev: &[u8],
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        let inner = self.inner.lock().unwrap();
        if inner.delta_force {
            return Ok(DeltaResult {
                payload: inner.state.clone(),
                is_full: true,
            });
        }
        // delta = "delta:" + state (ignoring prev for the fake).
        let _ = prev;
        let mut delta = Vec::with_capacity(inner.state.len() + 6);
        delta.extend_from_slice(b"delta:");
        delta.extend_from_slice(&inner.state);
        if delta.len() as u64 > threshold {
            return Ok(DeltaResult {
                payload: inner.state.clone(),
                is_full: true,
            });
        }
        Ok(DeltaResult {
            payload: delta,
            is_full: false,
        })
    }

    fn apply_delta(&mut self, delta: &[u8]) -> Result<(), PrecomputeError> {
        self.inner.lock().unwrap().state.extend_from_slice(delta);
        Ok(())
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        // Snapshot the other sketch and concatenate.
        let other_state = other.snapshot()?;
        self.inner
            .lock()
            .unwrap()
            .state
            .extend_from_slice(&other_state);
        Ok(())
    }

    fn reset(&mut self) {
        self.inner.lock().unwrap().state.clear();
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

struct FakeObserver;

impl SketchObserver for FakeObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        // Append a tag byte per value kind.
        let tag: &[u8] = match obs.value.kind {
            asap_precompute_rs::ObservationValueKind::Float => b"f",
            asap_precompute_rs::ObservationValueKind::Hash => b"h",
            asap_precompute_rs::ObservationValueKind::Bytes => &obs.value.bytes,
            asap_precompute_rs::ObservationValueKind::Envelope => b"",
        };
        sketch.apply_delta(tag)
    }
}

fn make_factory() -> Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> {
    Box::new(|| Box::new(FakeSketch::new()) as Box<dyn Sketch>)
}

// ----------------------------------------------------------------
// 1. SeriesKey byte-equivalence.
//
// Hardcoded golden values pin the series-key output.
//
// Format: "<aggID>|<resourceAttrs>|<dpAttrs>".

#[test]
fn series_key_no_aggregate_by_full_set() {
    // Fixture 1: aggID=42, resAttrs={service.name=web,host.name=node-1},
    // dpAttrs={http.method=GET,http.status=200}, no aggregate_by.
    // Sorted keys: host.name<service.name, http.method<http.status.
    let res = vec![
        KeyValue::new("host.name", "node-1"),
        KeyValue::new("service.name", "web"),
    ];
    let dp = vec![
        KeyValue::new("http.method", "GET"),
        KeyValue::new("http.status", "200"),
    ];
    let got = series_key(42, &res, &dp, &[]);
    assert_eq!(
        got,
        "42|host.name=node-1;service.name=web;|http.method=GET;http.status=200;"
    );
}

#[test]
fn series_key_with_aggregate_by_filters_keys() {
    // Fixture 2: aggregate_by filters dp keys; resource always
    // included sorted.
    let res = vec![
        KeyValue::new("region", "us-east"),
        KeyValue::new("service.name", "api"),
    ];
    let dp = vec![
        KeyValue::new("http.method", "POST"),
        KeyValue::new("http.status", "500"),
        KeyValue::new("k8s.pod", "p-1"),
    ];
    let got = series_key(
        7,
        &res,
        &dp,
        &["http.method".to_string(), "http.status".to_string()],
    );
    assert_eq!(
        got,
        "7|region=us-east;service.name=api;|http.method=POST;http.status=500;"
    );
}

#[test]
fn series_key_aggregate_by_with_missing_keys_skipped() {
    // Fixture 3: aggregate_by lists "b" but it's missing → skip.
    let res = vec![KeyValue::new("service.name", "ingest")];
    let dp = vec![KeyValue::new("a", "1"), KeyValue::new("c", "3")];
    let got = series_key(
        1,
        &res,
        &dp,
        &["a".to_string(), "b".to_string(), "c".to_string()],
    );
    assert_eq!(got, "1|service.name=ingest;|a=1;c=3;");
}

#[test]
fn series_key_empty_dp_attrs() {
    // Fixture 4.
    let res = vec![KeyValue::new("deployment.environment", "prod")];
    let got = series_key(99, &res, &[], &[]);
    assert_eq!(got, "99|deployment.environment=prod;|");
}

#[test]
fn series_key_empty_resource_attrs() {
    // Fixture 5.
    let dp = vec![KeyValue::new("x", "1"), KeyValue::new("y", "2")];
    let got = series_key(3, &[], &dp, &[]);
    assert_eq!(got, "3||x=1;y=2;");
}

#[test]
fn attributes_key_sorts_by_key() {
    // attributes_key is the per-segment building block. Without
    // aggregate_by, output is sorted by key.
    let labels = vec![
        KeyValue::new("z", "1"),
        KeyValue::new("a", "2"),
        KeyValue::new("m", "3"),
    ];
    let got = attributes_key(&labels, &[]);
    assert_eq!(got, "a=2;m=3;z=1;");
}

// ----------------------------------------------------------------
// 2. SnapshotCache always-refresh policy.

#[test]
fn snapshot_cache_first_call_returns_full() {
    let c = SnapshotCache::new();
    let s = FakeSketch::with_state(b"v1");
    let r = c.compute_delta("k1", &s, 1024).expect("compute");
    assert!(r.is_full);
    assert_eq!(r.payload, b"v1");
    // Cache populated.
    assert_eq!(c.get_outbound("k1").as_deref(), Some(b"v1".as_ref()));
}

#[test]
fn snapshot_cache_subsequent_returns_delta_and_refreshes() {
    let c = SnapshotCache::new();
    let s = FakeSketch::with_state(b"v1");
    c.compute_delta("k1", &s, 1024).expect("first");
    s.set_state(b"v1v2");
    let r = c.compute_delta("k1", &s, 1024).expect("second");
    assert!(!r.is_full);
    assert_eq!(r.payload, b"delta:v1v2");
    // Always-refresh: cache advances to the latest full state.
    assert_eq!(c.get_outbound("k1").as_deref(), Some(b"v1v2".as_ref()));
}

#[test]
fn snapshot_cache_always_refresh_chain() {
    // Each delta is computed against the immediately preceding
    // window's state — not the original baseline.
    let c = SnapshotCache::new();
    let s = FakeSketch::with_state(b"a");
    c.compute_delta("k", &s, 1024).expect("w0");
    assert_eq!(c.get_outbound("k").as_deref(), Some(b"a".as_ref()));

    s.set_state(b"ab");
    c.compute_delta("k", &s, 1024).expect("w1");
    assert_eq!(c.get_outbound("k").as_deref(), Some(b"ab".as_ref()));

    s.set_state(b"abc");
    c.compute_delta("k", &s, 1024).expect("w2");
    assert_eq!(c.get_outbound("k").as_deref(), Some(b"abc".as_ref()));
}

#[test]
fn snapshot_cache_above_threshold_refreshes() {
    let c = SnapshotCache::new();
    let s = FakeSketch::with_state(b"v1");
    c.compute_delta("k1", &s, 1024).expect("first");
    s.set_state(b"BIG");
    s.force_full_next();
    let r = c.compute_delta("k1", &s, 1024).expect("forced");
    assert!(r.is_full);
    assert_eq!(r.payload, b"BIG");
    assert_eq!(c.get_outbound("k1").as_deref(), Some(b"BIG".as_ref()));
}

// ----------------------------------------------------------------
// 3. Observe → tick cycle produces expected envelope count + shape.

#[test]
fn observe_then_tick_emits_one_envelope_per_series() {
    let cfg = PrecomputeConfig {
        agg_id: 42,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    let obs = |label: &str, ts: u64| Observation {
        timestamp_ms: ts,
        metric: "http.requests".into(),
        resource_labels: vec![KeyValue::new("service.name", "web")],
        labels: vec![KeyValue::new("method", label)],
        value: ObservationValue::float(1.0),
    };

    p.observe(&obs("GET", 1_000)).expect("o1");
    p.observe(&obs("GET", 2_000)).expect("o2");
    p.observe(&obs("POST", 3_000)).expect("o3");

    let envelopes = p.tick(10_000);
    assert_eq!(envelopes.len(), 2);
    for env in &envelopes {
        assert_eq!(env.agg_id, 42);
        assert_eq!(env.sketch_type, SketchType::DDSketch);
        assert_eq!(env.window_start_ms, 0);
        assert_eq!(env.window_end_ms, 10_000);
        assert_eq!(env.resource_labels.len(), 1);
        assert_eq!(env.resource_labels[0].value, "web");
        assert!(!env.payload.is_empty());
    }

    // Second tick before next boundary is a no-op.
    let again = p.tick(15_000);
    assert!(again.is_empty());
}

#[test]
fn observe_on_no_config_returns_no_config_error() {
    let p = PrecomputeImpl::new(None, Some(make_factory()), Some(Box::new(FakeObserver)));
    let obs = Observation {
        timestamp_ms: 1_000,
        metric: "m".into(),
        value: ObservationValue::float(1.0),
        ..Default::default()
    };
    let err = p.observe(&obs).unwrap_err();
    assert!(matches!(err, PrecomputeError::NoConfig));
}

#[test]
fn stats_account_observations() {
    let cfg = PrecomputeConfig {
        agg_id: 1,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );
    for _ in 0..5 {
        let _ = p.observe(&Observation {
            timestamp_ms: 1_000,
            metric: "m".into(),
            value: ObservationValue::float(1.0),
            ..Default::default()
        });
    }
    let s = p.stats();
    assert_eq!(s.input_observations, 5);
    assert_eq!(s.active_series, 1);
}

#[test]
fn delta_transmission_first_full_then_delta() {
    let cfg = PrecomputeConfig {
        agg_id: 1,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        delta_transmission: true,
        delta_threshold: 1024,
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    let obs = |ts: u64| Observation {
        timestamp_ms: ts,
        metric: "m".into(),
        labels: vec![KeyValue::new("k", "a")],
        value: ObservationValue::float(1.0),
        ..Default::default()
    };

    // Window 0 — first emission is full.
    p.observe(&obs(1_000)).expect("o1");
    let envs = p.tick(10_000);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].encoding, Encoding::ProtoFull);

    // Window 1 — small delta.
    p.observe(&obs(11_000)).expect("o2");
    let envs = p.tick(20_000);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].encoding, Encoding::ProtoDelta);
}

// ----------------------------------------------------------------
// 4. Drain rotates partial-window state.

#[test]
fn drain_flushes_mid_window_observations() {
    let cfg = PrecomputeConfig {
        agg_id: 7,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    for i in 0..3 {
        p.observe(&Observation {
            timestamp_ms: 1_000 + i as u64,
            metric: "m".into(),
            labels: vec![KeyValue::new("k", "a")],
            value: ObservationValue::float(1.0),
            ..Default::default()
        })
        .expect("observe");
    }

    // Mid-window Tick is a no-op: now_ms < active_end_ms.
    let no_envs = p.tick(2_000);
    assert!(no_envs.is_empty());

    // Drain flushes regardless of wall-clock time.
    let envs = p.drain();
    assert_eq!(envs.len(), 1);
    let env = &envs[0];
    assert_eq!(env.agg_id, 7);
    assert_eq!(env.window_start_ms, 0);
    assert_eq!(env.window_end_ms, 10_000);
    assert_eq!(env.count, 3);
    assert!(!env.payload.is_empty());

    // Second Drain on empty window is a no-op.
    let again = p.drain();
    assert!(again.is_empty());

    // After Drain, next active window starts where natural rotation
    // would have used (10_000), so mid-window Tick at 15_000 still
    // no-ops...
    p.observe(&Observation {
        timestamp_ms: 11_000,
        metric: "m".into(),
        labels: vec![KeyValue::new("k", "a")],
        value: ObservationValue::float(1.0),
        ..Default::default()
    })
    .expect("post-drain observe");
    let mid = p.tick(15_000);
    assert!(mid.is_empty());

    // ...but Tick at 20_000 (>= new active_end_ms) flushes window 1.
    let envs = p.tick(20_000);
    assert_eq!(envs.len(), 1);
    assert_eq!(envs[0].window_start_ms, 10_000);
    assert_eq!(envs[0].window_end_ms, 20_000);
}

#[test]
fn drain_equals_tick_at_boundary() {
    // Drain emits the same shape Tick would emit if called precisely
    // at active_end_ms. Pins "Drain is the shutdown twin of Tick".
    let mk = || {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::DDSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        PrecomputeImpl::new(
            Some(cfg),
            Some(make_factory()),
            Some(Box::new(FakeObserver)),
        )
    };
    let feed = |p: &PrecomputeImpl| {
        for i in 0..5 {
            let _ = p.observe(&Observation {
                timestamp_ms: 1_000 + i as u64,
                metric: "m".into(),
                labels: vec![KeyValue::new("k", "a")],
                value: ObservationValue::float((i + 1) as f64),
                ..Default::default()
            });
        }
    };

    let p_tick = mk();
    feed(&p_tick);
    let tick_envs = p_tick.tick(10_000);

    let p_drain = mk();
    feed(&p_drain);
    let drain_envs = p_drain.drain();

    assert_eq!(tick_envs.len(), drain_envs.len());
    for (te, de) in tick_envs.iter().zip(drain_envs.iter()) {
        assert_eq!(te.window_start_ms, de.window_start_ms);
        assert_eq!(te.window_end_ms, de.window_end_ms);
        assert_eq!(te.count, de.count);
        assert_eq!(te.payload, de.payload);
    }
}

// ----------------------------------------------------------------
// Envelope-merge inbound path.

#[test]
fn observe_envelope_merges_into_active_window() {
    let cfg = PrecomputeConfig {
        agg_id: 5,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    let env = asap_precompute_rs::SketchEnvelope {
        schema_version: 1,
        sketch_type: SketchType::DDSketch,
        agg_id: 5,
        labels: vec![KeyValue::new("k", "a")],
        window_start_ms: 0,
        window_end_ms: 10_000,
        encoding: Encoding::ProtoFull,
        payload: b"upstream-state".to_vec(),
        count: 17,
        ..Default::default()
    };
    p.observe_envelope(&env).expect("merge");

    // The envelope's window_end_ms=10_000 is the ref ts that
    // initializes the local active window to [10_000, 20_000), so
    // tick(20_000) is the boundary that flushes it.
    let out = p.tick(20_000);
    assert_eq!(out.len(), 1);
    let got = &out[0];
    assert_eq!(got.agg_id, 5);
    // entry.count carries the upstream envelope count.
    assert_eq!(got.count, 17);
}

#[test]
fn observe_envelope_rejects_agg_id_mismatch() {
    let cfg = PrecomputeConfig {
        agg_id: 5,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    let env = asap_precompute_rs::SketchEnvelope {
        schema_version: 1,
        sketch_type: SketchType::DDSketch,
        agg_id: 99,
        encoding: Encoding::ProtoFull,
        payload: b"x".to_vec(),
        ..Default::default()
    };
    let err = p.observe_envelope(&env).unwrap_err();
    assert!(matches!(err, PrecomputeError::AggIdMismatch { .. }));
}

// ----------------------------------------------------------------
// emit_window_stats appends the two operator-visibility attrs.

#[test]
fn emit_window_stats_adds_sample_count_and_window_duration() {
    let cfg = PrecomputeConfig {
        agg_id: 1,
        sketch_type: SketchType::CountSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(30),
            ..Default::default()
        },
        emit_window_stats: true,
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    for i in 0..4 {
        p.observe(&Observation {
            timestamp_ms: 1_000 + i as u64,
            metric: "m".into(),
            labels: vec![KeyValue::new("k", "a")],
            value: ObservationValue::float(1.0),
            ..Default::default()
        })
        .expect("observe");
    }
    let envs = p.tick(30_000);
    assert_eq!(envs.len(), 1);
    let labels = &envs[0].labels;
    let has = |k: &str, v: &str| labels.iter().any(|kv| kv.key == k && kv.value == v);
    assert!(has("k", "a"));
    assert!(has("sample_count", "4"));
    assert!(has("window_duration_seconds", "30"));
}

// ----------------------------------------------------------------
// Late-data and overflow.

#[test]
fn late_data_returns_late_error() {
    let cfg = PrecomputeConfig {
        agg_id: 1,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            allowed_lateness: Duration::from_secs(1),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    // Initialize at t=10500ms (active_start=10000).
    p.observe(&Observation {
        timestamp_ms: 10_500,
        metric: "m".into(),
        value: ObservationValue::float(1.0),
        ..Default::default()
    })
    .expect("first");

    // Observation at t=8000ms is >1s before active_start=10000.
    let err = p
        .observe(&Observation {
            timestamp_ms: 8_000,
            metric: "m".into(),
            value: ObservationValue::float(1.0),
            ..Default::default()
        })
        .unwrap_err();
    assert!(matches!(err, PrecomputeError::LateData));
    let s = p.stats();
    assert_eq!(s.dropped_late, 1);
}

#[test]
fn max_series_drop_returns_overflow() {
    let cfg = PrecomputeConfig {
        agg_id: 1,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        max_series: 1,
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(cfg),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    p.observe(&Observation {
        timestamp_ms: 100,
        metric: "m".into(),
        labels: vec![KeyValue::new("k", "a")],
        value: ObservationValue::float(1.0),
        ..Default::default()
    })
    .expect("first");
    let err = p
        .observe(&Observation {
            timestamp_ms: 200,
            metric: "m".into(),
            labels: vec![KeyValue::new("k", "b")],
            value: ObservationValue::float(1.0),
            ..Default::default()
        })
        .unwrap_err();
    assert!(matches!(err, PrecomputeError::SeriesCapExceeded));
    let s = p.stats();
    assert_eq!(s.dropped_overflow, 1);
}

// ----------------------------------------------------------------
// update_config swap preserves in-flight window.

#[test]
fn update_config_swaps_active() {
    let initial = PrecomputeConfig {
        agg_id: 1,
        sketch_type: SketchType::DDSketch,
        mode: AggregationMode::Tumbling,
        window: WindowSpec {
            size: Duration::from_secs(10),
            ..Default::default()
        },
        ..Default::default()
    };
    let p = PrecomputeImpl::new(
        Some(initial),
        Some(make_factory()),
        Some(Box::new(FakeObserver)),
    );

    let cs = PrecomputeConfigSet {
        version: 2,
        configs: vec![PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::KLLSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(5),
                ..Default::default()
            },
            ..Default::default()
        }],
    };
    p.update_config(&cs);
    // No way to read back active config from the public API — but
    // observe() should still succeed (sketch_type is informational
    // here).
    p.observe(&Observation {
        timestamp_ms: 1,
        metric: "m".into(),
        value: ObservationValue::float(1.0),
        ..Default::default()
    })
    .expect("observe under new config");
}

// ----------------------------------------------------------------
// 8. Real-sketch wrappers (asap_sketchlib-backed) drive the runtime
// end-to-end. One test per wrapper exercising
// observe → tick → envelope output.

mod real_sketch {
    use super::*;
    use asap_precompute_rs::sketches::{
        CMSObserver, CMSWrapper, CountSketchObserver, CountSketchWrapper, DDSketchObserver,
        DDSketchWrapper, HLLObserver, HLLWrapper, KLLObserver, KLLWrapper,
    };

    fn ddsketch_factory() -> Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> {
        Box::new(|| Box::new(DDSketchWrapper::new(0.01)) as Box<dyn Sketch>)
    }

    fn kll_factory() -> Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> {
        // Deterministic seed for byte-reproducible tests.
        Box::new(|| Box::new(KLLWrapper::new(200, Some(0xDEAD_BEEF))) as Box<dyn Sketch>)
    }

    fn hll_factory() -> Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> {
        Box::new(|| {
            Box::new(HLLWrapper::new(asap_sketchlib::HllVariant::Regular, 12)) as Box<dyn Sketch>
        })
    }

    fn count_sketch_factory() -> Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> {
        Box::new(|| Box::new(CountSketchWrapper::new(4, 32)) as Box<dyn Sketch>)
    }

    fn cms_factory() -> Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> {
        Box::new(|| Box::new(CMSWrapper::new(4, 32)) as Box<dyn Sketch>)
    }

    fn float_obs(metric: &str, ts: u64, label_v: &str, val: f64) -> Observation {
        Observation {
            timestamp_ms: ts,
            metric: metric.into(),
            resource_labels: vec![KeyValue::new("service.name", "test")],
            labels: vec![KeyValue::new("k", label_v)],
            value: ObservationValue::float(val),
        }
    }

    fn bytes_obs(metric: &str, ts: u64, label_v: &str, key: &[u8]) -> Observation {
        Observation {
            timestamp_ms: ts,
            metric: metric.into(),
            resource_labels: vec![KeyValue::new("service.name", "test")],
            labels: vec![KeyValue::new("k", label_v)],
            value: ObservationValue::bytes(key.to_vec()),
        }
    }

    #[test]
    fn ddsketch_wrapper_observe_tick_emits_envelope() {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::DDSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let p = PrecomputeImpl::new(
            Some(cfg),
            Some(ddsketch_factory()),
            Some(Box::new(DDSketchObserver)),
        );
        for i in 1..=20 {
            p.observe(&float_obs("latency_ms", 1_000, "GET", i as f64))
                .expect("observe");
        }
        let envs = p.tick(10_000);
        assert_eq!(envs.len(), 1);
        let env = &envs[0];
        assert_eq!(env.sketch_type, SketchType::DDSketch);
        assert_eq!(env.encoding, Encoding::ProtoFull);
        assert!(!env.payload.is_empty());
    }

    #[test]
    fn ddsketch_msgpack_delta_first_full_then_delta() {
        // End-to-end Stage-3 wiring: a msgpack-configured DDSketch emits a
        // full `Msgpack` frame on the first window and a sparse
        // `MsgpackDelta` frame on the next, each reconstructable via
        // sketchlib's msgpack codec.
        use asap_sketchlib::MessagePackCodec;

        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::DDSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            delta_transmission: true,
            delta_threshold: 1,
            encoding: Encoding::MsgpackDelta,
            ..Default::default()
        };
        let factory: Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> = Box::new(|| {
            Box::new(DDSketchWrapper::new(0.01).with_wire_encoding(Encoding::MsgpackDelta))
                as Box<dyn Sketch>
        });
        let p = PrecomputeImpl::new(Some(cfg), Some(factory), Some(Box::new(DDSketchObserver)));

        // Window 0 — first emission is a full msgpack frame.
        for i in 1..=50 {
            p.observe(&float_obs("latency_ms", 1_000, "GET", i as f64))
                .expect("observe");
        }
        let envs = p.tick(10_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].encoding, Encoding::Msgpack);
        let full = asap_sketchlib::DdSketch::from_msgpack(&envs[0].payload).expect("decode full");
        assert_eq!(full.total_count(), 50);

        // Window 1 — a sparse msgpack delta frame; applying it to an empty
        // base reconstructs this window's own count (no cross-window leak).
        for i in 1..=50 {
            p.observe(&float_obs("latency_ms", 11_000, "GET", i as f64))
                .expect("observe");
        }
        let envs = p.tick(20_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].encoding, Encoding::MsgpackDelta);
        assert!(!envs[0].payload.is_empty());
        let mut recon = asap_sketchlib::DdSketch::new(0.01);
        recon
            .apply_delta_msgpack_bytes(&envs[0].payload)
            .expect("apply msgpack delta");
        assert_eq!(recon.total_count(), 50);
    }

    #[test]
    fn cms_msgpack_full_roundtrips_through_ingest() {
        // A msgpack full frame emitted by one node is re-ingested by a
        // second (envelope-input) node via observe_envelope → the
        // encoding-aware full-merge path, and the merged estimate matches.
        use asap_sketchlib::MessagePackCodec;

        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::CountMinSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            encoding: Encoding::Msgpack,
            ..Default::default()
        };
        let factory: Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> = Box::new(|| {
            Box::new(CMSWrapper::new(4, 32).with_wire_encoding(Encoding::Msgpack))
                as Box<dyn Sketch>
        });
        let producer = PrecomputeImpl::new(
            Some(cfg.clone()),
            Some(factory),
            Some(Box::new(CMSObserver)),
        );
        for _ in 0..100 {
            producer
                .observe(&bytes_obs("events", 1_000, "hot", b"hot"))
                .expect("observe");
        }
        let envs = producer.tick(10_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].encoding, Encoding::Msgpack);

        // Second node ingests the emitted msgpack envelope and re-emits.
        let factory2: Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync> = Box::new(|| {
            Box::new(CMSWrapper::new(4, 32).with_wire_encoding(Encoding::Msgpack))
                as Box<dyn Sketch>
        });
        let merger = PrecomputeImpl::new(Some(cfg), Some(factory2), Some(Box::new(CMSObserver)));
        merger.observe_envelope(&envs[0]).expect("observe_envelope");
        let out = merger.tick(20_000);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].encoding, Encoding::Msgpack);
        let merged =
            asap_sketchlib::CountMinSketch::from_msgpack(&out[0].payload).expect("decode merged");
        assert!(merged.estimate("hot") >= 90.0);
    }

    #[test]
    fn kll_wrapper_observe_tick_emits_envelope() {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::KLLSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let p = PrecomputeImpl::new(Some(cfg), Some(kll_factory()), Some(Box::new(KLLObserver)));
        for i in 1..=10 {
            p.observe(&float_obs("latency_ms", 1_000, "GET", i as f64))
                .expect("observe");
        }
        let envs = p.tick(10_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].sketch_type, SketchType::KLLSketch);
        assert_eq!(envs[0].encoding, Encoding::ProtoFull);
        assert!(!envs[0].payload.is_empty());
    }

    #[test]
    fn hll_wrapper_observe_tick_emits_envelope() {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::HLLSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let p = PrecomputeImpl::new(Some(cfg), Some(hll_factory()), Some(Box::new(HLLObserver)));
        for i in 0..50u64 {
            p.observe(&bytes_obs("unique_users", 1_000, "ip", &i.to_le_bytes()))
                .expect("observe");
        }
        let envs = p.tick(10_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].sketch_type, SketchType::HLLSketch);
        assert!(!envs[0].payload.is_empty());
    }

    #[test]
    fn countsketch_wrapper_observe_tick_emits_envelope() {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::CountSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let p = PrecomputeImpl::new(
            Some(cfg),
            Some(count_sketch_factory()),
            Some(Box::new(CountSketchObserver {
                default_key: "default".into(),
            })),
        );
        // CountSketchObserver routes via Float-kind with bytes-payload
        // as the keying input. Build observations with bytes carrying
        // the key.
        for i in 0..20 {
            let key = format!("k-{}", i % 5);
            p.observe(&Observation {
                timestamp_ms: 1_000,
                metric: "events".into(),
                resource_labels: vec![KeyValue::new("service.name", "test")],
                labels: vec![KeyValue::new("k", "tag")],
                value: ObservationValue {
                    kind: asap_precompute_rs::ObservationValueKind::Float,
                    float: 1.0,
                    bytes: key.into_bytes(),
                    ..Default::default()
                },
            })
            .expect("observe");
        }
        let envs = p.tick(10_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].sketch_type, SketchType::CountSketch);
        assert!(!envs[0].payload.is_empty());
    }

    #[test]
    fn cms_wrapper_observe_tick_emits_envelope() {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::CountMinSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let p = PrecomputeImpl::new(Some(cfg), Some(cms_factory()), Some(Box::new(CMSObserver)));
        for i in 0..20 {
            let key = format!("flow-{}", i % 4);
            p.observe(&bytes_obs("flows", 1_000, "tag", key.as_bytes()))
                .expect("observe");
        }
        let envs = p.tick(10_000);
        assert_eq!(envs.len(), 1);
        assert_eq!(envs[0].sketch_type, SketchType::CountMinSketch);
        assert!(!envs[0].payload.is_empty());
    }
}
