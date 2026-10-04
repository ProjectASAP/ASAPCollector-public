//! Pane merging, expiration, and aligned rolling KLL semantics.
use asap_precompute_rs::kll_windows::{KllRollingPrecompute, KllWindowStore};
use asap_precompute_rs::precompute::PrecomputeError;
use asap_precompute_rs::{
    AggregationMode, Encoding, KeyValue, Observation, ObservationValue, Precompute,
    PrecomputeConfig, PrecomputeConfigSet, SketchType, WindowSpec,
};
use std::time::Duration;

fn config() -> PrecomputeConfig {
    PrecomputeConfig {
        sketch_type: SketchType::KLLSketch,
        encoding: Encoding::Msgpack,
        mode: AggregationMode::Sliding,
        transmit_sketch: false,
        metric_name: "request.duration.rolling_estimate".into(),
        quantiles: vec![0.5, 0.99],
        window: WindowSpec {
            size: Duration::from_millis(30),
            slide: Duration::from_millis(10),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn observation(time: u64, value: f64, source: &str) -> Observation {
    Observation::new(
        time,
        "request.duration",
        vec![KeyValue::new("service.name", source)],
        vec![KeyValue::new("route", "/checkout")],
        ObservationValue::float(value),
    )
}
fn store() -> KllWindowStore {
    KllWindowStore::new(
        config(),
        Duration::from_millis(10),
        Duration::from_millis(30),
    )
    .unwrap()
}

#[test]
fn aligned_ranges_merge_tumbling_panes_without_consuming_them() {
    let mut s = store();
    for (time, value) in [(1, 1.), (9, 2.), (10, 3.), (20, 4.)] {
        s.observe(&observation(time, value, "checkout")).unwrap();
    }
    s.advance(30);
    let all = s.query(0, 30).unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].count, 4);
    assert!(all[0].payload.starts_with(b"ASAPv1"));
    assert_eq!((all[0].window_start_ms, all[0].window_end_ms), (0, 30));
    assert_eq!(s.query(10, 30).unwrap()[0].count, 2);
    assert_eq!(s.query(0, 30).unwrap()[0].count, 4);
    assert!(s.query(1, 30).is_err());
    assert!(s.query(0, 40).is_err());
    assert!(s.query(10, 10).is_err());
}

#[test]
fn upstream_sources_merge_by_pane_and_preserve_series_identity() {
    let mut receiver = store();
    for (value, source) in [(1., "checkout"), (3., "checkout"), (100., "other")] {
        let mut upstream = store();
        upstream.observe(&observation(1, value, source)).unwrap();
        upstream.advance(10);
        receiver
            .merge_pane(&upstream.query(0, 10).unwrap()[0])
            .unwrap();
    }
    let merged = receiver.query(0, 10).unwrap();
    assert_eq!(merged.len(), 2);
    assert_eq!(
        merged
            .iter()
            .find(|e| e.resource_labels[0].value == "checkout")
            .unwrap()
            .count,
        2
    );
    assert_eq!(
        merged
            .iter()
            .find(|e| e.resource_labels[0].value == "other")
            .unwrap()
            .count,
        1
    );
    let mut wide = merged[0].clone();
    wide.window_end_ms = 20;
    assert!(receiver.merge_pane(&wide).is_err());
    let mut malformed = merged[0].clone();
    malformed.payload = vec![1, 2, 3];
    malformed.window_start_ms = 100;
    malformed.window_end_ms = 110;
    assert!(receiver.merge_pane(&malformed).is_err());
    // A corrupt future frame did not evict retained observations.
    assert_eq!(receiver.query(0, 10).unwrap().len(), 2);
}

#[test]
fn expiration_boundaries_idle_gaps_and_late_observations() {
    let mut s = store();
    s.observe(&observation(1, 1., "checkout")).unwrap();
    s.observe(&observation(10, 2., "checkout")).unwrap();
    s.advance(30);
    assert_eq!(s.query(0, 30).unwrap()[0].count, 2);
    assert!(matches!(
        s.observe(&observation(29, 3., "checkout")),
        Err(PrecomputeError::LateData)
    ));
    s.advance(40);
    assert!(s.query(0, 30).is_err());
    assert_eq!(s.query(10, 40).unwrap()[0].count, 1);
    s.advance(1000);
    assert_eq!(s.retained_panes(), 0);
    assert!(s.query(970, 1000).unwrap().is_empty());
    // Backward wall-clock motion cannot reopen expired panes.
    s.advance(20);
    assert!(s.query(10, 20).is_err());
}

#[test]
fn sliding_outputs_use_union_of_observations_and_evict_old_panes() {
    let p = KllRollingPrecompute::new(config()).unwrap();
    for (time, value) in [(1, 1.), (11, 2.), (21, 3.)] {
        p.observe(&observation(time, value, "checkout")).unwrap();
    }
    let first = p.tick(30);
    assert_eq!(first.len(), 2);
    assert!(first
        .iter()
        .all(|e| e.count == 3 && e.window_start_ms == 0 && e.window_end_ms == 30));
    assert_eq!(first[0].value, 2.);
    assert!(p.tick(35).is_empty());
    p.observe(&observation(31, 100., "checkout")).unwrap();
    let second = p.tick(40);
    assert_eq!(second.len(), 2);
    assert_eq!(second[0].value, 3.);
    assert_eq!(
        (second[0].window_start_ms, second[0].window_end_ms),
        (10, 40)
    );
    assert_eq!(second[1].value, 100.);
    assert!(p.tick(100).is_empty());
    assert!(p.drain().is_empty());
    p.shutdown().unwrap();
    assert!(p.observe(&observation(101, 1., "checkout")).is_err());
}

#[test]
fn many_panes_stay_bounded_and_unequal_compacted_panes_keep_weights() {
    let mut s = store();
    // Force compaction. One large pane and one tiny high-value pane make an
    // unweighted merge or averaging pane quantiles visibly incorrect.
    for value in 0..20_000 {
        s.observe(&observation(1, value as f64, "checkout"))
            .unwrap();
    }
    s.observe(&observation(11, 1_000_000., "checkout")).unwrap();
    s.advance(20);
    let estimated = s.estimate(0, 20).unwrap();
    assert_eq!(estimated[0].count, 20_001);
    assert!((estimated[0].value - 10_000.).abs() < 800.);
    assert!((estimated[1].value - 19_800.).abs() < 800.);
    assert!(s.query(0, 20).unwrap()[0].payload.len() < 20_000);
    let mut downstream = store();
    downstream.merge_pane(&s.query(0, 10).unwrap()[0]).unwrap();
    assert_eq!(downstream.query(0, 10).unwrap()[0].count, 20_000);
    for pane in 2..1000 {
        s.observe(&observation(pane * 10 + 1, pane as f64, "checkout"))
            .unwrap();
        assert!(s.retained_panes() <= 4);
    }
}

#[test]
fn rolling_can_consume_upstream_panes_and_resets_on_window_change() {
    let p = KllRollingPrecompute::new(config()).unwrap();
    let mut upstream = store();
    upstream.observe(&observation(1, 42., "checkout")).unwrap();
    upstream.advance(10);
    p.observe_envelope(&upstream.query(0, 10).unwrap()[0])
        .unwrap();
    assert_eq!(p.tick(30)[0].value, 42.);
    let mut next = config();
    next.window.size = Duration::from_millis(20);
    p.update_config(&PrecomputeConfigSet {
        version: 1,
        configs: vec![next],
    });
    assert!(p.tick(40).is_empty());
}

#[test]
fn invalid_windows_quantiles_and_nonfinite_values_are_rejected() {
    let mut cfg = config();
    cfg.window.size = Duration::from_millis(25);
    assert!(KllRollingPrecompute::new(cfg).is_err());
    let mut cfg = config();
    cfg.window.slide = Duration::ZERO;
    assert!(KllRollingPrecompute::new(cfg).is_err());
    let mut cfg = config();
    cfg.quantiles = vec![f64::NAN];
    assert!(KllRollingPrecompute::new(cfg).is_err());
    let mut cfg = config();
    cfg.sketch_type = SketchType::DDSketch;
    assert!(KllRollingPrecompute::new(cfg).is_err());
    let mut cfg = config();
    cfg.transmit_sketch = true;
    assert!(KllRollingPrecompute::new(cfg).is_err());
    let mut s = store();
    assert!(s
        .observe(&observation(1, f64::INFINITY, "checkout"))
        .is_err());
    assert_eq!(s.retained_panes(), 0);
}

#[test]
fn series_cap_applies_across_retained_panes() {
    let mut cfg = config();
    cfg.max_series = 1;
    let mut s =
        KllWindowStore::new(cfg, Duration::from_millis(10), Duration::from_millis(30)).unwrap();
    s.observe(&observation(1, 1., "checkout")).unwrap();
    assert!(matches!(
        s.observe(&observation(11, 2., "other")),
        Err(PrecomputeError::SeriesCapExceeded)
    ));
    s.advance(50);
    s.observe(&observation(51, 2., "other")).unwrap();
}

#[test]
fn upstream_timer_waits_for_new_complete_panes() {
    let p = KllRollingPrecompute::new(config()).unwrap();
    assert!(p.tick(30).is_empty());
    let mut upstream = store();
    upstream.observe(&observation(21, 42., "checkout")).unwrap();
    upstream.advance(30);
    p.observe_envelope(&upstream.query(20, 30).unwrap()[0])
        .unwrap();
    let first = p.tick(30);
    assert_eq!(first[0].window_end_ms, 30);
    assert_eq!(first[0].value, 42.);
    assert!(p.tick(40).is_empty());
    upstream
        .observe(&observation(31, 100., "checkout"))
        .unwrap();
    upstream.advance(40);
    p.observe_envelope(&upstream.query(30, 40).unwrap()[0])
        .unwrap();
    let next = p.tick(40);
    assert_eq!(next[0].count, 2);
    assert_eq!(next[0].window_end_ms, 40);
}

#[test]
fn disjoint_mode_transmits_full_panes_for_downstream_rolling_queries() {
    let mut cfg = config();
    cfg.window.size = cfg.window.slide;
    cfg.transmit_sketch = true;
    let creator = KllRollingPrecompute::new(cfg).unwrap();
    creator.observe(&observation(1, 42., "checkout")).unwrap();
    let pane = creator.tick(10);
    assert_eq!(pane.len(), 1);
    assert_eq!(pane[0].encoding, Encoding::Msgpack);
    let mut retained = store();
    retained.merge_pane(&pane[0]).unwrap();
    let answer = retained.quantile_over_time(0, 10, &[0.25, 0.75]).unwrap();
    assert_eq!(answer.len(), 2);
    assert!(answer.iter().all(|e| e.value == 42.));
    assert!(retained.quantile_over_time(0, 10, &[1.5]).is_err());
}

#[test]
fn small_high_value_pane_cannot_outweigh_a_large_compacted_low_value_pane() {
    let mut s = store();
    for _ in 0..20_000 {
        s.observe(&observation(1, 1., "checkout")).unwrap();
    }
    for _ in 0..10 {
        s.observe(&observation(11, 1_000_000., "checkout")).unwrap();
    }
    s.advance(20);
    let result = s.quantile_over_time(0, 20, &[0.5, 0.99]).unwrap();
    assert!(result.iter().all(|e| e.value == 1. && e.count == 20_010));
}
