//! Retained, mergeable KLL panes and aligned sliding-window estimates.
//!
//! A pane is a half-open tumbling interval. Range queries merge complete
//! sketches rather than quantiles or raw observations. Exact time selection
//! requires pane-aligned bounds; the quantile itself has KLL rank error.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use crate::config::{AggregationMode, PrecomputeConfig, PrecomputeConfigSet};
use crate::envelope::{Encoding, SketchEnvelope, SketchType};
use crate::matchers::series_attrs;
use crate::observation::{KeyValue, Observation, ObservationValueKind};
use crate::precompute::{Precompute, PrecomputeError, Sketch, StatsSnapshot};
use crate::sketches::KLLWrapper;

fn invalid(message: &str) -> PrecomputeError {
    PrecomputeError::Other(format!("KLL windows: {message}"))
}

struct PaneSeries {
    sketch: KLLWrapper,
    resource_labels: Vec<KeyValue>,
    labels: Vec<KeyValue>,
    count: u64,
}

/// In-memory KLL tumbling panes, retained for aligned time-range queries.
///
/// Each observation is inserted once. Closed panes can also be merged from
/// full ASAPv1 envelopes produced upstream. Delivery must be exactly once:
/// sending the same full pane twice would count its observations twice.
/// Empty panes consume no storage; advancing time expires old panes.
pub struct KllWindowStore {
    config: PrecomputeConfig,
    pane_ms: u64,
    retention_ms: u64,
    closed_until: u64,
    panes: BTreeMap<u64, HashMap<String, PaneSeries>>,
    k: i32,
    seed: Option<u64>,
}

impl KllWindowStore {
    /// Create a store. Retention must be a positive multiple of the pane size.
    /// Only KLL/ASAPv1 full sketches are accepted; no delta replay or partial
    /// pane splitting is supported.
    pub fn new(
        config: PrecomputeConfig,
        pane: Duration,
        retention: Duration,
    ) -> Result<Self, PrecomputeError> {
        let pane_ms =
            u64::try_from(pane.as_millis()).map_err(|_| invalid("pane duration overflow"))?;
        let retention_ms = u64::try_from(retention.as_millis())
            .map_err(|_| invalid("retention duration overflow"))?;
        if !pane.as_nanos().is_multiple_of(1_000_000)
            || !retention.as_nanos().is_multiple_of(1_000_000)
            || pane_ms == 0
            || retention_ms < pane_ms
            || !retention_ms.is_multiple_of(pane_ms)
        {
            return Err(invalid(
                "retention must be a positive multiple of a nonzero millisecond pane",
            ));
        }
        if config.sketch_type != SketchType::KLLSketch
            || config.encoding != Encoding::Msgpack
            || config.delta_transmission
        {
            return Err(invalid("requires KLL with full ASAPv1/Msgpack encoding"));
        }
        let k = config.sketch_params.get("k").copied().unwrap_or(200.0);
        if !k.is_finite() || k < 8.0 || k > i32::MAX as f64 || k.fract() != 0.0 {
            return Err(invalid("k must be an integer between 8 and i32::MAX"));
        }
        if config
            .quantiles
            .iter()
            .any(|q| !q.is_finite() || !(0.0..=1.0).contains(q))
        {
            return Err(invalid("quantiles must be finite values in [0, 1]"));
        }
        let seed = config
            .sketch_params
            .get("seed")
            .copied()
            .filter(|value| *value != 0.0)
            .map(f64::to_bits);
        Ok(Self {
            config,
            pane_ms,
            retention_ms,
            closed_until: 0,
            panes: BTreeMap::new(),
            k: k as i32,
            seed,
        })
    }

    /// Mark complete panes through `now_ms` and expire panes older than retention.
    /// The watermark never moves backward, including after a clock adjustment.
    pub fn advance(&mut self, now_ms: u64) {
        let boundary = now_ms / self.pane_ms * self.pane_ms;
        if boundary <= self.closed_until {
            return;
        }
        self.closed_until = boundary;
        let cutoff = self.closed_until.saturating_sub(self.retention_ms);
        self.panes = self.panes.split_off(&cutoff);
    }

    /// Number of nonempty panes currently retained (including the active pane).
    pub fn retained_panes(&self) -> usize {
        self.panes.len()
    }

    /// Number of distinct series across all retained panes.
    pub fn retained_series(&self) -> usize {
        self.panes
            .values()
            .flat_map(|pane| pane.keys())
            .collect::<HashSet<_>>()
            .len()
    }

    /// Insert a finite scalar into its event-time pane. Once the watermark has
    /// closed a pane, scalar observations for it are rejected as late.
    pub fn observe(&mut self, observation: &Observation) -> Result<(), PrecomputeError> {
        if !self.config.matches(observation) {
            return Ok(());
        }
        if observation.value.kind != ObservationValueKind::Float
            || !observation.value.float.is_finite()
        {
            return Err(invalid("expected a finite scalar observation"));
        }
        if observation.timestamp_ms < self.closed_until {
            return Err(PrecomputeError::LateData);
        }
        let start = observation.timestamp_ms / self.pane_ms * self.pane_ms;
        start
            .checked_add(self.pane_ms)
            .ok_or_else(|| invalid("pane end overflow"))?;
        self.advance(observation.timestamp_ms);
        let (resource, labels) = self.labels(&observation.resource_labels, &observation.labels);
        let key = self.config.series_key_for_entry(&resource, &labels);
        self.admit(&key)?;
        let k = self.k;
        let seed = self.seed;
        let entry = self
            .panes
            .entry(start)
            .or_default()
            .entry(key)
            .or_insert_with(|| PaneSeries {
                sketch: KLLWrapper::new(k, seed).with_wire_encoding(Encoding::Msgpack),
                resource_labels: resource,
                labels,
                count: 0,
            });
        entry.sketch.update(observation.value.float);
        entry.count = entry.count.saturating_add(1);
        Ok(())
    }

    /// Merge one full upstream tumbling pane. Multiple sources may contribute
    /// to the same pane. Overlapping wide sketches and unaligned panes are
    /// rejected because their observations cannot be assigned to one pane.
    pub fn merge_pane(&mut self, envelope: &SketchEnvelope) -> Result<(), PrecomputeError> {
        if envelope.sketch_type != SketchType::KLLSketch
            || envelope.encoding != Encoding::Msgpack
            || envelope.payload.is_empty()
        {
            return Err(invalid(
                "inbound pane must contain a full ASAPv1 KLL sketch",
            ));
        }
        if self.config.agg_id != 0 && envelope.agg_id != self.config.agg_id {
            return Err(PrecomputeError::AggIdMismatch {
                envelope: envelope.agg_id,
                config: self.config.agg_id,
            });
        }
        if !envelope.window_start_ms.is_multiple_of(self.pane_ms)
            || envelope.window_start_ms.checked_add(self.pane_ms) != Some(envelope.window_end_ms)
        {
            return Err(invalid(
                "inbound sketch must cover exactly one aligned pane",
            ));
        }
        let mut sketch = KLLWrapper::new(self.k, self.seed).with_wire_encoding(Encoding::Msgpack);
        // Decode before changing state: invalid payloads cannot advance retention.
        sketch.apply_delta_encoded(&envelope.payload, Encoding::Msgpack)?;
        let cutoff = self.closed_until.saturating_sub(self.retention_ms);
        if envelope.window_start_ms < cutoff {
            return Err(PrecomputeError::LateData);
        }
        let (resource, labels) = self.labels(&envelope.resource_labels, &envelope.labels);
        let key = self.config.series_key_for_entry(&resource, &labels);
        self.advance(envelope.window_end_ms);
        self.admit(&key)?;
        let entry = self
            .panes
            .entry(envelope.window_start_ms)
            .or_default()
            .entry(key)
            .or_insert_with(|| PaneSeries {
                sketch: KLLWrapper::new(self.k, self.seed).with_wire_encoding(Encoding::Msgpack),
                resource_labels: resource,
                labels,
                count: 0,
            });
        entry.sketch.merge(&sketch)?;
        entry.count = entry.count.saturating_add(envelope.count);
        Ok(())
    }

    fn labels(&self, resource: &[KeyValue], labels: &[KeyValue]) -> (Vec<KeyValue>, Vec<KeyValue>) {
        if self.config.global_aggregation {
            return (vec![], vec![]);
        }
        (
            if self.config.omit_resource_attrs {
                vec![]
            } else {
                resource.to_vec()
            },
            series_attrs(labels, &self.config.aggregate_by),
        )
    }

    fn admit(&self, key: &str) -> Result<(), PrecomputeError> {
        if self.config.max_series == 0 || self.panes.values().any(|pane| pane.contains_key(key)) {
            return Ok(());
        }
        let series: HashSet<_> = self.panes.values().flat_map(|pane| pane.keys()).collect();
        if series.len() as u64 >= self.config.max_series {
            return Err(PrecomputeError::SeriesCapExceeded);
        }
        Ok(())
    }

    /// Merge all complete panes in `[start_ms, end_ms)` into one full KLL per
    /// series. Queries never consume or mutate stored sketches. Bounds must
    /// align to panes, lie within retention, and exclude the active pane.
    /// Missing panes mean no observations, not interpolated samples.
    pub fn query(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SketchEnvelope>, PrecomputeError> {
        if start_ms >= end_ms
            || !start_ms.is_multiple_of(self.pane_ms)
            || !end_ms.is_multiple_of(self.pane_ms)
        {
            return Err(invalid(
                "query bounds must describe a nonempty aligned range",
            ));
        }
        if end_ms > self.closed_until
            || start_ms < self.closed_until.saturating_sub(self.retention_ms)
        {
            return Err(invalid("query includes an open or expired pane"));
        }
        let mut merged: BTreeMap<String, PaneSeries> = BTreeMap::new();
        for (_, pane) in self.panes.range(start_ms..end_ms) {
            for (key, source) in pane {
                let entry = merged.entry(key.clone()).or_insert_with(|| PaneSeries {
                    sketch: KLLWrapper::new(self.k, self.seed)
                        .with_wire_encoding(Encoding::Msgpack),
                    resource_labels: source.resource_labels.clone(),
                    labels: source.labels.clone(),
                    count: 0,
                });
                entry.sketch.merge(&source.sketch)?;
                entry.count = entry.count.saturating_add(source.count);
            }
        }
        merged
            .into_values()
            .map(|entry| {
                Ok(SketchEnvelope {
                    schema_version: 1,
                    sketch_type: SketchType::KLLSketch,
                    agg_id: self.config.agg_id,
                    resource_labels: entry.resource_labels,
                    labels: entry.labels,
                    window_start_ms: start_ms,
                    window_end_ms: end_ms,
                    encoding: Encoding::Msgpack,
                    payload: entry.sketch.snapshot()?,
                    hash_spec: None,
                    metric_name: self.config.metric_name.clone(),
                    count: entry.count,
                    aggregation_temporality: 1,
                    value: 0.0,
                })
            })
            .collect()
    }

    /// Estimate configured quantiles after merging complete panes. This is a
    /// quantile of the union of their observations, never a quantile of p99s.
    pub fn estimate(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SketchEnvelope>, PrecomputeError> {
        self.quantile_over_time(start_ms, end_ms, &self.config.quantiles)
    }

    /// Answer requested quantiles over an aligned range of retained raw-observation
    /// panes. Quantiles may differ on each query; sketches are merged before
    /// estimating and the retained state is unchanged.
    pub fn quantile_over_time(
        &self,
        start_ms: u64,
        end_ms: u64,
        quantiles: &[f64],
    ) -> Result<Vec<SketchEnvelope>, PrecomputeError> {
        if quantiles
            .iter()
            .any(|q| !q.is_finite() || !(0.0..=1.0).contains(q))
        {
            return Err(invalid("quantiles must be finite values in [0, 1]"));
        }
        let mut results = Vec::new();
        for envelope in self.query(start_ms, end_ms)? {
            let mut sketch =
                KLLWrapper::new(self.k, self.seed).with_wire_encoding(Encoding::Msgpack);
            sketch.apply_delta_encoded(&envelope.payload, Encoding::Msgpack)?;
            for point in sketch.estimate(quantiles, 0) {
                let mut result = envelope.clone();
                result.payload.clear();
                result.encoding = Encoding::Unspecified;
                result.value = point.value;
                result.labels.extend(point.labels);
                results.push(result);
            }
        }
        Ok(results)
    }
}

struct RollingState {
    store: KllWindowStore,
    last_emit: Option<u64>,
    upstream_complete_until: Option<u64>,
    stats: StatsSnapshot,
    closed: bool,
}

/// KLL sliding estimator implementing the same runtime API as tumbling
/// precompute. `size` is the lookback and `slide` is the pane/emission interval.
/// It emits the most recent complete range once per boundary, including warmup
/// ranges with fewer observations. Missed ticks coalesce to the latest range.
pub struct KllRollingPrecompute {
    state: Mutex<RollingState>,
}

impl KllRollingPrecompute {
    /// Construct an aligned sliding KLL estimator. Overlapping output is scalar;
    /// when size equals slide, disjoint panes may be transmitted as sketches.
    /// full upstream inputs must be disjoint tumbling panes of `slide` width.
    pub fn new(config: PrecomputeConfig) -> Result<Self, PrecomputeError> {
        if config.mode != AggregationMode::Sliding
            || (config.transmit_sketch && config.window.size != config.window.slide)
            || !config.window.allowed_lateness.is_zero()
        {
            return Err(invalid(
                "overlapping sliding requires scalar output and zero allowed_lateness",
            ));
        }
        if !config.transmit_sketch && config.quantiles.is_empty() {
            return Err(invalid(
                "scalar sliding output requires at least one quantile",
            ));
        }
        let store = KllWindowStore::new(config.clone(), config.window.slide, config.window.size)?;
        Ok(Self {
            state: Mutex::new(RollingState {
                store,
                last_emit: None,
                upstream_complete_until: None,
                stats: StatsSnapshot::default(),
                closed: false,
            }),
        })
    }
}

impl Precompute for KllRollingPrecompute {
    fn observe(&self, observation: &Observation) -> Result<(), PrecomputeError> {
        let mut state = self.state.lock().expect("rolling KLL lock");
        if state.closed {
            return Err(invalid("instance is closed"));
        }
        state.stats.input_observations = state.stats.input_observations.saturating_add(1);
        if observation.value.kind == ObservationValueKind::Envelope {
            drop(state);
            return self.observe_envelope(
                observation
                    .value
                    .envelope
                    .as_deref()
                    .ok_or_else(|| invalid("missing envelope"))?,
            );
        }
        let result = state.store.observe(observation);
        match &result {
            Err(PrecomputeError::LateData) => state.stats.dropped_late += 1,
            Err(PrecomputeError::SeriesCapExceeded) => state.stats.dropped_overflow += 1,
            _ => {}
        }
        result
    }
    fn observe_envelope(&self, envelope: &SketchEnvelope) -> Result<(), PrecomputeError> {
        let mut state = self.state.lock().expect("rolling KLL lock");
        if state.closed {
            return Err(invalid("instance is closed"));
        }
        state.stats.input_envelopes += 1;
        let result = state.store.merge_pane(envelope);
        if result.is_ok() {
            state.upstream_complete_until = Some(
                state
                    .upstream_complete_until
                    .unwrap_or(0)
                    .max(envelope.window_end_ms),
            );
        }
        match &result {
            Err(PrecomputeError::LateData) => state.stats.dropped_late += 1,
            Err(PrecomputeError::SeriesCapExceeded) => state.stats.dropped_overflow += 1,
            _ => {}
        }
        result
    }
    fn tick(&self, now_ms: u64) -> Vec<SketchEnvelope> {
        let mut state = self.state.lock().expect("rolling KLL lock");
        if state.closed {
            return vec![];
        }
        let wall_boundary = now_ms / state.store.pane_ms * state.store.pane_ms;
        let end = state
            .upstream_complete_until
            .map_or(wall_boundary, |complete| complete.min(wall_boundary));
        if state.last_emit.is_some_and(|last| end <= last) || end == 0 {
            return vec![];
        }
        if state.upstream_complete_until.is_none() {
            state.store.advance(now_ms);
        }
        let start = end.saturating_sub(state.store.retention_ms);
        let results = if state.store.config.transmit_sketch {
            state.store.query(start, end)
        } else {
            state.store.estimate(start, end)
        }
        .unwrap_or_default();
        if !results.is_empty() {
            state.last_emit = Some(end);
        }
        state.stats.last_tick_ms = now_ms;
        state.stats.last_emitted_envelopes = results.len() as u64;
        state.stats.output_envelopes += results.len() as u64;
        results
    }
    fn drain(&self) -> Vec<SketchEnvelope> {
        // A partial pane cannot represent a complete rolling interval. Stop
        // without inventing a future boundary or repeating the previous result.
        vec![]
    }
    fn update_config(&self, configs: &PrecomputeConfigSet) {
        let mut state = self.state.lock().expect("rolling KLL lock");
        if let Some(config) = configs
            .configs
            .iter()
            .find(|c| c.agg_id == state.store.config.agg_id)
        {
            if *config != state.store.config {
                if let Ok(replacement) = Self::new(config.clone()) {
                    let mut next = replacement.state.into_inner().expect("rolling KLL lock");
                    next.stats = state.stats;
                    next.closed = state.closed;
                    *state = next;
                }
            }
        }
    }
    fn active_config(&self) -> Option<PrecomputeConfig> {
        Some(
            self.state
                .lock()
                .expect("rolling KLL lock")
                .store
                .config
                .clone(),
        )
    }
    fn stats(&self) -> StatsSnapshot {
        let state = self.state.lock().expect("rolling KLL lock");
        let mut stats = state.stats;
        stats.active_series = state.store.retained_series() as i64;
        stats
    }
    fn shutdown(&self) -> Result<(), PrecomputeError> {
        self.state.lock().expect("rolling KLL lock").closed = true;
        Ok(())
    }
}
