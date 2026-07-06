//! [`PrecomputeConfig`] and friends.
//!
//! Defines all type-level surface (config struct,
//! aggregation mode, overflow policy, window spec, sketch params,
//! plan-set wrapper) plus the trivial accessors. Series-key
//! construction (`series_key_for`, `series_key_for_entry`) is
//! type-only here — the actual canonical-key bytes are defined by
//! [`crate::matchers`].

use std::collections::HashMap;
use std::time::Duration;

use crate::envelope::{Encoding, SketchType};
use crate::matchers::LabelMatcher;
use crate::observation::{KeyValue, Observation};

/// Controller-plan join key. One [`PrecomputeConfig`] per `AggId`; the
/// controller's plan emits a flat list keyed by `AggId`.
pub type AggId = u64;

/// Picks the windowing strategy.
///
/// Only [`AggregationMode::Tumbling`] is implemented;
/// [`AggregationMode::Sliding`] is deferred (see [`crate::window`]).
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum AggregationMode {
    /// Rotates the window every [`WindowSpec::size`]; observations
    /// land in exactly one window. This is what all five OTel
    /// processors use today.
    #[default]
    Tumbling,
    /// Rotates every [`WindowSpec::slide`]; observations land in
    /// every window whose `[start, end)` range covers their
    /// timestamp. Only Tumbling is implemented today.
    Sliding,
    /// Processes one batch end-to-end with no windowing (`mode: batch`).
    Batch,
}

impl AggregationMode {
    /// Returns a debug name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Tumbling => "Tumbling",
            Self::Sliding => "Sliding",
            Self::Batch => "Batch",
        }
    }
}

/// Behavior when a window's series-cardinality would exceed
/// [`PrecomputeConfig::max_series`].
///
/// `MaxSeries + OnOverflow` are new fields necessary because
/// extracting the runtime out of the OTel pipeline removes the
/// implicit channel-based backpressure.
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum OnOverflow {
    /// Drop new observations whose series isn't already tracked.
    /// Existing series keep accepting samples.
    #[default]
    Drop,
    /// Block the caller until the next window rotation frees
    /// capacity. Latency-hostile; reserved for integration tests.
    Block,
    /// Evict the least-recently-seen series to make room for the new
    /// one.
    EvictOldest,
}

impl OnOverflow {
    /// Returns a debug name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Drop => "Drop",
            Self::Block => "Block",
            Self::EvictOldest => "EvictOldest",
        }
    }
}

/// Configures the windowing parameters.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowSpec {
    /// Window duration. For [`AggregationMode::Tumbling`], this is
    /// the rotation period. For [`AggregationMode::Sliding`], it's
    /// the active-range length.
    #[serde(with = "duration_millis")]
    pub size: Duration,
    /// Rotation interval for sliding windows. Zero means tumbling
    /// (`slide == size` implicitly).
    #[serde(with = "duration_millis")]
    pub slide: Duration,
    /// Grace period for late-arriving samples; observations whose
    /// `timestamp_ms` is older than `active_start - allowed_lateness`
    /// are dropped as late.
    #[serde(with = "duration_millis")]
    pub allowed_lateness: Duration,
}

mod duration_millis {
    //! Serialize a `Duration` as a millisecond integer for round-trip
    //! through JSON / YAML adapters that don't understand Rust's
    //! native `Duration` formatting.
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_u64(d.as_millis() as u64)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
        let ms: u64 = u64::deserialize(de)?;
        Ok(Duration::from_millis(ms))
    }
}

/// Algorithm-specific tuning knobs.
///
/// Encoded as a flat map so
/// adapters can stuff in algorithm-specific values without the
/// runtime needing a typed schema for each.
///
/// Expected keys per sketch type:
/// - DDSketch: `relative_accuracy` (float, 0 < v < 1)
/// - KLL: `k` (uint, ≥ 8)
/// - HLL: `precision` (uint, 4-18)
/// - CountSketch: `epsilon`, `delta` (floats, 0 < v < 1) plus `width`,
///   `depth` (derived if absent)
/// - CountMin: `rows`, `columns` (uints)
pub type SketchParams = HashMap<String, f64>;

/// Returns the parameter value or `default` if not present.
pub fn sketch_param_get(params: &SketchParams, key: &str, default: f64) -> f64 {
    params.get(key).copied().unwrap_or(default)
}

/// Host-neutral form of today's per-OTel-processor `Config` struct,
/// plus `max_series` / `on_overflow`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PrecomputeConfig {
    /// Controller-plan join key. One config per `AggId`.
    pub agg_id: AggId,
    /// Determines which Sketch implementation handles observations
    /// for this `AggId`.
    pub sketch_type: SketchType,
    /// Picks the windowing strategy.
    pub mode: AggregationMode,
    /// Configures size / slide / lateness.
    pub window: WindowSpec,
    /// Selects observations by metric name and label values. All
    /// matchers must hold for the observation to be admitted.
    pub matchers: Vec<LabelMatcher>,
    /// Lists label keys to aggregate over. When non-empty, the
    /// series key is built only from these keys (cross-series
    /// aggregation). When empty, every distinct label set is its own
    /// series.
    pub aggregate_by: Vec<String>,
    /// Chooses between sketch-on-the-wire and quantile-summary-on-
    /// the-wire output.
    pub transmit_sketch: bool,
    /// Enables delta encoding against the cached outbound snapshot.
    pub delta_transmission: bool,
    /// Caps the absolute delta size; if the computed delta exceeds
    /// `delta_threshold * full-state size`, the runtime emits the
    /// full state instead.
    ///
    /// Mirrors today's `ddsketch::ComputeDelta` threshold semantics.
    /// The unit is sketch-specific (`u64` for DDSketch/CMS bucket
    /// counts; `f64` for CountSketch L2 cells, stored here as
    /// rounded-up `u64` — adapters convert as needed).
    pub delta_threshold: u64,
    /// Overrides the wire encoding of emitted envelopes.
    ///
    /// Default zero value (`Unspecified`, treated as `ProtoFull`) is
    /// used for non-delta transmissions. When `delta_transmission` is
    /// true, the runtime emits `ProtoDelta` frames after the initial
    /// `ProtoFull` snapshot. `Msgpack` is supported for legacy paths
    /// (HLL / CountSketch / CMS today carry an explicit `encoding`
    /// knob in their existing config).
    pub encoding: Encoding,
    /// Output mode when `transmit_sketch` is false. For
    /// quantile-sketch types (DDSketch / KLL), each quantile in this
    /// list becomes a separate gauge metric on emit. Empty list means
    /// "always emit envelope" (semantically equivalent to
    /// `transmit_sketch=true`). Ignored for non-quantile sketches.
    pub quantiles: Vec<f64>,
    /// Algorithm-specific tuning knobs. See [`SketchParams`].
    pub sketch_params: SketchParams,
    /// Caps the per-Precompute series cardinality. Zero means
    /// unbounded (matches today's processors which had no cap).
    pub max_series: u64,
    /// Controls behavior when [`Self::max_series`] is exceeded.
    pub on_overflow: OnOverflow,
    /// Metric name to stamp onto every [`crate::envelope::SketchEnvelope`]
    /// emitted by `tick`.
    ///
    /// A per-processor `MetricName` config knob. The runtime
    /// copies it verbatim into [`crate::envelope::SketchEnvelope::metric_name`]
    /// so `Adapter::encode` can set `pmetric::Metric.Name()` without
    /// a side-channel label hack.
    pub metric_name: String,
    /// OTel aggregation-temporality enum to stamp onto every
    /// [`crate::envelope::SketchEnvelope`] emitted by `tick`:
    /// 0 = unspecified, 1 = delta, 2 = cumulative.
    ///
    /// Stored as `i32` (not the OTel enum type) to keep the runtime
    /// host-neutral. Today's processors emit delta sums, so adapters
    /// that don't set this explicitly will see the zero value
    /// (unspecified) and should default to 1 (delta) themselves; the
    /// OTel shim sets it to 1.
    pub temporality: i32,
    /// Whether resource-scope attributes participate in series-key
    /// construction (and whether they are carried through to the
    /// emitted [`crate::envelope::SketchEnvelope::resource_labels`]).
    ///
    /// The zero value (`false`) means "include resource attrs" — the
    /// runtime's default and the shape today's DDSketch processor
    /// expects: series key distinguishes (resource, dp-labels)
    /// tuples.
    ///
    /// The four other legacy processors (KLL, HLL, CountSketch,
    /// CountMinSketch) build their batch-mode series key from the
    /// data-point attributes ONLY: they ignore resource attrs in the
    /// key AND emit output into a freshly-appended `ResourceMetrics`
    /// with an empty Resource. Setting this knob to `true` makes the
    /// runtime mirror that behavior: cross-resource observations with
    /// the same dp-labels collapse into a single series, and the
    /// emitted [`crate::envelope::SketchEnvelope::resource_labels`] is
    /// empty.
    ///
    /// Naming note: the field is stated as `omit_*` rather than
    /// `include_*` so the zero value is the today-correct default and
    /// existing literals don't need to change.
    pub omit_resource_attrs: bool,
    /// Collapses every admitted observation into a single "global"
    /// series — both resource attrs and dp-labels are ignored when
    /// constructing series key, and both are stripped from the
    /// emitted envelope.
    ///
    /// Used by the legacy CountSketch processor when `aggregate_by`
    /// is empty (its `buildPartitionKey` returns the literal string
    /// `"global"`, driving every observation into one shared
    /// sketch). When this is `true` the [`Self::omit_resource_attrs`]
    /// flag is implicitly also true (no resource attrs surface).
    ///
    /// Defaults to `false`; only the CountSketch shim sets it to
    /// `true`.
    pub global_aggregation: bool,
    /// Appends two operator-visibility attributes onto every emitted
    /// envelope's `labels` at flush time:
    ///   - `sample_count` (entry count, the number of admitted
    ///     observations contributing to the window)
    ///   - `window_duration_seconds` (`window.size`, in whole
    ///     seconds)
    ///
    /// Per-data-point observability attributes; downstream ignores
    /// them for routing — they're just hints. The runtime omits them
    /// by default
    /// because the other 4 sketch processors (DDSketch / KLL / HLL /
    /// CMS) do not emit them.
    pub emit_window_stats: bool,
}

impl PrecomputeConfig {
    /// Returns whether the observation passes all configured matchers.
    ///
    /// Returns `true` if matchers is empty, otherwise delegates to
    /// [`crate::matchers::LabelMatcher::matches_all`].
    pub fn matches(&self, obs: &Observation) -> bool {
        if self.matchers.is_empty() {
            return true;
        }
        LabelMatcher::matches_all(&self.matchers, obs)
    }

    /// Builds the canonical series key for an observation, honoring
    /// the [`Self::omit_resource_attrs`] and
    /// [`Self::global_aggregation`] flags.
    ///
    /// Thin wrapper over [`crate::matchers::series_key`], which owns
    /// the full canonical-byte formatting.
    pub fn series_key_for(&self, obs: &Observation) -> String {
        if self.global_aggregation {
            return crate::matchers::series_key(self.agg_id, &[], &[], &[]);
        }
        if self.omit_resource_attrs {
            return crate::matchers::series_key(self.agg_id, &[], &obs.labels, &self.aggregate_by);
        }
        crate::matchers::series_key(
            self.agg_id,
            &obs.resource_labels,
            &obs.labels,
            &self.aggregate_by,
        )
    }

    /// Rebuilds the same key from a series entry's stored labels.
    ///
    /// The invariant is: for a given config and an observation that
    /// produced an entry,
    /// `series_key_for(obs) == series_key_for_entry(resource_labels, labels)`.
    pub fn series_key_for_entry(
        &self,
        resource_labels: &[KeyValue],
        labels: &[KeyValue],
    ) -> String {
        if self.global_aggregation {
            return crate::matchers::series_key(self.agg_id, &[], &[], &[]);
        }
        if self.omit_resource_attrs {
            return crate::matchers::series_key(self.agg_id, &[], labels, &self.aggregate_by);
        }
        crate::matchers::series_key(self.agg_id, resource_labels, labels, &self.aggregate_by)
    }
}

/// Versioned bundle of configs delivered by
/// [`crate::control_channel::ControlChannel`].
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PrecomputeConfigSet {
    /// Monotonically increasing across plans; ack with
    /// `ControlChannel::ack`.
    pub version: u64,
    /// `PrecomputeConfig`s active for this host.
    pub configs: Vec<PrecomputeConfig>,
}

impl PrecomputeConfigSet {
    /// Returns the config matching `agg_id`, or `None`.
    pub fn find_by_agg_id(&self, agg_id: AggId) -> Option<&PrecomputeConfig> {
        self.configs.iter().find(|c| c.agg_id == agg_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precompute_config_default_has_sensible_defaults() {
        let cfg = PrecomputeConfig::default();
        assert_eq!(cfg.agg_id, 0);
        assert_eq!(cfg.sketch_type, SketchType::Unspecified);
        assert_eq!(cfg.mode, AggregationMode::Tumbling);
        assert_eq!(cfg.on_overflow, OnOverflow::Drop);
        assert_eq!(cfg.encoding, Encoding::Unspecified);
        assert_eq!(cfg.window.size, Duration::ZERO);
        assert!(!cfg.delta_transmission);
        assert!(!cfg.global_aggregation);
        assert!(!cfg.omit_resource_attrs);
        assert!(!cfg.emit_window_stats);
    }

    #[test]
    fn aggregation_mode_names_match_go() {
        assert_eq!(AggregationMode::Tumbling.name(), "Tumbling");
        assert_eq!(AggregationMode::Sliding.name(), "Sliding");
        assert_eq!(AggregationMode::Batch.name(), "Batch");
    }

    #[test]
    fn on_overflow_names_match_go() {
        assert_eq!(OnOverflow::Drop.name(), "Drop");
        assert_eq!(OnOverflow::Block.name(), "Block");
        assert_eq!(OnOverflow::EvictOldest.name(), "EvictOldest");
    }

    #[test]
    fn precompute_config_set_finds_by_agg_id() {
        let set = PrecomputeConfigSet {
            version: 7,
            configs: vec![
                PrecomputeConfig {
                    agg_id: 1,
                    ..Default::default()
                },
                PrecomputeConfig {
                    agg_id: 42,
                    ..Default::default()
                },
            ],
        };
        assert_eq!(set.find_by_agg_id(42).map(|c| c.agg_id), Some(42));
        assert!(set.find_by_agg_id(99).is_none());
    }

    #[test]
    fn sketch_params_get_returns_default_when_missing() {
        let mut p = SketchParams::new();
        p.insert("precision".into(), 12.0);
        assert_eq!(sketch_param_get(&p, "precision", 4.0), 12.0);
        assert_eq!(sketch_param_get(&p, "missing", 4.0), 4.0);
    }
}
