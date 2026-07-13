//! [`Sketch`] trait family + [`Precompute`] trait.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use thiserror::Error;

use crate::config::{PrecomputeConfig, PrecomputeConfigSet};
use crate::envelope::{Encoding, SketchEnvelope, SketchType};
use crate::matchers::series_attrs;
use crate::observation::{KeyValue, Observation, ObservationValueKind};
use crate::snapshot_cache::SnapshotCache;
use crate::window::{SeriesEntry, WindowState};

/// Narrow interface the Layer-3 runtime needs from a Layer-1 sketch
/// implementation.
///
/// Real sketches (DDSketch, KLL, HLL, CountSketch, CountMinSketch)
/// live in [`asap_sketchlib`]; thin wrappers impl this trait against
/// each concrete sketch.
///
/// The interface is intentionally minimal — `observe` is per-sketch
/// (because each sketch has different value-shapes: float, hash,
/// bytes), so the routing layer above lives in the per-shim glue
/// (see [`SketchObserver`]), not here.
pub trait Sketch: Send + Sync {
    /// Serializes the current sketch state to a portable
    /// proto-encoded byte slice.
    ///
    /// Used by [`crate::snapshot_cache::SnapshotCache::compute_delta`]
    /// and as the `ProtoFull` payload.
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError>;

    /// Computes a sparse delta between this sketch and the previous
    /// snapshot bytes.
    ///
    /// If the resulting delta is at least as large as a full snapshot
    /// scaled by `threshold`, returns the full state with
    /// `is_full = true` so the caller can avoid wasted work
    /// re-marshaling. `threshold` is the per-sketch absolute count
    /// cap (see today's `computeDDSketchDelta` /
    /// `countsketch.ComputeDelta`).
    fn compute_delta_against(
        &self,
        prev: &[u8],
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError>;

    /// Merges a previously-computed delta into this sketch in place.
    ///
    /// Used by [`Precompute::observe_envelope`] when an inbound
    /// envelope is encoded as `ProtoDelta`.
    fn apply_delta(&mut self, delta: &[u8]) -> Result<(), PrecomputeError>;

    /// Encoding-aware ingest of an inbound frame (full or delta).
    ///
    /// `encoding` is the inbound envelope's [`Encoding`] tag, which may
    /// differ from this node's configured outbound format (e.g. a central
    /// merger receiving `MsgpackDelta` while emitting `Msgpack`). The
    /// default delegates to [`Self::apply_delta`], preserving the proto
    /// behavior (which auto-detects a full envelope vs a proto delta);
    /// msgpack-capable wrappers override this to dispatch the proto vs
    /// msgpack decoder off the tag.
    fn apply_delta_encoded(
        &mut self,
        payload: &[u8],
        _encoding: Encoding,
    ) -> Result<(), PrecomputeError> {
        self.apply_delta(payload)
    }

    /// Folds another sketch (typically a freshly-decoded envelope
    /// payload) into this one.
    ///
    /// Used by [`Precompute::observe_envelope`] on `ProtoFull`
    /// inbound envelopes.
    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError>;

    /// Zeros the sketch in place. Used by window rotation and by
    /// sketch object pools.
    fn reset(&mut self);

    /// Per-window delta base for a window-reset producer.
    ///
    /// When a family opts in to "true per-window deltas",
    /// this returns the snapshot bytes
    /// the [`crate::snapshot_cache::SnapshotCache`] should cache as the
    /// outbound base AFTER each window-close emit — i.e. the snapshot of
    /// an EMPTY sketch of the same shape. The next window's
    /// [`Self::compute_delta_against`] then diffs against empty, so its
    /// delta is that window's own full per-window state encoded as a
    /// delta (no cross-window subtraction).
    ///
    /// The default returns `None`: such families keep the legacy
    /// always-refresh behavior (the cache is refreshed to the
    /// just-emitted full state). DDSketch, CMS, CountSketch, and HLL
    /// override this to opt in to per-window deltas; KLL keeps the default
    /// (full-only).
    fn delta_against_empty_base(&self) -> Result<Option<Vec<u8>>, PrecomputeError> {
        Ok(None)
    }

    /// Produce estimated output points for the `transmit_sketch = false`
    /// mode. `quantiles` are the configured quantiles (quantile sketches
    /// emit one point per quantile with a `quantile` label); `top_k`
    /// bounds frequency-sketch key output.
    ///
    /// The default returns empty — a sketch with no scalar estimate (or
    /// one whose estimate surface isn't wired) contributes no rows.
    /// DDSketch / KLL (quantiles) and HLL (cardinality) override it.
    /// Frequency sketches need a heavy-hitter tracker to enumerate keys,
    /// which the wire-format wrappers don't carry, so they stay on the
    /// empty default.
    fn estimate(&self, _quantiles: &[f64], _top_k: usize) -> Vec<EstimatePoint> {
        Vec::new()
    }

    /// Type-erased downcast accessor used by paired
    /// [`SketchObserver`] implementations to recover the concrete
    /// sketch type.
    ///
    /// Implementations that want observer downcasting (e.g. real
    /// sketch wrappers in [`crate::sketches`]) override this with
    /// `fn as_any_mut(&mut self) -> &mut dyn Any { self }`. Test
    /// fakes that route observations via byte-level apply_delta
    /// (see `tests/runtime.rs::FakeSketch`) can keep the default
    /// impl which never matches a real downcast — the FakeObserver
    /// doesn't call `as_any_mut`.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Output of [`Sketch::compute_delta_against`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeltaResult {
    /// Wire bytes — either a sparse delta (when `is_full = false`)
    /// or the full snapshot (when `is_full = true`).
    pub payload: Vec<u8>,
    /// Whether `payload` is the full snapshot rather than a sparse
    /// delta. Caller uses this to set
    /// [`crate::envelope::Encoding::ProtoFull`] vs
    /// [`crate::envelope::Encoding::ProtoDelta`].
    pub is_full: bool,
}

/// Implemented by sketches that can answer quantile queries.
///
/// DDSketch and KLL are the two `QuantileSketch` implementations.
/// The runtime never type-asserts to `QuantileSketch` — only adapter
/// code does, when materializing typed quantile output (e.g.
/// emitting one gauge per configured quantile when
/// `transmit_sketch=false`).
pub trait QuantileSketch: Sketch {
    /// Returns the q-th rank value (`0 ≤ q ≤ 1`) from the sketch's
    /// current state. Implementations should clamp `q` to `[0, 1]`
    /// and return a finite value (NaN is acceptable for an empty
    /// sketch).
    fn quantile(&self, q: f64) -> f64;
}

/// Implemented by sketches that answer distinct-count queries.
///
/// HyperLogLog is the canonical `CardinalitySketch`. Adapter code
/// downcasts to call [`Self::estimate_cardinality`] when emitting a
/// typed cardinality gauge from an HLL-backed envelope.
pub trait CardinalitySketch: Sketch {
    /// Returns the sketch's current distinct-element estimate as a
    /// float (HLL's bias-corrected estimator returns a non-integer;
    /// callers round if they want an integer gauge).
    fn estimate_cardinality(&self) -> f64;
}

/// Implemented by sketches that support the **coordinated** producer-side
/// sampling path (`SSP`).
///
/// Only the additive count/quantile families opt in —
/// [`crate::sketches::CMSWrapper`], `CountSketchWrapper`, and `DDSketchWrapper`.
/// `Sum`, `KLL`, and `HLL` are deliberately left out: the coordinator's
/// ε-floor `p` is whole-sketch and meant for additive sketches; HLL keeps its
/// own separate hash-threshold sampling, and Sum/KLL carry no sampling at all.
/// The runtime gates first by configured `SketchType`, then downcasts through
/// this trait, so a non-supporting sketch is left untouched (never panicked on
/// the observe path).
pub trait SampleSetter {
    /// Set the admission-sampling probability `p` (in `(0,1]`; `≥1`/`≤0`/NaN
    /// disables sampling) and reseed. Re-applied to the live sketch so a call
    /// after construction takes effect immediately.
    fn set_sample_p(&mut self, p: f64);
}

/// One entry in a [`FrequencySketch::top_k`] result.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FrequencyEntry {
    /// Opaque byte slice the sketch indexes by (the same shape
    /// passed to [`crate::observation::ObservationValue::bytes`]).
    pub key: Vec<u8>,
    /// Estimated frequency.
    pub count: f64,
}

/// One estimated output point for the `transmit_sketch = false` mode.
///
/// A quantile sketch emits one point per configured quantile (with a
/// `quantile` label); HLL emits a single cardinality point (no extra
/// label); a frequency sketch emits one point per top-k key (with a
/// `key` label).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EstimatePoint {
    /// Extra distinguishing labels appended to the series labels at
    /// emit (e.g. `quantile=0.99`, `key=/api`). Empty for a lone scalar.
    pub labels: Vec<KeyValue>,
    /// The estimated value written to the output `value` column.
    pub value: f64,
}

/// Implemented by sketches that answer count / top-k queries.
///
/// CountSketch and CountMinSketch are the two `FrequencySketch`
/// implementations. Adapter code downcasts to materialize per-key
/// counts or top-k tables.
pub trait FrequencySketch: Sketch {
    /// Returns the estimated frequency for the given key.
    /// Implementations may return a non-integer value (e.g.
    /// CountSketch's median-of-rows estimator).
    fn estimate_count(&self, key: &[u8]) -> f64;

    /// Returns the top-k highest-frequency entries observed by the
    /// sketch. Order is descending by `count`; implementations may
    /// return fewer than `k` entries when the sketch hasn't seen
    /// enough distinct keys.
    fn top_k(&self, k: usize) -> Vec<FrequencyEntry>;
}

/// Implemented by the per-shim glue that knows how to feed a raw
/// observation value (Float / Hash / Bytes) into the concrete
/// sketch.
///
/// NOT part of the [`Sketch`]
/// trait itself because the value type is sketch-specific (DDSketch
/// wants float, HLL wants hash, set-aggregator wants bytes), and
/// forcing all sketches to accept all kinds would push pointless
/// match-arms into every Layer-1 implementation.
pub trait SketchObserver: Send + Sync {
    /// Applies the observation to the sketch.
    ///
    /// Receives the full [`Observation`] so family-specific observers
    /// can key themselves off the observation's labels (e.g. CMS /
    /// CountSketch count the per-attribute-set frequency) in addition
    /// to the raw value.
    ///
    /// Implementations should panic-proof against unsupported kinds
    /// (return an error) and call [`Sketch`] methods directly.
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError>;
}

/// Errors returned by the precompute runtime.
///
/// Sentinel variants cover series-cap, late-data, missing-config,
/// and agg-id / sketch-type mismatches, plus a generic `Other` for
/// nested adapter / sketch errors.
#[derive(Debug, Error)]
pub enum PrecomputeError {
    /// `MaxSeries` is reached and `OnOverflow` is `Drop`.
    #[error("precompute: series cap exceeded")]
    SeriesCapExceeded,
    /// Observation timestamp is older than the active window's lower
    /// bound minus `AllowedLateness`.
    #[error("precompute: observation timestamp outside allowed lateness")]
    LateData,
    /// Precompute has no `PrecomputeConfig`.
    #[error("precompute: no config installed")]
    NoConfig,
    /// Envelope's `agg_id` doesn't match this Precompute's config.
    #[error(
        "precompute: envelope agg_id does not match config: envelope={envelope} config={config}"
    )]
    AggIdMismatch {
        /// Envelope-side `agg_id`.
        envelope: u64,
        /// Config-side `agg_id`.
        config: u64,
    },
    /// Envelope's `sketch_type` doesn't match this Precompute's
    /// config.
    #[error(
        "precompute: envelope sketch_type does not match config: envelope={envelope:?} config={config:?}"
    )]
    SketchTypeMismatch {
        /// Envelope-side type.
        envelope: SketchType,
        /// Config-side type.
        config: SketchType,
    },
    /// Generic catch-all for nested errors (proto decode, sketch
    /// internal failure, etc.).
    #[error("precompute: {0}")]
    Other(String),
}

/// Host-neutral runtime.
///
/// One [`Precompute`] instance
/// owns one sketch type (see [`crate::config::PrecomputeConfig::sketch_type`]);
/// a deployment with multiple sketch types runs multiple instances
/// side-by-side.
pub trait Precompute: Send + Sync {
    /// Routes a raw observation into the active window. May return
    /// [`PrecomputeError::SeriesCapExceeded`] or
    /// [`PrecomputeError::LateData`]; other errors indicate
    /// config / state problems.
    fn observe(&self, obs: &Observation) -> Result<(), PrecomputeError>;

    /// Merges a pre-aggregated upstream sketch into the active
    /// window.
    ///
    /// The envelope's bytes are NEVER expanded to scalar samples
    /// (bandwidth invariant).
    fn observe_envelope(&self, env: &SketchEnvelope) -> Result<(), PrecomputeError>;

    /// Rotates the active window (when due) and returns the closed
    /// window's series as `Vec<SketchEnvelope>` ready for emit.
    ///
    /// `now_ms` is the wall-clock time the caller (e.g.
    /// `Adapter::schedule_tick`) considers "now"; the runtime uses
    /// it to decide whether the window is due for rotation.
    fn tick(&self, now_ms: u64) -> Vec<SketchEnvelope>;

    /// Forces rotation of the active window regardless of
    /// wall-clock time and returns any envelopes that result.
    ///
    /// Use this on shutdown paths to flush pending observations
    /// that haven't reached their natural window boundary.
    /// Distinct from [`Self::tick`]: `tick` only rotates when
    /// `now_ms >= active_end_ms`, which silently drops mid-window
    /// data on early termination. After `drain` the next active
    /// window's bounds are advanced to the same boundary `tick`
    /// would have used at the natural rotation point.
    fn drain(&self) -> Vec<SketchEnvelope>;

    /// Atomically swaps the active config.
    ///
    /// The in-flight window is preserved (matchers / `aggregate_by`
    /// may change, but bytes already accumulated stay where they
    /// are).
    fn update_config(&self, cs: &PrecomputeConfigSet);

    /// Returns the live counters; safe to call concurrently.
    fn stats(&self) -> StatsSnapshot;

    /// Flushes any in-progress state; intended for the shim's
    /// shutdown path to run a final tick before returning.
    fn shutdown(&self) -> Result<(), PrecomputeError>;
}

/// Point-in-time snapshot of [`Precompute`] runtime counters.
///
/// Values across fields may be
/// drawn from slightly different instants — adapters that need an
/// atomic multi-counter view must add an explicit lock.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StatsSnapshot {
    /// Total `observe()` calls (all kinds).
    pub input_observations: u64,
    /// Total `observe_envelope()` calls.
    pub input_envelopes: u64,
    /// Envelopes emitted via `tick`.
    pub output_envelopes: u64,
    /// Current size of the per-`(agg_id, label_key)` map in the
    /// active window. Negative values are not expected but tolerated
    /// for atomic-decrement safety on series eviction.
    pub active_series: i64,
    /// Observations dropped due to `MaxSeries`.
    pub dropped_overflow: u64,
    /// Observations dropped due to `AllowedLateness`.
    pub dropped_late: u64,
    /// Wall-clock timestamp of the last `tick` call.
    pub last_tick_ms: u64,
    /// Count returned by the most recent `tick` (snapshot of
    /// one-tick output volume).
    pub last_emitted_envelopes: u64,
}

/// Type-erased boxed [`SketchObserver`].
///
/// The default impl in
/// [`PrecomputeImpl`] holds this in an `Option` because either
/// `cfg` or `observer` may be `None` at construction time.
pub type BoxedObserver = Box<dyn SketchObserver>;

/// Factory function that constructs an empty [`Sketch`] of the type
/// owned by a specific [`Precompute`] instance.
///
/// Construction is per-instance rather than registry-based to avoid
/// global state.
pub type SketchFactory = Box<dyn Fn() -> Box<dyn Sketch> + Send + Sync>;

/// Concrete implementation of [`Precompute`].
///
/// Exposed publicly so tests and downstream binaries
/// can construct one directly. Fields are private; construction
/// goes through [`PrecomputeImpl::new`].
pub struct PrecomputeImpl {
    cfg: Mutex<Option<PrecomputeConfig>>,
    sketch_factory: Option<SketchFactory>,
    observer: Option<BoxedObserver>,
    window: Mutex<WindowState>,
    snapshot_cache: SnapshotCache,
    stats: Mutex<StatsSnapshot>,
    sketch_type: SketchType,
    closed: AtomicBool,
}

impl PrecomputeImpl {
    /// Constructs a [`PrecomputeImpl`] given an initial config, a
    /// sketch factory that produces empty sketches of the configured
    /// type, and a [`SketchObserver`] that knows how to apply
    /// observation values to the sketch.
    ///
    /// Either `initial_cfg` or `observer` may be `None` at
    /// construction time, but [`Precompute::observe`] will return
    /// [`PrecomputeError::NoConfig`] until [`Precompute::update_config`]
    /// is called.
    pub fn new(
        initial_cfg: Option<PrecomputeConfig>,
        sketch_factory: Option<SketchFactory>,
        observer: Option<BoxedObserver>,
    ) -> Self {
        let sketch_type = initial_cfg
            .as_ref()
            .map(|c| c.sketch_type)
            .unwrap_or(SketchType::Unspecified);
        Self {
            cfg: Mutex::new(initial_cfg),
            sketch_factory,
            observer,
            window: Mutex::new(WindowState::new()),
            snapshot_cache: SnapshotCache::new(),
            stats: Mutex::new(StatsSnapshot::default()),
            sketch_type,
            closed: AtomicBool::new(false),
        }
    }

    /// Returns the configured sketch type.
    pub fn sketch_type(&self) -> SketchType {
        self.sketch_type
    }

    /// Returns whether the instance has been shut down.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl PrecomputeImpl {
    /// Returns a clone of the active config or `None`.
    fn active_config(&self) -> Option<PrecomputeConfig> {
        self.cfg.lock().expect("config lock poisoned").clone()
    }

    /// Walks the closed series, serializes each into a
    /// [`SketchEnvelope`] (honoring `delta_transmission`), and
    /// updates the rolling stats counters.
    fn finish_rotate(
        &self,
        closed: Vec<SeriesEntry>,
        rng: [u64; 2],
        now_ms: u64,
    ) -> Vec<SketchEnvelope> {
        if closed.is_empty() {
            return Vec::new();
        }
        let cfg = match self.active_config() {
            Some(c) => c,
            None => return Vec::new(),
        };
        let mut envelopes = Vec::with_capacity(closed.len());
        for entry in closed.into_iter() {
            // Best-effort: skip serialization errors. Real shims log
            // via their host logger; the Layer-3 runtime is host-
            // neutral and has no logger. Estimate mode may yield several
            // rows (one per quantile) per series.
            if let Ok(envs) = self.serialize_series(&entry, &cfg, rng) {
                envelopes.extend(envs);
            }
        }
        let mut stats = self.stats.lock().expect("stats lock poisoned");
        stats.output_envelopes = stats
            .output_envelopes
            .saturating_add(envelopes.len() as u64);
        stats.last_tick_ms = now_ms;
        stats.last_emitted_envelopes = envelopes.len() as u64;
        envelopes
    }

    /// Turns a closed series entry into a [`SketchEnvelope`].
    /// Honors `delta_transmission` via the snapshot cache.
    fn serialize_series(
        &self,
        entry: &SeriesEntry,
        cfg: &PrecomputeConfig,
        rng: [u64; 2],
    ) -> Result<Vec<SketchEnvelope>, PrecomputeError> {
        // Rebuild the same key the window used at admit time.
        let series_key = cfg.series_key_for_entry(&entry.resource_labels, &entry.labels);

        // Base labels shared by both output modes (series attrs + optional
        // operator-visibility window stats).
        let mut base_labels = series_attrs(&entry.labels, &cfg.aggregate_by);
        if cfg.emit_window_stats {
            let window_seconds = if cfg.window.size == Duration::ZERO {
                0
            } else {
                cfg.window.size.as_secs()
            };
            base_labels.push(KeyValue::new(
                "sample_count".to_string(),
                entry.count.to_string(),
            ));
            base_labels.push(KeyValue::new(
                "window_duration_seconds".to_string(),
                window_seconds.to_string(),
            ));
        }

        // Estimate mode (`transmit_sketch = false`): emit typed scalar rows
        // — one Gauge per configured quantile (DDSketch / KLL), a single
        // cardinality Gauge (HLL) — instead of sketch bytes. The estimate
        // value rides the `value` field; the distinguishing label (e.g.
        // `quantile`) is appended to the series labels.
        if !cfg.transmit_sketch {
            const DEFAULT_TOP_K: usize = 10;
            let points = entry.sketch.estimate(&cfg.quantiles, DEFAULT_TOP_K);
            let out = points
                .into_iter()
                .map(|p| {
                    let mut labels = base_labels.clone();
                    labels.extend(p.labels);
                    SketchEnvelope {
                        schema_version: 1,
                        sketch_type: cfg.sketch_type,
                        agg_id: cfg.agg_id,
                        resource_labels: entry.resource_labels.clone(),
                        labels,
                        window_start_ms: rng[0],
                        window_end_ms: rng[1],
                        // No sketch bytes on the estimate path.
                        encoding: Encoding::Unspecified,
                        payload: Vec::new(),
                        hash_spec: None,
                        metric_name: cfg.metric_name.clone(),
                        count: entry.count,
                        aggregation_temporality: cfg.temporality,
                        value: p.value,
                    }
                })
                .collect();
            return Ok(out);
        }

        // Sketch-on-the-wire mode.
        let (payload, encoding) = if cfg.delta_transmission {
            let result = self.snapshot_cache.compute_delta(
                &series_key,
                entry.sketch.as_ref(),
                cfg.delta_threshold,
            )?;
            let enc = if result.is_full {
                full_encoding(cfg.encoding)
            } else {
                delta_encoding(cfg.encoding)
            };
            (result.payload, enc)
        } else {
            let snap = entry.sketch.snapshot()?;
            // Even without delta transmission, refreshing the cached
            // outbound snapshot keeps the cache consistent for any
            // later config change that flips delta_transmission to
            // true.
            self.snapshot_cache.cache_outbound(&series_key, &snap);
            (snap, full_encoding(cfg.encoding))
        };
        if payload.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![SketchEnvelope {
            schema_version: 1,
            sketch_type: cfg.sketch_type,
            agg_id: cfg.agg_id,
            resource_labels: entry.resource_labels.clone(),
            labels: base_labels,
            window_start_ms: rng[0],
            window_end_ms: rng[1],
            encoding,
            payload,
            hash_spec: None,
            metric_name: cfg.metric_name.clone(),
            count: entry.count,
            aggregation_temporality: cfg.temporality,
            value: 0.0,
        }])
    }
}

/// Maps a configured wire format to the [`Encoding`] tag stamped on a
/// **full** frame: msgpack configs emit `Msgpack`, everything else
/// `ProtoFull`. The wrapper's `snapshot()` produces bytes in the matching
/// format because its `wire_encoding` is baked from the same `cfg.encoding`.
fn full_encoding(configured: Encoding) -> Encoding {
    if configured.is_msgpack() {
        Encoding::Msgpack
    } else {
        Encoding::ProtoFull
    }
}

/// Maps a configured wire format to the [`Encoding`] tag stamped on a
/// **delta** frame: msgpack configs emit `MsgpackDelta`, everything else
/// `ProtoDelta`.
fn delta_encoding(configured: Encoding) -> Encoding {
    if configured.is_msgpack() {
        Encoding::MsgpackDelta
    } else {
        Encoding::ProtoDelta
    }
}

impl Precompute for PrecomputeImpl {
    fn observe(&self, obs: &Observation) -> Result<(), PrecomputeError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PrecomputeError::Other("instance is closed".into()));
        }
        let cfg = match self.active_config() {
            Some(c) => c,
            None => return Err(PrecomputeError::NoConfig),
        };

        // Envelope-valued observations route through the dedicated
        // pre-aggregated path so we never explode them to scalars.
        // The input_observations counter is bumped before
        // routing so envelope-valued observations also count.
        {
            let mut stats = self.stats.lock().expect("stats lock poisoned");
            stats.input_observations = stats.input_observations.saturating_add(1);
        }

        if obs.value.kind == ObservationValueKind::Envelope {
            if let Some(env) = obs.value.envelope.as_ref() {
                return self.observe_envelope(env);
            }
        }

        if !cfg.matches(obs) {
            return Ok(());
        }

        let sketch_factory = self
            .sketch_factory
            .as_ref()
            .ok_or_else(|| PrecomputeError::Other("sketch factory not configured".into()))?;
        let observer = self
            .observer
            .as_ref()
            .ok_or_else(|| PrecomputeError::Other("sketch observer not configured".into()))?;

        let mut window = self.window.lock().expect("window lock poisoned");
        let mut stats = self.stats.lock().expect("stats lock poisoned");
        let result = window.observe(obs, &cfg, sketch_factory, observer, &mut stats);
        if let Err(err) = &result {
            match err {
                PrecomputeError::SeriesCapExceeded => {
                    stats.dropped_overflow = stats.dropped_overflow.saturating_add(1);
                }
                PrecomputeError::LateData => {
                    stats.dropped_late = stats.dropped_late.saturating_add(1);
                }
                _ => {}
            }
        }
        result
    }

    fn observe_envelope(&self, env: &SketchEnvelope) -> Result<(), PrecomputeError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(PrecomputeError::Other("instance is closed".into()));
        }
        let cfg = match self.active_config() {
            Some(c) => c,
            None => return Err(PrecomputeError::NoConfig),
        };
        // AggID match — strict. Mismatches are hard errors, not
        // silent drops.
        if cfg.agg_id != 0 && env.agg_id != 0 && env.agg_id != cfg.agg_id {
            return Err(PrecomputeError::AggIdMismatch {
                envelope: env.agg_id,
                config: cfg.agg_id,
            });
        }
        if cfg.sketch_type != SketchType::Unspecified
            && env.sketch_type != SketchType::Unspecified
            && env.sketch_type != cfg.sketch_type
        {
            return Err(PrecomputeError::SketchTypeMismatch {
                envelope: env.sketch_type,
                config: cfg.sketch_type,
            });
        }

        let sketch_factory = self
            .sketch_factory
            .as_ref()
            .ok_or_else(|| PrecomputeError::Other("sketch factory not configured".into()))?;

        let mut window = self.window.lock().expect("window lock poisoned");
        let mut stats = self.stats.lock().expect("stats lock poisoned");
        stats.input_envelopes = stats.input_envelopes.saturating_add(1);
        let result =
            window.observe_envelope(env, &cfg, sketch_factory, &self.snapshot_cache, &mut stats);
        if let Err(err) = &result {
            if matches!(err, PrecomputeError::SeriesCapExceeded) {
                stats.dropped_overflow = stats.dropped_overflow.saturating_add(1);
            }
        }
        result
    }

    fn tick(&self, now_ms: u64) -> Vec<SketchEnvelope> {
        let cfg = match self.active_config() {
            Some(c) => c,
            None => return Vec::new(),
        };
        let (closed, rng) = {
            let mut window = self.window.lock().expect("window lock poisoned");
            window.rotate(now_ms, &cfg)
        };
        self.finish_rotate(closed, rng, now_ms)
    }

    fn drain(&self) -> Vec<SketchEnvelope> {
        let cfg = match self.active_config() {
            Some(c) => c,
            None => return Vec::new(),
        };
        let (closed, rng) = {
            let mut window = self.window.lock().expect("window lock poisoned");
            window.drain(&cfg)
        };
        self.finish_rotate(closed, rng, rng[1])
    }

    fn update_config(&self, cs: &PrecomputeConfigSet) {
        if cs.configs.is_empty() {
            return;
        }
        let mut guard = self.cfg.lock().expect("config lock poisoned");
        let active_agg_id = guard.as_ref().map(|c| c.agg_id);
        let chosen = match active_agg_id {
            Some(id) => cs
                .configs
                .iter()
                .find(|c| c.agg_id == id)
                .or_else(|| cs.configs.first()),
            None => cs.configs.first(),
        };
        if let Some(c) = chosen {
            *guard = Some(c.clone());
        }
    }

    fn stats(&self) -> StatsSnapshot {
        *self.stats.lock().expect("stats lock poisoned")
    }

    fn shutdown(&self) -> Result<(), PrecomputeError> {
        // CompareAndSwap: only the first shutdown does work. A later
        // revision will run a final tick here on the shutdown path.
        let _ = self
            .closed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PrecomputeConfig;

    #[test]
    fn new_with_no_config_starts_unspecified() {
        let p = PrecomputeImpl::new(None, None, None);
        assert_eq!(p.sketch_type(), SketchType::Unspecified);
        assert!(!p.is_closed());
    }

    #[test]
    fn update_config_picks_matching_agg_id() {
        let initial = PrecomputeConfig {
            agg_id: 7,
            sketch_type: SketchType::DDSketch,
            ..Default::default()
        };
        let p = PrecomputeImpl::new(Some(initial), None, None);
        let new_set = PrecomputeConfigSet {
            version: 2,
            configs: vec![
                PrecomputeConfig {
                    agg_id: 1,
                    sketch_type: SketchType::HLLSketch,
                    ..Default::default()
                },
                PrecomputeConfig {
                    agg_id: 7,
                    sketch_type: SketchType::DDSketch,
                    metric_name: "bumped".into(),
                    ..Default::default()
                },
            ],
        };
        p.update_config(&new_set);
        let cfg = p.cfg.lock().unwrap();
        assert_eq!(cfg.as_ref().unwrap().agg_id, 7);
        assert_eq!(cfg.as_ref().unwrap().metric_name, "bumped");
    }

    #[test]
    fn shutdown_is_idempotent() {
        let p = PrecomputeImpl::new(None, None, None);
        p.shutdown().expect("first shutdown");
        p.shutdown().expect("second shutdown");
        assert!(p.is_closed());
    }
}
