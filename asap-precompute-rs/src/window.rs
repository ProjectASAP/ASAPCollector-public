//! Per-[`crate::precompute::Precompute`] window manager.

use std::collections::HashMap;
use std::time::Duration;

use crate::config::{OnOverflow, PrecomputeConfig};
use crate::envelope::{Encoding, SketchEnvelope};
use crate::observation::{KeyValue, Observation};
use crate::precompute::{BoxedObserver, PrecomputeError, Sketch, SketchFactory, StatsSnapshot};
use crate::snapshot_cache::SnapshotCache;

/// Per-series state held inside a window.
///
/// Owns one [`Sketch`] instance and the
/// labels needed to reconstruct the [`SketchEnvelope`] at flush
/// time.
pub struct SeriesEntry {
    /// Running sketch for this series. Owned here; the window calls
    /// `Sketch::reset` on rotation when the entry is recycled in
    /// place, OR drops the reference when `MaxSeries` triggers
    /// eviction.
    pub sketch: Box<dyn Sketch>,
    /// Resource-scope attribute set captured when the series was
    /// first observed. Stored alongside `labels` so the adapter's
    /// encode path can faithfully reconstruct the
    /// `pmetric::ResourceMetrics → ScopeMetrics → Metric` hierarchy
    /// (or its non-OTel equivalent) on emission.
    pub resource_labels: Vec<KeyValue>,
    /// Host-neutral data-point attribute set used by series-key
    /// construction and by the encode path at the adapter boundary.
    pub labels: Vec<KeyValue>,
    /// Most recent observation timestamp; used for
    /// [`crate::config::OnOverflow::EvictOldest`].
    pub last_seen_ms: u64,
    /// Total observation count accumulated for this series in the
    /// active window. Incremented once per scalar observation;
    /// envelope-valued observations contribute the upstream
    /// envelope's `count` when present (so chained pre-aggregation
    /// preserves the running sample count). Copied into
    /// [`SketchEnvelope::count`] at flush time so the OTel adapter
    /// can set `dp.SetCount()`.
    pub count: u64,
}

/// Per-Precompute window manager. Tumbling-only for now;
/// Sliding lands in a follow-up.
///
/// Locking is owned by the enclosing [`std::sync::Mutex`] in
/// [`crate::precompute::PrecomputeImpl`]; this struct itself is
/// `!Sync`-by-content (HashMap of boxed dyn Sketch) and relies on the
/// outer mutex to serialize access.
pub struct WindowState {
    /// Map from series-key (see [`crate::matchers::series_key`]) to
    /// the active series entry.
    pub(crate) series: HashMap<String, SeriesEntry>,
    /// Inclusive lower bound of the active window (Unix ms).
    pub(crate) active_start_ms: u64,
    /// Exclusive upper bound of the active window (Unix ms).
    pub(crate) active_end_ms: u64,
    /// Whether `active_start_ms` / `active_end_ms` have been
    /// initialized for the active config. Lazy-init avoids needing
    /// the constructor to know the config up front.
    pub(crate) initialized: bool,
}

impl Default for WindowState {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the active window size in milliseconds, or zero for
/// unsized (Batch) configs.
pub(crate) fn window_size_ms(cfg: &PrecomputeConfig) -> u64 {
    if cfg.window.size == Duration::ZERO {
        return 0;
    }
    cfg.window.size.as_millis() as u64
}

impl WindowState {
    /// Constructs an empty window.
    pub fn new() -> Self {
        Self {
            series: HashMap::new(),
            active_start_ms: 0,
            active_end_ms: 0,
            initialized: false,
        }
    }

    /// Returns the current series count.
    pub fn active_series_count(&self) -> usize {
        self.series.len()
    }

    /// Lazily computes the first window's bounds based on a
    /// reference timestamp.
    pub fn init_window(&mut self, ref_ms: u64, cfg: &PrecomputeConfig) {
        if self.initialized {
            return;
        }
        let size = window_size_ms(cfg);
        if size == 0 {
            // Batch mode: window covers a single observation set; use
            // a sentinel range that Tick treats as always-flushable.
            self.active_start_ms = ref_ms;
            self.active_end_ms = ref_ms;
            self.initialized = true;
            return;
        }
        // Align to size boundaries so multiple Precompute instances
        // on the same host produce comparable window edges.
        self.active_start_ms = (ref_ms / size) * size;
        self.active_end_ms = self.active_start_ms + size;
        self.initialized = true;
    }

    /// Routes an observation into the window.
    ///
    /// Creates a new series
    /// entry if needed; honors `OnOverflow`. Returns
    /// [`PrecomputeError::SeriesCapExceeded`] or
    /// [`PrecomputeError::LateData`] where applicable.
    pub fn observe(
        &mut self,
        obs: &Observation,
        cfg: &PrecomputeConfig,
        sketch_factory: &SketchFactory,
        observer: &BoxedObserver,
        stats: &mut StatsSnapshot,
    ) -> Result<(), PrecomputeError> {
        self.init_window(obs.timestamp_ms, cfg);

        // Late-data check.
        if cfg.window.allowed_lateness > Duration::ZERO {
            let lateness_ms = cfg.window.allowed_lateness.as_millis() as u64;
            if obs.timestamp_ms + lateness_ms < self.active_start_ms {
                return Err(PrecomputeError::LateData);
            }
        }

        let key = cfg.series_key_for(obs);

        if !self.series.contains_key(&key) {
            // New series — check cap.
            if cfg.max_series > 0 && self.series.len() as u64 >= cfg.max_series {
                match cfg.on_overflow {
                    OnOverflow::Drop | OnOverflow::Block => {
                        // Block degrades to Drop: latency-hostile
                        // semantics belong to integration tests, not
                        // the runtime hot path.
                        return Err(PrecomputeError::SeriesCapExceeded);
                    }
                    OnOverflow::EvictOldest => {
                        // Find and evict the oldest series.
                        let mut oldest_key: Option<String> = None;
                        let mut oldest_ms: u64 = u64::MAX;
                        for (k, e) in self.series.iter() {
                            if e.last_seen_ms < oldest_ms {
                                oldest_ms = e.last_seen_ms;
                                oldest_key = Some(k.clone());
                            }
                        }
                        if let Some(k) = oldest_key {
                            self.series.remove(&k);
                            stats.active_series -= 1;
                        }
                    }
                }
            }
            let sketch = sketch_factory();
            // Honor parity-mode flags by stripping the labels we
            // promised not to surface. GlobalAggregation collapses
            // everything; OmitResourceAttrs zeroes only the resource
            // segment.
            let (resource_copy, labels_copy) = if cfg.global_aggregation {
                (Vec::new(), Vec::new())
            } else if cfg.omit_resource_attrs {
                (Vec::new(), obs.labels.clone())
            } else {
                (obs.resource_labels.clone(), obs.labels.clone())
            };
            let entry = SeriesEntry {
                sketch,
                resource_labels: resource_copy,
                labels: labels_copy,
                last_seen_ms: obs.timestamp_ms,
                count: 0,
            };
            self.series.insert(key.clone(), entry);
            stats.active_series += 1;
        } else if let Some(entry) = self.series.get_mut(&key) {
            if obs.timestamp_ms > entry.last_seen_ms {
                entry.last_seen_ms = obs.timestamp_ms;
            }
        }

        let entry = self
            .series
            .get_mut(&key)
            .expect("series entry must exist after insert");
        observer.observe(entry.sketch.as_mut(), obs)?;
        entry.count += 1;
        Ok(())
    }

    /// Applies an inbound envelope to the appropriate series via
    /// the sketch's `apply_delta` or `merge` depending on encoding.
    ///
    /// Strategy A enforcement: inbound envelopes are merged
    /// into the local sketch as sketches, never expanded to scalar
    /// samples.
    pub fn observe_envelope(
        &mut self,
        env: &SketchEnvelope,
        cfg: &PrecomputeConfig,
        sketch_factory: &SketchFactory,
        snapshot_cache: &SnapshotCache,
        stats: &mut StatsSnapshot,
    ) -> Result<(), PrecomputeError> {
        // Use the envelope's window-end as the reference timestamp;
        // this lets a fresh Precompute initialize its window aligned
        // with the upstream sender.
        let ref_ms = if env.window_end_ms != 0 {
            env.window_end_ms
        } else {
            env.window_start_ms
        };
        self.init_window(ref_ms, cfg);

        // Envelopes carry a single flat labels list (the upstream
        // sender already collapsed any resource/datapoint
        // distinction), so resource labels are empty in this path.
        // We still route through series_key_for_entry so
        // GlobalAggregation collapses inbound envelopes into the
        // same global bucket as the scalar path.
        let key = cfg.series_key_for_entry(&[], &env.labels);

        if !self.series.contains_key(&key) {
            if cfg.max_series > 0
                && self.series.len() as u64 >= cfg.max_series
                && matches!(cfg.on_overflow, OnOverflow::Drop | OnOverflow::Block)
            {
                return Err(PrecomputeError::SeriesCapExceeded);
            }
            let sketch = sketch_factory();
            let entry = SeriesEntry {
                sketch,
                resource_labels: Vec::new(),
                labels: env.labels.clone(),
                last_seen_ms: ref_ms,
                count: 0,
            };
            self.series.insert(key.clone(), entry);
            stats.active_series += 1;
        }

        let entry = self
            .series
            .get_mut(&key)
            .expect("series entry must exist after insert");

        match env.encoding {
            Encoding::ProtoDelta => {
                // Delta apply path: feed the delta bytes directly
                // into the sketch; the wrapper knows the on-the-wire
                // delta format.
                entry.sketch.apply_delta(&env.payload)?;
                // Reconstruct the new full snapshot for cached
                // inbound use.
                if let Ok(snap) = entry.sketch.snapshot() {
                    snapshot_cache.cache_inbound(&key, &snap);
                }
            }
            Encoding::ProtoFull | Encoding::Msgpack | Encoding::Unspecified => {
                // Full-state path: deserialize into a temporary
                // sketch and merge. The Layer-3 runtime doesn't hold
                // a deserialize hook (those are sketch-specific); we
                // go through the SketchFactory + ApplyDelta-as-merge
                // convention.
                let mut other = sketch_factory();
                other.apply_delta(&env.payload)?;
                entry.sketch.merge(other.as_ref())?;
                snapshot_cache.cache_inbound(&key, &env.payload);
            }
        }
        // Carry the upstream envelope's observation count into our
        // running entry so the next emission reflects the merged
        // total. Envelopes with count==0 contribute zero.
        entry.count += env.count;
        Ok(())
    }

    /// Atomically drains the active window and returns the
    /// closed-window series for emission.
    ///
    /// For tumbling, rotation
    /// triggers when `now_ms >= active_end_ms`. Returns
    /// `(closed_series, [start, end))`. If the active window isn't
    /// yet due, returns an empty `Vec`.
    pub fn rotate(&mut self, now_ms: u64, cfg: &PrecomputeConfig) -> (Vec<SeriesEntry>, [u64; 2]) {
        if !self.initialized {
            return (Vec::new(), [0, 0]);
        }
        let size = window_size_ms(cfg);
        if size > 0 && now_ms < self.active_end_ms {
            // Window not yet due.
            return (Vec::new(), [0, 0]);
        }
        self.rotate_locked(now_ms, cfg)
    }

    /// Unconditionally rotates the active window regardless of
    /// wall-clock time. Used by `Precompute::drain` on shutdown
    /// paths.
    ///
    /// When the active window is already empty `drain` is a no-op.
    pub fn drain(&mut self, cfg: &PrecomputeConfig) -> (Vec<SeriesEntry>, [u64; 2]) {
        if !self.initialized {
            return (Vec::new(), [0, 0]);
        }
        if self.series.is_empty() {
            return (Vec::new(), [0, 0]);
        }
        // Hand a "now" pegged to the active end so advance_window
        // snaps the next window forward by exactly one size — the
        // same boundary Tick would have used had it fired naturally.
        let active_end = self.active_end_ms;
        self.rotate_locked(active_end, cfg)
    }

    /// Shared rotation body for [`Self::rotate`] and [`Self::drain`].
    /// Captures the active series, resets the map, and advances the
    /// window bounds.
    fn rotate_locked(
        &mut self,
        now_ms: u64,
        cfg: &PrecomputeConfig,
    ) -> (Vec<SeriesEntry>, [u64; 2]) {
        if self.series.is_empty() {
            // Slide the window forward but emit nothing.
            self.advance_window(now_ms, cfg);
            return (Vec::new(), [0, 0]);
        }

        let rng = [self.active_start_ms, self.active_end_ms];
        // Drain all entries.
        let map = std::mem::take(&mut self.series);
        let closed: Vec<SeriesEntry> = map.into_values().collect();
        self.advance_window(now_ms, cfg);
        (closed, rng)
    }

    /// Rolls the active window bounds forward.
    ///
    /// Tumbling: when `now_ms` is at least one full window past
    /// `active_end_ms`, jump to the bucket containing `now_ms` to
    /// avoid churning through many empty windows. Otherwise advance
    /// by one size.
    ///
    /// Batch: collapses to a no-op since size is zero.
    fn advance_window(&mut self, now_ms: u64, cfg: &PrecomputeConfig) {
        let size = window_size_ms(cfg);
        if size == 0 {
            // Batch / unsized — use the latest observation timestamp
            // as the new window start.
            self.active_start_ms = now_ms;
            self.active_end_ms = now_ms;
            return;
        }
        // Snap to the bucket containing now_ms to avoid lock-step
        // churn after long idle gaps.
        let bucket_start = (now_ms / size) * size;
        if bucket_start <= self.active_start_ms {
            // Defensive: at minimum move forward by one window.
            self.active_start_ms = self.active_end_ms;
        } else {
            self.active_start_ms = bucket_start;
        }
        self.active_end_ms = self.active_start_ms + size;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_starts_empty_and_uninitialized() {
        let w = WindowState::new();
        assert_eq!(w.active_series_count(), 0);
        assert!(!w.initialized);
        assert_eq!(w.active_start_ms, 0);
        assert_eq!(w.active_end_ms, 0);
    }

    #[test]
    fn init_window_aligns_to_size_boundary() {
        let mut w = WindowState::new();
        let cfg = PrecomputeConfig {
            window: crate::config::WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        w.init_window(15_000, &cfg);
        assert!(w.initialized);
        assert_eq!(w.active_start_ms, 10_000);
        assert_eq!(w.active_end_ms, 20_000);
    }
}
