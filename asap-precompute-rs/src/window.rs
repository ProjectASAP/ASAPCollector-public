//! Per-[`crate::precompute::Precompute`] window manager.

use std::collections::HashMap;
use std::time::Duration;

use crate::config::{OnOverflow, PrecomputeConfig};
use crate::envelope::SketchEnvelope;
use crate::observation::{KeyValue, Observation};
use crate::precompute::{BoxedObserver, PrecomputeError, Sketch, SketchFactory, StatsSnapshot};

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

/// One closed window's series, ready for emission.
pub struct ClosedWindow {
    /// The window's series.
    pub series: Vec<SeriesEntry>,
    /// Exclusive lower bound of the window (Unix ms).
    pub start_ms: u64,
    /// Inclusive upper bound of the window (Unix ms).
    pub end_ms: u64,
}

/// General tumbling-window manager. Retained KLL panes and sliding
/// estimates use [`crate::kll_windows`] instead.
///
/// Windows follow Prometheus range selectors: a window is `(start, end]`
/// and a sample at `t` belongs to the window ending at [`window_end_ms`].
/// One window is open at a time. It closes when a sample later than its
/// end arrives or when [`Self::tick`] is called past its end (the idle
/// timer); a sample whose window has closed is late.
///
/// Locking is owned by the enclosing [`std::sync::Mutex`] in
/// [`crate::precompute::PrecomputeImpl`]; this struct itself is
/// `!Sync`-by-content (HashMap of boxed dyn Sketch) and relies on the
/// outer mutex to serialize access.
pub struct WindowState {
    /// Map from series-key (see [`crate::matchers::series_key`]) to
    /// the open window's series entry.
    pub(crate) series: HashMap<String, SeriesEntry>,
    /// `[start_ms, end_ms]` of the open window `(start, end]`;
    /// `None` until a sample opens one.
    pub(crate) open_window_ms: Option<[u64; 2]>,
    /// Every window ending at or before this has closed (Unix ms).
    pub(crate) closed_through_ms: u64,
    /// Windows closed since the last [`Self::tick`] or [`Self::drain`].
    closed: Vec<ClosedWindow>,
    /// Reused buffer for the series key of the observation being routed.
    key: String,
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

/// End of the window `(end - size_ms, end]` holding a sample at
/// `timestamp_ms`: `⌈timestamp_ms / size_ms⌉ · size_ms`, as a Prometheus
/// range selector ending there selects it. `size_ms` must be nonzero.
pub fn window_end_ms(timestamp_ms: u64, size_ms: u64) -> u64 {
    timestamp_ms.div_ceil(size_ms) * size_ms
}

impl WindowState {
    /// Constructs an empty window.
    pub fn new() -> Self {
        Self {
            series: HashMap::new(),
            open_window_ms: None,
            closed_through_ms: 0,
            closed: Vec::new(),
            key: String::new(),
        }
    }

    /// Returns the current series count.
    pub fn active_series_count(&self) -> usize {
        self.series.len()
    }

    /// Makes the window holding `timestamp_ms` the open one, closing the
    /// open window if the timestamp is past its end. Returns
    /// [`PrecomputeError::LateData`] when that window has already closed.
    ///
    /// Batch (unsized) configs keep one window until the next tick.
    fn open_window_for(
        &mut self,
        timestamp_ms: u64,
        cfg: &PrecomputeConfig,
    ) -> Result<(), PrecomputeError> {
        let size = window_size_ms(cfg);
        if size == 0 {
            self.open_window_ms
                .get_or_insert([timestamp_ms, timestamp_ms]);
            return Ok(());
        }
        if timestamp_ms <= self.closed_through_ms {
            return Err(PrecomputeError::LateData);
        }
        if self
            .open_window_ms
            .is_some_and(|[_, end]| timestamp_ms <= end)
        {
            return Ok(());
        }
        self.close_open_window();
        let end = window_end_ms(timestamp_ms, size);
        let start = end - size;
        // A sample past `start` closes every window ending at or before it.
        self.closed_through_ms = start;
        self.open_window_ms = Some([start, end]);
        Ok(())
    }

    /// Moves the open window, if any, to the closed list.
    fn close_open_window(&mut self) {
        let Some([start_ms, end_ms]) = self.open_window_ms.take() else {
            return;
        };
        self.closed_through_ms = self.closed_through_ms.max(end_ms);
        if self.series.is_empty() {
            return;
        }
        let series = self.series.drain().map(|(_, entry)| entry).collect();
        self.closed.push(ClosedWindow {
            series,
            start_ms,
            end_ms,
        });
    }

    /// Routes an observation into its window.
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
        self.open_window_for(obs.timestamp_ms, cfg)?;

        cfg.write_series_key_for(&mut self.key, obs);
        if let Some(entry) = self.series.get_mut(self.key.as_str()) {
            if obs.timestamp_ms > entry.last_seen_ms {
                entry.last_seen_ms = obs.timestamp_ms;
            }
            observer.observe(entry.sketch.as_mut(), obs)?;
            entry.count += 1;
            return Ok(());
        }
        let key = self.key.clone();

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
        stats.active_series += 1;
        let entry = self.series.entry(key).or_insert(entry);
        observer.observe(entry.sketch.as_mut(), obs)?;
        entry.count += 1;
        Ok(())
    }

    /// Merges an inbound envelope's ASAPv1 payload into the matching
    /// series of the window holding the envelope's end, under the same
    /// rules as a sample at that time. Envelopes are merged as sketches,
    /// never expanded to scalar samples.
    pub fn observe_envelope(
        &mut self,
        env: &SketchEnvelope,
        cfg: &PrecomputeConfig,
        sketch_factory: &SketchFactory,
        stats: &mut StatsSnapshot,
    ) -> Result<(), PrecomputeError> {
        // An upstream `(start, end]` lies in our window holding `end`
        // when our window size is a multiple of the upstream one.
        let ref_ms = if env.window_end_ms != 0 {
            env.window_end_ms
        } else {
            env.window_start_ms
        };
        self.open_window_for(ref_ms, cfg)?;

        // Native OTAP envelopes preserve resource and data-point labels
        // separately. Keep both in the series identity across merge stages.
        let key = cfg.series_key_for_entry(&env.resource_labels, &env.labels);

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
                resource_labels: env.resource_labels.clone(),
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

        entry.sketch.merge_snapshot(&env.payload)?;
        // Carry the upstream envelope's observation count into our
        // running entry so the next emission reflects the merged
        // total. Envelopes with count==0 contribute zero.
        entry.count += env.count;
        Ok(())
    }

    /// Returns the windows closed since the last call: those a later
    /// sample closed, plus the open window when `now_ms` is past its end
    /// (`now_ms > end`: a sample at `end` still belongs to it). Unsized
    /// (Batch) configs close the open window on every tick.
    pub fn tick(&mut self, now_ms: u64, cfg: &PrecomputeConfig) -> Vec<ClosedWindow> {
        let size = window_size_ms(cfg);
        if self
            .open_window_ms
            .is_some_and(|[_, end]| size == 0 || now_ms > end)
        {
            self.close_open_window();
        }
        std::mem::take(&mut self.closed)
    }

    /// Closes the open window regardless of time and returns every
    /// closed window. Used by `Precompute::drain` on shutdown paths; a
    /// later sample in the drained window is late, as after any close.
    pub fn drain(&mut self) -> Vec<ClosedWindow> {
        self.close_open_window();
        std::mem::take(&mut self.closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_starts_empty_and_closed() {
        let w = WindowState::new();
        assert_eq!(w.active_series_count(), 0);
        assert_eq!(w.open_window_ms, None);
        assert_eq!(w.closed_through_ms, 0);
    }

    #[test]
    fn window_end_is_the_next_boundary_at_or_after_the_sample() {
        assert_eq!(window_end_ms(15_000, 10_000), 20_000);
        assert_eq!(window_end_ms(20_000, 10_000), 20_000);
        assert_eq!(window_end_ms(20_001, 10_000), 30_000);
    }

    #[test]
    fn first_sample_opens_the_window_ending_at_or_after_it() {
        let mut w = WindowState::new();
        let cfg = PrecomputeConfig {
            window: crate::config::WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        w.open_window_for(15_000, &cfg).unwrap();
        assert_eq!(w.open_window_ms, Some([10_000, 20_000]));
        assert_eq!(w.closed_through_ms, 10_000);
    }
}
