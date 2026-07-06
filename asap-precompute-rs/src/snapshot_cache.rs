//! Per-series sketch payload cache for delta encoding (outbound) and
//! inbound delta-apply against cached upstream snapshots.
//!
//! Mirrors today's per-processor `snapshots map[string][]byte`
//! (outbound) + `IngestState::sketch_snapshots` (inbound).

use std::collections::HashMap;
use std::sync::RwLock;

use crate::precompute::{DeltaResult, PrecomputeError, Sketch};

/// Stores per-series sketch payloads for delta encoding (outbound)
/// and inbound delta-apply against cached upstream snapshots.
///
/// Two maps avoid cross-direction collisions: the same series key
/// may be both produced (outbound) and consumed (inbound) by the
/// same host when running as a forwarder, and the two byte streams
/// are not interchangeable (a remote sender's snapshot is not what
/// we'd emit locally).
///
/// **Always-refresh policy**: every call to
/// [`Self::compute_delta`] updates the cached previous snapshot to
/// the current sketch state. Successive sub-threshold deltas are
/// therefore each computed against the immediately preceding
/// window, matching the established behavior of all five legacy
/// OTel sketch processors (DDSketch / KLL / HLL / CountSketch /
/// CountMinSketch). There is no configurable
/// "refresh-only-on-full" mode — that earlier design was a bug
/// because it forced downstream consumers to merge a chain of
/// deltas back to the original baseline rather than apply each
/// delta to the previous window's reconstructed state.
pub struct SnapshotCache {
    inner: RwLock<SnapshotCacheInner>,
}

struct SnapshotCacheInner {
    outbound: HashMap<String, Vec<u8>>,
    inbound: HashMap<String, Vec<u8>>,
}

impl Default for SnapshotCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotCache {
    /// Constructs an empty cache.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SnapshotCacheInner {
                outbound: HashMap::new(),
                inbound: HashMap::new(),
            }),
        }
    }

    /// Stores the latest full sketch payload, keyed by `series_key`.
    /// Returns `true` if this is the first snapshot for that key
    /// (caller can use this to force a [`crate::envelope::Encoding::ProtoFull`]
    /// on next emit).
    ///
    /// Stores a defensive copy of `payload` so a caller mutating its
    /// slice after caching does not race the cache reader.
    pub fn cache_outbound(&self, series_key: &str, payload: &[u8]) -> bool {
        let mut g = self.inner.write().expect("snapshot cache poisoned");
        let first_time = !g.outbound.contains_key(series_key);
        g.outbound.insert(series_key.to_string(), payload.to_vec());
        first_time
    }

    /// Returns the cached outbound payload, or `None`.
    pub fn get_outbound(&self, series_key: &str) -> Option<Vec<u8>> {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .outbound
            .get(series_key)
            .cloned()
    }

    /// Stores an upstream snapshot for delta apply.
    pub fn cache_inbound(&self, series_key: &str, payload: &[u8]) {
        let mut g = self.inner.write().expect("snapshot cache poisoned");
        g.inbound.insert(series_key.to_string(), payload.to_vec());
    }

    /// Returns the cached upstream snapshot or `None`.
    pub fn get_inbound(&self, series_key: &str) -> Option<Vec<u8>> {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .inbound
            .get(series_key)
            .cloned()
    }

    /// Diffs current sketch state against the cached outbound
    /// snapshot for `series_key`.
    ///
    /// Returns a [`DeltaResult`] where `is_full` means the runtime
    /// should emit `ProtoFull` (either no prior snapshot existed,
    /// or the delta exceeded `threshold`).
    ///
    /// Always-refresh: every call updates the cached previous
    /// snapshot to the current sketch state. When `is_full=true`
    /// the wire payload IS the full snapshot, so it is reused for
    /// the cache; otherwise a fresh full snapshot is serialized
    /// for the cache. Both branches end with the cache holding
    /// the latest full state.
    pub fn compute_delta(
        &self,
        series_key: &str,
        current: &dyn Sketch,
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        let prev = self
            .inner
            .read()
            .expect("snapshot cache poisoned")
            .outbound
            .get(series_key)
            .cloned();

        let result = match prev {
            None => {
                // First time — emit full.
                let full = current.snapshot()?;
                DeltaResult {
                    payload: full,
                    is_full: true,
                }
            }
            Some(prev_bytes) => current.compute_delta_against(&prev_bytes, threshold)?,
        };

        // Refresh the cached outbound base for the NEXT window's delta.
        //
        // Per-window deltas:
        // families that opt in via `Sketch::delta_against_empty_base`
        // (DDSketch / CMS / CountSketch / HLL) reset the cached base to the
        // EMPTY-sketch snapshot after each window-close emit, so the next
        // window diffs against empty and transmits its OWN per-window state
        // as a delta (no cross-window subtraction). This is what makes the
        // downstream per-window base rotation correct.
        //
        // Legacy always-refresh (KLL, and any family that returns the
        // `None` default):
        // update the cached base to the just-emitted full snapshot, so the
        // next delta is computed against the immediately preceding window.
        // When is_full=true the wire payload IS the snapshot; reuse it.
        // Otherwise serialize a fresh snapshot.
        match current.delta_against_empty_base()? {
            Some(empty_base) => {
                self.cache_outbound(series_key, &empty_base);
            }
            None => {
                if result.is_full {
                    self.cache_outbound(series_key, &result.payload);
                } else {
                    let full = current.snapshot()?;
                    self.cache_outbound(series_key, &full);
                }
            }
        }
        Ok(result)
    }

    /// Clears all cached state. Used in tests and on shutdown.
    pub fn reset(&self) {
        let mut g = self.inner.write().expect("snapshot cache poisoned");
        g.outbound.clear();
        g.inbound.clear();
    }

    /// Returns the number of cached outbound snapshots.
    pub fn len_outbound(&self) -> usize {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .outbound
            .len()
    }

    /// Returns the number of cached inbound snapshots.
    pub fn len_inbound(&self) -> usize {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .inbound
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_first_time_true_then_false() {
        let c = SnapshotCache::new();
        assert!(c.cache_outbound("series-1", &[1, 2, 3]));
        assert!(!c.cache_outbound("series-1", &[4, 5, 6]));
        assert_eq!(c.len_outbound(), 1);
        assert_eq!(c.get_outbound("series-1"), Some(vec![4, 5, 6]));
    }

    #[test]
    fn inbound_independent_from_outbound() {
        let c = SnapshotCache::new();
        c.cache_outbound("k", b"out");
        c.cache_inbound("k", b"in");
        assert_eq!(c.get_outbound("k"), Some(b"out".to_vec()));
        assert_eq!(c.get_inbound("k"), Some(b"in".to_vec()));
        assert_eq!(c.len_outbound(), 1);
        assert_eq!(c.len_inbound(), 1);
    }

    #[test]
    fn reset_clears_all() {
        let c = SnapshotCache::new();
        c.cache_outbound("k", b"v");
        c.cache_inbound("k", b"v");
        c.reset();
        assert_eq!(c.len_outbound(), 0);
        assert_eq!(c.len_inbound(), 0);
    }

    #[test]
    fn cache_stores_defensive_copy() {
        let c = SnapshotCache::new();
        let mut payload = vec![1, 2, 3];
        c.cache_outbound("k", &payload);
        payload[0] = 99;
        // Cache must not have mutated.
        assert_eq!(c.get_outbound("k"), Some(vec![1, 2, 3]));
    }

    // ---- per-window delta tests (DDSketch) ----

    use crate::precompute::QuantileSketch;
    use crate::sketches::ddsketch::DDSketchWrapper;

    /// Build a fresh per-window DDSketch wrapper from a value range. The
    /// runtime resets per-series state every window, so each window is an
    /// independent sketch — mirrored here by a fresh wrapper per window.
    fn window_sketch(alpha: f64, values: impl IntoIterator<Item = f64>) -> DDSketchWrapper {
        let mut w = DDSketchWrapper::new(alpha);
        for v in values {
            w.update(v);
        }
        w
    }

    /// With delta mode on, applying the emitted DDSketch delta
    /// to an EMPTY base reconstructs the window's full state, and
    /// quantiles match within the α relative-accuracy bound.
    #[test]
    fn ddsketch_delta_against_empty_round_trips() {
        let alpha = 0.01;
        let c = SnapshotCache::new();

        // Window 1 — first emit is FULL (no prior base).
        let w1 = window_sketch(alpha, (1..=200).map(|i| i as f64));
        let r1 = c.compute_delta("series", &w1, 1).unwrap();
        assert!(r1.is_full, "first window must emit a full frame");

        // Window 2 — a fresh per-window sketch; emit must be a DELTA now
        // (the cache reset the base to empty at window-1 close).
        let w2 = window_sketch(alpha, (1..=200).map(|i| i as f64));
        let r2 = c.compute_delta("series", &w2, 1).unwrap();
        assert!(!r2.is_full, "window 2 must emit a delta, not a full frame");
        assert!(!r2.payload.is_empty(), "delta payload must be non-empty");

        // Apply the delta to an EMPTY base → reconstructs window 2.
        let mut recon = DDSketchWrapper::new(alpha);
        recon.apply_delta(&r2.payload).unwrap();
        assert_eq!(recon.inner().total_count(), w2.inner().total_count());

        // Quantiles match within α.
        for q in [0.5, 0.9, 0.99] {
            let got = recon.quantile(q);
            let want = w2.quantile(q);
            assert!(
                (got / want - 1.0).abs() <= alpha,
                "q={q}: recon={got} want={want}"
            );
        }
    }

    /// Two consecutive windows each emit their OWN state —
    /// window 2's delta is NOT diffed against window 1 (no cross-window
    /// subtraction). Even when window 1's events overlap window 2's, the
    /// emitted delta reconstructs window 2 exactly from empty.
    #[test]
    fn ddsketch_consecutive_windows_no_cross_window_subtraction() {
        let alpha = 0.01;
        let c = SnapshotCache::new();

        // Window 1: each value inserted TWICE (high counts).
        let mut w1 = DDSketchWrapper::new(alpha);
        for i in 1..=100 {
            w1.update(i as f64);
            w1.update(i as f64);
        }
        let r1 = c.compute_delta("s", &w1, 1).unwrap();
        assert!(r1.is_full);

        // Window 2: SAME value range but each inserted ONCE (lower counts).
        // If the cache diffed window 2 against window 1 (cross-window),
        // every bucket delta would saturate to 0 and the emitted delta
        // would be empty — under-transmitting window 2 entirely.
        let mut w2 = DDSketchWrapper::new(alpha);
        for i in 1..=100 {
            w2.update(i as f64);
        }
        let r2 = c.compute_delta("s", &w2, 1).unwrap();
        assert!(!r2.is_full);

        // Reconstruct window 2 from EMPTY — must equal window 2's own
        // full state, proving no cross-window subtraction occurred.
        let mut recon = DDSketchWrapper::new(alpha);
        recon.apply_delta(&r2.payload).unwrap();
        assert_eq!(
            recon.inner().total_count(),
            w2.inner().total_count(),
            "window 2 delta must carry window 2's full count"
        );

        // The reconstructed-from-empty state must match window 2's own
        // full per-bucket counts (compared on absolute bucket index —
        // the reconstructed store's backing-array padding differs from
        // the chunk-grown original, but the logical distribution is
        // identical: delta-against-empty == window 2's own state).
        let w2_sk = w2.inner();
        let r_sk = recon.inner();
        for (i, &c) in w2_sk.store_counts.iter().enumerate() {
            if c == 0 {
                continue;
            }
            let k = w2_sk.store_offset + i as i32;
            let r_idx = (k - r_sk.store_offset) as usize;
            assert_eq!(r_sk.store_counts[r_idx], c, "bucket k={k}");
        }

        // And it must NOT equal window 1's state (which had double counts).
        assert_ne!(recon.inner().total_count(), w1.inner().total_count());
    }

    // ---- per-window delta tests (CMS) ----

    use crate::precompute::FrequencySketch;
    use crate::sketches::cms::CMSWrapper;

    /// With delta mode on, applying the emitted CMS delta to an EMPTY base
    /// reconstructs the window's per-cell matrix, and the estimated
    /// frequency matches.
    #[test]
    fn cms_delta_against_empty_round_trips() {
        let c = SnapshotCache::new();
        let (rows, cols) = (5, 1024);

        // Window 1 — first emit is FULL (no prior base).
        let mut w1 = CMSWrapper::new(rows, cols);
        for _ in 0..200 {
            w1.update("hot", 1.0);
        }
        let r1 = c.compute_delta("series", &w1, 1).unwrap();
        assert!(r1.is_full, "first window must emit a full frame");

        // Window 2 — fresh per-window sketch; emit must be a DELTA now.
        let mut w2 = CMSWrapper::new(rows, cols);
        for _ in 0..200 {
            w2.update("hot", 1.0);
        }
        let r2 = c.compute_delta("series", &w2, 1).unwrap();
        assert!(!r2.is_full, "window 2 must emit a delta, not a full frame");
        assert!(!r2.payload.is_empty(), "delta payload must be non-empty");

        // Apply the delta to an EMPTY base → reconstructs window 2.
        let mut recon = CMSWrapper::new(rows, cols);
        recon.apply_delta(&r2.payload).unwrap();
        let want = w2.estimate_count(b"hot");
        let got = recon.estimate_count(b"hot");
        assert!((got - want).abs() <= want * 0.1, "recon={got} want={want}");
    }

    /// Two consecutive windows each emit their OWN state — window 2's
    /// delta is NOT diffed against window 1. Window 1 counts "k" 300×,
    /// window 2 counts it 50×; reconstructing window 2 from EMPTY yields
    /// ~50, proving no cross-window subtraction.
    #[test]
    fn cms_consecutive_windows_no_cross_window_subtraction() {
        let c = SnapshotCache::new();
        let (rows, cols) = (5, 1024);

        let mut w1 = CMSWrapper::new(rows, cols);
        for _ in 0..300 {
            w1.update("k", 1.0);
        }
        assert!(c.compute_delta("s", &w1, 1).unwrap().is_full);

        let mut w2 = CMSWrapper::new(rows, cols);
        for _ in 0..50 {
            w2.update("k", 1.0);
        }
        let r2 = c.compute_delta("s", &w2, 1).unwrap();
        assert!(!r2.is_full);
        assert!(!r2.payload.is_empty());

        let mut recon = CMSWrapper::new(rows, cols);
        recon.apply_delta(&r2.payload).unwrap();
        let got = recon.estimate_count(b"k");
        let want = w2.estimate_count(b"k");
        assert!((got - want).abs() <= want * 0.1, "got={got} want={want}");
        assert!(got < 150.0, "window 2 leaked window 1's mass: got={got}");
    }

    // ---- per-window delta tests (CountSketch) ----

    use crate::sketches::countsketch::CountSketchWrapper;

    /// With delta mode on, applying the emitted CountSketch delta to an
    /// EMPTY base reconstructs the window's per-cell matrix.
    #[test]
    fn countsketch_delta_against_empty_round_trips() {
        let c = SnapshotCache::new();
        let (rows, cols) = (5, 1024);

        let mut w1 = CountSketchWrapper::new(rows, cols);
        for _ in 0..200 {
            w1.update("hot", 1.0);
        }
        assert!(c.compute_delta("series", &w1, 1).unwrap().is_full);

        let mut w2 = CountSketchWrapper::new(rows, cols);
        for _ in 0..200 {
            w2.update("hot", 1.0);
        }
        let r2 = c.compute_delta("series", &w2, 1).unwrap();
        assert!(!r2.is_full, "window 2 must emit a delta");
        assert!(!r2.payload.is_empty());

        let mut recon = CountSketchWrapper::new(rows, cols);
        recon.apply_delta(&r2.payload).unwrap();
        // CountSketch round-trips its cells exactly under
        // delta-against-empty: matrices must be identical.
        assert_eq!(recon.inner().matrix, w2.inner().matrix);
    }

    /// CountSketch consecutive windows each emit their own state.
    #[test]
    fn countsketch_consecutive_windows_no_cross_window_subtraction() {
        let c = SnapshotCache::new();
        let (rows, cols) = (5, 1024);

        let mut w1 = CountSketchWrapper::new(rows, cols);
        for _ in 0..300 {
            w1.update("k", 1.0);
        }
        assert!(c.compute_delta("s", &w1, 1).unwrap().is_full);

        let mut w2 = CountSketchWrapper::new(rows, cols);
        for _ in 0..50 {
            w2.update("k", 1.0);
        }
        let r2 = c.compute_delta("s", &w2, 1).unwrap();
        assert!(!r2.is_full);

        let mut recon = CountSketchWrapper::new(rows, cols);
        recon.apply_delta(&r2.payload).unwrap();
        // Reconstructed-from-empty matrix equals window 2's own — not
        // window 1's higher-count matrix.
        assert_eq!(recon.inner().matrix, w2.inner().matrix);
        assert_ne!(recon.inner().matrix, w1.inner().matrix);
    }

    // ---- per-window delta tests (HLL) ----

    use crate::precompute::CardinalitySketch;
    use crate::sketches::hll::HLLWrapper;
    use asap_sketchlib::HllVariant as RsHllVariant;

    /// With delta mode on, applying the emitted HLL register delta to an
    /// EMPTY base reconstructs the window's registers exactly, and the
    /// cardinality estimate matches.
    #[test]
    fn hll_delta_against_empty_round_trips() {
        let c = SnapshotCache::new();

        let mut w1 = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..2000u64 {
            w1.update(&i.to_le_bytes());
        }
        assert!(c.compute_delta("series", &w1, 1).unwrap().is_full);

        let mut w2 = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..2000u64 {
            w2.update(&i.to_le_bytes());
        }
        let r2 = c.compute_delta("series", &w2, 1).unwrap();
        assert!(!r2.is_full, "window 2 must emit a delta");
        assert!(!r2.payload.is_empty());

        let mut recon = HLLWrapper::new(RsHllVariant::Regular, 12);
        recon.apply_delta(&r2.payload).unwrap();
        // Register delta over an empty base carries every non-zero
        // register, so the reconstructed registers are identical.
        assert_eq!(recon.inner().registers, w2.inner().registers);
        let want = w2.estimate_cardinality();
        let got = recon.estimate_cardinality();
        assert!((got - want).abs() <= want * 0.001, "got={got} want={want}");
    }

    /// HLL consecutive windows each emit their OWN register state. Window
    /// 1 sees 5000 keys, window 2 a DISJOINT 200 keys. Reconstructing
    /// window 2 from EMPTY must yield ~200, NOT the ~5000-element union —
    /// proving the empty-base reset defeats register-MAX leakage.
    #[test]
    fn hll_consecutive_windows_no_cross_window_subtraction() {
        let c = SnapshotCache::new();

        let mut w1 = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..5000u64 {
            w1.update(&i.to_le_bytes());
        }
        assert!(c.compute_delta("s", &w1, 1).unwrap().is_full);

        let mut w2 = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 1_000_000u64..1_000_200u64 {
            w2.update(&i.to_le_bytes());
        }
        let r2 = c.compute_delta("s", &w2, 1).unwrap();
        assert!(!r2.is_full);

        let mut recon = HLLWrapper::new(RsHllVariant::Regular, 12);
        recon.apply_delta(&r2.payload).unwrap();
        let got = recon.estimate_cardinality();
        let want = w2.estimate_cardinality();
        assert!((got - want).abs() <= want * 0.05, "got={got} want={want}");
        assert!(
            got < 1000.0,
            "window 2 cardinality leaked window 1's registers: got={got}"
        );
    }
}
