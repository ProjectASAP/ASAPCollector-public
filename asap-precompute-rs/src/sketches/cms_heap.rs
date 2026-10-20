//! Count-Min sketch with a top-k heap, over [`asap_sketchlib::CountMinSketchWithHeap`].
//!
//! Implements [`Sketch`] + [`FrequencySketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope (kind `0x03 0x00`): the counter matrix plus the heap's
//! keys and counts.

use std::any::Any;

use asap_sketchlib::{CountMinSketchWithHeap, DataInput, OwnedDataInput, MATRIX_MAX_ROWS};

use crate::observation::{KeyValue, Observation};
use crate::precompute::{
    EstimatePoint, FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver,
};

/// Count-Min sketch plus heavy-hitter heap. Keys are raw bytes.
pub struct CMSHeapWrapper {
    sk: CountMinSketchWithHeap,
}

impl CMSHeapWrapper {
    /// Construct with `top_k` heap slots; `rows` is clamped to
    /// `1..=MATRIX_MAX_ROWS` as for [`super::CMSWrapper`].
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        Self {
            sk: CountMinSketchWithHeap::new(
                rows.clamp(1, MATRIX_MAX_ROWS),
                cols.max(1),
                top_k.max(1),
            ),
        }
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        CountMinSketchWithHeap::from_asapv1(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("CMSHeapWrapper decode: {e}")))
    }

    /// Add `count` occurrences of `key` and refresh its heap entry.
    pub fn update(&mut self, key: &[u8], count: i64) {
        self.sk.update_weighted(&DataInput::Bytes(key), count);
    }

    /// Merge counters, then re-rank both heaps' candidates on the merged counters.
    pub fn merge_from(&mut self, other: &Self) -> Result<(), PrecomputeError> {
        if (self.sk.rows(), self.sk.cols()) != (other.sk.rows(), other.sk.cols()) {
            return Err(PrecomputeError::Other(
                "CMSHeapWrapper merge: dimension mismatch".into(),
            ));
        }
        self.sk.merge(&other.sk);
        Ok(())
    }

    /// Borrow the underlying sketch.
    pub fn inner(&self) -> &CountMinSketchWithHeap {
        &self.sk
    }
}

impl Sketch for CMSHeapWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.heap().is_empty() {
            return Ok(Vec::new());
        }
        self.sk
            .to_asapv1()
            .map_err(|e| PrecomputeError::Other(format!("CMSHeapWrapper snapshot: {e}")))
    }

    fn merge_snapshot(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.merge_from(&Self::from_snapshot(bytes)?)
    }

    fn reset(&mut self) {
        self.sk =
            CountMinSketchWithHeap::new(self.sk.rows(), self.sk.cols(), self.sk.heap().capacity());
    }

    /// One point per heap entry (the configured `k`), labelled with its
    /// `key` and 1-based `rank`.
    fn estimate(&self, _quantiles: &[f64], _top_k: usize) -> Vec<EstimatePoint> {
        self.top_k(self.sk.heap().capacity())
            .into_iter()
            .enumerate()
            .map(|(rank, entry)| EstimatePoint {
                labels: vec![
                    KeyValue::new("key", String::from_utf8_lossy(&entry.key).into_owned()),
                    KeyValue::new("rank", (rank + 1).to_string()),
                ],
                value: entry.count,
            })
            .collect()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl FrequencySketch for CMSHeapWrapper {
    fn estimate_count(&self, key: &[u8]) -> f64 {
        self.sk.estimate(&DataInput::Bytes(key)) as f64
    }

    /// Heap entries, largest count first.
    fn top_k(&self, k: usize) -> Vec<FrequencyEntry> {
        let mut entries = self
            .sk
            .heap()
            .heap()
            .iter()
            .filter_map(|item| match &item.key {
                OwnedDataInput::Bytes(key) => Some(FrequencyEntry {
                    key: key.clone(),
                    count: item.count as f64,
                }),
                _ => None,
            })
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| b.count.total_cmp(&a.count).then_with(|| a.key.cmp(&b.key)));
        entries.truncate(k);
        entries
    }
}

/// Observer counting each observation's key once into a [`CMSHeapWrapper`].
///
/// The key is the observation's `bytes` value when present, otherwise its
/// full label set via [`crate::matchers::attributes_key`], as for
/// [`super::CMSObserver`].
pub struct CMSHeapObserver;

impl SketchObserver for CMSHeapObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<CMSHeapWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("CMSHeapObserver: sketch is not a CMSHeapWrapper".into())
            })?;
        if obs.value.bytes.is_empty() {
            w.update(
                crate::matchers::attributes_key(&obs.labels, &[]).as_bytes(),
                1,
            );
        } else {
            w.update(&obs.value.bytes, 1);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed(counts: &[(u64, i64)]) -> CMSHeapWrapper {
        let mut sk = CMSHeapWrapper::new(4, 2048, 3);
        for (key, count) in counts {
            sk.update(&key.to_be_bytes(), *count);
        }
        sk
    }

    fn keys(sk: &CMSHeapWrapper) -> Vec<u64> {
        sk.top_k(3)
            .iter()
            .map(|e| u64::from_be_bytes(e.key.as_slice().try_into().unwrap()))
            .collect()
    }

    #[test]
    fn top_k_orders_heavy_hitters() {
        let sk = keyed(&[(1, 5), (2, 50), (3, 20), (4, 1), (5, 30)]);
        assert_eq!(keys(&sk), vec![2, 5, 3]);
        assert_eq!(sk.estimate_count(&2u64.to_be_bytes()), 50.0);
    }

    #[test]
    fn snapshot_round_trips_through_asapv1() {
        let sk = keyed(&[(7, 9), (8, 4)]);
        let bytes = sk.snapshot().unwrap();
        let decoded = CMSHeapWrapper::from_snapshot(&bytes).unwrap();
        assert_eq!(decoded.top_k(3), sk.top_k(3));
        assert_eq!(decoded.snapshot().unwrap(), bytes);
    }

    #[test]
    fn merge_reranks_candidates_from_both_heaps() {
        let mut left = keyed(&[(1, 10), (2, 9), (3, 8), (4, 7)]);
        let right = keyed(&[(4, 7), (5, 1)]);
        left.merge(&right).unwrap();
        assert_eq!(keys(&left), vec![4, 1, 2]);
        assert_eq!(left.estimate_count(&4u64.to_be_bytes()), 14.0);
    }

    #[test]
    fn estimate_emits_ranked_heavy_hitters() {
        let points = keyed(&[(1, 5), (2, 50), (3, 20)]).estimate(&[], 0);
        assert_eq!(points.len(), 3);
        assert_eq!(points[0].value, 50.0);
        assert_eq!(points[0].labels[1], KeyValue::new("rank", "1"));
    }

    #[test]
    fn empty_sketch_has_empty_snapshot() {
        assert!(CMSHeapWrapper::new(4, 64, 2).snapshot().unwrap().is_empty());
    }
}
