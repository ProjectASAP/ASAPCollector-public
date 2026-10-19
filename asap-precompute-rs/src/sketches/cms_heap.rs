//! Count-Min sketch with a top-k heap, over [`asap_sketchlib::CMSHeap`].
//!
//! Implements [`Sketch`] + [`FrequencySketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope (kind `0x03 0x00`): the counter matrix plus the heap's
//! keys and counts. There is no delta encoding; every payload is full state.

use std::any::Any;

use asap_sketchlib::{CMSHeap, DataInput, HeapItem};

use super::cms::clamp_rows_for_hash_bits;
use crate::precompute::{DeltaResult, FrequencyEntry, FrequencySketch, PrecomputeError, Sketch};

/// Count-Min sketch plus heavy-hitter heap. Keys are raw bytes.
pub struct CMSHeapWrapper {
    sk: CMSHeap,
}

impl CMSHeapWrapper {
    /// Construct with `top_k` heap slots. `cols` is rounded up to a power of
    /// two and `rows` clamped to the 64-bit hash budget, as for
    /// [`super::CMSWrapper`].
    pub fn new(rows: usize, cols: usize, top_k: usize) -> Self {
        let cols = cols.max(1).next_power_of_two();
        let rows = clamp_rows_for_hash_bits(rows, cols);
        Self {
            sk: CMSHeap::new(rows, cols, top_k.max(1)),
        }
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        CMSHeap::deserialize_from_bytes(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("CMSHeapWrapper decode: {e}")))
    }

    /// Add `count` occurrences of `key` and refresh its heap entry.
    pub fn update(&mut self, key: &[u8], count: i64) {
        self.sk.insert_many(&DataInput::Bytes(key), count);
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
    pub fn inner(&self) -> &CMSHeap {
        &self.sk
    }
}

impl Sketch for CMSHeapWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.heap().is_empty() {
            return Ok(Vec::new());
        }
        self.sk
            .serialize_to_bytes()
            .map_err(|e| PrecomputeError::Other(format!("CMSHeapWrapper snapshot: {e}")))
    }

    fn compute_delta_against(&self, _: &[u8], _: u64) -> Result<DeltaResult, PrecomputeError> {
        Ok(DeltaResult {
            payload: self.snapshot()?,
            is_full: true,
        })
    }

    fn apply_delta(&mut self, payload: &[u8]) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        self.merge_from(&Self::from_snapshot(payload)?)
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        self.apply_delta(&other.snapshot()?)
    }

    fn reset(&mut self) {
        self.sk = CMSHeap::new(self.sk.rows(), self.sk.cols(), self.sk.heap().capacity());
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
                HeapItem::Bytes(key) => Some(FrequencyEntry {
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
    fn empty_sketch_has_empty_snapshot() {
        assert!(CMSHeapWrapper::new(4, 64, 2).snapshot().unwrap().is_empty());
    }
}
