//! KLL wrapper over [`asap_sketchlib::KLL`].
//!
//! Implements [`Sketch`] + [`QuantileSketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope. KLL uses random compaction, so the wrapper takes an
//! optional seed for deterministic compaction.

use asap_sketchlib::sketches::KLL;

use crate::observation::{KeyValue, Observation};
use crate::precompute::{EstimatePoint, PrecomputeError, QuantileSketch, Sketch, SketchObserver};

/// KLL wrapper over `asap_sketchlib::KLL<f64>`.
pub struct KLLWrapper {
    sk: KLL<f64>,
}

impl KLLWrapper {
    /// Construct an empty KLL with accuracy parameter `k`.
    /// `seed = Some(s)` enables deterministic compaction.
    pub fn new(k: i32, seed: Option<u64>) -> Self {
        let sk = match seed {
            Some(s) => KLL::init_kll_with_seed(k, s),
            None => KLL::init_kll(k),
        };
        Self { sk }
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        KLL::deserialize_from_bytes(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("KLLWrapper decode: {e}")))
    }

    /// Insert a single observation. Non-finite values are ignored.
    pub fn update(&mut self, value: f64) {
        if value.is_finite() {
            self.sk.update(&value);
        }
    }

    /// Insert a batch of observations.
    pub fn update_batch<I>(&mut self, values: I)
    where
        I: IntoIterator<Item = f64>,
    {
        for value in values {
            self.update(value);
        }
    }

    /// Borrow the underlying `KLL`.
    pub fn inner(&self) -> &KLL<f64> {
        &self.sk
    }
}

impl Sketch for KLLWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.count() == 0 {
            return Ok(Vec::new());
        }
        self.sk
            .serialize_to_bytes()
            .map_err(|e| PrecomputeError::Other(format!("KLLWrapper snapshot: {e}")))
    }

    /// Uses sketchlib's level-aware merge so each retained item keeps its
    /// `2^level` weight after compaction.
    fn merge_snapshot(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.sk.merge(&Self::from_snapshot(bytes)?.sk);
        Ok(())
    }

    fn reset(&mut self) {
        self.sk.clear();
    }

    fn estimate(&self, quantiles: &[f64], _top_k: usize) -> Vec<EstimatePoint> {
        if self.sk.count() == 0 {
            return Vec::new();
        }
        quantiles
            .iter()
            .map(|&q| EstimatePoint {
                labels: vec![KeyValue::new("quantile", format!("{q}"))],
                value: QuantileSketch::quantile(self, q),
            })
            .collect()
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl QuantileSketch for KLLWrapper {
    fn quantile(&self, q: f64) -> f64 {
        if self.sk.count() == 0 {
            return f64::NAN;
        }
        self.sk.quantile(q.clamp(0.0, 1.0))
    }
}

/// Observer routing `Float`-kind observations into a [`KLLWrapper`].
pub struct KLLObserver;

impl SketchObserver for KLLObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<KLLWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("KLLObserver: sketch is not a KLLWrapper".into())
            })?;
        match obs.value.kind {
            crate::observation::ObservationValueKind::Float => {
                w.update(obs.value.float);
                Ok(())
            }
            other => Err(PrecomputeError::Other(format!(
                "KLLObserver: unsupported value kind {}",
                other.name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        assert!(KLLWrapper::new(200, Some(0xDEAD))
            .snapshot()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn update_then_quantile() {
        let mut w = KLLWrapper::new(200, Some(0xDEAD));
        w.update_batch((1..=100).map(f64::from));
        let p50 = w.quantile(0.5);
        assert!((40.0..=60.0).contains(&p50), "p50={p50}");
    }

    #[test]
    fn snapshot_round_trips_through_asapv1() {
        let mut w = KLLWrapper::new(200, Some(0xDEAD));
        w.update_batch((1..=10).map(f64::from));
        let bytes = w.snapshot().unwrap();
        let decoded = KLLWrapper::from_snapshot(&bytes).unwrap();
        assert_eq!(decoded.inner().count(), 10);
    }

    #[test]
    fn compacted_state_stays_small() {
        let mut w = KLLWrapper::new(200, Some(42));
        w.update_batch((0..20_000).map(f64::from));
        // The compactor reports retained weight, not the exact raw input count.
        assert!(w.inner().count().abs_diff(20_000) < 400);
        assert!(w.snapshot().unwrap().len() < 20_000);
    }

    #[test]
    fn deterministic_seed_yields_identical_snapshot() {
        let mut a = KLLWrapper::new(200, Some(42));
        let mut b = KLLWrapper::new(200, Some(42));
        a.update_batch((1..=50).map(f64::from));
        b.update_batch((1..=50).map(f64::from));
        assert_eq!(a.snapshot().unwrap(), b.snapshot().unwrap());
    }

    /// Merging a compacted sketch must preserve the source's logical sample
    /// weight, not reduce it to the number of retained items.
    #[test]
    fn merge_preserves_compacted_sample_count() {
        let mut source = KLLWrapper::new(200, Some(42));
        source.update_batch((0..50_000).map(f64::from));
        let mut target = KLLWrapper::new(200, Some(99));
        target.merge(&source).expect("merge compacted KLL");
        assert_eq!(target.inner().count(), source.inner().count());
        let p50 = target.quantile(0.5);
        assert!((23_000.0..=27_000.0).contains(&p50), "p50={p50}");
    }
}
