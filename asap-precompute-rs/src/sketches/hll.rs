//! HyperLogLog wrapper over sketchlib's precision-14 Ertl-MLE
//! [`asap_sketchlib::HyperLogLog`].
//!
//! Implements [`Sketch`] + [`CardinalitySketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope (kind `0x01 0x02`).

use asap_sketchlib::{DataInput, Ertl, HyperLogLog};

use crate::observation::Observation;
use crate::precompute::{
    CardinalitySketch, EstimatePoint, PrecomputeError, Sketch, SketchObserver,
};

/// HyperLogLog wrapper (2^14 registers, Ertl MLE estimator).
#[derive(Default)]
pub struct HLLWrapper {
    sk: HyperLogLog<Ertl>,
}

impl HLLWrapper {
    /// Construct an empty HLL.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        HyperLogLog::<Ertl>::from_asapv1(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper decode: {e}")))
    }

    /// Insert a byte-slice key.
    pub fn update(&mut self, value: &[u8]) {
        self.sk.update(&DataInput::Bytes(value));
    }

    /// Borrow the underlying sketch.
    pub fn inner(&self) -> &HyperLogLog<Ertl> {
        &self.sk
    }

    fn is_empty(&self) -> bool {
        self.sk.registers_as_slice().iter().all(|&r| r == 0)
    }
}

impl Sketch for HLLWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        self.sk
            .to_asapv1()
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper snapshot: {e}")))
    }

    fn merge_snapshot(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.sk.merge(&Self::from_snapshot(bytes)?.sk);
        Ok(())
    }

    fn reset(&mut self) {
        self.sk = HyperLogLog::<Ertl>::new();
    }

    fn estimate(&self, _quantiles: &[f64], _top_k: usize) -> Vec<EstimatePoint> {
        if self.is_empty() {
            return Vec::new();
        }
        vec![EstimatePoint {
            labels: Vec::new(),
            value: CardinalitySketch::estimate_cardinality(self),
        }]
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl CardinalitySketch for HLLWrapper {
    fn estimate_cardinality(&self) -> f64 {
        self.sk.estimate() as f64
    }
}

/// Observer routing observations into an [`HLLWrapper`].
///
/// Accepts both `Bytes` (preferred — opaque key) and `Float` / `Hash`
/// (their little-endian bytes are hashed).
pub struct HLLObserver;

impl SketchObserver for HLLObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<HLLWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("HLLObserver: sketch is not an HLLWrapper".into())
            })?;
        match obs.value.kind {
            crate::observation::ObservationValueKind::Float => {
                w.update(&obs.value.float.to_le_bytes());
                Ok(())
            }
            crate::observation::ObservationValueKind::Bytes => {
                w.update(&obs.value.bytes);
                Ok(())
            }
            crate::observation::ObservationValueKind::Hash => {
                w.update(&obs.value.hash.to_le_bytes());
                Ok(())
            }
            other => Err(PrecomputeError::Other(format!(
                "HLLObserver: unsupported value kind {}",
                other.name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filled(range: std::ops::Range<u64>) -> HLLWrapper {
        let mut w = HLLWrapper::new();
        for i in range {
            w.update(&i.to_le_bytes());
        }
        w
    }

    #[test]
    fn new_wrapper_is_empty() {
        let w = HLLWrapper::new();
        assert!(w.snapshot().unwrap().is_empty());
        assert_eq!(w.estimate_cardinality(), 0.0);
    }

    #[test]
    fn update_then_estimate() {
        let est = filled(0..1_000).estimate_cardinality();
        assert!(est > 900.0 && est < 1100.0, "est={est}");
    }

    #[test]
    fn snapshot_round_trips_through_asapv1() {
        let w = filled(0..100);
        let bytes = w.snapshot().unwrap();
        let decoded = HLLWrapper::from_snapshot(&bytes).unwrap();
        assert_eq!(
            decoded.inner().registers_as_slice(),
            w.inner().registers_as_slice()
        );
    }

    #[test]
    fn merge_takes_register_max() {
        let mut a = filled(0..500);
        a.merge(&filled(250..750)).unwrap();
        let est = a.estimate_cardinality();
        assert!(est > 675.0 && est < 825.0, "est={est}");
    }
}
