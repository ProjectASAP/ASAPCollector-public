//! DDSketch wrapper over [`asap_sketchlib::DdSketch`].
//!
//! Implements [`Sketch`] + [`QuantileSketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope (kind `0x05 0x00`).

use asap_sketchlib::DdSketch;

use crate::observation::{KeyValue, Observation};
use crate::precompute::{EstimatePoint, PrecomputeError, QuantileSketch, Sketch, SketchObserver};

/// DDSketch wrapper.
pub struct DDSketchWrapper {
    sk: DdSketch,
}

impl DDSketchWrapper {
    /// Construct an empty DDSketch with relative-accuracy alpha.
    /// `alpha` must satisfy `0 < alpha < 1`.
    pub fn new(alpha: f64) -> Self {
        Self {
            sk: DdSketch::new(alpha),
        }
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        DdSketch::from_asapv1(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper decode: {e}")))
    }

    /// Insert a single observation. Non-finite values are ignored.
    pub fn update(&mut self, value: f64) {
        self.sk.add(&value);
    }

    /// Borrow the underlying sketch.
    pub fn inner(&self) -> &DdSketch {
        &self.sk
    }
}

impl Sketch for DDSketchWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.get_count() == 0 {
            return Ok(Vec::new());
        }
        self.sk
            .to_asapv1()
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper snapshot: {e}")))
    }

    fn merge_snapshot(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.sk
            .merge(&Self::from_snapshot(bytes)?.sk)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = DdSketch::new(self.sk.alpha());
    }

    fn estimate(&self, quantiles: &[f64], _top_k: usize) -> Vec<EstimatePoint> {
        if self.sk.get_count() == 0 {
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

impl QuantileSketch for DDSketchWrapper {
    fn quantile(&self, q: f64) -> f64 {
        self.sk
            .get_value_at_quantile(q.clamp(0.0, 1.0))
            .unwrap_or(f64::NAN)
    }
}

/// Observer routing `Float`-kind observations into a [`DDSketchWrapper`].
pub struct DDSketchObserver;

impl SketchObserver for DDSketchObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<DDSketchWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("DDSketchObserver: sketch is not a DDSketchWrapper".into())
            })?;
        match obs.value.kind {
            crate::observation::ObservationValueKind::Float => {
                w.update(obs.value.float);
                Ok(())
            }
            other => Err(PrecomputeError::Other(format!(
                "DDSketchObserver: unsupported value kind {}",
                other.name()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filled(range: std::ops::RangeInclusive<i32>) -> DDSketchWrapper {
        let mut w = DDSketchWrapper::new(0.01);
        for i in range {
            w.update(i as f64);
        }
        w
    }

    #[test]
    fn new_wrapper_is_empty() {
        assert!(DDSketchWrapper::new(0.01).snapshot().unwrap().is_empty());
    }

    #[test]
    fn update_then_quantile_within_bound() {
        let p50 = filled(1..=100).quantile(0.5);
        assert!((p50 - 50.0).abs() / 50.0 < 0.05, "p50={p50}");
    }

    #[test]
    fn snapshot_round_trips_through_asapv1() {
        let w = filled(1..=10);
        let bytes = w.snapshot().unwrap();
        assert_eq!(&bytes[..6], b"ASAPv1");
        let decoded = DDSketchWrapper::from_snapshot(&bytes).unwrap();
        assert_eq!(decoded.inner().get_count(), 10);
        assert_eq!(decoded.snapshot().unwrap(), bytes);
    }

    #[test]
    fn merge_combines_counts() {
        let mut a = filled(1..=5);
        a.merge(&filled(6..=10)).unwrap();
        assert_eq!(a.inner().get_count(), 10);
    }

    #[test]
    fn reset_zeros_state() {
        let mut w = filled(1..=2);
        w.reset();
        assert_eq!(w.inner().get_count(), 0);
    }
}
