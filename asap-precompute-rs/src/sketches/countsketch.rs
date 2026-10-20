//! Count Sketch wrapper over [`asap_sketchlib::CountSketch`].
//!
//! Implements [`Sketch`] + [`FrequencySketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope (kind `0x04 0x00`).

use asap_sketchlib::{CountSketch, DataInput, RegularPath, Vector2D, MATRIX_MAX_ROWS};

use crate::observation::Observation;
use crate::precompute::{FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver};

type Cs = CountSketch<Vector2D<i64>, RegularPath>;

/// Count Sketch wrapper. Keys are raw bytes.
pub struct CountSketchWrapper {
    sk: Cs,
}

impl CountSketchWrapper {
    /// Construct a Count Sketch with `rows` hash rows (clamped to
    /// `1..=MATRIX_MAX_ROWS`) and `cols` columns.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sk: Cs::with_dimensions(rows.clamp(1, MATRIX_MAX_ROWS), cols.max(1)),
        }
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        Cs::from_asapv1(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper decode: {e}")))
    }

    /// Add `count` occurrences of `key`.
    pub fn update(&mut self, key: &[u8], count: i64) {
        self.sk.update_weighted(&DataInput::Bytes(key), count);
    }

    /// Borrow the underlying sketch.
    pub fn inner(&self) -> &Cs {
        &self.sk
    }
}

impl Sketch for CountSketchWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.as_storage().as_slice().iter().all(|&c| c == 0) {
            return Ok(Vec::new());
        }
        self.sk
            .to_asapv1()
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper snapshot: {e}")))
    }

    fn merge_snapshot(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let other = Self::from_snapshot(bytes)?;
        if (self.sk.rows(), self.sk.cols()) != (other.sk.rows(), other.sk.cols()) {
            return Err(PrecomputeError::Other(
                "CountSketchWrapper merge: dimension mismatch".into(),
            ));
        }
        self.sk.merge(&other.sk);
        Ok(())
    }

    fn reset(&mut self) {
        self.sk = Cs::with_dimensions(self.sk.rows(), self.sk.cols());
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl FrequencySketch for CountSketchWrapper {
    fn estimate_count(&self, key: &[u8]) -> f64 {
        self.sk.estimate(&DataInput::Bytes(key))
    }

    /// A plain Count Sketch does not track keys.
    fn top_k(&self, _k: usize) -> Vec<FrequencyEntry> {
        Vec::new()
    }
}

/// Observer counting each observation's key.
///
/// The key is the observation's `bytes` field when present (weight: the
/// value's `float`, rounded), else the full label set via
/// [`crate::matchers::attributes_key`] (weight 1), falling back to
/// [`Self::default_key`] when there are no labels either.
pub struct CountSketchObserver {
    /// Key used when the observation has neither `bytes` nor labels.
    pub default_key: String,
}

impl SketchObserver for CountSketchObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<CountSketchWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other(
                    "CountSketchObserver: sketch is not a CountSketchWrapper".into(),
                )
            })?;
        if !obs.value.bytes.is_empty() {
            w.update(&obs.value.bytes, obs.value.float.round() as i64);
            return Ok(());
        }
        let attr_key = crate::matchers::attributes_key(&obs.labels, &[]);
        let key = if attr_key.is_empty() {
            &self.default_key
        } else {
            &attr_key
        };
        w.update(key.as_bytes(), 1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        assert!(CountSketchWrapper::new(4, 32)
            .snapshot()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn update_then_estimate() {
        let mut w = CountSketchWrapper::new(8, 64);
        w.update(b"hot-key", 100);
        w.update(b"cold-key", 5);
        let hot = w.estimate_count(b"hot-key");
        let cold = w.estimate_count(b"cold-key");
        assert!(hot.abs() > cold.abs(), "hot={hot} cold={cold}");
    }

    #[test]
    fn snapshot_round_trips_through_asapv1() {
        let mut w = CountSketchWrapper::new(4, 8);
        w.update(b"k", 1);
        let bytes = w.snapshot().unwrap();
        let decoded = CountSketchWrapper::from_snapshot(&bytes).unwrap();
        assert_eq!(
            decoded.inner().as_storage().as_slice(),
            w.inner().as_storage().as_slice()
        );
    }

    /// OTAP scalar rows arrive Float-kind with empty bytes; Count Sketch
    /// must count the attribute set with weight 1, not `default_key`.
    #[test]
    fn observe_counts_attribute_set_not_metric_name() {
        use crate::matchers::attributes_key;
        use crate::observation::{KeyValue, ObservationValue};

        let mut sketch = CountSketchWrapper::new(8, 64);
        let observer = CountSketchObserver {
            default_key: "events_per_path".into(),
        };
        let labels = vec![KeyValue::new("path", "/api")];
        let n = 30;
        for _ in 0..n {
            let obs = Observation::new(
                1_000,
                "events_per_path",
                vec![],
                labels.clone(),
                ObservationValue::float(1.0),
            );
            observer.observe(&mut sketch, &obs).expect("observe");
        }
        let attr_key = attributes_key(&labels, &[]);
        assert_eq!(attr_key, "path=/api;");
        let counted = sketch.estimate_count(attr_key.as_bytes());
        assert!(counted >= (n as f64) * 0.5, "counted={counted}");
        assert!(sketch.estimate_count(b"events_per_path").abs() < counted);
    }
}
