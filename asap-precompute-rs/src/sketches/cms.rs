//! Count-Min sketch wrapper over [`asap_sketchlib::CountMin`].
//!
//! Implements [`Sketch`] + [`FrequencySketch`]. Snapshots are sketchlib's
//! ASAPv1 envelope (kind `0x02 0x00`).

use asap_sketchlib::{CountMin, DataInput, RegularPath, Vector2D, MATRIX_MAX_ROWS};

use crate::observation::Observation;
use crate::precompute::{FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver};

type Cms = CountMin<Vector2D<i64>, RegularPath>;

/// Count-Min sketch wrapper. Keys are raw bytes.
pub struct CMSWrapper {
    sk: Cms,
}

impl CMSWrapper {
    /// Construct a CMS with `rows` hash rows (clamped to `1..=MATRIX_MAX_ROWS`,
    /// the number of independent sketchlib row seeds) and `cols` columns.
    pub fn new(rows: usize, cols: usize) -> Self {
        Self {
            sk: Cms::with_dimensions(rows.clamp(1, MATRIX_MAX_ROWS), cols.max(1)),
        }
    }

    /// Decode a full ASAPv1 snapshot.
    pub fn from_snapshot(bytes: &[u8]) -> Result<Self, PrecomputeError> {
        Cms::deserialize_from_bytes(bytes)
            .map(|sk| Self { sk })
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper decode: {e}")))
    }

    /// Add `count` occurrences of `key`.
    pub fn update(&mut self, key: &[u8], count: i64) {
        self.sk.insert_many(&DataInput::Bytes(key), count);
    }

    /// Borrow the underlying sketch.
    pub fn inner(&self) -> &Cms {
        &self.sk
    }
}

impl Sketch for CMSWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.as_storage().as_slice().iter().all(|&c| c == 0) {
            return Ok(Vec::new());
        }
        self.sk
            .serialize_to_bytes()
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper snapshot: {e}")))
    }

    fn merge_snapshot(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let other = Self::from_snapshot(bytes)?;
        if (self.sk.rows(), self.sk.cols()) != (other.sk.rows(), other.sk.cols()) {
            return Err(PrecomputeError::Other(
                "CMSWrapper merge: dimension mismatch".into(),
            ));
        }
        self.sk.merge(&other.sk);
        Ok(())
    }

    fn reset(&mut self) {
        self.sk = Cms::with_dimensions(self.sk.rows(), self.sk.cols());
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl FrequencySketch for CMSWrapper {
    fn estimate_count(&self, key: &[u8]) -> f64 {
        self.sk.estimate(&DataInput::Bytes(key)) as f64
    }

    /// A plain Count-Min does not track keys; use
    /// [`super::CMSHeapWrapper`] for top-k.
    fn top_k(&self, _k: usize) -> Vec<FrequencyEntry> {
        Vec::new()
    }
}

/// Observer counting each observation's key once.
///
/// The key is the observation's `bytes` value when present, otherwise its
/// full label set via [`crate::matchers::attributes_key`]. Any value kind
/// is accepted (OTAP scalar rows arrive as `Float` with empty bytes).
pub struct CMSObserver;

impl SketchObserver for CMSObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<CMSWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("CMSObserver: sketch is not a CMSWrapper".into())
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

    #[test]
    fn new_wrapper_is_empty() {
        assert!(CMSWrapper::new(4, 32).snapshot().unwrap().is_empty());
    }

    #[test]
    fn update_then_estimate() {
        let mut w = CMSWrapper::new(8, 64);
        w.update(b"k1", 50);
        w.update(b"k2", 3);
        assert!(w.estimate_count(b"k1") >= 50.0);
        assert!(w.estimate_count(b"k2") >= 3.0);
    }

    #[test]
    fn snapshot_round_trips_through_asapv1() {
        let mut w = CMSWrapper::new(4, 8);
        w.update(b"k", 1);
        let bytes = w.snapshot().unwrap();
        let decoded = CMSWrapper::from_snapshot(&bytes).unwrap();
        assert_eq!(
            decoded.inner().as_storage().as_slice(),
            w.inner().as_storage().as_slice()
        );
    }

    #[test]
    fn merge_adds_counts() {
        let mut a = CMSWrapper::new(4, 64);
        let mut b = CMSWrapper::new(4, 64);
        a.update(b"k", 2);
        b.update(b"k", 5);
        a.merge(&b).unwrap();
        assert_eq!(a.estimate_count(b"k"), 7.0);
    }

    /// OTAP scalar rows arrive Float-kind with empty bytes; CMS must count
    /// the attribute set, not the metric name.
    #[test]
    fn observe_counts_attribute_set_not_metric_name() {
        use crate::matchers::attributes_key;
        use crate::observation::{KeyValue, ObservationValue};

        let mut sketch = CMSWrapper::new(8, 64);
        let labels = vec![KeyValue::new("host", "h1")];
        for _ in 0..25 {
            let obs = Observation::new(
                1_000,
                "flow_count",
                vec![],
                labels.clone(),
                ObservationValue::float(1.0),
            );
            CMSObserver.observe(&mut sketch, &obs).expect("observe");
        }
        let attr_key = attributes_key(&labels, &[]);
        assert_eq!(attr_key, "host=h1;");
        assert!(sketch.estimate_count(attr_key.as_bytes()) >= 25.0);
        assert!(sketch.estimate_count(b"flow_count") < 25.0);
    }
}
