//! CountSketch wrapper over [`asap_sketchlib::CountSketch`].
//!
//! Implements [`Sketch`] + [`FrequencySketch`].

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, CountSketchState, CounterType, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::{CountSketch, MessagePackCodec};
use prost::Message;

use crate::envelope::Encoding;
use crate::observation::Observation;
use crate::precompute::{
    DeltaResult, FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver,
};

/// Width, in bits, of the single 64-bit per-item hash that sketchlib's
/// CountSketch bit-slices across rows. See `cms.rs::MAX_ROW_HASH_BITS`.
const MAX_ROW_HASH_BITS: usize = 64;

/// Clamp `rows` so `rows * ceil(log2(cols)) <= 64`. Identical to the CMS
/// helper (`cms.rs::clamp_rows_for_hash_bits`). `cols` is assumed already
/// rounded to a power of two. Returns at least 1.
fn clamp_rows_for_hash_bits(rows: usize, cols: usize) -> usize {
    let bits_per_row = cols.trailing_zeros() as usize;
    if bits_per_row == 0 {
        return rows.max(1);
    }
    let max_rows = (MAX_ROW_HASH_BITS / bits_per_row).max(1);
    rows.min(max_rows).max(1)
}

/// Fixed seed for CountSketch admission sampling.
const COUNTSKETCH_SAMPLE_SEED: u64 = 0x5a3e06d;

/// CountSketch wrapper.
pub struct CountSketchWrapper {
    sk: CountSketch,
    rows: usize,
    cols: usize,
    /// Per-sketch admission-sampling probability in (0,1]; 1.0 = exact (default).
    sample_p: f64,
    sampler: crate::sampling::GeometricSampler,
    /// Outbound wire format for this series' snapshots/deltas. Baked from
    /// `cfg.encoding` by the OTAP sketch factory.
    wire_encoding: Encoding,
}

impl CountSketchWrapper {
    /// Construct a CountSketch with the given dimensions.
    ///
    /// A non-power-of-two `cols` and the narrow-hash condition
    /// (`rows * log2(cols) > 64`) are invalid. `new()` returns `Self`
    /// (not `Result`), so instead of rejecting we apply the SAME
    /// normalization as `CMSWrapper::new`: round `cols` up to the next
    /// power of two and clamp `rows` by the 64-bit per-item hash budget.
    ///
    /// Any VALID config (power-of-two `cols` within the budget — e.g. the
    /// canonical `2048` / depth `4`) is left as-is; an invalid config is
    /// repaired here identically rather than panicking, preserving
    /// byte-parity.
    pub fn new(rows: usize, cols: usize) -> Self {
        let cols = cols.max(1).next_power_of_two();
        let rows = clamp_rows_for_hash_bits(rows, cols);
        Self {
            sk: CountSketch::new(rows, cols),
            rows,
            cols,
            sample_p: 1.0,
            sampler: crate::sampling::GeometricSampler::new(1.0, COUNTSKETCH_SAMPLE_SEED),
            wire_encoding: Encoding::ProtoFull,
        }
    }

    /// Set the outbound wire format (proto vs msgpack). Builder form used
    /// by the OTAP sketch factory to bake in `cfg.encoding`.
    pub fn with_wire_encoding(mut self, encoding: Encoding) -> Self {
        self.wire_encoding = encoding;
        self
    }

    /// Enable producer-side admission sampling at probability `p` (builder
    /// form).
    pub fn with_sample_p(mut self, p: f64) -> Self {
        self.set_sample_p(p);
        self
    }

    /// Set the admission-sampling probability and reseed. Used by the
    /// [`crate::precompute::SampleSetter`] coordinated path.
    pub fn set_sample_p(&mut self, p: f64) {
        self.sample_p = if !(p > 0.0) || p >= 1.0 || p.is_nan() {
            1.0
        } else {
            p
        };
        self.sampler.reset(self.sample_p, COUNTSKETCH_SAMPLE_SEED);
    }

    /// The configured admission-sampling probability (1.0 = exact).
    pub fn sample_p(&self) -> f64 {
        self.sample_p
    }

    /// Insert a string-keyed observation, admitted with probability `p` when
    /// sampling is active.
    pub fn update(&mut self, key: &str, value: f64) {
        if !self.sampler.admit() {
            return;
        }
        self.sk.update(key, value);
    }

    /// Borrow the underlying `CountSketch`.
    pub fn inner(&self) -> &CountSketch {
        &self.sk
    }

    fn build_state(&self) -> CountSketchState {
        // Emit packed sint64 `counts_int` (Opt-2: 4–8× smaller than f64
        // for typical small-integer counter values) and per-row L2
        // norms derived as `l2[r] = sum_c counts[r][c]^2`. Both fields
        // are required for cross-language byte parity against the golden
        // fixture; without them the envelope diverges in counter_type,
        // counts_*, and l2 simultaneously.
        let mut counts_int = Vec::with_capacity(self.rows * self.cols);
        let mut l2 = Vec::with_capacity(self.rows);
        for row in self.sk.matrix.iter().take(self.rows) {
            let mut row_l2 = 0.0f64;
            for &cell in row.iter().take(self.cols) {
                counts_int.push(cell as i64);
                row_l2 += cell * cell;
            }
            l2.push(row_l2);
        }
        CountSketchState {
            rows: self.rows as u32,
            cols: self.cols as u32,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            counts_float: Vec::new(),
            l2,
            topk: None,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: crate::sampling::wire_sample_p(self.sample_p),
            sketch_state: Some(sketch_envelope::SketchState::CountSketch(
                self.build_state(),
            )),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<CountSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::CountSketch(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "CountSketchWrapper: envelope did not carry CountSketchState".into(),
                ));
            }
        };
        let rows = state.rows as usize;
        let cols = state.cols as usize;
        let mut matrix = vec![vec![0.0f64; cols]; rows];
        if !state.counts_float.is_empty() {
            for (r, row) in matrix.iter_mut().enumerate().take(rows) {
                for (c, cell) in row.iter_mut().enumerate().take(cols) {
                    let idx = r * cols + c;
                    if idx < state.counts_float.len() {
                        *cell = state.counts_float[idx];
                    }
                }
            }
        } else if !state.counts_int.is_empty() {
            for (r, row) in matrix.iter_mut().enumerate().take(rows) {
                for (c, cell) in row.iter_mut().enumerate().take(cols) {
                    let idx = r * cols + c;
                    if idx < state.counts_int.len() {
                        *cell = state.counts_int[idx] as f64;
                    }
                }
            }
        }
        Ok(CountSketch::from_legacy_matrix(matrix, rows, cols))
    }

    /// Whether the sketch matrix is all zero.
    fn is_empty(&self) -> bool {
        self.sk
            .matrix
            .iter()
            .all(|row| row.iter().all(|&v| v == 0.0))
    }
}

impl Sketch for CountSketchWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        if self.wire_encoding.is_msgpack() {
            return self.sk.to_msgpack().map_err(|e| {
                PrecomputeError::Other(format!("CountSketchWrapper msgpack snapshot: {e}"))
            });
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        prev: &[u8],
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // Decode the prior snapshot envelope, then diff via
        // `asap_sketchlib`'s `CountSketch::compute_delta`. On an empty /
        // undecodable prior, or an empty current sketch, fall back to a
        // full snapshot so the emit path always produces a valid payload.
        //
        // Under per-window delta-against-empty (the snapshot cache resets
        // the cached base to the empty-sketch snapshot at each window
        // close), `prev` decodes to an empty `CountSketch`, so the computed
        // delta IS this window's full (signed) per-cell matrix encoded as
        // cell deltas — no cross-window subtraction.
        if self.is_empty() {
            let full = self.snapshot()?;
            return Ok(DeltaResult {
                payload: full,
                is_full: true,
            });
        }
        if prev.is_empty() {
            let full = self.snapshot()?;
            return Ok(DeltaResult {
                payload: full,
                is_full: true,
            });
        }
        if self.wire_encoding.is_msgpack() {
            let prev_sk = match CountSketch::from_msgpack(prev) {
                Ok(sk) => sk,
                Err(_) => {
                    let full = self.snapshot()?;
                    return Ok(DeltaResult {
                        payload: full,
                        is_full: true,
                    });
                }
            };
            return match self.sk.compute_delta_msgpack(&prev_sk, threshold as f64) {
                Ok(delta) => Ok(DeltaResult {
                    payload: delta,
                    is_full: false,
                }),
                Err(_) => {
                    let full = self.snapshot()?;
                    Ok(DeltaResult {
                        payload: full,
                        is_full: true,
                    })
                }
            };
        }
        let prev_sk = match Self::decode_envelope(prev) {
            Ok(sk) => sk,
            Err(_) => {
                let full = self.snapshot()?;
                return Ok(DeltaResult {
                    payload: full,
                    is_full: true,
                });
            }
        };
        match self.sk.compute_delta(&prev_sk, threshold as f64) {
            Ok(delta) => Ok(DeltaResult {
                payload: delta,
                is_full: false,
            }),
            Err(_) => {
                let full = self.snapshot()?;
                Ok(DeltaResult {
                    payload: full,
                    is_full: true,
                })
            }
        }
    }

    fn apply_delta(&mut self, payload: &[u8]) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        // Dispatch on payload shape. A full-state envelope (the
        // `SketchEnvelope{CountSketchState}` wire format) takes the decode +
        // merge path; otherwise the payload is a `CountSketchDelta` proto
        // and is applied additively via `apply_delta_bytes`.
        if let Ok(other) = Self::decode_envelope(payload) {
            return self
                .sk
                .merge(&other)
                .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper merge: {e}")));
        }
        self.sk
            .apply_delta_bytes(payload)
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper apply_delta: {e}")))
    }

    fn apply_delta_encoded(
        &mut self,
        payload: &[u8],
        encoding: Encoding,
    ) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        match encoding {
            Encoding::MsgpackDelta => self.sk.apply_delta_msgpack_bytes(payload).map_err(|e| {
                PrecomputeError::Other(format!("CountSketchWrapper msgpack apply_delta: {e}"))
            }),
            Encoding::Msgpack => {
                let other = CountSketch::from_msgpack(payload).map_err(|e| {
                    PrecomputeError::Other(format!("CountSketchWrapper msgpack decode: {e}"))
                })?;
                self.sk
                    .merge(&other)
                    .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper merge: {e}")))
            }
            _ => self.apply_delta(payload),
        }
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        // `other.snapshot()` is in that wrapper's `wire_encoding`; same
        // Precompute ⇒ same encoding as ours, so decode by our own tag.
        let decoded = if self.wire_encoding.is_msgpack() {
            CountSketch::from_msgpack(&bytes).map_err(|e| {
                PrecomputeError::Other(format!("CountSketchWrapper msgpack merge decode: {e}"))
            })?
        } else {
            Self::decode_envelope(&bytes)?
        };
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("CountSketchWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = CountSketch::new(self.rows, self.cols);
        self.sampler.reset(self.sample_p, COUNTSKETCH_SAMPLE_SEED);
    }

    fn delta_against_empty_base(&self) -> Result<Option<Vec<u8>>, PrecomputeError> {
        // CountSketch opts in to per-window deltas. After a window-close
        // emit the snapshot cache
        // caches THIS — the encoded envelope of an EMPTY CountSketch of the
        // same dimensions — so the next window's `compute_delta_against`
        // diffs against empty and emits that window's own (signed) per-cell
        // matrix as a delta (no cross-window subtraction).
        //
        // We encode the empty envelope rather than returning
        // `Sketch::snapshot()` of an empty sketch, because the latter
        // short-circuits to empty bytes (the runtime drops empty
        // payloads), and empty bytes would make `compute_delta_against`
        // fall back to a full snapshot instead of a delta.
        if self.wire_encoding.is_msgpack() {
            let empty = CountSketch::new(self.rows, self.cols);
            let bytes = empty.to_msgpack().map_err(|e| {
                PrecomputeError::Other(format!("CountSketchWrapper msgpack empty base: {e}"))
            })?;
            return Ok(Some(bytes));
        }
        let empty = CountSketchWrapper::new(self.rows, self.cols);
        Ok(Some(empty.encode_envelope()))
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl crate::precompute::SampleSetter for CountSketchWrapper {
    fn set_sample_p(&mut self, p: f64) {
        CountSketchWrapper::set_sample_p(self, p);
    }
}

impl FrequencySketch for CountSketchWrapper {
    fn estimate_count(&self, key: &[u8]) -> f64 {
        if key.is_empty() {
            return 0.0;
        }
        let s = std::str::from_utf8(key).unwrap_or_default();
        if s.is_empty() {
            return 0.0;
        }
        self.sk.estimate(s)
    }

    fn top_k(&self, _k: usize) -> Vec<FrequencyEntry> {
        // The wire-format `CountSketch` doesn't carry a TopK heap.
        // Returning empty matches the behavior when `TopK == nil`
        // (the legacy CMS processor never queries TopK; the CountSketch
        // wrapper exposes TopK only when the underlying sketch tracks it).
        Vec::new()
    }
}

/// Observer that routes observations into the wrapper, counting the
/// per-attribute-set frequency.
///
/// The key is the observation's `bytes` field when present (preserves
/// pre-shaped callers / unit tests), else the full label set via
/// [`crate::matchers::attributes_key`], falling back to
/// [`Self::default_key`] only when there are no labels either. The weight
/// is the value's `float` when a pre-shaped `bytes` key is present
/// (existing-test compat) or `1.0` for the OTAP labels path. Float-kind
/// input is accepted.
pub struct CountSketchObserver {
    /// Default key used when the observation has neither a `bytes`
    /// field nor any labels.
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
        let (key_str, weight): (String, f64) = if !obs.value.bytes.is_empty() {
            (
                String::from_utf8_lossy(&obs.value.bytes).into_owned(),
                obs.value.float,
            )
        } else {
            let attr_key = crate::matchers::attributes_key(&obs.labels, &[]);
            let key = if !attr_key.is_empty() {
                attr_key
            } else {
                self.default_key.clone()
            };
            (key, 1.0)
        };
        w.update(&key_str, weight);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = CountSketchWrapper::new(4, 32);
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_estimate() {
        let mut w = CountSketchWrapper::new(8, 64);
        for _ in 0..100 {
            w.update("hot-key", 1.0);
        }
        for _ in 0..5 {
            w.update("cold-key", 1.0);
        }
        let hot = w.estimate_count(b"hot-key");
        let cold = w.estimate_count(b"cold-key");
        // Median-of-rows estimator can over/undercount but should
        // place "hot" well above "cold".
        assert!(hot.abs() > cold.abs(), "hot={hot} cold={cold}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_matrix() {
        let mut w = CountSketchWrapper::new(4, 8);
        w.update("k", 1.0);
        let bytes = w.snapshot().unwrap();
        let decoded = CountSketchWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.matrix, w.sk.matrix);
    }

    // B6 regression: on the OTAP edge, observations arrive Float-kind
    // with empty bytes. CountSketch must count the per-attribute-set
    // key with weight 1.0, NOT degenerately count the metric NAME via
    // default_key.
    #[test]
    fn observe_counts_attribute_set_not_metric_name() {
        use crate::matchers::attributes_key;
        use crate::observation::{KeyValue, Observation, ObservationValue};
        use crate::precompute::SketchObserver;

        let mut sketch = CountSketchWrapper::new(8, 64);
        // default_key set to the metric name to prove we do NOT fall
        // back to it when labels are present.
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
                // Float-kind, empty bytes — OTAP scalar row shape.
                ObservationValue::float(1.0),
            );
            observer.observe(&mut sketch, &obs).expect("observe");
        }

        let attr_key = attributes_key(&labels, &[]);
        assert_eq!(attr_key, "path=/api;");
        let counted = sketch.estimate_count(attr_key.as_bytes());
        // CountSketch's median-of-rows estimator can over/undercount,
        // but the attribute-set key should be near N.
        assert!(
            counted >= (n as f64) * 0.5,
            "attribute-set key undercounted: {counted} (n={n})"
        );
        // The metric NAME (== default_key) must NOT be the counted key.
        let metric_count = sketch.estimate_count(b"events_per_path");
        assert!(
            metric_count.abs() < counted,
            "metric name was counted ({metric_count}) vs attr set ({counted})"
        );
    }
}
