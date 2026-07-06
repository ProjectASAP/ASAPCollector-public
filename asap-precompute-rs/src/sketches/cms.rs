//! CountMinSketch wrapper over [`asap_sketchlib::CountMinSketch`].
//!
//! Mirrors `asap-precompute-go/sketches/cms.go`. Implements
//! [`Sketch`] + [`FrequencySketch`].

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, CountMinState, CounterType, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::CountMinSketch;
use prost::Message;

use crate::observation::Observation;
use crate::precompute::{
    DeltaResult, FrequencyEntry, FrequencySketch, PrecomputeError, Sketch, SketchObserver,
};

/// Width, in bits, of the single 64-bit per-item hash that sketchlib's
/// CMS / CountSketch bit-slice across rows. Each row consumes
/// `ceil(log2(cols))` bits; once `rows * bitsPerRow > 64` the high rows
/// read shifted-out (zero) bits and collapse onto column 0. Mirrors Go's
/// `maxRowHashBits` (`asap-precompute-go/sketches/cms.go`).
const MAX_ROW_HASH_BITS: usize = 64;

/// Clamp `rows` so `rows * ceil(log2(cols)) <= 64`, mirroring Go's
/// `clampRowsForHashBits`. `cols` is assumed already rounded to a power
/// of two, so `cols.trailing_zeros()` == `log2(cols)` == the per-row bit
/// width. When `bits_per_row == 0` (cols == 1) every row maps to column 0
/// regardless, so the slicing never overflows and `rows` is left as-is.
/// Returns at least 1.
fn clamp_rows_for_hash_bits(rows: usize, cols: usize) -> usize {
    let bits_per_row = cols.trailing_zeros() as usize;
    if bits_per_row == 0 {
        return rows.max(1);
    }
    let max_rows = (MAX_ROW_HASH_BITS / bits_per_row).max(1);
    rows.min(max_rows).max(1)
}

/// Fixed seed for CMS admission sampling, mirroring Go's `cmsSampleSeed`
/// (`asap-precompute-go/sketches/cms.go`). A constant seed keeps the admitted
/// subset reproducible; it need not match Go's RNG sequence byte-for-byte
/// (producers sample independently — see `crate::sampling`), but we reuse the
/// value for symmetry.
const CMS_SAMPLE_SEED: u64 = 0x5A4D_5043; // "ZMPC"

/// CountMinSketch wrapper.
pub struct CMSWrapper {
    sk: CountMinSketch,
    rows: usize,
    cols: usize,
    /// Per-sketch admission-sampling probability in (0,1]; 1.0 = exact (default).
    sample_p: f64,
    sampler: crate::sampling::GeometricSampler,
}

impl CMSWrapper {
    /// Construct a CMS with the given dimensions.
    ///
    /// Mirrors Go `NewCMSWrapper` (`asap-precompute-go/sketches/cms.go`)
    /// for #243 byte-parity. The dimensions are normalized identically:
    ///
    /// - `cols` is rounded UP to the next power of two (after a floor of
    ///   1). sketchlib's CMS folds inserts with `% cols` but masks /
    ///   bit-slices the query hash assuming a power-of-two width, so a
    ///   non-pow2 cols mis-indexes. The canonical `cols = 2000` config
    ///   must produce a sketch mergeable with the Go runtime's; both
    ///   sides round to `2048`.
    /// - `rows` is clamped so `rows * log2(cols) <= 64` (the single
    ///   64-bit per-item hash budget); beyond that the high rows read
    ///   shifted-out (zero) bits and collapse onto column 0. See Go's
    ///   `clampRowsForHashBits`.
    ///
    /// The normalized `rows`/`cols` are stored on the struct AND used to
    /// build the underlying `CountMinSketch`, so `build_state` serializes
    /// the normalized dimensions onto the wire.
    pub fn new(rows: usize, cols: usize) -> Self {
        let cols = cols.max(1).next_power_of_two();
        let rows = clamp_rows_for_hash_bits(rows, cols);
        Self {
            sk: CountMinSketch::new(rows, cols),
            rows,
            cols,
            sample_p: 1.0,
            sampler: crate::sampling::GeometricSampler::new(1.0, CMS_SAMPLE_SEED),
        }
    }

    /// Enable producer-side admission sampling at probability `p` (builder
    /// form). `p >= 1` (or NaN/≤0) leaves the sketch exact. Mirrors Go's
    /// `CMSWrapper.WithSampleP`.
    pub fn with_sample_p(mut self, p: f64) -> Self {
        self.set_sample_p(p);
        self
    }

    /// Set the admission-sampling probability and reseed the sampler. Used by
    /// the [`crate::precompute::SampleSetter`] path so the coordinator can
    /// hot-adjust `p` on a live edge.
    pub fn set_sample_p(&mut self, p: f64) {
        self.sample_p = if !(p > 0.0) || p >= 1.0 || p.is_nan() {
            1.0
        } else {
            p
        };
        self.sampler.reset(self.sample_p, CMS_SAMPLE_SEED);
    }

    /// The configured admission-sampling probability (1.0 = exact).
    pub fn sample_p(&self) -> f64 {
        self.sample_p
    }

    /// Insert a string-keyed weighted observation. When sampling is active the
    /// update is admitted with probability `p` (geometric skip); the wire
    /// `sample_p` lets the query side rescale by `1/p`.
    pub fn update(&mut self, key: &str, value: f64) {
        if !self.sampler.admit() {
            return;
        }
        self.sk.update(key, value);
    }

    /// Borrow the underlying `CountMinSketch`.
    pub fn inner(&self) -> &CountMinSketch {
        &self.sk
    }

    fn build_state(&self) -> CountMinState {
        // Mirror sketchlib-go::CountMinSketch.SerializePortableFO:
        // emit packed sint64 `counts_int` (Opt-2: 4–8× smaller than
        // f64 for typical small-integer counter values) and per-row
        // L1/L2 norms (Go's InsertWithHash maintains
        // `L1[r] += weight` and `L2[r] += curr*curr - prev*prev`,
        // which collapse to `sum_c count[r][c]` and
        // `sum_c count[r][c]^2` for the unweighted unit-step stream
        // the parity harness drives — the only producer pattern this
        // wire path serves today). Omit `sum_counts` / `sum2_counts`
        // (Frequency-Only mode) to match Go's `SerializeProtoBytesFO`
        // payload bit-for-bit.
        let matrix = self.sk.sketch();
        let mut counts_int = Vec::with_capacity(self.rows * self.cols);
        let mut l1 = Vec::with_capacity(self.rows);
        let mut l2 = Vec::with_capacity(self.rows);
        for row in matrix.iter().take(self.rows) {
            let mut row_l1 = 0.0f64;
            let mut row_l2 = 0.0f64;
            for &cell in row.iter().take(self.cols) {
                counts_int.push(cell as i64);
                row_l1 += cell;
                row_l2 += cell * cell;
            }
            l1.push(row_l1);
            l2.push(row_l2);
        }
        CountMinState {
            rows: self.rows as u32,
            cols: self.cols as u32,
            counter_type: CounterType::Int64 as i32,
            counts_int,
            counts_float: Vec::new(),
            sum_counts: Vec::new(),
            sum2_counts: Vec::new(),
            l1,
            l2,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            // Stamp the configured p (or 0.0 when exact — the proto3 default,
            // dual-read as 1.0 by the backend, preserving byte-parity with the
            // unsampled path). See `sampling::wire_sample_p`.
            sample_p: crate::sampling::wire_sample_p(self.sample_p),
            sketch_state: Some(sketch_envelope::SketchState::CountMin(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<CountMinSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::CountMin(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "CMSWrapper: envelope did not carry CountMinState".into(),
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
        Ok(CountMinSketch::from_legacy_matrix(matrix, rows, cols))
    }

    fn is_empty(&self) -> bool {
        let m = self.sk.sketch();
        m.iter().all(|row| row.iter().all(|&v| v == 0.0))
    }
}

impl Sketch for CMSWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        prev: &[u8],
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // Mirror Go's `CMSWrapper.ComputeDeltaAgainst`: decode the prior
        // snapshot envelope, then diff via `asap_sketchlib`'s
        // `CountMinSketch::compute_delta`. On an empty / undecodable prior,
        // or an empty current sketch, fall back to a full snapshot so the
        // emit path always produces a valid payload.
        //
        // Under per-window delta-against-empty (the snapshot cache resets
        // the cached base to the empty-sketch snapshot at each window
        // close), `prev` decodes to an empty `CountMinSketch`, so the
        // computed delta IS this window's full per-cell matrix encoded as
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
        // Mirror Go's `CMSWrapper.ApplyDelta`, dispatching on payload
        // shape. A full-state envelope (the `SketchEnvelope{CountMinState}`
        // wire format) takes the decode + merge path; otherwise the payload
        // is a `CountMinDelta` proto and is applied additively via
        // `apply_delta_bytes`.
        if let Ok(other) = Self::decode_envelope(payload) {
            return self
                .sk
                .merge(&other)
                .map_err(|e| PrecomputeError::Other(format!("CMSWrapper merge: {e}")));
        }
        self.sk
            .apply_delta_bytes(payload)
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper apply_delta: {e}")))
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let decoded = Self::decode_envelope(&bytes)?;
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("CMSWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = CountMinSketch::new(self.rows, self.cols);
        // Preserve the configured p across window resets; just reseed the gap.
        self.sampler.reset(self.sample_p, CMS_SAMPLE_SEED);
    }

    fn delta_against_empty_base(&self) -> Result<Option<Vec<u8>>, PrecomputeError> {
        // (delta-baseline-contract.md §3): CMS opts in to per-window
        // deltas. After a window-close emit the snapshot cache caches THIS
        // — the encoded envelope of an EMPTY CountMinSketch of the same
        // dimensions — so the next window's `compute_delta_against` diffs
        // against empty and emits that window's own per-cell matrix as a
        // delta (no cross-window subtraction).
        //
        // We encode the empty envelope rather than returning
        // `Sketch::snapshot()` of an empty sketch, because the latter
        // short-circuits to empty bytes (the runtime drops empty
        // payloads), and empty bytes would make `compute_delta_against`
        // fall back to a full snapshot instead of a delta.
        let empty = CMSWrapper::new(self.rows, self.cols);
        Ok(Some(empty.encode_envelope()))
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl crate::precompute::SampleSetter for CMSWrapper {
    fn set_sample_p(&mut self, p: f64) {
        // Delegate to the inherent method (explicit path avoids resolving back
        // to this trait method).
        CMSWrapper::set_sample_p(self, p);
    }
}

impl FrequencySketch for CMSWrapper {
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
        // CountMinSketch is a frequency estimator over a known key
        // set; it does not natively track top-k. Returning an empty
        // slice matches the Go wrapper.
        Vec::new()
    }
}

/// Observer routing observations into the wrapper, counting the
/// per-attribute-set frequency.
///
/// The key is the observation's `bytes` value when present (preserves
/// pre-shaped callers / unit tests) or, for OTAP scalar observations
/// (Float-kind, empty bytes), the full label set via
/// [`crate::matchers::attributes_key`] — matching the Go edge's
/// `AttributesKey(labels, nil)`. Each observation contributes weight
/// `1.0`. We accept any value kind (including Float decoded from the
/// OTAP `value` column) so CMS records the attribute-set on the OTAP
/// edge instead of silently dropping every observation.
pub struct CMSObserver;

impl SketchObserver for CMSObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        let w = sketch
            .as_any_mut()
            .downcast_mut::<CMSWrapper>()
            .ok_or_else(|| {
                PrecomputeError::Other("CMSObserver: sketch is not a CMSWrapper".into())
            })?;
        let key = if !obs.value.bytes.is_empty() {
            String::from_utf8_lossy(&obs.value.bytes).into_owned()
        } else {
            crate::matchers::attributes_key(&obs.labels, &[])
        };
        w.update(&key, 1.0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = CMSWrapper::new(4, 32);
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_estimate() {
        let mut w = CMSWrapper::new(8, 64);
        for _ in 0..50 {
            w.update("k1", 1.0);
        }
        for _ in 0..3 {
            w.update("k2", 1.0);
        }
        let k1 = w.estimate_count(b"k1");
        let k2 = w.estimate_count(b"k2");
        assert!(k1 >= 50.0, "k1 underestimate: {k1}");
        assert!(k2 >= 3.0, "k2 underestimate: {k2}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_matrix() {
        let mut w = CMSWrapper::new(4, 8);
        w.update("k", 1.0);
        let bytes = w.snapshot().unwrap();
        let decoded = CMSWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.sketch(), w.sk.sketch());
    }

    // #30: coordinated sampling. WithSampleP(p<1) must (a) admit only ~p of
    // updates so the raw sketch count is ~p× the input, and (b) stamp the wire
    // sample_p so the query side can rescale by 1/p. Exact (p>=1) stays byte-clean.
    #[test]
    fn with_sample_p_admits_p_fraction_and_stamps_envelope() {
        use crate::precompute::SampleSetter;
        // exact wrapper: every update lands, envelope sample_p stays 0.0 (parity).
        let mut exact = CMSWrapper::new(8, 1024);
        for _ in 0..10_000 {
            exact.update("k", 1.0);
        }
        assert!((exact.estimate_count(b"k") - 10_000.0).abs() < 1.0);
        let env = ProtoEnvelope::decode(exact.snapshot().unwrap().as_slice()).unwrap();
        assert_eq!(
            env.sample_p, 0.0,
            "exact sketch must stamp 0.0 for byte-parity"
        );

        // sampled wrapper at p=0.1: raw count ~p×, stamped sample_p == p.
        let p = 0.1;
        let mut sampled = CMSWrapper::new(8, 1024).with_sample_p(p);
        assert_eq!(sampled.sample_p(), p);
        let n = 100_000;
        for _ in 0..n {
            sampled.update("k", 1.0);
        }
        let raw = sampled.estimate_count(b"k");
        let admitted_frac = raw / n as f64;
        assert!(
            (admitted_frac - p).abs() < 0.03,
            "admitted fraction {admitted_frac} not ≈ p={p}"
        );
        let env = ProtoEnvelope::decode(sampled.snapshot().unwrap().as_slice()).unwrap();
        assert!(
            (env.sample_p - p).abs() < 1e-12,
            "sampled envelope must stamp p={p}, got {}",
            env.sample_p
        );

        // SetSampleP (the coordinated hot-adjust path) works through the trait.
        let mut w = CMSWrapper::new(4, 256);
        SampleSetter::set_sample_p(&mut w, 0.25);
        assert_eq!(w.sample_p(), 0.25);
        SampleSetter::set_sample_p(&mut w, 1.0); // disable
        assert_eq!(w.sample_p(), 1.0);
    }

    // B6 regression: on the OTAP edge, observations arrive Float-kind
    // with empty bytes. CMS must (a) accept them instead of erroring
    // and (b) count the per-attribute-set key, NOT the metric name.
    #[test]
    fn observe_counts_attribute_set_not_metric_name() {
        use crate::matchers::attributes_key;
        use crate::observation::{KeyValue, Observation, ObservationValue};
        use crate::precompute::SketchObserver;

        let mut sketch = CMSWrapper::new(8, 64);
        let observer = CMSObserver;
        let labels = vec![KeyValue::new("host", "h1")];
        // Float-kind, empty bytes — exactly what the OTAP decoder
        // produces for a scalar row.
        let n = 25;
        for _ in 0..n {
            let obs = Observation::new(
                1_000,
                "flow_count",
                vec![],
                labels.clone(),
                ObservationValue::float(1.0),
            );
            observer.observe(&mut sketch, &obs).expect("observe");
        }

        let attr_key = attributes_key(&labels, &[]);
        assert_eq!(attr_key, "host=h1;");
        let counted = sketch.estimate_count(attr_key.as_bytes());
        assert!(
            counted >= n as f64,
            "attribute-set key undercounted: {counted} < {n}"
        );
        // The metric NAME must not be the counted subject.
        let metric_count = sketch.estimate_count(b"flow_count");
        assert!(
            metric_count < n as f64,
            "metric name was counted ({metric_count}); should be the attribute set"
        );
    }
}
