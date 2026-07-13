//! DDSketch wrapper over [`asap_sketchlib::DdSketch`].
//!
//! Adapts the wire-format-aligned `DdSketch` struct to the
//! host-neutral [`Sketch`] + [`QuantileSketch`] interfaces.

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, DdSketchState, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::{DdSketch, MessagePackCodec};
use prost::Message;

use crate::envelope::Encoding;
use crate::observation::{KeyValue, Observation};
use crate::precompute::{
    DeltaResult, EstimatePoint, PrecomputeError, QuantileSketch, Sketch, SketchObserver,
};

/// DDSketch wrapper.
///
/// Owns one `asap_sketchlib::DdSketch` (the wire-format-aligned variant
/// with public-field `store_counts` / `store_offset` / aggregates).
///
/// # Snapshot format
///
/// `Snapshot` produces a `prost`-encoded `SketchEnvelope` carrying the
/// inner `DDSketchState` proto.
pub struct DDSketchWrapper {
    sk: DdSketch,
    alpha: f64,
    /// Outbound wire format for this series' snapshots/deltas. Baked from
    /// `cfg.encoding` by the OTAP sketch factory; `snapshot` /
    /// `compute_delta_against` / `delta_against_empty_base` honor it to
    /// pick proto vs msgpack.
    wire_encoding: Encoding,
}

impl DDSketchWrapper {
    /// Construct an empty DDSketch with relative-accuracy alpha.
    /// `alpha` must satisfy `0 < alpha < 1`.
    pub fn new(alpha: f64) -> Self {
        Self {
            sk: DdSketch::new(alpha),
            alpha,
            wire_encoding: Encoding::ProtoFull,
        }
    }

    /// Set the outbound wire format (proto vs msgpack). Builder form used
    /// by the OTAP sketch factory to bake in `cfg.encoding`.
    pub fn with_wire_encoding(mut self, encoding: Encoding) -> Self {
        self.wire_encoding = encoding;
        self
    }

    /// Insert a single positive observation.
    pub fn update(&mut self, value: f64) {
        self.sk.update(value);
    }

    /// Borrow the underlying `DdSketch`.
    pub fn inner(&self) -> &DdSketch {
        &self.sk
    }

    fn build_state(&self) -> DdSketchState {
        DdSketchState {
            // Use the gamma-roundtripped alpha so the on-the-wire bytes
            // match the portable serialization exactly.
            //
            // The DataPoint-level METRIC scalars (count/sum/min/max) were
            // dropped from `DdSketchState`: the count is recoverable by
            // summing `store_counts` and the others are bucket-estimated,
            // so the wire format now carries only alpha + the bucket store.
            alpha: self.sk.wire_alpha(),
            store_counts: self.sk.store_counts.clone(),
            store_offset: self.sk.store_offset,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: 0.0,
            sketch_state: Some(sketch_envelope::SketchState::Ddsketch(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<DdSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::Ddsketch(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "DDSketchWrapper: envelope did not carry DDSketchState".into(),
                ));
            }
        };
        if !(state.alpha > 0.0 && state.alpha < 1.0) {
            return Err(PrecomputeError::Other(format!(
                "DDSketchWrapper: alpha {} out of range",
                state.alpha
            )));
        }
        // `DdSketchState` no longer carries the count/sum/min/max scalars.
        // The total count is recovered by summing `store_counts` (see
        // `DdSketch::total_count`); min/max are estimated from the extreme
        // non-empty buckets inside `DdSketch::quantile`, within DDSketch's
        // alpha relative-accuracy bound. So we reconstruct purely from the
        // bucket store + alpha.
        Ok(DdSketch::from_raw(
            state.alpha,
            state.store_counts,
            state.store_offset,
        ))
    }
}

impl Sketch for DDSketchWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.total_count() == 0 {
            // Empty sketch produces empty snapshot — the runtime drops
            // empty payloads rather than emitting zero-byte envelopes.
            return Ok(Vec::new());
        }
        if self.wire_encoding.is_msgpack() {
            return self.sk.to_msgpack().map_err(|e| {
                PrecomputeError::Other(format!("DDSketchWrapper msgpack snapshot: {e}"))
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
        // `asap_sketchlib`'s `DdSketch::compute_delta`. On an empty /
        // undecodable prior, or an empty current sketch, fall back to a
        // full snapshot so the emit path always produces a valid payload.
        //
        // Under per-window delta-against-empty (the snapshot cache resets
        // the cached base to the empty-sketch snapshot at each window
        // close), `prev` decodes to an empty `DdSketch`, so the computed
        // delta IS this window's full bucket store encoded as bucket
        // deltas — no cross-window subtraction.
        if self.sk.total_count() == 0 {
            // Empty current sketch produces an empty snapshot; the
            // runtime drops empty payloads.
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
            // Msgpack path: the cached base is a msgpack-encoded full
            // `DdSketch`; diff against it and emit the sparse msgpack delta.
            let prev_sk = match DdSketch::from_msgpack(prev) {
                Ok(sk) => sk,
                Err(_) => {
                    let full = self.snapshot()?;
                    return Ok(DeltaResult {
                        payload: full,
                        is_full: true,
                    });
                }
            };
            let delta = self.sk.compute_delta_msgpack(&prev_sk, threshold);
            return Ok(DeltaResult {
                payload: delta,
                is_full: false,
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
        let delta = self.sk.compute_delta(&prev_sk, threshold);
        Ok(DeltaResult {
            payload: delta,
            is_full: false,
        })
    }

    fn apply_delta(&mut self, delta: &[u8]) -> Result<(), PrecomputeError> {
        if delta.is_empty() {
            return Ok(());
        }
        // Dispatch on payload shape. A full-state envelope (the
        // `SketchEnvelope{DDSketchState}` wire format) takes the decode +
        // merge path; otherwise the payload is a `DDSketchDelta` proto and
        // is applied additively.
        if let Ok(other) = Self::decode_envelope(delta) {
            return self
                .sk
                .merge(&other)
                .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper merge: {e}")));
        }
        self.sk
            .apply_delta_bytes(delta)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper apply_delta: {e}")))
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
                PrecomputeError::Other(format!("DDSketchWrapper msgpack apply_delta: {e}"))
            }),
            Encoding::Msgpack => {
                let other = DdSketch::from_msgpack(payload).map_err(|e| {
                    PrecomputeError::Other(format!("DDSketchWrapper msgpack decode: {e}"))
                })?;
                self.sk
                    .merge(&other)
                    .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper merge: {e}")))
            }
            _ => self.apply_delta(payload),
        }
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        // The runtime always merges sketches owned by the same
        // Precompute (same alpha). Our trait is generic, so we
        // round-trip through the snapshot bytes.
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        // `other.snapshot()` is in that wrapper's `wire_encoding`; same
        // Precompute ⇒ same encoding as ours, so decode by our own tag.
        let decoded = if self.wire_encoding.is_msgpack() {
            DdSketch::from_msgpack(&bytes).map_err(|e| {
                PrecomputeError::Other(format!("DDSketchWrapper msgpack merge decode: {e}"))
            })?
        } else {
            Self::decode_envelope(&bytes)?
        };
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("DDSketchWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = DdSketch::new(self.alpha);
    }

    fn delta_against_empty_base(&self) -> Result<Option<Vec<u8>>, PrecomputeError> {
        // DDSketch opts in to per-window deltas. After a window-close
        // emit the snapshot cache
        // caches THIS — the encoded envelope of an EMPTY DDSketch of the
        // same alpha — so the next window's `compute_delta_against` diffs
        // against empty and emits that window's own bucket store as a
        // delta (no cross-window subtraction).
        //
        // Note: we deliberately encode the empty envelope rather than
        // returning `Sketch::snapshot()` of an empty sketch, because the
        // latter short-circuits to empty bytes (the runtime drops empty
        // payloads), and empty bytes would make `compute_delta_against`
        // fall back to a full snapshot instead of a delta.
        if self.wire_encoding.is_msgpack() {
            let empty = DdSketch::new(self.alpha);
            let bytes = empty.to_msgpack().map_err(|e| {
                PrecomputeError::Other(format!("DDSketchWrapper msgpack empty base: {e}"))
            })?;
            return Ok(Some(bytes));
        }
        let empty = DDSketchWrapper::new(self.alpha);
        Ok(Some(empty.encode_envelope()))
    }

    fn estimate(&self, quantiles: &[f64], _top_k: usize) -> Vec<EstimatePoint> {
        if self.sk.total_count() == 0 {
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
        if self.sk.total_count() == 0 {
            return f64::NAN;
        }
        self.sk.quantile(q.clamp(0.0, 1.0)).unwrap_or(f64::NAN)
    }
}

/// Observer routing `Float`-kind observations into a [`DDSketchWrapper`].
pub struct DDSketchObserver;

impl SketchObserver for DDSketchObserver {
    fn observe(&self, sketch: &mut dyn Sketch, obs: &Observation) -> Result<(), PrecomputeError> {
        // Use a `&mut dyn Sketch -> &mut DDSketchWrapper` downcast via
        // the panic-safe method below.
        let w = downcast_mut(sketch)?;
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

fn downcast_mut(sketch: &mut dyn Sketch) -> Result<&mut DDSketchWrapper, PrecomputeError> {
    sketch
        .as_any_mut()
        .downcast_mut::<DDSketchWrapper>()
        .ok_or_else(|| {
            PrecomputeError::Other("DDSketchObserver: sketch is not a DDSketchWrapper".into())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_wrapper_is_empty() {
        let w = DDSketchWrapper::new(0.01);
        assert_eq!(w.sk.total_count(), 0);
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_quantile_within_bound() {
        let mut w = DDSketchWrapper::new(0.01);
        for i in 1..=100 {
            w.update(i as f64);
        }
        let p50 = w.quantile(0.5);
        // Median of [1..=100] is 50 or 51; with α=0.01 the relative
        // error is ≤ 1%.
        assert!((p50 - 50.0).abs() / 50.0 < 0.05, "p50={p50}");
    }

    #[test]
    fn snapshot_decodes_back_to_equivalent_state() {
        let mut w = DDSketchWrapper::new(0.01);
        for i in 1..=10 {
            w.update(i as f64);
        }
        let bytes = w.snapshot().unwrap();
        let decoded = DDSketchWrapper::decode_envelope(&bytes).unwrap();
        // count is recovered exactly by summing the bucket store; the
        // count/sum scalars are no longer on the wire.
        assert_eq!(decoded.total_count(), w.sk.total_count());
        assert_eq!(decoded.store_counts, w.sk.store_counts);
        assert_eq!(decoded.store_offset, w.sk.store_offset);
    }

    #[test]
    fn merge_combines_counts() {
        let mut a = DDSketchWrapper::new(0.01);
        let mut b = DDSketchWrapper::new(0.01);
        for i in 1..=5 {
            a.update(i as f64);
        }
        for i in 6..=10 {
            b.update(i as f64);
        }
        let other_bytes = b.snapshot().unwrap();
        a.apply_delta(&other_bytes).unwrap();
        assert_eq!(a.sk.total_count(), 10);
    }

    #[test]
    fn reset_zeros_state() {
        let mut w = DDSketchWrapper::new(0.01);
        w.update(1.0);
        w.update(2.0);
        w.reset();
        assert_eq!(w.sk.total_count(), 0);
    }
}
