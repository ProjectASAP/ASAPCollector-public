//! HLL wrapper over [`asap_sketchlib::HllSketch`].
//!
//! HLL is the canonical [`CardinalitySketch`] implementation in this
//! crate.

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, HllVariant, HyperLogLogState, SketchEnvelope as ProtoEnvelope,
};
use asap_sketchlib::{HllSketch, HllVariant as RsHllVariant, MessagePackCodec};
use prost::Message;

use crate::envelope::Encoding;
use crate::observation::Observation;
use crate::precompute::{CardinalitySketch, DeltaResult, PrecomputeError, Sketch, SketchObserver};

/// HLL wrapper. Owns one `asap_sketchlib::HllSketch`.
pub struct HLLWrapper {
    sk: HllSketch,
    variant: RsHllVariant,
    precision: u32,
    /// Outbound wire format for this series' snapshots/deltas. Baked from
    /// `cfg.encoding` by the OTAP sketch factory.
    wire_encoding: Encoding,
}

impl HLLWrapper {
    /// Construct an empty HLL with the given variant and precision.
    pub fn new(variant: RsHllVariant, precision: u32) -> Self {
        Self {
            sk: HllSketch::new(variant, precision),
            variant,
            precision,
            wire_encoding: Encoding::ProtoFull,
        }
    }

    /// Set the outbound wire format (proto vs msgpack). Builder form used
    /// by the OTAP sketch factory to bake in `cfg.encoding`.
    pub fn with_wire_encoding(mut self, encoding: Encoding) -> Self {
        self.wire_encoding = encoding;
        self
    }

    /// Insert a byte slice, routed to a hashed-bytes path.
    pub fn update(&mut self, value: &[u8]) {
        self.sk.update(value);
    }

    /// Borrow the underlying `HllSketch`.
    pub fn inner(&self) -> &HllSketch {
        &self.sk
    }

    fn build_state(&self) -> HyperLogLogState {
        let proto_variant = match self.variant {
            RsHllVariant::Unspecified => HllVariant::Unspecified as i32,
            RsHllVariant::Regular => HllVariant::Regular as i32,
            RsHllVariant::Datafusion => HllVariant::ErtlMle as i32,
            RsHllVariant::Hip => HllVariant::Hip as i32,
        };
        HyperLogLogState {
            variant: proto_variant,
            precision: self.precision,
            registers: self.sk.registers.clone(),
            hip_kxq0: self.sk.hip_kxq0,
            hip_kxq1: self.sk.hip_kxq1,
            hip_est: self.sk.hip_est,
            // Emit the dense register encoding (tag 3); the sparse
            // encoding (tag 7) is left unset, matching the existing
            // wire form.
            registers_sparse: None,
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: 0.0,
            sketch_state: Some(sketch_envelope::SketchState::Hll(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope(bytes: &[u8]) -> Result<HllSketch, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper decode: {e}")))?;
        let state = match env.sketch_state {
            Some(sketch_envelope::SketchState::Hll(s)) => s,
            _ => {
                return Err(PrecomputeError::Other(
                    "HLLWrapper: envelope did not carry HyperLogLogState".into(),
                ));
            }
        };
        let variant = match HllVariant::try_from(state.variant) {
            Ok(HllVariant::Regular) => RsHllVariant::Regular,
            Ok(HllVariant::ErtlMle) => RsHllVariant::Datafusion,
            Ok(HllVariant::Hip) => RsHllVariant::Hip,
            _ => RsHllVariant::Unspecified,
        };
        Ok(HllSketch::from_raw(
            variant,
            state.precision,
            state.registers,
            state.hip_kxq0,
            state.hip_kxq1,
            state.hip_est,
        ))
    }
}

impl Sketch for HLLWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.sk.registers.iter().all(|&r| r == 0) {
            return Ok(Vec::new());
        }
        if self.wire_encoding.is_msgpack() {
            return self
                .sk
                .to_msgpack()
                .map_err(|e| PrecomputeError::Other(format!("HLLWrapper msgpack snapshot: {e}")));
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        prev: &[u8],
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // Decode the prior snapshot envelope, then diff via
        // `asap_sketchlib`'s `HllSketch::compute_delta` (a register delta
        // carrying every register where this window's value exceeds the
        // prior's). On an empty / undecodable prior, or an empty current
        // sketch, fall back to a full snapshot so the emit path always
        // produces a valid payload.
        //
        // Under per-window delta-against-empty (the snapshot cache resets
        // the cached base to the empty-sketch snapshot at each window
        // close), `prev` decodes to an empty `HllSketch`, so the register
        // delta carries every non-zero register of THIS window — i.e. this
        // window's own register state, with no cross-window register-MAX
        // leakage.
        if self.sk.registers.iter().all(|&r| r == 0) {
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
            let prev_sk = match HllSketch::from_msgpack(prev) {
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

    fn apply_delta(&mut self, payload: &[u8]) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        // Dispatch on payload shape. A full-state envelope (the
        // `SketchEnvelope{HyperLogLogState}` wire format) takes the decode +
        // merge path; otherwise the payload is an `HllDelta` proto and is
        // applied via register max-merge through `apply_delta_bytes`.
        if let Ok(other) = Self::decode_envelope(payload) {
            return self
                .sk
                .merge(&other)
                .map_err(|e| PrecomputeError::Other(format!("HLLWrapper merge: {e}")));
        }
        self.sk
            .apply_delta_bytes(payload)
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper apply_delta: {e}")))
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
                PrecomputeError::Other(format!("HLLWrapper msgpack apply_delta: {e}"))
            }),
            Encoding::Msgpack => {
                let other = HllSketch::from_msgpack(payload).map_err(|e| {
                    PrecomputeError::Other(format!("HLLWrapper msgpack decode: {e}"))
                })?;
                self.sk
                    .merge(&other)
                    .map_err(|e| PrecomputeError::Other(format!("HLLWrapper merge: {e}")))
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
            HllSketch::from_msgpack(&bytes).map_err(|e| {
                PrecomputeError::Other(format!("HLLWrapper msgpack merge decode: {e}"))
            })?
        } else {
            Self::decode_envelope(&bytes)?
        };
        self.sk
            .merge(&decoded)
            .map_err(|e| PrecomputeError::Other(format!("HLLWrapper merge: {e}")))
    }

    fn reset(&mut self) {
        self.sk = HllSketch::new(self.variant, self.precision);
    }

    fn delta_against_empty_base(&self) -> Result<Option<Vec<u8>>, PrecomputeError> {
        // HLL opts in to per-window deltas. After a window-close emit the
        // snapshot cache caches THIS — the encoded envelope of an EMPTY HLL
        // of the same variant / precision — so the next window's
        // `compute_delta_against` diffs against empty and emits that
        // window's own register state as a delta. The empty-base reset is
        // what makes window-scoped cardinality correct: HLL merges by
        // register-wise MAX over a never-reset base, which would over-count
        // without the reset.
        //
        // We encode the empty envelope rather than returning
        // `Sketch::snapshot()` of an empty sketch, because the latter
        // short-circuits to empty bytes (the runtime drops empty
        // payloads), and empty bytes would make `compute_delta_against`
        // fall back to a full snapshot instead of a delta.
        if self.wire_encoding.is_msgpack() {
            let empty = HllSketch::new(self.variant, self.precision);
            let bytes = empty.to_msgpack().map_err(|e| {
                PrecomputeError::Other(format!("HLLWrapper msgpack empty base: {e}"))
            })?;
            return Ok(Some(bytes));
        }
        let empty = HLLWrapper::new(self.variant, self.precision);
        Ok(Some(empty.encode_envelope()))
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl CardinalitySketch for HLLWrapper {
    fn estimate_cardinality(&self) -> f64 {
        self.sk.estimate()
    }
}

/// Observer routing observations into an [`HLLWrapper`].
///
/// Accepts both `Bytes` (preferred — opaque key) and `Float` (the
/// float bytes are hashed).
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
                let bytes = obs.value.float.to_le_bytes();
                w.update(&bytes);
                Ok(())
            }
            crate::observation::ObservationValueKind::Bytes => {
                w.update(&obs.value.bytes);
                Ok(())
            }
            crate::observation::ObservationValueKind::Hash => {
                let bytes = obs.value.hash.to_le_bytes();
                w.update(&bytes);
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

    #[test]
    fn new_wrapper_is_empty() {
        let w = HLLWrapper::new(RsHllVariant::Regular, 12);
        assert_eq!(w.snapshot().unwrap().len(), 0);
        assert_eq!(w.estimate_cardinality(), 0.0);
    }

    #[test]
    fn update_then_estimate() {
        let mut w = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..1_000u64 {
            w.update(&i.to_le_bytes());
        }
        let est = w.estimate_cardinality();
        // Loose bounds — HLL with precision=12 has std error ~1.6%.
        assert!(est > 800.0 && est < 1200.0, "est={est}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_registers() {
        let mut w = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..100u64 {
            w.update(&i.to_le_bytes());
        }
        let bytes = w.snapshot().unwrap();
        let decoded = HLLWrapper::decode_envelope(&bytes).unwrap();
        assert_eq!(decoded.registers, w.sk.registers);
    }

    #[test]
    fn merge_takes_register_max() {
        let mut a = HLLWrapper::new(RsHllVariant::Regular, 12);
        let mut b = HLLWrapper::new(RsHllVariant::Regular, 12);
        for i in 0..500u64 {
            a.update(&i.to_le_bytes());
        }
        for i in 250..750u64 {
            b.update(&i.to_le_bytes());
        }
        let pre = a.estimate_cardinality();
        let other_bytes = b.snapshot().unwrap();
        a.apply_delta(&other_bytes).unwrap();
        let post = a.estimate_cardinality();
        assert!(
            post >= pre,
            "merged estimate decreased: pre={pre} post={post}"
        );
    }
}
