//! KLL wrapper over [`asap_sketchlib::KLL`].
//!
//! KLL uses random compaction so the wrapper takes an explicit
//! (optional) seed for deterministic byte-identical replay
//! (`init_with_seed` lands a deterministic compaction RNG).

use asap_sketchlib::sketches::KLL;
use asap_sketchlib::{KllSketchData, MessagePackCodec};
use prost::Message;

use asap_sketchlib::proto::sketchlib::{
    sketch_envelope, CoinState, KllState, SketchEnvelope as ProtoEnvelope,
};

use crate::envelope::Encoding;
use crate::observation::Observation;
use crate::precompute::{DeltaResult, PrecomputeError, QuantileSketch, Sketch, SketchObserver};

/// KLL wrapper.
///
/// Owns one `asap_sketchlib::KLL<f64>`. Construction takes
/// `(k, optional seed)`; the seed is forwarded to
/// [`asap_sketchlib::KLL::init_kll_with_seed`] when provided so two
/// wrappers built with the same seed and fed the same input produce
/// byte-identical state.
pub struct KLLWrapper {
    sk: KLL<f64>,
    k: i32,
    seed: Option<u64>,
    /// Snapshot of all observations seen since the last reset. Kept
    /// alongside the in-tree compactor so [`Self::merge`] /
    /// [`Self::apply_delta`] can replay the peer's items into our
    /// compactor without poking at private state. The compactor
    /// itself drives [`Self::snapshot`] via the wire-format accessors
    /// added in `asap_sketchlib` (wire_levels/wire_items/wire_coin),
    /// so cross-language byte-parity now lives against the compactor's
    /// actual state, not this history vec.
    history: Vec<f64>,
    /// Outbound wire format. KLL is full-only, so msgpack means the
    /// portable `KllSketchData` form; proto means the `KllState` envelope.
    /// Baked from `cfg.encoding` by the OTAP sketch factory.
    wire_encoding: Encoding,
}

impl KLLWrapper {
    /// Construct an empty KLL with accuracy parameter `k`.
    /// `seed = Some(s)` enables deterministic compaction.
    pub fn new(k: i32, seed: Option<u64>) -> Self {
        Self {
            sk: build_kll(k, seed),
            k,
            seed,
            history: Vec::new(),
            wire_encoding: Encoding::ProtoFull,
        }
    }

    /// Set the outbound wire format (proto vs msgpack). Builder form used
    /// by the OTAP sketch factory to bake in `cfg.encoding`.
    pub fn with_wire_encoding(mut self, encoding: Encoding) -> Self {
        self.wire_encoding = encoding;
        self
    }

    /// Portable KLL msgpack full form — byte-identical to
    /// `asap_sketchlib::KllSketch::to_msgpack` for the same compactor
    /// state. Built straight from the backend's `serialize_to_bytes` so no
    /// `KllSketch` facade is needed.
    fn encode_msgpack(&self) -> Result<Vec<u8>, PrecomputeError> {
        let sketch_bytes = self
            .sk
            .serialize_to_bytes()
            .map_err(|e| PrecomputeError::Other(format!("KLLWrapper msgpack serialize: {e}")))?;
        KllSketchData {
            k: self.k as u16,
            sketch_bytes,
        }
        .to_msgpack()
        .map_err(|e| PrecomputeError::Other(format!("KLLWrapper msgpack encode: {e}")))
    }

    /// Merge a msgpack-encoded portable KLL frame in.
    ///
    /// Replays the peer's retained items (`wire_items`) into our compactor
    /// — the same approach the proto ingest path uses — rather than
    /// `KLL::merge`, which loses weight when the target is empty (skewing
    /// quantiles). This keeps msgpack KLL ingest behaviorally identical to
    /// the proto path.
    fn merge_msgpack(&mut self, bytes: &[u8]) -> Result<(), PrecomputeError> {
        let data = KllSketchData::from_msgpack(bytes)
            .map_err(|e| PrecomputeError::Other(format!("KLLWrapper msgpack decode: {e}")))?;
        let other: KLL<f64> = KLL::deserialize_from_bytes(&data.sketch_bytes)
            .map_err(|e| PrecomputeError::Other(format!("KLLWrapper msgpack deserialize: {e}")))?;
        for v in other.wire_items() {
            if v.is_finite() {
                self.history.push(v);
                self.sk.update(&v);
            }
        }
        Ok(())
    }

    /// Insert a single observation.
    pub fn update(&mut self, value: f64) {
        if value.is_finite() {
            self.history.push(value);
            self.sk.update(&value);
        }
    }

    /// Borrow the underlying `KLL`.
    pub fn inner(&self) -> &KLL<f64> {
        &self.sk
    }

    fn build_state(&self) -> KllState {
        // Wire-format-aligned: read directly from the compactor via
        // the `wire_*` accessors added in asap_sketchlib so the
        // emitted `KllState.levels` / `items` / `coin` bytes match the
        // portable serialization output.
        let (state, bit_cache, remaining_bits) = self.sk.wire_coin();
        KllState {
            k: self.sk.wire_k(),
            m: self.sk.wire_m(),
            num_levels: self.sk.wire_num_levels(),
            levels: self.sk.wire_levels(),
            items: self.sk.wire_items(),
            coin: Some(CoinState {
                state,
                bit_cache,
                remaining_bits,
            }),
            // Emit the raw-f64 item representation (field 5); the
            // value-offset fixed-point encoding (offset/value_scale/
            // residuals, fields 7-9) is left at its off defaults so the
            // wire form is unchanged.
            offset: 0.0,
            value_scale: 0,
            residuals: Vec::new(),
        }
    }

    fn encode_envelope(&self) -> Vec<u8> {
        let env = ProtoEnvelope {
            format_version: 1,
            producer: None,
            hash_spec: None,
            sample_p: 0.0,
            sketch_state: Some(sketch_envelope::SketchState::Kll(self.build_state())),
        };
        let mut buf = Vec::with_capacity(env.encoded_len());
        env.encode(&mut buf).expect("prost encode");
        buf
    }

    fn decode_envelope_into_history(bytes: &[u8]) -> Result<Vec<f64>, PrecomputeError> {
        let env = ProtoEnvelope::decode(bytes)
            .map_err(|e| PrecomputeError::Other(format!("KLLWrapper decode: {e}")))?;
        match env.sketch_state {
            Some(sketch_envelope::SketchState::Kll(s)) => Ok(s.items),
            _ => Err(PrecomputeError::Other(
                "KLLWrapper: envelope did not carry KllState".into(),
            )),
        }
    }
}

fn build_kll(k: i32, seed: Option<u64>) -> KLL<f64> {
    match seed {
        Some(s) => KLL::init_kll_with_seed(k, s),
        None => KLL::init_kll(k),
    }
}

impl Sketch for KLLWrapper {
    fn snapshot(&self) -> Result<Vec<u8>, PrecomputeError> {
        if self.wire_encoding.is_msgpack() {
            // Msgpack reads the compactor directly (not the history vec).
            if self.sk.count() == 0 {
                return Ok(Vec::new());
            }
            return self.encode_msgpack();
        }
        if self.history.is_empty() {
            return Ok(Vec::new());
        }
        Ok(self.encode_envelope())
    }

    fn compute_delta_against(
        &self,
        _prev: &[u8],
        _threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        // KLL uses random compaction and is not additively mergeable
        // in a delta sense. Always return the full snapshot.
        let full = self.snapshot()?;
        Ok(DeltaResult {
            payload: full,
            is_full: true,
        })
    }

    fn apply_delta(&mut self, payload: &[u8]) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        let other_history = Self::decode_envelope_into_history(payload)?;
        for &v in &other_history {
            if v.is_finite() {
                self.history.push(v);
                self.sk.update(&v);
            }
        }
        Ok(())
    }

    fn apply_delta_encoded(
        &mut self,
        payload: &[u8],
        encoding: Encoding,
    ) -> Result<(), PrecomputeError> {
        if payload.is_empty() {
            return Ok(());
        }
        // KLL is full-only; a msgpack frame is a portable KLL full state —
        // merge its compactor. Proto frames keep the history-replay path.
        if encoding.is_msgpack() {
            return self.merge_msgpack(payload);
        }
        self.apply_delta(payload)
    }

    fn merge(&mut self, other: &dyn Sketch) -> Result<(), PrecomputeError> {
        let bytes = other.snapshot()?;
        if bytes.is_empty() {
            return Ok(());
        }
        if self.wire_encoding.is_msgpack() {
            return self.merge_msgpack(&bytes);
        }
        let other_history = Self::decode_envelope_into_history(&bytes)?;
        for &v in &other_history {
            if v.is_finite() {
                self.history.push(v);
                self.sk.update(&v);
            }
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.sk = build_kll(self.k, self.seed);
        self.history.clear();
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl QuantileSketch for KLLWrapper {
    fn quantile(&self, q: f64) -> f64 {
        if self.history.is_empty() {
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
        let w = KLLWrapper::new(200, Some(0xDEAD));
        assert_eq!(w.snapshot().unwrap().len(), 0);
    }

    #[test]
    fn update_then_quantile() {
        let mut w = KLLWrapper::new(200, Some(0xDEAD));
        for i in 1..=100 {
            w.update(i as f64);
        }
        let p50 = w.quantile(0.5);
        assert!((40.0..=60.0).contains(&p50), "p50={p50}");
    }

    #[test]
    fn snapshot_roundtrip_preserves_count() {
        let mut w = KLLWrapper::new(200, Some(0xDEAD));
        for i in 1..=10 {
            w.update(i as f64);
        }
        let bytes = w.snapshot().unwrap();
        let history = KLLWrapper::decode_envelope_into_history(&bytes).unwrap();
        assert_eq!(history.len(), 10);
    }

    #[test]
    fn deterministic_seed_yields_identical_snapshot() {
        let mut a = KLLWrapper::new(200, Some(42));
        let mut b = KLLWrapper::new(200, Some(42));
        for i in 1..=50 {
            a.update(i as f64);
            b.update(i as f64);
        }
        assert_eq!(a.snapshot().unwrap(), b.snapshot().unwrap());
    }
}
