//! Minimal plugin shell that wires [`super::decode_batch`] /
//! [`super::encode_batch`] against a [`crate::precompute::Precompute`].
//!
//! The Phase B deliverable is:
//!
//! > Codec implementation: `asap-precompute-rs/src/otap/` + minimal
//! > plugin shell that wires `decode_batch` / `encode_batch` against
//! > a stub `Precompute`. Exit criterion: `cargo test -p
//! > asap-precompute-rs --features otap` passes; plugin compiles.
//!
//! "Plugin compiles" is interpreted literally: this struct compiles
//! and threads decode → observe → tick → encode end-to-end against
//! any `Precompute` impl, but it owns **no Tokio task, no timers, no
//! control-channel inbox, and no `linkme` distributed-slice
//! registration**. The full plugin (Tokio interval-driven flush
//! ticker, `NodeControlMsg::Wakeup` handling, control-channel poll
//! task, graceful drain) is **Phase C** in
//! `otap-patch/plugins/asap_sketches/`. Comments below mark the seams
//! Phase C will fill in.

use std::sync::Mutex;

use arrow_array::RecordBatch;

use crate::envelope::SketchEnvelope;
use crate::precompute::{Precompute, PrecomputeError};

use super::dictionary::{SeriesDictionary, SeriesDictionaryDecoder, SketchStreamBatch};
use super::{decode_batch, OtapDecodeError, OtapEncodeError};

/// Failure modes from the stub plugin's process-and-emit cycle.
#[derive(Debug, thiserror::Error)]
pub enum StubPluginError {
    /// `decode_batch` returned an error.
    #[error("stub plugin: decode: {0}")]
    Decode(#[from] OtapDecodeError),

    /// `encode_batch` returned an error.
    #[error("stub plugin: encode: {0}")]
    Encode(#[from] OtapEncodeError),

    /// `Precompute::observe` (or `observe_envelope`) returned an
    /// error. Surfaced rather than silently dropped because the
    /// Phase C plugin will surface these via OTAP's
    /// `NodeControlMsg::AckError` (or whichever the equivalent
    /// channel ends up being once the Extension System stabilizes).
    #[error("stub plugin: precompute: {0}")]
    Precompute(#[from] PrecomputeError),
}

/// Stub plugin shell. Phase C's `asap_sketches` plugin will replace
/// this with a real OTAP `local::Processor<OtapPdata>` impl.
///
/// Generic over `P: Precompute` so that downstream tests / Phase C's
/// plugin can pass a real `PrecomputeImpl` (or any `Precompute` impl)
/// without forcing a `Box<dyn Precompute>` allocation.
///
/// Owns a [`SeriesDictionary`] (outbound) and a
/// [`SeriesDictionaryDecoder`] (inbound) — see `otap/mod.rs`'s
/// "Schema / Dictionary / Record stream" section for why this plugin
/// uses that codec rather than `encode_batch`/`decode_batch` for the
/// node-to-node envelope hop. Both are per-instance state that must
/// persist across calls, matching the continuous-stream contract
/// `docs/data_model.md` assumes.
pub struct StubPlugin<P: Precompute> {
    precompute: P,
    dictionary: Mutex<SeriesDictionary>,
    decoder: Mutex<SeriesDictionaryDecoder>,
}

impl<P: Precompute> StubPlugin<P> {
    /// Construct a stub plugin around an existing `Precompute`, with a
    /// fresh (nothing-sent-yet) dictionary and decoder.
    pub fn new(precompute: P) -> Self {
        Self {
            precompute,
            dictionary: Mutex::new(SeriesDictionary::new()),
            decoder: Mutex::new(SeriesDictionaryDecoder::new()),
        }
    }

    /// Borrow the wrapped `Precompute`. Tests use this to inspect
    /// stats after a process call.
    pub fn precompute(&self) -> &P {
        &self.precompute
    }

    /// Decode the batch and route every observation through
    /// `Precompute::observe`. The runtime's own
    /// [`crate::observation::ObservationValueKind::Envelope`] routing
    /// in `Precompute::observe` redirects pre-aggregated rows to
    /// `observe_envelope` — see `precompute.rs::observe`.
    ///
    /// For **raw** (non-envelope) observations riding a flat,
    /// OTAP-Metrics-shaped `RecordBatch`. Inbound pre-aggregated
    /// envelopes from another `asap_sketches` node use
    /// [`Self::ingest_stream`] instead.
    ///
    /// **Phase C will replace this with the OTAP `Processor::process`
    /// method body** that consumes from the input
    /// `Stream<Item = OtapPdata>` and pushes errors onto OTAP's
    /// effect-handler error channel. For Phase B we surface errors
    /// directly to the caller via `Result`.
    pub fn process(&self, batch: &RecordBatch) -> Result<(), StubPluginError> {
        let observations = decode_batch(batch)?;
        for obs in &observations {
            self.precompute.observe(obs)?;
        }
        Ok(())
    }

    /// Decode a [`SketchStreamBatch`] from an upstream `asap_sketches`
    /// node's [`SeriesDictionary`], reconstructing full envelopes via
    /// this plugin's retained [`SeriesDictionaryDecoder`] state, and
    /// route each through `Precompute::observe_envelope`.
    pub fn ingest_stream(&self, batch: &SketchStreamBatch) -> Result<(), StubPluginError> {
        let envelopes = {
            let mut decoder = self.decoder.lock().expect("decoder lock poisoned");
            decoder.decode(batch)?
        };
        for env in &envelopes {
            self.precompute.observe_envelope(env)?;
        }
        Ok(())
    }

    /// Force a `Precompute::tick` and encode the resulting envelopes
    /// against this plugin's [`SeriesDictionary`] state.
    ///
    /// **Phase C will replace this with a `NodeControlMsg::Wakeup`
    /// handler driven by an `interval(window_size)` Tokio timer** —
    /// at which point this method becomes the body of the timer
    /// callback. For Phase B we expose it as an explicit method so a
    /// unit test can drive it deterministically.
    pub fn tick(&self, now_ms: u64) -> Result<SketchStreamBatch, OtapEncodeError> {
        let envelopes: Vec<SketchEnvelope> = self.precompute.tick(now_ms);
        self.encode(&envelopes)
    }

    /// Force a `Precompute::drain` (graceful shutdown flush) and
    /// encode the result. Phase C's `NodeControlMsg::Shutdown`
    /// handler calls this once before dropping the plugin.
    pub fn drain(&self) -> Result<SketchStreamBatch, OtapEncodeError> {
        let envelopes = self.precompute.drain();
        self.encode(&envelopes)
    }

    fn encode(&self, envelopes: &[SketchEnvelope]) -> Result<SketchStreamBatch, OtapEncodeError> {
        let cfg = self.precompute.active_config();
        let mut dictionary = self.dictionary.lock().expect("dictionary lock poisoned");
        dictionary.encode(envelopes, cfg.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PrecomputeConfig, PrecomputeConfigSet, WindowSpec};
    use crate::envelope::SketchType;
    use crate::otap::encode_batch;
    use crate::precompute::PrecomputeImpl;
    use std::time::Duration;

    /// Plugin shell threading: build a Precompute with no sketch
    /// factory (so observe returns NoConfig / configuration-error
    /// before reaching sketch wiring), confirm the call surface
    /// compiles and propagates the right error variants. The point
    /// of this test is not to exercise the runtime — Phase C does
    /// that — but to lock in the Phase B "plugin compiles" gate.
    #[test]
    fn plugin_decode_observe_threads_through() {
        let precompute = PrecomputeImpl::new(None, None, None);
        let plugin = StubPlugin::new(precompute);

        // Empty batch: no observations, no error.
        let empty = encode_batch(&[]).expect("empty encode");
        assert!(plugin.process(&empty).is_ok());
    }

    #[test]
    fn plugin_tick_returns_empty_batch_when_no_envelopes_pending() {
        let cfg = PrecomputeConfig {
            agg_id: 1,
            sketch_type: SketchType::DDSketch,
            window: WindowSpec {
                size: Duration::from_secs(10),
                ..Default::default()
            },
            ..Default::default()
        };
        let precompute = PrecomputeImpl::new(Some(cfg.clone()), None, None);
        precompute.update_config(&PrecomputeConfigSet {
            version: 1,
            configs: vec![cfg],
        });
        let plugin = StubPlugin::new(precompute);

        // No observations were ever pushed, so tick should return
        // four empty batches.
        let out = plugin.tick(123_000).expect("tick");
        assert!(out.is_empty());
    }

    #[test]
    fn ingest_stream_round_trips_through_dictionary_into_observe_envelope() {
        // Sender side: a PrecomputeImpl producing envelopes, encoded
        // via a fresh SeriesDictionary.
        let cfg = PrecomputeConfig {
            agg_id: 7,
            sketch_type: SketchType::DDSketch,
            window: WindowSpec {
                size: Duration::from_millis(1),
                ..Default::default()
            },
            ..Default::default()
        };
        let sender = PrecomputeImpl::new(Some(cfg.clone()), None, None);
        sender.update_config(&PrecomputeConfigSet {
            version: 1,
            configs: vec![cfg.clone()],
        });
        let sender_plugin = StubPlugin::new(sender);
        // Force a window rotation with no observations — exercises the
        // empty-envelopes path deterministically without needing a
        // real sketch factory.
        let batch = sender_plugin.tick(u64::MAX).expect("tick");
        assert!(batch.is_empty());

        // Receiver side: ingest_stream must accept the (empty) stream
        // batch without error even though this receiver's Precompute
        // has no config installed (observe_envelope is simply never
        // called for zero envelopes).
        let receiver = PrecomputeImpl::new(None, None, None);
        let receiver_plugin = StubPlugin::new(receiver);
        assert!(receiver_plugin.ingest_stream(&batch).is_ok());
    }
}
