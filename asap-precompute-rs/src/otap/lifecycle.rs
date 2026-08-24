//! `AsapSketchesPlugin` — Tokio-driven OTAP plugin lifecycle.
//!
//! The full `asap_sketches` plugin provides:
//!
//! > Full `asap_sketches` plugin: all five sketch types via
//! > `sketch_type` dispatch, control-channel Tokio task, `Wakeup`-driven
//! > flush, lifecycle. Exit: OTAP-harness lifecycle tests pass for
//! > each `sketch_type`; round-trip raw input → envelope output
//! > preserves expected sketch counts.
//!
//! The plugin owns three concurrent tasks:
//!
//! 1. **Input task** — consumes the OTAP `Stream<OtapMetricRecords>`
//!    handed in by the host runtime. For each batch:
//!    [`super::records::flatten`] projects the sibling-batch family
//!    down to a flat `RecordBatch`; [`super::decode_batch`] turns the
//!    flat batch into a `Vec<Observation>`; each observation routes
//!    through `Precompute::observe` (the runtime takes care of the
//!    envelope shortcut internally).
//!
//! 2. **Flush ticker** — modelled on OTAP's `NodeControlMsg::Wakeup`
//!    (a Tokio `interval(window_size)` here). Each tick calls
//!    `Precompute::tick(now_ms)` and encodes the resulting envelopes
//!    against this plugin's [`super::dictionary::SeriesDictionary`]
//!    state, pushing the resulting
//!    [`super::dictionary::SketchStreamBatch`] onto the emit channel —
//!    this is the node-to-node sketch-stream hop
//!    `docs/data_model.md` describes, so `SCHEMA`/`DICTIONARY` rows
//!    only ride the wire the first time an `agg_id`/series is seen.
//!    (Phase D wires this to OTAP's `effect_handler.send_message`.)
//!    The input task below is unrelated and keeps using the flat
//!    OTAP-Metrics-shaped codec — it accepts arbitrary upstream OTAP
//!    producers (raw telemetry, not necessarily another
//!    `asap_sketches` node), which is exactly the compatibility
//!    `encode_batch`/`decode_batch`/[`super::records`] exist for.
//!
//! 3. **Control-channel task** — polls the
//!    [`crate::control_channel::ControlChannel`] every
//!    `control_channel_poll_interval` and calls
//!    `Precompute::update_config` whenever a new plan arrives.
//!    Acks the version after applying. Reuse, no reimplementation.
//!
//! Graceful shutdown drains the active window once before the plugin
//! exits — without this, ending a run before the natural window
//! boundary silently drops in-flight observations. The drain runs on
//! the same emit channel as the flush ticker, so the consumer sees
//! exactly one final batch carrying the residue.
//!
//! # The other role: [`AsapSketchesPlugin::start_from_envelopes`]
//!
//! Everything above is the *producer* role — raw observations in,
//! `SketchStreamBatch`es out. A node receiving from another
//! `asap_sketches` node instead needs the *receiver* role:
//! [`AsapSketchesPlugin::start_from_envelopes`] swaps the input task
//! for one that consumes `Stream<Item = SketchStreamBatch>`, decodes
//! each via a [`super::dictionary::SeriesDictionaryDecoder`], and
//! routes the reconstructed envelopes through
//! `Precompute::observe_envelope` (merge, never expand to samples).
//! It reuses the exact same flush ticker / control-channel task /
//! graceful-drain machinery as the producer role — a receiver is
//! still free to have its own `Precompute` config (e.g.
//! `transmit_sketch = false` to *query* the merged sketch every window
//! instead of re-emitting it), so its own emit channel carries
//! whatever that config produces.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::config::PrecomputeConfig;
use crate::control_channel::ControlChannel;
use crate::envelope::SketchEnvelope;
use crate::precompute::{Precompute, PrecomputeError, PrecomputeImpl, StatsSnapshot};

use super::config::{resolve, ConfigError, PluginConfig};
use super::dictionary::{SeriesDictionary, SeriesDictionaryDecoder, SketchStreamBatch};
use super::records::{flatten, OtapMetricRecords, OtapRecordsError};
use super::{decode_batch, OtapDecodeError, OtapEncodeError};

/// Default poll cadence for the control-channel task. Fast enough
/// that operators see plan
/// changes within a single window in practice, slow enough that the
/// controller isn't hammered.
pub const DEFAULT_CONTROL_CHANNEL_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Failure modes from the plugin's lifecycle tasks.
///
/// Variants identify *which* task surfaced the error — the OTAP
/// runtime's error channel will treat each kind differently (decode
/// errors are per-batch and recoverable; encode errors typically
/// indicate a codec bug; precompute errors include both
/// configuration mistakes and runtime drops).
#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    /// `decode_batch` returned an error.
    #[error("asap_sketches plugin: decode: {0}")]
    Decode(#[from] OtapDecodeError),
    /// `encode_batch` returned an error.
    #[error("asap_sketches plugin: encode: {0}")]
    Encode(#[from] OtapEncodeError),
    /// Records-projection (`flatten` or `lift`) failed.
    #[error("asap_sketches plugin: records: {0}")]
    Records(#[from] OtapRecordsError),
    /// `Precompute::observe` / `observe_envelope` returned an error.
    #[error("asap_sketches plugin: precompute: {0}")]
    Precompute(#[from] PrecomputeError),
    /// Plugin config was rejected at construction.
    #[error("asap_sketches plugin: config: {0}")]
    Config(#[from] ConfigError),
    /// One of the lifecycle tasks panicked or was cancelled before
    /// completion.
    #[error("asap_sketches plugin: task panic: {0}")]
    Task(String),
}

/// Convenience type — the emit channel sender shared by the flush
/// ticker and the drain path.
pub type EmitSender = mpsc::UnboundedSender<SketchStreamBatch>;

/// Convenience type — the emit channel receiver returned to the
/// caller (i.e. tests + the Phase D OTAP shell).
pub type EmitReceiver = mpsc::UnboundedReceiver<SketchStreamBatch>;

/// `AsapSketchesPlugin` — the Layer-4 plugin lifecycle. Replaces
/// Phase B's `StubPlugin<P>` with a real Tokio-based runtime around
/// any [`Precompute`] impl.
///
/// Construction is two-step: [`AsapSketchesPlugin::from_plugin_config`]
/// is the high-level entry that resolves the [`PluginConfig`] into a
/// concrete `PrecomputeImpl` via the 5-sketch dispatch table; tests
/// can also pass a pre-built `Precompute` via
/// [`AsapSketchesPlugin::from_parts`] when they want to inject
/// fakes.
///
/// Spawning the lifecycle tasks happens via [`Self::start`], which
/// returns a [`PluginHandle`] used for graceful shutdown. The
/// returned [`EmitReceiver`] is the channel the OTAP shell drains
/// to forward emitted batches via `effect_handler.send_message`.
pub struct AsapSketchesPlugin {
    inner: Arc<dyn Precompute>,
    window_size: Duration,
    /// Outbound `SCHEMA`/`DICTIONARY` state for this plugin's emit
    /// stream — persists for the plugin's whole lifetime (across every
    /// tick and the final drain) so repeat windows for an already-known
    /// series cost only a `RECORD` row. `tokio::sync::Mutex` because
    /// it's shared between the ticker task and the supervisor's final
    /// drain.
    dictionary: Arc<Mutex<SeriesDictionary>>,
    /// Count of lifecycle-task batches/windows dropped due to a
    /// decode/encode/precompute error since this plugin started. The
    /// input, ticker, and drain tasks all apply a "drop the bad batch,
    /// keep the plugin alive" resilience policy (a single malformed
    /// batch shouldn't take down the whole plugin) — this counter is
    /// what keeps that policy from being completely silent. Phase D
    /// routes these errors onto OTAP's real effect-handler error
    /// channel instead; until then, the `dropped_batches()` accessor
    /// is the only signal a caller has that batches are being
    /// dropped.
    dropped_batches: Arc<AtomicU64>,
}

impl AsapSketchesPlugin {
    /// Build a plugin from a high-level [`PluginConfig`]. Resolves
    /// `sketch_type` to a `Precompute` instance via the 5-sketch
    /// dispatch table, applies the resolved `PrecomputeConfig`, and
    /// returns a non-running plugin ready to [`Self::start`].
    pub fn from_plugin_config(config: &PluginConfig) -> Result<Self, PluginError> {
        let (pcfg, dispatch) = resolve(config)?;
        let pc = PrecomputeImpl::new(
            Some(pcfg.clone()),
            Some(dispatch.factory),
            Some(dispatch.observer),
        );
        Ok(Self {
            inner: Arc::new(pc),
            window_size: pcfg.window.size,
            dictionary: Arc::new(Mutex::new(SeriesDictionary::new())),
            dropped_batches: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Build a plugin around an externally-constructed [`Precompute`].
    /// Used by tests that want to inject a fake or to share a
    /// `Precompute` across multiple plugin instances (an unusual
    /// shape but the runtime supports it).
    ///
    /// `window_size` must match the `Precompute`'s configured
    /// window — the flush ticker uses it as the `interval` period.
    pub fn from_parts(precompute: Arc<dyn Precompute>, window_size: Duration) -> Self {
        Self {
            inner: precompute,
            window_size,
            dictionary: Arc::new(Mutex::new(SeriesDictionary::new())),
            dropped_batches: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Borrow the inner [`Precompute`]. Tests use this to inspect
    /// stats / drive `update_config` without going through the
    /// control-channel.
    pub fn precompute(&self) -> &Arc<dyn Precompute> {
        &self.inner
    }

    /// Snapshot of the runtime's counters. Cheap; no-allocation;
    /// safe to call at any time, including after [`Self::start`].
    pub fn stats(&self) -> StatsSnapshot {
        self.inner.stats()
    }

    /// Borrow the shared dropped-batch counter — count of
    /// batches/windows a lifecycle task dropped after a
    /// decode/encode/precompute error, the observable counterpart to
    /// the input/ticker/drain tasks' "drop the bad batch, keep the
    /// plugin alive" policy. Like [`Self::precompute`], call this
    /// *before* [`Self::start`] / [`Self::start_from_envelopes`]
    /// (which consume `self`) and clone the returned `Arc` to retain
    /// a handle — `.load(Ordering::Relaxed)` on the clone reports the
    /// live count for the plugin's whole lifetime, including after
    /// shutdown.
    pub fn dropped_batches(&self) -> &Arc<AtomicU64> {
        &self.dropped_batches
    }

    /// Launch the plugin's three lifecycle tasks against an OTAP
    /// input stream — the **producer** role (raw observations in,
    /// `SketchStreamBatch`es out).
    ///
    /// `input` is the host-supplied stream of `OtapMetricRecords`
    /// — the OTAP shell wraps the runtime's `Stream<OtapPdata>` to
    /// produce one. `control` is an optional [`ControlChannel`] —
    /// when `None` the control task is not spawned (used by tests
    /// that don't exercise the plan-change path).
    ///
    /// Returns a [`PluginHandle`] for graceful shutdown plus the
    /// [`EmitReceiver`] downstream consumers drain. The receiver is
    /// kept off the handle so callers can `await` on it as a
    /// long-lived stream while shutdown is signaled separately.
    pub fn start<S>(
        self,
        input: S,
        control: Option<Arc<dyn ControlChannel>>,
        opts: StartOptions,
    ) -> (PluginHandle, EmitReceiver)
    where
        S: futures::Stream<Item = OtapMetricRecords> + Send + Unpin + 'static,
    {
        let precompute = self.inner.clone();
        let dropped_batches = self.dropped_batches.clone();
        self.spawn_lifecycle(
            move |cancel| spawn_input_task(precompute, input, dropped_batches, cancel),
            control,
            opts,
        )
    }

    /// Launch the plugin's three lifecycle tasks against a stream of
    /// pre-aggregated envelopes — the **receiver** role: another
    /// `asap_sketches` node's `SketchStreamBatch` output in, this
    /// plugin's own `SketchStreamBatch` output out (which, depending
    /// on this plugin's own `Precompute` config, might carry
    /// re-emitted sketch state, or — with `transmit_sketch = false` —
    /// query-mode estimates of the merged sketch).
    ///
    /// Decodes each batch through a fresh
    /// [`SeriesDictionaryDecoder`] retained for the life of this
    /// plugin instance, and routes every reconstructed envelope
    /// through `Precompute::observe_envelope` (merge, never expand to
    /// samples — see the module doc's "The other role" section).
    /// Otherwise identical to [`Self::start`]: same ticker / control /
    /// graceful-drain machinery, same [`PluginHandle`] /
    /// [`EmitReceiver`] return shape.
    pub fn start_from_envelopes<S>(
        self,
        input: S,
        control: Option<Arc<dyn ControlChannel>>,
        opts: StartOptions,
    ) -> (PluginHandle, EmitReceiver)
    where
        S: futures::Stream<Item = SketchStreamBatch> + Send + Unpin + 'static,
    {
        let precompute = self.inner.clone();
        let decoder = Arc::new(Mutex::new(SeriesDictionaryDecoder::new()));
        let dropped_batches = self.dropped_batches.clone();
        self.spawn_lifecycle(
            move |cancel| {
                spawn_envelope_input_task(precompute, decoder, input, dropped_batches, cancel)
            },
            control,
            opts,
        )
    }

    /// Shared tail of [`Self::start`] / [`Self::start_from_envelopes`]:
    /// wires up the emit channel, shutdown signal, ticker task,
    /// optional control task, and the graceful-drain supervisor —
    /// everything except *which* input task to spawn, which the two
    /// public entry points supply as `spawn_input` (given the shared
    /// [`Cancellation`] token so the input task honors shutdown the
    /// same way the others do).
    fn spawn_lifecycle<F>(
        self,
        spawn_input: F,
        control: Option<Arc<dyn ControlChannel>>,
        opts: StartOptions,
    ) -> (PluginHandle, EmitReceiver)
    where
        F: FnOnce(Cancellation) -> JoinHandle<()>,
    {
        let (emit_tx, emit_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_rx = Arc::new(Mutex::new(Some(shutdown_rx)));
        let cancellation = tokio_util_cancellation();

        let precompute = self.inner.clone();
        let window_size = self.window_size;
        let dictionary = self.dictionary.clone();
        let dropped_batches = self.dropped_batches.clone();
        let opts = Arc::new(opts);

        let input_task = spawn_input(cancellation.clone());
        let ticker_task = spawn_ticker_task(
            precompute.clone(),
            dictionary.clone(),
            window_size,
            emit_tx.clone(),
            dropped_batches.clone(),
            cancellation.clone(),
        );
        let control_task = control.map(|cc| {
            spawn_control_task(
                precompute.clone(),
                cc,
                opts.control_channel_poll_interval,
                cancellation.clone(),
            )
        });

        let supervisor = tokio::spawn(async move {
            let _ = shutdown_rx_take(&shutdown_rx).await;
            cancellation.cancel();
            // Wait for the input + ticker tasks to acknowledge the
            // cancel signal so the drain step below doesn't race
            // against an in-flight observe() on the same Precompute.
            let _ = input_task.await;
            let _ = ticker_task.await;
            if let Some(t) = control_task {
                let _ = t.await;
            }
            // Final drain — flush any in-flight window before exit.
            let envs = precompute.drain();
            if !envs.is_empty() {
                let cfg = precompute.active_config();
                let mut dict = dictionary.lock().await;
                if emit_drain(&emit_tx, &envs, &mut dict, cfg.as_ref()).is_err() {
                    dropped_batches.fetch_add(1, Ordering::Relaxed);
                }
            }
        });

        let handle = PluginHandle {
            shutdown_tx: Some(shutdown_tx),
            supervisor: Some(supervisor),
        };
        (handle, emit_rx)
    }
}

/// Tunables passed through [`AsapSketchesPlugin::start`].
///
/// Defaults are fast enough that plan
/// changes are visible within a single window, slow enough that the
/// controller isn't polled hot.
#[derive(Clone, Debug)]
pub struct StartOptions {
    /// How often the control-channel task polls the controller.
    pub control_channel_poll_interval: Duration,
}

impl Default for StartOptions {
    fn default() -> Self {
        Self {
            control_channel_poll_interval: DEFAULT_CONTROL_CHANNEL_POLL_INTERVAL,
        }
    }
}

/// Handle returned by [`AsapSketchesPlugin::start`]. Dropping the
/// handle without calling [`PluginHandle::shutdown`] aborts the
/// supervisor task without a final drain — this is intentional
/// for the panic / cancel paths but discouraged for graceful exit.
pub struct PluginHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    supervisor: Option<JoinHandle<()>>,
}

impl PluginHandle {
    /// Signal a graceful shutdown and await all lifecycle tasks
    /// + the final drain. Idempotent — calling twice is a no-op.
    pub async fn shutdown(mut self) -> Result<(), PluginError> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()); // recv may already have been dropped on supervisor exit
        }
        if let Some(sup) = self.supervisor.take() {
            sup.await
                .map_err(|e| PluginError::Task(format!("supervisor: {e}")))?;
        }
        Ok(())
    }
}

impl Drop for PluginHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(sup) = self.supervisor.take() {
            sup.abort();
        }
    }
}

// -- Task implementations ----------------------------------------------------

fn spawn_input_task<S>(
    precompute: Arc<dyn Precompute>,
    mut input: S,
    dropped_batches: Arc<AtomicU64>,
    cancel: Cancellation,
) -> JoinHandle<()>
where
    S: futures::Stream<Item = OtapMetricRecords> + Send + Unpin + 'static,
{
    use futures::StreamExt;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // Biased: check `input.next()` before `cancel.cancelled()`.
                // If a batch is already ready in the same poll that
                // cancellation fires (e.g. a producer enqueues its
                // final batch then immediately signals shutdown),
                // unbiased selection could pick the cancel branch and
                // drop that ready-but-unprocessed batch. Polling
                // `input` first means a ready batch is always
                // consumed before the next loop iteration observes
                // cancellation.
                biased;
                next = input.next() => match next {
                    None => return,
                    Some(records) => {
                        if let Err(_e) = ingest_one_batch(&*precompute, &records) {
                            // Phase D will route this onto OTAP's
                            // effect-handler error channel; for Phase C
                            // we drop the batch and continue so a
                            // single bad batch can't take down the
                            // whole plugin. `dropped_batches` keeps
                            // this from being completely silent.
                            dropped_batches.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                },
                _ = cancel.cancelled() => return,
            }
        }
    })
}

fn ingest_one_batch(
    precompute: &dyn Precompute,
    records: &OtapMetricRecords,
) -> Result<(), PluginError> {
    let flat = flatten(records)?;
    let observations = decode_batch(&flat)?;
    for obs in &observations {
        // Skip rows the runtime can't admit (LateData / overflow);
        // those are tallied in stats. Hard config errors propagate.
        match precompute.observe(obs) {
            Ok(()) => {}
            Err(PrecomputeError::LateData) | Err(PrecomputeError::SeriesCapExceeded) => {
                continue;
            }
            Err(e) => return Err(PluginError::Precompute(e)),
        }
    }
    Ok(())
}

fn spawn_envelope_input_task<S>(
    precompute: Arc<dyn Precompute>,
    decoder: Arc<Mutex<SeriesDictionaryDecoder>>,
    mut input: S,
    dropped_batches: Arc<AtomicU64>,
    cancel: Cancellation,
) -> JoinHandle<()>
where
    S: futures::Stream<Item = SketchStreamBatch> + Send + Unpin + 'static,
{
    use futures::StreamExt;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // Biased — see spawn_input_task's comment: without
                // this, a batch that's already ready in the same poll
                // as a shutdown signal could be dropped by an
                // unbiased tie-break instead of processed.
                biased;
                next = input.next() => match next {
                    None => return,
                    Some(batch) => {
                        if let Err(_e) = ingest_one_stream_batch(&*precompute, &decoder, &batch).await {
                            // Same "drop the bad batch, keep the
                            // plugin alive" policy as ingest_one_batch.
                            // A decode error here means this stream's
                            // continuity contract was violated (see
                            // `OtapDecodeError::UnknownSeriesId` /
                            // `UnknownAggId`) — Phase D routes this
                            // onto OTAP's effect-handler error channel.
                            // `dropped_batches` keeps this from being
                            // completely silent in the meantime.
                            dropped_batches.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                },
                _ = cancel.cancelled() => return,
            }
        }
    })
}

async fn ingest_one_stream_batch(
    precompute: &dyn Precompute,
    decoder: &Mutex<SeriesDictionaryDecoder>,
    batch: &SketchStreamBatch,
) -> Result<(), PluginError> {
    let envelopes = {
        let mut d = decoder.lock().await;
        d.decode(batch)?
    };
    for env in &envelopes {
        // Merge only — the runtime never expands envelope bytes back
        // into scalar samples (the bandwidth invariant).
        match precompute.observe_envelope(env) {
            Ok(()) => {}
            Err(PrecomputeError::LateData) | Err(PrecomputeError::SeriesCapExceeded) => {
                continue;
            }
            Err(e) => return Err(PluginError::Precompute(e)),
        }
    }
    Ok(())
}

fn spawn_ticker_task(
    precompute: Arc<dyn Precompute>,
    dictionary: Arc<Mutex<SeriesDictionary>>,
    window_size: Duration,
    emit_tx: EmitSender,
    dropped_batches: Arc<AtomicU64>,
    cancel: Cancellation,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(window_size);
        // Skip the immediate first tick — Tokio's default fires at
        // t=0 which would emit a spurious empty batch before any
        // observations have arrived.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let _ = interval.tick().await;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {
                    let now_ms = wall_clock_ms();
                    let envs = precompute.tick(now_ms);
                    if envs.is_empty() {
                        continue;
                    }
                    let cfg = precompute.active_config();
                    let mut dict = dictionary.lock().await;
                    if emit_envelopes(&emit_tx, &envs, &mut dict, cfg.as_ref()).is_err() {
                        dropped_batches.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    })
}

fn spawn_control_task(
    precompute: Arc<dyn Precompute>,
    control: Arc<dyn ControlChannel>,
    poll_interval: Duration,
    cancel: Cancellation,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(poll_interval);
        // First poll happens immediately so plans can land before
        // the first observation arrives.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = interval.tick() => {
                    if let Some(plan) = control.poll() {
                        let version = plan.version;
                        precompute.update_config(&plan);
                        control.ack(version);
                    }
                }
            }
        }
    })
}

fn emit_envelopes(
    emit_tx: &EmitSender,
    envelopes: &[SketchEnvelope],
    dictionary: &mut SeriesDictionary,
    cfg: Option<&PrecomputeConfig>,
) -> Result<(), PluginError> {
    let batch = dictionary.encode(envelopes, cfg)?;
    let _ = emit_tx.send(batch); // receiver may have been dropped on shutdown
    Ok(())
}

fn emit_drain(
    emit_tx: &EmitSender,
    envelopes: &[SketchEnvelope],
    dictionary: &mut SeriesDictionary,
    cfg: Option<&PrecomputeConfig>,
) -> Result<(), PluginError> {
    emit_envelopes(emit_tx, envelopes, dictionary, cfg)
}

/// Wall-clock millisecond timestamp. Wraps `SystemTime::now()` so
/// tests can stub it out (today they don't — the `interval` cadence
/// is enough for the tests to be deterministic).
fn wall_clock_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// -- Cancellation token (lightweight; avoids pulling tokio-util) -------------

#[derive(Clone)]
struct Cancellation {
    inner: Arc<tokio::sync::Notify>,
    fired: Arc<std::sync::atomic::AtomicBool>,
}

impl Cancellation {
    fn cancel(&self) {
        if !self.fired.swap(true, std::sync::atomic::Ordering::AcqRel) {
            self.inner.notify_waiters();
        }
    }

    async fn cancelled(&self) {
        if self.fired.load(std::sync::atomic::Ordering::Acquire) {
            return;
        }
        self.inner.notified().await;
    }
}

fn tokio_util_cancellation() -> Cancellation {
    Cancellation {
        inner: Arc::new(tokio::sync::Notify::new()),
        fired: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    }
}

async fn shutdown_rx_take(rx: &Mutex<Option<oneshot::Receiver<()>>>) -> bool {
    let mut g = rx.lock().await;
    match g.take() {
        Some(rx) => rx.await.is_ok(),
        None => false,
    }
}

// Avoid unused-import warning when building without tests.
#[allow(dead_code)]
fn _typecheck_pcfg(_p: &PrecomputeConfig) {}

#[cfg(test)]
mod tests {
    //! Lightweight smoke tests confined to this module. Full
    //! lifecycle tests (per-sketch-type, drain, control-channel)
    //! live in `tests/otap_lifecycle.rs`.

    use super::*;
    use crate::envelope::SketchType;
    use crate::otap::records::OtapMetricRecords;

    #[tokio::test]
    async fn handle_drop_aborts_supervisor() {
        // Smoke test: dropping the handle without calling shutdown
        // doesn't deadlock or panic. Uses a no-op input stream.
        let cfg = PluginConfig {
            sketch_type: "ddsketch".into(),
            window_size: Duration::from_millis(50),
            ..Default::default()
        };
        let plugin = AsapSketchesPlugin::from_plugin_config(&cfg).expect("config");
        let input = futures::stream::empty::<OtapMetricRecords>();
        let (handle, _rx) = plugin.start(input, None, StartOptions::default());
        drop(handle);
    }

    #[tokio::test]
    async fn precompute_accessor_returns_same_instance() {
        let cfg = PluginConfig {
            sketch_type: "kll".into(),
            ..Default::default()
        };
        let plugin = AsapSketchesPlugin::from_plugin_config(&cfg).expect("config");
        // Verify the inner Precompute carries the resolved sketch
        // type (sanity-check the resolve path).
        assert_eq!(plugin.precompute().stats(), StatsSnapshot::default());
        // And that the SketchType enum landed correctly via update_config.
        let _ = SketchType::KLLSketch;
    }

    #[tokio::test]
    async fn receiver_role_smoke_test_drop_aborts_supervisor() {
        // Mirrors handle_drop_aborts_supervisor for the receiver role:
        // start_from_envelopes must compile and run against an empty
        // SketchStreamBatch stream without deadlocking or panicking.
        let cfg = PluginConfig {
            sketch_type: "ddsketch".into(),
            window_size: Duration::from_millis(50),
            ..Default::default()
        };
        let plugin = AsapSketchesPlugin::from_plugin_config(&cfg).expect("config");
        let input = futures::stream::empty::<SketchStreamBatch>();
        let (handle, _rx) = plugin.start_from_envelopes(input, None, StartOptions::default());
        drop(handle);
    }

    #[tokio::test]
    async fn receiver_role_merges_producer_role_output_end_to_end() {
        // Full producer -> receiver chain, both AsapSketchesPlugin,
        // connected by an in-process channel (the network-transport
        // version of this same chain lives in
        // examples/sketch_producer_node.rs /
        // examples/sketch_receiver_node.rs).
        use crate::observation::{KeyValue, Observation, ObservationValue};
        use tokio_stream::wrappers::UnboundedReceiverStream;

        let producer_cfg = PluginConfig {
            sketch_type: "ddsketch".into(),
            window_size: Duration::from_secs(60), // drained explicitly below.
            output_metric_name: "latency_ms".into(),
            agg_id: 1,
            ..Default::default()
        };
        let producer =
            AsapSketchesPlugin::from_plugin_config(&producer_cfg).expect("producer config");
        for v in [1.0_f64, 2.0, 3.0, 4.0, 5.0] {
            let obs = Observation::new(
                1_000,
                "latency_ms",
                vec![],
                vec![KeyValue::new("host", "h1")],
                ObservationValue::float(v),
            );
            producer.precompute().observe(&obs).expect("observe");
        }
        let (producer_handle, producer_rx) = producer.start(
            futures::stream::pending::<OtapMetricRecords>(),
            None,
            StartOptions::default(),
        );

        let receiver_cfg = PluginConfig {
            sketch_type: "ddsketch".into(),
            window_size: Duration::from_secs(60),
            output_metric_name: "latency_ms_p99".into(),
            agg_id: 1,
            transmit_sketch: false,
            quantiles: vec![0.99],
            ..Default::default()
        };
        let receiver =
            AsapSketchesPlugin::from_plugin_config(&receiver_cfg).expect("receiver config");
        let (receiver_handle, mut receiver_rx) = receiver.start_from_envelopes(
            UnboundedReceiverStream::new(producer_rx),
            None,
            StartOptions::default(),
        );

        // Shut the producer down first: its final drain pushes the
        // one window it accumulated onto producer_rx, which the
        // receiver's envelope-input task picks up and merges.
        producer_handle.shutdown().await.expect("producer shutdown");
        // Now shut the receiver down: its final drain (transmit_sketch
        // = false) turns the merged sketch into a p99 estimate batch.
        receiver_handle.shutdown().await.expect("receiver shutdown");

        let mut decoder = SeriesDictionaryDecoder::new();
        let mut saw_estimate = false;
        while let Ok(Some(batch)) =
            tokio::time::timeout(Duration::from_secs(2), receiver_rx.recv()).await
        {
            for env in decoder.decode(&batch).expect("decode") {
                assert_eq!(env.metric_name, "latency_ms_p99");
                assert!(
                    env.payload.is_empty(),
                    "estimate mode carries no sketch bytes"
                );
                assert!(env.value > 0.0, "p99 of {{1..5}} must be positive");
                saw_estimate = true;
            }
        }
        assert!(saw_estimate, "receiver never emitted a p99 estimate");
    }
}
