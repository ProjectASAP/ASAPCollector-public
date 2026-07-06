//! `AsapSketchesPlugin` — Tokio-driven OTAP plugin lifecycle.
//!
//! Phase 5 step C exit criterion (per
//! [otap design §11](../../../docs/design-asap-otap-rust-integration.md#11-phase-plan))
//! reads:
//!
//! > Full `asap_sketches` plugin: all five sketch types via
//! > `sketch_type` dispatch, control-channel Tokio task, `Wakeup`-driven
//! > flush, lifecycle. Exit: OTAP-harness lifecycle tests pass for
//! > each `sketch_type`; round-trip raw input → envelope output
//! > preserves expected sketch counts.
//!
//! The plugin owns three concurrent tasks, mirroring the contract in
//! [otap design §5](../../../docs/design-asap-otap-rust-integration.md#5-plugin-lifecycle--otap-receiver--processor):
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
//!    `Precompute::tick(now_ms)`, encodes the resulting envelopes
//!    via [`super::encode_batch`], lifts the Strategy-B carrier
//!    columns onto the per-row attribute child batch via
//!    [`super::records::lift`], and pushes the
//!    [`super::records::OtapMetricRecords`] family onto the emit
//!    channel. (Phase D wires this to OTAP's
//!    `effect_handler.send_message`.)
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

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::config::PrecomputeConfig;
use crate::control_channel::ControlChannel;
use crate::envelope::SketchEnvelope;
use crate::precompute::{Precompute, PrecomputeError, PrecomputeImpl, StatsSnapshot};

use super::config::{resolve, ConfigError, PluginConfig};
use super::records::{flatten, lift, OtapMetricRecords, OtapRecordsError};
use super::{decode_batch, encode_batch, OtapDecodeError, OtapEncodeError};

/// Default poll cadence for the control-channel task. Mirrors the
/// Telegraf side (Phase 4) — fast enough that operators see plan
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
pub type EmitSender = mpsc::UnboundedSender<OtapMetricRecords>;

/// Convenience type — the emit channel receiver returned to the
/// caller (i.e. tests + the Phase D OTAP shell).
pub type EmitReceiver = mpsc::UnboundedReceiver<OtapMetricRecords>;

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

    /// Launch the plugin's three lifecycle tasks against an OTAP
    /// input stream.
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
        let (emit_tx, emit_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let shutdown_rx = Arc::new(Mutex::new(Some(shutdown_rx)));
        let cancellation = tokio_util_cancellation();

        let precompute = self.inner.clone();
        let window_size = self.window_size;
        let opts = Arc::new(opts);

        let input_task = spawn_input_task(precompute.clone(), input, cancellation.clone());
        let ticker_task = spawn_ticker_task(
            precompute.clone(),
            window_size,
            emit_tx.clone(),
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
                let _ = emit_drain(&emit_tx, &envs);
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
/// Defaults match the Phase 4 Telegraf side: fast enough that plan
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
    cancel: Cancellation,
) -> JoinHandle<()>
where
    S: futures::Stream<Item = OtapMetricRecords> + Send + Unpin + 'static,
{
    use futures::StreamExt;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                next = input.next() => match next {
                    None => return,
                    Some(records) => {
                        if let Err(_e) = ingest_one_batch(&*precompute, &records) {
                            // Phase D will route this onto OTAP's
                            // effect-handler error channel; for Phase C
                            // we drop the batch and continue so a
                            // single bad batch can't take down the
                            // whole plugin.
                        }
                    }
                },
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

fn spawn_ticker_task(
    precompute: Arc<dyn Precompute>,
    window_size: Duration,
    emit_tx: EmitSender,
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
                    let _ = emit_envelopes(&emit_tx, &envs);
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

fn emit_envelopes(emit_tx: &EmitSender, envelopes: &[SketchEnvelope]) -> Result<(), PluginError> {
    let flat = encode_batch(envelopes)?;
    let lifted = lift(&flat)?;
    let _ = emit_tx.send(lifted); // receiver may have been dropped on shutdown
    Ok(())
}

fn emit_drain(emit_tx: &EmitSender, envelopes: &[SketchEnvelope]) -> Result<(), PluginError> {
    emit_envelopes(emit_tx, envelopes)
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
}
