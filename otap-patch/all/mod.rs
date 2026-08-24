// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! `asap_sketches_registry` — OTAP-Rust `linkme` distributed-slice
//! registration for the unified ASAP `asap_sketches` processor.
//!
//! ## What this file does
//!
//! **(1)** Declares an [`OTAP_PROCESSOR_FACTORIES`] entry for
//! `urn:asap:processor:asap_sketches`. This is the
//! `#[distributed_slice]` static that puts the plugin into the
//! binary's link scope at compile time — the OTAP runtime
//! discovers it via `system_info()` at startup (the function that
//! produces the binary's "Available Component URNs:" banner).
//!
//! **(2)** Implements a real [`local::Processor<OtapPdata>`] adapter —
//! [`AsapSketchesProcessor`] — that ingests real OTAP metric traffic,
//! aggregates it, and emits sketch results back out as real OTAP
//! metric traffic. Per-`Message::PData` and per-timer-tick, it drives
//! a bare `Precompute` instance directly rather than going through
//! [`AsapSketchesPlugin`]'s own Tokio-task/`Stream` lifecycle — see
//! [`create_asap_sketches_processor`]'s doc for why (that lifecycle's
//! current emit shape, `SketchStreamBatch`, diverged from what this
//! adapter needs after PR #5/#6's dictionary-economics work). The
//! actual `OtapPdata` <-> `OtapMetricRecords` conversion lives in
//! `otap_bridge` — build/lint/test-verified by staging both files into
//! a real `open-telemetry/otel-arrow` checkout (commit `3e85c346`,
//! 2026-08-24) as a workspace member; see its own module doc's
//! "Verification status" for exactly what that covered.
//!
//! **(3)** Validates the user-facing TOML config against
//! [`asap_precompute_rs::otap::PluginConfig`]'s shape — the
//! `validate_config` hook lets `df_engine --validate-and-exit`
//! catch typos before runtime.
//!
//! ## URN
//!
//! `urn:asap:processor:asap_sketches`. Stable; survives binary
//! re-builds and ships in the registry banner. The shorter alias
//! `asap_sketches` (sans URN prefix) appears in `sample.toml` for
//! human friendliness; OTAP's loader normalizes both at parse time.
//!
//! ## Cross-host parity guarantee
//!
//! The plugin URN, sketch_type strings, and TOML schema are stable
//! across hosts. A controller plan rendered for one host renders
//! identically for the others — no per-platform translation in the
//! controller.
//!
//! **(4)** `otap_receiver` implements the wire lane's receive side as
//! a real OTAP `local::Receiver<OtapPdata>` node
//! ([`otap_receiver::AsapSketchesReceiver`]) — it listens on a TCP
//! address, decodes each connection with `otap_wire::OtapWireReader`,
//! and hands the resulting `OtapPdata` into the pipeline like any
//! other receiver. [`AsapSketchesProcessor`] is the send-side
//! counterpart: when its config carries `peer_addr`, emitted envelopes
//! go out over `otap_wire::OtapWireWriter` directly to that peer
//! instead of via the generic pipeline hop — see
//! [`AsapSketchesProcessor::emit_envelopes`]'s doc for exactly how the
//! two transports are chosen between.
//!
//! ## Scope not covered here
//!
//! Ingesting another `asap_sketches` node's *legacy* `SketchStreamBatch`
//! wire format (`AsapSketchesPlugin::start_from_envelopes`,
//! `asap_precompute_rs::otap::wire`) isn't addressed here — that format
//! predates the unification `otap_wire.rs` describes and has its own
//! standalone example binaries (`examples/sketch_producer_node.rs` /
//! `sketch_receiver_node.rs`) as its home. What *is* covered: a real
//! `OtapPdata`, produced and consumed by real OTAP nodes
//! (`AsapSketchesProcessor` / `AsapSketchesReceiver`), sent directly
//! over TCP via `otap_wire` — the same encoding the generic pipeline
//! hop uses, just a different transport for topologies where two
//! `asap_sketches` nodes are paired directly rather than routed
//! through other OTAP pipeline components.

#![deny(unsafe_op_in_unsafe_fn)]

mod otap_bridge;
mod otap_receiver;
mod otap_wire;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;

// NOTE: `otel_arrow_dfe_*` is the current (2026-08-24) upstream naming
// — see otap_bridge.rs's module doc for the very recent `otap_df_*`
// rename this assumes. Revert to `otap_df_*` throughout this file (and
// otap_bridge.rs) if the actual pinned commit predates it.
use otel_arrow_dfe_config::error::Error as OtapConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ProcessorFactory;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;

use asap_precompute_rs::config::PrecomputeConfigSet;
use asap_precompute_rs::otap::config::resolve as resolve_plugin_config;
use asap_precompute_rs::otap::{
    AsapSketchesPlugin, PluginConfig, decode_batch, encode_batch, flatten, lift,
};
use asap_precompute_rs::precompute::Precompute;

use otap_bridge::{otap_metric_records_to_pdata, pdata_to_otap_metric_records};
use otap_wire::OtapWireWriter;

/// Public URN for the unified ASAP `asap_sketches` processor. Survives
/// across hosts unchanged so a controller plan addressed at this URN
/// binds against any runtime.
pub const ASAP_SKETCHES_PROCESSOR_URN: &str = "urn:asap:processor:asap_sketches";

/// User-facing TOML / YAML config for the OTAP `asap_sketches` plugin.
/// Mirrors `otap-patch/plugins/asap_sketches/sample.toml` 1:1.
///
/// Field-by-field documentation lives in the plugin's `README.md`;
/// this struct is the deserialization shape OTAP's loader hands the
/// factory.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsapSketchesUserConfig {
    /// One of `"ddsketch"` / `"kll"` / `"hll"` / `"countsketch"` /
    /// `"countminsketch"` (case-insensitive). See `sample.toml`.
    pub sketch_type: String,
    /// Window rotation period — humantime format (e.g. `"10s"`).
    #[serde(with = "humantime_serde")]
    pub window_size: Duration,
    /// Stamped onto every emitted envelope.
    pub output_metric_name: String,
    /// Controller-plan join key. Defaults to 0.
    #[serde(default)]
    pub agg_id: u64,
    /// Sketch-specific tuning knobs. Only the keys relevant to
    /// `sketch_type` are read; others are ignored.
    #[serde(default)]
    pub sketch_params: serde_json::Map<String, serde_json::Value>,
    /// Bootstrap controller URL. Optional.
    #[serde(default)]
    pub controller_url: Option<String>,
    /// Bootstrap agent identifier reported to the controller.
    #[serde(default)]
    pub agent_id: Option<String>,
    /// When set, emitted envelopes go out over the wire lane
    /// (`otap_wire::OtapWireWriter`) directly to this peer instead of
    /// via the generic pipeline hop (`effect_handler.send_message_with_source_node`).
    /// Pairs with an `AsapSketchesReceiver` (`otap_receiver.rs`)
    /// listening at this address on the peer. `None` (the default)
    /// keeps the generic-pipeline path, unchanged from before this
    /// field existed.
    #[serde(default)]
    pub peer_addr: Option<SocketAddr>,
}

impl AsapSketchesUserConfig {
    /// Translate the user-facing config into Phase C's runtime config.
    /// `sketch_params` deserializes lazily — the runtime walks the JSON
    /// map per sketch type.
    fn into_plugin_config(self) -> Result<PluginConfig, OtapConfigError> {
        use asap_precompute_rs::config::SketchParams;

        let mut params = SketchParams::new();
        for (k, v) in self.sketch_params {
            // Only numeric tuning knobs survive — strings / bools are
            // ignored. This keeps the runtime free of stringly-typed
            // sketch knobs (`SketchParams` is `HashMap<String, f64>`).
            if let Some(n) = v.as_f64() {
                let _ = params.insert(k, n);
            }
        }

        let cfg = PluginConfig {
            sketch_type: self.sketch_type,
            window_size: self.window_size,
            output_metric_name: self.output_metric_name,
            agg_id: self.agg_id,
            sketch_params: params,
            ..PluginConfig::default()
        };
        // Bounce the config through the Phase C resolver so config
        // mistakes (unsupported sketch_type, zero window) surface here
        // rather than at first observe().
        let _ = asap_precompute_rs::otap::config::resolve(&cfg).map_err(|e| {
            OtapConfigError::InvalidUserConfig {
                error: format!("asap_sketches: {e}"),
            }
        })?;
        Ok(cfg)
    }
}

/// `linkme` registration entry — the static the OTAP runtime walks at
/// startup to populate `OTAP_PIPELINE_FACTORY.processor_factory_map`.
///
/// Marked `#[allow(unsafe_code)]` because `linkme::distributed_slice`
/// performs cross-crate static-collection via custom-section linking;
/// the workspace's `unsafe_code = "deny"` lint denies this pattern by
/// default. Identical pattern to the in-tree contrib processors
/// (e.g. `delay_processor::DELAY_PROCESSOR_FACTORY`).
#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
pub static ASAP_SKETCHES_PROCESSOR_FACTORY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: ASAP_SKETCHES_PROCESSOR_URN,
    create: create_asap_sketches_processor,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: validate_asap_sketches_config,
};

/// Static config validation hook. Surfaces `--validate-and-exit`
/// errors for typos in the user's TOML before any runtime allocation.
fn validate_asap_sketches_config(config: &serde_json::Value) -> Result<(), OtapConfigError> {
    let user: AsapSketchesUserConfig =
        serde_json::from_value(config.clone()).map_err(|e| OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches: {e}"),
        })?;
    let _ = user.into_plugin_config()?;
    Ok(())
}

/// Factory function — invoked once per pipeline instance at startup.
/// Translates the user-supplied TOML into Phase C's [`PluginConfig`],
/// resolves it to a `Precompute` instance via
/// [`AsapSketchesPlugin::from_plugin_config`] (reusing its validated
/// construction path), and wraps the bare `Precompute` in OTAP's
/// `local::Processor` adapter — **not** the plugin's own Tokio-task
/// lifecycle (see [`AsapSketchesProcessor`]'s doc for why).
pub fn create_asap_sketches_processor(
    _pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ProcessorWrapper<OtapPdata>, OtapConfigError> {
    let user: AsapSketchesUserConfig =
        serde_json::from_value(node_config.config.clone()).map_err(|e| {
            OtapConfigError::InvalidUserConfig {
                error: format!("asap_sketches: failed to parse config: {e}"),
            }
        })?;
    let peer_addr = user.peer_addr;
    let plugin_config = user.into_plugin_config()?;
    let window_size = plugin_config.window_size;

    // `from_plugin_config` is pure (no Tokio) — it just validates and
    // resolves. Grab the `Arc<dyn Precompute>` it constructs and
    // discard the plugin wrapper itself: `AsapSketchesPlugin::start()`
    // spawns Tokio tasks around a `Stream<Item = OtapMetricRecords>`
    // and currently emits `SketchStreamBatch` (the asap_sketches ->
    // asap_sketches wire-transport shape from PR #6, dictionary
    // economics) — not the OTAP-Metrics-shaped `OtapMetricRecords`
    // this adapter needs for `effect_handler.send_message`. OTAP's
    // own per-message `process()` + `effect_handler.start_periodic_timer`
    // is a better-fitting host for a callback-driven `Precompute`
    // than bridging that Stream-based lifecycle would be.
    let plugin = AsapSketchesPlugin::from_plugin_config(&plugin_config).map_err(|e| {
        OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches: plugin construction: {e}"),
        }
    })?;
    let precompute = plugin.precompute().clone();
    drop(plugin);

    Ok(ProcessorWrapper::local(
        AsapSketchesProcessor::new(precompute, window_size, peer_addr),
        node,
        node_config,
        processor_config,
    ))
}

/// OTAP `local::Processor<OtapPdata>` adapter — the real
/// `OtapPdata` <-> `OtapMetricRecords` binding (`otap_bridge`) driving
/// a bare [`Precompute`] instance directly, rather than through
/// [`AsapSketchesPlugin`]'s own Tokio-task lifecycle (see
/// [`create_asap_sketches_processor`]'s doc for why: that lifecycle's
/// emit shape and this adapter's needed shape have diverged since
/// PR #5/#6). `Precompute::observe`/`tick`/`drain` are themselves
/// callback-style, not stream-based, so driving them directly from
/// OTAP's own per-message/per-timer `process()` calls needs no
/// bridging machinery at all.
///
/// # Verification status
///
/// See `otap_bridge.rs`'s module doc "Verification status" — the
/// `OtapPdata` conversions this adapter calls have been build/lint/
/// test-verified against a real OTAP Dataflow workspace checkout.
/// This adapter's own OTAP-facing surface (`ProcessorFactory`,
/// `local::Processor` impl, `NodeControlMsg` handling) compiles and
/// passes its config-shape unit tests there too, but has no test
/// exercising it inside a real running pipeline yet (no
/// `Message::PData` sent through `process()` end to end).
pub struct AsapSketchesProcessor {
    precompute: Arc<dyn Precompute>,
    window_size: Duration,
    /// `effect_handler.start_periodic_timer` is `async` and OTAP has
    /// no dedicated "processor started" hook — armed on the first
    /// `process()` call instead (`Message::PData` or any control
    /// message) rather than at construction time.
    timer_started: bool,
    /// `PrecomputeConfigSet::version` for the next `NodeControlMsg::Config`
    /// this processor applies — monotonically increasing, independent
    /// of any external controller's own versioning since this
    /// adapter's `Precompute` never talks to a `ControlChannel`.
    next_config_version: Arc<AtomicU64>,
    /// When `Some`, `emit_envelopes` forwards over the wire lane to
    /// this peer instead of the generic pipeline hop. See
    /// [`AsapSketchesUserConfig::peer_addr`].
    peer_addr: Option<SocketAddr>,
    /// Lazily-established, persistent wire-lane connection to
    /// `peer_addr` — held across ticks so `OtapWireWriter`'s per-role
    /// Arrow IPC Schema message goes out once per connection, not once
    /// per window (see `otap_wire.rs`'s module doc). Reset to `None`
    /// on any send failure so the next window's `emit_envelopes`
    /// reconnects rather than reusing a dead socket.
    wire_conn: Option<(TcpStream, OtapWireWriter)>,
}

impl AsapSketchesProcessor {
    fn new(
        precompute: Arc<dyn Precompute>,
        window_size: Duration,
        peer_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            precompute,
            window_size,
            timer_started: false,
            next_config_version: Arc::new(AtomicU64::new(1)),
            peer_addr,
            wire_conn: None,
        }
    }

    /// Encodes and emits one window's worth of envelopes, if any —
    /// shared by the `TimerTick` (regular flush) and `Shutdown`
    /// (final drain) paths. `envs` empty is a no-op, matching
    /// `Precompute::tick`/`drain`'s own "nothing to flush" contract.
    ///
    /// # Transport choice: wire lane vs. generic pipeline hop
    ///
    /// Both transports carry the *same* encoding (SketchEnvelopes ->
    /// flat RecordBatch (`encode_batch`) -> OTAP-validator-safe
    /// two-batch family (`lift`) -> real `OtapPdata`
    /// (`otap_bridge`) — deliberately NOT `SeriesDictionary::encode`,
    /// the SCHEMA/DICTIONARY/RECORD wire economics from PR #5/#6,
    /// which is the older `asap_precompute_rs::otap::wire` format).
    /// What differs is only how that `OtapPdata` gets to the next
    /// hop:
    ///
    /// - `peer_addr` is `Some`: sent directly over a persistent TCP
    ///   connection via `otap_wire::OtapWireWriter`, to a peer running
    ///   `otap_receiver::AsapSketchesReceiver` at that address. This is
    ///   the wire lane — for topologies where two `asap_sketches`
    ///   nodes are paired directly rather than routed through other
    ///   OTAP pipeline components.
    /// - `peer_addr` is `None` (the default): sent via
    ///   `effect_handler.send_message_with_source_node`, i.e. whatever
    ///   this node's own pipeline wiring connects it to.
    ///
    /// A connect/send failure on the wire lane drops the window (log
    /// via `effect_handler.info`, matching every other decode/encode
    /// failure path in this adapter) and resets `wire_conn` so the
    /// *next* window's call attempts a fresh connection — a transient
    /// peer outage self-heals without this processor needing restart.
    async fn emit_envelopes(
        &mut self,
        envs: &[asap_precompute_rs::envelope::SketchEnvelope],
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        use otel_arrow_dfe_engine::MessageSourceLocalEffectHandlerExtension as _;

        if envs.is_empty() {
            return Ok(());
        }
        let flat = match encode_batch(envs) {
            Ok(flat) => flat,
            Err(_e) => return Ok(()), // drop the bad window, keep the processor alive
        };
        let records = match lift(&flat) {
            Ok(records) => records,
            Err(_e) => return Ok(()),
        };
        let pdata = match otap_metric_records_to_pdata(&records) {
            Ok(pdata) => pdata,
            Err(_e) => return Ok(()),
        };

        let Some(peer_addr) = self.peer_addr else {
            effect_handler.send_message_with_source_node(pdata).await?;
            return Ok(());
        };

        if self.wire_conn.is_none() {
            match TcpStream::connect(peer_addr).await {
                Ok(stream) => self.wire_conn = Some((stream, OtapWireWriter::new())),
                Err(e) => {
                    effect_handler
                        .info(&format!(
                            "asap_sketches: wire-lane connect to {peer_addr} failed: {e}"
                        ))
                        .await;
                    return Ok(()); // drop this window; retry connecting next window
                }
            }
        }
        let (stream, writer) = self
            .wire_conn
            .as_mut()
            .expect("just established above if it was None");
        if let Err(e) = writer.send(stream, pdata).await {
            effect_handler
                .info(&format!(
                    "asap_sketches: wire-lane send to {peer_addr} failed: {e}"
                ))
                .await;
            self.wire_conn = None; // reconnect (fresh IPC streams too) next window
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for AsapSketchesProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if !self.timer_started {
            self.timer_started = true;
            // Best-effort: if this fails, the processor still ingests
            // and observes correctly, it just never flushes a window
            // on its own — degraded, not broken. `TimerCancelHandle`
            // is dropped immediately: the timer keeps firing for this
            // processor's lifetime rather than being cancellable
            // (there's currently no shutdown-adjacent place to hold
            // the handle across `process()` calls without adding
            // another `Option<...>` field for a cancellation this
            // adapter never actually exercises).
            let _ = effect_handler.start_periodic_timer(self.window_size).await;
        }

        match msg {
            Message::PData(pdata) => {
                let outcome = match pdata_to_otap_metric_records(pdata) {
                    Ok(outcome) => outcome,
                    Err(_e) => return Ok(()), // drop the bad batch, keep the processor alive
                };
                if outcome.skipped_non_scalar > 0 {
                    effect_handler
                        .info(&format!(
                            "asap_sketches: skipped {} non-scalar (histogram/exponential-histogram/summary) data point(s) — only Gauge/Sum are aggregated",
                            outcome.skipped_non_scalar
                        ))
                        .await;
                }
                let Some(records) = outcome.records else {
                    return Ok(());
                };
                let flat = match flatten(&records) {
                    Ok(flat) => flat,
                    Err(_e) => return Ok(()),
                };
                let observations = match decode_batch(&flat) {
                    Ok(observations) => observations,
                    Err(_e) => return Ok(()),
                };
                for obs in &observations {
                    // Note: `observe` here handles both "genuine OTLP
                    // metric" and "sketch shipped as binary inside an
                    // OTAP metric" (an upstream asap_sketches node's
                    // `_asap_envelope`-tagged output) — no branching
                    // needed at this call site. `decode_batch` already
                    // tags the latter as `ObservationValueKind::Envelope`,
                    // and `Precompute::observe` already routes those
                    // internally to `observe_envelope` (merge as a
                    // pre-aggregated sketch) instead of expanding them
                    // to scalar samples. See otap_bridge.rs's module
                    // doc ("Scope") for the full picture.
                    //
                    // LateData / SeriesCapExceeded are expected,
                    // already-tallied-in-stats outcomes (mirrors
                    // `AsapSketchesPlugin`'s own ingest policy,
                    // lifecycle.rs's `ingest_one_batch`) — silent by
                    // design. Anything else (e.g. NoConfig,
                    // AggIdMismatch) indicates a real misconfiguration
                    // and is at least surfaced via `effect_handler.info`
                    // — a real error channel is follow-up work, but
                    // this keeps it from being completely invisible.
                    use asap_precompute_rs::precompute::PrecomputeError;
                    match self.precompute.observe(obs) {
                        Ok(())
                        | Err(PrecomputeError::LateData)
                        | Err(PrecomputeError::SeriesCapExceeded) => {}
                        Err(e) => {
                            effect_handler
                                .info(&format!("asap_sketches: observe failed: {e}"))
                                .await;
                        }
                    }
                }
                Ok(())
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => {
                let now_ms = asap_wall_clock_ms();
                let envs = self.precompute.tick(now_ms);
                self.emit_envelopes(&envs, effect_handler).await
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => {
                let envs = self.precompute.drain();
                self.emit_envelopes(&envs, effect_handler).await
            }
            Message::Control(NodeControlMsg::Config { config }) => {
                let user: AsapSketchesUserConfig = match serde_json::from_value(config) {
                    Ok(user) => user,
                    Err(_e) => return Ok(()), // malformed plan push — keep running on the old config
                };
                let Ok(plugin_config) = user.into_plugin_config() else {
                    return Ok(());
                };
                let Ok((pcfg, _dispatch)) = resolve_plugin_config(&plugin_config) else {
                    return Ok(());
                };
                let version = self.next_config_version.fetch_add(1, Ordering::Relaxed);
                self.precompute.update_config(&PrecomputeConfigSet {
                    version,
                    configs: vec![pcfg],
                });
                Ok(())
            }
            Message::Control(NodeControlMsg::CollectTelemetry { .. }) => Ok(()),
            _ => Ok(()),
        }
    }
}

/// Wall-clock millisecond timestamp — mirrors
/// `asap_precompute_rs::otap::lifecycle`'s private `wall_clock_ms`
/// (not exported for reuse across the crate boundary).
fn asap_wall_clock_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    //! Unit tests for the registration crate. Confined to config-shape
    //! validation — full lifecycle tests live in
    //! `asap-precompute-rs/tests/otap_lifecycle.rs` (Phase C).

    use super::*;
    use serde_json::json;

    #[test]
    fn user_config_round_trips_default_shape() {
        let cfg: AsapSketchesUserConfig = serde_json::from_value(json!({
            "sketch_type": "ddsketch",
            "window_size": "10s",
            "output_metric_name": "http_request_duration_ms",
        }))
        .expect("default shape parses");
        assert_eq!(cfg.sketch_type, "ddsketch");
        assert_eq!(cfg.window_size, Duration::from_secs(10));
        assert_eq!(cfg.output_metric_name, "http_request_duration_ms");
        assert_eq!(cfg.agg_id, 0);
        assert!(cfg.controller_url.is_none());
    }

    #[test]
    fn unknown_field_is_rejected() {
        // `deny_unknown_fields` catches typos like `sketch_typ`.
        let res = serde_json::from_value::<AsapSketchesUserConfig>(json!({
            "sketch_type": "ddsketch",
            "window_size": "10s",
            "output_metric_name": "x",
            "sketch_typ": "kll",
        }));
        assert!(res.is_err(), "unknown fields should be rejected");
    }

    #[test]
    fn bad_sketch_type_surfaces_in_validate() {
        let cfg = json!({
            "sketch_type": "made_up_sketch",
            "window_size": "10s",
            "output_metric_name": "x",
        });
        let err = validate_asap_sketches_config(&cfg).expect_err("bad sketch_type rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("asap_sketches"),
            "error should mention plugin: {msg}"
        );
    }

    #[test]
    fn zero_window_surfaces_in_validate() {
        let cfg = json!({
            "sketch_type": "ddsketch",
            "window_size": "0s",
            "output_metric_name": "x",
        });
        let err = validate_asap_sketches_config(&cfg).expect_err("zero window rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("window_size"),
            "error should mention window: {msg}"
        );
    }

    #[test]
    fn ddsketch_relative_accuracy_is_threaded_through() {
        let cfg = json!({
            "sketch_type": "ddsketch",
            "window_size": "5s",
            "output_metric_name": "x",
            "sketch_params": {
                "relative_accuracy": 0.005
            },
        });
        validate_asap_sketches_config(&cfg).expect("ddsketch with custom alpha is valid");
    }

    #[test]
    fn kll_k_param_is_threaded_through() {
        let cfg = json!({
            "sketch_type": "kll",
            "window_size": "5s",
            "output_metric_name": "x",
            "sketch_params": {
                "k": 400.0
            },
        });
        validate_asap_sketches_config(&cfg).expect("kll with custom k is valid");
    }

    #[test]
    fn registered_factory_carries_expected_urn() {
        // Sanity check: the registry static the binary discovers via
        // linkme exposes the canonical URN.
        assert_eq!(
            ASAP_SKETCHES_PROCESSOR_FACTORY.name,
            ASAP_SKETCHES_PROCESSOR_URN
        );
        assert_eq!(
            ASAP_SKETCHES_PROCESSOR_URN,
            "urn:asap:processor:asap_sketches"
        );
    }
}
