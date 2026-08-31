// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! `linkme` distributed-slice registration for the unified ASAP
//! `asap_sketches` processor, and the real
//! `local::Processor<OtapPdata>` adapter itself.
//!
//! ## What this file does
//!
//! **(1)** Declares an [`OTAP_PROCESSOR_FACTORIES`] entry for
//! `urn:asap:processor:asap_sketches`. This is the
//! `#[distributed_slice]` static that puts the plugin into the
//! binary's link scope at compile time — the OTAP runtime discovers
//! it via `system_info()` at startup (the function that produces the
//! binary's "Available Component URNs:" banner).
//!
//! **(2)** Implements a real [`local::Processor<OtapPdata>`] adapter —
//! [`AsapSketchesProcessor`] — that ingests real OTAP metric traffic,
//! aggregates it, and emits sketch results back out as real OTAP
//! metric traffic. Per-`Message::PData` and per-timer-tick, it drives
//! a bare `Precompute` instance directly. The actual
//! `OtapPdata <-> Vec<Observation>` conversion lives in [`super::codec`].
//!
//! **(3)** Validates the user-facing TOML config against
//! [`crate::otap::PluginConfig`]'s shape — the `validate_config` hook
//! lets `df_engine --validate-and-exit` catch typos before runtime.
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
//! ## There is exactly one transport: the pipeline
//!
//! [`AsapSketchesProcessor`] only ever sends via
//! `effect_handler.send_message_with_source_node` — whatever this
//! node's own pipeline wiring connects it to. An earlier revision also
//! carried a direct-TCP "wire lane" (a hand-rolled
//! `OtapWireWriter`/`OtapWireReader` pair, plus a standalone
//! `AsapSketchesReceiver` node) as an alternative transport; that's
//! gone.
//!
//! The real OTAP path now expresses the protocol structurally: aggregation
//! SCHEMA is attached to Resource rows, series DICTIONARY/LABELS to Scope
//! rows, and sketch RECORD fields to SummaryDataPoint attribute rows. OTAP's own
//! parent IDs carry the joins, while its stateful IPC producer retains Arrow
//! schemas and emits dictionary deltas. There is no parallel private sketch
//! transport; node boundaries carry native `OtapPdata` (or standard OTLP
//! metrics protobuf when crossing an OS-process boundary).
//!
//! ## Provenance / verification status
//!
//! Build/lint/test-verified against upstream `open-telemetry/otel-arrow`
//! commit `3e85c3460361446ebfce99e9f35fffd2dd5ab740` (2026-08-24) via
//! the plain git dependency this module's crate carries under the
//! `otap-engine` feature (see `Cargo.toml`) — `cargo build --features
//! otap-engine` / `cargo clippy --features otap-engine --all-targets
//! -D warnings` / `cargo fmt --check` / `cargo test --features
//! otap-engine` all pass without any manual staging into a local
//! checkout of that repo.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use serde::{Deserialize, Serialize};

use otel_arrow_dfe_config::error::Error as OtapConfigError;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::effect_handler::TimerCancelHandle;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::ProcessorFactory;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;

use crate::config::PrecomputeConfigSet;
use crate::envelope::{Encoding, SketchEnvelope};
use crate::otap::config::resolve as resolve_plugin_config;
use crate::otap::PluginConfig;
use crate::precompute::{Precompute, PrecomputeError, PrecomputeImpl};

use super::codec::{decode_pdata_to_observations, OtapSketchEncoder};

/// Public URN for the unified ASAP `asap_sketches` processor. Survives
/// across hosts unchanged so a controller plan addressed at this URN
/// binds against any runtime.
pub const ASAP_SKETCHES_PROCESSOR_URN: &str = "urn:asap:processor:asap_sketches";

/// User-facing TOML / YAML config for the OTAP `asap_sketches` plugin.
/// Mirrors `plugins/asap_sketches/sample.toml` 1:1.
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
    /// Optional fallback key for CountSketch observations without labels.
    #[serde(default)]
    pub default_key: Option<String>,
    /// Exclude resource attributes from series identity.
    #[serde(default)]
    pub omit_resource_attrs: bool,
    /// Collapse every observation into one series.
    #[serde(default)]
    pub global_aggregation: bool,
    /// Add sample-count and window-duration labels to emitted rows.
    #[serde(default)]
    pub emit_window_stats: bool,
    /// Sketch payload encoding. Defaults to `ProtoFull`.
    #[serde(default)]
    pub encoding: Option<Encoding>,
    /// Emit deltas after the first full snapshot.
    #[serde(default)]
    pub delta_transmission: bool,
    /// Emit sketch envelopes when true; emit scalar estimates when false.
    #[serde(default = "default_transmit_sketch")]
    pub transmit_sketch: bool,
    /// Quantiles emitted by DDSketch/KLL in estimate mode.
    #[serde(default)]
    pub quantiles: Vec<f64>,
    /// Bootstrap controller URL. Optional.
    #[serde(default)]
    pub controller_url: Option<String>,
    /// Bootstrap agent identifier reported to the controller.
    #[serde(default)]
    pub agent_id: Option<String>,
}

impl AsapSketchesUserConfig {
    /// Translate the user-facing config into the runtime config.
    /// `sketch_params` deserializes lazily — the runtime walks the
    /// JSON map per sketch type.
    fn into_plugin_config(self) -> Result<PluginConfig, OtapConfigError> {
        use crate::config::SketchParams;

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
            default_key: self.default_key,
            omit_resource_attrs: self.omit_resource_attrs,
            global_aggregation: self.global_aggregation,
            emit_window_stats: self.emit_window_stats,
            encoding: self.encoding.unwrap_or(Encoding::ProtoFull),
            delta_transmission: self.delta_transmission,
            transmit_sketch: self.transmit_sketch,
            quantiles: self.quantiles,
        };
        // Bounce the config through the resolver so config mistakes
        // (unsupported sketch_type, zero window) surface here rather
        // than at first observe().
        let _ = resolve_plugin_config(&cfg).map_err(|e| OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches: {e}"),
        })?;
        Ok(cfg)
    }
}

const fn default_transmit_sketch() -> bool {
    true
}

fn requires_precompute_rebuild(current: &PluginConfig, next: &PluginConfig) -> bool {
    !current.sketch_type.eq_ignore_ascii_case(&next.sketch_type)
        || current.sketch_params != next.sketch_params
        || current.encoding != next.encoding
        || current.default_key != next.default_key
        || current.agg_id != next.agg_id
        || current.omit_resource_attrs != next.omit_resource_attrs
        || current.global_aggregation != next.global_aggregation
        || (current.sketch_type.eq_ignore_ascii_case("countsketch")
            && current.default_key.is_none()
            && current.output_metric_name != next.output_metric_name)
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
/// Translates the user-supplied TOML into [`PluginConfig`], resolves
/// it to a `Precompute` instance, and wraps it in OTAP's
/// `local::Processor` adapter.
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
    let plugin_config = user.into_plugin_config()?;

    let (precompute_config, dispatch) =
        resolve_plugin_config(&plugin_config).map_err(|e| OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches: configuration: {e}"),
        })?;
    let precompute: Arc<dyn Precompute> = Arc::new(PrecomputeImpl::new(
        Some(precompute_config),
        Some(dispatch.factory),
        Some(dispatch.observer),
    ));

    Ok(ProcessorWrapper::local(
        AsapSketchesProcessor::new(precompute, plugin_config),
        node,
        node_config,
        processor_config,
    ))
}

/// OTAP `local::Processor<OtapPdata>` adapter — the real `OtapPdata`
/// binding ([`super::codec`]) driving a bare [`Precompute`] instance
/// directly. `Precompute::observe`/`tick`/`drain` are callback-style, so
/// OTAP's per-message/per-timer `process()` calls need no bridge.
///
/// Sends exclusively via `effect_handler.send_message_with_source_node`
/// — see this module's doc, "There is exactly one transport: the
/// pipeline", for why there's no second, direct-TCP transport here.
pub struct AsapSketchesProcessor {
    precompute: Arc<dyn Precompute>,
    plugin_config: PluginConfig,
    window_size: Duration,
    /// `effect_handler.start_periodic_timer` is `async` and OTAP has
    /// no dedicated "processor started" hook — armed on the first
    /// `process()` call instead (`Message::PData` or any control
    /// message) rather than at construction time.
    timer: Option<TimerCancelHandle<OtapPdata>>,
    /// `PrecomputeConfigSet::version` for the next `NodeControlMsg::Config`
    /// this processor applies — monotonically increasing, independent
    /// of any external controller's own versioning since this
    /// adapter's `Precompute` never talks to a `ControlChannel`.
    next_config_version: Arc<AtomicU64>,
    /// Stable SCHEMA/DICTIONARY identity for this processor's output stream.
    sketch_encoder: OtapSketchEncoder,
}

impl AsapSketchesProcessor {
    fn new(precompute: Arc<dyn Precompute>, plugin_config: PluginConfig) -> Self {
        let window_size = plugin_config.window_size;
        let sketch_encoder =
            OtapSketchEncoder::with_sketch_params(plugin_config.sketch_params.clone());
        Self {
            precompute,
            plugin_config,
            window_size,
            timer: None,
            next_config_version: Arc::new(AtomicU64::new(1)),
            sketch_encoder,
        }
    }

    /// Encodes and emits one window's worth of envelopes, if any —
    /// shared by the `TimerTick` (regular flush) and `Shutdown`
    /// (final drain) paths. `envs` empty is a no-op, matching
    /// `Precompute::tick`/`drain`'s own "nothing to flush" contract.
    ///
    /// [`OtapSketchEncoder`] builds the real `OtapPdata` directly from
    /// `envs`, retains stable series IDs across calls, and maps the protocol
    /// tiers onto native Resource/Scope/DataPoint joins. There is no
    /// private intermediate transport.
    async fn emit_envelopes(
        &mut self,
        envs: &[SketchEnvelope],
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        use otel_arrow_dfe_engine::MessageSourceLocalEffectHandlerExtension as _;

        if envs.is_empty() {
            return Ok(());
        }
        let pdata = match self.sketch_encoder.encode(envs) {
            Ok(pdata) => pdata,
            Err(_e) => return Ok(()), // drop the bad window, keep the processor alive
        };
        effect_handler.send_message_with_source_node(pdata).await?;
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
        if self.timer.is_none() {
            // Best-effort: if this fails, the processor still ingests
            // and observes correctly, it just never flushes a window
            // on its own. Retaining the handle lets a later config push
            // cancel and recreate the timer when window_size changes.
            self.timer = effect_handler
                .start_periodic_timer(self.window_size)
                .await
                .ok();
        }

        match msg {
            Message::PData(pdata) => {
                let outcome = match decode_pdata_to_observations(pdata) {
                    Ok(outcome) => outcome,
                    Err(_e) => return Ok(()), // drop the bad batch, keep the processor alive
                };
                if outcome.skipped_non_scalar > 0 {
                    effect_handler
                        .info(&format!(
                            "asap_sketches: skipped {} unsupported histogram/exponential-histogram data point(s)",
                            outcome.skipped_non_scalar
                        ))
                        .await;
                }
                for obs in &outcome.observations {
                    // Note: `observe` here handles both "genuine OTLP
                    // metric" and "sketch shipped as binary inside an
                    // OTAP metric" (an upstream asap_sketches node's
                    // `sketch.envelope` Summary output) — no branching
                    // needed at this call site. `decode_pdata_to_observations`
                    // already tags the latter as
                    // `ObservationValueKind::Envelope`, and
                    // `Precompute::observe` already routes those
                    // internally to `observe_envelope` (merge as a
                    // pre-aggregated sketch) instead of expanding them
                    // to scalar samples. See `codec.rs`'s module doc
                    // ("Scope") for the full picture.
                    //
                    // LateData / SeriesCapExceeded are expected,
                    // already-tallied-in-stats outcomes (mirrors
                    // the processor's ingest policy) — silent by design.
                    // Anything else (e.g. NoConfig,
                    // AggIdMismatch) indicates a real misconfiguration
                    // and is at least surfaced via `effect_handler.info`
                    // — a real error channel is follow-up work, but
                    // this keeps it from being completely invisible.
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
                let Ok((pcfg, dispatch)) = resolve_plugin_config(&plugin_config) else {
                    return Ok(());
                };

                // The sketch factory and observer are fixed when a
                // PrecomputeImpl is constructed. If a push changes the
                // algorithm or its construction parameters, updating only
                // PrecomputeConfig would stamp the new type onto sketches
                // still produced by the old implementation. Flush the old
                // instance and replace it with a consistently constructed
                // one instead.
                let factory_changed =
                    requires_precompute_rebuild(&self.plugin_config, &plugin_config);
                if factory_changed {
                    let replacement: Arc<dyn Precompute> = Arc::new(PrecomputeImpl::new(
                        Some(pcfg.clone()),
                        Some(dispatch.factory),
                        Some(dispatch.observer),
                    ));
                    let pending = self.precompute.drain();
                    self.emit_envelopes(&pending, effect_handler).await?;
                    self.precompute = replacement;
                    self.sketch_encoder =
                        OtapSketchEncoder::with_sketch_params(plugin_config.sketch_params.clone());
                } else {
                    let version = self.next_config_version.fetch_add(1, Ordering::Relaxed);
                    self.precompute.update_config(&PrecomputeConfigSet {
                        version,
                        configs: vec![pcfg],
                    });
                }

                if plugin_config.window_size != self.window_size {
                    if let Some(timer) = self.timer.take() {
                        let _ = timer.cancel().await;
                    }
                    self.timer = Some(
                        effect_handler
                            .start_periodic_timer(plugin_config.window_size)
                            .await?,
                    );
                    self.window_size = plugin_config.window_size;
                }
                self.plugin_config = plugin_config;
                Ok(())
            }
            Message::Control(NodeControlMsg::CollectTelemetry { .. }) => Ok(()),
            _ => Ok(()),
        }
    }
}

/// Current wall-clock timestamp in milliseconds.
fn asap_wall_clock_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    //! Unit tests confined to config-shape validation — full lifecycle
    //! integration tests live in `tests/otap_pipeline_e2e.rs`.

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
        assert!(cfg.transmit_sketch);
        assert!(cfg.quantiles.is_empty());
        assert!(cfg.encoding.is_none());
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
    fn estimate_and_wire_controls_are_threaded_through() {
        let user: AsapSketchesUserConfig = serde_json::from_value(json!({
            "sketch_type": "ddsketch",
            "window_size": "5s",
            "output_metric_name": "request.duration.p99",
            "encoding": "Msgpack",
            "delta_transmission": true,
            "transmit_sketch": false,
            "quantiles": [0.5, 0.99],
            "omit_resource_attrs": true,
            "global_aggregation": true,
            "emit_window_stats": true
        }))
        .expect("extended control-plane shape parses");
        let plugin = user.into_plugin_config().expect("extended config resolves");
        assert_eq!(plugin.encoding, Encoding::Msgpack);
        assert!(plugin.delta_transmission);
        assert!(!plugin.transmit_sketch);
        assert_eq!(plugin.quantiles, vec![0.5, 0.99]);
        assert!(plugin.omit_resource_attrs);
        assert!(plugin.global_aggregation);
        assert!(plugin.emit_window_stats);
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

    #[test]
    fn algorithm_and_factory_parameter_changes_require_rebuild() {
        let current = PluginConfig::default();

        let mut algorithm_change = current.clone();
        algorithm_change.sketch_type = "hll".to_string();
        assert!(requires_precompute_rebuild(&current, &algorithm_change));

        let mut parameter_change = current.clone();
        parameter_change
            .sketch_params
            .insert("relative_accuracy".to_string(), 0.005);
        assert!(requires_precompute_rebuild(&current, &parameter_change));

        let mut runtime_only_change = current.clone();
        runtime_only_change.output_metric_name = "renamed".to_string();
        runtime_only_change.window_size = Duration::from_secs(60);
        assert!(!requires_precompute_rebuild(&current, &runtime_only_change));

        let mut encoding_change = current.clone();
        encoding_change.encoding = Encoding::Msgpack;
        assert!(requires_precompute_rebuild(&current, &encoding_change));

        let mut series_identity_change = current.clone();
        series_identity_change.global_aggregation = true;
        assert!(requires_precompute_rebuild(
            &current,
            &series_identity_change
        ));

        let mut aggregation_plan_change = current.clone();
        aggregation_plan_change.agg_id = 9;
        assert!(requires_precompute_rebuild(
            &current,
            &aggregation_plan_change
        ));

        let mut countsketch = current.clone();
        countsketch.sketch_type = "countsketch".to_string();
        let mut countsketch_rename = countsketch.clone();
        countsketch_rename.output_metric_name = "renamed".to_string();
        assert!(requires_precompute_rebuild(
            &countsketch,
            &countsketch_rename
        ));
    }
}
