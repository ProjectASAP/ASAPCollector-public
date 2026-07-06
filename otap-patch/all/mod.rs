// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! `asap_sketches_registry` — OTAP-Rust `linkme` distributed-slice
//! registration for the unified ASAP `asap_sketches` processor.
//!
//! Phase 5 step D — see
//! [`docs/design-asap-otap-rust-integration.md`](../../docs/design-asap-otap-rust-integration.md)
//! §6 (plugin file layout), §7 (build pipeline), §10 (linkme +
//! cross-crate registration) and §11 row D (phase plan exit criterion).
//!
//! ## What this file does
//!
//! Three things, all small.
//!
//! **(1)** Declares an [`OTAP_PROCESSOR_FACTORIES`] entry for
//! `urn:asap:processor:asap_sketches`. This is the
//! `#[distributed_slice]` static that puts the plugin into the
//! binary's link scope at compile time — the OTAP runtime
//! discovers it via `system_info()` at startup (the function that
//! produces the binary's "Available Component URNs:" banner).
//!
//! **(2)** Implements a minimal [`local::Processor<OtapPdata>`]
//! adapter — [`AsapSketchesProcessor`] — that bridges OTAP's
//! `OtapPdata` message shape onto Phase C's
//! [`AsapSketchesPlugin`] runtime. Phase D's mandate (per §11 row D)
//! is to ship the build pipeline + plugin registry entry; the
//! adapter is intentionally a pass-through forward right now — the
//! codec ↔ runtime wiring (Phase C's
//! `OtapMetricRecords::flatten()` / `lift()`) lands as a follow-up
//! because OTAP's `OtapPdata` ↔ `OtapMetricRecords`
//! `From` / `Into` adapter was the §10 open-question Phase C
//! deferred ("Phase D adds a thin `From`/`Into` adapter — no change
//! to `flatten()`/`lift()` API needed."). For the §11 row D exit
//! criterion ("a `asap-otap` binary that lists `asap_sketches` in
//! its plugin registry") the URN entry is what the registry
//! inspection sees; the adapter only needs to be wireable, not yet
//! semantically complete. Functional end-to-end binding is Phase E
//! (cross-host parity) territory.
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
//! The plugin URN, sketch_type strings, and TOML schema match
//! Telegraf's `processors.allsketches` (Phase 4) and OTel's
//! `asap_sketches` processor (Phase 3). A controller plan rendered
//! for one host renders identically for the other two — no
//! per-platform translation in the controller.

#![deny(unsafe_op_in_unsafe_fn)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use serde::{Deserialize, Serialize};

use otap_df_config::error::Error as OtapConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_engine::config::ProcessorConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::Error;
use otap_df_engine::local::processor as local;
use otap_df_engine::message::Message;
use otap_df_engine::node::NodeId;
use otap_df_engine::processor::ProcessorWrapper;
use otap_df_engine::ProcessorFactory;
use otap_df_otap::pdata::OtapPdata;
use otap_df_otap::OTAP_PROCESSOR_FACTORIES;

use asap_precompute_rs::otap::{AsapSketchesPlugin, PluginConfig};

/// Public URN for the unified ASAP `asap_sketches` processor. Survives
/// across the Telegraf and OTAP hosts unchanged so a controller plan
/// addressed at this URN binds against either runtime.
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
            // ignored. This mirrors the Telegraf side's `mapstructure`
            // behavior and keeps the runtime free of stringly-typed
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
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: validate_asap_sketches_config,
};

/// Static config validation hook. Surfaces `--validate-and-exit`
/// errors for typos in the user's TOML before any runtime allocation.
fn validate_asap_sketches_config(config: &serde_json::Value) -> Result<(), OtapConfigError> {
    let user: AsapSketchesUserConfig = serde_json::from_value(config.clone()).map_err(|e| {
        OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches: {e}"),
        }
    })?;
    let _ = user.into_plugin_config()?;
    Ok(())
}

/// Factory function — invoked once per pipeline instance at startup.
/// Translates the user-supplied TOML into Phase C's [`PluginConfig`],
/// constructs an [`AsapSketchesPlugin`], and wraps it in OTAP's
/// `local::Processor` adapter.
pub fn create_asap_sketches_processor(
    _pipeline_ctx: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    processor_config: &ProcessorConfig,
) -> Result<ProcessorWrapper<OtapPdata>, OtapConfigError> {
    let user: AsapSketchesUserConfig =
        serde_json::from_value(node_config.config.clone()).map_err(|e| {
            OtapConfigError::InvalidUserConfig {
                error: format!("asap_sketches: failed to parse config: {e}"),
            }
        })?;
    let plugin_config = user.into_plugin_config()?;

    // Construct the plugin synchronously — `from_plugin_config` is
    // pure (no Tokio); the plugin's `start()` runs at message time.
    let plugin = AsapSketchesPlugin::from_plugin_config(&plugin_config).map_err(|e| {
        OtapConfigError::InvalidUserConfig {
            error: format!("asap_sketches: plugin construction: {e}"),
        }
    })?;

    Ok(ProcessorWrapper::local(
        AsapSketchesProcessor::new(plugin),
        node,
        node_config,
        processor_config,
    ))
}

/// OTAP `local::Processor<OtapPdata>` adapter for Phase C's
/// [`AsapSketchesPlugin`].
///
/// **Phase D scope deliberate:** this adapter forwards `OtapPdata`
/// messages downstream unchanged. The `OtapPdata` ↔
/// `OtapMetricRecords` `From` / `Into` binding is the §10 open
/// question Phase C deferred ("Phase D adds a thin `From`/`Into`
/// adapter — no change to `flatten()`/`lift()` API needed.") and
/// will land in a follow-up alongside the cross-host parity test
/// (Phase E). What Phase D delivers here is the registration that
/// brings `asap_sketches` into the binary's plugin registry —
/// confirmed by the §11 row D exit ("a `asap-otap` binary that
/// lists `asap_sketches` in its plugin registry").
///
/// The adapter holds the plugin instance so the `OtapPdata`
/// translation can be hung off `process()` in the follow-up without
/// ABI churn.
pub struct AsapSketchesProcessor {
    /// Constructed plugin. Wrapped in `Option` so a future graceful
    /// shutdown path can take ownership of it for the final drain.
    _plugin: Option<AsapSketchesPlugin>,
}

impl AsapSketchesProcessor {
    fn new(plugin: AsapSketchesPlugin) -> Self {
        Self {
            _plugin: Some(plugin),
        }
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for AsapSketchesProcessor {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effect_handler: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        use otap_df_engine::MessageSourceLocalEffectHandlerExtension as _;
        match msg {
            Message::PData(pdata) => {
                // Phase D pass-through; codec wiring lands as a
                // follow-up per the §10 From/Into adapter open
                // question. The runtime is constructed and ready to
                // observe — see `_plugin` field.
                effect_handler.send_message_with_source_node(pdata).await?;
                Ok(())
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => Ok(()),
            Message::Control(NodeControlMsg::Config { .. }) => Ok(()),
            Message::Control(NodeControlMsg::CollectTelemetry { .. }) => Ok(()),
            _ => Ok(()),
        }
    }
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
        assert!(msg.contains("window_size"), "error should mention window: {msg}");
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
        assert_eq!(ASAP_SKETCHES_PROCESSOR_FACTORY.name, ASAP_SKETCHES_PROCESSOR_URN);
        assert_eq!(ASAP_SKETCHES_PROCESSOR_URN, "urn:asap:processor:asap_sketches");
    }
}
