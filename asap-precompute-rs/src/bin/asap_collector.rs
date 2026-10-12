//! The official OTAP dataflow engine with the ASAP nodes linked in.
//!
//! `asap-collector --config pipeline.yaml` runs any pipeline the upstream
//! `df_engine` runs, plus `urn:asap:processor:asap_sketches` and
//! `urn:asap:receiver:scenario_generator`. Startup, admin endpoints
//! (`/metrics`, `/readyz`) and signal handling are the upstream controller's.
use asap_precompute_rs as _;
use otel_arrow_dfe_config::config_provider::{resolve_config, ConfigFormat};
use otel_arrow_dfe_config::engine::OtelDataflowSpec;
use otel_arrow_dfe_controller::{startup, BuildInfo, Controller, ControllerRunOptions};
use otel_arrow_dfe_core_nodes as _;
use otel_arrow_dfe_otap::OTAP_PIPELINE_FACTORY;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    otel_arrow_dfe_otap::crypto::install_crypto_provider()?;
    let config = std::env::args()
        .skip_while(|a| a != "--config" && a != "-c")
        .nth(1)
        .ok_or("usage: asap-collector --config <file>")?;
    let resolved = resolve_config(Some(&config))?;
    let spec = match resolved.format {
        ConfigFormat::Json => OtelDataflowSpec::from_json(&resolved.content)?,
        ConfigFormat::Yaml => OtelDataflowSpec::from_yaml(&resolved.content)?,
    };
    let options = ControllerRunOptions {
        build_info: BuildInfo {
            service_name: Some(env!("CARGO_BIN_NAME").to_string()),
            service_version: Some(env!("CARGO_PKG_VERSION").to_string()),
        },
        handle_os_signals: true,
        ..Default::default()
    };
    startup::validate_engine_components(&spec, &OTAP_PIPELINE_FACTORY)?;
    startup::validate_controller_extensions(&spec, &options.extensions)?;
    Controller::new(&OTAP_PIPELINE_FACTORY).run_forever_with_options(spec, options)?;
    Ok(())
}
