//! OpenTelemetry Arrow Protocol integration.
//!
//! With `otap-engine`, [`codec`] binds sketch envelopes directly to native
//! `OtapPdata`: sketch records are OTLP Summary data points carrying ASAPv1
//! bytes from `asap_sketchlib` in `SummaryDpAttrs["sketch.envelope"]`; scalar
//! estimates are Gauge number data points. [`processor`] registers the
//! corresponding local OTAP dataflow processor.

mod schema;

pub mod config;

#[cfg(feature = "otap-engine")]
pub mod codec;
#[cfg(feature = "otap-engine")]
pub mod processor;

pub use config::{ConfigError, PluginConfig, SketchDispatch};
pub use schema::{
    ATTR_AGG_ID, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SERIES_ID, ATTR_SKETCH_SIZE,
    ATTR_SKETCH_TYPE, ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS,
};
