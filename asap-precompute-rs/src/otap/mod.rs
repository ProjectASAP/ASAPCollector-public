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
pub mod answer_nodes;
#[cfg(feature = "otap-engine")]
pub mod baseline_nodes;
#[cfg(feature = "otap-engine")]
pub mod capabilities;
#[cfg(feature = "otap-engine")]
pub mod codec;
#[cfg(feature = "otap-engine")]
pub mod collector_views;
#[cfg(feature = "otap-engine")]
pub mod hh_accuracy;
#[cfg(feature = "otap-engine")]
pub mod planner_views;
#[cfg(feature = "otap-engine")]
pub mod processor;
#[cfg(feature = "otap-engine")]
pub mod raw_forward;
#[cfg(feature = "otap-engine")]
pub mod scenario_generator;
#[cfg(feature = "otap-engine")]
pub mod view_executor;
#[cfg(feature = "otap-engine")]
pub mod window_merge;

pub use config::{ConfigError, PluginConfig, SketchDispatch};
pub use schema::{
    ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SERIES_ID, ATTR_SKETCH_SIZE, ATTR_SKETCH_TYPE,
    ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, META_NODE_ID, META_PLAN_ID, META_VIEW_STATE_KEY,
    SERIES_SCOPE_NAME, VIEW_NAME_INFIX,
};
