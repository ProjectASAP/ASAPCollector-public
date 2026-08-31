//! OpenTelemetry Arrow Protocol integration.
//!
//! With `otap-engine`, [`codec`] binds sketch envelopes directly to native
//! `OtapPdata`: sketch records are OTLP Summary data points carrying canonical
//! ASAPv1 bytes in `SummaryDpAttrs["sketch.envelope"]`; scalar estimates are
//! Gauge number data points. [`processor`] registers the corresponding local
//! OTAP dataflow processor.
//!
//! The flat `RecordBatch` codec and `records` projection remain available to
//! standalone adapters. Node-to-node transport must use native `OtapPdata` or
//! standard OTLP metrics protobuf, not a private sketch batch type.

mod decode;
mod encode;
mod schema;

pub mod config;
pub mod records;

#[cfg(feature = "otap-engine")]
pub mod codec;
#[cfg(feature = "otap-engine")]
pub mod processor;

pub use config::{ConfigError, PluginConfig, SketchDispatch};
pub use decode::{decode_batch, OtapDecodeError};
pub use encode::{encode_batch, OtapEncodeError};
pub use records::{flatten, lift, OtapMetricRecords, OtapRecordsError};
pub use schema::{
    ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SKETCH_TYPE,
    ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};
