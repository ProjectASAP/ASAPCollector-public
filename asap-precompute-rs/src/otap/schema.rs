//! Attribute keys of the native OTAP sketch carrier.
//!
//! SCHEMA keys ride on the Resource, `sketch.series_id` on the Scope, and
//! RECORD keys on each data point (see `docs/data_model.md`).

/// ASAPv1 sketch envelope bytes from `asap_sketchlib` (RECORD).
pub const ATTR_ENVELOPE: &str = "sketch.envelope";
/// Canonical [`crate::envelope::SketchType::name`] (SCHEMA).
pub const ATTR_SKETCH_TYPE: &str = "sketch.type";
/// Controller-plan join key (SCHEMA).
pub const ATTR_AGG_ID: &str = "sketch.agg_id";
/// Envelope schema version (SCHEMA).
pub const ATTR_SCHEMA_VERSION: &str = "sketch.schema_version";
/// Inclusive window lower bound, Unix milliseconds (RECORD).
pub const ATTR_WINDOW_START_MS: &str = "sketch.window_start_ms";
/// Exclusive window upper bound, Unix milliseconds (RECORD).
pub const ATTR_WINDOW_END_MS: &str = "sketch.window_end_ms";
/// Stable series identity on the Scope (DICTIONARY / LABELS).
pub const ATTR_SERIES_ID: &str = "sketch.series_id";
/// Optional sketch size/accuracy parameters (SCHEMA).
pub const ATTR_SKETCH_SIZE: &str = "sketch.size";
