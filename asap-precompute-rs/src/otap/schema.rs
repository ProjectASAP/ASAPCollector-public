//! Attribute keys of the native OTAP sketch carrier.
//!
//! A hand-configured sketch: SCHEMA keys ride on the Resource,
//! `sketch.series_id` on the Scope, and RECORD keys on each data point (see
//! `docs/data_model.md`). A collector view's state keeps the received
//! Resource, Scope and point attributes; its view identity is the Metric's
//! metadata (`asap.*`) and its state the point's `sketch.envelope`.

/// ASAPv1 sketch envelope bytes from `asap_sketchlib` (RECORD).
pub const ATTR_ENVELOPE: &str = "sketch.envelope";
/// Canonical [`crate::envelope::SketchType::name`] (SCHEMA).
pub const ATTR_SKETCH_TYPE: &str = "sketch.type";
/// Envelope schema version (SCHEMA).
pub const ATTR_SCHEMA_VERSION: &str = "sketch.schema_version";
/// Exclusive lower bound of the window `(start, end]`, Unix milliseconds (RECORD).
pub const ATTR_WINDOW_START_MS: &str = "sketch.window_start_ms";
/// Inclusive upper bound of the window `(start, end]`, Unix milliseconds (RECORD).
pub const ATTR_WINDOW_END_MS: &str = "sketch.window_end_ms";
/// Stable series identity on the Scope (DICTIONARY / LABELS).
pub const ATTR_SERIES_ID: &str = "sketch.series_id";
/// Optional sketch size/accuracy parameters (SCHEMA).
pub const ATTR_SKETCH_SIZE: &str = "sketch.size";
/// Scope name of a hand-configured sketch's series (DICTIONARY / LABELS):
/// its attributes are the series labels.
pub const SERIES_SCOPE_NAME: &str = "asap.series";
/// Metric metadata of a collector view's state: `PhysicalASAPDAG::plan_id()`
/// of the installed plan (string).
pub const META_PLAN_ID: &str = "asap.plan_id";
/// Metric metadata of a collector view's state: the view's node id (int).
pub const META_NODE_ID: &str = "asap.node_id";
/// Metric metadata of a collector view's state: the planner's
/// `ViewStateKey`, as JSON (string).
pub const META_VIEW_STATE_KEY: &str = "asap.view_state_key";
/// A collector view's metric is named `<source metric>.asap_view.<node id>`;
/// names containing it are reserved.
pub const VIEW_NAME_INFIX: &str = ".asap_view.";
