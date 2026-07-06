//! `asap-precompute-rs` is the host-neutral edge precompute runtime.
//!
//! This crate owns the windowing, snapshot caching, and delta encoding
//! runtime for the Rust **edge** runtime. Future Rust-based
//! edge agents (Vector adapter, OTAP-Rust, Arrow-backed shims)
//! consume this crate.
//!
//! Per-platform Adapter implementations (the Layer-4 shims) translate
//! their host's native event into [`Observation`], hand it to a
//! [`Precompute`], and translate the runtime's emitted
//! [`SketchEnvelope`] back to the host's native event.

#![warn(missing_docs)]
// The sketch/sampling code intentionally guards against NaN with the
// negated-comparison idiom `!(p > 0.0)` (which rejects NaN, unlike the
// clippy-suggested `p <= 0.0`). Allow the lint crate-wide rather than
// rewrite five behavior-sensitive comparisons.
#![allow(clippy::neg_cmp_op_on_partial_ord)]

pub mod adapter;
pub mod config;
pub mod control_channel;
pub mod envelope;
pub mod matchers;
pub mod observation;
#[cfg(feature = "otap")]
pub mod otap;
pub mod precompute;
pub mod sampling;
pub mod sketches;
pub mod snapshot_cache;
pub mod window;

pub use config::{
    AggId, AggregationMode, OnOverflow, PrecomputeConfig, PrecomputeConfigSet, SketchParams,
    WindowSpec,
};
pub use envelope::{Encoding, SketchEnvelope, SketchType};
pub use matchers::{LabelMatcher, MatchOp};
pub use observation::{KeyValue, Observation, ObservationValue, ObservationValueKind};
pub use precompute::{
    CardinalitySketch, FrequencyEntry, FrequencySketch, Precompute, QuantileSketch, SampleSetter,
    Sketch, SketchObserver,
};
