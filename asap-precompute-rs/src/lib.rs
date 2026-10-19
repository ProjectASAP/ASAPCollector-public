//! `asap-precompute-rs` is the host-neutral edge precompute runtime.
//!
//! This crate owns the windowing runtime for the Rust **edge** runtime.
//! A host translates its native event into [`Observation`], hands it to
//! a [`Precompute`], and translates each emitted [`SketchEnvelope`]
//! (an ASAPv1 sketch payload from `asap_sketchlib` plus metadata) back
//! to its own format; the OTAP processor in `otap` is the bundled host.

#![warn(missing_docs)]
// The sketch code intentionally guards against NaN with the
// negated-comparison idiom `!(x > 0.0)` (which rejects NaN, unlike the
// clippy-suggested `x <= 0.0`). Allow the lint crate-wide rather than
// rewrite behavior-sensitive comparisons.
#![allow(clippy::neg_cmp_op_on_partial_ord)]

pub mod answers;
pub mod config;
pub mod envelope;
pub mod kll_windows;
pub mod matchers;
pub mod observation;
#[cfg(feature = "otap")]
pub mod otap;
pub mod precompute;
pub mod sketches;
pub mod window;

pub use config::{
    AggId, AggregationMode, OnOverflow, PrecomputeConfig, PrecomputeConfigSet, SketchParams,
    WindowSpec,
};
pub use envelope::{SketchEnvelope, SketchType};
pub use matchers::{LabelMatcher, MatchOp};
pub use observation::{KeyValue, Observation, ObservationValue, ObservationValueKind};
pub use precompute::{
    CardinalitySketch, FrequencyEntry, FrequencySketch, Precompute, QuantileSketch, Sketch,
    SketchObserver,
};
