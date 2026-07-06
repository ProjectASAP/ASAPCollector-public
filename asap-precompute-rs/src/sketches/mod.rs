//! Real sketch wrappers over [`asap_sketchlib`] types.
//!
//! Each wrapper adapts a concrete sketch implementation from
//! [`asap_sketchlib`] to the host-neutral [`crate::precompute::Sketch`]
//! trait family so a [`crate::precompute::Precompute`] instance can own
//! it as a generic sketch.
//!
//! # Wire format
//!
//! Wrappers serialize via `asap_sketchlib::proto::sketchlib::SketchEnvelope`
//! (prost-encoded). The envelope shape comes from the shared proto
//! definitions (`asap_sketchlib/proto/*.proto`). Field-level byte
//! stability depends on:
//!
//! - Identical proto field tags / wire types (guaranteed by the shared
//!   proto file).
//! - Identical numeric encoding (integer values map to fixed-tag
//!   varints deterministically).
//! - Identical floating-point bit patterns (no platform divergence
//!   for finite f64 in IEEE-754 round-trip).
//!
//! # API surface caveats
//!
//! - `asap_sketchlib` exposes per-sketch `compute_delta` /
//!   `apply_delta_bytes` for DDSketch, CMS, CountSketch, and HLL.
//!   Those four wrappers emit real `ProtoDelta` frames; KLL stays
//!   full-only.
//! - `asap_sketchlib` does not expose
//!   `DeserializeXxxFromProtoBytes` round-trip helpers for the
//!   high-throughput sketch types. The wrappers decode the wire-format
//!   `SketchEnvelope` envelope and reconstruct the wire-aligned
//!   sketch struct (`DdSketch`, `HllSketch`, …) directly from the
//!   inner state proto.

pub mod cms;
pub mod countsketch;
pub mod ddsketch;
pub mod hll;
pub mod kll;

pub use cms::{CMSObserver, CMSWrapper};
pub use countsketch::{CountSketchObserver, CountSketchWrapper};
pub use ddsketch::{DDSketchObserver, DDSketchWrapper};
pub use hll::{HLLObserver, HLLWrapper};
pub use kll::{KLLObserver, KLLWrapper};
