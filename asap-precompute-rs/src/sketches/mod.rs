//! Sketch wrappers over [`asap_sketchlib`] types.
//!
//! Each wrapper adapts one native sketchlib sketch to the host-neutral
//! [`crate::precompute::Sketch`] trait family. `snapshot()` returns the
//! sketch's self-describing ASAPv1 envelope from `serialize_to_bytes`, and
//! `merge_snapshot()` decodes one with `deserialize_from_bytes` and merges
//! it. Every payload is full state; there are no deltas.

pub mod cms;
pub mod cms_heap;
pub mod countsketch;
pub mod ddsketch;
pub mod hll;
pub mod kll;

pub use cms::{CMSObserver, CMSWrapper};
pub use cms_heap::{CMSHeapObserver, CMSHeapWrapper};
pub use countsketch::{CountSketchObserver, CountSketchWrapper};
pub use ddsketch::{DDSketchObserver, DDSketchWrapper};
pub use hll::{HLLObserver, HLLWrapper};
pub use kll::{KLLObserver, KLLWrapper};
