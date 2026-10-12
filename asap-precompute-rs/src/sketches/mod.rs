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

use crate::envelope::SketchType;
use crate::precompute::{PrecomputeError, Sketch};

/// Decodes an ASAPv1 snapshot into the wrapper for `sketch_type`.
pub fn decode_snapshot(
    sketch_type: SketchType,
    bytes: &[u8],
) -> Result<Box<dyn Sketch>, PrecomputeError> {
    Ok(match sketch_type {
        SketchType::DDSketch => Box::new(DDSketchWrapper::from_snapshot(bytes)?),
        SketchType::KLLSketch => Box::new(KLLWrapper::from_snapshot(bytes)?),
        SketchType::HLLSketch => Box::new(HLLWrapper::from_snapshot(bytes)?),
        SketchType::CountSketch => Box::new(CountSketchWrapper::from_snapshot(bytes)?),
        SketchType::CountMinSketch => Box::new(CMSWrapper::from_snapshot(bytes)?),
        SketchType::CountMinSketchWithHeap => Box::new(CMSHeapWrapper::from_snapshot(bytes)?),
        SketchType::Unspecified => {
            return Err(PrecomputeError::Other(
                "sketch.envelope requires a sketch.type".into(),
            ))
        }
    })
}
