//! [`Adapter`] trait.
//!
//! Adapters are the Layer-4 platform shims (OTel / Telegraf /
//! Vector / OTAP) that translate their host's native event into
//! [`crate::observation::Observation`], hand it to a
//! [`crate::precompute::Precompute`], and translate the runtime's
//! emitted [`crate::envelope::SketchEnvelope`] back to the host's
//! native event.

use std::time::Duration;

use crate::envelope::SketchEnvelope;
use crate::observation::Observation;
use crate::precompute::{PrecomputeError, StatsSnapshot};

/// Cancel handle returned by [`Adapter::schedule_tick`]. Dropping
/// the handle stops the periodic callback.
///
/// Boxed `FnOnce` so adapters can return any cancellation
/// closure (channel send, atomic flag flip, JoinHandle::abort,
/// etc.).
pub type CancelTick = Box<dyn FnOnce() + Send>;

/// Contract every Layer-4 platform shim implements.
///
/// The associated `Event` type is the host's native event / data
/// model:
///   - OTel: `pmetric::Metrics`
///   - Telegraf: `telegraf::Metric`
///   - Vector: `vector::Event`
///   - OTAP: `arrow::RecordBatch`
///
/// Using an associated type keeps each adapter monomorphically typed
/// without any runtime type-erasure dance.
pub trait Adapter {
    /// Host's native event / data model.
    type Event;

    /// Converts a host-native event into one or more host-neutral
    /// [`Observation`]s.
    ///
    /// MUST recognize sketch-typed inputs and produce
    /// [`crate::observation::ObservationValueKind::Envelope`] rather
    /// than expanding them to scalars (bandwidth invariant).
    fn decode(&self, ev: Self::Event) -> Result<Vec<Observation>, PrecomputeError>;

    /// Converts a slice of host-neutral [`SketchEnvelope`]s back
    /// into a host-native event suitable for handoff to the host's
    /// downstream pipeline.
    fn encode(&self, envelopes: &[SketchEnvelope]) -> Result<Self::Event, PrecomputeError>;

    /// Installs a periodic callback at `period`. The callback runs
    /// on a thread / task the adapter owns; returns a cancel handle
    /// to stop the worker on shutdown.
    ///
    /// The callback typically calls
    /// [`crate::precompute::Precompute::tick`] and feeds output
    /// back via [`Self::encode`] + the host's downstream dispatch.
    fn schedule_tick(&self, period: Duration, cb: Box<dyn Fn() + Send + Sync>) -> CancelTick;

    /// Publishes the [`crate::precompute::Precompute`]'s runtime
    /// stats through the host's native metric channel.
    fn emit_telemetry(&self, stats: &StatsSnapshot);
}
