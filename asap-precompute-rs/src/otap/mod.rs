//! OTAP-Rust codec (Layer-4 codec). Phase 5 step B —
//! see [`docs/design-asap-otap-rust-integration.md`](../../../docs/design-asap-otap-rust-integration.md)
//! §4 (data model mapping), §6 (plugin file layout), §11 (phase plan).
//!
//! This module is the OTAP-side mirror of `asap-precompute-go/telegraf/`
//! (Phase 4 step B, PR #234). It does **pure data-shape translation**:
//!
//! - [`decode_batch`] turns a single [`arrow_array::RecordBatch`] into a
//!   `Vec<Observation>` ready for [`crate::precompute::Precompute::observe`].
//! - [`encode_batch`] turns a slice of [`crate::envelope::SketchEnvelope`]
//!   back into a `RecordBatch` carrying the envelope payload as a
//!   per-row Strategy-B field
//!   (per [edge-framework §7.2](../../../docs/design-asap-edge-framework.md#72-two-encoding-strategies)).
//!
//! The codec **owns no Tokio tasks, no timers, no control-channel
//! inbox**. The full plugin lifecycle (`Wakeup`-driven flush ticker,
//! `NodeControlMsg` handling, control-channel poll task, graceful
//! drain) is **Phase C** and lives in `otap-patch/plugins/asap_sketches/`
//! once that arrives — kept deliberately separate from the codec to
//! match the two-layer split in
//! [otap design §2](../../../docs/design-asap-otap-rust-integration.md#2-architecture--the-two-layer-split).
//!
//! # Schema (v1)
//!
//! Per the design doc §4 ("Schema discovery — pin to OTel-Arrow's
//! well-known schema in v1"), the codec discovers a small set of
//! well-known columns by name. The well-known names match the
//! Strategy-B carrier keys defined in
//! [edge-framework §7.2](../../../docs/design-asap-edge-framework.md#72-two-encoding-strategies):
//!
//! | Column                  | Arrow type        | Required | Meaning                                                                  |
//! |-------------------------|-------------------|----------|--------------------------------------------------------------------------|
//! | `time_unix_nano`        | `UInt64`          | optional | observation timestamp (nanoseconds since epoch)                          |
//! | `metric`                | `Utf8`            | optional | metric name                                                              |
//! | `value`                 | `Float64`         | optional | scalar value (KindFloat path)                                            |
//! | `_asap_envelope`        | `Binary`          | optional | envelope payload bytes; if present the row routes through KindEnvelope   |
//! | `_asap_sketch_type`     | `Utf8`            | optional | one of `DDSketch`/`KLLSketch`/`HLLSketch`/`CountSketch`/`CountMinSketch` |
//! | `_asap_agg_id`          | `UInt64`          | optional | controller-plan join key                                                 |
//! | `_asap_schema_version`  | `UInt32`          | optional | envelope schema version                                                  |
//! | `_asap_window_start_ms` | `UInt64`          | optional | inclusive lower bound of the envelope's window                           |
//! | `_asap_window_end_ms`   | `UInt64`          | optional | exclusive upper bound of the envelope's window                           |
//! | `_asap_encoding`        | `Utf8`            | optional | one of `PROTO_FULL` / `PROTO_DELTA` / `MSGPACK`                          |
//! | (any other `Utf8` col)  | `Utf8`            | -        | treated as a per-row label key                                           |
//!
//! Columns absent from the input batch are simply skipped — adapters
//! upstream of the plugin (or test harnesses) only need to populate
//! the columns they care about. The encode side always emits the
//! envelope-side columns plus a stable union of label columns drawn
//! from the input envelopes' `labels` and `resource_labels`.
//!
//! Phase B's per-RecordBatch shape is intentionally **flatter** than
//! OTAP's full `OtapArrowRecords` (which carries sibling resource /
//! scope / per-row attribute child batches joined by integer ids).
//! The plugin shell in Phase C is the layer that runs OTAP's native
//! attribute join and projects an `OtapArrowRecords` down to a flat
//! per-row RecordBatch the codec can consume — see the §6 plugin
//! file layout in the design doc. Until the plugin shell exists, the
//! codec's flat shape is also the easiest to round-trip in a unit
//! test.
//!
//! # Phase C — full plugin lifecycle
//!
//! Phase B shipped the stateless codec (`decode_batch` /
//! `encode_batch`) plus a [`StubPlugin`] anchor. Phase C layers the
//! Tokio-driven plugin lifecycle on top:
//!
//! - [`config::PluginConfig`] + [`config::resolve`] — high-level
//!   plugin configuration with 5-sketch `sketch_type` dispatch
//!   (DDSketch / KLL / HLL / CountSketch / CountMinSketch).
//! - [`records::OtapMetricRecords`] + [`records::flatten`] /
//!   [`records::lift`] — local model of the upstream OTAP
//!   `OtapArrowRecords` family with the bidirectional
//!   sibling-batch ↔ flat-batch projection that Phase B deferred.
//! - [`lifecycle::AsapSketchesPlugin`] — Tokio runtime: input task
//!   consumes the host-supplied stream, `Wakeup`-driven flush
//!   ticker emits batches, control-channel poll task picks up
//!   plan changes, graceful drain on shutdown.
//!
//! The OTAP submodule wiring (linkme distributed-slice registration,
//! `build_asap_otap.sh`, `otap-patch/all/mod.rs` patch) is **Phase D**
//! per the §11 phase plan and is deliberately not touched here. The
//! Phase C plugin lifecycle is exercised end-to-end via the
//! `tests/otap_lifecycle.rs` harness.
//!
//! # Out of scope (Phase D / E, deliberately deferred)
//!
//! - `linkme` distributed-slice plugin registration in
//!   `otap-patch/all/mod.rs`.
//! - `build_asap_otap.sh`.
//! - Cross-host envelope parity (Phase E).
//! - `OtapArrowRecords` binding to the upstream Rust type — Phase D
//!   wires [`records::OtapMetricRecords`] to the upstream
//!   `OtapPdata` shape.
//!
//! # Stub plugin shell (kept for back-compat with Phase B tests)
//!
//! [`StubPlugin`] is a no-op lifecycle wrapper that threads
//! `decode_batch` / `encode_batch` against any
//! [`crate::precompute::Precompute`]. Phase C's full plugin lives in
//! [`lifecycle::AsapSketchesPlugin`]; the stub is retained to keep
//! Phase B's tests passing as a regression backstop.

mod decode;
mod encode;
mod plugin;
mod schema;

pub mod config;
pub mod lifecycle;
pub mod records;

pub use decode::{decode_batch, OtapDecodeError};
pub use encode::{encode_batch, OtapEncodeError};
pub use plugin::StubPlugin;
pub use schema::{
    ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SKETCH_TYPE,
    ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};

pub use config::{ConfigError, PluginConfig, SketchDispatch};
pub use lifecycle::{
    AsapSketchesPlugin, EmitReceiver, EmitSender, PluginError, PluginHandle, StartOptions,
};
pub use records::{flatten, lift, OtapMetricRecords, OtapRecordsError};
