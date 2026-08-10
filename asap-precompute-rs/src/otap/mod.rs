//! OTAP-Rust codec (Layer-4 codec).
//!
//! This module does **pure data-shape translation**:
//!
//! - [`decode_batch`] turns a single [`arrow_array::RecordBatch`] into a
//!   `Vec<Observation>` ready for [`crate::precompute::Precompute::observe`].
//! - [`encode_batch`] turns a slice of [`crate::envelope::SketchEnvelope`]
//!   back into a `RecordBatch` carrying the envelope payload as a
//!   per-row Strategy-B field.
//!
//! The codec **owns no Tokio tasks, no timers, no control-channel
//! inbox**. The full plugin lifecycle (`Wakeup`-driven flush ticker,
//! `NodeControlMsg` handling, control-channel poll task, graceful
//! drain) is **Phase C** and lives in `otap-patch/plugins/asap_sketches/`
//! once that arrives — kept deliberately separate from the codec to
//! match the two-layer split.
//!
//! # Schema (v1)
//!
//! The codec discovers a small set of
//! well-known columns by name. The well-known names match the
//! Strategy-B carrier keys:
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
//! per-row RecordBatch the codec can consume. Until the plugin shell exists, the
//! codec's flat shape is also the easiest to round-trip in a unit
//! test.
//!
//! # Schema / Dictionary / Record stream ([`dictionary`])
//!
//! [`encode_batch`] / [`decode_batch`] above are a *different* codec
//! from [`dictionary::SeriesDictionary`] / [`dictionary::SeriesDictionaryDecoder`],
//! not an earlier draft of it — they solve different problems:
//!
//! - `encode_batch` / `decode_batch` (+ [`records::flatten`] /
//!   [`records::lift`]) make a `SketchEnvelope` **look like** a
//!   generic OTAP-Metrics payload — one self-contained row per
//!   envelope, `_asap_*` carrier keys lifted onto the per-row
//!   attribute child batch — so it can transit an OTAP pipeline hop
//!   that only knows how to move Logs/Metrics/Traces payloads.
//! - [`dictionary::SeriesDictionary`] / [`dictionary::SeriesDictionaryDecoder`]
//!   implement `docs/data_model.md`'s `SCHEMA` / `DICTIONARY` / `RECORD`
//!   tiering for the hop that doc actually describes: sketch state
//!   crossing a node boundary between two `asap_sketches` processor
//!   instances (an ASAP-aware sender talking to an ASAP-aware
//!   receiver — see [`Precompute::tick`](crate::precompute::Precompute::tick) /
//!   `drain`, which is exactly that boundary). There, config-level
//!   facts (`sketch_type`, `sketch_size`, `encoding`, …) are sent once
//!   per `agg_id` and series identity (`metric` + labels) once per
//!   distinct series — not repeated on every window's `RECORD` row —
//!   because both ends are expected to retain that state across the
//!   stream, the same way an Arrow IPC decoder retains Schema /
//!   Dictionary state. [`AsapSketchesPlugin`] and [`StubPlugin`] both
//!   use this codec for their tick/drain (encode) and inbound-envelope
//!   (decode) paths; `encode_batch`/`decode_batch` remain for raw
//!   (non-envelope) observation ingestion and for whatever still needs
//!   OTAP-Metrics-payload compatibility.
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
//! and is deliberately not touched here. The
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
mod dictionary;
mod encode;
mod plugin;
mod schema;

pub mod config;
pub mod lifecycle;
pub mod records;
pub mod wire;

pub use decode::{decode_batch, OtapDecodeError};
pub use dictionary::{
    SchemaSnapshot, SeriesDictionary, SeriesDictionaryDecoder, SketchStreamBatch,
};
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
