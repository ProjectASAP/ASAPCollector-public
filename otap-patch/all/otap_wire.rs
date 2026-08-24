// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! Real-`OtapPdata` transport over TCP — the wire-lane counterpart of
//! `otap_bridge.rs`'s generic-pipeline path, carrying the **same**
//! encoding (a real `OtapArrowRecords::Metrics`, built via
//! `otap_bridge::otap_metric_records_to_pdata`) instead of ASAP's own
//! SCHEMA/DICTIONARY/RECORD `SketchStreamBatch` protocol
//! (`asap_precompute_rs::otap::{wire,dictionary}`). One encoding, two
//! transports: this module is "the same OTAP metric, sent directly
//! over a persistent socket to a peer" instead of "riding through an
//! arbitrary OTAP pipeline hop via `effect_handler.send_message`."
//!
//! This is what closes the metric-lane/wire-lane split the crate root
//! README and `otap_bridge.rs` describe: a producer and a receiver
//! that both speak real `OtapPdata` no longer need two different wire
//! formats depending on whether they're directly connected or routed
//! through other OTAP components — they can use this module for the
//! direct-connection case and get identical bytes-on-the-wire
//! semantics either way.
//!
//! # Design: reused, not reinvented
//!
//! The persistent-per-connection Arrow IPC framing here is the same
//! design `asap_precompute_rs::otap::wire::{WireWriter,WireReader}`
//! already validates (each role's Arrow IPC Schema message goes out
//! once per connection, not once per window) — duplicated rather than
//! shared, because `asap-precompute-rs` deliberately has no dependency
//! on the OTAP Dataflow crates this module needs
//! (`otel_arrow_dfe_pdata`, `otel_arrow_dfe_otap`), and pulling one in
//! would break that crate's "builds standalone" property. The one
//! generalization from that design: `SketchStreamBatch` has exactly
//! four fixed roles, but a real `OtapArrowRecords::Metrics` populates
//! a *variable* subset of up to nineteen `ArrowPayloadType`s (e.g.
//! `NumberDpAttrs` is simply absent when a data point has zero
//! attributes) — so roles here are keyed by `ArrowPayloadType` in a
//! `BTreeMap` (deterministic send order) rather than four named
//! struct fields.
//!
//! # Verification status
//!
//! Build/lint/test-verified the same way `otap_bridge.rs` was — staged
//! into a real `open-telemetry/otel-arrow` checkout
//! (`3e85c3460361446ebfce99e9f35fffd2dd5ab740`, 2026-08-24) as a
//! `crates/*` workspace member; see this module's own tests for what
//! that covered.
//!
//! # Not wired into `AsapSketchesProcessor` yet
//!
//! This module is a complete, tested transport layer with no consumer
//! yet inside this crate (`#[allow(dead_code)]` below reflects that
//! honestly rather than hiding it) — analogous to how
//! `asap_precompute_rs::otap::wire::{WireWriter,WireReader}` is used
//! by standalone example binaries
//! (`examples/sketch_producer_node.rs`/`sketch_receiver_node.rs`),
//! not baked into the generic plugin lifecycle. The natural next
//! consumer here is the same shape: either a standalone binary pairing
//! two `AsapSketchesProcessor`-equivalent instances directly (this
//! module's own tests already prove that shape works, just with
//! synthetic single-row fixtures instead of a full running
//! `Precompute`), or — more in the spirit of "ride OTAP's real
//! transport" — routing through OTAP's own gRPC exporter/receiver
//! pair instead of this module at all, since that already solves "get
//! `OtapPdata` to another node" as a first-class pipeline component.
//! Either way, wiring a *config-driven* choice of transport into
//! `AsapSketchesProcessor` itself (a `peer_addr` option, connection
//! lifecycle, reconnect behavior) is real follow-up work this module
//! doesn't attempt.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::io;

use arrow_array::RecordBatch;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;
use otel_arrow_dfe_pdata::{OtapPayload, TryIntoWithOptions};

/// Same cap as `asap_precompute_rs::otap::wire::MAX_FRAME_LEN`, and
/// for the same reason: never let an untrusted length prefix alone
/// dictate an allocation before a single content byte is validated.
pub const MAX_FRAME_LEN: usize = 256 * 1024 * 1024; // 256 MiB

/// Failure modes for [`OtapWireWriter::send`] / [`OtapWireReader::recv`].
#[derive(Debug, Error)]
pub enum OtapWireError {
    /// Arrow IPC encode/decode failed.
    #[error("otap wire (real OtapPdata): arrow ipc error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// Converting `OtapPdata`'s payload to/from `OtapArrowRecords`
    /// failed (e.g. malformed OTLP bytes, or a schema `set()`
    /// rejected).
    #[error("otap wire (real OtapPdata): real-otap conversion error: {0}")]
    Otap(String),

    /// A role's retained IPC state didn't yield a new batch after
    /// ingesting this frame's delta for it — a well-formed connection
    /// always has exactly one new RecordBatch message per role per
    /// [`OtapWireWriter::send`] call that included that role, so this
    /// signals a genuine desync with the peer's writer state.
    #[error("otap wire (real OtapPdata): payload type {0:?} had no new record batch this frame")]
    EmptyRecordBatch(ArrowPayloadType),

    /// A received payload-type tag didn't map to any known
    /// `ArrowPayloadType` — the peer and this build disagree on the
    /// proto enum (a version skew this framing has no way to reconcile).
    #[error("otap wire (real OtapPdata): unknown payload-type tag {0} on receive")]
    UnknownPayloadType(i32),

    /// The frame's length prefix didn't match the bytes actually
    /// available — a truncated or corrupt frame.
    #[error("otap wire (real OtapPdata): frame truncated: expected {expected} bytes, got {actual}")]
    Truncated {
        /// Bytes the length prefix promised.
        expected: usize,
        /// Bytes actually present.
        actual: usize,
    },

    /// Network I/O failed while sending/receiving a frame.
    #[error("otap wire (real OtapPdata): io error: {0}")]
    Io(#[from] io::Error),

    /// A length prefix exceeded what this module will represent
    /// (encode) or accept (decode) — see [`MAX_FRAME_LEN`].
    #[error("otap wire (real OtapPdata): frame length {len} exceeds cap of {max} bytes")]
    FrameTooLarge {
        /// The length that was rejected.
        len: usize,
        /// The cap it exceeded.
        max: usize,
    },
}

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> Result<(), OtapWireError> {
    let len = u32::try_from(body.len()).map_err(|_| OtapWireError::FrameTooLarge {
        len: body.len(),
        max: u32::MAX as usize,
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

/// Returns `Ok(None)` on a clean EOF at a frame boundary.
async fn read_frame(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, OtapWireError> {
    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(stream, &mut len_buf).await? {
        return Ok(None);
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        return Err(OtapWireError::FrameTooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(Some(body))
}

async fn read_exact_or_eof(stream: &mut TcpStream, buf: &mut [u8]) -> Result<bool, OtapWireError> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            return if filled == 0 {
                Ok(false)
            } else {
                Err(OtapWireError::Truncated {
                    expected: buf.len(),
                    actual: filled,
                })
            };
        }
        filled += n;
    }
    Ok(true)
}

/// One `ArrowPayloadType`'s ongoing Arrow IPC stream on the send
/// side. See `asap_precompute_rs::otap::wire::RoleWriter` — identical
/// design, duplicated per this module's doc.
struct RoleWriter(Option<arrow_ipc::writer::StreamWriter<Vec<u8>>>);

impl RoleWriter {
    const fn new() -> Self {
        Self(None)
    }

    fn write_delta(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, OtapWireError> {
        // Captured *before* establishing the writer: `StreamWriter::
        // try_new` writes the Schema message as part of construction,
        // so `before` must be 0 on the first call to include it in
        // the delta (see asap_precompute_rs::otap::wire's identical
        // fix for the bug this got wrong the first time).
        let before = match &self.0 {
            Some(w) => w.get_ref().len(),
            None => 0,
        };
        if self.0.is_none() {
            self.0 = Some(arrow_ipc::writer::StreamWriter::try_new(
                Vec::new(),
                &batch.schema(),
            )?);
        }
        let writer = self
            .0
            .as_mut()
            .expect("just initialized above if it was None");
        writer.write(batch)?;
        Ok(writer.get_ref()[before..].to_vec())
    }
}

/// One `ArrowPayloadType`'s ongoing Arrow IPC stream on the receive
/// side. See `asap_precompute_rs::otap::wire::RoleReader`.
struct RoleReader {
    accumulated: Vec<u8>,
    yielded: usize,
}

impl RoleReader {
    const fn new() -> Self {
        Self {
            accumulated: Vec::new(),
            yielded: 0,
        }
    }

    fn ingest(&mut self, delta: &[u8]) -> Result<Option<RecordBatch>, OtapWireError> {
        self.accumulated.extend_from_slice(delta);
        if self.accumulated.is_empty() {
            return Ok(None);
        }
        let mut reader =
            arrow_ipc::reader::StreamReader::try_new(self.accumulated.as_slice(), None)?;
        for _ in 0..self.yielded {
            let _ = reader.next();
        }
        match reader.next() {
            Some(batch) => {
                self.yielded += 1;
                Ok(Some(batch?))
            }
            None => Ok(None),
        }
    }
}

/// Persistent per-connection Arrow-IPC send state for real `OtapPdata`
/// — one ongoing IPC stream per `ArrowPayloadType` actually populated
/// on a given window's `OtapArrowRecords::Metrics`, for the whole life
/// of a connection. Construct one per connection and reuse it for
/// every [`Self::send`] call on that connection.
pub struct OtapWireWriter {
    roles: BTreeMap<ArrowPayloadType, RoleWriter>,
}

impl OtapWireWriter {
    /// Constructs a writer with no established IPC streams yet — each
    /// payload type's stream is established the first time
    /// [`Self::send`] sees it populated.
    pub const fn new() -> Self {
        Self {
            roles: BTreeMap::new(),
        }
    }

    /// Sends one window's `OtapPdata` (must carry a Metrics-signal
    /// payload — see [`otel_arrow_dfe_config::SignalType::Metrics`])
    /// over `stream`, appending to each populated payload type's
    /// ongoing IPC stream rather than starting fresh ones. The
    /// receiving side reads it back with a matching
    /// [`OtapWireReader::recv`] call on the same connection.
    pub async fn send(
        &mut self,
        stream: &mut TcpStream,
        pdata: OtapPdata,
    ) -> Result<(), OtapWireError> {
        let (_context, payload) = pdata.into_parts();
        let arrow_records: OtapArrowRecords = payload
            .try_into_with_default()
            .map_err(|e| OtapWireError::Otap(format!("{e}")))?;

        let mut parts: Vec<(ArrowPayloadType, Vec<u8>)> = Vec::new();
        for &payload_type in arrow_records.allowed_payload_types() {
            let Some(batch) = arrow_records.get(payload_type) else {
                continue;
            };
            let writer = self
                .roles
                .entry(payload_type)
                .or_insert_with(RoleWriter::new);
            parts.push((payload_type, writer.write_delta(batch)?));
        }

        let mut body = Vec::new();
        let count = u32::try_from(parts.len()).map_err(|_| OtapWireError::FrameTooLarge {
            len: parts.len(),
            max: u32::MAX as usize,
        })?;
        body.extend_from_slice(&count.to_be_bytes());
        for (payload_type, delta) in &parts {
            body.extend_from_slice(&(*payload_type as i32).to_be_bytes());
            let len = u32::try_from(delta.len()).map_err(|_| OtapWireError::FrameTooLarge {
                len: delta.len(),
                max: u32::MAX as usize,
            })?;
            body.extend_from_slice(&len.to_be_bytes());
            body.extend_from_slice(delta);
        }
        write_frame(stream, &body).await
    }
}

impl Default for OtapWireWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Persistent per-connection Arrow-IPC receive state — the read-side
/// counterpart to [`OtapWireWriter`]. Construct one per connection and
/// reuse it for every [`Self::recv`] call on that connection.
pub struct OtapWireReader {
    roles: BTreeMap<ArrowPayloadType, RoleReader>,
}

impl OtapWireReader {
    /// Constructs a reader with no retained IPC state yet.
    pub const fn new() -> Self {
        Self {
            roles: BTreeMap::new(),
        }
    }

    /// Reads one window's `OtapPdata` previously written by a matching
    /// [`OtapWireWriter::send`] call on the same connection.
    /// Reconstructed as a Metrics-signal `OtapArrowRecords`. Returns
    /// `Ok(None)` on a clean EOF at a frame boundary.
    pub async fn recv(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<Option<OtapPdata>, OtapWireError> {
        let Some(body) = read_frame(stream).await? else {
            return Ok(None);
        };

        let mut cursor: &[u8] = &body;
        let mut count_buf = [0u8; 4];
        std::io::Read::read_exact(&mut cursor, &mut count_buf).map_err(|_| {
            OtapWireError::Truncated {
                expected: 4,
                actual: cursor.len(),
            }
        })?;
        let count = u32::from_be_bytes(count_buf) as usize;

        let mut store = otel_arrow_dfe_pdata::otap::raw_batch_store::RawMetricsStore::new();
        let mut arrow_records = OtapArrowRecords::Metrics(store.clone().try_into().map_err(
            |e: otel_arrow_dfe_pdata::error::Error| OtapWireError::Otap(format!("{e}")),
        )?);
        let _ = &mut store; // store itself unused past constructing the empty Metrics above.

        for _ in 0..count {
            let mut tag_buf = [0u8; 4];
            std::io::Read::read_exact(&mut cursor, &mut tag_buf).map_err(|_| {
                OtapWireError::Truncated {
                    expected: 4,
                    actual: cursor.len(),
                }
            })?;
            let tag = i32::from_be_bytes(tag_buf);
            let payload_type = ArrowPayloadType::try_from(tag)
                .map_err(|_| OtapWireError::UnknownPayloadType(tag))?;

            let mut len_buf = [0u8; 4];
            std::io::Read::read_exact(&mut cursor, &mut len_buf).map_err(|_| {
                OtapWireError::Truncated {
                    expected: 4,
                    actual: cursor.len(),
                }
            })?;
            let len = u32::from_be_bytes(len_buf) as usize;
            if cursor.len() < len {
                return Err(OtapWireError::Truncated {
                    expected: len,
                    actual: cursor.len(),
                });
            }
            let (delta, rest) = cursor.split_at(len);
            cursor = rest;

            let reader = self
                .roles
                .entry(payload_type)
                .or_insert_with(RoleReader::new);
            let batch = reader
                .ingest(delta)?
                .ok_or(OtapWireError::EmptyRecordBatch(payload_type))?;
            arrow_records
                .set(payload_type, batch)
                .map_err(|e| OtapWireError::Otap(format!("{e}")))?;
        }

        let payload: OtapPayload = arrow_records.into();
        Ok(Some(OtapPdata::new_todo_context(payload)))
    }
}

impl Default for OtapWireReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otap_bridge::{otap_metric_records_to_pdata, pdata_to_otap_metric_records};
    use arrow_array::{BinaryArray, Float64Array, StringArray, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use asap_precompute_rs::otap::records::{
        ATTR_BATCH_BYTES, ATTR_BATCH_INT, ATTR_BATCH_KEY, ATTR_BATCH_PARENT_ID, ATTR_BATCH_STR,
    };
    use asap_precompute_rs::otap::{
        COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE, OtapMetricRecords,
    };
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};

    fn one_row_records(metric_name: &str, value: f64) -> OtapMetricRecords {
        let metrics = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Float64, false),
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![1_000_000_000_u64])),
                Arc::new(StringArray::from(vec![metric_name])),
                Arc::new(Float64Array::from(vec![value])),
                Arc::new(UInt32Array::from(vec![0_u32])),
            ],
        )
        .expect("metrics batch");
        let attributes = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
                Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
                Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
                Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
                Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(vec![0_u32])),
                Arc::new(StringArray::from(vec!["path"])),
                Arc::new(StringArray::from(vec![Some("/api")])),
                Arc::new(UInt64Array::from(vec![None::<u64>])),
                Arc::new(BinaryArray::from_opt_vec(vec![None])),
            ],
        )
        .expect("attributes batch");
        OtapMetricRecords {
            metrics,
            attributes,
        }
    }

    #[tokio::test]
    async fn round_trips_a_real_otap_pdata_over_a_tcp_loopback_socket() {
        let records = one_row_records("http_request_duration_ms", 42.5);
        let pdata = otap_metric_records_to_pdata(&records).expect("build real OtapPdata");

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            OtapWireReader::new()
                .recv(&mut socket)
                .await
                .expect("recv")
                .expect("Some(pdata)")
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        OtapWireWriter::new()
            .send(&mut client, pdata)
            .await
            .expect("send");
        drop(client);

        let received = server.await.expect("server task");
        let outcome = pdata_to_otap_metric_records(received).expect("decode");
        let decoded = outcome.records.expect("one row");
        let metric_col = decoded
            .metrics
            .column_by_name(COLUMN_METRIC)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(metric_col.value(0), "http_request_duration_ms");
        let value_col = decoded
            .metrics
            .column_by_name(COLUMN_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(value_col.value(0), 42.5);
    }

    #[tokio::test]
    async fn round_trips_multiple_windows_over_one_persistent_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut reader = OtapWireReader::new();
            let mut out = Vec::new();
            while let Some(pdata) = reader.recv(&mut socket).await.expect("recv") {
                out.push(pdata);
            }
            out
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        let mut writer = OtapWireWriter::new();
        for (name, value) in [("latency_ms", 10.0_f64), ("latency_ms", 20.0_f64)] {
            let records = one_row_records(name, value);
            let pdata = otap_metric_records_to_pdata(&records).expect("build");
            writer.send(&mut client, pdata).await.expect("send");
        }
        drop(client);

        let received = server.await.expect("server task");
        assert_eq!(received.len(), 2);
        for (i, expected_value) in [10.0_f64, 20.0_f64].into_iter().enumerate() {
            let outcome = pdata_to_otap_metric_records(received[i].clone()).expect("decode");
            let decoded = outcome.records.expect("one row");
            let value_col = decoded
                .metrics
                .column_by_name(COLUMN_VALUE)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(value_col.value(0), expected_value);
        }
    }
}
