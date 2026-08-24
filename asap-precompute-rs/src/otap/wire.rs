//! Arrow-IPC serialization + a minimal length-prefixed framing for
//! carrying a [`SketchStreamBatch`] across a real transport (a TCP
//! socket here) — the actual "crosses a node or network boundary"
//! hop `docs/data_model.md` opens with, rather than the in-process
//! `mpsc` channel [`crate::otap::lifecycle`]'s tests and
//! `examples/sketch_pipeline_demo.rs` use.
//!
//! # Wire shape
//!
//! One [`SketchStreamBatch`] is framed as:
//!
//! ```text
//! [u32 total_len]
//! [u32 schema_len]     [schema_len bytes: Arrow IPC message(s)]
//! [u32 dictionary_len] [dictionary_len bytes: Arrow IPC message(s)]
//! [u32 labels_len]     [labels_len bytes: Arrow IPC message(s)]
//! [u32 record_len]     [record_len bytes: Arrow IPC message(s)]
//! ```
//!
//! All four sub-batch lengths (and the leading `total_len`) are
//! big-endian `u32`s. A zero-row batch still contributes a real Arrow
//! IPC RecordBatch message — a wire-level frame always carries exactly
//! four sub-messages even when, say, `schema`/`dictionary`/`labels`
//! are empty because the series was already known.
//!
//! # One ongoing IPC stream per role, per connection
//!
//! [`WireWriter`]/[`WireReader`] are this module's one send/receive
//! API — there is deliberately no separate one-shot alternative. Each
//! holds one [`arrow_ipc::writer::StreamWriter`] / one incremental
//! decoder per role (schema, dictionary, labels, record) for the
//! whole life of a connection, rather than starting a fresh self-
//! contained IPC stream every window: a role's Arrow IPC **Schema
//! message** goes out exactly once per connection — the first
//! [`WireWriter::send`] call establishes it via `try_new`, and every
//! later call appends only a new RecordBatch message to that same
//! ongoing stream. [`WireReader`] mirrors this on the receive side: it
//! retains each role's decoder state across the connection without
//! retaining or re-parsing bytes from completed batches.
//!
//! This module used to also offer a pure, session-independent one-shot
//! codec (`encode_stream_batch`/`decode_stream_batch`,
//! `send_stream_batch`/`recv_stream_batch`) alongside this
//! persistent-connection API. It's gone, on purpose: keeping two ways
//! to move a `SketchStreamBatch` over the wire — one that pays a full
//! Arrow IPC Schema message every window, one that doesn't — meant
//! every caller had to know which one actually delivered the
//! dictionary economics this whole module exists for, and a genuinely
//! empty (0-row) sub-batch still paid that Schema-message cost every
//! window under the one-shot path even though
//! [`crate::otap::dictionary::SeriesDictionary`] had already reduced
//! its row count to zero. One path, and it's the one with the real
//! economics.

use std::io;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::dictionary::SketchStreamBatch;

/// Hard cap [`WireReader::recv`] applies to the untrusted `total_len`
/// prefix **before** allocating a buffer for it. Not a wire-format
/// constraint (the framing itself allows up to `u32::MAX`, ~4 GiB) —
/// without this cap, a malformed or hostile peer's 4-byte length
/// prefix alone could force an allocation of up to ~4 GiB before a
/// single content byte is validated, a trivial single-connection
/// memory-exhaustion vector against any node acting as a receiver.
/// Chosen well above any single-window `SketchStreamBatch` this crate
/// emits today.
pub const MAX_FRAME_LEN: usize = 256 * 1024 * 1024; // 256 MiB

/// Failure modes for [`WireWriter::send`] / [`WireReader::recv`].
#[derive(Debug, Error)]
pub enum WireError {
    /// Arrow IPC encode/decode failed (malformed batch, schema
    /// mismatch inside a sub-stream, etc.).
    #[error("otap wire: arrow ipc error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// A role's retained IPC state didn't yield a new batch after
    /// ingesting this frame's delta for it — a well-formed connection
    /// always has exactly one new RecordBatch message per role per
    /// `WireWriter::send` call, so this signals a genuine desync with
    /// the peer's `WireWriter` state.
    #[error("otap wire: sub-batch {which:?} had no new record batch this frame")]
    EmptyRecordBatch {
        /// Which of the four sub-batches was empty.
        which: &'static str,
    },

    /// A role delta contained more than the protocol's one record batch.
    #[error("otap wire: sub-batch {which:?} had more than one record batch this frame")]
    ExtraRecordBatch {
        /// Which of the four sub-batches contained extra data.
        which: &'static str,
    },

    /// The frame's length prefix didn't match the bytes actually
    /// available — a truncated or corrupt frame.
    #[error("otap wire: frame truncated: expected {expected} bytes, got {actual}")]
    Truncated {
        /// Bytes the length prefix promised.
        expected: usize,
        /// Bytes actually present.
        actual: usize,
    },

    /// Network I/O failed while sending/receiving a frame.
    #[error("otap wire: io error: {0}")]
    Io(#[from] io::Error),

    /// A length prefix (the frame's `total_len`, or a sub-batch's own
    /// serialized length on encode) exceeded what this module will
    /// represent/accept — either a hostile/corrupt peer's inflated
    /// `total_len` ([`WireReader::recv`]'s [`MAX_FRAME_LEN`] cap), or
    /// (on encode) a sub-batch too large for the wire format's `u32`
    /// length-prefix field to represent at all without truncating.
    #[error("otap wire: frame length {len} exceeds cap of {max} bytes")]
    FrameTooLarge {
        /// The length that was rejected.
        len: usize,
        /// The cap it exceeded.
        max: usize,
    },
}

/// Length-prefixes each of `parts` back to back (no leading
/// `total_len` — [`WireWriter::send`] adds that at its own framing
/// layer).
fn frame_parts(parts: &[Vec<u8>]) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::with_capacity(parts.iter().map(|p| p.len() + 4).sum());
    for part in parts {
        // Checked, not `as u32`: a silent truncation here would write
        // a length prefix smaller than the bytes that actually
        // follow, desynchronizing every subsequent frame the decoder
        // reads on this stream.
        let len = u32::try_from(part.len()).map_err(|_| WireError::FrameTooLarge {
            len: part.len(),
            max: u32::MAX as usize,
        })?;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(part);
    }
    Ok(out)
}

/// Inverse of [`frame_parts`]: splits `bytes` into `names.len()`
/// length-prefixed slices, pairing each with its role name for error
/// messages. Used by [`WireReader::recv`].
fn unframe_parts<'a>(
    bytes: &'a [u8],
    names: &[&'static str],
) -> Result<Vec<(&'a [u8], &'static str)>, WireError> {
    let mut cursor = bytes;
    let mut out = Vec::with_capacity(names.len());
    for &which in names {
        let mut len_buf = [0u8; 4];
        std::io::Read::read_exact(&mut cursor, &mut len_buf).map_err(|_| WireError::Truncated {
            expected: 4,
            actual: cursor.len(),
        })?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if cursor.len() < len {
            return Err(WireError::Truncated {
                expected: len,
                actual: cursor.len(),
            });
        }
        let (part, rest) = cursor.split_at(len);
        out.push((part, which));
        cursor = rest;
    }
    Ok(out)
}

async fn write_frame(stream: &mut TcpStream, body: &[u8]) -> Result<(), WireError> {
    let len = u32::try_from(body.len()).map_err(|_| WireError::FrameTooLarge {
        len: body.len(),
        max: u32::MAX as usize,
    })?;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

/// Returns `Ok(None)` on a clean EOF at a frame boundary.
async fn read_frame(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, WireError> {
    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(stream, &mut len_buf).await? {
        return Ok(None);
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME_LEN {
        // Reject before allocating — see MAX_FRAME_LEN's doc. A
        // peer's untrusted length prefix must never itself dictate an
        // allocation this large.
        return Err(WireError::FrameTooLarge {
            len,
            max: MAX_FRAME_LEN,
        });
    }
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(Some(body))
}

/// Like [`tokio::io::AsyncReadExt::read_exact`], but distinguishes "EOF
/// before any byte of this frame" (returns `Ok(false)` — a clean
/// stream close between frames) from "EOF partway through a frame's
/// length prefix" (still surfaced as an error by the subsequent
/// `read_exact`, since that's a truncated frame, not a clean close).
async fn read_exact_or_eof(stream: &mut TcpStream, buf: &mut [u8]) -> Result<bool, WireError> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            return if filled == 0 {
                Ok(false)
            } else {
                Err(WireError::Truncated {
                    expected: buf.len(),
                    actual: filled,
                })
            };
        }
        filled += n;
    }
    Ok(true)
}

/// One role's (schema/dictionary/labels/record) ongoing Arrow IPC
/// stream on the send side. `None` until the first `write_delta` call
/// establishes it — every `SketchStreamBatch` role has a fixed Arrow
/// schema across every call to `SeriesDictionary::encode` regardless
/// of row count, so the very first batch's schema is safe to reuse for
/// every later one on this role.
struct RoleWriter(Option<arrow_ipc::writer::StreamWriter<Vec<u8>>>);

impl RoleWriter {
    const fn new() -> Self {
        Self(None)
    }

    /// Writes `batch` to this role's ongoing IPC stream and returns
    /// exactly the bytes this call appended (a Schema message plus a
    /// RecordBatch message on the first call for this role; just a
    /// RecordBatch message on every call after).
    fn write_delta(&mut self, batch: &RecordBatch) -> Result<Vec<u8>, WireError> {
        // `try_new` writes the Schema message into the sink, so the
        // first drained delta includes it. Later calls start with an
        // empty sink and contain only the newly written batch.
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
        // StreamWriter keeps the Arrow schema/dictionary bookkeeping;
        // it does not require its output sink to retain bytes already
        // delivered to the transport. Drain the sink after every batch
        // so a long-lived connection retains only encoder state, not its
        // complete traffic history.
        Ok(std::mem::take(writer.get_mut()))
    }
}

/// One role's incremental Arrow IPC decoder. It retains schema and
/// dictionary state, but releases bytes belonging to completed
/// messages as soon as they are decoded.
struct RoleReader {
    decoder: StreamDecoder,
}

impl RoleReader {
    fn new() -> Self {
        Self {
            decoder: StreamDecoder::new(),
        }
    }

    /// Appends `delta` and returns the next not-yet-yielded batch for
    /// this role, if one is now complete. `SeriesDictionary::encode`
    /// always writes exactly one RecordBatch message per role per
    /// call (even 0-row ones), so under normal use this always
    /// returns `Some` once any bytes have been ingested; `None` only
    /// signals a genuinely incomplete/malformed delta.
    fn ingest(
        &mut self,
        delta: &[u8],
        which: &'static str,
    ) -> Result<Option<RecordBatch>, WireError> {
        if delta.is_empty() {
            return Ok(None);
        }
        let mut bytes = Buffer::from(delta.to_vec());
        let mut decoded = None;
        while !bytes.is_empty() {
            if let Some(batch) = self.decoder.decode(&mut bytes)? {
                if decoded.is_some() {
                    return Err(WireError::ExtraRecordBatch { which });
                }
                decoded = Some(batch);
            }
        }
        Ok(decoded)
    }
}

/// Persistent per-connection Arrow-IPC send state for
/// [`SketchStreamBatch`]es — see the module doc's "One ongoing IPC
/// stream per role, per connection" section. Construct one per
/// connection (not per window) and reuse it for every
/// [`WireWriter::send`] call on that connection.
pub struct WireWriter {
    schema: RoleWriter,
    dictionary: RoleWriter,
    labels: RoleWriter,
    record: RoleWriter,
}

impl WireWriter {
    /// Constructs a writer with no established IPC streams yet — the
    /// first [`Self::send`] call establishes all four.
    pub fn new() -> Self {
        Self {
            schema: RoleWriter::new(),
            dictionary: RoleWriter::new(),
            labels: RoleWriter::new(),
            record: RoleWriter::new(),
        }
    }

    /// Sends one [`SketchStreamBatch`] window over `stream`, appending
    /// to each role's ongoing IPC stream rather than starting a fresh
    /// one. The receiving side reads it back with a matching
    /// [`WireReader::recv`] call on the same connection.
    pub async fn send(
        &mut self,
        stream: &mut TcpStream,
        batch: &SketchStreamBatch,
    ) -> Result<(), WireError> {
        let parts = [
            self.schema.write_delta(&batch.schema)?,
            self.dictionary.write_delta(&batch.dictionary)?,
            self.labels.write_delta(&batch.labels)?,
            self.record.write_delta(&batch.record)?,
        ];
        let body = frame_parts(&parts)?;
        write_frame(stream, &body).await
    }
}

impl Default for WireWriter {
    fn default() -> Self {
        Self::new()
    }
}

/// Persistent per-connection Arrow-IPC receive state — the read-side
/// counterpart to [`WireWriter`]. Construct one per connection and
/// reuse it for every [`Self::recv`] call on that connection.
pub struct WireReader {
    schema: RoleReader,
    dictionary: RoleReader,
    labels: RoleReader,
    record: RoleReader,
}

impl WireReader {
    /// Constructs a reader with no retained IPC state yet.
    pub fn new() -> Self {
        Self {
            schema: RoleReader::new(),
            dictionary: RoleReader::new(),
            labels: RoleReader::new(),
            record: RoleReader::new(),
        }
    }

    /// Reads one [`SketchStreamBatch`] window previously written by a
    /// matching [`WireWriter::send`] call on the same connection.
    /// Returns `Ok(None)` on a clean EOF at a frame boundary.
    pub async fn recv(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<Option<SketchStreamBatch>, WireError> {
        let Some(body) = read_frame(stream).await? else {
            return Ok(None);
        };
        let names = ["schema", "dictionary", "labels", "record"];
        let parts = unframe_parts(&body, &names)?;
        let mut deltas = parts.into_iter();
        let (schema_delta, _) = deltas.next().expect("schema part");
        let (dictionary_delta, _) = deltas.next().expect("dictionary part");
        let (labels_delta, _) = deltas.next().expect("labels part");
        let (record_delta, _) = deltas.next().expect("record part");

        let schema = self
            .schema
            .ingest(schema_delta, "schema")?
            .ok_or(WireError::EmptyRecordBatch { which: "schema" })?;
        let dictionary = self
            .dictionary
            .ingest(dictionary_delta, "dictionary")?
            .ok_or(WireError::EmptyRecordBatch {
                which: "dictionary",
            })?;
        let labels = self
            .labels
            .ingest(labels_delta, "labels")?
            .ok_or(WireError::EmptyRecordBatch { which: "labels" })?;
        let record = self
            .record
            .ingest(record_delta, "record")?
            .ok_or(WireError::EmptyRecordBatch { which: "record" })?;

        Ok(Some(SketchStreamBatch {
            schema,
            dictionary,
            labels,
            record,
        }))
    }
}

impl Default for WireReader {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Encoding, SketchEnvelope, SketchType};
    use crate::observation::KeyValue;
    use crate::otap::dictionary::{SeriesDictionary, SeriesDictionaryDecoder};
    use tokio::net::{TcpListener, TcpStream};

    fn envelope() -> SketchEnvelope {
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::DDSketch,
            agg_id: 7,
            resource_labels: Vec::new(),
            labels: vec![KeyValue::new("path", "/api")],
            window_start_ms: 1_000,
            window_end_ms: 11_000,
            encoding: Encoding::ProtoFull,
            payload: vec![1, 2, 3, 4, 5],
            hash_spec: None,
            metric_name: "http_request_duration_ms".into(),
            count: 42,
            aggregation_temporality: 1,
            value: 0.0,
        }
    }

    /// Spawns a loopback listener and returns (client stream,
    /// accept-side server task handle) — shared setup for every test
    /// below.
    async fn loopback() -> (TcpStream, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let client = TcpStream::connect(addr).await.expect("connect");
        (client, listener)
    }

    #[tokio::test]
    async fn wire_writer_reader_round_trips_a_populated_batch() {
        let mut dict = SeriesDictionary::new();
        let env = envelope();
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");

        let (mut client, listener) = loopback().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            WireReader::new()
                .recv(&mut socket)
                .await
                .expect("recv")
                .expect("Some(batch)")
        });

        WireWriter::new()
            .send(&mut client, &batch)
            .await
            .expect("send");
        drop(client); // signal EOF after the one frame.

        let received = server.await.expect("server task");
        assert_eq!(received.schema.num_rows(), batch.schema.num_rows());
        assert_eq!(received.dictionary.num_rows(), batch.dictionary.num_rows());
        assert_eq!(received.labels.num_rows(), batch.labels.num_rows());
        assert_eq!(received.record.num_rows(), batch.record.num_rows());

        // The whole point: joining the IPC-round-tripped batch back
        // through SeriesDictionaryDecoder reconstructs the original
        // envelope exactly.
        let mut decoder = SeriesDictionaryDecoder::new();
        let out = decoder.decode(&received).expect("dictionary decode");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, env.payload);
        assert_eq!(out[0].labels, env.labels);
        assert_eq!(out[0].metric_name, env.metric_name);
        assert_eq!(out[0].sketch_type, env.sketch_type);
    }

    #[tokio::test]
    async fn wire_writer_reader_round_trips_an_empty_batch() {
        let mut dict = SeriesDictionary::new();
        let batch = dict.encode(&[], None).expect("encode empty");

        let (mut client, listener) = loopback().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            WireReader::new()
                .recv(&mut socket)
                .await
                .expect("recv")
                .expect("Some(batch)")
        });

        WireWriter::new()
            .send(&mut client, &batch)
            .await
            .expect("send");
        drop(client);

        let received = server.await.expect("server task");
        assert_eq!(received.schema.num_rows(), 0);
        assert_eq!(received.dictionary.num_rows(), 0);
        assert_eq!(received.labels.num_rows(), 0);
        assert_eq!(received.record.num_rows(), 0);
    }

    #[tokio::test]
    async fn recv_returns_none_on_clean_eof_between_frames() {
        let (client, listener) = loopback().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            WireReader::new().recv(&mut socket).await
        });

        drop(client); // close immediately, no frames sent.

        let result = server.await.expect("server task").expect("no error");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn wire_writer_reader_round_trip_multiple_windows_over_tcp() {
        // The persistent-connection API's whole point: send several
        // windows for the same series over one connection and get
        // them all back correctly, including the repeat-window case
        // where schema/dictionary/labels are genuinely empty.
        let mut dict = SeriesDictionary::new();
        let env1 = envelope();
        let batch1 = dict
            .encode(std::slice::from_ref(&env1), None)
            .expect("window 1");
        let env2 = SketchEnvelope {
            window_start_ms: 11_000,
            window_end_ms: 21_000,
            payload: vec![9, 9, 9],
            ..envelope()
        };
        let batch2 = dict
            .encode(std::slice::from_ref(&env2), None)
            .expect("window 2");
        // Confirm the dictionary-economics precondition this test
        // relies on: window 2 really is schema/dictionary/labels-free.
        assert_eq!(batch2.schema.num_rows(), 0);
        assert_eq!(batch2.dictionary.num_rows(), 0);
        assert_eq!(batch2.labels.num_rows(), 0);

        let (mut client, listener) = loopback().await;
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut reader = WireReader::new();
            let mut out = Vec::new();
            while let Some(batch) = reader.recv(&mut socket).await.expect("recv") {
                out.push(batch);
            }
            out
        });

        let mut writer = WireWriter::new();
        writer.send(&mut client, &batch1).await.expect("send w1");
        writer.send(&mut client, &batch2).await.expect("send w2");
        drop(client);

        let received = server.await.expect("server task");
        assert_eq!(received.len(), 2);

        let mut decoder = SeriesDictionaryDecoder::new();
        let out1 = decoder.decode(&received[0]).expect("decode w1");
        assert_eq!(out1.len(), 1);
        assert_eq!(out1[0].payload, env1.payload);

        let out2 = decoder.decode(&received[1]).expect("decode w2");
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].payload, env2.payload);
        assert_eq!(out2[0].window_start_ms, 11_000);
    }
}
