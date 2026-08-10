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
//! [u32 schema_len]     [schema_len bytes: Arrow IPC stream]
//! [u32 dictionary_len] [dictionary_len bytes: Arrow IPC stream]
//! [u32 labels_len]     [labels_len bytes: Arrow IPC stream]
//! [u32 record_len]     [record_len bytes: Arrow IPC stream]
//! ```
//!
//! Each sub-batch is its own self-contained Arrow IPC *stream*
//! (schema message + one record-batch message + EOS) via
//! [`arrow_ipc::writer::StreamWriter`] — not a shared/continuous
//! Arrow IPC stream across the whole session. That's a deliberate
//! simplification: a real continuous-stream transport would let the
//! four sub-streams themselves carry the Schema/Dictionary economics
//! at the Arrow IPC layer too (per `docs/data_model.md`'s closing
//! "Open design question"), but framing each `SketchStreamBatch` as
//! four independent one-shot streams keeps this module's job to
//! exactly "get the same four `RecordBatch`es to the other side
//! intact," leaving `SeriesDictionary`/`SeriesDictionaryDecoder` (not
//! this module) responsible for the actual dedup.
//!
//! All four sub-batch lengths (and the leading `total_len`) are
//! big-endian `u32`s. A zero-row batch still serializes to a valid
//! (small) Arrow IPC stream — a schema message plus an empty record
//! batch — so a wire-level frame always carries exactly four
//! sub-streams even when, say, `schema`/`dictionary`/`labels` are
//! empty because the series was already known.

use std::io;

use arrow_array::RecordBatch;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::dictionary::SketchStreamBatch;

/// Failure modes for [`encode_stream_batch`] / [`decode_stream_batch`]
/// / [`send_stream_batch`] / [`recv_stream_batch`].
#[derive(Debug, Error)]
pub enum WireError {
    /// Arrow IPC encode/decode failed (malformed batch, schema
    /// mismatch inside a sub-stream, etc.).
    #[error("otap wire: arrow ipc error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    /// A sub-stream decoded to zero record batches (`StreamReader`
    /// yielded nothing) — a well-formed IPC stream always carries
    /// exactly one, even for zero rows.
    #[error("otap wire: sub-batch {which:?} decoded no record batches")]
    EmptyRecordBatch {
        /// Which of the four sub-batches was empty.
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
}

/// Serializes one [`RecordBatch`] as a self-contained Arrow IPC
/// stream (schema + one record batch + EOS).
fn write_ipc_bytes(batch: &RecordBatch) -> Result<Vec<u8>, WireError> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserializes one [`RecordBatch`] from bytes written by
/// [`write_ipc_bytes`]. `which` names the sub-batch for error
/// messages only.
fn read_ipc_bytes(bytes: &[u8], which: &'static str) -> Result<RecordBatch, WireError> {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(bytes, None)?;
    match reader.next() {
        Some(batch) => Ok(batch?),
        None => Err(WireError::EmptyRecordBatch { which }),
    }
}

/// Serializes a whole [`SketchStreamBatch`] into one length-prefixed
/// frame — see the module doc for the exact byte layout. Does *not*
/// include the leading `total_len` prefix; that's added by
/// [`send_stream_batch`] (or by a caller framing its own transport,
/// e.g. writing this to a file).
pub fn encode_stream_batch(batch: &SketchStreamBatch) -> Result<Vec<u8>, WireError> {
    let parts = [
        write_ipc_bytes(&batch.schema)?,
        write_ipc_bytes(&batch.dictionary)?,
        write_ipc_bytes(&batch.labels)?,
        write_ipc_bytes(&batch.record)?,
    ];
    let mut out = Vec::with_capacity(parts.iter().map(|p| p.len() + 4).sum());
    for part in &parts {
        out.extend_from_slice(&(part.len() as u32).to_be_bytes());
        out.extend_from_slice(part);
    }
    Ok(out)
}

/// Inverse of [`encode_stream_batch`]: reconstructs a
/// [`SketchStreamBatch`] from a frame's body bytes (i.e. everything
/// after the leading `total_len` prefix, if any).
pub fn decode_stream_batch(bytes: &[u8]) -> Result<SketchStreamBatch, WireError> {
    let mut cursor = bytes;
    let names = ["schema", "dictionary", "labels", "record"];
    let mut parts: Vec<RecordBatch> = Vec::with_capacity(4);
    for which in names {
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
        parts.push(read_ipc_bytes(part, which)?);
        cursor = rest;
    }
    let mut parts = parts.into_iter();
    Ok(SketchStreamBatch {
        schema: parts.next().expect("schema part"),
        dictionary: parts.next().expect("dictionary part"),
        labels: parts.next().expect("labels part"),
        record: parts.next().expect("record part"),
    })
}

/// Sends one [`SketchStreamBatch`] over `stream`, framed with a
/// leading big-endian `u32` total length. The receiving side reads it
/// back with [`recv_stream_batch`].
pub async fn send_stream_batch(
    stream: &mut TcpStream,
    batch: &SketchStreamBatch,
) -> Result<(), WireError> {
    let body = encode_stream_batch(batch)?;
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    Ok(())
}

/// Reads one [`SketchStreamBatch`] previously written by
/// [`send_stream_batch`]. Returns `Ok(None)` on a clean EOF at a
/// frame boundary (the sender closed the connection after its last
/// batch) rather than an error.
pub async fn recv_stream_batch(
    stream: &mut TcpStream,
) -> Result<Option<SketchStreamBatch>, WireError> {
    let mut len_buf = [0u8; 4];
    if !read_exact_or_eof(stream, &mut len_buf).await? {
        return Ok(None);
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(Some(decode_stream_batch(&body)?))
}

/// Like [`tokio::io::AsyncReadExt::read_exact`], but distinguishes "EOF
/// before any byte of this frame" (returns `Ok(false)` — a clean
/// stream close between frames) from "EOF partway through a frame's
/// length prefix" (still surfaced as an error by the subsequent
/// `read_exact` inside [`recv_stream_batch`], since that's a
/// truncated frame, not a clean close).
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Encoding, SketchEnvelope, SketchType};
    use crate::observation::KeyValue;
    use crate::otap::dictionary::{SeriesDictionary, SeriesDictionaryDecoder};

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

    #[test]
    fn encode_decode_round_trips_a_populated_batch() {
        let mut dict = SeriesDictionary::new();
        let env = envelope();
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");

        let bytes = encode_stream_batch(&batch).expect("wire encode");
        let decoded_batch = decode_stream_batch(&bytes).expect("wire decode");

        assert_eq!(decoded_batch.schema.num_rows(), batch.schema.num_rows());
        assert_eq!(
            decoded_batch.dictionary.num_rows(),
            batch.dictionary.num_rows()
        );
        assert_eq!(decoded_batch.labels.num_rows(), batch.labels.num_rows());
        assert_eq!(decoded_batch.record.num_rows(), batch.record.num_rows());

        // The whole point: joining the IPC-round-tripped batch back
        // through SeriesDictionaryDecoder reconstructs the original
        // envelope exactly.
        let mut decoder = SeriesDictionaryDecoder::new();
        let out = decoder.decode(&decoded_batch).expect("dictionary decode");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, env.payload);
        assert_eq!(out[0].labels, env.labels);
        assert_eq!(out[0].metric_name, env.metric_name);
        assert_eq!(out[0].sketch_type, env.sketch_type);
    }

    #[test]
    fn encode_decode_round_trips_an_empty_batch() {
        let mut dict = SeriesDictionary::new();
        let batch = dict.encode(&[], None).expect("encode empty");
        let bytes = encode_stream_batch(&batch).expect("wire encode");
        let decoded = decode_stream_batch(&bytes).expect("wire decode");
        assert!(decoded.schema.num_rows() == 0);
        assert!(decoded.dictionary.num_rows() == 0);
        assert!(decoded.labels.num_rows() == 0);
        assert!(decoded.record.num_rows() == 0);
    }

    #[test]
    fn encode_decode_round_trips_repeat_window_dictionary_free_batch() {
        // The batch that matters most: window 2+ for an
        // already-known series, where schema/dictionary/labels are
        // genuinely empty and only `record` carries a row.
        let mut dict = SeriesDictionary::new();
        let env1 = envelope();
        let _ = dict
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
        assert_eq!(batch2.schema.num_rows(), 0);
        assert_eq!(batch2.dictionary.num_rows(), 0);
        assert_eq!(batch2.labels.num_rows(), 0);
        assert_eq!(batch2.record.num_rows(), 1);

        let bytes = encode_stream_batch(&batch2).expect("wire encode");
        let decoded_batch = decode_stream_batch(&bytes).expect("wire decode");

        let mut decoder = SeriesDictionaryDecoder::new();
        // Must ingest window 1 first — this decoded batch alone has
        // no DICTIONARY entry to resolve series_id 0 against.
        let bytes1 = encode_stream_batch(&dict_only_window1(&env1)).expect("encode w1");
        let decoded1 = decode_stream_batch(&bytes1).expect("decode w1");
        decoder.decode(&decoded1).expect("decode w1 into decoder");

        let out = decoder.decode(&decoded_batch).expect("decode w2");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, env2.payload);
        assert_eq!(out[0].window_start_ms, 11_000);
        assert_eq!(out[0].window_end_ms, 21_000);
    }

    /// Helper for the test above: re-derives window 1's batch from a
    /// fresh dictionary so it can be fed to a fresh decoder
    /// independently of the outer test's `dict` state.
    fn dict_only_window1(env: &SketchEnvelope) -> SketchStreamBatch {
        let mut dict = SeriesDictionary::new();
        dict.encode(std::slice::from_ref(env), None)
            .expect("encode")
    }

    #[tokio::test]
    async fn send_recv_round_trips_over_a_real_tcp_loopback_socket() {
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let mut dict = SeriesDictionary::new();
        let env = envelope();
        let batch = dict
            .encode(std::slice::from_ref(&env), None)
            .expect("encode");

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            recv_stream_batch(&mut socket)
                .await
                .expect("recv")
                .expect("Some(batch)")
        });

        let mut client = TcpStream::connect(addr).await.expect("connect");
        send_stream_batch(&mut client, &batch).await.expect("send");
        drop(client); // signal EOF after the one frame.

        let received = server.await.expect("server task");
        let mut decoder = SeriesDictionaryDecoder::new();
        let out = decoder.decode(&received).expect("decode");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload, env.payload);
    }

    #[tokio::test]
    async fn recv_returns_none_on_clean_eof_between_frames() {
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            recv_stream_batch(&mut socket).await
        });

        let client = TcpStream::connect(addr).await.expect("connect");
        drop(client); // close immediately, no frames sent.

        let result = server.await.expect("server task").expect("no error");
        assert!(result.is_none());
    }
}
