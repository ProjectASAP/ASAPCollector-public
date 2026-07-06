//! Integration tests for the OTAP-Rust codec.
//!
//! These exercise the codec end-to-end as a stable public surface,
//! complementing the unit tests in `src/otap/{decode,encode,schema,plugin}.rs`.
//!
//! Coverage map:
//!   - decode-then-encode round trip preserves payload bytes per row.
//!   - encode-then-decode round trip preserves envelope bytes.
//!   - per-sketch-type (DDSketch / KLL / HLL / CountSketch / CountMinSketch)
//!     envelopes round-trip with their `payload` field intact — the
//!     codec is sketch-agnostic, but the test asserts that promise.

#![cfg(feature = "otap")]

use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};

use asap_precompute_rs::envelope::{Encoding, SketchEnvelope, SketchType};
use asap_precompute_rs::observation::{KeyValue, ObservationValueKind};
use asap_precompute_rs::otap::{
    decode_batch, encode_batch, ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION,
    ATTR_SKETCH_TYPE, ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, COLUMN_METRIC,
    COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};

fn envelope_for(sketch_type: SketchType, payload: &[u8]) -> SketchEnvelope {
    SketchEnvelope {
        schema_version: 1,
        sketch_type,
        agg_id: 1234,
        resource_labels: vec![],
        labels: vec![
            KeyValue::new("path", "/api"),
            KeyValue::new("region", "us-east"),
        ],
        window_start_ms: 1_000,
        window_end_ms: 2_000,
        encoding: Encoding::ProtoFull,
        payload: payload.to_vec(),
        hash_spec: None,
        metric_name: format!("metric_{}", sketch_type.name()),
        count: 0,
        aggregation_temporality: 0,
    }
}

/// Build a hand-crafted Strategy-B RecordBatch carrying envelope rows
/// for every sketch type in `entries`. Used to assert the
/// decode → encode → decode round trip preserves payload bytes per row.
fn batch_from_envelope_entries(entries: &[(SketchType, &[u8])]) -> RecordBatch {
    let metric_col: Vec<&str> = entries
        .iter()
        .map(|(t, _)| match t {
            SketchType::DDSketch => "metric_DDSketch",
            SketchType::KLLSketch => "metric_KLLSketch",
            SketchType::HLLSketch => "metric_HLLSketch",
            SketchType::CountSketch => "metric_CountSketch",
            SketchType::CountMinSketch => "metric_CountMinSketch",
            SketchType::Unspecified => "metric_Unspecified",
        })
        .collect();
    let envelope_col: Vec<&[u8]> = entries.iter().map(|(_, p)| *p).collect();
    let sketch_type_col: Vec<&str> = entries.iter().map(|(t, _)| t.name()).collect();
    let agg_id_col: Vec<u64> = entries.iter().map(|_| 1234_u64).collect();
    let schema_version_col: Vec<u32> = entries.iter().map(|_| 1_u32).collect();
    let window_start_col: Vec<u64> = entries.iter().map(|_| 1_000_u64).collect();
    let window_end_col: Vec<u64> = entries.iter().map(|_| 2_000_u64).collect();
    let encoding_col: Vec<&str> = entries.iter().map(|_| "PROTO_FULL").collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new(COLUMN_METRIC, DataType::Utf8, false),
        Field::new(ATTR_ENVELOPE, DataType::Binary, false),
        Field::new(ATTR_SKETCH_TYPE, DataType::Utf8, false),
        Field::new(ATTR_AGG_ID, DataType::UInt64, false),
        Field::new(ATTR_SCHEMA_VERSION, DataType::UInt32, false),
        Field::new(ATTR_WINDOW_START_MS, DataType::UInt64, false),
        Field::new(ATTR_WINDOW_END_MS, DataType::UInt64, false),
        Field::new(ATTR_ENCODING, DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(metric_col)),
            Arc::new(BinaryArray::from(envelope_col)),
            Arc::new(StringArray::from(sketch_type_col)),
            Arc::new(UInt64Array::from(agg_id_col)),
            Arc::new(UInt32Array::from(schema_version_col)),
            Arc::new(UInt64Array::from(window_start_col)),
            Arc::new(UInt64Array::from(window_end_col)),
            Arc::new(StringArray::from(encoding_col)),
        ],
    )
    .expect("build batch")
}

#[test]
fn decode_then_encode_preserves_payload_bytes_per_row() {
    // Mix of distinct payloads across rows to verify the codec
    // doesn't accidentally hoist the first row's bytes onto every
    // subsequent row.
    let p0 = vec![0xde, 0xad, 0xbe, 0xef];
    let p1 = vec![0x01, 0x02, 0x03];
    let p2 = (0..=255_u8).collect::<Vec<_>>();
    let batch_in = batch_from_envelope_entries(&[
        (SketchType::DDSketch, &p0),
        (SketchType::KLLSketch, &p1),
        (SketchType::HLLSketch, &p2),
    ]);

    let observations = decode_batch(&batch_in).expect("decode");
    assert_eq!(observations.len(), 3);

    let envelopes: Vec<SketchEnvelope> = observations
        .into_iter()
        .map(|obs| {
            assert_eq!(obs.value.kind, ObservationValueKind::Envelope);
            *obs.value
                .envelope
                .expect("decoded row should be envelope-kind")
        })
        .collect();

    let batch_out = encode_batch(&envelopes).expect("encode");
    assert_eq!(batch_out.num_rows(), 3);

    let env_col = batch_out
        .column_by_name(ATTR_ENVELOPE)
        .expect("envelope column on encoded batch")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("Binary type");
    assert_eq!(env_col.value(0), p0.as_slice());
    assert_eq!(env_col.value(1), p1.as_slice());
    assert_eq!(env_col.value(2), p2.as_slice());
}

#[test]
fn encode_then_decode_preserves_envelope_bytes() {
    let envelopes = vec![
        envelope_for(SketchType::DDSketch, &[1, 1, 1, 1]),
        envelope_for(SketchType::KLLSketch, &[2, 2, 2]),
        envelope_for(
            SketchType::CountMinSketch,
            &(0..128_u8).collect::<Vec<u8>>(),
        ),
    ];

    let batch = encode_batch(&envelopes).expect("encode");
    let observations = decode_batch(&batch).expect("decode");
    assert_eq!(observations.len(), envelopes.len());

    for (obs, original) in observations.iter().zip(envelopes.iter()) {
        assert_eq!(obs.value.kind, ObservationValueKind::Envelope);
        let decoded = obs.value.envelope.as_ref().expect("envelope present");
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.sketch_type, original.sketch_type);
        assert_eq!(decoded.agg_id, original.agg_id);
        assert_eq!(decoded.schema_version, original.schema_version);
        assert_eq!(decoded.window_start_ms, original.window_start_ms);
        assert_eq!(decoded.window_end_ms, original.window_end_ms);
        assert_eq!(decoded.encoding, original.encoding);
        assert_eq!(decoded.metric_name, original.metric_name);
        // Labels round-trip through the union-of-keys label column
        // construction.
        assert_eq!(decoded.labels, original.labels);
    }
}

#[test]
fn per_sketch_type_round_trip_smoke() {
    // The codec is sketch-agnostic — it carries opaque envelope
    // bytes — but all five sketch types must round-trip through
    // encode/decode without payload corruption. Treat each as a
    // smoke test.
    for (sketch_type, payload) in [
        (SketchType::DDSketch, vec![0xaa; 32]),
        (SketchType::KLLSketch, vec![0xbb; 64]),
        (SketchType::HLLSketch, vec![0xcc; 16]),
        (SketchType::CountSketch, vec![0xdd; 128]),
        (SketchType::CountMinSketch, vec![0xee; 256]),
    ] {
        let env = envelope_for(sketch_type, &payload);
        let batch = encode_batch(std::slice::from_ref(&env)).expect("encode");
        let obs = decode_batch(&batch).expect("decode");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].value.kind, ObservationValueKind::Envelope);
        let decoded = obs[0].value.envelope.as_ref().expect("envelope");
        assert_eq!(
            decoded.payload, payload,
            "payload corruption for sketch type {:?}",
            sketch_type
        );
        assert_eq!(decoded.sketch_type, sketch_type);
    }
}

#[test]
fn scalar_then_back_round_trip_via_observation_does_not_corrupt_metric_name() {
    // Scalar (non-envelope) decode path: the codec emits a Float
    // observation. Encode is envelope-direction only by design (encode
    // is the egress path — ingest is decode-only). This test pins the
    // asymmetry: encoding a Float observation back to OTAP is NOT in the
    // codec's surface because the runtime never asks the codec to encode
    // raw scalars — only `Precompute::tick` envelopes flow through encode.
    let schema = Arc::new(Schema::new(vec![
        Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
        Field::new(COLUMN_METRIC, DataType::Utf8, false),
        Field::new(COLUMN_VALUE, DataType::Float64, false),
        Field::new("k1", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1_000_000_u64; 3])), // 1ms
            Arc::new(StringArray::from(vec!["m1", "m2", "m3"])),
            Arc::new(Float64Array::from(vec![1.0_f64, 2.0, 3.0])),
            Arc::new(StringArray::from(vec![Some("v"), None, Some("z")])),
        ],
    )
    .expect("build batch");

    let obs = decode_batch(&batch).expect("decode");
    assert_eq!(obs.len(), 3);
    assert_eq!(obs[0].metric, "m1");
    assert_eq!(obs[0].value.kind, ObservationValueKind::Float);
    assert_eq!(obs[0].value.float, 1.0);
    assert_eq!(obs[0].labels, vec![KeyValue::new("k1", "v")]);
    // Row 1 had a null label cell; decode skips it.
    assert!(obs[1].labels.is_empty());
}
