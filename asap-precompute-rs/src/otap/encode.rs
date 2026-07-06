//! `encode_batch`: `&[SketchEnvelope]` → [`arrow_array::RecordBatch`].
//!
//! Strategy-B encoding: one row per envelope, with the envelope
//! payload riding in the
//! well-known `_asap_envelope` Binary column and companion metadata
//! keys (`_asap_sketch_type`, `_asap_agg_id`, `_asap_schema_version`,
//! `_asap_window_start_ms`, `_asap_window_end_ms`, `_asap_encoding`)
//! riding in their typed columns.
//!
//! At the Phase-B layer those carrier keys are projected as flat
//! columns. The Phase-C plugin shell will lift them onto OTAP's
//! per-row attribute child batch (OTAP's strict schema validator
//! rejects extension columns on Logs/Metrics/Traces RecordBatches,
//! so the lift step is required before the batch goes downstream).

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use thiserror::Error;

use crate::envelope::SketchEnvelope;

use super::schema::{
    ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SKETCH_TYPE,
    ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};

/// Errors returned by [`encode_batch`].
#[derive(Debug, Error)]
pub enum OtapEncodeError {
    /// Constructing the Arrow `RecordBatch` failed. This indicates a
    /// codec bug (mismatched array lengths) rather than caller error;
    /// kept as an error variant rather than `unwrap` so the plugin
    /// shell can surface it via OTAP's error channel rather than
    /// crash the host process.
    #[error("otap encode: arrow record-batch construction failed: {0}")]
    ArrowError(String),
}

/// Encode a slice of [`SketchEnvelope`]s as a single OTAP `RecordBatch`.
///
/// Schema: the well-known scalar columns
/// (`time_unix_nano` UInt64, `metric` Utf8, `value` Float64) plus the
/// Strategy-B carrier columns (`_asap_envelope` Binary,
/// `_asap_sketch_type` Utf8, `_asap_agg_id` UInt64,
/// `_asap_schema_version` UInt32, `_asap_window_start_ms` UInt64,
/// `_asap_window_end_ms` UInt64, `_asap_encoding` Utf8) plus one
/// `Utf8` column per distinct label key drawn from the union of
/// every envelope's `labels` slice.
///
/// `value` is always encoded as 0.0 — encode is the
/// envelope-emission direction, so the `value` column is informational
/// only, and the actual sketch state rides in `_asap_envelope`. Encode
/// does not touch `Observation::resource_labels` because envelopes
/// flatten resource attrs into `labels` already (Strategy-B
/// platforms have no resource scope; per envelope.rs docstring).
///
/// `time_unix_nano` is set to `window_end_ms * 1_000_000`.
///
/// Returns an empty batch (zero rows, schema-only) when `envelopes` is
/// empty.
pub fn encode_batch(envelopes: &[SketchEnvelope]) -> Result<RecordBatch, OtapEncodeError> {
    let label_keys = collect_label_keys(envelopes);

    let mut fields: Vec<Field> = vec![
        Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, true),
        Field::new(COLUMN_METRIC, DataType::Utf8, true),
        Field::new(COLUMN_VALUE, DataType::Float64, true),
        Field::new(ATTR_ENVELOPE, DataType::Binary, true),
        Field::new(ATTR_SKETCH_TYPE, DataType::Utf8, true),
        Field::new(ATTR_AGG_ID, DataType::UInt64, true),
        Field::new(ATTR_SCHEMA_VERSION, DataType::UInt32, true),
        Field::new(ATTR_WINDOW_START_MS, DataType::UInt64, true),
        Field::new(ATTR_WINDOW_END_MS, DataType::UInt64, true),
        Field::new(ATTR_ENCODING, DataType::Utf8, true),
    ];
    for key in &label_keys {
        fields.push(Field::new(key, DataType::Utf8, true));
    }

    let mut time_col: Vec<Option<u64>> = Vec::with_capacity(envelopes.len());
    let mut metric_col: Vec<Option<String>> = Vec::with_capacity(envelopes.len());
    let mut value_col: Vec<Option<f64>> = Vec::with_capacity(envelopes.len());
    let mut envelope_col: Vec<Option<Vec<u8>>> = Vec::with_capacity(envelopes.len());
    let mut sketch_type_col: Vec<Option<&'static str>> = Vec::with_capacity(envelopes.len());
    let mut agg_id_col: Vec<Option<u64>> = Vec::with_capacity(envelopes.len());
    let mut schema_version_col: Vec<Option<u32>> = Vec::with_capacity(envelopes.len());
    let mut window_start_col: Vec<Option<u64>> = Vec::with_capacity(envelopes.len());
    let mut window_end_col: Vec<Option<u64>> = Vec::with_capacity(envelopes.len());
    let mut encoding_col: Vec<Option<&'static str>> = Vec::with_capacity(envelopes.len());
    let mut label_cols: Vec<Vec<Option<String>>> = (0..label_keys.len())
        .map(|_| Vec::with_capacity(envelopes.len()))
        .collect();

    for env in envelopes {
        time_col.push(Some(env.window_end_ms.saturating_mul(1_000_000)));
        metric_col.push(Some(env.metric_name.clone()));
        value_col.push(Some(0.0));
        envelope_col.push(Some(env.payload.clone()));
        sketch_type_col.push(Some(env.sketch_type.name()));
        agg_id_col.push(Some(env.agg_id));
        schema_version_col.push(Some(env.schema_version));
        window_start_col.push(Some(env.window_start_ms));
        window_end_col.push(Some(env.window_end_ms));
        encoding_col.push(Some(env.encoding.name()));

        for (col_idx, key) in label_keys.iter().enumerate() {
            let value = env.labels.iter().find_map(|kv| {
                if &kv.key == key {
                    Some(kv.value.clone())
                } else {
                    None
                }
            });
            label_cols[col_idx].push(value);
        }
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt64Array::from(time_col)),
        Arc::new(StringArray::from(metric_col)),
        Arc::new(Float64Array::from(value_col)),
        Arc::new(BinaryArray::from_opt_vec(
            envelope_col
                .iter()
                .map(|o| o.as_deref())
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(sketch_type_col)),
        Arc::new(UInt64Array::from(agg_id_col)),
        Arc::new(UInt32Array::from(schema_version_col)),
        Arc::new(UInt64Array::from(window_start_col)),
        Arc::new(UInt64Array::from(window_end_col)),
        Arc::new(StringArray::from(encoding_col)),
    ];
    for col in label_cols {
        columns.push(Arc::new(StringArray::from(col)));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| OtapEncodeError::ArrowError(e.to_string()))
}

/// Walk every envelope's `labels` slice and return the sorted union of
/// keys. Sorted so the emitted RecordBatch schema is deterministic
/// across runs — important for reproducibility tests and for the
/// cross-host parity tests in Phase E.
fn collect_label_keys(envelopes: &[SketchEnvelope]) -> Vec<String> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for env in envelopes {
        for kv in &env.labels {
            set.insert(kv.key.clone());
        }
    }
    set.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Encoding, SketchType};
    use crate::observation::KeyValue;

    fn sample_envelope(payload: &[u8], agg_id: u64) -> SketchEnvelope {
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::DDSketch,
            agg_id,
            resource_labels: vec![],
            labels: vec![KeyValue::new("region", "us-east")],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::ProtoFull,
            payload: payload.to_vec(),
            hash_spec: None,
            metric_name: "http_request_duration".into(),
            count: 0,
            aggregation_temporality: 0,
        }
    }

    #[test]
    fn empty_envelopes_produces_empty_batch() {
        let batch = encode_batch(&[]).expect("encode");
        assert_eq!(batch.num_rows(), 0);
        // Schema still carries the well-known columns.
        assert!(batch.schema().column_with_name(ATTR_ENVELOPE).is_some());
    }

    #[test]
    fn encode_single_envelope_carries_all_fields() {
        let env = sample_envelope(&[0xde, 0xad, 0xbe, 0xef], 7);
        let batch = encode_batch(std::slice::from_ref(&env)).expect("encode");
        assert_eq!(batch.num_rows(), 1);

        let env_col = batch
            .column_by_name(ATTR_ENVELOPE)
            .expect("envelope column")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary");
        assert_eq!(env_col.value(0), &[0xde, 0xad, 0xbe, 0xef]);

        let st = batch
            .column_by_name(ATTR_SKETCH_TYPE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(st.value(0), "DDSketch");

        let id = batch
            .column_by_name(ATTR_AGG_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(id.value(0), 7);
    }

    #[test]
    fn label_columns_are_union_sorted() {
        let env_a = SketchEnvelope {
            labels: vec![KeyValue::new("region", "a"), KeyValue::new("zone", "z1")],
            ..sample_envelope(&[1], 1)
        };
        let env_b = SketchEnvelope {
            labels: vec![KeyValue::new("region", "b"), KeyValue::new("path", "/x")],
            ..sample_envelope(&[2], 1)
        };
        let batch = encode_batch(&[env_a, env_b]).expect("encode");

        // The union is {path, region, zone} sorted; reserved columns
        // appear before the label columns.
        let schema = batch.schema();
        let label_names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .filter(|n| !super::super::schema::is_reserved_column(n))
            .collect();
        assert_eq!(label_names, vec!["path", "region", "zone"]);

        // Row 0 (env_a) has region=a, zone=z1, path=null.
        let path_col = batch
            .column_by_name("path")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(path_col.is_null(0));
        assert_eq!(path_col.value(1), "/x");
    }
}
