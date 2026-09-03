//! `decode_batch`: [`arrow_array::RecordBatch`] → `Vec<Observation>`.
//!
//! One `Observation` per row in the input batch. The OTAP shape is
//! column-major where OTel's pmetric is a tree, so the codec walks
//! the columns once up front (resolving them to typed Arrow array
//! references), then iterates rows pulling values by index. This is
//! the throughput win.

use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    UInt32Array, UInt64Array,
};
use arrow_schema::DataType;
use thiserror::Error;

use crate::envelope::{Encoding, SketchEnvelope, SketchType};
use crate::observation::{KeyValue, Observation, ObservationValue};

use super::schema::{
    is_reserved_column, ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION,
    ATTR_SKETCH_TYPE, ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, COLUMN_METRIC,
    COLUMN_TIME_UNIX_NANO, COLUMN_VALUE,
};

/// Errors returned by [`decode_batch`].
///
/// Granularity choice: each error variant identifies the **column** at
/// fault and the kind of failure (wrong type / missing required field).
/// Variants are intentionally informative-but-not-too-fine: the caller
/// (Phase C plugin) typically logs and drops the whole batch — there
/// is no per-row recovery path because the codec promises atomic
/// decode of the input batch.
#[derive(Debug, Error)]
pub enum OtapDecodeError {
    /// A required typed column was present but had an unexpected
    /// Arrow `DataType`. Typical cause: an upstream Arrow producer
    /// emitting `Int64` where `UInt64` is expected.
    #[error(
        "otap decode: column {column:?} has unexpected type: expected {expected}, got {actual:?}"
    )]
    WrongColumnType {
        /// Column name.
        column: String,
        /// Expected Arrow type, in human-readable form.
        expected: &'static str,
        /// Actual Arrow type observed.
        actual: DataType,
    },

    /// An envelope row (one with a non-null `_asap_envelope` cell)
    /// carried a `_asap_sketch_type` value the codec didn't recognize.
    /// Hard error rather than silent fallback because a misrouted
    /// envelope would silently corrupt downstream sketch state.
    #[error("otap decode: row {row}: unknown sketch type {value:?}")]
    UnknownSketchType {
        /// Row index in the input batch.
        row: usize,
        /// Raw string value observed.
        value: String,
    },

    /// An envelope row carried a `_asap_encoding` value the codec
    /// didn't recognize. Same reasoning as `UnknownSketchType`.
    #[error("otap decode: row {row}: unknown encoding {value:?}")]
    UnknownEncoding {
        /// Row index in the input batch.
        row: usize,
        /// Raw string value observed.
        value: String,
    },
}

/// Decode an OTAP `RecordBatch` into a `Vec<Observation>`.
///
/// One `Observation` per row. Column resolution is by name. Any row whose
/// `_asap_envelope` cell is non-null emits a
/// [`crate::observation::ObservationValueKind::Envelope`]
/// observation; otherwise the row routes through the scalar
/// [`crate::observation::ObservationValueKind::Float`] path using
/// the `value` column.
///
/// Resource-attribute scope: at this Phase-B flat-shape layer there
/// is no resource child batch; `Observation::resource_labels` is
/// always empty. Phase C's plugin shell joins the OTAP resource
/// child batch onto the metrics rows before calling `decode_batch`,
/// so resource attrs flow as ordinary `Utf8` columns from the
/// codec's perspective. Adapters that want the OTAP "resource scope"
/// distinction can pre-prefix columns with `resource.` before they
/// hit this codec.
///
/// Returns `Ok(vec![])` for an empty batch.
pub fn decode_batch(batch: &RecordBatch) -> Result<Vec<Observation>, OtapDecodeError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let columns = ResolvedColumns::resolve(batch)?;
    let mut out = Vec::with_capacity(batch.num_rows());

    for row in 0..batch.num_rows() {
        let metric = columns
            .metric
            .as_ref()
            .map(|arr| arr.value(row).to_string())
            .unwrap_or_default();
        let timestamp_ms = columns
            .timestamp
            .as_ref()
            .map(|t| t.timestamp_ms_at(row))
            .unwrap_or(0);
        let labels = columns.labels_for_row(row);

        let value = if let Some(env_arr) = &columns.envelope {
            if env_arr.is_null(row) {
                scalar_value(&columns, row)
            } else {
                let env =
                    build_envelope(&columns, row, &metric, &labels, env_arr.value(row).to_vec())?;
                ObservationValue::envelope(env)
            }
        } else {
            scalar_value(&columns, row)
        };

        out.push(Observation::new(
            timestamp_ms,
            metric,
            Vec::new(), // resource_labels — see docstring above.
            labels,
            value,
        ));
    }

    Ok(out)
}

/// Pulls scalar value for a row, defaulting to 0.0 if absent or null.
fn scalar_value(columns: &ResolvedColumns<'_>, row: usize) -> ObservationValue {
    let v = columns
        .value
        .as_ref()
        .filter(|arr| !arr.is_null(row))
        .map(|arr| arr.value(row))
        .unwrap_or(0.0);
    ObservationValue::float(v)
}

/// Build a [`SketchEnvelope`] for a Strategy-B row.
fn build_envelope(
    columns: &ResolvedColumns<'_>,
    row: usize,
    metric: &str,
    labels: &[KeyValue],
    payload: Vec<u8>,
) -> Result<SketchEnvelope, OtapDecodeError> {
    let sketch_type = if let Some(arr) = &columns.sketch_type {
        if arr.is_null(row) {
            SketchType::Unspecified
        } else {
            parse_sketch_type(row, arr.value(row))?
        }
    } else {
        SketchType::Unspecified
    };

    let encoding = if let Some(arr) = &columns.encoding {
        if arr.is_null(row) {
            Encoding::Unspecified
        } else {
            parse_encoding(row, arr.value(row))?
        }
    } else {
        Encoding::Unspecified
    };

    let agg_id = columns
        .agg_id
        .as_ref()
        .filter(|arr| !arr.is_null(row))
        .map(|arr| arr.value(row))
        .unwrap_or(0);
    let schema_version = columns
        .schema_version
        .as_ref()
        .filter(|arr| !arr.is_null(row))
        .map(|arr| arr.value(row))
        .unwrap_or(0);
    let window_start_ms = columns
        .window_start_ms
        .as_ref()
        .filter(|arr| !arr.is_null(row))
        .map(|arr| arr.value(row))
        .unwrap_or(0);
    let window_end_ms = columns
        .window_end_ms
        .as_ref()
        .filter(|arr| !arr.is_null(row))
        .map(|arr| arr.value(row))
        .unwrap_or(0);

    let value = columns
        .value
        .as_ref()
        .filter(|arr| !arr.is_null(row))
        .map(|arr| arr.value(row))
        .unwrap_or(0.0);

    Ok(SketchEnvelope {
        schema_version,
        sketch_type,
        agg_id,
        resource_labels: Vec::new(),
        labels: labels.to_vec(),
        window_start_ms,
        window_end_ms,
        encoding,
        payload,
        hash_spec: None,
        metric_name: metric.to_string(),
        count: 0,
        aggregation_temporality: 0,
        value,
    })
}

/// Parses a canonical sketch-type string. `pub(super)` so
/// [`super::dictionary`] can reuse the same parsing (and error
/// variant) for `SCHEMA.sketch_type` rows.
pub(super) fn parse_sketch_type(row: usize, raw: &str) -> Result<SketchType, OtapDecodeError> {
    match raw {
        "DDSketch" => Ok(SketchType::DDSketch),
        "KLLSketch" => Ok(SketchType::KLLSketch),
        "HLLSketch" => Ok(SketchType::HLLSketch),
        "CountSketch" => Ok(SketchType::CountSketch),
        "CountMinSketch" => Ok(SketchType::CountMinSketch),
        "Unspecified" | "" => Ok(SketchType::Unspecified),
        _ => Err(OtapDecodeError::UnknownSketchType {
            row,
            value: raw.to_string(),
        }),
    }
}

/// Parses a canonical encoding string. `pub(super)` so
/// [`super::dictionary`] can reuse the same parsing (and error
/// variant) for `SCHEMA.encoding` rows.
pub(super) fn parse_encoding(row: usize, raw: &str) -> Result<Encoding, OtapDecodeError> {
    match raw {
        "PROTO_FULL" => Ok(Encoding::ProtoFull),
        "PROTO_DELTA" => Ok(Encoding::ProtoDelta),
        "MSGPACK" => Ok(Encoding::Msgpack),
        "MSGPACK_DELTA" => Ok(Encoding::MsgpackDelta),
        "UNSPECIFIED" | "" => Ok(Encoding::Unspecified),
        _ => Err(OtapDecodeError::UnknownEncoding {
            row,
            value: raw.to_string(),
        }),
    }
}

/// Pre-resolved typed Arrow array references for the columns we know
/// how to read. Built once per batch in [`ResolvedColumns::resolve`];
/// per-row decoding only does index lookups against these references.
struct ResolvedColumns<'a> {
    timestamp: Option<TimestampColumn<'a>>,
    metric: Option<&'a StringArray>,
    value: Option<&'a Float64Array>,
    envelope: Option<&'a BinaryArray>,
    sketch_type: Option<&'a StringArray>,
    agg_id: Option<&'a UInt64Array>,
    schema_version: Option<&'a UInt32Array>,
    window_start_ms: Option<&'a UInt64Array>,
    window_end_ms: Option<&'a UInt64Array>,
    encoding: Option<&'a StringArray>,
    /// Non-reserved `Utf8` columns; treated as per-row labels.
    label_columns: Vec<(String, &'a StringArray)>,
}

/// Wrapper that unifies `UInt64` and `Timestamp(Nanosecond, _)` into a
/// per-row "milliseconds since epoch" reader so [`decode_batch`]
/// doesn't have to branch on type per row.
enum TimestampColumn<'a> {
    /// Plain `UInt64` interpreted as nanoseconds since epoch.
    UInt64Nanos(&'a UInt64Array),
    /// Native `Timestamp(Nanosecond, _)`.
    Timestamp(&'a TimestampNanosecondArray),
}

impl TimestampColumn<'_> {
    fn timestamp_ms_at(&self, row: usize) -> u64 {
        match self {
            Self::UInt64Nanos(arr) => {
                if arr.is_null(row) {
                    0
                } else {
                    arr.value(row) / 1_000_000
                }
            }
            Self::Timestamp(arr) => {
                if arr.is_null(row) {
                    0
                } else {
                    let v = arr.value(row);
                    if v <= 0 {
                        0
                    } else {
                        (v as u64) / 1_000_000
                    }
                }
            }
        }
    }
}

impl<'a> ResolvedColumns<'a> {
    fn resolve(batch: &'a RecordBatch) -> Result<Self, OtapDecodeError> {
        let schema = batch.schema();
        let mut timestamp = None;
        let mut metric = None;
        let mut value = None;
        let mut envelope = None;
        let mut sketch_type = None;
        let mut agg_id = None;
        let mut schema_version = None;
        let mut window_start_ms = None;
        let mut window_end_ms = None;
        let mut encoding = None;
        let mut label_columns: Vec<(String, &StringArray)> = Vec::new();

        for (i, field) in schema.fields().iter().enumerate() {
            let name = field.name();
            let col: &Arc<dyn Array> = batch.column(i);
            match name.as_str() {
                COLUMN_TIME_UNIX_NANO => {
                    timestamp = Some(resolve_timestamp(name, col)?);
                }
                COLUMN_METRIC => {
                    metric = Some(downcast_string(name, col)?);
                }
                COLUMN_VALUE => {
                    value = Some(downcast_float64(name, col)?);
                }
                ATTR_ENVELOPE => {
                    envelope = Some(downcast_binary(name, col)?);
                }
                ATTR_SKETCH_TYPE => {
                    sketch_type = Some(downcast_string(name, col)?);
                }
                ATTR_AGG_ID => {
                    agg_id = Some(downcast_u64(name, col)?);
                }
                ATTR_SCHEMA_VERSION => {
                    schema_version = Some(downcast_u32(name, col)?);
                }
                ATTR_WINDOW_START_MS => {
                    window_start_ms = Some(downcast_u64(name, col)?);
                }
                ATTR_WINDOW_END_MS => {
                    window_end_ms = Some(downcast_u64(name, col)?);
                }
                ATTR_ENCODING => {
                    encoding = Some(downcast_string(name, col)?);
                }
                _ if !is_reserved_column(name) && matches!(field.data_type(), DataType::Utf8) => {
                    let s = downcast_string(name, col)?;
                    label_columns.push((name.clone(), s));
                }
                _ => {
                    // Non-Utf8, non-reserved columns (e.g. extra
                    // numeric columns from a multi-field metric batch)
                    // are ignored — Phase B is value-column-only.
                    // Future v2 multi-field support reads those here.
                }
            }
        }

        Ok(Self {
            timestamp,
            metric,
            value,
            envelope,
            sketch_type,
            agg_id,
            schema_version,
            window_start_ms,
            window_end_ms,
            encoding,
            label_columns,
        })
    }

    /// Build the labels slice for a single row from all `Utf8` non-
    /// reserved columns. Skips null cells (treats them as "label not
    /// present for this row" which matches the OTel codec's
    /// "absent attribute = no key" behavior).
    fn labels_for_row(&self, row: usize) -> Vec<KeyValue> {
        if self.label_columns.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(self.label_columns.len());
        for (key, arr) in &self.label_columns {
            if arr.is_null(row) {
                continue;
            }
            out.push(KeyValue::new(key.clone(), arr.value(row).to_string()));
        }
        out
    }
}

fn resolve_timestamp<'a>(
    name: &str,
    col: &'a Arc<dyn Array>,
) -> Result<TimestampColumn<'a>, OtapDecodeError> {
    if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(TimestampColumn::UInt64Nanos(arr));
    }
    if let Some(arr) = col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        return Ok(TimestampColumn::Timestamp(arr));
    }
    Err(OtapDecodeError::WrongColumnType {
        column: name.to_string(),
        expected: "UInt64 or Timestamp(Nanosecond)",
        actual: col.data_type().clone(),
    })
}

fn downcast_string<'a>(
    name: &str,
    col: &'a Arc<dyn Array>,
) -> Result<&'a StringArray, OtapDecodeError> {
    col.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "Utf8",
            actual: col.data_type().clone(),
        })
}

fn downcast_float64<'a>(
    name: &str,
    col: &'a Arc<dyn Array>,
) -> Result<&'a Float64Array, OtapDecodeError> {
    col.as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "Float64",
            actual: col.data_type().clone(),
        })
}

fn downcast_binary<'a>(
    name: &str,
    col: &'a Arc<dyn Array>,
) -> Result<&'a BinaryArray, OtapDecodeError> {
    col.as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "Binary",
            actual: col.data_type().clone(),
        })
}

fn downcast_u64<'a>(
    name: &str,
    col: &'a Arc<dyn Array>,
) -> Result<&'a UInt64Array, OtapDecodeError> {
    col.as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "UInt64",
            actual: col.data_type().clone(),
        })
}

fn downcast_u32<'a>(
    name: &str,
    col: &'a Arc<dyn Array>,
) -> Result<&'a UInt32Array, OtapDecodeError> {
    col.as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| OtapDecodeError::WrongColumnType {
            column: name.to_string(),
            expected: "UInt32",
            actual: col.data_type().clone(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::ObservationValueKind;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch_from(fields: Vec<Field>, columns: Vec<Arc<dyn Array>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).expect("build batch")
    }

    #[test]
    fn empty_batch_decodes_to_empty_vec() {
        let batch = batch_from(
            vec![Field::new(COLUMN_METRIC, DataType::Utf8, false)],
            vec![Arc::new(StringArray::from(Vec::<&str>::new()))],
        );
        assert!(decode_batch(&batch).expect("decode").is_empty());
    }

    #[test]
    fn decode_scalar_row() {
        let batch = batch_from(
            vec![
                Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Float64, false),
                Field::new("region", DataType::Utf8, true),
            ],
            vec![
                Arc::new(UInt64Array::from(vec![1_000_000_000_u64])), // 1000 ms
                Arc::new(StringArray::from(vec!["http_requests"])),
                Arc::new(Float64Array::from(vec![2.5])),
                Arc::new(StringArray::from(vec![Some("us-east")])),
            ],
        );
        let obs = decode_batch(&batch).expect("decode");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].timestamp_ms, 1000);
        assert_eq!(obs[0].metric, "http_requests");
        assert_eq!(obs[0].value.kind, ObservationValueKind::Float);
        assert_eq!(obs[0].value.float, 2.5);
        assert_eq!(obs[0].labels.len(), 1);
        assert_eq!(obs[0].labels[0].key, "region");
        assert_eq!(obs[0].labels[0].value, "us-east");
    }

    #[test]
    fn decode_envelope_row_routes_to_envelope_kind() {
        let envelope_payload = vec![1_u8, 2, 3, 4];
        let batch = batch_from(
            vec![
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(ATTR_ENVELOPE, DataType::Binary, true),
                Field::new(ATTR_SKETCH_TYPE, DataType::Utf8, true),
                Field::new(ATTR_AGG_ID, DataType::UInt64, true),
                Field::new(ATTR_SCHEMA_VERSION, DataType::UInt32, true),
                Field::new(ATTR_WINDOW_START_MS, DataType::UInt64, true),
                Field::new(ATTR_WINDOW_END_MS, DataType::UInt64, true),
                Field::new(ATTR_ENCODING, DataType::Utf8, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["http_request_duration"])),
                Arc::new(BinaryArray::from(vec![envelope_payload.as_slice()])),
                Arc::new(StringArray::from(vec![Some("DDSketch")])),
                Arc::new(UInt64Array::from(vec![Some(42_u64)])),
                Arc::new(UInt32Array::from(vec![Some(1_u32)])),
                Arc::new(UInt64Array::from(vec![Some(1_000_u64)])),
                Arc::new(UInt64Array::from(vec![Some(2_000_u64)])),
                Arc::new(StringArray::from(vec![Some("PROTO_FULL")])),
            ],
        );
        let obs = decode_batch(&batch).expect("decode");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].value.kind, ObservationValueKind::Envelope);
        let env = obs[0].value.envelope.as_ref().expect("envelope");
        assert_eq!(env.payload, envelope_payload);
        assert_eq!(env.sketch_type, SketchType::DDSketch);
        assert_eq!(env.agg_id, 42);
        assert_eq!(env.schema_version, 1);
        assert_eq!(env.window_start_ms, 1_000);
        assert_eq!(env.window_end_ms, 2_000);
        assert_eq!(env.encoding, Encoding::ProtoFull);
        assert_eq!(env.metric_name, "http_request_duration");
    }

    #[test]
    fn decode_rejects_wrong_column_type() {
        // `value` arrives as Int64 instead of Float64 — schema mismatch.
        let batch = batch_from(
            vec![
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Int64, false),
            ],
            vec![
                Arc::new(StringArray::from(vec!["m"])),
                Arc::new(Int64Array::from(vec![1_i64])),
            ],
        );
        let err = decode_batch(&batch).expect_err("should reject");
        match err {
            OtapDecodeError::WrongColumnType { column, .. } => assert_eq!(column, "value"),
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_unknown_sketch_type() {
        let batch = batch_from(
            vec![
                Field::new(ATTR_ENVELOPE, DataType::Binary, false),
                Field::new(ATTR_SKETCH_TYPE, DataType::Utf8, false),
            ],
            vec![
                Arc::new(BinaryArray::from(vec![b"x".as_ref()])),
                Arc::new(StringArray::from(vec!["BogoSketch"])),
            ],
        );
        let err = decode_batch(&batch).expect_err("should reject");
        match err {
            OtapDecodeError::UnknownSketchType { row, value } => {
                assert_eq!(row, 0);
                assert_eq!(value, "BogoSketch");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_unknown_encoding() {
        let batch = batch_from(
            vec![
                Field::new(ATTR_ENVELOPE, DataType::Binary, false),
                Field::new(ATTR_ENCODING, DataType::Utf8, false),
            ],
            vec![
                Arc::new(BinaryArray::from(vec![b"x".as_ref()])),
                Arc::new(StringArray::from(vec!["URL_SAFE_BASE64"])),
            ],
        );
        let err = decode_batch(&batch).expect_err("should reject");
        match err {
            OtapDecodeError::UnknownEncoding { row, value } => {
                assert_eq!(row, 0);
                assert_eq!(value, "URL_SAFE_BASE64");
            }
            other => panic!("unexpected variant: {other:?}"),
        }
    }

    #[test]
    fn null_envelope_cell_falls_back_to_scalar() {
        // A row with an `_asap_envelope` column that is null should
        // route through the scalar value path, not erroneously emit
        // an empty-payload envelope.
        let batch = batch_from(
            vec![
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Float64, false),
                Field::new(ATTR_ENVELOPE, DataType::Binary, true),
            ],
            vec![
                Arc::new(StringArray::from(vec!["m"])),
                Arc::new(Float64Array::from(vec![7.5])),
                Arc::new(BinaryArray::from(vec![None as Option<&[u8]>])),
            ],
        );
        let obs = decode_batch(&batch).expect("decode");
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].value.kind, ObservationValueKind::Float);
        assert_eq!(obs[0].value.float, 7.5);
    }
}
