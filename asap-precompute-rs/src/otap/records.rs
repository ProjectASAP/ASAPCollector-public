//! `OtapArrowRecords` family modelling — sibling-batch projection
//! and Strategy-B attribute-lift.
//!
//! The `OtapArrowRecords` family → flat `RecordBatch` projection
//! and Strategy-B attribute-lift on emit are deliverables that Phase B
//! deferred. This module supplies both — without depending on the
//! upstream OTAP submodule (which only mounts at Phase D's build
//! time, per Phase C's scope boundary).
//!
//! # Why a local model
//!
//! The upstream OTAP `OtapArrowRecords` Rust type lives in the
//! `otap-dataflow` workspace (the submodule the Phase D build script
//! patches). At Phase C the plugin lifecycle code is exercised via
//! the `asap-precompute-rs` test harness, **not** an OTAP runtime.
//! The plugin's contract with OTAP is:
//!
//! 1. **On receive:** OTAP hands us an `OtapArrowRecords` (sibling
//!    `RecordBatch`es joined by integer ids — resource / scope /
//!    metric / per-row attribute). We project that down to a single
//!    flat `RecordBatch` whose schema [`super::decode_batch`] knows
//!    how to walk: well-known scalar columns plus Strategy-B
//!    `_asap_*` columns lifted out of the per-row attribute child
//!    batch.
//! 2. **On emit:** [`super::encode_batch`] returns a flat
//!    `RecordBatch` with `_asap_*` carrier columns. OTAP's strict
//!    schema validator (`crates/pdata/src/schema/payloads.rs::check_match`)
//!    rejects extension top-level columns on Logs/Metrics/Traces
//!    record batches.
//!    Phase C must lift the `_asap_*` columns onto the per-row
//!    attribute child batch (`AttributeValueType::Bytes` keyed
//!    entries) before passing the batch downstream.
//!
//! Both directions of this transform are **structural**, not
//! algorithmic: the per-row attribute batch is the canonical OTAP
//! Arrow attribute encoding (one row per `(parent_id, key, value)`).
//! Any binding to a real `OtapArrowRecords` Rust type at Phase D
//! becomes a thin wrapper over [`OtapMetricRecords`] below.
//!
//! # Schema
//!
//! Two batches make up the family:
//!
//! - **`metrics`** — well-known scalar columns plus a `parent_id`
//!   column. Each row corresponds to one observation; `parent_id`
//!   joins the row to its attribute set.
//! - **`attributes`** — three columns: `parent_id` (`UInt32`),
//!   `key` (`Utf8`), and one of `bytes` (`Binary`) / `str`
//!   (`Utf8`) / `int` (`UInt64`) typed value columns. Multiple rows
//!   per `parent_id` represent a multi-attribute set on one parent.
//!   This mirrors the upstream OTAP attribute-child-batch shape.
//!
//! Resource and scope child batches are deliberately omitted — Phase C's
//! scope boundary calls out cross-host parity (Phase E) as the place
//! that exercises the full resource scope. The `Observation::resource_labels`
//! field on the runtime side carries the resource attrs once the
//! upstream OTAP wrapper joins them on; here we model only the
//! per-row attribute batch which is the carrier for `_asap_*` keys.
//!
//! # Out of scope (Phase D)
//!
//! - Binding to the upstream `OtapArrowRecords` type itself.
//! - The `linkme` distributed-slice plugin registration.
//! - `effect_handler.send_message` / OTAP's submission API.
//!
//! Phase D wires this projection into the upstream `OtapPdata` shape;
//! the per-row attribute batch's schema we emit here is the same one
//! `crates/pdata/src/schema/payloads.rs::check_match` validates.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use thiserror::Error;

use super::schema::{
    is_reserved_column, ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION,
    ATTR_SKETCH_TYPE, ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS,
};

/// Local model of the upstream OTAP `OtapArrowRecords` type for
/// metrics — a sibling-batch family carrying one metrics batch and
/// one per-row attribute batch joined by integer parent ids.
///
/// Each record in `metrics` corresponds to a single observation. Its
/// `parent_id` (`UInt32`) joins to zero or more rows in `attributes`,
/// each carrying one key / typed-value pair.
///
/// The upstream type has additional sibling batches (resource scope,
/// scope, exemplars, …) — this minimal struct captures the two that
/// Phase C cares about. Phase D is the layer that maps to / from the
/// upstream type once the OTAP submodule is wired in.
#[derive(Debug, Clone)]
pub struct OtapMetricRecords {
    /// Metrics RecordBatch: well-known scalar columns plus
    /// `parent_id` (`UInt32`).
    pub metrics: RecordBatch,
    /// Per-row attribute child RecordBatch: `parent_id` (`UInt32`),
    /// `key` (`Utf8`), and at least one typed-value column from the
    /// canonical OTAP set: `bytes` (`Binary`), `str` (`Utf8`), or
    /// `int` (`UInt64`). Rows are not required to be sorted by
    /// `parent_id`; the projection joins by id-equality.
    pub attributes: RecordBatch,
}

/// Failure modes for [`flatten`] / [`lift`].
#[derive(Debug, Error)]
pub enum OtapRecordsError {
    /// Either of the input batches is missing a required column.
    #[error("otap records: batch {batch:?} missing required column {column:?}")]
    MissingColumn {
        /// Which sibling batch is missing the column.
        batch: &'static str,
        /// Column name.
        column: &'static str,
    },
    /// Required column has the wrong Arrow `DataType`.
    #[error(
        "otap records: column {column:?} on batch {batch:?} has wrong type: expected {expected}, got {actual:?}"
    )]
    WrongColumnType {
        /// Sibling batch.
        batch: &'static str,
        /// Column name.
        column: &'static str,
        /// Expected Arrow type, in human-readable form.
        expected: &'static str,
        /// Actual Arrow type observed.
        actual: DataType,
    },
    /// Constructing one of the output batches failed. Indicates a
    /// codec bug rather than caller error — kept as an error variant
    /// so the plugin shell surfaces it via OTAP's error channel
    /// rather than crashing the host process.
    #[error("otap records: arrow record-batch construction failed: {0}")]
    ArrowError(String),
}

/// Well-known column on the per-row attribute child batch (parent
/// join key).
pub const ATTR_BATCH_PARENT_ID: &str = "parent_id";

/// Well-known column on the per-row attribute child batch (key).
pub const ATTR_BATCH_KEY: &str = "key";

/// Well-known typed-value column on the per-row attribute child
/// batch carrying `Binary` values.
pub const ATTR_BATCH_BYTES: &str = "bytes";

/// Well-known typed-value column on the per-row attribute child
/// batch carrying `Utf8` values.
pub const ATTR_BATCH_STR: &str = "str";

/// Well-known typed-value column on the per-row attribute child
/// batch carrying `UInt64` values (also covers `UInt32`-shaped
/// schema-version values when up-cast).
pub const ATTR_BATCH_INT: &str = "int";

/// Project an [`OtapMetricRecords`] family down to a flat
/// `RecordBatch` whose schema matches what [`super::decode_batch`]
/// expects: well-known scalar columns (`time_unix_nano`, `metric`,
/// `value`) plus Strategy-B `_asap_*` columns lifted from the
/// per-row attribute child batch, plus any other `Utf8`-typed
/// attribute keys promoted to per-row label columns.
///
/// Resource attrs are not yet joined here — Phase C's runtime
/// `Observation::resource_labels` is sourced separately by the
/// upstream OTAP wrapper; see the docstring on [`OtapMetricRecords`].
///
/// Returns a flat `RecordBatch` ready to feed into
/// [`super::decode_batch`]. The schema is the union of:
///
/// - Every column already on `records.metrics` **except**
///   `parent_id` (consumed by the join and dropped from the flat
///   shape).
/// - Every Strategy-B `_asap_*` attribute lifted to a top-level
///   typed column (`_asap_envelope` Binary, `_asap_sketch_type` Utf8,
///   `_asap_agg_id` UInt64, `_asap_schema_version` UInt32,
///   `_asap_window_start_ms` UInt64, `_asap_window_end_ms` UInt64,
///   `_asap_encoding` Utf8).
/// - Every other `str`-valued attribute promoted to a `Utf8` label
///   column named after the attribute key.
///
/// Where a row in `records.metrics` has no matching attribute row
/// the corresponding cell is null.
pub fn flatten(records: &OtapMetricRecords) -> Result<RecordBatch, OtapRecordsError> {
    let n_rows = records.metrics.num_rows();
    let parent_ids = require_uint32(&records.metrics, "metrics", ATTR_BATCH_PARENT_ID)?.clone();

    // Index attribute rows by parent_id → vec<(key, typed value)>.
    let attr_index = build_attr_index(&records.attributes)?;

    // Walk all attribute keys to discover the union of label keys
    // (every `str` value whose key is non-reserved) so we know the
    // schema of the flat batch up front.
    let mut label_keys: BTreeMap<String, ()> = BTreeMap::new();
    for entries in attr_index.values() {
        for (key, val) in entries {
            if !is_reserved_column(key) {
                if let AttrValue::Str(_) = val {
                    label_keys.insert(key.clone(), ());
                }
            }
        }
    }
    let label_keys: Vec<String> = label_keys.into_keys().collect();

    // Build per-row Strategy-B carriers + label columns in one pass.
    let mut envelope_col: Vec<Option<Vec<u8>>> = Vec::with_capacity(n_rows);
    let mut sketch_type_col: Vec<Option<String>> = Vec::with_capacity(n_rows);
    let mut agg_id_col: Vec<Option<u64>> = Vec::with_capacity(n_rows);
    let mut schema_version_col: Vec<Option<u32>> = Vec::with_capacity(n_rows);
    let mut window_start_col: Vec<Option<u64>> = Vec::with_capacity(n_rows);
    let mut window_end_col: Vec<Option<u64>> = Vec::with_capacity(n_rows);
    let mut encoding_col: Vec<Option<String>> = Vec::with_capacity(n_rows);
    let mut label_cols: Vec<Vec<Option<String>>> = (0..label_keys.len())
        .map(|_| Vec::with_capacity(n_rows))
        .collect();

    for row in 0..n_rows {
        if parent_ids.is_null(row) {
            envelope_col.push(None);
            sketch_type_col.push(None);
            agg_id_col.push(None);
            schema_version_col.push(None);
            window_start_col.push(None);
            window_end_col.push(None);
            encoding_col.push(None);
            for col in &mut label_cols {
                col.push(None);
            }
            continue;
        }
        let pid = parent_ids.value(row);
        let entries = attr_index.get(&pid);
        let mut envelope: Option<Vec<u8>> = None;
        let mut sketch_type: Option<String> = None;
        let mut agg_id: Option<u64> = None;
        let mut schema_version: Option<u32> = None;
        let mut window_start: Option<u64> = None;
        let mut window_end: Option<u64> = None;
        let mut encoding: Option<String> = None;
        let mut row_labels: Vec<Option<String>> = vec![None; label_keys.len()];

        if let Some(entries) = entries {
            for (key, val) in entries {
                match (key.as_str(), val) {
                    (ATTR_ENVELOPE, AttrValue::Bytes(b)) => envelope = Some(b.clone()),
                    (ATTR_SKETCH_TYPE, AttrValue::Str(s)) => sketch_type = Some(s.clone()),
                    (ATTR_AGG_ID, AttrValue::Int(v)) => agg_id = Some(*v),
                    (ATTR_SCHEMA_VERSION, AttrValue::Int(v)) => {
                        schema_version = Some(*v as u32);
                    }
                    (ATTR_WINDOW_START_MS, AttrValue::Int(v)) => window_start = Some(*v),
                    (ATTR_WINDOW_END_MS, AttrValue::Int(v)) => window_end = Some(*v),
                    (ATTR_ENCODING, AttrValue::Str(s)) => encoding = Some(s.clone()),
                    (k, AttrValue::Str(s)) if !is_reserved_column(k) => {
                        if let Some(idx) = label_keys.iter().position(|x| x == k) {
                            row_labels[idx] = Some(s.clone());
                        }
                    }
                    _ => {
                        // Other attribute shapes (e.g. resource attrs
                        // typed as Int) are not yet mapped to label
                        // columns; Phase E (cross-host parity) is
                        // where the full resource-attr coverage lands.
                    }
                }
            }
        }
        envelope_col.push(envelope);
        sketch_type_col.push(sketch_type);
        agg_id_col.push(agg_id);
        schema_version_col.push(schema_version);
        window_start_col.push(window_start);
        window_end_col.push(window_end);
        encoding_col.push(encoding);
        for (idx, val) in row_labels.into_iter().enumerate() {
            label_cols[idx].push(val);
        }
    }

    // Carry forward every non-`parent_id`, non-Strategy-B column
    // already on `records.metrics`. Phase B's `decode_batch` walks
    // them by name, so keep their original schema field intact.
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for (i, field) in records.metrics.schema().fields().iter().enumerate() {
        if field.name() == ATTR_BATCH_PARENT_ID {
            continue;
        }
        // Skip any Strategy-B columns that may already be on the
        // metrics batch — the attribute-lift path on emit puts them
        // on the attribute batch, but a producer might double-encode
        // them. Phase C prefers attribute values when they exist.
        if is_strategy_b_attr(field.name()) {
            continue;
        }
        fields.push(field.as_ref().clone());
        columns.push(records.metrics.column(i).clone());
    }
    fields.push(Field::new(ATTR_ENVELOPE, DataType::Binary, true));
    columns.push(Arc::new(BinaryArray::from_opt_vec(
        envelope_col.iter().map(|o| o.as_deref()).collect(),
    )));
    fields.push(Field::new(ATTR_SKETCH_TYPE, DataType::Utf8, true));
    columns.push(Arc::new(StringArray::from(sketch_type_col)));
    fields.push(Field::new(ATTR_AGG_ID, DataType::UInt64, true));
    columns.push(Arc::new(UInt64Array::from(agg_id_col)));
    fields.push(Field::new(ATTR_SCHEMA_VERSION, DataType::UInt32, true));
    columns.push(Arc::new(UInt32Array::from(schema_version_col)));
    fields.push(Field::new(ATTR_WINDOW_START_MS, DataType::UInt64, true));
    columns.push(Arc::new(UInt64Array::from(window_start_col)));
    fields.push(Field::new(ATTR_WINDOW_END_MS, DataType::UInt64, true));
    columns.push(Arc::new(UInt64Array::from(window_end_col)));
    fields.push(Field::new(ATTR_ENCODING, DataType::Utf8, true));
    columns.push(Arc::new(StringArray::from(encoding_col)));
    for (key, col) in label_keys.iter().zip(label_cols) {
        fields.push(Field::new(key, DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(col)));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(|e| OtapRecordsError::ArrowError(e.to_string()))
}

/// Lift Strategy-B `_asap_*` carrier columns from a flat
/// `RecordBatch` onto the per-row attribute child batch, returning
/// an [`OtapMetricRecords`] family ready for OTAP downstream emit.
///
/// This is the inverse of [`flatten`]: where [`flatten`] lowers
/// attribute rows into top-level columns for the codec to consume,
/// `lift` raises top-level Strategy-B columns into attribute rows so
/// the resulting `RecordBatch` family passes OTAP's schema validator
/// (`crates/pdata/src/schema/payloads.rs::check_match` rejects any
/// extension column on Logs/Metrics/Traces RecordBatches).
///
/// Behaviour:
///
/// - Strategy-B `_asap_*` columns are removed from the metrics
///   batch and re-emitted as one `(parent_id, key, value)` row per
///   non-null cell on the attribute batch (`bytes` for
///   `_asap_envelope`; `str` for `_asap_sketch_type` / `_asap_encoding`;
///   `int` for the numeric carriers).
/// - A monotonic `parent_id` (`UInt32`, `[0, num_rows)`) is added
///   to the metrics batch so each row joins back to its attributes.
/// - Other top-level `Utf8` columns (label columns from
///   [`super::encode_batch`]'s union) are also lifted to attribute
///   rows with `str` typed values, since OTAP's metrics schema
///   validator rejects them as extension columns too. Reserved
///   well-known scalar columns (`time_unix_nano`, `metric`, `value`)
///   stay on the metrics batch.
pub fn lift(flat: &RecordBatch) -> Result<OtapMetricRecords, OtapRecordsError> {
    let n_rows = flat.num_rows();
    let parent_ids: Vec<u32> = (0..n_rows as u32).collect();

    // Resolve the Strategy-B carrier columns up front so per-row
    // walking is index-only.
    let envelope = match flat.column_by_name(ATTR_ENVELOPE) {
        Some(c) => Some(downcast_binary(c, "metrics", ATTR_ENVELOPE)?.clone()),
        None => None,
    };
    let sketch_type = optional_string(flat, ATTR_SKETCH_TYPE)?;
    let agg_id = optional_u64(flat, ATTR_AGG_ID)?;
    let schema_version = optional_u32(flat, ATTR_SCHEMA_VERSION)?;
    let window_start = optional_u64(flat, ATTR_WINDOW_START_MS)?;
    let window_end = optional_u64(flat, ATTR_WINDOW_END_MS)?;
    let encoding = optional_string(flat, ATTR_ENCODING)?;

    // Collect non-reserved Utf8 columns as label-attribute lifts.
    let label_arrays: Vec<(String, StringArray)> = flat
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            let name = f.name();
            if is_reserved_column(name) {
                return None;
            }
            if !matches!(f.data_type(), DataType::Utf8) {
                return None;
            }
            let arr = flat
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()?
                .clone();
            Some((name.clone(), arr))
        })
        .collect();

    // Build attribute rows, one per non-null `_asap_*` cell + label.
    let mut attr_parent: Vec<u32> = Vec::new();
    let mut attr_key: Vec<String> = Vec::new();
    let mut attr_bytes: Vec<Option<Vec<u8>>> = Vec::new();
    let mut attr_str: Vec<Option<String>> = Vec::new();
    let mut attr_int: Vec<Option<u64>> = Vec::new();

    for (row, &pid) in parent_ids.iter().enumerate().take(n_rows) {
        if let Some(arr) = &envelope {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_ENVELOPE.to_string());
                attr_bytes.push(Some(arr.value(row).to_vec()));
                attr_str.push(None);
                attr_int.push(None);
            }
        }
        if let Some(arr) = &sketch_type {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_SKETCH_TYPE.to_string());
                attr_bytes.push(None);
                attr_str.push(Some(arr.value(row).to_string()));
                attr_int.push(None);
            }
        }
        if let Some(arr) = &agg_id {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_AGG_ID.to_string());
                attr_bytes.push(None);
                attr_str.push(None);
                attr_int.push(Some(arr.value(row)));
            }
        }
        if let Some(arr) = &schema_version {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_SCHEMA_VERSION.to_string());
                attr_bytes.push(None);
                attr_str.push(None);
                attr_int.push(Some(arr.value(row) as u64));
            }
        }
        if let Some(arr) = &window_start {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_WINDOW_START_MS.to_string());
                attr_bytes.push(None);
                attr_str.push(None);
                attr_int.push(Some(arr.value(row)));
            }
        }
        if let Some(arr) = &window_end {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_WINDOW_END_MS.to_string());
                attr_bytes.push(None);
                attr_str.push(None);
                attr_int.push(Some(arr.value(row)));
            }
        }
        if let Some(arr) = &encoding {
            if !arr.is_null(row) {
                attr_parent.push(pid);
                attr_key.push(ATTR_ENCODING.to_string());
                attr_bytes.push(None);
                attr_str.push(Some(arr.value(row).to_string()));
                attr_int.push(None);
            }
        }
        for (key, arr) in &label_arrays {
            if arr.is_null(row) {
                continue;
            }
            attr_parent.push(pid);
            attr_key.push(key.clone());
            attr_bytes.push(None);
            attr_str.push(Some(arr.value(row).to_string()));
            attr_int.push(None);
        }
    }

    let attributes_schema = Arc::new(Schema::new(vec![
        Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
        Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
        Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
        Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
        Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
    ]));
    let attributes = RecordBatch::try_new(
        attributes_schema,
        vec![
            Arc::new(UInt32Array::from(attr_parent)),
            Arc::new(StringArray::from(attr_key)),
            Arc::new(BinaryArray::from_opt_vec(
                attr_bytes.iter().map(|o| o.as_deref()).collect(),
            )),
            Arc::new(StringArray::from(attr_str)),
            Arc::new(UInt64Array::from(attr_int)),
        ],
    )
    .map_err(|e| OtapRecordsError::ArrowError(e.to_string()))?;

    // Build the metrics-side batch: scalar columns + parent_id, no
    // Strategy-B columns, no lifted label columns.
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();
    for (i, field) in flat.schema().fields().iter().enumerate() {
        let name = field.name();
        if is_strategy_b_attr(name) {
            continue;
        }
        if !is_reserved_column(name)
            && matches!(field.data_type(), DataType::Utf8)
            && label_arrays.iter().any(|(k, _)| k == name)
        {
            // Label columns lifted to attributes — drop from metrics.
            continue;
        }
        fields.push(field.as_ref().clone());
        columns.push(flat.column(i).clone());
    }
    fields.push(Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false));
    columns.push(Arc::new(UInt32Array::from(parent_ids)));
    let metrics_schema = Arc::new(Schema::new(fields));
    let metrics = RecordBatch::try_new(metrics_schema, columns)
        .map_err(|e| OtapRecordsError::ArrowError(e.to_string()))?;

    Ok(OtapMetricRecords {
        metrics,
        attributes,
    })
}

/// Returns whether `name` is a Strategy-B carrier key (i.e. a
/// `_asap_*` column the lift step removes from the top-level
/// metrics batch).
fn is_strategy_b_attr(name: &str) -> bool {
    matches!(
        name,
        ATTR_ENVELOPE
            | ATTR_SKETCH_TYPE
            | ATTR_AGG_ID
            | ATTR_SCHEMA_VERSION
            | ATTR_WINDOW_START_MS
            | ATTR_WINDOW_END_MS
            | ATTR_ENCODING
    )
}

/// Internal typed-value carrier for a single attribute cell.
#[derive(Debug, Clone)]
enum AttrValue {
    Bytes(Vec<u8>),
    Str(String),
    Int(u64),
}

fn build_attr_index(
    attributes: &RecordBatch,
) -> Result<BTreeMap<u32, Vec<(String, AttrValue)>>, OtapRecordsError> {
    let parent_ids = require_uint32(attributes, "attributes", ATTR_BATCH_PARENT_ID)?;
    let keys = require_string(attributes, "attributes", ATTR_BATCH_KEY)?;
    let bytes = optional_binary(attributes, ATTR_BATCH_BYTES)?;
    let strs = optional_string(attributes, ATTR_BATCH_STR)?;
    let ints = optional_u64(attributes, ATTR_BATCH_INT)?;

    let mut out: BTreeMap<u32, Vec<(String, AttrValue)>> = BTreeMap::new();
    for row in 0..attributes.num_rows() {
        if parent_ids.is_null(row) || keys.is_null(row) {
            continue;
        }
        let pid = parent_ids.value(row);
        let key = keys.value(row).to_string();
        let val = if let Some(arr) = &bytes {
            if !arr.is_null(row) {
                AttrValue::Bytes(arr.value(row).to_vec())
            } else if let Some(arr) = &strs {
                if !arr.is_null(row) {
                    AttrValue::Str(arr.value(row).to_string())
                } else if let Some(arr) = &ints {
                    if !arr.is_null(row) {
                        AttrValue::Int(arr.value(row))
                    } else {
                        continue;
                    }
                } else {
                    continue;
                }
            } else if let Some(arr) = &ints {
                if !arr.is_null(row) {
                    AttrValue::Int(arr.value(row))
                } else {
                    continue;
                }
            } else {
                continue;
            }
        } else if let Some(arr) = &strs {
            if !arr.is_null(row) {
                AttrValue::Str(arr.value(row).to_string())
            } else if let Some(arr) = &ints {
                if !arr.is_null(row) {
                    AttrValue::Int(arr.value(row))
                } else {
                    continue;
                }
            } else {
                continue;
            }
        } else if let Some(arr) = &ints {
            if !arr.is_null(row) {
                AttrValue::Int(arr.value(row))
            } else {
                continue;
            }
        } else {
            continue;
        };
        out.entry(pid).or_default().push((key, val));
    }
    Ok(out)
}

fn require_uint32<'a>(
    batch: &'a RecordBatch,
    label: &'static str,
    column: &'static str,
) -> Result<&'a UInt32Array, OtapRecordsError> {
    let col = batch
        .column_by_name(column)
        .ok_or(OtapRecordsError::MissingColumn {
            batch: label,
            column,
        })?;
    col.as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| OtapRecordsError::WrongColumnType {
            batch: label,
            column,
            expected: "UInt32",
            actual: col.data_type().clone(),
        })
}

fn require_string<'a>(
    batch: &'a RecordBatch,
    label: &'static str,
    column: &'static str,
) -> Result<&'a StringArray, OtapRecordsError> {
    let col = batch
        .column_by_name(column)
        .ok_or(OtapRecordsError::MissingColumn {
            batch: label,
            column,
        })?;
    col.as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OtapRecordsError::WrongColumnType {
            batch: label,
            column,
            expected: "Utf8",
            actual: col.data_type().clone(),
        })
}

fn optional_binary<'a>(
    batch: &'a RecordBatch,
    column: &'static str,
) -> Result<Option<&'a BinaryArray>, OtapRecordsError> {
    Ok(match batch.column_by_name(column) {
        Some(col) => Some(downcast_binary(col, "attributes", column)?),
        None => None,
    })
}

fn optional_string(
    batch: &RecordBatch,
    column: &'static str,
) -> Result<Option<StringArray>, OtapRecordsError> {
    Ok(match batch.column_by_name(column) {
        Some(col) => Some(
            col.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OtapRecordsError::WrongColumnType {
                    batch: "attributes",
                    column,
                    expected: "Utf8",
                    actual: col.data_type().clone(),
                })?
                .clone(),
        ),
        None => None,
    })
}

fn optional_u64(
    batch: &RecordBatch,
    column: &'static str,
) -> Result<Option<UInt64Array>, OtapRecordsError> {
    Ok(match batch.column_by_name(column) {
        Some(col) => Some(
            col.as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| OtapRecordsError::WrongColumnType {
                    batch: "attributes",
                    column,
                    expected: "UInt64",
                    actual: col.data_type().clone(),
                })?
                .clone(),
        ),
        None => None,
    })
}

fn optional_u32(
    batch: &RecordBatch,
    column: &'static str,
) -> Result<Option<UInt32Array>, OtapRecordsError> {
    Ok(match batch.column_by_name(column) {
        Some(col) => Some(
            col.as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| OtapRecordsError::WrongColumnType {
                    batch: "attributes",
                    column,
                    expected: "UInt32",
                    actual: col.data_type().clone(),
                })?
                .clone(),
        ),
        None => None,
    })
}

fn downcast_binary<'a>(
    col: &'a Arc<dyn Array>,
    label: &'static str,
    column: &'static str,
) -> Result<&'a BinaryArray, OtapRecordsError> {
    col.as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| OtapRecordsError::WrongColumnType {
            batch: label,
            column,
            expected: "Binary",
            actual: col.data_type().clone(),
        })
}

#[cfg(test)]
mod tests {
    use super::super::schema::{COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE};
    use super::*;
    use arrow_array::Float64Array;
    use arrow_schema::{Field, Schema};

    /// Build a minimal `OtapMetricRecords` family carrying one
    /// envelope-bearing row with all Strategy-B carriers set on the
    /// attribute batch.
    fn fixture_records() -> OtapMetricRecords {
        let metrics_schema = Arc::new(Schema::new(vec![
            Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
            Field::new(COLUMN_METRIC, DataType::Utf8, false),
            Field::new(COLUMN_VALUE, DataType::Float64, true),
            Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
        ]));
        let metrics = RecordBatch::try_new(
            metrics_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1_000_000_u64])), // 1ms
                Arc::new(StringArray::from(vec!["http_latency"])),
                Arc::new(Float64Array::from(vec![Some(2.5)])),
                Arc::new(UInt32Array::from(vec![0_u32])),
            ],
        )
        .expect("build metrics");

        let attributes_schema = Arc::new(Schema::new(vec![
            Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
            Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
            Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
            Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
        ]));
        let attr_parent: Vec<u32> = vec![0, 0, 0, 0, 0, 0, 0, 0];
        let attr_key: Vec<&str> = vec![
            ATTR_ENVELOPE,
            ATTR_SKETCH_TYPE,
            ATTR_AGG_ID,
            ATTR_SCHEMA_VERSION,
            ATTR_WINDOW_START_MS,
            ATTR_WINDOW_END_MS,
            ATTR_ENCODING,
            "region", // arbitrary label attribute
        ];
        let payload: &[u8] = &[0xde, 0xad, 0xbe, 0xef];
        let attr_bytes: Vec<Option<&[u8]>> =
            vec![Some(payload), None, None, None, None, None, None, None];
        let attr_str: Vec<Option<&str>> = vec![
            None,
            Some("DDSketch"),
            None,
            None,
            None,
            None,
            Some("PROTO_FULL"),
            Some("us-east"),
        ];
        let attr_int: Vec<Option<u64>> = vec![
            None,
            None,
            Some(42),
            Some(1),
            Some(1_000),
            Some(2_000),
            None,
            None,
        ];
        let attributes = RecordBatch::try_new(
            attributes_schema,
            vec![
                Arc::new(UInt32Array::from(attr_parent)),
                Arc::new(StringArray::from(attr_key)),
                Arc::new(BinaryArray::from_opt_vec(attr_bytes)),
                Arc::new(StringArray::from(attr_str)),
                Arc::new(UInt64Array::from(attr_int)),
            ],
        )
        .expect("build attributes");
        OtapMetricRecords {
            metrics,
            attributes,
        }
    }

    #[test]
    fn flatten_lifts_strategy_b_attrs_to_top_level_columns() {
        let records = fixture_records();
        let flat = flatten(&records).expect("flatten");
        assert_eq!(flat.num_rows(), 1);

        // Strategy-B columns were promoted from attributes to top-level.
        let env = flat
            .column_by_name(ATTR_ENVELOPE)
            .expect("envelope column")
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Binary");
        assert_eq!(env.value(0), &[0xde, 0xad, 0xbe, 0xef]);

        let sk = flat
            .column_by_name(ATTR_SKETCH_TYPE)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sk.value(0), "DDSketch");

        let agg = flat
            .column_by_name(ATTR_AGG_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(agg.value(0), 42);

        let region = flat
            .column_by_name("region")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(region.value(0), "us-east");

        // parent_id was consumed by the join.
        assert!(flat.column_by_name(ATTR_BATCH_PARENT_ID).is_none());
    }

    #[test]
    fn flatten_then_decode_round_trips_envelope_payload() {
        let records = fixture_records();
        let flat = flatten(&records).expect("flatten");
        let observations = super::super::decode_batch(&flat).expect("decode");
        assert_eq!(observations.len(), 1);
        let env = observations[0]
            .value
            .envelope
            .as_ref()
            .expect("envelope routed through KindEnvelope");
        assert_eq!(env.payload, vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(env.agg_id, 42);
        assert_eq!(env.sketch_type, crate::envelope::SketchType::DDSketch);
    }

    #[test]
    fn lift_then_flatten_round_trips() {
        // Start with what `encode_batch` would produce (a flat batch
        // with `_asap_*` top-level columns).
        let env = crate::envelope::SketchEnvelope {
            schema_version: 1,
            sketch_type: crate::envelope::SketchType::DDSketch,
            agg_id: 7,
            resource_labels: vec![],
            labels: vec![crate::observation::KeyValue::new("path", "/api")],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: crate::envelope::Encoding::ProtoFull,
            payload: vec![1, 2, 3, 4],
            hash_spec: None,
            metric_name: "m".into(),
            count: 0,
            aggregation_temporality: 0,
        };
        let flat = super::super::encode_batch(std::slice::from_ref(&env)).expect("encode");
        let lifted = lift(&flat).expect("lift");

        // Lift removes Strategy-B columns from the metrics batch; the
        // top-level metrics schema must NOT carry `_asap_*` columns
        // (this is what passes OTAP's strict validator).
        for name in [
            ATTR_ENVELOPE,
            ATTR_SKETCH_TYPE,
            ATTR_AGG_ID,
            ATTR_SCHEMA_VERSION,
            ATTR_WINDOW_START_MS,
            ATTR_WINDOW_END_MS,
            ATTR_ENCODING,
            "path", // also lifted
        ] {
            assert!(
                lifted.metrics.column_by_name(name).is_none(),
                "metrics batch must not carry top-level {name}"
            );
        }
        // parent_id is on the metrics side.
        assert!(lifted
            .metrics
            .column_by_name(ATTR_BATCH_PARENT_ID)
            .is_some());

        // Round-trip the lifted family back through flatten and
        // confirm the envelope payload survives intact.
        let re_flat = flatten(&lifted).expect("flatten");
        let observations = super::super::decode_batch(&re_flat).expect("decode");
        assert_eq!(observations.len(), 1);
        let decoded = observations[0]
            .value
            .envelope
            .as_ref()
            .expect("envelope kind preserved");
        assert_eq!(decoded.payload, env.payload);
        assert_eq!(decoded.sketch_type, env.sketch_type);
        assert_eq!(decoded.agg_id, env.agg_id);
        // Labels survive via the `path` attribute lift.
        assert_eq!(decoded.labels, env.labels);
    }

    #[test]
    fn flatten_preserves_scalar_columns_when_no_envelope_attr() {
        // Build a metrics batch with only scalar columns + parent_id;
        // attributes batch is empty. Decode should observe a Float row.
        let metrics_schema = Arc::new(Schema::new(vec![
            Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
            Field::new(COLUMN_METRIC, DataType::Utf8, false),
            Field::new(COLUMN_VALUE, DataType::Float64, false),
            Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
        ]));
        let metrics = RecordBatch::try_new(
            metrics_schema,
            vec![
                Arc::new(UInt64Array::from(vec![1_000_000_u64])),
                Arc::new(StringArray::from(vec!["m"])),
                Arc::new(Float64Array::from(vec![2.71_f64])),
                Arc::new(UInt32Array::from(vec![0_u32])),
            ],
        )
        .expect("metrics");
        let attributes_schema = Arc::new(Schema::new(vec![
            Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
            Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
            Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
            Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
        ]));
        let attributes = RecordBatch::new_empty(attributes_schema);
        let records = OtapMetricRecords {
            metrics,
            attributes,
        };

        let flat = flatten(&records).expect("flatten");
        let obs = super::super::decode_batch(&flat).expect("decode");
        assert_eq!(obs.len(), 1);
        assert_eq!(
            obs[0].value.kind,
            crate::observation::ObservationValueKind::Float
        );
        assert_eq!(obs[0].value.float, 2.71_f64);
    }
}
