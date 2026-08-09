//! Well-known column / attribute names for the OTAP-Rust codec.
//!
//! Strategy-B carrier keys are case-sensitive, prefix-included.
//! Every Strategy-B adapter
//! (Telegraf today, OTAP / Vector tomorrow) uses these exact
//! spellings — the codec MUST NOT change them.

// ---------- Well-known scalar / metric columns ----------

/// Timestamp column (nanoseconds since the Unix epoch). Either `UInt64`
/// or Arrow's typed `Timestamp(Nanosecond, _)` is accepted on decode;
/// encode emits `UInt64` for portability across consumers that don't
/// have a timezone-aware Arrow runtime.
pub const COLUMN_TIME_UNIX_NANO: &str = "time_unix_nano";

/// Metric-name column. `Utf8` on decode and encode.
pub const COLUMN_METRIC: &str = "metric";

/// Scalar value column. `Float64` on decode and encode.
pub const COLUMN_VALUE: &str = "value";

// ---------- Strategy-B per-row attribute keys ----------
//
// In the OTAP carrier these ride as
// `AttributeValueType::Bytes` / typed values on the per-row attribute
// child batch (NOT as sibling top-level columns on the metrics batch
// — OTAP's strict schema validator rejects extension columns). The
// Phase-B flat-RecordBatch shape carries them as plain columns; the
// Phase-C plugin shell projects/joins them back to the per-row
// attribute child batch when emitting an OtapArrowRecords downstream.

/// Strategy-B envelope payload key. `Binary` column. Presence routes
/// the row through [`crate::observation::ObservationValueKind::Envelope`].
pub const ATTR_ENVELOPE: &str = "_asap_envelope";

/// Strategy-B sketch-type tag. `Utf8` column carrying canonical names
/// (`"DDSketch"` / `"KLLSketch"` / `"HLLSketch"` / `"CountSketch"` /
/// `"CountMinSketch"`).
pub const ATTR_SKETCH_TYPE: &str = "_asap_sketch_type";

/// Strategy-B controller-plan join key. `UInt64` column.
pub const ATTR_AGG_ID: &str = "_asap_agg_id";

/// Strategy-B envelope schema version. `UInt32` column.
pub const ATTR_SCHEMA_VERSION: &str = "_asap_schema_version";

/// Strategy-B window inclusive lower bound (Unix milliseconds).
/// `UInt64` column.
pub const ATTR_WINDOW_START_MS: &str = "_asap_window_start_ms";

/// Strategy-B window exclusive upper bound (Unix milliseconds).
/// `UInt64` column.
pub const ATTR_WINDOW_END_MS: &str = "_asap_window_end_ms";

/// Strategy-B encoding tag. `Utf8` column carrying canonical names
/// (`"PROTO_FULL"` / `"PROTO_DELTA"` / `"MSGPACK"`).
pub const ATTR_ENCODING: &str = "_asap_encoding";

/// Returns true if `name` is one of the well-known scalar columns or
/// `_asap_*` carrier keys; everything else is treated as a per-row
/// label by [`crate::otap::decode_batch`].
pub fn is_reserved_column(name: &str) -> bool {
    matches!(
        name,
        COLUMN_TIME_UNIX_NANO
            | COLUMN_METRIC
            | COLUMN_VALUE
            | ATTR_ENVELOPE
            | ATTR_SKETCH_TYPE
            | ATTR_AGG_ID
            | ATTR_SCHEMA_VERSION
            | ATTR_WINDOW_START_MS
            | ATTR_WINDOW_END_MS
            | ATTR_ENCODING
    )
}

// ---------- Schema / Dictionary / Record stream columns ----------
//
// Column names for the four-batch family produced by
// [`super::dictionary::SeriesDictionary::encode`], mirroring the ER
// diagram in `docs/data_model.md#schema--dictionary--record-as-entities`:
// `SCHEMA` is keyed by `agg_id`, `DICTIONARY` (+ its child `LABELS`) is
// keyed by `series_id`, and `RECORD` references a `DICTIONARY` entry by
// `series_id` instead of repeating `metric`/labels inline. Unlike the
// `ATTR_*` carrier keys above (which exist to smuggle these facts
// through a single OTAP-Metrics-shaped row), these four batches are
// ASAP's own inter-node sketch-stream wire shape — see the doc's
// opening line: "information carried when sketch state crosses a node
// or network boundary between `asap_sketches` processor instances."

/// `SCHEMA.agg_id` / `DICTIONARY.agg_id` — controller-plan join key.
/// `UInt64` column.
pub const SCHEMA_COLUMN_AGG_ID: &str = "agg_id";

/// `SCHEMA.sketch_type`. `Utf8` column; same canonical names as
/// [`ATTR_SKETCH_TYPE`].
pub const SCHEMA_COLUMN_SKETCH_TYPE: &str = "sketch_type";

/// `SCHEMA.sketch_size` — the algorithm's size/accuracy parameter
/// (relative accuracy, buffer size `k`, precision, or `width x depth`
/// — whichever `sketch_type` calls for), rendered as a string so one
/// column covers every algorithm's parameter shape without a lossy
/// numeric union. `Utf8`, optional.
pub const SCHEMA_COLUMN_SKETCH_SIZE: &str = "sketch_size";

/// `SCHEMA.hash_seed` — determinism contract for hash-based sketches:
/// the single seed value at
/// `hash_spec.seed_list[hash_spec.canonical_seed_index]`, resolved
/// from [`crate::envelope::SketchEnvelope::hash_spec`] by
/// `super::dictionary::resolve_hash_seed`. `UInt64`, optional — null
/// when the envelope carries no `hash_spec` (nothing upstream
/// populates it yet) or the sketch type doesn't hash at all (DDSketch,
/// KLL). Deliberately just the one resolved seed, not
/// `asap_sketchlib`'s full 20-entry `seed_list` — see
/// `resolve_hash_seed`'s doc comment for why one `SCHEMA` row (one
/// `agg_id`, one `sketch_type`) only ever needs one seed position.
pub const SCHEMA_COLUMN_HASH_SEED: &str = "hash_seed";

/// `SCHEMA.hash_function` — which hash function, for algorithms that
/// need one (`hash_spec.algorithm`'s canonical proto name, e.g.
/// `"HASH_ALGORITHM_XXH3_64"`). `Utf8`, optional. Same
/// null-when-no-`hash_spec` caveat as [`SCHEMA_COLUMN_HASH_SEED`].
pub const SCHEMA_COLUMN_HASH_FUNCTION: &str = "hash_function";

/// `SCHEMA.encoding`. `Utf8` column; same canonical names as
/// [`ATTR_ENCODING`].
pub const SCHEMA_COLUMN_ENCODING: &str = "encoding";

/// `SCHEMA.schema_version`. `UInt32` column.
pub const SCHEMA_COLUMN_SCHEMA_VERSION: &str = "schema_version";

/// `DICTIONARY.series_id` / `LABELS.series_id` / `RECORD.series_id` —
/// primary key of a `DICTIONARY` entry, and the join key `RECORD` uses
/// instead of repeating `metric`/labels inline. `UInt32` column.
pub const DICT_COLUMN_SERIES_ID: &str = "series_id";

/// `DICTIONARY.metric` — metric name. `Utf8` column.
pub const DICT_COLUMN_METRIC: &str = "metric";

/// `LABELS.key` — label key. `Utf8` column.
pub const LABELS_COLUMN_KEY: &str = "key";

/// `LABELS.value` — label value. `Utf8` column, optional.
pub const LABELS_COLUMN_VALUE: &str = "value";

/// `RECORD.window_start_ms` — inclusive lower bound of the window this
/// record summarizes (Unix milliseconds). `UInt64` column.
pub const RECORD_COLUMN_WINDOW_START_MS: &str = "window_start_ms";

/// `RECORD.window_end_ms` — exclusive upper bound of the window this
/// record summarizes (Unix milliseconds). `UInt64` column.
pub const RECORD_COLUMN_WINDOW_END_MS: &str = "window_end_ms";

/// `RECORD.envelope` — serialized sketch state or delta. `Binary`
/// column, optional (mutually exclusive with [`RECORD_COLUMN_VALUE`]
/// per record — a `RECORD` carries sketch state or an estimate, never
/// both).
pub const RECORD_COLUMN_ENVELOPE: &str = "envelope";

/// `RECORD.value` — estimate-mode scalar (quantile or cardinality
/// estimate), carried instead of [`RECORD_COLUMN_ENVELOPE`] when the
/// series emits estimates rather than sketch state. `Float64` column,
/// optional.
pub const RECORD_COLUMN_VALUE: &str = "value";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_keys_match_edge_framework_doc() {
        // Cross-check the Strategy-B key spellings — these MUST be
        // byte-identical for cross-platform interop.
        assert_eq!(ATTR_ENVELOPE, "_asap_envelope");
        assert_eq!(ATTR_SKETCH_TYPE, "_asap_sketch_type");
        assert_eq!(ATTR_AGG_ID, "_asap_agg_id");
        assert_eq!(ATTR_SCHEMA_VERSION, "_asap_schema_version");
        assert_eq!(ATTR_WINDOW_START_MS, "_asap_window_start_ms");
        assert_eq!(ATTR_WINDOW_END_MS, "_asap_window_end_ms");
        assert_eq!(ATTR_ENCODING, "_asap_encoding");
    }

    #[test]
    fn reserved_names_recognized() {
        for k in [
            COLUMN_TIME_UNIX_NANO,
            COLUMN_METRIC,
            COLUMN_VALUE,
            ATTR_ENVELOPE,
            ATTR_SKETCH_TYPE,
            ATTR_AGG_ID,
            ATTR_SCHEMA_VERSION,
            ATTR_WINDOW_START_MS,
            ATTR_WINDOW_END_MS,
            ATTR_ENCODING,
        ] {
            assert!(is_reserved_column(k), "{} should be reserved", k);
        }
        assert!(!is_reserved_column("region"));
        assert!(!is_reserved_column("path"));
        assert!(!is_reserved_column(""));
    }
}
