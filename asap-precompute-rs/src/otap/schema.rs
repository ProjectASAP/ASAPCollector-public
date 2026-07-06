//! Well-known column / attribute names for the OTAP-Rust codec.
//!
//! Strategy-B carrier keys are pinned by
//! [edge-framework §7.2](../../../docs/design-asap-edge-framework.md#72-two-encoding-strategies)
//! and are case-sensitive, prefix-included. Every Strategy-B adapter
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
// Per edge-framework §7.2: in the OTAP carrier these ride as
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn well_known_keys_match_edge_framework_doc() {
        // Cross-check against the Strategy-B keys table in
        // edge-framework §7.2 — these MUST be byte-identical to the
        // spellings the doc pins for cross-platform interop.
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
