//! Runtime view of the [`asap_sketchlib`] `SketchEnvelope` proto.
//!
//! Mirrors `asap-precompute-go/envelope.go`.
//!
//! The Go side defines a `SketchEnvelope` Go struct that wraps the
//! proto-encoded payload bytes plus the surrounding Go-side runtime
//! metadata that today's OTel modified-OTLP variants (Strategy A) and
//! tomorrow's Strategy-B adapters (Telegraf / Vector / OTAP) both need
//! (window bounds, agg_id, encoding tag, sketch_type tag, host-neutral
//! labels, metric name, count, temporality).
//!
//! The Rust side mirrors that struct verbatim. The actual prost-
//! generated proto type is re-exported as [`ProtoSketchEnvelope`] for
//! adapters that need proto-level access (e.g. for round-tripping the
//! `payload` against `asap_sketchlib`'s sketch-state oneof).

use crate::config::AggId;
use crate::observation::KeyValue;

/// Re-export of the prost-generated proto `SketchEnvelope` type from
/// `asap_sketchlib`.
///
/// This is the actual on-the-wire envelope (oneof'd over each sketch
/// state). Adapters that need to materialize the full proto
/// representation (e.g. to round-trip through `asap_sketchlib`'s
/// per-sketch state messages) reach for this type. The Layer-3 runtime
/// itself works only with the [`SketchEnvelope`] struct in this
/// module, which carries proto-encoded bytes in its `payload` field.
pub type ProtoSketchEnvelope = asap_sketchlib::proto::sketchlib::SketchEnvelope;

/// Identifies which sketch algorithm produced the envelope's payload
/// bytes.
///
/// Mirrors the design-doc §5.1 `SketchEnvelope.sketch_type` enum and
/// the Go `SketchType` constants. The `asap_sketchlib` proto today
/// carries the sketch type implicitly via its `sketch_state` oneof,
/// but the Layer-3 runtime keeps it as an explicit tag because
/// envelopes flow through code paths that don't always unmarshal the
/// inner state.
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum SketchType {
    /// Type was not set; reject at decode boundaries.
    #[default]
    Unspecified,
    /// DDSketch quantile sketch.
    DDSketch,
    /// KLL quantile sketch.
    KLLSketch,
    /// HyperLogLog cardinality sketch.
    HLLSketch,
    /// Count Sketch frequency sketch.
    CountSketch,
    /// Count-Min Sketch frequency sketch.
    CountMinSketch,
}

impl SketchType {
    /// Returns the canonical name for the sketch type, matching the
    /// well-known string spellings used by Strategy-B adapters
    /// (ADR-0003 §4 standardized keys) and the Go `String()` method.
    pub fn name(&self) -> &'static str {
        match self {
            Self::DDSketch => "DDSketch",
            Self::KLLSketch => "KLLSketch",
            Self::HLLSketch => "HLLSketch",
            Self::CountSketch => "CountSketch",
            Self::CountMinSketch => "CountMinSketch",
            Self::Unspecified => "Unspecified",
        }
    }
}

/// Describes how the bytes in [`SketchEnvelope::payload`] are encoded.
///
/// Mirrors the design-doc §5.1 `SketchEnvelope.encoding` enum and the
/// Go `Encoding` constants.
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum Encoding {
    /// Encoding was not set; treated as `ProtoFull` for backwards-
    /// compatibility but adapters should reject at strict-mode
    /// boundaries.
    #[default]
    Unspecified,
    /// `payload` is a proto-encoded full `SketchEnvelope` state.
    ProtoFull,
    /// `payload` is a proto-encoded sparse delta against the
    /// receiver's cached snapshot of this series.
    ProtoDelta,
    /// `payload` is a msgpack-encoded full state (some sketches expose
    /// msgpack as a faster wire format).
    Msgpack,
}

impl Encoding {
    /// Returns the canonical encoding name. Mirrors the Go `String()`
    /// method.
    pub fn name(&self) -> &'static str {
        match self {
            Self::ProtoFull => "PROTO_FULL",
            Self::ProtoDelta => "PROTO_DELTA",
            Self::Msgpack => "MSGPACK",
            Self::Unspecified => "UNSPECIFIED",
        }
    }
}

/// Runtime view of the `asap_sketchlib` `SketchEnvelope` proto plus
/// the surrounding metadata that today's OTel modified-OTLP variants
/// and tomorrow's Strategy-B adapters both need (window bounds,
/// `agg_id`, encoding tag, sketch-type tag, host-neutral labels).
///
/// Mirrors the Go `SketchEnvelope` struct field-by-field. The
/// `payload` bytes are byte-identical to today's `asap_sketchlib`
/// proto encoding (see ADR-0002 §"Behavior preservation").
///
/// The on-the-wire representation depends on the platform's encoding
/// strategy (design-doc §7.2):
///   - Strategy A (OTel today): `payload` rides a typed
///     `pmetric::Metric.data` oneof variant (DDSketch / KLLSketch / …).
///   - Strategy B (Telegraf / Vector / OTAP): the whole struct is
///     decomposed into the well-known `_asap_envelope` /
///     `_asap_sketch_type` / `_asap_agg_id` / etc. carrier keys.
///
/// Either way `payload` is byte-identical and is what
/// [`crate::precompute::Precompute::observe_envelope`] deserializes.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SketchEnvelope {
    /// Design-doc `SketchEnvelope.schema_version` field. Adapters
    /// reject envelopes whose version exceeds the highest version
    /// they understand.
    pub schema_version: u32,
    /// Identifies which sketch algorithm produced `payload`.
    pub sketch_type: SketchType,
    /// Controller-plan join key. Pairs the envelope to a specific
    /// [`crate::config::PrecomputeConfig`].
    pub agg_id: AggId,
    /// Resource-scope attributes captured at observation time (for
    /// OTel: `pmetric::ResourceMetrics::Resource()` attrs). Carried
    /// in-process so the adapter's encode path can faithfully
    /// reconstruct the host's resource hierarchy on emission.
    ///
    /// NOT on the proto wire today: proto's `repeated KeyValue labels`
    /// carries only data-point labels; cross-host hops that need to
    /// preserve resource scope can prefix-encode (e.g.
    /// `resource.region=…`) into `labels` at encode time. Strategy-B
    /// hosts (Telegraf / Vector / flat-attribute platforms) flatten
    /// resource into `labels`.
    pub resource_labels: Vec<KeyValue>,
    /// Host-neutral string-string label pairs identifying the
    /// data-point-level (metric × attrs) series.
    pub labels: Vec<KeyValue>,
    /// Inclusive lower bound of the window the payload summarizes
    /// (Unix milliseconds).
    pub window_start_ms: u64,
    /// Exclusive upper bound of the window the payload summarizes
    /// (Unix milliseconds).
    pub window_end_ms: u64,
    /// How `payload` is laid out.
    pub encoding: Encoding,
    /// Proto-encoded sketch state or delta. The bandwidth-invariant
    /// blob (design-doc §5.2).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub payload: Vec<u8>,
    /// Determinism contract for cross-language reconstruction. Comes
    /// from `asap_sketchlib`'s proto `HashSpec`.
    ///
    /// Wrapped in `Option` because not every adapter populates it,
    /// and the proto type has no `Default` impl chain we can rely on.
    /// `serde(skip)` because the proto type does not derive serde —
    /// shutdown/restore use cases serialize the rest of the envelope
    /// and re-fetch the hash spec from configuration.
    #[serde(skip)]
    pub hash_spec: Option<asap_sketchlib::proto::sketchlib::HashSpec>,
    /// Metric name the envelope was emitted for.
    ///
    /// Today's per-processor `flushWindow` builds the output
    /// `pmetric::Metric` and sets its `Name()` from a config field;
    /// once the shims delegate to this runtime, the runtime needs to
    /// know the metric name per-emission so `Adapter::encode` can
    /// stamp it onto the synthesized output Metric. This is an
    /// in-process Rust field — NOT a proto wire field. The canonical
    /// wire bytes remain `payload` (ADR-0002 §"Behavior
    /// preservation").
    pub metric_name: String,
    /// Total observation count this envelope represents (sum of
    /// `dp.Count()` for the producing window's input samples).
    ///
    /// The OTel adapter copies it into output Sum data points via
    /// `dp.SetCount()`; some downstream consumers use it as a sanity
    /// field. In-process only.
    pub count: u64,
    /// OTel temporality enum stored as an `i32` to keep the runtime
    /// host-neutral (no `pmetric` import here): 0 = unspecified,
    /// 1 = delta, 2 = cumulative.
    ///
    /// The OTel adapter encode path reads this to set
    /// `Sum::SetAggregationTemporality(...)`. In-process only.
    pub aggregation_temporality: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_type_names_match_go() {
        assert_eq!(SketchType::DDSketch.name(), "DDSketch");
        assert_eq!(SketchType::KLLSketch.name(), "KLLSketch");
        assert_eq!(SketchType::HLLSketch.name(), "HLLSketch");
        assert_eq!(SketchType::CountSketch.name(), "CountSketch");
        assert_eq!(SketchType::CountMinSketch.name(), "CountMinSketch");
        assert_eq!(SketchType::Unspecified.name(), "Unspecified");
    }

    #[test]
    fn encoding_names_match_go() {
        assert_eq!(Encoding::ProtoFull.name(), "PROTO_FULL");
        assert_eq!(Encoding::ProtoDelta.name(), "PROTO_DELTA");
        assert_eq!(Encoding::Msgpack.name(), "MSGPACK");
        assert_eq!(Encoding::Unspecified.name(), "UNSPECIFIED");
    }

    #[test]
    fn sketch_envelope_serde_round_trip() {
        let env = SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::DDSketch,
            agg_id: 42,
            resource_labels: vec![KeyValue::new("region", "us-east")],
            labels: vec![KeyValue::new("path", "/api")],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::ProtoFull,
            payload: vec![1, 2, 3, 4],
            hash_spec: None,
            metric_name: "http_request_duration".into(),
            count: 100,
            aggregation_temporality: 1,
        };
        let json = serde_json::to_string(&env).expect("serialize");
        let decoded: SketchEnvelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, decoded);
    }
}
