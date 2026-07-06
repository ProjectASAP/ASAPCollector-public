//! Host-neutral input to [`crate::precompute::Precompute::observe`].
//!
//! Adapters decode their
//! native event (`pmetric.NumberDataPoint` / `telegraf::Metric` /
//! `vector::Event` / `arrow::RecordBatch`) into one of these. The
//! runtime never sees host-specific types; it only sees [`Observation`]
//! and [`crate::envelope::SketchEnvelope`].

use crate::envelope::SketchEnvelope;

/// Host-neutral input to [`crate::precompute::Precompute::observe`].
///
/// Labels are `Vec<KeyValue>` rather than a host-specific attribute map
/// because attribute maps are host-specific (e.g. `pcommon::Map` is
/// OTel-only). Telegraf, Vector, and OTAP adapters carry the same
/// struct without pulling host crates in.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Observation {
    /// Wall-clock observation timestamp in milliseconds since the Unix
    /// epoch. Used for window assignment and late-data detection.
    pub timestamp_ms: u64,
    /// Metric name (e.g. `"http_requests_total"`). May be empty if the
    /// matchers don't key off metric name.
    pub metric: String,
    /// `(key, value)` pairs from the host's resource scope (e.g.
    /// `pmetric::ResourceMetrics::Resource()` attributes on OTel,
    /// analogous fields on Telegraf / Vector / OTAP). Today's OTel
    /// processors include resource attrs in the series key, so the
    /// runtime must too — preserved here as a separate slice so
    /// adapters don't have to flatten.
    pub resource_labels: Vec<KeyValue>,
    /// `(key, value)` pairs identifying this observation's
    /// data-point-level series. Order is not significant for matching;
    /// the series-key helper sorts by key for stable hashing.
    pub labels: Vec<KeyValue>,
    /// The actual measurement. Exactly one of the
    /// `float`/`hash`/`bytes`/`envelope` fields inside is meaningful
    /// per [`ObservationValueKind`].
    pub value: ObservationValue,
}

impl Observation {
    /// Construct a new [`Observation`].
    ///
    /// Trivial field-by-field constructor.
    pub fn new(
        timestamp_ms: u64,
        metric: impl Into<String>,
        resource_labels: Vec<KeyValue>,
        labels: Vec<KeyValue>,
        value: ObservationValue,
    ) -> Self {
        Self {
            timestamp_ms,
            metric: metric.into(),
            resource_labels,
            labels,
            value,
        }
    }
}

/// A single string-string label pair. Mirrors the `repeated KeyValue`
/// field in the `SketchEnvelope` proto.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KeyValue {
    /// Label key.
    pub key: String,
    /// Label value.
    pub value: String,
}

impl KeyValue {
    /// Construct a new [`KeyValue`] from any pair of string-likes.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Kind discriminator on [`ObservationValue`].
#[derive(
    Copy, Clone, Debug, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum ObservationValueKind {
    /// `float` is the meaningful field.
    #[default]
    Float,
    /// `hash` is the meaningful field; used by cardinality (HLL) and
    /// top-k (CountSketch) observations.
    Hash,
    /// `bytes` is the meaningful field; used by opaque-key
    /// set-aggregator observations.
    Bytes,
    /// `envelope` is the meaningful field; the observation carries a
    /// pre-aggregated upstream sketch and should be routed through
    /// [`crate::precompute::Precompute::observe_envelope`], NOT
    /// expanded to scalar samples.
    Envelope,
}

impl ObservationValueKind {
    /// Returns a debug-friendly name for the kind.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Float => "Float",
            Self::Hash => "Hash",
            Self::Bytes => "Bytes",
            Self::Envelope => "Envelope",
        }
    }
}

/// Kind-discriminated union over the four observation value shapes the
/// runtime supports.
///
/// Rather than a Rust enum, this uses a `Kind` discriminator with all
/// four fields side-by-side and an invariant that exactly one is
/// meaningful per kind — making the (kind, per-field) invariant
/// explicit on the wire and in serde output.
///
/// Construction goes through [`ObservationValue::float`] /
/// [`ObservationValue::hash`] / [`ObservationValue::bytes`] /
/// [`ObservationValue::envelope`] which guarantee the invariant.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ObservationValue {
    /// Discriminator: which of the four fields below is meaningful.
    pub kind: ObservationValueKind,
    /// Meaningful when `kind == Float`.
    pub float: f64,
    /// Meaningful when `kind == Hash` (cardinality / topk inputs).
    pub hash: u64,
    /// Meaningful when `kind == Bytes` (opaque keys for set
    /// aggregator).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bytes: Vec<u8>,
    /// Meaningful when `kind == Envelope`; pre-aggregated sketch from
    /// upstream that should be merged or delta-applied via
    /// [`crate::precompute::Precompute::observe_envelope`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<Box<SketchEnvelope>>,
}

impl ObservationValue {
    /// Construct an `ObservationValue` of [`ObservationValueKind::Float`].
    pub fn float(v: f64) -> Self {
        Self {
            kind: ObservationValueKind::Float,
            float: v,
            ..Default::default()
        }
    }

    /// Construct an `ObservationValue` of [`ObservationValueKind::Hash`].
    pub fn hash(h: u64) -> Self {
        Self {
            kind: ObservationValueKind::Hash,
            hash: h,
            ..Default::default()
        }
    }

    /// Construct an `ObservationValue` of [`ObservationValueKind::Bytes`].
    pub fn bytes(b: Vec<u8>) -> Self {
        Self {
            kind: ObservationValueKind::Bytes,
            bytes: b,
            ..Default::default()
        }
    }

    /// Construct an `ObservationValue` of
    /// [`ObservationValueKind::Envelope`].
    pub fn envelope(env: SketchEnvelope) -> Self {
        Self {
            kind: ObservationValueKind::Envelope,
            envelope: Some(Box::new(env)),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Encoding, SketchType};

    #[test]
    fn observation_constructs_with_all_four_value_kinds() {
        let labels = vec![KeyValue::new("region", "us-east")];

        let o_float = Observation::new(
            1000,
            "metric",
            vec![],
            labels.clone(),
            ObservationValue::float(1.0),
        );
        assert_eq!(o_float.value.kind, ObservationValueKind::Float);
        assert_eq!(o_float.value.float, 1.0);

        let o_hash = Observation::new(
            1000,
            "metric",
            vec![],
            labels.clone(),
            ObservationValue::hash(42),
        );
        assert_eq!(o_hash.value.kind, ObservationValueKind::Hash);
        assert_eq!(o_hash.value.hash, 42);

        let o_bytes = Observation::new(
            1000,
            "metric",
            vec![],
            labels.clone(),
            ObservationValue::bytes(vec![1, 2, 3]),
        );
        assert_eq!(o_bytes.value.kind, ObservationValueKind::Bytes);
        assert_eq!(o_bytes.value.bytes, vec![1, 2, 3]);

        let env = SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::DDSketch,
            encoding: Encoding::ProtoFull,
            ..Default::default()
        };
        let o_env = Observation::new(
            1000,
            "metric",
            vec![],
            labels,
            ObservationValue::envelope(env),
        );
        assert_eq!(o_env.value.kind, ObservationValueKind::Envelope);
        assert!(o_env.value.envelope.is_some());
    }

    #[test]
    fn kind_names_match_go_reference() {
        assert_eq!(ObservationValueKind::Float.name(), "Float");
        assert_eq!(ObservationValueKind::Hash.name(), "Hash");
        assert_eq!(ObservationValueKind::Bytes.name(), "Bytes");
        assert_eq!(ObservationValueKind::Envelope.name(), "Envelope");
    }
}
