//! Runtime sketch envelope: an ASAPv1 sketch payload from
//! [`asap_sketchlib`] plus the runtime metadata around it (window
//! bounds, `agg_id`, sketch type, labels, metric name, count,
//! temporality).

use crate::config::AggId;
use crate::observation::KeyValue;

/// Identifies which sketch algorithm produced the envelope's payload
/// bytes. The ASAPv1 payload carries its own kind id; the runtime keeps
/// this explicit tag so routing does not need to parse the payload.
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
    /// Count-Min Sketch plus a top-k heavy-hitter heap (ASAPv1 kind `0x03 0x00`).
    CountMinSketchWithHeap,
}

impl SketchType {
    /// Returns the canonical name for the sketch type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::DDSketch => "DDSketch",
            Self::KLLSketch => "KLLSketch",
            Self::HLLSketch => "HLLSketch",
            Self::CountSketch => "CountSketch",
            Self::CountMinSketch => "CountMinSketch",
            Self::CountMinSketchWithHeap => "CountMinSketchWithHeap",
            Self::Unspecified => "Unspecified",
        }
    }

    /// Parses a canonical [`Self::name`]; an empty string is `Unspecified`.
    pub fn from_name(name: &str) -> Option<Self> {
        [
            Self::DDSketch,
            Self::KLLSketch,
            Self::HLLSketch,
            Self::CountSketch,
            Self::CountMinSketch,
            Self::CountMinSketchWithHeap,
            Self::Unspecified,
        ]
        .into_iter()
        .find(|t| t.name() == name)
        .or_else(|| name.is_empty().then_some(Self::Unspecified))
    }
}

/// One emitted sketch (or scalar estimate) plus its runtime metadata.
///
/// `payload` is the sketch's ASAPv1 envelope bytes and is what
/// [`crate::precompute::Precompute::observe_envelope`] merges.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SketchEnvelope {
    /// Wire schema version. Adapters
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
    /// ASAPv1 sketch envelope bytes; empty on the estimate path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub payload: Vec<u8>,
    /// Metric name the envelope was emitted for.
    ///
    /// Today's per-processor `flushWindow` builds the output
    /// `pmetric::Metric` and sets its `Name()` from a config field;
    /// once the shims delegate to this runtime, the runtime needs to
    /// know the metric name per-emission so `Adapter::encode` can
    /// stamp it onto the synthesized output Metric.
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
    /// Estimated scalar result for the `transmit_sketch = false` output
    /// mode — a quantile value (DDSketch / KLL) or a cardinality estimate
    /// (HLL). Zero and ignored when a sketch `payload` is present; the
    /// distinguishing labels (e.g. `quantile`) live in [`Self::labels`].
    /// Encode writes it to the `value` column.
    #[serde(default)]
    pub value: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

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
            payload: vec![1, 2, 3, 4],
            metric_name: "http_request_duration".into(),
            count: 100,
            aggregation_temporality: 1,
            value: 88.0,
        };
        let json = serde_json::to_string(&env).expect("serialize");
        let decoded: SketchEnvelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(env, decoded);
    }
}
