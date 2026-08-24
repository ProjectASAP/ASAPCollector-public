// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! Direct `SketchEnvelope <-> OtapPdata` binding — the real `OtapPdata`
//! is built straight from a `&[SketchEnvelope]` slice, and a real
//! `OtapPdata` is decoded straight into `Vec<Observation>`. No
//! intermediate `RecordBatch` or `OtapMetricRecords` two-batch family
//! in between.
//!
//! # Why direct, not via `encode_batch`/`decode_batch`/`records::{flatten,lift}`
//!
//! Those three exist as a *general*, real-OTAP-free carrier format —
//! useful for other Strategy-B adapters (Telegraf, Vector) that might
//! one day consume the same flat shape, and for testing this codec
//! without any OTAP dependency at all. But this repo only ever ships
//! to OTAP, and no second adapter has actually shown up. Going through
//! them here would mean: `SketchEnvelope` -> flat `RecordBatch`
//! (`encode_batch`) -> two-batch `OtapMetricRecords` (`lift`) -> real
//! `OtapPdata` (this module) — three representations for what's really
//! one job. This module skips straight to the last step, implementing
//! OTAP's own `MetricsView` trait directly over `&[SketchEnvelope]`.
//!
//! `encode_batch`/`decode_batch`/`records::{flatten,lift}` still exist
//! and are still tested — they back the legacy `SeriesDictionary` /
//! `otap::wire` transport (`dictionary.rs`, standalone example
//! binaries), which predates this module and isn't part of this path.
//!
//! # A real bug this rewrite fixes
//!
//! `encode_batch` unconditionally sets `_asap_envelope` for every row,
//! including estimate-mode envelopes (`payload` empty, the gauge value
//! rides in `value` instead). `arrow_array::BinaryArray` treats
//! `Some(&[])` as a *present, empty* value, not null — confirmed
//! empirically (`BinaryArray::from_opt_vec(vec![Some(&[])]).is_null(0)
//! == false`) — so `decode_batch`'s `if env_arr.is_null(row)` check
//! never trips for an estimate-mode row, and it gets misrouted through
//! the envelope path with an empty payload instead of the scalar path.
//! This module's direct encoder only attaches `_asap_envelope` (and
//! its sibling `_asap_*` attributes) when `!envelope.payload.is_empty()`,
//! and its decoder additionally guards with `.filter(|b| !b.is_empty())`
//! defensively on the way in. The legacy `encode_batch`/`decode_batch`
//! pair still has the original behavior — untouched here since fixing
//! it isn't in scope for this rewrite, but worth knowing about if that
//! path is ever exercised with estimate-mode envelopes.
//!
//! # Provenance / verification status
//!
//! Written against, and build/lint/test-verified against, upstream
//! `open-telemetry/otel-arrow` commit
//! `3e85c3460361446ebfce99e9f35fffd2dd5ab740` (2026-08-24) via a plain
//! git dependency (see `Cargo.toml`'s `otap-engine` feature) — no
//! manual staging into a local checkout required, unlike the
//! `otap-patch/` overlay this module replaced.
//!
//! # Scope
//!
//! What "handling a metric" means on decode splits into two cases,
//! decided by content, not by which OTLP metric type carried it:
//!
//! - **A data point carrying `_asap_envelope`** (this module's own
//!   encode output, or any other `asap_sketches` node's) routes
//!   through `ObservationValueKind::Envelope`; `Precompute::observe`
//!   already dispatches those to `observe_envelope` (merge as a
//!   pre-aggregated sketch).
//! - **A genuine (non-envelope) OTLP metric** — Gauge/Sum data points
//!   become scalar `Observation`s. Histogram/ExponentialHistogram/
//!   Summary data points are skipped (counted, not silently dropped —
//!   see [`DecodeOutcome::skipped_non_scalar`]).
//!
//! On encode, every row is assumed to share one metric name — an
//! `AsapSketchesProcessor` instance has exactly one
//! `PluginConfig::output_metric_name`, so this holds by construction
//! for anything this crate's own `Precompute` produces;
//! [`encode_envelopes_to_pdata`] still checks it explicitly and errors
//! loudly on a real mismatch rather than silently dropping rows.
//!
//! Resource and scope are not modelled — every emitted metric attaches
//! to an empty `Resource` / unnamed `Scope`, same as the codec this
//! replaced.

use thiserror::Error;

use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::encode::encode_metrics_otap_batch;
use otel_arrow_dfe_pdata::otap::OtapArrowRecords;
use otel_arrow_dfe_pdata::views::otap::OtapMetricsView;
use otel_arrow_dfe_pdata::{OtapPayload, TryIntoWithOptions};
use otel_arrow_dfe_pdata_views::views::common::{
    AnyValueView, AttributeView, InstrumentationScopeView, Str, ValueType,
};
use otel_arrow_dfe_pdata_views::views::metrics::{
    AggregationTemporality, BucketsView, DataPointFlags, DataType as MetricKind, DataView,
    ExemplarView, ExponentialHistogramDataPointView, ExponentialHistogramView, GaugeView,
    HistogramDataPointView, HistogramView, MetricView, MetricsView, NumberDataPointView,
    ResourceMetricsView, ScopeMetricsView, SumView, SummaryDataPointView, SummaryView,
    Value as DpValue, ValueAtQuantileView,
};
use otel_arrow_dfe_pdata_views::views::resource::ResourceView;

use crate::envelope::SketchEnvelope;
use crate::observation::{KeyValue, Observation, ObservationValue};

use super::decode::{parse_encoding, parse_sketch_type};
use super::schema::{
    ATTR_AGG_ID, ATTR_ENCODING, ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SKETCH_TYPE,
    ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS,
};

/// Failure modes for [`encode_envelopes_to_pdata`] / [`decode_pdata_to_observations`].
#[derive(Debug, Error)]
pub enum CodecError {
    /// More than one distinct `metric_name` appeared across the
    /// envelopes passed to one [`encode_envelopes_to_pdata`] call — an
    /// `AsapSketchesProcessor` instance has exactly one
    /// `output_metric_name`, so this indicates envelopes from more
    /// than one processor instance were mixed into a single call.
    #[error(
        "otap codec: one encode call carries more than one metric name ({first:?} and {second:?} seen)"
    )]
    MixedMetricNames {
        /// First metric name seen.
        first: String,
        /// A second, different metric name seen in the same call.
        second: String,
    },
    /// Building the real `OtapArrowRecords::Metrics` batch failed.
    #[error("otap codec: encoding real OTAP metrics batch failed: {0}")]
    Encode(String),
    /// Reading the real `OtapArrowRecords::Metrics` batch failed.
    #[error("otap codec: reading real OTAP metrics batch failed: {0}")]
    Decode(String),
}

// ============================================================================
// Encode direction: &[SketchEnvelope] -> OtapPdata
// ============================================================================

/// Encodes a slice of [`SketchEnvelope`]s directly into a real
/// `OtapPdata` carrying an `OtapArrowRecords::Metrics` payload — the
/// actual "put the sketch envelope bytes onto an OTAP metric" step.
///
/// Builds a fresh, contextless `OtapPdata` (`OtapPdata::new_todo_context`)
/// rather than propagating any single input message's context: the
/// processor's flush ticker emits one window's worth of envelopes on
/// its own wall-clock schedule, decoupled from any specific triggering
/// input message, so there is no single Ack/Nack chain to attach the
/// output to.
///
/// Returns `Ok` of an `OtapPdata` with zero data points for an empty
/// `envelopes` slice (matches `encode_metrics_otap_batch`'s own
/// "empty in, empty batch out" contract).
pub fn encode_envelopes_to_pdata(envelopes: &[SketchEnvelope]) -> Result<OtapPdata, CodecError> {
    let view = AsapMetricsView::try_from_envelopes(envelopes)?;
    let arrow_records =
        encode_metrics_otap_batch(&view).map_err(|e| CodecError::Encode(e.to_string()))?;
    let payload: OtapPayload = arrow_records.into();
    Ok(OtapPdata::new_todo_context(payload))
}

/// Zero-copy adapter presenting a `&[SketchEnvelope]` as a
/// `MetricsView` directly — one Resource (empty), one Scope (unnamed),
/// one Metric (the envelopes' shared `metric_name`), one Gauge, one
/// `NumberDataPoint` per envelope.
struct AsapMetricsView<'a> {
    metric_name: String,
    envelopes: &'a [SketchEnvelope],
}

impl<'a> AsapMetricsView<'a> {
    fn try_from_envelopes(envelopes: &'a [SketchEnvelope]) -> Result<Self, CodecError> {
        let mut metric_name: Option<&str> = None;
        for env in envelopes {
            match metric_name {
                None => metric_name = Some(&env.metric_name),
                Some(seen) if seen == env.metric_name => {}
                Some(seen) => {
                    return Err(CodecError::MixedMetricNames {
                        first: seen.to_string(),
                        second: env.metric_name.clone(),
                    });
                }
            }
        }
        Ok(Self {
            metric_name: metric_name.unwrap_or_default().to_string(),
            envelopes,
        })
    }

    /// One row's worth of attributes, built directly from the
    /// envelope's own fields — no intermediate Arrow columns. The
    /// `_asap_*` Strategy-B carriers only appear when this row is
    /// actually an envelope-payload row (see this module's doc, "A
    /// real bug this rewrite fixes").
    fn attributes_for_row(&self, row: usize) -> Vec<AsapAttribute<'a>> {
        let env = &self.envelopes[row];
        let mut attrs = Vec::with_capacity(env.labels.len() + 7);
        if !env.payload.is_empty() {
            attrs.push(AsapAttribute {
                key: ATTR_ENVELOPE,
                value: AsapAnyValue::Bytes(&env.payload),
            });
            attrs.push(AsapAttribute {
                key: ATTR_SKETCH_TYPE,
                value: AsapAnyValue::Str(env.sketch_type.name()),
            });
            attrs.push(AsapAttribute {
                key: ATTR_AGG_ID,
                value: AsapAnyValue::Int(env.agg_id),
            });
            attrs.push(AsapAttribute {
                key: ATTR_SCHEMA_VERSION,
                value: AsapAnyValue::Int(u64::from(env.schema_version)),
            });
            attrs.push(AsapAttribute {
                key: ATTR_WINDOW_START_MS,
                value: AsapAnyValue::Int(env.window_start_ms),
            });
            attrs.push(AsapAttribute {
                key: ATTR_WINDOW_END_MS,
                value: AsapAnyValue::Int(env.window_end_ms),
            });
            attrs.push(AsapAttribute {
                key: ATTR_ENCODING,
                value: AsapAnyValue::Str(env.encoding.name()),
            });
        }
        for kv in &env.labels {
            attrs.push(AsapAttribute {
                key: &kv.key,
                value: AsapAnyValue::Str(&kv.value),
            });
        }
        attrs
    }
}

impl<'a> MetricsView for AsapMetricsView<'a> {
    type ResourceMetrics<'res>
        = AsapResourceMetricsView<'res, 'a>
    where
        Self: 'res;
    type ResourceMetricsIter<'res>
        = std::vec::IntoIter<AsapResourceMetricsView<'res, 'a>>
    where
        Self: 'res;

    fn resources(&self) -> Self::ResourceMetricsIter<'_> {
        if self.envelopes.is_empty() {
            Vec::new().into_iter()
        } else {
            vec![AsapResourceMetricsView { view: self }].into_iter()
        }
    }
}

struct AsapResourceMetricsView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
}

impl<'v, 'a> ResourceMetricsView for AsapResourceMetricsView<'v, 'a> {
    type Resource<'res>
        = AsapNoResource
    where
        Self: 'res;
    type ScopeMetrics<'scp>
        = AsapScopeMetricsView<'scp, 'a>
    where
        Self: 'scp;
    type ScopesIter<'scp>
        = std::vec::IntoIter<AsapScopeMetricsView<'scp, 'a>>
    where
        Self: 'scp;

    fn resource(&self) -> Option<Self::Resource<'_>> {
        None
    }

    fn scopes(&self) -> Self::ScopesIter<'_> {
        vec![AsapScopeMetricsView { view: self.view }].into_iter()
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        None
    }
}

struct AsapScopeMetricsView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
}

impl<'v, 'a> ScopeMetricsView for AsapScopeMetricsView<'v, 'a> {
    type Scope<'scp>
        = AsapNoScope
    where
        Self: 'scp;
    type Metric<'met>
        = AsapMetricView<'met, 'a>
    where
        Self: 'met;
    type MetricIter<'met>
        = std::vec::IntoIter<AsapMetricView<'met, 'a>>
    where
        Self: 'met;

    fn scope(&self) -> Option<Self::Scope<'_>> {
        None
    }

    fn metrics(&self) -> Self::MetricIter<'_> {
        vec![AsapMetricView { view: self.view }].into_iter()
    }

    fn schema_url(&self) -> Str<'_> {
        b""
    }
}

struct AsapMetricView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
}

impl<'v, 'a> MetricView for AsapMetricView<'v, 'a> {
    type Data<'dat>
        = AsapDataView<'dat, 'a>
    where
        Self: 'dat;
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;

    fn name(&self) -> Str<'_> {
        self.view.metric_name.as_bytes()
    }

    fn description(&self) -> Str<'_> {
        b""
    }

    fn unit(&self) -> Str<'_> {
        b""
    }

    fn data(&self) -> Option<Self::Data<'_>> {
        Some(AsapDataView { view: self.view })
    }

    fn metadata(&self) -> Self::AttributeIter<'_> {
        Vec::new().into_iter()
    }
}

struct AsapDataView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
}

impl<'v, 'a> DataView<'v> for AsapDataView<'v, 'a> {
    type Gauge<'gauge>
        = AsapGaugeView<'gauge, 'a>
    where
        Self: 'gauge;
    type Sum<'sum>
        = AsapNoSum
    where
        Self: 'sum;
    type Histogram<'histogram>
        = AsapNoHistogram
    where
        Self: 'histogram;
    type ExponentialHistogram<'exp>
        = AsapNoExpHistogram
    where
        Self: 'exp;
    type Summary<'summary>
        = AsapNoSummary
    where
        Self: 'summary;

    fn value_type(&self) -> MetricKind {
        MetricKind::Gauge
    }

    fn as_gauge(&self) -> Option<Self::Gauge<'_>> {
        Some(AsapGaugeView { view: self.view })
    }

    fn as_sum(&self) -> Option<Self::Sum<'_>> {
        None
    }

    fn as_histogram(&self) -> Option<Self::Histogram<'_>> {
        None
    }

    fn as_exponential_histogram(&self) -> Option<Self::ExponentialHistogram<'_>> {
        None
    }

    fn as_summary(&self) -> Option<Self::Summary<'_>> {
        None
    }
}

struct AsapGaugeView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
}

impl<'v, 'a> GaugeView for AsapGaugeView<'v, 'a> {
    type NumberDataPoint<'dp>
        = AsapNumberDataPointView<'dp, 'a>
    where
        Self: 'dp;
    type NumberDataPointIter<'dp>
        = std::vec::IntoIter<AsapNumberDataPointView<'dp, 'a>>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        (0..self.view.envelopes.len())
            .map(|row| AsapNumberDataPointView {
                view: self.view,
                row,
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

struct AsapNumberDataPointView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    row: usize,
}

impl<'v, 'a> NumberDataPointView for AsapNumberDataPointView<'v, 'a> {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;
    type Exemplar<'ex>
        = AsapNoExemplar
    where
        Self: 'ex;
    type ExemplarIter<'ex>
        = std::vec::IntoIter<AsapNoExemplar>
    where
        Self: 'ex;

    fn start_time_unix_nano(&self) -> u64 {
        0
    }

    fn time_unix_nano(&self) -> u64 {
        self.view.envelopes[self.row]
            .window_end_ms
            .saturating_mul(1_000_000)
    }

    fn value(&self) -> Option<DpValue> {
        Some(DpValue::Double(self.view.envelopes[self.row].value))
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        self.view.attributes_for_row(self.row).into_iter()
    }

    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        Vec::new().into_iter()
    }

    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(0)
    }
}

/// One typed attribute value ASAP's envelopes can carry — mirrors the
/// three-way union OTAP's own attribute value model supports for
/// what this codec actually produces (bytes / str / int).
enum AsapAnyValue<'a> {
    Str(&'a str),
    Int(u64),
    Bytes(&'a [u8]),
}

struct AsapAttribute<'a> {
    key: &'a str,
    value: AsapAnyValue<'a>,
}

impl<'a> AttributeView for AsapAttribute<'a> {
    type Val<'val>
        = AsapAnyValueView<'val>
    where
        Self: 'val;

    fn key(&self) -> Str<'_> {
        self.key.as_bytes()
    }

    fn value(&self) -> Option<Self::Val<'_>> {
        Some(AsapAnyValueView(&self.value))
    }
}

struct AsapAnyValueView<'a>(&'a AsapAnyValue<'a>);

impl<'a> AnyValueView<'a> for AsapAnyValueView<'a> {
    type KeyValue = AsapAttribute<'a>;
    type ArrayIter<'arr>
        = std::iter::Empty<Self>
    where
        Self: 'arr;
    type KeyValueIter<'kv>
        = std::iter::Empty<Self::KeyValue>
    where
        Self: 'kv;

    fn value_type(&self) -> ValueType {
        match self.0 {
            AsapAnyValue::Str(_) => ValueType::String,
            AsapAnyValue::Int(_) => ValueType::Int64,
            AsapAnyValue::Bytes(_) => ValueType::Bytes,
        }
    }

    fn as_string(&self) -> Option<Str<'_>> {
        match self.0 {
            AsapAnyValue::Str(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn as_int64(&self) -> Option<i64> {
        match self.0 {
            // Lossy above i64::MAX, which ASAP never actually
            // produces here (the only `_asap_*` int-typed attributes
            // are small counters/timestamps well under that bound).
            AsapAnyValue::Int(v) => Some(*v as i64),
            _ => None,
        }
    }

    fn as_double(&self) -> Option<f64> {
        None
    }

    fn as_bytes(&self) -> Option<&[u8]> {
        match self.0 {
            AsapAnyValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    fn as_array(&self) -> Option<Self::ArrayIter<'_>> {
        None
    }

    fn as_kvlist(&self) -> Option<Self::KeyValueIter<'_>> {
        None
    }
}

// -- Uninhabited placeholder types -------------------------------------------
//
// `resource()`/`scope()` always return `None`, and only Gauge is ever
// produced (`as_sum`/`as_histogram`/`as_exponential_histogram`/
// `as_summary` always return `None`, and there are no exemplars) — but
// the view traits still require *some* concrete, well-formed type for
// each associated type regardless of whether an instance is ever
// constructed. An uninhabited enum (`enum X {}`) lets every trait
// method be `match *self {}` — valid because no value of an
// uninhabited type can ever exist to call it on, so the body coerces
// to any return type without ever actually running.

enum AsapNoResource {}
impl ResourceView for AsapNoResource {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributesIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;

    fn attributes(&self) -> Self::AttributesIter<'_> {
        match *self {}
    }

    fn dropped_attributes_count(&self) -> u32 {
        match *self {}
    }
}

enum AsapNoScope {}
impl InstrumentationScopeView for AsapNoScope {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;

    fn name(&self) -> Option<Str<'_>> {
        match *self {}
    }

    fn version(&self) -> Option<Str<'_>> {
        match *self {}
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        match *self {}
    }

    fn dropped_attributes_count(&self) -> u32 {
        match *self {}
    }
}

enum AsapNoExemplar {}
impl ExemplarView for AsapNoExemplar {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;

    fn filtered_attributes(&self) -> Self::AttributeIter<'_> {
        match *self {}
    }

    fn time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn value(&self) -> Option<DpValue> {
        match *self {}
    }

    fn span_id(&self) -> Option<&[u8; 8]> {
        match *self {}
    }

    fn trace_id(&self) -> Option<&[u8; 16]> {
        match *self {}
    }
}

enum AsapNoSum {}
impl SumView for AsapNoSum {
    type NumberDataPoint<'dp>
        = AsapNumberDataPointView<'dp, 'static>
    where
        Self: 'dp;
    type NumberDataPointIter<'dp>
        = std::vec::IntoIter<AsapNumberDataPointView<'dp, 'static>>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        match *self {}
    }

    fn aggregation_temporality(&self) -> AggregationTemporality {
        match *self {}
    }

    fn is_monotonic(&self) -> bool {
        match *self {}
    }
}

enum AsapNoHistogram {}
impl HistogramView for AsapNoHistogram {
    type HistogramDataPoint<'dp>
        = AsapNoHistogramDataPoint
    where
        Self: 'dp;
    type HistogramDataPointIter<'dp>
        = std::vec::IntoIter<AsapNoHistogramDataPoint>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::HistogramDataPointIter<'_> {
        match *self {}
    }

    fn aggregation_temporality(&self) -> AggregationTemporality {
        match *self {}
    }
}

enum AsapNoHistogramDataPoint {}
impl HistogramDataPointView for AsapNoHistogramDataPoint {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;
    type BucketCountIter<'bc>
        = std::iter::Empty<u64>
    where
        Self: 'bc;
    type ExplicitBoundsIter<'eb>
        = std::iter::Empty<f64>
    where
        Self: 'eb;
    type Exemplar<'ex>
        = AsapNoExemplar
    where
        Self: 'ex;
    type ExemplarIter<'ex>
        = std::vec::IntoIter<AsapNoExemplar>
    where
        Self: 'ex;

    fn attributes(&self) -> Self::AttributeIter<'_> {
        match *self {}
    }

    fn start_time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn count(&self) -> u64 {
        match *self {}
    }

    fn sum(&self) -> Option<f64> {
        match *self {}
    }

    fn bucket_counts(&self) -> Self::BucketCountIter<'_> {
        match *self {}
    }

    fn explicit_bounds(&self) -> Self::ExplicitBoundsIter<'_> {
        match *self {}
    }

    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        match *self {}
    }

    fn flags(&self) -> DataPointFlags {
        match *self {}
    }

    fn min(&self) -> Option<f64> {
        match *self {}
    }

    fn max(&self) -> Option<f64> {
        match *self {}
    }
}

enum AsapNoExpHistogram {}
impl ExponentialHistogramView for AsapNoExpHistogram {
    type ExponentialHistogramDataPoint<'dp>
        = AsapNoExpHistogramDataPoint
    where
        Self: 'dp;
    type ExponentialHistogramDataPointIter<'dp>
        = std::vec::IntoIter<AsapNoExpHistogramDataPoint>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::ExponentialHistogramDataPointIter<'_> {
        match *self {}
    }

    fn aggregation_temporality(&self) -> AggregationTemporality {
        match *self {}
    }
}

enum AsapNoExpHistogramDataPoint {}
impl ExponentialHistogramDataPointView for AsapNoExpHistogramDataPoint {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;
    type Exemplar<'ex>
        = AsapNoExemplar
    where
        Self: 'ex;
    type ExemplarIter<'ex>
        = std::vec::IntoIter<AsapNoExemplar>
    where
        Self: 'ex;
    type Buckets<'b>
        = AsapNoBuckets
    where
        Self: 'b;

    fn start_time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn count(&self) -> u64 {
        match *self {}
    }

    fn sum(&self) -> Option<f64> {
        match *self {}
    }

    fn min(&self) -> Option<f64> {
        match *self {}
    }

    fn max(&self) -> Option<f64> {
        match *self {}
    }

    fn scale(&self) -> i32 {
        match *self {}
    }

    fn zero_count(&self) -> u64 {
        match *self {}
    }

    fn positive(&self) -> Option<Self::Buckets<'_>> {
        match *self {}
    }

    fn negative(&self) -> Option<Self::Buckets<'_>> {
        match *self {}
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        match *self {}
    }

    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        match *self {}
    }

    fn flags(&self) -> DataPointFlags {
        match *self {}
    }

    fn zero_threshold(&self) -> f64 {
        match *self {}
    }
}

enum AsapNoBuckets {}
impl BucketsView for AsapNoBuckets {
    type BucketCountIter<'bc>
        = std::iter::Empty<u64>
    where
        Self: 'bc;

    fn offset(&self) -> i32 {
        match *self {}
    }

    fn bucket_counts(&self) -> Self::BucketCountIter<'_> {
        match *self {}
    }
}

enum AsapNoSummary {}
impl SummaryView for AsapNoSummary {
    type SummaryDataPoint<'dp>
        = AsapNoSummaryDataPoint
    where
        Self: 'dp;
    type SummaryDataPointIter<'dp>
        = std::vec::IntoIter<AsapNoSummaryDataPoint>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::SummaryDataPointIter<'_> {
        match *self {}
    }
}

enum AsapNoSummaryDataPoint {}
impl SummaryDataPointView for AsapNoSummaryDataPoint {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;
    type ValueAtQuantile<'q>
        = AsapNoValueAtQuantile
    where
        Self: 'q;
    type ValueAtQuantileIter<'q>
        = std::vec::IntoIter<AsapNoValueAtQuantile>
    where
        Self: 'q;

    fn start_time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn time_unix_nano(&self) -> u64 {
        match *self {}
    }

    fn count(&self) -> u64 {
        match *self {}
    }

    fn sum(&self) -> f64 {
        match *self {}
    }

    fn quantile_values(&self) -> Self::ValueAtQuantileIter<'_> {
        match *self {}
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        match *self {}
    }

    fn flags(&self) -> DataPointFlags {
        match *self {}
    }
}

enum AsapNoValueAtQuantile {}
impl ValueAtQuantileView for AsapNoValueAtQuantile {
    fn quantile(&self) -> f64 {
        match *self {}
    }

    fn value(&self) -> f64 {
        match *self {}
    }
}

// ============================================================================
// Decode direction: OtapPdata -> Vec<Observation>
// ============================================================================

/// Outcome of [`decode_pdata_to_observations`].
#[derive(Debug, Default)]
pub struct DecodeOutcome {
    /// One `Observation` per scalar (Gauge/Sum) data point seen —
    /// empty when `pdata` wasn't Metrics, decoded to zero data points,
    /// or every data point was non-scalar.
    pub observations: Vec<Observation>,
    /// Histogram / ExponentialHistogram / Summary data points seen and
    /// skipped — not silently dropped, see this module's doc "Scope"
    /// section for why they aren't expanded.
    pub skipped_non_scalar: usize,
}

/// Converts a real `OtapPdata` directly into `Vec<Observation>` —
/// feeds `Precompute::observe` on the producer role's ingest path.
///
/// Accepts `pdata` by value (consumed) — the caller's `Message::PData`
/// match arm already owns it and has no further use for the original
/// context once this conversion runs.
pub fn decode_pdata_to_observations(pdata: OtapPdata) -> Result<DecodeOutcome, CodecError> {
    let (_context, payload) = pdata.into_parts();
    let arrow_records: OtapArrowRecords = payload
        .try_into_with_default()
        .map_err(|e| CodecError::Decode(format!("{e}")))?;

    let OtapArrowRecords::Metrics(_) = &arrow_records else {
        return Ok(DecodeOutcome::default());
    };

    let view = OtapMetricsView::try_from(&arrow_records)
        .map_err(|e| CodecError::Decode(format!("{e}")))?;

    let mut acc = DecodeAccumulator::default();
    let mut skipped_non_scalar = 0usize;

    for resource in view.resources() {
        for scope in resource.scopes() {
            for metric in scope.metrics() {
                let name = String::from_utf8_lossy(metric.name()).into_owned();
                let Some(data) = metric.data() else {
                    continue;
                };
                // Gauge/Sum each expose their own concrete
                // `NumberDataPointView` implementation (different
                // types, both borrowing from `gauge`/`sum`) — walked
                // inline per branch via a generic helper rather than
                // collected into one `Vec<_>` first, since collecting
                // would need `gauge`/`sum` to outlive the branch that
                // produced them.
                if let Some(gauge) = data.as_gauge() {
                    for dp in gauge.data_points() {
                        acc.push_data_point(dp, &name)?;
                    }
                } else if let Some(sum) = data.as_sum() {
                    for dp in sum.data_points() {
                        acc.push_data_point(dp, &name)?;
                    }
                } else {
                    // Histogram / ExponentialHistogram / Summary: no
                    // single well-defined scalar — count, don't expand.
                    skipped_non_scalar += match data.value_type() {
                        MetricKind::Histogram => data
                            .as_histogram()
                            .map(|h| h.data_points().count())
                            .unwrap_or(0),
                        MetricKind::ExponentialHistogram => data
                            .as_exponential_histogram()
                            .map(|h| h.data_points().count())
                            .unwrap_or(0),
                        MetricKind::Summary => data
                            .as_summary()
                            .map(|s| s.data_points().count())
                            .unwrap_or(0),
                        _ => 0,
                    };
                }
            }
        }
    }

    Ok(DecodeOutcome {
        observations: acc.observations,
        skipped_non_scalar,
    })
}

/// Accumulates one `Observation` per data point while walking the real
/// `OtapMetricsView` — factored out of [`decode_pdata_to_observations`]
/// so the Gauge and Sum branches (whose `NumberDataPointView`
/// implementations are different concrete types, both borrowing from
/// the `gauge`/`sum` view that produced them) can share one push
/// method via a generic function instead of duplicating the per-data-
/// point body twice, or trying to unify their different concrete
/// iterator types into one `Vec` first (which doesn't compile —
/// `gauge`/`sum` would need to outlive the branch that produced them).
#[derive(Default)]
struct DecodeAccumulator {
    observations: Vec<Observation>,
}

impl DecodeAccumulator {
    /// Builds and appends one `Observation` from a data point — a
    /// no-op if the data point carries no value.
    ///
    /// Mirrors `decode.rs`'s `decode_batch` row-decode logic exactly
    /// (same `_asap_*` key recognition, same envelope-vs-scalar
    /// routing, same hard error on an unrecognized
    /// `_asap_sketch_type`/`_asap_encoding`), just reading attributes
    /// directly off the real `NumberDataPointView` instead of off
    /// Arrow columns.
    fn push_data_point<D: NumberDataPointView>(
        &mut self,
        dp: D,
        metric_name: &str,
    ) -> Result<(), CodecError> {
        let Some(value) = dp.value() else {
            return Ok(());
        };
        let value = match value {
            DpValue::Double(v) => v,
            DpValue::Integer(v) => v as f64,
        };
        let timestamp_ms = dp.time_unix_nano() / 1_000_000;

        let mut envelope_bytes: Option<Vec<u8>> = None;
        let mut sketch_type_raw: Option<String> = None;
        let mut agg_id: Option<u64> = None;
        let mut schema_version: Option<u32> = None;
        let mut window_start_ms: Option<u64> = None;
        let mut window_end_ms: Option<u64> = None;
        let mut encoding_raw: Option<String> = None;
        let mut labels: Vec<KeyValue> = Vec::new();

        for attr in dp.attributes() {
            let Some(val) = attr.value() else { continue };
            let key = String::from_utf8_lossy(attr.key()).into_owned();
            match key.as_str() {
                ATTR_ENVELOPE => envelope_bytes = val.as_bytes().map(<[u8]>::to_vec),
                ATTR_SKETCH_TYPE => {
                    sketch_type_raw = val
                        .as_string()
                        .map(|s| String::from_utf8_lossy(s).into_owned());
                }
                ATTR_AGG_ID => agg_id = val.as_int64().filter(|v| *v >= 0).map(|v| v as u64),
                ATTR_SCHEMA_VERSION => {
                    schema_version = val.as_int64().filter(|v| *v >= 0).map(|v| v as u32);
                }
                ATTR_WINDOW_START_MS => {
                    window_start_ms = val.as_int64().filter(|v| *v >= 0).map(|v| v as u64);
                }
                ATTR_WINDOW_END_MS => {
                    window_end_ms = val.as_int64().filter(|v| *v >= 0).map(|v| v as u64);
                }
                ATTR_ENCODING => {
                    encoding_raw = val
                        .as_string()
                        .map(|s| String::from_utf8_lossy(s).into_owned());
                }
                _ => {
                    // Non-reserved: treated as a per-row label. Only
                    // string-typed values are representable as a
                    // label (matches decode.rs's Utf8-only label
                    // column behavior) — a non-string, non-reserved
                    // attribute is dropped, same as the legacy path.
                    if let Some(s) = val.as_string() {
                        labels.push(KeyValue::new(key, String::from_utf8_lossy(s).into_owned()));
                    }
                }
            }
        }

        // `.filter(|b| !b.is_empty())`: defensive guard against the
        // same empty-but-present-Binary-cell class of bug this
        // module's encoder fixes on the way out (see module doc, "A
        // real bug this rewrite fixes") — an empty `_asap_envelope`
        // from some other producer still routes to the scalar path
        // here rather than an empty-payload envelope.
        let observation_value = match envelope_bytes.filter(|b| !b.is_empty()) {
            Some(payload) => {
                let sketch_type = match sketch_type_raw {
                    Some(s) => {
                        parse_sketch_type(0, &s).map_err(|e| CodecError::Decode(e.to_string()))?
                    }
                    None => crate::envelope::SketchType::Unspecified,
                };
                let encoding = match encoding_raw {
                    Some(s) => {
                        parse_encoding(0, &s).map_err(|e| CodecError::Decode(e.to_string()))?
                    }
                    None => crate::envelope::Encoding::Unspecified,
                };
                ObservationValue::envelope(SketchEnvelope {
                    schema_version: schema_version.unwrap_or(0),
                    sketch_type,
                    agg_id: agg_id.unwrap_or(0),
                    resource_labels: Vec::new(),
                    labels: labels.clone(),
                    window_start_ms: window_start_ms.unwrap_or(0),
                    window_end_ms: window_end_ms.unwrap_or(0),
                    encoding,
                    payload,
                    hash_spec: None,
                    metric_name: metric_name.to_string(),
                    count: 0,
                    aggregation_temporality: 0,
                    value,
                })
            }
            None => ObservationValue::float(value),
        };

        self.observations.push(Observation::new(
            timestamp_ms,
            metric_name.to_string(),
            Vec::new(), // resource_labels — no resource child batch modelled, matches decode.rs.
            labels,
            observation_value,
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Encoding, SketchType};
    use crate::observation::{KeyValue as Kv, ObservationValueKind};

    fn one_envelope(
        metric_name: &str,
        value: f64,
        attr_key: &str,
        attr_value: &str,
    ) -> SketchEnvelope {
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            agg_id: 0,
            resource_labels: vec![],
            labels: vec![Kv::new(attr_key, attr_value)],
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            encoding: Encoding::Unspecified,
            payload: Vec::new(),
            hash_spec: None,
            metric_name: metric_name.to_string(),
            count: 0,
            aggregation_temporality: 0,
            value,
        }
    }

    #[test]
    fn encode_then_decode_round_trips_a_scalar_metric() {
        let env = one_envelope("http_request_duration_ms", 42.5, "path", "/api");
        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");
        assert_eq!(
            pdata.signal_type(),
            otel_arrow_dfe_config::SignalType::Metrics
        );

        let outcome = decode_pdata_to_observations(pdata).expect("decode");
        assert_eq!(outcome.skipped_non_scalar, 0);
        assert_eq!(outcome.observations.len(), 1);
        let obs = &outcome.observations[0];
        assert_eq!(obs.metric, "http_request_duration_ms");
        assert_eq!(obs.value.kind, ObservationValueKind::Float);
        assert_eq!(obs.value.float, 42.5);
        assert_eq!(obs.labels.len(), 1);
        assert_eq!(obs.labels[0].key, "path");
        assert_eq!(obs.labels[0].value, "/api");
    }

    #[test]
    fn encode_then_decode_round_trips_a_sketch_envelope_carried_as_a_metric_attribute() {
        let mut env = one_envelope("sketch_stream", 0.0, "region", "us-east");
        env.sketch_type = SketchType::DDSketch;
        env.encoding = Encoding::ProtoFull;
        env.agg_id = 7;
        env.payload = vec![0xde, 0xad, 0xbe, 0xef];

        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");
        let outcome = decode_pdata_to_observations(pdata).expect("decode");
        assert_eq!(outcome.observations.len(), 1);
        let obs = &outcome.observations[0];
        assert_eq!(obs.value.kind, ObservationValueKind::Envelope);
        let decoded_env = obs.value.envelope.as_ref().expect("envelope");
        assert_eq!(decoded_env.payload, vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(decoded_env.sketch_type, SketchType::DDSketch);
        assert_eq!(decoded_env.encoding, Encoding::ProtoFull);
        assert_eq!(decoded_env.agg_id, 7);
        assert_eq!(decoded_env.window_start_ms, 1_000);
        assert_eq!(decoded_env.window_end_ms, 2_000);
    }

    /// The bug this rewrite fixes: an estimate-mode envelope (empty
    /// `payload`, the estimate rides in `value`) must round-trip as a
    /// scalar `Float` observation, not get misrouted through the
    /// envelope path with an empty payload.
    #[test]
    fn estimate_mode_envelope_round_trips_as_a_scalar_not_an_empty_envelope() {
        let env = one_envelope("p99_latency_ms", 123.4, "route", "/checkout");
        assert!(
            env.payload.is_empty(),
            "estimate-mode envelope: no sketch payload"
        );

        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");
        let outcome = decode_pdata_to_observations(pdata).expect("decode");
        assert_eq!(outcome.observations.len(), 1);
        let obs = &outcome.observations[0];
        assert_eq!(
            obs.value.kind,
            ObservationValueKind::Float,
            "estimate-mode envelope must decode as Float, not Envelope"
        );
        assert_eq!(obs.value.float, 123.4);
    }

    #[test]
    fn decode_returns_empty_outcome_for_zero_data_points() {
        let pdata = encode_envelopes_to_pdata(&[]).expect("encode empty");
        let outcome = decode_pdata_to_observations(pdata).expect("decode empty");
        assert!(outcome.observations.is_empty());
        assert_eq!(outcome.skipped_non_scalar, 0);
    }

    #[test]
    fn mixed_metric_names_are_rejected() {
        let env_a = one_envelope("a", 1.0, "k", "v");
        let env_b = one_envelope("b", 2.0, "k", "v");
        let err = encode_envelopes_to_pdata(&[env_a, env_b]).expect_err("mixed names rejected");
        assert!(matches!(err, CodecError::MixedMetricNames { .. }));
    }

    /// This is the fact `processor.rs`'s "There is exactly one
    /// transport: the pipeline" doc rests on: OTAP's real Arrow
    /// encoder (`encode_metrics_otap_batch`, which
    /// [`encode_envelopes_to_pdata`] calls) dictionary-encodes the
    /// metric name and every string-valued attribute key/value on its
    /// own, by construction — this adapter doesn't have to ask for
    /// it. If this ever regresses upstream (a schema change stops
    /// using `DataType::Dictionary` for these columns), the rationale
    /// for not reinventing `SeriesDictionary`'s SCHEMA/DICTIONARY/
    /// RECORD tiering on this path goes with it — this test exists so
    /// that regression is loud, not discovered by someone re-deriving
    /// it from scratch.
    #[test]
    fn real_otap_encoding_dictionary_encodes_metric_name_and_string_attributes() {
        use arrow_schema::DataType;
        use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

        let env = one_envelope("http_request_duration_ms", 42.5, "path", "/api");
        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");

        let (_context, payload) = pdata.into_parts();
        let arrow_records: OtapArrowRecords = payload
            .try_into_with_default()
            .expect("payload converts to real OtapArrowRecords");

        let metrics_batch = arrow_records
            .get(ArrowPayloadType::UnivariateMetrics)
            .expect("a UnivariateMetrics batch was populated");
        let metrics_schema = metrics_batch.schema();
        let name_type = metrics_schema
            .field_with_name("name")
            .expect("metrics batch has a name column")
            .data_type();
        assert!(
            matches!(name_type, DataType::Dictionary(_, _)),
            "expected the metric name column to be dictionary-encoded, got {name_type:?}"
        );

        let attrs_batch = arrow_records
            .get(ArrowPayloadType::NumberDpAttrs)
            .expect("a NumberDpAttrs batch was populated");
        let attrs_schema = attrs_batch.schema();
        for column in ["key", "str"] {
            let field_type = attrs_schema
                .field_with_name(column)
                .unwrap_or_else(|_| panic!("attrs batch has a {column} column"))
                .data_type();
            assert!(
                matches!(field_type, DataType::Dictionary(_, _)),
                "expected attrs column {column:?} to be dictionary-encoded, got {field_type:?}"
            );
        }
    }
}
