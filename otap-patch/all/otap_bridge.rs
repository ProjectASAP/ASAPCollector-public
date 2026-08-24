// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! Bridge between ASAP's flat, host-neutral [`OtapMetricRecords`] (the
//! shape `asap_precompute_rs::otap::records::flatten`/`decode_batch`
//! already know how to walk) and OTAP's real `OtapPdata` /
//! `OtapArrowRecords::Metrics` shape — the "one seam left" the crate
//! root README calls out.
//!
//! This is the piece that actually puts a self-describing sketch's
//! serialized bytes onto the wire *as an OTAP metric*: `encode_batch`
//! (already implemented, unchanged by this file) turns a
//! `SketchEnvelope` into an `OtapMetricRecords` whose per-row
//! attribute batch carries the envelope bytes as a `_asap_envelope`
//! `Bytes`-typed attribute (Strategy B, `otap/schema.rs`); this module
//! is the layer above that turns *that* into something OTAP's real
//! engine can carry as `Message::PData(OtapPdata)`.
//!
//! # Provenance / verification status
//!
//! Written against, and **build/lint/test-verified against**, upstream
//! `open-telemetry/otel-arrow` commit
//! `3e85c3460361446ebfce99e9f35fffd2dd5ab740` (2026-08-24): this file
//! plus `mod.rs` were staged as a real `crates/*` workspace member
//! (`asap-sketches-registry`, path-depping back to `asap-precompute-rs`
//! exactly as `Cargo.toml`'s own doc describes) inside a real clone of
//! that commit, and `cargo build` / `cargo clippy -D warnings` /
//! `cargo fmt --check` / `cargo test` (10/10, including two round-trip
//! tests through the real `encode_metrics_otap_batch`/`OtapMetricsView`
//! machinery — one of them the `_asap_envelope`-carrying "sketch as
//! binary inside a metric" case) all passed there. `otap-patch/`
//! itself still has no standalone build *in this repo* (no OTAP
//! Dataflow workspace checked out here by default — see the repo
//! README's note on this directory); the verification above happened
//! by staging into a separate, temporary checkout of the real
//! workspace, not by anything this repo's own build wires up. The
//! OTAP Dataflow crates were renamed `otap_df_*` -> `otel_arrow_dfe_*`
//! very recently and unreleased as of that commit
//! (`.chloggen/otel-arrow-dfe-crate-prefix.yaml`, issue #1848); this
//! file targets the new names — re-verify against whatever commit this
//! repo's own build script actually pins if that differs.
//!
//! # Scope
//!
//! What "handling a metric" means on decode splits into two cases,
//! decided by content, not by which OTLP metric type carried it:
//!
//! - **A data point carrying `_asap_envelope`** (this module's own
//!   encode output, or any other `asap_sketches` node's) — this
//!   module doesn't special-case it at all. It round-trips into
//!   `OtapMetricRecords` like any other attribute, and
//!   `decode_batch` (unchanged, downstream of this module) already
//!   recognizes `_asap_envelope` and produces an
//!   `ObservationValueKind::Envelope`-kind `Observation`;
//!   `Precompute::observe` already dispatches those internally to
//!   `observe_envelope` (merge as a pre-aggregated sketch), never
//!   expanding them to scalar samples. So "sketch as a binary inside
//!   an OTAP metric" already gets sketch-side handling for free, by
//!   construction, with no branching needed here.
//! - **A genuine (non-envelope) OTLP metric** — real telemetry.
//!   Gauge/Sum (`NumberDataPoints`) are handled normally: each data
//!   point becomes a scalar `Observation`, the shape both
//!   `Observation` and `OtapMetricRecords` (well-known
//!   `time_unix_nano`/`metric`/`value` columns) already assume.
//!   Histogram / ExponentialHistogram / Summary data points are
//!   skipped (counted, not silently dropped — see
//!   [`DecodeOutcome::skipped_non_scalar`]) rather than expanded,
//!   since there's no single well-defined scalar to extract from a
//!   bucket/quantile set without picking a lossy expansion strategy
//!   this module doesn't want to own.
//!
//! On encode, every row is assumed to share one metric name — an
//! `AsapSketchesProcessor` instance has exactly one
//! `PluginConfig::output_metric_name`, so this holds by construction
//! for anything `encode_batch` itself produces; [`otap_metric_records_to_pdata`]
//! still checks it explicitly and errors loudly on a real mismatch
//! rather than silently dropping rows for the "wrong" metric.
//!
//! Resource and scope are not modelled — [`OtapMetricRecords`] itself
//! deliberately omits them (see that type's own doc); every emitted
//! metric attaches to an empty `Resource` / unnamed `Scope`.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, Float64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
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

use asap_precompute_rs::otap::records::{
    ATTR_BATCH_BYTES, ATTR_BATCH_INT, ATTR_BATCH_KEY, ATTR_BATCH_PARENT_ID, ATTR_BATCH_STR,
};
use asap_precompute_rs::otap::{
    COLUMN_METRIC, COLUMN_TIME_UNIX_NANO, COLUMN_VALUE, OtapMetricRecords,
};

/// Failure modes for [`otap_metric_records_to_pdata`] / [`pdata_to_otap_metric_records`].
#[derive(Debug, Error)]
pub enum BridgeError {
    /// A required column was missing or had the wrong Arrow type.
    #[error("otap bridge: column {column:?} on batch {batch:?} missing or wrong type")]
    BadColumn {
        /// Which sibling batch.
        batch: &'static str,
        /// Column name.
        column: &'static str,
    },
    /// More than one distinct `metric` name appeared in one
    /// `OtapMetricRecords.metrics` batch — an
    /// `AsapSketchesProcessor` instance has exactly one
    /// `output_metric_name`, so this indicates the caller fed rows
    /// from more than one processor instance into a single call.
    #[error(
        "otap bridge: one OtapMetricRecords batch carries more than one metric name ({first:?} and {second:?} seen)"
    )]
    MixedMetricNames {
        /// First metric name seen.
        first: String,
        /// A second, different metric name seen in the same batch.
        second: String,
    },
    /// Building the real `OtapArrowRecords::Metrics` batch failed.
    #[error("otap bridge: encoding real OTAP metrics batch failed: {0}")]
    Encode(String),
    /// Reading the real `OtapArrowRecords::Metrics` batch failed.
    #[error("otap bridge: reading real OTAP metrics batch failed: {0}")]
    Decode(String),
    /// Constructing the flat output `RecordBatch` failed.
    #[error("otap bridge: arrow record-batch construction failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
}

// ============================================================================
// Encode direction: OtapMetricRecords -> OtapPdata
// ============================================================================

/// Converts an [`OtapMetricRecords`] (as `encode_batch` produces it)
/// into a real `OtapPdata` carrying an `OtapArrowRecords::Metrics`
/// payload — the actual "put the sketch envelope bytes onto an OTAP
/// metric" step.
///
/// Builds a fresh, contextless `OtapPdata` (`OtapPdata::new_todo_context`)
/// rather than propagating any single input message's context: the
/// plugin's flush ticker emits one window's worth of envelopes on its
/// own wall-clock schedule, decoupled from any specific triggering
/// input message, so there is no single Ack/Nack chain to attach the
/// output to (the same reasoning any periodic/windowed aggregator's
/// output would follow).
pub fn otap_metric_records_to_pdata(records: &OtapMetricRecords) -> Result<OtapPdata, BridgeError> {
    let view = AsapMetricsView::try_new(records)?;
    let arrow_records =
        encode_metrics_otap_batch(&view).map_err(|e| BridgeError::Encode(e.to_string()))?;
    let payload: OtapPayload = arrow_records.into();
    Ok(OtapPdata::new_todo_context(payload))
}

/// Zero-copy-ish adapter presenting an [`OtapMetricRecords`]'s two
/// flat batches as a `MetricsView` — one Resource (empty), one Scope
/// (unnamed), one Metric (`records.metrics`'s single distinct `metric`
/// value), one Gauge, N `NumberDataPoint`s (one per row).
struct AsapMetricsView {
    metric_name: String,
    n_rows: usize,
    time_col: UInt64Array,
    value_col: Float64Array,
    /// `attr_index[parent_id]` = row indices into `records.attributes`
    /// carrying that parent's attributes. Precomputed once so per-data-point
    /// `attributes()` calls don't rescan the whole attribute batch.
    attr_index: BTreeMap<u32, Vec<usize>>,
    attr_key_col: StringArray,
    attr_bytes_col: Option<BinaryArray>,
    attr_str_col: Option<StringArray>,
    attr_int_col: Option<UInt64Array>,
}

impl<'a> AsapMetricsView {
    fn try_new(records: &'a OtapMetricRecords) -> Result<Self, BridgeError> {
        let n_rows = records.metrics.num_rows();

        let metric_col = require_string(&records.metrics, "metrics", COLUMN_METRIC)?;
        let mut metric_name: Option<String> = None;
        for row in 0..n_rows {
            let name = metric_col.value(row);
            match &metric_name {
                None => metric_name = Some(name.to_string()),
                Some(seen) if seen == name => {}
                Some(seen) => {
                    return Err(BridgeError::MixedMetricNames {
                        first: seen.clone(),
                        second: name.to_string(),
                    });
                }
            }
        }

        let time_col = require_uint64(&records.metrics, "metrics", COLUMN_TIME_UNIX_NANO)?.clone();
        let value_col = require_float64(&records.metrics, "metrics", COLUMN_VALUE)?.clone();
        let parent_col = require_uint32(&records.metrics, "metrics", ATTR_BATCH_PARENT_ID)?;

        let attr_parent_col =
            require_uint32(&records.attributes, "attributes", ATTR_BATCH_PARENT_ID)?;
        let attr_key_col =
            require_string(&records.attributes, "attributes", ATTR_BATCH_KEY)?.clone();
        let attr_bytes_col = optional_binary(&records.attributes, ATTR_BATCH_BYTES)?;
        let attr_str_col = optional_string(&records.attributes, ATTR_BATCH_STR)?;
        let attr_int_col = optional_uint64(&records.attributes, ATTR_BATCH_INT)?;

        let mut attr_index: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
        for row in 0..records.attributes.num_rows() {
            if attr_parent_col.is_null(row) {
                continue;
            }
            attr_index
                .entry(attr_parent_col.value(row))
                .or_default()
                .push(row);
        }
        // Not currently used beyond validating the column exists —
        // parent_id on `metrics` isn't needed for row->attribute
        // linking here (each metrics row's own index doubles as its
        // parent_id by `encode_batch`'s own convention), but keep the
        // column checked so a future divergence surfaces as a
        // decode-time BadColumn error rather than silent misjoin.
        let _ = parent_col;

        Ok(Self {
            metric_name: metric_name.unwrap_or_default(),
            n_rows,
            time_col,
            value_col,
            attr_index,
            attr_key_col,
            attr_bytes_col,
            attr_str_col,
            attr_int_col,
        })
    }

    fn attributes_for_row(&self, parent_id: u32) -> Vec<AsapAttribute<'_>> {
        let Some(rows) = self.attr_index.get(&parent_id) else {
            return Vec::new();
        };
        rows.iter()
            .filter_map(|&row| {
                let key = self.attr_key_col.value(row);
                let value = attr_row_value(
                    self.attr_bytes_col.as_ref(),
                    self.attr_str_col.as_ref(),
                    self.attr_int_col.as_ref(),
                    row,
                )?;
                Some(AsapAttribute { key, value })
            })
            .collect()
    }
}

impl MetricsView for AsapMetricsView {
    type ResourceMetrics<'res>
        = AsapResourceMetricsView<'res>
    where
        Self: 'res;
    type ResourceMetricsIter<'res>
        = std::vec::IntoIter<AsapResourceMetricsView<'res>>
    where
        Self: 'res;

    fn resources(&self) -> Self::ResourceMetricsIter<'_> {
        if self.n_rows == 0 {
            Vec::new().into_iter()
        } else {
            vec![AsapResourceMetricsView { view: self }].into_iter()
        }
    }
}

struct AsapResourceMetricsView<'a> {
    view: &'a AsapMetricsView,
}

impl<'a> ResourceMetricsView for AsapResourceMetricsView<'a> {
    type Resource<'res>
        = AsapNoResource
    where
        Self: 'res;
    type ScopeMetrics<'scp>
        = AsapScopeMetricsView<'scp>
    where
        Self: 'scp;
    type ScopesIter<'scp>
        = std::vec::IntoIter<AsapScopeMetricsView<'scp>>
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

struct AsapScopeMetricsView<'a> {
    view: &'a AsapMetricsView,
}

impl<'a> ScopeMetricsView for AsapScopeMetricsView<'a> {
    type Scope<'scp>
        = AsapNoScope
    where
        Self: 'scp;
    type Metric<'met>
        = AsapMetricView<'met>
    where
        Self: 'met;
    type MetricIter<'met>
        = std::vec::IntoIter<AsapMetricView<'met>>
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

struct AsapMetricView<'a> {
    view: &'a AsapMetricsView,
}

impl<'a> MetricView for AsapMetricView<'a> {
    type Data<'dat>
        = AsapDataView<'dat>
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

struct AsapDataView<'a> {
    view: &'a AsapMetricsView,
}

impl<'a> DataView<'a> for AsapDataView<'a> {
    type Gauge<'gauge>
        = AsapGaugeView<'gauge>
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

struct AsapGaugeView<'a> {
    view: &'a AsapMetricsView,
}

impl<'a> GaugeView for AsapGaugeView<'a> {
    type NumberDataPoint<'dp>
        = AsapNumberDataPointView<'dp>
    where
        Self: 'dp;
    type NumberDataPointIter<'dp>
        = std::vec::IntoIter<AsapNumberDataPointView<'dp>>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::NumberDataPointIter<'_> {
        (0..self.view.n_rows)
            .map(|row| AsapNumberDataPointView {
                view: self.view,
                row,
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

struct AsapNumberDataPointView<'a> {
    view: &'a AsapMetricsView,
    row: usize,
}

impl<'a> NumberDataPointView for AsapNumberDataPointView<'a> {
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
        self.view.time_col.value(self.row)
    }

    fn value(&self) -> Option<DpValue> {
        if self.view.value_col.is_null(self.row) {
            None
        } else {
            Some(DpValue::Double(self.view.value_col.value(self.row)))
        }
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        // `encode_batch`'s own convention: the metrics row's ordinal
        // index doubles as the parent_id its attribute rows join
        // against (see records.rs's `ATTR_BATCH_PARENT_ID` doc).
        self.view.attributes_for_row(self.row as u32).into_iter()
    }

    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        Vec::new().into_iter()
    }

    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(0)
    }
}

/// One typed attribute value ASAP's flat attribute batch can carry —
/// mirrors `records.rs`'s internal `AttrValue` three-way union
/// (`bytes` / `str` / `int` columns).
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

/// Reads the value at `row` from whichever of the three typed-value
/// columns actually has a non-null entry there — mirrors
/// `records.rs`'s private `build_attr_index` join logic (bytes, then
/// str, then int; exactly one is non-null per row by construction).
fn attr_row_value<'a>(
    bytes: Option<&'a BinaryArray>,
    strs: Option<&'a StringArray>,
    ints: Option<&'a UInt64Array>,
    row: usize,
) -> Option<AsapAnyValue<'a>> {
    if let Some(arr) = bytes {
        if !arr.is_null(row) {
            return Some(AsapAnyValue::Bytes(arr.value(row)));
        }
    }
    if let Some(arr) = strs {
        if !arr.is_null(row) {
            return Some(AsapAnyValue::Str(arr.value(row)));
        }
    }
    if let Some(arr) = ints {
        if !arr.is_null(row) {
            return Some(AsapAnyValue::Int(arr.value(row)));
        }
    }
    None
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
    fn span_id(&self) -> Option<&otel_arrow_dfe_pdata_views::SpanId> {
        match *self {}
    }
    fn trace_id(&self) -> Option<&otel_arrow_dfe_pdata_views::TraceId> {
        match *self {}
    }
}

enum AsapNoSum {}
impl SumView for AsapNoSum {
    type NumberDataPoint<'dp>
        = AsapNumberDataPointView<'dp>
    where
        Self: 'dp;
    type NumberDataPointIter<'dp>
        = std::vec::IntoIter<AsapNumberDataPointView<'dp>>
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
    type ExponentialHistogramDataPoint<'edp>
        = AsapNoExpHistogramDataPoint
    where
        Self: 'edp;
    type ExponentialHistogramDataPointIter<'edp>
        = std::vec::IntoIter<AsapNoExpHistogramDataPoint>
    where
        Self: 'edp;
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
    type Buckets<'b>
        = AsapNoBuckets
    where
        Self: 'b;
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
    fn flags(&self) -> DataPointFlags {
        match *self {}
    }
    fn exemplars(&self) -> Self::ExemplarIter<'_> {
        match *self {}
    }
    fn min(&self) -> Option<f64> {
        match *self {}
    }
    fn max(&self) -> Option<f64> {
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
    type ValueAtQuantile<'vaq>
        = AsapNoValueAtQuantile
    where
        Self: 'vaq;
    type ValueAtQuantileIter<'vaq>
        = std::vec::IntoIter<AsapNoValueAtQuantile>
    where
        Self: 'vaq;
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
    fn sum(&self) -> f64 {
        match *self {}
    }
    fn quantile_values(&self) -> Self::ValueAtQuantileIter<'_> {
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
// Decode direction: OtapPdata -> OtapMetricRecords
// ============================================================================

/// Outcome of [`pdata_to_otap_metric_records`]: the reconstructed flat
/// batch pair (`None` if `pdata` carried a non-Metrics signal or truly
/// zero metric rows), plus a count of rows this call had to skip.
pub struct DecodeOutcome {
    /// `None` when `pdata` wasn't Metrics, or decoded to zero rows.
    pub records: Option<OtapMetricRecords>,
    /// Histogram / ExponentialHistogram / Summary data points seen and
    /// skipped — not silently dropped, see this module's doc "Scope"
    /// section for why they aren't expanded.
    pub skipped_non_scalar: usize,
}

/// Accumulates flat rows across every resource/scope/metric/data-point
/// while walking the real `OtapMetricsView` — factored out of
/// [`pdata_to_otap_metric_records`] so the Gauge and Sum branches
/// (whose `NumberDataPointView` implementations are different
/// concrete types, both borrowing from the `gauge`/`sum` view that
/// produced them) can share one push method via a generic function
/// instead of duplicating the per-data-point body twice, or trying
/// (and failing to borrow-check) to collect both into one `Vec<_>`
/// before `gauge`/`sum` go out of scope.
#[derive(Default)]
struct DecodeAccumulator {
    time_unix_nano: Vec<u64>,
    metric_names: Vec<String>,
    values: Vec<f64>,
    attr_parent_ids: Vec<u32>,
    attr_keys: Vec<String>,
    attr_strs: Vec<Option<String>>,
    attr_ints: Vec<Option<u64>>,
    attr_bytes: Vec<Option<Vec<u8>>>,
    next_parent_id: u32,
}

impl DecodeAccumulator {
    /// Appends one data point's row (and its attribute rows, if any)
    /// — skipped (not pushed at all) if it carries no value.
    fn push_data_point<D: NumberDataPointView>(&mut self, dp: D, metric_name: &str) {
        let Some(value) = dp.value() else { return };
        let value = match value {
            DpValue::Double(v) => v,
            DpValue::Integer(v) => v as f64,
        };

        let parent_id = self.next_parent_id;
        self.next_parent_id += 1;

        self.time_unix_nano.push(dp.time_unix_nano());
        self.metric_names.push(metric_name.to_string());
        self.values.push(value);

        for attr in dp.attributes() {
            let Some(val) = attr.value() else { continue };
            self.attr_parent_ids.push(parent_id);
            self.attr_keys
                .push(String::from_utf8_lossy(attr.key()).into_owned());
            let (mut s, mut i, mut b) = (None, None, None);
            match val.value_type() {
                ValueType::String => {
                    s = val
                        .as_string()
                        .map(|v| String::from_utf8_lossy(v).into_owned());
                }
                ValueType::Bytes => {
                    b = val.as_bytes().map(|v| v.to_vec());
                }
                ValueType::Int64 => {
                    // ASAP's int attribute column is unsigned; a
                    // genuinely negative int attribute (rare for
                    // label-shaped data) isn't representable, so it's
                    // stringified instead of silently reinterpreted
                    // as a huge positive value.
                    match val.as_int64() {
                        Some(v) if v >= 0 => i = Some(v as u64),
                        Some(v) => s = Some(v.to_string()),
                        None => {}
                    }
                }
                ValueType::Double => {
                    s = val.as_double().map(|v| v.to_string());
                }
                ValueType::Bool => {
                    s = val.as_bool().map(|v| v.to_string());
                }
                ValueType::Empty | ValueType::Array | ValueType::KeyValueList => {
                    // Not representable in ASAP's flat label model;
                    // drop this one attribute (the data point itself
                    // is still kept).
                    self.attr_parent_ids.pop();
                    self.attr_keys.pop();
                    continue;
                }
            }
            self.attr_strs.push(s);
            self.attr_ints.push(i);
            self.attr_bytes.push(b);
        }
    }
}

/// Converts a real `OtapPdata` into ASAP's flat [`OtapMetricRecords`]
/// shape (feeding `decode_batch` on the producer role's ingest path).
///
/// Accepts `pdata` by value (consumed) — the caller's `Message::PData`
/// match arm already owns it and has no further use for the original
/// context once this conversion runs.
pub fn pdata_to_otap_metric_records(pdata: OtapPdata) -> Result<DecodeOutcome, BridgeError> {
    let (_context, payload) = pdata.into_parts();
    let arrow_records: OtapArrowRecords = payload
        .try_into_with_default()
        .map_err(|e| BridgeError::Decode(format!("{e}")))?;

    let OtapArrowRecords::Metrics(_) = &arrow_records else {
        return Ok(DecodeOutcome {
            records: None,
            skipped_non_scalar: 0,
        });
    };

    let view = OtapMetricsView::try_from(&arrow_records)
        .map_err(|e| BridgeError::Decode(format!("{e}")))?;

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
                        acc.push_data_point(dp, &name);
                    }
                } else if let Some(sum) = data.as_sum() {
                    for dp in sum.data_points() {
                        acc.push_data_point(dp, &name);
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

    if acc.time_unix_nano.is_empty() {
        return Ok(DecodeOutcome {
            records: None,
            skipped_non_scalar,
        });
    }

    let parent_ids: Vec<u32> = (0..acc.time_unix_nano.len() as u32).collect();
    let metrics = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
            Field::new(COLUMN_METRIC, DataType::Utf8, false),
            Field::new(COLUMN_VALUE, DataType::Float64, false),
            Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
        ])),
        vec![
            Arc::new(UInt64Array::from(acc.time_unix_nano)),
            Arc::new(StringArray::from(acc.metric_names)),
            Arc::new(Float64Array::from(acc.values)),
            Arc::new(UInt32Array::from(parent_ids)),
        ],
    )?;

    let attributes = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
            Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
            Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
            Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
        ])),
        vec![
            Arc::new(UInt32Array::from(acc.attr_parent_ids)),
            Arc::new(StringArray::from(acc.attr_keys)),
            Arc::new(StringArray::from(acc.attr_strs)),
            Arc::new(UInt64Array::from(acc.attr_ints)),
            Arc::new(BinaryArray::from_opt_vec(
                acc.attr_bytes.iter().map(|b| b.as_deref()).collect(),
            )),
        ],
    )?;

    Ok(DecodeOutcome {
        records: Some(OtapMetricRecords {
            metrics,
            attributes,
        }),
        skipped_non_scalar,
    })
}

// -- Small typed-column helpers (mirrors records.rs's own private ones) -----

fn require_string<'a>(
    batch: &'a RecordBatch,
    which: &'static str,
    column: &'static str,
) -> Result<&'a StringArray, BridgeError> {
    batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or(BridgeError::BadColumn {
            batch: which,
            column,
        })
}

fn require_uint64<'a>(
    batch: &'a RecordBatch,
    which: &'static str,
    column: &'static str,
) -> Result<&'a UInt64Array, BridgeError> {
    batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or(BridgeError::BadColumn {
            batch: which,
            column,
        })
}

fn require_uint32<'a>(
    batch: &'a RecordBatch,
    which: &'static str,
    column: &'static str,
) -> Result<&'a UInt32Array, BridgeError> {
    batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
        .ok_or(BridgeError::BadColumn {
            batch: which,
            column,
        })
}

fn require_float64<'a>(
    batch: &'a RecordBatch,
    which: &'static str,
    column: &'static str,
) -> Result<&'a Float64Array, BridgeError> {
    batch
        .column_by_name(column)
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or(BridgeError::BadColumn {
            batch: which,
            column,
        })
}

fn optional_string(
    batch: &RecordBatch,
    column: &'static str,
) -> Result<Option<StringArray>, BridgeError> {
    match batch.column_by_name(column) {
        None => Ok(None),
        Some(c) => c
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .map(Some)
            .ok_or(BridgeError::BadColumn {
                batch: "attributes",
                column,
            }),
    }
}

fn optional_uint64(
    batch: &RecordBatch,
    column: &'static str,
) -> Result<Option<UInt64Array>, BridgeError> {
    match batch.column_by_name(column) {
        None => Ok(None),
        Some(c) => c
            .as_any()
            .downcast_ref::<UInt64Array>()
            .cloned()
            .map(Some)
            .ok_or(BridgeError::BadColumn {
                batch: "attributes",
                column,
            }),
    }
}

fn optional_binary(
    batch: &RecordBatch,
    column: &'static str,
) -> Result<Option<BinaryArray>, BridgeError> {
    match batch.column_by_name(column) {
        None => Ok(None),
        Some(c) => c
            .as_any()
            .downcast_ref::<BinaryArray>()
            .cloned()
            .map(Some)
            .ok_or(BridgeError::BadColumn {
                batch: "attributes",
                column,
            }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BinaryArray, Float64Array, StringArray, UInt32Array, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

    /// One-row `OtapMetricRecords`: `metric_name` at `value`, one
    /// string attribute (`key` = `value`) and, if `envelope_bytes` is
    /// `Some`, a second attribute carrying it under `_asap_envelope`
    /// (Bytes-typed) — the "sketch shipped as binary inside an OTAP
    /// metric" case this bridge's whole "Scope" doc section is about.
    fn one_row_records(
        metric_name: &str,
        value: f64,
        attr_key: &str,
        attr_value: &str,
        envelope_bytes: Option<&[u8]>,
    ) -> OtapMetricRecords {
        let metrics = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Float64, false),
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(vec![1_000_000_000_u64])),
                Arc::new(StringArray::from(vec![metric_name])),
                Arc::new(Float64Array::from(vec![value])),
                Arc::new(UInt32Array::from(vec![0_u32])),
            ],
        )
        .expect("metrics batch");

        let n_attr_rows = if envelope_bytes.is_some() { 2 } else { 1 };
        let mut parent_ids = vec![0_u32; n_attr_rows];
        let mut keys = vec![attr_key.to_string()];
        let mut strs: Vec<Option<String>> = vec![Some(attr_value.to_string())];
        let mut ints: Vec<Option<u64>> = vec![None];
        let mut bytes: Vec<Option<&[u8]>> = vec![None];
        if let Some(env) = envelope_bytes {
            keys.push("_asap_envelope".to_string());
            strs.push(None);
            ints.push(None);
            bytes.push(Some(env));
        }
        parent_ids.truncate(keys.len());

        let attributes = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
                Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
                Field::new(ATTR_BATCH_STR, DataType::Utf8, true),
                Field::new(ATTR_BATCH_INT, DataType::UInt64, true),
                Field::new(ATTR_BATCH_BYTES, DataType::Binary, true),
            ])),
            vec![
                Arc::new(UInt32Array::from(parent_ids)),
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(strs)),
                Arc::new(UInt64Array::from(ints)),
                Arc::new(BinaryArray::from_opt_vec(bytes)),
            ],
        )
        .expect("attributes batch");

        OtapMetricRecords {
            metrics,
            attributes,
        }
    }

    #[test]
    fn encode_then_decode_round_trips_a_scalar_metric() {
        let records = one_row_records("http_request_duration_ms", 42.5, "path", "/api", None);

        let pdata = otap_metric_records_to_pdata(&records).expect("encode to real OtapPdata");
        assert_eq!(
            pdata.signal_type(),
            otel_arrow_dfe_config::SignalType::Metrics
        );

        let outcome = pdata_to_otap_metric_records(pdata).expect("decode real OtapPdata");
        assert_eq!(outcome.skipped_non_scalar, 0);
        let decoded = outcome.records.expect("one row decoded back");
        assert_eq!(decoded.metrics.num_rows(), 1);

        let metric_col = decoded
            .metrics
            .column_by_name(COLUMN_METRIC)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(metric_col.value(0), "http_request_duration_ms");

        let value_col = decoded
            .metrics
            .column_by_name(COLUMN_VALUE)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(value_col.value(0), 42.5);

        assert_eq!(decoded.attributes.num_rows(), 1);
        let key_col = decoded
            .attributes
            .column_by_name(ATTR_BATCH_KEY)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_col.value(0), "path");
        let str_col = decoded
            .attributes
            .column_by_name(ATTR_BATCH_STR)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(str_col.value(0), "/api");
    }

    #[test]
    fn encode_then_decode_round_trips_a_sketch_envelope_carried_as_a_metric_attribute() {
        // The "self-describing sketch binary inside an OTAP metric"
        // case: an `_asap_envelope`-tagged Bytes attribute must
        // survive the round trip byte-for-byte, alongside an ordinary
        // string label on the same data point.
        let envelope_bytes: Vec<u8> = vec![1, 2, 3, 4, 250, 251, 252, 253];
        let records = one_row_records(
            "http_request_duration_ms",
            0.0,
            "path",
            "/api",
            Some(&envelope_bytes),
        );

        let pdata = otap_metric_records_to_pdata(&records).expect("encode to real OtapPdata");
        let outcome = pdata_to_otap_metric_records(pdata).expect("decode real OtapPdata");
        let decoded = outcome.records.expect("one row decoded back");

        assert_eq!(decoded.attributes.num_rows(), 2);
        let keys = decoded
            .attributes
            .column_by_name(ATTR_BATCH_KEY)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bytes_col = decoded
            .attributes
            .column_by_name(ATTR_BATCH_BYTES)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        let envelope_row = (0..decoded.attributes.num_rows())
            .find(|&row| keys.value(row) == "_asap_envelope")
            .expect("_asap_envelope attribute present");
        assert!(!bytes_col.is_null(envelope_row));
        assert_eq!(bytes_col.value(envelope_row), envelope_bytes.as_slice());
    }

    #[test]
    fn decode_returns_none_records_for_zero_rows() {
        let records = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(COLUMN_TIME_UNIX_NANO, DataType::UInt64, false),
                Field::new(COLUMN_METRIC, DataType::Utf8, false),
                Field::new(COLUMN_VALUE, DataType::Float64, false),
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(Vec::<u64>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(Float64Array::from(Vec::<f64>::new())),
                Arc::new(UInt32Array::from(Vec::<u32>::new())),
            ],
        )
        .expect("empty metrics batch");
        let attributes = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(ATTR_BATCH_PARENT_ID, DataType::UInt32, false),
                Field::new(ATTR_BATCH_KEY, DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt32Array::from(Vec::<u32>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
        )
        .expect("empty attributes batch");
        let empty = OtapMetricRecords {
            metrics: records,
            attributes,
        };

        let pdata = otap_metric_records_to_pdata(&empty).expect("encode empty");
        let outcome = pdata_to_otap_metric_records(pdata).expect("decode empty");
        assert!(outcome.records.is_none());
        assert_eq!(outcome.skipped_non_scalar, 0);
    }

    /// This is the fact `mod.rs`'s "There is exactly one transport: the
    /// pipeline" doc rests on: OTAP's real Arrow encoder
    /// (`encode_metrics_otap_batch`, which [`otap_metric_records_to_pdata`]
    /// calls) dictionary-encodes the metric name and every string-valued
    /// attribute key/value on its own, by construction — this adapter
    /// doesn't have to ask for it. If this ever regresses upstream (a
    /// schema change stops using `DataType::Dictionary` for these
    /// columns), the module doc's rationale for not reinventing
    /// `SeriesDictionary`'s SCHEMA/DICTIONARY/RECORD tiering on this
    /// path goes with it — this test exists so that regression is loud,
    /// not discovered by someone re-deriving it from scratch.
    #[test]
    fn real_otap_encoding_dictionary_encodes_metric_name_and_string_attributes() {
        let records = one_row_records("http_request_duration_ms", 42.5, "path", "/api", None);
        let pdata = otap_metric_records_to_pdata(&records).expect("encode to real OtapPdata");

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
