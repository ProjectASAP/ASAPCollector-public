// Copyright The ASAP Authors
// SPDX-License-Identifier: MIT

//! Direct `SketchEnvelope <-> OtapPdata` binding — the real `OtapPdata`
//! is built straight from a `&[SketchEnvelope]` slice, and a real
//! `OtapPdata` is decoded straight into `Vec<Observation>`, by
//! implementing OTAP's own `MetricsView` over `&[SketchEnvelope]`.
//!
//! # Native protocol mapping
//!
//! The design in `docs/data_model.md` is represented by OTAP's own joins:
//! SCHEMA attributes are children of a Resource, DICTIONARY/LABELS are
//! children of an instrumentation Scope, and sketch RECORD fields are
//! SummaryDataPoint attributes. A stream-scoped [`OtapSketchEncoder`] assigns stable
//! series IDs across flushes. Arrow IPC schema and string dictionaries are
//! retained by OTAP's transport producer; resource and scope parent rows are
//! still present in each independently valid `OtapPdata` message.
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
//! - **A Summary data point carrying `sketch.envelope`** (this module's own
//!   encode output, or any other `asap_sketches` node's) routes
//!   through `ObservationValueKind::Envelope`; `Precompute::observe`
//!   already dispatches those to `observe_envelope` (merge as a
//!   pre-aggregated sketch).
//! - **A genuine (non-envelope) OTLP metric** — Gauge/Sum data points
//!   become scalar `Observation`s. Histogram/ExponentialHistogram/
//!   Summary data points are skipped (counted, not silently dropped —
//!   see [`DecodeOutcome::skipped_non_scalar`]).
//!
//! Multiple metric names are valid: each distinct series is represented by
//! its own Scope and Metric beneath the aggregation-plan Resource.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use asap_types::plan::materialized_view::ViewStateKey;

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

use crate::config::{sketch_size_string, SketchParams};
use crate::envelope::{SketchEnvelope, SketchType, ViewIdentity};
use crate::observation::{KeyValue, Observation, ObservationValue, SourceDescriptor};

use super::schema::{
    ATTR_ENVELOPE, ATTR_SCHEMA_VERSION, ATTR_SERIES_ID, ATTR_SKETCH_SIZE, ATTR_SKETCH_TYPE,
    ATTR_WINDOW_END_MS, ATTR_WINDOW_START_MS, META_NODE_ID, META_PLAN_ID, META_VIEW_STATE_KEY,
    SERIES_SCOPE_NAME,
};

/// Failure modes for [`encode_envelopes_to_pdata`] / [`decode_pdata_to_observations`].
#[derive(Debug, Error)]
pub enum CodecError {
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
    OtapSketchEncoder::default().encode(envelopes)
}

/// Stream-scoped encoder that assigns one stable `series_id` to each
/// `(metric, labels)` identity of a hand-configured sketch for the lifetime
/// of an OTAP output. A collector view's states carry no series id: they keep
/// the Resource, Scope and point attributes they arrived with.
pub struct OtapSketchEncoder {
    next_series_id: u32,
    series_ids: HashMap<String, u32>,
    sketch_params: Option<SketchParams>,
}

impl Default for OtapSketchEncoder {
    fn default() -> Self {
        Self {
            // OTAP's sparse attribute encoder elides an all-zero integer
            // column, so zero cannot be the first externally visible ID.
            next_series_id: 1,
            series_ids: HashMap::new(),
            sketch_params: None,
        }
    }
}

impl OtapSketchEncoder {
    /// Creates an encoder that can populate the optional SCHEMA size field.
    pub fn with_sketch_params(sketch_params: SketchParams) -> Self {
        Self {
            sketch_params: Some(sketch_params),
            ..Self::default()
        }
    }

    /// Encodes one flush while retaining series identity across later flushes.
    pub fn encode(&mut self, envelopes: &[SketchEnvelope]) -> Result<OtapPdata, CodecError> {
        for env in envelopes {
            validate_self_describing_payload(env.sketch_type, &env.payload)
                .map_err(CodecError::Encode)?;
        }
        let series_ids = envelopes
            .iter()
            .map(|env| match env.view {
                Some(_) => 0,
                None => self.series_id_for(env),
            })
            .collect::<Vec<_>>();
        let view =
            AsapMetricsView::from_envelopes(envelopes, &series_ids, self.sketch_params.as_ref());
        let arrow_records =
            encode_metrics_otap_batch(&view).map_err(|e| CodecError::Encode(e.to_string()))?;
        let payload: OtapPayload = arrow_records.into();
        Ok(OtapPdata::new_todo_context(payload))
    }

    fn series_id_for(&mut self, env: &SketchEnvelope) -> u32 {
        let mut key = format!("{}:{}:", env.metric_name.len(), env.metric_name);
        append_labels_to_key(&mut key, &env.resource_labels);
        append_labels_to_key(&mut key, &env.labels);
        if let Some(series_id) = self.series_ids.get(&key) {
            return *series_id;
        }
        let series_id = self.next_series_id;
        self.next_series_id = self.next_series_id.saturating_add(1);
        self.series_ids.insert(key, series_id);
        series_id
    }
}

/// Enforces the native OTAP carrier contract for sketch records: a
/// non-empty `sketch.envelope` must decode as the sketchlib ASAPv1 envelope
/// of the declared `sketch.type`.
fn validate_self_describing_payload(sketch_type: SketchType, payload: &[u8]) -> Result<(), String> {
    if payload.is_empty() {
        return Ok(());
    }
    if sketch_type == SketchType::Unspecified {
        return Err("sketch.envelope requires a sketch.type".into());
    }
    if sketch_type == SketchType::Exact {
        // Exact windows can be large; their consumer decodes them once.
        return Ok(());
    }
    let decoded = crate::sketches::decode_snapshot(sketch_type, payload).map(drop);
    decoded.map_err(|error| format!("invalid {} sketch.envelope: {error}", sketch_type.name()))
}

fn append_labels_to_key(key: &mut String, labels: &[KeyValue]) {
    let mut labels = labels.iter().collect::<Vec<_>>();
    labels.sort_by(|a, b| a.key.cmp(&b.key).then(a.value.cmp(&b.value)));
    for label in labels {
        key.push_str(&format!(
            "{}:{}{}:{}",
            label.key.len(),
            label.key,
            label.value.len(),
            label.value
        ));
    }
}

/// Whether two envelopes go under one Resource: a collector view's states
/// share a Resource with equal attributes; a hand-configured sketch's also
/// need equal SCHEMA attributes.
fn same_resource(left: &SketchEnvelope, right: &SketchEnvelope) -> bool {
    if left.view.is_some() != right.view.is_some() {
        return false;
    }
    if left.view.is_none()
        && (left.sketch_type != right.sketch_type || left.schema_version != right.schema_version)
    {
        return false;
    }
    let mut left_key = String::new();
    let mut right_key = String::new();
    append_labels_to_key(&mut left_key, &left.resource_labels);
    append_labels_to_key(&mut right_key, &right.resource_labels);
    left_key == right_key
}

/// Whether two collector-view states go under one Scope and one Metric: the
/// same received scope, and the same view and metric descriptor.
fn same_view_scope(left: &SketchEnvelope, right: &SketchEnvelope) -> bool {
    let (l, r) = (&left.source, &right.source);
    l.scope_name == r.scope_name
        && l.scope_version == r.scope_version
        && l.scope_attributes == r.scope_attributes
}

fn same_view_metric(left: &SketchEnvelope, right: &SketchEnvelope) -> bool {
    left.view == right.view
        && left.metric_name == right.metric_name
        && left.source.unit == right.source.unit
        && left.source.description == right.source.description
}

/// Zero-copy adapter presenting a `&[SketchEnvelope]` as a
/// `MetricsView` directly, using SummaryDataPoints for sketch envelopes and
/// Gauge NumberDataPoints for scalar estimates.
///
/// - A hand-configured sketch: one Resource per SCHEMA (the received
///   resource attributes plus `sketch.*`), one Scope `asap.series` per series
///   (its id and labels), one Metric.
/// - A collector view's state: the series' received Resource and Scope, one
///   Metric per view (`<metric>.asap_view.<node_id>`, with the view identity
///   as metadata) and the received point attributes plus `sketch.envelope`,
///   exactly as the raw forward sends the samples.
struct AsapMetricsView<'a> {
    envelopes: &'a [SketchEnvelope],
    resources: Vec<AsapResourceGroup>,
}

struct AsapResourceGroup {
    representative: usize,
    sketch_size: Option<String>,
    scopes: Vec<AsapScopeGroup>,
}

struct AsapScopeGroup {
    /// A hand-configured sketch's series id; 0 for a collector view.
    series_id: u32,
    /// The rows of each Metric under the scope.
    metrics: Vec<Vec<usize>>,
}

impl<'a> AsapMetricsView<'a> {
    fn from_envelopes(
        envelopes: &'a [SketchEnvelope],
        series_ids: &[u32],
        sketch_params: Option<&SketchParams>,
    ) -> Self {
        let mut resources: Vec<AsapResourceGroup> = Vec::new();
        for (row, env) in envelopes.iter().enumerate() {
            let resource_pos = resources
                .iter()
                .position(|group| same_resource(&envelopes[group.representative], env))
                .unwrap_or_else(|| {
                    resources.push(AsapResourceGroup {
                        representative: row,
                        sketch_size: sketch_params
                            .and_then(|params| sketch_size_string(env.sketch_type, params)),
                        scopes: Vec::new(),
                    });
                    resources.len() - 1
                });
            let scopes = &mut resources[resource_pos].scopes;
            let first_row = |group: &AsapScopeGroup| &envelopes[group.metrics[0][0]];
            let scope_pos = scopes
                .iter()
                .position(|group| match env.view {
                    Some(_) => same_view_scope(first_row(group), env),
                    None => group.series_id == series_ids[row],
                })
                .unwrap_or_else(|| {
                    scopes.push(AsapScopeGroup {
                        series_id: series_ids[row],
                        metrics: Vec::new(),
                    });
                    scopes.len() - 1
                });
            let metrics = &mut scopes[scope_pos].metrics;
            let metric_pos = metrics
                .iter()
                .position(|rows| match env.view {
                    Some(_) => same_view_metric(&envelopes[rows[0]], env),
                    None => true,
                })
                .unwrap_or_else(|| {
                    metrics.push(Vec::new());
                    metrics.len() - 1
                });
            metrics[metric_pos].push(row);
        }
        Self {
            envelopes,
            resources,
        }
    }

    /// One row's worth of attributes, built directly from the
    /// envelope's own fields. `sketch.envelope` only appears when this
    /// row actually carries a sketch payload.
    fn attributes_for_row(&self, row: usize) -> Vec<AsapAttribute<'a>> {
        let env = &self.envelopes[row];
        if env.view.is_some() {
            // The received point attributes, then the state.
            let mut attrs = string_attributes(&env.labels);
            if !env.payload.is_empty() {
                attrs.push(AsapAttribute {
                    key: ATTR_ENVELOPE,
                    value: AsapAnyValue::Bytes(&env.payload),
                });
            }
            return attrs;
        }
        let mut attrs = Vec::with_capacity(3);
        if !env.payload.is_empty() {
            attrs.push(AsapAttribute {
                key: ATTR_ENVELOPE,
                value: AsapAnyValue::Bytes(&env.payload),
            });
        }
        attrs.push(AsapAttribute {
            key: ATTR_WINDOW_START_MS,
            value: AsapAnyValue::Int(env.window_start_ms),
        });
        attrs.push(AsapAttribute {
            key: ATTR_WINDOW_END_MS,
            value: AsapAnyValue::Int(env.window_end_ms),
        });
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
        self.resources
            .iter()
            .map(|group| AsapResourceMetricsView { view: self, group })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

struct AsapResourceMetricsView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    group: &'v AsapResourceGroup,
}

impl<'v, 'a> ResourceMetricsView for AsapResourceMetricsView<'v, 'a> {
    type Resource<'res>
        = AsapResourceView<'res, 'a>
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
        Some(AsapResourceView {
            view: self.view,
            row: self.group.representative,
        })
    }

    fn scopes(&self) -> Self::ScopesIter<'_> {
        self.group
            .scopes
            .iter()
            .map(|group| AsapScopeMetricsView {
                view: self.view,
                group,
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn schema_url(&self) -> Option<Str<'_>> {
        None
    }
}

struct AsapScopeMetricsView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    group: &'v AsapScopeGroup,
}

impl<'v, 'a> ScopeMetricsView for AsapScopeMetricsView<'v, 'a> {
    type Scope<'scp>
        = AsapScopeView<'scp, 'a>
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
        Some(AsapScopeView {
            view: self.view,
            group: self.group,
        })
    }

    fn metrics(&self) -> Self::MetricIter<'_> {
        self.group
            .metrics
            .iter()
            .map(|rows| AsapMetricView {
                view: self.view,
                rows,
            })
            .collect::<Vec<_>>()
            .into_iter()
    }

    fn schema_url(&self) -> Str<'_> {
        b""
    }
}

struct AsapMetricView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    rows: &'v [usize],
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
        self.view.envelopes[self.rows[0]].metric_name.as_bytes()
    }

    fn description(&self) -> Str<'_> {
        self.view.envelopes[self.rows[0]]
            .source
            .description
            .as_bytes()
    }

    fn unit(&self) -> Str<'_> {
        self.view.envelopes[self.rows[0]].source.unit.as_bytes()
    }

    fn data(&self) -> Option<Self::Data<'_>> {
        Some(AsapDataView {
            view: self.view,
            rows: self.rows,
        })
    }

    /// A collector view's identity (`asap.plan_id`, `asap.node_id`,
    /// `asap.view_state_key`); none for a hand-configured sketch.
    fn metadata(&self) -> Self::AttributeIter<'_> {
        let Some(view) = &self.view.envelopes[self.rows[0]].view else {
            return Vec::new().into_iter();
        };
        vec![
            AsapAttribute {
                key: META_PLAN_ID,
                value: AsapAnyValue::Str(&view.plan_id),
            },
            AsapAttribute {
                key: META_NODE_ID,
                value: AsapAnyValue::Int(view.node_id),
            },
            AsapAttribute {
                key: META_VIEW_STATE_KEY,
                value: AsapAnyValue::Str(&view.view_state_key),
            },
        ]
        .into_iter()
    }
}

struct AsapDataView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    rows: &'v [usize],
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
        = AsapSummaryView<'summary, 'a>
    where
        Self: 'summary;

    fn value_type(&self) -> MetricKind {
        if self.view.envelopes[self.rows[0]].payload.is_empty() {
            MetricKind::Gauge
        } else {
            MetricKind::Summary
        }
    }

    fn as_gauge(&self) -> Option<Self::Gauge<'_>> {
        (self.value_type() == MetricKind::Gauge).then_some(AsapGaugeView {
            view: self.view,
            rows: self.rows,
        })
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
        (self.value_type() == MetricKind::Summary).then_some(AsapSummaryView {
            view: self.view,
            rows: self.rows,
        })
    }
}

struct AsapSummaryView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    rows: &'v [usize],
}

impl<'v, 'a> SummaryView for AsapSummaryView<'v, 'a> {
    type SummaryDataPoint<'dp>
        = AsapSummaryDataPointView<'dp, 'a>
    where
        Self: 'dp;
    type SummaryDataPointIter<'dp>
        = std::vec::IntoIter<AsapSummaryDataPointView<'dp, 'a>>
    where
        Self: 'dp;

    fn data_points(&self) -> Self::SummaryDataPointIter<'_> {
        self.rows
            .iter()
            .copied()
            .map(|row| AsapSummaryDataPointView {
                view: self.view,
                row,
            })
            .collect::<Vec<_>>()
            .into_iter()
    }
}

struct AsapSummaryDataPointView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    row: usize,
}

impl<'v, 'a> SummaryDataPointView for AsapSummaryDataPointView<'v, 'a> {
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
        self.view.attributes_for_row(self.row).into_iter()
    }

    fn start_time_unix_nano(&self) -> u64 {
        self.view.envelopes[self.row]
            .window_start_ms
            .saturating_mul(1_000_000)
    }

    fn time_unix_nano(&self) -> u64 {
        self.view.envelopes[self.row]
            .window_end_ms
            .saturating_mul(1_000_000)
    }

    fn count(&self) -> u64 {
        self.view.envelopes[self.row].count
    }

    fn sum(&self) -> f64 {
        self.view.envelopes[self.row].value
    }

    fn quantile_values(&self) -> Self::ValueAtQuantileIter<'_> {
        Vec::new().into_iter()
    }

    fn flags(&self) -> DataPointFlags {
        DataPointFlags::new(0)
    }
}

struct AsapGaugeView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    rows: &'v [usize],
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
        self.rows
            .iter()
            .copied()
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
            // produces here (the only `sketch.*` int-typed attributes
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
// Unsupported metric kinds and exemplars still require concrete associated
// types even though no instance is produced. The inhabited Resource, Scope,
// Gauge, and Summary types are defined above; these placeholders cover the
// remaining view surface.
// the view traits still require *some* concrete, well-formed type for
// each associated type regardless of whether an instance is ever
// constructed. An uninhabited enum (`enum X {}`) lets every trait
// method be `match *self {}` — valid because no value of an
// uninhabited type can ever exist to call it on, so the body coerces
// to any return type without ever actually running.

struct AsapResourceView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    row: usize,
}

impl<'v, 'a> ResourceView for AsapResourceView<'v, 'a> {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributesIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;

    fn attributes(&self) -> Self::AttributesIter<'_> {
        let env = &self.view.envelopes[self.row];
        let group = self
            .view
            .resources
            .iter()
            .find(|group| group.representative == self.row)
            .expect("resource group exists");
        let mut attrs = string_attributes(&env.resource_labels);
        if env.view.is_some() {
            // A collector view's Resource is the received one.
            return attrs.into_iter();
        }
        attrs.extend([
            AsapAttribute {
                key: ATTR_SKETCH_TYPE,
                value: AsapAnyValue::Str(env.sketch_type.name()),
            },
            AsapAttribute {
                key: ATTR_SCHEMA_VERSION,
                value: AsapAnyValue::Int(u64::from(env.schema_version)),
            },
        ]);
        if let Some(value) = group.sketch_size.as_deref() {
            attrs.push(AsapAttribute {
                key: ATTR_SKETCH_SIZE,
                value: AsapAnyValue::Str(value),
            });
        }
        attrs.into_iter()
    }

    fn dropped_attributes_count(&self) -> u32 {
        0
    }
}

/// `key = value` attributes with string values.
fn string_attributes(labels: &[KeyValue]) -> Vec<AsapAttribute<'_>> {
    labels
        .iter()
        .map(|label| AsapAttribute {
            key: label.key.as_str(),
            value: AsapAnyValue::Str(label.value.as_str()),
        })
        .collect()
}

struct AsapScopeView<'v, 'a> {
    view: &'v AsapMetricsView<'a>,
    group: &'v AsapScopeGroup,
}

impl<'v, 'a> InstrumentationScopeView for AsapScopeView<'v, 'a> {
    type Attribute<'att>
        = AsapAttribute<'att>
    where
        Self: 'att;
    type AttributeIter<'att>
        = std::vec::IntoIter<AsapAttribute<'att>>
    where
        Self: 'att;

    fn name(&self) -> Option<Str<'_>> {
        let env = &self.view.envelopes[self.group.metrics[0][0]];
        match env.view {
            Some(_) => Some(env.source.scope_name.as_bytes()),
            None => Some(SERIES_SCOPE_NAME.as_bytes()),
        }
    }

    fn version(&self) -> Option<Str<'_>> {
        let env = &self.view.envelopes[self.group.metrics[0][0]];
        env.view
            .as_ref()
            .map(|_| env.source.scope_version.as_bytes())
    }

    fn attributes(&self) -> Self::AttributeIter<'_> {
        let env = &self.view.envelopes[self.group.metrics[0][0]];
        if env.view.is_some() {
            return string_attributes(&env.source.scope_attributes).into_iter();
        }
        let mut attrs = Vec::with_capacity(env.labels.len() + 1);
        attrs.push(AsapAttribute {
            key: ATTR_SERIES_ID,
            value: AsapAnyValue::Int(u64::from(self.group.series_id)),
        });
        for kv in &env.labels {
            attrs.push(AsapAttribute {
                key: &kv.key,
                value: AsapAnyValue::Str(&kv.value),
            });
        }
        attrs.into_iter()
    }

    fn dropped_attributes_count(&self) -> u32 {
        0
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

/// Formats one metrics message as both logical OTLP records and physical OTAP
/// Arrow child batches. Intended for demos and diagnostics; envelope payloads
/// are reported by byte length rather than dumped verbatim.
pub fn describe_pdata_protocol(pdata: &OtapPdata) -> Result<String, CodecError> {
    let mut output = String::new();
    let (_context, payload) = pdata.clone().into_parts();
    let records: OtapArrowRecords = payload
        .try_into_with_default()
        .map_err(|e| CodecError::Decode(format!("{e}")))?;

    writeln!(output, "  OTAP PHYSICAL LAYOUT").expect("write String");
    for payload_type in records.allowed_payload_types() {
        let Some(batch) = records.get(*payload_type) else {
            continue;
        };
        let role = match payload_type {
            otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::ResourceAttrs => "SCHEMA",
            otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::ScopeAttrs => "DICTIONARY / LABELS",
            otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::UnivariateMetrics => "SERIES JOIN",
            otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::NumberDataPoints
            | otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::NumberDpAttrs
            | otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::SummaryDataPoints
            | otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType::SummaryDpAttrs => "RECORD",
            _ => "CHILD BATCH",
        };
        writeln!(
            output,
            "    +-- {payload_type:?}  [{role}]  rows={}",
            batch.num_rows()
        )
        .expect("write String");
        let fields = batch.schema();
        let name_width = fields
            .fields()
            .iter()
            .map(|field| field.name().len() + usize::from(field.is_nullable()))
            .max()
            .unwrap_or(0);
        for field in fields.fields() {
            let nullable_name = if field.is_nullable() {
                format!("{}?", field.name())
            } else {
                field.name().to_string()
            };
            writeln!(
                output,
                "    |   {nullable_name:<name_width$}  {}",
                pretty_arrow_type(field.data_type())
            )
            .expect("write String");
        }
        writeln!(output, "    |").expect("write String");
    }

    writeln!(output, "  OTLP LOGICAL METRICS").expect("write String");
    let decoded = decode_pdata_to_observations(pdata.clone())?;
    let observation_count = decoded.observations.len();
    for (index, observation) in decoded.observations.into_iter().enumerate() {
        if observation_count > 10 && (3..observation_count - 2).contains(&index) {
            if index == 3 {
                writeln!(
                    output,
                    "    ... {} additional NumberDataPoints omitted ...",
                    observation_count - 5
                )
                .expect("write String");
            }
            continue;
        }
        let labels = observation
            .labels
            .iter()
            .map(|kv| format!("{}={}", kv.key, kv.value))
            .collect::<Vec<_>>()
            .join(",");
        if let Some(envelope) = observation.value.envelope {
            let view = envelope
                .view
                .as_ref()
                .map(|v| format!("view={}/{}", v.plan_id, v.node_id))
                .unwrap_or_default();
            writeln!(
                output,
                "    +-- Metric  {}\n        |-- labels     {{{}}}\n        |-- value      self-describing sketch envelope ({} bytes)\n        |-- schema     {}  {}  v{}\n        +-- window     ({}, {}]",
                observation.metric, labels, envelope.payload.len(), view,
                envelope.sketch_type.name(), envelope.schema_version,
                envelope.window_start_ms, envelope.window_end_ms
            )
            .expect("write String");
        } else {
            writeln!(
                output,
                "    +-- Metric  {}\n        |-- labels     {{{}}}\n        +-- value      {}",
                observation.metric, labels, observation.value.float
            )
            .expect("write String");
        }
    }
    Ok(output)
}

fn pretty_arrow_type(data_type: &arrow_schema::DataType) -> String {
    use arrow_schema::DataType;

    match data_type {
        DataType::UInt8 => "u8".to_string(),
        DataType::UInt16 => "u16".to_string(),
        DataType::UInt32 => "u32".to_string(),
        DataType::UInt64 => "u64".to_string(),
        DataType::Int64 => "i64".to_string(),
        DataType::Float64 => "f64".to_string(),
        DataType::Utf8 => "utf8".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::Timestamp(unit, _) => format!("timestamp({unit:?})").to_lowercase(),
        DataType::Dictionary(key, value) => format!(
            "dict<{}, {}>",
            pretty_arrow_type(key),
            pretty_arrow_type(value)
        ),
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|field| {
                    format!(
                        "{}{}: {}",
                        field.name(),
                        if field.is_nullable() { "?" } else { "" },
                        pretty_arrow_type(field.data_type())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("struct<{children}>")
        }
        DataType::List(field) => format!("list<{}>", pretty_arrow_type(field.data_type())),
        other => format!("{other:?}").to_lowercase(),
    }
}

/// Converts a real `OtapPdata` directly into `Vec<Observation>` —
/// feeds `Precompute::observe` on the producer role's ingest path.
///
/// Accepts `pdata` by value (consumed) — the caller's `Message::PData`
/// match arm already owns it and has no further use for the original
/// context once this conversion runs.
pub fn decode_pdata_to_observations(pdata: OtapPdata) -> Result<DecodeOutcome, CodecError> {
    let mut observations = Vec::new();
    let skipped_non_scalar = decode_each(pdata, |obs| observations.push(obs.clone()))?;
    Ok(DecodeOutcome {
        observations,
        skipped_non_scalar,
    })
}

/// Walks `pdata` like [`decode_pdata_to_observations`], passing each data
/// point to `f` instead of collecting them. The observation is one buffer
/// rewritten per point, so a hot path decodes without allocating per point.
/// Returns the number of non-scalar data points skipped.
pub fn decode_each(pdata: OtapPdata, f: impl FnMut(&Observation)) -> Result<usize, CodecError> {
    let (_context, payload) = pdata.into_parts();
    let arrow_records: OtapArrowRecords = payload
        .try_into_with_default()
        .map_err(|e| CodecError::Decode(format!("{e}")))?;

    let OtapArrowRecords::Metrics(_) = &arrow_records else {
        return Ok(0);
    };

    let view = OtapMetricsView::try_from(&arrow_records)
        .map_err(|e| CodecError::Decode(format!("{e}")))?;

    let mut acc = DecodeAccumulator {
        scratch: Observation::default(),
        sink: f,
    };
    let mut skipped_non_scalar = 0usize;

    for resource in view.resources() {
        let mut schema = DecodeSchema::default();
        let mut resource_labels = Vec::new();
        if let Some(resource_view) = resource.resource() {
            for attr in resource_view.attributes() {
                let Some(val) = attr.value() else { continue };
                let key = String::from_utf8_lossy(attr.key()).into_owned();
                match key.as_str() {
                    ATTR_SKETCH_TYPE => {
                        schema.sketch_type = val
                            .as_string()
                            .map(|s| String::from_utf8_lossy(s).into_owned())
                    }
                    ATTR_SCHEMA_VERSION => {
                        schema.schema_version = val.as_int64().filter(|v| *v >= 0).map(|v| v as u32)
                    }
                    ATTR_SKETCH_SIZE => {}
                    _ => {
                        if let Some(s) = val.as_string() {
                            resource_labels
                                .push(KeyValue::new(key, String::from_utf8_lossy(s).into_owned()));
                        }
                    }
                }
            }
        }
        for scope in resource.scopes() {
            // A hand-configured sketch's series scope holds its labels; any
            // other scope is the instrumentation scope the points arrived
            // under, which is not a label (as in Prometheus).
            let mut series_labels = Vec::new();
            let mut source = SourceDescriptor::default();
            if let Some(scope_view) = scope.scope() {
                let text =
                    |s: Option<Str<'_>>| String::from_utf8_lossy(s.unwrap_or(b"")).into_owned();
                let name = text(scope_view.name());
                let attributes = if name == SERIES_SCOPE_NAME {
                    &mut series_labels
                } else {
                    source.scope_name = name;
                    source.scope_version = text(scope_view.version());
                    &mut source.scope_attributes
                };
                for attr in scope_view.attributes() {
                    let Some(val) = attr.value() else { continue };
                    let key = String::from_utf8_lossy(attr.key()).into_owned();
                    if key == ATTR_SERIES_ID {
                        continue;
                    }
                    if let Some(s) = val.as_string() {
                        attributes
                            .push(KeyValue::new(key, String::from_utf8_lossy(s).into_owned()));
                    }
                }
            }
            for metric in scope.metrics() {
                let name = String::from_utf8_lossy(metric.name()).into_owned();
                let Some(data) = metric.data() else {
                    continue;
                };
                let name = DecodeMetric {
                    view: view_identity(&metric, &name)?,
                    source: Arc::new(SourceDescriptor {
                        unit: String::from_utf8_lossy(metric.unit()).into_owned(),
                        description: String::from_utf8_lossy(metric.description()).into_owned(),
                        ..source.clone()
                    }),
                    name,
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
                        acc.push_data_point(dp, &name, &schema, &resource_labels, &series_labels)?;
                    }
                } else if let Some(sum) = data.as_sum() {
                    for dp in sum.data_points() {
                        acc.push_data_point(dp, &name, &schema, &resource_labels, &series_labels)?;
                    }
                } else if let Some(summary) = data.as_summary() {
                    for dp in summary.data_points() {
                        acc.push_summary_data_point(
                            dp,
                            &name,
                            &schema,
                            &resource_labels,
                            &series_labels,
                        )?;
                    }
                } else {
                    // Histogram / ExponentialHistogram: no single
                    // well-defined scalar or sketch envelope.
                    skipped_non_scalar += match data.value_type() {
                        MetricKind::Histogram => data
                            .as_histogram()
                            .map(|h| h.data_points().count())
                            .unwrap_or(0),
                        MetricKind::ExponentialHistogram => data
                            .as_exponential_histogram()
                            .map(|h| h.data_points().count())
                            .unwrap_or(0),
                        _ => 0,
                    };
                }
            }
        }
    }

    Ok(skipped_non_scalar)
}

/// Overwrites `labels[index]` (appending if needed) with `key=value`,
/// reusing the label's string buffers.
fn set_label(labels: &mut Vec<KeyValue>, index: usize, key: &[u8], value: &[u8]) {
    if index == labels.len() {
        labels.push(KeyValue::default());
    }
    let label = &mut labels[index];
    label.key.clear();
    label.key.push_str(&String::from_utf8_lossy(key));
    label.value.clear();
    label.value.push_str(&String::from_utf8_lossy(value));
}

/// Passes one `Observation` per data point to `sink` while walking the real
/// `OtapMetricsView` — factored out of [`decode_pdata_to_observations`]
/// so the Gauge and Sum branches (whose `NumberDataPointView`
/// implementations are different concrete types, both borrowing from
/// the `gauge`/`sum` view that produced them) can share one push
/// method via a generic function instead of duplicating the per-data-
/// point body twice, or trying to unify their different concrete
/// iterator types into one `Vec` first (which doesn't compile —
/// `gauge`/`sum` would need to outlive the branch that produced them).
struct DecodeAccumulator<F> {
    /// Rewritten for each data point.
    scratch: Observation,
    sink: F,
}

/// Parses an optional `sketch.type` value; absent means `Unspecified`.
fn parse_sketch_type(raw: Option<&str>) -> Result<SketchType, CodecError> {
    match raw {
        None => Ok(SketchType::Unspecified),
        Some(raw) => SketchType::from_name(raw)
            .ok_or_else(|| CodecError::Decode(format!("unknown sketch type {raw:?}"))),
    }
}

#[derive(Default)]
struct DecodeSchema {
    sketch_type: Option<String>,
    schema_version: Option<u32>,
}

/// The Metric a data point belongs to: its name, the collector view it is a
/// state of (from its metadata) and its scope and descriptor.
struct DecodeMetric {
    name: String,
    view: Option<Arc<ViewIdentity>>,
    source: Arc<SourceDescriptor>,
}

impl DecodeMetric {
    /// The sketch type of the metric's states: a collector view's, from θ
    /// in its view state key; a hand-configured sketch's, from the
    /// Resource's `sketch.type`.
    fn sketch_type(&self, schema: &DecodeSchema) -> Result<SketchType, CodecError> {
        let Some(view) = &self.view else {
            return parse_sketch_type(schema.sketch_type.as_deref());
        };
        let key: ViewStateKey = serde_json::from_str(&view.view_state_key).map_err(|e| {
            CodecError::Decode(format!("metric {}: {META_VIEW_STATE_KEY}: {e}", self.name))
        })?;
        key.logical_properties
            .parameters()
            .and_then(super::planner_views::sketch_type_spec_of)
            .map(|spec| spec.sketch_type)
            .ok_or_else(|| {
                CodecError::Decode(format!(
                    "metric {}: the view state key names no sketch the collector builds",
                    self.name
                ))
            })
    }
}

/// The collector view a Metric's points are states of, from its metadata:
/// `None` when it has no `asap.*` metadata; an error when it has some but
/// not all three, or one of the wrong type.
fn view_identity<M: MetricView>(
    metric: &M,
    name: &str,
) -> Result<Option<Arc<ViewIdentity>>, CodecError> {
    let (mut plan_id, mut node_id, mut view_state_key) = (None, None, None);
    let mut any = false;
    for attr in metric.metadata() {
        let key = attr.key();
        let value = attr.value();
        let text = || {
            value
                .as_ref()
                .and_then(|v| v.as_string())
                .map(|s| String::from_utf8_lossy(s).into_owned())
        };
        if key == META_PLAN_ID.as_bytes() {
            plan_id = text();
        } else if key == META_NODE_ID.as_bytes() {
            node_id = value
                .as_ref()
                .and_then(|v| v.as_int64())
                .and_then(|v| u64::try_from(v).ok());
        } else if key == META_VIEW_STATE_KEY.as_bytes() {
            view_state_key = text();
        } else {
            continue;
        }
        any = true;
    }
    match (plan_id, node_id, view_state_key) {
        (Some(plan_id), Some(node_id), Some(view_state_key)) => Ok(Some(Arc::new(ViewIdentity {
            plan_id,
            node_id,
            view_state_key,
        }))),
        _ if !any => Ok(None),
        _ => Err(CodecError::Decode(format!(
            "metric {name}: view identity needs {META_PLAN_ID} (string), {META_NODE_ID} (int) \
             and {META_VIEW_STATE_KEY} (string) metadata"
        ))),
    }
}

impl<F: FnMut(&Observation)> DecodeAccumulator<F> {
    fn push_summary_data_point<D: SummaryDataPointView>(
        &mut self,
        dp: D,
        metric: &DecodeMetric,
        inherited_schema: &DecodeSchema,
        resource_labels: &[KeyValue],
        series_labels: &[KeyValue],
    ) -> Result<(), CodecError> {
        let mut envelope_bytes: Option<Vec<u8>> = None;
        let mut window_start_ms = None;
        let mut window_end_ms = None;
        let mut labels = series_labels.to_vec();

        for attr in dp.attributes() {
            let Some(value) = attr.value() else { continue };
            let key = String::from_utf8_lossy(attr.key()).into_owned();
            match key.as_str() {
                ATTR_ENVELOPE => envelope_bytes = value.as_bytes().map(<[u8]>::to_vec),
                ATTR_WINDOW_START_MS => {
                    window_start_ms = value.as_int64().filter(|v| *v >= 0).map(|v| v as u64)
                }
                ATTR_WINDOW_END_MS => {
                    window_end_ms = value.as_int64().filter(|v| *v >= 0).map(|v| v as u64)
                }
                _ => {
                    if let Some(value) = value.as_string() {
                        labels.push(KeyValue::new(
                            key,
                            String::from_utf8_lossy(value).into_owned(),
                        ));
                    }
                }
            }
        }

        let payload = envelope_bytes
            .filter(|bytes| !bytes.is_empty())
            .ok_or_else(|| {
                CodecError::Decode("summary sketch is missing sketch.envelope".into())
            })?;
        let sketch_type = metric.sketch_type(inherited_schema)?;
        validate_self_describing_payload(sketch_type, &payload).map_err(CodecError::Decode)?;
        let timestamp_ms = dp.time_unix_nano() / 1_000_000;
        let metric_name = metric.name.as_str();
        let envelope = SketchEnvelope {
            schema_version: inherited_schema.schema_version.unwrap_or(0),
            sketch_type,
            view: metric.view.clone(),
            resource_labels: resource_labels.to_vec(),
            labels: labels.clone(),
            source: Arc::clone(&metric.source),
            window_start_ms: window_start_ms.unwrap_or(dp.start_time_unix_nano() / 1_000_000),
            window_end_ms: window_end_ms.unwrap_or(timestamp_ms),
            payload,
            metric_name: metric_name.to_string(),
            count: dp.count(),
            aggregation_temporality: 0,
            value: dp.sum(),
        };
        let mut observation = Observation::new(
            timestamp_ms,
            metric_name.to_string(),
            resource_labels.to_vec(),
            labels,
            ObservationValue::envelope(envelope),
        );
        observation.source = Arc::clone(&metric.source);
        (self.sink)(&observation);
        Ok(())
    }

    /// Builds and appends one `Observation` from a data point — a
    /// no-op if the data point carries no value. A non-empty
    /// `sketch.envelope` attribute routes it as a sketch envelope, validated
    /// like a Summary carrier; an unrecognized `sketch.type` is an error.
    fn push_data_point<D: NumberDataPointView>(
        &mut self,
        dp: D,
        metric: &DecodeMetric,
        inherited_schema: &DecodeSchema,
        resource_labels: &[KeyValue],
        series_labels: &[KeyValue],
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
        let mut sketch_type_raw = inherited_schema.sketch_type.clone();
        let mut schema_version = inherited_schema.schema_version;
        let mut window_start_ms: Option<u64> = None;
        let mut window_end_ms: Option<u64> = None;
        let obs = &mut self.scratch;
        for (index, label) in series_labels.iter().enumerate() {
            set_label(
                &mut obs.labels,
                index,
                label.key.as_bytes(),
                label.value.as_bytes(),
            );
        }
        let mut label_count = series_labels.len();

        for attr in dp.attributes() {
            let Some(val) = attr.value() else { continue };
            let key = attr.key();
            match std::str::from_utf8(key).unwrap_or_default() {
                ATTR_ENVELOPE => envelope_bytes = val.as_bytes().map(<[u8]>::to_vec),
                ATTR_SKETCH_TYPE => {
                    sketch_type_raw = val
                        .as_string()
                        .map(|s| String::from_utf8_lossy(s).into_owned());
                }
                ATTR_SCHEMA_VERSION => {
                    schema_version = val.as_int64().filter(|v| *v >= 0).map(|v| v as u32);
                }
                ATTR_WINDOW_START_MS => {
                    window_start_ms = val.as_int64().filter(|v| *v >= 0).map(|v| v as u64);
                }
                ATTR_WINDOW_END_MS => {
                    window_end_ms = val.as_int64().filter(|v| *v >= 0).map(|v| v as u64);
                }
                _ => {
                    // Non-reserved: treated as a per-row label. Only
                    // string-typed values are representable as a label;
                    // a non-string, non-reserved attribute is dropped.
                    if let Some(s) = val.as_string() {
                        set_label(&mut obs.labels, label_count, key, s);
                        label_count += 1;
                    }
                }
            }
        }
        obs.labels.truncate(label_count);

        // An empty `sketch.envelope` from some other producer routes to
        // the scalar path rather than an empty-payload envelope.
        obs.value = match envelope_bytes.filter(|b| !b.is_empty()) {
            Some(payload) => {
                let sketch_type = match &metric.view {
                    Some(_) => metric.sketch_type(inherited_schema)?,
                    None => parse_sketch_type(sketch_type_raw.as_deref())?,
                };
                validate_self_describing_payload(sketch_type, &payload)
                    .map_err(CodecError::Decode)?;
                ObservationValue::envelope(SketchEnvelope {
                    schema_version: schema_version.unwrap_or(0),
                    sketch_type,
                    view: metric.view.clone(),
                    resource_labels: Vec::new(),
                    labels: obs.labels.clone(),
                    source: Arc::clone(&metric.source),
                    window_start_ms: window_start_ms.unwrap_or(0),
                    window_end_ms: window_end_ms.unwrap_or(0),
                    payload,
                    metric_name: metric.name.clone(),
                    count: 0,
                    aggregation_temporality: 0,
                    value,
                })
            }
            None => ObservationValue::float(value),
        };
        obs.timestamp_ms = timestamp_ms;
        if obs.metric != metric.name {
            obs.metric.clear();
            obs.metric.push_str(&metric.name);
        }
        if !Arc::ptr_eq(&obs.source, &metric.source) {
            obs.source = Arc::clone(&metric.source);
        }
        if obs.resource_labels.as_slice() != resource_labels {
            obs.resource_labels.clear();
            obs.resource_labels.extend_from_slice(resource_labels);
        }
        (self.sink)(obs);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::{KeyValue as Kv, ObservationValueKind};
    use crate::precompute::Sketch;
    use crate::sketches::KLLWrapper;

    fn one_envelope(
        metric_name: &str,
        value: f64,
        attr_key: &str,
        attr_value: &str,
    ) -> SketchEnvelope {
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Unspecified,
            view: None,
            resource_labels: vec![],
            labels: vec![Kv::new(attr_key, attr_value)],
            source: Default::default(),
            window_start_ms: 1_000,
            window_end_ms: 2_000,
            payload: Vec::new(),
            metric_name: metric_name.to_string(),
            count: 0,
            aggregation_temporality: 0,
            value,
        }
    }

    fn encoded_series_id(pdata: OtapPdata) -> i64 {
        use arrow_array::types::UInt16Type;
        use arrow_array::{DictionaryArray, Int64Array};
        use otel_arrow_dfe_pdata::proto::opentelemetry::arrow::v1::ArrowPayloadType;

        let (_context, payload) = pdata.into_parts();
        let records: OtapArrowRecords = payload.try_into_with_default().expect("OTAP records");
        let attrs = records
            .get(ArrowPayloadType::ScopeAttrs)
            .expect("scope attrs");
        let ints = attrs
            .column_by_name("int")
            .unwrap_or_else(|| panic!("int column in {:?}", attrs.schema()))
            .as_any()
            .downcast_ref::<DictionaryArray<UInt16Type>>()
            .expect("dictionary int");
        let values = ints
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int values");
        values.value(ints.keys().value(0) as usize)
    }

    /// Scenario: a stream encoder sees one series twice and then a new label set.
    /// Guarantees: series IDs are stable across flushes and advance only for new identities.
    #[test]
    fn stream_encoder_reuses_series_id_across_windows() {
        let mut encoder = OtapSketchEncoder::default();
        let mut first = one_envelope("requests", 1.0, "route", "/api");
        let first_id = encoded_series_id(encoder.encode(std::slice::from_ref(&first)).unwrap());
        first.window_start_ms = 2_000;
        first.window_end_ms = 3_000;
        let second_id = encoded_series_id(encoder.encode(std::slice::from_ref(&first)).unwrap());
        let other = one_envelope("requests", 2.0, "route", "/checkout");
        let other_id = encoded_series_id(encoder.encode(std::slice::from_ref(&other)).unwrap());

        assert_eq!(first_id, second_id);
        assert_ne!(first_id, other_id);
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

    /// Two otherwise identical series belonging to different OTel resources
    /// must remain distinct and retain their resource attributes.
    #[test]
    fn encode_then_decode_preserves_distinct_resource_series() {
        let mut checkout = one_envelope("requests", 1.0, "route", "/api");
        checkout.resource_labels = vec![Kv::new("service.name", "checkout")];
        let mut payments = checkout.clone();
        payments.value = 2.0;
        payments.resource_labels = vec![Kv::new("service.name", "payments")];

        let pdata = encode_envelopes_to_pdata(&[checkout, payments]).expect("encode");
        let mut observations = decode_pdata_to_observations(pdata)
            .expect("decode")
            .observations;
        observations.sort_by(|left, right| left.value.float.total_cmp(&right.value.float));

        assert_eq!(observations.len(), 2);
        assert_eq!(
            observations[0].resource_labels,
            vec![Kv::new("service.name", "checkout")]
        );
        assert_eq!(
            observations[1].resource_labels,
            vec![Kv::new("service.name", "payments")]
        );
    }

    #[test]
    fn summary_carrier_round_trips_a_self_describing_sketch_envelope() {
        let mut env = one_envelope("sketch_stream", 0.0, "region", "us-east");
        env.sketch_type = SketchType::KLLSketch;
        let mut sketch = KLLWrapper::new(200, Some(42));
        sketch.update(42.0);
        env.payload = sketch
            .snapshot()
            .expect("self-describing KLL ASAPv1 envelope");
        let expected_payload = env.payload.clone();

        assert!(env.payload.starts_with(b"ASAPv1"));

        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");
        let outcome = decode_pdata_to_observations(pdata).expect("decode");
        assert_eq!(outcome.observations.len(), 1);
        let obs = &outcome.observations[0];
        assert_eq!(obs.value.kind, ObservationValueKind::Envelope);
        let decoded_env = obs.value.envelope.as_ref().expect("envelope");
        assert_eq!(decoded_env.payload, expected_payload);
        assert_eq!(decoded_env.sketch_type, SketchType::KLLSketch);
        assert_eq!(decoded_env.view, None);
        assert_eq!(decoded_env.window_start_ms, 1_000);
        assert_eq!(decoded_env.window_end_ms, 2_000);
    }

    #[test]
    fn summary_carrier_round_trips_a_cms_heap_envelope() {
        use crate::sketches::CMSHeapWrapper;
        let mut env = one_envelope("topk_stream", 0.0, "region", "us-east");
        env.sketch_type = SketchType::CountMinSketchWithHeap;
        let mut sketch = CMSHeapWrapper::new(4, 64, 3);
        sketch.update(b"checkout", 5);
        env.payload = sketch
            .snapshot()
            .expect("CountMinSketchWithHeap ASAPv1 envelope");
        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");
        let outcome = decode_pdata_to_observations(pdata).expect("decode");
        let decoded = outcome.observations[0].value.envelope.as_ref().unwrap();
        assert_eq!(decoded.sketch_type, SketchType::CountMinSketchWithHeap);
        assert_eq!(decoded.payload, env.payload);

        // A CountMinSketchWithHeap payload labelled as KLL is still rejected.
        env.sketch_type = SketchType::KLLSketch;
        assert!(encode_envelopes_to_pdata(std::slice::from_ref(&env)).is_err());
    }

    #[test]
    fn summary_carrier_round_trips_every_sketch_type() {
        use crate::sketches::{CMSWrapper, CountSketchWrapper, DDSketchWrapper, HLLWrapper};
        let mut dd = DDSketchWrapper::new(0.01);
        dd.update(3.0);
        let mut hll = HLLWrapper::new();
        hll.update(b"user-1");
        let mut cs = CountSketchWrapper::new(4, 64);
        cs.update(b"key", 2);
        let mut cms = CMSWrapper::new(4, 64);
        cms.update(b"key", 2);
        let sketches: [(SketchType, &dyn Sketch); 4] = [
            (SketchType::DDSketch, &dd),
            (SketchType::HLLSketch, &hll),
            (SketchType::CountSketch, &cs),
            (SketchType::CountMinSketch, &cms),
        ];
        for (sketch_type, sketch) in sketches {
            let mut env = one_envelope("sketch_stream", 0.0, "region", "us-east");
            env.sketch_type = sketch_type;
            env.payload = sketch.snapshot().unwrap();
            let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&env)).expect("encode");
            let outcome = decode_pdata_to_observations(pdata).expect("decode");
            let decoded = outcome.observations[0].value.envelope.as_ref().unwrap();
            assert_eq!(decoded.sketch_type, sketch_type);
            assert_eq!(decoded.payload, env.payload);
        }
    }

    #[test]
    fn asapv1_rejects_non_self_describing_envelope_bytes() {
        let mut env = one_envelope("sketch_stream", 0.0, "region", "us-east");
        env.sketch_type = SketchType::KLLSketch;
        env.payload = vec![0xde, 0xad, 0xbe, 0xef];

        let error = encode_envelopes_to_pdata(&[env]).expect_err("invalid ASAPv1 must fail");
        assert!(error
            .to_string()
            .contains("invalid KLLSketch sketch.envelope"));
    }

    #[test]
    fn sketch_payload_without_sketch_type_is_rejected() {
        let mut sketch = KLLWrapper::new(200, Some(42));
        sketch.update(1.0);
        let mut env = one_envelope("sketch_stream", 0.0, "region", "us-east");
        env.payload = sketch.snapshot().unwrap();
        assert!(encode_envelopes_to_pdata(&[env]).is_err());
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

    /// Scenario: one OTAP message carries two dictionary series with different metric names.
    /// Guarantees: each series becomes its own native scope/metric join instead of being rejected.
    #[test]
    fn mixed_metric_names_are_structurally_encoded() {
        let env_a = one_envelope("a", 1.0, "k", "v");
        let env_b = one_envelope("b", 2.0, "k", "v");
        let pdata = encode_envelopes_to_pdata(&[env_a, env_b]).expect("mixed names encode");
        let outcome = decode_pdata_to_observations(pdata).expect("mixed names decode");
        assert_eq!(outcome.observations.len(), 2);
        assert_eq!(outcome.observations[0].metric, "a");
        assert_eq!(outcome.observations[1].metric, "b");
    }

    /// Scenario: a scalar series is encoded through the real OTAP metrics encoder.
    /// Guarantees: SCHEMA, DICTIONARY/LABELS, and RECORD occupy native parent/child batches.
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

        let resource_attrs = arrow_records
            .get(ArrowPayloadType::ResourceAttrs)
            .expect("SCHEMA uses ResourceAttrs");
        // `sketch.type` and `sketch.schema_version`.
        assert_eq!(resource_attrs.num_rows(), 2);

        let scope_attrs = arrow_records
            .get(ArrowPayloadType::ScopeAttrs)
            .expect("DICTIONARY/LABELS use ScopeAttrs");
        assert_eq!(scope_attrs.num_rows(), 2);
        let attrs_schema = scope_attrs.schema();
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

        let record_attrs = arrow_records
            .get(ArrowPayloadType::NumberDpAttrs)
            .expect("RECORD uses NumberDpAttrs");
        assert_eq!(
            record_attrs.num_rows(),
            2,
            "only the two window bounds recur"
        );
    }

    // ---- collector views ---------------------------------------------------

    /// n1's view state key in the q01 fixture
    /// (`tests/fixtures/collector-views/q01`).
    const Q01_KEY: &str = r#"{"definition_id":"5597d603bdf9f1a148d7c7e5344c1a97","window":{"tumbling":{"width_ms":5000}},"logical_properties":{"parameters":{"DDSketch":{"alpha":0.05}}}}"#;

    /// The q01 view's identity, as the loader reads it from the fixture.
    fn q01_view() -> Arc<ViewIdentity> {
        let views = super::super::planner_views::parse(include_str!(
            "../../tests/fixtures/collector-views/q01/collector-views.json"
        ))
        .expect("q01 loads");
        Arc::clone(&views.views[0].identity)
    }

    /// q01's state for one series, as the design's worked example: Resource
    /// `service.name = j0`, point attributes `region`, `service`, `label_0`,
    /// 50 samples (1..=50) in the window `(T - 5 s, T]`, DDSketch α 0.05.
    fn q01_state(end_ms: u64) -> SketchEnvelope {
        let mut sketch = crate::sketches::DDSketchWrapper::new(0.05);
        for value in 1..=50 {
            sketch.update(f64::from(value));
        }
        SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::DDSketch,
            view: Some(q01_view()),
            resource_labels: vec![Kv::new("service.name", "j0")],
            labels: vec![
                Kv::new("region", "r0"),
                Kv::new("service", "s0"),
                Kv::new("label_0", "l0"),
            ],
            source: Arc::new(SourceDescriptor {
                scope_name: "app.meter".into(),
                scope_version: "1.0".into(),
                scope_attributes: vec![Kv::new("scope.attr", "a")],
                unit: "s".into(),
                description: "request latency".into(),
            }),
            window_start_ms: end_ms - 5_000,
            window_end_ms: end_ms,
            payload: sketch.snapshot().expect("ASAPv1"),
            metric_name: "data.asap_view.1".into(),
            count: 50,
            aggregation_temporality: 1,
            value: 0.0,
        }
    }

    /// `pdata` as the OTLP `ExportMetricsServiceRequest` an OTLP exporter sends.
    fn otlp_request(
        pdata: OtapPdata,
    ) -> (
        Vec<u8>,
        otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::MetricsData,
    ) {
        use otel_arrow_dfe_pdata::otlp::OtlpProtoBytes;
        use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
        let (_context, payload) = pdata.into_parts();
        let bytes: OtlpProtoBytes = payload.try_into_with_default().expect("to OTLP");
        let body = bytes.as_bytes().to_vec();
        let OtlpProtoMessage::Metrics(data) = OtlpProtoMessage::try_from(bytes).expect("decodes")
        else {
            panic!("metrics");
        };
        (body, data)
    }

    /// Encode (OTAP Arrow) → decode: the view identity, name, Resource,
    /// Scope, metric descriptor, point attributes, window and count survive.
    #[test]
    fn view_state_round_trips_through_otap_arrow() {
        let state = q01_state(1_767_225_600_000);
        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&state)).expect("encode");
        let decoded = decode_pdata_to_observations(pdata).expect("decode");
        let [obs] = decoded.observations.as_slice() else {
            panic!("one state");
        };
        let env = obs.value.envelope.as_ref().expect("a state");
        assert_eq!(env.view, state.view);
        assert_eq!(env.sketch_type, SketchType::DDSketch, "from θ in the key");
        assert_eq!(env.metric_name, "data.asap_view.1");
        assert_eq!(env.resource_labels, state.resource_labels);
        assert_eq!(env.labels, state.labels, "scope attributes are not labels");
        assert_eq!(env.source, state.source);
        assert_eq!(obs.source, state.source);
        assert_eq!(
            (env.window_start_ms, env.window_end_ms),
            (state.window_start_ms, state.window_end_ms)
        );
        assert_eq!(env.count, 50);
        assert_eq!(env.payload, state.payload);
    }

    /// Through OTAP Arrow and the OTAP → OTLP conversion, the state is an
    /// OTLP Summary point with the raw sample's structure, and the view
    /// identity is `Metric.metadata`; decoding the OTLP request again gives
    /// the same identity.
    #[test]
    fn view_identity_is_otlp_metric_metadata() {
        use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::any_value::Value;
        use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric::Data;
        use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;

        let state = q01_state(1_767_225_600_000);
        let pdata = encode_envelopes_to_pdata(std::slice::from_ref(&state)).expect("encode");
        let (_, data) = otlp_request(pdata);
        let [resource] = data.resource_metrics.as_slice() else {
            panic!("one resource");
        };
        let attrs = |kvs: &[otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::KeyValue]| {
            kvs.iter()
                .map(|kv| {
                    let value = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                        Some(Value::StringValue(s)) => s.clone(),
                        Some(Value::IntValue(i)) => i.to_string(),
                        Some(Value::BytesValue(b)) => format!("<{} bytes>", b.len()),
                        other => format!("{other:?}"),
                    };
                    (kv.key.clone(), value)
                })
                .collect::<Vec<_>>()
        };
        let owned = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            attrs(&resource.resource.as_ref().unwrap().attributes),
            owned(&[("service.name", "j0")]),
            "the received Resource, no sketch.* attributes"
        );
        let [scope] = resource.scope_metrics.as_slice() else {
            panic!("one scope");
        };
        let scope_id = scope.scope.as_ref().unwrap();
        assert_eq!(
            (scope_id.name.as_str(), scope_id.version.as_str()),
            ("app.meter", "1.0")
        );
        assert_eq!(attrs(&scope_id.attributes), owned(&[("scope.attr", "a")]));
        let [metric] = scope.metrics.as_slice() else {
            panic!("one metric");
        };
        assert_eq!(metric.name, "data.asap_view.1");
        assert_eq!(
            (metric.unit.as_str(), metric.description.as_str()),
            ("s", "request latency")
        );
        let view = state.view.as_ref().unwrap();
        assert_eq!(
            attrs(&metric.metadata),
            owned(&[
                ("asap.plan_id", view.plan_id.as_str()),
                ("asap.node_id", "1"),
                ("asap.view_state_key", Q01_KEY),
            ])
        );
        let Some(Data::Summary(summary)) = &metric.data else {
            panic!("a Summary");
        };
        let [point] = summary.data_points.as_slice() else {
            panic!("one point");
        };
        assert_eq!(
            attrs(&point.attributes),
            owned(&[
                ("region", "r0"),
                ("service", "s0"),
                ("label_0", "l0"),
                (
                    "sketch.envelope",
                    &format!("<{} bytes>", state.payload.len())
                ),
            ])
        );
        assert_eq!(point.start_time_unix_nano, 1_767_225_595_000_000_000);
        assert_eq!(point.time_unix_nano, 1_767_225_600_000_000_000);
        assert_eq!(point.count, 50);

        // Back from OTLP: the same identity.
        let pdata = OtapPdata::new_todo_context(
            OtapPayload::try_from(OtlpProtoMessage::Metrics(data)).expect("from OTLP"),
        );
        let decoded = decode_pdata_to_observations(pdata).expect("decode");
        let env = decoded.observations[0].value.envelope.as_ref().unwrap();
        assert_eq!(env.view, state.view);
        assert_eq!(env.payload, state.payload);
    }

    /// Two views over one source metric are two Metric entries, each with its
    /// own identity.
    #[test]
    fn two_views_of_one_series_are_two_metrics() {
        let first = q01_state(1_767_225_600_000);
        let mut second = first.clone();
        second.view = Some(Arc::new(ViewIdentity {
            node_id: 7,
            ..(**first.view.as_ref().unwrap()).clone()
        }));
        second.metric_name = "data.asap_view.7".into();
        let pdata = encode_envelopes_to_pdata(&[first, second]).expect("encode");
        let (_, data) = otlp_request(pdata);
        let metrics: Vec<_> = data.resource_metrics[0].scope_metrics[0]
            .metrics
            .iter()
            .map(|m| m.name.as_str())
            .collect();
        assert_eq!(metrics, ["data.asap_view.1", "data.asap_view.7"]);
    }

    /// Metadata naming only part of a view identity is refused.
    #[test]
    fn partial_view_identity_is_refused_on_decode() {
        use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
        let pdata = encode_envelopes_to_pdata(&[q01_state(1_767_225_600_000)]).expect("encode");
        let (_, mut data) = otlp_request(pdata);
        data.resource_metrics[0].scope_metrics[0].metrics[0]
            .metadata
            .retain(|kv| kv.key != "asap.node_id");
        let pdata = OtapPdata::new_todo_context(
            OtapPayload::try_from(OtlpProtoMessage::Metrics(data)).expect("from OTLP"),
        );
        let error = decode_pdata_to_observations(pdata).expect_err("refused");
        assert!(error.to_string().contains("asap.node_id"), "{error}");
    }

    /// The golden OTLP request body for q01 (the backend's test fixture):
    /// one series' DDSketch state of the window ending at
    /// 2026-01-01T00:00:00Z, as `tests/fixtures/collector-views/q01/otlp-body.pb`
    /// holds it. `ASAP_UPDATE_GOLDEN=1` rewrites the file.
    #[test]
    fn q01_otlp_body_matches_golden() {
        const GOLDEN: &str = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/collector-views/q01/otlp-body.pb"
        );
        let mut state = q01_state(1_767_225_600_000);
        // As an application's OTel SDK leaves them: no scope, unit or
        // description.
        state.source = Arc::default();
        let pdata = encode_envelopes_to_pdata(&[state]).expect("encode");
        let (body, _) = otlp_request(pdata);
        if std::env::var_os("ASAP_UPDATE_GOLDEN").is_some() {
            std::fs::write(GOLDEN, &body).expect("write golden");
        }
        assert_eq!(body, std::fs::read(GOLDEN).expect("golden exists"));
    }
}
