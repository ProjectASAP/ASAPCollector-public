//! Two processors that turn telemetry into [`crate::answers`] gauges:
//!
//! * `urn:asap:processor:sketch_detector` reads merged sketch windows (the
//!   output of `window_merge`) and emits each window's answers with bounds.
//! * `urn:asap:processor:exact_answers` reads raw data points, groups them
//!   into event-time windows, and emits the exact answers: the ground truth
//!   and the exact baseline.
//!
//! ```yaml
//! detector:
//!   type: urn:asap:processor:sketch_detector
//!   config: {quantiles: [0.5, 0.99], k: 10}
//! exact:
//!   type: urn:asap:processor:exact_answers
//!   config:
//!     window: 60s
//!     group_by: [http.route]      # quantile series, as aggregate_by
//!     per_attribute: [user.id]    # distinct values and top-k per key
//! ```
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::AckMsg;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::ConsumerEffectHandlerExtension;
use otel_arrow_dfe_engine::{MessageSourceLocalEffectHandlerExtension, ProcessorFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use serde::Deserialize;

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs, SeverityNumber,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::OtapPayload;

use super::codec::decode_pdata_to_observations;
use crate::answers::{
    exact_answers, ks_distance, sketch_answers, topk_of, CardinalityClassifier, CardinalityRules,
    DriftRules, DriftTracker, Event, ExactWindow, NoveltyTracker,
};
use crate::envelope::SketchEnvelope;
use crate::envelope::SketchType;
use crate::observation::{KeyValue, Observation, ObservationValueKind};
use crate::precompute::QuantileSketch;
use crate::sketches::KLLWrapper;
use std::collections::{HashMap, VecDeque};

/// Registered URN of the sketch detector.
pub const SKETCH_DETECTOR_URN: &str = "urn:asap:processor:sketch_detector";
/// Registered URN of the exact baseline.
pub const EXACT_ANSWERS_URN: &str = "urn:asap:processor:exact_answers";

fn default_quantiles() -> Vec<f64> {
    vec![0.5, 0.99]
}
fn default_k() -> usize {
    10
}
fn default_window() -> Duration {
    Duration::from_secs(60)
}
fn default_grace() -> Duration {
    Duration::from_secs(5)
}

/// `sketch_detector` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectorConfig {
    /// Quantiles answered from KLL windows.
    #[serde(default = "default_quantiles")]
    pub quantiles: Vec<f64>,
    /// Heavy hitters answered from CMS + heap windows.
    #[serde(default = "default_k")]
    pub k: usize,
    /// Thresholds for classifying per-attribute cardinality.
    #[serde(default)]
    pub cardinality: CardinalityRules,
    /// Thresholds for distribution drift of quantile series.
    #[serde(default)]
    pub drift: DriftRules,
    /// Windows whose top keys form the novelty baseline.
    #[serde(default = "default_novelty_windows")]
    pub novelty_windows: usize,
}

fn default_novelty_windows() -> usize {
    10
}

/// `exact_answers` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactConfig {
    /// Event-time window size.
    #[serde(default = "default_window", with = "humantime_serde")]
    pub window: Duration,
    /// Wait after a window's end before answering it.
    #[serde(default = "default_grace", with = "humantime_serde")]
    pub grace: Duration,
    /// Labels identifying a quantile series; empty disables quantiles.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Attribute keys whose distinct values and heavy hitters are answered.
    #[serde(default)]
    pub per_attribute: Vec<String>,
    /// Quantiles to answer.
    #[serde(default = "default_quantiles")]
    pub quantiles: Vec<f64>,
    /// Heavy hitters per attribute key.
    #[serde(default = "default_k")]
    pub k: usize,
    /// Thresholds for classifying per-attribute cardinality.
    #[serde(default)]
    pub cardinality: CardinalityRules,
    /// Thresholds for distribution drift of quantile series.
    #[serde(default)]
    pub drift: DriftRules,
    /// Windows whose top keys form the novelty baseline.
    #[serde(default = "default_novelty_windows")]
    pub novelty_windows: usize,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Plain OTLP gauges with every label on the data point, so backends such
/// as Prometheus keep them as series labels.
fn gauges_pdata(envs: &[SketchEnvelope]) -> Result<OtapPdata, String> {
    let mut by_metric: BTreeMap<&str, Vec<NumberDataPoint>> = BTreeMap::new();
    for e in envs {
        let attributes: Vec<OtlpKeyValue> = e
            .labels
            .iter()
            .map(|kv| OtlpKeyValue::new(kv.key.clone(), AnyValue::new_string(kv.value.clone())))
            .collect();
        by_metric.entry(&e.metric_name).or_default().push(
            NumberDataPoint::build()
                .start_time_unix_nano(e.window_start_ms * 1_000_000)
                .time_unix_nano(e.window_end_ms * 1_000_000)
                .value_double(e.value)
                .attributes(attributes)
                .finish(),
        );
    }
    let metrics = by_metric
        .into_iter()
        .map(|(name, points)| {
            Metric::build()
                .name(name.to_string())
                .data_gauge(Gauge::new(points))
                .finish()
        })
        .collect::<Vec<_>>();
    let data = MetricsData::new(vec![ResourceMetrics::new(
        Resource::build().finish(),
        vec![ScopeMetrics::new(
            InstrumentationScope::build().name("asap.answers").finish(),
            metrics,
        )],
    )]);
    let payload: OtapPayload = OtlpProtoMessage::Metrics(data)
        .try_into()
        .map_err(|e| format!("{e:?}"))?;
    Ok(OtapPdata::new_todo_context(payload))
}

/// Detection events as OTLP log records (for Loki or any log backend).
fn events_pdata(events: &[Event]) -> Result<OtapPdata, String> {
    let records = events
        .iter()
        .map(|e| {
            let mut attributes = vec![
                OtlpKeyValue::new("event.kind", AnyValue::new_string(e.kind.clone())),
                OtlpKeyValue::new("method", AnyValue::new_string(e.method.clone())),
            ];
            attributes.extend(e.attributes.iter().map(|kv| {
                OtlpKeyValue::new(kv.key.clone(), AnyValue::new_string(kv.value.clone()))
            }));
            LogRecord::build()
                .time_unix_nano(e.window_end_ms * 1_000_000)
                .observed_time_unix_nano(e.window_end_ms * 1_000_000)
                .severity_number(SeverityNumber::Info)
                .severity_text("INFO")
                .body(AnyValue::new_string(e.body.clone()))
                .attributes(attributes)
                .finish()
        })
        .collect::<Vec<_>>();
    let data = LogsData::new(vec![ResourceLogs::new(
        Resource::build()
            .attributes(vec![OtlpKeyValue::new(
                "service.name",
                AnyValue::new_string("asap-detector"),
            )])
            .finish(),
        vec![ScopeLogs::new(
            InstrumentationScope::build().name("asap.events").finish(),
            records,
        )],
    )]);
    let payload: OtapPayload = OtlpProtoMessage::Logs(data)
        .try_into()
        .map_err(|e| format!("{e:?}"))?;
    Ok(OtapPdata::new_todo_context(payload))
}

async fn emit(
    envs: &[SketchEnvelope],
    events: &[Event],
    effects: &mut local::EffectHandler<OtapPdata>,
) -> Result<(), Error> {
    if !envs.is_empty() {
        match gauges_pdata(envs) {
            Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
            Err(e) => effects.info(&format!("answers: encode failed: {e}")).await,
        }
    }
    if !events.is_empty() {
        match events_pdata(events) {
            Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
            Err(e) => {
                effects
                    .info(&format!("answers: event encode failed: {e}"))
                    .await
            }
        }
    }
    Ok(())
}

async fn observations(
    pdata: OtapPdata,
    effects: &mut local::EffectHandler<OtapPdata>,
) -> Vec<Observation> {
    match decode_pdata_to_observations(pdata) {
        Ok(outcome) => outcome.observations,
        Err(e) => {
            effects
                .info(&format!("answers: undecodable input: {e}"))
                .await;
            Vec::new()
        }
    }
}

struct SketchDetector {
    config: DetectorConfig,
    cardinality: CardinalityClassifier,
    novelty: NoveltyTracker,
    drift: DriftTracker,
    /// Recent KLL windows per series, the drift baseline.
    baselines: HashMap<String, VecDeque<KLLWrapper>>,
}

fn series_key(env: &SketchEnvelope) -> String {
    format!("{}|{:?}", env.metric_name, env.labels)
}

/// Percentile grid of both distributions: where their CDFs are compared.
fn grid(a: &dyn QuantileSketch, b: &dyn QuantileSketch) -> Vec<f64> {
    (1..100)
        .flat_map(|i| [a.quantile(i as f64 / 100.0), b.quantile(i as f64 / 100.0)])
        .collect()
}

impl SketchDetector {
    /// KS distance of this KLL window from the merge of the previous ones.
    fn drift(&mut self, env: &SketchEnvelope) -> (Vec<SketchEnvelope>, Option<Event>) {
        let Ok(current) = KLLWrapper::from_snapshot(&env.payload) else {
            return (Vec::new(), None);
        };
        let window = self.baselines.entry(series_key(env)).or_default();
        let mut result = (Vec::new(), None);
        if window.len() >= 3 && env.count >= self.drift.rules.min_points {
            let mut baseline = KLLWrapper::new(current.inner().k() as i32, None);
            for w in window.iter() {
                let _ = crate::precompute::Sketch::merge(&mut baseline, w);
            }
            fn cdf(s: &KLLWrapper) -> impl Fn(f64) -> f64 + '_ {
                let n = s.inner().count().max(1) as f64;
                move |x| s.inner().rank(x) as f64 / n
            }
            let ks = ks_distance(cdf(&current), cdf(&baseline), &grid(&current, &baseline));
            result = self.drift.observe(
                env,
                "sketch",
                ks,
                baseline.quantile(0.5),
                current.quantile(0.5),
            );
        }
        window.push_back(current);
        if window.len() > self.drift.rules.baseline_windows {
            window.pop_front();
        }
        result
    }
}

/// The `bound="estimate"` value of `metric` among `answers`.
fn estimate_of(answers: &[SketchEnvelope], metric: &str) -> Option<f64> {
    answers
        .iter()
        .find(|e| {
            e.metric_name == metric
                && e.labels
                    .iter()
                    .any(|kv| kv.key == "bound" && kv.value == "estimate")
        })
        .map(|e| e.value)
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for SketchDetector {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let Message::PData(pdata) = msg else {
            return Ok(());
        };
        // Consumed into state here: ack so upstream (e.g. fanout) can move on.
        effects.notify_ack(AckMsg::new(pdata.clone())).await?;
        let (mut out, mut events) = (Vec::new(), Vec::new());
        for obs in observations(pdata, effects).await {
            let Some(env) = obs.value.envelope.filter(|e| !e.payload.is_empty()) else {
                continue;
            };
            match sketch_answers(&env, &self.config.quantiles, self.config.k) {
                Ok(answers) => {
                    if env.sketch_type == SketchType::KLLSketch {
                        let (gauges, event) = self.drift(&env);
                        out.extend(gauges);
                        events.extend(event);
                    }
                    let top = topk_of(&answers);
                    if !top.is_empty() {
                        let (gauges, new) = self.novelty.observe(&env, "sketch", &top);
                        out.extend(gauges);
                        events.extend(new);
                    }
                    if let Some(d) = estimate_of(&answers, "asap.distinct") {
                        let (gauges, event) =
                            self.cardinality.observe(&env, "sketch", d, env.count);
                        out.extend(gauges);
                        events.extend(event);
                    }
                    out.extend(answers);
                }
                Err(e) => effects.info(&format!("sketch_detector: {e}")).await,
            }
        }
        emit(&out, &events, effects).await
    }
}

/// Exact state of one window: per quantile series and per attribute key.
#[derive(Default)]
struct ExactState {
    series: BTreeMap<String, (SketchEnvelope, ExactWindow)>,
}

struct ExactAnswers {
    config: ExactConfig,
    cardinality: CardinalityClassifier,
    novelty: NoveltyTracker,
    drift: DriftTracker,
    /// Recent windows' sorted values per quantile series.
    baselines: HashMap<String, VecDeque<Vec<f64>>>,
    /// Open windows by window end.
    windows: BTreeMap<u64, ExactState>,
    emitted_until: u64,
    timer_started: bool,
}

impl ExactAnswers {
    fn template(obs: &Observation, labels: Vec<KeyValue>, start: u64, end: u64) -> SketchEnvelope {
        SketchEnvelope {
            labels,
            window_start_ms: start,
            window_end_ms: end,
            metric_name: obs.metric.clone(),
            ..Default::default()
        }
    }

    fn observe(&mut self, obs: &Observation) {
        if obs.value.kind != ObservationValueKind::Float {
            return;
        }
        let size = self.config.window.as_millis().max(1) as u64;
        let start = obs.timestamp_ms / size * size;
        let end = start + size;
        if end <= self.emitted_until {
            return;
        }
        let state = self.windows.entry(end).or_default();
        let label = |k: &str| {
            obs.labels
                .iter()
                .find(|kv| kv.key == k)
                .map(|kv| kv.value.clone())
        };
        if !self.config.group_by.is_empty() {
            let labels: Vec<KeyValue> = self
                .config
                .group_by
                .iter()
                .filter_map(|k| label(k).map(|v| KeyValue::new(k.clone(), v)))
                .collect();
            let key = format!("q|{}|{labels:?}", obs.metric);
            let (_, w) = state.series.entry(key).or_insert_with(|| {
                (
                    Self::template(obs, labels, start, end),
                    ExactWindow::default(),
                )
            });
            w.values.push(obs.value.float);
            w.points += 1;
        }
        let all = self.config.per_attribute.iter().any(|k| k == "*");
        let keys: Vec<String> = if all {
            obs.labels.iter().map(|kv| kv.key.clone()).collect()
        } else {
            self.config.per_attribute.clone()
        };
        for attr in &keys {
            let Some(value) = label(attr) else { continue };
            let key = format!("a|{}|{attr}", obs.metric);
            let (_, w) = state.series.entry(key).or_insert_with(|| {
                let labels = vec![KeyValue::new("attribute.key", attr.clone())];
                (
                    Self::template(obs, labels, start, end),
                    ExactWindow::default(),
                )
            });
            *w.counts.entry(value.clone()).or_default() += 1;
            w.distinct.insert(value);
            w.points += 1;
        }
    }

    fn due(&mut self, now: u64) -> (Vec<SketchEnvelope>, Vec<Event>) {
        let cutoff = now.saturating_sub(self.config.grace.as_millis() as u64);
        let open = self.windows.split_off(&(cutoff + 1));
        let ready = std::mem::replace(&mut self.windows, open);
        let (mut out, mut events) = (Vec::new(), Vec::new());
        for (end, state) in ready {
            self.emitted_until = self.emitted_until.max(end);
            for (_, (mut template, mut w)) in state.series {
                template.count = w.points;
                if !w.distinct.is_empty() {
                    let distinct = w.distinct.len() as f64;
                    let (gauges, event) = self
                        .cardinality
                        .observe(&template, "exact", distinct, w.points);
                    out.extend(gauges);
                    events.extend(event);
                }
                let answers =
                    exact_answers(&template, &mut w, &self.config.quantiles, self.config.k);
                let top = topk_of(&answers);
                if !top.is_empty() {
                    let (gauges, new) = self.novelty.observe(&template, "exact", &top);
                    out.extend(gauges);
                    events.extend(new);
                }
                out.extend(answers);
                if !w.values.is_empty() {
                    // exact_answers left the values sorted.
                    let (gauges, event) =
                        self.exact_drift(&template, std::mem::take(&mut w.values));
                    out.extend(gauges);
                    events.extend(event);
                }
            }
        }
        (out, events)
    }
}

impl ExactAnswers {
    /// Exact KS distance of a window's sorted values from the previous windows.
    fn exact_drift(
        &mut self,
        template: &SketchEnvelope,
        sorted: Vec<f64>,
    ) -> (Vec<SketchEnvelope>, Option<Event>) {
        let window = self.baselines.entry(series_key(template)).or_default();
        let mut result = (Vec::new(), None);
        if window.len() >= 3 && sorted.len() as u64 >= self.drift.rules.min_points {
            let mut baseline: Vec<f64> = window.iter().flatten().copied().collect();
            baseline.sort_unstable_by(f64::total_cmp);
            let cdf = |v: &[f64], x: f64| v.partition_point(|&y| y <= x) as f64 / v.len() as f64;
            let at = |v: &[f64], q: f64| v[((v.len() - 1) as f64 * q).round() as usize];
            let xs: Vec<f64> = (1..100)
                .flat_map(|i| {
                    [
                        at(&sorted, i as f64 / 100.0),
                        at(&baseline, i as f64 / 100.0),
                    ]
                })
                .collect();
            let ks = ks_distance(|x| cdf(&sorted, x), |x| cdf(&baseline, x), &xs);
            result =
                self.drift
                    .observe(template, "exact", ks, at(&baseline, 0.5), at(&sorted, 0.5));
        }
        window.push_back(sorted);
        if window.len() > self.drift.rules.baseline_windows {
            window.pop_front();
        }
        result
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for ExactAnswers {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if !self.timer_started {
            self.timer_started = effects
                .start_periodic_timer(Duration::from_secs(1))
                .await
                .is_ok();
        }
        match msg {
            Message::PData(pdata) => {
                // Consumed into state here: ack so upstream (e.g. fanout) can move on.
                effects.notify_ack(AckMsg::new(pdata.clone())).await?;
                for obs in observations(pdata, effects).await {
                    self.observe(&obs);
                }
                Ok(())
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => {
                let (out, events) = self.due(now_ms());
                emit(&out, &events, effects).await
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => {
                let (out, events) = self.due(u64::MAX);
                emit(&out, &events, effects).await
            }
            _ => Ok(()),
        }
    }
}

fn invalid(e: impl std::fmt::Display) -> otel_arrow_dfe_config::error::Error {
    otel_arrow_dfe_config::error::Error::InvalidUserConfig {
        error: e.to_string(),
    }
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static SKETCH_DETECTOR: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: SKETCH_DETECTOR_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: DetectorConfig =
            serde_json::from_value(node_config.config.clone()).map_err(invalid)?;
        Ok(ProcessorWrapper::local(
            SketchDetector {
                cardinality: CardinalityClassifier::new(config.cardinality.clone()),
                novelty: NoveltyTracker::new(config.novelty_windows),
                drift: DriftTracker::new(config.drift.clone()),
                baselines: HashMap::new(),
                config,
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<DetectorConfig>,
};

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static EXACT_ANSWERS: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: EXACT_ANSWERS_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: ExactConfig =
            serde_json::from_value(node_config.config.clone()).map_err(invalid)?;
        Ok(ProcessorWrapper::local(
            ExactAnswers {
                cardinality: CardinalityClassifier::new(config.cardinality.clone()),
                novelty: NoveltyTracker::new(config.novelty_windows),
                drift: DriftTracker::new(config.drift.clone()),
                baselines: HashMap::new(),
                config,
                windows: BTreeMap::new(),
                emitted_until: 0,
                timer_started: false,
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<ExactConfig>,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::ObservationValue;

    fn point(ts: u64, route: &str, user: &str, v: f64) -> Observation {
        Observation {
            timestamp_ms: ts,
            metric: "latency".into(),
            resource_labels: vec![],
            labels: vec![
                KeyValue::new("http.route", route),
                KeyValue::new("user.id", user),
            ],
            value: ObservationValue::float(v),
        }
    }

    #[test]
    fn exact_windows_by_event_time_after_grace() {
        let config: ExactConfig = serde_json::from_value(serde_json::json!({
            "group_by": ["http.route"], "per_attribute": ["user.id"], "k": 2
        }))
        .unwrap();
        let mut exact = ExactAnswers {
            cardinality: CardinalityClassifier::new(config.cardinality.clone()),
            novelty: NoveltyTracker::new(config.novelty_windows),
            drift: DriftTracker::new(config.drift.clone()),
            baselines: HashMap::new(),
            config,
            windows: BTreeMap::new(),
            emitted_until: 0,
            timer_started: false,
        };
        for i in 0..100u64 {
            exact.observe(&point(1_000 + i, "/a", &format!("u{}", i % 4), i as f64));
        }
        exact.observe(&point(61_000, "/a", "u0", 1.0));
        assert!(exact.due(64_000).0.is_empty());
        let (out, _) = exact.due(65_000);
        let get = |metric: &str, extra: (&str, &str)| {
            out.iter()
                .find(|e| {
                    e.metric_name == metric
                        && e.labels
                            .iter()
                            .any(|kv| kv.key == extra.0 && kv.value == extra.1)
                })
                .map(|e| e.value)
        };
        assert_eq!(get("asap.quantile", ("quantile", "0.5")), Some(50.0));
        assert_eq!(
            get("asap.distinct", ("attribute.key", "user.id")),
            Some(4.0)
        );
        assert_eq!(get("asap.topk", ("rank", "1")), Some(25.0));
        assert!(out.iter().all(|e| e.window_end_ms == 60_000));
        // A late point for the emitted window is dropped.
        exact.observe(&point(2_000, "/a", "u9", 1.0));
        assert!(exact
            .due(125_000)
            .0
            .iter()
            .all(|e| e.window_end_ms == 120_000));
    }
}
