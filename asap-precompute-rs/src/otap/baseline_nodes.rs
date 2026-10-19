//! The exact baseline and the raw baseline, as separate pipelines that share
//! no processor with the sketch pipeline; only the generated input is the same.
//!
//! * `urn:asap:processor:exact_branch` (one per source) keeps every raw point
//!   of each event-time window as an [`ExactWindow`]: the values of each
//!   quantile series and a value -> count map per attribute key. It emits a
//!   window when data crosses its end, or 5 s after its end if the stream idles.
//! * `urn:asap:processor:exact_merge` adds up the sources' windows, `grace`
//!   after each window's end.
//! * `urn:asap:processor:exact_detector` computes each merged window's exact
//!   answers and events, with `method="exact"`: the ground truth.
//! * `urn:asap:processor:raw_sink` ends the raw pipeline, which only forwards
//!   raw points: it writes every point as one log record to a store through
//!   its `logs` output and watches the store's acknowledgements. For each
//!   event-time window it reports on its `latency` output when the store
//!   acknowledged the window's last point (`asap.answer.delay`,
//!   `method="raw"`), the per-point time from event to acknowledgement
//!   (`asap.raw.transit`) and rejected points (`asap.raw.failed`).
//!
//! ```yaml
//! exact_branch:
//!   type: urn:asap:processor:exact_branch
//!   config: {window: 60s, group_by: [http.route], per_attribute: [user.id]}
//! exact_merge:
//!   type: urn:asap:processor:exact_merge
//!   config: {grace: 5s}
//! exact_detector:
//!   type: urn:asap:processor:exact_detector
//!   config: {quantiles: [0.5, 0.99], k: 10}
//! raw_sink:
//!   type: urn:asap:processor:raw_sink
//!   outputs: [logs, latency]
//!   config: {window: 60s, grace: 5s}
//! ```
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, CallData, Context8u8, NodeControlMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::{
    ConsumerEffectHandlerExtension, Interests, ProducerEffectHandlerExtension,
};
use otel_arrow_dfe_engine::{MessageSourceLocalEffectHandlerExtension, ProcessorFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::OtapPayload;
use serde::Deserialize;

use super::answer_nodes::{
    default_grace, default_k, default_quantiles, default_window, emit, gauges_pdata, invalid,
    now_ms, observations, series_key,
};
use super::codec::OtapSketchEncoder;
use crate::answers::{exact_answers, gauge, ExactWindow};
use crate::envelope::{SketchEnvelope, SketchType};

use crate::observation::{KeyValue, Observation, ObservationValueKind};

/// Registered URN of the exact branch.
pub const EXACT_BRANCH_URN: &str = "urn:asap:processor:exact_branch";
/// Registered URN of the exact merge.
pub const EXACT_MERGE_URN: &str = "urn:asap:processor:exact_merge";
/// Registered URN of the exact detector.
pub const EXACT_DETECTOR_URN: &str = "urn:asap:processor:exact_detector";
/// Registered URN of the raw pipeline's sink.
pub const RAW_SINK_URN: &str = "urn:asap:processor:raw_sink";
/// `raw_sink` output with the log records for the store.
const LOGS_PORT: &str = "logs";
/// `raw_sink` output with the latency gauges.
const LATENCY_PORT: &str = "latency";

/// Wait after a window's end before the idle timer closes it, as in
/// `asap_sketches`: data crossing the end closes it on time.
const IDLE_CLOSE_GRACE: Duration = Duration::from_secs(5);

/// `exact_branch` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactBranchConfig {
    /// Event-time window size.
    #[serde(default = "default_window", with = "humantime_serde")]
    pub window: Duration,
    /// Labels identifying a quantile series, as `aggregate_by`; empty
    /// disables quantiles.
    #[serde(default)]
    pub group_by: Vec<String>,
    /// Attribute keys whose value counts are kept; `*` for every key.
    #[serde(default)]
    pub per_attribute: Vec<String>,
}

/// `exact_merge` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactMergeConfig {
    /// How long after a window's end to wait for the last source.
    #[serde(default = "default_grace", with = "humantime_serde")]
    pub grace: Duration,
}

/// `exact_detector` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactDetectorConfig {
    /// Quantiles to answer.
    #[serde(default = "default_quantiles")]
    pub quantiles: Vec<f64>,
    /// Heavy hitters per attribute key.
    #[serde(default = "default_k")]
    pub k: usize,
}

/// `raw_sink` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSinkConfig {
    /// Event-time window size.
    #[serde(default = "default_window", with = "humantime_serde")]
    pub window: Duration,
    /// Wait after a window's end before reporting it.
    #[serde(default = "default_grace", with = "humantime_serde")]
    pub grace: Duration,
}

/// Open windows by window end, then series.
type Windows = BTreeMap<u64, BTreeMap<String, (SketchEnvelope, ExactWindow)>>;

/// Removes and encodes every window ending at or before `until`.
fn close(windows: &mut Windows, until: u64) -> Vec<SketchEnvelope> {
    let open = windows.split_off(&(until.saturating_add(1)));
    std::mem::replace(windows, open)
        .into_values()
        .flat_map(|series| series.into_values())
        .map(|(mut env, w)| {
            env.count = w.points;
            env.payload = w.encode();
            env
        })
        .collect()
}

async fn send(
    encoder: &mut OtapSketchEncoder,
    envs: &[SketchEnvelope],
    effects: &mut local::EffectHandler<OtapPdata>,
) -> Result<(), Error> {
    if envs.is_empty() {
        return Ok(());
    }
    match encoder.encode(envs) {
        Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
        Err(e) => effects.info(&format!("exact: encode failed: {e}")).await,
    }
    Ok(())
}

struct ExactBranch {
    config: ExactBranchConfig,
    windows: Windows,
    /// Every window ending at or before this has been emitted.
    emitted_until: u64,
    encoder: OtapSketchEncoder,
    timer_started: bool,
}

impl ExactBranch {
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
        let template = |labels: Vec<KeyValue>| SketchEnvelope {
            schema_version: 1,
            sketch_type: SketchType::Exact,
            labels,
            window_start_ms: start,
            window_end_ms: end,
            metric_name: obs.metric.clone(),
            ..Default::default()
        };
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
            let (_, w) = state
                .entry(format!("q|{labels:?}"))
                .or_insert_with(|| (template(labels), ExactWindow::default()));
            w.values.push(obs.value.float);
            w.points += 1;
        }
        let all = self.config.per_attribute.iter().any(|k| k == "*");
        for kv in &obs.labels {
            if !all && !self.config.per_attribute.contains(&kv.key) {
                continue;
            }
            let (_, w) = state.entry(format!("a|{}", kv.key)).or_insert_with(|| {
                let labels = vec![KeyValue::new("attribute.key", kv.key.clone())];
                (template(labels), ExactWindow::default())
            });
            *w.counts.entry(kv.value.clone()).or_default() += 1;
            w.points += 1;
        }
    }

    fn close(&mut self, until: u64) -> Vec<SketchEnvelope> {
        let out = close(&mut self.windows, until);
        if let Some(end) = out.iter().map(|e| e.window_end_ms).max() {
            self.emitted_until = self.emitted_until.max(end);
        }
        out
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for ExactBranch {
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
        let out = match msg {
            Message::PData(pdata) => {
                // Consumed into state here: ack so upstream can move on.
                effects.notify_ack(AckMsg::new(pdata.clone())).await?;
                let points = observations(pdata, effects).await;
                // A window closes as soon as a point's event time passes its end.
                let out = match points.first() {
                    Some(first) => self.close(first.timestamp_ms),
                    None => Vec::new(),
                };
                points.iter().for_each(|obs| self.observe(obs));
                out
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => {
                self.close(now_ms().saturating_sub(IDLE_CLOSE_GRACE.as_millis() as u64))
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => self.close(u64::MAX),
            _ => Vec::new(),
        };
        send(&mut self.encoder, &out, effects).await
    }
}

struct ExactMerge {
    config: ExactMergeConfig,
    windows: Windows,
    emitted_until: u64,
    encoder: OtapSketchEncoder,
    timer_started: bool,
}

impl ExactMerge {
    /// Adds one source's window; windows already emitted drop it as late.
    fn add(&mut self, env: SketchEnvelope) -> Result<(), crate::precompute::PrecomputeError> {
        if env.window_end_ms <= self.emitted_until {
            return Ok(());
        }
        let w = ExactWindow::decode(&env.payload)?;
        let key = format!("{}|{}", series_key(&env), env.window_start_ms);
        let window = self.windows.entry(env.window_end_ms).or_default();
        match window.get_mut(&key) {
            Some((_, merged)) => merged.merge(w),
            None => {
                window.insert(key, (env, w));
            }
        }
        Ok(())
    }

    fn due(&mut self, now: u64) -> Vec<SketchEnvelope> {
        let cutoff = now.saturating_sub(self.config.grace.as_millis() as u64);
        let out = close(&mut self.windows, cutoff);
        if let Some(end) = out.iter().map(|e| e.window_end_ms).max() {
            self.emitted_until = self.emitted_until.max(end);
        }
        out
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for ExactMerge {
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
        let out = match msg {
            Message::PData(pdata) => {
                effects.notify_ack(AckMsg::new(pdata.clone())).await?;
                for obs in observations(pdata, effects).await {
                    let Some(env) = obs.value.envelope else {
                        continue;
                    };
                    if let Err(e) = self.add(*env) {
                        effects.info(&format!("exact_merge: {e}")).await;
                    }
                }
                Vec::new()
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => self.due(now_ms()),
            Message::Control(NodeControlMsg::Shutdown { .. }) => self.due(u64::MAX),
            _ => Vec::new(),
        };
        send(&mut self.encoder, &out, effects).await
    }
}

struct ExactDetector {
    config: ExactDetectorConfig,
}

impl ExactDetector {
    /// Answers of one merged window.
    fn answer(&mut self, mut template: SketchEnvelope) -> Vec<SketchEnvelope> {
        let mut out = Vec::new();
        let Ok(mut w) = ExactWindow::decode(&template.payload) else {
            return out;
        };
        template.payload = Vec::new();
        template.sketch_type = SketchType::Unspecified;
        template.count = w.points;
        out.extend(exact_answers(
            &template,
            &mut w,
            &self.config.quantiles,
            self.config.k,
        ));
        out
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for ExactDetector {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let Message::PData(pdata) = msg else {
            return Ok(());
        };
        effects.notify_ack(AckMsg::new(pdata.clone())).await?;
        let mut out = Vec::new();
        for obs in observations(pdata, effects).await {
            let Some(env) = obs.value.envelope.filter(|e| !e.payload.is_empty()) else {
                continue;
            };
            out.extend(self.answer(*env));
        }
        emit(&out, effects).await
    }
}

/// What the raw pipeline stored for one event-time window.
#[derive(Default)]
struct RawWindow {
    /// When the store acknowledged the window's last point (ms).
    last_ack_ms: u64,
    /// Store acknowledgement minus event time of every stored point (s).
    transit_s: Vec<f64>,
    /// Points the store rejected.
    failed: u64,
    /// Messages with points of this window still waiting for the store.
    in_flight: usize,
}

struct RawSink {
    config: RawSinkConfig,
    windows: BTreeMap<u64, RawWindow>,
    /// Event times (ms) by window end of every message the store has not
    /// acknowledged yet, by message id.
    in_flight: HashMap<u64, Vec<(u64, Vec<u64>)>>,
    next_id: u64,
    emitted_until: u64,
    timer_started: bool,
}

/// One log record per raw point: event time, the value as the body, the
/// point's attributes.
fn logs_pdata(points: &[Observation]) -> Result<OtapPdata, String> {
    let mut by_resource: BTreeMap<Vec<(String, String)>, Vec<LogRecord>> = BTreeMap::new();
    for p in points {
        let resource = p
            .resource_labels
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        let attributes: Vec<OtlpKeyValue> = p
            .labels
            .iter()
            .map(|kv| OtlpKeyValue::new(kv.key.clone(), AnyValue::new_string(kv.value.clone())))
            .collect();
        by_resource.entry(resource).or_default().push(
            LogRecord::build()
                .time_unix_nano(p.timestamp_ms * 1_000_000)
                .body(AnyValue::new_double(p.value.float))
                .attributes(attributes)
                .finish(),
        );
    }
    let resource_logs: Vec<ResourceLogs> = by_resource
        .into_iter()
        .map(|(resource, records)| {
            let attributes: Vec<OtlpKeyValue> = resource
                .into_iter()
                .map(|(k, v)| OtlpKeyValue::new(k, AnyValue::new_string(v)))
                .collect();
            ResourceLogs::new(
                Resource::build().attributes(attributes).finish(),
                vec![ScopeLogs::new(
                    InstrumentationScope::build().name("asap.raw").finish(),
                    records,
                )],
            )
        })
        .collect();
    let payload: OtapPayload = OtlpProtoMessage::Logs(LogsData::new(resource_logs))
        .try_into()
        .map_err(|e| format!("{e:?}"))?;
    Ok(OtapPdata::new_todo_context(payload))
}

impl RawSink {
    /// Registers a message about to go to the store; returns its id.
    fn send(&mut self, points: &[Observation]) -> u64 {
        let size = self.config.window.as_millis().max(1) as u64;
        let mut by_window: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for p in points {
            let end = p.timestamp_ms / size * size + size;
            if end > self.emitted_until {
                by_window.entry(end).or_default().push(p.timestamp_ms);
            }
        }
        for end in by_window.keys() {
            self.windows.entry(*end).or_default().in_flight += 1;
        }
        let id = self.next_id;
        self.next_id += 1;
        let _ = self.in_flight.insert(id, by_window.into_iter().collect());
        id
    }

    /// The store answered message `id` at `now`.
    fn answered(&mut self, id: u64, now: u64, stored: bool) {
        for (end, times) in self.in_flight.remove(&id).unwrap_or_default() {
            let Some(w) = self.windows.get_mut(&end) else {
                continue;
            };
            w.in_flight = w.in_flight.saturating_sub(1);
            if stored {
                w.last_ack_ms = w.last_ack_ms.max(now);
                w.transit_s
                    .extend(times.iter().map(|t| now.saturating_sub(*t) as f64 / 1000.0));
            } else {
                w.failed += times.len() as u64;
            }
        }
    }

    /// Gauges of every window that ended `grace` or more before `now` and
    /// has no message waiting for the store (or ended 10 graces ago).
    fn due(&mut self, now: u64) -> Vec<SketchEnvelope> {
        let grace = self.config.grace.as_millis() as u64;
        let size = self.config.window.as_millis() as u64;
        let ready: Vec<u64> = self
            .windows
            .iter()
            .filter(|(end, w)| {
                **end + grace <= now && (w.in_flight == 0 || **end + 10 * grace <= now)
            })
            .map(|(end, _)| *end)
            .collect();
        let mut out = Vec::new();
        for end in ready {
            let mut w = self.windows.remove(&end).unwrap_or_default();
            self.emitted_until = self.emitted_until.max(end);
            let template = SketchEnvelope {
                window_start_ms: end - size,
                window_end_ms: end,
                count: w.transit_s.len() as u64,
                ..Default::default()
            };
            let metric = |name: &str, extra: &[(&str, String)], v: f64| {
                let mut e = gauge(&template, name, "raw", extra, v);
                // Window-level gauges: no source metric label.
                e.labels.retain(|kv| kv.key != "metric");
                e
            };
            // Complete once the last point is stored, and not before the
            // window has ended.
            out.push(metric(
                "asap.answer.delay",
                &[],
                w.last_ack_ms.saturating_sub(end) as f64 / 1000.0,
            ));
            out.push(metric("asap.points", &[], w.transit_s.len() as f64));
            out.push(metric("asap.raw.failed", &[], w.failed as f64));
            if !w.transit_s.is_empty() {
                w.transit_s.sort_unstable_by(f64::total_cmp);
                let at =
                    |q: f64| w.transit_s[((w.transit_s.len() - 1) as f64 * q).round() as usize];
                for q in [0.5, 0.99] {
                    out.push(metric(
                        "asap.raw.transit",
                        &[("quantile", q.to_string())],
                        at(q),
                    ));
                }
            }
        }
        out
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for RawSink {
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
        let out = match msg {
            Message::PData(pdata) => {
                effects.notify_ack(AckMsg::new(pdata.clone())).await?;
                let points: Vec<Observation> = observations(pdata, effects)
                    .await
                    .into_iter()
                    .filter(|o| o.value.kind == ObservationValueKind::Float)
                    .collect();
                if points.is_empty() {
                    return Ok(());
                }
                match logs_pdata(&points) {
                    Ok(mut logs) => {
                        let id = self.send(&points);
                        let calldata = CallData::from_vec(vec![Context8u8::from(id)]);
                        effects.subscribe_to(Interests::ACKS_OR_NACKS, calldata, &mut logs);
                        effects
                            .send_message_to(LOGS_PORT, logs)
                            .await
                            .map_err(Error::from)?;
                    }
                    Err(e) => effects.info(&format!("raw_sink: encode failed: {e}")).await,
                }
                return Ok(());
            }
            Message::Control(NodeControlMsg::Ack(ack)) => {
                if let Some(id) = ack.unwind.route.calldata.first() {
                    self.answered(u64::from(*id), now_ms(), true);
                }
                return Ok(());
            }
            Message::Control(NodeControlMsg::Nack(nack)) => {
                if let Some(id) = nack.unwind.route.calldata.first() {
                    self.answered(u64::from(*id), now_ms(), false);
                }
                return Ok(());
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => self.due(now_ms()),
            Message::Control(NodeControlMsg::Shutdown { .. }) => self.due(u64::MAX),
            _ => return Ok(()),
        };
        if out.is_empty() {
            return Ok(());
        }
        // Not through `emit`: the delay here is the last store
        // acknowledgement, not the time this gauge is sent.
        match gauges_pdata(&out) {
            Ok(pdata) => effects
                .send_message_to(LATENCY_PORT, pdata)
                .await
                .map_err(Error::from)?,
            Err(e) => effects.info(&format!("raw_sink: encode failed: {e}")).await,
        }
        Ok(())
    }
}

fn config<T: serde::de::DeserializeOwned>(
    node_config: &NodeUserConfig,
) -> Result<T, otel_arrow_dfe_config::error::Error> {
    serde_json::from_value(node_config.config.clone()).map_err(invalid)
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static EXACT_BRANCH: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: EXACT_BRANCH_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        Ok(ProcessorWrapper::local(
            ExactBranch {
                config: config(&node_config)?,
                windows: Windows::new(),
                emitted_until: 0,
                encoder: OtapSketchEncoder::default(),
                timer_started: false,
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<ExactBranchConfig>,
};

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static EXACT_MERGE: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: EXACT_MERGE_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        Ok(ProcessorWrapper::local(
            ExactMerge {
                config: config(&node_config)?,
                windows: Windows::new(),
                emitted_until: 0,
                encoder: OtapSketchEncoder::default(),
                timer_started: false,
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<ExactMergeConfig>,
};

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static EXACT_DETECTOR: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: EXACT_DETECTOR_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: ExactDetectorConfig = config(&node_config)?;
        Ok(ProcessorWrapper::local(
            ExactDetector { config },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<ExactDetectorConfig>,
};

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static RAW_SINK: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: RAW_SINK_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        Ok(ProcessorWrapper::local(
            RawSink {
                config: config(&node_config)?,
                windows: BTreeMap::new(),
                in_flight: HashMap::new(),
                next_id: 0,
                emitted_until: 0,
                timer_started: false,
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<RawSinkConfig>,
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

    fn branch() -> ExactBranch {
        ExactBranch {
            config: serde_json::from_value(serde_json::json!({
                "group_by": ["http.route"], "per_attribute": ["user.id"]
            }))
            .unwrap(),
            windows: Windows::new(),
            emitted_until: 0,
            encoder: OtapSketchEncoder::default(),
            timer_started: false,
        }
    }

    fn get(out: &[SketchEnvelope], metric: &str, extra: (&str, &str)) -> Option<f64> {
        out.iter()
            .find(|e| {
                e.metric_name == metric
                    && e.labels
                        .iter()
                        .any(|kv| kv.key == extra.0 && kv.value == extra.1)
            })
            .map(|e| e.value)
    }

    #[test]
    fn exact_window_round_trips() {
        let mut w = ExactWindow {
            values: vec![1.5, -2.0],
            points: 3,
            ..Default::default()
        };
        w.counts.insert("a".into(), 2);
        w.counts.insert("bé".into(), 1);
        assert_eq!(ExactWindow::decode(&w.encode()).unwrap(), w);
        assert!(ExactWindow::decode(b"EXv1\x01").is_err());
    }

    #[test]
    fn branches_merge_and_answer_like_one_exact_window() {
        // Two sources, 50 points each in window [0, 60 s); a later point
        // closes the window in each branch.
        let (mut a, mut b) = (branch(), branch());
        for i in 0..50u64 {
            a.observe(&point(1_000 + i, "/a", &format!("u{}", i % 4), i as f64));
            b.observe(&point(
                1_000 + i,
                "/a",
                &format!("u{}", i % 4),
                (50 + i) as f64,
            ));
        }
        let mut sources = a.close(61_000);
        sources.extend(b.close(61_000));
        assert_eq!(
            sources.len(),
            4,
            "a quantile and an attribute series per source"
        );
        let mut merge = ExactMerge {
            config: ExactMergeConfig {
                grace: Duration::from_secs(5),
            },
            windows: Windows::new(),
            emitted_until: 0,
            encoder: OtapSketchEncoder::default(),
            timer_started: false,
        };
        sources.into_iter().for_each(|e| merge.add(e).unwrap());
        assert!(merge.due(64_000).is_empty());
        let merged = merge.due(65_000);
        assert_eq!(merged.len(), 2);
        assert!(merged.iter().all(|e| e.count == 100));
        let config: ExactDetectorConfig =
            serde_json::from_value(serde_json::json!({"k": 2})).unwrap();
        let mut detector = ExactDetector { config };
        let out: Vec<_> = merged
            .into_iter()
            .flat_map(|e| detector.answer(e))
            .collect();
        assert_eq!(get(&out, "asap.quantile", ("quantile", "0.5")), Some(50.0));
        assert_eq!(
            get(&out, "asap.distinct", ("attribute.key", "user.id")),
            Some(4.0)
        );
        assert_eq!(get(&out, "asap.topk", ("rank", "1")), Some(26.0));
        assert!(out.iter().all(|e| e.window_end_ms == 60_000));
        // A late window is dropped.
        merge.add(branch_window(2_000)).unwrap();
        assert!(merge.due(200_000).is_empty());
    }

    fn branch_window(ts: u64) -> SketchEnvelope {
        let mut b = branch();
        b.observe(&point(ts, "/a", "u0", 1.0));
        b.close(u64::MAX).remove(0)
    }

    #[test]
    fn raw_sink_reports_the_last_acknowledgement_after_grace() {
        let mut raw = RawSink {
            config: serde_json::from_value(serde_json::json!({})).unwrap(),
            windows: BTreeMap::new(),
            in_flight: HashMap::new(),
            next_id: 0,
            emitted_until: 0,
            timer_started: false,
        };
        let first = raw.send(&[point(10_000, "/a", "u0", 1.0)]);
        let last = raw.send(&[
            point(59_900, "/a", "u1", 1.0),
            point(59_950, "/b", "u2", 1.0),
        ]);
        let lost = raw.send(&[point(30_000, "/a", "u3", 1.0)]);
        raw.answered(first, 10_500, true);
        raw.answered(lost, 30_100, false);
        // Past the grace, but one message is still waiting for the store.
        assert!(raw.due(65_000).is_empty());
        raw.answered(last, 61_200, true);
        let out = raw.due(65_000);
        assert_eq!(get(&out, "asap.answer.delay", ("method", "raw")), Some(1.2));
        assert_eq!(get(&out, "asap.points", ("method", "raw")), Some(3.0));
        assert_eq!(get(&out, "asap.raw.failed", ("method", "raw")), Some(1.0));
        assert_eq!(
            get(&out, "asap.raw.transit", ("quantile", "0.99")),
            Some(1.3)
        );
    }
}
