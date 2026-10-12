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
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::{MessageSourceLocalEffectHandlerExtension, ProcessorFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use serde::Deserialize;

use super::codec::{decode_pdata_to_observations, OtapSketchEncoder};
use crate::answers::{exact_answers, sketch_answers, ExactWindow};
use crate::envelope::SketchEnvelope;
use crate::observation::{KeyValue, Observation, ObservationValueKind};

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
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

async fn emit(
    encoder: &mut OtapSketchEncoder,
    envs: &[SketchEnvelope],
    effects: &mut local::EffectHandler<OtapPdata>,
) -> Result<(), Error> {
    if envs.is_empty() {
        return Ok(());
    }
    match encoder.encode(envs) {
        Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
        Err(e) => effects.info(&format!("answers: encode failed: {e}")).await,
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
    encoder: OtapSketchEncoder,
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
        let mut out = Vec::new();
        for obs in observations(pdata, effects).await {
            let Some(env) = obs.value.envelope.filter(|e| !e.payload.is_empty()) else {
                continue;
            };
            match sketch_answers(&env, &self.config.quantiles, self.config.k) {
                Ok(answers) => out.extend(answers),
                Err(e) => effects.info(&format!("sketch_detector: {e}")).await,
            }
        }
        emit(&mut self.encoder, &out, effects).await
    }
}

/// Exact state of one window: per quantile series and per attribute key.
#[derive(Default)]
struct ExactState {
    series: BTreeMap<String, (SketchEnvelope, ExactWindow)>,
}

struct ExactAnswers {
    config: ExactConfig,
    /// Open windows by window end.
    windows: BTreeMap<u64, ExactState>,
    emitted_until: u64,
    encoder: OtapSketchEncoder,
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
        for attr in &self.config.per_attribute {
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

    fn due(&mut self, now: u64) -> Vec<SketchEnvelope> {
        let cutoff = now.saturating_sub(self.config.grace.as_millis() as u64);
        let open = self.windows.split_off(&(cutoff + 1));
        let ready = std::mem::replace(&mut self.windows, open);
        let mut out = Vec::new();
        for (end, state) in ready {
            self.emitted_until = self.emitted_until.max(end);
            for (_, (mut template, mut w)) in state.series {
                template.count = w.points;
                out.extend(exact_answers(
                    &template,
                    &mut w,
                    &self.config.quantiles,
                    self.config.k,
                ));
            }
        }
        out
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
                for obs in observations(pdata, effects).await {
                    self.observe(&obs);
                }
                Ok(())
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => {
                let out = self.due(now_ms());
                emit(&mut self.encoder, &out, effects).await
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => {
                let out = self.due(u64::MAX);
                emit(&mut self.encoder, &out, effects).await
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
                config,
                encoder: OtapSketchEncoder::default(),
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
                config,
                windows: BTreeMap::new(),
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
        let mut exact = ExactAnswers {
            config: serde_json::from_value(serde_json::json!({
                "group_by": ["http.route"], "per_attribute": ["user.id"], "k": 2
            }))
            .unwrap(),
            windows: BTreeMap::new(),
            emitted_until: 0,
            encoder: OtapSketchEncoder::default(),
            timer_started: false,
        };
        for i in 0..100u64 {
            exact.observe(&point(1_000 + i, "/a", &format!("u{}", i % 4), i as f64));
        }
        exact.observe(&point(61_000, "/a", "u0", 1.0));
        assert!(exact.due(64_000).is_empty());
        let out = exact.due(65_000);
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
            .iter()
            .all(|e| e.window_end_ms == 120_000));
    }
}
