//! `urn:asap:processor:sketch_detector` reads merged sketch windows (the
//! output of `window_merge`) and emits each window's answers with bounds. The
//! exact and raw baselines are in [`super::baseline_nodes`].
//!
//! ```yaml
//! detector:
//!   type: urn:asap:processor:sketch_detector
//!   config: {quantiles: [0.5, 0.99], k: 10}
//! ```
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::{MessageSourceLocalEffectHandlerExtension, ProcessorFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use serde::Deserialize;

use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue as OtlpKeyValue,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::OtapPayload;

use super::codec::decode_pdata_to_observations;
use crate::answers::sketch_answers;
use crate::envelope::SketchEnvelope;
use crate::observation::{KeyValue, Observation};

/// Registered URN of the sketch detector.
pub const SKETCH_DETECTOR_URN: &str = "urn:asap:processor:sketch_detector";

pub(super) fn default_quantiles() -> Vec<f64> {
    vec![0.5, 0.99]
}
pub(super) fn default_k() -> usize {
    10
}
pub(super) fn default_window() -> Duration {
    Duration::from_secs(60)
}
pub(super) fn default_grace() -> Duration {
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

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Adds `asap.answer.delay` (seconds from a window's end to its answers) once
/// per window and method: the end-to-end latency of each answer path.
fn with_delay(envs: &[SketchEnvelope], now_ms: u64) -> Vec<SketchEnvelope> {
    let mut out = envs.to_vec();
    let mut seen = std::collections::BTreeSet::new();
    for e in envs {
        let method = e
            .labels
            .iter()
            .find(|kv| kv.key == "method")
            .map(|kv| kv.value.clone());
        if let Some(method) = method {
            if seen.insert((e.window_end_ms, method.clone())) {
                out.push(SketchEnvelope {
                    labels: vec![KeyValue::new("method", method)],
                    metric_name: "asap.answer.delay".into(),
                    window_start_ms: e.window_start_ms,
                    window_end_ms: e.window_end_ms,
                    value: now_ms.saturating_sub(e.window_end_ms) as f64 / 1000.0,
                    ..Default::default()
                });
            }
        }
    }
    out
}

/// Plain OTLP gauges with every label on the data point, so backends such
/// as Prometheus keep them as series labels.
pub(super) fn gauges_pdata(envs: &[SketchEnvelope]) -> Result<OtapPdata, String> {
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

pub(super) async fn emit(
    envs: &[SketchEnvelope],
    effects: &mut local::EffectHandler<OtapPdata>,
) -> Result<(), Error> {
    if envs.is_empty() {
        return Ok(());
    }
    let envs = with_delay(envs, now_ms());
    match gauges_pdata(&envs) {
        Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
        Err(e) => effects.info(&format!("answers: encode failed: {e}")).await,
    }
    Ok(())
}

/// Identity of a series across windows: metric and labels.
pub(super) fn series_key(env: &SketchEnvelope) -> String {
    format!("{}|{:?}", env.metric_name, env.labels)
}

pub(super) async fn observations(
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
        emit(&out, effects).await
    }
}

pub(super) fn invalid(e: impl std::fmt::Display) -> otel_arrow_dfe_config::error::Error {
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
            SketchDetector { config },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<DetectorConfig>,
};
