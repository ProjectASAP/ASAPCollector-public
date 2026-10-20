//! `urn:asap:processor:heavy_hitter_accuracy` watches the answers of the
//! sketch and exact pipelines (their detectors each send it a copy) and, per
//! window and top-k series, reports how well the sketch found and counted the
//! heavy hitters: keys with at least `phi` of the window's points. It sits
//! outside both pipelines and only reads their output.
//!
//! Gauges, labelled with the series labels, `phi` and `method="sketch"`: `asap.hh.precision`,
//! `asap.hh.recall`, `asap.hh.f1`, `asap.hh.are` (average relative count error
//! over the true positives), `asap.hh.truth` and `asap.hh.reported`. A window
//! whose exact top-k may hide more heavy hitters (`k` entries, all above the
//! threshold) gets only `asap.hh.incomplete`.
//!
//! ```yaml
//! accuracy:
//!   type: urn:asap:processor:heavy_hitter_accuracy
//!   config: {phi: 0.01, k: 20}   # k: the detectors' top-k length
//! ```
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::AckMsg;
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

use super::answer_nodes::{default_k, gauges_pdata, invalid, observations};
use crate::answers::heavy_hitter_accuracy;
use crate::envelope::SketchEnvelope;
use crate::observation::{KeyValue, Observation};

/// Registered URN of the heavy-hitter accuracy observer.
pub const HH_ACCURACY_URN: &str = "urn:asap:processor:heavy_hitter_accuracy";

/// Windows kept waiting for the other method's answers.
const MAX_OPEN_WINDOWS: usize = 16;

fn default_phi() -> f64 {
    0.01
}

/// `heavy_hitter_accuracy` configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HhAccuracyConfig {
    /// A heavy hitter has at least this fraction of the window's points.
    #[serde(default = "default_phi")]
    pub phi: f64,
    /// Length of the detectors' top-k lists.
    #[serde(default = "default_k")]
    pub k: usize,
}

/// One method's answers for one window and series.
#[derive(Default)]
struct Answers {
    points: Option<f64>,
    /// `(rank, key, count)`.
    top: Vec<(u32, String, f64)>,
}

/// Window end, then series labels (without `method`, `key`, `rank`, `bound`).
type Key = (u64, Vec<(String, String)>);

struct HhAccuracy {
    config: HhAccuracyConfig,
    open: BTreeMap<Key, (Answers, Answers)>,
}

fn label<'a>(obs: &'a Observation, key: &str) -> Option<&'a str> {
    obs.labels
        .iter()
        .find(|kv| kv.key == key)
        .map(|kv| kv.value.as_str())
}

impl HhAccuracy {
    /// Records one detector answer: a top-k estimate or a window's points.
    fn observe(&mut self, obs: &Observation) {
        let topk = obs.metric == "asap.topk" && label(obs, "bound") == Some("estimate");
        if !topk && obs.metric != "asap.points" {
            return;
        }
        let method = match label(obs, "method") {
            Some("sketch") => 0,
            Some("exact") => 1,
            _ => return,
        };
        let mut series: Vec<(String, String)> = obs
            .labels
            .iter()
            .filter(|kv| !["method", "key", "rank", "bound"].contains(&kv.key.as_str()))
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        series.sort();
        let entry = self.open.entry((obs.timestamp_ms, series)).or_default();
        let answers = if method == 0 {
            &mut entry.0
        } else {
            &mut entry.1
        };
        if topk {
            let rank = label(obs, "rank").and_then(|r| r.parse().ok());
            if let (Some(rank), Some(key)) = (rank, label(obs, "key")) {
                answers.top.push((rank, key.to_string(), obs.value.float));
            }
        } else {
            answers.points = Some(obs.value.float);
        }
    }

    /// Accuracy gauges of the windows that have both methods' answers.
    fn ready(&mut self) -> Vec<SketchEnvelope> {
        let done: Vec<Key> = self
            .open
            .iter()
            .filter(|(_, (s, e))| e.points.is_some() && !s.top.is_empty() && !e.top.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        let mut out = Vec::new();
        for key in done {
            let (sketch, exact) = self.open.remove(&key).expect("listed above");
            out.extend(self.gauges(&key, sketch, exact));
        }
        // A window one method never answered (e.g. an idle series).
        while self.open.len() > MAX_OPEN_WINDOWS {
            self.open.pop_first();
        }
        out
    }

    fn gauges(&self, (end, series): &Key, sketch: Answers, exact: Answers) -> Vec<SketchEnvelope> {
        let list = |mut a: Answers| {
            a.top.sort_by_key(|t| t.0);
            a.top
                .into_iter()
                .map(|(_, k, c)| (k, c))
                .collect::<Vec<_>>()
        };
        let points = exact.points.unwrap_or(0.0);
        let mut labels: Vec<KeyValue> = series.iter().map(|(k, v)| KeyValue::new(k, v)).collect();
        labels.push(KeyValue::new("phi", self.config.phi.to_string()));
        // The sketch's accuracy; the store-and-query job reports `method="raw"`.
        labels.push(KeyValue::new("method", "sketch"));
        let gauge = |metric: &str, value: f64| SketchEnvelope {
            labels: labels.clone(),
            metric_name: metric.into(),
            window_end_ms: *end,
            window_start_ms: *end,
            value,
            ..Default::default()
        };
        let (k, phi) = (self.config.k, self.config.phi);
        let Some(acc) = heavy_hitter_accuracy(phi, points, k, &list(exact), &list(sketch)) else {
            return vec![gauge("asap.hh.incomplete", 1.0)];
        };
        let mut out = vec![
            gauge("asap.hh.precision", acc.precision),
            gauge("asap.hh.recall", acc.recall),
            gauge("asap.hh.f1", acc.f1),
            gauge("asap.hh.truth", acc.truth as f64),
            gauge("asap.hh.reported", acc.reported as f64),
        ];
        out.extend(
            acc.average_relative_error
                .map(|are| gauge("asap.hh.are", are)),
        );
        out
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for HhAccuracy {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let Message::PData(pdata) = msg else {
            return Ok(());
        };
        effects.notify_ack(AckMsg::new(pdata.clone())).await?;
        for obs in observations(pdata, effects).await {
            self.observe(&obs);
        }
        let out = self.ready();
        if out.is_empty() {
            return Ok(());
        }
        match gauges_pdata(&out) {
            Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
            Err(e) => {
                effects
                    .info(&format!("hh_accuracy: encode failed: {e}"))
                    .await
            }
        }
        Ok(())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static HH_ACCURACY: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: HH_ACCURACY_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: HhAccuracyConfig =
            serde_json::from_value(node_config.config.clone()).map_err(invalid)?;
        Ok(ProcessorWrapper::local(
            HhAccuracy {
                config,
                open: BTreeMap::new(),
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<HhAccuracyConfig>,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::ObservationValue;

    fn answer(method: &str, metric: &str, extra: &[(&str, &str)], value: f64) -> Observation {
        let mut labels = vec![
            KeyValue::new("attribute.key", "http.route"),
            KeyValue::new("method", method),
        ];
        labels.extend(extra.iter().map(|(k, v)| KeyValue::new(*k, *v)));
        Observation::new(
            60_000,
            metric,
            vec![],
            labels,
            ObservationValue::float(value),
        )
    }

    #[test]
    fn reports_a_window_once_both_methods_answered() {
        let mut node = HhAccuracy {
            config: HhAccuracyConfig { phi: 0.1, k: 20 },
            open: BTreeMap::new(),
        };
        let top = |method: &str, rank: &str, key: &str, v: f64| {
            answer(
                method,
                "asap.topk",
                &[("rank", rank), ("key", key), ("bound", "estimate")],
                v,
            )
        };
        node.observe(&answer("sketch", "asap.points", &[], 1000.0));
        node.observe(&top("sketch", "1", "a", 410.0));
        node.observe(&top("sketch", "2", "d", 105.0));
        // The sketch's lower bounds are not estimates.
        node.observe(&answer(
            "sketch",
            "asap.topk",
            &[("rank", "1"), ("key", "a"), ("bound", "lower")],
            300.0,
        ));
        assert!(node.ready().is_empty(), "waits for exact");
        node.observe(&answer("exact", "asap.points", &[], 1000.0));
        node.observe(&top("exact", "1", "a", 400.0));
        node.observe(&top("exact", "2", "c", 100.0));
        node.observe(&top("exact", "3", "d", 90.0));
        let out = node.ready();
        let get = |m: &str| out.iter().find(|e| e.metric_name == m).map(|e| e.value);
        assert_eq!(get("asap.hh.precision"), Some(0.5));
        assert_eq!(get("asap.hh.recall"), Some(0.5));
        assert_eq!(get("asap.hh.are"), Some(0.025));
        assert!(out.iter().all(|e| e.window_end_ms == 60_000
            && e.labels
                .contains(&KeyValue::new("attribute.key", "http.route"))
            && e.labels.contains(&KeyValue::new("phi", "0.1"))));
        assert!(node.open.is_empty());
    }
}
