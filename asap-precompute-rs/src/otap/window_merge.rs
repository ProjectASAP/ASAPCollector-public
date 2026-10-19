//! `urn:asap:processor:window_merge`: merges sketches from many sources by
//! the window they describe, not by when they arrive.
//!
//! Each input envelope names its series (labels, sketch type, agg id) and its
//! window. Envelopes with the same series and window are merged. A window is
//! emitted once, `grace` after its end on this host's clock; envelopes for a
//! window already emitted are counted as late and dropped. Scalar (non-sketch)
//! inputs pass through unchanged.
//!
//! ```yaml
//! type: urn:asap:processor:window_merge
//! config: {grace: 5s}
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

use super::answer_nodes::{send_gauges, stage_timing};
use super::codec::{decode_pdata_to_observations, OtapSketchEncoder};
use crate::envelope::SketchEnvelope;
use crate::observation::ObservationValueKind;
use crate::precompute::{PrecomputeError, Sketch};
use crate::sketches::decode_snapshot;

/// Registered URN.
pub const WINDOW_MERGE_URN: &str = "urn:asap:processor:window_merge";

/// Processor configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowMergeConfig {
    /// How long after a window's end to wait for the last source.
    #[serde(default = "default_grace", with = "humantime_serde")]
    pub grace: Duration,
}

fn default_grace() -> Duration {
    Duration::from_secs(5)
}

struct Pending {
    envelope: SketchEnvelope,
    sketch: Box<dyn Sketch>,
}

/// Windows waiting for their grace period, keyed by window end then series.
#[derive(Default)]
pub struct WindowMerger {
    windows: BTreeMap<u64, BTreeMap<String, Pending>>,
    /// Every window ending at or before this has been emitted.
    emitted_until: u64,
    /// Envelopes that arrived after their window was emitted.
    pub late: u64,
    /// When the last source's sketch of each open window arrived (ms).
    arrived: BTreeMap<u64, u64>,
}

fn series_key(e: &SketchEnvelope) -> String {
    let mut labels: Vec<_> = e.labels.iter().map(|kv| (&kv.key, &kv.value)).collect();
    labels.sort();
    let mut resource: Vec<_> = e
        .resource_labels
        .iter()
        .map(|kv| (&kv.key, &kv.value))
        .collect();
    resource.sort();
    format!(
        "{}|{}|{}|{}|{:?}|{:?}",
        e.metric_name,
        e.sketch_type.name(),
        e.agg_id,
        e.window_start_ms,
        resource,
        labels
    )
}

impl WindowMerger {
    /// Adds one source's sketch for a window.
    pub fn add(&mut self, envelope: SketchEnvelope) -> Result<(), PrecomputeError> {
        if envelope.window_end_ms <= self.emitted_until {
            self.late += 1;
            return Ok(());
        }
        let _ = self.arrived.insert(envelope.window_end_ms, now_ms());
        let window = self.windows.entry(envelope.window_end_ms).or_default();
        match window.entry(series_key(&envelope)) {
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                let pending = slot.get_mut();
                pending.sketch.merge_snapshot(&envelope.payload)?;
                pending.envelope.count += envelope.count;
            }
            std::collections::btree_map::Entry::Vacant(slot) => {
                let sketch = decode_snapshot(envelope.sketch_type, &envelope.payload)?;
                slot.insert(Pending { envelope, sketch });
            }
        }
        Ok(())
    }

    /// Removes and returns when the last sketch of window `end` arrived.
    pub fn take_arrival(&mut self, end: u64) -> Option<u64> {
        self.arrived.remove(&end)
    }

    /// Merged envelopes of every window that ended `grace` or more before `now_ms`.
    pub fn due(&mut self, now_ms: u64, grace: Duration) -> Vec<SketchEnvelope> {
        let cutoff = now_ms.saturating_sub(grace.as_millis() as u64);
        let open = self.windows.split_off(&(cutoff + 1));
        let ready = std::mem::replace(&mut self.windows, open);
        let mut out = Vec::new();
        for (end, series) in ready {
            self.emitted_until = self.emitted_until.max(end);
            for (_, mut p) in series {
                if let Ok(payload) = p.sketch.snapshot() {
                    p.envelope.payload = payload;
                    out.push(p.envelope);
                }
            }
        }
        out
    }
}

struct WindowMergeProcessor {
    config: WindowMergeConfig,
    merger: WindowMerger,
    encoder: OtapSketchEncoder,
    timer_started: bool,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl WindowMergeProcessor {
    async fn emit(
        &mut self,
        envs: Vec<SketchEnvelope>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if envs.is_empty() {
            return Ok(());
        }
        match self.encoder.encode(&envs) {
            Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
            Err(e) => {
                effects
                    .info(&format!(
                        "window_merge: dropping window after encode failure: {e}"
                    ))
                    .await
            }
        }
        // Merged windows (not passthrough) carry this stage's timing along to
        // the detector.
        let now = now_ms();
        let windows: BTreeMap<u64, u64> = envs
            .iter()
            .filter(|e| !e.payload.is_empty())
            .map(|e| (e.window_end_ms, e.window_start_ms))
            .collect();
        let timing: Vec<SketchEnvelope> = windows
            .into_iter()
            .flat_map(|(end, start)| {
                let arrived = self.merger.take_arrival(end).unwrap_or(now);
                stage_timing(start, end, "sketch", "merge", arrived, now)
            })
            .collect();
        send_gauges(&timing, effects).await
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for WindowMergeProcessor {
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
                let observations = match decode_pdata_to_observations(pdata) {
                    Ok(outcome) => outcome.observations,
                    Err(e) => {
                        effects
                            .info(&format!("window_merge: dropping undecodable input: {e}"))
                            .await;
                        return Ok(());
                    }
                };
                let mut passthrough = Vec::new();
                for obs in observations {
                    match (obs.value.kind, obs.value.envelope) {
                        (ObservationValueKind::Envelope, Some(env)) if !env.payload.is_empty() => {
                            if let Err(e) = self.merger.add(*env) {
                                effects.info(&format!("window_merge: {e}")).await;
                            }
                        }
                        (ObservationValueKind::Envelope, Some(env)) => passthrough.push(*env),
                        _ => {}
                    }
                }
                self.emit(passthrough, effects).await
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => {
                let due = self.merger.due(now_ms(), self.config.grace);
                self.emit(due, effects).await
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => {
                let all = self.merger.due(u64::MAX, Duration::ZERO);
                self.emit(all, effects).await
            }
            _ => Ok(()),
        }
    }
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static WINDOW_MERGE: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: WINDOW_MERGE_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: WindowMergeConfig = serde_json::from_value(node_config.config.clone())
            .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: format!("window_merge: {e}"),
            })?;
        Ok(ProcessorWrapper::local(
            WindowMergeProcessor {
                config,
                merger: WindowMerger::default(),
                encoder: OtapSketchEncoder::default(),
                timer_started: false,
            },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<WindowMergeConfig>,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::SketchType;
    use crate::observation::KeyValue;
    use crate::precompute::CardinalitySketch;
    use crate::sketches::HLLWrapper;

    fn hll(values: std::ops::Range<u32>, start: u64) -> SketchEnvelope {
        let mut sk = HLLWrapper::new();
        values.for_each(|v| sk.update(&v.to_le_bytes()));
        SketchEnvelope {
            sketch_type: SketchType::HLLSketch,
            labels: vec![KeyValue::new("attribute.key", "user.id")],
            window_start_ms: start,
            window_end_ms: start + 60_000,
            payload: sk.snapshot().unwrap(),
            metric_name: "asap.distinct".into(),
            count: 1000,
            ..Default::default()
        }
    }

    #[test]
    fn merges_sources_per_window_after_grace() {
        let mut m = WindowMerger::default();
        m.add(hll(0..1000, 0)).unwrap();
        m.add(hll(500..1500, 0)).unwrap();
        m.add(hll(0..10, 60_000)).unwrap();
        let grace = Duration::from_secs(5);
        assert!(m.due(64_000, grace).is_empty());
        let out = m.due(65_000, grace);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].count, 2000);
        let merged = HLLWrapper::from_snapshot(&out[0].payload).unwrap();
        let d = merged.estimate_cardinality();
        assert!((d - 1500.0).abs() < 1500.0 * 0.05, "{d}");
        // A source arriving after its window was emitted is late.
        m.add(hll(0..5, 0)).unwrap();
        assert_eq!(m.late, 1);
        assert_eq!(m.due(125_000, grace).len(), 1);
    }

    #[test]
    fn remembers_when_a_window_last_got_a_sketch() {
        let mut m = WindowMerger::default();
        let before = now_ms();
        m.add(hll(0..10, 0)).unwrap();
        let arrived = m.take_arrival(60_000).unwrap();
        assert!(arrived >= before && arrived <= now_ms());
        assert_eq!(m.take_arrival(60_000), None);
    }

    #[test]
    fn stage_timing_is_seconds_after_the_window_end() {
        let gauges =
            super::super::answer_nodes::stage_timing(0, 60_000, "sketch", "merge", 61_500, 65_250);
        let get = |name: &str| gauges.iter().find(|g| g.metric_name == name).unwrap();
        assert_eq!(get("asap.stage.received").value, 1.5);
        assert_eq!(get("asap.stage.emitted").value, 5.25);
        assert!(get("asap.stage.emitted")
            .labels
            .contains(&KeyValue::new("stage", "merge")));
    }
}
