//! `urn:asap:processor:raw_forward`: the raw path to the backend, minus the
//! metrics whose only readers are collector views.
//!
//! It loads the same `collector-views.json` as the collector views
//! ([`super::planner_views`]) and drops every metric the file marks
//! `backend_reads_raw: false`; every other metric, including one the plan
//! does not read, passes with its context (a message that drops nothing is
//! forwarded as received; one that drops some is re-encoded as OTLP). A
//! message left with no metric is acknowledged instead of forwarded, and one
//! that does not convert to OTLP is refused (nack).
//!
//! ```yaml
//! raw_forward:
//!   type: urn:asap:processor:raw_forward
//!   config: {planner_views: /etc/asap/collector-views.json}
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NackMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::MessageSourceLocalEffectHandlerExtension as _;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ProcessorFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use otel_arrow_dfe_pdata::otlp::OtlpProtoBytes;
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::MetricsData;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::{OtapPayload, TryIntoWithOptions};
use serde::Deserialize;

use super::planner_views::{load, PlannerViews};

/// Registered URN.
pub const RAW_FORWARD_URN: &str = "urn:asap:processor:raw_forward";

/// Processor configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawForwardConfig {
    /// The control plane's `collector-views.json`.
    pub planner_views: PathBuf,
}

/// Removes from `data` every metric `views` marks `backend_reads_raw:
/// false`, and the scopes and resources left empty; returns how many metrics
/// it removed.
pub fn drop_collector_only_metrics(data: &mut MetricsData, views: &PlannerViews) -> usize {
    let mut dropped = 0;
    for resource in &mut data.resource_metrics {
        for scope in &mut resource.scope_metrics {
            let before = scope.metrics.len();
            scope
                .metrics
                .retain(|metric| !views.drops_raw(&metric.name));
            dropped += before - scope.metrics.len();
        }
        resource
            .scope_metrics
            .retain(|scope| !scope.metrics.is_empty());
    }
    data.resource_metrics
        .retain(|resource| !resource.scope_metrics.is_empty());
    dropped
}

/// What [`filter`] does with one message.
pub enum Filtered {
    /// Forward this message: as received, or without the dropped metrics.
    Forward(OtapPdata),
    /// Every metric was dropped: acknowledge this message upstream.
    Consumed(OtapPdata),
    /// The message does not convert to OTLP: refuse it upstream.
    Refused {
        /// Why.
        reason: String,
        /// The message as received.
        pdata: OtapPdata,
    },
}

/// Applies [`drop_collector_only_metrics`] to one message. A message of
/// another signal, or one that drops nothing, is forwarded as received.
pub fn filter(pdata: OtapPdata, views: &PlannerViews) -> Filtered {
    if !views.backend_reads_raw.values().any(|reads| !reads) {
        return Filtered::Forward(pdata);
    }
    // Arrow batches and OTLP bytes are reference-counted: a cheap copy to
    // forward or refuse as received.
    let received = pdata.clone();
    match without_collector_only_metrics(pdata, views) {
        Ok(Some(filtered)) => filtered,
        Ok(None) => Filtered::Forward(received),
        Err(reason) => Filtered::Refused {
            reason,
            pdata: received,
        },
    }
}

/// `pdata` without the metrics `views` drops; `None` when it drops none.
fn without_collector_only_metrics(
    pdata: OtapPdata,
    views: &PlannerViews,
) -> Result<Option<Filtered>, String> {
    let (context, payload) = pdata.into_parts();
    let bytes: OtlpProtoBytes = payload
        .try_into_with_default()
        .map_err(|e| format!("to OTLP: {e}"))?;
    let OtlpProtoMessage::Metrics(mut data) =
        OtlpProtoMessage::try_from(bytes).map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    if drop_collector_only_metrics(&mut data, views) == 0 {
        return Ok(None);
    }
    let empty = data.resource_metrics.is_empty();
    let payload = OtapPayload::try_from(OtlpProtoMessage::Metrics(data))
        .map_err(|e| format!("from OTLP: {e}"))?;
    let pdata = OtapPdata::new(context, payload);
    Ok(Some(if empty {
        Filtered::Consumed(pdata)
    } else {
        Filtered::Forward(pdata)
    }))
}

struct RawForward {
    views: PlannerViews,
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for RawForward {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        let Message::PData(pdata) = msg else {
            return Ok(());
        };
        match filter(pdata, &self.views) {
            Filtered::Forward(pdata) => effects.send_message_with_source_node(pdata).await?,
            Filtered::Consumed(pdata) => effects.notify_ack(AckMsg::new(pdata)).await?,
            Filtered::Refused { reason, pdata } => {
                effects
                    .notify_nack(NackMsg::new(format!("raw_forward: {reason}"), pdata))
                    .await?
            }
        }
        Ok(())
    }
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static RAW_FORWARD: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: RAW_FORWARD_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let config: RawForwardConfig =
            serde_json::from_value(node_config.config.clone()).map_err(|e| {
                otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                    error: format!("raw_forward: {e}"),
                }
            })?;
        let views = load(&config.planner_views).map_err(|e| {
            otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: format!("raw_forward: {e}"),
            }
        })?;
        Ok(ProcessorWrapper::local(
            RawForward { views },
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<RawForwardConfig>,
};

#[cfg(test)]
mod tests {
    use super::*;
    use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
        metric::Data, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };

    fn views() -> PlannerViews {
        super::super::planner_views::parse(include_str!(
            "../../tests/fixtures/collector-views/q01/collector-views.json"
        ))
        .expect("q01 loads")
    }

    fn gauge(name: &str) -> Metric {
        Metric {
            name: name.into(),
            data: Some(Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint::default()],
            })),
            ..Default::default()
        }
    }

    fn data(names: &[&str]) -> MetricsData {
        MetricsData::new(vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: names.iter().map(|n| gauge(n)).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }])
    }

    fn names(pdata: OtapPdata) -> Vec<String> {
        let (_, payload) = pdata.into_parts();
        let bytes: OtlpProtoBytes = payload.try_into_with_default().unwrap();
        let OtlpProtoMessage::Metrics(data) = OtlpProtoMessage::try_from(bytes).unwrap() else {
            panic!("metrics");
        };
        data.resource_metrics
            .iter()
            .flat_map(|r| &r.scope_metrics)
            .flat_map(|s| &s.metrics)
            .map(|m| m.name.clone())
            .collect()
    }

    fn pdata(names: &[&str]) -> OtapPdata {
        OtapPdata::new_todo_context(
            OtapPayload::try_from(OtlpProtoMessage::Metrics(data(names))).unwrap(),
        )
    }

    /// q01 alone reads `data` only through its collector view, so the raw
    /// forward drops `data`; a metric the plan does not read passes.
    #[test]
    fn drops_only_metrics_the_backend_does_not_read_raw() {
        let Filtered::Forward(out) = filter(pdata(&["data", "other"]), &views()) else {
            panic!("other is forwarded");
        };
        assert_eq!(names(out), ["other"]);
    }

    /// A message of only such metrics is acknowledged, not forwarded.
    #[test]
    fn a_message_left_empty_is_consumed() {
        assert!(matches!(
            filter(pdata(&["data"]), &views()),
            Filtered::Consumed(_)
        ));
    }

    /// With every metric still read raw, nothing is decoded or dropped.
    #[test]
    fn metrics_the_backend_reads_raw_pass() {
        let mut views = views();
        views.backend_reads_raw.insert("data".into(), true);
        let Filtered::Forward(out) = filter(pdata(&["data"]), &views) else {
            panic!("forwarded");
        };
        assert_eq!(names(out), ["data"]);
    }
}
