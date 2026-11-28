//! `urn:asap:processor:collector_views`: builds the states of an installed
//! plan's collector views on ASAPPlanner's executor
//! ([`super::view_executor`]) and sends them as OTLP metrics, each a Summary
//! point with the ASAPv1 state in `sketch.envelope`, the series' Resource,
//! Scope and attributes, and the view identity as metric metadata
//! ([`super::codec`]).
//!
//! It loads the control plane's `collector-views.json`
//! ([`super::planner_views::load`]); a file the collector cannot execute
//! fails start-up. Each received message is acknowledged and its points of
//! the views' metrics are built; other points are ignored. A window closes
//! when a later sample of its metric passes it, or on the idle timer once the
//! wall clock is the processor's idle-close grace past its end, as
//! `asap_sketches` closes its tumbling windows; shutdown closes every open
//! window.
//!
//! ```yaml
//! collector_views:
//!   type: urn:asap:processor:collector_views
//!   config: {planner_views: /etc/asap/collector-views.json}
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ProcessorConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::{AckMsg, NodeControlMsg};
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::local::processor as local;
use otel_arrow_dfe_engine::message::Message;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::processor::ProcessorWrapper;
use otel_arrow_dfe_engine::MessageSourceLocalEffectHandlerExtension as _;
use otel_arrow_dfe_engine::{ConsumerEffectHandlerExtension, ProcessorFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_PROCESSOR_FACTORIES;
use serde::Deserialize;

use super::codec::{decode_each, encode_envelopes_to_pdata};
use super::planner_views::load;
use super::processor::tumbling_idle_close_now_ms;
use super::view_executor::{Closed, ViewExecutor};

/// Registered URN.
pub const COLLECTOR_VIEWS_URN: &str = "urn:asap:processor:collector_views";

/// Processor configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorViewsConfig {
    /// The control plane's `collector-views.json`.
    pub planner_views: PathBuf,
}

/// The idle timer's period: a second, or the narrowest window if narrower.
const MAX_TIMER_PERIOD: Duration = Duration::from_secs(1);

struct CollectorViews {
    executor: ViewExecutor,
    timer_period: Duration,
    timer_started: bool,
}

impl CollectorViews {
    /// Sends the closed windows' states downstream as one message. A lost
    /// window, or states that do not encode, are logged and dropped, as
    /// `asap_sketches` drops a window it cannot encode.
    async fn emit(
        &mut self,
        closed: Closed,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        for failure in &closed.failures {
            effects
                .info(&format!("collector_views: dropping window: {failure}"))
                .await;
        }
        if closed.states.is_empty() {
            return Ok(());
        }
        match encode_envelopes_to_pdata(&closed.states) {
            Ok(pdata) => effects.send_message_with_source_node(pdata).await?,
            Err(error) => {
                effects
                    .info(&format!("collector_views: dropping windows: {error}"))
                    .await
            }
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl local::Processor<OtapPdata> for CollectorViews {
    async fn process(
        &mut self,
        msg: Message<OtapPdata>,
        effects: &mut local::EffectHandler<OtapPdata>,
    ) -> Result<(), Error> {
        if !self.timer_started {
            // Without the timer, idle windows close only at shutdown.
            self.timer_started = effects
                .start_periodic_timer(self.timer_period)
                .await
                .is_ok();
        }
        match msg {
            Message::PData(pdata) => {
                effects.notify_ack(AckMsg::new(pdata.clone())).await?;
                let before = self.executor.dropped();
                let executor = &mut self.executor;
                if let Err(error) = decode_each(pdata, |obs| executor.observe(obs)) {
                    effects
                        .info(&format!("collector_views: dropping input: {error}"))
                        .await;
                }
                let after = self.executor.dropped();
                let (late, rejected) = (after.late - before.late, after.rejected - before.rejected);
                if late + rejected > 0 {
                    effects
                        .info(&format!(
                            "collector_views: dropped {late} sample(s) of closed windows, \
                             {rejected} value(s) a view's sketch refuses"
                        ))
                        .await;
                }
                let closed = self.executor.flush();
                self.emit(closed, effects).await
            }
            Message::Control(NodeControlMsg::TimerTick { .. }) => {
                let closed = self.executor.close_idle(tumbling_idle_close_now_ms());
                self.emit(closed, effects).await
            }
            Message::Control(NodeControlMsg::Shutdown { .. }) => {
                let closed = self.executor.drain();
                self.emit(closed, effects).await
            }
            Message::Control(_) => Ok(()),
        }
    }
}

/// Loads and compiles the configured file, refusing one the collector
/// cannot execute.
fn create(node_config: &NodeUserConfig) -> Result<CollectorViews, String> {
    let config: CollectorViewsConfig =
        serde_json::from_value(node_config.config.clone()).map_err(|e| e.to_string())?;
    let views = load(&config.planner_views).map_err(|e| e.to_string())?;
    let narrowest = views
        .views
        .iter()
        .map(|v| Duration::from_millis(v.tumbling_window_width_ms))
        .min()
        .unwrap_or(MAX_TIMER_PERIOD);
    let executor = ViewExecutor::new(&views).map_err(|e| format!("planner_views: {e}"))?;
    Ok(CollectorViews {
        executor,
        timer_period: narrowest.min(MAX_TIMER_PERIOD),
        timer_started: false,
    })
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_PROCESSOR_FACTORIES)]
static COLLECTOR_VIEWS: ProcessorFactory<OtapPdata> = ProcessorFactory {
    name: COLLECTOR_VIEWS_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             processor_config: &ProcessorConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let processor = create(&node_config).map_err(|error| {
            otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: format!("collector_views: {error}"),
            }
        })?;
        Ok(ProcessorWrapper::local(
            processor,
            node,
            node_config,
            processor_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<CollectorViewsConfig>,
};
