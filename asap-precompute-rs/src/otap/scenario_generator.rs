//! `urn:asap:receiver:scenario_generator`: seeded metric traffic whose
//! attribute cardinality, key popularity and value distribution follow a
//! scenario, including changes at set times.
//!
//! **Temporary.** The official `urn:otel:receiver:traffic_generator` cannot yet
//! shape attribute values or metric values; these knobs are expected to land
//! there, at which point this receiver is removed. Pacing and control handling
//! follow the official generator's open mode: each second's signals are sent in
//! `max_batch_size` batches, then the receiver waits for the next second.
//!
//! ```yaml
//! type: urn:asap:receiver:scenario_generator
//! config:
//!   seed: 1
//!   signals_per_second: 50000
//!   metric_name: http.server.request.duration
//!   resource_attributes: {service.name: checkout}
//!   attributes:
//!     - {key: http.route, cardinality: 200, zipf: 1.1}
//!     - {key: user.id, unique: true}
//!     - {key: k8s.pod.name, cardinality: 50, growth_per_minute: 0.05}
//!   value: {lognormal: {median: 50.0, sigma: 0.6}}
//!   changes:
//!     - {after_seconds: 1800, attribute: http.route, equals: http.route-3,
//!        value: {lognormal: {median: 100.0, sigma: 0.6}}}
//!     - {after_seconds: 2700, attribute: http.route, inject: new-hot, share: 0.2}
//! ```
//!
//! Attribute values are `<key>-<n>` (`<key>-<seed>-<n>` when unique); with
//! `zipf`, `<key>-0` is the most popular. Every data point is one Gauge value with all configured attributes.
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::config::ReceiverConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::{Error, ReceiverErrorKind};
use otel_arrow_dfe_engine::local::receiver as local;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_engine::{MessageSourceLocalEffectHandlerExtension, ReceiverFactory};
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_pdata::proto::opentelemetry::common::v1::{
    AnyValue, InstrumentationScope, KeyValue,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use otel_arrow_dfe_pdata::proto::opentelemetry::resource::v1::Resource;
use otel_arrow_dfe_pdata::proto::OtlpProtoMessage;
use otel_arrow_dfe_pdata::OtapPayload;
use serde::Deserialize;

/// Registered URN.
pub const SCENARIO_GENERATOR_URN: &str = "urn:asap:receiver:scenario_generator";

/// Receiver configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioConfig {
    /// RNG seed; the same seed and config produce the same values.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Data points per second.
    pub signals_per_second: usize,
    /// Data points per OTLP message.
    #[serde(default = "default_batch")]
    pub max_batch_size: usize,
    /// Metric name of every data point.
    pub metric_name: String,
    /// Resource attributes of every message.
    #[serde(default)]
    pub resource_attributes: BTreeMap<String, String>,
    /// Data-point attributes.
    #[serde(default)]
    pub attributes: Vec<AttributeSpec>,
    /// Value distribution before any change applies.
    pub value: Distribution,
    /// Changes that start at a time since the receiver started.
    #[serde(default)]
    pub changes: Vec<Change>,
}

/// How one attribute's values are drawn.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttributeSpec {
    /// Attribute key.
    pub key: String,
    /// Distinct values at the start.
    #[serde(default = "default_cardinality")]
    pub cardinality: u64,
    /// Zipf exponent over the values; uniform when absent.
    #[serde(default)]
    pub zipf: Option<f64>,
    /// A new value for every data point (identifier-like).
    #[serde(default)]
    pub unique: bool,
    /// Fractional growth of `cardinality` per minute (uniform values only).
    #[serde(default)]
    pub growth_per_minute: f64,
}

/// A value distribution.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Distribution {
    /// Log-normal with the given median and log-space standard deviation.
    Lognormal {
        /// Median value.
        median: f64,
        /// Standard deviation of ln(value).
        sigma: f64,
    },
    /// Uniform over `[min, max)`.
    Uniform {
        /// Lower bound.
        min: f64,
        /// Upper bound.
        max: f64,
    },
}

/// A change starting `after_seconds` into the run.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Change {
    /// Seconds after start.
    pub after_seconds: u64,
    /// Attribute the change refers to.
    pub attribute: String,
    /// With `value`: only points whose attribute equals this value change.
    #[serde(default)]
    pub equals: Option<String>,
    /// New value distribution for the matching points.
    #[serde(default)]
    pub value: Option<Distribution>,
    /// With `share`: this attribute value replaces the drawn one for that
    /// share of points (a new heavy hitter).
    #[serde(default)]
    pub inject: Option<String>,
    /// Fraction of points that get `inject`.
    #[serde(default)]
    pub share: f64,
}

fn default_seed() -> u64 {
    1
}
fn default_batch() -> usize {
    1024
}
fn default_cardinality() -> u64 {
    1
}

/// SplitMix64: small, seeded and reproducible.
struct Rng(u64);
impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut x = self.0;
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        x ^ (x >> 31)
    }
    /// Uniform in `[0, 1)`.
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1_u64 << 53) as f64
    }
    fn normal(&mut self) -> f64 {
        let u = self.unit().max(f64::MIN_POSITIVE);
        (-2.0 * u.ln()).sqrt() * (std::f64::consts::TAU * self.unit()).cos()
    }
    fn sample(&mut self, d: &Distribution) -> f64 {
        match d {
            Distribution::Lognormal { median, sigma } => median * (sigma * self.normal()).exp(),
            Distribution::Uniform { min, max } => min + (max - min) * self.unit(),
        }
    }
}

/// Draws data points for elapsed scenario time.
pub struct Scenario {
    cfg: ScenarioConfig,
    rng: Rng,
    /// Zipf CDF per attribute index, when `zipf` is set.
    cdfs: HashMap<usize, Vec<f64>>,
    unique_counter: u64,
}

impl Scenario {
    /// Validates the config and precomputes Zipf tables.
    pub fn new(cfg: ScenarioConfig) -> Result<Self, String> {
        if cfg.signals_per_second == 0 || cfg.max_batch_size == 0 {
            return Err("signals_per_second and max_batch_size must be positive".into());
        }
        let mut cdfs = HashMap::new();
        for (i, a) in cfg.attributes.iter().enumerate() {
            if a.cardinality == 0 || (a.zipf.is_some() && a.growth_per_minute != 0.0) {
                return Err(format!(
                    "{}: cardinality must be positive; growth needs uniform values",
                    a.key
                ));
            }
            if let Some(s) = a.zipf {
                let mut total = 0.0;
                let mut cdf: Vec<f64> = (1..=a.cardinality)
                    .map(|rank| {
                        total += (rank as f64).powf(-s);
                        total
                    })
                    .collect();
                cdf.iter_mut().for_each(|c| *c /= total);
                cdfs.insert(i, cdf);
            }
        }
        Ok(Self {
            rng: Rng(cfg.seed),
            cdfs,
            unique_counter: 0,
            cfg,
        })
    }

    fn attribute_value(&mut self, i: usize, minutes: f64) -> String {
        let a = &self.cfg.attributes[i];
        if a.unique {
            // The seed keeps sources with different seeds from reusing values.
            self.unique_counter += 1;
            return format!("{}-{}-{}", a.key, self.cfg.seed, self.unique_counter);
        }
        let n = if let Some(cdf) = self.cdfs.get(&i) {
            let u = self.rng.unit();
            cdf.partition_point(|&c| c < u).min(cdf.len() - 1) as u64
        } else {
            let live = (a.cardinality as f64 * (1.0 + a.growth_per_minute).powf(minutes)).max(1.0);
            (self.rng.unit() * live) as u64
        };
        format!("{}-{n}", a.key)
    }

    /// One batch of `count` points at `elapsed` since start, stamped `now_ns`.
    pub fn batch(&mut self, count: usize, elapsed: Duration, now_ns: u64) -> MetricsData {
        let secs = elapsed.as_secs();
        let minutes = elapsed.as_secs_f64() / 60.0;
        let points = (0..count)
            .map(|_| {
                let mut values: Vec<(String, String)> = (0..self.cfg.attributes.len())
                    .map(|i| {
                        (
                            self.cfg.attributes[i].key.clone(),
                            self.attribute_value(i, minutes),
                        )
                    })
                    .collect();
                let mut dist = self.cfg.value.clone();
                for c in self.cfg.changes.iter().filter(|c| secs >= c.after_seconds) {
                    let Some(slot) = values.iter_mut().find(|(k, _)| *k == c.attribute) else {
                        continue;
                    };
                    if let Some(v) = &c.inject {
                        if self.rng.unit() < c.share {
                            slot.1 = v.clone();
                        }
                    }
                    if let Some(d) = &c.value {
                        if c.equals.as_ref().is_none_or(|e| *e == slot.1) {
                            dist = d.clone();
                        }
                    }
                }
                NumberDataPoint::build()
                    .time_unix_nano(now_ns)
                    .value_double(self.rng.sample(&dist))
                    .attributes(
                        values
                            .into_iter()
                            .map(|(k, v)| KeyValue::new(k, AnyValue::new_string(v)))
                            .collect::<Vec<_>>(),
                    )
                    .finish()
            })
            .collect::<Vec<_>>();
        let metric = Metric::build()
            .name(self.cfg.metric_name.clone())
            .data_gauge(Gauge::new(points))
            .finish();
        let resource = Resource::build()
            .attributes(
                self.cfg
                    .resource_attributes
                    .iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), AnyValue::new_string(v.clone())))
                    .collect::<Vec<_>>(),
            )
            .finish();
        let scope = InstrumentationScope::build()
            .name("asap.scenario_generator")
            .finish();
        MetricsData::new(vec![ResourceMetrics::new(
            resource,
            vec![ScopeMetrics::new(scope, vec![metric])],
        )])
    }
}

struct ScenarioReceiver {
    scenario: Scenario,
}

#[allow(unsafe_code)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
static SCENARIO_GENERATOR: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SCENARIO_GENERATOR_URN,
    create: |_: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig,
             _: &otel_arrow_dfe_engine::capability::registry::Capabilities| {
        let invalid =
            |error: String| otel_arrow_dfe_config::error::Error::InvalidUserConfig { error };
        let cfg: ScenarioConfig = serde_json::from_value(node_config.config.clone())
            .map_err(|e| invalid(e.to_string()))?;
        Ok(ReceiverWrapper::local(
            ScenarioReceiver {
                scenario: Scenario::new(cfg).map_err(invalid)?,
            },
            node,
            node_config,
            receiver_config,
        ))
    },
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
    validate_config: otel_arrow_dfe_config::validation::validate_typed_config::<ScenarioConfig>,
};

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for ScenarioReceiver {
    async fn start(
        mut self: Box<Self>,
        mut ctrl: local::ControlChannel<OtapPdata>,
        effects: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let started = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                msg = ctrl.recv() => match msg.map_err(Error::ChannelRecvError)? {
                    NodeControlMsg::DrainIngress { .. } => {
                        effects.notify_receiver_drained().await?;
                        return Ok(TerminalState::default());
                    }
                    NodeControlMsg::Shutdown { .. } => return Ok(TerminalState::default()),
                    _ => {}
                },
                _ = ticker.tick() => {
                    let mut left = self.scenario.cfg.signals_per_second;
                    while left > 0 {
                        let n = left.min(self.scenario.cfg.max_batch_size);
                        left -= n;
                        let data = self.scenario.batch(n, started.elapsed(), now_ns());
                        let payload: OtapPayload = OtlpProtoMessage::Metrics(data)
                            .try_into()
                            .map_err(|e| Error::ReceiverError {
                                receiver: effects.receiver_id(),
                                kind: ReceiverErrorKind::Other,
                                error: format!("encode scenario batch: {e:?}"),
                                source_detail: String::new(),
                            })?;
                        effects
                            .send_message_with_source_node(OtapPdata::new_todo_context(payload))
                            .await?;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pipelines give the config as YAML; the engine hands it over as JSON.
    fn config(json: &str) -> ScenarioConfig {
        serde_json::from_str(json).unwrap()
    }

    fn points(s: &mut Scenario, n: usize, at: u64) -> Vec<NumberDataPoint> {
        let data = s.batch(n, Duration::from_secs(at), 1);
        match &data.resource_metrics[0].scope_metrics[0].metrics[0].data {
            Some(otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::metric::Data::Gauge(
                g,
            )) => g.data_points.clone(),
            _ => panic!("expected a gauge"),
        }
    }

    fn attr(p: &NumberDataPoint, key: &str) -> String {
        let v = p.attributes.iter().find(|kv| kv.key == key).unwrap();
        format!("{:?}", v.value)
    }

    fn value(p: &NumberDataPoint) -> f64 {
        match p.value {
            Some(otel_arrow_dfe_pdata::proto::opentelemetry::metrics::v1::number_data_point::Value::AsDouble(v)) => v,
            _ => panic!("expected a double"),
        }
    }

    #[test]
    fn cardinality_and_unique_values() {
        let mut s = Scenario::new(config(
            r#"{"signals_per_second":1,"metric_name":"m","value":{"uniform":{"min":0,"max":1}},
                "attributes":[{"key":"route","cardinality":20},{"key":"user","unique":true}]}"#,
        ))
        .unwrap();
        let pts = points(&mut s, 5000, 0);
        let routes: std::collections::HashSet<_> = pts.iter().map(|p| attr(p, "route")).collect();
        let users: std::collections::HashSet<_> = pts.iter().map(|p| attr(p, "user")).collect();
        assert_eq!(routes.len(), 20);
        assert_eq!(users.len(), 5000);
    }

    #[test]
    fn zipf_makes_the_first_value_heaviest() {
        let mut s = Scenario::new(config(
            r#"{"signals_per_second":1,"metric_name":"m","value":{"uniform":{"min":0,"max":1}},
                "attributes":[{"key":"route","cardinality":1000,"zipf":1.1}]}"#,
        ))
        .unwrap();
        let pts = points(&mut s, 20000, 0);
        let top = pts
            .iter()
            .filter(|p| attr(p, "route").contains("route-0\""))
            .count();
        assert!(top > 2000, "{top}");
    }

    #[test]
    fn changes_apply_after_their_time() {
        let cfg = r#"{"signals_per_second":1,"metric_name":"m","value":{"lognormal":{"median":10,"sigma":0.1}},
            "attributes":[{"key":"route","cardinality":2}],
            "changes":[{"after_seconds":60,"attribute":"route","equals":"route-1","value":{"lognormal":{"median":100,"sigma":0.1}}},
                       {"after_seconds":60,"attribute":"route","inject":"hot","share":0.5}]}"#;
        let mut s = Scenario::new(config(cfg)).unwrap();
        let before = points(&mut s, 2000, 30);
        assert!(before.iter().all(|p| value(p) < 30.0));
        let after = points(&mut s, 2000, 90);
        let slow = after
            .iter()
            .filter(|p| attr(p, "route").contains("route-1\""))
            .collect::<Vec<_>>();
        assert!(!slow.is_empty() && slow.iter().all(|p| value(p) > 50.0));
        let hot = after
            .iter()
            .filter(|p| attr(p, "route").contains("hot"))
            .count();
        assert!((800..1200).contains(&hot), "{hot}");
    }

    #[test]
    fn same_seed_same_points() {
        let cfg = r#"{"seed":7,"signals_per_second":1,"metric_name":"m","value":{"lognormal":{"median":10,"sigma":1}},
            "attributes":[{"key":"route","cardinality":50,"zipf":1.2}]}"#;
        let a = points(&mut Scenario::new(config(cfg)).unwrap(), 100, 0);
        let b = points(&mut Scenario::new(config(cfg)).unwrap(), 100, 0);
        assert_eq!(a, b);
    }
}
