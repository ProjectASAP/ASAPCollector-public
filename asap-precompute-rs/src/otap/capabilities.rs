//! What the collector can run, in ASAPPlanner's vocabulary: the JSON
//! `asap-collector --print-capabilities` prints. A control plane reads it to
//! decide which nodes of an installed plan run on the collector.
//!
//! Each capability is stated once:
//!
//! - `executor`: ASAPPlanner's `DeploymentCapabilities` with its own serde,
//!   except that each `supported_summaries` entry is the planner's
//!   `SupportedSummary` wrapped as `{"summary", "input", "parameters"}`. The
//!   sketches, their input kind and their parameter ranges come from the
//!   processor's dispatch table ([`SKETCH_TYPES`]).
//! - `predicate_ops`: the label comparisons the runtime's matchers
//!   ([`MatchOp`], `PrecomputeConfig::matchers`) evaluate, for every
//!   operator that takes a predicate. The processor's own config sets no
//!   matchers; the views a control plane assigns to the collector do.
//! - `operators`: the planner `OperatorKind`s the collector runs, with only
//!   their own limits.
//! - `placement`: every sample of a series reaches one collector instance.
//! - `window_close_delay_ms`: the longest time after a tumbling window's
//!   end before the processor emits it.

use std::time::Duration;

use asap_types::deployment_capabilities::{
    CounterSemanticsSupport, DeploymentCapabilities, SupportedSummary,
};
use asap_types::ir::operator::{OperatorKind, Reduction, Source};
use asap_types::ir::scalar::CompareOpKind;
use serde::ser::{Error as _, SerializeMap};
use serde::{Serialize, Serializer};

use super::config::{PluginConfig, SketchInput, SketchParameter, SKETCH_TYPES};
use crate::matchers::MatchOp;

/// The version of this format; a reader refuses one it does not know.
pub const CAPABILITIES_VERSION: u32 = 1;

/// What `asap-collector --print-capabilities` prints.
#[derive(Debug, Serialize)]
pub struct CollectorCapabilities {
    /// [`CAPABILITIES_VERSION`].
    pub capabilities_version: u32,
    /// The planner's executor capabilities, with the collector's sketch
    /// entries.
    pub executor: ExecutorCapabilities,
    /// Label comparisons with a string literal, for every operator that
    /// takes a predicate.
    pub predicate_ops: Vec<CompareOpKind>,
    /// The operators the collector runs.
    pub operators: Vec<OperatorCapability>,
    /// How series are partitioned across collector instances.
    pub placement: Placement,
    /// The longest time after a window's end before it is emitted.
    pub window_close_delay_ms: u64,
}

/// `executor`: `DeploymentCapabilities` with its planner serde, except that
/// `supported_summaries` holds [`CollectorSummary`] entries.
#[derive(Debug)]
pub struct ExecutorCapabilities {
    /// One entry per sketch the collector builds.
    pub supported_summaries: Vec<CollectorSummary>,
    /// Every other field; its own `supported_summaries` is `None`.
    pub rest: DeploymentCapabilities,
}

impl ExecutorCapabilities {
    /// The planner's `DeploymentCapabilities` these state: `rest` with the
    /// entries' `summary` fields as its `supported_summaries`.
    pub fn deployment_capabilities(&self) -> DeploymentCapabilities {
        DeploymentCapabilities {
            supported_summaries: Some(
                self.supported_summaries
                    .iter()
                    .map(|entry| entry.summary.clone())
                    .collect(),
            ),
            ..self.rest.clone()
        }
    }
}

impl Serialize for ExecutorCapabilities {
    /// `supported_summaries` first, then `rest`'s other fields as the
    /// planner serializes them.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let serde_json::Value::Object(rest) =
            serde_json::to_value(&self.rest).map_err(S::Error::custom)?
        else {
            return Err(S::Error::custom("DeploymentCapabilities is not an object"));
        };
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("supported_summaries", &self.supported_summaries)?;
        for (key, value) in rest.iter().filter(|(k, _)| *k != "supported_summaries") {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }
}

/// An entry of `executor.supported_summaries`: the planner's
/// `SupportedSummary` plus what only the collector states.
#[derive(Debug, Serialize)]
pub struct CollectorSummary {
    /// The sketch and the readouts the collector answers from it.
    pub summary: SupportedSummary,
    /// What the sketch reads from a sample.
    pub input: SketchInput,
    /// Each size parameter, by its `SketchParams` field name, with the
    /// values the collector accepts.
    #[serde(serialize_with = "serialize_parameters")]
    pub parameters: &'static [SketchParameter],
}

fn serialize_parameters<S: Serializer>(
    parameters: &&'static [SketchParameter],
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(parameters.len()))?;
    for parameter in parameters.iter() {
        map.serialize_entry(parameter.name, &parameter.range)?;
    }
    map.end()
}

/// One operator the collector runs, with the limits of that operator only.
#[derive(Debug, Serialize)]
pub struct OperatorCapability {
    /// The planner's name of the operator.
    pub kind: OperatorKind,
    /// `Scan`: the `Source` variants it reads.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
    /// Chain: the `Reduction` variants it runs.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reductions: Vec<String>,
}

/// How series are partitioned across collector instances.
#[derive(Debug, Serialize)]
pub struct Placement {
    /// What all samples reaching one instance share.
    pub partitioned_by: PartitionedBy,
}

/// What all samples reaching one collector instance share.
#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionedBy {
    /// All samples of one series reach one instance.
    Series,
}

/// The planner's comparison for one of the runtime's matchers.
fn compare_op(op: MatchOp) -> CompareOpKind {
    match op {
        MatchOp::Equal => CompareOpKind::Eq,
        MatchOp::NotEqual => CompareOpKind::Ne,
    }
}

/// The serde name of `value`'s enum variant, e.g. `TimeSeries` for
/// `Source::TimeSeries { .. }`, so the printed names are the planner's.
pub(crate) fn variant_name<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value).expect("a planner enum serializes") {
        serde_json::Value::String(name) => name,
        serde_json::Value::Object(map) if map.len() == 1 => map.keys().next().unwrap().clone(),
        other => panic!("not an enum variant: {other}"),
    }
}

/// The longest time after a tumbling window's end before the processor
/// emits it: the idle-close grace plus the longest tumbling-window timer
/// period (a timer tick closes the windows of idle series).
fn window_close_delay() -> Duration {
    let longest_window = PluginConfig {
        window_size: Duration::MAX,
        ..Default::default()
    };
    super::processor::IDLE_CLOSE_GRACE + super::processor::timer_period(&longest_window)
}

/// The collector's capabilities.
pub fn collector_capabilities() -> CollectorCapabilities {
    let supported_summaries = SKETCH_TYPES
        .iter()
        .map(|spec| CollectorSummary {
            summary: SupportedSummary {
                summary_type: spec.algorithm,
                // The collector reads no value from a sketch yet.
                readouts: Vec::new(),
            },
            input: spec.input,
            parameters: spec.parameters,
        })
        .collect();
    let rest = DeploymentCapabilities {
        supported_summaries: None,
        // Raw samples pass through; none is kept.
        raw_data_retained: false,
        query_time_results_retained: false,
        // No counter functions.
        counter_semantics: CounterSemanticsSupport {
            reset_handling: false,
            extrapolation: false,
        },
        ..Default::default()
    };
    let per_entity: Reduction = Reduction::PerEntity;
    CollectorCapabilities {
        capabilities_version: CAPABILITIES_VERSION,
        executor: ExecutorCapabilities {
            supported_summaries,
            rest,
        },
        // Every `MatchOp`; a new variant fails to compile in `compare_op`.
        predicate_ops: [MatchOp::Equal, MatchOp::NotEqual]
            .into_iter()
            .map(compare_op)
            .collect(),
        operators: vec![
            OperatorCapability {
                kind: OperatorKind::Scan,
                sources: vec![variant_name(&Source::TimeSeries {
                    metric: String::new(),
                })],
                reductions: Vec::new(),
            },
            OperatorCapability {
                kind: OperatorKind::Filter,
                sources: Vec::new(),
                reductions: Vec::new(),
            },
            // The processor's tumbling windows, one state per series.
            OperatorCapability {
                kind: OperatorKind::SummaryTumblingWindowChain,
                sources: Vec::new(),
                reductions: vec![variant_name(&per_entity)],
            },
        ],
        placement: Placement {
            partitioned_by: PartitionedBy::Series,
        },
        window_close_delay_ms: window_close_delay().as_millis() as u64,
    }
}

/// The JSON `--print-capabilities` prints.
pub fn capabilities_json() -> String {
    serde_json::to_string_pretty(&collector_capabilities()).expect("capabilities serialize")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The printed JSON, byte for byte.
    #[test]
    fn printed_json_matches_golden() {
        let golden = include_str!("../../tests/golden/capabilities.json");
        assert_eq!(capabilities_json(), golden.trim_end());
    }

    /// Every sketch the processor config accepts is in
    /// `executor.supported_summaries` exactly once, under its planner name.
    #[test]
    fn every_accepted_sketch_appears_once() {
        use super::super::config::*;
        let accepted = [
            (SKETCH_TYPE_DDSKETCH, "DDSketch"),
            (SKETCH_TYPE_KLL, "KLL"),
            (SKETCH_TYPE_HLL, "HLL"),
            (SKETCH_TYPE_COUNTSKETCH, "CountSketch"),
            (SKETCH_TYPE_COUNTMINSKETCH, "CountMinSketch"),
            (SKETCH_TYPE_COUNTMINSKETCH_HEAP, "CountMinSketchWithHeap"),
        ];
        let json: serde_json::Value = serde_json::from_str(&capabilities_json()).unwrap();
        let entries = json["executor"]["supported_summaries"].as_array().unwrap();
        assert_eq!(entries.len(), accepted.len());
        for (sketch_type, planner_name) in accepted {
            resolve(&PluginConfig {
                sketch_type: sketch_type.to_string(),
                ..Default::default()
            })
            .expect("the processor accepts it");
            let n = entries
                .iter()
                .filter(|e| e["summary"]["summary_type"] == planner_name)
                .count();
            assert_eq!(n, 1, "{planner_name}");
        }
    }

    /// The KLL `k` bound is the planner's (both are `asap_sketchlib`'s cap).
    #[test]
    fn kll_k_bound_is_the_planners() {
        let json: serde_json::Value = serde_json::from_str(&capabilities_json()).unwrap();
        let kll = &json["executor"]["supported_summaries"][1];
        assert_eq!(kll["summary"]["summary_type"], "KLL");
        assert_eq!(
            kll["parameters"]["k"]["le"],
            asap_types::summary_type::KLL_MAX_K
        );
    }

    /// The whole output loads into the design doc's control-plane types,
    /// which deny unknown fields.
    #[test]
    fn output_loads_into_the_control_plane_types() {
        use asap_types::deployment_capabilities::CandidateExclusion;
        use asap_types::summary_type::SummaryFamily;
        use serde::Deserialize;
        use std::collections::BTreeMap;

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Capabilities {
            capabilities_version: u32,
            executor: Executor,
            predicate_ops: Vec<CompareOpKind>,
            operators: Vec<Operator>,
            placement: PlacementIn,
            window_close_delay_ms: u64,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Executor {
            supported_summaries: Vec<Summary>,
            raw_data_retained: bool,
            query_time_results_retained: bool,
            counter_semantics: CounterSemanticsSupport,
            state_not_mergeable_across_windows: Vec<SummaryFamily>,
            excluded_candidates: Vec<CandidateExclusion>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Summary {
            summary: SupportedSummary,
            input: Input,
            parameters: BTreeMap<String, Range>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "lowercase")]
        enum Input {
            Value,
            Label,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Range {
            gt: Option<f64>,
            ge: Option<f64>,
            lt: Option<f64>,
            le: Option<f64>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Operator {
            kind: OperatorKind,
            #[serde(default)]
            sources: Vec<String>,
            #[serde(default)]
            reductions: Vec<String>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct PlacementIn {
            partitioned_by: Partition,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Partition {
            Series,
            #[allow(dead_code)]
            Labels(Vec<String>),
        }

        let loaded: Capabilities = serde_json::from_str(&capabilities_json()).unwrap();
        assert_eq!(loaded.capabilities_version, CAPABILITIES_VERSION);
        assert_eq!(
            loaded.executor.supported_summaries.len(),
            SKETCH_TYPES.len()
        );
        assert!(matches!(loaded.placement.partitioned_by, Partition::Series));
        assert_eq!(loaded.window_close_delay_ms, 6000);
    }

    /// `executor`, with each entry replaced by its `summary`, is the
    /// planner's `DeploymentCapabilities` (its serde denies unknown fields).
    #[test]
    fn executor_deserializes_into_deployment_capabilities() {
        let json: serde_json::Value = serde_json::from_str(&capabilities_json()).unwrap();
        let mut executor = json["executor"].clone();
        let summaries: Vec<serde_json::Value> = executor["supported_summaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["summary"].clone())
            .collect();
        executor["supported_summaries"] = summaries.into();
        let parsed: DeploymentCapabilities = serde_json::from_value(executor).unwrap();
        assert_eq!(
            parsed,
            collector_capabilities().executor.deployment_capabilities()
        );
        assert_eq!(
            parsed.supported_summaries.unwrap().len(),
            SKETCH_TYPES.len()
        );
        assert!(!parsed.raw_data_retained);
    }
}
