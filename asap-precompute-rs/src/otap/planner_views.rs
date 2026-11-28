//! The collector views of an installed ASAPPlanner plan, loaded from the
//! control plane's `collector-views.json` (`planner_views: <path>`;
//! `asap_sketches` takes it once [`window_state_config`] is built).
//!
//! The file holds the plan's collector sub-DAG in the planner's own serde:
//!
//! ```json
//! {"planner_wire_version": 16, "plan_id": "<PhysicalASAPDAG::plan_id() of the installed plan>",
//!  "commit_grace_ms": 8000, "backend_reads_raw": {"data": false},
//!  "outputs": [1], "dag": <PhysicalASAPDAG>}
//! ```
//!
//! [`load`] refuses a file of another `planner_wire_version`, validates the
//! sub-DAG with `PhysicalASAPDAG::materialized_views`, and checks every
//! output against the collector's own capabilities
//! ([`super::capabilities::collector_capabilities`], the table
//! `--print-capabilities` prints), naming the node and the field that fails.
//! v1 accepts each output as a leaf view: a `SummaryTumblingWindowChain`
//! over a `Scan` of `TimeSeries { metric }`, with only label `Filter`s
//! between them, and reads it into a [`CollectorView`] in the planner's
//! terms: the sketch and its θ (an out-of-range size is an error, never a
//! default), the tumbling width, and the label predicates on the way.
//!
//! Which engine builds a view's states is not decided here:
//! [`window_state_config`] is the seam where a view becomes the
//! `asap_sketches` processor's window state.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use asap_sketchlib::SketchAlgorithm;
use asap_types::ir::operator::arguments::Source;
use asap_types::ir::operator::{OperatorKind, Reduction};
use asap_types::ir::scalar::{ColumnRef, CompareOpKind, ScalarValue};
use asap_types::ir::schema::{DataType, FieldDataType, Schema};
use asap_types::ir::{ASAPOp, ExprSemantics, NonASAPOp, Operator, Predicate, ScalarExpr};
use asap_types::plan::materialized_view::InstallableDagError;
use asap_types::plan::physical_asap_dag::{
    PhysicalASAPDAG, PhysicalASAPDAGNode, PhysicalASAPNodeId,
};
use asap_types::summary_type::{parameter_list, SketchParams, SummaryInputExpr, SummaryUpdate};
use serde::Deserialize;

use super::capabilities::{
    collector_capabilities, variant_name, CollectorCapabilities, OperatorCapability,
};
use super::config::{PluginConfig, SketchInput, SketchTypeSpec, SKETCH_TYPES};
use super::schema::VIEW_NAME_INFIX;
use crate::envelope::ViewIdentity;

/// The `INSTALLABLE_PLAN_WIRE_VERSION` of the ASAPPlanner revision whose
/// `asap-types` this crate pins (`Cargo.toml`); bump the two together. The
/// constant lives in the `asap-planner` crate, which the collector does not
/// build.
pub const PLANNER_WIRE_VERSION: u32 = 16;

/// `collector-views.json`: the collector sub-DAG of an installed plan, as the
/// control plane writes it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectorViewsFile {
    /// The planner wire version the file was written with; must be
    /// [`PLANNER_WIRE_VERSION`].
    pub planner_wire_version: u32,
    /// `PhysicalASAPDAG::plan_id()` of the installed plan (not of `dag`).
    pub plan_id: String,
    /// How long after a window's end the backend commits it: read by the
    /// data plane, accepted and not kept here.
    #[allow(dead_code)]
    commit_grace_ms: serde::de::IgnoredAny,
    /// Per metric a `Scan` of the plan reads: whether the backend still
    /// reads its raw samples; the raw forward drops the metrics marked
    /// `false`.
    pub backend_reads_raw: BTreeMap<String, bool>,
    /// The collector views: the nodes whose states leave the collector.
    pub outputs: Vec<PhysicalASAPNodeId>,
    /// The collector sub-DAG; `roots = outputs`.
    pub dag: PhysicalASAPDAG,
}

/// One checked collector view, in the planner's terms: what a window state
/// of the collector must build, one state per series.
///
/// Example (q01): `source_metric: "data"`, `sketch: DDSketch`, `parameters:
/// DDSketch { alpha: 0.05 }`, `tumbling_window_width_ms: 5000`,
/// `label_predicates: []`, `output_metric_name: "data.asap_view.1"`.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectorView {
    /// The identity stamped on each of its states.
    pub identity: Arc<ViewIdentity>,
    /// The `Scan`'s `TimeSeries` metric.
    pub source_metric: String,
    /// The name its states are emitted under: `<metric>.asap_view.<node_id>`.
    pub output_metric_name: String,
    /// The chain's sketch.
    pub sketch: SketchAlgorithm,
    /// θ, within the collector's ranges for `sketch`.
    pub parameters: SketchParams,
    /// The chain's tumbling window width, positive.
    pub tumbling_window_width_ms: u64,
    /// Every `Scan`, `Filter` and chain-filter predicate on the way, each a
    /// label compared with a non-empty string by an op in `predicate_ops`.
    pub label_predicates: Vec<LabelPredicate>,
}

/// `label <op> "value"`, one conjunct of a view's selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelPredicate {
    /// The label (a point attribute).
    pub label: String,
    /// `Eq` or `Ne`.
    pub op: CompareOpKind,
    /// The non-empty literal.
    pub value: String,
}

/// The seam where a checked view becomes the `asap_sketches` processor's
/// window state: its `PluginConfig` (sketch type and sizes from θ, window =
/// the tumbling width, matchers `__name__ = metric` plus the label
/// predicates, output name [`CollectorView::output_metric_name`]). Not built until
/// the engine that runs collector views is chosen (the processor's window
/// state, or ASAPPlanner's executor).
pub fn window_state_config(view: &CollectorView) -> PluginConfig {
    todo!(
        "map collector view {} onto the asap_sketches window state",
        view.output_metric_name
    )
}

/// A loaded and checked `collector-views.json`.
#[derive(Debug, Clone)]
pub struct PlannerViews {
    /// One per output, in `outputs` order.
    pub views: Vec<CollectorView>,
    /// The file's `backend_reads_raw`.
    pub backend_reads_raw: BTreeMap<String, bool>,
}

impl PlannerViews {
    /// Whether the raw forward drops `metric`'s samples: the file marks it
    /// `false`. A metric the plan does not read is forwarded.
    pub fn drops_raw(&self, metric: &str) -> bool {
        self.backend_reads_raw.get(metric) == Some(&false)
    }
}

/// Why `collector-views.json` cannot be loaded. Every variant is bad input:
/// the file, or a plan the collector cannot execute.
#[derive(Debug, thiserror::Error)]
pub enum PlannerViewsError {
    /// The file cannot be read.
    #[error("planner_views {path}: {source}")]
    Read {
        /// The configured path.
        path: String,
        /// The I/O error.
        source: std::io::Error,
    },
    /// The file is not a collector-views file of this planner's serde.
    #[error("planner_views: {0}")]
    Parse(#[from] serde_json::Error),
    /// The file was written by a planner of another wire version.
    #[error("planner_views: planner_wire_version {found}, but this collector reads {expected}")]
    WireVersion {
        /// The file's version.
        found: u32,
        /// [`PLANNER_WIRE_VERSION`].
        expected: u32,
    },
    /// The sub-DAG is not a valid installable DAG.
    #[error("planner_views: {0}")]
    InvalidDag(#[from] InstallableDagError),
    /// An output is not a materialized view of the sub-DAG.
    #[error("planner_views: output {node} is not a materialized view of the sub-DAG")]
    OutputNotAView {
        /// The output's node id.
        node: PhysicalASAPNodeId,
    },
    /// A node, or one of its arguments, is outside the collector's
    /// capabilities.
    #[error("planner_views: node {node}: {field}: {reason}")]
    NotExecutable {
        /// The node's id.
        node: PhysicalASAPNodeId,
        /// The failing field of the node, e.g. `parameters.alpha`.
        field: String,
        /// Why it fails.
        reason: String,
    },
}

fn not_executable(
    node: PhysicalASAPNodeId,
    field: impl Into<String>,
    reason: impl Into<String>,
) -> PlannerViewsError {
    PlannerViewsError::NotExecutable {
        node,
        field: field.into(),
        reason: reason.into(),
    }
}

/// Reads and checks the file at `path` ([`parse`]).
pub fn load(path: &Path) -> Result<PlannerViews, PlannerViewsError> {
    let text = std::fs::read_to_string(path).map_err(|source| PlannerViewsError::Read {
        path: path.display().to_string(),
        source,
    })?;
    parse(&text)
}

/// Parses `collector-views.json` and reads each output into a
/// [`CollectorView`], refusing what the collector cannot execute.
pub fn parse(text: &str) -> Result<PlannerViews, PlannerViewsError> {
    // The version first: a file of another version may not parse as this one.
    #[derive(Deserialize)]
    struct Version {
        planner_wire_version: u32,
    }
    let Version {
        planner_wire_version,
    } = serde_json::from_str(text)?;
    if planner_wire_version != PLANNER_WIRE_VERSION {
        return Err(PlannerViewsError::WireVersion {
            found: planner_wire_version,
            expected: PLANNER_WIRE_VERSION,
        });
    }
    let file: CollectorViewsFile = serde_json::from_str(text)?;
    // Validates the DAG; each view's key depends only on its node.
    let materialized = file.dag.materialized_views()?;
    let nodes: HashMap<PhysicalASAPNodeId, &PhysicalASAPDAGNode> =
        file.dag.nodes.iter().map(|node| (node.id, node)).collect();
    let capabilities = collector_capabilities();
    let views = file
        .outputs
        .iter()
        .map(|&output| {
            let view = materialized
                .iter()
                .find(|view| view.node_id == output)
                .ok_or(PlannerViewsError::OutputNotAView { node: output })?;
            let identity = ViewIdentity {
                plan_id: file.plan_id.clone(),
                node_id: output as u64,
                view_state_key: serde_json::to_string(view.view_state_key())?,
            };
            leaf_view(&nodes, nodes[&output], &capabilities, identity)
        })
        .collect::<Result<_, _>>()?;
    Ok(PlannerViews {
        views,
        backend_reads_raw: file.backend_reads_raw,
    })
}

/// Reads output `chain` into a [`CollectorView`]: the chain, then down its
/// input through label `Filter`s to the `Scan`.
fn leaf_view(
    nodes: &HashMap<PhysicalASAPNodeId, &PhysicalASAPDAGNode>,
    chain: &PhysicalASAPDAGNode,
    capabilities: &CollectorCapabilities,
    identity: ViewIdentity,
) -> Result<CollectorView, PlannerViewsError> {
    let id = chain.id;
    let operator = operator_capability(chain, capabilities)?;
    let Operator::ASAP(ASAPOp::SummaryTumblingWindowChain {
        child,
        summary_type,
        input,
        reduction,
        filter,
        tumbling_window_width_ms,
    }) = &chain.payload
    else {
        return Err(not_executable(
            id,
            "payload",
            format!(
                "a collector view is a SummaryTumblingWindowChain, not {:?}",
                OperatorKind::of(&chain.payload)
            ),
        ));
    };
    if !operator
        .reductions
        .contains(&variant_name::<Reduction>(reduction))
    {
        return Err(not_executable(
            id,
            "reduction",
            format!("{reduction:?} is not in the collector's capabilities"),
        ));
    }
    if *tumbling_window_width_ms == 0 {
        return Err(not_executable(
            id,
            "tumbling_window_width_ms",
            "must be positive",
        ));
    }
    let (sketch, parameters) = sketch(chain, summary_type, input, capabilities)?;

    let mut label_predicates = Vec::new();
    if let Some(filter) = filter {
        label_predicates_of(
            id,
            "filter",
            filter,
            &nodes[child].output_schema,
            capabilities,
            &mut label_predicates,
        )?;
    }
    let mut node = nodes[child];
    let metric = loop {
        let operator = operator_capability(node, capabilities)?;
        match &node.payload {
            Operator::NonASAP(NonASAPOp::Filter { pred, child }) => {
                let input_schema = &nodes[child].output_schema;
                label_predicates_of(
                    node.id,
                    "pred",
                    pred,
                    input_schema,
                    capabilities,
                    &mut label_predicates,
                )?;
                node = nodes[child];
            }
            Operator::NonASAP(NonASAPOp::Scan {
                source,
                predicates,
                schema,
            }) => {
                if !operator.sources.contains(&variant_name(source)) {
                    return Err(not_executable(
                        node.id,
                        "source",
                        format!(
                            "{} is not in the collector's capabilities",
                            variant_name(source)
                        ),
                    ));
                }
                let Source::TimeSeries { metric } = source else {
                    return Err(not_executable(
                        node.id,
                        "source",
                        "a collector view reads a TimeSeries source",
                    ));
                };
                for pred in predicates {
                    label_predicates_of(
                        node.id,
                        "predicates",
                        pred,
                        schema,
                        capabilities,
                        &mut label_predicates,
                    )?;
                }
                break metric.clone();
            }
            other => {
                return Err(not_executable(
                    node.id,
                    "payload",
                    format!(
                        "a collector view reads a Scan through label Filters only, not {:?}",
                        OperatorKind::of(other)
                    ),
                ))
            }
        }
    };
    Ok(CollectorView {
        identity: Arc::new(identity),
        output_metric_name: format!("{metric}{VIEW_NAME_INFIX}{id}"),
        source_metric: metric,
        sketch,
        parameters,
        tumbling_window_width_ms: *tumbling_window_width_ms,
        label_predicates,
    })
}

/// `node`'s operator entry in the capabilities.
fn operator_capability<'c>(
    node: &PhysicalASAPDAGNode,
    capabilities: &'c CollectorCapabilities,
) -> Result<&'c OperatorCapability, PlannerViewsError> {
    let kind = OperatorKind::of(&node.payload);
    capabilities
        .operators
        .iter()
        .find(|operator| operator.kind == kind)
        .ok_or_else(|| {
            not_executable(
                node.id,
                "payload",
                format!("{kind:?} is not in the collector's capabilities"),
            )
        })
}

/// The chain's sketch and θ, from its summary type, input and parameters,
/// each checked against the capabilities' `supported_summaries` entry.
fn sketch(
    chain: &PhysicalASAPDAGNode,
    summary_type: &FieldDataType,
    input: &SummaryUpdate,
    capabilities: &CollectorCapabilities,
) -> Result<(SketchAlgorithm, SketchParams), PlannerViewsError> {
    let id = chain.id;
    let FieldDataType::Sketch(algorithm) = summary_type else {
        return Err(not_executable(
            id,
            "summary_type",
            format!("{summary_type:?} is not a sketch the collector builds"),
        ));
    };
    let entry = capabilities
        .executor
        .supported_summaries
        .iter()
        .find(|entry| entry.summary.summary_type == *algorithm)
        .ok_or_else(|| {
            not_executable(
                id,
                "summary_type",
                format!("{algorithm:?} is not in the collector's capabilities"),
            )
        })?;
    match entry.input {
        SketchInput::Value => {
            let sample_value = SummaryUpdate {
                item: None,
                weight: SummaryInputExpr::Column(ColumnRef::SampleValue),
            };
            if *input != sample_value {
                return Err(not_executable(
                    id,
                    "input",
                    format!("{algorithm:?} reads the sample value, not {input:?}"),
                ));
            }
        }
        // The processor builds a label-keyed sketch over every point of a
        // series; a per-series chain keyed by one of the series' own labels
        // is not built.
        SketchInput::Label => {
            return Err(not_executable(
                id,
                "input",
                format!("a per-series {algorithm:?} chain over a label item is not built yet"),
            ))
        }
    }

    let Some(parameters) = &chain.parameters else {
        return Err(not_executable(id, "parameters", "a sketch view needs θ"));
    };
    if sketch_type_spec_of(parameters).map(|spec| spec.algorithm) != Some(*algorithm) {
        return Err(not_executable(
            id,
            "parameters",
            format!("{parameters:?} does not size {algorithm:?}"),
        ));
    }
    // θ's fields by name, e.g. {"alpha": 0.05}.
    let serde_json::Value::Object(variant) = serde_json::to_value(parameters)? else {
        unreachable!("θ serializes as an object");
    };
    let fields = variant
        .into_values()
        .next()
        .and_then(|fields| fields.as_object().cloned())
        .unwrap_or_default();
    for (name, value) in &fields {
        let field = format!("parameters.{name}");
        let parameter = entry
            .parameters
            .iter()
            .find(|p| p.name == name)
            .ok_or_else(|| not_executable(id, &field, "not a size the collector sets"))?;
        let value = value
            .as_f64()
            .ok_or_else(|| not_executable(id, &field, format!("{value} is not a number")))?;
        if !parameter.range.contains(value) {
            return Err(not_executable(
                id,
                &field,
                format!(
                    "{value} is outside the collector's range {}",
                    serde_json::to_string(&parameter.range)?
                ),
            ));
        }
    }
    if let Some(missing) = entry
        .parameters
        .iter()
        .find(|p| !fields.contains_key(p.name))
    {
        return Err(not_executable(
            id,
            format!("parameters.{}", missing.name),
            "missing",
        ));
    }
    Ok((*algorithm, parameters.clone()))
}

/// The `SKETCH_TYPES` row θ `parameters` size: the one whose algorithm's
/// planner parameter list (`parameter_list`) holds θ's variant, e.g.
/// `DDSketch { alpha }` for DDSketch, `Cms { width, depth }` for Count-Min.
pub(crate) fn sketch_type_spec_of(parameters: &SketchParams) -> Option<&'static SketchTypeSpec> {
    let variant = variant_name(parameters);
    SKETCH_TYPES.iter().find(|spec| {
        parameter_list(spec.algorithm)
            .first()
            .is_some_and(|listed| variant_name(listed) == variant)
    })
}

/// Appends the label predicates `pred` (field `field` of node `node`, over
/// `schema`) is: a conjunction of `label <op> "literal"` with `op` in the
/// capabilities' `predicate_ops`.
fn label_predicates_of(
    node: PhysicalASAPNodeId,
    field: &str,
    pred: &Predicate<PhysicalASAPNodeId>,
    schema: &Schema,
    capabilities: &CollectorCapabilities,
    out: &mut Vec<LabelPredicate>,
) -> Result<(), PlannerViewsError> {
    let unsupported = |what: String| not_executable(node, field, what);
    let mut pending = vec![&pred.0];
    while let Some(expr) = pending.pop() {
        match expr {
            ScalarExpr::BoolAnd(terms) => pending.extend(terms.iter().rev()),
            ScalarExpr::Compare {
                left,
                op,
                right,
                semantics,
            } => {
                // SQL's `l != 'v'` excludes rows without `l` (NULL); PromQL's
                // `!=`, like the collector's, keeps them.
                if *op == CompareOpKind::Ne && *semantics != ExprSemantics::Promql {
                    return Err(unsupported(format!(
                        "{op:?} with {semantics:?} semantics is not supported"
                    )));
                }
                let (ScalarExpr::Column(column), ScalarExpr::Literal(ScalarValue::Utf8(value))) =
                    (left.as_ref(), right.as_ref())
                else {
                    return Err(unsupported(format!(
                        "only a label compared with a string literal is supported, not {expr:?}"
                    )));
                };
                if !capabilities.predicate_ops.contains(op) {
                    return Err(unsupported(format!(
                        "{op:?} is not in the collector's predicate_ops"
                    )));
                }
                let label = schema
                    .fields
                    .get(*column)
                    .filter(|f| f.dtype == FieldDataType::Plain(DataType::Utf8))
                    .ok_or_else(|| unsupported(format!("column {column} is not a label")))?;
                // PromQL's `l=""` also matches series without `l`, which the
                // processor's matchers do not.
                if value.is_empty() {
                    return Err(unsupported(format!(
                        "a matcher on {:?} with an empty value is not supported",
                        label.name
                    )));
                }
                out.push(LabelPredicate {
                    label: label.name.clone(),
                    op: op.clone(),
                    value: value.clone(),
                });
            }
            other => {
                return Err(unsupported(format!(
                    "only conjunctions of label matchers are supported, not {other:?}"
                )))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q01: &str = include_str!("../../tests/fixtures/collector-views/q01/collector-views.json");
    const Q01_LABELS: &str = include_str!(
        "../../tests/fixtures/collector-views/q01-label-matchers/collector-views.json"
    );

    /// The installed plan the fixture's sub-DAG was cut from (the planner's
    /// `InstallablePlan` file).
    #[derive(Deserialize)]
    struct InstalledPlan {
        dag: PhysicalASAPDAG,
    }

    /// `edit` applied to the q01 file's JSON.
    fn q01_with(edit: impl FnOnce(&mut serde_json::Value)) -> String {
        let mut file: serde_json::Value = serde_json::from_str(Q01).unwrap();
        edit(&mut file);
        file.to_string()
    }

    fn chain(file: &mut serde_json::Value) -> &mut serde_json::Value {
        &mut file["dag"]["nodes"][1]
    }

    fn refused_field(text: &str) -> (PhysicalASAPNodeId, String) {
        match parse(text) {
            Err(PlannerViewsError::NotExecutable { node, field, .. }) => (node, field),
            other => panic!("expected NotExecutable, got {other:?}"),
        }
    }

    /// The q01 fixture's view: DDSketch α 0.05 per series in 5 s windows over
    /// `data`, named and keyed as the installed plan's node 1.
    #[test]
    fn q01_loads_one_ddsketch_view_with_the_installed_plans_identity() {
        let installed: InstalledPlan = serde_json::from_str(include_str!(
            "../../tests/fixtures/collector-views/q01/installed-plan.json"
        ))
        .unwrap();
        let loaded = parse(Q01).expect("q01 loads");
        let [view] = loaded.views.as_slice() else {
            panic!("one view");
        };
        assert_eq!(view.identity.plan_id, installed.dag.plan_id());
        assert_eq!(view.identity.node_id, 1);
        let installed_view = installed
            .dag
            .materialized_views()
            .unwrap()
            .into_iter()
            .find(|v| v.node_id == 1)
            .unwrap();
        assert_eq!(
            view.identity.view_state_key,
            serde_json::to_string(installed_view.view_state_key()).unwrap()
        );
        assert!(view
            .identity
            .view_state_key
            .ends_with(r#""window":{"tumbling":{"width_ms":5000}},"logical_properties":{"parameters":{"DDSketch":{"alpha":0.05}}}}"#));
        assert_eq!(view.source_metric, "data");
        assert_eq!(view.output_metric_name, "data.asap_view.1");
        assert_eq!(view.sketch, SketchAlgorithm::DDSketch);
        assert_eq!(view.parameters, SketchParams::DDSketch { alpha: 0.05 });
        assert_eq!(view.tumbling_window_width_ms, 5000);
        assert!(view.label_predicates.is_empty());
        assert!(loaded.drops_raw("data"));
        assert!(!loaded.drops_raw("not_in_the_plan"));
    }

    /// `data{region="r0", service!="s1"}`: the `Scan`'s predicates, by label.
    #[test]
    fn scan_predicates_become_label_predicates() {
        let loaded = parse(Q01_LABELS).expect("loads");
        let predicate = |label: &str, op, value: &str| LabelPredicate {
            label: label.into(),
            op,
            value: value.into(),
        };
        assert_eq!(
            loaded.views[0].label_predicates,
            [
                predicate("region", CompareOpKind::Eq, "r0"),
                predicate("service", CompareOpKind::Ne, "s1"),
            ]
        );
    }

    #[test]
    fn another_wire_version_is_refused() {
        let text = q01_with(|f| f["planner_wire_version"] = 15.into());
        assert!(matches!(
            parse(&text),
            Err(PlannerViewsError::WireVersion {
                found: 15,
                expected: PLANNER_WIRE_VERSION
            })
        ));
    }

    /// A sketch outside the capabilities' `supported_summaries` (KMV).
    #[test]
    fn an_unsupported_sketch_is_refused_naming_node_and_field() {
        let text = q01_with(|f| {
            let chain = chain(f);
            chain["payload"]["ASAP"]["SummaryTumblingWindowChain"]["summary_type"] =
                serde_json::json!({"Sketch": "KMV"});
            chain["parameters"] = serde_json::json!({"Kmv": {"k": 1024}});
        });
        assert_eq!(refused_field(&text), (1, "summary_type".into()));
    }

    /// α = 1.5 is outside DDSketch's `(0, 1)`: refused, not defaulted.
    #[test]
    fn an_out_of_range_parameter_is_refused() {
        let text = q01_with(|f| {
            chain(f)["parameters"] = serde_json::json!({"DDSketch": {"alpha": 1.5}});
        });
        let Err(PlannerViewsError::NotExecutable {
            node,
            field,
            reason,
        }) = parse(&text)
        else {
            panic!("refused");
        };
        assert_eq!((node, field.as_str()), (1, "parameters.alpha"));
        assert!(reason.contains("1.5"), "{reason}");
    }

    /// θ of another sketch than the chain's.
    #[test]
    fn parameters_of_another_sketch_are_refused() {
        let text = q01_with(|f| chain(f)["parameters"] = serde_json::json!({"Kll": {"k": 200}}));
        assert_eq!(refused_field(&text), (1, "parameters".into()));
    }

    /// A regular-expression-like or non-label predicate: only `=` / `!=` on a
    /// label with a non-empty string.
    #[test]
    fn an_empty_matcher_value_is_refused() {
        let mut file: serde_json::Value = serde_json::from_str(Q01_LABELS).unwrap();
        file["dag"]["nodes"][0]["payload"]["NonASAP"]["Scan"]["predicates"][0]["Compare"]
            ["right"] = serde_json::json!({"Literal": {"Utf8": ""}});
        assert_eq!(refused_field(&file.to_string()), (0, "predicates".into()));
    }

    #[test]
    fn an_output_that_is_not_a_view_is_refused() {
        let text = q01_with(|f| f["outputs"] = serde_json::json!([0]));
        assert!(matches!(
            parse(&text),
            Err(PlannerViewsError::OutputNotAView { node: 0 })
        ));
    }

    #[test]
    fn an_invalid_sub_dag_is_refused() {
        let text = q01_with(|f| f["dag"]["roots"] = serde_json::json!([7]));
        assert!(matches!(
            parse(&text),
            Err(PlannerViewsError::InvalidDag(_))
        ));
    }

    #[test]
    fn unknown_fields_are_refused() {
        let text = q01_with(|f| f["extra"] = 1.into());
        assert!(matches!(parse(&text), Err(PlannerViewsError::Parse(_))));
    }

    /// The seam: a checked view becomes the processor's window state. Not
    /// built until the engine for collector views is chosen.
    #[test]
    #[ignore = "collector views are not mapped onto a window state yet"]
    fn q01_view_becomes_a_ddsketch_window_state() {
        let view = &parse(Q01).unwrap().views[0];
        let config = window_state_config(view);
        assert_eq!(config.sketch_type, "ddsketch");
        assert_eq!(config.sketch_params["relative_accuracy"], 0.05);
        assert_eq!(config.window_size, std::time::Duration::from_secs(5));
        assert_eq!(config.output_metric_name, "data.asap_view.1");
    }
}
