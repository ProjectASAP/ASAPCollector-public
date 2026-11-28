//! Builds the states of an installed plan's collector views on ASAPPlanner's
//! executor (`asap-executor`), the engine the backend builds the same views
//! with.
//!
//! [`ViewExecutor::new`] compiles the collector sub-DAG of a loaded
//! `collector-views.json` once ([`compile`], every output a root). The views
//! of one source metric and one tumbling width form a run group: each window
//! `(end - width, end]` of a group is one executor ingestion run
//! ([`RunContext`] with `Scope::Ingestion`, [`CompiledPhysicalDAG::instantiate`],
//! `execute_ingestion` over the group's outputs), started when the window
//! receives its first sample. The run's `Scan`s read a [`DataStore`] whose raw
//! data is a stream: each received batch of points becomes one Arrow batch
//! per `Scan` schema, sent to every open run it falls in, so one batch of a
//! metric reaches every view reading it. The executor re-applies the window
//! bounds and the views' label predicates.
//!
//! Series: each `(resource attributes, point attributes)` of a metric is
//! interned to a dense id on its first point, and its attributes and scope
//! kept for the output until every window holding it has closed; the id is
//! then reused. The `$promql_series_identity` column carries the id as
//! decimal text: a collector view is a leaf chain (`planner_views` refuses
//! anything else), which only groups by it, so the text need not be the
//! label map's canonical JSON. Label columns hold the point attribute of
//! their name, empty when the point has none, as the executor's own PromQL
//! label columns read a missing label.
//!
//! Windows close as `asap_sketches` closes them, per run group: a sample
//! closes every window of its group ending before its own window starts, and
//! [`ViewExecutor::close_idle`] every window ending before a time (the
//! processor's idle timer). Closing a window ends its run's streams; the run
//! seals its states, each encoded to ASAPv1 into a [`SketchEnvelope`]
//! carrying the view's identity, the series' attributes and the window. A
//! sample of a window its group has closed is late, and a value a view's
//! sketch refuses (NaN, ±Inf, a DDSketch magnitude outside its range) is
//! rejected: both are dropped and counted ([`ViewExecutor::dropped`]). A run
//! that fails anyway loses that window only ([`Closed::failures`]).
//!
//! Runs execute on one single-threaded [`LocalPool`], driven to a stall after
//! every batch and every close; nothing here blocks.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::sync::Arc;

use asap_executor::arrow::array::{
    ArrayRef, AsArray, DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray,
    UInt32Array,
};
use asap_executor::arrow::datatypes::UInt32Type;
use asap_executor::factory::{create_planner_accumulator, AccumulatorUpdater};
use asap_executor::physical_planner::{compile, CompiledPhysicalDAG};
use asap_executor::planner::ir::operator::Source;
use asap_executor::planner::ir::scalar::{resolve_column_ref, ColumnRef};
use asap_executor::planner::ir::schema::{DataType, FieldDataType, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::ir::{ASAPOp, Operator};
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::{PhysicalASAPDAG, PhysicalASAPNodeId};
use asap_executor::runtime::{IngestionOutputs, Limits, OutputStream, RunContext, Scope};
use asap_executor::sources::{DataStore, MaterializedViewRequest, RawDataRequest};
use asap_executor::summary_kernels::{DDSketchAccumulator, DatasketchesKLLAccumulator};
use asap_executor::values::{Batch, BatchColumn, SchemaRef, StateColumn};
use asap_executor::Error;
use asap_sketchlib::SketchAlgorithm;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::executor::LocalPool;
use futures::task::LocalSpawnExt;
use futures::StreamExt;

use super::config::SKETCH_TYPES;
use super::planner_views::{CollectorView, PlannerViews};
use crate::envelope::{SketchEnvelope, SketchType, ViewIdentity};
use crate::matchers::write_series_key;
use crate::observation::{KeyValue, Observation, ObservationValueKind, SourceDescriptor};
use crate::window::window_end_ms;

/// The memory one window's run may hold (its states and queued batches)
/// before the executor fails it; well above 100,000 series of a 5 s DDSketch.
const RUN_MEMORY_LIMIT_BYTES: usize = 1 << 30;

/// An OTLP delta point's temporality, as `asap_sketches` stamps its states.
const DELTA_TEMPORALITY: i32 = 1;

/// One sample routed to a run group: (series id, timestamp ms, value).
type Row = (u32, u64, f64);

/// The collector views of one installed plan, built on the executor.
pub struct ViewExecutor {
    compiled: CompiledPhysicalDAG,
    /// The views, by source metric name.
    metrics: HashMap<String, SourceMetric>,
    pool: LocalPool,
    dropped: DroppedSamples,
}

/// Samples a run group dropped, once per group dropping them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DroppedSamples {
    /// Of a window the group had closed.
    pub late: u64,
    /// A value one of the group's sketches refuses.
    pub rejected: u64,
}

/// The windows one call closed.
#[derive(Debug, Default)]
pub struct Closed {
    /// One per (view, series) of each closed window.
    pub states: Vec<SketchEnvelope>,
    /// Each window lost to a failed run or batch, e.g. `data, window ending
    /// 1767225600000: <executor error>`.
    pub failures: Vec<String>,
}

/// One source metric: its series and the run groups reading it.
struct SourceMetric {
    /// Series key (`write_series_key` of resource, then point attributes)
    /// to series id.
    ids: HashMap<String, u32>,
    /// By series id; `None` once evicted, until the id is reused.
    series: Vec<Option<Series>>,
    /// Evicted ids, reused first.
    free_ids: Vec<u32>,
    groups: Vec<RunGroup>,
    /// Reused buffer for the series key of the point being interned.
    key: String,
    /// Per series id, its code in the batch being built (`u32::MAX`: none).
    codes: Vec<u32>,
}

/// What a series' states are emitted with.
struct Series {
    /// The key it is interned under.
    key: String,
    /// The `$promql_series_identity` text: the series id in decimal.
    identity: Box<str>,
    resource_labels: Vec<KeyValue>,
    labels: Vec<KeyValue>,
    source: Arc<SourceDescriptor>,
    /// The latest window end (ms), over the groups, that holds a sample of
    /// it; once every group has closed it, the series is evicted.
    last_window_end_ms: u64,
}

/// The views of one source metric and one tumbling width.
struct RunGroup {
    width_ms: u64,
    /// The views' output nodes, in [`Self::views`] order.
    roots: Vec<u64>,
    views: Vec<GroupView>,
    /// The distinct `Scan` nodes below the views.
    scan_nodes: Vec<PhysicalASAPNodeId>,
    /// The distinct `Scan` schemas.
    scans: Vec<ScanSlot>,
    /// Every window ending at or before this has closed (ms).
    closed_through_ms: u64,
    /// Open windows by end (ms).
    windows: BTreeMap<u64, WindowRun>,
    /// The samples taken since the last flush.
    rows: Vec<Row>,
}

/// What a view's states are emitted as, and the values its sketch takes.
struct GroupView {
    identity: Arc<ViewIdentity>,
    output_metric_name: String,
    sketch: SketchAlgorithm,
    sketch_type: SketchType,
    /// The executor's kernel for the view's sketch, used only to check a
    /// value as the run will.
    domain: Box<dyn AccumulatorUpdater>,
}

/// One `Scan` schema of a run group, read by `readers` `Scan` nodes.
struct ScanSlot {
    schema: SchemaRef,
    columns: Vec<ScanColumn>,
    readers: usize,
}

/// How a batch fills one `Scan` column.
enum ScanColumn {
    Timestamp,
    Value,
    Identity,
    /// The point attribute of this name.
    Label(String),
}

/// One open window's ingestion run.
struct WindowRun {
    /// Per scan slot, the stream of each of its readers. Dropping them ends
    /// the run's input.
    senders: Vec<Vec<UnboundedSender<Batch>>>,
    /// Set when the run finishes.
    result: RunResult,
}

/// A window's run outcome, set by the run's task when it finishes.
type RunResult = Rc<RefCell<Option<Result<IngestionOutputs<Batch>, Error>>>>;

/// The raw data of one window's run: per `Scan` schema, one stream per
/// reader. `Scan`s of equal schema read the same batches, so which of them
/// takes which stream does not matter.
struct WindowStore {
    receivers: RefCell<Vec<(SchemaRef, UnboundedReceiver<Batch>)>>,
}

impl DataStore for WindowStore {
    fn read_raw_data(
        &self,
        _source: &Source,
        request: &RawDataRequest,
        _context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        let mut receivers = self.receivers.borrow_mut();
        let index = receivers
            .iter()
            .position(|(schema, _)| schema == request.schema)
            .ok_or_else(|| Error::Invalid("a Scan without a collector stream".into()))?;
        let (_, receiver) = receivers.swap_remove(index);
        Ok(receiver.map(Ok).boxed_local())
    }

    fn read_materialized_view(
        &self,
        view: &MaterializedView,
        _request: &MaterializedViewRequest,
        _context: RunContext,
    ) -> Result<OutputStream<'_, Batch>, Error> {
        Err(Error::Invalid(format!(
            "a collector view reads no stored view (node {})",
            view.node_id
        )))
    }
}

impl ViewExecutor {
    /// Compiles `views`' sub-DAG with every output as a root, and groups the
    /// outputs by source metric and tumbling width. Fails on a view whose
    /// states the collector cannot encode.
    pub fn new(views: &PlannerViews) -> Result<Self, Error> {
        let roots: Vec<u64> = views.views.iter().map(|v| v.identity.node_id).collect();
        let compiled = compile(&views.dag, &[], &roots)?;
        let mut metrics: HashMap<String, SourceMetric> = HashMap::new();
        for view in &views.views {
            let metric = metrics
                .entry(view.source_metric.clone())
                .or_insert_with(|| SourceMetric {
                    ids: HashMap::new(),
                    series: Vec::new(),
                    free_ids: Vec::new(),
                    groups: Vec::new(),
                    key: String::new(),
                    codes: Vec::new(),
                });
            let index = match metric
                .groups
                .iter()
                .position(|g| g.width_ms == view.tumbling_window_width_ms)
            {
                Some(index) => index,
                None => {
                    metric
                        .groups
                        .push(RunGroup::new(view.tumbling_window_width_ms));
                    metric.groups.len() - 1
                }
            };
            metric.groups[index].add(view, &views.dag, &compiled)?;
        }
        Ok(Self {
            compiled,
            metrics,
            pool: LocalPool::new(),
            dropped: DroppedSamples::default(),
        })
    }

    /// Takes one point: a float sample of a metric a view reads goes to
    /// every run group of the metric that has its window open and whose
    /// sketches take its value (otherwise that group drops and counts it);
    /// any other point is ignored. Built at the next [`Self::flush`].
    pub fn observe(&mut self, obs: &Observation) {
        if obs.value.kind != ObservationValueKind::Float {
            return;
        }
        let Some(metric) = self.metrics.get_mut(obs.metric.as_str()) else {
            return;
        };
        let value = obs.value.float;
        let mut id = None;
        for group in 0..metric.groups.len() {
            let run_group = &mut metric.groups[group];
            if !run_group.takes(value) {
                self.dropped.rejected += 1;
                continue;
            }
            if !run_group.admit(obs.timestamp_ms) {
                self.dropped.late += 1;
                continue;
            }
            let end = window_end_ms(obs.timestamp_ms, run_group.width_ms);
            let id = *id.get_or_insert_with(|| metric.intern(obs));
            let series = metric.series[id as usize].as_mut().expect("interned");
            series.last_window_end_ms = series.last_window_end_ms.max(end);
            metric.groups[group]
                .rows
                .push((id, obs.timestamp_ms, value));
        }
    }

    /// Sends the samples taken since the last flush to their windows' runs,
    /// then closes the windows they passed.
    pub fn flush(&mut self) -> Closed {
        let mut closed = Closed::default();
        self.send(&mut closed);
        self.seal(&mut closed);
        closed
    }

    /// Like [`Self::flush`], then closes every window ending before the
    /// window holding `now_ms` (the processor's idle timer, `now_ms` already
    /// less its grace).
    pub fn close_idle(&mut self, now_ms: u64) -> Closed {
        let mut closed = Closed::default();
        self.send(&mut closed);
        for group in self.metrics.values_mut().flat_map(|m| &mut m.groups) {
            let through = window_end_ms(now_ms, group.width_ms).saturating_sub(group.width_ms);
            group.closed_through_ms = group.closed_through_ms.max(through);
        }
        self.seal(&mut closed);
        closed
    }

    /// Like [`Self::flush`], then closes every open window (shutdown). A
    /// later sample of a drained window is late.
    pub fn drain(&mut self) -> Closed {
        let mut closed = Closed::default();
        self.send(&mut closed);
        for group in self.metrics.values_mut().flat_map(|m| &mut m.groups) {
            if let Some(&last) = group.windows.keys().next_back() {
                group.closed_through_ms = group.closed_through_ms.max(last);
            }
        }
        self.seal(&mut closed);
        closed
    }

    /// The samples dropped so far.
    pub fn dropped(&self) -> DroppedSamples {
        self.dropped
    }

    /// Sends every group's taken rows to their runs.
    fn send(&mut self, closed: &mut Closed) {
        for (name, metric) in &mut self.metrics {
            for group in &mut metric.groups {
                group.send(
                    name,
                    &metric.series,
                    &mut metric.codes,
                    &self.compiled,
                    &self.pool,
                    &mut closed.failures,
                );
            }
        }
        self.pool.run_until_stalled();
    }

    /// Ends the run of every window its group has closed, encodes the
    /// states, and evicts the series no open window holds.
    fn seal(&mut self, closed: &mut Closed) {
        let mut closing = Vec::new();
        for (name, metric) in &mut self.metrics {
            for (index, group) in metric.groups.iter_mut().enumerate() {
                if group
                    .windows
                    .first_key_value()
                    .is_none_or(|(&end, _)| end > group.closed_through_ms)
                {
                    continue;
                }
                let still_open = group
                    .windows
                    .split_off(&group.closed_through_ms.saturating_add(1));
                for (end, run) in std::mem::replace(&mut group.windows, still_open) {
                    // Dropping `run.senders` ends the run's streams.
                    closing.push((name.clone(), index, end, run.result));
                }
            }
        }
        if closing.is_empty() {
            return;
        }
        self.pool.run_until_stalled();
        for (name, index, end, result) in &closing {
            let metric = &self.metrics[name];
            let group = &metric.groups[*index];
            let outcome = result
                .borrow_mut()
                .take()
                .unwrap_or_else(|| {
                    Err(Error::Invalid(
                        "the run did not finish at the window's close".into(),
                    ))
                })
                .and_then(|outputs| {
                    let mut states = Vec::new();
                    for (view, batches) in group.views.iter().zip(&outputs) {
                        for batch in batches {
                            encode(
                                view,
                                batch,
                                *end,
                                group.width_ms,
                                &metric.series,
                                &mut states,
                            )?;
                        }
                    }
                    Ok(states)
                });
            match outcome {
                Ok(states) => closed.states.extend(states),
                Err(error) => closed
                    .failures
                    .push(format!("{name}, window ending {end}: {error}")),
            }
        }
        for (name, ..) in &closing {
            if let Some(metric) = self.metrics.get_mut(name) {
                metric.evict_closed_series();
            }
        }
    }
}

impl SourceMetric {
    /// `obs`'s series id, interning the series on its first point.
    fn intern(&mut self, obs: &Observation) -> u32 {
        write_series_key(&mut self.key, 0, &obs.resource_labels, &obs.labels, &[]);
        if let Some(&id) = self.ids.get(self.key.as_str()) {
            return id;
        }
        let id = self.free_ids.pop().unwrap_or(self.series.len() as u32);
        let series = Series {
            key: self.key.clone(),
            identity: id.to_string().into(),
            resource_labels: obs.resource_labels.clone(),
            labels: obs.labels.clone(),
            source: Arc::clone(&obs.source),
            last_window_end_ms: 0,
        };
        match self.series.get_mut(id as usize) {
            Some(slot) => *slot = Some(series),
            None => self.series.push(Some(series)),
        }
        self.ids.insert(self.key.clone(), id);
        id
    }

    /// Evicts every series whose windows have all closed, in every group.
    fn evict_closed_series(&mut self) {
        let closed_through_ms = self
            .groups
            .iter()
            .map(|g| g.closed_through_ms)
            .min()
            .unwrap_or(0);
        for (id, slot) in self.series.iter_mut().enumerate() {
            if slot
                .as_ref()
                .is_some_and(|s| s.last_window_end_ms <= closed_through_ms)
            {
                let series = slot.take().expect("checked");
                self.ids.remove(&series.key);
                self.free_ids.push(id as u32);
            }
        }
    }
}

impl RunGroup {
    fn new(width_ms: u64) -> Self {
        Self {
            width_ms,
            roots: Vec::new(),
            views: Vec::new(),
            scan_nodes: Vec::new(),
            scans: Vec::new(),
            closed_through_ms: 0,
            windows: BTreeMap::new(),
            rows: Vec::new(),
        }
    }

    /// Adds `view` (node of `dag`), and its `Scan` unless another view of
    /// the group reads the same `Scan` node (a node feeding several roots is
    /// read once).
    fn add(
        &mut self,
        view: &CollectorView,
        dag: &PhysicalASAPDAG,
        compiled: &CompiledPhysicalDAG,
    ) -> Result<(), Error> {
        if !matches!(
            view.sketch,
            SketchAlgorithm::DDSketch | SketchAlgorithm::KLL
        ) {
            return Err(Error::Invalid(format!(
                "node {}: the collector does not encode {:?} states",
                view.identity.node_id, view.sketch
            )));
        }
        let spec = SKETCH_TYPES
            .iter()
            .find(|spec| spec.algorithm == view.sketch)
            .expect("the loader accepts only SKETCH_TYPES sketches");
        let node = dag
            .nodes
            .iter()
            .find(|n| n.id as u64 == view.identity.node_id)
            .expect("a loaded view is a node of the DAG");
        let Operator::ASAP(ASAPOp::SummaryTumblingWindowChain { input, .. }) = &node.payload else {
            unreachable!("a loaded view is a SummaryTumblingWindowChain");
        };
        let domain = create_planner_accumulator(
            &FieldDataType::Sketch(view.sketch),
            node.parameters.as_ref(),
            input,
        )
        .map_err(|e| Error::Invalid(format!("node {}: {e}", view.identity.node_id)))?;
        self.roots.push(view.identity.node_id);
        self.views.push(GroupView {
            identity: Arc::clone(&view.identity),
            output_metric_name: view.output_metric_name.clone(),
            sketch: view.sketch,
            sketch_type: spec.sketch_type,
            domain,
        });
        if self.scan_nodes.contains(&view.scan_node) {
            return Ok(());
        }
        self.scan_nodes.push(view.scan_node);
        let schema = compiled.output_schema(view.scan_node as u64)?;
        match self.scans.iter_mut().find(|slot| slot.schema == schema) {
            Some(slot) => slot.readers += 1,
            None => self.scans.push(ScanSlot {
                columns: scan_columns(&schema)?,
                schema,
                readers: 1,
            }),
        }
        Ok(())
    }

    /// Whether every view's sketch takes `value`; a value one refuses would
    /// fail the window's whole run, which the group's views share. So a
    /// value only a DDSketch refuses (e.g. `5e-324`) is dropped for a KLL
    /// view of the same group too.
    fn takes(&self, value: f64) -> bool {
        self.views
            .iter()
            .all(|view| view.domain.validate_single_input(value).is_ok())
    }

    /// Whether a sample at `ts` is on time; an on-time sample closes every
    /// window ending before its own window starts.
    fn admit(&mut self, ts: u64) -> bool {
        if ts <= self.closed_through_ms {
            return false;
        }
        let start = window_end_ms(ts, self.width_ms) - self.width_ms;
        self.closed_through_ms = self.closed_through_ms.max(start);
        true
    }

    /// Sends the rows taken since the last flush to the runs of their
    /// windows, one batch per window and `Scan` schema, opening runs as
    /// needed. A window whose run cannot start, or whose batch cannot be
    /// built, is reported in `failures` and its rows dropped.
    fn send(
        &mut self,
        metric: &str,
        series: &[Option<Series>],
        codes: &mut Vec<u32>,
        compiled: &CompiledPhysicalDAG,
        pool: &LocalPool,
        failures: &mut Vec<String>,
    ) {
        if self.rows.is_empty() {
            return;
        }
        let rows = std::mem::take(&mut self.rows);
        let width = self.width_ms;
        let first_end = window_end_ms(rows[0].1, width);
        // Usually every row of a flush is in one window.
        let windows: Vec<(u64, Vec<Row>)> =
            if rows.iter().all(|r| window_end_ms(r.1, width) == first_end) {
                vec![(first_end, rows)]
            } else {
                let mut by_window: BTreeMap<u64, Vec<Row>> = BTreeMap::new();
                for row in rows {
                    by_window
                        .entry(window_end_ms(row.1, width))
                        .or_default()
                        .push(row);
                }
                by_window.into_iter().collect()
            };
        for (end, rows) in windows {
            if let Err(error) = self.send_window(end, &rows, series, codes, compiled, pool) {
                failures.push(format!("{metric}, window ending {end}: {error}"));
            }
        }
    }

    fn send_window(
        &mut self,
        end: u64,
        rows: &[Row],
        series: &[Option<Series>],
        codes: &mut Vec<u32>,
        compiled: &CompiledPhysicalDAG,
        pool: &LocalPool,
    ) -> Result<(), Error> {
        if !self.windows.contains_key(&end) {
            let run = self.open(end, compiled, pool)?;
            self.windows.insert(end, run);
        }
        let columns = TextColumns::new(rows, codes);
        let run = &self.windows[&end];
        for (slot, senders) in self.scans.iter().zip(&run.senders) {
            let batch = slot.batch(rows, &columns, series)?;
            for sender in senders {
                // A run that failed dropped its stream; its error surfaces
                // when the window closes.
                let _ = sender.unbounded_send(batch.clone());
            }
        }
        Ok(())
    }

    /// Starts the ingestion run of the window ending at `end`.
    fn open(
        &self,
        end: u64,
        compiled: &CompiledPhysicalDAG,
        pool: &LocalPool,
    ) -> Result<WindowRun, Error> {
        let mut senders = Vec::with_capacity(self.scans.len());
        let mut receivers = Vec::new();
        for slot in &self.scans {
            let mut slot_senders = Vec::with_capacity(slot.readers);
            for _ in 0..slot.readers {
                let (sender, receiver) = unbounded();
                slot_senders.push(sender);
                receivers.push((slot.schema.clone(), receiver));
            }
            senders.push(slot_senders);
        }
        let context = RunContext::new(
            Scope::Ingestion {
                window_start_ms: (end - self.width_ms) as i64,
                window_end_ms: end as i64,
            },
            Limits {
                max_bytes: RUN_MEMORY_LIMIT_BYTES,
            },
        )?;
        // The executor takes its store as `Arc<dyn DataStore>`; this one is
        // read on this thread only.
        #[allow(clippy::arc_with_non_send_sync)]
        let store: Arc<dyn DataStore> = Arc::new(WindowStore {
            receivers: RefCell::new(receivers),
        });
        let instance = compiled.instantiate(store)?;
        let roots = self.roots.clone();
        let result: RunResult = Rc::default();
        let finished = Rc::clone(&result);
        pool.spawner()
            .spawn_local(async move {
                let run = async { instance.execute_ingestion(&roots, context)?.await };
                *finished.borrow_mut() = Some(run.await);
            })
            .map_err(|e| Error::Invalid(format!("cannot start a window's run: {e}")))?;
        Ok(WindowRun { senders, result })
    }
}

/// The dictionary keys every text column of one window's batch shares: each
/// row's code, the index of its series among the batch's distinct series in
/// first-appearance order.
struct TextColumns {
    keys: UInt32Array,
    /// The batch's distinct series ids, by code.
    distinct: Vec<u32>,
}

impl TextColumns {
    /// `codes` is scratch indexed by series id, `u32::MAX` outside a call.
    fn new(rows: &[Row], codes: &mut Vec<u32>) -> Self {
        let mut distinct = Vec::new();
        let keys = rows
            .iter()
            .map(|&(id, _, _)| {
                let id = id as usize;
                if codes.len() <= id {
                    codes.resize(id + 1, u32::MAX);
                }
                if codes[id] == u32::MAX {
                    codes[id] = distinct.len() as u32;
                    distinct.push(id as u32);
                }
                codes[id]
            })
            .collect();
        for &id in &distinct {
            codes[id as usize] = u32::MAX;
        }
        Self { keys, distinct }
    }

    /// A text column: one value per distinct series.
    fn column<'a>(&self, values: impl Iterator<Item = &'a str>) -> Result<ArrayRef, Error> {
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(values));
        Ok(Arc::new(
            DictionaryArray::<UInt32Type>::try_new(self.keys.clone(), values)
                .map_err(|e| Error::Invalid(e.to_string()))?,
        ))
    }
}

/// How a batch fills each column of a `Scan` `schema`: its time index, its
/// sample value, the series identity, and labels (text).
fn scan_columns(schema: &SchemaRef) -> Result<Vec<ScanColumn>, Error> {
    let value = resolve_column_ref(&ColumnRef::SampleValue, schema)
        .map_err(|e| Error::Invalid(format!("a collector view's Scan: {e}")))?;
    let plain = |i: usize, dtype: DataType| schema.fields[i].dtype == FieldDataType::Plain(dtype);
    schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            Ok(
                if Some(i) == schema.time_index && plain(i, DataType::Timestamp) {
                    ScanColumn::Timestamp
                } else if i == value && plain(i, DataType::Float64) {
                    ScanColumn::Value
                } else if field.name == PROMQL_SERIES_IDENTITY && plain(i, DataType::Utf8) {
                    ScanColumn::Identity
                } else if plain(i, DataType::Utf8) {
                    ScanColumn::Label(field.name.clone())
                } else {
                    return Err(Error::Invalid(format!(
                        "a collector view's Scan column {:?} is not a timestamp, a float64 value, \
                     the series identity or a label",
                        field.name
                    )));
                },
            )
        })
        .collect()
}

impl ScanSlot {
    /// `rows` in this slot's schema, text columns keyed by `text`.
    fn batch(
        &self,
        rows: &[Row],
        text: &TextColumns,
        series: &[Option<Series>],
    ) -> Result<Batch, Error> {
        let series_of = |code: &u32| series[*code as usize].as_ref().expect("a live series");
        let columns = self
            .columns
            .iter()
            .map(|column| {
                let array: ArrayRef = match column {
                    ScanColumn::Timestamp => Arc::new(TimestampMillisecondArray::from_iter_values(
                        rows.iter().map(|r| r.1 as i64),
                    )),
                    ScanColumn::Value => {
                        Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.2)))
                    }
                    ScanColumn::Identity => {
                        text.column(text.distinct.iter().map(|id| &*series_of(id).identity))?
                    }
                    ScanColumn::Label(name) => text.column(text.distinct.iter().map(|id| {
                        series_of(id)
                            .labels
                            .iter()
                            .find(|kv| kv.key == *name)
                            .map_or("", |kv| kv.value.as_str())
                    }))?,
                };
                Ok(BatchColumn::Plain(array))
            })
            .collect::<Result<_, Error>>()?;
        Batch::try_from_columns(self.schema.clone(), columns)
    }
}

/// Appends one envelope per row of `batch`, a run's output for `view` of
/// the window ending at `end_ms`: the row's state as ASAPv1, under the
/// view's identity and name, with its series' attributes.
fn encode(
    view: &GroupView,
    batch: &Batch,
    end_ms: u64,
    width_ms: u64,
    series: &[Option<Series>],
    out: &mut Vec<SketchEnvelope>,
) -> Result<(), Error> {
    let fields = &batch.schema().fields;
    let state = fields
        .iter()
        .position(|f| !f.is_plain())
        .and_then(|i| Some((i, batch.state_column(i)?)))
        .ok_or_else(|| Error::Invalid("a collector view's output has no state column".into()))?;
    let identity = fields
        .iter()
        .position(|f| f.name == PROMQL_SERIES_IDENTITY)
        .ok_or_else(|| Error::Invalid("a collector view's output has no series identity".into()))?;
    let indices = batch.column(state.0).as_primitive::<UInt32Type>();
    let identities = batch.column(identity).as_dictionary::<UInt32Type>();
    let texts = identities.values().as_string::<i32>();
    for row in 0..batch.num_rows() {
        let text = texts.value(identities.keys().value(row) as usize);
        let series = text
            .parse::<usize>()
            .ok()
            .and_then(|id| series.get(id)?.as_ref())
            .ok_or_else(|| Error::Invalid(format!("an unknown series identity {text:?}")))?;
        let (payload, count) = asapv1(view.sketch, state.1, indices.value(row) as usize)?;
        out.push(SketchEnvelope {
            schema_version: 1,
            sketch_type: view.sketch_type,
            view: Some(Arc::clone(&view.identity)),
            resource_labels: series.resource_labels.clone(),
            labels: series.labels.clone(),
            source: Arc::clone(&series.source),
            window_start_ms: end_ms - width_ms,
            window_end_ms: end_ms,
            payload,
            metric_name: view.output_metric_name.clone(),
            count,
            aggregation_temporality: DELTA_TEMPORALITY,
            value: 0.0,
        });
    }
    Ok(())
}

/// State `index` of `states` as ASAPv1 bytes, and the samples it holds.
fn asapv1(
    sketch: SketchAlgorithm,
    states: &StateColumn,
    index: usize,
) -> Result<(Vec<u8>, u64), Error> {
    let state = states.state(index);
    let state = state.as_any();
    let not = || Error::Invalid(format!("a {sketch:?} view's state is of another family"));
    let codec = |e: &dyn std::fmt::Display| Error::Invalid(format!("ASAPv1: {e}"));
    match sketch {
        SketchAlgorithm::DDSketch => {
            let sketch = state
                .downcast_ref::<DDSketchAccumulator>()
                .ok_or_else(not)?
                .sketch();
            Ok((
                sketch.to_asapv1().map_err(|e| codec(&e))?,
                sketch.get_count(),
            ))
        }
        SketchAlgorithm::KLL => {
            let sketch = state
                .downcast_ref::<DatasketchesKLLAccumulator>()
                .ok_or_else(not)?
                .sketch();
            Ok((
                sketch.to_asapv1().map_err(|e| codec(&e))?,
                sketch.count() as u64,
            ))
        }
        other => Err(Error::Invalid(format!(
            "a {other:?} view is not encoded by the collector"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation::ObservationValue;
    use asap_executor::sources::InMemoryDataStore;
    use asap_executor::values::Value;
    use asap_sketchlib::DdSketch;
    use std::collections::BTreeMap as Map;

    const Q01: &str = include_str!("../../tests/fixtures/collector-views/q01/collector-views.json");
    const Q01_LABELS: &str = include_str!(
        "../../tests/fixtures/collector-views/q01-label-matchers/collector-views.json"
    );
    /// A window end: 2026-01-01T00:00:00Z.
    const T: u64 = 1_767_225_600_000;

    /// Series `i` of `data`: Resource `service.name`, point attributes
    /// `region`, `service` (absent for series 3) and `label_0`.
    fn sample(i: usize, ts: u64, value: f64) -> Observation {
        let mut labels = vec![
            KeyValue::new("region", format!("r{}", i % 3)),
            KeyValue::new("label_0", format!("l{i}")),
        ];
        if i != 3 {
            labels.insert(1, KeyValue::new("service", format!("s{}", i % 2)));
        }
        Observation {
            timestamp_ms: ts,
            metric: "data".into(),
            resource_labels: vec![KeyValue::new("service.name", format!("j{}", i % 2))],
            labels,
            source: Arc::new(SourceDescriptor {
                scope_name: "app.meter".into(),
                unit: "s".into(),
                ..Default::default()
            }),
            value: ObservationValue::float(value),
        }
    }

    /// 6 series over the windows ending at T - 5 s, T and T + 5 s, 8 samples
    /// each per window, out of order within the window, one at its end.
    fn samples() -> Vec<Observation> {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1u64 << 53) as f64
        };
        let mut out = Vec::new();
        for end in [T - 5_000, T, T + 5_000] {
            for offset in [3_000, 4_999, 1_000, 2_500, 5_000, 4_000, 10, 2_000] {
                for i in 0..6 {
                    let value = (1.0 + i as f64) * (1.0 - next()).powf(-0.5);
                    out.push(sample(i, end - 5_000 + offset, value));
                }
            }
        }
        out
    }

    /// `samples` through a `ViewExecutor` of `file`, `batch` points per
    /// flush, then drained.
    fn build(file: &str, samples: &[Observation], batch: usize) -> Vec<SketchEnvelope> {
        let views = super::super::planner_views::parse(file).expect("loads");
        let mut executor = ViewExecutor::new(&views).expect("compiles");
        let mut out = Vec::new();
        for chunk in samples.chunks(batch) {
            for obs in chunk {
                executor.observe(obs);
            }
            out.extend(states(executor.flush()));
        }
        out.extend(states(executor.drain()));
        out
    }

    /// The states of a call that lost no window.
    fn states(closed: Closed) -> Vec<SketchEnvelope> {
        assert!(closed.failures.is_empty(), "{:?}", closed.failures);
        closed.states
    }

    fn series_key(resource: &[KeyValue], labels: &[KeyValue]) -> String {
        format!("{resource:?}{labels:?}")
    }

    /// Each state by (view node, series, window end).
    fn by_key(states: &[SketchEnvelope]) -> Map<(u64, String, u64), &SketchEnvelope> {
        states
            .iter()
            .map(|s| {
                let key = series_key(&s.resource_labels, &s.labels);
                let node = s.view.as_ref().unwrap().node_id;
                ((node, key, s.window_end_ms), s)
            })
            .collect()
    }

    /// The backend's build of view `root` of `file`: per window, every
    /// sample of the window in one batch of literal rows, appended to the
    /// executor's in-memory store, and one ingestion run over the compiled
    /// sub-DAG. The series identity is the label map's canonical JSON.
    /// Returns the ASAPv1 state by (series, window end).
    fn executor_build(
        file: &str,
        root: u64,
        samples: &[Observation],
    ) -> Map<(String, u64), Vec<u8>> {
        let views = super::super::planner_views::parse(file).expect("loads");
        let view = views
            .views
            .iter()
            .find(|v| v.identity.node_id == root)
            .unwrap();
        let width = view.tumbling_window_width_ms;
        let compiled = compile(&views.dag, &[], &[root]).unwrap();
        let schema = compiled.output_schema(view.scan_node as u64).unwrap();
        let source = Source::TimeSeries {
            metric: view.source_metric.clone(),
        };
        let mut windows: Map<u64, Vec<&Observation>> = Map::new();
        for obs in samples {
            windows
                .entry(window_end_ms(obs.timestamp_ms, width))
                .or_default()
                .push(obs);
        }
        let mut out = Map::new();
        for (end, window) in windows {
            let mut keys = HashMap::new();
            let rows = window
                .iter()
                .map(|obs| {
                    let mut map: Map<&str, &str> = obs
                        .resource_labels
                        .iter()
                        .chain(&obs.labels)
                        .map(|kv| (kv.key.as_str(), kv.value.as_str()))
                        .collect();
                    map.insert("__name__", &obs.metric);
                    let identity = serde_json::to_string(&map).unwrap();
                    keys.insert(
                        identity.clone(),
                        series_key(&obs.resource_labels, &obs.labels),
                    );
                    schema
                        .fields
                        .iter()
                        .map(|field| match field.name.as_str() {
                            "ts" => Value::Timestamp(obs.timestamp_ms as i64),
                            "value" => Value::Float64(obs.value.float),
                            PROMQL_SERIES_IDENTITY => Value::Utf8(identity.as_str().into()),
                            label => Value::Utf8(
                                obs.labels
                                    .iter()
                                    .find(|kv| kv.key == label)
                                    .map_or("", |kv| kv.value.as_str())
                                    .into(),
                            ),
                        })
                        .collect()
                })
                .collect();
            let batch = Batch::try_new(schema.clone(), rows).unwrap();
            let start = (end - width) as i64;
            let mut store = InMemoryDataStore::default()
                .with_appended_raw_data(source.clone(), schema.clone(), None)
                .unwrap();
            store
                .append_raw_data(&source, (start, end as i64), vec![batch])
                .unwrap();
            let instance = compiled.instantiate(Arc::new(store)).unwrap();
            let context = RunContext::new(
                Scope::Ingestion {
                    window_start_ms: start,
                    window_end_ms: end as i64,
                },
                Limits::default(),
            )
            .unwrap();
            let outputs =
                futures::executor::block_on(instance.execute_ingestion(&[root], context).unwrap())
                    .unwrap();
            for batch in &outputs[0] {
                let fields = &batch.schema().fields;
                let state = fields.iter().position(|f| !f.is_plain()).unwrap();
                let id = fields
                    .iter()
                    .position(|f| f.name == PROMQL_SERIES_IDENTITY)
                    .unwrap();
                let states = batch.state_column(state).unwrap();
                let indices = batch.column(state).as_primitive::<UInt32Type>();
                let dict = batch.column(id).as_dictionary::<UInt32Type>();
                let texts = dict.values().as_string::<i32>();
                for row in 0..batch.num_rows() {
                    let text = texts.value(dict.keys().value(row) as usize);
                    let (bytes, _) =
                        asapv1(view.sketch, states, indices.value(row) as usize).unwrap();
                    out.insert((keys[text].clone(), end), bytes);
                }
            }
        }
        out
    }

    /// The states of view `node` in `states`, as `executor_build` keys them.
    fn bytes_of(states: &[SketchEnvelope], node: u64) -> Map<(String, u64), Vec<u8>> {
        by_key(states)
            .into_iter()
            .filter(|((n, _, _), _)| *n == node)
            .map(|((_, series, end), s)| ((series, end), s.payload.clone()))
            .collect()
    }

    /// q01 (DDSketch α 0.05 per series, 5 s windows over `data`): each
    /// series' window becomes one state under the view's identity and name,
    /// with the series' Resource, attributes and scope, the window
    /// `(end - 5 s, end]` and its sample count.
    #[test]
    fn q01_view_builds_ddsketch_states_on_the_executor() {
        let samples = samples();
        let states = build(Q01, &samples, 7);
        let views = super::super::planner_views::parse(Q01).unwrap();
        // 6 series x 3 windows.
        assert_eq!(states.len(), 18);
        let state = by_key(&states)[&(
            1,
            series_key(&samples[0].resource_labels, &samples[0].labels),
            T,
        )];
        assert_eq!(state.view, Some(Arc::clone(&views.views[0].identity)));
        assert_eq!(state.metric_name, "data.asap_view.1");
        assert_eq!(state.sketch_type, SketchType::DDSketch);
        assert_eq!((state.window_start_ms, state.window_end_ms), (T - 5_000, T));
        assert_eq!(state.resource_labels, samples[0].resource_labels);
        assert_eq!(state.labels, samples[0].labels);
        assert_eq!(state.source, samples[0].source);
        assert_eq!(state.aggregation_temporality, 1);
        assert_eq!(state.count, 8);
        let sketch = DdSketch::from_asapv1(&state.payload).expect("ASAPv1 DDSketch");
        assert_eq!(sketch.get_count(), 8);
    }

    /// The states are byte-for-byte the executor's own build of the same
    /// samples, whatever the batching: one batch per window, as the backend
    /// builds, or 1, 7 or 100 points per flush here.
    #[test]
    fn states_are_the_executors_own_build() {
        let samples = samples();
        let reference = executor_build(Q01, 1, &samples);
        assert_eq!(reference.len(), 18);
        for batch in [1, 7, 100] {
            assert_eq!(
                bytes_of(&build(Q01, &samples, batch), 1),
                reference,
                "{batch} per flush"
            );
        }
    }

    /// The answers equal `asap_sketches`' (DDSketch α 0.05, 5 s tumbling) on
    /// the same samples: the count and the 0.5, 0.9 and 0.99 quantiles of
    /// every series' window.
    #[test]
    fn answers_equal_the_asap_sketches_path() {
        use crate::config::{AggregationMode, PrecomputeConfig, WindowSpec};
        use crate::precompute::{Precompute, PrecomputeImpl};
        use crate::sketches::ddsketch::{DDSketchObserver, DDSketchWrapper};

        let samples = samples();
        let views = build(Q01, &samples, 7);
        let config = PrecomputeConfig {
            sketch_type: SketchType::DDSketch,
            mode: AggregationMode::Tumbling,
            window: WindowSpec {
                size: std::time::Duration::from_millis(5_000),
                ..Default::default()
            },
            ..Default::default()
        };
        let sketches = PrecomputeImpl::new(
            Some(config),
            Some(Box::new(|| Box::new(DDSketchWrapper::new(0.05)))),
            Some(Box::new(DDSketchObserver)),
        );
        for obs in &samples {
            sketches.observe(obs).unwrap();
        }
        let legacy = sketches.drain();
        assert_eq!(legacy.len(), views.len());
        let legacy: Map<(String, u64), &SketchEnvelope> = legacy
            .iter()
            .map(|s| {
                (
                    (series_key(&s.resource_labels, &s.labels), s.window_end_ms),
                    s,
                )
            })
            .collect();
        for ((_, series, end), state) in by_key(&views) {
            let other = legacy[&(series.clone(), end)];
            assert_eq!(state.count, other.count);
            let a = DdSketch::from_asapv1(&state.payload).unwrap();
            let b = DdSketch::from_asapv1(&other.payload).unwrap();
            assert_eq!(a.get_count(), b.get_count());
            for q in [0.5, 0.9, 0.99] {
                assert_eq!(
                    a.get_value_at_quantile(q),
                    b.get_value_at_quantile(q),
                    "{series} {end} q{q}"
                );
            }
        }
    }

    /// Out of order within an open window is on time; a sample of a window a
    /// later sample closed is dropped and counted; the idle close and a
    /// drain close windows too, and later samples of them are late.
    #[test]
    fn late_samples_are_dropped_and_counted() {
        let views = super::super::planner_views::parse(Q01).unwrap();
        let mut executor = ViewExecutor::new(&views).unwrap();
        let late = |executor: &ViewExecutor| executor.dropped().late;
        for (ts, value) in [(T - 1_000, 1.0), (T - 4_000, 2.0), (T, 3.0)] {
            executor.observe(&sample(0, ts, value));
        }
        assert!(states(executor.flush()).is_empty(), "the window is open");
        // A sample of the next window closes the window ending at T.
        executor.observe(&sample(0, T + 1, 4.0));
        // A sample of the closed window, in the same batch: late.
        executor.observe(&sample(0, T - 2_000, 5.0));
        let closed = states(executor.flush());
        assert_eq!(closed.len(), 1);
        assert_eq!((closed[0].window_end_ms, closed[0].count), (T, 3));
        assert_eq!(late(&executor), 1);
        // The idle close: the window ending at T + 5 s closes once the
        // clock is past it.
        assert!(states(executor.close_idle(T + 5_000)).is_empty());
        let idle = states(executor.close_idle(T + 5_001));
        assert_eq!((idle[0].window_end_ms, idle[0].count), (T + 5_000, 1));
        executor.observe(&sample(0, T + 4_000, 6.0));
        assert_eq!(late(&executor), 2);
        // Taken but not flushed: the idle close sends it before closing.
        executor.observe(&sample(0, T + 9_000, 7.0));
        let idle = states(executor.close_idle(T + 10_001));
        assert_eq!((idle[0].window_end_ms, idle[0].count), (T + 10_000, 1));
        executor.observe(&sample(0, T + 14_000, 8.0));
        assert_eq!(states(executor.drain()).len(), 1);
        executor.observe(&sample(0, T + 13_000, 9.0));
        assert_eq!(late(&executor), 3);
        assert!(states(executor.flush()).is_empty());
        assert_eq!(executor.dropped().rejected, 0);
    }

    /// A value the view's DDSketch refuses (NaN, ±Inf, a nonzero magnitude
    /// below its range) is dropped and counted; the window's other samples
    /// and series build as without it.
    #[test]
    fn values_the_sketch_refuses_are_dropped_and_counted() {
        let samples = samples();
        let mut with_bad = samples.clone();
        for (i, value) in [(0, f64::NAN), (1, f64::INFINITY), (2, 5e-324)] {
            with_bad.insert(60, sample(i, T - 2_500, value));
        }
        let views = super::super::planner_views::parse(Q01).unwrap();
        let mut executor = ViewExecutor::new(&views).unwrap();
        let mut built = Vec::new();
        for obs in &with_bad {
            executor.observe(obs);
            built.extend(states(executor.flush()));
        }
        built.extend(states(executor.drain()));
        assert_eq!(
            executor.dropped(),
            DroppedSamples {
                late: 0,
                rejected: 3
            }
        );
        assert_eq!(bytes_of(&built, 1), executor_build(Q01, 1, &samples));
    }

    /// A series is evicted once every window holding it has closed, and its
    /// id reused by the next new series; a series seen again is interned
    /// anew and its states keep its own attributes.
    #[test]
    fn closed_series_are_evicted_and_ids_reused() {
        let views = super::super::planner_views::parse(Q01).unwrap();
        let mut executor = ViewExecutor::new(&views).unwrap();
        executor.observe(&sample(0, T - 1_000, 1.0));
        executor.observe(&sample(1, T - 1_000, 2.0));
        states(executor.flush());
        // Series 2 only, in the next window: 0 and 1 are evicted.
        executor.observe(&sample(2, T + 1_000, 3.0));
        assert_eq!(states(executor.flush()).len(), 2);
        let metric = &executor.metrics["data"];
        assert_eq!(metric.ids.len(), 1);
        assert_eq!(metric.series.iter().flatten().count(), 1);
        assert_eq!(metric.free_ids.len(), 2);
        // Series 1 again: it takes a freed id.
        executor.observe(&sample(1, T + 2_000, 4.0));
        let drained = states(executor.drain());
        assert_eq!(executor.metrics["data"].series.len(), 3);
        let mut labels: Vec<String> = drained
            .iter()
            .map(|s| {
                s.labels
                    .iter()
                    .find(|kv| kv.key == "label_0")
                    .unwrap()
                    .value
                    .clone()
            })
            .collect();
        labels.sort();
        assert_eq!(labels, ["l1", "l2"]);
    }

    /// Other metrics, and points that are not float samples, are ignored.
    #[test]
    fn points_no_view_reads_are_ignored() {
        let views = super::super::planner_views::parse(Q01).unwrap();
        let mut executor = ViewExecutor::new(&views).unwrap();
        let mut other = sample(0, T, 1.0);
        other.metric = "other".into();
        executor.observe(&other);
        let mut hash = sample(0, T, 1.0);
        hash.value = ObservationValue::hash(7);
        executor.observe(&hash);
        assert!(states(executor.flush()).is_empty());
        assert!(states(executor.drain()).is_empty());
        assert_eq!(executor.dropped(), DroppedSamples::default());
    }

    /// q01 with three more views of `data`: node 2, KLL k = 200 in 5 s
    /// windows (the same run group, reading the shared `Scan` once), node 3,
    /// DDSketch α 0.01 in 10 s windows (its own runs), and node 5 over a
    /// second `Scan` of the same schema. Each view's states are the
    /// executor's own build of that view alone.
    #[test]
    fn several_views_of_one_metric_share_its_batches() {
        let mut file: serde_json::Value = serde_json::from_str(Q01).unwrap();
        let chain = file["dag"]["nodes"][1].clone();
        let edge = file["dag"]["edges"][0].clone();
        let mut kll = chain.clone();
        kll["id"] = 2.into();
        kll["payload"]["ASAP"]["SummaryTumblingWindowChain"]["summary_type"] =
            serde_json::json!({"Sketch": "KLL"});
        kll["parameters"] = serde_json::json!({"Kll": {"k": 200}});
        kll["output_schema"]["fields"][1]["dtype"] = serde_json::json!({"Sketch": "KLL"});
        let mut wide = chain;
        wide["id"] = 3.into();
        wide["payload"]["ASAP"]["SummaryTumblingWindowChain"]["tumbling_window_width_ms"] =
            10_000.into();
        wide["parameters"] = serde_json::json!({"DDSketch": {"alpha": 0.01}});
        for (node, id) in [(kll, 2), (wide, 3)] {
            file["dag"]["nodes"].as_array_mut().unwrap().push(node);
            let mut edge = edge.clone();
            edge["consumer"] = id.into();
            file["dag"]["edges"].as_array_mut().unwrap().push(edge);
        }
        // Node 4: a second Scan of `data`, of the same schema as node 0;
        // node 5: DDSketch α 0.02 in 5 s windows over it.
        let mut scan = file["dag"]["nodes"][0].clone();
        scan["id"] = 4.into();
        let mut over_scan = file["dag"]["nodes"][1].clone();
        over_scan["id"] = 5.into();
        over_scan["payload"]["ASAP"]["SummaryTumblingWindowChain"]["child"] = 4.into();
        over_scan["parameters"] = serde_json::json!({"DDSketch": {"alpha": 0.02}});
        file["dag"]["nodes"]
            .as_array_mut()
            .unwrap()
            .extend([scan, over_scan]);
        let mut edge = edge.clone();
        edge["producer"] = 4.into();
        edge["consumer"] = 5.into();
        file["dag"]["edges"].as_array_mut().unwrap().push(edge);
        file["dag"]["roots"] = serde_json::json!([1, 2, 3, 5]);
        file["outputs"] = serde_json::json!([1, 2, 3, 5]);
        let file = file.to_string();

        let views = super::super::planner_views::parse(&file).expect("four views load");
        let executor = ViewExecutor::new(&views).unwrap();
        let groups = &executor.metrics["data"].groups;
        assert_eq!(groups.len(), 2, "5 s and 10 s");
        assert_eq!(groups[0].roots, [1, 2, 5]);
        assert_eq!(groups[0].scans.len(), 1, "nodes 0 and 4 have one schema");
        assert_eq!(groups[0].scans[0].readers, 2, "node 0 once, node 4 once");

        let samples = samples();
        let states = build(&file, &samples, 7);
        for (node, sketch_type) in [
            (1, SketchType::DDSketch),
            (2, SketchType::KLLSketch),
            (3, SketchType::DDSketch),
            (5, SketchType::DDSketch),
        ] {
            let reference = executor_build(&file, node, &samples);
            assert!(!reference.is_empty());
            assert_eq!(bytes_of(&states, node), reference, "node {node}");
            assert!(states
                .iter()
                .filter(|s| s.view.as_ref().unwrap().node_id == node)
                .all(|s| s.sketch_type == sketch_type
                    && s.metric_name == format!("data.asap_view.{node}")));
        }
    }

    /// `data{region="r0", service!="s1"}`: the executor applies the `Scan`'s
    /// matchers. Series 0 and 3 are in `r0`; series 0 is `s0`, and series 3
    /// has no `service`, which PromQL's `!=` keeps.
    #[test]
    fn label_matchers_select_series() {
        let samples = samples();
        let states = build(Q01_LABELS, &samples, 7);
        let selected: std::collections::BTreeSet<String> = states
            .iter()
            .map(|s| {
                s.labels
                    .iter()
                    .find(|kv| kv.key == "label_0")
                    .unwrap()
                    .value
                    .clone()
            })
            .collect();
        assert_eq!(selected.into_iter().collect::<Vec<_>>(), ["l0", "l3"]);
        assert_eq!(
            bytes_of(&states, 1),
            executor_build(Q01_LABELS, 1, &samples)
        );
    }
}
