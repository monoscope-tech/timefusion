//! Latest-N reads that open only the file groups that can still contribute.
//!
//! A `SortPreservingMergeExec` needs a head batch from EVERY input before it emits a
//! row, so `ORDER BY timestamp DESC LIMIT n` over a 24-group scan decodes 24 head
//! files; under a sparse filter that is most of each file (prod 10-08: 1.7 GB served
//! for 501 rows of a 7-day errors list). [`BoundedMergeExec`] is ClickHouse's
//! read-in-order "virtual row": each input's statistics give the first leading-key
//! value it can yield, and an input is opened only once the merge's next row would
//! not strictly precede that bound. A read-ahead window keeps reads in flight and
//! doubles whenever the open inputs run dry, so a needle search regains full fan-out.
//!
//! Only planned under a fetch ([`BoundedMergeForFetch`]): a full scan reads every
//! input anyway and keeps the plain merge's parallelism.

use std::{
    cmp::Ordering,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, Int64Array, RecordBatch},
        compute::{cast, interleave_record_batch},
        datatypes::{DataType, SchemaRef},
        row::{RowConverter, Rows, SortField},
    },
    common::{
        Result, ScalarValue,
        config::ConfigOptions,
        stats::Precision,
        tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion},
    },
    datasource::{physical_plan::FileScanConfig, source::DataSourceExec},
    execution::TaskContext,
    physical_expr::{LexOrdering, OrderingRequirements, PhysicalExpr, expressions::Column},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
        projection::ProjectionExec,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
        union::UnionExec,
    },
};
use futures::{Stream, StreamExt};

use super::optimizers::downcast;

/// Inputs opened before the first row: enough to overlap two reads.
const INITIAL_READ_AHEAD: usize = 2;
/// Delta add-file stats store timestamps at millisecond precision, so a recorded max
/// may sit up to 999 µs below the true one; bounds are widened by this much (Quickwit
/// #6865 returned wrong top-K rows by trusting truncated split bounds).
const STATS_SLACK_MICROS: i64 = 1_000;

/// [`SortPreservingMergeExec`] that opens input `p` only once a row at `bounds[p]` could be
/// next. `bounds[p]` is the first leading-key value `p` can yield in output order; `None`
/// opens `p` at the start.
#[derive(Debug)]
pub struct BoundedMergeExec {
    input: Arc<dyn ExecutionPlan>,
    expr: LexOrdering,
    bounds: Vec<Option<i64>>,
    fetch: Option<usize>,
    /// Output batch cap: the fetch of the limit above. Rows the merge picks are all read
    /// before it yields, so a full session batch would open groups the limit never needs.
    batch_rows: Option<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl BoundedMergeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, expr: LexOrdering, bounds: Vec<Option<i64>>, fetch: Option<usize>) -> Self {
        let properties = Arc::clone(SortPreservingMergeExec::new(expr.clone(), Arc::clone(&input)).properties());
        Self { input, expr, bounds, fetch, batch_rows: fetch, properties, metrics: ExecutionPlanMetricsSet::new() }
    }

    pub fn with_batch_rows(self, batch_rows: Option<usize>) -> Self {
        Self { batch_rows, ..self }
    }
}

impl DisplayAs for BoundedMergeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let bounded = self.bounds.iter().flatten().count();
        write!(f, "BoundedMergeExec: [{}], bounded_inputs={bounded}/{}", self.expr, self.bounds.len())?;
        self.fetch.map_or(Ok(()), |fetch| write!(f, ", fetch={fetch}"))
    }
}

impl ExecutionPlan for BoundedMergeExec {
    fn name(&self) -> &'static str {
        "BoundedMergeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        for sort in self.expr.iter() {
            if f(&sort.expr)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }
    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::from(self.expr.clone()))]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0), self.expr.clone(), self.bounds.clone(), self.fetch).with_batch_rows(self.batch_rows)))
    }
    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        Some(Arc::new(Self::new(Arc::clone(&self.input), self.expr.clone(), self.bounds.clone(), fetch).with_batch_rows(self.batch_rows.min(fetch).or(fetch))))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(datafusion::error::DataFusionError::Internal(format!("BoundedMergeExec has one partition, asked for {partition}")));
        }
        let schema = self.input.schema();
        let fields = self.expr.iter().map(|sort| Ok(SortField::new_with_options(sort.expr.data_type(&schema)?, sort.options))).collect::<Result<Vec<_>>>()?;
        // Unbounded inputs first, then bounded ones in the order their rows can be next.
        let lead = self.expr.first().options;
        let mut closed: Vec<usize> = (0..self.bounds.len()).collect();
        closed.sort_by_key(|&p| (self.bounds[p].is_some(), self.bounds[p].map(|b| if lead.descending { -b } else { b })));
        closed.reverse();
        let primitive_lead = matches!(self.expr.first().expr.data_type(&schema)?, DataType::Int64 | DataType::Timestamp(..));
        let mut stream = BoundedMergeStream {
            input: Arc::clone(&self.input),
            context: Arc::clone(&context),
            expr: self.expr.clone(),
            converter: RowConverter::new(fields)?,
            primitive_lead,
            schema,
            bounds: self.bounds.clone(),
            inputs: (0..self.bounds.len()).map(|_| Input::Closed).collect(),
            winners: vec![None; 2 * self.bounds.len().max(1).next_power_of_two()],
            closed,
            needs_head: Vec::new(),
            read_ahead: INITIAL_READ_AHEAD,
            pool: Vec::new(),
            picked: Vec::new(),
            remaining: self.fetch,
            batch_size: self.batch_rows.map_or(context.session_config().batch_size(), |rows| rows.clamp(1, context.session_config().batch_size())),
            opened: MetricBuilder::new(&self.metrics).counter("inputs_opened", partition),
            skipped: MetricBuilder::new(&self.metrics).counter("inputs_never_opened", partition),
            encoded: MetricBuilder::new(&self.metrics).counter("key_rows_encoded", partition),
            head_slots_checked: MetricBuilder::new(&self.metrics).counter("head_slots_checked", partition),
            pending_head_slots: 0,
            key_comparisons: MetricBuilder::new(&self.metrics).counter("key_comparisons", partition),
            pending_key_comparisons: 0,
            baseline: BaselineMetrics::new(&self.metrics, partition),
        };
        while stream.closed.last().is_some_and(|&p| stream.bounds[p].is_none()) {
            stream.open_next()?;
        }
        for _ in 0..INITIAL_READ_AHEAD {
            stream.open_next()?;
        }
        Ok(Box::pin(stream))
    }
}

enum Input {
    Closed,
    Open { stream: SendableRecordBatchStream, head: Option<Head> },
    Done,
}

/// The input's current batch: its slot in the merge's batch pool, sort-key rows, and
/// leading key as i64 (`None` = null), read at `cursor`.
struct Head {
    slot: usize,
    rows: SortKeys,
    lead: Int64Array,
    cursor: usize,
}

impl Head {
    fn leading(&self) -> Option<i64> {
        (!self.lead.is_null(self.cursor)).then(|| self.lead.value(self.cursor))
    }
}

/// Keep evaluated keys until a leading-key tie requires compound comparison.
/// A single converter owns all row encodings, including dictionary state.
enum SortKeys {
    Columns(Vec<ArrayRef>),
    Rows(Rows),
}

impl SortKeys {
    fn rows(&mut self, converter: &RowConverter, encoded: &Count) -> Result<&Rows> {
        if let Self::Columns(columns) = self {
            let rows = converter.convert_columns(columns)?;
            encoded.add(rows.num_rows());
            *self = Self::Rows(rows);
        }
        match self {
            Self::Rows(rows) => Ok(rows),
            Self::Columns(_) => unreachable!("columns were encoded above"),
        }
    }
}

struct BoundedMergeStream {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    expr: LexOrdering,
    converter: RowConverter,
    primitive_lead: bool,
    schema: SchemaRef,
    bounds: Vec<Option<i64>>,
    inputs: Vec<Input>,
    /// Complete tournament tree; leaves follow partition order, preserving stable ties.
    winners: Vec<Option<usize>>,
    /// Inputs not yet opened, the next to open LAST.
    closed: Vec<usize>,
    /// Open inputs waiting for a batch; ready heads need no further polling.
    needs_head: Vec<usize>,
    read_ahead: usize,
    /// Batches the pending output rows point into.
    pool: Vec<RecordBatch>,
    picked: Vec<(usize, usize)>,
    remaining: Option<usize>,
    batch_size: usize,
    opened: Count,
    skipped: Count,
    encoded: Count,
    head_slots_checked: Count,
    pending_head_slots: usize,
    key_comparisons: Count,
    pending_key_comparisons: usize,
    baseline: BaselineMetrics,
}

impl BoundedMergeStream {
    fn open_next(&mut self) -> Result<bool> {
        let Some(p) = self.closed.pop() else { return Ok(false) };
        self.inputs[p] = Input::Open { stream: self.input.execute(p, Arc::clone(&self.context))?, head: None };
        self.needs_head.push(p);
        self.opened.add(1);
        Ok(true)
    }

    /// Does `value` come strictly before `bound` in output order? Strict, so every row
    /// sharing a timestamp with a closed input waits for it (keep-greatest dedup needs
    /// all versions of a key adjacent, and the key leads with the timestamp).
    fn precedes(&self, value: Option<i64>, bound: i64) -> bool {
        let options = self.expr.first().options;
        match value {
            None => options.nulls_first,
            Some(v) if options.descending => v > bound,
            Some(v) => v < bound,
        }
    }

    fn head(&mut self, slot_batch: RecordBatch) -> Result<Head> {
        let columns = self.expr.iter().map(|sort| sort.expr.evaluate(&slot_batch)?.into_array(slot_batch.num_rows())).collect::<Result<Vec<_>>>()?;
        let lead = cast(&columns[0], &DataType::Int64)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .cloned()
            .ok_or_else(|| datafusion::error::DataFusionError::Internal("BoundedMergeExec leading key is not integer-like".into()))?;
        let mut rows = SortKeys::Columns(columns);
        if !self.primitive_lead {
            rows.rows(&self.converter, &self.encoded)?;
        }
        self.pool.push(slot_batch);
        Ok(Head { slot: self.pool.len() - 1, rows, lead, cursor: 0 })
    }

    fn compare_heads(&mut self, a: usize, b: usize) -> Result<Ordering> {
        self.pending_key_comparisons += 1;
        // Winners of the left subtree always precede those of the right subtree.
        let (before, after) = self.inputs.split_at_mut(b);
        let Input::Open { head: Some(a), .. } = &mut before[a] else { unreachable!("selection requires a head") };
        let Input::Open { head: Some(b), .. } = &mut after[0] else { unreachable!("selection requires a head") };
        if self.primitive_lead {
            let options = self.expr.first().options;
            let order = match (a.leading(), b.leading()) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => {
                    if options.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (Some(_), None) => {
                    if options.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                (Some(a), Some(b)) => {
                    if options.descending {
                        b.cmp(&a)
                    } else {
                        a.cmp(&b)
                    }
                }
            };
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(a.rows.rows(&self.converter, &self.encoded)?.row(a.cursor).cmp(&b.rows.rows(&self.converter, &self.encoded)?.row(b.cursor)))
    }

    fn update_winner(&mut self, p: usize) -> Result<()> {
        let mut node = self.winners.len() / 2 + p;
        self.winners[node] = matches!(self.inputs[p], Input::Open { head: Some(_), .. }).then_some(p);
        while node > 1 {
            node /= 2;
            self.winners[node] = match (self.winners[2 * node], self.winners[2 * node + 1]) {
                (Some(a), Some(b)) => Some(if self.compare_heads(a, b)? != Ordering::Greater { a } else { b }),
                (a, b) => a.or(b),
            };
            // Recompute every ancestor even if the winner's partition did not change:
            // advancing its cursor changed the key it contributes to the next match.
        }
        Ok(())
    }

    /// Build the output from the picked rows, then keep only the batches heads still point into.
    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.picked.is_empty() {
            return Ok(None);
        }
        let pool: Vec<&RecordBatch> = self.pool.iter().collect();
        let batch = interleave_record_batch(&pool, &self.picked)?;
        self.picked.clear();
        let mut kept = Vec::new();
        for input in &mut self.inputs {
            if let Input::Open { head: Some(head), .. } = input {
                kept.push(self.pool[head.slot].clone());
                head.slot = kept.len() - 1;
            }
        }
        self.pool = kept;
        Ok(Some(batch))
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            if self.remaining == Some(0) {
                return Poll::Ready(self.flush().transpose());
            }
            // Every open input needs a head before any row can be ordered.
            let mut pending = false;
            self.pending_head_slots = self.pending_head_slots.saturating_add(self.needs_head.len());
            for index in 0..self.needs_head.len() {
                let p = self.needs_head[index];
                while let Input::Open { stream, head: None } = &mut self.inputs[p] {
                    match stream.poll_next_unpin(cx) {
                        Poll::Pending => {
                            pending = true;
                            break;
                        }
                        Poll::Ready(None) => self.inputs[p] = Input::Done,
                        Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                        Poll::Ready(Some(Ok(batch))) if batch.num_rows() == 0 => {}
                        Poll::Ready(Some(Ok(batch))) => {
                            let new_head = match self.head(batch) {
                                Ok(head) => head,
                                Err(error) => return Poll::Ready(Some(Err(error))),
                            };
                            if let Input::Open { head, .. } = &mut self.inputs[p] {
                                *head = Some(new_head);
                            }
                            if let Err(error) = self.update_winner(p) {
                                return Poll::Ready(Some(Err(error)));
                            }
                        }
                    }
                }
            }
            self.needs_head.retain(|&p| matches!(self.inputs[p], Input::Open { head: None, .. }));
            if pending {
                return match self.flush() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => Poll::Pending,
                    Err(error) => Poll::Ready(Some(Err(error))),
                };
            }
            let best = self.winners[1].map(|p| {
                let Input::Open { head: Some(head), .. } = &self.inputs[p] else { unreachable!("selected input has a head") };
                (p, head.leading())
            });
            let Some((p, lead)) = best else {
                // Every open input ran dry: widen the window so a needle keeps its fan-out.
                let mut opened = false;
                for _ in 0..self.read_ahead {
                    opened |= match self.open_next() {
                        Ok(opened) => opened,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                }
                self.read_ahead = self.read_ahead.saturating_mul(2);
                if opened {
                    continue;
                }
                return Poll::Ready(self.flush().transpose());
            };
            // A closed input whose bound the next row does not strictly precede may hold
            // that row or one before it: open it, and with it the read-ahead window.
            if self.closed.last().is_some_and(|&next| self.bounds[next].is_none_or(|bound| !self.precedes(lead, bound))) {
                for _ in 0..self.read_ahead {
                    if let Err(error) = self.open_next() {
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                continue;
            }
            let Input::Open { head: Some(head), .. } = &mut self.inputs[p] else { unreachable!("best is an open input with a head") };
            self.picked.push((head.slot, head.cursor));
            head.cursor += 1;
            if head.cursor == head.lead.len()
                && let Input::Open { head, .. } = &mut self.inputs[p]
            {
                *head = None;
                self.needs_head.push(p);
            }
            if let Err(error) = self.update_winner(p) {
                return Poll::Ready(Some(Err(error)));
            }
            self.remaining = self.remaining.map(|n| n - 1);
            if self.picked.len() >= self.batch_size {
                return Poll::Ready(self.flush().transpose());
            }
        }
    }
}

impl Stream for BoundedMergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        let checked = std::mem::take(&mut self.pending_head_slots);
        self.head_slots_checked.add(checked);
        let comparisons = std::mem::take(&mut self.pending_key_comparisons);
        self.key_comparisons.add(comparisons);
        self.baseline.record_poll(poll)
    }
}

/// Counted at drop: a limit above stops polling long before the end of the stream.
impl Drop for BoundedMergeStream {
    fn drop(&mut self) {
        self.skipped.add(self.closed.len());
        metrics::counter!("timefusion_bounded_merge_inputs_skipped_total").increment(self.closed.len() as u64);
    }
}

impl RecordBatchStream for BoundedMergeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// The first leading-key value each output partition of `plan` can yield, in output
/// order, from file statistics. `None` where nothing proves a bound.
pub(crate) fn partition_bounds(plan: &Arc<dyn ExecutionPlan>, column: &str, descending: bool) -> Vec<Option<i64>> {
    let partitions = plan.properties().partitioning.partition_count();
    let unknown = || vec![None; partitions];
    let children = plan.children();
    if let Some(source) = downcast::<DataSourceExec>(plan.as_ref())
        && let Some(config) = downcast::<FileScanConfig>(source.data_source().as_ref())
    {
        let schema = config.file_schema();
        let Ok(index) = schema.index_of(column) else { return unknown() };
        let nullable = schema.field(index).is_nullable();
        let file_bound = |file: &datafusion::datasource::listing::PartitionedFile| {
            let stats = file.statistics.as_ref()?.column_statistics.get(index)?;
            if nullable && stats.null_count.get_value() != Some(&0) {
                return None;
            }
            let value = if descending { &stats.max_value } else { &stats.min_value };
            let micros = match value {
                Precision::Exact(v) | Precision::Inexact(v) => scalar_micros(v)?,
                Precision::Absent => return None,
            };
            Some(if descending { micros.saturating_add(STATS_SLACK_MICROS) } else { micros.saturating_sub(STATS_SLACK_MICROS) })
        };
        let groups: Vec<Option<i64>> = config
            .file_groups
            .iter()
            .map(|group| group.files().iter().map(file_bound).collect::<Option<Vec<_>>>().and_then(|bounds| fold_bound(bounds.into_iter(), descending)))
            .collect();
        return if groups.len() == partitions { groups } else { unknown() };
    }
    if downcast::<UnionExec>(plan.as_ref()).is_some() {
        let bounds: Vec<Option<i64>> = children.iter().flat_map(|child| partition_bounds(child, column, descending)).collect();
        return if bounds.len() == partitions { bounds } else { unknown() };
    }
    let [child] = children.as_slice() else { return unknown() };
    // A projection must hand the column through unchanged (a cast keeps its values).
    if let Some(projection) = downcast::<ProjectionExec>(plan.as_ref())
        && !projection.expr().iter().any(|e| e.alias == column && passthrough_column(&e.expr) == Some(column))
    {
        return unknown();
    }
    let child_bounds = partition_bounds(child, column, descending);
    match plan.name() {
        // Row-removing, partition-preserving operators: a child's bound still holds.
        "ProjectionExec"
        | "FilterExec"
        | "CoalesceBatchesExec"
        | "CompactBatchesExec"
        | "GatedScanExec"
        | "DeltaScanExec"
        | "SortExec"
        | "OrderingProbeExec"
            if child_bounds.len() == partitions =>
        {
            child_bounds
        }
        // Merges into one partition: the extreme of the inputs' bounds.
        "SortPreservingMergeExec" | "BoundedMergeExec" | "DedupExec" | "CoalescePartitionsExec" if partitions == 1 => {
            vec![child_bounds.into_iter().collect::<Option<Vec<_>>>().and_then(|bounds| fold_bound(bounds.into_iter(), descending))]
        }
        _ => unknown(),
    }
}

fn fold_bound(bounds: impl Iterator<Item = i64>, descending: bool) -> Option<i64> {
    if descending { bounds.max() } else { bounds.min() }
}

fn passthrough_column(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    downcast::<Column>(expr.as_ref())
        .map(Column::name)
        .or_else(|| downcast::<datafusion::physical_expr::expressions::CastExpr>(expr.as_ref()).and_then(|cast| passthrough_column(cast.expr())))
}

fn scalar_micros(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::TimestampMicrosecond(Some(v), _) | ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.checked_mul(1_000),
        ScalarValue::TimestampSecond(Some(v), _) => v.checked_mul(1_000_000),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(v.div_euclid(1_000)),
        _ => None,
    }
}

/// Replaces the order-preserving merge beneath a fetch with a [`BoundedMergeExec`] when
/// file statistics bound at least two of its inputs. The fetch may sit above
/// `DedupExec`: the merge itself still emits every row, in the same order, so dedup sees
/// exactly what it saw before.
#[derive(Debug, Default)]
pub struct BoundedMergeForFetch;

impl PhysicalOptimizerRule for BoundedMergeForFetch {
    fn name(&self) -> &str {
        "bounded_merge_for_fetch"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| match node.fetch() {
            Some(fetch) => bound_merge_below(&node, fetch).map(|rewritten| rewritten.map_or_else(|| Transformed::no(node), Transformed::yes)),
            None => Ok(Transformed::no(node)),
        })
        .data()
    }
}

/// The node with every order-preserving merge beneath it bounded, walking down operators
/// that pass rows through in order and into each branch of a union (the certified-day
/// legs are unioned above the deduped one, each with its own merge).
fn bound_merge_below(node: &Arc<dyn ExecutionPlan>, fetch: usize) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let children = node.children();
    let descends = downcast::<UnionExec>(node.as_ref()).is_some()
        || downcast::<SortPreservingMergeExec>(node.as_ref()).is_some()
        || (children.len() == 1
            && node.maintains_input_order().first() == Some(&true)
            && matches!(
                node.name(),
                "ProjectionExec"
                    | "FilterExec"
                    | "DedupExec"
                    | "CompactBatchesExec"
                    | "CoalesceBatchesExec"
                    | "GlobalLimitExec"
                    | "LocalLimitExec"
                    | "AdmissionExec"
            ));
    if !descends {
        return Ok(None);
    }
    let rewritten: Vec<Option<Arc<dyn ExecutionPlan>>> = children.iter().map(|child| bound_merge_below(child, fetch)).collect::<Result<_>>()?;
    let changed = rewritten.iter().any(Option::is_some);
    let node = match changed {
        true => datafusion::physical_plan::replace_children_if_necessary(
            Arc::clone(node),
            rewritten.into_iter().zip(&children).map(|(new, old)| new.unwrap_or_else(|| Arc::clone(old))).collect(),
        )?,
        false => Arc::clone(node),
    };
    let Some(merge) = downcast::<SortPreservingMergeExec>(node.as_ref()) else {
        return Ok(changed.then_some(node));
    };
    let lead = merge.expr().first();
    let unbounded = || Ok(changed.then(|| Arc::clone(&node)));
    let Some(column) = downcast::<Column>(lead.expr.as_ref()) else { return unbounded() };
    let input = merge.input();
    if !matches!(lead.expr.data_type(&input.schema())?, DataType::Int64 | DataType::Timestamp(..)) {
        return unbounded();
    }
    let bounds = partition_bounds(input, column.name(), lead.options.descending);
    if bounds.iter().flatten().count() < 2 {
        return unbounded();
    }
    metrics::counter!("timefusion_bounded_merge_planned_total").increment(1);
    Ok(Some(Arc::new(BoundedMergeExec::new(Arc::clone(input), merge.expr().clone(), bounds, merge.fetch()).with_batch_rows(Some(fetch)))))
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{AsArray, Int64Array},
            compute::SortOptions,
            datatypes::{Field, Int64Type, Schema},
        },
        datasource::memory::MemorySourceConfig,
        physical_expr::PhysicalSortExpr,
    };

    use super::*;

    /// Partitions of `(ts, id)` sorted `ts DESC, id ASC`, one batch per `chunk` rows.
    fn input(partitions: &[Vec<(i64, i64)>], chunk: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false), Field::new("id", DataType::Int64, false)]));
        let batches = partitions
            .iter()
            .map(|rows| {
                rows.chunks(chunk.max(1))
                    .map(|rows| {
                        let column = |f: fn(&(i64, i64)) -> i64| Arc::new(Int64Array::from(rows.iter().map(f).collect::<Vec<_>>())) as _;
                        RecordBatch::try_new(Arc::clone(&schema), vec![column(|r| r.0), column(|r| r.1)]).unwrap()
                    })
                    .collect()
            })
            .collect::<Vec<Vec<_>>>();
        MemorySourceConfig::try_new_exec(&batches, schema, None).unwrap()
    }

    fn ordering() -> LexOrdering {
        LexOrdering::new(
            [("ts", 0, true), ("id", 1, false)]
                .map(|(name, index, descending)| PhysicalSortExpr::new(Arc::new(Column::new(name, index)), SortOptions { descending, nulls_first: true })),
        )
        .unwrap()
    }

    async fn rows(plan: Arc<dyn ExecutionPlan>) -> Vec<(i64, i64)> {
        let context = Arc::new(TaskContext::default().with_session_config(datafusion::prelude::SessionConfig::new().with_batch_size(3)));
        datafusion::physical_plan::collect(plan, context)
            .await
            .unwrap()
            .iter()
            .flat_map(|b| {
                let column = |i: usize| b.column(i).as_primitive::<Int64Type>().values().to_vec();
                column(0).into_iter().zip(column(1))
            })
            .collect()
    }

    proptest::proptest! {
        /// Same rows in the same order as `SortPreservingMergeExec`, fetch or not: bounds
        /// only delay opening an input, so a tie on `ts` across inputs still merges whole.
        #[test]
        fn merges_exactly_like_sort_preserving_merge(
            partitions in proptest::collection::vec(proptest::collection::vec((0..12i64, 0..4i64), 0..12), 1..39),
            unbounded in proptest::collection::vec(proptest::bool::ANY, 39),
            chunk in 1..5usize,
            fetch in proptest::option::of(0..20usize),
        ) {
            let partitions: Vec<Vec<(i64, i64)>> = partitions.into_iter().map(|mut rows| {
                rows.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
                rows
            }).collect();
            // The true first value is the tightest honest bound; an empty input may claim anything.
            let bounds = partitions.iter().zip(&unbounded).map(|(rows, &none)| (!none).then(|| rows.first().map_or(-1, |r| r.0))).collect();
            let plan = input(&partitions, chunk);
            let runtime = tokio::runtime::Runtime::new().unwrap();
            let want = runtime.block_on(rows(Arc::new(SortPreservingMergeExec::new(ordering(), Arc::clone(&plan)).with_fetch(fetch))));
            let got = runtime.block_on(rows(Arc::new(BoundedMergeExec::new(plan, ordering(), bounds, fetch))));
            proptest::prop_assert_eq!(got, want);
        }
    }

    #[tokio::test]
    async fn ready_heads_are_not_rechecked_per_row() {
        let partitions: Vec<_> = (0..38).map(|p| (0..3).rev().map(|ts| (ts, p)).collect()).collect();
        let input = input(&partitions, 3);
        let want = rows(Arc::new(SortPreservingMergeExec::new(ordering(), Arc::clone(&input)))).await;
        let plan = Arc::new(BoundedMergeExec::new(input, ordering(), vec![None; partitions.len()], None));
        // Isolate ready-head work from Tokio's task-budget retries.
        assert_eq!(tokio::task::unconstrained(rows(Arc::clone(&plan) as _)).await, want);
        let checked = plan.metrics().unwrap().sum_by_name("head_slots_checked").unwrap().as_usize();
        assert_eq!(checked, 2 * partitions.len(), "check each input only for its batch and EOF");
        let comparisons = plan.metrics().unwrap().sum_by_name("key_comparisons").unwrap().as_usize();
        let height = partitions.len().next_power_of_two().ilog2() as usize;
        assert!(comparisons <= (want.len() + 2 * partitions.len()) * height, "selection must update a logarithmic path: {comparisons} comparisons");
    }

    #[test_case::test_case(false => Some(0); "unique leading keys")]
    #[test_case::test_case(true => Some(4); "ties retain full encoding")]
    #[tokio::test]
    async fn leading_keys_avoid_row_encoding(tied: bool) -> Option<usize> {
        let ts = |value| if tied { 9 } else { value };
        let input = input(&[vec![(ts(9), 0), (ts(5), 2)], vec![(ts(8), 1), (ts(4), 3)]], 2);
        let want = rows(Arc::new(SortPreservingMergeExec::new(ordering(), Arc::clone(&input)))).await;
        let plan = Arc::new(BoundedMergeExec::new(input, ordering(), vec![None; 2], None));
        assert_eq!(rows(Arc::clone(&plan) as _).await, want);
        plan.metrics().unwrap().sum_by_name("key_rows_encoded").map(|value| value.as_usize())
    }

    #[test_case::test_case(DataType::Int64, false; "integer and strings")]
    #[test_case::test_case(DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None), true; "timestamp and dictionaries")]
    #[test_case::test_case(DataType::Float64, true; "fractional fallback and dictionaries")]
    #[tokio::test]
    async fn nullable_compound_keys_preserve_order(data_type: DataType, dictionary: bool) {
        use datafusion::{
            arrow::array::{DictionaryArray, Float64Array, StringArray},
            physical_plan::sorts::sort::SortExec,
        };
        let lead: ArrayRef = if data_type == DataType::Float64 {
            Arc::new(Float64Array::from(vec![Some(-2.5), None, Some(3.5), Some(3.25)]))
        } else {
            cast(&Int64Array::from(vec![Some(i64::MIN + 1), None, Some(i64::MAX - 1), Some(0)]), &data_type).unwrap()
        };
        let batches: Vec<Vec<RecordBatch>> = [vec![Some("z"), Some("b"), None, Some("a")], vec![Some("a"), None, Some("z"), Some("b")]]
            .into_iter()
            .map(|ids| {
                let ids: ArrayRef = if dictionary {
                    Arc::new(ids.into_iter().collect::<DictionaryArray<datafusion::arrow::datatypes::Int32Type>>())
                } else {
                    Arc::new(StringArray::from(ids))
                };
                let schema = Arc::new(Schema::new(vec![Field::new("ts", data_type.clone(), true), Field::new("id", ids.data_type().clone(), true)]));
                vec![RecordBatch::try_new(schema, vec![lead.clone(), ids]).unwrap()]
            })
            .chain([Vec::new()])
            .collect();
        let schema = batches[0][0].schema();
        let values = |batches: Vec<RecordBatch>| {
            batches
                .into_iter()
                .flat_map(|batch| {
                    (0..batch.num_rows())
                        .map(|row| batch.columns().iter().map(|col| ScalarValue::try_from_array(col, row).unwrap()).collect::<Vec<_>>())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>()
        };
        for descending in [false, true] {
            for nulls_first in [false, true] {
                let ordering = LexOrdering::new([
                    PhysicalSortExpr::new(Arc::new(Column::new("ts", 0)), SortOptions { descending, nulls_first }),
                    PhysicalSortExpr::new(Arc::new(Column::new("id", 1)), SortOptions { descending: !descending, nulls_first }),
                ])
                .unwrap();
                let input = MemorySourceConfig::try_new_exec(&batches, schema.clone(), None).unwrap();
                let input: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering.clone(), input).with_preserve_partitioning(true));
                let want = Arc::new(SortPreservingMergeExec::new(ordering.clone(), input.clone()));
                let got = Arc::new(BoundedMergeExec::new(input, ordering, vec![None; 3], None));
                let context = Arc::new(TaskContext::default());
                let want = datafusion::physical_plan::collect(want, context.clone()).await.unwrap();
                let actual = datafusion::physical_plan::collect(got.clone(), context).await.unwrap();
                assert_eq!(values(actual), values(want), "{data_type:?}, descending={descending}, nulls_first={nulls_first}");
                assert_eq!(got.metrics().unwrap().sum_by_name("key_rows_encoded").map(|v| v.as_usize()), Some(8), "ties encode each batch once");
            }
        }
    }

    /// The point of the operator: a latest-3 over six hourly inputs reads only the newest.
    #[tokio::test]
    async fn a_fetch_never_opens_inputs_that_cannot_contribute() {
        let partitions: Vec<Vec<(i64, i64)>> = (0..6).map(|hour| (0..5).map(|i| (hour * 100 + 50 - i, i)).collect()).collect();
        let bounds = partitions.iter().map(|rows| Some(rows[0].0)).collect();
        let merge = Arc::new(BoundedMergeExec::new(input(&partitions, 2), ordering(), bounds, Some(3)));
        assert_eq!(rows(Arc::clone(&merge) as _).await, vec![(550, 0), (549, 1), (548, 2)]);
        let metrics = merge.metrics().unwrap();
        let opened = metrics.sum_by_name("inputs_opened").map(|m| m.as_usize());
        assert_eq!(opened, Some(INITIAL_READ_AHEAD), "only the read-ahead window may open; metrics: {metrics}");
    }

    #[test_case::test_case(None => vec![0, 1]; "all rows")]
    #[test_case::test_case(Some(1) => vec![0]; "fetch")]
    #[tokio::test]
    async fn equal_keys_keep_partition_order_when_bounds_open_the_later_partition_first(fetch: Option<usize>) -> Vec<i64> {
        let schema = Arc::new(Schema::new(["ts", "id", "payload"].map(|name| Field::new(name, DataType::Int64, false)).to_vec()));
        let batches = (0..2)
            .map(|partition| {
                vec![RecordBatch::try_new(Arc::clone(&schema), [50, 0, partition].map(|value| Arc::new(Int64Array::from(vec![value])) as _).to_vec()).unwrap()]
            })
            .collect::<Vec<_>>();
        let input = MemorySourceConfig::try_new_exec(&batches, schema, None).unwrap();
        let plan = Arc::new(BoundedMergeExec::new(input, ordering(), vec![Some(50), Some(51)], fetch));
        datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default()))
            .await
            .unwrap()
            .iter()
            .flat_map(|batch| batch.column(2).as_primitive::<Int64Type>().values().to_vec())
            .collect()
    }
}

/// Oldest-first latest-N (`ORDER BY ts ASC LIMIT n`) over files sorted newest-first decodes
/// every file whole to reach its oldest rows. When file statistics name the oldest files
/// that already hold enough rows, the same query is planned again with `ts <= bound` (their
/// latest timestamp) so the Delta provider skips the rest through its own file selection,
/// which keeps deletion-vector scans consistent; nothing edits a planned scan. A full page
/// from that narrowed query is exact: it holds every row at or before its last timestamp,
/// ties included. A short page falls back to the full plan.
pub(crate) async fn narrow_ascending_fetch(
    logical: &datafusion::logical_expr::LogicalPlan, full: Arc<dyn ExecutionPlan>, state: &datafusion::execution::SessionState,
    planner: &datafusion::physical_planner::DefaultPhysicalPlanner,
) -> Result<Arc<dyn ExecutionPlan>> {
    use datafusion::physical_planner::PhysicalPlanner;
    let Some((page, column, scan)) = latest_page(logical, true) else { return Ok(full) };
    if full.properties().partitioning.partition_count() != 1 {
        return Ok(full);
    }
    // From the Delta log, not the planned scan: per-file statistics reach a deletion-vector
    // scan's physical plan only partially, and the add actions are what the provider prunes on.
    let Some(table) = datafusion::datasource::source_as_provider(&scan.source).ok() else { return Ok(full) };
    let Some(table) = downcast::<crate::database::ProjectRoutingTable>(&*table) else { return Ok(full) };
    let Some(mut files) = table.file_spans(&scan.filters, &column.name).await.filter(|f| !f.is_empty()) else { return Ok(full) };
    files.sort_by_key(|&(_, max, _)| max);
    let mut held = 0usize;
    let Some(&(_, max, _)) = files.iter().find(|&&(_, _, rows)| {
        held = held.saturating_add(rows);
        held >= page.rows.saturating_mul(NARROW_ROW_MARGIN)
    }) else {
        return Ok(full);
    };
    let bound = max.saturating_add(STATS_SLACK_MICROS);
    let skipped = files.iter().filter(|&&(min, _, _)| min > bound).count();
    if skipped == 0 {
        return Ok(full);
    }
    let narrowed = state.optimize(&with_bound(logical, &column, bound, true)?)?;
    let narrow = planner.create_physical_plan(&narrowed, state).await?;
    if narrow.schema() != full.schema() {
        return Ok(full);
    }
    metrics::counter!("timefusion_narrowed_fetch_planned_total").increment(1);
    Ok(Arc::new(NarrowedFetchExec {
        narrow,
        full: Fallback::Planned(full),
        rows: page.rows,
        total_files: files.len(),
        skipped,
        metrics: ExecutionPlanMetricsSet::new(),
    }))
}

/// Newest-first latest-N (`ORDER BY ts DESC LIMIT n`) under an indexed predicate reads every
/// match in the window to keep the newest `n`: a 30-day list of a few hundred errors decoded a
/// whole day of matches. The search index names the timestamp of the newest matches and the
/// query is planned with `ts >= bound`. Exact for the same reason as the ascending case (a full
/// page holds every row at or after its last timestamp), so the bound is only a heuristic: a
/// short page plans and runs the full query. Planned FIRST, because planning the full query is
/// itself the cost — its prefilter resolves every match in the window (prod: 463 MB of index
/// reads for 1,818 errors over 6 days).
pub(crate) async fn narrow_descending_fetch(
    logical: &datafusion::logical_expr::LogicalPlan, state: &datafusion::execution::SessionState,
    planner: &datafusion::physical_planner::DefaultPhysicalPlanner,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    use datafusion::physical_planner::PhysicalPlanner;
    let Some((page, column, scan)) = latest_page(logical, false) else { return Ok(None) };
    let Some(table) = datafusion::datasource::source_as_provider(&scan.source).ok() else { return Ok(None) };
    let Some(table) = downcast::<crate::database::ProjectRoutingTable>(&*table) else { return Ok(None) };
    let Some((lo, _)) = crate::database::filters_time_range(&scan.filters) else { return Ok(None) };
    let Some(bound) = table.newest_match_bound(&scan.filters, page.rows.saturating_mul(NARROW_ROW_MARGIN)).await.filter(|bound| *bound > lo) else {
        return Ok(None);
    };
    let narrowed = state.optimize(&with_bound(logical, &column, bound, false)?)?;
    let narrow = planner.create_physical_plan(&narrowed, state).await?;
    if narrow.properties().partitioning.partition_count() != 1 {
        return Ok(None);
    }
    metrics::counter!("timefusion_narrowed_desc_fetch_planned_total").increment(1);
    let full = Fallback::Deferred(Box::new((logical.clone(), state.clone())));
    Ok(Some(Arc::new(NarrowedFetchExec { narrow, full, rows: page.rows, total_files: 0, skipped: 0, metrics: ExecutionPlanMetricsSet::new() })))
}

struct AscendingPage {
    /// Rows the whole query returns when the window holds enough of them.
    rows: usize,
}

/// `[Projection|Limit|SubqueryAlias]* -> Sort(ts ASC|DESC, fetch) -> [Projection|Filter|SubqueryAlias]* -> TableScan`.
/// Anything that could pick or drop rows by something other than the sort key (aggregates,
/// DISTINCT ON, windows, joins, inner limits) disqualifies the plan: a bound on `ts`
/// below it could change which rows it keeps.
fn latest_page(
    plan: &datafusion::logical_expr::LogicalPlan, ascending: bool,
) -> Option<(AscendingPage, datafusion::common::Column, &datafusion::logical_expr::TableScan)> {
    use datafusion::logical_expr::{Expr, LogicalPlan};
    let (mut node, mut skip, mut limit) = (plan, 0usize, None::<usize>);
    let sort = loop {
        match node {
            LogicalPlan::Projection(p) => node = &p.input,
            LogicalPlan::SubqueryAlias(a) => node = &a.input,
            LogicalPlan::Limit(l) => {
                skip += match l.get_skip_type().ok()? {
                    datafusion::logical_expr::SkipType::Literal(s) => s,
                    _ => return None,
                };
                limit = match l.get_fetch_type().ok()? {
                    datafusion::logical_expr::FetchType::Literal(f) => f,
                    _ => return None,
                };
                node = &l.input;
            }
            LogicalPlan::Sort(s) => break s,
            _ => return None,
        }
    };
    let lead = sort.expr.first()?;
    let Expr::Column(column) = &lead.expr else { return None };
    if lead.asc != ascending {
        return None;
    }
    let fetch = sort.fetch?;
    let mut below = sort.input.as_ref();
    let scan = loop {
        match below {
            LogicalPlan::Projection(p) => below = &p.input,
            LogicalPlan::Filter(f) => below = &f.input,
            LogicalPlan::SubqueryAlias(a) => below = &a.input,
            LogicalPlan::TableScan(scan) if scan.fetch.is_none() && scan.projected_schema.has_column_with_unqualified_name(&column.name) => break scan,
            _ => return None,
        }
    };
    let rows = fetch.saturating_sub(skip).min(limit.unwrap_or(usize::MAX));
    (rows > 0).then(|| (AscendingPage { rows }, column.clone(), scan))
}

/// `plan` with `column <= bound` (`upper`) or `column >= bound` above its table scan, typed
/// like the scanned column.
fn with_bound(
    plan: &datafusion::logical_expr::LogicalPlan, column: &datafusion::common::Column, bound: i64, upper: bool,
) -> Result<datafusion::logical_expr::LogicalPlan> {
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
    plan.clone()
        .transform_up(|node| {
            let LogicalPlan::TableScan(scan) = &node else { return Ok(Transformed::no(node)) };
            let field = scan.projected_schema.field_with_unqualified_name(&column.name)?;
            let value = ScalarValue::TimestampMicrosecond(Some(bound), None).cast_to(field.data_type())?;
            let scanned = datafusion::common::Column::new(Some(scan.table_name.clone()), column.name.clone());
            let bounded = if upper { col(scanned).lt_eq(lit(value)) } else { col(scanned).gt_eq(lit(value)) };
            Ok(Transformed::yes(LogicalPlanBuilder::from(node.clone()).filter(bounded)?.build()?))
        })
        .data()
}

/// The oldest files must hold this many times the page, so filters, the window and dedup
/// rarely leave the narrowed page short.
const NARROW_ROW_MARGIN: usize = 4;

/// The full query a short narrowed page falls back to: already planned, or planned only then.
#[derive(Debug, Clone)]
enum Fallback {
    Planned(Arc<dyn ExecutionPlan>),
    Deferred(Box<(datafusion::logical_expr::LogicalPlan, datafusion::execution::SessionState)>),
}

impl Fallback {
    async fn plan(self) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion::physical_planner::PhysicalPlanner;
        match self {
            Self::Planned(plan) => Ok(plan),
            Self::Deferred(deferred) => {
                let (logical, state) = *deferred;
                datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![]).create_physical_plan(&logical, &state).await
            }
        }
    }
}

/// Runs `narrow` and returns it when it fills the page; otherwise runs `full`.
#[derive(Debug)]
struct NarrowedFetchExec {
    narrow: Arc<dyn ExecutionPlan>,
    full: Fallback,
    rows: usize,
    total_files: usize,
    skipped: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for NarrowedFetchExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NarrowedFetchExec: rows={}, files={}, skipped={}", self.rows, self.total_files, self.skipped)
    }
}

impl ExecutionPlan for NarrowedFetchExec {
    fn name(&self) -> &'static str {
        "NarrowedFetchExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.narrow.properties()
    }
    fn apply_expressions(&self, _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.full {
            Fallback::Planned(full) => vec![&self.narrow, full],
            Fallback::Deferred(_) => vec![&self.narrow],
        }
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let (narrow, full) = match (children.as_slice(), &self.full) {
            ([narrow, full], Fallback::Planned(_)) => (Arc::clone(narrow), Fallback::Planned(Arc::clone(full))),
            ([narrow], deferred @ Fallback::Deferred(_)) => (Arc::clone(narrow), deferred.clone()),
            _ => return Err(datafusion::error::DataFusionError::Internal("NarrowedFetchExec children do not match its fallback".into())),
        };
        Ok(Arc::new(Self { narrow, full, rows: self.rows, total_files: self.total_files, skipped: self.skipped, metrics: ExecutionPlanMetricsSet::new() }))
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        use futures::{StreamExt, TryStreamExt};
        let (narrow, full, rows, schema) = (Arc::clone(&self.narrow), self.full.clone(), self.rows, self.narrow.schema());
        let fell_back = MetricBuilder::new(&self.metrics).counter("fell_back", partition);
        MetricBuilder::new(&self.metrics).counter("files_skipped", partition).add(self.skipped);
        let stream = futures::stream::once(async move {
            let batches: Vec<RecordBatch> = narrow.execute(partition, Arc::clone(&context))?.try_collect().await?;
            if batches.iter().map(RecordBatch::num_rows).sum::<usize>() >= rows {
                return Ok::<_, datafusion::error::DataFusionError>(futures::stream::iter(batches.into_iter().map(Ok)).boxed());
            }
            fell_back.add(1);
            let full = full.plan().await?;
            if full.schema() != narrow.schema() {
                return Err(datafusion::error::DataFusionError::Internal("narrowed and full plans disagree on schema".into()));
            }
            Ok(full.execute(partition, context)?.boxed())
        })
        .try_flatten();
        Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream)))
    }
}
