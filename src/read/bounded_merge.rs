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
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use datafusion::{
    arrow::{
        array::{Array, Int64Array, RecordBatch},
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
        let mut stream = BoundedMergeStream {
            input: Arc::clone(&self.input),
            context: Arc::clone(&context),
            expr: self.expr.clone(),
            converter: RowConverter::new(fields)?,
            schema,
            bounds: self.bounds.clone(),
            inputs: (0..self.bounds.len()).map(|_| Input::Closed).collect(),
            closed,
            read_ahead: INITIAL_READ_AHEAD,
            pool: Vec::new(),
            picked: Vec::new(),
            remaining: self.fetch,
            batch_size: self.batch_rows.map_or(context.session_config().batch_size(), |rows| rows.clamp(1, context.session_config().batch_size())),
            opened: MetricBuilder::new(&self.metrics).counter("inputs_opened", partition),
            skipped: MetricBuilder::new(&self.metrics).counter("inputs_never_opened", partition),
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
    rows: Rows,
    lead: Int64Array,
    cursor: usize,
}

struct BoundedMergeStream {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    expr: LexOrdering,
    converter: RowConverter,
    schema: SchemaRef,
    bounds: Vec<Option<i64>>,
    inputs: Vec<Input>,
    /// Inputs not yet opened, the next to open LAST.
    closed: Vec<usize>,
    read_ahead: usize,
    /// Batches the pending output rows point into.
    pool: Vec<RecordBatch>,
    picked: Vec<(usize, usize)>,
    remaining: Option<usize>,
    batch_size: usize,
    opened: Count,
    skipped: Count,
    baseline: BaselineMetrics,
}

impl BoundedMergeStream {
    fn open_next(&mut self) -> Result<bool> {
        let Some(p) = self.closed.pop() else { return Ok(false) };
        self.inputs[p] = Input::Open { stream: self.input.execute(p, Arc::clone(&self.context))?, head: None };
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
        let rows = self.converter.convert_columns(&columns)?;
        let lead = cast(&columns[0], &DataType::Int64)?
            .as_any()
            .downcast_ref::<Int64Array>()
            .cloned()
            .ok_or_else(|| datafusion::error::DataFusionError::Internal("BoundedMergeExec leading key is not integer-like".into()))?;
        self.pool.push(slot_batch);
        Ok(Head { slot: self.pool.len() - 1, rows, lead, cursor: 0 })
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
            for p in 0..self.inputs.len() {
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
                        }
                    }
                }
            }
            if pending {
                return match self.flush() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => Poll::Pending,
                    Err(error) => Poll::Ready(Some(Err(error))),
                };
            }
            let best = (0..self.inputs.len())
                .filter_map(|p| match &self.inputs[p] {
                    Input::Open { head: Some(head), .. } => Some((p, head)),
                    _ => None,
                })
                .min_by(|(_, a), (_, b)| a.rows.row(a.cursor).cmp(&b.rows.row(b.cursor)))
                .map(|(p, head)| (p, (!head.lead.is_null(head.cursor)).then(|| head.lead.value(head.cursor))));
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
            if head.cursor == head.rows.num_rows()
                && let Input::Open { head, .. } = &mut self.inputs[p]
            {
                *head = None;
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
            partitions in proptest::collection::vec(proptest::collection::vec((0..12i64, 0..4i64), 0..12), 1..7),
            unbounded in proptest::collection::vec(proptest::bool::ANY, 7),
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
}
