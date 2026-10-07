//! Heavy-query admission control.
//!
//! Connections are cheap and TimeFusion caps none of them — the pgwire endpoint
//! accepts up to the proxy's `maxconn 10000`, and the historical "8" is
//! monoscope's own client-pool throttle, not a server limit. What a fixed memory
//! pool cannot do is run many *concurrent unbounded sorts*: each takes
//! `partitions x` a non-spillable merge reservation, so a 22 GB pool physically
//! holds only ~20 of them before `Resources exhausted`. The warehouse answer
//! (Redshift WLM, Snowflake queuing) is to accept every connection and admit K
//! heavy queries, queuing the rest with a bounded wait.
//!
//! This is that gate. A pgwire-only physical rule wraps a plan that contains a
//! spilling `SortExec`, or a multi-partition ordered merge feeding merge-on-read
//! dedup, in [`AdmissionExec`], which holds its weighted permits for the stream's lifetime.
//! The latter buffers one decoded batch per input partition and can consume close
//! to a GiB before a small outer `LIMIT` emits anything, but only for wide rows:
//! a merge is gated when its estimated buffer exceeds one sort reservation. Cheap queries —
//! rollup-routed aggregates, point lookups, bounded `TopK`, and one-partition
//! ordered scans — are never gated, so the read hit rate is untouched. The
//! scan-level [`crate::database::scan::GatedScanExec`] permits
//! are a *different* semaphore released per batch, so a query holding an
//! admission permit while its scans make progress can never deadlock: admission
//! is acquired once at the root before any scan permit, and scan permits never
//! wait on admission.

use std::{
    fmt,
    sync::{Arc, OnceLock},
};

use datafusion::{
    arrow::datatypes::{DataType, Schema},
    error::{DataFusionError, Result as DFResult},
    execution::TaskContext,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        coalesce_partitions::CoalescePartitionsExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        stream::RecordBatchStreamAdapter,
    },
};
use tokio::sync::OwnedSemaphorePermit;

use crate::{
    database::scan_metric_names,
    read::{DedupExec, optimizers::downcast},
};

/// How long a queued heavy query waits for a permit before failing with an
/// orderly error rather than blocking a client forever.
const HEAVY_QUEUE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// A permit held longer than this is logged from inside its own query span when
/// released, completed or cancelled alike. Statement logs stop timing at the
/// first batch, so without this the queries that fill the gate are invisible.
const HEAVY_HELD_LOG_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

/// The process-wide heavy-sort permit pool, sized once at first use from the
/// query-pool geometry.
struct HeavyGate {
    semaphore: Arc<tokio::sync::Semaphore>,
    partitions: usize,
    slots: usize,
}

static HEAVY_GATE: OnceLock<HeavyGate> = OnceLock::new();

fn heavy_gate() -> &'static HeavyGate {
    HEAVY_GATE.get_or_init(|| {
        let (partitions, slots) = crate::config::try_config().map_or((1, 8), |cfg| {
            let partitions = match cfg.memory.timefusion_query_partitions {
                0 => cfg.derived.cores(),
                n => n,
            };
            (partitions, crate::config::max_concurrent_heavy_sorts(partitions, cfg.derived.query_pool_bytes()))
        });
        HeavyGate { semaphore: Arc::new(tokio::sync::Semaphore::new(slots)), partitions, slots }
    })
}

pub(crate) fn heavy_sem() -> &'static Arc<tokio::sync::Semaphore> {
    &heavy_gate().semaphore
}

/// Does the plan contain a SPILLING sort — one with no `fetch` bound?
///
/// A `SortExec` carrying `fetch = Some(n)` is a `TopK`: it holds only `n` rows and
/// never spills, so it is not the shape that exhausts the pool. An unbounded sort
/// (merge-on-read dedup, `ORDER BY` without a small `LIMIT`) is, and is exactly
/// what the raw fallback plans on the log-explorer and session queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HeavyClass {
    SpillingSort,
    OrderedMorMerge { fan_in: usize, buffered_bytes: usize },
}

impl HeavyClass {
    fn label(self) -> &'static str {
        match self {
            Self::SpillingSort => "sort",
            Self::OrderedMorMerge { .. } => "ordered_mor_merge",
        }
    }
}

fn ordered_merge_fan_in(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    let here =
        downcast::<SortPreservingMergeExec>(plan.as_ref()).map(|merge| merge.input().properties().partitioning.partition_count()).filter(|&fan_in| fan_in > 1);
    plan.children().into_iter().filter_map(ordered_merge_fan_in).chain(here).max()
}

/// Decoded bytes per row, biased UP: an underestimate would un-gate a wide merge,
/// an overestimate only keeps a narrow one waiting.
fn estimated_row_bytes(schema: &Schema) -> usize {
    schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Boolean => 1,
            t if t.is_nested() => 1024,
            t => t.primitive_width().unwrap_or(64),
        })
        .sum()
}

/// Classify only shapes whose per-query memory is large enough to share the
/// heavy-query gate. An ordered merge is heavy only below an order-dependent
/// `DedupExec`, and only when one batch per input exceeds a sort reservation;
/// incidental merges elsewhere keep their existing concurrency.
fn heavy_node_class(plan: &Arc<dyn ExecutionPlan>, batch_rows: usize) -> Option<HeavyClass> {
    // A sort of an aggregate's GROUPS is no bigger than the aggregate, which the pool
    // already accounts and spills; gating it queued every dashboard chart's final
    // `ORDER BY time_bucket(..)` (~100 rows) behind long scans (10-02: 1h charts waited
    // the full 30 s for a permit). The search continues below for a wide merge.
    if downcast::<SortExec>(plan.as_ref()).is_some_and(|sort| sort.fetch().is_none() && !sorts_groups(sort.input())) {
        return Some(HeavyClass::SpillingSort);
    }
    if let Some(dedup) = downcast::<DedupExec>(plan.as_ref())
        && dedup.required_ordering().is_some()
        && let Some(fan_in) = plan.children().into_iter().filter_map(ordered_merge_fan_in).max()
    {
        let buffered_bytes = fan_in * batch_rows * estimated_row_bytes(&plan.children()[0].schema());
        return (buffered_bytes > crate::config::DEFAULT_SORT_SPILL_RESERVATION_BYTES).then_some(HeavyClass::OrderedMorMerge { fan_in, buffered_bytes });
    }
    None
}

fn heavy_class(plan: &Arc<dyn ExecutionPlan>, batch_rows: usize) -> Option<HeavyClass> {
    heavy_node_class(plan, batch_rows).or_else(|| plan.children().into_iter().find_map(|child| heavy_class(child, batch_rows)))
}

/// Charge the actual operators rather than assuming every query contains one
/// configured-width sort. Parent and child sorts may overlap while streaming;
/// account both. The pool share leaves the other half available for working rows.
fn heavy_permits(plan: &Arc<dyn ExecutionPlan>, batch_rows: usize, partitions: usize, slots: usize) -> usize {
    fn cost(plan: &Arc<dyn ExecutionPlan>, batch_rows: usize) -> usize {
        let here = match heavy_node_class(plan, batch_rows) {
            Some(HeavyClass::SpillingSort) => {
                let sort = downcast::<SortExec>(plan.as_ref()).unwrap();
                let batch_bytes = batch_rows.saturating_mul(estimated_row_bytes(&sort.input().schema()));
                crate::config::DEFAULT_SORT_SPILL_RESERVATION_BYTES.max(batch_bytes).saturating_mul(sort.input().properties().partitioning.partition_count())
            }
            Some(HeavyClass::OrderedMorMerge { buffered_bytes, .. }) => buffered_bytes,
            None => 0,
        };
        plan.children().into_iter().fold(here, |total, child| total.saturating_add(cost(child, batch_rows)))
    }
    let unit = crate::config::DEFAULT_SORT_SPILL_RESERVATION_BYTES.saturating_mul(partitions.max(1));
    // An oversized query takes the whole gate, rather than waiting forever for
    // more permits than exist. DataFusion's pool still bounds its allocations.
    cost(plan, batch_rows).div_ceil(unit).max(1).min(slots)
}

/// Whether `plan` is an aggregate's output, through row-preserving wrappers.
fn sorts_groups(plan: &Arc<dyn ExecutionPlan>) -> bool {
    use datafusion::physical_plan::{aggregates::AggregateExec, projection::ProjectionExec, repartition::RepartitionExec};
    let node = plan.as_ref();
    downcast::<AggregateExec>(node).is_some()
        || ((downcast::<ProjectionExec>(node).is_some() || downcast::<RepartitionExec>(node).is_some() || downcast::<CoalescePartitionsExec>(node).is_some())
            && plan.children().first().is_some_and(|child| sorts_groups(child)))
}

/// Wrap a heavy plan's root so its execution holds its priced heavy-query permits.
///
/// Registered ONLY on the pgwire-facing session (never maintenance, which has its
/// own pool). Runs last so it wraps the absolute root, whose `execute` the pgwire
/// result reader calls exactly once.
#[derive(Debug)]
pub struct HeavyQueryAdmission;

impl PhysicalOptimizerRule for HeavyQueryAdmission {
    fn name(&self) -> &'static str {
        "HeavyQueryAdmission"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &datafusion::config::ConfigOptions) -> DFResult<Arc<dyn ExecutionPlan>> {
        let Some(class) = heavy_class(&plan, config.execution.batch_size.get()) else {
            return Ok(plan);
        };
        let gate = heavy_gate();
        let permits = heavy_permits(&plan, config.execution.batch_size.get(), gate.partitions, gate.slots);
        // A multi-partition root is executed once per partition (`execute_stream`
        // coalesces over it); coalescing here charges the query only once.
        let plan = match plan.properties().partitioning.partition_count() > 1 {
            true => Arc::new(CoalescePartitionsExec::new(plan)),
            false => plan,
        };
        Ok(Arc::new(AdmissionExec::new(plan, class, permits)))
    }
}

/// Holds the query's weighted permits for the lifetime of the wrapped stream.
pub struct AdmissionExec {
    input: Arc<dyn ExecutionPlan>,
    class: HeavyClass,
    permits: usize,
    properties: std::sync::Arc<PlanProperties>,
}

impl AdmissionExec {
    fn new(input: Arc<dyn ExecutionPlan>, class: HeavyClass, permits: usize) -> Self {
        let properties = input.properties().clone();
        Self { input, class, permits, properties }
    }
}

impl fmt::Debug for AdmissionExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AdmissionExec")
    }
}

impl DisplayAs for AdmissionExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "AdmissionExec: class={}, permits={}", self.class.label(), self.permits)?;
                if let HeavyClass::OrderedMorMerge { fan_in, buffered_bytes } = self.class {
                    write!(f, ", fan_in={fan_in}, buffered_mb={}", buffered_bytes >> 20)?;
                }
                write!(f, ", available={}", heavy_sem().available_permits())
            }
            _ => write!(f, "AdmissionExec"),
        }
    }
}

/// Stream state: acquire the permit lazily on the first poll, then stream the
/// child holding it. A three-state unfold avoids `async-stream` (not a
/// dependency) while keeping the permit's drop tied to the stream's.
enum Admit {
    Pending(Arc<dyn ExecutionPlan>, usize, Arc<TaskContext>, tracing::Span),
    Running(SendableRecordBatchStream, HeldPermit),
    Done,
}

/// A permit plus who holds it, reported on release if held long.
struct HeldPermit {
    _permit: OwnedSemaphorePermit,
    span: tracing::Span,
    since: std::time::Instant,
    class: HeavyClass,
}

impl Drop for HeldPermit {
    fn drop(&mut self) {
        let held = self.since.elapsed();
        if held >= HEAVY_HELD_LOG_AFTER {
            let (class, held_ms) = (self.class.label(), held.as_millis() as u64);
            self.span.in_scope(|| tracing::warn!(event = "heavy_query_held", class, held_ms, "heavy-query permit held long"));
        }
    }
}

impl ExecutionPlan for AdmissionExec {
    no_physical_exprs!();
    fn name(&self) -> &'static str {
        "AdmissionExec"
    }
    fn properties(&self) -> &std::sync::Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.class, self.permits)))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let class = self.class;
        let permits = self.permits as u32;
        // Captured here, synchronously inside the statement's span, so a long hold
        // is reported with the query that caused it.
        let start = Admit::Pending(Arc::clone(&self.input), partition, context, tracing::Span::current());
        let stream = futures::stream::unfold(start, move |state| async move {
            match state {
                Admit::Pending(input, partition, context, span) => {
                    // Acquire BEFORE executing the child, so no scan starts until this
                    // query is admitted. The owned permit lives in the Running state and
                    // releases when the stream ends or is dropped (client disconnect /
                    // cancellation).
                    let sem = Arc::clone(heavy_sem());
                    let queued = sem.available_permits() < permits as usize;
                    let permit = match tokio::time::timeout(HEAVY_QUEUE_WAIT, sem.acquire_many_owned(permits)).await {
                        Ok(Ok(permit)) => HeldPermit { _permit: permit, span, since: std::time::Instant::now(), class },
                        // Semaphore closed only at shutdown: end the stream cleanly.
                        Ok(Err(_closed)) => return None,
                        Err(_elapsed) => {
                            metrics::counter!(scan_metric_names::HEAVY_QUERY_QUEUE_TIMEOUT).increment(1);
                            let err = DataFusionError::ResourcesExhausted(
                                "too many concurrent heavy queries; the request waited for a slot and timed out — retry shortly".to_owned(),
                            );
                            return Some((Err(err), Admit::Done));
                        }
                    };
                    metrics::counter!(scan_metric_names::HEAVY_QUERY_ADMITTED).increment(1);
                    if matches!(class, HeavyClass::OrderedMorMerge { .. }) {
                        metrics::counter!(scan_metric_names::HEAVY_QUERY_ORDERED_MOR_ADMITTED).increment(1);
                    }
                    if queued {
                        metrics::counter!(scan_metric_names::HEAVY_QUERY_QUEUED).increment(1);
                    }
                    let mut stream = match input.execute(partition, context) {
                        Ok(stream) => stream,
                        Err(error) => return Some((Err(error), Admit::Done)),
                    };
                    // Pull the first batch here so the transition carries an item; the
                    // permit rides into Running and drops with the stream.
                    match futures::StreamExt::next(&mut stream).await {
                        Some(batch) => Some((batch, Admit::Running(stream, permit))),
                        None => {
                            drop(permit);
                            None
                        }
                    }
                }
                Admit::Running(mut stream, permit) => match futures::StreamExt::next(&mut stream).await {
                    Some(batch) => Some((batch, Admit::Running(stream, permit))),
                    None => {
                        drop(permit);
                        None
                    }
                },
                Admit::Done => None,
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            compute::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            ExecutionPlan,
            empty::EmptyExec,
            sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        },
    };

    use crate::read::{DedupExec, LegKind, OrderingProbeExec};

    use super::{AdmissionExec, HeavyClass, heavy_class};

    /// DataFusion's default `batch_size`.
    const BATCH: usize = 8192;

    fn empty() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]))))
    }
    fn sort(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortExec::new(ordering(), input).with_fetch(fetch))
    }
    fn ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("t", 0)), SortOptions::default())]).unwrap()
    }
    /// The whale dashboard count's merge columns: key, version and tombstone only.
    fn narrow() -> Arc<Schema> {
        let ts = || DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()));
        Arc::new(Schema::new(vec![
            Field::new("t", ts(), true),
            Field::new("service", DataType::Utf8View, true),
            Field::new("id", DataType::Utf8View, true),
            Field::new("updated_at", ts(), true),
            Field::new("deleted", DataType::Boolean, true),
        ]))
    }
    /// A log-explorer row: the same key plus wide text and a Variant-like struct.
    fn wide() -> Arc<Schema> {
        let variant = DataType::Struct(vec![Field::new("metadata", DataType::Binary, true), Field::new("value", DataType::Binary, true)].into());
        let mut fields: Vec<Field> = narrow().fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.extend([Field::new("body", DataType::Utf8View, true), Field::new("attributes", variant, true)]);
        Arc::new(Schema::new(fields))
    }
    fn ordered_mor(fan_in: usize, probe: bool) -> Arc<dyn ExecutionPlan> {
        ordered_mor_over(wide(), fan_in, probe)
    }
    fn ordered_mor_over(schema: Arc<Schema>, fan_in: usize, probe: bool) -> Arc<dyn ExecutionPlan> {
        let key = schema.field(0).name().clone();
        let input = Arc::new(EmptyExec::new(schema).with_partitions(fan_in));
        let merge = Arc::new(SortPreservingMergeExec::new(ordering(), input)) as Arc<dyn ExecutionPlan>;
        let input = if probe { Arc::new(OrderingProbeExec::new(merge, LegKind::Delta)) as Arc<dyn ExecutionPlan> } else { merge };
        Arc::new(DedupExec::new(input, vec![key], None).unwrap().requiring(Some(ordering())))
    }

    /// The pool-exhausting shape — an unbounded sort — is gated.
    #[test]
    fn an_unbounded_sort_is_heavy() {
        assert_eq!(heavy_class(&sort(empty(), None), BATCH), Some(HeavyClass::SpillingSort));
    }

    #[test_case::test_case(8 => 1 ; "configured width")]
    #[test_case::test_case(26 => 4 ; "physical fanout exceeds configured width")]
    #[test_case::test_case(200 => 8 ; "oversized query takes the whole gate")]
    fn physical_sort_fanout_prices_admission(fanout: usize) -> usize {
        let input = Arc::new(EmptyExec::new(empty().schema()).with_partitions(fanout));
        super::heavy_permits(&sort(input, None), BATCH, 8, 8)
    }

    #[test]
    fn overlapping_sorts_share_the_same_budget() {
        let input = Arc::new(EmptyExec::new(empty().schema()).with_partitions(8));
        let first = Arc::new(SortExec::new(ordering(), input).with_preserve_partitioning(true));
        assert_eq!(super::heavy_permits(&sort(first, None), BATCH, 8, 8), 2);
    }

    /// The failing RUM population query sorts the same histogram input for
    /// both per-epoch and per-series windows, which can execute concurrently.
    #[tokio::test]
    async fn histogram_epoch_and_series_windows_charge_both_sorts() {
        let ctx = datafusion::prelude::SessionContext::new();
        let df = ctx
            .sql(
                "WITH source(series_id,start_timestamp,timestamp,distribution_count) AS (
                VALUES (1,0,1,10), (1,0,2,20), (1,2,3,30)
             ) SELECT LAG(distribution_count) OVER(PARTITION BY series_id,start_timestamp ORDER BY timestamp),
                      LAG(timestamp) OVER(PARTITION BY series_id ORDER BY timestamp)
               FROM source",
            )
            .await
            .unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        assert!(super::heavy_permits(&plan, BATCH, 1, 8) >= 2, "both window orderings must share the heavy budget");
        let rows = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
    }

    fn aggregate(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
        let group = PhysicalGroupBy::new_single(vec![(Arc::new(Column::new(input.schema().field(0).name(), 0)), "t".to_owned())]);
        Arc::new(AggregateExec::try_new(AggregateMode::Single, group, vec![], vec![], Arc::clone(&input), input.schema()).unwrap())
    }

    /// A chart's final ORDER BY sorts the aggregate's groups, not raw rows: not gated,
    /// while a wide merge feeding the aggregate still is.
    #[test_case::test_case(empty() => None ; "sort of groups over a cheap input")]
    #[test_case::test_case(ordered_mor(8, false) => Some("ordered_mor_merge") ; "a wide merge below the aggregate stays gated")]
    fn a_sort_of_aggregate_groups_is_not_heavy(input: Arc<dyn ExecutionPlan>) -> Option<&'static str> {
        heavy_class(&sort(aggregate(input), None), BATCH).map(|class| class.label())
    }

    /// A bounded TopK holds only `n` rows and never spills, so it is NOT gated —
    /// gating it would throttle the fast dashboard path that earned the hit rate.
    #[test]
    fn a_bounded_topk_is_not_heavy() {
        assert_eq!(heavy_class(&sort(empty(), Some(100)), BATCH), None);
    }

    /// A plan with no sort at all — a rollup-routed aggregate, a point lookup — is
    /// never gated.
    #[test]
    fn a_sortless_plan_is_not_heavy() {
        assert_eq!(heavy_class(&empty(), BATCH), None);
    }

    #[test]
    fn a_multi_partition_ordered_mor_merge_is_heavy() {
        assert_eq!(heavy_class(&ordered_mor(8, false), BATCH).map(|c| c.label()), Some("ordered_mor_merge"));
    }

    /// A narrow merge buffers a few MB even at a whale's fan-in, so gating it made
    /// every raw dashboard count of the largest project queue behind 8 slots.
    #[test]
    fn a_narrow_whale_count_merge_is_not_heavy() {
        assert_eq!(heavy_class(&ordered_mor_over(narrow(), 26, false), BATCH), None);
    }

    /// The shape the gate exists for, at prod's batch size and the smallest whale
    /// fan-in seen: every column of the real table must stay gated.
    #[test]
    fn a_full_width_log_explorer_merge_stays_heavy() {
        let schema = crate::schema::get_schema("otel_logs_and_spans").unwrap().schema_ref();
        assert_eq!(heavy_class(&ordered_mor_over(schema, 7, false), 4096).map(|c| c.label()), Some("ordered_mor_merge"));
    }

    #[test]
    fn a_single_partition_ordered_mor_merge_is_not_heavy() {
        assert_eq!(heavy_class(&ordered_mor(1, false), BATCH), None);
    }

    #[test]
    fn an_ordering_probe_does_not_hide_an_ordered_mor_merge() {
        assert_eq!(heavy_class(&ordered_mor(8, true), BATCH).map(|c| c.label()), Some("ordered_mor_merge"));
    }

    /// The wrapper is transparent to the optimizer contract: same schema, one
    /// child, reconstructable.
    #[test]
    fn the_wrapper_preserves_the_plan_contract() {
        let inner = sort(empty(), None);
        let wrapped = Arc::new(AdmissionExec::new(Arc::clone(&inner), HeavyClass::SpillingSort, 1));
        assert_eq!(wrapped.schema(), inner.schema());
        assert_eq!(wrapped.children().len(), 1);
        assert!(datafusion::physical_plan::replace_children_if_necessary(wrapped, vec![inner]).is_ok());
    }

    /// The whole point: a permit is HELD across the wrapped stream and RELEASED
    /// when it ends. Run the real `execute` path against the shared semaphore and
    /// assert availability returns to its start — the RAII contract that keeps K
    /// bounded no matter how the stream ends.
    #[tokio::test]
    async fn a_permit_is_held_across_the_stream_and_released_after() {
        use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
        use futures::StreamExt;

        let before = super::heavy_sem().available_permits();
        let wrapped = Arc::new(AdmissionExec::new(empty(), HeavyClass::SpillingSort, 1));
        let mut stream = wrapped.execute(0, Arc::new(TaskContext::default())).unwrap();
        // First poll drives the Pending arm: the permit is taken here.
        let _ = stream.next().await;
        // Draining to completion releases it.
        while stream.next().await.is_some() {}
        drop(stream);
        // Yield so the drop's release is observed.
        tokio::task::yield_now().await;
        assert_eq!(super::heavy_sem().available_permits(), before, "the permit must return to the pool when the stream ends");
    }

    /// Exhaust the real gate, prove the wrapped stream cannot start, then free a
    /// slot and prove it completes. This makes queue behavior independent of host
    /// memory and query scheduling.
    #[tokio::test]
    async fn an_exhausted_gate_queues_until_a_permit_is_released() {
        use datafusion::{execution::TaskContext, physical_plan::ExecutionPlan};
        use futures::StreamExt;

        let sem = Arc::clone(super::heavy_sem());
        let capacity = sem.available_permits();
        let held = Arc::clone(&sem).acquire_many_owned((capacity - 1).try_into().unwrap()).await.unwrap();
        let wrapped = Arc::new(AdmissionExec::new(empty(), HeavyClass::SpillingSort, 2));
        let mut stream = wrapped.execute(0, Arc::new(TaskContext::default())).unwrap();

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), stream.next()).await.is_err(),
            "a two-slot query must wait when only one slot is free"
        );
        drop(held);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await.unwrap().is_none(), "the empty child completes once admitted");
        assert_eq!(sem.available_permits(), capacity, "the admitted stream returns its permit");
    }

    /// A query that cannot get a slot in time fails with a retryable error
    /// instead of blocking its client.
    #[tokio::test(start_paused = true)]
    async fn an_exhausted_gate_times_out_with_a_retryable_error() {
        use datafusion::{error::DataFusionError, execution::TaskContext};
        use futures::StreamExt;

        let sem = Arc::clone(super::heavy_sem());
        let _held = Arc::clone(&sem).acquire_many_owned(sem.available_permits().try_into().unwrap()).await.unwrap();
        let wrapped = Arc::new(AdmissionExec::new(empty(), HeavyClass::SpillingSort, 1));
        let err = wrapped.execute(0, Arc::new(TaskContext::default())).unwrap().next().await.unwrap().unwrap_err();
        assert!(
            matches!(&err, DataFusionError::ResourcesExhausted(msg) if msg == "too many concurrent heavy queries; the request waited for a slot and timed out — retry shortly"),
            "{err}"
        );
    }
}
