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
//! dedup, in [`AdmissionExec`], which holds ONE permit for the stream's lifetime.
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
//!
//! A wide scan's fetch and decode heap is outside the query pool too, and its
//! size follows the bytes it selected, not the plan's shape. Behind
//! `timefusion_query_scan_byte_admission`, the same wrapper also charges the
//! selected bytes of every [`crate::database::GatedScanExec`] in the plan,
//! × 1.5, against a [`ScanByteGate`] the size of the query pool, in one
//! acquisition per query so two queries can never each hold half of it.

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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    database::{GatedScanExec, scan_metric_names},
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
static HEAVY_SEM: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

fn heavy_sem() -> &'static Arc<tokio::sync::Semaphore> {
    HEAVY_SEM.get_or_init(|| {
        let k = crate::config::try_config().map_or(8, |cfg| {
            let partitions = match cfg.memory.timefusion_query_partitions {
                0 => cfg.derived.cores(),
                n => n,
            };
            crate::config::max_concurrent_heavy_sorts(partitions, cfg.derived.query_pool_bytes())
        });
        Arc::new(tokio::sync::Semaphore::new(k))
    })
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
fn heavy_class(plan: &Arc<dyn ExecutionPlan>, batch_rows: usize) -> Option<HeavyClass> {
    if downcast::<SortExec>(plan.as_ref()).is_some_and(|sort| sort.fetch().is_none()) {
        return Some(HeavyClass::SpillingSort);
    }
    if let Some(dedup) = downcast::<DedupExec>(plan.as_ref())
        && dedup.required_ordering().is_some()
        && let Some(fan_in) = plan.children().into_iter().filter_map(ordered_merge_fan_in).max()
    {
        let buffered_bytes = fan_in * batch_rows * estimated_row_bytes(&plan.children()[0].schema());
        return (buffered_bytes > crate::config::DEFAULT_SORT_SPILL_RESERVATION_BYTES).then_some(HeavyClass::OrderedMorMerge { fan_in, buffered_bytes });
    }
    plan.children().into_iter().find_map(|child| heavy_class(child, batch_rows))
}

/// A byte budget in KiB (tokio's `acquire_many` takes a `u32`).
#[derive(Clone, Debug)]
pub(crate) struct ScanByteGate {
    sem: Arc<Semaphore>,
    capacity_kib: u32,
}

impl ScanByteGate {
    pub(crate) fn new(pool_bytes: usize) -> Self {
        let capacity_kib = u32::try_from(pool_bytes >> 10).unwrap_or(u32::MAX).max(1);
        Self { sem: Arc::new(Semaphore::new(capacity_kib as usize)), capacity_kib }
    }

    /// `selected` compressed bytes × 1.5, in KiB. Clamped to the budget, since a
    /// larger request would wait forever instead of running alone.
    fn charge_kib(&self, selected: u64) -> u32 {
        u32::try_from(selected.saturating_mul(3).div_ceil(2 << 10)).unwrap_or(u32::MAX).clamp(1, self.capacity_kib)
    }

    #[cfg(test)]
    pub(crate) fn sem(&self) -> &Arc<Semaphore> {
        &self.sem
    }
}

fn gated_scan_bytes(plan: &Arc<dyn ExecutionPlan>) -> Vec<u64> {
    let here = downcast::<GatedScanExec>(plan.as_ref()).and_then(GatedScanExec::selected_bytes);
    plan.children().into_iter().flat_map(gated_scan_bytes).chain(here).collect()
}

/// Wrap a heavy plan's root so its execution holds one heavy-query permit, and
/// with byte admission on, its wide scans' byte charge.
///
/// Registered ONLY on the pgwire-facing session (never maintenance, which has its
/// own pool). Runs last so it wraps the absolute root, whose `execute` the pgwire
/// result reader calls exactly once.
#[derive(Debug)]
pub struct HeavyQueryAdmission {
    pub(crate) scan_bytes: Option<ScanByteGate>,
    /// Largest compressed bytes one wide scan may select. 0 = off.
    pub(crate) scan_cap_bytes: u64,
}

impl PhysicalOptimizerRule for HeavyQueryAdmission {
    fn name(&self) -> &'static str {
        "HeavyQueryAdmission"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &datafusion::config::ConfigOptions) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scans = gated_scan_bytes(&plan);
        let cap = self.scan_cap_bytes;
        if let Some(&over) = scans.iter().find(|&&bytes| cap > 0 && bytes > cap) {
            metrics::counter!(scan_metric_names::SCAN_BYTES_CAP_REFUSED).increment(1);
            return Err(DataFusionError::ResourcesExhausted(format!(
                "a scan selects {over} parquet bytes, over the single-scan cap of {cap} — narrow the time range or add filters"
            )));
        }
        let selected: u64 = scans.iter().sum();
        let bytes = self.scan_bytes.as_ref().filter(|_| selected > 0).map(|gate| (gate.clone(), gate.charge_kib(selected)));
        let class = heavy_class(&plan, config.execution.batch_size.get());
        if class.is_none() && bytes.is_none() {
            return Ok(plan);
        }
        // `execute_stream` runs each partition of a multi-partition root under its
        // own coalesce; coalescing here, as it would, keeps the charge once per query.
        let plan = match bytes.is_some() && plan.properties().partitioning.partition_count() > 1 {
            true => Arc::new(CoalescePartitionsExec::new(plan)),
            false => plan,
        };
        Ok(Arc::new(AdmissionExec::new(plan, class, bytes)))
    }
}

/// Holds one heavy-query permit and/or a scan byte charge for the lifetime of
/// the wrapped stream.
pub struct AdmissionExec {
    input: Arc<dyn ExecutionPlan>,
    class: Option<HeavyClass>,
    bytes: Option<(ScanByteGate, u32)>,
    properties: std::sync::Arc<PlanProperties>,
}

impl AdmissionExec {
    fn new(input: Arc<dyn ExecutionPlan>, class: Option<HeavyClass>, bytes: Option<(ScanByteGate, u32)>) -> Self {
        let properties = input.properties().clone();
        Self { input, class, bytes, properties }
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
                match self.class {
                    Some(class @ HeavyClass::OrderedMorMerge { fan_in, buffered_bytes }) => write!(
                        f,
                        "AdmissionExec: class={}, fan_in={fan_in}, buffered_mb={}, available={}",
                        class.label(),
                        buffered_bytes >> 20,
                        heavy_sem().available_permits()
                    )?,
                    Some(class) => write!(f, "AdmissionExec: class={}, available={}", class.label(), heavy_sem().available_permits())?,
                    None => write!(f, "AdmissionExec: class=scan_bytes")?,
                }
                match &self.bytes {
                    Some((gate, kib)) => write!(f, ", scan_kib={kib}, scan_available_kib={}", gate.sem.available_permits()),
                    None => Ok(()),
                }
            }
            _ => write!(f, "AdmissionExec"),
        }
    }
}

/// Stream state: acquire the permit lazily on the first poll, then stream the
/// child holding it. A three-state unfold avoids `async-stream` (not a
/// dependency) while keeping the permit's drop tied to the stream's.
enum Admit {
    Pending(Arc<dyn ExecutionPlan>, usize, Arc<TaskContext>, tracing::Span, Option<(ScanByteGate, u32)>),
    Running(SendableRecordBatchStream, HeldPermit),
    Done,
}

/// The permits plus who holds them, reported on release if held long.
struct HeldPermit {
    _permits: [Option<OwnedSemaphorePermit>; 2],
    span: tracing::Span,
    since: std::time::Instant,
    class: Option<HeavyClass>,
}

impl Drop for HeldPermit {
    fn drop(&mut self) {
        let held = self.since.elapsed();
        if held >= HEAVY_HELD_LOG_AFTER {
            let (class, held_ms) = (self.class.map_or("scan_bytes", HeavyClass::label), held.as_millis() as u64);
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
        Ok(Arc::new(Self::new(children[0].clone(), self.class, self.bytes.clone())))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let class = self.class;
        // Captured here, synchronously inside the statement's span, so a long hold
        // is reported with the query that caused it.
        let start = Admit::Pending(Arc::clone(&self.input), partition, context, tracing::Span::current(), self.bytes.clone());
        let stream = futures::stream::unfold(start, move |state| async move {
            match state {
                Admit::Pending(input, partition, context, span, bytes) => {
                    // Acquire BEFORE executing the child, so no scan starts until this
                    // query is admitted. The owned permit lives in the Running state and
                    // releases when the stream ends or is dropped (client disconnect /
                    // cancellation).
                    // Always heavy slot, then bytes, under one deadline: a fixed order
                    // leaves no cycle between queries.
                    let deadline = tokio::time::Instant::now() + HEAVY_QUEUE_WAIT;
                    let heavy = match class {
                        Some(class) => {
                            match admit(Arc::clone(heavy_sem()), 1, deadline, HEAVY_METRICS, "too many concurrent heavy queries; the request waited for a slot")
                                .await
                            {
                                Some(Ok(permit)) => {
                                    if matches!(class, HeavyClass::OrderedMorMerge { .. }) {
                                        metrics::counter!(scan_metric_names::HEAVY_QUERY_ORDERED_MOR_ADMITTED).increment(1);
                                    }
                                    Some(permit)
                                }
                                Some(Err(err)) => return Some((Err(err), Admit::Done)),
                                None => return None,
                            }
                        }
                        None => None,
                    };
                    let scan = match bytes {
                        Some((gate, kib)) => {
                            match admit(gate.sem, kib, deadline, SCAN_BYTES_METRICS, "too many concurrent wide scans; the request waited for scan memory").await
                            {
                                Some(Ok(permit)) => Some(permit),
                                Some(Err(err)) => return Some((Err(err), Admit::Done)),
                                None => return None,
                            }
                        }
                        None => None,
                    };
                    let permit = HeldPermit { _permits: [heavy, scan], span, since: std::time::Instant::now(), class };
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

/// Admitted, queued and queue-timeout counters of one gate.
type GateMetrics = [&'static str; 3];
const HEAVY_METRICS: GateMetrics =
    [scan_metric_names::HEAVY_QUERY_ADMITTED, scan_metric_names::HEAVY_QUERY_QUEUED, scan_metric_names::HEAVY_QUERY_QUEUE_TIMEOUT];
const SCAN_BYTES_METRICS: GateMetrics =
    [scan_metric_names::SCAN_BYTES_ADMITTED, scan_metric_names::SCAN_BYTES_QUEUED, scan_metric_names::SCAN_BYTES_QUEUE_TIMEOUT];

/// Take `n` permits by `deadline`, or a retryable `busy` error. `None` means the
/// semaphore closed, which happens only at shutdown: end the stream cleanly.
async fn admit(
    sem: Arc<Semaphore>, n: u32, deadline: tokio::time::Instant, [admitted, queued, timed_out]: GateMetrics, busy: &str,
) -> Option<DFResult<OwnedSemaphorePermit>> {
    let waits = sem.available_permits() < n as usize;
    match tokio::time::timeout_at(deadline, sem.acquire_many_owned(n)).await {
        Ok(Ok(permit)) => {
            metrics::counter!(admitted).increment(1);
            if waits {
                metrics::counter!(queued).increment(1);
            }
            Some(Ok(permit))
        }
        Ok(Err(_closed)) => None,
        Err(_elapsed) => {
            metrics::counter!(timed_out).increment(1);
            Some(Err(DataFusionError::ResourcesExhausted(format!("{busy} and timed out — retry shortly"))))
        }
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

    use super::{AdmissionExec, HeavyClass, ScanByteGate, heavy_class};

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
        let wrapped = Arc::new(AdmissionExec::new(Arc::clone(&inner), Some(HeavyClass::SpillingSort), None));
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
        let wrapped = Arc::new(AdmissionExec::new(empty(), Some(HeavyClass::SpillingSort), None));
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
        let held = Arc::clone(&sem).acquire_many_owned(capacity.try_into().unwrap()).await.unwrap();
        let wrapped = Arc::new(AdmissionExec::new(empty(), Some(HeavyClass::SpillingSort), None));
        let mut stream = wrapped.execute(0, Arc::new(TaskContext::default())).unwrap();

        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), stream.next()).await.is_err(), "a stream must wait while every permit is held");
        drop(held);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await.unwrap().is_none(), "the empty child completes once admitted");
        assert_eq!(sem.available_permits(), capacity, "the admitted stream returns its permit");
    }

    /// Selected bytes × 1.5 in KiB, never zero and never past the budget.
    #[test_case::test_case(1 => 1 ; "any selection costs something")]
    #[test_case::test_case(2 << 20 => 3 << 10 ; "two MiB selected charges three")]
    #[test_case::test_case(u64::MAX => 1 << 20 ; "an over-pool scan runs alone rather than waiting forever")]
    fn a_scan_is_charged_half_again_its_selected_bytes(selected: u64) -> u32 {
        ScanByteGate::new(1 << 30).charge_kib(selected)
    }

    /// A query that cannot get scan memory in time fails with the same retryable
    /// class as a heavy-slot timeout, instead of blocking its client.
    #[tokio::test(start_paused = true)]
    async fn an_exhausted_byte_budget_times_out_with_a_retryable_error() {
        use datafusion::{error::DataFusionError, execution::TaskContext};
        use futures::StreamExt;

        let gate = ScanByteGate::new(1 << 20);
        let _held = Arc::clone(gate.sem()).acquire_many_owned(1 << 10).await.unwrap();
        let wrapped = Arc::new(AdmissionExec::new(empty(), None, Some((gate, 1))));
        let err = wrapped.execute(0, Arc::new(TaskContext::default())).unwrap().next().await.unwrap().unwrap_err();
        assert!(
            matches!(&err, DataFusionError::ResourcesExhausted(msg) if msg == "too many concurrent wide scans; the request waited for scan memory and timed out — retry shortly"),
            "{err}"
        );
    }
}
