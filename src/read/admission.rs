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
//! to a GiB before a small outer `LIMIT` emits anything. Cheap queries —
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
    error::{DataFusionError, Result as DFResult},
    execution::TaskContext,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
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
    OrderedMorMerge { fan_in: usize },
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

/// Classify only shapes whose per-query memory is large enough to share the
/// heavy-query gate. An ordered merge is heavy only below an order-dependent
/// `DedupExec`; incidental merges elsewhere keep their existing concurrency.
fn heavy_class(plan: &Arc<dyn ExecutionPlan>) -> Option<HeavyClass> {
    if downcast::<SortExec>(plan.as_ref()).is_some_and(|sort| sort.fetch().is_none()) {
        return Some(HeavyClass::SpillingSort);
    }
    if let Some(dedup) = downcast::<DedupExec>(plan.as_ref())
        && dedup.required_ordering().is_some()
        && let Some(fan_in) = plan.children().into_iter().filter_map(ordered_merge_fan_in).max()
    {
        return Some(HeavyClass::OrderedMorMerge { fan_in });
    }
    plan.children().into_iter().find_map(heavy_class)
}

/// Wrap a heavy plan's root so its execution holds one heavy-query permit.
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
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &datafusion::config::ConfigOptions) -> DFResult<Arc<dyn ExecutionPlan>> {
        match heavy_class(&plan) {
            Some(class) => Ok(Arc::new(AdmissionExec::new(plan, class))),
            None => Ok(plan),
        }
    }
}

/// Holds one heavy-query permit for the lifetime of the wrapped stream.
pub struct AdmissionExec {
    input: Arc<dyn ExecutionPlan>,
    class: HeavyClass,
    properties: std::sync::Arc<PlanProperties>,
}

impl AdmissionExec {
    fn new(input: Arc<dyn ExecutionPlan>, class: HeavyClass) -> Self {
        let properties = input.properties().clone();
        Self { input, class, properties }
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
            DisplayFormatType::Default | DisplayFormatType::Verbose => match self.class {
                HeavyClass::OrderedMorMerge { fan_in } => {
                    write!(f, "AdmissionExec: class={}, fan_in={fan_in}, available={}", self.class.label(), heavy_sem().available_permits())
                }
                HeavyClass::SpillingSort => write!(f, "AdmissionExec: class={}, available={}", self.class.label(), heavy_sem().available_permits()),
            },
            _ => write!(f, "AdmissionExec"),
        }
    }
}

/// Stream state: acquire the permit lazily on the first poll, then stream the
/// child holding it. A three-state unfold avoids `async-stream` (not a
/// dependency) while keeping the permit's drop tied to the stream's.
enum Admit {
    Pending(Arc<dyn ExecutionPlan>, usize, Arc<TaskContext>),
    Running(SendableRecordBatchStream, OwnedSemaphorePermit),
    Done,
}

impl ExecutionPlan for AdmissionExec {
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
        Ok(Arc::new(Self::new(children[0].clone(), self.class)))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let schema = self.input.schema();
        let class = self.class;
        let start = Admit::Pending(Arc::clone(&self.input), partition, context);
        let stream = futures::stream::unfold(start, move |state| async move {
            match state {
                Admit::Pending(input, partition, context) => {
                    // Acquire BEFORE executing the child, so no scan starts until this
                    // query is admitted. The owned permit lives in the Running state and
                    // releases when the stream ends or is dropped (client disconnect /
                    // cancellation).
                    let sem = Arc::clone(heavy_sem());
                    let queued = sem.available_permits() == 0;
                    let permit = match tokio::time::timeout(HEAVY_QUEUE_WAIT, sem.acquire_owned()).await {
                        Ok(Ok(permit)) => permit,
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

    fn empty() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new("t", DataType::Int64, false)]))))
    }
    fn sort(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Arc<dyn ExecutionPlan> {
        Arc::new(SortExec::new(ordering(), input).with_fetch(fetch))
    }
    fn ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new("t", 0)), SortOptions::default())]).unwrap()
    }
    fn ordered_mor(fan_in: usize, probe: bool) -> Arc<dyn ExecutionPlan> {
        let input = Arc::new(EmptyExec::new(empty().schema()).with_partitions(fan_in));
        let merge = Arc::new(SortPreservingMergeExec::new(ordering(), input)) as Arc<dyn ExecutionPlan>;
        let input = if probe { Arc::new(OrderingProbeExec::new(merge, LegKind::Delta)) as Arc<dyn ExecutionPlan> } else { merge };
        Arc::new(DedupExec::new(input, vec!["t".into()], None).unwrap().requiring(Some(ordering())))
    }

    /// The pool-exhausting shape — an unbounded sort — is gated.
    #[test]
    fn an_unbounded_sort_is_heavy() {
        assert_eq!(heavy_class(&sort(empty(), None)), Some(HeavyClass::SpillingSort));
    }

    /// A bounded TopK holds only `n` rows and never spills, so it is NOT gated —
    /// gating it would throttle the fast dashboard path that earned the hit rate.
    #[test]
    fn a_bounded_topk_is_not_heavy() {
        assert_eq!(heavy_class(&sort(empty(), Some(100))), None);
    }

    /// A plan with no sort at all — a rollup-routed aggregate, a point lookup — is
    /// never gated.
    #[test]
    fn a_sortless_plan_is_not_heavy() {
        assert_eq!(heavy_class(&empty()), None);
    }

    #[test]
    fn a_multi_partition_ordered_mor_merge_is_heavy() {
        assert_eq!(heavy_class(&ordered_mor(8, false)), Some(HeavyClass::OrderedMorMerge { fan_in: 8 }));
    }

    #[test]
    fn a_single_partition_ordered_mor_merge_is_not_heavy() {
        assert_eq!(heavy_class(&ordered_mor(1, false)), None);
    }

    #[test]
    fn an_ordering_probe_does_not_hide_an_ordered_mor_merge() {
        assert_eq!(heavy_class(&ordered_mor(8, true)), Some(HeavyClass::OrderedMorMerge { fan_in: 8 }));
    }

    /// The wrapper is transparent to the optimizer contract: same schema, one
    /// child, reconstructable.
    #[test]
    fn the_wrapper_preserves_the_plan_contract() {
        let inner = sort(empty(), None);
        let wrapped = Arc::new(AdmissionExec::new(Arc::clone(&inner), HeavyClass::SpillingSort));
        assert_eq!(wrapped.schema(), inner.schema());
        assert_eq!(wrapped.children().len(), 1);
        assert!(Arc::clone(&wrapped).with_new_children(vec![inner]).is_ok());
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
        let wrapped = Arc::new(AdmissionExec::new(empty(), HeavyClass::SpillingSort));
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
        let wrapped = Arc::new(AdmissionExec::new(empty(), HeavyClass::SpillingSort));
        let mut stream = wrapped.execute(0, Arc::new(TaskContext::default())).unwrap();

        assert!(tokio::time::timeout(std::time::Duration::from_millis(20), stream.next()).await.is_err(), "a stream must wait while every permit is held");
        drop(held);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), stream.next()).await.unwrap().is_none(), "the empty child completes once admitted");
        assert_eq!(sem.available_permits(), capacity, "the admitted stream returns its permit");
    }
}
