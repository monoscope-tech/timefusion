//! Read-side merge-on-read deduplication for `(timestamp, id)` rows.
//!
//! The physical operator runs after routing so filter and partition pushdown
//! remain intact. It stores encoded keys rather than wide payload rows.
//!
//! Two survivor policies:
//!
//! * **keep-first** stores seen keys. Arrival order chooses the physical copy.
//! * **keep-greatest** keeps the greatest tiebreak per key, with NULL lowest.
//!   Ordered input streams by timestamp run; each run is memory-capped.
//!
//! Unordered keep-greatest buffers to end-of-stream. It cannot degrade to
//! keep-first because doing so can return the pre-update version.
//!
//! This stays single-partition: repartitioning copies every wide column and was
//! slower and much larger in measured production-shaped benchmarks.
//!
//! The caller adds key/tiebreak columns, then restores the requested projection.

pub mod functions;
pub mod optimizers;
pub mod plan_cache;

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BooleanArray, LargeStringArray, RecordBatch, StringArray, StringViewArray},
        compute::{SortOptions, filter_record_batch},
        datatypes::{DataType, SchemaRef},
        row::{RowConverter, SortField},
    },
    error::{DataFusionError, Result as DFResult},
    execution::{
        TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
        metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet, RecordOutput},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::StreamExt;

use crate::observability::arrow_err;

/// Encoded Arrow keys, allocated only on first sight.
type SeenSet = HashSet<Box<[u8]>, ahash::RandomState>;

/// Maximum retained bytes for one ordered timestamp run.
const RUN_BUFFER_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Per-query ceiling for unordered keep-greatest state.
const UNBOUNDED_GREATEST_MAX_BYTES: usize = 2 * 1024 * 1024 * 1024;

fn check_unbounded_growth(current: usize, additional: usize) -> DFResult<()> {
    let requested =
        current.checked_add(additional).ok_or_else(|| DataFusionError::ResourcesExhausted("unordered merge-on-read dedup buffer size overflow".to_string()))?;
    if requested > UNBOUNDED_GREATEST_MAX_BYTES {
        return Err(DataFusionError::ResourcesExhausted(format!(
            "unordered merge-on-read dedup exceeded its {} MiB per-query limit; narrow the time window or compact unsorted files",
            UNBOUNDED_GREATEST_MAX_BYTES / 1024 / 1024
        )));
    }
    Ok(())
}

/// Tracks an ordered timestamp run so its dedup state can be released promptly.
struct Bound {
    /// Bound column index within the input schema.
    idx: usize,
    /// True when the sort is descending (bound decreases down the stream).
    desc: bool,
    /// The current run's bound value; `None` until the first row.
    last: Option<i64>,
}

impl Bound {
    /// Counts ordering violations for a specific union leg.
    fn advance_counting(&mut self, t: i64, leg: LegKind) {
        if let Some(l) = self.last
            && if self.desc { t > l } else { t < l }
        {
            leg.counter().fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if self.last.is_none_or(|l| if self.desc { t < l } else { t > l }) {
            self.last = Some(t);
        }
    }

    fn advance(&mut self, t: i64) -> bool {
        // A value moving AGAINST the declared direction proves this scan's
        // advertised ordering is false — a parquet footer's `sorting_columns` is
        // lying. Dedup stays sound either way (`dedup_key_idxs`), but this is the
        // only direct signal that the hot-tail footer repair still has work to
        // do; a zero here across prod would exonerate footers entirely and send
        // the 2026-08-07 under-count investigation elsewhere.
        if let Some(l) = self.last
            && if self.desc { t > l } else { t < l }
        {
            ORDERING_VIOLATIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.last.is_none_or(|l| if self.desc { t < l } else { t > l }) && self.last.replace(t).is_some()
    }
}

/// Which union leg a row came from. Also carries the sortability the plan
/// builder needs, so the two can no longer drift apart in parallel vectors —
/// the Delta leg is the one that must never be sorted at read time (an UPDATE
/// writes a row's ORIGINAL timestamp into a NEW file, so its files overlap and
/// the blocking sort that "fixes" that exhausted the query pool, prod
/// 2026-08-02).
#[derive(Clone, Copy, Debug, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum LegKind {
    Mem,
    Delta,
}

impl LegKind {
    pub fn sortable(self) -> bool {
        !matches!(self, LegKind::Delta)
    }

    pub fn label(self) -> &'static str {
        self.into()
    }

    fn counter(self) -> &'static std::sync::atomic::AtomicU64 {
        match self {
            LegKind::Mem => &ORDERING_VIOLATIONS_MEM,
            LegKind::Delta => &ORDERING_VIOLATIONS_DELTA,
        }
    }
}

pub(crate) static ORDERING_VIOLATIONS_MEM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) static ORDERING_VIOLATIONS_DELTA: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn ordering_violations_by_leg() -> [(&'static str, u64); 2] {
    [LegKind::Mem, LegKind::Delta].map(|leg| (leg.label(), leg.counter().load(std::sync::atomic::Ordering::Relaxed)))
}

/// Diagnostic wrapper that answers "which leg's declared ordering is false?".
///
/// `ORDERING_VIOLATIONS` is counted inside `DedupExec`, which is single-partition
/// and sits above the mem ∪ hot ∪ delta union — so by the time a violation is
/// seen the row's leg is gone, and the plan algebra alone cannot say which leg
/// lied (I tried; every leg looks honest on paper because a leg that declares
/// nothing stops the union from declaring either). This checks each leg against
/// its OWN declared ordering, so a nonzero counter names the culprit directly.
///
/// OFF by default (`TIMEFUSION_ORDERING_PROBE`): it costs one i64 compare per
/// row per leg, which is the same order as the bound check it duplicates. Turn
/// it on when `ordering_violations_total` is nonzero and you need attribution.
#[derive(derive_more::Debug)]
#[debug("OrderingProbeExec: leg={}", leg.label())]
pub struct OrderingProbeExec {
    inner: Arc<dyn ExecutionPlan>,
    leg: LegKind,
}

impl OrderingProbeExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, leg: LegKind) -> Self {
        Self { inner, leg }
    }
}

impl DisplayAs for OrderingProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OrderingProbeExec: leg={}", self.leg.label())
    }
}

impl ExecutionPlan for OrderingProbeExec {
    fn name(&self) -> &'static str {
        "OrderingProbeExec"
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(self: Arc<Self>, mut children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0), self.leg)))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DFResult<Arc<datafusion::common::Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        use futures::StreamExt;
        let stream = self.inner.execute(partition, context)?;
        let schema = stream.schema();
        // The leg's OWN claim — not the union's. `None` means this leg declares
        // nothing, and a leg that promises nothing cannot break a promise.
        let Some(mut bound) = detect_bound(&self.inner, &[], &schema, true).or_else(|| leading_bound(&self.inner, &schema)) else {
            return Ok(stream);
        };
        let leg = self.leg;
        let out = stream.map(move |batch| {
            let batch = batch?;
            if let Some(col) = batch.column(bound.idx).as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>() {
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        bound.advance_counting(col.value(i), leg);
                    }
                }
            } else if let Some(col) = batch.column(bound.idx).as_any().downcast_ref::<arrow::array::Int64Array>() {
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        bound.advance_counting(col.value(i), leg);
                    }
                }
            }
            Ok(batch)
        });
        Ok(Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// The leg's leading sort column as a `Bound`, ignoring whether it is a dedup
/// key — the probe cares only about "did this leg honour what it declared".
fn leading_bound(input: &Arc<dyn ExecutionPlan>, in_schema: &SchemaRef) -> Option<Bound> {
    let se = input.properties().output_ordering()?.iter().next()?;
    let col = sort_col(se)?;
    matches!(in_schema.field(col.index()).data_type(), DataType::Int64 | DataType::Timestamp(..)).then(|| Bound {
        idx: col.index(),
        desc: se.options.descending,
        last: None,
    })
}

/// Rows observed out of the order their scan declared. See `Bound::advance`.
pub(crate) static ORDERING_VIOLATIONS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn ordering_violations() -> u64 {
    ORDERING_VIOLATIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// The i64-backed values of a bound column (timestamps / Int64), for cheap
/// run-boundary comparison. `None` for any other type → bounded mode disabled.
pub(crate) fn bound_slice(col: &ArrayRef) -> Option<&[i64]> {
    use datafusion::arrow::{
        array::AsArray,
        datatypes::{Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType},
    };
    Some(match col.data_type() {
        DataType::Int64 => col.as_primitive::<Int64Type>().values(),
        DataType::Timestamp(TimeUnit::Second, _) => col.as_primitive::<TimestampSecondType>().values(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => col.as_primitive::<TimestampMillisecondType>().values(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => col.as_primitive::<TimestampMicrosecondType>().values(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col.as_primitive::<TimestampNanosecondType>().values(),
        _ => return None,
    })
}

/// Downcast a sort expr's physical expr to `Column`. Explicit `Any` upcast —
/// the `PhysicalExpr` trait's `as_any` collides with downcast-rs's blanket
/// method in this crate's scope.
fn sort_col(se: &datafusion::physical_expr::PhysicalSortExpr) -> Option<&datafusion::physical_expr::expressions::Column> {
    crate::read::optimizers::downcast(se.expr.as_ref())
}

/// Emergency kill switch for bounded[timestamp] dedup. Defaults ON.
///
/// Correctness does NOT depend on this — `dedup_key_idxs` keeps the operator
/// sound under a lying footer. Turning it off is a big hammer with a real cost:
/// bounded mode is what lets keep-greatest emit per run instead of buffering to
/// end-of-stream, so disabling it also disables LIMIT early termination
/// (`keep_greatest_limit_terminates_early` runs unbounded and does not finish).
/// A "top 100" log-explorer query would scan the whole window. Reach for it only
/// if a bounded scan is proven to be serving wrong rows again.
static BOUNDED_DEDUP_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

static ORDERING_PROBE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Per-leg ordering attribution (`OrderingProbeExec`). OFF unless
/// `TIMEFUSION_ORDERING_PROBE=true`: it costs an i64 compare per row per leg,
/// and it answers a question you only ask when `ordering_violations_total` is
/// already nonzero.
pub fn ordering_probe_enabled() -> bool {
    *ORDERING_PROBE.get_or_init(|| std::env::var("TIMEFUSION_ORDERING_PROBE").is_ok_and(|v| v.eq_ignore_ascii_case("true") || v == "1"))
}

pub fn bounded_dedup_enabled() -> bool {
    *BOUNDED_DEDUP_ENABLED.get_or_init(|| true)
}

/// Set the bounded-dedup kill switch. No-op after the first call (OnceLock).
pub fn set_bounded_dedup_enabled(enabled: bool) {
    let _ = BOUNDED_DEDUP_ENABLED.set(enabled);
}

/// The dedup key columns to hash, given the chosen bound.
///
/// The bound column is ALWAYS retained. It used to be filtered out here: within
/// a *genuinely* sorted run the bound is constant, so encoding it into every key
/// is redundant, and dropping it saved one timestamp encoding per physical row.
///
/// That reasoning holds only while the declared ordering is TRUE. A parquet
/// footer missing/misreporting `sorting_columns` makes a scan declare
/// `output_ordering=[timestamp DESC]` over data that is not in that order (see
/// the hot-tail footer repair). The bound then never advances across the
/// mis-ordered stretch, one "run" spans many timestamps, and a key reduced to
/// `id` alone collapses rows that differ only in `timestamp` — distinct rows.
/// Prod 2026-08-07: a single minute read 132 rows instead of 1620, surfacing as
/// multi-minute holes in customer dashboards.
///
/// Keeping the bound in the key makes bounded mode fail-SAFE: a false ordering
/// can now only under-dedup (emit a duplicate), never drop a distinct row.
fn dedup_key_idxs(bound: Option<&Bound>, key_idxs: &[usize]) -> Vec<usize> {
    let _ = bound;
    key_idxs.to_vec()
}

/// Enable bounded mode iff the input's leading sort column is a dedup key of an
/// i64-backed type AND `timefusion_read_dedup_bounded` is on.
///
/// The ordering here is *declared*, never verified — `output_ordering()` is only
/// as trustworthy as the parquet footer behind it. `dedup_key_idxs` keeps the
/// operator sound when that declaration lies; the flag is the kill switch for
/// when it lies badly enough to matter, and defaults OFF until the footer repair
/// has drained the poisoned files.
fn detect_bound(input: &Arc<dyn ExecutionPlan>, keys: &[String], in_schema: &SchemaRef, enabled: bool) -> Option<Bound> {
    if !enabled {
        return None;
    }
    let se = input.properties().output_ordering()?.iter().next()?;
    let col = sort_col(se)?;
    (keys.iter().any(|k| k == col.name()) && matches!(in_schema.field(col.index()).data_type(), DataType::Int64 | DataType::Timestamp(..))).then(|| Bound {
        idx: col.index(),
        desc: se.options.descending,
        last: None,
    })
}

/// The input's output ordering, remapped through `output_projection` onto the
/// dedup output schema. Keeps the longest prefix of plain-column sort exprs
/// whose columns survive the projection (`map_while`: a non-column or
/// projected-away expr truncates it); `None` when nothing survives or the input
/// declares no ordering.
fn remap_ordering(
    input: &Arc<dyn ExecutionPlan>, output_projection: &Option<Vec<usize>>, schema: &SchemaRef,
) -> Option<datafusion::physical_expr::LexOrdering> {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
    let out: Vec<PhysicalSortExpr> = input
        .properties()
        .output_ordering()?
        .iter()
        .map_while(|se| {
            let col = sort_col(se)?;
            let ni = match output_projection {
                None => col.index(),
                Some(idxs) => idxs.iter().position(|&i| i == col.index())?,
            };
            Some(PhysicalSortExpr::new(Arc::new(Column::new(schema.field(ni).name(), ni)), se.options))
        })
        .collect();
    LexOrdering::new(out)
}

#[derive(Debug)]
pub struct DedupExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<String>,
    /// Indices of the key columns within `input.schema()`.
    key_idxs: Vec<usize>,
    /// Schema's `dedup_tiebreak` column name, when the table declares one.
    /// Keep-greatest engages only if it is also present in the input schema.
    tiebreak: Option<String>,
    /// Ordering keep-greatest DEPENDS on, declared as *required* so
    /// `EnforceSorting` preserves it.
    ///
    /// Without this the operator is silently correctness-fragile: the caller
    /// builds a `SortPreservingMergeExec` to supply the run property, but
    /// EnforceSorting deletes any ordering no parent requires — which is every
    /// aggregate. The plan then reaches `execute` unordered, `detect_bound`
    /// returns `None`, keep-greatest degrades to keep-FIRST, and a merge-on-read
    /// table answers `MAX(updated_at)` with the PRE-update row while the same
    /// data read by a plain projection answers correctly. Requiring the ordering
    /// is what makes version resolution a property of the operator rather than
    /// of what happens to sit above it.
    required_ordering: Option<datafusion::physical_expr::LexOrdering>,
    /// Indices into `input.schema()` to emit after dedup, restoring the
    /// originally-requested projection. `None` = emit the input schema as-is.
    output_projection: Option<Vec<usize>>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DedupExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, keys: Vec<String>, output_projection: Option<Vec<usize>>) -> DFResult<Self> {
        Self::with_tiebreak(input, keys, None, output_projection)
    }

    pub fn with_tiebreak(input: Arc<dyn ExecutionPlan>, keys: Vec<String>, tiebreak: Option<String>, output_projection: Option<Vec<usize>>) -> DFResult<Self> {
        let in_schema = input.schema();
        let key_idxs = keys
            .iter()
            .map(|k| in_schema.index_of(k).map_err(|_| DataFusionError::Internal(format!("DedupExec key `{k}` not in input schema"))))
            .collect::<DFResult<Vec<_>>>()?;
        let schema = match &output_projection {
            Some(idxs) => Arc::new(in_schema.project(idxs)?),
            None => in_schema.clone(),
        };
        // Dedup preserves the input's row order (it only drops rows), so the
        // input's output ordering remains valid on the output (remapped through
        // the projection). Without this the sorted Delta scan's declared order
        // (fork sort-order pushdown) dies here and `ORDER BY timestamp LIMIT n`
        // re-sorts the whole window instead of early-terminating.
        let eq = match remap_ordering(&input, &output_projection, &schema) {
            Some(ordering) => datafusion::physical_expr::EquivalenceProperties::new_with_orderings(schema.clone(), [ordering]),
            None => datafusion::physical_expr::EquivalenceProperties::new(schema.clone()),
        };
        let properties =
            Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), input.properties().emission_type, input.properties().boundedness));
        Ok(Self { input, keys, key_idxs, tiebreak, required_ordering: None, output_projection, schema, properties, metrics: ExecutionPlanMetricsSet::new() })
    }

    /// Declare the ordering keep-greatest needs (see `required_ordering`).
    /// `None` leaves the operator ordering-agnostic — the pre-merge-on-read
    /// behaviour every table without `version_append` keeps.
    pub fn requiring(mut self, ordering: Option<datafusion::physical_expr::LexOrdering>) -> Self {
        self.required_ordering = ordering;
        self
    }

    /// The ordering keep-greatest depends on, for `DedupNeedsOrderedInput` to
    /// rebuild a merge the optimizer discharged as trivially satisfied.
    pub fn required_ordering(&self) -> Option<&datafusion::physical_expr::LexOrdering> {
        self.required_ordering.as_ref()
    }
}

impl DisplayAs for DedupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Surface which seen-set mode this scan will actually run. `bounded`
        // clears state whenever the bound advances (O(distinct keys within one
        // bound value)); `full-set` retains every key for the whole scan — the
        // multi-GB risk this module's docs warn about, and ~19% of prod live
        // heap in the 2026-07-31 profile. The two are indistinguishable in
        // EXPLAIN without this, so the only symptom is unexplained heap growth.
        let in_schema = self.input.schema();
        write!(f, "DedupExec: keys=[{}], mode=", self.keys.join(", "))?;
        // Which SURVIVOR rule runs matters as much as the seen-set size:
        // unbounded input with a tiebreak now keeps the GREATEST version
        // (buffering to end-of-stream); without a tiebreak it is keep-first.
        // Reading `full-set` alone used to hide that difference, and under
        // merge-on-read keep-first silently serves the pre-update row.
        let survivor = if self.tiebreak.as_ref().is_some_and(|tb| in_schema.index_of(tb).is_ok()) { "greatest" } else { "first" };
        match detect_bound(&self.input, &self.keys, &in_schema, bounded_dedup_enabled()) {
            Some(b) => write!(f, "bounded[{}]/{survivor}", in_schema.field(b.idx).name()),
            None => write!(f, "full-set/{survivor}"),
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for DedupExec {
    fn name(&self) -> &'static str {
        "DedupExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<datafusion::physical_expr::OrderingRequirements>> {
        vec![self.required_ordering.clone().map(datafusion::physical_expr::OrderingRequirements::from)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // Streaming dedup: surviving rows appear in input order (keep-greatest
        // emits a closed run in input position order). Lets EnforceSorting swap
        // the single-partition coalesce below for a SortPreservingMergeExec when
        // a downstream ordering requires it.
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            DedupExec::with_tiebreak(children[0].clone(), self.keys.clone(), self.tiebreak.clone(), self.output_projection.clone())?
                .requiring(self.required_ordering.clone()),
        ))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!("DedupExec only produces partition 0, got {partition}")));
        }
        let in_schema = self.input.schema();
        let bound = detect_bound(&self.input, &self.keys, &in_schema, bounded_dedup_enabled());
        let key_idxs = dedup_key_idxs(bound.as_ref(), &self.key_idxs);
        // A bound lets keep-greatest emit per run without buffering the stream,
        // so it is the preferred shape — but it is no longer REQUIRED. Refusing
        // to run unbounded is what forced `version_append` scans to manufacture
        // an ordering they could not get for free (merge-on-read writes a row's
        // ORIGINAL timestamp into a NEW file, so Delta files overlap in time),
        // and that blocking SortExec exhausted the query pool on prod
        // 2026-08-02. Unbounded keep-greatest buffers to end-of-stream instead —
        // strictly cheaper than the sort it replaces, and it keeps the operator
        // CORRECT, where the old unbounded fallback (keep-first) would serve the
        // pre-update row.
        // The run buffer is REAL heap the pool must see: unbounded keep-greatest
        // holds the whole scan's batches until end-of-stream, and untracked
        // that is exactly the anon-RSS growth that OOM-killed prod on
        // 2026-08-03 (kernel: anon-rss 125GB against a 120GiB cgroup). Under
        // the pool an oversized window fails ITS query with
        // ResourcesExhausted; the server survives.
        let reservation = MemoryConsumer::new("DedupExec[keep-greatest]").register(context.memory_pool());
        let greatest = self
            .tiebreak
            .as_ref()
            .and_then(|tb| in_schema.index_of(tb).ok())
            .map(|idx| Greatest::new(idx, in_schema.field(idx).data_type(), reservation))
            .transpose()?;
        let direct_string_key = (greatest.is_some() && key_idxs.len() == 1).then(|| key_idxs[0]).filter(|idx| {
            let field = in_schema.field(*idx);
            !field.is_nullable() && matches!(field.data_type(), DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
        });
        let dedup = Dedup {
            conv: RowConverter::new(key_idxs.iter().map(|&i| SortField::new(in_schema.field(i).data_type().clone())).collect()).map_err(arrow_err)?,
            key_idxs,
            output_projection: self.output_projection.clone(),
            seen: SeenSet::default(),
            bound,
            greatest,
            direct_string_key,
        };

        let input = self.input.execute(partition, context)?;
        let out_schema = self.schema.clone();
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let input_rows = MetricBuilder::new(&self.metrics).counter("input_rows", partition);
        // One input batch can yield several output batches (a keep-greatest
        // flush emits one per buffered batch) or none at all; `flat_map` fans
        // them out lazily — an empty result just re-polls the source, and a
        // downstream LIMIT can stop mid-run.
        let stream = futures::stream::unfold((input, dedup, baseline, input_rows, false), |(mut input, mut dedup, baseline, input_rows, done)| async move {
            if done {
                return None;
            }
            let (produced, done) = match input.next().await {
                None => {
                    let produced = {
                        let _timer = baseline.elapsed_compute().timer();
                        dedup.finish()
                    };
                    baseline.done();
                    (produced, true)
                }
                Some(Err(e)) => (Err(e), false),
                Some(Ok(batch)) => {
                    input_rows.add(batch.num_rows());
                    let produced = {
                        let _timer = baseline.elapsed_compute().timer();
                        dedup.push(&batch)
                    };
                    (produced, false)
                }
            };
            if let Ok(batches) = &produced {
                for batch in batches {
                    batch.record_output(&baseline);
                }
            }
            Some((produced, (input, dedup, baseline, input_rows, done)))
        })
        .flat_map(|r| futures::stream::iter(r.map_or_else(|e| vec![Err(e)], |bs| bs.into_iter().map(Ok).collect::<Vec<_>>())));

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Winning row for a dedup key within the open run: where it sits in the
/// buffered batches, plus its order-encoded tiebreak for comparison.
struct Cand {
    batch: u32,
    row: u32,
    tb: TiebreakValue,
}

/// Most telemetry timestamps identify one logical event, so a bounded run
/// normally has one winner (plus its physical MOR copies). Avoid hashing those
/// tiny runs; promote only an unusually wide equal-timestamp run to a map.
const SMALL_WINNER_LIMIT: usize = 8;

enum WinnerUpdate {
    Loses,
    ReplaceSmall(usize),
    ReplaceLarge,
    Insert,
}

enum Winners {
    Small(Vec<(Box<[u8]>, Cand)>),
    Large(HashMap<Box<[u8]>, Cand, ahash::RandomState>),
}

impl Winners {
    fn new() -> Self {
        Self::Small(Vec::with_capacity(SMALL_WINNER_LIMIT))
    }

    fn probe(&self, key: &[u8], tb: &TiebreakRef<'_>) -> WinnerUpdate {
        match self {
            Self::Small(entries) => match entries.iter().position(|(stored, _)| stored.as_ref() == key) {
                Some(i) if tb.beats(&entries[i].1.tb) => WinnerUpdate::ReplaceSmall(i),
                Some(_) => WinnerUpdate::Loses,
                None => WinnerUpdate::Insert,
            },
            Self::Large(entries) => match entries.get(key) {
                Some(cand) if tb.beats(&cand.tb) => WinnerUpdate::ReplaceLarge,
                Some(_) => WinnerUpdate::Loses,
                None => WinnerUpdate::Insert,
            },
        }
    }

    fn apply(&mut self, key: &[u8], cand: Cand, update: WinnerUpdate) {
        match update {
            WinnerUpdate::Loses => unreachable!("losing winner update must not be applied"),
            WinnerUpdate::ReplaceSmall(i) => {
                let Self::Small(entries) = self else { unreachable!("small winner probe changed representation") };
                entries[i].1 = cand;
            }
            WinnerUpdate::ReplaceLarge => {
                let Self::Large(entries) = self else { unreachable!("large winner probe changed representation") };
                entries.insert(key.into(), cand);
            }
            WinnerUpdate::Insert => match self {
                Self::Small(entries) if entries.len() < SMALL_WINNER_LIMIT => entries.push((key.into(), cand)),
                Self::Small(entries) => {
                    let mut promoted = HashMap::with_capacity_and_hasher(entries.len() + 1, ahash::RandomState::new());
                    promoted.extend(entries.drain(..));
                    promoted.insert(key.into(), cand);
                    *self = Self::Large(promoted);
                }
                Self::Large(entries) => {
                    entries.insert(key.into(), cand);
                }
            },
        }
    }

    fn drain(&mut self, mut f: impl FnMut(Box<[u8]>, Cand)) {
        match self {
            Self::Small(entries) => entries.drain(..).for_each(|(key, cand)| f(key, cand)),
            Self::Large(entries) => {
                entries.drain().for_each(|(key, cand)| f(key, cand));
                *self = Self::new();
            }
        }
    }

    fn shift_batches(&mut self, count: u32) {
        match self {
            Self::Small(entries) => entries.iter_mut().for_each(|(_, cand)| cand.batch -= count),
            Self::Large(entries) => entries.values_mut().for_each(|cand| cand.batch -= count),
        }
    }

    fn min_batch(&self, default: usize) -> usize {
        match self {
            Self::Small(entries) => entries.iter().map(|(_, cand)| cand.batch as usize).min().unwrap_or(default),
            Self::Large(entries) => entries.values().map(|cand| cand.batch as usize).min().unwrap_or(default),
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Small(entries) => entries.len(),
            Self::Large(entries) => entries.len(),
        }
    }
}

/// Owned winner tiebreak. Timestamp/int64 values stay primitive; other schema
/// types retain Arrow's generic order-preserving encoding.
enum TiebreakValue {
    I64(Option<i64>),
    Encoded(Box<[u8]>),
}

enum TiebreakRows<'a> {
    I64 { values: &'a [i64], column: &'a ArrayRef },
    Encoded(datafusion::arrow::row::Rows),
}

impl TiebreakRows<'_> {
    fn value(&self, row: usize) -> TiebreakRef<'_> {
        match self {
            Self::I64 { values, column } => TiebreakRef::I64(column.is_valid(row).then(|| values[row])),
            Self::Encoded(rows) => TiebreakRef::Encoded(rows.row(row).data()),
        }
    }
}

enum TiebreakRef<'a> {
    I64(Option<i64>),
    Encoded(&'a [u8]),
}

impl TiebreakRef<'_> {
    fn beats(&self, old: &TiebreakValue) -> bool {
        match (self, old) {
            (Self::I64(new), TiebreakValue::I64(old)) => new > old,
            (Self::Encoded(new), TiebreakValue::Encoded(old)) => *new > &old[..],
            _ => unreachable!("one Greatest instance uses one tiebreak representation"),
        }
    }

    fn into_owned(self) -> TiebreakValue {
        match self {
            Self::I64(value) => TiebreakValue::I64(value),
            Self::Encoded(value) => TiebreakValue::Encoded(value.into()),
        }
    }
}

/// Keep-greatest run state: the open run's batches (Arc clones) and its
/// per-key winners.
struct Greatest {
    /// Tiebreak column index within the input schema.
    idx: usize,
    /// Order-preserving encoder for the tiebreak (ascending, NULLs first ⇒ NULL
    /// encodes lowest, so a pre-existing row loses to any new version). Arrow's
    /// row format compares byte-lexicographically in value order, so `>` on the
    /// encoded bytes *is* `>` on the value, for any type.
    conv: Option<RowConverter>,
    best: Winners,
    batches: Vec<RecordBatch>,
    /// Winner masks accumulated for CLOSED runs. A bounded stream may contain
    /// thousands of timestamp runs in one Arrow batch; filtering the whole
    /// batch at every boundary made that path O(rows × batch_rows). We mark
    /// winners here and filter each retained batch once when it is safe to
    /// emit.
    masks: Vec<Vec<bool>>,
    bytes: usize,
    /// Pool accounting for `batches` (`bytes` mirrors its size). The winner
    /// map is second-order (one small entry per key) and stays untracked.
    reservation: MemoryReservation,
}

impl Greatest {
    fn new(idx: usize, dt: &DataType, reservation: MemoryReservation) -> DFResult<Self> {
        let primitive_i64 = matches!(dt, DataType::Int64 | DataType::Timestamp(..));
        let conv = (!primitive_i64)
            .then(|| {
                let sf = SortField::new_with_options(dt.clone(), SortOptions { descending: false, nulls_first: true });
                RowConverter::new(vec![sf]).map_err(arrow_err)
            })
            .transpose()?;
        Ok(Self { idx, conv, best: Winners::new(), batches: Vec::new(), masks: Vec::new(), bytes: 0, reservation })
    }

    fn tiebreak_rows<'a>(&self, column: &'a ArrayRef) -> DFResult<TiebreakRows<'a>> {
        match &self.conv {
            Some(conv) => Ok(TiebreakRows::Encoded(conv.convert_columns(std::slice::from_ref(column)).map_err(arrow_err)?)),
            None => {
                let values = bound_slice(column)
                    .ok_or_else(|| DataFusionError::Internal(format!("DedupExec primitive tiebreak column {} is not i64-backed", self.idx)))?;
                Ok(TiebreakRows::I64 { values, column })
            }
        }
    }

    /// Close the current bound-value run by marking its winning rows. `partial`
    /// is the memory-ceiling fallback: park those keys in `seen` so the tail of
    /// the same run cannot emit them again (the existing keep-first fallback).
    fn close_run(&mut self, seen: &mut SeenSet, partial: bool) {
        self.best.drain(|k, c| {
            self.masks[c.batch as usize][c.row as usize] = true;
            if partial {
                seen.insert(k);
            }
        });
    }

    /// Emit a closed prefix of retained batches exactly once. Any candidates
    /// for the still-open run live at or after `count`; their indices are
    /// shifted after the drain. This keeps at most the trailing cross-batch run
    /// buffered while completed rows stream onward.
    fn emit_prefix(&mut self, count: usize, output_projection: Option<&[usize]>) -> DFResult<Vec<RecordBatch>> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let batches: Vec<RecordBatch> = self.batches.drain(..count).collect();
        let masks: Vec<Vec<bool>> = self.masks.drain(..count).collect();
        let freed: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
        self.bytes = self.bytes.saturating_sub(freed);
        self.reservation.shrink(freed);
        self.best.shift_batches(count as u32);
        batches
            .into_iter()
            .zip(masks)
            .map(|(b, m)| filter_project_out(&b, &BooleanArray::from(m), output_projection))
            .filter(|r| !r.as_ref().is_ok_and(|o| o.num_rows() == 0))
            .collect()
    }
}

/// Per-partition streaming dedup state.
struct Dedup {
    key_idxs: Vec<usize>,
    conv: RowConverter,
    output_projection: Option<Vec<usize>>,
    /// Keep-first: keys already emitted. Keep-greatest: keys emitted by a
    /// partial (overflow) flush of the open run, cleared at the run boundary.
    seen: SeenSet,
    bound: Option<Bound>,
    greatest: Option<Greatest>,
    /// Greatest-mode fast path for one non-null string key. The production
    /// bounded key becomes just `id: Utf8View` after removing timestamp.
    direct_string_key: Option<usize>,
}

enum KeyRows {
    Encoded(datafusion::arrow::row::Rows),
    Utf8(ArrayRef),
    Utf8View(ArrayRef),
    LargeUtf8(ArrayRef),
}

impl KeyRows {
    fn value(&self, row: usize) -> &[u8] {
        match self {
            Self::Encoded(rows) => rows.row(row).data(),
            Self::Utf8(array) => array.as_any().downcast_ref::<StringArray>().expect("validated Utf8 key").value(row).as_bytes(),
            Self::Utf8View(array) => array.as_any().downcast_ref::<StringViewArray>().expect("validated Utf8View key").value(row).as_bytes(),
            Self::LargeUtf8(array) => array.as_any().downcast_ref::<LargeStringArray>().expect("validated LargeUtf8 key").value(row).as_bytes(),
        }
    }
}

impl Dedup {
    fn push(&mut self, batch: &RecordBatch) -> DFResult<Vec<RecordBatch>> {
        let Some(g) = self.greatest.as_mut() else {
            let key_arrays: Vec<ArrayRef> = self.key_idxs.iter().map(|&i| batch.column(i).clone()).collect();
            let keys = self.conv.convert_columns(&key_arrays).map_err(arrow_err)?;
            return Ok(dedup_first(batch, &keys, &mut self.seen, self.output_projection.as_deref(), self.bound.as_mut())?.into_iter().collect());
        };
        let keys = match self.direct_string_key {
            Some(idx) => match batch.column(idx).data_type() {
                DataType::Utf8 => KeyRows::Utf8(batch.column(idx).clone()),
                DataType::Utf8View => KeyRows::Utf8View(batch.column(idx).clone()),
                DataType::LargeUtf8 => KeyRows::LargeUtf8(batch.column(idx).clone()),
                _ => unreachable!("direct string key type was validated at construction"),
            },
            None => {
                let key_arrays: Vec<ArrayRef> = self.key_idxs.iter().map(|&i| batch.column(i).clone()).collect();
                KeyRows::Encoded(self.conv.convert_columns(&key_arrays).map_err(arrow_err)?)
            }
        };
        let proj = self.output_projection.as_deref();
        let mut out = Vec::new();
        // Only a BOUNDED run may flush early. Without a bound the whole scan is
        // one open run: any key can still be beaten by a later batch, so emitting
        // it now would serve the superseded row — exactly the merge-on-read bug
        // this operator exists to prevent. Unbounded therefore buffers until
        // end-of-stream, which is no worse than the blocking SortExec that
        // forcing an ordering used to insert (and is a hash, not a spill).
        if self.bound.is_some() && g.bytes > RUN_BUFFER_MAX_BYTES {
            g.close_run(&mut self.seen, true);
            out.extend(g.emit_prefix(g.batches.len(), proj)?);
        }
        let tbs = g.tiebreak_rows(batch.column(g.idx))?;
        let bvals = match self.bound.as_ref() {
            Some(bound) => Some(
                bound_slice(batch.column(bound.idx))
                    .ok_or_else(|| DataFusionError::Internal(format!("DedupExec bound column {} is not i64-backed", bound.idx)))?,
            ),
            None => None,
        };
        // Index of `batch` within the open run's buffer; `None` until a row of
        // this batch wins something, and reset by every flush (the run changed).
        let mut cur: Option<u32> = None;
        for i in 0..batch.num_rows() {
            if let (Some(bound), Some(vals)) = (self.bound.as_mut(), bvals.as_ref())
                && bound.advance(vals[i])
            {
                g.close_run(&mut self.seen, false);
                self.seen.clear();
            }
            let key = keys.value(i);
            if self.seen.contains(key) {
                continue;
            }
            let tb = tbs.value(i);
            let update = g.best.probe(key, &tb);
            if matches!(update, WinnerUpdate::Loses) {
                continue;
            }
            let bi = match cur {
                Some(bi) => bi,
                None => {
                    // Pool BEFORE buffering: on ResourcesExhausted the query
                    // fails here instead of the cgroup killing the server.
                    //
                    // Compact first, because the run buffer RETAINS what it
                    // holds. Batches read back by the DML UPDATE path are view
                    // arrays over the parquet reader's whole column-chunk
                    // blocks, so buffering one both charges the pool that block
                    // and keeps it alive — prod 2026-08-17: the enrichment
                    // UPDATE failed after 16.9s asking for 15.2 GB on a pool
                    // 1.8 GB into 16 GB, fed by 847 KB and 5 MB of files.
                    // `compact_batch` returns the batch untouched when there is
                    // nothing to compact.
                    let owned = crate::write::mem_buffer::compact_batch(batch.clone());
                    let size = owned.get_array_memory_size();
                    if self.bound.is_none() {
                        check_unbounded_growth(g.bytes, size)?;
                    }
                    g.reservation.try_grow(size)?;
                    g.batches.push(owned);
                    g.masks.push(vec![false; batch.num_rows()]);
                    g.bytes += size;
                    let bi = g.batches.len() as u32 - 1;
                    cur = Some(bi);
                    bi
                }
            };
            g.best.apply(key, Cand { batch: bi, row: i as u32, tb: tb.into_owned() }, update);
        }
        // Sorted input guarantees the open run is a suffix. Emit every batch
        // before the earliest candidate it still owns; a batch containing both
        // closed winners and the open run waits until the next boundary.
        let keep_from = g.best.min_batch(g.batches.len());
        out.extend(g.emit_prefix(keep_from, proj)?);
        Ok(out)
    }

    /// End of stream: emit the still-open run.
    fn finish(&mut self) -> DFResult<Vec<RecordBatch>> {
        match self.greatest.as_mut() {
            Some(g) => {
                g.close_run(&mut self.seen, false);
                g.emit_prefix(g.batches.len(), self.output_projection.as_deref())
            }
            None => Ok(Vec::new()),
        }
    }
}

/// Filter only columns the caller will consume. COUNT and narrow projections
/// augment the scan with ID/tiebreak columns solely for winner selection;
/// filtering those variable-width arrays and then throwing them away was a
/// large avoidable copy on amplified merge-on-read scans.
fn filter_project_out(batch: &RecordBatch, mask: &BooleanArray, output_projection: Option<&[usize]>) -> DFResult<RecordBatch> {
    let projected = match output_projection {
        Some(idxs) => batch.project(idxs).map_err(arrow_err)?,
        None => batch.clone(),
    };
    filter_record_batch(&projected, mask).map_err(arrow_err)
}

/// Keep-first: drop rows whose key tuple was already emitted, then restore the
/// requested projection. Returns `None` when nothing survives (caller pulls the
/// next batch).
fn dedup_first(
    batch: &RecordBatch, keys: &datafusion::arrow::row::Rows, seen: &mut SeenSet, output_projection: Option<&[usize]>, mut bound: Option<&mut Bound>,
) -> DFResult<Option<RecordBatch>> {
    // Bounded-window values (Tier 2). `bound_slice` returning None (unsupported
    // type) silently disables eviction for this batch — still correct.
    // `.map(idx)` first so the slice's lifetime is tied to `batch`, not to the
    // borrow of `bound` the closure below needs mutably.
    let bvals = bound.as_ref().map(|b| b.idx).and_then(|i| bound_slice(batch.column(i)));
    // Borrowed probe: hash the encoded bytes in place; on a miss (first sighting)
    // allocate one `Box<[u8]>`. Duplicates never allocate — the mask is the
    // negation folded into the `&&` so a hit short-circuits before `insert`.
    // When bounded and the bound advances past the current run, the seen-set is
    // cleared first: no earlier key can recur in a sorted stream.
    let mask: BooleanArray = (0..batch.num_rows())
        .map(|i| {
            if let (Some(b), Some(vals)) = (bound.as_deref_mut(), bvals)
                && b.advance(vals[i])
            {
                seen.clear();
            }
            let bytes = keys.row(i).data();
            !seen.contains(bytes) && {
                seen.insert(bytes.into());
                true
            }
        })
        .collect();
    let out = filter_project_out(batch, &mask, output_projection)?;
    Ok((out.num_rows() > 0).then_some(out))
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{Array, Int64Array, StringArray},
            datatypes::{Field, Schema},
        },
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column},
    };

    use super::*;

    #[test]
    fn winner_store_avoids_hashing_small_runs_and_promotes_wide_runs() {
        let mut winners = Winners::new();
        for row in 0..SMALL_WINNER_LIMIT {
            let key = format!("id-{row}");
            let tb = TiebreakRef::I64(Some(1));
            let update = winners.probe(key.as_bytes(), &tb);
            winners.apply(key.as_bytes(), Cand { batch: 0, row: row as u32, tb: tb.into_owned() }, update);
        }
        assert!(matches!(winners, Winners::Small(_)));

        let key = b"promotes";
        let tb = TiebreakRef::I64(Some(1));
        let update = winners.probe(key, &tb);
        winners.apply(key, Cand { batch: 0, row: 8, tb: tb.into_owned() }, update);
        assert!(matches!(winners, Winners::Large(_)));

        let newer = TiebreakRef::I64(Some(2));
        let update = winners.probe(key, &newer);
        assert!(matches!(update, WinnerUpdate::ReplaceLarge));
        winners.apply(key, Cand { batch: 1, row: 9, tb: newer.into_owned() }, update);
        assert_eq!(winners.min_batch(99), 0);

        let mut drained = 0;
        winners.drain(|_, _| drained += 1);
        assert_eq!(drained, SMALL_WINNER_LIMIT + 1);
        assert!(matches!(winners, Winners::Small(_)), "a closed wide run must return to the hash-free representation");
    }

    fn batch(ids: &[&str], vals: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false), Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(ids.to_vec())), Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    fn conv() -> RowConverter {
        RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap()
    }

    fn keys_of(b: &RecordBatch, idxs: &[usize], c: &RowConverter) -> datafusion::arrow::row::Rows {
        c.convert_columns(&idxs.iter().map(|&i| b.column(i).clone()).collect::<Vec<_>>()).unwrap()
    }

    /// COUNT-correctness + keep-first across batches: the seen-set threads
    /// state, duplicates (including cross-batch) collapse, and the *first*
    /// occurrence's row survives (its `v`). Regression guard for the
    /// bytes-keyed borrowed-probe rewrite (parity plan Point 3, Tier 1).
    #[test]
    fn dedup_batch_keeps_first_and_counts_distinct() {
        let converter = conv();
        let mut seen = SeenSet::default();
        let b1 = batch(&["a", "b", "a", "c"], &[1, 2, 3, 4]);
        let out1 = dedup_first(&b1, &keys_of(&b1, &[0], &converter), &mut seen, None, None).unwrap().unwrap();
        let ids = out1.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = out1.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.iter().flatten().collect::<Vec<_>>(), vec!["a", "b", "c"]);
        assert_eq!(vs.values(), &[1, 2, 4], "first occurrence survives (a→1, not a→3)");

        // Second batch: every key already seen → whole batch drops (None).
        let b2 = batch(&["b", "a"], &[9, 9]);
        assert!(dedup_first(&b2, &keys_of(&b2, &[0], &converter), &mut seen, None, None).unwrap().is_none());

        // A fresh key in an otherwise-dup batch survives alone.
        let b3 = batch(&["a", "d"], &[9, 5]);
        let out3 = dedup_first(&b3, &keys_of(&b3, &[0], &converter), &mut seen, None, None).unwrap().unwrap();
        assert_eq!(out3.num_rows(), 1);
        assert_eq!(out3.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "d");
    }

    /// Tier-2 bounded-window: keying on `(id, ts)` with `ts` as the bound, a
    /// later batch whose `ts` has advanced clears the seen-set — so an `id`
    /// re-seen at a *new* ts is a distinct dedup key and correctly survives
    /// (it is NOT the same row), while an exact `(id, ts)` dup within one run
    /// still collapses. Also asserts the set actually shrinks (eviction fired).
    #[test]
    fn dedup_batch_bounded_window_evicts_on_advance() {
        // Two-column key (id, ts); bound = ts (col 1), ascending.
        let converter = RowConverter::new(vec![SortField::new(DataType::Utf8), SortField::new(DataType::Int64)]).unwrap();
        let mut seen = SeenSet::default();
        let mut bound = Bound { idx: 1, desc: false, last: None };

        // Run ts=10: a,b,a → within-run dup of `a` collapses.
        let r1 = batch(&["a", "b", "a"], &[10, 10, 10]);
        let o1 = dedup_first(&r1, &keys_of(&r1, &[0, 1], &converter), &mut seen, None, Some(&mut bound)).unwrap().unwrap();
        assert_eq!(o1.num_rows(), 2, "(a,10),(b,10) survive; second (a,10) dropped");
        assert_eq!(seen.len(), 2);

        // ts advances to 11: seen cleared; `a` re-seen at a NEW ts is a distinct
        // key and survives. Set shrank to the new run only.
        let r2 = batch(&["a", "a"], &[11, 11]);
        let o2 = dedup_first(&r2, &keys_of(&r2, &[0, 1], &converter), &mut seen, None, Some(&mut bound)).unwrap().unwrap();
        assert_eq!(o2.num_rows(), 1, "(a,11) survives once; second (a,11) is a same-run dup");
        assert_eq!(seen.len(), 1, "seen-set bounded to the current run, not O(all distinct)");
        assert_eq!(bound.last, Some(11));
    }

    /// A parquet footer that DECLARES `timestamp DESC` over data not actually in
    /// that order makes the bound never advance, so one "run" spans many
    /// timestamps. Reducing the key to `id` alone (the old optimisation) then
    /// collapsed rows differing only in `timestamp` — distinct rows. Prod
    /// 2026-08-07: one minute read 132 rows instead of 1620, seen by customers
    /// as multi-minute holes in their dashboards.
    #[test]
    fn bounded_dedup_false_ordering_does_not_collapse_distinct_timestamps() {
        // Declared DESC, actually ASCENDING — the footer lied.
        let mut bound = Bound { idx: 1, desc: true, last: None };
        let b = batch(&["a", "a"], &[5, 10]);

        // The reduced key (`id` only) is what lost the rows.
        let reduced = RowConverter::new(vec![SortField::new(DataType::Utf8)]).unwrap();
        let mut seen = SeenSet::default();
        let lost = dedup_first(&b, &keys_of(&b, &[0], &reduced), &mut seen, None, Some(&mut bound)).unwrap().unwrap();
        assert_eq!(lost.num_rows(), 1, "documents the old bug: (a,5) and (a,10) collapsed");

        // `dedup_key_idxs` must therefore RETAIN the bound column, making the
        // operator fail-safe under a false ordering.
        assert_eq!(dedup_key_idxs(Some(&Bound { idx: 1, desc: true, last: None }), &[0, 1]), vec![0, 1], "bound column must stay in the dedup key");

        let full = RowConverter::new(vec![SortField::new(DataType::Utf8), SortField::new(DataType::Int64)]).unwrap();
        let mut bound2 = Bound { idx: 1, desc: true, last: None };
        let mut seen2 = SeenSet::default();
        let kept = dedup_first(&b, &keys_of(&b, &[0, 1], &full), &mut seen2, None, Some(&mut bound2)).unwrap().unwrap();
        assert_eq!(kept.num_rows(), 2, "(a,5) and (a,10) are DISTINCT rows and must both survive");
    }

    /// The kill switch must actually reach `detect_bound`: with bounded dedup
    /// disabled, no ordering — however confidently declared — selects bounded
    /// mode. It defaults ON (disabling it also disables LIMIT early
    /// termination), so correctness must not depend on it.
    #[test]
    fn bounded_dedup_kill_switch_forces_full_set() {
        let plan = greatest_plan(vec![vbatch(&["a", "a"], &[5, 10], &[Some(1), Some(2)])]);
        let schema = plan.input.schema();
        assert!(detect_bound(&plan.input, &plan.keys, &schema, false).is_none(), "flag off ⇒ full-set regardless of declared ordering");
        assert!(bounded_dedup_enabled(), "default must stay ON: full-set has no LIMIT early termination");
    }

    /// An out-of-order row must be COUNTED, so prod can tell whether footers
    /// actually lie rather than inferring it.
    #[test]
    fn advance_counts_declared_ordering_violations() {
        let before = ordering_violations();
        let mut b = Bound { idx: 0, desc: true, last: None };
        b.advance(10); // first row: no baseline, no violation
        b.advance(5); // DESC-consistent
        b.advance(9); // moves back UP under a DESC claim → violation
        assert_eq!(ordering_violations(), before + 1);
    }

    /// EXPLAIN must say WHICH seen-set mode a scan will run. `full-set` retains
    /// every key for the whole scan (multi-GB on wide scans, ~19% of prod live
    /// heap on 2026-07-31) while `bounded` clears per run — previously the plan
    /// rendered identically either way, so the only symptom was heap growth
    /// with no way to attribute it. Both spellings are asserted so this
    /// diagnostic can't quietly stop distinguishing them.
    #[test]
    fn explain_reveals_seen_set_mode() {
        let b = batch(&["a"], &[10]);
        let keys = vec!["id".to_string(), "v".to_string()];

        let sorted = DedupExec::new(source(&[vec![b.clone()]], Some(col_asc("v", 1))), keys.clone(), None).unwrap();
        let shown = format!("{}", datafusion::physical_plan::displayable(&sorted).one_line());
        assert!(shown.contains("mode=bounded[v]"), "sorted input must report the bounded window, got: {shown}");

        let unsorted = DedupExec::new(source(&[vec![b]], None), keys, None).unwrap();
        let shown = format!("{}", datafusion::physical_plan::displayable(&unsorted).one_line());
        assert!(shown.contains("mode=full-set"), "unsorted input must report the unbounded seen-set, got: {shown}");
    }

    // ---- plumbing ----

    fn source(partitions: &[Vec<RecordBatch>], ordering: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
        use datafusion::datasource::{memory::MemorySourceConfig, source::DataSourceExec};
        let schema = partitions[0][0].schema();
        let cfg = MemorySourceConfig::try_new(partitions, schema, None).unwrap();
        let cfg = match ordering {
            Some(o) => cfg.try_with_sort_information(vec![o]).unwrap(),
            None => cfg,
        };
        Arc::new(DataSourceExec::new(Arc::new(cfg)))
    }

    fn col_asc(name: &str, idx: usize) -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(Column::new(name, idx)), SortOptions::default())]).unwrap()
    }

    // ---- keep-greatest (merge-on-read phase 2) ----

    /// (id Utf8, ts Int64, tb Int64 nullable) — dedup key `(id, ts)` sorted by
    /// `ts`, tiebreak `tb`. Mirrors `(timestamp, id)` + `updated_at`.
    fn vbatch(ids: &[&str], ts: &[i64], tb: &[Option<i64>]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("tb", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(ids.to_vec())), Arc::new(Int64Array::from(ts.to_vec())), Arc::new(Int64Array::from(tb.to_vec()))],
        )
        .unwrap()
    }

    fn greatest_plan(batches: Vec<RecordBatch>) -> DedupExec {
        let src = source(&[batches], Some(col_asc("ts", 1)));
        DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], Some("tb".into()), None).unwrap()
    }

    /// Same, but the source declares NO ordering — the merge-on-read shape, where
    /// an UPDATE writes the row's original timestamp into a new file so the Delta
    /// leg's files overlap in time and no ordering can be declared.
    fn unbounded_greatest_plan(batches: Vec<RecordBatch>) -> DedupExec {
        let src = source(&[batches], None);
        DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], Some("tb".into()), None).unwrap()
    }

    /// Unsorted input MUST still keep the greatest version. This is the whole
    /// reason merge-on-read can be enabled without forcing an ordering: the old
    /// code refused keep-greatest without a bound and degraded to keep-FIRST,
    /// which serves the PRE-UPDATE row. Forcing the ordering instead inserted a
    /// blocking SortExec that exhausted the query pool on prod (2026-08-02:
    /// 1h ~13s, 3h timing out), so neither existing branch was usable.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_without_a_bound_still_picks_the_newest_version() {
        // `a` is updated in a LATER batch; `b`'s newer version arrives FIRST.
        // Neither ordering assumption holds, and the ts column is not monotonic.
        let plan = unbounded_greatest_plan(vec![vbatch(&["a", "b"], &[10, 20], &[Some(1), Some(9)]), vbatch(&["b", "a"], &[20, 10], &[Some(2), Some(7)])]);
        let mut got = collect_rows(&plan).await;
        got.sort();
        assert_eq!(got, vec![("a".into(), 10, Some(7)), ("b".into(), 20, Some(9))], "unbounded keep-greatest must win on the tiebreak, not on arrival order");

        // A NULL stamp (a legacy row written before the version column existed)
        // must still lose to any stamped version, in either arrival order.
        let plan = unbounded_greatest_plan(vec![vbatch(&["c"], &[30], &[None]), vbatch(&["c"], &[30], &[Some(4)])]);
        assert_eq!(collect_rows(&plan).await, vec![("c".into(), 30, Some(4))]);
    }

    /// Non-i64 tiebreaks retain the generic Arrow row encoding fallback.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_string_tiebreak_uses_generic_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("tb", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 10, 20])),
                Arc::new(StringArray::from(vec![Some("a"), Some("z"), None])),
            ],
        )
        .unwrap();
        let plan = DedupExec::with_tiebreak(source(&[vec![batch]], Some(col_asc("ts", 1))), vec!["ts".into(), "id".into()], Some("tb".into()), None).unwrap();
        let batches = datafusion::physical_plan::collect(Arc::new(plan), Arc::new(TaskContext::default())).await.unwrap();
        let rows = batches.iter().flat_map(|batch| {
            let ids = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let tbs = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            (0..batch.num_rows()).map(move |i| (ids.value(i).to_string(), tbs.is_valid(i).then(|| tbs.value(i).to_string())))
        });
        assert_eq!(rows.collect::<Vec<_>>(), vec![("a".into(), Some("z".into())), ("b".into(), None)]);
    }

    /// The unbounded run buffer must be POOL-TRACKED: untracked it grows with
    /// the whole scan as anon heap the cgroup cannot see coming, which
    /// OOM-killed prod on 2026-08-03 (kernel: anon-rss 125GB / 120GiB limit).
    /// Under a pool the oversized query fails with ResourcesExhausted and the
    /// server survives.
    #[tokio::test(flavor = "multi_thread")]
    async fn unbounded_run_buffer_is_pool_tracked_so_an_oversized_scan_fails_its_query() {
        use datafusion::execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder};
        let batches: Vec<RecordBatch> = (0..64).map(|i| vbatch(&[format!("k{i}").as_str()], &[i], &[Some(i)])).collect();
        let plan = unbounded_greatest_plan(batches);
        let runtime = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(GreedyMemoryPool::new(512))).build_arc().unwrap();
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut stream = plan.execute(0, ctx).unwrap();
        let mut err = None;
        while let Some(r) = futures::StreamExt::next(&mut stream).await {
            if let Err(e) = r {
                err = Some(e);
                break;
            }
        }
        let err = err.expect("a 512-byte pool must refuse the run buffer");
        assert!(format!("{err}").contains("Resources exhausted"), "must fail with the pool's error, got: {err}");
    }

    /// A batch whose columns are slices of a much larger parent must be charged
    /// for the rows it owns, not for the allocation it borrows from.
    ///
    /// Prod 2026-08-17: monoscope's enrichment UPDATE failed after 16.9s with
    /// `Failed to allocate additional 15.2 GB for DedupExec[keep-greatest]` on
    /// a pool that was 1.8 GB used of 16 GB, fed by scans reading 847 KB and
    /// 5 MB of files. Batches from the DML UPDATE path are view arrays over the
    /// parquet reader's full column-chunk blocks, and `get_array_memory_size`
    /// charges the whole block — the mechanism `mem_buffer::compact_batch`
    /// already exists to neutralize.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_slicing_a_big_parent_is_charged_for_its_own_rows() {
        use datafusion::execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder};
        // One 64k-row parent per column; each batch keeps two rows of it.
        let ids: Vec<String> = (0..65536).map(|i| format!("k{i}")).collect();
        let parent_id = StringArray::from(ids.iter().map(String::as_str).collect::<Vec<_>>());
        let parent_ts = Int64Array::from((0..65536i64).collect::<Vec<_>>());
        let parent_tb = Int64Array::from((0..65536i64).map(Some).collect::<Vec<_>>());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("tb", DataType::Int64, true),
        ]));
        let sliced = |off: usize| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(parent_id.slice(off, 2)), Arc::new(parent_ts.slice(off, 2)), Arc::new(parent_tb.slice(off, 2))])
                .unwrap()
        };
        let batches: Vec<RecordBatch> = (0..8).map(|i| sliced(i * 2)).collect();
        let owned: usize = batches.iter().map(|b| b.num_rows()).sum();
        let inherited = batches[0].get_array_memory_size();
        assert!(inherited > 64 * 1024, "a slice of a 64k-row parent must report the parent's buffers, else this test proves nothing");

        // Comfortably above the 16 rows actually retained, far below 8 x the
        // inherited charge. Only honest accounting fits here.
        let plan = unbounded_greatest_plan(batches);
        let runtime = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(GreedyMemoryPool::new(inherited))).build_arc().unwrap();
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let mut stream = plan.execute(0, ctx).unwrap();
        let mut rows = 0;
        while let Some(r) = futures::StreamExt::next(&mut stream).await {
            rows += r.expect("16 sliced rows must not be charged the whole parent").num_rows();
        }
        assert_eq!(rows, owned, "every distinct key must survive");
    }

    #[test]
    fn unbounded_run_buffer_has_a_per_query_ceiling() {
        assert!(check_unbounded_growth(UNBOUNDED_GREATEST_MAX_BYTES - 1, 1).is_ok());
        let err = check_unbounded_growth(UNBOUNDED_GREATEST_MAX_BYTES, 1).unwrap_err();
        assert!(format!("{err}").contains("per-query limit"));
        assert!(check_unbounded_growth(usize::MAX, 1).is_err(), "overflow must fail closed");
    }

    /// EXPLAIN must distinguish the SURVIVOR rule, not just the seen-set size:
    /// `full-set/first` silently serves the pre-update row under merge-on-read,
    /// while `full-set/greatest` is correct. They were indistinguishable before.
    #[test]
    fn explain_reports_the_survivor_rule() {
        let plan = unbounded_greatest_plan(vec![vbatch(&["a"], &[1], &[Some(1)])]);
        let shown = format!("{}", datafusion::physical_plan::displayable(&plan).one_line());
        assert!(shown.contains("mode=full-set/greatest"), "unsorted + tiebreak must report keep-greatest, got: {shown}");
    }

    async fn collect_rows(plan: &DedupExec) -> Vec<(String, i64, Option<i64>)> {
        let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut out = Vec::new();
        while let Some(b) = futures::StreamExt::next(&mut stream).await {
            let b = b.unwrap();
            let (ids, ts, tb) = (
                b.column(0).as_any().downcast_ref::<StringArray>().unwrap(),
                b.column(1).as_any().downcast_ref::<Int64Array>().unwrap(),
                b.column(2).as_any().downcast_ref::<Int64Array>().unwrap(),
            );
            out.extend((0..b.num_rows()).map(|i| (ids.value(i).to_string(), ts.value(i), tb.is_valid(i).then(|| tb.value(i)))));
        }
        out
    }

    /// The version-append contract: the greatest-tiebreak copy of a key wins,
    /// even when the newer version arrives in a later batch, and even when the
    /// key's versions straddle batches within one `ts` run. Counts stay right.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_picks_highest_tiebreak_across_batches() {
        let plan = greatest_plan(vec![
            vbatch(&["a", "b"], &[10, 10], &[Some(1), Some(5)]),
            vbatch(&["a", "b"], &[10, 10], &[Some(7), Some(2)]), // a upgraded, b's older loses
            vbatch(&["a"], &[10], &[Some(3)]),
        ]);
        assert_eq!(collect_rows(&plan).await, vec![("b".into(), 10, Some(5)), ("a".into(), 10, Some(7))]);
    }

    /// A production OTel batch contains many distinct timestamps. Bounded
    /// greatest-version dedup must filter that Arrow batch once, not once per
    /// timestamp run (the latter made a nominally streaming 24h scan spend
    /// seconds rebuilding full-batch Boolean masks).
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_greatest_coalesces_many_runs_in_one_input_batch() {
        let ids: Vec<String> = (0..4096).map(|i| format!("id-{i}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let ts: Vec<i64> = (0..4096).collect();
        let tb: Vec<Option<i64>> = (0..4096).map(Some).collect();
        let plan = greatest_plan(vec![vbatch(&id_refs, &ts, &tb)]);
        let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut batches = Vec::new();
        while let Some(batch) = futures::StreamExt::next(&mut stream).await {
            batches.push(batch.unwrap());
        }
        assert_eq!(batches.len(), 1, "one input batch must not fragment into one output batch per timestamp");
        assert_eq!(batches[0].num_rows(), 4096);
    }

    /// Runs are the unit of emission: keys of a closed run are emitted when the
    /// bound advances, and a key re-seen at a *new* ts is a different row.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_across_run_boundary() {
        let plan = greatest_plan(vec![
            vbatch(&["a", "a", "b"], &[10, 10, 11], &[Some(1), Some(9), Some(1)]),
            vbatch(&["a", "b"], &[11, 11], &[Some(4), Some(8)]),
            vbatch(&["a"], &[12], &[Some(0)]),
        ]);
        // Survivors are emitted at their own input positions (a@11 wins at row 0
        // of batch 2, b@11 at row 1), i.e. a subsequence of the input — so any
        // declared ordering, not just the bound column, still holds.
        assert_eq!(
            collect_rows(&plan).await,
            vec![("a".into(), 10, Some(9)), ("a".into(), 11, Some(4)), ("b".into(), 11, Some(8)), ("a".into(), 12, Some(0)),]
        );
    }

    /// NULL tiebreak sorts lowest: a pre-existing (unstamped) row always loses
    /// to any stamped version, in either arrival order.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_null_tiebreak_loses() {
        let plan = greatest_plan(vec![vbatch(&["a", "a", "b", "b"], &[1, 1, 2, 2], &[None, Some(1), Some(1), None])]);
        assert_eq!(collect_rows(&plan).await, vec![("a".into(), 1, Some(1)), ("b".into(), 2, Some(1))]);
        // All-NULL keeps exactly one row (no spurious duplicate).
        let plan = greatest_plan(vec![vbatch(&["a", "a"], &[1, 1], &[None, None])]);
        assert_eq!(collect_rows(&plan).await.len(), 1);
    }

    /// No `dedup_tiebreak` ⇒ byte-for-byte the old behaviour: keep-FIRST, even
    /// though a later copy would have won under keep-greatest.
    #[tokio::test(flavor = "multi_thread")]
    async fn no_tiebreak_stays_keep_first() {
        let src = source(&[vec![vbatch(&["a", "a"], &[1, 1], &[Some(1), Some(9)])]], Some(col_asc("ts", 1)));
        let plan = DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], None, None).unwrap();
        assert_eq!(collect_rows(&plan).await, vec![("a".into(), 1, Some(1))]);
    }

    /// Unordered input has no run boundary, so keep-greatest buffers to
    /// end-of-stream rather than degrading to keep-first. It used to degrade,
    /// which under merge-on-read served the PRE-UPDATE row; the alternative the
    /// planner then took — forcing an ordering — inserted a blocking SortExec
    /// that exhausted the query pool on prod (2026-08-02). Buffering here is
    /// strictly cheaper than that sort, and unlike keep-first it is correct.
    ///
    /// A table WITHOUT a tiebreak still keeps first: nothing ranks its versions.
    #[tokio::test(flavor = "multi_thread")]
    async fn unordered_input_keeps_greatest_and_without_a_tiebreak_keeps_first() {
        let src = source(&[vec![vbatch(&["a", "a"], &[1, 1], &[Some(1), Some(9)])]], None);
        let plan = DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], Some("tb".into()), None).unwrap();
        assert_eq!(collect_rows(&plan).await, vec![("a".into(), 1, Some(9))], "the newest version must win even with no ordering");

        let src = source(&[vec![vbatch(&["a", "a"], &[1, 1], &[Some(1), Some(9)])]], None);
        let plan = DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], None, None).unwrap();
        assert_eq!(collect_rows(&plan).await, vec![("a".into(), 1, Some(1))], "no tiebreak ⇒ keep-first, unchanged");
    }

    /// `output_projection` still restores the requested columns.
    #[tokio::test(flavor = "multi_thread")]
    async fn applies_output_projection() {
        let src = source(&[vec![batch(&["a", "a", "b"], &[1, 2, 3])]], None);
        let plan = DedupExec::new(src, vec!["id".into()], Some(vec![1])).unwrap();
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "v");
        let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let b = futures::StreamExt::next(&mut stream).await.unwrap().unwrap();
        assert_eq!(b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(), &[1, 3]);
    }

    /// PlanProperties must not lie: one output partition, the remapped input
    /// ordering preserved (that is what keeps `ORDER BY … LIMIT` streaming).
    #[test]
    fn plan_properties() {
        let data = vec![vec![batch(&["a"], &[1])]];
        let ser = DedupExec::new(source(&data, Some(col_asc("id", 0))), vec!["id".into()], None).unwrap();
        assert_eq!(ser.properties().output_partitioning().partition_count(), 1);
        assert!(ser.properties().output_ordering().is_some());
        assert!(matches!(ser.required_input_distribution()[0], Distribution::SinglePartition));
        assert_eq!(ser.maintains_input_order(), vec![true]);
    }

    /// Lazy ordered source that counts the batches actually pulled — the probe
    /// for early-LIMIT termination. One `ts` run per batch, each with a
    /// duplicate pair so dedup has real work to do.
    #[derive(Debug)]
    struct CountingExec {
        schema: SchemaRef,
        props: Arc<PlanProperties>,
        pulled: Arc<std::sync::atomic::AtomicUsize>,
        n: i64,
    }

    impl CountingExec {
        // Not `new`: this hands back an erased `Arc<dyn ExecutionPlan>`, not Self.
        fn arc(pulled: Arc<std::sync::atomic::AtomicUsize>, n: i64) -> Arc<dyn ExecutionPlan> {
            use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
            let schema = vbatch(&["a"], &[0], &[Some(0)]).schema();
            let eq = datafusion::physical_expr::EquivalenceProperties::new_with_orderings(schema.clone(), [col_asc("ts", 1)]);
            let props = Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded));
            Arc::new(Self { schema, props, pulled, n })
        }
    }

    impl DisplayAs for CountingExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "CountingExec")
        }
    }

    #[async_trait::async_trait]
    impl ExecutionPlan for CountingExec {
        fn name(&self) -> &'static str {
            "CountingExec"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(self: Arc<Self>, _c: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, _p: usize, _c: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
            let pulled = self.pulled.clone();
            let stream = futures::stream::iter((0..self.n).map(move |t| {
                pulled.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(vbatch(&["a", "a"], &[t, t], &[Some(0), Some(1)]))
            }));
            Ok(Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream)))
        }
    }

    /// The load-bearing claim of the merge-on-read design: keep-greatest still
    /// streams. A `LIMIT` over a huge ordered input must terminate after pulling
    /// a handful of batches — if the operator buffered versions past a run
    /// boundary it would drain the whole source instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_limit_terminates_early() {
        use datafusion::physical_plan::{collect, limit::GlobalLimitExec};
        let pulled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let src = CountingExec::arc(pulled.clone(), 1_000_000);
        let dedup = Arc::new(DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], Some("tb".into()), None).unwrap());
        let plan = Arc::new(GlobalLimitExec::new(dedup, 0, Some(5)));
        let out = collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        assert_eq!(out.iter().map(|b| b.num_rows()).sum::<usize>(), 5);
        let n = pulled.load(std::sync::atomic::Ordering::SeqCst);
        assert!(n <= 8, "LIMIT 5 pulled {n} of 1M batches — the operator is not streaming");
    }

    /// Run state is bounded: after N runs the candidate map and the buffered
    /// batches hold only the open run, not O(scan).
    #[test]
    fn keep_greatest_run_state_is_bounded() {
        let in_schema = vbatch(&["a"], &[0], &[Some(0)]).schema();
        let mut d = Dedup {
            key_idxs: vec![1, 0],
            conv: RowConverter::new(vec![SortField::new(DataType::Int64), SortField::new(DataType::Utf8)]).unwrap(),
            output_projection: None,
            seen: SeenSet::default(),
            bound: Some(Bound { idx: 1, desc: false, last: None }),
            direct_string_key: None,
            greatest: Some(
                Greatest::new(2, in_schema.field(2).data_type(), MemoryConsumer::new("test").register(&Arc::new(TaskContext::default()).memory_pool().clone()))
                    .unwrap(),
            ),
        };
        for t in 0..200i64 {
            d.push(&vbatch(&["a", "b", "a"], &[t, t, t], &[Some(1), Some(1), Some(2)])).unwrap();
        }
        let g = d.greatest.as_ref().unwrap();
        assert_eq!(g.best.len(), 2, "only the open run's keys are held");
        assert_eq!(g.batches.len(), 1, "only the open run's batches are held");
        assert!(d.seen.is_empty(), "seen only holds overflow-flushed keys");
    }
}

#[cfg(test)]
mod ordering_probe_tests {
    use std::sync::Arc;

    use arrow::{
        array::TimestampMicrosecondArray,
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use datafusion::{
        physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr, expressions::Column},
        physical_plan::{
            ExecutionPlan, Partitioning, PlanProperties,
            execution_plan::{Boundedness, EmissionType},
        },
        prelude::SessionContext,
    };
    use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
    use futures::StreamExt;

    use super::{LegKind, OrderingProbeExec, ordering_violations_by_leg};

    /// A leg whose declared ordering is a LIE: it claims timestamp DESC and
    /// then hands back ascending rows. This is the shape the probe exists to
    /// catch — `DedupExec` counts the violation globally, but it sits above the
    /// union and cannot say WHICH leg produced it.
    fn lying_desc_leg(values: Vec<i64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(TimestampMicrosecondArray::from(values))]).unwrap();
        let src = MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap();
        let exec = Arc::new(DataSourceExec::new(Arc::new(src))) as Arc<dyn ExecutionPlan>;
        // Declare DESC regardless of the data — exactly what a stale/false
        // parquet footer does to a Delta scan.
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new("timestamp", 0))).desc()]).unwrap();
        let props = PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, [ordering]),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Arc::new(LyingOrder { inner: exec, props: Arc::new(props) })
    }

    #[derive(Debug)]
    struct LyingOrder {
        inner: Arc<dyn ExecutionPlan>,
        props: Arc<PlanProperties>,
    }
    impl datafusion::physical_plan::DisplayAs for LyingOrder {
        fn fmt_as(&self, _t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "LyingOrder")
        }
    }
    impl ExecutionPlan for LyingOrder {
        fn name(&self) -> &'static str {
            "LyingOrder"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.inner]
        }
        fn with_new_children(self: Arc<Self>, _c: Vec<Arc<dyn ExecutionPlan>>) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self, p: usize, c: Arc<datafusion::execution::TaskContext>,
        ) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
            self.inner.execute(p, c)
        }
    }

    async fn drain(plan: Arc<dyn ExecutionPlan>) {
        let ctx = SessionContext::new();
        let mut s = plan.execute(0, ctx.task_ctx()).unwrap();
        while let Some(b) = s.next().await {
            b.unwrap();
        }
    }

    /// The probe must name the leg that lied, and leave the innocent legs at
    /// zero — a diagnostic that blames everyone is no better than the global
    /// counter it is meant to disambiguate.
    #[tokio::test]
    async fn the_probe_names_the_leg_whose_declared_order_is_false() {
        let before = ordering_violations_by_leg();
        // Ascending rows behind a DESC claim: every step after the first is a violation.
        drain(Arc::new(OrderingProbeExec::new(lying_desc_leg(vec![1, 2, 3, 4]), LegKind::Delta))).await;
        let after = ordering_violations_by_leg();
        let delta = |k: &str| {
            let g = |v: &[(&'static str, u64); 2]| v.iter().find(|(n, _)| *n == k).unwrap().1;
            g(&after) - g(&before)
        };
        assert!(delta("delta") >= 3, "the lying delta leg must be attributed, got {}", delta("delta"));
        assert_eq!(delta("mem"), 0, "an innocent leg must not be blamed");
    }

    /// A leg that honours its claim must be silent, or the counter is noise.
    #[tokio::test]
    async fn an_honest_leg_reports_nothing() {
        let before = ordering_violations_by_leg();
        drain(Arc::new(OrderingProbeExec::new(lying_desc_leg(vec![9, 8, 7, 6]), LegKind::Mem))).await;
        let after = ordering_violations_by_leg();
        let g = |v: &[(&'static str, u64); 2]| v.iter().find(|(n, _)| *n == "mem").unwrap().1;
        assert_eq!(g(&after) - g(&before), 0, "descending rows honour a DESC claim — nothing to report");
    }

    /// `LegKind::sortable()` is what replaced the parallel `leg_sortable` mask.
    /// The Delta leg must never be sortable: an UPDATE writes a row's ORIGINAL
    /// timestamp into a NEW file, so its files overlap, and the blocking sort
    /// that "fixes" that exhausted the query pool (prod 2026-08-02).
    #[test]
    fn only_the_in_memory_legs_are_sortable() {
        assert!(LegKind::Mem.sortable());
        assert!(!LegKind::Delta.sortable(), "sorting the Delta leg at read time is the 2026-08-02 pool exhaustion");
    }
}

// ===== count_pushdown =====
// COUNT(*) pushdown from Delta add-action statistics.
//
// `SELECT COUNT(*) FROM t WHERE project_id = 'x' AND timestamp >= lo AND
// timestamp < hi` is the highest-frequency dashboard tile shape. When every
// gate below holds, the answer is `Σ stats.numRecords` over the project's
// files that lie FULLY inside the window — zero parquet IO. Any doubt →
// `Ok(None)` and the normal scan runs; this module may only ever *decline*,
// never approximate.
//
// Gates (all required):
// - plan is exactly `[Projection] ← Aggregate[count, no groups] ←
//   [Projection/SubqueryAlias]* ← [Filter] ← TableScan(ProjectRoutingTable)`
//   (row count is invariant under projection/alias; anything else — Limit,
//   Join, Union, other aggregates — bails);
// - predicates are exactly `project_id = <lit>` + both timestamp bounds
//   (a missing upper bound would race incoming MemBuffer writes);
// - MemBuffer holds no rows in the window (fully flushed);
// - table has no dedup keys OR every window partition is sweep-verified
//   clean (same fingerprint gate as the read-side dedup skip — duplicates
//   in Delta would inflate numRecords);
// - every in-window file's `[min,max]` timestamp lies fully inside the
//   window (boundary-straddling files bail — v1 has no hybrid scan);
// - no file carries a deletion vector (numRecords is pre-DV);
// - the table cannot hold merge-on-read tombstones (`tombstone_column`
//   declared *and* `version_append` on — a declared-but-dormant column can
//   hold none, so it does not disqualify the fast path).

use datafusion::{
    arrow::array::Int64Array,
    datasource::{DefaultTableSource, memory::MemorySourceConfig, source::DataSourceExec},
    logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, utils::split_conjunction},
    scalar::ScalarValue,
};
use tracing::debug;

use crate::database::Database;

fn count_result(plan: &LogicalPlan, total: u64) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let total = i64::try_from(total).map_err(|_| datafusion::error::DataFusionError::Execution("COUNT(*) exceeds Int64".to_string()))?;
    let out_schema: SchemaRef = Arc::new(plan.schema().as_arrow().clone());
    if out_schema.fields().len() != 1 || out_schema.field(0).data_type() != &DataType::Int64 {
        return Ok(None);
    }
    let batch = RecordBatch::try_new(out_schema.clone(), vec![Arc::new(Int64Array::from(vec![total]))])?;
    let source = MemorySourceConfig::try_new(&[vec![batch]], out_schema, None)?;
    Ok(Some(Arc::new(DataSourceExec::new(Arc::new(source)))))
}

/// Predicate classification for one conjunct.
enum Conjunct {
    ProjectId(String),
    TsLow(i64),
    TsHigh(i64),
    True,
}

fn literal_micros(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts), _), _) => Some(*ts),
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _), _) => Some(*ts / 1000),
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), _), _) => Some(*ts * 1000),
        Expr::Literal(ScalarValue::TimestampSecond(Some(ts), _), _) => Some(*ts * 1_000_000),
        Expr::Cast(c) => literal_micros(&c.expr),
        _ => None,
    }
}

fn classify_conjunct(e: &Expr) -> Option<Conjunct> {
    use crate::read::optimizers::{extract_utf8_string, is_col_through_cast, swap_comparison};
    match e {
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => Some(Conjunct::True),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // project_id = 'lit'
            if *op == Operator::Eq {
                return match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(c)) if c.name == "project_id" => {
                        extract_utf8_string(v).map(Conjunct::ProjectId)
                    }
                    _ => None,
                };
            }
            // timestamp bound (either operand order, cast-wrapped column ok)
            let (lit, op) = if is_col_through_cast(left, "timestamp") {
                (literal_micros(right)?, *op)
            } else if is_col_through_cast(right, "timestamp") {
                (literal_micros(left)?, swap_comparison(*op))
            } else {
                return None;
            };
            match op {
                // Normalize to an INCLUSIVE window: a file whose min/max sits
                // exactly on a strict bound would otherwise be counted whole
                // while the predicate excludes its boundary rows, so `>`/`<`
                // shrink by 1µs.
                Operator::GtEq => Some(Conjunct::TsLow(lit)),
                Operator::Gt => Some(Conjunct::TsLow(lit.checked_add(1)?)),
                Operator::LtEq => Some(Conjunct::TsHigh(lit)),
                Operator::Lt => Some(Conjunct::TsHigh(lit.checked_sub(1)?)),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Peel one alias layer, so `count(*) AS n` matches like `count(*)`.
fn unalias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias(a) => a.expr.as_ref(),
        _ => e,
    }
}

/// The matched query shape: table + project + inclusive window.
struct CountQuery {
    table_name: String,
    project_id: String,
    lo: i64,
    hi: i64,
}

/// Match the COUNT(*) shape and extract the (table, project, window).
fn match_count_plan(plan: &LogicalPlan) -> Option<CountQuery> {
    use datafusion::logical_expr::expr::AggregateFunction;
    // Root: optional Projection whose exprs are pass-through columns/aliases.
    let agg_plan = match plan {
        LogicalPlan::Projection(p) if p.expr.iter().all(|e| matches!(unalias(e), Expr::Column(_))) => p.input.as_ref(),
        LogicalPlan::Projection(_) => return None,
        _ => plan,
    };
    let LogicalPlan::Aggregate(agg) = agg_plan else { return None };
    if !agg.group_expr.is_empty() || agg.aggr_expr.len() != 1 {
        return None;
    }
    // count(*) / count(1) / count(non-null literal); no DISTINCT, no FILTER.
    let count_ok = match unalias(&agg.aggr_expr[0]) {
        Expr::AggregateFunction(AggregateFunction { func, params }) => {
            func.name() == "count"
                && !params.distinct
                && params.filter.is_none()
                && match params.args.as_slice() {
                    [] => true,
                    [Expr::Literal(v, _)] => !v.is_null(),
                    _ => false,
                }
        }
        _ => false,
    };
    if !count_ok {
        return None;
    }

    // Walk down: row count is invariant under Projection/SubqueryAlias.
    // Collect Filter predicates and (below) the TableScan's pushed filters.
    // Imperative: a descent that rebinds `node` and breaks with the scan has
    // no iterator form that reads better.
    let mut node = agg.input.as_ref();
    let mut preds: Vec<&Expr> = Vec::new();
    let scan = loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(a) => node = a.input.as_ref(),
            LogicalPlan::Filter(f) => {
                preds.extend(split_conjunction(&f.predicate));
                node = f.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => break scan,
            _ => return None, // Limit/Join/Union/... change or gate row count
        }
    };
    if scan.fetch.is_some() {
        return None;
    }
    // The provider must BE the routing table — a bare-name match alone would
    // let a session-created table (`CREATE TABLE s.otel_logs_and_spans ...`)
    // or any name-colliding provider be answered from the real Delta stats.
    scan.source.downcast_ref::<DefaultTableSource>().and_then(|src| src.table_provider.downcast_ref::<crate::database::ProjectRoutingTable>())?;

    // No dedup needed although the same conjunct commonly appears in both the
    // Filter node and the scan's pushed filters: every fold step below is
    // idempotent (equal project_id, max/min of an equal bound).
    let (project_id, lo, hi) = preds.into_iter().chain(scan.filters.iter().flat_map(split_conjunction)).try_fold(
        (None::<String>, None::<i64>, None::<i64>),
        |(project_id, lo, hi), p| {
            Some(match classify_conjunct(p)? {
                Conjunct::ProjectId(v) if project_id.as_ref().is_none_or(|prev| *prev == v) => (Some(v), lo, hi),
                Conjunct::TsLow(v) => (project_id, Some(lo.map_or(v, |prev| prev.max(v))), hi),
                Conjunct::TsHigh(v) => (project_id, lo, Some(hi.map_or(v, |prev| prev.min(v)))),
                Conjunct::True => (project_id, lo, hi),
                _ => return None,
            })
        },
    )?;
    let (lo, hi) = finalize_window(lo, hi, chrono::Utc::now().timestamp_micros())?;
    Some(CountQuery { table_name: scan.table_name.table().to_string(), project_id: project_id?, lo, hi })
}

/// Resolve the count window's bounds. A lower bound is required (an unbounded
/// count would scan everything). A one-sided `timestamp > cutoff` (no upper
/// bound) is the common dashboard/export shape: treat the missing upper bound as
/// `now`, keeping the window bounded so the dedup-clean date check stays cheap.
/// The downstream MemBuffer-flushed + dedup-clean gates keep the result exact —
/// an unflushed or dirty recent tail simply bails to a normal scan. Returns
/// `None` when there's no lower bound or the window is empty (`lo > hi`).
fn finalize_window(lo: Option<i64>, hi: Option<i64>, now: i64) -> Option<(i64, i64)> {
    let lo = lo?;
    let hi = hi.unwrap_or(now);
    (lo <= hi).then_some((lo, hi))
}

/// Pure summing logic over per-file `(min_ts, max_ts, num_records)` stats:
/// `Some(total)` when every window-overlapping file is FULLY inside `[lo,hi]`,
/// `None` when a boundary file straddles (or stats are missing → caller
/// passes `None` fields → bail).
fn sum_fully_contained(files: impl IntoIterator<Item = (Option<i64>, Option<i64>, Option<i64>)>, lo: i64, hi: i64) -> Option<u64> {
    files.into_iter().try_fold(0u64, |total, (min, max, records)| {
        let (min, max, records) = (min?, max?, records?);
        if max < lo || min > hi {
            Some(total) // fully outside — contributes nothing
        } else if min >= lo && max <= hi {
            total.checked_add(u64::try_from(records).ok()?)
        } else {
            None // straddles the boundary — needs a real scan
        }
    })
}

/// Attempt the pushdown. `Ok(None)` = not applicable, plan normally.
pub async fn try_count_pushdown(plan: &LogicalPlan, database: &Arc<Database>) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    if !database.config().maintenance.timefusion_count_pushdown {
        return Ok(None);
    }
    let Some(q) = match_count_plan(plan) else { return Ok(None) };
    // Only tables served by ProjectRoutingTable qualify (system tables like
    // timefusion_stats share the session but not the storage model).
    let Some(schema) = crate::schema::get_schema(&q.table_name) else { return Ok(None) };
    if schema.tombstones_possible()
        && let Some(total) = try_logical_count(database, &q, schema).await
    {
        debug!("count_pushdown: answered {}/{} [{}, {}] = {} from logical-count index", q.project_id, q.table_name, q.lo, q.hi, total);
        crate::observability::record_logical_count_pushdown_used();
        return count_result(plan, total);
    }
    // Tombstones make `stats.numRecords` an over-count in exactly the way
    // deletion vectors do (below), except invisibly: a merge-on-read DELETE is
    // an APPEND, so the file stats count both the tombstone version and the
    // live version it retires. There is no per-file statistic that could tell
    // us how many — the answer only exists after the dedup+filter the scan
    // does. Silent wrong answer if we don't decline.
    //
    // Gated on tombstones being *possible*, not merely declared: a declared
    // column with `version_append: false` can hold none, and declining there
    // would trade the whole stats fast path (the highest-frequency dashboard
    // tile) for an unbounded scan that buys nothing. See
    // `TableSchema::tombstones_possible` for the ordering invariant that makes
    // this sound.
    if schema.tombstones_possible() {
        return Ok(None);
    }

    // Gate: window fully flushed (no MemBuffer rows in range).
    if let Some(layer) = database.buffered_layer()
        && layer.mem_buffer().has_rows_in_range(&q.project_id, &q.table_name, q.lo, q.hi)
    {
        return Ok(None);
    }

    // Resolve ONCE and hold a single read guard across the dedup-clean gate
    // and the stats sum — the fingerprint verdict applies to exactly the
    // snapshot being summed (no check-then-use window). The MemBuffer gate
    // above intentionally precedes this: rows leave the buffer only AFTER
    // their commit swapped the shared table, so anything missing from mem at
    // gate time is present in this (later) snapshot.
    let Ok(table_ref) = database.resolve_table(&q.project_id, &q.table_name).await else {
        return Ok(None);
    };
    let total = {
        let table = table_ref.read().await;
        // Gate: duplicates provably absent for the window, in THIS snapshot.
        if !schema.dedup_keys.is_empty() && !database.dedup_window_clean(&table, &q.project_id, &q.table_name, (q.lo, q.hi)).granted() {
            return Ok(None);
        }
        let Ok(snapshot) = table.snapshot() else { return Ok(None) };
        let Ok(actions) = snapshot.add_actions_table(true) else { return Ok(None) };
        let Some(total) = sum_from_actions(&actions, &q) else {
            debug!("count_pushdown: bailed for {}/{} (stats gaps or boundary files)", q.project_id, q.table_name);
            return Ok(None);
        };
        total
    };

    debug!("count_pushdown: answered {}/{} [{}, {}] = {} from add-action stats", q.project_id, q.table_name, q.lo, q.hi, total);
    crate::observability::record_count_pushdown_used();
    count_result(plan, total)
}

async fn try_logical_count(database: &Arc<Database>, q: &CountQuery, schema: &crate::schema::TableSchema) -> Option<u64> {
    if schema.dedup_keys != ["timestamp", "id"] {
        return None;
    }
    let tiebreak = schema.dedup_tiebreak.as_deref()?;
    let deleted = schema.tombstone_column.as_deref()?;
    let hi = q.hi.checked_add(1)?;
    let lo_date = chrono::DateTime::from_timestamp_micros(q.lo)?.date_naive();
    let hi_date = chrono::DateTime::from_timestamp_micros(q.hi)?.date_naive();
    let days = (hi_date - lo_date).num_days();
    // The resident budget guarantees four daily indexes at once, which covers
    // a three-day window crossing four UTC dates. Deeper scans keep the
    // authoritative plan instead of churning the hot dashboard working set.
    if !(0..=3).contains(&days) {
        return None;
    }
    let dates: Vec<_> = (0..=days).map(|offset| lo_date + chrono::Duration::days(offset)).collect();

    // Snapshot the unflushed tail before the Delta snapshot. Flush removes a
    // batch only after publishing its table snapshot, so a transitioning row
    // appears in at least one leg; an equal winner in both is a no-op overlay.
    let filters = vec![
        datafusion::logical_expr::col("timestamp").gt_eq(datafusion::logical_expr::lit(ScalarValue::TimestampMicrosecond(Some(q.lo), Some("UTC".into())))),
        datafusion::logical_expr::col("timestamp").lt(datafusion::logical_expr::lit(ScalarValue::TimestampMicrosecond(Some(hi), Some("UTC".into())))),
    ];
    let (mem_batches, mem_ranges) = match database.buffered_layer() {
        Some(layer) => {
            let mem = layer.query(&q.project_id, &q.table_name, &filters).ok()?;
            let mem_ranges = layer.get_bucket_ranges(&q.project_id, &q.table_name);
            (mem, mem_ranges)
        }
        None => (Vec::new(), Vec::new()),
    };
    let table_ref = database.resolve_table(&q.project_id, &q.table_name).await.ok()?;
    let (indexes, missing, added_files, stale_dates, delta_snapshot, log_store) = {
        let table = table_ref.read().await;
        let delta_snapshot = Arc::new(table.snapshot().ok()?.snapshot().clone());
        let paths: Vec<String> = delta_snapshot.log_data().iter().map(|file| file.path().to_string()).collect();
        let mut indexes = Vec::with_capacity(dates.len());
        let mut missing = Vec::new();
        let mut added_files = Vec::new();
        let mut stale_dates = Vec::new();
        for date in &dates {
            let date_string = date.to_string();
            let files: std::collections::HashSet<_> =
                crate::database::dedup_partition_paths(paths.iter().cloned(), &q.project_id, &date_string).into_iter().collect();
            match database.logical_count_memory_for_files(&q.project_id, &q.table_name, &date_string, &files) {
                Some((index, mut added)) => {
                    indexes.push((*date, index));
                    if !added.is_empty() {
                        stale_dates.push(date_string);
                    }
                    added_files.append(&mut added);
                }
                None => missing.push(date_string),
            }
        }
        (indexes, missing, added_files, stale_dates, delta_snapshot, table.log_store())
    };
    if !missing.is_empty() {
        for date in missing {
            database.schedule_logical_count_build(&q.project_id, &q.table_name, &date, false);
        }
        return None;
    }

    let columns = crate::read::LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak, deleted };
    // Keep the synchronous append delta small. The full rebuild is already
    // single-flight; a large gap falls back to authoritative DedupExec until
    // the new base is ready instead of moving that scan onto every query.
    if added_files.len() > crate::read::MAX_APPEND_OVERLAY_FILES {
        for date in stale_dates {
            database.schedule_logical_count_build(&q.project_id, &q.table_name, &date, true);
        }
        return None;
    }
    let authoritative_batches = mem_batches;
    let covered_ranges = crate::write::mem_buffer::merge_ranges(mem_ranges);
    let delta_batches = database.logical_count_overlay_batches(delta_snapshot, log_store, added_files, columns).await.ok()?;
    indexes.into_iter().try_fold(0u64, |total, (date, index)| {
        let day_lo = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        let day_hi = date.succ_opt()?.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        let input =
            crate::read::LogicalCountOverlay { authoritative_batches: &authoritative_batches, delta_batches: &delta_batches, covered_ranges: &covered_ranges };
        let count = index.count_with_covered_overlay(input, q.lo.max(day_lo), hi.min(day_hi), columns).ok()?;
        total.checked_add(count)
    })
}

/// Extract `(min_ts, max_ts, numRecords)` for this project's files from the
/// flattened add-actions batch and sum the fully-contained ones. `None` on
/// any missing column/stat, DV presence, or boundary straddle.
/// A timestamp stats column as microseconds, or `None` when it is absent or not
/// a timestamp. Shared with the rollup coverage fingerprint, which reads the
/// same per-file span this pushdown does.
pub(crate) fn ts_micros_column(b: &RecordBatch, name: &str) -> Option<Int64Array> {
    use datafusion::arrow::{array::TimestampMicrosecondArray, compute::cast, datatypes::TimeUnit};
    let c = b.column_by_name(name)?;
    matches!(c.data_type(), DataType::Timestamp(_, _)).then_some(())?;
    let c = cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
    Some(c.as_any().downcast_ref::<TimestampMicrosecondArray>()?.reinterpret_cast())
}

fn sum_from_actions(actions: &RecordBatch, q: &CountQuery) -> Option<u64> {
    let ts_micros_col = ts_micros_column;
    // Deletion vectors make numRecords an over-count — bail if ANY file has
    // one (column families vary by writer; check every dv-prefixed column).
    let any_dv = actions
        .schema()
        .fields()
        .iter()
        .enumerate()
        .any(|(i, f)| f.name().starts_with("deletionVector") && actions.column(i).null_count() < actions.num_rows());
    if any_dv {
        return None;
    }
    let pid = actions.column_by_name("partition.project_id")?.as_any().downcast_ref::<StringArray>()?;
    let records = actions.column_by_name("stats.numRecords")?.as_any().downcast_ref::<Int64Array>()?;
    let min_ts = ts_micros_col(actions, "stats.minValues.timestamp")?;
    let max_ts = ts_micros_col(actions, "stats.maxValues.timestamp")?;
    let rows = (0..actions.num_rows())
        .filter(|&i| pid.is_valid(i) && pid.value(i) == q.project_id)
        .map(|i| (min_ts.is_valid(i).then(|| min_ts.value(i)), max_ts.is_valid(i).then(|| max_ts.value(i)), records.is_valid(i).then(|| records.value(i))));
    sum_fully_contained(rows, q.lo, q.hi)
}

#[cfg(test)]
mod count_pushdown_tests {
    use super::*;

    #[test]
    fn fully_contained_sums_and_boundary_bails() {
        // two inside, one outside → sum of inside
        let f = |min, max, n| (Some(min), Some(max), Some(n));
        assert_eq!(sum_fully_contained([f(10, 20, 5), f(30, 40, 7), f(100, 200, 9)], 0, 50), Some(12));
        // straddling file → None
        assert_eq!(sum_fully_contained([f(10, 20, 5), f(45, 60, 7)], 0, 50), None);
        // missing stats on an overlapping file → None
        assert_eq!(sum_fully_contained([(Some(10), None, Some(5))], 0, 50), None);
        // missing stats on a file we can't even place → None (conservative)
        assert_eq!(sum_fully_contained([(None, Some(5), Some(1))], 100, 200), None);
        // empty file set → 0
        assert_eq!(sum_fully_contained([], 0, 50), Some(0));
    }

    #[test]
    fn finalize_window_defaults_open_upper_bound_to_now() {
        // Two-sided window passes through unchanged.
        assert_eq!(finalize_window(Some(10), Some(50), 999), Some((10, 50)));
        // One-sided `timestamp > cutoff` → upper bound becomes now.
        assert_eq!(finalize_window(Some(10), None, 999), Some((10, 999)));
        // No lower bound → not eligible (would scan everything).
        assert_eq!(finalize_window(None, Some(50), 999), None);
        // Empty window (lo > hi) → None.
        assert_eq!(finalize_window(Some(60), Some(50), 999), None);
    }
}

// ===== logical_count_index =====
// Exact logical row counts for merge-on-read tables.
//
// Cache tiers remove IO, but they cannot answer `COUNT(*)` without decoding
// and resolving every physical version. This index stores only the winning
// version of each dedup key and a timestamp histogram. It is derived data:
// callers must bind it to a Delta snapshot fingerprint and invalidate or
// advance it with every write before using it for a query.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::TimestampMicrosecondArray,
    datatypes::{Field, Schema, TimeUnit},
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

const FORMAT_VERSION: &str = "1";
const META_VERSION: &str = "tf.logical_count.version";
const META_FINGERPRINT: &str = "tf.logical_count.fingerprint";
const META_FILES: &str = "tf.logical_count.files";
pub(crate) const MAX_APPEND_OVERLAY_FILES: usize = 16;
const DISK_PARTITIONS_PER_PROJECT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Winner {
    tiebreak: Option<i64>,
    deleted: bool,
}

/// Packed immutable winner metadata. IDs live in one shared byte arena, so a
/// production partition pays no allocator/header cost per key.
#[derive(Debug, Clone, Copy)]
struct PackedWinner {
    timestamp: i64,
    tiebreak: i64,
    id_offset: u32,
    id_len: u16,
    flags: u8,
    _padding: u8,
}

const FLAG_TIEBREAK_PRESENT: u8 = 1;
const FLAG_DELETED: u8 = 2;

#[derive(Debug, Clone, Default)]
struct PackedIndex {
    winners: Vec<PackedWinner>,
    ids: Vec<u8>,
    /// One entry per live winner, sorted. Two binary searches answer any exact
    /// time window without the former per-timestamp BTree node overhead.
    live_timestamps: Vec<i64>,
}

/// Mutable build form plus a packed immutable query form. Builders use the
/// hash map for exact version resolution, then `finalize` releases it before
/// cache admission.
#[derive(Debug, Clone, Default)]
pub struct LogicalCountIndex {
    winners: HashMap<Box<[u8]>, Winner, ahash::RandomState>,
    packed: Option<PackedIndex>,
    key_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalCountColumns<'a> {
    pub timestamp: &'a str,
    pub id: &'a str,
    pub tiebreak: &'a str,
    pub deleted: &'a str,
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalCountOverlay<'a> {
    pub authoritative_batches: &'a [RecordBatch],
    pub delta_batches: &'a [RecordBatch],
    pub covered_ranges: &'a [(i64, i64)],
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CountPartition {
    pub project_id: String,
    pub table_name: String,
    /// UTC partition date (`YYYY-MM-DD`).
    pub date: String,
}

#[derive(Debug)]
struct CachedPartition {
    fingerprint: u64,
    files: Arc<std::collections::HashSet<String>>,
    index: Arc<LogicalCountIndex>,
    estimated_bytes: usize,
    last_access: AtomicU64,
}

/// Process-local front for persistent `.arrow` logical-count partitions.
///
/// Missing, stale, corrupt, or partially-written entries are ordinary cache
/// misses. Query code must fall back to the authoritative scan in every such
/// case; this cache never weakens correctness.
#[derive(Debug)]
pub struct LogicalCountCache {
    root: PathBuf,
    entries: dashmap::DashMap<CountPartition, CachedPartition>,
    max_resident_bytes: usize,
    resident_bytes: AtomicUsize,
    access_clock: AtomicU64,
    admission_lock: parking_lot::Mutex<()>,
}

impl LogicalCountCache {
    pub fn new(root: PathBuf, max_resident_bytes: usize) -> Self {
        Self {
            root,
            entries: dashmap::DashMap::new(),
            max_resident_bytes,
            resident_bytes: AtomicUsize::new(0),
            access_clock: AtomicU64::new(1),
            admission_lock: parking_lot::Mutex::new(()),
        }
    }

    fn next_access(&self) -> u64 {
        self.access_clock.fetch_add(1, Ordering::Relaxed)
    }

    fn insert_memory(&self, key: CountPartition, fingerprint: u64, files: std::collections::HashSet<String>, index: Arc<LogicalCountIndex>) -> bool {
        let estimated_bytes = index.estimated_heap_bytes();
        if estimated_bytes > self.max_resident_bytes {
            return false;
        }
        let _guard = self.admission_lock.lock();
        if let Some((_, old)) = self.entries.remove(&key) {
            self.resident_bytes.fetch_sub(old.estimated_bytes, Ordering::Relaxed);
        }
        while self.resident_bytes.load(Ordering::Relaxed).saturating_add(estimated_bytes) > self.max_resident_bytes {
            let Some(victim) = self.entries.iter().min_by_key(|entry| entry.last_access.load(Ordering::Relaxed)).map(|entry| entry.key().clone()) else {
                break;
            };
            if let Some((_, evicted)) = self.entries.remove(&victim) {
                self.resident_bytes.fetch_sub(evicted.estimated_bytes, Ordering::Relaxed);
            }
        }
        self.entries
            .insert(key, CachedPartition { fingerprint, files: Arc::new(files), index, estimated_bytes, last_access: AtomicU64::new(self.next_access()) });
        self.resident_bytes.fetch_add(estimated_bytes, Ordering::Relaxed);
        true
    }

    fn safe_component(value: &str) -> String {
        // Encode every byte, including otherwise-safe ASCII. Replacing unsafe
        // bytes with `_` made distinct tenants such as `a/b` and `a_b` share a
        // path; if their 64-bit file-set fingerprints happened to match, one
        // tenant could consume the other's exact-count index. Hex is injective
        // over UTF-8 bytes and contains no path separators.
        use std::fmt::Write;
        let mut encoded = String::with_capacity(value.len() * 2);
        for byte in value.bytes() {
            write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    fn path(&self, key: &CountPartition) -> PathBuf {
        self.root
            .join(Self::safe_component(&key.table_name))
            .join(Self::safe_component(&key.project_id))
            .join(format!("{}.arrow", Self::safe_component(&key.date)))
    }

    /// Install only after a builder has covered the complete physical
    /// partition represented by `fingerprint`.
    pub fn install(&self, key: CountPartition, fingerprint: u64, files: Vec<String>, mut index: LogicalCountIndex) -> Result<()> {
        index.finalize()?;
        let path = self.path(&key);
        index.save(&path, fingerprint, &files)?;
        Self::prune_disk_partitions(&path);
        anyhow::ensure!(
            self.insert_memory(key, fingerprint, files.into_iter().collect(), Arc::new(index)),
            "logical-count partition exceeds the resident cache budget"
        );
        Ok(())
    }

    fn prune_disk_partitions(installed: &Path) {
        let Some(parent) = installed.parent() else { return };
        let Ok(entries) = std::fs::read_dir(parent) else { return };
        let mut completed: Vec<PathBuf> =
            entries.flatten().map(|entry| entry.path()).filter(|path| path.extension().is_some_and(|extension| extension == "arrow")).collect();
        completed.sort();
        let remove = completed.len().saturating_sub(DISK_PARTITIONS_PER_PROJECT);
        for stale in completed.into_iter().take(remove) {
            let _ = std::fs::remove_file(stale);
        }
    }

    /// Return a complete exact index for this fingerprint, loading its Arrow
    /// file lazily after restart. Any validation failure is a cache miss.
    pub fn get(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        if let Some(index) = self.get_memory(key, fingerprint) {
            return Some(index);
        }
        let (loaded, files) = LogicalCountIndex::load(&self.path(key), fingerprint).ok()?;
        let loaded = Arc::new(loaded);
        self.insert_memory(key.clone(), fingerprint, files.into_iter().collect(), Arc::clone(&loaded)).then_some(loaded)
    }

    /// Background restart warm-up for an append-only successor snapshot.
    /// A removed base file refuses the load; newly added files are handled by
    /// the query's narrow append overlay.
    pub fn load_appendable(&self, key: &CountPartition, current_files: &std::collections::HashSet<String>) -> Option<usize> {
        if let Some(entry) = self.entries.get(key) {
            if entry.files.is_subset(current_files) {
                entry.last_access.store(self.next_access(), Ordering::Relaxed);
                return Some(current_files.len() - entry.files.len());
            }
            drop(entry);
            self.invalidate(key);
        }
        let (index, fingerprint, files) = LogicalCountIndex::load_file(&self.path(key)).ok()?;
        let files: std::collections::HashSet<String> = files.into_iter().collect();
        if !files.is_subset(current_files) {
            return None;
        }
        let added = current_files.len() - files.len();
        self.insert_memory(key.clone(), fingerprint, files, Arc::new(index)).then_some(added)
    }

    /// Query-path lookup that never performs filesystem IO. Disk loading and
    /// index construction belong to a bounded background builder; a cold SQL
    /// request must fall back to the authoritative scan instead of blocking a
    /// PGWire worker on a multi-million-key Arrow file.
    pub fn get_memory(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        let entry = self.entries.get(key)?;
        if entry.fingerprint != fingerprint {
            return None;
        }
        entry.last_access.store(self.next_access(), Ordering::Relaxed);
        Some(Arc::clone(&entry.index))
    }

    /// Return a snapshot whose indexed file set is an exact subset of the
    /// caller's current partition. The difference is safe to scan as a narrow
    /// append overlay. Any removal/rewrite declines because the base may then
    /// count rows no longer present.
    pub fn get_memory_appendable(
        &self, key: &CountPartition, current_files: &std::collections::HashSet<String>,
    ) -> Option<(Arc<LogicalCountIndex>, Vec<String>)> {
        let entry = self.entries.get(key)?;
        if !entry.files.is_subset(current_files) {
            return None;
        }
        entry.last_access.store(self.next_access(), Ordering::Relaxed);
        let added = current_files.difference(&entry.files).cloned().collect();
        Some((Arc::clone(&entry.index), added))
    }

    /// Remove only the memory front. The stale Arrow file remains harmless:
    /// its embedded fingerprint prevents it from being reused after a write.
    pub fn invalidate(&self, key: &CountPartition) {
        let _guard = self.admission_lock.lock();
        if let Some((_, removed)) = self.entries.remove(key) {
            self.resident_bytes.fetch_sub(removed.estimated_bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn stats(&self) -> (usize, usize, usize) {
        (self.entries.len(), self.resident_bytes.load(Ordering::Relaxed), self.max_resident_bytes)
    }
}

fn key(timestamp: i64, id: &str) -> Box<[u8]> {
    let mut out = Vec::with_capacity(8 + id.len());
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(id.as_bytes());
    out.into_boxed_slice()
}

fn packed_id<'a>(ids: &'a [u8], winner: &PackedWinner) -> &'a [u8] {
    let start = winner.id_offset as usize;
    let end = start + usize::from(winner.id_len);
    &ids[start..end]
}

impl LogicalCountIndex {
    pub fn new() -> Self {
        Self { winners: HashMap::default(), packed: None, key_bytes: 0 }
    }

    /// Apply one physical version. Returns whether it changed the logical row.
    ///
    /// Ordering exactly matches `DedupExec`'s primitive keep-greatest rule:
    /// `None` sorts below every non-null value, and an equal tiebreak does not
    /// replace the existing winner.
    pub fn apply(&mut self, timestamp: i64, id: &str, tiebreak: Option<i64>, deleted: bool) -> bool {
        assert!(self.packed.is_none(), "cannot mutate a finalized logical-count index");
        let encoded = key(timestamp, id);
        let old = self.winners.get(encoded.as_ref()).copied();
        if old.is_some_and(|winner| tiebreak <= winner.tiebreak) {
            return false;
        }

        if old.is_none() {
            self.key_bytes = self.key_bytes.saturating_add(encoded.len());
        }
        self.winners.insert(encoded, Winner { tiebreak, deleted });
        true
    }

    /// Convert the allocation-heavy builder map into the resident query form.
    pub fn finalize(&mut self) -> Result<()> {
        if self.packed.is_some() {
            return Ok(());
        }
        let winners = std::mem::take(&mut self.winners);
        let mut packed = PackedIndex {
            winners: Vec::with_capacity(winners.len()),
            ids: Vec::with_capacity(self.key_bytes.saturating_sub(winners.len().saturating_mul(8))),
            live_timestamps: Vec::with_capacity(winners.len()),
        };
        for (key, winner) in winners {
            let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
            let id = &key[8..];
            let id_offset = u32::try_from(packed.ids.len()).context("logical-count ID arena exceeds 4GiB")?;
            let id_len = u16::try_from(id.len()).context("logical-count ID exceeds 65535 bytes")?;
            packed.ids.extend_from_slice(id);
            let mut flags = 0;
            if winner.tiebreak.is_some() {
                flags |= FLAG_TIEBREAK_PRESENT;
            }
            if winner.deleted {
                flags |= FLAG_DELETED;
            } else {
                packed.live_timestamps.push(timestamp);
            }
            packed.winners.push(PackedWinner { timestamp, tiebreak: winner.tiebreak.unwrap_or_default(), id_offset, id_len, flags, _padding: 0 });
        }
        let ids = &packed.ids;
        packed.winners.sort_unstable_by(|left, right| left.timestamp.cmp(&right.timestamp).then_with(|| packed_id(ids, left).cmp(packed_id(ids, right))));
        packed.live_timestamps.sort_unstable();
        self.key_bytes = packed.ids.len();
        self.packed = Some(packed);
        Ok(())
    }

    fn winner(&self, timestamp: i64, id: &str) -> Option<Winner> {
        if let Some(packed) = &self.packed {
            let pos = packed
                .winners
                .binary_search_by(|candidate| candidate.timestamp.cmp(&timestamp).then_with(|| packed_id(&packed.ids, candidate).cmp(id.as_bytes())))
                .ok()?;
            let winner = packed.winners[pos];
            Some(Winner { tiebreak: (winner.flags & FLAG_TIEBREAK_PRESENT != 0).then_some(winner.tiebreak), deleted: winner.flags & FLAG_DELETED != 0 })
        } else {
            self.winners.get(key(timestamp, id).as_ref()).copied()
        }
    }

    /// Apply the four-column narrow form emitted by a count-index build:
    /// `timestamp`, `id`, version tiebreak, tombstone marker.
    pub fn apply_batch(&mut self, batch: &RecordBatch, columns: LogicalCountColumns<'_>) -> Result<usize> {
        let timestamps = timestamp_values(batch, columns.timestamp)?;
        let ids = StringValues::new(batch, columns.id)?;
        let tiebreaks = timestamp_values(batch, columns.tiebreak)?;
        let deleted = batch
            .column_by_name(columns.deleted)
            .with_context(|| format!("logical-count batch missing {}", columns.deleted))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .with_context(|| format!("logical-count {} is not Boolean", columns.deleted))?;
        let mut changed = 0;
        for row in 0..batch.num_rows() {
            // A bounded timestamp predicate never matches NULL, so such a row
            // contributes to no count index. ID is a declared dedup key and
            // must not be silently discarded if corrupt input violates it.
            if timestamps.is_null(row) {
                continue;
            }
            let id = ids.value(row).with_context(|| format!("logical-count {} is NULL at row {row}", columns.id))?;
            let tiebreak = (!tiebreaks.is_null(row)).then(|| tiebreaks.value(row));
            let is_deleted = !deleted.is_null(row) && deleted.value(row);
            changed += usize::from(self.apply(timestamps.value(row), id, tiebreak, is_deleted));
        }
        Ok(changed)
    }

    /// Exact base count with unflushed MemBuffer versions overlaid. The base
    /// index is never cloned or mutated: only keys present in `batches` occupy
    /// the temporary map, so the cost follows the hot tail rather than the
    /// full 24-hour cardinality.
    pub fn count_with_overlay(&self, batches: &[RecordBatch], lo: i64, hi: i64, columns: LogicalCountColumns<'_>) -> Result<u64> {
        self.count_with_covered_overlay(LogicalCountOverlay { authoritative_batches: batches, delta_batches: &[], covered_ranges: &[] }, lo, hi, columns)
    }

    /// Count after applying the same coverage contract as the ordinary
    /// `mem ∪ Delta` scan. Rows in `authoritative_batches` replace covered
    /// Delta rows; `delta_batches` are newly appended Delta files and remain
    /// subject to the same range gate as the indexed base.
    pub fn count_with_covered_overlay(&self, input: LogicalCountOverlay<'_>, lo: i64, hi: i64, columns: LogicalCountColumns<'_>) -> Result<u64> {
        let LogicalCountOverlay { authoritative_batches, delta_batches, covered_ranges } = input;
        #[derive(Clone, Copy)]
        struct Overlay {
            base: Option<Winner>,
            current: Winner,
            timestamp: i64,
        }

        let base_visible = |timestamp: i64| !covered_ranges.iter().any(|&(start, end)| (start..end).contains(&timestamp));
        let mut overlay: HashMap<Box<[u8]>, Overlay, ahash::RandomState> = HashMap::default();
        for (batches, authoritative) in [(authoritative_batches, true), (delta_batches, false)] {
            for batch in batches {
                let timestamps = timestamp_values(batch, columns.timestamp)?;
                let ids = StringValues::new(batch, columns.id)?;
                let tiebreaks = timestamp_values(batch, columns.tiebreak)?;
                let deleted = batch
                    .column_by_name(columns.deleted)
                    .with_context(|| format!("logical-count overlay missing {}", columns.deleted))?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .with_context(|| format!("logical-count overlay {} is not Boolean", columns.deleted))?;
                for row in 0..batch.num_rows() {
                    if timestamps.is_null(row) {
                        continue;
                    }
                    let timestamp = timestamps.value(row);
                    let id = ids.value(row).with_context(|| format!("logical-count overlay {} is NULL at row {row}", columns.id))?;
                    let encoded = key(timestamp, id);
                    let candidate =
                        Winner { tiebreak: (!tiebreaks.is_null(row)).then(|| tiebreaks.value(row)), deleted: !deleted.is_null(row) && deleted.value(row) };
                    if !authoritative && !base_visible(timestamp) {
                        continue;
                    }
                    match overlay.entry(encoded) {
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            if candidate.tiebreak > entry.get().current.tiebreak {
                                entry.get_mut().current = candidate;
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            let base = self.winner(timestamp, id).filter(|_| base_visible(timestamp));
                            let current = base.filter(|winner| winner.tiebreak >= candidate.tiebreak).unwrap_or(candidate);
                            entry.insert(Overlay { base, current, timestamp });
                        }
                    }
                }
            }
        }

        let mut count = i128::from(self.count(lo, hi));
        if !covered_ranges.is_empty() {
            count -= i128::from(self.count_covered_live(lo, hi, covered_ranges));
        }
        for state in overlay.values().filter(|state| (lo..hi).contains(&state.timestamp)) {
            count += i128::from(!state.current.deleted) - i128::from(state.base.is_some_and(|winner| !winner.deleted));
        }
        u64::try_from(count).context("logical-count overlay produced an invalid negative/overflow count")
    }

    fn count_covered_live(&self, lo: i64, hi: i64, covered_ranges: &[(i64, i64)]) -> u64 {
        let visible = |timestamp: i64, winner: Winner| {
            !winner.deleted && (lo..hi).contains(&timestamp) && covered_ranges.iter().any(|&(start, end)| (start..end).contains(&timestamp))
        };
        let count = if let Some(packed) = &self.packed {
            packed
                .winners
                .iter()
                .filter(|winner| {
                    visible(
                        winner.timestamp,
                        Winner { tiebreak: (winner.flags & FLAG_TIEBREAK_PRESENT != 0).then_some(winner.tiebreak), deleted: winner.flags & FLAG_DELETED != 0 },
                    )
                })
                .count()
        } else {
            self.winners
                .iter()
                .filter(|(key, winner)| {
                    let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
                    visible(timestamp, **winner)
                })
                .count()
        };
        u64::try_from(count).expect("logical-count partition length fits u64")
    }

    /// Exact live row count in the half-open interval `[lo, hi)`.
    pub fn count(&self, lo: i64, hi: i64) -> u64 {
        if lo >= hi {
            return 0;
        }
        if let Some(packed) = &self.packed {
            let start = packed.live_timestamps.partition_point(|timestamp| *timestamp < lo);
            let end = packed.live_timestamps.partition_point(|timestamp| *timestamp < hi);
            return u64::try_from(end - start).expect("logical-count partition length fits u64");
        }
        u64::try_from(
            self.winners
                .iter()
                .filter(|(key, winner)| {
                    let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
                    !winner.deleted && (lo..hi).contains(&timestamp)
                })
                .count(),
        )
        .expect("logical-count partition length fits u64")
    }

    pub fn logical_rows(&self) -> u64 {
        if let Some(packed) = &self.packed {
            u64::try_from(packed.live_timestamps.len()).expect("logical-count partition length fits u64")
        } else {
            u64::try_from(self.winners.values().filter(|winner| !winner.deleted).count()).expect("logical-count partition length fits u64")
        }
    }

    pub fn physical_keys(&self) -> usize {
        self.packed.as_ref().map_or(self.winners.len(), |packed| packed.winners.len())
    }

    /// Conservative resident-size estimate for build admission. Includes the
    /// key bytes plus allocation/hash-table overhead; it intentionally rounds
    /// up because this map lives outside DataFusion's tracked memory pool.
    pub fn estimated_heap_bytes(&self) -> usize {
        if let Some(packed) = &self.packed {
            return packed
                .winners
                .capacity()
                .saturating_mul(std::mem::size_of::<PackedWinner>())
                .saturating_add(packed.ids.capacity())
                .saturating_add(packed.live_timestamps.capacity().saturating_mul(std::mem::size_of::<i64>()));
        }
        self.key_bytes.saturating_add(self.winners.len().saturating_mul(64))
    }

    /// Atomically persist the derived winners as Arrow IPC.
    ///
    /// The compact timestamp histogram is rebuilt on load; persisting one
    /// canonical winner table avoids two sources of truth. A fingerprint is
    /// embedded in schema metadata and must match the caller's current Delta
    /// snapshot before the file can be served.
    pub fn save(&self, path: &Path, fingerprint: u64, files: &[String]) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create logical-count cache directory {}", parent.display()))?;
        }
        let mut metadata = HashMap::new();
        metadata.insert(META_VERSION.to_string(), FORMAT_VERSION.to_string());
        metadata.insert(META_FINGERPRINT.to_string(), fingerprint.to_string());
        metadata.insert(META_FILES.to_string(), serde_json::to_string(files).context("serialize logical-count file set")?);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("id", DataType::Utf8, false),
                Field::new("tiebreak", DataType::Int64, true),
                Field::new("deleted", DataType::Boolean, false),
            ],
            metadata,
        ));
        let tmp = path.with_extension(format!("arrow.tmp-{}", uuid::Uuid::new_v4()));
        let write = || -> Result<()> {
            let file = File::create(&tmp).with_context(|| format!("create logical-count cache {}", tmp.display()))?;
            let mut writer = FileWriter::try_new(file, schema.as_ref())?;
            // A production day can contain tens of millions of keys. Building
            // and sorting one second full-sized row vector here doubled the
            // index's peak memory during warm-up. IPC order is irrelevant to
            // correctness, so stream bounded batches directly from the map.
            const WRITE_ROWS: usize = 64 * 1024;
            let mut rows = Vec::with_capacity(WRITE_ROWS);
            let mut write_rows = |rows: &mut Vec<(i64, &str, Winner)>| -> Result<()> {
                if rows.is_empty() {
                    return Ok(());
                }
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                        Arc::new(Int64Array::from(rows.iter().map(|row| row.2.tiebreak).collect::<Vec<_>>())),
                        Arc::new(BooleanArray::from(rows.iter().map(|row| row.2.deleted).collect::<Vec<_>>())),
                    ],
                )?;
                writer.write(&batch)?;
                rows.clear();
                Ok(())
            };
            if let Some(packed) = &self.packed {
                for winner in &packed.winners {
                    let id = std::str::from_utf8(packed_id(&packed.ids, winner)).context("logical-count key contains non-UTF8 id")?;
                    rows.push((
                        winner.timestamp,
                        id,
                        Winner { tiebreak: (winner.flags & FLAG_TIEBREAK_PRESENT != 0).then_some(winner.tiebreak), deleted: winner.flags & FLAG_DELETED != 0 },
                    ));
                    if rows.len() == WRITE_ROWS {
                        write_rows(&mut rows)?;
                    }
                }
            } else {
                for (key, winner) in &self.winners {
                    let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
                    let id = std::str::from_utf8(&key[8..]).context("logical-count key contains non-UTF8 id")?;
                    rows.push((timestamp, id, *winner));
                    if rows.len() == WRITE_ROWS {
                        write_rows(&mut rows)?;
                    }
                }
            }
            write_rows(&mut rows)?;
            writer.finish()?;
            std::fs::rename(&tmp, path).with_context(|| format!("publish logical-count cache {}", path.display()))?;
            Ok(())
        };
        if let Err(error) = write() {
            let _ = std::fs::remove_file(&tmp);
            return Err(error);
        }
        Ok(())
    }

    /// Load only when the file belongs to the caller's exact snapshot.
    pub fn load(path: &Path, expected_fingerprint: u64) -> Result<(Self, Vec<String>)> {
        let (index, fingerprint, files) = Self::load_file(path)?;
        if fingerprint != expected_fingerprint {
            bail!("logical-count cache fingerprint mismatch: cached={fingerprint} current={expected_fingerprint}");
        }
        Ok((index, files))
    }

    fn load_file(path: &Path) -> Result<(Self, u64, Vec<String>)> {
        let file = File::open(path).with_context(|| format!("open logical-count cache {}", path.display()))?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();
        if schema.metadata().get(META_VERSION).map(String::as_str) != Some(FORMAT_VERSION) {
            bail!("unsupported logical-count cache format");
        }
        let expected_fields = [
            ("timestamp", &DataType::Int64, false),
            ("id", &DataType::Utf8, false),
            ("tiebreak", &DataType::Int64, true),
            ("deleted", &DataType::Boolean, false),
        ];
        if schema.fields().len() != expected_fields.len()
            || schema
                .fields()
                .iter()
                .zip(expected_fields)
                .any(|(field, (name, data_type, nullable))| field.name() != name || field.data_type() != data_type || field.is_nullable() != nullable)
        {
            bail!("logical-count cache has an incompatible Arrow schema");
        }
        let fingerprint = schema
            .metadata()
            .get(META_FINGERPRINT)
            .context("logical-count cache missing fingerprint")?
            .parse::<u64>()
            .context("logical-count cache fingerprint is invalid")?;
        let files: Vec<String> = serde_json::from_str(schema.metadata().get(META_FILES).context("logical-count cache missing file set")?)
            .context("logical-count cache file set is invalid")?;

        let mut index = Self::new();
        for batch in reader {
            let batch = batch?;
            let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().context("logical-count timestamp column has wrong type")?;
            let ids = batch.column(1).as_any().downcast_ref::<StringArray>().context("logical-count id column has wrong type")?;
            let tiebreaks = batch.column(2).as_any().downcast_ref::<Int64Array>().context("logical-count tiebreak column has wrong type")?;
            let deleted = batch.column(3).as_any().downcast_ref::<BooleanArray>().context("logical-count deleted column has wrong type")?;
            if timestamps.null_count() != 0 || ids.null_count() != 0 || deleted.null_count() != 0 {
                bail!("logical-count cache contains NULL in a required column");
            }
            for row in 0..batch.num_rows() {
                let tiebreak = tiebreaks.is_valid(row).then(|| tiebreaks.value(row));
                index.apply(timestamps.value(row), ids.value(row), tiebreak, deleted.value(row));
            }
        }
        index.finalize()?;
        Ok((index, fingerprint, files))
    }
}

fn timestamp_values<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a TimestampMicrosecondArray> {
    let column = batch.column_by_name(name).with_context(|| format!("logical-count batch missing {name}"))?;
    match column.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            column.as_any().downcast_ref::<TimestampMicrosecondArray>().with_context(|| format!("logical-count {name} is not TimestampMicrosecond"))
        }
        other => bail!("logical-count {name} has unsupported type {other}"),
    }
}

enum StringValues<'a> {
    View(&'a StringViewArray),
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl<'a> StringValues<'a> {
    fn new(batch: &'a RecordBatch, name: &str) -> Result<Self> {
        let column = batch.column_by_name(name).with_context(|| format!("logical-count batch missing {name}"))?;
        if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
            Ok(Self::View(values))
        } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(values))
        } else if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            Ok(Self::Large(values))
        } else {
            bail!("logical-count {name} has unsupported type {}", column.data_type())
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::View(values) => (!values.is_null(row)).then(|| values.value(row)),
            Self::Utf8(values) => (!values.is_null(row)).then(|| values.value(row)),
            Self::Large(values) => (!values.is_null(row)).then(|| values.value(row)),
        }
    }
}

#[cfg(test)]
mod logical_count_index_tests {
    use super::*;

    fn versions(rows: &[(i64, &str, Option<i64>, Option<bool>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("id", DataType::Utf8View, true),
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("deleted", DataType::Boolean, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|row| Some(row.0)).collect::<Vec<_>>()).with_timezone("UTC")),
                Arc::new(StringViewArray::from(rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>())),
                Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|row| row.2).collect::<Vec<_>>()).with_timezone("UTC")),
                Arc::new(BooleanArray::from(rows.iter().map(|row| row.3).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn resolves_duplicates_updates_and_tombstones_exactly() {
        let mut index = LogicalCountIndex::new();
        assert!(index.apply(10, "a", None, false));
        assert!(!index.apply(10, "a", None, false), "equal version is a duplicate");
        assert!(index.apply(10, "a", Some(2), false), "newer update remains one live row");
        assert_eq!(index.logical_rows(), 1);

        assert!(index.apply(10, "a", Some(3), true));
        assert_eq!(index.logical_rows(), 0);
        assert!(!index.apply(10, "a", Some(2), false), "stale update cannot resurrect a tombstone");
        assert!(index.apply(10, "a", Some(4), false));
        assert_eq!(index.logical_rows(), 1);
        assert_eq!(index.physical_keys(), 1);
    }

    #[test]
    fn arbitrary_ranges_use_exact_boundary_timestamps() {
        let mut index = LogicalCountIndex::new();
        for (timestamp, id) in [(-1, "neg"), (0, "zero"), (59_999_999, "left"), (60_000_000, "right"), (120_000_000, "end")] {
            index.apply(timestamp, id, Some(1), false);
        }
        assert_eq!(index.count(0, 120_000_000), 3);
        assert_eq!(index.count(1, 60_000_000), 1);
        assert_eq!(index.count(-1, 1), 2);
        assert_eq!(index.count(120_000_000, 120_000_001), 1);
        assert_eq!(index.count(5, 5), 0);
    }

    #[test]
    fn multiple_ids_at_one_timestamp_track_delete_transitions() {
        let mut index = LogicalCountIndex::new();
        index.apply(42, "a", Some(1), false);
        index.apply(42, "b", Some(1), false);
        index.apply(42, "c", Some(1), true);
        assert_eq!(index.count(42, 43), 2);
        index.apply(42, "a", Some(2), true);
        assert_eq!(index.count(42, 43), 1);
    }

    #[test]
    fn randomized_versions_and_ranges_match_a_reference_model() {
        let mut index = LogicalCountIndex::new();
        let mut reference: HashMap<(i64, String), Winner> = HashMap::new();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..20_000 {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let timestamp = i64::try_from(state % 300).unwrap() * 1_000_000 - 100_000_000;
            let id = format!("id-{}", (state >> 12) % 200);
            let tiebreak = (!(state >> 24).is_multiple_of(50)).then(|| i64::try_from((state >> 32) % 1_000).unwrap());
            let deleted = state & 7 == 0;
            index.apply(timestamp, &id, tiebreak, deleted);
            let winner = reference.entry((timestamp, id)).or_insert(Winner { tiebreak, deleted });
            if tiebreak > winner.tiebreak {
                *winner = Winner { tiebreak, deleted };
            }
        }

        for n in 0..200i64 {
            let lo = -120_000_000 + n * 1_700_000;
            let hi = lo + 37_000_001;
            let expected = reference.iter().filter(|((timestamp, _), winner)| (lo..hi).contains(timestamp) && !winner.deleted).count() as u64;
            assert_eq!(index.count(lo, hi), expected, "range [{lo}, {hi})");
        }
    }

    #[test]
    fn packed_form_preserves_exact_ranges_and_bounds_resident_bytes() {
        let mut index = LogicalCountIndex::new();
        for value in 0..100_000i64 {
            let id = format!("01234567-89ab-cdef-0123-{value:012}");
            index.apply(value, &id, Some(value), value % 11 == 0);
        }
        index.finalize().unwrap();

        assert_eq!(index.physical_keys(), 100_000);
        assert_eq!(index.logical_rows(), 90_909);
        assert_eq!(index.count(25_000, 75_000), 45_454);
        assert!(index.winners.is_empty(), "the allocation-heavy build map must be released");
        assert!(index.estimated_heap_bytes() < 7_000_000, "packed 36-byte IDs should stay below 70 bytes/key");
    }

    #[test]
    fn narrow_batches_build_and_overlay_unflushed_versions_exactly() {
        let columns = LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak: "updated_at", deleted: "deleted" };
        let mut index = LogicalCountIndex::new();
        index.apply_batch(&versions(&[(10, "a", Some(1), Some(false)), (20, "b", Some(1), None), (30, "gone", Some(2), Some(true))]), columns).unwrap();
        assert_eq!(index.count(0, 100), 2);

        // a is tombstoned, b gets a stale no-op, gone is resurrected, and c
        // is a new unflushed key. A repeated Delta+Mem copy with an equal
        // tiebreak remains one logical row.
        let tail = versions(&[
            (10, "a", Some(3), Some(true)),
            (20, "b", Some(0), Some(true)),
            (20, "b", Some(1), Some(false)),
            (30, "gone", Some(4), Some(false)),
            (40, "c", Some(1), None),
        ]);
        assert_eq!(index.count_with_overlay(&[tail], 0, 100, columns).unwrap(), 3);
        assert_eq!(index.logical_rows(), 2, "overlay must not mutate the persistent base");
    }

    #[test]
    fn covered_overlay_replaces_delta_rows_like_the_union_scan() {
        let columns = LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak: "updated_at", deleted: "deleted" };
        let mut index = LogicalCountIndex::new();
        index.apply(10, "old", Some(1), false);
        index.apply(20, "newer-delta", Some(5), false);
        index.finalize().unwrap();
        let mem = [versions(&[(10, "old", Some(2), Some(true)), (20, "newer-delta", Some(3), Some(true))])];

        let ranges = [(0, 50)];
        let input = LogicalCountOverlay { authoritative_batches: &mem, delta_batches: &[], covered_ranges: &ranges };
        assert_eq!(index.count_with_covered_overlay(input, 0, 100, columns).unwrap(), 0);
    }

    #[test]
    fn arrow_cache_round_trip_is_exact_and_snapshot_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("project/date.arrow");
        let mut index = LogicalCountIndex::new();
        index.apply(10, "a", None, false);
        index.apply(10, "a", Some(2), true);
        index.apply(60_000_001, "b", Some(3), false);
        let files = vec!["date=2026-08-04/a.parquet".to_string()];
        index.save(&path, 99, &files).unwrap();

        let (loaded, loaded_files) = LogicalCountIndex::load(&path, 99).unwrap();
        assert_eq!(loaded_files, files);
        assert_eq!(loaded.physical_keys(), 2);
        assert_eq!(loaded.logical_rows(), 1);
        assert_eq!(loaded.count(60_000_000, 60_000_002), 1);
        assert!(LogicalCountIndex::load(&path, 100).unwrap_err().to_string().contains("fingerprint mismatch"));
        assert!(!path.with_extension("arrow.tmp").exists());
    }

    #[test]
    fn arrow_cache_streams_more_than_one_write_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.arrow");
        let mut index = LogicalCountIndex::new();
        for value in 0..70_000 {
            index.apply(value, &value.to_string(), Some(value), false);
        }
        index.save(&path, 1, &["large.parquet".into()]).unwrap();
        let (loaded, _) = LogicalCountIndex::load(&path, 1).unwrap();
        assert_eq!(loaded.physical_keys(), 70_000);
        assert_eq!(loaded.count(0, 70_000), 70_000);
    }

    #[test]
    fn cache_lazily_loads_only_matching_partition_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let key = CountPartition { project_id: "p/unsafe".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        let mut index = LogicalCountIndex::new();
        index.apply(42, "id", Some(1), false);
        cache.install(key.clone(), 7, vec!["a.parquet".into()], index).unwrap();
        assert_eq!(cache.get(&key, 7).unwrap().logical_rows(), 1);
        assert!(cache.get(&key, 8).is_none());

        let restarted = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        assert_eq!(restarted.get(&key, 7).unwrap().count(0, 100), 1);
        let current = ["a.parquet".to_string(), "b.parquet".to_string()].into_iter().collect();
        let (_, added) = restarted.get_memory_appendable(&key, &current).unwrap();
        assert_eq!(added, vec!["b.parquet"]);
        let append_restart = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        assert_eq!(append_restart.load_appendable(&key, &current), Some(1));
        assert_eq!(append_restart.get_memory_appendable(&key, &current).unwrap().1, vec!["b.parquet"]);
        let far_ahead: std::collections::HashSet<_> =
            std::iter::once("a.parquet".to_string()).chain((0..=MAX_APPEND_OVERLAY_FILES).map(|i| format!("new-{i}.parquet"))).collect();
        assert_eq!(append_restart.load_appendable(&key, &far_ahead), Some(MAX_APPEND_OVERLAY_FILES + 1));
        let rewritten = ["replacement.parquet".to_string()].into_iter().collect();
        assert!(restarted.get_memory_appendable(&key, &rewritten).is_none(), "a removed base file must fail closed");
        restarted.invalidate(&key);
        assert!(restarted.get(&key, 8).is_none());
        assert!(dir.path().join("6f74656c/702f756e73616665/323032362d30382d3034.arrow").exists());
    }

    #[test]
    fn cache_paths_cannot_alias_distinct_partition_names() {
        let cache = LogicalCountCache::new(PathBuf::from("unused"), usize::MAX);
        let slash = CountPartition { project_id: "a/b".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let underscore = CountPartition { project_id: "a_b".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        assert_ne!(cache.path(&slash), cache.path(&underscore));
    }

    #[test]
    fn resident_cache_evicts_the_least_recent_partition_within_budget() {
        let dir = tempfile::tempdir().unwrap();
        let key = |project: &str| CountPartition { project_id: project.into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let mut first = LogicalCountIndex::new();
        first.apply(1, "a", Some(1), false);
        first.finalize().unwrap();
        let per_entry = first.estimated_heap_bytes();
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), per_entry);
        cache.install(key("a"), 1, vec!["a.parquet".into()], first).unwrap();
        assert!(cache.get_memory(&key("a"), 1).is_some());

        let mut second = LogicalCountIndex::new();
        second.apply(2, "b", Some(1), false);
        cache.install(key("b"), 2, vec!["b.parquet".into()], second).unwrap();
        assert!(cache.get_memory(&key("a"), 1).is_none());
        assert!(cache.get_memory(&key("b"), 2).is_some());
        assert!(cache.resident_bytes.load(Ordering::Relaxed) <= per_entry);
    }

    #[test]
    fn disk_cache_keeps_only_the_newest_completed_daily_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        for day in 1..=DISK_PARTITIONS_PER_PROJECT + 3 {
            let key = CountPartition { project_id: "p".into(), table_name: "otel".into(), date: format!("2026-08-{day:02}") };
            cache.install(key, u64::try_from(day).unwrap(), Vec::new(), LogicalCountIndex::new()).unwrap();
        }
        let project_dir = dir.path().join(LogicalCountCache::safe_component("otel")).join(LogicalCountCache::safe_component("p"));
        let files: Vec<_> = std::fs::read_dir(project_dir).unwrap().flatten().map(|entry| entry.path()).collect();
        assert_eq!(files.len(), DISK_PARTITIONS_PER_PROJECT);
        assert!(files.iter().all(|path| path.extension().is_some_and(|extension| extension == "arrow")));
    }
}

// ===== statistics =====
use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use datafusion::{
    arrow::compute::sum,
    common::{Statistics, stats::Precision},
};
use deltalake::DeltaTable;
use lru::LruCache;
use tokio::sync::RwLock;
use tracing::info;

const DEFAULT_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(50).unwrap();

/// Cache entry for basic table statistics
#[derive(Clone, Debug)]
pub struct CachedStatistics {
    pub stats: Statistics,
    pub timestamp: Instant,
    pub version: u64,
}

/// Simplified statistics extractor for Delta Lake tables: row count and byte
/// size only, cached per `(project_id, table_name)` and keyed on Delta version.
#[derive(Debug)]
pub struct DeltaStatisticsExtractor {
    cache: RwLock<LruCache<String, CachedStatistics>>,
    cache_ttl: Duration,
    page_row_limit: usize,
}

impl DeltaStatisticsExtractor {
    pub fn new(cache_size: usize, cache_ttl_seconds: u64, page_row_limit: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap_or(DEFAULT_CACHE_SIZE))),
            cache_ttl: Duration::from_secs(cache_ttl_seconds),
            page_row_limit,
        }
    }

    /// Extract basic statistics from a Delta table (row count and byte size only)
    pub async fn extract_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        let cache_key = cache_key(project_id, table_name);
        let version = table.version().unwrap_or(0);

        if let Some(stats) =
            self.cache.read().await.peek(&cache_key).filter(|c| c.version == version && c.timestamp.elapsed() < self.cache_ttl).map(|c| c.stats.clone())
        {
            debug!(%cache_key, version, "statistics cache hit");
            return Ok(stats);
        }

        let (num_files, num_rows, total_byte_size) = table_stats(table, self.page_row_limit)?;
        let stats = Statistics { num_rows: Precision::Inexact(num_rows), total_byte_size: Precision::Exact(total_byte_size), column_statistics: vec![] };

        info!(%cache_key, num_rows, total_byte_size, num_files, "extracted basic statistics");

        self.cache.write().await.put(cache_key, CachedStatistics { stats: stats.clone(), timestamp: Instant::now(), version });

        Ok(stats)
    }

    pub async fn clear_cache(&self) {
        self.cache.write().await.clear();
        info!("Statistics cache cleared");
    }

    /// Drop the cached entry for one table so the next extraction recomputes it.
    pub async fn invalidate(&self, project_id: &str, table_name: &str) {
        let cache_key = cache_key(project_id, table_name);
        if let Some(removed) = self.cache.write().await.pop(&cache_key) {
            debug!(%cache_key, version = removed.version, "invalidated statistics");
        }
    }

    /// Get cache statistics for monitoring: `(used, capacity)`
    pub async fn get_cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        (cache.len(), cache.cap().get())
    }
}

fn cache_key(project_id: &str, table_name: &str) -> String {
    format!("{project_id}:{table_name}")
}

/// Table-level `(files, rows, bytes)` summed from the flattened add-actions batch.
/// Falls back to `files × page_row_limit` when the snapshot carries no
/// `stats.numRecords` column.
fn table_stats(table: &DeltaTable, page_row_limit: usize) -> Result<(usize, usize, usize)> {
    let snapshot = table.snapshot().context("Failed to get Delta table snapshot")?;
    let actions = snapshot.add_actions_table(true).with_context(|| format!("Failed to get add actions for table at {}", table.table_url()))?;

    // `None` distinguishes "column absent" (→ fallback) from "present but empty/unsummable" (→ 0).
    let sum_i64 = |name| actions.column_by_name(name).map(|c| c.as_any().downcast_ref::<Int64Array>().and_then(sum).unwrap_or(0).max(0) as usize);

    let num_files = actions.num_rows();
    let rows = sum_i64("stats.numRecords").unwrap_or_else(|| num_files.saturating_mul(page_row_limit));
    Ok((num_files, rows, sum_i64("size_bytes").unwrap_or(0)))
}

// ===== hll =====
// HyperLogLog: a mergeable, bounded distinct-count sketch.
//
// The point is the same one t-digest makes for percentiles: `COUNT(DISTINCT x)`
// is not decomposable, so a rollup cannot store it and every dashboard tile that
// asks for one falls back to a raw scan of the whole window. A sketch IS
// decomposable — union is associative and commutative — so a distinct count can
// be pre-aggregated per bucket and folded across buckets, dimensions and rollup
// tiers at read time.
//
// Two representations behind one type:
//
// * **Sparse** — the exact set of hashes, up to [`SPARSE_MAX`]. The estimate is
//   then the set size, i.e. EXACT. Most dashboard groups (distinct users in a
//   minute, distinct services on a span) never leave this mode, so the common
//   case is not an approximation at all.
// * **Dense** — [`M`] one-byte registers once the exact set outgrows the dense
//   encoding. Standard error is `1.04/sqrt(M)` ≈ 1.6%.
//
// Serialized sketches are PERSISTED in rollup tables and merged months later,
// so both the hash function and the wire format are frozen. [`hash_bytes`] is
// written out here rather than taken from `ahash`/`RandomState` precisely so a
// dependency bump cannot silently re-hash the world and inflate every stored
// cardinality. Changing `SEED`, `P`, or the tag bytes requires a new rollup
// spec name, exactly like changing a measure.

/// Register-index bits. 12 → 4096 registers, 4 KiB dense, ~1.6% standard error.
const P: u32 = 12;
/// Register count.
const M: usize = 1 << P;
/// Above this many exact hashes the sparse encoding costs more than the dense
/// one (`M` bytes), so it converts. 512 × 8 B = 4 KiB, matching `M`.
const SPARSE_MAX: usize = 512;

const TAG_SPARSE: u8 = 1;
const TAG_DENSE: u8 = 2;

const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// splitmix64's finalizer: two multiply-shift-xor rounds, full avalanche.
#[inline]
const fn mix(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Stable 64-bit hash. Frozen: see the module note on persistence.
///
/// Eight bytes per round (~0.6 cycles/byte) rather than FNV's one, which matters
/// because this runs once per row over columns like `context___trace_id`.
#[inline]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut acc = SEED ^ (bytes.len() as u64);
    let mut chunks = bytes.chunks_exact(8);
    for chunk in &mut chunks {
        acc = mix(acc ^ u64::from_le_bytes(chunk.try_into().expect("chunks_exact(8) yields 8 bytes")));
    }
    let remainder = chunks.remainder();
    if !remainder.is_empty() {
        let mut tail = [0u8; 8];
        tail[..remainder.len()].copy_from_slice(remainder);
        acc = mix(acc ^ u64::from_le_bytes(tail));
    }
    mix(acc)
}

/// A distinct-count sketch. `Default` is the empty sketch, estimating 0.
#[derive(Debug, Clone, PartialEq, educe::Educe)]
#[educe(Default)]
pub enum Hll {
    #[educe(Default)]
    Sparse(HashSet<u64>),
    Dense(Box<[u8; M]>),
}

/// Split a hash into its register index and the 1-based position of the first
/// set bit in the remaining suffix.
#[inline]
const fn register_of(hash: u64) -> (usize, u8) {
    let index = (hash >> (64 - P)) as usize;
    // `| 1` bounds rho at 64-P+1 without a branch: the sentinel bit stops the
    // count when the whole suffix is zero.
    let rho = ((hash << P) | 1).leading_zeros() as u8 + 1;
    (index, rho)
}

impl Hll {
    /// Add one pre-hashed value.
    pub fn insert_hash(&mut self, hash: u64) {
        match self {
            Self::Sparse(hashes) => {
                hashes.insert(hash);
                if hashes.len() > SPARSE_MAX {
                    self.densify();
                }
            }
            Self::Dense(registers) => {
                let (index, rho) = register_of(hash);
                registers[index] = registers[index].max(rho);
            }
        }
    }

    fn densify(&mut self) {
        let Self::Sparse(hashes) = self else { return };
        let mut registers = Box::new([0u8; M]);
        for &hash in hashes.iter() {
            let (index, rho) = register_of(hash);
            registers[index] = registers[index].max(rho);
        }
        *self = Self::Dense(registers);
    }

    /// Union. Associative and commutative, which is the whole reason this type
    /// can live in a rollup.
    pub fn merge(&mut self, other: &Self) {
        match (&mut *self, other) {
            (Self::Sparse(mine), Self::Sparse(theirs)) => {
                mine.extend(theirs.iter().copied());
                if mine.len() > SPARSE_MAX {
                    self.densify();
                }
            }
            (Self::Sparse(_), Self::Dense(_)) => {
                let mine = std::mem::replace(self, other.clone());
                self.merge(&mine);
            }
            (Self::Dense(mine), Self::Sparse(theirs)) => {
                for &hash in theirs.iter() {
                    let (index, rho) = register_of(hash);
                    mine[index] = mine[index].max(rho);
                }
            }
            (Self::Dense(mine), Self::Dense(theirs)) => {
                for (slot, &their) in mine.iter_mut().zip(theirs.iter()) {
                    *slot = (*slot).max(their);
                }
            }
        }
    }

    /// Estimated distinct count — exact while sparse.
    pub fn estimate(&self) -> u64 {
        let registers = match self {
            Self::Sparse(hashes) => return hashes.len() as u64,
            Self::Dense(registers) => registers,
        };
        let zeros = registers.iter().filter(|&&r| r == 0).count();
        // Linear counting is the better estimator while most registers are still
        // empty; it is also what keeps the seam at the sparse→dense boundary from
        // jumping.
        if zeros > 0 {
            let linear = M as f64 * (M as f64 / zeros as f64).ln();
            if linear <= 2.5 * M as f64 {
                return linear.round() as u64;
            }
        }
        // Flajolet's harmonic-mean estimator. `alpha` is the standard bias
        // constant for m >= 128.
        let alpha = 0.7213 / (1.0 + 1.079 / M as f64);
        // 2^-r by constructing the IEEE-754 exponent directly, not `powi`. The
        // sum runs over all M registers on every estimate, and `powi` is an
        // opaque call LLVM will not vectorize; this is widen-subtract-shift-
        // bitcast, which it turns into AVX2. Total for any `r: u8` — the
        // exponent field cannot underflow, so a corrupt payload is still a
        // finite positive number rather than UB.
        let harmonic: f64 = registers.iter().map(|&r| f64::from_bits((1023 - u64::from(r)) << 52)).sum();
        (alpha * (M * M) as f64 / harmonic).round() as u64
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Sparse(hashes) => {
                let mut out = Vec::with_capacity(1 + hashes.len() * 8);
                out.push(TAG_SPARSE);
                out.extend(hashes.iter().flat_map(|hash| hash.to_le_bytes()));
                out
            }
            Self::Dense(registers) => {
                let mut out = Vec::with_capacity(1 + M);
                out.push(TAG_DENSE);
                out.extend_from_slice(&**registers);
                out
            }
        }
    }

    /// Decode. A malformed payload is an error rather than an empty sketch: a
    /// silently-empty sketch would under-report a cardinality forever.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        match bytes {
            [] => Ok(Self::default()),
            [TAG_SPARSE, rest @ ..] if rest.len() % 8 == 0 => {
                Ok(Self::Sparse(rest.chunks_exact(8).map(|c| u64::from_le_bytes(c.try_into().expect("chunks_exact(8)"))).collect()))
            }
            [TAG_DENSE, rest @ ..] if rest.len() == M => {
                Ok(Self::Dense(Box::new(rest.try_into().map_err(|_| "hll: dense payload is not M bytes".to_string())?)))
            }
            [tag, ..] => Err(format!("hll: malformed sketch (tag {tag}, {} bytes)", bytes.len())),
        }
    }

    /// Heap footprint, for the accumulator's memory accounting.
    pub fn size(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::Sparse(hashes) => hashes.capacity() * size_of::<u64>(),
                Self::Dense(_) => M,
            }
    }
}

#[cfg(test)]
mod hll_tests {
    use super::*;

    fn sketch_of(range: std::ops::Range<u64>) -> Hll {
        let mut hll = Hll::default();
        for value in range {
            hll.insert_hash(hash_bytes(&value.to_le_bytes()));
        }
        hll
    }

    /// The sparse mode is not an approximation, which is what makes this safe to
    /// put behind the low-cardinality dashboard tiles (distinct services, distinct
    /// hosts) where a 2% error would be visible as a wrong integer.
    #[test]
    fn small_cardinalities_are_exact() {
        for n in [0u64, 1, 7, 100, SPARSE_MAX as u64] {
            assert_eq!(sketch_of(0..n).estimate(), n, "sparse must be exact at {n}");
        }
    }

    #[test]
    fn large_cardinalities_land_within_the_error_bound() {
        // 1.04/sqrt(4096) = 1.6% standard error; allow 3 sigma so the test is not
        // flaky against a hash-dependent but deterministic outcome.
        for n in [5_000u64, 50_000, 1_000_000] {
            let estimate = sketch_of(0..n).estimate() as f64;
            let error = (estimate - n as f64).abs() / n as f64;
            assert!(error < 0.05, "n={n}: estimated {estimate}, error {:.3}%", error * 100.0);
        }
    }

    /// Union must be order-independent and must not double-count the overlap —
    /// the property the whole rollup design rests on.
    #[test]
    fn merge_is_a_union_not_a_sum() {
        let (mut left, right) = (sketch_of(0..30_000), sketch_of(20_000..50_000));
        left.merge(&right);
        let error = (left.estimate() as f64 - 50_000.0).abs() / 50_000.0;
        assert!(error < 0.05, "overlapping union estimated {}, want ~50000", left.estimate());
    }

    #[test]
    fn merge_crosses_the_sparse_dense_boundary_in_both_directions() {
        let (small, large) = (sketch_of(0..10), sketch_of(0..20_000));
        for (mut a, b) in [(small.clone(), large.clone()), (large.clone(), small.clone())] {
            a.merge(&b);
            let error = (a.estimate() as f64 - 20_000.0).abs() / 20_000.0;
            assert!(error < 0.05, "estimated {} from a mixed-mode merge", a.estimate());
        }
    }

    /// Sketches are persisted and merged months later, so a round-trip through
    /// bytes must be bit-identical, and a truncated payload must be loud.
    #[test]
    fn round_trips_through_bytes() {
        for n in [0u64, 10, SPARSE_MAX as u64 + 1, 100_000] {
            let sketch = sketch_of(0..n);
            assert_eq!(Hll::from_bytes(&sketch.to_bytes()).unwrap(), sketch, "n={n}");
        }
        assert!(Hll::from_bytes(&[TAG_DENSE, 0, 0]).is_err());
        assert!(Hll::from_bytes(&[TAG_SPARSE, 0, 0, 0]).is_err());
        assert!(Hll::from_bytes(&[9, 9]).is_err());
        assert_eq!(Hll::from_bytes(&[]).unwrap(), Hll::default());
    }

    /// A dense sketch is bounded no matter how many rows it sees: that bound is
    /// what lets a rollup row carry one.
    #[test]
    fn serialized_size_is_bounded() {
        assert!(sketch_of(0..10_000_000).to_bytes().len() <= M + 1);
    }

    /// Frozen hash: a dependency bump or refactor that changes these values
    /// invalidates every stored sketch on S3.
    #[test]
    fn the_hash_is_frozen() {
        assert_eq!(hash_bytes(b""), 16294208416658607535);
        assert_eq!(hash_bytes(b"timefusion"), 10501298223482614002);
    }
}
