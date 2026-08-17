//! Read-side dedup operator (parity plan Defect 2 #1).
//!
//! TF's write path can leave physical duplicates of a `(id, timestamp)` row in
//! Delta (cross-flush `SaveMode::Append`, late prior-day DLQ replays the
//! background sweep hasn't reached yet), and under the merge-on-read model
//! (`docs/plans/2026-08-01-merge-on-read-dml.md`) an `UPDATE` deliberately
//! appends a *new version* of the row. `DedupExec` collapses those copies over
//! the routed + pruned MemBuffer ∪ Delta union so `COUNT(*)` is correct at query
//! time, and — when the table declares a `dedup_tiebreak` — so the surviving
//! copy is the newest version, independent of sweep timing.
//!
//! Why a physical operator and not an `AnalyzerRule` wrapping the `TableScan`:
//! a `Distinct::On` node between the `project_id` filter and
//! `ProjectRoutingTable::scan` blocks `push_down_filter`, so routing falls back
//! to `default_project` and time/partition pruning is lost (see the plan's
//! reverted-blocker note). Deduping here, after routing, avoids all of that.
//!
//! Implementation: one input partition (DataFusion inserts a
//! `CoalescePartitionsExec`) so a key can't be split across partitions, and a
//! streaming filter per batch. Only the (tiny) encoded key rows are held — never
//! the fat body/attributes payload — so it streams, supports downstream
//! early-LIMIT, and never trips Arrow's 2 GB string-offset limit.
//!
//! Two survivor policies:
//!
//! * **keep-first** (no tiebreak, or no usable input ordering): a `HashSet` of
//!   seen key-rows filters each batch in place. "First" is only ever *arrival*
//!   order — `CoalescePartitionsExec` interleaves the union's partitions
//!   nondeterministically — so which physical copy survives is not a guarantee
//!   (COUNT is).
//! * **keep-greatest** (`dedup_tiebreak` present in the input *and* bounded mode
//!   available): the row with the greatest tiebreak per key wins, NULL lowest,
//!   so a pre-existing row always loses to any new version. Streaming survives
//!   because equal dedup keys share the same `timestamp` (the key is
//!   `(timestamp, id)`), so all versions of a key live inside one bound run
//!   (`Bound`): candidates are held only until the run closes, then emitted in
//!   input position order. A run's batches are held (Arc clones) while it is
//!   open, capped by `RUN_BUFFER_MAX_BYTES`; on overflow the run is flushed
//!   early and its tail degrades to keep-first — never unbounded, never worse
//!   than the old behaviour.
//!
//! Without a bound, keep-greatest still runs but must buffer to end-of-stream
//! (one candidate per distinct key, plus the batches they reference). That was
//! previously refused as the 2026-07-21 wide-scan OOM shape, and unordered input
//! degraded to keep-first instead — but keep-first serves the PRE-UPDATE row,
//! which merge-on-read cannot tolerate. The planner's other option, forcing an
//! ordering the Delta leg cannot provide (merge-on-read writes a row's ORIGINAL
//! timestamp into a NEW file, so files overlap in time), inserted a blocking
//! SortExec that exhausted the 27.5GB query pool on prod 2026-08-02. Buffering a
//! hash is strictly cheaper than sorting the same rows, so unbounded
//! keep-greatest is the better of the two — and the only correct one. The wide
//! scans that motivated the original refusal are bounded upstream by the
//! wide-scan admission gate and the hot-leg byte budget.
//!
//! Single-partition only, deliberately. A hash-partitioned mode existed and was
//! deleted: a mode shootout over the real 89-column `otel_logs_and_spans`
//! schema (counting allocator, release) measured it 2–4× slower and ~90× peak
//! heap on wide rows (0% dup: 54ms/26MB serial vs 205ms/2383MB hash),
//! because dedup cost is per-ROW (RowConverter + ahash probe) while the
//! `RepartitionExec` that `Distribution::HashPartitioned` forces copies every
//! byte of every column. It can only win on narrow rows with heavy duplication,
//! which is not the steady state. Don't re-litigate.
//!
//! Dedup keys (and the tiebreak) must be present in the input; the caller
//! augments the pushed projection so they are, then `output_projection` restores
//! the requested columns (via `RecordBatch::project`, which preserves row count
//! for the empty `COUNT(*)` projection).

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

use crate::errors::arrow_err;

/// Seen-key set for the streaming dedup. Arrow's `Row`/`OwnedRow` hash and
/// compare purely on their encoded `data` bytes (the `RowConfig` is not part of
/// identity), so we key directly on the raw bytes: `Box<[u8]>` drops the
/// per-entry `RowConfig`/`Arc` word `.owned()` would carry, and `Borrow<[u8]>`
/// lets us probe with a borrowed `&[u8]` and allocate only on a miss (never once
/// per duplicate row). `ahash` replaces std's SipHash.
type SeenSet = HashSet<Box<[u8]>, ahash::RandomState>;

/// Cap on the batches held open for one keep-greatest run. A run is one
/// `timestamp` value, normally a handful of rows; this only fires on pathological
/// input (a whole scan at one timestamp), where it flushes the run early rather
/// than growing without bound.
const RUN_BUFFER_MAX_BYTES: usize = 64 * 1024 * 1024;

/// One unordered merge-on-read query may not monopolize the global query
/// pool. Bounded inputs release at each timestamp run; only the unbounded
/// correctness fallback needs this ceiling. Production previously had four
/// such consumers retain 4.8-9.6GB each and exhaust the 30GB pool.
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

/// Bounded-window dedup state (parity plan Point 3, Tier 2). When the input is
/// already sorted by a dedup-key column (`timestamp` leads the table sort
/// order), duplicates of a key are confined to a single bound-value run: two
/// rows with the same dedup key share the same `timestamp` (equal key ⇒ equal
/// bound), so they arrive contiguously. Clearing state each time the bound
/// *advances* past the current run caps it at O(distinct keys within one
/// timestamp value) instead of O(distinct over the whole scan) — the fix for
/// the multi-GB seen-set risk on wide historical scans, and the property that
/// lets keep-greatest emit without buffering the stream. Opportunistic only: we
/// never *require* the ordering (that could make EnforceSorting insert a
/// blocking SortExec over unsorted MemBuffer partitions and break streaming);
/// when the input isn't sorted, `detect_bound` returns `None` and dedup falls
/// back to the full-set keep-first path — always sound.
struct Bound {
    /// Bound column index within the input schema.
    idx: usize,
    /// True when the sort is descending (bound decreases down the stream).
    desc: bool,
    /// The current run's bound value; `None` until the first row.
    last: Option<i64>,
}

impl Bound {
    /// Move to row value `t`, returning true when doing so *closed* a previous
    /// run (state built for the old run may then be dropped). A run opens on the
    /// first row ever, or on a value strictly past `last` in the sort direction.
    /// `advance`'s comparison, attributed to a leg instead of the global
    /// counter. Kept beside it deliberately: two copies of this predicate that
    /// could disagree is exactly how a diagnostic ends up exonerating the
    /// guilty party.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LegKind {
    Mem,
    Hot,
    Delta,
}

impl LegKind {
    pub fn sortable(self) -> bool {
        !matches!(self, LegKind::Delta)
    }

    pub fn label(self) -> &'static str {
        match self {
            LegKind::Mem => "mem",
            LegKind::Hot => "hot",
            LegKind::Delta => "delta",
        }
    }

    fn counter(self) -> &'static std::sync::atomic::AtomicU64 {
        match self {
            LegKind::Mem => &ORDERING_VIOLATIONS_MEM,
            LegKind::Hot => &ORDERING_VIOLATIONS_HOT,
            LegKind::Delta => &ORDERING_VIOLATIONS_DELTA,
        }
    }
}

pub(crate) static ORDERING_VIOLATIONS_MEM: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) static ORDERING_VIOLATIONS_HOT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub(crate) static ORDERING_VIOLATIONS_DELTA: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn ordering_violations_by_leg() -> [(&'static str, u64); 3] {
    use std::sync::atomic::Ordering::Relaxed;
    [
        (LegKind::Mem.label(), ORDERING_VIOLATIONS_MEM.load(Relaxed)),
        (LegKind::Hot.label(), ORDERING_VIOLATIONS_HOT.load(Relaxed)),
        (LegKind::Delta.label(), ORDERING_VIOLATIONS_DELTA.load(Relaxed)),
    ]
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
pub struct OrderingProbeExec {
    inner: Arc<dyn ExecutionPlan>,
    leg: LegKind,
}

impl OrderingProbeExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, leg: LegKind) -> Self {
        Self { inner, leg }
    }
}

impl std::fmt::Debug for OrderingProbeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "OrderingProbeExec: leg={}", self.leg.label())
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
    crate::optimizers::downcast(se.expr.as_ref())
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
                    let owned = crate::mem_buffer::compact_batch(batch.clone());
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
            let g = |v: &[(&'static str, u64); 3]| v.iter().find(|(n, _)| *n == k).unwrap().1;
            g(&after) - g(&before)
        };
        assert!(delta("delta") >= 3, "the lying delta leg must be attributed, got {}", delta("delta"));
        assert_eq!(delta("mem"), 0, "an innocent leg must not be blamed");
        assert_eq!(delta("hot"), 0, "an innocent leg must not be blamed");
    }

    /// A leg that honours its claim must be silent, or the counter is noise.
    #[tokio::test]
    async fn an_honest_leg_reports_nothing() {
        let before = ordering_violations_by_leg();
        drain(Arc::new(OrderingProbeExec::new(lying_desc_leg(vec![9, 8, 7, 6]), LegKind::Mem))).await;
        let after = ordering_violations_by_leg();
        let g = |v: &[(&'static str, u64); 3]| v.iter().find(|(n, _)| *n == "mem").unwrap().1;
        assert_eq!(g(&after) - g(&before), 0, "descending rows honour a DESC claim — nothing to report");
    }

    /// `LegKind::sortable()` is what replaced the parallel `leg_sortable` mask.
    /// The Delta leg must never be sortable: an UPDATE writes a row's ORIGINAL
    /// timestamp into a NEW file, so its files overlap, and the blocking sort
    /// that "fixes" that exhausted the query pool (prod 2026-08-02).
    #[test]
    fn only_the_in_memory_legs_are_sortable() {
        assert!(LegKind::Mem.sortable() && LegKind::Hot.sortable());
        assert!(!LegKind::Delta.sortable(), "sorting the Delta leg at read time is the 2026-08-02 pool exhaustion");
    }
}
