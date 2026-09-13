//! Read-side merge-on-read deduplication for `(timestamp, id)` rows.
//!
//! Two survivor policies:
//!
//! * **keep-first** stores seen keys; arrival order chooses the physical copy.
//! * **keep-greatest** keeps the greatest tiebreak per key, with NULL lowest.
//!   Ordered input streams by timestamp run; each run is memory-capped.
//!
//! Unordered keep-greatest buffers to end-of-stream. It must never degrade to
//! keep-first: doing so can return the pre-update version.
//!
//! The operator stays single-partition, and the caller adds key/tiebreak
//! columns then restores the requested projection.

pub mod bloom_prune;
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

use crate::{database::scan_metric_names, observability::arrow_err};

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
    /// Is `a` strictly further along the declared direction than `b`?
    const fn ahead(&self, a: i64, b: i64) -> bool {
        if self.desc { a < b } else { a > b }
    }

    /// Advance the run bound, counting violations into `violations`. Returns
    /// true when this row OPENS a new run (moved forward and was not the first
    /// row). A value moving AGAINST the declared direction means the scan's
    /// advertised ordering is false (a lying parquet footer); dedup stays sound
    /// because the bound column stays in the dedup key.
    fn step(&mut self, t: i64, violations: &AtomicU64) -> bool {
        if self.last.is_some_and(|l| self.ahead(l, t)) {
            violations.fetch_add(1, Ordering::Relaxed);
        }
        self.last.is_none_or(|l| self.ahead(t, l)) && self.last.replace(t).is_some()
    }

    /// Advance against the global counter every dedup scan feeds.
    fn advance(&mut self, t: i64) -> bool {
        self.step(t, &ORDERING_VIOLATIONS)
    }
}

/// Which union leg a row came from, plus the sortability the plan builder
/// needs. The Delta leg must never be sorted at read time: an UPDATE writes a
/// row's ORIGINAL timestamp into a NEW file, so its files overlap and the
/// blocking sort that "fixes" that exhausts the query pool.
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

    fn counter(self) -> &'static AtomicU64 {
        match self {
            LegKind::Mem => &ORDERING_VIOLATIONS_MEM,
            LegKind::Delta => &ORDERING_VIOLATIONS_DELTA,
        }
    }
}

pub(crate) static ORDERING_VIOLATIONS_MEM: AtomicU64 = AtomicU64::new(0);
pub(crate) static ORDERING_VIOLATIONS_DELTA: AtomicU64 = AtomicU64::new(0);

pub fn ordering_violations_by_leg() -> [(&'static str, u64); 2] {
    [LegKind::Mem, LegKind::Delta].map(|leg| (leg.label(), leg.counter().load(Ordering::Relaxed)))
}

/// A file's row-timestamp span, as Delta add-action statistics report it.
///
/// `None` means the file carries no timestamp statistics. It is NOT "empty" and
/// must never be treated as disjoint from anything.
pub(crate) type FileSpan = Option<(i64, i64)>;

/// May a set of files a sweep proved duplicate-free skip `DedupExec`, given the
/// files in the same scan that were NOT proved clean?
///
/// **The rule, applied PER FILE.** A certified file may skip iff no uncertified
/// file's timestamp span overlaps ITS span. Per file rather than per set, so one
/// neighbouring file cannot poison every certified file behind it.
///
/// **Why it holds.** The dedup key is `(timestamp, id)` and merge-on-read
/// re-appends preserve the original row's `timestamp`, so every version and
/// tombstone of a row carries that row's timestamp. A duplicate of a certified
/// row therefore lies inside the certified span and any file holding it must
/// overlap that span. Duplicates *within* the certified set are excluded by
/// construction: the sweep proved that set clean together.
///
/// **Fail-closed cases, all of which decline:** a certified or uncertified file
/// with no statistics (`None` span overlaps everything); an empty certified set
/// ("no evidence" must never read as "proved clean"); any uncertified span
/// touching the certified span on either side.
///
/// Spans are INCLUSIVE of both bounds, because Delta min/max statistics are.
///
/// Caller obligations this function cannot check, and which must hold:
/// - the scan is Delta-only; a MemBuffer leg can hold an uncertified newer
///   version whose file span does not exist to be compared;
/// - `certified` really is the sweep-proved set intersected with the LIVE file
///   list, so a file compacted away cannot vouch for its replacement.
pub(crate) fn skippable_certified_files<'a>(certified: impl IntoIterator<Item = (&'a str, FileSpan)>, uncertified: &[FileSpan]) -> HashSet<&'a str> {
    // One uncertified file without statistics has an unknown span, which
    // overlaps everything, so nothing in the scan can skip.
    if uncertified.iter().any(Option::is_none) {
        metrics::counter!(scan_metric_names::CERT_SKIP_BLOCKED_NO_STATS).increment(1);
        return HashSet::new();
    }
    let (skippable, blocked): (Vec<_>, Vec<_>) =
        certified.into_iter().partition(|(_, span)| span.is_some_and(|(lo, hi)| uncertified.iter().flatten().all(|(flo, fhi)| *fhi < lo || *flo > hi)));
    // Counted separately: overlap means certification is too SPARSE, while the
    // no-stats return above means one file poisoned the whole scan.
    metrics::counter!(scan_metric_names::CERT_SKIP_BLOCKED_OVERLAP).increment(blocked.len() as u64);
    metrics::counter!(scan_metric_names::CERT_SKIP_FILES).increment(skippable.len() as u64);
    skippable.into_iter().map(|(path, _)| path).collect()
}

/// Checks one union leg against its OWN declared ordering, so a nonzero counter
/// names which leg lied (`DedupExec` sits above the union and cannot).
///
/// OFF by default (`TIMEFUSION_ORDERING_PROBE`): costs one i64 compare per row
/// per leg. Turn it on when `ordering_violations_total` is nonzero.
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
        let stream = self.inner.execute(partition, context)?;
        let schema = stream.schema();
        // The leg's OWN claim: `None` means it declares nothing to violate.
        let Some(mut bound) = leading_bound(&self.inner, &schema, |_| true) else {
            return Ok(stream);
        };
        let counter = self.leg.counter();
        let out = stream.map(move |batch| {
            let batch = batch?;
            let col = batch.column(bound.idx);
            if let Some(values) = bound_slice(col) {
                (0..col.len()).filter(|&i| col.is_valid(i)).for_each(|i| {
                    bound.step(values[i], counter);
                });
            }
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, out)))
    }
}

/// The input's leading sort column as a `Bound`, when `accept`s its name and it
/// is i64-backed. The declared ordering is never verified — see `detect_bound`.
fn leading_bound(input: &Arc<dyn ExecutionPlan>, in_schema: &SchemaRef, accept: impl Fn(&str) -> bool) -> Option<Bound> {
    let se = input.properties().output_ordering()?.iter().next()?;
    let col = sort_col(se)?;
    (accept(col.name()) && matches!(in_schema.field(col.index()).data_type(), DataType::Int64 | DataType::Timestamp(..))).then(|| Bound {
        idx: col.index(),
        desc: se.options.descending,
        last: None,
    })
}

/// Rows observed out of the order their scan declared. See `Bound::step`.
pub(crate) static ORDERING_VIOLATIONS: AtomicU64 = AtomicU64::new(0);

pub fn ordering_violations() -> u64 {
    ORDERING_VIOLATIONS.load(Ordering::Relaxed)
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
/// Correctness does not depend on it, but turning it off also disables LIMIT
/// early termination: keep-greatest then buffers to end-of-stream, so a
/// "top 100" query scans the whole window.
static BOUNDED_DEDUP_ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

static ORDERING_PROBE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Per-leg ordering attribution (`OrderingProbeExec`); OFF unless
/// `TIMEFUSION_ORDERING_PROBE=true`.
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

/// The dedup key columns to hash. The bound column is ALWAYS retained: dropping
/// it (redundant only while the declared ordering is TRUE) lets a lying parquet
/// footer collapse rows that differ only in `timestamp`. Keeping it makes
/// bounded mode fail-safe — a false ordering can only under-dedup, never drop a
/// distinct row.
fn dedup_key_idxs(key_idxs: &[usize]) -> Vec<usize> {
    key_idxs.to_vec()
}

/// Enable bounded mode iff the input's leading sort column is a dedup key of an
/// i64-backed type AND `timefusion_read_dedup_bounded` is on.
///
/// The ordering here is *declared*, never verified — `output_ordering()` is only
/// as trustworthy as the parquet footer behind it; `dedup_key_idxs` keeps the
/// operator sound when that declaration lies.
fn detect_bound(input: &Arc<dyn ExecutionPlan>, keys: &[String], in_schema: &SchemaRef, enabled: bool) -> Option<Bound> {
    enabled.then(|| leading_bound(input, in_schema, |name| keys.iter().any(|k| k == name))).flatten()
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
    /// `EnforceSorting` preserves it. Without the requirement EnforceSorting
    /// deletes the ordering, keep-greatest degrades to keep-first, and a
    /// merge-on-read table answers with the PRE-update row.
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
        // Dedup only drops rows, so the input's ordering stays valid on the
        // output (remapped through the projection). Propagating it is what lets
        // `ORDER BY timestamp LIMIT n` early-terminate instead of re-sorting.
        let eq = datafusion::physical_expr::EquivalenceProperties::new_with_orderings(schema.clone(), remap_ordering(&input, &output_projection, &schema));
        let properties =
            Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), input.properties().emission_type, input.properties().boundedness));
        Ok(Self { input, keys, key_idxs, tiebreak, required_ordering: None, output_projection, schema, properties, metrics: ExecutionPlanMetricsSet::new() })
    }

    /// Declare the ordering keep-greatest needs. `None` leaves the operator
    /// ordering-agnostic (tables without `version_append`).
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
        // Surface the seen-set mode: `bounded` clears state when the bound
        // advances, `full-set` retains every key for the whole scan (a multi-GB
        // risk otherwise invisible in EXPLAIN).
        let in_schema = self.input.schema();
        write!(f, "DedupExec: keys=[{}], mode=", self.keys.join(", "))?;
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
        // Surviving rows appear in input order (keep-greatest emits a closed run
        // in input position order).
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
        // Bounded vs full-set is the difference between a LIMIT that terminates
        // early and one that buffers the whole window into the 2 GiB budget.
        metrics::counter!(match bound {
            Some(_) => scan_metric_names::DEDUP_BOUNDED_TOTAL,
            None => scan_metric_names::DEDUP_FULL_SET_TOTAL,
        })
        .increment(1);
        let key_idxs = dedup_key_idxs(&self.key_idxs);
        // A bound lets keep-greatest emit per run, but is not required: without
        // one it buffers to end-of-stream rather than forcing a blocking sort.
        // That buffer is real heap the pool must see, so an oversized window
        // fails its own query with ResourcesExhausted instead of OOM-killing the
        // server.
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
        // flush emits one per buffered batch) or none at all, so `flat_map` fans
        // them out lazily.
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
            produced.iter().flatten().for_each(|batch| {
                batch.record_output(&baseline);
            });
            Some((produced, (input, dedup, baseline, input_rows, done)))
        })
        .flat_map(|r| futures::stream::iter(r.map_or_else(|e| vec![Err(e)], |bs| bs.into_iter().map(Ok).collect::<Vec<_>>())));

        // Statement timeouts are enforced by dropping the in-flight future, and
        // that can only happen when a poll returns `Pending`. Unbounded
        // keep-greatest can burn minutes of pure CPU inside one `poll_next`, so
        // this custom operator must opt into cooperative yielding or the
        // deadline is unobservable.
        Ok(datafusion::physical_plan::coop::make_cooperative(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream))))
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

/// A bounded run normally has one winner, so tiny runs stay in a vec; only an
/// unusually wide equal-timestamp run is promoted to a hash map.
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
                    let promoted: HashMap<_, _, ahash::RandomState> = entries.drain(..).chain([(key.into(), cand)]).collect();
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

    /// Visit every live winner. Used both to shift indices after an emit and to
    /// re-point them after the buffer is compacted.
    fn cands_mut(&mut self, mut f: impl FnMut(&mut Cand)) {
        match self {
            Self::Small(entries) => entries.iter_mut().for_each(|(_, cand)| f(cand)),
            Self::Large(entries) => entries.values_mut().for_each(f),
        }
    }

    fn shift_batches(&mut self, count: u32) {
        self.cands_mut(|cand| cand.batch -= count);
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

/// A winner tiebreak. Timestamp/int64 values stay primitive; other schema types
/// retain Arrow's generic order-preserving encoding. `B` is `Box<[u8]>` when the
/// bytes are owned by a stored winner, `&[u8]` when borrowed from the batch.
enum Tiebreak<B> {
    I64(Option<i64>),
    Encoded(B),
}

type TiebreakValue = Tiebreak<Box<[u8]>>;
type TiebreakRef<'a> = Tiebreak<&'a [u8]>;

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

impl TiebreakRef<'_> {
    fn beats(&self, old: &TiebreakValue) -> bool {
        match (self, old) {
            (Self::I64(new), Tiebreak::I64(old)) => new > old,
            (Self::Encoded(new), Tiebreak::Encoded(old)) => *new > &old[..],
            _ => unreachable!("one Greatest instance uses one tiebreak representation"),
        }
    }

    fn into_owned(self) -> TiebreakValue {
        match self {
            Self::I64(value) => Tiebreak::I64(value),
            Self::Encoded(value) => Tiebreak::Encoded(value.into()),
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
    /// Winner masks accumulated for CLOSED runs. Marking here and filtering each
    /// retained batch once avoids an O(rows × batch_rows) filter per run
    /// boundary.
    masks: Vec<Vec<bool>>,
    bytes: usize,
    /// Pool accounting for `batches` (`bytes` mirrors its size). The winner map
    /// is second-order and stays untracked.
    reservation: MemoryReservation,
    /// Re-arm point for `compact_to_winners`: twice what the winners cost after
    /// the last compaction, so a buffer genuinely full of winners does not
    /// re-filter on every push.
    compact_floor: usize,
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
        Ok(Self { idx, conv, best: Winners::new(), batches: Vec::new(), masks: Vec::new(), bytes: 0, reservation, compact_floor: 0 })
    }

    /// Collapse the retained buffer to the rows that are CURRENT winners.
    ///
    /// **Sound only while the whole scan is one open run** — i.e. unbounded
    /// keep-greatest, where `close_run` has not fired and every `masks` entry is
    /// still false. Bounded mode must NOT call this: its masks carry closed-run
    /// winners that have not been emitted yet, and this would discard them.
    ///
    /// This is what bounds unordered dedup by DISTINCT KEYS instead of by rows:
    /// `emit_prefix` can only release the prefix ahead of the earliest live
    /// candidate, so one long-lived winner in batch 0 otherwise pins everything
    /// behind it.
    fn compact_to_winners(&mut self) -> DFResult<()> {
        if self.batches.is_empty() {
            return Ok(());
        }
        let mut keep: Vec<Vec<bool>> = self.batches.iter().map(|b| vec![false; b.num_rows()]).collect();
        self.best.cands_mut(|cand| keep[cand.batch as usize][cand.row as usize] = true);

        let old = std::mem::take(&mut self.batches);
        let dropped: usize = old.iter().map(RecordBatch::num_rows).sum::<usize>() - keep.iter().flatten().filter(|k| **k).count();
        // Old (batch, row) → new (batch, row), for every row of every old batch.
        // Dropped rows are never looked up: only live winners are re-pointed.
        let mut moved: Vec<Vec<(u32, u32)>> = Vec::with_capacity(old.len());
        let mut kept_bytes = 0usize;
        for (batch, mask) in old.into_iter().zip(&keep) {
            let mut rows = vec![(u32::MAX, u32::MAX); mask.len()];
            if !mask.iter().any(|k| *k) {
                moved.push(rows);
                continue;
            }
            let bi = self.batches.len() as u32;
            for (next, (row, _)) in mask.iter().enumerate().filter(|(_, k)| **k).enumerate() {
                rows[row] = (bi, next as u32);
            }
            // `compact_batch` after the filter: Arrow's filter over a view array
            // produces new views over the ORIGINAL buffers, so without it the
            // parquet column-chunk blocks stay alive and this frees nothing.
            let compacted = crate::write::mem_buffer::compact_batch(filter_record_batch(&batch, &BooleanArray::from(mask.clone())).map_err(arrow_err)?);
            kept_bytes += compacted.get_array_memory_size();
            self.batches.push(compacted);
            moved.push(rows);
        }
        self.best.cands_mut(|cand| {
            let (batch, row) = moved[cand.batch as usize][cand.row as usize];
            cand.batch = batch;
            cand.row = row;
        });
        self.masks = self.batches.iter().map(|b| vec![false; b.num_rows()]).collect();
        // The pool must see the drop, or the reservation outlives the memory it
        // stands for.
        match kept_bytes.cmp(&self.bytes) {
            std::cmp::Ordering::Less => self.reservation.shrink(self.bytes - kept_bytes),
            std::cmp::Ordering::Greater => self.reservation.try_grow(kept_bytes - self.bytes)?,
            std::cmp::Ordering::Equal => {}
        }
        self.bytes = kept_bytes;
        metrics::counter!(scan_metric_names::DEDUP_WINNER_COMPACTIONS_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::DEDUP_WINNER_COMPACTION_ROWS_DROPPED).increment(dropped as u64);
        Ok(())
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
    /// Greatest-mode fast path for a single non-null string key column.
    direct_string_key: Option<usize>,
}

enum KeyRows<'a> {
    Encoded(datafusion::arrow::row::Rows),
    /// The `direct_string_key` fast path: a single non-nullable string column,
    /// read in place rather than re-encoded through the row converter.
    Direct(StringValues<'a>),
}

impl KeyRows<'_> {
    fn value(&self, row: usize) -> &[u8] {
        match self {
            Self::Encoded(rows) => rows.row(row).data(),
            Self::Direct(values) => values.value(row).expect("direct string key is non-nullable").as_bytes(),
        }
    }
}

/// Order-encode the key columns of `batch`. A free fn, not a method: the callers
/// hold a `&mut` borrow of `Dedup::greatest` across it.
fn encode_keys(conv: &RowConverter, key_idxs: &[usize], batch: &RecordBatch) -> DFResult<datafusion::arrow::row::Rows> {
    conv.convert_columns(&key_idxs.iter().map(|&i| batch.column(i).clone()).collect::<Vec<ArrayRef>>()).map_err(arrow_err)
}

impl Dedup {
    fn push(&mut self, batch: &RecordBatch) -> DFResult<Vec<RecordBatch>> {
        let Some(g) = self.greatest.as_mut() else {
            let keys = encode_keys(&self.conv, &self.key_idxs, batch)?;
            return Ok(dedup_first(batch, &keys, &mut self.seen, self.output_projection.as_deref(), self.bound.as_mut())?.into_iter().collect());
        };
        let keys = match self.direct_string_key.and_then(|idx| StringValues::from_column(batch.column(idx))) {
            Some(values) => KeyRows::Direct(values),
            None => KeyRows::Encoded(encode_keys(&self.conv, &self.key_idxs, batch)?),
        };
        let proj = self.output_projection.as_deref();
        let mut out = Vec::new();
        // Only a BOUNDED run may flush early. Without a bound the whole scan is
        // one open run: any key can still be beaten by a later batch, so
        // emitting now would serve the superseded row.
        if self.bound.is_some() && g.bytes > RUN_BUFFER_MAX_BYTES {
            g.close_run(&mut self.seen, true);
            out.extend(g.emit_prefix(g.batches.len(), proj)?);
        } else if self.bound.is_none() && g.bytes > g.compact_floor.max(RUN_BUFFER_MAX_BYTES) {
            // Unbounded cannot emit early — a later batch may still beat any
            // key — but it CAN stop holding rows that are already beaten.
            g.compact_to_winners()?;
            g.compact_floor = g.bytes.saturating_mul(2);
        }
        let tbs = g.tiebreak_rows(batch.column(g.idx))?;
        let not_i64 = |idx| DataFusionError::Internal(format!("DedupExec bound column {idx} is not i64-backed"));
        let bvals = self.bound.as_ref().map(|b| bound_slice(batch.column(b.idx)).ok_or_else(|| not_i64(b.idx))).transpose()?;
        // Index of `batch` within the open run's buffer; `None` until a row of
        // this batch wins something.
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
                    // Pool BEFORE buffering, and compact first: a retained view
                    // array keeps the parquet reader's whole column-chunk block
                    // alive and charges the pool for it. `compact_batch` is a
                    // no-op when there is nothing to compact.
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
        let Some(g) = self.greatest.as_mut() else { return Ok(Vec::new()) };
        g.close_run(&mut self.seen, false);
        g.emit_prefix(g.batches.len(), self.output_projection.as_deref())
    }
}

/// Project BEFORE filtering, so the key/tiebreak columns added solely for winner
/// selection are not copied and then discarded.
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
    // `bound_slice` returning None (unsupported type) disables eviction for this
    // batch — still correct. `.map(idx)` first so the slice borrows `batch`, not
    // `bound`, which the closure below needs mutably.
    let bvals = bound.as_ref().map(|b| b.idx).and_then(|i| bound_slice(batch.column(i)));
    // Borrowed probe: hash the encoded bytes in place and allocate only on a
    // miss. When the bound advances past the current run the seen-set is cleared
    // first — no earlier key can recur in a sorted stream.
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
    /// The soundness rule for additive, file-set certification: getting it wrong
    /// silently over-counts. `("a", span)` names the file; the assertion is on
    /// which names survive.
    #[test_case::test_case(&[("a", Some((10, 20)))], &[Some((30, 40))], &["a"] ; "uncertified sits entirely after")]
    #[test_case::test_case(&[("a", Some((30, 40)))], &[Some((10, 20))], &["a"] ; "entirely before")]
    #[test_case::test_case(&[("a", Some((10, 20)))], &[], &["a"] ; "nothing uncertified to collide with")]
    #[test_case::test_case(&[("a", Some((10, 20)))], &[Some((20, 30))], &[] ; "touching at a single microsecond is an overlap — Delta min/max are INCLUSIVE")]
    #[test_case::test_case(&[("a", Some((10, 20)))], &[Some((5, 10))], &[] ; "touching on the low side")]
    #[test_case::test_case(&[("a", Some((10, 20)))], &[Some((12, 15))], &[] ; "uncertified contained by the certified file")]
    #[test_case::test_case(&[("a", Some((10, 20)))], &[Some((0, 99))], &[] ; "uncertified spanning it")]
    #[test_case::test_case(&[("a", Some((10, 20)))], &[None], &[] ; "an uncertified file with no stats overlaps everything, so NOTHING skips")]
    #[test_case::test_case(&[("a", None)], &[Some((30, 40))], &[] ; "a certified file with no stats has an unknown span")]
    #[test_case::test_case(&[], &[Some((30, 40))], &[] ; "an empty certified set proves nothing")]
    #[test_case::test_case(&[], &[], &[] ; "nothing at all still proves nothing")]
    // THE case the per-file rule exists for: judged by the certified set's UNION
    // span every file would be refused; per file, only the real neighbour is.
    #[test_case::test_case(&[("old", Some((10, 20))), ("mid", Some((45, 55))), ("new", Some((80, 90)))], &[Some((50, 60))], &["new", "old"] ; "only the overlapping certified file is held back")]
    fn certified_files_skip_dedup_only_when_no_uncertified_file_could_hold_another_version(
        certified: &[(&str, FileSpan)], uncertified: &[FileSpan], skippable: &[&str],
    ) {
        let mut got: Vec<&str> = skippable_certified_files(certified.iter().copied(), uncertified).into_iter().collect();
        got.sort_unstable();
        assert_eq!(got, skippable.to_vec());
    }

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
        // Probe then apply as `Dedup::push` does; reports whether the probe
        // chose ReplaceLarge.
        let offer = |w: &mut Winners, key: &[u8], batch: u32, row: u32, stamp: i64| -> bool {
            let tb = TiebreakRef::I64(Some(stamp));
            let update = w.probe(key, &tb);
            let replace_large = matches!(update, WinnerUpdate::ReplaceLarge);
            w.apply(key, Cand { batch, row, tb: tb.into_owned() }, update);
            replace_large
        };
        for row in 0..SMALL_WINNER_LIMIT {
            offer(&mut winners, format!("id-{row}").as_bytes(), 0, row as u32, 1);
        }
        assert!(matches!(winners, Winners::Small(_)));

        let key: &[u8] = b"promotes";
        offer(&mut winners, key, 0, 8, 1);
        assert!(matches!(winners, Winners::Large(_)));

        assert!(offer(&mut winners, key, 1, 9, 2), "a greater stamp must replace the stored large-run winner");
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

    /// Keep-first over `b` keyed on `idxs`, threading the caller's seen-set and
    /// bound.
    fn first(b: &RecordBatch, idxs: &[usize], seen: &mut SeenSet, bound: Option<&mut Bound>) -> Option<RecordBatch> {
        let schema = b.schema();
        let conv = RowConverter::new(idxs.iter().map(|&i| SortField::new(schema.field(i).data_type().clone())).collect()).unwrap();
        let keys = conv.convert_columns(&idxs.iter().map(|&i| b.column(i).clone()).collect::<Vec<_>>()).unwrap();
        dedup_first(b, &keys, seen, None, bound).unwrap()
    }

    /// Keep-first across batches: the seen-set threads state, duplicates
    /// (including cross-batch) collapse, and the *first* occurrence survives.
    #[test]
    fn dedup_batch_keeps_first_and_counts_distinct() {
        let mut seen = SeenSet::default();
        let b1 = batch(&["a", "b", "a", "c"], &[1, 2, 3, 4]);
        let out1 = first(&b1, &[0], &mut seen, None).unwrap();
        let ids = out1.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let vs = out1.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ids.iter().flatten().collect::<Vec<_>>(), vec!["a", "b", "c"]);
        assert_eq!(vs.values(), &[1, 2, 4], "first occurrence survives (a→1, not a→3)");

        // Second batch: every key already seen → whole batch drops (None).
        let b2 = batch(&["b", "a"], &[9, 9]);
        assert!(first(&b2, &[0], &mut seen, None).is_none());

        // A fresh key in an otherwise-dup batch survives alone.
        let b3 = batch(&["a", "d"], &[9, 5]);
        let out3 = first(&b3, &[0], &mut seen, None).unwrap();
        assert_eq!(out3.num_rows(), 1);
        assert_eq!(out3.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "d");
    }

    /// Bounded window: an advancing `ts` clears the seen-set, so an `id` re-seen
    /// at a NEW ts survives while an exact `(id, ts)` dup within a run collapses.
    #[test]
    fn dedup_batch_bounded_window_evicts_on_advance() {
        // Two-column key (id, ts); bound = ts (col 1), ascending.
        let mut seen = SeenSet::default();
        let mut bound = Bound { idx: 1, desc: false, last: None };

        // Run ts=10: a,b,a → within-run dup of `a` collapses.
        let r1 = batch(&["a", "b", "a"], &[10, 10, 10]);
        let o1 = first(&r1, &[0, 1], &mut seen, Some(&mut bound)).unwrap();
        assert_eq!(o1.num_rows(), 2, "(a,10),(b,10) survive; second (a,10) dropped");
        assert_eq!(seen.len(), 2);

        // ts advances to 11: seen cleared, so `a` at a NEW ts survives.
        let r2 = batch(&["a", "a"], &[11, 11]);
        let o2 = first(&r2, &[0, 1], &mut seen, Some(&mut bound)).unwrap();
        assert_eq!(o2.num_rows(), 1, "(a,11) survives once; second (a,11) is a same-run dup");
        assert_eq!(seen.len(), 1, "seen-set bounded to the current run, not O(all distinct)");
        assert_eq!(bound.last, Some(11));
    }

    /// A footer declaring `timestamp DESC` over unsorted data makes the bound
    /// never advance, so one "run" spans many timestamps; the key must still
    /// include the bound or distinct rows collapse.
    #[test]
    fn bounded_dedup_false_ordering_does_not_collapse_distinct_timestamps() {
        // Declared DESC, actually ASCENDING — the footer lied.
        let mut bound = Bound { idx: 1, desc: true, last: None };
        let b = batch(&["a", "a"], &[5, 10]);

        let mut seen = SeenSet::default();
        let lost = first(&b, &[0], &mut seen, Some(&mut bound)).unwrap();
        assert_eq!(lost.num_rows(), 1, "documents the old bug: (a,5) and (a,10) collapsed");

        assert_eq!(dedup_key_idxs(&[0, 1]), vec![0, 1], "bound column must stay in the dedup key");

        let mut bound2 = Bound { idx: 1, desc: true, last: None };
        let mut seen2 = SeenSet::default();
        let kept = first(&b, &[0, 1], &mut seen2, Some(&mut bound2)).unwrap();
        assert_eq!(kept.num_rows(), 2, "(a,5) and (a,10) are DISTINCT rows and must both survive");
    }

    /// With bounded dedup disabled, no declared ordering selects bounded mode.
    /// It defaults ON (off also disables LIMIT early termination).
    #[test]
    fn bounded_dedup_kill_switch_forces_full_set() {
        let plan = dedup_plan(vec![vbatch(&["a", "a"], &[5, 10], &[Some(1), Some(2)])], true, true);
        let schema = plan.input.schema();
        assert!(detect_bound(&plan.input, &plan.keys, &schema, false).is_none(), "flag off ⇒ full-set regardless of declared ordering");
        assert!(bounded_dedup_enabled(), "default must stay ON: full-set has no LIMIT early termination");
    }

    /// An out-of-order row under a declared ordering must be counted.
    #[test]
    fn advance_counts_declared_ordering_violations() {
        let before = ordering_violations();
        let mut b = Bound { idx: 0, desc: true, last: None };
        b.advance(10); // first row: no baseline, no violation
        b.advance(5); // DESC-consistent
        b.advance(9); // moves back UP under a DESC claim → violation
        assert_eq!(ordering_violations(), before + 1);
    }

    /// EXPLAIN must name both the seen-set mode — `full-set` retains every key
    /// for the whole scan, `bounded` clears per run — and the SURVIVOR rule:
    /// `full-set/first` serves the pre-update row, `full-set/greatest` is right.
    #[test_case::test_case(true, false => "bounded[ts]/first" ; "sorted input reports the bounded window")]
    #[test_case::test_case(false, false => "full-set/first" ; "unsorted input reports the unbounded seen-set")]
    #[test_case::test_case(false, true => "full-set/greatest" ; "unsorted plus a tiebreak reports keep-greatest")]
    #[test_case::test_case(true, true => "bounded[ts]/greatest" ; "sorted plus a tiebreak reports both")]
    fn explain_reveals_seen_set_mode_and_survivor_rule(ordered: bool, tiebreak: bool) -> String {
        let plan = dedup_plan(vec![vbatch(&["a"], &[10], &[Some(1)])], ordered, tiebreak);
        let shown = format!("{}", datafusion::physical_plan::displayable(&plan).one_line());
        shown.split_once("mode=").expect("EXPLAIN must report a mode").1.trim().to_string()
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

    /// A context whose memory pool admits exactly `bytes`.
    fn pool_ctx(bytes: usize) -> Arc<TaskContext> {
        use datafusion::execution::{memory_pool::GreedyMemoryPool, runtime_env::RuntimeEnvBuilder};
        let runtime = RuntimeEnvBuilder::new().with_memory_pool(Arc::new(GreedyMemoryPool::new(bytes))).build_arc().unwrap();
        Arc::new(TaskContext::default().with_runtime(runtime))
    }

    // ---- keep-greatest (merge-on-read phase 2) ----

    /// (id Utf8, ts Int64, tb Int64 nullable) — dedup key `(id, ts)` sorted by
    /// `ts`, tiebreak `tb`.
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

    /// A `DedupExec` over the `(id, ts, tb)` shape, keyed `(ts, id)`.
    /// `ordered` declares the `ts` ordering on the source (bounded runs);
    /// `ordered = false` is the merge-on-read shape, where an UPDATE rewrites a
    /// row under its original timestamp so the Delta leg's files overlap in
    /// time. `tiebreak` supplies the `tb` version stamp.
    fn dedup_plan(batches: Vec<RecordBatch>, ordered: bool, tiebreak: bool) -> DedupExec {
        let src = source(&[batches], ordered.then(|| col_asc("ts", 1)));
        DedupExec::with_tiebreak(src, vec!["ts".into(), "id".into()], tiebreak.then(|| "tb".to_string()), None).unwrap()
    }

    /// Unsorted input MUST still keep the greatest version: degrading to
    /// keep-first would serve the pre-UPDATE row under merge-on-read.
    #[tokio::test(flavor = "multi_thread")]
    async fn keep_greatest_without_a_bound_still_picks_the_newest_version() {
        // `a` is updated in a LATER batch; `b`'s newer version arrives FIRST.
        let plan = dedup_plan(vec![vbatch(&["a", "b"], &[10, 20], &[Some(1), Some(9)]), vbatch(&["b", "a"], &[20, 10], &[Some(2), Some(7)])], false, true);
        let mut got = collect_rows(&plan).await;
        got.sort();
        assert_eq!(got, vec![("a".into(), 10, Some(7)), ("b".into(), 20, Some(9))], "unbounded keep-greatest must win on the tiebreak, not on arrival order");

        // A NULL stamp must lose to any stamped version, in either order.
        let plan = dedup_plan(vec![vbatch(&["c"], &[30], &[None]), vbatch(&["c"], &[30], &[Some(4)])], false, true);
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
        let plan = dedup_plan(vec![batch], true, true);
        let batches = datafusion::physical_plan::collect(Arc::new(plan), Arc::new(TaskContext::default())).await.unwrap();
        let rows = batches.iter().flat_map(|batch| {
            let ids = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let tbs = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
            (0..batch.num_rows()).map(move |i| (ids.value(i).to_string(), tbs.is_valid(i).then(|| tbs.value(i).to_string())))
        });
        assert_eq!(rows.collect::<Vec<_>>(), vec![("a".into(), Some("z".into())), ("b".into(), None)]);
    }

    /// The unbounded run buffer must be pool-tracked, so an oversized scan
    /// fails its own query instead of growing untracked anon heap.
    #[tokio::test(flavor = "multi_thread")]
    async fn unbounded_run_buffer_is_pool_tracked_so_an_oversized_scan_fails_its_query() {
        let batches: Vec<RecordBatch> = (0..64).map(|i| vbatch(&[format!("k{i}").as_str()], &[i], &[Some(i)])).collect();
        let plan = dedup_plan(batches, false, true);
        let mut stream = plan.execute(0, pool_ctx(512)).unwrap();
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
    /// for the rows it owns, not for the allocation it borrows from:
    /// `get_array_memory_size` charges the whole parent column-chunk block.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_batch_slicing_a_big_parent_is_charged_for_its_own_rows() {
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

        // Above the 16 rows retained, far below 8 x the inherited charge:
        // only honest accounting fits in this pool.
        let out = datafusion::physical_plan::collect(Arc::new(dedup_plan(batches, false, true)), pool_ctx(inherited))
            .await
            .expect("16 sliced rows must not be charged the whole parent");
        assert_eq!(out.iter().map(RecordBatch::num_rows).sum::<usize>(), owned, "every distinct key must survive");
    }

    #[test]
    fn unbounded_run_buffer_has_a_per_query_ceiling() {
        assert!(check_unbounded_growth(UNBOUNDED_GREATEST_MAX_BYTES - 1, 1).is_ok());
        let err = check_unbounded_growth(UNBOUNDED_GREATEST_MAX_BYTES, 1).unwrap_err();
        assert!(format!("{err}").contains("per-query limit"));
        assert!(check_unbounded_growth(usize::MAX, 1).is_err(), "overflow must fail closed");
    }

    /// The `(id, ts, tb)` rows of `batches`, in batch order.
    fn batch_rows(batches: &[RecordBatch]) -> Vec<(String, i64, Option<i64>)> {
        batches
            .iter()
            .flat_map(|b| {
                let (ids, ts, tb) = (
                    b.column(0).as_any().downcast_ref::<StringArray>().unwrap(),
                    b.column(1).as_any().downcast_ref::<Int64Array>().unwrap(),
                    b.column(2).as_any().downcast_ref::<Int64Array>().unwrap(),
                );
                (0..b.num_rows()).map(|i| (ids.value(i).to_string(), ts.value(i), tb.is_valid(i).then(|| tb.value(i)))).collect::<Vec<_>>()
            })
            .collect()
    }

    async fn collect_rows(plan: &DedupExec) -> Vec<(String, i64, Option<i64>)> {
        let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut out = Vec::new();
        while let Some(b) = futures::StreamExt::next(&mut stream).await {
            out.push(b.unwrap());
        }
        batch_rows(&out)
    }

    /// THE survivor rules of `DedupExec`, one case per arrival shape. Rows are
    /// asserted in emission order: survivors come out at their own input
    /// positions, so any declared input ordering still holds on the output.
    /// `ordered` declares the `ts` ordering on the source (bounded runs);
    /// `tiebreak` supplies the `tb` version stamp.
    #[test_case::test_case(true, true, &[(&["a", "b"], &[10, 10], &[Some(1), Some(5)]), (&["a", "b"], &[10, 10], &[Some(7), Some(2)]), (&["a"], &[10], &[Some(3)])] => vec![("b".to_string(), 10, Some(5)), ("a".to_string(), 10, Some(7))] ; "keep_greatest picks the highest tiebreak across batches")]
    // Runs are the unit of emission: a closed run's keys emit when the bound
    // advances, and a key re-seen at a *new* ts is a different row.
    #[test_case::test_case(true, true, &[(&["a", "a", "b"], &[10, 10, 11], &[Some(1), Some(9), Some(1)]), (&["a", "b"], &[11, 11], &[Some(4), Some(8)]), (&["a"], &[12], &[Some(0)])] => vec![("a".to_string(), 10, Some(9)), ("a".to_string(), 11, Some(4)), ("b".to_string(), 11, Some(8)), ("a".to_string(), 12, Some(0))] ; "keep_greatest across a run boundary")]
    // NULL tiebreak sorts lowest, in either arrival order.
    #[test_case::test_case(true, true, &[(&["a", "a", "b", "b"], &[1, 1, 2, 2], &[None, Some(1), Some(1), None])] => vec![("a".to_string(), 1, Some(1)), ("b".to_string(), 2, Some(1))] ; "keep_greatest null tiebreak loses in either order")]
    // All-NULL keeps exactly ONE row. The two inputs are byte-identical, so
    // this asserts the row count only, not which row survived.
    #[test_case::test_case(true, true, &[(&["a", "a"], &[1, 1], &[None, None])] => vec![("a".to_string(), 1, None)] ; "keep_greatest all-NULL stamps keep exactly one row")]
    // No `dedup_tiebreak` ⇒ keep-FIRST, even though a later copy would have
    // won under keep-greatest.
    #[test_case::test_case(true, false, &[(&["a", "a"], &[1, 1], &[Some(1), Some(9)])] => vec![("a".to_string(), 1, Some(1))] ; "no tiebreak stays keep-first")]
    // Unordered input has no run boundary, so keep-greatest buffers to
    // end-of-stream rather than degrading to keep-first (which would serve the
    // pre-UPDATE row) or forcing a blocking sort.
    #[test_case::test_case(false, true, &[(&["a", "a"], &[1, 1], &[Some(1), Some(9)])] => vec![("a".to_string(), 1, Some(9))] ; "unordered input keeps the newest version")]
    // A table WITHOUT a tiebreak still keeps first: nothing ranks its versions.
    #[test_case::test_case(false, false, &[(&["a", "a"], &[1, 1], &[Some(1), Some(9)])] => vec![("a".to_string(), 1, Some(1))] ; "unordered input without a tiebreak keeps first")]
    fn dedup_survivor_rules(ordered: bool, tiebreak: bool, spec: &[BatchSpec<'_>]) -> Vec<(String, i64, Option<i64>)> {
        let plan = dedup_plan(spec.iter().map(|(ids, ts, tb)| vbatch(ids, ts, tb)).collect(), ordered, tiebreak);
        tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap().block_on(collect_rows(&plan))
    }

    /// Bounded greatest-version dedup must filter an input batch once, not
    /// once per timestamp run inside it.
    #[tokio::test(flavor = "multi_thread")]
    async fn bounded_greatest_coalesces_many_runs_in_one_input_batch() {
        let ids: Vec<String> = (0..4096).map(|i| format!("id-{i}")).collect();
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        let ts: Vec<i64> = (0..4096).collect();
        let tb: Vec<Option<i64>> = (0..4096).map(Some).collect();
        let plan = Arc::new(dedup_plan(vec![vbatch(&id_refs, &ts, &tb)], true, true));
        let batches = datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default())).await.unwrap();
        assert_eq!(batches.len(), 1, "one input batch must not fragment into one output batch per timestamp");
        assert_eq!(batches[0].num_rows(), 4096);
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

    /// One output partition, and the remapped input ordering preserved — that
    /// is what keeps `ORDER BY … LIMIT` streaming.
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
    /// for early-LIMIT termination. One `ts` run per batch, each a duplicate pair.
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

    /// Keep-greatest must still stream: a `LIMIT` over a huge ordered input
    /// terminates after a handful of batches, not by draining the source.
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
        let mut d = dedup_state(Some(Bound { idx: 1, desc: false, last: None }));
        for t in 0..200i64 {
            d.push(&vbatch(&["a", "b", "a"], &[t, t, t], &[Some(1), Some(1), Some(2)])).unwrap();
        }
        let g = d.greatest.as_ref().unwrap();
        assert_eq!(g.best.len(), 2, "only the open run's keys are held");
        assert_eq!(g.batches.len(), 1, "only the open run's batches are held");
        assert!(d.seen.is_empty(), "seen only holds overflow-flushed keys");
    }

    /// One `vbatch` as a case-table row: ids, timestamps, tiebreak stamps.
    type BatchSpec<'a> = (&'a [&'a str], &'a [i64], &'a [Option<i64>]);

    /// A keep-greatest `Dedup` over the `(id, ts, tb)` shape. `bound: None` is
    /// the unbounded merge-on-read shape that buffers to end-of-stream.
    fn dedup_state(bound: Option<Bound>) -> Dedup {
        let in_schema = vbatch(&["a"], &[0], &[Some(0)]).schema();
        Dedup {
            key_idxs: vec![1, 0],
            conv: RowConverter::new(vec![SortField::new(DataType::Int64), SortField::new(DataType::Utf8)]).unwrap(),
            output_projection: None,
            seen: SeenSet::default(),
            bound,
            direct_string_key: None,
            greatest: Some(
                Greatest::new(2, in_schema.field(2).data_type(), MemoryConsumer::new("test").register(&Arc::new(TaskContext::default()).memory_pool().clone()))
                    .unwrap(),
            ),
        }
    }

    fn rows_of(batches: &[RecordBatch]) -> Vec<(String, i64, Option<i64>)> {
        let mut out = batch_rows(batches);
        out.sort();
        out
    }

    /// Push `batches`, optionally collapsing the retained buffer after each one,
    /// and return the deduped answer. `compact_every` forces the compaction
    /// path that `RUN_BUFFER_MAX_BYTES` would otherwise only reach at 64 MB.
    fn run_unbounded(batches: &[RecordBatch], compact_every: bool) -> Vec<(String, i64, Option<i64>)> {
        let mut d = dedup_state(None);
        let mut out = Vec::new();
        for b in batches {
            out.extend(d.push(b).unwrap());
            if compact_every {
                d.greatest.as_mut().unwrap().compact_to_winners().unwrap();
            }
        }
        out.extend(d.finish().unwrap());
        rows_of(&out)
    }

    /// Collapsing the retained buffer to current winners must not change a
    /// single answer; in particular a key whose newest version arrives in a
    /// LATER batch must still be replaceable after an earlier compaction.
    /// Each case is one arrival order over the same logical rows.
    #[test_case::test_case(&[(&["a", "b"], &[10, 20], &[Some(1), Some(9)]), (&["b", "a"], &[20, 10], &[Some(2), Some(7)])] ; "update arrives in a later batch")]
    #[test_case::test_case(&[(&["a"], &[10], &[Some(7)]), (&["a"], &[10], &[Some(1)])] ; "the winner arrives FIRST and later versions lose")]
    #[test_case::test_case(&[(&["c"], &[30], &[None]), (&["c"], &[30], &[Some(4)])] ; "a NULL stamp loses across a compaction")]
    #[test_case::test_case(&[(&["c"], &[30], &[Some(4)]), (&["c"], &[30], &[None])] ; "and in the other arrival order")]
    #[test_case::test_case(&[(&["a", "a", "a"], &[10, 10, 10], &[Some(1), Some(3), Some(2)]), (&["a"], &[10], &[Some(9)]), (&["b"], &[20], &[Some(1)])] ; "many versions in one batch, then a later winner")]
    #[test_case::test_case(&[(&["a"], &[10], &[Some(1)]), (&["b"], &[20], &[Some(1)]), (&["a"], &[10], &[Some(2)]), (&["b"], &[20], &[Some(2)])] ; "two keys updated alternately")]
    // A TIE across the compaction: `beats` is strict, so the equal challenger
    // never unseats the incumbent. Pins that a tie loses no key and emits no
    // duplicate — not which of the two identical rows survived.
    #[test_case::test_case(&[(&["a"], &[10], &[Some(5)]), (&["a"], &[10], &[Some(5)])] ; "an equal stamp does not unseat the incumbent")]
    fn a_winner_compaction_changes_no_answer(spec: &[BatchSpec<'_>]) {
        let batches: Vec<RecordBatch> = spec.iter().map(|(ids, ts, tb)| vbatch(ids, ts, tb)).collect();
        assert_eq!(run_unbounded(&batches, true), run_unbounded(&batches, false), "compacting the retained buffer to current winners changed the answer");
    }

    /// `p` is a key whose winner lands in batch 0 and is never beaten, pinning
    /// the prefix at 0 so `push`'s `min_batch`/`emit_prefix` release can never
    /// fire and every later batch is retained whole, dead versions included.
    /// Over a wide window some key always plays `p`'s part.
    #[test]
    fn a_winner_compaction_retains_one_row_per_key_not_per_version() {
        let mut d = dedup_state(None);
        d.push(&vbatch(&["p"], &[1], &[Some(1)])).unwrap();
        // 40 further keys, each arriving as three versions inside one batch.
        for i in 0..40i64 {
            let id = format!("k{i}");
            d.push(&vbatch(&[&id, &id, &id], &[10 + i, 10 + i, 10 + i], &[Some(1), Some(3), Some(2)])).unwrap();
        }
        let g = d.greatest.as_mut().unwrap();
        let before: usize = g.batches.iter().map(RecordBatch::num_rows).sum();
        let bytes_before = g.bytes;
        assert_eq!(before, 121, "a pinned early candidate keeps every version buffered");

        g.compact_to_winners().unwrap();
        let after: usize = g.batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(after, 41, "one row per live key survives, got {after}");
        assert!(g.bytes < bytes_before, "the pool reservation must fall with the rows ({bytes_before} -> {})", g.bytes);

        // The surviving row per key is the WINNER (tb=3), not the first-arrived.
        let out = rows_of(&d.finish().unwrap());
        assert_eq!(out.len(), 41);
        assert!(out.iter().all(|(id, _, tb)| id == "p" || *tb == Some(3)), "a compaction re-pointed a candidate at the wrong row: {out:?}");
    }

    /// A compaction that leaves a batch with no winners must drop the batch and
    /// re-point every surviving candidate, since dropping shifts every later
    /// batch index. `p` pins the prefix so `push` cannot release it instead.
    #[test]
    fn a_compaction_that_empties_a_batch_repoints_the_survivors() {
        let mut d = dedup_state(None);
        d.push(&vbatch(&["p"], &[1], &[Some(1)])).unwrap();
        d.push(&vbatch(&["a"], &[10], &[Some(1)])).unwrap();
        // Beats `a`, so the middle batch loses its only winner and must go.
        d.push(&vbatch(&["a"], &[10], &[Some(5)])).unwrap();
        let g = d.greatest.as_mut().unwrap();
        assert_eq!(g.batches.len(), 3);
        g.compact_to_winners().unwrap();
        assert_eq!(g.batches.len(), 2, "the batch whose only winner was beaten is dropped");
        assert_eq!(rows_of(&d.finish().unwrap()), vec![("a".into(), 10, Some(5)), ("p".into(), 1, Some(1))]);
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

    use super::{LegKind, OrderingProbeExec, ordering_violations_by_leg};

    /// A leg whose declared ordering is a LIE: claims timestamp DESC, hands
    /// back ascending rows.
    fn lying_desc_leg(values: Vec<i64>) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(TimestampMicrosecondArray::from(values))]).unwrap();
        let src = MemorySourceConfig::try_new(&[vec![batch]], schema.clone(), None).unwrap();
        let exec = Arc::new(DataSourceExec::new(Arc::new(src))) as Arc<dyn ExecutionPlan>;
        // Declare DESC regardless of the data, as a stale parquet footer does.
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
        datafusion::physical_plan::collect(plan, SessionContext::new().task_ctx()).await.unwrap();
    }

    /// The probe must name the leg that lied and leave the other legs at zero.
    /// Returns the (delta, mem) violation counts this drain added.
    // Ascending rows behind a DESC claim: every step after the first violates.
    #[test_case::test_case(vec![1, 2, 3, 4], LegKind::Delta => with |(delta, mem): (u64, u64)| {
        assert!(delta >= 3, "the lying delta leg must be attributed, got {delta}");
        assert_eq!(mem, 0, "an innocent leg must not be blamed");
    } ; "the_probe_names_the_leg_whose_declared_order_is_false")]
    #[test_case::test_case(vec![9, 8, 7, 6], LegKind::Mem => with |(_, mem): (u64, u64)| {
        assert_eq!(mem, 0, "descending rows honour a DESC claim — nothing to report");
    } ; "an_honest_leg_reports_nothing")]
    #[tokio::test]
    async fn the_probe_attributes_ordering_violations_per_leg(values: Vec<i64>, leg: LegKind) -> (u64, u64) {
        let before = ordering_violations_by_leg();
        drain(Arc::new(OrderingProbeExec::new(lying_desc_leg(values), leg))).await;
        let after = ordering_violations_by_leg();
        let delta = |k: &str| {
            let g = |v: &[(&'static str, u64); 2]| v.iter().find(|(n, _)| *n == k).unwrap().1;
            g(&after) - g(&before)
        };
        (delta("delta"), delta("mem"))
    }

    /// The Delta leg must never be sortable: an UPDATE writes a row's ORIGINAL
    /// timestamp into a NEW file, so its files overlap, and the blocking sort
    /// that would "fix" that exhausts the query pool.
    #[test]
    fn only_the_in_memory_legs_are_sortable() {
        assert!(LegKind::Mem.sortable());
        assert!(!LegKind::Delta.sortable(), "sorting the Delta leg at read time is the 2026-08-02 pool exhaustion");
    }
}

// ===== count_pushdown =====
// COUNT(*) pushdown from Delta add-action statistics: `Σ stats.numRecords`
// over the project's files lying FULLY inside the window, with zero parquet IO.
//
// This module may only ever *decline* (`Ok(None)` → normal scan), never
// approximate. Every gate below must hold: the recognized plan shape; a
// `project_id` equality plus a timestamp window; no MemBuffer rows in the
// window; no duplicates possible; no boundary-straddling file; no deletion
// vector (numRecords is pre-DV); no merge-on-read tombstones.

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
                // Normalize to an INCLUSIVE window: `>`/`<` shrink by 1µs, or a
                // file sitting exactly on a strict bound is counted whole while
                // the predicate excludes its boundary rows.
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
    let Expr::AggregateFunction(AggregateFunction { func, params }) = unalias(&agg.aggr_expr[0]) else { return None };
    let args_ok = match params.args.as_slice() {
        [] => true,
        [Expr::Literal(v, _)] => !v.is_null(),
        _ => false,
    };
    if func.name() != "count" || params.distinct || params.filter.is_some() || !args_ok {
        return None;
    }

    // Walk down: row count is invariant under Projection/SubqueryAlias.
    // Collect Filter predicates and (below) the TableScan's pushed filters.
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
    // The provider must BE the routing table: a bare-name match would let a
    // name-colliding session table be answered from the real Delta stats.
    scan.source.downcast_ref::<DefaultTableSource>().and_then(|src| src.table_provider.downcast_ref::<crate::database::ProjectRoutingTable>())?;

    // The same conjunct commonly appears in both the Filter node and the scan's
    // pushed filters; every fold step below is idempotent, so no dedup needed.
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
/// count would scan everything); a missing upper bound becomes `now`, which
/// keeps the window bounded for the downstream dedup-clean check. Returns
/// `None` when there is no lower bound or the window is empty (`lo > hi`).
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
    let Some(schema) = crate::schema::get_schema(&q.table_name) else { return Ok(None) };
    // A merge-on-read DELETE is an APPEND, so file stats count both the
    // tombstone and the live version it retires and no per-file statistic can
    // correct for it. Gated on tombstones being *possible*, not merely
    // declared. The logical-count index is the one exact answer for such a
    // table, so try it first and decline when it has no covering partition.
    if schema.tombstones_possible() {
        let Some(total) = try_logical_count(database, &q, schema).await else { return Ok(None) };
        debug!("count_pushdown: answered {}/{} [{}, {}] = {} from logical-count index", q.project_id, q.table_name, q.lo, q.hi, total);
        crate::observability::record_logical_count_pushdown_used();
        return count_result(plan, total);
    }

    // Gate: window fully flushed (no MemBuffer rows in range).
    if let Some(layer) = database.buffered_layer()
        && layer.mem_buffer().has_rows_in_range(&q.project_id, &q.table_name, q.lo, q.hi)
    {
        return Ok(None);
    }

    // Hold ONE read guard across the dedup-clean gate and the stats sum, so the
    // verdict applies to exactly the snapshot being summed. The MemBuffer gate
    // must precede this: rows leave the buffer only after their commit swapped
    // the shared table, so anything missing from mem is in this later snapshot.
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

/// The dedup keys after `timestamp`. `None` when the key does not lead with
/// `timestamp`: the index buckets winners by timestamp, so a key that does not
/// start there cannot be grouped by it.
pub fn logical_count_keys(schema: &crate::schema::TableSchema) -> Option<Vec<&str>> {
    let (first, rest) = schema.dedup_keys.split_first()?;
    (first == "timestamp" && !rest.is_empty()).then(|| rest.iter().map(String::as_str).collect())
}

async fn try_logical_count(database: &Arc<Database>, q: &CountQuery, schema: &crate::schema::TableSchema) -> Option<u64> {
    let keys = logical_count_keys(schema)?;
    let tiebreak = schema.dedup_tiebreak.as_deref()?;
    let deleted = schema.tombstone_column.as_deref()?;
    let hi = q.hi.checked_add(1)?;
    let lo_date = chrono::DateTime::from_timestamp_micros(q.lo)?.date_naive();
    let hi_date = chrono::DateTime::from_timestamp_micros(q.hi)?.date_naive();
    let days = (hi_date - lo_date).num_days();
    // The resident budget guarantees four daily indexes at once, i.e. a
    // three-day window crossing four UTC dates. Deeper scans keep the
    // authoritative plan rather than churning the cache.
    if !(0..=3).contains(&days) {
        return None;
    }
    let dates: Vec<_> = (0..=days).map(|offset| lo_date + chrono::Duration::days(offset)).collect();

    // Snapshot the unflushed tail before the Delta snapshot. Flush removes a
    // batch only after publishing its table snapshot, so a transitioning row
    // appears in at least one leg; an equal winner in both is a no-op overlay.
    let (mem_batches, mem_ranges) = match database.buffered_layer() {
        Some(layer) => {
            let snapshot = layer.snapshot_for_merge(&q.project_id, &q.table_name, q.lo, hi).ok()?;
            (snapshot.batches, snapshot.covered_ranges)
        }
        None => (Vec::new(), Vec::new()),
    };
    let table_ref = database.resolve_table(&q.project_id, &q.table_name).await.ok()?;
    let (indexes, missing, added_files, stale_dates, delta_snapshot, log_store) = {
        let table = table_ref.read().await;
        let delta_snapshot = Arc::new(table.snapshot().ok()?.snapshot().clone());
        let mut indexes = Vec::with_capacity(dates.len());
        let mut missing = Vec::new();
        let mut added_files = Vec::new();
        let mut stale_dates = Vec::new();
        for date in &dates {
            let date_string = date.to_string();
            let (_, files) = Database::logical_count_partition_snapshot(&table, &q.project_id, &date_string).ok()?;
            let Some((index, mut added)) = database.logical_count_memory_for_files(&q.project_id, &q.table_name, &date_string, &files) else {
                missing.push(date_string);
                continue;
            };
            indexes.push((*date, index));
            if !added.is_empty() {
                stale_dates.push(date_string);
            }
            added_files.append(&mut added);
        }
        (indexes, missing, added_files, stale_dates, delta_snapshot, table.log_store())
    };
    if !missing.is_empty() {
        for date in missing {
            database.schedule_logical_count_build(&q.project_id, &q.table_name, &date, false);
        }
        return None;
    }

    let columns = crate::read::LogicalCountColumns { timestamp: "timestamp", keys: &keys, tiebreak, deleted };
    // Keep the synchronous append delta small: a large gap falls back to the
    // authoritative DedupExec until the rebuilt base is ready.
    if added_files.len() > crate::read::MAX_APPEND_OVERLAY_FILES {
        for date in stale_dates {
            database.schedule_logical_count_build(&q.project_id, &q.table_name, &date, true);
        }
        return None;
    }
    let covered_ranges = crate::write::mem_buffer::merge_ranges(mem_ranges);
    let delta_batches = database.logical_count_overlay_batches(delta_snapshot, log_store, added_files, columns).await.ok()?;
    indexes.into_iter().try_fold(0u64, |total, (date, index)| {
        let day_lo = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        let day_hi = date.succ_opt()?.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        let input = crate::read::LogicalCountOverlay { authoritative_batches: &mem_batches, delta_batches: &delta_batches, covered_ranges: &covered_ranges };
        let count = index.count_with_covered_overlay(input, q.lo.max(day_lo), hi.min(day_hi), columns).ok()?;
        total.checked_add(count)
    })
}

/// A timestamp stats column as microseconds, or `None` when it is absent or not
/// a timestamp.
pub(crate) fn ts_micros_column(b: &RecordBatch, name: &str) -> Option<Int64Array> {
    use datafusion::arrow::{array::TimestampMicrosecondArray, compute::cast, datatypes::TimeUnit};
    let c = b.column_by_name(name)?;
    matches!(c.data_type(), DataType::Timestamp(_, _)).then_some(())?;
    let c = cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
    Some(c.as_any().downcast_ref::<TimestampMicrosecondArray>()?.reinterpret_cast())
}

/// Extract `(min_ts, max_ts, numRecords)` for this project's files from the
/// flattened add-actions batch and sum the fully-contained ones. `None` on
/// any missing column/stat, DV presence, or boundary straddle.
fn sum_from_actions(actions: &RecordBatch, q: &CountQuery) -> Option<u64> {
    // Deletion vectors make numRecords an over-count — bail if ANY file has one
    // (column families vary by writer, so check every dv-prefixed column).
    if actions.schema().fields().iter().zip(actions.columns()).any(|(f, c)| f.name().starts_with("deletionVector") && c.null_count() < actions.num_rows()) {
        return None;
    }
    let pid = actions.column_by_name("partition.project_id")?.as_any().downcast_ref::<StringArray>()?;
    let records = actions.column_by_name("stats.numRecords")?.as_any().downcast_ref::<Int64Array>()?;
    let min_ts = ts_micros_column(actions, "stats.minValues.timestamp")?;
    let max_ts = ts_micros_column(actions, "stats.maxValues.timestamp")?;
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

    #[test_case::test_case(Some(10), Some(50) => Some((10, 50)) ; "a two-sided window passes through unchanged")]
    #[test_case::test_case(Some(10), None => Some((10, 999)) ; "one-sided timestamp > cutoff takes now as the upper bound")]
    #[test_case::test_case(None, Some(50) => None ; "no lower bound is ineligible - it would scan everything")]
    #[test_case::test_case(Some(60), Some(50) => None ; "an empty window lo > hi is ineligible")]
    fn finalize_window_defaults_open_upper_bound_to_now(lo: Option<i64>, hi: Option<i64>) -> Option<(i64, i64)> {
        finalize_window(lo, hi, 999)
    }
}

// ===== logical_count_index =====
// Exact logical row counts for merge-on-read tables: the winning version of
// each dedup key plus a timestamp histogram, so `COUNT(*)` needs no per-version
// decode. Derived data — callers must bind it to a Delta snapshot fingerprint
// and invalidate or advance it on every write before querying it.

use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::TimestampMicrosecondArray,
    datatypes::{Field, Schema},
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

// Bumped whenever an older on-disk index would be mis-read rather than merely
// stale — e.g. a narrower key tail, or winners not bound to full DV descriptors.
const FORMAT_VERSION: &str = "3";
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

/// Packed immutable winner metadata. IDs live in one shared byte arena, so
/// there is no per-key allocation.
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
    /// One entry per live winner, sorted: two binary searches answer any exact
    /// time window.
    live_timestamps: Vec<i64>,
}

/// Mutable build form plus a packed immutable query form: builders resolve
/// versions in the hash map, then `finalize` releases it before admission.
#[derive(Debug, Clone, Default)]
pub struct LogicalCountIndex {
    winners: HashMap<Box<[u8]>, Winner, ahash::RandomState>,
    packed: Option<PackedIndex>,
    key_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalCountColumns<'a> {
    pub timestamp: &'a str,
    /// The dedup keys after `timestamp`, in schema order. The index must group
    /// by the FULL key or it under-counts.
    pub keys: &'a [&'a str],
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

/// Exact visibility of every immutable Parquet file used to build an index.
pub type CountFiles = std::collections::BTreeMap<String, Option<deltalake::kernel::DeletionVectorDescriptor>>;

fn contains_count_files(current: &CountFiles, base: &CountFiles) -> bool {
    base.iter().all(|(path, dv)| current.get(path) == Some(dv))
}

#[derive(Debug)]
struct CachedPartition {
    fingerprint: u64,
    files: Arc<CountFiles>,
    index: Arc<LogicalCountIndex>,
    estimated_bytes: usize,
    last_access: AtomicU64,
}

/// Process-local front for persistent `.arrow` logical-count partitions.
/// Missing, stale, corrupt, or partially-written entries are ordinary cache
/// misses; query code must fall back to the authoritative scan in every case.
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

    /// Drop an entry and give its bytes back to the resident budget.
    fn remove_entry(&self, key: &CountPartition) {
        if let Some((_, removed)) = self.entries.remove(key) {
            self.resident_bytes.fetch_sub(removed.estimated_bytes, Ordering::Relaxed);
        }
    }

    fn insert_memory(&self, key: CountPartition, fingerprint: u64, files: CountFiles, index: Arc<LogicalCountIndex>) -> bool {
        let estimated_bytes = index.estimated_heap_bytes();
        if estimated_bytes > self.max_resident_bytes {
            return false;
        }
        let _guard = self.admission_lock.lock();
        self.remove_entry(&key);
        while self.resident_bytes.load(Ordering::Relaxed).saturating_add(estimated_bytes) > self.max_resident_bytes {
            let Some(victim) = self.entries.iter().min_by_key(|entry| entry.last_access.load(Ordering::Relaxed)).map(|entry| entry.key().clone()) else {
                break;
            };
            self.remove_entry(&victim);
        }
        self.entries
            .insert(key, CachedPartition { fingerprint, files: Arc::new(files), index, estimated_bytes, last_access: AtomicU64::new(self.next_access()) });
        self.resident_bytes.fetch_add(estimated_bytes, Ordering::Relaxed);
        true
    }

    fn safe_component(value: &str) -> String {
        // Hex-encode every byte: injective over UTF-8 and free of path
        // separators, so distinct tenants can never share a path (escaping
        // only the unsafe bytes collides `a/b` with `a_b`).
        hex::encode(value)
    }

    fn path(&self, key: &CountPartition) -> PathBuf {
        self.root
            .join(Self::safe_component(&key.table_name))
            .join(Self::safe_component(&key.project_id))
            .join(format!("{}.arrow", Self::safe_component(&key.date)))
    }

    /// Install only after a builder has covered the complete physical
    /// partition represented by `fingerprint`.
    pub fn install(&self, key: CountPartition, fingerprint: u64, files: CountFiles, mut index: LogicalCountIndex) -> Result<()> {
        index.finalize()?;
        let path = self.path(&key);
        index.save(&path, fingerprint, &files)?;
        Self::prune_disk_partitions(&path);
        anyhow::ensure!(self.insert_memory(key, fingerprint, files, Arc::new(index)), "logical-count partition exceeds the resident cache budget");
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

    /// Exact index for this fingerprint, loading its Arrow file lazily. Any
    /// validation failure is a cache miss.
    pub fn get(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        self.get_memory(key, fingerprint).or_else(|| {
            let (loaded, files) = LogicalCountIndex::load(&self.path(key), fingerprint).ok()?;
            let loaded = Arc::new(loaded);
            self.insert_memory(key.clone(), fingerprint, files, Arc::clone(&loaded)).then_some(loaded)
        })
    }

    /// Load a cached index whose file set is a subset of `current_files`,
    /// returning the number of added files. A removed base file refuses.
    pub fn load_appendable(&self, key: &CountPartition, current_files: &CountFiles) -> Option<usize> {
        if let Some(entry) = self.entries.get(key) {
            if contains_count_files(current_files, &entry.files) {
                entry.last_access.store(self.next_access(), Ordering::Relaxed);
                return Some(current_files.len() - entry.files.len());
            }
            drop(entry);
            self.invalidate(key);
        }
        let (index, fingerprint, files) = LogicalCountIndex::load_file(&self.path(key)).ok()?;

        if !contains_count_files(current_files, &files) {
            return None;
        }
        let added = current_files.len() - files.len();
        self.insert_memory(key.clone(), fingerprint, files, Arc::new(index)).then_some(added)
    }

    /// Query-path lookup that must never perform filesystem IO: a cold request
    /// falls back to the authoritative scan rather than blocking on disk.
    pub fn get_memory(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        let entry = self.entries.get(key)?;
        if entry.fingerprint != fingerprint {
            return None;
        }
        entry.last_access.store(self.next_access(), Ordering::Relaxed);
        Some(Arc::clone(&entry.index))
    }

    /// Snapshot whose indexed file set is a subset of `current_files`, plus the
    /// added paths to scan as an append overlay. Any removal/rewrite declines,
    /// since the base could then count rows no longer present.
    pub fn get_memory_appendable(&self, key: &CountPartition, current_files: &CountFiles) -> Option<(Arc<LogicalCountIndex>, Vec<String>)> {
        let entry = self.entries.get(key)?;
        if !contains_count_files(current_files, &entry.files) {
            return None;
        }
        entry.last_access.store(self.next_access(), Ordering::Relaxed);
        let added = current_files.keys().filter(|path| !entry.files.contains_key(*path)).cloned().collect();
        Some((Arc::clone(&entry.index), added))
    }

    /// Remove only the memory front; the stale Arrow file stays harmless because
    /// its embedded fingerprint bars reuse after a write.
    pub fn invalidate(&self, key: &CountPartition) {
        let _guard = self.admission_lock.lock();
        self.remove_entry(key);
    }

    pub(crate) fn stats(&self) -> (usize, usize, usize) {
        (self.entries.len(), self.resident_bytes.load(Ordering::Relaxed), self.max_resident_bytes)
    }
}

/// Separator and NULL marker for a multi-column key tail: control characters no
/// identifier can contain, so tuples cannot collide and NULL ≠ empty.
const KEY_SEP: char = '\u{1f}';
const KEY_NULL: char = '\u{0}';

/// The index key is `timestamp_be || tail`, where `tail` is the remaining dedup
/// keys joined by [`KEY_SEP`]. Widening `dedup_keys` requires a
/// `FORMAT_VERSION` bump: mixing tail widths over-counts.
fn key(timestamp: i64, id: &str) -> Box<[u8]> {
    [timestamp.to_be_bytes().as_slice(), id.as_bytes()].concat().into_boxed_slice()
}

/// The non-timestamp dedup-key columns of one batch, resolved once per batch.
struct KeyTail<'a>(Vec<StringValues<'a>>);

impl<'a> KeyTail<'a> {
    fn new(batch: &'a RecordBatch, names: &[&str]) -> Result<Self> {
        names.iter().map(|name| StringValues::new(batch, name)).collect::<Result<Vec<_>>>().map(Self)
    }

    /// Encode row `row` into `out`, reusing its allocation across rows.
    fn encode<'o>(&self, row: usize, out: &'o mut String) -> &'o str {
        out.clear();
        for (i, column) in self.0.iter().enumerate() {
            if i > 0 {
                out.push(KEY_SEP);
            }
            match column.value(row) {
                Some(value) => out.push_str(value),
                None => out.push(KEY_NULL),
            }
        }
        out
    }
}

/// The four narrow columns of a logical-count batch, resolved once per batch:
/// `timestamp`, the remaining dedup keys, the version tiebreak, the tombstone.
struct CountColumns<'a> {
    timestamps: &'a TimestampMicrosecondArray,
    tiebreaks: &'a TimestampMicrosecondArray,
    deleted: &'a BooleanArray,
    tail: KeyTail<'a>,
}

impl<'a> CountColumns<'a> {
    fn new(batch: &'a RecordBatch, columns: LogicalCountColumns<'_>) -> Result<Self> {
        Ok(Self {
            timestamps: column_as(batch, columns.timestamp)?,
            tiebreaks: column_as(batch, columns.tiebreak)?,
            deleted: column_as(batch, columns.deleted)?,
            tail: KeyTail::new(batch, columns.keys)?,
        })
    }

    /// `(timestamp, encoded key tail, winner)` for one row, reusing `buffer`.
    /// `None` when the timestamp is NULL — no bounded predicate matches one.
    fn row<'b>(&self, row: usize, buffer: &'b mut String) -> Option<(i64, &'b str, Winner)> {
        if self.timestamps.is_null(row) {
            return None;
        }
        let winner = Winner {
            tiebreak: (!self.tiebreaks.is_null(row)).then(|| self.tiebreaks.value(row)),
            deleted: !self.deleted.is_null(row) && self.deleted.value(row),
        };
        Some((self.timestamps.value(row), self.tail.encode(row, buffer), winner))
    }
}

fn packed_id<'a>(ids: &'a [u8], winner: &PackedWinner) -> &'a [u8] {
    let start = winner.id_offset as usize;
    let end = start + usize::from(winner.id_len);
    &ids[start..end]
}

/// The timestamp prefix every index key carries (see [`key`]).
fn key_timestamp(key: &[u8]) -> i64 {
    i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"))
}

impl PackedWinner {
    fn winner(&self) -> Winner {
        Winner { tiebreak: (self.flags & FLAG_TIEBREAK_PRESENT != 0).then_some(self.tiebreak), deleted: self.flags & FLAG_DELETED != 0 }
    }
}

impl LogicalCountIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Every stored winner as `(timestamp, id bytes, winner)`, over whichever of
    /// the packed/build representations is live.
    fn entries(&self) -> impl Iterator<Item = (i64, &[u8], Winner)> {
        match &self.packed {
            Some(packed) => itertools::Either::Left(packed.winners.iter().map(move |w| (w.timestamp, packed_id(&packed.ids, w), w.winner()))),
            None => itertools::Either::Right(self.winners.iter().map(|(key, winner)| (key_timestamp(key), &key[8..], *winner))),
        }
    }

    /// Live winners satisfying `keep`, over whichever representation is live.
    fn count_where(&self, keep: impl Fn(i64, Winner) -> bool) -> u64 {
        u64::try_from(self.entries().filter(|&(timestamp, _, winner)| keep(timestamp, winner)).count()).expect("logical-count partition length fits u64")
    }

    /// Apply one physical version; returns whether the logical row changed.
    /// Ordering must match `DedupExec`'s keep-greatest rule: `None` sorts below
    /// every value, and an equal tiebreak does not replace the winner.
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
            let timestamp = key_timestamp(&key);
            let id = &key[8..];
            let id_offset = u32::try_from(packed.ids.len()).context("logical-count ID arena exceeds 4GiB")?;
            let id_len = u16::try_from(id.len()).context("logical-count ID exceeds 65535 bytes")?;
            packed.ids.extend_from_slice(id);
            let flags = (u8::from(winner.tiebreak.is_some()) * FLAG_TIEBREAK_PRESENT) | (u8::from(winner.deleted) * FLAG_DELETED);
            if !winner.deleted {
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
            Some(packed.winners[pos].winner())
        } else {
            self.winners.get(key(timestamp, id).as_ref()).copied()
        }
    }

    /// Apply the four-column narrow form emitted by a count-index build:
    /// `timestamp`, `id`, version tiebreak, tombstone marker.
    pub fn apply_batch(&mut self, batch: &RecordBatch, columns: LogicalCountColumns<'_>) -> Result<usize> {
        let narrow = CountColumns::new(batch, columns)?;
        let mut buffer = String::new();
        let mut changed = 0;
        // Not an iterator chain: each row borrows from the reused `buffer`.
        for row in 0..batch.num_rows() {
            if let Some((timestamp, id, winner)) = narrow.row(row, &mut buffer) {
                changed += usize::from(self.apply(timestamp, id, winner.tiebreak, winner.deleted));
            }
        }
        Ok(changed)
    }

    /// Count under the same coverage contract as the `mem ∪ Delta` scan: rows in
    /// `authoritative_batches` replace covered Delta rows; `delta_batches` are
    /// newly appended files, gated by the same range as the indexed base.
    /// The base index is never cloned or mutated.
    pub fn count_with_covered_overlay(&self, input: LogicalCountOverlay<'_>, lo: i64, hi: i64, columns: LogicalCountColumns<'_>) -> Result<u64> {
        use std::collections::hash_map::Entry;
        let LogicalCountOverlay { authoritative_batches, delta_batches, covered_ranges } = input;
        #[derive(Clone, Copy)]
        struct Overlay {
            base: Option<Winner>,
            current: Winner,
            timestamp: i64,
        }

        let base_visible = |timestamp: i64| !covered_ranges.iter().any(|&(start, end)| (start..end).contains(&timestamp));
        let mut overlay: HashMap<Box<[u8]>, Overlay, ahash::RandomState> = HashMap::default();
        let mut buffer = String::new();
        for (batches, authoritative) in [(authoritative_batches, true), (delta_batches, false)] {
            for batch in batches {
                let narrow = CountColumns::new(batch, columns)?;
                for row in 0..batch.num_rows() {
                    let Some((timestamp, id, candidate)) = narrow.row(row, &mut buffer) else { continue };
                    if !authoritative && !base_visible(timestamp) {
                        continue;
                    }
                    match overlay.entry(key(timestamp, id)) {
                        Entry::Occupied(mut entry) => {
                            if candidate.tiebreak > entry.get().current.tiebreak {
                                entry.get_mut().current = candidate;
                            }
                        }
                        Entry::Vacant(entry) => {
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
        self.count_where(|timestamp, winner| {
            !winner.deleted && (lo..hi).contains(&timestamp) && covered_ranges.iter().any(|&(start, end)| (start..end).contains(&timestamp))
        })
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
        self.count_where(|timestamp, winner| !winner.deleted && (lo..hi).contains(&timestamp))
    }

    pub fn logical_rows(&self) -> u64 {
        match &self.packed {
            Some(packed) => u64::try_from(packed.live_timestamps.len()).expect("logical-count partition length fits u64"),
            None => self.count_where(|_, winner| !winner.deleted),
        }
    }

    pub fn physical_keys(&self) -> usize {
        self.packed.as_ref().map_or(self.winners.len(), |packed| packed.winners.len())
    }

    /// Resident-size estimate for build admission. Deliberately rounds up: this
    /// map lives outside DataFusion's tracked memory pool.
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

    /// Atomically persist the winners as Arrow IPC. `fingerprint` is embedded in
    /// schema metadata and must match the reader's Delta snapshot to be served.
    pub fn save(&self, path: &Path, fingerprint: u64, files: &CountFiles) -> Result<()> {
        use itertools::Itertools;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create logical-count cache directory {}", parent.display()))?;
        }
        let metadata = HashMap::from([
            (META_VERSION.to_string(), FORMAT_VERSION.to_string()),
            (META_FINGERPRINT.to_string(), fingerprint.to_string()),
            (META_FILES.to_string(), serde_json::to_string(files).context("serialize logical-count file set")?),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(ipc_fields(), metadata));
        let tmp = path.with_extension(format!("arrow.tmp-{}", uuid::Uuid::new_v4()));
        let write = || -> Result<()> {
            let file = File::create(&tmp).with_context(|| format!("create logical-count cache {}", tmp.display()))?;
            let mut writer = FileWriter::try_new(file, schema.as_ref())?;
            // Stream bounded batches: materializing all rows at once doubles
            // peak memory, and IPC order is irrelevant to correctness.
            const WRITE_ROWS: usize = 64 * 1024;
            for chunk in &self.entries().chunks(WRITE_ROWS) {
                let rows = chunk
                    .map(|(timestamp, id, winner)| Ok((timestamp, std::str::from_utf8(id).context("logical-count key contains non-UTF8 id")?, winner)))
                    .collect::<Result<Vec<_>>>()?;
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                        Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.2.tiebreak))),
                        Arc::new(BooleanArray::from_iter(rows.iter().map(|row| Some(row.2.deleted)))),
                    ],
                )?;
                writer.write(&batch)?;
            }
            writer.finish()?;
            std::fs::rename(&tmp, path).with_context(|| format!("publish logical-count cache {}", path.display()))?;
            Ok(())
        };
        write().inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })
    }

    /// Load only when the file belongs to the caller's exact snapshot.
    pub fn load(path: &Path, expected_fingerprint: u64) -> Result<(Self, CountFiles)> {
        let (index, fingerprint, files) = Self::load_file(path)?;
        if fingerprint != expected_fingerprint {
            bail!("logical-count cache fingerprint mismatch: cached={fingerprint} current={expected_fingerprint}");
        }
        Ok((index, files))
    }

    fn load_file(path: &Path) -> Result<(Self, u64, CountFiles)> {
        let file = File::open(path).with_context(|| format!("open logical-count cache {}", path.display()))?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();
        if schema.metadata().get(META_VERSION).map(String::as_str) != Some(FORMAT_VERSION) {
            bail!("unsupported logical-count cache format");
        }
        let expected = ipc_fields();
        if schema.fields().len() != expected.len()
            || schema
                .fields()
                .iter()
                .zip(&expected)
                .any(|(field, want)| field.name() != want.name() || field.data_type() != want.data_type() || field.is_nullable() != want.is_nullable())
        {
            bail!("logical-count cache has an incompatible Arrow schema");
        }
        let fingerprint = schema
            .metadata()
            .get(META_FINGERPRINT)
            .context("logical-count cache missing fingerprint")?
            .parse::<u64>()
            .context("logical-count cache fingerprint is invalid")?;
        let files: CountFiles = serde_json::from_str(schema.metadata().get(META_FILES).context("logical-count cache missing file set")?)
            .context("logical-count cache file set is invalid")?;

        let mut index = Self::new();
        for batch in reader {
            let batch = batch?;
            // Safe to address by name: the schema check above proved the layout.
            let timestamps = column_as::<Int64Array>(&batch, "timestamp")?;
            let ids = column_as::<StringArray>(&batch, "id")?;
            let tiebreaks = column_as::<Int64Array>(&batch, "tiebreak")?;
            let deleted = column_as::<BooleanArray>(&batch, "deleted")?;
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

/// The one definition of the persisted winner table: `load_file` refuses any
/// file whose schema does not match it exactly.
fn ipc_fields() -> Vec<Field> {
    vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("id", DataType::Utf8, false),
        Field::new("tiebreak", DataType::Int64, true),
        Field::new("deleted", DataType::Boolean, false),
    ]
}

/// One named column as its concrete Arrow array type. The downcast IS the type
/// check: a wrong timestamp unit fails here rather than being misread.
fn column_as<'a, A: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a A> {
    let column = batch.column_by_name(name).with_context(|| format!("logical-count batch missing {name}"))?;
    column.as_any().downcast_ref::<A>().with_context(|| format!("logical-count {name} has unsupported type {}", column.data_type()))
}

enum StringValues<'a> {
    View(&'a StringViewArray),
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl<'a> StringValues<'a> {
    /// `None` for any non-string column.
    fn from_column(column: &'a ArrayRef) -> Option<Self> {
        let any = column.as_any();
        any.downcast_ref::<StringViewArray>()
            .map(Self::View)
            .or_else(|| any.downcast_ref::<StringArray>().map(Self::Utf8))
            .or_else(|| any.downcast_ref::<LargeStringArray>().map(Self::Large))
    }

    fn new(batch: &'a RecordBatch, name: &str) -> Result<Self> {
        let column = batch.column_by_name(name).with_context(|| format!("logical-count batch missing {name}"))?;
        Self::from_column(column).with_context(|| format!("logical-count {name} has unsupported type {}", column.data_type()))
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
    use arrow::datatypes::TimeUnit;

    use super::*;

    fn unmasked(paths: &[&str]) -> super::CountFiles {
        paths.iter().map(|path| ((*path).to_owned(), None)).collect()
    }

    fn part(project_id: &str, date: &str) -> CountPartition {
        CountPartition { project_id: project_id.into(), table_name: "otel".into(), date: date.into() }
    }

    fn cols() -> LogicalCountColumns<'static> {
        LogicalCountColumns { timestamp: "timestamp", keys: &["id"], tiebreak: "updated_at", deleted: "deleted" }
    }

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

    fn index_of(rows: &[(i64, &str, Option<i64>, bool)]) -> LogicalCountIndex {
        let mut index = LogicalCountIndex::new();
        for &(timestamp, id, tiebreak, deleted) in rows {
            index.apply(timestamp, id, tiebreak, deleted);
        }
        index
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
        let mut index = index_of(&[(42, "a", Some(1), false), (42, "b", Some(1), false), (42, "c", Some(1), true)]);
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
        let columns = cols();
        let mut index = LogicalCountIndex::new();
        index.apply_batch(&versions(&[(10, "a", Some(1), Some(false)), (20, "b", Some(1), None), (30, "gone", Some(2), Some(true))]), columns).unwrap();
        assert_eq!(index.count(0, 100), 2);

        // a tombstoned, b a stale no-op then a repeat with an equal tiebreak, gone resurrected, c new and unflushed.
        let tail = [versions(&[
            (10, "a", Some(3), Some(true)),
            (20, "b", Some(0), Some(true)),
            (20, "b", Some(1), Some(false)),
            (30, "gone", Some(4), Some(false)),
            (40, "c", Some(1), None),
        ])];
        let overlay = LogicalCountOverlay { authoritative_batches: &tail, delta_batches: &[], covered_ranges: &[] };
        assert_eq!(index.count_with_covered_overlay(overlay, 0, 100, columns).unwrap(), 3);
        assert_eq!(index.logical_rows(), 2, "overlay must not mutate the persistent base");
    }

    #[test]
    fn covered_overlay_replaces_delta_rows_like_the_union_scan() {
        let columns = cols();
        let mut index = index_of(&[(10, "old", Some(1), false), (20, "newer-delta", Some(5), false)]);
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
        let index = index_of(&[(10, "a", None, false), (10, "a", Some(2), true), (60_000_001, "b", Some(3), false)]);
        let files = unmasked(&["date=2026-08-04/a.parquet"]);
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
        index.save(&path, 1, &unmasked(&["large.parquet"])).unwrap();
        let (loaded, _) = LogicalCountIndex::load(&path, 1).unwrap();
        assert_eq!(loaded.physical_keys(), 70_000);
        assert_eq!(loaded.count(0, 70_000), 70_000);
    }

    #[test]
    fn cache_lazily_loads_only_matching_partition_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let key = part("p/unsafe", "2026-08-04");
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        let index = index_of(&[(42, "id", Some(1), false)]);
        cache.install(key.clone(), 7, unmasked(&["a.parquet"]), index).unwrap();
        assert_eq!(cache.get(&key, 7).unwrap().logical_rows(), 1);
        assert!(cache.get(&key, 8).is_none());

        let restarted = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        assert_eq!(restarted.get(&key, 7).unwrap().count(0, 100), 1);
        let current = unmasked(&["a.parquet", "b.parquet"]);
        let (_, added) = restarted.get_memory_appendable(&key, &current).unwrap();
        assert_eq!(added, vec!["b.parquet"]);
        let append_restart = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        assert_eq!(append_restart.load_appendable(&key, &current), Some(1));
        assert_eq!(append_restart.get_memory_appendable(&key, &current).unwrap().1, vec!["b.parquet"]);
        let far_ahead: CountFiles = std::iter::once("a.parquet".to_string())
            .chain((0..=MAX_APPEND_OVERLAY_FILES).map(|i| format!("new-{i}.parquet")))
            .map(|path| (path, None))
            .collect();
        assert_eq!(append_restart.load_appendable(&key, &far_ahead), Some(MAX_APPEND_OVERLAY_FILES + 1));
        let rewritten = unmasked(&["replacement.parquet"]);
        assert!(restarted.get_memory_appendable(&key, &rewritten).is_none(), "a removed base file must fail closed");
        restarted.invalidate(&key);
        assert!(restarted.get(&key, 8).is_none());
        assert!(dir.path().join("6f74656c/702f756e73616665/323032362d30382d3034.arrow").exists());
    }

    #[test]
    fn legacy_logical_count_files_require_rebuilding() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy.arrow");
        for version in ["1", "2"] {
            let schema = Schema::empty().with_metadata([(META_VERSION.to_owned(), version.to_owned())].into());
            let mut writer = FileWriter::try_new(std::fs::File::create(&path).unwrap(), &schema).unwrap();
            writer.finish().unwrap();
            assert!(LogicalCountIndex::load(&path, 0).unwrap_err().to_string().contains("unsupported logical-count cache format"));
        }
    }

    #[test]
    fn same_path_visibility_changes_reject_memory_and_disk_counts() {
        use deltalake::kernel::{DeletionVectorDescriptor, StorageType::UuidRelativePath};
        let dv = |offset| {
            let path_or_inline_dv = "same-sidecar".into();
            Some(DeletionVectorDescriptor { storage_type: UuidRelativePath, path_or_inline_dv, offset: Some(offset), size_in_bytes: 10, cardinality: 1 })
        };
        for (before, after) in [(None, dv(1)), (dv(1), dv(2)), (dv(1), None)] {
            let dir = tempfile::tempdir().unwrap();
            let key = part("p", "2026-08-14");
            let cache = LogicalCountCache::new(dir.path().to_owned(), usize::MAX);
            let files = [("same.parquet".into(), before)].into_iter().collect();
            cache.install(key.clone(), 1, files, LogicalCountIndex::new()).unwrap();
            let changed = [("same.parquet".into(), after)].into_iter().collect();
            assert!(cache.get_memory_appendable(&key, &changed).is_none());
            let restarted = LogicalCountCache::new(dir.path().to_owned(), usize::MAX);
            assert!(restarted.load_appendable(&key, &changed).is_none());
        }
    }

    #[test]
    fn cache_paths_cannot_alias_distinct_partition_names() {
        let cache = LogicalCountCache::new(PathBuf::from("unused"), usize::MAX);
        assert_ne!(cache.path(&part("a/b", "2026-08-04")), cache.path(&part("a_b", "2026-08-04")));
    }

    #[test]
    fn resident_cache_evicts_the_least_recent_partition_within_budget() {
        let dir = tempfile::tempdir().unwrap();
        let key = |project: &str| part(project, "2026-08-04");
        let mut first = index_of(&[(1, "a", Some(1), false)]);
        first.finalize().unwrap();
        let per_entry = first.estimated_heap_bytes();
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), per_entry);
        cache.install(key("a"), 1, unmasked(&["a.parquet"]), first).unwrap();
        assert!(cache.get_memory(&key("a"), 1).is_some());

        let second = index_of(&[(2, "b", Some(1), false)]);
        cache.install(key("b"), 2, unmasked(&["b.parquet"]), second).unwrap();
        assert!(cache.get_memory(&key("a"), 1).is_none());
        assert!(cache.get_memory(&key("b"), 2).is_some());
        assert!(cache.resident_bytes.load(Ordering::Relaxed) <= per_entry);
    }

    #[test]
    fn disk_cache_keeps_only_the_newest_completed_daily_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        for day in 1..=DISK_PARTITIONS_PER_PROJECT + 3 {
            let key = part("p", &format!("2026-08-{day:02}"));
            cache.install(key, u64::try_from(day).unwrap(), CountFiles::new(), LogicalCountIndex::new()).unwrap();
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
// HyperLogLog: a mergeable, bounded distinct-count sketch, in two
// representations behind one type — Sparse (the exact hash set, up to
// [`SPARSE_MAX`], so the estimate is exact) and Dense ([`M`] one-byte
// registers, ~1.6% standard error).
//
// Sketches are PERSISTED in rollup tables and merged months later, so both the
// hash function and the wire format are frozen: changing `SEED`, `P` or the tag
// bytes requires a new rollup spec name, exactly like changing a measure.

/// Register-index bits. 12 → 4096 registers, 4 KiB dense, ~1.6% standard error.
const P: u32 = 12;
/// Register count.
const M: usize = 1 << P;
/// Sparse→dense conversion point: 512 × 8 B = 4 KiB, matching the dense `M`.
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

/// Stable 64-bit hash. Frozen — see the module note on persistence.
#[inline]
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut acc = SEED ^ (bytes.len() as u64);
    let (chunks, remainder) = bytes.as_chunks::<8>();
    for chunk in chunks {
        acc = mix(acc ^ u64::from_le_bytes(*chunk));
    }
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

/// Raise the register this hash lands in. The hash's register index is its top
/// `P` bits; the value is the 1-based position of the first set bit in the
/// remaining suffix.
#[inline]
fn set_register(registers: &mut [u8; M], hash: u64) {
    let index = (hash >> (64 - P)) as usize;
    // `| 1` bounds rho at 64-P+1 without a branch: the sentinel bit stops the
    // count when the whole suffix is zero.
    let rho = ((hash << P) | 1).leading_zeros() as u8 + 1;
    registers[index] = registers[index].max(rho);
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
            Self::Dense(registers) => set_register(registers, hash),
        }
    }

    fn densify(&mut self) {
        let Self::Sparse(hashes) = self else { return };
        let mut registers = Box::new([0u8; M]);
        hashes.iter().for_each(|&hash| set_register(&mut registers, hash));
        *self = Self::Dense(registers);
    }

    /// Union. Associative and commutative, which is what lets this live in a rollup.
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
            (Self::Dense(mine), Self::Sparse(theirs)) => theirs.iter().for_each(|&hash| set_register(mine, hash)),
            (Self::Dense(mine), Self::Dense(theirs)) => mine.iter_mut().zip(theirs.iter()).for_each(|(slot, &their)| *slot = (*slot).max(their)),
        }
    }

    /// Estimated distinct count — exact while sparse.
    pub fn estimate(&self) -> u64 {
        let registers = match self {
            Self::Sparse(hashes) => return hashes.len() as u64,
            Self::Dense(registers) => registers,
        };
        let zeros = registers.iter().filter(|&&r| r == 0).count();
        // Linear counting: the better estimator while most registers are empty,
        // and it keeps the sparse→dense seam continuous.
        if zeros > 0 {
            let linear = M as f64 * (M as f64 / zeros as f64).ln();
            if linear <= 2.5 * M as f64 {
                return linear.round() as u64;
            }
        }
        // Flajolet's harmonic-mean estimator. `alpha` is the standard bias
        // constant for m >= 128.
        let alpha = 0.7213 / (1.0 + 1.079 / M as f64);
        // 2^-r built as an IEEE-754 exponent, not `powi`: vectorizable, and
        // total for any `r: u8` — the exponent cannot underflow, so a corrupt
        // payload yields a finite positive number rather than UB.
        let harmonic: f64 = registers.iter().map(|&r| f64::from_bits((1023 - u64::from(r)) << 52)).sum();
        (alpha * (M * M) as f64 / harmonic).round() as u64
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Sparse(hashes) => [TAG_SPARSE].into_iter().chain(hashes.iter().flat_map(|hash| hash.to_le_bytes())).collect(),
            Self::Dense(registers) => [TAG_DENSE].into_iter().chain(registers.iter().copied()).collect(),
        }
    }

    /// Decode. A malformed payload errors rather than decoding to an empty
    /// sketch, which would under-report a cardinality forever.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        match bytes {
            [] => Ok(Self::default()),
            [TAG_SPARSE, rest @ ..] if rest.len() % 8 == 0 => Ok(Self::Sparse(rest.as_chunks::<8>().0.iter().copied().map(u64::from_le_bytes).collect())),
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

    fn merged(left: std::ops::Range<u64>, right: std::ops::Range<u64>) -> Hll {
        let mut sketch = sketch_of(left);
        sketch.merge(&sketch_of(right));
        sketch
    }

    /// Sparse mode is exact, not an approximation.
    #[test]
    fn small_cardinalities_are_exact() {
        for n in [0u64, 1, 7, 100, SPARSE_MAX as u64] {
            assert_eq!(sketch_of(0..n).estimate(), n, "sparse must be exact at {n}");
        }
    }

    /// Dense estimates stay inside the error bound and `merge` is an
    /// order-independent union that does not double-count the overlap. The 5%
    /// tolerance is 3 sigma on a 1.6% standard error.
    #[test_case::test_case(sketch_of(0..5_000), 5_000 ; "dense at 5k")]
    #[test_case::test_case(sketch_of(0..50_000), 50_000 ; "dense at 50k")]
    #[test_case::test_case(sketch_of(0..1_000_000), 1_000_000 ; "dense at 1M")]
    #[test_case::test_case(merged(0..30_000, 20_000..50_000), 50_000 ; "union is not a sum: the overlap is not double-counted")]
    #[test_case::test_case(merged(0..10, 0..20_000), 20_000 ; "sparse merged into dense")]
    #[test_case::test_case(merged(0..20_000, 0..10), 20_000 ; "dense merged into sparse")]
    fn estimates_land_within_the_error_bound(sketch: Hll, truth: u64) {
        let estimate = sketch.estimate();
        let error = (estimate as f64 - truth as f64).abs() / truth as f64;
        assert!(error < 0.05, "truth={truth}: estimated {estimate}, error {:.3}%", error * 100.0);
    }

    /// Byte round-trips must be exact and truncated payloads must be loud.
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

    proptest::proptest! {
        /// 400 hashes a side is enough for a union to cross `SPARSE_MAX` and
        /// densify inside `merge`.
        #[test]
        fn merge_is_commutative_and_bytes_round_trip(
            left in proptest::collection::vec(proptest::prelude::any::<u64>(), 0..400),
            right in proptest::collection::vec(proptest::prelude::any::<u64>(), 0..400),
        ) {
            let build = |hashes: &[u64]| hashes.iter().fold(Hll::default(), |mut hll, &h| { hll.insert_hash(h); hll });
            let (mut a, mut b) = (build(&left), build(&right));
            a.merge(&build(&right));
            b.merge(&build(&left));
            proptest::prop_assert_eq!(&a, &b);
            proptest::prop_assert_eq!(Hll::from_bytes(&a.to_bytes()).unwrap(), a);
        }
    }

    /// A dense sketch is bounded however many rows it sees — that bound is what
    /// lets a rollup row carry one.
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
