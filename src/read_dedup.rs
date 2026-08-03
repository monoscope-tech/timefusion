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
        array::{ArrayRef, BooleanArray, RecordBatch},
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
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream, stream::RecordBatchStreamAdapter,
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
    fn advance(&mut self, t: i64) -> bool {
        self.last.is_none_or(|l| if self.desc { t < l } else { t > l }) && self.last.replace(t).is_some()
    }
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

/// Enable bounded mode iff the input's leading sort column is a dedup key of an
/// i64-backed type. That guarantees equal dedup keys share the bound value, so
/// clearing on a bound advance can never evict a key that could still recur.
fn detect_bound(input: &Arc<dyn ExecutionPlan>, keys: &[String], in_schema: &SchemaRef) -> Option<Bound> {
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
        Ok(Self { input, keys, key_idxs, tiebreak, required_ordering: None, output_projection, schema, properties })
    }

    /// Declare the ordering keep-greatest needs (see `required_ordering`).
    /// `None` leaves the operator ordering-agnostic — the pre-merge-on-read
    /// behaviour every table without `version_append` keeps.
    pub fn requiring(mut self, ordering: Option<datafusion::physical_expr::LexOrdering>) -> Self {
        self.required_ordering = ordering;
        self
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
        match detect_bound(&self.input, &self.keys, &in_schema) {
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
        let bound = detect_bound(&self.input, &self.keys, &in_schema);
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
        let dedup = Dedup {
            key_idxs: self.key_idxs.clone(),
            conv: RowConverter::new(self.key_idxs.iter().map(|&i| SortField::new(in_schema.field(i).data_type().clone())).collect()).map_err(arrow_err)?,
            output_projection: self.output_projection.clone(),
            seen: SeenSet::default(),
            bound,
            greatest,
        };

        let input = self.input.execute(partition, context)?;
        let out_schema = self.schema.clone();
        // One input batch can yield several output batches (a keep-greatest
        // flush emits one per buffered batch) or none at all; `flat_map` fans
        // them out lazily — an empty result just re-polls the source, and a
        // downstream LIMIT can stop mid-run.
        let stream = futures::stream::unfold((input, dedup, false), |(mut input, mut dedup, done)| async move {
            if done {
                return None;
            }
            let (produced, done) = match input.next().await {
                None => (dedup.finish(), true),
                Some(Err(e)) => (Err(e), false),
                Some(Ok(batch)) => (dedup.push(&batch), false),
            };
            Some((produced, (input, dedup, done)))
        })
        .flat_map(|r| futures::stream::iter(r.map_or_else(|e| vec![Err(e)], |bs| bs.into_iter().map(Ok).collect::<Vec<_>>())));

        Ok(Box::pin(RecordBatchStreamAdapter::new(out_schema, stream)))
    }
}

/// Winning row for a dedup key within the open run: where it sits in the
/// buffered batches, plus its order-encoded tiebreak for comparison.
struct Cand {
    batch: u32,
    row: u32,
    tb: Box<[u8]>,
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
    conv: RowConverter,
    best: HashMap<Box<[u8]>, Cand, ahash::RandomState>,
    batches: Vec<RecordBatch>,
    bytes: usize,
    /// Pool accounting for `batches` (`bytes` mirrors its size). The winner
    /// map is second-order (one small entry per key) and stays untracked.
    reservation: MemoryReservation,
}

impl Greatest {
    fn new(idx: usize, dt: &DataType, reservation: MemoryReservation) -> DFResult<Self> {
        let sf = SortField::new_with_options(dt.clone(), SortOptions { descending: false, nulls_first: true });
        Ok(Self { idx, conv: RowConverter::new(vec![sf]).map_err(arrow_err)?, best: HashMap::default(), batches: Vec::new(), bytes: 0, reservation })
    }

    /// Emit the run's winners in input position order and reset. `partial` (a
    /// `RUN_BUFFER_MAX_BYTES` overflow, not a real run boundary) parks the
    /// emitted keys in `seen` so the run's tail can't re-emit them — that tail
    /// degrades to keep-first.
    fn flush(&mut self, output_projection: Option<&[usize]>, seen: &mut SeenSet, partial: bool) -> DFResult<Vec<RecordBatch>> {
        self.bytes = 0;
        self.reservation.free();
        let batches = std::mem::take(&mut self.batches);
        if self.best.is_empty() {
            return Ok(Vec::new());
        }
        let mut masks: Vec<Vec<bool>> = batches.iter().map(|b| vec![false; b.num_rows()]).collect();
        for (k, c) in self.best.drain() {
            masks[c.batch as usize][c.row as usize] = true;
            if partial {
                seen.insert(k);
            }
        }
        batches
            .into_iter()
            .zip(masks)
            .map(|(b, m)| project_out(filter_record_batch(&b, &BooleanArray::from(m)).map_err(arrow_err)?, output_projection))
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
}

impl Dedup {
    fn push(&mut self, batch: &RecordBatch) -> DFResult<Vec<RecordBatch>> {
        let key_arrays: Vec<ArrayRef> = self.key_idxs.iter().map(|&i| batch.column(i).clone()).collect();
        let keys = self.conv.convert_columns(&key_arrays).map_err(arrow_err)?;
        let Some(g) = self.greatest.as_mut() else {
            return Ok(dedup_first(batch, &keys, &mut self.seen, self.output_projection.as_deref(), self.bound.as_mut())?.into_iter().collect());
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
            out.extend(g.flush(proj, &mut self.seen, true)?);
        }
        let tbs = g.conv.convert_columns(&[batch.column(g.idx).clone()]).map_err(arrow_err)?;
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
                out.extend(g.flush(proj, &mut self.seen, false)?);
                self.seen.clear();
                cur = None;
            }
            let key = keys.row(i).data();
            if self.seen.contains(key) {
                continue;
            }
            let tb = tbs.row(i).data();
            if g.best.get(key).is_some_and(|c| tb <= &c.tb[..]) {
                continue;
            }
            let bi = match cur {
                Some(bi) => bi,
                None => {
                    // Pool BEFORE buffering: on ResourcesExhausted the query
                    // fails here instead of the cgroup killing the server.
                    let size = batch.get_array_memory_size();
                    if self.bound.is_none() {
                        check_unbounded_growth(g.bytes, size)?;
                    }
                    g.reservation.try_grow(size)?;
                    g.batches.push(batch.clone());
                    g.bytes += size;
                    let bi = g.batches.len() as u32 - 1;
                    cur = Some(bi);
                    bi
                }
            };
            g.best.insert(key.into(), Cand { batch: bi, row: i as u32, tb: tb.into() });
        }
        Ok(out)
    }

    /// End of stream: emit the still-open run.
    fn finish(&mut self) -> DFResult<Vec<RecordBatch>> {
        match self.greatest.as_mut() {
            Some(g) => g.flush(self.output_projection.as_deref(), &mut self.seen, false),
            None => Ok(Vec::new()),
        }
    }
}

/// Restore the requested projection on a surviving batch.
fn project_out(b: RecordBatch, output_projection: Option<&[usize]>) -> DFResult<RecordBatch> {
    match output_projection {
        Some(idxs) => b.project(idxs).map_err(arrow_err),
        None => Ok(b),
    }
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
    let out = project_out(filter_record_batch(batch, &mask).map_err(arrow_err)?, output_projection)?;
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
