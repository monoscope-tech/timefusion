//! Deferred, batched Delta legs for `UPDATE ... FROM` (DML coalescing), plus
//! the flush-watermark predicate clamp shared with the synchronous DML path.
//!
//! Why: one Delta MERGE commit per statement (monoscope's hash tagging runs
//! ~1.4k/hr) starves OPTIMIZE via OCC conflicts, accumulates small files, and
//! pays a full copy-on-write parquet rewrite per handful of rows. The mem-leg
//! (synchronous MemBuffer mutation, WAL-backed) already gives read-your-writes
//! through the scan overlay, so the Delta leg is pure durability convergence —
//! it can be deferred and batched.
//!
//! Grouping: statements coalesce when (project, table, join keys, assignments,
//! non-time residual predicate, source schema) all match; per-statement
//! timestamp-range conjuncts are widened to the union window. Same-key source
//! rows with different payloads (e.g. two tags for one span) cannot share one
//! MERGE (Delta forbids duplicate source matches), so the drained batch splits
//! into ordered rounds — round N holds each key's Nth occurrence.
//!
//! Contract (see `d_dml_coalesce_secs`): deferred statements must be
//! idempotent under re-application. A row flushed between the mem leg and the
//! drain sees the assignment applied twice, and a failed drain retries whole
//! groups (including rounds that already committed).
//!
//! Durability: the mem leg WAL-appends `UpdateWithSource` before enqueue, so
//! buffer-resident rows survive a crash with their post-DML values. What a
//! crash CAN lose is the deferred Delta leg for rows that were already in
//! Delta when the statement ran — bounded by the drain interval.
//!
//! A group that exhausts `MAX_DRAIN_ATTEMPTS` is **parked**, not dropped: its
//! rows go to `<wal_dir>/quarantine/dml` as Arrow IPC + a `.meta` sidecar
//! (`timefusion.dml.coalesce_quarantined`). Dropping was unrecoverable — the
//! Delta leg targets rows already flushed out of the buffer, so there is no
//! newer copy to converge from and read-side dedup (first-seen-wins) cannot
//! repair it. `timefusion.dml.coalesce_dropped` now means the *quarantine
//! write itself* failed, i.e. genuine loss.

use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use datafusion::{
    arrow::{
        array::{RecordBatch, UInt32Array},
        compute::{concat_batches, take},
        datatypes::SchemaRef,
        row::{RowConverter, SortField},
    },
    catalog::Session,
    common::ScalarValue,
    error::{DataFusionError, Result},
    logical_expr::{BinaryExpr, Expr, Operator, utils::split_conjunction},
    prelude::lit,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::dml::UpdateSource;

/// Queue-size pressure threshold: total buffered source rows above which a
/// drain is triggered immediately instead of waiting for the timer. Matches
/// `MAX_UPDATE_SOURCE_ROWS` — a drained group must stay mergeable.
const MAX_QUEUED_SOURCE_ROWS: usize = 1_000_000;

/// Drain attempts per group before it is quarantined (each drain already
/// carries perform_delta_merge_update's 4-attempt OCC retry underneath).
const MAX_DRAIN_ATTEMPTS: u32 = 3;

/// Max source rows fed to a single Delta MERGE. `MAX_QUEUED_SOURCE_ROWS` only
/// *notifies* a drain, so a group can grow past it unbounded — on 2026-07-27
/// one reached 1_252_311 rows (7457 statements) and every MERGE attempt died
/// with "Resources exhausted", costing the whole group. Rounds are therefore
/// chunked: many bounded merges instead of one unbounded one. Each chunk is an
/// independent commit, which the idempotence contract already permits.
const MAX_MERGE_ROWS: usize = 100_000;

/// Zero-copy slices of `batch` of at most `max` rows, covering every row once.
fn chunk_rows(batch: &RecordBatch, max: usize) -> impl Iterator<Item = RecordBatch> + '_ {
    (0..batch.num_rows()).step_by(max).map(move |off| batch.slice(off, max.min(batch.num_rows() - off)))
}

/// Persist a terminally-failed group's source rows as an Arrow IPC file plus a
/// `.meta` sidecar, so the Delta leg can be re-driven instead of lost.
///
/// Before this existed the terminal branch logged and dropped: 2026-07-27
/// 04:42Z lost 1_252_311 enrichment rows that way. The loss is permanent
/// without a sidecar — the mem leg already applied, and the Delta leg is
/// watermark-clamped to rows that have *already* flushed and left the buffer,
/// so Delta keeps stale pre-DML values with no newer copy anywhere (read-side
/// dedup is first-seen-wins and cannot repair it).
///
/// Returns false when nothing could be persisted; the caller must then keep
/// the loud error path, because the rows are genuinely gone.
fn quarantine_group(dir: &std::path::Path, key: &GroupKey, group: &PendingGroup, batches: &[RecordBatch], reason: &str) -> bool {
    if let Err(e) = std::fs::create_dir_all(dir) {
        error!("dml quarantine: cannot create {dir:?}: {e}");
        return false;
    }
    // Schema drift is itself a quarantine reason (concat failure), so keep
    // only what this IPC file can actually hold and say so if any are left.
    let (writable, skipped): (Vec<&RecordBatch>, usize) = {
        let keep: Vec<&RecordBatch> = batches.iter().filter(|b| b.schema() == group.schema).collect();
        let skipped = batches.len() - keep.len();
        (keep, skipped)
    };
    if writable.is_empty() {
        error!(
            "dml quarantine: no batch matches the group schema for {}/{} — {} rows LOST: {reason}",
            key.project_id,
            key.table_name,
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );
        return false;
    }
    let rows: usize = writable.iter().copied().map(RecordBatch::num_rows).sum();
    // Fingerprint disambiguates two groups for the same project/table parked in
    // the same microsecond; `create_new` then turns any residual collision into
    // an error rather than a silent truncation of already-parked user data.
    let stem = format!("{}_{:016x}_{}__{}", crate::clock::now_micros(), key.fingerprint, key.project_id, key.table_name).replace(['/', '\\', ':', '\0'], "_");
    let (path, file) = match (0..16).find_map(|n| {
        let p = dir.join(if n == 0 { format!("{stem}.arrow") } else { format!("{stem}-{n}.arrow") });
        crate::buffered_write_layer::create_owner_only(&p, true).ok().map(|f| (p, f))
    }) {
        Some(pf) => pf,
        None => {
            error!("dml quarantine: cannot create a unique payload file under {dir:?} for {}/{}", key.project_id, key.table_name);
            return false;
        }
    };

    // Arrow IPC (not raw bytes): self-describing schema, so a re-drive needs
    // only the sidecar for the merge shape. Streamed straight to the file —
    // buffering a multi-GB group in a Vec first risks OOM at the exact moment
    // memory exhaustion is what brought us here.
    match datafusion::arrow::ipc::writer::FileWriter::try_new(std::io::BufWriter::new(file), &group.schema) {
        Ok(mut w) => {
            if let Err(e) = writable.iter().try_for_each(|b| w.write(b)).and_then(|()| w.finish()) {
                error!("dml quarantine: IPC write failed for {path:?}: {e}");
                return false;
            }
        }
        Err(e) => {
            error!("dml quarantine: IPC writer init failed for {path:?}: {e}");
            return false;
        }
    }

    let meta = format!(
        "project_id={}\ntable_name={}\nfolded_projects={}\njoin_keys={}\nassignments={}\npredicate={}\ntime_col={}\nattempts={}\nrows={}\nstatements={}\nreason={reason}\n",
        key.project_id,
        key.table_name,
        group.folded_projects.as_ref().map_or(String::new(), |p| p.join(",")),
        group.join_keys.iter().map(|(t, s)| format!("{t}={s}")).collect::<Vec<_>>().join(","),
        group.assignments.iter().map(|(c, e)| format!("{c}:={e}")).collect::<Vec<_>>().join(","),
        group.predicate.reconstruct(group.time_col).map_or(String::new(), |e| e.to_string()),
        group.time_col,
        group.attempts,
        rows,
        group.batches.len(),
    );
    if let Err(e) = crate::buffered_write_layer::write_owner_only(&path.with_extension("meta"), meta.as_bytes()) {
        error!("dml quarantine: meta write failed for {path:?}: {e}");
    }
    error!("dml quarantine: parked {}/{} ({rows} rows) at {path:?}: {reason}", key.project_id, key.table_name);
    crate::metrics::record_dml_coalesce_quarantined();
    if skipped > 0 {
        // Partially parked is partially LOST — the skipped batches have no
        // other copy. Page on it, or the recoverable-looking quarantine metric
        // would mask real loss.
        let lost: usize = batches.iter().filter(|b| b.schema() != group.schema).map(RecordBatch::num_rows).sum();
        crate::metrics::record_dml_coalesce_dropped();
        error!("dml quarantine: {skipped} schema-mismatched batch(es) for {}/{} could NOT be parked — {lost} rows LOST", key.project_id, key.table_name);
    }
    true
}

/// The table's time column ("timestamp" unless the schema overrides it) —
/// the column whose range conjuncts are widened and watermark-clamped.
pub(crate) fn table_time_column(table_name: &str) -> &'static str {
    crate::schema_loader::get_schema(table_name).map_or("timestamp", |s| s.time_column_name())
}

/// One extracted `time_col CMP literal` conjunct.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TimeBound {
    pub value: ScalarValue,
    pub inclusive: bool,
}

/// A predicate split into at most one lower / one upper bound on the time
/// column plus everything else verbatim. Extracting only the first bound per
/// direction is always sound: the reconstruction conjoins residual + bounds,
/// so the predicate is preserved exactly — extra time conjuncts just live in
/// `residual` and block cross-statement grouping via the fingerprint.
#[derive(Debug, Clone, Default)]
pub(crate) struct DecomposedPredicate {
    pub residual: Vec<Expr>,
    pub lower: Option<TimeBound>,
    pub upper: Option<TimeBound>,
}

impl DecomposedPredicate {
    pub fn decompose(predicate: Option<&Expr>, time_col: &str) -> Self {
        let mut d = Self::default();
        let Some(pred) = predicate else { return d };
        for conjunct in split_conjunction(pred) {
            match classify_time_conjunct(conjunct, time_col) {
                Some((bound, true)) if d.lower.is_none() => d.lower = Some(bound),
                Some((bound, false)) if d.upper.is_none() => d.upper = Some(bound),
                _ => d.residual.push(conjunct.clone()),
            }
        }
        d
    }

    /// Conjoin residual + bounds back into a predicate (inverse of decompose).
    pub fn reconstruct(&self, time_col: &str) -> Option<Expr> {
        let time_expr = |b: &TimeBound, lower: bool| {
            let op = match (lower, b.inclusive) {
                (true, true) => Operator::GtEq,
                (true, false) => Operator::Gt,
                (false, true) => Operator::LtEq,
                (false, false) => Operator::Lt,
            };
            Expr::BinaryExpr(BinaryExpr { left: Box::new(datafusion::prelude::col(time_col)), op, right: Box::new(lit(b.value.clone())) })
        };
        self.residual
            .iter()
            .cloned()
            .chain(self.lower.as_ref().map(|b| time_expr(b, true)))
            .chain(self.upper.as_ref().map(|b| time_expr(b, false)))
            .reduce(|a, b| a.and(b))
    }

    /// Widen this predicate's time window to also cover `other`'s (union):
    /// lower takes the smaller bound, upper the larger; a missing bound on
    /// either side is unbounded and dominates.
    fn widen(&mut self, other: &Self) {
        self.lower = match (self.lower.take(), &other.lower) {
            (Some(a), Some(b)) => Some(widen_bound(a, b, true)),
            _ => None,
        };
        self.upper = match (self.upper.take(), &other.upper) {
            (Some(a), Some(b)) => Some(widen_bound(a, b, false)),
            _ => None,
        };
    }
}

/// Union-widen two bounds on the same side: for lowers keep the smaller
/// value, for uppers the larger; on equal values inclusive wins. Values that
/// don't compare (mixed types) widen to the safest available — the caller's
/// fingerprint makes this near-impossible, but never tighten on uncertainty.
fn widen_bound(a: TimeBound, b: &TimeBound, lower: bool) -> TimeBound {
    match a.value.partial_cmp(&b.value) {
        Some(std::cmp::Ordering::Equal) => TimeBound { inclusive: a.inclusive || b.inclusive, value: a.value },
        Some(ord) if (ord == std::cmp::Ordering::Less) == lower => a,
        Some(_) => b.clone(),
        None => TimeBound { inclusive: true, ..a },
    }
}

/// Drain-time merge window. The Delta MERGE hash-join builds against every
/// target row in the predicate's time window regardless of source chunking,
/// so join memory ∝ window width — 2026-07-30 union-window drains (~1h)
/// reserved 2.5+ GB per merge and exhausted the 10 GB pool, and k chunks paid
/// k full scans of the whole window. Bucketing statements by their own bounds
/// keeps each merge's target side ∝ bucket, not burst.
const DML_MERGE_BUCKET_MICROS: i64 = 5 * 60 * 1_000_000;

/// A statement's own time bounds, captured at enqueue (the group predicate is
/// widened to the union, so these are the only record of the narrow window).
type StmtBounds = (Option<TimeBound>, Option<TimeBound>);
type BoundBatch = (RecordBatch, StmtBounds);

/// Timestamp scalar → microseconds (inverse of [`watermark_scalar`]).
fn scalar_micros(v: &ScalarValue) -> Option<i64> {
    Some(match v {
        ScalarValue::TimestampSecond(Some(s), _) => s.checked_mul(1_000_000)?,
        ScalarValue::TimestampMillisecond(Some(ms), _) => ms.checked_mul(1_000)?,
        ScalarValue::TimestampMicrosecond(Some(us), _) => *us,
        ScalarValue::TimestampNanosecond(Some(ns), _) => ns.div_euclid(1_000),
        _ => return None,
    })
}

/// The `DML_MERGE_BUCKET_MICROS` bucket span a statement's window covers, or
/// None when a side is unbounded or non-timestamp (those statements share one
/// catch-all unit). Spanning statements keep their full window — the span key
/// just puts same-span statements together; it never narrows anything.
fn bounds_span(b: &StmtBounds) -> Option<(i64, i64)> {
    let lo = scalar_micros(&b.0.as_ref()?.value)?;
    let up = b.1.as_ref()?;
    // An exclusive upper on a bucket edge contains no row at the edge itself.
    let hi = scalar_micros(&up.value)? - i64::from(!up.inclusive);
    Some((lo.div_euclid(DML_MERGE_BUCKET_MICROS), hi.max(lo).div_euclid(DML_MERGE_BUCKET_MICROS)))
}

/// Split a drained group into per-time-bucket merge units: each unit keeps the
/// group's shape (residual, keys, assignments, attempts) but narrows the time
/// window to the union of only its own statements' bounds. Single-bucket
/// groups pass through untouched — exactly today's one-merge-unit behavior.
fn bucket_group(mut group: PendingGroup) -> Vec<PendingGroup> {
    let mut buckets: BTreeMap<Option<(i64, i64)>, Vec<BoundBatch>> = BTreeMap::new();
    for bb in std::mem::take(&mut group.batches) {
        buckets.entry(bounds_span(&bb.1)).or_default().push(bb);
    }
    if buckets.len() <= 1 {
        group.batches = buckets.into_values().next().unwrap_or_default();
        return vec![group];
    }
    buckets
        .into_values()
        .map(|batches| {
            let mut predicate = DecomposedPredicate { residual: group.predicate.residual.clone(), lower: batches[0].1.0.clone(), upper: batches[0].1.1.clone() };
            for (_, (lo, up)) in &batches[1..] {
                predicate.widen(&DecomposedPredicate { residual: vec![], lower: lo.clone(), upper: up.clone() });
            }
            PendingGroup {
                join_keys: group.join_keys.clone(),
                assignments: group.assignments.clone(),
                predicate,
                time_col: group.time_col,
                schema: group.schema.clone(),
                batches,
                session: group.session.clone(),
                attempts: group.attempts,
                folded_projects: group.folded_projects.clone(),
            }
        })
        .collect()
}

/// Classify a conjunct as `(bound, is_lower)` when it is
/// `time_col CMP literal` (either operand order); anything else is residual.
fn classify_time_conjunct(e: &Expr, time_col: &str) -> Option<(TimeBound, bool)> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = e else { return None };
    let (value, op) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(v, _)) if c.name == time_col => (v.clone(), *op),
        (Expr::Literal(v, _), Expr::Column(c)) if c.name == time_col => (v.clone(), op.swap()?),
        _ => return None,
    };
    match op {
        Operator::Gt => Some((TimeBound { value, inclusive: false }, true)),
        Operator::GtEq => Some((TimeBound { value, inclusive: true }, true)),
        Operator::Lt => Some((TimeBound { value, inclusive: false }, false)),
        Operator::LtEq => Some((TimeBound { value, inclusive: true }, false)),
        _ => None,
    }
}

/// Express `watermark_micros` in the same scalar type as `template` so bounds
/// stay comparable. None (no clamping) when the template isn't a timestamp or
/// the unit conversion overflows.
fn watermark_scalar(template: &ScalarValue, micros: i64) -> Option<ScalarValue> {
    Some(match template {
        ScalarValue::TimestampSecond(_, tz) => ScalarValue::TimestampSecond(Some(micros.div_euclid(1_000_000)), tz.clone()),
        ScalarValue::TimestampMillisecond(_, tz) => ScalarValue::TimestampMillisecond(Some(micros.div_euclid(1_000)), tz.clone()),
        ScalarValue::TimestampMicrosecond(_, tz) => ScalarValue::TimestampMicrosecond(Some(micros), tz.clone()),
        ScalarValue::TimestampNanosecond(_, tz) => ScalarValue::TimestampNanosecond(Some(micros.checked_mul(1_000)?), tz.clone()),
        _ => return None,
    })
}

/// Watermark clamp outcome for a DML Delta leg.
pub(crate) enum WatermarkClamp {
    /// Run the Delta leg with this (possibly tightened) predicate.
    Keep(Option<Expr>),
    /// The whole time window lies above the flush watermark: every matchable
    /// row is buffer-only, the flush persists its post-DML value, and the
    /// Delta leg would scan + commit for nothing. Skip it.
    SkipDelta,
}

/// Clamp `predicate`'s time window to rows that can exist in Delta: rows with
/// `time_col > watermark` were never handed to a Delta commit (the watermark
/// is raised before every commit and persisted with it), so the upper bound
/// tightens to the watermark — and when even the lower bound is above it, the
/// Delta leg skips entirely. Predicates without a literal time bound pass
/// through untouched (no type template to clamp against).
pub(crate) fn clamp_to_watermark(predicate: Option<&Expr>, time_col: &str, watermark_micros: i64) -> WatermarkClamp {
    let mut d = DecomposedPredicate::decompose(predicate, time_col);
    match clamp_decomposed(&mut d, watermark_micros) {
        ClampAction::Skip => WatermarkClamp::SkipDelta,
        ClampAction::Unchanged => WatermarkClamp::Keep(predicate.cloned()),
        ClampAction::Clamped => WatermarkClamp::Keep(d.reconstruct(time_col)),
    }
}

enum ClampAction {
    Unchanged,
    Clamped,
    Skip,
}

/// Shared clamp core over a decomposed predicate (used by both the
/// synchronous path above and the coalescer drain, which clamps the widened
/// window at drain time — the watermark only rises, so later is tighter).
fn clamp_decomposed(d: &mut DecomposedPredicate, watermark_micros: i64) -> ClampAction {
    let template = d.lower.as_ref().or(d.upper.as_ref()).map(|b| b.value.clone());
    let Some(wm) = template.and_then(|t| watermark_scalar(&t, watermark_micros)) else {
        return ClampAction::Unchanged;
    };
    if let Some(lo) = &d.lower {
        match lo.value.partial_cmp(&wm) {
            Some(std::cmp::Ordering::Greater) => return ClampAction::Skip,
            Some(std::cmp::Ordering::Equal) if !lo.inclusive => return ClampAction::Skip,
            _ => {}
        }
    }
    let tighter = match &d.upper {
        Some(up) => matches!(up.value.partial_cmp(&wm), Some(std::cmp::Ordering::Greater)),
        None => d.lower.is_some(), // only add a bound when a time window exists at all
    };
    if tighter {
        d.upper = Some(TimeBound { value: wm, inclusive: true });
        ClampAction::Clamped
    } else {
        ClampAction::Unchanged
    }
}

/// Split an [`UpdateSource`] into merge rounds so no single leg sees duplicate
/// join keys: the Delta MERGE rejects a source that matches a target row twice,
/// and the MemBuffer hash-join would silently keep only one match — dropping the
/// rest (prod 2026-07-19: same-key multi-tag hash enrichment lost tags / errored).
/// Returns one round for the common no-duplication case.
pub(crate) fn split_source_rounds(source: UpdateSource) -> Result<Vec<UpdateSource>> {
    let key_indices: Vec<usize> = source.join_keys.iter().map(|(_, s)| source.schema.index_of(s)).collect::<std::result::Result<_, _>>()?;
    Ok(split_rounds(&source.batch, &key_indices)?
        .into_iter()
        .map(|batch| UpdateSource { batch, schema: source.schema.clone(), join_keys: source.join_keys.clone() })
        .collect())
}

/// Split `batch` into merge rounds: exact-duplicate rows are dropped, and
/// rows sharing a join key land in successive rounds (round N = each key's
/// Nth distinct payload, in arrival order) so no single MERGE sees duplicate
/// source keys — Delta rejects a source that matches a target row twice.
fn split_rounds(batch: &RecordBatch, key_indices: &[usize]) -> Result<Vec<RecordBatch>> {
    let to_fields = |idxs: &[usize]| idxs.iter().map(|&i| SortField::new(batch.column(i).data_type().clone())).collect::<Vec<_>>();
    let key_cols: Vec<_> = key_indices.iter().map(|&i| batch.column(i).clone()).collect();
    let all_idx: Vec<usize> = (0..batch.num_columns()).collect();
    let key_rows = RowConverter::new(to_fields(key_indices))?.convert_columns(&key_cols)?;
    let full_rows = RowConverter::new(to_fields(&all_idx))?.convert_columns(batch.columns())?;

    // Bind each Row (a thin view) so the byte-slice keys can borrow from
    // them across the loop — no per-row heap copies.
    let full_row_views: Vec<_> = (0..batch.num_rows()).map(|i| full_rows.row(i)).collect();
    let key_row_views: Vec<_> = (0..batch.num_rows()).map(|i| key_rows.row(i)).collect();
    let mut seen_full: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
    let mut rounds: Vec<(std::collections::HashSet<&[u8]>, Vec<u32>)> = Vec::new();
    for i in 0..batch.num_rows() {
        if !seen_full.insert(full_row_views[i].as_ref()) {
            continue; // exact duplicate statement row — one application suffices
        }
        let key: &[u8] = key_row_views[i].as_ref();
        match rounds.iter_mut().find(|(keys, _)| !keys.contains(key)) {
            Some((keys, idxs)) => {
                keys.insert(key);
                idxs.push(i as u32);
            }
            None => rounds.push(([key].into_iter().collect(), vec![i as u32])),
        }
    }
    rounds
        .into_iter()
        .map(|(_, idxs)| {
            let idx = UInt32Array::from(idxs);
            let cols = batch.columns().iter().map(|c| take(c, &idx, None)).collect::<std::result::Result<Vec<_>, _>>()?;
            RecordBatch::try_new(batch.schema(), cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        })
        .collect()
}

/// Delta's DV MERGE rejects a source that matches one target row twice.
/// `split_rounds` should prevent it, yet prod groups still hit it (2026-07-28..30,
/// root cause open) — so on this error, bisect: halve the source and merge each
/// half. Single rows cannot multi-match, so recursion always terminates, the
/// data lands, and the log narrows down the offending key pair.
fn is_multi_source_match_err(msg: &str) -> bool {
    msg.contains("multiple source rows")
}

pub(crate) fn merge_bisect<'a>(
    db: &'a crate::database::Database, table_name: &'a str, project_id: &'a str, predicate: Option<Expr>, assignments: Vec<(String, Expr)>,
    source: UpdateSource, session: Arc<dyn Session>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64>> + Send + 'a>> {
    Box::pin(async move {
        let rows = source.batch.num_rows();
        match crate::dml::perform_delta_merge_update(db, table_name, project_id, predicate.clone(), assignments.clone(), source.clone(), session.clone()).await
        {
            Err(e) if is_multi_source_match_err(&e.to_string()) && rows > 1 => {
                let (a, b) = (source.batch.slice(0, rows / 2), source.batch.slice(rows / 2, rows - rows / 2));
                warn!("dml merge: multi-source match on {rows} rows for {project_id}/{table_name}; bisecting ({} + {} rows)", a.num_rows(), b.num_rows());
                let mut n = 0;
                for half in [a, b] {
                    let src = UpdateSource { batch: half, schema: source.schema.clone(), join_keys: source.join_keys.clone() };
                    n += merge_bisect(db, table_name, project_id, predicate.clone(), assignments.clone(), src, session.clone()).await?;
                }
                Ok(n)
            }
            r => r,
        }
    })
}

/// Re-drive parked `quarantine/dml/*` groups (hash-enrichment shape only; see
/// [`parse_quarantine_meta`]). Rebuilds the merge from the sidecar, replays it
/// through round-splitting + [`merge_bisect`], and moves recovered pairs into
/// `<dir>/redriven/`. Returns `(recovered, skipped)`.
pub async fn redrive_dml_quarantine(db: &Arc<crate::database::Database>, dir: &std::path::Path, dry_run: bool) -> (usize, usize) {
    use datafusion::logical_expr::{col, in_list};
    let Ok(rd) = std::fs::read_dir(dir) else {
        info!("dml redrive: no quarantine dir at {dir:?}");
        return (0, 0);
    };
    let ctx = db.clone().create_session_context();
    // Variant columns: the pgwire-facing session reads them as Utf8View, which
    // the DV-merge write leg cannot cast back to Struct{metadata,value}. Use
    // the same variant-safe session the interactive DML path hands to merges.
    let session: Arc<dyn Session> = crate::dml::delta_session_from(&ctx.state());
    let redriven = dir.join("redriven");
    let (mut ok, mut skipped) = (0usize, 0usize);
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "arrow") {
            continue;
        }
        let meta_path = path.with_extension("meta");
        let Some(meta) = std::fs::read_to_string(&meta_path).ok().as_deref().and_then(parse_quarantine_meta) else {
            warn!("dml redrive: {path:?} meta missing or not the reconstructible enrichment shape; leaving parked");
            skipped += 1;
            continue;
        };
        let batches: Result<Vec<RecordBatch>> = std::fs::File::open(&path)
            .map_err(|e| DataFusionError::External(Box::new(e)))
            .and_then(|f| datafusion::arrow::ipc::reader::FileReader::try_new(f, None).map_err(|e| DataFusionError::ArrowError(Box::new(e), None)))
            .and_then(|r| r.collect::<std::result::Result<Vec<_>, _>>().map_err(|e| DataFusionError::ArrowError(Box::new(e), None)));
        let merged = match batches.and_then(|b| {
            let schema = b.first().map(RecordBatch::schema).ok_or_else(|| DataFusionError::Execution("empty IPC file".into()))?;
            concat_batches(&schema, &b).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }) {
            Ok(m) => m,
            Err(e) => {
                warn!("dml redrive: cannot read {path:?}: {e}; leaving parked");
                skipped += 1;
                continue;
            }
        };
        // Predicate: same shape the group was parked with. Bare column names —
        // requalification in the merge routes them (project_id binds to the
        // source side, which the join equates with the target's anyway).
        //
        // WINDOW SLICING: the merge hash-join builds against every target row
        // in the predicate window — independent of source chunking — and a
        // burst group's full window exhausted a 24 GB pool (2026-07-30,
        // HashJoinInput 3.7 GB × several partitions). Each target row lies in
        // exactly one time slice, so merging the SAME source against each
        // slice is semantically identical with join memory ÷ slices.
        let tz: Arc<str> = Arc::from("UTC");
        let (ts_upper, upper_incl) = meta.ts_upper;
        let slices = (meta.rows / 150_000).clamp(1, 32) as i64;
        let span = (ts_upper - meta.ts_lower).max(slices);
        let base_conj = |lo: i64, hi: i64, hi_incl: bool| {
            let mut conj = vec![
                col("context___span_id").is_not_null(),
                col("context___trace_id").is_not_null(),
                col("date").gt_eq(lit(ScalarValue::Date32(Some(meta.date_bounds.0)))),
                col("date").lt_eq(lit(ScalarValue::Date32(Some(meta.date_bounds.1)))),
                col("timestamp").gt_eq(lit(ScalarValue::TimestampMicrosecond(Some(lo), Some(tz.clone())))),
                if hi_incl {
                    col("timestamp").lt_eq(lit(ScalarValue::TimestampMicrosecond(Some(hi), Some(tz.clone()))))
                } else {
                    col("timestamp").lt(lit(ScalarValue::TimestampMicrosecond(Some(hi), Some(tz.clone()))))
                },
            ];
            if meta.projects.len() > 1 {
                conj.push(in_list(col("project_id"), meta.projects.iter().map(lit).collect(), false));
            }
            conj.into_iter().reduce(Expr::and)
        };
        let slice_bounds: Vec<(i64, i64, bool)> = (0..slices)
            .map(|i| {
                let lo = meta.ts_lower + span * i / slices;
                let hi = if i + 1 == slices { ts_upper } else { meta.ts_lower + span * (i + 1) / slices };
                // interior slice upper bounds are exclusive; the last keeps the meta's inclusivity
                (lo, hi, if i + 1 == slices { upper_incl } else { false })
            })
            .collect();
        // Rebuilt equivalents of the two parked shapes (no empty-list literal
        // needed): scalar `tag` → append-or-singleton; list `new_hashes` →
        // take-or-concat.
        let assignment = if meta.list_source {
            datafusion::logical_expr::when(col("hashes").is_null(), col("new_hashes"))
                .otherwise(datafusion::functions_nested::expr_fn::array_concat(vec![col("hashes"), col("new_hashes")]))
                .expect("static CASE expr")
        } else {
            datafusion::logical_expr::when(col("hashes").is_not_null(), datafusion::functions_nested::expr_fn::array_append(col("hashes"), col("tag")))
                .otherwise(datafusion::functions_nested::expr_fn::make_array(vec![col("tag")]))
                .expect("static CASE expr")
        };
        info!(
            "dml redrive: {} rows for {} ({} projects), window {:?}: replaying{}",
            merged.num_rows(),
            meta.table_name,
            meta.projects.len(),
            meta.date_bounds,
            if dry_run { " [DRY RUN]" } else { "" }
        );
        if dry_run {
            ok += 1;
            continue;
        }
        let key_indices: Result<Vec<usize>> = meta.join_keys.iter().map(|(_, s)| Ok(merged.schema().index_of(s)?)).collect();
        let rounds = match key_indices.and_then(|idx| split_rounds(&merged, &idx)) {
            Ok(r) => r,
            Err(e) => {
                warn!("dml redrive: round split failed for {path:?}: {e}; leaving parked");
                skipped += 1;
                continue;
            }
        };
        let mut failed = false;
        'slices: for (si, &(lo, hi, hi_incl)) in slice_bounds.iter().enumerate() {
            let predicate = base_conj(lo, hi, hi_incl);
            if slice_bounds.len() > 1 {
                info!("dml redrive: {path:?} slice {}/{} [{lo}..{hi}]", si + 1, slice_bounds.len());
            }
            for round in rounds.iter().flat_map(|r| chunk_rows(r, MAX_MERGE_ROWS)) {
                let source = UpdateSource { batch: round, schema: merged.schema(), join_keys: meta.join_keys.clone() };
                if let Err(e) = merge_bisect(
                    db,
                    &meta.table_name,
                    &meta.project_id,
                    predicate.clone(),
                    vec![("hashes".into(), assignment.clone())],
                    source,
                    session.clone(),
                )
                .await
                {
                    error!(
                        "dml redrive: merge failed for {path:?} (slice {}/{}): {e}; leaving parked (applied slices re-append tags only on retried rows)",
                        si + 1,
                        slice_bounds.len()
                    );
                    failed = true;
                    break 'slices;
                }
            }
        }
        if failed {
            skipped += 1;
            continue;
        }
        ok += 1;
        if let Err(e) = std::fs::create_dir_all(&redriven)
            .and_then(|()| std::fs::rename(&path, redriven.join(entry.file_name())))
            .and_then(|()| std::fs::rename(&meta_path, redriven.join(meta_path.file_name().unwrap_or_default())))
        {
            warn!("dml redrive: recovered but could not move {path:?} to redriven/: {e}");
        }
    }
    info!("dml redrive: {ok} group(s) recovered, {skipped} left parked");
    (ok, skipped)
}

/// Parsed `quarantine/dml/*.meta` sidecar for the known hash-enrichment shape.
/// Only what the re-drive needs is machine-recovered; groups whose meta doesn't
/// match this shape stay parked (assignments/predicate are stored as Expr
/// Display strings, which are not generally re-parseable).
#[derive(Debug, PartialEq)]
pub(crate) struct QuarantineMeta {
    pub table_name: String,
    pub project_id: String,
    pub projects: Vec<String>,
    pub join_keys: Vec<(String, String)>,
    pub date_bounds: (i32, i32),
    /// (micros, inclusive_upper) — lower is always inclusive.
    pub ts_lower: i64,
    pub ts_upper: (i64, bool),
    pub rows: usize,
    /// Source carries a `new_hashes` list column (second enrichment shape)
    /// rather than a scalar `tag`.
    pub list_source: bool,
}

pub(crate) fn parse_quarantine_meta(meta: &str) -> Option<QuarantineMeta> {
    let field = |k: &str| meta.lines().find_map(|l| l.strip_prefix(k).and_then(|l| l.strip_prefix('=')).map(str::to_owned));
    let (table_name, project_id, predicate, assignments) = (field("table_name")?, field("project_id")?, field("predicate")?, field("assignments")?);
    // Shape guard: only the two hash-enrichment folds are reconstructible.
    let list_source = assignments.starts_with("hashes:=CASE WHEN o.hashes IS NULL THEN u.new_hashes");
    if !list_source && !assignments.starts_with("hashes:=array_concat(") {
        return None;
    }
    let join_keys: Vec<(String, String)> = field("join_keys")?
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|p| p.split_once('=').map(|(a, b)| (a.to_string(), b.to_string())))
        .collect::<Option<_>>()?;
    let projects: Vec<String> = {
        let fp = field("folded_projects").unwrap_or_default();
        if fp.is_empty() { vec![project_id.clone()] } else { fp.split(',').map(str::to_string).collect() }
    };
    // Date32("YYYY-MM-DD") bounds → day numbers since epoch.
    let dates: Vec<i32> = predicate
        .match_indices("Date32(\"")
        .filter_map(|(i, m)| {
            let s = &predicate[i + m.len()..];
            s.split('"').next()?.parse::<chrono::NaiveDate>().ok().map(|d| (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
        })
        .collect();
    let (lo, hi) = (dates.iter().min()?, dates.iter().max()?);
    // `timestamp >= TimestampMicrosecond(N` / `timestamp <[=] TimestampMicrosecond(N`.
    let ts_after = |pat: &str| {
        predicate.find(pat).and_then(|i| {
            let s = &predicate[i + pat.len()..];
            let s = s.split("TimestampMicrosecond(").nth(1)?;
            s.split(|c: char| !c.is_ascii_digit() && c != '-').next()?.parse::<i64>().ok()
        })
    };
    let ts_lower = ts_after("timestamp >= ")?;
    let (ts_upper, upper_incl) = match (ts_after("timestamp <= "), ts_after("timestamp < ")) {
        (Some(t), _) => (t, true),
        (None, Some(t)) => (t, false),
        (None, None) => return None,
    };
    Some(QuarantineMeta {
        table_name,
        project_id,
        projects,
        join_keys,
        date_bounds: (*lo, *hi),
        ts_lower,
        ts_upper: (ts_upper, upper_incl),
        rows: field("rows").and_then(|r| r.parse().ok()).unwrap_or(0),
        list_source,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    project_id: String,
    table_name: String,
    fingerprint: u64,
}

struct PendingGroup {
    join_keys: Vec<(String, String)>,
    assignments: Vec<(String, Expr)>,
    predicate: DecomposedPredicate,
    time_col: &'static str,
    schema: SchemaRef,
    /// Statement batches, each with its own time bounds so the drain can
    /// bucket by window ([`bucket_group`]).
    batches: Vec<BoundBatch>,
    /// Freshest enqueuing statement's session — keeps the drain's function
    /// registry identical to what the synchronous merge would have used.
    session: Arc<dyn Session>,
    attempts: u32,
    /// `Some(projects)` marks a drain-time cross-project fold (see
    /// `fold_groups`): batches carry an appended `project_id` column, the
    /// residual carries a `project_id IN (...)` filter, and the watermark
    /// clamp must use the MAX watermark across these projects (a row is
    /// provably buffer-only only when it is unflushed for EVERY member).
    /// `None` for ordinary single-project statement groups.
    folded_projects: Option<Vec<String>>,
}

/// Hash of everything that must match exactly for two statements to share
/// one MERGE: join keys, assignment exprs, residual predicate conjuncts
/// (order-insensitive), and the source schema.
fn shape_fingerprint(join_keys: &[(String, String)], assignments: &[(String, Expr)], residual: &[Expr], schema: &SchemaRef) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    join_keys.hash(&mut h);
    for (c, e) in assignments {
        (c, e.to_string()).hash(&mut h);
    }
    let mut res: Vec<String> = residual.iter().map(ToString::to_string).collect();
    res.sort_unstable();
    res.hash(&mut h);
    for f in schema.fields() {
        (f.name(), format!("{:?}", f.data_type())).hash(&mut h);
    }
    h.finish()
}

/// `project_id = '<id>'` equality conjunct, either operand order — via the
/// canonical matcher (`extract_project_id_from_expr`) so the routing, DML
/// extraction, and folding shapes can't drift apart. Any other shape stays in
/// the residual and blocks folding for that group.
fn is_project_eq(e: &Expr, project_id: &str) -> bool {
    crate::optimizers::extract_project_id_from_expr(e).as_deref() == Some(project_id)
}

/// Fold same-shape single-project groups on unified tables into one group per
/// (table, shape): `project_id` moves from a per-group residual equality into
/// a source column + join key + `IN (...)` partition filter, so one drain
/// issues ONE merge (one kernel metadata scan, one OCC commit) instead of one
/// per project. Data-scan bytes are unchanged — `project_id` is a partition
/// column, so the folded IN-list prunes to exactly the union of the files the
/// per-project merges would have read.
///
/// Groups are eligible only when they carry exactly one `project_id = <own>`
/// residual conjunct (anything else risks changing which rows the predicate
/// matches), their source schema has no `project_id` column yet, and the
/// project stores the table in the unified Delta table (custom-storage
/// projects resolve to physically separate tables — never fold those).
/// Ineligible groups and singleton buckets pass through untouched; any arrow
/// failure while folding a bucket falls back to its unfolded members.
fn fold_groups(groups: Vec<(GroupKey, PendingGroup)>, custom_storage: &std::collections::HashSet<(String, String)>) -> Vec<(GroupKey, PendingGroup)> {
    // A fold candidate: its enqueue key/group plus the residual with the
    // own-project equality stripped.
    type Member = (GroupKey, PendingGroup, Vec<Expr>);
    let mut out = Vec::with_capacity(groups.len());
    let mut buckets: HashMap<(String, u64), Vec<Member>> = HashMap::new();
    for (key, group) in groups {
        // The optimizer usually pushes `project_id = '<id>'` into the
        // TableScan (partition column), so most predicates carry no project
        // conjunct at all — scope rides in `key.project_id`. Strip an explicit
        // own-project equality when present; any OTHER reference to
        // project_id (IN, !=, expressions) is a shape we can't restate as the
        // folded IN-list, so it stays unfolded.
        let stripped: Vec<Expr> = group.predicate.residual.iter().filter(|e| !is_project_eq(e, &key.project_id)).cloned().collect();
        let eligible = group.folded_projects.is_none()
            && !stripped.iter().any(|e| e.column_refs().iter().any(|c| c.name == "project_id"))
            && group.schema.field_with_name("project_id").is_err()
            && !group.join_keys.iter().any(|(t, s)| t == "project_id" || s == "project_id")
            && !custom_storage.contains(&(key.project_id.clone(), key.table_name.clone()));
        if !eligible {
            out.push((key, group));
            continue;
        }
        let fp = shape_fingerprint(&group.join_keys, &group.assignments, &stripped, &group.schema);
        buckets.entry((key.table_name.clone(), fp)).or_default().push((key, group, stripped));
    }
    for ((table_name, shape_fp), mut members) in buckets {
        if members.len() == 1 {
            let (key, group, _) = members.pop().expect("len checked");
            out.push((key, group));
            continue;
        }
        // Deterministic member order → stable IN-list, fingerprint, and rep key.
        members.sort_by(|a, b| a.0.project_id.cmp(&b.0.project_id));
        let total_rows: usize = members.iter().flat_map(|(_, g, _)| &g.batches).map(|(b, _)| b.num_rows()).sum();
        if total_rows > MAX_QUEUED_SOURCE_ROWS {
            out.extend(members.into_iter().map(|(k, g, _)| (k, g)));
            continue;
        }
        match build_folded(&table_name, shape_fp, &members) {
            Ok(folded) => {
                debug!("dml coalesce: folded {} projects into one {} merge group", members.len(), table_name);
                out.push(folded);
            }
            Err(e) => {
                warn!("dml coalesce: folding {} groups for {} failed ({e}), draining per-project", members.len(), table_name);
                out.extend(members.into_iter().map(|(k, g, _)| (k, g)));
            }
        }
    }
    out
}

/// Assemble the folded group: append a constant `project_id` column to every
/// member batch, widen the union time window, and swap the per-project
/// equality residual for one `project_id IN (...)` filter. The folded
/// GroupKey's fingerprint hashes the shape AND the member set, so a failed
/// fold re-queued via `requeue` can only ever merge with a fold of the exact
/// same members — a different member set gets a different key (mixing them
/// would pin an older IN-list to newer members' rows and silently drop their
/// delta legs).
fn build_folded(table_name: &str, shape_fp: u64, members: &[(GroupKey, PendingGroup, Vec<Expr>)]) -> Result<(GroupKey, PendingGroup)> {
    use datafusion::arrow::{array::StringArray, datatypes::Field};
    let (rep_key, base, stripped) = &members[0];
    let projects: Vec<String> = members.iter().map(|(k, _, _)| k.project_id.clone()).collect();

    let schema: SchemaRef = Arc::new(datafusion::arrow::datatypes::Schema::new(
        base.schema
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(Field::new("project_id", datafusion::arrow::datatypes::DataType::Utf8, false))))
            .collect::<Vec<_>>(),
    ));
    let mut batches = Vec::new();
    for (key, group, _) in members {
        for (batch, bounds) in &group.batches {
            let project_col = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(key.project_id.as_str(), batch.num_rows())));
            let cols = batch.columns().iter().cloned().chain(std::iter::once(project_col as _)).collect();
            batches.push((RecordBatch::try_new(schema.clone(), cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?, bounds.clone()));
        }
    }

    let mut predicate = DecomposedPredicate {
        residual: stripped
            .iter()
            .cloned()
            .chain(std::iter::once(datafusion::prelude::col("project_id").in_list(projects.iter().map(|p| lit(p.as_str())).collect(), false)))
            .collect(),
        lower: base.predicate.lower.clone(),
        upper: base.predicate.upper.clone(),
    };
    for (_, group, _) in &members[1..] {
        predicate.widen(&group.predicate);
    }

    let mut h = std::hash::DefaultHasher::new();
    shape_fp.hash(&mut h);
    projects.hash(&mut h);
    let key = GroupKey { project_id: rep_key.project_id.clone(), table_name: table_name.to_string(), fingerprint: h.finish() };
    let group = PendingGroup {
        join_keys: base.join_keys.iter().cloned().chain(std::iter::once(("project_id".to_string(), "project_id".to_string()))).collect(),
        assignments: base.assignments.clone(),
        predicate,
        time_col: base.time_col,
        schema,
        batches,
        // Any member's session works: same shape ⇒ same function registry.
        session: base.session.clone(),
        attempts: members.iter().map(|(_, g, _)| g.attempts).max().unwrap_or(0),
        folded_projects: Some(projects),
    };
    Ok((key, group))
}

/// Accumulates deferred `UPDATE ... FROM` Delta legs and drains them as
/// batched merges. One instance per `Database`, created when
/// `TIMEFUSION_DML_COALESCE_SECS > 0`.
pub struct DmlCoalescer {
    interval_secs: u64,
    /// See `fold_groups` — cross-project folding of same-shape groups.
    fold: bool,
    groups: std::sync::Mutex<HashMap<GroupKey, PendingGroup>>,
    queued_rows: AtomicUsize,
    drain_notify: Notify,
    /// Serializes drains (timer vs shutdown vs test-triggered).
    drain_lock: tokio::sync::Mutex<()>,
    /// Where terminally-failed groups are parked instead of dropped.
    quarantine_dir: std::path::PathBuf,
}

impl std::fmt::Debug for DmlCoalescer {
    // Manual: PendingGroup holds an Arc<dyn Session>, which has no Debug.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmlCoalescer")
            .field("interval_secs", &self.interval_secs)
            .field("queued_rows", &self.queued_rows.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl DmlCoalescer {
    pub fn new(interval_secs: u64, fold: bool) -> Self {
        Self {
            interval_secs: interval_secs.max(1),
            fold,
            groups: std::sync::Mutex::new(HashMap::new()),
            queued_rows: AtomicUsize::new(0),
            drain_notify: Notify::new(),
            drain_lock: tokio::sync::Mutex::new(()),
            // try_config: unit tests construct a coalescer without init_config.
            quarantine_dir: crate::config::try_config()
                .map_or_else(|| std::path::PathBuf::from("./data/wal"), |c| c.core.wal_dir())
                .join(crate::wal::QUARANTINE_DIR_NAME)
                .join("dml"),
        }
    }

    /// Hold the drain serialization lock so a test can make any concurrent
    /// `drain()` block on lock acquisition (exercises shutdown's deadline).
    #[cfg(test)]
    pub(crate) async fn lock_drain_for_test(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.drain_lock.lock().await
    }

    /// Defer a statement's Delta merge. The caller has already applied the
    /// mem leg (and its WAL append) and verified committed data exists.
    pub fn enqueue(
        &self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)], source: &UpdateSource, session: Arc<dyn Session>,
    ) {
        let time_col = table_time_column(table_name);
        let decomposed = DecomposedPredicate::decompose(predicate, time_col);
        let key = GroupKey {
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            fingerprint: shape_fingerprint(&source.join_keys, assignments, &decomposed.residual, &source.schema),
        };
        let rows = source.batch.num_rows();
        let bounds = (decomposed.lower.clone(), decomposed.upper.clone());
        {
            let mut groups = self.groups.lock().expect("dml coalescer mutex poisoned");
            match groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut g) => {
                    let g = g.get_mut();
                    g.predicate.widen(&decomposed);
                    g.batches.push((source.batch.clone(), bounds));
                    g.session = session;
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(PendingGroup {
                        join_keys: source.join_keys.clone(),
                        assignments: assignments.to_vec(),
                        predicate: decomposed,
                        time_col,
                        schema: source.schema.clone(),
                        batches: vec![(source.batch.clone(), bounds)],
                        session,
                        attempts: 0,
                        folded_projects: None,
                    });
                }
            }
        }
        crate::metrics::record_dml_coalesce_enqueued();
        if self.queued_rows.fetch_add(rows, Ordering::Relaxed) + rows > MAX_QUEUED_SOURCE_ROWS {
            self.drain_notify.notify_one();
        }
    }

    /// Drain every pending group: clamp the widened window to the flush
    /// watermark, split into duplicate-key-free rounds, and run one merge per
    /// round. Failed groups are re-queued (merging with anything enqueued
    /// meanwhile) up to `MAX_DRAIN_ATTEMPTS`, then dropped loudly.
    pub async fn drain(&self, db: &crate::database::Database) {
        let _serial = self.drain_lock.lock().await;
        let groups: Vec<(GroupKey, PendingGroup)> = {
            let mut g = self.groups.lock().expect("dml coalescer mutex poisoned");
            self.queued_rows.store(0, Ordering::Relaxed);
            g.drain().collect()
        };
        let groups = if self.fold && groups.len() > 1 { fold_groups(groups, &db.custom_storage_keys().await) } else { groups };
        // Bucket after folding so each merge unit's target window stays ∝
        // DML_MERGE_BUCKET_MICROS, not the group's union window (see const).
        for (key, mut group) in groups.into_iter().flat_map(|(key, g)| bucket_group(g).into_iter().map(move |b| (key.clone(), b))) {
            if let Some(layer) = db.buffered_layer() {
                // Folded groups clamp against the MAX member watermark: a row
                // is excludable only when it is unflushed for every member.
                let wm = match &group.folded_projects {
                    Some(ps) => ps.iter().map(|p| layer.delta_flushed_watermark(p, &key.table_name)).max().unwrap_or(i64::MIN),
                    None => layer.delta_flushed_watermark(&key.project_id, &key.table_name),
                };
                match clamp_decomposed(&mut group.predicate, wm) {
                    ClampAction::Skip => {
                        crate::metrics::record_dml_delta_leg_skipped();
                        debug!("dml coalesce: skipping {}/{} group — window entirely above flush watermark", key.project_id, key.table_name);
                        continue;
                    }
                    ClampAction::Unchanged | ClampAction::Clamped => {}
                }
            }
            // A failure in any prep step (schema drift within a
            // fingerprint-matched group, missing join key, row conversion) is
            // a bug, not an operational state — but the rows are still
            // unapplied in Delta, so park them rather than drop them.
            let park_group = |stage: &str, e: &dyn std::fmt::Display, batches: &[RecordBatch]| {
                if !quarantine_group(&self.quarantine_dir, &key, &group, batches, &format!("{stage} failed: {e}")) {
                    crate::metrics::record_dml_coalesce_dropped();
                    error!("dml coalesce: {stage} failed for {}/{} — rows LOST: {e}", key.project_id, key.table_name);
                }
            };
            let statements = group.batches.len();
            let merged = match concat_batches(&group.schema, group.batches.iter().map(|(b, _)| b)) {
                Ok(b) => b,
                Err(e) => {
                    park_group("concat", &e, &group.batches.iter().map(|(b, _)| b.clone()).collect::<Vec<_>>());
                    continue;
                }
            };
            if merged.num_rows() == 0 {
                continue;
            }
            let key_indices: Result<Vec<usize>> = group.join_keys.iter().map(|(_, s)| Ok(merged.schema().index_of(s)?)).collect();
            let rounds = match key_indices.and_then(|idx| split_rounds(&merged, &idx)) {
                Ok(r) => r,
                Err(e) => {
                    park_group("round split", &e, std::slice::from_ref(&merged));
                    continue;
                }
            };
            let predicate = group.predicate.reconstruct(group.time_col);
            let mut failed = None;
            // Chunk each round to bound per-MERGE memory (see MAX_MERGE_ROWS).
            for round in rounds.iter().flat_map(|r| chunk_rows(r, MAX_MERGE_ROWS)) {
                let source = UpdateSource { batch: round, schema: group.schema.clone(), join_keys: group.join_keys.clone() };
                match merge_bisect(db, &key.table_name, &key.project_id, predicate.clone(), group.assignments.clone(), source, group.session.clone()).await {
                    Ok(rows) => {
                        crate::metrics::record_dml_coalesce_merge();
                        debug!("dml coalesce: merged {statements} stmts for {}/{} — {rows} rows updated", key.project_id, key.table_name);
                    }
                    Err(e) => {
                        failed = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = failed {
                group.attempts += 1;
                if group.attempts >= MAX_DRAIN_ATTEMPTS {
                    // Park, don't drop: the mem leg already applied and the
                    // Delta leg targets rows no longer in the buffer, so a
                    // dropped group is permanent divergence with no self-heal.
                    let reason = format!("{} failed drains: {e}", group.attempts);
                    if !quarantine_group(&self.quarantine_dir, &key, &group, std::slice::from_ref(&merged), &reason) {
                        crate::metrics::record_dml_coalesce_dropped();
                        error!(
                            "dml coalesce: LOST {}/{} group after {} failed drains ({statements} stmts, {} rows) — quarantine write failed: {e}",
                            key.project_id,
                            key.table_name,
                            group.attempts,
                            merged.num_rows()
                        );
                    }
                } else {
                    warn!(
                        "dml coalesce: drain failed for {}/{} (attempt {}/{MAX_DRAIN_ATTEMPTS}), re-queueing: {e}",
                        key.project_id, key.table_name, group.attempts
                    );
                    self.requeue(key, group);
                }
            }
        }
    }

    /// Put a failed group back, merging with statements enqueued during the
    /// drain. The failed batches are older, so they go in front to preserve
    /// per-key round order; the group's newer session wins.
    fn requeue(&self, key: GroupKey, group: PendingGroup) {
        let rows: usize = group.batches.iter().map(|(b, _)| b.num_rows()).sum();
        let mut groups = self.groups.lock().expect("dml coalescer mutex poisoned");
        match groups.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut g) => {
                let newer = g.get_mut();
                let mut batches = group.batches;
                batches.append(&mut newer.batches);
                newer.batches = batches;
                newer.predicate.widen(&group.predicate);
                newer.attempts = newer.attempts.max(group.attempts);
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(group);
            }
        }
        self.queued_rows.fetch_add(rows, Ordering::Relaxed);
    }

    /// Background drain loop: timer, queue-pressure notify, or shutdown (which
    /// runs one final drain so the stop-grace window flushes pending merges).
    pub async fn run(self: Arc<Self>, db: crate::database::Database, shutdown: CancellationToken) {
        info!("DML coalescer started (interval {}s)", self.interval_secs);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(self.interval_secs)) => {}
                _ = self.drain_notify.notified() => {}
                _ = shutdown.cancelled() => {
                    self.drain(&db).await;
                    info!("DML coalescer stopped");
                    return;
                }
            }
            self.drain(&db).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
        },
        prelude::col,
    };

    use super::*;

    fn ts(micros: i64) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into()))
    }

    fn window(lo: i64, hi: i64) -> Expr {
        col("timestamp").gt_eq(lit(ts(lo))).and(col("timestamp").lt(lit(ts(hi))))
    }

    /// Regression guard for the 2026-07-27 04:42Z loss: the terminal drain
    /// branch dropped 1_252_311 enrichment rows with only an `error!`. The
    /// rows must instead land on disk, self-describing and re-drivable.
    // #[serial]: asserts exact deltas on the process-global DML_STATS counters.
    #[test]
    #[serial_test::serial]
    fn terminal_failure_quarantines_rows_recoverably() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false), Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a", "b"])), Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
        let key = GroupKey { project_id: "proj/1".into(), table_name: "otel_logs_and_spans".into(), fingerprint: 7 };
        let group = PendingGroup {
            join_keys: vec![("id".into(), "id".into())],
            assignments: vec![("n".into(), lit(9i64))],
            predicate: DecomposedPredicate::decompose(Some(&window(100, 200)), "timestamp"),
            time_col: "timestamp",
            schema: schema.clone(),
            batches: vec![(batch.clone(), (None, None))],
            session: Arc::new(datafusion::prelude::SessionContext::new().state()),
            attempts: MAX_DRAIN_ATTEMPTS,
            folded_projects: Some(vec!["proj/1".into(), "proj2".into()]),
        };

        let before = crate::metrics::dml_stats().coalesce_quarantined.load(Ordering::Relaxed);
        assert!(quarantine_group(dir.path(), &key, &group, std::slice::from_ref(&batch), "resources exhausted"));
        assert_eq!(crate::metrics::dml_stats().coalesce_quarantined.load(Ordering::Relaxed), before + 1);

        let files: Vec<_> = std::fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().path()).collect();
        let arrow = files.iter().find(|p| p.extension().is_some_and(|e| e == "arrow")).expect("no .arrow payload");
        let meta = std::fs::read_to_string(files.iter().find(|p| p.extension().is_some_and(|e| e == "meta")).expect("no .meta")).unwrap();

        // Payload round-trips through Arrow IPC with rows intact — this is
        // what makes a re-drive possible at all.
        let f = std::fs::File::open(arrow).unwrap();
        let reader = datafusion::arrow::ipc::reader::FileReader::try_new(f, None).unwrap();
        let read: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        assert_eq!(read.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(read[0].schema().fields().len(), 2);

        // Sidecar carries the merge shape a re-drive needs.
        for want in ["project_id=proj/1", "rows=2", "join_keys=id=id", "reason=resources exhausted", "folded_projects=proj/1,proj2"] {
            assert!(meta.contains(want), "meta missing {want:?}:\n{meta}");
        }
        assert!(meta.contains("assignments=n:="), "meta missing assignments:\n{meta}");

        // Filename must not smuggle the '/' from project_id into a subdir.
        assert!(arrow.parent().unwrap() == dir.path(), "payload escaped the quarantine dir");
    }

    /// Two parks of the same project/table must never overwrite each other,
    /// even under a frozen clock (e2e virtual time) — an overwrite would be
    /// exactly the silent loss this whole mechanism exists to prevent. And a
    /// batch that cannot be parked must page via `coalesce_dropped`, not hide
    /// behind the recoverable-looking `coalesce_quarantined`.
    #[test]
    #[serial_test::serial]
    fn parks_are_collision_proof_and_partial_loss_pages() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let key = GroupKey { project_id: "p".into(), table_name: "t".into(), fingerprint: 1 };
        let mut group = PendingGroup {
            join_keys: vec![("n".into(), "n".into())],
            assignments: vec![("n".into(), lit(1i64))],
            predicate: DecomposedPredicate::decompose(None, "timestamp"),
            time_col: "timestamp",
            schema: schema.clone(),
            batches: vec![(batch.clone(), (None, None))],
            session: Arc::new(datafusion::prelude::SessionContext::new().state()),
            attempts: MAX_DRAIN_ATTEMPTS,
            folded_projects: None,
        };

        for _ in 0..3 {
            assert!(quarantine_group(dir.path(), &key, &group, std::slice::from_ref(&batch), "x"));
        }
        let payloads = std::fs::read_dir(dir.path()).unwrap().filter(|e| e.as_ref().unwrap().path().extension().is_some_and(|x| x == "arrow")).count();
        assert_eq!(payloads, 3, "parks overwrote each other instead of getting distinct names");

        // A batch whose schema differs from the group's cannot go in the IPC
        // file; that is real loss and must bump the paging metric.
        let other =
            RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)])), vec![Arc::new(StringArray::from(vec!["z"]))]).unwrap();
        group.batches = vec![(batch.clone(), (None, None)), (other.clone(), (None, None))];
        let dropped_before = crate::metrics::dml_stats().coalesce_quarantined.load(Ordering::Relaxed);
        assert!(quarantine_group(dir.path(), &key, &group, &[batch.clone(), other], "mixed"));
        assert_eq!(crate::metrics::dml_stats().coalesce_quarantined.load(Ordering::Relaxed), dropped_before + 1);
        let meta = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "meta"))
            .map(|p| std::fs::read_to_string(p).unwrap())
            .find(|m| m.contains("reason=mixed"))
            .expect("no meta for the mixed-schema park");
        // Only the matching batch's row is accounted as parked.
        assert!(meta.contains("rows=1"), "meta over-claims parked rows:\n{meta}");
    }

    /// A round larger than `MAX_MERGE_ROWS` must be fed to Delta as several
    /// bounded slices covering every row exactly once — one unbounded MERGE is
    /// what exhausted memory on 2026-07-27.
    #[test]
    fn oversized_round_chunks_to_bounded_merges() {
        let rows = MAX_MERGE_ROWS * 2 + 7;
        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
        let round = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from_iter_values(0..rows as i64))]).unwrap();

        let chunks: Vec<RecordBatch> = chunk_rows(&round, MAX_MERGE_ROWS).collect();

        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|c| c.num_rows() <= MAX_MERGE_ROWS));
        assert_eq!(chunks.iter().map(RecordBatch::num_rows).sum::<usize>(), rows, "chunking must not drop or duplicate rows");
    }

    #[test]
    fn decompose_extracts_bounds_and_residual() {
        let pred = col("project_id").eq(lit("p1")).and(window(100, 200));
        let d = DecomposedPredicate::decompose(Some(&pred), "timestamp");
        assert_eq!(d.lower, Some(TimeBound { value: ts(100), inclusive: true }));
        assert_eq!(d.upper, Some(TimeBound { value: ts(200), inclusive: false }));
        assert_eq!(d.residual.len(), 1);
        // Round-trip preserves the conjunction (order may differ).
        let rebuilt = d.reconstruct("timestamp").unwrap();
        let parts: Vec<String> = split_conjunction(&rebuilt).iter().map(ToString::to_string).collect();
        assert_eq!(parts.len(), 3);
        assert!(parts.iter().any(|p| p.contains("project_id")));
    }

    #[test]
    fn widen_takes_union_window() {
        let mut a = DecomposedPredicate::decompose(Some(&window(100, 200)), "timestamp");
        let b = DecomposedPredicate::decompose(Some(&window(50, 150)), "timestamp");
        a.widen(&b);
        assert_eq!(a.lower.unwrap().value, ts(50));
        assert_eq!(a.upper.unwrap().value, ts(200));

        // A statement without an upper bound widens the union to unbounded.
        let mut a = DecomposedPredicate::decompose(Some(&window(100, 200)), "timestamp");
        let unbounded = DecomposedPredicate::decompose(Some(&col("timestamp").gt_eq(lit(ts(10)))), "timestamp");
        a.widen(&unbounded);
        assert_eq!(a.lower.unwrap().value, ts(10));
        assert!(a.upper.is_none());
    }

    #[test]
    fn clamp_skips_fully_unflushed_window() {
        let pred = window(1_000, 2_000);
        assert!(matches!(clamp_to_watermark(Some(&pred), "timestamp", 500), WatermarkClamp::SkipDelta));
        // Exclusive lower exactly at the watermark also skips.
        let pred = col("timestamp").gt(lit(ts(500)));
        assert!(matches!(clamp_to_watermark(Some(&pred), "timestamp", 500), WatermarkClamp::SkipDelta));
        // Inclusive lower at the watermark must keep (row at wm may be flushed).
        let pred = col("timestamp").gt_eq(lit(ts(500)));
        assert!(matches!(clamp_to_watermark(Some(&pred), "timestamp", 500), WatermarkClamp::Keep(_)));
    }

    #[test]
    fn clamp_tightens_upper_bound_to_watermark() {
        let pred = window(100, 2_000);
        match clamp_to_watermark(Some(&pred), "timestamp", 500) {
            WatermarkClamp::Keep(Some(p)) => {
                let d = DecomposedPredicate::decompose(Some(&p), "timestamp");
                assert_eq!(d.upper, Some(TimeBound { value: ts(500), inclusive: true }));
                assert_eq!(d.lower.unwrap().value, ts(100));
            }
            _ => panic!("expected clamped predicate"),
        }
        // Window already below the watermark: untouched.
        let pred = window(100, 300);
        match clamp_to_watermark(Some(&pred), "timestamp", 500) {
            WatermarkClamp::Keep(Some(p)) => assert_eq!(p, pred),
            _ => panic!("expected unchanged predicate"),
        }
        // No time bounds at all: nothing to clamp against.
        let pred = col("project_id").eq(lit("p1"));
        match clamp_to_watermark(Some(&pred), "timestamp", 500) {
            WatermarkClamp::Keep(Some(p)) => assert_eq!(p, pred),
            _ => panic!("expected unchanged predicate"),
        }
    }

    #[test]
    fn parse_quarantine_meta_recovers_enrichment_shape() {
        // Verbatim shape of the 2026-07-28..30 parked groups.
        let meta = "project_id=00000000-0000-0000-0000-000000000000\n\
            table_name=otel_logs_and_spans\n\
            folded_projects=00000000-0000-0000-0000-000000000000,28f62f01-46a1-400e-8195-da7bc3505b5b\n\
            join_keys=context___span_id=span_id,context___trace_id=trace_id,project_id=project_id\n\
            assignments=hashes:=array_concat(CASE WHEN o.hashes IS NOT NULL THEN o.hashes ELSE List([]) END, make_array(u.tag))\n\
            predicate=otel_logs_and_spans.context___span_id IS NOT NULL AND date >= Date32(\"2026-07-30\") AND date <= Date32(\"2026-07-30\") AND project_id IN ([]) AND timestamp >= TimestampMicrosecond(1785408669559194, Some(\"UTC\")) AND timestamp < TimestampMicrosecond(1785410181576000, Some(\"UTC\"))\n\
            time_col=timestamp\nattempts=3\nrows=766\nstatements=20\nreason=3 failed drains: x\n";
        let m = parse_quarantine_meta(meta).expect("parses");
        assert_eq!(m.projects.len(), 2);
        assert_eq!(m.join_keys.len(), 3);
        assert_eq!(m.date_bounds, (20664, 20664)); // 2026-07-30
        assert_eq!(m.ts_lower, 1785408669559194);
        assert_eq!(m.ts_upper, (1785410181576000, false));
        assert_eq!(m.rows, 766);
        // The list-source shape parses too; unknown shapes stay parked.
        let list = meta.replace(
            "hashes:=array_concat(CASE WHEN o.hashes IS NOT NULL THEN o.hashes ELSE List([]) END, make_array(u.tag))",
            "hashes:=CASE WHEN o.hashes IS NULL THEN u.new_hashes ELSE array_concat(o.hashes, u.new_hashes) END",
        );
        assert!(parse_quarantine_meta(&list).is_some_and(|m| m.list_source));
        assert!(parse_quarantine_meta(&meta.replace("hashes:=array_concat(", "other:=fn(")).is_none());
    }

    #[test]
    fn split_rounds_separates_duplicate_keys_and_drops_exact_dups() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("span_id", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("n", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "a", "b"])),
                Arc::new(StringArray::from(vec!["t1", "t1", "t2", "t1", "t1"])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 2])),
            ],
        )
        .unwrap();
        // Key = span_id. Rows: a/t1/1, b/t1/2, a/t2/3, a/t1/4, b/t1/2(exact dup).
        let rounds = split_rounds(&batch, &[0]).unwrap();
        assert_eq!(rounds.len(), 3, "key 'a' has 3 distinct payloads");
        assert_eq!(rounds[0].num_rows(), 2); // a/t1/1, b/t1/2
        assert_eq!(rounds[1].num_rows(), 1); // a/t2/3
        assert_eq!(rounds[2].num_rows(), 1); // a/t1/4
        let total: usize = rounds.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "exact duplicate dropped");
    }

    #[test]
    fn is_project_eq_matches_only_exact_literal_equality() {
        assert!(is_project_eq(&col("project_id").eq(lit("p1")), "p1"));
        assert!(is_project_eq(&lit("p1").eq(col("project_id")), "p1")); // swapped operands
        assert!(!is_project_eq(&col("project_id").eq(lit("p2")), "p1")); // other project
        assert!(!is_project_eq(&col("project_id").not_eq(lit("p1")), "p1")); // wrong op
        assert!(!is_project_eq(&col("other_col").eq(lit("p1")), "p1")); // wrong column
    }

    /// Pin the folded group's whole shape: appended project column + join key,
    /// IN-list residual, union window, and a member-set-sensitive fingerprint
    /// (a re-queued fold must never merge with a fold of different members —
    /// its IN-list would drop the extra members' delta legs).
    #[test]
    fn build_folded_appends_project_column_and_unions_windows() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("span_id", DataType::Utf8, false), Field::new("tag", DataType::Utf8, false)]));
        let batch = |ids: &[&str]| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(ids.to_vec())), Arc::new(StringArray::from(vec!["t"; ids.len()]))]).unwrap()
        };
        let member = |project: &str, lo: i64, hi: i64, ids: &[&str]| {
            let pred = col("project_id").eq(lit(project)).and(window(lo, hi));
            let d = DecomposedPredicate::decompose(Some(&pred), "timestamp");
            let stripped: Vec<Expr> = d.residual.iter().filter(|e| !is_project_eq(e, project)).cloned().collect();
            assert!(stripped.is_empty());
            let bounds = (d.lower.clone(), d.upper.clone());
            let group = PendingGroup {
                join_keys: vec![("context___span_id".into(), "span_id".into())],
                assignments: vec![("hashes".into(), col("source.tag"))],
                predicate: d,
                time_col: "timestamp",
                schema: schema.clone(),
                batches: vec![(batch(ids), bounds)],
                session: Arc::new(datafusion::execution::SessionStateBuilder::new().build()),
                attempts: 0,
                folded_projects: None,
            };
            (GroupKey { project_id: project.into(), table_name: "otel_logs_and_spans".into(), fingerprint: 1 }, group, stripped)
        };
        let members = vec![member("p1", 100, 200, &["a", "b"]), member("p2", 50, 150, &["c"])];
        let (key, folded) = build_folded("otel_logs_and_spans", 42, &members).unwrap();

        // Schema + batches: project_id appended as a constant column per member.
        assert_eq!(folded.schema.fields().last().unwrap().name(), "project_id");
        assert_eq!(folded.batches.len(), 2);
        let projects_of = |b: &RecordBatch| b.column(2).as_any().downcast_ref::<StringArray>().unwrap().value(0).to_string();
        assert_eq!(projects_of(&folded.batches[0].0), "p1");
        assert_eq!(projects_of(&folded.batches[1].0), "p2");
        // Join keys extended; window is the union; residual is the IN-list.
        assert_eq!(folded.join_keys.last().unwrap(), &("project_id".to_string(), "project_id".to_string()));
        assert_eq!(folded.predicate.lower.as_ref().unwrap().value, ts(50));
        assert_eq!(folded.predicate.upper.as_ref().unwrap().value, ts(200));
        assert_eq!(folded.predicate.residual.len(), 1);
        assert!(folded.predicate.residual[0].to_string().contains("IN"));
        assert_eq!(folded.folded_projects.as_deref(), Some(&["p1".to_string(), "p2".to_string()][..]));

        // Member-set-sensitive key: same shape, different members → different fingerprint.
        let (key2, _) = build_folded("otel_logs_and_spans", 42, &members[..1]).unwrap();
        assert_ne!(key.fingerprint, key2.fingerprint);

        // Folded batches concat + round-split cleanly under the widened keys.
        let merged = concat_batches(&folded.schema, folded.batches.iter().map(|(b, _)| b)).unwrap();
        assert_eq!(merged.num_rows(), 3);
        let src = UpdateSource { batch: merged, schema: folded.schema.clone(), join_keys: folded.join_keys.clone() };
        assert_eq!(split_source_rounds(src).unwrap().len(), 1, "distinct (span, project) keys must stay in one round");
    }

    /// Build a group whose statements each carry their own window, the way
    /// `enqueue` records them (group predicate widened, per-batch bounds kept).
    fn group_with_windows(windows: &[(i64, Option<i64>)]) -> PendingGroup {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();
        let decomposed: Vec<DecomposedPredicate> = windows
            .iter()
            .map(|&(lo, hi)| {
                let pred = match hi {
                    Some(hi) => col("service").eq(lit("s1")).and(window(lo, hi)),
                    None => col("service").eq(lit("s1")).and(col("timestamp").gt_eq(lit(ts(lo)))),
                };
                DecomposedPredicate::decompose(Some(&pred), "timestamp")
            })
            .collect();
        let mut predicate = decomposed[0].clone();
        decomposed[1..].iter().for_each(|d| predicate.widen(d));
        PendingGroup {
            join_keys: vec![("id".into(), "id".into())],
            assignments: vec![("n".into(), lit(9i64))],
            predicate,
            time_col: "timestamp",
            schema,
            batches: decomposed.into_iter().map(|d| (batch.clone(), (d.lower, d.upper))).collect(),
            session: Arc::new(datafusion::prelude::SessionContext::new().state()),
            attempts: 2,
            folded_projects: None,
        }
    }

    const B: i64 = DML_MERGE_BUCKET_MICROS;

    /// Statements in distinct 5-min buckets become separate merge units whose
    /// time bounds cover only their own statements — not the group union.
    #[test]
    fn bucket_group_splits_distinct_buckets_with_narrowed_bounds() {
        let units = bucket_group(group_with_windows(&[(0, Some(60)), (10, Some(90)), (2 * B, Some(2 * B + 60))]));
        assert_eq!(units.len(), 2);
        assert_eq!(units[0].batches.len(), 2);
        assert_eq!(units[0].predicate.lower.as_ref().unwrap().value, ts(0));
        assert_eq!(units[0].predicate.upper.as_ref().unwrap().value, ts(90));
        assert_eq!(units[1].batches.len(), 1);
        assert_eq!(units[1].predicate.lower.as_ref().unwrap().value, ts(2 * B));
        assert_eq!(units[1].predicate.upper.as_ref().unwrap().value, ts(2 * B + 60));
        // Everything but the time window is carried through unchanged.
        for u in &units {
            assert_eq!(u.predicate.residual.len(), 1);
            assert!(u.predicate.residual[0].to_string().contains("service"));
            assert_eq!(u.attempts, 2);
            assert_eq!(u.join_keys, vec![("id".to_string(), "id".to_string())]);
        }
    }

    /// A statement spanning a bucket boundary keeps its FULL window (own unit),
    /// never narrowed to either bucket.
    #[test]
    fn bucket_group_never_narrows_spanning_statement() {
        let units = bucket_group(group_with_windows(&[(B - 10, Some(B + 10)), (0, Some(50))]));
        assert_eq!(units.len(), 2);
        let spanning = units.iter().find(|u| u.batches.len() == 1 && u.predicate.upper.as_ref().unwrap().value == ts(B + 10)).expect("spanning unit");
        assert_eq!(spanning.predicate.lower.as_ref().unwrap().value, ts(B - 10));
    }

    /// A group whose statements all share one bucket drains exactly as today:
    /// one merge unit with the union window.
    #[test]
    fn bucket_group_single_bucket_is_one_unit() {
        let units = bucket_group(group_with_windows(&[(0, Some(50)), (60, Some(100))]));
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].batches.len(), 2);
        assert_eq!(units[0].predicate.lower.as_ref().unwrap().value, ts(0));
        assert_eq!(units[0].predicate.upper.as_ref().unwrap().value, ts(100));
    }

    /// Unbounded statements can't be bucketed — they share a catch-all unit
    /// whose window stays unbounded; bounded statements still bucket narrowly.
    #[test]
    fn bucket_group_unbounded_statements_share_catchall() {
        let units = bucket_group(group_with_windows(&[(0, Some(50)), (10, None)]));
        assert_eq!(units.len(), 2);
        let unbounded = units.iter().find(|u| u.predicate.upper.is_none()).expect("catch-all unit");
        assert_eq!(unbounded.predicate.lower.as_ref().unwrap().value, ts(10));
        let bounded = units.iter().find(|u| u.predicate.upper.is_some()).unwrap();
        assert_eq!(bounded.predicate.upper.as_ref().unwrap().value, ts(50));
        // No statement is dropped or duplicated by bucketing.
        assert_eq!(units.iter().map(|u| u.batches.len()).sum::<usize>(), 2);
    }

    /// An exclusive upper exactly on a bucket edge still belongs to the bucket
    /// below the edge (the window contains no row at the boundary).
    #[test]
    fn bucket_group_exclusive_upper_on_edge_stays_below() {
        let units = bucket_group(group_with_windows(&[(0, Some(B)), (10, Some(20))]));
        assert_eq!(units.len(), 1);
    }

    #[test]
    fn bucket_group_empty_group_passes_through() {
        let mut g = group_with_windows(&[(0, Some(50))]);
        g.batches.clear();
        assert_eq!(bucket_group(g).into_iter().map(|u| u.batches.len()).sum::<usize>(), 0);
    }

    #[test]
    fn fingerprint_groups_same_shape_only() {
        let jk = vec![("context___span_id".to_string(), "span_id".to_string())];
        let assign = vec![("hashes".to_string(), col("source.tag"))];
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("span_id", DataType::Utf8, false)]));
        let d1 = DecomposedPredicate::decompose(Some(&col("project_id").eq(lit("p1")).and(window(1, 2))), "timestamp");
        let d2 = DecomposedPredicate::decompose(Some(&window(5, 9).and(col("project_id").eq(lit("p1")))), "timestamp");
        // Same residual (order-insensitive), different windows → same group.
        assert_eq!(shape_fingerprint(&jk, &assign, &d1.residual, &schema), shape_fingerprint(&jk, &assign, &d2.residual, &schema));
        // Different residual constant → different group.
        let d3 = DecomposedPredicate::decompose(Some(&col("project_id").eq(lit("p2")).and(window(1, 2))), "timestamp");
        assert_ne!(shape_fingerprint(&jk, &assign, &d1.residual, &schema), shape_fingerprint(&jk, &assign, &d3.residual, &schema));
    }
}
