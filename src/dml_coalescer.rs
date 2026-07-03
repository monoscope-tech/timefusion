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
//! Delta when the statement ran — bounded by the drain interval and surfaced
//! by `timefusion.dml.coalesce_dropped` when drains exhaust retries.

use std::{
    collections::HashMap,
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

/// Drain attempts per group before it is dropped (each drain already carries
/// perform_delta_merge_update's 4-attempt OCC retry underneath).
const MAX_DRAIN_ATTEMPTS: u32 = 3;

/// The table's time column ("timestamp" unless the schema overrides it) —
/// the column whose range conjuncts are widened and watermark-clamped.
pub(crate) fn table_time_column(table_name: &str) -> &'static str {
    crate::schema_loader::get_schema(table_name).map_or("timestamp", |s| s.time_column_name())
}

/// One extracted `time_col CMP literal` conjunct.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TimeBound {
    pub value:     ScalarValue,
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
    pub lower:    Option<TimeBound>,
    pub upper:    Option<TimeBound>,
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
            Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(datafusion::prelude::col(time_col)),
                op,
                right: Box::new(lit(b.value.clone())),
            })
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
        Some(std::cmp::Ordering::Equal) => TimeBound {
            inclusive: a.inclusive || b.inclusive,
            value:     a.value,
        },
        Some(ord) if (ord == std::cmp::Ordering::Less) == lower => a,
        Some(_) => b.clone(),
        None => TimeBound { inclusive: true, ..a },
    }
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

/// Split `batch` into merge rounds: exact-duplicate rows are dropped, and
/// rows sharing a join key land in successive rounds (round N = each key's
/// Nth distinct payload, in arrival order) so no single MERGE sees duplicate
/// source keys — Delta rejects a source that matches a target row twice.
fn split_rounds(batch: &RecordBatch, key_indices: &[usize]) -> Result<Vec<RecordBatch>> {
    let to_fields = |idxs: &[usize]| {
        idxs.iter()
            .map(|&i| SortField::new(batch.column(i).data_type().clone()))
            .collect::<Vec<_>>()
    };
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    project_id:  String,
    table_name:  String,
    fingerprint: u64,
}

struct PendingGroup {
    join_keys:   Vec<(String, String)>,
    assignments: Vec<(String, Expr)>,
    predicate:   DecomposedPredicate,
    time_col:    &'static str,
    schema:      SchemaRef,
    batches:     Vec<RecordBatch>,
    /// Freshest enqueuing statement's session — keeps the drain's function
    /// registry identical to what the synchronous merge would have used.
    session:     Arc<dyn Session>,
    attempts:    u32,
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

/// Accumulates deferred `UPDATE ... FROM` Delta legs and drains them as
/// batched merges. One instance per `Database`, created when
/// `TIMEFUSION_DML_COALESCE_SECS > 0`.
pub struct DmlCoalescer {
    interval_secs: u64,
    groups:        std::sync::Mutex<HashMap<GroupKey, PendingGroup>>,
    queued_rows:   AtomicUsize,
    drain_notify:  Notify,
    /// Serializes drains (timer vs shutdown vs test-triggered).
    drain_lock:    tokio::sync::Mutex<()>,
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
    pub fn new(interval_secs: u64) -> Self {
        Self {
            interval_secs: interval_secs.max(1),
            groups: std::sync::Mutex::new(HashMap::new()),
            queued_rows: AtomicUsize::new(0),
            drain_notify: Notify::new(),
            drain_lock: tokio::sync::Mutex::new(()),
        }
    }

    /// Defer a statement's Delta merge. The caller has already applied the
    /// mem leg (and its WAL append) and verified committed data exists.
    pub fn enqueue(
        &self, project_id: &str, table_name: &str, predicate: Option<&Expr>, assignments: &[(String, Expr)], source: &UpdateSource,
        session: Arc<dyn Session>,
    ) {
        let time_col = table_time_column(table_name);
        let decomposed = DecomposedPredicate::decompose(predicate, time_col);
        let key = GroupKey {
            project_id:  project_id.to_string(),
            table_name:  table_name.to_string(),
            fingerprint: shape_fingerprint(&source.join_keys, assignments, &decomposed.residual, &source.schema),
        };
        let rows = source.batch.num_rows();
        {
            let mut groups = self.groups.lock().expect("dml coalescer mutex poisoned");
            match groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut g) => {
                    let g = g.get_mut();
                    g.predicate.widen(&decomposed);
                    g.batches.push(source.batch.clone());
                    g.session = session;
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(PendingGroup {
                        join_keys: source.join_keys.clone(),
                        assignments: assignments.to_vec(),
                        predicate: decomposed,
                        time_col,
                        schema: source.schema.clone(),
                        batches: vec![source.batch.clone()],
                        session,
                        attempts: 0,
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
        for (key, mut group) in groups {
            if let Some(layer) = db.buffered_layer() {
                let wm = layer.delta_flushed_watermark(&key.project_id, &key.table_name);
                match clamp_decomposed(&mut group.predicate, wm) {
                    ClampAction::Skip => {
                        crate::metrics::record_dml_delta_leg_skipped();
                        debug!(
                            "dml coalesce: skipping {}/{} group — window entirely above flush watermark",
                            key.project_id, key.table_name
                        );
                        continue;
                    }
                    ClampAction::Unchanged | ClampAction::Clamped => {}
                }
            }
            // A failure in any prep step (schema drift within a
            // fingerprint-matched group, missing join key, row conversion) is
            // a bug, not an operational state — drop loudly and move on.
            let drop_group = |stage: &str, e: &dyn std::fmt::Display| {
                crate::metrics::record_dml_coalesce_dropped();
                error!("dml coalesce: {stage} failed for {}/{}: {e}", key.project_id, key.table_name);
            };
            let statements = group.batches.len();
            let merged = match concat_batches(&group.schema, &group.batches) {
                Ok(b) => b,
                Err(e) => {
                    drop_group("concat", &e);
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
                    drop_group("round split", &e);
                    continue;
                }
            };
            let predicate = group.predicate.reconstruct(group.time_col);
            let mut failed = None;
            for round in rounds {
                let source = UpdateSource {
                    batch:     round,
                    schema:    group.schema.clone(),
                    join_keys: group.join_keys.clone(),
                };
                match crate::dml::perform_delta_merge_update(
                    db,
                    &key.table_name,
                    &key.project_id,
                    predicate.clone(),
                    group.assignments.clone(),
                    source,
                    group.session.clone(),
                )
                .await
                {
                    Ok(rows) => {
                        crate::metrics::record_dml_coalesce_merge();
                        debug!(
                            "dml coalesce: merged {statements} stmts for {}/{} — {rows} rows updated",
                            key.project_id, key.table_name
                        );
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
                    crate::metrics::record_dml_coalesce_dropped();
                    error!(
                        "dml coalesce: DROPPING {}/{} group after {} failed drains ({statements} stmts, {} rows): {e}",
                        key.project_id,
                        key.table_name,
                        group.attempts,
                        merged.num_rows()
                    );
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
        let rows: usize = group.batches.iter().map(RecordBatch::num_rows).sum();
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
    fn fingerprint_groups_same_shape_only() {
        let jk = vec![("context___span_id".to_string(), "span_id".to_string())];
        let assign = vec![("hashes".to_string(), col("source.tag"))];
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new("span_id", DataType::Utf8, false)]));
        let d1 = DecomposedPredicate::decompose(Some(&col("project_id").eq(lit("p1")).and(window(1, 2))), "timestamp");
        let d2 = DecomposedPredicate::decompose(Some(&window(5, 9).and(col("project_id").eq(lit("p1")))), "timestamp");
        // Same residual (order-insensitive), different windows → same group.
        assert_eq!(
            shape_fingerprint(&jk, &assign, &d1.residual, &schema),
            shape_fingerprint(&jk, &assign, &d2.residual, &schema)
        );
        // Different residual constant → different group.
        let d3 = DecomposedPredicate::decompose(Some(&col("project_id").eq(lit("p2")).and(window(1, 2))), "timestamp");
        assert_ne!(
            shape_fingerprint(&jk, &assign, &d1.residual, &schema),
            shape_fingerprint(&jk, &assign, &d3.residual, &schema)
        );
    }
}
