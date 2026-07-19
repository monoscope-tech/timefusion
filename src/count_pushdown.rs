//! COUNT(*) pushdown from Delta add-action statistics.
//!
//! `SELECT COUNT(*) FROM t WHERE project_id = 'x' AND timestamp >= lo AND
//! timestamp < hi` is the highest-frequency dashboard tile shape. When every
//! gate below holds, the answer is `Σ stats.numRecords` over the project's
//! files that lie FULLY inside the window — zero parquet IO. Any doubt →
//! `Ok(None)` and the normal scan runs; this module may only ever *decline*,
//! never approximate.
//!
//! Gates (all required):
//! - plan is exactly `[Projection] ← Aggregate[count, no groups] ←
//!   [Projection/SubqueryAlias]* ← [Filter] ← TableScan(ProjectRoutingTable)`
//!   (row count is invariant under projection/alias; anything else — Limit,
//!   Join, Union, other aggregates — bails);
//! - predicates are exactly `project_id = <lit>` + both timestamp bounds
//!   (a missing upper bound would race incoming MemBuffer writes);
//! - MemBuffer holds no rows in the window (fully flushed);
//! - table has no dedup keys OR every window partition is sweep-verified
//!   clean (same fingerprint gate as the read-side dedup skip — duplicates
//!   in Delta would inflate numRecords);
//! - every in-window file's `[min,max]` timestamp lies fully inside the
//!   window (boundary-straddling files bail — v1 has no hybrid scan);
//! - no file carries a deletion vector (numRecords is pre-DV).

use std::sync::Arc;

use datafusion::{
    arrow::array::{Array, Int64Array, RecordBatch, StringArray},
    error::Result as DFResult,
    logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator},
    physical_plan::ExecutionPlan,
    scalar::ScalarValue,
};
use tracing::debug;

use crate::database::Database;

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
    use crate::optimizers::{is_col_through_cast, swap_comparison};
    match e {
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => Some(Conjunct::True),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // project_id = 'lit'
            if *op == Operator::Eq {
                let (c, l) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(c)) => (c, v),
                    _ => return None,
                };
                if c.name == "project_id" {
                    return crate::optimizers::extract_utf8_string(l).map(Conjunct::ProjectId);
                }
                return None;
            }
            // timestamp bound (either operand order, cast-wrapped column ok)
            let (lit, op) = if is_col_through_cast(left, "timestamp") {
                (literal_micros(right)?, *op)
            } else if is_col_through_cast(right, "timestamp") {
                (literal_micros(left)?, swap_comparison(op))
            } else {
                return None;
            };
            match op {
                // Bounds here are only used for full-containment checks
                // against file min/max, so >= vs > need not be distinguished:
                // treating both as the inclusive window errs toward bailing
                // (a boundary-equal file straddles and bails), never toward
                // counting a row the predicate excludes... except it WOULD
                // for `>`/`<` with a file min/max exactly on the bound. Be
                // exact instead: half-open bounds shrink by 1µs.
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

/// Split a predicate into conjuncts (`AND` tree flatten).
fn split_conjunction(e: &Expr, out: &mut Vec<Expr>) {
    if let Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) = e {
        split_conjunction(left, out);
        split_conjunction(right, out);
    } else {
        out.push(e.clone());
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
        LogicalPlan::Projection(p) => {
            let ok = p.expr.iter().all(|e| match e {
                Expr::Column(_) => true,
                Expr::Alias(a) => matches!(a.expr.as_ref(), Expr::Column(_)),
                _ => false,
            });
            if !ok {
                return None;
            }
            p.input.as_ref()
        }
        _ => plan,
    };
    let LogicalPlan::Aggregate(agg) = agg_plan else { return None };
    if !agg.group_expr.is_empty() || agg.aggr_expr.len() != 1 {
        return None;
    }
    // count(*) / count(1) / count(non-null literal); no DISTINCT, no FILTER.
    let count_ok = match &agg.aggr_expr[0] {
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
        Expr::Alias(a) => match a.expr.as_ref() {
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
        },
        _ => false,
    };
    if !count_ok {
        return None;
    }

    // Walk down: row count is invariant under Projection/SubqueryAlias.
    // Collect Filter predicates and the TableScan's pushed filters.
    let mut node = agg.input.as_ref();
    let mut preds: Vec<Expr> = Vec::new();
    let scan = loop {
        match node {
            LogicalPlan::Projection(p) => node = p.input.as_ref(),
            LogicalPlan::SubqueryAlias(a) => node = a.input.as_ref(),
            LogicalPlan::Filter(f) => {
                split_conjunction(&f.predicate, &mut preds);
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
    let is_routing = scan
        .source
        .downcast_ref::<datafusion::datasource::DefaultTableSource>()
        .and_then(|src| src.table_provider.downcast_ref::<crate::database::ProjectRoutingTable>())
        .is_some();
    if !is_routing {
        return None;
    }
    for f in &scan.filters {
        split_conjunction(f, &mut preds);
    }

    let (mut project_id, mut lo, mut hi) = (None, None, None);
    // Dedup textually — the same conjunct commonly appears in both the
    // Filter node and the scan's pushed filters.
    preds.sort_by_key(|e| e.to_string());
    preds.dedup_by_key(|e| e.to_string());
    for p in &preds {
        match classify_conjunct(p)? {
            Conjunct::ProjectId(v) if project_id.as_ref().is_none_or(|prev| *prev == v) => project_id = Some(v),
            Conjunct::TsLow(v) => lo = Some(lo.map_or(v, |p: i64| p.max(v))),
            Conjunct::TsHigh(v) => hi = Some(hi.map_or(v, |p: i64| p.min(v))),
            Conjunct::True => {}
            _ => return None,
        }
    }
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
    let mut total: u64 = 0;
    for (min, max, records) in files {
        let (min, max, records) = (min?, max?, records?);
        if max < lo || min > hi {
            continue; // fully outside — contributes nothing
        }
        if min >= lo && max <= hi {
            total = total.checked_add(u64::try_from(records).ok()?)?;
        } else {
            return None; // straddles the boundary — needs a real scan
        }
    }
    Some(total)
}

/// Attempt the pushdown. `Ok(None)` = not applicable, plan normally.
pub async fn try_count_pushdown(plan: &LogicalPlan, database: &Arc<Database>) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    if !database.config().maintenance.timefusion_count_pushdown {
        return Ok(None);
    }
    let Some(q) = match_count_plan(plan) else { return Ok(None) };
    // Only tables served by ProjectRoutingTable qualify (system tables like
    // timefusion_stats share the session but not the storage model).
    if crate::schema_loader::get_schema(&q.table_name).is_none() {
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
    let dedup_keys: Vec<String> = crate::schema_loader::get_schema(&q.table_name).map(|s| s.dedup_keys.clone()).unwrap_or_default();
    let Ok(table_ref) = database.resolve_table(&q.project_id, &q.table_name).await else {
        return Ok(None);
    };
    let total = {
        let table = table_ref.read().await;
        // Gate: duplicates provably absent for the window, in THIS snapshot.
        if !dedup_keys.is_empty() && !database.dedup_window_clean(&table, &q.project_id, &q.table_name, (q.lo, q.hi)) {
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
    crate::metrics::record_count_pushdown_used();
    // Single-row result matching the plan's output schema (count is Int64).
    let out_schema: datafusion::arrow::datatypes::SchemaRef = Arc::new(plan.schema().as_arrow().clone());
    if out_schema.fields().len() != 1 || out_schema.field(0).data_type() != &datafusion::arrow::datatypes::DataType::Int64 {
        return Ok(None);
    }
    let batch = RecordBatch::try_new(out_schema.clone(), vec![Arc::new(Int64Array::from(vec![total as i64]))])
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let source = datafusion::datasource::memory::MemorySourceConfig::try_new(&[vec![batch]], out_schema, None)?;
    Ok(Some(Arc::new(datafusion::datasource::source::DataSourceExec::new(Arc::new(source)))))
}

/// Extract `(min_ts, max_ts, numRecords)` for this project's files from the
/// flattened add-actions batch and sum the fully-contained ones. `None` on
/// any missing column/stat, DV presence, or boundary straddle.
fn sum_from_actions(actions: &RecordBatch, q: &CountQuery) -> Option<u64> {
    fn i64_col<'a>(b: &'a RecordBatch, name: &str) -> Option<&'a Int64Array> {
        b.column_by_name(name)?.as_any().downcast_ref::<Int64Array>()
    }
    fn ts_micros_col(b: &RecordBatch, name: &str) -> Option<Int64Array> {
        use datafusion::arrow::{array::TimestampMicrosecondArray, compute::cast, datatypes::DataType};
        let c = b.column_by_name(name)?;
        let c = if matches!(c.data_type(), DataType::Timestamp(_, _)) {
            cast(c, &DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)).ok()?
        } else {
            return None;
        };
        Some(c.as_any().downcast_ref::<TimestampMicrosecondArray>()?.reinterpret_cast())
    }
    // Deletion vectors make numRecords an over-count — bail if ANY file has
    // one (column families vary by writer; check every dv-prefixed column).
    for (i, f) in actions.schema().fields().iter().enumerate() {
        if f.name().starts_with("deletionVector") && actions.column(i).null_count() < actions.num_rows() {
            return None;
        }
    }
    let pid = actions.column_by_name("partition.project_id")?.as_any().downcast_ref::<StringArray>()?;
    let records = i64_col(actions, "stats.numRecords")?;
    let min_ts = ts_micros_col(actions, "stats.minValues.timestamp")?;
    let max_ts = ts_micros_col(actions, "stats.maxValues.timestamp")?;
    let rows = (0..actions.num_rows())
        .filter(|&i| pid.is_valid(i) && pid.value(i) == q.project_id)
        .map(|i| (min_ts.is_valid(i).then(|| min_ts.value(i)), max_ts.is_valid(i).then(|| max_ts.value(i)), records.is_valid(i).then(|| records.value(i))))
        .collect::<Vec<_>>();
    sum_fully_contained(rows, q.lo, q.hi)
}

#[cfg(test)]
mod tests {
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
