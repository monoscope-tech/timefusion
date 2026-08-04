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
//! - no file carries a deletion vector (numRecords is pre-DV);
//! - the table cannot hold merge-on-read tombstones (`tombstone_column`
//!   declared *and* `version_append` on — a declared-but-dormant column can
//!   hold none, so it does not disqualify the fast path).

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, SchemaRef},
    },
    datasource::{DefaultTableSource, memory::MemorySourceConfig, source::DataSourceExec},
    error::Result as DFResult,
    logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, utils::split_conjunction},
    physical_plan::ExecutionPlan,
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
    use crate::optimizers::{extract_utf8_string, is_col_through_cast, swap_comparison};
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
    let Some(schema) = crate::schema_loader::get_schema(&q.table_name) else { return Ok(None) };
    if schema.tombstones_possible()
        && let Some(total) = try_logical_count(database, &q, schema).await
    {
        debug!("count_pushdown: answered {}/{} [{}, {}] = {} from logical-count index", q.project_id, q.table_name, q.lo, q.hi, total);
        crate::metrics::record_logical_count_pushdown_used();
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
        if !schema.dedup_keys.is_empty() && !database.dedup_window_clean(&table, &q.project_id, &q.table_name, (q.lo, q.hi)) {
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
    count_result(plan, total)
}

async fn try_logical_count(database: &Arc<Database>, q: &CountQuery, schema: &crate::schema_loader::TableSchema) -> Option<u64> {
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
    let (mem_batches, hot_batches) = match database.buffered_layer() {
        Some(layer) => {
            let mem = layer.query(&q.project_id, &q.table_name, &filters).ok()?;
            // The ordinary MOR scan resolves mem ∪ hot ∪ Delta. Omitting
            // the raw hot-tier versions can leave a newer tombstone/update out
            // of the cached winner overlay even though the physical Delta base
            // is complete. Read only the four narrow index columns; files that
            // the hot tier declines remain represented by the Delta base.
            let arrow_schema = schema.schema_ref();
            let projection: Vec<usize> =
                ["timestamp", "id", tiebreak, deleted].into_iter().map(|column| arrow_schema.index_of(column).ok()).collect::<Option<_>>()?;
            let mem_ranges = layer.get_bucket_ranges(&q.project_id, &q.table_name);
            let hot = layer
                .hot_tier()
                .query_partitioned(&q.project_id, &q.table_name, Some((q.lo, q.hi)), &mem_ranges, &filters, &arrow_schema, Some(&projection))
                .await;
            (mem, hot.partitions.into_iter().flatten().collect())
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

    let columns = crate::logical_count_index::LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak, deleted };
    // Keep the synchronous append delta small. The full rebuild is already
    // single-flight; a large gap falls back to authoritative DedupExec until
    // the new base is ready instead of moving that scan onto every query.
    if added_files.len() > crate::logical_count_index::MAX_APPEND_OVERLAY_FILES {
        for date in stale_dates {
            database.schedule_logical_count_build(&q.project_id, &q.table_name, &date, true);
        }
        return None;
    }
    let mut overlay_batches = mem_batches;
    overlay_batches.extend(hot_batches);
    overlay_batches.extend(database.logical_count_overlay_batches(delta_snapshot, log_store, added_files, columns).await.ok()?);
    indexes.into_iter().try_fold(0u64, |total, (date, index)| {
        let day_lo = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        let day_hi = date.succ_opt()?.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
        let count = index.count_with_overlay(&overlay_batches, q.lo.max(day_lo), hi.min(day_hi), columns).ok()?;
        total.checked_add(count)
    })
}

/// Extract `(min_ts, max_ts, numRecords)` for this project's files from the
/// flattened add-actions batch and sum the fully-contained ones. `None` on
/// any missing column/stat, DV presence, or boundary straddle.
fn sum_from_actions(actions: &RecordBatch, q: &CountQuery) -> Option<u64> {
    fn ts_micros_col(b: &RecordBatch, name: &str) -> Option<Int64Array> {
        use datafusion::arrow::{array::TimestampMicrosecondArray, compute::cast, datatypes::TimeUnit};
        let c = b.column_by_name(name)?;
        matches!(c.data_type(), DataType::Timestamp(_, _)).then_some(())?;
        let c = cast(c, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
        Some(c.as_any().downcast_ref::<TimestampMicrosecondArray>()?.reinterpret_cast())
    }
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
