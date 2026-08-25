use datafusion::{
    logical_expr::{
        BinaryExpr, Expr, Operator,
        expr::{Cast, TryCast},
    },
    prelude::col,
    scalar::ScalarValue,
};

/// Avoids the competing `as_any` methods in this crate's trait scope.
pub fn downcast<T: 'static>(any: &dyn std::any::Any) -> Option<&T> {
    any.downcast_ref()
}

/// Extracts any UTF-8 scalar representation.
pub fn extract_utf8_string(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Matches a column through coercion casts.
pub fn is_col_through_cast(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(c) => c.name == name,
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => is_col_through_cast(expr, name),
        _ => false,
    }
}

/// Removes coercion casts that otherwise hide literals from pruning.
pub fn unwrap_literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(scalar, _) => Some(scalar),
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => unwrap_literal(expr),
        _ => None,
    }
}

/// Timestamp scalar of any unit → microseconds since epoch.
pub fn scalar_micros(v: &ScalarValue) -> Option<i64> {
    Some(match v {
        ScalarValue::TimestampSecond(Some(s), _) => s.checked_mul(1_000_000)?,
        ScalarValue::TimestampMillisecond(Some(ms), _) => ms.checked_mul(1_000)?,
        ScalarValue::TimestampMicrosecond(Some(us), _) => *us,
        ScalarValue::TimestampNanosecond(Some(ns), _) => ns.div_euclid(1_000),
        _ => return None,
    })
}

/// Reverses comparisons with swapped operands.
pub fn swap_comparison(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => other,
    }
}

/// Converts timestamp filters to Delta date-partition filters.
pub mod time_range_partition_pruner {
    use super::*;

    /// Derives partition dates from bounds on the declared time column.
    pub fn timestamp_to_date_filters(expr: &Expr, time_column: &str) -> Vec<Expr> {
        let date_filter = |expr: &Expr, op: Operator| {
            let date = chrono::DateTime::from_timestamp_micros(scalar_micros(unwrap_literal(expr)?)?)?.date_naive();
            let days_since_epoch = (date.and_hms_opt(0, 0, 0)?.and_utc().timestamp() / 86400) as i32;
            let date_op = match op {
                Operator::Gt | Operator::GtEq => Operator::GtEq,
                Operator::Lt | Operator::LtEq => Operator::LtEq,
                Operator::Eq => Operator::Eq,
                _ => return None,
            };
            Some(Expr::BinaryExpr(BinaryExpr::new(Box::new(col("date")), date_op, Box::new(Expr::Literal(ScalarValue::Date32(Some(days_since_epoch)), None)))))
        };

        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (lit_expr, op) = if is_col_through_cast(left.as_ref(), time_column) {
                    (right.as_ref(), *op)
                } else if is_col_through_cast(right.as_ref(), time_column) {
                    (left.as_ref(), swap_comparison(*op))
                } else {
                    return vec![];
                };
                date_filter(lit_expr, op).into_iter().collect()
            }
            Expr::Between(between) if !between.negated && is_col_through_cast(between.expr.as_ref(), time_column) => {
                [date_filter(between.low.as_ref(), Operator::GtEq), date_filter(between.high.as_ref(), Operator::LtEq)].into_iter().flatten().collect()
            }
            _ => vec![],
        }
    }

    /// Adds necessary date-partition bounds without excluding matching rows.
    pub fn with_date_partition_filters(predicate: Expr, time_column: &str) -> Expr {
        fn walk(expr: &Expr, time_column: &str) -> Vec<Expr> {
            match expr {
                Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => [left, right].into_iter().flat_map(|e| walk(e, time_column)).collect(),
                other => timestamp_to_date_filters(other, time_column),
            }
        }
        let date_filters = walk(&predicate, time_column); // bound separately: `predicate` is moved into the fold below
        date_filters.into_iter().fold(predicate, Expr::and)
    }

    /// Collects date bounds from an AND tree for pruning diagnostics.
    pub fn extract_date_bounds(expr: &Expr) -> Vec<(Operator, i32)> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => [left, right].into_iter().flat_map(|e| extract_date_bounds(e)).collect(),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), Expr::Literal(ScalarValue::Date32(Some(day)), _)) if c.name == "date" => vec![(*op, *day)],
                _ => vec![],
            },
            _ => vec![],
        }
    }
}

/// Extracts the first positive `project_id = literal` AND-conjunct.
pub fn extract_project_id_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(col)) if col.name == "project_id" => extract_utf8_string(v),
            _ => None,
        },
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => extract_project_id_from_expr(left).or_else(|| extract_project_id_from_expr(right)),
        _ => None,
    }
}

pub struct ProjectIdPushdown;

impl ProjectIdPushdown {
    pub fn has_project_id_filter(filters: &[Expr]) -> bool {
        filters.iter().any(Self::contains_project_id)
    }

    /// Conservative: recognises `project_id = 'x'` (either argument order) and
    /// AND-conjuncts that include one. **OR** is intentionally NOT handled —
    /// `WHERE project_id = 'a' OR project_id = 'b'` is rare in practice and
    /// reporting "no project_id filter" for it keeps the multi-tenant guard
    /// strict (the query then errors out instead of silently scanning all
    /// projects). Extend here if cross-project OR becomes a real workload.
    pub fn contains_project_id(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Column(col), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(col))
                if col.name == "project_id"
            ),
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => Self::contains_project_id(left) || Self::contains_project_id(right),
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::time_range_partition_pruner::{extract_date_bounds, timestamp_to_date_filters, with_date_partition_filters};
    use super::*;
    use datafusion::{
        arrow::datatypes::{DataType, TimeUnit},
        logical_expr::Between,
    };

    fn timestamp(micros: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into())), None)
    }

    fn date_filters(expr: Expr) -> Vec<(Operator, i32)> {
        timestamp_to_date_filters(&expr, "timestamp").iter().flat_map(extract_date_bounds).collect()
    }

    #[test]
    fn timestamp_between_derives_two_inclusive_date_bounds() {
        let expr = Expr::Between(Between::new(
            Box::new(col("timestamp")),
            false,
            Box::new(timestamp(1_704_067_200_000_000)),
            Box::new(timestamp(1_704_240_000_000_000)),
        ));

        assert_eq!(date_filters(expr), vec![(Operator::GtEq, 19_723), (Operator::LtEq, 19_725)]);
    }

    #[test]
    fn timestamp_comparisons_support_units_casts_and_reversed_operands() {
        let timestamp_col = col("timestamp");
        let cast_timestamp = Expr::Cast(Cast::new(Box::new(timestamp_col.clone()), DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))));
        let try_cast_timestamp = Expr::TryCast(TryCast::new(Box::new(timestamp_col.clone()), DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))));
        let start = 1_704_067_200_000_000i64;
        let cases = [
            (Expr::BinaryExpr(BinaryExpr::new(Box::new(timestamp_col.clone()), Operator::GtEq, Box::new(timestamp(start)))), Operator::GtEq),
            (Expr::BinaryExpr(BinaryExpr::new(Box::new(timestamp(start)), Operator::LtEq, Box::new(timestamp_col.clone()))), Operator::GtEq),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(cast_timestamp),
                    Operator::Lt,
                    Box::new(Expr::Literal(ScalarValue::TimestampNanosecond(Some(start * 1_000), Some("UTC".into())), None)),
                )),
                Operator::LtEq,
            ),
            (Expr::BinaryExpr(BinaryExpr::new(Box::new(try_cast_timestamp), Operator::Gt, Box::new(timestamp(start)))), Operator::GtEq),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(timestamp_col.clone()),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(Some(start / 1_000), Some("UTC".into())), None)),
                )),
                Operator::Eq,
            ),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(timestamp_col),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::TimestampSecond(Some(start / 1_000_000), Some("UTC".into())), None)),
                )),
                Operator::Eq,
            ),
        ];

        for (expr, expected_op) in cases {
            assert_eq!(date_filters(expr), vec![(expected_op, 19_723)]);
        }
    }

    /// Regression for the 2026-07-17 prod OOM: the monoscope hash-enrichment
    /// UPDATE-2 predicate (`project_id = ? AND timestamp >= ? AND timestamp < ?`)
    /// must gain `date` partition bounds so the Delta merge prunes files instead
    /// of scanning all 2704 partitions.
    #[test]
    fn monoscope_update_predicate_gains_date_partition_bounds() {
        let start = 1_704_067_200_000_000i64; // 2024-01-01 → day 19_723
        let end = 1_704_240_000_000_000i64; //   2024-01-03 → day 19_725
        let ts = || col("timestamp");
        let predicate = col("project_id")
            .eq(Expr::Literal(ScalarValue::Utf8(Some("p".into())), None))
            .and(Expr::BinaryExpr(BinaryExpr::new(Box::new(ts()), Operator::GtEq, Box::new(timestamp(start)))))
            .and(Expr::BinaryExpr(BinaryExpr::new(Box::new(ts()), Operator::Lt, Box::new(timestamp(end)))));

        // Bug: no `date` bounds derived from the raw timestamp predicate.
        assert!(extract_date_bounds(&predicate).is_empty());

        let augmented = with_date_partition_filters(predicate, "timestamp");
        let bounds = {
            let mut b = extract_date_bounds(&augmented); // sorted: derivation order isn't part of the contract
            b.sort_by_key(|(_, day)| *day);
            b
        };
        assert_eq!(bounds, vec![(Operator::GtEq, 19_723), (Operator::LtEq, 19_725)]);

        // No time-column bounds → predicate returned untouched.
        let no_ts = col("project_id").eq(Expr::Literal(ScalarValue::Utf8(Some("p".into())), None));
        assert!(extract_date_bounds(&with_date_partition_filters(no_ts, "timestamp")).is_empty());
    }

    /// Regression for the 2026-07-20 prod finding: ~6% of hash-enrichment merges
    /// full-scanned (207 GB, predicate_filtered=0) because extended-protocol param
    /// binding + TypeCoercion wraps the timestamp bound in a `Cast(Literal)` — the
    /// literal side must be unwrapped or no `date` bound is derived.
    #[test]
    fn cast_wrapped_timestamp_literal_still_derives_date_bounds() {
        let start = 1_704_067_200_000_000i64; // 2024-01-01 → day 19_723
        let ts_col = col("timestamp");
        // `timestamp >= CAST($1 AS Timestamp(ns))` where the bound param arrived as µs.
        let cast_lit = Expr::Cast(Cast::new(Box::new(timestamp(start)), DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))));
        let expr = Expr::BinaryExpr(BinaryExpr::new(Box::new(ts_col), Operator::GtEq, Box::new(cast_lit)));
        assert_eq!(date_filters(expr), vec![(Operator::GtEq, 19_723)]);
    }
}

use std::{collections::HashSet, sync::Arc};

use crate::read::functions::json_to_variant_udf;
use datafusion::{
    common::{
        DataFusionError, Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    logical_expr::{DmlStatement, LogicalPlan, Projection, ScalarUDF, Values, WriteOp, expr::ScalarFunction},
    optimizer::AnalyzerRule,
};
use tracing::debug;

use crate::schema::is_variant_type;

/// AnalyzerRule that rewrites INSERT statements to wrap Utf8 expressions
/// going into Variant columns with `json_to_variant()`.
///
/// This is necessary because DataFusion's type checker rejects Utf8 -> Variant(Struct)
/// casts outright; `json_to_variant()` gives the planner a concrete UDF call instead.
#[derive(Debug, Default)]
pub struct VariantInsertRewriter;

impl AnalyzerRule for VariantInsertRewriter {
    fn name(&self) -> &str {
        "variant_insert_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(rewrite_insert_node).map(|t| t.data)
    }
}

fn rewrite_insert_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let rewritten = match &plan {
        LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Insert(_)) => rewrite_insert(dml)?,
        _ => None,
    };
    Ok(rewritten.map_or_else(|| Transformed::no(plan), Transformed::yes))
}

fn rewrite_insert(dml: &DmlStatement) -> Result<Option<LogicalPlan>> {
    let target_schema = dml.target.schema();
    let input_schema = dml.input.schema();

    // An input column is Variant iff the TARGET column of the same *name* is.
    // Set (not Vec) because membership is tested per row × column below.
    let variant_indices: HashSet<usize> = input_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| target_schema.column_with_name(f.name()).is_some_and(|(_, t)| is_variant_type(t.data_type())))
        .map(|(i, _)| i)
        .collect();

    if variant_indices.is_empty() {
        return Ok(None);
    }
    debug!(table = %dml.table_name, ?variant_indices, "VariantInsertRewriter: wrapping Utf8 literals for Variant columns");

    Ok(rewrite_input_for_variant(&dml.input, &variant_indices)?.map(|input| LogicalPlan::Dml(DmlStatement { input: Arc::new(input), ..dml.clone() })))
}

/// Rewrite only the immediate child of the Dml node. `variant_indices` are
/// positions in `dml.input.schema()` (i.e. target table order) — they're only
/// valid for that single plan. Recursing into nested projections with the same
/// indices would mis-wrap unrelated columns whose positions happen to align.
fn rewrite_input_for_variant(input: &LogicalPlan, variant: &HashSet<usize>) -> Result<Option<LogicalPlan>> {
    match input {
        LogicalPlan::Values(v) => Ok(v.values.iter().any(|row| needs_wrap(row, variant)).then(|| {
            let udf = json_to_variant_udf();
            LogicalPlan::Values(Values { schema: v.schema.clone(), values: v.values.iter().map(|row| wrap_variant_exprs(row, variant, &udf)).collect() })
        })),
        LogicalPlan::Projection(p) => needs_wrap(&p.expr, variant)
            .then(|| Projection::try_new(wrap_variant_exprs(&p.expr, variant, &json_to_variant_udf()), p.input.clone()).map(LogicalPlan::Projection))
            .transpose(),
        // Shapes like `INSERT … SELECT col FROM staging` (TableScan, Filter, etc.)
        // don't currently get json_to_variant wrapping. Fail at plan time with
        // an actionable message rather than letting the write hit an opaque
        // type-mismatch error after travelling through the executor.
        other => Err(DataFusionError::Plan(format!(
            "INSERT into Variant column from input shape `{}` is not supported. \
             Use INSERT … VALUES, or add an explicit `json_to_variant(col)` in the SELECT projection.",
            other.display()
        ))),
    }
}

/// Shared by the probe and the rewrite below — the two must never disagree.
fn should_wrap(i: usize, e: &Expr, variant: &HashSet<usize>) -> bool {
    variant.contains(&i) && is_utf8_expr(e)
}

/// Short-circuiting probe so a no-op insert never pays the per-row clone.
fn needs_wrap(exprs: &[Expr], variant: &HashSet<usize>) -> bool {
    exprs.iter().enumerate().any(|(i, e)| should_wrap(i, e, variant))
}

fn wrap_variant_exprs(exprs: &[Expr], variant: &HashSet<usize>, udf: &Arc<ScalarUDF>) -> Vec<Expr> {
    exprs
        .iter()
        .enumerate()
        .map(|(i, e)| if should_wrap(i, e, variant) { Expr::ScalarFunction(ScalarFunction { func: udf.clone(), args: vec![e.clone()] }) } else { e.clone() })
        .collect()
}

/// Matches *literal* Utf8 only (and casts thereof). Column references — e.g.
/// `INSERT INTO t (payload) SELECT col FROM staging` — would need a type lookup
/// against the source plan's schema; today only the pgwire VALUES form needs
/// Variant coercion (other shapes are rejected in `rewrite_input_for_variant`).
fn is_utf8_expr(expr: &Expr) -> bool {
    match expr {
        // NULL literals must pass through: json_to_variant would try to parse "" and fail.
        Expr::Literal(ScalarValue::Utf8(Some(_)) | ScalarValue::Utf8View(Some(_)) | ScalarValue::LargeUtf8(Some(_)), _) => true,
        Expr::Cast(cast) => is_utf8_expr(&cast.expr),
        _ => false,
    }
}

// Variant-aware SELECT-plan post-processing.
//
// Two passes, both gated on the plan being a non-DML (SELECT-like) plan:
//
// 1. **TableScan schema patch.** TimeFusion's `ProjectRoutingTable::schema()`
//    returns a *lying* schema that substitutes Variant columns with
//    `Utf8View` so DataFusion's INSERT-VALUES type checker accepts raw
//    JSON string literals. For SELECT plans we want the real Variant
//    type so downstream UDFs (`variant_get`, `jsonb_path_exists`, …)
//    receive Struct{Binary,Binary} and call
//    `parquet_variant_compute::variant_get` directly. We walk each
//    `LogicalPlan::TableScan`, downcast its source to
//    `DefaultTableSource → ProjectRoutingTable`, and rebuild the scan's
//    `projected_schema` with Variant types restored.
//
// 2. **Root-projection JSON wrap.** Bare `SELECT payload` from a pgwire
//    client must serialize the Variant to JSON text for the wire. We
//    used to do this at the scan boundary (`VariantToJsonExec`) which
//    forced every intermediate operator to deal with Utf8 and made
//    Variant slower than plain JSON text. Now we wrap only the
//    *outermost* Projection — peeling Sort/Limit/Distinct/SubqueryAlias —
//    so intermediate `variant_get` / `jsonb_path_exists` etc. operate
//    on the binary Variant.

use std::collections::HashMap;

use datafusion::{
    arrow::datatypes::{DataType, Field},
    catalog::default_table_source::DefaultTableSource,
    common::{Column, DFSchema, DFSchemaRef},
    logical_expr::{
        Distinct, ExprSchemable, TableScan,
        expr::{InList, Like},
    },
    sql::TableReference,
};
use tracing::warn;

use crate::{
    database::ProjectRoutingTable,
    read::functions::{VariantToJsonExtUdf, variant_to_json_udf},
};

#[derive(Debug, Default)]
pub struct VariantSelectRewriter;

impl AnalyzerRule for VariantSelectRewriter {
    fn name(&self) -> &str {
        "variant_select_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Skip DML entirely. DML targets aren't a wire projection (no
        // variant_to_json wrap needed), and DML's input scans are already
        // handled by VariantInsertRewriter wrapping literals with
        // json_to_variant; injecting a Variant-typed schema there would
        // mismatch the writer's expected Utf8 input.
        if matches!(plan, LogicalPlan::Dml(_)) {
            return Ok(plan);
        }
        // Pass 1: bottom-up: patch each TableScan's projected_schema so
        // Variant columns carry the real Variant type, then recompute every
        // parent's cached DFSchema so the new type propagates up through
        // intermediate Projections / Sorts / Filters. Without the per-node
        // recompute, `wrap_projection`'s `is_variant_expr` check sees a
        // stale Utf8View type from a parent's cached schema and skips
        // wrapping (e.g. `ORDER BY x LIMIT n` introduces an outer
        // Projection over a Sort whose schema must be re-derived).
        //
        // Restoring the Variant type re-breaks any expression that had the
        // (previously Utf8View) column in a scalar-text position, so before the
        // recompute we run `coerce_variant_value_positions` on each node to lower
        // those Variant operands back to text via `variant_to_json`. Order
        // matters: the child scan is patched first (transform_up is bottom-up),
        // so a parent Filter/Projection sees Variant-typed input columns and can
        // coerce its predicates before recompute type-checks them.
        let patched = restore_variant_scan_types(plan)?.data;
        // Pass 2: wrap Variant-typed projections at the topmost SELECT
        // projection with variant_to_json for the wire.
        wrap_root_projection(patched)
    }
}

/// Restore the real Variant `Struct{Binary,Binary}` type on every TableScan's
/// `projected_schema` (see `patch_table_scan`), lower any Variant-in-text-
/// position expr back to `variant_to_json`, and recompute cached schemas
/// bottom-up so the restored type propagates. Returns `Transformed::yes` iff at
/// least one scan was actually re-typed.
///
/// Shared by the analyzer (`VariantSelectRewriter` Pass 1) and by
/// `VariantScanSchemaRestore`, the optimizer rule that re-applies it after
/// DataFusion's `optimize_projections` rebuilds each TableScan from the lying
/// `ProjectRoutingTable::schema()` and thereby reverts Variant → Utf8View.
pub(crate) fn restore_variant_scan_types(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut changed = false;
    let out = plan
        .transform_up(|node| {
            let patched = patch_table_scan(node)?;
            changed |= patched.transformed;
            // Once a scan below us was restored to Variant, this node's exprs may
            // face `Struct op Utf8`; coerce them and recompute the cached schema
            // so the new type propagates. Subtrees with no restored scan below
            // are left untouched (recompute is harmless but pointless there).
            if changed {
                let coerced = coerce_variant_value_positions(patched.data)?;
                Ok(Transformed::yes(coerced.recompute_schema()?))
            } else {
                Ok(patched)
            }
        })?
        .data;
    Ok(if changed { Transformed::yes(out) } else { Transformed::no(out) })
}

/// Re-applies `restore_variant_scan_types` after DataFusion's built-in
/// `optimize_projections` rebuilds each `TableScan` via `TableScan::try_new`,
/// which re-derives `projected_schema` from the lying `ProjectRoutingTable::schema()`
/// (Variant → Utf8View + `tf.pg_type=jsonb`) and so discards the analyzer's
/// Pass-1 Variant patch. The physical scan always emits the real Variant
/// struct, so the reverted logical scan disagrees with it. Most SELECTs never
/// notice, but DataFusion's Aggregate physical planner asserts
/// physical-input-schema == logical-input-schema — and `DISTINCT ON` lowers to
/// an Aggregate over `first_value` — so `SELECT DISTINCT ON (k) *` touching a
/// Variant column blew up with XX000 "Physical input schema should be the
/// same…" (2026-07-14 monoscope fetchEventExamples). Registered last so it runs
/// after `optimize_projections` in each optimizer pass.
#[derive(Debug, Default)]
pub struct VariantScanSchemaRestore;

impl datafusion::optimizer::OptimizerRule for VariantScanSchemaRestore {
    fn name(&self) -> &str {
        "variant_scan_schema_restore"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn datafusion::optimizer::OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        // DML input scans are handled by VariantInsertRewriter (Utf8 literals →
        // json_to_variant); re-typing them here would mismatch the writer.
        if matches!(plan, LogicalPlan::Dml(_)) {
            return Ok(Transformed::no(plan));
        }
        restore_variant_scan_types(plan)
    }
}

fn patch_table_scan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::TableScan(scan) = plan else {
        return Ok(Transformed::no(plan));
    };
    // Source must be a DefaultTableSource around ProjectRoutingTable.
    let Some(routing) = scan.source.downcast_ref::<DefaultTableSource>().and_then(|src| src.table_provider.downcast_ref::<ProjectRoutingTable>()) else {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    };
    // Fast path: if no Utf8View columns are projected, there can be no
    // Variant columns to un-lie about — bail before the HashMap+clones.
    // Over-approximates: a genuine non-Variant Utf8View column still falls
    // through to the map build below, which then finds no Variant counterpart
    // and bails — cheap enough for the common case (Variant scans).
    let lying_schema = scan.projected_schema.as_arrow();
    if !lying_schema.fields().iter().any(|f| matches!(f.data_type(), DataType::Utf8View)) {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }

    // Every Variant column of the real schema, by name (O(n) lookup below).
    let real = routing.real_schema();
    let variant_by_name: HashMap<&str, &Arc<Field>> = real.fields().iter().filter(|f| is_variant_type(f.data_type())).map(|f| (f.name().as_str(), f)).collect();
    if !lying_schema.fields().iter().any(|f| variant_by_name.contains_key(f.name().as_str())) {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }
    // We restore only the Variant Struct{Binary,Binary} *type*, with EMPTY
    // field metadata — NOT `Arc::clone(real_field)`, which would carry the
    // `ARROW:extension:name = arrow.parquet.variant` marker. The physical scan
    // (both MemBuffer and Delta paths) emits Variant fields with no metadata,
    // and DataFusion's Aggregate physical planner asserts that the physical
    // input schema equals the one derived from the logical input — including
    // field metadata. Cloning the marked field made `DISTINCT ON` over a
    // Variant column fail that assert with XX000 even once the data type
    // matched. The extension marker is only needed on the write path
    // (delta-rs / parquet-variant-compute); Variant detection at read time is
    // structural (`is_variant_type`), so dropping it here is safe.
    let patched_fields = lying_schema.fields().iter().map(|f| match variant_by_name.get(f.name().as_str()) {
        Some(rf) => Arc::new(Field::new(f.name(), rf.data_type().clone(), rf.is_nullable())),
        None => f.clone(),
    });
    // Preserve the original DFSchema's column qualifiers (e.g. table aliases).
    let qualified: Vec<(Option<TableReference>, Arc<Field>)> = scan.projected_schema.iter().map(|(q, _)| q.cloned()).zip(patched_fields).collect();
    let new_df: DFSchemaRef = Arc::new(DFSchema::new_with_metadata(qualified, lying_schema.metadata().clone())?);
    debug!(target: "variant_select_rewriter", "patched TableScan({}) schema → Variant", scan.table_name);
    Ok(Transformed::yes(LogicalPlan::TableScan(TableScan { projected_schema: new_df, ..scan })))
}

/// Value-position Variant → text coercion.
///
/// `patch_table_scan` restores the real Variant `Struct{Binary,Binary}` type on
/// the scan, which then propagates up. Any expression that had a Variant column
/// in a *scalar-text* position — a comparison/regex against a string, a
/// `LIKE`/`ILIKE`/`SIMILAR TO`, a `CAST(… AS text)`, or `IN (str, …)` — now
/// faces DataFusion with `Struct op Utf8`, which DF54 cannot coerce: bare
/// `body = 'x'` and `body LIKE …` error out, and `CAST(body AS text)` in a
/// projection silently yields empty strings. We lower the Variant side to
/// canonical JSON text via `variant_to_json` — exactly Postgres `jsonb::text`
/// semantics (scalar strings stay quoted, composites serialize), identical to
/// what the wire already returns for a bare `SELECT body`. The `->`/`->>`
/// accessors keep their own lowering (`VariantAwareExprPlanner`) and are not
/// touched here.
fn coerce_variant_value_positions(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Merge every input schema so column refs in this node's exprs resolve to
    // their (now Variant-restored) types. Single input for Filter/Projection;
    // multiple for joins whose ON clause may touch a Variant column. Leaves
    // (TableScan / Values) merge to nothing and fall out of the fast path.
    let schema = plan.inputs().iter().fold(DFSchema::empty(), |mut acc, input| {
        acc.merge(input.schema().as_ref());
        acc
    });
    // Fast path: no Variant column in scope → nothing to coerce.
    if !schema.fields().iter().any(|f| is_variant_type(f.data_type())) {
        return Ok(plan);
    }
    let to_json = variant_to_json_udf();
    plan.map_expressions(|expr| expr.transform_up(|e| coerce_expr(e, &schema, &to_json))).map(|t| t.data)
}

/// Bottom-up rewrite of a single expression: wrap any Variant operand that sits
/// in a scalar-text position with `variant_to_json`. Idempotent — an
/// already-wrapped operand types as `Utf8` and `is_variant_expr` returns false.
fn coerce_expr(e: Expr, schema: &DFSchema, to_json: &Arc<ScalarUDF>) -> Result<Transformed<Expr>> {
    let wrap = |x: Expr| Expr::ScalarFunction(ScalarFunction { func: to_json.clone(), args: vec![x] });
    // Box::new(wrap(*l.expr)) can't be written as struct-update (`..l`) — that
    // would read a partially moved `l` — so rebind through a local mut.
    let wrap_like = |mut l: Like| {
        l.expr = Box::new(wrap(*l.expr));
        l
    };
    match e {
        // CAST(variant AS text) → CAST(variant_to_json(variant) AS <same text type>).
        // Covers monoscope's `body::text` and its `COALESCE(NULLIF(body::text, ''),
        // …)` error_text. The outer cast is kept so the result stays real text
        // (pg OID 25) rather than variant_to_json's jsonb-tagged output (OID 3802) —
        // an explicit `::text` must describe as text over the wire.
        Expr::Cast(Cast { expr, field }) if is_text_type(field.data_type()) && is_variant_expr(&expr, schema) => {
            Ok(Transformed::yes(Expr::Cast(Cast { expr: Box::new(wrap(*expr)), field })))
        }
        // Comparison / regex where a Variant operand faces text.
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if is_text_comparison_op(op) && (is_variant_expr(&left, schema) || is_variant_expr(&right, schema)) =>
        {
            let left = if is_variant_expr(&left, schema) { Box::new(wrap(*left)) } else { left };
            let right = if is_variant_expr(&right, schema) { Box::new(wrap(*right)) } else { right };
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr { left, op, right })))
        }
        // LIKE / ILIKE / NOT LIKE / NOT ILIKE, and SIMILAR TO — same payload,
        // different constructor.
        Expr::Like(l) if is_variant_expr(&l.expr, schema) => Ok(Transformed::yes(Expr::Like(wrap_like(l)))),
        Expr::SimilarTo(l) if is_variant_expr(&l.expr, schema) => Ok(Transformed::yes(Expr::SimilarTo(wrap_like(l)))),
        // IN (str, …).
        Expr::InList(InList { expr, list, negated }) if is_variant_expr(&expr, schema) => {
            Ok(Transformed::yes(Expr::InList(InList { expr: Box::new(wrap(*expr)), list, negated })))
        }
        other => Ok(Transformed::no(other)),
    }
}

fn is_text_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

fn is_text_comparison_op(op: Operator) -> bool {
    use Operator::{
        Eq, Gt, GtEq, ILikeMatch, IsDistinctFrom, IsNotDistinctFrom, LikeMatch, Lt, LtEq, NotEq, NotILikeMatch, NotLikeMatch, RegexIMatch, RegexMatch,
        RegexNotIMatch, RegexNotMatch,
    };
    matches!(
        op,
        Eq | NotEq
            | Lt
            | LtEq
            | Gt
            | GtEq
            | IsDistinctFrom
            | IsNotDistinctFrom
            | RegexMatch
            | RegexIMatch
            | RegexNotMatch
            | RegexNotIMatch
            | LikeMatch
            | ILikeMatch
            | NotLikeMatch
            | NotILikeMatch
    )
}

/// Peel Sort / Limit / Distinct / SubqueryAlias from the root and wrap
/// the underlying Projection's Variant-typed expressions with
/// `variant_to_json()`. Returns the plan unchanged if no Projection sits
/// inside that peel.
fn wrap_root_projection(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Walk down via a single linear path of "peelable" parents, transforming
    // the first Projection we find. Anything outside this peel (Joins,
    // CTEs, Window, etc.) blocks wrapping — those nodes' inputs aren't the
    // wire output. Recursion is depth-bounded by the parser's plan-depth
    // limit; the explicit MAX_PEEL guard below is belt-and-suspenders against
    // an adversarial / nested-CTE plan stack-overflowing us.
    const MAX_PEEL: u16 = 256;
    fn peel(plan: LogicalPlan, depth: u16) -> Result<LogicalPlan> {
        if depth >= MAX_PEEL {
            // Pathological plan depth — bail to avoid stack overflow. Variant
            // columns inside the un-peeled subtree exit unwrapped; warn so this
            // is traceable instead of silent.
            warn!(
                target: "variant_select_rewriter",
                max_peel = MAX_PEEL,
                "wrap_root_projection hit MAX_PEEL — deeply nested Sort/Limit/Distinct/SubqueryAlias chain; Variant root wrapping skipped"
            );
            return Ok(plan);
        }
        let d = depth + 1;
        // Recurse into a node's single input in place, leaving the parent's own
        // fields (and cached schema) untouched.
        let down = |input: Arc<LogicalPlan>| -> Result<Arc<LogicalPlan>> { Ok(Arc::new(peel(Arc::unwrap_or_clone(input), d)?)) };
        match plan {
            LogicalPlan::Sort(mut s) => {
                s.input = down(s.input)?;
                Ok(LogicalPlan::Sort(s))
            }
            LogicalPlan::Limit(mut l) => {
                l.input = down(l.input)?;
                Ok(LogicalPlan::Limit(l))
            }
            LogicalPlan::Distinct(Distinct::All(input)) => Ok(LogicalPlan::Distinct(Distinct::All(down(input)?))),
            LogicalPlan::Distinct(Distinct::On(mut on)) => {
                on.input = down(on.input)?;
                Ok(LogicalPlan::Distinct(Distinct::On(on)))
            }
            LogicalPlan::SubqueryAlias(mut s) => {
                s.input = down(s.input)?;
                Ok(LogicalPlan::SubqueryAlias(s))
            }
            // Some DataFusion rewrite passes promote a Filter above the
            // outermost Projection. Peel through it so Variant columns still
            // reach the wire wrapped, not as raw binary.
            LogicalPlan::Filter(mut f) => {
                f.input = down(f.input)?;
                Ok(LogicalPlan::Filter(f))
            }
            LogicalPlan::Projection(proj) => wrap_projection(proj),
            // Union/Intersect/Except/Aggregate/Join/Window/etc. — anything we
            // can't peel through. We don't descend (would need branch-aware
            // rewriting that handles set ops, joins, aggregates differently),
            // but we *can* wrap above: emit a top-level Projection that calls
            // variant_to_json on each Variant-typed output column. Intermediate
            // ops still see binary Variant; only the wire boundary converts.
            other => add_root_variant_projection(other),
        }
    }
    peel(plan, 0)
}

/// Add a top-level Projection above `plan` that wraps every Variant-typed
/// output column with `variant_to_json`. Used for plan shapes that can't be
/// peeled into (Union/Aggregate/Join/Window/etc.) — the wrap is at the wire
/// only, so intermediate ops still operate on binary Variant.
///
/// Non-Variant columns pass through as bare `Expr::Column` so DataFusion's
/// schema accounting stays identical (same names, same qualifiers).
fn add_root_variant_projection(plan: LogicalPlan) -> Result<LogicalPlan> {
    let schema = plan.schema().clone();
    let variant_cols = schema.fields().iter().filter(|f| is_variant_type(f.data_type())).count();
    if variant_cols == 0 {
        return Ok(plan);
    }
    let variant_to_json = variant_to_json_udf();
    let exprs: Vec<Expr> = schema
        .iter()
        .map(|(qualifier, field)| {
            let col = Expr::Column(Column::new(qualifier.cloned(), field.name().clone()));
            if is_variant_type(field.data_type()) { wrap_with_variant_to_json(&col, &variant_to_json).alias(field.name()) } else { col }
        })
        .collect();
    debug!(target: "variant_select_rewriter", "added root Projection over un-peelable plan: wrapped {variant_cols} Variant column(s)");
    Ok(LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(plan))?))
}

fn wrap_projection(proj: Projection) -> Result<LogicalPlan> {
    let input_schema = proj.input.schema().clone();
    let wrapped = proj.expr.iter().filter(|e| is_variant_expr(e, &input_schema)).count();
    if wrapped == 0 {
        return Ok(LogicalPlan::Projection(proj));
    }
    let variant_to_json = variant_to_json_udf();
    let new_exprs: Vec<Expr> = proj
        .expr
        .iter()
        .map(|expr| if is_variant_expr(expr, &input_schema) { wrap_with_variant_to_json(expr, &variant_to_json) } else { expr.clone() })
        .collect();
    debug!(target: "variant_select_rewriter", "wrapped {wrapped} Variant exprs at root projection");
    Ok(LogicalPlan::Projection(Projection::try_new(new_exprs, proj.input.clone())?))
}

fn is_variant_expr(expr: &Expr, schema: &DFSchema) -> bool {
    // Idempotency guard: if the analyzer runs us twice, don't re-wrap an
    // already-wrapped call. Match by concrete UDF type (TypeId) rather than
    // by string name — renaming the UDF or registering another UDF with the
    // same name would otherwise silently break this check.
    !matches!(expr, Expr::ScalarFunction(sf) if sf.func.inner().downcast_ref::<VariantToJsonExtUdf>().is_some())
        && expr.get_type(schema).is_ok_and(|dt| is_variant_type(&dt))
}

fn wrap_with_variant_to_json(expr: &Expr, udf: &Arc<ScalarUDF>) -> Expr {
    let wrap = |inner: Expr| Expr::ScalarFunction(ScalarFunction { func: udf.clone(), args: vec![inner] });
    match expr {
        // Keep the alias outermost so the output column name is unchanged.
        Expr::Alias(a) => wrap(a.expr.as_ref().clone()).alias(a.name.clone()),
        other => wrap(other.clone()),
    }
}

/// Peephole: `json_as_text(variant_to_json(v), 'k')` → the Variant-native
/// extraction `json_to_pg_text(variant_to_json(variant_get(v, "['k']")))`.
///
/// `variant_to_json` serializes the WHOLE Variant to a JSON string per row and
/// `json_as_text` then re-parses that string to read one field. Measured on
/// prod `otel_metrics`: the extraction is 87-91% of the query (6h: 5.37s of
/// 5.93s wall on 11.85 MB scanned). Reading the leaf natively and serializing
/// only that is 4.2x on the same rows (24h: 26.4s → 6.26s), and at 3 days it
/// is the difference between ~19s and ~79s, i.e. over the 60s ceiling.
///
/// **Why an analyzer rule and not `VariantAwareExprPlanner`.** The planner sees
/// one binary op at a time, bottom-up, and here the left operand is
/// `variant_to_json(v)` — a Utf8 expression that datafusion-functions-json is
/// right to claim. Teaching the planner to unwrap it would compose correctly
/// *only* when the chain ends in `->>`: a TERMINAL `->` must keep returning the
/// JSON union it returns today (rewriting it to `variant_get` would change the
/// column's type and its pg wire OID), and at `plan_binary_op` time you cannot
/// tell a terminal `->` from one a later `->>` will consume. That is decidable
/// only with the whole expression tree in hand. It also catches the shape when
/// a client spells the functions out instead of using the operators.
///
/// Rewritten: an outer `json_as_text` over any chain of `json_get` calls whose
/// keys are all literals and whose base is `variant_to_json`. Deliberately NOT
/// rewritten:
/// - a terminal `json_get` (`->`): return type would go JSON-union → Variant.
/// - an intermediate `json_as_text`: it unquotes a JSON *string* leaf, so a
///   further lookup parses that string's contents; `variant_get` would not.
/// - `json_get_str`/`_int`/`_float`/`_bool`/`_json`: different NULL semantics.
/// - a non-literal key: the path must be a constant to become a variant path.
/// - a negative array index: JSON reads it as no-match (NULL), while
///   `VariantPath` fails to parse it — a NULL answer must not become an error.
#[derive(Debug, Default)]
pub struct VariantJsonAccessorPeephole;

impl AnalyzerRule for VariantJsonAccessorPeephole {
    fn name(&self) -> &str {
        "variant_json_accessor_peephole"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|plan| {
            plan.map_expressions(|expr| {
                expr.transform_down(|e| {
                    Ok(match variant_native_extraction(&e) {
                        // Alias to the original expression's schema name: this rule must not
                        // rename a projection output (that is the wire column name, and any
                        // outer reference resolves through it). Types are unchanged (Utf8 in,
                        // Utf8 out, no `tf.pg_type` tag on either), so no schema recompute.
                        Some(native) => Transformed::yes(native.alias(e.schema_name().to_string())),
                        None => Transformed::no(e),
                    })
                })
            })
        })
        .map(|t| t.data)
    }
}

/// The Variant-native equivalent of a `json_as_text` over `variant_to_json`,
/// or None when the shape is not one of the provably equivalent cases.
fn variant_native_extraction(expr: &Expr) -> Option<Expr> {
    use crate::read::functions::{PathComponent, build_variant_path, extract_path_component, json_to_pg_text_udf, variant_get_udf};

    fn peel(expr: &Expr) -> &Expr {
        match expr {
            Expr::Alias(alias) => peel(&alias.expr),
            other => other,
        }
    }
    // The JSON planner alias-wraps every node it plans, so peel at each step.
    fn call<'a>(expr: &'a Expr, name: &str) -> Option<&'a ScalarFunction> {
        match peel(expr) {
            Expr::ScalarFunction(sf) if sf.func.name() == name => Some(sf),
            _ => None,
        }
    }

    let (mut node, mut path) = {
        let sf = call(expr, "json_as_text")?;
        let (json, keys) = sf.args.split_first()?;
        (json, keys.iter().map(extract_path_component).collect::<Option<Vec<_>>>()?)
    };
    // Walk down the `->` chain, prepending each hop's keys. A json_get over a
    // json_get takes only the object/array arm of the inner union, which is
    // exactly a nested variant path: a non-container intermediate is NULL both ways.
    let variant = loop {
        if let Some(sf) = call(node, "variant_to_json") {
            let [variant] = sf.args.as_slice() else { return None };
            break variant;
        }
        let sf = call(node, "json_get")?;
        let (inner, keys) = sf.args.split_first()?;
        let mut head = keys.iter().map(extract_path_component).collect::<Option<Vec<_>>>()?;
        head.extend(path);
        (node, path) = (inner, head);
    };
    if path.iter().any(|p| matches!(p, PathComponent::Index(i) if *i < 0)) {
        return None;
    }

    let scalar = |func: Arc<ScalarUDF>, args: Vec<Expr>| Expr::ScalarFunction(ScalarFunction { func, args });
    let leaf = scalar(variant_get_udf(), vec![variant.clone(), Expr::Literal(ScalarValue::Utf8(Some(build_variant_path(&path))), None)]);
    // `variant_get` cannot stringify numeric/boolean leaves, so reuse the exact
    // composition `VariantAwareExprPlanner` already emits for `->>`.
    Some(scalar(json_to_pg_text_udf(), vec![scalar(variant_to_json_udf(), vec![leaf])]))
}

#[cfg(test)]
mod variant_json_accessor_tests {
    //! Equivalence, not assertion: every case runs the SAME SQL twice — once
    //! through a session that has the peephole and once through one that does
    //! not — and demands identical rows AND an identical schema. A case is
    //! also told whether it is supposed to be rewritten, so an accidentally
    //! narrowed (or widened) match fails here rather than in prod.
    use datafusion::prelude::SessionContext;

    use super::*;

    async fn run(sql: &str, with_rule: bool) -> (String, Vec<datafusion::arrow::record_batch::RecordBatch>) {
        let mut ctx = SessionContext::new();
        crate::read::functions::register_custom_functions(&mut ctx).expect("custom functions");
        datafusion_functions_json::register_all(&mut ctx).expect("json functions");
        if with_rule {
            ctx.add_analyzer_rule(Arc::new(VariantJsonAccessorPeephole));
        }
        let plan = ctx.sql(sql).await.expect("plan").into_optimized_plan().expect("optimize");
        let batches = SessionContext::from(ctx.state()).execute_logical_plan(plan.clone()).await.unwrap().collect().await.expect("execute");
        (plan.display_indent().to_string(), batches)
    }

    /// `doc` is the JSON the Variant holds; `accessor` is appended to
    /// `variant_to_json(json_to_variant(d))`. Every case also carries a NULL
    /// input row, and runs over a column (not a folded scalar) so the array
    /// kernels are the ones compared.
    #[test_case::test_case(r#"{"a":"x"}"#, "->>'a'", true ; "string leaf")]
    #[test_case::test_case(r#"{"a":42}"#, "->>'a'", true ; "numeric leaf keeps its text form")]
    #[test_case::test_case(r#"{"a":1.5}"#, "->>'a'", true ; "float leaf")]
    #[test_case::test_case(r#"{"a":true}"#, "->>'a'", true ; "boolean leaf")]
    #[test_case::test_case(r#"{"a":null}"#, "->>'a'", true ; "an explicit JSON null is SQL NULL")]
    #[test_case::test_case(r#"{"b":1}"#, "->>'a'", true ; "missing key")]
    #[test_case::test_case(r#"{"a":{"b":1,"c":[2]}}"#, "->>'a'", true ; "object leaf is returned as JSON text")]
    #[test_case::test_case(r#"{"a":[1,"two"]}"#, "->>'a'", true ; "array leaf is returned as JSON text")]
    #[test_case::test_case(r#"{"a":{"b":"c"}}"#, "->'a'->>'b'", true ; "chained through a json_get")]
    #[test_case::test_case(r#"{"a":{"b":{"c":"d"}}}"#, "->'a'->'b'->>'c'", true ; "chained twice")]
    #[test_case::test_case(r#"{"http.method":"GET"}"#, "->>'http.method'", true ; "a dotted OTel key is ONE key")]
    #[test_case::test_case(r#"{"a":["x","y"]}"#, "->'a'->>1", true ; "array index")]
    #[test_case::test_case(r#"{"a":"x"}"#, "->'a'->>'b'", true ; "descending into a string leaf is NULL both ways")]
    #[test_case::test_case(r#"{"a":"{\"b\":1}"}"#, "->'a'->>'b'", true ; "a string leaf holding JSON is NOT re-parsed")]
    #[test_case::test_case(r#"{"a":1}"#, "->>''", true ; "the empty key is a key, not an empty path")]
    #[test_case::test_case(r#"{"a":{"b":1}}"#, "->'a'", false ; "a terminal arrow keeps its JSON union type")]
    #[test_case::test_case(r#"{"a":{"b":"c"}}"#, "->>'a'->>'b'", false ; "an intermediate ->> unquotes, so it is not a variant path")]
    #[tokio::test]
    async fn both_spellings_agree(doc: &str, accessor: &str, rewritten: bool) {
        let sql = format!("SELECT variant_to_json(json_to_variant(d)){accessor} AS v FROM (VALUES ('{doc}'), (NULL)) t(d)");
        let (plan, batches) = run(&sql, true).await;
        let (base_plan, base) = run(&sql, false).await;

        assert_eq!(plan.contains("variant_get"), rewritten, "peephole fired unexpectedly\nwith: {plan}\nwithout: {base_plan}");
        assert!(!base_plan.contains("variant_get"), "control plan must keep the serialized form: {base_plan}");
        assert_eq!(batches.iter().map(|b| b.schema()).collect::<Vec<_>>(), base.iter().map(|b| b.schema()).collect::<Vec<_>>(), "wire schema changed");
        let fmt = |b: &[datafusion::arrow::record_batch::RecordBatch]| datafusion::arrow::util::pretty::pretty_format_batches(b).unwrap().to_string();
        assert_eq!(fmt(&batches), fmt(&base), "rows differ\nwith: {plan}");
    }

    /// A negative index is a no-match (NULL) in JSON but a hard parse error in
    /// `VariantPath`, so the peephole must leave it alone rather than turn a
    /// NULL answer into a failed query.
    #[test]
    fn a_negative_index_is_not_rewritten() {
        use datafusion::prelude::lit;
        let variant = Expr::ScalarFunction(ScalarFunction { func: variant_to_json_udf(), args: vec![col("v")] });
        let rewrite = |key: Expr| {
            super::variant_native_extraction(&Expr::ScalarFunction(ScalarFunction {
                func: datafusion_functions_json::udfs::json_as_text_udf(),
                args: vec![variant.clone(), key],
            }))
        };
        assert!(rewrite(lit(1i64)).is_some(), "a non-negative index is rewritten");
        assert!(rewrite(lit(-1i64)).is_none(), "a negative index must stay on the JSON path");
        assert!(rewrite(col("k")).is_none(), "a non-literal key has no constant variant path");
    }
}

#[cfg(test)]
mod peel_tests {
    //! Unit tests for `wrap_root_projection` peel logic. These exercise the
    //! Sort / Limit / Distinct / SubqueryAlias / Filter branches and the
    //! MAX_PEEL guard without standing up a server.
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::DFSchema,
        logical_expr::{EmptyRelation, builder::LogicalPlanBuilder, col, lit},
    };

    use super::*;

    fn variant_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Struct(vec![Arc::new(Field::new("metadata", DataType::Binary, false)), Arc::new(Field::new("value", DataType::Binary, false))].into()),
            true,
        )
        .with_metadata(HashMap::from([(crate::schema::VARIANT_EXT_KEY.to_string(), crate::schema::VARIANT_EXT_VALUE.to_string())]))
    }

    fn variant_projection() -> LogicalPlan {
        let schema = Schema::new(vec![variant_field("v")]);
        let df = Arc::new(DFSchema::try_from(schema).unwrap());
        let empty = LogicalPlan::EmptyRelation(EmptyRelation { produce_one_row: false, schema: df });
        LogicalPlanBuilder::from(empty).project(vec![col("v")]).unwrap().build().unwrap()
    }

    fn analyze(plan: LogicalPlan) -> LogicalPlan {
        let cfg = ConfigOptions::default();
        VariantSelectRewriter.analyze(plan, &cfg).unwrap()
    }

    fn is_variant_to_json_call(expr: &Expr) -> bool {
        let inner = match expr {
            Expr::Alias(a) => a.expr.as_ref(),
            other => other,
        };
        matches!(inner, Expr::ScalarFunction(sf) if sf.func.inner().downcast_ref::<VariantToJsonExtUdf>().is_some())
    }

    fn first_projection_expr(plan: &LogicalPlan) -> &Expr {
        fn find(p: &LogicalPlan) -> Option<&Expr> {
            if let LogicalPlan::Projection(proj) = p {
                return proj.expr.first();
            }
            p.inputs().into_iter().find_map(find)
        }
        find(plan).expect("expected a Projection in the plan")
    }

    #[test]
    fn wraps_bare_projection() {
        let out = analyze(variant_projection());
        assert!(is_variant_to_json_call(first_projection_expr(&out)));
    }

    #[test]
    fn peels_sort_limit_distinct_alias_filter() {
        let plan = LogicalPlanBuilder::from(variant_projection())
            .filter(lit(true))
            .unwrap()
            .distinct()
            .unwrap()
            .limit(0, Some(10))
            .unwrap()
            .sort(vec![col("v").sort(true, false)])
            .unwrap()
            .alias("a")
            .unwrap()
            .build()
            .unwrap();
        let out = analyze(plan);
        assert!(is_variant_to_json_call(first_projection_expr(&out)));
    }

    #[test]
    fn idempotent_on_double_analyze() {
        // Running the analyzer twice must not double-wrap; the inner-UDF guard
        // in `is_variant_expr` (matched by TypeId, not name) ensures the second
        // pass leaves the already-wrapped projection alone.
        let once = analyze(variant_projection());
        let twice = analyze(once.clone());
        let expr_twice = first_projection_expr(&twice);
        assert!(is_variant_to_json_call(expr_twice));
        let Expr::ScalarFunction(sf) = expr_twice else {
            panic!("not a scalar function");
        };
        // Args length stays at 1 (the bare column) — no nested variant_to_json call.
        assert_eq!(sf.args.len(), 1);
        assert!(matches!(sf.args[0], Expr::Column(_)), "second pass nested the call: {:?}", sf.args[0]);
    }

    #[test]
    fn max_peel_short_circuits_on_pathological_depth() {
        // > MAX_PEEL nested SubqueryAlias should make peel() bail rather than
        // recurse forever. DataFusion's own transform_up walk over a 300-deep
        // plan blows the default 2 MiB test stack, so we run the whole thing
        // on a larger thread — that itself is the assertion that peel()'s
        // depth guard is doing useful work alongside transform_up's recursion.
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let plan = (0..300).fold(variant_projection(), |p, i| LogicalPlanBuilder::from(p).alias(format!("a{i}")).unwrap().build().unwrap());
                let out = analyze(plan);
                assert!(!is_variant_to_json_call(first_projection_expr(&out)));
            })
            .unwrap()
            .join()
            .unwrap();
    }
}

// Transparent Tantivy acceleration for standard SQL predicates.
//
// Rewrites `col LIKE 'pattern'` and `col ILIKE 'pattern'` on tantivy-indexed
// columns by **additively** AND-ing a `text_match(col, q)` call to the
// predicate. Exact `col = 'lit'` on **raw-tokenized** columns is also routed
// (gated by `tantivy.route_equality`, default on) — this is the lever for
// high-cardinality id lookups (trace_id/span_id/id/parent_id) that bloom/stats
// can't prune when row groups are coarse. `IN`-lists on raw columns route as
// an OR of per-item `text_match` calls (same gates as `=`, capped at
// `MAX_ROUTED_IN_LIST`). Correctness under `OR` is enforced by
// `collect_text_match_tree`: an OR node is only routable when every branch
// is completely covered by a `text_match`, and the original predicate is
// always preserved as the post-filter backstop. `!=` / `NOT IN` are never
// routed — negation has no term form. The original comparison is never
// removed — it still applies as a post-filter on MemBuffer rows and Delta
// files whose tantivy index hasn't built yet (post-flush lag). The
// `text_match` call, once picked up by the existing routing logic in
// `ProjectRoutingTable`, produces an `id IN (...)` prefilter that narrows
// the Delta scan.
//
// Correctness invariants:
// 1. The original predicate is preserved verbatim in the plan.
// 2. Only rewrite predicates on columns confirmed `tantivy.indexed: true`.
// 3. Idempotent under repeated passes.
// 4. Patterns the *target column's tokenizer* can't accelerate are left
//    alone (correctness preserved via the original predicate).
//
// Patterns by tokenizer:
//
// | SQL form              | raw   | default | ngram3 |
// |-----------------------|-------|---------|--------|
// | `col = 'lit'`         | ✅ term (route_equality) | ❌ (bloom/stats) | ❌ (bloom/stats) |
// | `col LIKE 'lit'`      | ✅    | ✅      | ✅      |
// | `col LIKE 'pre%'`     | ✅ prefix | ✅ prefix | ✅ prefix |
// | `col LIKE '%suf'`     | ❌    | ❌      | ✅ via ngram |
// | `col LIKE '%mid%'`    | ❌    | ❌      | ✅ via ngram |
// | `col ILIKE 'lit'`     | ❌    | ✅ (lowercased literal) | ✅ |
// | `col ILIKE '%mid%'`   | ❌    | ❌      | ✅ |
// | `col::text ~* 'sub'`  | ❌    | ❌      | ✅ via ngram |
// | `col::text ~ 'sub'`   | ❌    | ❌      | ✅ via ngram |
//
// `~`/`~*` route ONLY when the pattern is a plain literal substring
// (`regex_literal_substring`) and the column is text-typed — this is the
// shape monoscope renders KQL `has`/`contains` into (`subject::text ~*
// escapeRegex(term)`). Anchored (`^`/`$`, i.e. startswith/endswith) and any
// other regex feature is left alone. Variant/List columns are excluded: the
// index holds our canonical rendering, not SQL's `::text` cast output.
//
// `_` (single-char wildcard) is never accelerated — semantics don't map
// cleanly to any tantivy primitive. Strings shorter than 3 chars on
// ngram3 columns fall through (no full trigram available).

use std::sync::OnceLock;

use datafusion::{
    common::tree_node::TreeNodeRecursion,
    logical_expr::{and, lit, or},
};

use crate::tantivy::{
    DEFAULT_TOKENIZER, NGRAM3_TOKENIZER, RAW_TOKENIZER,
    udf::{NGRAM_MIN_QUERY_LEN, TEXT_MATCH_NAME, TextMatchUdf, classify_like_pattern, is_eq_term_safe, regex_literal_substring},
};

/// Per-column index facts the rewriter needs: the resolved tokenizer, and
/// whether the *stored* column is a plain string. The latter gates regex
/// routing: on a Variant/List column the tantivy index holds our own
/// canonical text rendering (`builder::variant_to_text`), which need not be
/// byte-identical to what a SQL `col::text` cast produces — routing there
/// could drop rows, so only text-typed columns are eligible.
type IndexedCol = (&'static str, bool);
type IndexedCols = HashMap<String, IndexedCol>;

#[derive(Debug, Default)]
pub struct TantivyPredicateRewriter {
    /// Route exact `=` on raw columns through tantivy (`tantivy.route_equality`).
    /// Carried as a field rather than read from the global config singleton so
    /// the rule works under tests that build a `Database` from a local config
    /// (the singleton may be uninitialized → `config()` panics).
    route_equality: bool,
}

impl TantivyPredicateRewriter {
    pub fn new(route_equality: bool) -> Self {
        Self { route_equality }
    }
}

impl AnalyzerRule for TantivyPredicateRewriter {
    fn name(&self) -> &str {
        "tantivy_predicate_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Dml(_) => Ok(plan),
            plan => Ok(plan.transform_down(|p| rewrite_node(p, self.route_equality))?.data),
        }
    }
}

fn rewrite_node(plan: LogicalPlan, allow_eq: bool) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Filter(mut filter) => match scanned_indexed_columns(&filter.input) {
            None => Ok(Transformed::no(LogicalPlan::Filter(filter))),
            Some(columns) => {
                let rewritten = std::mem::replace(&mut filter.predicate, lit(true)).transform_down(|e| rewrite_expr(e, columns, allow_eq))?;
                filter.predicate = rewritten.data;
                // The expr traversal's `tnr` (possibly `Jump`) must not leak into the plan walk.
                Ok(Transformed::new(LogicalPlan::Filter(filter), rewritten.transformed, TreeNodeRecursion::Continue))
            }
        },
        _ => Ok(Transformed::no(plan)),
    }
}

/// Longest IN-list we'll expand into an OR of `text_match` calls. Beyond
/// this the per-item query cost outweighs the pruning (and the selectivity
/// cutoff would likely reject the hit set anyway).
const MAX_ROUTED_IN_LIST: usize = 100;

fn rewrite_expr(expr: Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Result<Transformed<Expr>> {
    // Skip the children of a text_match call (already a tantivy predicate).
    if matches!(&expr, Expr::ScalarFunction(sf) if sf.func.name() == TEXT_MATCH_NAME) {
        return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
    }
    let tantivy = match_indexed_predicate(&expr, indexed_columns, allow_eq).map(|(column, route)| route.into_call(column)).or_else(|| {
        match_indexed_in_list(&expr, indexed_columns, allow_eq).and_then(|(column, items)| items.into_iter().map(|r| r.into_call(column.clone())).reduce(or))
    });
    Ok(match tantivy {
        Some(tm) => Transformed::new(and(expr, tm), true, TreeNodeRecursion::Jump),
        None => Transformed::no(expr),
    })
}

/// `col IN ('a','b',...)` on a RAW-tokenized column → the per-item term
/// queries, under the same gates as exact `=` routing (raw tokenizer,
/// eq-term-safe literals, `route_equality` flag). Placeholder items defer to
/// scan-time classification. `NOT IN` is never routed.
fn match_indexed_in_list(expr: &Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Option<(String, Vec<Route>)> {
    let Expr::InList(InList { expr: col, list, negated: false }) = expr else {
        return None;
    };
    if !allow_eq || list.is_empty() || list.len() > MAX_ROUTED_IN_LIST {
        return None;
    }
    let Expr::Column(c) = col.as_ref() else { return None };
    if indexed_columns.get(&c.name)?.0 != RAW_TOKENIZER {
        return None;
    }
    list.iter().map(eq_term_route).collect::<Option<Vec<_>>>().map(|items| (c.name.clone(), items))
}

/// RHS of a raw-column equality (`=` or one `IN` item) as a route: literals are
/// classified now (bailing on empty / QueryParser-unsafe values, where the
/// preserved `=` still applies), placeholders defer to scan time.
fn eq_term_route(rhs: &Expr) -> Option<Route> {
    match rhs {
        Expr::Literal(s, _) => extract_utf8_string(s).filter(|v| !v.is_empty() && v.chars().all(is_eq_term_safe)).map(Route::Ready),
        // Prepared-statement path: value unknown until Bind. Route with a
        // deferred tag so plans cached with placeholders keep the prefilter.
        Expr::Placeholder(_) => Some(Route::Deferred { rhs: rhs.clone(), kind: "eq".into() }),
        _ => None,
    }
}

/// How a routed predicate reaches tantivy.
#[derive(Debug, PartialEq)]
enum Route {
    /// Literal classified at plan time → `text_match(col, query)`.
    Ready(String),
    /// `$N` placeholder — can't classify until parameter substitution.
    /// Emitted as `text_match(col, $N, kind)`; the scan-side collector runs
    /// `classify_deferred(kind, value)` once the literal is known and treats
    /// unclassifiable values as opaque (original predicate post-filters).
    Deferred { rhs: Expr, kind: String },
}

impl Route {
    /// `text_match(col, query)` — or `text_match(col, $N, kind)` when deferred.
    fn into_call(self, column: String) -> Expr {
        // Cached Arc — analyzer rules run on every query.
        static CELL: OnceLock<Arc<ScalarUDF>> = OnceLock::new();
        let col = Expr::Column(Column::new_unqualified(column));
        let args = match self {
            Route::Ready(query) => vec![col, lit(query)],
            Route::Deferred { rhs, kind } => vec![col, rhs, lit(kind)],
        };
        Expr::ScalarFunction(ScalarFunction { func: CELL.get_or_init(|| Arc::new(ScalarUDF::from(TextMatchUdf::default()))).clone(), args })
    }
}

/// If `expr` is a rewritable predicate on an indexed column, return
/// `(column_name, route)`. Decision depends on the column's
/// tokenizer — raw can't do substring; ngram3 can do everything; default
/// is in between.
fn match_indexed_predicate(expr: &Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Option<(String, Route)> {
    match expr {
        // Exact `col = 'lit'` on a RAW-tokenized column: route as a term query.
        // Raw is a single case-sensitive token, so the tantivy match set equals
        // the `=` match set (the id-prefilter is exact, not just a superset).
        // Safe under OR — `collect_text_match_tree` only routes an OR when
        // every branch is completely covered; otherwise the subtree is opaque
        // and the preserved `=` post-filters. `!=` is never routed (no term
        // form). Gated by `route_equality` for instant rollback.
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) if allow_eq => {
            let (c, rhs) = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(c), other) | (other, Expr::Column(c)) => (c, other),
                _ => return None,
            };
            if indexed_columns.get(&c.name)?.0 != RAW_TOKENIZER {
                return None; // only exact-match (raw) columns; ngram3/default are lossy for `=`
            }
            Some((c.name.clone(), eq_term_route(rhs)?))
        }
        Expr::Like(Like { negated: false, expr: l, pattern: r, escape_char, case_insensitive }) => {
            let Expr::Column(c) = l.as_ref() else { return None };
            let tok = indexed_columns.get(&c.name)?.0;
            // ILIKE on raw (case-sensitive single token) is not accelerable
            // without a parallel case-insensitive index — skip.
            if *case_insensitive && tok == RAW_TOKENIZER {
                return None;
            }
            // Both tokenizers that reach here lowercase on index and query side,
            // so ILIKE needs no extra work — case sensitivity is already lost in
            // the prefilter and the preserved LIKE/ILIKE re-runs with correct
            // semantics on the Delta side.
            let route = match r.as_ref() {
                Expr::Literal(s, _) => classify_like_pattern(&extract_utf8_string(s)?, *escape_char, tok == NGRAM3_TOKENIZER)
                    // ngram3 needs a full trigram to match anything.
                    .filter(|q| tok != NGRAM3_TOKENIZER || q.chars().filter(|c| *c != '*').count() >= NGRAM_MIN_QUERY_LEN)
                    .map(Route::Ready)?,
                // Pattern arrives at Bind: defer classification. Custom escape
                // chars aren't carried in the tag — don't route them.
                Expr::Placeholder(_) if escape_char.is_none() => {
                    Route::Deferred { rhs: r.as_ref().clone(), kind: format!("{}:{tok}", if *case_insensitive { "ilike" } else { "like" }) }
                }
                _ => return None,
            };
            Some((c.name.clone(), route))
        }
        // `col ~* 'substr'` / `col ~ 'substr'`, optionally through a
        // `CAST(col AS Utf8)` — the shape monoscope renders every KQL
        // has/contains into (`subject::text ~* escapeRegex(term)`). Routed as
        // the ngram3 substring query, i.e. identical to `col ILIKE '%substr%'`.
        //
        // Superset argument (what the `id IN (hits)` prefilter requires):
        // ngram3 lowercases + ASCII-folds both index and query side, so its
        // hit set is the case- and diacritic-insensitive substring match —
        // a superset of both `~` (case-sensitive) and `~*`. The original
        // regex is preserved as the post-filter, so results are unchanged.
        // Only PLAIN substrings route (see `regex_literal_substring`);
        // anchors and any other regex feature fall through untouched.
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::RegexMatch | Operator::RegexIMatch, right }) => {
            let c = column_through_string_cast(left)?;
            let (tok, text_typed) = *indexed_columns.get(&c.name)?;
            let Expr::Literal(s, _) = right.as_ref() else { return None };
            (tok == NGRAM3_TOKENIZER && text_typed)
                .then(|| regex_literal_substring(&extract_utf8_string(s)?).filter(|q| q.chars().count() >= NGRAM_MIN_QUERY_LEN))
                .flatten()
                .map(|q| (c.name.clone(), Route::Ready(q)))
        }
        _ => None,
    }
}

/// The column under zero or more string casts. Monoscope emits `col::text`,
/// which DataFusion may keep as a `Cast`/`TryCast` when the column is already
/// Utf8-ish; the cast is value-preserving for string types, so seeing through
/// it is safe (non-string sources are rejected by the `text_typed` gate).
fn column_through_string_cast(e: &Expr) -> Option<&Column> {
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::expr::{Cast, TryCast};
    match e {
        Expr::Column(c) => Some(c),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field })
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) =>
        {
            column_through_string_cast(expr)
        }
        _ => None,
    }
}

/// Indexed columns of the first TableScan below `plan` that has a tantivy
/// index. Stops at the first one (predicates above only see one scan in
/// practice; cross-table joins on indexed columns aren't supported in v1
/// — each filter is rewritten relative to its own subtree's scan).
fn scanned_indexed_columns(plan: &LogicalPlan) -> Option<&'static IndexedCols> {
    // `mut` forced: TreeNode::apply's visitor is FnMut(&Self) -> Result<TreeNodeRecursion>,
    // with no fold/accumulator variant able to return the found value directly.
    let mut found = None;
    let _ = plan.apply(|p| {
        Ok(match p {
            LogicalPlan::TableScan(ts) => match indexed_columns_for(ts.table_name.table()) {
                Some(cols) => {
                    found = Some(cols);
                    TreeNodeRecursion::Stop
                }
                None => TreeNodeRecursion::Continue,
            },
            _ => TreeNodeRecursion::Continue,
        })
    });
    found
}

/// Indexed columns for a table from the static schema registry — keyed by
/// column name, value is the resolved tokenizer (raw/default/ngram3).
/// Returns `None` when the table isn't in the registry.
///
/// The cache is populated *once* on first call. This is safe because
/// `schema_loader::registry()` is compiled-in YAML and immutable. If we ever
/// add runtime/hot-reload of schemas, this OnceLock must be replaced with an
/// invalidatable structure — newly-added Tantivy-indexed tables would
/// otherwise silently never accelerate.
fn indexed_columns_for(table: &str) -> Option<&'static IndexedCols> {
    static CACHE: OnceLock<HashMap<String, IndexedCols>> = OnceLock::new();
    CACHE
        .get_or_init(|| {
            let registry = crate::schema::registry();
            registry
                .list_tables()
                .into_iter()
                .filter_map(|name| {
                    let cols: IndexedCols = registry
                        .get(&name)?
                        .fields
                        .iter()
                        .filter_map(|f| {
                            let cfg = f.tantivy.as_ref().filter(|c| c.indexed)?;
                            let tok = match cfg.tokenizer.as_deref().unwrap_or(NGRAM3_TOKENIZER) {
                                RAW_TOKENIZER => RAW_TOKENIZER,
                                DEFAULT_TOKENIZER => DEFAULT_TOKENIZER,
                                _ => NGRAM3_TOKENIZER,
                            };
                            Some((f.name.clone(), (tok, matches!(f.data_type.as_str(), "Utf8" | "LargeUtf8" | "Utf8View"))))
                        })
                        .collect();
                    (!cols.is_empty()).then_some((name, cols))
                })
                .collect()
        })
        .get(table)
}

#[cfg(test)]
mod tantivy_rewriter_tests {
    use super::*;
    use crate::tantivy::udf::{PredNode, collect_text_match_tree};

    /// `(pattern, escape, allow_substring)` → routed query, over every
    /// tokenizer-visible shape. `_` and embedded `%` never accelerate;
    /// leading `%` only on ngram3 (`allow_substring`).
    #[test]
    fn like_classifier_cases() {
        for (pat, esc, substring, want) in [
            ("foo", None, false, Some("foo")),
            ("foo%", None, false, Some("foo*")),
            ("%foo", None, false, None),
            ("%foo", None, true, Some("foo")),
            ("%foo%", None, true, Some("foo")),
            ("%foo%", None, false, None),
            ("fo%o", None, true, None),
            ("fo%o", None, false, None),
            ("fo_", None, true, None),
            ("foo+bar", None, true, None),
            ("svc.user-api", None, false, Some("svc.user-api")),
            ("foo\\%", Some('\\'), false, None), // escaped metachar: bail conservatively
        ] {
            assert_eq!(classify_like_pattern(pat, esc, substring), want.map(str::to_string), "{pat:?} escape={esc:?} substring={substring}");
        }
    }

    /// Test column map; every entry is text-typed unless a test overrides it.
    fn cols_of<const N: usize>(items: [(&str, &'static str); N]) -> IndexedCols {
        items.into_iter().map(|(k, tok)| (k.to_string(), (tok, true))).collect()
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    fn cmp(c: &str, op: Operator, val: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(col(c)), op, Box::new(lit(val))))
    }

    fn eq(c: &str, val: &str) -> Expr {
        cmp(c, Operator::Eq, val)
    }

    /// Rewrite `e`, then collect the prefilter tree the scan side would see.
    fn routed_tree(e: Expr, cols: &IndexedCols, allow_eq: bool) -> Option<PredNode> {
        collect_text_match_tree(&[e.transform_down(|x| rewrite_expr(x, cols, allow_eq)).unwrap().data])
    }

    /// P0: exact `=` on a raw column is the high-cardinality trace/span lookup
    /// acceleration. Every other shape (ngram3, `!=`, flag off, literals the
    /// QueryParser would mis-handle against a single raw token) falls back to
    /// the plain `=` — correctness over acceleration.
    #[test]
    fn match_eq_routes_raw_columns_only_when_enabled() {
        let cols = cols_of([("tid", RAW_TOKENIZER), ("name", NGRAM3_TOKENIZER)]);
        let uid = "0fee13b9-ac71-5c55-acd1-109542595054";
        for (expr, allow_eq, want, why) in [
            (eq("tid", "d01762b88f4ed54d"), true, Some("d01762b88f4ed54d"), "raw column + flag on routes as a term"),
            (eq("tid", uid), true, Some(uid), "dashed uuid: the `-` survives (e2e-proven)"),
            (eq("tid", "abc123"), false, None, "flag off reverts to bloom/stats"),
            (eq("name", "runServer"), true, None, "ngram3 is lossy for equality"),
            (cmp("tid", Operator::NotEq, "abc"), true, None, "`!=` has no term form"),
            (eq("tid", "a:b"), true, None, "colon is query syntax"),
            (eq("tid", "foo bar"), true, None, "space → AND-split can't match one raw token"),
            (eq("tid", "a.b"), true, None, "dot conservatively excluded"),
            (eq("tid", ""), true, None, "empty"),
        ] {
            let want = want.map(|q| ("tid".to_string(), Route::Ready(q.to_string())));
            assert_eq!(match_indexed_predicate(&expr, &cols, allow_eq), want, "{why}");
        }
    }

    #[test]
    fn or_of_routed_eqs_becomes_or_node_but_partial_or_is_opaque() {
        // End-to-end OR-safety with the tree collector: a disjunction where
        // BOTH branches are rewritten routes as an Or node (union — new
        // capability); a disjunction with one unroutable branch must yield NO
        // prefilter at all (else the 2026-06-16 empty/partial-union bug
        // returns). A top-level conjunct still routes.
        let cols = cols_of([("tid", RAW_TOKENIZER), ("sid", RAW_TOKENIZER)]);

        let tree = routed_tree(or(eq("tid", "x"), eq("sid", "y")), &cols, true);
        assert!(matches!(&tree, Some(PredNode::Or(kids)) if kids.len() == 2), "both routed branches must union, got {tree:?}");

        // One branch on an UN-indexed column → whole OR must be opaque.
        let cols_partial = cols_of([("tid", RAW_TOKENIZER)]);
        let partial = routed_tree(or(eq("tid", "x"), eq("unindexed", "y")), &cols_partial, true);
        assert_eq!(partial, None, "an OR with an unroutable branch must not seed the prefilter");

        let tree = routed_tree(and(eq("tid", "x"), lit(true)), &cols, true);
        assert!(matches!(&tree, Some(PredNode::Leaf(p)) if p.column == "tid" && p.query == "x"), "expected a tid/x leaf, got {tree:?}");
    }

    #[test]
    fn in_list_routes_as_or_of_terms() {
        let cols = cols_of([("tid", RAW_TOKENIZER), ("name", NGRAM3_TOKENIZER)]);
        let in_list =
            |c: &str, items: &[&str], negated: bool| Expr::InList(InList { expr: Box::new(col(c)), list: items.iter().map(|s| lit(*s)).collect(), negated });
        // Routable IN-list → collector sees an Or of leaves (complete via the
        // AND with the preserved original).
        let tree = routed_tree(in_list("tid", &["a", "b"], false), &cols, true);
        assert!(matches!(&tree, Some(PredNode::Or(kids)) if kids.len() == 2), "expected an Or of 2 leaves, got {tree:?}");
        // NOT IN, ngram3 column, unsafe literal, flag off → never routed.
        for (e, allow) in [
            (in_list("tid", &["a"], true), true),
            (in_list("name", &["abc"], false), true),
            (in_list("tid", &["a:b"], false), true),
            (in_list("tid", &["a"], false), false),
        ] {
            assert_eq!(routed_tree(e, &cols, allow), None);
        }
    }

    #[test]
    fn match_ilike_routes_on_ngram3_but_never_on_raw() {
        // ILIKE on a raw-tokenized (case-sensitive) column would silently miss
        // case variants; ngram3 lowercases both sides, so substrings route.
        let cols = cols_of([("raw", RAW_TOKENIZER), ("c", NGRAM3_TOKENIZER)]);
        let ilike = |c: &str, pat: &str| {
            Expr::Like(Like { negated: false, expr: Box::new(col(c)), pattern: Box::new(lit(pat)), escape_char: None, case_insensitive: true })
        };
        assert_eq!(match_indexed_predicate(&ilike("raw", "foo"), &cols, true), None);
        assert_eq!(match_indexed_predicate(&ilike("c", "%foo%"), &cols, true), Some(("c".into(), Route::Ready("foo".into()))));
    }

    /// `col::text ~* 'lit'` — the shape monoscope renders every KQL
    /// has/contains into. Before this it never matched, so the ngram3 index
    /// was built on ingest and never read.
    #[test]
    fn match_regex_imatch_through_cast_routes_on_ngram3() {
        use arrow::datatypes::DataType;
        use datafusion::logical_expr::expr::Cast;
        let cols = cols_of([("name", NGRAM3_TOKENIZER), ("tid", RAW_TOKENIZER)]);
        let re = |c: &str, op: Operator, pat: &str, cast: bool| {
            let lhs = if cast { Expr::Cast(Cast::new(Box::new(col(c)), DataType::Utf8)) } else { col(c) };
            Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), op, Box::new(lit(pat))))
        };

        for op in [Operator::RegexIMatch, Operator::RegexMatch] {
            assert_eq!(
                match_indexed_predicate(&re("name", op, "runServer", true), &cols, true),
                Some(("name".into(), Route::Ready("runServer".into()))),
                "cast-wrapped {op:?} on an ngram3 column must route"
            );
            // Bare column (DataFusion may fold the no-op cast) routes too.
            assert_eq!(match_indexed_predicate(&re("name", op, "runServer", false), &cols, true), Some(("name".into(), Route::Ready("runServer".into()))));
        }
        // `escapeRegex` output: `.` arrives as `\.` and decodes to a literal.
        assert_eq!(
            match_indexed_predicate(&re("name", Operator::RegexIMatch, "svc\\.user-api", true), &cols, true),
            Some(("name".into(), Route::Ready("svc.user-api".into())))
        );
        // Unescaped metachars / anchors (startswith & endswith) / short
        // patterns / raw-tokenized columns / non-literal RHS: never routed.
        for (c, pat) in [
            ("name", "run.*"),
            ("name", "a|b"),
            ("name", "^foo"),
            ("name", "foo$"),
            ("name", "fo(o)"),
            ("name", "\\yword\\y"), // \y is a word boundary, not an escaped literal
            ("name", "ab"),         // < NGRAM_MIN_QUERY_LEN
            ("name", ""),
            ("name", "a\\"), // trailing backslash
            ("tid", "abcdef"),
        ] {
            assert_eq!(match_indexed_predicate(&re(c, Operator::RegexIMatch, pat, true), &cols, true), None, "{c} ~* {pat:?} must not route");
        }
        // Variant/List column (not text-typed): the index holds our canonical
        // rendering, which `::text` need not reproduce — never routed.
        let variant_cols: IndexedCols = HashMap::from([("body".to_string(), (NGRAM3_TOKENIZER, false))]);
        assert_eq!(match_indexed_predicate(&re("body", Operator::RegexIMatch, "boom", true), &variant_cols, true), None);
    }
}

// Rewrite Postgres array literals (`'{}'`, `'{a,b}'`) into typed list
// literals where an array type is expected.
//
// Postgres resolves `COALESCE(hashes, '{}')` by treating the untyped string
// literal as an array literal of the other argument's type. DataFusion's
// coercion can't unify `List(Utf8View)` with `Utf8`, so the same expression
// failed at planning — which broke monoscope's `lookupOtelRecord`
// random-access query (`COALESCE(hashes,'{}')`, `COALESCE(summary,'{}')`).
//
// This rule runs before `TypeCoercion`. For any `coalesce` call where at
// least one argument types to a list and another is a `'{...}'` string
// literal, the literal is re-parsed as a PG array literal of the list's
// element type. Only string element types are handled (the schema's arrays
// are all `VARCHAR[]`); anything else is left for TypeCoercion to report.
//
// Known divergences from PG (each pinned by a test):
// - a string COLUMN (or unrewritable literal) coalesced with a list errors
//   like PG ("cannot be matched") rather than silently wrapping into a
//   one-element list via arrow's blanket `(_, List)` cast — see
//   `coalesce_string_column_with_list_errors`.
// - malformed quoting WITHIN a brace-wrapped literal (`{"a"x,b}`, `{a"b"}`)
//   parses leniently (PG errors) — see `pg_array_parse_shapes`.
// - multi-dimensional literals are rejected → arg left untouched, the
//   string-arg guard errors (PG supports them; schema is 1-D only).

use std::mem::take;

use datafusion::common::{plan_err, tree_node::TreeNodeIterator};

/// `coalesce` wrapper whose coercion additionally unifies string args into a
/// sibling list type. Needed because the SQL planner computes projection
/// schemas (→ `coerce_types`) BEFORE analyzer rules run, so
/// `coalesce(List, Utf8)` must type-check up front; the analyzer rule below
/// then replaces the string literal with a real list literal so TypeCoercion
/// never has to cast Utf8 → List (unsupported in Arrow).
///
/// Registered under the built-in's name, shadowing it session-wide; every
/// trait method delegates to the inner built-in. On a DataFusion upgrade,
/// re-check `ScalarUDFImpl` for new methods whose defaults would diverge
/// from the built-in coalesce and forward them here too — the const assert
/// below fails the build on a version bump until that audit happens.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCoalesceUdf {
    inner: Arc<datafusion::logical_expr::ScalarUDF>,
}

/// DataFusion version `PgCoalesceUdf`'s method forwarding was last audited
/// against, compared at compile time to `datafusion::DATAFUSION_VERSION`.
/// A `cargo update` of datafusion breaks the build here on purpose:
/// re-audit `ScalarUDFImpl` for new methods, forward them above, then bump.
/// Maintenance invariant: DATAFUSION_VERSION resolves through the delta-rs
/// fork's patched graph — the fork must not bump it independently of the
/// workspace pin, or this check would pass against the wrong version. By the
/// same token, fork upgrades that keep the version string still need the
/// manual audit — the tripwire only catches plain `cargo update`s.
const AUDITED_DATAFUSION_VERSION: &str = "54.0.0";
// Byte loop because `&str` equality (PartialEq) isn't const-callable on
// stable — assert!(a == b) won't compile in a const block.
const _: () = {
    let (a, b) = (datafusion::DATAFUSION_VERSION.as_bytes(), AUDITED_DATAFUSION_VERSION.as_bytes());
    assert!(a.len() == b.len(), "DataFusion bumped: re-audit PgCoalesceUdf's ScalarUDFImpl forwarding, then update AUDITED_DATAFUSION_VERSION");
    let mut i = 0;
    while i < a.len() {
        assert!(a[i] == b[i], "DataFusion bumped: re-audit PgCoalesceUdf's ScalarUDFImpl forwarding, then update AUDITED_DATAFUSION_VERSION");
        i += 1;
    }
};

impl Default for PgCoalesceUdf {
    fn default() -> Self {
        Self { inner: datafusion::functions::core::coalesce() }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for PgCoalesceUdf {
    fn name(&self) -> &str {
        "coalesce"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        self.inner.signature()
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.inner().return_type(arg_types)
    }
    fn return_field_from_args(&self, args: datafusion::logical_expr::ReturnFieldArgs) -> Result<datafusion::arrow::datatypes::FieldRef> {
        self.inner.inner().return_field_from_args(args)
    }
    fn invoke_with_args(&self, args: datafusion::logical_expr::ScalarFunctionArgs) -> Result<datafusion::logical_expr::ColumnarValue> {
        self.inner.inner().invoke_with_args(args)
    }
    fn short_circuits(&self) -> bool {
        self.inner.inner().short_circuits()
    }
    fn simplify(
        &self, args: Vec<Expr>, info: &datafusion::logical_expr::simplify::SimplifyContext,
    ) -> Result<datafusion::logical_expr::simplify::ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types).or_else(|e| {
            // Fallback only runs after the built-in coercion failed. Promote
            // EVERY string arg (not just literals — arg types carry no
            // expression info here) to the sibling list type: PG treats any
            // string in array position as an array literal. The analyzer rule
            // rewrites the literals to real lists before execution and rejects
            // any string arg it can't rewrite (real string columns, malformed
            // literals) with a PG-style planning error — required because
            // arrow-cast's blanket `(_, List)` rule would otherwise "cast" the
            // string by wrapping it in a single-element list, silently.
            // First list type wins. If a call ever mixes list element types
            // (coalesce(utf8_list, int_list, '{}')) the retried coercion below
            // fails on the second list and the original error surfaces — no
            // silent mis-typing, just a planner-time error like today.
            let list_t = arg_types.iter().find(|t| matches!(t, DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(..))).ok_or(e)?.clone();
            let patched: Vec<DataType> = arg_types
                .iter()
                .map(|t| if matches!(t, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8) { list_t.clone() } else { t.clone() })
                .collect();
            self.inner.coerce_types(&patched)
        })
    }
}

#[derive(Debug, Default)]
pub struct PgArrayLiteralRewriter;

impl AnalyzerRule for PgArrayLiteralRewriter {
    fn name(&self) -> &str {
        "pg_array_literal_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(rewrite_in_plan).map(|t| t.data)
    }
}

fn rewrite_in_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // Leaf nodes (TableScan with pushed-down filters, Values) have no inputs;
    // their exprs are typed by their own schema instead. Without this, a
    // coalesce pushed into a TableScan's filter list would skip both the
    // rewrite and the string-arg guard below — arrow's
    // wrap-into-single-element-list cast would then fire silently. A filter on
    // a column the scan doesn't project still won't resolve here (filters are
    // typed by the source schema); it falls through to the TypeCoercion error.
    let input_schemas = match plan.inputs().as_slice() {
        [] => vec![Arc::clone(plan.schema())],
        inputs => inputs.iter().map(|i| Arc::clone(i.schema())).collect(),
    };
    plan.map_expressions(|expr| expr.transform_up(|e| rewrite_in_expr(e, &input_schemas)))
}

fn rewrite_in_expr(expr: Expr, input_schemas: &[Arc<DFSchema>]) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(Transformed::no(expr));
    };
    let no = |args| Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction { func: Arc::clone(&func), args })));
    if func.name() != "coalesce" {
        return no(args);
    }

    // Element type of the first arg that resolves to a list type.
    let Some(elem_type) = args.iter().find_map(|a| {
        input_schemas.iter().find_map(|s| match a.get_type(s.as_ref()).ok()? {
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f.data_type().clone()),
            _ => None,
        })
    }) else {
        return no(args);
    };

    let Transformed { data: new_args, transformed, .. } = args.into_iter().map_until_stop_and_collect(|a| {
        Ok(match pg_list_literal(&a, &elem_type) {
            Some(list) => Transformed::yes(Expr::Literal(list, None)),
            None => Transformed::no(a),
        })
    })?;
    // Any arg still string-typed here (a real string column, or a literal that
    // didn't parse as a PG array) survived only because PgCoalesceUdf's
    // coerce_types fallback over-promoted it to pass planning. Letting it
    // reach TypeCoercion would NOT error: arrow-cast's blanket `(_, List)`
    // rule wraps each value in a single-element list — a silently wrong
    // result. Reject it like PG ("COALESCE types ... cannot be matched").
    if let Some(bad) = new_args.iter().find_map(|a| {
        input_schemas.iter().find_map(|s| match a.get_type(s.as_ref()) {
            Ok(t @ (DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)) => Some(t),
            _ => None,
        })
    }) {
        return plan_err!("COALESCE types {bad} and List({elem_type}) cannot be matched");
    }
    Ok(Transformed::new_transformed(Expr::ScalarFunction(ScalarFunction { func, args: new_args }), transformed))
}

/// The typed list literal a string arg denotes as a PG array literal, or None
/// if it isn't a string literal, isn't a PG array literal, or has an element
/// that doesn't parse as `elem_type` (caller then leaves the arg alone).
/// Always emits `ScalarValue::List` even when the sibling column is
/// LargeList/FixedSizeList: coerce_types may claim those, but TypeCoercion
/// then casts the literal (arrow supports List → Large/FixedSizeList), so no
/// physical-planning mismatch — the variant only matters for element type.
fn pg_list_literal(arg: &Expr, elem_type: &DataType) -> Option<ScalarValue> {
    let Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) = arg else {
        return None;
    };
    let vals: Vec<ScalarValue> = parse_pg_string_array(s)?
        .into_iter()
        .map(|e| e.map_or_else(|| ScalarValue::try_from(elem_type).ok(), |s| ScalarValue::try_from_string(s, elem_type).ok()))
        .collect::<Option<_>>()?;
    Some(ScalarValue::List(ScalarValue::new_list_nullable(&vals, elem_type)))
}

/// Parse a PG array literal of strings: `{}`, `{a,b}`, `{"a,b",NULL}`.
/// Returns None if `s` isn't brace-wrapped (not an array literal), or if it
/// contains unquoted nested braces (multi-dimensional arrays like
/// `{{a},{b}}` — the schema is 1-D only, and misparsing the inner braces as
/// element text would be silently wrong; bail so the arg is left untouched).
/// Malformed quoting parses leniently rather than strictly: bailing would
/// only swap one error (this rewrite skipped → TypeCoercion's) for another,
/// while leniency keeps stable-but-sloppy client literals working.
/// Only the bare `NULL` keyword is a null element; `\N` is COPY text-format
/// syntax, not array-literal syntax, and stays literal text (as in PG).
fn parse_pg_string_array(s: &str) -> Option<Vec<Option<String>>> {
    let inner = s.trim().strip_prefix('{')?.strip_suffix('}')?;
    if inner.trim().is_empty() {
        return Some(vec![]);
    }
    let (mut elems, mut cur, mut in_quotes, mut was_quoted) = (Vec::new(), String::new(), false, false);
    let mut chars = inner.chars(); // `while let` + inner `next()` (escapes consume the next char) — not a `for` loop
    while let Some(c) = chars.next() {
        match c {
            '\\' if in_quotes => cur.push(chars.next()?),
            '{' | '}' if !in_quotes => return None, // multi-dimensional literal
            '"' => {
                if !in_quotes && cur.trim().is_empty() {
                    cur.clear(); // drop whitespace before an opening quote
                }
                in_quotes = !in_quotes;
                was_quoted = true;
            }
            ',' if !in_quotes => elems.push(finish_elem(take(&mut cur), take(&mut was_quoted))),
            _ if in_quotes || !was_quoted => cur.push(c),
            _ => {} // ignore trailing chars after a closing quote
        }
    }
    elems.push(finish_elem(cur, was_quoted));
    Some(elems)
}

/// One scanned element: unquoted `NULL` is the null element, quoted text keeps
/// its whitespace, unquoted text is trimmed.
fn finish_elem(raw: String, quoted: bool) -> Option<String> {
    if quoted {
        return Some(raw);
    }
    let trimmed = raw.trim();
    (!trimmed.eq_ignore_ascii_case("null")).then(|| trimmed.to_string())
}

#[cfg(test)]
mod pg_array_literal_rewriter_tests {
    use datafusion::{execution::session_state::SessionStateBuilder, prelude::SessionContext};

    use super::*;

    fn ctx_with_rule() -> SessionContext {
        let rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
            vec![Arc::new(PgArrayLiteralRewriter), Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new())];
        let state = SessionStateBuilder::new().with_default_features().with_analyzer_rules(rules).build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_udf(datafusion::logical_expr::ScalarUDF::from(PgCoalesceUdf::default()));
        ctx
    }

    async fn one_string(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.expect("plan ok").collect().await.expect("exec ok");
        datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string()
    }

    #[tokio::test]
    async fn coalesce_empty_pg_array_literal() {
        let ctx = ctx_with_rule();
        // cardinality(empty list) = 0 — asserts the actual coalesced value,
        // not merely that planning succeeded.
        let out = one_string(&ctx, "SELECT cardinality(COALESCE(CAST(NULL AS VARCHAR[]), '{}')) AS n FROM (SELECT 1)").await;
        assert!(out.contains("| 0 "), "{out}");
    }

    #[tokio::test]
    async fn coalesce_nonempty_pg_array_literal() {
        let ctx = ctx_with_rule();
        let out = one_string(&ctx, "SELECT COALESCE(CAST(NULL AS VARCHAR[]), '{a, b, \"c,d\", NULL}') AS v FROM (SELECT 1)").await;
        assert!(out.contains("[a, b, c,d, ]"), "{out}");
    }

    #[tokio::test]
    async fn non_array_string_untouched() {
        let ctx = ctx_with_rule();
        // No list-typed arg → rule must not fire; plain string coalesce still works.
        let out = one_string(&ctx, "SELECT COALESCE(CAST(NULL AS VARCHAR), '{}') AS v FROM (SELECT 1)").await;
        assert!(out.contains("{}"), "{out}");
    }

    // Analyzer rules run at physical planning, so the error may surface at
    // sql() or collect() — either way it must never execute successfully.
    async fn expect_plan_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("query must fail planning").to_string(),
        }
    }

    // arrow-cast's blanket `(_, List)` rule casts ANY type to a list by
    // wrapping each value in a single-element list, so without an explicit
    // guard these queries would silently return `[varchar_value]` instead of
    // failing like PG ("COALESCE types ... cannot be matched").
    #[tokio::test]
    async fn coalesce_string_column_with_list_errors() {
        let ctx = ctx_with_rule();
        let err = expect_plan_error(&ctx, "SELECT COALESCE(v, l) FROM (SELECT CAST('x' AS VARCHAR) AS v, CAST(NULL AS VARCHAR[]) AS l)").await;
        assert!(err.contains("cannot be matched"), "{err}");
    }

    #[tokio::test]
    async fn coalesce_unparseable_literal_with_list_errors() {
        let ctx = ctx_with_rule();
        let err = expect_plan_error(&ctx, "SELECT COALESCE(CAST(NULL AS VARCHAR[]), 'not-an-array') FROM (SELECT 1)").await;
        assert!(err.contains("cannot be matched"), "{err}");
    }

    // Guards the leaf-node path in rewrite_in_plan: a TableScan carrying a
    // pushed-down coalesce filter has no inputs, so its exprs must be typed
    // against its own schema for the rewrite to fire.
    #[test]
    fn rewrites_inside_table_scan_filters() {
        use datafusion::{
            arrow::datatypes::{Field, Schema},
            logical_expr::{
                col, lit,
                logical_plan::builder::{LogicalPlanBuilder, LogicalTableSource},
            },
        };
        let schema = Schema::new(vec![Field::new("hashes", DataType::List(Field::new("item", DataType::Utf8, true).into()), true)]);
        let coalesce = Expr::ScalarFunction(ScalarFunction::new_udf(datafusion::functions::core::coalesce(), vec![col("hashes"), lit("{}")]));
        let scan = LogicalPlanBuilder::scan_with_filters("t", Arc::new(LogicalTableSource::new(schema.into())), None, vec![coalesce.eq(col("hashes"))])
            .unwrap()
            .build()
            .unwrap();
        let analyzed = PgArrayLiteralRewriter.analyze(scan, &ConfigOptions::default()).unwrap();
        let LogicalPlan::TableScan(ts) = analyzed else { panic!("expected TableScan") };
        let Expr::BinaryExpr(be) = &ts.filters[0] else { panic!("expected eq filter") };
        let Expr::ScalarFunction(f) = be.left.as_ref() else { panic!("expected coalesce") };
        assert!(matches!(f.args[1], Expr::Literal(ScalarValue::List(_), _)), "array literal in TableScan filter not rewritten: {:?}", f.args[1]);
    }

    #[test]
    fn pg_array_parse_shapes() {
        assert_eq!(parse_pg_string_array("{}"), Some(vec![]));
        assert_eq!(parse_pg_string_array("{a,b}"), Some(vec![Some("a".into()), Some("b".into())]));
        assert_eq!(parse_pg_string_array(r#"{"a,b", c }"#), Some(vec![Some("a,b".into()), Some("c".into())]));
        assert_eq!(parse_pg_string_array("{NULL,\"NULL\"}"), Some(vec![None, Some("NULL".into())]));
        // Chars between a closing quote and the next comma are dropped — PG
        // rejects these literals outright; we parse leniently and keep the
        // quoted value. Pinned so the behavior is intentional, not accidental.
        assert_eq!(parse_pg_string_array("{\"a\"x,b}"), Some(vec![Some("a".into()), Some("b".into())]));
        // The mirror case — non-whitespace BEFORE an opening quote — is also
        // PG-invalid; we leniently concatenate. Pinned for the same reason.
        assert_eq!(parse_pg_string_array("{a\"b\"}"), Some(vec![Some("ab".into())]));
        // Lone backslash at end of input (inside quotes, escaping nothing):
        // chars.next()? propagates None → arg left unrewritten → the
        // string-arg guard errors (never a panic or a half-parsed list).
        assert_eq!(parse_pg_string_array("{\"a\\}"), None);
        // Multi-dimensional literals are rejected, not silently flattened;
        // braces inside quotes are ordinary element text.
        assert_eq!(parse_pg_string_array("{{a},{b}}"), None);
        assert_eq!(parse_pg_string_array(r#"{"{x}",y}"#), Some(vec![Some("{x}".into()), Some("y".into())]));
        assert_eq!(parse_pg_string_array("plain"), None);
    }
}

// Order the unordered branches of a routed MemBuffer∪Delta union so an
// `ORDER BY <sort-keys> LIMIT n` becomes a streaming, early-terminating TopK
// instead of a full blocking sort over the whole window.
//
// Background. `ProjectRoutingTable::scan` returns `Union([mem, delta])`. After
// the parquet sort-order pushdown (delta-rs fork) the Delta branch advertises
// the table's footer ordering (`[timestamp DESC, …]`); the MemBuffer branch
// (`MemorySourceConfig`) advertises none. A union is order-preserving only when
// *every* child shares the ordering, so the union is unordered and DataFusion
// inserts a blocking `SortExec` that reads the entire mem∪delta window before
// `LIMIT` — the Delta scan can never stop early.
//
// This rule runs *before* `EnforceDistribution`/`EnforceSorting`. When it finds
// a `SortExec`/`SortPreservingMergeExec` **with a fetch** (the `ORDER BY … LIMIT`
// shape) whose input contains such a union — one child already satisfying the
// requested ordering, another not — it wraps the unsatisfying child(ren) in a
// `SortExec(req).with_fetch(n)`. Now the union is order-preserving, so the
// built-in rules replace the coalesce with a `SortPreservingMergeExec` and drop
// the top blocking sort, keeping only the fetch: the merge pulls the front of
// the newest (mem) rows and the newest Delta files and stops.
//
// Scope guards (why it never regresses other plans):
// - Only fires under a *fetching* sort — plain scans / non-LIMIT sorts are
//   untouched, so no MemBuffer sort is bolted onto counts/aggregations.
// - Only fires on a *mixed* union (≥1 child already ordered, ≥1 not). When the
//   Delta pushdown is off (the mixed-footer window during the DESC rollout)
//   neither child is ordered → no-op → current blocking-sort behavior.
// - The requested ordering is re-resolved by column *name* against the union
//   schema, so an intervening projection (e.g. `DedupExec`'s column restore)
//   can't misalign the sort-key indices.

use datafusion::{
    arrow::datatypes::Schema,
    physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column as PhysColumn},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
};

#[derive(Debug, Default)]
pub struct OrderedUnionForTopK;

/// Re-express `req` (sort keys, possibly indexed against a downstream projected
/// schema) as plain columns resolved by name against `schema`. Returns `None`
/// if any key is not a plain column or is absent from `schema` — in which case
/// the rule bails (correctness over cleverness).
fn resolve_ordering(req: &LexOrdering, schema: &Schema) -> Option<LexOrdering> {
    req.iter()
        .map(|se| {
            let col = downcast::<PhysColumn>(se.expr.as_ref())?;
            let idx = schema.index_of(col.name()).ok()?;
            Some(PhysicalSortExpr::new(Arc::new(PhysColumn::new(col.name(), idx)), se.options))
        })
        .collect::<Option<Vec<_>>>()
        .and_then(LexOrdering::new)
}

/// The shared mechanism: `children` with every child that does not already
/// satisfy `req` wrapped in `SortExec(req)` (carrying `fetch`), so a union over
/// them advertises `req`.
///
/// Callers: this rule (top-K, `fetch` known) and `ProjectRoutingTable::scan`,
/// which uses it with no fetch so the mem ∪ hot ∪ delta union advertises the
/// table's lead sort key and `EnforceDistribution` satisfies `DedupExec`'s
/// `SinglePartition` with a `SortPreservingMergeExec` rather than a
/// `CoalescePartitionsExec` (which declares no ordering, leaving keep-greatest
/// dormant — see `docs/plans/2026-08-01-merge-on-read-dml.md` §3).
///
/// `Ok(None)` means "leave the plan alone":
/// - every child already satisfies `req` (nothing to inject), or
/// - a child that doesn't satisfy it is marked unsortable — `sortable[i] ==
///   false` says "this leg is a whole-window parquet scan; a blocking sort on
///   it costs far more than the ordering buys" (indices past `sortable`'s end
///   are sortable), or
/// - `require_ordered_child` and no child is ordered. The top-K rule sets this:
///   with the Delta footer pushdown off, neither branch is ordered and sorting
///   the mem branch alone buys nothing. `scan` does not — a MemBuffer-only scan
///   has no ordered leg by construction, yet is exactly where a fresh version
///   append lives, so it must still be ordered.
pub fn ordered_children(
    children: &[Arc<dyn ExecutionPlan>], req: &LexOrdering, fetch: Option<usize>, sortable: &[bool], require_ordered_child: bool,
) -> Result<Option<Vec<Arc<dyn ExecutionPlan>>>> {
    let sat: Vec<bool> = children.iter().map(|c| c.properties().equivalence_properties().ordering_satisfy(req.iter().cloned())).collect::<Result<Vec<_>>>()?;
    let unsortable = |i: usize| !sortable.get(i).copied().unwrap_or(true);
    let leave_alone = sat.iter().all(|&s| s) || sat.iter().enumerate().any(|(i, &s)| !s && unsortable(i)) || (require_ordered_child && !sat.iter().any(|&s| s));
    Ok((!leave_alone).then(|| {
        children
            .iter()
            .zip(sat)
            // `preserve_partitioning`: sort each input partition independently
            // (in parallel) and keep them, rather than requiring — and getting —
            // a `CoalescePartitionsExec` that serialises the leg. Per-partition
            // ordering is what the union advertises and what the merge above
            // consumes, so this is the cheaper shape for both callers.
            .map(|(c, s)| {
                if s {
                    Arc::clone(c)
                } else {
                    Arc::new(SortExec::new(req.clone(), Arc::clone(c)).with_preserve_partitioning(true).with_fetch(fetch)) as Arc<dyn ExecutionPlan>
                }
            })
            .collect()
    }))
}

/// Walk down from a fetching sort through single-child order-preserving
/// operators to the first `UnionExec`; if the union is mixed (some children
/// satisfy `req`, some don't), sort the unsatisfying children by `req` (with
/// `fetch`) so the union becomes order-preserving. Returns the rewritten
/// subtree, or `None` when nothing applied.
fn order_union(plan: &Arc<dyn ExecutionPlan>, req: &LexOrdering, fetch: Option<usize>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(union) = downcast::<UnionExec>(plan.as_ref()) {
        let Some(req_here) = resolve_ordering(req, &union.schema()) else {
            return Ok(None);
        };
        // Mixed union only (`require_ordered_child`): an ordered child to merge
        // toward AND an unordered child to fix. All-ordered needs nothing;
        // none-ordered means the Delta pushdown is off (mixed footers) — leave
        // the blocking sort in place.
        let children: Vec<Arc<dyn ExecutionPlan>> = union.children().into_iter().cloned().collect();
        return ordered_children(&children, &req_here, fetch, &[], true)?.map(UnionExec::try_new).transpose();
    }
    // Descend only through single-child, order-preserving operators so the
    // ordering we create actually propagates up to the sort — but NEVER through
    // a DedupExec: its survivors are decided across ALL input rows, and a
    // `with_fetch` cut on a leg below it truncates that input. Under
    // version_append the leg's top-n fills with row *versions*, so dedup can
    // emit fewer than n distinct rows while more exist, and an equal-timestamp
    // cut can keep a stale version whose newer sibling was truncated away.
    let children = plan.children();
    if children.len() == 1
        && downcast::<crate::read::DedupExec>(plan.as_ref()).is_none()
        && plan.maintains_input_order().first() == Some(&true)
        && let Some(new_child) = order_union(children[0], req, fetch)?
    {
        return Ok(Some(Arc::clone(plan).with_new_children(vec![new_child])?));
    }
    Ok(None)
}

impl PhysicalOptimizerRule for OrderedUnionForTopK {
    fn name(&self) -> &str {
        "ordered_union_for_topk"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_down(|node| {
                // Anchor: a fetching global sort (the `ORDER BY … LIMIT n` shape).
                let Some((req, fetch)) = downcast::<SortExec>(node.as_ref())
                    .map(|s| (s.expr().clone(), s.fetch()))
                    .or_else(|| downcast::<SortPreservingMergeExec>(node.as_ref()).map(|m| (m.expr().clone(), m.fetch())))
                    .filter(|(_, fetch)| fetch.is_some())
                else {
                    return Ok(Transformed::no(node));
                };
                let children = node.children();
                let rewritten = children.first().copied().map(|input| order_union(input, &req, fetch)).transpose()?.flatten();
                Ok(match rewritten {
                    Some(new_input) => Transformed::yes(node.with_new_children(vec![new_input])?),
                    None => Transformed::no(node),
                })
            })?
            .data)
    }
}

#[cfg(test)]
mod ordered_union_for_topk_tests {
    use datafusion::{
        arrow::{
            compute::SortOptions,
            datatypes::{DataType, Field, SchemaRef, TimeUnit},
        },
        physical_expr::{EquivalenceProperties, Partitioning, expressions::Column as PhysColumn},
        physical_plan::{
            PlanProperties,
            execution_plan::{Boundedness, EmissionType},
        },
    };

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
            Field::new("id", DataType::Utf8, false),
        ]))
    }

    fn ts_desc() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(PhysColumn::new("timestamp", 0)), SortOptions { descending: true, nulls_first: true })]).unwrap()
    }

    /// A leaf exec that lets the test declare whatever output ordering it wants,
    /// standing in for either the ordered Delta scan or the unordered MemBuffer.
    #[derive(Debug)]
    struct MockLeaf {
        props: Arc<PlanProperties>,
    }

    impl MockLeaf {
        fn leaf(schema: SchemaRef, ordering: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
            // `Option<LexOrdering>` is an iterator of 0..1 orderings — no branch needed.
            let eq = EquivalenceProperties::new_with_orderings(schema, ordering);
            let props = Arc::new(PlanProperties::new(eq, Partitioning::UnknownPartitioning(1), EmissionType::Incremental, Boundedness::Bounded));
            Arc::new(MockLeaf { props })
        }
    }

    impl datafusion::physical_plan::DisplayAs for MockLeaf {
        fn fmt_as(&self, _t: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "MockLeaf")
        }
    }

    impl ExecutionPlan for MockLeaf {
        fn name(&self) -> &'static str {
            "MockLeaf"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.props
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(self: Arc<Self>, _c: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, _p: usize, _c: Arc<datafusion::execution::TaskContext>) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    /// Number of `SortExec` nodes anywhere in the tree.
    fn count_sorts(plan: &Arc<dyn ExecutionPlan>) -> usize {
        usize::from(downcast::<SortExec>(plan.as_ref()).is_some()) + plan.children().into_iter().map(count_sorts).sum::<usize>()
    }

    /// `SortExec(ts DESC, fetch)` over `mem (unordered) ∪ delta`, where delta
    /// advertises `[ts DESC]` only when `delta_ordered` — the two dimensions the
    /// guards below vary. Returns the plan and the requested ordering.
    fn fetching_sort_over_union(delta_ordered: bool, fetch: Option<usize>) -> (Arc<dyn ExecutionPlan>, LexOrdering) {
        let (s, ord) = (schema(), ts_desc());
        let union = UnionExec::try_new(vec![MockLeaf::leaf(s.clone(), None), MockLeaf::leaf(s, delta_ordered.then(|| ord.clone()))]).unwrap();
        (Arc::new(SortExec::new(ord.clone(), union).with_fetch(fetch)), ord)
    }

    // Bug: `ORDER BY timestamp DESC LIMIT n` over a mem∪delta union re-sorts the
    // whole window because the MemBuffer branch advertises no ordering, so the
    // union is unordered and the delta scan can't early-terminate. The rule must
    // sort the mem branch to match delta's advertised ordering.
    #[test]
    fn wraps_unordered_mem_branch_of_fetching_sort() {
        let (top, ord) = fetching_sort_over_union(true, Some(50));

        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();

        // The union is now order-preserving: it advertises [ts DESC].
        assert!(
            out.children()[0].properties().equivalence_properties().ordering_satisfy(ord.iter().cloned()).unwrap(),
            "union must advertise the sort ordering after the rule runs"
        );
        // Top sort + one injected mem sort = 2 SortExecs.
        assert_eq!(count_sorts(&out), 2, "exactly one SortExec injected over the mem branch");
    }

    // Guard: no fetch (plain ORDER BY, no LIMIT) → rule must not touch the plan,
    // so counts/aggregations and unbounded sorts don't grow a MemBuffer sort.
    #[test]
    fn ignores_sort_without_fetch() {
        let (top, _) = fetching_sort_over_union(true, None);
        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();
        assert_eq!(count_sorts(&out), 1, "no injection when there is no fetch");
    }

    // `ordered_children` is also called directly by `ProjectRoutingTable::scan`
    // (merge-on-read), with `sortable` marking the cheap in-memory legs and
    // `require_ordered_child = false`. Both guards must hold: an unsortable
    // unordered leg (a whole-window Delta scan) aborts the whole rewrite, and a
    // MemBuffer-only scan with no ordered leg at all still gets sorted.
    #[test]
    fn ordered_children_honours_sortable_and_lone_leg() {
        let (s, ord) = (schema(), ts_desc());
        let mem = MockLeaf::leaf(s.clone(), None);
        let delta_ordered = MockLeaf::leaf(s.clone(), Some(ord.clone()));
        let delta_unordered = MockLeaf::leaf(s.clone(), None);

        // mem (sortable) ∪ delta (ordered) → mem gets sorted.
        let out = ordered_children(&[mem.clone(), delta_ordered], &ord, None, &[true, false], false).unwrap().expect("mixed union is rewritten");
        assert_eq!(count_sorts(&out[0]), 1, "the mem leg is sorted");
        assert_eq!(count_sorts(&out[1]), 0, "the already-ordered Delta leg is untouched");

        // mem (sortable) ∪ delta (UNORDERED, unsortable) → bail entirely: a
        // blocking sort over a whole-window parquet scan is the 2026-07-21 OOM.
        assert!(ordered_children(&[mem.clone(), delta_unordered], &ord, None, &[true, false], false).unwrap().is_none());

        // A lone unordered mem leg: nothing to merge toward, but it is exactly
        // where a fresh version append lives, so it is still sorted.
        assert!(ordered_children(std::slice::from_ref(&mem), &ord, None, &[true], false).unwrap().is_some());
        // ...unless the caller is the top-K rule, which requires an ordered peer.
        assert!(ordered_children(std::slice::from_ref(&mem), &ord, None, &[true], true).unwrap().is_none());
    }

    // Guard: when no child is ordered (Delta pushdown off during mixed-footer
    // rollout) the rule is a no-op — the built-in blocking sort stays.
    #[test]
    fn ignores_union_with_no_ordered_child() {
        let (top, _) = fetching_sort_over_union(false, Some(50));
        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();
        assert_eq!(count_sorts(&out), 1, "no injection when neither branch is ordered");
    }
}

// Rewrites `row_to_json(t)` — a bare relation alias standing for a whole row —
// into `row_to_json(named_struct('c1', t.c1, …))`.
//
// PostgreSQL lets a table alias name the entire row in a function argument.
// DataFusion rejects it while PLANNING the SQL ("No field named t. Valid
// fields are t.total, t.active"), which is before any analyzer or optimizer
// rule can see the plan — so unlike [`super::ExistsInProjection`] this has to
// happen on the statement, not the plan.
//
// pgAdmin's dashboard polls exactly this shape every 5 seconds:
//
// ```sql
// SELECT 'x' AS chart_name, pg_catalog.row_to_json(t) AS chart_data
// FROM (SELECT (…) AS "total", (…) AS "active") t
// ```
//
// The column names come from the derived table's own SELECT aliases, which are
// present in the AST, so no schema lookup is needed. A relation whose columns
// are not all explicitly aliased is left alone — inventing names there would be
// guessing, and the statement fails exactly as it does today.

use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue, ValueWithSpan, VisitMut, VisitorMut,
};

/// Cheap guard so the parse/unparse round trip only happens for statements that
/// could possibly need it.
pub fn might_need_rewrite(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains("row_to_json")
}

/// Returns the rewritten statement only when something actually changed, so an
/// untouched statement is never round-tripped through the unparser.
pub fn rewrite(statement: &mut Statement) -> bool {
    let mut visitor = RowToJsonRecord { rewrote: false };
    let _ = statement.visit(&mut visitor);
    visitor.rewrote
}

struct RowToJsonRecord {
    rewrote: bool,
}

impl VisitorMut for RowToJsonRecord {
    type Break = ();

    fn post_visit_query(&mut self, query: &mut Query) -> std::ops::ControlFlow<Self::Break> {
        self.rewrite_set_expr(query.body.as_mut());
        std::ops::ControlFlow::Continue(())
    }
}

impl RowToJsonRecord {
    /// A UNION's branches are `SetExpr`s, not `Query`s, so they are never
    /// reached by matching on `query.body` alone. pgAdmin's dashboard sends one
    /// `SELECT ... UNION ALL SELECT ...` per chart, and every branch was being
    /// skipped. Re-running over an already-rewritten branch is a no-op, since
    /// its argument is no longer a bare identifier.
    fn rewrite_set_expr(&mut self, body: &mut SetExpr) {
        match body {
            SetExpr::Select(select) => self.rewrite_select(select),
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_set_expr(left);
                self.rewrite_set_expr(right);
            }
            SetExpr::Query(query) => self.rewrite_set_expr(query.body.as_mut()),
            _ => {}
        }
    }

    fn rewrite_select(&mut self, select: &mut Select) {
        // `post_visit_query` means inner queries are already rewritten, so the
        // aliases collected here belong to this SELECT's own FROM.
        let relations: Vec<(String, Vec<String>)> = select.from.iter().filter_map(|from| derived_columns(&from.relation)).collect();
        if relations.is_empty() {
            return;
        }
        for item in &mut select.projection {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };
            self.rewrite_expr(expr, &relations);
        }
    }

    fn rewrite_expr(&mut self, expr: &mut SqlExpr, relations: &[(String, Vec<String>)]) {
        let SqlExpr::Function(function) = expr else { return };
        if !is_row_to_json(&function.name) {
            return;
        }
        // pgAdmin writes `pg_catalog.row_to_json`, and a schema-qualified UDF
        // name does not resolve ("Invalid function 'pg_catalog.row_to_json'").
        if function.name.0.len() > 1 {
            function.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("row_to_json"))]);
            self.rewrote = true;
        }
        let FunctionArguments::List(FunctionArgumentList { args, .. }) = &mut function.args else { return };
        let [FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Identifier(ident)))] = args.as_mut_slice() else {
            return;
        };
        let Some((alias, columns)) = relations.iter().find(|(alias, _)| alias.eq_ignore_ascii_case(&ident.value)) else {
            return;
        };
        args[0] = FunctionArg::Unnamed(FunctionArgExpr::Expr(named_struct(alias, columns)));
        self.rewrote = true;
    }
}

fn is_row_to_json(name: &ObjectName) -> bool {
    name.0.last().is_some_and(|part| match part {
        ObjectNamePart::Identifier(ident) => ident.value.eq_ignore_ascii_case("row_to_json"),
        _ => false,
    })
}

/// `named_struct('total', t."total", 'active', t."active", …)`, preserving the
/// declared column order.
fn named_struct(alias: &str, columns: &[String]) -> SqlExpr {
    let args = columns
        .iter()
        .flat_map(|column| {
            [
                FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(ValueWithSpan::from(SqlValue::SingleQuotedString(column.clone()))))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::CompoundIdentifier(vec![
                    Ident::new(alias.to_string()),
                    Ident::with_quote('"', column.clone()),
                ]))),
            ]
        })
        .collect();
    SqlExpr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("named_struct"))]),
        args: FunctionArguments::List(FunctionArgumentList { duplicate_treatment: None, args, clauses: vec![] }),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// `(SELECT … AS a, … AS b) t` → `("t", ["a", "b"])`. `None` unless the relation
/// is a derived table with an alias and every projected column is explicitly
/// named, since the column names are the whole point.
fn derived_columns(relation: &TableFactor) -> Option<(String, Vec<String>)> {
    let TableFactor::Derived { subquery, alias, .. } = relation else { return None };
    let alias = alias.as_ref()?;
    let SetExpr::Select(select) = subquery.body.as_ref() else { return None };
    let columns = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
            // A bare column still has a well-defined name.
            SelectItem::UnnamedExpr(SqlExpr::Identifier(ident)) => Some(ident.value.clone()),
            SelectItem::UnnamedExpr(SqlExpr::CompoundIdentifier(parts)) => parts.last().map(|part| part.value.clone()),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    (!columns.is_empty()).then(|| (alias.name.value.clone(), columns))
}

#[cfg(test)]
mod row_to_json_record_tests {
    use datafusion::sql::sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    use super::*;

    fn rewritten(sql: &str) -> String {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parses");
        let changed = rewrite(&mut statements[0]);
        assert!(changed, "expected a rewrite for: {sql}");
        statements[0].to_string()
    }

    #[test]
    fn bare_alias_becomes_named_struct_in_declared_order() {
        let sql = rewritten(r#"SELECT row_to_json(t) FROM (SELECT 1 AS "total", 2 AS "active") t"#);
        assert!(sql.contains(r#"named_struct('total', t."total", 'active', t."active")"#), "got: {sql}");
    }

    /// pgAdmin schema-qualifies the call.
    #[test]
    fn qualified_pg_catalog_call_is_rewritten() {
        let sql = rewritten(r#"SELECT pg_catalog.row_to_json(t) FROM (SELECT 1 AS "a") t"#);
        assert!(sql.contains("named_struct('a', t.\"a\")"), "got: {sql}");
        // A schema-qualified UDF name does not resolve in DataFusion.
        assert!(!sql.contains("pg_catalog.row_to_json"), "qualifier must be stripped: {sql}");
    }

    /// The shape prod actually sends: one branch per chart, UNION ALL. A visitor
    /// that only matches `query.body == Select` silently skips every branch.
    #[test]
    fn every_union_branch_is_rewritten() {
        let sql = rewritten(
            r#"SELECT 'a' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT 1 AS "Total") t
               UNION ALL
               SELECT 'b' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT 2 AS "Active") t"#,
        );
        assert!(sql.contains(r#"named_struct('Total', t."Total")"#), "first branch: {sql}");
        assert!(sql.contains(r#"named_struct('Active', t."Active")"#), "second branch: {sql}");
        assert!(!sql.contains("row_to_json(t)"), "no branch may keep the bare alias: {sql}");
    }

    fn unchanged(sql: &str) {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parses");
        assert!(!rewrite(&mut statements[0]), "should not rewrite: {sql}");
    }

    /// An unnamed column has no name to key the object by; guessing one would be
    /// worse than the planning error the user already gets.
    #[test]
    fn unaliased_derived_column_is_left_alone() {
        unchanged("SELECT row_to_json(t) FROM (SELECT count(*), 1 AS b) t");
    }

    /// `row_to_json(some_column)` is an ordinary call on a value, not a record.
    #[test]
    fn non_relation_identifier_is_left_alone() {
        unchanged("SELECT row_to_json(payload) FROM events");
    }

    /// A real table alias is not a derived table: its columns are not in the AST.
    #[test]
    fn plain_table_alias_is_left_alone() {
        unchanged("SELECT row_to_json(t) FROM some_table t");
    }
}

// Rewrites `EXISTS(q)` in a projection to `(SELECT count(1) FROM q) > 0`.
//
// DataFusion decorrelates EXISTS only in filter position. An EXISTS in a
// SELECT list survives every optimizer pass and dies in physical planning with
// "Physical plan does not support logical expression Exists". pgAdmin's
// object-explorer schema query computes `is_catalog` exactly that way, so the
// whole browser tree failed to load (2026-08-13).
//
// The rewrite is semantics-preserving rather than a compatibility shim:
// `EXISTS(q)` is true iff `q` returns at least one row, which is what
// `count(1) > 0` asks. Correlated *scalar* subqueries in a projection ARE
// decorrelated by DataFusion, which is why the count form plans and the EXISTS
// form does not.

use datafusion::{
    functions_aggregate::expr_fn::count,
    logical_expr::{LogicalPlanBuilder, Subquery},
};

#[derive(Debug)]
pub struct ExistsInProjection;

impl AnalyzerRule for ExistsInProjection {
    fn name(&self) -> &str {
        "exists_in_projection"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(|plan| match plan {
            LogicalPlan::Projection(projection) => {
                let (expr, transformed) = rewrite_exprs(projection.expr.clone())?;
                if !transformed {
                    return Ok(Transformed::no(LogicalPlan::Projection(projection)));
                }
                LogicalPlanBuilder::from(Arc::unwrap_or_clone(projection.input)).project(expr)?.build().map(Transformed::yes)
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|transformed| transformed.data)
    }
}

fn rewrite_exprs(exprs: Vec<Expr>) -> Result<(Vec<Expr>, bool)> {
    let mut any = false;
    let rewritten = exprs
        .into_iter()
        .map(|expr| {
            // `transform_up` so a nested EXISTS is rewritten before its parent
            // is inspected; `Alias` nodes must survive untouched or the
            // projection's output column names change.
            expr.transform_up(|expr| match expr {
                Expr::Exists(exists) => {
                    any = true;
                    Ok(Transformed::yes(match count_subquery(exists.subquery)? {
                        Some(count) if exists.negated => count.eq(lit(0_i64)),
                        Some(count) => count.gt(lit(0_i64)),
                        // Provably empty subquery: EXISTS is a constant.
                        None => lit(exists.negated),
                    }))
                }
                other => Ok(Transformed::no(other)),
            })
            .map(|transformed| transformed.data)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((rewritten, any))
}

/// `q` → scalar subquery `SELECT count(1) FROM q`, keeping the outer
/// references so DataFusion still sees it as correlated. `None` means `q` is
/// provably empty, so the caller can fold the EXISTS to a constant.
fn count_subquery(subquery: Subquery) -> Result<Option<Expr>> {
    let Subquery { subquery: plan, outer_ref_columns, spans } = subquery;
    let Some(plan) = peel_row_caps(Arc::unwrap_or_clone(plan)) else {
        return Ok(None);
    };
    let counted = LogicalPlanBuilder::from(plan).aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64))])?.build()?;
    Ok(Some(Expr::ScalarSubquery(Subquery { subquery: Arc::new(counted), outer_ref_columns, spans })))
}

/// Strips `LIMIT n` from the top of an EXISTS subquery. A row cap cannot change
/// whether *some* row exists, and it has to go: DataFusion will not decorrelate
/// a scalar subquery containing a Limit, and an un-decorrelated scalar subquery
/// fails physical planning exactly like the EXISTS it replaced. pgAdmin writes
/// every one of these as `... LIMIT 1`, so this is the common case, not an edge.
///
/// `OFFSET` is deliberately NOT stripped — `LIMIT 1 OFFSET 5` asks whether at
/// least 6 rows exist, so dropping it would change the answer. Such a subquery
/// keeps its Limit and simply fails to plan, as it does today.
///
/// Returns `None` for `LIMIT 0`, which can never produce a row.
fn peel_row_caps(mut plan: LogicalPlan) -> Option<LogicalPlan> {
    while let LogicalPlan::Limit(limit) = &plan {
        if limit.skip.as_deref().is_some_and(|skip| literal_count(skip) != Some(0)) {
            break;
        }
        if limit.fetch.as_deref().and_then(literal_count) == Some(0) {
            return None;
        }
        plan = Arc::unwrap_or_clone(limit.input.clone());
    }
    Some(plan)
}

fn literal_count(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value, ..) => value.cast_to(&datafusion::arrow::datatypes::DataType::Int64).ok().and_then(|value| match value {
            datafusion::scalar::ScalarValue::Int64(count) => count,
            _ => None,
        }),
        _ => None,
    }
}

#[cfg(test)]
mod exists_in_projection_tests {
    use datafusion::{execution::session_state::SessionStateBuilder, prelude::SessionContext};

    use super::*;

    async fn ctx() -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(ExistsInProjection))
            .with_analyzer_rule(Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.sql("CREATE TABLE outer_t(id INT) AS VALUES (1), (2)").await.unwrap().collect().await.unwrap();
        ctx.sql("CREATE TABLE inner_t(fk INT) AS VALUES (1)").await.unwrap().collect().await.unwrap();
        ctx
    }

    /// The failure this rule exists for: physical planning rejects a projection
    /// EXISTS outright, so the rewrite is what makes the query runnable at all.
    #[tokio::test]
    async fn correlated_exists_in_a_projection_plans_and_evaluates() {
        let batches = ctx()
            .await
            .sql("SELECT id, EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id LIMIT 1) AS present FROM outer_t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let flags = batches
            .iter()
            .flat_map(|batch| {
                let column = datafusion::arrow::array::AsArray::as_boolean(batch.column(1));
                (0..batch.num_rows()).map(|row| column.value(row)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(flags, vec![true, false], "id=1 has a match, id=2 does not");
    }

    #[tokio::test]
    async fn negated_exists_inverts() {
        let batches = ctx()
            .await
            .sql("SELECT id, NOT EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id) AS absent FROM outer_t ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let column = datafusion::arrow::array::AsArray::as_boolean(batches[0].column(1));
        assert!(!column.value(0) && column.value(1), "id=1 matches so absent=false; id=2 absent=true");
    }

    /// Boundary guard. `LIMIT 1 OFFSET 5` asks whether at least SIX rows exist,
    /// so peeling it would silently change the answer. Such a subquery keeps its
    /// Limit and still fails to plan — the honest outcome. If someone later
    /// makes this case pass, it must be by counting past the offset
    /// (`count > skip`), never by dropping the offset.
    #[tokio::test]
    async fn offset_is_not_peeled_and_is_never_answered_wrongly() {
        let result = ctx()
            .await
            .sql("SELECT id, EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id LIMIT 1 OFFSET 5) AS present FROM outer_t")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(result.is_err(), "an offset EXISTS must error rather than return a wrong answer");
    }

    /// `LIMIT 0` can never produce a row, so EXISTS folds to a constant.
    #[tokio::test]
    async fn limit_zero_folds_to_false() {
        let batches = ctx()
            .await
            .sql("SELECT id, EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id LIMIT 0) AS present FROM outer_t")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let column = datafusion::arrow::array::AsArray::as_boolean(batches[0].column(1));
        assert!((0..batches[0].num_rows()).all(|row| !column.value(row)));
    }
}

// Expand `qualifier.*` inside scalar function arguments into the explicit
// column list.
//
// Postgres special-cases `t.*` in function calls: it's syntactically expanded
// into the columns of `t`, in declared order, before the function is resolved.
// DataFusion's SQL planner parses `t.*` into `Expr::Wildcard { qualifier: …}`
// but never lowers it inside function-argument lists, so the call hits
// `TypeCoercion` with a typeless wildcard and fails with
// `error: Wildcard expressions are not allowed in this context`.
//
// This rule does the lowering. It runs before `TypeCoercion`. It only touches
// qualified wildcards (`sub.*`) — bare `*` keeps its existing meaning
// (already errors / handled elsewhere).
//
// Motivating case: monoscope's row-extraction wrapper
// `SELECT jsonb_build_array(sub.*) FROM (<inner>) sub` — works in PG natively,
// needs this rule on TimeFusion. After expansion the call is just
// `jsonb_build_array(sub.c1, sub.c2, …, sub.cN)`.
//
// Limitations / not-yet:
// - Only expands inside `ScalarFunction`. Aggregate / window calls with
//   `qualifier.*` are uncommon (Postgres `count(t.*)` is the main one and is
//   normally written `count(*)`), and they live behind different Expr
//   variants. Add when a real caller hits it.
// - Unqualified `Expr::Wildcard { qualifier: None, .. }` is left alone —
//   bare `f(*)` has different semantics (think `count(*)`) and we don't want
//   to silently coerce it here.

use datafusion::common::plan_datafusion_err;

#[derive(Debug, Default)]
pub struct WildcardFnArgExpander;

impl AnalyzerRule for WildcardFnArgExpander {
    fn name(&self) -> &str {
        "wildcard_fn_arg_expander"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(expand_in_plan).map(|t| t.data)
    }
}

fn expand_in_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    // Schemas of every direct input (Projection/Aggregate has one input;
    // joins have two; etc.). The wildcard's qualifier must resolve against
    // one of these.
    let input_schemas: Vec<_> = plan.inputs().iter().map(|i| i.schema().clone()).collect();
    if input_schemas.is_empty() {
        return Ok(Transformed::no(plan));
    }
    // No recompute_schema() needed (cf. VariantSelectRewriter which does call it):
    // `jsonb_build_array(VARIADIC any) -> Utf8View` returns the same type whether we
    // pass 1 wildcard or N expanded columns, so the projection's output schema
    // doesn't change. TypeCoercion (the next analyzer pass) re-checks types end-to-end
    // anyway, so any type drift from another rule would be caught there.
    plan.map_expressions(|expr| expr.transform_up(|e| expand_in_expr(e, &input_schemas)))
}

#[allow(deprecated)] // Expr::Wildcard is the actual variant the SQL planner emits today (#7765 plans to replace it, not gone yet)
fn expand_in_expr(expr: Expr, input_schemas: &[Arc<DFSchema>]) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(Transformed::no(expr));
    };
    // Cheap up-front check: any qualified wildcard in args at all?
    if !args.iter().any(|a| matches!(a, Expr::Wildcard { qualifier: Some(_), .. })) {
        return Ok(Transformed::no(Expr::ScalarFunction(ScalarFunction { func, args })));
    }
    let args = args
        .into_iter()
        .map(|arg| match arg {
            Expr::Wildcard { qualifier: Some(q), .. } => expand_qualifier(&q, input_schemas),
            other => Ok(vec![other]),
        })
        .collect::<Result<Vec<_>>>()?
        .concat();
    Ok(Transformed::yes(Expr::ScalarFunction(ScalarFunction { func, args })))
}

/// Columns of the first input schema owning `qualifier`, in declared order. SQL forbids
/// duplicate qualifier names in one scope, so first-match-wins is unambiguous — DataFusion
/// would already have rejected the plan if two schemas shared a name.
fn expand_qualifier(qualifier: &TableReference, input_schemas: &[Arc<DFSchema>]) -> Result<Vec<Expr>> {
    input_schemas
        .iter()
        .map(|schema| {
            schema
                .fields_indices_with_qualified(qualifier)
                .into_iter()
                .map(|idx| {
                    let (q, f) = schema.qualified_field(idx);
                    Expr::Column(Column::new(q.cloned(), f.name()))
                })
                .collect::<Vec<_>>()
        })
        .find(|cols| !cols.is_empty())
        .ok_or_else(|| plan_datafusion_err!("Unknown qualifier in function argument: {qualifier}"))
}

#[cfg(test)]
mod wildcard_fn_arg_expander_tests {
    use datafusion::{arrow::array::StringViewArray, execution::session_state::SessionStateBuilder, prelude::SessionContext};

    use super::*;

    fn ctx_with_rule() -> SessionContext {
        // Mirror database.rs ordering: WildcardFnArgExpander before TypeCoercion.
        let rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> = vec![
            Arc::new(datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction::new()),
            Arc::new(WildcardFnArgExpander),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
        ];
        let state = SessionStateBuilder::new().with_default_features().with_analyzer_rules(rules).build();
        let mut ctx = SessionContext::new_with_state(state);
        crate::read::functions::register_custom_functions(&mut ctx).unwrap();
        ctx
    }

    /// First row of the single output column, as the JSON text `jsonb_build_array` produces.
    async fn first_json(sql: &str) -> String {
        let batches = ctx_with_rule().sql(sql).await.expect("plan ok").collect().await.expect("exec ok");
        batches[0].column(0).as_any().downcast_ref::<StringViewArray>().expect("StringViewArray").value(0).to_string()
    }

    /// End-to-end: the exact shape monoscope wants — `jsonb_build_array(sub.*)`
    /// expands to the inner SELECT's column values in declared order.
    #[tokio::test]
    async fn jsonb_build_array_expands_qualified_wildcard() {
        assert_eq!(first_json("SELECT jsonb_build_array(sub.*) FROM (SELECT 1 AS a, 'x' AS b, true AS c) sub").await, r#"[1,"x",true]"#);
    }

    /// `sub` doesn't exist at this scope — DataFusion's SQL planner catches this
    /// before our analyzer runs (`Invalid qualifier sub`). Our rule's own
    /// "Unknown qualifier" plan_err is defensive — it would fire if the planner's
    /// scope check ever changed shape — but the user-visible error stays clear.
    #[tokio::test]
    async fn unknown_qualifier_errors_clearly() {
        let ctx = ctx_with_rule();
        let err = ctx.sql("SELECT jsonb_build_array(sub.*) FROM (SELECT 1 AS a) other").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Invalid qualifier sub"), "msg: {msg}");
    }

    /// Two qualifiers in one call — schema lookup must handle each independently
    /// and concatenate the column lists in argument order.
    #[tokio::test]
    async fn multiple_qualifiers_in_one_call() {
        let json = first_json(
            "SELECT jsonb_build_array(a.*, b.*) \
             FROM (SELECT 1 AS x, 2 AS y) a \
             CROSS JOIN (SELECT 'p' AS p, 'q' AS q) b",
        )
        .await;
        assert_eq!(json, r#"[1,2,"p","q"]"#);
    }

    /// Mixed wildcard and literal args — non-wildcard args must be preserved in
    /// their original position; the expansion only replaces the wildcard slot.
    #[tokio::test]
    async fn mixes_wildcard_with_other_args() {
        assert_eq!(first_json("SELECT jsonb_build_array(0, sub.*, 99) FROM (SELECT 1 AS a, 2 AS b) sub").await, r#"[0,1,2,99]"#);
    }

    /// `outer(inner(sub.*))` — transform_up visits the inner ScalarFunction first,
    /// so the wildcard expansion has to happen there, and the outer call then sees
    /// the resolved column args.
    #[tokio::test]
    async fn nested_function_calls_expand_inside_out() {
        assert_eq!(first_json("SELECT jsonb_build_array(jsonb_build_array(sub.*)) FROM (SELECT 1 AS a, 2 AS b) sub").await, r#"[[1,2]]"#);
    }
}

// Defer expensive scalar projections past TopK (Sort with fetch).
//
// `SELECT jsonb_build_array(...) FROM t WHERE ... ORDER BY ts DESC LIMIT 501`
// plans as `Sort(fetch=501)` over `Projection(expensive exprs)`: the JSON
// building / timestamp formatting / casts run for EVERY row in the time
// window before TopK keeps 501. On wide windows that's minutes of CPU and
// an OOM-sized allocation storm (observed killing prod TimeFusion).
//
// Rewrite: `Sort(fetch) → Projection(expensive)` becomes
// `Projection(expensive, rebuilt) → Sort(fetch, exprs inlined) → Projection(raw cols)`
// so non-trivial exprs are evaluated only on the `fetch` surviving rows.
// Registered after DataFusion's defaults, so `push_down_limit` has already
// folded LIMIT into `Sort.fetch` by the time it runs.

use std::collections::BTreeSet;

use datafusion::{
    logical_expr::{Sort, SortExpr},
    optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule},
};

#[derive(Debug, Default)]
pub struct DeferExpensiveProjection;

/// Columns, literals, and aliases thereof cost nothing per-row; everything
/// else (function calls, casts, IS NULL, arithmetic) is worth deferring.
fn is_trivial(e: &Expr) -> bool {
    match e {
        Expr::Column(_) | Expr::Literal(..) => true,
        Expr::Alias(a) => is_trivial(&a.expr),
        _ => false,
    }
}

impl OptimizerRule for DeferExpensiveProjection {
    fn name(&self) -> &str {
        "defer_expensive_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        let rewritten = defer(&plan)?;
        Ok(rewritten.map_or_else(|| Transformed::no(plan), Transformed::yes))
    }
}

/// `None` when the plan is not a `Sort(fetch)` over a projection worth deferring.
fn defer(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Sort(sort) = plan else { return Ok(None) };
    let (LogicalPlan::Projection(proj), Some(_)) = (sort.input.as_ref(), sort.fetch) else { return Ok(None) };
    if proj.expr.iter().all(is_trivial) {
        return Ok(None);
    }

    // (qualifier, name, unaliased expr) per projection output — used both to
    // inline sort keys below the TopK and to rebuild the projection above it.
    let outputs = || proj.schema.iter().zip(proj.expr.iter()).map(|((q, f), e)| (q.cloned(), f.name(), e.clone().unalias()));
    let out_map: HashMap<Column, Expr> = outputs().map(|(q, name, e)| (Column::new(q, name), e)).collect();

    let new_sort_exprs = sort
        .expr
        .iter()
        .map(|se| {
            let expr = se
                .expr
                .clone()
                .transform_up(|e| {
                    Ok(match &e {
                        // `match` over `map_or` here: the latter would clone `e` on the hit path too.
                        Expr::Column(c) => match out_map.get(c) {
                            Some(rep) => Transformed::yes(rep.clone()),
                            None => Transformed::no(e),
                        },
                        _ => Transformed::no(e),
                    })
                })?
                .data;
            Ok(SortExpr { expr, asc: se.asc, nulls_first: se.nulls_first })
        })
        .collect::<Result<Vec<_>>>()?;

    // Raw input columns the deferred exprs and inlined sort keys need; the set
    // is ordered so the rewritten plan is deterministic (`column_refs` is not).
    let needed: BTreeSet<&Column> = proj.expr.iter().chain(new_sort_exprs.iter().map(|se| &se.expr)).flat_map(|e| e.column_refs()).collect();
    if needed.is_empty() {
        return Ok(None);
    }

    let min_proj = Projection::try_new(needed.into_iter().cloned().map(Expr::Column).collect(), Arc::clone(&proj.input))?;
    let new_sort = LogicalPlan::Sort(Sort { expr: new_sort_exprs, input: Arc::new(LogicalPlan::Projection(min_proj)), fetch: sort.fetch });
    // Alias each rebuilt expr to its original (qualifier, name) so the parent
    // plan's column references and the root schema are unchanged.
    let rebuilt = outputs().map(|(q, name, e)| e.alias_qualified(q, name)).collect();
    let hoisted = Projection::try_new_with_schema(rebuilt, Arc::new(new_sort), Arc::clone(&proj.schema))?;
    Ok(Some(LogicalPlan::Projection(hoisted)))
}

#[cfg(test)]
mod defer_expensive_projection_tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, TimeUnit},
        datasource::MemTable,
        execution::session_state::SessionStateBuilder,
        prelude::SessionContext,
    };

    use super::*;

    async fn plans(sql: &str, with_rule: bool) -> (String, String) {
        let builder = SessionStateBuilder::new().with_default_features();
        let builder = if with_rule { builder.with_optimizer_rule(Arc::new(DeferExpensiveProjection)) } else { builder };
        let ctx = SessionContext::new_with_state(builder.build());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let logical = df.clone().into_optimized_plan().unwrap().display_indent().to_string();
        let physical = datafusion::physical_plan::displayable(df.create_physical_plan().await.unwrap().as_ref()).indent(false).to_string();
        (logical, physical)
    }

    async fn optimized_plan(sql: &str, with_rule: bool) -> String {
        plans(sql, with_rule).await.0
    }

    /// Regression guard for the prod OOM: jsonb-style row building must not
    /// run below the TopK. Without the rule, the expensive expr sits under
    /// `Sort`, i.e. it is evaluated for every row in the window.
    #[tokio::test]
    async fn defers_expensive_projection_past_topk() {
        let sql = "SELECT concat(id, name) FROM t ORDER BY timestamp DESC LIMIT 5";
        let before = optimized_plan(sql, false).await;
        let (b_sort, b_concat) = (before.find("Sort:").unwrap(), before.rfind("concat").unwrap());
        assert!(b_concat > b_sort, "baseline should evaluate concat below Sort:\n{before}");

        let (after, phys) = plans(sql, true).await;
        let sort_pos = after.find("Sort:").expect(&after);
        let concat_pos = after.find("concat").expect(&after);
        assert!(concat_pos < sort_pos, "expensive expr must be above the TopK sort:\n{after}");
        assert!(after.contains("fetch=5"), "TopK fetch must survive the rewrite:\n{after}");
        // The physical ProjectionPushdown rule must not push the expensive
        // projection back below SortExec (it can only do so when the sort key
        // survives the projection as a raw column, which it doesn't here).
        let p_sort = phys.find("SortExec").expect(&phys);
        assert!(phys.find("concat").expect(&phys) < p_sort, "physical plan must keep concat above SortExec:\n{phys}");
        assert!(phys.contains("TopK"), "SortExec must run as TopK:\n{phys}");
    }

    /// Sort keys that reference an expensive projection output must be
    /// inlined below the sort, and the query still plannable.
    #[tokio::test]
    async fn inlines_expensive_sort_key() {
        let sql = "SELECT upper(name) AS u, concat(id, name) FROM t ORDER BY u DESC LIMIT 3";
        let after = optimized_plan(sql, true).await;
        let sort_pos = after.find("Sort:").expect(&after);
        assert!(after.rfind("concat").unwrap() < sort_pos, "concat must be deferred:\n{after}");
    }

    /// No fetch → no rewrite (nothing to win without a TopK).
    #[tokio::test]
    async fn leaves_unfetched_sort_alone() {
        let sql = "SELECT concat(id, name) FROM t ORDER BY timestamp DESC";
        let after = optimized_plan(sql, true).await;
        let sort_pos = after.find("Sort:").expect(&after);
        assert!(after.rfind("concat").unwrap() > sort_pos, "plain sort should be untouched:\n{after}");
    }
}

// Keep `DedupExec` fed by an order-PRESERVING merge, not an order-erasing coalesce.
//
// `DedupExec` requires `Distribution::SinglePartition` and declares the ordering
// keep-greatest depends on, so the planner normally satisfies both at once with a
// `SortPreservingMergeExec` over the mem ∪ hot ∪ delta union.
//
// It stops doing that as soon as a pushed predicate pins the sort column to a constant.
// `timestamp = '...'` (a log-item detail lookup by primary key) makes DataFusion infer
// `timestamp` is constant on that leg, `ordering_satisfy` then reports the requirement
// *trivially* satisfied — a constant column is ordered under any direction — EnforceSorting
// drops the merge as redundant, and EnforceDistribution supplies the single partition with a
// `CoalescePartitionsExec` instead.
//
// That reasoning is sound for a logical ordering and wrong for this operator. `DedupExec`
// does not consume ordering as a property; it consumes the *physical run structure* the merge
// produces — every version of a key arriving contiguously. A coalesce interleaves partitions
// arbitrarily, so `detect_bound` finds no ordering, the operator falls back to `full-set`
// mode and buffers the whole scan.
//
// Measured on prod (project 28f62f01, 2026-08-15): the equality lookup planned
// `CoalescePartitionsExec` → `mode=full-set` and died on
// `unordered merge-on-read dedup exceeded its 2048 MiB per-query limit`, 500ing every log-item
// detail panel opened from a `target_event` link. The identical query written as a
// one-microsecond half-open range — same selectivity, but not constant-inferable — planned
// `SortPreservingMergeExec` → `mode=bounded[timestamp]` and answered in 113ms.
//
// So the rule runs after EnforceSorting/EnforceDistribution have had their say and puts the
// merge back wherever a `DedupExec` that needs ordering is fed by a coalesce. It is not a
// pessimisation: the branches underneath are already ordered (that is why the coalesce was
// legal), so this is a k-way merge of sorted streams, not a sort.

use datafusion::{common::tree_node::TransformedResult, physical_plan::coalesce_partitions::CoalescePartitionsExec};

use crate::read::DedupExec;

#[derive(Debug)]
pub struct DedupNeedsOrderedInput;

impl PhysicalOptimizerRule for DedupNeedsOrderedInput {
    fn name(&self) -> &str {
        "DedupNeedsOrderedInput"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|node| {
            let Some(dedup) = downcast::<DedupExec>(node.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            // No declared ordering means keep-greatest is dormant (the table has no
            // `version_append`); the operator is ordering-agnostic and a coalesce is fine.
            let Some(req) = dedup.required_ordering().cloned() else {
                return Ok(Transformed::no(node));
            };
            let child = Arc::clone(node.children()[0]);
            let Some(coalesce) = downcast::<CoalescePartitionsExec>(child.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            let merged = Arc::new(SortPreservingMergeExec::new(req, Arc::clone(coalesce.children()[0]))) as Arc<dyn ExecutionPlan>;
            Ok(Transformed::yes(node.with_new_children(vec![merged])?))
        })
        .data()
    }
}

#[cfg(test)]
mod dedup_needs_ordered_input_tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::{
        arrow::{
            array::{Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        datasource::{memory::MemorySourceConfig, source::DataSourceExec},
        physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column as PhysColumn},
    };

    fn ts_ordering() -> LexOrdering {
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(PhysColumn::new("ts", 0)), SortOptions { descending: true, nulls_first: false })]).unwrap()
    }

    /// Two ordered partitions, so a coalesce over them is legal but interleaving.
    fn ordered_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false), Field::new("id", DataType::Int64, false)]));
        let batch = |a: i64| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![a])), Arc::new(Int64Array::from(vec![a]))]).unwrap();
        let cfg = MemorySourceConfig::try_new(&[vec![batch(2)], vec![batch(1)]], schema, None).unwrap().try_with_sort_information(vec![ts_ordering()]).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(cfg)))
    }

    fn dedup_over(child: Arc<dyn ExecutionPlan>, requiring: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DedupExec::with_tiebreak(child, vec!["ts".into(), "id".into()], None, None).unwrap().requiring(requiring))
    }

    fn child_name(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{:?}", plan.children()[0]).split_whitespace().next().unwrap_or_default().to_string()
    }

    #[test]
    fn restores_the_merge_a_constant_sort_column_let_the_planner_discharge() {
        // The shape a `timestamp = '...'` point lookup reaches execute with: DedupExec needs
        // run structure, but its input is a coalesce that interleaves partitions, so
        // `detect_bound` finds no ordering and the operator buffers the whole scan.
        let coalesced = Arc::new(CoalescePartitionsExec::new(ordered_source())) as Arc<dyn ExecutionPlan>;
        let plan = dedup_over(coalesced, Some(ts_ordering()));
        assert!(child_name(&plan).contains("CoalescePartitionsExec"), "precondition");

        let fixed = DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap();

        assert!(child_name(&fixed).contains("SortPreservingMergeExec"), "coalesce must become an order-preserving merge, got: {}", child_name(&fixed));
    }

    #[test]
    fn leaves_an_ordering_agnostic_dedup_alone() {
        // No declared ordering = keep-greatest dormant (no `version_append`). Forcing a merge
        // there would charge every scan of such a table for a property nothing consumes.
        let coalesced = Arc::new(CoalescePartitionsExec::new(ordered_source())) as Arc<dyn ExecutionPlan>;
        let plan = dedup_over(coalesced, None);

        let out = DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap();

        assert!(child_name(&out).contains("CoalescePartitionsExec"), "must stay a coalesce, got: {}", child_name(&out));
    }

    #[test]
    fn leaves_an_already_merged_dedup_alone() {
        let merged = Arc::new(SortPreservingMergeExec::new(ts_ordering(), ordered_source())) as Arc<dyn ExecutionPlan>;
        let plan = dedup_over(merged, Some(ts_ordering()));

        let out = DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap();

        assert!(child_name(&out).contains("SortPreservingMergeExec"));
    }
}
