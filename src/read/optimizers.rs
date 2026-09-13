use datafusion::{
    logical_expr::{
        BinaryExpr, Expr, Operator,
        expr::{Cast, TryCast},
        utils::split_conjunction,
    },
    prelude::col,
    scalar::ScalarValue,
};

/// Avoids the competing `as_any` methods in this crate's trait scope.
pub fn downcast<T: 'static>(any: &dyn std::any::Any) -> Option<&T> {
    any.downcast_ref()
}

/// Borrows any UTF-8 scalar representation; `None` for NULL and non-string scalars.
fn utf8_scalar(v: &ScalarValue) -> Option<&String> {
    match v {
        ScalarValue::Utf8(s) | ScalarValue::Utf8View(s) | ScalarValue::LargeUtf8(s) => s.as_ref(),
        _ => None,
    }
}

/// Extracts any UTF-8 scalar representation.
pub fn extract_utf8_string(v: &ScalarValue) -> Option<String> {
    utf8_scalar(v).cloned()
}

/// Strips coercion casts that otherwise hide columns and literals from pruning.
fn peel_casts(expr: &Expr) -> &Expr {
    match expr {
        Expr::Cast(Cast { expr, .. }) | Expr::TryCast(TryCast { expr, .. }) => peel_casts(expr),
        other => other,
    }
}

/// Matches a column through coercion casts.
pub fn is_col_through_cast(expr: &Expr, name: &str) -> bool {
    matches!(peel_casts(expr), Expr::Column(c) if c.name == name)
}

/// Removes coercion casts that otherwise hide literals from pruning.
pub fn unwrap_literal(expr: &Expr) -> Option<&ScalarValue> {
    match peel_casts(expr) {
        Expr::Literal(scalar, _) => Some(scalar),
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
        let date_filters: Vec<Expr> = split_conjunction(&predicate).into_iter().flat_map(|e| timestamp_to_date_filters(e, time_column)).collect();
        date_filters.into_iter().fold(predicate, Expr::and)
    }

    /// Collects date bounds from an AND tree for pruning diagnostics.
    pub fn extract_date_bounds(expr: &Expr) -> Vec<(Operator, i32)> {
        split_conjunction(expr)
            .into_iter()
            .filter_map(|e| match e {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(ScalarValue::Date32(Some(day)), _)) if c.name == "date" => Some((*op, *day)),
                    _ => None,
                },
                _ => None,
            })
            .collect()
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

    /// Recognises `project_id = 'x'` (either argument order) and AND-conjuncts
    /// containing one. OR is intentionally NOT handled: the multi-tenant guard
    /// must stay strict rather than silently scan all projects.
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

    /// 2024-01-01T00:00:00Z in micros → day 19_723.
    const START: i64 = 1_704_067_200_000_000;
    /// 2024-01-03T00:00:00Z in micros → day 19_725.
    const END: i64 = 1_704_240_000_000_000;

    fn cmp(left: Expr, op: Operator, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
    }

    fn ns_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
    }

    fn cast_ns(expr: Expr) -> Expr {
        Expr::Cast(Cast::new(Box::new(expr), ns_type()))
    }

    fn try_cast_ns(expr: Expr) -> Expr {
        Expr::TryCast(TryCast::new(Box::new(expr), ns_type()))
    }

    fn scalar(v: ScalarValue) -> Expr {
        Expr::Literal(v, None)
    }

    #[test_case::test_case(Expr::Between(Between::new(Box::new(col("timestamp")), false, Box::new(timestamp(START)), Box::new(timestamp(END))))
        => vec![(Operator::GtEq, 19_723), (Operator::LtEq, 19_725)] ; "between derives two inclusive date bounds")]
    #[test_case::test_case(cmp(col("timestamp"), Operator::GtEq, timestamp(START)) => vec![(Operator::GtEq, 19_723)] ; "col >= micros literal")]
    #[test_case::test_case(cmp(timestamp(START), Operator::LtEq, col("timestamp")) => vec![(Operator::GtEq, 19_723)] ; "reversed operands: literal <= col")]
    #[test_case::test_case(cmp(cast_ns(col("timestamp")), Operator::Lt, scalar(ScalarValue::TimestampNanosecond(Some(START * 1_000), Some("UTC".into()))))
        => vec![(Operator::LtEq, 19_723)] ; "cast column, nanosecond literal, strict < widens to <=")]
    #[test_case::test_case(cmp(try_cast_ns(col("timestamp")), Operator::Gt, timestamp(START)) => vec![(Operator::GtEq, 19_723)] ; "try_cast column, strict > widens to >=")]
    #[test_case::test_case(cmp(col("timestamp"), Operator::Eq, scalar(ScalarValue::TimestampMillisecond(Some(START / 1_000), Some("UTC".into()))))
        => vec![(Operator::Eq, 19_723)] ; "millisecond literal equality")]
    #[test_case::test_case(cmp(col("timestamp"), Operator::Eq, scalar(ScalarValue::TimestampSecond(Some(START / 1_000_000), Some("UTC".into()))))
        => vec![(Operator::Eq, 19_723)] ; "second literal equality")]
    // Extended-protocol param binding wraps the bound in a `Cast(Literal)`.
    #[test_case::test_case(cmp(col("timestamp"), Operator::GtEq, cast_ns(timestamp(START)))
        => vec![(Operator::GtEq, 19_723)] ; "2026-07-20: cast-wrapped timestamp literal still derives date bounds")]
    fn timestamp_predicates_derive_date_bounds(expr: Expr) -> Vec<(Operator, i32)> {
        date_filters(expr)
    }

    /// A `project_id/timestamp` predicate must gain `date` partition bounds.
    #[test]
    fn monoscope_update_predicate_gains_date_partition_bounds() {
        let predicate = col("project_id")
            .eq(scalar(ScalarValue::Utf8(Some("p".into()))))
            .and(cmp(col("timestamp"), Operator::GtEq, timestamp(START)))
            .and(cmp(col("timestamp"), Operator::Lt, timestamp(END)));

        assert!(extract_date_bounds(&predicate).is_empty());

        let augmented = with_date_partition_filters(predicate, "timestamp");
        let bounds = {
            let mut b = extract_date_bounds(&augmented); // derivation order isn't part of the contract
            b.sort_by_key(|(_, day)| *day);
            b
        };
        assert_eq!(bounds, vec![(Operator::GtEq, 19_723), (Operator::LtEq, 19_725)]);

        // No time-column bounds → predicate returned untouched.
        let no_ts = col("project_id").eq(scalar(ScalarValue::Utf8(Some("p".into()))));
        assert!(extract_date_bounds(&with_date_partition_filters(no_ts, "timestamp")).is_empty());
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

/// Wraps Utf8 expressions going into Variant columns with `json_to_variant()`:
/// DataFusion's type checker rejects Utf8 -> Variant(Struct) casts outright.
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

/// Rewrites only the immediate child of the Dml node: `variant_indices` are
/// positions in `dml.input.schema()` and valid for that plan only.
fn rewrite_input_for_variant(input: &LogicalPlan, variant: &HashSet<usize>) -> Result<Option<LogicalPlan>> {
    match input {
        LogicalPlan::Values(v) => Ok(v.values.iter().any(|row| needs_wrap(row, variant)).then(|| {
            let udf = json_to_variant_udf();
            LogicalPlan::Values(Values { schema: v.schema.clone(), values: v.values.iter().map(|row| wrap_variant_exprs(row, variant, &udf)).collect() })
        })),
        LogicalPlan::Projection(p) => needs_wrap(&p.expr, variant)
            .then(|| Projection::try_new(wrap_variant_exprs(&p.expr, variant, &json_to_variant_udf()), p.input.clone()).map(LogicalPlan::Projection))
            .transpose(),
        // `INSERT … SELECT` shapes get no wrapping; fail at plan time.
        other => Err(DataFusionError::Plan(format!(
            "INSERT into Variant column from input shape `{}` is not supported. \
             Use INSERT … VALUES, or add an explicit `json_to_variant(col)` in the SELECT projection.",
            other.display()
        ))),
    }
}

fn call_udf(func: &Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction { func: func.clone(), args })
}

/// Shared by the probe and the rewrite below — the two must never disagree.
fn should_wrap(i: usize, e: &Expr, variant: &HashSet<usize>) -> bool {
    variant.contains(&i) && is_utf8_expr(e)
}

fn needs_wrap(exprs: &[Expr], variant: &HashSet<usize>) -> bool {
    exprs.iter().enumerate().any(|(i, e)| should_wrap(i, e, variant))
}

fn wrap_variant_exprs(exprs: &[Expr], variant: &HashSet<usize>, udf: &Arc<ScalarUDF>) -> Vec<Expr> {
    exprs.iter().enumerate().map(|(i, e)| if should_wrap(i, e, variant) { call_udf(udf, vec![e.clone()]) } else { e.clone() }).collect()
}

/// Matches *literal* Utf8 only (and casts thereof), not column references.
fn is_utf8_expr(expr: &Expr) -> bool {
    match expr {
        // NULL literals must pass through: json_to_variant would try to parse "" and fail.
        Expr::Literal(v, _) => utf8_scalar(v).is_some(),
        Expr::Cast(cast) => is_utf8_expr(&cast.expr),
        _ => false,
    }
}

// Variant-aware SELECT-plan post-processing (non-DML plans only):
//
// 1. TableScan schema patch — `ProjectRoutingTable::schema()` reports Variant
//    columns as `Utf8View` for the INSERT-VALUES type checker; SELECT plans need
//    the real Struct{Binary,Binary} type restored.
// 2. Root-projection JSON wrap — only the *outermost* Projection is wrapped in
//    `variant_to_json`, so intermediate operators keep binary Variant.

use std::collections::HashMap;

use datafusion::{
    arrow::datatypes::{DataType, Field, TimeUnit},
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
        // DML input scans are VariantInsertRewriter's; re-typing them here would
        // mismatch the writer's expected Utf8 input.
        if matches!(plan, LogicalPlan::Dml(_)) {
            return Ok(plan);
        }
        let patched = restore_variant_scan_types(plan)?.data;
        wrap_root_projection(patched)
    }
}

/// Restores the real Variant `Struct{Binary,Binary}` type on every TableScan's
/// `projected_schema`, lowers Variant-in-text-position exprs to
/// `variant_to_json`, and recomputes cached schemas bottom-up so the restored
/// type propagates. `Transformed::yes` iff at least one scan was re-typed.
pub(crate) fn restore_variant_scan_types(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut changed = false;
    let out = plan
        .transform_up(|node| {
            let patched = patch_table_scan(node)?;
            changed |= patched.transformed;
            // Once a scan below is Variant-typed this node's exprs may face
            // `Struct op Utf8`; coerce, then recompute so the type propagates.
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

/// Re-applies `restore_variant_scan_types` after DataFusion's `optimize_projections`
/// rebuilds each `TableScan` and reverts Variant → Utf8View, which would break the
/// physical planner's logical/physical schema-equality assert. Must be registered
/// LAST so it runs after `optimize_projections` in each optimizer pass.
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
        // DML input scans are VariantInsertRewriter's; re-typing them here would
        // mismatch the writer.
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
    let Some(routing) = scan.source.downcast_ref::<DefaultTableSource>().and_then(|src| src.table_provider.downcast_ref::<ProjectRoutingTable>()) else {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    };
    // Fast path: no Utf8View columns projected → no Variant columns to restore.
    let lying_schema = scan.projected_schema.as_arrow();
    if !lying_schema.fields().iter().any(|f| matches!(f.data_type(), DataType::Utf8View)) {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }

    let real = routing.real_schema();
    let variant_by_name: HashMap<&str, &Arc<Field>> = real.fields().iter().filter(|f| is_variant_type(f.data_type())).map(|f| (f.name().as_str(), f)).collect();
    if !lying_schema.fields().iter().any(|f| variant_by_name.contains_key(f.name().as_str())) {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }
    // Restore only the Variant *type*, with EMPTY field metadata — NOT
    // `Arc::clone(real_field)`, whose `ARROW:extension:name` marker the physical
    // scan does not emit, failing the schema-equality assert. Read-time Variant
    // detection is structural, so the marker is not needed.
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

/// Lowers a Variant operand in a scalar-text position (comparison, regex,
/// `LIKE`/`SIMILAR TO`, `CAST(… AS text)`, `IN (str, …)`) to canonical JSON text
/// via `variant_to_json`: DataFusion cannot coerce `Struct op Utf8`, and the
/// result matches Postgres `jsonb::text`. `->`/`->>` are lowered elsewhere
/// (`VariantAwareExprPlanner`) and are not touched here.
fn coerce_variant_value_positions(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Merge every input schema (joins have several) so column refs resolve to
    // their now Variant-restored types.
    let schema = plan.inputs().iter().fold(DFSchema::empty(), |mut acc, input| {
        acc.merge(input.schema().as_ref());
        acc
    });
    if !schema.fields().iter().any(|f| is_variant_type(f.data_type())) {
        return Ok(plan);
    }
    let to_json = variant_to_json_udf();
    plan.map_expressions(|expr| expr.transform_up(|e| coerce_expr(e, &schema, &to_json))).map(|t| t.data)
}

/// Wraps any Variant operand in a scalar-text position with `variant_to_json`.
/// Idempotent — an already-wrapped operand types as `Utf8`.
fn coerce_expr(e: Expr, schema: &DFSchema, to_json: &Arc<ScalarUDF>) -> Result<Transformed<Expr>> {
    let wrap = |x: Expr| call_udf(to_json, vec![x]);
    // Not struct-update (`..l`): that would read a partially moved `l`.
    let wrap_like = |mut l: Like| {
        l.expr = Box::new(wrap(*l.expr));
        l
    };
    match e {
        // The outer cast is kept so the result describes over the wire as real
        // text (pg OID 25), not variant_to_json's jsonb output (OID 3802).
        Expr::Cast(Cast { expr, field }) if is_text_type(field.data_type()) && is_variant_expr(&expr, schema) => {
            Ok(Transformed::yes(Expr::Cast(Cast { expr: Box::new(wrap(*expr)), field })))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right })
            if is_text_comparison_op(op) && (is_variant_expr(&left, schema) || is_variant_expr(&right, schema)) =>
        {
            let left = if is_variant_expr(&left, schema) { Box::new(wrap(*left)) } else { left };
            let right = if is_variant_expr(&right, schema) { Box::new(wrap(*right)) } else { right };
            Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr { left, op, right })))
        }
        Expr::Like(l) if is_variant_expr(&l.expr, schema) => Ok(Transformed::yes(Expr::Like(wrap_like(l)))),
        Expr::SimilarTo(l) if is_variant_expr(&l.expr, schema) => Ok(Transformed::yes(Expr::SimilarTo(wrap_like(l)))),
        Expr::InList(InList { expr, list, negated }) if is_variant_expr(&expr, schema) => {
            Ok(Transformed::yes(Expr::InList(InList { expr: Box::new(wrap(*expr)), list, negated })))
        }
        other => Ok(Transformed::no(other)),
    }
}

fn is_text_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
}

/// Deliberately enumerated (not `Operator::is_comparison_operator()`): exactly
/// the set that puts a Variant operand in a scalar-text position.
fn is_text_comparison_op(op: Operator) -> bool {
    use Operator::{
        Eq, Gt, GtEq, ILikeMatch, IsDistinctFrom, IsNotDistinctFrom, LikeMatch, Lt, LtEq, NotEq, NotILikeMatch, NotLikeMatch, RegexIMatch, RegexMatch,
        RegexNotIMatch, RegexNotMatch,
    };
    matches!(op, Eq | NotEq | Lt | LtEq | Gt | GtEq | IsDistinctFrom | IsNotDistinctFrom)
        || matches!(op, RegexMatch | RegexIMatch | RegexNotMatch | RegexNotIMatch | LikeMatch | ILikeMatch | NotLikeMatch | NotILikeMatch)
}

/// Peel Sort / Limit / Distinct / SubqueryAlias from the root and wrap the
/// underlying Projection's Variant-typed expressions with `variant_to_json()`.
fn wrap_root_projection(plan: LogicalPlan) -> Result<LogicalPlan> {
    // Bounds the recursion so a deeply nested plan cannot overflow the stack.
    const MAX_PEEL: u16 = 256;
    fn peel(plan: LogicalPlan, depth: u16) -> Result<LogicalPlan> {
        if depth >= MAX_PEEL {
            warn!(
                target: "variant_select_rewriter",
                max_peel = MAX_PEEL,
                "wrap_root_projection hit MAX_PEEL — deeply nested Sort/Limit/Distinct/SubqueryAlias chain; Variant root wrapping skipped"
            );
            return Ok(plan);
        }
        let d = depth + 1;
        let down = |input: Arc<LogicalPlan>| -> Result<Arc<LogicalPlan>> { Ok(Arc::new(peel(Arc::unwrap_or_clone(input), d)?)) };
        macro_rules! descend {
            ($rebuild:expr, $node:expr) => {{
                let mut node = $node;
                node.input = down(node.input)?;
                Ok($rebuild(node))
            }};
        }
        match plan {
            LogicalPlan::Sort(s) => descend!(LogicalPlan::Sort, s),
            LogicalPlan::Limit(l) => descend!(LogicalPlan::Limit, l),
            LogicalPlan::SubqueryAlias(s) => descend!(LogicalPlan::SubqueryAlias, s),
            // Some rewrite passes promote a Filter above the outermost Projection.
            LogicalPlan::Filter(f) => descend!(LogicalPlan::Filter, f),
            LogicalPlan::Distinct(Distinct::All(input)) => Ok(LogicalPlan::Distinct(Distinct::All(down(input)?))),
            LogicalPlan::Distinct(Distinct::On(on)) => descend!(|on| LogicalPlan::Distinct(Distinct::On(on)), on),
            LogicalPlan::Projection(proj) => wrap_projection(proj),
            // Un-peelable (Union/Aggregate/Join/Window/…): wrap above instead.
            other => add_root_variant_projection(other),
        }
    }
    peel(plan, 0)
}

/// Adds a top-level Projection wrapping every Variant-typed output column with
/// `variant_to_json`, for plan shapes that can't be peeled into. Non-Variant
/// columns pass through bare so names and qualifiers are unchanged.
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
    // Idempotency guard. Matched by concrete UDF type, not by name: a rename or a
    // same-named UDF would silently break a string check.
    !matches!(expr, Expr::ScalarFunction(sf) if sf.func.inner().downcast_ref::<VariantToJsonExtUdf>().is_some())
        && expr.get_type(schema).is_ok_and(|dt| is_variant_type(&dt))
}

fn wrap_with_variant_to_json(expr: &Expr, udf: &Arc<ScalarUDF>) -> Expr {
    let wrap = |inner: Expr| call_udf(udf, vec![inner]);
    match expr {
        // Keep the alias outermost so the output column name is unchanged.
        Expr::Alias(a) => wrap(a.expr.as_ref().clone()).alias(a.name.clone()),
        other => wrap(other.clone()),
    }
}

/// Peephole: `json_as_text(variant_to_json(v), 'k')` → the Variant-native
/// extraction `json_to_pg_text(variant_to_json(variant_get(v, "['k']")))`,
/// avoiding a whole-Variant serialize + re-parse per row.
///
/// Rewritten: an outer `json_as_text` over any chain of `json_get` calls whose
/// keys are all literals and whose base is `variant_to_json`. Deliberately NOT
/// rewritten, each because the result would differ:
/// - a terminal `json_get` (`->`): return type would go JSON-union → Variant.
/// - an intermediate `json_as_text`: it unquotes a JSON *string* leaf, so a
///   further lookup parses that string's contents; `variant_get` would not.
/// - `json_get_str`/`_int`/`_float`/`_bool`/`_json`: different NULL semantics.
/// - a non-literal key: the path must be a constant to become a variant path.
/// - a negative array index: JSON reads it as NULL, `VariantPath` errors.
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
                        // Alias to the original schema name: renaming a projection
                        // output would change the wire column name.
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
    // Walk down the `->` chain, prepending each hop's keys: a json_get over a
    // json_get takes only the container arm of the inner union, which is exactly
    // a nested variant path.
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

    let leaf = call_udf(&variant_get_udf(), vec![variant.clone(), Expr::Literal(ScalarValue::Utf8(Some(build_variant_path(&path))), None)]);
    // `variant_get` cannot stringify numeric/boolean leaves, so reuse the
    // composition `VariantAwareExprPlanner` emits for `->>`.
    Some(call_udf(&json_to_pg_text_udf(), vec![call_udf(&variant_to_json_udf(), vec![leaf])]))
}

#[cfg(test)]
mod variant_json_accessor_tests {
    //! Each case runs the same SQL with and without the peephole and demands
    //! identical rows and schema, plus whether the rewrite was to fire.
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
    /// `variant_to_json(json_to_variant(d))`. Cases run over a column, not a
    /// folded scalar, so the array kernels get compared.
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
    // datafusion-functions-json's own rewriter (registered here, not in TF's
    // session) folds a chained ->> into ONE multi-key json_as_text first.
    #[test_case::test_case(r#"{"a":{"b":"c"}}"#, "->>'a'->>'b'", true ; "chained long arrows, unnested upstream")]
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

    /// TF's session omits `ApplyFunctionRewrites`, so a chained
    /// `variant_to_json(v)->'a'->0->>'b'` stays a NESTED `json_get` tower. Walks
    /// the descent loop and pins the path encoding.
    #[test]
    fn the_nested_json_get_tower_prod_emits_folds_into_one_path() {
        // Exactly what JsonExprPlanner emits, aliases and all; `::` binds tighter
        // than `->>`, so the last key arrives wrapped in a Cast.
        let level1 =
            call_udf(&datafusion_functions_json::udfs::json_get_udf(), vec![call_udf(&variant_to_json_udf(), vec![col("v")]), lit("a")]).alias("v -> 'a'");
        let level2 = call_udf(&datafusion_functions_json::udfs::json_get_udf(), vec![level1, lit(0i64)]).alias("v -> 'a' -> 0");
        let cast_key = Expr::Cast(Cast::new(Box::new(lit("b")), DataType::Utf8));
        let tower = call_udf(&datafusion_functions_json::udfs::json_as_text_udf(), vec![level2, cast_key]);

        let native = super::variant_native_extraction(&tower).expect("the nested tower is the shape this rule exists for");
        assert!(native.to_string().contains(r"['a'][0]['b']"), "one bracket-quoted path, not dot notation: {native}");
    }

    /// Shapes the peephole must refuse, including ones the SQL cases cannot
    /// reach because DataFusion's JSON rewriter normalizes them away first.
    #[test]
    fn an_excluded_shape_is_left_alone() {
        let as_text = |args: Vec<Expr>| call_udf(&datafusion_functions_json::udfs::json_as_text_udf(), args);
        let variant = call_udf(&variant_to_json_udf(), vec![col("v")]);
        let rewrite = |e: Expr| super::variant_native_extraction(&e);

        assert!(rewrite(as_text(vec![variant.clone(), lit(1i64)])).is_some(), "a non-negative index is rewritten");
        assert!(rewrite(as_text(vec![variant.clone(), lit(-1i64)])).is_none(), "negative index");
        assert!(rewrite(as_text(vec![variant.clone(), col("k")])).is_none(), "non-literal key");
        assert!(rewrite(as_text(vec![as_text(vec![variant.clone(), lit("a")]), lit("b")])).is_none(), "nested json_as_text");
        assert!(rewrite(call_udf(&datafusion_functions_json::udfs::json_get_str_udf(), vec![variant, lit("a")])).is_none(), "json_get_str family");
    }
}

#[cfg(test)]
mod peel_tests {
    //! `wrap_root_projection` peel logic and the MAX_PEEL guard.
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::DFSchema,
        logical_expr::{EmptyRelation, builder::LogicalPlanBuilder, col, lit},
    };

    use super::*;

    fn variant_projection() -> LogicalPlan {
        let variant_field = Field::new(
            "v",
            DataType::Struct(vec![Arc::new(Field::new("metadata", DataType::Binary, false)), Arc::new(Field::new("value", DataType::Binary, false))].into()),
            true,
        )
        .with_metadata(HashMap::from([(crate::schema::VARIANT_EXT_KEY.to_string(), crate::schema::VARIANT_EXT_VALUE.to_string())]));
        let df = Arc::new(DFSchema::try_from(Schema::new(vec![variant_field])).unwrap());
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

    /// Each case stacks nodes on top of the bare variant projection and asks
    /// whether the root projection still ends up wrapped in `variant_to_json`.
    #[test_case::test_case(|b| b => true ; "bare projection is wrapped")]
    #[test_case::test_case(|b| b.filter(lit(true)).unwrap() => true ; "peels Filter")]
    #[test_case::test_case(|b| b.distinct().unwrap() => true ; "peels Distinct")]
    #[test_case::test_case(|b| b.limit(0, Some(10)).unwrap() => true ; "peels Limit")]
    #[test_case::test_case(|b| b.sort(vec![col("v").sort(true, false)]).unwrap() => true ; "peels Sort")]
    #[test_case::test_case(|b| b.alias("a").unwrap() => true ; "peels SubqueryAlias")]
    #[test_case::test_case(|b| b.filter(lit(true)).unwrap().distinct().unwrap().limit(0, Some(10)).unwrap().sort(vec![col("v").sort(true, false)]).unwrap().alias("a").unwrap()
        => true ; "peels sort/limit/distinct/alias/filter stacked together")]
    fn peels_to_the_root_projection(steps: fn(LogicalPlanBuilder) -> LogicalPlanBuilder) -> bool {
        let plan = steps(LogicalPlanBuilder::from(variant_projection())).build().unwrap();
        is_variant_to_json_call(first_projection_expr(&analyze(plan)))
    }

    #[test]
    fn idempotent_on_double_analyze() {
        // Running the analyzer twice must not double-wrap.
        let once = analyze(variant_projection());
        let twice = analyze(once.clone());
        let expr_twice = first_projection_expr(&twice);
        assert!(is_variant_to_json_call(expr_twice));
        let Expr::ScalarFunction(sf) = expr_twice else {
            panic!("not a scalar function");
        };
        assert_eq!(sf.args.len(), 1);
        assert!(matches!(sf.args[0], Expr::Column(_)), "second pass nested the call: {:?}", sf.args[0]);
    }

    #[test]
    fn max_peel_short_circuits_on_pathological_depth() {
        // Larger stack: DataFusion's transform_up walk over a 300-deep plan
        // blows the default 2 MiB test stack.
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

// Transparent Tantivy acceleration for standard SQL predicates: LIKE / ILIKE /
// `=` / `IN` / literal-substring regex on tantivy-indexed columns gain an
// additively AND-ed `text_match(col, q)` call, which `ProjectRoutingTable` turns
// into an `id IN (...)` prefilter narrowing the Delta scan.
//
// Correctness invariants:
// 1. The original predicate is preserved verbatim — it is the post-filter for
//    MemBuffer rows and Delta files whose index hasn't built yet.
// 2. Only columns confirmed `tantivy.indexed: true` are rewritten.
// 3. Idempotent under repeated passes.
// 4. Patterns the *target column's tokenizer* can't accelerate are left alone.
// 5. `!=` / `NOT IN` are never routed — negation has no term form. Under `OR`,
//    a node is routable only if every branch is fully covered by a `text_match`.
// 6. Regex routes only for a plain literal substring on a text-typed column:
//    on Variant/List the index holds our own rendering, not `::text` output.

use std::sync::OnceLock;

use datafusion::{
    common::tree_node::TreeNodeRecursion,
    logical_expr::{and, lit, or},
};

use crate::tantivy::{
    DEFAULT_TOKENIZER, NGRAM3_TOKENIZER, RAW_TOKENIZER,
    udf::{NGRAM_MIN_QUERY_LEN, TEXT_MATCH_NAME, TextMatchUdf, classify_like_pattern, is_eq_term_safe, regex_literal_substring},
};

/// Per-column index facts: the resolved tokenizer, and whether the *stored*
/// column is a plain string (the latter gates regex routing).
type IndexedCol = (&'static str, bool);
type IndexedCols = HashMap<String, IndexedCol>;

#[derive(Debug, Default)]
pub struct TantivyPredicateRewriter {
    /// Route exact `=` on raw columns through tantivy (`tantivy.route_equality`).
    /// A field, not a global-config read: the config singleton may be
    /// uninitialized when a `Database` is built from a local config.
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

/// Longest IN-list expanded into an OR of `text_match` calls; beyond this the
/// per-item query cost outweighs the pruning.
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

/// `col IN ('a','b',...)` on a RAW-tokenized column → per-item term queries,
/// under the same gates as exact `=` routing. `NOT IN` is never routed.
fn match_indexed_in_list(expr: &Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Option<(String, Vec<Route>)> {
    let Expr::InList(InList { expr: col, list, negated: false }) = expr else { return None };
    let Expr::Column(c) = col.as_ref() else { return None };
    if !allow_eq || !(1..=MAX_ROUTED_IN_LIST).contains(&list.len()) || indexed_columns.get(&c.name)?.0 != RAW_TOKENIZER {
        return None;
    }
    list.iter().map(eq_term_route).collect::<Option<Vec<_>>>().map(|items| (c.name.clone(), items))
}

/// RHS of a raw-column equality (`=` or one `IN` item) as a route: literals are
/// classified now, placeholders defer to scan time.
fn eq_term_route(rhs: &Expr) -> Option<Route> {
    match rhs {
        Expr::Literal(s, _) => extract_utf8_string(s).filter(|v| !v.is_empty() && v.chars().all(is_eq_term_safe)).map(Route::Ready),
        // Value unknown until Bind; the deferred tag keeps the prefilter in
        // plans cached with placeholders.
        Expr::Placeholder(_) => Some(Route::Deferred { rhs: rhs.clone(), kind: "eq".into() }),
        _ => None,
    }
}

/// How a routed predicate reaches tantivy.
#[derive(Debug, PartialEq)]
enum Route {
    /// Literal classified at plan time → `text_match(col, query)`.
    Ready(String),
    /// `$N` placeholder, emitted as `text_match(col, $N, kind)`; the scan-side
    /// collector classifies it once the literal is known.
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
        call_udf(CELL.get_or_init(|| Arc::new(ScalarUDF::from(TextMatchUdf::default()))), args)
    }
}

/// If `expr` is a rewritable predicate on an indexed column, returns
/// `(column_name, route)`. Raw tokenizers can't do substring; ngram3 can do
/// everything; default is in between.
fn match_indexed_predicate(expr: &Expr, indexed_columns: &IndexedCols, allow_eq: bool) -> Option<(String, Route)> {
    match expr {
        // Exact `col = 'lit'` on a RAW-tokenized column: raw is a single
        // case-sensitive token, so the tantivy match set equals the `=` match set.
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
            // without a parallel case-insensitive index. Other tokenizers
            // lowercase both index and query side, so ILIKE needs no extra work.
            if *case_insensitive && tok == RAW_TOKENIZER {
                return None;
            }
            let route = match r.as_ref() {
                Expr::Literal(s, _) => classify_like_pattern(utf8_scalar(s)?, *escape_char, tok == NGRAM3_TOKENIZER)
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
        // `col ~ 'substr'` / `~*`, optionally through a string cast, as an ngram3
        // substring query: ngram3 lowercases + ASCII-folds both sides, so its hit
        // set is a superset of both. Only PLAIN substrings route.
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::RegexMatch | Operator::RegexIMatch, right }) => {
            let c = column_through_string_cast(left)?;
            let (tok, text_typed) = *indexed_columns.get(&c.name)?;
            let Expr::Literal(s, _) = right.as_ref() else { return None };
            (tok == NGRAM3_TOKENIZER && text_typed)
                .then(|| regex_literal_substring(utf8_scalar(s)?).filter(|q| q.chars().count() >= NGRAM_MIN_QUERY_LEN))
                .flatten()
                .map(|q| (c.name.clone(), Route::Ready(q)))
        }
        _ => None,
    }
}

/// The column under zero or more string casts, which are value-preserving for
/// string types (non-string sources are rejected by the `text_typed` gate).
fn column_through_string_cast(e: &Expr) -> Option<&Column> {
    match e {
        Expr::Column(c) => Some(c),
        Expr::Cast(Cast { expr, field }) | Expr::TryCast(TryCast { expr, field }) if is_text_type(field.data_type()) => column_through_string_cast(expr),
        _ => None,
    }
}

/// Indexed columns of the first TableScan below `plan` that has a tantivy index.
/// Cross-table joins on indexed columns are not supported — each filter is
/// rewritten relative to its own subtree's scan.
fn scanned_indexed_columns(plan: &LogicalPlan) -> Option<&'static IndexedCols> {
    if let LogicalPlan::TableScan(ts) = plan
        && let Some(cols) = indexed_columns_for(ts.table_name.table())
    {
        return Some(cols);
    }
    plan.inputs().into_iter().find_map(scanned_indexed_columns)
}

/// Indexed columns for a table from the static schema registry; `None` when the
/// table isn't in it. The cache is populated once, which is only valid because
/// the registry is compiled-in YAML — runtime reload would need invalidation.
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
                            if cfg.list_mode == crate::schema::TantivyListMode::Elements {
                                return None; // exact membership has a separate route
                            }
                            let tok = match cfg.tokenizer.as_deref() {
                                Some(RAW_TOKENIZER) => RAW_TOKENIZER,
                                Some(DEFAULT_TOKENIZER) => DEFAULT_TOKENIZER,
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
    use test_case::test_case;

    /// `(pattern, escape, allow_substring)` → routed query. `_` and embedded `%`
    /// never accelerate; leading `%` only on ngram3 (`allow_substring`).
    #[test_case("foo", None, false => Some("foo".to_string()) ; "bare literal is a term")]
    #[test_case("foo%", None, false => Some("foo*".to_string()) ; "trailing % is a prefix query")]
    #[test_case("%foo", None, false => None ; "leading % without substring support")]
    #[test_case("%foo", None, true => Some("foo".to_string()) ; "leading % on ngram3")]
    #[test_case("%foo%", None, true => Some("foo".to_string()) ; "both-sided % on ngram3")]
    #[test_case("%foo%", None, false => None ; "both-sided % without substring support")]
    #[test_case("fo%o", None, true => None ; "embedded % never accelerates, ngram3")]
    #[test_case("fo%o", None, false => None ; "embedded % never accelerates, raw")]
    #[test_case("fo_", None, true => None ; "_ never accelerates")]
    #[test_case("foo+bar", None, true => None ; "query-syntax metachar")]
    #[test_case("svc.user-api", None, false => Some("svc.user-api".to_string()) ; "dots and dashes survive")]
    #[test_case("foo\\%", Some('\\'), false => None ; "escaped metachar: bail conservatively")]
    fn like_classifier_cases(pat: &str, esc: Option<char>, substring: bool) -> Option<String> {
        classify_like_pattern(pat, esc, substring)
    }

    const UID: &str = "0fee13b9-ac71-5c55-acd1-109542595054";

    /// `tid`/`sid` raw-tokenized, `name` ngram3, all text-typed; `body` is ngram3
    /// but NOT text-typed (a Variant/List column). `unindexed` is absent.
    fn cols() -> IndexedCols {
        [("tid", RAW_TOKENIZER, true), ("sid", RAW_TOKENIZER, true), ("name", NGRAM3_TOKENIZER, true), ("body", NGRAM3_TOKENIZER, false)]
            .into_iter()
            .map(|(k, tok, text)| (k.to_string(), (tok, text)))
            .collect()
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    fn eq(c: &str, val: &str) -> Expr {
        re(c, Operator::Eq, val, false)
    }

    fn ilike(c: &str, pat: &str) -> Expr {
        Expr::Like(Like { negated: false, expr: Box::new(col(c)), pattern: Box::new(lit(pat)), escape_char: None, case_insensitive: true })
    }

    /// `col::text <op> 'pat'` when `cast`, else the bare column.
    fn re(c: &str, op: Operator, pat: &str, cast: bool) -> Expr {
        let lhs = if cast { Expr::Cast(Cast::new(Box::new(col(c)), DataType::Utf8)) } else { col(c) };
        Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), op, Box::new(lit(pat))))
    }

    fn in_list(c: &str, items: &[&str], negated: bool) -> Expr {
        Expr::InList(InList { expr: Box::new(col(c)), list: items.iter().map(|s| lit(*s)).collect(), negated })
    }

    fn ready(c: &str, q: &str) -> Option<(String, Route)> {
        Some((c.to_string(), Route::Ready(q.to_string())))
    }

    /// Exact `=` routes only on a raw column; every other shape (ngram3, `!=`,
    /// flag off, literals the QueryParser would mis-handle) falls back to `=`.
    #[test_case(eq("tid", "d01762b88f4ed54d"), true => ready("tid", "d01762b88f4ed54d") ; "raw column + flag on routes as a term")]
    #[test_case(eq("tid", UID), true => ready("tid", UID) ; "dashed uuid: the `-` survives (e2e-proven)")]
    #[test_case(eq("tid", "abc123"), false => None ; "flag off reverts to bloom/stats")]
    #[test_case(eq("name", "runServer"), true => None ; "ngram3 is lossy for equality")]
    #[test_case(re("tid", Operator::NotEq, "abc", false), true => None ; "`!=` has no term form")]
    #[test_case(eq("tid", "a:b"), true => None ; "colon is query syntax")]
    #[test_case(eq("tid", "foo bar"), true => None ; "space, AND-split can't match one raw token")]
    #[test_case(eq("tid", "a.b"), true => None ; "dot conservatively excluded")]
    #[test_case(eq("tid", ""), true => None ; "empty literal")]
    // ILIKE on a raw (case-sensitive) column would silently miss case variants.
    #[test_case(ilike("tid", "foo"), true => None ; "ILIKE never routes on a raw column")]
    #[test_case(ilike("name", "%foo%"), true => ready("name", "foo") ; "ILIKE substring routes on ngram3")]
    #[test_case(re("name", Operator::RegexIMatch, "runServer", true), true => ready("name", "runServer") ; "cast-wrapped regex imatch on ngram3 routes")]
    #[test_case(re("name", Operator::RegexIMatch, "runServer", false), true => ready("name", "runServer") ; "bare column regex imatch routes (DataFusion may fold the no-op cast)")]
    #[test_case(re("name", Operator::RegexMatch, "runServer", true), true => ready("name", "runServer") ; "cast-wrapped regex match on ngram3 routes")]
    #[test_case(re("name", Operator::RegexMatch, "runServer", false), true => ready("name", "runServer") ; "bare column regex match routes")]
    #[test_case(re("name", Operator::RegexIMatch, "svc\\.user-api", true), true => ready("name", "svc.user-api") ; "escapeRegex output: `\\.` decodes to a literal")]
    // Unescaped metachars / anchors / short patterns / raw / non-text: never routed.
    #[test_case(re("name", Operator::RegexIMatch, "run.*", true), true => None ; "unescaped metachar")]
    #[test_case(re("name", Operator::RegexIMatch, "a|b", true), true => None ; "alternation")]
    #[test_case(re("name", Operator::RegexIMatch, "^foo", true), true => None ; "startswith anchor")]
    #[test_case(re("name", Operator::RegexIMatch, "foo$", true), true => None ; "endswith anchor")]
    #[test_case(re("name", Operator::RegexIMatch, "fo(o)", true), true => None ; "group")]
    #[test_case(re("name", Operator::RegexIMatch, "\\yword\\y", true), true => None ; "backslash-y is a word boundary, not an escaped literal")]
    #[test_case(re("name", Operator::RegexIMatch, "ab", true), true => None ; "shorter than NGRAM_MIN_QUERY_LEN")]
    #[test_case(re("name", Operator::RegexIMatch, "", true), true => None ; "empty pattern")]
    #[test_case(re("name", Operator::RegexIMatch, "a\\", true), true => None ; "trailing backslash")]
    #[test_case(re("tid", Operator::RegexIMatch, "abcdef", true), true => None ; "regex never routes on a raw column")]
    // Variant/List (not text-typed): the index holds our own rendering, which
    // `::text` need not reproduce.
    #[test_case(re("body", Operator::RegexIMatch, "boom", true), true => None ; "non-text column never routes")]
    fn match_indexed_predicate_cases(expr: Expr, allow_eq: bool) -> Option<(String, Route)> {
        match_indexed_predicate(&expr, &cols(), allow_eq)
    }

    /// OR-safety: both branches routed → an Or node; one unroutable branch →
    /// NO prefilter at all (`none`).
    #[test_case(or(eq("tid", "x"), eq("sid", "y")), true => "or(tid:x,sid:y)" ; "both routed branches must union")]
    #[test_case(or(eq("tid", "x"), eq("unindexed", "y")), true => "none" ; "an OR with an unroutable branch must not seed the prefilter")]
    #[test_case(and(eq("tid", "x"), lit(true)), true => "tid:x" ; "a top-level conjunct still routes")]
    #[test_case(in_list("tid", &["a", "b"], false), true => "or(tid:a,tid:b)" ; "IN-list routes as an Or of terms")]
    #[test_case(in_list("tid", &["a"], true), true => "none" ; "NOT IN is never routed")]
    #[test_case(in_list("name", &["abc"], false), true => "none" ; "IN-list on an ngram3 column")]
    #[test_case(in_list("tid", &["a:b"], false), true => "none" ; "IN-list with a QueryParser-unsafe literal")]
    #[test_case(in_list("tid", &["a"], false), false => "none" ; "IN-list with the flag off")]
    fn routed_tree_cases(expr: Expr, allow_eq: bool) -> String {
        fn go(n: &PredNode) -> String {
            let kids = |k: &[PredNode]| k.iter().map(go).collect::<Vec<_>>().join(",");
            match n {
                PredNode::Leaf(p) => format!("{}:{}", p.column, p.query),
                PredNode::And(k) => format!("and({})", kids(k)),
                PredNode::Or(k) => format!("or({})", kids(k)),
            }
        }
        collect_text_match_tree(&[expr.transform_down(|x| rewrite_expr(x, &cols(), allow_eq)).unwrap().data]).as_ref().map_or("none".to_string(), go)
    }
}

// Rewrite Postgres array literals (`'{}'`, `'{a,b}'`) into typed list literals
// where an array type is expected, e.g. `COALESCE(hashes, '{}')`. Must run
// before `TypeCoercion`. Only string element types are handled.

use std::mem::take;

use datafusion::common::{plan_err, tree_node::TreeNodeIterator};

/// `coalesce` wrapper whose coercion additionally unifies string args into a
/// sibling list type, because the SQL planner computes projection schemas (→
/// `coerce_types`) BEFORE analyzer rules run. Registered under the built-in's
/// name, shadowing it session-wide; every method must delegate to the inner
/// built-in, and a DataFusion upgrade needs new methods forwarded here too.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PgCoalesceUdf {
    inner: Arc<datafusion::logical_expr::ScalarUDF>,
}

/// DataFusion version `PgCoalesceUdf`'s method forwarding was last audited
/// against. A datafusion bump breaks the build here on purpose: re-audit
/// `ScalarUDFImpl` for new methods, forward them above, then bump this.
const AUDITED_DATAFUSION_VERSION: &str = "54.1.0";
// Byte loop because `&str` equality isn't const-callable on stable.
const _: () = {
    let (a, b) = (datafusion::DATAFUSION_VERSION.as_bytes(), AUDITED_DATAFUSION_VERSION.as_bytes());
    assert!(a.len() == b.len(), "DataFusion bumped: re-audit PgCoalesceUdf's ScalarUDFImpl forwarding, then update AUDITED_DATAFUSION_VERSION");
    let mut i = 0;
    while i < a.len() {
        assert!(a[i] == b[i], "DataFusion bumped: re-audit PgCoalesceUdf's ScalarUDFImpl forwarding, then update AUDITED_DATAFUSION_VERSION");
        i += 1;
    }
};

/// Element type of any list-shaped Arrow type; `None` for non-list types.
fn list_elem_type(t: &DataType) -> Option<&DataType> {
    match t {
        DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f.data_type()),
        _ => None,
    }
}

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
    fn conditional_arguments<'a>(&self, args: &'a [Expr]) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        self.inner.inner().conditional_arguments(args)
    }
    fn short_circuits(&self) -> bool {
        self.inner.inner().short_circuits()
    }
    fn documentation(&self) -> Option<&datafusion::logical_expr::Documentation> {
        self.inner.inner().documentation()
    }
    fn simplify(
        &self, args: Vec<Expr>, info: &datafusion::logical_expr::simplify::SimplifyContext,
    ) -> Result<datafusion::logical_expr::simplify::ExprSimplifyResult> {
        self.inner.inner().simplify(args, info)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types).or_else(|e| {
            // Only after the built-in coercion failed: promote EVERY string arg
            // to the sibling list type; the analyzer rule below rewrites the
            // literals and rejects any string arg it can't rewrite.
            let list_t = arg_types.iter().find(|t| list_elem_type(t).is_some()).ok_or(e)?.clone();
            let patched: Vec<DataType> = arg_types.iter().map(|t| if is_text_type(t) { list_t.clone() } else { t.clone() }).collect();
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
    // their exprs are typed by their own schema instead.
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
    let Some(elem_type) = args.iter().find_map(|a| input_schemas.iter().find_map(|s| list_elem_type(&a.get_type(s.as_ref()).ok()?).cloned())) else {
        return no(args);
    };

    let Transformed { data: new_args, transformed, .. } = args.into_iter().map_until_stop_and_collect(|a| {
        Ok(match pg_list_literal(&a, &elem_type) {
            Some(list) => Transformed::yes(Expr::Literal(list, None)),
            None => Transformed::no(a),
        })
    })?;
    // A still-string arg must be rejected here: arrow-cast's blanket `(_, List)`
    // rule would silently wrap each value in a single-element list instead.
    if let Some(bad) = new_args.iter().find_map(|a| input_schemas.iter().find_map(|s| a.get_type(s.as_ref()).ok().filter(is_text_type))) {
        return plan_err!("COALESCE types {bad} and List({elem_type}) cannot be matched");
    }
    Ok(Transformed::new_transformed(Expr::ScalarFunction(ScalarFunction { func, args: new_args }), transformed))
}

/// The typed list literal a string arg denotes as a PG array literal, or None
/// if it isn't one or an element doesn't parse as `elem_type` (caller then
/// leaves the arg alone). Always emits `ScalarValue::List`; TypeCoercion casts
/// it onward to LargeList/FixedSizeList.
fn pg_list_literal(arg: &Expr, elem_type: &DataType) -> Option<ScalarValue> {
    let Expr::Literal(v, _) = arg else { return None };
    let vals: Vec<ScalarValue> = parse_pg_string_array(utf8_scalar(v)?)?
        .into_iter()
        .map(|e| e.map_or_else(|| ScalarValue::try_from(elem_type).ok(), |s| ScalarValue::try_from_string(s, elem_type).ok()))
        .collect::<Option<_>>()?;
    Some(ScalarValue::List(ScalarValue::new_list_nullable(&vals, elem_type)))
}

/// Parse a PG array literal of strings: `{}`, `{a,b}`, `{"a,b",NULL}`.
/// None if `s` isn't brace-wrapped, or contains unquoted nested braces
/// (multi-dimensional arrays are unsupported). Malformed quoting parses
/// leniently. Only the bare `NULL` keyword is a null element.
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

    #[test_case::test_case("SELECT cardinality(COALESCE(CAST(NULL AS VARCHAR[]), '{}')) AS n FROM (SELECT 1)", "| 0 " ; "coalesce_empty_pg_array_literal")]
    #[test_case::test_case("SELECT COALESCE(CAST(NULL AS VARCHAR[]), '{a, b, \"c,d\", NULL}') AS v FROM (SELECT 1)", "[a, b, c,d, ]" ; "coalesce_nonempty_pg_array_literal")]
    #[test_case::test_case("SELECT COALESCE(CAST(NULL AS VARCHAR), '{}') AS v FROM (SELECT 1)", "{}" ; "non_array_string_untouched")]
    #[tokio::test]
    async fn coalesce_evaluates_to(sql: &str, want: &str) {
        let batches = ctx_with_rule().sql(sql).await.expect("plan ok").collect().await.expect("exec ok");
        let out = datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string();
        assert!(out.contains(want), "{sql}\n{out}");
    }

    // Without the explicit guard these would silently return `[value]`.
    #[test_case::test_case("SELECT COALESCE(v, l) FROM (SELECT CAST('x' AS VARCHAR) AS v, CAST(NULL AS VARCHAR[]) AS l)" ; "coalesce_string_column_with_list_errors")]
    #[test_case::test_case("SELECT COALESCE(CAST(NULL AS VARCHAR[]), 'not-an-array') FROM (SELECT 1)" ; "coalesce_unparseable_literal_with_list_errors")]
    #[tokio::test]
    async fn coalesce_with_list_arg_errors(sql: &str) {
        // The error may surface at sql() or collect(); either is acceptable.
        let ctx = ctx_with_rule();
        let err = match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => df.collect().await.expect_err("query must fail planning").to_string(),
        };
        assert!(err.contains("cannot be matched"), "{sql}\n{err}");
    }

    // Leaf-node path in rewrite_in_plan: a TableScan carrying a pushed-down
    // coalesce filter has no inputs.
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

    fn elems(v: &[Option<&str>]) -> Option<Vec<Option<String>>> {
        Some(v.iter().map(|e| e.map(str::to_string)).collect())
    }

    #[test_case::test_case("{}" => elems(&[]) ; "empty literal")]
    #[test_case::test_case("{a,b}" => elems(&[Some("a"), Some("b")]) ; "bare elements")]
    #[test_case::test_case(r#"{"a,b", c }"# => elems(&[Some("a,b"), Some("c")]) ; "quoted comma, unquoted element trimmed")]
    #[test_case::test_case("{NULL,\"NULL\"}" => elems(&[None, Some("NULL")]) ; "unquoted NULL is null, quoted NULL is text")]
    // PG rejects these literals; we parse leniently. Pinned as intentional.
    #[test_case::test_case("{\"a\"x,b}" => elems(&[Some("a"), Some("b")]) ; "trailing junk after a closing quote is dropped")]
    #[test_case::test_case("{a\"b\"}" => elems(&[Some("ab")]) ; "text before an opening quote is concatenated")]
    #[test_case::test_case("{\"a\\}" => None ; "lone trailing backslash")]
    #[test_case::test_case("{{a},{b}}" => None ; "multi-dimensional literal rejected")]
    #[test_case::test_case(r#"{"{x}",y}"# => elems(&[Some("{x}"), Some("y")]) ; "braces inside quotes are element text")]
    #[test_case::test_case("plain" => None ; "not brace-delimited")]
    fn pg_array_parse_shapes(s: &str) -> Option<Vec<Option<String>>> {
        parse_pg_string_array(s)
    }
}

// Order the unordered branches of a routed MemBuffer∪Delta union so an
// `ORDER BY <sort-keys> LIMIT n` becomes a streaming TopK instead of a blocking
// sort. A union is order-preserving only when EVERY child shares the ordering,
// so the unsatisfying children are wrapped in `SortExec(req).with_fetch(n)`.
// Must run before `EnforceDistribution`/`EnforceSorting`.

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

/// `node` with its only child replaced, or `node` untouched when `child` is None.
fn swap_child(node: Arc<dyn ExecutionPlan>, child: Option<Arc<dyn ExecutionPlan>>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    match child {
        Some(child) => Ok(Transformed::yes(node.with_new_children(vec![child])?)),
        None => Ok(Transformed::no(node)),
    }
}

#[derive(Debug, Default)]
pub struct OrderedUnionForTopK;

/// Re-express `req` as plain columns resolved by NAME against `schema`, so an
/// intervening projection cannot misalign the sort-key indices. `None` if any
/// key is not a plain column or is absent from `schema`.
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

/// `children` with every child that does not already satisfy `req` wrapped in
/// `SortExec(req)` (carrying `fetch`), so a union over them advertises `req`.
///
/// `Ok(None)` means "leave the plan alone":
/// - every child already satisfies `req`, or
/// - a child that doesn't satisfy it is marked unsortable — `sortable[i] ==
///   false` means a blocking sort on that leg costs more than the ordering buys
///   (indices past `sortable`'s end are sortable), or
/// - `require_ordered_child` and no child is ordered.
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
            // rather than serialising the leg through a CoalescePartitionsExec.
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
/// operators to the first `UnionExec`; if mixed, sort the unsatisfying children
/// so the union becomes order-preserving. `None` when nothing applied.
fn order_union(plan: &Arc<dyn ExecutionPlan>, req: &LexOrdering, fetch: Option<usize>) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(union) = downcast::<UnionExec>(plan.as_ref()) {
        let Some(req_here) = resolve_ordering(req, &union.schema()) else {
            return Ok(None);
        };
        // Mixed unions only: an ordered child to merge toward AND one to fix.
        let children: Vec<Arc<dyn ExecutionPlan>> = union.children().into_iter().cloned().collect();
        return ordered_children(&children, &req_here, fetch, &[], true)?.map(UnionExec::try_new).transpose();
    }
    // Never descend through a DedupExec: its survivors are decided across ALL
    // input rows, so a `with_fetch` cut below it can drop a row's newer version.
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
                let Some((req, fetch)) = downcast::<SortExec>(node.as_ref())
                    .map(|s| (s.expr().clone(), s.fetch()))
                    .or_else(|| downcast::<SortPreservingMergeExec>(node.as_ref()).map(|m| (m.expr().clone(), m.fetch())))
                    .filter(|(_, fetch)| fetch.is_some())
                else {
                    return Ok(Transformed::no(node));
                };
                let children = node.children();
                let rewritten = children.first().copied().map(|input| order_union(input, &req, fetch)).transpose()?.flatten();
                swap_child(node, rewritten)
            })?
            .data)
    }
}

/// Applied to a Delta leg as it leaves `provider.scan`: the delta-rs fork
/// isolates footer-less files into a sibling scan with no ordering claim, which
/// costs the whole union its ordering. Sorts each isolated unordered child whose
/// selected bytes fit `max_bytes`, restoring the claim. `0` disables it.
///
/// Not a `PhysicalOptimizerRule` because `DedupExec` declares no required input
/// ordering, so a sort injected before `EnforceSorting` is deleted as unused.
///
/// No fetch is pushed: a top-n cut on a leg under `DedupExec` would truncate
/// row versions.
pub(crate) fn repair_isolated_scan_ordering(plan: Arc<dyn ExecutionPlan>, max_bytes: u64) -> Result<Arc<dyn ExecutionPlan>> {
    // Bottom-up so a rewritten union's new ordering propagates through the
    // `DeltaScanExec` wrapping it as the parents are rebuilt.
    Ok(plan
        .transform_up(|node| {
            let Some(union) = downcast::<UnionExec>(node.as_ref()) else {
                return Ok(Transformed::no(node));
            };
            let children: Vec<Arc<dyn ExecutionPlan>> = union.children().into_iter().cloned().collect();
            // Only the fork's isolation shape: every child a bare file scan.
            let Some(sizes) = children.iter().map(crate::database::selected_file_work).collect::<Option<Vec<_>>>() else {
                return Ok(Transformed::no(node));
            };
            let Some(req) = children.iter().find_map(|c| c.properties().output_ordering()).cloned() else {
                // NO child declares an ordering: nothing to give back.
                metrics::counter!(crate::database::scan_metric_names::ORDERING_REPAIR_NO_CLAIM).increment(1);
                return Ok(Transformed::no(node));
            };
            // `sortable` IS the byte budget: sorting a whole-window parquet leg OOMs.
            let sortable: Vec<bool> = sizes.iter().map(|(_, bytes)| *bytes <= max_bytes).collect();
            Ok(match ordered_children(&children, &req, None, &sortable, true)? {
                Some(ordered) => {
                    metrics::counter!(crate::database::scan_metric_names::ORDERING_REPAIR_APPLIED).increment(1);
                    Transformed::yes(UnionExec::try_new(ordered)?)
                }
                None => {
                    // Declining costs the query its streaming top-N; the logged
                    // over-budget bytes are what
                    // `timefusion_read_sort_unordered_leg_max_mb` is set against.
                    let over: u64 = sizes.iter().map(|(_, bytes)| *bytes).filter(|b| *b > max_bytes).sum();
                    metrics::counter!(crate::database::scan_metric_names::ORDERING_REPAIR_DECLINED).increment(1);
                    if crate::observability::sample_rollup_miss("ordering_repair_declined") {
                        tracing::warn!(
                            children = children.len(),
                            unsortable = sortable.iter().filter(|s| !**s).count(),
                            over_budget_bytes = over,
                            max_bytes,
                            event = "ordering_repair_declined",
                            "the isolated leg exceeds the sort budget, so the union loses its ordering claim and ORDER BY ... LIMIT becomes a blocking sort"
                        );
                    }
                    Transformed::no(node)
                }
            })
        })?
        .data)
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

    /// A leaf exec that declares whatever output ordering the test wants.
    #[derive(Debug)]
    struct MockLeaf {
        props: Arc<PlanProperties>,
    }

    impl MockLeaf {
        fn leaf(schema: SchemaRef, ordering: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
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
    /// advertises `[ts DESC]` only when `delta_ordered`.
    fn fetching_sort_over_union(delta_ordered: bool, fetch: Option<usize>) -> (Arc<dyn ExecutionPlan>, LexOrdering) {
        let (s, ord) = (schema(), ts_desc());
        let union = UnionExec::try_new(vec![MockLeaf::leaf(s.clone(), None), MockLeaf::leaf(s, delta_ordered.then(|| ord.clone()))]).unwrap();
        (Arc::new(SortExec::new(ord.clone(), union).with_fetch(fetch)), ord)
    }

    /// Runs `ordered_children` over one `MockLeaf` per `ordered` flag and
    /// reports, per leg, how many `SortExec`s the rewrite left; `None` when it
    /// declined to rewrite at all.
    fn ordered_children_sorts(ordered: &[bool], sortable: &[bool], require_ordered_child: bool) -> Option<Vec<usize>> {
        let (s, ord) = (schema(), ts_desc());
        let legs: Vec<Arc<dyn ExecutionPlan>> = ordered.iter().map(|o| MockLeaf::leaf(s.clone(), o.then(|| ord.clone()))).collect();
        ordered_children(&legs, &ord, None, sortable, require_ordered_child).unwrap().map(|out| out.iter().map(count_sorts).collect())
    }

    // Top sort + one injected mem sort = 2 SortExecs.
    #[test_case::test_case(true, Some(50) => 2 ; "exactly one SortExec injected over the unordered mem branch")]
    #[test_case::test_case(true, None => 1 ; "no injection when there is no fetch")]
    #[test_case::test_case(false, Some(50) => 1 ; "no injection when neither branch is ordered")]
    fn topk_rule_sorts_mem_branch_only_when_it_can_merge(delta_ordered: bool, fetch: Option<usize>) -> usize {
        let (top, ord) = fetching_sort_over_union(delta_ordered, fetch);
        let out = OrderedUnionForTopK.optimize(top, &ConfigOptions::new()).unwrap();
        let sorts = count_sorts(&out);
        if sorts == 2 {
            assert!(
                out.children()[0].properties().equivalence_properties().ordering_satisfy(ord.iter().cloned()).unwrap(),
                "union must advertise the sort ordering after the rule runs"
            );
        }
        sorts
    }

    // `ordered_children` is also called directly by `ProjectRoutingTable::scan`,
    // with `sortable` marking the cheap in-memory legs.
    #[test_case::test_case(&[false, true], &[true, false], false => Some(vec![1, 0]) ; "mixed union: only the mem leg is sorted")]
    #[test_case::test_case(&[false, false], &[true, false], false => None ; "an unsortable unordered leg aborts the whole rewrite")]
    #[test_case::test_case(&[false], &[true], false => Some(vec![1]) ; "a lone sortable leg is still sorted")]
    #[test_case::test_case(&[false], &[true], true => None ; "require_ordered_child declines a lone leg with no ordered peer")]
    fn ordered_children_honours_sortable_and_lone_leg(ordered: &[bool], sortable: &[bool], require_ordered_child: bool) -> Option<Vec<usize>> {
        ordered_children_sorts(ordered, sortable, require_ordered_child)
    }
}

// Rewrites `row_to_json(t)` — a bare relation alias standing for a whole row —
// into `row_to_json(named_struct('c1', t.c1, …))`. Operates on the AST, not the
// plan: DataFusion rejects the bare alias while PLANNING the SQL, before any
// analyzer rule can see it. Column names come from the derived table's own
// SELECT aliases; a relation without them is left alone rather than guessed at.

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
    /// reached by matching on `query.body` alone.
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
        let relations: Vec<(String, Vec<String>)> = select.from.iter().filter_map(|from| derived_columns(&from.relation)).collect();
        if relations.is_empty() {
            return;
        }
        for item in &mut select.projection {
            if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
                self.rewrite_expr(expr, &relations);
            }
        }
    }

    fn rewrite_expr(&mut self, expr: &mut SqlExpr, relations: &[(String, Vec<String>)]) {
        let SqlExpr::Function(function) = expr else { return };
        if !is_row_to_json(&function.name) {
            return;
        }
        // A schema-qualified UDF name does not resolve in DataFusion.
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
    name.0.last().is_some_and(|part| matches!(part, ObjectNamePart::Identifier(ident) if ident.value.eq_ignore_ascii_case("row_to_json")))
}

/// `named_struct('total', t."total", …)`, preserving declared column order.
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
/// is a derived table with an alias and every projected column has a name.
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

    /// `Some(rewritten SQL)` when the rule fired, `None` when it declined.
    fn rewrite_sql(sql: &str) -> Option<String> {
        let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parses");
        rewrite(&mut statements[0]).then(|| statements[0].to_string())
    }

    #[test_case::test_case(r#"SELECT row_to_json(t) FROM (SELECT 1 AS "total", 2 AS "active") t"#,
        &[r#"named_struct('total', t."total", 'active', t."active")"#], &[] ; "bare alias becomes named_struct in declared order")]
    #[test_case::test_case(r#"SELECT pg_catalog.row_to_json(t) FROM (SELECT 1 AS "a") t"#,
        &["named_struct('a', t.\"a\")"], &["pg_catalog.row_to_json"] ; "qualified pg_catalog call is rewritten")]
    #[test_case::test_case(r#"SELECT 'a' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT 1 AS "Total") t
               UNION ALL
               SELECT 'b' AS chart_name, pg_catalog.row_to_json(t) AS chart_data FROM (SELECT 2 AS "Active") t"#,
        &[r#"named_struct('Total', t."Total")"#, r#"named_struct('Active', t."Active")"#], &["row_to_json(t)"] ; "every union branch is rewritten")]
    fn rewrites_record_calls(sql: &str, wants: &[&str], forbids: &[&str]) {
        let out = rewrite_sql(sql).unwrap_or_else(|| panic!("expected a rewrite for: {sql}"));
        for want in wants {
            assert!(out.contains(want), "missing {want}: {out}");
        }
        for forbid in forbids {
            assert!(!out.contains(forbid), "must not contain {forbid}: {out}");
        }
    }

    #[test_case::test_case("SELECT row_to_json(t) FROM (SELECT count(*), 1 AS b) t" => None ; "unaliased derived column is left alone")]
    #[test_case::test_case("SELECT row_to_json(payload) FROM events" => None ; "non-relation identifier is left alone")]
    // A real table alias is not a derived table: its columns are not in the AST.
    #[test_case::test_case("SELECT row_to_json(t) FROM some_table t" => None ; "plain table alias is left alone")]
    fn declines_to_rewrite(sql: &str) -> Option<String> {
        rewrite_sql(sql)
    }
}

// Rewrites `EXISTS(q)` in a projection to `(SELECT count(1) FROM q) > 0`:
// DataFusion decorrelates EXISTS only in filter position, but correlated
// *scalar* subqueries in a projection ARE decorrelated.

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
        plan.transform_up(|plan| {
            let LogicalPlan::Projection(projection) = plan else { return Ok(Transformed::no(plan)) };
            // `Alias` nodes must survive untouched or the projection's output
            // column names change.
            let Transformed { data, transformed, .. } = projection.expr.clone().into_iter().map_until_stop_and_collect(|expr| {
                expr.transform_up(|expr| match expr {
                    Expr::Exists(exists) => Ok(Transformed::yes(match count_subquery(exists.subquery)? {
                        Some(count) if exists.negated => count.eq(lit(0_i64)),
                        Some(count) => count.gt(lit(0_i64)),
                        // Provably empty subquery: EXISTS is a constant.
                        None => lit(exists.negated),
                    })),
                    other => Ok(Transformed::no(other)),
                })
            })?;
            if !transformed {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
            LogicalPlanBuilder::from(Arc::unwrap_or_clone(projection.input)).project(data)?.build().map(Transformed::yes)
        })
        .map(|transformed| transformed.data)
    }
}

/// `q` → scalar subquery `SELECT count(1) FROM q`, keeping the outer references
/// so DataFusion still sees it as correlated. `None` means `q` is provably empty.
fn count_subquery(subquery: Subquery) -> Result<Option<Expr>> {
    let Subquery { subquery: plan, outer_ref_columns, spans } = subquery;
    let Some(plan) = peel_row_caps(Arc::unwrap_or_clone(plan)) else {
        return Ok(None);
    };
    let counted = LogicalPlanBuilder::from(plan).aggregate(Vec::<Expr>::new(), vec![count(lit(1_i64))])?.build()?;
    Ok(Some(Expr::ScalarSubquery(Subquery { subquery: Arc::new(counted), outer_ref_columns, spans })))
}

/// Strips `LIMIT n` from the top of an EXISTS subquery; a row cap cannot change
/// whether *some* row exists, and DataFusion will not decorrelate a scalar
/// subquery containing a Limit. A non-zero `OFFSET` is NOT stripped — dropping
/// it would change the answer. Returns `None` for `LIMIT 0`.
fn peel_row_caps(plan: LogicalPlan) -> Option<LogicalPlan> {
    match plan {
        // A non-zero OFFSET is not peelable, so it falls through to `other`.
        LogicalPlan::Limit(limit) if limit.skip.as_deref().is_none_or(|skip| literal_count(skip) == Some(0)) => {
            if limit.fetch.as_deref().and_then(literal_count) == Some(0) {
                return None; // LIMIT 0 can never produce a row
            }
            peel_row_caps(Arc::unwrap_or_clone(limit.input))
        }
        other => Some(other),
    }
}

fn literal_count(expr: &Expr) -> Option<i64> {
    let Expr::Literal(value, ..) = expr else { return None };
    match value.cast_to(&DataType::Int64).ok()? {
        ScalarValue::Int64(count) => count,
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

    #[test_case::test_case("SELECT id, EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id LIMIT 1) AS present FROM outer_t ORDER BY id"
        => Ok(vec![true, false]) ; "correlated exists in a projection plans and evaluates: id=1 has a match, id=2 does not")]
    #[test_case::test_case("SELECT id, NOT EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id) AS absent FROM outer_t ORDER BY id"
        => Ok(vec![false, true]) ; "negated exists inverts: id=1 matches so absent=false; id=2 absent=true")]
    // `LIMIT 1 OFFSET 5` asks whether at least SIX rows exist: never peel an offset.
    #[test_case::test_case("SELECT id, EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id LIMIT 1 OFFSET 5) AS present FROM outer_t"
        => matches Err(_) ; "offset is not peeled and is never answered wrongly: an offset EXISTS must error rather than return a wrong answer")]
    #[test_case::test_case("SELECT id, EXISTS (SELECT 1 FROM inner_t WHERE fk = outer_t.id LIMIT 0) AS present FROM outer_t"
        => Ok(vec![false, false]) ; "limit zero folds to false")]
    #[tokio::test]
    async fn exists_in_a_projection(sql: &str) -> std::result::Result<Vec<bool>, String> {
        let batches = ctx().await.sql(sql).await.expect("logical planning must succeed").collect().await.map_err(|e| e.to_string())?;
        Ok(batches.iter().flat_map(|batch| datafusion::arrow::array::AsArray::as_boolean(batch.column(1)).values().iter().collect::<Vec<_>>()).collect())
    }
}

// Expand `qualifier.*` inside scalar function arguments into the explicit column
// list, as Postgres does. Must run before `TypeCoercion`, which rejects a typeless
// wildcard. Only `ScalarFunction` args and only QUALIFIED wildcards: bare `f(*)`
// has different semantics (`count(*)`) and is left alone.

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
    // The wildcard's qualifier must resolve against one of the direct inputs.
    let input_schemas: Vec<_> = plan.inputs().iter().map(|i| i.schema().clone()).collect();
    if input_schemas.is_empty() {
        return Ok(Transformed::no(plan));
    }
    // No recompute_schema() needed: expanding a variadic call's args does not
    // change the projection's output type.
    plan.map_expressions(|expr| expr.transform_up(|e| expand_in_expr(e, &input_schemas)))
}

#[allow(deprecated)] // Expr::Wildcard is still the variant the SQL planner emits
fn expand_in_expr(expr: Expr, input_schemas: &[Arc<DFSchema>]) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return Ok(Transformed::no(expr));
    };
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
/// duplicate qualifier names in one scope, so first-match-wins is unambiguous.
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
        // WildcardFnArgExpander must come before TypeCoercion.
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

    /// First row of the single output column.
    async fn first_json(sql: &str) -> String {
        let batches = ctx_with_rule().sql(sql).await.expect("plan ok").collect().await.expect("exec ok");
        batches[0].column(0).as_any().downcast_ref::<StringViewArray>().expect("StringViewArray").value(0).to_string()
    }

    #[test_case::test_case("SELECT jsonb_build_array(sub.*) FROM (SELECT 1 AS a, 'x' AS b, true AS c) sub"
        => r#"[1,"x",true]"#.to_string() ; "qualified wildcard expands in declared column order")]
    #[test_case::test_case("SELECT jsonb_build_array(a.*, b.*) FROM (SELECT 1 AS x, 2 AS y) a CROSS JOIN (SELECT 'p' AS p, 'q' AS q) b"
        => r#"[1,2,"p","q"]"#.to_string() ; "multiple qualifiers in one call concatenate in argument order")]
    #[test_case::test_case("SELECT jsonb_build_array(0, sub.*, 99) FROM (SELECT 1 AS a, 2 AS b) sub"
        => "[0,1,2,99]".to_string() ; "mixes wildcard with other args, positions preserved")]
    #[test_case::test_case("SELECT jsonb_build_array(jsonb_build_array(sub.*)) FROM (SELECT 1 AS a, 2 AS b) sub"
        => "[[1,2]]".to_string() ; "nested function calls expand inside out")]
    #[tokio::test]
    async fn expands_wildcard_fn_args(sql: &str) -> String {
        first_json(sql).await
    }

    /// An out-of-scope qualifier is caught by DataFusion's planner before this
    /// rule runs; the rule's own "Unknown qualifier" error is defensive.
    #[tokio::test]
    async fn unknown_qualifier_errors_clearly() {
        let ctx = ctx_with_rule();
        let err = ctx.sql("SELECT jsonb_build_array(sub.*) FROM (SELECT 1 AS a) other").await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Invalid qualifier sub"), "msg: {msg}");
    }
}

// Defer expensive scalar projections past TopK (Sort with fetch), so they are
// evaluated only on the surviving `fetch` rows:
// `Sort(fetch) → Projection(expensive)` becomes
// `Projection(expensive, rebuilt) → Sort(fetch, exprs inlined) → Projection(raw cols)`.
// Must be registered AFTER DataFusion's defaults, so `push_down_limit` has
// already folded LIMIT into `Sort.fetch`.

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

    // Ordered set, so the rewritten plan is deterministic (`column_refs` is not).
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

    const TOPK_SQL: &str = "SELECT concat(id, name) FROM t ORDER BY timestamp DESC LIMIT 5";

    /// True when EVERY `concat` sits above `Sort:` in the optimized logical plan.
    #[test_case::test_case(TOPK_SQL, false => false ; "baseline evaluates concat below Sort, for every row in the window")]
    #[test_case::test_case(TOPK_SQL, true => true ; "defers expensive projection past topk")]
    #[test_case::test_case("SELECT upper(name) AS u, concat(id, name) FROM t ORDER BY u DESC LIMIT 3", true => true ; "inlines expensive sort key")]
    #[test_case::test_case("SELECT concat(id, name) FROM t ORDER BY timestamp DESC", true => false ; "leaves unfetched sort alone")]
    #[tokio::test]
    async fn concat_is_deferred_above_the_sort(sql: &str, with_rule: bool) -> bool {
        let plan = plans(sql, with_rule).await.0;
        let sort = plan.find("Sort:").expect(&plan);
        plan.rfind("concat").expect(&plan) < sort
    }

    /// The deferral must survive physical planning, not just logical.
    #[tokio::test]
    async fn defers_expensive_projection_past_topk() {
        let (after, phys) = plans(TOPK_SQL, true).await;
        assert!(after.contains("fetch=5"), "TopK fetch must survive the rewrite:\n{after}");
        let p_sort = phys.find("SortExec").expect(&phys);
        assert!(phys.find("concat").expect(&phys) < p_sort, "physical plan must keep concat above SortExec:\n{phys}");
        assert!(phys.contains("TopK"), "SortExec must run as TopK:\n{phys}");
    }
}

// Keep `DedupExec` fed by an order-PRESERVING merge, not an order-erasing
// coalesce: it consumes physical run structure (every version of a key arriving
// contiguously), not ordering as a logical property, so a
// `CoalescePartitionsExec` interleaving partitions drops it into unbounded
// `full-set` mode. Runs after EnforceSorting/EnforceDistribution, which may
// discharge the merge when a pushed predicate pins the sort column to a constant.

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
            let merged = downcast::<DedupExec>(node.as_ref())
                // No declared ordering means keep-greatest is dormant; a coalesce is fine.
                .and_then(|dedup| dedup.required_ordering().cloned())
                .and_then(|req| {
                    let child = Arc::clone(node.children()[0]);
                    let coalesce = downcast::<CoalescePartitionsExec>(child.as_ref())?;
                    Some(Arc::new(SortPreservingMergeExec::new(req, Arc::clone(coalesce.children()[0]))) as Arc<dyn ExecutionPlan>)
                });
            swap_child(node, merged)
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

    /// Two ordered partitions, so a coalesce over them is legal but interleaves.
    fn ordered_source() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Int64, false), Field::new("id", DataType::Int64, false)]));
        let batch = |a: i64| RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![a])), Arc::new(Int64Array::from(vec![a]))]).unwrap();
        let cfg = MemorySourceConfig::try_new(&[vec![batch(2)], vec![batch(1)]], schema, None).unwrap().try_with_sort_information(vec![ts_ordering()]).unwrap();
        Arc::new(DataSourceExec::new(Arc::new(cfg)))
    }

    fn dedup_over(child: Arc<dyn ExecutionPlan>, requiring: Option<LexOrdering>) -> Arc<dyn ExecutionPlan> {
        Arc::new(DedupExec::with_tiebreak(child, vec!["ts".into(), "id".into()], None, None).unwrap().requiring(requiring))
    }

    /// Leading alphanumeric run of the child's Debug — the exec's type name.
    fn child_name(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!("{:?}", plan.children()[0]).split(|c: char| !c.is_alphanumeric()).next().unwrap_or_default().to_string()
    }

    #[test_case::test_case(true, true => "SortPreservingMergeExec" ; "restores the merge a constant sort column let the planner discharge")]
    #[test_case::test_case(true, false => "CoalescePartitionsExec" ; "leaves an ordering agnostic dedup alone")]
    #[test_case::test_case(false, true => "SortPreservingMergeExec" ; "leaves an already merged dedup alone")]
    fn rewrites_dedup_input(coalesced: bool, requires_order: bool) -> String {
        let child: Arc<dyn ExecutionPlan> = if coalesced {
            Arc::new(CoalescePartitionsExec::new(ordered_source()))
        } else {
            Arc::new(SortPreservingMergeExec::new(ts_ordering(), ordered_source()))
        };
        let plan = dedup_over(child, requires_order.then(ts_ordering));
        assert_eq!(child_name(&plan), if coalesced { "CoalescePartitionsExec" } else { "SortPreservingMergeExec" }, "precondition");

        child_name(&DedupNeedsOrderedInput.optimize(plan, &ConfigOptions::default()).unwrap())
    }
}

// ===== range_parallel_dedup =====

use datafusion::logical_expr::Filter;

/// Branches a wide aggregate window is split into. `<2` disables the rule.
static RANGE_SPLIT_BRANCHES: OnceLock<usize> = OnceLock::new();

pub fn range_split_branches() -> usize {
    *RANGE_SPLIT_BRANCHES.get_or_init(|| 4)
}

/// Set the range-split branch count. No-op after the first call (OnceLock).
pub fn set_range_split_branches(branches: usize) {
    let _ = RANGE_SPLIT_BRANCHES.set(branches);
}

/// Below this span one thread is comfortably inside the statement timeout, so
/// splitting only adds planning work and re-opens files.
const MIN_SPLIT_SPAN_MICROS: i64 = 8 * 24 * 3_600 * 1_000_000;

/// `DedupExec` declares `Distribution::SinglePartition`, so every row of a window
/// funnels through ONE thread. This rule splits a wide window into N disjoint
/// timestamp ranges, each with its own scan and `DedupExec`, unioned under the
/// original aggregate.
///
/// Exact because `timestamp` is the LEADING dedup key: two versions of a row carry
/// the same timestamp and can never land either side of a boundary. RANGE and not
/// HASH, because hashing would destroy the ordering `DedupExec` needs. Only under
/// an aggregate: a union of ranges advertises no ordering, so this beneath
/// `ORDER BY timestamp DESC LIMIT n` would cost the query its streaming TopK.
#[derive(Debug, Default)]
pub struct RangeParallelDedup;

impl OptimizerRule for RangeParallelDedup {
    fn name(&self) -> &str {
        "range_parallel_dedup"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> Result<Transformed<LogicalPlan>> {
        Ok(split_aggregate(&plan)?.map_or_else(|| Transformed::no(plan), Transformed::yes))
    }
}

/// `None` when the plan is not an aggregate over a window worth splitting.
fn split_aggregate(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let branches = range_split_branches();
    let LogicalPlan::Aggregate(aggregate) = plan else { return Ok(None) };
    // A Union input is this rule's own output. The optimizer runs to a
    // fixpoint, and re-splitting each branch would fan out geometrically.
    if branches < 2 || matches!(aggregate.input.as_ref(), LogicalPlan::Union(_)) {
        return Ok(None);
    }
    let Some((lo, hi)) = splittable_window(&aggregate.input).filter(|&(lo, hi)| hi.saturating_sub(lo) >= MIN_SPLIT_SPAN_MICROS) else {
        return Ok(None);
    };
    let step = (hi - lo) / branches as i64;
    let Some(narrowed) = (0..branches)
        .map(|i| {
            // Half-open [lo, hi) per branch so a row on a boundary belongs to
            // exactly one branch. The last branch takes `hi + 1` because the
            // window `hi` is INCLUSIVE (`<=` folds into it upstream).
            let branch_lo = lo + step * i as i64;
            let branch_hi = if i + 1 == branches { hi.saturating_add(1) } else { lo + step * (i as i64 + 1) };
            narrow_scan_window(&aggregate.input, branch_lo, branch_hi)
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let union = narrowed[1..].iter().try_fold(LogicalPlanBuilder::new(narrowed[0].clone()), |builder, branch| builder.union(branch.clone()))?.build()?;
    // A UNION's output fields are UNQUALIFIED, so a parent `t.id` stops resolving.
    // Re-attach the scan's qualifier; if the schema still differs, DECLINE — the
    // replacement must be indistinguishable to every parent.
    let union = if union.schema() == aggregate.input.schema() {
        union
    } else {
        let Some(qualifier) = scan_qualifier(&aggregate.input) else { return Ok(None) };
        let aliased = LogicalPlanBuilder::new(union).alias(qualifier)?.build()?;
        if aliased.schema() != aggregate.input.schema() {
            return Ok(None);
        }
        aliased
    };
    Ok(Some(LogicalPlan::Aggregate(datafusion::logical_expr::Aggregate::try_new(Arc::new(union), aggregate.group_expr.clone(), aggregate.aggr_expr.clone())?)))
}

/// The `TableScan` under a subtree the split may pass through, collecting the
/// filter predicates crossed on the way down.
///
/// A WHITELIST, deliberately: only nodes that commute with a row filter may be
/// listed. A LIMIT, DISTINCT, JOIN, WINDOW or nested aggregate breaks the
/// rewrite's premise, so an unrecognised node must decline, not miscount.
fn scan_under<'a>(plan: &'a LogicalPlan, predicates: &mut Vec<&'a Expr>) -> Option<&'a TableScan> {
    match plan {
        LogicalPlan::Filter(filter) => {
            predicates.push(&filter.predicate);
            scan_under(&filter.input, predicates)
        }
        LogicalPlan::Projection(projection) => scan_under(&projection.input, predicates),
        LogicalPlan::SubqueryAlias(alias) => scan_under(&alias.input, predicates),
        LogicalPlan::TableScan(scan) => Some(scan),
        _ => None,
    }
}

/// The finite timestamp window of a subtree the split may pass through.
fn splittable_window(plan: &LogicalPlan) -> Option<(i64, i64)> {
    let mut predicates = Vec::new();
    let scan = scan_under(plan, &mut predicates)?;
    bounded_window(predicates.into_iter().chain(&scan.filters).flat_map(split_conjunction))
}

/// Both bounds of `timestamp` across `conjuncts`, or `None` if either is open —
/// splitting on a guessed bound would drop every row outside it.
fn bounded_window<'a>(conjuncts: impl IntoIterator<Item = &'a Expr>) -> Option<(i64, i64)> {
    let literal_micros = |expr: &Expr| match expr {
        Expr::Literal(scalar, _) => scalar_micros(scalar),
        _ => None,
    };
    let (lo, hi) = conjuncts.into_iter().fold((None::<i64>, None::<i64>), |acc @ (lo, hi), conjunct| {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = conjunct else { return acc };
        let (bound, op) = if is_col_through_cast(left, "timestamp") {
            (literal_micros(right), *op)
        } else if is_col_through_cast(right, "timestamp") {
            (literal_micros(left), swap_comparison(*op))
        } else {
            return acc;
        };
        let Some(ts) = bound else { return acc };
        match op {
            Operator::Gt | Operator::GtEq => (Some(lo.map_or(ts, |l| l.max(ts))), hi),
            Operator::Lt | Operator::LtEq => (lo, Some(hi.map_or(ts, |h| h.min(ts)))),
            Operator::Eq => (Some(ts), Some(ts)),
            _ => acc,
        }
    });
    lo.zip(hi).filter(|(lo, hi)| lo < hi)
}

/// The scanned table's qualifier, used to restore the qualification a UNION drops.
fn scan_qualifier(plan: &LogicalPlan) -> Option<datafusion::common::TableReference> {
    scan_under(plan, &mut Vec::new()).map(|scan| scan.table_name.clone())
}

/// Rebuild `plan` with `[lo, hi)` pinned directly above its TableScan, so the
/// branch prunes to its own files rather than filtering a full-window scan.
fn narrow_scan_window(plan: &LogicalPlan, lo: i64, hi: i64) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            // Timezone comes from the scanned column, never assumed: a literal in the
            // wrong timezone or TimeUnit silently shifts the boundary.
            let field = scan.projected_schema.field_with_unqualified_name("timestamp").ok()?;
            let timezone = match field.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, timezone) => timezone.clone(),
                _ => return None,
            };
            let literal = |micros| Expr::Literal(ScalarValue::TimestampMicrosecond(Some(micros), timezone.clone()), None);
            let timestamp = Expr::Column(Column::new(Some(scan.table_name.clone()), "timestamp"));
            let (lower, upper) = (timestamp.clone().gt_eq(literal(lo)), timestamp.lt(literal(hi)));
            // The bound must go INTO the scan (this runs after `push_down_filter`,
            // so a Filter here is never folded in and every branch would prune to
            // the same files) AND stay as a Filter node — `TableScan.filters` is
            // only a hint the provider may apply approximately.
            let mut narrowed = scan.clone();
            narrowed.filters.push(lower.clone());
            narrowed.filters.push(upper.clone());
            Filter::try_new(lower.and(upper), Arc::new(LogicalPlan::TableScan(narrowed))).ok().map(LogicalPlan::Filter)
        }
        LogicalPlan::Filter(_) | LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => {
            let child = narrow_scan_window(plan.inputs()[0], lo, hi)?;
            plan.with_new_exprs(plan.expressions(), vec![child]).ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod range_parallel_dedup_tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, TimeUnit},
        datasource::MemTable,
        execution::session_state::SessionStateBuilder,
        prelude::SessionContext,
    };

    use super::*;

    /// `timestamp` is `Timestamp(us, UTC)`; the rule reads the timezone off it.
    async fn optimized(sql: &str, with_rule: bool) -> String {
        let builder = SessionStateBuilder::new().with_default_features();
        let builder = if with_rule { builder.with_optimizer_rule(Arc::new(RangeParallelDedup)) } else { builder };
        let ctx = SessionContext::new_with_state(builder.build());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        ]));
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())).unwrap();
        ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap().display_indent().to_string()
    }

    const WIDE: &str = "timestamp >= '2026-08-05T00:00:00Z'::timestamp AND timestamp <= '2026-09-04T00:00:00Z'::timestamp";

    #[tokio::test]
    async fn splits_a_wide_aggregate_window_into_branches() {
        let plan = optimized(&format!("SELECT count(*) FROM t WHERE {WIDE}"), true).await;
        assert!(plan.contains("Union"), "wide aggregate must split:\n{plan}");
        assert_eq!(plan.matches("TableScan: t").count(), range_split_branches(), "one scan per branch:\n{plan}");
        assert!(plan.matches("timestamp >=").count() >= range_split_branches(), "each branch needs its own lower bound:\n{plan}");
    }

    /// Every shape here breaks "rows of sub-range A == the window's rows restricted
    /// to A", or has nothing to gain, so each must come back byte-identical to the
    /// un-ruled plan. `{W}` is the wide window; the rest is the shape under test.
    #[test_case::test_case("SELECT count(*) FROM (SELECT * FROM t WHERE {W} LIMIT 100)" ; "limit under the aggregate")]
    #[test_case::test_case("SELECT id FROM t WHERE {W} ORDER BY timestamp DESC LIMIT 5" ; "order by with limit")]
    #[test_case::test_case("SELECT count(*) FROM t WHERE timestamp >= '2026-09-01T00:00:00Z'::timestamp AND timestamp <= '2026-09-04T00:00:00Z'::timestamp" ; "span below the threshold")]
    #[test_case::test_case("SELECT count(*) FROM t WHERE timestamp >= '2026-08-05T00:00:00Z'::timestamp" ; "half-open window")]
    #[tokio::test]
    async fn declines_where_a_split_would_change_the_answer(sql: &str) {
        let sql = sql.replace("{W}", WIDE);
        assert_eq!(optimized(&sql, true).await, optimized(&sql, false).await, "must not rewrite");
    }

    /// A UNION emits UNQUALIFIED fields, so a parent `t.id` would stop resolving.
    /// `SELECT DISTINCT` is itself an `Aggregate`, hence the shape used here.
    #[tokio::test]
    async fn split_preserves_the_scan_qualifier_a_union_would_drop() {
        let plan = optimized(&format!("SELECT count(*) FROM (SELECT DISTINCT id FROM t WHERE {WIDE})"), true).await;
        assert!(plan.contains("Union"), "distinct over a wide window should still split:\n{plan}");
        assert!(plan.contains("SubqueryAlias: t"), "the union must be re-qualified as `t`:\n{plan}");
        assert!(plan.contains("groupBy=[[t.id]]"), "the parent must still bind `t.id`:\n{plan}");
    }

    /// Branches must tile `[lo, hi]` with no gap (drops rows) and no overlap
    /// (double-counts).
    #[tokio::test]
    async fn branch_bounds_tile_the_window_exactly() {
        let plan = optimized(&format!("SELECT count(*) FROM t WHERE {WIDE}"), true).await;
        let bounds: Vec<(i64, i64)> = plan
            .lines()
            .filter(|line| line.trim_start().starts_with("Filter:"))
            .filter_map(|line| {
                let mut it = line
                    .match_indices("TimestampMicrosecond(")
                    .map(|(i, _)| line[i + "TimestampMicrosecond(".len()..].split(',').next().unwrap_or_default().parse::<i64>().unwrap_or_default());
                Some((it.next()?, it.next()?))
            })
            .collect();
        assert_eq!(bounds.len(), range_split_branches(), "one [lo, hi) per branch:\n{plan}");
        for (i, window) in bounds.windows(2).enumerate() {
            assert_eq!(window[0].1, window[1].0, "branch {i} must end exactly where branch {} begins:\n{plan}", i + 1);
        }
        assert!(bounds[0].1 > bounds[0].0, "each branch must be non-empty:\n{plan}");
    }
}

/// Hides the declared input ordering from an `AggregateExec` fed through a
/// `DedupExec`: Parquet files can advertise sorted row groups without being
/// sorted ACROSS row-group boundaries, so closing groups early would undercount.
/// Scans and LIMITs are left alone.
#[derive(Debug)]
pub struct AggregateInputOrdering;

#[derive(Debug)]
struct UnorderedAggregateInput {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<datafusion::physical_plan::PlanProperties>,
}

impl UnorderedAggregateInput {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let mut properties = input.properties().as_ref().clone();
        properties.eq_properties.clear_orderings();
        Self { input, properties: Arc::new(properties) }
    }
}

impl datafusion::physical_plan::DisplayAs for UnorderedAggregateInput {
    fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "UnorderedAggregateInput")
    }
}

impl ExecutionPlan for UnorderedAggregateInput {
    fn name(&self) -> &'static str {
        "UnorderedAggregateInput"
    }
    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        let [input]: [Arc<dyn ExecutionPlan>; 1] =
            children.try_into().map_err(|_| datafusion::common::DataFusionError::Internal("UnorderedAggregateInput requires one child".into()))?;
        Ok(Arc::new(Self::new(input)))
    }
    fn execute(&self, partition: usize, context: Arc<datafusion::execution::TaskContext>) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

impl PhysicalOptimizerRule for AggregateInputOrdering {
    fn name(&self) -> &str {
        "AggregateInputOrdering"
    }
    fn schema_check(&self) -> bool {
        true
    }
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, _: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        fn relies_on_storage_order(plan: &Arc<dyn ExecutionPlan>) -> bool {
            if downcast::<SortExec>(plan.as_ref()).is_some() || downcast::<UnorderedAggregateInput>(plan.as_ref()).is_some() {
                return false;
            }
            downcast::<DedupExec>(plan.as_ref()).is_some() || plan.children().into_iter().any(relies_on_storage_order)
        }
        plan.transform_up(|node| {
            let unordered = downcast::<datafusion::physical_plan::aggregates::AggregateExec>(node.as_ref())
                .map(|_| Arc::clone(node.children()[0]))
                .filter(relies_on_storage_order)
                .map(|input| Arc::new(UnorderedAggregateInput::new(input)) as Arc<dyn ExecutionPlan>);
            swap_child(node, unordered)
        })
        .data()
    }
}

#[cfg(test)]
mod aggregate_input_ordering_tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{Int64Array, TimestampMicrosecondArray},
            datatypes::{DataType, Field, TimeUnit},
            record_batch::RecordBatch,
        },
        datasource::{MemTable, source::DataSourceExec},
        physical_plan::collect,
        prelude::{SessionConfig, SessionContext},
    };

    /// A repeated bucket arrives after an older one: each bucket must be emitted
    /// once with its full count, never closed early on untrusted storage order.
    #[test_case::test_case(vec![vec![2, 1, 2, 0]] => vec![1_i64, 1, 2] ; "repeat after an older row inside one batch")]
    #[test_case::test_case(vec![vec![2, 1], vec![2, 0]] => vec![1_i64, 1, 2] ; "repeat arrives in a later batch")]
    #[tokio::test]
    async fn aggregate_does_not_close_groups_on_untrusted_storage_order(batches: Vec<Vec<i64>>) -> Vec<i64> {
        let mut ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        crate::read::functions::register_custom_functions(&mut ctx).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)]));
        let batches = batches
            .into_iter()
            .map(|values| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(TimestampMicrosecondArray::from(values.into_iter().map(|v| v * 1_000_000).collect::<Vec<_>>()).with_timezone("UTC"))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        ctx.register_table("events", Arc::new(MemTable::try_new(schema, vec![batches]).unwrap().with_sort_order(vec![vec![col("ts").sort(false, false)]])))
            .unwrap();
        let df = ctx.sql("SELECT time_bucket('1 second', ts) AS bucket, count(*) AS n FROM events GROUP BY 1 ORDER BY 1 DESC").await.unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        let plan = plan
            .transform_up(|node| {
                if downcast::<DataSourceExec>(node.as_ref()).is_some() {
                    return Ok(Transformed::yes(Arc::new(DedupExec::new(node, vec!["ts".into()], None)?) as Arc<dyn ExecutionPlan>));
                }
                Ok(Transformed::no(node))
            })
            .data()
            .unwrap();
        let plan = AggregateInputOrdering.optimize(plan, ctx.state().config_options()).unwrap();
        let rows = collect(plan, ctx.task_ctx()).await.unwrap();
        let mut counts = rows.iter().flat_map(|batch| batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()).collect::<Vec<_>>();
        counts.sort();
        counts
    }
}
