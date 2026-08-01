use std::{collections::HashSet, sync::Arc};

use crate::functions::json_to_variant_udf;
use datafusion::{
    common::{
        DataFusionError, Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    logical_expr::{DmlStatement, Expr, LogicalPlan, Projection, ScalarUDF, Values, WriteOp, expr::ScalarFunction},
    optimizer::AnalyzerRule,
    scalar::ScalarValue,
};
use tracing::debug;

use crate::schema_loader::is_variant_type;

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
