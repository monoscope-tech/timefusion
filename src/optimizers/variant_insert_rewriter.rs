use std::sync::Arc;

use datafusion::{
    common::{
        Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    logical_expr::{DmlStatement, Expr, LogicalPlan, Projection, Values, WriteOp, expr::ScalarFunction},
    optimizer::AnalyzerRule,
    scalar::ScalarValue,
};
use datafusion_variant::JsonToVariantUdf;
use tracing::debug;

use crate::schema_loader::is_variant_type;

/// AnalyzerRule that rewrites INSERT statements to wrap Utf8 expressions
/// going into Variant columns with `json_to_variant()`.
///
/// This is necessary because DataFusion's type checker rejects Utf8 -> Variant(Struct)
/// casts before our custom VariantConversionExec can run.
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
    if let LogicalPlan::Dml(dml) = &plan {
        if !matches!(dml.op, WriteOp::Insert(_)) {
            return Ok(Transformed::no(plan));
        }

        debug!("VariantInsertRewriter: INSERT into {}", dml.table_name);

        let target_schema = dml.target.schema();
        let input_schema = dml.input.schema();

        // For each input field, check if the TARGET column (by name) is Variant
        let variant_indices: Vec<usize> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, input_field)| {
                // Look up the target column by name and check if it's Variant
                target_schema.column_with_name(input_field.name()).map(|(_, f)| is_variant_type(f.data_type())).unwrap_or(false)
            })
            .map(|(i, _)| i)
            .collect();

        if variant_indices.is_empty() {
            return Ok(Transformed::no(plan));
        }

        debug!(
            "VariantInsertRewriter: Found {} variant columns at positions {:?} (names: {:?})",
            variant_indices.len(),
            variant_indices,
            variant_indices.iter().filter_map(|i| input_schema.fields().get(*i).map(|f| f.name())).collect::<Vec<_>>()
        );

        let new_input = rewrite_input_for_variant(&dml.input, &variant_indices)?;

        if let Some(new_input) = new_input {
            let new_dml = LogicalPlan::Dml(DmlStatement {
                op:            dml.op.clone(),
                table_name:    dml.table_name.clone(),
                target:        dml.target.clone(),
                input:         Arc::new(new_input),
                output_schema: dml.output_schema.clone(),
            });
            return Ok(Transformed::yes(new_dml));
        }
    }
    Ok(Transformed::no(plan))
}

/// Rewrite only the immediate child of the Dml node. `variant_indices` are
/// positions in `dml.input.schema()` (i.e. target table order) — they're only
/// valid for that single plan. Recursing into nested projections with the same
/// indices would mis-wrap unrelated columns whose positions happen to align.
fn rewrite_input_for_variant(input: &LogicalPlan, variant_indices: &[usize]) -> Result<Option<LogicalPlan>> {
    // Membership tests in the row/expr loops below were Vec::contains (O(n)) —
    // O(rows × cols × variant_cols) overall. Hoist into a HashSet once.
    let variant_set: std::collections::HashSet<usize> = variant_indices.iter().copied().collect();
    match input {
        LogicalPlan::Values(values) => rewrite_values_for_variant(values, &variant_set),
        LogicalPlan::Projection(proj) => rewrite_projection_for_variant(proj, &variant_set),
        _ => Ok(None),
    }
}

fn rewrite_values_for_variant(values: &Values, variant_indices: &std::collections::HashSet<usize>) -> Result<Option<LogicalPlan>> {
    let json_to_variant_udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(JsonToVariantUdf::default()));
    let mut modified = false;

    let new_rows: Vec<Vec<Expr>> = values
        .values
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(idx, expr)| {
                    if variant_indices.contains(&idx) && is_utf8_expr(expr) {
                        // (HashSet::contains: O(1))
                        modified = true;
                        wrap_with_json_to_variant(expr, &json_to_variant_udf)
                    } else {
                        expr.clone()
                    }
                })
                .collect()
        })
        .collect();

    if modified {
        Ok(Some(LogicalPlan::Values(Values {
            schema: values.schema.clone(),
            values: new_rows,
        })))
    } else {
        Ok(None)
    }
}

fn rewrite_projection_for_variant(proj: &Projection, variant_indices: &std::collections::HashSet<usize>) -> Result<Option<LogicalPlan>> {
    let json_to_variant_udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(JsonToVariantUdf::default()));
    let mut modified = false;

    let new_exprs: Vec<Expr> = proj
        .expr
        .iter()
        .enumerate()
        .map(|(idx, expr)| {
            if variant_indices.contains(&idx) && is_utf8_expr(expr) {
                modified = true;
                wrap_with_json_to_variant(expr, &json_to_variant_udf)
            } else {
                expr.clone()
            }
        })
        .collect();

    if modified {
        Ok(Some(LogicalPlan::Projection(Projection::try_new(new_exprs, proj.input.clone())?)))
    } else {
        Ok(None)
    }
}

fn is_utf8_expr(expr: &Expr) -> bool {
    // Limitation: matches *literal* Utf8 only (and casts thereof). Column
    // references — e.g. `INSERT INTO t (payload) SELECT col FROM staging`
    // where `col` is Utf8 — are deliberately *not* matched here. Wrapping
    // them would require type lookup against the source plan's schema and
    // is left as a follow-up; today the path that needs Variant coercion
    // is the VALUES form generated by pgwire INSERTs.
    match expr {
        // Only non-null Utf8 literals get wrapped with json_to_variant.
        // NULL literals must pass through (otherwise json_to_variant tries to parse "" and fails).
        Expr::Literal(ScalarValue::Utf8(Some(_)), _) | Expr::Literal(ScalarValue::Utf8View(Some(_)), _) | Expr::Literal(ScalarValue::LargeUtf8(Some(_)), _) => {
            true
        }
        Expr::Cast(cast) => is_utf8_expr(&cast.expr),
        _ => false,
    }
}

fn wrap_with_json_to_variant(expr: &Expr, udf: &Arc<datafusion::logical_expr::ScalarUDF>) -> Expr {
    Expr::ScalarFunction(ScalarFunction {
        func: udf.clone(),
        args: vec![expr.clone()],
    })
}
