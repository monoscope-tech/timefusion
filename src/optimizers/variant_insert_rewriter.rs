use std::collections::HashSet;
use std::sync::Arc;

use datafusion::{
    common::{Result, tree_node::{Transformed, TreeNode}},
    config::ConfigOptions,
    logical_expr::{
        DmlStatement, Expr, LogicalPlan, Projection, Values, WriteOp,
        expr::ScalarFunction,
    },
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
        plan.transform_up(|node| rewrite_insert_node(node)).map(|t| t.data)
    }
}

fn rewrite_insert_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Dml(dml) = &plan {
        if !matches!(dml.op, WriteOp::Insert(_)) {
            return Ok(Transformed::no(plan));
        }

        debug!("VariantInsertRewriter: INSERT into {}", dml.table_name);

        // Get target table schema to find variant column names
        let target_schema = dml.target.schema();
        let variant_column_names: HashSet<String> = target_schema
            .fields()
            .iter()
            .filter(|f| is_variant_type(f.data_type()))
            .map(|f| f.name().clone())
            .collect();

        if variant_column_names.is_empty() {
            return Ok(Transformed::no(plan));
        }

        // Get input schema to find which positions correspond to variant columns
        let input_schema = dml.input.schema();


        let variant_indices: Vec<usize> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| variant_column_names.contains(f.name()))
            .map(|(i, _)| i)
            .collect();


        if variant_indices.is_empty() {
            return Ok(Transformed::no(plan));
        }

        debug!(
            "VariantInsertRewriter: Found {} variant columns in INSERT: {:?}",
            variant_indices.len(),
            input_schema.fields().iter().enumerate()
                .filter(|(i, _)| variant_indices.contains(i))
                .map(|(_, f)| f.name())
                .collect::<Vec<_>>()
        );

        let new_input = rewrite_input_for_variant(&dml.input, &variant_indices)?;

        if let Some(new_input) = new_input {
            let new_dml = LogicalPlan::Dml(DmlStatement {
                op: dml.op.clone(),
                table_name: dml.table_name.clone(),
                target: dml.target.clone(),
                input: Arc::new(new_input),
                output_schema: dml.output_schema.clone(),
            });
            return Ok(Transformed::yes(new_dml));
        }
    }
    Ok(Transformed::no(plan))
}

fn rewrite_input_for_variant(input: &LogicalPlan, variant_indices: &[usize]) -> Result<Option<LogicalPlan>> {
    match input {
        LogicalPlan::Values(values) => rewrite_values_for_variant(values, variant_indices),
        LogicalPlan::Projection(proj) => rewrite_projection_for_variant(proj, variant_indices),
        _ => {
            if let Some(child) = input.inputs().first() {
                if let Some(new_child) = rewrite_input_for_variant(child, variant_indices)? {
                    let new_inputs = vec![new_child];
                    Ok(Some(input.with_new_exprs(input.expressions(), new_inputs)?))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }
}

fn rewrite_values_for_variant(values: &Values, variant_indices: &[usize]) -> Result<Option<LogicalPlan>> {
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

fn rewrite_projection_for_variant(proj: &Projection, variant_indices: &[usize]) -> Result<Option<LogicalPlan>> {
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
        let new_input = rewrite_input_for_variant(&proj.input, variant_indices)?;
        let input = new_input.map(Arc::new).unwrap_or_else(|| proj.input.clone());
        Ok(Some(LogicalPlan::Projection(Projection::try_new(new_exprs, input)?)))
    } else {
        let new_input = rewrite_input_for_variant(&proj.input, variant_indices)?;
        if let Some(new_input) = new_input {
            Ok(Some(LogicalPlan::Projection(Projection::try_new(proj.expr.clone(), Arc::new(new_input))?)))
        } else {
            Ok(None)
        }
    }
}

fn is_utf8_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Utf8(_), _) | Expr::Literal(ScalarValue::Utf8View(_), _) | Expr::Literal(ScalarValue::LargeUtf8(_), _) => true,
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
