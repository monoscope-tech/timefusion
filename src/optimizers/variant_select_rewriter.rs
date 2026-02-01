use std::sync::Arc;

use datafusion::{
    common::{DFSchema, Result, tree_node::{Transformed, TreeNode}},
    config::ConfigOptions,
    logical_expr::{Expr, ExprSchemable, LogicalPlan, Projection, expr::ScalarFunction},
    optimizer::AnalyzerRule,
};
use datafusion_variant::VariantToJsonUdf;
use tracing::debug;

use crate::schema_loader::is_variant_type;

/// AnalyzerRule that rewrites SELECT queries to wrap Variant columns with `variant_to_json()`.
/// This ensures Variant data is serialized as JSON strings for PostgreSQL wire protocol.
#[derive(Debug, Default)]
pub struct VariantSelectRewriter;

impl AnalyzerRule for VariantSelectRewriter {
    fn name(&self) -> &str {
        "variant_select_rewriter"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(rewrite_select_node).map(|t| t.data)
    }
}

fn rewrite_select_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Projection(proj) = &plan {
        let input_schema = proj.input.schema();
        let variant_to_json = Arc::new(datafusion::logical_expr::ScalarUDF::from(VariantToJsonUdf::default()));
        let mut modified = false;

        let new_exprs: Vec<Expr> = proj.expr.iter().map(|expr| {
            if is_variant_expr(expr, input_schema) {
                modified = true;
                wrap_with_variant_to_json(expr, &variant_to_json)
            } else {
                expr.clone()
            }
        }).collect();

        if modified {
            debug!("VariantSelectRewriter: Wrapped {} Variant columns with variant_to_json()",
                new_exprs.iter().filter(|e| matches!(e, Expr::ScalarFunction(_))).count());
            return Ok(Transformed::yes(LogicalPlan::Projection(Projection::try_new(new_exprs, proj.input.clone())?)));
        }
    }
    Ok(Transformed::no(plan))
}

fn is_variant_expr(expr: &Expr, schema: &DFSchema) -> bool {
    // Already wrapped - don't double-wrap
    if let Expr::ScalarFunction(sf) = expr {
        if sf.func.name() == "variant_to_json" {
            return false;
        }
    }
    // Check if expression's result type is Variant
    expr.get_type(schema).map(|dt| is_variant_type(&dt)).unwrap_or(false)
}

fn wrap_with_variant_to_json(expr: &Expr, udf: &Arc<datafusion::logical_expr::ScalarUDF>) -> Expr {
    // Preserve the alias if there is one
    let (inner, alias) = match expr {
        Expr::Alias(a) => (a.expr.as_ref().clone(), Some(a.name.clone())),
        _ => (expr.clone(), None),
    };

    let wrapped = Expr::ScalarFunction(ScalarFunction {
        func: udf.clone(),
        args: vec![inner],
    });

    match alias {
        Some(name) => wrapped.alias(name),
        None => wrapped,
    }
}
