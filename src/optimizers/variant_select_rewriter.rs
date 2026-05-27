//! Variant-aware SELECT-plan post-processing.
//!
//! Two passes, both gated on the plan being a non-DML (SELECT-like) plan:
//!
//! 1. **TableScan schema patch.** TimeFusion's `ProjectRoutingTable::schema()`
//!    returns a *lying* schema that substitutes Variant columns with
//!    `Utf8View` so DataFusion's INSERT-VALUES type checker accepts raw
//!    JSON string literals. For SELECT plans we want the real Variant
//!    type so downstream UDFs (`variant_get`, `jsonb_path_exists`, …)
//!    receive Struct{Binary,Binary} and call
//!    `parquet_variant_compute::variant_get` directly. We walk each
//!    `LogicalPlan::TableScan`, downcast its source to
//!    `DefaultTableSource → ProjectRoutingTable`, and rebuild the scan's
//!    `projected_schema` with Variant types restored.
//!
//! 2. **Root-projection JSON wrap.** Bare `SELECT payload` from a pgwire
//!    client must serialize the Variant to JSON text for the wire. We
//!    used to do this at the scan boundary (`VariantToJsonExec`) which
//!    forced every intermediate operator to deal with Utf8 and made
//!    Variant slower than plain JSON text. Now we wrap only the
//!    *outermost* Projection — peeling Sort/Limit/Distinct/SubqueryAlias —
//!    so intermediate `variant_get` / `jsonb_path_exists` etc. operate
//!    on the binary Variant.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{Field, Schema},
    catalog::default_table_source::DefaultTableSource,
    common::{
        DFSchema, DFSchemaRef, Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    logical_expr::{Expr, ExprSchemable, LogicalPlan, Projection, TableScan, expr::ScalarFunction},
    optimizer::AnalyzerRule,
};
use datafusion_variant::VariantToJsonUdf;
use tracing::debug;

use crate::{database::ProjectRoutingTable, schema_loader::is_variant_type};

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
        // Pass 1: patch each TableScan's projected_schema so Variant columns
        // carry the real Variant type, not Utf8View. Downstream operators
        // (variant_get, jsonb_path_exists, ->, ->>) need the real type.
        let patched = plan.transform_up(patch_table_scan).map(|t| t.data)?;
        // Pass 2: wrap Variant-typed projections at the topmost SELECT
        // projection with variant_to_json for the wire.
        wrap_root_projection(patched)
    }
}

fn patch_table_scan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::TableScan(scan) = plan else {
        return Ok(Transformed::no(plan));
    };
    // Source must be a DefaultTableSource around ProjectRoutingTable.
    let Some(default_src) = scan.source.as_any().downcast_ref::<DefaultTableSource>() else {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    };
    let Some(routing) = default_src.table_provider.as_any().downcast_ref::<ProjectRoutingTable>() else {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    };
    let real = routing.real_schema();

    // Build a patched arrow Schema where every Utf8View column whose
    // real-schema counterpart is Variant gets the Variant data type back
    // (and the extension-name metadata). O(n) lookup via a name→field map —
    // schemas with many columns made the original `column_with_name` loop
    // O(n²).
    let lying_schema = scan.projected_schema.as_arrow();
    let real_by_name: std::collections::HashMap<&str, &Arc<Field>> = real.fields().iter().map(|f| (f.name().as_str(), f)).collect();
    let mut patched_fields: Vec<Arc<Field>> = Vec::with_capacity(lying_schema.fields().len());
    let mut changed = false;
    for f in lying_schema.fields() {
        match real_by_name.get(f.name().as_str()) {
            Some(real_field) if is_variant_type(real_field.data_type()) => {
                patched_fields.push(Arc::clone(real_field));
                changed = true;
            }
            _ => patched_fields.push(f.clone()),
        }
    }
    if !changed {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }
    let patched_arrow = Arc::new(Schema::new_with_metadata(patched_fields, lying_schema.metadata().clone()));
    // Preserve the original DFSchema's column qualifiers (e.g. table aliases).
    let qualifiers: Vec<_> = scan.projected_schema.iter().map(|(q, _)| q.cloned()).collect();
    let mut zipped: Vec<(Option<datafusion::sql::TableReference>, Arc<Field>)> = qualifiers.into_iter().zip(patched_arrow.fields().iter().cloned()).collect();
    let new_df: DFSchemaRef = Arc::new(DFSchema::new_with_metadata(std::mem::take(&mut zipped), patched_arrow.metadata().clone())?);
    debug!(target: "variant_select_rewriter", "patched TableScan({}) schema → Variant", scan.table_name);
    Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
        projected_schema: new_df,
        ..scan
    })))
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
            return Ok(plan);
        }
        let d = depth + 1;
        match plan {
            LogicalPlan::Sort(mut s) => {
                let inner = Arc::unwrap_or_clone(s.input);
                s.input = Arc::new(peel(inner, d)?);
                Ok(LogicalPlan::Sort(s))
            }
            LogicalPlan::Limit(mut l) => {
                let inner = Arc::unwrap_or_clone(l.input);
                l.input = Arc::new(peel(inner, d)?);
                Ok(LogicalPlan::Limit(l))
            }
            LogicalPlan::Distinct(dist) => {
                use datafusion::logical_expr::Distinct;
                match dist {
                    Distinct::All(input) => {
                        let inner = Arc::unwrap_or_clone(input);
                        Ok(LogicalPlan::Distinct(Distinct::All(Arc::new(peel(inner, d)?))))
                    }
                    Distinct::On(mut on) => {
                        let inner = Arc::unwrap_or_clone(on.input);
                        on.input = Arc::new(peel(inner, d)?);
                        Ok(LogicalPlan::Distinct(Distinct::On(on)))
                    }
                }
            }
            LogicalPlan::SubqueryAlias(mut s) => {
                let inner = Arc::unwrap_or_clone(s.input);
                s.input = Arc::new(peel(inner, d)?);
                Ok(LogicalPlan::SubqueryAlias(s))
            }
            LogicalPlan::Projection(proj) => Ok(wrap_projection(proj)?),
            other => Ok(other),
        }
    }
    peel(plan, 0)
}

fn wrap_projection(proj: Projection) -> Result<LogicalPlan> {
    let input_schema = proj.input.schema().clone();
    let variant_to_json = Arc::new(datafusion::logical_expr::ScalarUDF::from(VariantToJsonUdf::default()));
    let mut wrapped = 0usize;
    let new_exprs: Vec<Expr> = proj
        .expr
        .iter()
        .map(|expr| {
            if is_variant_expr(expr, &input_schema) {
                wrapped += 1;
                wrap_with_variant_to_json(expr, &variant_to_json)
            } else {
                expr.clone()
            }
        })
        .collect();
    if wrapped == 0 {
        return Ok(LogicalPlan::Projection(proj));
    }
    debug!(target: "variant_select_rewriter", "wrapped {} Variant exprs at root projection", wrapped);
    Ok(LogicalPlan::Projection(Projection::try_new(new_exprs, proj.input.clone())?))
}

fn is_variant_expr(expr: &Expr, schema: &DFSchema) -> bool {
    if let Expr::ScalarFunction(sf) = expr
        && sf.func.name() == "variant_to_json"
    {
        return false;
    }
    expr.get_type(schema).map(|dt| is_variant_type(&dt)).unwrap_or(false)
}

fn wrap_with_variant_to_json(expr: &Expr, udf: &Arc<datafusion::logical_expr::ScalarUDF>) -> Expr {
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
