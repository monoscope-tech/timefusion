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
use tracing::{debug, warn};

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
    // Fast path: the lying schema only differs from the real one for
    // Variant columns (which appear as Utf8View). If no Utf8View columns
    // are projected, there's nothing to patch — avoid the HashMap+clones.
    let lying_schema = scan.projected_schema.as_arrow();
    use datafusion::arrow::datatypes::DataType;
    if !lying_schema.fields().iter().any(|f| matches!(f.data_type(), DataType::Utf8View)) {
        return Ok(Transformed::no(LogicalPlan::TableScan(scan)));
    }

    let real = routing.real_schema();
    // Build a patched arrow Schema where every Utf8View column whose
    // real-schema counterpart is Variant gets the Variant data type back
    // (and the extension-name metadata). O(n) lookup via a name→field map.
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
            LogicalPlan::Filter(mut f) => {
                // Some DataFusion rewrite passes promote a Filter above the
                // outermost Projection. Peel through it so Variant columns
                // still reach the wire wrapped, not as raw binary.
                let inner = Arc::unwrap_or_clone(f.input);
                f.input = Arc::new(peel(inner, d)?);
                Ok(LogicalPlan::Filter(f))
            }
            LogicalPlan::Projection(proj) => Ok(wrap_projection(proj)?),
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
    let variant_cols: Vec<usize> = schema.fields().iter().enumerate().filter(|(_, f)| is_variant_type(f.data_type())).map(|(i, _)| i).collect();
    if variant_cols.is_empty() {
        return Ok(plan);
    }
    let variant_to_json = Arc::new(datafusion::logical_expr::ScalarUDF::from(VariantToJsonUdf::default()));
    let exprs: Vec<Expr> = schema
        .iter()
        .map(|(qualifier, field)| {
            let col = Expr::Column(datafusion::common::Column::new(qualifier.cloned(), field.name().clone()));
            if is_variant_type(field.data_type()) {
                wrap_with_variant_to_json(&col, &variant_to_json).alias(field.name())
            } else {
                col
            }
        })
        .collect();
    debug!(
        target: "variant_select_rewriter",
        "added root Projection over un-peelable plan: wrapped {} Variant column(s)",
        variant_cols.len()
    );
    Ok(LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(plan))?))
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
    // Idempotency guard: if the analyzer runs us twice, don't re-wrap an
    // already-wrapped call. Match by concrete UDF type (TypeId) rather than
    // by string name — renaming the UDF or registering another UDF with the
    // same name would otherwise silently break this check.
    if let Expr::ScalarFunction(sf) = expr
        && sf.func.inner().as_any().is::<VariantToJsonUdf>()
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

#[cfg(test)]
mod peel_tests {
    //! Unit tests for `wrap_root_projection` peel logic. These exercise the
    //! Sort / Limit / Distinct / SubqueryAlias / Filter branches and the
    //! MAX_PEEL guard without standing up a server.
    use std::collections::HashMap;

    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::DFSchema,
        logical_expr::{EmptyRelation, builder::LogicalPlanBuilder, col, lit},
    };

    use super::*;

    fn variant_field(name: &str) -> Field {
        let mut md = HashMap::new();
        md.insert("ARROW:extension:name".to_string(), "arrow.parquet.variant".to_string());
        Field::new(
            name,
            DataType::Struct(vec![Arc::new(Field::new("metadata", DataType::Binary, false)), Arc::new(Field::new("value", DataType::Binary, false))].into()),
            true,
        )
        .with_metadata(md)
    }

    fn variant_projection() -> LogicalPlan {
        let schema = Schema::new(vec![variant_field("v")]);
        let df = Arc::new(DFSchema::try_from(schema).unwrap());
        let empty = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema:          df,
        });
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
        matches!(inner, Expr::ScalarFunction(sf) if sf.func.inner().as_any().is::<VariantToJsonUdf>())
    }

    fn first_projection_expr(plan: &LogicalPlan) -> &Expr {
        fn find(p: &LogicalPlan) -> Option<&Expr> {
            if let LogicalPlan::Projection(proj) = p {
                return proj.expr.first();
            }
            p.inputs().into_iter().find_map(|i| find(i))
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
                let mut plan = variant_projection();
                for i in 0..300 {
                    plan = LogicalPlanBuilder::from(plan).alias(format!("a{i}")).unwrap().build().unwrap();
                }
                let out = analyze(plan);
                assert!(!is_variant_to_json_call(first_projection_expr(&out)));
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
