//! Expand `qualifier.*` inside scalar function arguments into the explicit
//! column list.
//!
//! Postgres special-cases `t.*` in function calls: it's syntactically expanded
//! into the columns of `t`, in declared order, before the function is resolved.
//! DataFusion's SQL planner parses `t.*` into `Expr::Wildcard { qualifier: …}`
//! but never lowers it inside function-argument lists, so the call hits
//! `TypeCoercion` with a typeless wildcard and fails with
//! `error: Wildcard expressions are not allowed in this context`.
//!
//! This rule does the lowering. It runs before `TypeCoercion`. It only touches
//! qualified wildcards (`sub.*`) — bare `*` keeps its existing meaning
//! (already errors / handled elsewhere).
//!
//! Motivating case: monoscope's row-extraction wrapper
//! `SELECT jsonb_build_array(sub.*) FROM (<inner>) sub` — works in PG natively,
//! needs this rule on TimeFusion. After expansion the call is just
//! `jsonb_build_array(sub.c1, sub.c2, …, sub.cN)`.
//!
//! Limitations / not-yet:
//! - Only expands inside `ScalarFunction`. Aggregate / window calls with
//!   `qualifier.*` are uncommon (Postgres `count(t.*)` is the main one and is
//!   normally written `count(*)`), and they live behind different Expr
//!   variants. Add when a real caller hits it.
//! - Unqualified `Expr::Wildcard { qualifier: None, .. }` is left alone —
//!   bare `f(*)` has different semantics (think `count(*)`) and we don't want
//!   to silently coerce it here.

use std::sync::Arc;

use datafusion::{
    common::{
        Column, DFSchema, Result, TableReference, plan_datafusion_err,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    logical_expr::{Expr, LogicalPlan, expr::ScalarFunction},
    optimizer::AnalyzerRule,
};

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
mod tests {
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
        crate::functions::register_custom_functions(&mut ctx).unwrap();
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
