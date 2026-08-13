//! Rewrites `EXISTS(q)` in a projection to `(SELECT count(1) FROM q) > 0`.
//!
//! DataFusion decorrelates EXISTS only in filter position. An EXISTS in a
//! SELECT list survives every optimizer pass and dies in physical planning with
//! "Physical plan does not support logical expression Exists". pgAdmin's
//! object-explorer schema query computes `is_catalog` exactly that way, so the
//! whole browser tree failed to load (2026-08-13).
//!
//! The rewrite is semantics-preserving rather than a compatibility shim:
//! `EXISTS(q)` is true iff `q` returns at least one row, which is what
//! `count(1) > 0` asks. Correlated *scalar* subqueries in a projection ARE
//! decorrelated by DataFusion, which is why the count form plans and the EXISTS
//! form does not.

use std::sync::Arc;

use datafusion::{
    common::{
        Result,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    functions_aggregate::expr_fn::count,
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Subquery, lit},
    optimizer::AnalyzerRule,
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
mod tests {
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
