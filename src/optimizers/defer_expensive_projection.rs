//! Defer expensive scalar projections past TopK (Sort with fetch).
//!
//! `SELECT jsonb_build_array(...) FROM t WHERE ... ORDER BY ts DESC LIMIT 501`
//! plans as `Sort(fetch=501)` over `Projection(expensive exprs)`: the JSON
//! building / timestamp formatting / casts run for EVERY row in the time
//! window before TopK keeps 501. On wide windows that's minutes of CPU and
//! an OOM-sized allocation storm (observed killing prod TimeFusion).
//!
//! Rewrite: `Sort(fetch) → Projection(expensive)` becomes
//! `Projection(expensive, rebuilt) → Sort(fetch, exprs inlined) → Projection(raw cols)`
//! so non-trivial exprs are evaluated only on the `fetch` surviving rows.
//! Registered after DataFusion's defaults, so `push_down_limit` has already
//! folded LIMIT into `Sort.fetch` by the time it runs.

use std::sync::Arc;

use datafusion::{
    common::{
        Column, Result,
        tree_node::{Transformed, TreeNode},
    },
    logical_expr::{Expr, LogicalPlan, Projection, Sort, SortExpr},
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
        let LogicalPlan::Sort(sort) = plan else { return Ok(Transformed::no(plan)) };
        let (LogicalPlan::Projection(proj), Some(_)) = (sort.input.as_ref(), sort.fetch) else {
            return Ok(Transformed::no(LogicalPlan::Sort(sort)));
        };
        if proj.expr.iter().all(is_trivial) {
            return Ok(Transformed::no(LogicalPlan::Sort(sort)));
        }

        // Projection-output column → underlying (unaliased) expr, for inlining
        // sort keys that reference projection outputs.
        let out_map: std::collections::HashMap<Column, Expr> =
            proj.schema.iter().zip(proj.expr.iter()).map(|((q, f), e)| (Column::new(q.cloned(), f.name()), e.clone().unalias())).collect();
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
                Ok(SortExpr { expr, ..se.clone() })
            })
            .collect::<Result<Vec<_>>>()?;

        // Raw input columns the deferred exprs and inlined sort keys need.
        let mut needed: Vec<Column> = Vec::new();
        for e in proj.expr.iter().chain(new_sort_exprs.iter().map(|se| &se.expr)) {
            for c in e.column_refs() {
                if !needed.contains(c) {
                    needed.push(c.clone());
                }
            }
        }
        if needed.is_empty() {
            return Ok(Transformed::no(LogicalPlan::Sort(sort)));
        }

        let min_proj = Projection::try_new(needed.into_iter().map(Expr::Column).collect(), Arc::clone(&proj.input))?;
        let new_sort = LogicalPlan::Sort(Sort { expr: new_sort_exprs, input: Arc::new(LogicalPlan::Projection(min_proj)), fetch: sort.fetch });
        // Rebuild the original projection above the TopK, aliasing each expr
        // to its original (qualifier, name) so the parent plan's column
        // references and the root schema are unchanged.
        let rebuilt = proj.schema.iter().zip(proj.expr.iter()).map(|((q, f), e)| e.clone().unalias().alias_qualified(q.cloned(), f.name())).collect();
        let hoisted = Projection::try_new_with_schema(rebuilt, Arc::new(new_sort), Arc::clone(&proj.schema))?;
        Ok(Transformed::yes(LogicalPlan::Projection(hoisted)))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, TimeUnit},
        datasource::MemTable,
        execution::session_state::SessionStateBuilder,
        prelude::SessionContext,
    };

    use super::*;

    async fn plans(sql: &str, with_rule: bool) -> (String, String) {
        let mut builder = SessionStateBuilder::new().with_default_features();
        if with_rule {
            builder = builder.with_optimizer_rule(Arc::new(DeferExpensiveProjection));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())).unwrap();
        let df = ctx.sql(sql).await.unwrap();
        let logical = format!("{}", df.clone().into_optimized_plan().unwrap().display_indent());
        let physical = format!("{}", datafusion::physical_plan::displayable(df.create_physical_plan().await.unwrap().as_ref()).indent(false));
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
