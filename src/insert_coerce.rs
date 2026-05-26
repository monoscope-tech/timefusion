//! Multi-row INSERT placeholder coercion.
//!
//! Problem. DataFusion parses `INSERT INTO t (cols) VALUES ($1..$N), ($N+1..$2N), ...`
//! into:
//!
//! ```text
//! Dml(Insert)
//!   Projection: column1 AS target_col1, column2 AS target_col2, ...
//!     Values: ($1, ..), ($N+1, ..), ...
//! ```
//!
//! The Projection coerces each `columnX` to the target column type, but the
//! coercion lives on the *column reference* (e.g. `column1 AS target_col`),
//! not on the placeholders inside Values. So
//! `LogicalPlan::get_parameter_types()` reports each `$N` as `None`, and
//! `datafusion-postgres`'s `extract_placeholder_cast_types()` finds no
//! casts either. pgwire then *infers* types positionally from the first
//! row and applies them across all rows — so `$8` (a uuid in row 2 of a
//! 7-col INSERT) gets typed as the row-1 column-1 type (timestamptz) and
//! parsing the uuid string as a datetime errors out.
//!
//! Fix. After the plan is built and before pgwire reads placeholder types,
//! walk the tree, find Values nodes, and wrap each untyped placeholder in
//! `CAST($N AS <values_column_type>)`. The Values column types ARE correct
//! (they've been unified through the Projection), so this makes the
//! placeholders' types match what pgwire needs to ship back to the client.
//! Invoked from the `plan_cache` miss path so every parsed plan goes
//! through it once before being cached.

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    logical_expr::{Cast, Expr, LogicalPlan, Values},
};
use tracing::debug;

pub fn rewrite_plan(plan: LogicalPlan) -> LogicalPlan {
    let result = plan
        .clone()
        .transform_up(|node| {
            let LogicalPlan::Values(values) = node else {
                return Ok(Transformed::no(node));
            };
            let schema = values.schema.clone();
            let column_types: Vec<_> = schema.fields().iter().map(|f| f.data_type().clone()).collect();
            let new_rows: Vec<Vec<Expr>> = values
                .values
                .iter()
                .map(|row| {
                    row.iter()
                        .enumerate()
                        .map(|(col_idx, expr)| {
                            let Some(target_ty) = column_types.get(col_idx).cloned() else {
                                return expr.clone();
                            };
                            let Expr::Placeholder(_) = expr else {
                                return expr.clone();
                            };
                            // Always wrap in Cast. Even if the Placeholder's inferred
                            // `field` already has a matching type, that information
                            // is only set reliably for row-1 placeholders in a
                            // multi-row VALUES; row-2+ get `field: None` and so
                            // `get_parameter_types()` reports them as unknown. Adding
                            // the explicit Cast forces extract_placeholder_cast_types
                            // to pick up every placeholder.
                            Expr::Cast(Cast::new(Box::new(expr.clone()), target_ty))
                        })
                        .collect()
                })
                .collect();
            Ok(Transformed::yes(LogicalPlan::Values(Values { schema, values: new_rows })))
        })
        .map(|t| t.data);
    match result {
        Ok(p) => p,
        Err(e) => {
            debug!(target: "insert_coerce", "plan rewrite skipped: {e}");
            plan
        }
    }
}
