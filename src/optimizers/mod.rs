mod defer_expensive_projection;
mod ordered_union_for_topk;
pub mod pg_array_literal_rewriter;
mod tantivy_rewriter;
mod variant_insert_rewriter;
mod variant_select_rewriter;
mod wildcard_fn_arg_expander;

use datafusion::{
    logical_expr::{BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
pub use defer_expensive_projection::DeferExpensiveProjection;
pub use ordered_union_for_topk::OrderedUnionForTopK;
pub use pg_array_literal_rewriter::PgArrayLiteralRewriter;
pub use tantivy_rewriter::TantivyPredicateRewriter;
pub use variant_insert_rewriter::VariantInsertRewriter;
pub use variant_select_rewriter::{VariantScanSchemaRestore, VariantSelectRewriter};
pub use wildcard_fn_arg_expander::WildcardFnArgExpander;

/// Extract the string from a Utf8/Utf8View/LargeUtf8 scalar literal.
pub fn extract_utf8_string(v: &ScalarValue) -> Option<String> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::Utf8View(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.clone()),
        _ => None,
    }
}

/// True if `expr` references column `name`, seen through any `Cast`/`TryCast`
/// wrapper. TypeCoercion wraps a column in a Cast when the compared literal's
/// unit/type differs (e.g. a µs `timestamp` column vs an ns `NOW() - INTERVAL`
/// bound), which otherwise hides the column from filter-shape matching.
pub fn is_col_through_cast(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(c) => c.name == name,
        Expr::Cast(c) => is_col_through_cast(c.expr.as_ref(), name),
        Expr::TryCast(c) => is_col_through_cast(c.expr.as_ref(), name),
        _ => false,
    }
}

/// Reverse a comparison operator for swapped operands (`lit < col` ≡ `col > lit`).
/// Non-comparison operators pass through unchanged.
pub fn swap_comparison(op: &Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        other => *other,
    }
}

/// Utilities for converting timestamp filters to date partition filters
/// for better partition pruning in Delta Lake
pub mod time_range_partition_pruner {
    use super::*;

    /// Extract date predicates from a timestamp filter for partition pruning.
    /// Accepts any timestamp unit — pgwire literals arrive as Microsecond, not Nanosecond,
    /// so missing units silently disabled date pruning for point lookups.
    ///
    /// `time_column` is the schema-declared time column name (e.g. `"timestamp"`,
    /// `"event_time"`). Non-matching columns are skipped — pruning only fires for
    /// the table's declared time column.
    pub fn timestamp_to_date_filters(expr: &Expr, time_column: &str) -> Vec<Expr> {
        let date_filter = |expr: &Expr, op: Operator| {
            let Expr::Literal(scalar, _) = expr else { return None };
            let ts_nanos = match scalar {
                ScalarValue::TimestampNanosecond(Some(ts), _) => *ts,
                ScalarValue::TimestampMicrosecond(Some(ts), _) => ts.checked_mul(1_000)?,
                ScalarValue::TimestampMillisecond(Some(ts), _) => ts.checked_mul(1_000_000)?,
                ScalarValue::TimestampSecond(Some(ts), _) => ts.checked_mul(1_000_000_000)?,
                _ => return None,
            };
            let date = chrono::DateTime::from_timestamp_nanos(ts_nanos).date_naive();
            let days_since_epoch = (date.and_hms_opt(0, 0, 0)?.and_utc().timestamp() / 86400) as i32;
            let date_lit = Expr::Literal(ScalarValue::Date32(Some(days_since_epoch)), None);
            let date_col = Expr::Column(datafusion::common::Column::new_unqualified("date"));
            let date_op = match op {
                Operator::Gt | Operator::GtEq => Operator::GtEq,
                Operator::Lt | Operator::LtEq => Operator::LtEq,
                Operator::Eq => Operator::Eq,
                _ => return None,
            };
            Some(Expr::BinaryExpr(BinaryExpr::new(Box::new(date_col), date_op, Box::new(date_lit))))
        };

        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (lit_expr, op) = if is_col_through_cast(left.as_ref(), time_column) {
                    (right.as_ref(), *op)
                } else if is_col_through_cast(right.as_ref(), time_column) {
                    (left.as_ref(), swap_comparison(op))
                } else {
                    return vec![];
                };
                date_filter(lit_expr, op).into_iter().collect()
            }
            Expr::Between(between) if !between.negated && is_col_through_cast(between.expr.as_ref(), time_column) => {
                [date_filter(between.low.as_ref(), Operator::GtEq), date_filter(between.high.as_ref(), Operator::LtEq)].into_iter().flatten().collect()
            }
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::datatypes::{DataType, TimeUnit},
        logical_expr::{
            Between,
            expr::{Cast, TryCast},
        },
    };

    fn timestamp(micros: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMicrosecond(Some(micros), Some("UTC".into())), None)
    }

    fn date_filters(expr: Expr) -> Vec<(Operator, i32)> {
        time_range_partition_pruner::timestamp_to_date_filters(&expr, "timestamp")
            .into_iter()
            .map(|expr| match expr {
                Expr::BinaryExpr(BinaryExpr { left, op, right }) => match (*left, *right) {
                    (Expr::Column(col), Expr::Literal(ScalarValue::Date32(Some(day)), _)) if col.name == "date" => (op, day),
                    _ => panic!("unexpected date filter"),
                },
                _ => panic!("unexpected date filter"),
            })
            .collect()
    }

    #[test]
    fn timestamp_between_derives_two_inclusive_date_bounds() {
        let expr = Expr::Between(Between::new(
            Box::new(Expr::Column(datafusion::common::Column::new_unqualified("timestamp"))),
            false,
            Box::new(timestamp(1_704_067_200_000_000)),
            Box::new(timestamp(1_704_240_000_000_000)),
        ));

        assert_eq!(date_filters(expr), vec![(Operator::GtEq, 19_723), (Operator::LtEq, 19_725)]);
    }

    #[test]
    fn timestamp_comparisons_support_units_casts_and_reversed_operands() {
        let timestamp_col = Expr::Column(datafusion::common::Column::new_unqualified("timestamp"));
        let cast_timestamp = Expr::Cast(Cast::new(Box::new(timestamp_col.clone()), DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))));
        let try_cast_timestamp = Expr::TryCast(TryCast::new(Box::new(timestamp_col.clone()), DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))));
        let start = 1_704_067_200_000_000i64;
        let cases = [
            (Expr::BinaryExpr(BinaryExpr::new(Box::new(timestamp_col.clone()), Operator::GtEq, Box::new(timestamp(start)))), Operator::GtEq),
            (Expr::BinaryExpr(BinaryExpr::new(Box::new(timestamp(start)), Operator::LtEq, Box::new(timestamp_col.clone()))), Operator::GtEq),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(cast_timestamp),
                    Operator::Lt,
                    Box::new(Expr::Literal(ScalarValue::TimestampNanosecond(Some(start * 1_000), Some("UTC".into())), None)),
                )),
                Operator::LtEq,
            ),
            (Expr::BinaryExpr(BinaryExpr::new(Box::new(try_cast_timestamp), Operator::Gt, Box::new(timestamp(start)))), Operator::GtEq),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(timestamp_col.clone()),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::TimestampMillisecond(Some(start / 1_000), Some("UTC".into())), None)),
                )),
                Operator::Eq,
            ),
            (
                Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(timestamp_col),
                    Operator::Eq,
                    Box::new(Expr::Literal(ScalarValue::TimestampSecond(Some(start / 1_000_000), Some("UTC".into())), None)),
                )),
                Operator::Eq,
            ),
        ];

        for (expr, expected_op) in cases {
            assert_eq!(date_filters(expr), vec![(expected_op, 19_723)]);
        }
    }
}

/// Utilities for checking project_id filters
/// Extract the literal `project_id` value from an expression tree.
///
/// Walks the same shapes `ProjectIdPushdown::contains_project_id` recognises:
/// `project_id = 'x'` (either arg order, Utf8 / Utf8View) and through `AND`
/// parents. Returns the first match. Used by both the SELECT-side router
/// (`ProjectRoutingTable`) and DML extractor (`extract_dml_info` in
/// `dml.rs`); keep them in sync by always going through this function.
///
/// `NOT` is intentionally not walked into: `NOT project_id = 'x'` excludes
/// that project rather than selecting it, so returning it as the routing
/// target would route to the wrong tenant. Matching the conservative
/// `contains_project_id` shape ensures both helpers agree.
pub fn extract_project_id_from_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Literal(v, _)) | (Expr::Literal(v, _), Expr::Column(col)) if col.name == "project_id" => extract_utf8_string(v),
            _ => None,
        },
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => extract_project_id_from_expr(left).or_else(|| extract_project_id_from_expr(right)),
        _ => None,
    }
}

pub struct ProjectIdPushdown;

impl ProjectIdPushdown {
    pub fn has_project_id_filter(filters: &[Expr]) -> bool {
        filters.iter().any(Self::contains_project_id)
    }

    /// Conservative: recognises `project_id = 'x'` (either argument order) and
    /// AND-conjuncts that include one. **OR** is intentionally NOT handled —
    /// `WHERE project_id = 'a' OR project_id = 'b'` is rare in practice and
    /// reporting "no project_id filter" for it keeps the multi-tenant guard
    /// strict (the query then errors out instead of silently scanning all
    /// projects). Extend here if cross-project OR becomes a real workload.
    pub fn contains_project_id(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Column(col), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(col))
                if col.name == "project_id"
            ),
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => Self::contains_project_id(left) || Self::contains_project_id(right),
            _ => false,
        }
    }
}
