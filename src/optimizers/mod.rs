mod tantivy_rewriter;
mod variant_insert_rewriter;
mod variant_select_rewriter;

use datafusion::{
    logical_expr::{BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
pub use tantivy_rewriter::TantivyPredicateRewriter;
pub use variant_insert_rewriter::VariantInsertRewriter;
pub use variant_select_rewriter::VariantSelectRewriter;

/// Utilities for converting timestamp filters to date partition filters
/// for better partition pruning in Delta Lake
pub mod time_range_partition_pruner {
    use super::*;

    /// Extract date from timestamp filter for partition pruning.
    /// Accepts any timestamp unit — pgwire literals arrive as Microsecond, not Nanosecond,
    /// so missing units silently disabled date pruning for point lookups.
    ///
    /// `time_column` is the schema-declared time column name (e.g. `"timestamp"`,
    /// `"event_time"`). Non-matching columns are skipped — pruning only fires for
    /// the table's declared time column.
    pub fn timestamp_to_date_filter(expr: &Expr, time_column: &str) -> Option<Expr> {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return None;
        };
        let Expr::Column(col) = left.as_ref() else { return None };
        if col.name != time_column {
            return None;
        }
        let Expr::Literal(scalar, _) = right.as_ref() else { return None };
        let ts_nanos: i64 = match scalar {
            ScalarValue::TimestampNanosecond(Some(ts), _) => *ts,
            ScalarValue::TimestampMicrosecond(Some(ts), _) => ts.checked_mul(1_000)?,
            ScalarValue::TimestampMillisecond(Some(ts), _) => ts.checked_mul(1_000_000)?,
            ScalarValue::TimestampSecond(Some(ts), _) => ts.checked_mul(1_000_000_000)?,
            _ => return None,
        };
        let date = chrono::DateTime::from_timestamp_nanos(ts_nanos).date_naive();
        let days_since_epoch = (date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() / 86400) as i32;
        let date_lit = Expr::Literal(ScalarValue::Date32(Some(days_since_epoch)), None);
        let date_col = Expr::Column(datafusion::common::Column::new_unqualified("date"));
        // Map timestamp comparisons to inclusive date bounds: a strict `timestamp > T`
        // still admits rows on the same calendar day, so we widen `>` to `>=` and
        // `<` to `<=`. Equality stays exact since `date` is derived from the
        // timestamp at write time.
        let date_op = match op {
            Operator::Gt | Operator::GtEq => Operator::GtEq,
            Operator::Lt | Operator::LtEq => Operator::LtEq,
            Operator::Eq => Operator::Eq,
            _ => return None,
        };
        Some(Expr::BinaryExpr(BinaryExpr::new(Box::new(date_col), date_op, Box::new(date_lit))))
    }
}

/// Utilities for checking project_id filters
pub struct ProjectIdPushdown;

impl ProjectIdPushdown {
    pub fn has_project_id_filter(filters: &[Expr]) -> bool {
        filters.iter().any(Self::contains_project_id)
    }

    pub fn contains_project_id(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => matches!(
                (left.as_ref(), right.as_ref()),
                (Expr::Column(col), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(col))
                if col.name == "project_id"
            ),
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => Self::contains_project_id(left) || Self::contains_project_id(right),
            _ => false,
        }
    }
}
