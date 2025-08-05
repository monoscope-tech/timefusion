use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Utilities for converting timestamp filters to date partition filters
/// for better partition pruning in Delta Lake
pub mod time_range_partition_pruner {
    use super::*;
    
    /// Extract date from timestamp filter for partition pruning
    pub fn timestamp_to_date_filter(expr: &Expr) -> Option<Expr> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // Check if this is a timestamp comparison
                if let (Expr::Column(col), Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _tz), _)) = 
                    (left.as_ref(), right.as_ref()) {
                    if col.name == "timestamp" {
                        // Convert timestamp to date for partition filter
                        let datetime = chrono::DateTime::from_timestamp_nanos(*ts);
                        let date = datetime.date_naive();
                        
                        let date_scalar = ScalarValue::Date32(Some(
                            date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as i32 / 86400
                        ));
                        
                        // Create corresponding date filter
                        let date_col = Expr::Column(datafusion::common::Column::new_unqualified("date"));
                        let date_filter = match op {
                            Operator::Gt | Operator::GtEq => {
                                Expr::BinaryExpr(BinaryExpr::new(
                                    Box::new(date_col),
                                    *op,
                                    Box::new(Expr::Literal(date_scalar, None)),
                                ))
                            }
                            Operator::Lt | Operator::LtEq => {
                                Expr::BinaryExpr(BinaryExpr::new(
                                    Box::new(date_col),
                                    *op,
                                    Box::new(Expr::Literal(date_scalar, None)),
                                ))
                            }
                            Operator::Eq => {
                                Expr::BinaryExpr(BinaryExpr::new(
                                    Box::new(date_col),
                                    Operator::Eq,
                                    Box::new(Expr::Literal(date_scalar, None)),
                                ))
                            }
                            _ => return None,
                        };
                        
                        return Some(date_filter);
                    }
                }
                None
            }
            _ => None,
        }
    }
}

/// Utilities for checking project_id filters
pub struct ProjectIdPushdown {}

impl ProjectIdPushdown {
    pub fn has_project_id_filter(filters: &[Expr]) -> bool {
        filters.iter().any(Self::contains_project_id)
    }
    
    pub fn contains_project_id(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                matches!(
                    (left.as_ref(), right.as_ref()),
                    (Expr::Column(col), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(col))
                    if col.name == "project_id"
                )
            }
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
                Self::contains_project_id(left) || Self::contains_project_id(right)
            }
            _ => false,
        }
    }
}