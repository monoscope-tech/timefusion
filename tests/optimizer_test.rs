use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;
use datafusion::common::Column;
use timefusion::optimizers::{TimeRangePartitionPruner, ProjectIdPushdown};

#[test]
fn test_timestamp_to_date_filter_conversion() {
    // Create a timestamp filter
    let timestamp_col = Expr::Column(Column::new_unqualified("timestamp"));
    let timestamp_value = ScalarValue::TimestampNanosecond(
        Some(1704067200000000000), // 2024-01-01 00:00:00 UTC in nanoseconds
        None
    );
    let timestamp_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(timestamp_col),
        Operator::GtEq,
        Box::new(Expr::Literal(timestamp_value, None)),
    ));
    
    // Apply the optimizer
    let date_filter = TimeRangePartitionPruner::timestamp_to_date_filter(&timestamp_filter);
    
    // Verify a date filter was created
    assert!(date_filter.is_some(), "Should create a date filter from timestamp filter");
    
    // Check the date filter is correct
    if let Some(Expr::BinaryExpr(date_expr)) = date_filter {
        // Should have date column
        if let Expr::Column(col) = date_expr.left.as_ref() {
            assert_eq!(col.name, "date", "Should filter on date column");
        } else {
            panic!("Expected date column in filter");
        }
        
        // Should have the same operator
        assert_eq!(date_expr.op, Operator::GtEq, "Should preserve operator");
        
        // Should have a Date32 value
        if let Expr::Literal(ScalarValue::Date32(Some(_)), _) = date_expr.right.as_ref() {
            // Success - we have a date filter
        } else {
            panic!("Expected Date32 literal in filter");
        }
    } else {
        panic!("Expected BinaryExpr for date filter");
    }
}

#[test]
fn test_project_id_filter_detection() {
    // Test with project_id filter
    let project_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("project_id"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Utf8(Some("test_project".to_string())), None)),
    ));
    
    assert!(
        ProjectIdPushdown::has_project_id_filter(&[project_filter.clone()]),
        "Should detect project_id filter"
    );
    
    // Test without project_id filter
    let other_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(Expr::Column(Column::new_unqualified("name"))),
        Operator::Eq,
        Box::new(Expr::Literal(ScalarValue::Utf8(Some("test".to_string())), None)),
    ));
    
    assert!(
        !ProjectIdPushdown::has_project_id_filter(&[other_filter.clone()]),
        "Should not detect project_id in non-project_id filter"
    );
    
    // Test with AND expression containing project_id
    let combined_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(project_filter),
        Operator::And,
        Box::new(other_filter),
    ));
    
    assert!(
        ProjectIdPushdown::contains_project_id(&combined_filter),
        "Should detect project_id in AND expression"
    );
}

#[test]
fn test_optimizer_integration() {
    // This test verifies that timestamp filters get date filters added
    let timestamp_col = Expr::Column(Column::new_unqualified("timestamp"));
    let timestamp_value = ScalarValue::TimestampNanosecond(
        Some(1704067200000000000), // 2024-01-01 00:00:00 UTC
        None
    );
    let timestamp_filter = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(timestamp_col),
        Operator::GtEq,
        Box::new(Expr::Literal(timestamp_value, None)),
    ));
    
    // Apply the optimization
    let date_filter = TimeRangePartitionPruner::timestamp_to_date_filter(&timestamp_filter);
    assert!(date_filter.is_some(), "Should generate date filter for partition pruning");
    
    // In the actual implementation, this would be combined with the original filter
    // to ensure both timestamp and date filters are applied for optimal pruning
}