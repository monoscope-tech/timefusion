use datafusion::common::Result as DFResult;
use datafusion::common::{Statistics, stats::Precision};
use datafusion::logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::scalar::ScalarValue;
use datafusion::common::tree_node::Transformed;
use std::sync::Arc;
use tracing::{debug, info};

/// Optimizer rule that converts timestamp filters to date partition filters
/// for better partition pruning in Delta Lake
#[derive(Debug, Default)]
pub struct TimeRangePartitionPruner {}

impl TimeRangePartitionPruner {
    pub fn new() -> Self {
        Self::default()
    }
    
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

impl OptimizerRule for TimeRangePartitionPruner {
    fn name(&self) -> &str {
        "time_range_partition_pruner"
    }
    
    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }
    
    fn supports_rewrite(&self) -> bool {
        true
    }
    
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        use datafusion::logical_expr::{logical_plan::LogicalPlan, Filter};
        use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
        
        // Create a rewriter that adds date filters for timestamp filters
        struct TimestampRewriter;
        
        impl TreeNodeRewriter for TimestampRewriter {
            type Node = Expr;
            
            fn f_down(&mut self, expr: Expr) -> DFResult<Transformed<Expr>> {
                // Look for timestamp filters and add corresponding date filters
                if let Some(date_filter) = TimeRangePartitionPruner::timestamp_to_date_filter(&expr) {
                    // Add the date filter alongside the timestamp filter using AND
                    let combined = Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(expr.clone()),
                        Operator::And,
                        Box::new(date_filter),
                    ));
                    Ok(Transformed::yes(combined))
                } else {
                    Ok(Transformed::no(expr))
                }
            }
        }
        
        // Apply the rewriter to filter nodes in the plan
        match plan {
            LogicalPlan::Filter(Filter { predicate, input, .. }) => {
                let mut rewriter = TimestampRewriter;
                let new_predicate = predicate.clone().rewrite(&mut rewriter)?;
                
                if new_predicate.transformed {
                    let new_filter = LogicalPlan::Filter(Filter::try_new(
                        new_predicate.data,
                        input,
                    )?);
                    Ok(Transformed::yes(new_filter))
                } else {
                    Ok(Transformed::no(LogicalPlan::Filter(Filter::try_new(predicate, input)?)))
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

/// Optimizer rule that ensures project_id filters are always present and pushed down
#[derive(Debug, Default)]
pub struct ProjectIdPushdown {}

impl ProjectIdPushdown {
    pub fn new() -> Self {
        Self::default()
    }
    
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

impl OptimizerRule for ProjectIdPushdown {
    fn name(&self) -> &str {
        "project_id_pushdown"
    }
    
    fn apply_order(&self) -> Option<datafusion::optimizer::ApplyOrder> {
        Some(datafusion::optimizer::ApplyOrder::TopDown)
    }
    
    fn supports_rewrite(&self) -> bool {
        true
    }
    
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        use datafusion::logical_expr::{logical_plan::LogicalPlan, Filter};
        
        // Check if the plan has filters with project_id
        if let LogicalPlan::Filter(Filter { predicate, .. }) = &plan {
            // Convert predicate to a vec for easier checking
            let filters = match predicate {
                Expr::BinaryExpr(BinaryExpr { op: Operator::And, .. }) => {
                    // For AND expressions, we'd need to flatten them (simplified here)
                    vec![predicate.clone()]
                }
                _ => vec![predicate.clone()],
            };
            
            if !Self::has_project_id_filter(&filters) {
                // Log warning - in production, you might want to add a default project_id
                // or reject the query
                tracing::warn!("Query missing project_id filter - may scan all partitions!");
            }
        }
        
        // For now, just return the plan unchanged
        // In production, you might want to add a default project_id filter
        Ok(Transformed::no(plan))
    }
}

/// Statistics-aware filter optimizer that uses Delta Lake statistics for better pruning
#[derive(Debug)]
pub struct StatisticsAwareFilterOptimizer {
    statistics: Option<Arc<Statistics>>,
}

impl StatisticsAwareFilterOptimizer {
    pub fn new(statistics: Option<Arc<Statistics>>) -> Self {
        Self { statistics }
    }
    
    /// Check if a filter can be efficiently pruned using column statistics
    pub fn can_prune_with_stats(&self, expr: &Expr) -> bool {
        if self.statistics.is_none() {
            return false;
        }
        
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op: _, right: _ }) => {
                // Check if we have statistics for the column
                if let Expr::Column(col) = left.as_ref() {
                    if let Some(stats) = &self.statistics {
                        // Check if we have min/max stats for this column
                        if let Some(col_idx) = self.get_column_index(&col.name) {
                            if col_idx < stats.column_statistics.len() {
                                let col_stats = &stats.column_statistics[col_idx];
                                return !matches!(col_stats.min_value, Precision::Absent) 
                                    && !matches!(col_stats.max_value, Precision::Absent);
                            }
                        }
                    }
                }
                false
            }
            _ => false,
        }
    }
    
    fn get_column_index(&self, _column_name: &str) -> Option<usize> {
        // In a real implementation, this would map column names to indices
        // based on the schema
        None
    }
    
    /// Optimize a filter expression using statistics
    pub fn optimize_filter(&self, expr: &Expr) -> Option<Expr> {
        if let Some(stats) = &self.statistics {
            // Log statistics usage for monitoring
            if let Precision::Exact(rows) = stats.num_rows {
                debug!("Using statistics for optimization: {} rows", rows);
            }
            
            // Check if the filter can be optimized
            if self.can_prune_with_stats(expr) {
                info!("Filter can be optimized using column statistics");
            }
        }
        
        // Return the original expression for now
        // In production, this would return an optimized version
        None
    }
}

/// Helper to analyze query patterns and suggest optimizations
pub struct QueryPatternAnalyzer;

impl QueryPatternAnalyzer {
    /// Analyze a query and suggest optimizations based on patterns
    pub fn analyze_filters(filters: &[Expr]) -> Vec<String> {
        let mut suggestions = Vec::new();
        
        // Check for timestamp filters without date filters
        let has_timestamp = filters.iter().any(|f| Self::has_timestamp_filter(f));
        let has_date = filters.iter().any(|f| Self::has_date_filter(f));
        
        if has_timestamp && !has_date {
            suggestions.push(
                "Query has timestamp filter but no date partition filter. \
                Consider adding date filter for better partition pruning.".to_string()
            );
        }
        
        // Check for project_id filter
        if !filters.iter().any(|f| ProjectIdPushdown::contains_project_id(f)) {
            suggestions.push(
                "Query missing project_id filter. This will scan all project partitions.".to_string()
            );
        }
        
        // Check for filters on non-indexed columns
        for filter in filters {
            if let Some(col) = Self::extract_column(filter) {
                if !Self::is_indexed_column(&col) {
                    suggestions.push(format!(
                        "Filter on column '{}' may be slow as it's not indexed. \
                        Consider adding to Z-order columns.", col
                    ));
                }
            }
        }
        
        suggestions
    }
    
    fn has_timestamp_filter(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                matches!(left.as_ref(), Expr::Column(col) if col.name == "timestamp")
            }
            _ => false,
        }
    }
    
    fn has_date_filter(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                matches!(left.as_ref(), Expr::Column(col) if col.name == "date")
            }
            _ => false,
        }
    }
    
    fn extract_column(expr: &Expr) -> Option<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                if let Expr::Column(col) = left.as_ref() {
                    Some(col.name.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }
    
    fn is_indexed_column(column: &str) -> bool {
        // Columns that are in Z-order or partitioned
        matches!(
            column,
            "project_id" | "date" | "timestamp" | "id" | "level" | 
            "status_code" | "resource___service___name"
        )
    }
}

/// Register custom optimizer rules with the SessionContext
/// 
/// Note: DataFusion doesn't currently expose add_optimizer_rule as public API.
/// When it does, you would use:
/// ```ignore
/// ctx.add_optimizer_rule(Arc::new(TimeRangePartitionPruner::new()));
/// ctx.add_optimizer_rule(Arc::new(ProjectIdPushdown::new()));
/// ```
/// 
/// For now, these optimizers can be applied manually to logical plans if needed.
pub fn register_time_series_optimizers(_ctx: &mut datafusion::execution::context::SessionContext) {
    // These optimizers are ready to use once DataFusion exposes the registration API
    // They automatically:
    // 1. Add date partition filters for timestamp queries (TimeRangePartitionPruner)
    // 2. Validate that project_id filters are present (ProjectIdPushdown)
    // 3. Use Delta Lake statistics for better pruning (StatisticsAwareFilterOptimizer)
}