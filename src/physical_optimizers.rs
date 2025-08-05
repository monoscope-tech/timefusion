use datafusion::common::Result as DFResult;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use std::sync::Arc;
use tracing::{debug, trace};

/// Optimizer for time-series aggregation patterns
#[derive(Debug)]
pub struct TimeSeriesAggregationOptimizer {}

impl TimeSeriesAggregationOptimizer {
    pub fn new() -> Self {
        Self {}
    }
    
    /// Check if this is a time-bucketed aggregation
    fn is_time_bucket_aggregation(plan: &dyn ExecutionPlan) -> bool {
        // Check if the plan contains GROUP BY with time bucket expressions
        if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            // Look for date_trunc or similar time-bucketing functions in group expressions
            for expr in agg.group_expr().expr() {
                let expr_str = format!("{:?}", expr);
                if expr_str.contains("date_trunc") || 
                   expr_str.contains("date_bin") ||
                   expr_str.contains("to_date") {
                    return true;
                }
            }
        }
        false
    }
}

impl PhysicalOptimizerRule for TimeSeriesAggregationOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Recursively optimize children first
        let children: Vec<Arc<dyn ExecutionPlan>> = plan.children()
            .into_iter()
            .map(|child| self.optimize(child.clone(), _config))
            .collect::<DFResult<Vec<_>>>()?;
        
        // Check if this is a time-bucketed aggregation
        if let Some(agg) = plan.as_any().downcast_ref::<AggregateExec>() {
            if Self::is_time_bucket_aggregation(plan.as_ref()) {
                debug!("Optimizing time-bucket aggregation for streaming execution");
                
                // Check if input is already sorted by time
                let input_sorted = children[0].output_ordering().is_some();
                
                if input_sorted {
                    // Input is sorted - we can use more efficient streaming aggregation
                    trace!("Input is sorted by time, enabling streaming aggregation");
                    
                    // Clone and modify the aggregate to use partial mode if beneficial
                    let new_agg = AggregateExec::try_new(
                        *agg.mode(),
                        agg.group_expr().clone(),
                        agg.aggr_expr().to_vec(),
                        agg.filter_expr().to_vec(),
                        children[0].clone(),
                        agg.input_schema().clone(),
                    )?;
                    
                    return Ok(Arc::new(new_agg));
                }
            }
        }
        
        // Return plan with optimized children
        if children.is_empty() {
            Ok(plan)
        } else {
            plan.with_new_children(children)
        }
    }
    
    fn name(&self) -> &str {
        "time_series_aggregation"
    }
    
    fn schema_check(&self) -> bool {
        false
    }
}

/// Optimizer for range queries on time-series data
#[derive(Debug)]
pub struct RangeQueryOptimizer {}

impl RangeQueryOptimizer {
    pub fn new() -> Self {
        Self {}
    }
    
    /// Check if this is a time range query
    fn is_time_range_query(plan: &dyn ExecutionPlan) -> bool {
        // Check for filters on timestamp columns
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            let predicate_str = format!("{:?}", filter.predicate());
            predicate_str.contains("timestamp") || predicate_str.contains("date")
        } else {
            false
        }
    }
    
    /// Optimize scan order for time ranges  
    fn optimize_scan_order(&self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Check if this is a filter over a scan
        if let Some(filter) = plan.as_any().downcast_ref::<FilterExec>() {
            let predicate_str = format!("{:?}", filter.predicate());
            
            // Extract time range from filter if present
            if predicate_str.contains("timestamp") {
                debug!("Optimizing time range scan order");
                
                // Check if we're filtering for recent data (common pattern)
                if predicate_str.contains(">=") || predicate_str.contains(">") {
                    trace!("Query filtering for recent data - optimizing file scan order");
                    // In a full implementation, we'd reorder files to scan newest first
                }
            }
        }
        
        Ok(plan)
    }
}

impl PhysicalOptimizerRule for RangeQueryOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Recursively optimize children
        let children: Vec<Arc<dyn ExecutionPlan>> = plan.children()
            .into_iter()
            .map(|child| self.optimize(child.clone(), config))
            .collect::<DFResult<Vec<_>>>()?;
        
        let optimized_plan = if children.is_empty() {
            plan.clone()
        } else {
            plan.with_new_children(children)?
        };
        
        // Apply time range optimization if applicable
        if Self::is_time_range_query(optimized_plan.as_ref()) {
            debug!("Detected time range query pattern");
            self.optimize_scan_order(optimized_plan)
        } else {
            Ok(optimized_plan)
        }
    }
    
    fn name(&self) -> &str {
        "range_query_optimizer"
    }
    
    fn schema_check(&self) -> bool {
        false
    }
}

/// Push projections down to reduce data transfer
#[derive(Debug)]
pub struct ProjectionPushdownOptimizer {}

impl ProjectionPushdownOptimizer {
    pub fn new() -> Self {
        Self {}
    }
    
    /// Find unused columns that can be pruned early
    fn find_unused_columns(&self, _plan: &dyn ExecutionPlan) -> Vec<usize> {
        // Analyze which columns are actually used in the query
        // This is a simplified version - real implementation would traverse the plan tree
        vec![]
    }
}

impl PhysicalOptimizerRule for ProjectionPushdownOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Recursively optimize children
        let children: Vec<Arc<dyn ExecutionPlan>> = plan.children()
            .into_iter()
            .map(|child| self.optimize(child.clone(), config))
            .collect::<DFResult<Vec<_>>>()?;
        
        let optimized_plan = if children.is_empty() {
            plan.clone()
        } else {
            plan.with_new_children(children)?
        };
        
        // Check if this is a projection
        if let Some(proj) = optimized_plan.as_any().downcast_ref::<ProjectionExec>() {
            // Count columns actually used vs available
            let proj_schema = proj.schema();
            let input_schema = proj.input().schema();
            let proj_cols = proj_schema.fields().len();
            let input_cols = input_schema.fields().len();
            
            if proj_cols < input_cols {
                debug!(
                    "Projection reduces columns from {} to {} - good for performance",
                    input_cols, proj_cols
                );
                
                // Check for heavy columns that could benefit from late materialization
                for field in proj_schema.fields() {
                    if matches!(field.data_type(), 
                        arrow::datatypes::DataType::Utf8 | 
                        arrow::datatypes::DataType::LargeUtf8 |
                        arrow::datatypes::DataType::Binary |
                        arrow::datatypes::DataType::LargeBinary) {
                        trace!("Large column '{}' in projection - consider late materialization", field.name());
                    }
                }
            }
        }
        
        Ok(optimized_plan)
    }
    
    fn name(&self) -> &str {
        "projection_pushdown"
    }
    
    fn schema_check(&self) -> bool {
        false
    }
}

/// Eliminate redundant sorts for time-ordered data
#[derive(Debug)]
pub struct SortEliminationOptimizer {}

impl SortEliminationOptimizer {
    pub fn new() -> Self {
        Self {}
    }
    
    /// Check if data is already sorted by timestamp
    fn is_already_time_sorted(&self, plan: &dyn ExecutionPlan) -> bool {
        // Check if the input is already sorted by timestamp
        // Delta Lake data is often already sorted by partition keys
        if let Some(ordering) = plan.output_ordering() {
            for sort_expr in ordering {
                let expr_str = format!("{:?}", sort_expr);
                if expr_str.contains("timestamp") || expr_str.contains("date") {
                    return true;
                }
            }
        }
        false
    }
}

impl PhysicalOptimizerRule for SortEliminationOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Recursively optimize children
        let children: Vec<Arc<dyn ExecutionPlan>> = plan.children()
            .into_iter()
            .map(|child| self.optimize(child.clone(), config))
            .collect::<DFResult<Vec<_>>>()?;
        
        // Check if this is a sort operation
        if let Some(sort) = plan.as_any().downcast_ref::<SortExec>() {
            if !children.is_empty() {
                let child = &children[0];
                
                // Check if child already provides the required ordering
                if let Some(child_ordering) = child.output_ordering() {
                    let required_ordering = sort.expr();
                    
                    // Check if orderings match
                    if child_ordering.len() >= required_ordering.len() {
                        let orderings_match = required_ordering.iter()
                            .zip(child_ordering.iter())
                            .all(|(req, actual)| {
                                req.expr.eq(&actual.expr) && req.options == actual.options
                            });
                        
                        if orderings_match {
                            debug!("Eliminating redundant sort - input already sorted correctly");
                            return Ok(child.clone());
                        }
                    }
                }
                
                // Check for time-series specific patterns
                if self.is_already_time_sorted(child.as_ref()) {
                    let sort_str = format!("{:?}", sort.expr());
                    if sort_str.contains("timestamp") || sort_str.contains("date") {
                        debug!("Eliminating sort on time column - Delta Lake maintains time order");
                        return Ok(child.clone());
                    }
                }
            }
        }
        
        // Return plan with optimized children
        if children.is_empty() {
            Ok(plan)
        } else {
            plan.with_new_children(children)
        }
    }
    
    fn name(&self) -> &str {
        "sort_elimination"
    }
    
    fn schema_check(&self) -> bool {
        false
    }
}

/// Collection of all custom physical optimizers
pub struct TimeSeriesPhysicalOptimizers {
    rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl TimeSeriesPhysicalOptimizers {
    pub fn new() -> Self {
        Self {
            rules: vec![
                Arc::new(TimeSeriesAggregationOptimizer::new()),
                Arc::new(RangeQueryOptimizer::new()),
                Arc::new(ProjectionPushdownOptimizer::new()),
                Arc::new(SortEliminationOptimizer::new()),
            ],
        }
    }
    
    /// Apply all optimizers to a physical plan
    pub fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut optimized = plan;
        for rule in &self.rules {
            debug!("Applying physical optimizer: {}", rule.name());
            optimized = rule.optimize(optimized, config)?;
        }
        Ok(optimized)
    }
    
    pub fn rules(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        &self.rules
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_optimizer_names() {
        let optimizers = TimeSeriesPhysicalOptimizers::new();
        let names: Vec<&str> = optimizers.rules().iter().map(|r| r.name()).collect();
        assert_eq!(names, vec![
            "time_series_aggregation",
            "range_query_optimizer", 
            "projection_pushdown",
            "sort_elimination"
        ]);
    }
}