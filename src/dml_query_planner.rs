use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, WriteOp, Expr, Projection, BinaryExpr, Operator};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use std::sync::Arc;

use crate::dml_executor::DeltaUpdateExec;

/// Custom query planner that intercepts DML operations (UPDATE, DELETE)
pub struct DmlQueryPlanner {
    planner: DefaultPhysicalPlanner,
    database: Arc<crate::database::Database>,
}

impl std::fmt::Debug for DmlQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmlQueryPlanner").finish()
    }
}

impl DmlQueryPlanner {
    pub fn new(database: Arc<crate::database::Database>) -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(vec![]),
            database,
        }
    }
}

#[async_trait]
impl QueryPlanner for DmlQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match logical_plan {
            LogicalPlan::Dml(dml) if dml.op == WriteOp::Update => {
                // Extract information from the DML input plan
                let (table_name, project_id, predicate, assignments) = 
                    extract_update_info(&dml.input, &dml.table_name.to_string())?;
                
                // Create the physical plan for the input (to get matching rows)
                let input_exec = self
                    .planner
                    .create_physical_plan(&dml.input, session_state)
                    .await?;
                
                // Create our Delta UPDATE execution plan
                let update_exec = DeltaUpdateExec::new(
                    table_name,
                    project_id,
                    dml.output_schema.clone(),
                    predicate,
                    assignments,
                    input_exec,
                    self.database.clone(),
                );
                
                Ok(Arc::new(update_exec))
            }
            LogicalPlan::Dml(dml) if dml.op == WriteOp::Delete => {
                // DELETE operations will be implemented similarly
                datafusion::common::plan_err!(
                    "DELETE operations are not yet implemented. Coming soon."
                )
            }
            // All other plans fallback to the default planner
            _ => self.planner.create_physical_plan(logical_plan, session_state).await,
        }
    }
}

/// Extract update information from the logical plan
fn extract_update_info(
    input: &LogicalPlan,
    table_name: &str,
) -> Result<(String, String, Option<Expr>, Vec<(String, Expr)>)> {
    // Navigate through the plan to find the Filter and Projection
    let mut current_plan = input;
    let mut predicate = None;
    let mut assignments = Vec::new();
    let mut project_id = String::new();
    
    loop {
        match current_plan {
            LogicalPlan::Projection(proj) => {
                // Extract assignments from the projection
                // In UPDATE plans, projections contain both original columns and new values
                assignments = extract_assignments_from_projection(proj)?;
                current_plan = proj.input.as_ref();
            }
            LogicalPlan::Filter(filter) => {
                // Extract the WHERE clause predicate
                predicate = Some(filter.predicate.clone());
                // Try to extract project_id from the predicate
                if let Some(pid) = extract_project_id(&filter.predicate) {
                    if project_id.is_empty() {
                        project_id = pid;
                    }
                }
                current_plan = filter.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => {
                // The filters are in the TableScan for UPDATE queries
                
                // Combine all filters with AND
                if !scan.filters.is_empty() {
                    let mut combined_predicate = scan.filters[0].clone();
                    for (i, filter) in scan.filters.iter().enumerate() {
                        if let Some(pid) = extract_project_id(filter) {
                            project_id = pid;
                        }
                        if i > 0 {
                            combined_predicate = Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(combined_predicate),
                                op: Operator::And,
                                right: Box::new(filter.clone()),
                            });
                        }
                    }
                    // Only set predicate if we don't already have one from a Filter node
                    if predicate.is_none() {
                        predicate = Some(combined_predicate);
                    }
                }
                break;
            }
            _ => {
                // Try to go deeper
                let inputs = current_plan.inputs();
                if !inputs.is_empty() {
                    current_plan = inputs[0];
                } else {
                    break;
                }
            }
        }
    }
    
    if project_id.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "UPDATE requires a project_id filter in WHERE clause".to_string()
        ));
    }
    
    Ok((table_name.to_string(), project_id, predicate, assignments))
}

/// Extract assignments from a projection in an UPDATE plan
fn extract_assignments_from_projection(proj: &Projection) -> Result<Vec<(String, Expr)>> {
    // In UPDATE plans, DataFusion creates projections where updated columns
    // have new expressions while unchanged columns reference the original
    let mut assignments = Vec::new();
    
    // Look for expressions that are not simple column references
    // In DataFusion, Projection has a expr field that is Vec<Expr>
    // We need to check if the expressions contain Alias nodes
    for expr in &proj.expr {
        match expr {
            Expr::Alias(alias) => {
                // Check if the inner expression is not just a column reference
                match &*alias.expr {
                    Expr::Column(col) if col.name == alias.name => continue, // Skip unchanged columns
                    _ => {
                        // This is an updated column
                        assignments.push((alias.name.clone(), (*alias.expr).clone()));
                    }
                }
            }
            _ => continue, // Skip non-aliased expressions
        }
    }
    
    // If no assignments found, it might be a different projection structure
    // Try to find assignments by comparing column names with expressions
    if assignments.is_empty() {
        let fields: Vec<_> = proj.schema.fields().iter().map(|f| f.name().clone()).collect();
        for (i, expr) in proj.expr.iter().enumerate() {
            if i < fields.len() {
                let field_name = &fields[i];
                match expr {
                    Expr::Column(col) if col.name == *field_name => continue,
                    Expr::Alias(alias) if alias.name == *field_name => {
                        match &*alias.expr {
                            Expr::Column(col) if col.name == *field_name => continue,
                            _ => assignments.push((field_name.clone(), (*alias.expr).clone())),
                        }
                    }
                    _ => {
                        // This might be an assignment
                        if !matches!(expr, Expr::Column(_)) {
                            assignments.push((field_name.clone(), expr.clone()));
                        }
                    }
                }
            }
        }
    }
    
    Ok(assignments)
}

/// Extract project_id from a filter expression
fn extract_project_id(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(col), Expr::Literal(val, _)) if col.name == "project_id" => {
                    // Extract string value from ScalarValue
                    match val {
                        datafusion::scalar::ScalarValue::Utf8(Some(s)) => Some(s.clone()),
                        _ => Some(val.to_string()),
                    }
                }
                (Expr::Literal(val, _), Expr::Column(col)) if col.name == "project_id" => {
                    // Extract string value from ScalarValue
                    match val {
                        datafusion::scalar::ScalarValue::Utf8(Some(s)) => Some(s.clone()),
                        _ => Some(val.to_string()),
                    }
                }
                _ => None,
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => {
            extract_project_id(left).or_else(|| extract_project_id(right))
        }
        _ => None,
    }
}