use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{LogicalPlan, WriteOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use std::sync::Arc;

/// Custom query planner that intercepts DML operations (UPDATE, DELETE)
/// This is a placeholder that will be implemented when Delta Lake support is added
pub struct DmlQueryPlanner {
    planner: DefaultPhysicalPlanner,
}

impl std::fmt::Debug for DmlQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmlQueryPlanner").finish()
    }
}

impl DmlQueryPlanner {
    pub fn new() -> Self {
        Self {
            planner: DefaultPhysicalPlanner::with_extension_planners(vec![]),
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
                // UPDATE operations will be implemented with Delta Lake support
                datafusion::common::plan_err!(
                    "UPDATE operations are not yet implemented. Delta Lake write support coming soon."
                )
            }
            LogicalPlan::Dml(dml) if dml.op == WriteOp::Delete => {
                // DELETE operations will be implemented with Delta Lake support
                datafusion::common::plan_err!(
                    "DELETE operations are not yet implemented. Delta Lake write support coming soon."
                )
            }
            // All other plans fallback to the default planner
            _ => self.planner.create_physical_plan(logical_plan, session_state).await,
        }
    }
}