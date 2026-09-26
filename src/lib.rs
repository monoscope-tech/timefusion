#![recursion_limit = "512"]

/// `ExecutionPlan::apply_expressions` for a node that owns no physical expressions.
macro_rules! no_physical_exprs {
    () => {
        fn apply_expressions(
            &self, _f: &mut dyn FnMut(&std::sync::Arc<dyn datafusion::physical_expr::PhysicalExpr>) -> datafusion::common::Result<datafusion::common::tree_node::TreeNodeRecursion>,
        ) -> datafusion::common::Result<datafusion::common::tree_node::TreeNodeRecursion> {
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        }
    };
}

pub mod config;
pub mod database;
pub mod dml;
pub mod maintenance_coordinator;
pub mod maintenance_sim;
pub mod observability;
pub mod read;
pub mod rollup;
pub mod rollup_journal;
pub mod schema;
pub mod server;
pub mod storage;
pub mod support;
pub mod tantivy;
pub mod write;
