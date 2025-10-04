use std::sync::Arc;
use std::any::Any;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    stream::RecordBatchStreamAdapter,
};
use deltalake::DeltaOps;
use tracing::{error, info};

use crate::database::Database;

/// Physical execution plan for UPDATE operations on Delta tables
#[derive(Debug)]
pub struct DeltaUpdateExec {
    /// Table name to update
    table_name: String,
    /// Project ID (extracted from filter predicates)
    project_id: String,
    /// Schema of the table
    table_schema: Arc<DFSchema>,
    /// Filter predicate from WHERE clause
    predicate: Option<Expr>,
    /// Update assignments (column_name -> new_value_expr)
    assignments: Vec<(String, Expr)>,
    /// Input plan that provides the matching rows
    input: Arc<dyn ExecutionPlan>,
    /// Database instance for accessing Delta tables
    database: Arc<Database>,
}

impl DeltaUpdateExec {
    pub fn new(
        table_name: String,
        project_id: String,
        table_schema: Arc<DFSchema>,
        predicate: Option<Expr>,
        assignments: Vec<(String, Expr)>,
        input: Arc<dyn ExecutionPlan>,
        database: Arc<Database>,
    ) -> Self {
        Self {
            table_name,
            project_id,
            table_schema,
            predicate,
            assignments,
            input,
            database,
        }
    }
}

impl DisplayAs for DeltaUpdateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeltaUpdateExec: table={}, project_id={}, assignments=[",
                    self.table_name, self.project_id
                )?;
                for (i, (col, expr)) in self.assignments.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} = {}", col, expr)?;
                }
                write!(f, "]")?;
                if let Some(ref pred) = self.predicate {
                    write!(f, ", predicate={}", pred)?;
                }
                Ok(())
            }
            _ => write!(f, "DeltaUpdateExec"),
        }
    }
}

#[async_trait]
impl ExecutionPlan for DeltaUpdateExec {
    fn name(&self) -> &'static str {
        "DeltaUpdateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        // Updates return a single batch with row count
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<datafusion::physical_plan::Distribution> {
        vec![datafusion::physical_plan::Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            table_name: self.table_name.clone(),
            project_id: self.project_id.clone(),
            table_schema: self.table_schema.clone(),
            predicate: self.predicate.clone(),
            assignments: self.assignments.clone(),
            input: children[0].clone(),
            database: self.database.clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let table_name = self.table_name.clone();
        let project_id = self.project_id.clone();
        let assignments = self.assignments.clone();
        let predicate = self.predicate.clone();
        let database = self.database.clone();

        let schema = Arc::new(Schema::new(vec![
            Field::new("rows_updated", DataType::Int64, false),
        ]));
        let schema_clone = schema.clone();

        let future = async move {
            match database.perform_delta_update(
                &table_name,
                &project_id,
                predicate,
                assignments,
            ).await {
                Ok(rows_updated) => {
                    let batch = RecordBatch::try_new(
                        schema_clone,
                        vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![rows_updated as i64]))],
                    );
                    
                    batch.map_err(|e| DataFusionError::External(Box::new(e)))
                }
                Err(e) => {
                    error!("Delta UPDATE failed: {}", e);
                    Err(e)
                }
            }
        };
        
        let stream = futures::stream::once(future);

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Internal implementation of Delta table update
pub async fn perform_delta_update_internal(
    database: &Database,
    table_name: &str,
    project_id: &str,
    predicate: Option<Expr>,
    assignments: Vec<(String, Expr)>,
) -> Result<u64> {
    info!(
        "Performing Delta UPDATE on table {} for project {}",
        table_name, project_id
    );

    // Get the Delta table from the database
    let table_key = (project_id.to_string(), table_name.to_string());
    let table_lock = database
        .project_configs()
        .read()
        .await
        .get(&table_key)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Table not found: {} for project {}",
                table_name, project_id
            ))
        })?
        .clone();

    let delta_table = table_lock.write().await;
    
    // Create the DeltaOps wrapper for the update operation
    let mut update_builder = DeltaOps(delta_table.clone()).update();

    // Apply the predicate if provided
    if let Some(pred) = predicate.clone() {
        // Convert DataFusion Expr to delta-rs Expression
        let delta_expr = convert_expr_to_delta(&pred)?;
        update_builder = update_builder.with_predicate(delta_expr);
    }

    // Apply the assignments
    for (column, value_expr) in assignments.clone() {
        // Convert the value expression to delta-rs format
        let delta_value_expr = convert_expr_to_delta(&value_expr)?;
        update_builder = update_builder.with_update(column, delta_value_expr);
    }

    // Execute the update
    match update_builder.await {
        Ok((new_table, metrics)) => {
            // Update the table reference in the lock
            drop(delta_table);
            *table_lock.write().await = new_table;

            // Return the number of rows updated
            let rows_updated = metrics.num_updated_rows;

            info!("Delta UPDATE completed: {} rows updated", rows_updated);
            Ok(rows_updated as u64)
        }
        Err(e) => {
            error!("Delta UPDATE failed: {}", e);
            Err(DataFusionError::Execution(format!(
                "Failed to execute Delta UPDATE: {}",
                e
            )))
        }
    }
}

/// Convert DataFusion Expr to Delta Lake Expression
fn convert_expr_to_delta(expr: &Expr) -> Result<Expr> {
    // Delta-rs UpdateBuilder expects DataFusion Expr directly
    // But we need to strip table qualifiers from column references
    match expr {
        Expr::Column(col) => {
            // Strip table qualification if present
            Ok(Expr::Column(datafusion::common::Column::from_name(&col.name)))
        }
        Expr::BinaryExpr(binary) => {
            // Recursively convert left and right expressions
            let left = convert_expr_to_delta(&binary.left)?;
            let right = convert_expr_to_delta(&binary.right)?;
            Ok(Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                left: Box::new(left),
                op: binary.op.clone(),
                right: Box::new(right),
            }))
        }
        _ => {
            // For other expression types, return as-is
            Ok(expr.clone())
        }
    }
}