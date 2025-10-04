use std::sync::Arc;
use std::any::Any;

use async_trait::async_trait;
use datafusion::{
    arrow::{array::RecordBatch, datatypes::{DataType, Field, Schema}},
    common::{DFSchema, Result, Column},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext, context::{QueryPlanner, SessionState}},
    logical_expr::{LogicalPlan, WriteOp, Expr, BinaryExpr, Operator},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Distribution, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use deltalake::DeltaOps;
use tracing::{error, info};

use crate::database::Database;

/// Custom query planner that intercepts DML operations
pub struct DmlQueryPlanner {
    planner: DefaultPhysicalPlanner,
    database: Arc<Database>,
}

impl std::fmt::Debug for DmlQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DmlQueryPlanner").finish()
    }
}

impl DmlQueryPlanner {
    pub fn new(database: Arc<Database>) -> Self {
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
            LogicalPlan::Dml(dml) => {
                let input_exec = self.planner
                    .create_physical_plan(&dml.input, session_state)
                    .await?;
                
                match dml.op {
                    WriteOp::Update => {
                        let (table_name, project_id, predicate, assignments) = 
                            extract_dml_info(&dml.input, &dml.table_name.to_string(), true)?;
                        
                        Ok(Arc::new(DmlExec::update(
                            table_name,
                            project_id,
                            dml.output_schema.clone(),
                            predicate,
                            assignments.unwrap_or_default(),
                            input_exec,
                            self.database.clone(),
                        )))
                    }
                    WriteOp::Delete => {
                        let (table_name, project_id, predicate, _) = 
                            extract_dml_info(&dml.input, &dml.table_name.to_string(), false)?;
                        
                        Ok(Arc::new(DmlExec::delete(
                            table_name,
                            project_id,
                            dml.output_schema.clone(),
                            predicate,
                            input_exec,
                            self.database.clone(),
                        )))
                    }
                    _ => self.planner.create_physical_plan(logical_plan, session_state).await,
                }
            }
            _ => self.planner.create_physical_plan(logical_plan, session_state).await,
        }
    }
}

/// Extract DML information from logical plan
fn extract_dml_info(
    input: &LogicalPlan,
    table_name: &str,
    extract_assignments: bool,
) -> Result<(String, String, Option<Expr>, Option<Vec<(String, Expr)>>)> {
    let mut current_plan = input;
    let mut predicate = None;
    let mut assignments = None;
    let mut project_id = String::new();
    
    loop {
        match current_plan {
            LogicalPlan::Projection(proj) if extract_assignments => {
                assignments = Some(extract_assignments_from_projection(proj)?);
                current_plan = proj.input.as_ref();
            }
            LogicalPlan::Filter(filter) => {
                predicate = Some(filter.predicate.clone());
                if let Some(pid) = extract_project_id(&filter.predicate) {
                    project_id = pid;
                }
                current_plan = filter.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => {
                if !scan.filters.is_empty() {
                    let combined = scan.filters.iter()
                        .enumerate()
                        .fold(None, |acc, (i, filter)| {
                            if project_id.is_empty() {
                                if let Some(pid) = extract_project_id(filter) {
                                    project_id = pid;
                                }
                            }
                            match i {
                                0 => Some(filter.clone()),
                                _ => acc.map(|prev| Expr::BinaryExpr(BinaryExpr {
                                    left: Box::new(prev),
                                    op: Operator::And,
                                    right: Box::new(filter.clone()),
                                })),
                            }
                        });
                    
                    if predicate.is_none() {
                        predicate = combined;
                    }
                }
                break;
            }
            _ => {
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
        return Err(DataFusionError::Plan(
            format!("{} requires a project_id filter in WHERE clause", 
                if extract_assignments { "UPDATE" } else { "DELETE" })
        ));
    }
    
    Ok((table_name.to_string(), project_id, predicate, assignments))
}

/// Extract assignments from projection
fn extract_assignments_from_projection(proj: &datafusion::logical_expr::Projection) -> Result<Vec<(String, Expr)>> {
    let fields: Vec<_> = proj.schema.fields().iter()
        .map(|f| f.name().clone())
        .collect();
    
    Ok(proj.expr.iter()
        .zip(&fields)
        .filter_map(|(expr, field_name)| {
            match expr {
                Expr::Column(col) if col.name == *field_name => None,
                Expr::Alias(alias) if alias.name == *field_name => {
                    match &*alias.expr {
                        Expr::Column(col) if col.name == *field_name => None,
                        _ => Some((field_name.clone(), (*alias.expr).clone())),
                    }
                }
                _ if !matches!(expr, Expr::Column(_)) => {
                    Some((field_name.clone(), expr.clone()))
                }
                _ => None,
            }
        })
        .collect())
}

/// Extract project_id from filter expression
fn extract_project_id(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(col), Expr::Literal(val, _)) | 
                (Expr::Literal(val, _), Expr::Column(col)) if col.name == "project_id" => {
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

/// Unified DML execution plan
#[derive(Debug)]
pub struct DmlExec {
    op_type: DmlOperation,
    table_name: String,
    project_id: String,
    table_schema: Arc<DFSchema>,
    predicate: Option<Expr>,
    assignments: Vec<(String, Expr)>,
    input: Arc<dyn ExecutionPlan>,
    database: Arc<Database>,
}

#[derive(Debug, Clone, PartialEq)]
enum DmlOperation {
    Update,
    Delete,
}

impl DmlExec {
    pub fn update(
        table_name: String,
        project_id: String,
        table_schema: Arc<DFSchema>,
        predicate: Option<Expr>,
        assignments: Vec<(String, Expr)>,
        input: Arc<dyn ExecutionPlan>,
        database: Arc<Database>,
    ) -> Self {
        Self {
            op_type: DmlOperation::Update,
            table_name,
            project_id,
            table_schema,
            predicate,
            assignments,
            input,
            database,
        }
    }

    pub fn delete(
        table_name: String,
        project_id: String,
        table_schema: Arc<DFSchema>,
        predicate: Option<Expr>,
        input: Arc<dyn ExecutionPlan>,
        database: Arc<Database>,
    ) -> Self {
        Self {
            op_type: DmlOperation::Delete,
            table_name,
            project_id,
            table_schema,
            predicate,
            assignments: vec![],
            input,
            database,
        }
    }
}

impl DisplayAs for DmlExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "Delta{}Exec: table={}, project_id={}", 
                    if self.op_type == DmlOperation::Update { "Update" } else { "Delete" },
                    self.table_name, 
                    self.project_id
                )?;
                
                if self.op_type == DmlOperation::Update && !self.assignments.is_empty() {
                    write!(f, ", assignments=[")?;
                    for (i, (col, expr)) in self.assignments.iter().enumerate() {
                        if i > 0 { write!(f, ", ")?; }
                        write!(f, "{} = {}", col, expr)?;
                    }
                    write!(f, "]")?;
                }
                
                if let Some(ref pred) = self.predicate {
                    write!(f, ", predicate={}", pred)?;
                }
                Ok(())
            }
            _ => write!(f, "Delta{}Exec", 
                if self.op_type == DmlOperation::Update { "Update" } else { "Delete" }
            ),
        }
    }
}

#[async_trait]
impl ExecutionPlan for DmlExec {
    fn name(&self) -> &'static str {
        match self.op_type {
            DmlOperation::Update => "DeltaUpdateExec",
            DmlOperation::Delete => "DeltaDeleteExec",
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            op_type: self.op_type.clone(),
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
        let op_type = self.op_type.clone();
        let table_name = self.table_name.clone();
        let project_id = self.project_id.clone();
        let assignments = self.assignments.clone();
        let predicate = self.predicate.clone();
        let database = self.database.clone();

        let field_name = match op_type {
            DmlOperation::Update => "rows_updated",
            DmlOperation::Delete => "rows_deleted",
        };
        
        let schema = Arc::new(Schema::new(vec![
            Field::new(field_name, DataType::Int64, false),
        ]));
        let schema_clone = schema.clone();

        let future = async move {
            let result = match op_type {
                DmlOperation::Update => {
                    perform_delta_update(
                        &database, &table_name, &project_id, predicate, assignments
                    ).await
                }
                DmlOperation::Delete => {
                    perform_delta_delete(
                        &database, &table_name, &project_id, predicate
                    ).await
                }
            };

            match result {
                Ok(rows_affected) => {
                    RecordBatch::try_new(
                        schema_clone,
                        vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![rows_affected as i64]))],
                    ).map_err(|e| DataFusionError::External(Box::new(e)))
                }
                Err(e) => {
                    error!("Delta {} failed: {}", 
                        if matches!(op_type, DmlOperation::Update) { "UPDATE" } else { "DELETE" }, 
                        e
                    );
                    Err(e)
                }
            }
        };
        
        let stream = futures::stream::once(future);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Perform Delta UPDATE operation
pub async fn perform_delta_update(
    database: &Database,
    table_name: &str,
    project_id: &str,
    predicate: Option<Expr>,
    assignments: Vec<(String, Expr)>,
) -> Result<u64> {
    info!("Performing Delta UPDATE on table {} for project {}", table_name, project_id);
    
    perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        let mut update_builder = DeltaOps(delta_table).update();
        
        if let Some(pred) = predicate {
            update_builder = update_builder.with_predicate(convert_expr_to_delta(&pred)?);
        }
        
        for (column, value_expr) in assignments {
            update_builder = update_builder.with_update(column, convert_expr_to_delta(&value_expr)?);
        }
        
        update_builder.await
            .map(|(table, metrics)| (table, metrics.num_updated_rows as u64))
            .map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta UPDATE: {}", e)))
    }).await
}

/// Perform Delta DELETE operation
pub async fn perform_delta_delete(
    database: &Database,
    table_name: &str,
    project_id: &str,
    predicate: Option<Expr>,
) -> Result<u64> {
    info!("Performing Delta DELETE on table {} for project {}", table_name, project_id);
    
    perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        let mut delete_builder = DeltaOps(delta_table).delete();
        
        if let Some(pred) = predicate {
            delete_builder = delete_builder.with_predicate(convert_expr_to_delta(&pred)?);
        }
        
        delete_builder.await
            .map(|(table, metrics)| (table, metrics.num_deleted_rows as u64))
            .map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta DELETE: {}", e)))
    }).await
}

/// Common Delta operation logic
async fn perform_delta_operation<F, Fut>(
    database: &Database,
    table_name: &str,
    project_id: &str,
    operation: F,
) -> Result<u64> 
where
    F: FnOnce(deltalake::DeltaTable) -> Fut,
    Fut: std::future::Future<Output = Result<(deltalake::DeltaTable, u64)>>,
{
    let table_key = (project_id.to_string(), table_name.to_string());
    let table_lock = database
        .project_configs()
        .read()
        .await
        .get(&table_key)
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Table not found: {} for project {}", table_name, project_id
            ))
        })?
        .clone();

    let delta_table = table_lock.write().await;
    let (new_table, rows_affected) = operation(delta_table.clone()).await?;
    
    drop(delta_table);
    *table_lock.write().await = new_table;
    
    Ok(rows_affected)
}

/// Convert DataFusion Expr to Delta-compatible format
fn convert_expr_to_delta(expr: &Expr) -> Result<Expr> {
    match expr {
        Expr::Column(col) => Ok(Expr::Column(Column::from_name(&col.name))),
        Expr::BinaryExpr(binary) => Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(convert_expr_to_delta(&binary.left)?),
            op: binary.op.clone(),
            right: Box::new(convert_expr_to_delta(&binary.right)?),
        })),
        _ => Ok(expr.clone()),
    }
}

// Public API functions for Database
pub use self::perform_delta_update as perform_delta_update_internal;
pub use self::perform_delta_delete as perform_delta_delete_internal;