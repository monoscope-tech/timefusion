use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::RecordBatch,
        datatypes::{DataType, Field, Schema},
    },
    common::{Column, DFSchema, Result},
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, TaskContext,
        context::{QueryPlanner, SessionState},
    },
    logical_expr::{BinaryExpr, Expr, LogicalPlan, Operator, WriteOp},
    physical_plan::{DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties, stream::RecordBatchStreamAdapter},
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
};
use tracing::field::Empty;
use tracing::{Instrument, error, info, instrument};

use crate::database::Database;

/// Type alias for DML information extracted from logical plan
type DmlInfo = (String, String, Option<Expr>, Option<Vec<(String, Expr)>>);

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
    #[instrument(
        name = "dml.create_physical_plan",
        skip_all,
        fields(
            operation = Empty,
            table.name = Empty,
            project_id = Empty,
        )
    )]
    async fn create_physical_plan(&self, logical_plan: &LogicalPlan, session_state: &SessionState) -> Result<Arc<dyn ExecutionPlan>> {
        match logical_plan {
            LogicalPlan::Dml(dml) if matches!(dml.op, WriteOp::Update | WriteOp::Delete) => {
                let span = tracing::Span::current();
                let operation = if matches!(dml.op, WriteOp::Update) { "UPDATE" } else { "DELETE" };
                span.record("operation", operation);

                let input_exec = self.planner.create_physical_plan(&dml.input, session_state).await?;
                let is_update = matches!(dml.op, WriteOp::Update);
                let (table_name, project_id, predicate, assignments) = extract_dml_info(&dml.input, &dml.table_name.to_string(), is_update)?;

                span.record("table.name", &table_name.as_str());
                span.record("project_id", &project_id.as_str());

                Ok(Arc::new(if is_update {
                    DmlExec::update(
                        table_name,
                        project_id,
                        dml.output_schema.clone(),
                        predicate,
                        assignments.unwrap_or_default(),
                        input_exec,
                        self.database.clone(),
                    )
                } else {
                    DmlExec::delete(table_name, project_id, dml.output_schema.clone(), predicate, input_exec, self.database.clone())
                }))
            }
            _ => self.planner.create_physical_plan(logical_plan, session_state).await,
        }
    }
}

/// Extract DML information from logical plan
fn extract_dml_info(input: &LogicalPlan, table_name: &str, extract_assignments: bool) -> Result<DmlInfo> {
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
                project_id = extract_project_id(&filter.predicate).unwrap_or(project_id);
                current_plan = filter.input.as_ref();
            }
            LogicalPlan::TableScan(scan) => {
                project_id = scan.filters.iter().find_map(extract_project_id).unwrap_or(project_id);

                predicate = predicate.or_else(|| {
                    (!scan.filters.is_empty())
                        .then(|| {
                            scan.filters.iter().cloned().reduce(|acc, filter| {
                                Expr::BinaryExpr(BinaryExpr {
                                    left: Box::new(acc),
                                    op: Operator::And,
                                    right: Box::new(filter),
                                })
                            })
                        })
                        .flatten()
                });
                break;
            }
            _ => match current_plan.inputs().first() {
                Some(input) => current_plan = input,
                None => break,
            },
        }
    }

    if project_id.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "{} requires a project_id filter in WHERE clause",
            if extract_assignments { "UPDATE" } else { "DELETE" }
        )));
    }

    Ok((table_name.to_string(), project_id, predicate, assignments))
}

/// Extract assignments from projection
fn extract_assignments_from_projection(proj: &datafusion::logical_expr::Projection) -> Result<Vec<(String, Expr)>> {
    Ok(proj
        .expr
        .iter()
        .zip(proj.schema.fields())
        .filter_map(|(expr, field)| {
            let field_name = field.name();
            match expr {
                Expr::Column(col) if col.name == *field_name => None,
                Expr::Alias(alias) if alias.name == *field_name => {
                    (!matches!(&*alias.expr, Expr::Column(col) if col.name == *field_name)).then(|| (field_name.clone(), (*alias.expr).clone()))
                }
                Expr::Column(_) => None,
                _ => Some((field_name.clone(), expr.clone())),
            }
        })
        .collect())
}

/// Extract project_id from filter expression
fn extract_project_id(expr: &Expr) -> Option<String> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(col), Expr::Literal(val, _)) | (Expr::Literal(val, _), Expr::Column(col)) if col.name == "project_id" => Some(val.to_string()),
            _ => None,
        },
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => extract_project_id(left).or_else(|| extract_project_id(right)),
        _ => None,
    }
}

/// Unified DML execution plan
#[derive(Debug, Clone)]
pub struct DmlExec {
    op_type: DmlOperation,
    table_name: String,
    project_id: String,
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
    fn new(
        op_type: DmlOperation, table_name: String, project_id: String, predicate: Option<Expr>, assignments: Vec<(String, Expr)>,
        input: Arc<dyn ExecutionPlan>, database: Arc<Database>,
    ) -> Self {
        Self {
            op_type,
            table_name,
            project_id,
            predicate,
            assignments,
            input,
            database,
        }
    }

    pub fn update(
        table_name: String, project_id: String, _table_schema: Arc<DFSchema>, predicate: Option<Expr>, assignments: Vec<(String, Expr)>,
        input: Arc<dyn ExecutionPlan>, database: Arc<Database>,
    ) -> Self {
        Self::new(DmlOperation::Update, table_name, project_id, predicate, assignments, input, database)
    }

    pub fn delete(
        table_name: String, project_id: String, _table_schema: Arc<DFSchema>, predicate: Option<Expr>, input: Arc<dyn ExecutionPlan>, database: Arc<Database>,
    ) -> Self {
        Self::new(DmlOperation::Delete, table_name, project_id, predicate, vec![], input, database)
    }
}

impl DisplayAs for DmlExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let op_name = match self.op_type {
            DmlOperation::Update => "Update",
            DmlOperation::Delete => "Delete",
        };

        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "Delta{}Exec: table={}, project_id={}", op_name, self.table_name, self.project_id)?;

                if self.op_type == DmlOperation::Update && !self.assignments.is_empty() {
                    write!(
                        f,
                        ", assignments=[{}]",
                        self.assignments.iter().map(|(col, expr)| format!("{} = {}", col, expr)).collect::<Vec<_>>().join(", ")
                    )?;
                }

                if let Some(ref pred) = self.predicate {
                    write!(f, ", predicate={}", pred)?;
                }
                Ok(())
            }
            _ => write!(f, "Delta{}Exec", op_name),
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

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            ..(*self).clone()
        }))
    }

    #[instrument(
        name = "dml.execute",
        skip_all,
        fields(
            operation = match self.op_type { DmlOperation::Update => "UPDATE", DmlOperation::Delete => "DELETE" },
            table.name = %self.table_name,
            project_id = %self.project_id,
            has_predicate = self.predicate.is_some(),
            rows.affected = Empty,
        )
    )]
    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let span = tracing::Span::current();
        let field_name = match self.op_type {
            DmlOperation::Update => "rows_updated",
            DmlOperation::Delete => "rows_deleted",
        };

        let schema = Arc::new(Schema::new(vec![Field::new(field_name, DataType::Int64, false)]));
        let schema_clone = schema.clone();

        let op_type = self.op_type.clone();
        let table_name = self.table_name.clone();
        let project_id = self.project_id.clone();
        let assignments = self.assignments.clone();
        let predicate = self.predicate.clone();
        let database = self.database.clone();

        let future = async move {
            let result = match op_type {
                DmlOperation::Update => {
                    let update_span = tracing::trace_span!(parent: &span, "delta.update");
                    perform_delta_update(&database, &table_name, &project_id, predicate, assignments).instrument(update_span).await
                }
                DmlOperation::Delete => {
                    let delete_span = tracing::trace_span!(parent: &span, "delta.delete");
                    perform_delta_delete(&database, &table_name, &project_id, predicate).instrument(delete_span).await
                }
            };

            match &result {
                Ok(rows) => {
                    span.record("rows.affected", rows);
                }
                Err(_) => {}
            }

            result
                .and_then(|rows| {
                    RecordBatch::try_new(schema_clone, vec![Arc::new(datafusion::arrow::array::Int64Array::from(vec![rows as i64]))])
                        .map_err(|e| DataFusionError::External(Box::new(e)))
                })
                .map_err(|e| {
                    error!(
                        "Delta {} failed: {}",
                        match op_type {
                            DmlOperation::Update => "UPDATE",
                            DmlOperation::Delete => "DELETE",
                        },
                        e
                    );
                    e
                })
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, futures::stream::once(future))))
    }
}

/// Perform Delta UPDATE operation
#[instrument(
    name = "delta.perform_update",
    skip_all,
    fields(
        table.name = %table_name,
        project_id = %project_id,
        has_predicate = predicate.is_some(),
        assignments_count = assignments.len(),
        rows.updated = Empty,
    )
)]
pub async fn perform_delta_update(
    database: &Database, table_name: &str, project_id: &str, predicate: Option<Expr>, assignments: Vec<(String, Expr)>,
) -> Result<u64> {
    info!("Performing Delta UPDATE on table {} for project {}", table_name, project_id);

    let span = tracing::Span::current();
    let result = perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        let mut builder = delta_table.update();

        if let Some(pred) = predicate {
            builder = builder.with_predicate(convert_expr_to_delta(&pred)?);
        }

        for (column, value_expr) in assignments {
            builder = builder.with_update(column, convert_expr_to_delta(&value_expr)?);
        }

        builder
            .await
            .map(|(table, metrics)| (table, metrics.num_updated_rows as u64))
            .map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta UPDATE: {}", e)))
    })
    .await;

    if let Ok(rows) = &result {
        span.record("rows.updated", rows);
    }

    result
}

/// Perform Delta DELETE operation
#[instrument(
    name = "delta.perform_delete",
    skip_all,
    fields(
        table.name = %table_name,
        project_id = %project_id,
        has_predicate = predicate.is_some(),
        rows.deleted = Empty,
    )
)]
pub async fn perform_delta_delete(database: &Database, table_name: &str, project_id: &str, predicate: Option<Expr>) -> Result<u64> {
    info!("Performing Delta DELETE on table {} for project {}", table_name, project_id);

    let span = tracing::Span::current();
    let result = perform_delta_operation(database, table_name, project_id, |delta_table| async move {
        let mut builder = delta_table.delete();

        if let Some(pred) = predicate {
            builder = builder.with_predicate(convert_expr_to_delta(&pred)?);
        }

        builder
            .await
            .map(|(table, metrics)| (table, metrics.num_deleted_rows as u64))
            .map_err(|e| DataFusionError::Execution(format!("Failed to execute Delta DELETE: {}", e)))
    })
    .await;

    if let Ok(rows) = &result {
        span.record("rows.deleted", rows);
    }

    result
}

/// Common Delta operation logic
async fn perform_delta_operation<F, Fut>(database: &Database, table_name: &str, project_id: &str, operation: F) -> Result<u64>
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
        .ok_or_else(|| DataFusionError::Execution(format!("Table not found: {} for project {}", table_name, project_id)))?
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
            op: binary.op,
            right: Box::new(convert_expr_to_delta(&binary.right)?),
        })),
        _ => Ok(expr.clone()),
    }
}
