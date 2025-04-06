use std::{any::Any, collections::HashMap, env, sync::Arc};

use anyhow::Result;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::scalar::ScalarValue;

use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DFResult},
    execution::context::SessionContext,
    logical_expr::{dml::InsertOp, BinaryExpr},
    physical_expr::EquivalenceProperties,
    physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream},
};
use delta_kernel::arrow::record_batch::RecordBatch;
use deltalake::delta_datafusion::{DataFusionMixins, DeltaScanConfig, DeltaTableProvider};
use deltalake::{storage::StorageOptions, DeltaOps, DeltaTable, DeltaTableBuilder};
use futures::FutureExt;
use log::{info, warn};
use serde_arrow::schema::{SchemaLike, TracingOptions};
use tokio::sync::RwLock;
use url::Url;

use crate::persistent_queue::OtelLogsAndSpans;

type ProjectConfig = (String, StorageOptions, Arc<RwLock<DeltaTable>>);

pub type ProjectConfigs = Arc<RwLock<HashMap<String, ProjectConfig>>>;

#[derive(Debug)]
pub struct Database {
    project_configs: ProjectConfigs,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            project_configs: Arc::clone(&self.project_configs),
        }
    }
}

impl Database {
    pub async fn new() -> Result<Self> {
        let bucket = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET environment variable not set");
        let aws_endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());

        // Generate a unique prefix for this run's data
        let prefix = env::var("TIMEFUSION_TABLE_PREFIX").unwrap_or_else(|_| "timefusion".to_string());
        let storage_uri = format!("s3://{}/{}/?endpoint={}", bucket, prefix, aws_endpoint);
        info!("Storage URI configured: {}", storage_uri);

        let aws_url = Url::parse(&aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

        let project_configs = HashMap::new();

        let db = Self {
            project_configs: Arc::new(RwLock::new(project_configs)),
        };

        db.register_project("default", &storage_uri, None, None, None).await?;

        Ok(db)
    }

    pub async fn resolve_table(&self, project_id: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        let project_configs = self.project_configs.read().await;

        // Try to get the requested project table first
        if let Some((_, _, table)) = project_configs.get(project_id) {
            return Ok(table.clone());
        }

        // If not found and project_id is not "default", try the default table
        if project_id != "default" {
            if let Some((_, _, table)) = project_configs.get("default") {
                warn!("Project '{}' not found, falling back to default project", project_id);
                return Ok(table.clone());
            }
        }

        // If we get here, neither the requested project nor default exists
        Err(DataFusionError::Execution(format!(
            "Unknown project_id: {} and no default project found",
            project_id
        )))
    }

    pub async fn insert_records_batch(&self, _table: &str, batch: Vec<RecordBatch>) -> Result<()> {
        let (_conn_str, _options, table_ref) = {
            let configs = self.project_configs.read().await;
            configs.get("default").ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", "default"))?.clone()
        };

        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());

        // Use the mode parameter to ensure data is written correctly with partitioning
        *table = ops.write(batch).await?;
        Ok(())
    }

    pub async fn insert_records(&self, records: &Vec<crate::persistent_queue::OtelLogsAndSpans>) -> Result<()> {
        // TODO: insert records doesn't need to accept a project_id as they can be read from the
        // record.
        // Records should be grouped by span, and separated into groups then inserted into the
        // correct table.
        let (_conn_str, _options, table_ref) = {
            let configs = self.project_configs.read().await;
            configs.get("default").ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", "default"))?.clone()
        };

        let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(TracingOptions::default())?;
        let batch = serde_arrow::to_record_batch(&fields, &records)?;

        // Debug logging to understand what's in the batch
        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());

        // Use the mode parameter to ensure data is written correctly with partitioning
        *table = ops.write(vec![batch]).await?;
        Ok(())
    }

    pub async fn register_project(
        &self, project_id: &str, conn_str: &str, access_key: Option<&str>, secret_key: Option<&str>, endpoint: Option<&str>,
    ) -> Result<()> {
        let mut storage_options = StorageOptions::default();

        if let Some(key) = access_key.filter(|k| !k.is_empty()) {
            storage_options.0.insert("AWS_ACCESS_KEY_ID".to_string(), key.to_string());
        }

        if let Some(key) = secret_key.filter(|k| !k.is_empty()) {
            storage_options.0.insert("AWS_SECRET_ACCESS_KEY".to_string(), key.to_string());
        }

        if let Some(ep) = endpoint.filter(|e| !e.is_empty()) {
            storage_options.0.insert("AWS_ENDPOINT".to_string(), ep.to_string());
        }

        storage_options.0.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());

        let table = match DeltaTableBuilder::from_uri(conn_str).with_storage_options(storage_options.0.clone()).with_allow_http(true).load().await {
            Ok(table) => table,
            Err(err) => {
                warn!("table doesn't exist. creating new table. err: {:?}", err);

                // Create the table with project_id partitioning only for now
                // Timestamp partitioning is likely causing issues with nanosecond precision
                let delta_ops = DeltaOps::try_from_uri(&conn_str).await?;
                delta_ops
                    .create()
                    .with_columns(OtelLogsAndSpans::columns().unwrap_or_default())
                    .with_partition_columns(OtelLogsAndSpans::partitions())
                    .with_storage_options(storage_options.0.clone())
                    .await?
            }
        };

        // No need to create routing table here - it's done at the application level

        let mut configs = self.project_configs.write().await;
        configs.insert(project_id.to_string(), (conn_str.to_string(), storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
}

impl ProjectRoutingTable {
    pub fn new(default_project: String, database: Arc<Database>, schema: SchemaRef) -> Self {
        Self {
            default_project,
            database,
            schema,
        }
    }

    async fn resolve_table(&self, project_id: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        self.database.resolve_table(project_id).await
    }

    fn extract_project_id_from_filters(&self, filters: &[Expr]) -> Option<String> {
        // Look for expressions like "project_id = 'some_value'"
        for filter in filters {
            if let Some(project_id) = self.extract_project_id(filter) {
                return Some(project_id);
            }
        }
        None
    }

    fn extract_project_id(&self, expr: &Expr) -> Option<String> {
        match expr {
            // Binary expression: "project_id = 'value'"
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                // Check if this is an equality operation
                if *op == Operator::Eq {
                    // Check if left side is a column reference to "project_id"
                    if let Expr::Column(col) = left.as_ref() {
                        if col.name == "project_id" {
                            // Check if right side is a literal string
                            if let Expr::Literal(lit) = right.as_ref() {
                                if let ScalarValue::Utf8(Some(value)) = lit {
                                    return Some(value.clone());
                                }
                            }
                        }
                    }

                    // Also check if right side is the column (order might be flipped)
                    if let Expr::Column(col) = right.as_ref() {
                        if col.name == "project_id" {
                            // Check if left side is a literal string
                            if let Expr::Literal(lit) = left.as_ref() {
                                if let ScalarValue::Utf8(Some(value)) = lit {
                                    return Some(value.clone());
                                }
                            }
                        }
                    }
                }
                None
            }
            // // Recursive: AND, OR expressions
            // Expr::BooleanQuery { operands, .. } => {
            //     for operand in operands {
            //         if let Some(project_id) = self.extract_project_id(operand) {
            //             return Some(project_id);
            //         }
            //     }
            //     None
            // }
            // Look inside NOT expressions
            Expr::Not(inner) => self.extract_project_id(inner),
            _ => None,
        }
    }
}

#[async_trait]
impl TableProvider for ProjectRoutingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        // For example, assume that the default project’s table has the right schema.
        // (You might need to cache this schema during initialization.)
        let schema = match self.database.resolve_table(&self.default_project).now_or_never() {
            Some(Ok(table_lock)) => match table_lock.read().now_or_never() {
                Some(guard) => guard.snapshot().unwrap().arrow_schema().unwrap(),
                None => panic!("Failed to acquire read lock immediately"),
            },
            Some(Err(err)) => panic!("Failed to resolve table: {}", err),
            None => panic!("resolve_table did not complete immediately"),
        };
        schema.into()
    }

    // async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, _insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
    //     Ok(Arc::new(DeltaInsertExec::new(input, self.default_project.clone(), Arc::clone(&self.database))))
    // }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filter.iter().map(|_| TableProviderFilterPushDown::Inexact).collect())
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Log the projection indices for debugging
        if let Some(proj) = projection {
            info!("Scan projection indices: {:?}", proj);

            // Log original schema field names
            info!(
                "Original schema fields: {:?}",
                self.schema.fields().iter().enumerate().map(|(i, f)| format!("{}: {}", i, f.name())).collect::<Vec<_>>()
            );

            // Convert indices to field names for better logging
            let field_names: Vec<String> = proj
                .iter()
                .map(|&idx| {
                    if idx < self.schema.fields().len() {
                        format!("{}: {}", idx, self.schema.field(idx).name())
                    } else {
                        format!("unknown_field_{}", idx)
                    }
                })
                .collect();
            info!("Projected fields by index: {:?}", field_names);
        } else {
            info!("Scan with no projection");
        }

        // Log filters
        info!("Scan filters: {:?}", filters);

        // Get project_id from filters if possible, otherwise use default
        let project_id = self.extract_project_id_from_filters(filters).unwrap_or_else(|| self.default_project.clone());

        info!("Routing query to project_id: {}", project_id);

        let delta_table = self.database.resolve_table(&project_id).await?;
        let table = delta_table.read().await;
        table.scan(state, projection, filters, limit).await
    }
}

// #[derive(Debug)]
// pub struct DeltaInsertExec {
//     input: Arc<dyn ExecutionPlan>,
//     default_project: String,
//     database: Arc<Database>,
//     schema: SchemaRef,
//     plan_properties: PlanProperties,
// }
//
// impl DeltaInsertExec {
//     pub fn new(input: Arc<dyn ExecutionPlan>, default_project: String, database: Arc<Database>) -> Self {
//         let schema = OtelLogsAndSpans::schema_ref();
//         let plan_properties = Self::create_schema(&input, DeltaInsertExec::make_count_schema());
//         Self {
//             input,
//             default_project,
//             database,
//             schema,
//             plan_properties,
//         }
//     }
//
//     fn create_schema(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
//         let eq_properties = EquivalenceProperties::new(schema);
//         PlanProperties::new(
//             eq_properties,
//             Partitioning::UnknownPartitioning(1),
//             input.pipeline_behavior(),
//             input.boundedness(),
//         )
//     }
//
//     fn make_count_schema() -> SchemaRef {
//         // Define a schema.
//         Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]))
//     }
// }
//
// impl ExecutionPlan for DeltaInsertExec {
//     fn name(&self) -> &str {
//         "delta_insert_exec"
//     }
//
//     fn as_any(&self) -> &dyn Any {
//         self
//     }
//
//     fn schema(&self) -> SchemaRef {
//         self.schema.clone()
//     }
//
//     fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
//         vec![&self.input]
//     }
//
//     fn properties(&self) -> &PlanProperties {
//         &self.plan_properties
//     }
//
//     fn with_new_children(self: Arc<Self>, new_children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
//         Ok(Arc::new(Self::new(
//             new_children[0].clone(),
//             self.default_project.clone(),
//             Arc::clone(&self.database),
//         )))
//     }
//
//     fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
//         // Use a much simpler approach for now - just return an empty stream
//         // In a real implementation, this would write batches to the delta table
//
//         // Create an empty execution plan with the right schema
//         let empty_exec = datafusion::physical_plan::empty::EmptyExec::new(self.schema.clone());
//
//         // Execute the empty plan and return a stream of record batches
//         empty_exec.execute(partition, context)
//     }
// }
//
// impl DisplayAs for DeltaInsertExec {
//     fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "DeltaInsertExec")
//     }
// }
//
#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use chrono::{TimeZone, Utc};
    use datafusion::assert_batches_eq;
    use dotenv::dotenv;
    use tokio::time;
    use uuid::Uuid;

    use super::*;

    // Helper function to create a test database with a unique table prefix
    async fn setup_test_database() -> Result<(Database, SessionContext, String)> {
        let _ = env_logger::builder().is_test(true).try_init();
        dotenv().ok();

        // Set a unique test-specific prefix for a clean Delta table
        let test_prefix = format!("test-data-{}", Uuid::new_v4());
        unsafe {
            env::set_var("TIMEFUSION_TABLE_PREFIX", &test_prefix);
        }

        let db = Database::new().await?;
        let session_context = SessionContext::new();
        let schema = OtelLogsAndSpans::schema_ref();

        // Log schema fields for debugging
        info!("Test schema fields: {:?}", schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());

        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::new(db.clone()), schema);
        session_context.register_table("otel_logs_and_spans", Arc::new(routing_table))?;

        Ok((db, session_context, test_prefix))
    }

    // Helper function to refresh the table after modifications
    async fn refresh_table(db: &Database, session_context: &SessionContext) -> Result<()> {
        // Refresh the Delta table
        // Get the delta table using our resolve_table method
        let table_ref = db.resolve_table("default").await.map_err(|e| anyhow::anyhow!("Failed to resolve default table: {:?}", e))?;

        let mut table = table_ref.write().await;
        *table = deltalake::open_table(&table.table_uri()).await?;

        // Create a new ProjectRoutingTable with the refreshed database
        let schema = OtelLogsAndSpans::schema_ref();
        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::new(db.clone()), schema);

        // Re-register the table with the session context
        session_context.deregister_table("otel_logs_and_spans")?;
        session_context.register_table("otel_logs_and_spans", Arc::new(routing_table))?;

        // Add a small delay to ensure the changes propagate
        sleep(time::Duration::from_millis(100));

        Ok(())
    }

    // Helper function to create sample test records
    fn create_test_records() -> Vec<OtelLogsAndSpans> {
        let timestamp1 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let timestamp2 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 10, 0).unwrap();

        vec![
            OtelLogsAndSpans {
                project_id: "test_project".to_string(),
                timestamp: timestamp1,
                observed_timestamp: timestamp1,
                id: "span1".to_string(),
                name: "test_span_1".to_string(),
                context___trace_id: "trace1".to_string(),
                context___span_id: "span1".to_string(),
                start_time: timestamp1,
                duration: 100_000_000,
                status_code: Some("OK".to_string()),
                level: Some("INFO".to_string()),
                ..Default::default()
            },
            OtelLogsAndSpans {
                project_id: "test_project".to_string(),
                timestamp: timestamp2,
                observed_timestamp: timestamp2,
                id: "span2".to_string(),
                name: "test_span_2".to_string(),
                context___trace_id: "trace2".to_string(),
                context___span_id: "span2".to_string(),
                start_time: timestamp2,
                duration: 200_000_000,
                status_code: Some("ERROR".to_string()),
                level: Some("ERROR".to_string()),
                status_message: Some("Error occurred".to_string()),
                ..Default::default()
            },
        ]
    }

    // Helper to create SQL INSERT statements
    fn create_insert_sql(
        timestamp: &str, id: &str, name: &str, trace_id: &str, span_id: &str, duration: u64, level: &str, status_code: &str, status_message: Option<&str>,
    ) -> String {
        let sql = if let Some(message) = status_message {
            format!(
                "INSERT INTO otel_logs_and_spans (
                    project_id, timestamp, observed_timestamp, id, name, 
                    context___trace_id, context___span_id,
                    duration, start_time, level, status_code, status_message
                ) VALUES (
                    'test_project', '{}', '{}', '{}', '{}',
                    '{}', '{}', 
                    {}, '{}', '{}', '{}', '{}'
                )",
                timestamp, timestamp, id, name, trace_id, span_id, duration, timestamp, level, status_code, message
            )
        } else {
            format!(
                "INSERT INTO otel_logs_and_spans (
                    project_id, timestamp, observed_timestamp, id, name, 
                    context___trace_id, context___span_id,
                    duration, start_time, level, status_code
                ) VALUES (
                    'test_project', '{}', '{}', '{}', '{}',
                    '{}', '{}', 
                    {}, '{}', '{}', '{}'
                )",
                timestamp, timestamp, id, name, trace_id, span_id, duration, timestamp, level, status_code
            )
        };

        sql
    }

    #[tokio::test]
    async fn test_database_query() -> Result<()> {
        // Setup test database with helper function
        let (db, ctx, test_prefix) = setup_test_database().await?;
        log::info!("Using test-specific table prefix: {}", test_prefix);

        // Create sample records with different timestamps and durations
        let records = create_test_records();

        log::info!("Inserting records");
        db.insert_records(&records).await?;

        // Refresh the table to ensure it sees the newly written data
        log::info!("Refreshing table metadata");
        refresh_table(&db, &ctx).await?;

        // Add a small delay to ensure propagation
        sleep(time::Duration::from_millis(100));

        // Test 1: Basic count query to verify record insertion
        let count_df = ctx.sql("SELECT COUNT(*) as count FROM otel_logs_and_spans").await?;
        let result = count_df.collect().await?;

        #[rustfmt::skip]
        assert_batches_eq!(
            [
                "+-------+", 
                "| count |", 
                "+-------+", 
                "| 2     |", 
                "+-------+",
            ], &result);

        // TODO: The current implementation has schema mapping issues that need to be resolved
        // For now, we'll skip the detailed tests and just verify that we can retrieve records

        // Test 2: Query with field selection and ordering
        log::info!("Testing field selection and ordering");
        let df = ctx.sql("SELECT timestamp, name, status_code, level FROM otel_logs_and_spans ORDER BY name").await?;
        let result = df.collect().await?;

        assert_batches_eq!(
            [
                "+---------------------+-------------+-------------+-------+",
                "| timestamp           | name        | status_code | level |",
                "+---------------------+-------------+-------------+-------+",
                "| 2023-01-01T10:00:00 | test_span_1 | OK          | INFO  |",
                "| 2023-01-01T10:10:00 | test_span_2 | ERROR       | ERROR |",
                "+---------------------+-------------+-------------+-------+",
            ],
            &result
        );

        // Test 3: Filtering by project_id and level
        log::info!("Testing filtering by project_id and level");
        let df = ctx
            .sql("SELECT name, level, status_code, status_message FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'")
            .await?;
        let result = df.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+-------+-------------+----------------+",
                "| name        | level | status_code | status_message |",
                "+-------------+-------+-------------+----------------+",
                "| test_span_2 | ERROR | ERROR       | Error occurred |",
                "+-------------+-------+-------------+----------------+",
            ],
            &result
        );

        // Test 4: Complex query with multiple data types (including timestamp and end_time)
        // Note: For timestamp columns, we need to format them for the test to use assert_batches_eq
        log::info!("Testing complex query with multiple data types");
        let df = ctx
            .sql(
                "
                    SELECT
                        id,
                        name,
                        project_id,
                        duration,
                        CAST(duration / 1000000 AS INTEGER) as duration_ms,
                        context___trace_id,
                        status_code,
                        level,
                        to_char(timestamp, '%Y-%m-%d %H:%M') as formatted_timestamp,
                        to_char(end_time, 'YYYY-MM-DD HH24:MI') as formatted_end_time,
                        CASE WHEN status_code = 'ERROR' THEN status_message ELSE NULL END as conditional_message
                    FROM otel_logs_and_spans
                    ORDER BY id
                ",
            )
            .await?;
        let result = df.collect().await?;

        assert_batches_eq!(
                [
                    "+-------+-------------+--------------+-----------+-------------+--------------------+-------------+-------+---------------------+--------------------+---------------------+",
                    "| id    | name        | project_id   | duration  | duration_ms | context___trace_id | status_code | level | formatted_timestamp | formatted_end_time | conditional_message |",
                    "+-------+-------------+--------------+-----------+-------------+--------------------+-------------+-------+---------------------+--------------------+---------------------+",
                    "| span1 | test_span_1 | test_project | 100000000 | 100         | trace1             | OK          | INFO  | 2023-01-01 10:00    |                    |                     |",
                    "| span2 | test_span_2 | test_project | 200000000 | 200         | trace2             | ERROR       | ERROR | 2023-01-01 10:10    |                    | Error occurred      |",
                    "+-------+-------------+--------------+-----------+-------------+--------------------+-------------+-------+---------------------+--------------------+---------------------+",
                ],
                &result
            );

        // Test 5: Timestamp filtering
        log::info!("Testing timestamp filtering");
        let df = ctx.sql("SELECT COUNT(*) as count FROM otel_logs_and_spans WHERE timestamp > '2023-01-01T10:05:00.000000Z'").await?;
        let result = df.collect().await?;

        #[rustfmt::skip]
            assert_batches_eq!(
                [
                    "+-------+",
                    "| count |",
                    "+-------+",
                    "| 1     |",
                    "+-------+",
                ],
                &result
            );

        // Test 6: Duration filtering
        log::info!("Testing duration filtering");
        let df = ctx.sql("SELECT name FROM otel_logs_and_spans WHERE duration > 150000000").await?;
        let result = df.collect().await?;

        #[rustfmt::skip]
            assert_batches_eq!(
                [
                    "+-------------+",
                    "| name        |",
                    "+-------------+",
                    "| test_span_2 |",
                    "+-------------+",
                ],
                &result
            );

        // Test 7: Complex filtering with timestamp columns in ISO 8601 format
        log::info!("Testing complex filtering with timestamp columns in ISO 8601 format");
        let df = ctx
            .sql(
                "
                    WITH time_data AS (
                        SELECT
                            name,
                            status_code,
                            level,
                            CAST(duration / 1000000 AS INTEGER) as duration_ms,
                            timestamp,
                            to_char(timestamp, '%Y-%m-%d') as date_only,
                            EXTRACT(HOUR FROM timestamp) as hour,
                            to_char(timestamp, '%Y-%m-%dT%H:%M:%S%.6fZ') as iso_timestamp
                        FROM otel_logs_and_spans
                        WHERE
                            project_id = 'test_project'
                            AND timestamp > '2023-01-01T10:05:00.000000Z'
                    )
                    SELECT
                        name,
                        status_code,
                        level,
                        duration_ms,
                        date_only,
                        hour,
                        iso_timestamp
                    FROM time_data
                    ORDER BY name
                ",
            )
            .await?;
        let result = df.collect().await?;

        assert_batches_eq!(
            [
                "+-------------+-------------+-------+-------------+------------+------+-----------------------------+",
                "| name        | status_code | level | duration_ms | date_only  | hour | iso_timestamp               |",
                "+-------------+-------------+-------+-------------+------------+------+-----------------------------+",
                "| test_span_2 | ERROR       | ERROR | 200         | 2023-01-01 | 10   | 2023-01-01T10:10:00.000000Z |",
                "+-------------+-------------+-------+-------------+------------+------+-----------------------------+",
            ],
            &result
        );

        Ok(())
    }
}
