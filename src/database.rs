use std::{any::Any, collections::HashMap, env, sync::Arc};

use anyhow::Result;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
// Import TableProviderFilterPushDown from the logical_expr module.
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DFResult},
    execution::context::SessionContext,
    logical_expr::{BinaryExpr, Expr, Operator, dml::InsertOp},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayFormatType,
        ExecutionPlan,
        ExecutionPlanProperties, // <-- added ExecutionPlanProperties
        Partitioning,
        PlanProperties,
        SendableRecordBatchStream,
        stream::RecordBatchStreamAdapter,
    },
    scalar::ScalarValue,
};
use delta_kernel::arrow::record_batch::RecordBatch as DeltaRecordBatch;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder, storage::StorageOptions};
use futures::StreamExt; // removed FutureExt since not used
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
        // Since register_handlers returns () we remove the '?'.
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
        if let Some((_, _, table)) = project_configs.get(project_id) {
            return Ok(table.clone());
        }
        if project_id != "default" {
            if let Some((_, _, table)) = project_configs.get("default") {
                warn!("Project '{}' not found, falling back to default project", project_id);
                return Ok(table.clone());
            }
        }
        Err(DataFusionError::Execution(format!(
            "Unknown project_id: {} and no default project found",
            project_id
        )))
    }

    pub async fn insert_records_batch(&self, _table: &str, batch: Vec<datafusion::arrow::record_batch::RecordBatch>) -> Result<()> {
        // Get the table reference for the default project.
        let (_conn_str, _options, table_ref) = {
            let configs = self.project_configs.read().await;
            configs.get("default").ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", "default"))?.clone()
        };
        info!("insert_records_batch: acquired table access");
        let mut table = table_ref.write().await;
        let ops = DeltaOps(table.clone());
        info!("insert_records_batch: writing batch of size {:?}", batch.len());
        // Use partition columns from OtelLogsAndSpans.
        let write_op = ops.write(batch).with_partition_columns(OtelLogsAndSpans::partitions());
        *table = write_op.await?;
        info!("insert_records_batch: write completed successfully");
        Ok(())
    }

    pub async fn insert_records(&self, records: &Vec<OtelLogsAndSpans>) -> Result<()> {
        // Convert records to an Arrow RecordBatch using serde_arrow.
        let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(TracingOptions::default())?;
        let batch = serde_arrow::to_record_batch(&fields, &records)?;
        info!("Converting {} records to batch for insertion", records.len());
        self.insert_records_batch("default", vec![batch]).await
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
                warn!("Table doesn't exist. Creating new table. Err: {:?}", err);
                let delta_ops = DeltaOps::try_from_uri(&conn_str).await?;
                delta_ops
                    .create()
                    .with_columns(OtelLogsAndSpans::columns().unwrap_or_default())
                    .with_partition_columns(OtelLogsAndSpans::partitions())
                    .with_storage_options(storage_options.0.clone())
                    .await?
            }
        };

        let mut configs = self.project_configs.write().await;
        configs.insert(project_id.to_string(), (conn_str.to_string(), storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}

#[derive(Debug)]
pub struct ProjectRoutingTable {
    default_project: String,
    database:        Arc<Database>,
    schema:          SchemaRef,
}

impl ProjectRoutingTable {
    pub fn new(default_project: String, database: Arc<Database>, schema: SchemaRef) -> Self {
        Self {
            default_project,
            database,
            schema,
        }
    }

    fn extract_project_id_from_filters(&self, filters: &[Expr]) -> Option<String> {
        for filter in filters {
            if let Some(project_id) = self.extract_project_id(filter) {
                return Some(project_id);
            }
        }
        None
    }

    fn extract_project_id(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::Eq {
                    if let Expr::Column(col) = left.as_ref() {
                        if col.name == "project_id" {
                            if let Expr::Literal(lit) = right.as_ref() {
                                if let ScalarValue::Utf8(Some(value)) = lit {
                                    return Some(value.clone());
                                }
                            }
                        }
                    }
                    if let Expr::Column(col) = right.as_ref() {
                        if col.name == "project_id" {
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
        self.schema.clone()
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let project_id = self.extract_project_id_from_filters(filters).unwrap_or_else(|| self.default_project.clone());
        info!("Routing query to project_id: {}", project_id);
        let delta_table = self.database.resolve_table(&project_id).await?;
        let table = delta_table.read().await;
        table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters.iter().map(|_| TableProviderFilterPushDown::Inexact).collect())
    }

    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, _insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DeltaInsertExec::new(input, self.default_project.clone(), Arc::clone(&self.database))))
    }
}


#[derive(Debug)]
pub struct DeltaInsertExec {
    input:           Arc<dyn ExecutionPlan>,
    default_project: String,
    database:        Arc<Database>,
    schema:          SchemaRef,
    plan_properties: PlanProperties,
}

impl DeltaInsertExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, default_project: String, database: Arc<Database>) -> Self {
        let schema = Self::make_count_schema();
        let plan_properties = Self::create_schema(&input, schema.clone());
        Self {
            input,
            default_project,
            database,
            schema,
            plan_properties,
        }
    }

    fn create_schema(input: &Arc<dyn ExecutionPlan>, schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(), // Requires ExecutionPlanProperties trait.
            input.boundedness(),       // Requires ExecutionPlanProperties trait.
        )
    }

    fn make_count_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]))
    }
}

impl ExecutionPlan for DeltaInsertExec {
    fn name(&self) -> &str {
        "delta_insert_exec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn with_new_children(self: Arc<Self>, new_children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            new_children[0].clone(),
            self.default_project.clone(),
            Arc::clone(&self.database),
        )))
    }

    fn execute(&self, partition: usize, context: Arc<datafusion::execution::TaskContext>) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal("DeltaInsertExec can only be called on partition 0".to_string()));
        }

        let db = self.database.clone();
        let default_project = self.default_project.clone();
        let input = self.input.clone();
        let count_schema = self.schema.clone();

        let stream = futures::stream::once(async move {
            let result = async {
                let input_stream = input.execute(0, context.clone())?;
                let mut batches = Vec::new();
                let mut stream = input_stream;
                let mut total_rows = 0;
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            total_rows += batch.num_rows();
                            batches.push(batch);
                        }
                        Err(e) => return Err(DataFusionError::Execution(format!("Error fetching batch: {}", e))),
                    }
                }
                if !batches.is_empty() {
                    let delta_batches: Vec<DeltaRecordBatch> = batches
                        .into_iter()
                        .map(|batch| DeltaRecordBatch::try_new(batch.schema().into(), batch.columns().to_vec()))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| DataFusionError::Execution(format!("Failed to convert record batch: {}", e)))?;
                    db.insert_records_batch(&default_project, delta_batches)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("Failed to insert records: {}", e)))?;
                }
                let array = arrow::array::UInt64Array::from(vec![total_rows as u64]);
                let batch = datafusion::arrow::record_batch::RecordBatch::try_new(count_schema.clone(), vec![Arc::new(array)])
                    .map_err(|e| DataFusionError::Execution(format!("Failed to create count batch: {}", e)))?;
                Ok(batch)
            }
            .await;
            result
        })
        .boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(Self::make_count_schema(), stream)))
    }
}

impl datafusion::physical_plan::DisplayAs for DeltaInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DeltaInsertExec")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringArray, TimestampNanosecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use chrono::{TimeZone, Utc};
    use datafusion::{assert_batches_eq, prelude::SessionContext};
    use futures::FutureExt;
    use tempfile::tempdir;
    use tokio::time::{Duration, sleep};
    use uuid::Uuid;

    use super::*;

    // Helper: set up a test database and register a routing table.
    async fn setup_test_database() -> Result<(Arc<Database>, SessionContext, String)> {
        let test_prefix = format!("test-data-{}", Uuid::new_v4());
        unsafe {
            env::set_var("TIMEFUSION_TABLE_PREFIX", &test_prefix);
        }
        let db = Arc::new(Database::new().await?);
        let session_context = SessionContext::new();
        let schema = OtelLogsAndSpans::schema_ref();
        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::clone(&db), schema);
        session_context.register_table("otel_logs_and_spans", Arc::new(routing_table))?;
        Ok((db, session_context, test_prefix))
    }

    // Helper: refresh table metadata.
    async fn refresh_table(db: &Database, session_context: &SessionContext) -> Result<()> {
        let table_ref = db.resolve_table("default").await?;
        let mut table = table_ref.write().await;
        *table = deltalake::open_table(&table.table_uri()).await?;
        let schema = OtelLogsAndSpans::schema_ref();
        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::new(db.clone()), schema);
        session_context.deregister_table("otel_logs_and_spans")?;
        session_context.register_table("otel_logs_and_spans", Arc::new(routing_table))?;
        sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    // Helper: create sample records.
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

    #[tokio::test]
    async fn test_database_query() -> Result<()> {
        let (db, ctx, test_prefix) = setup_test_database().await?;
        log::info!("Using test table prefix: {}", test_prefix);
        let records = create_test_records();
        db.insert_records(&records).await?;
        refresh_table(&db, &ctx).await?;
        sleep(Duration::from_millis(100)).await;

        // Test: count query.
        let count_df = ctx.sql("SELECT COUNT(*) as count FROM otel_logs_and_spans").await?;
        let result = count_df.collect().await?;
        assert_batches_eq!(&["+-------+", "| count |", "+-------+", "| 2     |", "+-------+",], &result);

        // Test: field selection.
        let df = ctx.sql("SELECT name, status_code, level FROM otel_logs_and_spans ORDER BY name").await?;
        let result = df.collect().await?;
        assert_batches_eq!(
            &[
                "+-------------+-------------+-------+",
                "| name        | status_code | level |",
                "+-------------+-------------+-------+",
                "| test_span_1 | OK          | INFO  |",
                "| test_span_2 | ERROR       | ERROR |",
                "+-------------+-------------+-------+",
            ],
            &result
        );

        // Test: filtering by project_id and level.
        let df = ctx
            .sql("SELECT name, level, status_code, status_message FROM otel_logs_and_spans WHERE project_id = 'test_project' AND level = 'ERROR'")
            .await?;
        let result = df.collect().await?;
        assert_batches_eq!(
            &[
                "+-------------+-------+-------------+----------------+",
                "| name        | level | status_code | status_message |",
                "+-------------+-------+-------------+----------------+",
                "| test_span_2 | ERROR | ERROR       | Error occurred |",
                "+-------------+-------+-------------+----------------+",
            ],
            &result
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sql_insert() -> Result<()> {
        let (db, ctx, test_prefix) = setup_test_database().await?;
        log::info!("Using test table prefix for SQL INSERT: {}", test_prefix);

        // Direct API insertion.
        let datetime = chrono::DateTime::parse_from_rfc3339("2023-02-01T15:30:00.000000Z").unwrap().with_timezone(&Utc);
        let record = OtelLogsAndSpans {
            project_id: "test_project".to_string(),
            timestamp: datetime,
            observed_timestamp: datetime,
            id: "sql_span1".to_string(),
            name: "sql_test_span".to_string(),
            duration: 150000000,
            start_time: datetime,
            context___trace_id: "sql_trace1".to_string(),
            context___span_id: "sql_span1".to_string(),
            status_code: Some("OK".to_string()),
            status_message: Some("SQL inserted successfully".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![record]).await?;

        // SQL INSERT via the routing table.
        let insert_sql = "INSERT INTO otel_logs_and_spans (
            project_id, timestamp, observed_timestamp, id, name, duration, start_time,
            context___trace_id, context___span_id, status_code, status_message, level
        ) VALUES (
            'test_project', TIMESTAMP '2023-01-01T10:00:00Z', TIMESTAMP '2023-01-01T10:00:00Z', 'sql_span2', 'sql_test_span_2',
            200000000, TIMESTAMP '2023-01-01T10:00:00Z', 'sql_trace2', 'sql_span2', 'ERROR', 'SQL error inserted', 'ERROR'
        )";
        let insert_df = ctx.sql(insert_sql).await?;
        let insert_result = insert_df.collect().await?;
        assert_batches_eq!(&["+-------+", "| count |", "+-------+", "| 1     |", "+-------+"], &insert_result);
        refresh_table(&db, &ctx).await?;
        sleep(Duration::from_millis(100)).await;
        let verify_df = ctx.sql("SELECT id, name, status_message FROM otel_logs_and_spans WHERE id = 'sql_span2'").await?;
        let verify_result = verify_df.collect().await?;
        assert_batches_eq!(
            &[
                "+----------+--------------+----------------------+",
                "| id       | name         | status_message       |",
                "+----------+--------------+----------------------+",
                "| sql_span2| sql_test_span_2 | SQL error inserted |",
                "+----------+--------------+----------------------+",
            ],
            &verify_result
        );
        let count_df = ctx.sql("SELECT COUNT(*) as count FROM otel_logs_and_spans WHERE id = 'sql_span2'").await?;
        let count_result = count_df.collect().await?;
        assert_batches_eq!(&["+-------+", "| count |", "+-------+", "| 1     |", "+-------+"], &count_result);
        Ok(())
    }

    #[tokio::test]
    async fn test_nanosecond_precision() -> Result<()> {
        let (db, ctx, test_prefix) = setup_test_database().await?;
        log::info!("Using test table prefix for nanosecond precision: {}", test_prefix);
        let base = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let ts = base.with_nanosecond(123456789).unwrap();
        let record = OtelLogsAndSpans {
            project_id: "ns_project".to_string(),
            timestamp: ts,
            observed_timestamp: ts,
            id: "span_ns".to_string(),
            name: "nanosecond_test".to_string(),
            context___trace_id: "trace_ns".to_string(),
            context___span_id: "span_ns".to_string(),
            start_time: ts,
            duration: 1000,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![record]).await?;
        let df = ctx.sql("SELECT timestamp FROM otel_logs_and_spans WHERE id = 'span_ns'").await?;
        let batches = df.collect().await?;
        let column = batches[0].column(0);
        let retrieved_ts_nanos = if let Some(ts_array) = column.as_any().downcast_ref::<TimestampNanosecondArray>() {
            ts_array.value(0)
        } else if let Some(int64_array) = column.as_any().downcast_ref::<Int64Array>() {
            int64_array.value(0)
        } else {
            panic!("Column 'timestamp' is not of an expected type. Schema: {:?}", batches[0].schema());
        };
        let expected = ts.timestamp_nanos_opt().unwrap();
        assert_eq!(retrieved_ts_nanos, expected, "Nanosecond precision mismatch");
        Ok(())
    }

    #[tokio::test]
    async fn test_grouping_by_span() -> Result<()> {
        let (db, ctx, _) = setup_test_database().await?;
        let timestamp = Utc.with_ymd_and_hms(2023, 1, 1, 12, 0, 0).unwrap();
        let record1 = OtelLogsAndSpans {
            project_id: "group_project".to_string(),
            timestamp,
            observed_timestamp: timestamp,
            id: "group_span".to_string(),
            name: "group_test_1".to_string(),
            context___trace_id: "trace_group".to_string(),
            context___span_id: "group_span".to_string(),
            start_time: timestamp,
            duration: 500,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        let record2 = OtelLogsAndSpans {
            project_id: "group_project".to_string(),
            timestamp,
            observed_timestamp: timestamp,
            id: "group_span".to_string(),
            name: "group_test_2".to_string(),
            context___trace_id: "trace_group".to_string(),
            context___span_id: "group_span".to_string(),
            start_time: timestamp,
            duration: 600,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![record1, record2]).await?;
        let df = ctx.sql("SELECT name FROM otel_logs_and_spans WHERE id = 'group_span'").await?;
        let batches = df.collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "Expected 2 rows for grouping by span");
        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_queries() -> Result<()> {
        use std::env;

        use datafusion::prelude::SessionContext;
        let bucket1 = env::var("AWS_S3_BUCKET1").unwrap_or_else(|_| "bucket1".to_string());
        let bucket2 = env::var("AWS_S3_BUCKET2").unwrap_or_else(|_| "bucket2".to_string());
        let endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());
        let storage_uri1 = format!("s3://{}?endpoint={}", bucket1, endpoint);
        let storage_uri2 = format!("s3://{}?endpoint={}", bucket2, endpoint);
        let db = Database::new().await?;
        db.register_project("project1", &storage_uri1, None, None, None).await?;
        db.register_project("project2", &storage_uri2, None, None, None).await?;
        let now = Utc::now();
        let record1 = crate::persistent_queue::OtelLogsAndSpans {
            project_id: "project1".to_string(),
            timestamp: now,
            observed_timestamp: now,
            id: "span1".to_string(),
            name: "record1_project1".to_string(),
            context___trace_id: "trace1".to_string(),
            context___span_id: "span1".to_string(),
            start_time: now,
            duration: 100,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        let record2 = crate::persistent_queue::OtelLogsAndSpans {
            project_id: "project2".to_string(),
            timestamp: now,
            observed_timestamp: now,
            id: "span1".to_string(),
            name: "record1_project2".to_string(),
            context___trace_id: "trace2".to_string(),
            context___span_id: "span1".to_string(),
            start_time: now,
            duration: 200,
            status_code: Some("OK".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![record1]).await?;
        db.insert_records(&vec![record2]).await?;
        async fn refresh_ctx(db: &Database) -> Result<SessionContext> {
            let new_ctx = SessionContext::new();
            let configs = db.project_configs.read().await;
            for (project_id, (_, _, table_lock)) in configs.iter() {
                let table_name = format!("otel_logs_and_spans_{}", project_id);
                new_ctx.register_table(&table_name, Arc::new(table_lock.read().await.clone()))?;
            }
            Ok(new_ctx)
        }
        let new_ctx = refresh_ctx(&db).await?;
        let query1 = {
            let ctx = new_ctx.clone();
            async move {
                let df = ctx.sql("SELECT name FROM otel_logs_and_spans_project1").await?;
                df.collect().await
            }
        };
        let query2 = {
            let ctx = new_ctx.clone();
            async move {
                let df = ctx.sql("SELECT name FROM otel_logs_and_spans_project2").await?;
                df.collect().await
            }
        };
        let (res1, res2) = tokio::join!(query1, query2);
        let batches1 = res1?;
        let batches2 = res2?;
        let count1: usize = batches1.iter().map(|b| b.num_rows()).sum();
        let count2: usize = batches2.iter().map(|b| b.num_rows()).sum();
        assert!(count1 > 0, "project1 query returned no rows");
        assert!(count2 > 0, "project2 query returned no rows");
        Ok(())
    }
}
