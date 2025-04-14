use crate::persistent_queue::OtelLogsAndSpans;
use anyhow::Result;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::common::not_impl_err;
use datafusion::common::SchemaExt;
use datafusion::execution::context::SessionContext;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::intervals::utils::check_support;
use datafusion::physical_plan::insert::{DataSink, DataSinkExec};
use datafusion::physical_plan::DisplayAs;
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DFResult},
    logical_expr::{dml::InsertOp, BinaryExpr},
    physical_plan::{DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
};
use delta_kernel::arrow::record_batch::RecordBatch;
use deltalake::checkpoints;
use deltalake::{storage::StorageOptions, DeltaOps, DeltaTable, DeltaTableBuilder};
use futures::StreamExt;
use std::fmt;
use std::{any::Any, collections::HashMap, env, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use url::Url;

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

    /// Create and configure a SessionContext with DataFusion settings
    pub fn create_session_context(&self) -> SessionContext {
        use datafusion::config::ConfigOptions;
        use datafusion::execution::context::SessionContext;

        let mut options = ConfigOptions::new();
        let _ = options.set("datafusion.sql_parser.enable_information_schema", "true");
        SessionContext::new_with_config(options.into())
    }

    /// Setup the session context with tables and register DataFusion tables
    pub fn setup_session_context(&self, ctx: &SessionContext) -> DFResult<()> {
        use crate::persistent_queue::OtelLogsAndSpans;

        // Create tables and register them with session context
        let schema = OtelLogsAndSpans::schema_ref();
        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::new(self.clone()), schema);
        ctx.register_table(OtelLogsAndSpans::table_name(), Arc::new(routing_table))?;
        info!("Registered ProjectRoutingTable with SessionContext");

        self.register_pg_settings_table(ctx)?;
        self.register_set_config_udf(ctx);

        Ok(())
    }

    /// Register PostgreSQL settings table for compatibility
    pub fn register_pg_settings_table(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::StringArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("setting", DataType::Utf8, false),
        ]));

        let names = vec![
            "TimeZone".to_string(),
            "client_encoding".to_string(),
            "datestyle".to_string(),
            "client_min_messages".to_string(),
            // Add more PostgreSQL settings that clients might try to set
            "lc_monetary".to_string(),
            "lc_numeric".to_string(),
            "lc_time".to_string(),
            "standard_conforming_strings".to_string(),
            "application_name".to_string(),
            "search_path".to_string(),
        ];

        let settings = vec![
            "UTC".to_string(),
            "UTF8".to_string(),
            "ISO, MDY".to_string(),
            "notice".to_string(),
            // Default values for the additional settings
            "C".to_string(),
            "C".to_string(),
            "C".to_string(),
            "on".to_string(),
            "TimeFusion".to_string(),
            "public".to_string(),
        ];

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(names)), Arc::new(StringArray::from(settings))])?;

        ctx.register_batch("pg_settings", batch)?;
        Ok(())
    }

    /// Register set_config UDF for PostgreSQL compatibility
    pub fn register_set_config_udf(&self, ctx: &SessionContext) {
        use datafusion::arrow::array::{StringArray, StringBuilder};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarFunctionImplementation, Volatility};

        let set_config_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            let param_value_array = match &args[1] {
                ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringArray>().expect("set_config second arg must be a StringArray"),
                _ => panic!("set_config second arg must be an array"),
            };

            let mut builder = StringBuilder::new();
            for i in 0..param_value_array.len() {
                if param_value_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(param_value_array.value(i));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        });

        let set_config_udf = create_udf(
            "set_config",
            vec![DataType::Utf8, DataType::Utf8, DataType::Boolean],
            DataType::Utf8,
            Volatility::Volatile,
            set_config_fn,
        );

        ctx.register_udf(set_config_udf);
    }

    /// Start a PGWire server with the given session context
    pub async fn start_pgwire_server(
        &self, session_context: SessionContext, port: u16, shutdown_token: CancellationToken,
    ) -> Result<tokio::task::JoinHandle<()>> {
        use datafusion_postgres::{DfSessionService, HandlerFactory};
        use tokio::net::TcpListener;

        let pg_service = Arc::new(DfSessionService::new(session_context));
        let handler_factory = Arc::new(HandlerFactory(pg_service.clone()));

        info!("Attempting to bind PGWire server to 0.0.0.0:{}", port);
        let bind_addr = format!("0.0.0.0:{}", port);
        let pg_listener = match TcpListener::bind(&bind_addr).await {
            Ok(listener) => {
                info!("PGWire server successfully bound to {}", bind_addr);
                listener
            }
            Err(e) => {
                error!("Failed to bind PGWire server to {}: {:?}", bind_addr, e);
                return Err(anyhow::anyhow!("Failed to bind PGWire server: {:?}", e));
            }
        };

        // Log all local addresses this process is listening on
        info!("PGWire server running on 0.0.0.0:{}", port);
        info!("PGWire server local address: {:?}", pg_listener.local_addr());

        let pgwire_shutdown = shutdown_token.clone();

        let pg_server = tokio::spawn({
            let handler_factory = handler_factory.clone();
            async move {
                loop {
                    tokio::select! {
                        _ = pgwire_shutdown.cancelled() => {
                            info!("PGWire server shutting down.");
                            break;
                        }
                        result = pg_listener.accept() => {
                            match result {
                                Ok((socket, addr)) => {
                                    info!("PGWire: Received connection from {}, preparing to process", addr);
                                    info!("PGWire: Socket details - local addr: {:?}, peer addr: {:?}",
                                        socket.local_addr().map_err(|e| debug!("Failed to get local addr: {:?}", e)),
                                        socket.peer_addr().map_err(|e| debug!("Failed to get peer addr: {:?}", e))
                                    );

                                    let handler_factory = handler_factory.clone();
                                    tokio::spawn(async move {
                                        info!("PGWire: Started processing connection from {}", addr);
                                        match pgwire::tokio::process_socket(socket, None, handler_factory).await {
                                            Ok(()) => {
                                                info!("PGWire: Connection from {} processed successfully", addr);
                                            }
                                            Err(e) => {
                                                error!("PGWire: Error processing connection from {}: {:?}", addr, e);
                                                error!("PGWire: Error details - {}", e);
                                            }
                                        }
                                    });
                                }
                                Err(e) => {
                                    error!("PGWire: Error accepting connection: {:?}", e);
                                    error!("PGWire: Connection accept error details - {}", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(pg_server)
    }

    pub async fn resolve_table(&self, project_id: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        let project_configs = self.project_configs.read().await;

        // Try to get the requested project table first
        if let Some((_, _, table)) = project_configs.get(project_id) {
            // Update the table before returning to ensure we have the latest version
            {
                let mut table_write = table.write().await;
                // Run update to load any new transactions
                match table_write.update().await {
                    Ok(_) => debug!("Updated table for project '{}' to latest version", project_id),
                    Err(e) => error!("Failed to update table for project '{}': {}", project_id, e),
                }
            }

            return Ok(table.clone());
        }

        // If not found and project_id is not "default", try the default table
        if project_id != "default" {
            if let Some((_, _, table)) = project_configs.get("default") {
                log::warn!("Project '{}' not found, falling back to default project", project_id);

                // Update the default table before returning
                {
                    let mut table_write = table.write().await;
                    // Run update to load any new transactions
                    match table_write.update().await {
                        Ok(_) => debug!("Updated default table to latest version"),
                        Err(e) => error!("Failed to update default table: {}", e),
                    }
                }

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

        let write_op = ops.write(batch).with_partition_columns(OtelLogsAndSpans::partitions());
        *table = write_op.await?;

        // Checkpoint the table every 10 versions
        let version = table.version();
        if version > 0 && version % 10 == 0 {
            info!("Checkpointing Delta table at version {}", version);
            checkpoints::create_checkpoint(&table, None).await?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub async fn insert_records(&self, records: &Vec<crate::persistent_queue::OtelLogsAndSpans>) -> Result<()> {
        // TODO: insert records doesn't need to accept a project_id as they can be read from the
        // record.
        // Records should be grouped by span, and separated into groups then inserted into the
        // correct table.

        use serde_arrow::schema::SchemaLike;

        // Convert OtelLogsAndSpans records to Arrow RecordBatch format
        let fields = Vec::<arrow_schema::FieldRef>::from_type::<OtelLogsAndSpans>(serde_arrow::schema::TracingOptions::default())?;
        let batch = serde_arrow::to_record_batch(&fields, &records)?;

        // Call insert_records_batch with the converted batch to reuse common insertion logic
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

        let mut table = match DeltaTableBuilder::from_uri(conn_str).with_storage_options(storage_options.0.clone()).with_allow_http(true).load().await {
            Ok(table) => {
                // Check if table needs checkpointing - use same threshold as in insert_records_batch
                let version = table.version();
                // Only checkpoint if it's a multiple of 20 to be consistent with our write policy
                if version > 0 && version % 20 == 0 {
                    info!("Checkpointing table for project '{}' at initial load, version {}", project_id, version);
                    checkpoints::create_checkpoint(&table, None).await?;
                }
                table
            }
            Err(err) => {
                log::warn!("table doesn't exist. creating new table. err: {:?}", err);

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

        let mut configs = self.project_configs.write().await;
        configs.insert(project_id.to_string(), (conn_str.to_string(), storage_options, Arc::new(RwLock::new(table))));
        Ok(())
    }
}

#[derive(Debug, Clone)]
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

    fn extract_project_id_from_filters(&self, filters: &[Expr]) -> Option<String> {
        // Look for expressions like "project_id = 'some_value'"
        for filter in filters {
            if let Some(project_id) = self.extract_project_id(filter) {
                return Some(project_id);
            }
        }
        None
    }

    fn schema(&self) -> SchemaRef {
        OtelLogsAndSpans::schema_ref()
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
                            if let Expr::Literal(ScalarValue::Utf8(Some(value))) = right.as_ref() {
                                return Some(value.clone());
                            }
                        }
                    }

                    // Also check if right side is the column (order might be flipped)
                    if let Expr::Column(col) = right.as_ref() {
                        if col.name == "project_id" {
                            // Check if left side is a literal string
                            if let Expr::Literal(ScalarValue::Utf8(Some(value))) = left.as_ref() {
                                return Some(value.clone());
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

// Needed by DataSink
impl DisplayAs for ProjectRoutingTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProjectRoutingTable ")
            } // DisplayFormatType::TreeRender => {
              //     // TODO: collect info
              //     write!(f, "")
              // }
        }
    }
}

#[async_trait]
impl DataSink for ProjectRoutingTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let mut new_batches = vec![];
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            new_batches.push(batch);
        }
        self.database
            .insert_records_batch("", new_batches)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to insert records: {}", e)))?;
        Ok(row_count as u64)
    }

    fn as_any(&self) -> &dyn Any {
        self
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
        self.schema()
    }

    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        match self.schema().logically_equivalent_names_and_types(&input.schema()) {
            Ok(_) => info!("Schema validation passed"),
            Err(e) => {
                error!("Schema validation failed: {}", e);
                return Err(e);
            }
        }

        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }

        // Create sink executor but with additional logging
        let sink = DataSinkExec::new(input, Arc::new(self.clone()), None);

        Ok(Arc::new(sink))
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filter.iter().map(|_| TableProviderFilterPushDown::Inexact).collect())
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Get project_id from filters if possible, otherwise use default
        let project_id = self.extract_project_id_from_filters(filters).unwrap_or_else(|| self.default_project.clone());

        let delta_table = self.database.resolve_table(&project_id).await?;
        let table = delta_table.read().await;
        table.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use datafusion::assert_batches_eq;
    use datafusion::prelude::SessionContext;
    use dotenv::dotenv;
    use serial_test::serial;
    use uuid::Uuid;

    use super::*;

    // Helper function to create a test database with a unique table prefix
    async fn setup_test_database(prefix: String) -> Result<(Database, SessionContext, String)> {
        let _ = env_logger::builder().is_test(true).try_init();
        dotenv().ok();

        // Set a unique test-specific prefix for a clean Delta table
        let test_prefix = format!("test-data-{}", prefix);
        unsafe {
            env::set_var("TIMEFUSION_TABLE_PREFIX", &test_prefix);
        }

        let db = Database::new().await?;
        let mut session_context = SessionContext::new();
        datafusion_functions_json::register_all(&mut session_context)?;
        let schema = OtelLogsAndSpans::schema_ref();

        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::new(db.clone()), schema);
        session_context.register_table(OtelLogsAndSpans::table_name(), Arc::new(routing_table))?;

        Ok((db, session_context, test_prefix))
    }

    // Helper function to create sample test records
    fn create_test_records() -> Vec<OtelLogsAndSpans> {
        let timestamp1 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 0, 0).unwrap();
        let timestamp2 = Utc.with_ymd_and_hms(2023, 1, 1, 10, 10, 0).unwrap();

        vec![
            OtelLogsAndSpans {
                project_id: "test_project".to_string(),
                timestamp: timestamp1,
                observed_timestamp: Some(timestamp1),
                id: "span1".to_string(),
                name: Some("test_span_1".to_string()),
                context___trace_id: Some("trace1".to_string()),
                context___span_id: Some("span1".to_string()),
                start_time: Some(timestamp1),
                duration: Some(100_000_000),
                status_code: Some("OK".to_string()),
                level: Some("INFO".to_string()),
                ..Default::default()
            },
            OtelLogsAndSpans {
                project_id: "test_project".to_string(),
                timestamp: timestamp2,
                observed_timestamp: Some(timestamp2),
                id: "span2".to_string(),
                name: Some("test_span_2".to_string()),
                context___trace_id: Some("trace2".to_string()),
                context___span_id: Some("span2".to_string()),
                start_time: Some(timestamp2),
                duration: Some(200_000_000),
                status_code: Some("ERROR".to_string()),
                level: Some("ERROR".to_string()),
                status_message: Some("Error occurred".to_string()),
                ..Default::default()
            },
        ]
    }

    #[serial]
    #[tokio::test]
    async fn test_database_query() -> Result<()> {
        let (db, ctx, test_prefix) = setup_test_database(Uuid::new_v4().to_string() + "query").await?;
        log::info!("Using test-specific table prefix: {}", test_prefix);

        let records = create_test_records();
        db.insert_records(&records).await?;

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

    #[serial]
    #[tokio::test]
    async fn test_sql_insert() -> Result<()> {
        let (db, ctx, test_prefix) = setup_test_database(Uuid::new_v4().to_string() + "insert").await?;
        log::info!("Using test-specific table prefix for SQL INSERT test: {}", test_prefix);

        let datetime = chrono::DateTime::parse_from_rfc3339("2023-02-01T15:30:00.000000Z").unwrap().with_timezone(&chrono::Utc);
        let record = OtelLogsAndSpans {
            project_id: "default".to_string(),
            timestamp: datetime,
            observed_timestamp: Some(datetime),
            id: "sql_span1a".to_string(),
            name: Some("sql_test_span".to_string()),
            duration: Some(150000000),
            start_time: Some(datetime),
            context___trace_id: Some("sql_trace1".to_string()),
            context___span_id: Some("sql_span1".to_string()),
            status_code: Some("OK".to_string()),
            status_message: Some("SQL inserted successfully".to_string()),
            level: Some("INFO".to_string()),
            ..Default::default()
        };
        db.insert_records(&vec![record]).await?;

        let verify_df = ctx.sql("SELECT id, name, timestamp from otel_logs_and_spans").await?.collect().await?;
        #[rustfmt::skip]
        assert_batches_eq!(
            [
                "+------------+---------------+---------------------+",
                "| id         | name          | timestamp           |",
                "+------------+---------------+---------------------+",
                "| sql_span1a | sql_test_span | 2023-02-01T15:30:00 |",
                "+------------+---------------+---------------------+",
        ], &verify_df);
        //
        let insert_sql = "INSERT INTO otel_logs_and_spans (
                project_id, timestamp, id,
                parent_id, name, kind,
                status_code, status_message, level, severity___severity_text, severity___severity_number,
                body, duration, start_time, end_time
            ) VALUES (
                'test_project', TIMESTAMP '2023-01-01T10:00:00Z', 'sql_span1',
                NULL, 'sql_test_span', NULL,
                'OK', 'span inserted successfully', 'INFO', 'INFORMATION', NULL,
                NULL, 150000000, TIMESTAMP '2023-01-01T10:00:00Z', NULL
            )";

        let insert_result = ctx.sql(insert_sql).await?.collect().await?;
        #[rustfmt::skip]
        assert_batches_eq!(
            ["+-------+",
            "| count |",
            "+-------+",
            "| 1     |",
            "+-------+",
        ], &insert_result);

        let verify_df = ctx
            .sql("SELECT project_id, id, name, timestamp, kind, status_code, severity___severity_text, duration, start_time from otel_logs_and_spans order by timestamp desc")
            .await?
            .collect()
            .await?;
        #[rustfmt::skip]
        assert_batches_eq!(
        [
            "+--------------+------------+---------------+---------------------+------+-------------+--------------------------+-----------+---------------------+",
            "| project_id   | id         | name          | timestamp           | kind | status_code | severity___severity_text | duration  | start_time          |",
            "+--------------+------------+---------------+---------------------+------+-------------+--------------------------+-----------+---------------------+",
            "| default      | sql_span1a | sql_test_span | 2023-02-01T15:30:00 |      | OK          |                          | 150000000 | 2023-02-01T15:30:00 |",
            "| test_project | sql_span1  | sql_test_span | 2023-01-01T10:00:00 |      | OK          | INFORMATION              | 150000000 | 2023-01-01T10:00:00 |",
            "+--------------+------------+---------------+---------------------+------+-------------+--------------------------+-----------+---------------------+",
        ]
        , &verify_df);

        log::info!("Inserting record directly via insert statement");
        let insert_sql = "INSERT INTO otel_logs_and_spans (
                project_id, timestamp, observed_timestamp, id,
                parent_id, name, kind,
                status_code, status_message, level, severity___severity_text, severity___severity_number,
                body, duration, start_time, end_time,
                context___trace_id, context___span_id, context___trace_state, context___trace_flags,
                context___is_remote, events, links,
                attributes___client___address, attributes___client___port,

                attributes___server___address, attributes___server___port, attributes___network___local__address, attributes___network___local__port,
                attributes___network___peer___address, attributes___network___peer__port, attributes___network___protocol___name, attributes___network___protocol___version,
                attributes___network___transport, attributes___network___type,attributes___code___number, attributes___code___file___path,
                attributes___code___function___name, attributes___code___line___number, attributes___code___stacktrace, attributes___log__record___original,

                attributes___log__record___uid, attributes___error___type, attributes___exception___type, attributes___exception___message,
                attributes___exception___stacktrace, attributes___url___fragment, attributes___url___full, attributes___url___path,
                attributes___url___query, attributes___url___scheme, attributes___user_agent___original, attributes___http___request___method,
                attributes___http___request___method_original,attributes___http___response___status_code, attributes___http___request___resend_count, attributes___http___request___body___size,

                attributes___session___id, attributes___session___previous___id, attributes___db___system___name, attributes___db___collection___name,
                attributes___db___namespace, attributes___db___operation___name, attributes___db___response___status_code, attributes___db___operation___batch___size,
                attributes___db___query___summary, attributes___db___query___text, attributes___user___id, attributes___user___email,
                attributes___user___full_name, attributes___user___name, attributes___user___hash, resource___service___name,

                resource___service___version, resource___service___instance___id, resource___service___namespace, resource___telemetry___sdk___language,
                resource___telemetry___sdk___name, resource___telemetry___sdk___version, resource___user_agent___original
            ) VALUES (
                'test_project', TIMESTAMP '2023-01-02T10:00:00Z', NULL, 'sql_span2',
                NULL, 'sql_test_span', NULL,
                'OK', 'span inserted successfully', 'INFO', NULL, NULL,
                NULL, 150000000, TIMESTAMP '2023-01-01T10:00:00Z', NULL,
                'sql_trace1', 'sql_span1', NULL, NULL,
                NULL, NULL, NULL,
                NULL, NULL,

                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,

                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,

                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,

                NULL, NULL, NULL, NULL,
                NULL, NULL, NULL
            )";

        let insert_result = ctx.sql(insert_sql).await?.collect().await?;
        #[rustfmt::skip]
        assert_batches_eq!(
            ["+-------+",
            "| count |",
            "+-------+",
            "| 1     |",
            "+-------+",
        ], &insert_result);

        // Verify that the SQL-inserted record exists
        let verify_df = ctx.sql("SELECT id, name, status_message FROM otel_logs_and_spans WHERE id = 'sql_span1'").await?;
        let verify_result = verify_df.collect().await?;

        // Check that we can retrieve the inserted record
        assert_batches_eq!(
            [
                "+-----------+---------------+----------------------------+",
                "| id        | name          | status_message             |",
                "+-----------+---------------+----------------------------+",
                "| sql_span1 | sql_test_span | span inserted successfully |",
                "+-----------+---------------+----------------------------+",
            ],
            &verify_result
        );

        // TODO: verify the correct copy to syntax
        // let copy_sql = "COPY (VALUES (
        //         NULL, 'sql_span2copy',
        //         NULL, 'sql_test_span_copy', NULL,
        //         'OK', 'span copied into successfully', 'INFO', NULL, NULL,
        //         NULL, 150000000, TIMESTAMP '2023-01-01T10:00:00Z', NULL,
        //         'sql_trace1copy', 'sql_span1copy', NULL, NULL,
        //         NULL, NULL, NULL,
        //         NULL, NULL,
        //
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL, NULL,
        //
        //         NULL, NULL, NULL, NULL,
        //         NULL, NULL, NULL,
        //
        //         'test_project', TIMESTAMP '2023-01-02T10:00:00Z'
        //     )) TO otel_logs_and_spans ";
        //
        // let insert_result = ctx.sql(copy_sql).await?.collect().await?;
        // #[rustfmt::skip]
        // assert_batches_eq!(
        //     ["+-------+",
        //     "| count |",
        //     "+-------+",
        //     "| 1     |",
        //     "+-------+",
        // ], &insert_result);

        let verify_df = ctx
            .sql("SELECT project_id, id, name, timestamp, kind, status_code, severity___severity_text, duration, start_time from otel_logs_and_spans order by timestamp desc")
            .await?
            .collect()
            .await?;
        #[rustfmt::skip]
        assert_batches_eq!(
            [
                "+--------------+------------+---------------+---------------------+------+-------------+--------------------------+-----------+---------------------+",
                "| project_id   | id         | name          | timestamp           | kind | status_code | severity___severity_text | duration  | start_time          |",
                "+--------------+------------+---------------+---------------------+------+-------------+--------------------------+-----------+---------------------+",
                "| default      | sql_span1a | sql_test_span | 2023-02-01T15:30:00 |      | OK          |                          | 150000000 | 2023-02-01T15:30:00 |",
                "| test_project | sql_span2  | sql_test_span | 2023-01-02T10:00:00 |      | OK          |                          | 150000000 | 2023-01-01T10:00:00 |",
                "| test_project | sql_span1  | sql_test_span | 2023-01-01T10:00:00 |      | OK          | INFORMATION              | 150000000 | 2023-01-01T10:00:00 |",
                "+--------------+------------+---------------+---------------------+------+-------------+--------------------------+-----------+---------------------+",
            ],
            &verify_df
        );

        Ok(())
    }
}
