use crate::persistent_queue::OtelLogsAndSpans;
use anyhow::Result;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::array::Array;
use datafusion::common::SchemaExt;
use datafusion::common::not_impl_err;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::insert::{DataSink, DataSinkExec};
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DFResult},
    logical_expr::{BinaryExpr, dml::InsertOp},
    physical_plan::{DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_postgres::{DfSessionService, HandlerFactory};
use delta_kernel::arrow::record_batch::RecordBatch;
use deltalake::checkpoints;
use deltalake::datafusion::parquet::basic::{Compression, ZstdLevel};
use deltalake::datafusion::parquet::file::properties::WriterProperties;
use deltalake::operations::transaction::CommitProperties;
use deltalake::{DeltaOps, DeltaTable, DeltaTableBuilder, storage::StorageOptions};
use futures::StreamExt;
use std::fmt;
use std::{any::Any, collections::HashMap, env, sync::Arc};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::RwLock;
use tokio::{net::TcpListener, time::timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use url::Url;

type ProjectConfig = (String, StorageOptions, Arc<RwLock<DeltaTable>>);

pub type ProjectConfigs = Arc<RwLock<HashMap<String, ProjectConfig>>>;

#[derive(Debug)]
pub struct Database {
    project_configs: ProjectConfigs,
    batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    maintenance_shutdown: Arc<CancellationToken>,
}

impl Clone for Database {
    fn clone(&self) -> Self {
        Self {
            project_configs: Arc::clone(&self.project_configs),
            batch_queue: self.batch_queue.clone(),
            maintenance_shutdown: Arc::clone(&self.maintenance_shutdown),
        }
    }
}

impl Database {
    pub async fn new() -> Result<Self> {
        let bucket = env::var("AWS_S3_BUCKET").expect("AWS_S3_BUCKET environment variable not set");
        let aws_endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());

        // Generate a unique prefix for this run's data
        let prefix = env::var("TIMEFUSION_TABLE_PREFIX").unwrap_or_else(|_| "timefusion".to_string());
        let table_name = "otel_logs_and_spans";
        let storage_uri = format!("s3://{}/{}/{}/?endpoint={}", bucket, prefix, table_name, aws_endpoint);
        info!("Storage URI configured: {}", storage_uri);

        let aws_url = Url::parse(&aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

        let project_configs = HashMap::new();

        let db = Self {
            project_configs: Arc::new(RwLock::new(project_configs)),
            batch_queue: None, // Batch queue is set later
            maintenance_shutdown: Arc::new(CancellationToken::new()),
        };

        db.register_project("default", &storage_uri, None, None, None).await?;

        Ok(db)
    }

    /// Set the batch queue to use for insert operations
    pub fn with_batch_queue(mut self, batch_queue: Arc<crate::batch_queue::BatchQueue>) -> Self {
        self.batch_queue = Some(batch_queue);
        self
    }

    /// Start background maintenance schedulers for optimize and vacuum operations
    pub async fn start_maintenance_schedulers(self) -> Result<Self> {
        use tokio_cron_scheduler::{Job, JobScheduler};

        let scheduler = JobScheduler::new().await?;
        let db = Arc::new(self.clone());

        // Optimize job - every 3 hours
        let optimize_job = Job::new_async("0 0 */3 * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Running scheduled optimize on all tables");
                    for (project_id, (_, _, table)) in db.project_configs.read().await.iter() {
                        if let Err(e) = db.optimize_table(table).await {
                            error!("Optimize failed for {}: {}", project_id, e);
                        }
                    }
                })
            }
        })?;

        scheduler.add(optimize_job).await?;

        // Vacuum job - daily at 3AM
        let vacuum_job = Job::new_async("0 0 3 * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Running scheduled vacuum on all tables");
                    let retention_hours = env::var("TIMEFUSION_VACUUM_RETENTION_HOURS").unwrap_or_else(|_| "336".to_string()).parse::<u64>().unwrap_or(336);

                    for (project_id, (_, _, table)) in db.project_configs.read().await.iter() {
                        info!("Vacuuming {} (retention: {}h)", project_id, retention_hours);
                        db.vacuum_table(table, retention_hours).await;
                    }
                })
            }
        })?;

        scheduler.add(vacuum_job).await?;

        // Start the scheduler
        scheduler.start().await?;

        // Handle shutdown
        let shutdown = self.maintenance_shutdown.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            info!("Shutting down maintenance scheduler");
        });

        Ok(self)
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

        // Get batch queue from the app state if available
        let batch_queue = self.batch_queue.as_ref().map(Arc::clone);

        let routing_table = ProjectRoutingTable::new("default".to_string(), Arc::new(self.clone()), schema, batch_queue);

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
        use datafusion::logical_expr::{ColumnarValue, ScalarFunctionImplementation, Volatility, create_udf};

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

    pub async fn start_pgwire_server(
        &self, session_ctx: SessionContext, port: u16, shutdown: CancellationToken,
    ) -> anyhow::Result<tokio::task::JoinHandle<()>> {
        // 1) build listener
        // Simple binding with clear logging
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        info!("Binding PGWire server to {}...", addr);

        // Use standard tokio TcpListener
        let listener = TcpListener::bind(addr).await?;

        // Log successful binding
        if let Ok(local_addr) = listener.local_addr() {
            info!("PGWire server successfully bound to {}", local_addr);
        }

        // 2) pgwire service + handler
        let service = Arc::new(DfSessionService::new(session_ctx));
        let factory = Arc::new(HandlerFactory(service));

        // 3) concurrency + logging
        let max_conn = std::env::var("MAX_PG_CONNECTIONS").ok().and_then(|v| v.parse().ok()).unwrap_or(100) as usize;
        info!("PGWire listening on 0.0.0.0:{} (limit {})", port, max_conn);

        // 4) spawn the accept‐&‐process loop
        let handle = tokio::spawn({
            let shutdown = shutdown.clone();
            let stream = TcpListenerStream::new(listener);
            async move {
                stream
                    .take_until(shutdown.cancelled())
                    .for_each_concurrent(max_conn, |conn| async {
                        match conn {
                            Ok(sock) => {
                                // Set TCP nodelay option for better performance
                                if let Err(e) = sock.set_nodelay(true) {
                                    error!("Failed to set TCP_NODELAY: {}", e);
                                }

                                // Log client connection info
                                if let Ok(peer_addr) = sock.peer_addr() {
                                    info!("Client connected from {}", peer_addr);
                                }

                                // Use a longer timeout to prevent idle disconnections
                                let timeout_duration = Duration::from_secs(3600); // 1 hour
                                info!("Starting PGWire connection processing");
                                let start_time = std::time::Instant::now();

                                match timeout(timeout_duration, pgwire::tokio::process_socket(sock, None, factory.clone())).await {
                                    Ok(Ok(_)) => {
                                        let elapsed = start_time.elapsed();
                                        info!("PGWire connection completed successfully (duration: {:?})", elapsed);
                                    }
                                    Ok(Err(e)) => {
                                        let elapsed = start_time.elapsed();
                                        error!("PGWire connection error after {:?}: {}", elapsed, e);
                                    }
                                    Err(_) => {
                                        error!("PGWire connection timed out after 1 hour");
                                    }
                                }
                            }
                            Err(e) => error!("TCP accept error: {}", e),
                        }
                    })
                    .await;
                info!("PGWire server shut down.");
            }
        });

        Ok(handle)
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

            // Use Arc::clone instead of table.clone() to avoid deep copying
            return Ok(Arc::clone(table));
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

                // Use Arc::clone instead of table.clone() to avoid deep copying
                return Ok(Arc::clone(table));
            }
        }

        // If we get here, neither the requested project nor default exists
        Err(DataFusionError::Execution(format!(
            "Unknown project_id: {} and no default project found",
            project_id
        )))
    }

    pub async fn insert_records_batch(&self, _table: &str, batches: Vec<RecordBatch>, skip_queue: bool) -> Result<()> {
        // Check if we should use the batch queue based on:
        // 1. skip_queue parameter (if true, always skip)
        // 2. ENABLE_BATCH_QUEUE env var (if set to "true", allow queue usage)
        // 3. batch_queue existence
        let enable_queue = env::var("ENABLE_BATCH_QUEUE").unwrap_or_else(|_| "false".to_string()) == "true";

        if !skip_queue && enable_queue && self.batch_queue.is_some() {
            let queue = self.batch_queue.as_ref().unwrap();
            // Add to batch queue
            for batch in batches {
                if let Err(e) = queue.queue(batch) {
                    return Err(anyhow::anyhow!("Queue error: {}", e));
                }
            }
            return Ok(());
        }

        // Direct insert logic if skip_queue=true, queue disabled, no batch queue, or when processing from batch queue
        let (_conn_str, _options, table_ref) = {
            let configs = self.project_configs.read().await;
            configs.get("default").ok_or_else(|| anyhow::anyhow!("Project ID '{}' not found", "default"))?.clone()
        };

        // Create writer properties with ZSTD compression level 6 and bloom filters
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(6).unwrap()))
            .set_bloom_filter_enabled(true)
            .set_sorting_columns(Some(OtelLogsAndSpans::sorting_columns()))
            .build();

        // Scope the write lock to minimize lock time
        {
            let mut table = table_ref.write().await;

            // Create the DeltaOps with a clone of the table
            let write_op = DeltaOps(table.clone())
                .write(batches)
                .with_partition_columns(OtelLogsAndSpans::partitions())
                .with_writer_properties(writer_properties);

            let new_table = write_op.await?;
            *table = new_table;

            // Note: Checkpointing, optimization, and vacuum are now managed by scheduled jobs
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
        let fields = OtelLogsAndSpans::fields()?;
        let batch = serde_arrow::to_record_batch(&fields, &records)?;

        // Call insert_records_batch with the converted batch to reuse common insertion logic
        // In tests we always skip the queue for direct insertion
        self.insert_records_batch("default", vec![batch], true).await
    }

    /// Optimize the Delta table using Z-ordering on timestamp and id columns
    /// This improves query performance for time-based queries
    async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>) -> Result<()> {
        // Log the start of the optimization operation
        info!("Starting Delta table optimization with Z-ordering");

        // Get a clone of the table to avoid holding the lock during the operation
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Run optimize operation with Z-order on the timestamp and id columns
        // and a target size of 256MB for optimal file size
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(6).unwrap()))
            .set_bloom_filter_enabled(true)
            .set_sorting_columns(Some(OtelLogsAndSpans::sorting_columns()))
            .build();

        // Note: Z-order functionality is achieved through sorting_columns in writer_properties
        let optimize_result = DeltaOps(table_clone)
            .optimize()
            .with_type(deltalake::operations::optimize::OptimizeType::ZOrder(OtelLogsAndSpans::z_order_columns()))
            .with_target_size(268435456) // 256MB
            .with_writer_properties(writer_properties)
            .await;

        match optimize_result {
            Ok((new_table, metrics)) => {
                info!(
                    "Optimization with sorted columns completed: {} files removed, {} files added, {} partitions optimized, {} total files considered, {} files skipped",
                    metrics.num_files_removed,
                    metrics.num_files_added,
                    metrics.partitions_optimized,
                    metrics.total_considered_files,
                    metrics.total_files_skipped
                );

                // Update the table reference with the optimized version
                let mut table = table_ref.write().await;
                *table = new_table;

                Ok(())
            }
            Err(e) => {
                error!("Optimization operation failed: {}", e);
                Err(anyhow::anyhow!("Table optimization failed: {}", e))
            }
        }
    }

    /// Vacuum the Delta table to clean up old files that are no longer needed
    /// This reduces storage costs and improves query performance
    async fn vacuum_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) {
        // Log the start of the vacuum operation
        info!("Starting vacuum operation with retention period of {} hours", retention_hours);

        // Get a clone of the table to avoid holding the lock during the operation
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Directly run vacuum without dry run to delete old files
        match DeltaOps(table_clone)
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            .await
        {
            Ok((_, metrics)) => {
                let files_deleted = metrics.files_deleted.len();
                info!("Vacuum completed successfully, deleted {} files", files_deleted);

                // Update the table reference with the vacuumed version
                let mut table = table_ref.write().await;
                if let Ok(()) = table.update().await {
                    info!("Table updated after vacuum");
                } else {
                    error!("Failed to update table after vacuum");
                }
            }
            Err(e) => error!("Vacuum operation failed: {}", e),
        }
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
                let commit_properties = CommitProperties::default().with_create_checkpoint(true).with_cleanup_expired_logs(Some(true));

                // Create table with ZSTD compression and auto-optimization
                // Note: z-ordering will be applied via sorting_columns in the writer properties
                delta_ops
                    .create()
                    .with_columns(OtelLogsAndSpans::columns().unwrap_or_default())
                    .with_partition_columns(OtelLogsAndSpans::partitions())
                    .with_storage_options(storage_options.0.clone())
                    .with_commit_properties(commit_properties)
                    .with_configuration_property(deltalake::TableProperty::AutoOptimizeOptimizeWrite, Some("true"))
                    .with_configuration_property(deltalake::TableProperty::AutoOptimizeAutoCompact, Some("true"))
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
    batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
}

impl ProjectRoutingTable {
    pub fn new(default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>) -> Self {
        Self {
            default_project,
            database,
            schema,
            batch_queue,
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
        let mut row_count = 0;
        let mut batches = Vec::new();

        // Collect all batches from the stream
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            batches.push(batch);
        }

        if batches.is_empty() {
            return Ok(0);
        }

        // Let the database handle the queue decision with skip_queue=false
        // This means it will use the queue if it's available and not disabled via env var
        self.database
            .insert_records_batch("", batches, false)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Insert error: {}", e)))?;

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
            Ok(_) => debug!("insert_into; Schema validation passed"),
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

        let routing_table = ProjectRoutingTable::new(
            "default".to_string(),
            Arc::new(db.clone()),
            schema,
            None, // No batch queue in tests
        );
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
                // date: timestamp1.date_naive(),
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
                // date: timestamp2.date_naive(),
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

        let insert_sql = "INSERT INTO otel_logs_and_spans (
                 project_id, date, timestamp, id, hashes,
                 parent_id, name, kind,
                 status_code, status_message, level, severity___severity_text, severity___severity_number,
                 body, duration, start_time, end_time
             ) VALUES (
                 'test_project', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 'sql_span1', ARRAY[],
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
                project_id, date, timestamp, observed_timestamp, id, hashes,
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
                'test_project','2023-01-02',  TIMESTAMP '2023-01-02T10:00:00Z', NULL, 'sql_span2', ARRAY[],
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
