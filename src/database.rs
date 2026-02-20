use crate::config::{self, AppConfig};
use crate::object_store_cache::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache};
use crate::schema_loader::{create_insert_compatible_schema, get_default_schema, get_schema, is_variant_type};
use crate::statistics::DeltaStatisticsExtractor;
use anyhow::Result;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use chrono::Utc;
use datafusion::arrow::array::Array;
use datafusion::common::Statistics;
use datafusion::common::not_impl_err;
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::expressions::{CastExpr, Column as PhysicalColumn};
use datafusion::physical_plan::DisplayAs;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlanProperties, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion::{
    catalog::Session,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DFResult},
    logical_expr::{BinaryExpr, col, dml::InsertOp, lit},
    physical_plan::{DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, union::UnionExec},
};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_functions_json;
use delta_kernel::arrow::record_batch::RecordBatch;
use deltalake::PartitionFilter;
use deltalake::datafusion::parquet::file::metadata::SortingColumn;
use deltalake::datafusion::parquet::file::properties::WriterProperties;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::operations::create::CreateBuilder;
use deltalake::{DeltaTable, DeltaTableBuilder};
use futures::StreamExt;
use instrumented_object_store::instrument_object_store;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::fmt;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::field::Empty;
use tracing::{Instrument, debug, error, info, instrument, warn};
use url::Url;

// Unified tables: one Delta table per schema (table_name -> DeltaTable)
// All default projects share the same table, with project_id as a partition column
pub type UnifiedTables = Arc<RwLock<HashMap<String, Arc<RwLock<DeltaTable>>>>>;

// Custom project tables: projects with their own S3 bucket get isolated tables
// Key: (project_id, table_name) -> DeltaTable
pub type CustomProjectTables = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;

/// Get a Delta table from custom project tables by project_id and table_name
pub async fn get_custom_delta_table(custom_tables: &CustomProjectTables, project_id: &str, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    custom_tables.read().await.get(&(project_id.to_string(), table_name.to_string())).cloned()
}

/// Get a Delta table from unified tables by table_name
pub async fn get_unified_delta_table(unified_tables: &UnifiedTables, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    unified_tables.read().await.get(table_name).cloned()
}

// Helper function to extract project_id from a batch
pub fn extract_project_id(batch: &RecordBatch) -> Option<String> {
    use datafusion::arrow::array::{StringArray, StringViewArray};

    batch.schema().fields().iter().position(|f| f.name() == "project_id").and_then(|idx| {
        let column = batch.column(idx);
        // Try Utf8View first (our preferred type), then fall back to Utf8
        if let Some(arr) = column.as_any().downcast_ref::<StringViewArray>() {
            (arr.len() > 0 && !arr.is_null(0)).then(|| arr.value(0).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            (arr.len() > 0 && !arr.is_null(0)).then(|| arr.value(0).to_string())
        } else {
            None
        }
    })
}

/// Convert string columns to Variant binary format where the target schema expects Variant type.
/// This enables automatic JSON string → Variant conversion during INSERT.
pub fn convert_variant_columns(batch: RecordBatch, target_schema: &SchemaRef) -> DFResult<RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{DataType, Field};

    let batch_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut new_fields: Vec<Arc<Field>> = batch_schema.fields().iter().cloned().collect();

    for (idx, target_field) in target_schema.fields().iter().enumerate() {
        if !is_variant_type(target_field.data_type()) {
            continue;
        }
        if idx >= columns.len() {
            debug!("Column index {} exceeds batch length {}, skipping", idx, columns.len());
            continue;
        }

        let col = &columns[idx];
        let col_type = col.data_type();

        // Only convert if source is a string type and target is Variant
        let converted: Option<ArrayRef> =
            match col_type {
                DataType::Utf8View => {
                    let arr = col.as_any().downcast_ref::<StringViewArray>().ok_or_else(|| {
                        DataFusionError::Execution(format!("Expected StringViewArray for field '{}' but downcast failed", target_field.name()))
                    })?;
                    Some(Arc::new(json_strings_to_variant(arr.iter())?))
                }
                DataType::Utf8 => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| DataFusionError::Execution(format!("Expected StringArray for field '{}' but downcast failed", target_field.name())))?;
                    Some(Arc::new(json_strings_to_variant(arr.iter())?))
                }
                DataType::LargeUtf8 => {
                    let arr = col.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| {
                        DataFusionError::Execution(format!("Expected LargeStringArray for field '{}' but downcast failed", target_field.name()))
                    })?;
                    Some(Arc::new(json_strings_to_variant(arr.iter())?))
                }
                _ => None, // Already Variant or other type, skip
            };

        if let Some(variant_array) = converted {
            columns[idx] = variant_array;
            new_fields[idx] = target_field.clone();
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, columns).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Convert an iterator of optional JSON strings to a Variant StructArray.
/// Fails fast on invalid JSON to ensure data integrity.
fn json_strings_to_variant<'a>(iter: impl Iterator<Item = Option<&'a str>>) -> DFResult<datafusion::arrow::array::StructArray> {
    use parquet_variant_compute::VariantArrayBuilder;
    use parquet_variant_json::JsonToVariant;

    let items: Vec<_> = iter.collect();
    let mut builder = VariantArrayBuilder::new(items.len());

    for (row_idx, item) in items.into_iter().enumerate() {
        match item {
            Some(json_str) => builder
                .append_json(json_str)
                .map_err(|e| DataFusionError::Execution(format!("Invalid JSON at row {}: {} (value: '{}')", row_idx, e, json_str)))?,
            None => builder.append_null(),
        }
    }

    Ok(builder.build().into())
}

/// Convert Variant columns to JSON strings for SELECT output.
/// This enables pgwire to properly encode Variant data as JSON text.
pub fn variant_columns_to_json(batch: RecordBatch, real_schema: &SchemaRef) -> DFResult<RecordBatch> {
    use datafusion::arrow::array::{ArrayRef, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::arrow::record_batch::RecordBatchOptions;

    let batch_schema = batch.schema();
    let row_count = batch.num_rows();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut new_fields: Vec<Arc<Field>> = batch_schema.fields().iter().cloned().collect();

    // Iterate over batch columns (which may be projected) and look up by name in real schema
    for (idx, batch_field) in batch_schema.fields().iter().enumerate() {
        let is_variant = real_schema.column_with_name(batch_field.name()).is_some_and(|(_, f)| is_variant_type(f.data_type()));
        if !is_variant {
            continue;
        }

        let col = &columns[idx];
        if let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() {
            let json_arr = variant_struct_to_json(struct_arr)?;
            columns[idx] = Arc::new(json_arr);
            new_fields[idx] = Arc::new(Field::new(batch_field.name(), DataType::Utf8, batch_field.is_nullable()));
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    // Use try_new_with_options to preserve row count for empty-column batches (e.g., COUNT(*) queries)
    RecordBatch::try_new_with_options(new_schema, columns, &RecordBatchOptions::new().with_row_count(Some(row_count)))
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// Convert a Variant StructArray to a StringArray of JSON values.
fn variant_struct_to_json(arr: &datafusion::arrow::array::StructArray) -> DFResult<datafusion::arrow::array::StringArray> {
    use datafusion::arrow::array::StringBuilder;
    use parquet_variant_compute::VariantArray;
    use parquet_variant_json::VariantToJson;

    let variant_arr = VariantArray::try_new(arr).map_err(|e| DataFusionError::Execution(format!("Failed to create VariantArray: {}", e)))?;

    let mut builder = StringBuilder::new();
    for i in 0..variant_arr.len() {
        if variant_arr.is_null(i) {
            builder.append_null();
        } else {
            let variant = variant_arr.value(i);
            let json = variant.to_json_string().map_err(|e| DataFusionError::Execution(format!("Failed to convert variant to JSON: {}", e)))?;
            builder.append_value(&json);
        }
    }
    Ok(builder.finish())
}

/// Custom execution plan that converts Variant columns to JSON strings for SELECT.
#[derive(Debug)]
struct VariantToJsonExec {
    input: Arc<dyn ExecutionPlan>,
    real_schema: SchemaRef,
    output_schema: SchemaRef,
    properties: PlanProperties,
}

impl VariantToJsonExec {
    fn new(input: Arc<dyn ExecutionPlan>, real_schema: SchemaRef) -> Self {
        use datafusion::arrow::datatypes::{DataType, Field};
        // Output schema: for each column in input, convert Variant to Utf8
        let input_schema = input.schema();
        let output_fields: Vec<Arc<Field>> = input_schema
            .fields()
            .iter()
            .map(|f| {
                let is_variant = real_schema.column_with_name(f.name()).is_some_and(|(_, rf)| is_variant_type(rf.data_type()));
                if is_variant { Arc::new(Field::new(f.name(), DataType::Utf8, f.is_nullable())) } else { f.clone() }
            })
            .collect();
        let output_schema = Arc::new(Schema::new(output_fields));
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(output_schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            Boundedness::Bounded,
        );
        Self {
            input,
            real_schema,
            output_schema,
            properties,
        }
    }
}

impl DisplayAs for VariantToJsonExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VariantToJsonExec")
    }
}

impl ExecutionPlan for VariantToJsonExec {
    fn name(&self) -> &str {
        "VariantToJsonExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(VariantToJsonExec::new(children[0].clone(), self.real_schema.clone())))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let real_schema = self.real_schema.clone();
        let output_schema = self.output_schema.clone();

        let converted_stream = input_stream.map(move |batch_result| batch_result.and_then(|batch| variant_columns_to_json(batch, &real_schema)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(output_schema, converted_stream)))
    }
}

/// Check if input schema is compatible with target schema for INSERT operations.
/// This allows string types (Utf8, Utf8View, LargeUtf8) to be inserted into Variant columns,
/// since convert_variant_columns() will handle the conversion in write_all().
fn is_schema_compatible_for_insert(input_schema: &SchemaRef, target_schema: &SchemaRef) -> DFResult<()> {
    use datafusion::arrow::datatypes::DataType;

    if input_schema.fields().len() != target_schema.fields().len() {
        return Err(DataFusionError::Plan(format!(
            "Schema field count mismatch: input has {} fields, target has {} fields",
            input_schema.fields().len(),
            target_schema.fields().len()
        )));
    }

    fn is_string_type(dt: &DataType) -> bool {
        matches!(dt, DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8)
    }

    fn types_compatible(input: &DataType, target: &DataType) -> bool {
        if input == target {
            return true;
        }
        if is_string_type(input) && is_string_type(target) {
            return true;
        }
        // String -> Variant (string will be converted to variant)
        if is_string_type(input) && is_variant_type(target) {
            return true;
        }
        // Variant -> Utf8View (INSERT-compatible schema uses Utf8View for Variant cols)
        if is_variant_type(input) && is_string_type(target) {
            return true;
        }
        // List types with compatible element types
        if let (DataType::List(in_f), DataType::List(tgt_f)) = (input, target) {
            return types_compatible(in_f.data_type(), tgt_f.data_type());
        }
        if let (DataType::LargeList(in_f), DataType::LargeList(tgt_f)) = (input, target) {
            return types_compatible(in_f.data_type(), tgt_f.data_type());
        }
        input.equals_datatype(target)
    }

    for (input_field, target_field) in input_schema.fields().iter().zip(target_schema.fields()) {
        if !types_compatible(input_field.data_type(), target_field.data_type()) {
            return Err(DataFusionError::Plan(format!(
                "Schema mismatch for field '{}': input type {:?} is not compatible with target type {:?}",
                input_field.name(),
                input_field.data_type(),
                target_field.data_type()
            )));
        }
    }

    Ok(())
}

/// Custom execution plan that converts string columns to Variant type.
/// This wraps an input plan and transforms string columns to Variant in the output.
#[derive(Debug)]
struct VariantConversionExec {
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    properties: PlanProperties,
}

impl VariantConversionExec {
    fn new(input: Arc<dyn ExecutionPlan>, target_schema: SchemaRef) -> Self {
        let properties = PlanProperties::new(
            datafusion::physical_expr::EquivalenceProperties::new(target_schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            Boundedness::Bounded,
        );
        Self {
            input,
            target_schema,
            properties,
        }
    }
}

impl DisplayAs for VariantConversionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "VariantConversionExec")
    }
}

impl ExecutionPlan for VariantConversionExec {
    fn name(&self) -> &str {
        "VariantConversionExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(VariantConversionExec::new(children[0].clone(), self.target_schema.clone())))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let target_schema = self.target_schema.clone();

        let converted_stream = input_stream.map(move |batch_result| batch_result.and_then(|batch| convert_variant_columns(batch, &target_schema)));

        Ok(Box::pin(RecordBatchStreamAdapter::new(self.target_schema.clone(), converted_stream)))
    }
}

// Compression level for parquet files - kept for WriterProperties fallback
const ZSTD_COMPRESSION_LEVEL: i32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct StorageConfig {
    project_id: String,
    table_name: String,
    s3_bucket: String,
    s3_prefix: String,
    s3_region: String,
    s3_access_key_id: String,
    s3_secret_access_key: String,
    s3_endpoint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Database {
    config: Arc<AppConfig>,
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables: UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables: CustomProjectTables,
    batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    maintenance_shutdown: Arc<CancellationToken>,
    config_pool: Option<PgPool>,
    storage_configs: Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    default_s3_bucket: Option<String>,
    default_s3_prefix: Option<String>,
    default_s3_endpoint: Option<String>,
    object_store_cache: Option<Arc<SharedFoyerCache>>,
    statistics_extractor: Arc<DeltaStatisticsExtractor>,
    last_written_versions: Arc<RwLock<HashMap<(String, String), i64>>>,
    buffered_layer: Option<Arc<crate::buffered_write_layer::BufferedWriteLayer>>,
}

impl Database {
    /// Get the config for this database instance
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Get the unified tables cache for direct access
    pub fn unified_tables(&self) -> &UnifiedTables {
        &self.unified_tables
    }

    /// Get the custom project tables cache for direct access
    pub fn custom_project_tables(&self) -> &CustomProjectTables {
        &self.custom_project_tables
    }

    /// Perform a Delta table UPDATE operation
    pub async fn perform_delta_update(
        &self, table_name: &str, project_id: &str, predicate: Option<datafusion::logical_expr::Expr>,
        assignments: Vec<(String, datafusion::logical_expr::Expr)>, session: Arc<dyn datafusion::catalog::Session>,
    ) -> Result<u64, DataFusionError> {
        crate::dml::perform_delta_update(self, table_name, project_id, predicate, assignments, session).await
    }

    /// Perform a Delta table DELETE operation
    pub async fn perform_delta_delete(
        &self, table_name: &str, project_id: &str, predicate: Option<datafusion::logical_expr::Expr>, session: Arc<dyn datafusion::catalog::Session>,
    ) -> Result<u64, DataFusionError> {
        crate::dml::perform_delta_delete(self, table_name, project_id, predicate, session).await
    }

    /// Build storage options with consistent configuration including DynamoDB locking if enabled
    fn build_storage_options(&self) -> HashMap<String, String> {
        let storage_options = self.config.aws.build_storage_options(self.default_s3_endpoint.as_deref());

        let safe_options: HashMap<_, _> = storage_options.iter().filter(|(k, _)| !k.contains("secret") && !k.contains("password")).collect();
        info!("Storage options configured: {:?}", safe_options);
        storage_options
    }

    /// Creates standard writer properties used across different operations
    fn create_writer_properties(&self, sorting_columns: Vec<SortingColumn>, fields: &[crate::schema_loader::FieldDef]) -> WriterProperties {
        use deltalake::datafusion::parquet::basic::{Compression, Encoding, ZstdLevel};
        use deltalake::datafusion::parquet::file::properties::EnabledStatistics;
        use deltalake::datafusion::parquet::schema::types::ColumnPath;

        let page_row_count_limit = self.config.parquet.timefusion_page_row_count_limit;
        let compression_level = self.config.parquet.timefusion_zstd_compression_level;
        let max_row_group_size = self.config.parquet.timefusion_max_row_group_size;

        let mut builder = WriterProperties::builder()
            .set_compression(Compression::ZSTD(
                ZstdLevel::try_new(compression_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap()),
            ))
            .set_max_row_group_size(max_row_group_size)
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(8388608)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_bloom_filter_enabled(!self.config.parquet.timefusion_bloom_filter_disabled)
            .set_bloom_filter_fpp(0.01)
            .set_bloom_filter_ndv(100_000)
            .set_data_page_row_count_limit(page_row_count_limit)
            .set_sorting_columns(if sorting_columns.is_empty() { None } else { Some(sorting_columns) });

        for field in fields {
            let dt = field.data_type.as_str();
            let col = ColumnPath::from(field.name.as_str());
            if dt.starts_with("Timestamp") || dt == "Date32" {
                builder = builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED).set_column_dictionary_enabled(col, false);
            } else if matches!(dt, "Int32" | "Int64" | "UInt32" | "UInt64") {
                builder = builder.set_column_encoding(col, Encoding::DELTA_BINARY_PACKED);
            }
        }

        builder.build()
    }

    /// Updates a DeltaTable and handles errors consistently
    async fn update_table(&self, table: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) -> Result<()> {
        // Try to update with retries for eventual consistency
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5;

        loop {
            let mut table_write = table.write().await;
            match table_write.update_state().await {
                Ok(()) => {
                    if let Some(version) = table_write.version() {
                        debug!("Updated table for {}/{} to version {}", project_id, table_name, version);
                        // Update our version tracking to reflect what we just loaded
                        let mut versions = self.last_written_versions.write().await;
                        versions.insert((project_id.to_string(), table_name.to_string()), version);
                    }
                    return Ok(());
                }
                Err(e) => {
                    // Release the lock before retrying
                    drop(table_write);

                    retries += 1;
                    if retries >= MAX_RETRIES {
                        error!("Failed to update table for {}/{} after {} retries: {}", project_id, table_name, MAX_RETRIES, e);
                        return Err(anyhow::anyhow!("Failed to update table: {}", e));
                    }

                    debug!(
                        "Failed to update table for {}/{} (attempt {}/{}): {}, retrying...",
                        project_id, table_name, retries, MAX_RETRIES, e
                    );
                    // Exponential backoff with jitter
                    let delay = 100 * retries as u64 + (retries as u64 * 50);
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                }
            }
        }
    }

    /// Load storage configurations from PostgreSQL
    async fn load_storage_configs(pool: &PgPool) -> Result<HashMap<(String, String), StorageConfig>> {
        // Ensure table exists
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS timefusion_projects (
                project_id VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                s3_bucket VARCHAR(255) NOT NULL,
                s3_prefix VARCHAR(500) NOT NULL,
                s3_region VARCHAR(100) NOT NULL,
                s3_access_key_id VARCHAR(500) NOT NULL,
                s3_secret_access_key VARCHAR(500) NOT NULL,
                s3_endpoint VARCHAR(500),
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (project_id, table_name)
            )
            "#,
        )
        .execute(pool)
        .await?;

        let configs: Vec<StorageConfig> = sqlx::query_as(
            "SELECT project_id, table_name, s3_bucket, s3_prefix, s3_region, 
             s3_access_key_id, s3_secret_access_key, s3_endpoint 
             FROM timefusion_projects WHERE is_active = true",
        )
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for config in configs {
            info!("Loaded config: {}/{}", config.project_id, config.table_name);
            map.insert((config.project_id.clone(), config.table_name.clone()), config);
        }
        Ok(map)
    }

    async fn initialize_cache_with_retry(cfg: &AppConfig) -> Option<Arc<SharedFoyerCache>> {
        // Check if cache is disabled
        if cfg.cache.is_disabled() {
            info!("Foyer cache is disabled via TIMEFUSION_FOYER_DISABLED");
            return None;
        }

        let foyer_config = FoyerCacheConfig::from_app_config(&cfg);
        info!(
            "Initializing shared Foyer hybrid cache (memory: {}MB, disk: {}GB, TTL: {}s)",
            foyer_config.memory_size_bytes / 1024 / 1024,
            foyer_config.disk_size_bytes / 1024 / 1024 / 1024,
            foyer_config.ttl.as_secs()
        );

        for attempt in 1..=3 {
            match SharedFoyerCache::new(foyer_config.clone()).await {
                Ok(cache) => {
                    info!("Shared Foyer cache initialized successfully for all tables");
                    return Some(Arc::new(cache));
                }
                Err(e) if attempt < 3 => {
                    warn!("Failed to initialize shared Foyer cache (attempt {}/3): {}. Retrying...", attempt, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("Failed to initialize shared Foyer cache after 3 retries: {}. Continuing without cache.", e);
                    return None;
                }
            }
        }
        None
    }

    /// Create a new Database with explicit config.
    /// Prefer this over `new()` for better testability.
    pub async fn with_config(cfg: Arc<AppConfig>) -> Result<Self> {
        let aws_endpoint = &cfg.aws.aws_s3_endpoint;
        let aws_url = Url::parse(aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

        // Check for DynamoDB locking configuration
        if cfg.aws.is_dynamodb_locking_enabled() {
            if let Some(ref table) = cfg.aws.dynamodb.delta_dynamo_table_name {
                info!("DynamoDB locking enabled with table: {}", table);

                if let Some(ref endpoint) = cfg.aws.dynamodb.aws_endpoint_url_dynamodb {
                    info!("DynamoDB endpoint: {}", endpoint);
                }
                if let Some(ref region) = cfg.aws.dynamodb.aws_region_dynamodb {
                    info!("DynamoDB region: {}", region);
                }
                info!(
                    "DynamoDB credentials configured: access_key={}, secret_key={}",
                    cfg.aws.dynamodb.aws_access_key_id_dynamodb.is_some(),
                    cfg.aws.dynamodb.aws_secret_access_key_dynamodb.is_some()
                );
            }
        } else {
            info!(
                "DynamoDB locking not configured. AWS_S3_LOCKING_PROVIDER={:?}, DELTA_DYNAMO_TABLE_NAME={:?}",
                cfg.aws.dynamodb.aws_s3_locking_provider, cfg.aws.dynamodb.delta_dynamo_table_name
            );
        }

        // Store default S3 settings for unconfigured mode
        let default_s3_bucket = cfg.aws.aws_s3_bucket.clone();
        let default_s3_prefix = cfg.core.timefusion_table_prefix.clone();
        let default_s3_endpoint = Some(aws_endpoint.clone());

        // Try to connect to config database if URL is provided
        let (config_pool, storage_configs) = match &cfg.core.timefusion_config_database_url {
            Some(db_url) => match PgPoolOptions::new().max_connections(2).connect(db_url).await {
                Ok(pool) => {
                    let configs = Self::load_storage_configs(&pool).await.unwrap_or_default();
                    (Some(pool), configs)
                }
                Err(_) => {
                    info!("Could not connect to config database, using default mode");
                    (None, HashMap::new())
                }
            },
            None => (None, HashMap::new()),
        };

        // Initialize object store cache BEFORE creating any tables
        // This ensures all tables benefit from caching
        let object_store_cache = Self::initialize_cache_with_retry(&cfg).await;

        // Initialize statistics extractor with configurable cache size
        let stats_cache_size = cfg.parquet.timefusion_stats_cache_size;
        let page_row_limit = cfg.parquet.timefusion_page_row_count_limit;
        let statistics_extractor = Arc::new(DeltaStatisticsExtractor::new(stats_cache_size, 300, page_row_limit));

        let db = Self {
            config: cfg,
            unified_tables: Arc::new(RwLock::new(HashMap::new())),
            custom_project_tables: Arc::new(RwLock::new(HashMap::new())),
            batch_queue: None,
            maintenance_shutdown: Arc::new(CancellationToken::new()),
            config_pool,
            storage_configs: Arc::new(RwLock::new(storage_configs)),
            default_s3_bucket: default_s3_bucket.clone(),
            default_s3_prefix: Some(default_s3_prefix.clone()),
            default_s3_endpoint,
            object_store_cache,
            statistics_extractor,
            last_written_versions: Arc::new(RwLock::new(HashMap::new())),
            buffered_layer: None,
        };

        Ok(db)
    }

    /// Create a new Database using global config (for production).
    /// For tests, prefer `with_config()` to pass config explicitly.
    pub async fn new() -> Result<Self> {
        let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;
        // Convert &'static to Arc - it's fine since static lives forever
        // We clone the config to create an owned Arc
        let cfg_arc = Arc::new(cfg.clone());
        Self::with_config(cfg_arc).await
    }

    /// Set the batch queue to use for insert operations
    pub fn with_batch_queue(mut self, batch_queue: Arc<crate::batch_queue::BatchQueue>) -> Self {
        self.batch_queue = Some(batch_queue);
        self
    }

    /// Set the buffered write layer for WAL + in-memory buffer
    pub fn with_buffered_layer(mut self, layer: Arc<crate::buffered_write_layer::BufferedWriteLayer>) -> Self {
        self.buffered_layer = Some(layer);
        self
    }

    /// Get the buffered write layer if configured
    pub fn buffered_layer(&self) -> Option<&Arc<crate::buffered_write_layer::BufferedWriteLayer>> {
        self.buffered_layer.as_ref()
    }

    /// Query Delta tables directly, bypassing the in-memory buffer (for testing).
    pub async fn query_delta_only(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut db_clone = self.clone();
        db_clone.buffered_layer = None;
        let db_arc = Arc::new(db_clone);
        let mut ctx = Arc::clone(&db_arc).create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db_arc.setup_session_context(&mut ctx)?;
        Ok(ctx.sql(sql).await?.collect().await?)
    }

    /// Enable object store cache with foyer (deprecated - cache is now initialized in new())
    /// This method is kept for backward compatibility but is now a no-op
    pub async fn with_object_store_cache(self) -> Result<Self> {
        // Cache is now initialized in new(), so this is a no-op
        Ok(self)
    }

    /// Start background maintenance schedulers for optimize and vacuum operations
    pub async fn start_maintenance_schedulers(self) -> Result<Self> {
        use tokio_cron_scheduler::{Job, JobScheduler};

        let scheduler = JobScheduler::new().await?;
        let db = Arc::new(self.clone());

        // Light optimize job - every 5 minutes for small recent files
        let light_optimize_schedule = &self.config.maintenance.timefusion_light_optimize_schedule;

        if !light_optimize_schedule.is_empty() {
            info!("Light optimize job scheduled with cron expression: {}", light_optimize_schedule);

            let light_optimize_job = Job::new_async(light_optimize_schedule, {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled light optimize on recent small files");
                        // Optimize unified tables
                        for (table_name, table) in db.unified_tables.read().await.iter() {
                            match db.optimize_table_light(table, table_name).await {
                                Ok(_) => info!("Light optimize completed for unified table '{}'", table_name),
                                Err(e) => error!("Light optimize failed for unified table '{}': {}", table_name, e),
                            }
                        }
                        // Optimize custom project tables
                        for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                            match db.optimize_table_light(table, table_name).await {
                                Ok(_) => info!("Light optimize completed for custom project '{}' table '{}'", project_id, table_name),
                                Err(e) => error!("Light optimize failed for custom project '{}' table '{}': {}", project_id, table_name, e),
                            }
                        }
                    })
                }
            })?;

            scheduler.add(light_optimize_job).await?;
        } else {
            info!("Light optimize job scheduling skipped - empty schedule");
        }

        // Optimize job - configurable schedule (default: every 30mins)
        let optimize_schedule = &self.config.maintenance.timefusion_optimize_schedule;

        if !optimize_schedule.is_empty() {
            info!(
                "Optimize job scheduled with cron expression: {} (processes last 28 hours only)",
                optimize_schedule
            );

            let optimize_job = Job::new_async(optimize_schedule, {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled optimize on all tables");
                        // Optimize unified tables
                        for (table_name, table) in db.unified_tables.read().await.iter() {
                            if let Err(e) = db.optimize_table(table, table_name, None).await {
                                error!("Optimize failed for unified table '{}': {}", table_name, e);
                            }
                        }
                        // Optimize custom project tables
                        for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                            if let Err(e) = db.optimize_table(table, table_name, None).await {
                                error!("Optimize failed for custom project '{}' table '{}': {}", project_id, table_name, e);
                            }
                        }
                    })
                }
            })?;

            scheduler.add(optimize_job).await?;
        } else {
            info!("Optimize job scheduling skipped - empty schedule");
        }

        // Vacuum job - configurable schedule (default: daily at 2AM)
        let vacuum_schedule = &self.config.maintenance.timefusion_vacuum_schedule;
        let vacuum_retention = self.config.maintenance.timefusion_vacuum_retention_hours;

        if !vacuum_schedule.is_empty() {
            info!("Vacuum job scheduled with cron expression: {}", vacuum_schedule);

            let vacuum_job = Job::new_async(vacuum_schedule.as_str(), {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled vacuum on all tables");
                        let retention_hours = vacuum_retention;

                        // Vacuum unified tables
                        for (table_name, table) in db.unified_tables.read().await.iter() {
                            info!("Vacuuming unified table '{}' (retention: {}h)", table_name, retention_hours);
                            db.vacuum_table(table, retention_hours).await;
                        }
                        // Vacuum custom project tables
                        for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                            info!(
                                "Vacuuming custom project '{}' table '{}' (retention: {}h)",
                                project_id, table_name, retention_hours
                            );
                            db.vacuum_table(table, retention_hours).await;
                        }
                    })
                }
            })?;

            scheduler.add(vacuum_job).await?;
        } else {
            info!("Vacuum job scheduling skipped - empty schedule");
        }

        // Cache stats job - every 5 minutes
        let cache_stats_job = Job::new_async("0 */5 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    // Log Foyer cache stats if available
                    if let Some(ref cache) = db.object_store_cache {
                        cache.log_stats().await;
                    }

                    // Log statistics cache stats
                    let (used, capacity) = db.statistics_extractor.get_cache_stats().await;
                    info!("Statistics cache: {}/{} entries used", used, capacity);
                })
            }
        })?;

        scheduler.add(cache_stats_job).await?;

        // Statistics refresh job - every 15 minutes
        let stats_refresh_job = Job::new_async("0 */15 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Refreshing Delta Lake statistics cache");
                    db.statistics_extractor.clear_cache().await;

                    // Pre-warm cache for unified tables
                    for (table_name, table) in db.unified_tables.read().await.iter() {
                        let table = table.read().await;
                        let current_version = table.version().unwrap_or(0);
                        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
                        let schema = schema_def.schema_ref();
                        // Use empty string for project_id since unified tables are shared
                        if let Err(e) = db.statistics_extractor.extract_statistics(&table, "", table_name, &schema).await {
                            error!("Failed to refresh statistics for unified table '{}': {}", table_name, e);
                        } else {
                            debug!("Refreshed statistics for unified table '{}' (version {})", table_name, current_version);
                        }
                    }
                    // Pre-warm cache for custom project tables
                    for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                        let table = table.read().await;
                        let current_version = table.version().unwrap_or(0);
                        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
                        let schema = schema_def.schema_ref();
                        if let Err(e) = db.statistics_extractor.extract_statistics(&table, project_id, table_name, &schema).await {
                            error!("Failed to refresh statistics for {}:{}: {}", project_id, table_name, e);
                        } else {
                            debug!("Refreshed statistics for {}:{} (version {})", project_id, table_name, current_version);
                        }
                    }
                })
            }
        })?;

        scheduler.add(stats_refresh_job).await?;

        // Start the scheduler
        scheduler.start().await?;

        // Handle shutdown
        let shutdown = self.maintenance_shutdown.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            info!("Shutting down maintenance scheduler");
            // Note: scheduler will be dropped when this task ends
        });

        Ok(self)
    }

    /// Create and configure a SessionContext with DataFusion settings
    pub fn create_session_context(self: Arc<Self>) -> SessionContext {
        use crate::dml::DmlQueryPlanner;
        use datafusion::config::ConfigOptions;
        use datafusion::execution::SessionStateBuilder;
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::runtime_env::RuntimeEnvBuilder;
        use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};
        use std::sync::Arc;

        let mut options = ConfigOptions::new();
        let _ = options.set("datafusion.catalog.information_schema", "true");

        // Ensure Utf8View handling for consistent string types across DataFusion and Delta
        let _ = options.set("datafusion.execution.parquet.schema_force_view_types", "true");
        let _ = options.set("datafusion.sql_parser.map_string_types_to_utf8view", "true");

        // Enable Parquet statistics for better query optimization with Delta Lake
        // These settings ensure DataFusion uses file and column statistics for pruning
        let _ = options.set("datafusion.execution.parquet.statistics_enabled", "page");
        let _ = options.set("datafusion.execution.parquet.pushdown_filters", "true");
        let _ = options.set("datafusion.execution.parquet.reorder_filters", "true");
        let _ = options.set("datafusion.execution.parquet.enable_page_index", "true");
        let _ = options.set("datafusion.execution.parquet.pruning", "true");
        let _ = options.set("datafusion.execution.parquet.skip_metadata", "false");
        let _ = options.set("datafusion.explain.show_schema", "true");
        let _ = options.set("datafusion.runtime.metadata_cache_limit", "500M");

        // Enable general statistics collection for query optimization
        // TOOD: Delete, since its true by default
        let _ = options.set("datafusion.execution.collect_statistics", "true");

        // Enable bloom filter pruning if available in Parquet files
        let _ = options.set("datafusion.execution.parquet.bloom_filter_on_read", "true");

        // Time-series optimized settings
        // Larger batch size for better throughput with time-series data
        let _ = options.set("datafusion.execution.batch_size", "65536");

        // Optimize for sorted data (timestamps are typically sorted)
        let _ = options.set("datafusion.optimizer.prefer_existing_sort", "true");

        // Enable repartition for better parallel aggregations
        let _ = options.set("datafusion.optimizer.repartition_aggregations", "true");

        // Disable round-robin repartitioning to maintain sort order
        let _ = options.set("datafusion.optimizer.enable_round_robin_repartition", "false");

        // Enable filter and limit pushdown optimizations
        let _ = options.set("datafusion.optimizer.filter_null_join_keys", "true");
        let _ = options.set("datafusion.optimizer.skip_failed_rules", "false");

        // Enable proper limit handling across partitions
        let _ = options.set("datafusion.optimizer.enable_distinct_aggregation_soft_limit", "true");
        let _ = options.set("datafusion.optimizer.enable_topk_aggregation", "true");

        // Memory management for large time-series queries
        let _ = options.set("datafusion.execution.coalesce_batches", "true");
        let _ = options.set("datafusion.execution.coalesce_target_batch_size", "65536");

        // Enable all optimizer rules for maximum optimization
        let _ = options.set("datafusion.optimizer.max_passes", "5");

        // Configure memory limit for DataFusion operations
        let memory_limit_bytes = self.config.memory.memory_limit_bytes();
        let memory_fraction = self.config.memory.timefusion_memory_fraction;
        let sort_spill_reservation_bytes = self.config.memory.timefusion_sort_spill_reservation_bytes.unwrap_or(67_108_864);

        // Set memory-related configuration options
        let _ = options.set("datafusion.execution.memory_fraction", &memory_fraction.to_string());
        let _ = options.set("datafusion.execution.sort_spill_reservation_bytes", &sort_spill_reservation_bytes.to_string());

        // Create runtime environment with FairSpillPool for per-query memory fairness
        let pool_size = (memory_limit_bytes as f64 * memory_fraction) as usize;
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(pool_size)))
            .build()
            .expect("Failed to create runtime environment");

        let runtime_env = Arc::new(runtime_env);

        // Set up tracing options with configurable sampling
        let record_metrics = self.config.memory.timefusion_tracing_record_metrics;

        let tracing_options = InstrumentationOptions::builder().record_metrics(record_metrics).preview_limit(5).build();

        // Create instrumentation rule
        let instrument_rule = instrument_with_info_spans!(
            options: tracing_options,
        );

        // Create session state with tracing rule and DML support
        // Rule ordering: VariantInsertRewriter runs BEFORE TypeCoercion (rewrites string->json_to_variant)
        //                VariantSelectRewriter runs AFTER TypeCoercion (wraps Variant cols with variant_to_json)
        let analyzer_rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> = vec![
            Arc::new(datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction::new()),
            Arc::new(crate::optimizers::VariantInsertRewriter),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
            Arc::new(crate::optimizers::VariantSelectRewriter),
        ];

        let session_state = SessionStateBuilder::new()
            .with_config(options.into())
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            .with_physical_optimizer_rule(instrument_rule)
            .with_query_planner(Arc::new({
                let planner = DmlQueryPlanner::new(self.clone());
                if let Some(layer) = self.buffered_layer.as_ref() {
                    planner.with_buffered_layer(Arc::clone(layer))
                } else {
                    planner
                }
            }))
            .build();

        SessionContext::new_with_state(session_state)
    }

    /// Setup the session context with tables and register DataFusion tables
    pub fn setup_session_context(&self, ctx: &mut SessionContext) -> DFResult<()> {
        use crate::schema_loader::registry;

        // Get batch queue from the app state if available
        let batch_queue = self.batch_queue.as_ref().map(Arc::clone);

        // Register a routing table for each schema in the registry
        let registry = registry();
        for table_name in registry.list_tables() {
            if let Some(schema) = registry.get(&table_name) {
                let routing_table = ProjectRoutingTable::new(
                    "default".to_string(),
                    Arc::new(self.clone()),
                    schema.schema_ref(),
                    batch_queue.clone(),
                    table_name.clone(),
                );

                ctx.register_table(&table_name, Arc::new(routing_table))?;
                info!("Registered ProjectRoutingTable for table '{}' with SessionContext", table_name);
            }
        }

        self.register_pg_settings_table(ctx)?;
        self.register_set_config_udf(ctx);

        // CRITICAL: Register custom functions BEFORE JSON functions to ensure VariantAwareExprPlanner
        // intercepts -> and ->> operators on Variant columns before JsonExprPlanner handles them as strings
        crate::functions::register_custom_functions(ctx).map_err(|e| DataFusionError::Execution(format!("Failed to register custom functions: {}", e)))?;

        // JSON functions (JsonExprPlanner for -> and ->> on string columns - must come after Variant handlers)
        self.register_json_functions(ctx);

        Ok(())
    }

    /// Register PostgreSQL settings table for compatibility
    pub fn register_pg_settings_table(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        use datafusion::arrow::array::StringViewArray;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, false),
            Field::new("setting", DataType::Utf8View, false),
        ]));

        let names: Vec<&str> = vec![
            "TimeZone",
            "client_encoding",
            "datestyle",
            "client_min_messages",
            "lc_monetary",
            "lc_numeric",
            "lc_time",
            "standard_conforming_strings",
            "application_name",
            "search_path",
        ];

        let settings: Vec<&str> = vec!["UTC", "UTF8", "ISO, MDY", "notice", "C", "C", "C", "on", "TimeFusion", "public"];

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringViewArray::from(names)), Arc::new(StringViewArray::from(settings))],
        )?;

        ctx.register_batch("pg_settings", batch)?;
        Ok(())
    }

    /// Register set_config UDF for PostgreSQL compatibility
    pub fn register_set_config_udf(&self, ctx: &SessionContext) {
        use datafusion::arrow::array::{StringViewArray, StringViewBuilder};
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{ColumnarValue, ScalarFunctionImplementation, Volatility, create_udf};

        let set_config_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            let param_value_array = match &args[1] {
                ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringViewArray>().expect("set_config second arg must be a StringViewArray"),
                _ => panic!("set_config second arg must be an array"),
            };

            let mut builder = StringViewBuilder::new();
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
            vec![DataType::Utf8View, DataType::Utf8View, DataType::Boolean],
            DataType::Utf8View,
            Volatility::Volatile,
            set_config_fn,
        );

        ctx.register_udf(set_config_udf);
    }

    /// Register JSON functions from datafusion-functions-json
    pub fn register_json_functions(&self, ctx: &mut SessionContext) {
        datafusion_functions_json::register_all(ctx).expect("Failed to register JSON functions");
        info!("Registered JSON functions with SessionContext");
    }

    /// Check if a project has custom storage configuration (their own S3 bucket)
    async fn has_custom_storage(&self, project_id: &str, table_name: &str) -> bool {
        self.storage_configs.read().await.contains_key(&(project_id.to_string(), table_name.to_string()))
    }

    #[instrument(
        name = "database.resolve_table",
        skip(self),
        fields(
            project_id = %project_id,
            table.name = %table_name,
            cache_hit = Empty,
            is_custom = Empty,
        )
    )]
    pub async fn resolve_table(&self, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        let span = tracing::Span::current();

        // Try to reload custom configs from database if we have a pool (lazy loading)
        if let Some(ref pool) = self.config_pool
            && let Ok(new_configs) = Self::load_storage_configs(pool).await
        {
            let mut configs = self.storage_configs.write().await;
            *configs = new_configs;
        }

        // Check if project has custom storage config → use isolated table
        if self.has_custom_storage(project_id, table_name).await {
            span.record("is_custom", true);
            return self.resolve_custom_table(project_id, table_name).await;
        }

        span.record("is_custom", false);
        // Default: use unified table (all projects share the same table, partitioned by project_id)
        self.resolve_unified_table(table_name).await
    }

    /// Resolve a unified table (shared by all default projects, partitioned by project_id)
    async fn resolve_unified_table(&self, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // Check unified_tables cache first
        {
            let tables = self.unified_tables.read().await;
            if let Some(table) = tables.get(table_name) {
                debug!("Found unified table '{}' in cache", table_name);
                // For unified tables, we use table_name as the key for version tracking
                let last_written_version = {
                    let versions = self.last_written_versions.read().await;
                    // Use empty string for project_id since unified tables aren't project-specific
                    versions.get(&("".to_string(), table_name.to_string())).cloned()
                };

                let current_version = table.read().await.version();
                let should_update = match (current_version, last_written_version) {
                    (Some(current), Some(last)) => current < last,
                    (Some(_), None) => true,
                    _ => false,
                };

                if should_update {
                    self.update_table(table, "", table_name)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("Failed to update table: {}", e)))?;
                }

                return Ok(Arc::clone(table));
            }
        }

        // Not in cache, create/load it
        self.get_or_create_unified_table(table_name)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get or create unified table: {}", e)))
    }

    /// Resolve a custom project table (isolated table for projects with their own S3 bucket)
    async fn resolve_custom_table(&self, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // Check custom_project_tables cache first
        {
            let tables = self.custom_project_tables.read().await;
            if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
                debug!("Found custom table for project '{}' table '{}' in cache", project_id, table_name);
                let last_written_version = {
                    let versions = self.last_written_versions.read().await;
                    versions.get(&(project_id.to_string(), table_name.to_string())).cloned()
                };

                let current_version = table.read().await.version();
                let should_update = match (current_version, last_written_version) {
                    (Some(current), Some(last)) => current < last,
                    (Some(_), None) => true,
                    _ => false,
                };

                if should_update {
                    self.update_table(table, project_id, table_name)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("Failed to update table: {}", e)))?;
                }

                return Ok(Arc::clone(table));
            }
        }

        // Not in cache, create/load it
        self.get_or_create_custom_table(project_id, table_name)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get or create custom table: {}", e)))
    }

    #[instrument(
        name = "database.get_or_create_unified_table",
        skip(self),
        fields(table.name = %table_name)
    )]
    pub async fn get_or_create_unified_table(&self, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Check cache first
        {
            let tables = self.unified_tables.read().await;
            if let Some(table) = tables.get(table_name) {
                return Ok(Arc::clone(table));
            }
        }

        let Some(ref bucket) = self.default_s3_bucket else {
            return Err(anyhow::anyhow!("No default S3 bucket configured for unified table '{}'", table_name));
        };

        let prefix = self.default_s3_prefix.as_ref().unwrap();
        let endpoint = self.default_s3_endpoint.as_ref().unwrap();
        // Unified table path: s3://{bucket}/{prefix}/{table_name}/ (NO project_id subdirectory)
        let storage_uri = format!("s3://{}/{}/{}/?endpoint={}", bucket, prefix, table_name, endpoint);
        let storage_options = self.build_storage_options();

        info!("Creating or loading unified table '{}' at: {}", table_name, storage_uri);

        // Hold write lock during table creation
        let mut tables = self.unified_tables.write().await;

        // Double-check after acquiring write lock
        if let Some(table) = tables.get(table_name) {
            return Ok(Arc::clone(table));
        }

        let table = self.create_delta_table_internal(&storage_uri, &storage_options, table_name).await?;
        let table_arc = Arc::new(RwLock::new(table));
        tables.insert(table_name.to_string(), Arc::clone(&table_arc));
        info!("Cached unified table '{}', cache now contains {} entries", table_name, tables.len());

        Ok(table_arc)
    }

    #[instrument(
        name = "database.get_or_create_custom_table",
        skip(self),
        fields(project_id = %project_id, table.name = %table_name)
    )]
    pub async fn get_or_create_custom_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Check cache first
        {
            let tables = self.custom_project_tables.read().await;
            if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
                return Ok(Arc::clone(table));
            }
        }

        // Get custom storage config for this project
        let configs = self.storage_configs.read().await;
        let config = configs
            .get(&(project_id.to_string(), table_name.to_string()))
            .ok_or_else(|| anyhow::anyhow!("No storage config found for project '{}' table '{}'", project_id, table_name))?
            .clone();
        drop(configs);

        let storage_uri = format!(
            "s3://{}/{}/?endpoint={}",
            config.s3_bucket,
            config.s3_prefix,
            config
                .s3_endpoint
                .as_ref()
                .unwrap_or(&self.default_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string()))
        );

        let mut storage_options = HashMap::new();
        storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), config.s3_access_key_id.clone());
        storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), config.s3_secret_access_key.clone());
        storage_options.insert("AWS_REGION".to_string(), config.s3_region.clone());
        if let Some(ref endpoint) = config.s3_endpoint {
            storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone());
        }

        // Add DynamoDB locking configuration if enabled
        if self.config.aws.is_dynamodb_locking_enabled() {
            storage_options.insert("AWS_S3_LOCKING_PROVIDER".to_string(), "dynamodb".to_string());
            if let Some(ref table) = self.config.aws.dynamodb.delta_dynamo_table_name {
                storage_options.insert("DELTA_DYNAMO_TABLE_NAME".to_string(), table.clone());
            }
            if let Some(ref key) = self.config.aws.dynamodb.aws_access_key_id_dynamodb {
                storage_options.insert("AWS_ACCESS_KEY_ID_DYNAMODB".to_string(), key.clone());
            }
            if let Some(ref secret) = self.config.aws.dynamodb.aws_secret_access_key_dynamodb {
                storage_options.insert("AWS_SECRET_ACCESS_KEY_DYNAMODB".to_string(), secret.clone());
            }
            if let Some(ref region) = self.config.aws.dynamodb.aws_region_dynamodb {
                storage_options.insert("AWS_REGION_DYNAMODB".to_string(), region.clone());
            }
            if let Some(ref endpoint) = self.config.aws.dynamodb.aws_endpoint_url_dynamodb {
                storage_options.insert("AWS_ENDPOINT_URL_DYNAMODB".to_string(), endpoint.clone());
            }
        }

        info!(
            "Creating or loading custom table for project '{}' table '{}' at: {}",
            project_id, table_name, storage_uri
        );

        // Hold write lock during table creation
        let mut tables = self.custom_project_tables.write().await;

        // Double-check after acquiring write lock
        if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
            return Ok(Arc::clone(table));
        }

        let table = self.create_delta_table_internal(&storage_uri, &storage_options, table_name).await?;
        let table_arc = Arc::new(RwLock::new(table));
        tables.insert((project_id.to_string(), table_name.to_string()), Arc::clone(&table_arc));
        info!(
            "Cached custom table for project '{}' table '{}', cache now contains {} entries",
            project_id,
            table_name,
            tables.len()
        );

        Ok(table_arc)
    }

    /// Internal helper to create/load a Delta table with caching and retry logic
    async fn create_delta_table_internal(&self, storage_uri: &str, storage_options: &HashMap<String, String>, table_name: &str) -> Result<DeltaTable> {
        // Create the base S3 object store
        let base_store = self.create_object_store(storage_uri, storage_options).instrument(tracing::trace_span!("create_object_store")).await?;
        let instrumented_store = instrument_object_store(base_store, "s3");

        let cached_store = if let Some(ref shared_cache) = self.object_store_cache {
            Arc::new(FoyerObjectStoreCache::new_with_shared_cache(instrumented_store.clone(), shared_cache)) as Arc<dyn object_store::ObjectStore>
        } else {
            warn!("Shared Foyer cache not initialized, using uncached object store");
            instrumented_store
        };

        // Try to load existing table
        match self.create_or_load_delta_table(storage_uri, storage_options.clone(), cached_store.clone()).await {
            Ok(table) => {
                info!("Loaded existing table '{}'", table_name);
                Ok(table)
            }
            Err(load_err) => {
                info!("Table '{}' doesn't exist, creating new table. err: {:?}", table_name, load_err);

                let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
                let mut create_attempts = 0;

                loop {
                    create_attempts += 1;
                    let commit_properties = CommitProperties::default().with_create_checkpoint(true).with_cleanup_expired_logs(Some(true));
                    let checkpoint_interval = self.config.parquet.timefusion_checkpoint_interval.to_string();

                    let mut config = HashMap::new();
                    config.insert("delta.checkpointInterval".to_string(), Some(checkpoint_interval));
                    config.insert("delta.checkpointPolicy".to_string(), Some("v2".to_string()));

                    match CreateBuilder::new()
                        .with_location(storage_uri)
                        .with_columns(schema.columns().unwrap_or_default())
                        .with_partition_columns(schema.partitions.clone())
                        .with_storage_options(storage_options.clone())
                        .with_commit_properties(commit_properties)
                        .with_configuration(config)
                        .await
                    {
                        Ok(table) => break Ok(table),
                        Err(create_err) => {
                            let err_str = create_err.to_string();
                            if (err_str.contains("already exists") || err_str.contains("version 0") || err_str.contains("ConditionalCheckFailedException"))
                                && create_attempts < 3
                            {
                                debug!("Table creation conflict, attempting to load existing table (attempt {})", create_attempts);
                                let backoff_ms = 100 * (2_u64.pow(create_attempts.min(5)));
                                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;

                                match self.create_or_load_delta_table(storage_uri, storage_options.clone(), cached_store.clone()).await {
                                    Ok(table) => break Ok(table),
                                    Err(reload_err) => {
                                        debug!("Failed to load table after creation conflict: {:?}", reload_err);
                                        continue;
                                    }
                                }
                            } else {
                                break Err(anyhow::anyhow!("Failed to create table: {}", create_err));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Legacy method for backward compatibility - routes to unified or custom table
    #[instrument(
        name = "database.get_or_create_table",
        skip(self),
        fields(project_id = %project_id, table.name = %table_name)
    )]
    pub async fn get_or_create_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Route to appropriate table based on whether project has custom storage
        if self.has_custom_storage(project_id, table_name).await {
            self.get_or_create_custom_table(project_id, table_name).await
        } else {
            self.get_or_create_unified_table(table_name).await
        }
    }

    /// Create an object store for the given URI and storage options
    async fn create_object_store(&self, storage_uri: &str, storage_options: &HashMap<String, String>) -> Result<Arc<dyn object_store::ObjectStore>> {
        use object_store::aws::AmazonS3Builder;
        use object_store::{BackoffConfig, ClientOptions, RetryConfig};
        use std::time::Duration;

        // Parse the S3 URI to extract bucket and prefix
        let url = Url::parse(storage_uri)?;
        let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("Invalid S3 URI: missing bucket"))?;

        // Configure retry with exponential backoff for transient network errors
        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(180),
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(15),
                base: 2.0,
            },
        };

        // Configure HTTP client with reasonable timeouts
        let client_options = ClientOptions::new().with_connect_timeout(Duration::from_secs(30)).with_timeout(Duration::from_secs(300));

        // Build S3 configuration
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket).with_retry(retry_config).with_client_options(client_options);

        // Apply storage options
        if let Some(access_key) = storage_options.get("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = storage_options.get("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(region) = storage_options.get("AWS_REGION") {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = storage_options.get("AWS_ENDPOINT_URL") {
            builder = builder.with_endpoint(endpoint);
            // If endpoint is HTTP, allow HTTP connections
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        // Use config values as fallback
        if storage_options.get("AWS_ACCESS_KEY_ID").is_none()
            && let Some(ref key) = self.config.aws.aws_access_key_id
        {
            builder = builder.with_access_key_id(key);
        }
        if storage_options.get("AWS_SECRET_ACCESS_KEY").is_none()
            && let Some(ref secret) = self.config.aws.aws_secret_access_key
        {
            builder = builder.with_secret_access_key(secret);
        }
        if storage_options.get("AWS_REGION").is_none()
            && let Some(ref region) = self.config.aws.aws_default_region
        {
            builder = builder.with_region(region);
        }

        // Check if we need to use config for endpoint and allow HTTP
        if storage_options.get("AWS_ENDPOINT_URL").is_none() {
            let endpoint = &self.config.aws.aws_s3_endpoint;
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        let store = builder.build()?;

        // Log if DynamoDB locking is enabled for this store
        if storage_options.get("AWS_S3_LOCKING_PROVIDER") == Some(&"dynamodb".to_string())
            && let Some(table_name) = storage_options.get("DELTA_DYNAMO_TABLE_NAME")
        {
            debug!("Object store configured with DynamoDB locking using table: {}", table_name);
        }

        Ok(Arc::new(store))
    }

    /// Creates or loads a DeltaTable with proper configuration.
    async fn create_or_load_delta_table(
        &self, storage_uri: &str, storage_options: HashMap<String, String>, cached_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<DeltaTable> {
        DeltaTableBuilder::from_url(Url::parse(storage_uri)?)?
            .with_storage_backend(cached_store.clone(), Url::parse(storage_uri)?)
            .with_storage_options(storage_options.clone())
            .with_allow_http(true)
            .load()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to load table: {}", e))
    }

    #[instrument(
        name = "delta.insert_batch",
        skip_all,
        fields(
            table.name = %table_name,
            project_id = %project_id,
            batches.count = batches.len(),
            rows.count = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            use_queue = Empty,
        )
    )]
    pub async fn insert_records_batch(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool) -> Result<()> {
        let span = tracing::Span::current();

        // Extract project_id from first batch if not provided
        let project_id = if project_id.is_empty() && !batches.is_empty() {
            extract_project_id(&batches[0]).unwrap_or_else(|| "default".to_string())
        } else if project_id.is_empty() {
            "default".to_string()
        } else {
            project_id.to_string()
        };

        // Use provided table_name or default to otel_logs_and_spans
        let table_name = if table_name.is_empty() { "otel_logs_and_spans".to_string() } else { table_name.to_string() };

        // If buffered layer is configured and not skipping, use it (WAL → MemBuffer flow)
        if !skip_queue && let Some(ref layer) = self.buffered_layer {
            span.record("use_queue", "buffered_layer");
            return layer.insert(&project_id, &table_name, batches).await;
        }

        // Fallback to legacy batch queue if configured
        let enable_queue = self.config.core.enable_batch_queue;
        if !skip_queue
            && enable_queue
            && let Some(ref queue) = self.batch_queue
        {
            span.record("use_queue", true);
            for batch in batches {
                if let Err(e) = queue.queue(batch) {
                    return Err(anyhow::anyhow!("Queue error: {}", e));
                }
            }
            return Ok(());
        }

        span.record("use_queue", false);

        // Get or create the table
        let table_ref = self.get_or_create_table(&project_id, &table_name).await?;

        // Get the appropriate schema for this table
        let schema = get_schema(&table_name).unwrap_or_else(get_default_schema);

        let writer_properties = self.create_writer_properties(schema.sorting_columns(), &schema.fields);

        // Retry logic for concurrent writes
        let max_retries = 5;
        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count < max_retries {
            // Hold the write lock for the entire operation to prevent concurrent conflicts
            let mut table = table_ref.write().await;

            // Update the table state to get the latest version before writing
            if let Err(e) = table.update_state().await {
                debug!("Failed to update table state before write (attempt {}): {}", retry_count + 1, e);
            }

            let write_span = tracing::trace_span!(parent: &span, "delta.write_operation", retry_attempt = retry_count + 1);
            let write_result = async {
                // Schema evolution enabled: new columns will be automatically added to the table
                table
                    .clone()
                    .write(batches.clone())
                    .with_partition_columns(schema.partitions.clone())
                    .with_writer_properties(writer_properties.clone())
                    .with_save_mode(deltalake::protocol::SaveMode::Append)
                    .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
                    .await
            }
            .instrument(write_span)
            .await;

            match write_result {
                Ok(new_table) => {
                    // Track the version we just wrote
                    if let Some(version) = new_table.version() {
                        // Store the last written version for read-after-write consistency
                        let mut versions = self.last_written_versions.write().await;
                        versions.insert((project_id.clone(), table_name.clone()), version);
                        debug!("Stored last written version for {}/{}: {}", project_id, table_name, version);
                    } else {
                        debug!("WARNING: No version available after write for {}/{}", project_id, table_name);
                    }

                    *table = new_table;

                    // Invalidate statistics cache after successful write
                    drop(table); // Release write lock before async operation
                    self.statistics_extractor.invalidate(&project_id, &table_name).await;
                    debug!("Invalidated statistics cache after write to {}/{}", project_id, table_name);

                    return Ok(());
                }
                Err(e) => {
                    let error_str = e.to_string();
                    if error_str.contains("already exists")
                        || error_str.contains("conflict")
                        || error_str.contains("version")
                        || error_str.contains("ConditionalCheckFailedException")
                        || error_str.contains("concurrent modification")
                    {
                        // This is a version conflict or DynamoDB locking conflict, retry
                        retry_count += 1;
                        last_error = Some(e);
                        debug!(
                            "Delta write conflict detected (possibly DynamoDB lock conflict), retrying... (attempt {}/{})",
                            retry_count, max_retries
                        );

                        // Exponential backoff for better handling of concurrent writes
                        let backoff_ms = 100 * (2_u64.pow(retry_count.min(5)));
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;

                        // Drop the lock and try to reload the table
                        drop(table);

                        // Force a table state reload on conflict
                        if let Err(reload_err) = table_ref.write().await.update_state().await {
                            debug!("Failed to reload table state after conflict: {}", reload_err);
                        }
                    } else {
                        // Non-retryable error
                        return Err(anyhow::anyhow!("Delta write failed: {}", e));
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Delta write failed after {} retries: {}",
            max_retries,
            last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    /// Optimize the Delta table using Z-ordering on timestamp and id columns
    /// This improves query performance for time-based queries
    pub async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, _target_size: Option<i64>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let window_hours = self.config.maintenance.timefusion_optimize_window_hours.max(1);
        info!("Starting Delta table optimization with Z-ordering (last {} hours)", window_hours);

        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        let target_size = self.config.parquet.timefusion_optimize_target_size;

        // Generate partition filters for each date in the configurable window
        let now = Utc::now();
        let num_days = (window_hours / 24).max(1);
        let partition_filters: Vec<PartitionFilter> = (0..=num_days)
            .filter_map(|days_ago| {
                let date = (now - chrono::Duration::days(days_ago as i64)).date_naive();
                PartitionFilter::try_from(("date", "=", date.to_string().as_str())).ok()
            })
            .collect();
        info!("Optimizing files from {} date partitions", partition_filters.len());

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let writer_properties = self.create_writer_properties(schema.sorting_columns(), &schema.fields);

        let optimize_result = table_clone
            .optimize()
            .with_filters(&partition_filters)
            .with_type(if schema.z_order_columns.is_empty() {
                deltalake::operations::optimize::OptimizeType::Compact
            } else {
                deltalake::operations::optimize::OptimizeType::ZOrder(schema.z_order_columns.clone())
            })
            .with_target_size(target_size as u64)
            .with_writer_properties(writer_properties)
            .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
            .await;

        match optimize_result {
            Ok((new_table, metrics)) => {
                let min_files = self.config.maintenance.timefusion_compact_min_files;
                if metrics.total_considered_files < min_files {
                    debug!(
                        "Skipping optimization commit: {} files < min threshold {}",
                        metrics.total_considered_files, min_files
                    );
                    return Ok(());
                }
                let duration = start_time.elapsed();
                info!(
                    "Optimization completed in {:?}: {} files removed, {} files added, {} partitions optimized, {} total files considered, {} files skipped",
                    duration,
                    metrics.num_files_removed,
                    metrics.num_files_added,
                    metrics.partitions_optimized,
                    metrics.total_considered_files,
                    metrics.total_files_skipped
                );
                if metrics.num_files_removed > 0 {
                    let compression_ratio = metrics.num_files_removed as f64 / metrics.num_files_added as f64;
                    info!("Optimization compression ratio: {:.2}x", compression_ratio);
                }
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

    pub async fn optimize_table_light(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        let today = Utc::now().date_naive();
        info!("Light optimizing files from date: {}", today);

        let partition_filters = vec![PartitionFilter::try_from(("date", "=", today.to_string().as_str()))?];
        let target_size = self.config.maintenance.timefusion_light_optimize_target_size;

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let optimize_result = table_clone
            .optimize()
            .with_filters(&partition_filters)
            .with_type(deltalake::operations::optimize::OptimizeType::Compact)
            .with_target_size(target_size as u64)
            .with_writer_properties(self.create_writer_properties(schema.sorting_columns(), &schema.fields))
            .with_min_commit_interval(tokio::time::Duration::from_secs(30))
            .await;

        match optimize_result {
            Ok((new_table, metrics)) => {
                let min_files = self.config.maintenance.timefusion_compact_min_files;
                if metrics.total_considered_files < min_files {
                    debug!(
                        "Skipping light optimization commit: {} files < min threshold {}",
                        metrics.total_considered_files, min_files
                    );
                    return Ok(());
                }
                let duration = start_time.elapsed();
                info!(
                    "Light optimization completed in {:?}: {} files removed, {} files added",
                    duration, metrics.num_files_removed, metrics.num_files_added
                );
                let mut table = table_ref.write().await;
                *table = new_table;
                Ok(())
            }
            Err(e) => {
                error!("Light optimization operation failed: {}", e);
                Err(anyhow::anyhow!("Light table optimization failed: {}", e))
            }
        }
    }

    /// Vacuum the Delta table to clean up old files that are no longer needed
    /// This reduces storage costs and improves query performance
    async fn vacuum_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) {
        // Log the start of the vacuum operation
        let start_time = std::time::Instant::now();
        info!("Starting vacuum operation with retention period of {} hours", retention_hours);

        // Get a clone of the table to avoid holding the lock during the operation
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Directly run vacuum without dry run to delete old files
        match table_clone
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            .await
        {
            Ok((_, metrics)) => {
                let duration = start_time.elapsed();
                let files_deleted = metrics.files_deleted.len();
                info!("Vacuum completed in {:?}, deleted {} files", duration, files_deleted);

                // Log file sizes for monitoring storage savings
                if !metrics.files_deleted.is_empty() {
                    let _total_size: u64 = metrics
                        .files_deleted
                        .iter()
                        .filter_map(|_path| {
                            // Extract size from path if available
                            // This is a simplified approach - in production you might want to query actual file sizes
                            None::<u64>
                        })
                        .sum();
                    debug!("Vacuum operation details: {:?}", metrics.files_deleted);
                }

                // Update the table state after vacuum
                let mut table = table_ref.write().await;
                if table.update_state().await.is_ok() {
                    info!("Table state updated after vacuum");
                } else {
                    error!("Failed to update table state after vacuum");
                }
            }
            Err(e) => error!("Vacuum operation failed: {}", e),
        }
    }

    /// Get table statistics using the statistics extractor
    pub async fn get_table_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        // Get the schema for this table
        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
        let schema = schema_def.schema_ref();
        self.statistics_extractor.extract_statistics(table, project_id, table_name, &schema).await
    }

    /// Clear the statistics cache
    pub async fn clear_statistics_cache(&self) {
        self.statistics_extractor.clear_cache().await
    }

    /// Invalidate statistics for a specific table
    pub async fn invalidate_table_statistics(&self, project_id: &str, table_name: &str) {
        self.statistics_extractor.invalidate(project_id, table_name).await
    }

    /// Gracefully shutdown the database, including cache and maintenance tasks
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down TimeFusion database...");

        // Cancel maintenance tasks
        self.maintenance_shutdown.cancel();

        // Shutdown batch queue if present
        if let Some(ref queue) = self.batch_queue {
            info!("Flushing batch queue...");
            queue.shutdown().await;
        }

        // Log final cache stats and shutdown cache
        if let Some(ref cache) = self.object_store_cache {
            info!("Shutting down Foyer cache...");
            cache.log_stats().await;
            cache.shutdown().await?;
        }

        // Close PostgreSQL connection pool if present
        if let Some(ref pool) = self.config_pool {
            pool.close().await;
        }

        info!("Database shutdown complete");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
    _batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    table_name: String,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>, table_name: String,
    ) -> Self {
        Self {
            default_project,
            database,
            schema,
            _batch_queue: batch_queue,
            table_name,
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

    fn schema(&self) -> SchemaRef {
        // Return INSERT-compatible schema where Variant columns appear as Utf8View.
        // This allows INSERT statements with JSON strings to pass DataFusion's type validation.
        // VariantConversionExec handles string->Variant conversion during write.
        // The pgwire layer handles Variant->JSON conversion during read via VariantJsonExec.
        create_insert_compatible_schema(&self.schema)
    }

    /// Return the actual schema with Variant types (for internal use)
    fn real_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    #[allow(clippy::only_used_in_recursion)]
    fn extract_project_id(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
                // Check column = value (both Utf8 and Utf8View)
                if let Expr::Column(col) = left.as_ref()
                    && col.name == "project_id"
                {
                    match right.as_ref() {
                        Expr::Literal(ScalarValue::Utf8(Some(v)), _) => return Some(v.clone()),
                        Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => return Some(v.clone()),
                        _ => {}
                    }
                }
                // Check value = column (both Utf8 and Utf8View)
                if let Expr::Column(col) = right.as_ref()
                    && col.name == "project_id"
                {
                    match left.as_ref() {
                        Expr::Literal(ScalarValue::Utf8(Some(v)), _) => return Some(v.clone()),
                        Expr::Literal(ScalarValue::Utf8View(Some(v)), _) => return Some(v.clone()),
                        _ => {}
                    }
                }
                None
            }
            Expr::Not(inner) => self.extract_project_id(inner),
            _ => None,
        }
    }

    /// Determines if a filter can be pushed down exactly to Delta Lake
    fn is_exact_pushdown_filter(expr: &Expr) -> bool {
        match expr {
            // AND expressions are exact if all parts are exact (check this first)
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => Self::is_exact_pushdown_filter(left) && Self::is_exact_pushdown_filter(right),
            // Simple column comparisons are exact
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let is_column_literal = matches!(
                    (left.as_ref(), right.as_ref()),
                    (Expr::Column(_), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(_))
                );

                let is_supported_op = matches!(
                    op,
                    Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                );

                if is_column_literal && is_supported_op {
                    // Check if it's a partition column or indexed column
                    if let Expr::Column(col) = left.as_ref() {
                        return Self::is_pushdown_column(&col.name);
                    }
                    if let Expr::Column(col) = right.as_ref() {
                        return Self::is_pushdown_column(&col.name);
                    }
                }
                false
            }
            // IS NULL/IS NOT NULL are exact
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                matches!(inner.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            // IN lists are exact for pushdown columns
            Expr::InList(in_list) => {
                matches!(in_list.expr.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            _ => false,
        }
    }

    /// Checks if a column supports exact pushdown (partitions, sorted columns, indexed columns)
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(
            column_name,
            "project_id" | "date" | "timestamp" | "id" | "level" | "status_code" | "resource___service___name" | "name" | "duration"
        )
    }

    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::optimizers::time_range_partition_pruner;

        let mut optimized_filters = Vec::new();
        let mut has_date_filter = false;

        // First, check if we already have a date filter to avoid duplicates
        for filter in filters {
            if Self::is_date_filter(filter) {
                has_date_filter = true;
            }
            optimized_filters.push(filter.clone());
        }

        // Only add date filters if we don't already have one
        if !has_date_filter {
            for filter in filters {
                // Check if this is a timestamp filter that needs a date filter added
                if let Some(date_filter) = time_range_partition_pruner::timestamp_to_date_filter(filter) {
                    optimized_filters.push(date_filter);
                    debug!("Added date partition filter for timestamp query optimization");
                }
            }
        }

        // Check if project_id filter is present
        if !self.has_project_id_in_filters(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }

        Ok(optimized_filters)
    }

    /// Check if an expression is a date filter
    fn is_date_filter(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                matches!(left.as_ref(), Expr::Column(col) if col.name == "date")
            }
            _ => false,
        }
    }

    /// Check if filters contain a project_id filter
    fn has_project_id_in_filters(&self, filters: &[Expr]) -> bool {
        use crate::optimizers::ProjectIdPushdown;
        ProjectIdPushdown::has_project_id_filter(filters)
    }

    /// Create a MemorySourceConfig-based execution plan with multiple partitions
    fn create_memory_exec(&self, partitions: &[Vec<RecordBatch>], projection: Option<&Vec<usize>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mem_source =
            MemorySourceConfig::try_new(partitions, self.schema.clone(), projection.cloned()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(DataSourceExec::new(Arc::new(mem_source))))
    }

    /// Scan a Delta table and coerce output schema to match our expected types.
    /// Handles object store registration, projection translation, and type coercion (e.g., Utf8 -> Utf8View).
    async fn scan_delta_table(
        &self, table: &DeltaTable, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        table.update_datafusion_session(state).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let provider = table.table_provider().await.map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Translate projection indices from our schema to delta table's schema.
        // DataFusion passes indices based on ProjectRoutingTable.schema, but the
        // delta table provider expects indices based on its own schema.
        let delta_schema = provider.schema();
        let translated_projection = projection.map(|proj| {
            let mut translated = Vec::with_capacity(proj.len());
            for &idx in proj {
                let col_name = self.schema.field(idx).name();
                if let Some(delta_idx) = delta_schema.fields().iter().position(|f| f.name() == col_name) {
                    translated.push(delta_idx);
                } else {
                    warn!(
                        "Column '{}' requested in projection but not found in Delta schema for table '{}'",
                        col_name, self.table_name
                    );
                }
            }
            translated
        });

        let delta_plan = provider.scan(state, translated_projection.as_ref(), filters, limit).await?;

        // Determine target schema based on projection
        let target_schema = match projection {
            Some(proj) => Arc::new(arrow_schema::Schema::new(
                proj.iter().map(|&idx| self.schema.field(idx).clone()).collect::<Vec<_>>(),
            )),
            None => self.schema.clone(),
        };

        Self::coerce_plan_to_schema(delta_plan, &target_schema)
    }

    /// Wrap an execution plan with type coercion if the output schema doesn't match the target.
    /// This handles cases like Delta returning Utf8 when we expect Utf8View.
    fn coerce_plan_to_schema(plan: Arc<dyn ExecutionPlan>, target_schema: &SchemaRef) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan_schema = plan.schema();
        if plan_schema.fields().len() != target_schema.fields().len() {
            return Ok(plan);
        }

        let needs_coercion = plan_schema
            .fields()
            .iter()
            .zip(target_schema.fields())
            .any(|(plan_field, target_field)| plan_field.data_type() != target_field.data_type());

        if !needs_coercion {
            return Ok(plan);
        }

        let cast_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = plan_schema
            .fields()
            .iter()
            .enumerate()
            .zip(target_schema.fields())
            .map(|((idx, plan_field), target_field)| {
                let col_expr = Arc::new(PhysicalColumn::new(plan_field.name(), idx)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
                let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = if plan_field.data_type() != target_field.data_type() {
                    Arc::new(CastExpr::new(col_expr, target_field.data_type().clone(), None))
                } else {
                    col_expr
                };
                (expr, target_field.name().clone())
            })
            .collect();

        Ok(Arc::new(ProjectionExec::try_new(cast_exprs, plan)?))
    }

    /// Helper to scan Delta only (when no MemBuffer data)
    async fn scan_delta_only(
        &self, state: &dyn Session, project_id: &str, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let delta_table = self.database.resolve_table(project_id, &self.table_name).await?;
        let table = delta_table.read().await;
        self.scan_delta_table(&table, state, projection, filters, limit).await
    }

    /// Extract time range (min, max) from query filters.
    /// Returns None if no time constraints found.
    fn extract_time_range_from_filters(&self, filters: &[Expr]) -> Option<(i64, i64)> {
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        for filter in filters {
            if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
                // Check if left side is timestamp column
                let is_timestamp_col = matches!(left.as_ref(), Expr::Column(c) if c.name == "timestamp");
                if !is_timestamp_col {
                    continue;
                }

                // Extract timestamp value from right side
                let ts_value = match right.as_ref() {
                    Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts), _), _) => Some(*ts),
                    Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _), _) => Some(*ts / 1000),
                    Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), _), _) => Some(*ts * 1000),
                    Expr::Literal(ScalarValue::TimestampSecond(Some(ts), _), _) => Some(*ts * 1_000_000),
                    _ => None,
                };

                if let Some(ts) = ts_value {
                    match op {
                        Operator::Gt | Operator::GtEq => {
                            min_ts = Some(min_ts.map_or(ts, |m| m.max(ts)));
                        }
                        Operator::Lt | Operator::LtEq => {
                            max_ts = Some(max_ts.map_or(ts, |m| m.min(ts)));
                        }
                        Operator::Eq => {
                            min_ts = Some(ts);
                            max_ts = Some(ts);
                        }
                        _ => {}
                    }
                }
            }
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Some((min, max)),
            (Some(min), None) => Some((min, i64::MAX)),
            (None, Some(max)) => Some((i64::MIN, max)),
            (None, None) => None,
        }
    }
}

// Needed by DataSink
impl DisplayAs for ProjectRoutingTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProjectRoutingTable ")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "ProjectRoutingTable ")
            }
        }
    }
}

#[async_trait]
impl DataSink for ProjectRoutingTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[instrument(
        name = "datafusion.table.write",
        skip_all,
        fields(
            table.name = %self.table_name,
            operation = "INSERT",
            rows.count = Empty,
            projects.count = Empty,
        )
    )]
    async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let span = tracing::Span::current();
        let mut total_row_count = 0;
        let mut project_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let target_schema = self.schema();

        // Collect and group batches by project_id, converting variant columns
        while let Some(batch) = data.next().await.transpose()? {
            let batch_rows = batch.num_rows();
            debug!("write_all: received batch with {} rows", batch_rows);
            total_row_count += batch_rows;
            let project_id = extract_project_id(&batch).unwrap_or_else(|| self.default_project.clone());

            // Convert string columns to Variant where target schema expects Variant
            let converted_batch = convert_variant_columns(batch, &target_schema)?;
            project_batches.entry(project_id).or_default().push(converted_batch);
        }

        span.record("rows.count", total_row_count);
        span.record("projects.count", project_batches.len());

        if project_batches.is_empty() {
            return Ok(0);
        }

        // Insert batches for each project
        for (project_id, batches) in project_batches {
            let batch_count = batches.len();
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            debug!(
                "write_all: inserting {} batches with {} total rows for project {}",
                batch_count, row_count, project_id
            );

            let insert_span = tracing::trace_span!(parent: &span, "delta_table.insert", project_id = %project_id, rows = row_count);
            self.database
                .insert_records_batch(&project_id, &self.table_name, batches, false)
                .instrument(insert_span)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Insert error for project {} table {}: {}", project_id, self.table_name, e)))?;
        }

        debug!("write_all: completed insertion of {} total rows", total_row_count);
        Ok(total_row_count as u64)
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
        // Check that the schema of the plan is compatible with this table.
        // Use custom compatibility check that allows string -> Variant conversion.
        match is_schema_compatible_for_insert(&input.schema(), &self.schema()) {
            Ok(_) => debug!("insert_into; Schema validation passed (with Variant compatibility)"),
            Err(e) => {
                error!("Schema validation failed: {}", e);
                return Err(e);
            }
        }

        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }

        // Wrap input with VariantConversionExec to convert string columns to Variant.
        let converted_input: Arc<dyn ExecutionPlan> = Arc::new(VariantConversionExec::new(input, self.real_schema()));

        // Create sink executor with the converted input
        let sink = DataSinkExec::new(converted_input, Arc::new(self.clone()), None);

        Ok(Arc::new(sink))
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Analyze each filter to determine if it can be pushed down exactly
        Ok(filter
            .iter()
            .map(|f| {
                if Self::is_exact_pushdown_filter(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    #[instrument(
        name = "datafusion.table.scan",
        skip_all,
        fields(
            table.name = %self.table_name,
            table.project_id = Empty,
            scan.filters_count = filters.len(),
            scan.has_limit = limit.is_some(),
            scan.limit = limit.unwrap_or(0),
            scan.has_projection = projection.is_some(),
            scan.uses_mem_buffer = false,
            scan.skipped_delta = false,
        )
    )]
    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let span = tracing::Span::current();

        // Apply our custom optimizations to the filters
        let optimized_filters = self.apply_time_series_optimizations(filters)?;

        // Get project_id from filters if possible, otherwise use default
        let project_id = self.extract_project_id_from_filters(&optimized_filters).unwrap_or_else(|| self.default_project.clone());
        span.record("table.project_id", project_id.as_str());

        let has_variant_columns = self.real_schema().fields().iter().any(|f| is_variant_type(f.data_type()));
        let wrap_result = |plan: Arc<dyn ExecutionPlan>| -> DFResult<Arc<dyn ExecutionPlan>> {
            if has_variant_columns {
                Ok(Arc::new(VariantToJsonExec::new(plan, self.real_schema())))
            } else {
                Ok(plan)
            }
        };

        // Check if buffered layer is configured
        let has_layer = self.database.buffered_layer().is_some();
        debug!("ProjectRoutingTable::scan - buffered_layer present: {}, project_id: {}", has_layer, project_id);
        let Some(layer) = self.database.buffered_layer() else {
            // No buffered layer, query Delta directly
            debug!("No buffered layer, querying Delta only");
            let plan = self.scan_delta_only(state, &project_id, projection, &optimized_filters, limit).await?;
            return wrap_result(plan);
        };

        span.record("scan.uses_mem_buffer", true);

        // Get MemBuffer's time range for this project/table
        let mem_time_range = layer.get_time_range(&project_id, &self.table_name);

        // Extract query time range from filters
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);

        // Determine if we can skip Delta (query entirely within MemBuffer range)
        let skip_delta = match (mem_time_range, query_time_range) {
            (Some((mem_oldest, mem_newest)), Some((query_min, query_max))) => {
                // Skip Delta if query's entire time range is within MemBuffer
                query_min >= mem_oldest && query_max <= mem_newest
            }
            _ => false,
        };

        // Query MemBuffer with partitioned data for parallel execution
        let mem_partitions = match layer.query_partitioned(&project_id, &self.table_name, &optimized_filters) {
            Ok(partitions) => partitions,
            Err(e) => {
                warn!("Failed to query mem buffer: {}", e);
                vec![]
            }
        };

        // If no mem buffer data, query Delta only
        debug!("MemBuffer partitions count: {} for {}/{}", mem_partitions.len(), project_id, self.table_name);
        if mem_partitions.is_empty() {
            debug!("No MemBuffer data, querying Delta only for {}/{}", project_id, self.table_name);
            let plan = self.scan_delta_only(state, &project_id, projection, &optimized_filters, limit).await?;
            return wrap_result(plan);
        }

        // Create MemorySourceConfig with multiple partitions for parallel execution
        let mem_plan = self.create_memory_exec(&mem_partitions, projection)?;

        // If we can skip Delta, return mem plan directly
        if skip_delta {
            span.record("scan.skipped_delta", true);
            debug!(
                "Skipping Delta scan - query time range entirely within MemBuffer for {}/{}",
                project_id, self.table_name
            );
            return wrap_result(mem_plan);
        }

        // Get oldest timestamp from MemBuffer for time-based exclusion
        let oldest_mem_ts = mem_time_range.map(|(oldest, _)| oldest);

        // Build Delta filters with time exclusion
        let delta_filters = if let Some(cutoff) = oldest_mem_ts {
            let exclusion = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("timestamp")),
                op: Operator::Lt,
                right: Box::new(lit(ScalarValue::TimestampMicrosecond(Some(cutoff), Some("UTC".into())))),
            });
            let mut filters = optimized_filters.clone();
            filters.push(exclusion);
            filters
        } else {
            optimized_filters.clone()
        };

        // Execute Delta query
        let resolve_span = tracing::trace_span!(parent: &span, "resolve_delta_table");
        let delta_table = self.database.resolve_table(&project_id, &self.table_name).instrument(resolve_span).await?;
        let table = delta_table.read().await;
        let delta_plan = self.scan_delta_table(&table, state, projection, &delta_filters, limit).await?;

        // Union both plans (mem data first for recency, then Delta for historical)
        wrap_result(UnionExec::try_new(vec![mem_plan, delta_plan])?)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Cancel maintenance tasks immediately
        self.maintenance_shutdown.cancel();

        // Note: We can't do async cleanup in Drop, but cancelling the token
        // will cause background tasks to stop, preventing the panic
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;
    use crate::test_utils::test_helpers::*;
    use serial_test::serial;
    use std::path::PathBuf;

    /// Helper function to extract string value from array column, handling different string array types
    fn get_str(array: &dyn Array, idx: usize) -> String {
        use datafusion::arrow::array::{LargeStringArray, StringArray, StringViewArray};
        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            arr.value(idx).to_string()
        } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
            arr.value(idx).to_string()
        } else if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
            arr.value(idx).to_string()
        } else {
            panic!("Unsupported string array type: {:?}", array.data_type())
        }
    }

    fn create_test_config(test_id: &str) -> Arc<AppConfig> {
        let mut cfg = AppConfig::default();
        // S3/MinIO settings
        cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
        cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
        cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
        cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
        cfg.aws.aws_default_region = Some("us-east-1".to_string());
        cfg.aws.aws_allow_http = Some("true".to_string());
        // Core settings - unique per test
        cfg.core.timefusion_table_prefix = format!("test-{}", test_id);
        cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-db-{}", test_id));
        // Disable Foyer cache for tests
        cfg.cache.timefusion_foyer_disabled = true;
        Arc::new(cfg)
    }

    async fn setup_test_database() -> Result<(Database, SessionContext, String)> {
        let test_prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_prefix);
        let db = Database::with_config(cfg).await?;
        let db_arc = Arc::new(db.clone());
        let mut ctx = db_arc.create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db.setup_session_context(&mut ctx)?;
        Ok((db, ctx, test_prefix))
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_and_query() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("project_{}", prefix);

            // Test basic insert
            let batch = json_to_batch(vec![test_span("test1", "span1", &project_id)])?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

            // Verify count
            let result = ctx
                .sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
                .await?
                .collect()
                .await?;
            use datafusion::arrow::array::AsArray;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            assert_eq!(count, 1);

            // Test field selection
            let result = ctx
                .sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "test1");
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "span1");

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_projects() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let projects: Vec<String> = (1..=3).map(|i| format!("proj{}_{}", i, prefix)).collect();

            // Insert data for multiple projects
            for project in &projects {
                let batch = json_to_batch(vec![test_span(&format!("id_{}", project), &format!("span_{}", project), project)])?;
                db.insert_records_batch(project, "otel_logs_and_spans", vec![batch], true).await?;
            }

            // Verify project isolation
            use datafusion::arrow::array::AsArray;
            for project in &projects {
                let sql = format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                assert_eq!(result[0].num_rows(), 1);
                assert_eq!(get_str(result[0].column(0).as_ref(), 0), format!("id_{}", project));
            }

            // Verify total count - need to check across all projects
            let mut total_count = 0;
            for project in &projects {
                let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
                total_count += count;
            }
            assert_eq!(total_count, 3);

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_filtering() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("filter_proj_{}", prefix);
            use chrono::Utc;
            use serde_json::json;

            let now = Utc::now();
            let records = vec![
                json!({
                    "timestamp": now.timestamp_micros(),
                    "id": "span1",
                    "name": "test_span_1",
                    "project_id": &project_id,
                    "level": "INFO",
                    "status_code": "OK",
                    "duration": 100_000_000,
                    "date": now.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Test span 1 - INFO level"]
                }),
                json!({
                    "timestamp": (now + chrono::Duration::minutes(10)).timestamp_micros(),
                    "id": "span2",
                    "name": "test_span_2",
                    "project_id": &project_id,
                    "level": "ERROR",
                    "status_code": "ERROR",
                    "status_message": "Error occurred",
                    "duration": 200_000_000,
                    "date": now.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Test span 2 - ERROR level"]
                }),
            ];

            let batch = json_to_batch(records)?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

            // Test filtering by level
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "span2");

            // Test filtering by duration
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND duration > 150000000",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "span2");

            // Test compound filtering
            let result = ctx
                .sql(&format!(
                    "SELECT id, status_message FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "Error occurred");

            // Shutdown database to ensure proper cleanup
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_insert() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let proj1 = format!("default_{}", prefix);
            let proj2 = format!("proj2_{}", prefix);
            use datafusion::arrow::array::AsArray;

            // Insert via API first
            let batch = json_to_batch(vec![test_span("id1", "name1", &proj1)])?;
            db.insert_records_batch(&proj1, "otel_logs_and_spans", vec![batch], true).await?;

            // Insert via SQL
            let sql = format!(
                "INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES (
                       '{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z',
                       'sql_id', ARRAY[], 'sql_name', 'INFO', 'OK', ARRAY['SQL inserted test span']
                     )",
                proj2
            );
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);

            // Verify both records exist - need to check both projects
            let mut total_count = 0;
            for project in [&proj1, &proj2] {
                let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
                total_count += count;
            }
            assert_eq!(total_count, 2);

            // Verify SQL-inserted record
            let result = ctx
                .sql(&format!(
                    "SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'sql_id'",
                    proj2
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "sql_name");

            db.shutdown().await?;
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_multi_row_sql_insert() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("multirow_{}", prefix);
            use datafusion::arrow::array::AsArray;

            // Test multi-row INSERT
            let sql = format!("INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES
                     ('{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 'id1', ARRAY[], 'name1', 'INFO', 'OK', ARRAY['Multi-row insert test 1']),
                     ('{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T11:00:00Z', 'id2', ARRAY[], 'name2', 'INFO', 'OK', ARRAY['Multi-row insert test 2']),
                     ('{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T12:00:00Z', 'id3', ARRAY[], 'name3', 'ERROR', 'ERROR', ARRAY['Multi-row insert test 3 - ERROR'])",
                     project_id, project_id, project_id);

            // Multi-row INSERT returns a count of rows inserted
            let result = ctx.sql(&sql).await?.collect().await?;
            let inserted_count = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
            assert_eq!(inserted_count, 3);

            // Verify all 3 records exist
            let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
            let result = ctx.sql(&sql).await?.collect().await?;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            assert_eq!(count, 3);

            // Verify individual records
            let result = ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}' ORDER BY id", project_id)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 3);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "id1");
            assert_eq!(get_str(result[0].column(0).as_ref(), 1), "id2");
            assert_eq!(get_str(result[0].column(0).as_ref(), 2), "id3");

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_timestamp_operations() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("ts_test_{}", prefix);
            use chrono::Utc;
            use serde_json::json;

            let base_time = chrono::DateTime::parse_from_rfc3339("2023-01-01T10:00:00Z").unwrap().with_timezone(&Utc);
            let records = vec![
                json!({
                    "timestamp": base_time.timestamp_micros(),
                    "id": "early",
                    "name": "early_span",
                    "project_id": &project_id,
                    "date": base_time.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Early span for timestamp test"]
                }),
                json!({
                    "timestamp": (base_time + chrono::Duration::hours(2)).timestamp_micros(),
                    "id": "late",
                    "name": "late_span",
                    "project_id": &project_id,
                    "date": base_time.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Late span for timestamp test"]
                }),
            ];

            let batch = json_to_batch(records)?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

            // First check if any records were inserted - need to specify project_id
            let all_records = ctx
                .sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
                .await?
                .collect()
                .await?;
            assert!(!all_records.is_empty(), "No records found in table");

            // Test timestamp filtering - need to include project_id
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp > '2023-01-01T11:00:00Z'",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert!(!result.is_empty(), "Query returned no results");
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "late");

            // Test timestamp formatting - need to include project_id
            let result = ctx
                .sql(&format!(
                    "SELECT id, to_char(timestamp, '%Y-%m-%d %H:%M') as ts FROM otel_logs_and_spans WHERE project_id = '{}' ORDER BY timestamp",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 2);
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "2023-01-01 10:00");
            assert_eq!(get_str(result[0].column(1).as_ref(), 1), "2023-01-01 12:00");

            // Shutdown database to ensure proper cleanup
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_writes_same_project() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Database::new().await?;
            let db = Arc::new(db);
            let project_id = format!("concurrent_test_{}", uuid::Uuid::new_v4());

            // Create 3 concurrent write tasks (reduced from 10 to minimize Delta conflicts)
            let tasks = (0..3).map(|i| {
                let db = Arc::clone(&db);
                let project = project_id.clone();

                tokio::spawn(async move {
                    let batch_id = format!("batch_{}", i);
                    let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project)])?;
                    db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], true).await.map(|_| batch_id)
                })
            });

            let results: Vec<Result<String, _>> = futures::future::join_all(tasks)
                .await
                .into_iter()
                .map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?)
                .collect();

            let successful_writes: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;
            assert_eq!(successful_writes.len(), 3, "All 3 concurrent writes should succeed");

            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 60 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_table_creation() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Database::new().await?;
            let db = Arc::new(db);

            // Create multiple projects concurrently - each will try to create its own table
            let tasks = (0..5).map(|i| {
                let db = Arc::clone(&db);
                let project_id = format!("project_create_test_{}", i);

                tokio::spawn(async move {
                    let batch_id = format!("init_batch_{}", i);
                    let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project_id)])?;
                    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await.map(|_| project_id)
                })
            });

            // Wait for all tasks to complete
            let results: Vec<Result<String, _>> = futures::future::join_all(tasks)
                .await
                .into_iter()
                .map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?)
                .collect();

            let created_projects: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;
            assert_eq!(created_projects.len(), 5, "All 5 projects should be created successfully");

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 60 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_queue_under_load() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            use crate::batch_queue::BatchQueue;

            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Arc::new(Database::new().await?);
            let queue = BatchQueue::new(Arc::clone(&db), 100, 50); // 100ms interval, 50 rows max

            let project_id = format!("queue_test_{}", uuid::Uuid::new_v4());

            // Queue many batches rapidly
            for i in 0..100 {
                let batch_id = format!("queued_batch_{}", i);
                let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project_id)])?;

                match queue.queue(batch) {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("Queue full") => break,
                    Err(e) => return Err(e),
                }
            }

            // Give queue time to process
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            queue.shutdown().await;
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_mixed_operations() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Database::new().await?;
            let db = Arc::new(db);

            // Test concurrent writes to DIFFERENT projects (no conflicts)
            let mut handles = Vec::new();
            for i in 0..3 {
                let db_clone = Arc::clone(&db);
                let project_id = format!("project_{}", i);
                handles.push(tokio::spawn(async move {
                    let batch = json_to_batch(vec![test_span(&format!("id_{}", i), &format!("span_{}", i), &project_id)])?;
                    db_clone.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;
                    Ok::<_, anyhow::Error>(())
                }));
            }

            // Wait for all writes
            for handle in handles {
                handle.await??;
            }

            // Now test concurrent reads across all projects
            let mut read_handles = Vec::new();
            for i in 0..3 {
                let db_clone = Arc::clone(&db);
                let project_id = format!("project_{}", i);
                read_handles.push(tokio::spawn(async move {
                    let ctx = db_clone.clone().create_session_context();
                    let _ = ctx.sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id)).await;
                    Ok::<_, anyhow::Error>(())
                }));
            }

            for handle in read_handles {
                handle.await??;
            }

            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 60 seconds"))?
    }
}
