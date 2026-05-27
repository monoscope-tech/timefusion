use std::{any::Any, collections::HashMap, fmt, sync::Arc};

use anyhow::Result;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::{
    arrow::{array::Array, record_batch::RecordBatch},
    catalog::Session,
    common::{Statistics, not_impl_err},
    datasource::{
        TableProvider, TableType,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result as DFResult},
    execution::{TaskContext, context::SessionContext},
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown, col, dml::InsertOp, lit},
    physical_expr::expressions::{CastExpr, Column as PhysicalColumn},
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, projection::ProjectionExec, union::UnionExec},
    scalar::ScalarValue,
};
use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use datafusion_functions_json;
use deltalake::{
    DeltaTable, DeltaTableBuilder, PartitionFilter, datafusion::parquet::file::properties::WriterProperties, kernel::transaction::CommitProperties,
    operations::create::CreateBuilder,
};
use futures::StreamExt;
use instrumented_object_store::instrument_object_store;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, field::Empty, info, instrument, warn};
use url::Url;

use crate::{
    config::{self, AppConfig},
    object_store_cache::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache},
    schema_loader::{create_insert_compatible_schema, get_default_schema, get_schema, is_variant_type},
    statistics::DeltaStatisticsExtractor,
};

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

/// Should `resolve_*_table` call `update_state()` on the cached snapshot?
/// Refresh when this process knows the snapshot is behind (last_written ahead
/// of current) *or* when this process hasn't written but something else (e.g.
/// the buffered_write_layer's background flusher) may have committed. The
/// `(Some(_), None) => false` shortcut once tempted us — it broke buffer→Delta
/// visibility — so the bias is toward refreshing more often, not less.
fn should_refresh_table(current_version: Option<u64>, last_written_version: Option<u64>) -> bool {
    match (current_version, last_written_version) {
        (Some(current), Some(last)) => current < last,
        // Either: process hasn't directly written but a background flusher may have.
        // Or: snapshot has no version yet but we know someone wrote one.
        // Both warrant a refresh.
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    }
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

/// Convert Utf8/Utf8View/LargeUtf8 columns to Variant binary StructArrays where the target
/// schema expects Variant. Called from `DataSink::write_all` so that INSERT statements (where
/// the table provider presents Variant cols as Utf8View for the SQL planner's type check) can
/// land their JSON-string values in the underlying Delta storage which expects Variant structs.
/// Normalize incoming Timestamp columns whose timezone is a numeric UTC
/// offset (`"+00:00"` — what psycopg / pgwire emit for timestamptz) to the
/// IANA name `"UTC"`. Delta-rs's Arrow→Delta schema converter rejects
/// `Timestamp(µs, "+00:00")` even though it's semantically identical to
/// `"UTC"`; without normalization every flush errors out and MemBuffer
/// fills until eviction warnings, with no data ever reaching Delta.
///
/// We only retag — the underlying micros-since-epoch buffer is unchanged.
/// Build a minimal `SessionState` for delta-rs `OptimizeBuilder` to use.
///
/// delta-rs's default `DeltaSessionConfig` turns `schema_force_view_types`
/// ON, which makes the optimize-internal Parquet reader cast our Variant
/// columns' Binary buffers to BinaryView at read time. The kernel's
/// `unshredded_variant()` schema then mismatches and the rewrite errors
/// out ("Expected ... Binary, got ... BinaryView"). Passing this session
/// via `.with_session_state(...)` overrides the default and keeps the
/// read schema as declared.
fn build_optimize_session_state() -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    let cfg = SessionConfig::new().set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    SessionStateBuilder::new().with_config(cfg).with_default_features().build()
}

/// Cast Variant struct columns (Struct{BinaryView,BinaryView}) to the
/// Binary-backed form delta-kernel's `unshredded_variant()` requires on
/// write. No-op for any column that's not a Variant struct or already in
/// Binary form. Called from `insert_records_batch` right before the
/// Delta write so MemBuffer can keep its natural BinaryView layout
/// (matches what parquet reads produce → no per-row read-side cast).
fn cast_variant_columns_to_binary(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::{array::StructArray, compute::cast};
    use datafusion::arrow::datatypes::{DataType, Field};
    let schema = batch.schema();
    let mut new_cols = batch.columns().to_vec();
    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut changed = false;
    for (i, field) in schema.fields().iter().enumerate() {
        if !is_variant_type(field.data_type()) {
            continue;
        }
        let DataType::Struct(struct_fields) = field.data_type() else { continue };
        // Only act if any inner field is BinaryView.
        let needs = struct_fields.iter().any(|f| matches!(f.data_type(), DataType::BinaryView));
        if !needs {
            continue;
        }
        let Some(struct_arr) = batch.columns()[i].as_any().downcast_ref::<StructArray>() else {
            continue;
        };
        let casted_cols: Vec<arrow::array::ArrayRef> = struct_arr
            .columns()
            .iter()
            .zip(struct_fields.iter())
            .map(|(arr, f)| -> DFResult<arrow::array::ArrayRef> {
                if matches!(f.data_type(), DataType::BinaryView) {
                    cast(arr, &DataType::Binary).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                } else {
                    Ok(arr.clone())
                }
            })
            .collect::<DFResult<_>>()?;
        let casted_fields: arrow::datatypes::Fields = struct_fields
            .iter()
            .map(|f| {
                if matches!(f.data_type(), DataType::BinaryView) {
                    Arc::new(Field::new(f.name(), DataType::Binary, f.is_nullable()))
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>()
            .into();
        new_cols[i] = Arc::new(StructArray::new(casted_fields.clone(), casted_cols, struct_arr.nulls().cloned()));
        new_fields[i] = Arc::new(Field::new(field.name(), DataType::Struct(casted_fields), field.is_nullable()).with_metadata(field.metadata().clone()));
        changed = true;
    }
    if !changed {
        return Ok(batch);
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(new_fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, new_cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn normalize_timestamp_tz(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::array::{TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    // Accept anything that semantically means UTC. Case-insensitive on alphabetic
    // forms ("UTC"/"Utc"/"utc"/"Z"/"GMT") and tolerant of the common offset
    // representations clients emit (+/- 00:00, 0000, 00). Delta-rs only
    // accepts the IANA "UTC" string, so we rewrite any of these to it.
    let is_utc_offset = |tz: &str| {
        matches!(tz, "+00:00" | "-00:00" | "+0000" | "-0000" | "+00" | "-00" | "00:00" | "0000")
            || tz.eq_ignore_ascii_case("UTC")
            || tz.eq_ignore_ascii_case("GMT")
            || tz.eq_ignore_ascii_case("Z")
    };
    let schema = batch.schema();
    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut new_cols = batch.columns().to_vec();
    let mut changed = false;
    for (i, field) in schema.fields().iter().enumerate() {
        if let DataType::Timestamp(unit, Some(tz)) = field.data_type()
            && is_utc_offset(tz.as_ref())
        {
            let col = &batch.columns()[i];
            // Downcasts are guarded by the outer `DataType::Timestamp(unit, ..)` match,
            // but Arrow's trait-object dispatch isn't an unsafe-level guarantee — return
            // an error rather than panic on the INSERT path if a future Arrow version
            // diverges.
            let bad = |w| DataFusionError::Execution(format!("timestamp downcast failed for field '{}' with width {w}", field.name()));
            let retagged: Arc<dyn arrow::array::Array> = match unit {
                TimeUnit::Microsecond => {
                    Arc::new(col.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| bad("Microsecond"))?.clone().with_timezone("UTC"))
                }
                TimeUnit::Millisecond => {
                    Arc::new(col.as_any().downcast_ref::<TimestampMillisecondArray>().ok_or_else(|| bad("Millisecond"))?.clone().with_timezone("UTC"))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(col.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| bad("Nanosecond"))?.clone().with_timezone("UTC"))
                }
                TimeUnit::Second => Arc::new(col.as_any().downcast_ref::<TimestampSecondArray>().ok_or_else(|| bad("Second"))?.clone().with_timezone("UTC")),
            };
            new_cols[i] = retagged;
            new_fields[i] =
                Arc::new(Field::new(field.name(), DataType::Timestamp(*unit, Some("UTC".into())), field.is_nullable()).with_metadata(field.metadata().clone()));
            changed = true;
        }
    }
    if !changed {
        return Ok(batch);
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(new_fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, new_cols).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn convert_variant_columns(batch: RecordBatch, target_schema: &SchemaRef) -> DFResult<RecordBatch> {
    use datafusion::arrow::{
        array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray, StructArray},
        compute::cast,
        datatypes::{DataType, Field},
    };
    use parquet_variant_compute::VariantArrayBuilder;
    use parquet_variant_json::JsonToVariant;

    let batch_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut new_fields: Vec<Arc<Field>> = batch_schema.fields().iter().cloned().collect();

    let utf8_to_variant = |iter: Box<dyn Iterator<Item = Option<&str>> + '_>| -> DFResult<StructArray> {
        let items: Vec<_> = iter.collect();
        let mut builder = VariantArrayBuilder::new(items.len());
        for (idx, item) in items.into_iter().enumerate() {
            match item {
                Some(s) => builder
                    .append_json(s)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid JSON at row {idx}: {e} (value: '{s}')")))?,
                None => builder.append_null(),
            }
        }
        // Cast VariantArrayBuilder's BinaryView output to Binary so the
        // batch matches `delta_kernel::unshredded_variant()` (which is what
        // our schema declares). Both Delta reads and MemBuffer end up as
        // Binary → no per-row casts on the read path.
        let arr: StructArray = builder.build().into();
        let metadata = cast(arr.column(0), &DataType::Binary).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let value = cast(arr.column(1), &DataType::Binary).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        let fields = vec![
            Arc::new(Field::new(crate::schema_loader::VARIANT_METADATA_FIELD, DataType::Binary, false)),
            Arc::new(Field::new(crate::schema_loader::VARIANT_VALUE_FIELD, DataType::Binary, false)),
        ];
        Ok(StructArray::new(fields.into(), vec![metadata, value], arr.nulls().cloned()))
    };

    for (idx, target_field) in target_schema.fields().iter().enumerate() {
        if !is_variant_type(target_field.data_type()) || idx >= columns.len() {
            continue;
        }
        let col = &columns[idx];
        // Downcasts are guarded by the `DataType::*` match arm above. If Arrow ever
        // returns a different concrete array for the same logical type, surface as
        // a DataFusionError instead of panicking on the INSERT path.
        let name = target_field.name();
        let bad_downcast = |ty: &str| DataFusionError::Execution(format!("{ty} downcast failed for column {name}"));
        let converted: Option<ArrayRef> = match col.data_type() {
            DataType::Utf8View => Some(Arc::new(utf8_to_variant(Box::new(
                col.as_any().downcast_ref::<StringViewArray>().ok_or_else(|| bad_downcast("Utf8View"))?.iter(),
            ))?) as ArrayRef),
            DataType::Utf8 => Some(Arc::new(utf8_to_variant(Box::new(
                col.as_any().downcast_ref::<StringArray>().ok_or_else(|| bad_downcast("Utf8"))?.iter(),
            ))?) as ArrayRef),
            DataType::LargeUtf8 => Some(Arc::new(utf8_to_variant(Box::new(
                col.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| bad_downcast("LargeUtf8"))?.iter(),
            ))?) as ArrayRef),
            _ => None, // already Variant struct
        };
        if let Some(arr) = converted {
            columns[idx] = arr;
            new_fields[idx] = target_field.clone();
        }
    }

    let new_schema = Arc::new(arrow_schema::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, columns).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

// Fallback ZSTD level when a configured/tier level is rejected as out-of-range.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;
// Parquet footer key-value metadata key recording the ZSTD level used to
// write the file. Read by `recompress_partition` to skip files already
// at-or-above the target tier without rewriting.
const COMPRESSION_TIER_KEY: &str = "timefusion.compression_tier";

#[derive(Clone, Serialize, Deserialize, sqlx::FromRow)]
struct StorageConfig {
    project_id:           String,
    table_name:           String,
    s3_bucket:            String,
    s3_prefix:            String,
    s3_region:            String,
    /// Skipped on serialize so credentials never leak through serde-based dumps
    /// (debug endpoints, metrics serialization, etc.). sqlx::FromRow bypasses
    /// serde so DB-row loading is unaffected.
    #[serde(serialize_with = "redact_str")]
    s3_access_key_id:     String,
    #[serde(serialize_with = "redact_str")]
    s3_secret_access_key: String,
    s3_endpoint:          Option<String>,
}

fn redact_str<S: serde::Serializer>(_: &str, ser: S) -> std::result::Result<S::Ok, S::Error> {
    ser.serialize_str("[redacted]")
}

// Manual Debug — never let the AWS credentials land in a {:?} log line.
// Derived Debug would, derived Serialize already does (only used for the
// PG-backed config table, but worth noting as a future audit point).
impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageConfig")
            .field("project_id", &self.project_id)
            .field("table_name", &self.table_name)
            .field("s3_bucket", &self.s3_bucket)
            .field("s3_prefix", &self.s3_prefix)
            .field("s3_region", &self.s3_region)
            .field("s3_access_key_id", &"[redacted]")
            .field("s3_secret_access_key", &"[redacted]")
            .field("s3_endpoint", &self.s3_endpoint)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    config:                          Arc<AppConfig>,
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables:                  UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables:           CustomProjectTables,
    batch_queue:                     Option<Arc<crate::batch_queue::BatchQueue>>,
    maintenance_shutdown:            Arc<CancellationToken>,
    config_pool:                     Option<PgPool>,
    storage_configs:                 Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    /// Monotonic deadline (nanos since process start) for when the next
    /// storage-configs refresh from the config DB is allowed. Capped at 30s
    /// so a hot SQL path doesn't hit PG on every statement.
    storage_configs_next_refresh_ns: Arc<std::sync::atomic::AtomicU64>,
    default_s3_bucket:               Option<String>,
    default_s3_prefix:               Option<String>,
    default_s3_endpoint:             Option<String>,
    object_store_cache:              Option<Arc<SharedFoyerCache>>,
    statistics_extractor:            Arc<DeltaStatisticsExtractor>,
    last_written_versions:           Arc<RwLock<HashMap<(String, String), u64>>>,
    buffered_layer:                  Option<Arc<crate::buffered_write_layer::BufferedWriteLayer>>,
    tantivy_search:                  Option<Arc<crate::tantivy_index::search::TantivySearchService>>,
    tantivy_indexer:                 Option<Arc<crate::tantivy_index::service::TantivyIndexService>>,
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

        // debug! (not info!) because this is called on every insert path —
        // info-level logging here would flood production logs.
        let safe_options: HashMap<_, _> = storage_options.iter().filter(|(k, _)| !k.contains("secret") && !k.contains("password")).collect();
        debug!("Storage options configured: {:?}", safe_options);
        storage_options
    }

    /// Creates writer properties for a Delta write at a given compression tier.
    ///
    /// Tiered strategy: hot writes use level 3 (fast ingest);
    /// `recompress_partition` rewrites older partitions at 9/15/19 to
    /// maximize storage savings on
    /// cold data. The chosen level is embedded in Parquet footer key-value
    /// metadata (`timefusion.compression_tier`) so re-sweeps can skip files
    /// already at the target tier.
    ///
    /// Encoding strategy per column:
    /// - Timestamps/Date32, ints: `DELTA_BINARY_PACKED` (dict off for timestamps).
    /// - Sorted-key Utf8 columns: `DELTA_BYTE_ARRAY` (delta-encoded, dict off) —
    ///   excellent ratios on sorted ids/service names; harmless when only mostly
    ///   sorted (still better than raw PLAIN).
    /// - Other Utf8: default (dict on, auto-falls back to PLAIN at 8MB).
    /// - Per-field `dictionary: false` opt-out for high-entropy free-text.
    /// - Per-field `bloom_filter: true` opt-in for point-lookup columns
    ///   (ids/trace_ids/span_ids); NDV scaled to row-group size.
    fn create_writer_properties(&self, schema: &crate::schema_loader::TableSchema, zstd_level: i32) -> WriterProperties {
        build_writer_properties(&self.config.parquet, schema, zstd_level)
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
                    // Exponential backoff with jitter, capped at ~6.4s.
                    // `100 << retries` doubles each attempt; clamp to 6 shifts
                    // so a long retry chain doesn't sleep for minutes. Jitter
                    // is `± delay/4` so concurrent retriers don't thunder.
                    let base = 100u64 << retries.min(6);
                    let jitter = fastrand::u64(0..=base / 2);
                    let delay = base / 2 * 3 + jitter; // base*0.75 .. base*1.25
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                }
            }
        }
    }

    /// One-time DDL to ensure the config schema exists. Run during Database
    /// construction, not on every config reload — DDL in a hot read path is
    /// surprising and serializes concurrent callers.
    async fn ensure_storage_configs_schema(pool: &PgPool) -> Result<()> {
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
        Ok(())
    }

    /// Load storage configurations from PostgreSQL.
    async fn load_storage_configs(pool: &PgPool) -> Result<HashMap<(String, String), StorageConfig>> {
        let configs: Vec<StorageConfig> = sqlx::query_as(
            "SELECT project_id, table_name, s3_bucket, s3_prefix, s3_region, 
             s3_access_key_id, s3_secret_access_key, s3_endpoint 
             FROM timefusion_projects WHERE is_active = true",
        )
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for config in configs {
            debug!("Loaded config: {}/{}", config.project_id, config.table_name);
            map.insert((config.project_id.clone(), config.table_name.clone()), config);
        }
        info!("Loaded {} storage configs from timefusion_projects", map.len());
        Ok(map)
    }

    async fn initialize_cache_with_retry(cfg: &AppConfig) -> Option<Arc<SharedFoyerCache>> {
        // Check if cache is disabled
        if cfg.cache.is_disabled() {
            info!("Foyer cache is disabled via TIMEFUSION_FOYER_DISABLED");
            return None;
        }

        let foyer_config = FoyerCacheConfig::from_app_config(cfg);
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
                    if let Err(e) = Self::ensure_storage_configs_schema(&pool).await {
                        warn!("Could not ensure timefusion_projects schema (continuing — table may already exist): {}", e);
                    }
                    let configs = Self::load_storage_configs(&pool).await.unwrap_or_default();
                    (Some(pool), configs)
                }
                Err(e) => {
                    warn!(
                        "Could not connect to config database, falling back to default mode (custom project routing disabled): {}",
                        e
                    );
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
            storage_configs_next_refresh_ns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            default_s3_bucket: default_s3_bucket.clone(),
            default_s3_prefix: Some(default_s3_prefix.clone()),
            default_s3_endpoint,
            object_store_cache,
            statistics_extractor,
            last_written_versions: Arc::new(RwLock::new(HashMap::new())),
            buffered_layer: None,
            tantivy_search: None,
            tantivy_indexer: None,
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

    /// Attach the tantivy search service used by the scan-side prefilter.
    pub fn with_tantivy_search(mut self, svc: Arc<crate::tantivy_index::search::TantivySearchService>) -> Self {
        self.tantivy_search = Some(svc);
        self
    }

    pub fn tantivy_search(&self) -> Option<&Arc<crate::tantivy_index::search::TantivySearchService>> {
        self.tantivy_search.as_ref()
    }

    /// Attach the write-side tantivy service. Used by the compaction-GC hook
    /// in `optimize_table` to clean up stale sidecar indexes after files are
    /// rewritten away.
    pub fn with_tantivy_indexer(mut self, svc: Arc<crate::tantivy_index::service::TantivyIndexService>) -> Self {
        self.tantivy_indexer = Some(svc);
        self
    }

    pub fn tantivy_indexer(&self) -> Option<&Arc<crate::tantivy_index::service::TantivyIndexService>> {
        self.tantivy_indexer.as_ref()
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

        // Recompress job - daily tier upgrade for cool (7-30d) and cold (30d+).
        // Skips partitions whose probe file already advertises the target tier
        // via Parquet footer metadata, so re-runs are cheap on stable data.
        let recompress_schedule = self.config.maintenance.timefusion_recompress_schedule.clone();
        let cool_cutoff = self.config.parquet.timefusion_cool_cutoff_days;
        let cold_cutoff = self.config.parquet.timefusion_cold_cutoff_days;
        let zstd_cool = self.config.parquet.timefusion_zstd_level_cool;
        let zstd_cold = self.config.parquet.timefusion_zstd_level_cold;

        if !recompress_schedule.is_empty() {
            info!(
                "Recompress job scheduled: {} (warm→cool@{}d zstd={}, cool→cold@{}d zstd={})",
                recompress_schedule, cool_cutoff, zstd_cool, cold_cutoff, zstd_cold
            );
            // Cold sweep upper bound — partitions older than this fall under
            // vacuum; we don't need to keep extending the window indefinitely.
            let cold_upper = (self.config.maintenance.timefusion_vacuum_retention_hours / 24).max(cold_cutoff + 60);

            let recompress_job = Job::new_async(recompress_schedule.as_str(), {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled tier recompression");
                        // Flatten unified + custom tables into one (name, table) list.
                        let mut targets: Vec<(String, Arc<RwLock<DeltaTable>>)> =
                            db.unified_tables.read().await.iter().map(|(n, t)| (n.clone(), t.clone())).collect();
                        targets.extend(db.custom_project_tables.read().await.iter().map(|((_, n), t)| (n.clone(), t.clone())));
                        // Cool tier first, then cold — order matters only at
                        // the cutoff boundary where files may need two hops.
                        for (name, table) in &targets {
                            if let Err(e) = db.recompress_tier_window(table, name, cool_cutoff, cold_cutoff, zstd_cool).await {
                                error!("Recompress (cool tier) failed for '{}': {}", name, e);
                            }
                            if let Err(e) = db.recompress_tier_window(table, name, cold_cutoff, cold_upper, zstd_cold).await {
                                error!("Recompress (cold tier) failed for '{}': {}", name, e);
                            }
                        }
                    })
                }
            })?;
            scheduler.add(recompress_job).await?;
        } else {
            info!("Recompress job scheduling skipped - empty schedule");
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
        use std::sync::Arc;

        use datafusion::{
            config::ConfigOptions,
            execution::{SessionStateBuilder, context::SessionContext, runtime_env::RuntimeEnvBuilder},
        };
        use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};

        use crate::dml::DmlQueryPlanner;

        let mut options = ConfigOptions::new();
        let _ = options.set("datafusion.catalog.information_schema", "true");

        // Must be false: delta_kernel's unshredded_variant() schema uses Binary (not BinaryView).
        // Forcing view types causes UPDATE/DELETE rewrites to fail schema validation against variant columns.
        let _ = options.set("datafusion.execution.parquet.schema_force_view_types", "false");
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

        // Enable general statistics collection for query optimization.
        // (DataFusion default is `true` — set explicitly so a future default flip
        // doesn't silently regress query plans.)
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
            // Tantivy predicate rewriter runs BEFORE TypeCoercion so the
            // injected `text_match(col, lit)` calls get coerced like any
            // other UDF args (Utf8 vs Utf8View etc).
            Arc::new(crate::optimizers::TantivyPredicateRewriter),
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

        // Register the introspection table. `SELECT * FROM timefusion_stats`
        // returns a flat (component, key, value) snapshot of MemBuffer / WAL /
        // BufferedWriteLayer counters — see src/stats_table.rs.
        ctx.register_table(
            "timefusion_stats",
            Arc::new(crate::stats_table::StatsTableProvider::new(self.buffered_layer.clone())),
        )?;

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
        use datafusion::arrow::{
            array::StringViewArray,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

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
        use datafusion::{
            arrow::{
                array::{StringViewArray, StringViewBuilder},
                datatypes::DataType,
            },
            logical_expr::{ColumnarValue, ScalarFunctionImplementation, Volatility, create_udf},
        };

        let set_config_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            let ColumnarValue::Array(array) = &args[1] else {
                return Err(DataFusionError::Execution("set_config: second argument must be an array".into()));
            };
            let param_value_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| DataFusionError::Execution(format!("set_config: second argument must be StringViewArray, got {:?}", array.data_type())))?;

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

        // Lazy reload of storage configs from PG, but at most once per
        // STORAGE_CONFIGS_TTL_NS. Without this, every SQL statement that hits
        // resolve_table issues a fresh PG roundtrip — death by a thousand cuts
        // under load.
        if let Some(ref pool) = self.config_pool {
            const STORAGE_CONFIGS_TTL_NS: u64 = 30 * 1_000_000_000; // 30s
            use std::{sync::atomic::Ordering, time::Instant};
            // Lazily anchor the clock so we use a monotonic delta from process start.
            static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
            let start = START.get_or_init(Instant::now);
            let now_ns = start.elapsed().as_nanos() as u64;
            let next = self.storage_configs_next_refresh_ns.load(Ordering::Relaxed);
            if now_ns >= next
                && self
                    .storage_configs_next_refresh_ns
                    .compare_exchange(next, now_ns + STORAGE_CONFIGS_TTL_NS, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                && let Ok(new_configs) = Self::load_storage_configs(pool).await
            {
                let mut configs = self.storage_configs.write().await;
                *configs = new_configs;
            }
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
                let should_update = should_refresh_table(current_version, last_written_version);

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
                let should_update = should_refresh_table(current_version, last_written_version);

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

        let prefix = self
            .default_s3_prefix
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No default S3 prefix configured for unified table '{}'", table_name))?;
        let endpoint = self
            .default_s3_endpoint
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No default S3 endpoint configured for unified table '{}'", table_name))?;
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
                    // Default of 32 leaf columns isn't enough for our wide schema (90+ fields);
                    // -1 = index all columns. Needed so kernel data-skipping can evaluate
                    // predicates on columns beyond the first 32 without "No such field" errors.
                    config.insert("delta.dataSkippingNumIndexedCols".to_string(), Some("-1".to_string()));

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
    /// Return the live parquet file URIs of a Delta table after refreshing
    /// its state. Returns empty if the table doesn't exist yet (pre-create).
    /// Used by the buffered-layer's Delta callback to surface "files added
    /// by this commit" to the sidecar tantivy indexer.
    pub async fn list_file_uris(&self, project_id: &str, table_name: &str) -> Result<Vec<String>> {
        let table_ref = match self.resolve_table(project_id, table_name).await {
            Ok(r) => r,
            Err(_) => return Ok(Vec::new()),
        };
        let mut table = table_ref.write().await;
        let _ = table.update_state().await;
        let uris: Vec<String> = table.get_file_uris()?.collect();
        Ok(uris)
    }

    pub async fn get_or_create_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Route to appropriate table based on whether project has custom storage
        if self.has_custom_storage(project_id, table_name).await {
            self.get_or_create_custom_table(project_id, table_name).await
        } else {
            self.get_or_create_unified_table(table_name).await
        }
    }

    /// Create an object store for the given URI and storage options
    pub async fn create_object_store(&self, storage_uri: &str, storage_options: &HashMap<String, String>) -> Result<Arc<dyn object_store::ObjectStore>> {
        use std::time::Duration;

        use object_store::{BackoffConfig, ClientOptions, RetryConfig, aws::AmazonS3Builder};

        // Parse the S3 URI to extract bucket and prefix
        let url = Url::parse(storage_uri)?;
        let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("Invalid S3 URI: missing bucket"))?;

        // Configure retry with exponential backoff for transient network errors
        let retry_config = RetryConfig {
            max_retries:   5,
            retry_timeout: Duration::from_secs(180),
            backoff:       BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff:  Duration::from_secs(15),
                base:         2.0,
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
        // Normalize timezone-as-offset (`+00:00`) timestamp columns to the
        // IANA `"UTC"` form. Delta-rs Arrow→Delta schema conversion only
        // accepts `"UTC"`; without this normalisation the flush callback
        // path (which feeds MemBuffer batches straight into Delta) errors
        // out and data piles up in MemBuffer.
        let batches: Vec<RecordBatch> = batches.into_iter().map(normalize_timestamp_tz).collect::<DFResult<Vec<_>>>()?;

        // Extract project_id from first batch if not provided. If neither the
        // caller nor the data carries one, log loudly and bucket under
        // "default" — silently misrouting writes is the worst outcome, but
        // returning an error would break callers that already rely on the
        // legacy fallback.
        let project_id = if project_id.is_empty() && !batches.is_empty() {
            extract_project_id(&batches[0]).unwrap_or_else(|| {
                warn!("insert_records_batch: empty project_id and batch has no project_id column → bucketing under 'default'");
                "default".to_string()
            })
        } else if project_id.is_empty() {
            warn!("insert_records_batch: empty project_id and no batches → bucketing under 'default'");
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

        // Delta-kernel's `unshredded_variant()` expects Struct{Binary,Binary}
        // on write, but our MemBuffer carries Struct{BinaryView,BinaryView}
        // (matches what the parquet reader natively produces — no per-row
        // casts on read). Cast just-before-write so the Delta commit
        // accepts the schema.
        let batches: Vec<RecordBatch> = batches.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;

        // Get or create the table
        let table_ref = self.get_or_create_table(&project_id, &table_name).await?;

        // Get the appropriate schema for this table
        let schema = get_schema(&table_name).unwrap_or_else(get_default_schema);

        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level);

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
        // Full Z-order optimize runs every 30 min over a 48h window — promote
        // these rewrites to the "warm" tier so day-old data lands smaller on
        // disk without slowing the hot flush path.
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm);

        // Same trade-off as optimize_table_light: best-effort, don't pause
        // flushes (see comment there). Z-order full optimize is daily-ish,
        // so an occasional OCC failure is fine.
        let optimize_result = table_clone
            .optimize()
            .with_filters(&partition_filters)
            .with_type(if schema.z_order_columns.is_empty() {
                deltalake::operations::optimize::OptimizeType::Compact
            } else {
                deltalake::operations::optimize::OptimizeType::ZOrder(schema.z_order_columns.clone())
            })
            .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
            .with_writer_properties(writer_properties)
            .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
            // Avoid the BinaryView read for Variant columns (same issue as
            // optimize_table_light); delta-rs's internal session defaults to
            // schema_force_view_types=true.
            .with_session_state(Arc::new(build_optimize_session_state()))
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
                // Capture live file URIs from the new table *before* taking
                // the write lock to swap it in — used by the tantivy GC hook
                // below to drop indexes whose covered files no longer exist.
                let live_uris: Vec<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                let mut table = table_ref.write().await;
                *table = new_table;
                drop(table);
                // Tantivy compaction GC — drop sidecar indexes for files that
                // were rewritten away. Best-effort: errors are logged.
                if let Some(svc) = self.tantivy_indexer().cloned() {
                    let svc_table = table_name.to_string();
                    // Per-project: collect all (project_id, ...) values from
                    // manifests in this table prefix. Today only the unified
                    // "default" path is exercised in practice; iterate over
                    // known custom projects too.
                    let mut project_ids: Vec<String> =
                        self.custom_project_tables.read().await.keys().filter(|(_, t)| t == table_name).map(|(p, _)| p.clone()).collect();
                    project_ids.push("default".to_string());
                    for pid in project_ids {
                        match svc.gc_after_compaction(&svc_table, &pid, &live_uris).await {
                            Ok(report) if report.entries_removed > 0 => {
                                info!(
                                    "tantivy gc: project={} table={} removed={} kept={} blobs_deleted={}",
                                    pid, svc_table, report.entries_removed, report.kept, report.blobs_deleted
                                );
                            }
                            Ok(_) => {}
                            Err(e) => warn!("tantivy gc failed for project={} table={}: {}", pid, svc_table, e),
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                error!("Optimization operation failed: {}", e);
                Err(anyhow::anyhow!("Table optimization failed: {}", e))
            }
        }
    }

    /// Rewrites a date partition at a higher ZSTD level using Z-order (or
    /// Compact if no z_order_columns). Skips partitions whose probe file
    /// already advertises a tier `>= target_level` via Parquet footer KV
    /// metadata (`timefusion.compression_tier`).
    ///
    /// Probes only one file per partition. Safe in steady state: each
    /// successful recompress rewrites every file in the partition at the
    /// same level, so all files share a tier. A partial-rewrite failure
    /// would leave mixed tiers — the next sweep then sees the probe's tier
    /// and may skip, but the partition will be re-evaluated the day after.
    /// Acceptable for an idempotent daily job.
    pub async fn recompress_partition(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_level: i32) -> Result<()> {
        use deltalake::datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
        use object_store::{ObjectStoreExt, path::Path as OsPath};

        let date_str = date.to_string();
        let date_marker = format!("date={}", date_str);

        let (uris, log_store, table_uri) = {
            let table = table_ref.read().await;
            let uris: Vec<String> = table.get_file_uris()?.filter(|u| u.contains(&date_marker)).collect();
            (uris, table.log_store(), table.table_url().to_string())
        };
        if uris.is_empty() {
            debug!("recompress: no files in partition date={} for table={}", date_str, table_name);
            return Ok(());
        }

        // Probe one file's footer KV metadata. URIs returned by delta-rs are
        // absolute (s3://bucket/...); the table's object_store is rooted at
        // table_uri, so the relative key is the URI with that prefix stripped.
        // `table_url()` may include a `?endpoint=...` query string (non-AWS
        // backends like MinIO) which `get_file_uris()` does not — strip it
        // before matching.
        let probe_uri = &uris[0];
        let table_prefix = table_uri.split('?').next().unwrap_or(&table_uri).trim_end_matches('/');
        let probe_tier = match probe_uri.strip_prefix(table_prefix).and_then(|s| s.strip_prefix('/').or(Some(s))) {
            Some(rel) => {
                let object_store = log_store.object_store(None);
                let path = OsPath::from(rel);
                // `head()` returns `meta.location` relative to the bucket,
                // but `ParquetObjectReader` consumes object-store-relative
                // paths and would double-prefix. Pass our original `path`.
                match object_store.head(&path).await {
                    Ok(meta) => {
                        let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone()).with_file_size(meta.size);
                        reader.get_metadata(None).await.ok().and_then(|pq| {
                            pq.file_metadata().key_value_metadata().and_then(|kvs| {
                                kvs.iter()
                                    .find(|kv| kv.key == COMPRESSION_TIER_KEY)
                                    .and_then(|kv| kv.value.as_ref())
                                    .and_then(|v| v.parse::<i32>().ok())
                            })
                        })
                    }
                    Err(e) => {
                        warn!("recompress probe: head failed for {}: {}; rewriting anyway", probe_uri, e);
                        None
                    }
                }
            }
            None => {
                warn!(
                    "recompress probe: could not relativize {} against {}; rewriting anyway",
                    probe_uri, table_prefix
                );
                None
            }
        };

        // If probe failed or tier is unknown, fall through to rewrite — safer
        // than skipping a partition that may still be at hot tier.
        if let Some(t) = probe_tier
            && t >= target_level
        {
            debug!("recompress: skip date={} table={} (already at tier {})", date_str, table_name, t);
            return Ok(());
        }

        info!(
            "recompress: rewriting date={} table={} at zstd={} ({} files)",
            date_str,
            table_name,
            target_level,
            uris.len()
        );

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let writer_properties = self.create_writer_properties(schema, target_level);
        let partition_filters = vec![PartitionFilter::try_from(("date", "=", date_str.as_str()))?];
        let target_size = self.config.parquet.timefusion_optimize_target_size;

        let table_clone = table_ref.read().await.clone();
        let optimize_result = table_clone
            .optimize()
            .with_filters(&partition_filters)
            // Z-order rewrites every file in the partition (Compact only
            // touches small files), which is exactly what we need to lift
            // the partition's tier.
            .with_type(if schema.z_order_columns.is_empty() {
                deltalake::operations::optimize::OptimizeType::Compact
            } else {
                deltalake::operations::optimize::OptimizeType::ZOrder(schema.z_order_columns.clone())
            })
            .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
            .with_writer_properties(writer_properties)
            .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
            .with_session_state(Arc::new(build_optimize_session_state()))
            .await;

        match optimize_result {
            Ok((new_table, metrics)) => {
                info!(
                    "recompress: date={} table={} removed={} added={} considered={}",
                    date_str, table_name, metrics.num_files_removed, metrics.num_files_added, metrics.total_considered_files
                );
                *table_ref.write().await = new_table;
                Ok(())
            }
            Err(e) => {
                error!("recompress failed for date={} table={}: {}", date_str, table_name, e);
                Err(anyhow::anyhow!("recompress failed: {}", e))
            }
        }
    }

    /// Sweep partitions in [age_min_days, age_max_days) and recompress any
    /// whose probe tier is below `target_level`. Iterates day-by-day; each
    /// day's optimize is its own Delta commit so a mid-sweep failure leaves
    /// completed days at the new tier.
    pub async fn recompress_tier_window(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, age_min_days: u64, age_max_days: u64, target_level: i32,
    ) -> Result<()> {
        let today = Utc::now().date_naive();
        for days_ago in age_min_days..age_max_days {
            let date = today - chrono::Duration::days(days_ago as i64);
            if let Err(e) = self.recompress_partition(table_ref, table_name, date, target_level).await {
                warn!("recompress_tier_window: skipping date={} after error: {}", date, e);
            }
        }
        Ok(())
    }

    pub async fn optimize_table_light(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();
        let today = Utc::now().date_naive();
        let partition_filters = vec![PartitionFilter::try_from(("date", "=", today.to_string().as_str()))?];
        let target_size = self.config.maintenance.timefusion_light_optimize_target_size;
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level);

        // Best-effort optimize: retry on OCC conflict but DO NOT hold the
        // flush lock. Earlier we wrapped this in `with_flush_paused` to
        // ensure optimize won the race against flush commits, but the
        // retry+OCC time is 4–10s and flushes accumulate buckets during
        // that window — at 25h-bench scale we saw 46+ stuck MemBuffer
        // buckets and a 10× drop in ingest throughput. Better to let
        // optimize fail loudly during heavy ingest; the next scheduler
        // tick (5 min later) usually catches a quiet enough window.
        self.optimize_table_light_inner(table_ref, today, &partition_filters, target_size, &writer_properties, start_time).await
    }

    /// Inner optimize loop. Caller is expected to hold the flush lock when
    /// a `BufferedWriteLayer` is active; the retry loop here remains as a
    /// safety net against bursts from `flush_all_now` or shutdown flushes.
    async fn optimize_table_light_inner(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, today: chrono::NaiveDate, partition_filters: &[PartitionFilter], target_size: i64,
        writer_properties: &WriterProperties, start_time: std::time::Instant,
    ) -> Result<()> {
        const MAX_RETRIES: usize = 4;
        let mut last_err: Option<deltalake::DeltaTableError> = None;
        for attempt in 0..MAX_RETRIES {
            let table_clone = {
                let table = table_ref.read().await;
                table.clone()
            };
            if attempt == 0 {
                info!("Light optimizing files from date: {}", today);
            } else {
                debug!("Light optimize retry {}/{} after OCC conflict", attempt + 1, MAX_RETRIES);
            }
            let optimize_result = table_clone
                .optimize()
                .with_filters(partition_filters)
                .with_type(deltalake::operations::optimize::OptimizeType::Compact)
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                .with_writer_properties(writer_properties.clone())
                .with_min_commit_interval(tokio::time::Duration::from_secs(30))
                // Variant columns are stored as Struct{Binary, Binary} on disk; if
                // the optimize-internal Parquet read uses `schema_force_view_types=true`
                // (delta-rs's default), it returns BinaryView and the rewrite blows up
                // mid-scan with "Expected ... Binary, got ... BinaryView".
                .with_session_state(Arc::new(build_optimize_session_state()))
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
                        "Light optimization completed in {:?} (attempt {}): {} files removed, {} files added",
                        duration,
                        attempt + 1,
                        metrics.num_files_removed,
                        metrics.num_files_added
                    );
                    let mut table = table_ref.write().await;
                    *table = new_table;
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_conflict = msg.contains("concurrent transaction") || msg.contains("Commit failed");
                    // "Found unmasked nulls for non-nullable StructArray" surfaces
                    // when delta-rs is mid-rewrite and the in-flight Add log lines
                    // for partition struct values aren't fully populated yet.
                    // It usually clears on a fresh re-scan, so treat as transient.
                    let is_transient_schema = msg.contains("Found unmasked nulls");
                    if (is_conflict || is_transient_schema) && attempt + 1 < MAX_RETRIES {
                        // Quick backoff scaled so we straddle multiple flush
                        // ticks (~2s each) — picks 150, 300, 600 ms.
                        let backoff_ms = 150u64 << attempt;
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        last_err = Some(e);
                        continue;
                    }
                    error!("Light optimization operation failed (attempt {}): {}", attempt + 1, e);
                    return Err(anyhow::anyhow!("Light table optimization failed: {}", e));
                }
            }
        }
        let err = last_err.map(|e| e.to_string()).unwrap_or_else(|| "exhausted retries".into());
        warn!("Light optimization gave up after {} OCC conflicts; will retry next tick: {}", MAX_RETRIES, err);
        Ok(())
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

/// Pure builder for parquet `WriterProperties` at a given compression tier.
/// Lives outside `impl Database` so unit tests can exercise tier/encoding/bloom
/// decisions without instantiating a Database (which needs S3/MinIO).
fn build_writer_properties(parquet_cfg: &crate::config::ParquetConfig, schema: &crate::schema_loader::TableSchema, zstd_level: i32) -> WriterProperties {
    use deltalake::datafusion::parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::{metadata::KeyValue, properties::EnabledStatistics},
        schema::types::ColumnPath,
    };

    let page_row_count_limit = parquet_cfg.timefusion_page_row_count_limit;
    let max_row_group_size = parquet_cfg.timefusion_max_row_group_size;
    let bloom_globally_disabled = parquet_cfg.timefusion_bloom_filter_disabled;

    // Per-column bloom NDV sized to a typical row-group row count.
    // 1M rows ≈ parquet-rs's default `set_max_row_group_size`; gives an
    // ~1.7MB bloom per column at fpp=0.01, vs ~150MB if we naively scaled
    // by the byte-sized `max_row_group_size`. The legacy global 100k
    // produced near-1.0 false-positive rates at scale.
    const BLOOM_NDV: u64 = 1_000_000;

    let sorting_columns_pq = schema.sorting_columns();
    let sort_key_names: std::collections::HashSet<&str> = schema.sorting_columns.iter().map(|c| c.name.as_str()).collect();

    // Note: do NOT call `set_bloom_filter_fpp` at the global level — parquet-rs
    // treats any global bloom setter (other than `set_bloom_filter_enabled`)
    // as implicit enable, which then uses the default NDV (~1M) and triggers
    // massive bloom buffer allocations on every column. We set fpp per-column
    // only, for the columns we actually want blooms on.
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(zstd_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap()),
        ))
        .set_max_row_group_row_count(Some(max_row_group_size))
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(8388608)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_bloom_filter_enabled(false)
        .set_data_page_row_count_limit(page_row_count_limit)
        .set_sorting_columns(if sorting_columns_pq.is_empty() { None } else { Some(sorting_columns_pq) })
        .set_key_value_metadata(Some(vec![KeyValue::new(COMPRESSION_TIER_KEY.to_string(), zstd_level.to_string())]));

    for field in &schema.fields {
        let dt = field.data_type.as_str();
        let col = ColumnPath::from(field.name.as_str());
        let is_sort_key = sort_key_names.contains(field.name.as_str());

        if dt.starts_with("Timestamp") || dt == "Date32" {
            builder = builder
                .set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED)
                .set_column_dictionary_enabled(col.clone(), false);
        } else if matches!(dt, "Int32" | "Int64" | "UInt32" | "UInt64") {
            builder = builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED);
        } else if dt == "Utf8" && is_sort_key {
            builder = builder.set_column_encoding(col.clone(), Encoding::DELTA_BYTE_ARRAY).set_column_dictionary_enabled(col.clone(), false);
        }

        // Explicit per-column dict opt-out (overrides defaults above only
        // when set to Some(false); Some(true)/None leaves defaults intact).
        if field.dictionary == Some(false) {
            builder = builder.set_column_dictionary_enabled(col.clone(), false);
        }

        if field.bloom_filter && !bloom_globally_disabled {
            builder = builder
                .set_column_bloom_filter_enabled(col.clone(), true)
                .set_column_bloom_filter_ndv(col.clone(), BLOOM_NDV)
                .set_column_bloom_filter_fpp(col, 0.01);
        }
    }

    builder.build()
}

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database:        Arc<Database>,
    schema:          SchemaRef,
    _batch_queue:    Option<Arc<crate::batch_queue::BatchQueue>>,
    table_name:      String,
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
        filters.iter().find_map(crate::optimizers::extract_project_id_from_expr)
    }

    fn schema(&self) -> SchemaRef {
        // Present Variant cols as Utf8View at the table-provider boundary so the SQL planner's
        // INSERT VALUES type check accepts JSON string literals (arrow has no Utf8→Struct cast).
        // `write_all` converts these Utf8 columns back to Variant structs before the Delta write.
        create_insert_compatible_schema(&self.schema)
    }

    /// Real (Variant-typed) schema for internal use.
    pub fn real_schema(&self) -> SchemaRef {
        self.schema.clone()
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

    /// Checks if a column supports *exact* pushdown — meaning the table
    /// provider promises to fully apply the filter so DataFusion can drop
    /// the FilterExec on top. Only true partition columns qualify:
    /// Delta's partition pruning is genuinely exact, and partition values
    /// are also compared exactly inside MemBuffer.
    ///
    /// Previously this list included `timestamp`, `id`, `level`, etc. on
    /// the assumption that MemBuffer's row-level filter (best-effort) plus
    /// Delta's row-group statistics would catch them. But MemBuffer's
    /// physical-expr compilation silently falls back to "no filter" if the
    /// expression can't be lowered for any reason (type coercion, Utf8View
    /// vs Utf8, etc.) — and with Exact pushdown, FilterExec is gone, so
    /// rows leak through unfiltered. Bench harness caught this as
    /// `timestamp >= '02:55' AND timestamp < '03:00'` returning the entire
    /// 10-minute bucket.
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(column_name, "project_id" | "date")
    }

    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::optimizers::time_range_partition_pruner;

        // Resolve the schema-declared time column for this table; falls back to
        // "timestamp" when the schema isn't registered (custom/dynamic tables).
        let time_column = crate::schema_loader::get_schema(&self.table_name)
            .map(|s| s.time_column_name().to_string())
            .unwrap_or_else(|| "timestamp".to_string());

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
                if let Some(date_filter) = time_range_partition_pruner::timestamp_to_date_filter(filter, &time_column) {
                    optimized_filters.push(date_filter);
                    debug!("Added date partition filter for {} on column {}", self.table_name, time_column);
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

        // Build the delta-rs table provider with our session so its scan
        // inherits `schema_force_view_types=false` (set in
        // `create_session_context`). delta-rs's default is `true` (BinaryView),
        // which mismatches our Binary-typed MemBuffer at the union and
        // panics in physical planning. The session is a SessionState in
        // practice; clone the concrete type so we can hand an
        // `Arc<dyn Session + 'static>` to `with_session`.
        let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
        let provider = if let Some(ss) = session_state {
            table.table_provider().with_session(Arc::new(ss)).await
        } else {
            table.table_provider().await
        }
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

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

        // Variant columns are an Arrow ExtensionType whose inner storage may
        // be either Struct{Binary,Binary} or Struct{BinaryView,BinaryView}
        // depending on which session built the scan plan. The
        // parquet-variant-compute kernel and our UDFs accept both, so a
        // per-row CAST(BinaryView→Binary) here is pure overhead — it was
        // costing ~4× on `SELECT payload`. Skip the coercion for any field
        // whose target type is Variant; let the kernel handle the layout.
        let differs = |plan_field: &arrow_schema::Field, target_field: &arrow_schema::Field| -> bool {
            if plan_field.data_type() == target_field.data_type() {
                return false;
            }
            !crate::schema_loader::is_variant_type(target_field.data_type())
        };

        let needs_coercion = plan_schema
            .fields()
            .iter()
            .zip(target_schema.fields())
            .any(|(plan_field, target_field)| differs(plan_field, target_field));

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
                let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = if differs(plan_field, target_field) {
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
        let target_schema = self.real_schema();
        // Collect and group batches by project_id, converting Utf8/Utf8View columns into
        // Variant structs where the target schema expects Variant (INSERT path: schema()
        // presented Variant cols as Utf8View, so the inbound batches may carry strings).
        while let Some(batch) = data.next().await.transpose()? {
            let batch_rows = batch.num_rows();
            debug!("write_all: received batch with {} rows", batch_rows);
            total_row_count += batch_rows;
            let project_id = extract_project_id(&batch).unwrap_or_else(|| self.default_project.clone());
            let batch = normalize_timestamp_tz(batch)?;
            let converted = convert_variant_columns(batch, &target_schema)?;
            project_batches.entry(project_id).or_default().push(converted);
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
        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }
        // No `logically_equivalent_names_and_types(&input.schema())` check here:
        // `self.schema()` returns the "insert-compatible" (lying) schema where
        // Variant columns appear as Utf8View so VALUES literals type-check.
        // Validating against that shape would reject the real downstream batches
        // (which carry Variant). `write_all` coerces back to Variant before
        // the Delta commit, so the type contract is enforced at the boundary
        // that matters.
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(self.clone()), None)))
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

        // Tantivy prefilter. Two independent paths:
        //
        // 1. Delta side — query the sidecar tantivy service, build `id IN
        //    (delta_ids)` and apply it to the Delta scan only. Delta files
        //    contain only flushed data; MemBuffer rows are never here, so
        //    using delta_ids on MemBuffer would drop valid rows.
        //
        // 2. MemBuffer side — `query_partitioned_with_text_match` handles
        //    its own atomic per-bucket prefilter under the bucket lock. The
        //    caller (us) does NOT compute or pass MemBuffer ids — doing so
        //    would re-introduce the race where a concurrent insert lands a
        //    row in the snapshot that isn't in the pre-computed id set.
        let text_match_preds = crate::tantivy_index::udf::collect_text_matches(&optimized_filters);
        let mut tantivy_id_filter: Option<Expr> = None;
        if !text_match_preds.is_empty()
            && let Some(svc) = self.database.tantivy_search()
        {
            use datafusion::logical_expr::{Expr, lit};
            let tcfg = &self.database.config().tantivy;
            let max_hits = tcfg.prefilter_max_hits();
            let min_sel_pct = tcfg.prefilter_min_selectivity_pct() as u64;
            crate::metrics::record_tantivy_prefilter_attempt();

            let mut delta_ids: Option<std::collections::HashSet<String>> = None;
            let mut delta_indexed_rows: u64 = 0;
            let mut delta_any_usable = false;
            let mut abort_reason: Option<&'static str> = None;
            for p in &text_match_preds {
                match svc.search_with_stats(&self.table_name, &project_id, &p.column, &p.query, max_hits).await {
                    Ok(Some(result)) => {
                        delta_any_usable = true;
                        delta_indexed_rows = delta_indexed_rows.saturating_add(result.indexed_rows);
                        let ids: std::collections::HashSet<String> = result.hits.into_iter().map(|h| h.id).collect();
                        delta_ids = Some(match delta_ids.take() {
                            None => ids,
                            Some(prev) => prev.intersection(&ids).cloned().collect(),
                        });
                    }
                    Ok(None) => {
                        abort_reason = Some("delta_no_index_or_cap_exceeded");
                        delta_any_usable = false;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "tantivy search failed for {}/{}: {} — falling back to full scan",
                            project_id, self.table_name, e
                        );
                        crate::metrics::record_tantivy_prefilter_error();
                        abort_reason = Some("delta_error");
                        delta_any_usable = false;
                        break;
                    }
                }
            }

            if delta_any_usable {
                if let Some(ids) = delta_ids {
                    // No indexed rows = no useful prefilter. Without this guard
                    // we'd emit an empty IN(...) list that zeros the Delta
                    // scan even when matching rows exist there (e.g. data
                    // written directly without triggering an index build).
                    if delta_indexed_rows == 0 {
                        crate::metrics::record_tantivy_prefilter_skipped();
                        debug!("Tantivy prefilter skipped for {}/{}: empty_index", project_id, self.table_name);
                    } else if (ids.len() as u64) * 100 >= delta_indexed_rows * min_sel_pct {
                        // Selectivity cutoff: if the hit set covers most of the
                        // indexed rows, the IN-list won't prune enough to be
                        // worth its planning cost. Bail; original predicate
                        // re-runs as the correctness backstop.
                        crate::metrics::record_tantivy_prefilter_skipped();
                        debug!("Tantivy prefilter skipped for {}/{}: low_selectivity", project_id, self.table_name);
                    } else {
                        crate::metrics::record_tantivy_prefilter_used();
                        tantivy_id_filter = Some(Expr::InList(datafusion::logical_expr::expr::InList {
                            expr:    Box::new(datafusion::logical_expr::col("id")),
                            list:    ids.into_iter().map(lit).collect(),
                            negated: false,
                        }));
                    }
                }
            } else {
                crate::metrics::record_tantivy_prefilter_skipped();
                if let Some(reason) = abort_reason {
                    debug!("Tantivy prefilter skipped for {}/{}: {}", project_id, self.table_name, reason);
                }
            }
        }

        // Variant binary flows through scans untouched; downstream nodes
        // (variant_get, ->, ->>) consume it directly. JSON serialization
        // happens only at the root projection via VariantSelectRewriter.
        let wrap_result = |plan: Arc<dyn ExecutionPlan>| -> DFResult<Arc<dyn ExecutionPlan>> { Ok(plan) };

        // Check if buffered layer is configured
        let has_layer = self.database.buffered_layer().is_some();
        debug!("ProjectRoutingTable::scan - buffered_layer present: {}, project_id: {}", has_layer, project_id);
        let Some(layer) = self.database.buffered_layer() else {
            // No buffered layer, query Delta directly
            debug!("No buffered layer, querying Delta only");
            let mut delta_only_filters = optimized_filters.clone();
            if let Some(f) = tantivy_id_filter.clone() {
                delta_only_filters.push(f);
            }
            let plan = self.scan_delta_only(state, &project_id, projection, &delta_only_filters, limit).await?;
            return wrap_result(plan);
        };

        span.record("scan.uses_mem_buffer", true);

        // Get MemBuffer's time range for this project/table
        let mem_time_range = layer.get_time_range(&project_id, &self.table_name);

        // Extract query time range from filters
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);

        // Skip Delta when the query's lower bound is at/after MemBuffer's
        // oldest row. Delta is excluded from MemBuffer's range by the
        // per-bucket logic below, so no Delta row can satisfy
        // `timestamp >= query_min` in that case — upper bound doesn't matter
        // (covers open-ended `WHERE timestamp >= now() - 5m` dashboards).
        let skip_delta = match (mem_time_range, query_time_range) {
            (Some((mem_oldest, _)), Some((query_min, _))) => query_min >= mem_oldest,
            _ => false,
        };

        // MemBuffer query. `query_partitioned_with_text_match` handles its
        // own atomic per-bucket prefilter inside the bucket lock — we must
        // NOT prepend `tantivy_id_filter` here (that filter is derived from
        // delta-side IDs only and would drop legitimate MemBuffer rows).
        let mem_partitions = match layer.query_partitioned_with_text_match(&project_id, &self.table_name, &optimized_filters, &text_match_preds) {
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
            let mut delta_only_filters = optimized_filters.clone();
            if let Some(f) = tantivy_id_filter.clone() {
                delta_only_filters.push(f);
            }
            let plan = self.scan_delta_only(state, &project_id, projection, &delta_only_filters, limit).await?;
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

        // Build Delta filters with per-bucket exclusion.
        //
        // The MemBuffer / Delta union must not double-count rows: any time
        // range currently held by a MemBuffer bucket is served *by*
        // MemBuffer (it's authoritative for those rows) so Delta must
        // exclude them. The old logic used a single `timestamp < oldest_mem_ts`
        // cutoff, which broke catastrophically when a bucket got stuck in
        // MemBuffer (e.g. failed flush) — it dragged `oldest_mem_ts`
        // backwards and wrongly hid all the Delta rows *above* it. Fixed
        // by listing the actual ranges MemBuffer currently holds and
        // excluding only those.
        let mem_ranges = layer.get_bucket_ranges(&project_id, &self.table_name);
        let mut delta_filters = optimized_filters.clone();
        let ts_col = || Box::new(col("timestamp"));
        let ts_lit = |t: i64| Box::new(lit(ScalarValue::TimestampMicrosecond(Some(t), Some("UTC".into()))));
        for (start, end) in &mem_ranges {
            // NOT (ts >= start AND ts < end)  ≡  (ts < start) OR (ts >= end)
            let below = Expr::BinaryExpr(BinaryExpr {
                left:  ts_col(),
                op:    Operator::Lt,
                right: ts_lit(*start),
            });
            let at_or_above = Expr::BinaryExpr(BinaryExpr {
                left:  ts_col(),
                op:    Operator::GtEq,
                right: ts_lit(*end),
            });
            delta_filters.push(Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(below),
                op:    Operator::Or,
                right: Box::new(at_or_above),
            }));
        }
        if let Some(f) = tantivy_id_filter.clone() {
            delta_filters.push(f);
        }

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
mod writer_properties_tests {
    use deltalake::datafusion::parquet::{
        basic::{Compression, ZstdLevel},
        schema::types::ColumnPath,
    };

    use super::*;
    use crate::schema_loader::{FieldDef, SortingColumnDef, TableSchema};

    fn cfg() -> crate::config::ParquetConfig {
        serde_json::from_str("{}").unwrap()
    }

    fn field(name: &str, dt: &str) -> FieldDef {
        FieldDef {
            name:         name.into(),
            data_type:    dt.into(),
            nullable:     true,
            tantivy:      None,
            dictionary:   None,
            bloom_filter: false,
        }
    }

    fn schema_with(fields: Vec<FieldDef>, sort: Vec<&str>) -> TableSchema {
        TableSchema {
            table_name: "t".into(),
            partitions: vec![],
            sorting_columns: sort
                .into_iter()
                .map(|n| SortingColumnDef {
                    name:        n.into(),
                    descending:  false,
                    nulls_first: false,
                })
                .collect(),
            z_order_columns: vec![],
            fields,
            time_column: None,
        }
    }

    #[test]
    fn compression_level_drives_zstd() {
        for level in [3, 9, 15, 19] {
            let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), level);
            assert_eq!(
                p.compression(&ColumnPath::from("anything")),
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap())
            );
        }
    }

    #[test]
    fn invalid_zstd_level_falls_back() {
        let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), 999);
        assert_eq!(
            p.compression(&ColumnPath::from("x")),
            Compression::ZSTD(ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap())
        );
    }

    #[test]
    fn footer_kv_metadata_carries_tier() {
        let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), 15);
        let kv = p.key_value_metadata().expect("KV metadata present");
        let tier = kv.iter().find(|k| k.key == COMPRESSION_TIER_KEY).expect("tier key present");
        assert_eq!(tier.value.as_deref(), Some("15"));
    }

    #[test]
    fn bloom_opt_in_only_for_flagged_columns() {
        let mut f1 = field("id", "Utf8");
        f1.bloom_filter = true;
        let p = build_writer_properties(&cfg(), &schema_with(vec![f1, field("body", "Utf8")], vec![]), 3);
        assert!(p.bloom_filter_properties(&ColumnPath::from("id")).is_some(), "flagged column has bloom");
        assert!(p.bloom_filter_properties(&ColumnPath::from("body")).is_none(), "unflagged column has no bloom");
    }

    #[test]
    fn global_bloom_kill_switch_overrides_opt_in() {
        let mut f = field("id", "Utf8");
        f.bloom_filter = true;
        let mut c = cfg();
        c.timefusion_bloom_filter_disabled = true;
        let p = build_writer_properties(&c, &schema_with(vec![f], vec![]), 3);
        assert!(p.bloom_filter_properties(&ColumnPath::from("id")).is_none());
    }

    #[test]
    fn dictionary_opt_out_disables_dict() {
        let mut f = field("stacktrace", "Utf8");
        f.dictionary = Some(false);
        let p = build_writer_properties(&cfg(), &schema_with(vec![f], vec![]), 3);
        assert!(!p.dictionary_enabled(&ColumnPath::from("stacktrace")));
    }

    #[test]
    fn sort_key_utf8_uses_delta_byte_array_and_no_dict() {
        use deltalake::datafusion::parquet::basic::Encoding;
        let p = build_writer_properties(&cfg(), &schema_with(vec![field("id", "Utf8")], vec!["id"]), 3);
        assert_eq!(p.encoding(&ColumnPath::from("id")), Some(Encoding::DELTA_BYTE_ARRAY));
        assert!(!p.dictionary_enabled(&ColumnPath::from("id")));
    }

    #[test]
    fn timestamp_and_int_use_delta_binary_packed() {
        use deltalake::datafusion::parquet::basic::Encoding;
        let p = build_writer_properties(
            &cfg(),
            &schema_with(vec![field("ts", "Timestamp(Nanosecond, None)"), field("n", "Int64")], vec![]),
            3,
        );
        assert_eq!(p.encoding(&ColumnPath::from("ts")), Some(Encoding::DELTA_BINARY_PACKED));
        assert!(!p.dictionary_enabled(&ColumnPath::from("ts")));
        assert_eq!(p.encoding(&ColumnPath::from("n")), Some(Encoding::DELTA_BINARY_PACKED));
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serial_test::serial;

    use super::*;
    use crate::{config::AppConfig, test_utils::test_helpers::*};

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

    /// End-to-end test of `recompress_partition`. Skip behavior is the
    /// load-bearing property: if the footer-tier probe breaks, the daily
    /// cron rewrites every partition every night. We assert via file-set
    /// comparison since the production code path itself reads the footer.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_recompress_partition_skip_idempotency() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            let (db, _ctx, prefix) = setup_test_database().await?;
            let project_id = format!("project_{}", prefix);
            let today = chrono::Utc::now().date_naive();

            let batch = json_to_batch(vec![test_span("rc1", "span1", &project_id)])?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true).await?;

            let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");

            // First recompress at tier 9 — must rewrite files.
            let files_before: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert!(!files_before.is_empty(), "expected files in today's partition");
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9).await?;
            let files_after: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_ne!(files_before, files_after, "first recompress must rewrite files");

            // Re-run at the same tier — footer probe must detect tier=9 and skip,
            // so the file set is unchanged. If skip is broken, this assertion
            // fails because Optimize emits a fresh part file.
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9).await?;
            let files_after_rerun: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_eq!(files_after, files_after_rerun, "rerun at same tier must skip");

            // Downgrade target — also skip.
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 3).await?;
            let files_after_downgrade: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_eq!(files_after, files_after_downgrade, "downgrade target must skip");

            db.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
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

    // The three #[ignore]'d tests below stress real Delta-table concurrency against
    // S3 (MinIO). They run cleanly in isolated environments (`make test-all`) but
    // wedge in the shared GHA test process because `config::init_config()` uses a
    // OnceLock — so every test inherits the *first* test's TIMEFUSION_TABLE_PREFIX.
    // By the time a "concurrent" test runs, the table has accumulated versions
    // from earlier tests and 3-way commit contention without DynamoDB locking
    // (CI runs with AWS_S3_LOCKING_PROVIDER="") retries past any reasonable
    // timeout. Run with `cargo test -- --ignored` locally to exercise them.
    #[serial]
    #[ignore = "wedges under shared-state CI; see comment above. Run with cargo test -- --ignored"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_writes_same_project() -> Result<()> {
        // Locally <3s; CI's MinIO + fresh Delta-table create-on-write under 3-way
        // concurrent contention regularly exceeds 60s on the GHA runner. Headroom.
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
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
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
    }

    #[serial]
    #[ignore = "wedges under shared-state CI; see test_concurrent_writes_same_project comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_table_creation() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
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
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
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
    #[ignore = "wedges under shared-state CI; see test_concurrent_writes_same_project comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_mixed_operations() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
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
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
    }
}
