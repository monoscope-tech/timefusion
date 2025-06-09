use std::{collections::HashMap, sync::Arc, time::Duration};

use deltalake::{
    DeltaTable, DeltaTableBuilder, DeltaTableError,
    arrow::record_batch::RecordBatch,
    operations::{create::CreateBuilder, write::WriteBuilder},
    storage::ObjectStoreRef,
};
use object_store::{aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, memory::InMemory};
use tokio;
use url::Url;

use crate::obj_store::{CacheMetrics, DeltaCacheBuilder, DeltaCacheConfig};

/// Helper struct for creating Delta tables with caching
pub struct CachedDeltaTableBuilder {
    table_uri:       String,
    cache_config:    Option<DeltaCacheConfig>,
    storage_options: HashMap<String, String>,
}

impl CachedDeltaTableBuilder {
    pub fn new<S: Into<String>>(table_uri: S) -> Self {
        Self {
            table_uri:       table_uri.into(),
            cache_config:    None,
            storage_options: HashMap::new(),
        }
    }

    /// Enable caching with custom configuration
    pub fn with_cache_config(mut self, config: DeltaCacheConfig) -> Self {
        self.cache_config = Some(config);
        self
    }

    /// Enable caching with default configuration
    pub fn with_cache(mut self) -> Self {
        self.cache_config = Some(DeltaCacheConfig::default());
        self
    }

    /// Add storage options (AWS credentials, etc.)
    pub fn with_storage_options(mut self, options: HashMap<String, String>) -> Self {
        self.storage_options = options;
        self
    }

    /// Add a single storage option
    pub fn with_storage_option<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.storage_options.insert(key.into(), value.into());
        self
    }

    /// Build the Delta table with caching
    pub async fn build(self) -> Result<DeltaTable, DeltaTableError> {
        let base_store = self.create_base_object_store().await?;

        let final_store: ObjectStoreRef = if let Some(cache_config) = self.cache_config {
            // Wrap with cache
            let cached_store = DeltaCacheBuilder::new()
                .with_memory_capacity(cache_config.memory_capacity)
                .with_disk_capacity(cache_config.disk_capacity)
                .with_disk_path(cache_config.disk_cache_dir)
                .with_ttl(Duration::from_secs(cache_config.ttl_seconds))
                .with_compression(cache_config.compression_level)
                .enable_metrics(cache_config.enable_metrics)
                .enable_cache_warming(cache_config.enable_cache_warming)
                .cache_transaction_logs(cache_config.cache_transaction_logs)
                .cache_parquet_metadata(cache_config.cache_parquet_metadata)
                .cache_checkpoints(cache_config.cache_checkpoints)
                .build(base_store)
                .await
                .map_err(|e| DeltaTableError::ObjectStore { source: e })?;

            cached_store
        } else {
            base_store
        };

        // Create Delta table with the (potentially cached) object store
        DeltaTableBuilder::from_uri(&self.table_uri)
            .with_storage_backend(final_store, Url::parse(&self.table_uri).unwrap())
            .load()
            .await
    }

    /// Create the base object store based on URI scheme
    async fn create_base_object_store(&self) -> Result<ObjectStoreRef, DeltaTableError> {
        let uri = Url::parse(&self.table_uri).map_err(|e| DeltaTableError::Generic(format!("Invalid URI: {}", e)))?;

        match uri.scheme() {
            "s3" | "s3a" => {
                let mut builder = AmazonS3Builder::new();

                // Apply storage options
                for (key, value) in &self.storage_options {
                    match key.as_str() {
                        "AWS_ACCESS_KEY_ID" => builder = builder.with_access_key_id(value),
                        "AWS_SECRET_ACCESS_KEY" => builder = builder.with_secret_access_key(value),
                        "AWS_REGION" => builder = builder.with_region(value),
                        "AWS_ENDPOINT" => builder = builder.with_endpoint(value),
                        "AWS_BUCKET_NAME" => builder = builder.with_bucket_name(value),
                        _ => {}
                    }
                }

                let bucket = uri.host_str().ok_or_else(|| DeltaTableError::Generic("No bucket in S3 URI".to_string()))?;

                Ok(Arc::new(
                    builder.with_bucket_name(bucket).build().map_err(|e| DeltaTableError::ObjectStore { source: e })?,
                ))
            }

            "abfs" | "abfss" => {
                let mut builder = MicrosoftAzureBuilder::new();

                // Apply storage options
                for (key, value) in &self.storage_options {
                    match key.as_str() {
                        "AZURE_STORAGE_ACCOUNT_NAME" => builder = builder.with_account(value),
                        "AZURE_STORAGE_ACCOUNT_KEY" => builder = builder.with_access_key(value),
                        // "AZURE_STORAGE_SAS_TOKEN" => builder = builder.with_sas_authorization(vec![(value,value)]),
                        "AZURE_STORAGE_CONTAINER_NAME" => builder = builder.with_container_name(value),
                        _ => {}
                    }
                }

                Ok(Arc::new(builder.build().map_err(|e| DeltaTableError::ObjectStore { source: e })?))
            }

            "gs" => {
                let mut builder = GoogleCloudStorageBuilder::new();

                // Apply storage options
                for (key, value) in &self.storage_options {
                    match key.as_str() {
                        "GOOGLE_SERVICE_ACCOUNT" => builder = builder.with_service_account_path(value),
                        "GOOGLE_SERVICE_ACCOUNT_KEY" => builder = builder.with_service_account_key(value),
                        "GOOGLE_BUCKET_NAME" => builder = builder.with_bucket_name(value),
                        _ => {}
                    }
                }

                let bucket = uri.host_str().ok_or_else(|| DeltaTableError::Generic("No bucket in GCS URI".to_string()))?;

                Ok(Arc::new(
                    builder.with_bucket_name(bucket).build().map_err(|e| DeltaTableError::ObjectStore { source: e })?,
                ))
            }

            "file" => {
                let path = uri.to_file_path().map_err(|_| DeltaTableError::Generic("Invalid file path".to_string()))?;
                Ok(Arc::new(
                    LocalFileSystem::new_with_prefix(path).map_err(|e| DeltaTableError::ObjectStore { source: e })?,
                ))
            }

            "memory" => Ok(Arc::new(InMemory::new())),

            scheme => Err(DeltaTableError::Generic(format!("Unsupported scheme: {}", scheme))),
        }
    }
}

/// Extension trait for DeltaTable to access cache metrics
pub trait DeltaTableCacheExt {
    /// Get cache metrics if the table is using a cached store
    async fn cache_metrics(&self) -> Option<CacheMetrics>;

    /// Get access patterns if the table is using a cached store
    async fn access_patterns(&self) -> Option<HashMap<String, u64>>;
}

impl DeltaTableCacheExt for DeltaTable {
    async fn cache_metrics(&self) -> Option<CacheMetrics> {
        // // Try to downcast the object store to our cached implementation
        // let store = self.object_store();
        // if let Some(cached_store) = store{
        //     Some(cached_store.metrics().await)
        // } else {
        //     None
        // }
        todo!()
    }

    async fn access_patterns(&self) -> Option<HashMap<String, u64>> {
        // let store = self.object_store();
        // if let Some(cached_store) = store {
        //     Some(cached_store.get_access_patterns().await)
        // } else {
        //     None
        // }
        todo!()
    }
}

/// Convenience functions for common Delta operations with caching
pub struct CachedDeltaOps;

impl CachedDeltaOps {
    /// Create a new Delta table with caching enabled
    pub async fn create_table(
        table_uri: &str, schema: arrow::datatypes::SchemaRef, cache_config: Option<DeltaCacheConfig>,
    ) -> Result<DeltaTable, DeltaTableError> {
        let mut builder = CachedDeltaTableBuilder::new(table_uri);

        if let Some(config) = cache_config {
            builder = builder.with_cache_config(config);
        }

        let table = builder.build().await?;

        // Create the table if it doesn't exist
        CreateBuilder::new()
            .with_log_store(table.log_store())
            .with_table_name(table_uri)
            // .with_columns(schema.fields().iter().cloned())
            .await?;

        // Reload to get the created table
        // builder.build().await
        todo!()
    }

    /// Open an existing Delta table with caching
    pub async fn open_table(
        table_uri: &str, cache_config: Option<DeltaCacheConfig>, storage_options: Option<HashMap<String, String>>,
    ) -> Result<DeltaTable, DeltaTableError> {
        let mut builder = CachedDeltaTableBuilder::new(table_uri);

        if let Some(config) = cache_config {
            builder = builder.with_cache_config(config);
        }

        if let Some(options) = storage_options {
            builder = builder.with_storage_options(options);
        }

        builder.build().await
    }

    /// Write data to a Delta table with caching
    pub async fn write_to_table(table: &mut DeltaTable, batches: Vec<RecordBatch>) -> Result<(), DeltaTableError> {
        let m = table.state.clone();
        WriteBuilder::new(table.log_store(), m).with_input_batches(batches);

        // Reload the table to see the new data

        Ok(table.load().await.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_cached_delta_table_creation() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = format!("file://{}", temp_dir.path().to_str().unwrap());

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create cache config
        let cache_config = DeltaCacheConfig {
            memory_capacity: 64 * 1024 * 1024, // 64MB
            disk_capacity: 128 * 1024 * 1024,  // 128MB
            disk_cache_dir: temp_dir.path().join("cache").to_str().unwrap().to_string(),
            ttl_seconds: 300, // 5 minutes
            enable_metrics: true,
            ..Default::default()
        };

        // Create table with caching
        let table = CachedDeltaOps::create_table(&table_uri, schema.clone(), Some(cache_config)).await.unwrap();

        // Verify the table was created
        assert!(table.get_files_count() == 0); // New table, no data files yet

        // Check if cache metrics are available
        if let Some(metrics) = table.cache_metrics().await {
            println!("Cache metrics: {:?}", metrics);
        }
    }

    #[tokio::test]
    async fn test_write_and_read_with_cache() {
        let temp_dir = TempDir::new().unwrap();
        let table_uri = format!("file://{}", temp_dir.path().to_str().unwrap());

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create table with cache
        let mut table = CachedDeltaOps::create_table(&table_uri, schema.clone(), Some(DeltaCacheConfig::default())).await.unwrap();

        // Create some test data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]))],
        )
        .unwrap();

        // Write data
        CachedDeltaOps::write_to_table(&mut table, vec![batch]).await.unwrap();

        // Read data back (should hit cache on subsequent reads)
        let files = table.get_file_uris();
        assert!(!files.is_err());

        // Check cache metrics
        if let Some(metrics) = table.cache_metrics().await {
            println!("After write - Cache metrics: {:?}", metrics);
        }

        // Read again to test cache hit
        let _files_again = table.get_file_uris();

        if let Some(metrics) = table.cache_metrics().await {
            println!("After second read - Cache metrics: {:?}", metrics);
            assert!(metrics.total_requests > 0);
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example 1: Simple cached Delta table
    let cache_config = DeltaCacheConfig {
        memory_capacity: 256 * 1024 * 1024, // 256MB
        disk_capacity: 1024 * 1024 * 1024,  // 1GB
        disk_cache_dir: "/tmp/delta_cache".to_string(),
        ttl_seconds: 3600, // 1 hour
        enable_metrics: true,
        enable_cache_warming: true,
        ..Default::default()
    };

    let table = CachedDeltaTableBuilder::new("s3://my-bucket/my-table")
        .with_cache_config(cache_config)
        .with_storage_option("AWS_REGION", "us-west-2")
        .with_storage_option("AWS_ACCESS_KEY_ID", "your-access-key")
        .with_storage_option("AWS_SECRET_ACCESS_KEY", "your-secret-key")
        .build()
        .await?;

    println!("Table loaded with {} files", table.get_files_count());

    // Check cache performance
    if let Some(metrics) = table.cache_metrics().await {
        println!("Cache hit rate: {:.2}%", metrics.hit_rate() * 100.0);
        println!("Total requests: {}", metrics.total_requests);
        println!("Cache hits: {}", metrics.hits);
        println!("Cache misses: {}", metrics.misses);
    }

    // Example 2: Monitor access patterns
    if let Some(patterns) = table.access_patterns().await {
        println!("Most accessed files:");
        let mut sorted_patterns: Vec<_> = patterns.iter().collect();
        sorted_patterns.sort_by(|a, b| b.1.cmp(a.1));

        for (path, count) in sorted_patterns.iter().take(10) {
            println!("  {}: {} accesses", path, count);
        }
    }

    Ok(())
}
