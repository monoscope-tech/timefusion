use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
use datafusion::physical_plan::ColumnStatistics;
use datafusion::common::stats::Precision;
use deltalake::DeltaTable;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Cache entry for table statistics
#[derive(Clone, Debug)]
pub struct CachedStatistics {
    pub stats: Statistics,
    pub timestamp: std::time::Instant,
    pub version: i64,
}

/// Statistics extractor for Delta Lake tables
#[derive(Debug)]
pub struct DeltaStatisticsExtractor {
    cache: Arc<RwLock<LruCache<String, CachedStatistics>>>,
    cache_ttl_seconds: u64,
}

impl DeltaStatisticsExtractor {
    pub fn new(cache_size: usize, cache_ttl_seconds: u64) -> Self {
        let cache = LruCache::new(NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(50).unwrap()));
        Self {
            cache: Arc::new(RwLock::new(cache)),
            cache_ttl_seconds,
        }
    }

    /// Extract statistics from a Delta table
    pub async fn extract_statistics(
        &self,
        table: &DeltaTable,
        project_id: &str,
        table_name: &str,
        schema: &SchemaRef,
    ) -> Result<Statistics> {
        let cache_key = format!("{}:{}", project_id, table_name);
        
        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.peek(&cache_key) {
                if cached.timestamp.elapsed().as_secs() < self.cache_ttl_seconds {
                    debug!("Statistics cache hit for {}", cache_key);
                    return Ok(cached.stats.clone());
                }
            }
        }

        debug!("Extracting fresh statistics for {}", cache_key);
        
        // Get table metadata
        let version = table.version();
        let _metadata = table.metadata()?;
        
        // Extract basic statistics
        let num_files = table.get_file_uris()?.count();
        
        // Calculate row count and byte size from Delta metadata
        // Note: In production Delta Lake, you'd parse the transaction log for exact counts
        let (num_rows, total_byte_size) = self.calculate_table_stats(table).await?;
        
        // Extract column statistics
        let column_statistics = self.extract_column_statistics(table, schema).await?;
        
        let stats = Statistics {
            num_rows: Precision::Inexact(num_rows as usize),
            total_byte_size: Precision::Inexact(total_byte_size as usize),
            column_statistics,
        };
        
        // Update cache
        {
            let mut cache = self.cache.write().await;
            cache.put(
                cache_key.clone(),
                CachedStatistics {
                    stats: stats.clone(),
                    timestamp: std::time::Instant::now(),
                    version: version.unwrap_or(0),
                },
            );
        }
        
        info!(
            "Extracted statistics for {}: {} rows, {} bytes, {} files",
            cache_key, num_rows, total_byte_size, num_files
        );
        
        Ok(stats)
    }

    /// Calculate table-level statistics
    async fn calculate_table_stats(&self, table: &DeltaTable) -> Result<(u64, u64)> {
        // Get file URIs and count
        let files: Vec<_> = table.get_file_uris()?.collect();
        let num_files = files.len() as u64;
        
        // Get snapshot to access state
        let _snapshot = table.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {}", e))?;
        
        // Use better estimate based on our page row count limit
        // Delta Lake doesn't expose row count directly in the Rust API
        let num_rows = num_files * 20_000; // Default page row count limit from DELTA_CONFIG.md
        
        // Estimate bytes based on typical Parquet compression ratio
        let estimated_bytes_per_file = 10_000_000; // 10MB compressed
        let total_bytes = num_files * estimated_bytes_per_file;
        
        Ok((num_rows, total_bytes))
    }

    /// Extract column-level statistics
    async fn extract_column_statistics(
        &self,
        table: &DeltaTable,
        schema: &SchemaRef,
    ) -> Result<Vec<ColumnStatistics>> {
        let mut column_stats = Vec::new();
        
        // Get snapshot to potentially access file statistics
        let snapshot = table.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {}", e))?;
        
        // For each column in the schema
        for field in schema.fields() {
            // Check if this is a partition column - these often have better statistics
            let is_partition_col = snapshot.metadata().partition_columns().contains(&field.name().to_string());
            
            let stats = if is_partition_col {
                // Partition columns typically have exact statistics
                ColumnStatistics {
                    null_count: Precision::Exact(0), // Partitions don't allow nulls
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                }
            } else {
                // For non-partition columns, we'd need to parse parquet metadata
                // This requires reading the actual parquet files which is expensive
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                }
            };
            column_stats.push(stats);
        }
        
        Ok(column_stats)
    }

    /// Clear the statistics cache
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        info!("Statistics cache cleared");
    }

    /// Get cache size
    pub async fn cache_size(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Invalidate specific table statistics
    pub async fn invalidate(&self, project_id: &str, table_name: &str) {
        let cache_key = format!("{}:{}", project_id, table_name);
        let mut cache = self.cache.write().await;
        cache.pop(&cache_key);
        debug!("Invalidated statistics for {}", cache_key);
    }
    
    /// Get cache statistics for monitoring
    pub async fn get_cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        (cache.len(), cache.cap().get())
    }
}

/// Statistics for file-level pruning
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilePruningStats {
    pub file_path: String,
    pub num_rows: u64,
    pub size_bytes: u64,
    pub column_bounds: HashMap<String, ColumnBounds>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnBounds {
    pub min_value: Option<serde_json::Value>,
    pub max_value: Option<serde_json::Value>,
    pub null_count: u64,
}

impl DeltaStatisticsExtractor {
    /// Extract file-level statistics for pruning (useful for advanced optimizations)
    pub async fn extract_file_pruning_stats(&self, table: &DeltaTable) -> Result<Vec<FilePruningStats>> {
        let mut file_stats = Vec::new();
        
        // Get the snapshot to access file information
        let _snapshot = table.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {}", e))?;
        
        // Get file URIs
        let files: Vec<_> = table.get_file_uris()?.collect();
        
        for file_path in files {
            // Create basic file stats
            // In production, you would parse the Parquet file metadata to get actual statistics
            let stats = FilePruningStats {
                file_path: file_path.clone(),
                num_rows: 20_000, // Estimate based on page row count limit
                size_bytes: 10_000_000, // 10MB estimate
                column_bounds: HashMap::new(), // Would be populated from Parquet metadata
            };
            file_stats.push(stats);
        }
        
        Ok(file_stats)
    }
}

impl FilePruningStats {
    /// Check if this file can be skipped based on predicates
    pub fn can_skip(&self, column: &str, min: Option<&serde_json::Value>, max: Option<&serde_json::Value>) -> bool {
        if let Some(bounds) = self.column_bounds.get(column) {
            // If all values are null, we can skip for non-null comparisons
            if bounds.null_count == self.num_rows {
                return true;
            }
            
            // Check if the file's range overlaps with the query range
            if let (Some(file_min), Some(file_max)) = (&bounds.min_value, &bounds.max_value) {
                if let Some(query_min) = min {
                    // Compare as numbers if both are numbers
                    if let (Some(file_val), Some(query_val)) = (file_max.as_f64(), query_min.as_f64()) {
                        if file_val < query_val {
                            return true; // File max is less than query min
                        }
                    }
                }
                if let Some(query_max) = max {
                    if let (Some(file_val), Some(query_val)) = (file_min.as_f64(), query_max.as_f64()) {
                        if file_val > query_val {
                            return true; // File min is greater than query max
                        }
                    }
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_statistics_cache() {
        let extractor = DeltaStatisticsExtractor::new(10, 300);
        assert_eq!(extractor.cache_size().await, 0);
        
        extractor.invalidate("project1", "table1").await;
        assert_eq!(extractor.cache_size().await, 0);
    }

    #[test]
    fn test_file_pruning() {
        let mut column_bounds = HashMap::new();
        column_bounds.insert(
            "timestamp".to_string(),
            ColumnBounds {
                min_value: Some(serde_json::json!(100)),
                max_value: Some(serde_json::json!(200)),
                null_count: 0,
            },
        );
        
        let stats = FilePruningStats {
            file_path: "test.parquet".to_string(),
            num_rows: 1000,
            size_bytes: 100000,
            column_bounds,
        };
        
        // File range is [100, 200]
        // Should skip if query is entirely before or after
        assert!(stats.can_skip("timestamp", None, Some(&serde_json::json!(50))));
        assert!(stats.can_skip("timestamp", Some(&serde_json::json!(250)), None));
        
        // Should not skip if ranges overlap
        assert!(!stats.can_skip("timestamp", Some(&serde_json::json!(150)), None));
        assert!(!stats.can_skip("timestamp", None, Some(&serde_json::json!(150))));
    }
}