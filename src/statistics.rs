use anyhow::Result;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use deltalake::DeltaTable;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Cache entry for basic table statistics
#[derive(Clone, Debug)]
pub struct CachedStatistics {
    pub stats: Statistics,
    pub timestamp: std::time::Instant,
    pub version: i64,
}

// TODO: delete this file in favor of using:
/// Simplified statistics extractor for Delta Lake tables
/// Only extracts basic row count and byte size statistics
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

    /// Extract basic statistics from a Delta table (row count and byte size only)
    pub async fn extract_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str, _schema: &SchemaRef) -> Result<Statistics> {
        let cache_key = format!("{}:{}", project_id, table_name);

        // Check cache first
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.peek(&cache_key) {
                let elapsed = cached.timestamp.elapsed().as_secs();
                let current_version = table.version().unwrap_or(-1);

                if elapsed < self.cache_ttl_seconds && cached.version == current_version {
                    debug!("Statistics cache hit for {} (version {})", cache_key, current_version);
                    return Ok(cached.stats.clone());
                }
            }
        }

        debug!("Extracting basic statistics for {}", cache_key);

        // Get table metadata
        let version = table.version();
        let num_files = table.get_file_uris()?.count();

        // Calculate row count and byte size from Delta metadata
        let (num_rows, total_byte_size) = self.calculate_table_stats(table).await?;

        // Create basic statistics without column-level details
        let stats = Statistics {
            num_rows: Precision::Inexact(num_rows as usize),
            total_byte_size: Precision::Exact(total_byte_size as usize),
            column_statistics: vec![], // No column statistics needed
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
            "Extracted basic statistics for {}: {} rows, {} bytes, {} files",
            cache_key, num_rows, total_byte_size, num_files
        );

        Ok(stats)
    }

    /// Calculate table-level statistics
    async fn calculate_table_stats(&self, table: &DeltaTable) -> Result<(u64, u64)> {
        let snapshot = table.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {}", e))?;

        // Try to get actual statistics from Delta log
        let _metadata = snapshot.metadata();

        // Get file actions to calculate real stats
        let log_store = table.log_store();
        let file_actions = snapshot.file_actions(log_store.as_ref()).await?;
        let mut total_rows = 0u64;
        let mut total_bytes = 0u64;
        let mut has_row_stats = false;

        for action in file_actions {
            // Delta stores actual row count and size in the log
            if let Some(stats) = &action.stats {
                // Parse stats JSON if available
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(stats) {
                    if let Some(num_records) = parsed.get("numRecords").and_then(|v| v.as_u64()) {
                        total_rows += num_records;
                        has_row_stats = true;
                    }
                }
            }
            total_bytes += action.size as u64;
        }

        // Fallback to estimates if stats not available
        if !has_row_stats {
            let log_store = table.log_store();
            let num_files = snapshot.file_actions(log_store.as_ref()).await?.len() as u64;
            let page_row_limit = std::env::var("TIMEFUSION_PAGE_ROW_COUNT_LIMIT").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(20_000);
            total_rows = num_files * page_row_limit;
        }

        Ok((total_rows, total_bytes))
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
        if let Some(removed) = cache.pop(&cache_key) {
            debug!("Invalidated statistics for {} (was version {})", cache_key, removed.version);
        }
    }

    /// Get cache statistics for monitoring
    pub async fn get_cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        (cache.len(), cache.cap().get())
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
}
