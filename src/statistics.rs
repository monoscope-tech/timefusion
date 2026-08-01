use std::{
    num::NonZeroUsize,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use datafusion::{
    arrow::{array::Int64Array, compute::sum},
    common::{Statistics, stats::Precision},
};
use deltalake::DeltaTable;
use lru::LruCache;
use tokio::sync::RwLock;
use tracing::{debug, info};

const DEFAULT_CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(50).unwrap();

/// Cache entry for basic table statistics
#[derive(Clone, Debug)]
pub struct CachedStatistics {
    pub stats: Statistics,
    pub timestamp: Instant,
    pub version: u64,
}

/// Simplified statistics extractor for Delta Lake tables: row count and byte
/// size only, cached per `(project_id, table_name)` and keyed on Delta version.
#[derive(Debug)]
pub struct DeltaStatisticsExtractor {
    cache: RwLock<LruCache<String, CachedStatistics>>,
    cache_ttl: Duration,
    page_row_limit: usize,
}

impl DeltaStatisticsExtractor {
    pub fn new(cache_size: usize, cache_ttl_seconds: u64, page_row_limit: usize) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(NonZeroUsize::new(cache_size).unwrap_or(DEFAULT_CACHE_SIZE))),
            cache_ttl: Duration::from_secs(cache_ttl_seconds),
            page_row_limit,
        }
    }

    /// Extract basic statistics from a Delta table (row count and byte size only)
    pub async fn extract_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        let cache_key = cache_key(project_id, table_name);
        let version = table.version().unwrap_or(0);

        if let Some(stats) =
            self.cache.read().await.peek(&cache_key).filter(|c| c.version == version && c.timestamp.elapsed() < self.cache_ttl).map(|c| c.stats.clone())
        {
            debug!(%cache_key, version, "statistics cache hit");
            return Ok(stats);
        }

        let (num_files, num_rows, total_byte_size) = table_stats(table, self.page_row_limit)?;
        let stats = Statistics { num_rows: Precision::Inexact(num_rows), total_byte_size: Precision::Exact(total_byte_size), column_statistics: vec![] };

        info!(%cache_key, num_rows, total_byte_size, num_files, "extracted basic statistics");

        self.cache.write().await.put(cache_key, CachedStatistics { stats: stats.clone(), timestamp: Instant::now(), version });

        Ok(stats)
    }

    pub async fn clear_cache(&self) {
        self.cache.write().await.clear();
        info!("Statistics cache cleared");
    }

    /// Drop the cached entry for one table so the next extraction recomputes it.
    pub async fn invalidate(&self, project_id: &str, table_name: &str) {
        let cache_key = cache_key(project_id, table_name);
        if let Some(removed) = self.cache.write().await.pop(&cache_key) {
            debug!(%cache_key, version = removed.version, "invalidated statistics");
        }
    }

    /// Get cache statistics for monitoring: `(used, capacity)`
    pub async fn get_cache_stats(&self) -> (usize, usize) {
        let cache = self.cache.read().await;
        (cache.len(), cache.cap().get())
    }
}

fn cache_key(project_id: &str, table_name: &str) -> String {
    format!("{project_id}:{table_name}")
}

/// Table-level `(files, rows, bytes)` summed from the flattened add-actions batch.
/// Falls back to `files × page_row_limit` when the snapshot carries no
/// `stats.numRecords` column.
fn table_stats(table: &DeltaTable, page_row_limit: usize) -> Result<(usize, usize, usize)> {
    let snapshot = table.snapshot().context("Failed to get Delta table snapshot")?;
    let actions = snapshot.add_actions_table(true).with_context(|| format!("Failed to get add actions for table at {}", table.table_url()))?;

    // `None` distinguishes "column absent" (→ fallback) from "present but empty/unsummable" (→ 0).
    let sum_i64 = |name| actions.column_by_name(name).map(|c| c.as_any().downcast_ref::<Int64Array>().and_then(sum).unwrap_or(0).max(0) as usize);

    let num_files = actions.num_rows();
    let rows = sum_i64("stats.numRecords").unwrap_or_else(|| num_files.saturating_mul(page_row_limit));
    Ok((num_files, rows, sum_i64("size_bytes").unwrap_or(0)))
}
