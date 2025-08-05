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
    pub row_count_hash: u64, // Hash of row counts for quick comparison
}

/// Statistics extractor for Delta Lake tables
#[derive(Debug)]
pub struct DeltaStatisticsExtractor {
    cache: Arc<RwLock<LruCache<String, CachedStatistics>>>,
    cache_ttl_seconds: u64,
}

impl DeltaStatisticsExtractor {
    /// Convert JSON value to DataFusion ScalarValue
    fn json_to_scalar(json_val: &serde_json::Value, data_type: &arrow::datatypes::DataType) -> Result<datafusion::scalar::ScalarValue> {
        use arrow::datatypes::{DataType, TimeUnit};
        use datafusion::scalar::ScalarValue;
        
        match (json_val, data_type) {
            (serde_json::Value::String(s), DataType::Utf8) => Ok(ScalarValue::Utf8(Some(s.clone()))),
            (serde_json::Value::Number(n), DataType::Int64) => {
                n.as_i64().map(|v| ScalarValue::Int64(Some(v)))
                    .ok_or_else(|| anyhow::anyhow!("Invalid Int64 value"))
            }
            (serde_json::Value::Number(n), DataType::Float64) => {
                n.as_f64().map(|v| ScalarValue::Float64(Some(v)))
                    .ok_or_else(|| anyhow::anyhow!("Invalid Float64 value"))
            }
            (serde_json::Value::String(s), DataType::Timestamp(unit, tz)) => {
                // Parse ISO 8601 timestamp strings
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
                    match unit {
                        TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(Some(nanos), tz.clone())),
                        TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(Some(nanos / 1000), tz.clone())),
                        TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(Some(nanos / 1_000_000), tz.clone())),
                        TimeUnit::Second => Ok(ScalarValue::TimestampSecond(Some(nanos / 1_000_000_000), tz.clone())),
                    }
                } else if let Ok(ts) = s.parse::<i64>() {
                    // Handle numeric timestamp strings
                    match unit {
                        TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(Some(ts), tz.clone())),
                        TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(Some(ts), tz.clone())),
                        TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(Some(ts), tz.clone())),
                        TimeUnit::Second => Ok(ScalarValue::TimestampSecond(Some(ts), tz.clone())),
                    }
                } else {
                    Err(anyhow::anyhow!("Invalid timestamp format: {}", s))
                }
            }
            (serde_json::Value::Number(n), DataType::Timestamp(unit, tz)) => {
                // Handle numeric timestamps in JSON
                if let Some(ts) = n.as_i64() {
                    match unit {
                        TimeUnit::Nanosecond => Ok(ScalarValue::TimestampNanosecond(Some(ts), tz.clone())),
                        TimeUnit::Microsecond => Ok(ScalarValue::TimestampMicrosecond(Some(ts), tz.clone())),
                        TimeUnit::Millisecond => Ok(ScalarValue::TimestampMillisecond(Some(ts), tz.clone())),
                        TimeUnit::Second => Ok(ScalarValue::TimestampSecond(Some(ts), tz.clone())),
                    }
                } else {
                    Err(anyhow::anyhow!("Invalid timestamp number"))
                }
            }
            (serde_json::Value::Bool(b), DataType::Boolean) => Ok(ScalarValue::Boolean(Some(*b))),
            (serde_json::Value::Number(n), DataType::Int32) => {
                n.as_i64().and_then(|v| i32::try_from(v).ok())
                    .map(|v| ScalarValue::Int32(Some(v)))
                    .ok_or_else(|| anyhow::anyhow!("Invalid Int32 value"))
            }
            (serde_json::Value::String(s), DataType::Date32) => {
                // Parse date strings
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let days_since_epoch = (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                    Ok(ScalarValue::Date32(Some(days_since_epoch)))
                } else {
                    Err(anyhow::anyhow!("Invalid date format: {}", s))
                }
            }
            _ => Err(anyhow::anyhow!("Unsupported type conversion: {:?} to {:?}", json_val, data_type)),
        }
    }
    
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
                // Check both TTL and version
                let elapsed = cached.timestamp.elapsed().as_secs();
                let current_version = table.version().unwrap_or(-1);
                
                if elapsed < self.cache_ttl_seconds && cached.version == current_version {
                    debug!("Statistics cache hit for {} (version {})", cache_key, current_version);
                    return Ok(cached.stats.clone());
                } else if cached.version != current_version {
                    debug!("Statistics cache miss for {} - version changed from {} to {}", 
                           cache_key, cached.version, current_version);
                } else {
                    debug!("Statistics cache miss for {} - TTL expired ({}s)", cache_key, elapsed);
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
        
        // Use Exact precision when we have actual counts from Delta metadata
        let row_precision = if self.has_exact_row_count(&table).await {
            Precision::Exact(num_rows as usize)
        } else {
            Precision::Inexact(num_rows as usize)
        };
        
        let stats = Statistics {
            num_rows: row_precision,
            total_byte_size: Precision::Exact(total_byte_size as usize), // File sizes are always exact
            column_statistics,
        };
        
        // Calculate row count hash for quick invalidation checks
        let row_count_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            num_rows.hash(&mut hasher);
            total_byte_size.hash(&mut hasher);
            hasher.finish()
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
                    row_count_hash,
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
        let snapshot = table.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {}", e))?;
        
        // Try to get actual statistics from Delta log
        let _metadata = snapshot.metadata();
        
        // Get file actions to calculate real stats
        let file_actions = snapshot.file_actions()?;
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
            let num_files = snapshot.file_actions()?.len() as u64;
            let page_row_limit = std::env::var("TIMEFUSION_PAGE_ROW_COUNT_LIMIT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(20_000);
            total_rows = num_files * page_row_limit;
        }
        
        Ok((total_rows, total_bytes))
    }
    
    /// Check if we have exact row counts from Delta metadata
    async fn has_exact_row_count(&self, table: &DeltaTable) -> bool {
        if let Ok(snapshot) = table.snapshot() {
            if let Ok(actions) = snapshot.file_actions() {
                for action in actions.into_iter().take(1) { // Check just first file
                    if let Some(stats) = &action.stats {
                        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(stats) {
                            return parsed.get("numRecords").is_some();
                        }
                    }
                }
            }
        }
        false
    }

    /// Extract column-level statistics
    async fn extract_column_statistics(
        &self,
        table: &DeltaTable,
        schema: &SchemaRef,
    ) -> Result<Vec<ColumnStatistics>> {
        use datafusion::scalar::ScalarValue;
        use std::collections::HashMap;
        
        let snapshot = table.snapshot().map_err(|e| anyhow::anyhow!("Failed to get snapshot: {}", e))?;
        let mut column_stats = Vec::new();
        
        // Aggregate statistics across all files
        let mut col_min_values: HashMap<String, ScalarValue> = HashMap::new();
        let mut col_max_values: HashMap<String, ScalarValue> = HashMap::new();
        let mut col_null_counts: HashMap<String, u64> = HashMap::new();
        let col_distinct_counts: HashMap<String, u64> = HashMap::new();
        
        // Parse Delta statistics from file actions
        for action in snapshot.file_actions()? {
            if let Some(stats_json) = &action.stats {
                if let Ok(stats) = serde_json::from_str::<serde_json::Value>(stats_json) {
                    // Extract min/max values for each column
                    if let Some(min_values) = stats.get("minValues").and_then(|v| v.as_object()) {
                        for (col_name, min_val) in min_values {
                            if let Some(field) = schema.field_with_name(col_name).ok() {
                                if let Ok(scalar) = Self::json_to_scalar(min_val, field.data_type()) {
                                    col_min_values.entry(col_name.clone())
                                        .and_modify(|v| {
                                            if scalar.partial_cmp(v) == Some(std::cmp::Ordering::Less) {
                                                *v = scalar.clone();
                                            }
                                        })
                                        .or_insert(scalar);
                                }
                            }
                        }
                    }
                    
                    if let Some(max_values) = stats.get("maxValues").and_then(|v| v.as_object()) {
                        for (col_name, max_val) in max_values {
                            if let Some(field) = schema.field_with_name(col_name).ok() {
                                if let Ok(scalar) = Self::json_to_scalar(max_val, field.data_type()) {
                                    col_max_values.entry(col_name.clone())
                                        .and_modify(|v| {
                                            if scalar.partial_cmp(v) == Some(std::cmp::Ordering::Greater) {
                                                *v = scalar.clone();
                                            }
                                        })
                                        .or_insert(scalar);
                                }
                            }
                        }
                    }
                    
                    // Extract null counts
                    if let Some(null_counts) = stats.get("nullCount").and_then(|v| v.as_object()) {
                        for (col_name, null_count) in null_counts {
                            if let Some(count) = null_count.as_u64() {
                                *col_null_counts.entry(col_name.clone()).or_insert(0) += count;
                            }
                        }
                    }
                }
            }
        }
        
        // Build column statistics for each field
        for field in schema.fields() {
            let col_name = field.name();
            let is_partition_col = snapshot.metadata().partition_columns().contains(&col_name.to_string());
            
            let stats = if is_partition_col {
                // Partition columns have exact statistics
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                }
            } else {
                // Use extracted statistics
                // Estimate distinct count for certain columns
                let distinct_precision = if let Some(&count) = col_distinct_counts.get(col_name) {
                    Precision::Inexact(count as usize)
                } else {
                    // Heuristic estimation for common columns
                    match col_name.as_str() {
                        "project_id" => Precision::Inexact(100), // Estimated number of projects
                        "level" => Precision::Exact(5), // ERROR, WARN, INFO, DEBUG, TRACE
                        "status_code" => Precision::Inexact(50), // Common HTTP status codes
                        "resource___service___name" => Precision::Inexact(1000), // Service names
                        _ => {
                            // For other columns, estimate based on min/max if available
                            if let (Some(min), Some(max)) = (col_min_values.get(col_name), col_max_values.get(col_name)) {
                                Self::estimate_distinct_from_range(min, max, col_name)
                            } else {
                                Precision::Absent
                            }
                        }
                    }
                };
                
                ColumnStatistics {
                    null_count: col_null_counts.get(col_name)
                        .map(|&c| Precision::Exact(c as usize))
                        .unwrap_or(Precision::Absent),
                    min_value: col_min_values.get(col_name)
                        .map(|v| Precision::Exact(v.clone()))
                        .unwrap_or(Precision::Absent),
                    max_value: col_max_values.get(col_name)
                        .map(|v| Precision::Exact(v.clone()))
                        .unwrap_or(Precision::Absent),
                    distinct_count: distinct_precision,
                    sum_value: Precision::Absent, // Not commonly used for time-series
                }
            };
            column_stats.push(stats);
        }
        
        Ok(column_stats)
    }
    
    /// Estimate distinct count based on min/max range
    fn estimate_distinct_from_range(min: &datafusion::scalar::ScalarValue, max: &datafusion::scalar::ScalarValue, col_name: &str) -> Precision<usize> {
        
        use datafusion::scalar::ScalarValue;
        
        match (min, max) {
            (ScalarValue::Int64(Some(min_val)), ScalarValue::Int64(Some(max_val))) => {
                let range = (max_val - min_val).abs() as usize + 1;
                // For ID-like columns, assume most values are present
                if col_name.contains("id") || col_name == "duration" {
                    Precision::Inexact(range.min(1_000_000)) // Cap at 1M for safety
                } else {
                    Precision::Inexact((range as f64).sqrt() as usize) // Conservative estimate
                }
            }
            (ScalarValue::Float64(Some(min_val)), ScalarValue::Float64(Some(max_val))) => {
                let range = (max_val - min_val).abs();
                Precision::Inexact((range * 100.0) as usize) // Assume 100 buckets
            }
            (ScalarValue::TimestampNanosecond(Some(min_ts), _), ScalarValue::TimestampNanosecond(Some(max_ts), _)) => {
                let duration_secs = (max_ts - min_ts) / 1_000_000_000;
                // For timestamps, estimate based on typical data patterns
                if col_name == "timestamp" {
                    // Assume one distinct value per second on average
                    Precision::Inexact((duration_secs as usize).min(10_000_000))
                } else {
                    Precision::Inexact((duration_secs as f64).sqrt() as usize)
                }
            }
            _ => Precision::Absent,
        }
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
    
    /// Check if statistics need refresh based on version
    pub async fn needs_refresh(&self, project_id: &str, table_name: &str, current_version: i64) -> bool {
        let cache_key = format!("{}:{}", project_id, table_name);
        let cache = self.cache.read().await;
        
        if let Some(cached) = cache.peek(&cache_key) {
            cached.version != current_version
        } else {
            true // Not cached, needs refresh
        }
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