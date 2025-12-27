use crate::mem_buffer::{FlushableBucket, MemBuffer, MemBufferStats};
use crate::wal::WalManager;
use arrow::array::RecordBatch;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 600; // 10 minutes
const DEFAULT_RETENTION_MINS: u64 = 90;
const DEFAULT_EVICTION_INTERVAL_SECS: u64 = 60; // 1 minute

#[derive(Debug, Clone)]
pub struct BufferConfig {
    pub wal_data_dir: PathBuf,
    pub flush_interval_secs: u64,
    pub retention_mins: u64,
    pub eviction_interval_secs: u64,
    pub max_memory_mb: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            wal_data_dir: PathBuf::from("/var/lib/timefusion/wal"),
            flush_interval_secs: DEFAULT_FLUSH_INTERVAL_SECS,
            retention_mins: DEFAULT_RETENTION_MINS,
            eviction_interval_secs: DEFAULT_EVICTION_INTERVAL_SECS,
            max_memory_mb: 4096,
        }
    }
}

impl BufferConfig {
    pub fn from_env() -> Self {
        let wal_dir = std::env::var("WALRUS_DATA_DIR").unwrap_or_else(|_| "/var/lib/timefusion/wal".to_string());

        Self {
            wal_data_dir: PathBuf::from(wal_dir),
            flush_interval_secs: std::env::var("TIMEFUSION_FLUSH_INTERVAL_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_FLUSH_INTERVAL_SECS),
            retention_mins: std::env::var("TIMEFUSION_BUFFER_RETENTION_MINS").ok().and_then(|v| v.parse().ok()).unwrap_or(DEFAULT_RETENTION_MINS),
            eviction_interval_secs: std::env::var("TIMEFUSION_EVICTION_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_EVICTION_INTERVAL_SECS),
            max_memory_mb: std::env::var("TIMEFUSION_BUFFER_MAX_MEMORY_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(4096),
        }
    }
}

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub entries_replayed: u64,
    pub batches_recovered: u64,
    pub oldest_entry_timestamp: Option<i64>,
    pub newest_entry_timestamp: Option<i64>,
    pub recovery_duration_ms: u64,
}

pub type DeltaWriteCallback = Arc<dyn Fn(String, String, Vec<RecordBatch>) -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

pub struct BufferedWriteLayer {
    wal: Arc<WalManager>,
    mem_buffer: Arc<MemBuffer>,
    config: BufferConfig,
    shutdown: CancellationToken,
    delta_write_callback: Option<DeltaWriteCallback>,
}

impl std::fmt::Debug for BufferedWriteLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedWriteLayer")
            .field("config", &self.config)
            .field("has_callback", &self.delta_write_callback.is_some())
            .finish()
    }
}

impl BufferedWriteLayer {
    pub fn new(config: BufferConfig) -> anyhow::Result<Self> {
        let wal = Arc::new(WalManager::new(config.wal_data_dir.clone())?);
        let mem_buffer = Arc::new(MemBuffer::new());

        Ok(Self {
            wal,
            mem_buffer,
            config,
            shutdown: CancellationToken::new(),
            delta_write_callback: None,
        })
    }

    pub fn with_delta_writer(mut self, callback: DeltaWriteCallback) -> Self {
        self.delta_write_callback = Some(callback);
        self
    }

    pub fn wal(&self) -> &Arc<WalManager> {
        &self.wal
    }

    pub fn mem_buffer(&self) -> &Arc<MemBuffer> {
        &self.mem_buffer
    }

    pub fn config(&self) -> &BufferConfig {
        &self.config
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub async fn insert(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        let timestamp_micros = chrono::Utc::now().timestamp_micros();

        // Step 1: Write to WAL for durability
        self.wal.append_batch(project_id, table_name, &batches)?;

        // Step 2: Write to MemBuffer for fast queries
        self.mem_buffer.insert_batches(project_id, table_name, batches, timestamp_micros)?;

        debug!("BufferedWriteLayer insert complete: project={}, table={}", project_id, table_name);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn recover_from_wal(&self) -> anyhow::Result<RecoveryStats> {
        let start = std::time::Instant::now();
        let retention_micros = (self.config.retention_mins as i64) * 60 * 1_000_000;
        let cutoff = chrono::Utc::now().timestamp_micros() - retention_micros;

        info!("Starting WAL recovery, cutoff={}", cutoff);

        let entries = self.wal.read_all_entries(Some(cutoff))?;

        let mut stats = RecoveryStats::default();
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;

        for (entry, batch) in entries {
            self.mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros)?;

            stats.entries_replayed += 1;
            stats.batches_recovered += 1;

            oldest_ts = Some(oldest_ts.map_or(entry.timestamp_micros, |ts| ts.min(entry.timestamp_micros)));
            newest_ts = Some(newest_ts.map_or(entry.timestamp_micros, |ts| ts.max(entry.timestamp_micros)));
        }

        stats.oldest_entry_timestamp = oldest_ts;
        stats.newest_entry_timestamp = newest_ts;
        stats.recovery_duration_ms = start.elapsed().as_millis() as u64;

        info!(
            "WAL recovery complete: entries={}, duration={}ms",
            stats.entries_replayed, stats.recovery_duration_ms
        );
        Ok(stats)
    }

    pub fn start_background_tasks(self: &Arc<Self>) {
        let this = Arc::clone(self);

        // Start flush task
        let flush_this = Arc::clone(&this);
        tokio::spawn(async move {
            flush_this.run_flush_task().await;
        });

        // Start eviction task
        let eviction_this = Arc::clone(&this);
        tokio::spawn(async move {
            eviction_this.run_eviction_task().await;
        });

        info!("BufferedWriteLayer background tasks started");
    }

    async fn run_flush_task(&self) {
        let flush_interval = Duration::from_secs(self.config.flush_interval_secs);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(flush_interval) => {
                    if let Err(e) = self.flush_completed_buckets().await {
                        error!("Flush task error: {}", e);
                    }
                }
                _ = self.shutdown.cancelled() => {
                    info!("Flush task shutting down");
                    break;
                }
            }
        }
    }

    async fn run_eviction_task(&self) {
        let eviction_interval = Duration::from_secs(self.config.eviction_interval_secs);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(eviction_interval) => {
                    self.evict_old_data();
                }
                _ = self.shutdown.cancelled() => {
                    info!("Eviction task shutting down");
                    break;
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn flush_completed_buckets(&self) -> anyhow::Result<()> {
        let current_bucket = MemBuffer::current_bucket_id();
        let flushable = self.mem_buffer.get_flushable_buckets(current_bucket);

        if flushable.is_empty() {
            debug!("No buckets to flush");
            return Ok(());
        }

        info!("Flushing {} buckets to Delta", flushable.len());

        for bucket in flushable {
            match self.flush_bucket(&bucket).await {
                Ok(()) => {
                    // Drain from MemBuffer after successful flush
                    self.mem_buffer.drain_bucket(&bucket.project_id, &bucket.table_name, bucket.bucket_id);

                    // Checkpoint WAL
                    if let Err(e) = self.wal.checkpoint(&bucket.project_id, &bucket.table_name) {
                        warn!("WAL checkpoint failed: {}", e);
                    }

                    debug!(
                        "Flushed bucket: project={}, table={}, bucket_id={}, rows={}",
                        bucket.project_id, bucket.table_name, bucket.bucket_id, bucket.row_count
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to flush bucket: project={}, table={}, bucket_id={}: {}",
                        bucket.project_id, bucket.table_name, bucket.bucket_id, e
                    );
                    // Keep bucket in MemBuffer for retry next cycle
                }
            }
        }

        Ok(())
    }

    async fn flush_bucket(&self, bucket: &FlushableBucket) -> anyhow::Result<()> {
        if let Some(ref callback) = self.delta_write_callback {
            callback(bucket.project_id.clone(), bucket.table_name.clone(), bucket.batches.clone()).await?;
        } else {
            warn!("No delta write callback configured, skipping flush");
        }
        Ok(())
    }

    fn evict_old_data(&self) {
        let retention_micros = (self.config.retention_mins as i64) * 60 * 1_000_000;
        let cutoff = chrono::Utc::now().timestamp_micros() - retention_micros;

        let evicted = self.mem_buffer.evict_old_data(cutoff);
        if evicted > 0 {
            debug!("Evicted {} old buckets", evicted);
        }

        // Also prune WAL
        if let Err(e) = self.wal.prune_older_than(cutoff) {
            warn!("WAL prune failed: {}", e);
        }
    }

    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("BufferedWriteLayer shutdown initiated");

        // Signal background tasks to stop
        self.shutdown.cancel();

        // Wait a bit for tasks to notice
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Force flush all remaining data
        let all_buckets = self.mem_buffer.get_all_buckets();
        info!("Flushing {} remaining buckets on shutdown", all_buckets.len());

        for bucket in all_buckets {
            match self.flush_bucket(&bucket).await {
                Ok(()) => {
                    self.mem_buffer.drain_bucket(&bucket.project_id, &bucket.table_name, bucket.bucket_id);
                    if let Err(e) = self.wal.checkpoint(&bucket.project_id, &bucket.table_name) {
                        warn!("WAL checkpoint on shutdown failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Shutdown flush failed for bucket {}: {}", bucket.bucket_id, e);
                }
            }
        }

        info!("BufferedWriteLayer shutdown complete");
        Ok(())
    }

    pub fn get_stats(&self) -> MemBufferStats {
        self.mem_buffer.get_stats()
    }

    pub fn get_oldest_timestamp(&self, project_id: &str, table_name: &str) -> Option<i64> {
        self.mem_buffer.get_oldest_timestamp(project_id, table_name)
    }

    /// Get the time range (oldest, newest) for a project/table in microseconds.
    pub fn get_time_range(&self, project_id: &str, table_name: &str) -> Option<(i64, i64)> {
        self.mem_buffer.get_time_range(project_id, table_name)
    }

    pub fn query(&self, project_id: &str, table_name: &str, filters: &[datafusion::logical_expr::Expr]) -> anyhow::Result<Vec<RecordBatch>> {
        self.mem_buffer.query(project_id, table_name, filters)
    }

    /// Query and return partitioned data - one partition per time bucket.
    /// This enables parallel execution across time buckets in DataFusion.
    pub fn query_partitioned(&self, project_id: &str, table_name: &str) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
        self.mem_buffer.query_partitioned(project_id, table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::tempdir;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["a", "b", "c"]);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let dir = tempdir().unwrap();
        let config = BufferConfig {
            wal_data_dir: dir.path().to_path_buf(),
            ..Default::default()
        };

        let layer = BufferedWriteLayer::new(config).unwrap();
        let batch = create_test_batch();

        layer.insert("project1", "table1", vec![batch.clone()]).await.unwrap();

        let results = layer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[tokio::test]
    #[ignore = "walrus-rust topic recovery needs investigation"]
    async fn test_recovery() {
        let dir = tempdir().unwrap();
        let config = BufferConfig {
            wal_data_dir: dir.path().to_path_buf(),
            retention_mins: 90,
            ..Default::default()
        };

        // First instance - write data
        {
            let layer = BufferedWriteLayer::new(config.clone()).unwrap();
            let batch = create_test_batch();
            layer.insert("project1", "table1", vec![batch]).await.unwrap();
            // Give WAL time to sync (uses FsyncSchedule::Milliseconds(200))
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }

        // Second instance - recover from WAL
        {
            let layer = BufferedWriteLayer::new(config).unwrap();
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(stats.entries_replayed > 0);

            let results = layer.query("project1", "table1", &[]).unwrap();
            assert!(!results.is_empty());
        }
    }
}
