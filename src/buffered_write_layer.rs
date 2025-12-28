use crate::config::{self, BufferConfig};
use crate::mem_buffer::{FlushableBucket, MemBuffer, MemBufferStats, estimate_batch_size, extract_min_timestamp};
use crate::wal::WalManager;
use arrow::array::RecordBatch;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

const MEMORY_OVERHEAD_MULTIPLIER: f64 = 1.2; // 20% overhead for DashMap, RwLock, schema refs

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub entries_replayed: u64,
    pub batches_recovered: u64,
    pub oldest_entry_timestamp: Option<i64>,
    pub newest_entry_timestamp: Option<i64>,
    pub recovery_duration_ms: u64,
    pub corrupted_entries_skipped: u64,
}

pub type DeltaWriteCallback = Arc<dyn Fn(String, String, Vec<RecordBatch>) -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

pub struct BufferedWriteLayer {
    wal: Arc<WalManager>,
    mem_buffer: Arc<MemBuffer>,
    shutdown: CancellationToken,
    delta_write_callback: Option<DeltaWriteCallback>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    flush_lock: Mutex<()>,
    reserved_bytes: AtomicUsize, // Memory reserved for in-flight writes
}

impl std::fmt::Debug for BufferedWriteLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedWriteLayer")
            .field("has_callback", &self.delta_write_callback.is_some())
            .finish()
    }
}

impl BufferedWriteLayer {
    /// Create a new BufferedWriteLayer using global config.
    pub fn new() -> anyhow::Result<Self> {
        let cfg = config::config();
        let wal = Arc::new(WalManager::new(cfg.core.walrus_data_dir.clone())?);
        let mem_buffer = Arc::new(MemBuffer::new());

        Ok(Self {
            wal,
            mem_buffer,
            shutdown: CancellationToken::new(),
            delta_write_callback: None,
            background_tasks: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
            reserved_bytes: AtomicUsize::new(0),
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

    fn buffer_config(&self) -> &BufferConfig {
        &config::config().buffer
    }

    fn max_memory_bytes(&self) -> usize {
        self.buffer_config().max_memory_mb() * 1024 * 1024
    }

    /// Total effective memory including reserved bytes for in-flight writes.
    fn effective_memory_bytes(&self) -> usize {
        self.mem_buffer.estimated_memory_bytes() + self.reserved_bytes.load(Ordering::Acquire)
    }

    fn is_memory_pressure(&self) -> bool {
        self.effective_memory_bytes() >= self.max_memory_bytes()
    }

    fn is_hard_limit_exceeded(&self) -> bool {
        // Hard limit at 120% of configured max to provide back-pressure
        // Use division to avoid overflow: current >= max + max/5
        let max_bytes = self.max_memory_bytes();
        self.effective_memory_bytes() >= max_bytes.saturating_add(max_bytes / 5)
    }

    /// Try to reserve memory atomically before a write.
    /// Returns estimated batch size on success, or error if hard limit would be exceeded.
    fn try_reserve_memory(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let batch_size: usize = batches.iter().map(estimate_batch_size).sum();
        let estimated_size = (batch_size as f64 * MEMORY_OVERHEAD_MULTIPLIER) as usize;

        let max_bytes = self.max_memory_bytes();
        let hard_limit = max_bytes.saturating_add(max_bytes / 5);

        loop {
            let current_reserved = self.reserved_bytes.load(Ordering::Acquire);
            let current_mem = self.mem_buffer.estimated_memory_bytes();
            let new_total = current_mem + current_reserved + estimated_size;

            if new_total > hard_limit {
                anyhow::bail!(
                    "Memory limit exceeded: {}MB + {}MB reservation > {}MB hard limit",
                    (current_mem + current_reserved) / (1024 * 1024),
                    estimated_size / (1024 * 1024),
                    hard_limit / (1024 * 1024)
                );
            }

            match self.reserved_bytes.compare_exchange(current_reserved, current_reserved + estimated_size, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(estimated_size),
                Err(_) => continue, // Retry on contention
            }
        }
    }

    fn release_reservation(&self, size: usize) {
        self.reserved_bytes.fetch_sub(size, Ordering::Release);
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub async fn insert(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        // Check memory pressure and trigger early flush if needed
        if self.is_memory_pressure() {
            warn!(
                "Memory pressure detected ({}MB >= {}MB), triggering early flush",
                self.effective_memory_bytes() / (1024 * 1024),
                self.buffer_config().max_memory_mb()
            );
            if let Err(e) = self.flush_completed_buckets().await {
                error!("Early flush due to memory pressure failed: {}", e);
            }
        }

        // Reserve memory atomically before writing - prevents race condition
        let reserved_size = self.try_reserve_memory(&batches)?;

        // Write WAL and MemBuffer, ensuring reservation is released regardless of outcome
        let result: anyhow::Result<()> = (|| {
            // Step 1: Write to WAL for durability
            self.wal.append_batch(project_id, table_name, &batches)?;

            // Step 2: Write to MemBuffer for fast queries
            let now = chrono::Utc::now().timestamp_micros();
            for batch in &batches {
                let timestamp_micros = extract_min_timestamp(batch).unwrap_or(now);
                self.mem_buffer.insert(project_id, table_name, batch.clone(), timestamp_micros)?;
            }

            Ok(())
        })();

        // Release reservation (memory is now tracked by MemBuffer)
        self.release_reservation(reserved_size);

        result?;
        debug!("BufferedWriteLayer insert complete: project={}, table={}", project_id, table_name);
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn recover_from_wal(&self) -> anyhow::Result<RecoveryStats> {
        let start = std::time::Instant::now();
        let retention_micros = (self.buffer_config().retention_mins() as i64) * 60 * 1_000_000;
        let cutoff = chrono::Utc::now().timestamp_micros() - retention_micros;

        info!("Starting WAL recovery, cutoff={}", cutoff);

        // Use checkpoint=true to advance the read cursor and consume entries.
        // Entries are replayed to MemBuffer and will be re-persisted on flush.
        let (entries, error_count) = self.wal.read_all_entries(Some(cutoff), true)?;

        let mut entries_replayed = 0u64;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;

        for (entry, batch) in entries {
            self.mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros)?;

            entries_replayed += 1;
            oldest_ts = Some(oldest_ts.map_or(entry.timestamp_micros, |ts| ts.min(entry.timestamp_micros)));
            newest_ts = Some(newest_ts.map_or(entry.timestamp_micros, |ts| ts.max(entry.timestamp_micros)));
        }

        let stats = RecoveryStats {
            entries_replayed,
            batches_recovered: entries_replayed,
            oldest_entry_timestamp: oldest_ts,
            newest_entry_timestamp: newest_ts,
            recovery_duration_ms: start.elapsed().as_millis() as u64,
            corrupted_entries_skipped: error_count as u64,
        };

        if stats.corrupted_entries_skipped > 0 {
            warn!(
                "WAL recovery complete: entries={}, skipped={}, duration={}ms",
                stats.entries_replayed, stats.corrupted_entries_skipped, stats.recovery_duration_ms
            );
        } else {
            info!(
                "WAL recovery complete: entries={}, duration={}ms",
                stats.entries_replayed, stats.recovery_duration_ms
            );
        }
        Ok(stats)
    }

    pub fn start_background_tasks(self: &Arc<Self>) {
        let this = Arc::clone(self);

        // Start flush task
        let flush_this = Arc::clone(&this);
        let flush_handle = tokio::spawn(async move {
            flush_this.run_flush_task().await;
        });

        // Start eviction task
        let eviction_this = Arc::clone(&this);
        let eviction_handle = tokio::spawn(async move {
            eviction_this.run_eviction_task().await;
        });

        // Store handles - use blocking lock since this runs at startup
        {
            let mut handles = this.background_tasks.blocking_lock();
            handles.push(flush_handle);
            handles.push(eviction_handle);
        }

        info!("BufferedWriteLayer background tasks started");
    }

    async fn run_flush_task(&self) {
        let flush_interval = Duration::from_secs(self.buffer_config().flush_interval_secs());

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
        let eviction_interval = Duration::from_secs(self.buffer_config().eviction_interval_secs());

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
        // Acquire flush lock to prevent concurrent flushes (e.g., during shutdown)
        let _flush_guard = self.flush_lock.lock().await;

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
                    // Order: drain MemBuffer FIRST, then checkpoint WAL
                    // If crash after drain but before checkpoint: WAL replays on recovery,
                    // may cause duplicates in Delta but no data loss (prefer duplicates over loss)
                    self.mem_buffer.drain_bucket(&bucket.project_id, &bucket.table_name, bucket.bucket_id);

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
        let retention_micros = (self.buffer_config().retention_mins() as i64) * 60 * 1_000_000;
        let cutoff = chrono::Utc::now().timestamp_micros() - retention_micros;

        let evicted = self.mem_buffer.evict_old_data(cutoff);
        if evicted > 0 {
            debug!("Evicted {} old buckets", evicted);
        }
        // WAL pruning is handled by checkpointing after successful Delta flush
    }

    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("BufferedWriteLayer shutdown initiated");

        // Signal background tasks to stop
        self.shutdown.cancel();

        // Compute dynamic timeout based on current buffer size
        let current_memory_mb = self.mem_buffer.estimated_memory_bytes() / (1024 * 1024);
        let task_timeout = self.buffer_config().compute_shutdown_timeout(current_memory_mb);
        debug!("Shutdown timeout: {:?} for {}MB buffer", task_timeout, current_memory_mb);

        // Wait for background tasks to complete (with timeout)
        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.background_tasks.lock().await;
            std::mem::take(&mut *guard)
        };

        for handle in handles {
            match tokio::time::timeout(task_timeout, handle).await {
                Ok(Ok(())) => debug!("Background task completed cleanly"),
                Ok(Err(e)) => warn!("Background task panicked: {}", e),
                Err(_) => warn!("Background task did not complete within timeout ({:?})", task_timeout),
            }
        }

        // Acquire flush lock - waits for any in-progress flush to complete
        let _flush_guard = self.flush_lock.lock().await;

        // Force flush all remaining data
        let all_buckets = self.mem_buffer.get_all_buckets();
        info!("Flushing {} remaining buckets on shutdown", all_buckets.len());

        for bucket in all_buckets {
            match self.flush_bucket(&bucket).await {
                Ok(()) => {
                    // Drain MemBuffer first, then checkpoint WAL (prefer duplicates over data loss)
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

    /// Check if a table exists in the memory buffer.
    pub fn has_table(&self, project_id: &str, table_name: &str) -> bool {
        self.mem_buffer.has_table(project_id, table_name)
    }

    /// Delete rows matching the predicate from the memory buffer.
    /// Returns the number of rows deleted.
    #[instrument(skip(self, predicate), fields(project_id, table_name))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>) -> datafusion::error::Result<u64> {
        self.mem_buffer.delete(project_id, table_name, predicate)
    }

    /// Update rows matching the predicate with new values in the memory buffer.
    /// Returns the number of rows updated.
    #[instrument(skip(self, predicate, assignments), fields(project_id, table_name))]
    pub fn update(
        &self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>, assignments: &[(String, datafusion::logical_expr::Expr)],
    ) -> datafusion::error::Result<u64> {
        self.mem_buffer.update(project_id, table_name, predicate, assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use tempfile::tempdir;

    fn init_test_config(wal_dir: &str) {
        // Set WAL dir before config init (tests run in same process, so first one wins)
        unsafe { std::env::set_var("WALRUS_DATA_DIR", wal_dir); }
        let _ = config::init_config();
    }

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
        init_test_config(&dir.path().to_string_lossy());

        let layer = BufferedWriteLayer::new().unwrap();
        let batch = create_test_batch();

        layer.insert("project1", "table1", vec![batch.clone()]).await.unwrap();

        let results = layer.query("project1", "table1", &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir().unwrap();
        init_test_config(&dir.path().to_string_lossy());

        // First instance - write data
        {
            let layer = BufferedWriteLayer::new().unwrap();
            let batch = create_test_batch();
            layer.insert("project1", "table1", vec![batch]).await.unwrap();
            // Give WAL time to sync (uses FsyncSchedule::Milliseconds(200))
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }

        // Second instance - recover from WAL
        {
            let layer = BufferedWriteLayer::new().unwrap();
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(stats.entries_replayed > 0, "Expected entries to be replayed from WAL");

            let results = layer.query("project1", "table1", &[]).unwrap();
            assert!(!results.is_empty(), "Expected results after WAL recovery");
        }
    }

    #[tokio::test]
    async fn test_memory_reservation() {
        let dir = tempdir().unwrap();
        init_test_config(&dir.path().to_string_lossy());

        let layer = BufferedWriteLayer::new().unwrap();

        // First insert should succeed
        let batch = create_test_batch();
        layer.insert("project1", "table1", vec![batch]).await.unwrap();

        // Verify reservation is released (should be 0 after successful insert)
        assert_eq!(layer.reserved_bytes.load(Ordering::Acquire), 0);
    }
}
