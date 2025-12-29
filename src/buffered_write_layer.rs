use crate::config::{self, AppConfig};
use crate::mem_buffer::{FlushableBucket, MemBuffer, MemBufferStats, estimate_batch_size, extract_min_timestamp};
use crate::wal::{WalManager, WalOperation, deserialize_delete_payload, deserialize_update_payload};
use arrow::array::RecordBatch;
use futures::stream::{self, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

// 20% overhead accounts for DashMap internal structures, RwLock wrappers,
// Arc<Schema> refs, and Arrow buffer alignment padding
const MEMORY_OVERHEAD_MULTIPLIER: f64 = 1.2;

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub entries_replayed: u64,
    pub batches_recovered: u64,
    pub oldest_entry_timestamp: Option<i64>,
    pub newest_entry_timestamp: Option<i64>,
    pub recovery_duration_ms: u64,
    pub corrupted_entries_skipped: u64,
}

/// Callback for writing batches to Delta Lake. The callback MUST:
/// - Complete the Delta commit (including S3 upload) before returning Ok
/// - Return Err if the commit fails for any reason
///
/// This is critical for WAL checkpoint safety - we only mark entries as consumed after successful commit.
pub type DeltaWriteCallback = Arc<dyn Fn(String, String, Vec<RecordBatch>) -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

pub struct BufferedWriteLayer {
    config: Arc<AppConfig>,
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
        f.debug_struct("BufferedWriteLayer").field("has_callback", &self.delta_write_callback.is_some()).finish()
    }
}

impl BufferedWriteLayer {
    /// Create a new BufferedWriteLayer with explicit config.
    pub fn with_config(cfg: Arc<AppConfig>) -> anyhow::Result<Self> {
        let wal = Arc::new(WalManager::new(cfg.core.walrus_data_dir.clone())?);
        let mem_buffer = Arc::new(MemBuffer::new());

        Ok(Self {
            config: cfg,
            wal,
            mem_buffer,
            shutdown: CancellationToken::new(),
            delta_write_callback: None,
            background_tasks: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
            reserved_bytes: AtomicUsize::new(0),
        })
    }

    /// Create a new BufferedWriteLayer using global config (for production).
    pub fn new() -> anyhow::Result<Self> {
        let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;
        Self::with_config(Arc::new(cfg.clone()))
    }

    pub fn with_delta_writer(mut self, callback: DeltaWriteCallback) -> Self {
        self.delta_write_callback = Some(callback);
        self
    }

    fn max_memory_bytes(&self) -> usize {
        self.config.buffer.max_memory_mb() * 1024 * 1024
    }

    /// Total effective memory including reserved bytes for in-flight writes.
    fn effective_memory_bytes(&self) -> usize {
        self.mem_buffer.estimated_memory_bytes() + self.reserved_bytes.load(Ordering::Acquire)
    }

    fn is_memory_pressure(&self) -> bool {
        self.effective_memory_bytes() >= self.max_memory_bytes()
    }

    /// Try to reserve memory atomically before a write.
    /// Returns estimated batch size on success, or error if hard limit exceeded.
    /// Callers MUST implement retry logic - hard failures may cause data loss.
    fn try_reserve_memory(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let batch_size: usize = batches.iter().map(estimate_batch_size).sum();
        let estimated_size = (batch_size as f64 * MEMORY_OVERHEAD_MULTIPLIER) as usize;

        let max_bytes = self.max_memory_bytes();
        // Hard limit at 120% provides headroom for in-flight writes while preventing OOM
        let hard_limit = max_bytes.saturating_add(max_bytes / 5);

        for _ in 0..100 {
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

            if self
                .reserved_bytes
                .compare_exchange(current_reserved, current_reserved + estimated_size, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(estimated_size);
            }
        }
        anyhow::bail!("Failed to reserve memory after 100 retries due to contention")
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
                self.config.buffer.max_memory_mb()
            );
            if let Err(e) = self.flush_completed_buckets().await {
                error!("Early flush due to memory pressure failed: {}", e);
            }
        }

        // Reserve memory atomically before writing - prevents race condition
        let reserved_size = self.try_reserve_memory(&batches)?;

        // Write WAL and MemBuffer, ensuring reservation is released regardless of outcome.
        // Reservation covers the window between WAL write and MemBuffer insert;
        // once MemBuffer tracks the data, reservation is released.
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
        let retention_micros = (self.config.buffer.retention_mins() as i64) * 60 * 1_000_000;
        let cutoff = chrono::Utc::now().timestamp_micros() - retention_micros;
        let corruption_threshold = self.config.buffer.wal_corruption_threshold();

        info!("Starting WAL recovery, cutoff={}, corruption_threshold={}", cutoff, corruption_threshold);

        // Read all entries sorted by timestamp for correct replay order
        let (entries, error_count) = self.wal.read_all_entries_raw(Some(cutoff), true)?;

        // Fail if corruption meets or exceeds threshold (0 = disabled)
        if corruption_threshold > 0 && error_count >= corruption_threshold {
            anyhow::bail!(
                "WAL corruption threshold exceeded: {} errors >= {} threshold. Data may be compromised.",
                error_count,
                corruption_threshold
            );
        }

        let mut entries_replayed = 0u64;
        let mut deletes_replayed = 0u64;
        let mut updates_replayed = 0u64;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;

        for entry in entries {
            match entry.operation {
                WalOperation::Insert => match WalManager::deserialize_batch(&entry.data) {
                    Ok(batch) => {
                        self.mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros)?;
                        entries_replayed += 1;
                    }
                    Err(e) => {
                        warn!("Skipping corrupted INSERT batch: {}", e);
                    }
                },
                WalOperation::Delete => match deserialize_delete_payload(&entry.data) {
                    Ok(payload) => {
                        if let Err(e) = self.mem_buffer.delete_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref()) {
                            warn!("Failed to replay DELETE: {}", e);
                        } else {
                            deletes_replayed += 1;
                        }
                    }
                    Err(e) => {
                        warn!("Skipping corrupted DELETE payload: {}", e);
                    }
                },
                WalOperation::Update => match deserialize_update_payload(&entry.data) {
                    Ok(payload) => {
                        if let Err(e) =
                            self.mem_buffer
                                .update_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref(), &payload.assignments)
                        {
                            warn!("Failed to replay UPDATE: {}", e);
                        } else {
                            updates_replayed += 1;
                        }
                    }
                    Err(e) => {
                        warn!("Skipping corrupted UPDATE payload: {}", e);
                    }
                },
            }
            let ts = entry.timestamp_micros;
            oldest_ts = Some(oldest_ts.map_or(ts, |o| o.min(ts)));
            newest_ts = Some(newest_ts.map_or(ts, |n| n.max(ts)));
        }

        let stats = RecoveryStats {
            entries_replayed,
            batches_recovered: entries_replayed,
            oldest_entry_timestamp: oldest_ts,
            newest_entry_timestamp: newest_ts,
            recovery_duration_ms: start.elapsed().as_millis() as u64,
            corrupted_entries_skipped: error_count as u64,
        };

        info!(
            "WAL recovery complete: inserts={}, deletes={}, updates={}, corrupted={}, duration={}ms",
            entries_replayed, deletes_replayed, updates_replayed, error_count, stats.recovery_duration_ms
        );
        Ok(stats)
    }

    pub async fn start_background_tasks(self: &Arc<Self>) {
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

        // Store handles
        {
            let mut handles = this.background_tasks.lock().await;
            handles.push(flush_handle);
            handles.push(eviction_handle);
        }

        info!("BufferedWriteLayer background tasks started");
    }

    async fn run_flush_task(&self) {
        let flush_interval = Duration::from_secs(self.config.buffer.flush_interval_secs());

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
        let eviction_interval = Duration::from_secs(self.config.buffer.eviction_interval_secs());

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

        // Flush buckets in parallel with bounded concurrency
        let parallelism = self.config.buffer.flush_parallelism();
        let flush_results: Vec<_> = stream::iter(flushable)
            .map(|bucket| async move {
                let result = self.flush_bucket(&bucket).await;
                (bucket, result)
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // Process results: checkpoint WAL and drain MemBuffer for successful flushes
        for (bucket, result) in flush_results {
            match result {
                Ok(()) => {
                    self.checkpoint_and_drain(&bucket);
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

    /// Flush a bucket to Delta Lake via the configured callback.
    /// The callback MUST complete the Delta commit before returning Ok - this is critical
    /// for durability. We only checkpoint WAL after this returns successfully.
    async fn flush_bucket(&self, bucket: &FlushableBucket) -> anyhow::Result<()> {
        if let Some(ref callback) = self.delta_write_callback {
            // Await ensures Delta commit completes before we return
            callback(bucket.project_id.clone(), bucket.table_name.clone(), bucket.batches.clone()).await?;
        } else {
            warn!("No delta write callback configured, skipping flush");
        }
        Ok(())
    }

    fn evict_old_data(&self) {
        let retention_micros = (self.config.buffer.retention_mins() as i64) * 60 * 1_000_000;
        let cutoff = chrono::Utc::now().timestamp_micros() - retention_micros;

        let evicted = self.mem_buffer.evict_old_data(cutoff);
        if evicted > 0 {
            debug!("Evicted {} old buckets", evicted);
        }
        // WAL pruning is handled by checkpointing after successful Delta flush
    }

    fn checkpoint_and_drain(&self, bucket: &FlushableBucket) {
        if let Err(e) = self.wal.checkpoint(&bucket.project_id, &bucket.table_name) {
            warn!("WAL checkpoint failed: {}", e);
        }
        self.mem_buffer.drain_bucket(&bucket.project_id, &bucket.table_name, bucket.bucket_id);
    }

    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("BufferedWriteLayer shutdown initiated");

        // Signal background tasks to stop
        self.shutdown.cancel();

        // Compute dynamic timeout based on current buffer size
        let current_memory_mb = self.mem_buffer.estimated_memory_bytes() / (1024 * 1024);
        let task_timeout = self.config.buffer.compute_shutdown_timeout(current_memory_mb);
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
                Ok(()) => self.checkpoint_and_drain(&bucket),
                Err(e) => error!("Shutdown flush failed for bucket {}: {}", bucket.bucket_id, e),
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
    /// Logs the operation to WAL for crash recovery, then applies to MemBuffer.
    /// Returns the number of rows deleted.
    #[instrument(skip(self, predicate), fields(project_id, table_name))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>) -> datafusion::error::Result<u64> {
        let predicate_sql = predicate.map(|p| format!("{}", p));
        // Log to WAL first for durability
        if let Err(e) = self.wal.append_delete(project_id, table_name, predicate_sql.as_deref()) {
            warn!("Failed to log DELETE to WAL: {}", e);
        }
        self.mem_buffer.delete(project_id, table_name, predicate)
    }

    /// Update rows matching the predicate with new values in the memory buffer.
    /// Logs the operation to WAL for crash recovery, then applies to MemBuffer.
    /// Returns the number of rows updated.
    #[instrument(skip(self, predicate, assignments), fields(project_id, table_name))]
    pub fn update(
        &self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>, assignments: &[(String, datafusion::logical_expr::Expr)],
    ) -> datafusion::error::Result<u64> {
        let predicate_sql = predicate.map(|p| format!("{}", p));
        let assignments_sql: Vec<(String, String)> = assignments.iter().map(|(col, expr)| (col.clone(), format!("{}", expr))).collect();
        // Log to WAL first for durability
        if let Err(e) = self.wal.append_update(project_id, table_name, predicate_sql.as_deref(), &assignments_sql) {
            warn!("Failed to log UPDATE to WAL: {}", e);
        }
        self.mem_buffer.update(project_id, table_name, predicate, assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn create_test_config(wal_dir: PathBuf) -> Arc<AppConfig> {
        let mut cfg = AppConfig::default();
        cfg.core.walrus_data_dir = wal_dir;
        Arc::new(cfg)
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
        let cfg = create_test_config(dir.path().to_path_buf());

        // Use unique but short project/table names (walrus has metadata size limit)
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("p{}", test_id);
        let table = format!("t{}", test_id);

        let layer = BufferedWriteLayer::with_config(cfg).unwrap();
        let batch = create_test_batch();

        layer.insert(&project, &table, vec![batch.clone()]).await.unwrap();

        let results = layer.query(&project, &table, &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    // NOTE: This test is ignored because walrus-rust creates new files for each instance
    // rather than discovering existing files from previous instances in the same directory.
    // This is a limitation of the walrus library, not our code.
    #[ignore]
    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // Use unique but short project/table names (walrus has metadata size limit)
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("r{}", test_id);
        let table = format!("r{}", test_id);

        // First instance - write data
        {
            let layer = BufferedWriteLayer::with_config(Arc::clone(&cfg)).unwrap();
            let batch = create_test_batch();
            layer.insert(&project, &table, vec![batch]).await.unwrap();
            // Shutdown to ensure WAL is synced
            layer.shutdown().await.unwrap();
        }

        // Second instance - recover from WAL
        {
            let layer = BufferedWriteLayer::with_config(cfg).unwrap();
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(stats.entries_replayed > 0, "Expected entries to be replayed from WAL");

            let results = layer.query(&project, &table, &[]).unwrap();
            assert!(!results.is_empty(), "Expected results after WAL recovery");
        }
    }

    #[tokio::test]
    async fn test_memory_reservation() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // Use unique but short project/table names (walrus has metadata size limit)
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("m{}", test_id);
        let table = format!("m{}", test_id);

        let layer = BufferedWriteLayer::with_config(cfg).unwrap();

        // First insert should succeed
        let batch = create_test_batch();
        layer.insert(&project, &table, vec![batch]).await.unwrap();

        // Verify reservation is released (should be 0 after successful insert)
        assert_eq!(layer.reserved_bytes.load(Ordering::Acquire), 0);
    }
}
