use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::array::RecordBatch;
use futures::stream::{self, StreamExt};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    config::AppConfig,
    mem_buffer::{FlushableBucket, MemBuffer, MemBufferStats, estimate_batch_size, extract_min_timestamp},
    wal::{WalEntry, WalManager, WalOperation, deserialize_delete_payload, deserialize_update_payload},
};

// Reservation-side scale factor applied to `estimate_batch_size()` to
// account for what that estimator doesn't already cover: per-batch Vec
// headers, DashMap node overhead, and allocator fragmentation.
//
// `estimate_batch_size()` already uses `batch.get_array_memory_size()`,
// which captures all underlying Arrow buffers including 64-byte alignment
// padding and validity bitmaps. Empirical measurement (bench/multiplier_bench.py,
// 2026-05-17, 4.7k inserts, 16 writers, single-project) shows MemBuffer
// `estimated_bytes` tracks within ~10–15% of the actual marginal heap
// growth — RSS growth is dominated by fixed costs (walrus mmaps, Foyer,
// tantivy) which `max_memory_bytes()` already subtracts out separately.
// 1.15x gives a safety margin for allocator fragmentation; the previous
// 1.5x value was an unmeasured guess that wasted ~23% of the configured
// `max_memory_mb` budget.
const MEMORY_OVERHEAD_MULTIPLIER: f64 = 1.15;
/// Hard limit = `max_bytes + max_bytes / HARD_LIMIT_HEADROOM_DIVISOR` →
/// 120% of the configured budget, leaving headroom for in-flight writes
/// while preventing unbounded growth. Named "divisor" (not "multiplier")
/// because the math is `/ N`; `5` → +20%.
const HARD_LIMIT_HEADROOM_DIVISOR: usize = 5;
/// Maximum CAS retry attempts before failing
const MAX_CAS_RETRIES: u32 = 100;
/// Base backoff delay in microseconds for CAS retries
const CAS_BACKOFF_BASE_MICROS: u64 = 1;
/// Maximum backoff exponent (caps delay at ~1ms)
const CAS_BACKOFF_MAX_EXPONENT: u32 = 10;

/// Persist a corrupted/unreplayable WAL entry to `{wal_dir}/quarantine/`
/// so ops can post-mortem without blocking recovery. Best-effort: write
/// failures are logged but never propagated — quarantine is observability,
/// not durability.
/// Write raw bytes to a path with owner-only (0600) permissions on Unix.
/// On Windows we fall back to plain write — ACL hardening there is out of
/// scope for this helper.
fn write_owner_only(path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::{io::Write, os::unix::fs::OpenOptionsExt};
        let mut f = std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o600).open(path)?;
        f.write_all(contents)?;
        f.sync_all()
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)
    }
}

fn quarantine_entry(quarantine_dir: &std::path::Path, entry: &WalEntry, kind: &str, reason: &str) {
    if let Err(e) = std::fs::create_dir_all(quarantine_dir) {
        error!("Failed to create WAL quarantine dir {:?}: {}", quarantine_dir, e);
        return;
    }
    // Sanitize topic for filename: project:table can contain '/' or other chars
    let topic = format!("{}__{}", entry.project_id, entry.table_name).replace(['/', '\\', ':', '\0'], "_");
    let filename = format!("{}_{}_{}.bin", entry.timestamp_micros, kind, topic);
    let path = quarantine_dir.join(&filename);
    // Quarantine files contain raw user data that failed to deserialize —
    // write with mode 0600 so they're not world-readable on shared hosts.
    if let Err(e) = write_owner_only(&path, &entry.data) {
        error!("Failed to write quarantine file {:?}: {}", path, e);
        return;
    }
    // Sidecar metadata file for human inspection
    let meta_path = path.with_extension("meta");
    let meta = format!(
        "ts_micros={}\nproject_id={}\ntable_name={}\noperation={:?}\nkind={}\nreason={}\nbytes={}\n",
        entry.timestamp_micros,
        entry.project_id,
        entry.table_name,
        entry.operation,
        kind,
        reason,
        entry.data.len()
    );
    if let Err(e) = write_owner_only(&meta_path, meta.as_bytes()) {
        error!("Failed to write quarantine meta {:?}: {}", meta_path, e);
    }
    error!("Quarantined WAL entry to {:?} (kind={}, bytes={})", path, kind, entry.data.len());
    crate::metrics::record_wal_corruption();
}

/// Operator-visible snapshot of the BufferedWriteLayer state. Returned by
/// `snapshot_stats()` and rendered as rows by `timefusion.stats()`.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub mem_project_count:      usize,
    pub mem_total_buckets:      usize,
    pub mem_total_rows:         usize,
    pub mem_total_batches:      usize,
    pub mem_estimated_bytes:    usize,
    pub reserved_bytes:         usize,
    pub max_memory_bytes:       usize,
    pub pressure_pct:           u32,
    pub wal_files:              usize,
    pub wal_disk_bytes:         u64,
    pub wal_shards_per_topic:   usize,
    pub wal_known_topics:       usize,
    pub bucket_duration_micros: i64,
    /// Age of the oldest bucket in MemBuffer (seconds, computed from
    /// `now - min(bucket.min_timestamp)`). None when MemBuffer is empty.
    /// Alerting target: alert at > 2× `flush_interval_secs`.
    pub oldest_bucket_age_secs: Option<u64>,
}

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub entries_replayed:          u64,
    pub batches_recovered:         u64,
    pub oldest_entry_timestamp:    Option<i64>,
    pub newest_entry_timestamp:    Option<i64>,
    pub recovery_duration_ms:      u64,
    pub corrupted_entries_skipped: u64,
}

#[derive(Debug, Default)]
pub struct FlushStats {
    pub buckets_flushed: u64,
    pub buckets_failed:  u64,
    pub total_rows:      u64,
}

/// Callback for writing batches to Delta Lake. The callback MUST:
/// - Complete the Delta commit (including S3 upload) before returning Ok
/// - Return Err if the commit fails for any reason
/// - Return the URIs of files added by this commit (used by sidecar indexers
///   so a tantivy entry can later be GC'd when its covering parquet files
///   are compacted away)
///
/// This is critical for WAL checkpoint safety - we only mark entries as consumed after successful commit.
/// Per-shard walrus watermark snapshot at bucket-seal time. `None` for shards
/// the bucket never wrote to. The callback writes this into the Delta commit
/// metadata so a crash-mid-flush can derive the cursor from Delta on restart.
pub type DeltaWatermark = Vec<Option<walrus_rust::WalPosition>>;

pub type DeltaWriteCallback = Arc<
    dyn Fn(String, String, Vec<RecordBatch>, DeltaWatermark) -> futures::future::BoxFuture<'static, anyhow::Result<Vec<String>>>
        + Send
        + Sync,
>;

/// Optional callback invoked AFTER a successful Delta commit. Receives the
/// `(project_id, table_name, batches, added_file_uris)` and is responsible
/// for building and uploading any sidecar index. The `added_file_uris` are
/// the parquet files Delta wrote for this batch; the indexer records them in
/// the manifest entry so that later compaction GC can determine whether the
/// index still covers live data. Failures are logged but DO NOT fail the
/// flush — the index is an optimization.
pub type TantivyIndexCallback =
    Arc<dyn Fn(String, String, Vec<RecordBatch>, Vec<String>) -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

pub struct BufferedWriteLayer {
    config:                 Arc<AppConfig>,
    wal:                    Arc<WalManager>,
    mem_buffer:             Arc<MemBuffer>,
    shutdown:               CancellationToken,
    delta_write_callback:   Option<DeltaWriteCallback>,
    tantivy_index_callback: Option<TantivyIndexCallback>,
    background_tasks:       Mutex<Vec<JoinHandle<()>>>,
    flush_lock:             Mutex<()>,
    reserved_bytes:         AtomicUsize, // Memory reserved for in-flight writes
    pressure_notify:        Arc<Notify>, // Wakes flush task when pressure threshold crossed
    // Required for WAL replay of UPDATE/DELETE whose SQL references UDFs.
    function_registry:      Arc<crate::functions::FnRegistry>,
}

impl std::fmt::Debug for BufferedWriteLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferedWriteLayer").field("has_callback", &self.delta_write_callback.is_some()).finish()
    }
}

impl BufferedWriteLayer {
    /// Create a new BufferedWriteLayer with explicit config and a function
    /// registry. The registry MUST be the same one the runtime SessionContext
    /// uses so WAL replay can resolve UDFs in stored UPDATE/DELETE SQL.
    pub fn with_config(cfg: Arc<AppConfig>, function_registry: Arc<crate::functions::FnRegistry>) -> anyhow::Result<Self> {
        let wal = Arc::new(WalManager::with_fsync_mode_and_shards(
            cfg.core.wal_dir(),
            cfg.buffer.wal_fsync_mode(),
            cfg.buffer.wal_shards_per_topic(),
        )?);
        // Apply configurable bucket duration before MemBuffer reads it.
        crate::mem_buffer::set_bucket_duration_micros((cfg.buffer.bucket_duration_secs() as i64) * 1_000_000);
        // Text-index cache budget: 25% of the MemBuffer memory budget.
        // Rationale: indexed text is roughly 1.5–2x raw text in postings,
        // and indexed columns are a fraction of total row bytes. 25% is a
        // soft ceiling — LRU drops oldest entries before this is exceeded.
        let text_index_max_bytes = (cfg.buffer.max_memory_mb() / 4).max(16) * 1024 * 1024;
        let mem_buffer = Arc::new(MemBuffer::new_with_max_index_bytes_and_shards(
            text_index_max_bytes,
            wal.shards_per_topic(),
        ));

        Ok(Self {
            config: cfg,
            wal,
            mem_buffer,
            shutdown: CancellationToken::new(),
            delta_write_callback: None,
            tantivy_index_callback: None,
            background_tasks: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
            reserved_bytes: AtomicUsize::new(0),
            pressure_notify: Arc::new(Notify::new()),
            function_registry,
        })
    }

    pub fn with_delta_writer(mut self, callback: DeltaWriteCallback) -> Self {
        self.delta_write_callback = Some(callback);
        self
    }

    pub fn with_tantivy_indexer(mut self, callback: TantivyIndexCallback) -> Self {
        self.tantivy_index_callback = Some(callback);
        self
    }

    /// Effective MemBuffer budget after subtracting other long-lived allocations
    /// the process holds (Foyer in-memory caches, peak tantivy writer heap).
    /// Without this subtraction the configured `max_memory_mb` looks satisfied
    /// while RSS quietly grows past it.
    fn max_memory_bytes(&self) -> usize {
        let configured = self.config.buffer.max_memory_mb() * 1024 * 1024;
        let foyer = if self.config.cache.is_disabled() {
            0
        } else {
            self.config.cache.memory_size_bytes() + self.config.cache.metadata_memory_size_bytes()
        };
        // Each in-flight flush may spawn one tantivy writer with WRITER_HEAP_BYTES.
        // Always reserve the peak when there's at least one indexed table — cheaper
        // to slightly over-reserve than to OOM on a flush burst.
        let tantivy_peak = if self.config.tantivy.indexed_tables().is_empty() {
            0
        } else {
            crate::tantivy_index::builder::WRITER_HEAP_BYTES * self.config.buffer.flush_parallelism()
        };
        let reserved = foyer.saturating_add(tantivy_peak);
        // Always leave at least a 64MB working budget for MemBuffer so a
        // misconfigured cache/tantivy combo can't drive the budget to zero.
        const MIN_BUFFER_BYTES: usize = 64 * 1024 * 1024;
        configured.saturating_sub(reserved).max(MIN_BUFFER_BYTES)
    }

    /// MemBuffer fill ratio (0..=100). Used by ingress to emit soft
    /// backpressure before hitting the hard reservation limit.
    pub fn pressure_pct(&self) -> u32 {
        let max = self.max_memory_bytes().max(1);
        ((self.effective_memory_bytes() as u128 * 100 / max as u128).min(100)) as u32
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
    /// Uses exponential backoff to reduce CPU thrashing under contention.
    async fn try_reserve_memory(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let batch_size: usize = batches.iter().map(estimate_batch_size).sum();
        let estimated_size = (batch_size as f64 * MEMORY_OVERHEAD_MULTIPLIER) as usize;

        let max_bytes = self.max_memory_bytes();
        let hard_limit = max_bytes.saturating_add(max_bytes / HARD_LIMIT_HEADROOM_DIVISOR);

        for attempt in 0..MAX_CAS_RETRIES {
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
                // If post-reservation we crossed the configured pressure threshold,
                // wake the flush task so it can drain completed buckets without
                // waiting for the next tick.
                let threshold = self.config.buffer.pressure_flush_pct();
                let new_total_bytes = current_mem + current_reserved + estimated_size;
                let pct = ((new_total_bytes as u128 * 100 / max_bytes.max(1) as u128).min(100)) as u32;
                if pct >= threshold {
                    self.pressure_notify.notify_one();
                }
                return Ok(estimated_size);
            }

            if attempt < 5 {
                std::hint::spin_loop();
            } else {
                let backoff_micros = CAS_BACKOFF_BASE_MICROS << attempt.min(CAS_BACKOFF_MAX_EXPONENT);
                tokio::time::sleep(std::time::Duration::from_micros(backoff_micros)).await;
            }
        }
        anyhow::bail!("Failed to reserve memory after {} retries due to contention", MAX_CAS_RETRIES)
    }

    fn release_reservation(&self, size: usize) {
        self.reserved_bytes.fetch_sub(size, Ordering::Release);
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub async fn insert(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        // Check memory pressure and trigger early flush if needed.
        // We use `flush_all_now` (not `flush_completed_buckets`) here because
        // under memory pressure the data consuming the budget is almost
        // always the *current* bucket — `flush_completed_buckets` only
        // drains buckets older than `bucket_duration_secs`, which leaves
        // the current-bucket pressure unrelieved and the next insert
        // still fails. `flush_all_now` includes the current bucket and
        // matches what an operator would do manually in this state.
        if self.is_memory_pressure() {
            warn!(
                "Memory pressure detected ({}MB >= {}MB), triggering early flush_all_now",
                self.effective_memory_bytes() / (1024 * 1024),
                self.config.buffer.max_memory_mb()
            );
            if let Err(e) = self.flush_all_now().await {
                error!("Early flush due to memory pressure failed: {}", e);
            }
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Reserve memory atomically before writing - prevents race condition
        let reserved_size = self.try_reserve_memory(&batches).await?;

        // No per-topic mutex needed: WAL now shards each (project, table)
        // across N walrus collections via `WalManager::pick_shard`, so
        // concurrent appends to the same topic land in different shards and
        // walrus's single-writer-per-collection invariant is never contended.
        // MemBuffer is DashMap-based and already concurrent-safe.
        let result: anyhow::Result<()> = (|| {
            // Step 1: Write to WAL for durability (sharded, parallel-safe).
            // `append_batch` returns `(shard, count)`; record both against the
            // MemBuffer bucket so `advance_by_counts` on flush can move the
            // cursor by exactly this much per shard (and not past entries
            // belonging to the open follow-on bucket).
            let (shard, _count) = self.wal.append_batch(project_id, table_name, &batches)?;

            // Best-effort post-append snapshot; failure just omits this
            // bucket's watermark contribution for this shard.
            let post_append_position = self.wal.current_position_for_shard(project_id, table_name, shard).ok();

            // Step 2: Write to MemBuffer for fast queries and attribute one
            // WAL entry per batch to its destination bucket (batches in one
            // append all land on the same shard, but may straddle bucket
            // boundaries if their timestamps differ).
            let now = crate::clock::now_micros();
            for batch in &batches {
                let timestamp_micros = extract_min_timestamp(batch).unwrap_or(now);
                self.mem_buffer.insert(project_id, table_name, batch.clone(), timestamp_micros)?;
                self.mem_buffer.record_wal_append(
                    project_id,
                    table_name,
                    timestamp_micros,
                    shard,
                    1,
                    post_append_position,
                );
            }

            Ok(())
        })();

        // Release reservation (memory is now tracked by MemBuffer)
        self.release_reservation(reserved_size);

        match &result {
            Ok(()) => crate::metrics::record_insert(project_id, table_name, row_count as u64),
            Err(_) => crate::metrics::record_ingest_error(project_id, table_name),
        }
        result?;

        // Immediate flush mode: flush after every insert
        if self.config.buffer.flush_immediately() {
            self.flush_all_now().await?;
        }

        debug!("BufferedWriteLayer insert complete: project={}, table={}", project_id, table_name);
        Ok(())
    }

    /// Exposed so startup can run `derive_wal_cursors_from_delta` on the same
    /// `WalManager` instance the layer owns — no second `Walrus` handle, no
    /// shadow state.
    pub fn wal(&self) -> &Arc<WalManager> {
        &self.wal
    }

    #[instrument(skip(self))]
    pub async fn recover_from_wal(&self) -> anyhow::Result<RecoveryStats> {
        let start = std::time::Instant::now();
        let retention_micros = (self.config.buffer.retention_mins() as i64) * 60 * 1_000_000;
        let cutoff = crate::clock::now_micros() - retention_micros;
        let corruption_threshold = self.config.buffer.wal_corruption_threshold();

        info!("Starting WAL recovery, cutoff={}, corruption_threshold={}", cutoff, corruption_threshold);

        // Stream entries one at a time and replay directly into MemBuffer.
        // Bounded recovery memory: O(1) entries in flight rather than
        // O(retention_window × throughput) (potentially GiBs).
        let mut entries_replayed = 0u64;
        let mut deletes_replayed = 0u64;
        let mut updates_replayed = 0u64;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;
        let mem_buffer = &self.mem_buffer;

        let quarantine_dir = self.wal.data_dir().join("quarantine");
        let registry_ref: Option<&crate::functions::FnRegistry> = Some(self.function_registry.as_ref());
        let (_total, error_count) = self.wal.for_each_entry(Some(cutoff), true, |entry| {
            match entry.operation {
                WalOperation::Insert => match WalManager::deserialize_batch(&entry.data, &entry.table_name) {
                    Ok(batch) => {
                        if batch.num_rows() == 0 {
                            warn!("Skipping empty batch during WAL recovery for {}.{}", entry.project_id, entry.table_name);
                            return;
                        }
                        match mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros) {
                            Ok(()) => entries_replayed += 1,
                            Err(e) => {
                                error!("WAL REPLAY FAILED: incompatible INSERT for {}.{}: {}", entry.project_id, entry.table_name, e);
                                quarantine_entry(&quarantine_dir, &entry, "insert_incompatible", &e.to_string());
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "WAL CORRUPTION: undeserializable INSERT batch for {}.{}: {}",
                            entry.project_id, entry.table_name, e
                        );
                        quarantine_entry(&quarantine_dir, &entry, "insert_corrupt", &e.to_string());
                    }
                },
                WalOperation::Delete => match deserialize_delete_payload(&entry.data) {
                    Ok(payload) => {
                        if let Err(e) = mem_buffer.delete_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref(), registry_ref) {
                            error!("WAL REPLAY FAILED: DELETE for {}.{}: {}", entry.project_id, entry.table_name, e);
                            quarantine_entry(&quarantine_dir, &entry, "delete_replay_failed", &e.to_string());
                        } else {
                            deletes_replayed += 1;
                        }
                    }
                    Err(e) => {
                        error!(
                            "WAL CORRUPTION: undeserializable DELETE payload for {}.{}: {}",
                            entry.project_id, entry.table_name, e
                        );
                        quarantine_entry(&quarantine_dir, &entry, "delete_corrupt", &e.to_string());
                    }
                },
                WalOperation::Update => match deserialize_update_payload(&entry.data) {
                    Ok(payload) => {
                        if let Err(e) = mem_buffer.update_by_sql(
                            &entry.project_id,
                            &entry.table_name,
                            payload.predicate_sql.as_deref(),
                            &payload.assignments,
                            registry_ref,
                        ) {
                            error!("WAL REPLAY FAILED: UPDATE for {}.{}: {}", entry.project_id, entry.table_name, e);
                            quarantine_entry(&quarantine_dir, &entry, "update_replay_failed", &e.to_string());
                        } else {
                            updates_replayed += 1;
                        }
                    }
                    Err(e) => {
                        error!(
                            "WAL CORRUPTION: undeserializable UPDATE payload for {}.{}: {}",
                            entry.project_id, entry.table_name, e
                        );
                        quarantine_entry(&quarantine_dir, &entry, "update_corrupt", &e.to_string());
                    }
                },
            }
            let ts = entry.timestamp_micros;
            oldest_ts = Some(oldest_ts.map_or(ts, |o| o.min(ts)));
            newest_ts = Some(newest_ts.map_or(ts, |n| n.max(ts)));
        })?;

        // Fail if corruption meets or exceeds threshold (0 = disabled).
        if corruption_threshold > 0 && error_count >= corruption_threshold {
            anyhow::bail!(
                "WAL corruption threshold exceeded: {} errors >= {} threshold. Data may be compromised.",
                error_count,
                corruption_threshold
            );
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
            let trigger = tokio::select! {
                _ = tokio::time::sleep(flush_interval) => "timer",
                _ = self.pressure_notify.notified() => "pressure",
                _ = self.shutdown.cancelled() => {
                    info!("Flush task shutting down");
                    break;
                }
            };

            if trigger == "pressure" {
                debug!(
                    "Pressure-triggered flush at {}% (threshold {}%)",
                    self.pressure_pct(),
                    self.config.buffer.pressure_flush_pct()
                );
            }

            if let Err(e) = self.flush_completed_buckets().await {
                crate::metrics::record_flush(false);
                error!("Flush task error: {}", e);
            }
            // WAL monitoring: check file accumulation
            let (file_count, total_bytes) = self.wal.wal_stats();
            if trigger == "timer" {
                info!("WAL stats: {} files, {}MB", file_count, total_bytes / (1024 * 1024));
            }
            let max_files = self.config.buffer.wal_max_file_count();
            if max_files > 0 && file_count > max_files {
                warn!("WAL file count {} exceeds threshold {}, triggering emergency flush", file_count, max_files);
                if let Err(e) = self.flush_all_now().await {
                    error!("Emergency WAL flush failed: {}", e);
                }
            }
        }
    }

    async fn run_eviction_task(&self) {
        let eviction_interval = Duration::from_secs(self.config.buffer.eviction_interval_secs());

        loop {
            tokio::select! {
                _ = tokio::time::sleep(eviction_interval) => {
                    // The "eviction" task no longer evicts unconditionally —
                    // doing so could drop a bucket from MemBuffer before it
                    // ever reached Delta (silent data loss when flush was
                    // slow or misconfigured). Instead, we drive an extra
                    // flush attempt: successful flushes call
                    // `checkpoint_and_drain` which removes the bucket from
                    // MemBuffer; failed flushes leave the bucket so the next
                    // cycle retries. The hard memory limit on
                    // `BufferedWriteLayer::try_reserve_memory` is the
                    // backpressure if flushes never recover.
                    if let Err(e) = self.flush_completed_buckets().await {
                        error!("Eviction-task flush failed: {}", e);
                    }
                    self.evict_drained_metadata();
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

        debug!("Flushing {} buckets to Delta", flushable.len());

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
                    crate::metrics::record_flush(true);
                    debug!(
                        "Flushed bucket: project={}, table={}, bucket_id={}, rows={}",
                        bucket.project_id, bucket.table_name, bucket.bucket_id, bucket.row_count
                    );
                }
                Err(e) => {
                    crate::metrics::record_flush(false);
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
        let added_files = if let Some(ref callback) = self.delta_write_callback {
            // Await ensures Delta commit completes before we return. The
            // wal_positions snapshot becomes the watermark recorded in
            // commit metadata for exact-once crash recovery.
            callback(
                bucket.project_id.clone(),
                bucket.table_name.clone(),
                bucket.batches.clone(),
                bucket.wal_positions.clone(),
            )
            .await?
        } else {
            warn!("No delta write callback configured, skipping flush");
            Vec::new()
        };
        // Sidecar tantivy index — best-effort, never fails the flush.
        // We still count the failure so ops can alert on accumulating index
        // drift (silent UDF-fallback degradation is otherwise invisible).
        if let Some(ref idx_cb) = self.tantivy_index_callback
            && let Err(e) = idx_cb(bucket.project_id.clone(), bucket.table_name.clone(), bucket.batches.clone(), added_files).await
        {
            crate::metrics::record_tantivy_build_failure();
            warn!(
                "Tantivy index build failed (non-fatal): project={}, table={}, bucket_id={}: {}",
                bucket.project_id, bucket.table_name, bucket.bucket_id, e
            );
        }
        Ok(())
    }

    /// Sanity check: warn loudly if any bucket has aged past retention
    /// without being flushed. This used to silently `drain_bucket` such
    /// buckets — that lost data. Now we keep them and surface the
    /// condition so an operator can see flushes are stuck.
    fn evict_drained_metadata(&self) {
        let retention_micros = (self.config.buffer.retention_mins() as i64) * 60 * 1_000_000;
        let cutoff = crate::clock::now_micros() - retention_micros;
        let stuck = self.mem_buffer.count_buckets_with_max_ts_before(cutoff);
        if stuck > 0 {
            warn!(
                "{} bucket(s) older than retention ({}min) still in MemBuffer — flush is failing or backed up",
                stuck,
                self.config.buffer.retention_mins()
            );
        }
    }

    fn checkpoint_and_drain(&self, bucket: &FlushableBucket) {
        if let Err(e) = self.wal.advance_by_counts(&bucket.project_id, &bucket.table_name, &bucket.wal_shard_counts) {
            warn!("WAL advance_by_counts failed: {}", e);
        }
        self.mem_buffer.drain_bucket(&bucket.project_id, &bucket.table_name, bucket.bucket_id);
    }

    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        info!("BufferedWriteLayer shutdown initiated");

        // Signal background tasks to stop
        self.shutdown.cancel();
        let task_timeout = self.config.buffer.compute_shutdown_timeout();
        debug!("Shutdown timeout: {:?}", task_timeout);

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

    /// Acquire the flush mutex for the duration of `f`. Pauses the periodic
    /// flush task so a Delta-mutating maintenance op (e.g. `OPTIMIZE`) can
    /// commit without racing the flush callback. Don't hold this across S3
    /// roundtrips longer than your insert SLO can tolerate — while held,
    /// `flush_completed_buckets` blocks and new rows accumulate in
    /// MemBuffer.
    pub async fn with_flush_paused<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _guard = self.flush_lock.lock().await;
        f().await
    }

    /// Force flush all buffered data to Delta immediately.
    pub async fn flush_all_now(&self) -> anyhow::Result<FlushStats> {
        let _flush_guard = self.flush_lock.lock().await;
        let all_buckets = self.mem_buffer.get_all_buckets();
        let mut stats = FlushStats {
            total_rows: all_buckets.iter().map(|b| b.row_count as u64).sum(),
            ..Default::default()
        };

        for bucket in all_buckets {
            match self.flush_bucket(&bucket).await {
                Ok(()) => {
                    self.checkpoint_and_drain(&bucket);
                    stats.buckets_flushed += 1;
                }
                Err(e) => {
                    error!("flush_all_now: failed bucket {}: {}", bucket.bucket_id, e);
                    stats.buckets_failed += 1;
                }
            }
        }
        Ok(stats)
    }

    /// Check if buffer is empty (all data flushed).
    pub fn is_empty(&self) -> bool {
        self.mem_buffer.get_stats().total_rows == 0
    }

    /// Direct accessor for the underlying `MemBuffer`. Used by the SQL
    /// routing layer to call `search_text_match` (the in-memory tantivy
    /// prefilter for buckets that haven't flushed yet).
    pub fn mem_buffer(&self) -> &MemBuffer {
        &self.mem_buffer
    }

    pub fn get_stats(&self) -> MemBufferStats {
        self.mem_buffer.get_stats()
    }

    /// Snapshot every interesting internal counter for operator visibility.
    /// Backs `SELECT * FROM timefusion.stats()`. All fields are point-in-time;
    /// no locks held across the snapshot — callers see a consistent view of
    /// each individual counter but not necessarily across counters.
    pub fn snapshot_stats(&self) -> StatsSnapshot {
        let mem = self.mem_buffer.get_stats();
        let (wal_files, wal_bytes) = self.wal.wal_stats();
        let oldest_bucket_age_secs = mem.oldest_bucket_micros.map(|ts| {
            let now = crate::clock::now_micros();
            ((now - ts).max(0) / 1_000_000) as u64
        });
        StatsSnapshot {
            mem_project_count: mem.project_count,
            mem_total_buckets: mem.total_buckets,
            mem_total_rows: mem.total_rows,
            mem_total_batches: mem.total_batches,
            mem_estimated_bytes: mem.estimated_memory_bytes,
            reserved_bytes: self.reserved_bytes.load(Ordering::Acquire),
            max_memory_bytes: self.max_memory_bytes(),
            pressure_pct: self.pressure_pct(),
            wal_files,
            wal_disk_bytes: wal_bytes,
            wal_shards_per_topic: self.wal.shards_per_topic(),
            wal_known_topics: self.wal.known_topic_count(),
            bucket_duration_micros: crate::mem_buffer::bucket_duration_micros(),
            oldest_bucket_age_secs,
        }
    }

    pub fn get_oldest_timestamp(&self, project_id: &str, table_name: &str) -> Option<i64> {
        self.mem_buffer.get_oldest_timestamp(project_id, table_name)
    }

    /// Get the time range (oldest, newest) for a project/table in microseconds.
    pub fn get_bucket_ranges(&self, project_id: &str, table_name: &str) -> Vec<(i64, i64)> {
        self.mem_buffer.get_bucket_ranges(project_id, table_name)
    }

    pub fn get_time_range(&self, project_id: &str, table_name: &str) -> Option<(i64, i64)> {
        self.mem_buffer.get_time_range(project_id, table_name)
    }

    pub fn query(&self, project_id: &str, table_name: &str, filters: &[datafusion::logical_expr::Expr]) -> anyhow::Result<Vec<RecordBatch>> {
        self.mem_buffer.query(project_id, table_name, filters)
    }

    /// Query and return partitioned data - one partition per time bucket.
    /// This enables parallel execution across time buckets in DataFusion.
    pub fn query_partitioned(&self, project_id: &str, table_name: &str, filters: &[datafusion::logical_expr::Expr]) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
        self.mem_buffer.query_partitioned(project_id, table_name, filters)
    }

    /// MemBuffer query with atomic text-match prefilter. Used by the SQL
    /// routing layer when text_match predicates are present — guarantees
    /// the per-bucket prefilter and the returned snapshot reflect the same
    /// point-in-time bucket state. Falls through to `query_partitioned`
    /// behavior when `preds` is empty or the table has no indexed fields.
    pub fn query_partitioned_with_text_match(
        &self, project_id: &str, table_name: &str, filters: &[datafusion::logical_expr::Expr], preds: &[crate::tantivy_index::udf::TextMatchPred],
    ) -> anyhow::Result<Vec<Vec<RecordBatch>>> {
        self.mem_buffer.query_partitioned_with_text_match(project_id, table_name, filters, preds)
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
        // Log to WAL first for durability. Failure here means the delete is
        // not recoverable after a crash — propagate so the client knows the
        // operation didn't commit, rather than apply in-memory and lose it
        // on the next restart's WAL replay.
        self.wal
            .append_delete(project_id, table_name, predicate_sql.as_deref())
            .map_err(|e| datafusion::error::DataFusionError::External(format!("WAL append_delete failed: {e}").into()))?;
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
        // See `delete()` — WAL failure must propagate so the client doesn't
        // see a "successful" update that disappears on the next restart.
        self.wal
            .append_update(project_id, table_name, predicate_sql.as_deref(), &assignments_sql)
            .map_err(|e| datafusion::error::DataFusionError::External(format!("WAL append_update failed: {e}").into()))?;
        self.mem_buffer.update(project_id, table_name, predicate, assignments)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serial_test::serial;
    use tempfile::tempdir;

    use super::*;
    use crate::test_utils::test_helpers::{json_to_batch, test_span};

    fn create_test_config(data_dir: PathBuf) -> Arc<AppConfig> {
        let mut cfg = AppConfig::default();
        cfg.core.timefusion_data_dir = data_dir;
        Arc::new(cfg)
    }

    fn create_test_batch(project_id: &str) -> RecordBatch {
        // Use test_span helper which creates data matching the default schema
        json_to_batch(vec![
            test_span("test1", "span1", project_id),
            test_span("test2", "span2", project_id),
            test_span("test3", "span3", project_id),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn test_insert_and_query() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // Use unique but short project/table names (walrus has metadata size limit)
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("p{}", test_id);
        let table = format!("t{}", test_id);

        let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        let batch = create_test_batch(&project);

        layer.insert(&project, &table, vec![batch.clone()]).await.unwrap();

        let results = layer.query(&project, &table, &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[serial]
    #[tokio::test]
    async fn test_recovery() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // SAFETY: walrus-rust reads WALRUS_DATA_DIR from environment. We use #[serial]
        // to prevent concurrent access to this process-global state.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        // Use unique but short project/table names (walrus has metadata size limit)
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("r{}", test_id);
        let table = format!("r{}", test_id);

        // First instance - write data
        {
            let layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            let batch = create_test_batch(&project);
            layer.insert(&project, &table, vec![batch]).await.unwrap();
            // Layer drops here - WAL data should be persisted
        }

        // Second instance - recover from WAL
        {
            let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(stats.entries_replayed > 0, "Expected entries to be replayed from WAL");

            let results = layer.query(&project, &table, &[]).unwrap();
            assert!(!results.is_empty(), "Expected results after WAL recovery");
        }
    }

    #[tokio::test]
    async fn test_pressure_pct() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("p{}", test_id);
        let table = format!("t{}", test_id);

        let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        assert_eq!(layer.pressure_pct(), 0, "empty layer should report 0%");

        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();
        let pct = layer.pressure_pct();
        assert!(pct <= 100, "pressure must be bounded 0..=100, got {pct}");
        // Tiny batch on 4GB default budget — should be effectively 0%.
        assert!(pct < 5, "expected ~0% after tiny insert, got {pct}");
    }

    /// After an insert, the FlushableBucket snapshot must carry per-shard
    /// counts whose total equals the number of WAL entries appended. Before
    /// this regression test the counts didn't exist and `wal.checkpoint`
    /// drained the whole column to its tail.
    #[tokio::test]
    async fn wal_shard_counts_recorded_on_insert() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("c{}", test_id);
        let table = format!("c{}", test_id);

        let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        // 3 batches → 3 WAL entries on one shard for this insert.
        let batches = vec![
            create_test_batch(&project),
            create_test_batch(&project),
            create_test_batch(&project),
        ];
        layer.insert(&project, &table, batches).await.unwrap();

        let buckets = layer.mem_buffer.get_all_buckets();
        let target: Vec<_> = buckets.iter().filter(|b| b.project_id == project && b.table_name == table).collect();
        assert!(!target.is_empty(), "expected at least one bucket for the inserted rows");
        let total: u64 = target.iter().flat_map(|b| b.wal_shard_counts.iter()).sum();
        assert_eq!(total, 3, "per-shard counts must sum to total WAL entries appended");
    }

    /// Flushing a sealed bucket must NOT advance the walrus cursor past
    /// entries belonging to a still-open follow-on bucket. Before this fix,
    /// `wal.checkpoint` drained to walrus tail and silently consumed the
    /// open bucket's entries — on crash they were lost (cursor said
    /// "consumed", Delta didn't have them, MemBuffer was volatile).
    ///
    /// We exercise it by inserting into bucket B (older timestamp, sealed),
    /// inserting into bucket B' (current timestamp, open), force-flushing B
    /// only, then asserting B''s entries still exist in WAL by replaying
    /// recovery into a fresh layer.
    #[serial]
    #[tokio::test]
    async fn flush_does_not_consume_open_bucket_wal_entries() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial]
        // protects the global.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("o{}", test_id);
        let table = format!("o{}", test_id);

        // Use a stub delta callback so flush succeeds without S3.
        let delta_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let delta_calls_cb = delta_calls.clone();
        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _batches, _wm| {
            let c = delta_calls_cb.clone();
            Box::pin(async move {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(Vec::new())
            })
        }));
        let layer = Arc::new(layer);

        // Insert "old" rows into a stale bucket (one bucket-duration in the past).
        let bucket_dur_micros = crate::mem_buffer::bucket_duration_micros();
        let now = crate::clock::now_micros();
        let old_ts = now - 2 * bucket_dur_micros;
        let old_batch = crate::test_utils::test_helpers::json_to_batch(vec![
            crate::test_utils::test_helpers::test_span_ts("old", "spanA", &project, old_ts),
        ])
        .unwrap();
        layer.insert(&project, &table, vec![old_batch]).await.unwrap();

        // Insert "current" rows into the open follow-on bucket.
        let new_batch = create_test_batch(&project);
        layer.insert(&project, &table, vec![new_batch]).await.unwrap();

        // Flush only completed (= old) buckets. Open bucket stays in MemBuffer + WAL.
        layer.flush_completed_buckets().await.unwrap();
        assert!(delta_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1, "old bucket should have flushed");

        // Drop this layer; spin up a fresh one and recover. The open bucket's
        // WAL entry must still be there.
        drop(layer);
        let layer2 = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        let stats = layer2.recover_from_wal().await.unwrap();
        assert!(
            stats.entries_replayed >= 1,
            "open-bucket WAL entry must survive flush of the sealed bucket; replayed={}",
            stats.entries_replayed
        );
    }

    /// On flush, the Delta write callback must receive a per-shard watermark
    /// that contains a non-origin position for whichever shard the bucket's
    /// appends landed on. Proves the seal-time snapshot in
    /// `FlushableBucket.wal_positions` propagates through `flush_bucket` →
    /// callback intact. Without this the watermark would never reach Delta
    /// commit metadata and Step 5 recovery would silently no-op.
    #[serial]
    #[tokio::test]
    async fn flush_callback_receives_per_shard_watermark() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("w{}", test_id);
        let table = format!("w{}", test_id);

        let captured_wm: Arc<std::sync::Mutex<Option<crate::buffered_write_layer::DeltaWatermark>>> = Arc::new(std::sync::Mutex::new(None));
        let captured_wm_cb = captured_wm.clone();

        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _batches, wm| {
            let captured = captured_wm_cb.clone();
            Box::pin(async move {
                *captured.lock().unwrap() = Some(wm);
                Ok(Vec::new())
            })
        }));
        let layer = Arc::new(layer);

        // Insert into a sealed (past-cutoff) bucket so flush_completed_buckets picks it up.
        let bucket_dur_micros = crate::mem_buffer::bucket_duration_micros();
        let old_ts = crate::clock::now_micros() - 2 * bucket_dur_micros;
        let old_batch = crate::test_utils::test_helpers::json_to_batch(vec![
            crate::test_utils::test_helpers::test_span_ts("seal", "spanA", &project, old_ts),
        ])
        .unwrap();
        layer.insert(&project, &table, vec![old_batch]).await.unwrap();

        layer.flush_completed_buckets().await.unwrap();

        let wm = captured_wm.lock().unwrap().clone().expect("callback must have been invoked with a watermark");
        assert_eq!(wm.len(), layer.wal().shards_per_topic(), "watermark must have one entry per shard");
        let non_origin: Vec<_> = wm.iter().enumerate().filter_map(|(s, p)| p.filter(|p| !p.is_origin()).map(|p| (s, p))).collect();
        assert_eq!(
            non_origin.len(),
            1,
            "exactly one shard should carry a non-origin position (the one we appended to); got {:?}",
            wm
        );
    }

    #[tokio::test]
    async fn test_memory_reservation() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // Use unique but short project/table names (walrus has metadata size limit)
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("m{}", test_id);
        let table = format!("m{}", test_id);

        let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();

        // First insert should succeed
        let batch = create_test_batch(&project);
        layer.insert(&project, &table, vec![batch]).await.unwrap();

        // Verify reservation is released (should be 0 after successful insert)
        assert_eq!(layer.reserved_bytes.load(Ordering::Acquire), 0);
    }
}
