use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
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
    errors::wal_err,
    mem_buffer::{FlushableBucket, MemBuffer, MemBufferStats, estimate_batch_size, extract_min_timestamp},
    wal::{DeletePayload, UpdatePayload, UpdateWithSourcePayload, WalEntry, WalManager, WalOperation, decode_payload, deserialize_record_batch},
};

// Safety margin over `estimate_batch_size()` for costs it can't see: Vec
// headers, DashMap node overhead, allocator fragmentation. The estimator's
// `get_array_memory_size()` already covers Arrow buffers (alignment, validity
// bitmaps), and fixed costs (walrus mmaps, Foyer, tantivy) are subtracted via
// `max_memory_bytes()`. Measured within ~10–15% of marginal heap growth
// (bench/multiplier_bench.py, 2026-05-17).
const MEMORY_OVERHEAD_MULTIPLIER: f64 = 1.15;

/// Estimated reserved bytes for a write: raw Arrow size × the overhead multiplier.
/// Single source of truth shared by `try_reserve_memory` and `force_reserve` so the
/// admit and force-admit paths can't drift apart.
fn estimate_reservation(batches: &[RecordBatch]) -> usize {
    let batch_size: usize = batches.iter().map(estimate_batch_size).sum();
    (batch_size as f64 * MEMORY_OVERHEAD_MULTIPLIER) as usize
}

/// Hard limit = `max_bytes + max_bytes / N` = 120% of budget (`5` → +20%),
/// leaving headroom for in-flight writes without unbounded growth.
const HARD_LIMIT_HEADROOM_DIVISOR: usize = 5;
/// Maximum CAS retry attempts before failing
const MAX_CAS_RETRIES: u32 = 100;
/// Base backoff delay in microseconds for CAS retries
const CAS_BACKOFF_BASE_MICROS: u64 = 1;
/// Maximum backoff exponent (caps delay at ~1ms)
const CAS_BACKOFF_MAX_EXPONENT: u32 = 10;

/// Write raw bytes with owner-only (0600) permissions on Unix; plain write
/// elsewhere.
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

/// Returns false when the payload could not be persisted — the WAL is then
/// the entry's ONLY copy, and recovery must not advance past it.
fn quarantine_entry(quarantine_dir: &std::path::Path, entry: &WalEntry, kind: &str, reason: &str) -> bool {
    if let Err(e) = std::fs::create_dir_all(quarantine_dir) {
        error!("Failed to create WAL quarantine dir {:?}: {}", quarantine_dir, e);
        return false;
    }
    // Sanitize topic for filename: project:table can contain '/' or other chars
    let topic = format!("{}__{}", entry.project_id, entry.table_name).replace(['/', '\\', ':', '\0'], "_");
    let filename = format!("{}_{}_{}.bin", entry.timestamp_micros, kind, topic);
    let path = quarantine_dir.join(&filename);
    // Quarantine files contain raw user data that failed to deserialize —
    // write with mode 0600 so they're not world-readable on shared hosts.
    if let Err(e) = write_owner_only(&path, &entry.data) {
        error!("Failed to write quarantine file {:?}: {}", path, e);
        return false;
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
    true
}

/// Operator-visible snapshot of the BufferedWriteLayer state. Returned by
/// `snapshot_stats()` and rendered as rows by `timefusion.stats()`.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub mem_project_count:              usize,
    pub mem_total_buckets:              usize,
    pub mem_total_rows:                 usize,
    pub mem_total_batches:              usize,
    pub mem_estimated_bytes:            usize,
    pub reserved_bytes:                 usize,
    pub max_memory_bytes:               usize,
    pub pressure_pct:                   u32,
    pub wal_files:                      usize,
    pub wal_disk_bytes:                 u64,
    pub wal_shards_per_topic:           usize,
    pub wal_known_topics:               usize,
    pub bucket_duration_micros:         i64,
    /// Oldest flushable MemBuffer bucket's flush-dwell in secs (`now - bucket
    /// creation time`), None when none past the open window. Measures how long
    /// a bucket has waited to flush — NOT its rows' event-time age — so
    /// backfill/late data can't false-trip it. Alert at > 2× `flush_interval_secs`.
    pub oldest_bucket_age_secs:         Option<u64>,
    /// Cumulative flush successes/failures since start. Mirror the OTel
    /// `timefusion.flush.completed`/`failed` counters for OTel-free tests.
    pub flush_completed_total:          u64,
    pub flush_failed_total:             u64,
    /// Inserts that hit the hard limit and applied backpressure (sync flush)
    /// instead of rejecting. Sustained growth = ingest outpacing flush.
    pub backpressure_engaged_total:     u64,
    /// Inserts rejected after backpressure failed to free memory. PAGE on any
    /// growth — data is safe in WAL but ingest is now dropping.
    pub backpressure_rejected_total:    u64,
    /// Open-bucket force-flush escalations (one busy window was itself the
    /// pressure). Sustained growth = windows too large for the budget.
    pub backpressure_force_flush_total: u64,
    /// Cumulative rows accepted vs drained to Delta. Both climbing with ingest
    /// faster = throughput wedge, not a stuck flush.
    pub rows_ingested_total:            u64,
    pub rows_flushed_total:             u64,
    /// MemBuffer bytes reclaimed by flushes. Flat while `pressure_pct=100` and
    /// flushes commit = memory is in buckets the flush path isn't reaching.
    pub flush_freed_bytes_total:        u64,
    /// Real process RSS (Linux `/proc/self/statm`), None off-Linux. Gap vs
    /// `mem_buffer.estimated_bytes` reveals per-bucket estimate inflation.
    pub process_rss_bytes:              Option<usize>,
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

/// MemBuffer bytes a flush reclaims — uses the same `estimate_batch_size` as
/// the per-bucket accounting, so `flush_freed_bytes_total` is directly
/// comparable to `mem_buffer.estimated_bytes`.
fn flushable_bytes(b: &crate::mem_buffer::FlushableBucket) -> u64 {
    b.batches.iter().map(crate::mem_buffer::estimate_batch_size).sum::<usize>() as u64
}

/// Resident set size of this process in bytes from `/proc/self/statm`
/// (Linux only; None elsewhere). Compare against the MemBuffer's
/// `estimate_batch_size` charge: a large RSS-below-estimate gap means the
/// per-bucket estimate (`get_array_memory_size` on wide Utf8View / replayed
/// batches) is over-counting, so backpressure is tripping on phantom bytes
/// rather than real memory.
fn process_rss_bytes() -> Option<usize> {
    // statm fields are in pages; resident is field 2. 4 KiB pages on every
    // Linux target TF deploys to (x86_64) — avoids a libc dependency.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    statm.split_whitespace().nth(1)?.parse::<usize>().ok().map(|pages| pages * 4096)
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

pub type DeltaWriteCallback =
    Arc<dyn Fn(String, String, Vec<RecordBatch>, DeltaWatermark) -> futures::future::BoxFuture<'static, anyhow::Result<Vec<String>>> + Send + Sync>;

/// Accumulator used by `flush_completed_buckets` to fold every per-bucket
/// `FlushableBucket` for one (project_id, table_name) into a single combined
/// commit. Each Delta commit pays a fixed cost (log scan + commit log write +
/// S3 RTT + tantivy build); coalescing turns N×O(commit) into 1×O(commit).
#[derive(Default)]
struct CoalescedGroup {
    /// `Option` not `String` so the first-bucket sentinel doesn't collide with
    /// the legitimate empty-project_id path (which falls back to "default" in
    /// the buffered-layer but reaches this code as `""`). Using `is_empty` as
    /// the sentinel previously let every subsequent bucket in such a group
    /// silently re-overwrite project_id/table_name.
    key:                 Option<(String, String)>,
    batches:             Vec<RecordBatch>,
    row_count:           usize,
    /// Per-shard min hold across all absorbed buckets — registered as the
    /// commit's in-flight cursor hold while it's airborne.
    wal_first_positions: Vec<Option<walrus_rust::WalPosition>>,
    /// The taken source buckets, kept whole so a failed commit can restore
    /// each one (rows + holds) to MemBuffer. Batches are Arc-backed, so this
    /// duplicates pointers, not data.
    source_buckets:      Vec<crate::mem_buffer::FlushableBucket>,
    /// Min/max timestamp across absorbed buckets (Option so the derived Default's
    /// 0 can't corrupt the min). Carried onto the combined FlushableBucket.
    min_timestamp:       Option<i64>,
    max_timestamp:       Option<i64>,
}

struct CombinedBucket {
    combined:       crate::mem_buffer::FlushableBucket,
    source_buckets: Vec<crate::mem_buffer::FlushableBucket>,
}

/// Per-shard min-merge of cursor holds: the combined hold is the earliest
/// position any input still pins.
fn merge_wal_holds(a: Vec<Option<walrus_rust::WalPosition>>, b: Vec<Option<walrus_rust::WalPosition>>) -> Vec<Option<walrus_rust::WalPosition>> {
    let len = a.len().max(b.len());
    (0..len)
        .map(|i| match (a.get(i).cloned().flatten(), b.get(i).cloned().flatten()) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (x, y) => x.or(y),
        })
        .collect()
}

impl CoalescedGroup {
    fn absorb(&mut self, b: crate::mem_buffer::FlushableBucket) {
        self.key.get_or_insert_with(|| (b.project_id.clone(), b.table_name.clone()));
        self.row_count += b.row_count;
        self.batches.extend(b.batches.iter().cloned());
        self.wal_first_positions = merge_wal_holds(std::mem::take(&mut self.wal_first_positions), b.wal_first_positions.clone());
        self.min_timestamp = Some(self.min_timestamp.map_or(b.min_timestamp, |m| m.min(b.min_timestamp)));
        self.max_timestamp = Some(self.max_timestamp.map_or(b.max_timestamp, |m| m.max(b.max_timestamp)));
        self.source_buckets.push(b);
    }

    fn into_combined_bucket(self) -> CombinedBucket {
        let CoalescedGroup {
            key,
            batches,
            row_count,
            wal_first_positions,
            source_buckets,
            min_timestamp,
            max_timestamp,
        } = self;
        // `absorb` is only called via `groups.entry(..).or_default().absorb(b)`
        // so `key` is always set by the time we collapse the group.
        let (project_id, table_name) = key.unwrap_or_default();
        // Use the max source bucket_id as a stable identifier for tracing only.
        let bucket_id = source_buckets.iter().map(|b| b.bucket_id).max().unwrap_or(0);
        let combined = crate::mem_buffer::FlushableBucket {
            project_id,
            table_name,
            bucket_id,
            batches,
            row_count,
            wal_first_positions,
            snapshot_gen: 0, // per-source-bucket gens are checked via source_buckets
            min_timestamp: min_timestamp.unwrap_or(i64::MAX),
            max_timestamp: max_timestamp.unwrap_or(i64::MIN),
        };
        CombinedBucket { combined, source_buckets }
    }
}

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
    config:                         Arc<AppConfig>,
    wal:                            Arc<WalManager>,
    mem_buffer:                     Arc<MemBuffer>,
    shutdown:                       CancellationToken,
    delta_write_callback:           Option<DeltaWriteCallback>,
    tantivy_index_callback:         Option<TantivyIndexCallback>,
    background_tasks:               Mutex<Vec<JoinHandle<()>>>,
    flush_lock:                     Mutex<()>,
    // Single-flights insert-path backpressure relief: only the writer that wins
    // this try_lock drives a relief flush; the rest wait for it to free RAM.
    // Without it, every blocked writer ran its own flush cycle (the ~20s p99
    // herd). Distinct from `flush_lock` so relief never blocks behind a routine
    // background flush already holding `flush_lock`.
    relief_lock:                    Mutex<()>,
    reserved_bytes:                 AtomicUsize, // Memory reserved for in-flight writes
    pressure_notify:                Arc<Notify>, // Wakes flush task when pressure threshold crossed
    /// Notified at the end of every flush task iteration (success or failure).
    /// Test hook: lets E2E harnesses await actual completion of background work
    /// instead of racing wall-clock sleeps.
    flush_tick_notify:              Arc<Notify>,
    /// Notified at the end of every eviction task iteration.
    eviction_tick_notify:           Arc<Notify>,
    /// Cumulative flush counters mirrored alongside OTel `record_flush`.
    /// OTel global metric state is opt-in (only initialized when telemetry is
    /// configured), so these atomics give the harness an in-process way to
    /// assert on what the global counters would be.
    flush_completed_total:          AtomicU64,
    flush_failed_total:             AtomicU64,
    backpressure_engaged_total:     AtomicU64,
    backpressure_rejected_total:    AtomicU64,
    backpressure_force_flush_total: AtomicU64,
    /// Cumulative rows accepted into MemBuffer (post-WAL) and rows drained to
    /// Delta. Diff two `timefusion_stats` scrapes to get ingest-rate vs
    /// drain-rate: if `rows_ingested_total` climbs faster than
    /// `rows_flushed_total` while `pressure_pct=100`, the flush is succeeding
    /// but ingest is outpacing drain (the file-count-throttled-dedup wedge),
    /// distinct from a stuck flush (`flush_failed_total` climbing).
    rows_ingested_total:            AtomicU64,
    rows_flushed_total:             AtomicU64,
    /// Cumulative MemBuffer bytes (per `estimate_batch_size`) reclaimed by
    /// successful flushes. Pair with `pressure_pct`: if `pressure_pct=100` and
    /// this is flat while flushes commit, the drained buckets are near-empty —
    /// the memory lives in buckets the flush path isn't reaching (e.g. an open
    /// window needing force-flush). If it climbs in step with ingest, the drain
    /// is working and ingest is simply outpacing it.
    flush_freed_bytes_total:        AtomicU64,
    // Required for WAL replay of UPDATE/DELETE whose SQL references UDFs.
    function_registry:              Arc<crate::functions::FnRegistry>,
    /// Caps concurrent detached tantivy sidecar builds so a fast flush cycle
    /// (post-F4 — one build per (project, table) per cycle) can't fan out
    /// past S3 connection / memory limits when many tables flush together.
    /// FOLLOW-UP: handles aren't stored; graceful shutdown does not await
    /// in-flight tantivy uploads. Acceptable for now because the sidecar is
    /// best-effort and the index can be rebuilt from Delta on demand.
    tantivy_spawn_sem:              Arc<tokio::sync::Semaphore>,
    /// Per-(project, table) max row timestamp ever handed to a Delta commit
    /// this process lifetime, floored at `boot_micros`. Delta cannot hold
    /// rows newer than this, so a query whose lower time bound is above it
    /// can skip the Delta scan — the steady-state recent-window fast path.
    /// Unlike the old `query_min >= mem_buffer_oldest` heuristic this stays
    /// sound when Delta holds rows *inside* MemBuffer's range: force-flushed
    /// open buckets and out-of-order drains after a failed flush (2026-06-11
    /// visibility gap). Raised before the commit so a query can't race in
    /// between commit-visible and watermark-raise; a failed commit leaves it
    /// conservatively high.
    delta_flushed_watermark:        dashmap::DashMap<crate::mem_buffer::TableKey, i64>,
    /// Recovery-time floor for the watermark: anything committed by earlier
    /// process lifetimes has row timestamps at/below roughly this (event
    /// timestamps drive bucketing; far-future-skewed pre-boot rows are the
    /// accepted residual exposure, same as the old heuristic).
    boot_micros:                    i64,
    /// WAL read-cursor holds for inserts whose entry is appended but whose
    /// MemBuffer bucket hasn't recorded its hold yet (the append→record
    /// window). Registered under the shard append lock BEFORE the entry
    /// exists — see `WalManager::append_batch` for the ordering argument.
    /// Keyed (project, table) → token → (shard, pre-append position).
    pending_wal_holds:              dashmap::DashMap<(String, String), std::collections::HashMap<u64, (usize, walrus_rust::WalPosition)>>,
    /// Holds for buckets taken out of MemBuffer for an in-flight Delta
    /// commit: while airborne they're invisible to `MemBuffer::wal_holds`,
    /// but until the commit lands their WAL entries must still pin the
    /// cursor. Keyed (project, table) → token → per-shard holds.
    inflight_flush_holds:           dashmap::DashMap<(String, String), std::collections::HashMap<u64, ShardHolds>>,
    /// Holds for buckets that could not be restored after a failed commit
    /// (evicted / incompatible schema): the rows exist only in the WAL, so
    /// the cursor must stay pinned until restart replays them. Kept apart
    /// from `inflight_flush_holds` so `await_inflight_flushes` (the DML
    /// Delta-leg ordering) doesn't treat a process-lifetime orphan as an
    /// airborne commit and stall every DML for the full watchdog budget.
    orphaned_wal_holds:             dashmap::DashMap<(String, String), ShardHolds>,
    wal_hold_seq:                   AtomicU64,
}

/// Per-shard WAL cursor holds (`None` = no hold on that shard).
type ShardHolds = Vec<Option<walrus_rust::WalPosition>>;

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
        let mem_buffer = Arc::new(MemBuffer::new_with_max_index_bytes_and_shards(text_index_max_bytes, wal.shards_per_topic()));

        Ok(Self {
            config: cfg,
            wal,
            mem_buffer,
            shutdown: CancellationToken::new(),
            delta_write_callback: None,
            tantivy_index_callback: None,
            background_tasks: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
            relief_lock: Mutex::new(()),
            reserved_bytes: AtomicUsize::new(0),
            pressure_notify: Arc::new(Notify::new()),
            flush_tick_notify: Arc::new(Notify::new()),
            eviction_tick_notify: Arc::new(Notify::new()),
            flush_completed_total: AtomicU64::new(0),
            flush_failed_total: AtomicU64::new(0),
            backpressure_engaged_total: AtomicU64::new(0),
            backpressure_rejected_total: AtomicU64::new(0),
            backpressure_force_flush_total: AtomicU64::new(0),
            rows_ingested_total: AtomicU64::new(0),
            rows_flushed_total: AtomicU64::new(0),
            flush_freed_bytes_total: AtomicU64::new(0),
            function_registry,
            // 16 is well above realistic per-cycle table fan-out for the
            // monoscope workload (~5 distinct table names) while still
            // bounding worst-case S3 / tantivy heap usage if more tables
            // appear.
            tantivy_spawn_sem: Arc::new(tokio::sync::Semaphore::new(16)),
            delta_flushed_watermark: dashmap::DashMap::new(),
            boot_micros: crate::clock::now_micros(),
            pending_wal_holds: dashmap::DashMap::new(),
            inflight_flush_holds: dashmap::DashMap::new(),
            orphaned_wal_holds: dashmap::DashMap::new(),
            wal_hold_seq: AtomicU64::new(0),
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
        let estimated_size = estimate_reservation(batches);

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

    /// Reserve memory unconditionally — adds the estimated bytes even past the
    /// hard limit. Only used on the `wal_admit_decouple` path when backpressure
    /// is exhausted: we admit over-budget rather than drop, since the WAL already
    /// holds the batch durably. Wakes the flush task to drain the overage.
    fn force_reserve(&self, batches: &[RecordBatch]) -> usize {
        let estimated_size = estimate_reservation(batches);
        self.reserved_bytes.fetch_add(estimated_size, Ordering::AcqRel);
        self.pressure_notify.notify_one();
        estimated_size
    }

    /// Reserve memory for a write, applying *backpressure* instead of dropping
    /// the write when the hard limit is hit. The rows are already destined for
    /// the durable WAL, and Delta/S3 is effectively unbounded "disk" — so when
    /// RAM is full the correct move is to flush MemBuffer → Delta to make room
    /// (the spill), not to reject. We retry the reservation after each drain
    /// and only fail after `write_backpressure_timeout` with no progress, which
    /// means Delta itself is unavailable.
    ///
    /// This deliberately reintroduces synchronous flushing into the insert path
    /// (previously removed to keep inserts non-blocking). The trade-off is
    /// intentional and now load-bearing: for a time-series DB a slow write is
    /// far better than a rejected one the producer must DLQ. Normal sub-limit
    /// inserts take the fast path and never block here.
    async fn reserve_with_backpressure(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let first = self.try_reserve_memory(batches).await;
        let timeout = self.config.buffer.write_backpressure_timeout();
        if first.is_ok() || timeout.is_zero() {
            return first;
        }

        let deadline = std::time::Instant::now() + timeout;
        crate::metrics::record_backpressure_engaged();
        self.backpressure_engaged_total.fetch_add(1, Ordering::Relaxed);
        warn!(
            "Write backpressure engaged: used={}MB ≥ hard limit; waking background flush to free RAM (not rejecting, not flushing on insert thread)",
            self.effective_memory_bytes() / (1024 * 1024)
        );
        loop {
            // Single-flight relief: only the writer that wins `relief_lock`
            // drives the synchronous flush; everyone else just nudges the
            // background flusher and waits. Previously every blocked writer ran
            // its own `flush_completed_buckets` + force-flush cycle, all queued
            // on `flush_lock` — with N writers the one at the back of the herd
            // waited O(N × commit), the source of the ~20s p99 tail. Now one
            // writer flushes (O(commit)) while the rest sleep below.
            if let Ok(_relief) = self.relief_lock.try_lock() {
                self.relieve_memory_pressure().await;
            } else {
                self.pressure_notify.notify_one();
            }

            match self.try_reserve_memory(batches).await {
                Ok(sz) => return Ok(sz),
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        crate::metrics::record_backpressure_rejected();
                        self.backpressure_rejected_total.fetch_add(1, Ordering::Relaxed);
                        // NOTE: this rejection happens in `insert()` BEFORE
                        // `wal.append_batch`, so the batch is NOT durable here —
                        // the old "data remains in WAL" wording was wrong. The
                        // batch is dropped from TF's side and recovery depends on
                        // the caller retrying / the upstream DLQ. Removing this
                        // loss seam is parity-plan Defect 1 (WAL-before-admit).
                        error!(
                            "Write backpressure exhausted after {:?}: used={}MB still over hard limit — Delta flush is not freeing memory; rejecting batch (NOT yet durable — WAL append happens only after admission; caller must retry or rely on the upstream DLQ)",
                            timeout,
                            self.effective_memory_bytes() / (1024 * 1024)
                        );
                        return Err(e);
                    }
                    // Wait for a flush to free RAM, then retry. Woken early by
                    // `flush_tick_notify` (the relief winner / background task
                    // signals it on every flush), capped at 25ms so a missed
                    // wakeup can't stall the retry.
                    tokio::select! {
                        _ = self.flush_tick_notify.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                }
            }
        }
    }

    /// One pass of pressure relief: drain completed buckets, then — if still
    /// over the limit — force-flush the current open bucket(s). Order matters:
    /// `force_flush_current_buckets` self-gates while completed buckets remain
    /// (WAL-ordering invariant), so completed buckets must drain first. Shared
    /// by the insert backpressure path (single-flighted via `relief_lock`) and
    /// the background flush task; both warn-and-continue on flush errors so the
    /// caller's retry/no-progress logic decides when to give up.
    async fn relieve_memory_pressure(&self) {
        if let Err(e) = self.flush_completed_buckets().await {
            warn!("pressure: flush_completed_buckets failed: {}", e);
        }
        if self.is_memory_pressure()
            && let Err(e) = self.force_flush_current_buckets().await
        {
            warn!("pressure: force_flush_current_buckets failed: {}", e);
        }
        // Memory may now be below the limit — wake any backpressured writers
        // parked on `flush_tick_notify` so they retry their reservation
        // immediately instead of waiting out their 25ms poll.
        self.flush_tick_notify.notify_waiters();
    }

    /// Force-flush the current (still-open) bucket(s) to Delta. Normal flushing
    /// excludes the current bucket so a full window accumulates in RAM; under
    /// sustained single-window pressure that window alone can exceed the budget,
    /// so this is the escalation tier. `take_bucket_for_flush` removes a
    /// bucket's rows atomically under the insert lock (no lost-write race) and
    /// leaves the bucket in place for ongoing inserts. On commit failure the
    /// rows are restored — durability never depended on this (WAL holds them).
    pub(crate) async fn force_flush_current_buckets(&self) -> anyhow::Result<()> {
        let _flush_guard = self.flush_lock.lock().await;
        let current = MemBuffer::current_bucket_id();
        let mut attempted = false;
        // No stuck-older-bucket gate anymore: the watermark advance is
        // order-safe by construction (an unflushed older bucket pins the
        // cursor via its holds), so force-flushing the open window can never
        // move the cursor past it — the old count-based consume could.
        for (project_id, table_name, bucket_id) in self.mem_buffer.bucket_keys(|id| id >= current) {
            let Some(bucket) = self.mem_buffer.take_bucket_for_flush(&project_id, &table_name, bucket_id) else {
                continue;
            };
            if !attempted {
                crate::metrics::record_backpressure_force_flush();
                self.backpressure_force_flush_total.fetch_add(1, Ordering::Relaxed);
                attempted = true;
            }
            match self.flush_taken_bucket(&bucket).await {
                Ok(()) => {
                    self.rows_flushed_total.fetch_add(bucket.row_count as u64, Ordering::Relaxed);
                    self.flush_freed_bytes_total.fetch_add(flushable_bytes(&bucket), Ordering::Relaxed);
                    self.flush_completed_total.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!("force-flush: Delta commit failed; rows restored to MemBuffer (WAL holds them): {}", e);
                    self.flush_failed_total.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub async fn insert(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        // Memory pressure no longer triggers a synchronous flush_all_now in the
        // insert path — that violated the "inserts return fast, Delta happens on
        // a routine" invariant by stalling pgwire/gRPC threads on S3 commits
        // (and worse, holding the global flush_lock so one slow tenant froze
        // ingest for everyone). The safety nets are: (a) `try_reserve_memory`
        // rejects inserts past the 120% hard limit, surfacing backpressure to
        // the client; (b) the post-CAS `pressure_notify.notify_one()` already
        // wakes the background flush task when reservations cross the
        // configured pressure threshold.
        if self.is_memory_pressure() {
            warn!(
                "Memory pressure (used={}MB / max={}MB) — notifying background flush; insert path will not block on Delta",
                self.effective_memory_bytes() / (1024 * 1024),
                self.config.buffer.max_memory_mb()
            );
            self.pressure_notify.notify_one();
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Compact before reservation AND WAL serialization: scan-backed DML
        // batches and IPC-decoded inputs otherwise reserve at phantom size
        // and serialize entire inherited buffers into the WAL (2026-06-11:
        // fat UPDATE entries re-inflated the buffer to 772GB on every
        // replay). MemBuffer's insert re-runs this as a cheap no-op.
        let batches: Vec<RecordBatch> = batches.into_iter().map(crate::mem_buffer::compact_batch).collect();

        // Reserve memory atomically before writing - prevents race condition.
        // Applies backpressure (synchronous flush-to-Delta + retry) instead of
        // rejecting when at the hard limit — see `reserve_with_backpressure`.
        let reserved_size = match self.reserve_with_backpressure(&batches).await {
            Ok(sz) => sz,
            // Decouple (parity plan Defect 1, default OFF): never DROP a write
            // whose backpressure budget is exhausted. The WAL append below is the
            // durability boundary, so admitting over-budget beats rejecting. The
            // batch is still admitted to MemBuffer + recorded, so the count-based
            // FIFO WAL advance stays correct (no skipped/un-admitted entry). Growth
            // is bounded by the relief flush + WAL replay on restart — soak before
            // prod enable.
            Err(e) if self.config.buffer.wal_admit_decouple() => {
                warn!("wal_admit_decouple: admitting over-budget instead of rejecting (WAL is durable): {}", e);
                self.force_reserve(&batches)
            }
            Err(e) => return Err(e),
        };

        // No per-topic mutex needed: WAL now shards each (project, table)
        // across N walrus collections via `WalManager::pick_shard`, so
        // concurrent appends to the same topic land in different shards and
        // walrus's single-writer-per-collection invariant is never contended.
        // MemBuffer is DashMap-based and already concurrent-safe.
        // WAL append + MemBuffer apply under a single pin lifecycle (see
        // `with_wal_pin`): the pending hold covers the append→apply window,
        // then each destination bucket is pinned at the pre-append position
        // atomically with its batch (batches in one append all land on the
        // same shard, but may straddle bucket boundaries if their timestamps
        // differ; the shared pre-position is ≤ every entry of this append,
        // so it's a valid hold for all).
        let result: anyhow::Result<()> = self.with_wal_pin(
            project_id,
            table_name,
            "append_batch",
            |on_pre| self.wal.append_batch(project_id, table_name, &batches, on_pre),
            |hold| {
                let now = crate::clock::now_micros();
                for batch in &batches {
                    let timestamp_micros = extract_min_timestamp(batch).unwrap_or(now);
                    self.mem_buffer.insert_with_hold(project_id, table_name, batch.clone(), timestamp_micros, hold)?;
                }
                Ok(())
            },
        );

        // Release reservation (memory is now tracked by MemBuffer)
        self.release_reservation(reserved_size);

        match &result {
            Ok(()) => {
                self.rows_ingested_total.fetch_add(row_count as u64, Ordering::Relaxed);
                crate::metrics::record_insert(project_id, table_name, row_count as u64);
            }
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

        // Crash-safe replay: rewind to a leftover marker (previous replay
        // crashed mid-run), then persist the pre-recovery cursors P0 before
        // consuming anything. Replay-created buckets are pinned at P0 below;
        // the cursor is parked at the surviving holds after the loop and the
        // marker removed only then. See `write_recovery_rewind_marker`.
        self.wal.apply_recovery_rewind_marker().map_err(|e| anyhow::anyhow!("recovery rewind marker apply failed: {}", e))?;
        let p0 = self.wal.write_recovery_rewind_marker().map_err(|e| anyhow::anyhow!("recovery rewind marker write failed: {}", e))?;

        // Stream entries one at a time and replay directly into MemBuffer.
        // Bounded recovery memory: O(1) entries in flight rather than
        // O(retention_window × throughput) (potentially GiBs).
        let mut entries_replayed = 0u64;
        // Recovered rows land straight in MemBuffer, bypassing insert()'s
        // rows_ingested_total bump. Count them here and fold in after replay so
        // rows_ingested_total/rows_flushed_total stay comparable post-restart —
        // otherwise the recovered rows flush and inflate flushed against a 0
        // ingested, clamping rows_in_buffer_lag and blinding the wedge signal.
        let mut recovered_rows = 0u64;
        let mut deletes_replayed = 0u64;
        let mut updates_replayed = 0u64;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;
        // Per-op-type cost accounting to attribute the replay wall-clock. INSERT
        // is split into Arrow-IPC decode vs MemBuffer apply; DML arms are timed
        // whole (they run a DataFusion parse+plan+predicate-eval per entry).
        let (mut insert_decode_nanos, mut insert_apply_nanos, mut insert_bytes) = (0u128, 0u128, 0u64);
        let (mut delete_nanos, mut update_nanos) = (0u128, 0u128);
        let mem_buffer = &self.mem_buffer;

        let quarantine_dir = self.wal.data_dir().join("quarantine");
        // Entries whose quarantine copy failed to persist still exist ONLY in
        // the WAL — recovery must not park past them / drop the marker.
        let mut quarantine_failures = 0u64;
        let registry_ref: Option<&crate::functions::FnRegistry> = Some(self.function_registry.as_ref());
        let (_total, error_count) = self.wal.for_each_entry(Some(cutoff), true, |entry| {
            let entry_start = std::time::Instant::now();
            match entry.operation {
                WalOperation::Insert => {
                    insert_bytes += entry.data.len() as u64;
                    let decoded = WalManager::deserialize_batch(&entry.data, &entry.table_name);
                    insert_decode_nanos += entry_start.elapsed().as_nanos();
                    match decoded {
                        Ok(batch) => {
                            if batch.num_rows() == 0 {
                                warn!("Skipping empty batch during WAL recovery for {}.{}", entry.project_id, entry.table_name);
                                return;
                            }
                            let apply_start = std::time::Instant::now();
                            let rows = batch.num_rows() as u64;
                            let insert_res = mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros);
                            insert_apply_nanos += apply_start.elapsed().as_nanos();
                            match insert_res {
                                Ok(()) => {
                                    entries_replayed += 1;
                                    recovered_rows += rows;
                                    // Pin the bucket at the pre-recovery cursor so the
                                    // watermark can't pass this entry until it flushes.
                                    // Borrowing find over the handful of topics avoids two
                                    // String allocs per replayed row on the bloated-WAL boot
                                    // path the team already optimizes.
                                    if let Some((_, holds)) = p0.iter().find(|((p, t), _)| p == &entry.project_id && t == &entry.table_name) {
                                        mem_buffer.record_replay_holds(&entry.project_id, &entry.table_name, entry.timestamp_micros, holds);
                                    }
                                }
                                Err(e) => {
                                    error!("WAL REPLAY FAILED: incompatible INSERT for {}.{}: {}", entry.project_id, entry.table_name, e);
                                    if !quarantine_entry(&quarantine_dir, &entry, "insert_incompatible", &e.to_string()) {
                                        quarantine_failures += 1;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(
                                "WAL CORRUPTION: undeserializable INSERT batch for {}.{}: {}",
                                entry.project_id, entry.table_name, e
                            );
                            if !quarantine_entry(&quarantine_dir, &entry, "insert_corrupt", &e.to_string()) {
                                quarantine_failures += 1;
                            }
                        }
                    }
                }
                WalOperation::Delete => match decode_payload::<DeletePayload>(&entry.data) {
                    Ok(payload) => {
                        if let Err(e) = mem_buffer.delete_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref(), registry_ref) {
                            error!("WAL REPLAY FAILED: DELETE for {}.{}: {}", entry.project_id, entry.table_name, e);
                            if !quarantine_entry(&quarantine_dir, &entry, "delete_replay_failed", &e.to_string()) {
                                quarantine_failures += 1;
                            }
                        } else {
                            deletes_replayed += 1;
                        }
                    }
                    Err(e) => {
                        error!(
                            "WAL CORRUPTION: undeserializable DELETE payload for {}.{}: {}",
                            entry.project_id, entry.table_name, e
                        );
                        if !quarantine_entry(&quarantine_dir, &entry, "delete_corrupt", &e.to_string()) {
                            quarantine_failures += 1;
                        }
                    }
                },
                WalOperation::Update => match decode_payload::<UpdatePayload>(&entry.data) {
                    Ok(payload) => {
                        if let Err(e) = mem_buffer.update_by_sql(
                            &entry.project_id,
                            &entry.table_name,
                            payload.predicate_sql.as_deref(),
                            &payload.assignments,
                            registry_ref,
                        ) {
                            error!("WAL REPLAY FAILED: UPDATE for {}.{}: {}", entry.project_id, entry.table_name, e);
                            if !quarantine_entry(&quarantine_dir, &entry, "update_replay_failed", &e.to_string()) {
                                quarantine_failures += 1;
                            }
                        } else {
                            updates_replayed += 1;
                        }
                    }
                    Err(e) => {
                        error!(
                            "WAL CORRUPTION: undeserializable UPDATE payload for {}.{}: {}",
                            entry.project_id, entry.table_name, e
                        );
                        if !quarantine_entry(&quarantine_dir, &entry, "update_corrupt", &e.to_string()) {
                            quarantine_failures += 1;
                        }
                    }
                },
                WalOperation::UpdateWithSource => match decode_payload::<UpdateWithSourcePayload>(&entry.data) {
                    Ok(payload) => match deserialize_record_batch(&payload.source.batch_ipc) {
                        Ok(source_batch) => {
                            if let Err(e) = mem_buffer.update_with_source_by_sql(
                                &entry.project_id,
                                &entry.table_name,
                                payload.predicate_sql.as_deref(),
                                &payload.assignments,
                                &payload.source.join_keys,
                                source_batch,
                                registry_ref,
                            ) {
                                error!("WAL REPLAY FAILED: UPDATE_WITH_SOURCE for {}.{}: {}", entry.project_id, entry.table_name, e);
                                if !quarantine_entry(&quarantine_dir, &entry, "update_with_source_replay_failed", &e.to_string()) {
                                    quarantine_failures += 1;
                                }
                            } else {
                                updates_replayed += 1;
                            }
                        }
                        Err(e) => {
                            error!(
                                "WAL CORRUPTION: undeserializable UPDATE_WITH_SOURCE Arrow batch for {}.{}: {}",
                                entry.project_id, entry.table_name, e
                            );
                            if !quarantine_entry(&quarantine_dir, &entry, "update_with_source_batch_corrupt", &e.to_string()) {
                                quarantine_failures += 1;
                            }
                        }
                    },
                    Err(e) => {
                        error!(
                            "WAL CORRUPTION: undeserializable UPDATE_WITH_SOURCE payload for {}.{}: {}",
                            entry.project_id, entry.table_name, e
                        );
                        if !quarantine_entry(&quarantine_dir, &entry, "update_with_source_corrupt", &e.to_string()) {
                            quarantine_failures += 1;
                        }
                    }
                },
            }
            // INSERT timing is split above; attribute the DML arms here (full
            // arm = bincode/Arrow decode + DataFusion predicate eval).
            match entry.operation {
                WalOperation::Delete => delete_nanos += entry_start.elapsed().as_nanos(),
                WalOperation::Update | WalOperation::UpdateWithSource => update_nanos += entry_start.elapsed().as_nanos(),
                WalOperation::Insert => {}
            }
            let ts = entry.timestamp_micros;
            oldest_ts = Some(oldest_ts.map_or(ts, |o| o.min(ts)));
            newest_ts = Some(newest_ts.map_or(ts, |n| n.max(ts)));
        })?;

        // Corruption threshold (0 = disabled): do NOT abort the boot when
        // every corrupt entry's payload is preserved on disk by
        // quarantine_entry — bailing with the rewind marker intact made the
        // next boot rewind and re-read the same corrupt prefix, a
        // deterministic crash-loop. Surface loudly and come up; the operator
        // recovers quarantined data out-of-band (alerting keys off
        // corrupted_entries_skipped / the WAL-corruption metric). But if any
        // quarantine WRITE failed (disk full — plausible exactly when the
        // WAL is bloated), the WAL is the only copy: keep the marker and
        // bail so nothing advances past the un-preserved entries.
        if corruption_threshold > 0 && error_count >= corruption_threshold {
            if quarantine_failures > 0 {
                anyhow::bail!(
                    "WAL corruption threshold exceeded ({} errors >= {}) AND {} quarantine write(s) failed — keeping the rewind marker; free disk / fix permissions on {:?} and restart",
                    error_count,
                    corruption_threshold,
                    quarantine_failures,
                    quarantine_dir
                );
            }
            error!(
                "WAL corruption threshold exceeded: {} errors >= {} threshold — corrupt entries quarantined under {:?}; continuing boot",
                error_count, corruption_threshold, quarantine_dir
            );
        }

        // Park the cursor at the earliest hold still owned by an unflushed
        // replayed bucket (the loop above consumed to tail). Topics whose
        // entries were all cutoff-filtered or DML-only have no holds and keep
        // the consumed tail — that's what finally lets bloated WAL backlogs
        // GC after replay. Only then is the rewind marker safe to drop.
        let shards = self.wal.shards_per_topic();
        for (project_id, table_name) in p0.keys() {
            let holds = self.mem_buffer.wal_holds(project_id, table_name, shards);
            if holds.iter().any(Option::is_some)
                && let Err(e) = self.wal.set_positions_allow_rewind(project_id, table_name, &holds)
            {
                // Cursor stays at tail with the marker gone would lose the
                // unflushed replayed buckets on a crash — keep the marker and
                // surface the failure instead.
                anyhow::bail!("failed to park WAL cursor for {}.{} after replay: {}", project_id, table_name, e);
            }
        }
        self.wal.remove_recovery_rewind_marker();

        // NB: replay loads entries straight into MemBuffer (bypassing the
        // insert-path reservation), so a large backlog can leave the process
        // over the memory budget. We deliberately do NOT drain here — that
        // blocked the PGWire listener for the entire flush (prod 2026-06-12:
        // +215s of 57P03 write-rejection). `drain_to_budget` runs in the
        // background (spawned by `start_background_tasks`) while we serve; reads
        // see MemBuffer (unioned with Delta) and new inserts flush-to-make-room
        // via insert-path backpressure.

        self.rows_ingested_total.fetch_add(recovered_rows, Ordering::Relaxed);

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
        // Attribution of the replay wall-clock by op type, so we know whether the
        // long pole is Arrow decode, MemBuffer apply, or per-entry DML SQL eval.
        let avg_ms = |nanos: u128, n: u64| if n > 0 { nanos as f64 / n as f64 / 1_000_000.0 } else { 0.0 };
        info!(
            "WAL recovery cost breakdown: insert_decode={}ms ({:.3}ms/ea), insert_apply={}ms ({:.3}ms/ea), \
             delete={}ms ({:.3}ms/ea), update={}ms ({:.3}ms/ea), insert_payload={}MB (avg {}B/ea)",
            insert_decode_nanos / 1_000_000,
            avg_ms(insert_decode_nanos, entries_replayed),
            insert_apply_nanos / 1_000_000,
            avg_ms(insert_apply_nanos, entries_replayed),
            delete_nanos / 1_000_000,
            avg_ms(delete_nanos, deletes_replayed),
            update_nanos / 1_000_000,
            avg_ms(update_nanos, updates_replayed),
            insert_bytes / (1024 * 1024),
            if entries_replayed > 0 { insert_bytes / entries_replayed } else { 0 },
        );
        Ok(stats)
    }

    /// Flush completed buckets to Delta until memory is back under budget, then
    /// stop. Spawned as a background task after WAL replay: replay can leave the
    /// process well over the memory budget, but draining no longer blocks the
    /// PGWire listener — we serve while this runs (reads see MemBuffer unioned
    /// with Delta; new inserts flush-to-make-room via insert backpressure).
    /// Bounded + progress-gated so a missing/failing Delta callback can't spin
    /// forever, and cancel-aware so shutdown returns promptly. Replayed buckets
    /// are pinned at the pre-recovery cursor P0; flushing them here releases
    /// those holds and lets the watermark advance past the replayed backlog.
    async fn drain_to_budget(&self) {
        if self.delta_write_callback.is_none() {
            return;
        }
        let max_bytes = self.max_memory_bytes();
        let mut prev = usize::MAX;
        for _ in 0..64 {
            if self.shutdown.is_cancelled() {
                return;
            }
            let used = self.effective_memory_bytes();
            if used <= max_bytes {
                break;
            }
            info!(
                "Post-replay drain: {}MB > {}MB budget — flushing completed buckets to Delta (background, serving concurrently)",
                used / (1024 * 1024),
                max_bytes / (1024 * 1024)
            );
            if let Err(e) = self.flush_completed_buckets().await {
                warn!("Post-replay drain flush failed: {}", e);
                break;
            }
            let now = self.effective_memory_bytes();
            if now + now / 100 >= prev {
                break; // <1% progress: nothing left to drain (or flush is stuck).
            }
            prev = now;
        }
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

        // Start periodic WAL GC. Without this, walrus' per-process
        // FileStateTracker leaks files across restarts (see `wal::gc_wal_files`).
        let gc_this = Arc::clone(&this);
        let gc_handle = tokio::spawn(async move {
            gc_this.run_wal_gc_task().await;
        });

        // One-shot post-replay drain. WAL replay loads the backlog straight into
        // MemBuffer; this flushes it down under budget in the background so the
        // listener serves immediately instead of blocking on the drain.
        let drain_this = Arc::clone(&this);
        let drain_handle = tokio::spawn(async move {
            drain_this.drain_to_budget().await;
        });

        // Store handles
        {
            let mut handles = this.background_tasks.lock().await;
            handles.push(flush_handle);
            handles.push(eviction_handle);
            handles.push(gc_handle);
            handles.push(drain_handle);
        }

        info!("BufferedWriteLayer background tasks started");
    }

    async fn run_wal_gc_task(&self) {
        // Sweep immediately, then every 10 minutes. The walk touches at most
        // a few dozen files, and waiting a full retention period before the
        // first sweep meant a process that restarted faster than that never
        // reclaimed anything — the 2026-06-11 crash loop (10-min OOM kills)
        // re-accumulated 30GB this way despite this task existing.
        const SWEEP_INTERVAL: Duration = Duration::from_secs(600);
        let max_age = self.config.buffer.wal_gc_max_age();
        let wal_dir = self.wal.data_dir().clone();
        loop {
            let dir = wal_dir.clone();
            // Filesystem walk is sync — push to a blocking thread so we
            // don't stall the runtime if the dir got huge before this fix
            // landed.
            let res = tokio::task::spawn_blocking(move || crate::wal::gc_wal_files(&dir, max_age)).await;
            match res {
                Ok(Ok((deleted, bytes_freed))) if deleted > 0 => {
                    info!("WAL GC: deleted {} stale files, freed {} bytes", deleted, bytes_freed);
                    if let Some(m) = crate::metrics::registry() {
                        m.wal_gc_deleted_files.add(deleted, &[]);
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => warn!("WAL GC error: {}", e),
                Err(e) => warn!("WAL GC task panicked: {}", e),
            }
            tokio::select! {
                _ = tokio::time::sleep(SWEEP_INTERVAL) => {}
                _ = self.shutdown.cancelled() => {
                    info!("WAL GC task shutting down");
                    break;
                }
            }
        }
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
                self.flush_failed_total.fetch_add(1, Ordering::Relaxed);
                error!("Flush task error: {}", e);
            }

            // Pressure escalation off the insert path: a single still-open
            // window can be the whole budget, which completed-bucket flushing
            // Drain until below the limit, a round commits nothing, or a bounded
            // round cap. Gate on COMMIT PROGRESS, not a byte delta: under
            // old-event-time backfill each flushed bucket is tiny, so the old
            // "<1% bytes freed → bail" quit at pressure=100 while hundreds of old
            // buckets were still draining slowly. As long as rounds keep
            // committing buckets (and ingest keeps adding flushable ones), keep
            // draining; only stop when a round commits nothing (completed buckets
            // gone or every commit failing/blocked — looping won't free RAM) so
            // we don't busy-spin when Delta is the bottleneck.
            const MAX_RELIEF_ROUNDS: u32 = 50;
            for _ in 0..MAX_RELIEF_ROUNDS {
                if !self.is_memory_pressure() {
                    break;
                }
                let before = self.flush_completed_total.load(Ordering::Relaxed);
                self.relieve_memory_pressure().await;
                if self.flush_completed_total.load(Ordering::Relaxed) == before {
                    error!(
                        "Pressure relief made no progress: used={}MB still over the limit — Delta flush committed nothing this round",
                        self.effective_memory_bytes() / (1024 * 1024)
                    );
                    break;
                }
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
            // Test-hook signal: every iteration end (success or failure).
            // `notify_waiters` wakes all currently parked awaiters; if no
            // test is watching, the call is essentially free.
            self.flush_tick_notify.notify_waiters();
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
                    // Release DML-emptied shells whose WAL entries aged past
                    // the replay cutoff — the only way their cursor holds
                    // ever release (they can't flush; see
                    // reap_expired_empty_buckets).
                    let retention_micros = (self.config.buffer.retention_mins() as i64) * 60 * 1_000_000;
                    self.mem_buffer.reap_expired_empty_buckets(crate::clock::now_micros() - retention_micros);
                    self.eviction_tick_notify.notify_waiters();
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
        // Group the sealed bucket keys per (project, table) FIRST: the
        // in-flight registration below must precede any snapshot of that
        // topic's buckets.
        let mut by_topic: std::collections::HashMap<(String, String), Vec<i64>> = std::collections::HashMap::new();
        for (p, t, id) in self.mem_buffer.bucket_keys(|id| id < current_bucket) {
            by_topic.entry((p, t)).or_default().push(id);
        }

        // Snapshot (not take): rows stay queryable in MemBuffer while the
        // Delta commit is airborne — a take here blacked out the flushed
        // window for reads until the commit landed. Holds are reset at
        // snapshot time, so a late insert into the sealed bucket pins itself
        // and `finish_flushed_snapshot` below preserves its rows: it can
        // neither be dropped by the drain nor have its WAL entry passed by
        // the watermark.
        //
        // Registration order is load-bearing: the airborne marker is
        // registered BEFORE the topic's first snapshot resets any bucket
        // hold, and upgraded with the real holds synchronously (no await),
        // so (a) `compute_wal_watermark` never observes a hold-less window
        // (we hold flush_lock, so no advance runs concurrently anyway) and
        // (b) `await_inflight_flushes` — the DML Delta-leg ordering — sees
        // the commit as airborne from the instant its pre-DML snapshot
        // exists. Deferring registration into the flush stream left a
        // seconds-long window (queued groups beyond the parallelism cap)
        // where a DELETE's Delta leg ran before the stale commit landed,
        // permanently resurrecting the deleted rows.
        //
        // Coalesce per (project_id, table_name): one Delta commit per table
        // per cycle instead of one per bucket — each commit pays a fixed
        // cost (log scan + JSON write + S3 RTT + tantivy build), and dedup
        // spans all flushed time windows.
        let mut bucket_count = 0usize;
        let mut groups: Vec<(CombinedBucket, u64)> = Vec::with_capacity(by_topic.len());
        for ((p, t), ids) in by_topic {
            let token = self.register_inflight_holds(&p, &t, Vec::new()); // airborne marker
            let mut group = CoalescedGroup::default();
            for id in ids {
                if let Some(b) = self.mem_buffer.snapshot_bucket_for_flush(&p, &t, id) {
                    group.absorb(b);
                }
            }
            if group.source_buckets.is_empty() {
                self.release_inflight_holds(&p, &t, token);
                continue;
            }
            bucket_count += group.source_buckets.len();
            let combined = group.into_combined_bucket();
            if let Some(mut m) = self.inflight_flush_holds.get_mut(&(p, t))
                && let Some(holds) = m.get_mut(&token)
            {
                *holds = combined.combined.wal_first_positions.clone();
            }
            groups.push((combined, token));
        }

        if groups.is_empty() {
            debug!("No buckets to flush");
            return Ok(());
        }

        debug!("Flushing {} bucket(s) → {} per-table commit(s)", bucket_count, groups.len());

        // Flush groups in parallel with bounded concurrency. Per-(project,table)
        // commits are independent — each Delta table has its own write lock
        // inside `insert_records_batch`, so parallelism here = cross-table
        // concurrency.
        let parallelism = self.config.buffer.flush_parallelism();
        let flush_results: Vec<_> = stream::iter(groups)
            .map(|(combined, token)| async move {
                let result = self.flush_bucket(&combined.combined).await;
                (combined, token, result)
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // Process results: checkpoint WAL and drain MemBuffer for successful flushes.
        //
        // Counter semantics: `flush_completed_total`/`flush_failed_total` continue
        // to count source bucket IDs (not coalesced groups). Pre-F4 each bucket
        // was its own commit, so `count = buckets = commits`. Post-F4 it's
        // `count = buckets ≠ commits`; the per-cycle commit count is
        // `groups.len()` and is visible only in the `Flushing N → M commits`
        // debug log. Dashboards thresholding on these counters keep their old
        // numeric meaning (work units done), but a "commits per minute"
        // dashboard derived from them now overstates real Delta commit rate.
        let mut any_ok = false;
        for (combined, token, result) in flush_results {
            let CombinedBucket { combined, source_buckets } = combined;
            match result {
                Ok(()) => {
                    // Rows are in Delta: remove exactly the snapshotted
                    // prefix from each source bucket (late arrivals stay;
                    // gen-dirty buckets keep everything for re-flush),
                    // release the in-flight holds so the watermark can pass
                    // the flushed entries, then advance. A failed advance is
                    // benign: the cursor stays behind and the next boot
                    // re-replays rows that are already in Delta — dedup_keys
                    // (write-side) and DedupExec (read-side) collapse them.
                    // Metrics count only DRAINED buckets: a dirty-kept
                    // bucket's rows are neither freed nor authoritative in
                    // Delta, and will be counted when its re-flush drains.
                    let drained: Vec<_> = source_buckets.iter().filter(|b| self.mem_buffer.finish_flushed_snapshot(b)).collect();
                    self.release_and_advance(&combined.project_id, &combined.table_name, token);
                    any_ok = true;
                    crate::metrics::record_flush(true);
                    let drained_rows: u64 = drained.iter().map(|b| b.row_count as u64).sum();
                    let drained_bytes: u64 = drained.iter().map(|b| flushable_bytes(b)).sum();
                    self.rows_flushed_total.fetch_add(drained_rows, Ordering::Relaxed);
                    self.flush_freed_bytes_total.fetch_add(drained_bytes, Ordering::Relaxed);
                    self.flush_completed_total.fetch_add(drained.len() as u64, Ordering::Relaxed);
                    debug!(
                        "Flushed coalesced commit: project={}, table={}, buckets={} ({} drained), rows={}",
                        combined.project_id,
                        combined.table_name,
                        source_buckets.len(),
                        drained.len(),
                        combined.row_count
                    );
                }
                Err(e) => {
                    // Merge the snapshots' holds back (rows never left the
                    // buckets) BEFORE releasing the in-flight holds, so the
                    // cursor is pinned by one or the other at every instant.
                    // If any restore fails (bucket evicted meanwhile) the
                    // in-flight hold stays registered until restart, keeping
                    // the entries replayable.
                    // fold, not all(): every bucket must be restored even after one fails.
                    let all_restored = source_buckets.iter().fold(true, |ok, bucket| self.mem_buffer.restore_snapshot_holds(bucket) && ok);
                    if all_restored {
                        self.release_inflight_holds(&combined.project_id, &combined.table_name, token);
                    } else {
                        self.orphan_inflight_holds(&combined.project_id, &combined.table_name, token, combined.wal_first_positions.clone());
                    }
                    crate::metrics::record_flush(false);
                    self.flush_failed_total.fetch_add(source_buckets.len() as u64, Ordering::Relaxed);
                    error!(
                        "Failed to flush coalesced commit: project={}, table={}, buckets={:?}: {}",
                        combined.project_id,
                        combined.table_name,
                        source_buckets.iter().map(|b| b.bucket_id).collect::<Vec<_>>(),
                        e
                    );
                }
            }
        }
        if any_ok {
            self.write_post_flush_snapshot().await;
        }

        Ok(())
    }

    /// Flush a bucket to Delta Lake via the configured callback.
    /// The callback MUST complete the Delta commit before returning Ok - this is critical
    /// for durability. We only advance the WAL watermark after this returns successfully.
    async fn flush_bucket(&self, bucket: &FlushableBucket) -> anyhow::Result<()> {
        // Raise the Delta watermark before the commit (see field docs).
        if bucket.max_timestamp != i64::MIN {
            let key = (Arc::<str>::from(bucket.project_id.as_str()), Arc::<str>::from(bucket.table_name.as_str()));
            self.delta_flushed_watermark
                .entry(key)
                .and_modify(|w| *w = (*w).max(bucket.max_timestamp))
                .or_insert(bucket.max_timestamp);
        }
        // Last-write-wins dedup on the per-table key set from schema YAML.
        // Empty key list = pass-through. Runs before both Delta write and the
        // tantivy sidecar so both see the same row set.
        let schema = crate::schema_loader::get_schema(&bucket.table_name);
        let dedup_keys = schema.map(|s| s.dedup_keys.as_slice()).unwrap_or(&[]);
        let tiebreak = schema.and_then(|s| s.dedup_tiebreak.as_deref());
        let batches = crate::mem_buffer::dedup_batches(bucket.batches.clone(), dedup_keys, tiebreak)?;
        let after: usize = batches.iter().map(|b| b.num_rows()).sum();
        if bucket.row_count > after {
            let dropped = bucket.row_count - after;
            crate::metrics::record_dedup_dropped(dropped as u64);
            debug!(
                "Dedup dropped {} rows: project={}, table={}, bucket_id={}",
                dropped, bucket.project_id, bucket.table_name, bucket.bucket_id
            );
        }
        let added_files = if let Some(ref callback) = self.delta_write_callback {
            // Await ensures Delta commit completes before we return. The
            // commit metadata records the CONSERVATIVE watermark (all holds,
            // including this flush's own): a boot-time derive from Delta then
            // never passes this commit's entries. An as-if-landed watermark
            // was wrong when the commit went gen-dirty — a crash before the
            // re-flush let derive skip inserts whose post-DML state only
            // lived behind the cursor, silently reverting acked DML. The
            // cost is re-replay + dedup of this commit's rows on a
            // crash-mid-flush boot, which is the safe direction.
            let delta_watermark = self.compute_wal_watermark(&bucket.project_id, &bucket.table_name);
            let commit = callback(bucket.project_id.clone(), bucket.table_name.clone(), batches.clone(), delta_watermark);
            // Watchdog: an un-timed-out commit that hangs would pin `flush_lock`
            // forever with no log (see `d_flush_bucket_timeout_secs`). On elapse
            // we bail so the caller counts flush_failed + retries next cycle;
            // rows stay durable in MemBuffer + WAL. 0 disables the watchdog.
            //
            // Abandoned-commit window: dropping the timed-out future cancels
            // its polling, but a Delta commit PUT already issued to S3 can
            // still land after the drop. The retained bucket is then re-
            // committed next cycle → the same rows land twice. Accepted:
            // dedup_keys (write-side) and DedupExec (read-side) collapse the
            // duplicates, and a slow-but-successful commit is rare next to a
            // truly hung one; size the timeout well above normal commit p99.
            let timeout = self.config.buffer.flush_bucket_timeout();
            if timeout.is_zero() {
                commit.await?
            } else {
                tokio::time::timeout(timeout, commit)
                    .await
                    .map_err(|_| {
                        crate::metrics::record_flush_stalled();
                        error!(
                            "flush_bucket Delta commit stalled >{:?} (project={}, table={}, bucket_id={}) — aborting this flush so flush_lock releases and relief can retry; rows remain durable in MemBuffer + WAL",
                            timeout, bucket.project_id, bucket.table_name, bucket.bucket_id
                        );
                        anyhow::anyhow!("flush_bucket commit timed out after {:?} (Delta/S3 stalled)", timeout)
                    })??
            }
        } else {
            warn!("No delta write callback configured, skipping flush");
            Vec::new()
        };
        // Sidecar tantivy index — best-effort, never fails the flush.
        // Spawned as a detached task so the Delta commit critical path doesn't
        // wait on tar.zst + S3 upload (a per-bucket cost that was dominating
        // flush latency at prod scale). F4 already collapses N bucket flushes
        // into one tantivy build per (project, table) per cycle; the semaphore
        // bounds the worst-case fan-out (many tables flushing simultaneously)
        // so concurrent uploads can't saturate S3 connections or grow tantivy
        // writer heap unbounded.
        if let Some(ref idx_cb) = self.tantivy_index_callback {
            let cb = idx_cb.clone();
            let pid = bucket.project_id.clone();
            let tname = bucket.table_name.clone();
            let bid = bucket.bucket_id;
            let sem = self.tantivy_spawn_sem.clone();
            tokio::spawn(async move {
                let _permit = match sem.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return, // semaphore closed — process is shutting down
                };
                if let Err(e) = cb(pid.clone(), tname.clone(), batches, added_files).await {
                    crate::metrics::record_tantivy_build_failure();
                    warn!(
                        "Tantivy index build failed (non-fatal): project={}, table={}, bucket_id={}: {}",
                        pid, tname, bid, e
                    );
                }
            });
        }
        Ok(())
    }

    /// Sanity check: warn loudly if any bucket has aged past retention
    /// without being flushed. This used to silently drain such
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

    /// Compute the safe per-shard WAL read-cursor watermark for a topic: the
    /// earliest position still held by unflushed data, or the write tail when
    /// nothing holds. Everything strictly before the watermark is durable in
    /// Delta, so crash replay from it can never lose an acked write; entries
    /// at/after it re-replay and dedup collapses any overlap.
    ///
    /// Ordering safety: the tail is snapshotted FIRST. Any entry appended
    /// after the snapshot sits at/after the tail, so min() can't pass it; any
    /// entry appended before it registered its pending hold under the append
    /// lock *before* appending, so the holds read below observes it.
    fn compute_wal_watermark(&self, project_id: &str, table_name: &str) -> ShardHolds {
        let shards = self.wal.shards_per_topic();
        // Tail FIRST — see the ordering argument in the doc comment above.
        let mut wm: ShardHolds = (0..shards).map(|s| self.wal.current_position_for_shard(project_id, table_name, s).ok()).collect();
        let key = (project_id.to_string(), table_name.to_string());
        let mut pending_holds: ShardHolds = vec![None; shards];
        if let Some(pending) = self.pending_wal_holds.get(&key) {
            for (shard, pos) in pending.values().filter(|(s, _)| *s < shards) {
                pending_holds[*shard] = Some(pending_holds[*shard].map_or(*pos, |p| p.min(*pos)));
            }
        }
        wm = merge_wal_holds(wm, pending_holds);
        if let Some(inflight) = self.inflight_flush_holds.get(&key) {
            for holds in inflight.values() {
                wm = merge_wal_holds(wm, holds.clone());
            }
        }
        if let Some(orphaned) = self.orphaned_wal_holds.get(&key) {
            wm = merge_wal_holds(wm, orphaned.clone());
        }
        merge_wal_holds(wm, self.mem_buffer.wal_holds(project_id, table_name, shards))
    }

    /// Forward-only advance of the topic's persisted read cursor to the
    /// current watermark. Called after a successful flush releases holds.
    /// Failure is benign: the cursor stays behind and the next boot re-replays
    /// entries whose rows are already in Delta (dedup collapses them).
    fn advance_wal_watermark(&self, project_id: &str, table_name: &str) {
        let wm = self.compute_wal_watermark(project_id, table_name);
        if let Err(e) = self.wal.merge_persisted_positions(project_id, table_name, &wm) {
            warn!(
                "WAL watermark advance failed for {}.{} (cursor stays behind; replay+dedup cover it): {}",
                project_id, table_name, e
            );
        }
    }

    /// Register the holds of buckets taken for an in-flight flush; returns the
    /// token to release with [`Self::release_inflight_holds`].
    fn register_inflight_holds(&self, project_id: &str, table_name: &str, holds: Vec<Option<walrus_rust::WalPosition>>) -> u64 {
        let token = self.wal_hold_seq.fetch_add(1, Ordering::Relaxed);
        self.inflight_flush_holds.entry((project_id.to_string(), table_name.to_string())).or_default().insert(token, holds);
        token
    }

    /// Release an in-flight flush's holds and advance the cursor to the new
    /// watermark — always paired after a successful commit.
    fn release_and_advance(&self, project_id: &str, table_name: &str, token: u64) {
        self.release_inflight_holds(project_id, table_name, token);
        self.advance_wal_watermark(project_id, table_name);
    }

    /// A failed commit whose buckets couldn't be restored: convert the
    /// in-flight holds into a process-lifetime orphan pin (min-merged) and
    /// release the airborne marker so DML ordering doesn't wait on it.
    fn orphan_inflight_holds(&self, project_id: &str, table_name: &str, token: u64, holds: ShardHolds) {
        self.orphaned_wal_holds
            .entry((project_id.to_string(), table_name.to_string()))
            .and_modify(|existing| *existing = merge_wal_holds(std::mem::take(existing), holds.clone()))
            .or_insert(holds);
        self.release_inflight_holds(project_id, table_name, token);
    }

    fn release_inflight_holds(&self, project_id: &str, table_name: &str, token: u64) {
        let key = (project_id.to_string(), table_name.to_string());
        if let Some(mut m) = self.inflight_flush_holds.get_mut(&key) {
            m.remove(&token);
        }
        // Prune the emptied outer entry — per-topic entries otherwise
        // accumulate forever under project/table churn. remove_if re-checks
        // under the shard lock, so a racing register keeps its entry.
        self.inflight_flush_holds.remove_if(&key, |_, m| m.is_empty());
    }

    /// Wait until no Delta commit is airborne for this table. The DML Delta
    /// leg must run AFTER any in-flight commit: a commit snapshotted before
    /// the DML's mem apply lands PRE-DML row values, and only a Delta
    /// merge/delete running after it can correct them — updates get merged,
    /// deleted rows removed. (The gen-dirty mem bucket masks the stale
    /// copies meanwhile, but DELETEd rows have nothing left in memory to
    /// re-flush, so without this ordering they'd resurface once the bucket
    /// drains.) Bounded: proceed with a warning past the flush watchdog
    /// budget — degrading to today's racy behavior only under a hung commit.
    pub async fn await_inflight_flushes(&self, project_id: &str, table_name: &str) {
        let key = (project_id.to_string(), table_name.to_string());
        // Fallback mirrors the flush watchdog default (see
        // `d_flush_bucket_timeout_secs`); the +pad covers post-commit
        // bookkeeping before the hold releases.
        const WATCHDOG_DISABLED_FALLBACK: Duration = Duration::from_secs(600);
        const POST_COMMIT_PAD: Duration = Duration::from_secs(30);
        let budget = match self.config.buffer.flush_bucket_timeout() {
            t if t.is_zero() => WATCHDOG_DISABLED_FALLBACK,
            t => t + POST_COMMIT_PAD,
        };
        let start = std::time::Instant::now();
        while self.inflight_flush_holds.get(&key).is_some_and(|m| !m.is_empty()) {
            if start.elapsed() > budget {
                warn!(
                    "await_inflight_flushes: commit still airborne after {:?} for {}.{} — proceeding (hung commit?)",
                    budget, project_id, table_name
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Persist a `clean_shutdown=false` cursor snapshot for the next boot.
    /// Called once per flush cycle (not per bucket) — the snapshot reads
    /// every topic's positions, so collapsing N per-bucket calls into one
    /// post-cycle write turns this from O(N²) into O(N).
    ///
    /// On write failure we delete any pre-existing snapshot: a stale file
    /// would let the (shallow) boot verifier skip commits made since the
    /// last successful write. Removing it forces a fresh Delta scan, which
    /// is correct-but-slow rather than fast-but-wrong.
    ///
    /// Callers: `flush_completed_buckets` (guards on `any_ok`) and
    /// `flush_all_now` (guards on `stats.buckets_flushed > 0`). Shutdown's
    /// per-bucket loop deliberately does NOT call this — the trailing
    /// `write_cursor_snapshot(true)` in `shutdown()` writes the
    /// definitive `clean_shutdown=true` snapshot and supersedes any
    /// dirty one we'd write here.
    async fn write_post_flush_snapshot(&self) {
        // Local-disk JSON write + rename is normally <1 ms but the call is on
        // a Tokio worker thread; offload to a blocking pool so a slow mount
        // (network-backed WAL dir, hung syscall) can't stall the flush task.
        let wal = self.wal.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) = wal.write_cursor_snapshot(false) {
                // Silent failure erodes the fast-boot guarantee over time —
                // surface at warn! so an operator at info level notices.
                warn!("write_cursor_snapshot (post-flush) failed: {} — will delete stale snapshot", e);
                if let Err(rm_err) = wal.delete_cursor_snapshot() {
                    // Worse: stale snapshot survives → next boot may restore
                    // outdated cursors and the shallow Delta verifier (default
                    // depth 8) can miss commits made since the last good write.
                    // See RUNBOOK.md "Stale cursor snapshot" for recovery.
                    warn!(
                        "delete stale cursor snapshot also failed: {} — next boot may restore stale state; \
                         delete `.timefusion_meta/cursor_snapshot.json` manually if symptoms appear",
                        rm_err
                    );
                }
            }
        })
        .await;
    }

    /// Shutdown with the full configured stop grace as the budget. Callers
    /// that already spent part of the grace on earlier drain phases (main.rs)
    /// use `shutdown_by` with the shared absolute deadline instead.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.shutdown_by(tokio::time::Instant::now() + self.config.buffer.stop_grace()).await
    }

    #[instrument(skip(self))]
    pub async fn shutdown_by(&self, deadline: tokio::time::Instant) -> anyhow::Result<()> {
        info!("BufferedWriteLayer shutdown initiated");

        // Signal background tasks to stop, then run the rest of shutdown by
        // `deadline` — the remainder of the process-wide stop grace, which must
        // fit inside the orchestrator's SIGTERM→SIGKILL window so the clean
        // cursor snapshot below ALWAYS gets written. Anything not flushed in
        // time is durable in the WAL and simply replays (and background-drains)
        // on next boot. Prod 2026-06-12: an unbounded force-flush of a 38GB
        // buffer blew past the grace, was SIGKILLed mid-flush, and left
        // clean_shutdown=false → next boot paid delta_cursor_reconcile + a full
        // blocking replay. Keep TIMEFUSION_STOP_GRACE_SECS below the
        // orchestrator grace (Docker `StopGracePeriod`).
        self.shutdown.cancel();
        let budget = deadline.saturating_duration_since(tokio::time::Instant::now());
        let flush_deadline = deadline - budget.mul_f32(0.2); // reserve 20% for the snapshot
        let hard_deadline = deadline;
        debug!("Shutdown budget: {:?}", budget);

        // Wait for background tasks to stop, bounded by the flush deadline.
        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.background_tasks.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            match tokio::time::timeout_at(flush_deadline, handle).await {
                Ok(Ok(())) => debug!("Background task completed cleanly"),
                Ok(Err(e)) => warn!("Background task panicked: {}", e),
                Err(_) => warn!("Background task did not stop before shutdown flush deadline"),
            }
        }

        // Best-effort flush of remaining buckets, bounded by flush_deadline so a
        // slice is reserved for the snapshot. The whole loop is deadline-wrapped,
        // so even a single flush stuck on a slow/unreachable Delta backend can't
        // blow the grace — the future is dropped and unflushed buckets replay
        // from the WAL on next boot.
        match tokio::time::timeout_at(flush_deadline, self.flush_lock.lock()).await {
            Ok(_flush_guard) => {
                let keys = self.mem_buffer.bucket_keys(|_| true);
                let total = keys.len();
                let mut flushed = 0usize;
                let done = tokio::time::timeout_at(flush_deadline, async {
                    for (p, t, id) in keys {
                        let Some(bucket) = self.mem_buffer.take_bucket_for_flush(&p, &t, id) else {
                            continue;
                        };
                        match self.flush_taken_bucket(&bucket).await {
                            Ok(()) => flushed += 1,
                            Err(e) => error!("Shutdown flush failed for bucket {}: {}", bucket.bucket_id, e),
                        }
                    }
                })
                .await;
                match done {
                    Ok(()) => info!("Shutdown flush: {}/{} buckets flushed; remainder (if any) replays from WAL", flushed, total),
                    Err(_) => info!(
                        "Shutdown flush deadline reached: {}/{} buckets flushed; remainder replays from WAL",
                        flushed, total
                    ),
                }
            }
            Err(_) => warn!("Flush lock not acquired before deadline — skipping shutdown flush; WAL holds all data"),
        }

        // ALWAYS write the clean-shutdown snapshot (even after a partial flush):
        // it records the post-flush cursor positions so the next boot can skip
        // `derive_wal_cursors_from_delta`. The WAL holds anything unflushed.
        let wal_for_snap = self.wal.clone();
        match tokio::time::timeout_at(hard_deadline, tokio::task::spawn_blocking(move || wal_for_snap.write_cursor_snapshot(true))).await {
            Ok(Ok(Ok(()))) => info!("Cursor snapshot written (clean_shutdown=true)"),
            Ok(Ok(Err(e))) => warn!("Cursor snapshot on shutdown failed: {} — next boot will Delta-scan", e),
            Ok(Err(join_err)) => warn!("Cursor snapshot blocking task panicked: {} — next boot will Delta-scan", join_err),
            Err(_) => warn!("Cursor snapshot did not finish before shutdown deadline — next boot will Delta-scan"),
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
        let mut stats = FlushStats::default();
        for (p, t, id) in self.mem_buffer.bucket_keys(|_| true) {
            let Some(bucket) = self.mem_buffer.take_bucket_for_flush(&p, &t, id) else {
                continue;
            };
            stats.total_rows += bucket.row_count as u64;
            match self.flush_taken_bucket(&bucket).await {
                Ok(()) => stats.buckets_flushed += 1,
                Err(e) => {
                    error!("flush_all_now: failed bucket {}: {}", bucket.bucket_id, e);
                    stats.buckets_failed += 1;
                }
            }
        }
        if stats.buckets_flushed > 0 {
            self.write_post_flush_snapshot().await;
        }
        Ok(stats)
    }

    /// Flush one taken bucket: force-flushed marking + in-flight hold
    /// registration + Delta commit + watermark advance on success, restore on
    /// failure. Shared by `force_flush_current_buckets`, `flush_all_now`, and
    /// the shutdown loop.
    async fn flush_taken_bucket(&self, bucket: &FlushableBucket) -> anyhow::Result<()> {
        // A concurrent insert can revive the taken bucket between the take
        // and its remove_if; the revived bucket's range exclusion would then
        // mask this commit's rows with nothing to punch through — mark
        // BEFORE the commit so no query races into a masked window.
        self.mem_buffer.mark_force_flushed(&bucket.project_id, &bucket.table_name, bucket.bucket_id);
        let token = self.register_inflight_holds(&bucket.project_id, &bucket.table_name, bucket.wal_first_positions.clone());
        match self.flush_bucket(bucket).await {
            Ok(()) => {
                self.release_and_advance(&bucket.project_id, &bucket.table_name, token);
                Ok(())
            }
            Err(e) => {
                // Release the in-flight hold ONLY when the rows made it back
                // into MemBuffer (whose bucket holds then pin the cursor). A
                // failed restore converts the hold into an orphan pin until
                // restart: the watermark can't pass the WAL-only entries,
                // but DML ordering doesn't mistake it for an airborne commit.
                if self.mem_buffer.restore_taken_bucket(bucket) {
                    self.release_inflight_holds(&bucket.project_id, &bucket.table_name, token);
                } else {
                    self.orphan_inflight_holds(&bucket.project_id, &bucket.table_name, token, bucket.wal_first_positions.clone());
                }
                Err(e)
            }
        }
    }

    /// Check if buffer is empty (all data flushed).
    pub fn is_empty(&self) -> bool {
        self.mem_buffer.get_stats().total_rows == 0
    }

    /// Test hook: synchronously run one eviction-task iteration
    /// (drain-then-evict-metadata). Production code should not call this —
    /// the eviction task is already spawned by `start_background_tasks`.
    pub async fn force_evict_now(&self) -> anyhow::Result<()> {
        self.flush_completed_buckets().await?;
        self.evict_drained_metadata();
        self.eviction_tick_notify.notify_waiters();
        Ok(())
    }

    /// Test hook: returns a `Notify` that is pinged at the end of every
    /// flush-task iteration. Call `notified()` BEFORE the action that should
    /// trigger a flush (otherwise the notification is missed).
    pub fn flush_tick_notify(&self) -> Arc<Notify> {
        self.flush_tick_notify.clone()
    }

    /// Test hook: returns a `Notify` pinged at end of every eviction-task
    /// iteration. Same caveat as `flush_tick_notify`.
    pub fn eviction_tick_notify(&self) -> Arc<Notify> {
        self.eviction_tick_notify.clone()
    }

    /// Test hook: simulates a crash by cancelling background tasks WITHOUT
    /// the final-flush graceful shutdown. Used by the e2e restart harness
    /// to test WAL replay — `shutdown()` would flush pending rows to Delta
    /// and checkpoint the WAL, which is not what a real crash does.
    pub async fn crash_for_test(&self) {
        self.shutdown.cancel();
        let handles: Vec<JoinHandle<()>> = {
            let mut guard = self.background_tasks.lock().await;
            std::mem::take(&mut *guard)
        };
        for handle in handles {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
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
            flush_completed_total: self.flush_completed_total.load(Ordering::Relaxed),
            flush_failed_total: self.flush_failed_total.load(Ordering::Relaxed),
            backpressure_engaged_total: self.backpressure_engaged_total.load(Ordering::Relaxed),
            backpressure_rejected_total: self.backpressure_rejected_total.load(Ordering::Relaxed),
            backpressure_force_flush_total: self.backpressure_force_flush_total.load(Ordering::Relaxed),
            rows_ingested_total: self.rows_ingested_total.load(Ordering::Relaxed),
            rows_flushed_total: self.rows_flushed_total.load(Ordering::Relaxed),
            flush_freed_bytes_total: self.flush_freed_bytes_total.load(Ordering::Relaxed),
            process_rss_bytes: process_rss_bytes(),
        }
    }

    pub fn get_bucket_ranges(&self, project_id: &str, table_name: &str) -> Vec<(i64, i64)> {
        self.mem_buffer.get_bucket_ranges(project_id, table_name)
    }

    /// Upper bound on row timestamps Delta can hold for this table — see
    /// `delta_flushed_watermark`. Queries bounded strictly above this can
    /// skip the Delta scan.
    pub fn delta_flushed_watermark(&self, project_id: &str, table_name: &str) -> i64 {
        let key = (Arc::<str>::from(project_id), Arc::<str>::from(table_name));
        self.delta_flushed_watermark.get(&key).map_or(self.boot_micros, |w| (*w).max(self.boot_micros))
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

    /// Serialize a DML `Expr` to *parseable SQL* for the WAL. `Expr`'s Display
    /// form renders literals like `TimestampMicrosecond(123, Some("UTC"))`,
    /// which the replay-side SQL parser rejects ("Invalid function
    /// 'timestampmicrosecond'") — quarantining every replayed UPDATE/DELETE
    /// that carried a timestamp predicate (prod 2026-07-03). The unparser
    /// covers less of the Expr space than Display, so on unparse failure fall
    /// back to Display rather than failing the client's statement: the
    /// fallback preserves the pre-fix behavior (statement succeeds, replay
    /// may quarantine that one entry) for exotic shapes only.
    fn expr_to_wal_sql(expr: &datafusion::logical_expr::Expr) -> String {
        datafusion::sql::unparser::expr_to_sql(expr).map(|ast| ast.to_string()).unwrap_or_else(|e| {
            warn!("DML expr unparse failed ({e}); falling back to Display form — replay of this entry may quarantine");
            format!("{expr}")
        })
    }

    fn assignments_to_wal_sql(assignments: &[(String, datafusion::logical_expr::Expr)]) -> Vec<(String, String)> {
        assignments.iter().map(|(col, expr)| (col.clone(), Self::expr_to_wal_sql(expr))).collect()
    }

    /// Run a WAL append + in-memory apply while pinning the entry at every
    /// instant: a pending hold covers the whole append→apply window
    /// (registered under the shard append lock — because registration
    /// happens-before the append, a concurrent watermark that snapshots the
    /// tail first can never pass an entry whose hold it hasn't seen), and
    /// the apply migrates the pin onto the buckets that own the data
    /// (`insert_with_hold` / `note_dml_mutation`). The single owner of the
    /// pin lifecycle for BOTH inserts and DML — keep it that way.
    fn with_wal_pin<T, R, E: From<datafusion::error::DataFusionError>>(
        &self, project_id: &str, table_name: &str, op: &'static str,
        append: impl FnOnce(Box<dyn FnOnce(usize, Option<walrus_rust::WalPosition>) + '_>) -> Result<T, crate::wal::WalError>,
        apply: impl FnOnce(Option<(usize, walrus_rust::WalPosition)>) -> Result<R, E>,
    ) -> Result<R, E> {
        let hold_key = (project_id.to_string(), table_name.to_string());
        let token = self.wal_hold_seq.fetch_add(1, Ordering::Relaxed);
        let captured = std::cell::Cell::new(None);
        let res = append(Box::new(|shard, pre| {
            // ORIGIN fallback on a failed tail read: the hold IS the
            // durability pin, so degrade to over-pinning, never to an
            // unpinned acked entry.
            let pre = pre.unwrap_or(walrus_rust::WalPosition::ORIGIN);
            self.pending_wal_holds.entry(hold_key.clone()).or_default().insert(token, (shard, pre));
            captured.set(Some((shard, pre)));
        }));
        let out = match res {
            Ok(_) => apply(captured.get()),
            Err(e) => Err(E::from(wal_err(op)(e))),
        };
        if let Some(mut m) = self.pending_wal_holds.get_mut(&hold_key) {
            m.remove(&token);
        }
        // Prune the emptied outer entry (see release_inflight_holds).
        self.pending_wal_holds.remove_if(&hold_key, |_, m| m.is_empty());
        out
    }

    /// Delete rows matching the predicate from the memory buffer.
    /// Logs the operation to WAL for crash recovery, then applies to MemBuffer.
    /// Returns the number of rows deleted.
    #[instrument(skip(self, predicate), fields(project_id, table_name))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>) -> datafusion::error::Result<u64> {
        let predicate_sql = predicate.map(Self::expr_to_wal_sql);
        // Log to WAL first for durability. Failure here means the delete is
        // not recoverable after a crash — propagate so the client knows the
        // operation didn't commit, rather than apply in-memory and lose it
        // on the next restart's WAL replay.
        self.with_wal_pin(
            project_id,
            table_name,
            "append_delete",
            |on_pre| self.wal.append_delete(project_id, table_name, predicate_sql.as_deref(), on_pre),
            |hold| self.mem_buffer.delete(project_id, table_name, predicate, hold),
        )
    }

    /// Update rows matching the predicate with new values in the memory buffer.
    /// Logs the operation to WAL for crash recovery, then applies to MemBuffer.
    /// Returns the number of rows updated.
    #[instrument(skip(self, predicate, assignments), fields(project_id, table_name))]
    pub fn update(
        &self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>, assignments: &[(String, datafusion::logical_expr::Expr)],
    ) -> datafusion::error::Result<u64> {
        let predicate_sql = predicate.map(Self::expr_to_wal_sql);
        let assignments_sql = Self::assignments_to_wal_sql(assignments);
        // See `delete()` — WAL failure must propagate so the client doesn't
        // see a "successful" update that disappears on the next restart.
        self.with_wal_pin(
            project_id,
            table_name,
            "append_update",
            |on_pre| self.wal.append_update(project_id, table_name, predicate_sql.as_deref(), &assignments_sql, on_pre),
            |hold| self.mem_buffer.update(project_id, table_name, predicate, assignments, hold),
        )
    }

    /// Apply `UPDATE ... FROM` to the memory buffer. Serializes the source
    /// `RecordBatch` to Arrow IPC and writes a `WalOperation::UpdateWithSource`
    /// entry before mutating in-memory state, so WAL replay can faithfully
    /// reconstruct the join after a restart.
    #[instrument(skip(self, predicate, assignments, source), fields(project_id, table_name, source_rows = source.batch.num_rows()))]
    pub fn update_with_source(
        &self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>,
        assignments: &[(String, datafusion::logical_expr::Expr)], source: &crate::dml::UpdateSource,
    ) -> datafusion::error::Result<u64> {
        let predicate_sql = predicate.map(Self::expr_to_wal_sql);
        let assignments_sql = Self::assignments_to_wal_sql(assignments);

        let batch_ipc = crate::wal::serialize_record_batch(&source.batch).map_err(wal_err("source serialize"))?;
        let serialized_source = crate::wal::SerializedSource {
            join_keys: source.join_keys.clone(),
            batch_ipc,
        };

        self.with_wal_pin(
            project_id,
            table_name,
            "append_update_with_source",
            |on_pre| {
                self.wal
                    .append_update_with_source(project_id, table_name, predicate_sql.as_deref(), &assignments_sql, &serialized_source, on_pre)
            },
            |hold| self.mem_buffer.update_with_source(project_id, table_name, predicate, assignments, source, hold),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serial_test::serial;
    use tempfile::{TempDir, tempdir};

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

    #[serial]
    #[tokio::test]
    async fn test_insert_and_query() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

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

    /// Regression: prod 2026-07-03 acked-write loss. The WAL cursor advance was
    /// count-based FIFO: flushing a sealed bucket consumed N entries from each
    /// shard's head, but entries from *different* event-time buckets interleave
    /// in arrival order, so the advance consumed entries belonging to the
    /// still-open bucket. A crash then replayed from the over-advanced cursor
    /// and the open bucket's acked rows were gone (prod: 02:55–03:00 UTC window
    /// empty in TF, present in the dual-write store; DLQ empty because the
    /// writes were acked).
    ///
    /// Arrival order (shards round-robin per topic, 4 shards):
    ///   i0: CURRENT-bucket row → shard 0   (stays open, must survive crash)
    ///   i1–i3: old-bucket rows → shards 1–3
    ///   i4: old-bucket row     → shard 0   (behind i0 on the same shard)
    /// Flushing the old bucket must NOT move shard 0's cursor past i0.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_advance_must_not_consume_open_bucket_entries() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("wm{}", test_id);
        let table = format!("wm{}", test_id);

        let now = crate::clock::now_micros();
        let old = now - 2 * crate::mem_buffer::bucket_duration_micros();
        let row = |id: &str, ts: i64| {
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts(id, id, &project, ts)]).unwrap()
        };

        {
            let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            // Mock Delta writer so the sealed bucket "commits" successfully.
            layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| Box::pin(async move { Ok(Vec::new()) })));
            let layer = Arc::new(layer);

            layer.insert(&project, &table, vec![row("live", now)]).await.unwrap(); // i0 → shard 0
            for k in 1..=3 {
                layer.insert(&project, &table, vec![row(&format!("old{k}"), old)]).await.unwrap(); // shards 1–3
            }
            layer.insert(&project, &table, vec![row("old0", old)]).await.unwrap(); // i4 → shard 0

            // Flush sealed buckets only; the "live" row's bucket stays open.
            layer.flush_completed_buckets().await.unwrap();
            // Crash: drop without shutdown — no clean-shutdown cursor snapshot.
        }

        {
            let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
            layer.recover_from_wal().await.unwrap();
            let ids = crate::test_utils::test_helpers::query_col_strings(&layer, &project, &table, "id");
            assert!(
                ids.contains(&"live".to_string()),
                "acked open-bucket row lost across crash: WAL cursor advanced past its entry (got rows {ids:?})"
            );
        }
    }

    /// A DELETE racing an airborne commit must stick: the commit lands
    /// pre-delete row values, so `finish_flushed_snapshot` must judge the
    /// bucket dirty (keep the post-delete state, no drain), and the deleted
    /// rows must stay gone across a crash + replay (the DELETE entry's hold
    /// keeps it replayable alongside the insert entries). The stale Delta
    /// copies are corrected by the DML Delta leg's `await_inflight_flushes`
    /// ordering, which is exercised at the dml.rs/e2e level.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_during_airborne_commit_sticks_across_crash() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("dd{}", test_id);
        let table = format!("dd{}", test_id);

        let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
        let row =
            |id: &str| crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts(id, id, &project, old_ts)]).unwrap();
        // Utf8View literal — the buffered `id` column is Utf8View and Arrow's
        // eq kernel rejects mixed Utf8View/Utf8 comparisons.
        let pred = datafusion::prelude::col("id").eq(datafusion::logical_expr::lit(datafusion::common::ScalarValue::Utf8View(Some("doomed".into()))));

        {
            let entered = Arc::new(Notify::new());
            let release = Arc::new(tokio::sync::Semaphore::new(0));
            let (entered_cb, release_cb) = (entered.clone(), release.clone());
            let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| {
                let (entered, release) = (entered_cb.clone(), release_cb.clone());
                Box::pin(async move {
                    entered.notify_one();
                    let _ = release.acquire().await;
                    Ok(Vec::new())
                })
            }));
            let layer = Arc::new(layer);

            layer.insert(&project, &table, vec![row("doomed")]).await.unwrap();
            layer.insert(&project, &table, vec![row("keeper")]).await.unwrap();

            let entered_wait = entered.notified();
            let flusher = {
                let layer = layer.clone();
                tokio::spawn(async move { layer.flush_completed_buckets().await })
            };
            entered_wait.await; // commit airborne, holding PRE-delete rows

            let deleted = layer.delete(&project, &table, Some(&pred)).unwrap();
            assert_eq!(deleted, 1, "mem leg must delete the doomed row mid-flight");

            release.add_permits(1);
            flusher.await.unwrap().unwrap();

            // Dirty finish must keep the post-delete state — not drain the
            // shifted prefix (which would drop 'keeper').
            let ids = crate::test_utils::test_helpers::query_col_strings(&layer, &project, &table, "id");
            assert_eq!(ids, vec!["keeper".to_string()], "post-delete state must survive the dirty finish (got {ids:?})");
            // Crash.
        }

        {
            let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
            layer.recover_from_wal().await.unwrap();
            let ids = crate::test_utils::test_helpers::query_col_strings(&layer, &project, &table, "id");
            assert!(
                !ids.contains(&"doomed".to_string()),
                "acked DELETE resurrected after crash+replay (got {ids:?})"
            );
            assert!(ids.contains(&"keeper".to_string()), "surviving row lost across crash (got {ids:?})");
        }
    }

    /// Sealed rows must stay queryable while their Delta commit is airborne.
    /// Regression: a take-based flush removed the rows from MemBuffer before
    /// the commit started, so every periodic flush blacked out the flushed
    /// window (rows in neither store) for the full commit duration.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn sealed_rows_stay_queryable_during_flush_commit() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("vz{}", test_id);
        let table = format!("vz{}", test_id);

        // Delta callback parks until released so we can query mid-commit.
        let entered = Arc::new(Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let (entered_cb, release_cb) = (entered.clone(), release.clone());
        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| {
            let (entered, release) = (entered_cb.clone(), release_cb.clone());
            Box::pin(async move {
                entered.notify_one();
                let _ = release.acquire().await;
                Ok(Vec::new())
            })
        }));
        let layer = Arc::new(layer);

        let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
        let batch =
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts("v1", "spanV", &project, old_ts)]).unwrap();
        layer.insert(&project, &table, vec![batch]).await.unwrap();

        let entered_wait = entered.notified();
        let flusher = {
            let layer = layer.clone();
            tokio::spawn(async move { layer.flush_completed_buckets().await })
        };
        entered_wait.await; // commit is airborne now

        let rows: usize = layer.query(&project, &table, &[]).unwrap().iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "sealed rows must remain queryable while the Delta commit is in flight");

        release.add_permits(1);
        flusher.await.unwrap().unwrap();
        assert!(layer.is_empty(), "flushed prefix must drain from MemBuffer after the commit lands");
    }

    /// Regression: prod-boot crash-loop. Corruption at/over the threshold
    /// used to bail recovery with the rewind marker intact, so every boot
    /// rewound to P0, re-read the same corrupt prefix, and bailed again —
    /// forever. With the payloads quarantined, recovery must come up (and a
    /// second recovery must too).
    #[serial]
    #[tokio::test]
    async fn corruption_threshold_boots_instead_of_crash_looping() {
        let dir = tempdir().unwrap();
        let mut cfg = AppConfig::default();
        cfg.core.timefusion_data_dir = dir.path().to_path_buf();
        cfg.buffer.timefusion_wal_corruption_threshold = 1;
        let cfg = Arc::new(cfg);
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("cr{}", test_id);
        let table = format!("cr{}", test_id);

        {
            let layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();
            layer.wal().append_raw_for_test(&project, &table, b"WAL2\x80garbage-not-bincode").unwrap();
            // Crash without shutdown.
        }

        for boot in 0..2 {
            let layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            let stats = layer
                .recover_from_wal()
                .await
                .unwrap_or_else(|e| panic!("boot {boot} must survive over-threshold corruption (quarantined payloads), got: {e}"));
            assert!(stats.corrupted_entries_skipped >= 1, "boot {boot}: corruption must be counted, got {stats:?}");
            let rows: usize = layer.query(&project, &table, &[]).unwrap().iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 3, "boot {boot}: healthy entries must still replay");
        }
    }

    /// A DML entry's shard must stay pinned while the buckets it mutated are
    /// unflushed: DML entries land on their own round-robin shard, which the
    /// buckets' insert holds don't cover, so without a topic-wide pin any
    /// unrelated flush advances that shard's cursor to tail and a crash
    /// silently reverts the acked UPDATE (deletes would resurrect rows).
    /// Arrival: i0 current-bucket insert (shard 0), i1 old-bucket insert
    /// (shard 1), UPDATE (shard 2). Flush the old bucket, crash, recover:
    /// the current bucket's rows must still carry the update.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn dml_entry_survives_unrelated_flush_and_crash() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("dm{}", test_id);
        let table = format!("dm{}", test_id);

        let now = crate::clock::now_micros();
        let old = now - 2 * crate::mem_buffer::bucket_duration_micros();
        let row = |id: &str, ts: i64| {
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts(id, id, &project, ts)]).unwrap()
        };
        let assignments = vec![("name".to_string(), datafusion::logical_expr::lit("renamed"))];

        {
            let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| Box::pin(async move { Ok(Vec::new()) })));
            let layer = Arc::new(layer);

            layer.insert(&project, &table, vec![row("live", now)]).await.unwrap(); // shard 0
            layer.insert(&project, &table, vec![row("old", old)]).await.unwrap(); // shard 1
            let updated = layer.update(&project, &table, None, &assignments).unwrap(); // shard 2
            assert_eq!(updated, 2);

            layer.flush_completed_buckets().await.unwrap(); // flushes the old bucket only
            // Crash: drop without shutdown.
        }

        {
            let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
            layer.recover_from_wal().await.unwrap();
            let results = layer.query(&project, &table, &[]).unwrap();
            let combined = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
            let names = arrow::compute::cast(combined.column(combined.schema().index_of("name").unwrap()), &arrow::datatypes::DataType::Utf8).unwrap();
            let names = names.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..combined.num_rows() {
                assert_eq!(
                    names.value(i),
                    "renamed",
                    "acked UPDATE reverted: its WAL entry was drained by an unrelated flush"
                );
            }
        }
    }

    /// Regression: prod 2026-07-03 — DML predicates carrying timestamp literals
    /// were WAL-serialized via `Expr`'s Display form
    /// (`TimestampMicrosecond(123, Some("UTC"))`), which is not parseable SQL,
    /// so every UPDATE replay failed planning with "Invalid function
    /// 'timestampmicrosecond'" and was quarantined — dual-write updates were
    /// silently dropped on every crash recovery.
    #[serial]
    #[tokio::test]
    async fn update_with_timestamp_predicate_replays_after_restart() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("tp{}", test_id);
        let table = format!("tp{}", test_id);

        let cutoff = crate::clock::now_micros() - 3_600_000_000; // 1h ago — matches all rows
        let pred = datafusion::prelude::col("timestamp").gt_eq(datafusion::logical_expr::lit(datafusion::common::ScalarValue::TimestampMicrosecond(
            Some(cutoff),
            Some("UTC".into()),
        )));
        let assignments = vec![("name".to_string(), datafusion::logical_expr::lit("renamed"))];

        {
            let layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();
            let updated = layer.update(&project, &table, Some(&pred), &assignments).unwrap();
            assert_eq!(updated, 3, "pre-restart update should hit all rows");
        }

        {
            let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
            layer.recover_from_wal().await.unwrap();
            let results = layer.query(&project, &table, &[]).unwrap();
            assert!(!results.is_empty(), "expected rows after WAL recovery");
            let combined = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
            let names = arrow::compute::cast(combined.column(combined.schema().index_of("name").unwrap()), &arrow::datatypes::DataType::Utf8).unwrap();
            let names = names.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            for i in 0..combined.num_rows() {
                assert_eq!(
                    names.value(i),
                    "renamed",
                    "WAL replay dropped the UPDATE — timestamp-literal predicate failed to parse on replay"
                );
            }
        }
    }

    /// Shutdown must finish within its budget AND persist a `clean_shutdown=true`
    /// snapshot even when the Delta flush can't keep up — otherwise the next boot
    /// pays `delta_cursor_reconcile` + a full blocking replay (prod 2026-06-12: a
    /// 38GB shutdown flush blew past CapRover's grace, was SIGKILLed mid-flush,
    /// and left `clean_shutdown=false`). Models a slow/hung Delta backend with a
    /// callback that sleeps far longer than the shutdown budget; shutdown must
    /// bound the flush and still write the clean snapshot.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_writes_clean_snapshot_under_deadline() {
        let dir = tempdir().unwrap();
        let mut base = AppConfig::default();
        base.core.timefusion_data_dir = dir.path().to_path_buf();
        base.buffer.timefusion_stop_grace_secs = 1; // budget=1s, flush_deadline=0.8s
        let cfg = Arc::new(base);
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("s{}", test_id);
        let table = format!("s{}", test_id);

        // Delta callback that blocks far longer than the shutdown budget.
        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                Ok(Vec::new())
            })
        }));
        let layer = Arc::new(layer);

        // Insert into a stale (sealed) bucket so shutdown's flush has work to do.
        let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
        let batch =
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts("x", "spanX", &project, old_ts)]).unwrap();
        layer.insert(&project, &table, vec![batch]).await.unwrap();

        // Shutdown must return promptly (bounded by the budget), not hang on the
        // 60s flush, and must persist the clean snapshot.
        let t = std::time::Instant::now();
        layer.shutdown().await.unwrap();
        assert!(
            t.elapsed() < std::time::Duration::from_secs(10),
            "shutdown must be deadline-bounded, took {:?}",
            t.elapsed()
        );

        let snap = layer.wal().load_cursor_snapshot().expect("clean snapshot must be written on shutdown");
        assert!(snap.clean_shutdown, "shutdown must mark clean_shutdown=true even on a partial flush");
    }

    /// Build the (`UpdateSource`, assignments) pair used by the `update_with_source`
    /// unit tests. Source schema is `(lookup_name: Utf8, new_id: Utf8)`; the join
    /// matches target `name` against source `lookup_name` and overwrites target
    /// `id` with `source.new_id`. Keeps the test side small while exercising the
    /// full hash-join + widened-batch eval path.
    fn build_update_source_for_id_rewrite(rows: &[(&str, &str)]) -> (crate::dml::UpdateSource, Vec<(String, datafusion::logical_expr::Expr)>) {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use datafusion::prelude::col;

        let lookup_names: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|(n, _)| *n).collect::<Vec<_>>()));
        let new_ids: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|(_, i)| *i).collect::<Vec<_>>()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("lookup_name", DataType::Utf8, false),
            Field::new("new_id", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![lookup_names, new_ids]).unwrap();

        let source = crate::dml::UpdateSource {
            batch,
            schema,
            join_keys: vec![("name".to_string(), "lookup_name".to_string())],
        };
        let assignments = vec![("id".to_string(), col("source.new_id"))];
        (source, assignments)
    }

    /// MemBuffer hash-join path: insert rows (they live in MemBuffer only,
    /// no Delta flush), apply `UPDATE ... FROM` via `update_with_source`,
    /// verify both the per-target update and that non-matched rows are
    /// untouched.
    ///
    /// `#[ignore]`d: the test harness inserts via `layer.insert` which yields
    /// MemBuffer entries with `Utf8View` string storage, while the source
    /// `RecordBatch` built from `StringArray` is `Utf8`. Arrow's
    /// `RowConverter` requires byte-identical types — even with a cast in
    /// `update_with_source` the lookup currently returns 0 matches, which
    /// suggests the cast isn't producing comparable `OwnedRow` bytes (or the
    /// cast happens on the wrong side). Needs a targeted Utf8↔Utf8View
    /// RowConverter probe to pin down before re-enabling.
    #[ignore = "Utf8/Utf8View RowConverter lookup miss — see comment"]
    #[serial]
    #[tokio::test]
    async fn update_with_source_buffered_only() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        // Walrus reads WALRUS_DATA_DIR from env — serialize for safety.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("b{}", test_id);
        let table = format!("b{}", test_id);

        let layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        // create_test_batch produces three rows with names test1/test2/test3
        // and matching ids span1/span2/span3.
        let (source, assignments) = build_update_source_for_id_rewrite(&[("test1", "rewritten-1"), ("test3", "rewritten-3")]);
        let updated = layer.update_with_source(&project, &table, None, &assignments, &source).unwrap();
        assert_eq!(updated, 2, "expected 2 rows matched by the join");

        let results = layer.query(&project, &table, &[]).unwrap();
        let combined = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
        let name_col = combined
            .column(combined.schema().index_of("name").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("name column should be Utf8");
        let id_col = combined
            .column(combined.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("id column should be Utf8");

        for i in 0..combined.num_rows() {
            match name_col.value(i) {
                "test1" => assert_eq!(id_col.value(i), "rewritten-1", "test1 row should have new id"),
                "test2" => assert_eq!(id_col.value(i), "span2", "test2 row was not in source; must be unchanged"),
                "test3" => assert_eq!(id_col.value(i), "rewritten-3", "test3 row should have new id"),
                other => panic!("unexpected row name {other}"),
            }
        }
    }

    /// WAL replay after restart: write an `UPDATE ... FROM` against
    /// MemBuffer-only rows, drop the layer, re-bootstrap a fresh layer over
    /// the same data dir, call `recover_from_wal`, and verify the update is
    /// replayed (id rewrites land in the recovered MemBuffer state).
    ///
    /// `#[ignore]`d: blocked on the same Utf8↔Utf8View lookup miss as
    /// `update_with_source_buffered_only`. The WAL serialization /
    /// deserialization plumbing is exercised end-to-end (`Insert` +
    /// `UpdateWithSource` entries round-trip through bincode + Arrow IPC),
    /// but the in-memory hash join after replay returns 0 matches for the
    /// same reason. Re-enable when the buffered-only test passes.
    #[ignore = "Utf8/Utf8View RowConverter lookup miss — see comment"]
    #[serial]
    #[tokio::test]
    async fn update_with_source_wal_replay_after_restart() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());

        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("u{}", test_id);
        let table = format!("u{}", test_id);

        // First instance: insert + UPDATE FROM, then drop without flushing
        // to Delta so the only durable record is the WAL.
        {
            let layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

            let (source, assignments) = build_update_source_for_id_rewrite(&[("test2", "post-replay-2")]);
            let updated = layer.update_with_source(&project, &table, None, &assignments, &source).unwrap();
            assert_eq!(updated, 1, "pre-restart update should affect exactly one row");
            // Layer drops here; WAL contains 1 Insert + 1 UpdateWithSource.
        }

        // Second instance: replay WAL into a fresh MemBuffer + verify the
        // UpdateWithSource entry reapplied.
        {
            let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(
                stats.entries_replayed >= 2,
                "expected ≥2 entries replayed (Insert + UpdateWithSource), got {stats:?}"
            );

            let results = layer.query(&project, &table, &[]).unwrap();
            assert!(!results.is_empty(), "expected rows after WAL recovery");
            let combined = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
            let name_col = combined
                .column(combined.schema().index_of("name").unwrap())
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let id_col = combined.column(combined.schema().index_of("id").unwrap()).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();

            let mut found_rewritten = false;
            for i in 0..combined.num_rows() {
                if name_col.value(i) == "test2" {
                    assert_eq!(
                        id_col.value(i),
                        "post-replay-2",
                        "WAL replay did not reapply UpdateWithSource — id should be 'post-replay-2'"
                    );
                    found_rewritten = true;
                }
            }
            assert!(found_rewritten, "test2 row missing after WAL replay");
        }
    }

    // #[serial] + own WALRUS_DATA_DIR: this test writes WAL via `insert`, and
    // walrus reads its data dir from the process-global WALRUS_DATA_DIR. Without
    // pinning it, a concurrent #[serial] test's dropped tempdir leaves the global
    // pointing at a deleted path → ENOENT on append (flaky in the full suite).
    #[serial]
    #[tokio::test]
    async fn test_pressure_pct() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
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

    /// After an insert, the FlushableBucket snapshot must carry the shard's
    /// pre-append cursor hold — the hold is what pins the WAL read cursor
    /// behind unflushed data (without it, the watermark drains straight to
    /// tail and a crash loses the bucket).
    #[serial]
    #[tokio::test]
    async fn wal_holds_recorded_on_insert() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("c{}", test_id);
        let table = format!("c{}", test_id);

        let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        // 3 batches → 3 WAL entries on one shard for this insert.
        let batches = vec![create_test_batch(&project), create_test_batch(&project), create_test_batch(&project)];
        layer.insert(&project, &table, batches).await.unwrap();

        let holds = layer.mem_buffer.wal_holds(&project, &table, layer.wal.shards_per_topic());
        assert!(
            holds.iter().any(Option::is_some),
            "insert must record a pre-append cursor hold on its shard, got {holds:?}"
        );
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
        let old_batch =
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts("old", "spanA", &project, old_ts)]).unwrap();
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
        let old_batch =
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts("seal", "spanA", &project, old_ts)]).unwrap();
        layer.insert(&project, &table, vec![old_batch]).await.unwrap();

        layer.flush_completed_buckets().await.unwrap();

        let wm = captured_wm.lock().unwrap().clone().expect("callback must have been invoked with a watermark");
        assert_eq!(wm.len(), layer.wal().shards_per_topic(), "watermark must have one entry per shard");
        // The metadata watermark is CONSERVATIVE: it includes this commit's
        // own holds, so it must never sit past the commit's own entries. The
        // single appended entry's pre-append hold is ORIGIN (fresh topic), so
        // no shard may report a position beyond origin — a boot-time
        // Delta-derived cursor then can't skip this commit's entries (which
        // matters when the commit goes gen-dirty and its post-DML state
        // still lives behind the cursor).
        assert!(
            wm.iter().all(|p| p.is_none() || p.is_some_and(|p| p.is_origin())),
            "conservative metadata watermark must not pass the commit's own entries; got {:?}",
            wm
        );
    }

    /// Regression: prod 2026-06-11 wedged with MemBuffer at 15.8GB > 8GB hard
    /// limit, every insert rejected with "Memory limit exceeded". The insert
    /// path must apply *backpressure* — synchronously flush MemBuffer → Delta to
    /// free RAM and retry — instead of dropping the write. Cap the budget at the
    /// 64MB floor (hard limit ~76.8MB) and push ~96MB of flushable (old-bucket)
    /// data through `insert()` with a working Delta callback. Pre-fix, the insert
    /// crossing the limit returns Err; post-fix every insert succeeds because the
    /// over-limit reservation drives a flush of the completed bucket first.
    #[serial]
    #[tokio::test]
    async fn backpressure_flushes_instead_of_rejecting() {
        use std::sync::atomic::{AtomicUsize, Ordering as O};

        use arrow::{
            array::{StringArray, TimestampMicrosecondArray},
            datatypes::{DataType, Field, Schema, TimeUnit},
        };

        let dir = tempdir().unwrap();
        let mut cfg = AppConfig::default();
        cfg.core.timefusion_data_dir = dir.path().to_path_buf();
        cfg.buffer.timefusion_buffer_max_memory_mb = 64; // floor → hard limit ~76.8MB
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
        let cfg = Arc::new(cfg);

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("bp{}", test_id);
        let table = format!("bp{}", test_id);

        let flush_calls = Arc::new(AtomicUsize::new(0));
        let fc = flush_calls.clone();
        let mut layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| {
            let c = fc.clone();
            Box::pin(async move {
                c.fetch_add(1, O::SeqCst);
                Ok(Vec::new())
            })
        }));
        let layer = Arc::new(layer);

        // Old timestamp → all rows land in a completed (flushable) bucket.
        let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let rows = 30_000usize;
        let make_batch = || {
            let ts = TimestampMicrosecondArray::from(vec![old_ts; rows]);
            let payload = StringArray::from(vec!["x".repeat(400); rows]); // ~12MB
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(payload)]).unwrap()
        };

        // ~96MB cumulative into a ~76.8MB buffer: at least one insert must cross
        // the hard limit and rely on backpressure. All must succeed.
        for i in 0..8 {
            layer
                .insert(&project, &table, vec![make_batch()])
                .await
                .unwrap_or_else(|e| panic!("insert {i} must succeed under backpressure, got: {e}"));
        }

        assert!(flush_calls.load(O::SeqCst) >= 1, "backpressure must have forced at least one Delta flush");
        assert!(
            layer.snapshot_stats().backpressure_engaged_total >= 1,
            "backpressure_engaged_total must record the over-limit event"
        );
    }

    /// Build a layer with a no-op (always-succeed) Delta callback and an old-bucket
    /// batch maker, sharing the over-limit setup of the two decouple tests below.
    /// `backpressure_secs = 0` makes `reserve_with_backpressure` return immediately
    /// on the over-limit insert (no relief loop), so the exhaustion path is
    /// deterministic without depending on flush timing.
    /// Returns the `TempDir` guard too — the caller must hold it for the test's
    /// lifetime (dropping it deletes the data dir out from under the layer).
    fn decouple_test_layer(decouple: bool) -> (Arc<BufferedWriteLayer>, TempDir, impl Fn() -> RecordBatch) {
        use arrow::{
            array::{StringArray, TimestampMicrosecondArray},
            datatypes::{DataType, Field, Schema, TimeUnit},
        };
        let dir = tempdir().unwrap();
        let mut cfg = AppConfig::default();
        cfg.core.timefusion_data_dir = dir.path().to_path_buf();
        cfg.buffer.timefusion_buffer_max_memory_mb = 64; // floor → hard limit ~76.8MB
        cfg.buffer.timefusion_write_backpressure_secs = 0; // exhaust immediately
        cfg.buffer.timefusion_wal_admit_decouple = decouple;
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::new(cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| Box::pin(async move { Ok(Vec::new()) })));
        let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let make_batch = move || {
            let rows = 30_000usize;
            let ts = TimestampMicrosecondArray::from(vec![old_ts; rows]);
            let payload = StringArray::from(vec!["x".repeat(400); rows]); // ~12MB
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(payload)]).unwrap()
        };
        (Arc::new(layer), dir, make_batch)
    }

    /// Baseline for the decouple flag: with it OFF and backpressure exhausted
    /// (timeout 0), an over-hard-limit insert is REJECTED — the loss seam parity
    /// plan Defect 1 targets (the rejected batch was never WAL-appended).
    #[serial]
    #[tokio::test]
    async fn wal_admit_decouple_off_rejects_when_backpressure_exhausted() {
        let (layer, _dir, make_batch) = decouple_test_layer(false);
        let mut rejected = false;
        for _ in 0..8 {
            if layer.insert("d", "d", vec![make_batch()]).await.is_err() {
                rejected = true;
                break;
            }
        }
        assert!(rejected, "flag OFF: an over-hard-limit insert must be rejected once backpressure is exhausted");
    }

    /// Parity plan Defect 1 fix (flag ON): the same exhausted-backpressure scenario
    /// must NOT drop. Every insert succeeds (admitted over-budget; the WAL append
    /// is the durability boundary) and the rows are retained — closing the
    /// drop-before-durability loss seam.
    #[serial]
    #[tokio::test]
    async fn wal_admit_decouple_on_never_drops_over_budget() {
        let (layer, _dir, make_batch) = decouple_test_layer(true);
        for i in 0..8 {
            layer
                .insert("d", "d", vec![make_batch()])
                .await
                .unwrap_or_else(|e| panic!("flag ON: insert {i} must be admitted over-budget, not dropped; got {e}"));
        }
        assert!(!layer.is_empty(), "admitted rows must be retained (durable), not dropped");
        // Over-budget by construction: effective memory exceeds the hard limit.
        let max = layer.max_memory_bytes();
        assert!(
            layer.effective_memory_bytes() > max,
            "decouple must admit past the hard limit ({}MB), got {}MB",
            max / (1024 * 1024),
            layer.effective_memory_bytes() / (1024 * 1024)
        );
    }

    /// The current (open) bucket is excluded from normal flushing, so a single
    /// busy 10-min window accumulates in RAM with nothing able to drain it.
    /// `force_flush_current_buckets` is the escalation tier that can. Prove the
    /// open bucket survives a completed-bucket flush but is drained by the force
    /// path.
    #[serial]
    #[tokio::test]
    async fn force_flush_current_bucket_drains_open_window() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("fc{}", test_id);
        let table = format!("fc{}", test_id);

        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| Box::pin(async move { Ok(Vec::new()) })));
        let layer = Arc::new(layer);

        // create_test_batch uses now() timestamps → the current (open) bucket.
        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        // Normal completed-bucket flush must NOT touch the open bucket.
        layer.flush_completed_buckets().await.unwrap();
        assert!(!layer.is_empty(), "completed-bucket flush must leave the open bucket in MemBuffer");

        // The force path reaches it.
        layer.force_flush_current_buckets().await.unwrap();
        assert!(layer.is_empty(), "force_flush_current_buckets must drain the open bucket");
    }

    /// Force-flushing the open bucket while an older completed bucket is
    /// still un-flushed must not lose the older bucket across a crash. Under
    /// the count-based cursor advance this ordering was unsafe (consuming the
    /// open bucket's count ate the older bucket's entries), so force-flush
    /// was gated off entirely — wedging a tenant under single-window
    /// pressure. The position watermark pins the cursor at the stuck bucket's
    /// first entry, so the force-flush proceeds AND the stuck bucket's rows
    /// survive crash + replay.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn force_flush_with_stuck_completed_bucket_keeps_it_durable() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("g{}", test_id);
        let table = format!("g{}", test_id);

        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls_cb = calls.clone();
        {
            let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| {
                let c = calls_cb.clone();
                Box::pin(async move {
                    c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(Vec::new())
                })
            }));
            let layer = Arc::new(layer);

            // Old (completed, never flushed) bucket + current (open) bucket.
            let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
            let old_batch =
                crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts("old", "spanA", &project, old_ts)]).unwrap();
            layer.insert(&project, &table, vec![old_batch]).await.unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

            layer.force_flush_current_buckets().await.unwrap();
            assert_eq!(
                calls.load(std::sync::atomic::Ordering::SeqCst),
                1,
                "force-flush must drain the open window even with a stuck completed bucket"
            );
            // Crash: drop without shutdown.
        }

        let layer = crate::test_utils::test_helpers::test_layer(cfg).unwrap();
        layer.recover_from_wal().await.unwrap();
        let ids = crate::test_utils::test_helpers::query_col_strings(&layer, &project, &table, "id");
        assert!(
            ids.contains(&"old".to_string()),
            "stuck completed bucket must survive force-flush + crash (got {ids:?})"
        );
    }

    /// Regression: a stuck completed bucket in ONE tenant must not freeze
    /// current-bucket force-flush for OTHER tenants — pre-fix a *global*
    /// stuck-bucket gate short-circuited the whole call, so one poison tenant
    /// wedged the entire instance at the hard limit (every insert rejected
    /// with "Memory limit exceeded"). T2's open window drains; T1's completed
    /// bucket isn't current, so the force path leaves it for the sealed flush.
    #[serial]
    #[tokio::test]
    async fn force_flush_isolates_stuck_tenant() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let t1 = format!("a{}", test_id); // stuck tenant: completed bucket, never flushed
        let t2 = format!("b{}", test_id); // healthy tenant: open bucket only

        let flushed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let flushed_cb = flushed.clone();
        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |p, _t, _b, _wm| {
            let f = flushed_cb.clone();
            Box::pin(async move {
                f.lock().unwrap().push(p);
                Ok(Vec::new())
            })
        }));
        let layer = Arc::new(layer);

        // T1: a sealed (completed) bucket left un-flushed → a bucket-before-current.
        let old_ts = crate::clock::now_micros() - 2 * crate::mem_buffer::bucket_duration_micros();
        let old_batch =
            crate::test_utils::test_helpers::json_to_batch(vec![crate::test_utils::test_helpers::test_span_ts("old", "spanA", &t1, old_ts)]).unwrap();
        layer.insert(&t1, &t1, vec![old_batch]).await.unwrap();
        // T2: only an open (current) bucket.
        layer.insert(&t2, &t2, vec![create_test_batch(&t2)]).await.unwrap();

        layer.force_flush_current_buckets().await.unwrap();

        let flushed = flushed.lock().unwrap().clone();
        assert!(
            flushed.contains(&t2),
            "healthy tenant's open bucket must force-flush despite a stuck tenant; flushed={:?}",
            flushed
        );
        assert!(
            !flushed.contains(&t1),
            "stuck tenant's completed bucket must stay un-advanced (per-topic WAL gate); flushed={:?}",
            flushed
        );
    }

    /// Regression: prod 2026-07-01 wedged with the write buffer pinned at the
    /// hard limit, 1300+ inserts rejected, yet 0 flushes / 0 flush errors — a
    /// Delta commit hung inside `flush_bucket`, pinning `flush_lock` forever so
    /// no relief could free memory and the stall was invisible. The
    /// `flush_bucket_timeout` watchdog must abort a hung commit: the flush
    /// returns instead of hanging, rows are restored to MemBuffer (still durable
    /// in the WAL), and the lock releases so the next cycle retries. Pre-fix this
    /// test hangs forever (outer timeout fires); post-fix it completes in ~1s.
    #[serial]
    #[tokio::test]
    async fn flush_bucket_watchdog_aborts_hung_commit() {
        let dir = tempdir().unwrap();
        let mut cfg = AppConfig::default();
        cfg.core.timefusion_data_dir = dir.path().to_path_buf();
        cfg.buffer.timefusion_flush_bucket_timeout_secs = 1; // trip the watchdog fast
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
        let cfg = Arc::new(cfg);

        let test_id = &uuid::Uuid::new_v4().to_string()[..4];
        let project = format!("w{}", test_id);
        let table = format!("w{}", test_id);

        let mut layer = crate::test_utils::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        // Callback that never resolves — models a stalled S3/commit-lock wait.
        layer.delta_write_callback = Some(Arc::new(move |_p, _t, _b, _wm| Box::pin(std::future::pending())));
        let layer = Arc::new(layer);

        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        // Outer bound proves the watchdog broke the hang (well above the 1s
        // watchdog, well below "forever").
        let res = tokio::time::timeout(Duration::from_secs(10), layer.force_flush_current_buckets()).await;
        assert!(
            res.is_ok(),
            "force_flush must return once the flush watchdog trips — it hung waiting on the stalled commit"
        );
        res.unwrap().unwrap();

        // Commit never succeeded → rows restored, still buffered (and in the WAL).
        assert!(!layer.is_empty(), "a timed-out flush must restore the bucket, not drop it");
    }

    #[serial]
    #[tokio::test]
    async fn test_memory_reservation() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };

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
