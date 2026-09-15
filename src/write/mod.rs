pub mod mem_buffer;
pub mod wal;

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arrow::array::RecordBatch;
use dashmap::DashMap;
use futures::stream::{self, StreamExt};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    config::AppConfig,
    observability::wal_err,
    write::mem_buffer::{FlushableBucket, MemBuffer, MemBufferStats, batch_timestamp_range, estimate_batch_size, strip_column_qualifiers},
    write::wal::{DeletePayload, UpdatePayload, UpdateWithSourcePayload, WalEntry, WalManager, WalOperation, decode_payload, deserialize_record_batch},
};

// Safety margin over `estimate_batch_size()` for costs it can't see: Vec
// headers, DashMap node overhead, allocator fragmentation.
const MEMORY_OVERHEAD_MULTIPLIER: f64 = 1.15;

/// Estimated reserved bytes for a write: raw Arrow size × the overhead multiplier.
fn estimate_reservation(batches: &[RecordBatch]) -> usize {
    let batch_size: usize = batches.iter().map(estimate_batch_size).sum();
    (batch_size as f64 * MEMORY_OVERHEAD_MULTIPLIER) as usize
}

/// Fill ratio (0..=100) of `used` against the budget, clamped.
fn fill_pct(used: usize, max_bytes: usize) -> u32 {
    ((used as u128 * 100 / max_bytes.max(1) as u128).min(100)) as u32
}

/// Hard limit = `max_bytes + max_bytes / N` = 120% of budget (`5` → +20%).
const HARD_LIMIT_HEADROOM_DIVISOR: usize = 5;
/// Bucket-id time slices one flush cycle commits before releasing `flush_lock`,
/// so a deep backlog drains as many small commits that each finish.
const FLUSH_CHUNK_BUCKET_IDS: usize = 2;
/// The reservation ceiling live writers are rejected at.
fn hard_limit(max_bytes: usize) -> usize {
    max_bytes.saturating_add(max_bytes / HARD_LIMIT_HEADROOM_DIVISOR)
}

const MAX_CAS_RETRIES: u32 = 100;
/// Attempts spent spinning before backing off with a sleep.
const CAS_SPIN_ATTEMPTS: u32 = 5;
const CAS_BACKOFF_BASE_MICROS: u64 = 1;
/// Caps the backoff delay at ~1ms.
const CAS_BACKOFF_MAX_EXPONENT: u32 = 10;

/// Create `path` for writing with owner-only (0600) permissions on Unix.
/// `exclusive` fails if the file already exists.
pub(crate) fn create_owner_only(path: &std::path::Path, exclusive: bool) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    // `create_new` supersedes create/truncate, so the flags are just !exclusive.
    opts.write(true).create_new(exclusive).create(!exclusive).truncate(!exclusive);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// Write raw bytes with owner-only (0600) permissions on Unix.
pub(crate) fn write_owner_only(path: &std::path::Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = create_owner_only(path, false)?;
    f.write_all(contents)?;
    f.sync_all()
}

/// 2000-01-01T00:00:00Z. Timestamps below this, or more than
/// [`EVENT_TIME_MAX_FUTURE_MICROS`] past ingest, are client unit errors
/// (seconds/millis where micros belong), not data.
const EVENT_TIME_MIN_MICROS: i64 = 946_684_800_000_000;
const EVENT_TIME_MAX_FUTURE_MICROS: i64 = 48 * 3600 * 1_000_000;

/// Admission-time bound on the table's event-time column: rows outside
/// [2000-01-01, now+48h] are dropped. Null timestamps and non-microsecond
/// columns pass through untouched. Must run before the WAL append.
fn bound_event_time(project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    use arrow::array::TimestampMicrosecondArray;
    let time_col = crate::dml::table_time_column(table_name);
    let hi = crate::support::now_micros() + EVENT_TIME_MAX_FUTURE_MICROS;
    let mut dropped = 0u64;
    let bounded: Vec<RecordBatch> = batches
        .into_iter()
        .filter_map(|batch| {
            let Some(ts) = batch.column_by_name(time_col).and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>()) else {
                return Some(batch);
            };
            if let (Some(min), Some(max)) = (arrow::compute::min(ts), arrow::compute::max(ts))
                && min >= EVENT_TIME_MIN_MICROS
                && max <= hi
            {
                return Some(batch);
            }
            let mask: arrow::array::BooleanArray = ts.iter().map(|v| Some(v.is_none_or(|v| (EVENT_TIME_MIN_MICROS..=hi).contains(&v)))).collect();
            dropped += mask.false_count() as u64;
            match arrow::compute::filter_record_batch(&batch, &mask) {
                Ok(kept) if kept.num_rows() == 0 => None,
                Ok(kept) => Some(kept),
                Err(e) => {
                    error!("event-time bound filter failed, admitting batch unfiltered: {e}");
                    Some(batch)
                }
            }
        })
        .collect();
    if dropped > 0 {
        warn!("dropped {dropped} rows with event timestamps outside [2000-01-01, now+48h] for {project_id}/{table_name} — client timestamp unit error?");
        crate::observability::record_event_time_bounded(project_id, table_name, dropped);
    }
    bounded
}

/// Returns false when the payload could not be persisted — the WAL is then
/// the entry's ONLY copy, and recovery must not advance past it.
fn quarantine_entry(quarantine_dir: &std::path::Path, entry: &WalEntry, kind: &str, reason: &str) -> bool {
    if let Err(e) = std::fs::create_dir_all(quarantine_dir) {
        error!("Failed to create WAL quarantine dir {:?}: {}", quarantine_dir, e);
        return false;
    }
    let WalEntry { timestamp_micros, project_id, table_name, operation, data } = entry;
    // project:table can contain chars unusable in a filename
    let topic = format!("{project_id}__{table_name}").replace(['/', '\\', ':', '\0'], "_");
    let path = quarantine_dir.join(format!("{timestamp_micros}_{kind}_{topic}.bin"));
    // Raw user data that failed to deserialize — 0600, not world-readable.
    if let Err(e) = write_owner_only(&path, data) {
        error!("Failed to write quarantine file {:?}: {}", path, e);
        return false;
    }
    let meta_path = path.with_extension("meta");
    let meta = format!(
        "ts_micros={timestamp_micros}\nproject_id={project_id}\ntable_name={table_name}\noperation={operation}\nkind={kind}\nreason={reason}\nbytes={}\n",
        data.len()
    );
    if let Err(e) = write_owner_only(&meta_path, meta.as_bytes()) {
        error!("Failed to write quarantine meta {:?}: {}", meta_path, e);
    }
    error!("Quarantined WAL entry to {:?} (kind={}, bytes={})", path, kind, data.len());
    crate::observability::record_wal_corruption();
    true
}

/// Operator-visible snapshot of the BufferedWriteLayer state.
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    pub mem_project_count: usize,
    pub mem_total_buckets: usize,
    pub mem_total_rows: usize,
    pub mem_total_batches: usize,
    pub mem_estimated_bytes: usize,
    /// WAL-replay DML entries consumed as no-ops (table had no buffered rows).
    pub mem_replay_dml_noops: u64,
    pub reserved_bytes: usize,
    pub max_memory_bytes: usize,
    pub pressure_pct: u32,
    pub wal_files: usize,
    pub wal_disk_bytes: u64,
    /// Parked payloads awaiting a human re-drive; not counted in
    /// `wal_disk_bytes`. Non-zero means deferred data loss.
    pub quarantine_files: usize,
    pub quarantine_bytes: u64,
    pub wal_shards_per_topic: usize,
    pub wal_known_topics: usize,
    pub bucket_duration_micros: i64,
    /// Oldest flushable bucket's flush-dwell in secs (`now - bucket creation
    /// time`), None when none past the open window. Wait-to-flush, NOT the
    /// rows' event-time age, so backfill can't false-trip it.
    pub oldest_bucket_age_secs: Option<u64>,
    pub flush_completed_total: u64,
    pub flush_failed_total: u64,
    /// Inserts that hit the hard limit and applied backpressure (sync flush)
    /// instead of rejecting.
    pub backpressure_engaged_total: u64,
    /// Inserts rejected after backpressure failed to free memory.
    pub backpressure_rejected_total: u64,
    /// Open-bucket force-flush escalations.
    pub backpressure_force_flush_total: u64,
    pub rows_ingested_total: u64,
    pub rows_flushed_total: u64,
    pub flush_freed_bytes_total: u64,
    /// Real process RSS (Linux `/proc/self/statm`), None off-Linux.
    pub process_rss_bytes: Option<usize>,
    /// Topics whose failed-commit holds could not be restored to MemBuffer —
    /// their rows exist ONLY in the WAL until a restart replays them, and
    /// each pins the WAL GC floor for its files.
    pub orphaned_topics: usize,
    /// Age (secs) of the oldest orphan's GC-floor pin. None when no orphan
    /// carries a pin.
    pub orphan_pin_age_secs: Option<u64>,
    /// True when no buffered, airborne, or orphaned WAL-backed work remains.
    pub drained: bool,
    pub wal_recovery_duration_ms: u64,
    /// Rows re-inserted by replay. Replay is deliberately not idempotent — a
    /// failed cursor advance re-replays rows already in Delta; dedup collapses them.
    pub wal_replay_rows: u64,
    /// Flushes declined because their rows were provably already committed,
    /// and the rows those flushes would have re-written.
    pub landed_skips_total: u64,
    pub landed_skipped_rows_total: u64,
    /// True only after startup WAL recovery has returned successfully.
    pub wal_recovery_complete: bool,
    /// Committed Parquet files awaiting post-replay Tantivy indexing.
    pub tantivy_recovery_pending_files: usize,
    /// Process start time; distinguishes a replacement from its predecessor.
    pub boot_micros: i64,
}

#[derive(Debug, Default)]
pub struct RecoveryStats {
    pub entries_replayed: u64,
    pub batches_recovered: u64,
    pub oldest_entry_timestamp: Option<i64>,
    pub newest_entry_timestamp: Option<i64>,
    pub recovery_duration_ms: u64,
    pub corrupted_entries_skipped: u64,
    pub tantivy_files_deferred: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeferredTantivyFile {
    pub project_id: String,
    pub table_name: String,
    pub uri: String,
}

fn deferred_tantivy_path(config: &AppConfig) -> std::path::PathBuf {
    config.core.timefusion_data_dir.join("tantivy-recovery-pending.json")
}

/// Called under `deferred_tantivy_files`'s `std::sync::Mutex` from async tasks,
/// so the blocking write must run off the worker's queue.
fn persist_deferred_tantivy_files(path: &std::path::Path, files: &[DeferredTantivyFile]) {
    crate::support::without_blocking_the_worker(|| {
        let Ok(bytes) = serde_json::to_vec(files) else { return };
        let tmp = path.with_extension("tmp");
        if std::fs::create_dir_all(path.parent().unwrap_or_else(|| std::path::Path::new("."))).is_ok() && std::fs::write(&tmp, bytes).is_ok() {
            let _ = std::fs::rename(tmp, path);
        }
    });
}

#[derive(Debug, Default)]
pub struct FlushStats {
    pub buckets_flushed: u64,
    pub buckets_failed: u64,
    pub total_rows: u64,
}

/// MemBuffer bytes a flush reclaims. Must use the same `estimate_batch_size`
/// as the per-bucket accounting so the totals stay comparable.
fn flushable_bytes(b: &FlushableBucket) -> u64 {
    b.batches.iter().map(estimate_batch_size).sum::<usize>() as u64
}

/// How recently a flush failure still counts as "flush is broken" for the
/// compaction brake: a few flush cycles, but short enough that one transient S3
/// error doesn't park compaction for the rest of the hour.
const FLUSH_FAILURE_BRAKE_WINDOW_MICROS: i64 = 5 * 60 * 1_000_000;

fn wal_backlog_over_threshold(backlog_bytes: u64, max_unflushed_bytes: u64, last_flush_failure_micros: i64, now_micros: i64) -> bool {
    (max_unflushed_bytes > 0 && backlog_bytes > max_unflushed_bytes)
        // Flush broken ⇒ the backlog is about to be real regardless of its
        // current size: brake so compaction isn't competing with recovery.
        || (last_flush_failure_micros > 0 && now_micros.saturating_sub(last_flush_failure_micros) < FLUSH_FAILURE_BRAKE_WINDOW_MICROS)
}

/// The bytes leg is UNFLUSHED backlog, never on-disk size, so leftover WAL
/// files on disk cannot trip it.
fn wal_emergency_flush_needed(file_count: usize, max_files: usize, unflushed_bytes: u64, max_unflushed_bytes: u64) -> bool {
    (max_files > 0 && file_count > max_files) || unflushed_bytes > max_unflushed_bytes
}

/// Per-shard walrus watermark snapshot at bucket-seal time. `None` for shards
/// the bucket never wrote to. Written into the Delta commit metadata so a
/// crash-mid-flush can derive the cursor from Delta on restart.
pub type DeltaWatermark = Vec<Option<walrus_rust::WalPosition>>;

/// Callback for writing batches to Delta Lake. It MUST complete the Delta
/// commit (including S3 upload) before returning Ok, return Err if the commit
/// fails, and return the URIs of files this commit added (sidecar indexers use
/// them for later GC). WAL entries are marked consumed only after Ok.
pub type DeltaWriteCallback =
    Arc<dyn Fn(String, String, Vec<RecordBatch>, DeltaWatermark) -> futures::future::BoxFuture<'static, anyhow::Result<Vec<String>>> + Send + Sync>;

/// Width of a landed-batch identity; 128 bits for the few dozen identities
/// live per topic.
pub const DIGEST_BYTES: usize = 16;

/// Identity of a batch set. See [`landed_digest`].
pub type LandedDigest = [u8; DIGEST_BYTES];

/// Multiple of `delta_scan_depth` bounding the in-process landed-identity set
/// per topic. Only duplicates are at stake if it is too small.
const LANDED_WINDOW_COMMITS: usize = 4;

/// Whether "identical content" is safe to read as "already durable" for this
/// table — only when it declares `dedup_keys`. On an append-only table two
/// byte-identical batches are two distinct facts, so dropping one is a loss.
pub(crate) fn landed_identity_applies(table_name: &str) -> bool {
    crate::schema::get_schema(table_name).is_some_and(|s| !s.dedup_keys.is_empty())
}

/// Identity of a set of batches, used to decline a flush whose rows are
/// provably already committed.
///
/// One hash per batch, combined by **wrapping addition**: commutative, so the
/// digest is immune to batch ORDER, and unlike XOR it does not cancel in pairs
/// (two identical batches must not digest as no batches at all).
///
/// The bytes hashed are the batch's IPC round-trip fixed point, not its current
/// encoding — a client batch and the same batch rebuilt from the WAL serialize
/// differently, while a second round-trip is stable.
///
/// `None` for an empty set and on any serialization failure — both mean "no
/// identity", which declines the skip and flushes normally.
pub fn landed_digest(batches: &[RecordBatch]) -> Option<LandedDigest> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return None;
    }
    batches
        .iter()
        .try_fold(0u128, |acc, batch| {
            let once = crate::write::wal::serialize_record_batch(batch).ok()?;
            let canonical = crate::write::wal::deserialize_record_batch(&once)
                .ok()
                .map(crate::write::mem_buffer::compact_batch)
                .and_then(|b| crate::write::wal::serialize_record_batch(&b).ok())?;
            Some(acc.wrapping_add(twox_hash::XxHash3_128::oneshot(&canonical)))
        })
        .map(u128::to_be_bytes)
}

/// Per-row (key_hash, content_hash) for ingest-time client-retry dedup.
/// `key_idxs` are the schema's dedup-key columns; `content_idxs` are ALL
/// columns EXCEPT the TF-stamped tiebreak (it is overwritten per batch, so a
/// retry could never match on it). A row is a client-retry duplicate iff BOTH
/// hashes match a previously-flushed row. `None` on any encoding failure
/// (fail-open: keep the row — a duplicate at worst).
pub fn per_row_identities(batch: &RecordBatch, key_idxs: &[usize], content_idxs: &[usize]) -> Option<Vec<(u128, u128)>> {
    Some(row_hashes(batch, key_idxs)?.into_iter().zip(row_hashes(batch, content_idxs)?).collect())
}

/// One 128-bit hash per row over the given column subset, via Arrow's row
/// format (which delimits variable-length fields, so `("ab","c")` and
/// `("a","bc")` differ). `None` on any encoding failure — caller keeps the row.
pub fn row_hashes(batch: &RecordBatch, idxs: &[usize]) -> Option<Vec<u128>> {
    use datafusion::arrow::row::{RowConverter, SortField};
    if batch.num_rows() == 0 {
        return Some(Vec::new());
    }
    let (fields, cols): (Vec<SortField>, Vec<_>) = idxs.iter().map(|&i| (SortField::new(batch.column(i).data_type().clone()), batch.column(i).clone())).unzip();
    let rows = RowConverter::new(fields).ok()?.convert_columns(&cols).ok()?;
    Some((0..batch.num_rows()).map(|r| twox_hash::XxHash3_128::oneshot(rows.row(r).as_ref())).collect())
}

/// Column indices feeding [`per_row_identities`] for one batch of `table_name`:
/// `(key_idxs, content_idxs)`. `None` when the identity is undefined for this
/// batch (no schema, no `dedup_keys`, or a key column absent) — the
/// probe/populate is then skipped.
pub fn ingest_identity_idxs(table_name: &str, schema: &arrow::datatypes::Schema) -> Option<(Vec<usize>, Vec<usize>)> {
    let spec = crate::schema::get_schema(table_name)?;
    if spec.dedup_keys.is_empty() {
        return None;
    }
    let key_idxs = spec.dedup_keys.iter().map(|k| schema.index_of(k).ok()).collect::<Option<Vec<_>>>()?;
    let tiebreak = if spec.version_append { spec.dedup_tiebreak.as_deref() } else { None };
    let content_idxs = (0..schema.fields().len()).filter(|&i| Some(schema.field(i).name().as_str()) != tiebreak).collect();
    Some((key_idxs, content_idxs))
}

/// Two-stage dedup filter for ONE batch against the recently-flushed index:
/// `(kept_batch, key_hits, dropped_rows)`; `None` when every row was a dup.
/// Stage 1 probes dedup-key hashes only; stage 2 hashes full content for just
/// the key-hit rows. Every failure direction fails OPEN (keep the row — a
/// duplicate at worst, resolved by DV-dedup downstream; never a loss).
pub fn ingest_dedup_filter_batch(idx: &IngestDedupIndex, table_name: &str, batch: RecordBatch) -> (Option<RecordBatch>, u64, u64) {
    let pass = |b: RecordBatch, hits: u64| (Some(b), hits, 0);
    let Some((key_idxs, content_idxs)) = ingest_identity_idxs(table_name, &batch.schema()) else { return pass(batch, 0) };
    let Some(key_hashes) = row_hashes(&batch, &key_idxs) else { return pass(batch, 0) };
    let hits: Vec<_> = key_hashes
        .iter()
        .enumerate()
        .filter_map(|(r, &k)| {
            let c = idx.key_contents(k);
            (c.0.is_some() || c.1.is_some()).then_some((r, c))
        })
        .collect();
    if hits.is_empty() {
        return pass(batch, 0);
    }
    let key_hits = hits.len() as u64;
    let indices = arrow::array::UInt64Array::from(hits.iter().map(|&(r, _)| r as u64).collect::<Vec<_>>());
    let Ok(sub) = datafusion::arrow::compute::take_record_batch(&batch, &indices) else { return pass(batch, key_hits) };
    let Some(content_hashes) = row_hashes(&sub, &content_idxs) else { return pass(batch, key_hits) };
    let dropped: HashSet<usize> =
        hits.iter().zip(&content_hashes).filter(|&(&(_, (cur, prev)), &c)| cur == Some(c) || prev == Some(c)).map(|(&(r, _), _)| r).collect();
    if dropped.is_empty() {
        return pass(batch, key_hits);
    }
    if dropped.len() == batch.num_rows() {
        return (None, key_hits, dropped.len() as u64);
    }
    let keep = arrow::array::BooleanArray::from((0..batch.num_rows()).map(|r| !dropped.contains(&r)).collect::<Vec<bool>>());
    match datafusion::arrow::compute::filter_record_batch(&batch, &keep) {
        Ok(kept) => (Some(kept), key_hits, dropped.len() as u64),
        Err(_) => pass(batch, key_hits),
    }
}

/// Bounded recently-flushed content-identity index for ingest-time client-retry
/// dedup. Two epochs so eviction is O(1): a probe checks both `current` and
/// `previous`; a populate writes `current`; a rotation drops `previous`, moves
/// `current` there, and starts a fresh `current`. Coverage oscillates between
/// window/2 and window.
///
/// The `RwLock` guards only the two `Arc<DashMap>` handles, so probe/populate
/// do not serialize. The bounds limit coverage, never correctness: an evicted
/// identity costs a duplicate (DV-dedup backstop), never a loss.
pub struct IngestDedupIndex {
    epochs: std::sync::RwLock<IngestEpochs>,
    /// Rotate when `current` reaches this many entries OR is this old.
    rotate_at_entries: usize,
    rotate_at_micros: i64,
}

struct IngestEpochs {
    current: Arc<DashMap<u128, u128>>,
    previous: Arc<DashMap<u128, u128>>,
    current_started_micros: i64,
}

/// Per-(project, table) byte budget for the ingest-dedup index. Its own budget,
/// OUTSIDE the MemBuffer cap.
const INGEST_DEDUP_MAX_BYTES: usize = 256 * 1024 * 1024;
/// Retry-coverage window; the epoch pair rotates at window/2.
const INGEST_DEDUP_WINDOW_MICROS: i64 = 6 * 3600 * 1_000_000;

impl IngestDedupIndex {
    /// `max_bytes` is the index's own budget (outside the MemBuffer cap);
    /// `window_micros` is the retry-coverage window (epochs rotate at window/2).
    pub fn new(max_bytes: usize, window_micros: i64, now_micros: i64) -> Self {
        const BYTES_PER_ENTRY: usize = 50; // 32 B payload + DashMap overhead
        Self {
            epochs: std::sync::RwLock::new(IngestEpochs {
                current: Arc::new(DashMap::new()),
                previous: Arc::new(DashMap::new()),
                current_started_micros: now_micros,
            }),
            rotate_at_entries: (max_bytes / 2 / BYTES_PER_ENTRY).max(1),
            rotate_at_micros: (window_micros / 2).max(1),
        }
    }

    /// True iff `(key_hash, content_hash)` matches a previously-populated row.
    /// A key hit with DIFFERENT content is a new version, not a duplicate.
    #[cfg(test)]
    pub(crate) fn is_duplicate(&self, key_hash: u128, content_hash: u128) -> bool {
        self.probe(key_hash, content_hash).1
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, IngestEpochs> {
        self.epochs.read().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `(key_hit, duplicate)`; a duplicate is always a key hit.
    pub fn probe(&self, key_hash: u128, content_hash: u128) -> (bool, bool) {
        let (cur, prev) = self.key_contents(key_hash);
        (cur.is_some() || prev.is_some(), cur == Some(content_hash) || prev == Some(content_hash))
    }

    /// Content hashes recorded for `key_hash` in the (current, previous) epochs.
    /// Both are returned: a key re-populated in `current` with newer content
    /// must not shadow the flushed original still in `previous`.
    pub fn key_contents(&self, key_hash: u128) -> (Option<u128>, Option<u128>) {
        let (current, previous) = {
            let e = self.read();
            (Arc::clone(&e.current), Arc::clone(&e.previous))
        };
        (current.get(&key_hash).map(|c| *c), previous.get(&key_hash).map(|c| *c))
    }

    /// Record a flushed row's identity; last write wins on `key_hash`.
    pub fn populate(&self, key_hash: u128, content_hash: u128) {
        // Clone the handle out so the read lock is dropped before the insert.
        let current = Arc::clone(&self.read().current);
        current.insert(key_hash, content_hash);
    }

    /// Rotate epochs if `current` is over its size or age bound; true iff this
    /// call rotated. Idempotent under contention (a double rotation only
    /// shrinks coverage).
    pub fn maybe_rotate(&self, now_micros: i64) -> bool {
        let due = |e: &IngestEpochs| e.current.len() >= self.rotate_at_entries || now_micros - e.current_started_micros >= self.rotate_at_micros;
        if !due(&self.read()) {
            return false;
        }
        let mut e = self.epochs.write().unwrap_or_else(std::sync::PoisonError::into_inner);
        // Re-check under the write lock (another thread may have rotated).
        if due(&e) {
            e.previous = std::mem::replace(&mut e.current, Arc::new(DashMap::new()));
            e.current_started_micros = now_micros;
            return true;
        }
        false
    }

    /// Total live entries across both epochs.
    pub fn entries(&self) -> usize {
        let e = self.read();
        e.current.len() + e.previous.len()
    }
}

/// One (project, table) group of a flush tick, handed to the coalescing writer
/// so all of them share ONE Delta commit per physical table.
pub struct FlushUnit {
    pub project_id: String,
    pub table_name: String,
    pub batches: Vec<RecordBatch>,
    pub watermark: DeltaWatermark,
}

/// Cross-project flush commit coalescing: writes every unit's parquet but
/// emits ONE commit per physical Delta table. MUST return exactly one result
/// per input unit, in input order — each drives that project's own
/// settle/requeue, so a short or reordered vector would strand buckets.
pub type DeltaCoalescedWriteCallback = Arc<dyn Fn(Vec<FlushUnit>) -> futures::future::BoxFuture<'static, Vec<anyhow::Result<Vec<String>>>> + Send + Sync>;

/// Folds every per-bucket `FlushableBucket` for one (project_id, table_name)
/// into a single combined commit.
#[derive(Default)]
struct CoalescedGroup {
    batches: Vec<RecordBatch>,
    row_count: usize,
    /// Per-shard min hold across absorbed buckets; registered as the commit's
    /// in-flight cursor hold while it is airborne.
    wal_first_positions: ShardHolds,
    /// Taken source buckets, kept whole so a failed commit can restore each one
    /// (rows + holds) to MemBuffer. Batches are Arc-backed: pointers, not data.
    source_buckets: Vec<crate::write::mem_buffer::FlushableBucket>,
    /// Option so the derived Default's 0 can't corrupt the min.
    min_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    /// Min WAL GC floor across absorbed buckets.
    first_wal_pin: Option<i64>,
}

struct CombinedBucket {
    combined: crate::write::mem_buffer::FlushableBucket,
    source_buckets: Vec<crate::write::mem_buffer::FlushableBucket>,
}

/// Per-shard min-merge of cursor holds: the combined hold is the earliest
/// position any input still pins. An empty slice is the identity.
fn merge_wal_holds(a: &[Option<walrus_rust::WalPosition>], b: &[Option<walrus_rust::WalPosition>]) -> ShardHolds {
    (0..a.len().max(b.len()))
        .map(|i| match (a.get(i).copied().flatten(), b.get(i).copied().flatten()) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (x, y) => x.or(y),
        })
        .collect()
}

impl CoalescedGroup {
    fn absorb(&mut self, b: crate::write::mem_buffer::FlushableBucket) {
        self.row_count += b.row_count;
        self.batches.extend(b.batches.iter().cloned());
        self.wal_first_positions = merge_wal_holds(&self.wal_first_positions, &b.wal_first_positions);
        self.min_timestamp = Some(self.min_timestamp.map_or(b.min_timestamp, |m| m.min(b.min_timestamp)));
        self.max_timestamp = Some(self.max_timestamp.map_or(b.max_timestamp, |m| m.max(b.max_timestamp)));
        self.first_wal_pin = Some(self.first_wal_pin.map_or(b.first_wal_pin_micros, |m| m.min(b.first_wal_pin_micros)));
        self.source_buckets.push(b);
    }

    fn into_combined_bucket(self, project_id: String, table_name: String) -> CombinedBucket {
        let CoalescedGroup { batches, row_count, wal_first_positions, source_buckets, min_timestamp, max_timestamp, first_wal_pin } = self;
        // Max source bucket_id, a stable identifier for tracing only.
        let bucket_id = source_buckets.iter().map(|b| b.bucket_id).max().unwrap_or(0);
        let combined = crate::write::mem_buffer::FlushableBucket {
            project_id,
            table_name,
            bucket_id,
            batches,
            row_count,
            wal_first_positions,
            snapshot_gen: 0, // per-source-bucket gens are checked via source_buckets
            min_timestamp: min_timestamp.unwrap_or(i64::MAX),
            max_timestamp: max_timestamp.unwrap_or(i64::MIN),
            first_wal_pin_micros: first_wal_pin.unwrap_or(i64::MAX),
            // Built from snapshots (buckets keep their pins); u64::MAX release is a no-op.
            taking_pin_seq: u64::MAX,
        };
        CombinedBucket { combined, source_buckets }
    }
}

/// Invoked AFTER a successful Delta commit with
/// `(project_id, table_name, batches, added_file_uris)` to build and upload a
/// sidecar index. Failures are logged but DO NOT fail the flush.
pub type TantivyIndexCallback =
    Arc<dyn Fn(String, String, Vec<RecordBatch>, Vec<String>) -> futures::future::BoxFuture<'static, anyhow::Result<()>> + Send + Sync>;

#[derive(derive_more::Debug)]
#[debug("BufferedWriteLayer {{ has_callback: {} }}", delta_write_callback.is_some())]
pub struct BufferedWriteLayer {
    config: Arc<AppConfig>,
    wal: Arc<WalManager>,
    mem_buffer: Arc<MemBuffer>,
    shutdown: CancellationToken,
    /// Write-admission barrier for graceful handoff. Must be closed before
    /// server connection drain, so already-accepted PGWire sockets cannot
    /// append after the shutdown flush/snapshot.
    accepting_writes: AtomicBool,
    active_writes: AtomicU64,
    writes_drained: Notify,
    /// Invalidates leased pre-deploy write fences. Shutdown increments it
    /// again, so an old lease timer can never reopen admission after SIGTERM.
    handoff_generation: AtomicU64,
    /// True only after HANDOFF fenced admission, quiesced admitted writers,
    /// and flushed every WAL-backed hold. A start-first replacement may then
    /// ask this process to relinquish the single-writer WAL lock.
    deploy_handoff_ready: AtomicBool,
    delta_write_callback: Option<DeltaWriteCallback>,
    /// Used instead of `delta_write_callback` when
    /// `TIMEFUSION_FLUSH_COALESCE_COMMITS` is on.
    coalesced_write_callback: Option<DeltaCoalescedWriteCallback>,
    tantivy_index_callback: Option<TantivyIndexCallback>,
    background_tasks: Mutex<Vec<JoinHandle<()>>>,
    flush_lock: Mutex<()>,
    // Single-flights insert-path backpressure relief: only the writer that wins
    // this try_lock drives a relief flush. Must stay distinct from `flush_lock`
    // so relief never blocks behind a routine background flush.
    relief_lock: Mutex<()>,
    reserved_bytes: AtomicUsize,  // Memory reserved for in-flight writes
    pressure_notify: Arc<Notify>, // Wakes flush task when pressure threshold crossed
    /// Notified at the end of every flush task iteration (success or failure);
    /// lets tests await background work instead of racing wall-clock sleeps.
    flush_tick_notify: Arc<Notify>,
    /// Notified at the end of every eviction task iteration.
    eviction_tick_notify: Arc<Notify>,
    flush_completed_total: AtomicU64,
    flush_failed_total: AtomicU64,
    /// `support::now_micros()` of the most recent flush failure (0 = never).
    /// A broken flush is a durability backlog even while the backlog gauge is
    /// briefly small, so the compaction brake ORs on its recency.
    last_flush_failure_micros: AtomicI64,
    backpressure_engaged_total: AtomicU64,
    backpressure_rejected_total: AtomicU64,
    backpressure_force_flush_total: AtomicU64,
    /// Set by the flush loop when on-disk WAL bytes exceed the HARD limit
    /// (`wal_hard_limit_bytes`); while set, `insert` rejects instead of acking
    /// into an unbounded backlog (the upstream DLQ absorbs + replays). Cleared
    /// by the same loop once the backlog drains below the limit.
    wal_hard_backpressure: AtomicBool,
    /// Rows accepted into MemBuffer (post-WAL) vs rows drained to Delta.
    rows_ingested_total: AtomicU64,
    rows_flushed_total: AtomicU64,
    flush_freed_bytes_total: AtomicU64,
    // Required for WAL replay of UPDATE/DELETE whose SQL references UDFs.
    function_registry: Arc<crate::read::functions::FnRegistry>,
    /// Caps concurrent detached tantivy sidecar builds. Handles aren't stored,
    /// so graceful shutdown does not await in-flight tantivy uploads — the
    /// sidecar is best-effort and rebuildable from Delta.
    tantivy_spawn_sem: Arc<tokio::sync::Semaphore>,
    /// Per-(project, table) max row timestamp ever handed to a Delta commit this
    /// process lifetime, floored at `boot_micros`. A query whose lower time
    /// bound is above it can skip the Delta scan. Must be raised BEFORE the
    /// commit so a query can't race between commit-visible and watermark-raise;
    /// a failed commit then leaves it conservatively high.
    delta_flushed_watermark: DashMap<crate::write::mem_buffer::TableKey, i64>,
    /// Recovery-time floor for the watermark: anything committed by earlier
    /// process lifetimes has row timestamps at/below roughly this.
    boot_micros: i64,
    /// WAL read-cursor holds for inserts whose entry is appended but whose
    /// MemBuffer bucket hasn't recorded its hold yet (the append→record
    /// window). Registered under the shard append lock BEFORE the entry
    /// exists — see `WalManager::append_batch` for the ordering argument.
    /// Keyed (project, table) → token → (shard, pre-append position).
    pending_wal_holds: DashMap<(String, String), HashMap<u64, (usize, walrus_rust::WalPosition)>>,
    /// Holds for buckets taken out of MemBuffer for an in-flight Delta commit:
    /// while airborne they're invisible to `MemBuffer::wal_holds`, but until the
    /// commit lands their WAL entries must still pin the cursor.
    /// Keyed (project, table) → token → per-shard holds.
    inflight_flush_holds: DashMap<(String, String), HashMap<u64, ShardHolds>>,
    /// Per-topic cursor holds + GC-floor pin (oldest WAL-append micros;
    /// i64::MAX = none) for buckets that could not be restored after a failed
    /// commit. Kept apart from `inflight_flush_holds` so `await_inflight_flushes`
    /// doesn't treat a process-lifetime orphan as an airborne commit and stall
    /// every DML for the full watchdog budget.
    orphaned_wal_holds: DashMap<(String, String), (ShardHolds, i64)>,
    /// WAL GC floor legs for taken buckets while their commit is airborne:
    /// token → `first_wal_pin_micros`. Keeps `gc_wal_files` from deleting
    /// files their entries live in.
    inflight_wal_pins: DashMap<u64, i64>,
    wal_hold_seq: AtomicU64,
    /// True while `recover_from_wal` runs. Suppresses cursor-snapshot writes
    /// from mid-replay relief flushes.
    recovery_active: AtomicBool,
    wal_recovery_duration_ms: AtomicU64,
    wal_replay_rows: AtomicU64,
    /// Set only after `recover_from_wal` returns successfully.
    wal_recovery_complete: AtomicBool,
    /// Per-topic pre-recovery cursor (P0), set once at the start of
    /// `recover_from_wal`. While `recovery_active`, `compute_wal_watermark`
    /// floors its result here so a mid-replay Delta commit's watermark metadata
    /// never claims coverage past P0 — a later boot's
    /// `derive_wal_cursors_from_delta` could otherwise forward the cursor past
    /// un-flushed replayed entries. Read-only during replay; empty outside it.
    recovery_commit_floor: DashMap<(String, String), ShardHolds>,
    /// Delta files committed by replay relief flushes. Indexing them before
    /// replay ends would publish a partial replayed state.
    deferred_tantivy_files: std::sync::Mutex<Vec<DeferredTantivyFile>>,
    deferred_tantivy_path: std::path::PathBuf,
    /// Batch-set identities this table's recent commits already contain, loaded
    /// at boot from the Delta history scan and extended as flushes land. A flush
    /// whose digest is here is provably already durable and is declined. Empty
    /// means "no proof of anything", which costs duplicates, never a loss.
    landed_digests: DashMap<(String, String), HashSet<LandedDigest>>,
    /// Per-(project, table) recently-flushed content-identity index for
    /// ingest-time client-retry dedup. Arc so a probe clones the handle out
    /// instead of holding a DashMap shard guard across its per-row loop.
    ingest_dedup: DashMap<(String, String), Arc<IngestDedupIndex>>,
    /// Test hook: drop the post-commit cursor advance, modelling a Delta commit
    /// that LANDS while the advance that should follow it is lost. Not
    /// `#[cfg(test)]` — the e2e suite links the real crate.
    test_drop_cursor_advance: AtomicBool,
    landed_skips_total: AtomicU64,
    landed_skipped_rows_total: AtomicU64,
    /// Test-only: bail out of `recover_from_wal` once this many relief drains
    /// have committed + advanced the rewind marker, simulating a crash
    /// mid-replay. `u64::MAX` = disabled.
    #[cfg(test)]
    test_crash_after_reliefs: AtomicU64,
}

/// Per-shard WAL cursor holds (`None` = no hold on that shard).
type ShardHolds = Vec<Option<walrus_rust::WalPosition>>;

struct WriteAdmission<'a> {
    layer: &'a BufferedWriteLayer,
}

impl Drop for WriteAdmission<'_> {
    fn drop(&mut self) {
        if self.layer.active_writes.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.layer.writes_drained.notify_waiters();
        }
    }
}

impl BufferedWriteLayer {
    fn admit_write(&self) -> Result<WriteAdmission<'_>, &'static str> {
        const DRAINING: &str = "TimeFusion is draining for deployment; retry on the replacement";
        if !self.accepting_writes.load(Ordering::Acquire) {
            return Err(DRAINING);
        }
        self.active_writes.fetch_add(1, Ordering::AcqRel);
        let admission = WriteAdmission { layer: self };
        // Close-vs-increment race: shutdown stores false before waiting on this
        // counter, so this recheck after the increment is required — every
        // admitted writer is then either visible to that wait or rejected here.
        if !self.accepting_writes.load(Ordering::Acquire) {
            return Err(DRAINING);
        }
        Ok(admission)
    }

    /// Close write admission before network-server drain. Idempotent. In-flight
    /// reads may finish; new INSERT/UPDATE/DELETE calls fail retryably instead
    /// of racing the final WAL cursor snapshot.
    pub fn stop_accepting_writes(&self) {
        self.handoff_generation.fetch_add(1, Ordering::AcqRel);
        self.deploy_handoff_ready.store(false, Ordering::Release);
        self.accepting_writes.store(false, Ordering::Release);
    }

    /// Whether a start-first replacement may safely trigger this process's
    /// graceful exit while reads are still being served.
    pub fn is_deploy_handoff_ready(&self) -> bool {
        self.deploy_handoff_ready.load(Ordering::Acquire)
            && !self.accepting_writes.load(Ordering::Acquire)
            && self.active_writes.load(Ordering::Acquire) == 0
            && self.is_drained()
    }

    async fn wait_for_active_writes_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.writes_drained.notified();
            if self.active_writes.load(Ordering::Acquire) == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return self.active_writes.load(Ordering::Acquire) == 0;
            }
        }
    }

    /// `function_registry` MUST be the same one the runtime SessionContext uses,
    /// so WAL replay can resolve UDFs in stored UPDATE/DELETE SQL.
    pub fn with_config(cfg: Arc<AppConfig>, function_registry: Arc<crate::read::functions::FnRegistry>) -> anyhow::Result<Self> {
        let wal = Arc::new(
            WalManager::with_fsync_mode_and_shards(cfg.core.wal_dir(), cfg.buffer.wal_fsync_mode(), cfg.buffer.wal_shards_per_topic())?
                .with_ack_fsync(cfg.buffer.wal_ack_fsync()),
        );
        // Must precede MemBuffer construction, which reads the bucket duration.
        crate::write::mem_buffer::set_bucket_duration_micros((cfg.buffer.bucket_duration_secs() as i64) * 1_000_000);
        crate::read::set_bounded_dedup_enabled(cfg.maintenance.timefusion_read_dedup_bounded);
        crate::read::optimizers::set_range_split_branches(cfg.maintenance.timefusion_query_range_split_branches);
        // Text-index cache budget: 25% of the MemBuffer budget, enforced by LRU.
        let text_index_max_bytes = (cfg.buffer.max_memory_mb() / 4).max(16) * 1024 * 1024;
        let mem_buffer = Arc::new(MemBuffer::new_with_max_index_bytes_and_shards(text_index_max_bytes, wal.shards_per_topic()));
        let deferred_path = deferred_tantivy_path(&cfg);

        Ok(Self {
            config: cfg,
            wal,
            mem_buffer,
            shutdown: CancellationToken::new(),
            accepting_writes: AtomicBool::new(true),
            active_writes: AtomicU64::new(0),
            writes_drained: Notify::new(),
            handoff_generation: AtomicU64::new(0),
            deploy_handoff_ready: AtomicBool::new(false),
            delta_write_callback: None,
            coalesced_write_callback: None,
            tantivy_index_callback: None,
            background_tasks: Mutex::new(Vec::new()),
            flush_lock: Mutex::new(()),
            relief_lock: Mutex::new(()),
            reserved_bytes: AtomicUsize::new(0),
            wal_hard_backpressure: AtomicBool::new(false),
            pressure_notify: Arc::new(Notify::new()),
            flush_tick_notify: Arc::new(Notify::new()),
            eviction_tick_notify: Arc::new(Notify::new()),
            flush_completed_total: AtomicU64::new(0),
            flush_failed_total: AtomicU64::new(0),
            last_flush_failure_micros: AtomicI64::new(0),
            backpressure_engaged_total: AtomicU64::new(0),
            backpressure_rejected_total: AtomicU64::new(0),
            backpressure_force_flush_total: AtomicU64::new(0),
            rows_ingested_total: AtomicU64::new(0),
            rows_flushed_total: AtomicU64::new(0),
            flush_freed_bytes_total: AtomicU64::new(0),
            function_registry,
            // Above realistic per-cycle table fan-out, still bounding
            // worst-case S3 / tantivy heap usage.
            tantivy_spawn_sem: Arc::new(tokio::sync::Semaphore::new(16)),
            delta_flushed_watermark: DashMap::new(),
            boot_micros: crate::support::now_micros(),
            pending_wal_holds: DashMap::new(),
            inflight_flush_holds: DashMap::new(),
            orphaned_wal_holds: DashMap::new(),
            inflight_wal_pins: DashMap::new(),
            wal_hold_seq: AtomicU64::new(0),
            recovery_active: AtomicBool::new(false),
            wal_recovery_duration_ms: AtomicU64::new(0),
            wal_replay_rows: AtomicU64::new(0),
            wal_recovery_complete: AtomicBool::new(false),
            recovery_commit_floor: DashMap::new(),
            landed_digests: DashMap::new(),
            ingest_dedup: DashMap::new(),
            test_drop_cursor_advance: AtomicBool::new(false),
            landed_skips_total: AtomicU64::new(0),
            landed_skipped_rows_total: AtomicU64::new(0),
            deferred_tantivy_files: std::sync::Mutex::new(std::fs::read(&deferred_path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()),
            deferred_tantivy_path: deferred_path,
            #[cfg(test)]
            test_crash_after_reliefs: AtomicU64::new(u64::MAX),
        })
    }

    pub fn with_delta_writer(mut self, callback: DeltaWriteCallback) -> Self {
        self.delta_write_callback = Some(callback);
        self
    }

    pub fn with_coalesced_delta_writer(mut self, callback: DeltaCoalescedWriteCallback) -> Self {
        self.coalesced_write_callback = Some(callback);
        self
    }

    pub fn with_tantivy_indexer(mut self, callback: TantivyIndexCallback) -> Self {
        self.tantivy_index_callback = Some(callback);
        self
    }

    pub fn deferred_tantivy_files(&self) -> Vec<DeferredTantivyFile> {
        self.deferred_tantivy_files.lock().unwrap().clone()
    }

    pub fn complete_deferred_tantivy_file(&self, file: &DeferredTantivyFile) {
        let mut files = self.deferred_tantivy_files.lock().unwrap();
        files.retain(|f| f != file);
        persist_deferred_tantivy_files(&self.deferred_tantivy_path, &files);
    }

    fn defer_tantivy_files(&self, project_id: &str, table_name: &str, uris: Vec<String>) {
        let mut files = self.deferred_tantivy_files.lock().unwrap();
        files.extend(uris.into_iter().filter(|uri| uri.ends_with(".parquet")).map(|uri| DeferredTantivyFile {
            project_id: project_id.into(),
            table_name: table_name.into(),
            uri,
        }));
        files.sort();
        files.dedup();
        persist_deferred_tantivy_files(&self.deferred_tantivy_path, &files);
    }

    /// MemBuffer budget after subtracting other long-lived allocations the
    /// process holds (Foyer in-memory caches, peak tantivy writer heap).
    /// Without this, `max_memory_mb` looks satisfied while RSS grows past it.
    fn max_memory_bytes(&self) -> usize {
        let configured = self.config.buffer.max_memory_mb() * 1024 * 1024;
        let foyer = if self.config.cache.is_disabled() { 0 } else { self.config.cache.memory_size_bytes() + self.config.cache.metadata_memory_size_bytes() };
        // Each in-flight flush may spawn one tantivy writer with WRITER_HEAP_BYTES.
        let tantivy_peak =
            if self.config.tantivy.indexed_tables().is_empty() { 0 } else { crate::tantivy::WRITER_HEAP_BYTES * self.config.buffer.flush_parallelism() };
        let reserved = foyer.saturating_add(tantivy_peak);
        // Floor so a misconfigured cache/tantivy combo can't zero the budget.
        const MIN_BUFFER_BYTES: usize = 64 * 1024 * 1024;
        configured.saturating_sub(reserved).max(MIN_BUFFER_BYTES)
    }

    /// MemBuffer fill ratio (0..=100). Used by ingress to emit soft
    /// backpressure before hitting the hard reservation limit.
    pub fn pressure_pct(&self) -> u32 {
        fill_pct(self.effective_memory_bytes(), self.max_memory_bytes())
    }

    /// Total effective memory including reserved bytes for in-flight writes.
    fn effective_memory_bytes(&self) -> usize {
        self.mem_buffer.estimated_memory_bytes() + self.reserved_bytes.load(Ordering::Acquire)
    }

    fn is_memory_pressure(&self) -> bool {
        self.effective_memory_bytes() >= self.max_memory_bytes()
    }

    /// The stall watchdog's budget for ONE commit, scaled by remaining ingest
    /// headroom: full budget when there is room, contracting as the buffer
    /// fills so the global `flush_lock` is released while a retry can still fit.
    ///
    /// The floor is half of `base`, never a small absolute number: aborting a
    /// slow-but-progressing drain discards the work for a retry that is no
    /// cheaper. `0` (watchdog disabled) stays disabled.
    fn adaptive_flush_timeout(&self) -> Duration {
        let base = self.config.buffer.flush_bucket_timeout();
        if base.is_zero() {
            return base;
        }
        let min_flush_timeout = base / 2;
        // Headroom below this fraction of the cap starts contracting the budget;
        // at the cap it is the floor.
        const RELAXED_BELOW: f64 = 0.5;
        let (used, max) = (self.effective_memory_bytes() as f64, self.max_memory_bytes().max(1) as f64);
        let pressure = (used / max).clamp(0.0, 1.0);
        if pressure <= RELAXED_BELOW {
            return base;
        }
        // Linear from `base` at RELAXED_BELOW down to the floor at the cap.
        let span = (base.saturating_sub(min_flush_timeout)).as_secs_f64();
        let scale = (1.0 - pressure) / (1.0 - RELAXED_BELOW);
        min_flush_timeout + Duration::from_secs_f64(span * scale)
    }

    /// Above the hard reservation ceiling, where live writers are rejected. WAL
    /// replay parks on this rather than `is_memory_pressure` so it keeps
    /// overlapping an in-flight relief drain instead of stopping at first
    /// pressure.
    fn is_hard_memory_pressure(&self) -> bool {
        self.effective_memory_bytes() >= hard_limit(self.max_memory_bytes())
    }

    /// Atomically reserve memory before a write. Returns the estimated batch
    /// size, or an error if the hard limit would be exceeded.
    async fn try_reserve_memory(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let estimated_size = estimate_reservation(batches);

        let max_bytes = self.max_memory_bytes();
        let hard_limit = hard_limit(max_bytes);
        let threshold = self.config.buffer.pressure_flush_pct();
        // Hoisted deliberately: only `reserved_bytes` is re-read per CAS attempt.
        let current_mem = self.mem_buffer.estimated_memory_bytes();

        for attempt in 0..MAX_CAS_RETRIES {
            let current_reserved = self.reserved_bytes.load(Ordering::Acquire);
            let new_total = current_mem + current_reserved + estimated_size;

            if new_total > hard_limit {
                anyhow::bail!(
                    "Memory limit exceeded: {}MB + {}MB reservation > {}MB hard limit",
                    (current_mem + current_reserved) / (1024 * 1024),
                    estimated_size / (1024 * 1024),
                    hard_limit / (1024 * 1024)
                );
            }

            if self.reserved_bytes.compare_exchange(current_reserved, current_reserved + estimated_size, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                // Crossing the threshold wakes the flush task early.
                if fill_pct(new_total, max_bytes) >= threshold {
                    self.pressure_notify.notify_one();
                }
                return Ok(estimated_size);
            }

            if attempt < CAS_SPIN_ATTEMPTS {
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

    /// Reserve memory unconditionally, even past the hard limit. Only for the
    /// `wal_admit_decouple` path once backpressure is exhausted: admitting
    /// over-budget is safe there because the WAL already holds the batch.
    fn force_reserve(&self, batches: &[RecordBatch]) -> usize {
        let estimated_size = estimate_reservation(batches);
        self.reserved_bytes.fetch_add(estimated_size, Ordering::AcqRel);
        self.pressure_notify.notify_one();
        estimated_size
    }

    /// Reserve memory, applying backpressure instead of dropping the write when
    /// the hard limit is hit: flush MemBuffer → Delta to make room, retrying
    /// after each drain, failing only after `write_backpressure_timeout` with no
    /// progress. This deliberately puts a synchronous flush on the insert path —
    /// a slow write beats a rejected one. Sub-limit inserts never block here.
    async fn reserve_with_backpressure(&self, batches: &[RecordBatch]) -> anyhow::Result<usize> {
        let first = self.try_reserve_memory(batches).await;
        let timeout = self.config.buffer.write_backpressure_timeout();
        if first.is_ok() || timeout.is_zero() {
            return first;
        }

        let deadline = std::time::Instant::now() + timeout;
        crate::observability::record_backpressure_engaged();
        self.backpressure_engaged_total.fetch_add(1, Ordering::Relaxed);
        warn!(
            "Write backpressure engaged: used={}MB ≥ hard limit; waking background flush to free RAM (not rejecting, not flushing on insert thread)",
            self.effective_memory_bytes() / (1024 * 1024)
        );
        loop {
            // Single-flight relief: only the `relief_lock` winner drives the
            // flush, so N blocked writers cost O(commit), not O(N × commit).
            if let Ok(_relief) = self.relief_lock.try_lock() {
                self.relieve_memory_pressure().await;
            } else {
                self.pressure_notify.notify_one();
            }

            match self.try_reserve_memory(batches).await {
                Ok(sz) => return Ok(sz),
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        crate::observability::record_backpressure_rejected();
                        self.backpressure_rejected_total.fetch_add(1, Ordering::Relaxed);
                        // Rejection happens BEFORE `wal.append_batch`, so the
                        // batch is NOT durable: recovery depends on the caller
                        // retrying or on the upstream DLQ.
                        error!(
                            "Write backpressure exhausted after {:?}: used={}MB still over hard limit — Delta flush is not freeing memory; rejecting batch (NOT yet durable — WAL append happens only after admission; caller must retry or rely on the upstream DLQ)",
                            timeout,
                            self.effective_memory_bytes() / (1024 * 1024)
                        );
                        return Err(e);
                    }
                    // The 25ms cap means a missed wakeup can't stall the retry.
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
    /// (WAL-ordering invariant), so completed buckets must drain first.
    async fn relieve_memory_pressure(&self) {
        if let Err(e) = self.flush_completed_buckets().await {
            warn!("pressure: flush_completed_buckets failed: {}", e);
        }
        if self.is_memory_pressure()
            && let Err(e) = self.force_flush_current_buckets().await
        {
            warn!("pressure: force_flush_current_buckets failed: {}", e);
        }
        self.flush_tick_notify.notify_waiters();
    }

    /// Force-flush the current (still-open) bucket(s) to Delta — the escalation
    /// tier for when one open window alone exceeds the memory budget. Rows are
    /// taken atomically under the insert lock and restored on commit failure;
    /// durability never depends on this (the WAL holds them).
    pub(crate) async fn force_flush_current_buckets(&self) -> anyhow::Result<()> {
        let _flush_guard = self.flush_lock.lock().await;
        let current = MemBuffer::current_bucket_id();
        let mut attempted = false;
        // No stuck-older-bucket gate needed: an unflushed older bucket pins the
        // cursor via its holds, so force-flushing the open window can never move
        // the cursor past it.
        for (project_id, table_name, bucket_id) in self.mem_buffer.bucket_keys(|id| id >= current) {
            let Some(bucket) = self.mem_buffer.take_bucket_for_flush(&project_id, &table_name, bucket_id) else {
                continue;
            };
            if !std::mem::replace(&mut attempted, true) {
                crate::observability::record_backpressure_force_flush();
                self.backpressure_force_flush_total.fetch_add(1, Ordering::Relaxed);
            }
            match self.flush_taken_bucket(&bucket).await {
                Ok(()) => {
                    self.rows_flushed_total.fetch_add(bucket.row_count as u64, Ordering::Relaxed);
                    self.flush_freed_bytes_total.fetch_add(flushable_bytes(&bucket), Ordering::Relaxed);
                    self.flush_completed_total.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    warn!("force-flush: Delta commit failed; rows restored to MemBuffer (WAL holds them): {}", e);
                    self.note_flush_failure(1);
                }
            }
        }
        Ok(())
    }

    /// Exempt the buckets a merge-on-read version append lands in from the
    /// Delta-scan exclusion. A version append carries the row's ORIGINAL
    /// timestamp, so an unmarked bucket would make MemBuffer authoritative for
    /// the whole window and hide Delta's other rows in it.
    ///
    /// Must be called BEFORE the insert: between a bucket being created and
    /// being marked, a concurrent scan would see it unmarked and exclude the
    /// window.
    pub fn mark_version_buckets(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) {
        let time_col = crate::dml::table_time_column(table_name);
        for bucket in batches.iter().flat_map(|b| crate::write::mem_buffer::batch_bucket_ids(b, time_col)) {
            self.mem_buffer.mark_force_flushed(project_id, table_name, bucket);
        }
    }

    pub async fn insert(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
        self.insert_bounded(project_id, table_name, batches, true).await
    }

    /// `bound: false` skips the event-time admission bound — for DML
    /// re-appends only: tombstones/updates keep the original row's timestamp,
    /// which may legitimately lie outside the bound.
    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub async fn insert_bounded(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, bound: bool) -> anyhow::Result<()> {
        let _admission = self.admit_write().map_err(anyhow::Error::msg)?;
        // Fail fast over the WAL hard cap; the producer's DLQ replays.
        if self.wal_hard_backpressure.load(Ordering::Relaxed) {
            crate::observability::record_ingest_error(project_id, table_name);
            anyhow::bail!(
                "WAL backlog exceeds hard limit ({}GB); insert rejected under backpressure — retry later",
                self.config.buffer.timefusion_wal_hard_limit_gb
            );
        }
        // The insert path must never flush synchronously — that stalls pgwire
        // threads on S3 commits under the global flush_lock. Only notify.
        if self.is_memory_pressure() {
            warn!(
                "Memory pressure (used={}MB / max={}MB) — notifying background flush; insert path will not block on Delta",
                self.effective_memory_bytes() / (1024 * 1024),
                self.config.buffer.max_memory_mb()
            );
            self.pressure_notify.notify_one();
        }

        // Compact before reservation AND WAL serialization: scan-backed DML
        // batches otherwise reserve at phantom size and serialize entire
        // inherited buffers into the WAL.
        let batches: Vec<RecordBatch> = batches.into_iter().map(crate::write::mem_buffer::compact_batch).collect();

        // Drop absurd event timestamps before anything is reserved or made
        // durable: `date` derives from `timestamp`, so a client unit error mints
        // garbage partitions. Bounding at flush would wedge already-acked data.
        let batches = if bound { bound_event_time(project_id, table_name, batches) } else { batches };
        if batches.is_empty() {
            return Ok(());
        }

        // Ingest-time client-retry dedup. Gated to live bounded ingest — DML
        // re-appends (bound=false) legitimately re-state row content, and
        // filtering WAL replay would silently revert acked DML.
        let batches = if bound && landed_identity_applies(table_name) { self.filter_ingest_dedup(project_id, table_name, batches) } else { batches };
        if batches.is_empty() {
            return Ok(());
        }
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        let reserved_size = match self.reserve_with_backpressure(&batches).await {
            Ok(sz) => sz,
            // Opt-in: admit over-budget rather than drop the write.
            Err(e) if self.config.buffer.wal_admit_decouple() => {
                warn!("wal_admit_decouple: admitting over-budget instead of rejecting (WAL is durable): {}", e);
                self.force_reserve(&batches)
            }
            Err(e) => return Err(e),
        };

        // WAL append + MemBuffer apply share one pin lifecycle (`with_wal_pin`):
        // the pending hold covers the append→apply window, then each destination
        // bucket is pinned at the pre-append position, which is ≤ every entry of
        // this append and so a valid hold for all of them.
        let result: anyhow::Result<()> = self.with_wal_pin(
            project_id,
            table_name,
            "append_batch",
            |on_pre| self.wal.append_batch(project_id, table_name, &batches, on_pre),
            |hold| {
                let now = crate::support::now_micros();
                batches.iter().try_for_each(|batch| {
                    self.mem_buffer.insert_with_hold(
                        project_id,
                        table_name,
                        batch.clone(),
                        batch_timestamp_range(batch).map(|(min, _)| min).unwrap_or(now),
                        hold,
                    )
                })
            },
        );

        self.release_reservation(reserved_size);

        result
            .inspect(|()| {
                self.rows_ingested_total.fetch_add(row_count as u64, Ordering::Relaxed);
                crate::observability::record_insert(project_id, table_name, row_count as u64);
            })
            .inspect_err(|_| crate::observability::record_ingest_error(project_id, table_name))?;

        if self.config.buffer.flush_immediately() {
            self.flush_all_now().await?;
        }

        debug!("BufferedWriteLayer insert complete: project={}, table={}", project_id, table_name);
        Ok(())
    }

    /// Record batch-set identities a table's commits are known to contain, so an
    /// identical re-flush can be declined.
    pub fn note_landed_digests(&self, project_id: &str, table_name: &str, digests: impl IntoIterator<Item = LandedDigest>) {
        let mut digests = digests.into_iter().peekable();
        if digests.peek().is_none() {
            return;
        }
        let mut known = self.landed_digests.entry((project_id.to_string(), table_name.to_string())).or_default();
        known.extend(digests);
        // Cleared rather than trimmed: a HashSet has no recency order, and
        // dropping identities only costs a duplicate.
        let cap = self.config.buffer.delta_scan_depth().saturating_mul(LANDED_WINDOW_COMMITS);
        if known.len() > cap {
            debug!("landed-identity window full for {}.{} ({} > {}) — clearing", project_id, table_name, known.len(), cap);
            known.clear();
        }
    }

    /// Whether this batch set is provably already in Delta. Returns `false`
    /// (flush it) for every uncertainty; `true` requires a full 256-bit match
    /// against an identity a commit recorded.
    fn already_landed(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) -> bool {
        if !self.config.buffer.landed_skip_enabled() || !landed_identity_applies(table_name) {
            return false;
        }
        // Cheap guard first: the digest costs an IPC round-trip. The shard guard
        // must be dropped before it, or concurrent flushes on this table stall.
        let key = (project_id.to_string(), table_name.to_string());
        if self.landed_digests.get(&key).is_none_or(|known| known.is_empty()) {
            return false;
        }
        let Some(digest) = landed_digest(batches) else { return false };
        self.landed_digests.get(&key).is_some_and(|known| known.contains(&digest))
    }

    /// Get-or-create the ingest-dedup index for one (project, table), cloned
    /// OUT of the map so no shard guard is held across per-row work.
    fn ingest_dedup_index(&self, project_id: &str, table_name: &str) -> Arc<IngestDedupIndex> {
        Arc::clone(
            &self
                .ingest_dedup
                .entry((project_id.to_string(), table_name.to_string()))
                .or_insert_with(|| Arc::new(IngestDedupIndex::new(INGEST_DEDUP_MAX_BYTES, INGEST_DEDUP_WINDOW_MICROS, crate::support::now_micros()))),
        )
    }

    /// Drop exact client-retry duplicates before they are reserved or made durable.
    fn filter_ingest_dedup(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
        let idx = self.ingest_dedup_index(project_id, table_name);
        let stats = crate::observability::maintenance_stats();
        let (mut key_hits, mut dropped) = (0u64, 0u64);
        let kept: Vec<RecordBatch> = batches
            .into_iter()
            .filter_map(|batch| {
                let (kept, h, d) = ingest_dedup_filter_batch(&idx, table_name, batch);
                key_hits += h;
                dropped += d;
                kept
            })
            .collect();
        stats.ingest_dedup_key_hits.fetch_add(key_hits, Ordering::Relaxed);
        stats.ingest_dedup_dropped_rows.fetch_add(dropped, Ordering::Relaxed);
        if dropped > 0 {
            debug!("ingest dedup dropped {dropped} exact client-retry rows: project={project_id}, table={table_name}");
        }
        if idx.maybe_rotate(crate::support::now_micros()) {
            stats.ingest_dedup_epoch_rotations.fetch_add(1, Ordering::Relaxed);
        }
        kept
    }

    /// Record flushed rows' content identities in the ingest-dedup index. Must
    /// run POST-COMMIT: a crash before the commit then costs a duplicate, never
    /// a loss.
    fn populate_ingest_dedup(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) {
        let idx = self.ingest_dedup_index(project_id, table_name);
        batches
            .iter()
            .filter_map(|batch| ingest_identity_idxs(table_name, &batch.schema()).map(|(k, c)| (batch, k, c)))
            .flat_map(|(batch, key_idxs, content_idxs)| per_row_identities(batch, &key_idxs, &content_idxs).unwrap_or_default())
            .for_each(|(k, c)| idx.populate(k, c));
        let stats = crate::observability::maintenance_stats();
        // Rotate here too: a table written only via DML re-appends never takes
        // the probe path, and the index must stay bounded either way.
        if idx.maybe_rotate(crate::support::now_micros()) {
            stats.ingest_dedup_epoch_rotations.fetch_add(1, Ordering::Relaxed);
        }
        stats.ingest_dedup_index_entries.store(self.ingest_dedup.iter().map(|e| e.value().entries() as u64).sum(), Ordering::Relaxed);
    }

    /// The layer's own `WalManager`. Startup must use this instance rather than
    /// opening a second `Walrus` handle.
    pub fn wal(&self) -> &Arc<WalManager> {
        &self.wal
    }

    #[instrument(skip(self))]
    pub async fn recover_from_wal(self: &Arc<Self>) -> anyhow::Result<RecoveryStats> {
        let start = std::time::Instant::now();
        let corruption_threshold = self.config.buffer.wal_corruption_threshold();

        info!("Starting WAL recovery, corruption_threshold={}", corruption_threshold);

        // Crash-safe replay: rewind to a leftover marker, then persist the
        // pre-recovery cursors P0 before consuming anything. The cursor is parked
        // at the surviving holds after the loop; only then is the marker removed.
        let rewind_applied = self.wal.apply_recovery_rewind_marker().map_err(|e| anyhow::anyhow!("recovery rewind marker apply failed: {}", e))?;

        // Zero-replay fast path: cursor == shard tail proves there is no unread
        // payload. The rewind marker must be applied first so an interrupted
        // prior recovery is never mistaken for a consumed log.
        if self.wal.is_fully_consumed().map_err(|e| anyhow::anyhow!("WAL tail/cursor check failed: {}", e))? {
            if rewind_applied {
                self.wal.remove_recovery_rewind_marker();
            }
            self.wal.request_reclaim_sweep();
            let recovery_duration_ms = start.elapsed().as_millis() as u64;
            self.wal_recovery_duration_ms.store(recovery_duration_ms, Ordering::Relaxed);
            self.wal_recovery_complete.store(true, Ordering::Relaxed);
            info!("WAL recovery complete: exact cursor/tail match, no replay required, duration={}ms", recovery_duration_ms);
            return Ok(RecoveryStats { recovery_duration_ms, ..Default::default() });
        }
        let p0 = self.wal.write_recovery_rewind_marker().map_err(|e| anyhow::anyhow!("recovery rewind marker write failed: {}", e))?;

        // Gate cursor-snapshot writes for the whole replay.
        self.recovery_active.store(true, Ordering::Relaxed);

        // Resumable replay, two independent mechanisms:
        //  1. `recovery_commit_floor` floors compute_wal_watermark at P0 for the
        //     whole replay, so a mid-replay commit never records coverage a later
        //     Delta-derive could use to skip un-flushed replayed entries.
        //  2. The rewind marker advances as buckets drain, so a mid-replay crash
        //     re-replays only the still-un-drained tail.
        for ((p, t), holds) in p0.iter() {
            self.recovery_commit_floor.insert((p.clone(), t.clone()), holds.clone());
        }

        let mut entries_replayed = 0u64;
        // Recovered rows bypass insert()'s rows_ingested_total bump; counted here
        // and folded in after replay.
        let mut recovered_rows = 0u64;
        let mut deletes_replayed = 0u64;
        let mut updates_replayed = 0u64;
        let mut oldest_ts: Option<i64> = None;
        let mut newest_ts: Option<i64> = None;
        let (mut insert_decode_nanos, mut insert_apply_nanos, mut insert_bytes) = (0u128, 0u128, 0u64);
        let (mut delete_nanos, mut update_nanos) = (0u128, 0u128);
        let mem_buffer = &self.mem_buffer;

        let quarantine_dir = self.wal.data_dir().join(crate::write::wal::QUARANTINE_DIR_NAME);
        // Entries whose quarantine copy failed to persist exist ONLY in the WAL —
        // recovery must not park past them or drop the marker.
        let quarantine_failures = AtomicU64::new(0u64);
        let registry_ref: Option<&crate::read::functions::FnRegistry> = Some(self.function_registry.as_ref());
        // Park an unapplicable entry: log, persist the payload, and count a park
        // that itself failed. `kind` is the quarantine filename tag.
        let park = |entry: &WalEntry, kind: &str, corrupt: bool, what: &str, e: &dyn std::fmt::Display| {
            error!("{}: {} for {}.{}: {}", if corrupt { "WAL CORRUPTION" } else { "WAL REPLAY FAILED" }, what, entry.project_id, entry.table_name, e);
            if !quarantine_entry(&quarantine_dir, entry, kind, &e.to_string()) {
                quarantine_failures.fetch_add(1, Ordering::Relaxed);
            }
        };
        fn decode_insert(entry: &WalEntry) -> (Result<RecordBatch, crate::write::wal::WalError>, u128) {
            let started = std::time::Instant::now();
            (deserialize_record_batch(&entry.data).map(crate::write::mem_buffer::compact_batch), started.elapsed().as_nanos())
        }
        // No age cutoff: the persisted cursor already bounds replay to un-flushed
        // entries, and an age filter would lose acked writes older than retention.
        let mut process_entry = |entry: WalEntry,
                                 shard: usize,
                                 pos: walrus_rust::WalPosition,
                                 predecoded_insert: Option<(Result<RecordBatch, crate::write::wal::WalError>, u128)>| {
            let entry_start = std::time::Instant::now();
            match entry.operation {
                WalOperation::Insert => {
                    insert_bytes += entry.data.len() as u64;
                    let (decoded, decode_nanos) = predecoded_insert.unwrap_or_else(|| decode_insert(&entry));
                    insert_decode_nanos += decode_nanos;
                    match decoded {
                        Ok(batch) => {
                            if batch.num_rows() == 0 {
                                warn!("Skipping empty batch during WAL recovery for {}.{}", entry.project_id, entry.table_name);
                                return;
                            }
                            // Seed the version clock: the first stamp issued after
                            // this boot must exceed every replayed one.
                            crate::write::observe_batch(&entry.table_name, &batch);
                            let apply_start = std::time::Instant::now();
                            let rows = batch.num_rows() as u64;
                            let insert_res = mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros);
                            insert_apply_nanos += apply_start.elapsed().as_nanos();
                            match insert_res {
                                Ok(()) => {
                                    entries_replayed += 1;
                                    recovered_rows += rows;
                                    // Pin at this entry's real WAL position so the marker
                                    // advances as buckets drain (resumable replay).
                                    mem_buffer.record_replay_hold(&entry.project_id, &entry.table_name, entry.timestamp_micros, shard, pos);
                                }
                                Err(e) => park(&entry, "insert_incompatible", false, "incompatible INSERT", &e),
                            }
                        }
                        Err(e) => park(&entry, "insert_corrupt", true, "undeserializable INSERT batch", &e),
                    }
                }
                WalOperation::Delete => match decode_payload::<DeletePayload>(&entry.data) {
                    Ok(payload) => {
                        match mem_buffer.delete_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref(), registry_ref, Some((shard, pos)))
                        {
                            Ok(_) => deletes_replayed += 1,
                            Err(e) => park(&entry, "delete_replay_failed", false, "DELETE", &e),
                        }
                    }
                    Err(e) => park(&entry, "delete_corrupt", true, "undeserializable DELETE payload", &e),
                },
                WalOperation::Update => match decode_payload::<UpdatePayload>(&entry.data) {
                    Ok(payload) => match mem_buffer.update_by_sql(
                        &entry.project_id,
                        &entry.table_name,
                        payload.predicate_sql.as_deref(),
                        &payload.assignments,
                        registry_ref,
                        Some((shard, pos)),
                    ) {
                        Ok(_) => updates_replayed += 1,
                        Err(e) => park(&entry, "update_replay_failed", false, "UPDATE", &e),
                    },
                    Err(e) => park(&entry, "update_corrupt", true, "undeserializable UPDATE payload", &e),
                },
                WalOperation::UpdateWithSource => match decode_payload::<UpdateWithSourcePayload>(&entry.data) {
                    Ok(payload) => match deserialize_record_batch(&payload.source.batch_ipc) {
                        Ok(source_batch) => match mem_buffer.update_with_source_by_sql(
                            &entry.project_id,
                            &entry.table_name,
                            payload.predicate_sql.as_deref(),
                            &payload.assignments,
                            crate::dml::UpdateSource { schema: source_batch.schema(), batch: source_batch, join_keys: payload.source.join_keys.clone() },
                            registry_ref,
                            Some((shard, pos)),
                        ) {
                            Ok(_) => updates_replayed += 1,
                            Err(e) => park(&entry, "update_with_source_replay_failed", false, "UPDATE_WITH_SOURCE", &e),
                        },
                        Err(e) => park(&entry, "update_with_source_batch_corrupt", true, "undeserializable UPDATE_WITH_SOURCE Arrow batch", &e),
                    },
                    Err(e) => park(&entry, "update_with_source_corrupt", true, "undeserializable UPDATE_WITH_SOURCE payload", &e),
                },
            }
            match entry.operation {
                WalOperation::Delete => delete_nanos += entry_start.elapsed().as_nanos(),
                WalOperation::Update | WalOperation::UpdateWithSource => update_nanos += entry_start.elapsed().as_nanos(),
                WalOperation::Insert => {}
            }
            let ts = entry.timestamp_micros;
            oldest_ts = Some(oldest_ts.map_or(ts, |o| o.min(ts)));
            newest_ts = Some(newest_ts.map_or(ts, |n| n.max(ts)));
        };

        // Budget-bounded replay: replay bypasses the insert path's memory
        // reservation, so a backlog larger than the buffer budget would land
        // wholesale in MemBuffer and OOM. Reliefs run CONCURRENTLY with replay
        // (spawned, single-flight); replay parks only at the HARD ceiling while a
        // drain is in flight, and the entry-count gate paces re-spawns when no
        // drain is running. Mid-replay flushes are safe because watermarks are
        // floored at P0.
        const RELIEF_BACKOFF_ENTRIES: u64 = 200;
        let mut relief_gate = 0u64;
        let mut replay_reliefs = 0u64;
        let mut drain_task: Option<JoinHandle<()>> = None;
        let mut iter = self.wal.replay_iter().map_err(|e| anyhow::anyhow!("WAL replay iterator init failed: {}", e))?;
        let mut processed_total = 0u64;
        let mut applied_frontiers: HashMap<(String, String), ShardHolds> = HashMap::new();
        const DECODE_CHUNK_ENTRIES: usize = 64;
        /// Replay must not monopolise a box also serving the early-bind PGWire responder.
        const DECODE_MAX_TASKS: usize = 4;
        /// Below this a chunk decodes inline — the join costs more than it saves.
        const DECODE_PARALLEL_MIN_ENTRIES: usize = 8;
        const DECODE_CHUNK_BYTES: usize = 32 * 1024 * 1024;
        loop {
            // Capture the frontier per item, so a relief commit's rewind marker
            // uses the last APPLIED item's frontier, not the prefetched head.
            let mut chunk = Vec::with_capacity(DECODE_CHUNK_ENTRIES);
            let mut chunk_bytes = 0usize;
            for _ in 0..DECODE_CHUNK_ENTRIES {
                let Some((entry, shard, pos)) = iter.next_entry() else { break };
                chunk_bytes = chunk_bytes.saturating_add(entry.data.len());
                let frontier = iter.frontier();
                chunk.push((entry, shard, pos, frontier));
                // Entries may be as large as WAL_SPLIT_TARGET, so bound retained
                // payload bytes as well as count.
                if chunk_bytes >= DECODE_CHUNK_BYTES {
                    break;
                }
            }
            if chunk.is_empty() {
                break;
            }

            // Decode the chunk across tasks; `DECODE_CHUNK_BYTES` bounds the
            // payload in flight. ORDER IS PRESERVED — tasks take disjoint
            // contiguous slices and are joined in order, which DML replay needs.
            // Non-capturing (so `Copy`, hence usable from every spawned task).
            let decode_slice = |slice: Vec<(WalEntry, _, _, _)>| {
                slice
                    .into_iter()
                    .map(|(entry, shard, pos, frontier)| {
                        let d = (entry.operation == WalOperation::Insert).then(|| decode_insert(&entry));
                        (entry, shard, pos, frontier, d)
                    })
                    .collect::<Vec<_>>()
            };
            let threads = std::thread::available_parallelism().map_or(1, |n| n.get()).min(DECODE_MAX_TASKS);
            let chunk_len = chunk.len();
            let ready = if threads > 1 && chunk_len >= DECODE_PARALLEL_MIN_ENTRIES {
                // Scoped so the (non-`Sync`) chunker is dropped before the await.
                let handles: Vec<_> = {
                    let slices = chunk.into_iter().chunks(chunk_len.div_ceil(threads));
                    slices
                        .into_iter()
                        .map(|s| {
                            let slice = s.collect_vec();
                            tokio::task::spawn_blocking(move || decode_slice(slice))
                        })
                        .collect()
                };
                futures::future::join_all(handles).await.into_iter().flat_map(|r| r.expect("WAL decode task panicked")).collect()
            } else {
                decode_slice(chunk)
            };

            for (entry, shard, pos, (safe_topic, safe_frontier), decoded) in ready {
                process_entry(entry, shard, pos, decoded);
                processed_total += 1;
                if let Some((project_id, table_name)) = safe_topic {
                    // `None` means the shard is exhausted; normalize to the startup
                    // write tail (immutable — write admission is not open yet).
                    let normalized = (0..self.wal.shards_per_topic())
                        .map(|s| safe_frontier.get(s).copied().flatten().or_else(|| self.wal.current_position_for_shard(&project_id, &table_name, s).ok()))
                        .collect();
                    applied_frontiers.insert((project_id, table_name), normalized);
                }
                // Back off (entry-count gate) only when a completed drain LEFT
                // pressure standing — that's the flushes-failing case. A drain
                // that relieved pressure re-arms immediately.
                if let Some(h) = drain_task.as_ref()
                    && h.is_finished()
                {
                    drain_task = None;
                    relief_gate = if self.is_memory_pressure() { processed_total + RELIEF_BACKOFF_ENTRIES } else { 0 };
                    // Advance the marker only to this applied entry's frontier.
                    if quarantine_failures.load(Ordering::Relaxed) == 0 {
                        self.refresh_replay_rewind_marker(&p0, &applied_frontiers);
                    }
                    #[cfg(test)]
                    if replay_reliefs >= self.test_crash_after_reliefs.load(Ordering::Relaxed) {
                        anyhow::bail!("test: simulated crash mid-replay after {} relief(s)", replay_reliefs);
                    }
                }
                if drain_task.is_none() && processed_total >= relief_gate && self.is_memory_pressure() {
                    replay_reliefs += 1;
                    info!(
                        "WAL replay: memory pressure at entry {} ({}MB buffered) — draining oldest buckets in background (relief #{})",
                        processed_total,
                        self.effective_memory_bytes() / (1024 * 1024),
                        replay_reliefs
                    );
                    let this = Arc::clone(self);
                    drain_task = Some(tokio::spawn(async move { this.drain_replay_backlog().await }));
                }
                while self.is_hard_memory_pressure() && drain_task.as_ref().is_some_and(|h| !h.is_finished()) {
                    let _ = tokio::time::timeout(Duration::from_millis(100), self.flush_tick_notify.notified()).await;
                }
            }
        }
        let (iter_read_nanos, iter_envelope_nanos) = (iter.read_nanos, iter.envelope_nanos);
        // The drain mutates the hold/orphan state the cursor parking below reads,
        // and its airborne commit must land before positions are parked — await
        // it, don't abort it.
        if let Some(h) = drain_task.take() {
            let _ = h.await;
        }
        let error_count = iter.errors;
        if replay_reliefs > 0 {
            info!("WAL replay ran {} concurrent relief drain(s) during recovery", replay_reliefs);
        }

        // Corruption threshold (0 = disabled) must NOT abort the boot while every
        // corrupt payload is preserved on disk: bailing with the rewind marker
        // intact re-reads the same corrupt prefix forever.
        //
        // A failed quarantine WRITE is disjoint from that and DOES bail: the WAL
        // is then the entry's only copy, so nothing may advance past it.
        if quarantine_failures.load(Ordering::Relaxed) > 0 {
            anyhow::bail!(
                "{} quarantine write(s) failed during WAL replay — the WAL is those entries' only copy; keeping the rewind marker. Free disk / fix permissions on {:?} and restart",
                quarantine_failures.load(Ordering::Relaxed),
                quarantine_dir
            );
        }
        if corruption_threshold > 0 && error_count >= corruption_threshold {
            error!(
                "WAL corruption threshold exceeded: {} errors >= {} threshold — corrupt entries quarantined under {:?}; continuing boot",
                error_count, corruption_threshold, quarantine_dir
            );
        }

        // Park the cursor at the earliest hold still owned by an unflushed
        // replayed bucket; hold-free topics keep the consumed tail so WAL GC can
        // reclaim them. Only then is the rewind marker safe to drop. Orphaned
        // holds MUST be merged in, or acked WAL-only rows whose relief flush
        // failed are stranded forever.
        let shards = self.wal.shards_per_topic();
        for (project_id, table_name) in p0.keys() {
            let holds = self.recovery_parking_holds(project_id, table_name, shards);
            // Replay advances walrus's read heads only in memory; persist once
            // per shard here rather than fsyncing the cursor index per entry.
            let tails: ShardHolds = (0..shards).map(|shard| self.wal.current_position_for_shard(project_id, table_name, shard).ok()).collect();
            let parked = merge_wal_holds(&tails, &holds);
            if let Err(e) = self.wal.set_positions_allow_rewind(project_id, table_name, &parked) {
                // Keep the marker: removing it after a partial cursor write could
                // skip an unflushed replayed bucket on a crash.
                anyhow::bail!("failed to park WAL cursor for {}.{} after replay: {}", project_id, table_name, e);
            }
        }
        self.wal.remove_recovery_rewind_marker();

        // Snapshot writes are safe again; if relief flushed mid-replay, rewrite it
        // with the PARKED positions so it never carries the consumed-ahead cursor.
        self.recovery_active.store(false, Ordering::Relaxed);
        self.recovery_commit_floor.clear();
        if replay_reliefs > 0 {
            self.write_post_flush_snapshot().await;
        }

        self.wal.request_reclaim_sweep();

        // Replay can leave the process over the memory budget. Deliberately do
        // NOT drain here — that would block the PGWire listener for the entire
        // flush; `drain_to_budget` runs in the background while we serve.

        self.rows_ingested_total.fetch_add(recovered_rows, Ordering::Relaxed);
        self.wal_replay_rows.store(recovered_rows, Ordering::Relaxed);

        let stats = RecoveryStats {
            entries_replayed,
            batches_recovered: entries_replayed,
            oldest_entry_timestamp: oldest_ts,
            newest_entry_timestamp: newest_ts,
            recovery_duration_ms: start.elapsed().as_millis() as u64,
            corrupted_entries_skipped: error_count as u64,
            tantivy_files_deferred: self.deferred_tantivy_files.lock().unwrap().len() as u64,
        };

        self.wal_recovery_duration_ms.store(stats.recovery_duration_ms, Ordering::Relaxed);
        self.wal_recovery_complete.store(true, Ordering::Relaxed);
        info!(
            "WAL recovery complete: inserts={}, deletes={}, updates={}, corrupted={}, duration={}ms",
            entries_replayed, deletes_replayed, updates_replayed, error_count, stats.recovery_duration_ms
        );
        let avg_ms = |nanos: u128, n: u64| if n > 0 { nanos as f64 / n as f64 / 1_000_000.0 } else { 0.0 };
        info!(
            // insert_decode is summed across decode tasks, so it can exceed wall clock.
            "WAL recovery cost breakdown: insert_decode={}ms cpu ({:.3}ms/ea), insert_apply={}ms ({:.3}ms/ea), \
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
            insert_bytes.checked_div(entries_replayed).unwrap_or(0),
        );
        info!(
            "WAL recovery read path: walrus_read={}ms ({:.3}ms/ea), envelope_decode={}ms ({:.3}ms/ea)",
            iter_read_nanos / 1_000_000,
            avg_ms(iter_read_nanos, entries_replayed),
            iter_envelope_nanos / 1_000_000,
            avg_ms(iter_envelope_nanos, entries_replayed),
        );

        // Quarantine must never be a quiet outcome: it is a loss-class alarm.
        let (q_files, q_bytes) = crate::write::wal::quarantine_stats(self.wal.data_dir());
        if q_files > 0 {
            crate::observability::record_quarantine_backlog();
            error!(
                "ALERT: {} quarantined WAL payload(s) ({:.1} MB) remain under {:?} — acked data is NOT in the store; investigate and re-drive (see RUNBOOK 'WAL quarantine')",
                q_files,
                q_bytes as f64 / (1024.0 * 1024.0),
                self.wal.data_dir().join(crate::write::wal::QUARANTINE_DIR_NAME)
            );
        }
        Ok(stats)
    }

    /// Background quarantine re-drive: each `quarantine/*.bin` payload whose
    /// `.meta` says `operation=Insert` goes back through the durable insert path,
    /// and moves to `quarantine/redriven/` only once acked-durable. Everything
    /// else stays parked for a human.
    async fn redrive_quarantine(self: &Arc<Self>) {
        let qdir = self.wal.data_dir().join(crate::write::wal::QUARANTINE_DIR_NAME);
        let Ok(rd) = std::fs::read_dir(&qdir) else { return };
        let redriven_dir = qdir.join(crate::write::wal::QUARANTINE_REDRIVEN_DIR_NAME);
        let (mut ok, mut failed) = (0u64, 0u64);
        for entry in rd.flatten() {
            if self.shutdown.is_cancelled() {
                break;
            }
            let path = entry.path();
            if path.extension().is_none_or(|e| e != "bin") {
                continue;
            }
            let meta_path = path.with_extension("meta");
            let meta = std::fs::read_to_string(&meta_path).unwrap_or_default();
            let field = |k: &str| meta.lines().find_map(|l| l.strip_prefix(k).and_then(|l| l.strip_prefix('=')).map(str::to_owned));
            let (Some(project_id), Some(table_name)) = (field("project_id"), field("table_name")) else {
                warn!("quarantine re-drive: {:?} has no parseable .meta sidecar; leaving parked", path);
                failed += 1;
                continue;
            };
            if field("operation").and_then(|o| o.parse().ok()) != Some(WalOperation::Insert) {
                failed += 1;
                continue;
            }
            let res: anyhow::Result<()> = async {
                let data = std::fs::read(&path)?;
                let batch = deserialize_record_batch(&data)?;
                anyhow::ensure!(batch.num_rows() > 0, "empty batch");
                self.insert(&project_id, &table_name, vec![batch]).await
            }
            .await;
            match res {
                Ok(()) => {
                    ok += 1;
                    crate::observability::record_quarantine_redriven();
                    if let Err(e) = std::fs::create_dir_all(&redriven_dir)
                        .and_then(|()| std::fs::rename(&path, redriven_dir.join(entry.file_name())))
                        .and_then(|()| std::fs::rename(&meta_path, redriven_dir.join(meta_path.file_name().unwrap_or_default())))
                    {
                        // Data is already durable; a leftover copy only inflates the alert count.
                        warn!("quarantine re-drive: re-ingested {:?} but failed to archive it: {}", path, e);
                    }
                }
                Err(e) => {
                    failed += 1;
                    warn!("quarantine re-drive: {:?} failed, leaving parked: {}", path, e);
                }
            }
            // A quarantine can be several GiB; pace it so live ingest isn't starved.
            if self.sleep_or_shutdown(Duration::from_millis(10)).await {
                break;
            }
        }
        if ok + failed > 0 {
            info!("quarantine re-drive: {} re-ingested (moved to redriven/), {} left parked", ok, failed);
        }
    }

    /// Mid-replay pressure relief: flush the OLDEST completed buckets (oldest
    /// quartile per pass) until usage is back under budget. Oldest-first keeps
    /// recently replayed rows in MemBuffer so trailing WAL UPDATE/DELETE entries
    /// still find their targets. Deliberately not cancel-aware — `recover_from_wal`
    /// awaits this task for hold/parking correctness.
    async fn drain_replay_backlog(&self) {
        let mut prev = usize::MAX;
        while self.is_memory_pressure() {
            let current = MemBuffer::current_bucket_id();
            // The oldest quartile's upper id is the cutoff.
            let ids: BTreeSet<i64> = self.mem_buffer.bucket_keys(|id| id < current).into_iter().map(|(_, _, id)| id).collect();
            let Some(&cutoff) = ids.iter().nth(ids.len().saturating_sub(1) / 4) else { break };
            if let Err(e) = self.flush_buckets_where(|id| id <= cutoff).await {
                warn!("replay relief: flush failed: {}", e);
                break;
            }
            self.flush_tick_notify.notify_waiters();
            let now = self.effective_memory_bytes();
            if now + now / 100 >= prev {
                break; // <1% progress: flushes failing or nothing drainable.
            }
            prev = now;
        }
        if self.is_memory_pressure()
            && let Err(e) = self.force_flush_current_buckets().await
        {
            warn!("replay relief: force-flush of open window failed: {}", e);
        }
        self.flush_tick_notify.notify_waiters();
    }

    /// Flush completed buckets to Delta until memory is back under budget.
    /// Bounded (64 iterations) and progress-gated so a failing Delta callback
    /// can't spin forever.
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

    /// Sleep `d` unless shutdown fires first; `true` means shutdown won.
    async fn sleep_or_shutdown(&self, d: Duration) -> bool {
        tokio::select! {
            () = self.shutdown.cancelled() => true,
            () = tokio::time::sleep(d) => false,
        }
    }

    pub async fn start_background_tasks(self: &Arc<Self>) {
        // Each method is a distinct opaque future type, so a macro (not a fn
        // taking a method pointer) is what keeps this one line per task.
        macro_rules! spawn_task {
            ($m:ident) => {{
                let this = Arc::clone(self);
                tokio::spawn(async move { this.$m().await })
            }};
        }
        self.background_tasks.lock().await.extend([
            spawn_task!(run_flush_task),
            spawn_task!(run_eviction_task),
            // Without WAL GC, walrus' per-process FileStateTracker leaks files
            // across restarts.
            spawn_task!(run_wal_gc_task),
            spawn_task!(run_wal_gate_task),
            spawn_task!(drain_to_budget),
            spawn_task!(run_quarantine_redrive_task),
        ]);

        info!("BufferedWriteLayer background tasks started");
    }

    async fn run_quarantine_redrive_task(self: Arc<Self>) {
        // Let PGWire bind and the post-replay drain start first.
        if self.sleep_or_shutdown(Duration::from_secs(5)).await {
            return;
        }
        self.redrive_quarantine().await;
    }

    async fn run_wal_gc_task(&self) {
        // Sweep immediately, then periodically: a process restarting faster than
        // the sweep interval would otherwise never reclaim anything.
        const SWEEP_INTERVAL: Duration = Duration::from_secs(600);
        let max_age = self.config.buffer.wal_gc_max_age();
        let wal_dir = self.wal.data_dir().clone();
        loop {
            let dir = wal_dir.clone();
            // Durability floor: never delete a file that un-flushed data
            // (buffered, airborne, or orphaned) may still replay from.
            let floor = self.oldest_unflushed_wal_append_micros();
            // Filesystem walk is sync — keep it off the runtime.
            let res = tokio::task::spawn_blocking(move || crate::write::wal::gc_wal_files(&dir, max_age, floor)).await;
            match res {
                Ok(Ok((deleted, bytes_freed))) if deleted > 0 => {
                    info!("WAL GC: deleted {} stale files, freed {} bytes", deleted, bytes_freed);
                    if let Some(m) = crate::observability::registry() {
                        m.wal_gc_deleted_files.add(deleted, &[]);
                    }
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => warn!("WAL GC error: {}", e),
                Err(e) => warn!("WAL GC task panicked: {}", e),
            }
            if self.sleep_or_shutdown(SWEEP_INTERVAL).await {
                info!("WAL GC task shutting down");
                break;
            }
        }
    }

    async fn run_flush_task(&self) {
        let flush_interval = Duration::from_secs(self.config.buffer.flush_interval_secs());

        loop {
            let by_pressure = tokio::select! {
                _ = tokio::time::sleep(flush_interval) => false,
                _ = self.pressure_notify.notified() => true,
                _ = self.shutdown.cancelled() => {
                    info!("Flush task shutting down");
                    break;
                }
            };

            if by_pressure {
                debug!("Pressure-triggered flush at {}% (threshold {}%)", self.pressure_pct(), self.config.buffer.pressure_flush_pct());
            } else {
                // Timer ticks only, never pressure wakeups: O(tables × buckets).
                self.mem_buffer.reconcile_estimated_bytes();
            }

            if let Err(e) = self.flush_completed_buckets().await {
                crate::observability::record_flush(false);
                self.note_flush_failure(1);
                error!("Flush task error: {}", e);
            }

            // Pressure escalation must gate on COMMIT PROGRESS, not a byte delta:
            // under old-event-time backfill each flushed bucket is tiny, so a
            // bytes-freed gate bails while hundreds of buckets still drain.
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
            let (file_count, total_bytes) = self.wal.wal_stats();
            if !by_pressure {
                info!("WAL stats: {} files, {}MB", file_count, total_bytes / (1024 * 1024));
            }
            // Emergency drain: flush_all_now advances the read cursor so WAL GC
            // can reclaim the backlog, keeping restart replay bounded.
            if self.is_wal_over_threshold() {
                warn!(
                    "WAL over threshold (files {}/{}, unflushed {}MB/{}MB, disk {}MB), triggering emergency flush",
                    file_count,
                    self.config.effective_wal_max_files(),
                    self.unflushed_backlog_bytes() / (1024 * 1024),
                    self.config.effective_wal_max_unflushed_bytes() / (1024 * 1024),
                    total_bytes / (1024 * 1024)
                );
                if let Err(e) = self.flush_all_now().await {
                    error!("Emergency WAL flush failed: {}", e);
                }
            }
            self.flush_tick_notify.notify_waiters();
        }
    }

    /// Timestamp a bucket must be older than to count as past retention.
    fn retention_cutoff_micros(&self) -> i64 {
        crate::support::now_micros() - (self.config.buffer.retention_mins() as i64) * 60 * 1_000_000
    }

    fn note_flush_failure(&self, n: u64) {
        self.flush_failed_total.fetch_add(n, Ordering::Relaxed);
        self.last_flush_failure_micros.store(crate::support::now_micros(), Ordering::Relaxed);
    }

    /// Bytes accepted (durable in the WAL) but NOT yet in Delta — the flush
    /// backlog. Deliberately NOT on-disk WAL size: that gauge is a workload
    /// property, so any brake reading it would sit permanently engaged.
    pub fn unflushed_backlog_bytes(&self) -> u64 {
        self.effective_memory_bytes() as u64
    }

    /// The compaction brake predicate: is durability genuinely behind?
    pub fn is_wal_backlog_over_threshold(&self) -> bool {
        wal_backlog_over_threshold(
            self.unflushed_backlog_bytes(),
            self.config.effective_wal_max_unflushed_bytes(),
            self.last_flush_failure_micros.load(Ordering::Relaxed),
            crate::support::now_micros(),
        )
    }

    /// Buckets past the hot-buffer retention that still have not landed in Delta.
    /// A stronger persistence-debt signal than bytes alone — a small old bucket
    /// can sit below the byte threshold indefinitely.
    pub fn stale_unflushed_bucket_count(&self) -> usize {
        self.mem_buffer.count_buckets_dwelling_since(self.retention_cutoff_micros())
    }

    /// The emergency-flush predicate: file sprawl OR a real unflushed backlog.
    /// The bytes leg must compare UNFLUSHED bytes, not on-disk size — flushing
    /// cannot shrink on-disk residue, so a disk signal engages permanently.
    pub fn is_wal_over_threshold(&self) -> bool {
        wal_emergency_flush_needed(
            self.wal.wal_stats().0,
            self.config.effective_wal_max_files(),
            self.unflushed_backlog_bytes(),
            self.config.effective_wal_max_unflushed_bytes(),
        )
    }

    /// Hard WAL cap — a disk-runaway breaker. Must be its own task, not part of
    /// the flush loop: that loop awaits flushes inline, so a stalled S3 flush
    /// would delay engagement unboundedly. The gauge is TOTAL on-disk WAL bytes
    /// (including flushed-but-not-yet-GCd segments), so the limit must sit far
    /// above normal residue.
    async fn run_wal_gate_task(&self) {
        let Some(hard) = self.config.buffer.wal_hard_limit_bytes() else { return };
        loop {
            if self.sleep_or_shutdown(Duration::from_secs(15)).await {
                return;
            }
            let (_, backlog) = self.wal.wal_stats();
            let over = backlog > hard;
            if over != self.wal_hard_backpressure.swap(over, Ordering::Relaxed) {
                if over {
                    error!(
                        "WAL backlog {}MB exceeds HARD limit {}MB — rejecting INSERTs until flush catches up (writes fail into the upstream DLQ)",
                        backlog / (1024 * 1024),
                        hard / (1024 * 1024)
                    );
                } else {
                    info!("WAL backlog back under hard limit — accepting INSERTs again");
                }
            }
        }
    }

    async fn run_eviction_task(&self) {
        let eviction_interval = Duration::from_secs(self.config.buffer.eviction_interval_secs());

        loop {
            if self.sleep_or_shutdown(eviction_interval).await {
                info!("Eviction task shutting down");
                break;
            }
            // Never evict unconditionally: that can drop a bucket before it
            // reaches Delta. Drive an extra flush attempt instead.
            if let Err(e) = self.flush_completed_buckets().await {
                error!("Eviction-task flush failed: {}", e);
            }
            self.evict_drained_metadata();
            // Release DML-emptied shells after a grace period — the only way their
            // cursor holds ever release (they can't flush).
            self.mem_buffer.reap_expired_empty_buckets(self.retention_cutoff_micros());
            self.eviction_tick_notify.notify_waiters();
        }
    }

    #[instrument(skip(self))]
    /// Flush sealed buckets oldest-first, in BOUNDED chunks. Chunking is
    /// load-bearing: one commit for all sealed buckets scales the unit of work
    /// with buffer occupancy against a fixed watchdog, and an aborted commit
    /// frees nothing, so the next cycle retries the same too-big commit.
    async fn flush_completed_buckets(&self) -> anyhow::Result<()> {
        let current_bucket = MemBuffer::current_bucket_id();
        // Snapshot the bucket list ONCE: re-deriving the remaining set each pass
        // would immediately re-flush a bucket dirty-kept because a DML mutated it
        // mid-commit, discarding the post-delete state (silent data loss).
        //
        // DWELL GATE: a sealed-but-young bucket waits one bucket_duration from its
        // CREATION unless it is already big, or MOR version-appends would mint a
        // tiny parquet file per minute. Pressure relief, pgwire FLUSH and shutdown
        // bypass the gate; rows stay WAL-durable and readable while they dwell.
        const FLUSH_DWELL_BYPASS_BYTES: usize = 32 << 20;
        let dwell_micros = self.config.buffer.flush_dwell_micros();
        let now = crate::support::now_micros();
        let meta = self.mem_buffer.bucket_flush_meta(|id| id < current_bucket);
        let fresh_small: BTreeSet<i64> = meta
            .iter()
            .filter(|(_, created, bytes)| now.saturating_sub(*created) < dwell_micros && *bytes < FLUSH_DWELL_BYPASS_BYTES)
            .map(|(id, _, _)| *id)
            .collect();
        // An id is held back while ANY table's bucket at it is fresh and small
        // (flushing below cannot split one id per table). This cannot starve: a
        // held bucket is not flushed, so no NEW bucket replaces it at that id.
        let ids: Vec<i64> = meta.iter().map(|(id, _, _)| *id).filter(|id| !fresh_small.contains(id)).collect::<BTreeSet<_>>().into_iter().collect();
        for chunk in ids.chunks(FLUSH_CHUNK_BUCKET_IDS) {
            // Membership, not a range: the ids between chunk members may be
            // dwell-held and a range predicate would flush them anyway.
            let members: BTreeSet<i64> = chunk.iter().copied().collect();
            self.flush_buckets_where(move |id| members.contains(&id)).await?;
        }
        Ok(())
    }

    /// Snapshot-flush every bucket whose id matches `pred`, coalesced into one
    /// Delta commit per (project, table) and flushed `flush_parallelism`-wide,
    /// largest table first (so a deadline-bounded caller cuts off the cheap tail).
    /// The one flush pipeline: periodic task, pressure relief, pgwire `FLUSH` and
    /// shutdown all route through it.
    async fn flush_buckets_where(&self, pred: impl Fn(i64) -> bool) -> anyhow::Result<FlushStats> {
        let _flush_guard = self.flush_lock.lock().await;

        // Group per (project, table) FIRST: the in-flight registration below must
        // precede any snapshot of that topic's buckets.
        let by_topic: HashMap<(String, String), Vec<i64>> = self.mem_buffer.bucket_keys(&pred).into_iter().map(|(p, t, id)| ((p, t), id)).into_group_map();

        // Snapshot (not take): rows stay queryable in MemBuffer while the Delta
        // commit is airborne.
        //
        // Registration order is load-bearing: the airborne marker is registered
        // BEFORE the topic's first snapshot resets any bucket hold, and upgraded
        // with the real holds synchronously (no await), so `compute_wal_watermark`
        // never observes a hold-less window and `await_inflight_flushes` sees the
        // commit as airborne from the instant its pre-DML snapshot exists.
        // Deferring registration lets a DELETE's Delta leg run before a stale
        // commit lands, resurrecting the deleted rows.
        //
        // Coalesce per (project_id, table_name): each commit pays a fixed cost
        // (log scan + JSON write + S3 RTT + tantivy build).
        let current_bucket = MemBuffer::current_bucket_id();
        let mut groups: Vec<(CombinedBucket, u64)> = by_topic
            .into_iter()
            .filter_map(|((p, t), ids)| {
                let token = self.register_inflight_holds(&p, &t, Vec::new()); // airborne marker
                let group = ids.into_iter().fold(CoalescedGroup::default(), |mut group, id| {
                    if let Some(b) = self.mem_buffer.snapshot_bucket_for_flush(&p, &t, id) {
                        // Not-yet-sealed bucket: exempt it from the Delta-scan range
                        // exclusion up front, or once the window seals the exclusion
                        // hides post-commit late arrivals from reads.
                        if id >= current_bucket {
                            self.mem_buffer.mark_force_flushed(&p, &t, id);
                        }
                        group.absorb(b);
                    }
                    group
                });
                if group.source_buckets.is_empty() {
                    self.release_inflight_holds(&p, &t, token);
                    return None;
                }
                let combined = group.into_combined_bucket(p.clone(), t.clone());
                if let Some(mut m) = self.inflight_flush_holds.get_mut(&(p, t))
                    && let Some(holds) = m.get_mut(&token)
                {
                    *holds = combined.combined.wal_first_positions.clone();
                }
                Some((combined, token))
            })
            .collect();

        if groups.is_empty() {
            debug!("No buckets to flush");
            return Ok(FlushStats::default());
        }
        groups.sort_by_key(|(c, _)| std::cmp::Reverse(c.combined.row_count));

        debug!("Flushing {} bucket(s) → {} per-table commit(s)", groups.iter().map(|(c, _)| c.source_buckets.len()).sum::<usize>(), groups.len());

        let parallelism = self.config.buffer.flush_parallelism();
        // Post-commit effects must run INSIDE each group's future, not after a
        // collect() barrier: the shutdown flush DROPS this call on deadline, so
        // behind a barrier an already-landed commit would lose its
        // drain/hold-release/cursor advance and re-replay as duplicates.
        let group_stats: Vec<(bool, FlushStats)> = match self.coalesced_write_callback.clone().filter(|_| self.config.buffer.flush_coalesce_commits()) {
            Some(callback) => self.flush_groups_coalesced(groups, callback).await,
            None => {
                stream::iter(groups)
                    .map(|(combined, token)| async move {
                        let result = self.flush_bucket(&combined.combined).await;
                        self.settle_flushed_group(combined, token, result)
                    })
                    .buffer_unordered(parallelism)
                    .collect()
                    .await
            }
        };

        let (any_ok, stats) = group_stats.into_iter().fold((false, FlushStats::default()), |(any, mut acc), (ok, s)| {
            acc.buckets_flushed += s.buckets_flushed;
            acc.buckets_failed += s.buckets_failed;
            acc.total_rows += s.total_rows;
            (any | ok, acc)
        });
        if any_ok {
            self.write_post_flush_snapshot().await;
        }

        Ok(stats)
    }

    /// Cross-project flush commit coalescing: prepare every group, hand them all
    /// to the coalescing writer, then settle each with its own result.
    ///
    /// Required semantics: a group that fails to PREPARE never reaches the writer
    /// and settles as its own failure; the writer returns one result per unit, so
    /// one project's failed parquet write does not block its co-tenants; a failed
    /// shared commit fails EVERY project it covered (no partial settle).
    async fn flush_groups_coalesced(&self, groups: Vec<(CombinedBucket, u64)>, callback: DeltaCoalescedWriteCallback) -> Vec<(bool, FlushStats)> {
        // Pairs stay in input order — the writer contract is positional.
        type Pending = (CombinedBucket, u64, Vec<RecordBatch>, DeltaWatermark);
        let (mut settled, pending): (Vec<(bool, FlushStats)>, Vec<Pending>) =
            groups.into_iter().partition_map(|(combined, token)| match self.prepare_flush(&combined.combined) {
                // Already landed: drop out of the shared commit but settle as a
                // success — the rows are in Delta, so draining is correct.
                Ok((batches, _)) if self.already_landed(&combined.combined.project_id, &combined.combined.table_name, &batches) => {
                    self.note_landed_skip(&combined.combined, &batches);
                    itertools::Either::Left(self.settle_flushed_group(combined, token, Ok(())))
                }
                Ok((batches, watermark)) => itertools::Either::Right((combined, token, batches, watermark)),
                Err(e) => itertools::Either::Left(self.settle_flushed_group(combined, token, Err(e))),
            });
        if pending.is_empty() {
            return settled;
        }
        let units: Vec<FlushUnit> = pending
            .iter()
            .map(|(combined, _, batches, watermark)| FlushUnit {
                project_id: combined.combined.project_id.clone(),
                table_name: combined.combined.table_name.clone(),
                batches: batches.clone(),
                watermark: watermark.clone(),
            })
            .collect();
        debug!("Coalescing {} flush group(s) into per-physical-table commit(s)", pending.len());

        // Stall watchdog: an un-timed-out hang would pin `flush_lock` forever.
        let expected = units.len();
        let timeout = self.adaptive_flush_timeout();
        let commit = callback(units);
        let results = if timeout.is_zero() {
            commit.await
        } else {
            tokio::time::timeout(timeout, commit).await.unwrap_or_else(|_| {
                crate::observability::record_flush_stalled();
                error!(
                    "coalesced Delta commit stalled >{:?} across {} group(s) — aborting so flush_lock releases and relief can retry; rows remain durable in MemBuffer + WAL",
                    timeout, expected
                );
                Vec::new()
            })
        };
        // A wrong-length result vector would strand groups (unsettled = leaked
        // in-flight holds). Fail them all instead: a requeue costs a duplicate
        // replay, a strand costs the WAL floor.
        let results = if results.len() == expected {
            results
        } else {
            if !results.is_empty() {
                error!("coalesced writer returned {} results for {} units — failing all groups", results.len(), expected);
            }
            (0..expected).map(|_| Err(anyhow::anyhow!("coalesced commit produced no result for this group"))).collect()
        };
        settled.extend(pending.into_iter().zip(results).map(|((combined, token, batches, _), result)| {
            let outcome = result.map(|added_files| self.index_flushed_files(&combined.combined, batches, added_files));
            self.settle_flushed_group(combined, token, outcome)
        }));
        settled
    }

    /// Apply one commit's post-flush effects — drain/restore, hold
    /// release/orphaning, cursor advance, metrics. Must stay synchronous (no
    /// await): the deadline-bounded shutdown flush may drop this future at any
    /// await point. `flush_completed_total`/`flush_failed_total` count source
    /// bucket IDs, not commits.
    fn settle_flushed_group(&self, combined: CombinedBucket, token: u64, result: anyhow::Result<()>) -> (bool, FlushStats) {
        let CombinedBucket { combined, source_buckets } = combined;
        match result {
            Ok(()) => {
                // Rows are in Delta: remove exactly the snapshotted prefix from each source
                // bucket (late arrivals stay; gen-dirty buckets keep everything for re-flush),
                // release the holds, then advance. A failed advance is benign — the next boot
                // re-replays rows already in Delta and dedup collapses them.
                let drained: Vec<_> = source_buckets.iter().filter(|b| self.mem_buffer.finish_flushed_snapshot(b)).collect();
                if self.test_drop_cursor_advance.load(Ordering::Relaxed) {
                    warn!("test hook: dropping the cursor advance after a landed commit for {}.{}", combined.project_id, combined.table_name);
                } else {
                    self.release_and_advance(&combined.project_id, &combined.table_name, token);
                }
                crate::observability::record_flush(true);
                let drained_rows: u64 = drained.iter().map(|b| b.row_count as u64).sum();
                self.rows_flushed_total.fetch_add(drained_rows, Ordering::Relaxed);
                self.flush_freed_bytes_total.fetch_add(drained.iter().map(|b| flushable_bytes(b)).sum(), Ordering::Relaxed);
                self.flush_completed_total.fetch_add(drained.len() as u64, Ordering::Relaxed);
                debug!(
                    "Flushed coalesced commit: project={}, table={}, buckets={} ({} drained), rows={}",
                    combined.project_id,
                    combined.table_name,
                    source_buckets.len(),
                    drained.len(),
                    combined.row_count
                );
                (true, FlushStats { buckets_flushed: drained.len() as u64, total_rows: drained_rows, ..Default::default() })
            }
            Err(e) => {
                // Merge the snapshots' holds back BEFORE releasing the in-flight holds, so the
                // cursor is pinned by one or the other at every instant. Collected first so
                // every bucket is restored even after one fails — `all` would short-circuit.
                let restored: Vec<_> = source_buckets.iter().map(|bucket| self.mem_buffer.restore_snapshot_holds(bucket)).collect();
                if restored.into_iter().all(|ok| ok) {
                    self.release_inflight_holds(&combined.project_id, &combined.table_name, token);
                } else {
                    // Carry the GC-floor pin: the orphaned rows' WAL files must survive GC.
                    self.orphan_inflight_holds(
                        &combined.project_id,
                        &combined.table_name,
                        token,
                        combined.wal_first_positions.clone(),
                        combined.first_wal_pin_micros,
                    );
                }
                crate::observability::record_flush(false);
                self.note_flush_failure(source_buckets.len() as u64);
                error!(
                    "Failed to flush coalesced commit: project={}, table={}, buckets={:?}: {}",
                    combined.project_id,
                    combined.table_name,
                    source_buckets.iter().map(|b| b.bucket_id).collect::<Vec<_>>(),
                    e
                );
                (false, FlushStats { buckets_failed: source_buckets.len() as u64, ..Default::default() })
            }
        }
    }

    /// Pre-commit half of a bucket flush: raise the Delta watermark, dedup the
    /// rows, and compute the conservative WAL watermark the commit must carry.
    fn prepare_flush(&self, bucket: &FlushableBucket) -> anyhow::Result<(Vec<RecordBatch>, DeltaWatermark)> {
        // Raise the Delta watermark before the commit (see field docs).
        if bucket.max_timestamp != i64::MIN {
            let key = (Arc::<str>::from(bucket.project_id.as_str()), Arc::<str>::from(bucket.table_name.as_str()));
            self.delta_flushed_watermark.entry(key).and_modify(|w| *w = (*w).max(bucket.max_timestamp)).or_insert(bucket.max_timestamp);
        }
        // Last-write-wins dedup on the per-table key set from schema YAML (empty key list =
        // pass-through). Must run before both the Delta write and the tantivy sidecar so both
        // see the same row set.
        let schema = crate::schema::get_schema(&bucket.table_name);
        let dedup_keys = schema.map(|s| s.dedup_keys.as_slice()).unwrap_or(&[]);
        let tiebreak = schema.and_then(|s| s.dedup_tiebreak.as_deref());
        // Never drops tombstones: older versions of the key are already in Delta, so the
        // tombstone must reach Delta to keep winning there.
        let batches = crate::write::mem_buffer::dedup_batches(bucket.batches.clone(), dedup_keys, tiebreak, None)?;
        let after: usize = batches.iter().map(|b| b.num_rows()).sum();
        if bucket.row_count > after {
            let dropped = bucket.row_count - after;
            crate::observability::record_dedup_dropped(dropped as u64);
            debug!("Dedup dropped {} rows: project={}, table={}, bucket_id={}", dropped, bucket.project_id, bucket.table_name, bucket.bucket_id);
        }
        // The commit metadata records the CONSERVATIVE watermark (all holds, including this
        // flush's own), so a boot-time derive from Delta never passes this commit's entries.
        // An as-if-landed watermark would let a crash-before-re-flush skip inserts whose
        // post-DML state lives only behind the cursor, silently reverting acked DML.
        let watermark = self.compute_wal_watermark(&bucket.project_id, &bucket.table_name);
        Ok((batches, watermark))
    }

    /// Flush a bucket to Delta Lake via the configured callback.
    /// The callback MUST complete the Delta commit before returning Ok - this is critical
    /// for durability. We only advance the WAL watermark after this returns successfully.
    async fn flush_bucket(&self, bucket: &FlushableBucket) -> anyhow::Result<()> {
        let (batches, delta_watermark) = self.prepare_flush(bucket)?;
        if self.already_landed(&bucket.project_id, &bucket.table_name, &batches) {
            self.note_landed_skip(bucket, &batches);
            return Ok(());
        }
        // A missing callback must FAIL the flush, never "succeed" having written nothing: the
        // caller reads Ok as "durable in Delta" and drains the rows out of MemBuffer.
        let Some(callback) = self.delta_write_callback.as_ref() else {
            return Err(anyhow::anyhow!(
                "no delta write callback configured for {}.{} — refusing to drain bucket {} (rows stay in MemBuffer + WAL)",
                bucket.project_id,
                bucket.table_name,
                bucket.bucket_id
            ));
        };
        let commit = callback(bucket.project_id.clone(), bucket.table_name.clone(), batches.clone(), delta_watermark);
        // Watchdog: an un-timed-out hung commit would pin `flush_lock` forever. 0 disables it.
        // Dropping the timed-out future cancels polling but a PUT already issued to S3 can
        // still land, so the retained bucket may commit twice — dedup collapses it.
        let timeout = self.adaptive_flush_timeout();
        let added_files = if timeout.is_zero() {
            commit.await?
        } else {
            tokio::time::timeout(timeout, commit)
                .await
                .map_err(|_| {
                    crate::observability::record_flush_stalled();
                    error!(
                        "flush_bucket Delta commit stalled >{:?} (project={}, table={}, bucket_id={}) — aborting this flush so flush_lock releases and relief can retry; rows remain durable in MemBuffer + WAL",
                        timeout, bucket.project_id, bucket.table_name, bucket.bucket_id
                    );
                    anyhow::anyhow!("flush_bucket commit timed out after {:?} (Delta/S3 stalled)", timeout)
                })??
        };
        self.index_flushed_files(bucket, batches, added_files);
        Ok(())
    }

    /// Record a flush declined because its rows are provably already committed. Skips the
    /// Delta write AND the tantivy sidecar but returns Ok, so the caller drains the bucket and
    /// advances the cursor as a real commit would.
    fn note_landed_skip(&self, bucket: &FlushableBucket, batches: &[RecordBatch]) {
        if landed_identity_applies(&bucket.table_name) {
            self.populate_ingest_dedup(&bucket.project_id, &bucket.table_name, batches);
        }
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        self.landed_skips_total.fetch_add(1, Ordering::Relaxed);
        self.landed_skipped_rows_total.fetch_add(rows as u64, Ordering::Relaxed);
        info!(
            "Declined an already-landed flush: project={}, table={}, bucket_id={}, rows={} — these rows are in Delta already (WAL replay re-inserted them)",
            bucket.project_id, bucket.table_name, bucket.bucket_id, rows
        );
    }

    /// Post-commit half of a bucket flush: hand the committed rows + the files this bucket's
    /// project added to the tantivy sidecar. `added_files` is already attributed per project by
    /// the writer, so a coalesced commit feeds each project only its own files.
    ///
    /// The tantivy index build is best-effort and never fails the flush; it is spawned detached
    /// and the semaphore bounds fan-out when many tables flush at once.
    fn index_flushed_files(&self, bucket: &FlushableBucket, batches: Vec<RecordBatch>, added_files: Vec<String>) {
        if landed_identity_applies(&bucket.table_name) {
            if self.config.buffer.landed_skip_enabled() {
                self.note_landed_digests(&bucket.project_id, &bucket.table_name, landed_digest(&batches));
            }
            // Post-commit placement is required: only rows Delta provably holds may ever
            // drop a retry.
            self.populate_ingest_dedup(&bucket.project_id, &bucket.table_name, &batches);
        }
        if self.recovery_active.load(Ordering::Relaxed) {
            self.defer_tantivy_files(&bucket.project_id, &bucket.table_name, added_files);
            crate::observability::record_tantivy_recovery_deferred();
        } else if let Some(ref idx_cb) = self.tantivy_index_callback {
            let cb = idx_cb.clone();
            let pid = bucket.project_id.clone();
            let tname = bucket.table_name.clone();
            let bid = bucket.bucket_id;
            let sem = self.tantivy_spawn_sem.clone();
            tokio::spawn(async move {
                // semaphore closed — process is shutting down
                let Ok(_permit) = sem.acquire_owned().await else { return };
                if let Err(e) = cb(pid.clone(), tname.clone(), batches, added_files).await {
                    crate::observability::record_tantivy_build_failure();
                    warn!("Tantivy index build failed (non-fatal): project={}, table={}, bucket_id={}: {:#}", pid, tname, bid, e);
                }
            });
        }
    }

    /// Warn if any bucket has aged past retention without being flushed. Such
    /// buckets are deliberately KEPT (draining them would lose data); this only
    /// surfaces that flushes are stuck.
    fn evict_drained_metadata(&self) {
        let stuck = self.stale_unflushed_bucket_count();
        if stuck > 0 {
            warn!("{} bucket(s) older than retention ({}min) still in MemBuffer — flush is failing or backed up", stuck, self.config.buffer.retention_mins());
        }
    }

    /// Compute the safe per-shard WAL read-cursor watermark for a topic: the
    /// earliest position still held by unflushed data, or the write tail when nothing holds.
    /// Everything strictly before the watermark is durable in Delta.
    ///
    /// Ordering safety: the tail MUST be snapshotted first. An entry appended after the
    /// snapshot sits at/after the tail so min() can't pass it; one appended before registered
    /// its pending hold under the append lock, so the holds read below observes it.
    fn compute_wal_watermark(&self, project_id: &str, table_name: &str) -> ShardHolds {
        let shards = self.wal.shards_per_topic();
        let baseline: ShardHolds = (0..shards).map(|s| self.wal.current_position_for_shard(project_id, table_name, s).ok()).collect();
        let wm = self.merge_holds_over_baseline(project_id, table_name, baseline);
        // During replay, floor at the pre-recovery cursor P0, or a later boot's
        // `derive_wal_cursors_from_delta` could forward the cursor past un-flushed entries.
        match self.recovery_active.load(Ordering::Relaxed).then(|| self.recovery_commit_floor.get(&(project_id.to_string(), table_name.to_string()))).flatten()
        {
            Some(floor) => merge_wal_holds(&wm, floor.value()),
            None => wm,
        }
    }

    /// Merge all live holds (pending appends, in-flight flushes, orphaned, buffered buckets)
    /// into `baseline`, taking the per-shard min — the earliest position still owned by
    /// unflushed data.
    fn merge_holds_over_baseline(&self, project_id: &str, table_name: &str, baseline: ShardHolds) -> ShardHolds {
        let shards = self.wal.shards_per_topic();
        let key = (project_id.to_string(), table_name.to_string());
        // An absent leg contributes the empty slice, which `merge_wal_holds` treats as
        // identity — so every leg folds in unconditionally.
        let pending = self.pending_wal_holds.get(&key).map_or_else(Vec::new, |pending| {
            pending.values().filter(|(s, _)| *s < shards).fold(vec![None; shards], |mut acc: ShardHolds, (shard, pos)| {
                acc[*shard] = Some(acc[*shard].map_or(*pos, |p| p.min(*pos)));
                acc
            })
        });
        let inflight = self.inflight_flush_holds.get(&key).map_or_else(Vec::new, |m| m.values().fold(Vec::new(), |acc, h| merge_wal_holds(&acc, h)));
        let orphaned = self.orphaned_wal_holds.get(&key).map_or_else(Vec::new, |o| o.0.clone());
        [pending, inflight, orphaned, self.mem_buffer.wal_holds(project_id, table_name, shards)].iter().fold(baseline, |acc, leg| merge_wal_holds(&acc, leg))
    }

    /// Forward-only advance of the topic's persisted read cursor to the current watermark.
    /// Failure is benign: the next boot re-replays entries already in Delta and dedup
    /// collapses them.
    fn advance_wal_watermark(&self, project_id: &str, table_name: &str) {
        let wm = self.compute_wal_watermark(project_id, table_name);
        if let Err(e) = self.wal.merge_persisted_positions(project_id, table_name, &wm) {
            warn!("WAL watermark advance failed for {}.{} (cursor stays behind; replay+dedup cover it): {}", project_id, table_name, e);
        }
    }

    /// Rewrite the recovery rewind marker to the current per-topic watermark, so a crash
    /// resumes from the earliest still-un-drained entry rather than the pre-recovery cursor.
    ///
    /// `applied_frontiers` holds the next unprocessed position for every topic replay has
    /// touched (exhausted shards normalized to their startup write tails); untouched topics
    /// retain their durable P0. Each baseline is min'd with live holds but NOT the commit
    /// floor. Best-effort: a failure only means a crash re-replays a bit more.
    fn refresh_replay_rewind_marker(&self, p0: &HashMap<(String, String), ShardHolds>, applied_frontiers: &HashMap<(String, String), ShardHolds>) {
        let positions: HashMap<(String, String), ShardHolds> = p0
            .iter()
            .map(|(key, p0_holds)| {
                let baseline = applied_frontiers.get(key).unwrap_or(p0_holds).clone();
                (key.clone(), self.merge_holds_over_baseline(&key.0, &key.1, baseline))
            })
            .collect();
        if let Err(e) = self.wal.write_recovery_rewind_marker_at(&positions) {
            warn!("failed to refresh replay rewind marker (a crash would re-replay more, no data loss): {}", e);
        }
    }

    /// Register the holds of buckets taken for an in-flight flush; returns the
    /// token to release with [`Self::release_inflight_holds`].
    fn register_inflight_holds(&self, project_id: &str, table_name: &str, holds: ShardHolds) -> u64 {
        let token = self.wal_hold_seq.fetch_add(1, Ordering::Relaxed);
        self.inflight_flush_holds.entry((project_id.to_string(), table_name.to_string())).or_default().insert(token, holds);
        token
    }

    /// Keep the WAL GC floor covering a taken bucket's entries while its
    /// commit is airborne (its `first_wal_pin_micros` left MemBuffer with it).
    fn register_inflight_pin(&self, token: u64, first_wal_pin_micros: i64) {
        if let Some(pin) = crate::write::mem_buffer::pin_opt(first_wal_pin_micros) {
            self.inflight_wal_pins.insert(token, pin);
        }
    }

    /// Oldest GC-floor pin across orphaned topics — `None` when no orphan carries one.
    fn oldest_orphan_pin_micros(&self) -> Option<i64> {
        self.orphaned_wal_holds.iter().filter_map(|e| crate::write::mem_buffer::pin_opt(e.value().1)).min()
    }

    /// Oldest WAL-append real-clock micros any un-flushed data may depend on
    /// (MemBuffer buckets + airborne takes + per-topic orphans) — the
    /// runtime WAL GC floor. `None` = nothing un-flushed.
    fn oldest_unflushed_wal_append_micros(&self) -> Option<i64> {
        [self.mem_buffer.oldest_wal_append_micros(), self.inflight_wal_pins.iter().map(|e| *e.value()).min(), self.oldest_orphan_pin_micros()]
            .into_iter()
            .flatten()
            .min()
    }

    /// True only when no buffered, airborne, or orphaned WAL-backed work remains.
    pub fn is_drained(&self) -> bool {
        self.oldest_unflushed_wal_append_micros().is_none()
    }

    /// Holds the recovery cursor must park at for a topic: live-bucket holds
    /// merged with orphaned holds — an orphan's rows exist only in the WAL,
    /// so parking past them (before the rewind marker is dropped) would make
    /// them unreplayable forever.
    fn recovery_parking_holds(&self, project_id: &str, table_name: &str, shards: usize) -> ShardHolds {
        let holds = self.mem_buffer.wal_holds(project_id, table_name, shards);
        match self.orphaned_wal_holds.get(&(project_id.to_string(), table_name.to_string())) {
            Some(o) => merge_wal_holds(&holds, &o.0),
            None => holds,
        }
    }

    /// Release an in-flight flush's holds and advance the cursor to the new
    /// watermark — always paired after a successful commit.
    fn release_and_advance(&self, project_id: &str, table_name: &str, token: u64) {
        self.release_inflight_holds(project_id, table_name, token);
        self.advance_wal_watermark(project_id, table_name);
    }

    /// A failed commit whose buckets couldn't be restored: convert the in-flight holds into a
    /// process-lifetime orphan (min-merged holds + GC-floor pin) and release the airborne
    /// marker so DML ordering doesn't wait on it. `first_wal_pin_micros` is the orphaned data's
    /// oldest WAL append time (i64::MAX = none); the pin must outlive the token because those
    /// rows exist only in the WAL until restart.
    fn orphan_inflight_holds(&self, project_id: &str, table_name: &str, token: u64, holds: ShardHolds, first_wal_pin_micros: i64) {
        self.orphaned_wal_holds
            .entry((project_id.to_string(), table_name.to_string()))
            .and_modify(|(existing, pin)| {
                *existing = merge_wal_holds(existing, &holds);
                *pin = (*pin).min(first_wal_pin_micros);
            })
            .or_insert((holds, first_wal_pin_micros));
        self.release_inflight_holds(project_id, table_name, token);
    }

    /// Drop `token` from a per-topic hold map and prune the emptied outer entry — entries
    /// otherwise accumulate forever under project/table churn. `remove_if` re-checks under the
    /// shard lock, so a racing register keeps its entry.
    fn drop_hold_token<V>(map: &DashMap<(String, String), HashMap<u64, V>>, key: &(String, String), token: u64) {
        if let Some(mut m) = map.get_mut(key) {
            m.remove(&token);
        }
        map.remove_if(key, |_, m| m.is_empty());
    }

    fn release_inflight_holds(&self, project_id: &str, table_name: &str, token: u64) {
        Self::drop_hold_token(&self.inflight_flush_holds, &(project_id.to_string(), table_name.to_string()), token);
        // After the hold drop, never before: the transient where the pin outlives its entry
        // over-pins the GC floor, never gaps it.
        self.inflight_wal_pins.remove(&token);
    }

    /// Wait until no Delta commit is airborne for this table. The DML Delta leg must run AFTER
    /// any in-flight commit: a commit snapshotted before the DML's mem apply lands PRE-DML row
    /// values, and only a Delta merge/delete running after it can correct them. Bounded —
    /// proceeds with a warning once the flush watchdog budget expires.
    pub async fn await_inflight_flushes(&self, project_id: &str, table_name: &str) {
        let key = (project_id.to_string(), table_name.to_string());
        // Fallback mirrors the flush watchdog default; the pad covers post-commit bookkeeping
        // before the hold releases.
        const WATCHDOG_DISABLED_FALLBACK: Duration = Duration::from_secs(600);
        const POST_COMMIT_PAD: Duration = Duration::from_secs(30);
        let watchdog = self.config.buffer.flush_bucket_timeout();
        let budget = if watchdog.is_zero() { WATCHDOG_DISABLED_FALLBACK } else { watchdog + POST_COMMIT_PAD };
        let start = std::time::Instant::now();
        while self.inflight_flush_holds.get(&key).is_some_and(|m| !m.is_empty()) {
            if start.elapsed() > budget {
                warn!("await_inflight_flushes: commit still airborne after {:?} for {}.{} — proceeding (hung commit?)", budget, project_id, table_name);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Persist a `clean_shutdown=false` cursor snapshot for the next boot. Call once per flush
    /// CYCLE, not per bucket — the snapshot reads every topic's positions, so per-bucket calls
    /// would be O(N²).
    ///
    /// On write failure any pre-existing snapshot is deleted: a stale file would let the
    /// shallow boot verifier skip commits made since the last successful write.
    ///
    /// Shutdown deliberately does NOT call this — the trailing
    /// `write_cursor_snapshot(true, drained)` in `shutdown_by` is definitive.
    async fn write_post_flush_snapshot(&self) {
        // NEVER during recovery: replay consumes the walrus cursor ahead of what's flushed, so
        // a snapshot here would persist consumed-ahead positions that bypass the P0 watermark
        // floor. `recover_from_wal` writes the parked snapshot itself once done.
        if self.recovery_active.load(Ordering::Relaxed) {
            return;
        }
        // Offloaded to a blocking pool so a slow mount can't stall the flush task.
        let wal = self.wal.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Err(e) = wal.write_cursor_snapshot(false, false) {
                warn!("write_cursor_snapshot (post-flush) failed: {} — will delete stale snapshot", e);
                if let Err(rm_err) = wal.delete_cursor_snapshot() {
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
        let configured = self.config.buffer.stop_grace();
        let grace = if self.is_drained() { configured.min(Duration::from_secs(1)) } else { configured };
        self.shutdown_by(tokio::time::Instant::now() + grace).await
    }

    #[instrument(skip(self))]
    pub async fn shutdown_by(&self, deadline: tokio::time::Instant) -> anyhow::Result<()> {
        info!("BufferedWriteLayer shutdown initiated");

        // The rest of shutdown runs by `deadline` — the remainder of the process-wide stop
        // grace, which must fit inside the orchestrator's SIGTERM→SIGKILL window so the clean
        // cursor snapshot below ALWAYS gets written. Anything not flushed in time is durable in
        // the WAL and replays on next boot. Keep TIMEFUSION_STOP_GRACE_SECS below the
        // orchestrator grace (Docker `StopGracePeriod`).
        self.stop_accepting_writes();
        self.shutdown.cancel();
        let budget = deadline.saturating_duration_since(tokio::time::Instant::now());
        // Cancellation-aware workers normally stop in one scheduler turn; give them a small,
        // explicit slice so the first wedged task can't consume the whole flush allocation.
        // A task still running after it retains its WAL holds, so moving on is safe.
        let already_drained = self.is_drained();
        let cancellation_slice = if already_drained { budget.mul_f32(0.1).min(Duration::from_millis(250)) } else { budget.mul_f32(0.1) };
        let background_deadline = tokio::time::Instant::now() + cancellation_slice;
        let flush_deadline = deadline - budget.mul_f32(0.2); // reserve 20% for the snapshot
        debug!("Shutdown budget: {:?}", budget);

        // The admission fence above makes zero an enduring state: once these writers leave, no
        // accepted connection can append behind the final flush/snapshot. A writer that ignores
        // the deadline forces clean_shutdown=false below rather than delaying WAL-lock handoff.
        let writes_quiesced = self.wait_for_active_writes_until(background_deadline).await;
        if !writes_quiesced {
            warn!("{} write(s) still active at shutdown cutoff; cursor snapshot will be marked dirty", self.active_writes.load(Ordering::Acquire));
        }

        // Deliberately a separate deadline from the shutdown flush, so a hung worker cannot
        // consume the grace that the drain needs.
        for mut handle in self.take_background_tasks().await {
            match tokio::time::timeout_at(background_deadline, &mut handle).await {
                Ok(Ok(())) => debug!("Background task completed cleanly"),
                Ok(Err(e)) => warn!("Background task panicked: {}", e),
                Err(_) => {
                    // Dropping a JoinHandle detaches the task, and after this function returns
                    // the replacement process owns the same WAL directory. Abort and join so no
                    // old worker can touch WAL state behind the new owner's lock.
                    warn!("Background task did not stop within its shutdown slice; aborting before WAL handoff");
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }

        // Best-effort flush of remaining buckets, bounded by flush_deadline so a slice is
        // reserved for the snapshot. A flush stuck on a slow Delta backend is dropped
        // mid-flight (airborne holds stay registered, pinning the cursor) and its buckets
        // replay from the WAL on next boot.
        if already_drained && writes_quiesced && self.is_drained() {
            info!("Shutdown flush skipped: write-fenced layer is already fully drained");
        } else {
            match tokio::time::timeout_at(flush_deadline, self.flush_buckets_where(|_| true)).await {
                Ok(Ok(stats)) => info!(
                    "Shutdown flush: {} bucket(s) flushed ({} rows), {} failed; remainder (if any) replays from WAL",
                    stats.buckets_flushed, stats.total_rows, stats.buckets_failed
                ),
                Ok(Err(e)) => warn!("Shutdown flush error: {} — WAL holds all data", e),
                Err(_) => info!("Shutdown flush deadline reached; remainder replays from WAL"),
            }
        }

        // ALWAYS write the clean-shutdown snapshot (even after a partial flush): it records the
        // post-flush cursor positions so the next boot can skip `derive_wal_cursors_from_delta`.
        // `drained` (the boot-GC authorizer) is a separate claim — true only when nothing
        // un-flushed remains anywhere (buckets, airborne takes, orphans).
        let clean_shutdown = writes_quiesced && self.active_writes.load(Ordering::Acquire) == 0;
        let drained = clean_shutdown && self.is_drained();
        let wal_for_snap = self.wal.clone();
        match tokio::time::timeout_at(deadline, tokio::task::spawn_blocking(move || wal_for_snap.write_cursor_snapshot(clean_shutdown, drained))).await {
            Ok(Ok(Ok(()))) => info!("Cursor snapshot written (clean_shutdown={clean_shutdown}, drained={drained})"),
            Ok(Ok(Err(e))) => warn!("Cursor snapshot on shutdown failed: {} — next boot will Delta-scan", e),
            Ok(Err(join_err)) => warn!("Cursor snapshot blocking task panicked: {} — next boot will Delta-scan", join_err),
            Err(_) => warn!("Cursor snapshot did not finish before shutdown deadline — next boot will Delta-scan"),
        }

        info!("BufferedWriteLayer shutdown complete");
        Ok(())
    }

    /// Acquire the flush mutex for the duration of `f`, pausing the periodic flush task so a
    /// Delta-mutating maintenance op can commit without racing the flush callback. While held,
    /// `flush_completed_buckets` blocks and rows accumulate in MemBuffer — keep `f` short.
    pub async fn with_flush_paused<F, Fut, T>(&self, f: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _guard = self.flush_lock.lock().await;
        f().await
    }

    /// Force flush all buffered data to Delta immediately (coalesced, one
    /// commit per table, `flush_parallelism`-wide).
    pub async fn flush_all_now(&self) -> anyhow::Result<FlushStats> {
        self.flush_buckets_where(|_| true).await
    }

    /// Undo the handoff fence, but only while `generation` still owns it —
    /// shutdown or a newer handoff takes ownership and must not be undone.
    /// Returns whether the fence was ours to reopen.
    fn reopen_write_admission(&self, generation: u64) -> bool {
        let owned = !self.shutdown.is_cancelled() && self.handoff_generation.load(Ordering::Acquire) == generation;
        if owned {
            self.deploy_handoff_ready.store(false, Ordering::Release);
            self.accepting_writes.store(true, Ordering::Release);
        }
        owned
    }

    /// Fence new writes and drain the now-finite WAL tail while this process remains available
    /// for reads. Called immediately before task replacement.
    ///
    /// The fence is leased: if orchestration never delivers SIGTERM, admission reopens after
    /// five minutes. Shutdown invalidates the lease generation before closing admission, so its
    /// permanent fence cannot be undone by the timer.
    pub async fn prepare_deploy_handoff(self: &Arc<Self>) -> anyhow::Result<FlushStats> {
        const HANDOFF_LEASE: Duration = Duration::from_secs(5 * 60);
        const DRAIN_BUDGET: Duration = Duration::from_secs(4 * 60);

        let generation = self.handoff_generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.deploy_handoff_ready.store(false, Ordering::Release);
        crate::write::wal::clear_takeover_request(self.wal.data_dir());
        self.accepting_writes.store(false, Ordering::Release);

        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::time::sleep(HANDOFF_LEASE).await;
            let Some(layer) = weak.upgrade() else { return };
            if layer.reopen_write_admission(generation) {
                warn!("Deploy handoff lease expired before shutdown; write admission reopened");
            }
        });

        // A cancelled query drops this future without taking an error branch; restore admission
        // on both paths, unless shutdown or a newer handoff now owns the fence.
        let reopen_admission = scopeguard::guard((), |_| {
            self.reopen_write_admission(generation);
        });
        let deadline = tokio::time::Instant::now() + DRAIN_BUDGET;
        if !self.wait_for_active_writes_until(deadline).await {
            anyhow::bail!("HANDOFF timed out waiting for admitted writers; write admission reopened");
        }
        let stats = tokio::time::timeout_at(deadline, self.flush_buckets_where(|_| true))
            .await
            .map_err(|_| anyhow::anyhow!("HANDOFF flush exceeded four minutes; write admission reopened"))?
            .map_err(|e| e.context("HANDOFF flush failed; write admission reopened"))?;
        if stats.buckets_failed > 0 || !self.is_drained() {
            anyhow::bail!("HANDOFF left {} failed bucket(s) or undrained WAL state; write admission reopened", stats.buckets_failed);
        }
        info!(
            "Deploy handoff ready: write-fenced and drained ({} bucket(s), {} rows); lease={}s",
            stats.buckets_flushed,
            stats.total_rows,
            HANDOFF_LEASE.as_secs()
        );
        self.deploy_handoff_ready.store(true, Ordering::Release);
        scopeguard::ScopeGuard::into_inner(reopen_admission);
        Ok(stats)
    }

    /// Ask walrus to unlink files made eligible by a successful administrative FLUSH and wait
    /// for the sweep to complete (bounded at ten seconds), so the replacement process doesn't
    /// rescan a large already-consumed WAL.
    pub async fn reclaim_wal_after_flush(&self) {
        let before = self.wal.reclaim_state_counts();
        let epoch = self.wal.request_reclaim_sweep();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !self.wal.reclaim_sweep_complete(epoch) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let after = self.wal.reclaim_state_counts();
        let counts = |c: &crate::write::wal::ReclaimStateCounts| {
            format!("total={}, eligible={}, locked={}, uncheckpointed={}, open={}", c.total, c.eligible, c.locked, c.uncheckpointed, c.open)
        };
        info!("WAL reclaim handoff: before({}), after({}), completed={}", counts(&before), counts(&after), self.wal.reclaim_sweep_complete(epoch));
    }

    /// Flush one taken bucket: force-flushed marking + in-flight hold registration + Delta
    /// commit + watermark advance on success, restore on failure.
    async fn flush_taken_bucket(&self, bucket: &FlushableBucket) -> anyhow::Result<()> {
        // A concurrent insert can revive the taken bucket; the revived bucket's range exclusion
        // would then mask this commit's rows with nothing to punch through — mark BEFORE the
        // commit so no query races into a masked window.
        self.mem_buffer.mark_force_flushed(&bucket.project_id, &bucket.table_name, bucket.bucket_id);
        let token = self.register_inflight_holds(&bucket.project_id, &bucket.table_name, bucket.wal_first_positions.clone());
        self.register_inflight_pin(token, bucket.first_wal_pin_micros);
        // Only now may the take-time parking pin go: the two pins must overlap, never gap, or
        // a concurrent GC sweep could delete the taken bucket's backing WAL file.
        self.mem_buffer.release_taking_pin(bucket.taking_pin_seq);
        match self.flush_bucket(bucket).await {
            Ok(()) => {
                self.release_and_advance(&bucket.project_id, &bucket.table_name, token);
                Ok(())
            }
            Err(e) => {
                // Release the in-flight hold ONLY when the rows made it back into MemBuffer
                // (whose bucket holds then pin the cursor); otherwise orphan it so the
                // watermark still can't pass the WAL-only entries.
                if self.mem_buffer.restore_taken_bucket(bucket) {
                    self.release_inflight_holds(&bucket.project_id, &bucket.table_name, token);
                } else {
                    self.orphan_inflight_holds(&bucket.project_id, &bucket.table_name, token, bucket.wal_first_positions.clone(), bucket.first_wal_pin_micros);
                }
                Err(e)
            }
        }
    }

    /// Check if buffer is empty (all data flushed).
    pub fn is_empty(&self) -> bool {
        self.mem_buffer.get_stats().total_rows == 0
    }

    /// Test hook: synchronously run one eviction-task iteration. Production code should not
    /// call this — `start_background_tasks` already spawns the eviction task.
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

    /// See [`Self::test_drop_cursor_advance`].
    pub fn set_drop_cursor_advance_for_test(&self, on: bool) {
        self.test_drop_cursor_advance.store(on, Ordering::Relaxed);
    }

    /// Test hook: simulates a crash by cancelling background tasks WITHOUT the final-flush
    /// graceful shutdown, so WAL replay is exercised.
    pub async fn crash_for_test(&self) {
        self.shutdown.cancel();
        for handle in self.take_background_tasks().await {
            let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        }
    }

    async fn take_background_tasks(&self) -> Vec<JoinHandle<()>> {
        std::mem::take(&mut *self.background_tasks.lock().await)
    }

    /// Direct accessor for the underlying `MemBuffer`.
    pub fn mem_buffer(&self) -> &MemBuffer {
        &self.mem_buffer
    }

    pub fn get_stats(&self) -> MemBufferStats {
        self.mem_buffer.get_stats()
    }

    /// Snapshot every internal counter for operator visibility; backs
    /// `SELECT * FROM timefusion.stats()`. Each counter is point-in-time but the set is not
    /// mutually consistent — no lock is held across the snapshot.
    pub fn snapshot_stats(&self) -> StatsSnapshot {
        let mem = self.mem_buffer.get_stats();
        let (wal_files, wal_bytes) = self.wal.wal_stats();
        let (quarantine_files, quarantine_bytes) = crate::write::wal::quarantine_stats(self.wal.data_dir());
        let now = crate::support::now_micros();
        let oldest_bucket_age_secs = mem.oldest_bucket_micros.map(|ts| ((now - ts).max(0) / 1_000_000) as u64);
        StatsSnapshot {
            mem_replay_dml_noops: mem.replay_dml_noops,
            mem_project_count: mem.project_count,
            mem_total_buckets: mem.total_buckets,
            mem_total_rows: mem.total_rows,
            mem_total_batches: mem.total_batches,
            mem_estimated_bytes: mem.estimated_memory_bytes,
            reserved_bytes: self.reserved_bytes.load(Ordering::Acquire),
            max_memory_bytes: self.max_memory_bytes(),
            pressure_pct: self.pressure_pct(),
            wal_files,
            quarantine_files,
            quarantine_bytes,
            wal_disk_bytes: wal_bytes,
            wal_shards_per_topic: self.wal.shards_per_topic(),
            wal_known_topics: self.wal.known_topic_count(),
            bucket_duration_micros: crate::write::mem_buffer::bucket_duration_micros(),
            oldest_bucket_age_secs,
            flush_completed_total: self.flush_completed_total.load(Ordering::Relaxed),
            flush_failed_total: self.flush_failed_total.load(Ordering::Relaxed),
            backpressure_engaged_total: self.backpressure_engaged_total.load(Ordering::Relaxed),
            backpressure_rejected_total: self.backpressure_rejected_total.load(Ordering::Relaxed),
            backpressure_force_flush_total: self.backpressure_force_flush_total.load(Ordering::Relaxed),
            rows_ingested_total: self.rows_ingested_total.load(Ordering::Relaxed),
            rows_flushed_total: self.rows_flushed_total.load(Ordering::Relaxed),
            flush_freed_bytes_total: self.flush_freed_bytes_total.load(Ordering::Relaxed),
            process_rss_bytes: crate::observability::process_rss_bytes(),
            orphaned_topics: self.orphaned_wal_holds.len(),
            orphan_pin_age_secs: self.oldest_orphan_pin_micros().map(|pin| ((chrono::Utc::now().timestamp_micros() - pin).max(0) / 1_000_000) as u64),
            drained: self.is_drained(),
            wal_recovery_duration_ms: self.wal_recovery_duration_ms.load(Ordering::Relaxed),
            wal_replay_rows: self.wal_replay_rows.load(Ordering::Relaxed),
            landed_skips_total: self.landed_skips_total.load(Ordering::Relaxed),
            landed_skipped_rows_total: self.landed_skipped_rows_total.load(Ordering::Relaxed),
            wal_recovery_complete: self.wal_recovery_complete.load(Ordering::Relaxed),
            tantivy_recovery_pending_files: self.deferred_tantivy_files.lock().unwrap().len(),
            boot_micros: self.boot_micros,
        }
    }

    pub fn get_bucket_ranges(&self, project_id: &str, table_name: &str) -> Vec<(i64, i64)> {
        self.mem_buffer.get_bucket_ranges(project_id, table_name)
    }

    /// Captures memory rows together with the Delta ranges they replace.
    pub fn snapshot_for_merge(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> anyhow::Result<mem_buffer::MemSnapshot> {
        self.mem_buffer.snapshot_for_merge(project_id, table_name, lo, hi)
    }

    pub fn has_rows_in_range(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> bool {
        self.mem_buffer.has_rows_in_range(project_id, table_name, lo, hi)
    }

    pub fn min_buffered_micros(&self, project_id: &str, table_name: &str, lo: i64, hi: i64) -> Option<i64> {
        self.mem_buffer.min_buffered_micros(project_id, table_name, lo, hi)
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
    pub fn query_partitioned(
        &self, project_id: &str, table_name: &str, filters: &[datafusion::logical_expr::Expr],
    ) -> anyhow::Result<crate::write::mem_buffer::MemLeg> {
        self.mem_buffer.query_partitioned(project_id, table_name, filters)
    }

    /// MemBuffer query with an atomic text-match prefilter: the prefilter and the returned
    /// snapshot reflect the same point-in-time bucket state. Falls through to
    /// `query_partitioned` when `node` is None or the table has no indexed fields.
    pub fn query_partitioned_with_text_match(
        &self, project_id: &str, table_name: &str, filters: &[datafusion::logical_expr::Expr], node: Option<&crate::tantivy::udf::PredNode>,
    ) -> anyhow::Result<crate::write::mem_buffer::MemLeg> {
        self.mem_buffer.query_partitioned_with_text_match(project_id, table_name, filters, node)
    }

    /// Check if a table exists in the memory buffer.
    pub fn has_table(&self, project_id: &str, table_name: &str) -> bool {
        self.mem_buffer.has_table(project_id, table_name)
    }

    /// Serialize a DML `Expr` to *parseable* SQL for the WAL. `Expr`'s Display form renders
    /// literals the replay-side SQL parser rejects (e.g. `TimestampMicrosecond(123, ...)`), so
    /// the unparser is used; it covers less of the Expr space, hence the Display fallback,
    /// which lets the client's statement succeed at the cost of quarantining that one entry.
    fn expr_to_wal_sql(expr: &datafusion::logical_expr::Expr) -> String {
        datafusion::sql::unparser::expr_to_sql(expr).map(|ast| ast.to_string()).unwrap_or_else(|e| {
            warn!("DML expr unparse failed ({e}); falling back to Display form — replay of this entry may quarantine");
            expr.to_string()
        })
    }

    /// Normalize a DML expr to the bare-target + `source__`-prefixed-source form that WAL
    /// replay parses against (mirrors `MemBuffer::update_with_source`'s rewrite), THEN unparse.
    /// Without this the unparser emits table- and alias-qualified columns the replay schema
    /// cannot resolve, quarantining every `UPDATE ... FROM` on restart. `source_cols` are the
    /// source batch's field names; pass an empty set for plain UPDATE/DELETE, whose replay
    /// parses against the bare buffer schema and so needs only the qualifier strip.
    fn normalized_wal_sql(expr: &datafusion::logical_expr::Expr, source_cols: &HashSet<String>) -> String {
        use datafusion::{common::tree_node::TreeNode, logical_expr::Expr};
        let bare = strip_column_qualifiers(expr.clone()).unwrap_or_else(|_| expr.clone());
        let normalized = bare
            .transform(|e| {
                use datafusion::common::{Column, tree_node::Transformed};
                match &e {
                    Expr::Column(c) if c.relation.is_none() && source_cols.contains(&c.name) => {
                        Ok(Transformed::yes(Expr::Column(Column::from_name(format!("source__{}", c.name)))))
                    }
                    _ => Ok(Transformed::no(e)),
                }
            })
            .map(|t| t.data)
            .unwrap_or_else(|_| expr.clone());
        Self::expr_to_wal_sql(&normalized)
    }

    fn assignments_to_wal_sql(assignments: &[(String, datafusion::logical_expr::Expr)], source_cols: &HashSet<String>) -> Vec<(String, String)> {
        assignments.iter().map(|(col, expr)| (col.clone(), Self::normalized_wal_sql(expr, source_cols))).collect()
    }

    /// Run a WAL append + in-memory apply while pinning the entry at every instant: a pending
    /// hold covers the whole append→apply window (registered under the shard append lock, so
    /// registration happens-before the append and a concurrent watermark can never pass an
    /// entry whose hold it hasn't seen), and the apply migrates the pin onto the owning
    /// buckets. The single owner of the pin lifecycle for BOTH inserts and DML — keep it so.
    fn with_wal_pin<T, R, E: From<datafusion::error::DataFusionError>>(
        &self, project_id: &str, table_name: &str, op: &'static str,
        append: impl FnOnce(Box<dyn FnOnce(usize, Option<walrus_rust::WalPosition>) + '_>) -> Result<T, crate::write::wal::WalError>,
        apply: impl FnOnce(Option<(usize, walrus_rust::WalPosition)>) -> Result<R, E>,
    ) -> Result<R, E> {
        let hold_key = (project_id.to_string(), table_name.to_string());
        let token = self.wal_hold_seq.fetch_add(1, Ordering::Relaxed);
        let captured = std::cell::Cell::new(None);
        let res = append(Box::new(|shard, pre| {
            // ORIGIN fallback on a failed tail read: the hold IS the durability pin, so
            // degrade to over-pinning, never to an unpinned acked entry.
            let pre = pre.unwrap_or(walrus_rust::WalPosition::ORIGIN);
            self.pending_wal_holds.entry(hold_key.clone()).or_default().insert(token, (shard, pre));
            captured.set(Some((shard, pre)));
        }));
        let out = match res {
            Ok(_) => apply(captured.get()),
            Err(e) => Err(E::from(wal_err(op)(e))),
        };
        Self::drop_hold_token(&self.pending_wal_holds, &hold_key, token);
        out
    }

    /// [`Self::admit_write`] for the DML entry points, whose error type is DataFusion's.
    fn admit_dml(&self) -> datafusion::error::Result<WriteAdmission<'_>> {
        self.admit_write().map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
    }

    /// Delete rows matching the predicate from the memory buffer.
    /// Logs the operation to WAL for crash recovery, then applies to MemBuffer.
    /// Returns the number of rows deleted.
    #[instrument(skip(self, predicate), fields(project_id, table_name))]
    pub fn delete(&self, project_id: &str, table_name: &str, predicate: Option<&datafusion::logical_expr::Expr>) -> datafusion::error::Result<u64> {
        let _admission = self.admit_dml()?;
        let predicate_sql = predicate.map(|p| Self::normalized_wal_sql(p, &HashSet::new()));
        // WAL first: a failed append must propagate rather than apply in-memory, or the client
        // sees a commit that the next restart's replay loses.
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
        let _admission = self.admit_dml()?;
        let predicate_sql = predicate.map(|p| Self::normalized_wal_sql(p, &HashSet::new()));
        let assignments_sql = Self::assignments_to_wal_sql(assignments, &HashSet::new());
        // See `delete()` — WAL failure must propagate.
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
        let _admission = self.admit_dml()?;
        let source_cols: HashSet<String> = source.schema.fields().iter().map(|f| f.name().clone()).collect();
        let predicate_sql = predicate.map(|p| Self::normalized_wal_sql(p, &source_cols));
        let assignments_sql = Self::assignments_to_wal_sql(assignments, &source_cols);
        let batch_ipc = crate::write::wal::serialize_record_batch(&source.batch).map_err(wal_err("source serialize"))?;
        let serialized_source = crate::write::wal::SerializedSource { join_keys: source.join_keys.clone(), batch_ipc };

        self.with_wal_pin(
            project_id,
            table_name,
            "append_update_with_source",
            |on_pre| self.wal.append_update_with_source(project_id, table_name, predicate_sql.as_deref(), &assignments_sql, &serialized_source, on_pre),
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
    use crate::support::test_helpers::{json_to_batch, query_col_strings, test_layer, test_span};

    /// `landed_digest` must be order-independent (replay reorders batches relative to the
    /// original flush) and must never cancel — hence addition, not XOR, which would collapse a
    /// pair of identical batches to "nothing" and decline a write that must happen.
    #[test]
    fn landed_digest_is_order_independent_and_never_cancels() {
        let batch = |id: &str| json_to_batch(vec![test_span(id, "span", "proj")]).unwrap();
        let (a, b) = (batch("a"), batch("b"));

        assert_eq!(landed_digest(&[a.clone(), b.clone()]), landed_digest(&[b.clone(), a.clone()]));
        assert_ne!(landed_digest(std::slice::from_ref(&a)), landed_digest(std::slice::from_ref(&b)));
        // The XOR trap: two copies must not collapse to "nothing".
        assert_ne!(landed_digest(&[a.clone(), a.clone()]), None);
        assert_ne!(landed_digest(&[a.clone(), a.clone()]), landed_digest(std::slice::from_ref(&a)));
        // No identity for an empty set — nothing to skip.
        assert_eq!(landed_digest(&[]), None);
    }

    /// A retry is a duplicate iff BOTH the key hash and the content hash match; a new version
    /// must not match, and distinct multi-column keys must not collide (`("ab","c")` vs
    /// `("a","bc")`).
    #[test]
    fn per_row_identities_key_and_content_are_length_safe() {
        use datafusion::arrow::array::{Int64Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        // cols: [service(0), id(1), body(2)] — key = [service,id], content = all.
        let schema = Arc::new(Schema::new(vec![
            Field::new("service", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Int64, false),
        ]));
        let batch = |svc: Vec<&str>, id: Vec<&str>, body: Vec<i64>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(svc)), Arc::new(StringArray::from(id)), Arc::new(Int64Array::from(body))])
                .unwrap()
        };
        let key_idxs = &[0usize, 1];
        let content_idxs = &[0usize, 1, 2];

        // Row0 and Row1 share the key but differ in content (a new version).
        let b = batch(vec!["svc", "svc"], vec!["x", "x"], vec![1, 2]);
        let ids = per_row_identities(&b, key_idxs, content_idxs).unwrap();
        assert_eq!(ids[0].0, ids[1].0, "same key => same key_hash");
        assert_ne!(ids[0].1, ids[1].1, "different content => different content_hash (a version, not a dup)");

        // An exact retry: identical key AND content => both hashes equal.
        let b2 = batch(vec!["svc"], vec!["x"], vec![1]);
        let retry = per_row_identities(&b2, key_idxs, content_idxs).unwrap();
        assert_eq!(retry[0], ids[0], "byte-identical row => identical (key_hash, content_hash)");

        // The concatenation trap: ("ab","c") and ("a","bc") must NOT collide.
        let split = batch(vec!["ab", "a"], vec!["c", "bc"], vec![0, 0]);
        let s = per_row_identities(&split, key_idxs, content_idxs).unwrap();
        assert_ne!(s[0].0, s[1].0, "('ab','c') and ('a','bc') must have distinct key hashes");
    }

    #[test]
    fn ingest_dedup_index_matches_retries_versions_and_rotates() {
        // 3 entries per epoch (rotate_at_entries = max_bytes/2/50). max_bytes=300 -> 3.
        let idx = IngestDedupIndex::new(300, 1_000_000, 0);
        idx.populate(1, 100); // key 1, content 100
        assert!(idx.is_duplicate(1, 100), "exact (key,content) => duplicate");
        assert!(!idx.is_duplicate(1, 999), "same key, different content => a version, not a dup");
        assert!(!idx.is_duplicate(2, 100), "unseen key => not a dup");
        assert_eq!(idx.entries(), 1);

        // Last-write-wins on key: a newer version overwrites and the stale content no longer
        // matches (fail-open).
        idx.populate(1, 200);
        assert!(idx.is_duplicate(1, 200));
        assert!(!idx.is_duplicate(1, 100), "overwritten content no longer matches");

        // Fill current to the entry cap, then rotate: its entries survive in `previous`.
        idx.populate(2, 20);
        idx.populate(3, 30); // current now has keys {1,2,3} = 3 >= cap
        idx.maybe_rotate(1); // over size -> rotate
        assert!(idx.is_duplicate(1, 200), "post-rotation, previous epoch is still probed");
        assert!(idx.is_duplicate(3, 30));

        // A second rotation drops the oldest epoch entirely.
        idx.populate(4, 40);
        idx.maybe_rotate(2_000_000); // over age -> rotate again; the {1,2,3} epoch falls off
        assert!(idx.is_duplicate(4, 40), "newest still present");
        assert!(!idx.is_duplicate(1, 200), "two rotations => oldest epoch evicted (coverage bound)");
    }

    /// Pins the hashed column set: on a `version_append` table the TF-stamped tiebreak must be
    /// EXCLUDED from content (`stamp_version` rewrites it per batch, so including it makes the
    /// probe permanently inert); on a non-`version_append` table the tiebreak is client-owned
    /// and IS content.
    #[test]
    fn ingest_identity_idxs_excludes_only_the_stamped_tiebreak() {
        let spec = crate::schema::get_schema("otel_logs_and_spans").unwrap();
        assert!(spec.version_append, "premise: otel is version_append");
        let schema = spec.schema_ref();
        let (key_idxs, content_idxs) = ingest_identity_idxs("otel_logs_and_spans", &schema).unwrap();
        let expected_keys: Vec<usize> = spec.dedup_keys.iter().map(|k| schema.index_of(k).unwrap()).collect();
        assert_eq!(key_idxs, expected_keys, "key hashes must cover exactly the schema dedup_keys");
        let tb = schema.index_of(spec.dedup_tiebreak.as_deref().unwrap()).unwrap();
        assert!(!content_idxs.contains(&tb), "the TF-stamped tiebreak must not be hashed into content");
        assert_eq!(content_idxs.len(), schema.fields().len() - 1, "every other column IS content");

        let dormant = crate::schema::get_schema("mor_dormant").unwrap();
        assert!(!dormant.version_append && dormant.dedup_tiebreak.is_some(), "premise: a client-owned tiebreak");
        let dschema = dormant.schema_ref();
        let (_, content_idxs) = ingest_identity_idxs("mor_dormant", &dschema).unwrap();
        assert_eq!(content_idxs.len(), dschema.fields().len(), "a client-owned tiebreak IS content");

        assert!(ingest_identity_idxs("no_such_table", &dschema).is_none(), "no schema => identity undefined");
    }

    /// The 12GiB unflushed-bytes threshold both WAL brakes are measured against.
    const BRAKE_MAX: u64 = 12 * 1024 * 1024 * 1024;
    const BRAKE_NOW: i64 = 1_000_000_000;

    /// The compaction brake must read the flush BACKLOG, not the WAL directory size — on-disk
    /// WAL routinely exceeds the threshold while flush is healthy.
    #[test_case::test_case(64 * 1024 * 1024, 0 => false ; "healthy: on-disk WAL far over threshold is irrelevant, backlog ~0")]
    #[test_case::test_case(BRAKE_MAX + 1, 0 => true ; "genuine backlog: unflushed bytes over threshold")]
    #[test_case::test_case(1024, BRAKE_NOW - 60_000_000 => true ; "flush broken with a small backlog still brakes")]
    #[test_case::test_case(1024, BRAKE_NOW - 10 * 60_000_000 => false ; "but a stale flush failure does not")]
    fn wal_backlog_brake_ignores_directory_size_and_fires_on_real_backlog(backlog_bytes: u64, last_flush_failure_micros: i64) -> bool {
        wal_backlog_over_threshold(backlog_bytes, BRAKE_MAX, last_flush_failure_micros, BRAKE_NOW)
    }

    /// The EMERGENCY-FLUSH gate must be disk-residue-immune too, or flushed-but-retained WAL
    /// files fire `flush_all_now` in a loop and freeze the dedup drain.
    #[test_case::test_case(15, 50, 780 * 1024 * 1024 => false ; "flushed residue over threshold with a tiny backlog: no storm")]
    #[test_case::test_case(15, 50, BRAKE_MAX + 1 => true ; "real unflushed backlog fires")]
    #[test_case::test_case(51, 50, 0 => true ; "file sprawl fires regardless of bytes")]
    #[test_case::test_case(51, 0, 0 => false ; "max_files=0 disables the sprawl leg")]
    fn wal_emergency_flush_ignores_disk_residue_and_fires_on_backlog_or_sprawl(file_count: usize, max_files: usize, unflushed_bytes: u64) -> bool {
        wal_emergency_flush_needed(file_count, max_files, unflushed_bytes, BRAKE_MAX)
    }

    /// A byte threshold alone misses small persistence debt, so the brake needs an AGE signal —
    /// and the age that matters is DWELL, not event time: a merge-on-read UPDATE appends the
    /// row's ORIGINAL timestamp, so counting by `max_timestamp` would report a just-buffered
    /// bucket as debt and wedge the brake permanently.
    #[test]
    fn stale_unflushed_bucket_count_measures_dwell_not_event_time() {
        let (_dir, cfg, layer) = test_env();
        let now = crate::support::now_micros();
        let retention = cfg.buffer.retention_mins() as i64 * 60 * 1_000_000;

        // A merge-on-read version append: buffered NOW, carrying event time from beyond
        // retention.
        layer.mem_buffer.insert("mor", "mor", create_test_batch("mor"), now - retention - 1).unwrap();
        layer.mem_buffer.insert("hot", "hot", create_test_batch("hot"), now).unwrap();

        assert_eq!(layer.stale_unflushed_bucket_count(), 0, "a backdated arrival is not persistence debt");
        // A cutoff past both insertions proves the count keys on dwell.
        assert_eq!(layer.mem_buffer.count_buckets_dwelling_since(now + 1_000_000), 2, "both buckets are debt once they have dwelled");
        assert!(!layer.is_wal_backlog_over_threshold(), "the age brake must cover debt too small for the byte threshold");
    }

    /// The stored WAL SQL must be normalized (bare target cols + `source__` source cols) so
    /// replay's bare-schema parser resolves it — otherwise every UPDATE...FROM quarantines.
    #[test]
    fn normalized_wal_sql_strips_qualifiers_and_prefixes_source() {
        use datafusion::logical_expr::col;
        let source_cols: HashSet<String> = ["id", "tag"].iter().map(|s| s.to_string()).collect();

        let pred = col("otel_logs_and_spans.context___span_id").is_not_null();
        let norm = BufferedWriteLayer::normalized_wal_sql(&pred, &source_cols);
        assert!(!norm.contains("otel_logs_and_spans"), "target qualifier must be stripped: {norm}");
        assert!(norm.contains("context___span_id"));
        // Raw unparse keeps the qualifier — the form replay cannot resolve.
        assert!(BufferedWriteLayer::expr_to_wal_sql(&pred).contains("otel_logs_and_spans"));

        // Source-aliased col → `source__`-prefixed so it resolves against the widened schema.
        assert_eq!(BufferedWriteLayer::normalized_wal_sql(&col("u.tag"), &source_cols), "source__tag");
    }

    /// A Delta write callback that always succeeds with no files.
    fn noop_delta() -> DeltaWriteCallback {
        Arc::new(|_p, _t, _b, _w| Box::pin(async { Ok(Vec::new()) }))
    }

    /// A succeeding Delta write callback that tallies commits and rows.
    fn counting_delta(commits: Arc<AtomicU64>, rows: Arc<AtomicU64>) -> DeltaWriteCallback {
        Arc::new(move |_p: String, _t: String, batches: Vec<RecordBatch>, _w| {
            let (commits, rows) = (commits.clone(), rows.clone());
            Box::pin(async move {
                commits.fetch_add(1, Ordering::Relaxed);
                rows.fetch_add(batches.iter().map(|b| b.num_rows() as u64).sum::<u64>(), Ordering::Relaxed);
                Ok(vec!["s3://test/part.parquet".to_string()])
            })
        })
    }

    /// Default config rooted at `data_dir`, with `tweak` applied before freezing.
    fn test_config_with(data_dir: PathBuf, tweak: impl FnOnce(&mut AppConfig)) -> Arc<AppConfig> {
        let mut cfg = AppConfig::default();
        cfg.core.timefusion_data_dir = data_dir;
        tweak(&mut cfg);
        Arc::new(cfg)
    }

    /// tempdir + [`create_test_config`] + a layer on it. The `TempDir` must stay bound for the
    /// test's lifetime — dropping it deletes the layer's data dir.
    fn test_env() -> (TempDir, Arc<AppConfig>, BufferedWriteLayer) {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        (dir, cfg, layer)
    }

    /// The stall watchdog must contract as ingest headroom disappears: any fixed value either
    /// aborts legitimate multi-GB drains or lets a hung commit hold the global `flush_lock`
    /// until the ingest buffer fills. The budget has to come from the buffer's state.
    #[tokio::test]
    async fn flush_watchdog_contracts_as_the_ingest_buffer_fills() {
        let (_dir, _cfg, layer) = test_env();

        let ceiling = layer.config.buffer.flush_bucket_timeout();
        let max = layer.max_memory_bytes();

        // Empty buffer: a slow-but-progressing commit gets the full ceiling.
        assert_eq!(layer.adaptive_flush_timeout(), ceiling, "with headroom the watchdog must not cut a progressing commit short");

        // Nearly full: the budget contracts so the lock is released while there
        // is still room to retry into — but never past half the ceiling, or a
        // legitimate large drain can never finish.
        layer.reserved_bytes.store(max * 99 / 100, Ordering::Release);
        let under_pressure = layer.adaptive_flush_timeout();
        assert!(under_pressure < ceiling, "at 99% of the ingest cap the watchdog must contract, got {under_pressure:?} against a {ceiling:?} ceiling");
        assert!(under_pressure >= ceiling / 2, "...but never below half the ceiling, or a legitimate large drain can never finish: {under_pressure:?}");

        layer.reserved_bytes.store(max * 70 / 100, Ordering::Release);
        assert!(layer.adaptive_flush_timeout() >= under_pressure, "the budget must shrink monotonically with pressure");
    }

    fn create_test_config(data_dir: PathBuf) -> Arc<AppConfig> {
        // Dwell off: these tests assert the pre-dwell contract "sealed =>
        // next tick flushes". The gate has its own dedicated tests.
        test_config_with(data_dir, |c| c.buffer.timefusion_flush_dwell_secs = 0)
    }

    fn create_test_batch(project_id: &str) -> RecordBatch {
        json_to_batch(vec![test_span("test1", "span1", project_id), test_span("test2", "span2", project_id), test_span("test3", "span3", project_id)]).unwrap()
    }

    /// Absurd client event timestamps must be dropped at admission, before the
    /// WAL append, or they mint garbage Delta partitions nothing ever touches.
    #[tokio::test]
    async fn insert_drops_rows_with_absurd_event_timestamps() {
        use crate::support::test_helpers::test_span_ts;
        let (_dir, _cfg, layer) = test_env();

        let now = crate::support::now_micros();
        let far_future = 8_486_812_800_000_000i64; // 2238-12-31T00:00:00Z
        let ancient = 100_000_000i64; // 1970-01-01T00:01:40Z — pre-2000 garbage
        let batch = json_to_batch(vec![
            test_span_ts("ok", "span-ok", "pbound", now),
            test_span_ts("bad-future", "span-future", "pbound", far_future),
            test_span_ts("bad-past", "span-past", "pbound", ancient),
        ])
        .unwrap();

        layer.insert("pbound", "otel_logs_and_spans", vec![batch]).await.unwrap();

        let ids = crate::support::test_helpers::query_col_strings(&layer, "pbound", "otel_logs_and_spans", "id");
        assert_eq!(ids, vec!["ok"], "only the sane-timestamp row may be admitted, got {ids:?}");
    }

    /// A deep backlog must drain as MANY bounded commits, not one unbounded one:
    /// a single coalesced commit grows with buffer occupancy while the watchdog
    /// stays fixed, so it eventually can never finish and never frees anything.
    #[serial]
    #[tokio::test]
    async fn a_deep_backlog_flushes_in_bounded_chunks() {
        let dir = tempdir().unwrap();
        let (project, table) = test_ids("u");
        let (commits, rows) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let layer = layer_with(create_test_config(dir.path().to_path_buf()), counting_delta(commits.clone(), rows.clone()));

        const SLICES: i64 = 6;
        let bucket = crate::write::mem_buffer::bucket_duration_micros();
        let now = crate::support::now_micros();
        for slice in 1..=SLICES {
            layer.mem_buffer.insert(&project, &table, create_test_batch(&project), now - slice * bucket).unwrap();
        }

        layer.flush_completed_buckets().await.unwrap();

        let commits = commits.load(Ordering::Relaxed);
        assert_eq!(rows.load(Ordering::Relaxed), (SLICES * 3) as u64, "every buffered row must still reach Delta");
        assert!(commits > 1, "a {SLICES}-slice backlog must not be committed as one unbounded unit, got {commits} commit(s)");
        assert!(
            commits >= SLICES as u64 / FLUSH_CHUNK_BUCKET_IDS as u64,
            "each commit must cover at most {FLUSH_CHUNK_BUCKET_IDS} bucket-id slice(s), got {commits} commit(s) for {SLICES}"
        );
    }

    /// The dwell gate: a freshly-created sealed bucket must NOT flush on the
    /// next tick; it flushes after dwelling one bucket_duration, so a
    /// minute-by-minute dribble coalesces into one parquet file.
    #[serial]
    #[tokio::test]
    async fn fresh_small_sealed_buckets_dwell_then_flush() {
        let dir = tempdir().unwrap();
        let cfg = test_config_with(dir.path().to_path_buf(), |_| {});
        let (project, table) = test_ids("u");
        let commits = Arc::new(AtomicU64::new(0));
        let layer = layer_with(cfg, counting_delta(commits.clone(), Arc::new(AtomicU64::new(0))));

        let bucket = crate::write::mem_buffer::bucket_duration_micros();
        let now = crate::support::now_micros();
        layer.mem_buffer.insert(&project, &table, create_test_batch(&project), now - 3 * bucket).unwrap();

        layer.flush_completed_buckets().await.unwrap();
        assert_eq!(commits.load(Ordering::Relaxed), 0, "a fresh small sealed bucket must dwell, not flush on the next tick");

        crate::support::set_micros(now + 2 * bucket);
        layer.flush_completed_buckets().await.unwrap();
        crate::support::unfreeze();
        assert_eq!(commits.load(Ordering::Relaxed), 1, "the dwelled bucket must flush exactly once");
    }

    #[serial]
    #[tokio::test]
    async fn test_insert_and_query() {
        let (_dir, _cfg, layer) = test_env();
        let (project, table) = test_ids("u");

        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        let results = layer.query(&project, &table, &[]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    /// One tick's groups for several projects on the same table must reach the
    /// writer as ONE call carrying every project's unit and its own watermark,
    /// with each project's buckets drained and its OWN files handed to tantivy.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalesced_flush_hands_every_project_to_one_writer_call() {
        let (_dir, cfg, table, projects) = cotenant_env("cc", 3, |c| c.buffer.timefusion_flush_coalesce_commits = true);

        type WatermarkCalls = Arc<std::sync::Mutex<Vec<Vec<(String, String, DeltaWatermark)>>>>;
        let calls: WatermarkCalls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = calls.clone();
        let mut layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.coalesced_write_callback = Some(Arc::new(move |units: Vec<FlushUnit>| {
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock().unwrap().push(units.iter().map(|u| (u.project_id.clone(), u.table_name.clone(), u.watermark.clone())).collect());
                units.iter().map(|u| Ok(vec![format!("s3://test/{}/project_id={}/date=2026-07-29/part.parquet", u.table_name, u.project_id)])).collect()
            })
        }));
        type IndexedFiles = Arc<std::sync::Mutex<Vec<(String, Vec<String>)>>>;
        let indexed: IndexedFiles = Arc::new(std::sync::Mutex::new(Vec::new()));
        let idx = indexed.clone();
        layer.tantivy_index_callback = Some(Arc::new(move |p: String, _t, _b, files: Vec<String>| {
            let idx = idx.clone();
            Box::pin(async move {
                idx.lock().unwrap().push((p, files));
                Ok(())
            })
        }));
        let layer = Arc::new(layer);

        for project in &projects {
            layer.insert(project, &table, vec![create_test_batch(project)]).await.unwrap();
        }
        let stats = layer.flush_all_now().await.unwrap();

        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1, "coalescing must produce ONE writer call per tick, got {}", calls.len());
        assert_eq!(calls[0].len(), projects.len(), "every project's group must ride the same call");
        for project in &projects {
            let unit = calls[0].iter().find(|(p, _, _)| p == project).unwrap_or_else(|| panic!("{project} missing from the coalesced call"));
            assert_eq!(unit.1, table);
            assert!(unit.2.iter().any(Option::is_some), "{project} unit carried no watermark position");
        }

        assert_eq!(stats.buckets_flushed, projects.len() as u64, "every project's bucket must settle on the shared commit");
        assert_eq!(stats.buckets_failed, 0);
        for project in &projects {
            assert_eq!(rows_in(&layer, project, &table), 0, "{project} rows were not drained after its coalesced commit landed");
        }
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let indexed = indexed.lock().unwrap().clone();
        for project in &projects {
            let files = &indexed.iter().find(|(p, _)| p == project).unwrap_or_else(|| panic!("{project} never reached the indexer")).1;
            assert!(files.iter().all(|f| f.contains(&format!("project_id={project}/"))), "{project} was indexed with a co-tenant's files: {files:?}");
        }
    }

    /// A FAILED shared commit must fail-and-requeue EVERY project it covered —
    /// no partial settle; rows stay in MemBuffer + WAL and re-flush next cycle.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalesced_commit_failure_requeues_every_project() {
        let (_dir, cfg, table, projects) = cotenant_env("cf", 3, |c| c.buffer.timefusion_flush_coalesce_commits = true);

        let fail = Arc::new(AtomicBool::new(true));
        let f = fail.clone();
        let mut layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.coalesced_write_callback = Some(Arc::new(move |units: Vec<FlushUnit>| {
            let f = f.clone();
            Box::pin(async move {
                let failing = f.load(Ordering::Relaxed);
                let files = |u: &FlushUnit| vec![format!("s3://t/project_id={}/p.parquet", u.project_id)];
                units.iter().map(|u| if failing { Err(anyhow::anyhow!("shared commit failed")) } else { Ok(files(u)) }).collect()
            })
        }));
        let layer = Arc::new(layer);

        let mut expected: Vec<usize> = Vec::new();
        for project in &projects {
            layer.insert(project, &table, vec![create_test_batch(project)]).await.unwrap();
            expected.push(rows_in(&layer, project, &table));
        }

        let stats = layer.flush_all_now().await.unwrap();
        assert_eq!(stats.buckets_flushed, 0, "a failed shared commit must settle NO project as flushed");
        assert_eq!(stats.buckets_failed, projects.len() as u64, "every project covered by the failed commit must be counted failed");
        for (project, rows) in projects.iter().zip(&expected) {
            assert_eq!(rows_in(&layer, project, &table), *rows, "{project} lost rows on a failed shared commit — they must stay queued for re-flush");
        }

        fail.store(false, Ordering::Relaxed);
        let stats = layer.flush_all_now().await.unwrap();
        assert_eq!(stats.buckets_flushed, projects.len() as u64, "requeued groups must re-flush on the next cycle");
        for project in &projects {
            assert_eq!(rows_in(&layer, project, &table), 0, "{project} did not drain on the successful retry");
        }
    }

    /// A writer that returns fewer results than units must not strand the
    /// un-answered groups — an unsettled group leaks its in-flight WAL hold and
    /// pins the GC floor until restart. All groups fail instead.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalesced_writer_short_result_vector_fails_every_group() {
        let (_dir, cfg, table, projects) = cotenant_env("cs", 2, |c| c.buffer.timefusion_flush_coalesce_commits = true);

        let mut layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.coalesced_write_callback = Some(Arc::new(move |_units: Vec<FlushUnit>| Box::pin(async move { vec![Ok(Vec::new())] })));
        let layer = Arc::new(layer);
        for project in &projects {
            layer.insert(project, &table, vec![create_test_batch(project)]).await.unwrap();
        }
        let stats = layer.flush_all_now().await.unwrap();
        assert_eq!(stats.buckets_flushed, 0);
        assert_eq!(stats.buckets_failed, projects.len() as u64, "every group must be settled (as failed), never left stranded");
        for project in &projects {
            assert!(rows_in(&layer, project, &table) > 0, "{project} rows must survive for re-flush");
        }
    }

    /// With coalescing OFF (the default) the per-project writer is used and the
    /// coalescing writer is never called, even when both are wired.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalescing_disabled_uses_the_per_project_writer() {
        let (_dir, cfg, table, projects) = cotenant_env("cd", 2, |c| c.buffer.timefusion_flush_dwell_secs = 0);
        assert!(!cfg.buffer.flush_coalesce_commits(), "coalescing must default to OFF");

        let (per_project, coalesced) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let (pp, cc) = (per_project.clone(), coalesced.clone());
        let mut layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.delta_write_callback = Some(Arc::new(move |p: String, _t, _b, _w| {
            let pp = pp.clone();
            Box::pin(async move {
                pp.fetch_add(1, Ordering::Relaxed);
                Ok(vec![format!("s3://t/project_id={p}/p.parquet")])
            })
        }));
        layer.coalesced_write_callback = Some(Arc::new(move |units: Vec<FlushUnit>| {
            let cc = cc.clone();
            Box::pin(async move {
                cc.fetch_add(1, Ordering::Relaxed);
                units.iter().map(|_| Ok(Vec::new())).collect()
            })
        }));
        let layer = Arc::new(layer);
        for project in &projects {
            layer.insert(project, &table, vec![create_test_batch(project)]).await.unwrap();
        }
        layer.flush_all_now().await.unwrap();
        assert_eq!(coalesced.load(Ordering::Relaxed), 0, "coalescing writer must not run while the flag is off");
        assert_eq!(per_project.load(Ordering::Relaxed), projects.len() as u64, "each project keeps its own commit while the flag is off");
    }

    #[serial]
    #[tokio::test]
    async fn test_recovery() {
        let (_dir, cfg, project, table) = test_ids_env("r");

        ack_then_crash(&cfg, &project, &table).await;

        {
            let layer = Arc::new(test_layer(cfg).unwrap());
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(stats.entries_replayed > 0, "Expected entries to be replayed from WAL");
            assert_eq!(layer.snapshot_stats().wal_recovery_duration_ms, stats.recovery_duration_ms);
            assert!(layer.snapshot_stats().wal_recovery_complete);

            let results = layer.query(&project, &table, &[]).unwrap();
            assert!(!results.is_empty(), "Expected results after WAL recovery");
        }
    }

    /// The WAL cursor is the only sound replay boundary: entry AGE must never
    /// drop entries, or an acked write that sat un-flushed longer than
    /// retention is discarded at the next boot.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn wal_replay_restores_entries_older_than_retention() {
        let (_dir, cfg, project, table) = test_ids_env("ar");

        // Crash without flushing — the WAL is the rows' only copy.
        ack_then_crash(&cfg, &project, &table).await;

        let retention_micros = cfg.buffer.retention_mins() as i64 * 60 * 1_000_000;
        crate::support::set_micros(chrono::Utc::now().timestamp_micros() + 2 * retention_micros);

        let layer = Arc::new(crate::support::test_helpers::test_layer(cfg).unwrap());
        let recovered = layer.recover_from_wal().await;
        crate::support::unfreeze();
        let stats = recovered.unwrap();
        assert!(stats.entries_replayed > 0, "aged un-flushed WAL entries were dropped instead of replayed");
        assert_eq!(rows_in(&layer, &project, &table), 3, "acked rows lost: aged WAL entries were consumed without replay");
    }

    /// The load-bearing assumption of the landed-batch skip: a bucket rebuilt
    /// from the WAL must re-flush to the SAME digest as the original, or the
    /// skip can never fire. Replay reads shard-by-shard while the original
    /// arrived as separate `insert` calls, which is why `landed_digest`
    /// combines per-batch hashes commutatively.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn a_replayed_bucket_reflushes_to_the_same_digest() {
        let (_dir, cfg, project, table) = test_ids_env("ld");

        // Capture what the flush hands the writer, then fail the commit: the
        // crash-after-commit-before-cursor-advance shape.
        let flush_and_digest = |layer: &mut BufferedWriteLayer| {
            let seen: Arc<std::sync::Mutex<Vec<RecordBatch>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink = Arc::clone(&seen);
            layer.delta_write_callback = Some(Arc::new(move |_p, _t, batches: Vec<RecordBatch>, _w| {
                sink.lock().unwrap().extend(batches);
                Box::pin(async { Err(anyhow!("commit landed but this process never learned it")) })
            }));
            seen
        };

        let ts = crate::support::now_micros();
        let rows = |ids: [&str; 2]| ids.into_iter().map(|id| crate::support::test_helpers::test_span_ts(id, "s", &project, ts)).collect::<Vec<_>>();

        let original = {
            let mut layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            let seen = flush_and_digest(&mut layer);
            let layer = Arc::new(layer);
            // Several separate inserts, so the bucket holds multiple batches
            // whose ORDER replay could plausibly change.
            for pair in [["a", "b"], ["c", "d"], ["e", "f"]] {
                layer.insert(&project, &table, vec![crate::support::test_helpers::json_to_batch(rows(pair)).unwrap()]).await.unwrap();
            }
            let _ = layer.flush_all_now().await;
            let batches = seen.lock().unwrap().clone();
            assert!(!batches.is_empty(), "the flush never reached the writer — the test proves nothing");
            landed_digest(&batches).expect("original flush must have an identity")
        };

        let mut layer = crate::support::test_helpers::test_layer(cfg).unwrap();
        let seen = flush_and_digest(&mut layer);
        let layer = Arc::new(layer);
        let stats = layer.recover_from_wal().await.unwrap();
        assert!(stats.entries_replayed > 0, "nothing replayed — the test proves nothing");
        let _ = layer.flush_all_now().await;
        let replayed = landed_digest(&seen.lock().unwrap().clone()).expect("replayed flush must have an identity");

        assert_eq!(original, replayed, "a replayed bucket must re-flush to the same identity, or the landed-batch skip can never fire");
    }

    /// A boot that knows the identity of what already landed declines to write
    /// it again, and still DRAINS the bucket and advances the cursor. The guard:
    /// a bucket whose content changed (a DML retiring a row) must NOT be
    /// declined — which is why identity is taken over CONTENT, not WAL positions.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_already_landed_batch_set_is_declined_but_a_changed_one_still_flushes() {
        // `table` declares `dedup_keys` — identity is only defined there.
        let (_dir, cfg, project, keyless) = test_env_with("lk", |c| {
            c.buffer.timefusion_flush_dwell_secs = 0;
            c.buffer.timefusion_landed_skip_enabled = true;
        });
        let table = "otel_logs_and_spans".to_string();

        let writes = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&writes);
        let layer = layer_with(
            Arc::clone(&cfg),
            Arc::new(move |_p, _t, _b, _w| {
                counted.fetch_add(1, Ordering::Relaxed);
                Box::pin(async { Ok(Vec::new()) })
            }),
        );

        let ts = crate::support::now_micros();
        let pair = |ids: [&str; 2]| json_to_batch(ids.map(|id| crate::support::test_helpers::test_span_ts(id, "s", &project, ts)).to_vec()).unwrap();
        layer.insert(&project, &table, vec![pair(["a", "b"])]).await.unwrap();

        // What a boot would load from Delta: the identity of the batch set the
        // buffer is holding. Take it the same way the writer does.
        let staged = layer.mem_buffer.query(&project, &table, &[]).unwrap();
        let digest = landed_digest(&staged).expect("buffered rows must have an identity");
        layer.note_landed_digests(&project, &table, [digest]);

        layer.flush_all_now().await.unwrap();
        assert_eq!(writes.load(Ordering::Relaxed), 0, "a flush whose rows are provably already in Delta must not be written again");
        assert_eq!(layer.snapshot_stats().landed_skips_total, 1);
        assert_eq!(layer.snapshot_stats().landed_skipped_rows_total, 2);
        assert_eq!(layer.snapshot_stats().mem_total_rows, 0, "a declined flush must still DRAIN — otherwise the rows replay forever");

        // Fresh rows, then a DML that changes what the bucket holds: the noted
        // identity no longer matches, so this one must reach Delta.
        layer.insert(&project, &table, vec![pair(["c", "d"])]).await.unwrap();
        let staged = layer.mem_buffer.query(&project, &table, &[]).unwrap();
        layer.note_landed_digests(&project, &table, landed_digest(&staged));
        let pred = datafusion::prelude::col("id").eq(datafusion::prelude::lit(datafusion::scalar::ScalarValue::Utf8View(Some("c".into()))));
        let deleted = layer.delete(&project, &table, Some(&pred)).unwrap();
        assert_eq!(deleted, 1, "the DML must actually change the bucket, or the assertion below proves nothing");

        layer.flush_all_now().await.unwrap();
        assert_eq!(writes.load(Ordering::Relaxed), 1, "a bucket whose content changed must still be written — a DML must never be swallowed by the skip");
        assert_eq!(layer.snapshot_stats().landed_skips_total, 1, "the changed bucket must not count as a skip");

        // A table with NO dedup_keys has no identity: two byte-identical
        // batches are two distinct facts, so even an exact digest match writes.
        assert!(!landed_identity_applies(&keyless), "a schema-less table must not have a landed identity");
        layer.insert(&project, &keyless, vec![span_batch("a", "s", &project, ts)]).await.unwrap();
        let staged = layer.mem_buffer.query(&project, &keyless, &[]).unwrap();
        layer.note_landed_digests(&project, &keyless, landed_digest(&staged));
        layer.flush_all_now().await.unwrap();
        assert_eq!(writes.load(Ordering::Relaxed), 2, "a keyless table's duplicate rows are DISTINCT DATA — declining them would lose acked writes");
    }

    /// Ingest dedup: an exact-content retry re-sent after its flush drained the
    /// buffer is DROPPED (the insert still acks Ok — the content is durable);
    /// the same key with different content is a version and is KEPT.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn ingest_dedup_drops_an_exact_retry_but_keeps_a_new_version() {
        let (_dir, cfg, project, _) = test_ids_env("sd");
        let table = "otel_logs_and_spans".to_string();

        let layer = layer_with(Arc::clone(&cfg), noop_delta());
        let stats = crate::observability::maintenance_stats();
        let (drop0, hits0) = (stats.ingest_dedup_dropped_rows.load(Ordering::Relaxed), stats.ingest_dedup_key_hits.load(Ordering::Relaxed));

        let ts = crate::support::now_micros();
        let row = |name: &str| span_batch("a", name, &project, ts);

        layer.insert(&project, &table, vec![row("s")]).await.unwrap();
        layer.flush_all_now().await.unwrap(); // populate runs POST-COMMIT
        assert!(stats.ingest_dedup_index_entries.load(Ordering::Relaxed) >= 1, "a landed flush must populate the index");

        // Identical content re-sent after the flush drained it: dropped BEFORE
        // the buffer (and before the WAL), yet still acked.
        layer.insert(&project, &table, vec![row("s")]).await.unwrap();
        assert_eq!(rows_in(&layer, &project, &table), 0, "an exact retry of committed content must be dropped, not buffered again");
        assert_eq!(stats.ingest_dedup_dropped_rows.load(Ordering::Relaxed) - drop0, 1);
        assert_eq!(stats.ingest_dedup_key_hits.load(Ordering::Relaxed) - hits0, 1);

        layer.insert(&project, &table, vec![row("edited")]).await.unwrap();
        assert_eq!(rows_in(&layer, &project, &table), 1, "a new version (same key, different content) must pass through");
        assert_eq!(stats.ingest_dedup_dropped_rows.load(Ordering::Relaxed) - drop0, 1, "a version is not a retry");
        assert_eq!(stats.ingest_dedup_key_hits.load(Ordering::Relaxed) - hits0, 2, "its key hit is version traffic, counted apart");

        // A DML re-append (bound=false) legitimately re-states row content and
        // must BYPASS the filter even when the index knows the identity.
        layer.flush_all_now().await.unwrap();
        assert_eq!(rows_in(&layer, &project, &table), 0);
        layer.insert_bounded(&project, &table, vec![row("edited")], false).await.unwrap();
        assert_eq!(rows_in(&layer, &project, &table), 1, "bound=false (DML re-append) must never be filtered — dropping it silently reverts acked DML");
        assert_eq!(stats.ingest_dedup_dropped_rows.load(Ordering::Relaxed) - drop0, 1);
    }

    /// A table with no schema/dedup_keys has NO content identity: nothing is
    /// filtered, and no per-table index is even built.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn ingest_dedup_leaves_a_keyless_table_untouched() {
        let (_dir, cfg, project, table) = test_ids_env("od");
        assert!(!landed_identity_applies(&table), "premise: no schema => no identity");

        let layer = layer_with(Arc::clone(&cfg), noop_delta());

        let ts = crate::support::now_micros();
        let batch = || span_batch("a", "s", &project, ts);
        layer.insert(&project, &table, vec![batch()]).await.unwrap();
        layer.flush_all_now().await.unwrap();
        layer.insert(&project, &table, vec![batch()]).await.unwrap();

        assert_eq!(rows_in(&layer, &project, &table), 1, "a keyless table's byte-identical re-send is DISTINCT DATA and must land");
        assert!(layer.ingest_dedup.is_empty(), "no identity => no index built");
    }

    /// WAL replay must re-insert rows freely even when the ingest-dedup index
    /// holds their exact identities — replay goes through `mem_buffer.insert`
    /// directly, never the probe. Rerouting it through `insert_bounded` would
    /// silently revert acked DML.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn wal_replay_reinserts_rows_the_ingest_dedup_index_knows() {
        let (_dir, cfg, project, _) = test_ids_env("rp");
        let table = "otel_logs_and_spans".to_string();
        let batch = span_batch("a", "s", &project, crate::support::now_micros());

        // Unclean exit: the row is in the WAL, never flushed.
        {
            let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
            layer.insert(&project, &table, vec![batch.clone()]).await.unwrap();
        }

        // Next boot, with the row's identity ALREADY in the index.
        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
        let compacted = crate::write::mem_buffer::compact_batch(batch);
        let (key_idxs, content_idxs) = ingest_identity_idxs(&table, &compacted.schema()).unwrap();
        let idx = layer.ingest_dedup_index(&project, &table);
        for (k, c) in per_row_identities(&compacted, &key_idxs, &content_idxs).unwrap() {
            idx.populate(k, c);
        }
        let stats = crate::observability::maintenance_stats();
        let drop0 = stats.ingest_dedup_dropped_rows.load(Ordering::Relaxed);

        layer.recover_from_wal().await.unwrap();
        assert_eq!(rows_in(&layer, &project, &table), 1, "replay must re-insert acked rows even when the index knows their identity");
        assert_eq!(stats.ingest_dedup_dropped_rows.load(Ordering::Relaxed), drop0, "replay must never take the drop path");
    }

    /// The two-stage filter on a MIXED batch: the dup row is dropped, the new
    /// row and the new-version row survive in one filtered batch.
    #[test]
    fn ingest_dedup_filter_batch_drops_only_the_exact_retry_rows() {
        let table = "otel_logs_and_spans";
        let ts = crate::support::now_micros();
        let mk = |rows: Vec<(&str, &str)>| {
            let spans = rows.into_iter().map(|(id, name)| crate::support::test_helpers::test_span_ts(id, name, "p", ts)).collect();
            crate::write::mem_buffer::compact_batch(json_to_batch(spans).unwrap())
        };
        let idx = IngestDedupIndex::new(INGEST_DEDUP_MAX_BYTES, INGEST_DEDUP_WINDOW_MICROS, 0);

        let first = mk(vec![("a", "s"), ("b", "t")]);
        let (kept, hits, dropped) = ingest_dedup_filter_batch(&idx, table, first.clone());
        assert_eq!((kept.as_ref().unwrap().num_rows(), hits, dropped), (2, 0, 0));

        let (key_idxs, content_idxs) = ingest_identity_idxs(table, &first.schema()).unwrap();
        for (k, c) in per_row_identities(&first, &key_idxs, &content_idxs).unwrap() {
            idx.populate(k, c);
        }

        let mixed = mk(vec![("a", "s"), ("b", "edited"), ("c", "u")]);
        let (kept, hits, dropped) = ingest_dedup_filter_batch(&idx, table, mixed);
        assert_eq!((hits, dropped), (2, 1), "two key hits, only the exact-content one drops");
        assert_eq!(kept.as_ref().unwrap().num_rows(), 2);

        let (kept, _, dropped) = ingest_dedup_filter_batch(&idx, table, mk(vec![("a", "s"), ("b", "t")]));
        assert!(kept.is_none(), "an all-retry batch must vanish entirely");
        assert_eq!(dropped, 2);
    }

    /// Replay must drain to Delta when it crosses the memory budget, and every
    /// mid-replay commit's watermark must stay ≤ the pre-recovery cursor P0 —
    /// a tail claim lets the next boot skip entries the commit never contained.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn wal_replay_drains_to_budget() {
        // Roomy budget so the backlog can be acked into the WAL.
        let (dir, cfg_big, project, table) = test_ids_env("bb");

        const ROWS: u64 = 96;
        let fat = "x".repeat(1024 * 1024); // ~1MB per row
        {
            let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg_big)).unwrap();
            for i in 0..ROWS {
                let mut span = crate::support::test_helpers::test_span(&format!("row{i}"), "n", &project);
                span["summary"] = serde_json::json!([fat]);
                let batch = json_to_batch(vec![span]).unwrap();
                layer.insert(&project, &table, vec![batch]).await.unwrap();
            }
            // Crash without flushing: the acked rows exist only in the WAL.
        }

        // Tight budget — replay must flush-to-make-room.
        let cfg_small = test_config_with(dir.path().to_path_buf(), |c| c.buffer.timefusion_buffer_max_memory_mb = 64);

        let flushed_rows = Arc::new(AtomicU64::new(0));
        let flushes = Arc::new(AtomicU64::new(0));
        let unsafe_claims = Arc::new(AtomicU64::new(0));
        let (fr, fl, tc) = (flushed_rows.clone(), flushes.clone(), unsafe_claims.clone());
        let mut layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg_small)).unwrap();
        let wal_probe = Arc::clone(layer.wal());
        layer.delta_write_callback = Some(Arc::new(move |p: String, t: String, batches: Vec<RecordBatch>, wm: DeltaWatermark| {
            let (fr, fl, tc, wal_probe) = (fr.clone(), fl.clone(), tc.clone(), wal_probe.clone());
            Box::pin(async move {
                fl.fetch_add(1, Ordering::Relaxed);
                fr.fetch_add(batches.iter().map(|b| b.num_rows() as u64).sum::<u64>(), Ordering::Relaxed);
                // A mid-replay commit's watermark must not exceed the durable
                // P0 cursor; an absent persisted cursor means ORIGIN.
                let read_cursor = wal_probe.persisted_read_positions(&p, &t).unwrap_or_default();
                for (shard, claimed) in wm.iter().enumerate() {
                    if let Some(claimed) = claimed
                        && *claimed > read_cursor.get(shard).copied().flatten().unwrap_or(walrus_rust::WalPosition::ORIGIN)
                    {
                        tc.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Ok(vec![format!("s3://test/otel_logs_and_spans/project_id={p}/date=2026-07-20/part-{t}.parquet")])
            })
        }));
        let tantivy_builds = Arc::new(AtomicU64::new(0));
        let tb = tantivy_builds.clone();
        layer.tantivy_index_callback = Some(Arc::new(move |_p, _t, _b, _files| {
            let tb = tb.clone();
            Box::pin(async move {
                tb.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
        }));
        let layer = Arc::new(layer);

        layer.recover_from_wal().await.unwrap();

        assert_eq!(tantivy_builds.load(Ordering::Relaxed), 0, "replay relief must not build Tantivy indexes before WAL recovery completes");
        assert!(!layer.deferred_tantivy_files().is_empty(), "replay-relief output files must be queued for post-recovery indexing");
        assert!(flushes.load(Ordering::Relaxed) > 0, "replay never drained to Delta despite exceeding the budget");
        assert_eq!(unsafe_claims.load(Ordering::Relaxed), 0, "mid-replay commit claimed a watermark beyond the durable read cursor");
        // Mid-replay relief flushes must not persist consumed-ahead cursors:
        // the snapshot must hold exactly the PARKED positions, or a post-crash
        // boot's forward-only restore skips un-flushed replayed entries.
        let snap = layer.wal().load_cursor_snapshot().expect("recovery with mid-replay flushes must rewrite the snapshot");
        let parked: Vec<Option<(u64, u64)>> =
            layer.wal().persisted_read_positions(&project, &table).unwrap().into_iter().map(|p| p.map(|p| (p.block_id, p.offset))).collect();
        assert_eq!(
            snap.entries.get(&format!("{project}:{table}")),
            Some(&parked),
            "cursor snapshot must match the parked positions, not the consumed-ahead replay cursor"
        );
        let stats = layer.snapshot_stats();
        assert!(stats.mem_estimated_bytes <= stats.max_memory_bytes, "replay finished over budget: {} > {}", stats.mem_estimated_bytes, stats.max_memory_bytes);
        let remaining: u64 = layer.query(&project, &table, &[]).unwrap().iter().map(|b| b.num_rows() as u64).sum();
        assert_eq!(flushed_rows.load(Ordering::Relaxed) + remaining, ROWS, "rows lost across budget-bounded replay");
    }

    /// Ack `per_bucket` ~1MB rows into each of two event-time buckets 20 min
    /// apart per tenant, oldest-first so WAL order ≈ bucket order. Drops the
    /// layer without flushing: the rows live only in the WAL.
    async fn seed_fat_wal_backlog(cfg: &Arc<AppConfig>, tenants: &[(String, String)], per_bucket: u64) {
        let base = chrono::Utc::now().timestamp_micros();
        let gap = 20 * 60 * 1_000_000i64;
        let fat = "x".repeat(1024 * 1024); // ~1MB/row so the tight budget forces a relief
        let layer = crate::support::test_helpers::test_layer(Arc::clone(cfg)).unwrap();
        for (project, table) in tenants {
            for (b, ts) in [base, base + gap].into_iter().enumerate() {
                for i in 0..per_bucket {
                    let mut span = crate::support::test_helpers::test_span_ts(&format!("b{b}r{i}"), "n", project, ts + i as i64);
                    span["summary"] = serde_json::json!([fat]);
                    layer.insert(project, table, vec![json_to_batch(vec![span]).unwrap()]).await.unwrap();
                }
            }
        }
    }

    /// A layer whose Delta callback tallies the rows it was handed.
    fn counting_row_layer(cfg: &Arc<AppConfig>, flushed: Arc<AtomicU64>) -> Arc<BufferedWriteLayer> {
        layer_with(
            Arc::clone(cfg),
            Arc::new(move |_p: String, _t: String, batches: Vec<RecordBatch>, _wm: DeltaWatermark| {
                let flushed = flushed.clone();
                Box::pin(async move {
                    flushed.fetch_add(batches.iter().map(|b| b.num_rows() as u64).sum::<u64>(), Ordering::Relaxed);
                    Ok(Vec::new())
                })
            }),
        )
    }

    /// Crash replay after its first relief drain, then resume on a fresh layer.
    /// Returns (rows drained before the crash, rows drained after it, the
    /// resumed run's stats, the resumed layer).
    async fn crash_then_resume_replay(cfg: &Arc<AppConfig>) -> (u64, u64, RecoveryStats, Arc<BufferedWriteLayer>) {
        let pre = Arc::new(AtomicU64::new(0));
        let layer2 = counting_row_layer(cfg, pre.clone());
        layer2.test_crash_after_reliefs.store(1, Ordering::Relaxed);
        assert!(layer2.recover_from_wal().await.is_err(), "test hook should have crashed replay mid-run");
        let flushed_pre = pre.load(Ordering::Relaxed);
        drop(layer2);

        let post = Arc::new(AtomicU64::new(0));
        let layer3 = counting_row_layer(cfg, post.clone());
        let stats = layer3.recover_from_wal().await.unwrap();
        (flushed_pre, post.load(Ordering::Relaxed), stats, layer3)
    }

    /// A crash mid-replay must re-replay only the still-un-drained tail, not
    /// the whole backlog (pgwire gates on replay completion). Crashes after the
    /// first relief drain commits; the resumed boot must skip the drained
    /// prefix and lose no rows.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn resumable_replay_after_crash_skips_drained_prefix() {
        let (dir, cfg_big, project, table) = test_ids_env("rr");

        const PER_BUCKET: u64 = 48;
        const TOTAL: u64 = 2 * PER_BUCKET;
        seed_fat_wal_backlog(&cfg_big, &[(project.clone(), table.clone())], PER_BUCKET).await;

        // Tight budget, so replay must relief-drain.
        let cfg_small = test_config_with(dir.path().to_path_buf(), |c| c.buffer.timefusion_buffer_max_memory_mb = 64);

        // Crash right after the first relief drain advances the marker.
        let (flushed_pre_crash, flushed_post, stats, layer3) = crash_then_resume_replay(&cfg_small).await;
        assert!(flushed_pre_crash > 0, "no bucket drained before the simulated crash");
        let replayed = stats.entries_replayed;
        assert!(replayed < TOTAL, "resume re-replayed the whole backlog ({replayed} of {TOTAL}) — rewind marker never advanced");
        // No acked-write loss: drained-across-both-lives plus still-buffered
        // must cover every original row.
        let buffered = rows_in(&layer3, &project, &table) as u64;
        assert!(flushed_pre_crash + flushed_post + buffered >= TOTAL, "rows lost across crash+resume: {flushed_pre_crash}+{flushed_post}+{buffered} < {TOTAL}");
    }

    /// Resumable replay across MULTIPLE topics: the mid-replay marker refresh
    /// rebuilds the marker for every topic, and a topic the iterator hasn't
    /// reached yet must keep its pre-recovery cursor, NOT be nulled to ORIGIN
    /// (which re-replays its entire history).
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn resumable_replay_multi_topic_no_loss_across_crash() {
        let (dir, cfg_big, id, _) = test_ids_env("mt");
        let tenants: Vec<(String, String)> = (0..2).map(|k| (format!("{id}{k}"), format!("{id}{k}"))).collect();

        const PER_BUCKET: u64 = 32;
        const PER_TENANT: u64 = 2 * PER_BUCKET;
        const TOTAL: u64 = 2 * PER_TENANT;
        seed_fat_wal_backlog(&cfg_big, &tenants, PER_BUCKET).await;

        let cfg_small = test_config_with(dir.path().to_path_buf(), |c| c.buffer.timefusion_buffer_max_memory_mb = 64);

        // Crash after the first relief drain — at that point the second
        // tenant's topic has almost certainly not been reached yet.
        let (flushed_pre, flushed_post, stats, layer3) = crash_then_resume_replay(&cfg_small).await;
        let buffered: u64 = tenants.iter().map(|(project, table)| rows_in(&layer3, project, table) as u64).sum();
        assert!(flushed_pre + flushed_post + buffered >= TOTAL, "rows lost across multi-topic crash+resume: {flushed_pre}+{flushed_post}+{buffered} < {TOTAL}");
        // A tenant rewound to ORIGIN would re-replay its full history on top of
        // the crashed tenant's remainder, pushing entries_replayed over TOTAL.
        let replayed = stats.entries_replayed;
        assert!(replayed <= TOTAL, "resume re-replayed more than the whole backlog ({replayed} > {TOTAL}) — a caught-up topic was rewound to ORIGIN");
    }

    /// Flushing a sealed bucket must not advance a shard's cursor past entries
    /// belonging to a still-open bucket: entries from different event-time
    /// buckets interleave in arrival order, so a count-based FIFO advance
    /// over-consumes and a later crash loses the open bucket's acked rows.
    ///
    /// Arrival order (shards round-robin per topic, 4 shards):
    ///   i0: CURRENT-bucket row → shard 0   (stays open, must survive crash)
    ///   i1–i3: old-bucket rows → shards 1–3
    ///   i4: old-bucket row     → shard 0   (behind i0 on the same shard)
    /// Flushing the old bucket must NOT move shard 0's cursor past i0.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_advance_must_not_consume_open_bucket_entries() {
        let (_dir, cfg, project, table) = test_ids_env("wm");

        let now = crate::support::now_micros();
        let old = now - 2 * crate::write::mem_buffer::bucket_duration_micros();
        let row = |id: &str, ts: i64| span_batch(id, id, &project, ts);

        {
            let layer = layer_with(Arc::clone(&cfg), noop_delta());

            layer.insert(&project, &table, vec![row("live", now)]).await.unwrap(); // i0 → shard 0
            for k in 1..=3 {
                layer.insert(&project, &table, vec![row(&format!("old{k}"), old)]).await.unwrap(); // shards 1–3
            }
            layer.insert(&project, &table, vec![row("old0", old)]).await.unwrap(); // i4 → shard 0

            // Flush sealed buckets only; the "live" row's bucket stays open.
            layer.flush_completed_buckets().await.unwrap();
            // Crash: drop without shutdown — no clean-shutdown cursor snapshot.
        }

        let ids = recovered_col(cfg, &project, &table, "id").await;
        assert!(ids.contains(&"live".to_string()), "acked open-bucket row lost across crash: WAL cursor advanced past its entry (got rows {ids:?})");
    }

    /// A DELETE racing an airborne commit must stick: the commit lands
    /// pre-delete row values, so `finish_flushed_snapshot` must judge the
    /// bucket dirty (keep the post-delete state, no drain), and the deleted
    /// rows must stay gone across a crash + replay.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn delete_during_airborne_commit_sticks_across_crash() {
        let (_dir, cfg, project, table) = test_ids_env("dd");

        let old_ts = crate::support::now_micros() - 2 * crate::write::mem_buffer::bucket_duration_micros();
        let row = |id: &str| span_batch(id, id, &project, old_ts);
        // Utf8View literal — the buffered `id` column is Utf8View and Arrow's
        // eq kernel rejects mixed Utf8View/Utf8 comparisons.
        let pred = datafusion::prelude::col("id").eq(datafusion::logical_expr::lit(datafusion::common::ScalarValue::Utf8View(Some("doomed".into()))));

        {
            let (entered, release, cb) = parked_delta();
            let layer = layer_with(Arc::clone(&cfg), cb);

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

            // Dirty finish must keep the post-delete state, not drain the
            // shifted prefix (which would drop 'keeper').
            let ids = crate::support::test_helpers::query_col_strings(&layer, &project, &table, "id");
            assert_eq!(ids, vec!["keeper".to_string()], "post-delete state must survive the dirty finish (got {ids:?})");
        }

        let ids = recovered_col(cfg, &project, &table, "id").await;
        assert!(!ids.contains(&"doomed".to_string()), "acked DELETE resurrected after crash+replay (got {ids:?})");
        assert!(ids.contains(&"keeper".to_string()), "surviving row lost across crash (got {ids:?})");
    }

    /// Sealed rows must stay queryable while their Delta commit is airborne —
    /// a take-based flush would black out that window for the commit's duration.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn sealed_rows_stay_queryable_during_flush_commit() {
        let (_dir, cfg, project, table) = test_ids_env("vz");

        let (entered, release, cb) = parked_delta();
        let layer = layer_with(Arc::clone(&cfg), cb);

        let old_ts = crate::support::now_micros() - 2 * crate::write::mem_buffer::bucket_duration_micros();
        layer.insert(&project, &table, vec![span_batch("v1", "spanV", &project, old_ts)]).await.unwrap();

        let entered_wait = entered.notified();
        let flusher = {
            let layer = layer.clone();
            tokio::spawn(async move { layer.flush_completed_buckets().await })
        };
        entered_wait.await; // commit is airborne now

        assert_eq!(rows_in(&layer, &project, &table), 1, "sealed rows must remain queryable while the Delta commit is in flight");

        release.add_permits(1);
        flusher.await.unwrap().unwrap();
        assert!(layer.is_empty(), "flushed prefix must drain from MemBuffer after the commit lands");
    }

    /// Corruption at/over the threshold must quarantine the payloads and let
    /// recovery come up (on both boots), never bail and rewind to P0 forever.
    #[serial]
    #[tokio::test]
    async fn corruption_threshold_boots_instead_of_crash_looping() {
        let (_dir, cfg, project, table) = test_env_with("cr", |c| c.buffer.timefusion_wal_corruption_threshold = 1);

        {
            let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();
            layer.wal().append_raw_for_test(&project, &table, b"WAL2\x80garbage-not-bincode").unwrap();
            // Crash without shutdown.
        }

        for boot in 0..2 {
            let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
            let recovered = layer.recover_from_wal().await;
            let stats = recovered.unwrap_or_else(|e| panic!("boot {boot} must survive over-threshold corruption (quarantined payloads), got: {e}"));
            // Only boot 0 sees the corrupt entry: the corrupt-only shard holds
            // no live bucket, so replay parks past it and never re-reads it.
            if boot == 0 {
                assert!(stats.corrupted_entries_skipped >= 1, "boot 0: corruption must be counted, got {stats:?}");
            }
            assert_eq!(rows_in(&layer, &project, &table), 3, "boot {boot}: healthy entries must still replay");
        }
    }

    /// A unique (project, table) pair tagged with `prefix`. Kept short: walrus
    /// caps a topic's metadata at 62 bytes.
    fn test_ids(prefix: &str) -> (String, String) {
        let id = &uuid::Uuid::new_v4().to_string()[..4];
        (format!("{prefix}{id}"), format!("{prefix}{id}"))
    }

    /// tempdir + config + a unique prefixed (project, table). The `TempDir`
    /// must stay bound for the test's lifetime (bind as `_dir`, never `_`):
    /// dropping it deletes the layer's data dir out from under the test.
    fn test_env_with(prefix: &str, tweak: impl FnOnce(&mut AppConfig)) -> (TempDir, Arc<AppConfig>, String, String) {
        let dir = tempdir().unwrap();
        let cfg = test_config_with(dir.path().to_path_buf(), tweak);
        let (project, table) = test_ids(prefix);
        (dir, cfg, project, table)
    }

    /// [`test_env_with`] on the default (dwell-off) config.
    fn test_ids_env(prefix: &str) -> (TempDir, Arc<AppConfig>, String, String) {
        test_env_with(prefix, |c| c.buffer.timefusion_flush_dwell_secs = 0)
    }

    /// [`test_env_with`] plus `n` co-tenant projects sharing the one table.
    fn cotenant_env(prefix: &str, n: usize, tweak: impl FnOnce(&mut AppConfig)) -> (TempDir, Arc<AppConfig>, String, Vec<String>) {
        let (dir, cfg, table, _) = test_env_with(prefix, tweak);
        let projects = (0..n).map(|i| format!("{table}{i}")).collect();
        (dir, cfg, table, projects)
    }

    /// Rows currently visible for `(project, table)`.
    fn rows_in(layer: &BufferedWriteLayer, project: &str, table: &str) -> usize {
        layer.query(project, table, &[]).unwrap().iter().map(|b| b.num_rows()).sum()
    }

    /// Boot a fresh layer on `cfg`, replay the WAL, and read one column back —
    /// the post-crash half of the crash+recover tests.
    async fn recovered_col(cfg: Arc<AppConfig>, project: &str, table: &str, col: &str) -> Vec<String> {
        let layer = Arc::new(test_layer(cfg).unwrap());
        layer.recover_from_wal().await.unwrap();
        query_col_strings(&layer, project, table, col)
    }

    /// Ack one [`create_test_batch`] into the WAL and drop the layer without
    /// flushing: the rows exist only in the WAL, the unclean-exit shape.
    async fn ack_then_crash(cfg: &Arc<AppConfig>, project: &str, table: &str) {
        let layer = test_layer(Arc::clone(cfg)).unwrap();
        layer.insert(project, table, vec![create_test_batch(project)]).await.unwrap();
    }

    /// A one-row batch for `project`, stamped at `ts`.
    fn span_batch(id: &str, name: &str, project: &str, ts: i64) -> RecordBatch {
        json_to_batch(vec![crate::support::test_helpers::test_span_ts(id, name, project, ts)]).unwrap()
    }

    /// A Delta callback that signals `entered` once a commit is airborne and
    /// parks until `release` hands out a permit, so a test can observe the
    /// mid-commit state.
    fn parked_delta() -> (Arc<Notify>, Arc<tokio::sync::Semaphore>, DeltaWriteCallback) {
        let (entered, release) = (Arc::new(Notify::new()), Arc::new(tokio::sync::Semaphore::new(0)));
        let (entered_cb, release_cb) = (entered.clone(), release.clone());
        let cb: DeltaWriteCallback = Arc::new(move |_p, _t, _b, _wm| {
            let (entered, release) = (entered_cb.clone(), release_cb.clone());
            Box::pin(async move {
                entered.notify_one();
                let _ = release.acquire().await;
                Ok(Vec::new())
            })
        });
        (entered, release, cb)
    }

    /// An `Arc`d layer whose Delta commits go through `cb`.
    fn layer_with(cfg: Arc<AppConfig>, cb: DeltaWriteCallback) -> Arc<BufferedWriteLayer> {
        let mut layer = crate::support::test_helpers::test_layer(cfg).unwrap();
        layer.delta_write_callback = Some(cb);
        Arc::new(layer)
    }

    /// A succeeding Delta write callback that tallies commits (and, unlike
    /// `counting_delta`, reports no written files).
    fn tally_delta(calls: Arc<AtomicUsize>) -> DeltaWriteCallback {
        Arc::new(move |_p: String, _t: String, _b: Vec<RecordBatch>, _w: DeltaWatermark| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            })
        })
    }

    /// Shut the layer down, returning how long that took and the snapshot it
    /// persisted.
    async fn shutdown_snap(layer: &Arc<BufferedWriteLayer>) -> (std::time::Duration, crate::write::wal::CursorSnapshot) {
        let started = std::time::Instant::now();
        layer.shutdown().await.unwrap();
        let elapsed = started.elapsed();
        (elapsed, layer.wal().load_cursor_snapshot().expect("shutdown must persist a cursor snapshot (dirty ones still record conservative cursors)"))
    }

    /// A DML entry's shard must stay pinned while the buckets it mutated are
    /// unflushed: DML entries land on their own round-robin shard, which the
    /// buckets' insert holds don't cover, so without a topic-wide pin any
    /// unrelated flush advances that shard to tail and a crash reverts the
    /// acked UPDATE.
    /// Arrival: i0 current-bucket insert (shard 0), i1 old-bucket insert
    /// (shard 1), UPDATE (shard 2). Flush the old bucket, crash, recover:
    /// the current bucket's rows must still carry the update.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn dml_entry_survives_unrelated_flush_and_crash() {
        let (_dir, cfg, project, table) = test_ids_env("dm");

        let now = crate::support::now_micros();
        let old = now - 2 * crate::write::mem_buffer::bucket_duration_micros();
        let row = |id: &str, ts: i64| span_batch(id, id, &project, ts);
        let assignments = vec![("name".to_string(), datafusion::logical_expr::lit("renamed"))];

        {
            let layer = layer_with(Arc::clone(&cfg), noop_delta());

            layer.insert(&project, &table, vec![row("live", now)]).await.unwrap(); // shard 0
            layer.insert(&project, &table, vec![row("old", old)]).await.unwrap(); // shard 1
            let updated = layer.update(&project, &table, None, &assignments).unwrap(); // shard 2
            assert_eq!(updated, 2);

            layer.flush_completed_buckets().await.unwrap(); // flushes the old bucket only
            // Crash: drop without shutdown.
        }

        let names = recovered_col(cfg, &project, &table, "name").await;
        assert!(!names.is_empty(), "expected rows after WAL recovery");
        assert!(names.iter().all(|n| n == "renamed"), "acked UPDATE reverted: its WAL entry was drained by an unrelated flush (got {names:?})");
    }

    /// A DML predicate carrying a timestamp literal must round-trip through the
    /// WAL as parseable SQL — `Expr`'s Display form is not, so replay would
    /// fail planning and quarantine the UPDATE.
    #[serial]
    #[tokio::test]
    async fn update_with_timestamp_predicate_replays_after_restart() {
        let (_dir, cfg, project, table) = test_ids_env("tp");

        let cutoff = crate::support::now_micros() - 3_600_000_000; // 1h ago — matches all rows
        let pred = datafusion::prelude::col("timestamp")
            .gt_eq(datafusion::logical_expr::lit(datafusion::common::ScalarValue::TimestampMicrosecond(Some(cutoff), Some("UTC".into()))));
        let assignments = vec![("name".to_string(), datafusion::logical_expr::lit("renamed"))];

        {
            let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();
            let updated = layer.update(&project, &table, Some(&pred), &assignments).unwrap();
            assert_eq!(updated, 3, "pre-restart update should hit all rows");
        }

        let names = recovered_col(cfg, &project, &table, "name").await;
        assert!(!names.is_empty(), "expected rows after WAL recovery");
        assert!(names.iter().all(|n| n == "renamed"), "WAL replay dropped the UPDATE — timestamp-literal predicate failed to parse on replay (got {names:?})");
    }

    /// Shutdown must finish within its budget AND persist a `clean_shutdown=true`
    /// snapshot even when the Delta flush can't keep up — otherwise the next boot
    /// pays `delta_cursor_reconcile` plus a full blocking replay.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_writes_clean_snapshot_under_deadline() {
        let (_dir, cfg, project, table) = test_env_with("s", |c| c.buffer.timefusion_stop_grace_secs = 1);

        // Delta callback that blocks far longer than the shutdown budget.
        let layer = layer_with(
            Arc::clone(&cfg),
            Arc::new(move |_p, _t, _b, _wm| {
                Box::pin(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    Ok(Vec::new())
                })
            }),
        );

        let old_ts = crate::support::now_micros() - 2 * crate::write::mem_buffer::bucket_duration_micros();
        layer.insert(&project, &table, vec![span_batch("x", "spanX", &project, old_ts)]).await.unwrap();

        let (elapsed, snap) = shutdown_snap(&layer).await;
        assert!(elapsed < std::time::Duration::from_secs(10), "shutdown must be deadline-bounded, took {elapsed:?}");
        assert!(snap.clean_shutdown, "shutdown must mark clean_shutdown=true even on a partial flush");
        // clean ≠ drained: the hung flush left the bucket WAL-only, so this
        // snapshot must NOT authorize the next boot's pure-mtime WAL sweep.
        assert!(!snap.drained, "a partial-flush shutdown must not claim drained (boot GC would eat the backlog)");
        assert!(!layer.is_drained(), "a timed-out flush must remain visibly undrained");
    }

    /// Counterpart: a shutdown whose flush fully drains MUST claim drained, so
    /// the next boot's pre-walrus GC still runs on the healthy path.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_claims_drained_when_flush_completes() {
        let (_dir, cfg, project, table) = test_ids_env("d");

        let layer = layer_with(Arc::clone(&cfg), noop_delta());
        layer.insert(&project, &table, vec![json_to_batch(vec![test_span("x", "spanX", &project)]).unwrap()]).await.unwrap();

        let (_, snap) = shutdown_snap(&layer).await;
        assert!(snap.clean_shutdown && snap.drained, "a fully-drained shutdown must claim drained=true");
        assert!(layer.is_drained(), "a successful flush must report drained");
    }

    /// Live ingestion can follow a pre-deploy FLUSH before SIGTERM arrives; the
    /// admission fence makes that tail finite and planned shutdown must flush
    /// it rather than turn it into boot replay.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_flushes_post_predeploy_flush_tail() {
        let (_dir, cfg, project, _) = test_env_with("pd", |c| c.buffer.timefusion_stop_grace_secs = 70);
        let flush_calls = Arc::new(AtomicUsize::new(0));
        let layer = layer_with(Arc::clone(&cfg), tally_delta(Arc::clone(&flush_calls)));
        let batch = json_to_batch(vec![test_span("tail", "span", &project)]).unwrap();
        layer.insert(&project, "otel_logs_and_spans", vec![batch]).await.unwrap();
        let (_, snap) = shutdown_snap(&layer).await;
        assert_eq!(flush_calls.load(Ordering::SeqCst), 1, "shutdown must flush the finite post-FLUSH tail");
        assert!(snap.clean_shutdown && snap.drained, "successful final flush must authorize a drained boot");
        assert!(layer.wal().is_fully_consumed().unwrap(), "replacement must not replay the flushed tail");
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn deploy_handoff_fences_writes_and_makes_shutdown_constant_time() {
        let (_dir, cfg, ..) = test_env_with("hf", |c| c.buffer.timefusion_stop_grace_secs = 70);
        let layer = layer_with(Arc::clone(&cfg), noop_delta());
        let batch = json_to_batch(vec![test_span("handoff", "span", "p")]).unwrap();
        layer.insert("p", "otel_logs_and_spans", vec![batch.clone()]).await.unwrap();

        let stats = layer.prepare_deploy_handoff().await.unwrap();
        assert_eq!(stats.buckets_failed, 0);
        assert!(layer.is_drained());
        assert!(layer.is_deploy_handoff_ready(), "drained write fence must authorize start-first ownership request");
        let err = layer.insert("p", "otel_logs_and_spans", vec![batch]).await.unwrap_err();
        assert!(err.to_string().contains("draining for deployment"));

        let (elapsed, snap) = shutdown_snap(&layer).await;
        assert!(!layer.is_deploy_handoff_ready(), "shutdown must invalidate the leased takeover authorization");
        assert!(elapsed < Duration::from_secs(1), "drained handoff shutdown must stay constant-time");
        assert!(snap.clean_shutdown && snap.drained);
    }

    #[test_case::test_case(false ; "cancelled handoff reopens admission")]
    #[test_case::test_case(true ; "shutdown fence keeps admission closed")]
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn cancelled_deploy_handoff_restores_admission_unless_shutdown_fenced_it(shutting_down: bool) {
        let (_dir, cfg, ..) = test_ids_env("ch");
        let layer = Arc::new(test_layer(cfg).unwrap());
        let _flush = layer.flush_lock.lock().await;
        let mut handoff = Box::pin(layer.prepare_deploy_handoff());
        assert!(futures::poll!(handoff.as_mut()).is_pending());
        assert!(layer.admit_write().is_err());
        if shutting_down {
            layer.stop_accepting_writes();
        }
        drop(handoff);
        assert_eq!(layer.admit_write().is_ok(), !shutting_down);
        assert!(!layer.is_deploy_handoff_ready());
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_deploy_handoff_reopens_write_admission() {
        let (_dir, cfg, ..) = test_ids_env("fh");
        let layer = layer_with(Arc::clone(&cfg), Arc::new(|_p, _t, _b, _wm| Box::pin(async { anyhow::bail!("injected Delta failure") })));
        let batch = json_to_batch(vec![test_span("handoff", "span", "p")]).unwrap();
        layer.insert("p", "otel_logs_and_spans", vec![batch.clone()]).await.unwrap();

        assert!(layer.prepare_deploy_handoff().await.is_err());
        assert!(!layer.is_deploy_handoff_ready());
        layer.insert("p", "otel_logs_and_spans", vec![batch]).await.expect("failed handoff must reopen writes");
    }

    /// A drainable WAL tail must still be drained (and a drained snapshot
    /// persisted) even when a background worker ignores cancellation.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn wedged_background_task_does_not_starve_shutdown_flush() {
        let (_dir, cfg, project, table) = test_env_with("bg", |c| c.buffer.timefusion_stop_grace_secs = 1);

        let layer = layer_with(Arc::clone(&cfg), noop_delta());
        layer.background_tasks.lock().await.push(tokio::spawn(async {
            // Deliberately ignores the layer's cancellation token.
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }));

        layer.insert(&project, &table, vec![json_to_batch(vec![test_span("x", "spanX", &project)]).unwrap()]).await.unwrap();

        let (_, snap) = shutdown_snap(&layer).await;
        assert!(snap.clean_shutdown && snap.drained, "the drainable WAL tail must not be left for startup behind a wedged worker");
        assert!(layer.is_drained());
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_write_fence_rejects_late_appends_and_marks_active_writer_dirty() {
        let (_dir, cfg, ..) = test_env_with("wf", |c| c.buffer.timefusion_stop_grace_secs = 1);
        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());

        // A writer admitted before the fence: the barrier must refuse a clean
        // claim it could invalidate.
        let active = layer.admit_write().unwrap();
        layer.stop_accepting_writes();
        let batch = json_to_batch(vec![test_span("late", "span", "p")]).unwrap();
        let err = layer.insert("p", "otel_logs_and_spans", vec![batch]).await.unwrap_err();
        assert!(err.to_string().contains("draining for deployment"));

        let (_, snap) = shutdown_snap(&layer).await;
        assert!(!snap.clean_shutdown && !snap.drained, "an active pre-fence writer forbids clean/drained claims");
        drop(active);
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn already_drained_shutdown_caps_wedged_worker_handoff_to_250ms() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let (_dir, cfg, ..) = test_env_with("dw", |c| c.buffer.timefusion_stop_grace_secs = 70);
        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
        let worker_dropped = Arc::new(AtomicBool::new(false));
        let dropped = Arc::clone(&worker_dropped);
        let worker_started = Arc::new(Notify::new());
        let started_signal = Arc::clone(&worker_started);
        layer.background_tasks.lock().await.push(tokio::spawn(async move {
            let _drop_probe = Dropped(dropped);
            started_signal.notify_one();
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }));
        worker_started.notified().await;

        let (elapsed, snap) = shutdown_snap(&layer).await;
        assert!(elapsed < std::time::Duration::from_secs(1), "an already-drained deploy handoff must not spend 7s on a wedged worker");
        assert!(worker_dropped.load(Ordering::Acquire), "timed-out worker must be aborted and joined before the WAL lock can pass to the replacement");
        assert!(snap.clean_shutdown && snap.drained);
    }

    /// An orphan (failed commit whose rows couldn't be restored) must be
    /// included in the recovery-parking holds (otherwise the cursor parks past
    /// its WAL-only rows and they are lost) and must pin the WAL GC floor.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn orphaned_holds_park_recovery_cursor_and_pin_gc_floor() {
        let (_dir, cfg, ..) = test_ids_env("oh");
        let layer = crate::support::test_helpers::test_layer(cfg).unwrap();
        let shards = layer.wal.shards_per_topic();

        let pos = walrus_rust::WalPosition::ORIGIN;
        let mut holds: ShardHolds = vec![None; shards];
        holds[0] = Some(pos);
        let pin = chrono::Utc::now().timestamp_micros() - 3600 * 1_000_000;
        let token = layer.register_inflight_holds("op", "ot", holds.clone());
        layer.orphan_inflight_holds("op", "ot", token, holds, pin);

        let parked = layer.recovery_parking_holds("op", "ot", shards);
        assert_eq!(parked[0], Some(pos), "recovery parking must include orphaned holds");
        // The per-topic GC floor must survive the token release.
        assert_eq!(layer.oldest_unflushed_wal_append_micros(), Some(pin), "orphan must pin the WAL GC floor");
        let stats = layer.snapshot_stats();
        assert_eq!(stats.orphaned_topics, 1, "orphan must be visible in stats");
        assert!(stats.orphan_pin_age_secs.unwrap_or(0) >= 3599, "orphan pin age must be surfaced");
    }

    /// Replaying an UPDATE...FROM whose table has no buffered rows must consume
    /// the entry as a no-op, not quarantine it against an empty schema.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_replays_dml_for_flushed_table_without_quarantine() {
        use arrow::{
            array::{Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use datafusion::logical_expr::col;
        let (_dir, cfg, project, table) = test_ids_env("u");

        let src_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("new_name", DataType::Utf8, false)])),
            vec![Arc::new(Int64Array::from(vec![1i64])), Arc::new(StringArray::from(vec!["x"]))],
        )
        .unwrap();
        let source = crate::dml::UpdateSource { schema: src_batch.schema(), batch: src_batch, join_keys: vec![("id".to_string(), "id".to_string())] };

        {
            let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            // Untracked table: the live leg no-ops but the WAL entry is still written.
            let pred = col("context___span_id").is_not_null();
            let assigns = [("name".to_string(), col("new_name"))];
            let n = layer.update_with_source(&project, &table, Some(&pred), &assigns, &source).unwrap();
            assert_eq!(n, 0);
        }

        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
        layer.recover_from_wal().await.expect("recovery must succeed");
        let quarantined = std::fs::read_dir(cfg.core.wal_dir().join("quarantine")).map(|d| d.count()).unwrap_or(0);
        assert_eq!(quarantined, 0, "DML replay for a flushed/untracked table must no-op, not quarantine");
    }

    /// Post-commit bookkeeping must settle per group inside the shutdown flush
    /// stream: a deadline drop may lose in-flight groups but never the
    /// drain/hold-release/cursor-advance of a commit that already landed.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_deadline_preserves_landed_commits_bookkeeping() {
        let (_dir, cfg, table, _) = test_env_with("t", |c| c.buffer.timefusion_stop_grace_secs = 1);
        let (fast, slow) = (format!("f{table}"), format!("w{table}"));

        let slow_p = slow.clone();
        let layer = layer_with(
            Arc::clone(&cfg),
            Arc::new(move |p, _t, _b, _wm| {
                let hang = p == slow_p;
                Box::pin(async move {
                    if hang {
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    }
                    Ok(Vec::new())
                })
            }),
        );

        let old_ts = sealed_ts();
        // The fast table gets more rows so largest-first ordering flushes it
        // first even at flush_parallelism = 1.
        let mk = |id: &str, span: &str, p: &str| crate::support::test_helpers::test_span_ts(id, span, p, old_ts);
        layer.insert(&fast, &table, vec![json_to_batch(vec![mk("a", "s1", &fast), mk("b", "s2", &fast)]).unwrap()]).await.unwrap();
        layer.insert(&slow, &table, vec![json_to_batch(vec![mk("c", "s3", &slow)]).unwrap()]).await.unwrap();

        layer.shutdown().await.unwrap();

        let stats = layer.snapshot_stats();
        assert_eq!(stats.mem_total_rows, 1, "landed commit's buckets must drain despite the deadline drop; only the hung table's row may remain");
    }

    /// A force-flushed open bucket must stay exempt from the Delta-scan
    /// exclusion for its whole surviving lifetime: a late arrival re-narrows
    /// the bucket's range over the committed rows and would mask them.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn flush_all_now_exempts_surviving_open_bucket_from_exclusion() {
        let (_dir, cfg, project, table) = test_ids_env("o");

        // Freeze the clock so the bucket stays open across the flush. Unfreeze
        // even on panic — a leaked frozen clock breaks unrelated tests.
        struct Unfreeze;
        impl Drop for Unfreeze {
            fn drop(&mut self) {
                crate::support::unfreeze();
            }
        }
        let _uf = Unfreeze;
        let t0 = chrono::Utc::now().timestamp_micros();
        crate::support::set_micros(t0);

        let (entered, release, cb) = parked_delta();
        let layer = layer_with(Arc::clone(&cfg), cb);

        layer.insert(&project, &table, vec![span_batch("a", "s1", &project, t0)]).await.unwrap();

        let entered_wait = entered.notified();
        let l2 = Arc::clone(&layer);
        let flush = tokio::spawn(async move { l2.flush_all_now().await });
        entered_wait.await; // snapshot taken, commit airborne
        // Late arrival into the SAME open window: it survives the drain and
        // keeps the bucket alive.
        layer.insert(&project, &table, vec![span_batch("b", "s2", &project, t0 + 1_000)]).await.unwrap();
        release.add_permits(1);
        flush.await.unwrap().unwrap();

        // Seal the window: the surviving bucket is no longer `current`.
        crate::support::set_micros(t0 + 2 * crate::write::mem_buffer::bucket_duration_micros());
        let ranges = layer.mem_buffer.get_bucket_ranges(&project, &table);
        assert!(ranges.is_empty(), "force-flushed open bucket must stay exempt from the Delta-scan exclusion (it would mask the committed rows): {ranges:?}");
    }

    /// A failed quarantine WRITE means the WAL is the entry's only copy: replay
    /// must keep the rewind marker and bail, regardless of the frame-error
    /// corruption threshold (the two counters are disjoint).
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_bails_when_quarantine_write_fails() {
        use arrow::{
            array::{ArrayRef, Int64Array, StringViewArray, TimestampMicrosecondArray},
            datatypes::{DataType, Field, Schema, TimeUnit},
        };
        let (_dir, cfg, project, table) = test_ids_env("q");

        let ts = crate::support::now_micros();
        let ts_col = Arc::new(TimestampMicrosecondArray::from(vec![ts]).with_timezone("UTC")) as ArrayRef;
        let with_id = |dt: DataType, ids: ArrayRef| {
            let schema =
                Schema::new(vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false), Field::new("id", dt, false)]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::clone(&ts_col), ids]).unwrap()
        };
        let batch_int = with_id(DataType::Int64, Arc::new(Int64Array::from(vec![1])));
        let batch_str = with_id(DataType::Utf8View, Arc::new(StringViewArray::from(vec!["x"])));

        {
            let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![batch_int]).await.unwrap();
            // Incompatible column type: the WAL append succeeds, the MemBuffer
            // apply fails — the entry shape replay quarantines.
            assert!(layer.insert(&project, &table, vec![batch_str]).await.is_err());
        }

        // Make the quarantine dir read-only so the quarantine file write
        // fails (a plain blocking file would be scanned by walrus's boot
        // dir walk and trip its debug asserts — dirs are skipped).
        let qdir = cfg.core.wal_dir().join("quarantine");
        std::fs::create_dir_all(&qdir).unwrap();
        let mut ro = std::fs::metadata(&qdir).unwrap().permissions();
        #[cfg(unix)]
        std::os::unix::fs::PermissionsExt::set_mode(&mut ro, 0o555);
        std::fs::set_permissions(&qdir, ro.clone()).unwrap();

        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
        let res = layer.recover_from_wal().await;
        // Restore perms before asserting so tempdir cleanup works either way.
        #[cfg(unix)]
        std::os::unix::fs::PermissionsExt::set_mode(&mut ro, 0o755);
        std::fs::set_permissions(&qdir, ro).unwrap();
        let err = res.expect_err("recovery must bail when a quarantine write fails — the WAL is the only copy");
        assert!(err.to_string().contains("quarantine"), "unexpected error: {err}");
        assert!(
            crate::write::wal::meta_path(&cfg.core.wal_dir(), "recovery_rewind.json").exists(),
            "rewind marker must survive the bail so the next boot re-reads the un-preserved entries"
        );
    }

    /// An insert stamps `updated_at` BEFORE the WAL append (so replay reproduces
    /// the value rather than re-issuing one), and replay folds the replayed
    /// maximum back into the clock so the first post-boot stamp exceeds
    /// everything durable even after a backwards wall-clock step.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn version_stamp_is_durable_and_seeds_the_clock_at_boot() {
        let dir = tempdir().unwrap();
        let cfg = create_test_config(dir.path().to_path_buf());
        // `mor_versioned` — a table whose tiebreak TF owns. The WAL/MemBuffer
        // legs are schema-free, so an otel-shaped batch is fine.
        let table = "mor_versioned";
        let project = format!("v{}", &uuid::Uuid::new_v4().to_string()[..4]);

        // Far-future clock so the value can't be confused with one issued after
        // the simulated reboot.
        crate::support::set_micros(4_000_000_000_000_000);
        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
        let batch = span_batch("a", "s1", &project, crate::support::now_micros());
        // Stamping normally happens one level up, in `Database::insert_records_batch`.
        let batches = crate::write::stamp_version(table, vec![batch]);
        layer.insert(&project, table, batches).await.unwrap();
        let stamped = crate::support::test_helpers::query_col_strings(&layer, &project, table, "updated_at");
        assert_eq!(stamped.len(), 1);
        assert!(!stamped[0].is_empty(), "a row inserted without updated_at must come back with one");
        drop(layer);

        // Reboot: clock steps back an epoch and the in-memory clock is gone.
        crate::support::set_micros(3_000_000_000_000_000);
        crate::write::reset_stamp_state(table);
        let replayed = recovered_col(cfg, &project, table, "updated_at").await;
        assert_eq!(replayed, stamped, "replay must reproduce the durable stamp, not re-issue one");
        let next = crate::write::next_stamp(table);
        crate::support::unfreeze();
        assert!(next > 4_000_000_000_000_000, "post-boot stamp {next} must exceed everything replayed");
    }

    /// A parked insert payload with a valid Arrow IPC body must go back through
    /// the durable insert path and be archived under `redriven/`; a torn-tail
    /// payload stays parked and keeps the alert count non-zero.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn boot_redrives_quarantined_insert_payloads() {
        let (_dir, cfg, project, table) = test_ids_env("r");

        let batch = span_batch("a", "s1", &project, crate::support::now_micros());
        let data = crate::write::wal::serialize_record_batch(&batch).unwrap();

        // Build the layer FIRST: walrus's boot dir scan reads any pre-existing
        // top-level file as a WAL segment.
        let layer = Arc::new(crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap());
        let wal_dir = cfg.core.wal_dir();
        let qdir = wal_dir.join("quarantine");
        std::fs::create_dir_all(&qdir).unwrap();
        let meta = |kind: &str| format!("ts_micros=1\nproject_id={project}\ntable_name={table}\noperation=Insert\nkind={kind}\nreason=x\nbytes=0\n");
        std::fs::write(qdir.join("1_insert_incompatible_t.bin"), &data).unwrap();
        std::fs::write(qdir.join("1_insert_incompatible_t.meta"), meta("insert_incompatible")).unwrap();
        std::fs::write(qdir.join("2_insert_corrupt_t.bin"), &data[..data.len() / 2]).unwrap();
        std::fs::write(qdir.join("2_insert_corrupt_t.meta"), meta("insert_corrupt")).unwrap();
        layer.recover_from_wal().await.unwrap();
        assert_eq!(layer.snapshot_stats().mem_total_rows, 0, "quarantine re-drive must not block recovery/readiness");
        layer.start_background_tasks().await;
        // Wait for the ARCHIVE, not the row: the re-drive inserts first and
        // renames into `redriven/` after, so waiting on `mem_total_rows` races
        // the assertions below.
        tokio::time::timeout(Duration::from_secs(10), async {
            while !qdir.join("redriven/1_insert_incompatible_t.meta").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("background quarantine re-drive did not finish");

        assert_eq!(layer.snapshot_stats().mem_total_rows, 1, "re-driven payload's row must be back in the store");
        assert!(qdir.join("redriven/1_insert_incompatible_t.bin").exists(), "re-driven payload archived for forensics");
        assert!(qdir.join("redriven/1_insert_incompatible_t.meta").exists());
        let (files, _) = crate::write::wal::quarantine_stats(&wal_dir);
        assert_eq!(files, 1, "torn-tail payload stays parked and keeps the alert count non-zero");
    }

    /// Source schema `(lookup_name: Utf8, new_id: Utf8)`; the join matches target
    /// `name` against source `lookup_name` and overwrites target `id`.
    fn build_update_source_for_id_rewrite(rows: &[(&str, &str)]) -> (crate::dml::UpdateSource, Vec<(String, datafusion::logical_expr::Expr)>) {
        use std::sync::Arc;

        use arrow::{
            array::{ArrayRef, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use datafusion::prelude::col;

        let lookup_names: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|(n, _)| *n).collect::<Vec<_>>()));
        let new_ids: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|(_, i)| *i).collect::<Vec<_>>()));
        let schema = Arc::new(Schema::new(vec![Field::new("lookup_name", DataType::Utf8, false), Field::new("new_id", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![lookup_names, new_ids]).unwrap();

        let source = crate::dml::UpdateSource { batch, schema, join_keys: vec![("name".to_string(), "lookup_name".to_string())] };
        let assignments = vec![("id".to_string(), col("source.new_id"))];
        (source, assignments)
    }

    /// Every visible `(name, id)` pair for `(project, table)` — the read side of
    /// the `update_with_source` tests. Both columns must be `Utf8`.
    fn name_id_rows(layer: &BufferedWriteLayer, project: &str, table: &str) -> Vec<(String, String)> {
        let results = layer.query(project, table, &[]).unwrap();
        assert!(!results.is_empty(), "expected rows for {project}/{table}");
        let combined = arrow::compute::concat_batches(&results[0].schema(), &results).unwrap();
        let col = |n: &str| {
            combined
                .column(combined.schema().index_of(n).unwrap())
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap_or_else(|| panic!("{n} column should be Utf8"))
                .clone()
        };
        let (names, ids) = (col("name"), col("id"));
        (0..combined.num_rows()).map(|i| (names.value(i).to_string(), ids.value(i).to_string())).collect()
    }

    /// MemBuffer-only `UPDATE ... FROM` via `update_with_source`: matched rows
    /// are rewritten, non-matched rows untouched.
    ///
    /// `#[ignore]`d: MemBuffer stores strings as `Utf8View` while the source
    /// batch is `Utf8`, and Arrow's `RowConverter` requires byte-identical
    /// types, so the join lookup returns 0 matches. Re-enable once that is fixed.
    #[serial]
    #[tokio::test]
    async fn update_with_source_buffered_only() {
        let (_dir, cfg, project, table) = test_ids_env("b");

        let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        // create_test_batch produces three rows with names test1/test2/test3
        // and matching ids span1/span2/span3.
        let (source, assignments) = build_update_source_for_id_rewrite(&[("test1", "rewritten-1"), ("test3", "rewritten-3")]);
        let updated = layer.update_with_source(&project, &table, None, &assignments, &source).unwrap();
        assert_eq!(updated, 2, "expected 2 rows matched by the join");

        for (name, id) in name_id_rows(&layer, &project, &table) {
            match name.as_str() {
                "test1" => assert_eq!(id, "rewritten-1", "test1 row should have new id"),
                "test2" => assert_eq!(id, "span2", "test2 row was not in source; must be unchanged"),
                "test3" => assert_eq!(id, "rewritten-3", "test3 row should have new id"),
                other => panic!("unexpected row name {other}"),
            }
        }
    }

    /// An `UPDATE ... FROM` against MemBuffer-only rows must be reapplied by
    /// `recover_from_wal` after a restart.
    ///
    /// `#[ignore]`d: blocked on the same Utf8/Utf8View `RowConverter` lookup
    /// miss as `update_with_source_buffered_only`.
    #[serial]
    #[tokio::test]
    async fn update_with_source_wal_replay_after_restart() {
        let (_dir, cfg, project, table) = test_ids_env("u");

        // Drop without flushing to Delta so the only durable record is the WAL.
        {
            let layer = crate::support::test_helpers::test_layer(Arc::clone(&cfg)).unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

            let (source, assignments) = build_update_source_for_id_rewrite(&[("test2", "post-replay-2")]);
            let updated = layer.update_with_source(&project, &table, None, &assignments, &source).unwrap();
            assert_eq!(updated, 1, "pre-restart update should affect exactly one row");
        }

        {
            let layer = Arc::new(crate::support::test_helpers::test_layer(cfg).unwrap());
            let stats = layer.recover_from_wal().await.unwrap();
            assert!(stats.entries_replayed >= 2, "expected ≥2 entries replayed (Insert + UpdateWithSource), got {stats:?}");

            let rewritten: Vec<String> = name_id_rows(&layer, &project, &table).into_iter().filter(|(n, _)| n == "test2").map(|(_, id)| id).collect();
            assert_eq!(rewritten, ["post-replay-2"], "WAL replay did not reapply UpdateWithSource — test2's id should be 'post-replay-2'");
        }
    }

    // #[serial] + its own WAL dir: a concurrent test's dropped tempdir otherwise
    // leaves the walrus data dir pointing at a deleted path (ENOENT on append).
    #[serial]
    #[tokio::test]
    async fn test_pressure_pct() {
        let (_dir, layer, ..) = layer_after_insert("p", 1).await;

        let pct = layer.pressure_pct();
        assert!(pct <= 100, "pressure must be bounded 0..=100, got {pct}");
        // Tiny batch on 4GB default budget — should be effectively 0%.
        assert!(pct < 5, "expected ~0% after tiny insert, got {pct}");
    }

    /// An insert must record the shard's pre-append cursor hold: the hold is what
    /// pins the WAL read cursor behind unflushed data.
    #[serial]
    #[tokio::test]
    async fn wal_holds_recorded_on_insert() {
        // 3 batches → 3 WAL entries on one shard for this insert.
        let (_dir, layer, project, table) = layer_after_insert("c", 3).await;

        let holds = layer.mem_buffer.wal_holds(&project, &table, layer.wal.shards_per_topic());
        assert!(holds.iter().any(Option::is_some), "insert must record a pre-append cursor hold on its shard, got {holds:?}");
    }

    /// A layer on its own tempdir with one insert of `batches` [`create_test_batch`]es
    /// applied. Asserts the empty-layer pressure floor on the way through; the
    /// returned `TempDir` must outlive the layer.
    async fn layer_after_insert(prefix: &str, batches: usize) -> (TempDir, BufferedWriteLayer, String, String) {
        let (dir, cfg, project, table) = test_ids_env(prefix);
        let layer = test_layer(cfg).unwrap();
        assert_eq!(layer.pressure_pct(), 0, "empty layer should report 0%");
        layer.insert(&project, &table, vec![create_test_batch(&project); batches]).await.unwrap();
        (dir, layer, project, table)
    }

    /// A timestamp two bucket-durations in the past — i.e. inside a bucket that
    /// is already sealed (completed) and therefore flushable.
    fn sealed_ts() -> i64 {
        crate::support::now_micros() - 2 * crate::write::mem_buffer::bucket_duration_micros()
    }

    /// Flushing a sealed bucket must NOT advance the walrus cursor past entries
    /// belonging to a still-open follow-on bucket (they would be lost on crash).
    #[serial]
    #[tokio::test]
    async fn flush_does_not_consume_open_bucket_wal_entries() {
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial]
        // protects the global.
        let (_dir, cfg, project, table) = test_ids_env("o");

        let delta_calls = Arc::new(AtomicUsize::new(0));
        let layer = layer_with(Arc::clone(&cfg), tally_delta(delta_calls.clone()));

        // "old" rows into a sealed bucket, then "current" rows into the open one.
        layer.insert(&project, &table, vec![span_batch("old", "spanA", &project, sealed_ts())]).await.unwrap();
        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        // Flush only completed (= old) buckets. Open bucket stays in MemBuffer + WAL.
        layer.flush_completed_buckets().await.unwrap();
        assert!(delta_calls.load(Ordering::SeqCst) >= 1, "old bucket should have flushed");

        drop(layer);
        let layer2 = Arc::new(crate::support::test_helpers::test_layer(cfg).unwrap());
        let stats = layer2.recover_from_wal().await.unwrap();
        assert!(stats.entries_replayed >= 1, "open-bucket WAL entry must survive flush of the sealed bucket; replayed={}", stats.entries_replayed);
    }

    /// The seal-time snapshot in `FlushableBucket.wal_positions` must reach the
    /// Delta write callback intact, or the watermark never lands in commit
    /// metadata and recovery silently no-ops.
    #[serial]
    #[tokio::test]
    async fn flush_callback_receives_per_shard_watermark() {
        let (_dir, cfg, project, table) = test_ids_env("w");

        let captured_wm: Arc<std::sync::Mutex<Option<crate::write::DeltaWatermark>>> = Arc::new(std::sync::Mutex::new(None));
        let captured_wm_cb = captured_wm.clone();

        let layer = layer_with(
            Arc::clone(&cfg),
            Arc::new(move |_p, _t, _batches, wm| {
                let captured = captured_wm_cb.clone();
                Box::pin(async move {
                    *captured.lock().unwrap() = Some(wm);
                    Ok(Vec::new())
                })
            }),
        );

        // Insert into a sealed (past-cutoff) bucket so flush_completed_buckets picks it up.
        layer.insert(&project, &table, vec![span_batch("seal", "spanA", &project, sealed_ts())]).await.unwrap();

        layer.flush_completed_buckets().await.unwrap();

        let wm = captured_wm.lock().unwrap().clone().expect("callback must have been invoked with a watermark");
        assert_eq!(wm.len(), layer.wal().shards_per_topic(), "watermark must have one entry per shard");
        // The metadata watermark is CONSERVATIVE — it includes this commit's own
        // holds and must never sit past the commit's own entries, so a boot-time
        // Delta-derived cursor cannot skip them. Here the only hold is ORIGIN.
        assert!(
            wm.iter().all(|p| p.is_none() || p.is_some_and(|p| p.is_origin())),
            "conservative metadata watermark must not pass the commit's own entries; got {:?}",
            wm
        );
    }

    /// An insert that crosses the memory hard limit must apply backpressure —
    /// synchronously flush MemBuffer → Delta and retry — never drop the write.
    #[serial]
    #[tokio::test]
    async fn backpressure_flushes_instead_of_rejecting() {
        let (project, table) = test_ids("bp");

        let flush_calls = Arc::new(AtomicUsize::new(0));
        // Old timestamp → all rows land in a completed (flushable) bucket.
        let (layer, _dir, make_batch) = over_limit_layer(tally_delta(flush_calls.clone()), true, |_| {});

        // ~96MB cumulative into a ~76.8MB buffer: at least one insert crosses the
        // hard limit and relies on backpressure. All must succeed.
        for i in 0..8 {
            layer.insert(&project, &table, vec![make_batch()]).await.unwrap_or_else(|e| panic!("insert {i} must succeed under backpressure, got: {e}"));
        }

        assert!(flush_calls.load(Ordering::SeqCst) >= 1, "backpressure must have forced at least one Delta flush");
        assert!(layer.snapshot_stats().backpressure_engaged_total >= 1, "backpressure_engaged_total must record the over-limit event");
    }

    /// Memory pressure with ONLY current-bucket (never-completed) data: the
    /// relief path must force-flush the open bucket rather than find nothing
    /// flushable and reject the insert.
    #[serial]
    #[tokio::test]
    async fn pressure_flushes_current_bucket() {
        let (project, table) = test_ids("cb");

        // NOW timestamp → every row lands in the current (unsealed) bucket.
        let (layer, _dir, make_batch) = over_limit_layer(noop_delta(), false, |_| {});

        for i in 0..8 {
            layer
                .insert(&project, &table, vec![make_batch()])
                .await
                .unwrap_or_else(|e| panic!("insert {i} with only current-bucket data must succeed under pressure, got: {e}"));
        }
    }

    /// Over-limit ingest rig: a 64MB budget (hard limit ~76.8MB) plus a ~12MB
    /// batch maker, so 8 inserts push ~96MB through a buffer that cannot hold it.
    /// `sealed` dates rows two bucket-durations back (a flushable bucket).
    /// The caller must hold the returned `TempDir` for the test's lifetime.
    fn over_limit_layer(
        cb: DeltaWriteCallback, sealed: bool, tweak: impl FnOnce(&mut AppConfig),
    ) -> (Arc<BufferedWriteLayer>, TempDir, impl Fn() -> RecordBatch) {
        use arrow::{
            array::{StringArray, TimestampMicrosecondArray},
            datatypes::{DataType, Field, Schema, TimeUnit},
        };
        let dir = tempdir().unwrap();
        let cfg = test_config_with(dir.path().to_path_buf(), |c| {
            c.buffer.timefusion_buffer_max_memory_mb = 64; // floor → hard limit ~76.8MB
            tweak(c);
        });
        let layer = layer_with(cfg, cb);
        let ts_micros = if sealed { sealed_ts() } else { crate::support::now_micros() };
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let make_batch = move || {
            let rows = 30_000usize;
            let ts = TimestampMicrosecondArray::from(vec![ts_micros; rows]);
            let payload = StringArray::from(vec!["x".repeat(400); rows]); // ~12MB
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(payload)]).unwrap()
        };
        (layer, dir, make_batch)
    }

    /// Same rig with `backpressure_secs = 0`, so the exhaustion path is
    /// deterministic instead of depending on flush timing.
    fn decouple_test_layer(decouple: bool) -> (Arc<BufferedWriteLayer>, TempDir, impl Fn() -> RecordBatch) {
        over_limit_layer(noop_delta(), true, move |c| {
            c.buffer.timefusion_write_backpressure_secs = 0; // exhaust immediately
            c.buffer.timefusion_wal_admit_decouple = decouple;
        })
    }

    /// Baseline: with the decouple flag OFF and backpressure exhausted, an
    /// over-hard-limit insert is rejected (and never WAL-appended).
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

    /// With the flag ON the same scenario must not drop: inserts are admitted
    /// over-budget (the WAL append is the durability boundary) and retained.
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
        let max = layer.max_memory_bytes();
        assert!(
            layer.effective_memory_bytes() > max,
            "decouple must admit past the hard limit ({}MB), got {}MB",
            max / (1024 * 1024),
            layer.effective_memory_bytes() / (1024 * 1024)
        );
    }

    /// The open bucket is excluded from normal flushing; only
    /// `force_flush_current_buckets` can drain it.
    #[serial]
    #[tokio::test]
    async fn force_flush_current_bucket_drains_open_window() {
        let (_dir, cfg, project, table) = test_ids_env("fc");
        let layer = layer_with(cfg, noop_delta());

        // create_test_batch uses now() timestamps → the current (open) bucket.
        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        layer.flush_completed_buckets().await.unwrap();
        assert!(!layer.is_empty(), "completed-bucket flush must leave the open bucket in MemBuffer");

        layer.force_flush_current_buckets().await.unwrap();
        assert!(layer.is_empty(), "force_flush_current_buckets must drain the open bucket");
    }

    /// Force-flushing the open bucket while an older completed bucket is still
    /// un-flushed must not lose the older bucket across a crash: the position
    /// watermark pins the cursor at the stuck bucket's first entry.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn force_flush_with_stuck_completed_bucket_keeps_it_durable() {
        let (_dir, cfg, project, table) = test_ids_env("g");

        let calls = Arc::new(AtomicUsize::new(0));
        {
            let layer = layer_with(Arc::clone(&cfg), tally_delta(calls.clone()));

            layer.insert(&project, &table, vec![span_batch("old", "spanA", &project, sealed_ts())]).await.unwrap();
            layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

            layer.force_flush_current_buckets().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1, "force-flush must drain the open window even with a stuck completed bucket");
            // Crash: drop without shutdown.
        }

        let ids = recovered_col(cfg, &project, &table, "id").await;
        assert!(ids.contains(&"old".to_string()), "stuck completed bucket must survive force-flush + crash (got {ids:?})");
    }

    /// A stuck completed bucket in ONE tenant must not freeze current-bucket
    /// force-flush for OTHER tenants (the WAL gate is per-topic, not global).
    #[serial]
    #[tokio::test]
    async fn force_flush_isolates_stuck_tenant() {
        let (_dir, cfg, t1, _) = test_ids_env("a");
        let t2 = format!("b{}", &t1[1..]);

        let flushed = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let flushed_cb = flushed.clone();
        let layer = layer_with(
            cfg,
            Arc::new(move |p, _t, _b, _wm| {
                let f = flushed_cb.clone();
                Box::pin(async move {
                    f.lock().unwrap().push(p);
                    Ok(Vec::new())
                })
            }),
        );

        // T1: a sealed bucket left un-flushed. T2: only an open bucket.
        layer.insert(&t1, &t1, vec![span_batch("old", "spanA", &t1, sealed_ts())]).await.unwrap();
        layer.insert(&t2, &t2, vec![create_test_batch(&t2)]).await.unwrap();

        layer.force_flush_current_buckets().await.unwrap();

        let flushed = flushed.lock().unwrap().clone();
        assert!(flushed.contains(&t2), "healthy tenant's open bucket must force-flush despite a stuck tenant; flushed={:?}", flushed);
        assert!(!flushed.contains(&t1), "stuck tenant's completed bucket must stay un-advanced (per-topic WAL gate); flushed={:?}", flushed);
    }

    /// A hung Delta commit pins `flush_lock`, so no relief can free memory. The
    /// `flush_bucket_timeout` watchdog must abort it: the flush returns, rows are
    /// restored to MemBuffer (still durable in the WAL), and the lock releases.
    #[serial]
    #[tokio::test]
    async fn flush_bucket_watchdog_aborts_hung_commit() {
        // 1s flush-bucket timeout trips the watchdog fast
        let (_dir, cfg, project, table) = test_env_with("w", |c| c.buffer.timefusion_flush_bucket_timeout_secs = 1);

        // Callback that never resolves — models a stalled S3/commit-lock wait.
        let layer = layer_with(cfg, Arc::new(move |_p, _t, _b, _wm| Box::pin(std::future::pending())));

        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.unwrap();

        // Outer bound: above the 1s watchdog, well below "forever".
        let res = tokio::time::timeout(Duration::from_secs(10), layer.force_flush_current_buckets()).await;
        assert!(res.is_ok(), "force_flush must return once the flush watchdog trips — it hung waiting on the stalled commit");
        res.unwrap().unwrap();

        assert!(!layer.is_empty(), "a timed-out flush must restore the bucket, not drop it");
    }

    #[serial]
    #[tokio::test]
    async fn test_memory_reservation() {
        let (_dir, layer, ..) = layer_after_insert("m", 1).await;

        // The reservation is handed to MemBuffer on success, so it must read 0.
        assert_eq!(layer.reserved_bytes.load(Ordering::Acquire), 0);
    }

    /// While `wal_hard_backpressure` is set inserts must be rejected, and
    /// accepted again the moment it clears.
    #[tokio::test]
    #[serial_test::serial]
    async fn insert_rejected_while_wal_hard_backpressure_set() {
        let (_dir, cfg, project, table) = test_ids_env("w");
        let layer = crate::support::test_helpers::test_layer(cfg).unwrap();

        layer.wal_hard_backpressure.store(true, Ordering::Relaxed);
        let err = layer.insert(&project, &table, vec![create_test_batch(&project)]).await.expect_err("insert must be rejected under WAL hard backpressure");
        assert!(err.to_string().contains("hard limit"), "unexpected error: {err}");

        layer.wal_hard_backpressure.store(false, Ordering::Relaxed);
        layer.insert(&project, &table, vec![create_test_batch(&project)]).await.expect("insert must succeed once backpressure clears");
    }
}

#[cfg(test)]
mod replay_cost_probe {
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::support::test_helpers::{json_to_batch, test_layer, test_span};

    /// Not an assertion — a measurement. Builds a replay backlog, then times
    /// `recover_from_wal` so its own cost breakdown attributes the wall clock.
    /// Scale with REPLAY_ENTRIES / REPLAY_ROWS; the default is small so
    /// `make test-all` stays fast.
    ///
    ///   WALRUS_QUIET=1 REPLAY_ENTRIES=6000 \
    ///     cargo nextest run --lib replay_cost_probe --no-capture --run-ignored all
    ///
    /// WALRUS_QUIET matters: without it walrus `println!`s per block read and the
    /// measurement becomes ~4x its real cost.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "measurement, not an assertion"]
    async fn measure_replay_cost() {
        // The attribution lives in `recover_from_wal`'s own `info!` breakdown.
        let _ = tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).with_test_writer().try_init();
        let env_num = |k: &str, default: usize| std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default);
        let (entries, rows_per_entry, topics) = (env_num("REPLAY_ENTRIES", 1_000), env_num("REPLAY_ROWS", 50), env_num("REPLAY_TOPICS", 1));

        // Replay consumes cursors, so the corpus is built once into
        // REPLAY_BUILD_DIR and each run replays a COPY; replaying the original in
        // place would make the second run measure an empty log.
        let scratch = tempdir().unwrap();
        let corpus = std::env::var("REPLAY_BUILD_DIR").map_or_else(|_| scratch.path().join("corpus"), std::path::PathBuf::from);

        let mk_cfg = |data_dir: std::path::PathBuf| {
            let mut cfg = crate::config::AppConfig::default();
            cfg.core.timefusion_data_dir = data_dir;
            // Never flush during build or replay: we are timing replay, not IO.
            cfg.buffer.timefusion_flush_interval_secs = 86_400;
            cfg.buffer.timefusion_buffer_max_memory_mb = 64_000;
            Arc::new(cfg)
        };

        if !corpus.join("wal").exists() {
            std::fs::create_dir_all(&corpus).unwrap();
            let layer = Arc::new(test_layer(mk_cfg(corpus.clone())).unwrap());
            let batch = json_to_batch((0..rows_per_entry).map(|i| test_span(&format!("id{i}"), &format!("span{i}"), "probe-project")).collect()).unwrap();
            let t_build = std::time::Instant::now();
            for i in 0..entries {
                // Spread across `topics` (project, table) pairs to exercise the
                // cross-topic path; 1 keeps the original single-topic shape.
                let project = format!("probe-project-{}", i % topics);
                layer.insert(&project, "otel_logs_and_spans", vec![batch.clone()]).await.unwrap();
            }
            let build_ms = t_build.elapsed().as_millis();
            println!(
                "\n== built {entries} entries x {rows_per_entry} rows across {topics} topic(s) in {build_ms}ms; wal dir {}MB ==",
                walkdir_size(&corpus) / (1024 * 1024)
            );
            drop(layer);
        } else {
            println!("\n== reusing corpus at {} ({}MB) ==", corpus.display(), walkdir_size(&corpus) / (1024 * 1024));
        }

        // Replay a COPY so the corpus stays pristine for the next variant.
        let run_dir = scratch.path().join("run");
        copy_dir(&corpus, &run_dir);
        let replayer = Arc::new(test_layer(mk_cfg(run_dir)).unwrap());
        let t = std::time::Instant::now();
        let stats = replayer.recover_from_wal().await.unwrap();
        println!(
            "== REPLAY: {} entries in {}ms ({:.3}ms/entry) ==\n",
            stats.entries_replayed,
            t.elapsed().as_millis(),
            t.elapsed().as_secs_f64() * 1000.0 / stats.entries_replayed.max(1) as f64
        );
    }

    fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for e in std::fs::read_dir(from).unwrap().flatten() {
            let (src, dst) = (e.path(), to.join(e.file_name()));
            if e.metadata().unwrap().is_dir() {
                copy_dir(&src, &dst);
            } else {
                std::fs::copy(&src, &dst).unwrap();
            }
        }
    }

    fn walkdir_size(p: &std::path::Path) -> u64 {
        std::fs::read_dir(p)
            .into_iter()
            .flatten()
            .flatten()
            .map(|e| match e.metadata() {
                Ok(m) if m.is_dir() => walkdir_size(&e.path()),
                Ok(m) => m.len(),
                Err(_) => 0,
            })
            .sum()
    }
}

// ===== batch_queue =====
use anyhow::{Result, anyhow};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub struct BatchQueue {
    tx: mpsc::Sender<RecordBatch>,
    shutdown: CancellationToken,
    worker: tokio_util::task::TaskTracker,
}

/// Row-wise partition of a chunk: one queued batch may carry rows for many projects.
fn group_by_project(batches: impl IntoIterator<Item = RecordBatch>) -> HashMap<String, Vec<RecordBatch>> {
    batches
        .into_iter()
        .filter_map(|batch| {
            crate::database::partition_batch_by_project(batch, "default")
                .inspect_err(|e| error!(error = %e, "Skipping batch: failed to partition by project_id"))
                .ok()
        })
        .flatten()
        .into_group_map()
}

impl BatchQueue {
    pub fn new(db: Arc<crate::database::Database>, interval_ms: u64, max_rows: usize) -> Self {
        let (tx, rx) = mpsc::channel(db.config().core.timefusion_batch_queue_capacity);
        let shutdown = CancellationToken::new();
        let cancelled = shutdown.clone();

        let worker = tokio_util::task::TaskTracker::new();
        worker.spawn(async move {
            // Fully qualified: this module already has futures' `StreamExt` in scope.
            let stream = tokio_stream::StreamExt::chunks_timeout(ReceiverStream::new(rx), max_rows, Duration::from_millis(interval_ms));
            tokio::pin!(stream);

            while let Some(chunk) = tokio::select! {
                chunk = futures::StreamExt::next(&mut stream) => chunk,
                _ = cancelled.cancelled() => None,
            } {
                // Effectful loop: one awaited Delta insert per project.
                for (project_id, batches) in group_by_project(chunk) {
                    let (count, rows) = (batches.len(), batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>());
                    match db.insert_records_batch(&project_id, "otel_logs_and_spans", batches, true, None).await {
                        Ok(_) => info!(%project_id, count, ?rows, "Inserted batches"),
                        Err(e) => error!(%project_id, count, error = %e, "Failed to insert batches"),
                    }
                }
            }
        });

        worker.close();
        Self { tx, shutdown, worker }
    }

    pub fn queue(&self, batch: RecordBatch) -> Result<()> {
        self.tx.try_send(batch).map_err(|e| match e {
            tokio::sync::mpsc::error::TrySendError::Full(_) => anyhow!("batch queue full"),
            tokio::sync::mpsc::error::TrySendError::Closed(_) => anyhow!("batch queue worker has shut down"),
        })
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.worker.wait().await;
    }
}

#[cfg(test)]
mod batch_queue_tests {
    use serial_test::serial;
    use tokio::time::sleep;

    use super::*;
    use crate::{database::Database, support::test_helpers::*};

    /// SAFETY: `set_var` races other threads' env reads; every caller is `#[serial]`.
    async fn test_queue(max_rows: usize) -> Result<BatchQueue> {
        dotenv::dotenv().ok();
        unsafe {
            std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
            std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-bq-{}", uuid::Uuid::new_v4()));
        }
        Ok(BatchQueue::new(Arc::new(Database::new().await?), 100, max_rows))
    }

    /// Queue one span per (id, project), let the flush interval fire, then shut
    /// down. A queue/insert error or the 30s timeout fails the case.
    #[test_case::test_case(10, (0..5).map(|i| (format!("test-{i}"), "test-project-uuid".to_string())).collect::<Vec<_>>() ; "processing: one project, chunk cap 10")]
    #[test_case::test_case(100, ["project_a", "project_b", "project_c"].map(|p| (format!("id_{p}"), p.to_string())).to_vec() ; "grouping: three projects in one chunk")]
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_queue_drains_queued_spans(max_rows: usize, spans: Vec<(String, String)>) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async move {
            let queue = test_queue(max_rows).await?;
            spans.into_iter().try_for_each(|(id, project)| queue.queue(json_to_batch(vec![test_span(&id, &format!("span_{project}"), &project)])?))?;
            sleep(Duration::from_millis(200)).await;
            queue.shutdown().await;
            Ok(())
        })
        .await
        .map_err(|_| anyhow!("Test timed out"))?
    }
}

// ===== insert_coerce =====
// Write-path coercion: multi-row INSERT placeholder types (below), and the
// TF-owned version stamp applied to inbound batches (bottom of the file).
//
// In a multi-row `VALUES ($1,..), ($N+1,..)`, DataFusion attaches the target
// type to the Projection's column references, not to the placeholders, so
// `get_parameter_types()` reports row-2+ placeholders as unknown and pgwire
// infers their types positionally from row 1 (a uuid typed as timestamptz).
// `rewrite_plan` stamps each placeholder's own `field` from its Values column,
// on the `plan_cache` miss path. It rewrites IN PLACE rather than wrapping,
// because a prepared statement retains its whole plan for the life of the
// connection and a node per cell is unaffordable.

use std::sync::OnceLock;

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, TimestampMicrosecondArray},
        datatypes::{DataType, Field, FieldRef, Schema, TimeUnit},
    },
    common::tree_node::{Transformed, TreeNode},
    logical_expr::{Expr, LogicalPlan, Values, expr::Placeholder},
};

pub fn rewrite_plan(plan: LogicalPlan) -> LogicalPlan {
    plan.clone()
        .transform_up(|node| {
            let LogicalPlan::Values(values) = node else {
                return Ok(Transformed::no(node));
            };
            let Values { schema, values } = values;
            let values = values
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .zip(schema.fields())
                        // Type the placeholder IN PLACE, never by wrapping: a per-cell node
                        // is unaffordable in a retained prepared-statement plan.
                        .map(|(expr, f)| match expr {
                            Expr::Placeholder(p) => Expr::Placeholder(Placeholder::new_with_field(p.id, Some(Arc::clone(f)))),
                            _ => expr,
                        })
                        .collect()
                })
                .collect();
            Ok(Transformed::yes(LogicalPlan::Values(Values { schema, values })))
        })
        .map(|t| t.data)
        .unwrap_or_else(|e| {
            // Falling back to the un-coerced plan can leave pgwire serving the wrong
            // placeholder types for multi-row INSERTs.
            warn!(target: "insert_coerce", "plan rewrite skipped (multi-row INSERT type inference may suffer): {e}");
            plan
        })
}

// ---------------------------------------------------------------------------
// TF-owned version stamp for the schema's `dedup_tiebreak` column.
// ---------------------------------------------------------------------------

/// Per-table hybrid logical clock: the last value issued for each table.
///
/// **INVARIANT: single writer per table.** The ordering derives from ONE
/// process's wall clock plus its own last-issued value; two instances writing
/// the same table would issue un-orderable stamps and "greatest wins" would pick
/// the wrong version. Scaling writers out needs a real sequencer.
static LAST_ISSUED: OnceLock<DashMap<String, AtomicI64>> = OnceLock::new();

fn with_cell<R>(table: &str, f: impl FnOnce(&AtomicI64) -> R) -> R {
    let map = LAST_ISSUED.get_or_init(DashMap::default);
    // Early-return on the read path so we never hold a shard's read guard while
    // taking its write guard (same-shard deadlock).
    if let Some(cell) = map.get(table) {
        return f(cell.value());
    }
    f(map.entry(table.to_string()).or_insert(AtomicI64::new(i64::MIN)).value())
}

/// Issue the next stamp for `table`: `max(now, last_issued + 1)`. Strictly
/// increasing by construction, so two versions of a row written inside the same
/// microsecond — or after the wall clock steps backwards — can never tie.
pub fn next_stamp(table: &str) -> i64 {
    with_cell(table, |cell| {
        let now = crate::support::now_micros();
        let next = |prev: i64| now.max(prev.saturating_add(1));
        // Ok/Err both carry the CAS'd `prev`, so re-deriving `next(prev)` is exactly what was stored.
        next(cell.fetch_update(Ordering::AcqRel, Ordering::Relaxed, |prev| Some(next(prev))).unwrap_or_else(|prev| prev))
    })
}

/// Fold an already-issued value into the table's clock. Called for every stamp
/// seen during WAL replay so the first stamp issued after a boot exceeds
/// everything durable — without this, a boot behind an NTP step (or with a
/// stamp issued from `last + 1` past wall-clock) would re-issue values that
/// already exist and a new version could lose to an old one.
pub fn observe_stamp(table: &str, value: i64) {
    with_cell(table, |cell| cell.fetch_max(value, Ordering::AcqRel));
}

/// The declared version-stamp column for `table`: the schema's `dedup_tiebreak`,
/// when it is a microsecond timestamp AND the table declares `version_append`.
///
/// `version_append` is what makes the tiebreak **TF-owned**. A microsecond
/// tiebreak alone is not enough: stamping a client-supplied column would destroy
/// ingested data, so a table with a client-owned tiebreak must leave
/// `version_append` off.
fn stamp_column(table: &str) -> Option<(FieldRef, Option<Arc<str>>)> {
    let schema = crate::schema::get_schema(table).filter(|s| s.version_append)?;
    let name = schema.dedup_tiebreak.as_deref()?;
    let (dt, nullable) = schema.field_def(name)?;
    let DataType::Timestamp(TimeUnit::Microsecond, tz) = &dt else { return None };
    Some((Arc::new(Field::new(name, dt.clone(), nullable)), tz.clone()))
}

/// Stamp every batch's version column with a fresh monotonic value.
///
/// One stamp per batch, not per row: a batch is one write, and rows inside it
/// are distinct rows rather than versions of each other. Successive writes get
/// strictly increasing stamps, which is what versioning needs.
///
/// Any value the client sent is **overwritten** (a client-supplied stamp would
/// break monotonicity); a missing column is appended.
///
/// WAL replay deliberately bypasses this — replayed rows keep the stamp from
/// their original append and feed `observe_stamp` instead.
pub fn stamp_version(table: &str, batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let Some((field, tz)) = stamp_column(table) else {
        return batches;
    };
    batches
        .into_iter()
        .map(|batch| {
            let arr = Arc::new(TimestampMicrosecondArray::from(vec![next_stamp(table); batch.num_rows()]).with_timezone_opt(tz.clone())) as ArrayRef;
            let old_schema = batch.schema();
            let (mut fields, mut columns) = (old_schema.fields().to_vec(), batch.columns().to_vec());
            match old_schema.index_of(field.name()) {
                Ok(i) => (fields[i], columns[i]) = (field.clone(), arr),
                Err(_) => {
                    fields.push(field.clone());
                    columns.push(arr);
                }
            }
            let schema = Arc::new(Schema::new_with_metadata(fields, old_schema.metadata().clone()));
            // Infallible by construction; fall back to the un-stamped batch
            // rather than fail a write.
            RecordBatch::try_new(schema, columns).unwrap_or_else(|e| {
                warn!(target: "insert_coerce", "version stamp skipped for table {table}: {e}");
                batch
            })
        })
        .collect()
}

/// Forget a table's issued-stamp state, so a test can simulate a fresh boot.
#[cfg(test)]
pub fn reset_stamp_state(table: &str) {
    LAST_ISSUED.get_or_init(DashMap::default).remove(table);
}

/// Fold a replayed batch's stamps into the table's clock (see `observe_stamp`).
pub fn observe_batch(table: &str, batch: &RecordBatch) {
    if let Some((field, _)) = stamp_column(table)
        && let Some(max) =
            batch.column_by_name(field.name()).and_then(|c| c.as_any().downcast_ref::<TimestampMicrosecondArray>()).and_then(datafusion::arrow::compute::max)
    {
        observe_stamp(table, max);
    }
}

#[cfg(test)]
mod coerce_tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema, TimeUnit},
        common::tree_node::TreeNodeRecursion,
        datasource::MemTable,
        prelude::SessionContext,
        sql::{
            parser::Statement as DfStatement,
            sqlparser::{dialect::PostgreSqlDialect, parser::Parser},
        },
    };

    use super::*;

    /// `INSERT INTO t (ts, id, name) VALUES ($1,$2,$3), ($4,$5,$6), ...` planned
    /// and coerced, the way the pgwire Parse path does it.
    async fn coerced_insert(rows: usize) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).unwrap())).unwrap();
        let tuples: Vec<String> = (0..rows).map(|r| format!("(${},${},${})", r * 3 + 1, r * 3 + 2, r * 3 + 3)).collect();
        let sql = format!("INSERT INTO t (ts, id, name) VALUES {}", tuples.join(","));
        let ast = Parser::parse_sql(&PostgreSqlDialect {}, &sql).unwrap().remove(0);
        rewrite_plan(ctx.state().statement_to_plan(DfStatement::Statement(Box::new(ast))).await.unwrap())
    }

    fn count_exprs(plan: &LogicalPlan, mut pred: impl FnMut(&Expr) -> bool) -> usize {
        let mut n = 0;
        plan.apply(|node| {
            node.apply_expressions(|e| {
                e.apply(|e| {
                    n += usize::from(pred(e));
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })
        .unwrap();
        n
    }

    /// Every placeholder must carry its own column's type — row-2+ placeholders
    /// coming back untyped is what makes pgwire infer positionally from row 1.
    #[tokio::test]
    async fn every_placeholder_in_every_row_is_typed() {
        let plan = coerced_insert(3).await;
        let types = plan.get_parameter_types().unwrap();
        assert_eq!(types.len(), 9, "one entry per placeholder");
        for i in 1..=9 {
            let got = types.get(&format!("${i}")).unwrap_or_else(|| panic!("${i} missing"));
            let expect = if i % 3 == 1 { DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())) } else { DataType::Utf8 };
            assert_eq!(got.as_ref(), Some(&expect), "${i} must carry its own column's type, not row 1's");
        }
    }

    /// The coercion must cost NOTHING per cell: a per-cell `CAST` node bloats
    /// the plans prepared statements retain for the life of a connection.
    #[tokio::test]
    async fn coercion_adds_no_nodes_per_cell() {
        let plan = coerced_insert(10).await;
        let casts = count_exprs(&plan, |e| matches!(e, Expr::Cast(_)));
        assert_eq!(casts, 0, "placeholders must be typed in place, never wrapped in a per-cell Cast");
        assert_eq!(count_exprs(&plan, |e| matches!(e, Expr::Placeholder(_))), 30, "all 30 placeholders survive the rewrite");
    }
}

#[cfg(test)]
mod stamp_tests {
    use datafusion::arrow::array::{Int64Array, StringArray};
    use serial_test::serial;

    use super::*;

    fn unique_table() -> String {
        format!("t{}", &uuid::Uuid::new_v4().to_string()[..8])
    }

    /// One row batch with an `id` column, plus optionally a client-supplied
    /// `updated_at` we expect TF to overwrite.
    fn batch_with(client_stamp: Option<i64>) -> RecordBatch {
        let id = (Arc::new(Field::new("id", DataType::Int64, false)), Arc::new(Int64Array::from(vec![1i64])) as ArrayRef);
        let stamp = client_stamp.map(|v| {
            (
                Arc::new(Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)),
                Arc::new(TimestampMicrosecondArray::from(vec![v]).with_timezone("UTC")) as ArrayRef,
            )
        });
        let (fields, cols): (Vec<Arc<Field>>, Vec<ArrayRef>) = std::iter::once(id).chain(stamp).unzip();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap()
    }

    fn stamp_of(batch: &RecordBatch) -> Option<i64> {
        let col = batch.column_by_name("updated_at")?;
        let arr = col.as_any().downcast_ref::<TimestampMicrosecondArray>()?;
        arr.is_valid(0).then(|| arr.value(0))
    }

    /// Every hazard that could tie or regress a stamp, in one frozen timeline.
    /// The clock is frozen (not slept on) so the microsecond really is identical,
    /// and values are asserted after `unfreeze` so a failure can't leak it.
    #[test]
    #[serial]
    fn stamps_are_strictly_monotone_across_clock_hazards() {
        let t = unique_table();
        crate::support::set_micros(4_000_000_000_000_000);
        // Two writes inside the SAME microsecond must not tie.
        let (a, b, c) = (next_stamp(&t), next_stamp(&t), next_stamp(&t));
        // A backwards clock step must not re-issue a stamp an old version holds.
        crate::support::set_micros(3_000_000_000_000_000);
        let after = next_stamp(&t);
        // Boot seeding: reset first so `observe_stamp` is exercised on a table
        // with NO issued-stamp entry, which is what a fresh boot hands it.
        reset_stamp_state(&t);
        let replayed = 9_000_000_000_000_000_i64; // well past "now"
        observe_stamp(&t, replayed);
        let next = next_stamp(&t);
        crate::support::unfreeze();
        assert!(a < b && b < c, "stamps must be strictly increasing, got {a} {b} {c}");
        assert!(after > c, "stamp regressed across a backwards clock step: {c} -> {after}");
        assert!(next > replayed, "post-boot stamp {next} must exceed the replayed max {replayed}");
    }

    /// TF owns the tiebreak of a `version_append` table — filling the column when
    /// absent and OVERWRITING a client-supplied value — and owns nothing else's.
    /// Covers both non-owned shapes: no tiebreak (`variant_bench`) and a declared
    /// tiebreak with `version_append` off (`mor_dormant`).
    #[test]
    fn only_version_append_tables_are_stamped() {
        let out = stamp_version("mor_versioned", vec![batch_with(None), batch_with(Some(1_234))]);
        let filled = stamp_of(&out[0]).expect("missing column is appended and populated");
        let overwritten = stamp_of(&out[1]).expect("client value is replaced, not left");
        assert_ne!(overwritten, 1_234, "a client-supplied stamp must be overwritten by TF's");
        assert!(overwritten > filled, "successive batches get increasing stamps");

        for t in ["variant_bench", "mor_dormant"] {
            assert!(stamp_column(t).is_none(), "{t} is not a version_append table — TF must not own its tiebreak");
            let before = batch_with(None);
            let out = stamp_version(t, vec![before.clone()]);
            assert_eq!(out[0].schema(), before.schema(), "{t} batch schema must be untouched");
            assert_eq!(out.len(), 1);
        }
        // Unknown tables (per-test WAL/MemBuffer tables) are likewise untouched.
        assert!(stamp_column(&unique_table()).is_none());
    }

    /// `observe_batch` reads the schema's declared tiebreak column, whatever it
    /// is named — nothing hard-codes `updated_at`.
    #[test]
    fn observe_batch_is_schema_driven() {
        let t = unique_table();
        let declared = crate::schema::get_schema("mor_versioned").expect("fixture registered").dedup_tiebreak.clone().expect("declares a tiebreak");
        assert_eq!(stamp_column("mor_versioned").map(|(f, _)| f.name().clone()), Some(declared.clone()), "the stamp column comes from the YAML, not a literal");
        let schema = Arc::new(Schema::new(vec![Field::new(&declared, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(TimestampMicrosecondArray::from(vec![7_777_i64]).with_timezone("UTC")) as ArrayRef]).unwrap();
        observe_batch("mor_versioned", &batch);
        assert!(next_stamp("mor_versioned") > 7_777);
        // A table with no schema observes nothing (and must not panic).
        observe_batch(
            &t,
            &RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)])), vec![Arc::new(StringArray::from(vec!["a"])) as ArrayRef])
                .unwrap(),
        );
    }
}
