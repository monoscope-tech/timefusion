use std::path::PathBuf;

use arrow::array::RecordBatch;
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use bincode::{Decode, Encode};
use dashmap::DashSet;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};
use walrus_rust::{FsyncSchedule, ReadConsistency, WalPosition, Walrus};

#[derive(Debug, Error)]
pub enum WalError {
    #[error("WAL entry too short: {len} bytes")]
    TooShort { len: usize },
    #[error("Batch too large: {size} bytes exceeds max {max}")]
    BatchTooLarge { size: usize, max: usize },
    #[error("Invalid WAL operation type: {0}")]
    InvalidOperation(u8),
    #[error("Unsupported WAL version: {version} (expected {expected})")]
    UnsupportedVersion { version: u8, expected: u8 },
    #[error("Bincode decode error: {0}")]
    BincodeDecode(#[from] bincode::error::DecodeError),
    #[error("Bincode encode error: {0}")]
    BincodeEncode(#[from] bincode::error::EncodeError),
    #[error("Arrow IPC error: {0}")]
    ArrowIpc(#[from] arrow::error::ArrowError),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("No record batch found in data")]
    EmptyBatch,
    #[error("Internal WAL invariant violated: {0}")]
    Internal(String),
}

/// Magic bytes to identify the WAL format ("WAL2").
const WAL_MAGIC: [u8; 4] = [0x57, 0x41, 0x4C, 0x32];
/// Insert batches are stored as Arrow IPC stream bytes. Embeds the schema so
/// the reader doesn't need a separate registry lookup, and round-trips every
/// Arrow type (List/Struct/Variant/…) without the per-buffer bincode shuffle
/// the older CompactBatch format required.
///
/// Bump on any breaking change to the on-disk WAL format or the walrus key
/// derivation. The startup version-stamp check refuses to open a directory
/// written by a different version, so existing data must be wiped on bump.
const WAL_VERSION: u8 = 1;
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
/// On-disk format version for `cursor_snapshot.json`. Bump on any breaking
/// schema change so older readers fall back to the Delta scan instead of
/// silently misinterpreting the file.
const SNAPSHOT_VERSION: u32 = 1;

/// `WalPosition` serialized as `(block_id, offset)` — tuples already have
/// Serialize/Deserialize, so we skip the mirror struct.
type SnapPos = (u64, u64);
/// Per-(project, table) per-shard cursor positions (`None` = never persisted).
pub type TopicPositions = std::collections::HashMap<(String, String), Vec<Option<WalPosition>>>;
fn pos_to_snap(p: WalPosition) -> SnapPos {
    (p.block_id, p.offset)
}
fn snap_to_pos((block_id, offset): SnapPos) -> WalPosition {
    WalPosition { block_id, offset }
}

/// Serialized form of every known topic's per-shard persisted-read cursor.
/// Written after every successful Delta flush + on graceful shutdown; read
/// on boot to skip the Delta scan when the cursor is known-current.
///
/// Correctness assumes this timefusion process is the **only** writer to its
/// Delta tables — `BufferedWriteLayer::flush` is the sole commit path. If you
/// ever run a parallel writer (manual `OPTIMIZE`, an external delta-rs
/// client, a sister process) between a clean-shutdown snapshot and the next
/// boot, delete `cursor_snapshot.json` to force a Delta reconciliation; the
/// `clean_shutdown` flag alone won't catch out-of-band commits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorSnapshot {
    pub version: u32,
    /// Wall-clock micros (`clock::now_micros`) at write time. Informational
    /// only — surfaced in the boot log so operators can spot a stale
    /// snapshot, but not enforced as a max-age gate.
    pub written_at_micros: i64,
    pub shards_per_topic: usize,
    /// True only when written by the graceful-shutdown path. Boot uses this
    /// flag to decide whether the Delta verifier can be skipped entirely.
    /// NOT a drain claim — shutdown writes it even after a partial/timed-out
    /// flush (the WAL holds the remainder); see `drained` for that.
    pub clean_shutdown: bool,
    /// True only when the shutdown flush left NOTHING un-flushed (no
    /// MemBuffer buckets, no airborne or orphaned holds). Sole authorizer of
    /// the pure-mtime boot WAL GC — with un-flushed data the old files may
    /// BE the backlog. `#[serde(default)]`: snapshots from older builds parse
    /// as drained=false, which just skips the boot sweep (safe direction).
    #[serde(default)]
    pub drained: bool,
    /// `"project_id:table_name"` → per-shard cursor (None = never written).
    pub entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>>,
}

/// Maximum size for a single record batch (100MB) - prevents unbounded memory allocation from malicious/corrupted WAL
/// Hard cap on a single WAL entry's batch payload (1GiB) — the replay
/// acceptance bound, guarding against unbounded allocation from a corrupted
/// entry, and the limit for unsplittable payloads (UPDATE...FROM sources,
/// single oversized rows). Ceiling is walrus's `MAX_ALLOC` (1GiB/block,
/// vendor/walrus-rust config.rs): entries can't physically exceed it, so
/// don't raise this without touching the vendored WAL engine.
const MAX_BATCH_SIZE: usize = 1024 * 1024 * 1024;
/// Append-side split target for INSERT batches — purely a replay-memory and
/// blast-radius knob, invisible to clients and to Delta (flush re-coalesces
/// per table into one commit regardless of WAL chunking). Each WAL entry is
/// read + Arrow-decoded whole during recovery inside the buffer budget, and a
/// corrupted entry quarantines whole — so keep the unit small even though
/// acceptance goes up to `MAX_BATCH_SIZE`.
const WAL_SPLIT_TARGET: usize = 100 * 1024 * 1024;
/// Fsync schedule interval in milliseconds - balances durability with performance
const FSYNC_SCHEDULE_MS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(u8)]
pub enum WalOperation {
    Insert = 0,
    Delete = 1,
    Update = 2,
    /// `UPDATE ... FROM` with a materialized source RecordBatch serialized
    /// alongside the predicate/assignments. Added in V2 of the UPDATE shape;
    /// old binaries will reject these entries with `InvalidOperation(3)`.
    UpdateWithSource = 3,
}

impl TryFrom<u8> for WalOperation {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WalOperation::Insert),
            1 => Ok(WalOperation::Delete),
            2 => Ok(WalOperation::Update),
            3 => Ok(WalOperation::UpdateWithSource),
            _ => Err(WalError::InvalidOperation(value)),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id: String,
    pub table_name: String,
    pub operation: WalOperation,
    #[bincode(with_serde)]
    pub data: Vec<u8>,
}

impl WalEntry {
    fn new(project_id: &str, table_name: &str, operation: WalOperation, data: Vec<u8>) -> Self {
        Self { timestamp_micros: chrono::Utc::now().timestamp_micros(), project_id: project_id.into(), table_name: table_name.into(), operation, data }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct DeletePayload {
    pub predicate_sql: Option<String>,
}

#[derive(Debug, Encode, Decode)]
pub struct UpdatePayload {
    pub predicate_sql: Option<String>,
    pub assignments: Vec<(String, String)>,
}

/// `UPDATE ... FROM` source side, persisted alongside the predicate +
/// assignments so WAL replay can reconstruct the join after a restart.
/// `batch_ipc` is an Arrow IPC stream of the source `RecordBatch`.
#[derive(Debug, Clone, Encode, Decode)]
pub struct SerializedSource {
    /// `(target_col, source_col)` pairs — bare column names.
    pub join_keys: Vec<(String, String)>,
    /// Arrow IPC stream bytes of the materialized source batch.
    pub batch_ipc: Vec<u8>,
}

#[derive(Debug, Encode, Decode)]
pub struct UpdateWithSourcePayload {
    pub predicate_sql: Option<String>,
    pub assignments: Vec<(String, String)>,
    pub source: SerializedSource,
}

/// Number of walrus shards per logical (project_id, table_name) topic.
/// Walrus serializes appends within a single collection — the per-collection
/// `is_batch_writing` AtomicBool returns WouldBlock on concurrent batch
/// writes. Routing each write to one of N hash-distinguished shards lifts the
/// single-project ceiling near-linearly (different shards never contend on
/// the same walrus block/offset), at the cost of merging N streams in
/// timestamp order during recovery.
///
/// 4 is a defensible default for a developer/single-host workload; production
/// deployments override via `BufferConfig::timefusion_wal_shards_per_topic`.
const WAL_SHARDS_PER_TOPIC_DEFAULT: usize = 4;

/// Stripe count for the per-collection append locks (see
/// `WalManager::append_locks`). Far exceeds the realistic distinct-collection
/// count (topics × shards) so false sharing between unrelated collections is
/// negligible.
const WAL_APPEND_LOCK_STRIPES: usize = 256;

pub struct WalManager {
    wal: Walrus,
    data_dir: PathBuf,
    /// Logical topic strings ("{project_id}:{table_name}") — one entry per
    /// (project, table). Each maps to `shards_per_topic` walrus collections.
    known_topics: DashSet<String>,
    /// Per-topic round-robin counter chooses which shard the next batch is
    /// appended to. Topic-scoped (rather than global) so we don't penalize
    /// the cold-cache miss for an idle topic.
    shard_counter: dashmap::DashMap<String, std::sync::atomic::AtomicU64>,
    shards_per_topic: usize,
    /// Per-collection append serialization. Walrus rejects *concurrent* appends
    /// to one collection with "another batch write already in progress".
    /// `pick_shard` spreads load across `shards_per_topic` collections, but more
    /// than `shards_per_topic` concurrent appends to one topic still collide on
    /// a shard. These striped locks make colliders QUEUE (briefly — an append is
    /// an in-memory write; fsync is decoupled) instead of erroring the insert,
    /// which would dead-letter the row. Striped by `walrus_key` hash.
    append_locks: Vec<std::sync::Mutex<()>>,
    /// Fsync the shard before returning from single-entry (DML) appends —
    /// see `BufferConfig::timefusion_wal_ack_fsync`. Batched INSERT appends
    /// are always flushed before return by walrus's `batch_write`.
    ack_fsync: bool,
}

impl WalManager {
    pub fn new(data_dir: PathBuf) -> Result<Self, WalError> {
        Self::with_fsync_mode(data_dir, crate::config::WalFsyncMode::Milliseconds(FSYNC_SCHEDULE_MS))
    }

    pub fn with_fsync_ms(data_dir: PathBuf, fsync_ms: u64) -> Result<Self, WalError> {
        Self::with_fsync_mode(data_dir, crate::config::WalFsyncMode::Milliseconds(fsync_ms))
    }

    pub fn with_fsync_mode(data_dir: PathBuf, mode: crate::config::WalFsyncMode) -> Result<Self, WalError> {
        Self::with_fsync_mode_and_shards(data_dir, mode, WAL_SHARDS_PER_TOPIC_DEFAULT)
    }

    pub fn with_fsync_mode_and_shards(data_dir: PathBuf, mode: crate::config::WalFsyncMode, shards_per_topic: usize) -> Result<Self, WalError> {
        std::fs::create_dir_all(&data_dir)?;
        Self::check_wal_version_stamp(&data_dir)?;

        let schedule = match mode {
            crate::config::WalFsyncMode::Milliseconds(ms) => FsyncSchedule::Milliseconds(ms),
            crate::config::WalFsyncMode::SyncEach => FsyncSchedule::SyncEach,
            crate::config::WalFsyncMode::None => FsyncSchedule::NoFsync,
        };
        let wal = Walrus::with_consistency_and_schedule(ReadConsistency::StrictlyAtOnce, schedule)?;

        // Load known topics from index file
        let meta_dir = data_dir.join(".timefusion_meta");
        let _ = std::fs::create_dir_all(&meta_dir);
        let topics_file = meta_dir.join("topics");

        let known_topics = DashSet::new();
        if let Ok(content) = std::fs::read_to_string(&topics_file) {
            for topic in content.lines().filter(|l| !l.is_empty()) {
                known_topics.insert(topic.to_string());
            }
        }

        // Sweep a leftover snapshot tmp file from a crash between
        // `fs::write(tmp)` and `fs::rename(tmp, target)`. Harmless if it
        // sticks around but trivial to clean here.
        let _ = std::fs::remove_file(meta_dir.join("cursor_snapshot.json.tmp"));

        let shards_per_topic = shards_per_topic.max(1);
        info!("WAL initialized at {:?}, known topics: {}, shards/topic: {}", data_dir, known_topics.len(), shards_per_topic);
        Ok(Self {
            wal,
            data_dir,
            known_topics,
            shard_counter: dashmap::DashMap::new(),
            shards_per_topic,
            append_locks: (0..WAL_APPEND_LOCK_STRIPES).map(|_| std::sync::Mutex::new(())).collect(),
            ack_fsync: false,
        })
    }

    /// Verify the on-disk WAL was written by a compatible binary before we
    /// open it. Each `WAL_VERSION` bump is a breaking change to the entry
    /// encoding (or, for 131, to the walrus collection key); silently mixing
    /// versions strands data and produces noisy per-entry errors during
    /// recovery. We write a `wal_version` stamp in `.timefusion_meta/` on
    /// first init and refuse to start if it doesn't match.
    ///
    /// Fresh directories (no stamp, no walrus state) auto-stamp the current
    /// version. A pre-existing walrus dir without a stamp is treated as
    /// pre-stamp legacy and refused.
    fn check_wal_version_stamp(data_dir: &std::path::Path) -> Result<(), WalError> {
        let meta_dir = data_dir.join(".timefusion_meta");
        let _ = std::fs::create_dir_all(&meta_dir);
        let stamp_path = meta_dir.join("wal_version");

        let has_walrus_state =
            std::fs::read_dir(data_dir).map(|rd| rd.flatten().any(|e| e.file_name() != ".timefusion_meta" && e.file_name() != "wal_version")).unwrap_or(false);

        match std::fs::read_to_string(&stamp_path) {
            Ok(s) => {
                let on_disk: u8 = s.trim().parse().map_err(|_| WalError::UnsupportedVersion { version: 0, expected: WAL_VERSION })?;
                if on_disk != WAL_VERSION {
                    error!(
                        "WAL on-disk version {} != binary version {}. IN-FLIGHT DATA WILL BE LOST \
                         IF YOU PROCEED. Wipe {:?} to start fresh, or run a matching binary.",
                        on_disk, WAL_VERSION, data_dir
                    );
                    return Err(WalError::UnsupportedVersion { version: on_disk, expected: WAL_VERSION });
                }
                Ok(())
            }
            Err(_) if has_walrus_state => {
                error!(
                    "WAL directory {:?} has data but no version stamp (pre-stamp legacy). \
                     Wipe the directory to start fresh on WAL v{}.",
                    data_dir, WAL_VERSION
                );
                Err(WalError::UnsupportedVersion { version: 0, expected: WAL_VERSION })
            }
            Err(_) => {
                std::fs::write(&stamp_path, WAL_VERSION.to_string())?;
                info!("WAL initialized fresh at v{}", WAL_VERSION);
                Ok(())
            }
        }
    }

    // Persist topic to index file. Called after WAL append - if crash occurs between
    // append and persist, orphan entries are still recovered via for_each_entry
    // which scans all known WAL topics in the directory.
    fn persist_topic(&self, topic: &str) {
        if self.known_topics.insert(topic.to_string()) {
            let meta_dir = self.data_dir.join(".timefusion_meta");
            if let Err(e) = std::fs::create_dir_all(&meta_dir) {
                warn!("Failed to create WAL meta dir {:?}: {}", meta_dir, e);
                return;
            }
            match std::fs::OpenOptions::new().create(true).append(true).open(meta_dir.join("topics")) {
                Ok(mut file) => {
                    use std::io::Write;
                    if let Err(e) = writeln!(file, "{}", topic) {
                        warn!("Failed to write topic '{}' to index: {}", topic, e);
                    }
                }
                Err(e) => warn!("Failed to open topics file: {}", e),
            }
        }
    }

    /// Human-readable topic identifier for metadata/logging
    fn make_topic(project_id: &str, table_name: &str) -> String {
        format!("{}:{}", project_id, table_name)
    }

    /// Short hash for walrus topic key, scoped to a shard so we get N
    /// independent walrus collections per logical (project, table).
    /// Walrus's metadata budget is 62 bytes; 16 hex chars + a `-` + 2 digits
    /// shard suffix stays well under.
    fn walrus_topic_key(project_id: &str, table_name: &str, shard: usize) -> String {
        // Must be stable across compilations — the key indexes durable WAL
        // data. AHasher::default() seeds itself per build, which would silently
        // strand entries after an upgrade. FNV-1a is deterministic, fast, and
        // 64-bit-wide (the only width walrus's 62-byte key budget needs).
        //
        // Length-prefix each field so ("a:b","c") and ("a","b:c") (or any
        // pair that would concatenate to the same bytes) hash distinctly.
        // Don't rely on `str::hash`'s 0xff terminator for separation — that's
        // a stdlib implementation detail, not a contract.
        use std::hash::Hasher;

        use fnv::FnvHasher;
        let mut hasher = FnvHasher::default();
        hasher.write_u64(project_id.len() as u64);
        hasher.write(project_id.as_bytes());
        hasher.write_u64(table_name.len() as u64);
        hasher.write(table_name.as_bytes());
        format!("{:016x}-{:02}", hasher.finish(), shard)
    }

    /// Round-robin shard chooser for a topic. Bumps a per-topic counter so
    /// concurrent batches for the same topic spread across N walrus
    /// collections rather than serializing at walrus's per-collection write
    /// lock.
    fn pick_shard(&self, topic: &str) -> usize {
        use std::sync::atomic::Ordering;
        let counter = self.shard_counter.entry(topic.to_string()).or_insert_with(|| std::sync::atomic::AtomicU64::new(0));
        (counter.fetch_add(1, Ordering::Relaxed) as usize) % self.shards_per_topic
    }

    fn parse_topic(topic: &str) -> Option<(String, String)> {
        topic.split_once(':').map(|(p, t)| (p.to_string(), t.to_string()))
    }

    /// Acquire the append lock for a walrus collection so concurrent appends to
    /// it queue instead of hitting walrus's "another batch write already in
    /// progress". Held only across the (fast, in-memory) walrus write — never an
    /// `.await` — so blocking a worker is brief. The guard wraps `()`, so a
    /// poisoned lock carries no invalid state and is safe to recover.
    fn append_lock(&self, walrus_key: &str) -> std::sync::MutexGuard<'_, ()> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        walrus_key.hash(&mut h);
        let idx = (h.finish() as usize) % self.append_locks.len();
        self.append_locks[idx].lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Enable fsync-before-ack for single-entry appends (`TIMEFUSION_WAL_ACK_FSYNC`).
    pub fn with_ack_fsync(mut self, on: bool) -> Self {
        self.ack_fsync = on;
        self
    }

    /// Serialize and append one entry under the shard's `append_lock` so
    /// concurrent same-shard appends queue instead of erroring. The guard
    /// drops when this returns, so callers' `persist_topic` file I/O runs
    /// outside the critical section — keep it after the call, not before.
    /// `on_pre` fires with the pre-append tail under the lock — same
    /// hold-registration contract as [`Self::append_batch`].
    fn locked_append(&self, walrus_key: &str, entry: &WalEntry, on_pre: impl FnOnce(Option<WalPosition>)) -> Result<(), WalError> {
        let entry_bytes = serialize_wal_entry(entry)?;
        let guard = self.append_lock(walrus_key);
        on_pre(self.wal.current_position(walrus_key).ok());
        self.wal.append_for_topic(walrus_key, &entry_bytes)?;
        // Sync OUTSIDE the stripe lock: the entry's bytes are already in the
        // mmap and `Writer::sync` flushes the whole active block, so
        // sync-before-ack holds — while an ms-scale msync under the stripe
        // would stall every same-stripe append (the lock's contract is
        // "fast, in-memory only").
        drop(guard);
        if self.ack_fsync {
            self.wal.sync_topic(walrus_key).map_err(WalError::Io)?;
        }
        Ok(())
    }

    /// Returns the shard the entry was appended to.
    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> Result<usize, WalError> {
        self.append_batch(project_id, table_name, std::slice::from_ref(batch), |_, _| {}).map(|(shard, _)| shard)
    }

    /// Returns `(shard, pre_append_position)` — every batch becomes one walrus
    /// entry on the chosen shard.
    ///
    /// `on_pre_append(shard, position)` fires under the shard's append lock
    /// BEFORE the entries exist, with the shard's write tail at that instant.
    /// Callers register a read-cursor *hold* there: because registration
    /// happens-before the append, a concurrent watermark computation that
    /// snapshots the tail first and then reads holds can never advance the
    /// cursor past an entry whose hold it hasn't seen (see
    /// `BufferedWriteLayer::compute_wal_watermark`).
    #[instrument(skip(self, batches, on_pre_append), fields(project_id, table_name, batch_count))]
    pub fn append_batch(
        &self, project_id: &str, table_name: &str, batches: &[RecordBatch], on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<(usize, Option<WalPosition>), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batches.len());
        for batch in batches {
            for data in split_to_wal_payloads(batch, WAL_SPLIT_TARGET, MAX_BATCH_SIZE)? {
                payloads.push(serialize_wal_entry(&WalEntry::new(project_id, table_name, WalOperation::Insert, data))?);
            }
        }

        let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        let pre_pos;
        {
            // Guard scoped tightly: dropped before persist_topic so the shard
            // lock never covers persist_topic's synchronous file I/O.
            let _guard = self.append_lock(&walrus_key);
            pre_pos = self.wal.current_position(&walrus_key).ok();
            on_pre_append(shard, pre_pos);
            self.wal.batch_append_for_topic(&walrus_key, &payload_refs)?;
        }
        self.persist_topic(&topic);
        debug!("WAL batch append INSERT: topic={}, shard={}, batches={}", topic, shard, batches.len());
        Ok((shard, pre_pos))
    }

    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, on_pre_append), fields(project_id, table_name))]
    pub fn append_delete(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let data = bincode::encode_to_vec(&DeletePayload { predicate_sql: predicate_sql.map(String::from) }, BINCODE_CONFIG)?;
        let entry = WalEntry::new(project_id, table_name, WalOperation::Delete, data);
        self.locked_append(&walrus_key, &entry, |pre| on_pre_append(shard, pre))?;
        self.persist_topic(&topic);
        debug!("WAL append DELETE: topic={}, shard={}, predicate={:?}", topic, shard, predicate_sql);
        Ok(shard)
    }

    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, assignments, on_pre_append), fields(project_id, table_name))]
    pub fn append_update(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)],
        on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let payload = UpdatePayload { predicate_sql: predicate_sql.map(String::from), assignments: assignments.to_vec() };
        let entry = WalEntry::new(project_id, table_name, WalOperation::Update, bincode::encode_to_vec(&payload, BINCODE_CONFIG)?);
        self.locked_append(&walrus_key, &entry, |pre| on_pre_append(shard, pre))?;
        self.persist_topic(&topic);
        debug!("WAL append UPDATE: topic={}, shard={}, predicate={:?}, assignments={}", topic, shard, predicate_sql, assignments.len());
        Ok(shard)
    }

    /// Append an `UPDATE ... FROM` entry. Stores the source `RecordBatch`
    /// (already serialized to Arrow IPC bytes by the caller) alongside the
    /// predicate + assignments so WAL replay can reconstruct the join.
    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, assignments, source, on_pre_append), fields(project_id, table_name, source_ipc_bytes = source.batch_ipc.len()))]
    pub fn append_update_with_source(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], source: &SerializedSource,
        on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        // The replay-side deserializer rejects over-cap source batches, so an
        // acked oversized entry would be silently dropped at the next boot —
        // fail the append instead so the client sees the error. (INSERTs are
        // split transparently; a JOIN source can't be split without changing
        // update semantics for non-unique keys.)
        if source.batch_ipc.len() > MAX_BATCH_SIZE {
            return Err(WalError::BatchTooLarge { size: source.batch_ipc.len(), max: MAX_BATCH_SIZE });
        }
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let payload = UpdateWithSourcePayload { predicate_sql: predicate_sql.map(String::from), assignments: assignments.to_vec(), source: source.clone() };
        let entry = WalEntry::new(project_id, table_name, WalOperation::UpdateWithSource, bincode::encode_to_vec(&payload, BINCODE_CONFIG)?);
        self.locked_append(&walrus_key, &entry, |pre| on_pre_append(shard, pre))?;
        self.persist_topic(&topic);
        debug!(
            "WAL append UPDATE_WITH_SOURCE: topic={}, shard={}, predicate={:?}, assignments={}, source_keys={}, source_bytes={}",
            topic,
            shard,
            predicate_sql,
            assignments.len(),
            source.join_keys.len(),
            source.batch_ipc.len()
        );
        Ok(shard)
    }

    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn read_entries_raw(
        &self, project_id: &str, table_name: &str, since_timestamp_micros: Option<i64>, checkpoint: bool,
    ) -> Result<(Vec<WalEntry>, usize), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let cutoff = since_timestamp_micros.unwrap_or(0);
        let mut results = Vec::new();
        let mut error_count = 0usize;

        // Each topic is split across `shards_per_topic` walrus collections; we
        // drain each in append order, then sort the merged slice by
        // timestamp so the caller sees a topic-wide ordering.
        for shard in 0..self.shards_per_topic {
            let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
            loop {
                match self.wal.read_next(&walrus_key, checkpoint) {
                    Ok(Some(entry_data)) => match deserialize_wal_entry(&entry_data.data) {
                        Ok(entry) if entry.timestamp_micros >= cutoff => results.push(entry),
                        Ok(_) => {} // Skip old entries
                        Err(e @ WalError::UnsupportedVersion { .. }) => {
                            error!(
                                "WAL on-disk version mismatch on shard {} ({e}); IN-FLIGHT DATA WILL BE LOST. \
                                 Wipe ${{TIMEFUSION_DATA_DIR}}/wal to start fresh, or roll back to a binary \
                                 that wrote the existing entries.",
                                shard
                            );
                            error_count += 1;
                        }
                        Err(e) => {
                            error!("WAL CORRUPTION on shard {}: undeserializable entry: {}", shard, e);
                            error_count += 1;
                        }
                    },
                    Ok(None) => break,
                    Err(e) => {
                        error!("I/O error reading WAL shard {}: {}", shard, e);
                        error_count += 1;
                        break;
                    }
                }
            }
        }
        results.sort_by_key(|e| e.timestamp_micros);

        if error_count > 0 {
            warn!("WAL read: topic={}, entries={}, errors={}", topic, results.len(), error_count);
        } else {
            debug!("WAL read: topic={}, entries={}", topic, results.len());
        }
        Ok((results, error_count))
    }

    /// Pull-based stream of every un-consumed WAL entry, topic by topic.
    /// Bounded recovery memory: at most one entry per shard is alive at a
    /// time, vs the old `read_all_entries_raw` which materialized the entire
    /// slice (millions of entries / GiBs at long retention) into a Vec.
    ///
    /// Within each topic, the N shard streams are merged by `timestamp_micros`
    /// using a min-heap (k-way merge), so DELETE-after-INSERT ordering within
    /// a topic is preserved even when those operations happen on different
    /// shards. Cross-topic ordering is not preserved — that's fine because
    /// DELETE and UPDATE only mutate their own topic's MemBuffer.
    ///
    /// Pull-based (vs the old callback `for_each_entry`) so `recover_from_wal`
    /// can await flush-to-make-room between entries — budget-bounded replay.
    ///
    /// Always checkpointing: an uncheckpointed `read_next` never advances the
    /// cursor, so a read-until-None stream would re-read the first entry
    /// forever (2026-07-08: hung the suite and OOM'd the host). Recovery
    /// parks the cursor back afterwards via `set_positions_allow_rewind`.
    pub fn replay_iter(&self) -> Result<WalReplayIter<'_>, WalError> {
        Ok(WalReplayIter {
            wal: self,
            topics: self.list_topics()?,
            topic_idx: 0,
            heap: std::collections::BinaryHeap::new(),
            shard_keys: Vec::new(),
            pending: Vec::new(),
            cur_topic: None,
            total: 0,
            errors: 0,
        })
    }

    /// Read the next entry from a shard, skipping corrupted ones. Returns
    /// `None` at end of stream. Shared by `WalReplayIter`'s k-way merge.
    fn next_from_shard(wal: &Walrus, key: &str, checkpoint: bool, errors: &mut usize) -> Option<(WalEntry, WalPosition)> {
        loop {
            match wal.read_next_with_position(key, checkpoint) {
                Ok(Some((d, pos))) => match deserialize_wal_entry(&d.data) {
                    Ok(entry) => return Some((entry, pos)),
                    Err(e @ WalError::UnsupportedVersion { .. }) => {
                        error!(
                            "WAL on-disk version mismatch on shard {} ({e}); IN-FLIGHT DATA WILL BE LOST. \
                             Wipe ${{TIMEFUSION_DATA_DIR}}/wal to start fresh, or roll back to a binary \
                             that wrote the existing entries.",
                            key
                        );
                        *errors += 1;
                    }
                    Err(e) => {
                        error!("WAL CORRUPTION on shard {}: undeserializable entry: {}", key, e);
                        *errors += 1;
                    }
                },
                Ok(None) => return None,
                Err(e) => {
                    error!("I/O error reading WAL shard {}: {}", key, e);
                    *errors += 1;
                    return None;
                }
            }
        }
    }

    pub fn deserialize_batch(data: &[u8], _table_name: &str) -> Result<RecordBatch, WalError> {
        deserialize_record_batch(data)
    }

    pub fn list_topics(&self) -> Result<Vec<String>, WalError> {
        Ok(self.known_topics.iter().map(|t| t.clone()).collect())
    }

    /// Same as `list_topics` but parsed into `(project_id, table_name)` pairs.
    /// Callers iterating topics shouldn't need to know the joining convention.
    pub fn list_topic_pairs(&self) -> Result<Vec<(String, String)>, WalError> {
        Ok(self.known_topics.iter().filter_map(|t| Self::parse_topic(&t)).collect())
    }

    /// Set each shard's persisted read cursor to `positions[shard]`
    /// unconditionally — backward moves allowed (unlike the forward-only
    /// [`Self::merge_persisted_positions`]). Used by WAL recovery, which
    /// consumes to tail while replaying and must then park the cursor back at
    /// the earliest entry still owned by an unflushed MemBuffer bucket.
    /// `None` shards are left untouched.
    pub fn set_positions_allow_rewind(&self, project_id: &str, table_name: &str, positions: &[Option<WalPosition>]) -> Result<(), WalError> {
        self.check_shard_len("set_positions_allow_rewind", positions.len())?;
        for (shard, pos) in positions.iter().enumerate() {
            if let Some(pos) = pos {
                let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
                self.wal.set_persisted_read_position(&walrus_key, *pos).map_err(WalError::Io)?;
            }
        }
        Ok(())
    }

    fn for_each_shard<T>(&self, project_id: &str, table_name: &str, mut f: impl FnMut(&str) -> std::io::Result<T>) -> Result<Vec<T>, WalError> {
        (0..self.shards_per_topic).map(|shard| f(&Self::walrus_topic_key(project_id, table_name, shard)).map_err(WalError::Io)).collect()
    }

    fn check_shard_len(&self, label: &str, len: usize) -> Result<(), WalError> {
        if len != self.shards_per_topic {
            return Err(WalError::Internal(format!("{}: len={} but shards_per_topic={}", label, len, self.shards_per_topic)));
        }
        Ok(())
    }

    /// Snapshot the walrus write tail per shard. Used at bucket-seal time to
    /// capture the watermark recorded in Delta commit metadata.
    pub fn current_position(&self, project_id: &str, table_name: &str) -> Result<Vec<WalPosition>, WalError> {
        self.for_each_shard(project_id, table_name, |k| self.wal.current_position(k))
    }

    /// Snapshot the walrus write tail on a single shard. No-allocation variant
    /// for the per-insert hot path.
    pub fn current_position_for_shard(&self, project_id: &str, table_name: &str, shard: usize) -> Result<WalPosition, WalError> {
        let key = Self::walrus_topic_key(project_id, table_name, shard);
        self.wal.current_position(&key).map_err(WalError::Io)
    }

    /// Read the walrus persisted-read cursor per shard. `None` for shards
    /// whose cursor has never been persisted.
    pub fn persisted_read_positions(&self, project_id: &str, table_name: &str) -> Result<Vec<Option<WalPosition>>, WalError> {
        self.for_each_shard(project_id, table_name, |k| self.wal.persisted_read_position(k))
    }

    /// Set the walrus persisted-read cursor per shard. Used at startup to
    /// fast-forward to a Delta-derived watermark when Delta is ahead of
    /// locally-fsynced walrus state.
    pub fn set_persisted_positions(&self, project_id: &str, table_name: &str, positions: &[WalPosition]) -> Result<(), WalError> {
        self.check_shard_len("set_persisted_positions", positions.len())?;
        for (shard, pos) in positions.iter().enumerate() {
            let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
            self.wal.set_persisted_read_position(&walrus_key, *pos).map_err(WalError::Io)?;
        }
        Ok(())
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Test hook: append raw bytes as a WAL entry so recovery-corruption
    /// paths can be exercised (a valid appender can't produce a corrupt
    /// payload by construction).
    #[cfg(test)]
    pub fn append_raw_for_test(&self, project_id: &str, table_name: &str, bytes: &[u8]) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let _guard = self.append_lock(&walrus_key);
        self.wal.append_for_topic(&walrus_key, bytes)?;
        drop(_guard);
        self.persist_topic(&topic);
        Ok(())
    }

    fn cursor_snapshot_path(&self) -> PathBuf {
        cursor_snapshot_path_in(&self.data_dir)
    }

    /// Capture every known topic's per-shard persisted-read cursor to a single
    /// JSON file on local disk. On boot, the file lets us skip
    /// `derive_wal_cursors_from_delta`'s ~6.5-minute R2 scan when it's known
    /// to be current — the dominant cold-boot cost.
    ///
    /// `clean_shutdown=true` is set only by the graceful-shutdown path; flush
    /// callers pass false so a hard kill still falls back to the Delta scan
    /// to verify the cursor.
    ///
    /// Atomic: writes to `.tmp` then renames. Best-effort: returns Err but the
    /// caller logs-and-continues — a missing snapshot only costs us the next
    /// boot's fast path, never correctness.
    pub fn write_cursor_snapshot(&self, clean_shutdown: bool, drained: bool) -> Result<(), WalError> {
        let mut entries = std::collections::BTreeMap::new();
        for (project_id, table_name) in self.list_topic_pairs()? {
            let positions = match self.persisted_read_positions(&project_id, &table_name) {
                Ok(p) => p,
                Err(e) => {
                    debug!("write_cursor_snapshot: skipping {}/{}: {}", project_id, table_name, e);
                    continue;
                }
            };
            entries.insert(Self::make_topic(&project_id, &table_name), positions.into_iter().map(|p| p.map(pos_to_snap)).collect());
        }
        let snap = CursorSnapshot {
            version: SNAPSHOT_VERSION,
            written_at_micros: crate::clock::now_micros(),
            shards_per_topic: self.shards_per_topic,
            clean_shutdown,
            drained,
            entries,
        };
        // `.timefusion_meta/` is created in `with_fsync_mode_and_shards`; no
        // create_dir_all needed on every flush.
        let target = self.cursor_snapshot_path();
        // Defensive: serde_json::to_vec on a struct with only primitive +
        // standard-collection fields is infallible. Keep the map_err so a
        // future field addition (custom Serialize) still surfaces clearly.
        let bytes = serde_json::to_vec(&snap).map_err(|e| WalError::Internal(format!("cursor snapshot encode: {}", e)))?;
        // Not durable: a lost snapshot only costs the next boot's fast path,
        // and drained=true reverting to absent is the safe direction.
        write_atomic(&target, &bytes, false)?;
        Ok(())
    }

    /// Remove the on-disk cursor snapshot. Called after a snapshot write
    /// fails so the next boot doesn't fall back to stale-but-readable state
    /// and shallow-scan over commits made since the last good write. NotFound
    /// is silently ignored (caller's intent — "no file" is the goal state).
    pub fn delete_cursor_snapshot(&self) -> Result<(), WalError> {
        match std::fs::remove_file(self.cursor_snapshot_path()) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(WalError::Io(e)),
        }
    }

    fn recovery_rewind_path(&self) -> PathBuf {
        self.data_dir.join(".timefusion_meta").join("recovery_rewind.json")
    }

    /// Crash-safety for WAL replay: replay consumes the walrus cursor as it
    /// reads (persisting progress), so a crash mid-replay would skip the
    /// consumed entries on the next boot even though their data never reached
    /// Delta. Before replaying, the recovery path persists this marker with
    /// the pre-recovery cursors; a marker found at boot means the previous
    /// recovery crashed — rewind to it and re-replay (into a fresh MemBuffer,
    /// so re-application is exact). Deleted only after the post-replay
    /// watermark parks the cursor safely.
    ///
    /// Returns the captured pre-recovery positions per (project, table) so
    /// the caller can pin replay-created buckets at them.
    pub fn write_recovery_rewind_marker(&self) -> Result<TopicPositions, WalError> {
        let mut entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>> = std::collections::BTreeMap::new();
        let mut p0 = std::collections::HashMap::new();
        for (project_id, table_name) in self.list_topic_pairs()? {
            let positions = self.persisted_read_positions(&project_id, &table_name)?;
            entries.insert(Self::make_topic(&project_id, &table_name), positions.iter().map(|p| p.map(pos_to_snap)).collect());
            // Never-persisted shards (None) become explicit ORIGIN holds: a
            // brand-new topic's replayed buckets must still be pinned, or the
            // consumed-to-tail cursor + deleted marker would lose them on the
            // next crash before their first flush.
            p0.insert((project_id, table_name), positions.into_iter().map(|p| Some(p.unwrap_or(WalPosition::ORIGIN))).collect());
        }
        let target = self.recovery_rewind_path();
        let bytes = serde_json::to_vec(&entries).map_err(|e| WalError::Internal(format!("rewind marker encode: {}", e)))?;
        // The marker's whole job is surviving a crash mid-replay while walrus
        // fsyncs cursor progress — match that durability (sync file + dir).
        write_atomic(&target, &bytes, true)?;
        Ok(p0)
    }

    /// Rewrite the rewind marker to `positions` (per-topic, per-shard) — the
    /// current replay watermark. Called after a mid-replay drain durably
    /// commits its buckets to Delta: advancing the marker means a subsequent
    /// crash re-replays only from the earliest still-un-drained entry instead
    /// of the pre-recovery cursor, removing the "restart re-reads the whole
    /// backlog" amplification. Full overwrite: `positions` MUST carry every
    /// topic the initial marker held (the caller passes each topic's real
    /// watermark, or its pre-recovery cursor for un-started topics, so nothing
    /// is silently rewound to ORIGIN). A genuinely-None shard maps to ORIGIN on
    /// apply, which is correct only when that shard has no covered data.
    /// Durable (fsync) — same as the initial marker.
    pub fn write_recovery_rewind_marker_at(&self, positions: &std::collections::HashMap<(String, String), Vec<Option<WalPosition>>>) -> Result<(), WalError> {
        let entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>> =
            positions.iter().map(|((p, t), shards)| (Self::make_topic(p, t), shards.iter().map(|s| s.map(pos_to_snap)).collect())).collect();
        let bytes = serde_json::to_vec(&entries).map_err(|e| WalError::Internal(format!("rewind marker encode: {}", e)))?;
        write_atomic(&self.recovery_rewind_path(), &bytes, true)?;
        Ok(())
    }

    /// Apply a leftover rewind marker (see `write_recovery_rewind_marker`).
    /// Returns true when a marker was found and applied.
    pub fn apply_recovery_rewind_marker(&self) -> Result<bool, WalError> {
        let bytes = match std::fs::read(self.recovery_rewind_path()) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(WalError::Io(e)),
        };
        let entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>> =
            serde_json::from_slice(&bytes).map_err(|e| WalError::Internal(format!("rewind marker decode: {}", e)))?;
        for (topic, positions) in &entries {
            let Some((project_id, table_name)) = Self::parse_topic(topic) else {
                // Same reasoning as the shard-count mismatch below: silently
                // skipping lets recovery overwrite the marker with the crashed
                // replay's consumed cursors, losing what it consumed.
                return Err(WalError::Internal(format!("rewind marker has unparseable topic {:?} — refusing to recover past it", topic)));
            };
            if positions.len() != self.shards_per_topic {
                // Skipping would let recover_from_wal overwrite the marker
                // with the crashed replay's already-consumed cursors —
                // permanently losing the entries it consumed. Fail the boot
                // so an operator restores the shard config (or resolves by
                // hand) with the marker intact.
                return Err(WalError::Internal(format!(
                    "rewind marker entry for {} has {} shards but topic has {} — refusing to recover with a shard-count mismatch (restore TIMEFUSION_WAL_SHARDS_PER_TOPIC or handle the marker manually)",
                    topic,
                    positions.len(),
                    self.shards_per_topic
                )));
            }
            // A None marker shard means "never persisted" = cursor at origin
            // pre-recovery; the crashed replay may have persisted progress on
            // it since, so rewind it explicitly to ORIGIN.
            let positions: Vec<Option<WalPosition>> = positions.iter().map(|p| Some(p.map_or(WalPosition::ORIGIN, snap_to_pos))).collect();
            self.set_positions_allow_rewind(&project_id, &table_name, &positions)?;
        }
        warn!("Applied recovery rewind marker for {} topic(s) — previous replay crashed mid-run; re-replaying", entries.len());
        Ok(true)
    }

    pub fn remove_recovery_rewind_marker(&self) {
        if let Err(e) = std::fs::remove_file(self.recovery_rewind_path())
            && e.kind() != std::io::ErrorKind::NotFound
        {
            warn!("failed to remove recovery rewind marker: {}", e);
        }
    }

    /// Read the cursor snapshot if present. Returns None on missing/parse/version
    /// mismatch so the boot path falls through to Delta reconciliation.
    pub fn load_cursor_snapshot(&self) -> Option<CursorSnapshot> {
        let snap = read_cursor_snapshot(&self.data_dir)?;
        if snap.shards_per_topic != self.shards_per_topic {
            warn!("cursor snapshot shards_per_topic {} != current {} — ignoring (config changed)", snap.shards_per_topic, self.shards_per_topic);
            return None;
        }
        // 24h is well past the normal restart cadence; an older snapshot
        // usually means the file was ported across hosts, the system clock
        // moved backward, or the process was offline for an extended window.
        // Surface it but still trust the snapshot — clean_shutdown is the
        // gate, age is informational.
        const STALE_AFTER_MICROS: i64 = 24 * 3600 * 1_000_000;
        let age_micros = crate::clock::now_micros().saturating_sub(snap.written_at_micros);
        if age_micros > STALE_AFTER_MICROS {
            warn!(
                "cursor snapshot is unusually old: age={}h, clean_shutdown={} — check for clock skew, ported data dir, or long downtime",
                age_micros / 3_600_000_000,
                snap.clean_shutdown
            );
        }
        Some(snap)
    }

    /// Fast-forward walrus persisted-read cursors from a loaded snapshot.
    /// Returns the number of *tables* where at least one shard moved (not the
    /// total shard-advance count — that's per-call via
    /// [`merge_persisted_positions`]).
    pub fn restore_cursor_snapshot(&self, snap: &CursorSnapshot) -> Result<usize, WalError> {
        let mut tables_advanced = 0usize;
        for (topic, snapshot_positions) in &snap.entries {
            let Some((project_id, table_name)) = Self::parse_topic(topic) else { continue };
            if snapshot_positions.len() != self.shards_per_topic {
                // load_cursor_snapshot already rejects whole-file mismatches;
                // hitting this means a per-entry corruption. Surface it so a
                // future "why didn't this table restore?" investigation has
                // a thread to pull on.
                warn!(
                    "cursor snapshot entry for {}/{} has {} shards but topic has {} — skipping",
                    project_id,
                    table_name,
                    snapshot_positions.len(),
                    self.shards_per_topic
                );
                continue;
            }
            // Seed `known_topics` so a later list_topic_pairs() includes a
            // table that hasn't yet been re-touched in this process.
            self.persist_topic(topic);

            let candidate: Vec<Option<WalPosition>> = snapshot_positions.iter().map(|p| p.map(snap_to_pos)).collect();
            if self.merge_persisted_positions(&project_id, &table_name, &candidate)? > 0 {
                tables_advanced += 1;
            }
        }
        Ok(tables_advanced)
    }

    /// Fast-forward each shard's persisted-read cursor to `candidate[shard]`
    /// when the candidate is strictly ahead. Returns the number of shards
    /// that moved. Shared by snapshot restore and Delta-derived reconciliation.
    pub fn merge_persisted_positions(&self, project_id: &str, table_name: &str, candidate: &[Option<WalPosition>]) -> Result<usize, WalError> {
        if candidate.len() != self.shards_per_topic {
            return Ok(0);
        }
        let local = self.persisted_read_positions(project_id, table_name).unwrap_or_else(|_| vec![None; self.shards_per_topic]);
        let mut to_set: Vec<WalPosition> = local.iter().map(|p| p.unwrap_or(WalPosition::ORIGIN)).collect();
        let mut advanced = 0usize;
        for shard in 0..self.shards_per_topic {
            let Some(cand) = candidate[shard] else { continue };
            let ahead = local[shard].map_or(!cand.is_origin(), |lpos| cand > lpos);
            if ahead {
                to_set[shard] = cand;
                advanced += 1;
            }
        }
        if advanced > 0 {
            self.set_persisted_positions(project_id, table_name, &to_set)?;
        }
        Ok(advanced)
    }

    /// Configured number of walrus collections per logical topic. Reported
    /// out for `timefusion.stats()` so operators can see effective parallelism.
    pub fn shards_per_topic(&self) -> usize {
        self.shards_per_topic
    }

    /// Number of registered logical topics (one per (project, table) pair),
    /// independent of shard count.
    pub fn known_topic_count(&self) -> usize {
        self.known_topics.len()
    }

    /// Returns WAL file count and total size in bytes by scanning the data directory.
    pub fn wal_stats(&self) -> (usize, u64) {
        let mut file_count = 0usize;
        let mut total_bytes = 0u64;
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata()
                    && meta.is_file()
                {
                    file_count += 1;
                    total_bytes += meta.len();
                }
            }
        }
        (file_count, total_bytes)
    }
}

pub(crate) fn serialize_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, WalError> {
    let mut buf = Vec::with_capacity(batch.get_array_memory_size() + 1024);
    {
        let mut w = StreamWriter::try_new_with_options(&mut buf, batch.schema_ref(), IpcWriteOptions::default())?;
        w.write(batch)?;
        w.finish()?;
    }
    Ok(buf)
}

/// Serialize `batch` into one or more independently-replayable IPC payloads:
/// each within `target` bytes where row-boundary splitting allows, never over
/// `hard_max` (the replay acceptance bound — appending past it would ack a
/// write the next boot silently drops; 2026-07-08: a 121MB INSERT quarantined
/// as `insert_corrupt`). Splits linearly: one serialize sizes the batch, the
/// chunk count is derived from it, and each row-chunk is compacted (so sliced
/// view/offset buffers are privatized and the IPC size actually shrinks) and
/// serialized exactly once — the parent's IPC bytes are dropped before the
/// chunks serialize, keeping the transient footprint (which the insert path's
/// reservation does NOT cover) near ~2x the batch's IPC size. A single row
/// over `target` passes through whole (inserts are row-independent, so every
/// chunk is a complete self-contained IPC stream); a single row over
/// `hard_max` can't be stored — explicit error, surfaced at append time.
fn split_to_wal_payloads(batch: &RecordBatch, target: usize, hard_max: usize) -> Result<Vec<Vec<u8>>, WalError> {
    let data = serialize_record_batch(batch)?;
    if data.len() <= target {
        return Ok(vec![data]);
    }
    if batch.num_rows() <= 1 {
        return if data.len() <= hard_max { Ok(vec![data]) } else { Err(WalError::BatchTooLarge { size: data.len(), max: hard_max }) };
    }
    // Row-slicing can't shrink dictionary columns (every IPC stream carries
    // the full dictionary, and compact_batch has no Dictionary arm), so
    // flatten them to their value types first — otherwise the split
    // degenerates toward one near-full-size entry per row. Flattening
    // replicates values per row, so re-measure: the chunk math and the
    // shrink-bail below must use the size that will actually be sliced.
    let (batch, parent_len) = match flatten_dictionary_columns(batch)? {
        Some(flat) => {
            let len = serialize_record_batch(&flat)?.len();
            (flat, len)
        }
        None => (batch.clone(), data.len()),
    };
    drop(data);
    // +1 chunk of headroom absorbs row-size skew without a second pass.
    let chunks = parent_len.div_ceil(target) + 1;
    let rows_per = batch.num_rows().div_ceil(chunks).max(1);
    let mut out = Vec::with_capacity(chunks);
    let mut start = 0;
    while start < batch.num_rows() {
        let len = rows_per.min(batch.num_rows() - start);
        let chunk = crate::mem_buffer::compact_batch(batch.slice(start, len));
        let chunk_data = serialize_record_batch(&chunk)?;
        if chunk_data.len() <= target || len <= 1 {
            if chunk_data.len() > hard_max {
                return Err(WalError::BatchTooLarge { size: chunk_data.len(), max: hard_max });
            }
            out.push(chunk_data);
        } else if chunk_data.len().saturating_mul(3) >= parent_len.saturating_mul(2) {
            // The chunk barely shrank despite holding a fraction of the rows:
            // some payload is shared across rows and row-slicing can't divide
            // it. Bail explicitly rather than recurse toward a per-row
            // explosion of near-full-size entries.
            return Err(WalError::BatchTooLarge { size: chunk_data.len(), max: target });
        } else {
            // Skewed rows left this chunk over target — re-split just it.
            drop(chunk_data);
            out.extend(split_to_wal_payloads(&chunk, target, hard_max)?);
        }
        start += len;
    }
    Ok(out)
}

/// Cast top-level dictionary columns to their value types (`None` when the
/// batch has no dictionary columns). Dictionary batches reach the WAL only
/// via gRPC ingest (client-supplied Arrow IPC is forwarded with no schema
/// coercion), and their shared dictionary defeats row-boundary splitting —
/// see `split_to_wal_payloads`.
fn flatten_dictionary_columns(batch: &RecordBatch) -> Result<Option<RecordBatch>, WalError> {
    use arrow::datatypes::{DataType, Field};
    if !batch.schema().fields().iter().any(|f| matches!(f.data_type(), DataType::Dictionary(_, _))) {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(batch.num_columns());
    let mut cols = Vec::with_capacity(batch.num_columns());
    for (f, c) in batch.schema().fields().iter().zip(batch.columns()) {
        match f.data_type() {
            DataType::Dictionary(_, value_type) => {
                cols.push(arrow::compute::cast(c, value_type)?);
                fields.push(Field::new(f.name(), (**value_type).clone(), f.is_nullable()));
            }
            _ => {
                cols.push(c.clone());
                fields.push((**f).clone());
            }
        }
    }
    Ok(Some(RecordBatch::try_new(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)), cols)?))
}

pub(crate) fn deserialize_record_batch(data: &[u8]) -> Result<RecordBatch, WalError> {
    if data.len() > MAX_BATCH_SIZE {
        return Err(WalError::BatchTooLarge { size: data.len(), max: MAX_BATCH_SIZE });
    }
    let mut reader = StreamReader::try_new(std::io::Cursor::new(data), None)?;
    match reader.next() {
        Some(batch) => Ok(batch?),
        None => Err(WalError::EmptyBatch),
    }
}

fn serialize_wal_entry(entry: &WalEntry) -> Result<Vec<u8>, WalError> {
    let mut buffer = WAL_MAGIC.to_vec();
    buffer.push(WAL_VERSION);
    buffer.push(entry.operation as u8);
    buffer.extend(bincode::encode_to_vec(entry, BINCODE_CONFIG)?);
    Ok(buffer)
}

fn deserialize_wal_entry(data: &[u8]) -> Result<WalEntry, WalError> {
    if data.len() < 5 {
        return Err(WalError::TooShort { len: data.len() });
    }

    if data[0..4] != WAL_MAGIC {
        return Err(WalError::UnsupportedVersion { version: data[0], expected: WAL_VERSION });
    }
    if data.len() < 6 || data[4] != WAL_VERSION {
        return Err(WalError::UnsupportedVersion { version: data[4], expected: WAL_VERSION });
    }
    WalOperation::try_from(data[5])?;
    let (entry, _): (WalEntry, _) = bincode::decode_from_slice(&data[6..], BINCODE_CONFIG)?;
    Ok(entry)
}

/// Decode any bincode DML payload (Delete/Update/UpdateWithSource) from WAL bytes.
pub fn decode_payload<T: Decode<()>>(data: &[u8]) -> Result<T, WalError> {
    let (payload, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
    Ok(payload)
}

/// See [`WalManager::replay_iter`]. Heap is keyed by `(timestamp, shard)` so
/// smaller timestamps come out first; shard index breaks ties
/// deterministically. The entry payload travels in a parallel Vec slot
/// indexed by shard, avoiding an `Ord` bound on `WalEntry`. Invariant: at
/// most one in-flight entry per shard is alive at a time → replay memory is
/// O(shards_per_topic), not O(total entries).
pub struct WalReplayIter<'a> {
    wal: &'a WalManager,
    topics: Vec<String>,
    topic_idx: usize,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(i64, usize)>>,
    shard_keys: Vec<String>,
    pending: Vec<Option<(WalEntry, WalPosition)>>,
    /// The (project, table) currently being drained (last topic primed).
    cur_topic: Option<(String, String)>,
    /// Entries yielded so far.
    pub total: u64,
    /// Corrupt/unreadable entries skipped so far.
    pub errors: usize,
}

impl WalReplayIter<'_> {
    /// The topic currently being replayed and its per-shard *frontier* — the
    /// position of the next entry each shard will yield (`None` = that shard is
    /// exhausted). Everything strictly before `frontier[shard]` on that shard
    /// has already been yielded AND processed; the entry AT `frontier[shard]`
    /// (the prefetched `pending` entry) has not. This is the safe watermark
    /// baseline for the in-progress topic: the walrus read cursor sits one
    /// prefetched entry per shard *ahead* of it. `None` topic before the first
    /// `next_entry`.
    pub fn frontier(&self) -> (Option<(String, String)>, Vec<Option<WalPosition>>) {
        (self.cur_topic.clone(), self.pending.iter().map(|p| p.as_ref().map(|(_, pos)| *pos)).collect())
    }

    /// Yields `(entry, shard, position)` — `position` is the entry's WAL
    /// position on its `shard`, so recovery can pin the buffered bucket at its
    /// real WAL position (resumable replay) instead of a conservative floor.
    pub fn next_entry(&mut self) -> Option<(WalEntry, usize, WalPosition)> {
        use std::cmp::Reverse;
        loop {
            if let Some(Reverse((_, shard))) = self.heap.pop() {
                let (entry, pos) = self.pending[shard].take().expect("heap and pending out of sync");
                self.total += 1;
                if let Some(next) = WalManager::next_from_shard(&self.wal.wal, &self.shard_keys[shard], true, &mut self.errors) {
                    self.heap.push(Reverse((next.0.timestamp_micros, shard)));
                    self.pending[shard] = Some(next);
                }
                return Some((entry, shard, pos));
            }
            // Current topic exhausted — prime the next parseable topic.
            let (project_id, table_name) = loop {
                let topic = self.topics.get(self.topic_idx)?;
                self.topic_idx += 1;
                if let Some(pair) = WalManager::parse_topic(topic) {
                    break pair;
                }
            };
            let shards = self.wal.shards_per_topic;
            self.shard_keys = (0..shards).map(|s| WalManager::walrus_topic_key(&project_id, &table_name, s)).collect();
            self.cur_topic = Some((project_id.clone(), table_name.clone()));
            self.pending = (0..shards).map(|_| None).collect();
            for shard in 0..shards {
                if let Some(next) = WalManager::next_from_shard(&self.wal.wal, &self.shard_keys[shard], true, &mut self.errors) {
                    self.heap.push(Reverse((next.0.timestamp_micros, shard)));
                    self.pending[shard] = Some(next);
                }
            }
        }
    }
}

pub(crate) fn cursor_snapshot_path_in(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join(".timefusion_meta").join("cursor_snapshot.json")
}

/// Atomic file write via tmp + rename. `durable` additionally fsyncs the file
/// before the rename and the parent dir after — required whenever the content
/// authorizes destructive action (rewind marker, drained-flag consumption);
/// pure hint files (post-flush cursor snapshot) skip the syncs.
fn write_atomic(target: &std::path::Path, bytes: &[u8], durable: bool) -> std::io::Result<()> {
    use std::io::Write;
    let tmp = target.with_extension("json.tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        if durable {
            f.sync_all()?;
        }
    }
    std::fs::rename(&tmp, target)?;
    if durable
        && let Some(dir) = target.parent()
        && let Ok(d) = std::fs::File::open(dir)
    {
        let _ = d.sync_all();
    }
    Ok(())
}

/// Read + parse + version-check the cursor snapshot. Free function so the
/// pre-walrus boot path can use it; `WalManager::load_cursor_snapshot` adds
/// the shard-count and staleness checks on top.
fn read_cursor_snapshot(wal_dir: &std::path::Path) -> Option<CursorSnapshot> {
    let path = cursor_snapshot_path_in(wal_dir);
    let bytes = std::fs::read(&path).ok()?;
    let snap: CursorSnapshot = match serde_json::from_slice(&bytes) {
        Ok(s) => s,
        Err(e) => {
            warn!("cursor snapshot at {:?} unreadable, falling back to Delta scan: {}", path, e);
            return None;
        }
    };
    if snap.version != SNAPSHOT_VERSION {
        warn!("cursor snapshot version {} != {} — ignoring", snap.version, SNAPSHOT_VERSION);
        return None;
    }
    Some(snap)
}

/// Pre-walrus boot WAL GC, shared by `main.rs` and the e2e `bootstrap()`.
///
/// Deletes dead files before walrus enumerates the dir (accumulated leaks
/// dominated startup — 467 GB / 12-min boot, see `wal_bloat_startup.md`).
/// A pure-mtime sweep is sound ONLY when the previous life's shutdown flush
/// fully drained (snapshot `drained=true`): otherwise the old files may BE
/// the un-flushed backlog (2026-07-08 acked-write loss). After sweeping, the
/// drained claim is consumed (rewritten false): this life will accept new
/// acked writes, and if it crashes before its first successful flush the
/// stale claim must not authorize the NEXT boot's sweep. Dirty/undrained
/// boot: skip — the floor-aware runtime sweep (first pass right after replay
/// parks the cursors) reclaims instead.
pub fn boot_wal_gc(wal_dir: &std::path::Path, max_age: std::time::Duration) {
    let t = std::time::Instant::now();
    let Some(mut snap) = read_cursor_snapshot(wal_dir).filter(|s| s.drained) else {
        info!("bootstrap.phase=wal_gc skipped=not_drained (runtime sweep reclaims post-replay)");
        return;
    };
    // Consume the drained claim FIRST and DURABLY (sync file + dir, matching
    // the rewind marker — the WAL data itself is msync'd within 200ms, so an
    // un-fsynced flag would be the weakest link in a deletion authorization):
    // sweep-then-consume fails open — a power loss reverting the un-fsynced
    // rewrite would resurrect drained=true for the next boot's unsound sweep
    // after this boot's deletions already persisted.
    snap.drained = false;
    let target = cursor_snapshot_path_in(wal_dir);
    let consume = serde_json::to_vec(&snap).map_err(std::io::Error::other).and_then(|bytes| write_atomic(&target, &bytes, true));
    if let Err(e) = consume {
        // Fail closed: without a durable consume the authorization must not
        // be used. Try to remove the stale claim entirely (costs one Delta
        // scan, never correctness); if even that fails, surface it loudly —
        // the un-swept dir only costs startup time, and the next boot
        // re-attempts.
        warn!("bootstrap.phase=wal_gc could not consume drained flag ({e}) — skipping sweep, deleting snapshot");
        if let Err(rm) = std::fs::remove_file(&target)
            && rm.kind() != std::io::ErrorKind::NotFound
        {
            error!(
                "bootstrap.phase=wal_gc stale drained=true snapshot could not be removed ({rm}) — \
                 delete {:?} manually before the next restart or the boot sweep may delete un-flushed WAL",
                target
            );
        }
        return;
    }
    match gc_wal_files(wal_dir, max_age, None) {
        Ok((deleted, bytes_freed)) => info!("bootstrap.phase=wal_gc deleted={deleted} bytes_freed={bytes_freed} elapsed_ms={}", t.elapsed().as_millis()),
        Err(e) => warn!("bootstrap.phase=wal_gc error={e} elapsed_ms={}", t.elapsed().as_millis()),
    }
}

/// Slack subtracted from the durability floor before it bounds GC: covers
/// the insert path's append→bucket-record window and mtime granularity.
///
/// ASSUMPTION: the wall clock never steps BACKWARD by more than this slack.
/// A larger backward step (hard NTP step, VM pause/resume) can rewrite a
/// shared active file's mtime below `floor − slack` while it still holds
/// pre-step un-flushed entries, defeating both cutoff arms (2026-07-08
/// review). NTP slew is fine; if hosts can hard-step >10min, widen this or
/// move to position-based deletability (file deletable iff every entry ≤
/// every shard's persisted cursor).
const GC_FLOOR_SLACK_MICROS: i64 = 10 * 60 * 1_000_000;

/// Delete WAL files older than `max_age` by mtime, recursing into subdirs.
/// Skips dotfiles/dotdirs (`.timefusion_meta/`).
///
/// Why this exists: `walrus-rust`'s GC bookkeeping (`FileStateTracker`) is an
/// in-process `HashMap`. On restart every file walrus wrote previously is
/// invisible to its delete predicate, so files leak forever. Prod was at
/// 467 GB of orphaned WAL on 2026-06-09 (12-min startup); see memory
/// `wal_bloat_startup.md`. mtime is a sufficient proxy for a file's newest
/// entry — walrus rotates to a new file once one is fully allocated.
///
/// `unflushed_floor_micros` makes the age heuristic sound: mtime age alone
/// assumed "old ⇒ already flushed", which deleted un-flushed backlog during
/// crash loops / flush wedges (the 2026-07-08 acked-write loss). Callers pass
/// the oldest WAL-append time any un-flushed data may depend on
/// (`BufferedWriteLayer::oldest_unflushed_wal_append_micros`); no file at or
/// after `floor − slack` is deleted, whatever its age. `None` = no un-flushed
/// data ⇒ pure mtime.
pub fn gc_wal_files(wal_dir: &std::path::Path, max_age: std::time::Duration, unflushed_floor_micros: Option<i64>) -> std::io::Result<(u64, u64)> {
    use std::time::SystemTime;
    let mut cutoff = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
    if let Some(floor) = unflushed_floor_micros {
        let floor_ts = SystemTime::UNIX_EPOCH + std::time::Duration::from_micros(floor.saturating_sub(GC_FLOOR_SLACK_MICROS).max(0) as u64);
        cutoff = cutoff.min(floor_ts);
    }
    let mut deleted = 0u64;
    let mut bytes_freed = 0u64;
    let mut stack: Vec<PathBuf> = vec![wal_dir.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        for entry in rd.flatten() {
            let name = entry.file_name();
            if name.to_string_lossy().starts_with('.') {
                continue;
            }
            let path = entry.path();
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if meta.is_dir() {
                stack.push(path);
                continue;
            }
            let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
            if modified < cutoff {
                let size = meta.len();
                match std::fs::remove_file(&path) {
                    Ok(()) => {
                        deleted += 1;
                        bytes_freed += size;
                    }
                    Err(e) => warn!("wal gc: failed to remove {}: {}", path.display(), e),
                }
            }
        }
    }
    Ok((deleted, bytes_freed))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array, StringViewArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("name", DataType::Utf8View, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(StringViewArray::from(vec!["a", "b", "c"]))]).unwrap()
    }

    #[test]
    fn test_record_batch_serialization() {
        let batch = create_test_batch();
        let serialized = serialize_record_batch(&batch).unwrap();
        let deserialized = deserialize_record_batch(&serialized).unwrap();
        assert_eq!(batch.num_rows(), deserialized.num_rows());
        assert_eq!(batch.num_columns(), deserialized.num_columns());
    }

    // Prod 2026-06-11 night: WAL replay of 6,546 entries charged 772.5GB
    // (~118MB each ≈ 89 cols × ~1.3MB message). Arrow IPC decode reads the
    // whole message body into one allocation and hands every column a slice
    // of it, so each column's `Buffer::capacity()` reports the full body —
    // a replayed batch is charged ~n_cols × message size unless the buffers
    // are privatized before entering a bucket.
    #[test]
    fn replayed_batch_charged_logical_not_message_body() {
        let n_cols = 30;
        let n_rows = 50;
        let payload: Vec<String> = (0..n_rows).map(|i| format!("{i:0>100}")).collect();
        let mut fields = vec![Field::new("ts", DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())), false)];
        fields.extend((0..n_cols).map(|i| Field::new(format!("c{i}"), if i % 2 == 0 { DataType::Utf8View } else { DataType::Utf8 }, true)));
        let ts = chrono::Utc::now().timestamp_micros();
        let mut cols: Vec<ArrayRef> = vec![Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![ts; n_rows]).with_timezone("UTC"))];
        let strs: Vec<&str> = payload.iter().map(|s| s.as_str()).collect();
        cols.extend((0..n_cols).map(|i| -> ArrayRef {
            if i % 2 == 0 { Arc::new(StringViewArray::from(strs.clone())) } else { Arc::new(arrow::array::StringArray::from(strs.clone())) }
        }));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), cols).unwrap();

        let bytes = serialize_record_batch(&batch).unwrap();
        let replayed = deserialize_record_batch(&bytes).unwrap();

        let buffer = crate::mem_buffer::MemBuffer::new();
        buffer.insert("p1", "t1", replayed, ts).unwrap();
        let charged = buffer.estimated_memory_bytes();
        // logical ≈ 30 cols × 50 rows × 100B ≈ 150KB; without privatization
        // each column charges the ~190KB message body (~5.7MB total).
        assert!(charged < 1024 * 1024, "replayed ~150KB-logical batch charged {charged} bytes — IPC message-body slices are leaking into accounting");
    }

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry {
            timestamp_micros: 1234567890,
            project_id: "project-123".to_string(),
            table_name: "test_table".to_string(),
            operation: WalOperation::Insert,
            data: vec![1, 2, 3, 4, 5],
        };
        let serialized = serialize_wal_entry(&entry).unwrap();
        let deserialized = deserialize_wal_entry(&serialized).unwrap();
        assert_eq!(entry.timestamp_micros, deserialized.timestamp_micros);
        assert_eq!(entry.project_id, deserialized.project_id);
        assert_eq!(entry.table_name, deserialized.table_name);
        assert_eq!(entry.operation, deserialized.operation);
        assert_eq!(entry.data, deserialized.data);
    }

    #[test]
    fn test_delete_payload_serialization() {
        let payload = DeletePayload { predicate_sql: Some("id = 1".to_string()) };
        let serialized = bincode::encode_to_vec(&payload, BINCODE_CONFIG).unwrap();
        let deserialized = decode_payload::<DeletePayload>(&serialized).unwrap();
        assert_eq!(payload.predicate_sql, deserialized.predicate_sql);

        let payload_none = DeletePayload { predicate_sql: None };
        let serialized_none = bincode::encode_to_vec(&payload_none, BINCODE_CONFIG).unwrap();
        let deserialized_none = decode_payload::<DeletePayload>(&serialized_none).unwrap();
        assert_eq!(payload_none.predicate_sql, deserialized_none.predicate_sql);
    }

    /// 2026-07-08 prod incident: a 121MB acked INSERT sat in the WAL until the
    /// next boot, where replay's `deserialize_record_batch` size cap rejected
    /// it → quarantined → the acked write silently dropped. The cap must be
    /// enforced at APPEND time (by splitting), so every acked entry is
    /// replayable by construction.
    #[serial_test::serial]
    #[test]
    fn oversized_insert_append_survives_replay() {
        let dir = tempfile::tempdir().unwrap();
        let _env = crate::test_utils::test_helpers::walrus_env_guard(dir.path());
        let table = format!("big_{}", uuid::Uuid::new_v4().simple());
        let wal = WalManager::with_fsync_mode_and_shards(dir.path().to_path_buf(), crate::config::WalFsyncMode::None, 2).unwrap();

        // ~112MB of string payload (35k rows × 3.2KB) — over WAL_SPLIT_TARGET,
        // mirroring the prod 121MB / 39k-row entry.
        let n_rows = 35_000;
        let strs: Vec<String> = (0..n_rows).map(|i| format!("{i:0>3200}")).collect();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
            vec![Arc::new(arrow::array::StringArray::from(strs.iter().map(|s| s.as_str()).collect::<Vec<_>>()))],
        )
        .unwrap();

        wal.append_batch("proj", &table, std::slice::from_ref(&batch), |_, _| {}).unwrap();

        let (entries, errors) = wal.read_entries_raw("proj", &table, None, true).unwrap();
        assert_eq!(errors, 0);
        let mut rows = 0usize;
        for e in &entries {
            let b = deserialize_record_batch(&e.data).expect("every acked WAL entry must be replayable (within the size cap)");
            rows += b.num_rows();
        }
        assert_eq!(rows, n_rows, "no acked rows may be lost across the WAL round-trip");
        assert!(entries.len() > 1, "an over-cap batch must have been split into multiple entries");
    }

    /// The splitter's contract at small scale: multi-row batches split to
    /// payloads within `target`, rows preserved in order; a single row over
    /// `target` but under `hard_max` passes through whole (unsplittable but
    /// replayable); a single row over `hard_max` is an explicit error.
    #[test]
    fn split_to_wal_payloads_bounds_every_entry() {
        use arrow::array::Array;
        let make = |strs: Vec<String>| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
                vec![Arc::new(arrow::array::StringArray::from(strs.iter().map(|s| s.as_str()).collect::<Vec<_>>()))],
            )
            .unwrap()
        };
        let (target, hard_max) = (8 * 1024, 32 * 1024);
        let batch = make((0..100).map(|i| format!("{i:0>200}")).collect());
        let payloads = split_to_wal_payloads(&batch, target, hard_max).unwrap();
        assert!(payloads.len() > 1);
        let mut rows: Vec<String> = Vec::new();
        for p in &payloads {
            assert!(p.len() <= target, "payload {} bytes exceeds target {target}", p.len());
            let b = deserialize_record_batch(p).unwrap();
            let col = b.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            rows.extend((0..col.len()).map(|i| col.value(i).to_string()));
        }
        assert_eq!(rows, (0..100).map(|i| format!("{i:0>200}")).collect::<Vec<_>>(), "rows preserved in order");

        let fat_row = make(vec!["x".repeat(16 * 1024)]);
        let whole = split_to_wal_payloads(&fat_row, target, hard_max).unwrap();
        assert_eq!(whole.len(), 1, "a single row between target and hard cap must pass through whole");
        assert_eq!(deserialize_record_batch(&whole[0]).unwrap().num_rows(), 1);

        let too_fat_row = make(vec!["x".repeat(64 * 1024)]);
        assert!(
            matches!(split_to_wal_payloads(&too_fat_row, target, hard_max), Err(WalError::BatchTooLarge { .. })),
            "a single row over the hard cap must fail the append explicitly, not ack-then-drop"
        );
    }

    /// Dictionary-encoded columns (reachable via gRPC ingest, which forwards
    /// client Arrow IPC verbatim) defeat row-boundary splitting: every IPC
    /// stream carries the full dictionary, so halving rows doesn't halve
    /// bytes and the pre-fix splitter degenerated to one near-full-size
    /// entry per row (a 39k-row batch with a 150MB dictionary → multi-TB
    /// append attempt). The splitter must flatten dictionaries first and
    /// still bound every payload.
    #[test]
    fn split_to_wal_payloads_flattens_dictionary_columns() {
        use arrow::array::{Array, DictionaryArray, Int32Array, StringArray};
        let values = StringArray::from((0..200).map(|i| format!("{i:0>2048}")).collect::<Vec<_>>());
        let keys = Int32Array::from((0..1000).map(|i| i % 200).collect::<Vec<i32>>());
        let dict = DictionaryArray::<arrow::datatypes::Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("body", dict.data_type().clone(), false)])), vec![Arc::new(dict)]).unwrap();

        let (target, hard_max) = (64 * 1024, 1024 * 1024);
        let payloads = split_to_wal_payloads(&batch, target, hard_max).expect("dictionary batch must split, not explode or bail");
        assert!(payloads.len() > 1);
        // Way fewer entries than rows — the pre-fix splitter emitted ~1 per row.
        assert!(payloads.len() < 100, "split degenerated toward per-row entries: {} payloads", payloads.len());
        let mut rows = 0usize;
        for p in &payloads {
            assert!(p.len() <= target, "payload {} bytes exceeds target {target}", p.len());
            let b = deserialize_record_batch(p).unwrap();
            rows += b.num_rows();
        }
        assert_eq!(rows, 1000, "no rows lost across the dictionary split");
    }

    /// UPDATE...FROM sources can't be split without changing join semantics —
    /// an over-cap source must fail the append (client-visible) instead of
    /// acking an entry the next boot's replay will reject.
    #[serial_test::serial]
    #[test]
    fn append_update_with_source_rejects_oversized_source() {
        let dir = tempfile::tempdir().unwrap();
        let _env = crate::test_utils::test_helpers::walrus_env_guard(dir.path());
        let wal = WalManager::with_fsync_mode_and_shards(dir.path().to_path_buf(), crate::config::WalFsyncMode::None, 2).unwrap();
        let source = SerializedSource { join_keys: vec![("id".to_string(), "id".to_string())], batch_ipc: vec![0u8; MAX_BATCH_SIZE + 1] };
        let res = wal.append_update_with_source("proj", "tbl", None, &[], &source, |_, _| {});
        assert!(matches!(res, Err(WalError::BatchTooLarge { .. })));
    }

    /// Stability anchor: `walrus_topic_key` must produce the same bytes across
    /// builds and library versions. A regression here silently strands WAL
    /// entries on upgrade — see WAL_VERSION 131/132 bump rationale.
    #[test]
    fn walrus_topic_key_is_stable() {
        let k = WalManager::walrus_topic_key("project", "table", 0);
        // 16-hex-char FNV-1a + "-00" suffix. Concrete value is pinned below;
        // shape check first so a regression reports a useful diff.
        assert_eq!(k.len(), 19, "key shape changed: {k}");
        assert!(k.ends_with("-00"));
        // Pinned values — update both lines together if the encoding changes,
        // and bump WAL_VERSION + document in the const's Bumps section.
        assert_eq!(WalManager::walrus_topic_key("project", "table", 0), "d8751a406eed3d9a-00");
        assert_eq!(WalManager::walrus_topic_key("p1", "otel_logs_and_spans", 3), "ae0768bab343abd1-03");
    }

    /// Collision guards: distinct (project_id, table_name) tuples must map
    /// to distinct walrus keys regardless of contents. Length-prefix
    /// encoding makes this hold even when one input embeds the separator.
    #[test]
    fn walrus_topic_key_no_collisions() {
        let pairs = [
            (("ab", "c"), ("a", "bc")),   // boundary slide
            (("a:b", "c"), ("a", "b:c")), // ':' inside an input — previously the failure mode
            (("a", ""), ("", "a")),       // empty / non-empty swap
            (("aa", ""), ("a", "a")),     // boundary slide with empty
        ];
        for ((p1, t1), (p2, t2)) in pairs {
            assert_ne!(WalManager::walrus_topic_key(p1, t1, 0), WalManager::walrus_topic_key(p2, t2, 0), "({p1:?},{t1:?}) and ({p2:?},{t2:?}) collide");
        }
    }

    /// `TIMEFUSION_WAL_ACK_FSYNC` plumbing: single-entry (DML) appends sync
    /// the shard before returning and stay readable. (True power-loss
    /// durability isn't unit-testable; this guards the sync_topic call path.)
    #[serial_test::serial]
    #[test]
    fn ack_fsync_appends_are_synced_and_readable() {
        let dir = tempfile::tempdir().unwrap();
        let _env = crate::test_utils::test_helpers::walrus_env_guard(dir.path());
        let table = format!("tbl_{}", uuid::Uuid::new_v4().simple());
        let wal = WalManager::with_fsync_mode_and_shards(dir.path().to_path_buf(), crate::config::WalFsyncMode::Milliseconds(60_000), 2)
            .unwrap()
            .with_ack_fsync(true);

        wal.append_delete("proj", &table, Some("id = 'x'"), |_, _| {}).unwrap();
        wal.append_batch("proj", &table, &[create_test_batch()], |_, _| {}).unwrap();

        // checkpoint=true: walrus's uncheckpointed read_next never advances
        // the cursor, so a read-all loop with `false` re-reads the first
        // entry forever (2026-07-08: hung the suite and OOM'd the host).
        let (entries, errors) = wal.read_entries_raw("proj", &table, None, true).unwrap();
        assert_eq!(errors, 0);
        assert_eq!(entries.len(), 2, "both appends must be present and readable with ack_fsync on");
        assert!(entries.iter().any(|e| e.operation == WalOperation::Delete));
        assert!(entries.iter().any(|e| e.operation == WalOperation::Insert));
    }

    /// Concurrent appends to a *single* topic must queue, not error. Walrus
    /// rejects concurrent appends to one collection ("another batch write
    /// already in progress"); `pick_shard` only spreads across
    /// `shards_per_topic`, so more concurrent writers than shards collide on a
    /// shard. The per-collection `append_lock` serializes them. Regression for
    /// the 2026-06-22 prod DLQ flood, where these errors dead-lettered live
    /// inserts under backfill concurrency.
    #[serial_test::serial]
    #[test]
    fn concurrent_appends_same_topic_do_not_error() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let dir = tempfile::tempdir().unwrap();
        // SAFETY: walrus reads its data dir from the process-global
        // WALRUS_DATA_DIR; #[serial] protects the mutation. Without this the
        // test inherits a prior test's now-dropped tempdir and walrus I/O fails.
        unsafe { std::env::set_var("WALRUS_DATA_DIR", dir.path()) };
        let table = format!("tbl_{}", uuid::Uuid::new_v4().simple());
        let wal = Arc::new(WalManager::with_fsync_mode_and_shards(dir.path().to_path_buf(), crate::config::WalFsyncMode::None, 4).unwrap());

        // Far more concurrent writers than the 4 shards → guaranteed same-shard
        // collisions under round-robin. Without `append_lock` walrus errors.
        let errors = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|s| {
            for _ in 0..32 {
                let (wal, table, errors) = (wal.clone(), table.clone(), errors.clone());
                s.spawn(move || {
                    let batch = create_test_batch();
                    let source = SerializedSource { join_keys: vec![("id".into(), "id".into())], batch_ipc: vec![1, 2, 3] };
                    for i in 0..8 {
                        // Interleave append_batch with append_update_with_source so a
                        // same-shard collision exercises both append paths' locking.
                        let res = if i % 2 == 0 {
                            wal.append_batch("proj", &table, std::slice::from_ref(&batch), |_, _| {}).map(|_| ())
                        } else {
                            wal.append_update_with_source("proj", &table, Some("id = 1"), &[("v".into(), "1".into())], &source, |_, _| {}).map(|_| ())
                        };
                        if res.is_err() {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(errors.load(Ordering::Relaxed), 0, "concurrent same-topic appends must queue, not error with 'another batch write already in progress'");
    }

    /// Rewind marker round-trip: capture P0, consume the cursor (simulating
    /// a crashed replay's checkpoint progress), apply the marker → cursor is
    /// back at P0 and the entry is readable again. Also: P0 for a
    /// never-persisted shard must come back as an explicit ORIGIN hold, and
    /// removing the marker is idempotent.
    #[test]
    #[serial_test::serial]
    fn recovery_rewind_marker_restores_consumed_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let table = format!("rw_{}", uuid::Uuid::new_v4().simple());
        let wal = WalManager::with_fsync_mode_and_shards(path, crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        wal.append("proj", &table, &create_test_batch()).unwrap();

        let p0 = wal.write_recovery_rewind_marker().unwrap();
        let holds = &p0[&("proj".to_string(), table.clone())];
        assert!(holds.iter().all(Option::is_some), "P0 must map never-persisted shards to explicit ORIGIN holds, got {holds:?}");

        // Simulate a crashed replay: consume to tail (persists progress).
        let (entries, _) = wal.read_entries_raw("proj", &table, None, true).unwrap();
        assert_eq!(entries.len(), 1);
        let (entries, _) = wal.read_entries_raw("proj", &table, None, true).unwrap();
        assert!(entries.is_empty(), "cursor must be at tail after consuming");

        assert!(wal.apply_recovery_rewind_marker().unwrap(), "marker must be found and applied");
        let (entries, _) = wal.read_entries_raw("proj", &table, None, true).unwrap();
        assert_eq!(entries.len(), 1, "rewind must make the consumed entry replayable again");

        wal.remove_recovery_rewind_marker();
        assert!(!wal.apply_recovery_rewind_marker().unwrap(), "marker gone after removal");
        wal.remove_recovery_rewind_marker(); // idempotent
    }

    /// Round-trip cursor snapshot: write, drop the manager, re-open, restore.
    /// Verifies the on-disk file is enough to seed walrus's known_topics on
    /// a fresh process without touching Delta — the whole point of the fast
    /// boot path.
    ///
    /// Scope note: this exercises the *idempotent* path — walrus's own fsync
    /// already persisted shard 0's advance to disk in Process A, so when
    /// Process B opens the same dir, restore finds nothing to advance and
    /// returns 0 tables. The rescue path (snapshot is ahead of walrus's
    /// own fsynced state) is covered by
    /// [`cursor_snapshot_restore_advances_walrus_past_local_state`].
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_roundtrip_restores_persisted_positions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        // Unique topic per run: walrus state lives under the process-global
        // WALRUS_DATA_DIR (whatever the last test pointed it at), so a fixed
        // "proj:tbl" topic inherits blocks/cursors appended by earlier tests
        // in the same process and the exact-position asserts below flake.
        let table = format!("tbl_{}", uuid::Uuid::new_v4().simple());

        // Process A: append, advance cursor, write snapshot with clean flag.
        {
            let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
            let batch = create_test_batch();
            wal.append("proj", &table, &batch).unwrap();
            // Advance shard 0 (the only shard written — round-robin picks it
            // first for an unseen topic) to the write tail, watermark-style.
            let tail = wal.current_position("proj", &table).unwrap();
            wal.merge_persisted_positions("proj", &table, &[Some(tail[0]), None, None, None]).unwrap();
            let before = wal.persisted_read_positions("proj", &table).unwrap();
            assert!(before[0].is_some_and(|p| !p.is_origin()), "advance must move shard 0 off origin");

            wal.write_cursor_snapshot(true, true).unwrap();
            assert!(path.join(".timefusion_meta/cursor_snapshot.json").exists());
        }

        // Process B: fresh manager, snapshot present, no walrus state mutation
        // beyond what restore does.
        {
            let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
            let snap = wal.load_cursor_snapshot().expect("snapshot loadable");
            assert!(snap.clean_shutdown);
            assert_eq!(snap.shards_per_topic, 4);
            assert!(snap.entries.contains_key(&WalManager::make_topic("proj", &table)));

            // Restore is idempotent — walrus state already reflects the
            // advance, so `restore` advances 0 shards but seeds known_topics.
            let advanced = wal.restore_cursor_snapshot(&snap).unwrap();
            assert_eq!(advanced, 0, "snapshot positions match walrus's own fsynced state");
            assert!(wal.list_topic_pairs().unwrap().iter().any(|(p, t)| p == "proj" && *t == table));
        }
    }

    /// Snapshot version mismatch (or a corrupted file) must return None so
    /// boot falls through to the Delta scan rather than misinterpreting the
    /// payload.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_rejects_version_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        // `.timefusion_meta/` is guaranteed by WalManager construction.
        std::fs::write(
            path.join(".timefusion_meta/cursor_snapshot.json"),
            br#"{"version":999,"written_at_micros":0,"shards_per_topic":4,"clean_shutdown":true,"entries":{}}"#,
        )
        .unwrap();
        assert!(wal.load_cursor_snapshot().is_none());
    }

    /// If an operator changes `TIMEFUSION_WAL_SHARDS_PER_TOPIC` between
    /// restarts, the snapshot's per-shard layout is incompatible. Reject it
    /// so the boot falls through to the Delta scan rather than restoring
    /// shard-misaligned positions.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_rejects_shard_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        // Unique topic per run — see cursor_snapshot_roundtrip_restores_persisted_positions.
        let table = format!("tbl_{}", uuid::Uuid::new_v4().simple());
        {
            let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
            wal.append("proj", &table, &create_test_batch()).unwrap();
            let tail = wal.current_position("proj", &table).unwrap();
            wal.merge_persisted_positions("proj", &table, &[Some(tail[0]), None, None, None]).unwrap();
            wal.write_cursor_snapshot(true, true).unwrap();
        }
        // Re-open with a different shard count — load must refuse.
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 8).unwrap();
        assert!(wal.load_cursor_snapshot().is_none());
    }

    /// Rescue path: walrus has no fsynced state for this topic (simulating a
    /// crash that lost the persisted cursor while the WAL files themselves
    /// survived). Restore from a hand-crafted snapshot pointing past origin
    /// must actually move walrus's persisted_read_position forward — this
    /// is the scenario the snapshot exists to handle, distinct from the
    /// idempotent path in `..._roundtrip_restores_persisted_positions`.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_restore_advances_walrus_past_local_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path, crate::config::WalFsyncMode::SyncEach, 4).unwrap();

        // Walrus uses a process-global `WALRUS_DATA_DIR` so other tests may
        // have seeded state for shared collection keys. Use a per-test
        // unique topic name so the hashed walrus key is guaranteed fresh.
        let table = format!("rescue_{}", uuid::Uuid::new_v4().simple());
        let project = "p";

        let before = wal.persisted_read_positions(project, &table).unwrap();
        assert!(before.iter().all(Option::is_none), "fresh walrus key must have no persisted cursor");

        let mut entries = std::collections::BTreeMap::new();
        entries.insert(WalManager::make_topic(project, &table), vec![Some((7u64, 42u64)), None, Some((3, 0)), None]);
        let snap = CursorSnapshot { version: SNAPSHOT_VERSION, written_at_micros: 0, shards_per_topic: 4, clean_shutdown: true, drained: false, entries };
        let tables_advanced = wal.restore_cursor_snapshot(&snap).unwrap();
        assert_eq!(tables_advanced, 1, "the one snapshot table must advance from origin");

        let after = wal.persisted_read_positions(project, &table).unwrap();
        assert_eq!(after[0].map(|p| (p.block_id, p.offset)), Some((7, 42)));
        assert_eq!(after[2].map(|p| (p.block_id, p.offset)), Some((3, 0)));
    }

    /// On crash between `fs::write(tmp)` and `fs::rename(tmp, target)` we
    /// leave `cursor_snapshot.json.tmp` behind. The next WalManager init
    /// must sweep it so it doesn't accumulate over many crash-restart cycles.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_tmp_swept_on_init() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        // First init creates `.timefusion_meta/`.
        drop(WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap());
        let tmp = path.join(".timefusion_meta/cursor_snapshot.json.tmp");
        std::fs::write(&tmp, b"partial").unwrap();
        assert!(tmp.exists());
        drop(WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap());
        assert!(!tmp.exists(), "init must sweep leftover tmp file");
    }

    /// Worst case for `write_post_flush_snapshot`: the meta dir is
    /// read-only so the tmp write fails AND the subsequent
    /// `delete_cursor_snapshot` also fails (POSIX unlink needs write on the
    /// parent). The stale snapshot survives — documented in RUNBOOK.md as
    /// "Stale cursor snapshot." The unit invariant we lock in here is just
    /// that both calls return Err cleanly without panicking, so the flush
    /// task can carry on.
    #[cfg(unix)]
    #[test]
    #[serial_test::serial]
    fn write_and_delete_both_fail_under_readonly_meta_dir() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        wal.write_cursor_snapshot(true, true).unwrap();
        let meta = path.join(".timefusion_meta");
        let target = meta.join("cursor_snapshot.json");

        // Lock the meta dir: r-x only.
        let original = std::fs::metadata(&meta).unwrap().permissions();
        std::fs::set_permissions(&meta, std::fs::Permissions::from_mode(0o555)).unwrap();

        assert!(wal.write_cursor_snapshot(false, false).is_err(), "write into RO dir must fail");
        assert!(wal.delete_cursor_snapshot().is_err(), "unlink under RO parent must fail");
        assert!(target.exists(), "stale snapshot survives both failures");

        // Restore so tempdir teardown can clean up.
        std::fs::set_permissions(&meta, original).unwrap();
    }

    /// Simulates the BufferedWriteLayer write_post_flush_snapshot recovery
    /// path: if `write_cursor_snapshot` fails after a previous good write,
    /// the caller must remove the now-stale file so the next boot's shallow
    /// verifier doesn't trust it. Failure is forced by putting a directory
    /// in the spot the atomic-rename tmp would occupy — `fs::write` to a
    /// directory path errors, so the rename never happens.
    #[test]
    #[serial_test::serial]
    fn write_cursor_snapshot_failure_requires_caller_to_delete_stale_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        wal.write_cursor_snapshot(true, true).unwrap();
        let target = path.join(".timefusion_meta/cursor_snapshot.json");
        let tmp = path.join(".timefusion_meta/cursor_snapshot.json.tmp");
        assert!(target.exists());

        // Force the next write to fail by squatting on the tmp path with a dir.
        std::fs::create_dir(&tmp).unwrap();
        assert!(wal.write_cursor_snapshot(false, false).is_err(), "tmp-path collision must fail the write");
        assert!(target.exists(), "stale snapshot still on disk after failed write");

        // BufferedWriteLayer's recovery: delete the stale file.
        wal.delete_cursor_snapshot().unwrap();
        assert!(!target.exists(), "delete clears the stale snapshot");
    }

    /// `delete_cursor_snapshot` removes a present file and is a no-op when
    /// absent. The flush path calls this on write failure to keep boot from
    /// restoring stale state.
    #[test]
    #[serial_test::serial]
    fn delete_cursor_snapshot_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        // Missing → Ok.
        wal.delete_cursor_snapshot().unwrap();
        wal.write_cursor_snapshot(true, true).unwrap();
        assert!(path.join(".timefusion_meta/cursor_snapshot.json").exists());
        wal.delete_cursor_snapshot().unwrap();
        assert!(!path.join(".timefusion_meta/cursor_snapshot.json").exists());
        // Second call must still be Ok.
        wal.delete_cursor_snapshot().unwrap();
    }

    /// A snapshot written from the flush path (clean_shutdown=false) loads
    /// fine but must not let the boot path skip the Delta verifier — that
    /// gate is reserved for the graceful-shutdown marker.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_dirty_path_loads_but_signals_unclean() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        // Unique topic per run — see cursor_snapshot_roundtrip_restores_persisted_positions.
        let table = format!("tbl_{}", uuid::Uuid::new_v4().simple());
        wal.append("proj", &table, &create_test_batch()).unwrap();
        let tail = wal.current_position("proj", &table).unwrap();
        wal.merge_persisted_positions("proj", &table, &[Some(tail[0]), None, None, None]).unwrap();
        wal.write_cursor_snapshot(false, false).unwrap();

        let snap = wal.load_cursor_snapshot().expect("dirty snapshot must still be loadable");
        assert!(!snap.clean_shutdown, "dirty snapshot must not claim clean_shutdown");
        // Sanity: restore is still safe (idempotent on matching state).
        let tables_advanced = wal.restore_cursor_snapshot(&snap).unwrap();
        assert_eq!(tables_advanced, 0);
    }

    #[test]
    fn test_update_payload_serialization() {
        let payload = UpdatePayload { predicate_sql: Some("id = 1".to_string()), assignments: vec![("name".to_string(), "'updated'".to_string())] };
        let serialized = bincode::encode_to_vec(&payload, BINCODE_CONFIG).unwrap();
        let deserialized = decode_payload::<UpdatePayload>(&serialized).unwrap();
        assert_eq!(payload.predicate_sql, deserialized.predicate_sql);
        assert_eq!(payload.assignments, deserialized.assignments);
    }

    #[test]
    fn gc_wal_files_skips_meta_and_respects_cutoff() {
        use std::time::Duration;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // Layout mirrors prod: data files + a `.timefusion_meta/` sibling
        // that must survive both passes.
        std::fs::create_dir_all(root.join(".timefusion_meta")).unwrap();
        std::fs::write(root.join(".timefusion_meta/cursor_snapshot.json"), b"{}").unwrap();
        std::fs::write(root.join(".timefusion_meta/topics"), b"").unwrap();

        let f1 = root.join("1779989695814");
        let f2 = root.join("1780994113609");
        std::fs::write(&f1, vec![0u8; 1024]).unwrap();
        std::fs::write(&f2, vec![0u8; 2048]).unwrap();

        // Pass 1: max_age = 1h. Both files were created just now → kept.
        let (deleted, bytes_freed) = gc_wal_files(root, Duration::from_secs(3600), None).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(bytes_freed, 0);
        assert!(f1.exists() && f2.exists());

        // Pass 2: max_age = 0 → cutoff is "now" → every existing file is
        // strictly older than the cutoff and gets deleted, but `.timefusion_meta`
        // is exempt.
        let (deleted, bytes_freed) = gc_wal_files(root, Duration::ZERO, None).unwrap();
        assert_eq!(deleted, 2);
        assert_eq!(bytes_freed, 1024 + 2048);
        assert!(!f1.exists() && !f2.exists());
        assert!(root.join(".timefusion_meta/cursor_snapshot.json").exists(), "meta dir must be skipped");
        assert!(root.join(".timefusion_meta/topics").exists());
    }

    /// Regression: prod 2026-07-08. GC deleted files purely by mtime,
    /// assuming "old ⇒ already flushed" — during a crash loop the aged files
    /// WERE the un-flushed backlog. The durability floor must override age.
    #[test]
    fn gc_wal_files_respects_unflushed_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let f = root.join("1770000000000");
        std::fs::write(&f, vec![0u8; 512]).unwrap();

        // max_age=0 alone would delete it (mtime < now) — but un-flushed data
        // depends on appends from 1h ago, so the floor (minus slack) keeps
        // every file modified since then.
        let floor = chrono::Utc::now().timestamp_micros() - 3600 * 1_000_000;
        let (deleted, _) = gc_wal_files(root, std::time::Duration::ZERO, Some(floor)).unwrap();
        assert_eq!(deleted, 0, "file newer than the un-flushed floor must survive");
        assert!(f.exists());

        // A floor far in the future imposes no extra protection beyond age.
        let (deleted, _) = gc_wal_files(root, std::time::Duration::ZERO, Some(chrono::Utc::now().timestamp_micros() + 3600 * 1_000_000)).unwrap();
        assert_eq!(deleted, 1);
        assert!(!f.exists());
    }

    /// Boot GC gating (2026-07-08 review findings): `clean_shutdown=true` is
    /// NOT a drain claim — shutdown writes it even after a partial flush, so
    /// only `drained=true` may authorize the pure-mtime sweep. And the
    /// drained claim must be CONSUMED by the boot that uses it: a stale
    /// drained=true surviving a crash-without-flush life would authorize the
    /// next boot to delete that life's un-flushed backlog.
    #[test]
    fn boot_wal_gc_requires_drained_and_consumes_it() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let meta = root.join(".timefusion_meta");
        std::fs::create_dir_all(&meta).unwrap();
        let snap_path = meta.join("cursor_snapshot.json");
        // An old-mtime WAL file that may be the un-flushed backlog.
        let old_file = root.join("00.wal");
        let make_old_file = || {
            std::fs::write(&old_file, b"data").unwrap();
            let old = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
            let f = std::fs::File::options().append(true).open(&old_file).unwrap();
            f.set_times(std::fs::FileTimes::new().set_modified(old)).unwrap();
        };
        let write_snap = |clean: bool, drained: bool, version: u32| {
            let bytes = serde_json::to_vec(&CursorSnapshot {
                version,
                written_at_micros: 0,
                shards_per_topic: 4,
                clean_shutdown: clean,
                drained,
                entries: Default::default(),
            })
            .unwrap();
            std::fs::write(&snap_path, bytes).unwrap();
        };
        let max_age = std::time::Duration::from_secs(60);

        make_old_file();
        boot_wal_gc(root, max_age); // missing snapshot → skip
        std::fs::write(&snap_path, b"not json").unwrap();
        boot_wal_gc(root, max_age); // unreadable → skip
        write_snap(true, false, SNAPSHOT_VERSION);
        boot_wal_gc(root, max_age); // clean but NOT drained (partial flush) → skip
        write_snap(true, true, 999);
        boot_wal_gc(root, max_age); // version mismatch → skip
        assert!(old_file.exists(), "un-drained/unreadable snapshots must never authorize the mtime sweep");

        // drained=true authorizes exactly one sweep, then is consumed.
        write_snap(true, true, SNAPSHOT_VERSION);
        boot_wal_gc(root, max_age);
        assert!(!old_file.exists(), "drained snapshot must run the sweep");
        let reread: CursorSnapshot = serde_json::from_slice(&std::fs::read(&snap_path).unwrap()).unwrap();
        assert!(!reread.drained, "the drained claim must be consumed by the boot that used it");
        assert!(reread.clean_shutdown, "consuming drained must not clobber the cursor-restore flag");
        make_old_file();
        boot_wal_gc(root, max_age);
        assert!(old_file.exists(), "second boot must not reuse the consumed drained claim");

        // Fail-closed (2026-07-08 review finding 3): if the drained flag
        // cannot be durably consumed, the sweep must NOT run — sweeping
        // first would fail open (a power loss reverting the un-fsynced
        // rewrite resurrects the authorization after files are gone).
        write_snap(true, true, SNAPSHOT_VERSION);
        std::fs::create_dir_all(meta.join("cursor_snapshot.json.tmp")).unwrap(); // blocks File::create(tmp)
        boot_wal_gc(root, max_age);
        assert!(old_file.exists(), "sweep must be skipped when the drained consume fails");
        assert!(!snap_path.exists(), "unconsumable drained snapshot must be deleted (fail closed)");
    }

    #[test]
    fn gc_wal_files_handles_missing_dir() {
        // Pre-init sweep on a fresh deployment hits a not-yet-created dir;
        // must not error.
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        let (deleted, bytes_freed) = gc_wal_files(&missing, std::time::Duration::ZERO, None).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(bytes_freed, 0);
    }
}
