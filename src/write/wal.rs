use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use bincode::{Decode, Encode};
use dashmap::DashSet;
use fs4::fs_std::FileExt;
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
    #[error("Bad WAL magic: {got:02x?}")]
    BadMagic { got: [u8; 4] },
    /// Fatal: a process that cannot own the WAL must exit rather than linger half-started.
    #[error("{0}")]
    LockContention(String),
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

/// TimeFusion's own metadata directory alongside the walrus data files (topic
/// list, WAL version stamp, cursor snapshot, sidecars). Skipped by WAL GC.
pub const META_DIR: &str = ".timefusion_meta";
const TAKEOVER_REQUEST_FILE: &str = "takeover.request";

/// How long a contender waits for the WAL lock before exiting non-zero. Far
/// beyond any real handoff, so only a predecessor that will never release trips it.
const LOCK_WAIT_GIVE_UP: std::time::Duration = std::time::Duration::from_secs(900);

/// How long the holder tolerates an outstanding takeover request before taking
/// the (lossless) graceful shutdown path anyway.
pub const TAKEOVER_ESCALATE_AFTER: std::time::Duration = std::time::Duration::from_secs(180);

/// `<data_dir>/.timefusion_meta/<file>`.
pub fn meta_path(data_dir: &Path, file: &str) -> PathBuf {
    data_dir.join(META_DIR).join(file)
}

/// Remove `path`, treating "already absent" as success.
fn remove_if_exists(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Magic bytes identifying the WAL format ("WAL2").
const WAL_MAGIC: [u8; 4] = [0x57, 0x41, 0x4C, 0x32];
/// Bump on any breaking change to the on-disk WAL format or the walrus key
/// derivation. The startup version-stamp check refuses to open a directory
/// written by a different version, so existing data must be wiped on bump.
const WAL_VERSION: u8 = 1;
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
/// On-disk format version for `cursor_snapshot.json`. Bump on any breaking
/// schema change so older readers fall back to the Delta scan instead of
/// silently misinterpreting the file.
const SNAPSHOT_VERSION: u32 = 1;

/// `WalPosition` serialized as `(block_id, offset)`.
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
/// Correctness assumes this process is the **only** writer to its Delta tables.
/// If a parallel writer commits out of band, delete `cursor_snapshot.json` to
/// force a Delta reconciliation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorSnapshot {
    pub version: u32,
    /// Wall-clock micros at write time. Informational only — not a max-age gate.
    pub written_at_micros: i64,
    pub shards_per_topic: usize,
    /// Written by the graceful-shutdown path only. NOT a drain claim — shutdown
    /// writes it even after a partial/timed-out flush; see `drained` for that.
    pub clean_shutdown: bool,
    /// True only when the shutdown flush left NOTHING un-flushed. Sole authorizer
    /// of the pure-mtime boot WAL GC — with un-flushed data the old files may BE
    /// the backlog. Defaults to false for older snapshots (skips the sweep).
    #[serde(default)]
    pub drained: bool,
    /// `"project_id:table_name"` → per-shard cursor (None = never written).
    pub entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct ReclaimStateCounts {
    pub total: usize,
    pub eligible: usize,
    pub locked: usize,
    pub uncheckpointed: usize,
    pub open: usize,
}

/// Hard cap on a single WAL entry's batch payload — the replay acceptance bound
/// and the limit for unsplittable payloads. Ceiling is walrus's `MAX_ALLOC`
/// (1GiB/block), so raising it requires changing the vendored WAL engine.
const MAX_BATCH_SIZE: usize = 1024 * 1024 * 1024;
/// Append-side split target for INSERT batches; invisible to clients and to
/// Delta (flush re-coalesces per table). Each entry is read + Arrow-decoded
/// whole during recovery and a corrupted entry quarantines whole, so the unit
/// is kept small even though acceptance goes up to `MAX_BATCH_SIZE`.
const WAL_SPLIT_TARGET: usize = 100 * 1024 * 1024;

/// `Display`/`FromStr` are the on-disk spelling in the quarantine `.meta`
/// sidecar, so the re-drive filter is compiler-checked against a rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode, strum::FromRepr, strum::Display, strum::EnumString)]
#[repr(u8)]
pub enum WalOperation {
    Insert = 0,
    Delete = 1,
    Update = 2,
    /// `UPDATE ... FROM` with a materialized source RecordBatch serialized
    /// alongside the predicate/assignments.
    UpdateWithSource = 3,
}

impl TryFrom<u8> for WalOperation {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::from_repr(value).ok_or(WalError::InvalidOperation(value))
    }
}

#[derive(Debug, Encode, Decode)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id: String,
    pub table_name: String,
    pub operation: WalOperation,
    /// Must NOT be `#[bincode(with_serde)]`: serde encodes `Vec<u8>` as a
    /// sequence and decodes it element by element, which is ~800x slower here.
    /// Both encodings are byte-identical on disk (`compare_vec_u8_encodings`).
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

/// Stripe count for the per-collection append locks; far exceeds the realistic
/// distinct-collection count (topics × shards).
const WAL_APPEND_LOCK_STRIPES: usize = 256;

pub struct WalManager {
    wal: Walrus,
    data_dir: PathBuf,
    /// Logical topic strings ("{project_id}:{table_name}") — one entry per
    /// (project, table). Each maps to `shards_per_topic` walrus collections.
    known_topics: DashSet<String>,
    /// Per-topic round-robin counter choosing the shard for the next batch.
    shard_counter: dashmap::DashMap<String, std::sync::atomic::AtomicU64>,
    /// Walrus serializes appends within one collection, so N shards lift the
    /// single-project ceiling, at the cost of merging N streams in timestamp
    /// order during recovery.
    shards_per_topic: usize,
    /// Per-collection append serialization, striped by `walrus_key` hash.
    /// Walrus rejects *concurrent* appends to one collection; without these
    /// locks a collision errors the insert and dead-letters the row.
    append_locks: Vec<std::sync::Mutex<()>>,
    /// Fsync the shard before returning from single-entry (DML) appends.
    /// Batched INSERT appends are always flushed by walrus's `batch_write`.
    ack_fsync: bool,
}

impl WalManager {
    pub fn with_fsync_mode_and_shards(data_dir: PathBuf, mode: crate::config::WalFsyncMode, shards_per_topic: usize) -> Result<Self, WalError> {
        std::fs::create_dir_all(&data_dir)?;
        Self::check_wal_version_stamp(&data_dir)?;

        let schedule = match mode {
            crate::config::WalFsyncMode::Milliseconds(ms) => FsyncSchedule::Milliseconds(ms),
            crate::config::WalFsyncMode::SyncEach => FsyncSchedule::SyncEach,
            crate::config::WalFsyncMode::None => FsyncSchedule::NoFsync,
        };
        // Root the WAL at the dir we were handed, NOT at walrus's process-global
        // `WALRUS_DATA_DIR` — that would make every `WalManager` in a process
        // share one directory and corrupt each other's blocks.
        let wal = Walrus::with_root(&data_dir, ReadConsistency::StrictlyAtOnce, schedule)?;

        let meta_dir = data_dir.join(META_DIR);
        let _ = std::fs::create_dir_all(&meta_dir);
        let known_topics: DashSet<String> =
            std::fs::read_to_string(meta_dir.join("topics")).map(|c| c.lines().filter(|l| !l.is_empty()).map(String::from).collect()).unwrap_or_default();

        // Sweep a leftover snapshot tmp file from a crash between write and rename.
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

    /// Refuse to open a WAL directory written by an incompatible binary.
    /// Fresh directories (no stamp, no walrus state) auto-stamp the current
    /// version; a pre-existing walrus dir without a stamp is refused.
    fn check_wal_version_stamp(data_dir: &std::path::Path) -> Result<(), WalError> {
        let meta_dir = data_dir.join(META_DIR);
        let _ = std::fs::create_dir_all(&meta_dir);
        let stamp_path = meta_dir.join("wal_version");

        // Lazy: only the missing-stamp arms need the directory scan.
        let has_walrus_state =
            || std::fs::read_dir(data_dir).map(|rd| rd.flatten().any(|e| e.file_name() != META_DIR && e.file_name() != "wal_version")).unwrap_or(false);

        match std::fs::read_to_string(&stamp_path).map(|s| s.trim().parse::<u8>()) {
            Ok(Ok(v)) if v == WAL_VERSION => Ok(()),
            // Mismatched or unparseable stamp — both mean "written by an incompatible binary".
            Ok(parsed) => {
                let on_disk = parsed.unwrap_or(0);
                error!(
                    "WAL on-disk version {} != binary version {}. IN-FLIGHT DATA WILL BE LOST \
                     IF YOU PROCEED. Wipe {:?} to start fresh, or run a matching binary.",
                    on_disk, WAL_VERSION, data_dir
                );
                Err(WalError::UnsupportedVersion { version: on_disk, expected: WAL_VERSION })
            }
            Err(_) if has_walrus_state() => {
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

    fn persist_topic(&self, topic: &str) {
        // contains-first: `insert` alone would allocate a String on every append.
        if self.known_topics.contains(topic) || !self.known_topics.insert(topic.to_string()) {
            return;
        }
        use std::io::Write;
        let meta_dir = self.data_dir.join(META_DIR);
        let _ = std::fs::create_dir_all(&meta_dir)
            .and_then(|()| std::fs::OpenOptions::new().create(true).append(true).open(meta_dir.join("topics")))
            .and_then(|mut f| writeln!(f, "{}", topic))
            .inspect_err(|e| warn!("Failed to persist WAL topic '{}' to {:?}: {}", topic, meta_dir, e));
    }

    /// Human-readable topic identifier for metadata/logging
    fn make_topic(project_id: &str, table_name: &str) -> String {
        format!("{}:{}", project_id, table_name)
    }

    /// Short hash for the walrus topic key, scoped to a shard. Walrus's
    /// metadata budget is 62 bytes; 16 hex chars + `-` + 2 digits stays under.
    fn walrus_topic_key(project_id: &str, table_name: &str, shard: usize) -> String {
        // The hash MUST be stable across compilations — it indexes durable WAL
        // data, and a per-build-seeded hasher would silently strand entries.
        // Fields are length-prefixed so ("a:b","c") and ("a","b:c") differ.
        use std::hash::Hasher;

        use fnv::FnvHasher;
        let mut hasher = FnvHasher::default();
        hasher.write_u64(project_id.len() as u64);
        hasher.write(project_id.as_bytes());
        hasher.write_u64(table_name.len() as u64);
        hasher.write(table_name.as_bytes());
        format!("{:016x}-{:02}", hasher.finish(), shard)
    }

    /// Round-robin shard chooser for a topic, so concurrent batches spread
    /// across N walrus collections instead of serializing on one write lock.
    fn pick_shard(&self, topic: &str) -> usize {
        use std::sync::atomic::{AtomicU64, Ordering};
        // get-first: `entry` would allocate the String key on every append. The
        // `get` guard is released before the `entry` in the None arm (dashmap
        // self-deadlocks otherwise).
        let ticket = match self.shard_counter.get(topic) {
            Some(counter) => counter.fetch_add(1, Ordering::Relaxed),
            None => self.shard_counter.entry(topic.to_string()).or_insert_with(|| AtomicU64::new(0)).fetch_add(1, Ordering::Relaxed),
        };
        (ticket as usize) % self.shards_per_topic
    }

    fn parse_topic(topic: &str) -> Option<(String, String)> {
        topic.split_once(':').map(|(p, t)| (p.to_string(), t.to_string()))
    }

    /// Acquire the append lock for a walrus collection so concurrent appends
    /// queue instead of erroring. Must be held only across the fast in-memory
    /// walrus write — never across an `.await`.
    fn append_lock(&self, walrus_key: &str) -> std::sync::MutexGuard<'_, ()> {
        use std::hash::{Hash, Hasher};
        let mut h = twox_hash::XxHash3_64::default();
        walrus_key.hash(&mut h);
        let idx = (h.finish() as usize) % self.append_locks.len();
        self.append_locks[idx].lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Enable fsync-before-ack for single-entry appends (`TIMEFUSION_WAL_ACK_FSYNC`).
    pub fn with_ack_fsync(mut self, on: bool) -> Self {
        self.ack_fsync = on;
        self
    }

    /// Serialize and append one entry under the shard's `append_lock`. Callers
    /// must keep `persist_topic` AFTER this call so its file I/O stays outside
    /// the critical section. `on_pre` fires with the pre-append tail under the
    /// lock — same hold-registration contract as [`Self::append_batch`].
    fn locked_append(&self, walrus_key: &str, entry: &WalEntry, on_pre: impl FnOnce(Option<WalPosition>)) -> Result<(), WalError> {
        crate::support::without_blocking_the_worker(|| {
            let entry_bytes = serialize_wal_entry(entry)?;
            let guard = self.append_lock(walrus_key);
            on_pre(self.wal.current_position(walrus_key).ok());
            self.wal.append_for_topic(walrus_key, &entry_bytes)?;
            // Sync OUTSIDE the stripe lock: the bytes are already in the mmap and
            // `Writer::sync` flushes the whole active block, so sync-before-ack
            // still holds, and an ms-scale msync never stalls same-stripe appends.
            drop(guard);
            if self.ack_fsync {
                self.wal.sync_topic(walrus_key).map_err(WalError::Io)?;
            }
            Ok(())
        })
    }

    /// Returns the shard the entry was appended to.
    #[instrument(skip(self, batch))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> Result<usize, WalError> {
        self.append_batch(project_id, table_name, std::slice::from_ref(batch), |_, _| {}).map(|(shard, _)| shard)
    }

    /// Returns `(shard, pre_append_position)` — every batch becomes one walrus
    /// entry on the chosen shard.
    ///
    /// `on_pre_append(shard, position)` fires under the shard's append lock
    /// BEFORE the entries exist, with the shard's write tail at that instant.
    /// Callers register a read-cursor *hold* there; registration must
    /// happen-before the append so a concurrent watermark computation can never
    /// advance the cursor past an entry whose hold it hasn't seen.
    #[instrument(skip(self, batches, on_pre_append))]
    pub fn append_batch(
        &self, project_id: &str, table_name: &str, batches: &[RecordBatch], on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<(usize, Option<WalPosition>), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        // Serialize AND append off the worker's own queue: the default
        // `sync_each` mode fsyncs inside `batch_append_for_topic`, a blocking
        // syscall at ingest frequency.
        let pre_pos = crate::support::without_blocking_the_worker(|| -> Result<_, WalError> {
            // Imperative on purpose: a `map(..).collect::<Result<Vec<_>,_>>()` over
            // the splits would hold every batch's split output alive alongside the
            // serialized entries, doubling the transient footprint of a big append.
            let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(batches.len());
            for batch in batches {
                for data in split_to_wal_payloads(batch, WAL_SPLIT_TARGET, MAX_BATCH_SIZE)? {
                    payloads.push(serialize_wal_entry(&WalEntry::new(project_id, table_name, WalOperation::Insert, data))?);
                }
            }
            let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
            // Guard scoped tightly: dropped before persist_topic so the shard
            // lock never covers persist_topic's synchronous file I/O.
            let _guard = self.append_lock(&walrus_key);
            let pre_pos = self.wal.current_position(&walrus_key).ok();
            on_pre_append(shard, pre_pos);
            self.wal.batch_append_for_topic(&walrus_key, &payload_refs)?;
            Ok(pre_pos)
        })?;
        self.persist_topic(&topic);
        debug!(%topic, shard, batches = batches.len(), "WAL batch append INSERT");
        Ok((shard, pre_pos))
    }

    /// Encode a DML payload and append it as one entry; returns the chosen shard.
    fn append_dml<P: Encode>(
        &self, project_id: &str, table_name: &str, operation: WalOperation, payload: &P, on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let entry = WalEntry::new(project_id, table_name, operation, bincode::encode_to_vec(payload, BINCODE_CONFIG)?);
        self.locked_append(&walrus_key, &entry, |pre| on_pre_append(shard, pre))?;
        self.persist_topic(&topic);
        Ok(shard)
    }

    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, on_pre_append))]
    pub fn append_delete(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        let payload = DeletePayload { predicate_sql: predicate_sql.map(String::from) };
        let shard = self.append_dml(project_id, table_name, WalOperation::Delete, &payload, on_pre_append)?;
        debug!(project_id, table_name, shard, ?predicate_sql, "WAL append DELETE");
        Ok(shard)
    }

    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, assignments, on_pre_append))]
    pub fn append_update(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)],
        on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        let payload = UpdatePayload { predicate_sql: predicate_sql.map(String::from), assignments: assignments.to_vec() };
        let shard = self.append_dml(project_id, table_name, WalOperation::Update, &payload, on_pre_append)?;
        debug!(project_id, table_name, shard, ?predicate_sql, assignments = assignments.len(), "WAL append UPDATE");
        Ok(shard)
    }

    /// Append an `UPDATE ... FROM` entry. Stores the source `RecordBatch`
    /// (already serialized to Arrow IPC bytes by the caller) alongside the
    /// predicate + assignments so WAL replay can reconstruct the join.
    /// `on_pre_append` — same hold-registration contract as [`Self::append_batch`].
    #[instrument(skip(self, assignments, source, on_pre_append), fields(source_ipc_bytes = source.batch_ipc.len()))]
    pub fn append_update_with_source(
        &self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)], source: &SerializedSource,
        on_pre_append: impl FnOnce(usize, Option<WalPosition>),
    ) -> Result<usize, WalError> {
        // Replay rejects over-cap source batches, so an acked oversized entry
        // would be silently dropped at the next boot. A JOIN source can't be
        // split without changing update semantics, so fail the append instead.
        if source.batch_ipc.len() > MAX_BATCH_SIZE {
            return Err(WalError::BatchTooLarge { size: source.batch_ipc.len(), max: MAX_BATCH_SIZE });
        }
        let payload = UpdateWithSourcePayload { predicate_sql: predicate_sql.map(String::from), assignments: assignments.to_vec(), source: source.clone() };
        let shard = self.append_dml(project_id, table_name, WalOperation::UpdateWithSource, &payload, on_pre_append)?;
        debug!(
            project_id,
            table_name,
            shard,
            ?predicate_sql,
            assignments = assignments.len(),
            source_keys = source.join_keys.len(),
            source_bytes = source.batch_ipc.len(),
            "WAL append UPDATE_WITH_SOURCE"
        );
        Ok(shard)
    }

    #[instrument(skip(self))]
    pub fn read_entries_raw(
        &self, project_id: &str, table_name: &str, since_timestamp_micros: Option<i64>, checkpoint: bool,
    ) -> Result<(Vec<WalEntry>, usize), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let cutoff = since_timestamp_micros.unwrap_or(0);
        let mut results = Vec::new();
        let mut error_count = 0usize;

        // Drain each shard in append order, then sort the merged slice by
        // timestamp so the caller sees a topic-wide ordering.
        for shard in 0..self.shards_per_topic {
            let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
            while let Some((entry, _)) = Self::next_from_shard_timed(&self.wal, &walrus_key, checkpoint, true, &mut error_count, &mut 0, &mut 0) {
                if entry.timestamp_micros >= cutoff {
                    results.push(entry);
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

    /// Pull-based stream of every un-consumed WAL entry, topic by topic. At
    /// most one entry per shard is alive at a time, so replay memory is
    /// O(shards), and callers can await flush-to-make-room between entries.
    ///
    /// Within a topic the N shard streams are k-way merged by
    /// `timestamp_micros`, preserving DELETE-after-INSERT ordering. Cross-topic
    /// ordering is not preserved; DML only mutates its own topic's MemBuffer.
    ///
    /// Always checkpointing: an uncheckpointed `read_next` never advances the
    /// cursor, so a read-until-None stream would re-read the first entry
    /// forever. Recovery parks the cursor back via `set_positions_allow_rewind`.
    pub fn replay_iter(&self) -> Result<WalReplayIter<'_>, WalError> {
        Ok(WalReplayIter {
            wal: self,
            topics: self.known_topics.iter().map(|t| t.clone()).collect(),
            topic_idx: 0,
            heap: std::collections::BinaryHeap::new(),
            shard_keys: Vec::new(),
            pending: Vec::new(),
            cur_topic: None,
            total: 0,
            errors: 0,
            read_nanos: 0,
            envelope_nanos: 0,
        })
    }

    /// Read the next entry from a shard, skipping corrupted ones; `None` at end
    /// of stream. Attributes wall clock to the walrus read and the envelope
    /// decode separately.
    fn next_from_shard_timed(
        wal: &Walrus, key: &str, checkpoint: bool, persist_checkpoint: bool, errors: &mut usize, read_nanos: &mut u128, envelope_nanos: &mut u128,
    ) -> Option<(WalEntry, WalPosition)> {
        loop {
            let t_read = std::time::Instant::now();
            let next = if checkpoint && !persist_checkpoint { wal.read_next_volatile_with_position(key) } else { wal.read_next_with_position(key, checkpoint) };
            *read_nanos += t_read.elapsed().as_nanos();
            match next {
                Ok(Some((d, pos))) => {
                    let t_env = std::time::Instant::now();
                    let decoded = deserialize_wal_entry(&d.data);
                    *envelope_nanos += t_env.elapsed().as_nanos();
                    match decoded {
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
                    }
                }
                Ok(None) => return None,
                Err(e) => {
                    error!("I/O error reading WAL shard {}: {}", key, e);
                    *errors += 1;
                    return None;
                }
            }
        }
    }

    /// Known topics parsed into `(project_id, table_name)` pairs.
    pub fn list_topic_pairs(&self) -> Vec<(String, String)> {
        self.known_topics.iter().filter_map(|t| Self::parse_topic(&t)).collect()
    }

    /// Set each shard's persisted read cursor to `positions[shard]`
    /// unconditionally — backward moves allowed (unlike the forward-only
    /// [`Self::merge_persisted_positions`]). Used by WAL recovery, which
    /// consumes to tail while replaying and must then park the cursor back at
    /// the earliest entry still owned by an unflushed MemBuffer bucket.
    /// `None` shards are left untouched.
    pub fn set_positions_allow_rewind(&self, project_id: &str, table_name: &str, positions: &[Option<WalPosition>]) -> Result<(), WalError> {
        self.check_shard_len("set_positions_allow_rewind", positions.len())?;
        self.apply_positions(project_id, table_name, positions.iter().copied())
    }

    /// Write `positions[shard]` (skipping `None`) to each shard's persisted read cursor.
    fn apply_positions(&self, project_id: &str, table_name: &str, positions: impl IntoIterator<Item = Option<WalPosition>>) -> Result<(), WalError> {
        positions.into_iter().enumerate().filter_map(|(shard, pos)| pos.map(|p| (shard, p))).try_for_each(|(shard, pos)| {
            self.wal.set_persisted_read_position(&Self::walrus_topic_key(project_id, table_name, shard), pos).map_err(WalError::Io)
        })
    }

    fn for_each_shard<T>(&self, project_id: &str, table_name: &str, mut f: impl FnMut(&str) -> std::io::Result<T>) -> Result<Vec<T>, WalError> {
        (0..self.shards_per_topic).map(|shard| f(&Self::walrus_topic_key(project_id, table_name, shard)).map_err(WalError::Io)).collect()
    }

    fn check_shard_len(&self, label: &str, len: usize) -> Result<(), WalError> {
        (len == self.shards_per_topic)
            .then_some(())
            .ok_or_else(|| WalError::Internal(format!("{}: len={} but shards_per_topic={}", label, len, self.shards_per_topic)))
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

    /// True when every WAL shard's durable read cursor is exactly at its
    /// current write tail. `None` is equivalent to origin only for a
    /// never-written shard; any non-origin tail without a cursor must replay.
    pub fn is_fully_consumed(&self) -> Result<bool, WalError> {
        for (project_id, table_name) in self.list_topic_pairs() {
            let tails = self.current_position(&project_id, &table_name)?;
            let cursors = self.persisted_read_positions(&project_id, &table_name)?;
            if tails.into_iter().zip(cursors).any(|(tail, cursor)| cursor.unwrap_or(WalPosition::ORIGIN) != tail) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Whether startup may skip remote Delta cursor reconciliation from local
    /// WAL state alone. A leftover rewind marker vetoes the shortcut: applying
    /// it will make already-consumed entries unread again.
    pub fn can_skip_delta_reconcile(&self) -> Result<bool, WalError> {
        if self.recovery_rewind_path().exists() {
            return Ok(false);
        }
        self.is_fully_consumed()
    }

    /// Set the walrus persisted-read cursor per shard. Used at startup to
    /// fast-forward to a Delta-derived watermark when Delta is ahead of
    /// locally-fsynced walrus state.
    pub fn set_persisted_positions(&self, project_id: &str, table_name: &str, positions: &[WalPosition]) -> Result<(), WalError> {
        self.check_shard_len("set_persisted_positions", positions.len())?;
        self.apply_positions(project_id, table_name, positions.iter().copied().map(Some))
    }

    /// Trigger walrus's file reclaim worker immediately. Returns an epoch to
    /// pass to [`Self::reclaim_sweep_complete`].
    pub fn request_reclaim_sweep(&self) -> u64 {
        self.wal.request_reclaim_sweep()
    }

    pub fn reclaim_sweep_complete(&self, epoch: u64) -> bool {
        self.wal.reclaim_sweep_complete(epoch)
    }

    pub fn reclaim_state_counts(&self) -> ReclaimStateCounts {
        let prefix = self.data_dir.to_string_lossy();
        Walrus::file_reclaim_states().into_iter().filter(|(path, ..)| path.starts_with(prefix.as_ref())).fold(
            ReclaimStateCounts::default(),
            |mut counts, (_, locked, checkpointed, total, fully_allocated)| {
                counts.total += 1;
                counts.eligible += usize::from(locked == 0 && checkpointed >= total && fully_allocated);
                counts.locked += usize::from(locked > 0);
                counts.uncheckpointed += usize::from(checkpointed < total);
                counts.open += usize::from(!fully_allocated);
                counts
            },
        )
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Test hook: append raw bytes as a WAL entry to exercise recovery-corruption paths.
    #[cfg(test)]
    pub fn append_raw_for_test(&self, project_id: &str, table_name: &str, bytes: &[u8]) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        {
            // Guard scoped so persist_topic's file I/O runs outside it.
            let _guard = self.append_lock(&walrus_key);
            self.wal.append_for_topic(&walrus_key, bytes)?;
        }
        self.persist_topic(&topic);
        Ok(())
    }

    fn cursor_snapshot_path(&self) -> PathBuf {
        cursor_snapshot_path_in(&self.data_dir)
    }

    /// Capture every known topic's per-shard persisted-read cursor to a JSON
    /// file, letting boot skip `derive_wal_cursors_from_delta`'s remote scan.
    ///
    /// `clean_shutdown=true` only from the graceful-shutdown path; flush callers
    /// pass false so a hard kill falls back to the Delta scan.
    ///
    /// Atomic (write `.tmp` + rename) and best-effort: a missing snapshot only
    /// costs the next boot's fast path, never correctness.
    pub fn write_cursor_snapshot(&self, clean_shutdown: bool, drained: bool) -> Result<(), WalError> {
        let entries = self
            .list_topic_pairs()
            .into_iter()
            .filter_map(|(project_id, table_name)| {
                self.persisted_read_positions(&project_id, &table_name)
                    .inspect_err(|e| debug!("write_cursor_snapshot: skipping {}/{}: {}", project_id, table_name, e))
                    .ok()
                    .map(|positions| (Self::make_topic(&project_id, &table_name), positions.into_iter().map(|p| p.map(pos_to_snap)).collect()))
            })
            .collect();
        let snap = CursorSnapshot {
            version: SNAPSHOT_VERSION,
            written_at_micros: crate::support::now_micros(),
            shards_per_topic: self.shards_per_topic,
            clean_shutdown,
            drained,
            entries,
        };
        // Deliberately not fsynced: a lost snapshot only costs the next boot's
        // fast path, and drained=true reverting to absent is the safe direction.
        write_json_atomic(&self.cursor_snapshot_path(), &snap, false, "cursor snapshot")
    }

    /// Remove the on-disk cursor snapshot so the next boot doesn't trust stale
    /// state. NotFound is silently ignored.
    pub fn delete_cursor_snapshot(&self) -> Result<(), WalError> {
        Ok(remove_if_exists(&self.cursor_snapshot_path())?)
    }

    fn recovery_rewind_path(&self) -> PathBuf {
        meta_path(&self.data_dir, "recovery_rewind.json")
    }

    /// Crash-safety for WAL replay: replay consumes the walrus cursor as it
    /// reads, so a crash mid-replay would skip consumed entries whose data
    /// never reached Delta. This marker holds the pre-recovery cursors; a
    /// marker found at boot means rewind and re-replay. Must be deleted only
    /// after the post-replay watermark parks the cursor.
    ///
    /// Returns the captured pre-recovery positions per (project, table) so
    /// the caller can pin replay-created buckets at them.
    pub fn write_recovery_rewind_marker(&self) -> Result<TopicPositions, WalError> {
        // Never-persisted shards (None) become explicit ORIGIN holds, or a new
        // topic's replayed buckets would be unpinned and lost on a crash before
        // their first flush.
        let p0: TopicPositions = self
            .list_topic_pairs()
            .into_iter()
            .map(|(project_id, table_name)| {
                let positions = self.persisted_read_positions(&project_id, &table_name)?;
                Ok(((project_id, table_name), positions.into_iter().map(|p| Some(p.unwrap_or(WalPosition::ORIGIN))).collect()))
            })
            .collect::<Result<_, WalError>>()?;
        self.write_recovery_rewind_marker_at(&p0)?;
        Ok(p0)
    }

    /// Rewrite the rewind marker to the current replay watermark, so a later
    /// crash re-replays only from the earliest still-un-drained entry.
    ///
    /// Full overwrite: `positions` MUST carry every topic the initial marker
    /// held, or the omitted ones are silently rewound to ORIGIN. A None shard
    /// maps to ORIGIN on apply, correct only when it has no covered data.
    /// Durable (fsync).
    pub fn write_recovery_rewind_marker_at(&self, positions: &TopicPositions) -> Result<(), WalError> {
        let entries: std::collections::BTreeMap<String, Vec<Option<SnapPos>>> =
            positions.iter().map(|((p, t), shards)| (Self::make_topic(p, t), shards.iter().map(|s| s.map(pos_to_snap)).collect())).collect();
        // Must be as durable as walrus's own cursor fsync (sync file + dir).
        write_json_atomic(&self.recovery_rewind_path(), &entries, true, "rewind marker")
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
        entries.iter().try_for_each(|(topic, positions)| {
            let Some((project_id, table_name)) = Self::parse_topic(topic) else {
                // Skipping would let recovery overwrite the marker with the
                // crashed replay's consumed cursors, losing what it consumed.
                return Err(WalError::Internal(format!("rewind marker has unparseable topic {:?} — refusing to recover past it", topic)));
            };
            if positions.len() != self.shards_per_topic {
                // Fail the boot with the marker intact rather than lose the
                // entries the crashed replay already consumed.
                return Err(WalError::Internal(format!(
                    "rewind marker entry for {} has {} shards but topic has {} — refusing to recover with a shard-count mismatch (restore TIMEFUSION_WAL_SHARDS_PER_TOPIC or handle the marker manually)",
                    topic,
                    positions.len(),
                    self.shards_per_topic
                )));
            }
            // None = never persisted pre-recovery; the crashed replay may have
            // persisted progress since, so rewind explicitly to ORIGIN.
            let positions: Vec<Option<WalPosition>> = positions.iter().map(|p| Some(p.map_or(WalPosition::ORIGIN, snap_to_pos))).collect();
            self.set_positions_allow_rewind(&project_id, &table_name, &positions)
        })?;
        warn!("Applied recovery rewind marker for {} topic(s) — previous replay crashed mid-run; re-replaying", entries.len());
        Ok(true)
    }

    pub fn remove_recovery_rewind_marker(&self) {
        if let Err(e) = remove_if_exists(&self.recovery_rewind_path()) {
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
        // Age is informational only — `clean_shutdown` is the gate.
        const STALE_AFTER_MICROS: i64 = 24 * 3600 * 1_000_000;
        let age_micros = crate::support::now_micros().saturating_sub(snap.written_at_micros);
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
        snap.entries.iter().try_fold(0usize, |tables_advanced, (topic, snapshot_positions)| {
            let Some((project_id, table_name)) = Self::parse_topic(topic) else { return Ok(tables_advanced) };
            if snapshot_positions.len() != self.shards_per_topic {
                warn!(
                    "cursor snapshot entry for {}/{} has {} shards but topic has {} — skipping",
                    project_id,
                    table_name,
                    snapshot_positions.len(),
                    self.shards_per_topic
                );
                return Ok(tables_advanced);
            }
            // Seed `known_topics` so a later list_topic_pairs() includes a
            // table not yet re-touched in this process.
            self.persist_topic(topic);

            let candidate: Vec<Option<WalPosition>> = snapshot_positions.iter().map(|p| p.map(snap_to_pos)).collect();
            let moved = self.merge_persisted_positions(&project_id, &table_name, &candidate)? > 0;
            Ok(tables_advanced + usize::from(moved))
        })
    }

    /// Fast-forward each shard's persisted-read cursor to `candidate[shard]`
    /// when the candidate is strictly ahead. Returns the number of shards moved.
    pub fn merge_persisted_positions(&self, project_id: &str, table_name: &str, candidate: &[Option<WalPosition>]) -> Result<usize, WalError> {
        if candidate.len() != self.shards_per_topic {
            return Ok(0);
        }
        let local = self.persisted_read_positions(project_id, table_name).unwrap_or_else(|_| vec![None; self.shards_per_topic]);
        // Per shard: Some(pos) when the candidate is strictly ahead, else None.
        let moved: Vec<Option<WalPosition>> =
            local.iter().zip(candidate).map(|(&local, &cand)| cand.filter(|c| local.map_or(!c.is_origin(), |l| *c > l))).collect();
        let advanced = moved.iter().flatten().count();
        if advanced > 0 {
            let to_set: Vec<WalPosition> = moved.iter().zip(&local).map(|(m, l)| m.or(*l).unwrap_or(WalPosition::ORIGIN)).collect();
            self.set_persisted_positions(project_id, table_name, &to_set)?;
        }
        Ok(advanced)
    }

    /// Configured number of walrus collections per logical topic.
    pub fn shards_per_topic(&self) -> usize {
        self.shards_per_topic
    }

    /// Number of registered logical topics, independent of shard count.
    pub fn known_topic_count(&self) -> usize {
        self.known_topics.len()
    }

    /// Returns WAL file count and total size in bytes by scanning the data directory.
    pub fn wal_stats(&self) -> (usize, u64) {
        std::fs::read_dir(&self.data_dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter_map(|e| e.metadata().ok().filter(|m| m.is_file()))
            .fold((0, 0), |(files, bytes), m| (files + 1, bytes + m.len()))
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
/// write the next boot silently drops). Each row-chunk is compacted before
/// serializing so sliced view/offset buffers are privatized and the IPC size
/// actually shrinks; the parent's bytes are dropped first, keeping the
/// transient footprint (NOT covered by the insert path's reservation) near 2x
/// the batch's IPC size. A single row over `target` passes through whole; over
/// `hard_max` it errors at append time.
fn split_to_wal_payloads(batch: &RecordBatch, target: usize, hard_max: usize) -> Result<Vec<Vec<u8>>, WalError> {
    let data = serialize_record_batch(batch)?;
    if data.len() <= target {
        return Ok(vec![data]);
    }
    if batch.num_rows() <= 1 {
        return if data.len() <= hard_max { Ok(vec![data]) } else { Err(WalError::BatchTooLarge { size: data.len(), max: hard_max }) };
    }
    // Row-slicing can't shrink dictionary columns (every IPC stream carries the
    // full dictionary), so flatten them first or the split degenerates to one
    // near-full-size entry per row. Flattening replicates values per row, so the
    // chunk math below must re-measure the flattened size.
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
    (0..batch.num_rows()).step_by(rows_per).try_fold(Vec::with_capacity(chunks), |mut out, start| {
        let len = rows_per.min(batch.num_rows() - start);
        let chunk = crate::write::mem_buffer::compact_batch(batch.slice(start, len));
        let chunk_data = serialize_record_batch(&chunk)?;
        if chunk_data.len() <= target || len <= 1 {
            if chunk_data.len() > hard_max {
                return Err(WalError::BatchTooLarge { size: chunk_data.len(), max: hard_max });
            }
            out.push(chunk_data);
        } else if chunk_data.len().saturating_mul(3) >= parent_len.saturating_mul(2) {
            // Barely shrank despite holding a fraction of the rows: some payload
            // is shared across rows and row-slicing can't divide it. Bail rather
            // than recurse toward a per-row explosion of near-full-size entries.
            return Err(WalError::BatchTooLarge { size: chunk_data.len(), max: target });
        } else {
            // Skewed rows left this chunk over target — re-split just it.
            drop(chunk_data);
            out.extend(split_to_wal_payloads(&chunk, target, hard_max)?);
        }
        Ok(out)
    })
}

/// Cast top-level dictionary columns to their value types (`None` when the
/// batch has no dictionary columns).
fn flatten_dictionary_columns(batch: &RecordBatch) -> Result<Option<RecordBatch>, WalError> {
    use arrow::datatypes::{DataType, Field};
    if !batch.schema().fields().iter().any(|f| matches!(f.data_type(), DataType::Dictionary(_, _))) {
        return Ok(None);
    }
    let (fields, cols): (Vec<Field>, Vec<arrow::array::ArrayRef>) = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(f, c)| match f.data_type() {
            DataType::Dictionary(_, value_type) => Ok((Field::new(f.name(), (**value_type).clone(), f.is_nullable()), arrow::compute::cast(c, value_type)?)),
            _ => Ok(((**f).clone(), c.clone())),
        })
        .collect::<Result<_, WalError>>()?;
    Ok(Some(RecordBatch::try_new(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)), cols)?))
}

pub(crate) fn deserialize_record_batch(data: &[u8]) -> Result<RecordBatch, WalError> {
    if data.len() > MAX_BATCH_SIZE {
        return Err(WalError::BatchTooLarge { size: data.len(), max: MAX_BATCH_SIZE });
    }
    StreamReader::try_new(std::io::Cursor::new(data), None)?.next().transpose()?.ok_or(WalError::EmptyBatch)
}

fn serialize_wal_entry(entry: &WalEntry) -> Result<Vec<u8>, WalError> {
    Ok([&WAL_MAGIC[..], &[WAL_VERSION, entry.operation as u8], &bincode::encode_to_vec(entry, BINCODE_CONFIG)?[..]].concat())
}

fn deserialize_wal_entry(data: &[u8]) -> Result<WalEntry, WalError> {
    let [m0, m1, m2, m3, version, operation, payload @ ..] = data else {
        return Err(WalError::TooShort { len: data.len() });
    };
    if [*m0, *m1, *m2, *m3] != WAL_MAGIC {
        return Err(WalError::BadMagic { got: [*m0, *m1, *m2, *m3] });
    }
    if *version != WAL_VERSION {
        return Err(WalError::UnsupportedVersion { version: *version, expected: WAL_VERSION });
    }
    WalOperation::try_from(*operation)?;
    let (entry, _): (WalEntry, _) = bincode::decode_from_slice(payload, BINCODE_CONFIG)?;
    Ok(entry)
}

/// Decode any bincode DML payload (Delete/Update/UpdateWithSource) from WAL bytes.
pub fn decode_payload<T: Decode<()>>(data: &[u8]) -> Result<T, WalError> {
    let (payload, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
    Ok(payload)
}

/// See [`WalManager::replay_iter`]. Heap is keyed by `(timestamp, shard)`;
/// payloads travel in the `pending` slot indexed by shard, avoiding an `Ord`
/// bound on `WalEntry`. Invariant: at most one in-flight entry per shard, so
/// replay memory is O(shards_per_topic).
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
    /// Wall-clock inside the walrus read (I/O + the copy out of the block).
    pub read_nanos: u128,
    /// Wall-clock decoding the WAL envelope (the bincode step before Arrow).
    pub envelope_nanos: u128,
}

impl WalReplayIter<'_> {
    /// The topic currently being replayed and its per-shard *frontier* — the
    /// position of the next entry each shard will yield (`None` = exhausted).
    /// Everything strictly before `frontier[shard]` has been yielded AND
    /// processed; the entry AT it has not. This is the safe watermark baseline:
    /// the walrus read cursor sits one prefetched entry per shard ahead of it.
    pub fn frontier(&self) -> (Option<(String, String)>, Vec<Option<WalPosition>>) {
        (self.cur_topic.clone(), self.pending.iter().map(|p| p.as_ref().map(|(_, pos)| *pos)).collect())
    }

    /// Prefetch the shard's next entry into `pending[shard]` + the heap,
    /// preserving the one-in-flight-entry-per-shard invariant.
    fn prime(&mut self, shard: usize) {
        // Non-persisting checkpoint: recovery's durable rewind marker already
        // covers a crash, so persisting per prefetched entry is one wasted fsync.
        if let Some(next) = WalManager::next_from_shard_timed(
            &self.wal.wal,
            &self.shard_keys[shard],
            true,
            false,
            &mut self.errors,
            &mut self.read_nanos,
            &mut self.envelope_nanos,
        ) {
            self.heap.push(std::cmp::Reverse((next.0.timestamp_micros, shard)));
            self.pending[shard] = Some(next);
        }
    }

    /// Yields `(entry, shard, position)` — `position` is the entry's WAL
    /// position on its `shard`, so recovery can pin the buffered bucket exactly.
    pub fn next_entry(&mut self) -> Option<(WalEntry, usize, WalPosition)> {
        use std::cmp::Reverse;
        loop {
            if let Some(Reverse((_, shard))) = self.heap.pop() {
                let (entry, pos) = self.pending[shard].take().expect("heap and pending out of sync");
                self.total += 1;
                self.prime(shard);
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
            self.cur_topic = Some((project_id, table_name));
            self.pending = (0..shards).map(|_| None).collect();
            (0..shards).for_each(|shard| self.prime(shard));
        }
    }
}

fn cursor_snapshot_path_in(data_dir: &std::path::Path) -> PathBuf {
    meta_path(data_dir, "cursor_snapshot.json")
}

/// [`write_atomic_with`] of a JSON-encoded value; `what` names the document in
/// the encode-failure message.
fn write_json_atomic<T: Serialize>(target: &std::path::Path, value: &T, durable: bool, what: &str) -> Result<(), WalError> {
    use std::io::Write;
    let bytes = serde_json::to_vec(value).map_err(|e| WalError::Internal(format!("{what} encode: {e}")))?;
    Ok(write_atomic_with(target, durable, |f| f.write_all(&bytes))?)
}

/// Atomic file write via tmp + rename; content is streamed through `write`.
/// `durable` additionally fsyncs the file before the rename and the parent dir
/// after — required whenever the content authorizes destructive action (rewind
/// marker, drained-flag consumption); pure hint files may skip the syncs. The
/// temp file is removed on failure. Callers are runtime tasks and `durable`
/// costs two fsyncs, so the whole body runs off the worker thread.
pub(crate) fn write_atomic_with(target: &std::path::Path, durable: bool, write: impl FnOnce(&mut std::fs::File) -> std::io::Result<()>) -> std::io::Result<()> {
    crate::support::without_blocking_the_worker(move || {
        let mut tmp = target.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        (|| {
            let mut f = std::fs::File::create(&tmp)?;
            write(&mut f)?;
            if durable { f.sync_all() } else { Ok(()) }
        })()
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })?;
        std::fs::rename(&tmp, target)?;
        if durable
            && let Some(dir) = target.parent()
            && let Ok(d) = std::fs::File::open(dir)
        {
            let _ = d.sync_all();
        }
        Ok(())
    })
}

/// Read + parse + version-check the cursor snapshot.
/// `WalManager::load_cursor_snapshot` adds shard-count and staleness checks.
fn read_cursor_snapshot(wal_dir: &std::path::Path) -> Option<CursorSnapshot> {
    let path = cursor_snapshot_path_in(wal_dir);
    let bytes = std::fs::read(&path).ok()?;
    let snap: CursorSnapshot =
        serde_json::from_slice(&bytes).inspect_err(|e| warn!("cursor snapshot at {:?} unreadable, falling back to Delta scan: {}", path, e)).ok()?;
    if snap.version != SNAPSHOT_VERSION {
        warn!("cursor snapshot version {} != {} — ignoring", snap.version, SNAPSHOT_VERSION);
        return None;
    }
    Some(snap)
}

/// Process-lifetime exclusive lock on the WAL directory.
///
/// TimeFusion's WAL is single-writer: two live processes sharing one WAL dir
/// fork it and silently lose the older process's appends. This holds an OS
/// advisory `flock` on `<wal_dir>/.timefusion_meta/wal.lock` so a second
/// process waits for the first to exit before touching the WAL. The lock is
/// never stolen and never times out; the kernel releases it on process death.
pub struct WalDirLock {
    // Never read after construction — its liveness IS the lock.
    _file: std::fs::File,
}

impl WalDirLock {
    /// Acquire the exclusive WAL-dir lock, waiting until any other TimeFusion
    /// process holding it exits; errors once `LOCK_WAIT_GIVE_UP` elapses.
    pub async fn acquire(wal_dir: &std::path::Path) -> Result<Self, WalError> {
        let meta_dir = wal_dir.join(META_DIR);
        std::fs::create_dir_all(&meta_dir)?;
        let path = meta_dir.join("wal.lock");
        let file = std::fs::OpenOptions::new().create(true).read(true).write(true).truncate(false).open(&path)?;
        let mut waits = 0u64;
        loop {
            match file.try_lock_exclusive() {
                Ok(true) => {
                    clear_takeover_request(wal_dir);
                    if waits > 0 {
                        info!("WAL dir lock acquired after waiting for a previous process to exit");
                    }
                    return Ok(Self { _file: file });
                }
                // Ok(false) = another live TimeFusion process owns the WAL.
                // Poll at 25ms; log every ~10s (400 polls), escalate past ~60s.
                Ok(false) => {
                    // Asks a drained start-first predecessor to take its normal
                    // graceful-exit path; it never authorizes this contender to
                    // touch WAL state before acquiring the lock.
                    if waits.is_multiple_of(400) {
                        let request = meta_dir.join(TAKEOVER_REQUEST_FILE);
                        // Written ONCE, never refreshed: the predecessor
                        // escalates on the request's age, so rewriting it would
                        // reset that age and the escalation could never fire.
                        if !request.is_file() {
                            let _ =
                                std::fs::write(&request, format!("pid={} requested_at_micros={}\n", std::process::id(), chrono::Utc::now().timestamp_micros()));
                        }
                        let secs = waits / 40;
                        if waits >= 2_400 {
                            error!(
                                "WAL dir {:?} still locked by another TimeFusion process after {secs}s — predecessor may be wedged (check for a stuck/duplicate instance)",
                                path
                            );
                        } else {
                            warn!("WAL dir {:?} is locked by another TimeFusion process; waiting for it to exit before recovery", path);
                        }
                    }
                    // Bounded on purpose: a predecessor that never releases (an
                    // orphaned container) must turn into an ordinary crash-loop
                    // rather than a half-started process occupying memory.
                    if waits >= LOCK_WAIT_GIVE_UP.as_millis() as u64 / 25 {
                        return Err(WalError::LockContention(format!(
                            "WAL dir {path:?} still locked after {}s; giving up so this process restarts instead of \
                             occupying memory forever (look for an orphaned TimeFusion container holding the lock)",
                            LOCK_WAIT_GIVE_UP.as_secs()
                        )));
                    }
                    waits += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                }
                Err(e) => return Err(WalError::Io(e)),
            }
        }
    }
}

pub fn takeover_requested(wal_dir: &std::path::Path) -> bool {
    meta_path(wal_dir, TAKEOVER_REQUEST_FILE).is_file()
}

/// How long a takeover request has been outstanding, or `None` when none is.
pub fn takeover_request_age(wal_dir: &std::path::Path) -> Option<std::time::Duration> {
    let path = meta_path(wal_dir, TAKEOVER_REQUEST_FILE);
    let requested_at =
        std::fs::read_to_string(&path).ok()?.split_whitespace().find_map(|field| field.strip_prefix("requested_at_micros=")?.parse::<i64>().ok())?;
    let elapsed = crate::support::now_micros().saturating_sub(requested_at).max(0);
    Some(std::time::Duration::from_micros(elapsed as u64))
}

pub fn clear_takeover_request(wal_dir: &std::path::Path) {
    if let Err(e) = remove_if_exists(&meta_path(wal_dir, TAKEOVER_REQUEST_FILE)) {
        warn!("could not clear WAL takeover request: {e}");
    }
}

/// Pre-walrus boot WAL GC: deletes dead files before walrus enumerates the dir.
///
/// A complete sweep is sound ONLY when the previous life's shutdown flush fully
/// drained (snapshot `drained=true`); otherwise the old files may BE the
/// un-flushed backlog. The claim is consumed (rewritten false) so it cannot
/// authorize a later boot's sweep. Undrained boot: skip — the floor-aware
/// runtime sweep reclaims instead.
pub fn boot_wal_gc(wal_dir: &std::path::Path) {
    let t = std::time::Instant::now();
    let Some(mut snap) = read_cursor_snapshot(wal_dir).filter(|s| s.clean_shutdown && s.drained) else {
        info!("bootstrap.phase=wal_gc skipped=not_drained (runtime sweep reclaims post-replay)");
        return;
    };
    // Consume the drained claim FIRST and DURABLY: sweep-then-consume fails
    // open, since a power loss reverting the un-fsynced rewrite would resurrect
    // drained=true after this boot's deletions already persisted.
    snap.drained = false;
    // The sweep removes every WAL segment, so old block/offset pairs have no
    // meaning in the fresh Walrus generation and could make a new block look
    // consumed. Empty positions mean origin, matching the empty WAL.
    snap.entries.clear();
    let target = cursor_snapshot_path_in(wal_dir);
    if let Err(e) = write_json_atomic(&target, &snap, true, "cursor snapshot") {
        // Fail closed: without a durable consume the authorization must not be
        // used; remove the stale claim entirely instead.
        warn!("bootstrap.phase=wal_gc could not consume drained flag ({e}) — skipping sweep, deleting snapshot");
        if let Err(rm) = remove_if_exists(&target) {
            error!(
                "bootstrap.phase=wal_gc stale drained=true snapshot could not be removed ({rm}) — \
                 delete {:?} manually before the next restart or the boot sweep may delete un-flushed WAL",
                target
            );
        }
        return;
    }
    // `drained=true` means all WAL-backed data is already in Delta, so even
    // recent segments go — Walrus startup stays independent of prior WAL size.
    match gc_wal_files(wal_dir, std::time::Duration::ZERO, None) {
        Ok((deleted, bytes_freed)) => info!("bootstrap.phase=wal_gc deleted={deleted} bytes_freed={bytes_freed} elapsed_ms={}", t.elapsed().as_millis()),
        Err(e) => warn!("bootstrap.phase=wal_gc error={e} elapsed_ms={}", t.elapsed().as_millis()),
    }
}

/// Slack subtracted from the durability floor before it bounds GC: covers
/// the insert path's append→bucket-record window and mtime granularity.
///
/// ASSUMPTION: the wall clock never steps BACKWARD by more than this slack; a
/// larger backward step can push an active file's mtime below `floor − slack`
/// while it still holds un-flushed entries, defeating both cutoff arms.
const GC_FLOOR_SLACK_MICROS: i64 = 10 * 60 * 1_000_000;
/// Directory under the WAL dir holding quarantined payloads (WAL entries that
/// failed to decode, DML groups that exhausted their drains). Exempt from
/// [`gc_wal_files`]: these bytes are the only remaining copy of that data.
pub const QUARANTINE_DIR_NAME: &str = "quarantine";
/// Subdir of the quarantine dir holding payloads that were successfully
/// re-ingested at boot (`redrive_quarantine`). Kept for forensics, but no
/// longer "awaiting a human" — excluded from [`quarantine_stats`].
pub(crate) const QUARANTINE_REDRIVEN_DIR_NAME: &str = "redriven";

/// Recursive `(payload_files, total_bytes)` under `<wal_dir>/quarantine`.
///
/// `payload_files` counts re-drivable items (`.bin` WAL entries, `.arrow` DML
/// groups) and excludes `.meta` sidecars, so it reads as "items awaiting a
/// human"; `bytes` bills everything on disk. Alert on payload_files > 0.
pub fn quarantine_stats(wal_dir: &std::path::Path) -> (usize, u64) {
    let (mut files, mut bytes) = (0usize, 0u64);
    let mut stack = vec![wal_dir.join(QUARANTINE_DIR_NAME)];
    while let Some(dir) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&dir) else { continue };
        for entry in rd.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                // `redriven/` holds already-re-ingested payloads: not pending loss.
                if entry.file_name() != QUARANTINE_REDRIVEN_DIR_NAME {
                    stack.push(entry.path());
                }
                continue;
            }
            bytes += meta.len();
            files += usize::from(matches!(std::path::Path::new(&entry.file_name()).extension().and_then(|e| e.to_str()), Some("bin" | "arrow")));
        }
    }
    (files, bytes)
}

/// Delete WAL files older than `max_age` by mtime, recursing into subdirs.
/// Skips dotfiles/dotdirs (`.timefusion_meta/`).
///
/// This is the FALLBACK for what walrus's own position-exact reclaim cannot
/// free: files pinned by dead/stalled shards whose cursor will never advance,
/// and foreign junk. mtime is a sufficient proxy for a file's newest entry —
/// walrus rotates to a new file once one is fully allocated.
///
/// `unflushed_floor_micros` makes the age heuristic sound: callers pass the
/// oldest WAL-append time any un-flushed data may depend on, and no file at or
/// after `floor − slack` is deleted whatever its age. `None` = no un-flushed
/// data ⇒ pure mtime. Without it, a crash loop's aged files ARE the backlog.
pub fn gc_wal_files(wal_dir: &std::path::Path, max_age: std::time::Duration, unflushed_floor_micros: Option<i64>) -> std::io::Result<(u64, u64)> {
    use std::time::SystemTime;
    let by_age = SystemTime::now().checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);
    let cutoff = unflushed_floor_micros.map_or(by_age, |floor| {
        by_age.min(SystemTime::UNIX_EPOCH + std::time::Duration::from_micros(floor.saturating_sub(GC_FLOOR_SLACK_MICROS).max(0) as u64))
    });
    let (mut deleted, mut bytes_freed) = (0u64, 0u64);
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
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                // Never recurse into quarantine: this walk deletes by mtime with
                // no name filter, and quarantined bytes are the ONLY copy of
                // data parked for a human to re-drive.
                if name.eq_ignore_ascii_case(QUARANTINE_DIR_NAME) {
                    continue;
                }
                stack.push(path);
                continue;
            }
            if meta.modified().unwrap_or(SystemTime::UNIX_EPOCH) < cutoff {
                match std::fs::remove_file(&path) {
                    Ok(()) => (deleted, bytes_freed) = (deleted + 1, bytes_freed + meta.len()),
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
    use test_case::test_case;

    use super::*;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false), Field::new("name", DataType::Utf8View, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(StringViewArray::from(vec!["a", "b", "c"]))]).unwrap()
    }

    /// Per-test-unique table name: a fixed topic inherits blocks/cursors from
    /// earlier tests in the same process and exact-position asserts flake.
    fn uniq(prefix: &str) -> String {
        format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
    }

    /// A WalManager rooted in `dir`. Takes `&TempDir` so the caller keeps the
    /// guard alive across drop/reopen (process A / process B) sequences.
    fn wal_in(dir: &tempfile::TempDir, mode: crate::config::WalFsyncMode, shards: usize) -> WalManager {
        WalManager::with_fsync_mode_and_shards(dir.path().to_path_buf(), mode, shards).unwrap()
    }

    /// The common case: 4 shards, fsync on every append.
    fn sync_wal(dir: &tempfile::TempDir) -> WalManager {
        wal_in(dir, crate::config::WalFsyncMode::SyncEach, 4)
    }

    /// A fresh dir, a WAL over it, and a unique table. Destructuring the tuple
    /// keeps the manager dropping before the dir it lives in.
    fn wal_fixture(prefix: &str, mode: crate::config::WalFsyncMode, shards: usize) -> (tempfile::TempDir, WalManager, String) {
        let dir = tempfile::tempdir().unwrap();
        let wal = wal_in(&dir, mode, shards);
        (dir, wal, uniq(prefix))
    }

    /// [`wal_fixture`] in the common case: 4 shards, fsync on every append.
    fn sync_fixture(prefix: &str) -> (tempfile::TempDir, WalManager, String) {
        wal_fixture(prefix, crate::config::WalFsyncMode::SyncEach, 4)
    }

    /// Append one batch and advance shard 0's persisted cursor to the write
    /// tail; round-robin picks shard 0 first for an unseen topic. 4-shard only.
    fn seed_shard0(wal: &WalManager, project: &str, table: &str) {
        wal.append(project, table, &create_test_batch()).unwrap();
        let tail = wal.current_position(project, table).unwrap();
        wal.merge_persisted_positions(project, table, &[Some(tail[0]), None, None, None]).unwrap();
    }

    /// `n` zero-padded decimal strings of exactly `width` bytes each.
    fn wide_strs(n: usize, width: usize) -> Vec<String> {
        (0..n).map(|i| format!("{i:0>width$}")).collect()
    }

    /// Read every entry of `("proj", table)`, asserting no frame failed to decode.
    fn read_all(wal: &WalManager, table: &str) -> Vec<WalEntry> {
        let (entries, errors) = wal.read_entries_raw("proj", table, None, true).unwrap();
        assert_eq!(errors, 0);
        entries
    }

    /// Process A over `dir`: seed shard 0, write a clean snapshot, return shard
    /// 0's position. The manager drops here so the caller can reopen the dir.
    fn seed_and_snapshot(dir: &tempfile::TempDir, table: &str) -> Option<WalPosition> {
        let wal = sync_wal(dir);
        seed_shard0(&wal, "proj", table);
        let shard0 = wal.persisted_read_positions("proj", table).unwrap()[0];
        wal.write_cursor_snapshot(true, true).unwrap();
        shard0
    }

    /// Single non-null `body: Utf8` column; payload size is controlled by row width.
    fn str_batch(strs: &[String]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)])),
            vec![Arc::new(arrow::array::StringArray::from(strs.iter().map(|s| s.as_str()).collect::<Vec<_>>()))],
        )
        .unwrap()
    }

    /// Poll until the blocked contender publishes its takeover request.
    async fn await_takeover_request(path: &std::path::Path, msg: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !takeover_requested(path) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect(msg);
    }

    /// A held WAL-dir lock plus a contender blocked on it, parked until the
    /// contender has published its takeover request.
    async fn blocked_contender(msg: &str) -> (tempfile::TempDir, WalDirLock, tokio::task::JoinHandle<Result<WalDirLock, WalError>>) {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_path_buf();
        let owner = WalDirLock::acquire(&path).await.unwrap();
        let contender = tokio::spawn(async move { WalDirLock::acquire(&path).await });
        await_takeover_request(tmp.path(), msg).await;
        (tmp, owner, contender)
    }

    /// `size` zero bytes at `root/rel`, parent dirs created; returns the path.
    fn touch(root: &std::path::Path, rel: &str, size: usize) -> PathBuf {
        let path = root.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, vec![0u8; size]).unwrap();
        path
    }

    /// Decode every split payload, asserting the splitter's size bound on each.
    fn decode_bounded(payloads: &[Vec<u8>], target: usize) -> Vec<RecordBatch> {
        payloads
            .iter()
            .map(|p| {
                assert!(p.len() <= target, "payload {} bytes exceeds target {target}", p.len());
                deserialize_record_batch(p).unwrap()
            })
            .collect()
    }

    // Arrow IPC decode hands every column a slice of one message-body
    // allocation, so each column reports the full body as its capacity: a
    // replayed batch is charged ~n_cols × message size unless the buffers are
    // privatized before entering a bucket.
    #[test]
    fn replayed_batch_charged_logical_not_message_body() {
        let (n_cols, n_rows) = (30usize, 50usize);
        let payload = wide_strs(n_rows, 100);
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

        let buffer = crate::write::mem_buffer::MemBuffer::new();
        buffer.insert("p1", "t1", replayed, ts).unwrap();
        let charged = buffer.estimated_memory_bytes();
        // logical ≈ 30 cols × 50 rows × 100B ≈ 150KB; without privatization
        // each column charges the ~190KB message body (~5.7MB total).
        assert!(charged < 1024 * 1024, "replayed ~150KB-logical batch charged {charged} bytes — IPC message-body slices are leaking into accounting");
    }

    fn payload_roundtrip<T: Encode + Decode<()>>(payload: &T) -> T {
        decode_payload(&bincode::encode_to_vec(payload, BINCODE_CONFIG).unwrap()).unwrap()
    }

    /// Every on-disk shape must survive its round-trip: the Arrow IPC batch,
    /// the framed `WalEntry` (every field), and every DML payload shape —
    /// including the `None` predicate (a bare `DELETE`/`UPDATE` over the whole
    /// table).
    #[test]
    fn wal_serialization_roundtrips() {
        let batch = create_test_batch();
        let deserialized = deserialize_record_batch(&serialize_record_batch(&batch).unwrap()).unwrap();
        assert_eq!(batch.num_rows(), deserialized.num_rows());
        assert_eq!(batch.num_columns(), deserialized.num_columns());

        let entry = WalEntry::new("project-123", "test_table", WalOperation::Insert, vec![1, 2, 3, 4, 5]);
        let back = deserialize_wal_entry(&serialize_wal_entry(&entry).unwrap()).unwrap();
        assert_eq!(
            (back.timestamp_micros, back.project_id, back.table_name, back.operation, back.data),
            (entry.timestamp_micros, entry.project_id, entry.table_name, entry.operation, entry.data)
        );

        for predicate_sql in [Some("id = 1".to_string()), None] {
            assert_eq!(payload_roundtrip(&DeletePayload { predicate_sql: predicate_sql.clone() }).predicate_sql, predicate_sql);
            let assignments = vec![("name".to_string(), "'updated'".to_string())];
            let back = payload_roundtrip(&UpdatePayload { predicate_sql: predicate_sql.clone(), assignments: assignments.clone() });
            assert_eq!((back.predicate_sql, back.assignments), (predicate_sql, assignments));
        }
    }

    /// The volatile replay API must not persist an intermediate cursor, and the
    /// final parked tail must survive reopen.
    #[serial_test::serial]
    #[test]
    fn volatile_replay_persists_only_the_final_parked_cursor() {
        let (dir, wal, table) = wal_fixture("volatile", crate::config::WalFsyncMode::None, 1);
        let batch = create_test_batch();
        for _ in 0..8 {
            wal.append_batch("proj", &table, std::slice::from_ref(&batch), |_, _| {}).unwrap();
        }

        let key = WalManager::walrus_topic_key("proj", &table, 0);
        let read = std::iter::from_fn(|| wal.wal.read_next_volatile_with_position(&key).unwrap()).count();
        assert_eq!(read, 8);
        assert_eq!(wal.wal.persisted_read_position(&key).unwrap(), None, "volatile replay must not fsync an intermediate cursor");

        let tail = wal.current_position_for_shard("proj", &table, 0).unwrap();
        wal.set_positions_allow_rewind("proj", &table, &[Some(tail)]).unwrap();
        drop(wal);

        let reopened = wal_in(&dir, crate::config::WalFsyncMode::None, 1);
        assert!(reopened.is_fully_consumed().unwrap(), "the one final parked cursor write must survive reopen");
    }

    /// The replay size cap must be enforced at APPEND time (by splitting), so
    /// every acked entry is replayable by construction.
    #[serial_test::serial]
    #[test]
    fn oversized_insert_append_survives_replay() {
        let (_dir, wal, table) = wal_fixture("big", crate::config::WalFsyncMode::None, 2);

        // ~112MB of string payload (35k rows × 3.2KB) — over WAL_SPLIT_TARGET.
        let n_rows = 35_000;
        let batch = str_batch(&wide_strs(n_rows, 3200));

        wal.append_batch("proj", &table, std::slice::from_ref(&batch), |_, _| {}).unwrap();

        let entries = read_all(&wal, &table);
        let rows: usize =
            entries.iter().map(|e| deserialize_record_batch(&e.data).expect("every acked WAL entry must be replayable (within the size cap)").num_rows()).sum();
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
        let (target, hard_max) = (8 * 1024, 32 * 1024);
        let expected = wide_strs(100, 200);
        let payloads = split_to_wal_payloads(&str_batch(&expected), target, hard_max).unwrap();
        assert!(payloads.len() > 1);
        let rows: Vec<String> = decode_bounded(&payloads, target)
            .iter()
            .flat_map(|b| {
                let col = b.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
                (0..col.len()).map(|i| col.value(i).to_string()).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(rows, expected, "rows preserved in order");

        let whole = split_to_wal_payloads(&str_batch(&["x".repeat(16 * 1024)]), target, hard_max).unwrap();
        assert_eq!(whole.len(), 1, "a single row between target and hard cap must pass through whole");
        assert_eq!(deserialize_record_batch(&whole[0]).unwrap().num_rows(), 1);

        assert!(
            matches!(split_to_wal_payloads(&str_batch(&["x".repeat(64 * 1024)]), target, hard_max), Err(WalError::BatchTooLarge { .. })),
            "a single row over the hard cap must fail the append explicitly, not ack-then-drop"
        );
    }

    /// Dictionary-encoded columns defeat row-boundary splitting: every IPC
    /// stream carries the full dictionary, so halving rows doesn't halve bytes.
    /// The splitter must flatten dictionaries first and still bound every payload.
    #[test]
    fn split_to_wal_payloads_flattens_dictionary_columns() {
        use arrow::array::{Array, DictionaryArray, Int32Array, StringArray};
        let values = StringArray::from(wide_strs(200, 2048));
        let keys = Int32Array::from((0..1000).map(|i| i % 200).collect::<Vec<i32>>());
        let dict = DictionaryArray::<arrow::datatypes::Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("body", dict.data_type().clone(), false)])), vec![Arc::new(dict)]).unwrap();

        let (target, hard_max) = (64 * 1024, 1024 * 1024);
        let payloads = split_to_wal_payloads(&batch, target, hard_max).expect("dictionary batch must split, not explode or bail");
        assert!(payloads.len() > 1);
        assert!(payloads.len() < 100, "split degenerated toward per-row entries: {} payloads", payloads.len());
        let rows: usize = decode_bounded(&payloads, target).iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1000, "no rows lost across the dictionary split");
    }

    /// UPDATE...FROM sources can't be split without changing join semantics —
    /// an over-cap source must fail the append (client-visible) instead of
    /// acking an entry the next boot's replay will reject.
    #[serial_test::serial]
    #[test]
    fn append_update_with_source_rejects_oversized_source() {
        let (_dir, wal, _) = wal_fixture("upd", crate::config::WalFsyncMode::None, 2);
        let source = SerializedSource { join_keys: vec![("id".to_string(), "id".to_string())], batch_ipc: vec![0u8; MAX_BATCH_SIZE + 1] };
        let res = wal.append_update_with_source("proj", "tbl", None, &[], &source, |_, _| {});
        assert!(matches!(res, Err(WalError::BatchTooLarge { .. })));
    }

    /// `walrus_topic_key` must produce the same bytes across builds; a change
    /// silently strands WAL entries on upgrade. Shape: 16-hex-char FNV-1a plus
    /// a "-NN" shard suffix. Changing the encoding requires a WAL_VERSION bump.
    #[test_case("project", "table", 0 => "d8751a406eed3d9a-00".to_string() ; "shard 0 suffix")]
    #[test_case("p1", "otel_logs_and_spans", 3 => "ae0768bab343abd1-03".to_string() ; "shard 3 suffix")]
    fn walrus_topic_key_is_stable(project: &str, table: &str, shard: usize) -> String {
        WalManager::walrus_topic_key(project, table, shard)
    }

    /// Distinct (project_id, table_name) tuples must map to distinct walrus
    /// keys even when an input embeds the separator (length-prefix encoding).
    #[test_case(("ab", "c"), ("a", "bc") ; "boundary slide")]
    #[test_case(("a:b", "c"), ("a", "b:c") ; "separator inside an input")]
    #[test_case(("a", ""), ("", "a") ; "empty and non empty swap")]
    #[test_case(("aa", ""), ("a", "a") ; "boundary slide with empty")]
    fn walrus_topic_key_no_collisions(a: (&str, &str), b: (&str, &str)) {
        assert_ne!(WalManager::walrus_topic_key(a.0, a.1, 0), WalManager::walrus_topic_key(b.0, b.1, 0), "{a:?} and {b:?} collide");
    }

    /// `TIMEFUSION_WAL_ACK_FSYNC` plumbing: single-entry (DML) appends sync the
    /// shard before returning and stay readable. Guards the sync_topic call path.
    #[serial_test::serial]
    #[test]
    fn ack_fsync_appends_are_synced_and_readable() {
        let (_dir, wal, table) = wal_fixture("tbl", crate::config::WalFsyncMode::Milliseconds(60_000), 2);
        let wal = wal.with_ack_fsync(true);

        wal.append_delete("proj", &table, Some("id = 'x'"), |_, _| {}).unwrap();
        wal.append_batch("proj", &table, &[create_test_batch()], |_, _| {}).unwrap();

        // checkpoint=true: walrus's uncheckpointed read_next never advances the
        // cursor, so a read-all loop with `false` re-reads the first entry forever.
        let entries = read_all(&wal, &table);
        assert_eq!(entries.len(), 2, "both appends must be present and readable with ack_fsync on");
        assert!(entries.iter().any(|e| e.operation == WalOperation::Delete));
        assert!(entries.iter().any(|e| e.operation == WalOperation::Insert));
    }

    /// Concurrent appends to a *single* topic must queue, not error. Walrus
    /// rejects concurrent appends to one collection, and more concurrent writers
    /// than shards collide; the per-collection `append_lock` serializes them.
    #[serial_test::serial]
    #[test]
    fn concurrent_appends_same_topic_do_not_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (_dir, wal, table) = wal_fixture("tbl", crate::config::WalFsyncMode::None, 4);

        // Far more concurrent writers than the 4 shards → guaranteed same-shard
        // collisions under round-robin.
        let errors = AtomicUsize::new(0);
        std::thread::scope(|s| {
            for _ in 0..32 {
                s.spawn(|| {
                    let batch = create_test_batch();
                    let source = SerializedSource { join_keys: vec![("id".into(), "id".into())], batch_ipc: vec![1, 2, 3] };
                    for i in 0..8 {
                        // Interleaved so a collision exercises both append paths.
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

    /// Rewind marker round-trip: capture P0, consume the cursor, apply the
    /// marker → the entry is readable again. A never-persisted shard's P0 must
    /// come back as an explicit ORIGIN hold; removal is idempotent.
    #[test]
    #[serial_test::serial]
    fn recovery_rewind_marker_restores_consumed_cursor() {
        let (_dir, wal, table) = sync_fixture("rw");
        wal.append("proj", &table, &create_test_batch()).unwrap();

        let p0 = wal.write_recovery_rewind_marker().unwrap();
        let holds = &p0[&("proj".to_string(), table.clone())];
        assert!(holds.iter().all(Option::is_some), "P0 must map never-persisted shards to explicit ORIGIN holds, got {holds:?}");

        // Simulate a crashed replay: consume to tail (persists progress).
        assert_eq!(read_all(&wal, &table).len(), 1);
        assert!(read_all(&wal, &table).is_empty(), "cursor must be at tail after consuming");

        assert!(wal.apply_recovery_rewind_marker().unwrap(), "marker must be found and applied");
        assert_eq!(read_all(&wal, &table).len(), 1, "rewind must make the consumed entry replayable again");

        wal.remove_recovery_rewind_marker();
        assert!(!wal.apply_recovery_rewind_marker().unwrap(), "marker gone after removal");
        wal.remove_recovery_rewind_marker(); // idempotent
    }

    /// The zero-replay proof is position-exact: an appended entry makes the
    /// WAL non-consumed, advancing only to an earlier position is still
    /// non-consumed, and advancing to the actual tail makes it consumed.
    /// Empty shards (`None` cursor, origin tail) are harmless.
    #[test]
    #[serial_test::serial]
    fn fully_consumed_requires_every_cursor_at_its_exact_tail() {
        let (_dir, wal, table) = sync_fixture("fc");

        assert!(wal.is_fully_consumed().unwrap(), "an empty WAL has nothing to replay");
        wal.append("proj", &table, &create_test_batch()).unwrap();
        assert!(!wal.is_fully_consumed().unwrap(), "an append beyond an absent cursor must replay");

        let tails = wal.current_position("proj", &table).unwrap();
        wal.merge_persisted_positions("proj", &table, &tails.iter().copied().map(Some).collect::<Vec<_>>()).unwrap();
        assert!(wal.is_fully_consumed().unwrap(), "cursor equality with every shard tail proves zero replay");
        assert!(wal.can_skip_delta_reconcile().unwrap(), "a fully consumed WAL needs no remote cursor derivation");

        // A marker means those tail cursors may be consumed-ahead state from a
        // crashed replay, so it must veto the remote-scan shortcut before it is
        // applied and rewinds them.
        wal.write_recovery_rewind_marker().unwrap();
        assert!(!wal.can_skip_delta_reconcile().unwrap(), "an interrupted-recovery marker must retain Delta reconciliation");
        wal.remove_recovery_rewind_marker();

        wal.append("proj", &table, &create_test_batch()).unwrap();
        assert!(!wal.is_fully_consumed().unwrap(), "a late accepted write must invalidate the zero-replay proof");
    }

    /// Round-trip cursor snapshot: the on-disk file alone must seed walrus's
    /// known_topics on a fresh process without touching Delta. Exercises the
    /// *idempotent* path (restore advances nothing); the rescue path is
    /// [`cursor_snapshot_restore_advances_walrus_past_local_state`].
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_roundtrip_restores_persisted_positions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let table = uniq("tbl");

        // Process A: append, advance cursor, write snapshot with clean flag.
        let before = seed_and_snapshot(&dir, &table);
        assert!(before.is_some_and(|p| !p.is_origin()), "advance must move shard 0 off origin");
        assert!(path.join(".timefusion_meta/cursor_snapshot.json").exists());

        // Process B: fresh manager, snapshot present.
        {
            let wal = sync_wal(&dir);
            let snap = wal.load_cursor_snapshot().expect("snapshot loadable");
            assert!(snap.clean_shutdown);
            assert_eq!(snap.shards_per_topic, 4);
            assert!(snap.entries.contains_key(&WalManager::make_topic("proj", &table)));

            let advanced = wal.restore_cursor_snapshot(&snap).unwrap();
            assert_eq!(advanced, 0, "snapshot positions match walrus's own fsynced state");
            assert!(wal.list_topic_pairs().iter().any(|(p, t)| p == "proj" && *t == table));
        }
    }

    /// An unloadable snapshot must return None so boot falls through to the
    /// Delta scan. Rejected: version mismatch, and shard-count mismatch (a
    /// changed `TIMEFUSION_WAL_SHARDS_PER_TOPIC` would seed misaligned positions).
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_rejects_version_or_shard_count_mismatch() {
        // Version mismatch.
        let (dir, wal, _) = sync_fixture("ver");
        std::fs::write(
            dir.path().join(".timefusion_meta/cursor_snapshot.json"),
            br#"{"version":999,"written_at_micros":0,"shards_per_topic":4,"clean_shutdown":true,"entries":{}}"#,
        )
        .unwrap();
        assert!(wal.load_cursor_snapshot().is_none(), "version mismatch must be rejected");

        // Shard-count mismatch: write a 4-shard snapshot, re-open with 8.
        let dir = tempfile::tempdir().unwrap();
        let table = uniq("tbl");
        let _ = seed_and_snapshot(&dir, &table);
        let wal = wal_in(&dir, crate::config::WalFsyncMode::SyncEach, 8);
        assert!(wal.load_cursor_snapshot().is_none(), "shard-count mismatch must be rejected");
    }

    /// Rescue path: walrus has no fsynced state for this topic (a crash lost
    /// the persisted cursor while the WAL files survived), so restoring a
    /// snapshot past origin must move `persisted_read_position` forward.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_restore_advances_walrus_past_local_state() {
        let (_dir, wal, table) = sync_fixture("rescue");
        let project = "p";

        let before = wal.persisted_read_positions(project, &table).unwrap();
        assert!(before.iter().all(Option::is_none), "fresh walrus key must have no persisted cursor");

        let entries = std::collections::BTreeMap::from([(WalManager::make_topic(project, &table), vec![Some((7u64, 42u64)), None, Some((3, 0)), None])]);
        let snap = CursorSnapshot { version: SNAPSHOT_VERSION, written_at_micros: 0, shards_per_topic: 4, clean_shutdown: true, drained: false, entries };
        let tables_advanced = wal.restore_cursor_snapshot(&snap).unwrap();
        assert_eq!(tables_advanced, 1, "the one snapshot table must advance from origin");

        let after = wal.persisted_read_positions(project, &table).unwrap();
        assert_eq!(after[0].map(|p| (p.block_id, p.offset)), Some((7, 42)));
        assert_eq!(after[2].map(|p| (p.block_id, p.offset)), Some((3, 0)));
    }

    /// A crash between tmp-write and rename leaves `cursor_snapshot.json.tmp`;
    /// the next WalManager init must sweep it so it cannot accumulate.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_tmp_swept_on_init() {
        let dir = tempfile::tempdir().unwrap();
        drop(sync_wal(&dir));
        let tmp = dir.path().join(".timefusion_meta/cursor_snapshot.json.tmp");
        std::fs::write(&tmp, b"partial").unwrap();
        assert!(tmp.exists());
        drop(sync_wal(&dir));
        assert!(!tmp.exists(), "init must sweep leftover tmp file");
    }

    /// Read-only meta dir: both the write and the follow-up delete must return
    /// Err cleanly without panicking, so the flush task can carry on.
    #[cfg(unix)]
    #[test]
    #[serial_test::serial]
    fn write_and_delete_both_fail_under_readonly_meta_dir() {
        use std::os::unix::fs::PermissionsExt;
        let (dir, wal, _) = sync_fixture("ro");
        wal.write_cursor_snapshot(true, true).unwrap();
        let meta = dir.path().join(".timefusion_meta");
        let target = meta.join("cursor_snapshot.json");

        let original = std::fs::metadata(&meta).unwrap().permissions();
        std::fs::set_permissions(&meta, std::fs::Permissions::from_mode(0o555)).unwrap();

        assert!(wal.write_cursor_snapshot(false, false).is_err(), "write into RO dir must fail");
        assert!(wal.delete_cursor_snapshot().is_err(), "unlink under RO parent must fail");
        assert!(target.exists(), "stale snapshot survives both failures");

        // Restore so tempdir teardown can clean up.
        std::fs::set_permissions(&meta, original).unwrap();
    }

    /// If `write_cursor_snapshot` fails after a previous good write, the caller
    /// must remove the now-stale file so the next boot's verifier doesn't trust
    /// it. `delete_cursor_snapshot` is asserted idempotent here too.
    #[test]
    #[serial_test::serial]
    fn write_cursor_snapshot_failure_requires_caller_to_delete_stale_file() {
        let (dir, wal, _) = sync_fixture("stale");
        let path = dir.path();
        wal.delete_cursor_snapshot().unwrap(); // missing → Ok
        wal.write_cursor_snapshot(true, true).unwrap();
        let target = path.join(".timefusion_meta/cursor_snapshot.json");
        let tmp = path.join(".timefusion_meta/cursor_snapshot.json.tmp");
        assert!(target.exists());

        // Force the next write to fail by squatting on the tmp path with a dir.
        std::fs::create_dir(&tmp).unwrap();
        assert!(wal.write_cursor_snapshot(false, false).is_err(), "tmp-path collision must fail the write");
        assert!(target.exists(), "stale snapshot still on disk after failed write");

        wal.delete_cursor_snapshot().unwrap();
        assert!(!target.exists(), "delete clears the stale snapshot");
        wal.delete_cursor_snapshot().unwrap(); // second call must still be Ok
    }

    /// A snapshot written from the flush path (clean_shutdown=false) loads
    /// fine but must not let the boot path skip the Delta verifier — that
    /// gate is reserved for the graceful-shutdown marker.
    #[test]
    #[serial_test::serial]
    fn cursor_snapshot_dirty_path_loads_but_signals_unclean() {
        let (_dir, wal, table) = sync_fixture("tbl");
        seed_shard0(&wal, "proj", &table);
        wal.write_cursor_snapshot(false, false).unwrap();

        let snap = wal.load_cursor_snapshot().expect("dirty snapshot must still be loadable");
        assert!(!snap.clean_shutdown, "dirty snapshot must not claim clean_shutdown");
        let tables_advanced = wal.restore_cursor_snapshot(&snap).unwrap();
        assert_eq!(tables_advanced, 0);
    }

    #[tokio::test]
    async fn wal_dir_lock_is_exclusive_and_releases_on_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let guard = WalDirLock::acquire(tmp.path()).await.unwrap();
        let other = std::fs::OpenOptions::new().read(true).write(true).open(tmp.path().join(".timefusion_meta/wal.lock")).unwrap();
        assert!(!other.try_lock_exclusive().unwrap(), "second opener must not acquire the lock while the guard is held");
        drop(guard);
        assert!(other.try_lock_exclusive().unwrap(), "lock must be acquirable after the holder drops");
    }

    /// The predecessor escalates on how long a takeover request has gone
    /// unanswered, so the request's timestamp must stay that of the FIRST ask —
    /// refreshing it on each poll would keep the escalation from ever firing.
    #[tokio::test]
    async fn a_takeover_request_keeps_its_original_timestamp_while_the_contender_polls() {
        let (tmp, _owner, contender) = blocked_contender("contender must request a takeover").await;
        let path = tmp.path();
        let request = || std::fs::read_to_string(meta_path(path, TAKEOVER_REQUEST_FILE)).unwrap();
        let first = request();

        // Poll well past the contender's ~10s rewrite interval.
        tokio::time::sleep(std::time::Duration::from_millis(11_000)).await;
        assert_eq!(first, request(), "the request must not be refreshed, or its age can never reach the escalation threshold");
        assert!(
            takeover_request_age(path).is_some_and(|age| age >= std::time::Duration::from_secs(10)),
            "the age must grow with wall clock: {:?}",
            takeover_request_age(path)
        );
        contender.abort();
    }

    #[tokio::test]
    async fn wal_lock_contender_requests_takeover_and_clears_marker_on_acquire() {
        let (tmp, owner, contender) = blocked_contender("blocked replacement must publish a takeover request").await;
        drop(owner);
        let replacement = tokio::time::timeout(std::time::Duration::from_secs(2), contender).await.unwrap().unwrap().unwrap();
        assert!(!takeover_requested(tmp.path()), "new WAL owner must clear the consumed request");
        drop(replacement);
    }

    /// One GC contract over four sweep configurations, returning
    /// `(deleted, bytes_freed)`. Only aged, unprotected WAL segments are
    /// reclaimable: the durability floor overrides mtime age (during a crash
    /// loop the aged files ARE the un-flushed backlog), `.timefusion_meta` is
    /// exempt, and the walk — which deletes ANY file past the cutoff with no
    /// name filter — must never descend into `quarantine/`, the ONLY copy of
    /// data parked for a human. `floor_offset_secs` is relative to now.
    #[test_case(3600, None => (0, 0) ; "segments younger than max_age are kept")]
    #[test_case(0, None => (2, 3072) ; "past the cutoff only wal segments are reclaimed")]
    #[test_case(0, Some(-3600) => (0, 0) ; "unflushed floor overrides mtime age")]
    #[test_case(0, Some(3600) => (2, 3072) ; "a future floor adds no protection beyond age")]
    fn gc_wal_files_reclaims_only_aged_unprotected_segments(max_age_secs: u64, floor_offset_secs: Option<i64>) -> (u64, u64) {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let meta = [touch(root, ".timefusion_meta/cursor_snapshot.json", 2), touch(root, ".timefusion_meta/topics", 0)];
        let segs = [touch(root, "1779989695814", 1024), touch(root, "1780994113609", 2048)];
        let parked = [
            touch(root, "quarantine/1779989695814_insert_corrupt_p__t.bin", 256),
            touch(root, "quarantine/dml/1785139919343391_abc_p__t.arrow", 256),
            touch(root, "quarantine/dml/1785139919343391_abc_p__t.meta", 256),
        ];

        let floor = floor_offset_secs.map(|secs| chrono::Utc::now().timestamp_micros() + secs * 1_000_000);
        let swept = gc_wal_files(root, std::time::Duration::from_secs(max_age_secs), floor).unwrap();

        assert!(meta.iter().all(|p| p.exists()), "meta dir must be skipped");
        assert!(parked.iter().all(|p| p.exists()), "quarantine payloads and sidecars must never be reclaimed by GC");
        assert_eq!(segs.iter().filter(|p| p.exists()).count(), 2 - swept.0 as usize, "the deleted count must match the segments actually gone");
        swept
    }

    /// The alertable count must include BOTH the flat `quarantine/*.bin` WAL
    /// entries and the nested `quarantine/dml/*` groups.
    #[test]
    fn quarantine_stats_counts_both_tiers_recursively() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        // A WAL segment outside quarantine must NOT be counted.
        touch(root, "1779989695814", 4096);
        touch(root, "quarantine/a_insert_corrupt.bin", 100);
        touch(root, "quarantine/dml/g.arrow", 250);
        touch(root, "quarantine/dml/g.meta", 50);

        // Sidecars and redriven/ copies are billed in bytes but not counted as
        // parked items awaiting re-drive.
        touch(root, "quarantine/redriven/old_insert_corrupt.bin", 900);
        let (files, bytes) = quarantine_stats(root);
        assert_eq!(files, 2, "one .bin + one .arrow payload");
        assert_eq!(bytes, 400, "all quarantine bytes incl. the .meta sidecar");

        // Absent dir must be zero, not an error.
        let empty = tempfile::tempdir().unwrap();
        assert_eq!(quarantine_stats(empty.path()), (0, 0));
    }

    /// Boot GC gating: `clean_shutdown=true` is NOT a drain claim (shutdown
    /// writes it even after a partial flush), so only `drained=true` authorizes
    /// the sweep — and it must be CONSUMED by the boot that uses it.
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
        make_old_file();
        boot_wal_gc(root); // missing snapshot → skip
        std::fs::write(&snap_path, b"not json").unwrap();
        boot_wal_gc(root); // unreadable → skip
        write_snap(true, false, SNAPSHOT_VERSION);
        boot_wal_gc(root); // clean but NOT drained (partial flush) → skip
        write_snap(false, true, SNAPSHOT_VERSION);
        boot_wal_gc(root); // inconsistent dirty+drained claim → fail closed
        write_snap(true, true, 999);
        boot_wal_gc(root); // version mismatch → skip
        assert!(old_file.exists(), "un-drained/unreadable snapshots must never authorize the mtime sweep");

        // drained=true authorizes exactly one complete generation sweep —
        // recent segments and the read index included — then is consumed.
        let recent_file = touch(root, "1780000000000", 6);
        let read_index = touch(root, "read_offset_idx_index.db", 9);
        write_snap(true, true, SNAPSHOT_VERSION);
        boot_wal_gc(root);
        assert!(!old_file.exists(), "drained snapshot must run the sweep");
        assert!(!recent_file.exists(), "drained sweep must remove recent WAL segments");
        assert!(!read_index.exists(), "drained sweep must remove positions for the deleted WAL generation");
        let reread: CursorSnapshot = serde_json::from_slice(&std::fs::read(&snap_path).unwrap()).unwrap();
        assert!(!reread.drained, "the drained claim must be consumed by the boot that used it");
        assert!(reread.clean_shutdown, "consuming drained must not clobber the cursor-restore flag");
        assert!(reread.entries.is_empty(), "positions from deleted WAL generations must not be restored");
        make_old_file();
        boot_wal_gc(root);
        assert!(old_file.exists(), "second boot must not reuse the consumed drained claim");

        // Fail closed: if the drained flag cannot be durably consumed, the
        // sweep must NOT run.
        write_snap(true, true, SNAPSHOT_VERSION);
        std::fs::create_dir_all(meta.join("cursor_snapshot.json.tmp")).unwrap(); // blocks File::create(tmp)
        boot_wal_gc(root);
        assert!(old_file.exists(), "sweep must be skipped when the drained consume fails");
        assert!(!snap_path.exists(), "unconsumable drained snapshot must be deleted (fail closed)");
    }

    #[test]
    fn gc_wal_files_handles_missing_dir() {
        // Pre-init sweep hits a not-yet-created dir; must not error.
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        let (deleted, bytes_freed) = gc_wal_files(&missing, std::time::Duration::ZERO, None).unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(bytes_freed, 0);
    }

    /// `WalEntry::data` must NOT carry `#[bincode(with_serde)]`: bincode's
    /// native `Vec<u8>` impl produces the same bytes far faster. If a future
    /// upgrade breaks that byte identity, this fails and the change needs a
    /// WAL version bump instead.
    #[test]
    fn wal_payload_encoding_is_identical_with_and_without_serde() {
        #[derive(Encode, Decode)]
        struct ViaSerde {
            #[bincode(with_serde)]
            data: Vec<u8>,
        }
        #[derive(Encode, Decode)]
        struct Native {
            data: Vec<u8>,
        }
        // Arrow IPC-like: uniformly random bytes, so ~50% are >= 128.
        let payload: Vec<u8> = (0..86_408u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
        let s = bincode::encode_to_vec(ViaSerde { data: payload.clone() }, BINCODE_CONFIG).unwrap();
        let n = bincode::encode_to_vec(Native { data: payload }, BINCODE_CONFIG).unwrap();
        assert_eq!(s, n, "wire format must be unchanged for this to be a safe swap");
        let (via_serde, _): (ViaSerde, _) = bincode::decode_from_slice(&s, BINCODE_CONFIG).unwrap();
        let (native, _): (Native, _) = bincode::decode_from_slice(&n, BINCODE_CONFIG).unwrap();
        assert_eq!(via_serde.data, native.data);
    }
}
