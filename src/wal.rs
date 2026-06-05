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
    pub version:           u32,
    /// Wall-clock micros (`clock::now_micros`) at write time. Informational
    /// only — surfaced in the boot log so operators can spot a stale
    /// snapshot, but not enforced as a max-age gate.
    pub written_at_micros: i64,
    pub shards_per_topic:  usize,
    /// True only when written by the graceful-shutdown path. Boot uses this
    /// flag to decide whether the Delta verifier can be skipped entirely.
    pub clean_shutdown:    bool,
    /// `"project_id:table_name"` → per-shard cursor (None = never written).
    pub entries:           std::collections::BTreeMap<String, Vec<Option<SnapPos>>>,
}

/// Maximum size for a single record batch (100MB) - prevents unbounded memory allocation from malicious/corrupted WAL
const MAX_BATCH_SIZE: usize = 100 * 1024 * 1024;
/// Fsync schedule interval in milliseconds - balances durability with performance
const FSYNC_SCHEDULE_MS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
#[repr(u8)]
pub enum WalOperation {
    Insert = 0,
    Delete = 1,
    Update = 2,
}

impl TryFrom<u8> for WalOperation {
    type Error = WalError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(WalOperation::Insert),
            1 => Ok(WalOperation::Delete),
            2 => Ok(WalOperation::Update),
            _ => Err(WalError::InvalidOperation(value)),
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id:       String,
    pub table_name:       String,
    pub operation:        WalOperation,
    #[bincode(with_serde)]
    pub data:             Vec<u8>,
}

impl WalEntry {
    fn new(project_id: &str, table_name: &str, operation: WalOperation, data: Vec<u8>) -> Self {
        Self {
            timestamp_micros: chrono::Utc::now().timestamp_micros(),
            project_id: project_id.into(),
            table_name: table_name.into(),
            operation,
            data,
        }
    }
}

#[derive(Debug, Encode, Decode)]
pub struct DeletePayload {
    pub predicate_sql: Option<String>,
}

#[derive(Debug, Encode, Decode)]
pub struct UpdatePayload {
    pub predicate_sql: Option<String>,
    pub assignments:   Vec<(String, String)>,
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

pub struct WalManager {
    wal:              Walrus,
    data_dir:         PathBuf,
    /// Logical topic strings ("{project_id}:{table_name}") — one entry per
    /// (project, table). Each maps to `shards_per_topic` walrus collections.
    known_topics:     DashSet<String>,
    /// Per-topic round-robin counter chooses which shard the next batch is
    /// appended to. Topic-scoped (rather than global) so we don't penalize
    /// the cold-cache miss for an idle topic.
    shard_counter:    dashmap::DashMap<String, std::sync::atomic::AtomicU64>,
    shards_per_topic: usize,
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
        info!(
            "WAL initialized at {:?}, known topics: {}, shards/topic: {}",
            data_dir,
            known_topics.len(),
            shards_per_topic
        );
        Ok(Self {
            wal,
            data_dir,
            known_topics,
            shard_counter: dashmap::DashMap::new(),
            shards_per_topic,
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

        let has_walrus_state = std::fs::read_dir(data_dir)
            .map(|rd| rd.flatten().any(|e| e.file_name() != ".timefusion_meta" && e.file_name() != "wal_version"))
            .unwrap_or(false);

        match std::fs::read_to_string(&stamp_path) {
            Ok(s) => {
                let on_disk: u8 = s.trim().parse().map_err(|_| WalError::UnsupportedVersion {
                    version:  0,
                    expected: WAL_VERSION,
                })?;
                if on_disk != WAL_VERSION {
                    error!(
                        "WAL on-disk version {} != binary version {}. IN-FLIGHT DATA WILL BE LOST \
                         IF YOU PROCEED. Wipe {:?} to start fresh, or run a matching binary.",
                        on_disk, WAL_VERSION, data_dir
                    );
                    return Err(WalError::UnsupportedVersion {
                        version:  on_disk,
                        expected: WAL_VERSION,
                    });
                }
                Ok(())
            }
            Err(_) if has_walrus_state => {
                error!(
                    "WAL directory {:?} has data but no version stamp (pre-stamp legacy). \
                     Wipe the directory to start fresh on WAL v{}.",
                    data_dir, WAL_VERSION
                );
                Err(WalError::UnsupportedVersion {
                    version:  0,
                    expected: WAL_VERSION,
                })
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

    /// Returns the shard the entry was appended to. Callers must record this
    /// against their MemBuffer bucket so the WAL cursor can later be advanced
    /// by exactly the right count per shard (see `advance_by_counts`).
    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let entry = WalEntry::new(project_id, table_name, WalOperation::Insert, serialize_record_batch(batch)?);
        self.wal.append_for_topic(&walrus_key, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!("WAL append INSERT: topic={}, shard={}, rows={}", topic, shard, batch.num_rows());
        Ok(shard)
    }

    /// Returns `(shard, entry_count)` — every batch becomes one walrus entry
    /// on the chosen shard, so `entry_count == batches.len()`.
    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn append_batch(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) -> Result<(usize, usize), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let payloads: Vec<Vec<u8>> = batches
            .iter()
            .map(|batch| serialize_wal_entry(&WalEntry::new(project_id, table_name, WalOperation::Insert, serialize_record_batch(batch)?)))
            .collect::<Result<_, _>>()?;

        let payload_refs: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
        self.wal.batch_append_for_topic(&walrus_key, &payload_refs)?;
        self.persist_topic(&topic);
        debug!("WAL batch append INSERT: topic={}, shard={}, batches={}", topic, shard, batches.len());
        Ok((shard, batches.len()))
    }

    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn append_delete(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let data = bincode::encode_to_vec(
            &DeletePayload {
                predicate_sql: predicate_sql.map(String::from),
            },
            BINCODE_CONFIG,
        )?;
        let entry = WalEntry::new(project_id, table_name, WalOperation::Delete, data);
        self.wal.append_for_topic(&walrus_key, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!("WAL append DELETE: topic={}, shard={}, predicate={:?}", topic, shard, predicate_sql);
        Ok(shard)
    }

    #[instrument(skip(self, assignments), fields(project_id, table_name))]
    pub fn append_update(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)]) -> Result<usize, WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let payload = UpdatePayload {
            predicate_sql: predicate_sql.map(String::from),
            assignments:   assignments.to_vec(),
        };
        let entry = WalEntry::new(project_id, table_name, WalOperation::Update, bincode::encode_to_vec(&payload, BINCODE_CONFIG)?);
        self.wal.append_for_topic(&walrus_key, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!(
            "WAL append UPDATE: topic={}, shard={}, predicate={:?}, assignments={}",
            topic,
            shard,
            predicate_sql,
            assignments.len()
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

    /// Stream every WAL entry past `since_timestamp_micros` through `callback`.
    /// Bounded recovery memory: at most one entry per shard is alive at a
    /// time, vs the old `read_all_entries_raw` which materialized the entire
    /// post-cutoff slice (millions of entries / GiBs at long retention) into
    /// a Vec.
    ///
    /// Within each topic, the N shard streams are merged by `timestamp_micros`
    /// using a min-heap (k-way merge), so DELETE-after-INSERT ordering within
    /// a topic is preserved even when those operations happen on different
    /// shards. Cross-topic ordering is not preserved — that's fine because
    /// DELETE and UPDATE only mutate their own topic's MemBuffer.
    #[instrument(skip(self, callback))]
    pub fn for_each_entry<F>(&self, since_timestamp_micros: Option<i64>, checkpoint: bool, mut callback: F) -> Result<(u64, usize), WalError>
    where
        F: FnMut(WalEntry),
    {
        use std::{cmp::Reverse, collections::BinaryHeap};

        let cutoff = since_timestamp_micros.unwrap_or(0);
        let mut total_entries = 0u64;
        let mut total_errors = 0usize;

        for topic in self.list_topics()? {
            let Some((project_id, table_name)) = Self::parse_topic(&topic) else {
                continue;
            };

            // Prime the heap with each shard's first eligible entry. Heap is
            // keyed by (timestamp, shard) so smaller timestamps come out first;
            // shard index breaks ties deterministically. The entry payload
            // travels alongside the key in a parallel Vec slot indexed by
            // shard, avoiding the `Ord` bound on `WalEntry`.
            //
            // Invariant: at most one in-flight entry per shard is alive at a
            // time → recovery memory is O(shards_per_topic), not O(total entries).
            let mut heap: BinaryHeap<Reverse<(i64, usize)>> = BinaryHeap::with_capacity(self.shards_per_topic);
            let shard_keys: Vec<String> = (0..self.shards_per_topic).map(|s| Self::walrus_topic_key(&project_id, &table_name, s)).collect();
            let mut pending: Vec<Option<WalEntry>> = (0..self.shards_per_topic).map(|_| None).collect();
            for shard in 0..self.shards_per_topic {
                if let Some(entry) = Self::next_eligible_from_shard(&self.wal, &shard_keys[shard], cutoff, checkpoint, &mut total_errors) {
                    heap.push(Reverse((entry.timestamp_micros, shard)));
                    pending[shard] = Some(entry);
                }
            }

            while let Some(Reverse((_, shard))) = heap.pop() {
                let entry = pending[shard].take().expect("heap and pending out of sync");
                total_entries += 1;
                callback(entry);
                if let Some(next) = Self::next_eligible_from_shard(&self.wal, &shard_keys[shard], cutoff, checkpoint, &mut total_errors) {
                    heap.push(Reverse((next.timestamp_micros, shard)));
                    pending[shard] = Some(next);
                }
            }
        }

        if total_errors > 0 {
            warn!("WAL read all: total_entries={}, cutoff={}, errors={}", total_entries, cutoff, total_errors);
        } else {
            info!("WAL read all: total_entries={}, cutoff={}", total_entries, cutoff);
        }
        Ok((total_entries, total_errors))
    }

    /// Read until we get an entry whose timestamp is `>= cutoff`, dropping
    /// older entries and skipping corrupted ones. Returns `None` at end of
    /// stream. Shared by `for_each_entry`'s k-way merge.
    fn next_eligible_from_shard(wal: &Walrus, key: &str, cutoff: i64, checkpoint: bool, errors: &mut usize) -> Option<WalEntry> {
        loop {
            match wal.read_next(key, checkpoint) {
                Ok(Some(d)) => match deserialize_wal_entry(&d.data) {
                    Ok(entry) if entry.timestamp_micros >= cutoff => return Some(entry),
                    Ok(_) => continue, // drop pre-cutoff
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

    /// Advance the walrus read cursor by exactly `counts[shard]` entries on
    /// each shard for `(project_id, table_name)`. Callers pass the per-shard
    /// WAL-entry counts they recorded against a successfully-flushed bucket
    /// (snapshotted at bucket-seal time) — so the cursor advances only over
    /// rows that are definitely in Delta now, never past entries belonging
    /// to a still-accumulating bucket.
    ///
    /// `counts.len()` must equal `shards_per_topic`.
    ///
    /// Replaces the older `checkpoint`, which drained each shard to its tail
    /// regardless of bucket boundaries and silently lost entries belonging
    /// to the open follow-on bucket on crash.
    #[instrument(skip(self, counts))]
    pub fn advance_by_counts(&self, project_id: &str, table_name: &str, counts: &[u64]) -> Result<(), WalError> {
        self.check_shard_len("advance_by_counts", counts.len())?;
        let topic = Self::make_topic(project_id, table_name);
        let mut total = 0u64;
        for (shard, &target) in counts.iter().enumerate() {
            if target == 0 {
                continue;
            }
            let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
            let mut consumed = 0u64;
            while consumed < target {
                match self.wal.read_next(&walrus_key, true) {
                    Ok(Some(_)) => consumed += 1,
                    Ok(None) => {
                        warn!(
                            "advance_by_counts short read: topic={}, shard={}, expected={}, got={} — cursor may be behind expected position",
                            topic, shard, target, consumed
                        );
                        break;
                    }
                    Err(e) => {
                        warn!("Error during advance_by_counts for {} shard {}: {}", topic, shard, e);
                        break;
                    }
                }
            }
            total += consumed;
        }
        if total > 0 {
            debug!("WAL advance: topic={}, consumed={}", topic, total);
        }
        Ok(())
    }

    fn for_each_shard<T>(&self, project_id: &str, table_name: &str, mut f: impl FnMut(&str) -> std::io::Result<T>) -> Result<Vec<T>, WalError> {
        (0..self.shards_per_topic)
            .map(|shard| f(&Self::walrus_topic_key(project_id, table_name, shard)).map_err(WalError::Io))
            .collect()
    }

    fn check_shard_len(&self, label: &str, len: usize) -> Result<(), WalError> {
        if len != self.shards_per_topic {
            return Err(WalError::Internal(format!(
                "{}: len={} but shards_per_topic={}",
                label, len, self.shards_per_topic
            )));
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

    fn cursor_snapshot_path(&self) -> PathBuf {
        self.data_dir.join(".timefusion_meta").join("cursor_snapshot.json")
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
    pub fn write_cursor_snapshot(&self, clean_shutdown: bool) -> Result<(), WalError> {
        let mut entries = std::collections::BTreeMap::new();
        for (project_id, table_name) in self.list_topic_pairs()? {
            let positions = match self.persisted_read_positions(&project_id, &table_name) {
                Ok(p) => p,
                Err(e) => {
                    debug!("write_cursor_snapshot: skipping {}/{}: {}", project_id, table_name, e);
                    continue;
                }
            };
            entries.insert(
                Self::make_topic(&project_id, &table_name),
                positions.into_iter().map(|p| p.map(pos_to_snap)).collect(),
            );
        }
        let snap = CursorSnapshot {
            version: SNAPSHOT_VERSION,
            written_at_micros: crate::clock::now_micros(),
            shards_per_topic: self.shards_per_topic,
            clean_shutdown,
            entries,
        };
        // `.timefusion_meta/` is created in `with_fsync_mode_and_shards`; no
        // create_dir_all needed on every flush.
        let target = self.cursor_snapshot_path();
        let tmp = target.with_extension("json.tmp");
        // Defensive: serde_json::to_vec on a struct with only primitive +
        // standard-collection fields is infallible. Keep the map_err so a
        // future field addition (custom Serialize) still surfaces clearly.
        let bytes = serde_json::to_vec(&snap).map_err(|e| WalError::Internal(format!("cursor snapshot encode: {}", e)))?;
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(&tmp, &target)?;
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

    /// Read the cursor snapshot if present. Returns None on missing/parse/version
    /// mismatch so the boot path falls through to Delta reconciliation.
    pub fn load_cursor_snapshot(&self) -> Option<CursorSnapshot> {
        let path = self.cursor_snapshot_path();
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
        if snap.shards_per_topic != self.shards_per_topic {
            warn!(
                "cursor snapshot shards_per_topic {} != current {} — ignoring (config changed)",
                snap.shards_per_topic, self.shards_per_topic
            );
            return None;
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

fn serialize_record_batch(batch: &RecordBatch) -> Result<Vec<u8>, WalError> {
    let mut buf = Vec::with_capacity(batch.get_array_memory_size() + 1024);
    {
        let mut w = StreamWriter::try_new_with_options(&mut buf, batch.schema_ref(), IpcWriteOptions::default())?;
        w.write(batch)?;
        w.finish()?;
    }
    Ok(buf)
}

fn deserialize_record_batch(data: &[u8]) -> Result<RecordBatch, WalError> {
    if data.len() > MAX_BATCH_SIZE {
        return Err(WalError::BatchTooLarge {
            size: data.len(),
            max:  MAX_BATCH_SIZE,
        });
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
        return Err(WalError::UnsupportedVersion {
            version:  data[0],
            expected: WAL_VERSION,
        });
    }
    if data.len() < 6 || data[4] != WAL_VERSION {
        return Err(WalError::UnsupportedVersion {
            version:  data[4],
            expected: WAL_VERSION,
        });
    }
    WalOperation::try_from(data[5])?;
    let (entry, _): (WalEntry, _) = bincode::decode_from_slice(&data[6..], BINCODE_CONFIG)?;
    Ok(entry)
}

pub fn deserialize_delete_payload(data: &[u8]) -> Result<DeletePayload, WalError> {
    let (payload, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
    Ok(payload)
}

pub fn deserialize_update_payload(data: &[u8]) -> Result<UpdatePayload, WalError> {
    let (payload, _) = bincode::decode_from_slice(data, BINCODE_CONFIG)?;
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int64Array, StringViewArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8View, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(StringViewArray::from(vec!["a", "b", "c"]))],
        )
        .unwrap()
    }

    #[test]
    fn test_record_batch_serialization() {
        let batch = create_test_batch();
        let serialized = serialize_record_batch(&batch).unwrap();
        let deserialized = deserialize_record_batch(&serialized).unwrap();
        assert_eq!(batch.num_rows(), deserialized.num_rows());
        assert_eq!(batch.num_columns(), deserialized.num_columns());
    }

    #[test]
    fn test_wal_entry_serialization() {
        let entry = WalEntry {
            timestamp_micros: 1234567890,
            project_id:       "project-123".to_string(),
            table_name:       "test_table".to_string(),
            operation:        WalOperation::Insert,
            data:             vec![1, 2, 3, 4, 5],
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
        let payload = DeletePayload {
            predicate_sql: Some("id = 1".to_string()),
        };
        let serialized = bincode::encode_to_vec(&payload, BINCODE_CONFIG).unwrap();
        let deserialized = deserialize_delete_payload(&serialized).unwrap();
        assert_eq!(payload.predicate_sql, deserialized.predicate_sql);

        let payload_none = DeletePayload { predicate_sql: None };
        let serialized_none = bincode::encode_to_vec(&payload_none, BINCODE_CONFIG).unwrap();
        let deserialized_none = deserialize_delete_payload(&serialized_none).unwrap();
        assert_eq!(payload_none.predicate_sql, deserialized_none.predicate_sql);
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
            assert_ne!(
                WalManager::walrus_topic_key(p1, t1, 0),
                WalManager::walrus_topic_key(p2, t2, 0),
                "({p1:?},{t1:?}) and ({p2:?},{t2:?}) collide"
            );
        }
    }

    /// Round-trip cursor snapshot: write, drop the manager, re-open, restore.
    /// Verifies the on-disk file is enough to seed walrus's known_topics on
    /// a fresh process without touching Delta — the whole point of the fast
    /// boot path.
    ///
    /// Scope note: this exercises the *idempotent* path — walrus's own fsync
    /// already persisted shard 0's advance to disk in Process A, so when
    /// Process B opens the same dir, restore finds nothing to advance and
    /// returns 0 tables. The scenario the snapshot actually *rescues* is a
    /// hard kill where walrus's in-memory cursor moved but its fsync hadn't
    /// landed — hard to simulate here without injecting into walrus's
    /// internal state, so it's covered by the prod canary in the plan
    /// instead.
    #[test]
    fn cursor_snapshot_roundtrip_restores_persisted_positions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Process A: append, advance cursor, write snapshot with clean flag.
        {
            let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
            let batch = create_test_batch();
            wal.append("proj", "tbl", &batch).unwrap();
            // Advance by 1 on the only shard we wrote to (round-robin picks
            // shard 0 first for an unseen topic).
            wal.advance_by_counts("proj", "tbl", &[1, 0, 0, 0]).unwrap();
            let before = wal.persisted_read_positions("proj", "tbl").unwrap();
            assert!(before[0].is_some_and(|p| !p.is_origin()), "advance must move shard 0 off origin");

            wal.write_cursor_snapshot(true).unwrap();
            assert!(path.join(".timefusion_meta/cursor_snapshot.json").exists());
        }

        // Process B: fresh manager, snapshot present, no walrus state mutation
        // beyond what restore does.
        {
            let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
            let snap = wal.load_cursor_snapshot().expect("snapshot loadable");
            assert!(snap.clean_shutdown);
            assert_eq!(snap.shards_per_topic, 4);
            assert!(snap.entries.contains_key("proj:tbl"));

            // Restore is idempotent — walrus state already reflects the
            // advance, so `restore` advances 0 shards but seeds known_topics.
            let advanced = wal.restore_cursor_snapshot(&snap).unwrap();
            assert_eq!(advanced, 0, "snapshot positions match walrus's own fsynced state");
            assert!(wal.list_topic_pairs().unwrap().iter().any(|(p, t)| p == "proj" && t == "tbl"));
        }
    }

    /// Snapshot version mismatch (or a corrupted file) must return None so
    /// boot falls through to the Delta scan rather than misinterpreting the
    /// payload.
    #[test]
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
    fn cursor_snapshot_rejects_shard_count_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
            wal.append("proj", "tbl", &create_test_batch()).unwrap();
            wal.advance_by_counts("proj", "tbl", &[1, 0, 0, 0]).unwrap();
            wal.write_cursor_snapshot(true).unwrap();
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
    fn cursor_snapshot_restore_advances_walrus_past_local_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path, crate::config::WalFsyncMode::SyncEach, 4).unwrap();

        // Walrus knows nothing about ("p", "t") yet → all shards None.
        assert!(wal.persisted_read_positions("p", "t").unwrap().iter().all(Option::is_none));

        let mut entries = std::collections::BTreeMap::new();
        entries.insert(WalManager::make_topic("p", "t"), vec![Some((7u64, 42u64)), None, Some((3, 0)), None]);
        let snap = CursorSnapshot {
            version: SNAPSHOT_VERSION,
            written_at_micros: 0,
            shards_per_topic: 4,
            clean_shutdown: true,
            entries,
        };
        let tables_advanced = wal.restore_cursor_snapshot(&snap).unwrap();
        assert_eq!(tables_advanced, 1, "the one snapshot table must advance from origin");

        let after = wal.persisted_read_positions("p", "t").unwrap();
        assert_eq!(after[0].map(|p| (p.block_id, p.offset)), Some((7, 42)));
        assert_eq!(after[2].map(|p| (p.block_id, p.offset)), Some((3, 0)));
    }

    /// On crash between `fs::write(tmp)` and `fs::rename(tmp, target)` we
    /// leave `cursor_snapshot.json.tmp` behind. The next WalManager init
    /// must sweep it so it doesn't accumulate over many crash-restart cycles.
    #[test]
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

    /// `delete_cursor_snapshot` removes a present file and is a no-op when
    /// absent. The flush path calls this on write failure to keep boot from
    /// restoring stale state.
    #[test]
    fn delete_cursor_snapshot_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        // Missing → Ok.
        wal.delete_cursor_snapshot().unwrap();
        wal.write_cursor_snapshot(true).unwrap();
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
    fn cursor_snapshot_dirty_path_loads_but_signals_unclean() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let wal = WalManager::with_fsync_mode_and_shards(path.clone(), crate::config::WalFsyncMode::SyncEach, 4).unwrap();
        wal.append("proj", "tbl", &create_test_batch()).unwrap();
        wal.advance_by_counts("proj", "tbl", &[1, 0, 0, 0]).unwrap();
        wal.write_cursor_snapshot(false).unwrap();

        let snap = wal.load_cursor_snapshot().expect("dirty snapshot must still be loadable");
        assert!(!snap.clean_shutdown, "dirty snapshot must not claim clean_shutdown");
        // Sanity: restore is still safe (idempotent on matching state).
        let tables_advanced = wal.restore_cursor_snapshot(&snap).unwrap();
        assert_eq!(tables_advanced, 0);
    }

    #[test]
    fn test_update_payload_serialization() {
        let payload = UpdatePayload {
            predicate_sql: Some("id = 1".to_string()),
            assignments:   vec![("name".to_string(), "'updated'".to_string())],
        };
        let serialized = bincode::encode_to_vec(&payload, BINCODE_CONFIG).unwrap();
        let deserialized = deserialize_update_payload(&serialized).unwrap();
        assert_eq!(payload.predicate_sql, deserialized.predicate_sql);
        assert_eq!(payload.assignments, deserialized.assignments);
    }
}
