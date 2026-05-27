use std::path::PathBuf;

use arrow::array::RecordBatch;
use arrow_ipc::{
    reader::StreamReader,
    writer::{IpcWriteOptions, StreamWriter},
};
use bincode::{Decode, Encode};
use dashmap::DashSet;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};
use walrus_rust::{FsyncSchedule, ReadConsistency, Walrus};

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
}

/// Magic bytes to identify the WAL format ("WAL2").
const WAL_MAGIC: [u8; 4] = [0x57, 0x41, 0x4C, 0x32];
/// Insert batches are stored as Arrow IPC stream bytes. Embeds the schema so
/// the reader doesn't need a separate registry lookup, and round-trips every
/// Arrow type (List/Struct/Variant/…) without the per-buffer bincode shuffle
/// the older CompactBatch format required.
///
/// Version byte must be > 2 to distinguish from legacy operation bytes
/// (0=Insert, 1=Delete, 2=Update). We're at 131; older formats are intentionally
/// unsupported — wipe the WAL directory if upgrading.
///
/// Bumps:
///   130: Arrow IPC payload format.
///   131: Walrus collection key uses deterministic FNV-1a instead of AHasher
///        (AHasher's per-build seed silently stranded entries on upgrade).
const WAL_VERSION: u8 = 131;
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();
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
        use std::hash::{Hash, Hasher};

        use fnv::FnvHasher;
        let mut hasher = FnvHasher::default();
        project_id.hash(&mut hasher);
        ":".hash(&mut hasher); // separator so ("ab","c") and ("a","bc") don't collide
        table_name.hash(&mut hasher);
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

    #[instrument(skip(self, batch), fields(project_id, table_name, rows))]
    pub fn append(&self, project_id: &str, table_name: &str, batch: &RecordBatch) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let shard = self.pick_shard(&topic);
        let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
        let entry = WalEntry::new(project_id, table_name, WalOperation::Insert, serialize_record_batch(batch)?);
        self.wal.append_for_topic(&walrus_key, &serialize_wal_entry(&entry)?)?;
        self.persist_topic(&topic);
        debug!("WAL append INSERT: topic={}, shard={}, rows={}", topic, shard, batch.num_rows());
        Ok(())
    }

    #[instrument(skip(self, batches), fields(project_id, table_name, batch_count))]
    pub fn append_batch(&self, project_id: &str, table_name: &str, batches: &[RecordBatch]) -> Result<(), WalError> {
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
        Ok(())
    }

    #[instrument(skip(self), fields(project_id, table_name))]
    pub fn append_delete(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>) -> Result<(), WalError> {
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
        Ok(())
    }

    #[instrument(skip(self, assignments), fields(project_id, table_name))]
    pub fn append_update(&self, project_id: &str, table_name: &str, predicate_sql: Option<&str>, assignments: &[(String, String)]) -> Result<(), WalError> {
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
        Ok(())
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

    #[instrument(skip(self))]
    pub fn checkpoint(&self, project_id: &str, table_name: &str) -> Result<(), WalError> {
        let topic = Self::make_topic(project_id, table_name);
        let mut count = 0;
        for shard in 0..self.shards_per_topic {
            let walrus_key = Self::walrus_topic_key(project_id, table_name, shard);
            loop {
                match self.wal.read_next(&walrus_key, true) {
                    Ok(Some(_)) => count += 1,
                    Ok(None) => break,
                    Err(e) => {
                        warn!("Error during checkpoint for {} shard {}: {}", topic, shard, e);
                        break;
                    }
                }
            }
        }
        if count > 0 {
            debug!("WAL checkpoint: topic={}, consumed={}", topic, count);
        }
        Ok(())
    }

    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
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
    /// entries on upgrade — see WAL_VERSION 131 bump rationale.
    #[test]
    fn walrus_topic_key_is_stable() {
        assert_eq!(WalManager::walrus_topic_key("project", "table", 0), "40df847bedad365d-00");
        assert_eq!(WalManager::walrus_topic_key("p1", "otel_logs_and_spans", 3), "39ffdd9cbe44176d-03");
        // Separator guard: ("ab","c") and ("a","bc") must produce different keys.
        assert_ne!(WalManager::walrus_topic_key("ab", "c", 0), WalManager::walrus_topic_key("a", "bc", 0));
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
