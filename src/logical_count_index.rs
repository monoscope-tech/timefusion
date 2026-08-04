//! Exact logical row counts for merge-on-read tables.
//!
//! Cache tiers remove IO, but they cannot answer `COUNT(*)` without decoding
//! and resolving every physical version. This index stores only the winning
//! version of each dedup key and a timestamp histogram. It is derived data:
//! callers must bind it to a Delta snapshot fingerprint and invalidate or
//! advance it with every write before using it for a query.

use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::{Array, BooleanArray, Int64Array, LargeStringArray, StringArray, StringViewArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

const FORMAT_VERSION: &str = "1";
const META_VERSION: &str = "tf.logical_count.version";
const META_FINGERPRINT: &str = "tf.logical_count.fingerprint";
const META_FILES: &str = "tf.logical_count.files";
pub(crate) const MAX_APPEND_OVERLAY_FILES: usize = 16;
const DISK_PARTITIONS_PER_PROJECT: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Winner {
    tiebreak: Option<i64>,
    deleted: bool,
}

/// Packed immutable winner metadata. IDs live in one shared byte arena, so a
/// production partition pays no allocator/header cost per key.
#[derive(Debug, Clone, Copy)]
struct PackedWinner {
    timestamp: i64,
    tiebreak: i64,
    id_offset: u32,
    id_len: u16,
    flags: u8,
    _padding: u8,
}

const FLAG_TIEBREAK_PRESENT: u8 = 1;
const FLAG_DELETED: u8 = 2;

#[derive(Debug, Clone, Default)]
struct PackedIndex {
    winners: Vec<PackedWinner>,
    ids: Vec<u8>,
    /// One entry per live winner, sorted. Two binary searches answer any exact
    /// time window without the former per-timestamp BTree node overhead.
    live_timestamps: Vec<i64>,
}

/// Mutable build form plus a packed immutable query form. Builders use the
/// hash map for exact version resolution, then `finalize` releases it before
/// cache admission.
#[derive(Debug, Clone, Default)]
pub struct LogicalCountIndex {
    winners: HashMap<Box<[u8]>, Winner, ahash::RandomState>,
    packed: Option<PackedIndex>,
    key_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct LogicalCountColumns<'a> {
    pub timestamp: &'a str,
    pub id: &'a str,
    pub tiebreak: &'a str,
    pub deleted: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CountPartition {
    pub project_id: String,
    pub table_name: String,
    /// UTC partition date (`YYYY-MM-DD`).
    pub date: String,
}

#[derive(Debug)]
struct CachedPartition {
    fingerprint: u64,
    files: Arc<std::collections::HashSet<String>>,
    index: Arc<LogicalCountIndex>,
    estimated_bytes: usize,
    last_access: AtomicU64,
}

/// Process-local front for persistent `.arrow` logical-count partitions.
///
/// Missing, stale, corrupt, or partially-written entries are ordinary cache
/// misses. Query code must fall back to the authoritative scan in every such
/// case; this cache never weakens correctness.
#[derive(Debug)]
pub struct LogicalCountCache {
    root: PathBuf,
    entries: dashmap::DashMap<CountPartition, CachedPartition>,
    max_resident_bytes: usize,
    resident_bytes: AtomicUsize,
    access_clock: AtomicU64,
    admission_lock: parking_lot::Mutex<()>,
}

impl LogicalCountCache {
    pub fn new(root: PathBuf, max_resident_bytes: usize) -> Self {
        Self {
            root,
            entries: dashmap::DashMap::new(),
            max_resident_bytes,
            resident_bytes: AtomicUsize::new(0),
            access_clock: AtomicU64::new(1),
            admission_lock: parking_lot::Mutex::new(()),
        }
    }

    fn next_access(&self) -> u64 {
        self.access_clock.fetch_add(1, Ordering::Relaxed)
    }

    fn insert_memory(&self, key: CountPartition, fingerprint: u64, files: std::collections::HashSet<String>, index: Arc<LogicalCountIndex>) -> bool {
        let estimated_bytes = index.estimated_heap_bytes();
        if estimated_bytes > self.max_resident_bytes {
            return false;
        }
        let _guard = self.admission_lock.lock();
        if let Some((_, old)) = self.entries.remove(&key) {
            self.resident_bytes.fetch_sub(old.estimated_bytes, Ordering::Relaxed);
        }
        while self.resident_bytes.load(Ordering::Relaxed).saturating_add(estimated_bytes) > self.max_resident_bytes {
            let Some(victim) = self.entries.iter().min_by_key(|entry| entry.last_access.load(Ordering::Relaxed)).map(|entry| entry.key().clone()) else {
                break;
            };
            if let Some((_, evicted)) = self.entries.remove(&victim) {
                self.resident_bytes.fetch_sub(evicted.estimated_bytes, Ordering::Relaxed);
            }
        }
        self.entries
            .insert(key, CachedPartition { fingerprint, files: Arc::new(files), index, estimated_bytes, last_access: AtomicU64::new(self.next_access()) });
        self.resident_bytes.fetch_add(estimated_bytes, Ordering::Relaxed);
        true
    }

    fn safe_component(value: &str) -> String {
        // Encode every byte, including otherwise-safe ASCII. Replacing unsafe
        // bytes with `_` made distinct tenants such as `a/b` and `a_b` share a
        // path; if their 64-bit file-set fingerprints happened to match, one
        // tenant could consume the other's exact-count index. Hex is injective
        // over UTF-8 bytes and contains no path separators.
        use std::fmt::Write;
        let mut encoded = String::with_capacity(value.len() * 2);
        for byte in value.bytes() {
            write!(encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        encoded
    }

    fn path(&self, key: &CountPartition) -> PathBuf {
        self.root
            .join(Self::safe_component(&key.table_name))
            .join(Self::safe_component(&key.project_id))
            .join(format!("{}.arrow", Self::safe_component(&key.date)))
    }

    /// Install only after a builder has covered the complete physical
    /// partition represented by `fingerprint`.
    pub fn install(&self, key: CountPartition, fingerprint: u64, files: Vec<String>, mut index: LogicalCountIndex) -> Result<()> {
        index.finalize()?;
        let path = self.path(&key);
        index.save(&path, fingerprint, &files)?;
        Self::prune_disk_partitions(&path);
        anyhow::ensure!(
            self.insert_memory(key, fingerprint, files.into_iter().collect(), Arc::new(index)),
            "logical-count partition exceeds the resident cache budget"
        );
        Ok(())
    }

    fn prune_disk_partitions(installed: &Path) {
        let Some(parent) = installed.parent() else { return };
        let Ok(entries) = std::fs::read_dir(parent) else { return };
        let mut completed: Vec<PathBuf> =
            entries.flatten().map(|entry| entry.path()).filter(|path| path.extension().is_some_and(|extension| extension == "arrow")).collect();
        completed.sort();
        let remove = completed.len().saturating_sub(DISK_PARTITIONS_PER_PROJECT);
        for stale in completed.into_iter().take(remove) {
            let _ = std::fs::remove_file(stale);
        }
    }

    /// Return a complete exact index for this fingerprint, loading its Arrow
    /// file lazily after restart. Any validation failure is a cache miss.
    pub fn get(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        if let Some(index) = self.get_memory(key, fingerprint) {
            return Some(index);
        }
        let (loaded, files) = LogicalCountIndex::load(&self.path(key), fingerprint).ok()?;
        let loaded = Arc::new(loaded);
        self.insert_memory(key.clone(), fingerprint, files.into_iter().collect(), Arc::clone(&loaded)).then_some(loaded)
    }

    /// Background restart warm-up for an append-only successor snapshot.
    /// A removed base file refuses the load; newly added files are handled by
    /// the query's narrow append overlay.
    pub fn load_appendable(&self, key: &CountPartition, current_files: &std::collections::HashSet<String>) -> Option<usize> {
        if let Some(entry) = self.entries.get(key) {
            if entry.files.is_subset(current_files) {
                entry.last_access.store(self.next_access(), Ordering::Relaxed);
                return Some(current_files.len() - entry.files.len());
            }
            drop(entry);
            self.invalidate(key);
        }
        let (index, fingerprint, files) = LogicalCountIndex::load_file(&self.path(key)).ok()?;
        let files: std::collections::HashSet<String> = files.into_iter().collect();
        if !files.is_subset(current_files) {
            return None;
        }
        let added = current_files.len() - files.len();
        self.insert_memory(key.clone(), fingerprint, files, Arc::new(index)).then_some(added)
    }

    /// Query-path lookup that never performs filesystem IO. Disk loading and
    /// index construction belong to a bounded background builder; a cold SQL
    /// request must fall back to the authoritative scan instead of blocking a
    /// PGWire worker on a multi-million-key Arrow file.
    pub fn get_memory(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        let entry = self.entries.get(key)?;
        if entry.fingerprint != fingerprint {
            return None;
        }
        entry.last_access.store(self.next_access(), Ordering::Relaxed);
        Some(Arc::clone(&entry.index))
    }

    /// Return a snapshot whose indexed file set is an exact subset of the
    /// caller's current partition. The difference is safe to scan as a narrow
    /// append overlay. Any removal/rewrite declines because the base may then
    /// count rows no longer present.
    pub fn get_memory_appendable(
        &self, key: &CountPartition, current_files: &std::collections::HashSet<String>,
    ) -> Option<(Arc<LogicalCountIndex>, Vec<String>)> {
        let entry = self.entries.get(key)?;
        if !entry.files.is_subset(current_files) {
            return None;
        }
        entry.last_access.store(self.next_access(), Ordering::Relaxed);
        let added = current_files.difference(&entry.files).cloned().collect();
        Some((Arc::clone(&entry.index), added))
    }

    /// Remove only the memory front. The stale Arrow file remains harmless:
    /// its embedded fingerprint prevents it from being reused after a write.
    pub fn invalidate(&self, key: &CountPartition) {
        let _guard = self.admission_lock.lock();
        if let Some((_, removed)) = self.entries.remove(key) {
            self.resident_bytes.fetch_sub(removed.estimated_bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn stats(&self) -> (usize, usize, usize) {
        (self.entries.len(), self.resident_bytes.load(Ordering::Relaxed), self.max_resident_bytes)
    }
}

fn key(timestamp: i64, id: &str) -> Box<[u8]> {
    let mut out = Vec::with_capacity(8 + id.len());
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(id.as_bytes());
    out.into_boxed_slice()
}

fn packed_id<'a>(ids: &'a [u8], winner: &PackedWinner) -> &'a [u8] {
    let start = winner.id_offset as usize;
    let end = start + usize::from(winner.id_len);
    &ids[start..end]
}

impl LogicalCountIndex {
    pub fn new() -> Self {
        Self { winners: HashMap::default(), packed: None, key_bytes: 0 }
    }

    /// Apply one physical version. Returns whether it changed the logical row.
    ///
    /// Ordering exactly matches `DedupExec`'s primitive keep-greatest rule:
    /// `None` sorts below every non-null value, and an equal tiebreak does not
    /// replace the existing winner.
    pub fn apply(&mut self, timestamp: i64, id: &str, tiebreak: Option<i64>, deleted: bool) -> bool {
        assert!(self.packed.is_none(), "cannot mutate a finalized logical-count index");
        let encoded = key(timestamp, id);
        let old = self.winners.get(encoded.as_ref()).copied();
        if old.is_some_and(|winner| tiebreak <= winner.tiebreak) {
            return false;
        }

        if old.is_none() {
            self.key_bytes = self.key_bytes.saturating_add(encoded.len());
        }
        self.winners.insert(encoded, Winner { tiebreak, deleted });
        true
    }

    /// Convert the allocation-heavy builder map into the resident query form.
    pub fn finalize(&mut self) -> Result<()> {
        if self.packed.is_some() {
            return Ok(());
        }
        let winners = std::mem::take(&mut self.winners);
        let mut packed = PackedIndex {
            winners: Vec::with_capacity(winners.len()),
            ids: Vec::with_capacity(self.key_bytes.saturating_sub(winners.len().saturating_mul(8))),
            live_timestamps: Vec::with_capacity(winners.len()),
        };
        for (key, winner) in winners {
            let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
            let id = &key[8..];
            let id_offset = u32::try_from(packed.ids.len()).context("logical-count ID arena exceeds 4GiB")?;
            let id_len = u16::try_from(id.len()).context("logical-count ID exceeds 65535 bytes")?;
            packed.ids.extend_from_slice(id);
            let mut flags = 0;
            if winner.tiebreak.is_some() {
                flags |= FLAG_TIEBREAK_PRESENT;
            }
            if winner.deleted {
                flags |= FLAG_DELETED;
            } else {
                packed.live_timestamps.push(timestamp);
            }
            packed.winners.push(PackedWinner { timestamp, tiebreak: winner.tiebreak.unwrap_or_default(), id_offset, id_len, flags, _padding: 0 });
        }
        let ids = &packed.ids;
        packed.winners.sort_unstable_by(|left, right| left.timestamp.cmp(&right.timestamp).then_with(|| packed_id(ids, left).cmp(packed_id(ids, right))));
        packed.live_timestamps.sort_unstable();
        self.key_bytes = packed.ids.len();
        self.packed = Some(packed);
        Ok(())
    }

    fn winner(&self, timestamp: i64, id: &str) -> Option<Winner> {
        if let Some(packed) = &self.packed {
            let pos = packed
                .winners
                .binary_search_by(|candidate| candidate.timestamp.cmp(&timestamp).then_with(|| packed_id(&packed.ids, candidate).cmp(id.as_bytes())))
                .ok()?;
            let winner = packed.winners[pos];
            Some(Winner { tiebreak: (winner.flags & FLAG_TIEBREAK_PRESENT != 0).then_some(winner.tiebreak), deleted: winner.flags & FLAG_DELETED != 0 })
        } else {
            self.winners.get(key(timestamp, id).as_ref()).copied()
        }
    }

    /// Apply the four-column narrow form emitted by a count-index build:
    /// `timestamp`, `id`, version tiebreak, tombstone marker.
    pub fn apply_batch(&mut self, batch: &RecordBatch, columns: LogicalCountColumns<'_>) -> Result<usize> {
        let timestamps = timestamp_values(batch, columns.timestamp)?;
        let ids = StringValues::new(batch, columns.id)?;
        let tiebreaks = timestamp_values(batch, columns.tiebreak)?;
        let deleted = batch
            .column_by_name(columns.deleted)
            .with_context(|| format!("logical-count batch missing {}", columns.deleted))?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .with_context(|| format!("logical-count {} is not Boolean", columns.deleted))?;
        let mut changed = 0;
        for row in 0..batch.num_rows() {
            // A bounded timestamp predicate never matches NULL, so such a row
            // contributes to no count index. ID is a declared dedup key and
            // must not be silently discarded if corrupt input violates it.
            if timestamps.is_null(row) {
                continue;
            }
            let id = ids.value(row).with_context(|| format!("logical-count {} is NULL at row {row}", columns.id))?;
            let tiebreak = (!tiebreaks.is_null(row)).then(|| tiebreaks.value(row));
            let is_deleted = !deleted.is_null(row) && deleted.value(row);
            changed += usize::from(self.apply(timestamps.value(row), id, tiebreak, is_deleted));
        }
        Ok(changed)
    }

    /// Exact base count with unflushed MemBuffer versions overlaid. The base
    /// index is never cloned or mutated: only keys present in `batches` occupy
    /// the temporary map, so the cost follows the hot tail rather than the
    /// full 24-hour cardinality.
    pub fn count_with_overlay(&self, batches: &[RecordBatch], lo: i64, hi: i64, columns: LogicalCountColumns<'_>) -> Result<u64> {
        #[derive(Clone, Copy)]
        struct Overlay {
            base: Option<Winner>,
            current: Winner,
            timestamp: i64,
        }

        let mut overlay: HashMap<Box<[u8]>, Overlay, ahash::RandomState> = HashMap::default();
        for batch in batches {
            let timestamps = timestamp_values(batch, columns.timestamp)?;
            let ids = StringValues::new(batch, columns.id)?;
            let tiebreaks = timestamp_values(batch, columns.tiebreak)?;
            let deleted = batch
                .column_by_name(columns.deleted)
                .with_context(|| format!("logical-count overlay missing {}", columns.deleted))?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .with_context(|| format!("logical-count overlay {} is not Boolean", columns.deleted))?;
            for row in 0..batch.num_rows() {
                if timestamps.is_null(row) {
                    continue;
                }
                let timestamp = timestamps.value(row);
                let id = ids.value(row).with_context(|| format!("logical-count overlay {} is NULL at row {row}", columns.id))?;
                let encoded = key(timestamp, id);
                let candidate =
                    Winner { tiebreak: (!tiebreaks.is_null(row)).then(|| tiebreaks.value(row)), deleted: !deleted.is_null(row) && deleted.value(row) };
                match overlay.entry(encoded) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if candidate.tiebreak > entry.get().current.tiebreak {
                            entry.get_mut().current = candidate;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let base = self.winner(timestamp, id);
                        let current = base.filter(|winner| winner.tiebreak >= candidate.tiebreak).unwrap_or(candidate);
                        entry.insert(Overlay { base, current, timestamp });
                    }
                }
            }
        }

        let mut count = i128::from(self.count(lo, hi));
        for state in overlay.values().filter(|state| (lo..hi).contains(&state.timestamp)) {
            count += i128::from(!state.current.deleted) - i128::from(state.base.is_some_and(|winner| !winner.deleted));
        }
        u64::try_from(count).context("logical-count overlay produced an invalid negative/overflow count")
    }

    /// Exact live row count in the half-open interval `[lo, hi)`.
    pub fn count(&self, lo: i64, hi: i64) -> u64 {
        if lo >= hi {
            return 0;
        }
        if let Some(packed) = &self.packed {
            let start = packed.live_timestamps.partition_point(|timestamp| *timestamp < lo);
            let end = packed.live_timestamps.partition_point(|timestamp| *timestamp < hi);
            return u64::try_from(end - start).expect("logical-count partition length fits u64");
        }
        u64::try_from(
            self.winners
                .iter()
                .filter(|(key, winner)| {
                    let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
                    !winner.deleted && (lo..hi).contains(&timestamp)
                })
                .count(),
        )
        .expect("logical-count partition length fits u64")
    }

    pub fn logical_rows(&self) -> u64 {
        if let Some(packed) = &self.packed {
            u64::try_from(packed.live_timestamps.len()).expect("logical-count partition length fits u64")
        } else {
            u64::try_from(self.winners.values().filter(|winner| !winner.deleted).count()).expect("logical-count partition length fits u64")
        }
    }

    pub fn physical_keys(&self) -> usize {
        self.packed.as_ref().map_or(self.winners.len(), |packed| packed.winners.len())
    }

    /// Conservative resident-size estimate for build admission. Includes the
    /// key bytes plus allocation/hash-table overhead; it intentionally rounds
    /// up because this map lives outside DataFusion's tracked memory pool.
    pub fn estimated_heap_bytes(&self) -> usize {
        if let Some(packed) = &self.packed {
            return packed
                .winners
                .capacity()
                .saturating_mul(std::mem::size_of::<PackedWinner>())
                .saturating_add(packed.ids.capacity())
                .saturating_add(packed.live_timestamps.capacity().saturating_mul(std::mem::size_of::<i64>()));
        }
        self.key_bytes.saturating_add(self.winners.len().saturating_mul(64))
    }

    /// Atomically persist the derived winners as Arrow IPC.
    ///
    /// The compact timestamp histogram is rebuilt on load; persisting one
    /// canonical winner table avoids two sources of truth. A fingerprint is
    /// embedded in schema metadata and must match the caller's current Delta
    /// snapshot before the file can be served.
    pub fn save(&self, path: &Path, fingerprint: u64, files: &[String]) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create logical-count cache directory {}", parent.display()))?;
        }
        let mut metadata = HashMap::new();
        metadata.insert(META_VERSION.to_string(), FORMAT_VERSION.to_string());
        metadata.insert(META_FINGERPRINT.to_string(), fingerprint.to_string());
        metadata.insert(META_FILES.to_string(), serde_json::to_string(files).context("serialize logical-count file set")?);
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("id", DataType::Utf8, false),
                Field::new("tiebreak", DataType::Int64, true),
                Field::new("deleted", DataType::Boolean, false),
            ],
            metadata,
        ));
        let tmp = path.with_extension(format!("arrow.tmp-{}", uuid::Uuid::new_v4()));
        let write = || -> Result<()> {
            let file = File::create(&tmp).with_context(|| format!("create logical-count cache {}", tmp.display()))?;
            let mut writer = FileWriter::try_new(file, schema.as_ref())?;
            // A production day can contain tens of millions of keys. Building
            // and sorting one second full-sized row vector here doubled the
            // index's peak memory during warm-up. IPC order is irrelevant to
            // correctness, so stream bounded batches directly from the map.
            const WRITE_ROWS: usize = 64 * 1024;
            let mut rows = Vec::with_capacity(WRITE_ROWS);
            let mut write_rows = |rows: &mut Vec<(i64, &str, Winner)>| -> Result<()> {
                if rows.is_empty() {
                    return Ok(());
                }
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                        Arc::new(Int64Array::from(rows.iter().map(|row| row.2.tiebreak).collect::<Vec<_>>())),
                        Arc::new(BooleanArray::from(rows.iter().map(|row| row.2.deleted).collect::<Vec<_>>())),
                    ],
                )?;
                writer.write(&batch)?;
                rows.clear();
                Ok(())
            };
            if let Some(packed) = &self.packed {
                for winner in &packed.winners {
                    let id = std::str::from_utf8(packed_id(&packed.ids, winner)).context("logical-count key contains non-UTF8 id")?;
                    rows.push((
                        winner.timestamp,
                        id,
                        Winner { tiebreak: (winner.flags & FLAG_TIEBREAK_PRESENT != 0).then_some(winner.tiebreak), deleted: winner.flags & FLAG_DELETED != 0 },
                    ));
                    if rows.len() == WRITE_ROWS {
                        write_rows(&mut rows)?;
                    }
                }
            } else {
                for (key, winner) in &self.winners {
                    let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
                    let id = std::str::from_utf8(&key[8..]).context("logical-count key contains non-UTF8 id")?;
                    rows.push((timestamp, id, *winner));
                    if rows.len() == WRITE_ROWS {
                        write_rows(&mut rows)?;
                    }
                }
            }
            write_rows(&mut rows)?;
            writer.finish()?;
            std::fs::rename(&tmp, path).with_context(|| format!("publish logical-count cache {}", path.display()))?;
            Ok(())
        };
        if let Err(error) = write() {
            let _ = std::fs::remove_file(&tmp);
            return Err(error);
        }
        Ok(())
    }

    /// Load only when the file belongs to the caller's exact snapshot.
    pub fn load(path: &Path, expected_fingerprint: u64) -> Result<(Self, Vec<String>)> {
        let (index, fingerprint, files) = Self::load_file(path)?;
        if fingerprint != expected_fingerprint {
            bail!("logical-count cache fingerprint mismatch: cached={fingerprint} current={expected_fingerprint}");
        }
        Ok((index, files))
    }

    fn load_file(path: &Path) -> Result<(Self, u64, Vec<String>)> {
        let file = File::open(path).with_context(|| format!("open logical-count cache {}", path.display()))?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();
        if schema.metadata().get(META_VERSION).map(String::as_str) != Some(FORMAT_VERSION) {
            bail!("unsupported logical-count cache format");
        }
        let expected_fields = [
            ("timestamp", &DataType::Int64, false),
            ("id", &DataType::Utf8, false),
            ("tiebreak", &DataType::Int64, true),
            ("deleted", &DataType::Boolean, false),
        ];
        if schema.fields().len() != expected_fields.len()
            || schema
                .fields()
                .iter()
                .zip(expected_fields)
                .any(|(field, (name, data_type, nullable))| field.name() != name || field.data_type() != data_type || field.is_nullable() != nullable)
        {
            bail!("logical-count cache has an incompatible Arrow schema");
        }
        let fingerprint = schema
            .metadata()
            .get(META_FINGERPRINT)
            .context("logical-count cache missing fingerprint")?
            .parse::<u64>()
            .context("logical-count cache fingerprint is invalid")?;
        let files: Vec<String> = serde_json::from_str(schema.metadata().get(META_FILES).context("logical-count cache missing file set")?)
            .context("logical-count cache file set is invalid")?;

        let mut index = Self::new();
        for batch in reader {
            let batch = batch?;
            let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().context("logical-count timestamp column has wrong type")?;
            let ids = batch.column(1).as_any().downcast_ref::<StringArray>().context("logical-count id column has wrong type")?;
            let tiebreaks = batch.column(2).as_any().downcast_ref::<Int64Array>().context("logical-count tiebreak column has wrong type")?;
            let deleted = batch.column(3).as_any().downcast_ref::<BooleanArray>().context("logical-count deleted column has wrong type")?;
            if timestamps.null_count() != 0 || ids.null_count() != 0 || deleted.null_count() != 0 {
                bail!("logical-count cache contains NULL in a required column");
            }
            for row in 0..batch.num_rows() {
                let tiebreak = tiebreaks.is_valid(row).then(|| tiebreaks.value(row));
                index.apply(timestamps.value(row), ids.value(row), tiebreak, deleted.value(row));
            }
        }
        index.finalize()?;
        Ok((index, fingerprint, files))
    }
}

fn timestamp_values<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a TimestampMicrosecondArray> {
    let column = batch.column_by_name(name).with_context(|| format!("logical-count batch missing {name}"))?;
    match column.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            column.as_any().downcast_ref::<TimestampMicrosecondArray>().with_context(|| format!("logical-count {name} is not TimestampMicrosecond"))
        }
        other => bail!("logical-count {name} has unsupported type {other}"),
    }
}

enum StringValues<'a> {
    View(&'a StringViewArray),
    Utf8(&'a StringArray),
    Large(&'a LargeStringArray),
}

impl<'a> StringValues<'a> {
    fn new(batch: &'a RecordBatch, name: &str) -> Result<Self> {
        let column = batch.column_by_name(name).with_context(|| format!("logical-count batch missing {name}"))?;
        if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
            Ok(Self::View(values))
        } else if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            Ok(Self::Utf8(values))
        } else if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            Ok(Self::Large(values))
        } else {
            bail!("logical-count {name} has unsupported type {}", column.data_type())
        }
    }

    fn value(&self, row: usize) -> Option<&'a str> {
        match self {
            Self::View(values) => (!values.is_null(row)).then(|| values.value(row)),
            Self::Utf8(values) => (!values.is_null(row)).then(|| values.value(row)),
            Self::Large(values) => (!values.is_null(row)).then(|| values.value(row)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn versions(rows: &[(i64, &str, Option<i64>, Option<bool>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("id", DataType::Utf8View, true),
            Field::new("updated_at", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("deleted", DataType::Boolean, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|row| Some(row.0)).collect::<Vec<_>>()).with_timezone("UTC")),
                Arc::new(StringViewArray::from(rows.iter().map(|row| Some(row.1)).collect::<Vec<_>>())),
                Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|row| row.2).collect::<Vec<_>>()).with_timezone("UTC")),
                Arc::new(BooleanArray::from(rows.iter().map(|row| row.3).collect::<Vec<_>>())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn resolves_duplicates_updates_and_tombstones_exactly() {
        let mut index = LogicalCountIndex::new();
        assert!(index.apply(10, "a", None, false));
        assert!(!index.apply(10, "a", None, false), "equal version is a duplicate");
        assert!(index.apply(10, "a", Some(2), false), "newer update remains one live row");
        assert_eq!(index.logical_rows(), 1);

        assert!(index.apply(10, "a", Some(3), true));
        assert_eq!(index.logical_rows(), 0);
        assert!(!index.apply(10, "a", Some(2), false), "stale update cannot resurrect a tombstone");
        assert!(index.apply(10, "a", Some(4), false));
        assert_eq!(index.logical_rows(), 1);
        assert_eq!(index.physical_keys(), 1);
    }

    #[test]
    fn arbitrary_ranges_use_exact_boundary_timestamps() {
        let mut index = LogicalCountIndex::new();
        for (timestamp, id) in [(-1, "neg"), (0, "zero"), (59_999_999, "left"), (60_000_000, "right"), (120_000_000, "end")] {
            index.apply(timestamp, id, Some(1), false);
        }
        assert_eq!(index.count(0, 120_000_000), 3);
        assert_eq!(index.count(1, 60_000_000), 1);
        assert_eq!(index.count(-1, 1), 2);
        assert_eq!(index.count(120_000_000, 120_000_001), 1);
        assert_eq!(index.count(5, 5), 0);
    }

    #[test]
    fn multiple_ids_at_one_timestamp_track_delete_transitions() {
        let mut index = LogicalCountIndex::new();
        index.apply(42, "a", Some(1), false);
        index.apply(42, "b", Some(1), false);
        index.apply(42, "c", Some(1), true);
        assert_eq!(index.count(42, 43), 2);
        index.apply(42, "a", Some(2), true);
        assert_eq!(index.count(42, 43), 1);
    }

    #[test]
    fn randomized_versions_and_ranges_match_a_reference_model() {
        let mut index = LogicalCountIndex::new();
        let mut reference: HashMap<(i64, String), Winner> = HashMap::new();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for _ in 0..20_000 {
            state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            let timestamp = i64::try_from(state % 300).unwrap() * 1_000_000 - 100_000_000;
            let id = format!("id-{}", (state >> 12) % 200);
            let tiebreak = (!(state >> 24).is_multiple_of(50)).then(|| i64::try_from((state >> 32) % 1_000).unwrap());
            let deleted = state & 7 == 0;
            index.apply(timestamp, &id, tiebreak, deleted);
            let winner = reference.entry((timestamp, id)).or_insert(Winner { tiebreak, deleted });
            if tiebreak > winner.tiebreak {
                *winner = Winner { tiebreak, deleted };
            }
        }

        for n in 0..200i64 {
            let lo = -120_000_000 + n * 1_700_000;
            let hi = lo + 37_000_001;
            let expected = reference.iter().filter(|((timestamp, _), winner)| (lo..hi).contains(timestamp) && !winner.deleted).count() as u64;
            assert_eq!(index.count(lo, hi), expected, "range [{lo}, {hi})");
        }
    }

    #[test]
    fn packed_form_preserves_exact_ranges_and_bounds_resident_bytes() {
        let mut index = LogicalCountIndex::new();
        for value in 0..100_000i64 {
            let id = format!("01234567-89ab-cdef-0123-{value:012}");
            index.apply(value, &id, Some(value), value % 11 == 0);
        }
        index.finalize().unwrap();

        assert_eq!(index.physical_keys(), 100_000);
        assert_eq!(index.logical_rows(), 90_909);
        assert_eq!(index.count(25_000, 75_000), 45_454);
        assert!(index.winners.is_empty(), "the allocation-heavy build map must be released");
        assert!(index.estimated_heap_bytes() < 7_000_000, "packed 36-byte IDs should stay below 70 bytes/key");
    }

    #[test]
    fn narrow_batches_build_and_overlay_unflushed_versions_exactly() {
        let columns = LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak: "updated_at", deleted: "deleted" };
        let mut index = LogicalCountIndex::new();
        index.apply_batch(&versions(&[(10, "a", Some(1), Some(false)), (20, "b", Some(1), None), (30, "gone", Some(2), Some(true))]), columns).unwrap();
        assert_eq!(index.count(0, 100), 2);

        // a is tombstoned, b gets a stale no-op, gone is resurrected, and c
        // is a new unflushed key. A repeated Delta+Mem copy with an equal
        // tiebreak remains one logical row.
        let tail = versions(&[
            (10, "a", Some(3), Some(true)),
            (20, "b", Some(0), Some(true)),
            (20, "b", Some(1), Some(false)),
            (30, "gone", Some(4), Some(false)),
            (40, "c", Some(1), None),
        ]);
        assert_eq!(index.count_with_overlay(&[tail], 0, 100, columns).unwrap(), 3);
        assert_eq!(index.logical_rows(), 2, "overlay must not mutate the persistent base");
    }

    #[test]
    fn arrow_cache_round_trip_is_exact_and_snapshot_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("project/date.arrow");
        let mut index = LogicalCountIndex::new();
        index.apply(10, "a", None, false);
        index.apply(10, "a", Some(2), true);
        index.apply(60_000_001, "b", Some(3), false);
        let files = vec!["date=2026-08-04/a.parquet".to_string()];
        index.save(&path, 99, &files).unwrap();

        let (loaded, loaded_files) = LogicalCountIndex::load(&path, 99).unwrap();
        assert_eq!(loaded_files, files);
        assert_eq!(loaded.physical_keys(), 2);
        assert_eq!(loaded.logical_rows(), 1);
        assert_eq!(loaded.count(60_000_000, 60_000_002), 1);
        assert!(LogicalCountIndex::load(&path, 100).unwrap_err().to_string().contains("fingerprint mismatch"));
        assert!(!path.with_extension("arrow.tmp").exists());
    }

    #[test]
    fn arrow_cache_streams_more_than_one_write_batch() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.arrow");
        let mut index = LogicalCountIndex::new();
        for value in 0..70_000 {
            index.apply(value, &value.to_string(), Some(value), false);
        }
        index.save(&path, 1, &["large.parquet".into()]).unwrap();
        let (loaded, _) = LogicalCountIndex::load(&path, 1).unwrap();
        assert_eq!(loaded.physical_keys(), 70_000);
        assert_eq!(loaded.count(0, 70_000), 70_000);
    }

    #[test]
    fn cache_lazily_loads_only_matching_partition_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let key = CountPartition { project_id: "p/unsafe".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        let mut index = LogicalCountIndex::new();
        index.apply(42, "id", Some(1), false);
        cache.install(key.clone(), 7, vec!["a.parquet".into()], index).unwrap();
        assert_eq!(cache.get(&key, 7).unwrap().logical_rows(), 1);
        assert!(cache.get(&key, 8).is_none());

        let restarted = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        assert_eq!(restarted.get(&key, 7).unwrap().count(0, 100), 1);
        let current = ["a.parquet".to_string(), "b.parquet".to_string()].into_iter().collect();
        let (_, added) = restarted.get_memory_appendable(&key, &current).unwrap();
        assert_eq!(added, vec!["b.parquet"]);
        let append_restart = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        assert_eq!(append_restart.load_appendable(&key, &current), Some(1));
        assert_eq!(append_restart.get_memory_appendable(&key, &current).unwrap().1, vec!["b.parquet"]);
        let far_ahead: std::collections::HashSet<_> =
            std::iter::once("a.parquet".to_string()).chain((0..=MAX_APPEND_OVERLAY_FILES).map(|i| format!("new-{i}.parquet"))).collect();
        assert_eq!(append_restart.load_appendable(&key, &far_ahead), Some(MAX_APPEND_OVERLAY_FILES + 1));
        let rewritten = ["replacement.parquet".to_string()].into_iter().collect();
        assert!(restarted.get_memory_appendable(&key, &rewritten).is_none(), "a removed base file must fail closed");
        restarted.invalidate(&key);
        assert!(restarted.get(&key, 8).is_none());
        assert!(dir.path().join("6f74656c/702f756e73616665/323032362d30382d3034.arrow").exists());
    }

    #[test]
    fn cache_paths_cannot_alias_distinct_partition_names() {
        let cache = LogicalCountCache::new(PathBuf::from("unused"), usize::MAX);
        let slash = CountPartition { project_id: "a/b".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let underscore = CountPartition { project_id: "a_b".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        assert_ne!(cache.path(&slash), cache.path(&underscore));
    }

    #[test]
    fn resident_cache_evicts_the_least_recent_partition_within_budget() {
        let dir = tempfile::tempdir().unwrap();
        let key = |project: &str| CountPartition { project_id: project.into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let mut first = LogicalCountIndex::new();
        first.apply(1, "a", Some(1), false);
        first.finalize().unwrap();
        let per_entry = first.estimated_heap_bytes();
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), per_entry);
        cache.install(key("a"), 1, vec!["a.parquet".into()], first).unwrap();
        assert!(cache.get_memory(&key("a"), 1).is_some());

        let mut second = LogicalCountIndex::new();
        second.apply(2, "b", Some(1), false);
        cache.install(key("b"), 2, vec!["b.parquet".into()], second).unwrap();
        assert!(cache.get_memory(&key("a"), 1).is_none());
        assert!(cache.get_memory(&key("b"), 2).is_some());
        assert!(cache.resident_bytes.load(Ordering::Relaxed) <= per_entry);
    }

    #[test]
    fn disk_cache_keeps_only_the_newest_completed_daily_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let cache = LogicalCountCache::new(dir.path().to_path_buf(), usize::MAX);
        for day in 1..=DISK_PARTITIONS_PER_PROJECT + 3 {
            let key = CountPartition { project_id: "p".into(), table_name: "otel".into(), date: format!("2026-08-{day:02}") };
            cache.install(key, u64::try_from(day).unwrap(), Vec::new(), LogicalCountIndex::new()).unwrap();
        }
        let project_dir = dir.path().join(LogicalCountCache::safe_component("otel")).join(LogicalCountCache::safe_component("p"));
        let files: Vec<_> = std::fs::read_dir(project_dir).unwrap().flatten().map(|entry| entry.path()).collect();
        assert_eq!(files.len(), DISK_PARTITIONS_PER_PROJECT);
        assert!(files.iter().all(|path| path.extension().is_some_and(|extension| extension == "arrow")));
    }
}
