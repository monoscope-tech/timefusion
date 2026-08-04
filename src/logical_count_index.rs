//! Exact logical row counts for merge-on-read tables.
//!
//! Cache tiers remove IO, but they cannot answer `COUNT(*)` without decoding
//! and resolving every physical version. This index stores only the winning
//! version of each dedup key and a timestamp histogram. It is derived data:
//! callers must bind it to a Delta snapshot fingerprint and invalidate or
//! advance it with every write before using it for a query.

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use arrow::{
    array::{Array, BooleanArray, Int64Array, LargeStringArray, StringArray, StringViewArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use arrow_ipc::{reader::FileReader, writer::FileWriter};

const MINUTE_MICROS: i64 = 60 * 1_000_000;
const FORMAT_VERSION: &str = "1";
const META_VERSION: &str = "tf.logical_count.version";
const META_FINGERPRINT: &str = "tf.logical_count.fingerprint";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Winner {
    tiebreak: Option<i64>,
    deleted: bool,
}

/// Mutable build/overlay form of an exact count index.
///
/// `winners` is required for version resolution. `minutes` is the compact
/// query structure: full minutes are answered from one scalar, while at most
/// two boundary minutes consult their exact timestamp histogram.
#[derive(Debug, Clone, Default)]
pub struct LogicalCountIndex {
    winners: HashMap<Box<[u8]>, Winner, ahash::RandomState>,
    minutes: BTreeMap<i64, MinuteCounts>,
}

#[derive(Debug, Clone, Default)]
struct MinuteCounts {
    live: u64,
    by_timestamp: BTreeMap<i64, u64>,
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
    index: Arc<LogicalCountIndex>,
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
}

impl LogicalCountCache {
    pub fn new(root: PathBuf) -> Self {
        Self { root, entries: dashmap::DashMap::new() }
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
    pub fn install(&self, key: CountPartition, fingerprint: u64, index: LogicalCountIndex) -> Result<()> {
        index.save(&self.path(&key), fingerprint)?;
        self.entries.insert(key, CachedPartition { fingerprint, index: Arc::new(index) });
        Ok(())
    }

    /// Return a complete exact index for this fingerprint, loading its Arrow
    /// file lazily after restart. Any validation failure is a cache miss.
    pub fn get(&self, key: &CountPartition, fingerprint: u64) -> Option<Arc<LogicalCountIndex>> {
        if let Some(entry) = self.entries.get(key) {
            return (entry.fingerprint == fingerprint).then(|| Arc::clone(&entry.index));
        }
        let loaded = LogicalCountIndex::load(&self.path(key), fingerprint).ok()?;
        let loaded = Arc::new(loaded);
        self.entries.insert(key.clone(), CachedPartition { fingerprint, index: Arc::clone(&loaded) });
        Some(loaded)
    }

    /// Remove only the memory front. The stale Arrow file remains harmless:
    /// its embedded fingerprint prevents it from being reused after a write.
    pub fn invalidate(&self, key: &CountPartition) {
        self.entries.remove(key);
    }
}

fn key(timestamp: i64, id: &str) -> Box<[u8]> {
    let mut out = Vec::with_capacity(8 + id.len());
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(id.as_bytes());
    out.into_boxed_slice()
}

fn minute(timestamp: i64) -> i64 {
    timestamp.div_euclid(MINUTE_MICROS)
}

impl LogicalCountIndex {
    pub fn new() -> Self {
        Self { winners: HashMap::default(), minutes: BTreeMap::new() }
    }

    /// Apply one physical version. Returns whether it changed the logical row.
    ///
    /// Ordering exactly matches `DedupExec`'s primitive keep-greatest rule:
    /// `None` sorts below every non-null value, and an equal tiebreak does not
    /// replace the existing winner.
    pub fn apply(&mut self, timestamp: i64, id: &str, tiebreak: Option<i64>, deleted: bool) -> bool {
        let encoded = key(timestamp, id);
        let old = self.winners.get(encoded.as_ref()).copied();
        if old.is_some_and(|winner| tiebreak <= winner.tiebreak) {
            return false;
        }

        let old_live = old.is_some_and(|winner| !winner.deleted);
        let new_live = !deleted;
        self.winners.insert(encoded, Winner { tiebreak, deleted });
        match (old_live, new_live) {
            (false, true) => self.adjust(timestamp, 1),
            (true, false) => self.adjust(timestamp, -1),
            _ => {}
        }
        true
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
                        let base = self.winners.get(entry.key().as_ref()).copied();
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

    fn adjust(&mut self, timestamp: i64, delta: i8) {
        let minute = self.minutes.entry(minute(timestamp)).or_default();
        if delta > 0 {
            minute.live += 1;
            *minute.by_timestamp.entry(timestamp).or_default() += 1;
        } else {
            minute.live -= 1;
            let remove = {
                let count = minute.by_timestamp.get_mut(&timestamp).expect("live winner must have a timestamp count");
                *count -= 1;
                *count == 0
            };
            if remove {
                minute.by_timestamp.remove(&timestamp);
            }
        }
    }

    /// Exact live row count in the half-open interval `[lo, hi)`.
    pub fn count(&self, lo: i64, hi: i64) -> u64 {
        if lo >= hi {
            return 0;
        }
        let lo_minute = minute(lo);
        let hi_minute = minute(hi - 1);
        self.minutes
            .range(lo_minute..=hi_minute)
            .map(|(bucket, counts)| {
                let start = bucket.saturating_mul(MINUTE_MICROS);
                let end = start.saturating_add(MINUTE_MICROS);
                if lo <= start && end <= hi { counts.live } else { counts.by_timestamp.range(lo..hi).map(|(_, count)| *count).sum() }
            })
            .sum()
    }

    pub fn logical_rows(&self) -> u64 {
        self.minutes.values().map(|minute| minute.live).sum()
    }

    pub fn physical_keys(&self) -> usize {
        self.winners.len()
    }

    /// Atomically persist the derived winners as Arrow IPC.
    ///
    /// The compact timestamp histogram is rebuilt on load; persisting one
    /// canonical winner table avoids two sources of truth. A fingerprint is
    /// embedded in schema metadata and must match the caller's current Delta
    /// snapshot before the file can be served.
    pub fn save(&self, path: &Path, fingerprint: u64) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).with_context(|| format!("create logical-count cache directory {}", parent.display()))?;
        }
        let mut rows = self
            .winners
            .iter()
            .map(|(key, winner)| {
                let timestamp = i64::from_be_bytes(key[..8].try_into().expect("logical-count key always starts with timestamp"));
                let id = std::str::from_utf8(&key[8..]).context("logical-count key contains non-UTF8 id")?;
                Ok((timestamp, id, *winner))
            })
            .collect::<Result<Vec<_>>>()?;
        rows.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));

        let mut metadata = HashMap::new();
        metadata.insert(META_VERSION.to_string(), FORMAT_VERSION.to_string());
        metadata.insert(META_FINGERPRINT.to_string(), fingerprint.to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("timestamp", DataType::Int64, false),
                Field::new("id", DataType::Utf8, false),
                Field::new("tiebreak", DataType::Int64, true),
                Field::new("deleted", DataType::Boolean, false),
            ],
            metadata,
        ));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(Int64Array::from(rows.iter().map(|row| row.2.tiebreak).collect::<Vec<_>>())),
                Arc::new(BooleanArray::from(rows.iter().map(|row| row.2.deleted).collect::<Vec<_>>())),
            ],
        )?;

        let tmp = path.with_extension(format!("arrow.tmp-{}", uuid::Uuid::new_v4()));
        let write = || -> Result<()> {
            let file = File::create(&tmp).with_context(|| format!("create logical-count cache {}", tmp.display()))?;
            let mut writer = FileWriter::try_new(file, schema.as_ref())?;
            writer.write(&batch)?;
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
    pub fn load(path: &Path, expected_fingerprint: u64) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open logical-count cache {}", path.display()))?;
        let reader = FileReader::try_new(file, None)?;
        let schema = reader.schema();
        if schema.metadata().get(META_VERSION).map(String::as_str) != Some(FORMAT_VERSION) {
            bail!("unsupported logical-count cache format");
        }
        let fingerprint = schema
            .metadata()
            .get(META_FINGERPRINT)
            .context("logical-count cache missing fingerprint")?
            .parse::<u64>()
            .context("logical-count cache fingerprint is invalid")?;
        if fingerprint != expected_fingerprint {
            bail!("logical-count cache fingerprint mismatch: cached={fingerprint} current={expected_fingerprint}");
        }

        let mut index = Self::new();
        for batch in reader {
            let batch = batch?;
            let timestamps = batch.column(0).as_any().downcast_ref::<Int64Array>().context("logical-count timestamp column has wrong type")?;
            let ids = batch.column(1).as_any().downcast_ref::<StringArray>().context("logical-count id column has wrong type")?;
            let tiebreaks = batch.column(2).as_any().downcast_ref::<Int64Array>().context("logical-count tiebreak column has wrong type")?;
            let deleted = batch.column(3).as_any().downcast_ref::<BooleanArray>().context("logical-count deleted column has wrong type")?;
            for row in 0..batch.num_rows() {
                let tiebreak = tiebreaks.is_valid(row).then(|| tiebreaks.value(row));
                index.apply(timestamps.value(row), ids.value(row), tiebreak, deleted.value(row));
            }
        }
        Ok(index)
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
        index.save(&path, 99).unwrap();

        let loaded = LogicalCountIndex::load(&path, 99).unwrap();
        assert_eq!(loaded.physical_keys(), 2);
        assert_eq!(loaded.logical_rows(), 1);
        assert_eq!(loaded.count(60_000_000, 60_000_002), 1);
        assert!(LogicalCountIndex::load(&path, 100).unwrap_err().to_string().contains("fingerprint mismatch"));
        assert!(!path.with_extension("arrow.tmp").exists());
    }

    #[test]
    fn cache_lazily_loads_only_matching_partition_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let key = CountPartition { project_id: "p/unsafe".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let cache = LogicalCountCache::new(dir.path().to_path_buf());
        let mut index = LogicalCountIndex::new();
        index.apply(42, "id", Some(1), false);
        cache.install(key.clone(), 7, index).unwrap();
        assert_eq!(cache.get(&key, 7).unwrap().logical_rows(), 1);
        assert!(cache.get(&key, 8).is_none());

        let restarted = LogicalCountCache::new(dir.path().to_path_buf());
        assert_eq!(restarted.get(&key, 7).unwrap().count(0, 100), 1);
        restarted.invalidate(&key);
        assert!(restarted.get(&key, 8).is_none());
        assert!(dir.path().join("6f74656c/702f756e73616665/323032362d30382d3034.arrow").exists());
    }

    #[test]
    fn cache_paths_cannot_alias_distinct_partition_names() {
        let cache = LogicalCountCache::new(PathBuf::from("unused"));
        let slash = CountPartition { project_id: "a/b".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        let underscore = CountPartition { project_id: "a_b".into(), table_name: "otel".into(), date: "2026-08-04".into() };
        assert_ne!(cache.path(&slash), cache.path(&underscore));
    }
}
