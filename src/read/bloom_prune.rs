//! File-level needle pruning: per-file bloom sidecars consulted at
//! file-selection time so a point lookup (`trace_id = '…'`) selects only
//! files that can contain the needle, instead of every file in the window
//! (measured 284→284 file-level pruning at 24h before this existed).
//!
//! The builder never decodes rows — it lifts each file's EXISTING parquet
//! blooms (already folded to actual NDV by the writer) off the footer
//! metadata and stores them per (project, date) in object storage. The
//! read path consults only the in-process registry: a miss spawns one
//! background load and contributes no rejections, so the plan path never
//! awaits IO. Staleness is safe in both directions — an unknown file is
//! included, a rejected-but-retired file is a no-op, and Delta never
//! reuses a file path for different contents.
//!
//! Design + review trail: docs/plans/2026-08-22-file-level-needle-pruning.md

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use bincode::{Decode, Encode};
use dashmap::{DashMap, DashSet};
use datafusion::{
    logical_expr::{Expr, Operator, utils::split_conjunction},
    scalar::ScalarValue,
};
use deltalake::datafusion::parquet::{
    arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    bloom_filter::Sbbf,
};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::time::Instant;
use tracing::{debug, warn};

/// IN-lists above this skip pruning — probing the registry per value must
/// stay negligible next to the planning it saves.
pub const MAX_NEEDLE_VALUES: usize = 64;
/// A file whose bloom payload exceeds this is recorded `no_bloom`: a bloom
/// this dense prunes little and would bloat the blob (whale-file guard).
pub const PER_FILE_BLOOM_CAP_BYTES: u64 = 4 * 1024 * 1024;
/// Windows wider than this skip pruning — the per-date probe cost scales
/// with the window while its value concentrates in point lookups.
pub const MAX_PRUNE_DATES: usize = 92;
const SIDECAR_VERSION: u8 = 1;
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// One file's serialized blooms: column name → serialized `Sbbf`
/// (header+bitset, byte-identical to the parquet payload) per row group.
/// A column appears only when EVERY row group has a bloom — a partial set
/// cannot prove absence.
#[derive(Encode, Decode)]
pub struct FileBlooms {
    pub rel: String,
    pub no_bloom: bool,
    pub columns: Vec<(String, Vec<Vec<u8>>)>,
}

#[derive(Default, Encode, Decode)]
pub struct DateSidecar {
    pub files: Vec<FileBlooms>,
}

pub fn sidecar_path(table: &str, project_id: &str, date: &str) -> Path {
    Path::from(format!("{table}/{project_id}/{date}.bin"))
}

pub fn encode_sidecar(sidecar: &DateSidecar) -> Result<Vec<u8>> {
    Ok(std::iter::once(SIDECAR_VERSION).chain(bincode::encode_to_vec(sidecar, BINCODE_CONFIG)?).collect())
}

pub fn decode_sidecar(bytes: &[u8]) -> Result<DateSidecar> {
    match bytes.split_first() {
        Some((&SIDECAR_VERSION, rest)) => Ok(bincode::decode_from_slice(rest, BINCODE_CONFIG)?.0),
        Some((v, _)) => Err(anyhow!("unknown bloom sidecar version {v}")),
        None => Err(anyhow!("empty bloom sidecar")),
    }
}

/// `project_id=<pid>/date=<d>/part-….parquet` → (pid, date).
///
/// ```
/// use timefusion::read::bloom_prune::project_date_of_rel;
/// assert_eq!(project_date_of_rel("project_id=abc/date=2026-08-22/part-0.parquet"), Some(("abc", "2026-08-22")));
/// assert_eq!(project_date_of_rel("weird/path.parquet"), None);
/// ```
pub fn project_date_of_rel(rel: &str) -> Option<(&str, &str)> {
    let mut segs = rel.split('/');
    let pid = segs.next()?.strip_prefix("project_id=")?;
    let date = segs.next()?.strip_prefix("date=")?;
    Some((pid, date))
}

/// Equality/IN needles over bloom-enabled, non-version-mutable string
/// columns, from the query's top-level conjuncts. Two conjuncts on the same
/// column merge value lists — weaker (OR for rejection) but always safe.
pub fn extract_needles(filters: &[Expr], schema: &crate::schema::TableSchema, mutable: Option<&HashSet<String>>) -> Vec<(String, Vec<String>)> {
    let eligible = |name: &str| schema.fields.iter().any(|f| f.name == name && f.bloom_filter) && !mutable.is_some_and(|m| m.contains(name));
    let lit_str = |e: &Expr| match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    };
    filters
        .iter()
        .flat_map(|f| split_conjunction(f))
        .filter_map(|conjunct| {
            let (col, values) = match conjunct {
                Expr::BinaryExpr(b) if b.op == Operator::Eq => match (&*b.left, &*b.right) {
                    (Expr::Column(c), rhs) => (c.name.clone(), lit_str(rhs).map(|v| vec![v])),
                    (lhs, Expr::Column(c)) => (c.name.clone(), lit_str(lhs).map(|v| vec![v])),
                    _ => return None,
                },
                Expr::InList(l) if !l.negated && l.list.len() <= MAX_NEEDLE_VALUES => match &*l.expr {
                    Expr::Column(c) => (c.name.clone(), l.list.iter().map(lit_str).collect::<Option<Vec<_>>>()),
                    _ => return None,
                },
                _ => return None,
            };
            values.filter(|_| eligible(&col)).map(|values| (col, values))
        })
        .fold(HashMap::<String, Vec<String>>::new(), |mut by_col, (col, values)| {
            by_col.entry(col).or_default().extend(values);
            by_col
        })
        .into_iter()
        .collect()
}

/// Dates (YYYY-MM-DD, UTC) covered by a micros time range; `None` when the
/// range is unbounded or wider than `MAX_PRUNE_DATES`.
///
/// ```
/// use timefusion::read::bloom_prune::dates_in_range;
/// const DAY: i64 = 86_400_000_000;
/// assert_eq!(dates_in_range((0, 0)).unwrap(), ["1970-01-01"]);
/// assert_eq!(dates_in_range((0, 2 * DAY)).unwrap().len(), 3);
/// assert!(dates_in_range((0, 400 * DAY)).is_none(), "over-wide windows skip pruning");
/// assert!(dates_in_range((i64::MIN, 0)).is_none(), "unbounded windows skip pruning");
/// ```
pub fn dates_in_range(range: (i64, i64)) -> Option<Vec<String>> {
    let day = |micros: i64| chrono::DateTime::from_timestamp_micros(micros).map(|dt| dt.date_naive());
    let (lo, hi) = (day(range.0)?, day(range.1)?);
    let span = usize::try_from((hi - lo).num_days()).ok().filter(|s| *s < MAX_PRUNE_DATES)?;
    Some(lo.iter_days().take(span + 1).map(|d| d.format("%Y-%m-%d").to_string()).collect())
}

struct FileProbe {
    no_bloom: bool,
    columns: HashMap<String, Vec<Sbbf>>,
}

impl FileProbe {
    /// True iff some needle column has complete blooms in this file and every
    /// value of that needle misses in every row group — the file provably
    /// contains no matching row (and, because updates and tombstones are
    /// full-row copies, no other version of one either).
    fn rejects(&self, needles: &[(String, Vec<String>)]) -> bool {
        !self.no_bloom
            && needles
                .iter()
                .any(|(col, values)| self.columns.get(col).is_some_and(|blooms| values.iter().all(|v| blooms.iter().all(|b| !b.check(v.as_str())))))
    }
}

/// (table, project_id, date) — the resident-cache key.
type DateKey = (String, String, String);

pub struct DateBlooms {
    files: HashMap<String, FileProbe>,
    bytes: usize,
    loaded_at: Instant,
}

impl DateBlooms {
    fn from_sidecar(sidecar: DateSidecar, raw_len: usize) -> Self {
        let files = sidecar
            .files
            .into_iter()
            .filter_map(|f| {
                let columns = f
                    .columns
                    .into_iter()
                    .map(|(col, blooms)| Ok((col, blooms.iter().map(|b| Sbbf::from_bytes(b)).collect::<Result<Vec<_>, _>>()?)))
                    .collect::<Result<HashMap<_, _>, deltalake::datafusion::parquet::errors::ParquetError>>()
                    .ok()?; // parse failure ⇒ drop the entry ⇒ file included
                Some((f.rel, FileProbe { no_bloom: f.no_bloom, columns }))
            })
            .collect();
        Self { files, bytes: raw_len, loaded_at: Instant::now() }
    }
}

#[derive(Default)]
pub struct BloomPruneStats {
    pub queries_pruned: AtomicU64,
    pub files_probed: AtomicU64,
    pub files_rejected: AtomicU64,
    pub registry_hits: AtomicU64,
    pub registry_misses: AtomicU64,
    pub loads: AtomicU64,
    pub load_errors: AtomicU64,
    pub build_files: AtomicU64,
    pub build_errors: AtomicU64,
}

/// Resident per-(table, project, date) bloom cache. The plan path calls
/// `rejected_rels` and never blocks; loads and refreshes happen on spawned
/// tasks, single-flighted per key.
pub struct BloomPruneRegistry {
    store: Arc<dyn ObjectStore>,
    entries: DashMap<DateKey, Arc<DateBlooms>>,
    inflight: DashSet<DateKey>,
    bytes: AtomicUsize,
    cap_bytes: usize,
    refresh: Duration,
    pub stats: BloomPruneStats,
}

impl std::fmt::Debug for BloomPruneRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomPruneRegistry").field("entries", &self.entries.len()).field("bytes", &self.bytes.load(Relaxed)).finish()
    }
}

impl BloomPruneRegistry {
    pub fn new(store: Arc<dyn ObjectStore>, cap_bytes: usize, refresh: Duration) -> Self {
        Self { store, entries: DashMap::new(), inflight: DashSet::new(), bytes: AtomicUsize::new(0), cap_bytes, refresh, stats: BloomPruneStats::default() }
    }

    /// Table-relative paths among the resident sidecars for `dates` that
    /// provably lack every needle. Files without a sidecar entry are simply
    /// absent from the result — inclusion on unknown.
    pub fn rejected_rels(self: &Arc<Self>, table: &str, project_id: &str, dates: &[String], needles: &[(String, Vec<String>)]) -> HashSet<String> {
        let mut rejected = HashSet::new();
        // Effectful loop: each date may spawn a load and bumps counters.
        for date in dates {
            let key = (table.to_string(), project_id.to_string(), date.clone());
            let Some(blooms) = self.entries.get(&key).map(|e| e.clone()) else {
                self.stats.registry_misses.fetch_add(1, Relaxed);
                self.spawn_load(key);
                continue;
            };
            self.stats.registry_hits.fetch_add(1, Relaxed);
            if blooms.loaded_at.elapsed() > self.refresh {
                self.spawn_load(key);
            }
            let hits: Vec<_> = blooms.files.iter().filter(|(_, probe)| probe.rejects(needles)).map(|(rel, _)| rel.clone()).collect();
            self.stats.files_probed.fetch_add(blooms.files.len() as u64, Relaxed);
            self.stats.files_rejected.fetch_add(hits.len() as u64, Relaxed);
            rejected.extend(hits);
        }
        if !rejected.is_empty() {
            self.stats.queries_pruned.fetch_add(1, Relaxed);
        }
        rejected
    }

    fn spawn_load(self: &Arc<Self>, key: DateKey) {
        if !self.inflight.insert(key.clone()) {
            return; // single-flight: a load for this key is already running
        }
        let reg = self.clone();
        tokio::spawn(async move {
            reg.stats.loads.fetch_add(1, Relaxed);
            match reg.load(&key).await {
                Ok(entry) => reg.insert(key.clone(), entry),
                // NotFound is the common cold case (date not built yet):
                // cache an empty entry so we don't re-GET per query.
                Err(e) if matches!(e.downcast_ref::<object_store::Error>(), Some(object_store::Error::NotFound { .. })) => {
                    reg.insert(key.clone(), DateBlooms::from_sidecar(DateSidecar::default(), 64));
                }
                Err(e) => {
                    reg.stats.load_errors.fetch_add(1, Relaxed);
                    debug!(?key, "bloom sidecar load failed: {e:#}");
                }
            }
            reg.inflight.remove(&key);
        });
    }

    async fn load(&self, (table, project_id, date): &DateKey) -> Result<DateBlooms> {
        let bytes = self.store.get(&sidecar_path(table, project_id, date)).await?.bytes().await?;
        Ok(DateBlooms::from_sidecar(decode_sidecar(&bytes)?, bytes.len()))
    }

    fn insert(&self, key: DateKey, entry: DateBlooms) {
        let added = entry.bytes;
        if let Some(old) = self.entries.insert(key, Arc::new(entry)) {
            self.bytes.fetch_sub(old.bytes, Relaxed);
        }
        self.bytes.fetch_add(added, Relaxed);
        // Byte-capped: evict oldest-loaded until under. Entry count is small
        // (dates × projects touched), so a scan per eviction is fine.
        while self.bytes.load(Relaxed) > self.cap_bytes {
            let Some(oldest) = self.entries.iter().min_by_key(|e| e.value().loaded_at).map(|e| e.key().clone()) else { break };
            if let Some((_, old)) = self.entries.remove(&oldest) {
                self.bytes.fetch_sub(old.bytes, Relaxed);
            }
        }
    }

    pub async fn load_sidecar_raw(&self, table: &str, project_id: &str, date: &str) -> Option<DateSidecar> {
        let bytes = self.store.get(&sidecar_path(table, project_id, date)).await.ok()?.bytes().await.ok()?;
        decode_sidecar(&bytes).map_err(|e| warn!(table, project_id, date, "corrupt bloom sidecar, rebuilding: {e:#}")).ok()
    }

    /// Persist a rebuilt sidecar and refresh the resident entry in place so
    /// readers see it without waiting out the TTL.
    pub async fn store_sidecar(&self, table: &str, project_id: &str, date: &str, sidecar: DateSidecar) -> Result<()> {
        let bytes = encode_sidecar(&sidecar)?;
        let raw_len = bytes.len();
        self.store.put(&sidecar_path(table, project_id, date), bytes.into()).await.context("put bloom sidecar")?;
        self.insert((table.to_string(), project_id.to_string(), date.to_string()), DateBlooms::from_sidecar(sidecar, raw_len));
        Ok(())
    }

    pub fn resident_bytes(&self) -> usize {
        self.bytes.load(Relaxed)
    }
}

/// Lift `cols`' existing parquet blooms out of one file: footer + bloom
/// ranges only, no row decode. `no_bloom` when the file predates bloom
/// writing or its payload exceeds the density cap.
pub async fn build_file_blooms(store: Arc<dyn ObjectStore>, rel: &str, file_size: u64, cols: &[String]) -> Result<FileBlooms> {
    let reader = ParquetObjectReader::new(store, Path::from(rel)).with_file_size(file_size);
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.context("parquet footer")?;
    let n_rg = builder.metadata().num_row_groups();
    // Leaf indices are resolved up front because reading a bloom borrows the builder mutably.
    let leaves: Vec<Option<usize>> = cols.iter().map(|col| builder.parquet_schema().columns().iter().position(|c| c.path().string() == *col)).collect();
    // Imperative: each bloom is an await, and both the labeled per-column exit
    // and the cap's early return escape a stream pipeline.
    let mut columns = Vec::with_capacity(cols.len());
    let mut total = 0u64;
    'col: for (col, leaf) in cols.iter().zip(leaves).filter_map(|(col, leaf)| Some((col, leaf?))) {
        let mut blooms = Vec::with_capacity(n_rg);
        for rg in 0..n_rg {
            // A column qualifies only with a bloom in EVERY row group — a
            // partial set can't prove absence.
            let Some(sbbf) = builder.get_row_group_column_bloom_filter(rg, leaf).await.context("read bloom")? else { continue 'col };
            let mut bytes = Vec::new();
            sbbf.write(&mut bytes).map_err(|e| anyhow!("serialize bloom: {e}"))?;
            total += bytes.len() as u64;
            if total > PER_FILE_BLOOM_CAP_BYTES {
                return Ok(FileBlooms { rel: rel.to_string(), no_bloom: true, columns: Vec::new() });
            }
            blooms.push(bytes);
        }
        columns.push((col.clone(), blooms));
    }
    Ok(FileBlooms { rel: rel.to_string(), no_bloom: columns.is_empty(), columns })
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    /// `f1` blooms `context___trace_id` over {present-a, present-b}; `f2` is
    /// `no_bloom`. Built through encode/decode so every case also exercises the
    /// sidecar round trip (a serialize/parse fault would show as a false negative).
    fn fixture() -> DateBlooms {
        let mut bloom = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
        for v in ["present-a", "present-b"] {
            bloom.insert(v);
        }
        let mut bytes = Vec::new();
        bloom.write(&mut bytes).unwrap();
        let sidecar = DateSidecar {
            files: vec![
                FileBlooms {
                    rel: "project_id=p/date=2026-08-22/f1.parquet".into(),
                    no_bloom: false,
                    columns: vec![("context___trace_id".into(), vec![bytes])],
                },
                FileBlooms { rel: "project_id=p/date=2026-08-22/f2.parquet".into(), no_bloom: true, columns: vec![] },
            ],
        };
        DateBlooms::from_sidecar(decode_sidecar(&encode_sidecar(&sidecar).unwrap()).unwrap(), 0)
    }

    /// `None` = the file is unknown to the sidecar, i.e. included on unknown.
    #[test_case("f1", "context___trace_id", &["present-a"] => Some(false) ; "present needle keeps the file")]
    #[test_case("f1", "context___trace_id", &["absent-value"] => Some(true) ; "absent needle rejects the file")]
    #[test_case("f1", "context___trace_id", &["absent", "present-b"] => Some(false) ; "IN-list: any present value keeps")]
    #[test_case("f1", "id", &["absent"] => Some(false) ; "needle on an unbloomed column keeps")]
    #[test_case("f2", "context___trace_id", &["absent"] => Some(false) ; "no_bloom file is never rejected")]
    #[test_case("unknown", "context___trace_id", &["absent"] => None ; "unknown file is unknown")]
    fn probe_semantics(file: &str, col: &str, values: &[&str]) -> Option<bool> {
        let needles: Vec<(String, Vec<String>)> = vec![(col.to_string(), values.iter().map(|v| v.to_string()).collect())];
        fixture().files.get(&format!("project_id=p/date=2026-08-22/{file}.parquet")).map(|probe| probe.rejects(&needles))
    }
}
