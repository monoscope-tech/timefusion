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
use dashmap::DashMap;
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
    let mut out = vec![SIDECAR_VERSION];
    out.extend(bincode::encode_to_vec(sidecar, BINCODE_CONFIG)?);
    Ok(out)
}

pub fn decode_sidecar(bytes: &[u8]) -> Result<DateSidecar> {
    match bytes.split_first() {
        Some((&SIDECAR_VERSION, rest)) => Ok(bincode::decode_from_slice(rest, BINCODE_CONFIG)?.0),
        Some((v, _)) => Err(anyhow!("unknown bloom sidecar version {v}")),
        None => Err(anyhow!("empty bloom sidecar")),
    }
}

/// `project_id=<pid>/date=<d>/part-….parquet` → (pid, date).
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
    let mut by_col: HashMap<String, Vec<String>> = HashMap::new();
    for conjunct in filters.iter().flat_map(|f| split_conjunction(f)) {
        let (col, values) = match conjunct {
            Expr::BinaryExpr(b) if b.op == Operator::Eq => match (&*b.left, &*b.right) {
                (Expr::Column(c), rhs) => (c.name.clone(), lit_str(rhs).map(|v| vec![v])),
                (lhs, Expr::Column(c)) => (c.name.clone(), lit_str(lhs).map(|v| vec![v])),
                _ => continue,
            },
            Expr::InList(l) if !l.negated && l.list.len() <= MAX_NEEDLE_VALUES => match &*l.expr {
                Expr::Column(c) => (c.name.clone(), l.list.iter().map(lit_str).collect::<Option<Vec<_>>>()),
                _ => continue,
            },
            _ => continue,
        };
        if let Some(values) = values.filter(|_| eligible(&col)) {
            by_col.entry(col).or_default().extend(values);
        }
    }
    by_col.into_iter().collect()
}

/// Dates (YYYY-MM-DD, UTC) covered by a micros time range; `None` when the
/// range is unbounded or wider than `MAX_PRUNE_DATES`.
pub fn dates_in_range(range: (i64, i64)) -> Option<Vec<String>> {
    let day = |micros: i64| chrono::DateTime::from_timestamp_micros(micros).map(|dt| dt.date_naive());
    let (lo, hi) = (day(range.0)?, day(range.1)?);
    let span = (hi - lo).num_days();
    if !(0..MAX_PRUNE_DATES as i64).contains(&span) {
        return None;
    }
    Some(lo.iter_days().take(span as usize + 1).map(|d| d.format("%Y-%m-%d").to_string()).collect())
}

struct FileProbe {
    no_bloom: bool,
    columns: HashMap<String, Vec<Sbbf>>,
}

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

    /// True iff some needle column has complete blooms in this file and every
    /// value of that needle misses in every row group — the file provably
    /// contains no matching row (and, because updates and tombstones are
    /// full-row copies, no other version of one either).
    fn rejects(&self, rel: &str, needles: &[(String, Vec<String>)]) -> Option<bool> {
        let probe = self.files.get(rel)?;
        if probe.no_bloom {
            return Some(false);
        }
        Some(
            needles.iter().any(|(col, values)| probe.columns.get(col).is_some_and(|blooms| values.iter().all(|v| blooms.iter().all(|b| !b.check(v.as_str()))))),
        )
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
    entries: DashMap<(String, String, String), Arc<DateBlooms>>,
    inflight: DashMap<(String, String, String), ()>,
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
        Self { store, entries: DashMap::new(), inflight: DashMap::new(), bytes: AtomicUsize::new(0), cap_bytes, refresh, stats: BloomPruneStats::default() }
    }

    /// Table-relative paths among the resident sidecars for `dates` that
    /// provably lack every needle. Files without a sidecar entry are simply
    /// absent from the result — inclusion on unknown.
    pub fn rejected_rels(self: &Arc<Self>, table: &str, project_id: &str, dates: &[String], needles: &[(String, Vec<String>)]) -> HashSet<String> {
        let mut rejected = HashSet::new();
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
            for rel in blooms.files.keys() {
                self.stats.files_probed.fetch_add(1, Relaxed);
                if blooms.rejects(rel, needles) == Some(true) {
                    self.stats.files_rejected.fetch_add(1, Relaxed);
                    rejected.insert(rel.clone());
                }
            }
        }
        if !rejected.is_empty() {
            self.stats.queries_pruned.fetch_add(1, Relaxed);
        }
        rejected
    }

    fn spawn_load(self: &Arc<Self>, key: (String, String, String)) {
        if self.inflight.insert(key.clone(), ()).is_some() {
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
                    reg.insert(key.clone(), DateBlooms { files: HashMap::new(), bytes: 64, loaded_at: Instant::now() });
                }
                Err(e) => {
                    reg.stats.load_errors.fetch_add(1, Relaxed);
                    debug!(?key, "bloom sidecar load failed: {e:#}");
                }
            }
            reg.inflight.remove(&key);
        });
    }

    async fn load(&self, (table, project_id, date): &(String, String, String)) -> Result<DateBlooms> {
        let bytes = self.store.get(&sidecar_path(table, project_id, date)).await?.bytes().await?;
        Ok(DateBlooms::from_sidecar(decode_sidecar(&bytes)?, bytes.len()))
    }

    fn insert(&self, key: (String, String, String), entry: DateBlooms) {
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
    let leaves: Vec<Option<usize>> = cols.iter().map(|col| builder.parquet_schema().columns().iter().position(|c| c.path().string() == *col)).collect();
    let mut columns = Vec::new();
    let mut total = 0u64;
    'col: for (col, leaf) in cols.iter().zip(leaves) {
        let Some(leaf) = leaf else { continue };
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
    use super::*;

    #[test]
    fn sidecar_roundtrip_and_probe_semantics() {
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
        let decoded = decode_sidecar(&encode_sidecar(&sidecar).unwrap()).unwrap();
        let blooms = DateBlooms::from_sidecar(decoded, 0);

        let needle = |v: &str| vec![("context___trace_id".to_string(), vec![v.to_string()])];
        // No false negatives through the serialize/parse round trip.
        assert_eq!(blooms.rejects("project_id=p/date=2026-08-22/f1.parquet", &needle("present-a")), Some(false));
        assert_eq!(blooms.rejects("project_id=p/date=2026-08-22/f1.parquet", &needle("absent-value")), Some(true));
        // IN-list: any present value keeps the file.
        let in_list = vec![("context___trace_id".to_string(), vec!["absent".to_string(), "present-b".to_string()])];
        assert_eq!(blooms.rejects("project_id=p/date=2026-08-22/f1.parquet", &in_list), Some(false));
        // no_bloom and unknown files are never rejected.
        assert_eq!(blooms.rejects("project_id=p/date=2026-08-22/f2.parquet", &needle("absent")), Some(false));
        assert_eq!(blooms.rejects("project_id=p/date=2026-08-22/unknown.parquet", &needle("absent")), None);
        // A needle on a column the file has no bloom for keeps the file.
        let other_col = vec![("id".to_string(), vec!["absent".to_string()])];
        assert_eq!(blooms.rejects("project_id=p/date=2026-08-22/f1.parquet", &other_col), Some(false));
    }

    #[test]
    fn dates_in_range_bounds() {
        let day = 86_400_000_000i64;
        assert_eq!(dates_in_range((0, 0)).unwrap(), vec!["1970-01-01"]);
        assert_eq!(dates_in_range((0, 2 * day)).unwrap().len(), 3);
        assert!(dates_in_range((0, 400 * day)).is_none(), "over-wide windows skip pruning");
        assert!(dates_in_range((i64::MIN, 0)).is_none(), "unbounded windows skip pruning");
    }

    #[test]
    fn rel_parse() {
        assert_eq!(project_date_of_rel("project_id=abc/date=2026-08-22/part-0.parquet"), Some(("abc", "2026-08-22")));
        assert_eq!(project_date_of_rel("weird/path.parquet"), None);
    }
}
