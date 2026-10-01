//! File-level needle pruning: per-(project, date) sidecars holding each
//! file's parquet blooms, consulted at file-selection time so a point lookup
//! selects only files that can contain the needle.
//!
//! The plan path never awaits IO — a registry miss spawns a background load
//! and contributes no rejections. Staleness is safe both ways: an unknown file
//! is included, and a rejected-but-retired file is a no-op.

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
use deltalake::datafusion::parquet::{arrow::async_reader::ParquetRecordBatchStreamBuilder, bloom_filter::Sbbf};
use itertools::Itertools;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::time::Instant;
use tracing::{debug, warn};

/// IN-lists above this skip pruning: per-value probe cost must stay
/// negligible next to the planning it saves.
pub(crate) const MAX_NEEDLE_VALUES: usize = 64;
/// Per-file sidecar budget. Columns are kept smallest first while they fit, so a
/// large file drops its near-unique columns (dense blooms that prune little) but
/// keeps the cheap ones.
pub(crate) const PER_FILE_BLOOM_CAP_BYTES: u64 = 4 * 1024 * 1024;
/// Windows wider than this skip pruning: per-date probe cost scales with the
/// window, while the value concentrates in point lookups.
pub(crate) const MAX_PRUNE_DATES: usize = 92;
/// v1 recorded a file over the budget as `no_bloom`; those entries decode as absent so the
/// reconcile re-lifts them under the per-column fit.
const SIDECAR_VERSION: u8 = 2;
const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// One file's serialized blooms: column name → one serialized `Sbbf` per row
/// group. A column appears only when EVERY row group has a bloom — a partial
/// set cannot prove absence.
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
    let Some((&v, rest)) = bytes.split_first() else { anyhow::bail!("empty bloom sidecar") };
    anyhow::ensure!(matches!(v, 1..=SIDECAR_VERSION), "unknown bloom sidecar version {v}");
    let mut sidecar: DateSidecar = bincode::decode_from_slice(rest, BINCODE_CONFIG)?.0;
    if v == 1 {
        sidecar.files.retain(|f| !f.no_bloom);
    }
    Ok(sidecar)
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

/// Equality/IN needles over bloom-enabled string columns that are not
/// version-mutable (or are `enrich_only`, needles over non-empty values only),
/// from the query's top-level conjuncts. Two conjuncts on the same column merge
/// value lists — weaker, but always safe.
pub fn extract_needles(filters: &[Expr], schema: &crate::schema::TableSchema, mutable: Option<&HashSet<String>>) -> Vec<(String, Vec<String>)> {
    let enrich = |name: &str| schema.field(name).is_some_and(|f| f.enrich_only);
    let eligible = |name: &str| schema.field(name).is_some_and(|f| f.bloom_filter) && (enrich(name) || !mutable.is_some_and(|m| m.contains(name)));
    filters
        .iter()
        .flat_map(|f| split_conjunction(f))
        .filter_map(|conjunct| {
            needle(conjunct, &eligible).filter(|(col, values)| values.len() <= MAX_NEEDLE_VALUES && !(enrich(col) && values.iter().any(String::is_empty)))
        })
        .into_group_map()
        .into_iter()
        .map(|(col, groups)| (col, groups.concat()))
        .collect()
}

/// `col = 'v'` / `col IN (...)` over an `enrich_only` column, every value
/// non-empty: an older version (still empty) cannot match unless the winner does.
pub(crate) fn is_enrich_needle(e: &Expr, schema: &crate::schema::TableSchema) -> bool {
    needle(e, &|name| schema.field(name).is_some_and(|f| f.enrich_only)).is_some_and(|(_, values)| !values.iter().any(String::is_empty))
}

/// `col = 'v'`, `col IN (...)`, or an OR of those over one column — DataFusion
/// lowers IN lists / `= ANY(array)` of up to 3 items to such an OR chain. An
/// AND implies either side, so it yields whichever side is a needle.
fn needle(e: &Expr, eligible: &impl Fn(&str) -> bool) -> Option<(String, Vec<String>)> {
    let lit_str = |e: &Expr| match e {
        Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    };
    let on = |c: &datafusion::common::Column, values: Option<Vec<String>>| values.filter(|_| eligible(&c.name)).map(|v| (c.name.clone(), v));
    match e {
        Expr::BinaryExpr(b) => match (b.op, &*b.left, &*b.right) {
            (Operator::Eq, Expr::Column(c), v) | (Operator::Eq, v, Expr::Column(c)) => on(c, lit_str(v).map(|v| vec![v])),
            (Operator::Or, l, r) => {
                let ((lc, mut lv), (rc, rv)) = (needle(l, eligible)?, needle(r, eligible)?);
                (lc == rc).then(|| {
                    lv.extend(rv);
                    (lc, lv)
                })
            }
            (Operator::And, l, r) => needle(l, eligible).or_else(|| needle(r, eligible)),
            _ => None,
        },
        Expr::InList(l) if !l.negated => match &*l.expr {
            Expr::Column(c) => on(c, l.list.iter().map(lit_str).collect()),
            _ => None,
        },
        _ => None,
    }
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
    /// True iff some needle column has complete blooms here and every value of
    /// that needle misses in every row group, so the file provably contains no
    /// matching row (updates and tombstones are full-row copies, so no other
    /// version of one either).
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
                let columns: HashMap<_, _> = f
                    .columns
                    .into_iter()
                    .map(|(col, blooms)| blooms.iter().map(|b| Sbbf::from_bytes(b)).collect::<Result<Vec<_>, _>>().map(|s| (col, s)))
                    .collect::<Result<_, _>>()
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
        // Evict oldest-loaded until under the byte cap; entry count is small,
        // so a scan per eviction is fine.
        while self.bytes.load(Relaxed) > self.cap_bytes {
            let Some(oldest) = self.entries.iter().min_by_key(|e| e.value().loaded_at).map(|e| e.key().clone()) else { break };
            if let Some((_, old)) = self.entries.remove(&oldest) {
                self.bytes.fetch_sub(old.bytes, Relaxed);
            }
        }
    }

    /// `Ok(None)` ONLY for a sidecar that does not exist. A transient GET failure must stay an
    /// `Err`: reported as "absent" it would let the rebuild pass republish a truncated sidecar
    /// over a good one.
    pub async fn load_sidecar_raw(&self, table: &str, project_id: &str, date: &str) -> Result<Option<DateSidecar>> {
        let bytes = match self.store.get(&sidecar_path(table, project_id, date)).await {
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            r => r.context("get bloom sidecar")?.bytes().await.context("read bloom sidecar")?,
        };
        // Corruption IS a rebuild trigger: the bytes are unusable either way.
        Ok(decode_sidecar(&bytes).map_err(|e| warn!(table, project_id, date, "corrupt bloom sidecar, rebuilding: {e:#}")).ok())
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

/// Lift `cols`' existing parquet blooms out of one file: footer + bloom ranges
/// only, no row decode. Keeps the columns that `fit_columns` admits; `no_bloom`
/// when none qualifies.
pub async fn build_file_blooms(store: Arc<dyn ObjectStore>, rel: &str, file_size: u64, cols: &[String]) -> Result<FileBlooms> {
    let reader = crate::storage::ObjectStoreReader::new(store, Path::from(rel), file_size);
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.context("parquet footer")?;
    let n_rg = builder.metadata().num_row_groups();
    // Leaf indices are resolved up front because reading a bloom borrows the builder mutably.
    let leaves: Vec<Option<usize>> = cols.iter().map(|col| builder.parquet_schema().columns().iter().position(|c| c.path().string() == *col)).collect();
    let mut columns = Vec::with_capacity(cols.len());
    'col: for (col, leaf) in cols.iter().zip(leaves).filter_map(|(col, leaf)| Some((col, leaf?))) {
        let (mut blooms, mut size) = (Vec::with_capacity(n_rg), 0u64);
        for rg in 0..n_rg {
            // A column qualifies only with a bloom in EVERY row group.
            let Some(sbbf) = builder.get_row_group_column_bloom_filter(rg, leaf).await.context("read bloom")? else { continue 'col };
            let mut bytes = Vec::new();
            sbbf.write(&mut bytes).map_err(|e| anyhow!("serialize bloom: {e}"))?;
            size += bytes.len() as u64;
            if size > PER_FILE_BLOOM_CAP_BYTES {
                continue 'col;
            }
            blooms.push(bytes);
        }
        columns.push((col.clone(), blooms));
    }
    let columns = fit_columns(columns, PER_FILE_BLOOM_CAP_BYTES);
    Ok(FileBlooms { rel: rel.to_string(), no_bloom: columns.is_empty(), columns })
}

/// The smallest columns whose blooms fit `cap` together.
fn fit_columns(mut columns: Vec<(String, Vec<Vec<u8>>)>, cap: u64) -> Vec<(String, Vec<Vec<u8>>)> {
    let size = |(_, blooms): &(String, Vec<Vec<u8>>)| blooms.iter().map(|b| b.len() as u64).sum::<u64>();
    columns.sort_by_cached_key(size);
    columns
        .into_iter()
        .scan(0, |total, c| {
            *total += size(&c);
            (*total <= cap).then_some(c)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    /// `f1` blooms `context___trace_id` over {present-a, present-b}; `f2` is
    /// `no_bloom`. Built through encode/decode so every case also exercises the
    /// sidecar round trip.
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

    /// Sizes are per-column bloom bytes; a large file keeps its cheap columns rather than none.
    #[test_case(&[("id", 5), ("session", 1), ("trace", 2)], 4 => vec!["session", "trace"] ; "drops the dense column that busts the cap")]
    #[test_case(&[("id", 5), ("session", 1)], 10 => vec!["session", "id"] ; "everything fits")]
    #[test_case(&[("id", 5)], 4 => Vec::<&str>::new() ; "nothing fits")]
    fn fit_columns_keeps_smallest(cols: &[(&str, usize)], cap: u64) -> Vec<String> {
        let cols = cols.iter().map(|(c, n)| (c.to_string(), vec![vec![0u8; *n]])).collect();
        fit_columns(cols, cap).into_iter().map(|(c, _)| c).collect()
    }

    /// v1 sidecars decode with their over-cap `no_bloom` stubs dropped, so reconcile re-lifts them;
    /// a current-version stub is a settled verdict and stays.
    #[test_case(1 => vec!["f1"] ; "v1 stub is absent")]
    #[test_case(SIDECAR_VERSION => vec!["f1", "f2"] ; "current stub is kept")]
    fn legacy_no_bloom_decodes_absent(version: u8) -> Vec<String> {
        let entry = |rel: &str, no_bloom| FileBlooms { rel: rel.into(), no_bloom, columns: vec![] };
        let mut bytes = encode_sidecar(&DateSidecar { files: vec![entry("f1", false), entry("f2", true)] }).unwrap();
        bytes[0] = version;
        decode_sidecar(&bytes).unwrap().files.into_iter().map(|f| f.rel).collect()
    }

    const T: &str = "context___trace_id";
    const S: &str = "attributes___session___id";
    #[test_case("context___trace_id = 'a' OR context___trace_id = 'b' OR context___trace_id = 'c'", Some((T, vec!["a", "b", "c"])) ; "short IN / ANY lowered to an OR chain")]
    #[test_case("(context___trace_id = 'a' AND name = 'x') OR context___trace_id = 'b'", Some((T, vec!["a", "b"])) ; "AND legs yield their needle")]
    #[test_case("context___trace_id = 'a' OR name = 'b'", None ; "OR across columns proves nothing")]
    #[test_case("attributes___user___id = 'b'", None ; "a mutable column is never a needle")]
    #[test_case("attributes___session___id IN ('a', 'b')", Some((S, vec!["a", "b"])) ; "an enrich_only column is a needle")]
    #[test_case("attributes___session___id IN ('a', '')", None ; "an empty enrich_only value is not a needle")]
    #[test_case("context___trace_id NOT IN ('a')", None ; "NOT IN is never a needle")]
    fn needles_from_or_chains(sql: &str, want: Option<(&str, Vec<&str>)>) {
        use datafusion::{
            arrow::datatypes::{DataType, Field, Schema},
            common::DFSchema,
            prelude::SessionContext,
        };
        let fields = [T, S, "attributes___user___id", "name"].map(|n| Field::new(n, DataType::Utf8, true));
        let expr = SessionContext::new().parse_sql_expr(sql, &DFSchema::try_from(Schema::new(fields.to_vec())).unwrap()).unwrap();
        let mutable = crate::database::ProjectRoutingTable::version_mutable_columns("otel_logs_and_spans");
        let got = extract_needles(&[expr], crate::schema::get_schema("otel_logs_and_spans").unwrap(), mutable.as_ref());
        assert_eq!(got, want.map(|(c, v)| vec![(c.to_string(), v.into_iter().map(String::from).collect())]).unwrap_or_default());
    }
}
