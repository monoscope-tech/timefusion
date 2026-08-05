use std::{collections::HashMap, fmt, path::PathBuf, sync::Arc};

use anyhow::Result;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use chrono::Utc;
use datafusion::{
    arrow::{array::Array, record_batch::RecordBatch},
    catalog::Session,
    common::{Statistics, not_impl_err},
    datasource::{
        TableProvider, TableType,
        sink::{DataSink, DataSinkExec},
    },
    error::{DataFusionError, Result as DFResult},
    execution::{TaskContext, context::SessionContext},
    logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown, col, dml::InsertOp, lit},
    physical_expr::expressions::{CastExpr, Column as PhysicalColumn},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream, projection::ProjectionExec, stream::RecordBatchStreamAdapter,
        union::UnionExec,
    },
    scalar::ScalarValue,
};
use datafusion_datasource::{file_scan_config::FileScanConfig, memory::MemorySourceConfig, source::DataSourceExec};
use datafusion_functions_json;
use deltalake::{
    DeltaTable, DeltaTableBuilder, PartitionFilter, datafusion::parquet::file::properties::WriterProperties, kernel::transaction::CommitProperties,
    logstore::LogStore, operations::create::CreateBuilder,
};
use futures::{StreamExt, TryStreamExt};
use instrumented_object_store::instrument_object_store;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, field::Empty, info, instrument, warn};
use url::Url;

use crate::{
    config::{self, AppConfig},
    errors::arrow_err,
    object_store_cache::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache},
    schema_loader::{create_insert_compatible_schema, get_default_schema, get_schema, is_variant_type},
    statistics::DeltaStatisticsExtractor,
};

// Unified tables: one Delta table per schema (table_name -> DeltaTable)
// All default projects share the same table, with project_id as a partition column
pub type UnifiedTables = Arc<RwLock<HashMap<String, Arc<RwLock<DeltaTable>>>>>;

/// Soft size at which the no-eviction table caches log a warning.
/// Picked at 10× the documented design target ("thousands of tenants").
/// Crossings are once-per-threshold-multiple, so a runaway tenant churn
/// surfaces as growing log frequency rather than a single quiet spike.
const CACHE_SOFT_LIMIT_WARN: usize = 10_000;

/// Per-key build de-duplicator for the cached Delta `TableProvider`. The inner
/// `OnceCell` is initialised exactly once per `(project, table, version)`; all
/// concurrent first-time misses share the same Arc and await the same build.
type DeltaProviderCell = tokio::sync::OnceCell<Arc<dyn datafusion::datasource::TableProvider>>;

struct CachedDeltaProvider {
    version: u64,
    created_at: std::time::Instant,
    cell: Arc<DeltaProviderCell>,
}

impl fmt::Debug for CachedDeltaProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedDeltaProvider").field("version", &self.version).field("age", &self.created_at.elapsed()).finish_non_exhaustive()
    }
}

/// How many snapshot versions to keep cached per `(project, table)`: the
/// latest plus two predecessors.
///
/// One entry per key made the cache degenerate under flush cadence: every
/// commit bumps `table.version()`, evicting the sole entry, so the next query
/// rebuilt the delta-rs provider from scratch (snapshot replay — the
/// `SnapshotVisitor` stacks that dominated live heap in prod, at a 65% hit
/// rate). Version observation is *not* monotone across concurrent readers
/// either: two tasks can hold `DeltaTable` handles at v=N and v=N+1 at the
/// same instant (`fast_resolve_cache` shares one handle that another task
/// updates), so a single slot thrashes between the two, each miss evicting
/// the other's freshly built provider.
///
/// Retaining a short ring costs at most 3× the provider state per hot key and
/// cannot serve stale data: lookup is by *exact* version, so a query only ever
/// gets the provider for the version its own resolved handle reports.
const PROVIDER_VERSION_RETENTION: usize = 3;

/// The recent-version ring for one `(project, table)`, newest first.
#[derive(Debug, Default)]
struct ProviderVersions {
    versions: Vec<CachedDeltaProvider>,
}

impl ProviderVersions {
    /// Cell for `version`, if cached and within TTL. Exact-version match — an
    /// older retained version is never handed to a query that resolved a newer
    /// snapshot.
    fn get(&self, version: u64, ttl: std::time::Duration) -> Option<Arc<DeltaProviderCell>> {
        self.versions.iter().find(|e| e.version == version && e.created_at.elapsed() <= ttl).map(|e| Arc::clone(&e.cell))
    }

    /// Install a fresh cell for `version` at the head, dropping any expired or
    /// same-version predecessor, and keep only `PROVIDER_VERSION_RETENTION`.
    fn install(&mut self, version: u64, ttl: std::time::Duration) -> Arc<DeltaProviderCell> {
        let cell = Arc::new(DeltaProviderCell::new());
        self.versions.retain(|e| e.version != version && e.created_at.elapsed() <= ttl);
        self.versions.insert(0, CachedDeltaProvider { version, created_at: std::time::Instant::now(), cell: Arc::clone(&cell) });
        self.versions.truncate(PROVIDER_VERSION_RETENTION);
        cell
    }

    /// Drop expired versions; returns how many were removed.
    fn prune(&mut self, ttl: std::time::Duration) -> usize {
        let before = self.versions.len();
        self.versions.retain(|e| e.created_at.elapsed() <= ttl);
        before - self.versions.len()
    }

    fn len(&self) -> usize {
        self.versions.len()
    }

    /// Is `cell` still retained (at any version)?
    fn holds(&self, cell: &Arc<DeltaProviderCell>) -> bool {
        self.versions.iter().any(|e| Arc::ptr_eq(&e.cell, cell))
    }
}

type DeltaProviderCache = Arc<dashmap::DashMap<(String, String), ProviderVersions>>;
/// (project_id, date, bucket_id) — a dirty-bin drain candidate.
type DrainBin = (String, String, i64);
type FastResolveCache = Arc<dashmap::DashMap<(String, String), Arc<RwLock<DeltaTable>>>>;

/// Captured per-scan to feed `ScanMetrics::record_scan`. Cheap to copy.
#[derive(Debug, Default, Clone, Copy)]
struct ScanShape {
    skipped_delta: bool,
    has_mem: bool,
    has_delta: bool,
    fast_resolve_hit: Option<bool>,
    /// Read-side dedup skip engaged (all window partitions sweep-verified clean).
    skip_dedup: bool,
}

/// Counters surfaced via `timefusion_stats` for production debugging. Cheap to
/// update on the hot path (Relaxed atomics); read via `snapshot()`. Histogram
/// is fixed-bucket microsecond bins so percentile estimates are O(buckets) to
/// compute without sorting.
#[derive(Debug, Default)]
pub struct ScanMetrics {
    pub scans_total: std::sync::atomic::AtomicU64,
    pub scans_skipped_delta: std::sync::atomic::AtomicU64,
    pub scans_mem_only: std::sync::atomic::AtomicU64,
    pub scans_delta_only: std::sync::atomic::AtomicU64,
    pub scans_mem_plus_delta: std::sync::atomic::AtomicU64,
    pub fast_resolve_hits: std::sync::atomic::AtomicU64,
    pub fast_resolve_misses: std::sync::atomic::AtomicU64,
    /// Delta TableProvider cache: hit = cached cell at the current snapshot
    /// version; miss = either no entry, or an entry at a stale version that
    /// had to be replaced. Operators tracking the cold-start vs steady-state
    /// cliff watch the hit ratio: after the first ~tens of seconds per
    /// (project, table), this should stay high; a low ratio in prod means
    /// version is churning faster than expected (e.g. very aggressive
    /// compaction) and the cache isn't paying for itself.
    pub provider_cache_hits: std::sync::atomic::AtomicU64,
    pub provider_cache_misses: std::sync::atomic::AtomicU64,
    pub provider_cache_evictions: std::sync::atomic::AtomicU64,
    /// Provider builds that started against a version that was already
    /// stale by the time the build finished — the DashMap entry got
    /// replaced under us (a flush bumped the version) and the rebuilt
    /// provider had to be dropped. Cheap-to-skip in the steady state
    /// (flush cadence is seconds apart); a non-zero rate here under
    /// sustained traffic flags either very frequent compaction or a
    /// pathological version-churn pattern worth investigating.
    pub provider_build_abandoned: std::sync::atomic::AtomicU64,
    /// Planning-stage wall time. Totals pair with counts so
    /// `timefusion_stats` can expose an average without a lock or histogram.
    /// These deliberately stop before execution: they explain the gap between
    /// `EXPLAIN` wall time and the physical operators' elapsed metrics.
    pub provider_build_us_total: std::sync::atomic::AtomicU64,
    pub provider_build_total: std::sync::atomic::AtomicU64,
    pub provider_scan_us_total: std::sync::atomic::AtomicU64,
    pub provider_scan_total: std::sync::atomic::AtomicU64,
    pub mem_plan_us_total: std::sync::atomic::AtomicU64,
    pub mem_plan_total: std::sync::atomic::AtomicU64,
    pub hot_plan_us_total: std::sync::atomic::AtomicU64,
    pub hot_plan_total: std::sync::atomic::AtomicU64,
    /// Latency histogram of the full `ProjectRoutingTable::scan` call in
    /// microseconds. Buckets are powers of two so reads at any duration land
    /// in a single bucket via `usize::leading_zeros` math. Bucket i holds
    /// scans whose duration_us fits in `[1<<i, 1<<(i+1))`. 32 buckets covers
    /// 1us through ~1.2 hours.
    pub scan_latency_buckets: [std::sync::atomic::AtomicU64; 32],
    /// End-to-end pgwire query latency histogram (same bucket scheme as
    /// `scan_latency_buckets`). Recorded by `LoggingSimpleHandler` and
    /// `LoggingExtendedQueryHandler` around the `DfSessionService::do_query`
    /// call — the FULL server-side path from "harness received our query"
    /// through "result encoded back to client". Compare to scan p95/p99 to
    /// see how much of the user-visible tail is outside the scan call.
    pub pgwire_total: std::sync::atomic::AtomicU64,
    pub pgwire_latency_buckets: [std::sync::atomic::AtomicU64; 32],
    /// Parquet decode heap, measured at the `GatedScanExec` choke point.
    ///
    /// Decode is the one large consumer outside every budget — no DataFusion
    /// pool tracks it (`config.rs` says so explicitly), so it draws purely on
    /// whatever slack the configured budgets leave. On the prod box that slack
    /// is ~12GB, and a single wide scan's 48-way decode has exceeded it
    /// (2026-07-20). Nobody has ever had a number for it; these three make it
    /// measurable before anything tries to *bound* it:
    ///
    /// `worst-case concurrent decode heap ≈ decode_peak_batch_bytes ×
    /// decode_polls_inflight_peak`, which is the figure to size a Transient
    /// budget from.
    pub decode_bytes_total: std::sync::atomic::AtomicU64,
    pub decode_peak_batch_bytes: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight_peak: std::sync::atomic::AtomicU64,
    /// Decode polls that ran with pressure-reduced concurrency (see
    /// `scan_pressure_permits`). Non-zero means the process was close enough
    /// to its OOM line that wide-scan decodes were serialized instead of
    /// letting an allocation burst outrun reclaim.
    pub decode_pressure_throttled: std::sync::atomic::AtomicU64,
}

impl ScanMetrics {
    /// One gated decode entered: bump the in-flight gauge and its high-water
    /// mark. Returns nothing — the caller pairs it with `decode_end`.
    fn decode_begin(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let n = self.decode_polls_inflight.fetch_add(1, Relaxed) + 1;
        self.decode_polls_inflight_peak.fetch_max(n, Relaxed);
    }

    /// One gated decode finished, having produced `bytes` of Arrow.
    fn decode_end(&self, bytes: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.decode_polls_inflight.fetch_sub(1, Relaxed);
        self.decode_bytes_total.fetch_add(bytes, Relaxed);
        self.decode_peak_batch_bytes.fetch_max(bytes, Relaxed);
    }

    pub fn record_scan(&self, duration_us: u64, skipped_delta: bool, has_mem: bool, has_delta: bool, fast_resolve_hit: Option<bool>) {
        use std::sync::atomic::Ordering::Relaxed;
        self.scans_total.fetch_add(1, Relaxed);
        let by_source = match (has_mem, has_delta) {
            (true, false) => Some(&self.scans_mem_only),
            (false, true) => Some(&self.scans_delta_only),
            (true, true) => Some(&self.scans_mem_plus_delta),
            (false, false) => None,
        };
        let by_resolve = fast_resolve_hit.map(|hit| if hit { &self.fast_resolve_hits } else { &self.fast_resolve_misses });
        for c in skipped_delta.then_some(&self.scans_skipped_delta).into_iter().chain(by_source).chain(by_resolve) {
            c.fetch_add(1, Relaxed);
        }
        self.scan_latency_buckets[latency_bucket(duration_us)].fetch_add(1, Relaxed);
    }

    /// Record a pgwire end-to-end query duration. Cheap on hot path —
    /// just a counter bump and one histogram bin increment.
    pub fn record_pgwire_query(&self, duration_us: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.pgwire_total.fetch_add(1, Relaxed);
        self.pgwire_latency_buckets[latency_bucket(duration_us)].fetch_add(1, Relaxed);
    }

    /// Estimate percentile from the power-of-two histogram. Returns the upper
    /// bound of the bucket containing the p-th percentile, in microseconds.
    /// Coarse — accurate to a factor of 2 — but adequate for prod alerting.
    pub fn latency_percentile_us(&self, p: f64) -> u64 {
        Self::percentile_from_buckets(&self.scan_latency_buckets, p)
    }
    pub fn pgwire_percentile_us(&self, p: f64) -> u64 {
        Self::percentile_from_buckets(&self.pgwire_latency_buckets, p)
    }
    fn percentile_from_buckets(buckets: &[std::sync::atomic::AtomicU64; 32], p: f64) -> u64 {
        use std::sync::atomic::Ordering::Relaxed;
        // Snapshot once: loading a second time for the cumulative walk could see
        // a distribution that no longer sums to `total` under concurrent updates.
        let counts = buckets.each_ref().map(|b| b.load(Relaxed));
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * p) as u64;
        counts
            .iter()
            .scan(0u64, |cum, c| {
                *cum += c;
                Some(*cum)
            })
            .position(|cum| cum >= target)
            .map_or(1u64 << 32, |i| 1u64 << (i + 1))
    }
}

/// Power-of-two microsecond bucket index for the 32-bin latency histograms.
fn latency_bucket(duration_us: u64) -> usize {
    if duration_us <= 1 { 0 } else { (64 - duration_us.leading_zeros() - 1).min(31) as usize }
}

// Custom project tables: projects with their own S3 bucket get isolated tables
// Key: (project_id, table_name) -> DeltaTable
pub type CustomProjectTables = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;

// Per-table (keyed by storage URL), per-date set of live file URIs at the last
// successful z-order optimize. Backs the ZOrder idempotence guard.
type ZOrderFilesets = Arc<RwLock<HashMap<String, HashMap<chrono::NaiveDate, std::collections::HashSet<String>>>>>;
/// Per-(project_id, table_name) DML serialization mutexes — see `Database::dml_lock`.
type DmlLocks = Arc<dashmap::DashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>;
/// Per-physical-table count of flush/ingest committers QUEUED on the commit lock
/// — see `Database::flush_waiters` and the priority check in `commit_wave`.
type FlushWaiterCounts = Arc<dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicUsize>>>;

/// RAII registration of one flush/ingest committer waiting for a per-table
/// commit lock. Waves read this count to decide whether to queue at all, so it
/// must drop the instant the flush acquires the lock — OR the instant its
/// future is cancelled by a watchdog/timeout. `Drop` covers both; a manual
/// decrement after `lock().await` silently leaks the count on cancellation and
/// would wedge maintenance forever.
struct FlushWaiter(Arc<std::sync::atomic::AtomicUsize>);

impl FlushWaiter {
    fn register(count: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(count.clone())
    }
}

impl Drop for FlushWaiter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Get a Delta table from custom project tables by project_id and table_name
pub async fn get_custom_delta_table(custom_tables: &CustomProjectTables, project_id: &str, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    custom_tables.read().await.get(&(project_id.to_string(), table_name.to_string())).cloned()
}

/// Get a Delta table from unified tables by table_name
pub async fn get_unified_delta_table(unified_tables: &UnifiedTables, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    unified_tables.read().await.get(table_name).cloned()
}

/// Should `resolve_*_table` call `update_state()` on the cached snapshot?
/// Refresh when this process knows the snapshot is behind (last_written ahead
/// of current) *or* when this process hasn't written but something else (e.g.
/// the buffered_write_layer's background flusher) may have committed. The
/// `(Some(_), None) => false` shortcut once tempted us — it broke buffer→Delta
/// visibility — so the bias is toward refreshing more often, not less.
fn should_refresh_table(current_version: Option<u64>, last_written_version: Option<u64>) -> bool {
    match (current_version, last_written_version) {
        (Some(current), Some(last)) => current < last,
        // Either: process hasn't directly written but a background flusher may have.
        // Or: snapshot has no version yet but we know someone wrote one.
        // Both warrant a refresh.
        (Some(_), None) | (None, Some(_)) => true,
        (None, None) => false,
    }
}

/// Max commits behind for the append-only fast catch-up in `refresh_table_snapshot`:
/// each commit in the range costs one log read for the Remove check, so cap it
/// and let larger gaps take the single full re-materialize instead.
const REFRESH_APPEND_CATCHUP_MAX_GAP: u64 = 64;

/// Refresh `table`'s snapshot WITHOUT holding the write lock across
/// `update_state()` — that's a full Delta log replay plus object-store IO
/// (1s+ per refresh on prod's 40k-action log), and every query refreshes
/// after a flush commit, so holding the write lock here convoyed all
/// concurrent planning behind it (observed 50-110s stalls, 2026-06-11).
/// Clone-update-swap instead: readers keep planning against the old snapshot
/// while a clone refreshes; the write lock is held only for the swap. The
/// swap is version-guarded because a concurrent committer (flush, optimize)
/// may have advanced the shared handle past our clone — never regress it.
/// Returns the shared handle's version after the refresh. Single choke-point
/// for snapshot refreshes so the lock discipline can't drift between sites.
pub(crate) async fn refresh_table_snapshot(table: &Arc<RwLock<DeltaTable>>, incremental: bool) -> std::result::Result<Option<u64>, deltalake::DeltaTableError> {
    // Staleness probe before the expensive path: commit files are immutable
    // and versions are contiguous, so the snapshot is current iff
    // `{version+1}.json` doesn't exist — one GET/404 instead of the
    // `_delta_log` LIST that `update_state()` always pays (LISTs bypass the
    // Foyer cache; this was the residual per-query S3 metadata traffic).
    // A probe hit also warms the cache for the commit read below. On probe
    // *error* fall through to the full refresh — never skip on uncertainty.
    {
        let guard = table.read().await;
        if let Some(v) = guard.version() {
            let log_store = guard.log_store();
            drop(guard);
            if matches!(log_store.read_commit_entry(v + 1).await, Ok(None)) {
                return Ok(Some(v));
            }
        }
    }
    let mut fresh = table.read().await.clone();
    // Fast path (gated by the caller's `incremental` flag, i.e. self.config):
    // carry the materialized file list forward over the catch-up range —
    // appending the new files and dropping the tombstoned ones (compaction /
    // replace_where) — instead of re-collecting the whole active set, the
    // O(active files) re-materialize `update_state` pays (2-8s on the 26k-file
    // unified table). Falls back to the full update when not applicable (gap
    // too large, not materialized, unreadable commit). The fallback path is
    // slightly *more* expensive than a bare update_state — it pays
    // advance_catchup's probe (one get_latest_version + cached commit GETs)
    // before the full update_state's uncached `_delta_log` LIST + re-materialize
    // — but the common case (in-gap, materialized) is always the win now that
    // removes no longer force the fallback.
    let advanced = if incremental {
        let log_store = fresh.log_store();
        match fresh.state.as_mut() {
            Some(state) => match state.advance_catchup(log_store.as_ref(), REFRESH_APPEND_CATCHUP_MAX_GAP).await {
                Ok(advanced) => advanced,
                // Non-fatal: the full update_state below re-attempts the same IO
                // and surfaces any persistent error; log so a table silently
                // never taking the fast path is at least visible.
                Err(e) => {
                    debug!("incremental catch-up failed, falling back to full update_state: {e}");
                    false
                }
            },
            None => false,
        }
    } else {
        false
    };
    if !advanced {
        fresh.update_state().await?;
    }
    let fresh_version = fresh.version();
    let mut guard = table.write().await;
    // Option<u64> ordering: None < Some(_), so an unloaded handle always swaps.
    if fresh_version > guard.version() {
        *guard = fresh;
    }
    Ok(guard.version())
}

/// Reconcile table properties existing tables predate, idempotently (no
/// commit when already set) and best-effort — a failed property commit must
/// never block table load. Currently retrofits:
/// - `delta.deletedFileRetentionDuration`: prod tables sat at delta's 7-day
///   default; the unified checkpoint carried 38.5k Remove tombstones (93% of
///   its 41.8k actions, 23.6MB) that every snapshot load and refresh replayed.
/// - `delta.checkpointInterval`: pre-existing tables sat at delta's default
///   of 100, so boot replay walked up to 100 commit JSONs past the
///   checkpoint; new tables get the configured interval at creation.
pub(crate) async fn ensure_table_properties(table: DeltaTable, desired: HashMap<String, String>) -> DeltaTable {
    let current = table.snapshot().ok().map(|s| s.metadata().configuration().clone()).unwrap_or_default();
    if desired.iter().all(|(k, v)| current.get(k) == Some(v)) {
        return table;
    }
    match table.clone().set_tbl_properties().with_properties(desired.clone()).await {
        Ok(updated) => {
            info!("Reconciled table properties {desired:?}");
            updated
        }
        Err(e) => {
            warn!("Failed to set table properties {desired:?}: {e}; table keeps its current settings");
            table
        }
    }
}

/// Whether `uri` belongs to a partition no older than `cutoff` (inclusive).
/// Parses the `date=YYYY-MM-DD` Hive partition segment; if absent or
/// unparseable, returns `true` (warm rather than silently skip a file we can't
/// classify). A `None` cutoff means "no recency limit".
fn within_recency(uri: &str, cutoff: Option<chrono::NaiveDate>) -> bool {
    // Single source of truth for `date=` partition recency parsing, shared with
    // the object-store cache admission window.
    crate::object_store_cache::date_partition_within(uri, cutoff)
}

/// Whether `uri`'s `date=YYYY-MM-DD` Hive partition overlaps the `[lo, hi]`
/// microsecond window, at day granularity. Absent/unparseable date ⇒ `true`
/// (conservative: treat as in-window so the coverage gate still demands an
/// index for it). Open bounds (`i64::MIN`/`MAX`) match everything on that side.
fn uri_date_in_window(uri: &str, lo: i64, hi: i64) -> bool {
    let Some(d) = crate::object_store_cache::date_partition_of(uri) else {
        return true;
    };
    let to_date = |ts: i64, open: i64| (ts != open).then(|| chrono::DateTime::from_timestamp_micros(ts)).flatten().map(|dt| dt.date_naive());
    to_date(lo, i64::MIN).is_none_or(|l| d >= l) && to_date(hi, i64::MAX).is_none_or(|h| d <= h)
}

/// The cache-key prefix for a table: its URI minus any `?endpoint=...` query
/// string (`table_url()` may carry one; `get_file_uris()` omits it) and trailing
/// slash. File URIs are relativized against this to form cache keys.
fn table_cache_prefix(table_uri: &str) -> &str {
    table_uri.split('?').next().unwrap_or(table_uri).trim_end_matches('/')
}

/// Relativize an absolute file URI against a `table_cache_prefix`, yielding the
/// bucket-relative path the cached object store keys full files by. `None` on
/// prefix mismatch (trailing-slash or query-string drift between `table_url()`
/// and `get_file_uris()`). Shared by the warm and evict paths so a single-char
/// difference can't desync which key was warmed vs. evicted.
fn relativize_to_prefix(prefix: &str, uri: &str) -> Option<object_store::path::Path> {
    uri.strip_prefix(prefix).map(|rel| object_store::path::Path::from(rel.trim_start_matches('/')))
}

/// The table's path WITHIN its bucket (`"s3://bucket/tf/tbl"` → `"tf/tbl"`) —
/// the namespace the cache keys full files under. Cache inserts happen BELOW
/// delta-rs' `PrefixStore`, so keys are bucket-relative, while
/// `relativize_to_prefix` output is table-relative; joining the two yields the
/// key inserts actually used. Prod 2026-08-03: evict-after-compaction and the
/// flush-confirm `contains_data` probe both addressed table-relative keys, so
/// evictions were silent no-ops (47GB of unreachable tombstoned bodies) and
/// every confirm re-downloaded bytes it had just uploaded.
fn table_path_in_bucket(prefix: &str) -> &str {
    prefix.splitn(4, '/').nth(3).unwrap_or("").trim_matches('/')
}

/// Bucket-relative cache key for a table-relative path (see
/// [`table_path_in_bucket`]).
fn bucket_cache_key(table_path: &str, rel: &object_store::path::Path) -> String {
    match table_path.is_empty() {
        true => rel.as_ref().to_string(),
        false => format!("{table_path}/{rel}"),
    }
}

/// Select and order the files `warm_cache_for_uris` will warm. Returns
/// `(path, recent)` pairs: footers warm for every returned file; full-file
/// warming additionally requires `recent`. With `warm_all_footers` (default)
/// non-recent files are kept (recent=false → footer-only); without it they
/// are dropped entirely. Ordered NEWEST date-partition first: dashboards
/// query recent partitions, and prod showed a boot-time warm can be cut
/// short (slow object store, restart) — oldest-first left exactly those
/// partitions cold. The old LRU argument for oldest-first only matters when
/// the warm set exceeds the metadata cache (3k footers ≈ 200MB vs 5GB disk —
/// nowhere close). Undated files sort last. Returns the count of URIs that
/// failed to relativize for the caller to log.
fn select_warm_paths(
    uris: Vec<String>, prefix: &str, warm_all_footers: bool, cutoff: Option<chrono::NaiveDate>,
) -> (Vec<(object_store::path::Path, bool)>, usize) {
    let mut dropped = 0usize;
    let mut paths: Vec<(object_store::path::Path, bool)> = uris
        .into_iter()
        .filter(|u| u.ends_with(".parquet"))
        .map(|u| {
            let recent = within_recency(&u, cutoff);
            (u, recent)
        })
        .filter(|(_, recent)| warm_all_footers || *recent)
        .filter_map(|(u, recent)| match relativize_to_prefix(prefix, &u) {
            Some(path) => Some((path, recent)),
            None => {
                // Prefix mismatch (e.g. trailing-slash or query-string drift
                // between table_url() and get_file_uris()). Warming this file
                // would address the wrong key, so skip it.
                dropped += 1;
                None
            }
        })
        .collect();
    // Assumes 10-char ISO dates (date=YYYY-MM-DD, lexically sortable). A
    // missing or differently-shaped date= segment keys as "" — sorts last
    // under Reverse (treated as oldest), never a crash.
    let date_key = |p: &object_store::path::Path| {
        let s = p.as_ref();
        s.find("date=").and_then(|i| s.get(i + 5..i + 15)).unwrap_or("").to_string()
    };
    // cached_key: one allocation per path, not one per comparison.
    paths.sort_by_cached_key(|(p, _)| std::cmp::Reverse(date_key(p)));
    (paths, dropped)
}

// Helper function to extract project_id from a batch
pub fn extract_project_id(batch: &RecordBatch) -> Option<String> {
    use datafusion::arrow::array::{StringArray, StringViewArray};

    let idx = batch.schema().fields().iter().position(|f| f.name() == "project_id")?;
    let column = batch.column(idx);
    // Utf8View first (our preferred type), then fall back to Utf8.
    column
        .as_any()
        .downcast_ref::<StringViewArray>()
        .and_then(|arr| arr.iter().next().flatten())
        .or_else(|| column.as_any().downcast_ref::<StringArray>().and_then(|arr| arr.iter().next().flatten()))
        .map(str::to_string)
}

/// Split a batch row-wise by its `project_id` column into per-project sub-batches.
///
/// A single multi-row INSERT (or queued batch) may carry rows for several
/// projects. TimeFusion stores each project in its own Delta table, so routing
/// must follow each row's own `project_id` — reading only row 0 (as a plain
/// [`extract_project_id`] does) silently misroutes every other row into row 0's
/// table. Rows with a null/absent `project_id` fall back to `default_project`.
/// A homogeneous batch is returned as-is (no copy); mixed batches are split with
/// `take`. Groups are keyed in sorted order for deterministic table writes.
pub fn partition_batch_by_project(batch: RecordBatch, default_project: &str) -> DFResult<Vec<(String, RecordBatch)>> {
    use std::collections::BTreeMap;

    use datafusion::arrow::{
        array::{StringArray, StringViewArray, UInt32Array},
        compute::take_record_batch,
    };

    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(vec![]);
    }
    let Some(col_idx) = batch.schema().fields().iter().position(|f| f.name() == "project_id") else {
        return Ok(vec![(default_project.to_string(), batch)]);
    };
    let column = batch.column(col_idx);

    // Group row indices by project. Imperative `get_mut`-or-`insert` (not a fold)
    // so the owned key String is allocated once per distinct project, not per row.
    // The block scopes the boxed iterator's borrow of `batch` so `batch` can move below.
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    {
        let rows: Box<dyn Iterator<Item = Option<&str>> + '_> =
            match (column.as_any().downcast_ref::<StringViewArray>(), column.as_any().downcast_ref::<StringArray>()) {
                (Some(arr), _) => Box::new(arr.iter()),
                (_, Some(arr)) => Box::new(arr.iter()),
                _ => return Ok(vec![(default_project.to_string(), batch)]),
            };
        for (i, pid) in rows.enumerate() {
            let pid = pid.unwrap_or(default_project);
            match groups.get_mut(pid) {
                Some(v) => v.push(i as u32),
                None => drop(groups.insert(pid.to_string(), vec![i as u32])),
            }
        }
    }

    // Homogeneous batch: route the whole thing, skip the take/copy.
    if groups.len() == 1 {
        let pid = groups.into_keys().next().unwrap();
        return Ok(vec![(pid, batch)]);
    }

    groups.into_iter().map(|(pid, indices)| Ok((pid, take_record_batch(&batch, &UInt32Array::from(indices))?))).collect()
}

/// Build a minimal `SessionState` for delta-rs `OptimizeBuilder` to use.
///
/// delta-rs's default `DeltaSessionConfig` turns `schema_force_view_types`
/// ON, which makes the optimize-internal Parquet reader cast our Variant
/// columns' Binary buffers to BinaryView at read time. The kernel's
/// `unshredded_variant()` schema then mismatches and the rewrite errors
/// out ("Expected ... Binary, got ... BinaryView"). Passing this session
/// via `.with_session_state(...)` overrides the default and keeps the
/// read schema as declared.
fn build_optimize_session_state(
    target_partitions: usize, runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    // batch_size 2048 (was 8192): merge memory ≈ fan-in × batch, and otel rows are
    // wide — 8192-row decode batches measured up to 145MB (2026-07-27). Quartering
    // the batch bounds a 32-file bin merge near ~1GB. Bare `SessionConfig::new()`
    // otherwise keeps DataFusion's larger default. Under the maintenance-CLI
    // budget profile the batch drops to 256 rows — a batch is the sort's
    // indivisible admission unit, and small-cgroup pools must be able to admit
    // one before spilling can engage.
    let batch_size = crate::config::try_config().map_or("2048", |c| c.derived.maintenance_batch_size());
    let cfg = maintenance_session_config(SessionConfig::new(), batch_size, target_partitions);
    // `runtime_env` is the dedicated bounded maintenance pool (see
    // `maintenance_runtime_env`): allocations still fail as errors rather than
    // OOM-killing the process, but are isolated from query pressure and can spill.
    SessionStateBuilder::new().with_config(cfg).with_runtime_env(runtime_env).with_default_features().build()
}

/// Parallelism cap for every maintenance session. Each partition's
/// `ExternalSorter` reserves `sort_spill_reservation_bytes` up-front from the
/// bounded maintenance pool, so the query-derived count (≈ CPU cores) exhausts it
/// before the sort can even start (prod 2026-07-12: ~46 sorters × 64 MB > the
/// 4.8 GB pool). Legacy and high-file-count partitions still blew the reservation
/// at 4 partitions, hence 2.
const MAINTENANCE_MAX_PARTITIONS: usize = 2;

/// Config tuning shared by every delta-rs maintenance session.
/// `schema_force_view_types=false` keeps Variant columns as `Binary` (not
/// `BinaryView`) so delta_kernel's unshredded-variant schema check passes; the
/// sort-spill floor lets a sort spill instead of erroring under the bounded
/// maintenance pool; `skip_physical_aggregate_schema_check` mirrors
/// `create_session_context` (2026-07-31, 7d68f01) because maintenance reads the
/// very files carrying the widened nullability.
fn maintenance_session_config(base: datafusion::prelude::SessionConfig, batch_size: &str, target_partitions: usize) -> datafusion::prelude::SessionConfig {
    let mut cfg = base.set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    for (k, v) in [
        ("datafusion.execution.batch_size", batch_size),
        ("datafusion.execution.sort_spill_reservation_bytes", "33554432"),
        ("datafusion.execution.skip_physical_aggregate_schema_check", "true"),
    ] {
        let _ = cfg.options_mut().set(k, v);
    }
    let parts = if target_partitions == 0 { MAINTENANCE_MAX_PARTITIONS } else { target_partitions.min(MAINTENANCE_MAX_PARTITIONS) };
    cfg.with_target_partitions(parts)
}

/// Session for delta-rs *write* execution (recompress's `replace_where`
/// overwrite). Like `build_optimize_session_state` but built on
/// `DeltaSessionConfig` and carrying delta-rs's `DeltaPlanner` — the write path
/// wraps its input in a `MetricObserver` custom node that only that planner can
/// convert to a physical plan. `schema_force_view_types=false` keeps Variant
/// columns as `Binary` (not `BinaryView`) so delta_kernel's unshredded-variant
/// schema check passes (same reason as `dml.rs::delta_session_from`).
fn build_delta_write_session_state(
    target_partitions: usize, runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    let base: SessionConfig = deltalake::delta_datafusion::DeltaSessionConfig::default().into();
    let cfg = maintenance_session_config(base, "8192", target_partitions);
    SessionStateBuilder::new()
        .with_config(cfg)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .with_query_planner(deltalake::delta_datafusion::planner::DeltaPlanner::new())
        .build()
}

/// Spawn a background task that runs `job` at each wall-clock occurrence of the
/// cron `schedule` (croner, 6-field with seconds, UTC). Fire times are computed
/// from the system clock via `find_next_occurrence`, so they are predictable and
/// independent of process start time — the same job fires at e.g. :00/:30 on
/// every replica regardless of when it booted. Exits when `cancel` fires.
/// Each fire runs on its own detached task; if it is still running at the next
/// tick the tick is skipped (counted in `maintenance.cron_ticks_skipped`). Slow
/// but healthy runs are never aborted just because later ticks occur — only
/// shutdown forces an in-flight abort. A long-running warning threshold is
/// logged/metriced when a run outlives several ticks, so wedged I/O is visible
/// without being killed pre-emptively.
///
/// Replaces tokio-cron-scheduler, which silently stopped dispatching ticks in
/// prod (2026-07-13: 0 optimize/checkpoint runs over 14h of uptime despite the
/// jobs being scheduled at boot). Driving the loop ourselves keeps it debuggable.
fn spawn_cron_job<F, Fut>(name: &'static str, schedule: &str, cancel: Arc<CancellationToken>, job: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    if schedule.trim().is_empty() {
        info!("{name} job scheduling skipped - empty schedule");
        return;
    }
    let cron: croner::Cron = match schedule.parse() {
        Ok(c) => c,
        Err(e) => {
            error!("{name} job disabled - invalid cron '{schedule}': {e}");
            return;
        }
    };
    // Log a warning once a run has been in flight this long. This is purely
    // observability — slow-but-progressing work is allowed to finish.
    const LONG_RUNNING_WARN_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(600);
    info!("{name} job scheduled with cron expression: {schedule}");
    tokio::spawn(async move {
        let mut running: Option<tokio::task::JoinHandle<()>> = None;
        let mut running_since: Option<std::time::Instant> = None;
        let mut skips = 0u32;
        loop {
            let now = chrono::Utc::now();
            let dur = match cron.find_next_occurrence(&now, false) {
                // Strictly-future (inclusive=false) next fire, so `dur` is always > 0.
                Ok(next) => (next - now).to_std().unwrap_or(std::time::Duration::from_secs(1)),
                Err(e) => {
                    error!("{name} job stopped - no next occurrence: {e}");
                    return;
                }
            };
            tokio::select! {
                _ = cancel.cancelled() => {
                    // Don't let an in-flight run race the shutdown flush.
                    if let Some(h) = running {
                        h.abort();
                    }
                    info!("{name} job stopped (shutdown)");
                    return;
                }
                _ = tokio::time::sleep(dur) => {
                    // Fire on a detached task so a wedged/overlong run can never
                    // freeze this loop; overlapping runs are skipped instead of
                    // piled up (maintenance jobs are periodic + idempotent).
                    match running.as_ref() {
                        Some(h) if !h.is_finished() => {
                            skips += 1;
                            crate::metrics::maintenance_stats().cron_ticks_skipped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if running_since.is_some_and(|s| s.elapsed() >= LONG_RUNNING_WARN_THRESHOLD) {
                                warn!("{name} job run still in progress after {:?} — may be wedged or just slow (skips={skips})", LONG_RUNNING_WARN_THRESHOLD);
                                crate::metrics::record_cron_long_running();
                            } else {
                                warn!("{name} job tick skipped: previous run still in progress ({skips} consecutive)");
                            }
                            continue;
                        }
                        Some(_) => {
                            // Previous run finished between ticks — reset skip
                            // count and overwrite the handle below.
                            skips = 0;
                        }
                        None => {}
                    }
                    crate::metrics::maintenance_stats().cron_ticks_fired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    running_since = Some(std::time::Instant::now());
                    running = Some(tokio::spawn(job()));
                }
            }
        }
    });
}

/// On-disk key for the WAL watermark stored in `commitInfo.info`. Constant so
/// the writer (this file) and reader (`derive_wal_cursor_for_table`) can't
/// drift, and the roundtrip test below pins the format.
const WAL_WATERMARK_KEY: &str = "timefusion.wal_watermark";

/// Serialize a per-shard watermark to the JSON map shape we store in
/// `commitInfo.info[WAL_WATERMARK_KEY]`. Only shards with a position are
/// included — absent shards mean "no constraint from this commit", which is
/// how the per-shard MAX aggregation across commits ignores them.
fn serialize_watermark_to_json(
    watermark: &crate::buffered_write_layer::DeltaWatermark, project_id: &str, table_name: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut map: serde_json::Map<String, serde_json::Value> = watermark
        .iter()
        .enumerate()
        .filter_map(|(shard, pos)| pos.map(|p| (shard.to_string(), serde_json::json!({ "block_id": p.block_id, "offset": p.offset }))))
        .collect();
    if !map.is_empty() {
        // Topic scope. Unified-table tenants all commit to ONE Delta log, and
        // walrus positions are per-topic, so an unscoped watermark read back by
        // a different tenant advances its cursor past unreplayed entries.
        // Non-numeric keys are already skipped by older readers, so adding this
        // is backward-compatible on the read side.
        map.insert(WATERMARK_TOPIC_KEY.to_string(), serde_json::Value::String(wal_topic(project_id, table_name)));
    }
    map
}

/// Key inside the watermark object naming the topic that produced it.
const WATERMARK_TOPIC_KEY: &str = "topic";

/// Key inside the watermark object holding the MULTI-topic map written by a
/// cross-project coalesced commit: `{ "<project>:<table>": <single-topic map> }`.
/// Only present when one commit carries more than one topic's watermark — a
/// single-topic commit keeps the flat legacy shape byte-for-byte, so nothing
/// changes on the non-coalesced path and an old binary reading those commits
/// behaves exactly as before. An old binary reading a MULTI commit finds no
/// top-level `topic` key, so it contributes nothing (under-advance = replay +
/// dedup, never loss) — the safe rollback direction.
const WATERMARK_TOPICS_KEY: &str = "topics";

/// Serialize the watermarks of every (project, table) carried by ONE commit.
/// Topics whose watermark has no positions are dropped (same rule as the
/// single-topic form). Exactly one surviving topic ⇒ flat legacy shape.
fn serialize_watermarks_to_json(
    entries: impl IntoIterator<Item = (String, String, crate::buffered_write_layer::DeltaWatermark)>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut per_topic: Vec<(String, serde_json::Map<String, serde_json::Value>)> = entries
        .into_iter()
        .filter_map(|(project_id, table_name, wm)| {
            let map = serialize_watermark_to_json(&wm, &project_id, &table_name);
            (!map.is_empty()).then(|| (wal_topic(&project_id, &table_name), map))
        })
        .collect();
    if per_topic.len() <= 1 {
        return per_topic.pop().map(|(_, map)| map).unwrap_or_default();
    }
    // Dedup defensively: two units for the same topic in one commit would
    // otherwise silently drop one. Take the per-shard MAX so the surviving
    // entry can never be BEHIND either contributor (behind = over-replay,
    // which is safe, but ahead would be loss).
    let topics = per_topic.into_iter().fold(serde_json::Map::new(), |mut topics, (topic, map)| {
        match topics.get_mut(&topic).and_then(serde_json::Value::as_object_mut) {
            Some(existing) => merge_max_watermark_maps(existing, map),
            None => drop(topics.insert(topic, serde_json::Value::Object(map))),
        }
        topics
    });
    [(WATERMARK_TOPICS_KEY.to_string(), serde_json::Value::Object(topics))].into_iter().collect()
}

/// Per-shard MAX merge of two single-topic watermark maps, in place on `into`.
fn merge_max_watermark_maps(into: &mut serde_json::Map<String, serde_json::Value>, from: serde_json::Map<String, serde_json::Value>) {
    let pos = |v: &serde_json::Value| {
        (v.get("block_id").and_then(serde_json::Value::as_u64).unwrap_or(0), v.get("offset").and_then(serde_json::Value::as_u64).unwrap_or(0))
    };
    for (shard, value) in from {
        if shard.parse::<usize>().is_err() {
            continue; // the "topic" key — already present and identical
        }
        match into.get(&shard) {
            Some(existing) if pos(existing) >= pos(&value) => {}
            _ => {
                into.insert(shard, value);
            }
        }
    }
}

/// Delete `datafusion-*` spill directories left behind by a previous process.
///
/// DataFusion's `DiskManager` removes its temp dirs on `Drop`, which a SIGKILL
/// skips — so every OOM kill leaks that run's spill files. Prod had 205 orphans
/// dating back 15 days: 133 GB in `maintenance_spill` plus 53 GB in
/// `light_optimize_spill`, on the same volume as the WAL. A full disk means WAL
/// appends fail, which is an outage plus write loss, so this is a durability
/// concern rather than housekeeping.
///
/// Safe despite the runtime env being built lazily (`get_or_init` on the first
/// maintenance job, not at startup): the orphan list is snapshotted *inline*,
/// before this env's DiskManager exists, so nothing this process wrote can be
/// in it. Everything in the snapshot belongs to a dead process — the WAL dir
/// flock guarantees no second live TimeFusion.
///
/// Deletion runs on a detached thread: prod's backlog was 186 GB across 205
/// dirs, and walking plus unlinking that inline would stall the maintenance
/// job that happened to trigger initialization (and any caller blocked on the
/// same `OnceCell`). Reclaiming a dead process's garbage is never urgent.
fn reap_orphaned_spill_dirs(spill_dir: &std::path::Path) {
    // Snapshot the orphan list synchronously, BEFORE the caller builds this
    // env's DiskManager. `read_dir` streams lazily, so enumerating on the
    // detached thread raced dirs the *new* DiskManager creates: with a large
    // orphan backlog the slow size-walk was still iterating when the first
    // sort's live spill dir appeared, and `remove_dir_all` yanked it mid-sort
    // (prod 2026-07-29: light optimize failed with ENOENT on its own spill
    // file). Only the pre-existing snapshot is deleted off-thread.
    let orphans: Vec<std::path::PathBuf> = std::fs::read_dir(spill_dir)
        .map(|entries| {
            entries
                .flatten()
                // DiskManager names them `datafusion-XXXXXX`; touch nothing else.
                .filter(|e| e.file_name().to_string_lossy().starts_with("datafusion-"))
                .map(|e| e.path())
                .collect()
        })
        .unwrap_or_default();
    if orphans.is_empty() {
        return;
    }
    let dir = spill_dir.to_path_buf();
    std::thread::Builder::new()
        .name("spill-reap".into())
        .spawn(move || reap_orphaned_spill_dirs_blocking(&dir, orphans))
        .map_or_else(|e| warn!("spill reap: cannot spawn reaper for {spill_dir:?}: {e}"), |_| ());
}

fn reap_orphaned_spill_dirs_blocking(spill_dir: &std::path::Path, orphans: Vec<std::path::PathBuf>) {
    let (dirs, bytes) = orphans.into_iter().fold((0u64, 0u64), |(dirs, bytes), path| {
        let size = dir_size_bytes(&path);
        match std::fs::remove_dir_all(&path) {
            Ok(()) => (dirs + 1, bytes + size),
            Err(err) => {
                warn!("spill reap: cannot remove {path:?}: {err}");
                (dirs, bytes)
            }
        }
    });
    if dirs > 0 {
        info!("spill reap: removed {dirs} orphaned spill dir(s), {} MB freed from {spill_dir:?}", bytes / (1024 * 1024));
    }
}

/// Recursive byte total, best-effort (unreadable entries count as 0).
fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else { return 0 };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_size_bytes(&e.path()),
            _ => e.metadata().map_or(0, |m| m.len()),
        })
        .sum()
}

/// Logical WAL topic for a (project, table) — matches `wal.rs`'s topic naming.
fn wal_topic(project_id: &str, table_name: &str) -> String {
    format!("{project_id}:{table_name}")
}

/// Inverse of `serialize_watermark_to_json`. Out-of-range or malformed shards
/// are dropped silently — schema-evolution-friendly: future writers can add
/// fields without breaking older readers.
fn parse_watermark_from_json(
    info: &std::collections::HashMap<String, serde_json::Value>, shards: usize, project_id: &str, table_name: &str,
) -> Vec<Option<walrus_rust::WalPosition>> {
    let mut out = vec![None; shards];
    let Some(wm) = info.get(WAL_WATERMARK_KEY).and_then(|v| v.as_object()) else {
        return out;
    };
    let topic = wal_topic(project_id, table_name);
    // A cross-project coalesced commit nests one map per topic. Selecting by
    // key IS the topic scoping — a project only ever reads its own entry, so
    // the crash-recovery resume position stays per-project exactly as it is on
    // the one-commit-per-project path. An absent topic contributes nothing.
    let wm = match wm.get(WATERMARK_TOPICS_KEY).and_then(|v| v.as_object()) {
        Some(topics) => {
            let Some(mine) = topics.get(&topic).and_then(|v| v.as_object()) else { return out };
            mine
        }
        // Only apply a watermark to the topic that wrote it. A commit from another
        // tenant of the same unified table, or a legacy commit with no topic at
        // all, contributes nothing — an unattributable position is indistinguishable
        // from another tenant's, and over-advancing a cursor loses acked writes,
        // whereas under-advancing only replays duplicates (read-side dedup drops
        // those).
        None if wm.get(WATERMARK_TOPIC_KEY).and_then(|v| v.as_str()) == Some(topic.as_str()) => wm,
        None => return out,
    };
    for (shard_str, pos_val) in wm {
        let Ok(shard) = shard_str.parse::<usize>() else { continue };
        if shard >= shards {
            continue;
        }
        let block_id = pos_val.get("block_id").and_then(|v| v.as_u64()).unwrap_or(0);
        let offset = pos_val.get("offset").and_then(|v| v.as_u64()).unwrap_or(0);
        out[shard] = Some(walrus_rust::WalPosition { block_id, offset });
    }
    out
}

/// Take the per-shard MAX position across a sequence of commit-info maps.
/// `None` for a shard means no commit observed had a position for it.
/// Used during startup to compute the cursor each shard should sit at to
/// be consistent with all recent Delta commits.
fn max_watermark_across_commits<'a>(
    commit_infos: impl IntoIterator<Item = &'a std::collections::HashMap<String, serde_json::Value>>, shards: usize, project_id: &str, table_name: &str,
) -> Vec<Option<walrus_rust::WalPosition>> {
    commit_infos.into_iter().fold(vec![None; shards], |acc, info| {
        acc.into_iter()
            .zip(parse_watermark_from_json(info, shards, project_id, table_name))
            .map(|(prev, candidate)| match (prev, candidate) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            })
            .collect()
    })
}

/// Base [`CommitProperties`] for every ingest/maintenance commit. Disables the
/// delta-rs post-commit checkpoint + expired-log-cleanup hooks: those run AFTER
/// `N.json` is durably written, but a hook failure (R2 500 on the checkpoint
/// PUT or the bulk `?delete`) is surfaced as a commit error — which the flush/
/// dedup error arms misread as "commit never landed" and then delete the parquet
/// the landed commit references (2026-07-09 incident: 14 dangling Adds). Both
/// hooks now run out-of-band in the maintenance scheduler, tolerant of R2 500s.
/// `Some(false)` overrides the `enableExpiredLogCleanup` table property per-commit.
fn base_commit_properties() -> CommitProperties {
    CommitProperties::default().with_create_checkpoint(false).with_cleanup_expired_logs(Some(false))
}

/// Build [`CommitProperties`] carrying the watermark under [`WAL_WATERMARK_KEY`].
/// Empty when the watermark has no positions (e.g. WAL-replay-derived buckets);
/// delta-rs writes the commit without the key in that case, and recovery
/// silently skips that commit.
/// Takes every (project, table, watermark) the commit carries — one entry on
/// the per-project path, N on a coalesced cross-project commit.
fn build_watermark_commit_properties(watermarks: impl IntoIterator<Item = (String, String, crate::buffered_write_layer::DeltaWatermark)>) -> CommitProperties {
    let entries = serialize_watermarks_to_json(watermarks);
    if entries.is_empty() {
        return base_commit_properties();
    }
    base_commit_properties().with_metadata([(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(entries))])
}

/// `CommitProperties` for a compaction/dedup commit (Add + Remove): when
/// `enabled`, the post-commit hook advances the materialized snapshot
/// incrementally instead of re-materializing every active file. `false` is the
/// plain full-update behaviour.
fn incremental_commit_properties(enabled: bool) -> CommitProperties {
    base_commit_properties().with_incremental_advance(enabled)
}

/// Active-file URIs of `table`, restricted to files whose log path contains
/// every marker in `scope` (`partition=value` path segments). Equivalent to
/// `get_file_uris()` + a filter, except the predicate runs on the *borrowed* log
/// path: a skipped file costs no allocation, where `get_file_uris()` allocates a
/// `Path` **and** a URI `String` for every active file before you can filter. On
/// the 26k-file unified table that turns a single-partition walk from ~52k
/// allocations into a few hundred. Empty `scope` = whole table. An unloaded
/// snapshot yields an empty set (matching the `unwrap_or_default()` call sites).
fn scoped_file_uris(table: &DeltaTable, scope: &[&str]) -> Vec<String> {
    let Ok(state) = table.snapshot() else { return Vec::new() };
    let log_store = table.log_store();
    state
        .log_data()
        .into_iter()
        .filter_map(|f| {
            let path = f.path();
            // Mirrors the fork's `object_store_path()`: prefer the percent-encoding-
            // preserving parse, fall back to the lossy `from` exactly as it does, so
            // the URIs produced here are byte-identical to `get_file_uris()`.
            scope.iter().all(|m| path.contains(m)).then(|| {
                let p = object_store::path::Path::parse(path.as_ref()).unwrap_or_else(|_| object_store::path::Path::from(path.as_ref()));
                log_store.to_uri(&p)
            })
        })
        .collect()
}

/// True for the retryable Delta OCC conflicts — a single retry on a refreshed
/// snapshot resolves them. Shared by the flush, dedup, and light-optimize commit
/// loops so they classify identically. Substrings match the real delta-rs
/// Display strings: VersionAlreadyExists ("... version N already exists."), the
/// conflict_checker variants ("Commit failed: a concurrent transaction ..."),
/// MetadataChanged ("Metadata changed since last commit."), and the predicate
/// re-evaluation failure ("Transaction failed ..."). Deliberately NOT a bare
/// "version" — that also matches the permanent Unsupported{Reader,Writer}Version
/// errors, which must fail fast.
pub(crate) fn is_occ_conflict_err(msg: &str) -> bool {
    msg.contains("already exists")
        || msg.contains("Commit failed")
        || msg.contains("concurrent transaction")
        || msg.contains("Metadata changed")
        || msg.contains("Transaction failed")
}

/// A Parquet file ends with `[footer_len: u32 LE][PAR1]`. Cheap structural
/// check that catches the real-world checkpoint-corruption classes — an object
/// overwritten with foreign bytes (an S3 XML error / SelectObjectContent body,
/// 2026-07-17) or a truncated write — without reading the whole file. `tail`
/// is the file's last 8 bytes.
fn parquet_tail_ok(tail: &[u8], file_len: u64) -> bool {
    tail.len() == 8 && &tail[4..] == b"PAR1" && {
        let footer_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
        footer_len > 0 && footer_len + 8 <= file_len
    }
}

/// Verify the checkpoint that `_last_checkpoint` points to is a readable
/// Parquet before TF trusts it enough to prune the JSON commit log behind it.
/// `Ok(true)` = every part has a sane footer; `Ok(false)` = at least one part
/// is definitively corrupt/foreign; `Err` = couldn't determine (missing part /
/// transient store error). A missing `_last_checkpoint` returns `Ok(true)` —
/// delta lists the log in that case, so it is not our gate to hold.
async fn last_checkpoint_readable(store: &Arc<dyn object_store::ObjectStore>) -> Result<bool, object_store::Error> {
    use object_store::{GetOptions, GetRange, ObjectStore, path::Path};
    let lc = match store.get_opts(&Path::from("_delta_log/_last_checkpoint"), GetOptions::default()).await {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => return Ok(true),
        Err(e) => return Err(e),
    };
    let meta: serde_json::Value = serde_json::from_slice(&lc).map_err(|e| object_store::Error::Generic { store: "checkpoint_verify", source: Box::new(e) })?;
    let Some(version) = meta.get("version").and_then(serde_json::Value::as_u64) else { return Ok(false) };
    let parts = meta.get("parts").and_then(serde_json::Value::as_u64).unwrap_or(1);
    let paths: Vec<Path> = if parts <= 1 {
        vec![Path::from(format!("_delta_log/{version:020}.checkpoint.parquet"))]
    } else {
        (1..=parts).map(|p| Path::from(format!("_delta_log/{version:020}.checkpoint.{p:010}.{parts:010}.parquet"))).collect()
    };
    for p in &paths {
        let res = store.get_opts(p, GetOptions { range: Some(GetRange::Suffix(8)), ..Default::default() }).await?;
        let size = res.meta.size;
        if !parquet_tail_ok(&res.bytes().await?, size) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Exponential backoff between OCC conflict retries — single policy for every
/// retry site (flush, optimize, dedup, DML): 150, 300, 600ms… capped so the
/// shift can't overflow if a caller raises its attempt limit.
pub(crate) fn occ_backoff(attempt: usize) -> tokio::time::Duration {
    tokio::time::Duration::from_millis(150 << attempt.min(6))
}

/// True for transient S3/network transport failures worth retrying the whole
/// operation on. object_store retries individual requests (max_retries/180s),
/// but a multipart part whose connection drops mid-body (R2 under concurrent
/// large PUTs) can bubble up as terminal — aborting a compaction merge that
/// committed nothing. delta-rs wraps these as "Failed to parse parquet" via the
/// async parquet writer, so we match the transport phrases, not the wrapper.
/// Deliberately excludes auth/permanent errors (403, NoSuchBucket) which must
/// fail fast. Phrases are anchored to genuinely-transient transport states —
/// notably NOT a bare "connection", which matches the permanent "connection
/// refused" (misconfigured endpoint / firewall) and would burn the whole retry
/// budget + backoff before failing.
/// TODO: object_store exposes typed variants (Error::Generic, retryable flags)
/// under DeltaTableError::ObjectStore; downcasting the error chain would be
/// version-stable vs. this string match. Revisit on the next object_store bump.
fn is_transient_s3_err(msg: &str) -> bool {
    msg.contains("error sending request")
        || msg.contains("connection reset")
        || msg.contains("connection closed")
        || msg.contains("broken pipe")
        || msg.contains("reset by peer")
        || msg.contains("timed out")
        || msg.contains("timeout")
}

/// Synthetic per-row source-file column exposed on the dedup sweep's table
/// provider (see `dedup_partition`): the targeted rewrite needs to know which
/// FILES hold the duplicate window's rows so it can commit exact Remove+Add
/// actions instead of a predicate-evaluated replace_where.
const DEDUP_FILE_COL: &str = "__tf_dedup_file";
const DEDUP_SCAN_NAME: &str = "__dedup_src";

/// Order-insensitive fingerprint of a partition's live file set (read-side
/// dedup skip): sorted-uris hash, so any add/remove/rewrite changes it.
fn partition_file_fp(mut files: Vec<String>) -> u64 {
    use std::hash::{Hash, Hasher};
    files.sort();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    files.hash(&mut h);
    h.finish()
}

/// UTC dates covered by a `[lo, hi]` microsecond window, or `None` when the
/// window is unbounded/invalid/wider than a year (bounds the per-date
/// fingerprint checks; such queries just keep DedupExec).
fn window_dates(lo: i64, hi: i64) -> Option<Vec<chrono::NaiveDate>> {
    let lo_d = chrono::DateTime::from_timestamp_micros(lo)?.date_naive();
    let hi_d = chrono::DateTime::from_timestamp_micros(hi)?.date_naive();
    let span = (hi_d - lo_d).num_days();
    if !(0..=366).contains(&span) {
        return None;
    }
    Some((0..=span).map(|d| lo_d + chrono::Duration::days(d)).collect())
}

/// A `Remove` tombstone for `add` with `data_change: true` (the dedup rewrite
/// drops rows, unlike optimize's data-preserving `data_change: false`).
/// Whether a commit that returned an error actually landed. delta-rs surfaces a
/// post-commit hook / snapshot-refresh failure as a commit `Err` even though
/// `N.json` is already durably written (2026-07-09: an R2 500 on the checkpoint
/// PUT / bulk log `?delete`). Deleting the staged parquet in that case orphans
/// the Adds the landed commit references, so the flush path must tell the cases
/// apart. See `Database::probe_commit_landed`.
enum CommitProbe {
    /// `N.json` landed; every staged Add is active. Treat as success + drain.
    Landed,
    /// Confirmed the commit did not land; the staged parquet is safe to delete.
    NotLanded,
    /// Could not confirm (snapshot refresh / read failed) — leak the staged
    /// parquet rather than risk deleting files a landed commit references.
    Inconclusive,
}

/// One (project, table) flush unit handed to [`Database::insert_records_batches_coalesced`].
pub struct CoalescedWriteUnit {
    pub project_id: String,
    pub table_name: String,
    pub batches: Vec<RecordBatch>,
    pub watermark: crate::buffered_write_layer::DeltaWatermark,
}

/// A unit whose parquet is uploaded and whose `Add` actions are waiting for the
/// shared commit.
struct StagedUnit {
    table_ref: Arc<RwLock<DeltaTable>>,
    schema: &'static crate::schema_loader::TableSchema,
    dirty_bins: Vec<(String, i64)>,
    adds: Vec<deltalake::kernel::Action>,
    stage_store: Arc<dyn object_store::ObjectStore>,
}

/// Marks a commit error where landing could NOT be confirmed. The staged
/// parquet must be left in place (deleting files a landed commit references
/// creates dangling Adds — the 2026-07-09 incident shape).
const INCONCLUSIVE_COMMIT_MARKER: &str = "landing-unconfirmed";

/// LAST-RESORT circuit breaker on a network await taken WHILE a per-table
/// commit lock is held. That lock serializes every committer for one physical
/// table (staged flush, coalesced flush, dedup waves, light-optimize waves), so
/// a request that never returns pins the whole table at zero commit throughput
/// (prod 2026-07-30 01:20–01:34: a dedup wave hung mid-commit and every other
/// committer queued behind it).
///
/// It is NOT the mechanism that bounds commits — that is the commit-log request
/// class (see `AwsConfig::log_request_timeout` and
/// [`crate::object_store_cache::RequestClassRouter`]), which bounds each log
/// request at 30s and the whole retry ladder at `RetryConfig::retry_timeout`
/// (180s). Bounding at the client is strictly better because a timeout there is
/// an ordinary commit error the landed-probe already classifies, while a
/// timeout HERE abandons a future mid-flight and manufactures an unconfirmed
/// landing (staged parquet must then be leaked rather than reclaimed).
///
/// So this fires only if a request escapes both client bounds — a bug or a
/// pathological hang. 600s matches `CHECKPOINT_OP_TIMEOUT`; every firing is
/// counted (`timefusion.commit.lock_timeouts`) and warned, and expected to be
/// permanently zero.
const COMMIT_LOCK_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// A bounded in-guard commit await that did not succeed.
struct CommitFailure {
    message: String,
    /// The await was ABANDONED mid-flight rather than returning an error. The
    /// commit may still have landed (the request is running on R2's side), and
    /// the store is proven slow — so no probe may be trusted to say "no". See
    /// [`probe_after_timeout`].
    timed_out: bool,
}

/// Run one commit-path future under `bound`, so a hung object-store request can
/// never pin a commit lock for the S3 client's 900s request timeout. A timeout
/// is reported as a failure whose landing is UNKNOWN, never as "did not
/// commit": callers must route it through the unconfirmed-landing path
/// (`probe_after_timeout` + `CommitProbe::Inconclusive`), which leaves staged
/// parquet in place and requeues the work.
async fn bounded_commit_await<T, E: std::fmt::Display>(
    bound: std::time::Duration, op: &'static str, table_name: &str, fut: impl std::future::IntoFuture<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, CommitFailure> {
    let started = std::time::Instant::now();
    match tokio::time::timeout(bound, fut.into_future()).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => Err(CommitFailure { message: e.to_string(), timed_out: false }),
        Err(_) => {
            crate::metrics::record_commit_timeout(op);
            warn!(
                table_name,
                op,
                elapsed_ms = started.elapsed().as_millis() as u64,
                timeout_secs = bound.as_secs(),
                event = "commit_lock_timeout",
                "commit-lock operation exceeded its bound — releasing the lock, landing UNCONFIRMED"
            );
            Err(CommitFailure { message: format!("{op} exceeded {}s while holding the commit lock", bound.as_secs()), timed_out: true })
        }
    }
}

/// A commit whose await TIMED OUT can never be classified `NotLanded`. We
/// abandoned the request in flight, so `N.json` may be written already; and the
/// probe reads the same store that just proved slow, so "I don't see our Adds"
/// is not evidence of absence. Downgrading to `Inconclusive` preserves the one
/// invariant that matters: never delete staged parquet a landed commit
/// references. `Landed` still passes through — that IS positive evidence.
fn probe_after_timeout(probe: CommitProbe, timed_out: bool) -> CommitProbe {
    match (probe, timed_out) {
        (CommitProbe::NotLanded, true) => CommitProbe::Inconclusive,
        (probe, _) => probe,
    }
}

/// Split a coalesced commit's newly-added file URIs per project. Files are
/// written under the `project_id=<id>/` partition path, so the path IS the
/// attribution — downstream consumers (tantivy sidecar, cache warming) keep
/// receiving only their own project's files even though one commit produced
/// them all. A single-project group returns the full list unfiltered, byte-for-
/// byte what the per-project path returns.
fn attribute_added_files(added: Vec<String>, projects: &[&str]) -> Vec<Vec<String>> {
    if projects.len() == 1 {
        return vec![added];
    }
    projects
        .iter()
        .map(|p| {
            let marker = format!("project_id={p}/");
            added.iter().filter(|u| u.contains(&marker)).cloned().collect()
        })
        .collect()
}

/// A prepared write plus the PHYSICAL-table key (`table_lock_key`) it must be
/// coalesced under.
type PreparedForPhysicalTable = (PreparedWrite, (String, String));

/// Output of [`Database::prepare_staged_write`] — see its doc comment.
struct PreparedWrite {
    table_ref: Arc<RwLock<DeltaTable>>,
    schema: &'static crate::schema_loader::TableSchema,
    dirty_bins: Vec<(String, i64)>,
    /// Lazy: the sort-merge runs when the staging writer drains it, so a
    /// batch-prepare of N units doesn't hold N sorted buckets at once.
    batches: FlushBatches,
    writer_properties: WriterProperties,
    /// Store the staged parquet lands in — used to clean it up on a terminal
    /// commit failure (those objects have no Add/Remove, so VACUUM never
    /// reclaims them).
    stage_store: Arc<dyn object_store::ObjectStore>,
    staged_writer: Option<deltalake::writer::RecordBatchWriter>,
}

/// `data_change`: true when the rewrite drops rows (dedup), false for a
/// data-preserving compaction — the fork's conflict checker only counts
/// `data_change: true` removals as conflicts (conflict_checker.rs:561), so a
/// compaction Remove marked true loses every OCC race to concurrent appends
/// (the aa50480 incident the fork rev is pinned for).
fn remove_for_add(add: &deltalake::kernel::Add, data_change: bool) -> deltalake::kernel::Remove {
    deltalake::kernel::Remove {
        path: add.path.clone(),
        data_change,
        deletion_timestamp: Some(Utc::now().timestamp_millis()),
        size: Some(add.size),
        extended_file_metadata: Some(true),
        partition_values: Some(add.partition_values.clone()),
        tags: add.tags.clone(),
        deletion_vector: add.deletion_vector.clone(),
        base_row_id: add.base_row_id,
        default_row_commit_version: add.default_row_commit_version,
    }
}

/// Collect matched Adds keeping at most ONE per path, so a rewrite's target set
/// is always countable against the distinct files it was derived from.
///
/// A path can appear twice in an in-memory snapshot even though the Delta log is
/// clean: the incremental advance concatenated a carried-forward file list with
/// a kernel "delta" that was silently a FULL file set whenever the refresh
/// crossed a newly written checkpoint (fixed in the fork, but a snapshot is
/// shared mutable state and this is the only place the damage is observable).
/// Both rewrite planners compare `targets.len()` against the number of distinct
/// files they mean to rewrite, so a duplicate turned every plan into a
/// permanent, silent mismatch — "mapped 2/1 files", 3 wasted re-plans, and dedup
/// plus hot-tail compaction stalled indefinitely (prod 2026-08-02).
fn dedup_adds_by_path(adds: impl Iterator<Item = deltalake::kernel::Add>, table_name: &str) -> Vec<deltalake::kernel::Add> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<deltalake::kernel::Add> = Vec::new();
    let mut dropped = 0usize;
    for add in adds {
        if seen.insert(add.path.clone()) {
            out.push(add);
        } else {
            dropped += 1;
        }
    }
    if dropped > 0 {
        warn!(table_name, dropped, event = "snapshot_duplicate_adds", "snapshot listed the same file more than once — reads over it double-count rows");
        crate::metrics::maintenance_stats().snapshot_duplicate_adds.fetch_add(dropped as u64, std::sync::atomic::Ordering::Relaxed);
    }
    out
}

/// Drop `name` from `batch` (no-op when absent) — strips the synthetic
/// [`DEDUP_FILE_COL`] before deduped rows are written back.
fn drop_batch_column(mut batch: RecordBatch, name: &str) -> RecordBatch {
    if let Ok(idx) = batch.schema().index_of(name) {
        batch.remove_column(idx);
    }
    batch
}

/// Cast Variant struct columns (Struct{BinaryView,BinaryView}) to the
/// Binary-backed form delta-kernel's `unshredded_variant()` requires on
/// write. No-op for any column that's not a Variant struct or already in
/// Binary form. Called from `insert_records_batch` right before the
/// Delta write so MemBuffer can keep its natural BinaryView layout
/// (matches what parquet reads produce → no per-row read-side cast).
fn cast_variant_columns_to_binary(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::{array::StructArray, compute::cast};
    use datafusion::arrow::datatypes::{DataType, Field};
    remap_batch_columns(batch, |field, col| {
        let DataType::Struct(struct_fields) = field.data_type() else { return Ok(None) };
        // Only act on Variant structs that still carry a BinaryView leg.
        if !is_variant_type(field.data_type()) || !struct_fields.iter().any(|f| matches!(f.data_type(), DataType::BinaryView)) {
            return Ok(None);
        }
        let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() else { return Ok(None) };
        let casted_cols: Vec<arrow::array::ArrayRef> = struct_arr
            .columns()
            .iter()
            .zip(struct_fields)
            .map(|(arr, f)| match f.data_type() {
                DataType::BinaryView => cast(arr, &DataType::Binary).map_err(arrow_err),
                _ => Ok(arr.clone()),
            })
            .collect::<DFResult<_>>()?;
        let casted_fields: arrow::datatypes::Fields = struct_fields
            .iter()
            .map(|f| match f.data_type() {
                DataType::BinaryView => Arc::new(Field::new(f.name(), DataType::Binary, f.is_nullable())),
                _ => f.clone(),
            })
            .collect::<Vec<_>>()
            .into();
        let new_field =
            Arc::new(Field::new(field.name(), DataType::Struct(casted_fields.clone()), field.is_nullable()).with_metadata(field.metadata().clone()));
        Ok(Some((new_field, Arc::new(StructArray::new(casted_fields, casted_cols, struct_arr.nulls().cloned())) as arrow::array::ArrayRef)))
    })
}

/// Rebuild `batch` with the columns for which `remap` yields a replacement
/// `(field, array)`; `None` leaves a column untouched, and an all-`None` pass
/// returns `batch` itself (no copy). Schema-level metadata is preserved.
/// Shared by the Variant→Binary cast and the timestamp-timezone normalizer so
/// their no-op and metadata semantics can't drift.
fn remap_batch_columns(
    batch: RecordBatch, remap: impl Fn(&arrow_schema::FieldRef, &arrow::array::ArrayRef) -> DFResult<Option<(arrow_schema::FieldRef, arrow::array::ArrayRef)>>,
) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let remapped = schema.fields().iter().zip(batch.columns()).map(|(f, c)| remap(f, c)).collect::<DFResult<Vec<_>>>()?;
    if remapped.iter().all(Option::is_none) {
        return Ok(batch);
    }
    let (fields, columns): (Vec<_>, Vec<_>) =
        remapped.into_iter().zip(schema.fields().iter().zip(batch.columns())).map(|(new, (f, c))| new.unwrap_or_else(|| (f.clone(), c.clone()))).unzip();
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, columns).map_err(arrow_err)
}

/// Normalize incoming Timestamp columns whose timezone is a numeric UTC
/// offset (`"+00:00"` — what psycopg / pgwire emit for timestamptz) to the
/// IANA name `"UTC"`. Delta-rs's Arrow→Delta schema converter rejects
/// `Timestamp(µs, "+00:00")` even though it's semantically identical to
/// `"UTC"`; without normalization every flush errors out and MemBuffer
/// fills until eviction warnings, with no data ever reaching Delta.
///
/// We only retag — the underlying micros-since-epoch buffer is unchanged.
fn normalize_timestamp_tz(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::array::{TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
    // Accept anything that semantically means UTC. Case-insensitive on alphabetic
    // forms ("UTC"/"Utc"/"utc"/"Z"/"GMT") and tolerant of the common offset
    // representations clients emit (+/- 00:00, 0000, 00). Delta-rs only
    // accepts the IANA "UTC" string, so we rewrite any of these to it.
    let is_utc_offset = |tz: &str| {
        matches!(tz, "+00:00" | "-00:00" | "+0000" | "-0000" | "+00" | "-00" | "00:00" | "0000")
            || tz.eq_ignore_ascii_case("UTC")
            || tz.eq_ignore_ascii_case("GMT")
            || tz.eq_ignore_ascii_case("Z")
    };
    remap_batch_columns(batch, |field, col| {
        let DataType::Timestamp(unit, Some(tz)) = field.data_type() else { return Ok(None) };
        if !is_utc_offset(tz.as_ref()) {
            return Ok(None);
        }
        // Downcasts are guarded by the `DataType::Timestamp(unit, ..)` match above,
        // but Arrow's trait-object dispatch isn't an unsafe-level guarantee — return
        // an error rather than panic on the INSERT path if a future Arrow version
        // diverges.
        let bad = |w| DataFusionError::Execution(format!("timestamp downcast failed for field '{}' with width {w}", field.name()));
        let retagged: arrow::array::ArrayRef = match unit {
            TimeUnit::Microsecond => {
                Arc::new(col.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| bad("Microsecond"))?.clone().with_timezone("UTC"))
            }
            TimeUnit::Millisecond => {
                Arc::new(col.as_any().downcast_ref::<TimestampMillisecondArray>().ok_or_else(|| bad("Millisecond"))?.clone().with_timezone("UTC"))
            }
            TimeUnit::Nanosecond => {
                Arc::new(col.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| bad("Nanosecond"))?.clone().with_timezone("UTC"))
            }
            TimeUnit::Second => Arc::new(col.as_any().downcast_ref::<TimestampSecondArray>().ok_or_else(|| bad("Second"))?.clone().with_timezone("UTC")),
        };
        let new_field =
            Arc::new(Field::new(field.name(), DataType::Timestamp(*unit, Some("UTC".into())), field.is_nullable()).with_metadata(field.metadata().clone()));
        Ok(Some((new_field, retagged)))
    })
}

/// `date` is a physical UTC partition key, never caller-owned data. Rebuild it
/// from `timestamp` before every shared write path so timestamp pruning cannot
/// hide rows that arrived with a stale or malformed client-provided date.
fn derive_date_partition(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::array::{Date32Array, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
    use datafusion::arrow::datatypes::{DataType, TimeUnit};

    let schema = batch.schema();
    let (Ok(date_idx), Ok(timestamp_idx)) = (schema.index_of("date"), schema.index_of("timestamp")) else { return Ok(batch) };
    if !matches!(schema.field(date_idx).data_type(), DataType::Date32) {
        return Err(DataFusionError::Execution("date partition column must be Date32".to_string()));
    }
    let timestamp = batch.column(timestamp_idx);
    let fail = |message| DataFusionError::Execution(format!("timestamp-to-date partition conversion failed: {message}"));
    let micros = |row| -> DFResult<Option<i64>> {
        if timestamp.is_null(row) {
            return Ok(None);
        }
        let value = match schema.field(timestamp_idx).data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                timestamp.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| fail("nanosecond downcast"))?.value(row).div_euclid(1_000)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                timestamp.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| fail("microsecond downcast"))?.value(row)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => timestamp
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| fail("millisecond downcast"))?
                .value(row)
                .checked_mul(1_000)
                .ok_or_else(|| fail("millisecond overflow"))?,
            DataType::Timestamp(TimeUnit::Second, _) => timestamp
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| fail("second downcast"))?
                .value(row)
                .checked_mul(1_000_000)
                .ok_or_else(|| fail("second overflow"))?,
            _ => return Err(fail("timestamp column is not a timestamp")),
        };
        Ok(Some(value))
    };
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let dates = (0..batch.num_rows())
        .map(|row| {
            micros(row)?
                .map(|micros| {
                    chrono::DateTime::from_timestamp_micros(micros)
                        .ok_or_else(|| fail("invalid timestamp"))
                        .map(|ts| ts.date_naive().signed_duration_since(epoch).num_days() as i32)
                })
                .transpose()
        })
        .collect::<DFResult<Vec<_>>>()?;
    let mut columns = batch.columns().to_vec();
    columns[date_idx] = Arc::new(Date32Array::from(dates));
    RecordBatch::try_new(schema, columns).map_err(arrow_err)
}

/// Convert Utf8/Utf8View/LargeUtf8 columns to Variant binary StructArrays where the target
/// schema expects Variant. Called from `DataSink::write_all` so that INSERT statements (where
/// the table provider presents Variant cols as Utf8View for the SQL planner's type check) can
/// land their JSON-string values in the underlying Delta storage which expects Variant structs.
fn convert_variant_columns(batch: RecordBatch, target_schema: &SchemaRef) -> DFResult<RecordBatch> {
    use datafusion::arrow::{
        array::{Array, ArrayRef, LargeStringArray, StringArray, StringViewArray, StructArray},
        compute::cast,
        datatypes::{DataType, Field},
    };
    use parquet_variant_compute::VariantArrayBuilder;
    use parquet_variant_json::JsonToVariant;

    let batch_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut new_fields: Vec<Arc<Field>> = batch_schema.fields().iter().cloned().collect();

    fn utf8_to_variant<'a>(iter: impl ExactSizeIterator<Item = Option<&'a str>>) -> DFResult<StructArray> {
        let mut builder = VariantArrayBuilder::new(iter.len());
        for (idx, item) in iter.enumerate() {
            match item {
                Some(s) => builder.append_json(s).map_err(|e| DataFusionError::Execution(format!("Invalid JSON at row {idx}: {e} (value: '{s}')")))?,
                None => builder.append_null(),
            }
        }
        // Cast VariantArrayBuilder's BinaryView output to Binary so the
        // batch matches `delta_kernel::unshredded_variant()` (which is what
        // our schema declares). Both Delta reads and MemBuffer end up as
        // Binary → no per-row casts on the read path.
        let arr: StructArray = builder.build().into();
        let metadata = cast(arr.column(0), &DataType::Binary).map_err(arrow_err)?;
        let value = cast(arr.column(1), &DataType::Binary).map_err(arrow_err)?;
        let fields = vec![
            Arc::new(Field::new(crate::schema_loader::VARIANT_METADATA_FIELD, DataType::Binary, false)),
            Arc::new(Field::new(crate::schema_loader::VARIANT_VALUE_FIELD, DataType::Binary, false)),
        ];
        Ok(StructArray::new(fields.into(), vec![metadata, value], arr.nulls().cloned()))
    }

    for (idx, target_field) in target_schema.fields().iter().enumerate().take(columns.len()).filter(|(_, f)| is_variant_type(f.data_type())) {
        let col = &columns[idx];
        // Downcasts are guarded by the `DataType::*` match arm above. If Arrow ever
        // returns a different concrete array for the same logical type, surface as
        // a DataFusionError instead of panicking on the INSERT path.
        let name = target_field.name();
        let bad_downcast = |ty: &str| DataFusionError::Execution(format!("{ty} downcast failed for column {name}"));
        let converted: ArrayRef = match col.data_type() {
            DataType::Utf8View => Arc::new(utf8_to_variant(col.as_any().downcast_ref::<StringViewArray>().ok_or_else(|| bad_downcast("Utf8View"))?.iter())?),
            DataType::Utf8 => Arc::new(utf8_to_variant(col.as_any().downcast_ref::<StringArray>().ok_or_else(|| bad_downcast("Utf8"))?.iter())?),
            DataType::LargeUtf8 => Arc::new(utf8_to_variant(col.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| bad_downcast("LargeUtf8"))?.iter())?),
            _ => continue, // already Variant struct
        };
        columns[idx] = converted;
        new_fields[idx] = target_field.clone();
    }

    let new_schema = Arc::new(arrow_schema::Schema::new(new_fields));
    RecordBatch::try_new(new_schema, columns).map_err(arrow_err)
}

// Fallback ZSTD level when a configured/tier level is rejected as out-of-range.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;
// Parquet footer key-value metadata key recording the ZSTD level used to
// write the file. Read by `recompress_partition` to skip files already
// at-or-above the target tier without rewriting.
const COMPRESSION_TIER_KEY: &str = "timefusion.compression_tier";

#[derive(Clone, Serialize, Deserialize, sqlx::FromRow, derive_more::Debug)]
struct StorageConfig {
    project_id: String,
    table_name: String,
    s3_bucket: String,
    s3_prefix: String,
    s3_region: String,
    /// Skipped on serialize so credentials never leak through serde-based dumps
    /// (debug endpoints, metrics serialization, etc.). sqlx::FromRow bypasses
    /// serde so DB-row loading is unaffected. `#[debug("[redacted]")]` keeps
    /// them out of `{:?}` log lines.
    #[serde(serialize_with = "redact_str")]
    #[debug("[redacted]")]
    s3_access_key_id: String,
    #[serde(serialize_with = "redact_str")]
    #[debug("[redacted]")]
    s3_secret_access_key: String,
    s3_endpoint: Option<String>,
}

fn redact_str<S: serde::Serializer>(_: &str, ser: S) -> std::result::Result<S::Ok, S::Error> {
    ser.serialize_str("[redacted]")
}

#[derive(Debug, Clone)]
pub struct Database {
    config: Arc<AppConfig>,
    /// One RuntimeEnv (and thus one memory pool) shared by every session
    /// context, across `Database` clones. Per-context pools each granted the
    /// full `memory_limit × fraction` budget, so N contexts oversubscribed
    /// the cgroup N×; the pool only enforces a global cap if it's global.
    runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Dedicated maintenance (optimize / dedup / recompress) `RuntimeEnv`: a
    /// bounded FairSpill pool + on-disk spill dir, kept separate from the query
    /// pool so a Z-order global sort can always reserve its merge floor and spill
    /// instead of losing the race for the saturated shared Greedy pool.
    maintenance_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Hot-tail light-optimize slice carved out of the maintenance budget:
    /// heavy rewrites (dedup, recompress, Z-order) can hold the shared pool
    /// for minutes, and the small (≤target_size) hot-tail sorts starved
    /// behind them every tick (prod 2026-07-22: 22 `Resources exhausted`
    /// in 25 min with zero OCC conflicts). Separate pool ⇒ today's
    /// compaction always has its reserve; total budget stays constant.
    light_optimize_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Flush-path sort pool, for buckets too large for the in-process sort.
    /// Its own slice, not maintenance's: flush is on the INGEST path and must
    /// not queue behind a Z-order holding the maintenance pool for minutes.
    /// Bounded + spillable, which is the whole point — the in-process sort
    /// allocates outside every pool, and authorising GBs of that on a 26 GB box
    /// is what took prod down on 2026-08-01.
    flush_sort_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Caps how many spilling flush sorts share `flush_sort_runtime_env` at
    /// once. The pool is a `FairSpillPool`, so N concurrent sorts each get
    /// ~pool/N; below a viable slice `ExternalSorterMerge` cannot merge its
    /// spill files and the sort fails with "Not enough memory to continue
    /// external sort" (prod, 2026-08-02). The caller then writes the group
    /// UNSORTED, and a single file with no `sorting_columns` footer disables
    /// the reader's all-or-nothing ordering for every scan touching that
    /// partition — so pool starvation shows up as slow queries, not as an
    /// error. Serialising a few oversized sorts is much cheaper than that.
    flush_sort_gate: Arc<tokio::sync::Semaphore>,
    /// Memoized `build_optimize_session_state` results, one per runtime env.
    /// Building a `SessionState` re-registers every analyzer/optimizer rule and
    /// the whole UDF/UDAF set; the maintenance loop did that on EVERY optimize
    /// attempt (up to 12 bins × 4 retries per project per 5-min tick). Inputs
    /// are constant for the process lifetime (query partitions + the OnceLock'd
    /// runtime env), so build once and clone per use — a clone is a handful of
    /// Arc bumps against a full rebuild.
    ///
    /// ONLY for `.with_session_state(...)` on delta-rs builders. A `SessionState`
    /// clone SHARES its `catalog_list` Arc, so the `SessionContext::new_with_state`
    /// dedup/recompress sites — which `register_table("__dedup_src", …)` — must
    /// keep building a fresh state, or concurrent chunks overwrite each other's
    /// registration and scan the wrong table (caught by
    /// `dirty_dedup_bins_enqueue_seal_and_requeue`).
    maintenance_session_state: Arc<std::sync::OnceLock<datafusion::execution::session_state::SessionState>>,
    light_optimize_session_state: Arc<std::sync::OnceLock<datafusion::execution::session_state::SessionState>>,
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables: UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables: CustomProjectTables,
    /// Lock-free per-(project,table) cache of resolved Delta table refs. The
    /// inner `Arc<RwLock<DeltaTable>>` is the same object held in
    /// `unified_tables`/`custom_project_tables`, so update_state on the slow
    /// path mutates the table seen by hot-path callers too. Read path:
    /// `DashMap.get` (lock-free) → `Arc` clone. Skips the 3 tokio RwLock
    /// `.await`s in `resolve_unified_table` / `resolve_custom_table` that
    /// otherwise dominated the per-query latency under load (proven via
    /// `slow delta scan` instrumentation showing `resolve` was 99% of cost).
    ///
    /// **Growth**: this map has no eviction — size scales with the unique
    /// `(project_id, table_name)` pairs seen since process start. For
    /// unified tables every entry holds an `Arc::clone` of the same
    /// `DeltaTable` (cheap, ~16 bytes), so 100 k tenants = a few MB. Custom
    /// tables hold distinct objects so memory tracks the number of distinct
    /// custom configs. Operators with churn far above expected tenant
    /// counts should add a periodic sweeper; for the current target
    /// (thousands of tenants) the leakage is well under noise.
    ///
    /// **No drop eviction**: same caveat as `delta_provider_cache` below
    /// — entries for tables dropped at runtime persist until process
    /// restart. Watch `scan.fast_resolve_cache_entries` in
    /// `timefusion_stats` for unbounded growth.
    fast_resolve_cache: FastResolveCache,
    /// Per-(project,table) sticky bit: "Delta may hold matching files."
    /// Two seed paths so the bit is always at least as conservative as truth
    /// — never falsely `false`:
    ///   1. **Cold start / first resolve**: `resolve_table` reads
    ///      `DeltaTable.version()` from the snapshot we just loaded. The
    ///      snapshot itself is hydrated from `_delta_log/*.json` on S3, so
    ///      a fresh process inherits the S3 truth. `version > 0` ⇒ true.
    ///   2. **Steady state**: the flush callback (`main.rs`) calls
    ///      `mark_delta_has_files` after every successful commit that adds
    ///      files. Sticky-monotonic — once `true`, never flipped back, so
    ///      compaction churn doesn't mistakenly hide data.
    ///
    /// While `false`, `ProjectRoutingTable::scan` short-circuits the Delta
    /// scan entirely — MemBuffer is authoritative for all rows. Avoids the
    /// per-query cost of building a delta-rs TableProvider + scan plan for
    /// a project that has never committed (common at warm-up and in the
    /// multi-tenant case where most projects sit below the flush threshold).
    /// The safe direction (`true` when actually empty after vacuum) just
    /// runs the scan unnecessarily — no correctness risk.
    /// `Arc<AtomicBool>` rather than just `AtomicBool` because `Database`
    /// derives `Clone` (see `db.clone()` in the flush callback wiring in
    /// `main.rs`) and `AtomicBool: !Clone`. Dropping the wrap would force
    /// either a manual `Clone` impl that re-creates fresh atomics
    /// (incorrect — would lose visibility between clones) or removing the
    /// derive (invasive). The extra heap allocation per tenant pair is a
    /// few bytes and well off the hot path.
    delta_has_files: dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicBool>>,
    /// Per-(project,table) cached Delta-side `TableProvider` along with the
    /// snapshot version it was built against. Steady-state (post-flush)
    /// queries that have to UNION mem + delta were rebuilding the provider
    /// on every scan — measured as ~30 ms p95 of pure Delta-side overhead
    /// in the prior session. The provider is parameter-independent: every
    /// query for `(project, table)` at the same snapshot version uses the
    /// same provider, varying only filters/projection/limit on scan().
    /// Invalidation: look the resolved `table.version()` up in the key's
    /// recent-version ring (`PROVIDER_VERSION_RETENTION`); absent → build and
    /// install at the head. Lookup is exact-version, so a bump never serves
    /// stale files — it just doesn't throw the predecessor away. Versions
    /// expire after the configured TTL and the total provider count is capped
    /// at the configured capacity.
    ///
    /// Concurrent misses are de-duplicated through a per-key `OnceCell`:
    /// the first task to miss installs the cell and starts the build; later
    /// tasks find the cell, await its completion, and share the same Arc.
    /// Without this guard, N concurrent first-time queries would each pay
    /// the full build cost.
    ///
    /// **Known limitation — no drop eviction**: entries for tables that
    /// are dropped at runtime stay in the map. The cached `Arc<dyn
    /// TableProvider>` keeps the underlying state alive (file lists,
    /// snapshot metadata), so memory tracks the historical max of
    /// distinct `(project, table)` pairs, not the live set. For
    /// workloads with steady tenant counts this is invisible; for a
    /// churning create/drop pattern, expose `scan.provider_cache_entries`
    /// in `timefusion_stats` (already wired) for alerting, and add a
    /// TTL sweep here when it ever becomes a real problem.
    delta_provider_cache: DeltaProviderCache,
    /// Per-process scan-path counters. Read by `timefusion_stats` so operators
    /// can see — in prod — whether the in-memory shortcut is being taken,
    /// what the resolve cache hit rate looks like, and how the latency
    /// distribution shifts under real load. Counters are cumulative since
    /// process start; deltas are useful for rate analysis.
    pub scan_metrics: Arc<ScanMetrics>,
    batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    maintenance_shutdown: Arc<CancellationToken>,
    /// Cancels `maintenance_shutdown` when the LAST guard-holding `Database`
    /// clone drops. Database is Clone, so a per-value `impl Drop` cancelling
    /// the shared token killed every cron job / the DML coalescer / dedup
    /// sweeps as soon as ANY transient clone dropped (2026-07-14 prod outage:
    /// all maintenance silently dead minutes after boot). `None` in clones
    /// handed to long-lived background tasks (see `background_clone`) —
    /// otherwise a task waiting on the token would hold its own kill-switch
    /// alive and the guard could never fire.
    _maintenance_cancel_guard: Option<Arc<tokio_util::sync::DropGuard>>,
    /// One-shot guard for `preload_tables` — main.rs and bootstrap.rs are
    /// disjoint entry points today, but a second call must not double the
    /// boot-time S3 warm burst.
    preload_started: Arc<std::sync::atomic::AtomicBool>,
    config_pool: Option<PgPool>,
    storage_configs: Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    /// Monotonic deadline (nanos since process start) for when the next
    /// storage-configs refresh from the config DB is allowed. Capped at 30s
    /// so a hot SQL path doesn't hit PG on every statement.
    storage_configs_next_refresh_ns: Arc<std::sync::atomic::AtomicU64>,
    default_s3_bucket: Option<String>,
    default_s3_prefix: Option<String>,
    default_s3_endpoint: Option<String>,
    object_store_cache: Option<Arc<SharedFoyerCache>>,
    statistics_extractor: Arc<DeltaStatisticsExtractor>,
    last_written_versions: Arc<RwLock<HashMap<(String, String), u64>>>,
    /// Delta snapshot version at last dedup sweep, per scheduler key. Skips
    /// the sweep when the version hasn't moved (no commits → no new dupes).
    /// Same unbounded-growth caveat as `last_written_versions`.
    last_dedup_versions: Arc<RwLock<HashMap<String, u64>>>,
    /// (project, table, date) → fingerprint (hash of the partition's sorted
    /// live file set) captured when a dedup sweep pass found ZERO duplicates
    /// and the file set was unchanged across the pass. A query whose window
    /// partitions all fingerprint-match the current snapshot provably reads
    /// no duplicates from Delta, so `DedupExec` (and its LIMIT-pushdown
    /// suppression) can be skipped (`timefusion_read_dedup_skip_swept`).
    /// Any commit touching the partition changes its file set → mismatch →
    /// dedup stays on until the next clean sweep pass.
    dedup_clean_fp: Arc<dashmap::DashMap<(String, String, String), u64>>,
    /// Exact merge-on-read count partitions. Query threads use only the
    /// process-local front; disk loads and Delta builds are single-flight and
    /// bounded in the background so a cold cache cannot amplify query load.
    logical_count_cache: Arc<crate::logical_count_index::LogicalCountCache>,
    logical_count_building: Arc<dashmap::DashSet<crate::logical_count_index::CountPartition>>,
    logical_count_build_sem: Arc<tokio::sync::Semaphore>,
    /// Dirty `(project, table, date, 10-minute bin)` keys recorded only after
    /// a Delta append commits. In-memory by design: after restart the
    /// read-side DedupExec remains the correctness backstop.
    dedup_dirty_bins: Arc<dashmap::DashMap<(String, String, String, i64), ()>>,
    /// Exponential failure backoff per (table, project, date) dedup target:
    /// (attempts, earliest next try). Without it a failing partition re-runs
    /// on every 5-minute sweep tick forever — the 2026-07-04 crash-loop's
    /// pacing. Cleared on success; in-memory only (a restart retries once).
    dedup_backoff: Arc<dashmap::DashMap<String, (u32, std::time::Instant)>>,
    /// Caps concurrent HEAVY maintenance rewrites — and ONLY those: dedup
    /// dirty-bin staging (`stage_dedup_chunk`), full optimize (Z-order /
    /// consolidate), nightly light-consolidate (`optimize_table_light_inner`)
    /// and `recompress_partition`. Their Arrow footprint is invisible to the
    /// DataFusion memory pool (a `SELECT * … collect()` doesn't reserve through
    /// it), so aggregate concurrency — not the pool — is the real bound against
    /// the cgroup OOM (prod 2026-07-04). Permits = `derived.rewrite_permits()`.
    ///
    /// The hot-tail WAVE engine deliberately does NOT share this semaphore —
    /// see [`Self::light_rewrite_sem`].
    maintenance_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent hot-tail WAVE staging (`stage_hot_bin`) — and nothing
    /// else. SEPARATE from `maintenance_rewrite_sem` on purpose: prod
    /// 2026-07-30, one dedup dirty-bin drain held the shared rewrite permits
    /// for 25+ minutes and starved hot compaction (62 bins selected, 1 wave
    /// committed in 35 min; 5-min ticks overrunning 2-3x). The two engines are
    /// individually bounded and target disjoint partitions (dedup skips today,
    /// hot compaction only selects today), so serializing them against each
    /// other is pure loss.
    ///
    /// Sized to the light pool's OWN slice (`derived.max_light_optimize_k()`,
    /// the same K `light_optimize_pool_bytes` was budgeted for) rather than the
    /// heavy permit count: heavy permits (2) are far below K, so borrowing them
    /// here would cap waves at 2 and re-create the starvation this split fixes.
    /// Its purpose is a single instrumented choke point (permit-wait ms in
    /// `wave_bin_staged`) plus a hard ceiling should a future caller drive
    /// staging outside `round_robin_bins`' K.
    light_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent user DML MERGE-UPDATEs (hash enrichment). Each scans the
    /// time-windowed target to hash-join keys; ungated bursts starve reads on a
    /// CPU-throttled box (prod 2026-07-19). Permits = `timefusion_dml_merge_concurrency`.
    dml_merge_sem: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent Parquet batch-decodes for WIDE read scans (see
    /// `timefusion_max_concurrent_scan_readers`). Shared across all queries so a
    /// burst of wide-window dashboards can't stack decode buffers into an OOM.
    heavy_scan_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes the outer full and light maintenance jobs. Their rewrite
    /// permits alone are insufficient: a waiting light job can exhaust its
    /// table timeout before it ever starts work.
    maintenance_job_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes in-process Delta commits (flush appends vs dedup
    /// replace_where) PER PHYSICAL TABLE, keyed via `table_lock_key`.
    /// delta-kernel's OCC checker cannot evaluate the bare-string timestamp
    /// predicate replace_where commits carry (errors "arrow_cast should have
    /// been simplified"), so a dedup commit racing any concurrent append to
    /// the SAME log aborts — every attempt, forever, on a busy table.
    /// Serializing per-log commits lets the rebase see no newer versions and
    /// skip the checker. Formerly a process-wide mutex, which needlessly
    /// serialized commits to *different* Delta logs and capped flush
    /// throughput below `flush_parallelism` (issue #83).
    commit_locks: DmlLocks,
    /// Flush/ingest committers currently QUEUED on each table's `commit_locks`
    /// entry (same key). `tokio::sync::Mutex` is FIFO and every holder is
    /// bounded, but a backlogged maintenance tick can still park SEVERAL
    /// minutes-long wave commits (OCC ladders over a big log) ahead of a flush —
    /// prod 2026-07-30: flush waited >600s to ACQUIRE and its watchdog killed
    /// the attempt while `commit_lock_timeouts` stayed 0 (nobody hung; flush
    /// starved in the queue). Durability outranks maintenance, so `commit_wave`
    /// declines to enqueue while this is nonzero and flush latency is bounded by
    /// at most ONE in-flight wave commit instead of a queue of them.
    flush_waiter_counts: FlushWaiterCounts,
    /// Per-table serialization for in-process DML (see `dml_lock`): concurrent
    /// merges on the same table would OCC-conflict and redo full parquet
    /// rewrites, so they queue here — without touching the table's RwLock,
    /// which stays free for readers and insert commits.
    dml_locks: DmlLocks,
    /// Last time each table's snapshot was persisted to disk (keyed by table
    /// url). `persist_snapshot` throttles on this: the on-disk snapshot is only
    /// a boot-recovery seed (restore V, replay commits > V), so rewriting the
    /// whole 5k-file state on *every* commit is wasted CPU (13% in the
    /// 2026-07-05 profile, serde_json + zstd). A slightly stale snapshot just
    /// makes boot replay a few more (sub-second) commits.
    snapshot_persist_gate: Arc<dashmap::DashMap<String, std::time::Instant>>,
    /// Late-binding shared cell: boot must create the pgwire SessionContext
    /// (whose FunctionRegistry the WAL replay needs) BEFORE the layer exists,
    /// so the layer is published through a OnceLock shared across all clones —
    /// including ones captured earlier (DmlQueryPlanner). A plain
    /// `Option<Arc<_>>` here silently left pre-layer clones without the mem
    /// leg: pgwire UPDATEs skipped the buffer and lost updates to unflushed
    /// rows.
    buffered_layer: Arc<std::sync::OnceLock<Arc<crate::buffered_write_layer::BufferedWriteLayer>>>,
    /// Per-clone override for `query_delta_only`: hides the shared layer so
    /// scans bypass the in-memory buffer.
    bypass_buffer: bool,
    /// Late-binding shared cells like `buffered_layer`: attached by `with_*`
    /// builders after boot has already cloned Database into sessions/planners,
    /// so a plain Option would leave those clones silently service-less.
    tantivy_search: Arc<std::sync::OnceLock<Arc<crate::tantivy_index::search::TantivySearchService>>>,
    tantivy_indexer: Arc<std::sync::OnceLock<Arc<crate::tantivy_index::service::TantivyIndexService>>>,
    /// Deferred-DML coalescer (see `dml_coalescer`) — populated by
    /// `start_dml_coalescer` when `TIMEFUSION_DML_COALESCE_SECS > 0`. Same
    /// late-binding shared-cell pattern as `buffered_layer`: the DML planner
    /// clones Database before boot wiring finishes.
    dml_coalescer: Arc<std::sync::OnceLock<Arc<crate::dml_coalescer::DmlCoalescer>>>,
    /// Per-table, per-date set of live file URIs as of the last successful full
    /// (z-order) optimize. delta-rs's ZOrder planner has no idempotence guard —
    /// it rewrites every file in the window on every run, even sealed days that
    /// didn't change, minting cold multipart objects that cold-start the
    /// object-store cache (which PR #39 then has to re-warm). This lets
    /// `optimize_table` skip a sealed partition whose file set is unchanged.
    /// Keyed by table storage URL (unique per physical table). In-memory only:
    /// a restart re-z-orders each partition once, which is harmless.
    zorder_filesets: ZOrderFilesets,
    /// Last version the out-of-band checkpoint task checkpointed, keyed by table
    /// storage URL. Lets that task skip idle tables and tables whose version
    /// hasn't advanced by `checkpoint_interval` since the last checkpoint. Since
    /// checkpoint/log-cleanup no longer run in the commit hook (base_commit_properties),
    /// this task and `checkpoint_after_waves` (post-tick) are the only
    /// checkpoint drivers. In-memory only: after a restart
    /// the first tick checkpoints every table once, which is harmless.
    checkpoint_versions: Arc<dashmap::DashMap<String, u64>>,
    /// Serializes local staged-intent manifest append/rewrite operations.
    /// Orphan reconciliation runs after readiness and may finish while a
    /// maintenance wave records a new intent; without this lock its compacting
    /// rewrite could overwrite that append and remove the only targeted-cleanup
    /// record for a subsequently orphaned parquet file.
    staged_intent_manifest_lock: Arc<std::sync::Mutex<()>>,
    /// Where the last truncated light-optimize tick stopped, as an index into
    /// that tick's debt-ordered project list. The next tick rotates its plan by
    /// this so an overloaded box degrades to "every project within k ticks"
    /// instead of forever re-serving the same debt-ordered prefix.
    light_optimize_cursor: Arc<std::sync::atomic::AtomicUsize>,
}

impl Database {
    /// Get the config for this database instance
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Concurrency gate for user DML MERGE-UPDATEs — see `dml_merge_sem`.
    pub(crate) fn dml_merge_sem(&self) -> &Arc<tokio::sync::Semaphore> {
        &self.dml_merge_sem
    }

    /// Get the unified tables cache for direct access
    pub fn unified_tables(&self) -> &UnifiedTables {
        &self.unified_tables
    }

    /// Get the custom project tables cache for direct access
    pub fn custom_project_tables(&self) -> &CustomProjectTables {
        &self.custom_project_tables
    }

    /// Perform a Delta table UPDATE operation
    pub async fn perform_delta_update(
        &self, table_name: &str, project_id: &str, predicate: Option<datafusion::logical_expr::Expr>,
        assignments: Vec<(String, datafusion::logical_expr::Expr)>, session: Arc<dyn datafusion::catalog::Session>,
    ) -> Result<u64, DataFusionError> {
        crate::dml::perform_delta_update(self, table_name, project_id, predicate, assignments, session).await
    }

    /// Perform a Delta table DELETE operation
    pub async fn perform_delta_delete(
        &self, table_name: &str, project_id: &str, predicate: Option<datafusion::logical_expr::Expr>, session: Arc<dyn datafusion::catalog::Session>,
    ) -> Result<u64, DataFusionError> {
        crate::dml::perform_delta_delete(self, table_name, project_id, predicate, session).await
    }

    /// Build storage options with consistent configuration for S3.
    fn build_storage_options(&self) -> HashMap<String, String> {
        let storage_options = self.config.aws.build_storage_options(self.default_s3_endpoint.as_deref());

        // debug! (not info!) because this is called on every insert path —
        // info-level logging here would flood production logs.
        let safe_options: HashMap<_, _> = storage_options.iter().filter(|(k, _)| !k.contains("secret") && !k.contains("password")).collect();
        debug!("Storage options configured: {:?}", safe_options);
        storage_options
    }

    /// Creates writer properties for a Delta write at a given compression tier.
    ///
    /// Tiered strategy: hot writes use level 3 (fast ingest);
    /// `recompress_partition` rewrites older partitions at 9/15/19 to
    /// maximize storage savings on
    /// cold data. The chosen level is embedded in Parquet footer key-value
    /// metadata (`timefusion.compression_tier`) so re-sweeps can skip files
    /// already at the target tier.
    ///
    /// Encoding strategy per column:
    /// - Timestamps/Date32, ints: `DELTA_BINARY_PACKED` (dict off for timestamps).
    /// - Sorted-key Utf8 columns: `DELTA_BYTE_ARRAY` (delta-encoded, dict off) —
    ///   excellent ratios on sorted ids/service names; harmless when only mostly
    ///   sorted (still better than raw PLAIN).
    /// - Other Utf8: default (dict on, auto-falls back to PLAIN at 8MB).
    /// - Per-field `dictionary: false` opt-out for high-entropy free-text.
    /// - Per-field `bloom_filter: true` opt-in for point-lookup columns
    ///   (ids/trace_ids/span_ids); NDV scaled to row-group size.
    ///
    /// `declare_sorted`: pass `true` only from paths that sort rows by the
    /// schema sort keys before writing (flush, dedup). Optimize/compact pass
    /// `false`. See `build_writer_properties`.
    fn create_writer_properties(&self, schema: &crate::schema_loader::TableSchema, zstd_level: i32, declare_sorted: bool) -> WriterProperties {
        build_writer_properties(&self.config.parquet, schema, zstd_level, declare_sorted)
    }

    /// WriterProperties for the DML rewrite paths (`dml::perform_delta_{merge_update,
    /// update,delete}`, used by monoscope's `UPDATE`/`DELETE`/`UPDATE ... FROM` +
    /// the dml_coalescer). Standard zstd tier; `declare_sorted=false` because these
    /// rewrite/reorder matched rows (no global sort). Without passing this, delta-rs's
    /// Merge/Update/Delete builders fall back to their SNAPPY default, leaving
    /// `.snappy.parquet` files that inflate storage/scan bytes and force the daily
    /// recompress to rewrite them to zstd.
    pub(crate) fn dml_writer_properties(&self, table_name: &str) -> WriterProperties {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // `declare_sorted=true` is HONEST here only because the DV-merge path
        // now sorts its appended rows by these same keys before writing
        // (`MergeDvUpdate::append_sort_by`, fork rev 94f9cfe4). Before that the
        // appended file carried the join's row order and had to declare
        // nothing — and ONE such file disabled the reader's all-or-nothing
        // footer ordering for the whole partition, which is how a
        // continuously-enriched table lost its top-N pushdown permanently
        // (measured prod 2026-08-01: a 1-row DML file among 24 sorted ones).
        //
        // Load-bearing pair: if the sort is ever removed from the append path,
        // this must go back to `false` or the footer starts lying and the
        // reader will merge on an order the file does not have.
        self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, true)
    }

    /// Updates a DeltaTable and handles errors consistently
    async fn update_table(&self, table: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) -> Result<()> {
        // Try to update with retries for eventual consistency
        let mut retries = 0;
        const MAX_RETRIES: u32 = 5;

        loop {
            match refresh_table_snapshot(table, self.config.maintenance.timefusion_incremental_snapshot).await {
                Ok(version) => {
                    if let Some(version) = version {
                        debug!("Updated table for {}/{} to version {}", project_id, table_name, version);
                        // Update our version tracking to reflect what we just loaded
                        let mut versions = self.last_written_versions.write().await;
                        versions.insert((project_id.to_string(), table_name.to_string()), version);
                    }
                    return Ok(());
                }
                Err(e) => {
                    retries += 1;
                    if retries >= MAX_RETRIES {
                        error!("Failed to update table for {}/{} after {} retries: {}", project_id, table_name, MAX_RETRIES, e);
                        return Err(anyhow::anyhow!("Failed to update table: {}", e));
                    }

                    debug!("Failed to update table for {}/{} (attempt {}/{}): {}, retrying...", project_id, table_name, retries, MAX_RETRIES, e);
                    // Exponential backoff with jitter, capped at ~6.4s.
                    // `100 << retries` doubles each attempt; clamp to 6 shifts
                    // so a long retry chain doesn't sleep for minutes. Jitter
                    // is `± delay/4` so concurrent retriers don't thunder.
                    let base = 100u64 << retries.min(6);
                    let jitter = fastrand::u64(0..=base / 2);
                    let delay = base / 2 * 3 + jitter; // base*0.75 .. base*1.25
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                }
            }
        }
    }

    /// One-time DDL to ensure the config schema exists. Run during Database
    /// construction, not on every config reload — DDL in a hot read path is
    /// surprising and serializes concurrent callers.
    async fn ensure_storage_configs_schema(pool: &PgPool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS timefusion_projects (
                project_id VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                s3_bucket VARCHAR(255) NOT NULL,
                s3_prefix VARCHAR(500) NOT NULL,
                s3_region VARCHAR(100) NOT NULL,
                s3_access_key_id VARCHAR(500) NOT NULL,
                s3_secret_access_key VARCHAR(500) NOT NULL,
                s3_endpoint VARCHAR(500),
                is_active BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (project_id, table_name)
            )
            "#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Load storage configurations from PostgreSQL. AWS credential columns
    /// are decrypted in-place when prefixed with `enc:v1:` (see
    /// `secret_crypto`); legacy plaintext rows pass through with a warning
    /// so the encryption rollout can be gradual.
    async fn load_storage_configs(pool: &PgPool) -> Result<HashMap<(String, String), StorageConfig>> {
        let configs: Vec<StorageConfig> = sqlx::query_as(
            "SELECT project_id, table_name, s3_bucket, s3_prefix, s3_region,
             s3_access_key_id, s3_secret_access_key, s3_endpoint
             FROM timefusion_projects WHERE is_active = true",
        )
        .fetch_all(pool)
        .await?;

        let key_set = crate::secret_crypto::key_configured();
        let mut map = HashMap::new();
        let mut plaintext_rows = 0usize;
        for mut config in configs {
            let enc_access = config.s3_access_key_id.starts_with(crate::secret_crypto::ENC_PREFIX);
            let enc_secret = config.s3_secret_access_key.starts_with(crate::secret_crypto::ENC_PREFIX);
            match crate::secret_crypto::decrypt_or_passthrough(&config.s3_access_key_id) {
                Ok(v) => config.s3_access_key_id = v,
                Err(e) => {
                    error!("Skipping {}/{}: cannot decrypt s3_access_key_id: {}", config.project_id, config.table_name, e);
                    continue;
                }
            }
            match crate::secret_crypto::decrypt_or_passthrough(&config.s3_secret_access_key) {
                Ok(v) => config.s3_secret_access_key = v,
                Err(e) => {
                    error!("Skipping {}/{}: cannot decrypt s3_secret_access_key: {}", config.project_id, config.table_name, e);
                    continue;
                }
            }
            if !(enc_access && enc_secret) {
                plaintext_rows += 1;
            }
            debug!("Loaded config: {}/{}", config.project_id, config.table_name);
            map.insert((config.project_id.clone(), config.table_name.clone()), config);
        }
        if plaintext_rows > 0 {
            warn!(
                "{} timefusion_projects row(s) hold AWS credentials in plaintext. Re-encrypt with `timefusion encrypt-secret <value>` and UPDATE the row.",
                plaintext_rows
            );
        }
        info!("Loaded {} storage configs from timefusion_projects (encryption key: {})", map.len(), if key_set { "configured" } else { "NOT configured" });
        Ok(map)
    }

    async fn initialize_cache_with_retry(cfg: &AppConfig) -> Option<Arc<SharedFoyerCache>> {
        // Check if cache is disabled
        if cfg.cache.is_disabled() {
            info!("Foyer cache is disabled via TIMEFUSION_FOYER_DISABLED");
            return None;
        }

        let foyer_config = FoyerCacheConfig::from_app_config(cfg);
        info!(
            "Initializing shared Foyer hybrid cache (memory: {}MB, disk: {}GB, TTL: {}s)",
            foyer_config.memory_size_bytes / 1024 / 1024,
            foyer_config.disk_size_bytes / 1024 / 1024 / 1024,
            foyer_config.ttl.as_secs()
        );

        for attempt in 1..=3 {
            match SharedFoyerCache::new(foyer_config.clone()).await {
                Ok(cache) => {
                    info!("Shared Foyer cache initialized successfully for all tables");
                    return Some(Arc::new(cache));
                }
                Err(e) if attempt < 3 => {
                    warn!("Failed to initialize shared Foyer cache (attempt {}/3): {}. Retrying...", attempt, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("Failed to initialize shared Foyer cache after 3 retries: {}. Continuing without cache.", e);
                    return None;
                }
            }
        }
        None
    }

    /// Create a new Database with explicit config.
    /// Prefer this over `new()` for better testability.
    pub async fn with_config(cfg: Arc<AppConfig>) -> Result<Self> {
        // Active tables rewrite their snapshot every flush; week-stale files
        // belong to dropped/idle tables and would otherwise accumulate forever.
        crate::snapshot_cache::prune_stale(&Self::delta_snapshot_dir(&cfg), crate::snapshot_cache::SNAPSHOT_MAX_AGE);
        let dedup_dirty_bins = Arc::new(dashmap::DashMap::new());
        for bin in crate::dirty_bin_queue::load(&cfg.core.timefusion_data_dir) {
            dedup_dirty_bins.insert((bin.project_id, bin.table_name, bin.date, bin.bin), ());
        }
        crate::metrics::maintenance_stats().dirty_bin_queue_depth.store(dedup_dirty_bins.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let aws_endpoint = &cfg.aws.aws_s3_endpoint;
        let aws_url = Url::parse(aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

        // Store default S3 settings for unconfigured mode
        let default_s3_bucket = cfg.aws.aws_s3_bucket.clone();
        let default_s3_prefix = cfg.core.timefusion_table_prefix.clone();
        let default_s3_endpoint = Some(aws_endpoint.clone());

        // Try to connect to config database if URL is provided
        let (config_pool, storage_configs) = match &cfg.core.timefusion_config_database_url {
            Some(db_url) => match PgPoolOptions::new().max_connections(2).connect(db_url).await {
                Ok(pool) => {
                    if let Err(e) = Self::ensure_storage_configs_schema(&pool).await {
                        warn!("Could not ensure timefusion_projects schema (continuing — table may already exist): {}", e);
                    }
                    let configs = Self::load_storage_configs(&pool).await.unwrap_or_default();
                    (Some(pool), configs)
                }
                Err(e) => {
                    warn!("Could not connect to config database, falling back to default mode (custom project routing disabled): {}", e);
                    (None, HashMap::new())
                }
            },
            None => (None, HashMap::new()),
        };

        // Initialize object store cache BEFORE creating any tables
        // This ensures all tables benefit from caching
        let object_store_cache = Self::initialize_cache_with_retry(&cfg).await;

        // Initialize statistics extractor with configurable cache size
        let stats_cache_size = cfg.parquet.timefusion_stats_cache_size;
        let page_row_limit = cfg.parquet.timefusion_page_row_count_limit;
        let statistics_extractor = Arc::new(DeltaStatisticsExtractor::new(stats_cache_size, 300, page_row_limit));

        // Captured before `cfg` is moved into the struct literal below.
        let maint_rewrite_permits = cfg.derived.rewrite_permits().max(1);
        let light_rewrite_permits = cfg.derived.max_light_optimize_k().max(1);
        let dml_merge_permits = cfg.maintenance.timefusion_dml_merge_concurrency.max(1);
        let heavy_scan_permits = cfg.memory.timefusion_max_concurrent_scan_readers.max(1);
        // Each concurrent spilling sort needs a workable slice of the shared
        // FairSpillPool or its merge phase starves — see `flush_sort_gate`.
        const MIN_SPILL_SORT_BYTES: usize = 512 << 20;
        let flush_sort_permits = (cfg.maintenance.flush_sort_pool_bytes() / MIN_SPILL_SORT_BYTES).max(1);
        let maintenance_shutdown = CancellationToken::new();
        let maintenance_cancel_guard = Arc::new(maintenance_shutdown.clone().drop_guard());
        let logical_count_cache = Arc::new(crate::logical_count_index::LogicalCountCache::new(
            cfg.core.timefusion_data_dir.join("logical_count"),
            cfg.derived.logical_count_memory_bytes(),
        ));
        let db = Self {
            config: cfg,
            runtime_env: Arc::new(std::sync::OnceLock::new()),
            maintenance_runtime_env: Arc::new(std::sync::OnceLock::new()),
            light_optimize_runtime_env: Arc::new(std::sync::OnceLock::new()),
            flush_sort_gate: Arc::new(tokio::sync::Semaphore::new(flush_sort_permits)),
            flush_sort_runtime_env: Arc::new(std::sync::OnceLock::new()),
            maintenance_session_state: Arc::new(std::sync::OnceLock::new()),
            light_optimize_session_state: Arc::new(std::sync::OnceLock::new()),
            unified_tables: Arc::new(RwLock::new(HashMap::new())),
            custom_project_tables: Arc::new(RwLock::new(HashMap::new())),
            fast_resolve_cache: Arc::new(dashmap::DashMap::new()),
            delta_has_files: dashmap::DashMap::new(),
            delta_provider_cache: Arc::new(dashmap::DashMap::new()),
            scan_metrics: Arc::new(ScanMetrics::default()),
            batch_queue: None,
            maintenance_shutdown: Arc::new(maintenance_shutdown),
            _maintenance_cancel_guard: Some(maintenance_cancel_guard),
            preload_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config_pool,
            storage_configs: Arc::new(RwLock::new(storage_configs)),
            storage_configs_next_refresh_ns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            default_s3_bucket: default_s3_bucket.clone(),
            default_s3_prefix: Some(default_s3_prefix.clone()),
            default_s3_endpoint,
            object_store_cache,
            statistics_extractor,
            last_written_versions: Arc::new(RwLock::new(HashMap::new())),
            last_dedup_versions: Arc::new(RwLock::new(HashMap::new())),
            dedup_clean_fp: Arc::new(dashmap::DashMap::new()),
            logical_count_cache,
            logical_count_building: Arc::new(dashmap::DashSet::new()),
            // A build retains one winner per logical key. Serial construction
            // is deliberate at production cardinality; query execution and
            // the other cache tiers remain concurrent.
            logical_count_build_sem: Arc::new(tokio::sync::Semaphore::new(1)),
            dedup_dirty_bins,
            dedup_backoff: Arc::new(dashmap::DashMap::new()),
            maintenance_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(maint_rewrite_permits)),
            light_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(light_rewrite_permits)),
            dml_merge_sem: Arc::new(tokio::sync::Semaphore::new(dml_merge_permits)),
            heavy_scan_sem: Arc::new(tokio::sync::Semaphore::new(heavy_scan_permits)),
            maintenance_job_sem: Arc::new(tokio::sync::Semaphore::new(1)),
            commit_locks: Arc::new(dashmap::DashMap::new()),
            flush_waiter_counts: Arc::new(dashmap::DashMap::new()),
            dml_locks: Arc::new(dashmap::DashMap::new()),
            snapshot_persist_gate: Arc::new(dashmap::DashMap::new()),
            buffered_layer: Arc::new(std::sync::OnceLock::new()),
            bypass_buffer: false,
            tantivy_search: Arc::new(std::sync::OnceLock::new()),
            tantivy_indexer: Arc::new(std::sync::OnceLock::new()),
            dml_coalescer: Arc::new(std::sync::OnceLock::new()),
            zorder_filesets: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_versions: Arc::new(dashmap::DashMap::new()),
            light_optimize_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            staged_intent_manifest_lock: Arc::new(std::sync::Mutex::new(())),
        };

        Ok(db)
    }

    /// Create a new Database using global config (for production).
    /// For tests, prefer `with_config()` to pass config explicitly.
    pub async fn new() -> Result<Self> {
        let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;
        // Convert &'static to Arc - it's fine since static lives forever
        // We clone the config to create an owned Arc
        let cfg_arc = Arc::new(cfg.clone());
        Self::with_config(cfg_arc).await
    }

    /// Set the batch queue to use for insert operations
    pub fn with_batch_queue(mut self, batch_queue: Arc<crate::batch_queue::BatchQueue>) -> Self {
        self.batch_queue = Some(batch_queue);
        self
    }

    /// Set the buffered write layer for WAL + in-memory buffer. Publishes to
    /// every existing clone (shared OnceLock) — set-once; a second call is a
    /// no-op.
    pub fn with_buffered_layer(self, layer: Arc<crate::buffered_write_layer::BufferedWriteLayer>) -> Self {
        let _ = self.buffered_layer.set(layer);
        self
    }

    /// Get the buffered write layer if configured
    pub fn buffered_layer(&self) -> Option<&Arc<crate::buffered_write_layer::BufferedWriteLayer>> {
        if self.bypass_buffer { None } else { self.buffered_layer.get() }
    }

    /// The deferred-DML coalescer, when enabled (see `start_dml_coalescer`).
    pub fn dml_coalescer(&self) -> Option<&Arc<crate::dml_coalescer::DmlCoalescer>> {
        self.dml_coalescer.get()
    }

    /// Start the DML coalescer + its background drain task when
    /// `TIMEFUSION_DML_COALESCE_SECS > 0`. Idempotent (shared OnceLock).
    /// The drain loop stops — after one final drain — on the same
    /// cancellation token as the maintenance tasks.
    pub fn start_dml_coalescer(&self) {
        let secs = self.config.buffer.dml_coalesce_secs();
        if secs == 0 {
            return;
        }
        let coalescer = Arc::new(crate::dml_coalescer::DmlCoalescer::new(secs, self.config.buffer.dml_coalesce_fold()));
        if self.dml_coalescer.set(coalescer.clone()).is_ok() {
            tokio::spawn(coalescer.run(self.background_clone(), (*self.maintenance_shutdown).clone()));
        }
    }

    /// Attach the tantivy search service used by the scan-side prefilter.
    /// Publishes to every existing clone (shared OnceLock, set-once).
    pub fn with_tantivy_search(self, svc: Arc<crate::tantivy_index::search::TantivySearchService>) -> Self {
        let _ = self.tantivy_search.set(svc);
        self
    }

    pub fn tantivy_search(&self) -> Option<&Arc<crate::tantivy_index::search::TantivySearchService>> {
        self.tantivy_search.get()
    }

    /// Attach the write-side tantivy service. Used by the compaction-GC hook
    /// in `optimize_table` to clean up stale sidecar indexes after files are
    /// rewritten away. Publishes to every existing clone (shared OnceLock).
    pub fn with_tantivy_indexer(self, svc: Arc<crate::tantivy_index::service::TantivyIndexService>) -> Self {
        let _ = self.tantivy_indexer.set(svc);
        self
    }

    pub fn tantivy_indexer(&self) -> Option<&Arc<crate::tantivy_index::service::TantivyIndexService>> {
        self.tantivy_indexer.get()
    }

    /// Startup backfill (gated on `timefusion_tantivy_backfill`): build
    /// partition-mirrored indexes for live parquet files that no successful
    /// manifest entry covers, oldest partition first. Every covered file
    /// widens the windows where the coverage gate lets the prefilter engage
    /// (pre-tantivy history, failed builds, pre-reindex compactions).
    pub fn spawn_tantivy_backfill(&self) {
        let Some(svc) = self.tantivy_indexer().cloned() else { return };
        if !self.config.tantivy.timefusion_tantivy_backfill {
            return;
        }
        let db = self.clone();
        tokio::spawn(async move {
            for table_name in svc.config.indexed_tables() {
                match db.backfill_table_indexes(&svc, &table_name).await {
                    Ok(0) => {}
                    Ok(n) => info!("tantivy backfill: table={} built={}", table_name, n),
                    Err(e) => warn!("tantivy backfill failed for {}: {}", table_name, e),
                }
            }
        });
    }

    /// Rebuild only files deferred by WAL replay, after replay has completed.
    /// The on-disk queue is removed entry-by-entry only after a successful
    /// build, so a second restart resumes the remaining work.
    pub fn spawn_deferred_tantivy_reindex(self: &Arc<Self>, layer: Arc<crate::buffered_write_layer::BufferedWriteLayer>) {
        let Some(svc) = self.tantivy_indexer().cloned() else { return };
        if layer.deferred_tantivy_files().is_empty() {
            return;
        }
        let db = Arc::clone(self);
        tokio::spawn(async move {
            for file in layer.deferred_tantivy_files() {
                let table_ref = match db.resolve_table("default", &file.table_name).await {
                    Ok(table) => Ok(table),
                    Err(_) => db.resolve_table(&file.project_id, &file.table_name).await,
                };
                let result = async {
                    let table = table_ref?;
                    let store = table.read().await.log_store().object_store(None);
                    let rel = crate::tantivy_index::service::parquet_rel_of_uri(&file.uri)
                        .ok_or_else(|| anyhow::anyhow!("invalid deferred parquet URI {}", file.uri))?;
                    svc.build_index_for_file(&file.table_name, &file.project_id, rel, &file.uri, store).await
                }
                .await;
                match result {
                    Ok(()) => layer.complete_deferred_tantivy_file(&file),
                    Err(e) => warn!("tantivy recovery reindex failed for {}/{} {}: {e:#}", file.project_id, file.table_name, file.uri),
                }
            }
        });
    }

    /// Startup cache warmer (gated on `timefusion_tantivy_prefetch_days`):
    /// pull recent index blobs into the local disk cache in the background.
    pub fn spawn_tantivy_prefetch(&self) {
        let days = self.config.tantivy.timefusion_tantivy_prefetch_days;
        let Some(search) = self.tantivy_search().cloned() else { return };
        if days == 0 {
            return;
        }
        let tables = self.config.tantivy.indexed_tables();
        tokio::spawn(async move {
            for t in tables {
                match search.warm_recent(&t, days).await {
                    Ok(0) => {}
                    Ok(n) => info!("tantivy prefetch: table={} blobs_warmed={}", t, n),
                    Err(e) => warn!("tantivy prefetch failed for {}: {}", t, e),
                }
            }
        });
    }

    /// Synchronous backfill + GC for one table — the optimize CLI's
    /// post-compaction reconcile. Builds indexes for every live parquet not
    /// covered by a manifest (repairing earlier CLI runs that compacted with
    /// no indexer attached, and wave commits which carry no tantivy hook),
    /// then prunes manifest entries and blobs whose covered files are gone.
    /// Returns (indexes_built, manifest_entries_removed, blobs_deleted).
    pub async fn tantivy_reconcile_table(&self, table_name: &str) -> anyhow::Result<(usize, usize, usize)> {
        let Some(svc) = self.tantivy_indexer().cloned() else { return Ok((0, 0, 0)) };
        if !svc.config.is_table_indexed(table_name) {
            return Ok((0, 0, 0));
        }
        // GC FIRST, then backfill. Stale entries only ever cover dead files, so
        // pruning them cannot regress live-file coverage — and it must not be
        // hostage to the build phase (a memory-tight runner that OOMs on its
        // first oversized parquet would otherwise never GC anything). This is
        // the standalone-reconcile ordering only; the post-optimize hook keeps
        // build-outputs-before-GC-inputs. GC every manifest that exists (keyed
        // by project uuid at build time) — a fixed "default"+customs list never
        // visits unified tenants.
        let (mut removed, mut blobs) = (0usize, 0usize);
        for pid in crate::tantivy_index::manifest::list_projects(svc.object_store.as_ref(), table_name).await? {
            let Ok(table_ref) = self.resolve_table(&pid, table_name).await else { continue };
            let live_uris: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            let report = svc.gc_after_compaction(table_name, &pid, &live_uris).await?;
            if report.entries_removed > 0 || report.blob_delete_errors > 0 {
                info!(
                    "tantivy reconcile gc: table={table_name} project={pid} entries_removed={} blobs_deleted={} delete_errors={}",
                    report.entries_removed, report.blobs_deleted, report.blob_delete_errors
                );
            }
            removed += report.entries_removed;
            blobs += report.blobs_deleted;
        }
        let built = self.backfill_table_indexes(&svc, table_name).await?;
        Ok((built, removed, blobs))
    }

    async fn backfill_table_indexes(&self, svc: &Arc<crate::tantivy_index::service::TantivyIndexService>, table_name: &str) -> anyhow::Result<usize> {
        use crate::tantivy_index::{
            manifest,
            service::{parquet_rel_of_uri, project_id_of_uri},
        };
        // Unified table ("default") holds every default-routed project's
        // files; custom project tables are resolved separately.
        let mut roots: Vec<String> = vec!["default".into()];
        roots.extend(self.custom_project_tables.read().await.keys().filter(|(_, t)| t == table_name).map(|(p, _)| p.clone()));
        let mut built = 0usize;
        for root in roots {
            let Ok(table_ref) = self.resolve_table(&root, table_name).await else {
                continue;
            };
            let (uris, sizes, delta_store) = {
                let t = table_ref.read().await;
                let sizes: HashMap<String, u64> = match t.snapshot() {
                    Ok(s) => s.log_data().iter().map(|f| (f.path().into_owned(), f.size() as u64)).collect(),
                    Err(_) => HashMap::new(),
                };
                (t.get_file_uris()?.collect::<Vec<String>>(), sizes, t.log_store().object_store(None))
            };
            // Group live files by owning project (partition segment).
            let mut by_pid: HashMap<String, Vec<String>> = HashMap::new();
            for u in uris.into_iter().filter(|u| u.ends_with(".parquet")) {
                if let Some(pid) = project_id_of_uri(&u) {
                    by_pid.entry(pid.to_string()).or_default().push(u);
                }
            }
            let max_bytes = self.config.tantivy.timefusion_tantivy_backfill_max_file_mb * 1024 * 1024;
            for (pid, mut uris) in by_pid {
                let m = manifest::load(svc.object_store.as_ref(), table_name, &pid).await?;
                let covered: std::collections::HashSet<&String> =
                    m.entries.values().filter(|e| e.index.is_some() && e.error.is_none()).flat_map(|e| e.covered_files.iter()).collect();
                uris.retain(|u| !covered.contains(u));
                if max_bytes > 0 {
                    let before = uris.len();
                    uris.retain(|u| parquet_rel_of_uri(u).and_then(|rel| sizes.get(rel)).is_none_or(|sz| *sz <= max_bytes));
                    let skipped = before - uris.len();
                    if skipped > 0 {
                        // No silent caps: oversized files stay uncovered until a
                        // bigger-memory runner reconciles without the limit.
                        warn!(table_name, project_id = %pid, skipped, "tantivy backfill skipped files over TIMEFUSION_TANTIVY_BACKFILL_MAX_FILE_MB");
                    }
                }
                uris.sort(); // lexical == chronological for date= partitions
                let work: Vec<(String, String)> = uris.iter().filter_map(|uri| Some((parquet_rel_of_uri(uri)?.to_string(), uri.clone()))).collect();
                let table_owned = table_name.to_string();
                let mut jobs = futures::stream::iter(work.into_iter().map(|(rel, uri)| {
                    let (svc, store, pid, table) = (svc.clone(), delta_store.clone(), pid.clone(), table_owned.clone());
                    async move { svc.build_index_for_file(&table, &pid, &rel, &uri, store).await }
                }))
                .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1));
                while let Some(r) = jobs.next().await {
                    match r {
                        Ok(()) => built += 1,
                        Err(e) => warn!("tantivy backfill build failed table={} project={}: {}", table_name, pid, e),
                    }
                }
            }
        }
        Ok(built)
    }

    /// Query Delta tables directly, bypassing the in-memory buffer (for testing).
    pub async fn query_delta_only(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut db_clone = self.clone();
        db_clone.bypass_buffer = true;
        let db_arc = Arc::new(db_clone);
        let mut ctx = Arc::clone(&db_arc).create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db_arc.setup_session_context(&mut ctx)?;
        Ok(ctx.sql(sql).await?.collect().await?)
    }

    /// Enable object store cache with foyer (deprecated - cache is now initialized in new())
    /// This method is kept for backward compatibility but is now a no-op
    pub async fn with_object_store_cache(self) -> Result<Self> {
        // Cache is now initialized in new(), so this is a no-op
        Ok(self)
    }

    /// Start background maintenance schedulers for optimize and vacuum operations
    pub async fn start_maintenance_schedulers(self) -> Result<Self> {
        let db = Arc::new(self.background_clone());
        let cancel = self.maintenance_shutdown.clone();

        // Always-on pressure sampling. `scan_pressure_permits` samples lazily
        // (on gated decode polls), so a climb driven by anything OTHER than a
        // wide scan — flush sorts, replay, maintenance — reached the OOM kill
        // with zero tier transitions logged and a stale tier for the first
        // gated poll that DID arrive (2026-08-03 04:36 kill: no valve line in
        // the whole 24-min life). A 250ms heartbeat keeps the tier fresh and
        // the transition log complete no matter who is allocating.
        {
            let total = self.config.memory.timefusion_max_concurrent_scan_readers as u32;
            let cancel = cancel.clone();
            tokio::spawn(async move {
                while !cancel.is_cancelled() {
                    let _ = scan_pressure_permits(total);
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
            });
        }

        // Delete staged parquet left by an interrupted wave after readiness.
        // This is best-effort derived-data cleanup, not recovery: serial R2
        // DELETE latency must not hold PGWire in 57P03. The manifest rewrite is
        // serialized with new appends, and the minimum-age gate excludes work
        // staged by this process, so cleanup remains safe if a cron tick starts
        // concurrently. `all_tables` includes custom-storage tables too.
        {
            let cleanup_db = Arc::clone(&db);
            tokio::spawn(async move {
                for (_project_id, table_name, table) in cleanup_db.all_tables().await {
                    if cleanup_db.maintenance_shutdown.is_cancelled() {
                        return;
                    }
                    cleanup_db.reconcile_staged_intents(&table, &table_name).await;
                }
            });
        }

        // Hot compact — bin-pack today's small files (every ~5 min). Runs WITHOUT
        // the maintenance_job_sem so it can't be starved behind the dedup backlog
        // or a long full-optimize: prod 2026-07-20 showed the busy project only
        // got compacted ~every 40 min because dedup churning an old-date backlog
        // wedged the shared serial pass >600s. Compaction touches only today; dedup
        // skips today — disjoint partitions, so decoupling is safe. Peak heap stays
        // bounded by the wave engine's OWN light_rewrite_sem (+ the light pool
        // slice it is sized from), never the heavy maintenance_rewrite_sem:
        // sharing that one re-couples the two engines (prod 2026-07-30).
        spawn_cron_job("Hot compact", &self.config.maintenance.timefusion_light_optimize_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    if !db.config.maintenance.timefusion_light_optimize_enabled {
                        return;
                    }
                    info!("Running scheduled hot-tail compaction on today's small files");
                    for (project_id, table_name, table) in db.all_tables().await {
                        if db.maintenance_shutdown.is_cancelled() {
                            return;
                        }
                        db.run_hot_compact_for_table(&table, &table_name, &Self::table_label(&project_id, &table_name)).await;
                    }
                    // Hot compaction only ever touches TODAY's partition, so a
                    // sealed day the daily cold sweep failed to finish stays
                    // fragmented forever. Piggy-back a bounded catch-up slice on
                    // this tick, which actually runs often enough to converge.
                    let passes = db.config.maintenance.timefusion_consolidate_catchup_passes;
                    for (_, table_name, table) in db.all_tables().await {
                        if db.maintenance_shutdown.is_cancelled() || passes == 0 {
                            return;
                        }
                        if let Err(e) = db.consolidate_catchup(&table, &table_name, passes).await {
                            warn!("consolidate-catchup failed for '{}': {}", table_name, e);
                        }
                    }
                }
            }
        });

        // Dedup — collapse duplicates in sealed (< today) partitions on its own
        // cron, decoupled from hot compaction above. Keeps the job_sem so it stays
        // serialized against the full optimize job (the other job_sem holder).
        // spawn_cron_job skips overlapping ticks.
        spawn_cron_job("Dedup", &self.config.maintenance.timefusion_dedup_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    let Ok(_maintenance_job) = db.maintenance_job_sem.clone().acquire_owned().await else {
                        return;
                    };
                    info!("Running scheduled dedup on sealed partitions");
                    for (project_id, table_name, table) in db.all_tables().await {
                        if db.maintenance_shutdown.is_cancelled() {
                            return;
                        }
                        // Dedup key: bare table name for unified tables, tenant-scoped
                        // for custom-storage ones (they are separate Delta logs).
                        let key = if project_id.is_empty() { table_name.clone() } else { format!("{project_id}:{table_name}") };
                        db.run_dedup_for_table(&table, &table_name, &key, &Self::table_label(&project_id, &table_name)).await;
                    }
                }
            }
        });

        // Full optimize — window-wide compaction (every ~30 min; Compact, see optimize_table).
        spawn_cron_job("Optimize", &self.config.maintenance.timefusion_optimize_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    let Ok(_maintenance_job) = db.maintenance_job_sem.clone().acquire_owned().await else {
                        return;
                    };
                    info!("Running scheduled optimize on all tables");
                    for (project_id, table_name, table) in db.all_tables().await {
                        if let Err(e) = db.optimize_table(&table, &table_name, None).await {
                            error!("Optimize failed for {}: {}", Self::table_label(&project_id, &table_name), e);
                        }
                    }
                }
            }
        });

        // Consolidate — daily cold sweep bin-packing sealed partitions (older than
        // cold_optimize_after_days) to the 512MB cold target, beyond the 48h warm window.
        spawn_cron_job("Consolidate", &self.config.maintenance.timefusion_consolidate_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    info!("Running scheduled cold consolidation on sealed partitions");
                    let mut targets: Vec<(String, Arc<RwLock<DeltaTable>>)> =
                        db.unified_tables.read().await.iter().map(|(n, t)| (n.clone(), t.clone())).collect();
                    targets.extend(db.custom_project_tables.read().await.iter().map(|((_, n), t)| (n.clone(), t.clone())));
                    for (name, table) in &targets {
                        if let Err(e) = db.consolidate_sealed_partitions(table, name).await {
                            error!("Consolidate (cold tier) failed for '{}': {}", name, e);
                        }
                    }
                }
            }
        });

        // Recompress — daily tier upgrade for cold (14d+). Skips partitions whose
        // probe file already advertises the target tier, so re-runs are cheap.
        let cold_cutoff = self.config.parquet.timefusion_cold_cutoff_days;
        let zstd_cold = self.config.parquet.timefusion_zstd_level_cold;
        // Cold sweep upper bound — older partitions fall under vacuum.
        let cold_upper = (self.config.maintenance.timefusion_vacuum_retention_hours / 24).max(cold_cutoff + 60);
        spawn_cron_job("Recompress", &self.config.maintenance.timefusion_recompress_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    info!("Running scheduled tier recompression (warm→cold@{}d zstd={})", cold_cutoff, zstd_cold);
                    let mut targets: Vec<(String, Arc<RwLock<DeltaTable>>)> =
                        db.unified_tables.read().await.iter().map(|(n, t)| (n.clone(), t.clone())).collect();
                    targets.extend(db.custom_project_tables.read().await.iter().map(|((_, n), t)| (n.clone(), t.clone())));
                    for (name, table) in &targets {
                        if let Err(e) = db.recompress_tier_window(table, name, cold_cutoff, cold_upper, zstd_cold).await {
                            error!("Recompress (cold tier) failed for '{}': {}", name, e);
                        }
                    }
                }
            }
        });

        // Vacuum — expired-file removal (default: daily at 2AM).
        let vacuum_retention = self.config.maintenance.timefusion_vacuum_retention_hours;
        spawn_cron_job("Vacuum", &self.config.maintenance.timefusion_vacuum_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    info!("Running scheduled vacuum on all tables");
                    for (project_id, table_name, table) in db.all_tables().await {
                        info!("Vacuuming {} (retention: {}h)", Self::table_label(&project_id, &table_name), vacuum_retention);
                        db.vacuum_table(&project_id, &table_name, &table, vacuum_retention).await;
                    }
                }
            }
        });

        // Tantivy reconcile — nightly index consistency: backfill every live
        // parquet no manifest covers (wave commits carry no index hook; failed
        // builds; emergency-CLI compactions), then GC entries for dead files
        // across every per-uuid manifest. Gated at tick time: the indexer is
        // attached after construction, and may be absent entirely (no indexed
        // tables / no bucket).
        spawn_cron_job("Tantivy reconcile", &self.config.maintenance.timefusion_tantivy_reconcile_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    let Some(svc) = db.tantivy_indexer().cloned() else { return };
                    for table_name in svc.config.indexed_tables() {
                        match db.tantivy_reconcile_table(&table_name).await {
                            Ok((0, 0, 0)) => {}
                            Ok((built, removed, blobs)) => {
                                info!("tantivy nightly reconcile: table={} built={} entries_removed={} blobs_deleted={}", table_name, built, removed, blobs);
                            }
                            Err(e) => warn!("tantivy nightly reconcile failed for {}: {}", table_name, e),
                        }
                    }
                }
            }
        });

        // Checkpoint + expired-log cleanup — runs the post-commit hooks out-of-band
        // (see the 2026-07-09 incident) so R2 500s on the checkpoint PUT / bulk log
        // delete never fail a landed commit; faster cadence keeps the log bounded.
        spawn_cron_job("Checkpoint", &self.config.maintenance.timefusion_checkpoint_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move { db.run_checkpoint_maintenance().await }
            }
        });

        // Reconcile — repair dangling Add entries (committed parquet deleted by a
        // past commit-path failure) by Remove'ing them via filesystem_check.
        spawn_cron_job("Reconcile", &self.config.maintenance.timefusion_reconcile_schedule, cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move { db.run_reconcile_maintenance().await }
            }
        });

        // Cache stats — every 5 minutes.
        spawn_cron_job("Cache stats", "0 */5 * * * *", cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    if let Some(ref cache) = db.object_store_cache {
                        cache.log_stats().await;
                    }
                    let (used, capacity) = db.statistics_extractor.get_cache_stats().await;
                    info!("Statistics cache: {}/{} entries used", used, capacity);
                }
            }
        });

        // Statistics refresh — every 15 minutes.
        spawn_cron_job("Statistics refresh", "0 */15 * * * *", cancel.clone(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    info!("Refreshing Delta Lake statistics cache");
                    db.statistics_extractor.clear_cache().await;
                    // Unified tables pre-warm under an empty project_id — they're shared.
                    for (project_id, table_name, table) in db.all_tables().await {
                        let label = Self::table_label(&project_id, &table_name);
                        let table = table.read().await;
                        let current_version = table.version().unwrap_or(0);
                        if let Err(e) = db.statistics_extractor.extract_statistics(&table, &project_id, &table_name).await {
                            error!("Failed to refresh statistics for {}: {}", label, e);
                        } else {
                            debug!("Refreshed statistics for {} (version {})", label, current_version);
                        }
                    }
                }
            }
        });

        // Each spawn_cron_job task exits on its own when `maintenance_shutdown`
        // fires (cancel_maintenance()), so no separate scheduler teardown is needed.
        Ok(self)
    }

    /// Create and configure a SessionContext with DataFusion settings
    pub fn create_session_context(self: Arc<Self>) -> SessionContext {
        use std::sync::Arc;

        use datafusion::{
            config::ConfigOptions,
            execution::{SessionStateBuilder, context::SessionContext},
        };
        use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};

        use crate::dml::DmlQueryPlanner;

        let mut options = ConfigOptions::new();
        let _ = options.set("datafusion.catalog.information_schema", "true");

        // INCIDENT 2026-07-31 (7d68f01): during the ~30 min that commit was live,
        // TF wrote parquet files into the LIVE `otel_logs_and_spans` table whose
        // physical schema had `timestamp` (and `id`) NULLABLE, while the YAML —
        // and therefore every logical plan — declares them NOT NULL. Those files
        // are permanent table content: the rollback restored the binary, but it
        // cannot un-write them, and no `metaData` action was ever committed so
        // the Delta schema itself is unchanged and there is nothing to "fix".
        //
        // DataFusion then rejects any aggregate grouping on such a column:
        //   Internal error: Physical input schema should be the same as the one
        //   converted from logical input schema. Differences: field nullability
        //   at index 0 [timestamp]: (physical) true vs (logical) false.
        // which took out every `GROUP BY time_bucket(timestamp)` dashboard on
        // every project (`GROUP BY status_code` was fine; `otel_metrics` was
        // unaffected). This flag exists for exactly that physical-vs-logical
        // nullability mismatch. Widening nullability is always SAFE to read: a
        // column declared NOT NULL simply has no nulls to observe.
        //
        // Keep it set until those files have aged out of retention or been
        // rewritten by compaction; removing it re-breaks the dashboards.
        let _ = options.set("datafusion.execution.skip_physical_aggregate_schema_check", "true");

        // Must be false: delta_kernel's unshredded_variant() schema uses Binary (not BinaryView).
        // Forcing view types causes UPDATE/DELETE rewrites to fail schema validation against variant columns.
        let _ = options.set("datafusion.execution.parquet.schema_force_view_types", "false");
        let _ = options.set("datafusion.sql_parser.map_string_types_to_utf8view", "true");
        // PostgreSQL dialect for ctx.sql() parsing. The default GenericDialect gives
        // the JSON `->`/`->>` operators precedence *below* `=` (PgOther 16 < Eq 20), so
        // `body->>'k'='v'` mis-parses as `body->>('k'='v')`. PostgreSQL binds them
        // *above* comparison (matching real Postgres + the pgwire fork's own parser),
        // so unparenthesized `col->>'k'='v'` works without the caller adding parens.
        let _ = options.set("datafusion.sql_parser.dialect", "postgresql");

        // Enable Parquet statistics for better query optimization with Delta Lake
        // These settings ensure DataFusion uses file and column statistics for pruning
        let _ = options.set("datafusion.execution.parquet.statistics_enabled", "page");
        let _ = options.set("datafusion.execution.parquet.pushdown_filters", "true");
        let _ = options.set("datafusion.execution.parquet.reorder_filters", "true");
        let _ = options.set("datafusion.execution.parquet.enable_page_index", "true");
        let _ = options.set("datafusion.execution.parquet.pruning", "true");
        let _ = options.set("datafusion.execution.parquet.skip_metadata", "false");
        // One-shot footer read sized to match `warm_footer`'s suffix range: the
        // Foyer metadata cache keys on (path, exact range), so the reader's
        // first fetch (size-hint..size) hits the entry the warm task populated.
        // Without this the reader does 8-byte-tail + metadata-range reads —
        // two sequential S3 RTTs on different keys that can never be pre-warmed
        // (measured 1.6 s of metadata_load_time on a cold OVH partition).
        let _ = options.set("datafusion.execution.parquet.metadata_size_hint", &self.config.cache.timefusion_parquet_metadata_size_hint.to_string());
        let _ = options.set("datafusion.explain.show_schema", "true");
        // NOTE: the decoded-metadata cache limit is NOT set here — a
        // `datafusion.runtime.*` SessionConfig string does not reconfigure an
        // already-built RuntimeEnv. It is applied on the RuntimeEnvBuilder
        // below via `build_query_runtime_env` instead.

        // Cap query parallelism at the container's CPU quota (derived in
        // autotune::apply; 0 = leave DataFusion's default). See MemoryConfig.
        if self.config.memory.timefusion_query_partitions > 0 {
            let _ = options.set("datafusion.execution.target_partitions", &self.config.memory.timefusion_query_partitions.to_string());
        }

        // Enable general statistics collection for query optimization.
        // (DataFusion default is `true` — set explicitly so a future default flip
        // doesn't silently regress query plans.)
        let _ = options.set("datafusion.execution.collect_statistics", "true");

        // Enable bloom filter pruning if available in Parquet files
        let _ = options.set("datafusion.execution.parquet.bloom_filter_on_read", "true");

        // Batch size = DataFusion's 8192 default. A prior 65536 (8×) was set for
        // "time-series throughput", but on the wide otel schema (body/attributes/
        // resource are KB-wide byte-view columns) it made every CoalesceBatchesExec
        // in-progress buffer hold 65536 wide rows — per partition, per concurrent
        // query — and that buffering is NOT pool-accounted. Heap profiling
        // (2026-07-05) showed InProgressByteViewArray::coalesce as the dominant
        // live consumer at 10-27GB. 8192 cuts the per-buffer footprint 8× for
        // negligible per-batch overhead on an IO-bound DB.
        let _ = options.set("datafusion.execution.batch_size", "8192");

        // Optimize for sorted data (timestamps are typically sorted)
        let _ = options.set("datafusion.optimizer.prefer_existing_sort", "true");

        // Enable repartition for better parallel aggregations
        let _ = options.set("datafusion.optimizer.repartition_aggregations", "true");

        // Disable round-robin repartitioning to maintain sort order
        let _ = options.set("datafusion.optimizer.enable_round_robin_repartition", "false");

        // Enable filter and limit pushdown optimizations
        let _ = options.set("datafusion.optimizer.filter_null_join_keys", "true");
        let _ = options.set("datafusion.optimizer.skip_failed_rules", "false");

        // Disable leaf-expression pushdown (DF54 extract_leaf_expressions /
        // push_down_leaf_projections). Those rules call
        // `Unnest::with_new_exprs(unnest.expressions(), …)` while routing
        // get_field (struct/map access) toward leaves, but `Unnest::expressions()`
        // returns its exec_columns whereas `with_new_exprs` asserts none — so any
        // multi-column UNNEST whose plan carries a get_field panics with
        // "Assertion failed: expr.is_empty()" (upstream DF bug). This hit prod via
        // monoscope's `UPDATE otel_logs_and_spans … FROM (SELECT unnest($1),
        // unnest($2), unnest($3)) u` dual-write. The rules only fire on get_field;
        // TF's Variant access uses the `variant_get` UDF (no MoveTowardsLeafNodes
        // placement), so disabling them does not affect Variant query plans.
        let _ = options.set("datafusion.optimizer.enable_leaf_expression_pushdown", "false");

        // Enable proper limit handling across partitions
        let _ = options.set("datafusion.optimizer.enable_distinct_aggregation_soft_limit", "true");
        let _ = options.set("datafusion.optimizer.enable_topk_aggregation", "true");

        // Memory management for large time-series queries
        let _ = options.set("datafusion.execution.coalesce_batches", "true");
        let _ = options.set("datafusion.execution.coalesce_target_batch_size", "8192");

        // Enable all optimizer rules for maximum optimization
        let _ = options.set("datafusion.optimizer.max_passes", "5");

        // Configure memory limit for DataFusion operations
        // datafusion.execution.memory_fraction is the per-query share of the (already
        // tree-sized) pool — a fixed 0.9 replaces the deleted TIMEFUSION_MEMORY_FRACTION
        // knob, whose prod value was calibrated against the old hand-set limit.
        let memory_fraction = 0.9;
        let sort_spill_reservation_bytes = self.config.memory.timefusion_sort_spill_reservation_bytes.unwrap_or(67_108_864);

        // Set memory-related configuration options
        let _ = options.set("datafusion.execution.memory_fraction", &memory_fraction.to_string());
        let _ = options.set("datafusion.execution.sort_spill_reservation_bytes", &sort_spill_reservation_bytes.to_string());

        let runtime_env = self.shared_runtime_env();

        // Set up tracing options with configurable sampling
        let record_metrics = self.config.memory.timefusion_tracing_record_metrics;

        // Cell-capped preview formatter — the default renders whole cell values;
        // see `telemetry::capped_preview_fn` for the 2026-07-06 OOM it prevents.
        let tracing_options =
            InstrumentationOptions::builder().record_metrics(record_metrics).preview_limit(5).preview_fn(Arc::new(crate::telemetry::capped_preview_fn)).build();

        let instrument_rule = instrument_with_info_spans!(options: tracing_options);

        // Create session state with tracing rule and DML support
        // Rule ordering: VariantInsertRewriter runs BEFORE TypeCoercion (rewrites string->json_to_variant)
        //                VariantSelectRewriter runs AFTER TypeCoercion (wraps Variant cols with variant_to_json)
        let analyzer_rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> = vec![
            Arc::new(datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction::new()),
            Arc::new(crate::optimizers::VariantInsertRewriter),
            // Tantivy predicate rewriter runs BEFORE TypeCoercion so the
            // injected `text_match(col, lit)` calls get coerced like any
            // other UDF args (Utf8 vs Utf8View etc).
            Arc::new(crate::optimizers::TantivyPredicateRewriter::new(self.config.tantivy.route_equality())),
            // Expands `f(qualifier.*)` into `f(qualifier.c1, …, qualifier.cN)`
            // before TypeCoercion rejects the typeless wildcard. Postgres parity.
            Arc::new(crate::optimizers::WildcardFnArgExpander),
            // PG parity: `COALESCE(list_col, '{}')` — re-type PG array string
            // literals as list literals before TypeCoercion fails the call.
            Arc::new(crate::optimizers::PgArrayLiteralRewriter),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
            Arc::new(crate::optimizers::VariantSelectRewriter),
        ];

        let session_state = SessionStateBuilder::new()
            .with_config(options.into())
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            // Appended after DataFusion's defaults so push_down_limit has
            // already folded LIMIT into Sort.fetch — see the rule's docs.
            .with_optimizer_rule(Arc::new(crate::optimizers::DeferExpensiveProjection))
            // Must run LAST: re-restores Variant scan types that
            // optimize_projections reverts to Utf8View when it rebuilds each
            // TableScan from the lying provider schema — see the rule's docs
            // (fixes XX000 on `DISTINCT ON` over Variant columns).
            .with_optimizer_rule(Arc::new(crate::optimizers::VariantScanSchemaRestore))
            // Physical rules: start from DataFusion's defaults, splice our
            // mem∪delta union-ordering rule in *before* EnforceDistribution so
            // the built-in EnforceDistribution/EnforceSorting do the
            // SortPreservingMerge insertion + redundant-sort removal for us
            // (turns `ORDER BY timestamp DESC LIMIT n` into a streaming,
            // early-terminating TopK). Tracing instrument rule stays last.
            .with_physical_optimizer_rules({
                let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
                let pos = rules.iter().position(|r| r.name() == "EnforceDistribution").unwrap_or(0);
                rules.insert(pos, Arc::new(crate::optimizers::OrderedUnionForTopK));
                rules.push(instrument_rule);
                rules
            })
            // The planner resolves the buffered layer at plan time (late-
            // binding): sessions are created during boot before the layer
            // exists.
            .with_query_planner(Arc::new(DmlQueryPlanner::new(self.clone())))
            // PG parity: resolve `'<path>'::jsonpath` casts to Utf8 so the path
            // literal reaches jsonb_path_exists as text (covers simple + extended).
            .with_type_planner(Arc::new(crate::functions::JsonPathTypePlanner))
            .build();

        SessionContext::new_with_state(session_state)
    }

    /// Register UDFs only — safe to call before `with_buffered_layer`.
    pub fn setup_session_udfs(&self, ctx: &mut SessionContext) -> DFResult<()> {
        self.register_set_config_udf(ctx);
        // CRITICAL: Register custom functions BEFORE JSON functions to ensure VariantAwareExprPlanner
        // intercepts -> and ->> operators on Variant columns before JsonExprPlanner handles them as strings
        crate::functions::register_custom_functions(ctx).map_err(|e| DataFusionError::Execution(format!("Failed to register custom functions: {}", e)))?;
        self.register_json_functions(ctx);
        Ok(())
    }

    /// Register routing + stats + pg_settings tables. Depends on `self.buffered_layer`
    /// being set (stats table holds an Arc to it).
    pub fn setup_session_tables(&self, ctx: &mut SessionContext) -> DFResult<()> {
        use crate::schema_loader::registry;

        let batch_queue = self.batch_queue.as_ref().map(Arc::clone);
        let registry = registry();
        for table_name in registry.list_tables() {
            if let Some(schema) = registry.get(&table_name) {
                let routing_table =
                    ProjectRoutingTable::new("default".to_string(), Arc::new(self.clone()), schema.schema_ref(), batch_queue.clone(), table_name.clone());
                ctx.register_table(&table_name, Arc::new(routing_table))?;
                info!("Registered ProjectRoutingTable for table '{}' with SessionContext", table_name);

                // Bulk-write alias: `INSERT INTO {table}__bulk ...` commits
                // straight to Delta (skip_queue), bypassing WAL + MemBuffer, so a
                // backfill / DLQ drain can't pressure the live buffer. The session
                // context is shared across connections (see pgwire_handlers), so a
                // per-connection GUC can't isolate the bulk writer — a dedicated
                // table name is how a client opts into the direct path. Internal
                // `table_name` stays the real table, so writes and reads both hit
                // the same Delta table.
                let bulk_table =
                    ProjectRoutingTable::new("default".to_string(), Arc::new(self.clone()), schema.schema_ref(), batch_queue.clone(), table_name.clone())
                        .with_skip_queue(true);
                ctx.register_table(format!("{table_name}__bulk"), Arc::new(bulk_table))?;
            }
        }

        // Register the introspection table. `SELECT * FROM timefusion_stats`
        // returns a flat (component, key, value) snapshot of MemBuffer / WAL /
        // BufferedWriteLayer counters — see src/stats_table.rs.
        // DashMap::clone is cheap (Arc bump on internal shard storage) and
        // shares the live state with `self` — the closure observes inserts
        // happening after registration, not a snapshot taken now.
        let fr_handle = self.fast_resolve_cache.clone();
        let dp_handle = self.delta_provider_cache.clone();
        // Provider count, not key count: a key holds a small version ring, and
        // the provider total is what tracks retained heap.
        let cache_sizes: crate::stats_table::CacheSizeSnapshot = Arc::new(move || (fr_handle.len(), dp_handle.iter().map(|e| e.value().len()).sum()));
        let foyer = self.object_store_cache.clone();
        let foyer_stats: crate::stats_table::FoyerStatsSnapshot =
            Arc::new(move || foyer.as_ref().map_or_else(crate::object_store_cache::FoyerRuntimeStats::default, |cache| cache.runtime_stats()));
        ctx.register_table(
            "timefusion_stats",
            Arc::new(
                crate::stats_table::StatsTableProvider::new(self.buffered_layer().cloned())
                    .with_scan_metrics(self.scan_metrics.clone())
                    .with_cache_sizes(cache_sizes)
                    .with_foyer_stats(foyer_stats)
                    .with_logical_count({
                        let cache = Arc::clone(&self.logical_count_cache);
                        let building = Arc::clone(&self.logical_count_building);
                        Arc::new(move || {
                            let (entries, resident, limit) = cache.stats();
                            (entries, resident, limit, building.len())
                        })
                    })
                    .with_query_pool({
                        let env = self.shared_runtime_env();
                        let size = self.config.derived.query_pool_bytes();
                        Arc::new(move || (env.memory_pool.reserved(), size))
                    }),
            ),
        )?;

        self.register_pg_settings_table(ctx)?;
        Ok(())
    }

    /// Setup the session context with both UDFs and tables. Preserves the legacy
    /// table-then-UDF ordering for existing callers that wire everything up at once.
    pub fn setup_session_context(&self, ctx: &mut SessionContext) -> DFResult<()> {
        self.setup_session_tables(ctx)?;
        self.setup_session_udfs(ctx)
    }

    /// Register PostgreSQL settings table for compatibility
    pub fn register_pg_settings_table(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        use datafusion::arrow::{
            array::StringViewArray,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };

        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8View, false), Field::new("setting", DataType::Utf8View, false)]));

        let names: Vec<&str> = vec![
            "TimeZone",
            "client_encoding",
            "datestyle",
            "client_min_messages",
            "lc_monetary",
            "lc_numeric",
            "lc_time",
            "standard_conforming_strings",
            "application_name",
            "search_path",
        ];

        let settings: Vec<&str> = vec!["UTC", "UTF8", "ISO, MDY", "notice", "C", "C", "C", "on", "TimeFusion", "public"];

        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringViewArray::from(names)), Arc::new(StringViewArray::from(settings))])?;

        ctx.register_batch("pg_settings", batch)?;
        Ok(())
    }

    /// Register set_config UDF for PostgreSQL compatibility
    pub fn register_set_config_udf(&self, ctx: &SessionContext) {
        use datafusion::{
            arrow::{
                array::{StringViewArray, StringViewBuilder},
                datatypes::DataType,
            },
            logical_expr::{ColumnarValue, ScalarFunctionImplementation, Volatility, create_udf},
        };

        let set_config_fn: ScalarFunctionImplementation = Arc::new(move |args: &[ColumnarValue]| -> datafusion::error::Result<ColumnarValue> {
            let ColumnarValue::Array(array) = &args[1] else {
                return Err(DataFusionError::Execution("set_config: second argument must be an array".into()));
            };
            let param_value_array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| DataFusionError::Execution(format!("set_config: second argument must be StringViewArray, got {:?}", array.data_type())))?;

            let mut builder = StringViewBuilder::new();
            for i in 0..param_value_array.len() {
                if param_value_array.is_null(i) {
                    builder.append_null();
                } else {
                    builder.append_value(param_value_array.value(i));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        });

        let set_config_udf =
            create_udf("set_config", vec![DataType::Utf8View, DataType::Utf8View, DataType::Boolean], DataType::Utf8View, Volatility::Volatile, set_config_fn);

        ctx.register_udf(set_config_udf);
    }

    /// Register JSON functions from datafusion-functions-json
    pub fn register_json_functions(&self, ctx: &mut SessionContext) {
        datafusion_functions_json::register_all(ctx).expect("Failed to register JSON functions");
        info!("Registered JSON functions with SessionContext");
    }

    /// Check if a project has custom storage configuration (their own S3 bucket)
    async fn has_custom_storage(&self, project_id: &str, table_name: &str) -> bool {
        self.storage_configs.read().await.contains_key(&(project_id.to_string(), table_name.to_string()))
    }

    /// Snapshot of the custom-storage (project, table) keys — one lock
    /// acquisition for callers that need many membership checks (the
    /// coalescer's fold pass). Custom storage is rare, so the clone is tiny.
    pub(crate) async fn custom_storage_keys(&self) -> std::collections::HashSet<(String, String)> {
        self.storage_configs.read().await.keys().cloned().collect()
    }

    #[instrument(
        name = "database.resolve_table",
        skip(self),
        fields(
            project_id = %project_id,
            table.name = %table_name,
            cache_hit = Empty,
            is_custom = Empty,
        )
    )]
    /// Lock-free hot-path resolve. Returns the cached `Arc<RwLock<DeltaTable>>`
    /// without any `.await`. Skips the version-refresh check; that runs in
    /// the slow path (`resolve_table`) which is still called on first miss and
    /// from background tasks. Use this for read queries where stale snapshots
    /// (a few seconds behind a flush) are acceptable.
    pub fn try_fast_resolve(&self, project_id: &str, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
        // Two String allocations per call. Measured: ~70 ns each on the
        // hot path; absorbed by the 12 µs (release-iter) p50 query budget.
        // The DashMap-with-borrowed-key fix needs a wrapper type plus an
        // `Equivalent` impl (DashMap doesn't let `&(&str, &str)` look up
        // a `(String, String)` key directly). Holding for now — the
        // allocations are not the bottleneck and the lock removal in this
        // PR already eliminated the dominant overhead.
        self.fast_resolve_cache.get(&(project_id.to_string(), table_name.to_string())).map(|r| Arc::clone(r.value()))
    }

    /// `true` iff the scan path is allowed to skip the Delta side entirely
    /// for `(project, table)` — i.e., we've previously resolved this table
    /// AND have positive evidence it had no files at that observation (or
    /// has remained empty since — the `delta_has_files` bit is sticky-true,
    /// never sticky-false). Returns `false` for "we don't know yet" (table
    /// never resolved), so callers fall through to the full scan path and
    /// never falsely skip Delta.
    ///
    /// Reads as the predicate the scan path actually wants at the call
    /// site (`if delta_scan_can_be_skipped { ... }`), without the
    /// double-negative the prior `delta_is_known_empty` name imposed.
    /// Internally the stored bit is the positive `delta_has_files`
    /// (matches the flush callback's mental model — "we know what we
    /// wrote"); this method flips polarity exactly once, here, so call
    /// sites stay readable.
    pub fn delta_scan_can_be_skipped(&self, project_id: &str, table_name: &str) -> bool {
        // Two String allocations per call — same caveat as `try_fast_resolve`.
        // Lumped together as a deferred follow-up in
        // `docs/membuffer_flush_fix_plan.md` (borrowed-tuple-key wrapper for
        // all three table-keyed DashMaps at once).
        self.delta_has_files
            .get(&(project_id.to_string(), table_name.to_string()))
            // Acquire-load pairs with the Release-store in mark_delta_has_files
            // and populate_resolve_caches. The DashMap shard lock already
            // provides a happens-before via its own acquire/release of the
            // shard's internal lock, but defending Relaxed here would break
            // the moment a future refactor reads the Arc<AtomicBool> outside
            // the shard guard. Cost on ARM is one `dmb ish` per query;
            // negligible against the work it protects.
            .is_some_and(|f| !f.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Mark a (project, table) as having Delta files. Called by the flush
    /// callback after a successful commit.
    pub fn mark_delta_has_files(&self, project_id: &str, table_name: &str) {
        let key = (project_id.to_string(), table_name.to_string());
        let flag = self.delta_has_files.entry(key).or_insert_with(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        flag.store(true, std::sync::atomic::Ordering::Release);
    }

    /// Total cached providers across every key (a key holds up to
    /// `PROVIDER_VERSION_RETENTION` versions). This — not the key count — is
    /// what tracks the cache's heap footprint, so it is what
    /// `scan.provider_cache_entries` reports.
    fn delta_provider_cache_entries(&self) -> usize {
        self.delta_provider_cache.iter().map(|e| e.value().len()).sum()
    }

    fn trim_delta_provider_cache(&self) {
        let ttl = self.config.cache.provider_cache_ttl();
        // Per-version TTL prune, then drop keys left with nothing.
        let mut evicted = 0usize;
        self.delta_provider_cache.retain(|_, entry| {
            evicted += entry.prune(ttl);
            entry.len() > 0
        });
        // The caller is about to insert one provider, so leave one slot free
        // and keep the configured capacity strict after that insertion.
        // Capacity is counted in providers (not keys) so the bound still
        // reflects retained memory now that a key holds a version ring.
        let capacity = self.config.cache.provider_cache_capacity().saturating_sub(1);
        let total = self.delta_provider_cache_entries();
        if total > capacity {
            // Collect first, remove after: DashMap's iterator holds the shard
            // read lock, so removing mid-iteration can deadlock.
            let doomed: Vec<_> = self
                .delta_provider_cache
                .iter()
                .scan(total, |remaining, entry| {
                    (*remaining > capacity).then(|| {
                        *remaining -= entry.value().len();
                        entry.key().clone()
                    })
                })
                .collect();
            evicted += doomed.iter().filter_map(|key| self.delta_provider_cache.remove(key)).map(|(_, entry)| entry.len()).sum::<usize>();
        }
        self.scan_metrics.provider_cache_evictions.fetch_add(evicted as u64, std::sync::atomic::Ordering::Relaxed);
    }

    pub async fn resolve_table(&self, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        let span = tracing::Span::current();

        // Lazy reload of storage configs from PG, but at most once per
        // STORAGE_CONFIGS_TTL_NS. Without this, every SQL statement that hits
        // resolve_table issues a fresh PG roundtrip — death by a thousand cuts
        // under load.
        if let Some(ref pool) = self.config_pool {
            const STORAGE_CONFIGS_TTL_NS: u64 = 30 * 1_000_000_000; // 30s
            use std::{sync::atomic::Ordering, time::Instant};
            // Lazily anchor the clock so we use a monotonic delta from process start.
            static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
            let start = START.get_or_init(Instant::now);
            let now_ns = start.elapsed().as_nanos() as u64;
            let next = self.storage_configs_next_refresh_ns.load(Ordering::Relaxed);
            if now_ns >= next
                && self.storage_configs_next_refresh_ns.compare_exchange(next, now_ns + STORAGE_CONFIGS_TTL_NS, Ordering::AcqRel, Ordering::Relaxed).is_ok()
                && let Ok(new_configs) = Self::load_storage_configs(pool).await
            {
                let mut configs = self.storage_configs.write().await;
                *configs = new_configs;
            }
        }

        // Check if project has custom storage config → use isolated table
        if self.has_custom_storage(project_id, table_name).await {
            span.record("is_custom", true);
            let t = self.resolve_custom_table(project_id, table_name).await?;
            self.populate_resolve_caches(project_id, table_name, &t).await;
            return Ok(t);
        }

        span.record("is_custom", false);
        // Default: use unified table (all projects share the same table, partitioned by project_id)
        let t = self.resolve_unified_table(table_name).await?;
        self.populate_resolve_caches(project_id, table_name, &t).await;
        Ok(t)
    }

    /// Seed `fast_resolve_cache` and (sticky-up only) `delta_has_files` from a
    /// freshly-resolved Delta table handle. STICKY-TRUE INVARIANT: this only
    /// ever flips `delta_has_files` false → true. If a prior flush callback
    /// already observed files for `(project, table)`, or another task saw
    /// version > 0 first, the snapshot we just loaded may still report
    /// version == 0 (delta-rs caches state per handle and our update_state
    /// scheduling is racy under load). Downgrading the bit here would let the
    /// scan path skip Delta and silently hide rows. The default cell is
    /// false-seeded; positive evidence (version > 0 or `mark_delta_has_files`)
    /// is the only path to true.
    ///
    /// **Cold-start with pre-existing S3 data**: when this is the first
    /// `resolve_table` call after process start AND there is pre-existing
    /// data on S3 from a prior process, we rely on
    /// `create_or_load_delta_table` calling `DeltaTableBuilder::load()`,
    /// which populates the snapshot state from S3 inline. The handle
    /// returned by `resolve_unified_table` / `resolve_custom_table` has its
    /// `version()` already reflecting the on-S3 truth — so `has_files`
    /// here is accurate and the bit is seeded true. Removing the synchronous
    /// `.load()` in `create_or_load_delta_table` (e.g. switching to a lazy
    /// loader) would reopen the staleness window described above and break
    /// this seeding step; don't.
    async fn populate_resolve_caches(&self, project_id: &str, table_name: &str, t: &Arc<RwLock<DeltaTable>>) {
        let key = (project_id.to_string(), table_name.to_string());
        let was_new = self.fast_resolve_cache.insert(key.clone(), Arc::clone(t)).is_none();
        // Operator-visible warning so unbounded growth (documented on the
        // field) doesn't sit unseen in `scan.fast_resolve_cache_entries`.
        // Fires on first-insert crossings of the soft threshold, then again
        // every threshold-multiple, so log volume tracks tenant-population
        // growth rather than per-query traffic.
        if was_new {
            let size = self.fast_resolve_cache.len();
            if size >= CACHE_SOFT_LIMIT_WARN && size.is_multiple_of(CACHE_SOFT_LIMIT_WARN) {
                tracing::warn!(
                    target = "table_caches",
                    fast_resolve_cache_entries = size,
                    threshold = CACHE_SOFT_LIMIT_WARN,
                    "fast_resolve_cache crossed soft limit (no eviction by design). If your steady-state tenant count is below the threshold, dropped or transient project_ids are accumulating. Watch scan.fast_resolve_cache_entries in timefusion_stats."
                );
            }
        }
        let has_files = t.read().await.version().is_some_and(|v| v > 0);
        let entry = self.delta_has_files.entry(key).or_insert_with(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        if has_files {
            // Release pairs with the Acquire load in delta_scan_can_be_skipped
            // (see comment there). Same rationale.
            entry.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Resolve a unified table (shared by all default projects, partitioned by project_id)
    async fn resolve_unified_table(&self, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // Check unified_tables cache first. Clone the handle and DROP the map
        // guard before the refresh: `update_table` replays the Delta log, and a
        // read guard held across it blocks writers — which, tokio's RwLock being
        // write-preferring, then blocks every later reader (the maintenance-wedge
        // shape documented on `all_tables`).
        let cached = self.unified_tables.read().await.get(table_name).cloned();
        if let Some(table) = cached {
            debug!("Found unified table '{}' in cache", table_name);
            // Version tracking keys unified tables under an empty project_id —
            // they aren't project-specific.
            return self.refresh_cached_table(table, "", table_name).await;
        }

        // Not in cache, create/load it
        self.get_or_create_unified_table(table_name).await.map_err(|e| DataFusionError::Execution(format!("Failed to get or create unified table: {}", e)))
    }

    /// Refresh a cache-hit handle when this process's view may be behind. Shared
    /// by `resolve_unified_table` / `resolve_custom_table` so the staleness rule
    /// can't drift between them.
    async fn refresh_cached_table(&self, table: Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        let last_written_version = self.last_written_versions.read().await.get(&(project_id.to_string(), table_name.to_string())).cloned();
        let current_version = table.read().await.version();
        if should_refresh_table(current_version, last_written_version) {
            self.update_table(&table, project_id, table_name).await.map_err(|e| DataFusionError::Execution(format!("Failed to update table: {e}")))?;
        }
        Ok(table)
    }

    /// Resolve a custom project table (isolated table for projects with their own S3 bucket)
    async fn resolve_custom_table(&self, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // Check custom_project_tables cache first — handle cloned and the map
        // guard dropped before the refresh (see `resolve_unified_table`).
        let cached = self.custom_project_tables.read().await.get(&(project_id.to_string(), table_name.to_string())).cloned();
        if let Some(table) = cached {
            debug!("Found custom table for project '{}' table '{}' in cache", project_id, table_name);
            return self.refresh_cached_table(table, project_id, table_name).await;
        }

        // Not in cache, create/load it
        self.get_or_create_custom_table(project_id, table_name)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get or create custom table: {}", e)))
    }

    #[instrument(
        name = "database.get_or_create_unified_table",
        skip(self),
        fields(table.name = %table_name)
    )]
    pub async fn get_or_create_unified_table(&self, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Check cache first
        {
            let tables = self.unified_tables.read().await;
            if let Some(table) = tables.get(table_name) {
                return Ok(Arc::clone(table));
            }
        }

        let Some(ref bucket) = self.default_s3_bucket else {
            return Err(anyhow::anyhow!("No default S3 bucket configured for unified table '{}'", table_name));
        };

        let prefix = self.default_s3_prefix.as_ref().ok_or_else(|| anyhow::anyhow!("No default S3 prefix configured for unified table '{}'", table_name))?;
        let endpoint =
            self.default_s3_endpoint.as_ref().ok_or_else(|| anyhow::anyhow!("No default S3 endpoint configured for unified table '{}'", table_name))?;
        // Unified table path: s3://{bucket}/{prefix}/{table_name}/ (NO project_id subdirectory)
        let storage_uri = format!("s3://{}/{}/{}/?endpoint={}", bucket, prefix, table_name, endpoint);
        let storage_options = self.build_storage_options();

        info!("Creating or loading unified table '{}' at: {}", table_name, storage_uri);

        // Load OUTSIDE the write lock. `create_delta_table_internal` replays the
        // Delta log over the network and is not time-bounded (a dead endpoint or a
        // huge log can hold it for minutes or forever); holding the map's write
        // guard across it blocks EVERY reader of the map, and because tokio's
        // RwLock is write-preferring one queued writer also blocks every later
        // reader. That wedges all maintenance jobs on their first table lookup
        // while ingest/queries stay healthy on `fast_resolve_cache` — the
        // 2026-07-29 22:05 total-maintenance-wedge shape. Two racing first
        // touches may both load; the double-check below keeps the first insert.
        let table = self.create_delta_table_internal(&storage_uri, &storage_options, table_name).await?;
        let mut tables = self.unified_tables.write().await;
        if let Some(table) = tables.get(table_name) {
            return Ok(Arc::clone(table));
        }
        let table_arc = Arc::new(RwLock::new(table));
        tables.insert(table_name.to_string(), Arc::clone(&table_arc));
        info!("Cached unified table '{}', cache now contains {} entries", table_name, tables.len());

        Ok(table_arc)
    }

    #[instrument(
        name = "database.get_or_create_custom_table",
        skip(self),
        fields(project_id = %project_id, table.name = %table_name)
    )]
    pub async fn get_or_create_custom_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Check cache first
        {
            let tables = self.custom_project_tables.read().await;
            if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
                return Ok(Arc::clone(table));
            }
        }

        // Get custom storage config for this project
        let configs = self.storage_configs.read().await;
        let config = configs
            .get(&(project_id.to_string(), table_name.to_string()))
            .ok_or_else(|| anyhow::anyhow!("No storage config found for project '{}' table '{}'", project_id, table_name))?
            .clone();
        drop(configs);

        let storage_uri = format!(
            "s3://{}/{}/?endpoint={}",
            config.s3_bucket,
            config.s3_prefix,
            config.s3_endpoint.as_ref().unwrap_or(&self.default_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string()))
        );

        // Start from the shared base options so BYO buckets inherit AWS_ALLOW_HTTP +
        // connect_timeout like the unified table (delta-rs rejects http/on-prem
        // endpoints without AWS_ALLOW_HTTP), then override with this tenant's
        // credentials. Endpoint stays tenant-scoped: a BYO bucket with no custom
        // endpoint must resolve against real AWS S3, so drop the inherited default
        // rather than point it at ours.
        let mut storage_options = self.build_storage_options();
        storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), config.s3_access_key_id.clone());
        storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), config.s3_secret_access_key.clone());
        storage_options.insert("AWS_REGION".to_string(), config.s3_region.clone());
        match config.s3_endpoint.as_ref() {
            Some(endpoint) => storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone()),
            None => storage_options.remove("AWS_ENDPOINT_URL"),
        };

        info!("Creating or loading custom table for project '{}' table '{}' at: {}", project_id, table_name, storage_uri);

        // Load OUTSIDE the write lock — see `get_or_create_unified_table`. Worse
        // here: the load targets a TENANT's BYO bucket, so one unreachable
        // endpoint or stale credential set can pin this map's write guard
        // indefinitely and wedge every maintenance job that walks the map.
        let table = self.create_delta_table_internal(&storage_uri, &storage_options, table_name).await?;
        let mut tables = self.custom_project_tables.write().await;
        if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
            return Ok(Arc::clone(table));
        }
        let table_arc = Arc::new(RwLock::new(table));
        tables.insert((project_id.to_string(), table_name.to_string()), Arc::clone(&table_arc));
        info!("Cached custom table for project '{}' table '{}', cache now contains {} entries", project_id, table_name, tables.len());

        Ok(table_arc)
    }

    /// Internal helper to create/load a Delta table with caching and retry logic
    async fn create_delta_table_internal(&self, storage_uri: &str, storage_options: &HashMap<String, String>, table_name: &str) -> Result<DeltaTable> {
        // Create the base S3 object store. TWO clients, one per request class:
        // the data client keeps the generous `request_timeout` a multi-MB
        // parquet part needs, while `_delta_log` control-plane traffic gets a
        // short-timeout client so a hung commit PUT can no longer pin this
        // table's commit lock for the data bound (prod 2026-07-30). The router
        // sits BELOW instrumentation + the foyer cache, so caching, metrics and
        // cache keys are unchanged.
        let base_store = self.create_object_store(storage_uri, storage_options).instrument(tracing::trace_span!("create_object_store")).await?;
        let log_store_client = self
            .create_object_store_with_timeout(storage_uri, storage_options, self.config.aws.log_request_timeout())
            .instrument(tracing::trace_span!("create_object_store_log_class"))
            .await?;
        let routed = Arc::new(crate::object_store_cache::RequestClassRouter::new(log_store_client, base_store)) as Arc<dyn object_store::ObjectStore>;
        let instrumented_store = instrument_object_store(routed, "s3");

        let cached_store = if let Some(ref shared_cache) = self.object_store_cache {
            Arc::new(FoyerObjectStoreCache::new_with_shared_cache(instrumented_store.clone(), shared_cache)) as Arc<dyn object_store::ObjectStore>
        } else {
            warn!("Shared Foyer cache not initialized, using uncached object store");
            instrumented_store
        };

        // Try to load existing table
        match self.create_or_load_delta_table(storage_uri, storage_options.clone(), cached_store.clone()).await {
            Ok(table) => {
                info!("Loaded existing table '{}'", table_name);
                let mut desired = HashMap::from([
                    ("delta.deletedFileRetentionDuration".to_string(), format!("interval {} hours", self.config.maintenance.timefusion_vacuum_retention_hours)),
                    ("delta.checkpointInterval".to_string(), self.config.parquet.timefusion_checkpoint_interval.to_string()),
                    // Reconcile _delta_log retention on EXISTING tables too — a config
                    // change alone wouldn't shrink a table that baked in the old value
                    // at create (the live otel_logs_and_spans sat at 1 day and regrew
                    // its log to ~6.7k objects → 3-5s commits, 2026-06-26).
                    ("delta.logRetentionDuration".to_string(), format!("interval {} hours", self.config.maintenance.timefusion_log_retention_hours)),
                    // Reconciled on EXISTING tables too, or the stats trimming
                    // never reaches the one table that pays the CPU. Takes
                    // precedence over the legacy `dataSkippingNumIndexedCols=-1`
                    // baked in at create (delta-rs reads stats_columns first), so
                    // the old key is left alone rather than removed.
                    ("delta.dataSkippingStatsColumns".to_string(), stats_columns_for(get_schema(table_name).unwrap_or_else(get_default_schema))),
                ]);
                // One-time protocol upgrade so merge-on-read UPDATE/DELETE can attach DVs.
                // Only when opted in; ensure_table_properties is idempotent (no commit if set).
                if self.config.maintenance.timefusion_use_deletion_vectors {
                    desired.insert("delta.enableDeletionVectors".to_string(), "true".to_string());
                }
                Ok(ensure_table_properties(table, desired).await)
            }
            Err(load_err) => {
                info!("Table '{}' doesn't exist, creating new table. err: {:?}", table_name, load_err);

                let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
                let mut create_attempts = 0;

                loop {
                    create_attempts += 1;
                    let commit_properties = CommitProperties::default().with_create_checkpoint(true).with_cleanup_expired_logs(Some(true));
                    let checkpoint_interval = self.config.parquet.timefusion_checkpoint_interval.to_string();

                    let mut config = HashMap::new();
                    config.insert("delta.checkpointInterval".to_string(), Some(checkpoint_interval));
                    // Aligned with vacuum retention so checkpoints prune Remove
                    // tombstones as soon as vacuum has had its shot at the files.
                    config.insert(
                        "delta.deletedFileRetentionDuration".to_string(),
                        Some(format!("interval {} hours", self.config.maintenance.timefusion_vacuum_retention_hours)),
                    );
                    // Bound the _delta_log so per-commit version-discovery LISTs stay cheap.
                    // Delta's 30-day default let the log reach 68k objects → ~35s commits
                    // (2026-06-25 DLQ incident). enableExpiredLogCleanup prunes on checkpoint.
                    config.insert(
                        "delta.logRetentionDuration".to_string(),
                        Some(format!("interval {} hours", self.config.maintenance.timefusion_log_retention_hours)),
                    );
                    config.insert("delta.enableExpiredLogCleanup".to_string(), Some("true".to_string()));
                    // Stats for an EXPLICIT column list, not all 90+ leaf columns
                    // (the old `dataSkippingNumIndexedCols=-1`). Whole-schema stats
                    // made every Add carry a min/max/nullCount for each wide
                    // JSON/variant column: 18.4% of process CPU was `parse_json_impl`
                    // on Add stats during log replay (cpu-000014.svg, 2026-07-29),
                    // paid by queries and maintenance alike. The listed columns are
                    // the only ones data-skipping and compaction actually prune on
                    // (see `stats_columns_for`); everything else was carried cost.
                    config.insert("delta.dataSkippingStatsColumns".to_string(), Some(stats_columns_for(schema)));
                    // Enable merge-on-read deletion vectors at create so DV UPDATE/DELETE
                    // works without a later protocol upgrade. Opt-in only.
                    if self.config.maintenance.timefusion_use_deletion_vectors {
                        config.insert("delta.enableDeletionVectors".to_string(), Some("true".to_string()));
                    }

                    match CreateBuilder::new()
                        .with_location(storage_uri)
                        .with_columns(schema.columns().unwrap_or_default())
                        .with_partition_columns(schema.partitions.clone())
                        .with_storage_options(storage_options.clone())
                        .with_commit_properties(commit_properties)
                        .with_configuration(config)
                        .await
                    {
                        Ok(table) => break Ok(table),
                        Err(create_err) => {
                            let err_str = create_err.to_string();
                            if (err_str.contains("already exists") || err_str.contains("version 0") || err_str.contains("ConditionalCheckFailedException"))
                                && create_attempts < 3
                            {
                                debug!("Table creation conflict, attempting to load existing table (attempt {})", create_attempts);
                                let backoff_ms = 100 * (2_u64.pow(create_attempts.min(5)));
                                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;

                                match self.create_or_load_delta_table(storage_uri, storage_options.clone(), cached_store.clone()).await {
                                    Ok(table) => break Ok(table),
                                    Err(reload_err) => {
                                        debug!("Failed to load table after creation conflict: {:?}", reload_err);
                                        continue;
                                    }
                                }
                            } else {
                                break Err(anyhow::anyhow!("Failed to create table: {}", create_err));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Legacy method for backward compatibility - routes to unified or custom table
    #[instrument(
        name = "database.get_or_create_table",
        skip(self),
        fields(project_id = %project_id, table.name = %table_name)
    )]
    /// Return the live parquet file URIs of a Delta table after refreshing
    /// its state. Returns empty if the table doesn't exist yet (pre-create).
    /// Used by the buffered-layer's Delta callback to surface "files added
    /// by this commit" to the sidecar tantivy indexer.
    pub async fn list_file_uris(&self, project_id: &str, table_name: &str) -> Result<Vec<String>> {
        let table_ref = match self.resolve_table(project_id, table_name).await {
            Ok(r) => r,
            Err(_) => return Ok(Vec::new()),
        };
        let _ = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let uris: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
        Ok(uris)
    }

    /// Best-effort warm of the Foyer cache for parquet files just written by a
    /// flush or optimize commit. Reuses the read path so the recent partitions
    /// dashboards query don't cold-start after every compaction: a ranged GET
    /// of each new footer primes the metadata cache (query planning pays zero
    /// S3 round-trips), and — when `timefusion_warm_full_files` is set — a full
    /// GET primes the main cache for data reads.
    ///
    /// Normally non-blocking and strictly best-effort: the job runs in a
    /// detached, concurrency-bounded task and never affects the commit. Files
    /// are filtered to partitions within `timefusion_warm_recency_days` so we
    /// don't spend S3 GETs (and evict useful entries) warming cold partitions
    /// nobody reads.
    ///
    /// `confirm` switches to the Influx-oracle flush-path mode: the metadata
    /// pass is awaited under that deadline, while full bodies are left to the
    /// detached post-commit pass. `allow_full_files` is false during bootstrap
    /// so restart recovery cannot saturate object-store bandwidth by fetching
    /// every recent body before queries can populate compact range entries.
    async fn warm_cache_for_uris(
        &self, object_store: Arc<dyn object_store::ObjectStore>, table_uri: String, uris: Vec<String>, confirm: Option<std::time::Duration>,
        allow_full_files: bool,
    ) {
        let maint = &self.config.maintenance;
        if !maint.timefusion_warm_after_compaction || uris.is_empty() {
            return;
        }
        let (warm_full_files, warm_all_footers) = match confirm {
            // Confirm only proves the small metadata reads before the
            // MemBuffer handoff. Full bodies warm detached below: a slow R2
            // GET must not make the flush path abandon metadata or wedge.
            Some(_) => (false, false),
            None => (allow_full_files && maint.timefusion_warm_full_files, maint.timefusion_warm_all_footers),
        };
        let recency_days = maint.timefusion_warm_recency_days;
        // Confirm fetches whole parquet bodies into untracked transient heap ON
        // the flush path, so it gets its own much lower bound — never the
        // 16-way detached compaction knob. See the config docs.
        let concurrency = match confirm {
            Some(_) => crate::config::CACHE_CONFIRM_CONCURRENCY,
            None => maint.timefusion_warm_concurrency,
        }
        .max(1);
        let metadata_size_hint = self.config.cache.timefusion_parquet_metadata_size_hint as u64;
        let stats_cache = self.object_store_cache.clone();

        // Relativize absolute s3:// URIs against the table root: the cached
        // object store consumes bucket-relative paths.
        let prefix = table_cache_prefix(&table_uri);
        let table_path = table_path_in_bucket(prefix);
        // Cap the day count before the i64 cast — recency_days is a config
        // value so overflow can't happen in practice, but a silent wrap would
        // turn a misconfiguration into "warm nothing". 3650d (~10y) is well
        // past any partition we'd query.
        let cutoff = (recency_days > 0).then(|| Utc::now().date_naive() - chrono::Duration::days(recency_days.min(3650) as i64));

        // With warm_all_footers (default): footers warm for EVERY live file
        // (tens of KB each — they turn a deep-partition first touch from
        // footer+data RTTs into a single data fetch). On tables with
        // thousands of files that's thousands of boot-time GETs (bounded by
        // `concurrency`); disable the flag to recency-bound footers too.
        // Full-file warming is always recency-bounded. Oldest partitions warm
        // FIRST so the newest land last in LRU order: if the warm set exceeds
        // the metadata cache (size it as metadata_disk ≥ live_files ×
        // parquet_metadata_size_hint), eviction then drops the least-queried
        // old partitions instead of whichever files happened to warm late.
        let (paths, dropped) = select_warm_paths(uris, prefix, warm_all_footers, cutoff);
        if dropped > 0 {
            // warn: a systematic prefix mismatch silently no-ops the whole
            // warm pass (the wrong key would never be hit), and prod runs at
            // warn level — debug would make it invisible exactly where it
            // matters (boot-time preload).
            warn!("warm: skipped {} file(s) that did not relativize against prefix {}", dropped, prefix);
        }
        if paths.is_empty() {
            return;
        }

        let count = paths.len();
        // Baseline the cache stats *before* warming: the warm GETs are all
        // misses (they fetch from the inner store to populate Foyer), so a
        // post-warm hit rate would read artificially low. The real
        // beneficiary is the next dashboard query — log the pre-warm
        // steady-state rate as the relevant baseline.
        let baseline = match (&stats_cache, confirm) {
            (Some(cache), None) => {
                let s = cache.get_stats().await.main;
                let rate = if s.hits + s.misses > 0 { (s.hits as f64 / (s.hits + s.misses) as f64) * 100.0 } else { 0.0 };
                Some(rate)
            }
            _ => None,
        };

        // Labelled scope rather than `full=true/false` so warm logs are easy to
        // filter (e.g. in Loki) by what was actually primed.
        let scope = if warm_full_files { "full" } else { "footer-only" };
        // Surface the burst size up front so operators can see what a restart
        // is about to issue against S3 (the completion log alone can't —
        // a large warm set takes minutes to get there). The confirm pass is
        // per-flush and bounded, so it only logs on completion.
        if confirm.is_none() {
            info!("Cache warm start: {count} files (scope={scope}, concurrency={concurrency})");
        }
        let t0 = std::time::Instant::now();
        // Progress heartbeat: a 10k-file boot warm runs minutes; without one
        // operators can't tell warming from a hang. The {count} denominator
        // is the selected warm set (footer warms); full-file warming covers
        // only the `recent` subset of it.
        const WARM_PROGRESS_INTERVAL: usize = 500;
        let (done, fetched) = (std::sync::atomic::AtomicUsize::new(0), std::sync::atomic::AtomicUsize::new(0));
        let (done, fetched, shared) = (&done, &fetched, stats_cache.as_ref());
        let pass = futures::stream::iter(paths).for_each_concurrent(concurrency, |(path, recent)| {
            let store = object_store.clone();
            async move {
                // Metadata is cheap and required independently of a full-body
                // warm: a cold Parquet header/footer probe must never trigger a
                // whole-object GET.
                let _ = crate::object_store_cache::warm_parquet_metadata(store.as_ref(), &path, metadata_size_hint).await;
                if warm_full_files && recent {
                    let hit = match shared {
                        Some(shared) => {
                            crate::object_store_cache::warm_full_if_absent(store.as_ref(), shared, &path, &bucket_cache_key(table_path, &path)).await
                        }
                        None => crate::object_store_cache::warm_full(store.as_ref(), &path).await,
                    };
                    fetched.fetch_add(hit as usize, std::sync::atomic::Ordering::Relaxed);
                }
                let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if n.is_multiple_of(WARM_PROGRESS_INTERVAL) {
                    // Elapsed on the heartbeat lets operators extrapolate
                    // time-remaining without waiting for completion.
                    info!("Cache warm progress: {n}/{count} files ({:.1}s elapsed)", t0.elapsed().as_secs_f64());
                }
            }
        });

        let Some(deadline) = confirm else {
            pass.await;
            let elapsed_s = t0.elapsed().as_secs_f64();
            match baseline {
                Some(rate) => info!(
                    "Cache warm complete: {} files warmed (scope={}) in {:.1}s; foyer main hit rate before warm was {:.2}% (next query benefits)",
                    count, scope, elapsed_s, rate
                ),
                None => info!("Cache warm complete: {} files warmed (scope={}) in {:.1}s", count, scope, elapsed_s),
            }
            return;
        };
        // Never a durability gate: on timeout the commit is already done and the
        // uncached tail just costs the next query an object-store round trip.
        if tokio::time::timeout(deadline, pass).await.is_err() {
            crate::metrics::record_cache_confirm_timeout();
            warn!("cache confirm exceeded {:?} for {} file(s) — proceeding uncached (commit unaffected)", deadline, count);
        }
        let fetched = fetched.load(std::sync::atomic::Ordering::Relaxed);
        crate::metrics::record_cache_confirm(count as u64, fetched as u64);
        debug!("cache confirm: {count} file(s), {fetched} fetched, {:.1}s", t0.elapsed().as_secs_f64());
    }

    /// Proactively evict the cached full-file bytes of files a compaction
    /// tombstoned (present pre-commit, gone post-commit), so dead compaction
    /// outputs don't linger in the cache until VACUUM / TTL / LRU reclaims them.
    ///
    /// Correctness is unaffected: the files still exist in S3 until VACUUM, so a
    /// straggler query holding the old Delta snapshot just re-reads them from S3
    /// (a cache miss), never a wrong result. Cheap and in-cache only (no S3),
    /// so it runs inline.
    fn evict_cache_for_uris(&self, table_uri: &str, removed: &[String]) {
        if !self.config.maintenance.timefusion_evict_after_compaction || removed.is_empty() {
            return;
        }
        let Some(cache) = self.object_store_cache.as_ref() else {
            return;
        };
        // Same relativization as warm_cache_for_uris: the cache keys full files
        // by their object-store-relative path.
        let prefix = table_cache_prefix(table_uri);
        let table_path = table_path_in_bucket(prefix);
        let (evicted, dropped) = removed.iter().fold((0usize, 0usize), |(evicted, dropped), u| match relativize_to_prefix(prefix, u) {
            Some(path) => {
                cache.evict_data_entry(&bucket_cache_key(table_path, &path));
                (evicted + 1, dropped)
            }
            // Prefix mismatch (trailing-slash or query-string drift between
            // table_url() and get_file_uris()) — we'd evict the wrong key, so
            // skip. Log like the warm path: a systematic mismatch here means
            // tombstoned files linger in cache until TTL/LRU, which is worth
            // diagnosing rather than silently swallowing.
            None => {
                if dropped == 0 {
                    debug!("evict: URI {} does not start with table prefix {}; skipping (evict only)", u, prefix);
                }
                (evicted, dropped + 1)
            }
        });
        if evicted > 0 {
            debug!("Evicted {} tombstoned file(s) from cache after compaction", evicted);
        }
        if dropped > 0 {
            debug!("evict: skipped {} file(s) that did not relativize against prefix {}", dropped, prefix);
        }
    }

    /// Warm the cache for files added by a just-committed flush/optimize on the
    /// given logical table. Fire-and-forget: resolving the table (which may
    /// issue a rate-limited PG roundtrip) and taking the read lock both happen
    /// inside a spawned task, so the caller — notably the flush callback — is
    /// never blocked. No-op when warming is disabled or the list is empty.
    pub fn warm_cache_for_table(&self, project_id: &str, table_name: &str, uris: Vec<String>) {
        if uris.is_empty() || !self.config.maintenance.timefusion_warm_after_compaction {
            return;
        }
        let db = self.clone();
        let project_id = project_id.to_string();
        let table_name = table_name.to_string();
        tokio::spawn(async move {
            if let Ok(table_ref) = db.resolve_table(&project_id, &table_name).await {
                let (store, table_uri) = {
                    let t = table_ref.read().await;
                    (t.log_store().object_store(None), t.table_url().to_string())
                };
                // Already inside a detached task — await the warm directly
                // instead of spawning a second nested task.
                db.warm_cache_for_uris(store, table_uri, uris, None, true).await;
            }
        });
    }

    /// Resolve every registry table and warm parquet footers in the
    /// background (ALL live files by default; recency-bounded when
    /// `TIMEFUSION_WARM_ALL_FOOTERS=false` — see warm_cache_for_uris), so the
    /// first query after a deploy doesn't pay Delta log replay + parquet
    /// footer reads inline (measured 1.4 s cold vs 13 ms warm against OVH S3
    /// for a single-partition random-access lookup).
    pub fn preload_tables(self: &Arc<Self>) {
        // Idempotent: main.rs and bootstrap.rs are disjoint entry points, but
        // a second call must not double the boot-time S3 warm burst.
        // Relaxed: the swap's atomicity alone decides the winner; no other
        // memory needs to be ordered around it.
        if self.preload_started.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Tables preload concurrently — a slow object-store round-trip on one
        // must not delay the others' first-query readiness — but the fan-out
        // is capped at the same bound as per-file warming: each table preload
        // is a Delta log replay (object-store round-trips), so an unbounded
        // spawn-per-table would spike S3 at boot as the registry grows.
        let db = Arc::clone(self);
        let shutdown = self.maintenance_shutdown.clone();
        let concurrency = self.config.maintenance.timefusion_warm_concurrency.max(1);
        tokio::spawn(async move {
            let preload_all = futures::stream::iter(crate::schema_loader::registry().list_tables()).for_each_concurrent(concurrency, |table_name| {
                let db = Arc::clone(&db);
                async move {
                    let t = std::time::Instant::now();
                    match db.resolve_table("default", &table_name).await {
                        Ok(table_ref) => {
                            // Warm via the already-resolved handle — warm_cache_for_table
                            // would redundantly resolve_table a second time.
                            let (uris, store, table_uri) = {
                                let table = table_ref.read().await;
                                let uris: Vec<String> = table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                                (uris, table.log_store().object_store(None), table.table_url().to_string())
                            };
                            info!("bootstrap.phase=table_preload table={table_name} files={} elapsed_ms={}", uris.len(), t.elapsed().as_millis());
                            // Restart reconstructs table and parquet metadata
                            // only. Re-downloading every uncached recent body at
                            // boot saturated object-store bandwidth (13GB during
                            // three 1h queries) and made a recovered deployment
                            // slower than a cold range read. New files still get
                            // full-body warming on their post-commit paths.
                            db.warm_cache_for_uris(store, table_uri, uris, None, false).await;
                        }
                        Err(e) => warn!("bootstrap.phase=table_preload table={table_name} skipped: {e}"),
                    }
                }
            });
            // Abandon warming on shutdown so in-flight S3 calls can't slow
            // a fast restart during initial boot.
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = preload_all => {}
            }
        });
    }

    /// Atomically swap a freshly-optimized `new_table` in under the write lock,
    /// then refresh the cache for the file-set delta vs `pre_uris`: warm the
    /// files this optimize added and evict the ones it tombstoned. Returns the
    /// new table's live file URIs (captured before the swap) for callers that
    /// need them (e.g. the tantivy GC hook).
    ///
    /// `pre_uris` is `None` when the caller isn't tracking the file set (both
    /// warm- and evict-after-compaction disabled) — the diff and its warm/evict
    /// are then skipped entirely rather than degenerating into "everything is
    /// new". `scope` must be the SAME partition markers the caller scoped
    /// `pre_uris` with (see [`scoped_file_uris`]); diffing a scoped pre-set
    /// against an unscoped live set would warm every other partition.
    ///
    /// Both optimize paths — full Z-order and light — funnel through here so the
    /// warm/evict pair can't drift; the evict call was once missing from the
    /// light path, and a single helper keeps them in lockstep.
    async fn swap_and_refresh_cache(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, new_table: DeltaTable, pre_uris: Option<&std::collections::HashSet<String>>, scope: &[&str],
    ) -> Vec<String> {
        // Capture live URIs off `new_table` *before* the swap moves it in.
        let live_uris: Vec<String> = scoped_file_uris(&new_table, scope);
        let (added, removed): (Vec<String>, Vec<String>) = match pre_uris {
            Some(pre) => {
                let live_set: std::collections::HashSet<&str> = live_uris.iter().map(String::as_str).collect();
                (live_uris.iter().filter(|u| !pre.contains(*u)).cloned().collect(), pre.iter().filter(|u| !live_set.contains(u.as_str())).cloned().collect())
            }
            None => (Vec::new(), Vec::new()),
        };
        let warm_store = new_table.log_store().object_store(None);
        let warm_table_uri = new_table.table_url().to_string();
        self.persist_snapshot(&new_table);
        {
            // Version-guarded swap so this is safe to call WITHOUT holding
            // the commit lock: a concurrent committer may have already
            // advanced `table_ref` past our just-committed version (its refresh
            // picks our commit up from the log), and a bare `*table = new_table`
            // would regress the handle. None < Some(_), so an unloaded handle
            // always swaps. Same guard as `refresh_table_snapshot`.
            let mut table = table_ref.write().await;
            if new_table.version() > table.version() {
                *table = new_table;
            }
        }
        // WARM BEFORE EVICT, always: any multi-file swap that evicts first
        // cold-starts the hottest query window (the 2026-07-21 cache-thrash
        // shape, concentrated by wave commits swapping K bins at once). Warming
        // issues S3 GETs, so the whole pair is detached — the maintenance loop
        // never blocks on priming the cache, and eviction rides behind the warm
        // inside the same task so the ordering holds without blocking anyone.
        let db = self.clone();
        tokio::spawn(async move {
            let uri = warm_table_uri;
            db.warm_cache_for_uris(warm_store, uri.clone(), added, None, true).await;
            db.evict_cache_for_uris(&uri, &removed);
        });
        live_uris
    }

    pub async fn get_or_create_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Route to appropriate table based on whether project has custom storage
        if self.has_custom_storage(project_id, table_name).await {
            self.get_or_create_custom_table(project_id, table_name).await
        } else {
            self.get_or_create_unified_table(table_name).await
        }
    }

    /// Create an object store for the given URI and storage options
    pub async fn create_object_store(&self, storage_uri: &str, storage_options: &HashMap<String, String>) -> Result<Arc<dyn object_store::ObjectStore>> {
        self.create_object_store_with_timeout(storage_uri, storage_options, self.config.aws.request_timeout()).await
    }

    /// `create_object_store` with an explicit per-request timeout, so the
    /// commit-log request class can get its own client (see
    /// [`crate::object_store_cache::RequestClassRouter`]). Every other setting
    /// — retries, pool sizing, credentials — is identical by construction.
    pub async fn create_object_store_with_timeout(
        &self, storage_uri: &str, storage_options: &HashMap<String, String>, request_timeout: String,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        use std::time::Duration;

        use object_store::{BackoffConfig, ClientConfigKey, ClientOptions, RetryConfig, aws::AmazonS3Builder};

        // Parse the S3 URI to extract bucket and prefix
        let url = Url::parse(storage_uri)?;
        let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("Invalid S3 URI: missing bucket"))?;

        // Configure retry with exponential backoff for transient network errors
        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(180),
            backoff: BackoffConfig { init_backoff: Duration::from_millis(100), max_backoff: Duration::from_secs(15), base: 2.0 },
        };

        // Configure HTTP client timeouts from config (TIMEFUSION_S3_CONNECT_TIMEOUT /
        // TIMEFUSION_S3_REQUEST_TIMEOUT). object_store parses the humantime strings;
        // this is the path the unified + custom data tables (and compaction) use, so
        // its timeouts must match build_storage_options rather than being hardcoded.
        // PoolMaxIdlePerHost keeps connections warm so concurrent uploads
        // (raised flush_parallelism, multi-part compaction PUTs) reuse sockets
        // instead of re-establishing TLS and starving R2 — the connection
        // starvation that failed the 2026-06-24 compaction. R2 tolerates 64+
        // concurrent ops per bucket.
        let client_options = ClientOptions::new()
            .with_config(ClientConfigKey::ConnectTimeout, self.config.aws.connect_timeout())
            .with_config(ClientConfigKey::Timeout, request_timeout)
            .with_config(ClientConfigKey::PoolMaxIdlePerHost, crate::config::S3_POOL_MAX_IDLE_PER_HOST.to_string());

        // Build S3 configuration
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket).with_retry(retry_config).with_client_options(client_options);

        // Apply storage options
        if let Some(access_key) = storage_options.get("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = storage_options.get("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(region) = storage_options.get("AWS_REGION") {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = storage_options.get("AWS_ENDPOINT_URL") {
            builder = builder.with_endpoint(endpoint);
            // If endpoint is HTTP, allow HTTP connections
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        // Use config values as fallback
        if storage_options.get("AWS_ACCESS_KEY_ID").is_none()
            && let Some(ref key) = self.config.aws.aws_access_key_id
        {
            builder = builder.with_access_key_id(key);
        }
        if storage_options.get("AWS_SECRET_ACCESS_KEY").is_none()
            && let Some(ref secret) = self.config.aws.aws_secret_access_key
        {
            builder = builder.with_secret_access_key(secret);
        }
        if storage_options.get("AWS_REGION").is_none()
            && let Some(ref region) = self.config.aws.aws_default_region
        {
            builder = builder.with_region(region);
        }

        // Check if we need to use config for endpoint and allow HTTP
        if storage_options.get("AWS_ENDPOINT_URL").is_none() {
            let endpoint = &self.config.aws.aws_s3_endpoint;
            builder = builder.with_endpoint(endpoint);
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        let store = builder.build()?;
        Ok(Arc::new(store))
    }

    /// Directory holding locally persisted Delta snapshots (see `snapshot_cache`).
    fn delta_snapshot_dir(cfg: &AppConfig) -> PathBuf {
        crate::wal::meta_path(&cfg.core.timefusion_data_dir, "delta_snapshots")
    }

    /// Whether snapshot refreshes may take the incremental catch-up fast path
    /// (see [`refresh_table_snapshot`]) — exposed for the DML path in dml.rs.
    pub(crate) fn incremental_snapshot(&self) -> bool {
        self.config.maintenance.timefusion_incremental_snapshot
    }

    /// The process-wide `RuntimeEnv`: one memory pool + parquet-metadata cache
    /// shared by EVERY session (pgwire, internal SQL, maintenance) so the
    /// `TIMEFUSION_MEMORY_LIMIT_GB × fraction` cap is a real budget. Memory
    /// pool: defaults to Greedy (single global cap, no per-consumer slicing)
    /// for ingest-heavy workloads; opt into FairSpill via
    /// `TIMEFUSION_MEMORY_POOL=fair_spill`. Maintenance jobs (optimize, dedup,
    /// recompress) MUST run under this env — the 2026-07-04 crash-loop was the
    /// dedup sweep materializing chunks in a fresh unpooled session and OOM-
    /// killing the process instead of erroring.
    fn shared_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.runtime_env
            .get_or_init(|| {
                let pool_size = self.config.derived.query_pool_bytes();
                use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool, TrackConsumersPool};
                // TrackConsumersPool: "Resources exhausted" errors name the top
                // pool holders. The 30G pool pinned to 100% twice on 2026-08-03
                // (07:06Z 45min, ~09:30Z), starving KB-scale DedupExec
                // reservations and failing enrichment UPDATEs — with a bare
                // Greedy pool the holder was unattributable.
                let top = std::num::NonZeroUsize::new(5).unwrap();
                let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = match self.config.memory.timefusion_memory_pool {
                    crate::config::MemoryPoolKind::Greedy => Arc::new(TrackConsumersPool::new(GreedyMemoryPool::new(pool_size), top)),
                    crate::config::MemoryPoolKind::FairSpill => Arc::new(TrackConsumersPool::new(FairSpillPool::new(pool_size), top)),
                };
                let meta_cache_bytes = self.config.cache.timefusion_df_metadata_cache_mb * 1024 * 1024;
                Arc::new(build_query_runtime_env(pool, meta_cache_bytes))
            })
            .clone()
    }

    /// Dedicated `RuntimeEnv` for maintenance jobs (optimize/dedup/recompress).
    /// Distinct from `shared_runtime_env` for two reasons the Z-order failures
    /// exposed: (1) a **FairSpill** pool fences off spillable memory per consumer,
    /// so the sort's `ExternalSorterMerge` can always reserve its floor and spill
    /// — a Greedy pool saturated by concurrent queries starves the merge and it
    /// errors with "Resources exhausted". (2) an **explicit on-disk spill dir**
    /// under the data dir, so spills hit the 120 GB data volume rather than a
    /// possibly RAM-backed container `/tmp` (spilling to RAM defeats the point).
    /// The pool is bounded (still pooled → fails-as-error, never OOM-kills, per
    /// the 2026-07-04 incident) and sized from the budget left over the query
    /// pool so query + maintenance together stay within `memory_limit`.
    fn build_spill_runtime_env(&self, pool_size: usize, spill_subdir: &str) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        use datafusion::execution::{
            disk_manager::{DiskManagerBuilder, DiskManagerMode},
            memory_pool::FairSpillPool,
            runtime_env::RuntimeEnvBuilder,
        };
        let spill_dir = self.config.core.timefusion_data_dir.join(spill_subdir);
        let _ = std::fs::create_dir_all(&spill_dir);
        reap_orphaned_spill_dirs(&spill_dir);
        let disk = DiskManagerBuilder::default().with_mode(DiskManagerMode::Directories(vec![spill_dir]));
        Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(pool_size)))
                .with_disk_manager_builder(disk)
                .build()
                .expect("build maintenance runtime env"),
        )
    }

    /// Light-optimize slice of the maintenance budget: one per-sort budget
    /// (1/3 of the pool) per concurrent hot-tail sort, capped so heavy
    /// maintenance always keeps at least 1/4. 1/4 (6GB of 24) was marginal:
    /// a single busiest-project bin sort peaked ~5.8GB (prod 2026-07-23,
    /// SortPreservingMerge exhaustion even with serial fan-out).
    fn light_optimize_pool_bytes(&self) -> usize {
        let pool = self.config.derived.maintenance_pool_bytes();
        (pool / 3 * self.config.derived.max_light_optimize_k()).min(pool * 3 / 4)
    }

    /// Heavy maintenance (dedup, recompress, Z-order): the budget left after
    /// the light-optimize slice.
    fn maintenance_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.maintenance_runtime_env
            .get_or_init(|| self.build_spill_runtime_env(self.config.derived.maintenance_pool_bytes() - self.light_optimize_pool_bytes(), "maintenance_spill"))
            .clone()
    }

    /// Hot-tail light optimize: the reserved slice (see field doc).
    fn light_optimize_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.light_optimize_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.light_optimize_pool_bytes(), "light_optimize_spill")).clone()
    }

    /// Sort one flush group, picking the strategy by size.
    ///
    /// Below `timefusion_sort_skip_bytes` the in-process sort wins on latency
    /// and this is the ingest path. Above it, escalate to the pooled+spilling
    /// DataFusion sort rather than SKIPPING the sort as this used to: a skipped
    /// sort writes a file with no `sorting_columns` footer, and one such file
    /// disables the reader's all-or-nothing ordering for every scan touching
    /// the partition. Spilling is the better failure mode.
    async fn sort_flush_group(&self, schema: &crate::schema_loader::TableSchema, batches: Vec<RecordBatch>) -> (FlushBatches, bool) {
        let ceiling = self.config.maintenance.timefusion_sort_skip_bytes;
        let total: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        if total <= ceiling || batches.is_empty() || schema.sorting_columns.is_empty() {
            // `usize::MAX`: the size decision is made here, so the in-process
            // helper must not second-guess it and silently skip.
            return sort_batches_by_schema(schema, batches, usize::MAX);
        }
        match self.sort_flush_group_spilling(schema, &batches).await {
            Some(sorted) => {
                debug!("flush sort: escalated {} MB group to the spilling DataFusion sort", total / (1 << 20));
                (FlushBatches::Ready(sorted.into_iter()), true)
            }
            // Never lose rows to a sort failure: write the originals unsorted.
            // Counted because one such file disables the reader's ordering for
            // its whole partition — this must never be silent (2026-08-03).
            None => {
                crate::metrics::record_flush_sort_unsorted_fallback();
                (FlushBatches::Ready(batches.into_iter()), false)
            }
        }
    }

    /// Flush-path sort pool (see field doc). Bounded and spillable, so an
    /// oversized bucket degrades to disk I/O instead of an unpooled spike.
    fn flush_sort_runtime_env(&self) -> Arc<datafusion::execution::runtime_env::RuntimeEnv> {
        self.flush_sort_runtime_env.get_or_init(|| self.build_spill_runtime_env(self.config.maintenance.flush_sort_pool_bytes(), "flush_sort_spill")).clone()
    }

    /// Sort an oversized flush group INSIDE a DataFusion plan.
    ///
    /// The in-process path (`sort_batches_by_schema`) allocates outside every
    /// memory pool, so past a point the only safe options are "skip the sort"
    /// (which writes a file with no `sorting_columns` footer, costing the
    /// reader's ordering) or this: a pooled sort that spills to disk. Spilling
    /// is strictly better than skipping — the footer stays honest and the peak
    /// is bounded by the pool rather than by the bucket.
    ///
    /// Returns `None` if anything goes wrong, so the caller falls back to
    /// writing the ORIGINAL batches unsorted. A flush must never lose rows to
    /// a sort failure.
    async fn sort_flush_group_spilling(&self, schema: &crate::schema_loader::TableSchema, batches: &[RecordBatch]) -> Option<Vec<RecordBatch>> {
        use datafusion::{datasource::MemTable, prelude::SessionContext};
        // Hold a slice of the shared spill pool for the whole sort. Queueing
        // here costs latency on an already-oversized group; losing the slice
        // costs the partition's footer ordering on every later scan.
        let _slice = self.flush_sort_gate.acquire().await.ok()?;
        let first = batches.first()?.schema();
        // Schema-diverse buckets (an evolved nullable column) must be unified
        // before MemTable will accept them; give up rather than guess.
        let arrow_schema = match batches.iter().all(|b| b.schema() == first) {
            true => first,
            false => Arc::new(arrow_schema::Schema::try_merge(batches.iter().map(|b| b.schema().as_ref().clone())).ok()?),
        };
        let unified: Vec<RecordBatch> = batches
            .iter()
            .map(|b| match b.schema() == arrow_schema {
                true => Ok(b.clone()),
                false => deltalake::kernel::schema::cast_record_batch(b, arrow_schema.clone(), true, true),
            })
            .collect::<Result<_, _>>()
            .ok()?;

        let order_by = schema
            .sorting_columns
            .iter()
            .filter(|c| arrow_schema.index_of(&c.name).is_ok())
            .map(|c| format!("\"{}\" {} NULLS {}", c.name, if c.descending { "DESC" } else { "ASC" }, if c.nulls_first { "FIRST" } else { "LAST" }))
            .collect::<Vec<_>>()
            .join(", ");
        if order_by.is_empty() {
            return None;
        }

        // ONE pool consumer per gate permit — the invariant `flush_sort_gate`'s
        // 512 MB-per-permit sizing assumes. With N>1 partitions this plan fans
        // out to N ExternalSorters (32 MB merge reservation each) plus an
        // UNSPILLABLE SortPreservingMergeExec, and concurrent escalations
        // starved the FairSpillPool into the unsorted fallback (prod
        // 2026-08-03). A single-partition sort is slower but has no merge exec
        // and spills within its fair share, so the footer stays honest.
        let state = build_delta_write_session_state(1, self.flush_sort_runtime_env());
        let ctx = SessionContext::new_with_state(state);
        let name = format!("flush_sort_{}", uuid::Uuid::new_v4().simple());
        ctx.register_table(&name, Arc::new(MemTable::try_new(arrow_schema, vec![unified]).ok()?)).ok()?;
        let out = ctx.sql(&format!("SELECT * FROM {name} ORDER BY {order_by}")).await.ok()?.collect().await;
        let _ = ctx.deregister_table(&name);
        match out {
            Ok(sorted) => Some(sorted),
            Err(e) => {
                warn!("flush sort: spilling DataFusion sort failed, writing unsorted: {e}");
                None
            }
        }
    }

    /// Heavy-maintenance session state, built once (see field doc).
    fn maintenance_session_state(&self) -> datafusion::execution::session_state::SessionState {
        self.maintenance_session_state
            .get_or_init(|| build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()))
            .clone()
    }

    /// Light-optimize session state, built once (see field doc).
    fn light_optimize_session_state(&self) -> datafusion::execution::session_state::SessionState {
        self.light_optimize_session_state
            .get_or_init(|| build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.light_optimize_runtime_env()))
            .clone()
    }

    /// The DML serialization mutex for the PHYSICAL table backing
    /// `(project_id, table_name)`. Unified tables are one shared Delta table
    /// across all default projects, so their key drops the project — two
    /// projects' merges on `otel_logs_and_spans` would otherwise run
    /// concurrently, OCC-conflict at the shared Delta log, and redo full
    /// parquet rewrites (observed as sustained `dml.conflict` in prod).
    /// Custom-storage tables are physically isolated and keep the full key.
    /// Physical-Delta-log lock key: collapses all default projects sharing a
    /// unified table onto one key (empty project_id — not a valid id, so it
    /// can't collide), while custom-storage tables keep per-project isolation.
    /// Shared by `dml_lock` and `commit_lock` so both serialize at
    /// physical-log granularity.
    async fn table_lock_key(&self, project_id: &str, table_name: &str) -> (String, String) {
        let project_key = if self.has_custom_storage(project_id, table_name).await { project_id.to_string() } else { String::new() };
        (project_key, table_name.to_string())
    }

    pub(crate) async fn dml_lock(&self, project_id: &str, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.dml_locks.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Per-physical-table Delta commit lock (see `commit_locks`).
    pub(crate) async fn commit_lock(&self, project_id: &str, table_name: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.commit_locks.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Waiter count for the SAME key as [`Self::commit_lock`] (see
    /// `flush_waiter_counts`). Flush/ingest commit paths register a
    /// [`FlushWaiter`] on it across their `lock().await`; `commit_wave` reads it
    /// and stands down while it is nonzero.
    pub(crate) async fn flush_waiters(&self, project_id: &str, table_name: &str) -> Arc<std::sync::atomic::AtomicUsize> {
        self.flush_waiter_counts.entry(self.table_lock_key(project_id, table_name).await).or_default().clone()
    }

    /// Persist `table`'s post-commit snapshot locally (detached) so the next
    /// boot restores it and replays only later commits (see `snapshot_cache`).
    /// Called from every commit path that swaps a fresh table state in.
    pub(crate) fn persist_snapshot(&self, table: &DeltaTable) {
        // Throttle: at most one persist per table per interval. The snapshot is
        // a boot-recovery seed, not a durability requirement, so skipping most
        // commits just replays a few extra commits on next boot (see field docs).
        const MIN_PERSIST_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        let url = table.table_url().to_string();
        let now = std::time::Instant::now();
        match self.snapshot_persist_gate.get(&url) {
            Some(last) if now.duration_since(*last) < MIN_PERSIST_INTERVAL => return,
            _ => {}
        }
        if let Some(state) = table.state.clone() {
            self.snapshot_persist_gate.insert(url.clone(), now);
            let dir = Self::delta_snapshot_dir(&self.config);
            tokio::task::spawn_blocking(move || crate::snapshot_cache::store(&dir, &url, &state));
        }
    }

    /// Materialize a table snapshot's active file list in memory. `reconcile`
    /// rebuilds it from object-store truth; otherwise it materializes once if
    /// not already done. No-op when the table carries no state.
    async fn materialize_snapshot_files(table: &mut DeltaTable, reconcile: bool) -> Result<()> {
        let log_store = table.log_store();
        match table.state.as_mut() {
            Some(state) if reconcile => state.rematerialize_files(log_store.as_ref()).await.map_err(Into::into),
            Some(state) => state.ensure_materialized_files(log_store.as_ref()).await.map_err(Into::into),
            None => Ok(()),
        }
    }

    /// Creates or loads a DeltaTable with proper configuration. Prefers the
    /// locally persisted snapshot (restore at version V + incremental replay
    /// of commits > V) over a full checkpoint + log-tail rebuild from S3;
    /// falls back to the full load on any restore failure.
    async fn create_or_load_delta_table(
        &self, storage_uri: &str, storage_options: HashMap<String, String>, cached_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<DeltaTable> {
        let url = Url::parse(storage_uri)?;
        let builder = || -> Result<DeltaTableBuilder> {
            Ok(DeltaTableBuilder::from_url(url.clone())?
                .with_storage_backend(cached_store.clone(), url.clone())
                .with_storage_options(storage_options.clone())
                .with_allow_http(true))
        };
        let restored = match crate::snapshot_cache::load(&Self::delta_snapshot_dir(&self.config), storage_uri) {
            Some(state) => {
                let restored_version = state.version();
                let mut table = builder()?.build()?;
                table.state = Some(state);
                // `update_state()` only probes versions *after* the supplied
                // state. It returns Ok when the local snapshot is ahead of the
                // durable log, even if its own commit disappeared (prod
                // 2026-08-04: local otel_metrics v140816, S3 ended at v140806).
                // Such a zombie snapshot serves removed files and makes every
                // subsequent commit fail with InvalidTableVersion. Require its
                // anchor commit to exist; if log cleanup legitimately removed
                // it behind a newer checkpoint, a full load is also the right
                // path because it starts from that durable checkpoint.
                match table.log_store().read_commit_entry(restored_version).await {
                    Ok(Some(_)) => table
                        .update_state()
                        .await
                        .inspect_err(|e| warn!("Local snapshot catch-up failed for '{storage_uri}': {e}; falling back to full load"))
                        .ok()
                        .map(|()| {
                            info!("Restored '{storage_uri}' from local snapshot at v{restored_version}, caught up to {:?}", table.version());
                            table
                        }),
                    Ok(None) => {
                        warn!("Local snapshot anchor v{restored_version} is absent for '{storage_uri}'; falling back to durable checkpoint/log load");
                        None
                    }
                    Err(e) => {
                        warn!(
                            "Could not validate local snapshot anchor v{restored_version} for '{storage_uri}': {e}; falling back to durable checkpoint/log load"
                        );
                        None
                    }
                }
            }
            None => None,
        };
        let mut table = match restored {
            Some(t) => t,
            None => builder()?.load().await.map_err(|e| anyhow::anyhow!("Failed to load table: {}", e))?,
        };
        // Materialize the file list once so every post-commit update stays
        // incremental. With incremental snapshots on this is a *correctness*
        // requirement, not just perf: a non-materialized snapshot enumerates an
        // EMPTY file set, and the fast-advance post-commit hook would build on
        // it — so fail loud rather than cache a handle that serves empty results
        // (the caller retries on next access). load()/restore normally arrive
        // materialized, so this no-ops and can only fail on the rare path that
        // actually has to materialize.
        if self.config.maintenance.timefusion_incremental_snapshot {
            Self::materialize_snapshot_files(&mut table, false)
                .await
                .map_err(|e| anyhow::anyhow!("Materializing file list for '{storage_uri}' failed: {e}"))?;
        }
        Ok(table)
    }

    /// Everything a staged (lock-free parquet upload) Delta write needs, built
    /// once per (project, table) unit. Shared by `insert_records_batch` and the
    /// cross-project coalesced flush path so both prepare writes identically.
    ///
    /// `staged_writer` is `None` when the fast path is unavailable — a batch
    /// carries a column the table schema lacks (delta-rs' Default-mode
    /// `RecordBatchWriter` cannot evolve schema on a partitioned table), or the
    /// writer could not be built at all. That unit must take the locked
    /// WriteBuilder merge path.
    async fn prepare_staged_write(&self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>) -> Result<PreparedWrite> {
        // Delta-kernel's `unshredded_variant()` expects Struct{Binary,Binary}
        // on write, but our MemBuffer carries Struct{BinaryView,BinaryView}
        // (matches what the parquet reader natively produces — no per-row
        // casts on read). Cast just-before-write so the Delta commit
        // accepts the schema.
        let batches: Vec<RecordBatch> = batches.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;

        // Get or create the table
        let table_ref = self.get_or_create_table(project_id, table_name).await?;

        // Get the appropriate schema for this table
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);

        let dirty_bins: Vec<(String, i64)> = if schema.dedup_keys.is_empty() {
            Vec::new()
        } else {
            const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
            batches
                .iter()
                .filter_map(|batch| batch.column_by_name("timestamp"))
                .filter_map(|column| column.as_any().downcast_ref::<datafusion::arrow::array::TimestampMicrosecondArray>())
                .flat_map(|timestamps| {
                    timestamps.iter().flatten().filter_map(|timestamp| {
                        chrono::DateTime::from_timestamp_micros(timestamp).map(|time| (time.date_naive().to_string(), timestamp.div_euclid(BIN_MICROS)))
                    })
                })
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect()
        };

        // Cluster by the declared sort keys (timestamp-first) so the parquet
        // SortingColumn footer is honest and the page index localizes the lead
        // key. `sorted` is false when a schema-evolved bucket can't be combined
        // (we then write unsorted) — declare the footer only when it's true.
        let (batches, sorted) = self.sort_flush_group(schema, batches).await;
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted);

        let staging_table = { table_ref.read().await.clone() };
        let stage_store = staging_table.log_store().object_store(None);
        let staged_writer = match deltalake::writer::RecordBatchWriter::for_table(&staging_table) {
            Ok(w) => {
                let w = w.with_writer_properties(writer_properties.clone());
                let arrow_schema = w.arrow_schema();
                let table_fields: std::collections::HashSet<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
                let evolves = batches.schemas().iter().any(|s| s.fields().iter().any(|f| !table_fields.contains(f.name().as_str())));
                (!evolves).then_some(w)
            }
            Err(e) => {
                debug!("RecordBatchWriter::for_table failed, using merge path: {}", e);
                None
            }
        };
        Ok(PreparedWrite { table_ref, schema, dirty_bins, batches, writer_properties, stage_store, staged_writer })
    }

    /// Insert batches and return the URIs of files newly added by this commit
    /// (empty for the buffered-layer / batch-queue paths where the actual
    /// Delta write happens later). Callers use the returned list to drive
    /// cache warming and the tantivy sidecar without paying for a second
    /// `update_state()` log scan.
    #[instrument(
        name = "delta.insert_batch",
        skip_all,
        fields(
            table.name = %table_name,
            project_id = %project_id,
            batches.count = batches.len(),
            rows.count = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            use_queue = Empty,
        )
    )]
    pub async fn insert_records_batch(
        &self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool, watermark: Option<&crate::buffered_write_layer::DeltaWatermark>,
    ) -> Result<Vec<String>> {
        self.insert_records_batch_bounded(project_id, table_name, batches, skip_queue, watermark, true).await
    }

    /// `bound: false` is for DML re-appends only — see
    /// [`crate::buffered_write_layer::BufferedWriteLayer::insert_bounded`].
    pub async fn insert_records_batch_bounded(
        &self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool,
        watermark: Option<&crate::buffered_write_layer::DeltaWatermark>, bound: bool,
    ) -> Result<Vec<String>> {
        let span = tracing::Span::current();
        // Normalize timezone-as-offset (`+00:00`) timestamp columns to the
        // IANA `"UTC"` form. Delta-rs Arrow→Delta schema conversion only
        // accepts `"UTC"`; without this normalisation the flush callback
        // path (which feeds MemBuffer batches straight into Delta) errors
        // out and data piles up in MemBuffer.
        let batches: Vec<RecordBatch> =
            batches.into_iter().map(normalize_timestamp_tz).map(|batch| batch.and_then(derive_date_partition)).collect::<DFResult<_>>()?;

        // Extract project_id from first batch if not provided. If neither the
        // caller nor the data carries one, log loudly and bucket under
        // "default" — silently misrouting writes is the worst outcome, but
        // returning an error would break callers that already rely on the
        // legacy fallback.
        let project_id = if project_id.is_empty() && !batches.is_empty() {
            extract_project_id(&batches[0]).unwrap_or_else(|| {
                warn!("insert_records_batch: empty project_id and batch has no project_id column → bucketing under 'default'");
                "default".to_string()
            })
        } else if project_id.is_empty() {
            warn!("insert_records_batch: empty project_id and no batches → bucketing under 'default'");
            "default".to_string()
        } else {
            project_id.to_string()
        };

        // Use provided table_name or default to otel_logs_and_spans
        let table_name = if table_name.is_empty() { "otel_logs_and_spans".to_string() } else { table_name.to_string() };

        // Stamp the schema's TF-owned version column. This is the single funnel
        // every *inbound* write passes through — pgwire INSERT (`write_all`),
        // the `__bulk` direct-to-Delta alias, gRPC ingest, the legacy batch
        // queue — regardless of whether the buffered layer is configured, and it
        // runs before the WAL append so the durable record carries the value.
        //
        // A `watermark` marks the one caller that is NOT inbound: the flush of
        // buffered rows back out to Delta (bucket flush, coalesced flush, boot
        // relief). Those rows were stamped on their way in and must keep that
        // value — a re-stamp would give a crash-retried flush a different value
        // than the WAL holds. WAL replay bypasses this function entirely and
        // seeds the clock via `insert_coerce::observe_batch` instead.
        let batches = if watermark.is_none() { crate::insert_coerce::stamp_version(&table_name, batches) } else { batches };

        // If buffered layer is configured and not skipping, use it (WAL → MemBuffer flow).
        // No files are written synchronously on this path; an empty URI list is correct.
        if !skip_queue && let Some(layer) = self.buffered_layer() {
            span.record("use_queue", "buffered_layer");
            layer.insert_bounded(&project_id, &table_name, batches, bound).await?;
            return Ok(Vec::new());
        }

        // Fallback to legacy batch queue if configured
        let enable_queue = self.config.core.enable_batch_queue;
        if !skip_queue
            && enable_queue
            && let Some(ref queue) = self.batch_queue
        {
            span.record("use_queue", true);
            for batch in batches {
                if let Err(e) = queue.queue(batch) {
                    return Err(anyhow::anyhow!("Queue error: {}", e));
                }
            }
            return Ok(Vec::new());
        }

        span.record("use_queue", false);

        let PreparedWrite { table_ref, schema, dirty_bins, batches, writer_properties, stage_store, staged_writer } =
            self.prepare_staged_write(&project_id, &table_name, batches).await?;

        // Hoist out of the retry loop — the watermark is the same on every attempt.
        let commit_properties = watermark.map(|w| build_watermark_commit_properties([(project_id.clone(), table_name.clone(), w.clone())]));
        // Let the post-commit hook advance the snapshot incrementally — carry
        // the materialized file list forward, append the committed files, drop
        // any removed ones — instead of re-materializing the whole active set.
        // Safe for the staged (pure-append) and schema-evolution merge paths
        // alike: the hook rebuilds the kernel snapshot from the log, so a
        // MetaData/schema change IS applied; only the file-list re-materialize
        // is skipped.
        let commit_properties = if self.config.maintenance.timefusion_incremental_snapshot {
            Some(commit_properties.unwrap_or_else(base_commit_properties).with_incremental_advance(true))
        } else {
            commit_properties
        };
        let max_retries = 5;
        // STAGED COMMIT (fast path): encode parquet + upload to S3 OUTSIDE the
        // per-table commit lock, then serialize only the tiny commit-log
        // append. The old path held the lock across the whole `.write()`
        // (parquet encode + S3 upload + commit), serializing every tenant's
        // upload behind one mutex — the ~8-17 rows/s flush ceiling under heavy
        // backfill. A staged write parallelizes the uploads and pays the lock
        // only for a sub-second log append; OCC conflicts re-commit the already
        // uploaded parquet (no re-encode/re-upload).
        //
        // delta-rs' Default-mode RecordBatchWriter cannot evolve schema on a
        // partitioned table, so when a batch carries a column absent from the
        // table schema `prepare_staged_write` returns no staged writer and we
        // fall back to the locked WriteBuilder merge path below.
        if let Some(mut writer) = staged_writer {
            use deltalake::{
                kernel::{Action, transaction::TableReference},
                protocol::DeltaOperation,
                writer::DeltaWriter,
            };

            // Upload parquet (no commit) on the staging clone — outside the lock.
            // RecordBatchWriter (unlike WriteBuilder) doesn't cast the batch to
            // the table schema, so cast each batch to the table's arrow schema
            // first — Utf8View→Utf8 etc, filling any missing column with nulls
            // (safe=true, add_missing=true mirrors WriteBuilder's own coercion).
            let target_schema = writer.arrow_schema();
            let stage_span = tracing::trace_span!(parent: &span, "delta.stage_parquet");
            let adds: Vec<Action> = async {
                for b in batches {
                    let casted = deltalake::kernel::schema::cast_record_batch(&b?, target_schema.clone(), true, true)?;
                    writer.write(casted).await?;
                }
                writer.flush().await
            }
            .instrument(stage_span)
            .await
            .map_err(|e| anyhow::anyhow!("staged parquet flush failed: {}", e))?
            .into_iter()
            .map(Action::Add)
            .collect();
            if adds.is_empty() {
                return Ok(Vec::new());
            }

            let partition_by = (!schema.partitions.is_empty()).then(|| schema.partitions.clone());
            let op = DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by, predicate: None };
            // Store to clean up the staged parquet on a terminal commit failure —
            // those objects have no Add/Remove in the log, so Delta VACUUM won't
            // reclaim them; abandoning them leaks files on S3 forever.
            let stage_store = stage_store.clone();

            let commit_lock = self.commit_lock(&project_id, &table_name).await;
            let flush_waiters = self.flush_waiters(&project_id, &table_name).await;
            let mut retry_count = 0;
            loop {
                // Refresh UNDER the lock (the merge path refreshes before locking).
                // The per-table commit lock serializes all in-process commits to
                // THIS log, so refreshing here guarantees we build on the previous
                // committer's version and never self-conflict; refresh is
                // probe-cheap (a single GET that 404-short-circuits when already
                // current), so the extra lock-hold is sub-millisecond on the common
                // path.
                // FLUSH PRIORITY: registered across the WAIT only. Waves stand
                // down while this is nonzero (see `flush_waiter_counts`), so the
                // count must fall the moment we hold the lock — or the moment a
                // watchdog cancels this future.
                let commit_guard = {
                    let _waiting = FlushWaiter::register(&flush_waiters);
                    commit_lock.lock().await
                };
                // DIAG (commit-throughput profiling): time the serial commit phases
                // (refresh + Delta log append) under the lock — these bound the
                // process-wide commit rate. Remove once the flush bottleneck is found.
                let _t_refresh = std::time::Instant::now();
                if let Err(e) = bounded_commit_await(
                    COMMIT_LOCK_OP_TIMEOUT,
                    "flush_refresh",
                    &table_name,
                    refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot),
                )
                .await
                {
                    debug!("pre-commit refresh failed (attempt {}): {}", retry_count + 1, e.message);
                }
                let _refresh_ms = _t_refresh.elapsed().as_millis();
                let mut new_table = { table_ref.read().await.clone() };
                let _t_build = std::time::Instant::now();
                // Bounded for the same reason as the wave path: this await holds
                // the per-table commit lock every other committer queues on.
                let commit_res = bounded_commit_await(
                    COMMIT_LOCK_OP_TIMEOUT,
                    "flush_commit",
                    &table_name,
                    deltalake::kernel::transaction::CommitBuilder::from(commit_properties.clone().unwrap_or_else(base_commit_properties))
                        .with_actions(adds.clone())
                        .build(Some(new_table.snapshot()? as &dyn TableReference), new_table.log_store(), op.clone()),
                )
                .await;
                let _build_ms = _t_build.elapsed().as_millis();
                match commit_res {
                    Ok(finalized) => {
                        // Diff pre- vs post-commit file URIs for `added`. Capture
                        // pre-uris here (only on success) — before the state swap
                        // below makes `new_table` post-commit — so failed attempts
                        // don't pay the full-table file-URI walk.
                        let pre_uris: std::collections::HashSet<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                        new_table.state = Some(finalized.snapshot());
                        drop(commit_guard);
                        let _t_record = std::time::Instant::now();
                        let _committed = self
                            .record_committed_write(
                                &table_ref,
                                &[(project_id.as_str(), dirty_bins.as_slice())],
                                &table_name,
                                new_table,
                                &pre_uris,
                                watermark.is_some(),
                            )
                            .await;
                        info!(
                            "commit_timing project={} table={} refresh_ms={} build_ms={} record_ms={} files={}",
                            project_id,
                            table_name,
                            _refresh_ms,
                            _build_ms,
                            _t_record.elapsed().as_millis(),
                            adds.len()
                        );
                        return Ok(_committed);
                    }
                    Err(CommitFailure { message: e, timed_out }) => {
                        drop(commit_guard);
                        if !timed_out && is_occ_conflict_err(&e) {
                            retry_count += 1;
                            if retry_count >= max_retries {
                                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                                return Err(anyhow::anyhow!("staged commit failed after {} retries: {}", max_retries, e));
                            }
                            debug!("staged commit conflict, retrying ({}/{}): {}", retry_count, max_retries, e);
                            tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                            continue;
                        }
                        // Non-OCC error: the commit MAY have landed (post-commit
                        // hook / snapshot refresh failed AFTER N.json was written).
                        // Capture the pre-commit file set from the still-pre-commit
                        // clone (only on this rare branch — the OCC-retry path must
                        // not pay the full-table URI walk), then probe.
                        let pre_uris: std::collections::HashSet<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                        match probe_after_timeout(self.probe_commit_landed_bounded(&table_ref, &adds).await, timed_out) {
                            CommitProbe::Landed => {
                                warn!(
                                    "staged commit for {}/{} reported an error but LANDED (post-commit hook failed) — draining bucket: {}",
                                    project_id, table_name, e
                                );
                                let post = { table_ref.read().await.clone() };
                                let committed = self
                                    .record_committed_write(
                                        &table_ref,
                                        &[(project_id.as_str(), dirty_bins.as_slice())],
                                        &table_name,
                                        post,
                                        &pre_uris,
                                        watermark.is_some(),
                                    )
                                    .await;
                                return Ok(committed);
                            }
                            CommitProbe::NotLanded => {
                                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                                return Err(anyhow::anyhow!("staged commit failed: {}", e));
                            }
                            CommitProbe::Inconclusive => {
                                warn!(
                                    "staged commit for {}/{} errored and landing is UNCONFIRMED (snapshot read failed) — leaving staged parquet in place to avoid a dangling Add: {}",
                                    project_id, table_name, e
                                );
                                return Err(anyhow::anyhow!("staged commit failed (landing unconfirmed): {}", e));
                            }
                        }
                    }
                }
            }
        }

        // SCHEMA-EVOLUTION FALLBACK: locked WriteBuilder merge path. Holds the
        // commit lock across the whole write so the schema-metadata merge can't
        // race a concurrent commit. Rare (only when a batch adds a column).
        //
        // WriteBuilder re-submits the same rows on every OCC retry, so the lazy
        // sort-merge has to be materialized once here — this path keeps the old
        // whole-bucket residency by necessity. It is unreachable when a staged
        // writer exists (the block above always returns).
        let batches: Vec<RecordBatch> = batches.collect::<Result<_, _>>()?;
        let commit_lock = self.commit_lock(&project_id, &table_name).await;
        let flush_waiters = self.flush_waiters(&project_id, &table_name).await;
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count < max_retries {
            if let Err(e) = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                debug!("Failed to update table state before write (attempt {}): {}", retry_count + 1, e);
            }
            let commit_guard = {
                let _waiting = FlushWaiter::register(&flush_waiters);
                commit_lock.lock().await
            };
            let (table, pre_uris) = {
                let guard = table_ref.read().await;
                let pre: std::collections::HashSet<String> = guard.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                (guard.clone(), pre)
            };

            let write_span = tracing::trace_span!(parent: &span, "delta.write_operation", retry_attempt = retry_count + 1);
            let write_result = async {
                table
                    .clone()
                    .write(batches.clone())
                    .with_partition_columns(schema.partitions.clone())
                    .with_writer_properties(writer_properties.clone())
                    .with_save_mode(deltalake::protocol::SaveMode::Append)
                    .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
                    // Always set base properties (hooks off) — a None here would
                    // let WriteBuilder's own default re-enable the checkpoint hook.
                    .with_commit_properties(commit_properties.clone().unwrap_or_else(base_commit_properties))
                    .await
            }
            .instrument(write_span)
            .await;

            match write_result {
                Ok(new_table) => {
                    let added = self
                        .record_committed_write(
                            &table_ref,
                            &[(project_id.as_str(), dirty_bins.as_slice())],
                            &table_name,
                            new_table,
                            &pre_uris,
                            watermark.is_some(),
                        )
                        .await;
                    return Ok(added);
                }
                Err(e) => {
                    if is_occ_conflict_err(&e.to_string()) {
                        retry_count += 1;
                        last_error = Some(e);
                        debug!("Delta write conflict detected, retrying... (attempt {}/{})", retry_count, max_retries);
                        // Release the commit lock BEFORE the backoff sleep — do
                        // not remove. Holding it across the sleep serializes
                        // every other writer behind this writer's backoff.
                        drop(commit_guard);
                        tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                        drop(table); // stale clone — the retry re-clones after the reload
                        if let Err(reload_err) = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                            debug!("Failed to reload table state after conflict: {}", reload_err);
                        }
                    } else {
                        return Err(anyhow::anyhow!("Delta write failed: {}", e));
                    }
                }
            }
        }

        Err(anyhow::anyhow!(
            "Delta write failed after {} retries: {}",
            max_retries,
            last_error.map(|e| e.to_string()).unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    /// Cross-project flush commit coalescing (C3).
    ///
    /// One tick's per-project flush units become ONE Delta commit per PHYSICAL
    /// table. All default-storage projects share a single `_delta_log`, and
    /// `table_lock_key` already funnels their commits through one mutex — so the
    /// per-project commits were serialized anyway and merely multiplied log
    /// entries, snapshot refreshes and Delta-log JSON parses. Custom-storage
    /// projects have their own log and keep their own commit (the grouping key
    /// IS `table_lock_key`, so isolation is structural, not a special case).
    ///
    /// Parallelism is unchanged in the part that matters: parquet encode +
    /// upload still fan out `flush_parallelism`-wide per unit, OUTSIDE any lock.
    /// Only the commit-log append is shared.
    ///
    /// Isolation guarantees, matching the per-project path exactly:
    /// - a unit whose parquet staging fails is excluded from the commit and gets
    ///   its own `Err` — the other projects still commit;
    /// - a shared commit failure fails EVERY unit in that physical group
    ///   identically (no partial settle: the caller requeues them all);
    /// - a unit needing schema evolution is split OUT of the coalesced group and
    ///   committed on its own via the locked WriteBuilder merge path, rather
    ///   than dragging its co-tenants through it.
    ///
    /// Returns one result per input unit, in input order. `Ok` carries the URIs
    /// of files that unit added (attributed by `project_id=` partition path when
    /// the commit spanned projects, so the tantivy sidecar still indexes each
    /// project's own files).
    pub async fn insert_records_batches_coalesced(&self, units: Vec<CoalescedWriteUnit>) -> Vec<Result<Vec<String>>> {
        use deltalake::{kernel::Action, protocol::DeltaOperation, writer::DeltaWriter};
        use futures::stream::{self, StreamExt};
        let parallelism = self.config.buffer.flush_parallelism();
        let mut results: Vec<Result<Vec<String>>> = units.iter().map(|_| Ok(Vec::new())).collect();
        let units = std::sync::Arc::new(units);

        // ---- Phase 1: prepare (bounded-concurrent; table resolution + casts).
        let prepared: Vec<(usize, Result<PreparedForPhysicalTable>)> = stream::iter(0..units.len())
            .map(|i| {
                let units = units.clone();
                async move {
                    let u = &units[i];
                    let prep = self.prepare_staged_write(&u.project_id, &u.table_name, u.batches.clone()).await;
                    let key = self.table_lock_key(&u.project_id, &u.table_name).await;
                    (i, prep.map(|p| (p, key)))
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // ---- Phase 2: stage parquet OUTSIDE any lock, `flush_parallelism`-wide.
        // Schema-evolution units never reach here: they are split out to the solo
        // (locked WriteBuilder) path so one project's merge can't stall the rest.
        let mut solo: Vec<usize> = Vec::new();
        let mut stageable: Vec<(usize, PreparedWrite, (String, String))> = Vec::new();
        for (i, prep) in prepared {
            match prep {
                Err(e) => results[i] = Err(e),
                Ok((p, _)) if p.staged_writer.is_none() => {
                    debug!("coalesced flush: {}/{} needs schema evolution — splitting out of the shared commit", units[i].project_id, units[i].table_name);
                    drop(p);
                    solo.push(i);
                }
                Ok((p, key)) => stageable.push((i, p, key)),
            }
        }

        let staged: Vec<(usize, (String, String), Result<StagedUnit>)> = stream::iter(stageable)
            .map(|(i, prep, key)| async move {
                let PreparedWrite { table_ref, schema, dirty_bins, batches, stage_store, staged_writer, .. } = prep;
                let mut writer = staged_writer.expect("filtered above");
                // RecordBatchWriter (unlike WriteBuilder) doesn't cast the batch
                // to the table schema — cast first (Utf8View→Utf8 etc, missing
                // columns filled with nulls), mirroring the per-project path.
                let target_schema = writer.arrow_schema();
                let adds = async {
                    for b in batches {
                        let casted = deltalake::kernel::schema::cast_record_batch(&b?, target_schema.clone(), true, true)?;
                        writer.write(casted).await?;
                    }
                    writer.flush().await
                }
                .await
                .map(|adds| adds.into_iter().map(Action::Add).collect::<Vec<Action>>())
                .map_err(|e| anyhow::anyhow!("staged parquet flush failed: {}", e));
                (i, key, adds.map(|adds| StagedUnit { table_ref, schema, dirty_bins, adds, stage_store }))
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;

        // ---- Phase 3: one commit per PHYSICAL table.
        let mut by_physical: std::collections::HashMap<(String, String), Vec<(usize, StagedUnit)>> = std::collections::HashMap::new();
        for (i, key, unit) in staged {
            match unit {
                Err(e) => results[i] = Err(e),
                // Nothing was written (all rows filtered out) — no Add to commit.
                Ok(u) if u.adds.is_empty() => results[i] = Ok(Vec::new()),
                Ok(u) => by_physical.entry(key).or_default().push((i, u)),
            }
        }

        let committed: Vec<Vec<(usize, Result<Vec<String>>)>> = stream::iter(by_physical.into_values())
            .map(|group| {
                let units = units.clone();
                async move {
                    let indices: Vec<usize> = group.iter().map(|(i, _)| *i).collect();
                    let table_name = units[indices[0]].table_name.clone();
                    let projects: Vec<&str> = indices.iter().map(|i| units[*i].project_id.as_str()).collect();
                    let table_ref = group[0].1.table_ref.clone();
                    let schema = group[0].1.schema;
                    let adds: Vec<Action> = group.iter().flat_map(|(_, u)| u.adds.iter().cloned()).collect();
                    let watermarks = indices.iter().map(|i| (units[*i].project_id.clone(), units[*i].table_name.clone(), units[*i].watermark.clone()));
                    let commit_properties = build_watermark_commit_properties(watermarks);
                    let commit_properties = if self.config.maintenance.timefusion_incremental_snapshot {
                        commit_properties.with_incremental_advance(true)
                    } else {
                        commit_properties
                    };
                    let per_project: Vec<(&str, &[(String, i64)])> =
                        group.iter().map(|(i, u)| (units[*i].project_id.as_str(), u.dirty_bins.as_slice())).collect();
                    let partition_by = (!schema.partitions.is_empty()).then(|| schema.partitions.clone());
                    let op = DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by, predicate: None };

                    let outcome = self
                        .commit_coalesced_group(&table_ref, &per_project, &table_name, adds.clone(), commit_properties, op)
                        .await
                        .map(|added| attribute_added_files(added, &projects));
                    match outcome {
                        Ok(per_project_added) => indices.into_iter().zip(per_project_added).map(|(i, a)| (i, Ok(a))).collect::<Vec<_>>(),
                        Err(e) => {
                            // Fail EVERY project in the group identically — no
                            // partial settle. The caller requeues each one's buckets
                            // with unchanged retry semantics.
                            if !e.to_string().contains(INCONCLUSIVE_COMMIT_MARKER) {
                                // Every unit in a physical group stages into the SAME
                                // store (same Delta table), so one store deletes all.
                                Self::cleanup_orphaned_parquet(&group[0].1.stage_store, &adds).await;
                            }
                            indices.into_iter().map(|i| (i, Err(anyhow::anyhow!("coalesced commit failed for {}: {}", table_name, e)))).collect()
                        }
                    }
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;
        for (i, r) in committed.into_iter().flatten() {
            results[i] = r;
        }

        // ---- Phase 4: schema-evolution units, each on its own (locked merge path).
        let solo_results: Vec<(usize, Result<Vec<String>>)> = stream::iter(solo)
            .map(|i| {
                let units = units.clone();
                async move {
                    let u = &units[i];
                    (i, self.insert_records_batch(&u.project_id, &u.table_name, u.batches.clone(), true, Some(&u.watermark)).await)
                }
            })
            .buffer_unordered(parallelism)
            .collect()
            .await;
        for (i, r) in solo_results {
            results[i] = r;
        }
        results
    }

    /// The shared commit-log append for one physical table's coalesced group.
    /// Mirrors the per-project staged-commit loop (same OCC retry budget +
    /// backoff, same landed-despite-error probe); the only difference is that
    /// the actions and the watermark metadata span several projects. Cleanup of
    /// staged parquet is the caller's (it owns every unit's store).
    async fn commit_coalesced_group(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, projects: &[(&str, &[(String, i64)])], table_name: &str, adds: Vec<deltalake::kernel::Action>,
        commit_properties: CommitProperties, op: deltalake::protocol::DeltaOperation,
    ) -> Result<Vec<String>> {
        use deltalake::kernel::transaction::TableReference;
        const MAX_RETRIES: u32 = 5;
        // Any member resolves to the same physical lock (the group key IS
        // `table_lock_key`), so serialization is identical to the per-project path.
        let commit_lock = self.commit_lock(projects[0].0, table_name).await;
        let flush_waiters = self.flush_waiters(projects[0].0, table_name).await;
        let mut retry_count = 0u32;
        loop {
            let commit_guard = {
                let _waiting = FlushWaiter::register(&flush_waiters);
                commit_lock.lock().await
            };
            if let Err(e) = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "coalesced_refresh",
                table_name,
                refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot),
            )
            .await
            {
                debug!("pre-commit refresh failed (attempt {}): {}", retry_count + 1, e.message);
            }
            let mut new_table = { table_ref.read().await.clone() };
            let commit_res = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "coalesced_commit",
                table_name,
                deltalake::kernel::transaction::CommitBuilder::from(commit_properties.clone()).with_actions(adds.clone()).build(
                    Some(new_table.snapshot()? as &dyn TableReference),
                    new_table.log_store(),
                    op.clone(),
                ),
            )
            .await;
            match commit_res {
                Ok(finalized) => {
                    let pre_uris: std::collections::HashSet<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                    new_table.state = Some(finalized.snapshot());
                    drop(commit_guard);
                    let added = self.record_committed_write(table_ref, projects, table_name, new_table, &pre_uris, true).await;
                    debug!("coalesced commit landed: table={} projects={} files={}", table_name, projects.len(), adds.len());
                    return Ok(added);
                }
                Err(CommitFailure { message: e, timed_out }) => {
                    drop(commit_guard);
                    if !timed_out && is_occ_conflict_err(&e) {
                        retry_count += 1;
                        if retry_count >= MAX_RETRIES {
                            return Err(anyhow::anyhow!("coalesced staged commit failed after {} retries: {}", MAX_RETRIES, e));
                        }
                        debug!("coalesced commit conflict, retrying ({}/{}): {}", retry_count, MAX_RETRIES, e);
                        tokio::time::sleep(occ_backoff(retry_count as usize)).await;
                        continue;
                    }
                    // Non-OCC: the commit MAY have landed (post-commit hook failed
                    // after N.json was written). Same three-way probe as the
                    // per-project path — never delete parquet a landed commit
                    // references.
                    let pre_uris: std::collections::HashSet<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                    match probe_after_timeout(self.probe_commit_landed_bounded(table_ref, &adds).await, timed_out) {
                        CommitProbe::Landed => {
                            warn!("coalesced commit for {} reported an error but LANDED (post-commit hook failed) — draining: {}", table_name, e);
                            let post = { table_ref.read().await.clone() };
                            return Ok(self.record_committed_write(table_ref, projects, table_name, post, &pre_uris, true).await);
                        }
                        CommitProbe::NotLanded => return Err(anyhow::anyhow!("coalesced staged commit failed: {}", e)),
                        CommitProbe::Inconclusive => {
                            warn!("coalesced commit for {} errored and landing is UNCONFIRMED — leaving staged parquet in place: {}", table_name, e);
                            // Signal "do not delete the parquet" by returning a
                            // distinct marker error the caller checks.
                            return Err(anyhow::anyhow!("{}: coalesced staged commit failed (landing unconfirmed): {}", INCONCLUSIVE_COMMIT_MARKER, e));
                        }
                    }
                }
            }
        }
    }

    /// Probe whether a staged commit landed despite returning an error: refresh
    /// the snapshot from the log and check that every Add we tried to commit is
    /// now active. `Landed` ⇒ treat as success (drain the bucket); `NotLanded` ⇒
    /// safe to delete the staged parquet; `Inconclusive` ⇒ the refresh/read
    /// itself failed, so we can't confirm — leak the parquet rather than risk
    /// deleting files a landed commit references.
    /// `probe_commit_landed` under the same last-resort bound. Every caller
    /// reaches it on an already-degraded store; its log reads are commit-log
    /// class (30s per request), so this only catches a hang that escapes the
    /// client. A probe that times out is `Inconclusive` by construction —
    /// never `NotLanded`, which would authorize deleting staged parquet.
    async fn probe_commit_landed_bounded(&self, table_ref: &Arc<RwLock<DeltaTable>>, adds: &[deltalake::kernel::Action]) -> CommitProbe {
        match tokio::time::timeout(COMMIT_LOCK_OP_TIMEOUT, self.probe_commit_landed(table_ref, adds)).await {
            Ok(probe) => probe,
            Err(_) => {
                crate::metrics::record_commit_timeout("landing_probe");
                CommitProbe::Inconclusive
            }
        }
    }

    async fn probe_commit_landed(&self, table_ref: &Arc<RwLock<DeltaTable>>, adds: &[deltalake::kernel::Action]) -> CommitProbe {
        use deltalake::kernel::Action;
        if refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await.is_err() {
            return CommitProbe::Inconclusive;
        }
        let our_paths: Vec<&str> = adds
            .iter()
            .filter_map(|a| match a {
                Action::Add(add) => Some(add.path.as_str()),
                _ => None,
            })
            .collect();
        if our_paths.is_empty() {
            return CommitProbe::NotLanded;
        }
        let guard = table_ref.read().await;
        let Ok(snap) = guard.snapshot() else {
            return CommitProbe::Inconclusive;
        };
        let active: std::collections::HashSet<String> = snap.log_data().iter().map(|f| f.path().into_owned()).collect();
        if our_paths.iter().all(|p| active.contains(*p)) { CommitProbe::Landed } else { CommitProbe::NotLanded }
    }

    /// Best-effort delete of staged-but-uncommitted parquet after a terminal
    /// staged-commit failure. Those objects have no Add/Remove action in the
    /// Delta log, so VACUUM never reclaims them — abandoning them leaks files on
    /// S3 forever. Logs any path it couldn't remove so an operator can clean up.
    async fn cleanup_orphaned_parquet(store: &Arc<dyn object_store::ObjectStore>, adds: &[deltalake::kernel::Action]) {
        use object_store::ObjectStoreExt; // dyn-safe `delete` wrapper
        for action in adds {
            if let deltalake::kernel::Action::Add(add) = action {
                let path = object_store::path::Path::from(add.path.as_str());
                if let Err(e) = store.delete(&path).await {
                    warn!("orphaned staged parquet (manual cleanup needed): {} — delete failed: {}", add.path, e);
                }
            }
        }
    }

    /// Shared post-commit bookkeeping for both the staged and merge write
    /// paths: record the version for read-after-write, swap the shared handle
    /// (version-guarded), warm the just-written files, invalidate stats, and
    /// return the newly added file URIs.
    ///
    /// `projects` is every (project_id, dirty_bins) the commit carried — one on
    /// the per-project path, N on a coalesced cross-project commit. Everything
    /// per-project (last-written version for read-after-write, statistics
    /// invalidation, dirty-bin enqueue) runs once per entry; the table-wide work
    /// (snapshot persist, handle swap, warm, reconcile) runs once for the commit.
    #[allow(clippy::too_many_arguments)]
    async fn record_committed_write(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, projects: &[(&str, &[(String, i64)])], table_name: &str, new_table: DeltaTable,
        pre_uris: &std::collections::HashSet<String>, warm: bool,
    ) -> Vec<String> {
        // Jitter anchor + logging identity: any member of the physical group is
        // equivalent (they all commit to the same log).
        let project_id = projects.first().map(|(p, _)| *p).unwrap_or("");
        let committed_version = new_table.version();
        if let Some(version) = committed_version {
            let mut versions = self.last_written_versions.write().await;
            for (project, _) in projects {
                versions.insert((project.to_string(), table_name.to_string()), version);
            }
            debug!("Stored last written version for {}/{} (+{} coalesced): {}", project_id, table_name, projects.len().saturating_sub(1), version);
        } else {
            debug!("WARNING: No version available after write for {}/{}", project_id, table_name);
        }
        let added: Vec<String> = new_table.get_file_uris().map(|it| it.filter(|u| !pre_uris.contains(u)).collect()).unwrap_or_default();
        // Capture the store off the committed handle so the warm task never
        // re-resolves the table (a possible PG roundtrip + Delta state reload).
        let (warm_store, warm_table_uri) = (new_table.log_store().object_store(None), new_table.table_url().to_string());
        self.persist_snapshot(&new_table);
        // Brief write lock for the swap only. Version-guarded: a concurrent
        // maintenance commit may have advanced the shared handle past ours.
        {
            let mut shared = table_ref.write().await;
            if new_table.version() > shared.version() {
                *shared = new_table;
            }
        }
        // Freshly-flushed files are queried next; warm them now (repeat queries
        // measured ~300 ms cold vs 8 ms warm on R2). Gated on `warm` (only the
        // BufferedWriteLayer flush path sets it): direct inserts — tests, tools
        // — must not spawn detached warm tasks whose in-flight connections
        // outlive a short-lived runtime and poison the shared client pool.
        if warm {
            // Influx-oracle ordering: the MemBuffer prefix drains right after
            // this returns (`settle_flushed_group`), so on the flush path we
            // confirm the new files are cached BEFORE that handoff — a detached
            // warm loses the race and the next dashboard query pays an R2
            // first-byte per fresh file. Bounded + best-effort: it can never
            // fail the commit. Same warm path either way — only WHEN it returns
            // differs.
            let warm_added = added.clone();
            if self.object_store_cache.is_some() {
                // Establish header/footer coverage before the MemBuffer drains.
                // Full bodies are warmed by the normal detached path below;
                // the confirm must never make flush durability depend on R2.
                self.warm_cache_for_uris(warm_store.clone(), warm_table_uri.clone(), warm_added.clone(), Some(crate::config::CACHE_CONFIRM_TIMEOUT), false)
                    .await;
                let db = self.clone();
                let shutdown = self.maintenance_shutdown.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = shutdown.cancelled() => {}
                        _ = db.warm_cache_for_uris(warm_store, warm_table_uri, warm_added, None, true) => {}
                    }
                });
            } else {
                let db = self.clone();
                let shutdown = self.maintenance_shutdown.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = shutdown.cancelled() => {}
                        _ = db.warm_cache_for_uris(warm_store, warm_table_uri, warm_added, None, true) => {}
                    }
                });
            }
        }
        for (project, dirty_bins) in projects {
            self.statistics_extractor.invalidate(project, table_name).await;
            for (date, bin) in *dirty_bins {
                self.enqueue_dirty_bin(project, table_name, date, *bin);
            }
        }
        debug!("Invalidated statistics cache after write to {}/{}", project_id, table_name);
        // Periodic reconcile, OFF the flush path: every Nth commit (offset per
        // table so tables with uniform write rates don't all rebuild at once)
        // rebuild the file list from S3 truth in the background. This bounds any
        // incremental-replay drift without blocking the WAL cursor, and runs on
        // a detached clone so it never touches `added` (tantivy coverage) or the
        // persisted snapshot — both already captured from the committed state.
        let reconcile_n = self.config.maintenance.timefusion_snapshot_reconcile_commits;
        if self.config.maintenance.timefusion_incremental_snapshot
            && reconcile_n > 0
            && committed_version.is_some_and(|v| (v + Self::reconcile_offset(project_id, table_name, reconcile_n)).is_multiple_of(reconcile_n))
        {
            let (table_ref, shutdown) = (table_ref.clone(), self.maintenance_shutdown.clone());
            let (project_id, table_name) = (project_id.to_string(), table_name.to_string());
            tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = Self::reconcile_snapshot(&table_ref, &project_id, &table_name) => {}
                }
            });
        }
        added
    }

    /// Stable per-table offset into the reconcile cycle so tables committing in
    /// lockstep don't all hit their `% reconcile_n == 0` boundary together.
    fn reconcile_offset(project_id: &str, table_name: &str, reconcile_n: u64) -> u64 {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut h = DefaultHasher::new();
        (project_id, table_name).hash(&mut h);
        h.finish() % reconcile_n
    }

    /// Rebuild a table's in-memory file list from object-store truth and swap it
    /// in — but only if no commit advanced the handle while we rebuilt, since a
    /// rebuild is pinned to its version and a stale swap would drop newer files.
    /// Runs detached (off the flush path); never persists (the commit path
    /// already persisted the correct incremental state).
    async fn reconcile_snapshot(table_ref: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) {
        let mut fresh = table_ref.read().await.clone();
        if let Err(e) = Self::materialize_snapshot_files(&mut fresh, true).await {
            warn!("Snapshot reconcile failed for {project_id}/{table_name}: {e}");
            return;
        }
        let fresh_version = fresh.version();
        let mut shared = table_ref.write().await;
        if fresh_version == shared.version() {
            *shared = fresh;
            debug!("Reconciled snapshot for {project_id}/{table_name} at v{fresh_version:?}");
        }
    }

    /// Read the latest commit metadata for each WAL topic and fast-forward the
    /// walrus persisted-read cursor to `max(local, delta)` per shard. Closes
    /// the crash-mid-flush window where Delta committed but the watermark advance
    /// didn't finish — without this, restart replays entries already in Delta
    /// and the next flush writes them a second time.
    ///
    /// Must run *before* `recover_from_wal`. Best-effort: any failure to read
    /// metadata is logged and skipped (walrus's locally-fsynced cursor wins),
    /// so this can't make recovery worse than today's at-least-once behaviour.
    pub async fn derive_wal_cursors_from_delta(&self, wal: &crate::wal::WalManager) -> anyhow::Result<usize> {
        use futures::stream::{self, StreamExt};

        // Group logical WAL topics by physical Delta log. Default-storage
        // projects share one unified table, so opening and scanning that table
        // once per project made a dirty boot pay the same remote snapshot load
        // dozens of times. Custom-storage topics retain their isolated group.
        let custom = self.custom_storage_keys().await;
        let mut physical: std::collections::HashMap<(String, String), Vec<(String, String)>> = std::collections::HashMap::new();
        for (project_id, table_name) in wal.list_topic_pairs() {
            let physical_project = if custom.contains(&(project_id.clone(), table_name.clone())) { project_id.clone() } else { String::new() };
            physical.entry((physical_project, table_name.clone())).or_default().push((project_id, table_name));
        }
        let totals: Vec<usize> = stream::iter(physical.into_values())
            .map(|topics| async move { self.derive_wal_cursors_for_physical_table(wal, topics).await.unwrap_or(0) })
            .buffer_unordered(self.config.buffer.delta_scan_concurrency())
            .collect()
            .await;
        Ok(totals.into_iter().sum())
    }

    async fn derive_wal_cursors_for_physical_table(&self, wal: &crate::wal::WalManager, topics: Vec<(String, String)>) -> anyhow::Result<usize> {
        let Some((representative_project, representative_table)) = topics.first() else { return Ok(0) };
        // Scan recent commits; replay-derived commits without a watermark
        // contribute nothing so they can't reset the MAX backward.
        let Ok(table_ref) = self.resolve_table(representative_project, representative_table).await else {
            return Ok(0);
        };
        let table = table_ref.read().await;
        let commits: Vec<_> = match table.history(Some(self.config.buffer.delta_scan_depth())).await {
            Ok(it) => it.collect(),
            Err(e) => {
                debug!("derive_wal_cursor: history unavailable for {}/{}: {}", representative_project, representative_table, e);
                return Ok(0);
            }
        };
        drop(table);

        let mut total_advanced = 0;
        for (project_id, table_name) in topics {
            let delta_max = max_watermark_across_commits(commits.iter().map(|ci| &ci.info), wal.shards_per_topic(), &project_id, &table_name);
            let advanced = wal.merge_persisted_positions(&project_id, &table_name, &delta_max)?;
            if advanced > 0 {
                info!("Delta-derived cursor advance: project={}, table={}, shards_advanced={}", project_id, table_name, advanced);
            }
            total_advanced += advanced;
        }
        Ok(total_advanced)
    }

    /// Optimize the Delta table using Z-ordering on timestamp and id columns
    /// This improves query performance for time-based queries
    pub async fn optimize_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, _target_size: Option<i64>) -> Result<()> {
        let start_time = std::time::Instant::now();
        let window_hours = self.config.maintenance.timefusion_optimize_window_hours.max(1);

        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Candidate date partitions in the window (today .. today-num_days).
        let now = Utc::now();
        let today = now.date_naive();
        let num_days = (window_hours / 24).max(1);
        // Cold consolidation (daily) owns sealed partitions older than
        // `cold_optimize_after_days` and bin-packs them to the 512MB target.
        // Exclude them from the 30-min warm Z-order so it can't fragment those
        // cold files back to the warm target every cycle (oscillation = wasted
        // S3 I/O). With after_days=1 this leaves warm processing only today —
        // the partition still taking writes.
        let after_days = self.config.parquet.cold_optimize_after_days();
        // When the light (hot-tail) tier is on, it owns `today`: its event-time
        // binned selection produces time-DISJOINT sorted runs, while this
        // whole-partition rewrite re-bins them in snapshot (arrival) order —
        // undoing the disjointness — and its full-day SortBy is the rewrite
        // that kept dying of external-sort starvation (prod 2026-07-21).
        let skip_today = self.config.maintenance.timefusion_light_optimize_enabled;
        let window_dates: Vec<chrono::NaiveDate> = (0..=num_days)
            .map(|days_ago| (now - chrono::Duration::days(days_ago as i64)).date_naive())
            .filter(|d| !(Self::date_is_cold(today, *d, after_days) || skip_today && *d == today))
            .collect();

        // Snapshot the current live file set once: drives both the ZOrder
        // idempotence guard (below) and PR #39's warm/evict (`pre_uris`).
        let all_uris: Vec<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();
        let table_url = table_clone.table_url().to_string();
        let current = Self::filesets_for_dates(&all_uris, &window_dates);

        // Pre-state file set, used to derive the files this optimize *adds*
        // (to warm) and *removes* (to evict) — see warm/evict_cache_for_uris.
        // Reuses (moves) the walk above instead of a second copy, and is hoisted
        // out of the OCC retry loop below: the live file set only changes on a
        // *successful* commit, which returns.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let pre_uris: Option<std::collections::HashSet<String>> = track_files.then(|| all_uris.into_iter().collect());

        // Keep the active partition at the light-compaction target. A single
        // day-sized file would make 1h and 3h predicates select the same file
        // even when timestamp ordering makes their row groups disjoint.
        let target_size = if window_dates.contains(&today) {
            self.config.maintenance.timefusion_light_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        };

        // delta-rs ZOrder has NO idempotence guard (unlike Compact it does no
        // size / single-file / already-sorted check): it rewrites every file in
        // the selected partitions on every run, even sealed days that didn't
        // change — and PR #39 then has to re-warm all those cold rewrites. Skip
        // any partition whose live file set is identical to the last successful
        // optimize. `today` is always processed (growing leading edge).
        let kept_dates: Vec<chrono::NaiveDate> = {
            let guard = self.zorder_filesets.read().await;
            let prev = guard.get(&table_url);
            window_dates
                .iter()
                .filter(|d| match current.get(*d) {
                    None => false,
                    Some(cur) if cur.is_empty() => false,
                    Some(cur) => **d == today || prev.and_then(|m| m.get(*d)).map(|p| p != cur).unwrap_or(true),
                })
                .copied()
                .collect()
        };
        let skipped = window_dates.len().saturating_sub(kept_dates.len());

        if kept_dates.is_empty() {
            info!("optimize: table={} all {} window partitions unchanged since last run — skipping (cache churn avoided)", table_name, window_dates.len());
            crate::metrics::record_optimize_partitions(0, skipped as u64);
            return Ok(());
        }

        info!(
            "Starting optimize (sort): table={} rewriting {} of {} window partitions, skipping {} unchanged (last {}h)",
            table_name,
            kept_dates.len(),
            window_dates.len(),
            skipped,
            window_hours
        );

        let partition_filters: Vec<PartitionFilter> =
            kept_dates.iter().filter_map(|d| PartitionFilter::try_from(("date", "=", d.to_string().as_str())).ok()).collect();

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // Sorting keeps rewritten files timestamp-local, so short ranges can
        // prune whole files and row groups. It remains an incident kill switch.
        let (optimize_type, declare_sorted) = full_optimize_type(schema, self.config.maintenance.timefusion_optimize_sort_by);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
        // SortBy bins materialize their decompressed Arrow set in the shared
        // maintenance pool (a 256MB-zstd bin ≈ 2–6GB Arrow), so concurrent bin
        // sorts starve each other's external-sort reservations and the whole
        // optimize fails ("Not enough memory to continue external sort", prod
        // 2026-07-21). Serialize sort bins — same rule as compact_date.
        let optimize_concurrency = if declare_sorted { 1 } else { self.config.derived.optimize_merge_tasks() };

        // Best-effort: retry bounded OCC conflicts against a fresh snapshot,
        // but never pause flushes (see optimize_table_light). This preserves
        // ingestion latency and prevents maintenance from running unbounded.
        //
        // Hold a maintenance-rewrite permit across the .optimize() — this is
        // the HEAVIEST rewrite (full-window ZOrder/Compact materializing a
        // large pool-invisible Arrow set), so leaving it outside the
        // concurrency cap would let it stack with a dedup/recompress and
        // reproduce the cgroup OOM the cap exists to prevent (prod 2026-07-04).
        // Scoped to the optimize call so the post-commit warm/evict bookkeeping
        // below runs without the permit.
        const MAX_RETRIES: usize = 4;
        let optimize_result: Result<_> = {
            let mut attempt = 0;
            loop {
                if attempt > 0 {
                    tokio::time::sleep(occ_backoff(attempt - 1)).await;
                    if let Err(e) = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                        break Err(anyhow::anyhow!("optimize refresh before retry failed: {e}"));
                    }
                }
                let table_clone = { table_ref.read().await.clone() };
                let result = {
                    let _rewrite_permit =
                        self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
                    table_clone
                        .optimize()
                        .with_filters(&partition_filters)
                        .with_type(optimize_type.clone())
                        .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                        .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                        .with_max_concurrent_tasks(optimize_concurrency)
                        .with_writer_properties(writer_properties.clone())
                        .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
                        .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                        // Avoid the BinaryView read for Variant columns (same issue as
                        // optimize_table_light); delta-rs's internal session defaults to
                        // schema_force_view_types=true.
                        .with_session_state(Arc::new(self.maintenance_session_state()))
                        .await
                };
                match result {
                    Ok(result) => break Ok(result),
                    Err(e) if is_occ_conflict_err(&e.to_string()) && attempt + 1 < MAX_RETRIES => {
                        crate::metrics::record_optimize_conflict();
                        attempt += 1;
                        warn!("Optimize OCC conflict for table={} (attempt {}/{}), refreshing + retrying: {}", table_name, attempt, MAX_RETRIES, e);
                    }
                    Err(e) => break Err(e.into()),
                }
            }
        };

        match optimize_result {
            Ok((new_table, metrics)) => {
                // Record the post-commit file set for the partitions we
                // rewrote so the next run skips them if nothing changes. Done
                // before the min_files early-return so state stays consistent
                // even when we don't adopt the new handle (delta-rs has already
                // committed the rewrite by this point regardless).
                {
                    let new_uris: Vec<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                    let new_sets = Self::filesets_for_dates(&new_uris, &kept_dates);
                    let mut guard = self.zorder_filesets.write().await;
                    let entry = guard.entry(table_url.clone()).or_default();
                    for d in &kept_dates {
                        entry.insert(*d, new_sets.get(d).cloned().unwrap_or_default());
                    }
                }
                crate::metrics::record_optimize_partitions(kept_dates.len() as u64, skipped as u64);

                let min_files = self.config.maintenance.timefusion_compact_min_files;
                if metrics.total_considered_files < min_files {
                    debug!("Skipping optimization commit: {} files < min threshold {}", metrics.total_considered_files, min_files);
                    return Ok(());
                }
                let duration = start_time.elapsed();
                info!(
                    "Optimization completed in {:?}: {} files removed, {} files added, {} partitions optimized, {} total files considered, {} files skipped",
                    duration,
                    metrics.num_files_removed,
                    metrics.num_files_added,
                    metrics.partitions_optimized,
                    metrics.total_considered_files,
                    metrics.total_files_skipped
                );
                if metrics.num_files_removed > 0 {
                    let compression_ratio = metrics.num_files_removed as f64 / metrics.num_files_added as f64;
                    info!("Optimization compression ratio: {:.2}x", compression_ratio);
                }
                // Swap the optimized table in and refresh the cache (warm
                // newly-added files, evict tombstoned ones). Returns the new
                // live file URIs for the tantivy GC hook below.
                let live_uris = self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &[]).await;
                // Tantivy compaction reindex + GC. Order matters: build
                // indexes for the compaction's OUTPUT files first, then GC the
                // inputs' entries — so window coverage never regresses (the
                // pre-existing gap where GC deleted indexes nothing rebuilt
                // left old windows permanently un-prefiltered). Best-effort:
                // errors are logged; the coverage gate keeps queries correct.
                if let Some(svc) = self.tantivy_indexer().cloned()
                    && svc.config.is_table_indexed(table_name)
                {
                    use crate::tantivy_index::service::{parquet_rel_of_uri, project_id_of_uri};
                    let delta_store = { table_ref.read().await.log_store().object_store(None) };
                    let added: Vec<(String, String, String)> = live_uris
                        .iter()
                        // `None` (file tracking off) behaves as the empty pre-set,
                        // exactly as before: every live parquet is treated as new.
                        .filter(|u| !pre_uris.as_ref().is_some_and(|p| p.contains(*u)) && u.ends_with(".parquet"))
                        .filter_map(|u| Some((project_id_of_uri(u)?.to_string(), parquet_rel_of_uri(u)?.to_string(), u.clone())))
                        .collect();
                    let mut built = 0usize;
                    let mut reindex_errs = 0usize;
                    let table_owned = table_name.to_string();
                    let mut jobs = futures::stream::iter(added.into_iter().map(|(pid, rel, uri)| {
                        let (svc, store, table) = (svc.clone(), delta_store.clone(), table_owned.clone());
                        async move { svc.build_index_for_file(&table, &pid, &rel, &uri, store).await }
                    }))
                    .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1));
                    while let Some(r) = jobs.next().await {
                        match r {
                            Ok(()) => built += 1,
                            Err(e) => {
                                reindex_errs += 1;
                                warn!("tantivy post-optimize reindex failed for table={}: {}", table_name, e);
                            }
                        }
                    }
                    drop(jobs);
                    if built > 0 || reindex_errs > 0 {
                        info!("tantivy post-optimize reindex: table={} built={} errors={}", table_name, built, reindex_errs);
                    }
                }
                // Drop sidecar index entries for files rewritten away.
                if let Some(svc) = self.tantivy_indexer().cloned() {
                    let svc_table = table_name.to_string();
                    // Manifests are keyed by the project uuid taken from the
                    // parquet URI at build time — enumerate them rather than
                    // guessing (a fixed "default"+customs list never visited
                    // unified tenants' manifests, so their stale entries
                    // outlived every compaction until the nightly reconcile).
                    let project_ids = match crate::tantivy_index::manifest::list_projects(svc.object_store.as_ref(), table_name).await {
                        Ok(pids) => pids,
                        Err(e) => {
                            warn!("tantivy gc: manifest enumeration failed for {}: {}", table_name, e);
                            Vec::new()
                        }
                    };
                    for pid in project_ids {
                        match svc.gc_after_compaction(&svc_table, &pid, &live_uris).await {
                            Ok(report) if report.entries_removed > 0 => {
                                info!(
                                    "tantivy gc: project={} table={} removed={} kept={} blobs_deleted={}",
                                    pid, svc_table, report.entries_removed, report.kept, report.blobs_deleted
                                );
                            }
                            Ok(_) => {}
                            Err(e) => warn!("tantivy gc failed for project={} table={}: {}", pid, svc_table, e),
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                if is_occ_conflict_err(&e.to_string()) {
                    crate::metrics::record_optimize_conflict();
                }
                crate::metrics::record_optimize_failed();
                error!("Optimization operation failed: {}", e);
                Err(anyhow::anyhow!("Table optimization failed: {}", e))
            }
        }
    }

    /// Group live file URIs by their `date=YYYY-MM-DD` Hive partition, for the
    /// given dates only. URIs not matching any of `dates` are ignored. Every
    /// requested date gets an entry (possibly empty) so the idempotence guard
    /// can tell "no files" from "not looked at".
    fn filesets_for_dates(uris: &[String], dates: &[chrono::NaiveDate]) -> HashMap<chrono::NaiveDate, std::collections::HashSet<String>> {
        let markers: Vec<(chrono::NaiveDate, String)> = dates.iter().map(|d| (*d, format!("date={d}"))).collect();
        let mut out: HashMap<chrono::NaiveDate, std::collections::HashSet<String>> = dates.iter().map(|d| (*d, std::collections::HashSet::new())).collect();
        for uri in uris {
            if let Some((d, _)) = markers.iter().find(|(_, marker)| uri.contains(marker)) {
                out.entry(*d).or_default().insert(uri.clone());
            }
        }
        out
    }

    /// Project IDs with live files in one hot `(project_id, date)` partition.
    /// A light optimize must use both partition predicates: filtering by `date`
    /// alone conflicts with every project's append to the active day.
    fn hot_project_ids(uris: &[String], date: chrono::NaiveDate) -> Vec<String> {
        let date_marker = format!("/date={date}/");
        let counts = uris
            .iter()
            .filter(|uri| uri.contains(&date_marker))
            .filter_map(|uri| uri.split('/').find_map(|segment| segment.strip_prefix("project_id=")))
            .filter(|project_id| !project_id.is_empty())
            .fold(std::collections::HashMap::<&str, usize>::new(), |mut counts, project_id| {
                *counts.entry(project_id).or_default() += 1;
                counts
            });
        // Most-fragmented partition first: it's the one whose recent-window
        // queries open the most files, so it benefits most from an early tick.
        let mut projects: Vec<_> = counts.into_iter().collect();
        projects.sort_unstable_by(|(a, a_count), (b, b_count)| b_count.cmp(a_count).then_with(|| a.cmp(b)));
        projects.into_iter().map(|(project_id, _)| project_id.to_owned()).collect()
    }

    /// Select the specific files a light optimize should bin-pack, instead of
    /// letting `OptimizeBuilder` rewrite the whole `date=today` partition. That
    /// partition-wide rewrite records a read predicate spanning the live tail,
    /// so every concurrent ingestion flush trips the OCC conflict checker and
    /// the commit loses (prod 2026-07-20: never converged, 541 tiny files on a
    /// 3h window). Here we pick only already-flushed small files up to
    /// `target_size`, plus at most one existing sorted run to merge into, and
    /// hand that exact set to `with_binned_files` — the appends that land after
    /// selection aren't in the set, so they don't conflict.
    /// `sorted_run_cap` bounds which already-tagged sorted runs are re-admitted
    /// to the packing: the cold tier passes `i64::MAX` (its leveled re-merge
    /// folds any sub-target run), the hot tier passes `target/4` so each tick's
    /// small output run keeps folding into the next pack until it reaches ~1/4
    /// target — otherwise a busy project accrues one 5-min run per tick (~100+
    /// files/day again). Growing a run 4× before exclusion bounds rewrite
    /// amplification at ~3× per byte. Files at ≥ 7/8·target are always
    /// excluded — they're converged, and re-selecting one alone would rewrite
    /// it 1→1 forever.
    async fn light_optimize_tail(
        table: &DeltaTable, filters: &[PartitionFilter], target_size: i64, min_files: usize, sorted_run_cap: i64,
    ) -> Result<Vec<String>> {
        let adds: Vec<_> = table.get_active_add_actions_by_partitions(filters).try_collect::<Vec<_>>().await?;
        let tail: Vec<TailAdd> = adds
            .iter()
            .filter(|add| add.size() < target_size.max(1)) // cheap gate before the stats parse
            .map(|add| TailAdd::from_stats(add.path().to_string(), add.size(), is_sorted_run(&add.tags()), add.stats().as_deref()))
            .collect();
        Ok(select_tail_bin(&tail, target_size, min_files, sorted_run_cap, seal_micros_now()))
    }

    /// ONE tag-first walk of the snapshot that plans a bin for EVERY hot project
    /// of `date=today`, replacing the per-project `light_optimize_tail` walk.
    /// The old shape re-walked and re-parsed Add stats once per project per pass
    /// — up to 132 walks/tick over 24,343 files, and `ScanLogReplayProcessor`
    /// plus Add-stats JSON parsing were 34.5% + 18.4% of process CPU
    /// (prod profile 2026-07-29). Here the converged (≥ 7/8 target) and
    /// over-cap sorted-run files are skipped by size/tag BEFORE their stats JSON
    /// is touched, so the parse cost is O(live tail), not O(active files).
    ///
    /// Returns one bin per project, ordered by compaction debt (raw small-file
    /// count — the most fragmented partition opens the most files per query, so
    /// it goes first within the round).
    /// TODO: weight the debt score by read traffic — a partition nobody queries
    /// is deferred work, not urgent work (needs per-project query counters).
    fn select_all_hot_bins(
        table: &DeltaTable, schema: &crate::schema_loader::TableSchema, today_str: &str, target_size: i64, min_files: usize, sorted_run_cap: i64,
    ) -> Result<Vec<(String, Vec<String>)>> {
        let date_marker = format!("date={today_str}/");
        let seal = seal_micros_now();
        // Only a table that declares a sort order has a footer to repair.
        let repairable = !schema.sorting_columns.is_empty();
        let cap = target_size.max(1);
        let converged = cap - cap / 8;
        let per_project = table
            .snapshot()?
            .log_data()
            .iter()
            .filter(|file| file.path().contains(&date_marker))
            // Tag-first: both exclusions are pure metadata, so a converged file
            // or an over-cap sorted run never reaches the stats parse.
            .filter_map(|file| {
                let (size, sorted_run) = (file.size(), is_sorted_run(&file.tags()));
                // A converged file is normally done — EXCEPT when it is converged
                // and UNSORTED. Such a file declares no `sorting_columns`, and one
                // of them disables the reader's all-or-nothing footer ordering for
                // every scan that touches it (costing the streaming top-N pushdown
                // and bounded dedup). Nothing else repairs it: the daily
                // consolidate/recompress crons rarely survive this process's restart
                // cadence, so hot-tail is the only pass that runs often enough to be
                // relied on. `select_tail_bin` admits at most one per bin, so the
                // backlog drains over ticks instead of rewriting several
                // hundred-megabyte files at once.
                let converged_done = size >= converged && (sorted_run || !repairable);
                let over_cap_run = size >= sorted_run_cap && sorted_run;
                if converged_done || over_cap_run {
                    return None;
                }
                // `stats()` is reached only past both tag/size exclusions.
                let path = file.path();
                let project_id = path.split('/').find_map(|s| s.strip_prefix("project_id=")).filter(|p| !p.is_empty()).map(str::to_owned)?;
                Some((project_id, TailAdd::from_stats(path.into_owned(), size, sorted_run, file.stats().as_deref())))
            })
            .fold(HashMap::<String, Vec<TailAdd>>::new(), |mut per_project, (project_id, add)| {
                per_project.entry(project_id).or_default().push(add);
                per_project
            });
        let mut planned: Vec<(String, Vec<String>, usize)> = per_project
            .into_iter()
            .map(|(project_id, adds)| {
                let debt = adds.len();
                (project_id, select_tail_bin(&adds, target_size, min_files, sorted_run_cap, seal), debt)
            })
            .filter(|(_, bin, _)| !bin.is_empty())
            .collect();
        planned.sort_unstable_by(|a, b| b.2.cmp(&a.2).then_with(|| a.0.cmp(&b.0)));
        Ok(planned.into_iter().map(|(project_id, bin, _)| (project_id, bin)).collect())
    }

    /// `[min, max]` event time (micros) of a file from its raw Add stats JSON.
    /// Timestamp stats serialize as RFC3339 strings (epoch numbers accepted for
    /// long-typed columns).
    fn event_time_range_from_stats(stats: &str) -> Option<(i64, i64)> {
        let stats: serde_json::Value = serde_json::from_str(stats).ok()?;
        let get = |key: &str| {
            let v = &stats[key]["timestamp"];
            v.as_str().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).map(|d| d.timestamp_micros()).or_else(|| v.as_i64())
        };
        Some((get("minValues")?, get("maxValues")?))
    }

    /// Partition-ownership boundary between the warm (30-min Z-order) and cold
    /// (daily 512MB consolidate) tiers: a `date` is cold-owned once it's at least
    /// `after_days` older than `today`. The warm optimize processes the
    /// complement, so the two tiers never rewrite the same partition (no
    /// 256MB↔512MB oscillation). Single source of truth for both schedulers.
    fn date_is_cold(today: chrono::NaiveDate, date: chrono::NaiveDate, after_days: u64) -> bool {
        (today - date).num_days() >= after_days as i64
    }

    /// Compacted-file target by partition age (calendar-based): sealed days
    /// consolidate to the larger cold target (fewer files → smaller checkpoint
    /// → faster commits); the current day stays at the warm target so a
    /// still-filling partition isn't rewritten to the cold target repeatedly.
    fn optimize_target_for_date(&self, date: chrono::NaiveDate) -> i64 {
        if Self::date_is_cold(Utc::now().date_naive(), date, self.config.parquet.cold_optimize_after_days()) {
            self.config.parquet.timefusion_cold_optimize_target_size
        } else {
            self.config.parquet.timefusion_optimize_target_size
        }
    }

    /// Compact a single `date=` partition by bin-packing its small files
    /// (`Compact`, not Z-order — a pure row-group merge that preserves
    /// Variant/Binary column bytes). Powers the on-demand `OPTIMIZE <table>
    /// WHERE date = '...'` pgwire command and the `optimize` CLI subcommand
    /// (the daily cold sweep uses `consolidate_date_binned` for event-time
    /// disjoint runs). Target size scales with partition age
    /// (`optimize_target_for_date`). Commits once; returns (removed, added).
    pub async fn compact_date(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>,
    ) -> Result<(u64, u64)> {
        self.compact_date_with(table_ref, table_name, date, project_id, self.config.derived.optimize_merge_tasks()).await
    }

    /// `compact_date` with an explicit bin concurrency (off-box CLI
    /// `--concurrency N`); `None` keeps the in-server default.
    pub async fn compact_date_concurrent(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>, concurrency: Option<usize>,
    ) -> Result<(u64, u64)> {
        let n = concurrency.unwrap_or_else(|| self.config.derived.optimize_merge_tasks()).max(1);
        self.compact_date_with(table_ref, table_name, date, project_id, n).await
    }

    /// `compact_date` with an explicit merge concurrency. The cold consolidation
    /// sweep passes 1: a 512MB-target merge holds ~target-sized output buffers per
    /// task, so concurrency × 512MB can OOM the memory-tight in-process instance
    /// (the off-box recipe uses concurrency 1 for the same reason). The on-demand
    /// pgwire/CLI callers keep the configured concurrency.
    async fn compact_date_with(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, project_id: Option<&str>, max_concurrent: usize,
    ) -> Result<(u64, u64)> {
        let target_size = self.optimize_target_for_date(date);
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let mut partition_filters = vec![PartitionFilter::try_from(("date", "=", date.to_string().as_str()))?];
        // Scope to one tenant when asked: a whole date spans every project's
        // files (tens of GB on a busy day — doesn't fit in-process), one
        // (project, date) partition is a few GB.
        if let Some(pid) = project_id {
            partition_filters.push(PartitionFilter::try_from(("project_id", "=", pid))?);
        }
        // Old-event-time backlog data still lands in recent-old partitions, so a
        // concurrent flush/dedup can delete files mid-merge → Serializable OCC
        // conflict at commit (the merge read now-removed files). Refresh the
        // snapshot and retry rather than fail; an intermittently-written
        // partition lands on a later attempt. Mirrors the dedup retry loop.
        //
        // PROGRESS-AWARE BUDGET: `MAX_ATTEMPTS` counts consecutive attempts
        // that shrank nothing. An attempt that reduced the partition's file
        // count banked incremental commits (bins land every
        // min_commit_interval), so it resets the counter — under a busy
        // commit stream (2026-07-30: prod flush/dedup churning the same
        // partition) a fixed budget quit mid-convergence. TOTAL_ATTEMPTS is
        // the runaway backstop.
        const MAX_ATTEMPTS: usize = 4;
        const TOTAL_ATTEMPTS: usize = 32;
        // Pre-state file set for the warm/evict diff, hoisted out of the retry
        // loop (only a successful commit — which returns — changes it) and
        // scoped to the partition being compacted: `optimize().with_filters()`
        // can only add/remove files under these markers, so diffing the whole
        // table's URI set was pure waste. `None` when neither warm- nor
        // evict-after-compaction is on, so the walk is skipped outright.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let scope: Vec<String> = std::iter::once(format!("date={date}/")).chain(project_id.map(|pid| format!("project_id={pid}/"))).collect();
        let scope: Vec<&str> = scope.iter().map(String::as_str).collect();
        let pre_uris: Option<std::collections::HashSet<String>> =
            if track_files { Some(scoped_file_uris(&*table_ref.read().await, &scope).into_iter().collect()) } else { None };
        let mut scope_files = scoped_file_uris(&*table_ref.read().await, &scope).len();
        let (mut attempt, mut total_attempts) = (0usize, 0usize);
        loop {
            // The snapshot is refreshed in the Err arm (needed there anyway for
            // the progress check), so every retry re-plans against fresh state.
            let table_clone = { table_ref.read().await.clone() };
            // SortBy: sort the partition by the schema keys and declare it, so
            // cold/consolidated partitions keep an honest DESC footer for the
            // ordering pushdown (plain Compact concatenates → declare false).
            // SortBy reads via the ordering-advertising DeltaScanNext: over
            // already-sorted files `df.sort()` collapses to a streaming
            // SortPreservingMergeExec (bounded k-way merge). The one exception
            // is a partition still holding legacy pre-sort files — its first
            // rewrite is a one-time blocking sort. Force concurrency 1 on the
            // SortBy path so those transition sorts can't stack and exhaust the
            // maintenance pool (the 2026-07-14 OOM multiplier); steady-state
            // SortBy is cheap SPM, so serializing partitions costs little.
            let (optimize_type, declare_sorted) = choose_optimize_type(schema, false, self.config.maintenance.timefusion_optimize_sort_by);
            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
            // SortBy forces serial bins ONLY at in-server concurrency (≤ the
            // pinned merge-task count): transition sorts stacking on the shared
            // maintenance pool was the 2026-07-14 OOM multiplier. An explicitly
            // higher `max_concurrent` (off-box CLI --concurrency, dedicated
            // container pool) opts out — serial bins on a 25-40 GB pool waste
            // nearly all of it (2026-07-31: 100-bin whale-days at 5-8 h serial).
            let sort_concurrency = if declare_sorted && max_concurrent <= self.config.derived.optimize_merge_tasks() { 1 } else { max_concurrent };
            let result = table_clone
                .optimize()
                .with_filters(&partition_filters)
                .with_type(optimize_type)
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                .with_max_concurrent_tasks(sort_concurrency)
                .with_writer_properties(writer_properties)
                // 2min (was 10): bins run serially on the SortBy path, so a
                // short interval banks incremental commits — an OCC loss to a
                // concurrent dedup/flush costs one bin's work, not the whole
                // partition (2026-07-14 all-or-nothing starvation).
                .with_min_commit_interval(tokio::time::Duration::from_secs(2 * 60))
                .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                // Variant columns: same BinaryView-avoidance session as optimize_table.
                .with_session_state(Arc::new(self.maintenance_session_state()))
                .await;
            match result {
                Ok((new_table, metrics)) => {
                    self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &scope).await;
                    info!("compact date={date} table={table_name}: {} files removed, {} files added", metrics.num_files_removed, metrics.num_files_added);
                    return Ok((metrics.num_files_removed, metrics.num_files_added));
                }
                Err(e) => {
                    let msg = e.to_string();
                    let (occ, s3) = (is_occ_conflict_err(&msg), is_transient_s3_err(&msg));
                    total_attempts += 1;
                    // Progress check: a failed attempt whose banked bin commits
                    // shrank the partition resets the no-progress budget (needs
                    // a fresh snapshot; the retry-refresh above is skipped when
                    // we bail, so refresh here before counting).
                    if (occ || s3) && total_attempts < TOTAL_ATTEMPTS {
                        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
                        let now_files = scoped_file_uris(&*table_ref.read().await, &scope).len();
                        if now_files < scope_files {
                            scope_files = now_files;
                            attempt = 0;
                        } else {
                            attempt += 1;
                        }
                        if attempt < MAX_ATTEMPTS {
                            if occ {
                                crate::metrics::record_optimize_conflict();
                                warn!(
                                    "compact date={date}: OCC conflict (no-progress attempt {attempt}/{MAX_ATTEMPTS}, total {total_attempts}), refreshing + retrying: {e}"
                                );
                                // Exponential backoff — matches dedup_partition. Zero-delay
                                // retries under concurrent heavy ingest amplify contention.
                                tokio::time::sleep(occ_backoff(attempt.max(1) - 1)).await;
                            } else {
                                // A multipart part connection-dropped mid-merge (nothing committed).
                                warn!(
                                    "compact date={date}: transient S3 error (no-progress attempt {attempt}/{MAX_ATTEMPTS}, total {total_attempts}), backing off + retrying: {e}"
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(2 * attempt.max(1) as u64)).await;
                            }
                            continue;
                        }
                    }
                    if occ {
                        crate::metrics::record_optimize_conflict();
                    }
                    crate::metrics::record_optimize_failed();
                    return Err(anyhow::anyhow!("compact date={date} table={table_name} failed: {e}"));
                }
            }
        }
    }

    /// Distinct `date=YYYY-MM-DD` partitions present in the live file set,
    /// ascending. Drives the CLI/pgwire "compact old partitions" loop.
    pub async fn partition_dates(&self, table_ref: &Arc<RwLock<DeltaTable>>) -> Result<Vec<chrono::NaiveDate>> {
        let uris: Vec<String> = { table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default() };
        let mut dates = std::collections::BTreeSet::new();
        for uri in &uris {
            if let Some(i) = uri.find("date=") {
                let tail = &uri[i + 5..];
                if let Ok(d) = tail.get(..10).unwrap_or(tail).parse::<chrono::NaiveDate>() {
                    dates.insert(d);
                }
            }
        }
        Ok(dates.into_iter().collect())
    }

    /// Projects present in `date`'s live file set, most-fragmented first.
    /// Drives the CLI's per-project consolidate/dedup loops.
    pub async fn partition_projects(&self, table_ref: &Arc<RwLock<DeltaTable>>, date: chrono::NaiveDate) -> Result<Vec<String>> {
        let uris: Vec<String> = { table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default() };
        Ok(Self::hot_project_ids(&uris, date))
    }

    /// Rewrites a date partition at a higher ZSTD level using Z-order (or
    /// Compact if no z_order_columns). Skips partitions whose probe file
    /// already advertises a tier `>= target_level` via Parquet footer KV
    /// metadata (`timefusion.compression_tier`).
    ///
    /// Probes only one file per partition. Safe in steady state: each
    /// successful recompress rewrites every file in the partition at the
    /// same level, so all files share a tier. A partial-rewrite failure
    /// would leave mixed tiers — the next sweep then sees the probe's tier
    /// and may skip, but the partition will be re-evaluated the day after.
    /// Acceptable for an idempotent daily job.
    pub async fn recompress_partition(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_level: i32) -> Result<()> {
        use deltalake::datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
        use object_store::{ObjectStoreExt, path::Path as OsPath};

        let date_str = date.to_string();
        let date_marker = format!("date={}", date_str);

        let (uris, log_store, table_uri) = {
            let table = table_ref.read().await;
            let uris: Vec<String> = table.get_file_uris()?.filter(|u| u.contains(&date_marker)).collect();
            (uris, table.log_store(), table.table_url().to_string())
        };
        if uris.is_empty() {
            debug!("recompress: no files in partition date={} for table={}", date_str, table_name);
            return Ok(());
        }

        // Recompress rewrites whole partitions — same pool-invisible Arrow
        // materialization as dedup/optimize; hold a maintenance-rewrite permit.
        // Acquired after the empty-partition early-out so no-op calls are free.
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;

        // Probe one file's footer KV metadata. URIs returned by delta-rs are
        // absolute (s3://bucket/...); the table's object_store is rooted at
        // table_uri, so the relative key is the URI with that prefix stripped.
        // `table_url()` may include a `?endpoint=...` query string (non-AWS
        // backends like MinIO) which `get_file_uris()` does not — strip it
        // before matching.
        let probe_uri = &uris[0];
        let table_prefix = table_uri.split('?').next().unwrap_or(&table_uri).trim_end_matches('/');
        let probe_tier = match probe_uri.strip_prefix(table_prefix).and_then(|s| s.strip_prefix('/').or(Some(s))) {
            Some(rel) => {
                let object_store = log_store.object_store(None);
                let path = OsPath::from(rel);
                // `head()` returns `meta.location` relative to the bucket,
                // but `ParquetObjectReader` consumes object-store-relative
                // paths and would double-prefix. Pass our original `path`.
                match object_store.head(&path).await {
                    Ok(meta) => {
                        let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone()).with_file_size(meta.size);
                        reader.get_metadata(None).await.ok().and_then(|pq| {
                            pq.file_metadata().key_value_metadata().and_then(|kvs| {
                                kvs.iter().find(|kv| kv.key == COMPRESSION_TIER_KEY).and_then(|kv| kv.value.as_ref()).and_then(|v| v.parse::<i32>().ok())
                            })
                        })
                    }
                    Err(e) => {
                        warn!("recompress probe: head failed for {}: {}; rewriting anyway", probe_uri, e);
                        None
                    }
                }
            }
            None => {
                warn!("recompress probe: could not relativize {} against {}; rewriting anyway", probe_uri, table_prefix);
                None
            }
        };

        // If probe failed or tier is unknown, fall through to rewrite — safer
        // than skipping a partition that may still be at hot tier.
        if let Some(t) = probe_tier
            && t >= target_level
        {
            debug!("recompress: skip date={} table={} (already at tier {})", date_str, table_name, t);
            return Ok(());
        }

        info!("recompress: rewriting date={} table={} at zstd={} ({} files)", date_str, table_name, target_level, uris.len());

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // Sort the rewrite by the schema keys via an `ORDER BY` on the input
        // plan and declare the footer, so a recompressed partition keeps an
        // honest DESC footer — a bare `SELECT *` concatenation would strip the
        // ordering that optimize/compact established. `declare_sorted` tracks
        // whether we actually sort (empty clause when no sort order declared).
        // Gated by timefusion_optimize_sort_by: a global ORDER BY sort of a cold
        // partition can exhaust the bounded maintenance pool (same limit that
        // disables SortBy for optimize). Off → bare SELECT * (no sort, declare false).
        let order_by = if self.config.maintenance.timefusion_optimize_sort_by { schema_order_by_clause(schema) } else { String::new() };
        let declare_sorted = !order_by.is_empty();
        let writer_properties = self.create_writer_properties(schema, target_level, declare_sorted);
        let target_size = self.config.parquet.timefusion_optimize_target_size;

        // Force a full-partition rewrite at the new zstd tier via a streaming
        // `replace_where` overwrite — NOT Z-order. delta-rs `Compact` skips
        // files already ≥ target and drops single-file bins, so it can't lift
        // an already-consolidated partition's tier; Z-order *can* force the
        // rewrite but its space-filling curve scatters `timestamp` across row
        // groups, wrecking the dominant time-range predicate's pruning. Instead
        // we read the partition (`date = X`, all project_ids) and write it back
        // with `SaveMode::Overwrite` + `replace_where`, which atomically
        // Remove-tombstones the old files and Adds the recompressed ones
        // (data_change semantics preserved). `with_input_plan` streams the scan
        // through the writer (bounded by target_file_size) rather than
        // materializing the whole partition, so peak memory matches a normal
        // flush — unlike Z-order's global sort. The scan runs on the
        // variant-safe maintenance session (no `variant_to_json` wrap), so
        // Variant columns round-trip as raw Struct. Decoupling from
        // `z_order_columns` lets the schema keep that list empty for queries.
        let (snapshot, log_store, table_clone) = {
            let table = table_ref.read().await;
            (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store(), table.clone())
        };
        let pre_uris: std::collections::HashSet<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();

        let provider = deltalake::delta_datafusion::TableProviderBuilder::default()
            .with_log_store(log_store)
            .with_eager_snapshot(snapshot)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("recompress scan provider: {e}"))?;
        // Must be the delta *write* session (carries DeltaPlanner): the write
        // wraps its input in a MetricObserver node only that planner can
        // physically plan. It now also reserves sort-spill memory so the added
        // ORDER BY spills rather than erroring on a large partition.
        let session = build_delta_write_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env());
        let ctx = datafusion::prelude::SessionContext::new_with_state(session);
        ctx.register_table("recompress_src", Arc::new(provider))?;
        // Literal date is safe: `date_str` is a parsed `chrono::NaiveDate`. The
        // `order_by` clause (quoted identifiers) makes the rewrite globally
        // sorted so `declare_sorted` above is honest.
        let input_plan = ctx.sql(&format!("SELECT * FROM recompress_src WHERE date = '{date_str}'{order_by}")).await?.into_optimized_plan()?;

        let replace_pred = format!("date = '{date_str}'");
        let write_result = table_clone
            .write(Vec::<RecordBatch>::new())
            .with_input_plan(input_plan)
            .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
            .with_replace_where(replace_pred.as_str())
            .with_writer_properties(writer_properties)
            .with_target_file_size(std::num::NonZero::new(target_size as u64))
            .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
            .with_session_state(Arc::new(ctx.state()))
            .await;

        match write_result {
            Ok(new_table) => {
                info!("recompress: date={} table={} rewritten at zstd={} (was {} files)", date_str, table_name, target_level, pre_uris.len());
                // Swap + warm-added/evict-removed like the other optimize
                // paths. A bare swap left the rewritten cold-tier files
                // un-warmed and the tombstoned ones cached — the next query
                // on a recompressed partition paid full S3 reads (1.5 s
                // observed against OVH).
                self.swap_and_refresh_cache(table_ref, new_table, Some(&pre_uris), &[]).await;
                Ok(())
            }
            Err(e) => {
                error!("recompress failed for date={} table={}: {}", date_str, table_name, e);
                Err(anyhow::anyhow!("recompress failed: {}", e))
            }
        }
    }

    /// Sweep partitions in [age_min_days, age_max_days) and recompress any
    /// whose probe tier is below `target_level`. Iterates day-by-day; each
    /// day's optimize is its own Delta commit so a mid-sweep failure leaves
    /// completed days at the new tier.
    pub async fn recompress_tier_window(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, age_min_days: u64, age_max_days: u64, target_level: i32,
    ) -> Result<()> {
        let today = Utc::now().date_naive();
        for days_ago in age_min_days..age_max_days {
            let date = today - chrono::Duration::days(days_ago as i64);
            if let Err(e) = self.recompress_partition(table_ref, table_name, date, target_level).await {
                warn!("recompress_tier_window: skipping date={} after error: {}", date, e);
            }
        }
        Ok(())
    }

    /// Daily cold consolidation: bin-pack every sealed partition (date older
    /// than `cold_optimize_after_days`) toward the 512MB cold target. Calendar-age
    /// driven and idempotent — converged runs are excluded from re-selection,
    /// so already-consolidated partitions cost a snapshot scan, not a rewrite
    /// (bounds S3 I/O across the whole cold backlog). Covers "previous days and
    /// further", picking up backfill that landed in old partitions.
    pub async fn consolidate_sealed_partitions(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) -> Result<()> {
        let today = crate::clock::today_utc();
        let after_days = self.config.parquet.cold_optimize_after_days();
        let dates: Vec<chrono::NaiveDate> = self.partition_dates(table_ref).await?.into_iter().filter(|d| Self::date_is_cold(today, *d, after_days)).collect();
        info!("consolidate: table={} sweeping {} sealed partition(s) older than {}d", table_name, dates.len(), after_days);
        for date in dates {
            let target = self.optimize_target_for_date(date);
            if let Err(e) = self.consolidate_date_binned(table_ref, table_name, date, target, None, usize::MAX).await {
                warn!("consolidate: skipping date={} after error: {}", date, e);
            }
        }
        Ok(())
    }

    /// Incremental catch-up for the cold sweep above, for partitions it has not
    /// reached.
    ///
    /// `consolidate_sealed_partitions` runs once a day and sweeps EVERY cold
    /// date in one long job, so it only helps if the process survives the whole
    /// sweep. Prod does not: it restarts every 30-120 minutes, and on
    /// 2026-08-01 the previous day's partitions held 3128-3515 files each while
    /// every partition older than that sat at 1-99 — the daily job had landed
    /// for those and simply never finished for the newest sealed day. Files
    /// then stay fragmented forever, and file count is what arms the wide-scan
    /// gate and drives decode heap.
    ///
    /// So do the same work from the frequent tick in a BOUNDED slice: pick the
    /// single most fragmented cold partition and give it a few passes. Each
    /// pass is its own commit, so whatever finishes before a restart is kept
    /// and the next tick resumes from the new snapshot. No date can starve —
    /// consolidating the worst one lowers its count until another becomes the
    /// worst.
    pub async fn consolidate_catchup(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, max_passes: usize) -> Result<()> {
        let today = crate::clock::today_utc();
        let after_days = self.config.parquet.cold_optimize_after_days();
        let target_of = |d| self.optimize_target_for_date(d);
        // Count only files still BELOW their date's target: a partition of big
        // converged runs is done, however many of them there are, and must not
        // out-rank a genuinely fragmented one.
        let worst = {
            let table = table_ref.read().await;
            table
                .snapshot()?
                .log_data()
                .iter()
                .filter_map(|f| {
                    let path = f.path();
                    let date = path.split('/').find_map(|s| s.strip_prefix("date="))?.parse::<chrono::NaiveDate>().ok()?;
                    let project_id = path.split('/').find_map(|s| s.strip_prefix("project_id="))?.to_owned();
                    (Self::date_is_cold(today, date, after_days) && f.size() < target_of(date)).then_some((date, project_id))
                })
                .fold(HashMap::<(chrono::NaiveDate, String), usize>::new(), |mut acc, key| {
                    *acc.entry(key).or_default() += 1;
                    acc
                })
                .into_iter()
                // Ties break to the NEWEST date: it is the one queries read.
                .filter(|(_, n)| *n >= 2)
                .max_by(|((a_date, a_project), a_n), ((b_date, b_project), b_n)| {
                    a_n.cmp(b_n).then_with(|| a_date.cmp(b_date)).then_with(|| b_project.cmp(a_project))
                })
        };
        let Some(((date, project_id), small_files)) = worst else {
            return Ok(());
        };
        info!(
            "consolidate-catchup: table={} project={} date={} {} small file(s), running up to {} pass(es)",
            table_name, project_id, date, small_files, max_passes
        );
        self.consolidate_date_binned(table_ref, table_name, date, target_of(date), Some(&project_id), max_passes).await
    }

    /// Leveled (L2) consolidation of one sealed `date`: per project, repeatedly
    /// select the earliest event-time slice of small files up to the cold
    /// target and rewrite it as one sorted run (`with_binned_files`, one commit
    /// per run). Successive passes take strictly later slices, so — unlike the
    /// whole-partition optimize, whose internal bins pack in snapshot order and
    /// mix event times — the output runs are event-time DISJOINT: a recent-window
    /// or `ORDER BY timestamp DESC LIMIT` query reads only the run(s) overlapping
    /// its range instead of every file in the day. Per-pass memory is bounded by
    /// one ≤target sort (the whole-day SortBy died of external-sort starvation,
    /// prod 2026-07-21). Converges: outputs ≥ 7/8·target (and lone tail runs)
    /// are excluded from re-selection by `light_optimize_tail`.
    /// `target_size`/`only_project` are caller-supplied so the off-box CLI can
    /// consolidate a still-hot date to the cold (512MB) target for one tenant
    /// without waiting for the partition to seal.
    pub async fn consolidate_date_binned(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date: chrono::NaiveDate, target_size: i64, only_project: Option<&str>, max_passes: usize,
    ) -> Result<()> {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // This path already bounds each rewrite to one event-time bin at the
        // cold target, so it does not share the whole-partition external-sort
        // hazard guarded by `timefusion_optimize_sort_by`. Its contract is to
        // produce disjoint sorted runs: leaving this behind that global kill
        // switch made the default cold compactor strip ordering from historical
        // files and forced read-side greatest-version dedup to buffer the full
        // scan. Always sort/dedup the bounded bin; whole-partition optimize and
        // recompress remain gated by the kill switch.
        let (optimize_type, declare_sorted) = consolidate_optimize_type(schema, true);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, declare_sorted);
        let date_str = date.to_string();
        let uris: Vec<String> = { table_ref.read().await.get_file_uris().map(|it| it.collect()).unwrap_or_default() };
        // Backstop against a selection that stops shrinking (e.g. a rewrite
        // that keeps losing OCC to a dedup); a normal day converges in
        // partition_bytes/target passes.
        // Backstop for the full sweep; the catch-up caller passes a small budget
        // so one tick's work fits between restarts.
        let max_passes = max_passes.clamp(1, 128);
        for project_id in Self::hot_project_ids(&uris, date).into_iter().filter(|p| only_project.is_none_or(|only| only == p)) {
            let partition_filters =
                vec![PartitionFilter::try_from(("project_id", "=", project_id.as_str()))?, PartitionFilter::try_from(("date", "=", date_str.as_str()))?];
            for _ in 0..max_passes {
                let selected_files = {
                    let table = table_ref.read().await;
                    Self::light_optimize_tail(&table, &partition_filters, target_size, 2, i64::MAX).await?
                };
                if selected_files.is_empty() {
                    break;
                }
                self.optimize_table_light_inner(
                    table_ref,
                    table_name,
                    date,
                    &project_id,
                    &partition_filters,
                    &selected_files,
                    target_size,
                    &writer_properties,
                    optimize_type.clone(),
                    2,
                    std::time::Instant::now(),
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Cross-flush dedup: collapse a `(project_id, date)` partition by the
    /// schema's `dedup_keys` (last-write-wins) and write back via
    /// `replace_where`. No-op on no dedup_keys / no duplicates (avoids
    /// gratuitous Foyer churn). Returns rows dropped.
    /// Returns `(rows_dropped, complete)`. `complete=false` means duplicate-
    /// bearing work was SKIPPED (unsealed chunks, rewrite budget, vanished
    /// snapshot rows) — the partition must NOT be fingerprinted clean
    /// (2026-07-05 review: a clean fp over skipped dupes let the read-side
    /// dedup skip and the COUNT pushdown serve duplicates).
    pub async fn dedup_partition(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate,
    ) -> Result<(u64, bool)> {
        self.dedup_partition_range(table_ref, table_name, project_id, date, None).await
    }

    /// Stage-and-commit one partition (or one 10-minute bin of it) as a SINGLE
    /// wave. Used by the fallback sweep, which has no queue to batch across; the
    /// dirty-bin path stages with [`Self::stage_dedup_partition_range`] directly
    /// so one wave can span many bins.
    async fn dedup_partition_range(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate, bin: Option<i64>,
    ) -> Result<(u64, bool)> {
        let (units, complete) = self.stage_dedup_partition_range(table_ref, table_name, project_id, date, bin, None).await?;
        if units.is_empty() {
            return Ok((0, complete));
        }
        let markers = vec![format!("date={date}/")];
        let result = self.commit_wave(table_ref, table_name, &markers, true, units, 0).await;
        let dropped = wave_dropped_rows(&result.landed);
        for bin in &result.landed {
            if let Some(d) = &bin.dedup {
                info!("dedup rewrite: table={} chunk=[{}] dropped={} (before={} after={})", table_name, d.label, d.dropped(), d.before, d.after);
            }
        }
        // A unit that didn't land left its duplicates in place — the partition
        // must NOT be certified clean (2026-07-05 review).
        Ok((dropped, complete && result.failed.is_empty()))
    }

    /// Probe-only DataFusion ctx over ONE (project, date) partition's snapshot
    /// files, registered as [`DEDUP_SCAN_NAME`]. Bypasses ProjectRoutingTable:
    /// its MemBuffer union would feed in-flight rows to dedup, which would then
    /// be written to Delta — double-writing on the next real flush.
    ///
    /// Restricts provider construction itself, not merely the SQL scan. An
    /// unrestricted provider eagerly materializes statistics for every live
    /// file in the unified table before partition pruning; at production
    /// scale one 10-minute bin therefore planned ~34k files and retained
    /// hundreds of MB of allocator churn. Paths come from this exact eager
    /// snapshot, so the selection cannot omit a file belonging to the
    /// project/date being certified.
    async fn dedup_probe_ctx(&self, table_ref: &Arc<RwLock<DeltaTable>>, project_id: &str, date_str: &str) -> Result<datafusion::prelude::SessionContext> {
        use deltalake::delta_datafusion::{FileSelection, TableProviderBuilder};
        let (snapshot, log_store) = {
            let table = table_ref.read().await;
            (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
        };
        let partition_files = dedup_partition_paths(snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
        // Probe-only provider (chunk detection). The rewrite builds its own
        // provider per attempt — from a FRESH snapshot, with the synthetic
        // source-file column — in `dedup_rewrite_chunk`.
        let provider = TableProviderBuilder::default()
            .with_log_store(log_store)
            .with_eager_snapshot(snapshot)
            .with_file_selection(FileSelection::from_file_paths(partition_files))
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("delta table provider: {e}"))?;
        // A fresh state is intentional: SessionState clones retain mutable
        // catalog/execution internals and can resolve the scan name to an older
        // eager snapshot. FileSelection above removes the expensive all-table
        // statistics replay that made fresh states harmful in production.
        let ctx = datafusion::prelude::SessionContext::new_with_state(build_optimize_session_state(
            self.config.memory.timefusion_query_partitions,
            self.maintenance_runtime_env(),
        ));
        ctx.register_table(DEDUP_SCAN_NAME, Arc::new(provider))?;
        Ok(ctx)
    }

    /// The 10-minute duplicate probe: returns the bucket starts whose
    /// dedup-key groups have count > 1 under `filter`. Aggregates group keys
    /// only — bounded by key cardinality, not row width (a `SELECT *` +
    /// collect() of a whole day partition transiently allocated tens of GB
    /// outside any memory pool — prod's 2026-06-11 OOM crash loop).
    async fn dup_bin_starts(ctx: &datafusion::prelude::SessionContext, filter: &str, keys_csv: &str) -> Result<Vec<chrono::NaiveDateTime>> {
        let probe = format!(
            "SELECT CAST(date_bin(INTERVAL '10 minutes', \"timestamp\", TIMESTAMP '1970-01-01T00:00:00') AS VARCHAR) FROM \
             (SELECT \"timestamp\", count(*) AS c FROM {DEDUP_SCAN_NAME} WHERE {filter} GROUP BY {keys_csv}) AS g \
             WHERE c > 1 GROUP BY 1 ORDER BY 1"
        );
        let mut starts = Vec::new();
        for batch in ctx.sql(&probe).await?.collect().await? {
            let col = datafusion::arrow::compute::cast(batch.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
            let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("cast to Utf8");
            for i in 0..col.len() {
                if col.is_null(i) {
                    continue;
                }
                // CAST .. AS VARCHAR may append fractional seconds or a
                // timezone suffix; the leading 19 chars are the datetime.
                if let Some(start) = col.value(i).get(..19).and_then(|h19| {
                    chrono::NaiveDateTime::parse_from_str(h19, "%Y-%m-%dT%H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(h19, "%Y-%m-%d %H:%M:%S"))
                        .ok()
                }) {
                    starts.push(start);
                }
            }
        }
        Ok(starts)
    }

    /// BATCH probe (2026-08-05): classify EVERY 10-minute bin of one
    /// (project, date) with a single duplicate probe, returning the bin ids
    /// that contain duplicates. A dup group shares one exact `timestamp` (it
    /// is a dedup key), so the group's bin is derived exactly — only valid
    /// when `timestamp` is a dedup key.
    async fn probe_dup_bins(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date_str: &str,
    ) -> Result<std::collections::HashSet<i64>> {
        const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let ctx = self.dedup_probe_ctx(table_ref, project_id, date_str).await?;
        let safe_pid = project_id.replace('\'', "''");
        let filter = format!("project_id = '{safe_pid}' AND date = DATE '{date_str}'");
        let keys_csv = schema.dedup_keys.iter().map(|k| format!("\"{k}\"")).collect::<Vec<_>>().join(", ");
        Ok(Self::dup_bin_starts(&ctx, &filter, &keys_csv).await?.into_iter().map(|s| s.and_utc().timestamp_micros() / BIN_MICROS).collect())
    }

    /// Probe one partition/bin for duplicates and STAGE (never commit) a
    /// replacement parquet set per duplicate-bearing chunk. Returns the staged
    /// units plus `complete` — false when duplicate-bearing work was skipped
    /// (unsealed chunks, budget guards, vanished snapshot rows), which forbids
    /// certifying the partition clean.
    async fn stage_dedup_partition_range(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate, bin: Option<i64>, key: Option<DirtyBinKey>,
    ) -> Result<(Vec<StagedBin>, bool)> {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        if schema.dedup_keys.is_empty() {
            return Ok((Vec::new(), true));
        }
        let date_str = date.to_string();
        let ctx = self.dedup_probe_ctx(table_ref, project_id, &date_str).await?;
        let scan_name = DEDUP_SCAN_NAME;
        // project_id is currently always a UUID/controlled identifier, but defend in depth: escape single quotes
        // so a future caller can't inject SQL through the partition predicate. date_str comes from NaiveDate::to_string
        // and is already safe.
        let safe_pid = project_id.replace('\'', "''");
        // Keep the full partition predicate separate from the dirty-bin probe
        // scope. `stage_dedup_chunk` removes every file touched by the scoped
        // chunk, then re-reads those files with `partition_filter` so rows in
        // adjacent bins survive the replacement. Passing the bin predicate as
        // `partition_filter` silently kept only ten minutes from a multi-bin
        // parquet file and dropped the rest (prod 2026-08-03).
        let partition_filter = format!("project_id = '{}' AND date = DATE '{}'", safe_pid, date_str);
        let filter = if let Some(bin) = bin {
            const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
            let start = chrono::DateTime::from_timestamp_micros(bin * BIN_MICROS).ok_or_else(|| anyhow::anyhow!("invalid dedup bin {bin}"))?;
            let end = start + chrono::Duration::minutes(10);
            format!(
                "{partition_filter} AND \"timestamp\" >= TIMESTAMP '{}' AND \"timestamp\" < TIMESTAMP '{}'",
                start.format("%Y-%m-%d %H:%M:%S"),
                end.format("%Y-%m-%d %H:%M:%S")
            )
        } else {
            partition_filter.clone()
        };
        // Probe for duplicates BEFORE materializing anything: the common case
        // is zero dupes, and `SELECT *` + collect() of a whole day partition
        // (1.4M wide OTel rows observed) transiently allocated tens of GB
        // outside any memory pool, every 5-minute sweep, for every project —
        // the direct cause of prod's 2026-06-11 OOM crash loop (each kill
        // replayed the WAL, minting the dupes that fattened the next sweep).
        // The probe aggregates group keys only: bounded by key cardinality,
        // not row width. It also stops the every-5-min whole-partition
        // replace_where rewrite, the main Remove-tombstone factory.
        let keys_csv = schema.dedup_keys.iter().map(|k| format!("\"{k}\"")).collect::<Vec<_>>().join(", ");

        // Identify the hour buckets that actually contain duplicates. A dup
        // group shares one exact `timestamp` (it's a dedup key), so chunking
        // the rewrite by hour can never split a group — and it bounds the
        // materialization below to one hour of one project instead of the
        // whole day (the crash-loop backlog made EVERY project probe-positive,
        // so the probe alone still ballooned tens of GB per sweep).
        let (chunks, skipped_any): (Vec<(String, String)>, bool) = if schema.dedup_keys.iter().any(|k| k == "timestamp") {
            // 10-minute bins (not hours): one HOUR of the largest project is
            // >2.1GB of string data — past Arrow's i32 offset limit ("Offset
            // overflow error: 2222394106" in prod) and tens of GB materialized.
            // 10 minutes matches the flush-bucket granularity.
            //
            // Rewriting an hour that late data may still flush into races
            // replace_where against the append (the stale materialized chunk
            // would win and drop the fresh rows — same race the old
            // whole-partition rewrite had for the entire day). The buffer
            // holds up to ~70 min of data, so only hours sealed for 2h+ are
            // rewritten; newer dupes clear on a later sweep.
            let sealed_before = Utc::now().naive_utc() - chrono::Duration::hours(2);
            let mut skipped_unsealed = 0usize;
            let built: Vec<_> = Self::dup_bin_starts(&ctx, &filter, &keys_csv)
                .await?
                .into_iter()
                .filter_map(|start| {
                    let end = start + chrono::Duration::minutes(10);
                    if end > sealed_before {
                        debug!("dedup: skipping unsealed chunk starting {start} (cleared on a later sweep)");
                        skipped_unsealed += 1;
                        return None;
                    }
                    let (s, e) = (start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
                    Some((
                        format!("{filter} AND \"timestamp\" >= TIMESTAMP '{s}' AND \"timestamp\" < TIMESTAMP '{e}'"),
                        // Log label only. The rewrite commits targeted
                        // Remove+Add actions — no replace_where, so no
                        // predicate ever needs kernel evaluation (the old
                        // bare-string predicate defeated file pruning AND
                        // errored delta-kernel's OCC checker).
                        format!("project_id = '{safe_pid}' AND date = '{date_str}' AND timestamp in ['{s}', '{e}')"),
                    ))
                })
                .collect();
            (built, skipped_unsealed > 0)
        } else {
            // No timestamp dedup key → can't chunk safely; whole-partition
            // rewrite, gated on the same any-dupes probe.
            let probe =
                format!("SELECT coalesce(sum(c - 1), 0) FROM (SELECT count(*) AS c FROM {scan_name} WHERE {filter} GROUP BY {keys_csv}) AS g WHERE c > 1");
            let dup_rows = ctx
                .sql(&probe)
                .await?
                .collect()
                .await?
                .first()
                .filter(|b| b.num_rows() > 0)
                .and_then(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|a| a.value(0)))
                .unwrap_or(0);
            if dup_rows <= 0 { (Vec::new(), false) } else { (vec![(filter.clone(), format!("project_id = '{safe_pid}' AND date = '{date_str}'"))], false) }
        };
        if chunks.is_empty() {
            return Ok((Vec::new(), !skipped_any));
        }

        // Chunks of one partition stage CONCURRENTLY, bounded by the same
        // `maintenance_rewrite_sem` each staging task takes around its Arrow
        // materialization — `buffer_unordered` only decides how many tasks are
        // in flight, the semaphore decides how many are decoding at once.
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        let staged: Vec<Result<BinOutcome<StagedBin>>> =
            futures::stream::iter(chunks.into_iter().map(|(chunk_filter, label)| {
                let (partition_filter, key, date_str) = (&partition_filter, key.clone(), date_str.as_str());
                async move {
                    self.stage_dedup_chunk(table_ref, table_name, project_id, schema, scan_name, partition_filter, &chunk_filter, &label, date_str, key).await
                }
            }))
            .buffer_unordered(permits)
            .collect()
            .await;
        let mut units = Vec::new();
        let mut all_complete = !skipped_any;
        let mut first_err = None;
        for outcome in staged {
            match outcome {
                Ok(BinOutcome::Staged(unit)) => units.push(unit),
                // The chunk's rows vanished / were rewritten concurrently:
                // nothing was verified, so the partition stays uncertified.
                Ok(BinOutcome::Retry) => all_complete = false,
                // Probe false-positive: verified duplicate-free, nothing to commit.
                Ok(BinOutcome::Converged) => {}
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_err {
            // One chunk's failure abandons the partition's whole staging batch:
            // clean up the siblings' parquet rather than leaking it (their
            // Adds are in no commit and VACUUM would take days to notice).
            self.discard_bins(&units).await;
            return Err(e);
        }
        Ok((units, all_complete))
    }

    /// STAGE one duplicate-bearing chunk as a TARGETED file rewrite: learn
    /// exactly which files hold the chunk's rows (via the provider's synthetic
    /// [`DEDUP_FILE_COL`]), re-read those files' FULL row sets (rows outside the
    /// chunk window are carried into the replacements verbatim), dedup, and write
    /// replacement parquet. Returns the Remove(old)+Add(new) actions for a WAVE
    /// commit — this function commits nothing.
    ///
    /// Staging without committing is what ends the 572s serial dedup runs
    /// (prod 2026-07-29): chunks used to rewrite AND commit strictly one at a
    /// time because concurrent per-chunk commits to one Delta log were an OCC
    /// storm. With [`Self::commit_wave`] batching them under the shared
    /// per-physical-table commit lock, that reason is gone — and so are the
    /// optimize-vs-dedup delete-delete aborts, since both engines' Removes now
    /// serialize instead of racing inside each other's OCC window.
    ///
    /// No `replace_where`: its predicate had to be a bare string (delta-rs
    /// can't stringify typed TIMESTAMP literals into the commit), which
    /// delta-kernel can't evaluate — defeating file pruning (observed planning
    /// against all 3.6k files / 124GB, the 2026-07-04 OOM crash-loop) and
    /// erroring the OCC checker on every mid-write concurrent commit. With
    /// explicit file actions the conflict surface is exactly the touched
    /// files, and — since files are immutable — there is no race against
    /// concurrent flush appends: fresh rows in the window live in files this
    /// commit never touches.
    ///
    /// `Retry` = the chunk's rows vanished under a concurrent rewrite (caller
    /// must not certify the partition clean); `Converged` = probe false-positive,
    /// verified duplicate-free.
    #[allow(clippy::too_many_arguments)]
    async fn stage_dedup_chunk(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, schema: &crate::schema_loader::TableSchema, scan_name: &str,
        partition_filter: &str, chunk_filter: &str, label: &str, date_str: &str, key: Option<DirtyBinKey>,
    ) -> Result<BinOutcome<StagedBin>> {
        use deltalake::{kernel::Action, writer::DeltaWriter};
        let read_string_column = |batches: Vec<RecordBatch>| -> Result<Vec<String>> {
            let mut out = Vec::new();
            for batch in batches {
                let col = datafusion::arrow::compute::cast(batch.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
                let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("cast to Utf8");
                out.extend((0..col.len()).filter(|&i| !col.is_null(i)).map(|i| col.value(i).to_string()));
            }
            Ok(out)
        };
        // Re-plan loop: a concurrent rewrite (optimize / z-order / another
        // dedup) can remove a target file mid-flight, so the scan's file ids no
        // longer map onto the snapshot's Adds. The snapshot→commit window is NOT
        // handled here any more — `commit_wave`'s liveness check drops the stale
        // unit and the dirty bin is requeued for the next tick.
        const MAX_REPLANS: usize = 3;
        for replan in 0..MAX_REPLANS {
            // Scan and file-mapping MUST share one snapshot: the caller's ctx
            // is pinned at dedup_partition entry, and on the heavily-churned
            // unified table the live file set diverges from it within seconds
            // (flush appends + light optimize) — mapping scan results against
            // the LIVE snapshot mismatched on every attempt in prod
            // (28/28 re-plan exhaustions, zero successes, 2026-07-04). Each
            // re-plan therefore rebuilds provider + ctx from a fresh eager
            // snapshot; the commit-time liveness check below still guards the
            // remaining snapshot→commit window.
            let (chunk_snapshot, chunk_log_store) = {
                let table = table_ref.read().await;
                (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
            };
            use deltalake::delta_datafusion::{FileSelection, TableProviderBuilder};
            let partition_files = dedup_partition_paths(chunk_snapshot.log_data().iter().map(|f| f.path().to_string()), project_id, date_str);
            let provider = TableProviderBuilder::default()
                .with_log_store(chunk_log_store)
                .with_eager_snapshot(Arc::clone(&chunk_snapshot))
                .with_file_column(DEDUP_FILE_COL)
                .with_file_selection(FileSelection::from_file_paths(partition_files))
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("dedup rewrite provider: {e}"))?;
            let ctx = datafusion::prelude::SessionContext::new_with_state(build_optimize_session_state(
                self.config.memory.timefusion_query_partitions,
                self.maintenance_runtime_env(),
            ));
            ctx.register_table(scan_name, Arc::new(provider))?;

            // 1. Which files hold the chunk's rows — ground truth from the
            // scan itself, no per-file stats parsing.
            let files_sql = format!("SELECT DISTINCT \"{DEDUP_FILE_COL}\" FROM {scan_name} WHERE {chunk_filter}");
            let file_ids = read_string_column(ctx.sql(&files_sql).await?.collect().await?)?;
            if file_ids.is_empty() {
                // Probe saw dupes but this snapshot has no rows for the chunk
                // (concurrent rewrite) — nothing verified, don't certify clean.
                return Ok(BinOutcome::Retry);
            }
            // 2. Map scan values to Add actions in the SAME snapshot
            // (suffix-match either direction: the scan column carries the
            // store path, the log a table-relative one).
            let targets = dedup_adds_by_path(
                chunk_snapshot
                    .log_data()
                    .iter()
                    .filter(|f| {
                        let p = f.path();
                        file_ids.iter().any(|v| v.ends_with(p.as_ref()) || p.ends_with(v.as_str()))
                    })
                    // Deprecated in favour of arrow-direct access, but the
                    // Remove tombstones below need the Add's exact fields.
                    .map(|f| {
                        #[allow(deprecated)]
                        f.add_action()
                    }),
                table_name,
            );
            if targets.len() != file_ids.len() {
                warn!(
                    "dedup rewrite: mapped {}/{} files for table={} chunk=[{}] (sample scan value: {:?}), re-planning",
                    targets.len(),
                    file_ids.len(),
                    table_name,
                    label,
                    file_ids.first()
                );
                tokio::time::sleep(occ_backoff(replan)).await;
                continue;
            }

            // 2026-07-29 (Phase 2): the delta-rs `SortByDedup` OptimizeBuilder
            // fast path was removed here. It rewrote AND committed inside one
            // call, so it could not be staged for a wave — and its per-chunk
            // commit is precisely the delete-delete partner that aborted against
            // light optimize. The shard path below covers the same inputs: the
            // fast path only ran when the whole chunk fit the rewrite budgets,
            // which is the shard path's `shards == 1` case.
            //
            // 3. Decide the shard count. A dedup `SELECT * … collect()` decodes to
            // Arrow at 5-20× compressed OUTSIDE the memory pool, so an over-budget
            // chunk used to be skipped (dupe left forever). Instead we split the
            // rewrite into K passes bucketed by an md5 hash of the dedup keys — every
            // copy of a key hashes to one bucket (never split), and md5 (not `key % K`,
            // which collides for ms-aligned values) spreads evenly and is NULL-safe.
            // K = ceil(estimated decoded bytes / budget); the estimate is the
            // row-count-vs-inflation MAX ×2 documented on the config fields.
            let rewrite_bytes: i64 = targets.iter().map(|a| a.size).sum();
            // Fail closed unless the provider's full-file re-read can be
            // checked against Delta's independent row-count metadata. This is
            // the invariant that would have stopped the 2026-08-03 loss: the
            // buggy bin-scoped re-read produced 63k rows while removing files
            // whose Add actions described 5.8M live rows. Deletion-vector
            // cardinality is subtracted because the provider correctly hides
            // those already-deleted physical rows.
            let expected_live_rows = targets.iter().try_fold(0u64, |sum, add| -> Result<u64> {
                let stats = add.get_stats()?.ok_or_else(|| anyhow::anyhow!("dedup rewrite refuses target without num_records stats: {}", add.path))?;
                let rows = u64::try_from(stats.num_records).map_err(|_| anyhow::anyhow!("dedup rewrite target has negative num_records: {}", add.path))?;
                let deleted = u64::try_from(add.deletion_vector.as_ref().map_or(0, |dv| dv.cardinality))
                    .map_err(|_| anyhow::anyhow!("dedup rewrite target has negative deletion-vector cardinality: {}", add.path))?;
                let live = rows
                    .checked_sub(deleted)
                    .ok_or_else(|| anyhow::anyhow!("dedup rewrite target deletion-vector cardinality exceeds num_records: {}", add.path))?;
                sum.checked_add(live).ok_or_else(|| anyhow::anyhow!("dedup rewrite target row count overflow"))
            })?;
            let compressed_budget = self.config.maintenance.timefusion_dedup_max_rewrite_bytes;
            let inflation = self.config.maintenance.timefusion_dedup_decode_inflation.max(1);
            let decoded_budget = self.config.maintenance.timefusion_dedup_max_decoded_bytes;
            let bytes_per_row = self.config.maintenance.timefusion_dedup_bytes_per_row;
            let est_decoded_bytes: u64 = targets
                .iter()
                .map(|a| {
                    let by_rows = a.get_stats().ok().flatten().map_or(0, |s| (s.num_records.max(0) as u64).saturating_mul(bytes_per_row));
                    let by_size = (a.size.max(0) as u64).saturating_mul(inflation);
                    by_rows.max(by_size)
                })
                .sum::<u64>()
                .saturating_mul(2); // RowConverter keyed copy in dedup_batches
            let shards = dedup_shard_count(est_decoded_bytes, rewrite_bytes.max(0) as u64, decoded_budget, compressed_budget);
            let in_list = file_ids.iter().map(|v| format!("'{}'", v.replace('\'', "''"))).collect::<Vec<_>>().join(", ");
            // Bucket = first byte of md5 over the dedup keys (2 hex chars =
            // DEDUP_BUCKET_COUNT buckets, evenly spread); chr(31) separates keys so
            // distinct tuples can't collide. Also the GROUP BY for the skew probe below.
            let keys_varchar = schema.dedup_keys.iter().map(|k| format!("CAST(\"{k}\" AS VARCHAR)")).collect::<Vec<_>>().join(", ");
            let bucket_expr = format!("substr(md5(concat_ws(chr(31), {keys_varchar})), 1, 2)");
            // Independent narrow oracle for the staged output count. The
            // Arrow rewrite below chooses the greatest tiebreak per key, but it
            // must still emit exactly one row per distinct key (tombstones are
            // retained). A disagreement rejects the unit before Remove actions
            // can reach `commit_wave`.
            let logical_rows_sql = format!(
                "SELECT count(*) FROM (SELECT 1 FROM {scan_name} WHERE {partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}) GROUP BY {keys_varchar})"
            );
            let expected_logical_rows = ctx
                .sql(&logical_rows_sql)
                .await?
                .collect()
                .await?
                .first()
                .filter(|batch| batch.num_rows() == 1)
                .and_then(|batch| batch.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>())
                .map(|array| array.value(0))
                .ok_or_else(|| anyhow::anyhow!("dedup rewrite distinct-key validation returned no scalar"))?;
            let expected_logical_rows =
                u64::try_from(expected_logical_rows).map_err(|_| anyhow::anyhow!("dedup rewrite distinct-key validation returned a negative count"))?;

            // Sharding can't split a single key group — all copies share one bucket.
            // If the largest group alone would blow the budget, no shard count helps,
            // so skip (preserving the pre-fix OOM-safety) rather than materialize it.
            if shards > 1 && decoded_budget > 0 {
                let max_group_sql = format!(
                    "SELECT coalesce(max(c), 0) FROM (SELECT count(*) AS c FROM {scan_name} WHERE {partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}) GROUP BY {keys_varchar})"
                );
                let max_group = ctx
                    .sql(&max_group_sql)
                    .await?
                    .collect()
                    .await?
                    .first()
                    .and_then(|b| b.column(0).as_any().downcast_ref::<datafusion::arrow::array::Int64Array>().map(|a| a.value(0)))
                    .unwrap_or(0);
                if (max_group.max(0) as u64).saturating_mul(bytes_per_row).saturating_mul(2) > decoded_budget {
                    crate::metrics::record_dedup_chunk_skipped();
                    error!(
                        "dedup rewrite SKIPPED (single key group of {} rows over decoded budget — unshardable): table={} chunk=[{}] files={} — duplicates persist until compaction shrinks the file set",
                        max_group,
                        table_name,
                        label,
                        targets.len()
                    );
                    return Ok(BinOutcome::Retry);
                }
            }

            // 4. Rewrite each shard independently: collect (bounded to ~one budget by
            // the bucket range), dedup, stage its own parquet. The permit bounds
            // concurrent Arrow materializations across the sweep — unlike hot-wave
            // staging (which has its own K-bounded light pool), dedup materializes
            // Arrow OUTSIDE any pool, which is exactly what this semaphore is for.
            // Held for the shard loop only, dropped before the unit is handed to a
            // wave (the commit decodes nothing). Out-of-window rows in the target files carry through verbatim
            // (their keys are unique → no drop). On any per-shard error, already-staged
            // parquet is cleaned before returning so a mid-loop failure leaks nothing.
            let rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
            let staging_table = { table_ref.read().await.clone() };
            let stage_store = staging_table.log_store().object_store(None);
            let (mut before, mut after) = (0usize, 0usize);
            let mut adds: Vec<Action> = Vec::new();
            let stage_result: anyhow::Result<()> = async {
                for shard in 0..shards {
                    let shard_pred = if shards > 1 {
                        // Contiguous bucket range per shard (even ±1); string compare of
                        // zero-padded lowercase hex == numeric order.
                        let (lo, hi) = (shard * DEDUP_BUCKET_COUNT / shards, (shard + 1) * DEDUP_BUCKET_COUNT / shards);
                        let upper = if hi < DEDUP_BUCKET_COUNT { format!(" AND {bucket_expr} < '{hi:02x}'") } else { String::new() };
                        format!(" AND {bucket_expr} >= '{lo:02x}'{upper}")
                    } else {
                        String::new()
                    };
                    let rows_sql = format!("SELECT * FROM {scan_name} WHERE {partition_filter} AND \"{DEDUP_FILE_COL}\" IN ({in_list}){shard_pred}");
                    let batches: Vec<RecordBatch> =
                        ctx.sql(&rows_sql).await?.collect().await?.into_iter().map(|b| drop_batch_column(b, DEDUP_FILE_COL)).collect();
                    let shard_before: usize = batches.iter().map(|b| b.num_rows()).sum();
                    if shard_before == 0 {
                        continue;
                    }
                    before += shard_before;
                    // Version collapse: greatest `dedup_tiebreak` per key wins, so a
                    // merge-on-read table's newest version survives and the older ones
                    // are dropped here rather than at every read.
                    //
                    // Tombstones are RETAINED (`drop_tombstones = None`). Dropping one
                    // requires that no older version of its key can exist outside this
                    // rewrite's input. The input is every live file of this
                    // (project_id, date) snapshot holding a row in the 10-minute chunk
                    // window; since `timestamp` is a dedup key and `date` derives from
                    // it, all versions of a key do share that window — but three ways
                    // an older version outlives the rewrite are NOT excludable here:
                    //   1. files appended after the file-id query (flush, WAL replay,
                    //      an off-box writer). `commit_wave`'s liveness check verifies
                    //      the TARGETS still exist; it cannot see a new file carrying
                    //      an older version of the same key.
                    //   2. rows still in MemBuffer/WAL/hot tier. The 2h sealed-chunk
                    //      guard bounds EVENT time, not arrival: a late client re-send
                    //      (or a version append, which carries the base row's original
                    //      `timestamp`) lands in a long-sealed window at any wall clock.
                    //   3. tables whose `dedup_keys` omit `timestamp` take the
                    //      whole-partition branch above, where versions of one key may
                    //      sit in date partitions this sweep never holds together.
                    // A retained tombstone costs one row per deleted key forever; a
                    // dropped one silently resurrects the row. Retain.
                    let deduped = crate::mem_buffer::dedup_batches(batches, &schema.dedup_keys, schema.dedup_tiebreak.as_deref(), None)?;
                    after += deduped.iter().map(|b| b.num_rows()).sum::<usize>();
                    // Variant struct columns may still be BinaryView if the partition
                    // mixes tiers — cast to Binary so the write accepts the schema.
                    let deduped: Vec<RecordBatch> = deduped.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;
                    let (deduped, sorted) = self.sort_flush_group(schema, deduped).await;
                    let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_intermediate, sorted);
                    let mut writer = deltalake::writer::RecordBatchWriter::for_table(&staging_table)
                        .map_err(|e| anyhow::anyhow!("dedup rewrite writer: {e}"))?
                        .with_writer_properties(writer_properties);
                    let target_schema = writer.arrow_schema();
                    for b in deduped {
                        let casted = deltalake::kernel::schema::cast_record_batch(&b?, target_schema.clone(), true, true)?;
                        writer.write(casted).await.map_err(|e| anyhow::anyhow!("dedup rewrite stage: {e}"))?;
                    }
                    adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("dedup rewrite flush: {e}"))?.into_iter().map(Action::Add));
                }
                Ok(())
            }
            .await;
            drop(rewrite_permit);
            if let Err(e) = stage_result {
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                return Err(e);
            }
            if !dedup_rewrite_counts_match(before as u64, expected_live_rows, after as u64, expected_logical_rows) {
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                anyhow::bail!(
                    "dedup rewrite validation failed for table={} chunk=[{}]: reread={}/{} expected live rows, output={}/{} expected logical rows",
                    table_name,
                    label,
                    before,
                    expected_live_rows,
                    after,
                    expected_logical_rows
                );
            }
            if before == 0 {
                return Ok(BinOutcome::Retry);
            }
            if before == after {
                // Probe false-positive (a concurrent rewrite already deduped): discard
                // the staged no-op copies, certify clean, commit nothing.
                Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                return Ok(BinOutcome::Converged);
            }
            // Row-DROPPING rewrite: data_change=true on both sides. See
            // `staged_actions` — the snapshot-isolation downgrade the hot path
            // enjoys is only sound for data-preserving commits.
            let (removes, adds) = staged_actions(&targets, adds, true);
            // Record the intent BEFORE the unit can be handed to a wave commit, so
            // a crash anywhere in the staging->commit window leaves a trail to
            // clean up (same guarantee as hot bins).
            let wave_id = uuid::Uuid::new_v4().to_string();
            self.record_staged_intent(&StagedIntent {
                wave_id: wave_id.clone(),
                table_name: table_name.to_string(),
                project_id: project_id.to_string(),
                recorded_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
                paths: adds.iter().filter_map(|a| if let Action::Add(add) = a { Some(add.path.clone()) } else { None }).collect(),
            });
            debug!(table_name, project_id, chunk = label, files = targets.len(), before, after, event = "dedup_chunk_staged");
            return Ok(BinOutcome::Staged(StagedBin {
                project_id: project_id.to_string(),
                wave_id,
                target_paths: targets.iter().map(|t| t.path.clone()).collect(),
                removes,
                adds,
                stage_store,
                dedup: Some(DedupUnit { key: key.clone(), date: date_str.to_string(), label: label.to_string(), before: before as u64, after: after as u64 }),
            }));
        }
        anyhow::bail!("dedup rewrite: re-plan attempts exhausted for table={} chunk=[{}]", table_name, label)
    }

    /// Live parquet files of one `date=` partition, grouped by the
    /// `project_id=` path segment ("default" when absent — custom-project
    /// tables don't embed it). Shared by the sweep's fingerprint capture and
    /// the read-side dedup-skip check so both hash identical groupings.
    fn partition_files_by_pid(table: &DeltaTable, date_marker: &str) -> Result<HashMap<String, Vec<String>>> {
        let mut m: HashMap<String, Vec<String>> = HashMap::new();
        for uri in table.get_file_uris()?.filter(|u| u.contains(date_marker) && u.ends_with(".parquet")) {
            let pid = uri.split('/').find_map(|seg| seg.strip_prefix("project_id=")).unwrap_or("default").to_string();
            m.entry(pid).or_default().push(uri);
        }
        Ok(m)
    }

    /// True iff every (project, date) partition overlapping `window` carries
    /// a clean fingerprint matching its CURRENT live file set (0-drop sweep
    /// pass, unchanged since). Shared by the read-side DedupExec skip and the
    /// COUNT(*) stats pushdown — both need duplicates provably absent.
    /// Takes the SAME `table` guard the caller will scan/sum from, so the
    /// fingerprint verdict and the data read share one snapshot (no
    /// check-then-use window; 2026-07-05 review hardening).
    pub(crate) fn dedup_window_clean(&self, table: &DeltaTable, project_id: &str, table_name: &str, (lo, hi): (i64, i64)) -> bool {
        let Some(dates) = window_dates(lo, hi) else { return false };
        dates.into_iter().all(|date| {
            let Ok(mut by_pid) = Self::partition_files_by_pid(table, &format!("date={date}")) else { return false };
            // The sweep keys custom-project tables (no project_id= path
            // segment) under "default"; match its grouping exactly.
            let Some((key_pid, files)) =
                by_pid.remove(project_id).map(|f| (project_id.to_string(), f)).or_else(|| by_pid.remove("default").map(|f| ("default".to_string(), f)))
            else {
                return true; // no Delta files for this date → nothing to dedup
            };
            let fp_key = (key_pid, table_name.to_string(), date.to_string());
            self.dedup_clean_fp.get(&fp_key).is_some_and(|fp| *fp.value() == partition_file_fp(files))
        })
    }

    fn logical_count_partition_snapshot(table: &DeltaTable, project_id: &str, date: &str) -> Result<(u64, Vec<String>)> {
        let snapshot = table.snapshot()?.snapshot();
        let files = dedup_partition_paths(snapshot.log_data().iter().map(|file| file.path().to_string()), project_id, date);
        Ok((partition_file_fp(files.clone()), files))
    }

    /// Memory-only lookup for a base whose files are all present in the table
    /// snapshot the caller holds. Newly appended files are returned for a
    /// narrow overlay; any removal/rewrite declines. Filesystem IO is forbidden
    /// on this query path.
    pub(crate) fn logical_count_memory_for_files(
        &self, project_id: &str, table_name: &str, date: &str, files: &std::collections::HashSet<String>,
    ) -> Option<(Arc<crate::logical_count_index::LogicalCountIndex>, Vec<String>)> {
        let key = crate::logical_count_index::CountPartition { project_id: project_id.to_string(), table_name: table_name.to_string(), date: date.to_string() };
        self.logical_count_cache.get_memory_appendable(&key, files)
    }

    pub(crate) async fn logical_count_overlay_batches(
        &self, snapshot: Arc<deltalake::kernel::EagerSnapshot>, log_store: deltalake::logstore::LogStoreRef, files: Vec<String>,
        columns: crate::logical_count_index::LogicalCountColumns<'_>,
    ) -> Result<Vec<RecordBatch>> {
        use deltalake::delta_datafusion::{FileSelection, TableProviderBuilder};
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let provider = TableProviderBuilder::default()
            .with_log_store(log_store)
            .with_eager_snapshot(snapshot)
            .with_file_selection(FileSelection::from_file_paths(files))
            .build()
            .await
            .map_err(|error| anyhow::anyhow!("logical-count overlay provider: {error}"))?;
        let context = SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.shared_runtime_env()));
        context.register_table("__logical_count_overlay", Arc::new(provider))?;
        Ok(context
            .table("__logical_count_overlay")
            .await?
            .select_columns(&[columns.timestamp, columns.id, columns.tiebreak, columns.deleted])?
            .collect()
            .await?)
    }

    /// Schedule one exact partition build. Concurrent misses share the same
    /// single-flight key and the global semaphore bounds winner-map memory.
    pub(crate) fn schedule_logical_count_build(self: &Arc<Self>, project_id: &str, table_name: &str, date: &str, force_refresh: bool) {
        let key = crate::logical_count_index::CountPartition { project_id: project_id.to_string(), table_name: table_name.to_string(), date: date.to_string() };
        if !self.logical_count_building.insert(key.clone()) {
            return;
        }
        let database = Arc::clone(self);
        tokio::spawn(async move {
            let result = database.build_logical_count_partition(&key, force_refresh).await;
            database.logical_count_building.remove(&key);
            if let Err(error) = result {
                warn!(project_id = key.project_id, table_name = key.table_name, date = key.date, %error, "logical-count background build failed");
            }
        });
    }

    async fn build_logical_count_partition(&self, key: &crate::logical_count_index::CountPartition, force_refresh: bool) -> Result<()> {
        use deltalake::delta_datafusion::{FileSelection, TableProviderBuilder};

        let _permit = tokio::select! {
            permit = self.logical_count_build_sem.acquire() => permit?,
            () = self.maintenance_shutdown.cancelled() => return Ok(()),
        };
        let started = std::time::Instant::now();
        let table_ref = self.resolve_table(&key.project_id, &key.table_name).await?;
        let (fingerprint, files, eager_snapshot, log_store) = {
            let table = table_ref.read().await;
            let (fingerprint, files) = Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?;
            (fingerprint, files, Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
        };

        // Restart warm-up first tries the persistent Arrow tier off the async
        // worker. A valid file installs its memory front without scanning Delta.
        let cache = Arc::clone(&self.logical_count_cache);
        let disk_key = key.clone();
        let current_files = files.iter().cloned().collect();
        if !force_refresh
            && let Some(added_files) = tokio::task::spawn_blocking(move || cache.load_appendable(&disk_key, &current_files)).await?
            && added_files <= crate::logical_count_index::MAX_APPEND_OVERLAY_FILES
        {
            return Ok(());
        }

        let declared = get_schema(&key.table_name).ok_or_else(|| anyhow::anyhow!("logical-count table is not registered"))?;
        anyhow::ensure!(declared.dedup_keys == ["timestamp", "id"], "logical-count currently requires dedup keys [timestamp,id]");
        let tiebreak = declared.dedup_tiebreak.as_deref().ok_or_else(|| anyhow::anyhow!("logical-count table has no dedup tiebreak"))?;
        let deleted = declared.tombstone_column.as_deref().ok_or_else(|| anyhow::anyhow!("logical-count table has no tombstone column"))?;
        let columns = crate::logical_count_index::LogicalCountColumns { timestamp: "timestamp", id: "id", tiebreak, deleted };
        let mut index = crate::logical_count_index::LogicalCountIndex::new();

        if !files.is_empty() {
            let provider = TableProviderBuilder::default()
                .with_log_store(log_store)
                .with_eager_snapshot(eager_snapshot)
                .with_file_selection(FileSelection::from_file_paths(files.clone()))
                .build()
                .await
                .map_err(|error| anyhow::anyhow!("logical-count provider: {error}"))?;
            let context =
                SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions, self.maintenance_runtime_env()));
            context.register_table("__logical_count_src", Arc::new(provider))?;
            let frame = context.table("__logical_count_src").await?.select_columns(&[columns.timestamp, columns.id, columns.tiebreak, columns.deleted])?;
            let mut stream = frame.execute_stream().await?;
            loop {
                let batch = tokio::select! {
                    batch = stream.try_next() => batch?,
                    () = self.maintenance_shutdown.cancelled() => return Ok(()),
                };
                let Some(batch) = batch else { break };
                index.apply_batch(&batch, columns)?;
                // The mutable builder intentionally costs more than the packed
                // resident form. Let it use half of this cache's budget while
                // retaining the host brake below; applying the four-way
                // resident limit here prevented large days from ever reaching
                // `finalize`, where their allocator/hash overhead is released.
                let build_limit = (self.config.derived.logical_count_memory_bytes() / 2).max(1);
                anyhow::ensure!(
                    index.estimated_heap_bytes() <= build_limit,
                    "logical-count partition exceeded its {}MB temporary build limit",
                    build_limit / (1024 * 1024)
                );
                let host_limit = self.config.derived.memory_brake_limit_bytes();
                anyhow::ensure!(
                    process_memory_bytes().is_none_or(|used| used <= host_limit),
                    "logical-count build stopped at the host memory brake ({}MB)",
                    host_limit / (1024 * 1024)
                );
            }
        }

        // Release the allocation-heavy mutable hash map before cache
        // admission. The packed form is exact and is the representation used
        // by every query and persisted Arrow partition.
        index.finalize()?;
        // A three-day dashboard window can touch four UTC partitions. Reserve
        // room for all four after compaction so valid daily indexes cannot
        // evict one another into a permanent rebuild loop.
        let per_index_limit = (self.config.derived.logical_count_memory_bytes() / 4).max(1);
        anyhow::ensure!(
            index.estimated_heap_bytes() <= per_index_limit,
            "logical-count partition exceeded its {}MB packed resident limit",
            per_index_limit / (1024 * 1024)
        );

        // Concurrent appends are safe: the query overlays their new files.
        // A removal/rewrite is not; it would leave winners from files no longer
        // in the table, so refuse publication and let the next miss rebuild.
        let current_files = {
            let table = table_ref.read().await;
            Self::logical_count_partition_snapshot(&table, &key.project_id, &key.date)?.1.into_iter().collect::<std::collections::HashSet<_>>()
        };
        anyhow::ensure!(files.iter().all(|file| current_files.contains(file)), "logical-count partition was rewritten during build");

        let physical_keys = index.physical_keys();
        let logical_rows = index.logical_rows();
        let estimated_bytes = index.estimated_heap_bytes();
        let file_count = files.len();
        let cache = Arc::clone(&self.logical_count_cache);
        let install_key = key.clone();
        tokio::task::spawn_blocking(move || cache.install(install_key, fingerprint, files, index)).await??;
        info!(
            project_id = key.project_id,
            table_name = key.table_name,
            date = key.date,
            fingerprint,
            file_count,
            physical_keys,
            logical_rows,
            estimated_bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "logical-count partition ready"
        );
        Ok(())
    }

    /// Sweep every `(project_id, today)` partition in this table via
    /// `dedup_partition`. Skips when Delta version is unchanged since the
    /// last sweep, and skips partitions in failure backoff. Best-effort:
    /// per-partition errors are logged and back the partition off.
    pub async fn dedup_today_partitions(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str) -> Result<()> {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        if schema.dedup_keys.is_empty() {
            return Ok(());
        }
        // Sweep today plus a lookback window: a cross-flush dupe that lands in a
        // prior-day partition (late DLQ replay crossing midnight UTC) would never
        // collapse under a today-only scope. The global version skip below still
        // bounds cost — we only re-scan the window when the table has new commits.
        let today = Utc::now().date_naive();
        let lookback = self.config.maintenance.timefusion_dedup_lookback_days as i64;
        let dates: Vec<chrono::NaiveDate> = (0..=lookback).rev().map(|d| today - chrono::Duration::days(d)).collect();

        let pre_version = table_ref.read().await.version().unwrap_or(0);
        if self.last_dedup_versions.read().await.get(dedup_key).copied() == Some(pre_version) {
            debug!("dedup sweep: table={} version={} unchanged — skipping", table_name, pre_version);
            return Ok(());
        }

        let mut total_dropped = 0u64;
        let mut any_ok = false;
        for date in dates {
            let date_marker = format!("date={}", date);
            // Per-project live file lists for this date. Custom-project tables
            // don't embed project_id in the path; sweep "default".
            let files_by_pid: HashMap<String, Vec<String>> = {
                let table = table_ref.read().await;
                Self::partition_files_by_pid(&table, &date_marker)?
            };
            let project_ids: std::collections::HashSet<String> =
                if files_by_pid.is_empty() { std::iter::once("default".to_string()).collect() } else { files_by_pid.keys().cloned().collect() };
            for pid in &project_ids {
                // Bail promptly on shutdown — a mid-sweep tick must not run
                // against a closing Foyer cache and hang the graceful drain.
                if self.maintenance_shutdown.is_cancelled() {
                    debug!("dedup sweep: shutdown requested, aborting table={}", table_name);
                    return Ok(());
                }
                // Incremental skip: a partition already certified clean whose live
                // file set is unchanged since that pass can't have gained dupes —
                // they only arrive in NEW files. Skip the whole-partition probe,
                // keeping the sweep O(partitions-changed). The version guard above
                // only fires when the WHOLE table is unchanged, which never holds
                // under continuous ingest; this per-partition check does (sealed
                // lookback days, and today between flushes).
                let fp_key = (pid.clone(), table_name.to_string(), date.to_string());
                let cur_files = files_by_pid.get(pid).cloned().unwrap_or_default();
                if !cur_files.is_empty() && self.dedup_clean_fp.get(&fp_key).map(|e| *e.value()) == Some(partition_file_fp(cur_files.clone())) {
                    continue;
                }
                let backoff_key = format!("{dedup_key}:{pid}:{date}");
                if let Some(entry) = self.dedup_backoff.get(&backoff_key)
                    && std::time::Instant::now() < entry.value().1
                {
                    crate::metrics::record_dedup_chunk_skipped();
                    debug!("dedup sweep: {} in failure backoff, skipping", backoff_key);
                    continue;
                }
                match self.dedup_partition(table_ref, table_name, pid, date).await {
                    Ok((d, complete)) => {
                        self.dedup_backoff.remove(&backoff_key);
                        total_dropped += d;
                        any_ok = true;
                        // Clean-partition fingerprint for the read-side dedup
                        // skip: a 0-drop pass over a file set that is STILL
                        // the live set proves the partition duplicate-free.
                        // Any concurrent commit (flush/compaction) changes
                        // the set → don't mark; a >0 pass marks nothing (the
                        // NEXT 0-drop pass confirms the rewrite held).
                        let pre = cur_files;
                        let post = {
                            let table = table_ref.read().await;
                            Self::partition_files_by_pid(&table, &date_marker)?.remove(pid).unwrap_or_default()
                        };
                        // `complete` is required: Ok(0) with skipped unsealed/
                        // over-budget dup chunks must NOT certify the partition.
                        let fp_post = partition_file_fp(post.clone());
                        if d == 0 && complete && !post.is_empty() && partition_file_fp(pre) == fp_post {
                            self.dedup_clean_fp.insert(fp_key, fp_post);
                        } else {
                            self.dedup_clean_fp.remove(&fp_key);
                        }
                    }
                    Err(e) => {
                        // Exponential backoff, 10min doubling to a 6h cap —
                        // a failing partition must not re-run (and re-fail)
                        // on every 5-minute sweep tick.
                        let attempts = self.dedup_backoff.get(&backoff_key).map_or(0, |e| e.value().0) + 1;
                        let delay = std::time::Duration::from_secs((600u64 << (attempts.min(7) - 1)).min(21_600));
                        self.dedup_backoff.insert(backoff_key, (attempts, std::time::Instant::now() + delay));
                        self.dedup_clean_fp.remove(&fp_key);
                        warn!(
                            "dedup sweep: project={} date={} table={} failed (attempt {}, next retry in {}s): {}",
                            pid,
                            date,
                            table_name,
                            attempts,
                            delay.as_secs(),
                            e
                        );
                    }
                }
            }
        }
        // Only refresh the skip cache when at least one partition ran cleanly,
        // so persistent failures don't silently suppress future sweeps.
        // TODO: same unbounded-growth caveat as `last_written_versions`.
        if any_ok {
            let post_version = table_ref.read().await.version().unwrap_or(pre_version);
            self.last_dedup_versions.write().await.insert(dedup_key.to_string(), post_version);
        }
        if total_dropped > 0 {
            info!("dedup sweep: table={} key={} total_dropped={}", table_name, dedup_key, total_dropped);
        }
        Ok(())
    }

    fn persist_dirty_bins(&self) {
        let mut bins: Vec<_> = self
            .dedup_dirty_bins
            .iter()
            .map(|entry| {
                let (project_id, table_name, date, bin) = entry.key();
                crate::dirty_bin_queue::DirtyBin { project_id: project_id.clone(), table_name: table_name.clone(), date: date.clone(), bin: *bin }
            })
            .collect();
        bins.sort_by(|a, b| (&a.table_name, &a.project_id, &a.date, a.bin).cmp(&(&b.table_name, &b.project_id, &b.date, b.bin)));
        crate::dirty_bin_queue::store(&self.config.core.timefusion_data_dir, &bins);
        crate::metrics::maintenance_stats().dirty_bin_queue_depth.store(bins.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    fn enqueue_dirty_bin(&self, project_id: &str, table_name: &str, date: &str, bin: i64) {
        let key = (project_id.to_string(), table_name.to_string(), date.to_string(), bin);
        if self.dedup_dirty_bins.insert(key, ()).is_none() {
            crate::metrics::maintenance_stats().dirty_bin_enqueued.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(project_id, table_name, date, bin, event = "dirty_bin_enqueued");
            self.persist_dirty_bins();
        }
    }

    /// Is persistence healthy enough to spend the shared commit path on dedup?
    /// Reuses the compaction brake's signal (`is_wal_backlog_over_threshold`),
    /// which is true both when the unflushed backlog is over its threshold and
    /// while a recent flush FAILURE is inside the brake window.
    fn dedup_flush_healthy(&self) -> bool {
        // Memory brake included: the drain's chunk loop is the one heavy path
        // that never crosses a wave boundary, so the wave-level brake could
        // not stop it — prod 2026-07-30 04:31 OOM-killed at 112GB anon with
        // memory_brakes_total=0 while a drain pass rode RSS up unbraked.
        !self.buffered_layer().is_some_and(|layer| layer.is_wal_backlog_over_threshold()) && self.light_optimize_brake().is_none()
    }

    /// Order one drain pass and split off the work it will not do.
    ///
    /// NEWEST-FIRST: recent partitions are the ones queries actually read, so
    /// dedup there pays immediately; ancient duplicates are already invisible
    /// (read-side `DedupExec` collapses them) and only cost storage. Boot
    /// 2026-07-30 drained a 10-day backlog oldest-first and spent its whole
    /// life rewriting 2026-07-20 while the hot window stayed dirty.
    ///
    /// COLD BINS LAST, never dropped: a date owned by the nightly consolidate
    /// (`date_is_cold`) is bin-packed by `consolidate_date_binned`, which is a
    /// pure compaction (`OptimizeType::Compact`/`SortBy`) and does NOT collapse
    /// duplicates — so this drain remains their only physical dedup. They sink
    /// to lowest priority (drained only when no hot bin is waiting) and the
    /// remainder is counted + summarized once per pass, never one line per bin.
    /// (`cold_optimize_after_days` defaults to 1 and the drain already skips
    /// today, so today that split degenerates to "everything is cold" and the
    /// newest-first order alone protects the hot window; the tier stays wired to
    /// `date_is_cold` so raising the setting does the right thing.)
    ///
    /// Returns `(ready, deferred_cold)`; `deferred_cold` stays on the queue.
    /// Dates are ISO-8601, so lexicographic order is chronological.
    fn select_drain_bins(mut candidates: Vec<DrainBin>, today: chrono::NaiveDate, after_days: u64, batch: usize) -> (Vec<DrainBin>, Vec<DrainBin>) {
        candidates.sort_by(|a, b| (&b.1, b.2).cmp(&(&a.1, a.2)));
        // An unparseable date sorts cold: it can't be shown to be hot, and the
        // staging call will surface the parse error when it is finally served.
        let (hot, mut cold): (Vec<_>, Vec<_>) = candidates
            .into_iter()
            .partition(|(_, date, _)| chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_ok_and(|d| !Self::date_is_cold(today, d, after_days)));
        // Cold bins get a RESERVED share of the batch. Hot-first is right — a
        // boot that drained a 10-day backlog oldest-first never reached the hot
        // window (2026-07-30) — but giving hot the WHOLE batch starves cold
        // forever whenever hot work is continuous: prod 2026-08-02 sat at
        // queue=22135 with 20556 deferred cold and dirty_bin_processed=0, so the
        // backlog that keeps files duplicated never shrank at all. Reserving half
        // keeps hot's priority while making the cold backlog drain monotonically.
        let cold_reserve = cold.len().min(batch / 2);
        let mut ready: Vec<_> = hot.into_iter().take(batch.saturating_sub(cold_reserve)).collect();
        // Hot under-using its share hands the remainder back to cold.
        let deferred = cold.split_off(cold.len().min(batch.saturating_sub(ready.len())));
        ready.extend(cold);
        (ready, deferred)
    }

    async fn dedup_dirty_bins_for_table(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, flush_healthy: &(dyn Fn() -> bool + Sync), stage_deadline: std::time::Duration,
    ) -> Result<()> {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        if schema.dedup_keys.is_empty() {
            return Ok(());
        }
        // Dedup is an OPTIMIZATION — read-side DedupExec and flush-time dedup
        // already keep results correct — so it must never compete with the
        // persistence path for the per-table commit lock.
        if !flush_healthy() {
            crate::metrics::maintenance_stats().dedup_passes_flush_yields.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(table_name, event = "dedup_drain_flush_yield");
            return Ok(());
        }
        const BIN_MICROS: i64 = 10 * 60 * 1_000_000;
        // Eligible bins drained per table per tick. 8 couldn't keep up with the
        // enqueue rate (prod backlog 3341, 2026-07-20); 128 was sized for the
        // per-bin-probe cost model and drained a 22k backlog in ~a day. With
        // the batch probe, per-pass cost is ~one probe per (project, date)
        // GROUP plus staging for the dup-bearing minority (~3%), so a large
        // batch classifies a whole backlog in a handful of cheap probes — a
        // 25k queue spans only ~140 groups (2026-08-05).
        const DIRTY_BIN_DRAIN_BATCH: usize = 1024;
        // Per-shard byte budgets in `stage_dedup_chunk` bound Arrow
        // materialization; `stage_deadline` bounds each bin's WALL CLOCK (see
        // the call site in `run_dedup_for_table`).
        let sealed_before = (Utc::now() - chrono::Duration::hours(2)).timestamp_micros();
        // Today's SEALED bins are eligible. The old whole-partition
        // replace_where path had to skip today because it repeatedly planned
        // and rewrote a growing partition (579 rows dropped / 648s observed).
        // Staging is now restricted to snapshot-exact project/date files and
        // rewrites only files proven to hold the bin, while `sealed_before`
        // keeps it away from the live MemBuffer/late-arrival window. Deferring
        // all of today until tomorrow left recent dashboard queries doing >2x
        // merge-on-read work by construction.
        let today_date = Utc::now().date_naive();
        let candidates: Vec<_> = self
            .dedup_dirty_bins
            .iter()
            .filter_map(|entry| {
                let (project, name, date, bin) = entry.key();
                (name == table_name && (*bin + 1) * BIN_MICROS <= sealed_before).then(|| (project.clone(), date.clone(), *bin))
            })
            .collect();
        let (ready, deferred) = Self::select_drain_bins(
            candidates,
            today_date,
            self.config.parquet.cold_optimize_after_days(),
            // Fixed, not a knob: the env override this replaced
            // (TIMEFUSION_DIRTY_BIN_DRAIN_BATCH=1, a stale incident throttle
            // resurrected by every CapRover deploy) froze a 22k-bin backlog at
            // one bin per tick (2026-08-03). Deleted like the other drifted
            // memory knobs — the drain self-regulates via flush-health yields,
            // the memory brake and the rewrite semaphore, not operator envs.
            DIRTY_BIN_DRAIN_BATCH,
        );
        if !deferred.is_empty() {
            crate::metrics::maintenance_stats().dedup_bins_deferred_cold.fetch_add(deferred.len() as u64, std::sync::atomic::Ordering::Relaxed);
            // ONE bounded summary per pass — a 10-day backlog is thousands of bins.
            info!(
                table_name,
                deferred = deferred.len(),
                oldest = deferred.last().map(|(_, date, _)| date.as_str()).unwrap_or_default(),
                event = "dedup_bins_deferred_cold"
            );
        }
        if ready.is_empty() {
            return Ok(());
        }
        // Phase 3 (2026-08-05): BATCH the probes. Every flushed bin is
        // enqueued, so most queued bins carry no duplicates at all (~97% in
        // prod: 601 probed clean vs 18 rewritten) — yet each paid its own
        // partition-restricted probe scan. One whole-date probe classifies
        // every queued bin of a (project, date) at once; only dup-bearing
        // bins continue into per-bin staging. Probe failure or timeout fails
        // OPEN to the per-bin path.
        let ready =
            if schema.dedup_keys.iter().any(|k| k == "timestamp") { self.batch_probe_classify(table, table_name, ready, stage_deadline).await } else { ready };
        if ready.is_empty() {
            self.persist_dirty_bins();
            return Ok(());
        }
        // Phase 2 (2026-07-29): bins STAGE in parallel and commit in WAVES.
        // Previously each bin rewrote and committed strictly one at a time —
        // serialization was deliberate, because concurrent per-bin commits to
        // one Delta log were an OCC storm — and a drain took up to 572s. Batched
        // commits remove that reason, so the only remaining bound on rewrite
        // parallelism is memory: `stage_dedup_chunk` takes a
        // `maintenance_rewrite_sem` permit around its (pool-invisible) Arrow
        // materialization, and `buffer_unordered(permits)` keeps in-flight
        // staging matched to it rather than unbounded.
        //
        // A bounded stream, not the hot path's `round_robin_bins` driver: the
        // dirty queue is already fair (FIFO, capped per tick by
        // `dirty_bin_drain_batch`), each bin is served exactly once per tick,
        // and there is no per-round re-plan — the driver's rotation/round
        // semantics would add ceremony with nothing to schedule.
        use futures::stream::StreamExt;
        let permits = self.config.derived.rewrite_permits().max(1);
        // A wave's units all sit in memory as Delta actions only (their parquet
        // is already in R2), so the cap is about commit size, not memory.
        const DEDUP_WAVE_UNITS: usize = 8;
        let mut staging = futures::stream::iter(ready.into_iter().map(|(project_id, date, bin)| async move {
            let key: DirtyBinKey = (project_id.clone(), table_name.to_string(), date.clone(), bin);
            // No persist here: rewriting the whole multi-MB queue file per
            // dequeue made the drain O(queue x batch) in fsync I/O. Crash
            // direction is safe — an unpersisted dequeue reappears after
            // restart and re-dedups (idempotent). End-of-pass persists.
            self.dedup_dirty_bins.remove(&key);
            crate::metrics::maintenance_stats().dirty_bin_eligible.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            info!(project_id, table_name, date, bin, event = "dirty_bin_dequeued");
            let started = std::time::Instant::now();
            let staged = match chrono::NaiveDate::parse_from_str(&date, "%Y-%m-%d") {
                // Timing out a bin discards its staged work (any uploaded
                // parquet is uncommitted and falls to VACUUM) and retries it
                // next pass — acceptable now that per-shard byte budgets keep
                // legitimate bins to minutes, after an UNBOUNDED staging read
                // wedged the whole drain for 6.5h behind the 1-permit
                // maintenance semaphore (prod 2026-08-05). The Err lands in
                // the ordinary failure arm below: requeue + warn.
                Ok(parsed) => {
                    match tokio::time::timeout(
                        stage_deadline,
                        self.stage_dedup_partition_range(table, table_name, &project_id, parsed, Some(bin), Some(key.clone())),
                    )
                    .await
                    {
                        Ok(staged) => staged,
                        Err(_) => {
                            crate::metrics::maintenance_stats().dedup_bin_stage_timeouts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Err(anyhow::anyhow!("staging exceeded the {stage_deadline:?} per-bin deadline (hung object-store read?)"))
                        }
                    }
                }
                Err(e) => Err(anyhow::anyhow!("invalid dirty-bin date {date}: {e}")),
            };
            (key, started.elapsed(), staged)
        }))
        .buffer_unordered(permits);

        let mut wave: Vec<StagedBin> = Vec::new();
        let requeue = |key: DirtyBinKey, counter: &std::sync::atomic::AtomicU64| {
            self.dedup_dirty_bins.insert(key, ());
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        };
        // Once the wave gate gives up on flush recovery, stop committing but
        // KEEP DRAINING the stream — an in-flight staging future has already
        // removed its key from the queue and dropping it would lose the bin.
        let mut committing = true;
        while let Some((key, elapsed, staged)) = staging.next().await {
            let stats = crate::metrics::maintenance_stats();
            let (project_id, _, date, bin) = key.clone();
            if !committing {
                requeue(key, &stats.dirty_bin_requeued);
                continue;
            }
            match staged {
                Err(error) => {
                    requeue(key, &stats.dirty_bin_requeued);
                    warn!(project_id, table_name, date, bin, %error, event = "dirty_bin_failure");
                    continue;
                }
                Ok((units, complete)) => {
                    stats.dirty_bin_rewrite_duration_ms.fetch_add(elapsed.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
                    // Duplicate-bearing work was skipped inside the bin (unsealed
                    // chunk, unshardable key group): the bin is NOT done, so it
                    // goes back on the queue even if its other chunks land.
                    if !complete {
                        requeue(key, &stats.dirty_bin_requeued);
                        warn!(project_id, table_name, date, bin, event = "dirty_bin_requeued");
                    } else if units.is_empty() {
                        // A bin with nothing to rewrite (already compacted /
                        // no duplicates) never enters a wave, so count its
                        // drain here or the processed metric reads 0 while
                        // the queue empties.
                        stats.dirty_bin_processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    wave.extend(units);
                }
            }
            if wave.len() >= DEDUP_WAVE_UNITS {
                committing = self.commit_dedup_wave_when_flush_healthy(table, table_name, &mut wave, flush_healthy, &requeue).await;
            }
        }
        if !wave.is_empty() {
            self.commit_dedup_wave_when_flush_healthy(table, table_name, &mut wave, flush_healthy, &requeue).await;
        }
        self.persist_dirty_bins();
        Ok(())
    }

    /// Runs the batch probe over each (project, date) with ≥2 queued bins and
    /// strips the probe-clean bins out of `ready`, consuming them. Group keys
    /// are dequeued BEFORE the probe so dirtiness enqueued while it runs
    /// re-queues the bin (the same ordering the per-bin path relies on). A
    /// singleton keeps the per-bin path — its bin-scoped probe prunes to ten
    /// minutes of files where the whole-date probe scans them all.
    async fn batch_probe_classify(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, ready: Vec<(String, String, i64)>, deadline: std::time::Duration,
    ) -> Vec<(String, String, i64)> {
        use std::sync::atomic::Ordering::Relaxed;
        let mut groups: std::collections::HashMap<(&str, &str), Vec<i64>> = Default::default();
        for (project, date, bin) in &ready {
            groups.entry((project, date)).or_default().push(*bin);
        }
        let mut clean: std::collections::HashSet<(String, String, i64)> = Default::default();
        for ((project, date), bins) in groups {
            if bins.len() < 2 || chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").is_err() {
                continue;
            }
            for bin in &bins {
                self.dedup_dirty_bins.remove(&(project.to_string(), table_name.to_string(), date.to_string(), *bin));
            }
            match tokio::time::timeout(deadline, self.probe_dup_bins(table, table_name, project, date)).await {
                Ok(Ok(dup_bins)) => {
                    let stats = crate::metrics::maintenance_stats();
                    let cleared: Vec<_> = bins.iter().filter(|b| !dup_bins.contains(b)).collect();
                    stats.dirty_bin_processed.fetch_add(cleared.len() as u64, Relaxed);
                    stats.dirty_bin_batch_probe_clean.fetch_add(cleared.len() as u64, Relaxed);
                    info!(project, table_name, date, queued = bins.len(), clean = cleared.len(), event = "dedup_batch_probe");
                    clean.extend(cleared.into_iter().map(|b| (project.to_string(), date.to_string(), *b)));
                }
                Ok(Err(error)) => warn!(project, table_name, date, %error, event = "dedup_batch_probe_failure"),
                Err(_) => {
                    crate::metrics::maintenance_stats().dedup_bin_stage_timeouts.fetch_add(1, Relaxed);
                    warn!(project, table_name, date, event = "dedup_batch_probe_timeout");
                }
            }
        }
        ready.into_iter().filter(|b| !clean.contains(b)).collect()
    }

    /// Wave-commit gate: a pass can outlive its start-of-pass health check (a
    /// full drain batch is minutes of rewrites), and dedup must not compete
    /// with persistence for the commit lock — but ONE transient unhealthy
    /// sample must not forfeit the batch either. Latching used to requeue
    /// every remaining bin after a single bad sample, silently discarding an
    /// entire 128-bin pass at boot (~75 min of staging, prod 2026-08-05).
    /// Waits (bounded) for flush to recover; if it doesn't, requeues this
    /// wave and returns false so the pass stops committing.
    async fn commit_dedup_wave_when_flush_healthy(
        &self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, wave: &mut Vec<StagedBin>, flush_healthy: &(dyn Fn() -> bool + Sync),
        requeue: &(dyn Fn(DirtyBinKey, &std::sync::atomic::AtomicU64) + Sync),
    ) -> bool {
        const FLUSH_RECOVERY_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
        let t0 = std::time::Instant::now();
        while !flush_healthy() {
            if t0.elapsed() >= FLUSH_RECOVERY_WAIT {
                let stats = crate::metrics::maintenance_stats();
                stats.dedup_passes_flush_yields.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(table_name, requeued = wave.len(), event = "dedup_drain_flush_yield");
                for key in wave.drain(..).filter_map(|unit| unit.dedup.as_ref().and_then(|d| d.key.clone())) {
                    requeue(key, &stats.dirty_bin_requeued);
                }
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        self.commit_dedup_wave(table, table_name, std::mem::take(wave)).await;
        true
    }

    /// Commit one dedup wave and settle its units' dirty-bin bookkeeping: a unit
    /// that didn't land (stale target, failed/unconfirmed commit) puts its bin
    /// back on the queue, because its duplicates are still in the table.
    async fn commit_dedup_wave(&self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, units: Vec<StagedBin>) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut markers: Vec<String> = units.iter().filter_map(|u| u.dedup.as_ref()).map(|d| format!("date={}/", d.date)).collect();
        markers.sort();
        markers.dedup();
        let result = self.commit_wave(table, table_name, &markers, true, units, 0).await;
        let stats = crate::metrics::maintenance_stats();
        let mut landed_bins: std::collections::HashSet<DirtyBinKey> = std::collections::HashSet::new();
        for unit in &result.landed {
            let Some(d) = &unit.dedup else { continue };
            info!(table_name, chunk = d.label, dropped = d.dropped(), before = d.before, after = d.after, event = "dirty_bin_chunk_complete");
            stats.dirty_bin_dropped_rows.fetch_add(d.dropped(), Relaxed);
            if let Some(key) = d.key.clone() {
                landed_bins.insert(key);
            }
        }
        for unit in &result.failed {
            let Some(key) = unit.dedup.as_ref().and_then(|d| d.key.clone()) else { continue };
            landed_bins.remove(&key);
            let (project_id, _, date, bin) = key.clone();
            self.dedup_dirty_bins.insert(key, ());
            stats.dirty_bin_requeued.fetch_add(1, Relaxed);
            warn!(project_id, table_name, date, bin, event = "dirty_bin_requeued");
        }
        stats.dirty_bin_processed.fetch_add(landed_bins.len() as u64, Relaxed);
        self.persist_dirty_bins();
    }

    /// One table's dedup of sealed partitions (dirty-bin rewrite + optional
    /// fallback sweep). The 90s deadline is a warning threshold, not a
    /// cancellation: a slow-but-healthy table is allowed to finish.
    async fn run_dedup_for_table(&self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str, label: &str) {
        if !self.config.maintenance.timefusion_dirty_bin_dedup_enabled {
            debug!(table_name, event = "dirty_bin_dedup_paused", "physical dirty-bin dedup is disabled; read-side dedup remains active");
            return;
        }
        const DEDUP_WARN: std::time::Duration = std::time::Duration::from_secs(90);
        let t0 = std::time::Instant::now();
        // Deadline per bin STAGING attempt, not per pass — generous enough
        // that only a pathological hang trips it (typical bins stage in
        // seconds; sharded oversized bins in minutes, including rewrite-sem
        // waits shared with light-optimize).
        const DEDUP_BIN_STAGE_DEADLINE: std::time::Duration = std::time::Duration::from_secs(900);
        match self.dedup_dirty_bins_for_table(table, table_name, &|| self.dedup_flush_healthy(), DEDUP_BIN_STAGE_DEADLINE).await {
            Ok(()) if t0.elapsed() > DEDUP_WARN => {
                warn!("Dirty-bin dedup for {label} took {:?} (exceeds {DEDUP_WARN:?} warning threshold)", t0.elapsed());
                crate::metrics::maintenance_stats().dedup_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Ok(()) => {}
            Err(e) => {
                crate::metrics::maintenance_stats().dedup_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!("Dirty-bin dedup failed for {label}: {e}");
            }
        }
        if self.config.maintenance.timefusion_dedup_sweep_fallback {
            let t0 = std::time::Instant::now();
            match self.dedup_today_partitions(table, table_name, dedup_key).await {
                Ok(()) if t0.elapsed() > DEDUP_WARN => {
                    warn!("Dedup fallback sweep for {label} took {:?} (exceeds {DEDUP_WARN:?} warning threshold)", t0.elapsed());
                    crate::metrics::maintenance_stats().dedup_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Ok(()) => {}
                Err(e) => {
                    crate::metrics::maintenance_stats().dedup_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    error!("Dedup fallback sweep failed for {label}: {e}");
                }
            }
        }
    }

    /// One table's hot-tail compaction (bin-pack today's small files). The 180s
    /// deadline is a warning threshold, not a cancellation.
    async fn run_hot_compact_for_table(&self, table: &Arc<RwLock<DeltaTable>>, table_name: &str, label: &str) {
        const OPTIMIZE_WARN: std::time::Duration = std::time::Duration::from_secs(180);
        let t0 = std::time::Instant::now();
        match self.optimize_table_light(table, table_name).await {
            Ok(()) => {
                if t0.elapsed() > OPTIMIZE_WARN {
                    warn!("Light optimize for {label} took {:?} (exceeds {OPTIMIZE_WARN:?} warning threshold)", t0.elapsed());
                    crate::metrics::maintenance_stats().light_optimize_timed_out.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    info!("Light optimize completed for {label}");
                }
            }
            Err(e) => {
                crate::metrics::maintenance_stats().light_optimize_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                error!("Light optimize failed for {label}: {e}");
            }
        }
    }

    /// Hot-tail compaction for one table, as plan-once → rewrite-parallel →
    /// commit-once WAVES (design doc: docs/compaction-redesign-2026-07-29.md).
    ///
    /// Per tick: ONE tag-first metadata walk plans a bin for every hot project
    /// (`select_all_hot_bins`), each round's bins are rewritten to staged parquet
    /// in parallel WITHOUT touching the Delta log, and the whole round lands in
    /// ONE `CommitBuilder` transaction. This replaces the per-bin
    /// `OptimizeBuilder` path: it was 132 metadata walks and ~130 commits per
    /// tick on one shared log, where the commits alone (OCC ladders to attempt
    /// 9-20) were most of a 40-65s pass, and the per-project greedy drain left 8
    /// of 11 hot projects unreached on a 5-min cron (prod 2026-07-29).
    ///
    /// `TIMEFUSION_LIGHT_OPTIMIZE_ENABLED=false` remains the incident kill switch.
    pub async fn optimize_table_light(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        // `crate::clock`, not `Utc::now()`: the hot tail scopes itself to TODAY's
        // partition and to an event-time seal window, so a wall-clock read here
        // makes the whole pass unreachable from the virtual-time e2e harness —
        // which is why this path had no end-to-end coverage and shipped writing
        // every output unsorted. In production the clock IS the wall clock.
        let today = crate::clock::now_micros();
        let today = chrono::DateTime::from_timestamp_micros(today).map(|d| d.date_naive()).unwrap_or_else(|| Utc::now().date_naive());
        let today_str = today.to_string();
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        let target_size = self.config.maintenance.timefusion_light_optimize_target_size;
        let min_files = self.config.maintenance.timefusion_compact_min_files;
        // Plan ONCE for round 0; later rounds re-plan from the post-commit
        // snapshot (see `plan` below) so a wave never re-selects the run it just
        // wrote. Bins are ordered by compaction debt.
        let mut planned = {
            let table = table_ref.read().await;
            Self::select_all_hot_bins(&table, schema, &today_str, target_size, min_files, target_size / 2)?
        };
        // Rotation cursor: start where the last truncated tick stopped so the
        // same tail is never skipped twice in a row (a truncated tick otherwise
        // always serves the same debt-ordered prefix).
        let cursor = self.light_optimize_cursor.swap(0, Relaxed);
        if cursor > 0 && cursor < planned.len() {
            planned.rotate_left(cursor);
        }
        if planned.is_empty() {
            return Ok(());
        }
        crate::metrics::maintenance_stats().light_optimize_projects_planned.fetch_add(planned.len() as u64, Relaxed);
        info!(table_name, date = %today, projects = planned.len(), event = "light_optimize_planned");
        let project_ids: Vec<String> = planned.iter().map(|(project_id, _)| project_id.clone()).collect();
        // Bins the current wave should stage, replaced wholesale by each wave's
        // post-commit re-plan. A project absent from the map has no work left
        // this tick and drops out of the round-robin.
        let plan: tokio::sync::Mutex<HashMap<String, Vec<String>>> = tokio::sync::Mutex::new(planned.into_iter().collect());

        let concurrency = self.config.derived.light_optimize_k(project_ids.len());
        // Bound total rounds so a large backlog can't wedge the tick even if the
        // wall-clock budget is raised.
        const MAX_WAVES: usize = 12;
        let deadline = std::time::Instant::now() + self.light_optimize_tick_budget();
        let order_index: HashMap<String, usize> = project_ids.iter().enumerate().map(|(i, p)| (p.clone(), i)).collect();
        let failed = round_robin_bins(
            project_ids,
            MAX_WAVES,
            concurrency,
            deadline,
            |round, remaining| {
                info!(table_name, round, remaining = remaining.len(), event = "light_optimize_tick_budget_exhausted");
                crate::metrics::maintenance_stats().light_optimize_tick_truncated.fetch_add(1, Relaxed);
                // Next tick starts at the first project this tick never served.
                let resume = remaining.first().and_then(|p| order_index.get(p).copied()).unwrap_or(0);
                self.light_optimize_cursor.store(resume, Relaxed);
            },
            || self.light_optimize_brake(),
            |project_id, round| {
                let (schema, plan) = (schema, &plan);
                async move {
                    let files = plan.lock().await.remove(&project_id).unwrap_or_default();
                    if files.is_empty() {
                        return (project_id, Ok(BinOutcome::Converged));
                    }
                    info!(table_name, project_id, date = %today, selected_files = files.len(), round, event = "light_optimize_tail_selected");
                    let staged = self.stage_hot_bin(table_ref, table_name, schema, &project_id, files).await;
                    (project_id, staged)
                }
            },
            |bins, round| {
                let (plan, today_str) = (&plan, today_str.as_str());
                async move {
                    let staged = bins.len();
                    let failed = self.commit_wave(table_ref, table_name, &[format!("date={today_str}/")], false, bins, round).await.failed.len();
                    // Round 0 only: one bin per project, so this is directly
                    // comparable to `projects_planned` (the alert is
                    // completed < planned for N consecutive ticks).
                    if round == 0 {
                        crate::metrics::maintenance_stats().light_optimize_projects_completed.fetch_add((staged - failed.min(staged)) as u64, Relaxed);
                    }
                    // Re-plan the NEXT wave from the just-committed snapshot: the
                    // outputs are tagged sorted runs and excluded from
                    // re-selection, so this yields each project's next time slice
                    // — never the run this wave wrote. One walk per wave, not per
                    // project per pass — and none at all when no further round
                    // can run (round cap / deadline), which would walk the
                    // snapshot only to discard the result.
                    if round + 1 < MAX_WAVES && std::time::Instant::now() < deadline {
                        let next = {
                            let table = table_ref.read().await;
                            Self::select_all_hot_bins(&table, schema, today_str, target_size, min_files, target_size / 2).unwrap_or_default()
                        };
                        *plan.lock().await = next.into_iter().collect();
                    }
                    failed
                }
            },
        )
        .await;
        // Checkpoint after the tick's final commit rather than per N versions:
        // wave commits are ~40x rarer than the old per-bin commits, so a
        // version-count cadence would checkpoint ~40x less often exactly where
        // replay-tail length is the top CPU cost (34.5% of process CPU in
        // ScanLogReplayProcessor, prod profile 2026-07-29).
        self.checkpoint_after_waves(table_ref, table_name).await;
        anyhow::ensure!(failed == 0, "Light optimize failed for {failed} hot bin(s)");
        Ok(())
    }

    /// Stage ONE bin's rewrite: read exactly the selected files, sort by the
    /// schema keys, write staged parquet, and return the Remove+Add actions for
    /// the wave commit. No Delta commit and no table lock — pure object-store +
    /// CPU work, so waves parallelize against the idle cores instead of
    /// serializing behind the log. Uncommitted parquet is invisible to readers,
    /// and any failure cleans up its own staged files (`cleanup_orphaned_parquet`)
    /// so a failed bin leaks nothing and blocks no other bin.
    ///
    /// `Retry` = the bin's files were rewritten concurrently (dedup race); the
    /// project stays in the rotation and the next round's re-plan serves it a
    /// fresh bin. `Converged` = nothing worth staging.
    async fn stage_hot_bin(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, schema: &crate::schema_loader::TableSchema, project_id: &str, files: Vec<String>,
    ) -> Result<BinOutcome<StagedBin>> {
        use deltalake::{delta_datafusion::TableProviderBuilder, kernel::Action, writer::DeltaWriter};
        // One read-lock, one table clone per bin: the pinned scan snapshot and
        // the writer's staging table both derive from it (a second clone per
        // bin was pure waste — K bins x up to 12 waves per tick).
        let staging_table = { table_ref.read().await.clone() };
        let (snapshot, log_store) = (Arc::new(staging_table.snapshot()?.snapshot().clone()), staging_table.log_store());
        // Map paths to Add actions in the SAME snapshot the scan reads, so the
        // Remove tombstones carry the exact fields of the files we rewrote.
        let wanted: std::collections::HashSet<&str> = files.iter().map(String::as_str).collect();
        let targets = dedup_adds_by_path(
            snapshot.log_data().iter().filter(|f| wanted.contains(f.path().as_ref())).map(|f| {
                #[allow(deprecated)]
                f.add_action()
            }),
            table_name,
        );
        if targets.len() != files.len() {
            debug!(table_name, project_id, mapped = targets.len(), selected = files.len(), event = "light_optimize_bin_vanished");
            return Ok(BinOutcome::Retry);
        }
        // The wave engine's OWN permit — NEVER maintenance_rewrite_sem. That
        // semaphore (2 permits, dedup/optimize/recompress) exists because heavy
        // rewrites' Arrow is pool-invisible; taking it here would cap waves at 2
        // (or 0 while a dedup drain holds both — prod 2026-07-30: 25+ min of
        // hot-compact starvation) and burn the tick deadline waiting. Wave
        // staging is already bounded by K and sized by the light pool slice, so
        // this permit is a ceiling + the instrumented wait point below.
        let permit_wait = std::time::Instant::now();
        let _light_permit = self.light_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("light rewrite semaphore closed: {e}"))?;
        let permit_wait_ms = permit_wait.elapsed().as_millis() as u64;
        let stage_started = std::time::Instant::now();
        // Bytes read into this rewrite — free here (the Adds are already mapped)
        // and the divisor that turns staging duration into observed R2 throughput.
        let bytes_in: i64 = targets.iter().map(|a| a.size).sum();
        let stage_store = staging_table.log_store().object_store(None);
        let mut adds: Vec<Action> = Vec::new();
        let staged: Result<()> = async {
            // File-scoped provider over the pinned snapshot: reads exactly this
            // bin's files, so no predicate and no per-file stats parsing.
            let provider = TableProviderBuilder::default()
                .with_log_store(log_store)
                .with_eager_snapshot(Arc::clone(&snapshot))
                .with_file_paths(files.clone())
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("hot bin provider: {e}"))?;
            // The light session state forces non-view Parquet types: Variant
            // columns are Struct{Binary, Binary} on disk and a view-typed read
            // blows the rewrite up mid-scan with "Expected Binary, got BinaryView".
            let ctx = datafusion::prelude::SessionContext::new_with_state(self.light_optimize_session_state());
            // Unique per staging: the cached session state's clone SHARES its
            // catalog, so a fixed name collides across the k concurrent
            // stagings ("The table hot_bin already exists", prod 2026-07-30 —
            // serial k=1-2 never collided; k~9 parallelism exposed it).
            // Deregistered right after the read so the shared catalog can't
            // accumulate entries.
            let bin_table = format!("hot_bin_{}", uuid::Uuid::new_v4().simple());
            ctx.register_table(&bin_table, Arc::new(provider))?;
            // ORDER BY in the PLAN, streamed — not `collect()` + an in-process
            // Arrow lexsort.
            //
            // `sort_batches_by_schema` refuses to sort past `SORT_SKIP_BYTES`
            // (256 MB of in-memory Arrow) and silently returns `sorted=false`.
            // A hot bin is packed to `light_optimize_target_size` — 256 MB of
            // FILE bytes — and prod's zstd ratio is ~17x, so EVERY bin arrived
            // ~17x over that threshold and every hot-tail output was written
            // unsorted: measured 2026-08-01, 0 of the 8 largest files in a live
            // partition declared `sorting_columns`. One such file is enough,
            // because the reader's `derive_common_ordering` is all-or-nothing —
            // so the scan lost its declared ordering, which cost the streaming
            // top-N pushdown AND forced `DedupExec` into its unbounded
            // `full-set` seen-set, the per-query memory behind the OOM/restart
            // cycle.
            //
            // Sorting in the plan fixes both halves: DataFusion merges the
            // already-sorted inputs with a `SortPreservingMergeExec` (one batch
            // per file, independent of bin size) and falls back to a SortExec
            // that spills into the light pool where they are not — instead of
            // materialising the whole bin 2-3x with no pool and no spill. The
            // footer declaration is then honest by construction.
            let order_by = schema_order_by_clause(schema);
            let sorted = !order_by.is_empty();
            let read = ctx.sql(&format!("SELECT * FROM {bin_table}{order_by}")).await;
            // Intermediate tier: this output is rewritten tonight by
            // consolidate/recompress, so it isn't worth max compression.
            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_intermediate, sorted);
            let mut writer = deltalake::writer::RecordBatchWriter::for_table(&staging_table)
                .map_err(|e| anyhow::anyhow!("hot bin writer: {e}"))?
                .with_writer_properties(writer_properties);
            let target_schema = writer.arrow_schema();
            let mut stream = match read {
                Ok(df) => df.execute_stream().await,
                Err(e) => Err(e),
            }?;
            let mut rows_staged = 0usize;
            while let Some(batch) = stream.next().await {
                let batch = cast_variant_columns_to_binary(batch?)?;
                if batch.num_rows() == 0 {
                    continue;
                }
                rows_staged += batch.num_rows();
                let casted = deltalake::kernel::schema::cast_record_batch(&batch, target_schema.clone(), true, true)?;
                writer.write(casted).await.map_err(|e| anyhow::anyhow!("hot bin stage: {e}"))?;
            }
            drop(stream);
            let _ = ctx.deregister_table(&bin_table);
            if rows_staged == 0 {
                return Ok(());
            }
            adds.extend(writer.flush().await.map_err(|e| anyhow::anyhow!("hot bin flush: {e}"))?.into_iter().map(|mut add| {
                // Tag the output so the next tick's selection treats it as a
                // sorted run (folded only while under the sorted-run cap).
                if sorted {
                    add.tags.get_or_insert_with(Default::default).insert(SORTED_RUN_TAG.to_string(), Some("true".to_string()));
                }
                Action::Add(add)
            }));
            Ok(())
        }
        .await;
        if let Err(e) = staged {
            Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
            warn!("Light optimize staging failed for project={} table={}: {}", project_id, table_name, e);
            return Err(e);
        }
        if adds.is_empty() {
            // Zero rows staged: nothing to commit, and retrying the same
            // zero-row selection would loop — treat as converged for this tick.
            return Ok(BinOutcome::Converged);
        }
        // Record the intent BEFORE the bin can be handed to a wave commit, so a
        // crash anywhere in the staging→commit window leaves a trail to clean up.
        let wave_id = uuid::Uuid::new_v4().to_string();
        self.record_staged_intent(&StagedIntent {
            wave_id: wave_id.clone(),
            table_name: table_name.to_string(),
            project_id: project_id.to_string(),
            recorded_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
            paths: adds.iter().filter_map(|a| if let Action::Add(add) = a { Some(add.path.clone()) } else { None }).collect(),
        });
        // Data-preserving compaction: BOTH sides carry data_change=false so the
        // fork's snapshot-isolation downgrade applies and concurrent ingest
        // appends can't veto the wave (see `staged_actions`; aa50480).
        let (removes, adds) = staged_actions(&targets, adds, false);
        // Splits a slow tick into its two causes: permit contention vs the
        // object-store rewrite itself (bytes_in / staging_ms = observed R2
        // throughput). One line per staged bin — waves are ~K per round.
        info!(
            table_name,
            project_id,
            selected_files = targets.len(),
            bytes_in,
            staging_ms = stage_started.elapsed().as_millis() as u64,
            permit_wait_ms,
            event = "wave_bin_staged"
        );
        Ok(BinOutcome::Staged(StagedBin { project_id: project_id.to_string(), wave_id, target_paths: files, removes, adds, stage_store, dedup: None }))
    }

    /// Commit one WAVE: every staged unit's Remove+Add in a SINGLE transaction.
    /// Before committing, each unit's target files are verified still live in the
    /// refreshed snapshot; a unit whose target was rewritten concurrently has ONLY
    /// its own actions dropped (and its staged parquet cleaned) — the rest of the
    /// wave still commits.
    ///
    /// Shared by BOTH producers — hot-tail compaction (today's partitions) and
    /// dirty-bin dedup (sealed dates). They are disjoint by construction: dedup
    /// skips `date == today` and hot compaction only ever selects it, so the two
    /// engines never stage the same file. What they DO share is this commit path
    /// and the per-physical-table commit lock, which is what ends the
    /// optimize-vs-dedup delete-delete aborts (prod 2026-07-29, 3x in one day):
    /// their Removes can no longer interleave inside each other's OCC window.
    ///
    /// `data_change` selects the two engines' one real difference — see
    /// [`staged_actions`] and [`wave_operation`]. Every unit in a wave must agree
    /// on it (waves are built by one producer).
    async fn commit_wave(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, date_markers: &[String], data_change: bool, bins: Vec<StagedBin>, round: usize,
    ) -> WaveResult {
        use deltalake::kernel::{Action, transaction::TableReference};
        debug_assert!(bins.iter().all(|b| b.data_change() == data_change), "a wave must not mix data-preserving and row-dropping units");
        let engine = if data_change { "dedup" } else { "light optimize" };
        let mut bins = bins;
        let mut failed: Vec<StagedBin> = Vec::new();
        // Bins already CONFIRMED landed by an earlier attempt of this wave (see
        // the self-landed split below). Carried across OCC retries so their
        // credit — and their dirty-bin certification — is never lost.
        let mut carried: Vec<StagedBin> = Vec::new();
        // Key on "" explicitly: the wave spans MULTIPLE projects of one physical
        // table, and every other unified-log writer (flush, dedup, coalesced
        // commit) serializes under the ("", table) key. Keying on
        // bins[0].project_id would silently pick a DIFFERENT lock if that
        // project has custom storage (table_lock_key only collapses non-custom
        // projects) — the liveness check would then race dedup's Removes.
        let commit_lock = self.commit_lock("", table_name).await;
        // Same key as the lock above — flush/ingest committers queued on it.
        let flush_waiters = self.flush_waiters("", table_name).await;
        // The wave spans several projects of a handful of dates, so the
        // warm/evict diff is scoped to those dates rather than the whole
        // (26k-file) table.
        let markers: Vec<&str> = date_markers.iter().map(String::as_str).collect();
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        const MAX_RETRIES: usize = 4;
        for attempt in 0..MAX_RETRIES {
            // FLUSH PRIORITY (prod 2026-07-30). The lock is FIFO, so joining the
            // queue ahead of a waiting flush costs it OUR whole commit — and on a
            // backlogged tick several wave commits, each legally minutes long,
            // stack up in front of it (flush waited >600s to ACQUIRE and its
            // watchdog killed the attempt; nothing was hung). Durability outranks
            // maintenance: we don't enqueue at all while a flush is waiting, so
            // flush latency is bounded by ONE in-flight wave commit.
            //
            // NOT a starvation risk for waves: flush is periodic (60s cadence) and
            // its commit is a short log append, so the count is zero for most of
            // every minute — a wave that stands down here re-stages and finds a
            // gap on a later tick. If it were EVER continuously nonzero, flush
            // would be saturating the commit path and compaction is exactly the
            // work that should yield.
            if flush_waiters.load(std::sync::atomic::Ordering::SeqCst) > 0 {
                crate::metrics::maintenance_stats().wave_commits_yielded_to_flush.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(table_name, engine, round, attempt = attempt + 1, bins = bins.len(), event = "wave_commit_flush_yield");
                // Nothing was committed, so the bins' target files are still live
                // and the staged parquet is referenced by nothing. Delta VACUUM
                // cannot see uncommitted staged files, so leaving them would leak
                // on S3 forever; the next tick re-stages from the same (still
                // live) targets. Dedup's bins go back on the dirty queue via the
                // `failed` list — a partition with duplicates still in it must
                // never be certified clean.
                self.discard_bins(&bins).await;
                failed.extend(bins);
                return WaveResult { landed: carried, failed };
            }
            let commit_guard = commit_lock.lock().await;
            // Bounded: this reads the log over the network with the commit lock
            // held. A timeout here just means we build on a possibly-stale
            // snapshot — the liveness check + OCC retry ladder below already
            // handle that, and the lock is freed on schedule either way.
            if let Err(e) = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "wave_refresh",
                table_name,
                refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot),
            )
            .await
            {
                debug!("{engine} wave pre-commit refresh failed (attempt {}): {}", attempt + 1, e.message);
            }
            let mut new_table = { table_ref.read().await.clone() };
            let live: std::collections::HashSet<String> = match new_table.snapshot() {
                Ok(s) => s.log_data().iter().map(|f| f.path().into_owned()).collect(),
                Err(e) => {
                    drop(commit_guard);
                    error!("{engine} wave: no snapshot for {table_name}: {e}");
                    self.discard_bins(&bins).await;
                    failed.extend(bins);
                    return WaveResult { landed: carried, failed };
                }
            };
            let (fresh, stale) = split_live_bins(bins, |b| &b.target_paths, &live);
            // SELF-LANDED SPLIT — do not remove. A bin is "stale" because its
            // target files left the snapshot, and the normal cause is a
            // concurrent rewrite, whose staged parquet nothing references (safe
            // to delete). But OUR OWN previous attempt landing is
            // indistinguishable by targets alone: a commit that landed and then
            // reported an error (post-commit hook, or an outer bound firing)
            // takes the same shape — targets gone, and this retry would DELETE
            // the very files the landed commit now references (dangling Adds,
            // the 2026-07-09 incident shape).
            //
            // The Adds settle it: staged parquet is uuid-named by the writer, so
            // nobody else can produce those paths. Present in the snapshot ⇒ our
            // commit landed.
            let (self_landed, stale): (Vec<StagedBin>, Vec<StagedBin>) = stale.into_iter().partition(|b| bin_adds_live(b, &live));
            for bin in &stale {
                debug!(table_name, project_id = %bin.project_id, engine, event = "wave_bin_stale_at_commit");
            }
            self.discard_bins(&stale).await;
            failed.extend(stale);
            if !self_landed.is_empty() {
                warn!(
                    table_name,
                    engine,
                    bins = self_landed.len(),
                    attempt = attempt + 1,
                    event = "wave_bin_self_landed",
                    "a previous attempt's commit LANDED despite erroring — crediting its bins instead of deleting their (now live) files"
                );
                self.clear_bin_intents(&self_landed);
                Self::record_wave_landed(&self_landed, data_change);
                carried.extend(self_landed);
            }
            // Two dirty 10-minute bins can live in the same compacted parquet
            // file. Each staged unit is a full-file replacement, so committing
            // both units would remove the file twice and add two copies of all
            // its rows. Land only a target-disjoint subset per wave; failed
            // units are requeued and will re-plan from the replacement file on
            // the next tick. This is also required for Delta action validity.
            let mut claimed_targets = std::collections::HashSet::new();
            let (fresh, overlapping): (Vec<_>, Vec<_>) = fresh.into_iter().partition(|bin| {
                if bin.target_paths.iter().any(|path| claimed_targets.contains(path)) {
                    false
                } else {
                    claimed_targets.extend(bin.target_paths.iter().cloned());
                    true
                }
            });
            for bin in &overlapping {
                debug!(table_name, project_id = %bin.project_id, engine, event = "wave_bin_overlapping_target");
            }
            self.discard_bins(&overlapping).await;
            failed.extend(overlapping);
            if fresh.is_empty() {
                drop(commit_guard);
                return WaveResult { landed: carried, failed };
            }
            let actions: Vec<Action> = fresh.iter().flat_map(|b| b.removes.iter().chain(b.adds.iter()).cloned()).collect();
            let pre_uris: Option<std::collections::HashSet<String>> = track_files.then(|| scoped_file_uris(&new_table, &markers).into_iter().collect());
            let partitions = get_schema(table_name).unwrap_or_else(get_default_schema).partitions.clone();
            let op = wave_operation(data_change, self.config.maintenance.timefusion_light_optimize_target_size, (!partitions.is_empty()).then_some(partitions));
            let snapshot_ref = match new_table.snapshot() {
                Ok(s) => s as &dyn TableReference,
                Err(_) => {
                    drop(commit_guard);
                    failed.extend(fresh);
                    return WaveResult { landed: carried, failed };
                }
            };
            // Bounded: the proven prod hang (2026-07-30) was HERE — one R2
            // request pinned this lock and every committer on the table stalled.
            let commit_res = bounded_commit_await(
                COMMIT_LOCK_OP_TIMEOUT,
                "wave_commit",
                table_name,
                deltalake::kernel::transaction::CommitBuilder::from(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                    .with_actions(actions)
                    .build(Some(snapshot_ref), new_table.log_store(), op),
            )
            .await;
            match commit_res {
                Ok(finalized) => {
                    new_table.state = Some(finalized.snapshot());
                    // Release before post-commit work (swap + cache warm) —
                    // holding it would serialize ingest appends.
                    drop(commit_guard);
                    let bins_committed = fresh.len();
                    self.clear_bin_intents(&fresh);
                    info!(table_name, engine, round, bins = bins_committed, attempt = attempt + 1, event = "wave_committed");
                    // WARM BEFORE EVICT: a wave swaps K bins at once, so
                    // evicting first would cold-start the hottest query window
                    // every wave (the 2026-07-21 cache-thrash lesson).
                    self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &markers).await;
                    Self::record_wave_landed(&fresh, data_change);
                    return WaveResult { landed: concat_landed(carried, fresh), failed };
                }
                Err(CommitFailure { message: e, timed_out }) => {
                    // Released BEFORE the probe: on a timeout the store is
                    // already slow, and the probe is another log read — holding
                    // the lock across it would re-create the very stall this
                    // bound exists to end.
                    drop(commit_guard);
                    let occ = !timed_out && is_occ_conflict_err(&e);
                    if occ {
                        crate::metrics::record_optimize_conflict();
                    }
                    if occ && attempt + 1 < MAX_RETRIES {
                        debug!("{engine} wave OCC conflict (attempt {}/{}) table={}", attempt + 1, MAX_RETRIES, table_name);
                        tokio::time::sleep(occ_backoff(attempt)).await;
                        bins = fresh; // re-verify liveness against the newer snapshot
                        continue;
                    }
                    // Terminal: probe before deleting the NEW files. A
                    // landed-but-hook-failed commit already Removed the OLD
                    // files, so the new files are the only live copy.
                    let all_adds: Vec<Action> = fresh.iter().flat_map(|b| b.adds.iter().cloned()).collect();
                    match probe_after_timeout(self.probe_commit_landed_bounded(table_ref, &all_adds).await, timed_out) {
                        CommitProbe::Landed => {
                            warn!("{engine} wave for '{}' reported an error but LANDED (post-commit hook failed): {}", table_name, e);
                            let post = { table_ref.read().await.clone() };
                            self.swap_and_refresh_cache(table_ref, post, pre_uris.as_ref(), &markers).await;
                            self.clear_bin_intents(&fresh);
                            Self::record_wave_landed(&fresh, data_change);
                            return WaveResult { landed: concat_landed(carried, fresh), failed };
                        }
                        CommitProbe::NotLanded => {
                            crate::metrics::record_optimize_failed();
                            error!("{engine} wave commit failed for '{}': {}", table_name, e);
                            self.discard_bins(&fresh).await;
                            failed.extend(fresh);
                            return WaveResult { landed: carried, failed };
                        }
                        CommitProbe::Inconclusive => {
                            // Staged files stay in place (they may be the only
                            // live copy) — the units still count as failed, so a
                            // dedup unit's dirty bin is requeued.
                            //
                            // CONVERGENCE (both for an errored and a TIMED-OUT
                            // commit): the next wave's first act under the lock
                            // is `refresh_table_snapshot`, which re-reads the
                            // Delta log — so a commit that landed while we were
                            // not looking is observed there. Its Adds are then
                            // live, its Removes applied, and this wave's targets
                            // are gone from the snapshot, so the re-staged bins
                            // fail the liveness check and drop out instead of
                            // double-applying. If it truly did not land, the
                            // targets are still live and the bin is simply
                            // re-staged. Either way the only cost of an
                            // unconfirmed landing is a leaked staged file, which
                            // the boot-time staged-intent reconcile reclaims.
                            warn!("{engine} wave for '{}' errored, landing UNCONFIRMED — leaving new files in place: {}", table_name, e);
                            failed.extend(fresh);
                            return WaveResult { landed: carried, failed };
                        }
                    }
                }
            }
        }
        WaveResult { landed: carried, failed }
    }

    /// Per-engine counters for a landed wave. Dedup's dropped-row accounting is
    /// reported HERE and nowhere else: staging knows `before`/`after` long before
    /// the transaction exists, and a unit that loses the liveness check or the
    /// commit dropped exactly zero rows from the table.
    fn record_wave_landed(landed: &[StagedBin], data_change: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = crate::metrics::maintenance_stats();
        if data_change {
            for dropped in landed.iter().filter_map(|b| b.dedup.as_ref()).map(DedupUnit::dropped).filter(|d| *d > 0) {
                crate::metrics::record_compaction_dedup_dropped(dropped);
            }
            // Dedup waves count under their own counters — crediting them to
            // light_optimize_* made the stats under-report committed waves
            // (2026-07-30: 3 wave_committed log events, counter said 1).
            stats.dedup_bins_committed.fetch_add(landed.len() as u64, Relaxed);
            stats.dedup_waves_committed.fetch_add(1, Relaxed);
        } else {
            stats.light_optimize_bins_committed.fetch_add(landed.len() as u64, Relaxed);
            stats.light_optimize_waves_committed.fetch_add(1, Relaxed);
        }
    }

    /// Cleanup + intent-clear for bins leaving the wave uncommitted. One helper
    /// because the pair IS the crash-safety invariant — a drifted copy that
    /// cleans without clearing (or vice versa) breaks the manifest's meaning.
    async fn discard_bins(&self, bins: &[StagedBin]) {
        for bin in bins {
            Self::cleanup_orphaned_parquet(&bin.stage_store, &bin.adds).await;
        }
        self.clear_bin_intents(bins);
    }

    fn clear_bin_intents(&self, bins: &[StagedBin]) {
        self.clear_staged_intent(&bins.iter().map(|b| b.wave_id.as_str()).collect::<Vec<_>>());
    }

    /// Local-disk record of staged-but-uncommitted parquet, so a crash between
    /// staging and the wave commit doesn't leave objects in R2 that nothing
    /// references and nothing knows to delete. Lives on the WAL volume (same
    /// disk, already durable-by-design for exactly this class of intent).
    ///
    /// Written AFTER the writer flush rather than before the first write: the
    /// delta-rs `RecordBatchWriter` mints the object names itself, so the
    /// "intended paths" are not knowable earlier. That leaves only a
    /// crash-mid-PUT window uncovered (a partial object under a name nobody
    /// ever learned) — VACUUM's job, as before. The window this closes is the
    /// long one: staging → wave commit, which spans the whole wave.
    fn staged_intent_path(&self) -> PathBuf {
        let wal_dir = self.config.core.wal_dir();
        wal_dir.parent().map(|p| p.to_path_buf()).unwrap_or(wal_dir).join("staged_intent.jsonl")
    }

    /// Append one bin's staged paths. Best-effort: a manifest write failure
    /// must never fail the compaction, only widen the VACUUM backstop's job.
    fn record_staged_intent(&self, entry: &StagedIntent) {
        use std::io::Write;
        let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self.staged_intent_path();
        let write = (|| -> std::io::Result<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut file = std::fs::OpenOptions::new().create(true).append(true).open(&path)?;
            writeln!(file, "{}", serde_json::to_string(entry)?)
        })();
        if let Err(e) = write {
            warn!("staged-intent manifest append failed ({:?}): {} — orphan cleanup falls back to VACUUM", path, e);
        }
    }

    /// Drop one wave's entries, rewrite-compacting the append-only file. Called
    /// after the wave commits or after its staged parquet is cleaned up, i.e.
    /// once the entry can no longer describe an orphan.
    fn clear_staged_intent(&self, wave_ids: &[&str]) {
        let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let path = self.staged_intent_path();
        let Ok(contents) = std::fs::read_to_string(&path) else { return };
        let kept: Vec<String> = parse_staged_intents(&contents)
            .into_iter()
            .filter(|e| !wave_ids.contains(&e.wave_id.as_str()))
            .filter_map(|e| serde_json::to_string(&e).ok())
            .collect();
        let write = if kept.is_empty() { std::fs::write(&path, b"") } else { std::fs::write(&path, kept.join("\n") + "\n") };
        if let Err(e) = write {
            warn!("staged-intent manifest compaction failed ({:?}): {}", path, e);
        }
    }

    /// Boot-time orphan sweep: delete staged parquet the Delta log doesn't
    /// reference, BY KEY (no LIST — R2 listing is a known incident source).
    /// Every failure mode degrades to a `warn!` and a no-op: the manifest is a
    /// cleanup aid, correctness never depends on it.
    async fn reconcile_staged_intents(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        use object_store::ObjectStoreExt;
        let path = self.staged_intent_path();
        let contents = {
            let _manifest_guard = self.staged_intent_manifest_lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            let Ok(contents) = std::fs::read_to_string(&path) else { return };
            contents
        };
        let entries = parse_staged_intents(&contents);
        if entries.is_empty() {
            return;
        }
        let (referenced, store) = {
            let table = table_ref.read().await;
            let Ok(snapshot) = table.snapshot() else {
                warn!("staged-intent reconcile skipped for '{table_name}': no snapshot loaded");
                return;
            };
            (snapshot.log_data().iter().map(|f| f.path().into_owned()).collect::<std::collections::HashSet<String>>(), table.log_store().object_store(None))
        };
        let now_secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
        let orphans = staged_orphan_deletions(&entries, table_name, now_secs, &referenced);
        // Deletes are independent single-key calls — run them concurrently so a
        // crash that left many staged bins doesn't serialize N R2 round-trips
        // in front of maintenance startup.
        let orphan_count = orphans.len();
        let deleted = futures::stream::iter(orphans)
            .map(|orphan| {
                let store = &store;
                async move {
                    match store.delete(&object_store::path::Path::from(orphan.as_str())).await {
                        // NotFound = already gone (cleanup ran, or the crash preceded the PUT).
                        Ok(()) | Err(object_store::Error::NotFound { .. }) => 1usize,
                        Err(e) => {
                            warn!("staged-intent reconcile: delete failed for {}: {}", orphan, e);
                            0
                        }
                    }
                }
            })
            .buffer_unordered(8)
            .fold(0usize, |acc, n| async move { acc + n })
            .await;
        info!(table_name, entries = entries.len(), orphans = orphan_count, deleted, event = "staged_intent_reconciled");
        // Clear ONLY the entries this reconcile actually judged: this table's,
        // old enough to be unambiguous. Other tables' entries (and young ones)
        // stay for their own reconcile pass.
        let ids: Vec<&str> = entries
            .iter()
            .filter(|e| e.table_name == table_name && now_secs.saturating_sub(e.recorded_at) >= STAGED_INTENT_MIN_AGE_SECS)
            .map(|e| e.wave_id.as_str())
            .collect();
        self.clear_staged_intent(&ids);
    }

    /// Checkpoint after a tick's waves when the log has advanced enough since the
    /// last checkpoint. Owned by the wave engine rather than left to the commit
    /// count: waves cut compaction commits ~40x, so a per-N-versions cadence
    /// would checkpoint ~40x less often exactly where the replay tail is the top
    /// CPU cost (34.5% of process CPU in log replay, prod profile 2026-07-29).
    async fn checkpoint_after_waves(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        /// Small on purpose: a tick's waves add ~2-3 versions, so this
        /// checkpoints every few ticks instead of every tick.
        const WAVE_CHECKPOINT_VERSIONS: u64 = 20;
        let (url, version) = {
            let g = table_ref.read().await;
            (g.table_url().to_string(), g.version().unwrap_or(0))
        };
        let last = self.checkpoint_versions.get(&url).map(|e| *e).unwrap_or(0);
        if version.saturating_sub(last) >= WAVE_CHECKPOINT_VERSIONS {
            self.checkpoint_and_cleanup_table(table_ref, table_name).await;
        }
    }

    /// One-way safety brakes, checked at WAVE BOUNDARIES only (in-flight bins
    /// always finish and commit). Both convert an overload into a smaller tick
    /// rather than an incident: durability outranks compaction, and an OOM-kill
    /// (exit 137) means WAL recovery and quarantine — our known silent-loss
    /// sink. Never sizes concurrency upward; strictly "start no more work".
    ///
    /// Two levels, because the failure modes differ. WAL backlog can be a
    /// sustained property of a busy hour (and of every post-boot replay), so
    /// it DEGRADES to a service floor — serial, one bin in flight, still
    /// bounded by the tick deadline — a full stop there starves compaction
    /// indefinitely, which degrades reads and merges. Memory near the cgroup
    /// limit is an imminent OOM: hard STOP.
    fn light_optimize_brake(&self) -> Option<Brake> {
        use std::sync::atomic::Ordering::Relaxed;
        if let Some(stale_buckets) = self.buffered_layer().map(|layer| layer.stale_unflushed_bucket_count()).filter(|count| *count > 0) {
            info!(stale_buckets, event = "light_optimize_flush_debt_yield");
            crate::metrics::maintenance_stats().light_optimize_flush_debt_yields.fetch_add(1, Relaxed);
            return Some(Brake::Stop("stale_unflushed_buckets"));
        }
        if self.buffered_layer().is_some_and(|layer| layer.is_wal_backlog_over_threshold()) {
            info!(event = "light_optimize_wal_yield");
            crate::metrics::maintenance_stats().light_optimize_wal_yields.fetch_add(1, Relaxed);
            return Some(Brake::Degrade("wal_backlog_over_threshold"));
        }
        // HOST pressure, not just our cgroup: on an over-committed host the
        // kernel's global OOM killer fires long before our 120GiB memcg limit
        // (2026-07-30 10:57: TF killed at 91.5GB anon by a GLOBAL oom while
        // the cgroup brake read healthy). /proc/meminfo is the host's inside
        // a container, so MemAvailable is exactly the number the global OOM
        // killer is racing against.
        const HOST_MEM_BRAKE_FLOOR_BYTES: u64 = 12 * 1024 * 1024 * 1024;
        if host_mem_available_bytes().is_some_and(|avail| avail < HOST_MEM_BRAKE_FLOOR_BYTES) {
            info!(event = "light_optimize_host_memory_brake");
            crate::metrics::maintenance_stats().light_optimize_memory_brakes.fetch_add(1, Relaxed);
            return Some(Brake::Stop("host_memory_low"));
        }
        let limit = self.config.derived.memory_brake_limit_bytes();
        if limit > 0 && process_memory_bytes().is_some_and(|used| used > limit) {
            info!(limit, event = "light_optimize_memory_brake");
            crate::metrics::maintenance_stats().light_optimize_memory_brakes.fetch_add(1, Relaxed);
            return Some(Brake::Stop("memory_brake"));
        }
        None
    }

    /// Wall-clock budget for one light-optimize tick. Bounds the round-robin so a
    /// backlog can't run past its own cron period and stack ticks (prod 2026-07-29:
    /// "still in progress after 600s" on a 300s schedule).
    fn light_optimize_tick_budget(&self) -> std::time::Duration {
        let period = cron_period(&self.config.maintenance.timefusion_light_optimize_schedule);
        self.config.derived.tick_budget(period)
    }

    /// Inner optimize loop for the COLD consolidate path (the 5-min hot tail
    /// moved to `stage_hot_bin`/`commit_hot_wave`). Caller is expected to hold the flush lock when
    /// a `BufferedWriteLayer` is active; the retry loop here remains as a
    /// safety net against bursts from `flush_all_now` or shutdown flushes.
    #[allow(clippy::too_many_arguments)]
    async fn optimize_table_light_inner(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, today: chrono::NaiveDate, project_id: &str, partition_filters: &[PartitionFilter],
        selected_files: &[String], target_size: i64, writer_properties: &WriterProperties, optimize_type: deltalake::operations::optimize::OptimizeType,
        min_files: usize, start_time: std::time::Instant,
    ) -> Result<()> {
        const MAX_RETRIES: usize = 4;
        // Optimize rewrites (compaction) materialize Arrow like dedup — hold a
        // maintenance-rewrite permit so it can't stack with a concurrent dedup
        // or recompress and blow the cgroup (their footprint is pool-invisible).
        let _rewrite_permit = self.maintenance_rewrite_sem.acquire().await.map_err(|e| anyhow::anyhow!("maintenance rewrite semaphore closed: {e}"))?;
        let mut last_err: Option<deltalake::DeltaTableError> = None;
        // Pre-state file set for deriving the files this optimize adds (to warm)
        // and removes (to evict). Hoisted out of the retry loop — only a
        // successful commit (which returns) changes the file set — and scoped to
        // the one hot `(project_id, today)` partition this optimize is filtered
        // to. Sole remaining caller is the nightly consolidate sweep (up to
        // 128 passes x 4 attempts per sealed date) — still worth scoping: it
        // once walked the whole 26k-file table each time.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let (pid_marker, date_marker) = (format!("project_id={project_id}/"), format!("date={today}/"));
        let scope = [pid_marker.as_str(), date_marker.as_str()];
        let pre_uris: Option<std::collections::HashSet<String>> =
            if track_files { Some(scoped_file_uris(&*table_ref.read().await, &scope).into_iter().collect()) } else { None };
        for attempt in 0..MAX_RETRIES {
            let table_clone = {
                let table = table_ref.read().await;
                table.clone()
            };
            if attempt == 0 {
                info!(table_name, project_id, date = %today, target_size, max_concurrent_tasks = self.config.derived.optimize_merge_tasks(), event = "light_optimize_started");
            } else {
                debug!("Light optimize retry {}/{} after OCC conflict", attempt + 1, MAX_RETRIES);
            }
            let optimize_result = table_clone
                .optimize()
                .with_filters(partition_filters)
                // Restrict the rewrite to the pre-selected sealed files so live
                // appends after selection aren't in the commit's file set (avoids
                // the OCC race on the hot today-partition).
                .with_binned_files(selected_files)
                // Cloned per attempt: the retry loop re-submits after OCC conflicts.
                .with_type(optimize_type.clone())
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                .with_max_files_per_bin(self.config.derived.optimize_max_files_per_bin())
                .with_max_concurrent_tasks(self.config.derived.optimize_merge_tasks())
                .with_writer_properties(writer_properties.clone())
                .with_min_commit_interval(tokio::time::Duration::from_secs(30))
                // Apply the compaction's Add+Remove to the materialized snapshot
                // incrementally rather than re-materializing all active files in
                // the post-commit hook (see the dedup path).
                .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                // Variant columns are stored as Struct{Binary, Binary} on disk; if
                // the optimize-internal Parquet read uses `schema_force_view_types=true`
                // (delta-rs's default), it returns BinaryView and the rewrite blows up
                // mid-scan with "Expected ... Binary, got ... BinaryView".
                .with_session_state(Arc::new(self.light_optimize_session_state()))
                .await;
            match optimize_result {
                Ok((new_table, metrics)) => {
                    if metrics.total_considered_files < min_files {
                        debug!(
                            "Skipping light optimization commit for table={} project={} date={}: {} files < min threshold {}",
                            table_name, project_id, today, metrics.total_considered_files, min_files
                        );
                        return Ok(());
                    }
                    let duration = start_time.elapsed();
                    info!(
                        "Light optimization completed for table={} project={} date={} in {:?} (attempt {}): {} files considered, {} removed, {} added",
                        table_name,
                        project_id,
                        today,
                        duration,
                        attempt + 1,
                        metrics.total_considered_files,
                        metrics.num_files_removed,
                        metrics.num_files_added
                    );
                    // Swap the optimized table in and refresh the cache (warm
                    // freshly-compacted files, evict the small files just
                    // tombstoned) via the shared helper.
                    self.swap_and_refresh_cache(table_ref, new_table, pre_uris.as_ref(), &scope).await;
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_conflict = is_occ_conflict_err(&msg);
                    if is_conflict {
                        crate::metrics::record_optimize_conflict();
                    }
                    // "Found unmasked nulls for non-nullable StructArray" surfaces
                    // when delta-rs is mid-rewrite and the in-flight Add log lines
                    // for partition struct values aren't fully populated yet.
                    // It usually clears on a fresh re-scan, so treat as transient.
                    let is_transient_schema = msg.contains("Found unmasked nulls");
                    if (is_conflict || is_transient_schema) && attempt + 1 < MAX_RETRIES {
                        tokio::time::sleep(occ_backoff(attempt)).await;
                        last_err = Some(e);
                        continue;
                    }
                    crate::metrics::record_optimize_failed();
                    error!(
                        "Light optimization operation failed for table={} project={} date={} (attempt {}): {}",
                        table_name,
                        project_id,
                        today,
                        attempt + 1,
                        e
                    );
                    return Err(anyhow::anyhow!("Light table optimization failed: {}", e));
                }
            }
        }
        let err = last_err.map(|e| e.to_string()).unwrap_or_else(|| "exhausted retries".into());
        warn!(
            "Light optimization gave up for table={} project={} date={} after {} OCC conflicts; will retry next tick: {}",
            table_name, project_id, today, MAX_RETRIES, err
        );
        Ok(())
    }

    /// Vacuum the Delta table to clean up old files that are no longer needed
    /// This reduces storage costs and improves query performance
    /// On-demand vacuum of a single unified table (pgwire `VACUUM <table>`).
    /// `retention_hours = None` uses the configured default. Mirrors
    /// `compact_date`: resolves the table then delegates, keeping config private.
    pub async fn vacuum_named(&self, table_name: &str, retention_hours: Option<u64>) -> Result<usize> {
        let retention = retention_hours.unwrap_or(self.config.maintenance.timefusion_vacuum_retention_hours);
        let table_ref = self.get_or_create_unified_table(table_name).await?;
        Ok(self.vacuum_table("", table_name, &table_ref, retention).await)
    }

    /// Returns the number of files deleted (0 on failure — the error is logged).
    async fn vacuum_table(&self, project_id: &str, table_name: &str, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) -> usize {
        // Log the start of the vacuum operation
        let start_time = std::time::Instant::now();
        info!("Starting vacuum operation with retention period of {} hours", retention_hours);

        // Full vacuum lists unreferenced parquet as well as retained Remove
        // actions. Serialize that classification with every local writer and
        // refresh inside the critical section: cloning before the commit lock
        // lets a concurrent flush land a file that Full vacuum can mistake for
        // an orphan. The table RwLock alone is insufficient because commit
        // paths deliberately clone-update-swap without holding it across IO.
        let commit_lock = self.commit_lock(project_id, table_name).await;
        let _commit_guard = commit_lock.lock().await;
        if let Err(e) = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
            error!("Vacuum aborted: failed to refresh '{}' before Full orphan sweep: {}", Self::table_label(project_id, table_name), e);
            return 0;
        }

        // Get a clone so the table RwLock is not held across object-store IO.
        // The per-physical-table commit lock above keeps this snapshot stable.
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Directly run vacuum without dry run to delete old files
        match table_clone
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            // Full also sweeps orphaned parquet whose tombstones have already
            // left the retained log. Keep this mode: bounding the transaction
            // log must not turn old orphan files into a permanent storage leak.
            .with_mode(deltalake::operations::vacuum::VacuumMode::Full)
            .await
        {
            Ok((_, metrics)) => {
                let duration = start_time.elapsed();
                let files_deleted = metrics.files_deleted.len();
                info!("Vacuum completed in {:?}, deleted {} files", duration, files_deleted);

                // Log file sizes for monitoring storage savings
                if !metrics.files_deleted.is_empty() {
                    let _total_size: u64 = metrics
                        .files_deleted
                        .iter()
                        .filter_map(|_path| {
                            // Extract size from path if available
                            // This is a simplified approach - in production you might want to query actual file sizes
                            None::<u64>
                        })
                        .sum();
                    debug!("Vacuum operation details: {:?}", metrics.files_deleted);
                }

                // Update the table state after vacuum
                if refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await.is_ok() {
                    info!("Table state updated after vacuum");
                } else {
                    error!("Failed to update table state after vacuum");
                }
                files_deleted
            }
            Err(e) => {
                error!("Vacuum operation failed: {}", e);
                0
            }
        }
    }

    /// Out-of-band checkpoint + expired-log cleanup for one table. Runs on the
    /// maintenance schedule instead of in the delta-rs commit hook
    /// (`base_commit_properties` disables the hook) so an R2 500 on the
    /// checkpoint PUT or the bulk log `?delete` can never fail a landed commit
    /// (2026-07-09 incident). Best-effort: any error is logged + counted and
    /// retried next tick; ingest is never touched. Checkpoints only when the
    /// version advanced by ≥ `checkpoint_interval` since the last checkpoint
    /// (tracked in-memory per table URL), so idle tables are skipped.
    async fn checkpoint_and_cleanup_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        use std::sync::atomic::Ordering::Relaxed;
        // Checkpoint the latest committed version, not a stale clone.
        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let (table, url, version) = {
            let g = table_ref.read().await;
            (g.clone(), g.table_url().to_string(), g.version().unwrap_or(0))
        };
        let interval = self.config.parquet.timefusion_checkpoint_interval.max(1);
        let lag = version.saturating_sub(self.checkpoint_versions.get(&url).map(|e| *e).unwrap_or(0));
        // Gauge: max lag seen this tick (job resets to 0 first). A large, growing
        // value means the checkpoint task is failing or wedged.
        crate::metrics::maintenance_stats().checkpoint_lag_versions.fetch_max(lag, Relaxed);
        if lag < interval {
            return;
        }
        // Each store-heavy op is individually bounded so one wedged R2 call
        // can't starve the rest of the sweep (and each timeout lands in the
        // right failure counter). 600s is ~35x the largest observed catch-up
        // (a 179k-version lag checkpointed in 17s, 2026-07-14); hitting it
        // means a stuck backend, not a big table. Dropping the future
        // mid-checkpoint is safe: the checkpoint PUT is atomic and retried
        // next tick.
        const CHECKPOINT_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);
        match tokio::time::timeout(CHECKPOINT_OP_TIMEOUT, deltalake::checkpoints::create_checkpoint(&table, None)).await {
            Ok(Ok(())) => {
                // Verify the just-written checkpoint is a readable Parquet before
                // advancing the boundary or letting cleanup prune JSON behind it.
                // A foreign/corrupt checkpoint object (an S3 error/Select body
                // written over it, 2026-07-17) must never gate log cleanup — the
                // JSON commit log is the only recovery source, and today's
                // recovery depended on it still being present.
                let store = table.log_store().object_store(None);
                match last_checkpoint_readable(&store).await {
                    Ok(true) => {
                        self.checkpoint_versions.insert(url, version);
                        crate::metrics::maintenance_stats().checkpoints_created.fetch_add(1, Relaxed);
                        debug!("out-of-band checkpoint created + verified for '{}' at v{}", table_name, version);
                    }
                    Ok(false) => {
                        crate::metrics::record_checkpoint_corrupt();
                        error!(
                            "checkpoint for '{}' at v{} is unreadable after write (foreign/corrupt object) — withholding log cleanup to preserve the JSON recovery log; PAGE",
                            table_name, version
                        );
                        return;
                    }
                    Err(e) => {
                        crate::metrics::record_checkpoint_failed();
                        warn!("could not verify checkpoint for '{}' at v{}: {} — withholding log cleanup this tick", table_name, version, e);
                        return;
                    }
                }
            }
            Ok(Err(e)) => {
                crate::metrics::record_checkpoint_failed();
                warn!("out-of-band checkpoint failed for '{}' at v{}: {} (retry next tick)", table_name, version, e);
                return; // no fresh checkpoint boundary → skip cleanup this tick
            }
            Err(_) => {
                crate::metrics::record_checkpoint_failed();
                warn!("out-of-band checkpoint for '{}' timed out after {CHECKPOINT_OP_TIMEOUT:?} (retry next tick)", table_name);
                return;
            }
        }
        // Log cleanup prunes only up to a checkpoint boundary, so run it after a
        // successful checkpoint. Uses the table's logRetentionDuration.
        match tokio::time::timeout(CHECKPOINT_OP_TIMEOUT, deltalake::checkpoints::cleanup_metadata(&table, None)).await {
            Ok(Ok(n)) if n > 0 => {
                crate::metrics::maintenance_stats().log_files_cleaned.fetch_add(n as u64, Relaxed);
                debug!("out-of-band log cleanup removed {} expired files for '{}'", n, table_name);
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                crate::metrics::record_log_cleanup_failed();
                warn!("out-of-band log cleanup failed for '{}': {} (retry next tick)", table_name, e);
            }
            Err(_) => {
                crate::metrics::record_log_cleanup_failed();
                warn!("out-of-band log cleanup for '{}' timed out after {CHECKPOINT_OP_TIMEOUT:?} (retry next tick)", table_name);
            }
        }
    }

    /// Reconcile a table's active Add entries against object-store truth and
    /// commit `Remove` actions for any whose parquet is missing. Repairs
    /// dangling Adds left by a commit-path parquet deletion (2026-07-09: an
    /// R2-500 post-commit-hook failure made the flush delete files the landed
    /// commit referenced). Those rows were re-flushed into fresh files, so the
    /// Remove is lossless — it just stops queries 404-ing on the dead paths. A
    /// nonzero removal count means committed data was destroyed elsewhere, so it
    /// is logged loudly + counted (PAGE-worthy). delta-rs `filesystem_check`
    /// does the list-and-diff; we only force hooks off and surface the count.
    async fn reconcile_dangling_adds(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) {
        let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let table = { table_ref.read().await.clone() };
        match table.filesystem_check().with_commit_properties(base_commit_properties()).await {
            Ok((_, metrics)) => {
                let n = metrics.files_removed.len();
                if n > 0 {
                    crate::metrics::record_dangling_removed(n as u64);
                    warn!(
                        "reconcile: '{}' had {} dangling Add(s) (committed parquet missing from store) — Remove'd: {:?}",
                        table_name, n, metrics.files_removed
                    );
                    let _ = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
                }
            }
            Err(e) => {
                crate::metrics::maintenance_stats().reconcile_failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                warn!("reconcile filesystem_check failed for '{}': {} (retry next tick)", table_name, e);
            }
        }
    }

    /// One out-of-band checkpoint + log-cleanup tick across every registered
    /// table. Driven by the checkpoint cron job (and directly by tests).
    pub async fn run_checkpoint_maintenance(&self) {
        // Reset the lag gauge so it reflects THIS tick's worst table.
        crate::metrics::maintenance_stats().checkpoint_lag_versions.store(0, std::sync::atomic::Ordering::Relaxed);
        for (_project_id, name, table) in self.all_tables().await {
            self.checkpoint_and_cleanup_table(&table, &name).await;
        }
    }

    /// One dangling-Add reconcile tick across every registered table. Driven by
    /// the reconcile cron job (and directly by tests).
    pub async fn run_reconcile_maintenance(&self) {
        for (_project_id, name, table) in self.all_tables().await {
            self.reconcile_dangling_adds(&table, &name).await;
        }
    }

    /// Test-only: run `probe_commit_landed` against the table's current active
    /// files. Returns true iff the probe reports `Landed` (every active file's
    /// object is present). Lets an e2e test exercise the landed-vs-not-landed
    /// decision deterministically against a real store, without fighting
    /// delta-rs's post-commit error timing.
    #[cfg(any(test, feature = "e2e"))]
    #[allow(deprecated)] // add_action() is deprecated but fine for a test-only probe
    pub async fn test_probe_landed(&self, project_id: &str, table_name: &str) -> Result<bool> {
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let adds: Vec<deltalake::kernel::Action> = {
            let guard = table_ref.read().await;
            guard.snapshot()?.log_data().iter().map(|f| deltalake::kernel::Action::Add(f.add_action())).collect()
        };
        Ok(matches!(self.probe_commit_landed(&table_ref, &adds).await, CommitProbe::Landed))
    }

    /// Test-only: probe with a fabricated Add whose path was never committed.
    /// The probe must report NOT landed (the "commit didn't write our adds to
    /// the log" case that the flush error arm treats as safe-to-clean-up).
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_probe_bogus_not_landed(&self, project_id: &str, table_name: &str) -> Result<bool> {
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let bogus = deltalake::kernel::Action::Add(deltalake::kernel::Add {
            path: "project_id=nope/date=1970-01-01/part-never-committed.parquet".to_string(),
            partition_values: std::collections::HashMap::new(),
            size: 1,
            modification_time: 0,
            data_change: true,
            stats: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        });
        Ok(matches!(self.probe_commit_landed(&table_ref, &[bogus]).await, CommitProbe::NotLanded))
    }

    /// Test-only: number of `.checkpoint.parquet` objects in the table's
    /// `_delta_log`. Lets a test assert the commit path does NOT checkpoint
    /// (Phase 1) and that the out-of-band task DOES (Phase 2).
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_checkpoint_file_count(&self, project_id: &str, table_name: &str) -> Result<usize> {
        use futures::StreamExt;
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let store = { table_ref.read().await.log_store().object_store(None) };
        let prefix = object_store::path::Path::from("_delta_log");
        let mut n = 0;
        let mut stream = store.list(Some(&prefix));
        while let Some(item) = stream.next().await {
            if item?.location.as_ref().contains(".checkpoint.parquet") {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Test-only: delete the first active parquet object of a table directly
    /// from the store (no Delta commit), reproducing the commit-path deletion
    /// bug so a test can then assert `reconcile_dangling_adds` heals the dangling
    /// Add. Returns the deleted relative path.
    #[cfg(any(test, feature = "e2e"))]
    pub async fn test_delete_first_active_file(&self, project_id: &str, table_name: &str) -> Result<String> {
        use object_store::ObjectStoreExt;
        let table_ref = self.get_or_create_table(project_id, table_name).await?;
        let guard = table_ref.read().await;
        let snap = guard.snapshot()?;
        let path = snap.log_data().iter().next().map(|f| f.path().into_owned()).ok_or_else(|| anyhow::anyhow!("no active files to delete"))?;
        guard.log_store().object_store(None).delete(&object_store::path::Path::from(path.as_str())).await?;
        Ok(path)
    }

    /// Flatten unified + custom project tables into one (project_id, name, handle)
    /// list — `project_id` empty for unified tables (shared by all default
    /// projects). A SNAPSHOT by design: every maintenance pass must iterate this
    /// instead of `MAP.read().await.iter()`, because holding a table-map read
    /// guard across the pass's awaits lets one queued writer (a first-touch table
    /// load) block every subsequent reader — tokio's RwLock is write-preferring —
    /// and wedge all maintenance jobs at once (2026-07-29 22:05).
    async fn all_tables(&self) -> Vec<(String, String, Arc<RwLock<DeltaTable>>)> {
        let mut out: Vec<(String, String, Arc<RwLock<DeltaTable>>)> =
            self.unified_tables.read().await.iter().map(|(n, t)| (String::new(), n.clone(), t.clone())).collect();
        out.extend(self.custom_project_tables.read().await.iter().map(|((p, n), t)| (p.clone(), n.clone(), t.clone())));
        out
    }

    /// Human label for a table from `all_tables`, matching the pre-existing
    /// per-job log wording so operator greps keep working.
    fn table_label(project_id: &str, table_name: &str) -> String {
        if project_id.is_empty() { format!("unified table '{table_name}'") } else { format!("custom project '{project_id}' table '{table_name}'") }
    }

    /// Get table statistics using the statistics extractor
    pub async fn get_table_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        self.statistics_extractor.extract_statistics(table, project_id, table_name).await
    }

    /// Clear the statistics cache
    pub async fn clear_statistics_cache(&self) {
        self.statistics_extractor.clear_cache().await
    }

    /// Foyer cache handle (None if Foyer disabled). Test hook for harnesses
    /// that want hit/miss assertions; also used by the warm-cache path.
    pub fn object_store_cache(&self) -> Option<&Arc<SharedFoyerCache>> {
        self.object_store_cache.as_ref()
    }

    /// Invalidate statistics for a specific table
    pub async fn invalidate_table_statistics(&self, project_id: &str, table_name: &str) {
        self.statistics_extractor.invalidate(project_id, table_name).await
    }

    /// Gracefully shutdown the database, including cache and maintenance tasks
    /// Signal maintenance/background tasks (scheduler, dedup sweep, coalescer)
    /// to stop. Idempotent; `shutdown()` also fires it. Called early in the
    /// drain so an in-flight sweep bails before the buffered-layer flush.
    pub fn cancel_maintenance(&self) {
        self.maintenance_shutdown.cancel();
    }

    /// True once maintenance/background tasks have been told to stop. Exposed
    /// for tests — a `true` here on a live instance means every cron job is
    /// dead (the 2026-07-14 outage signature).
    pub fn is_maintenance_cancelled(&self) -> bool {
        self.maintenance_shutdown.is_cancelled()
    }

    /// Clone for long-lived background tasks (cron loops, DML coalescer):
    /// omits the cancel guard so a task waiting on `maintenance_shutdown`
    /// doesn't keep its own kill-switch alive (guard-holding clone captured by
    /// the task → last-drop cancellation unreachable).
    fn background_clone(&self) -> Self {
        Self { _maintenance_cancel_guard: None, ..self.clone() }
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_by(tokio::time::Instant::now() + self.config.buffer.stop_grace()).await
    }

    /// Graceful shutdown; every phase that can block on a slow/stuck Delta or
    /// S3 backend — the DML-coalescer drain and the foyer `close()` (whose
    /// flush-on-close overran for minutes in prod, stalling `wal.lock`
    /// release, #82) — is bounded by `deadline`, the remainder of the
    /// process-wide stop grace shared with `BufferedWriteLayer::shutdown_by`.
    /// Un-drained deferred Delta legs are the coalescer's documented
    /// crash-equivalent loss (mem-leg values survive in the WAL); foyer close
    /// abandons only rebuildable cache warmth.
    pub async fn shutdown_by(&self, deadline: tokio::time::Instant) -> Result<()> {
        info!("Shutting down TimeFusion database...");

        // Flush deferred DML merges before anything is torn down. The drain
        // task also runs a final drain on cancellation, but doing it here
        // deterministically (drains are serialized + idempotent) means
        // shutdown doesn't race the task's select loop. Bounded by `deadline`:
        // an un-drained group's deferred Delta leg is the SAME accepted,
        // WAL-surfaced loss a crash incurs (dml_coalescer durability contract —
        // mem-leg rows are WAL-durable; only the Delta leg for rows already in
        // Delta is at risk). Better than overrunning the stop grace on a
        // slow/stuck Delta backend and being SIGKILLed mid-drain, which loses
        // the same legs AND stalls wal.lock release (issue #82).
        if let Some(coalescer) = self.dml_coalescer()
            && tokio::time::timeout_at(deadline, coalescer.drain(self)).await.is_err()
        {
            warn!("DML coalescer drain exceeded shutdown deadline — un-drained deferred Delta legs lost (crash-equivalent; mem-leg values survive in WAL)");
        }

        // Cancel maintenance tasks
        self.maintenance_shutdown.cancel();

        // Shutdown batch queue if present
        if let Some(ref queue) = self.batch_queue {
            info!("Flushing batch queue...");
            if tokio::time::timeout_at(deadline, queue.shutdown()).await.is_err() {
                warn!("Batch queue shutdown exceeded shutdown deadline — proceeding with process teardown");
            }
        }

        // Log final cache stats and shutdown cache
        if let Some(ref cache) = self.object_store_cache {
            info!("Shutting down Foyer cache...");
            cache.log_stats().await;
            cache.shutdown_by(deadline).await?;
        }

        // Close PostgreSQL connection pool if present
        if let Some(ref pool) = self.config_pool
            && tokio::time::timeout_at(deadline, pool.close()).await.is_err()
        {
            warn!("PostgreSQL pool close exceeded shutdown deadline — dropping connections on process exit");
        }

        info!("Database shutdown complete");
        Ok(())
    }
}

/// Build the shared query `RuntimeEnv`: the global memory pool plus the
/// decoded-parquet-metadata cache limit. The limit MUST be set on the builder
/// here — setting `datafusion.runtime.metadata_cache_limit` on the SessionConfig
/// does NOT reconfigure an already-built RuntimeEnv, so it silently falls back to
/// DataFusion's 50MB default and every scan re-decodes the parquet footer + page
/// index (measured ~900ms metadata_load_time per query on prod).
fn build_query_runtime_env(
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>, metadata_cache_bytes: usize,
) -> datafusion::execution::runtime_env::RuntimeEnv {
    datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_metadata_cache_limit(metadata_cache_bytes)
        .build()
        .expect("Failed to create runtime environment")
}

/// Sort `batches` by the table's declared `sorting_columns` and report whether
/// the result is actually in that order, as `(batches, sorted)`:
/// - `sorted == true`: rows are globally ordered by the sort keys, so the caller
///   may declare the parquet `SortingColumn` footer (`declare_sorted=true`).
/// - `sorted == false`: no sort keys present, OR the bucket mixed schemas
///   (nullable-field evolution within a 10-min window — `schemas_compatible` in
///   `mem_buffer` admits this) so `concat_batches` couldn't combine it. We then
///   write the rows unsorted rather than abort the flush (matching the old
///   `SchemaMode::Merge` write path), and the caller MUST pass
///   `declare_sorted=false` so the footer never claims an order we didn't write.
///
/// Footer honesty is tied to the returned bool — never assumed. A single batch
/// skips the concat copy; an already-ordered batch skips the `take` copy.
/// Number of md5-prefix buckets the sharded dedup rewrite hashes into — the first
/// digest byte, i.e. 2 hex chars. Shard count is clamped to this: shards partition
/// `[0, DEDUP_BUCKET_COUNT)` into contiguous ranges, so more shards than buckets
/// would leave rows uncovered. Doubles as a runaway-shard-count backstop.
const DEDUP_BUCKET_COUNT: u64 = 256;

/// Returns the hash-shard count needed to keep one dedup rewrite within either
/// configured byte budget. A zero budget disables that ceiling.
fn dedup_shard_count(decoded_bytes: u64, rewrite_bytes: u64, decoded_budget: u64, rewrite_budget: u64) -> u64 {
    let shards_for = |bytes: u64, budget: u64| if budget > 0 { bytes.div_ceil(budget) } else { 1 };
    shards_for(decoded_bytes, decoded_budget).max(shards_for(rewrite_bytes, rewrite_budget)).clamp(1, DEDUP_BUCKET_COUNT)
}

/// Two independent conservation checks required before a physical dedup unit
/// may create Remove actions. The first proves the full live contents of every
/// target file were re-read; the second proves winner selection emitted exactly
/// one row per logical key.
fn dedup_rewrite_counts_match(reread: u64, expected_live: u64, output: u64, expected_logical: u64) -> bool {
    reread == expected_live && output == expected_logical
}

/// Rows a bucket must exceed before the streaming merge is worth its setup
/// (row encoding + heap) over just sorting the single concatenated batch.
const MERGE_MIN_ROWS: usize = 4_096;

/// Bytes of one emitted merge chunk. The parquet writer coalesces chunks into
/// row groups itself (`set_max_row_group_bytes`), so this only bounds OUR
/// transient copy: peak = the (already resident) sorted runs + one chunk,
/// instead of the concat copy + the `take` copy the old path built.
const MERGE_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MERGE_CHUNK_ROWS_MIN: usize = 1_024;
const MERGE_CHUNK_ROWS_MAX: usize = 65_536;

/// Batches a flush/rewrite writes, either eagerly held or produced on demand by
/// the streaming sort-merge. Iterating yields writer-sized chunks; the merge
/// frees each sorted run (and its encoded keys) as it is drained, so the whole
/// bucket is never materialized twice.
enum FlushBatches {
    /// Nothing to merge: no sort keys, an unsortable/oversize bucket, or a
    /// single already-sorted batch. Yielded verbatim.
    Ready(std::vec::IntoIter<RecordBatch>),
    Merge(SortMergeStream),
}

impl FlushBatches {
    /// Schemas the output may carry — the caller checks these against the table
    /// schema to decide whether the write needs schema evolution. A merge
    /// unified its runs, so it has exactly one.
    fn schemas(&self) -> Vec<arrow_schema::SchemaRef> {
        match self {
            Self::Ready(it) => it.as_slice().iter().map(|b| b.schema()).collect(),
            Self::Merge(m) => vec![m.schema.clone()],
        }
    }
}

impl Iterator for FlushBatches {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Ready(it) => it.next().map(Ok),
            Self::Merge(m) => m.next_chunk(),
        }
    }
}

/// K-way merge of individually sorted runs, emitting chunk-sized batches.
///
/// Equivalence to the old concat + `lexsort_to_indices` + `take` path, pinned by
/// `streaming_merge_matches_reference_sort`: identical multiset of rows, and an
/// identical sequence of sort-key values (both are total orders by the same
/// keys, with the same `SortOptions` — `arrow-row` encoding orders identically
/// to the lexicographic comparator `lexsort_to_indices` uses).
///
/// Rows that tie on EVERY sort key may come out in a different relative order
/// than the old path did. That is not a regression to fix but an ambiguity the
/// old path never resolved: `arrow_ord::sort::lexsort_to_indices` is an
/// *unstable* sort (`sort_unstable_by`), so its tie order was already
/// arbitrary and input-layout dependent. The merge is strictly better defined —
/// ties break by run index, i.e. by input batch order — and nothing downstream
/// reads tie order: flush-time dedup already happened upstream
/// (`mem_buffer::dedup_batches`, which picks survivors by the schema tiebreak,
/// not by position), and the parquet footer only claims the key ordering.
struct SortMergeStream {
    schema: arrow_schema::SchemaRef,
    converter: arrow::row::RowConverter,
    /// Individually sorted runs, in input order. Drained runs are replaced by
    /// an empty batch so their payload frees mid-merge.
    runs: Vec<RecordBatch>,
    /// Encoded sort keys, aligned with `runs`; freed on run exhaustion.
    keys: Vec<arrow::row::Rows>,
    /// Next unconsumed row per run.
    pos: Vec<usize>,
    /// Min-heap of run indices ordered by (current key row, run index).
    heap: Vec<usize>,
    chunk_rows: usize,
}

/// `a`'s head sorts before `b`'s. Ties break by run index — this is what makes
/// the merge stable w.r.t. the concatenation order.
fn head_less(a: usize, b: usize, keys: &[arrow::row::Rows], pos: &[usize]) -> bool {
    match keys[a].row(pos[a]).cmp(&keys[b].row(pos[b])) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Greater => false,
        std::cmp::Ordering::Equal => a < b,
    }
}

fn sift_up(heap: &mut [usize], mut i: usize, keys: &[arrow::row::Rows], pos: &[usize]) {
    while i > 0 {
        let parent = (i - 1) / 2;
        if !head_less(heap[i], heap[parent], keys, pos) {
            break;
        }
        heap.swap(i, parent);
        i = parent;
    }
}

fn sift_down(heap: &mut [usize], mut i: usize, keys: &[arrow::row::Rows], pos: &[usize]) {
    loop {
        let (l, r) = (2 * i + 1, 2 * i + 2);
        let mut min = i;
        if l < heap.len() && head_less(heap[l], heap[min], keys, pos) {
            min = l;
        }
        if r < heap.len() && head_less(heap[r], heap[min], keys, pos) {
            min = r;
        }
        if min == i {
            return;
        }
        heap.swap(i, min);
        i = min;
    }
}

impl SortMergeStream {
    /// Emit the next chunk, or `None` when every run is drained.
    fn next_chunk(&mut self) -> Option<Result<RecordBatch, arrow_schema::ArrowError>> {
        if self.heap.is_empty() {
            return None;
        }
        let mut indices: Vec<(usize, usize)> = Vec::with_capacity(self.chunk_rows);
        let mut drained: Vec<usize> = Vec::new();
        while indices.len() < self.chunk_rows && !self.heap.is_empty() {
            let run = self.heap[0];
            indices.push((run, self.pos[run]));
            self.pos[run] += 1;
            if self.pos[run] < self.runs[run].num_rows() {
                sift_down(&mut self.heap, 0, &self.keys, &self.pos);
            } else {
                let last = self.heap.len() - 1;
                self.heap.swap(0, last);
                self.heap.truncate(last);
                if !self.heap.is_empty() {
                    sift_down(&mut self.heap, 0, &self.keys, &self.pos);
                }
                drained.push(run);
            }
        }
        let refs: Vec<&RecordBatch> = self.runs.iter().collect();
        let chunk = arrow::compute::interleave_record_batch(&refs, &indices);
        // Free what this chunk finished with: the run's columns and its encoded
        // keys. Without this a merge would hold the whole bucket to the end.
        for run in drained {
            self.runs[run] = RecordBatch::new_empty(self.schema.clone());
            self.keys[run] = self.converter.empty_rows(0, 0);
        }
        Some(chunk)
    }
}

/// Sort one batch by `sort_idx`, returning it untouched when already ordered.
fn sort_one_batch(batch: &RecordBatch, sort_idx: &[(usize, &crate::schema_loader::SortingColumnDef)]) -> Result<RecordBatch, arrow_schema::ArrowError> {
    use arrow::compute::{SortColumn, SortOptions, lexsort_to_indices, take_record_batch};
    let sort_cols: Vec<SortColumn> = sort_idx
        .iter()
        .map(|(i, sc)| SortColumn { values: batch.column(*i).clone(), options: Some(SortOptions { descending: sc.descending, nulls_first: sc.nulls_first }) })
        .collect();
    let indices = lexsort_to_indices(&sort_cols, None)?;
    // Already ordered (common: append-ordered, ~monotonic timestamp) → skip the take copy.
    if indices.values().iter().enumerate().all(|(i, &v)| v as usize == i) {
        return Ok(batch.clone());
    }
    take_record_batch(batch, &indices)
}

/// Default in-process sort budget for the FLUSH path, in in-memory Arrow bytes.
/// Compaction must not reuse it — see the note in `sort_batches_by_schema`.
/// Test-only: production callers pass `maintenance.timefusion_sort_skip_bytes`.
#[cfg(test)]
pub(crate) const DEFAULT_SORT_SKIP_BYTES: usize = 256 * 1024 * 1024;

fn sort_batches_by_schema(schema: &crate::schema_loader::TableSchema, batches: Vec<RecordBatch>, skip_over_bytes: usize) -> (FlushBatches, bool) {
    use arrow::{
        compute::{SortOptions, concat_batches},
        row::{RowConverter, SortField},
    };
    let unsorted = |b: Vec<RecordBatch>| (FlushBatches::Ready(b.into_iter()), false);
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return unsorted(batches);
    }
    // `skip_over_bytes` is the caller's budget for an IN-PROCESS sort, measured
    // in in-memory Arrow bytes — NOT file bytes. The two differ by the zstd
    // ratio, ~17x on prod otel data, which is exactly how a compaction path
    // packing to a 256 MB FILE target silently blew a 256 MB in-memory budget on
    // every single bin (see `stage_hot_bin`). A caller that sorts in a
    // DataFusion plan instead should not be using this function at all.
    //
    // Skip the in-flight sort for very large coalesced groups (bulk backfill):
    // concat + lexsort + take materializes the whole group 2-3x on the flush
    // path — a serial CPU + RSS spike that, multiplied by flush_parallelism,
    // slows commits and risks OOM. Write unsorted (declare_sorted=false is
    // correctness-safe — the footer just won't advertise an order) and let
    // scheduled compaction re-sort/Z-order. Steady-state per-(project,table)
    // groups stay well under the threshold, keeping their sorted footer +
    // compression; only giant backfill coalesces trip it.
    let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if total_bytes > skip_over_bytes {
        return unsorted(batches);
    }
    let first_schema = batches[0].schema();
    // A schema-diverse bucket (mem_buffer's `schemas_compatible` admits batches
    // that differ by an evolved nullable column) is unified to a common
    // superset schema and every batch cast to it, so the bucket STILL flushes
    // as one globally sorted file with an honest `sorting_columns` footer.
    // Bailing here (the old behavior) left the file unsorted, and one unsorted
    // file disables the delta-rs reader's all-or-nothing footer-ordering
    // pushdown for the whole scan — degrading `ORDER BY <keys> LIMIT n` to a
    // blocking full-window sort (top-N pushdown inert on prod, 2026-07-15).
    // `try_merge` yields a lossless superset (missing/nullable fields unioned),
    // and `cast_record_batch(add_missing=true)` fills absent columns with
    // nulls; any incompatibility falls back to the old unsorted write.
    let (arrow_schema, batches) = if batches.iter().all(|b| b.schema() == first_schema) {
        (first_schema, batches)
    } else {
        let merged = match arrow_schema::Schema::try_merge(batches.iter().map(|b| b.schema().as_ref().clone())) {
            Ok(m) => Arc::new(m),
            Err(e) => {
                warn!("sort_batches_by_schema: schema merge failed, writing unsorted: {e}");
                return unsorted(batches);
            }
        };
        match batches.iter().map(|b| deltalake::kernel::schema::cast_record_batch(b, merged.clone(), true, true)).collect::<Result<Vec<_>, _>>() {
            Ok(normalized) => (merged, normalized),
            Err(e) => {
                warn!("sort_batches_by_schema: schema-unify cast failed, writing unsorted: {e}");
                return unsorted(batches);
            }
        }
    };
    let sort_idx: Vec<(usize, &crate::schema_loader::SortingColumnDef)> =
        schema.sorting_columns.iter().filter_map(|sc| arrow_schema.index_of(&sc.name).ok().map(|i| (i, sc))).collect();
    if sort_idx.is_empty() {
        return unsorted(batches);
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // Small buckets: one concat + one sort is cheaper than encoding rows and
    // running a heap, and the peak it can reach is bounded by MERGE_MIN_ROWS.
    if batches.len() == 1 || total_rows <= MERGE_MIN_ROWS {
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            match concat_batches(&arrow_schema, &batches) {
                Ok(c) => c,
                Err(e) => {
                    warn!("sort_batches_by_schema: concat failed, writing unsorted: {e}");
                    return unsorted(batches);
                }
            }
        };
        return match sort_one_batch(&combined, &sort_idx) {
            Ok(sorted) => (FlushBatches::Ready(vec![sorted].into_iter()), true),
            Err(e) => {
                warn!("sort_batches_by_schema: sort failed, writing unsorted: {e}");
                (FlushBatches::Ready(vec![combined].into_iter()), false)
            }
        };
    }
    // Streaming path: sort each run, then k-way merge into writer-sized chunks.
    // Setup is fallible (row encoding rejects a few exotic key types, lexsort
    // can fail); any failure before the first chunk falls back to writing the
    // bucket unsorted — the same downgrade the old concat path took, and the
    // footer stays honest because `sorted` is decided here, up front. Once the
    // merge starts there is no downgrade left (rows are already going into a
    // writer configured with the sorted footer), so a mid-merge error
    // propagates and fails the flush rather than writing a dishonest file.
    let converter = match sort_idx
        .iter()
        .map(|(i, sc)| {
            Ok(SortField::new_with_options(arrow_schema.field(*i).data_type().clone(), SortOptions { descending: sc.descending, nulls_first: sc.nulls_first }))
        })
        .collect::<Result<Vec<_>, arrow_schema::ArrowError>>()
        .and_then(RowConverter::new)
    {
        Ok(c) => c,
        Err(e) => {
            warn!("sort_batches_by_schema: row converter unavailable, writing unsorted: {e}");
            return unsorted(batches);
        }
    };
    let mut merge = SortMergeStream {
        schema: arrow_schema.clone(),
        converter,
        runs: Vec::with_capacity(batches.len()),
        keys: Vec::with_capacity(batches.len()),
        pos: Vec::with_capacity(batches.len()),
        heap: Vec::with_capacity(batches.len()),
        // Chunk sized in rows from the bucket's own average row width.
        chunk_rows: (MERGE_CHUNK_BYTES / (total_bytes / total_rows.max(1)).max(1)).clamp(MERGE_CHUNK_ROWS_MIN, MERGE_CHUNK_ROWS_MAX),
    };
    // Sort run-by-run, consuming the input as we go: the unsorted original is
    // dropped as soon as its sorted copy exists, so the bucket is never
    // resident twice.
    let mut leftover: Vec<RecordBatch> = Vec::new();
    for batch in batches {
        if !leftover.is_empty() {
            leftover.push(batch);
            continue;
        }
        if batch.num_rows() == 0 {
            continue;
        }
        match sort_one_batch(&batch, &sort_idx).and_then(|run| {
            let key_cols: Vec<_> = sort_idx.iter().map(|(i, _)| run.column(*i).clone()).collect();
            merge.converter.convert_columns(&key_cols).map(|keys| (run, keys))
        }) {
            Ok((run, keys)) => {
                merge.runs.push(run);
                merge.keys.push(keys);
                merge.pos.push(0);
            }
            Err(e) => {
                // Keep every row: the sorted-so-far runs plus this batch and
                // the rest go out unsorted (same rows, no order claim).
                warn!("sort_batches_by_schema: run sort/encode failed, writing unsorted: {e}");
                leftover = std::mem::take(&mut merge.runs);
                leftover.push(batch);
            }
        }
    }
    if !leftover.is_empty() {
        return unsorted(leftover);
    }
    if merge.runs.is_empty() {
        return unsorted(Vec::new());
    }
    for run in 0..merge.runs.len() {
        merge.heap.push(run);
        let last = merge.heap.len() - 1;
        sift_up(&mut merge.heap, last, &merge.keys, &merge.pos);
    }
    (FlushBatches::Merge(merge), true)
}

/// Reference implementation of [`sort_batches_by_schema`]: concat the whole
/// bucket, one global `lexsort_to_indices`, one `take`. Superseded in
/// production by the streaming merge (which peaks at ~1 copy instead of 2-3),
/// and kept as the equivalence oracle the property test compares against —
/// the streaming path must reproduce this row order exactly, ties included.
#[cfg(test)]
fn sort_batches_by_schema_reference(schema: &crate::schema_loader::TableSchema, batches: Vec<RecordBatch>) -> (Vec<RecordBatch>, bool) {
    use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch};
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return (batches, false);
    }
    let first_schema = batches[0].schema();
    let (arrow_schema, batches) = if batches.iter().all(|b| b.schema() == first_schema) {
        (first_schema, batches)
    } else {
        let Ok(merged) = arrow_schema::Schema::try_merge(batches.iter().map(|b| b.schema().as_ref().clone())).map(Arc::new) else {
            return (batches, false);
        };
        match batches.iter().map(|b| deltalake::kernel::schema::cast_record_batch(b, merged.clone(), true, true)).collect::<Result<Vec<_>, _>>() {
            Ok(normalized) => (merged, normalized),
            Err(_) => return (batches, false),
        }
    };
    let sort_idx: Vec<(usize, &crate::schema_loader::SortingColumnDef)> =
        schema.sorting_columns.iter().filter_map(|sc| arrow_schema.index_of(&sc.name).ok().map(|i| (i, sc))).collect();
    if sort_idx.is_empty() {
        return (batches, false);
    }
    let combined = if batches.len() == 1 { batches.into_iter().next().unwrap() } else { concat_batches(&arrow_schema, &batches).unwrap() };
    let sort_cols: Vec<SortColumn> = sort_idx
        .iter()
        .map(|(i, sc)| SortColumn {
            values: combined.column(*i).clone(),
            options: Some(SortOptions { descending: sc.descending, nulls_first: sc.nulls_first }),
        })
        .collect();
    let indices = lexsort_to_indices(&sort_cols, None).unwrap();
    if indices.values().iter().enumerate().all(|(i, &v)| v as usize == i) {
        return (vec![combined], true);
    }
    (vec![take_record_batch(&combined, &indices).unwrap()], true)
}

/// delta-rs optimize `SortColumn` spec from the table's declared
/// `sorting_columns`. Empty when the table declares none (caller falls back to
/// `Compact`). Directions mirror the schema so the written order matches the
/// footer the flush/dedup path already declares.
fn schema_optimize_sort_columns(schema: &crate::schema_loader::TableSchema) -> Vec<deltalake::operations::optimize::SortColumn> {
    schema
        .sorting_columns
        .iter()
        .map(|c| deltalake::operations::optimize::SortColumn { column: c.name.clone(), descending: c.descending, nulls_first: c.nulls_first })
        .collect()
}

/// Columns that get per-file min/max/nullCount stats in the Delta log
/// (`delta.dataSkippingStatsColumns`). Deliberately narrow: the time column
/// (every query and `light_optimize_tail`'s event-time binning read it), the
/// declared sort keys (what file pruning is ordered by), and the dedup keys +
/// tiebreak (what the dedup probes prune on). Partition columns are excluded —
/// they're encoded in the path, not the stats. Writing stats for the whole 90+
/// column schema instead cost 18.4% of process CPU in Add-stats `parse_json_impl`
/// on every log replay (prod profile 2026-07-29).
fn stats_columns_for(schema: &crate::schema_loader::TableSchema) -> String {
    std::iter::once(schema.time_column_name())
        .chain(schema.sorting_columns.iter().map(|c| c.name.as_str()))
        .chain(schema.dedup_keys.iter().map(String::as_str))
        .chain(schema.dedup_tiebreak.as_deref())
        .filter(|c| !schema.partitions.iter().any(|p| p == c))
        // Dedup preserving first-seen order; the list is a handful of columns.
        .fold(Vec::<&str>::new(), |mut cols, c| {
            if !cols.contains(&c) {
                cols.push(c);
            }
            cols
        })
        .join(",")
}

/// SQL `ORDER BY` clause (leading space, quoted identifiers) matching the
/// schema's sort order, for rewrite paths that stream through a `SELECT` rather
/// than delta-rs optimize (recompress). Empty when no sort order is declared.
fn schema_order_by_clause(schema: &crate::schema_loader::TableSchema) -> String {
    if schema.sorting_columns.is_empty() {
        return String::new();
    }
    let cols = schema
        .sorting_columns
        .iter()
        .map(|c| format!("\"{}\" {}{}", c.name, if c.descending { "DESC" } else { "ASC" }, if c.nulls_first { " NULLS FIRST" } else { " NULLS LAST" }))
        .collect::<Vec<_>>()
        .join(", ");
    format!(" ORDER BY {cols}")
}

/// Full compaction optionally sorts by the schema's timestamp-leading keys so
/// rewritten files retain tight timestamp statistics and an honest footer.
fn full_optimize_type(schema: &crate::schema_loader::TableSchema, allow_sort: bool) -> (deltalake::operations::optimize::OptimizeType, bool) {
    choose_optimize_type(schema, false, allow_sort)
}

/// One staged bin's intent line in the staged-intent manifest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct StagedIntent {
    wave_id: String,
    /// Owning table — reconcile/clear act ONLY on their own table's entries;
    /// without this the first table reconciled at boot judged every other
    /// table's entries "orphan" against the wrong snapshot and cleared them.
    #[serde(default)]
    table_name: String,
    project_id: String,
    /// Unix seconds at staging. Reconcile skips entries younger than a wave's
    /// max age: on an overlapping rolling deploy the booting instance must not
    /// delete parquet a still-running instance staged but hasn't committed yet
    /// (the v279-shaped hazard). Old entries are unambiguous crash leftovers.
    #[serde(default)]
    recorded_at: u64,
    paths: Vec<String>,
}

/// Parse the append-only manifest, SKIPPING any line that doesn't decode. The
/// manifest is a cleanup aid, never a correctness input (staged parquet is
/// invisible to readers until the atomic wave commit, and VACUUM is the
/// backstop), so a torn tail from an unclean shutdown must degrade to "fewer
/// entries", never to a boot failure.
fn parse_staged_intents(contents: &str) -> Vec<StagedIntent> {
    contents.lines().filter(|l| !l.trim().is_empty()).filter_map(|line| serde_json::from_str::<StagedIntent>(line).ok()).collect()
}

/// Which staged paths to delete on boot: everything the Delta log does NOT
/// reference. A referenced path belongs to a wave that DID commit (the manifest
/// entry just never got removed — crash after commit, before compaction of the
/// manifest), and deleting it would destroy live data. Pure so the decision is
/// testable without an object store; deliberately takes the referenced set from
/// the snapshot rather than a LIST — R2 listing is a known incident source and
/// this path must never issue one.
/// `now_secs` gates the rolling-deploy hazard: entries younger than
/// `STAGED_INTENT_MIN_AGE_SECS` may belong to a live instance sharing the
/// volume and are left for its own wave commit / the next boot / VACUUM.
const STAGED_INTENT_MIN_AGE_SECS: u64 = 30 * 60;

fn staged_orphan_deletions(entries: &[StagedIntent], table_name: &str, now_secs: u64, referenced: &std::collections::HashSet<String>) -> Vec<String> {
    entries
        .iter()
        .filter(|e| e.table_name == table_name)
        .filter(|e| now_secs.saturating_sub(e.recorded_at) >= STAGED_INTENT_MIN_AGE_SECS)
        .flat_map(|e| e.paths.iter())
        .filter(|p| !referenced.contains(p.as_str()))
        .cloned()
        .collect()
}

/// One bin rewritten to staged parquet but NOT yet committed. Uncommitted Adds
/// are invisible to Delta readers, so a wave can hold several of these while
/// other bins finish; the wave commit turns them all into one transaction.
struct StagedBin {
    project_id: String,
    /// Manifest key for this bin's staged files (see `record_staged_intent`).
    wave_id: String,
    /// The files this bin replaces — re-verified live under the commit lock.
    target_paths: Vec<String>,
    removes: Vec<deltalake::kernel::Action>,
    adds: Vec<deltalake::kernel::Action>,
    /// Store the staged parquet was written to, for `cleanup_orphaned_parquet`.
    stage_store: Arc<dyn object_store::ObjectStore>,
    /// Dedup-only accounting; `None` for hot compaction bins. Also THE source of
    /// truth for `data_change` (see [`StagedBin::data_change`]) — a dedup unit
    /// drops rows by definition, a compaction unit cannot.
    dedup: Option<DedupUnit>,
}

impl StagedBin {
    /// Derived, never stored: a second `data_change` field could disagree with
    /// `dedup`, and disagreeing here means committing a row-dropping rewrite
    /// under snapshot isolation (see [`staged_actions`]).
    fn data_change(&self) -> bool {
        self.dedup.is_some()
    }
}

/// Per-unit outcome of one wave commit. Per-unit (not a count) because dedup
/// has to requeue exactly the dirty bins that did NOT land.
struct WaveResult {
    landed: Vec<StagedBin>,
    failed: Vec<StagedBin>,
}

/// Dirty-bin queue key: (project_id, table_name, date, 10-minute bin).
type DirtyBinKey = (String, String, String, i64);

/// Snapshot-relative files belonging to one physical project/date partition.
/// Unified tables carry `project_id=` path segments; custom-project tables do
/// not, because the whole physical table already belongs to that project.
pub(crate) fn dedup_partition_paths(paths: impl IntoIterator<Item = String>, project_id: &str, date: &str) -> Vec<String> {
    let date_segment = format!("date={date}");
    let project_segment = format!("project_id={project_id}");
    let date_files: Vec<String> = paths.into_iter().filter(|path| path.split('/').any(|segment| segment == date_segment)).collect();
    if date_files.iter().any(|path| path.split('/').any(|segment| segment.starts_with("project_id="))) {
        date_files.into_iter().filter(|path| path.split('/').any(|segment| segment == project_segment)).collect()
    } else {
        date_files
    }
}

/// The extra baggage a dedup unit carries through a wave: the rows it drops
/// (reported once the unit LANDS, never at staging time) and the dirty-bin it
/// came from, so a unit that loses the liveness check or the commit is requeued
/// instead of silently certifying a partition that still holds duplicates.
struct DedupUnit {
    /// `None` for the fallback whole-partition sweep, which has no queue entry.
    key: Option<DirtyBinKey>,
    /// Partition date, for scoping the wave's cache warm/evict diff.
    date: String,
    /// Human label for logs only (`project … timestamp in [a, b)`).
    label: String,
    before: u64,
    after: u64,
}

impl DedupUnit {
    fn dropped(&self) -> u64 {
        self.before.saturating_sub(self.after)
    }
}

/// Rows a set of LANDED dedup units removed from the table. Units that never
/// landed contribute nothing — they staged a smaller copy that no commit
/// references, so their `before - after` is a rewrite that didn't happen.
fn wave_dropped_rows(bins: &[StagedBin]) -> u64 {
    bins.iter().filter_map(|b| b.dedup.as_ref()).map(DedupUnit::dropped).sum()
}

/// Assemble one staged unit's Remove+Add actions.
///
/// `data_change` is the load-bearing bit of the whole wave engine and the ONE
/// thing that differs between its two producers:
///
/// * **Hot compaction (`false`)** — the rewrite preserves every row, so both
///   sides are marked data-preserving and the fork's conflict checker downgrades
///   the commit to snapshot isolation (`conflict_checker.rs:561` only counts
///   `data_change: true` removals as conflicts). That is what lets a wave commit
///   next to concurrent ingest appends without the OCC ladder (aa50480).
/// * **Dedup (`true`)** — the rewrite DROPS rows. Marking those actions
///   data-preserving would be a lie to every concurrent reader/writer: a
///   transaction that read the removed files would no longer be told its read
///   set was invalidated, so the isolation downgrade is only sound for
///   data-preserving commits. Dedup pays the honest OCC price; the wave still
///   wins because N chunk commits collapse into one.
///
/// [`wave_operation`] derives the `DeltaOperation` from the same flag so the two
/// can never drift apart.
fn staged_actions(
    targets: &[deltalake::kernel::Add], staged: Vec<deltalake::kernel::Action>, data_change: bool,
) -> (Vec<deltalake::kernel::Action>, Vec<deltalake::kernel::Action>) {
    use deltalake::kernel::Action;
    let removes: Vec<Action> = targets.iter().map(|a| Action::Remove(remove_for_add(a, data_change))).collect();
    let adds: Vec<Action> = staged
        .into_iter()
        .map(|a| match a {
            Action::Add(mut add) => {
                add.data_change = data_change;
                Action::Add(add)
            }
            other => other,
        })
        .collect();
    (removes, adds)
}

/// The operation a wave commits under, derived from [`staged_actions`]'s
/// `data_change`. An Optimize over data-preserving actions is what the fork
/// downgrades to snapshot isolation; a Write/Overwrite would re-inherit the OCC
/// ladder the wave design exists to kill (aa50480 / 96f4785). Conversely a
/// row-dropping dedup MUST stay a Write — an Optimize that silently changed the
/// logical data would let a concurrent transaction keep a read set the commit
/// invalidated.
fn wave_operation(data_change: bool, target_size: i64, partition_by: Option<Vec<String>>) -> deltalake::protocol::DeltaOperation {
    use deltalake::protocol::DeltaOperation;
    if data_change {
        DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Overwrite, partition_by, predicate: None }
    } else {
        DeltaOperation::Optimize { predicate: None, target_size }
    }
}

/// Gap between the light-optimize schedule's next two fires — the period the
/// tick budget is derived from (`derived.tick_budget`). Zero when the schedule
/// can't be parsed, in which case the budget falls back to the cron period the
/// scheduler itself would use.
fn cron_period(schedule: &str) -> std::time::Duration {
    let Ok(cron) = schedule.parse::<croner::Cron>() else { return std::time::Duration::ZERO };
    let now = Utc::now();
    let Ok(first) = cron.find_next_occurrence(&now, false) else { return std::time::Duration::ZERO };
    let Ok(second) = cron.find_next_occurrence(&first, false) else { return std::time::Duration::ZERO };
    (second - first).to_std().unwrap_or(std::time::Duration::ZERO)
}

/// Current process memory footprint for the wave-boundary brake. cgroup
/// `memory.current` is the number the OOM killer acts on (it includes page
/// cache the kernel can reclaim, so this is conservative); `/proc/self/statm`
/// RSS is the fallback. `None` on platforms with neither (dev macOS) — the
/// brake then never engages, which is the safe default for a non-prod box.
/// Charged memory for the brake: cgroup `memory.current` (what memcg OOM-kills
/// on, page cache included) — deliberately NOT the same number as the WAL
/// layer's RSS diagnostic, whose statm reader we reuse only as the fallback.
pub(crate) fn process_memory_bytes() -> Option<usize> {
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        && let Ok(v) = raw.trim().parse::<usize>()
    {
        // Discount the INACTIVE file cache. `memory.current` counts it, but the
        // kernel reclaims it before OOM-killing anything, so charging it to the
        // brake is charging pressure that does not exist — and it is not small:
        // prod 2026-08-01 read 88.4 GB current = 75.6 GB anon + 11.7 GB
        // inactive file. That 11.7 GB brought the brake forward by the same
        // amount, and since the brake means "no maintenance AT ALL", it shortens
        // the post-restart window in which compaction and dedup can make any
        // progress (19.8k-deep dirty-bin backlog, 14 bins processed per process
        // lifetime). Active file cache is left charged: it is being read now and
        // reclaiming it costs IO.
        return Some(v.saturating_sub(cgroup_inactive_file_bytes().unwrap_or(0)));
    }
    crate::metrics::process_rss_bytes()
}

/// `inactive_file` from cgroup v2 `memory.stat` — the page cache the kernel
/// drops first under pressure. `None` when unreadable (the caller then charges
/// the full `memory.current`, i.e. today's behaviour).
fn cgroup_inactive_file_bytes() -> Option<usize> {
    let raw = std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?;
    raw.lines().find_map(|l| l.strip_prefix("inactive_file ")?.trim().parse().ok())
}

/// Host free memory: `MemAvailable` from /proc/meminfo, which inside a
/// container is the HOST's — exactly the figure the kernel's global OOM
/// killer races against on an over-committed host. `None` on parse failure
/// (the host brake then never engages; the cgroup brake still does).
fn host_mem_available_bytes() -> Option<u64> {
    let raw = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = raw.lines().find(|l| l.starts_with("MemAvailable:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// Tag the fork stamps on sorted-run outputs (delta-rs optimize.rs). Kept in
/// sync by the exact rev pin; a fork rename would need a deliberate bump.
const SORTED_RUN_TAG: &str = "delta-rs.optimize.sort_by";

fn is_sorted_run(tags: &HashMap<String, Option<String>>) -> bool {
    tags.get(SORTED_RUN_TAG).is_some_and(|v| v.as_deref() == Some("true"))
}

/// Files whose newest event is within SEAL_LAG of now may still receive appends
/// / DV-merge rewrites, so compacting them (a) races concurrent commits → OCC
/// abort (the wedged optimize on today's partition, prod 2026-07-20) and (b)
/// would need re-compaction. Only sealed time slices are compacted.
/// 15min: a 5min lag was tried (shrinks the recent file count faster) but
/// THRASHED the Foyer cache — rewriting the recent window every 5min churns file
/// IDs faster than 1h-window queries reuse the warm bodies, so 1h latency stuck
/// at 2-7s and never warmed while 3h (stable older files) did (prod 2026-07-21).
fn seal_micros_now() -> i64 {
    const SEAL_LAG_MICROS: i64 = 15 * 60 * 1_000_000;
    crate::clock::now_micros() - SEAL_LAG_MICROS
}

/// The metadata one planner walk collects per candidate file. Decoupling
/// selection from the snapshot API is what makes the packing policy testable
/// (and lets `select_all_hot_bins` parse each file's stats exactly once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailAdd {
    pub path: String,
    pub size: i64,
    pub is_sorted_run: bool,
    /// (min, max) event time from Add stats; None when stats are absent —
    /// one field so a half-present range is unrepresentable.
    pub event_range: Option<(i64, i64)>,
}

impl TailAdd {
    /// Parse the raw Add stats JSON: the snapshot's parsed-stats column
    /// (`stats_parsed`) isn't materialized on this path, so the kernel
    /// `stat_min_i64`/`stat_max_i64` accessors return None for every file — the
    /// 2026-07-20 event-time binning read them and silently selected NOTHING
    /// (hot compaction was a no-op in prod while today-partitions grew to 474+
    /// files).
    fn from_stats(path: String, size: i64, is_sorted_run: bool, stats: Option<&str>) -> Self {
        let range = stats.and_then(Database::event_time_range_from_stats);
        Self { path, size, is_sorted_run, event_range: range }
    }
}

/// Pick the files one light-optimize bin should rewrite. Pure so the packing
/// policy — every clause of which is an incident scar — is unit-testable
/// without a Delta table; see `light_optimize_tail` for why the selection is
/// bounded at all (partition-wide rewrites lose every OCC race on the hot
/// today-partition).
///
/// `sorted_run_cap` bounds which already-tagged sorted runs are re-admitted: the
/// cold tier passes `i64::MAX` (its leveled re-merge folds any sub-target run),
/// the hot tier passes `target/2` so each tick's small output run keeps folding
/// into the next pack until it reaches ~1/2 target — otherwise a busy project
/// accrues one 5-min run per tick (~100+ files/day again). Was `target/4`:
/// on the busiest tenant that stranded a 64–128MB band today's queries had to
/// open by the hundreds (2026-07-30, 696 live files p50 22MB); folding one
/// level further costs ~1 extra rewrite per byte and halves steady-state file
/// count. Files at
/// ≥ 7/8·target are always excluded — they're converged, and re-selecting one
/// alone would rewrite it 1→1 forever.
///
/// Binning by EVENT time (not arrival/modification_time) is what makes the
/// output runs time-DISJOINT, so file-range pruning can exclude files outside a
/// query's window (arrival-time binning left every file overlapping →
/// files_ranges_pruned=56→56, i.e. no pruning).
pub(crate) fn select_tail_bin(adds: &[TailAdd], target_size: i64, min_files: usize, sorted_run_cap: i64, seal_micros: i64) -> Vec<String> {
    let cap = target_size.max(1);
    let converged = cap - cap / 8;
    // An oversized file that is NOT a sorted run is a REPAIR candidate, not a
    // converged one: it declares no `sorting_columns`, and one such file
    // disables the reader's all-or-nothing footer ordering for the whole scan.
    // Exactly one is admitted per bin (`repair_budget`) so a backlog of
    // hundred-megabyte files drains across ticks rather than in one rewrite.
    let is_repair = |add: &TailAdd| add.size >= converged && !add.is_sorted_run;
    let mut fresh: Vec<(&str, i64, i64, bool)> = adds
        .iter()
        .filter(|add| add.size < sorted_run_cap || !add.is_sorted_run)
        .filter(|add| add.size < converged || is_repair(add))
        .filter_map(|add| match add.event_range {
            Some((min, max)) if max <= seal_micros => Some((add.path.as_str(), min, add.size, is_repair(add))),
            _ => None,
        })
        .collect();
    // A lone repair file is real work — it is the only way an oversized
    // unsorted file ever gets rewritten — so it does not need `min_files`
    // company to justify a pass.
    let repairs_present = fresh.iter().any(|(_, _, _, repair)| *repair);
    if fresh.len() < min_files && !repairs_present {
        return vec![];
    }
    fresh.sort_unstable_by_key(|(_, min, _, _)| *min);
    // Pack the earliest contiguous slice up to `cap` → one time-disjoint run
    // per tick. Small commit converges quickly and shrinks the conflict window;
    // later ticks pack the next (strictly later) slice.
    let mut bytes = 0i64;
    let mut files: Vec<String> = vec![];
    for (path, _, size, repair) in fresh.iter().filter(|(_, _, _, r)| !*r) {
        let (path, size) = (*path, *size);
        let _ = repair;
        if !files.is_empty() && bytes + size > cap {
            // A lone-file slice is already a run — rewriting it is pure churn.
            // Skip past it to the next time slice instead of wedging the pass
            // behind it.
            if files.len() >= 2 {
                break;
            }
            bytes = 0;
            files.clear();
        }
        bytes += size;
        files.push(path.to_string());
    }
    if files.len() < 2 {
        files.clear();
    }
    // Repair runs in the GAPS, never against the primary path: only once a
    // project has no packable slice left does a tick spend itself rewriting one
    // oversized unsorted file. Preempting normal packing would starve it while
    // a backlog of legacy files drained. Steady state is exactly when the gaps
    // appear — the sub-target backlog is packed into converged runs, and the
    // leftover poison files are what remain.
    if files.is_empty()
        && let Some((path, _, _, _)) = fresh.iter().find(|(_, _, _, repair)| *repair)
    {
        return vec![path.to_string()];
    }
    files
}

/// Split staged bins into (committable, stale) against the live file set of the
/// refreshed snapshot. One conflicting file used to fail one commit of one bin;
/// with a batched wave commit, dropping only the stale bin's actions keeps the
/// blast radius identical to the per-bin path (the dropped bin's files are just
/// re-selected next tick). Pure + generic so the wave-assembly test needs no
/// object store.
fn split_live_bins<T>(bins: Vec<T>, targets: impl Fn(&T) -> &[String], live: &std::collections::HashSet<String>) -> (Vec<T>, Vec<T>) {
    bins.into_iter().partition(|bin| targets(bin).iter().all(|t| live.contains(t)))
}

/// Whether a bin's OWN staged Adds are active in the snapshot — i.e. its commit
/// landed. Distinguishes "another writer rewrote my targets" (staged parquet is
/// garbage) from "my own earlier attempt landed and then errored" (staged
/// parquet is LIVE DATA). See the self-landed split in `commit_wave`.
/// A bin with no Adds can't have landed anything.
fn bin_adds_live(bin: &StagedBin, live: &std::collections::HashSet<String>) -> bool {
    let mut adds = bin.adds.iter().filter_map(|a| match a {
        deltalake::kernel::Action::Add(add) => Some(add.path.as_str()),
        _ => None,
    });
    adds.next().is_some_and(|first| live.contains(first) && adds.all(|p| live.contains(p)))
}

/// `landed ++ more` — the wave's two landing sources (bins confirmed by an
/// earlier attempt, and the ones this attempt just committed) as one list.
fn concat_landed(mut landed: Vec<StagedBin>, more: Vec<StagedBin>) -> Vec<StagedBin> {
    landed.extend(more);
    landed
}

/// Drive one bin per project per round, each round being a WAVE: every project
/// gets its Nth bin before any project gets its (N+1)th, the round's bins are
/// staged in parallel (`op`, `buffer_unordered(concurrency)`) WITHOUT
/// committing, and the whole round lands in ONE `commit_wave` call. Batching the
/// commit is what collapses the OCC ladder: ~130 commits/tick against concurrent
/// ingest appends retried to attempt 9-20 (~10s each), so 40-65s of a pass was
/// commit waiting (prod 2026-07-29).
///
/// Stops when every project's tail is converged, the round cap is hit, the
/// wall-clock deadline passes, or `should_pause` engages a safety brake. In
/// every stop case the in-flight wave still finishes and commits — brakes are
/// one-way "start no more work", never a cancellation.
///
/// Generic over the per-bin operation + the wave commit so the fairness
/// invariant is unit-testable without a Delta table. Returns the count of bins
/// that errored (staging failures + commit-side drops).
/// Per-project outcome of one round's staging. A dedicated type because
/// `Result<Option<T>>` overloaded `None`: "converged" (drop for the tick) and
/// "bin vanished under a concurrent rewrite" (project verifiably HAS work —
/// dropping it silenced its compaction for the rest of the tick, prod-shaped
/// dedup race found in the 2026-07-29 review).
enum BinOutcome<T> {
    /// Bin staged; project stays in the round-robin.
    Staged(T),
    /// Tail converged — nothing left this tick; drop from the rotation.
    Converged,
    /// Selection went stale (concurrent rewrite); keep the project pending so
    /// the next round's re-plan serves it a fresh bin.
    Retry,
}

/// A wave-boundary safety brake. Two levels: `Degrade` keeps a SERVICE FLOOR
/// (one project, concurrency 1, for the rest of the tick) so a chronic overload
/// signal throttles compaction instead of starving it; `Stop` ends the tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Brake {
    Degrade(&'static str),
    Stop(&'static str),
}

#[allow(clippy::too_many_arguments)] // scheduling params are positional by design; a struct would just rename them
async fn round_robin_bins<F, Fut, T, C, CFut>(
    projects: Vec<String>, max_rounds: usize, concurrency: usize, deadline: std::time::Instant, on_truncate: impl Fn(usize, &[String]),
    should_pause: impl Fn() -> Option<Brake>, op: F, commit_wave: C,
) -> usize
where
    F: Fn(String, usize) -> Fut,
    Fut: std::future::Future<Output = (String, Result<BinOutcome<T>>)>,
    C: Fn(Vec<T>, usize) -> CFut,
    CFut: std::future::Future<Output = usize>,
{
    let mut pending = projects;
    let mut failed = 0usize;
    let mut concurrency = concurrency;
    for round in 0..max_rounds {
        if pending.is_empty() {
            break;
        }
        if std::time::Instant::now() >= deadline {
            on_truncate(round, &pending);
            break;
        }
        match should_pause() {
            Some(Brake::Stop(reason)) => {
                info!(round, remaining = pending.len(), reason, event = "light_optimize_wave_paused");
                // A pause is a truncation for fairness purposes: without rotating,
                // a chronically-engaged brake restarts every tick at the
                // debt-ordered head — the exact starvation the cursor fixes.
                on_truncate(round, &pending);
                break;
            }
            // Service floor: drop to SERIAL, not to one project. One bin in
            // flight bounds the instantaneous heap exactly as a one-project cut
            // did, while the tick deadline still bounds total work — but every
            // project the deadline admits gets served. The old cut-to-head
            // floor starved compaction for the whole post-boot WAL replay
            // (backlog stays over threshold for ~an hour, so 11 hot projects
            // compacted once per ~55min each and today's flush dribble piled up
            // to 50-65 files/partition, prod 2026-08-03). Projects the
            // deadline does cut still rotate via `on_truncate`.
            Some(Brake::Degrade(reason)) => {
                concurrency = 1;
                crate::metrics::maintenance_stats().light_optimize_ticks_degraded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(round, served = pending.len(), reason, event = "light_optimize_wave_degraded");
            }
            None => {}
        }
        // Admission control INSIDE the round. The `should_pause` above guards
        // only the round BOUNDARY, but the memory a round costs is allocated by
        // the staging below — prod 2026-08-01 watched RSS go 50GB → 110GB
        // (cgroup 128GB) inside one round while the brake, sampled only between
        // rounds, never got the chance to fire; it recorded its first Stop after
        // the round had already landed. `buffer_unordered` polls this closure as
        // it admits each bin, so sampling here stops a round taking on NEW work
        // the moment it crosses the line. Bins already in flight still finish —
        // aborting them would waste the sort that is holding the memory.
        // Deferred projects stay `pending`, so the next round's boundary check
        // truncates the tick and rotates the cursor exactly as a brake there
        // would have.
        let outcomes: Vec<(String, Result<BinOutcome<T>>)> = futures::stream::iter(std::mem::take(&mut pending))
            .map(|project_id| {
                let (op, deferred) = (&op, matches!(should_pause(), Some(Brake::Stop(_))));
                async move {
                    match deferred {
                        true => (project_id, Ok(BinOutcome::Retry)),
                        false => op(project_id, round).await,
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;
        // Carry forward projects that still have work; converged or failed
        // projects drop out for the rest of the tick.
        let mut staged = Vec::with_capacity(outcomes.len());
        for (project_id, outcome) in outcomes {
            match outcome {
                Ok(BinOutcome::Staged(bin)) => {
                    staged.push(bin);
                    pending.push(project_id);
                }
                Ok(BinOutcome::Retry) => pending.push(project_id),
                Ok(BinOutcome::Converged) => {}
                Err(_) => failed += 1,
            }
        }
        if !staged.is_empty() {
            failed += commit_wave(staged, round).await;
        }
    }
    failed
}

fn choose_optimize_type(
    schema: &crate::schema_loader::TableSchema, allow_zorder: bool, allow_sort: bool,
) -> (deltalake::operations::optimize::OptimizeType, bool) {
    use deltalake::operations::optimize::OptimizeType;
    if allow_zorder && !schema.z_order_columns.is_empty() {
        return (OptimizeType::ZOrder(schema.z_order_columns.clone()), false);
    }
    let sort_cols = schema_optimize_sort_columns(schema);
    if allow_sort && !sort_cols.is_empty() { (OptimizeType::SortBy(sort_cols), true) } else { (OptimizeType::Compact, false) }
}

/// Consolidation upgrades SortBy to SortByDedup: duplicates share every sort
/// key (`id` is a content hash), so they're consecutive in the sorted stream
/// and the fork drops them for free while writing. Tiebreak mirrors flush
/// dedup's last-write-wins — greatest `dedup_tiebreak` (observed_timestamp)
/// sorts first and survives, so the enriched re-emit beats the base row.
fn consolidate_optimize_type(schema: &crate::schema_loader::TableSchema, allow_sort: bool) -> (deltalake::operations::optimize::OptimizeType, bool) {
    use deltalake::operations::optimize::{DedupConfig, OptimizeType, SortColumn};
    match choose_optimize_type(schema, false, allow_sort) {
        (OptimizeType::SortBy(cols), true) if !schema.dedup_keys.is_empty() => {
            let tiebreak = schema.dedup_tiebreak.as_ref().map(|tb| SortColumn { column: tb.clone(), descending: true, nulls_first: false });
            (OptimizeType::SortByDedup(cols, DedupConfig { columns: schema.dedup_keys.clone(), tiebreak }), true)
        }
        other => other,
    }
}

/// Pure builder for parquet `WriterProperties` at a given compression tier.
/// Lives outside `impl Database` so unit tests can exercise tier/encoding/bloom
/// decisions without instantiating a Database (which needs S3/MinIO).
/// `declare_sorted` controls whether the parquet footer advertises the schema's
/// `sorting_columns`. Only the write paths that actually sort the rows in that
/// order (flush/append, dedup) may pass `true`. Optimize/compact/recompress
/// rewrite rows into Z-order or concatenation, so they MUST pass `false` —
/// declaring an order the data doesn't have is a latent wrong-results bug for
/// any reader that trusts it. SortBy is the rewrite path that may pass `true`.
fn build_writer_properties(
    parquet_cfg: &crate::config::ParquetConfig, schema: &crate::schema_loader::TableSchema, zstd_level: i32, declare_sorted: bool,
) -> WriterProperties {
    use deltalake::datafusion::parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::{metadata::KeyValue, properties::EnabledStatistics},
        schema::types::ColumnPath,
    };

    let page_row_count_limit = parquet_cfg.timefusion_page_row_count_limit;
    let max_row_group_size = parquet_cfg.timefusion_max_row_group_size;
    let bloom_globally_disabled = parquet_cfg.timefusion_bloom_filter_disabled;

    // Per-column bloom NDV sized to a typical row-group row count.
    // 1M rows ≈ parquet-rs's default `set_max_row_group_size`; gives an
    // ~1.7MB bloom per column at fpp=0.01, vs ~150MB if we naively scaled
    // by the byte-sized `max_row_group_size`. The legacy global 100k
    // produced near-1.0 false-positive rates at scale.
    const BLOOM_NDV: u64 = 1_000_000;

    let sorting_columns_pq = schema.sorting_columns();
    let sort_key_names: std::collections::HashSet<&str> = schema.sorting_columns.iter().map(|c| c.name.as_str()).collect();

    // Note: do NOT call `set_bloom_filter_fpp` at the global level — parquet-rs
    // treats any global bloom setter (other than `set_bloom_filter_enabled`)
    // as implicit enable, which then uses the default NDV (~1M) and triggers
    // massive bloom buffer allocations on every column. We set fpp per-column
    // only, for the columns we actually want blooms on.
    let builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(zstd_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap())))
        .set_max_row_group_bytes(Some(max_row_group_size))
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(8388608)
        // Page-level stats only where they prune (the declared sort keys, set
        // per-column below). Page stats on wide JSON/variant columns
        // (body/attributes/resource) bloat the ColumnIndex with a min/max per
        // page — tens of MB of decoded metadata per file that re-decodes on
        // every scan. Chunk = one min/max per row group for those columns.
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(false)
        .set_data_page_row_count_limit(page_row_count_limit)
        .set_sorting_columns(if declare_sorted && !sorting_columns_pq.is_empty() { Some(sorting_columns_pq) } else { None })
        .set_key_value_metadata(Some(vec![KeyValue::new(COMPRESSION_TIER_KEY.to_string(), zstd_level.to_string())]));

    schema
        .fields
        .iter()
        .fold(builder, |builder, field| {
            let dt = field.data_type.as_str();
            let col = ColumnPath::from(field.name.as_str());
            let is_sort_key = sort_key_names.contains(field.name.as_str());
            let time_like = dt.starts_with("Timestamp") || dt == "Date32";

            // Page-level stats only where they prune AND are cheap: the declared
            // sort keys, plus any timestamp/date column (8-byte min/max, common
            // range predicates like observed_timestamp/start_time/end_time). Wide
            // JSON/variant/string columns stay at the Chunk default so the
            // ColumnIndex doesn't balloon.
            let builder = if is_sort_key || time_like { builder.set_column_statistics_enabled(col.clone(), EnabledStatistics::Page) } else { builder };

            let builder = if time_like {
                builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED).set_column_dictionary_enabled(col.clone(), false)
            } else if matches!(dt, "Int32" | "Int64" | "UInt32" | "UInt64") {
                builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED)
            } else if dt == "Utf8" && is_sort_key {
                builder.set_column_encoding(col.clone(), Encoding::DELTA_BYTE_ARRAY).set_column_dictionary_enabled(col.clone(), false)
            } else {
                builder
            };

            // Explicit per-column dict opt-out (overrides defaults above only
            // when set to Some(false); Some(true)/None leaves defaults intact).
            let builder = if field.dictionary == Some(false) { builder.set_column_dictionary_enabled(col.clone(), false) } else { builder };

            if field.bloom_filter && !bloom_globally_disabled {
                builder
                    .set_column_bloom_filter_enabled(col.clone(), true)
                    .set_column_bloom_filter_ndv(col.clone(), BLOOM_NDV)
                    .set_column_bloom_filter_fpp(col, 0.01)
            } else {
                builder
            }
        })
        .build()
}

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
    _batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>,
    table_name: String,
    /// When true, INSERTs commit straight to Delta (`skip_queue=true`),
    /// bypassing the BufferedWriteLayer (WAL + MemBuffer). Backs the
    /// `{table}__bulk` alias for backfills / DLQ drains that must not pressure
    /// the live MemBuffer. Reads route to the same underlying Delta table.
    skip_queue: bool,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>, table_name: String,
    ) -> Self {
        Self { default_project, database, schema, _batch_queue: batch_queue, table_name, skip_queue: false }
    }

    /// Route this provider's INSERTs straight to Delta, bypassing the
    /// BufferedWriteLayer. Backs the `{table}__bulk` alias.
    pub fn with_skip_queue(mut self, skip_queue: bool) -> Self {
        self.skip_queue = skip_queue;
        self
    }

    fn extract_project_id_from_filters(&self, filters: &[Expr]) -> Option<String> {
        filters.iter().find_map(crate::optimizers::extract_project_id_from_expr)
    }

    /// pgwire-INSERT fast path. Skips `DataSinkExec` + `ValuesExec` entirely:
    /// caller (the plan_cache hook) has already materialized the incoming
    /// VALUES into a RecordBatch from substituted literals, so we just run
    /// the per-batch fixups (`convert_variant_columns`, project-id routing,
    /// `normalize_timestamp_tz` is run inside `insert_records_batch`) and
    /// hand straight to `insert_records_batch` → `BufferedWriteLayer.insert`.
    /// Returns the inserted row count.
    pub async fn fast_insert_batch(&self, batch: RecordBatch) -> DFResult<u64> {
        let total_rows = batch.num_rows() as u64;
        if total_rows == 0 {
            return Ok(0);
        }
        let target_schema = self.real_schema();
        // Partition row-wise: one INSERT may carry rows for many projects, each
        // landing in its own Delta table. Distinct projects write concurrently.
        let writes = partition_batch_by_project(batch, &self.default_project)?
            .into_iter()
            .map(|(project_id, sub)| {
                let converted = convert_variant_columns(sub, &target_schema)?;
                Ok(async move {
                    self.database
                        .insert_records_batch(&project_id, &self.table_name, vec![converted], self.skip_queue, None)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("fast_insert_batch for project {} table {}: {}", project_id, self.table_name, e)))
                })
            })
            .collect::<DFResult<Vec<_>>>()?;
        futures::future::try_join_all(writes).await?;
        Ok(total_rows)
    }

    fn schema(&self) -> SchemaRef {
        // Present Variant cols as Utf8View at the table-provider boundary so the SQL planner's
        // INSERT VALUES type check accepts JSON string literals (arrow has no Utf8→Struct cast).
        // `write_all` converts these Utf8 columns back to Variant structs before the Delta write.
        create_insert_compatible_schema(&self.schema)
    }

    /// Real (Variant-typed) schema for internal use.
    pub fn real_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Determines if a filter can be pushed down exactly to Delta Lake
    fn is_exact_pushdown_filter(expr: &Expr) -> bool {
        match expr {
            // AND expressions are exact if all parts are exact (check this first)
            Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => Self::is_exact_pushdown_filter(left) && Self::is_exact_pushdown_filter(right),
            // Simple column comparisons are exact
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let is_column_literal =
                    matches!((left.as_ref(), right.as_ref()), (Expr::Column(_), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(_)));

                let is_supported_op = matches!(op, Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq);

                if is_column_literal && is_supported_op {
                    // Check if it's a partition column or indexed column
                    if let Expr::Column(col) = left.as_ref() {
                        return Self::is_pushdown_column(&col.name);
                    }
                    if let Expr::Column(col) = right.as_ref() {
                        return Self::is_pushdown_column(&col.name);
                    }
                }
                false
            }
            // IS NULL/IS NOT NULL are exact
            Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
                matches!(inner.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            // IN lists are exact for pushdown columns
            Expr::InList(in_list) => {
                matches!(in_list.expr.as_ref(), Expr::Column(col) if Self::is_pushdown_column(&col.name))
            }
            _ => false,
        }
    }

    /// Checks if a column supports *exact* pushdown — meaning the table
    /// provider promises to fully apply the filter so DataFusion can drop
    /// the FilterExec on top. Only true partition columns qualify:
    /// Delta's partition pruning is genuinely exact, and partition values
    /// are also compared exactly inside MemBuffer.
    ///
    /// Previously this list included `timestamp`, `id`, `level`, etc. on
    /// the assumption that MemBuffer's row-level filter (best-effort) plus
    /// Delta's row-group statistics would catch them. But MemBuffer's
    /// physical-expr compilation silently falls back to "no filter" if the
    /// expression can't be lowered for any reason (type coercion, Utf8View
    /// vs Utf8, etc.) — and with Exact pushdown, FilterExec is gone, so
    /// rows leak through unfiltered. Bench harness caught this as
    /// `timestamp >= '02:55' AND timestamp < '03:00'` returning the entire
    /// 10-minute bucket.
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(column_name, "project_id" | "date")
    }

    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::optimizers::time_range_partition_pruner;

        // Resolve the schema-declared time column for this table; falls back to
        // "timestamp" when the schema isn't registered (custom/dynamic tables).
        let time_column =
            crate::schema_loader::get_schema(&self.table_name).map(|s| s.time_column_name().to_string()).unwrap_or_else(|| "timestamp".to_string());

        let mut optimized_filters = filters.to_vec();

        for filter in filters {
            let date_filters = time_range_partition_pruner::timestamp_to_date_filters(filter, &time_column);
            if !date_filters.is_empty() {
                debug!("Added {} date partition filter(s) for {} on column {}", date_filters.len(), self.table_name, time_column);
                optimized_filters.extend(date_filters);
            }
        }

        // Check if project_id filter is present
        if !self.has_project_id_in_filters(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }

        Ok(optimized_filters)
    }

    /// Check if filters contain a project_id filter
    fn has_project_id_in_filters(&self, filters: &[Expr]) -> bool {
        use crate::optimizers::ProjectIdPushdown;
        ProjectIdPushdown::has_project_id_filter(filters)
    }

    /// Create a MemorySourceConfig-based execution plan with multiple partitions.
    ///
    /// `sorted` asserts every partition is already ordered by the table's
    /// declared `sorting_columns`; it is the caller's correctness claim (see
    /// `MemLeg::sorted` / `HotLeg::sorted`), and declaring it is what stops
    /// `ordered_children` injecting a blocking `SortExec` over this leg on
    /// every merge-on-read scan.
    fn create_memory_exec(&self, partitions: &[Vec<RecordBatch>], projection: Option<&Vec<usize>>, sorted: bool) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mem_source =
            MemorySourceConfig::try_new(partitions, self.schema.clone(), projection.cloned()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let out = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        Ok(Arc::new(DataSourceExec::new(Arc::new(Self::declare_ordering(mem_source, sorted, &self.table_name, &out)))))
    }

    /// Attach the table's declared ordering to an in-memory source.
    ///
    /// Declared against the source's OUTPUT schema (post-projection), and only
    /// for the leading run of sorting columns that survived it — a query that
    /// projects `timestamp` away gets no claim rather than a false one. Failure
    /// to attach is never fatal: an undeclared source is merely slower.
    fn declare_ordering(source: MemorySourceConfig, sorted: bool, table_name: &str, out: &SchemaRef) -> MemorySourceConfig {
        use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
        let Some(table) = crate::schema_loader::get_schema(table_name).filter(|_| sorted) else { return source };
        let exprs: Vec<PhysicalSortExpr> = table
            .sorting_columns
            .iter()
            .map_while(|sc| {
                let idx = out.index_of(&sc.name).ok()?;
                Some(PhysicalSortExpr::new(
                    Arc::new(PhysicalColumn::new(&sc.name, idx)),
                    arrow::compute::SortOptions { descending: sc.descending, nulls_first: sc.nulls_first },
                ))
            })
            .collect();
        let Some(ordering) = LexOrdering::new(exprs) else { return source };
        // `try_with_sort_information` consumes the source, so keep a copy to
        // fall back to — the undeclared source must still serve its rows.
        let undeclared = source.clone();
        source.try_with_sort_information(vec![ordering]).unwrap_or_else(|e| {
            debug!("in-memory leg for {table_name} could not declare its ordering ({e}); the plan will sort it instead");
            undeclared
        })
    }

    /// Scan a Delta table and coerce output schema to match our expected types.
    /// Handles object store registration, projection translation, and type coercion (e.g., Utf8 -> Utf8View).
    ///
    /// `exclude_files`: parquet URIs the tantivy prefilter proved hold no
    /// matching rows (zero-hit covering index). When present, the scan is
    /// restricted to the remaining files via the provider's `FileSelection`
    /// — file-level pruning, computed against THIS `table`'s snapshot so a
    /// concurrent compaction can't shift rows out of the selection.
    #[allow(clippy::too_many_arguments)]
    async fn scan_delta_table(
        &self, table: &DeltaTable, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
        exclude_files: Option<&std::collections::HashSet<String>>, row_selections: Option<&std::collections::HashMap<String, Vec<u64>>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Extract project_id from filters for the provider cache key.
        // Falls back to table_name-only key if absent (multi-project queries).
        let project_id = self.extract_project_id_from_filters(filters).unwrap_or_else(|| self.default_project.clone());
        let cache_key = (project_id, self.table_name.clone());

        table.update_datafusion_session(state).map_err(|e| DataFusionError::External(Box::new(e)))?;

        // File-pruned scans bypass the provider cache (the selection is
        // query-specific). Bail to the unrestricted path unless EVERY
        // surviving live file maps to a table-relative path — a restriction
        // that silently missed an unmappable file would drop its rows.
        let file_selection: Option<Vec<String>> = exclude_files.filter(|e| !e.is_empty()).and_then(|exclude| {
            // Scoped so the (non-Send) file-view iterator drops before any await.
            // `collect::<Option<_>>` reproduces the bail-the-whole-selection
            // semantics: one unmappable URI aborts the restriction entirely.
            table
                .get_file_uris()
                .ok()?
                .filter(|u| u.ends_with(".parquet") && !exclude.contains(u))
                .map(|u| crate::tantivy_index::service::parquet_rel_of_uri(&u).map(str::to_string))
                .collect::<Option<Vec<String>>>()
        });
        // Row-selection pushdown: per-file matching ordinals keyed by rel path.
        // Purely narrowing — files without an entry scan normally — so unlike
        // `file_selection` an unmappable URI just drops that file's selection.
        let ordinal_selections: std::collections::HashMap<String, Vec<u64>> = row_selections
            .into_iter()
            .flatten()
            .filter_map(|(uri, ords)| Some((crate::tantivy_index::service::parquet_rel_of_uri(uri)?.to_string(), ords.clone())))
            .collect();
        if file_selection.is_some() || !ordinal_selections.is_empty() {
            use deltalake::delta_datafusion::{FileSelection, MissingSelectedFilePolicy};
            if let Some(sel) = &file_selection {
                debug!(
                    "tantivy file pruning: {}/{} scanning {} files (excluded {})",
                    cache_key.0,
                    self.table_name,
                    sel.len(),
                    exclude_files.map_or(0, |e| e.len())
                );
            }
            let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
            let mut builder = table.table_provider();
            if let Some(selected) = file_selection {
                builder = builder.with_file_selection(FileSelection::from_file_paths(selected).with_missing_file_policy(MissingSelectedFilePolicy::Ignore));
            }
            if !ordinal_selections.is_empty() {
                debug!("tantivy row selection: {}/{} selections for {} files", cache_key.0, self.table_name, ordinal_selections.len());
                builder = builder.with_row_ordinal_selections(ordinal_selections);
            }
            if let Some(ss) = session_state {
                builder = builder.with_session(Arc::new(ss));
            }
            let provider: Arc<dyn TableProvider> = Arc::new(builder.build().await.map_err(|e| DataFusionError::External(Box::new(e)))?);
            return self.scan_via_provider(provider, state, projection, filters, limit).await;
        }

        // Per-(project,table) provider cache: only rebuild when the Delta
        // snapshot version changes. Provider construction is parameter-
        // independent so the cached value is correct for every query at
        // the same version. Measured: ~30 ms p95 of pure provider-build
        // overhead per query under load before this cache. Cache hits
        // skip the whole `table.table_provider().with_session(...).await`
        // chain.
        let current_version = table.version().unwrap_or(0);
        // Resolve or install a OnceCell for this (key, version). The DashMap
        // shard write-lock spans three operations: the `or_insert_with` (a
        // single hash + slot write on miss, a hash on hit), the
        // `entry.0 != current_version` compare, and the optional in-place
        // tuple replacement. All three are O(1) field accesses with no IO,
        // so the lock window stays in the tens of nanoseconds on the steady
        // path. The expensive provider build runs OUTSIDE the lock, while
        // concurrent tasks all clone the same cell Arc and await its single
        // init.
        //
        // The `entry.0 != current_version` branch serialises the readers of
        // the *same* (project, table) when a new snapshot lands: each
        // thread grabs the per-shard write lock just long enough to replace
        // the stale cell with a fresh one. At our flush cadence (seconds
        // apart per project, single-digit-per-second under heavy load) the
        // serialisation window is microseconds — meaningful only if a
        // version-change burst races with hundreds of concurrent readers,
        // which doesn't happen in our workload. If that pattern ever
        // emerges, prefer a CAS on an `Arc<AtomicU64>` version cell read
        // outside the DashMap lock.
        // Optimistic read path: under 300+ concurrent readers, the prior
        // `entry()`-on-every-call took a per-shard WRITE lock and serialised
        // every cache hit hashing to the same shard. The read-only `get()`
        // takes a per-shard READ lock, so concurrent hits don't block each
        // other. We only take the write path on miss or version mismatch —
        // events that happen seconds apart per project, not per query.
        let ttl = self.database.config.cache.provider_cache_ttl();
        //
        // Lookup is by EXACT version against the key's small recent-version
        // ring (see `PROVIDER_VERSION_RETENTION`): a query always gets the
        // provider matching the snapshot version its own resolved handle
        // reports, never an older retained one. Retention only means the
        // provider for v=N survives a bump to v=N+1, so a task still holding a
        // v=N handle — and a re-bump back through cached versions under
        // concurrent `update_state` — hits instead of rebuilding the snapshot.
        let read_hit = self.database.delta_provider_cache.get(&cache_key).and_then(|entry| entry.get(current_version, ttl));
        let (cell, was_fresh_cell, brand_new_entry) = if let Some(c) = read_hit {
            (c, false, false)
        } else {
            // Eviction is deliberately miss-only: scanning the whole map on
            // every warm dashboard request would cost more than the provider
            // construction this cache removes.
            self.database.trim_delta_provider_cache();
            // Miss / stale — take the write path. Re-check after acquiring
            // the entry lock since another thread may have populated it
            // between our get() and entry() (DashMap doesn't upgrade locks).
            let entry = self.database.delta_provider_cache.entry(cache_key.clone());
            let brand_new = matches!(entry, dashmap::Entry::Vacant(_));
            let mut e = entry.or_default();
            // "Hit" = a cell already existed at this version when we found it.
            // Miss covers both "never seen" and "expired/absent version".
            match e.get(current_version, ttl) {
                Some(c) => (c, false, brand_new),
                None => (e.install(current_version, ttl), true, brand_new),
            }
        };
        if was_fresh_cell || !cell.initialized() {
            self.database.scan_metrics.provider_cache_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        } else {
            self.database.scan_metrics.provider_cache_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        // Soft-limit warning on the brand-new-entry path — mirrors the
        // fast_resolve_cache logic. Threshold-multiple cadence keeps log
        // volume tracking tenant growth, not query rate.
        if brand_new_entry {
            let size = self.database.delta_provider_cache.len();
            if size >= CACHE_SOFT_LIMIT_WARN && size.is_multiple_of(CACHE_SOFT_LIMIT_WARN) {
                tracing::warn!(
                    target = "table_caches",
                    provider_cache_keys = size,
                    threshold = CACHE_SOFT_LIMIT_WARN,
                    "delta_provider_cache crossed soft limit (no eviction by design). Watch scan.provider_cache_entries in timefusion_stats."
                );
            }
        }
        // Bounded staleness: a task that captured the v=N cell before a
        // concurrent flush bumped the DashMap entry to v=N+1 will still
        // complete its query against the v=N provider it awaited. That
        // single query returns pre-flush data. Subsequent queries observe
        // the new v=N+1 cell. Acceptable for append-only OLAP: the window
        // is one query, and a few-second-old reading is the expected
        // semantics of the user-provided MemBuffer/Delta split anyway.
        // Eagerly checking version after the await would just trade this
        // for the original per-query rebuild cost (the 30 ms problem this
        // cache exists to solve).
        let provider = cell
            .get_or_try_init(|| async {
                let started = std::time::Instant::now();
                let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
                // Build the delta-rs table provider with our session so its scan
                // inherits `schema_force_view_types=false` (set in
                // `create_session_context`). delta-rs's default is `true` (BinaryView),
                // which mismatches our Binary-typed MemBuffer at the union and
                // panics in physical planning.
                // Opt into parquet pushdown even under Deletion Vectors: this is a
                // READ-ONLY scan, so it's safe (unlike DV write ops, which need row
                // positions preserved). Reclaims row-group/page pruning that DV
                // otherwise disables table-wide — the recent-window dashboard lever.
                let result = if let Some(ss) = session_state {
                    table.table_provider().with_session(Arc::new(ss)).with_pushdown_with_deletion_vectors(true).await
                } else {
                    table.table_provider().with_pushdown_with_deletion_vectors(true).await
                };
                self.database.scan_metrics.provider_build_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.database.scan_metrics.provider_build_us_total.fetch_add(started.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
                result.map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .await?
            .clone();
        // Abandoned-build detection: if the key's version ring no longer holds
        // the cell we built into, our work is wasted. With version retention a
        // plain version bump no longer abandons the build — only churn deeper
        // than `PROVIDER_VERSION_RETENTION` (or a TTL/capacity eviction) does,
        // which makes a non-zero count a sharper signal of pathological churn.
        if let Some(current_entry) = self.database.delta_provider_cache.get(&cache_key)
            && !current_entry.holds(&cell)
        {
            self.database.scan_metrics.provider_build_abandoned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        self.scan_via_provider(provider, state, projection, filters, limit).await
    }

    /// Shared tail of the Delta scan: projection-index translation into the
    /// provider's schema, the provider scan itself, and type coercion.
    async fn scan_via_provider(
        &self, provider: Arc<dyn TableProvider>, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Translate projection indices from our schema to delta table's schema.
        // DataFusion passes indices based on ProjectRoutingTable.schema, but the
        // delta table provider expects indices based on its own schema.
        let delta_schema = provider.schema();
        let translated_projection = projection.map(|proj| {
            proj.iter()
                .filter_map(|&idx| {
                    let col_name = self.schema.field(idx).name();
                    delta_schema.fields().iter().position(|f| f.name() == col_name).or_else(|| {
                        warn!("Column '{}' requested in projection but not found in Delta schema for table '{}'", col_name, self.table_name);
                        None
                    })
                })
                .collect::<Vec<_>>()
        });

        let started = std::time::Instant::now();
        let delta_plan = provider.scan(state, translated_projection.as_ref(), filters, limit).await;
        self.database.scan_metrics.provider_scan_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.database.scan_metrics.provider_scan_us_total.fetch_add(started.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
        let delta_plan = delta_plan?;

        // Determine target schema based on projection
        let target_schema = match projection {
            Some(proj) => Arc::new(arrow_schema::Schema::new(proj.iter().map(|&idx| self.schema.field(idx).clone()).collect::<Vec<_>>())),
            None => self.schema.clone(),
        };

        let coerced = Self::coerce_plan_to_schema(delta_plan, &target_schema)?;
        Ok(self.gate_if_wide(coerced, filters))
    }

    /// How far back a scan reaches (`now - min_ts`), in micros. `None` = no
    /// lower time bound, i.e. infinitely deep. Depth, not raw window width, so
    /// the hot one-sided `>= now()-1h` dashboard (whose max is open-ended)
    /// reads as shallow while a `[30d ago, 29d ago]` history slice does not.
    fn scan_lookback_micros(&self, filters: &[Expr]) -> Option<i64> {
        match self.extract_time_range_from_filters(filters) {
            Some((min, _)) if min != i64::MIN => Some(crate::clock::now_micros().saturating_sub(min)),
            _ => None,
        }
    }

    /// Wrap a "wide" Delta scan — one reaching further back than the configured
    /// lookback, or with no lower time bound at all, where a one-sided
    /// `timestamp >= cutoff` can't prune files past the date cut and every
    /// file's row groups are fully decoded — so its Parquet decoding draws from
    /// the shared `heavy_scan_sem`, bounding concurrent decode heap across all
    /// queries.
    fn gate_if_wide(&self, plan: Arc<dyn ExecutionPlan>, filters: &[Expr]) -> Arc<dyn ExecutionPlan> {
        let depth = self.scan_lookback_micros(filters);
        let deeper_than = |micros: i64| depth.is_none_or(|d| d > micros);
        let mem = &self.database.config.memory;
        if !deeper_than((mem.timefusion_wide_scan_lookback_hours as i64).saturating_mul(3_600_000_000)) {
            return plan;
        }
        // Depth is only a proxy for decode heap, and pruning breaks the proxy: a
        // deep query on a well-pruned partition selects one file and 8 KB, yet
        // paid ~40s queued behind this shared gate in prod (2026-08-01, 115m =
        // 255ms vs 125m = 40-57s for the identical result). Refine with the work
        // the plan ACTUALLY selected, which is known here because pruning has
        // already run. Only ever *releases* a scan the depth rule would gate —
        // nothing becomes newly gated — so the guard's ceiling is unchanged.
        // `None` = no readable file groups, so fall back to depth alone.
        if let Some((files, bytes)) = selected_file_work(&plan)
            && files <= mem.timefusion_wide_scan_max_files
            && bytes <= mem.timefusion_wide_scan_max_mb.saturating_mul(1 << 20)
        {
            return plan;
        }
        // Two thresholds over one measure, not two notions of "wide": the gate
        // bounds decode HEAP and must fire early (hours), while the cache
        // bypass gives up cache population and must NOT fire on a merely-widish
        // dashboard that will be re-read — hence its own, higher, knob.
        let bypass_cache = self.database.config.cache.cache_bypass_scan_micros().is_some_and(deeper_than);
        Arc::new(GatedScanExec::new(
            plan,
            self.database.heavy_scan_sem.clone(),
            Some(self.database.scan_metrics.clone()),
            bypass_cache,
            mem.timefusion_max_concurrent_scan_readers as u32,
        ))
    }

    /// The lead sort key that makes `DedupExec`'s keep-greatest engage: the
    /// table's first declared sorting column, but only when the table declares a
    /// `dedup_tiebreak` AND that column is itself a dedup key of an i64-backed
    /// type. That is `read_dedup::detect_bound`'s exact contract — equal dedup
    /// keys then share the bound value, so all versions of a row live in one
    /// contiguous run and the operator can emit without buffering the scan.
    ///
    /// One column, not the whole declared sort order: it is all the operator
    /// reads, and it keeps the sort injected over the in-memory legs as cheap as
    /// it can be. `None` (no tiebreak, or the sort key isn't a dedup key) leaves
    /// the plan exactly as it was before merge-on-read.
    fn keep_greatest_ordering(table: &crate::schema_loader::TableSchema, leg_schema: &SchemaRef) -> Option<datafusion::physical_expr::LexOrdering> {
        use datafusion::{
            arrow::{compute::SortOptions, datatypes::DataType},
            physical_expr::{LexOrdering, PhysicalSortExpr},
        };
        table.dedup_tiebreak.as_ref()?;
        let sc = table.sorting_columns.first()?;
        if !table.dedup_keys.iter().any(|k| k == &sc.name) {
            return None;
        }
        let idx = leg_schema.index_of(&sc.name).ok()?;
        if !matches!(leg_schema.field(idx).data_type(), DataType::Int64 | DataType::Timestamp(..)) {
            return None;
        }
        let opts = SortOptions { descending: sc.descending, nulls_first: sc.nulls_first };
        LexOrdering::new(vec![PhysicalSortExpr::new(Arc::new(PhysicalColumn::new(&sc.name, idx)), opts)])
    }

    /// On a `version_append` table, the columns an UPDATE can change — i.e.
    /// everything except the dedup keys and the partition columns, which
    /// identify the row and so are the same in every version of it. `None` for
    /// a table that appends no versions (nothing is version-mutable there).
    ///
    /// A predicate on one of these must NEVER reach a scan leg. Applied at the
    /// source it selects rows by a value that may belong to a SUPERSEDED
    /// version, AND it removes the newer version from `DedupExec`'s input — so
    /// keep-greatest returns the stale row and it then passes the same filter
    /// above. `WHERE status_code = 'OK'` matched a row already updated to
    /// 'ERROR' (2026-08-02, `integration::test_update_operations`), which for an
    /// UPDATE means writing a new version of a row the statement never matched.
    ///
    /// `Inexact` is NOT sufficient: re-applying the filter above the scan
    /// cannot recover a version the source already dropped. It has to be
    /// `Unsupported` so the leg reads every version and the filter runs above
    /// `DedupExec`. This generalizes [`Self::references_tombstone`], which is
    /// this exact bug for one specific column.
    fn version_mutable_columns(table_name: &str) -> Option<std::collections::HashSet<String>> {
        let schema = crate::schema_loader::get_schema(table_name).filter(|s| s.version_append)?;
        let immutable: std::collections::HashSet<&str> =
            schema.dedup_keys.iter().map(String::as_str).chain(schema.partitions.iter().map(String::as_str)).collect();
        Some(schema.schema_ref().fields().iter().map(|f| f.name().clone()).filter(|n| !immutable.contains(n.as_str())).collect())
    }

    /// Does `f` mention the table's tombstone marker? Such a predicate must
    /// NEVER reach a scan leg. Applied at the source — `MemBuffer::query`'s
    /// `compile_filter_conjunction`, or the Delta kernel — it would drop the
    /// tombstone row *before* the dedup, leaving the older live version to win
    /// keep-greatest: a deleted row silently resurrects, with no error anywhere.
    /// Reported `Unsupported` so DataFusion keeps its own `FilterExec` above the
    /// whole scan (above `DedupExec` and the tombstone filter), and stripped
    /// again in `scan` so a filter arriving by any other route still can't be
    /// pushed into a leg. Both together, because someone will try to optimize
    /// this downward later.
    pub(crate) fn references_tombstone(table_name: &str, f: &Expr) -> bool {
        crate::schema_loader::get_schema(table_name).and_then(|s| s.tombstone_column.as_deref()).is_some_and(|t| f.column_refs().iter().any(|c| c.name == t))
    }

    /// Drop rows whose WINNING version is a tombstone (merge-on-read `DELETE`).
    ///
    /// Sits ABOVE the dedup, deliberately: the tombstone must first beat the
    /// older live version of its key on `dedup_tiebreak`, and only then remove
    /// the row. Filtering below the dedup would delete the tombstone and let the
    /// stale live version survive — the row would come back.
    ///
    /// `marker IS DISTINCT FROM true` — NULL and `false` are both live, so the
    /// column can exist (all-NULL) on a table nothing has ever tombstoned with
    /// no effect on any result. `keep` strips the marker back off when it was
    /// projected in only for this filter.
    fn filter_tombstones(plan: Arc<dyn ExecutionPlan>, marker: &str, keep: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        use datafusion::{
            physical_expr::{PhysicalExpr, expressions::binary},
            physical_plan::filter::FilterExec,
        };
        let schema = plan.schema();
        let Ok(idx) = schema.index_of(marker) else { return Ok(plan) };
        let live = binary(
            Arc::new(PhysicalColumn::new(marker, idx)),
            Operator::IsDistinctFrom,
            datafusion::physical_expr::expressions::lit(ScalarValue::Boolean(Some(true))),
            &schema,
        )?;
        let filtered = Arc::new(FilterExec::try_new(live, plan)?) as Arc<dyn ExecutionPlan>;
        let Some(k) = keep.filter(|&k| k < schema.fields().len()) else { return Ok(filtered) };
        let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> =
            (0..k).map(|i| (Arc::new(PhysicalColumn::new(schema.field(i).name(), i)) as Arc<dyn PhysicalExpr>, schema.field(i).name().clone())).collect();
        Ok(Arc::new(ProjectionExec::try_new(exprs, filtered)?))
    }

    /// Wrap an execution plan with type coercion if the output schema doesn't match the target.
    /// This handles cases like Delta returning Utf8 when we expect Utf8View.
    fn coerce_plan_to_schema(plan: Arc<dyn ExecutionPlan>, target_schema: &SchemaRef) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan_schema = plan.schema();
        if plan_schema.fields().len() != target_schema.fields().len() {
            return Ok(plan);
        }

        // Variant columns are an Arrow ExtensionType whose inner storage may
        // be either Struct{Binary,Binary} or Struct{BinaryView,BinaryView}
        // depending on which session built the scan plan. The
        // parquet-variant-compute kernel and our UDFs accept both, so a
        // per-row CAST(BinaryView→Binary) here is pure overhead — it was
        // costing ~4× on `SELECT payload`. Skip the coercion for any field
        // whose target type is Variant; let the kernel handle the layout.
        let differs = |plan_field: &arrow_schema::Field, target_field: &arrow_schema::Field| -> bool {
            if plan_field.data_type() == target_field.data_type() {
                return false;
            }
            !crate::schema_loader::is_variant_type(target_field.data_type())
        };

        let needs_coercion = plan_schema.fields().iter().zip(target_schema.fields()).any(|(plan_field, target_field)| differs(plan_field, target_field));

        if !needs_coercion {
            return Ok(plan);
        }

        let cast_exprs: Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> = plan_schema
            .fields()
            .iter()
            .enumerate()
            .zip(target_schema.fields())
            .map(|((idx, plan_field), target_field)| {
                let col_expr = Arc::new(PhysicalColumn::new(plan_field.name(), idx)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>;
                let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                    if differs(plan_field, target_field) { Arc::new(CastExpr::new(col_expr, target_field.data_type().clone(), None)) } else { col_expr };
                (expr, target_field.name().clone())
            })
            .collect();

        Ok(Arc::new(ProjectionExec::try_new(cast_exprs, plan)?))
    }

    /// Read-side coverage gate for the tantivy prefilter. Returns `true` iff
    /// every live Delta file whose `date=` partition overlaps the query window
    /// is present in `covered` (the union of successful indexes' covered files).
    ///
    /// Sound at day granularity even though search time-prunes at microsecond
    /// granularity: any divergence (a file the gate counts in-window but whose
    /// covering index search pruned, or an uncovered out-of-window file) only
    /// concerns rows the query's own timestamp filter already excludes. If the
    /// table can't be resolved, returns `false` (fail safe — skip the prefilter
    /// rather than risk dropping rows).
    async fn prefilter_coverage_complete(&self, project_id: &str, window: Option<(i64, i64)>, covered: &std::collections::HashSet<String>) -> bool {
        let Ok(table_ref) = self.database.resolve_table(project_id, &self.table_name).await else {
            return false;
        };
        let Ok(uris) = ({ table_ref.read().await.get_file_uris().map(|it| it.collect::<Vec<String>>()) }) else {
            return false;
        };
        let (lo, hi) = window.unwrap_or((i64::MIN, i64::MAX));
        uris.into_iter().filter(|u| u.ends_with(".parquet") && uri_date_in_window(u, lo, hi)).all(|u| covered.contains(&u))
    }

    /// Read-side dedup skip (`timefusion_read_dedup_skip_swept`): true iff
    /// every (project, date) partition in the query window carries a clean
    /// fingerprint that STILL matches the live file set — i.e. a sweep pass
    /// proved it duplicate-free and nothing has committed to it since. Only
    /// consulted on Delta-only paths (mem∪delta overlap needs DedupExec).
    ///
    /// NEVER granted on a `version_append` table. There, `DedupExec` is not an
    /// optimization that a clean sweep can prove unnecessary — it IS the
    /// mechanism that resolves versions, and the sweep's own "duplicate-free"
    /// verdict is about duplicate KEYS, which is exactly what merge-on-read
    /// creates on purpose. Skipping it serves every superseded version and
    /// every tombstoned row alongside the current one: an UPDATE reads back
    /// pre-update and a DELETE does not delete (caught by
    /// `buffer_consistency_test::test_{update,delete}::immediate` at the
    /// 2026-08-02 flip).
    fn dedup_skip_allowed(&self, table: &DeltaTable, project_id: &str, window: Option<(i64, i64)>, dedup_keys: &[String]) -> bool {
        if dedup_keys.is_empty() || !self.database.config.maintenance.timefusion_read_dedup_skip_swept {
            return false;
        }
        if crate::schema_loader::get_schema(&self.table_name).is_some_and(|s| s.version_append) {
            return false;
        }
        let Some(window) = window else { return false };
        self.database.dedup_window_clean(table, project_id, &self.table_name, window)
    }

    /// Extract time range (min, max) from query filters.
    /// Returns None if no time constraints found.
    fn extract_time_range_from_filters(&self, filters: &[Expr]) -> Option<(i64, i64)> {
        use crate::optimizers::{is_col_through_cast, swap_comparison};
        // Literal bound → microseconds. Strict (no Cast unwrap) so a cast-to-a-
        // different-unit literal yields None (→ widest window) rather than a
        // wrong-narrow one that could prune indexes holding matching rows.
        fn literal_micros(e: &Expr) -> Option<i64> {
            match e {
                Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts), _), _) => Some(*ts),
                Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _), _) => Some(*ts / 1000),
                Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), _), _) => Some(*ts * 1000),
                Expr::Literal(ScalarValue::TimestampSecond(Some(ts), _), _) => Some(*ts * 1_000_000),
                _ => None,
            }
        }

        let (min_ts, max_ts) = filters.iter().fold((None::<i64>, None::<i64>), |acc @ (min_ts, max_ts), filter| {
            let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else { return acc };
            // Accept `timestamp <op> lit`, `lit <op> timestamp` (operands
            // reversed → flip the comparison), and a Cast-wrapped column.
            let (ts_value, op) = if is_col_through_cast(left, "timestamp") {
                (literal_micros(right), *op)
            } else if is_col_through_cast(right, "timestamp") {
                (literal_micros(left), swap_comparison(*op))
            } else {
                return acc;
            };
            let Some(ts) = ts_value else { return acc };
            match op {
                Operator::Gt | Operator::GtEq => (Some(min_ts.map_or(ts, |m| m.max(ts))), max_ts),
                Operator::Lt | Operator::LtEq => (min_ts, Some(max_ts.map_or(ts, |m| m.min(ts)))),
                Operator::Eq => (Some(ts), Some(ts)),
                _ => acc,
            }
        });

        (min_ts.is_some() || max_ts.is_some()).then(|| (min_ts.unwrap_or(i64::MIN), max_ts.unwrap_or(i64::MAX)))
    }
}

/// What [`Database::migrate_add_columns`] did.
pub struct ColumnMigrationReport {
    pub stored_before: usize,
    pub stored_after: usize,
    /// Columns actually added; empty when the stored schema already had them.
    pub added: Vec<String>,
}

impl Database {
    /// Evolve a live table's STORED Delta schema to include new nullable
    /// columns, without touching any YAML.
    ///
    /// A shipped table cannot gain a column by editing its YAML: the YAML is one
    /// schema, and each Delta table's transaction log holds another that was
    /// fixed at creation time. Widening only the YAML makes the write path build
    /// wider batches than storage declares — prod 7d68f01: `number of
    /// columns(94) must match number of fields(92)`, 268 flush failures and
    /// rejected INSERTs within minutes. So storage must be widened FIRST, by
    /// this, and only then may the YAML declare the columns.
    ///
    /// Mechanism: commit a ZERO-ROW batch carrying only the new columns under
    /// `SchemaMode::Merge`. That unions them into the stored schema while
    /// writing no data and rewriting no existing row. Columns already present
    /// are skipped, so a half-finished run is simply re-run.
    pub async fn migrate_add_columns(&self, table_name: &str, adds: &[(String, String)], dry_run: bool) -> Result<ColumnMigrationReport> {
        use arrow::array::{ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, TimeUnit};

        let table_ref = self.get_or_create_unified_table(table_name).await?;
        let stored: Vec<String> = {
            let t = table_ref.read().await;
            t.snapshot()?.schema().fields().map(|f| f.name().to_string()).collect()
        };
        let missing: Vec<&(String, String)> = adds.iter().filter(|(n, _)| !stored.contains(n)).collect();
        let report = |after: usize, added: Vec<String>| ColumnMigrationReport { stored_before: stored.len(), stored_after: after, added };
        if missing.is_empty() || dry_run {
            return Ok(report(stored.len(), missing.iter().map(|(n, _)| n.clone()).collect()));
        }

        let (fields, columns): (Vec<Field>, Vec<ArrayRef>) = missing
            .iter()
            .map(|(n, t)| match t.as_str() {
                "timestamp" => Ok((
                    Field::new(n, DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
                    Arc::new(TimestampMicrosecondArray::from(Vec::<i64>::new()).with_timezone("UTC")) as ArrayRef,
                )),
                "boolean" => Ok((Field::new(n, DataType::Boolean, true), Arc::new(BooleanArray::from(Vec::<bool>::new())) as ArrayRef)),
                other => anyhow::bail!("unsupported column type '{other}' (expected timestamp|boolean)"),
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)?;

        let t = { table_ref.read().await.clone() };
        t.write(vec![batch])
            .with_save_mode(deltalake::protocol::SaveMode::Append)
            .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
            .await
            .map_err(|e| anyhow::anyhow!("schema-merge commit failed: {e}"))?;

        // Re-read from the log rather than trusting the write: the whole point
        // is that storage, not our intent, carries the columns.
        let after: Vec<String> = {
            let mut g = table_ref.write().await;
            g.load().await?;
            g.snapshot()?.schema().fields().map(|f| f.name().to_string()).collect()
        };
        let added: Vec<String> = missing.iter().map(|(n, _)| n.clone()).collect();
        let still: Vec<&String> = added.iter().filter(|n| !after.contains(n)).collect();
        anyhow::ensure!(still.is_empty(), "migration committed but columns are still absent from the stored schema: {still:?}");
        Ok(report(after.len(), added))
    }
}

/// Files and bytes a scan will actually open, read off the plan's file groups
/// AFTER pruning. `None` when the plan carries no file scan (so the caller must
/// not read "no files" as "no work").
fn selected_file_work(plan: &Arc<dyn ExecutionPlan>) -> Option<(usize, u64)> {
    if let Some(src) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<DataSourceExec>()
        && let Some(conf) = (src.data_source().as_ref() as &dyn std::any::Any).downcast_ref::<FileScanConfig>()
    {
        let files = conf.file_groups.iter().flat_map(|g| g.files());
        return Some(files.fold((0, 0), |(n, b), f| (n + 1, b + f.object_meta.size)));
    }
    // Fold children so a `None` child (a leg with no file scan, e.g. the
    // in-memory leg) doesn't erase a sibling's real work.
    plan.children().into_iter().filter_map(selected_file_work).reduce(|(n, b), (n2, b2)| (n + n2, b + b2))
}

/// Decode-admission pressure valve: how many of the wide-scan semaphore's
/// `total` permits one decode poll must claim right now. 1 under normal
/// pressure (full concurrency); a quarter of the pool from 88% of the cgroup
/// limit; the whole pool (fully serialized decodes) from 95%. Decode heap is
/// the one large consumer no DataFusion pool tracks, so near the OOM line the
/// only lever is concurrency: 16 concurrent ~150MB-batch decodes were exactly
/// the burst that outran jemalloc purge + memcg reclaim in the 2026-08-01/02
/// hourly-OOM regime (70 kills / 3 days). Queries degrade to queued-but-alive
/// instead of the whole process dying and paying a cold-cache restart spiral.
/// The pressure number is memcg-charged usage minus reclaimable file cache —
/// the same measure the maintenance wave brake acts on — sampled at most every
/// 250ms (decode polls are ~ms-scale, so per-poll file reads would be pure
/// overhead).
fn scan_pressure_permits(total: u32) -> u32 {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    static SAMPLED_AT_MS: AtomicU64 = AtomicU64::new(u64::MAX);
    static USAGE_PCT: AtomicU64 = AtomicU64::new(0);
    let now_ms = EPOCH.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64;
    let last = SAMPLED_AT_MS.load(Relaxed);
    if last == u64::MAX || now_ms.saturating_sub(last) >= 250 {
        SAMPLED_AT_MS.store(now_ms, Relaxed);
        let limit = crate::config::try_config().map_or(0, |c| c.derived.memory_limit_bytes);
        let pct = match (process_memory_bytes(), limit) {
            (Some(used), l) if l > 0 => (used * 100 / l) as u64,
            _ => 0,
        };
        let prev = USAGE_PCT.swap(pct, Relaxed);
        // Tier transitions are rare and load-bearing for OOM post-mortems:
        // counters die with the process, the log survives it.
        let (was, now) = (pressure_permit_claim(prev, total), pressure_permit_claim(pct, total));
        if was != now {
            warn!("scan pressure valve: {prev}% -> {pct}% of cgroup limit, decode permit claim {was} -> {now} (of {total})");
        }
    }
    pressure_permit_claim(USAGE_PCT.load(Relaxed), total)
}

/// Tier math for `scan_pressure_permits`, separated for testability.
fn pressure_permit_claim(usage_pct: u64, total: u32) -> u32 {
    match usage_pct {
        p if p >= 95 => total,
        p if p >= 88 => (total / 4).max(1),
        _ => 1,
    }
}

/// Concurrency-gates a wide read scan: each output partition acquires a permit
/// from a shared semaphore around every batch decode, bounding the number of
/// Parquet row groups decoded at once across ALL wide queries. This is the
/// admission guard that keeps a wide-window dashboard (hundreds of files, no
/// page pruning) from OOM-restarting the process — Parquet decode heap is
/// untracked by the DataFusion memory pool, so at full `target_partitions`
/// parallelism a single 7-day query took the box down (prod 2026-07-20).
///
/// Acquisition is PER-BATCH, not per-stream: a permit held for a partition's
/// whole lifetime would deadlock `SortPreservingMergeExec`, which needs a batch
/// from every input partition before it can emit — with fewer permits than
/// partitions, the un-permitted partitions could never produce their first
/// batch. Releasing between batches lets all partitions make forward progress
/// in waves of `permits`.
#[derive(Debug)]
struct GatedScanExec {
    input: Arc<dyn ExecutionPlan>,
    sem: Arc<tokio::sync::Semaphore>,
    properties: Arc<PlanProperties>,
    /// Decode accounting only — this operator never denies on memory.
    metrics: Option<Arc<ScanMetrics>>,
    /// Scan-resistant admission: a scan deep enough to be reading history — not
    /// the hot tail — must not evict the hot tail on its way through. Derived
    /// once, from the caller's filters (see `gate_if_wide`).
    bypass_cache: bool,
    /// Size of `sem`'s pool — `scan_pressure_permits` scales its claim off it
    /// (tokio semaphores don't expose their initial size).
    pool_size: u32,
}

impl GatedScanExec {
    fn new(input: Arc<dyn ExecutionPlan>, sem: Arc<tokio::sync::Semaphore>, metrics: Option<Arc<ScanMetrics>>, bypass_cache: bool, pool_size: u32) -> Self {
        let properties = input.properties().clone();
        Self { input, sem, properties, metrics, bypass_cache, pool_size }
    }
}

impl DisplayAs for GatedScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(f, "GatedScanExec: permits={}", self.sem.available_permits()),
            _ => write!(f, "GatedScanExec"),
        }
    }
}

impl ExecutionPlan for GatedScanExec {
    fn name(&self) -> &'static str {
        "GatedScanExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.sem.clone(), self.metrics.clone(), self.bypass_cache, self.pool_size)))
    }
    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        let schema = inner.schema();
        let sem = self.sem.clone();
        let metrics = self.metrics.clone();
        let bypass = self.bypass_cache;
        let pool_size = self.pool_size;
        // Hold a permit only across each `poll_next` (one batch decode), then
        // release so other partitions/queries can proceed — see type docs.
        // The permit window is also exactly the decode window, which is what
        // makes this the honest place to measure decode heap.
        let gated = futures::stream::unfold(inner, move |mut inner| {
            let sem = sem.clone();
            let metrics = metrics.clone();
            async move {
                // Near the OOM line each poll claims more of the pool,
                // shrinking effective decode concurrency (see
                // `scan_pressure_permits`). `acquire_many` never exceeds the
                // pool size, so progress is guaranteed.
                let want = scan_pressure_permits(pool_size);
                let _permit = sem.acquire_many_owned(want).await.ok()?;
                if let Some(m) = &metrics {
                    m.decode_begin();
                    if want > 1 {
                        m.decode_pressure_throttled.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                // The object-store fetches for this batch happen inside the
                // poll, so the bypass scope covers exactly them. Only paid for
                // when it's actually suppressing.
                let next = match bypass {
                    true => crate::object_store_cache::scan_bypass_scope(true, futures::StreamExt::next(&mut inner)).await,
                    false => futures::StreamExt::next(&mut inner).await,
                };
                if let Some(m) = &metrics {
                    // Size the decoded Arrow, not the compressed parquet.
                    m.decode_end(next.as_ref().and_then(|r| r.as_ref().ok()).map_or(0, |b: &RecordBatch| b.get_array_memory_size() as u64));
                }
                next.map(|item| (item, inner))
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, gated)))
    }
}

// Needed by DataSink
impl DisplayAs for ProjectRoutingTable {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProjectRoutingTable ")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "ProjectRoutingTable ")
            }
        }
    }
}

#[async_trait]
impl DataSink for ProjectRoutingTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    #[instrument(
        name = "datafusion.table.write",
        skip_all,
        fields(
            table.name = %self.table_name,
            operation = "INSERT",
            rows.count = Empty,
            projects.count = Empty,
        )
    )]
    async fn write_all(&self, mut data: SendableRecordBatchStream, _context: &Arc<TaskContext>) -> DFResult<u64> {
        let span = tracing::Span::current();
        let mut total_row_count = 0;
        let mut project_batches: HashMap<String, Vec<RecordBatch>> = HashMap::new();
        let target_schema = self.real_schema();
        // Collect batches, converting Utf8/Utf8View columns into Variant structs where the
        // target schema expects Variant (INSERT path: schema() presented Variant cols as
        // Utf8View, so inbound batches may carry strings), then partition each batch row-wise
        // by project_id — a single batch may carry rows for many projects, each of which
        // lands in its own Delta table.
        while let Some(batch) = data.next().await.transpose()? {
            let batch_rows = batch.num_rows();
            debug!("write_all: received batch with {} rows", batch_rows);
            total_row_count += batch_rows;
            let batch = normalize_timestamp_tz(batch)?;
            let converted = convert_variant_columns(batch, &target_schema)?;
            for (project_id, sub) in partition_batch_by_project(converted, &self.default_project)? {
                project_batches.entry(project_id).or_default().push(sub);
            }
        }

        span.record("rows.count", total_row_count);
        span.record("projects.count", project_batches.len());

        if project_batches.is_empty() {
            return Ok(0);
        }

        // Distinct projects → distinct Delta tables/WAL shards: insert them concurrently,
        // with no cross-project lock contention.
        let writes = project_batches.into_iter().map(|(project_id, batches)| {
            let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
            debug!("write_all: inserting {} batches with {} total rows for project {}", batches.len(), row_count, project_id);
            let insert_span = tracing::trace_span!(parent: &span, "delta_table.insert", project_id = %project_id, rows = row_count);
            async move {
                self.database
                    .insert_records_batch(&project_id, &self.table_name, batches, self.skip_queue, None)
                    .instrument(insert_span)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("Insert error for project {} table {}: {}", project_id, self.table_name, e)))
            }
        });
        futures::future::try_join_all(writes).await?;

        debug!("write_all: completed insertion of {} total rows", total_row_count);
        Ok(total_row_count as u64)
    }
}

#[async_trait]
impl TableProvider for ProjectRoutingTable {
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn schema(&self) -> SchemaRef {
        self.schema()
    }

    async fn insert_into(&self, _state: &dyn Session, input: Arc<dyn ExecutionPlan>, insert_op: InsertOp) -> DFResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            error!("Unsupported insert operation: {:?}", insert_op);
            return not_impl_err!("{insert_op} not implemented for MemoryTable yet");
        }
        // No `logically_equivalent_names_and_types(&input.schema())` check here:
        // `self.schema()` returns the "insert-compatible" (lying) schema where
        // Variant columns appear as Utf8View so VALUES literals type-check.
        // Validating against that shape would reject the real downstream batches
        // (which carry Variant). `write_all` coerces back to Variant before
        // the Delta commit, so the type contract is enforced at the boundary
        // that matters.
        Ok(Arc::new(DataSinkExec::new(input, Arc::new(self.clone()), None)))
    }

    fn supports_filters_pushdown(&self, filter: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Variant columns are Struct(Binary, Binary); the delta-kernel scan cannot
        // evaluate predicates on them ("Predicate references unknown column: <col>").
        // Mark any filter that references a Variant column `Unsupported` so DataFusion
        // applies it via a FilterExec above the scan rather than pushing it into the
        // kernel. (Variant predicates can't prune row groups anyway.)
        let variant_cols: std::collections::HashSet<String> = crate::schema_loader::registry()
            .get(&self.table_name)
            .map(|s| s.schema_ref().fields().iter().filter(|f| crate::schema_loader::is_variant_type(f.data_type())).map(|f| f.name().clone()).collect())
            .unwrap_or_default();
        let mutable = Self::version_mutable_columns(&self.table_name);
        Ok(filter
            .iter()
            .map(|f| {
                if Self::references_tombstone(&self.table_name, f)
                    || (!variant_cols.is_empty() && f.column_refs().iter().any(|c| variant_cols.contains(&c.name)))
                    || mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name)))
                {
                    TableProviderFilterPushDown::Unsupported
                } else if Self::is_exact_pushdown_filter(f) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    #[instrument(
        name = "datafusion.table.scan",
        skip_all,
        fields(
            table.name = %self.table_name,
            table.project_id = Empty,
            scan.filters_count = filters.len(),
            scan.has_limit = limit.is_some(),
            scan.limit = limit.unwrap_or(0),
            scan.has_projection = projection.is_some(),
            scan.uses_mem_buffer = false,
            scan.skipped_delta = false,
            parquet.files = Empty,
            parquet.bytes = Empty,
            parquet.file_ids = Empty,
            parquet.selected_row_groups = Empty,
        )
    )]
    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let span = tracing::Span::current();
        let scan_start = std::time::Instant::now();
        let scan_metrics = self.database.scan_metrics.clone();

        // Apply our custom optimizations to the filters
        // Second line of defence behind `supports_filters_pushdown`: neither a
        // predicate on the tombstone marker (see `references_tombstone` — it
        // resurrects deleted rows silently) nor one on any other
        // version-mutable column (see `version_mutable_columns` — it serves a
        // superseded version) may reach a scan leg, however it arrived here.
        //
        // On a merge-on-read table this stripping constrains the tantivy
        // prefilter too, but only PART of it is unsound. The invariant: leaf
        // pruning below DedupExec commutes with keep-greatest only when the
        // predicate evaluates identically on every version of a key. File
        // exclusion and row selections violate that for mutable columns (a
        // search for a value only the OLD version carries excludes the file
        // holding the NEW one; DedupExec never sees the winner and serves the
        // stale row) — those stay OFF below. The id-set half is sound even
        // for mutable columns: `id` is a dedup key, so `id IN (hits)` admits
        // or drops whole KEYS atomically — every version of a matching id
        // passes, keep-greatest still picks the newest, and a stale-only
        // match is rejected by the above-dedup filter. The coverage gate
        // guarantees the winner's file was searched, so its id is in the set.
        let mutable = Self::version_mutable_columns(&self.table_name);
        let unstripped_filters = filters;
        let leg_safe = |f: &Expr| {
            !Self::references_tombstone(&self.table_name, f) && !mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name)))
        };
        let filters: Vec<Expr> = filters.iter().filter(|f| leg_safe(f)).cloned().collect();
        let optimized_filters = self.apply_time_series_optimizations(&filters)?;

        // Get project_id from filters if possible, otherwise use default
        let project_id = self.extract_project_id_from_filters(&optimized_filters).unwrap_or_else(|| self.default_project.clone());
        span.record("table.project_id", project_id.as_str());

        // Tantivy prefilter. Two independent paths:
        //
        // 1. Delta side — query the sidecar tantivy service, build `id IN
        //    (delta_ids)` and apply it to the Delta scan only. Delta files
        //    contain only flushed data; MemBuffer rows are never here, so
        //    using delta_ids on MemBuffer would drop valid rows.
        //
        // 2. MemBuffer side — `query_partitioned_with_text_match` handles
        //    its own atomic per-bucket prefilter under the bucket lock. The
        //    caller (us) does NOT compute or pass MemBuffer ids — doing so
        //    would re-introduce the race where a concurrent insert lands a
        //    row in the snapshot that isn't in the pre-computed id set.
        // On a MOR table the leg filters had mutable-column predicates
        // stripped, so collect the tree from the UNSTRIPPED filters — the
        // sidecar id-set (the only output allowed below on such tables, see
        // the invariant note above) is sound for them. Non-MOR tables keep
        // the stripped set (identical there anyway).
        let text_match_tree = match mutable.is_some() {
            false => crate::tantivy_index::udf::collect_text_match_tree(&optimized_filters),
            true => crate::tantivy_index::udf::collect_text_match_tree(&self.apply_time_series_optimizations(unstripped_filters)?),
        };
        // Query [lo,hi] timestamp window, shared by the tantivy prefilter (time-
        // prunes the sidecar search + scopes the coverage gate to a needle's
        // window, not every index the project built) and the skip-delta
        // watermark check below.
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);
        let mut tantivy_id_filter: Option<Expr> = None;
        // Files the prefilter proved hold no matches (zero-hit covering
        // index) — excluded from the Delta scan when file pruning is on.
        let mut tantivy_exclude: Option<std::collections::HashSet<String>> = None;
        // Per-file matching row ordinals (row-selection pushdown), for files
        // whose covering index was built in parquet row order.
        let mut tantivy_row_selections: Option<std::collections::HashMap<String, Vec<u64>>> = None;
        if let Some(tree) = text_match_tree.as_ref()
            && let Some(svc) = self.database.tantivy_search()
        {
            use datafusion::logical_expr::{Expr, lit};
            let tcfg = &self.database.config().tantivy;
            let max_hits = tcfg.prefilter_max_hits();
            let min_sel_pct = tcfg.prefilter_min_selectivity_pct() as u64;
            crate::metrics::record_tantivy_prefilter_attempt();

            let mut delta_ids: Option<std::collections::HashSet<String>> = None;
            let mut delta_indexed_rows: u64 = 0;
            let mut delta_covered: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut delta_zero_hit: std::collections::HashSet<String> = std::collections::HashSet::new();
            let mut delta_row_sel: std::collections::HashMap<String, Vec<u64>> = std::collections::HashMap::new();
            let mut delta_field_gap = false;
            let mut delta_any_usable = false;
            let mut abort_reason: Option<&'static str> = None;
            // ONE pass over the in-window index set: the routable predicate
            // tree compiles to a single tantivy BooleanQuery per index
            // (And→Must, Or→Should; `collect_text_match_tree` only emits OR
            // nodes whose every branch is completely covered), hits unioned
            // across indexes (they cover disjoint row sets).
            match svc.search_with_stats(&self.table_name, &project_id, tree, max_hits, query_time_range).await {
                Ok(Some(result)) => {
                    delta_any_usable = true;
                    delta_indexed_rows = result.indexed_rows;
                    delta_covered = result.covered_files;
                    delta_field_gap = result.field_coverage_gap;
                    delta_zero_hit = result.zero_hit_files;
                    delta_row_sel = result.row_selections;
                    delta_ids = Some(result.hits.into_iter().map(|h| h.id).collect());
                }
                Ok(None) => {
                    abort_reason = Some("delta_no_index_or_cap_exceeded");
                }
                Err(e) => {
                    warn!("tantivy search failed for {}/{}: {:#} — falling back to full scan", project_id, self.table_name, e);
                    crate::metrics::record_tantivy_prefilter_error();
                    abort_reason = Some("delta_error");
                }
            }

            if delta_any_usable {
                if let Some(ids) = delta_ids {
                    // No indexed rows = no useful prefilter. Without this guard
                    // we'd emit an empty IN(...) list that zeros the Delta
                    // scan even when matching rows exist there (e.g. data
                    // written directly without triggering an index build).
                    if delta_indexed_rows == 0 {
                        crate::metrics::record_tantivy_prefilter_skipped();
                        debug!("Tantivy prefilter skipped for {}/{}: empty_index", project_id, self.table_name);
                    } else if (ids.len() as u64) * 100 >= delta_indexed_rows * min_sel_pct {
                        // Selectivity cutoff: if the hit set covers most of the
                        // indexed rows, the IN-list won't prune enough to be
                        // worth its planning cost. Bail; original predicate
                        // re-runs as the correctness backstop.
                        crate::metrics::record_tantivy_prefilter_skipped();
                        debug!("Tantivy prefilter skipped for {}/{}: low_selectivity", project_id, self.table_name);
                    } else if delta_field_gap {
                        // An in-window index lacked one of the queried fields
                        // (schema evolution added a tantivy column after it was
                        // built). It can't answer that predicate yet appears
                        // "covered", so the IN-list would drop its rows — skip.
                        crate::metrics::record_tantivy_prefilter_skipped();
                        debug!("Tantivy prefilter skipped for {}/{}: field_coverage_gap", project_id, self.table_name);
                    } else if !self.prefilter_coverage_complete(&project_id, query_time_range, &delta_covered).await {
                        // Coverage gate (correctness): `id IN (hits)` intersects,
                        // so a live file overlapping the window that ISN'T covered
                        // by a successful index would have its matching rows
                        // silently dropped. If any in-window live file is
                        // uncovered (compacted, external write, failed build),
                        // skip the prefilter — the original predicate full-scans.
                        crate::metrics::record_tantivy_prefilter_skipped();
                        debug!("Tantivy prefilter skipped for {}/{}: incomplete_coverage", project_id, self.table_name);
                    } else {
                        crate::metrics::record_tantivy_prefilter_used();
                        tantivy_id_filter = Some(Expr::InList(datafusion::logical_expr::expr::InList {
                            expr: Box::new(datafusion::logical_expr::col("id")),
                            list: ids.into_iter().map(lit).collect(),
                            negated: false,
                        }));
                        // File pruning is only sound once every gate above
                        // passed (coverage complete, no field gap): a
                        // zero-hit covering index then proves its files hold
                        // no matches for the routed predicates. NEVER on a
                        // version_append table — a "hitless" file may hold
                        // the NEWEST version of a key whose match lives only
                        // in an older version (mutable column), and dropping
                        // it below DedupExec serves the stale row. The id-set
                        // above stays sound there (whole-key granularity).
                        if mutable.is_none() {
                            if tcfg.timefusion_tantivy_file_pruning && !delta_zero_hit.is_empty() {
                                tantivy_exclude = Some(delta_zero_hit);
                            }
                            if tcfg.timefusion_tantivy_row_selection && !delta_row_sel.is_empty() {
                                tantivy_row_selections = Some(delta_row_sel);
                            }
                        }
                    }
                }
            } else {
                crate::metrics::record_tantivy_prefilter_skipped();
                if let Some(reason) = abort_reason {
                    debug!("Tantivy prefilter skipped for {}/{}: {}", project_id, self.table_name, reason);
                }
            }
        }

        // Variant binary flows through scans untouched; downstream nodes
        // (variant_get, ->, ->>) consume it directly. JSON serialization
        // happens only at the root projection via VariantSelectRewriter.
        // Metric tags accumulated during the scan. parking_lot::Mutex is
        // Send (Cell isn't) so the async future stays multi-thread-safe;
        // uncontended lock+unlock is sub-100ns so the overhead is dwarfed
        // by the work being measured.
        // Read-side dedup setup (parity plan Defect 2 #1): collapse physical
        // duplicates of dedup-key rows over the routed/pruned union at query
        // time, so COUNT(*) is correct regardless of sweep timing. Augment the
        // pushed projection with any dedup-key columns the query projected away
        // (so DedupExec can see them); `output_projection` then restores the
        // requested columns. No-op when the table declares no dedup_keys.
        let table_schema = crate::schema_loader::get_schema(&self.table_name);
        let dedup_keys: Vec<String> = table_schema.as_ref().map(|s| s.dedup_keys.clone()).unwrap_or_default();
        // The tiebreak rides in with the keys ONLY for merge-on-read tables:
        // DedupExec keeps the GREATEST version per key there
        // (docs/plans/2026-08-01-merge-on-read-dml.md), so it must see the
        // column even when the query projected it away.
        //
        // Gated on `version_append` because pulling it in unconditionally is a
        // pure cost on every other table: keep-greatest cannot engage without
        // the ordered union (also version_append-gated), so the column is read
        // and never used. Measured on prod 2026-07-31: adding
        // `observed_timestamp` to every otel_logs_and_spans scan took
        // count(1h) 14.8s -> 31s and count(3h) 22.9s -> >150s.
        let dedup_tiebreak: Option<String> = table_schema.as_ref().filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone());
        // Merge-on-read DELETE: a tombstone version must reach the filter ABOVE
        // the dedup, so its marker column rides in with the keys and is stripped
        // again afterwards. `None` on every table that declares none.
        let tombstone: Option<String> = table_schema.and_then(|s| s.tombstone_column.clone());
        // Only a `Some` projection over a dedup_keys/tombstone table can hide the
        // columns those mechanisms need: a `None` projection already scans every
        // column and a plain table needs nothing — both fold into the pass-through
        // arm, which also skips the `self.schema()` build (it un-types Variant
        // cols) on the common tables. `tombstone_keep` is the requested width when
        // the marker was projected in purely for the filter (it then occupies one
        // trailing column that the post-filter projection removes).
        let (scan_projection, output_projection, tombstone_keep): (Option<Vec<usize>>, Option<Vec<usize>>, Option<usize>) = match projection {
            Some(p) if !dedup_keys.is_empty() || tombstone.is_some() => {
                let full_schema = self.schema();
                let missing: Vec<usize> = dedup_keys
                    .iter()
                    .chain(dedup_tiebreak.iter())
                    .chain(tombstone.iter())
                    .filter_map(|k| full_schema.index_of(k).ok())
                    .filter(|i| !p.contains(i))
                    .collect();
                if missing.is_empty() {
                    (Some(p.clone()), None, None)
                } else {
                    let mut aug = p.clone();
                    aug.extend(&missing);
                    // Requested columns occupy the first p.len() positions of the augmented output.
                    let mut out: Vec<usize> = (0..p.len()).collect();
                    // The marker alone must survive DedupExec's projection restore.
                    let extra = tombstone.as_ref().and_then(|t| full_schema.index_of(t).ok()).filter(|i| !p.contains(i));
                    if let Some(ti) = extra {
                        out.push(aug.iter().position(|&i| i == ti).expect("just extended with it"));
                    }
                    (Some(aug), Some(out), extra.map(|_| p.len()))
                }
            }
            _ => (projection.cloned(), None, None),
        };
        let projection = scan_projection.as_ref();
        // When DedupExec is active it drops rows AFTER the scan, so a pushed
        // `limit` must NOT truncate the underlying scans — otherwise the deduped
        // result can yield < limit distinct rows even when more exist below the
        // cut, and the outer GlobalLimitExec (which DataFusion keeps) can't
        // recover them. Suppress the per-scan limit; the outer limit still caps.
        // `orig_limit` is restored on Delta-only paths that skip DedupExec. The
        // tombstone filter drops rows after the scan for exactly the same reason,
        // so it suppresses the pushed limit even where dedup doesn't.
        let orig_limit = limit;
        let post_scan_row_drop = !dedup_keys.is_empty() || tombstone.is_some();
        let limit = if post_scan_row_drop { None } else { limit };

        let scan_state = parking_lot::Mutex::new(ScanShape::default());
        // Legs of the mem ∪ hot ∪ delta union, in recency order.
        let wrap_result = |legs: Vec<Arc<dyn ExecutionPlan>>, leg_sortable: Vec<bool>| -> DFResult<Arc<dyn ExecutionPlan>> {
            let shape = *scan_state.lock();
            let us = scan_start.elapsed().as_micros() as u64;
            scan_metrics.record_scan(us, shape.skipped_delta, shape.has_mem, shape.has_delta, shape.fast_resolve_hit);
            let dedup_on = !dedup_keys.is_empty() && !shape.skip_dedup;
            let mut plans = legs;
            // Merge-on-read prerequisite: `DedupExec`'s keep-greatest only engages
            // while its input still declares an ordering on the leading dedup key
            // (that is what makes every version of a key arrive in one run), and
            // `Distribution::SinglePartition` otherwise gets an ordering-erasing
            // `CoalescePartitionsExec`. Sort the in-memory legs up to the Delta
            // leg's declared footer ordering, then merge explicitly.
            //
            // The `SortPreservingMergeExec` is built here rather than left to
            // `EnforceDistribution`: `DedupExec` declares no *required* input
            // ordering, so `EnforceSorting` would delete the injected leg sorts as
            // unnecessary and we would be back to a coalesce. The SPM makes the
            // ordering required, and — being already single-partition —
            // `EnforceDistribution` then adds nothing.
            //
            // Gated on `version_append`: the ordering exists ONLY to let
            // keep-greatest pick between versions of a key, and until the write
            // path appends versions there are none. Ungated it charged every
            // scan of the table a blocking `SortExec` over the mem (≤70min) and
            // hot (≤6h) legs plus a k-way `SortPreservingMergeExec` holding one
            // in-flight batch per Delta partition (48 × the measured 145MB peak
            // batch = the 2026-07-20 OOM shape) for a dormant feature. The
            // mechanism ships inert and activates with the write path it serves.
            let mut merge_req = None;
            if dedup_on
                && table_schema.is_some_and(|t| t.version_append)
                && let Some(req) = table_schema.and_then(|t| Self::keep_greatest_ordering(t, &plans[0].schema()))
            {
                // Per-leg sortability: the DELTA leg is NEVER sortable — under
                // merge-on-read an UPDATE appends the row's ORIGINAL timestamp
                // into a NEW file, files overlap, and the blocking SortExec
                // that "fixes" that exhausted the 27.5GB query pool on prod
                // 2026-08-02 (1h ~13s, 3h timing out). `ordered_children`
                // bails (None) whenever an unsortable leg misses `req`, so a
                // Delta sort is structurally impossible here.
                //
                // The in-memory legs (mem, hot) ARE sortable: their data is
                // already materialized, the sort is bounded and cheap, and a
                // mem-only scan is exactly where a fresh version append lives
                // — all-false left it permanently on unbounded keep-greatest.
                match crate::optimizers::ordered_children(&plans, &req, None, &leg_sortable, false)? {
                    Some(ordered) => {
                        // Only in-memory legs can reach here (Delta is marked
                        // unsortable), so this is cheap — no metric alarm.
                        plans = ordered;
                        merge_req = Some(req);
                    }
                    // `None` is either "every leg already satisfies `req`" (merge
                    // anyway — the legs are still N partitions) or "an unsortable
                    // leg doesn't" (a Delta scan whose footer ordering isn't
                    // declared: bail, keep-greatest stays dormant, keep-first is
                    // still sound and the dedup sweep remains the authority).
                    None => {
                        let all = plans
                            .iter()
                            .map(|p| p.properties().equivalence_properties().ordering_satisfy(req.iter().cloned()))
                            .collect::<DFResult<Vec<_>>>()?;
                        merge_req = all.iter().all(|&s| s).then_some(req);
                    }
                }
            }
            let plan = if plans.len() == 1 { plans.remove(0) } else { UnionExec::try_new(plans)? };
            let plan = match merge_req.clone() {
                Some(req) => Arc::new(datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(req, plan)),
                None => plan,
            };
            let plan = match dedup_on {
                true => Arc::new(
                    crate::read_dedup::DedupExec::with_tiebreak(plan, dedup_keys.clone(), dedup_tiebreak.clone(), output_projection.clone())?
                        // Declaring it REQUIRED is what stops EnforceSorting from
                        // deleting the merge above as unused — see the field docs.
                        .requiring(merge_req.clone()),
                ),
                false => plan,
            };
            match &tombstone {
                Some(marker) => Self::filter_tombstones(plan, marker, tombstone_keep),
                None => Ok(plan),
            }
        };
        let tag_shape = |f: &dyn Fn(&mut ScanShape)| {
            f(&mut scan_state.lock());
        };

        // Check if buffered layer is configured
        let has_layer = self.database.buffered_layer().is_some();
        debug!("ProjectRoutingTable::scan - buffered_layer present: {}, project_id: {}", has_layer, project_id);
        let Some(layer) = self.database.buffered_layer() else {
            // No buffered layer, query Delta directly
            debug!("No buffered layer, querying Delta only");
            let mut delta_only_filters = optimized_filters.clone();
            if let Some(f) = tantivy_id_filter.clone() {
                delta_only_filters.push(f);
            }
            // Skip is only sound when no output-projection restore is needed
            // (an augmented projection minus DedupExec would leak key columns).
            let delta_table = self.database.resolve_table(&project_id, &self.table_name).await?;
            let table = delta_table.read().await;
            // Same guard for the dedup gate and the scan: the fingerprint
            // verdict applies to exactly the snapshot being read.
            let skip_dedup = output_projection.is_none() && self.dedup_skip_allowed(&table, &project_id, query_time_range, &dedup_keys);
            if skip_dedup {
                tag_shape(&|s| s.skip_dedup = true);
            }
            // Restoring the pushed limit is only sound when nothing above the
            // scan drops rows — the tombstone filter does, regardless of dedup.
            let eff_limit = if skip_dedup && tombstone.is_none() { orig_limit } else { limit };
            let plan = self
                .scan_delta_table(&table, state, projection, &delta_only_filters, eff_limit, tantivy_exclude.as_ref(), tantivy_row_selections.as_ref())
                .await?;
            return wrap_result(vec![plan], vec![false]);
        };

        span.record("scan.uses_mem_buffer", true);

        // Skip Delta when the query's lower bound is strictly above the
        // per-table flushed watermark (max row ts ever handed to a Delta
        // commit, floored at boot) — Delta provably holds nothing newer, so
        // MemBuffer alone serves open-ended `WHERE timestamp >= now() - 5m`
        // dashboards. The previous `query_min >= mem_oldest` heuristic was
        // unsound whenever Delta held rows inside MemBuffer's range —
        // force-flushed open buckets, or a newer bucket drained while an
        // older one was stuck after a failed flush — and silently hid those
        // rows (2026-06-11 visibility gap).
        let skip_delta = match query_time_range {
            Some((query_min, _)) => query_min > layer.delta_flushed_watermark(&project_id, &self.table_name),
            None => false,
        };
        // Sticky-empty short-circuit: if no flush has ever committed for this
        // (project, table), Delta is guaranteed empty and we can skip the
        // scan-plan-build cost. Flipped by the flush callback after a
        // successful commit; never flipped back (compaction reduces files but
        // doesn't go to zero in steady state).
        let skip_delta = skip_delta || self.database.delta_scan_can_be_skipped(&project_id, &self.table_name);
        tag_shape(&|s| s.skipped_delta = skip_delta);

        // MemBuffer query. `query_partitioned_with_text_match` handles its
        // own atomic per-bucket prefilter inside the bucket lock — we must
        // NOT prepend `tantivy_id_filter` here (that filter is derived from
        // delta-side IDs only and would drop legitimate MemBuffer rows).
        // On a MOR table the per-bucket ROW prefilter is below DedupExec and
        // the tree may reference mutable columns (it was collected unstripped
        // for the delta id-set) — dropping a stale version's row here while
        // its match-bearing sibling sits in another leg breaks keep-greatest,
        // so the mem leg gets no tree.
        let mem_tree = text_match_tree.as_ref().filter(|_| mutable.is_none());
        let mem_plan_started = std::time::Instant::now();
        let mem_leg = layer.query_partitioned_with_text_match(&project_id, &self.table_name, &optimized_filters, mem_tree).unwrap_or_else(|e| {
            warn!("Failed to query mem buffer: {}", e);
            Default::default()
        });
        scan_metrics.mem_plan_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        scan_metrics.mem_plan_us_total.fetch_add(mem_plan_started.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
        let mem_partitions = mem_leg.partitions;

        // Hot-tier third leg (P1) — see `HotTier::query_partitioned` for the
        // coverage contract. Consulted only when Delta is actually being
        // scanned: `skip_delta` means the window is entirely newer than
        // anything ever flushed, and the hot tier only ever holds flushed
        // (post-commit) data.
        // ...and only when the scan is shallow enough for the tier to be worth
        // its heap. The hot leg is materialized EAGERLY at plan time into a
        // `MemorySourceConfig` — charged to the query pool only at execute
        // time (`HotLegPooledExec`) and outside `GatedScanExec`, which wraps
        // only the Delta plan. A 7d/14d scan's
        // `query_time_range` covers the whole hot window, so it would pull the
        // entire tier into heap to shave a few files off a scan already
        // dominated by thousands; past the retention window the tier is by
        // definition a fraction of the answer. Same depth signal the wide-scan
        // gate uses (`scan_lookback_micros`: now - min_ts, so a one-sided
        // `>= now()-1h` dashboard reads as shallow), thresholded on the tier's
        // own window rather than the gate's — the tier exists for exactly the
        // 1h/3h reads inside it.
        //
        // DEDUP: a non-empty hot leg forces the union path below, which never
        // sets `skip_dedup` — see `HotTier::query_partitioned`'s dedup contract
        // (the hot leg serves pre-dedup rows and relies on `DedupExec`).
        let too_deep = crate::hot_tier::skip_for_lookback(self.scan_lookback_micros(&optimized_filters), layer.hot_tier_retention_micros());
        let mem_ranges = layer.get_bucket_ranges(&project_id, &self.table_name);
        let hot_plan_started = std::time::Instant::now();
        let hot: crate::hot_tier::HotLeg = match skip_delta || too_deep {
            true => Default::default(),
            false => {
                layer
                    .hot_tier()
                    .query_partitioned(&project_id, &self.table_name, query_time_range, &mem_ranges, &optimized_filters, &self.schema, projection)
                    .await
            }
        };
        scan_metrics.hot_plan_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        scan_metrics.hot_plan_us_total.fetch_add(hot_plan_started.elapsed().as_micros() as u64, std::sync::atomic::Ordering::Relaxed);
        let (hot_partitions, hot_ranges, version_gate, hot_sorted, hot_bytes) = (hot.partitions, hot.ranges, hot.version_gate, hot.sorted, hot.bytes);

        // Nothing above Delta to union with: query Delta alone.
        debug!("MemBuffer partitions count: {} for {}/{}", mem_partitions.len(), project_id, self.table_name);
        if mem_partitions.is_empty() && hot_partitions.is_empty() && hot_ranges.is_empty() {
            debug!("No MemBuffer data, querying Delta only for {}/{}", project_id, self.table_name);
            let mut delta_only_filters = optimized_filters.clone();
            if let Some(f) = tantivy_id_filter.clone() {
                delta_only_filters.push(f);
            }
            tag_shape(&|s| s.has_delta = true);
            let delta_table = self.database.resolve_table(&project_id, &self.table_name).await?;
            let table = delta_table.read().await;
            // Same guard for the dedup gate and the scan (see branch above).
            let skip_dedup = output_projection.is_none() && self.dedup_skip_allowed(&table, &project_id, query_time_range, &dedup_keys);
            if skip_dedup {
                tag_shape(&|s| s.skip_dedup = true);
            }
            // Restoring the pushed limit is only sound when nothing above the
            // scan drops rows — the tombstone filter does, regardless of dedup.
            let eff_limit = if skip_dedup && tombstone.is_none() { orig_limit } else { limit };
            let plan = self
                .scan_delta_table(&table, state, projection, &delta_only_filters, eff_limit, tantivy_exclude.as_ref(), tantivy_row_selections.as_ref())
                .await?;
            return wrap_result(vec![plan], vec![false]);
        }

        // Create MemorySourceConfig with multiple partitions for parallel execution
        let mem_plan = match mem_partitions.is_empty() {
            true => None,
            false => {
                tag_shape(&|s| s.has_mem = true);
                Some(self.create_memory_exec(&mem_partitions, projection, mem_leg.sorted)?)
            }
        };

        // If we can skip Delta, return mem plan directly (the hot leg is empty
        // by construction on this path — see above).
        if let Some(mem_plan) = mem_plan.clone().filter(|_| skip_delta) {
            span.record("scan.skipped_delta", true);
            debug!("Skipping Delta scan - query time range entirely within MemBuffer for {}/{}", project_id, self.table_name);
            return wrap_result(vec![mem_plan], vec![true]);
        }

        // Build Delta filters with per-bucket exclusion.
        //
        // The MemBuffer / Delta union must not double-count rows: a sealed
        // bucket's rows can briefly sit in both stores during its normal
        // commit-then-drain flush, so Delta excludes the row ranges
        // MemBuffer currently holds. `get_bucket_ranges` returns exactly
        // the ranges where MemBuffer is authoritative — actual per-bucket
        // [min, max] row ranges, skipping the current (open) bucket and any
        // force-flushed bucket, whose windows legitimately hold disjoint
        // row sets in both stores (force-flush removes rows from MemBuffer
        // *before* committing). Excluding those windows hid the Delta share
        // for hours when the flush pipeline backed up (2026-06-11).
        //
        // The hot tier's included files are authoritative for their own row
        // ranges in exactly the same sense, so their ranges join the exclusion
        // list — mem ∪ hot ∪ delta then covers each timestamp window once.
        // Delta excludes the UNION of those ranges, and consecutive sealed
        // buckets are contiguous, so merging first collapses what would be one
        // conjunct per bucket (~36 for a 6h tier) into typically one.
        //
        // MERGE-ON-READ: on a `version_append` table an UPDATE appends a new
        // version of the row carrying its ORIGINAL timestamp, so the newer
        // version lands in Delta *inside* one of these ranges and the exclusion
        // above would hide it — serving the pre-update row forever. Each
        // conjunct is therefore weakened with `OR stamp > gate`: at or below the
        // gate the in-memory/hot leg already holds the newest version of every
        // row in the window, above it only Delta does. Applied to the merged
        // (mem ∪ hot) ranges rather than the hot ones alone because weakening is
        // safe in one direction only — an over-admitted row is a duplicate
        // `DedupExec` collapses, an under-admitted one is a stale read — and the
        // union path never grants `skip_dedup`.
        let mut delta_filters = optimized_filters.clone();
        let ts_col = || Box::new(col("timestamp"));
        let ts_lit = |t: i64| Box::new(lit(ScalarValue::TimestampMicrosecond(Some(t), Some("UTC".into()))));
        let version_col = table_schema.as_ref().filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone());
        for (start, end) in crate::mem_buffer::merge_ranges([mem_ranges, hot_ranges].concat()) {
            // NOT (ts >= start AND ts < end)  ≡  (ts < start) OR (ts >= end)
            let below = Expr::BinaryExpr(BinaryExpr { left: ts_col(), op: Operator::Lt, right: ts_lit(start) });
            let at_or_above = Expr::BinaryExpr(BinaryExpr { left: ts_col(), op: Operator::GtEq, right: ts_lit(end) });
            let outside = Expr::BinaryExpr(BinaryExpr { left: Box::new(below), op: Operator::Or, right: Box::new(at_or_above) });
            delta_filters.push(match (&version_col, version_gate) {
                (Some(c), Some(g)) => Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(outside),
                    op: Operator::Or,
                    right: Box::new(Expr::BinaryExpr(BinaryExpr { left: Box::new(col(c)), op: Operator::Gt, right: ts_lit(g) })),
                }),
                _ => outside,
            });
        }
        if let Some(f) = tantivy_id_filter.clone() {
            delta_filters.push(f);
        }

        // Execute Delta query — fast path skips the 3 tokio RwLock `.await`s
        // when we've already resolved this (project, table) pair before.
        let resolve_span = tracing::trace_span!(parent: &span, "resolve_delta_table");
        let delta_table = match self.database.try_fast_resolve(&project_id, &self.table_name) {
            Some(t) => {
                tag_shape(&|s| s.fast_resolve_hit = Some(true));
                t
            }
            None => {
                tag_shape(&|s| s.fast_resolve_hit = Some(false));
                self.database.resolve_table(&project_id, &self.table_name).instrument(resolve_span).await?
            }
        };
        let table = delta_table.read().await;
        let delta_plan =
            self.scan_delta_table(&table, state, projection, &delta_filters, limit, tantivy_exclude.as_ref(), tantivy_row_selections.as_ref()).await?;
        tag_shape(&|s| s.has_delta = true);

        // Union the legs in recency order — mem, then hot tier, then Delta —
        // so DedupExec's keep-first favours the freshest copy of a row. The hot
        // leg already applied the projection (it filters post-projection), so
        // it carries the projected schema and pushes nothing further.
        let hot_plan = match hot_partitions.is_empty() {
            true => None,
            false => {
                let hot_schema = match projection {
                    Some(p) => Arc::new(self.schema.project(p)?),
                    None => self.schema.clone(),
                };
                let hot_out = hot_schema.clone();
                let source = MemorySourceConfig::try_new(&hot_partitions, hot_schema, None).map_err(|e| DataFusionError::External(Box::new(e)))?;
                let source = Self::declare_ordering(source, hot_sorted, &self.table_name, &hot_out);
                let exec = Arc::new(DataSourceExec::new(Arc::new(source)));
                Some(Arc::new(crate::hot_tier::HotLegPooledExec::new(exec, hot_bytes)) as Arc<dyn ExecutionPlan>)
            }
        };
        // Sortable mask tracks the flatten: in-memory legs true, Delta false.
        let leg_sortable: Vec<bool> = [mem_plan.as_ref().map(|_| true), hot_plan.as_ref().map(|_| true), Some(false)].into_iter().flatten().collect();
        let legs: Vec<Arc<dyn ExecutionPlan>> = [mem_plan, hot_plan, Some(delta_plan)].into_iter().flatten().collect();
        wrap_result(legs, leg_sortable)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

#[cfg(test)]
mod writer_properties_tests {
    use deltalake::datafusion::parquet::{
        basic::{Compression, ZstdLevel},
        schema::types::ColumnPath,
    };

    use super::*;
    use crate::schema_loader::{FieldDef, SortingColumnDef, TableSchema};

    fn cfg() -> crate::config::ParquetConfig {
        serde_json::from_str("{}").unwrap()
    }

    fn field(name: &str, dt: &str) -> FieldDef {
        FieldDef { name: name.into(), data_type: dt.into(), nullable: true, tantivy: None, dictionary: None, bloom_filter: false }
    }

    fn schema_with(fields: Vec<FieldDef>, sort: Vec<&str>) -> TableSchema {
        TableSchema {
            table_name: "t".into(),
            partitions: vec![],
            sorting_columns: sort.into_iter().map(|n| SortingColumnDef { name: n.into(), descending: false, nulls_first: false }).collect(),
            z_order_columns: vec![],
            fields,
            time_column: None,
            dedup_keys: vec![],
            dedup_tiebreak: None,
            tombstone_column: None,
            version_append: false,
        }
    }

    /// Drain the streaming sort into batches, failing loudly on a merge error.
    fn drain(f: FlushBatches) -> Vec<RecordBatch> {
        f.collect::<Result<Vec<_>, _>>().expect("merge must not fail on well-formed input")
    }

    /// Row-wise rendering of a batch list, independent of how rows are chunked
    /// or of the physical encoding a path happened to produce (dictionary
    /// remapping, view vs non-view). `cols = None` renders whole rows; a
    /// column subset renders just those (used for the sort-key sequence).
    fn render(batches: &[RecordBatch], cols: Option<&[&str]>) -> Vec<String> {
        use arrow::util::display::{ArrayFormatter, FormatOptions};
        let opts = FormatOptions::default().with_null("<NULL>");
        let mut out = Vec::new();
        for b in batches {
            let schema = b.schema();
            let picked: Vec<(usize, &str)> = schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| cols.is_none_or(|c| c.contains(&f.name().as_str())))
                .map(|(i, f)| (i, f.name().as_str()))
                .collect();
            let fmt: Vec<_> = picked.iter().map(|(i, _)| ArrayFormatter::try_new(b.column(*i).as_ref(), &opts).unwrap()).collect();
            for row in 0..b.num_rows() {
                out.push(picked.iter().zip(&fmt).map(|((_, n), f)| format!("{n}={}", f.value(row))).collect::<Vec<_>>().join("|"));
            }
        }
        out
    }

    /// EQUIVALENCE: the streaming k-way merge must produce exactly the rows the
    /// old concat + global-lexsort + take path produced, in the same key order.
    ///
    /// Adversarial by construction: batches are individually unsorted, key
    /// ranges overlap across batches, the key domain is tiny so ties are
    /// everywhere (with a payload column that makes tie ORDER observable),
    /// empty batches are interleaved, and the payload includes dictionary and
    /// byte-view columns whose physical encoding differs between the two paths.
    ///
    /// What is asserted, and why not byte-for-byte row order: the reference
    /// path's tie order is NOT a specification — `lexsort_to_indices` sorts
    /// with `sort_unstable_by`, so which of two rows that tie on every sort key
    /// came first was already arbitrary. So we pin the three properties that
    /// actually matter: (1) the same multiset of rows — nothing dropped,
    /// duplicated or mangled; (2) an identical sort-KEY sequence, so the
    /// declared `sorting_columns` footer describes both files identically;
    /// (3) both paths agree on `sorted`. `unique_keys_order_is_identical`
    /// below covers exact row-for-row order for the unambiguous case.
    #[test]
    fn streaming_merge_matches_reference_sort() {
        use arrow::array::{DictionaryArray, Int32Array, Int64Array, StringArray, StringViewArray};
        use arrow_schema::{DataType, Field, Schema};

        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, true),
            Field::new("id", DataType::Utf8, true),
            // Payload columns: never sort keys, so any order difference between
            // the two paths shows up here.
            Field::new("seq", DataType::Int64, false),
            Field::new("body", DataType::Utf8View, true),
            Field::new("svc", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), true),
        ]));

        let mut merged_paths = 0;
        for case in 0..40u64 {
            let mut rng = fastrand::Rng::with_seed(case);
            let mut sch = schema_with(vec![], vec!["timestamp", "id"]);
            // Exercise every direction/null-placement combination.
            sch.sorting_columns[0].descending = case % 2 == 0;
            sch.sorting_columns[0].nulls_first = case % 3 == 0;
            sch.sorting_columns[1].descending = case % 5 == 0;
            sch.sorting_columns[1].nulls_first = case % 7 == 0;

            let mut batches = Vec::new();
            let mut seq = 0i64;
            for _ in 0..rng.usize(1..10) {
                // Empty batches interleaved — they must not shift the output.
                let rows = if rng.u8(0..8) == 0 { 0 } else { rng.usize(1..2_400) };
                let (mut ts, mut ids, mut seqs, mut bodies, mut svcs) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
                for _ in 0..rows {
                    // Tiny key domain + nulls ⇒ heavy duplicate-key traffic.
                    ts.push((rng.u8(0..12) != 0).then(|| rng.i64(0..7)));
                    ids.push((rng.u8(0..12) != 0).then(|| format!("k{}", rng.u8(0..5))));
                    seqs.push(seq);
                    seq += 1;
                    bodies.push((rng.bool()).then(|| format!("body-{}", rng.u32(0..1000))));
                    svcs.push(Some(format!("svc-{}", rng.u8(0..3))));
                }
                let svc: DictionaryArray<arrow::datatypes::Int32Type> = svcs.iter().map(|s| s.as_deref()).collect();
                batches.push(
                    RecordBatch::try_new(
                        arrow_schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(ts)),
                            Arc::new(StringArray::from(ids)),
                            Arc::new(Int64Array::from(seqs)),
                            Arc::new(StringViewArray::from(bodies)),
                            Arc::new(svc) as Arc<dyn arrow::array::Array>,
                        ],
                    )
                    .unwrap(),
                );
            }
            let _ = Int32Array::from(vec![0]); // keep the import honest for dictionary key typing

            let (want, want_sorted) = sort_batches_by_schema_reference(&sch, batches.clone());
            let (got, got_sorted) = sort_batches_by_schema(&sch, batches, DEFAULT_SORT_SKIP_BYTES);
            if matches!(got, FlushBatches::Merge(_)) {
                merged_paths += 1;
            }
            let got = drain(got);
            assert_eq!(want_sorted, got_sorted, "case {case}: both paths must agree on whether the footer may claim an order");
            assert_eq!(
                render(&want, Some(&["timestamp", "id"])),
                render(&got, Some(&["timestamp", "id"])),
                "case {case}: sort-key sequence diverged — the two files are not in the same order"
            );
            let (mut want_rows, mut got_rows) = (render(&want, None), render(&got, None));
            want_rows.sort();
            got_rows.sort();
            assert_eq!(want_rows, got_rows, "case {case}: streaming merge changed the row content (dropped/duplicated/mangled)");
        }
        assert!(merged_paths >= 10, "the streaming merge path must actually be exercised (hit {merged_paths}/40 cases)");
    }

    /// With unique sort keys the order is unambiguous, so the streaming merge
    /// must match the reference row for row — no tie-order escape hatch.
    #[test]
    fn unique_keys_order_is_identical() {
        use arrow::array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let s = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false), Field::new("payload", DataType::Utf8, false)]));
        let mut rng = fastrand::Rng::with_seed(99);
        let mut keys: Vec<i64> = (0..12_000).collect();
        rng.shuffle(&mut keys);
        let batches: Vec<RecordBatch> = keys
            .chunks(1_500)
            .map(|c| {
                RecordBatch::try_new(
                    s.clone(),
                    vec![Arc::new(Int64Array::from(c.to_vec())), Arc::new(StringArray::from(c.iter().map(|k| format!("p{k}")).collect::<Vec<_>>()))],
                )
                .unwrap()
            })
            .collect();
        let sch = schema_with(vec![], vec!["timestamp"]);
        let (want, _) = sort_batches_by_schema_reference(&sch, batches.clone());
        let (got, sorted) = sort_batches_by_schema(&sch, batches, DEFAULT_SORT_SKIP_BYTES);
        assert!(sorted);
        assert!(matches!(got, FlushBatches::Merge(_)), "this input must take the streaming path");
        assert_eq!(render(&want, None), render(&drain(got), None));
    }

    /// The merge emits many bounded chunks rather than one whole-bucket batch —
    /// that bound is the entire point of the change, so pin it.
    #[test]
    fn streaming_merge_emits_bounded_chunks() {
        use arrow::array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        let s = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]));
        // Two interleaved runs, 200k rows total — past the row cap, so the
        // merge must hand the writer several chunks instead of one big batch.
        let mk =
            |off: i64| RecordBatch::try_new(s.clone(), vec![Arc::new(Int64Array::from((0..100_000).map(|i| i * 2 + off).rev().collect::<Vec<_>>()))]).unwrap();
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), vec![mk(0), mk(1)], DEFAULT_SORT_SKIP_BYTES);
        assert!(sorted);
        let chunks = drain(out);
        assert!(chunks.len() > 1, "output must be chunked, not one 200k-row batch");
        assert!(chunks.iter().all(|c| c.num_rows() <= MERGE_CHUNK_ROWS_MAX), "no chunk may exceed the row cap");
        let all: Vec<i64> = chunks.iter().flat_map(|c| c.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()).collect();
        assert_eq!(all.len(), 200_000);
        assert!(all.windows(2).all(|w| w[0] <= w[1]), "merge of sorted runs is globally sorted");
    }

    // Regression: a schema-diverse 10-min bucket (mem_buffer's
    // `schemas_compatible` admits batches that differ by an evolved nullable
    // column) must STILL flush as a globally sorted file with an honest parquet
    // `sorting_columns` footer. Before the fix `sort_batches_by_schema` bailed
    // (sorted=false) on any heterogeneous bucket, so the file carried no footer
    // ordering — and one unsorted file disables the delta-rs reader's
    // all-or-nothing footer-ordering pushdown for the whole scan, degrading
    // `ORDER BY timestamp DESC LIMIT n` to a blocking full-window sort
    // (observed inert on prod 2026-07-15; top-N pushdown never fired).
    #[test]
    fn heterogeneous_bucket_still_sorts_with_honest_footer() {
        use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
        use arrow_schema::{DataType, Field, Schema, TimeUnit};

        let mut sch = schema_with(vec![], vec!["timestamp"]);
        sch.sorting_columns[0].descending = true;
        sch.sorting_columns[0].nulls_first = true;

        let ts = |v: Vec<i64>| Arc::new(TimestampMicrosecondArray::from(v).with_timezone("UTC"));
        let ts_ty = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));

        let schema_a = Arc::new(Schema::new(vec![Field::new("timestamp", ts_ty.clone(), false), Field::new("id", DataType::Utf8, false)]));
        let batch_a = RecordBatch::try_new(schema_a, vec![ts(vec![100, 300]), Arc::new(StringArray::from(vec!["a", "c"]))]).unwrap();

        // batch_b carries an extra nullable column absent from batch_a.
        let schema_b = Arc::new(Schema::new(vec![
            Field::new("timestamp", ts_ty, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("extra", DataType::Int64, true),
        ]));
        let batch_b = RecordBatch::try_new(
            schema_b,
            vec![ts(vec![200, 400]), Arc::new(StringArray::from(vec!["b", "d"])), Arc::new(Int64Array::from(vec![Some(1), Some(2)]))],
        )
        .unwrap();

        let (out, sorted) = sort_batches_by_schema(&sch, vec![batch_a, batch_b], DEFAULT_SORT_SKIP_BYTES);
        let out = drain(out);

        assert!(sorted, "heterogeneous bucket must still be reported sorted so the footer is declared");
        assert_eq!(out.len(), 1, "batches must be unified into one sorted file");
        let got: Vec<i64> = out[0].column_by_name("timestamp").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().values().to_vec();
        assert_eq!(got, vec![400, 300, 200, 100], "rows must be globally timestamp-DESC across the merged batches");
        assert!(out[0].schema().column_with_name("extra").is_some(), "merged superset column must survive (no data loss)");
    }

    #[test]
    fn uri_date_in_window_gates_on_partition_day() {
        let day = |y, m, d| chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
        let u = "s3://b/timefusion/default/otel/project_id=p/date=2026-06-15/f.parquet";
        // window squarely containing the partition day
        assert!(uri_date_in_window(u, day(2026, 6, 1), day(2026, 6, 30)));
        // window entirely before / after the partition day
        assert!(!uri_date_in_window(u, day(2026, 6, 16), day(2026, 6, 20)));
        assert!(!uri_date_in_window(u, day(2026, 5, 1), day(2026, 6, 14)));
        // boundary days are inclusive
        assert!(uri_date_in_window(u, day(2026, 6, 15), day(2026, 6, 15)));
        // open bounds match that side
        assert!(uri_date_in_window(u, i64::MIN, day(2026, 6, 30)));
        assert!(uri_date_in_window(u, day(2026, 6, 1), i64::MAX));
        assert!(uri_date_in_window(u, i64::MIN, i64::MAX));
        // missing/unparseable date ⇒ conservatively in-window (demand coverage)
        assert!(uri_date_in_window("s3://b/no-partition/f.parquet", day(2026, 6, 16), day(2026, 6, 20)));
    }

    #[test]
    fn compression_level_drives_zstd() {
        for level in [3, 9, 15, 19] {
            let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), level, true);
            assert_eq!(p.compression(&ColumnPath::from("anything")), Compression::ZSTD(ZstdLevel::try_new(level).unwrap()));
        }
    }

    #[test]
    fn row_group_size_is_a_byte_limit() {
        let mut c = cfg();
        c.timefusion_max_row_group_size = 128 * 1024 * 1024;
        let p = build_writer_properties(&c, &schema_with(vec![], vec![]), 3, true);
        assert_eq!(p.max_row_group_bytes(), Some(c.timefusion_max_row_group_size));
    }

    // Regression for the 2026-07-17 otel_metrics outage: an S3
    // SelectObjectContentRequest XML body (299 bytes, no PAR1 magic) was
    // written over a delta-log checkpoint. The footer check must reject it so
    // the checkpoint task withholds log cleanup and the JSON stays recoverable.
    #[test]
    fn parquet_tail_ok_rejects_foreign_and_truncated_objects() {
        let good = b"\x10\x00\x00\x00PAR1"; // footer_len=16, magic ok
        assert!(super::parquet_tail_ok(good, 1024));
        // The real clobber: an XML body's last 8 bytes, no PAR1 magic.
        assert!(!super::parquet_tail_ok(b"quest>\x00\x00", 299));
        assert!(!super::parquet_tail_ok(b"Result>\n", 299));
        // Valid magic but a footer length that can't fit in the file (corrupt).
        assert!(!super::parquet_tail_ok(b"\xff\xff\xff\x7fPAR1", 64));
        // footer_len == 0 is impossible for a real file.
        assert!(!super::parquet_tail_ok(b"\x00\x00\x00\x00PAR1", 1024));
        // Wrong length input.
        assert!(!super::parquet_tail_ok(b"PAR1", 8));
    }

    #[test]
    fn invalid_zstd_level_falls_back() {
        let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), 999, true);
        assert_eq!(p.compression(&ColumnPath::from("x")), Compression::ZSTD(ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap()));
    }

    #[test]
    fn footer_kv_metadata_carries_tier() {
        let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), 15, true);
        let kv = p.key_value_metadata().expect("KV metadata present");
        let tier = kv.iter().find(|k| k.key == COMPRESSION_TIER_KEY).expect("tier key present");
        assert_eq!(tier.value.as_deref(), Some("15"));
    }

    // Pins the warm_all_footers default: non-recent files stay in the warm
    // set as footer-only (recent=false), NEWEST partition first (the
    // partitions dashboards query must be warm within seconds of boot, even
    // if the process dies mid-warm); with the flag off they are dropped
    // entirely.
    #[test]
    fn select_warm_paths_pins_warm_all_footers_default() {
        let prefix = "s3://bucket/timefusion/default/otel";
        let uris = vec![
            format!("{prefix}/project_id=p/date=2099-01-01/new.parquet"),
            format!("{prefix}/project_id=p/date=2020-01-01/old.parquet"),
            format!("{prefix}/project_id=p/date=2099-01-02/checkpoint.json"),
            "s3://elsewhere/unrelated.parquet".to_string(),
        ];
        let cutoff = Some(chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());

        let (paths, dropped) = select_warm_paths(uris.clone(), prefix, true, cutoff);
        assert_eq!(dropped, 1, "prefix-mismatched URI counted as dropped");
        let got: Vec<(&str, bool)> = paths.iter().map(|(p, r)| (p.as_ref(), *r)).collect();
        assert_eq!(
            got,
            vec![
                ("project_id=p/date=2099-01-01/new.parquet", true),  // newest warms first
                ("project_id=p/date=2020-01-01/old.parquet", false), // footer-only, backfills last
            ]
        );

        let (paths, _) = select_warm_paths(uris, prefix, false, cutoff);
        assert_eq!(paths.len(), 1, "warm_all_footers=false drops non-recent files");
        assert!(paths[0].0.as_ref().contains("date=2099-01-01"));
    }

    #[test]
    fn bloom_opt_in_only_for_flagged_columns() {
        let mut f1 = field("id", "Utf8");
        f1.bloom_filter = true;
        let p = build_writer_properties(&cfg(), &schema_with(vec![f1, field("body", "Utf8")], vec![]), 3, true);
        assert!(p.bloom_filter_properties(&ColumnPath::from("id")).is_some(), "flagged column has bloom");
        assert!(p.bloom_filter_properties(&ColumnPath::from("body")).is_none(), "unflagged column has no bloom");
    }

    #[test]
    fn global_bloom_kill_switch_overrides_opt_in() {
        let mut f = field("id", "Utf8");
        f.bloom_filter = true;
        let mut c = cfg();
        c.timefusion_bloom_filter_disabled = true;
        let p = build_writer_properties(&c, &schema_with(vec![f], vec![]), 3, true);
        assert!(p.bloom_filter_properties(&ColumnPath::from("id")).is_none());
    }

    #[test]
    fn dictionary_opt_out_disables_dict() {
        let mut f = field("stacktrace", "Utf8");
        f.dictionary = Some(false);
        let p = build_writer_properties(&cfg(), &schema_with(vec![f], vec![]), 3, true);
        assert!(!p.dictionary_enabled(&ColumnPath::from("stacktrace")));
    }

    #[test]
    fn sort_key_utf8_uses_delta_byte_array_and_no_dict() {
        use deltalake::datafusion::parquet::basic::Encoding;
        let p = build_writer_properties(&cfg(), &schema_with(vec![field("id", "Utf8")], vec!["id"]), 3, true);
        assert_eq!(p.encoding(&ColumnPath::from("id")), Some(Encoding::DELTA_BYTE_ARRAY));
        assert!(!p.dictionary_enabled(&ColumnPath::from("id")));
    }

    #[test]
    fn timestamp_and_int_use_delta_binary_packed() {
        use deltalake::datafusion::parquet::basic::Encoding;
        let p = build_writer_properties(&cfg(), &schema_with(vec![field("ts", "Timestamp(Nanosecond, None)"), field("n", "Int64")], vec![]), 3, true);
        assert_eq!(p.encoding(&ColumnPath::from("ts")), Some(Encoding::DELTA_BINARY_PACKED));
        assert!(!p.dictionary_enabled(&ColumnPath::from("ts")));
        assert_eq!(p.encoding(&ColumnPath::from("n")), Some(Encoding::DELTA_BINARY_PACKED));
    }

    // Fix #3: page-level stats only on declared sort keys; wide columns get
    // chunk-level stats to keep the ColumnIndex (decoded-metadata) small.
    #[test]
    fn page_stats_only_for_sort_keys() {
        use deltalake::datafusion::parquet::file::properties::EnabledStatistics;
        let p = build_writer_properties(
            &cfg(),
            &schema_with(vec![field("timestamp", "Timestamp(Microsecond, None)"), field("body", "Utf8")], vec!["timestamp"]),
            3,
            true,
        );
        assert_eq!(p.statistics_enabled(&ColumnPath::from("timestamp")), EnabledStatistics::Page);
        assert_eq!(p.statistics_enabled(&ColumnPath::from("body")), EnabledStatistics::Chunk);
    }

    // Option A: only declare the parquet SortingColumn footer when the writer
    // actually sorted the rows. Optimize/compact paths (declare_sorted=false)
    // must NOT claim an order they don't write, or order-trusting readers break.
    #[test]
    fn sorting_columns_declared_only_when_sorted() {
        let s = schema_with(vec![field("timestamp", "Timestamp(Microsecond, None)"), field("id", "Utf8")], vec!["timestamp", "id"]);
        let sorted = build_writer_properties(&cfg(), &s, 3, true);
        let unsorted = build_writer_properties(&cfg(), &s, 3, false);
        assert!(sorted.sorting_columns().is_some(), "flush/dedup path declares the sort order");
        assert!(unsorted.sorting_columns().is_none(), "optimize/compact path declares no order");
    }

    // Fix #1: the decoded-metadata cache limit must reach the RuntimeEnv (a
    // SessionConfig `datafusion.runtime.*` string would not).
    #[test]
    fn runtime_env_applies_metadata_cache_limit() {
        let pool = std::sync::Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(1024 * 1024));
        let bytes = 321 * 1024 * 1024;
        let rt = build_query_runtime_env(pool, bytes);
        assert_eq!(rt.cache_manager.get_metadata_cache_limit(), bytes);
    }

    // Read-side dedup skip: fingerprint is order-insensitive but content-
    // sensitive, and the window→dates expansion bounds itself.
    #[test]
    fn dedup_skip_fingerprint_and_window_dates() {
        let a = vec!["p/date=2026-07-01/f1.parquet".to_string(), "p/date=2026-07-01/f2.parquet".to_string()];
        let mut b = a.clone();
        b.reverse();
        assert_eq!(partition_file_fp(a.clone()), partition_file_fp(b), "order must not matter");
        let c = vec![a[0].clone()];
        assert_ne!(partition_file_fp(a), partition_file_fp(c), "content must matter");

        let day = 86_400_000_000i64;
        assert_eq!(window_dates(0, 0).map(|d| d.len()), Some(1));
        assert_eq!(window_dates(0, 2 * day).map(|d| d.len()), Some(3));
        assert_eq!(window_dates(2 * day, 0), None, "inverted window");
        assert_eq!(window_dates(0, 400 * day), None, "wider than a year → keep DedupExec");
    }

    #[test]
    fn dedup_file_selection_is_exact_for_unified_and_custom_tables() {
        let paths = vec![
            "project_id=p1/date=2026-08-03/a.parquet".to_string(),
            "project_id=p2/date=2026-08-03/b.parquet".to_string(),
            "project_id=p1/date=2026-08-02/c.parquet".to_string(),
        ];
        assert_eq!(dedup_partition_paths(paths, "p1", "2026-08-03"), vec!["project_id=p1/date=2026-08-03/a.parquet"]);

        let custom = vec!["date=2026-08-03/a.parquet".to_string(), "date=2026-08-02/b.parquet".to_string()];
        assert_eq!(dedup_partition_paths(custom, "physical-owner", "2026-08-03"), vec!["date=2026-08-03/a.parquet"]);
    }

    // Fix #4: batches are globally sorted by the declared lead key before write.
    #[test]
    fn sort_batches_orders_by_declared_keys() {
        use arrow::array::{Array, Int64Array};
        use arrow_schema::{DataType, Field, Schema};
        let s = std::sync::Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(s.clone(), vec![std::sync::Arc::new(Int64Array::from(vec![3, 1]))]).unwrap();
        let b2 = RecordBatch::try_new(s.clone(), vec![std::sync::Arc::new(Int64Array::from(vec![2, 0]))]).unwrap();
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), vec![b1, b2], DEFAULT_SORT_SKIP_BYTES);
        assert!(sorted);
        let out = drain(out);
        assert_eq!(out.len(), 1);
        let col = out[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.values(), &[0, 1, 2, 3]);
        // No declared sort columns → input returned untouched, sorted=false.
        let (passthrough, sorted) = sort_batches_by_schema(&schema_with(vec![], vec![]), vec![out[0].clone(), out[0].clone()], DEFAULT_SORT_SKIP_BYTES);
        assert!(!sorted);
        assert_eq!(drain(passthrough).len(), 2);
    }

    // A bucket whose batches have evolved schemas (an extra nullable column on
    // the 2nd batch, which mem_buffer's schemas_compatible admits) is unified
    // to a superset schema and globally sorted — no abort, no data loss — so the
    // flushed file gets an honest footer and stays eligible for the reader's
    // ordering pushdown. (Previously this returned sorted=false; see
    // `heterogeneous_bucket_still_sorts_with_honest_footer` for why that
    // silently disabled top-N pushdown on prod.)
    #[test]
    fn sort_batches_tolerates_schema_evolution() {
        use arrow::array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let s1 = std::sync::Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]));
        let s2 = std::sync::Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false), Field::new("extra", DataType::Utf8, true)]));
        let b1 = RecordBatch::try_new(s1, vec![std::sync::Arc::new(Int64Array::from(vec![2, 1]))]).unwrap();
        let b2 =
            RecordBatch::try_new(s2, vec![std::sync::Arc::new(Int64Array::from(vec![3])), std::sync::Arc::new(StringArray::from(vec![Some("x")]))]).unwrap();
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), vec![b1, b2], DEFAULT_SORT_SKIP_BYTES);
        let out = drain(out);
        assert!(sorted, "mixed-schema bucket is unified and sorted, not left unsorted");
        assert_eq!(out.len(), 1, "batches unified into one sorted file");
        let ts = out[0].column_by_name("timestamp").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ts.values(), &[1, 2, 3], "globally sorted by the declared key across evolved batches");
        assert!(out[0].schema().column_with_name("extra").is_some(), "evolved superset column survives (no data loss)");
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serial_test::serial;

    use super::*;
    use crate::{config::AppConfig, test_utils::test_helpers::*};

    /// The merge-on-read gate: `keep_greatest_ordering` yields the lead sort key
    /// only when the table declares a `dedup_tiebreak` AND that key is a dedup
    /// key of an i64-backed type. Without a tiebreak it must yield `None` — that
    /// is what keeps a non-merge-on-read table's plan byte-identical to the
    /// pre-merge-on-read shape (no leg sort, no merge).
    #[test]
    fn keep_greatest_ordering_requires_a_tiebreak() {
        let otel = get_schema("otel_logs_and_spans").expect("registered");
        let schema = otel.schema_ref();
        let ord = ProjectRoutingTable::keep_greatest_ordering(otel, &schema).expect("otel declares a tiebreak + timestamp-led sort");
        assert_eq!(ord.to_string(), "timestamp@0 DESC", "one column only — all `detect_bound` reads, and the cheapest leg sort");

        let mut no_tiebreak = otel.clone();
        no_tiebreak.dedup_tiebreak = None;
        assert!(ProjectRoutingTable::keep_greatest_ordering(&no_tiebreak, &schema).is_none(), "no tiebreak ⇒ no plan change at all");

        // A sort key that isn't a dedup key breaks `detect_bound`'s contract
        // (equal keys would no longer share the bound value), so: no ordering.
        let mut unkeyed = otel.clone();
        unkeyed.dedup_keys = vec!["id".into()];
        assert!(ProjectRoutingTable::keep_greatest_ordering(&unkeyed, &schema).is_none(), "lead sort key must itself be a dedup key");
    }

    /// A predicate on the tombstone marker must never be handed to a scan leg —
    /// applied at the source it drops the tombstone before the dedup and the
    /// stale live version wins (silent resurrection).
    #[test]
    fn tombstone_predicates_are_never_pushed_down() {
        let deleted = col("deleted").eq(lit(true));
        assert!(ProjectRoutingTable::references_tombstone("mor_versioned", &deleted));
        assert!(!ProjectRoutingTable::references_tombstone("mor_versioned", &col("id").eq(lit("x"))));
        // The shipped merge-on-read tables must be protected too.
        for t in ["otel_logs_and_spans", "otel_metrics"] {
            assert!(ProjectRoutingTable::references_tombstone(t, &deleted), "{t} ships merge-on-read — its tombstone predicate must not reach a leg");
        }
        // Tables declaring no tombstone column have no such predicate to protect
        // — there `deleted` is just an unknown column name.
        for t in ["variant_bench", "mor_dormant"] {
            assert!(!ProjectRoutingTable::references_tombstone(t, &deleted));
        }
    }

    /// The optimize/dedup session must carry a bounded batch size and a sort
    /// spill reservation so the Z-order external sort spills instead of failing
    /// with "Resources exhausted" (prod 2026-07-12). Guards the config half of
    /// that fix; the dedicated maintenance pool + spill dir are covered by the
    /// dedup_compaction integration tests.
    #[test]
    fn optimize_session_sets_batch_size_and_spill_reservation() {
        let state = build_optimize_session_state(0, Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()));
        let exec = &state.config().options().execution;
        assert_eq!(exec.batch_size, 2048, "merge memory ≈ fan-in × batch; 8192-row otel batches measured up to 145MB");
        assert_eq!(exec.sort_spill_reservation_bytes, 33_554_432);
        // Parallelism capped so per-partition spill reservations fit the bounded pool.
        assert_eq!(exec.target_partitions, 2, "0 (all cores) must cap to the maintenance limit");
        assert_eq!(
            build_optimize_session_state(64, Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default()))
                .config()
                .options()
                .execution
                .target_partitions,
            2
        );
    }

    /// GatedScanExec must release its permit BETWEEN batches: a per-stream hold
    /// deadlocks any consumer that needs a batch from every input partition at
    /// once (SortPreservingMerge) when permits < partitions. Here 8 partitions
    /// contend for 2 permits and every partition's first batch is awaited
    /// concurrently — a regression to per-stream holding hangs this barrier.
    #[tokio::test(flavor = "multi_thread")]
    async fn gated_scan_exec_releases_permit_between_batches_no_deadlock() {
        use arrow::array::Int32Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int32, false)]));
        let partitions: Vec<Vec<RecordBatch>> =
            (0..8).map(|i| vec![RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))]).unwrap()]).collect();
        let src: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(&partitions, schema.clone(), None).unwrap())));
        let gated = Arc::new(GatedScanExec::new(src, Arc::new(tokio::sync::Semaphore::new(2)), None, false, 2));
        let ctx = Arc::new(TaskContext::default());
        let mut streams: Vec<_> = (0..8).map(|p| gated.execute(p, ctx.clone()).unwrap()).collect();
        let firsts = tokio::time::timeout(std::time::Duration::from_secs(10), futures::future::join_all(streams.iter_mut().map(futures::StreamExt::next)))
            .await
            .expect("GatedScanExec deadlocked — per-batch permit release regressed");
        let mut vals: Vec<i32> = firsts.into_iter().map(|b| b.unwrap().unwrap().column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0)).collect();
        vals.sort();
        assert_eq!(vals, (0..8).collect::<Vec<_>>(), "every gated partition must yield its row");
    }

    /// Parquet decode is outside every DataFusion pool, so its heap has never
    /// been measured — a single wide scan's decode exceeded the box's whole
    /// slack on 2026-07-20. GatedScanExec's permit window IS the decode window,
    /// so it can account decoded Arrow bytes there. Accounting only: it must
    /// never refuse a batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn gated_scan_exec_accounts_decoded_bytes() {
        use arrow::array::Int32Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::atomic::Ordering::Relaxed;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from((0..512).collect::<Vec<i32>>()))]).unwrap();
        let want = batch.get_array_memory_size() as u64;
        let partitions = vec![vec![batch.clone(), batch.clone()]];
        let src: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(&partitions, schema, None).unwrap())));

        let metrics = Arc::new(ScanMetrics::default());
        let gated = Arc::new(GatedScanExec::new(src, Arc::new(tokio::sync::Semaphore::new(4)), Some(metrics.clone()), false, 4));
        let mut stream = gated.execute(0, Arc::new(TaskContext::default())).unwrap();
        let mut rows = 0;
        while let Some(b) = futures::StreamExt::next(&mut stream).await {
            rows += b.unwrap().num_rows();
        }

        assert_eq!(rows, 1024, "gating must not drop batches");
        assert_eq!(metrics.decode_bytes_total.load(Relaxed), want * 2, "both decoded batches must be accounted");
        assert_eq!(metrics.decode_peak_batch_bytes.load(Relaxed), want);
        assert_eq!(metrics.decode_polls_inflight.load(Relaxed), 0, "in-flight gauge must return to zero");
        assert_eq!(metrics.decode_polls_inflight_peak.load(Relaxed), 1, "one partition polled serially = peak 1");
    }

    /// The decode pressure valve: full concurrency until 88% of the cgroup
    /// limit, quarter pool to 95%, fully serialized past that. Claims never
    /// exceed the pool (progress guaranteed) and never drop to zero.
    #[test]
    fn pressure_permit_claim_tiers() {
        assert_eq!(pressure_permit_claim(0, 16), 1);
        assert_eq!(pressure_permit_claim(87, 16), 1);
        assert_eq!(pressure_permit_claim(88, 16), 4);
        assert_eq!(pressure_permit_claim(94, 16), 4);
        assert_eq!(pressure_permit_claim(95, 16), 16);
        assert_eq!(pressure_permit_claim(200, 16), 16);
        assert_eq!(pressure_permit_claim(88, 2), 1, "tiny pools floor at 1");
        assert_eq!(pressure_permit_claim(95, 1), 1);
    }

    /// spawn_cron_job must fire on the wall-clock schedule (regression: the
    /// tokio-cron-scheduler it replaced silently stopped ticking in prod, 14h /
    /// 0 runs) and stop firing once the maintenance cancel token is triggered.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cron_job_fires_on_schedule_then_stops_on_cancel() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let count = Arc::new(AtomicUsize::new(0));
        let cancel = Arc::new(CancellationToken::new());
        {
            let count = count.clone();
            // "* * * * * *" = every second (6-field, seconds).
            spawn_cron_job("test", "* * * * * *", cancel.clone(), move || {
                let count = count.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        let fired = count.load(Ordering::SeqCst);
        assert!(fired >= 2, "every-second cron should fire >=2x in 2.5s, got {fired}");

        cancel.cancel();
        // Allow an in-flight tick to settle, then confirm the count is frozen.
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let after_cancel = count.load(Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
        assert_eq!(count.load(Ordering::SeqCst), after_cancel, "no fires after cancel");
    }

    /// A wedged job body must not freeze the cron loop: later ticks are skipped
    /// (not queued) and the skip counter grows, so the schedule survives and the
    /// wedge is visible in `timefusion_stats`.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cron_job_skips_ticks_while_previous_run_hangs() {
        use std::sync::atomic::Ordering::Relaxed;
        let cancel = Arc::new(CancellationToken::new());
        let skipped_before = crate::metrics::maintenance_stats().cron_ticks_skipped.load(Relaxed);
        spawn_cron_job("hung-test", "* * * * * *", cancel.clone(), move || async move {
            std::future::pending::<()>().await; // never returns
        });
        // Generous window: needs >=2 wall-clock second boundaries even on a
        // loaded CI runner where task startup and sleep wakeups slip.
        tokio::time::sleep(std::time::Duration::from_millis(5200)).await;
        cancel.cancel();
        let skipped = crate::metrics::maintenance_stats().cron_ticks_skipped.load(Relaxed) - skipped_before;
        assert!(skipped >= 1, "later ticks must be skipped (loop alive) while the first run hangs, got {skipped} skips");
    }

    /// A slow-but-healthy job must be allowed to complete across multiple
    /// skipped ticks. Previously the scheduler aborted after three skips,
    /// losing work on a still-progressing run.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cron_job_lets_slow_runs_finish() {
        use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
        let cancel = Arc::new(CancellationToken::new());
        let completed = Arc::new(AtomicUsize::new(0));
        spawn_cron_job("slow-test", "* * * * * *", cancel.clone(), {
            let completed = completed.clone();
            move || {
                let completed = completed.clone();
                async move {
                    tokio::time::sleep(std::time::Duration::from_millis(4500)).await;
                    completed.fetch_add(1, Relaxed);
                }
            }
        });
        // 6.5s spans 6 one-second ticks; the first run starts at t≈0 and would
        // be aborted at the 4s tick under the old 3-skip rule. It must instead
        // finish at t≈4.5s.
        tokio::time::sleep(std::time::Duration::from_millis(6500)).await;
        cancel.cancel();
        assert_eq!(completed.load(Relaxed), 1, "slow-but-healthy run must complete, not be aborted");
    }

    /// The shared OCC classifier must treat every retryable delta-rs conflict as
    /// retryable — including `VersionAlreadyExists` ("already exists", which can
    /// hit the dedup path under multi-replica races), `MetadataChanged`, and the
    /// predicate re-evaluation failure ("Transaction failed") — while permanent
    /// errors (protocol version, auth/IO) fail fast. Guards the dedup/optimize
    /// loops, which previously omitted some of these substrings.
    // Warm (30-min Z-order) and cold (daily 512MB consolidate) tiers must own
    // disjoint partitions, or they oscillate the same day 256MB↔512MB every cycle.
    // `date_is_cold` is the single boundary both use; assert today is warm and
    // every earlier day is cold at the default after_days=1 ("past the current
    // day"), and that a larger boundary keeps the warm window in sync.
    #[test]
    fn warm_and_cold_partition_ownership_is_disjoint() {
        use chrono::{Duration, NaiveDate};
        let today = NaiveDate::from_ymd_opt(2026, 6, 28).unwrap();

        // after_days = 1: only today is warm; yesterday and older are cold.
        assert!(!Database::date_is_cold(today, today, 1), "today must be warm (still taking writes)");
        assert!(Database::date_is_cold(today, today - Duration::days(1), 1), "yesterday must be cold");
        assert!(Database::date_is_cold(today, today - Duration::days(90), 1), "old backfill day must be cold");

        // Larger boundary keeps recent days warm; no date is ever both tiers.
        for days_ago in 0..120 {
            let d = today - Duration::days(days_ago);
            let after = 3;
            let cold = Database::date_is_cold(today, d, after);
            let warm = !cold; // warm optimize processes exactly the complement
            assert_ne!(cold, warm, "a partition must be warm xor cold, never both");
            assert_eq!(cold, days_ago >= after as i64, "boundary off-by-one at days_ago={days_ago}");
        }
    }

    #[test]
    fn is_occ_conflict_err_classifies_retryable_vs_permanent() {
        for retryable in [
            "Delta transaction failed, version 58420 already exists.",
            "Commit failed: a concurrent transaction overlapped",
            "concurrent transaction wrote to the same files",
            "Metadata changed since last commit.",
            "Transaction failed: Error evaluating predicate",
        ] {
            assert!(is_occ_conflict_err(retryable), "should retry: {retryable}");
        }
        for permanent in [
            "Generic S3 error: Access Denied",
            "Unsupported reader version: requires 3, have 2",
            "Unsupported writer version required",
            "Arrow error: Invalid argument",
        ] {
            assert!(!is_occ_conflict_err(permanent), "must fail fast: {permanent}");
        }
    }

    // Regression: a single Arrow batch carrying rows for several projects (as a
    // genuine multi-row pgwire INSERT produces) must split row-wise — each row to
    // its own project. The old routing read only row 0 and dumped every row into
    // the first row's project, silently corrupting the rest.
    #[test]
    fn test_partition_batch_by_project_row_wise() {
        use std::sync::Arc;

        use datafusion::arrow::{
            array::{ArrayRef, AsArray, Int64Array, StringArray, StringViewArray},
            datatypes::{DataType, Field, Int64Type, Schema},
        };

        let check = |pid_col: ArrayRef| {
            let schema = Arc::new(Schema::new(vec![Field::new("project_id", pid_col.data_type().clone(), true), Field::new("id", DataType::Int64, false)]));
            let ids = Int64Array::from(vec![1, 2, 3, 4]); // interleaved A/B/A + null→default
            let batch = RecordBatch::try_new(schema, vec![pid_col, Arc::new(ids)]).unwrap();

            // BTreeMap → deterministic sorted keys: A, B, default
            let parts = partition_batch_by_project(batch, "default").unwrap();
            let shape: Vec<(String, Vec<i64>)> = parts.iter().map(|(p, b)| (p.clone(), b.column(1).as_primitive::<Int64Type>().values().to_vec())).collect();
            assert_eq!(
                shape,
                vec![("A".into(), vec![1, 3]), ("B".into(), vec![2]), ("default".into(), vec![4])],
                "each project keeps exactly its own rows; null falls back to default"
            );
        };

        check(Arc::new(StringViewArray::from(vec![Some("A"), Some("B"), Some("A"), None])));
        check(Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("A"), None]))); // Utf8 path too

        // Homogeneous batch: single group, whole batch (no split).
        let schema = Arc::new(Schema::new(vec![Field::new("project_id", DataType::Utf8View, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringViewArray::from(vec!["A", "A", "A"]))]).unwrap();
        let parts = partition_batch_by_project(batch, "default").unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!((parts[0].0.as_str(), parts[0].1.num_rows()), ("A", 3));
    }

    #[test]
    fn test_within_recency() {
        let cutoff = chrono::NaiveDate::from_ymd_opt(2026, 6, 4);

        // Files on/after the cutoff date are warmed.
        assert!(within_recency("s3://b/t/date=2026-06-06/part-0.parquet", cutoff));
        assert!(within_recency("s3://b/t/date=2026-06-04/part-0.parquet", cutoff), "cutoff is inclusive");
        // Older partitions are skipped.
        assert!(!within_recency("s3://b/t/date=2026-06-01/part-0.parquet", cutoff));

        // No `date=` segment → warm (don't silently skip an unclassifiable file).
        assert!(within_recency("s3://b/t/part-0.parquet", cutoff));
        // Unparseable date → warm.
        assert!(within_recency("s3://b/t/date=not-a-date/part-0.parquet", cutoff));
        // Truncated date (segment shorter than YYYY-MM-DD) → warm.
        assert!(within_recency("s3://b/t/date=2026-06", cutoff));

        // None cutoff → no recency limit, always warm even very old partitions.
        assert!(within_recency("s3://b/t/date=2000-01-01/part-0.parquet", None));

        // Nested partitioning (project_id then date) still locates `date=`.
        assert!(!within_recency("s3://b/t/project_id=default/date=2026-05-01/part.parquet", cutoff));
    }

    /// Roundtrip the watermark through serialize → JSON → parse. Pins the
    /// on-disk format so a future change to `serialize_watermark_to_json`
    /// can't silently break `derive_wal_cursors_from_delta`. Absent shards
    /// stay absent (not coerced to ORIGIN) — that's required for the
    /// per-shard MAX aggregation to ignore commits that didn't touch a shard.
    #[test]
    fn watermark_serialize_parse_roundtrip() {
        use walrus_rust::WalPosition;
        let wm = vec![Some(WalPosition { block_id: 7, offset: 1024 }), None, Some(WalPosition { block_id: 9, offset: 0 }), None];
        let json = serialize_watermark_to_json(&wm, "p", "t");
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(json));
        let parsed = parse_watermark_from_json(&info, wm.len(), "p", "t");
        assert_eq!(parsed, wm);
    }

    /// Orphaned `datafusion-*` spill dirs are reaped; anything else in the
    /// spill dir is left alone. Prod leaked 186 GB this way because a SIGKILL
    /// skips `DiskManager`'s Drop cleanup, and the volume is shared with the WAL.
    #[test]
    fn spill_reap_removes_only_datafusion_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let orphan = dir.path().join("datafusion-AbCdEf");
        std::fs::create_dir_all(orphan.join("nested")).unwrap();
        std::fs::write(orphan.join("nested").join("0.arrow"), vec![7u8; 2048]).unwrap();
        let keep_dir = dir.path().join("something-else");
        std::fs::create_dir_all(&keep_dir).unwrap();
        let keep_file = dir.path().join("notes.txt");
        std::fs::write(&keep_file, b"keep").unwrap();

        assert_eq!(dir_size_bytes(&orphan), 2048);
        let orphans: Vec<_> =
            std::fs::read_dir(dir.path()).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().starts_with("datafusion-")).map(|e| e.path()).collect();
        // A dir created AFTER the snapshot (a live DiskManager's) must survive —
        // deleting it mid-sort was the 2026-07-29 light-optimize ENOENT failure.
        let live = dir.path().join("datafusion-LiVe01");
        std::fs::create_dir_all(&live).unwrap();
        reap_orphaned_spill_dirs_blocking(dir.path(), orphans);

        assert!(!orphan.exists(), "orphaned spill dir survived");
        assert!(live.exists(), "reaper deleted a live (post-snapshot) spill dir");
        assert!(keep_dir.exists() && keep_file.exists(), "reaper touched unrelated entries");
        // Idempotent / tolerant of an empty or absent dir.
        reap_orphaned_spill_dirs_blocking(dir.path(), vec![]);
        reap_orphaned_spill_dirs_blocking(&dir.path().join("does-not-exist"), vec![]);
    }

    /// On the unified table every default project commits to ONE Delta log, so
    /// `derive_wal_cursor_for_table` sees every tenant's watermarks. Positions
    /// are per-`topic:shard` walrus offsets and are NOT comparable across
    /// topics: a busy tenant's high block_id applied to a quiet tenant's cursor
    /// skips that tenant's unreplayed WAL entries — acked-write loss on any
    /// Delta-scan boot (which runs on EVERY boot, `bootstrap.rs`). A watermark
    /// must therefore only ever be applied to the topic that wrote it.
    #[test]
    fn watermark_is_scoped_to_its_own_topic() {
        use walrus_rust::WalPosition;
        let busy = serialize_watermark_to_json(&vec![Some(WalPosition { block_id: 9_000, offset: 0 })], "busy_proj", "otel_logs_and_spans");
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(busy));

        // The tenant that wrote it still gets it.
        assert_eq!(parse_watermark_from_json(&info, 1, "busy_proj", "otel_logs_and_spans"), vec![Some(WalPosition { block_id: 9_000, offset: 0 })]);
        // Another tenant sharing the same physical log must get nothing.
        assert_eq!(parse_watermark_from_json(&info, 1, "quiet_proj", "otel_logs_and_spans"), vec![None]);
        // Same project, different table is also a different topic.
        assert_eq!(parse_watermark_from_json(&info, 1, "busy_proj", "otel_metrics"), vec![None]);

        // A legacy (pre-fix) commit carries no topic. It must contribute
        // nothing rather than be applied to everyone: unattributable is
        // indistinguishable from another tenant's, and duplicates (which
        // read-side dedup removes) are always preferable to loss.
        let legacy: serde_json::Map<String, serde_json::Value> =
            [("0".to_string(), serde_json::json!({ "block_id": 9_000, "offset": 0 }))].into_iter().collect();
        let mut old = std::collections::HashMap::new();
        old.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(legacy));
        assert_eq!(parse_watermark_from_json(&old, 1, "any_proj", "otel_logs_and_spans"), vec![None]);
    }

    /// All-None watermark serializes to an empty object, which
    /// `build_watermark_commit_properties` turns into a default
    /// `CommitProperties` (no metadata written). Recovery sees no key and
    /// silently skips the commit — same path as old commits from before
    /// this feature landed.
    #[test]
    fn watermark_all_none_omits_metadata() {
        let wm: crate::buffered_write_layer::DeltaWatermark = vec![None, None, None];
        assert!(serialize_watermark_to_json(&wm, "p", "t").is_empty());
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(serde_json::Map::new()));
        assert!(parse_watermark_from_json(&info, 3, "p", "t").iter().all(|p| p.is_none()));
    }

    /// Per-shard MAX across commits: a shard's position is whichever commit
    /// observed the furthest. A commit missing a shard contributes nothing
    /// (replay-derived commits without watermarks must not reset the MAX).
    #[test]
    fn watermark_max_across_commits_takes_per_shard_furthest() {
        use walrus_rust::WalPosition;
        let mk_info = |entries: &[(usize, u64, u64)]| {
            let map: serde_json::Map<String, serde_json::Value> =
                entries.iter().map(|(s, b, o)| (s.to_string(), serde_json::json!({ "block_id": b, "offset": o }))).collect();
            let mut map = map;
            map.insert(WATERMARK_TOPIC_KEY.to_string(), serde_json::Value::String(wal_topic("p", "t")));
            let mut info = std::collections::HashMap::new();
            info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(map));
            info
        };
        // Commit A: shard 0 at (5, 100), shard 1 at (5, 50)
        let a = mk_info(&[(0, 5, 100), (1, 5, 50)]);
        // Commit B: shard 0 at (6, 0) — past A on shard 0; nothing for shard 1
        let b = mk_info(&[(0, 6, 0)]);
        // Commit C: replay-derived, no watermark key at all
        let c: std::collections::HashMap<String, serde_json::Value> = std::collections::HashMap::new();
        // Commit D: shard 1 at (5, 30) — BEHIND A on shard 1; must lose to A
        let d = mk_info(&[(1, 5, 30)]);

        let max = max_watermark_across_commits([&a, &b, &c, &d], 3, "p", "t");
        assert_eq!(max[0], Some(WalPosition { block_id: 6, offset: 0 }));
        assert_eq!(max[1], Some(WalPosition { block_id: 5, offset: 50 }));
        assert_eq!(max[2], None, "shard 2 unwritten by all commits stays None");
    }

    /// C3 crash-recovery invariant: ONE coalesced commit carries every included
    /// project's watermark, and on replay each project resumes from ITS OWN
    /// position. No project may inherit, skip, or lose a position because a
    /// co-tenant's watermark rode the same commit — positions are per-topic
    /// walrus offsets and are not comparable across topics (applying a busy
    /// tenant's offset to a quiet one skips unreplayed entries = acked-write
    /// loss on every boot).
    #[test]
    fn coalesced_commit_resumes_each_project_from_its_own_watermark() {
        use walrus_rust::WalPosition;
        let t = "otel_logs_and_spans";
        let a = vec![Some(WalPosition { block_id: 900, offset: 10 }), None];
        let b = vec![None, Some(WalPosition { block_id: 4, offset: 7 })];
        let c = vec![Some(WalPosition { block_id: 1, offset: 1 }), Some(WalPosition { block_id: 2, offset: 2 })];
        let json = serialize_watermarks_to_json([
            ("proj_a".to_string(), t.to_string(), a.clone()),
            ("proj_b".to_string(), t.to_string(), b.clone()),
            ("proj_c".to_string(), t.to_string(), c.clone()),
        ]);
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(json));

        assert_eq!(parse_watermark_from_json(&info, 2, "proj_a", t), a, "proj_a must resume from its own position");
        assert_eq!(parse_watermark_from_json(&info, 2, "proj_b", t), b, "proj_b must resume from its own position");
        assert_eq!(parse_watermark_from_json(&info, 2, "proj_c", t), c, "proj_c must resume from its own position");
        // A project that was NOT in the commit gets nothing — never a co-tenant's
        // (far ahead) position.
        assert_eq!(parse_watermark_from_json(&info, 2, "proj_d", t), vec![None, None]);
        // Same project, different table is a different topic.
        assert_eq!(parse_watermark_from_json(&info, 2, "proj_a", "otel_metrics"), vec![None, None]);
        // And the per-shard MAX across a coalesced + a per-project commit still
        // resolves per project (boot-time cursor derivation).
        let mut solo = std::collections::HashMap::new();
        solo.insert(
            WAL_WATERMARK_KEY.to_string(),
            serde_json::Value::Object(serialize_watermark_to_json(&vec![Some(WalPosition { block_id: 901, offset: 0 }), None], "proj_a", t)),
        );
        let max = max_watermark_across_commits([&info, &solo], 2, "proj_a", t);
        assert_eq!(max, vec![Some(WalPosition { block_id: 901, offset: 0 }), None]);
        assert_eq!(max_watermark_across_commits([&info, &solo], 2, "proj_b", t), b, "proj_b unaffected by proj_a's later solo commit");
    }

    /// A one-project "coalesced" commit must serialize to the EXACT legacy flat
    /// shape — the non-coalesced path and any older binary reading these commits
    /// must see no format change at all.
    #[test]
    fn single_project_coalesced_watermark_keeps_legacy_shape() {
        use walrus_rust::WalPosition;
        let wm = vec![Some(WalPosition { block_id: 3, offset: 4 }), None];
        assert_eq!(
            serialize_watermarks_to_json([("p".to_string(), "t".to_string(), wm.clone())]),
            serialize_watermark_to_json(&wm, "p", "t"),
            "single-topic coalesced commits must keep the flat legacy shape byte-for-byte"
        );
        // All-None topics drop out entirely; a commit with nothing to say writes
        // no metadata (recovery silently skips it).
        assert!(serialize_watermarks_to_json([("p".to_string(), "t".to_string(), vec![None, None])]).is_empty());
        // Two topics where only one has positions collapses to the flat form.
        let one = serialize_watermarks_to_json([("p".to_string(), "t".to_string(), wm.clone()), ("q".to_string(), "t".to_string(), vec![None, None])]);
        assert_eq!(one, serialize_watermark_to_json(&wm, "p", "t"));
    }

    /// Two units for the SAME topic in one commit (should not happen — the flush
    /// layer coalesces per (project, table) first — but must never silently drop
    /// one): the survivor takes the per-shard MAX, so it can never sit BEHIND a
    /// contributor's rows that this commit made durable.
    #[test]
    fn duplicate_topic_in_one_commit_takes_per_shard_max() {
        use walrus_rust::WalPosition;
        let json = serialize_watermarks_to_json([
            ("p".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 5, offset: 100 }), Some(WalPosition { block_id: 1, offset: 0 })]),
            ("p".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 5, offset: 40 }), Some(WalPosition { block_id: 9, offset: 0 })]),
            ("q".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 2, offset: 0 })]),
        ]);
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(json));
        assert_eq!(
            parse_watermark_from_json(&info, 2, "p", "t"),
            vec![Some(WalPosition { block_id: 5, offset: 100 }), Some(WalPosition { block_id: 9, offset: 0 })]
        );
    }

    /// Files are written under `project_id=<id>/`, so the path IS the per-project
    /// attribution for a commit that spanned projects — the tantivy sidecar and
    /// cache warming keep receiving only their own project's files. A
    /// single-project group returns the whole list unfiltered (identical to the
    /// per-project commit path).
    #[test]
    fn added_files_attribute_to_their_own_project() {
        let added = vec![
            "s3://b/t/project_id=alpha/date=2026-07-29/a.parquet".to_string(),
            "s3://b/t/project_id=beta/date=2026-07-29/b.parquet".to_string(),
            "s3://b/t/project_id=alpha/date=2026-07-29/c.parquet".to_string(),
        ];
        let split = attribute_added_files(added.clone(), &["alpha", "beta", "gamma"]);
        assert_eq!(split[0], vec![added[0].clone(), added[2].clone()]);
        assert_eq!(split[1], vec![added[1].clone()]);
        assert!(split[2].is_empty(), "a project that added no files gets none of its co-tenants'");
        // Single project → unfiltered, byte-for-byte the per-project behaviour
        // (works even for tables/layouts without a project_id partition segment).
        assert_eq!(
            attribute_added_files(vec!["s3://b/t/date=2026-07-29/x.parquet".to_string()], &["alpha"]),
            vec![vec!["s3://b/t/date=2026-07-29/x.parquet".to_string()]]
        );
    }

    /// Out-of-range shard indices in the JSON (e.g. a writer with more shards
    /// than this reader configures) are dropped silently. Avoids panicking
    /// on a config-skew restart.
    #[test]
    fn watermark_parse_ignores_out_of_range_shards() {
        let mut info = std::collections::HashMap::new();
        info.insert(
            WAL_WATERMARK_KEY.to_string(),
            serde_json::json!({
                "topic": "p:t",
                "0": {"block_id": 1, "offset": 10},
                "99": {"block_id": 1, "offset": 999},
                "garbage": {"block_id": 1, "offset": 0},
            }),
        );
        let parsed = parse_watermark_from_json(&info, 4, "p", "t");
        assert_eq!(parsed[0], Some(walrus_rust::WalPosition { block_id: 1, offset: 10 }));
        assert!(parsed[1..].iter().all(|p| p.is_none()));
    }

    /// `filesets_for_dates` buckets URIs by their `date=` partition and
    /// pre-seeds every requested date (so the guard can tell "empty" from
    /// "absent"). URIs outside the requested dates are dropped.
    #[test]
    fn filesets_for_dates_groups_by_partition() {
        use std::collections::HashSet;
        let d0 = chrono::NaiveDate::from_ymd_opt(2026, 6, 6).unwrap();
        let d1 = chrono::NaiveDate::from_ymd_opt(2026, 6, 5).unwrap();
        let uris = vec![
            "s3://b/t/date=2026-06-06/part-a.parquet".to_string(),
            "s3://b/t/date=2026-06-06/part-b.parquet".to_string(),
            "s3://b/t/date=2026-06-05/part-c.parquet".to_string(),
            "s3://b/t/date=2026-06-01/part-x.parquet".to_string(), // outside window
        ];
        let sets = Database::filesets_for_dates(&uris, &[d0, d1]);
        assert_eq!(sets[&d0].len(), 2);
        assert_eq!(sets[&d1], HashSet::from(["s3://b/t/date=2026-06-05/part-c.parquet".to_string()]));
        // A date with no files is still present (empty), not missing.
        let d2 = chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap();
        let sets = Database::filesets_for_dates(&uris, &[d2]);
        assert!(sets[&d2].is_empty());
    }

    /// Every clause of `select_tail_bin` is an incident scar (see its doc), so
    /// they're pinned individually here rather than through a Delta table.
    #[test]
    fn select_tail_bin_policy() {
        const TARGET: i64 = 1000;
        const SEAL: i64 = 10_000;
        let f = |path: &str, size: i64, sorted: bool, min: i64, max: i64| super::TailAdd {
            path: path.into(),
            size,
            is_sorted_run: sorted,
            event_range: Some((min, max)),
        };
        // Seal lag: a file whose newest event is past the seal is still filling
        // (prod 2026-07-20: compacting it lost every OCC race).
        let unsealed = vec![f("a", 10, false, 1, 1), f("b", 10, false, 2, SEAL + 1)];
        assert_eq!(super::select_tail_bin(&unsealed, TARGET, 2, TARGET / 4, SEAL), Vec::<String>::new(), "unsealed files leave < min_files");

        // Converged (>= 7/8 target) files are never re-selected: rewriting one
        // alone is a 1→1 rewrite forever.
        let converged = vec![f("big", 900, false, 1, 2), f("a", 10, false, 3, 4), f("b", 10, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&converged, TARGET, 2, TARGET / 4, SEAL), vec!["a", "b"]);

        // REPAIR: an oversized file that is NOT a sorted run declares no
        // `sorting_columns`, and one of those disables the reader's
        // all-or-nothing footer ordering for every scan touching the partition.
        // Nothing else rewrites it — hot-tail used to skip it as converged, and
        // the daily crons rarely survive this process's restart cadence. So it
        // is repaired, but only in the GAPS: while a project still has a
        // packable slice, that slice wins.
        let poisoned = vec![f("big_unsorted", 900, false, 1, 2), f("a", 10, false, 3, 4), f("b", 10, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&poisoned, TARGET, 2, TARGET / 4, SEAL), vec!["a", "b"], "normal packing must not be starved by a pending repair");
        let only_poison = vec![f("big_unsorted", 900, false, 1, 2)];
        assert_eq!(
            super::select_tail_bin(&only_poison, TARGET, 2, TARGET / 4, SEAL),
            vec!["big_unsorted"],
            "with no packable slice left, the tick repairs one oversized unsorted file — alone, and below min_files"
        );
        // ...and exactly one per bin, so a backlog drains across ticks instead
        // of rewriting several hundred-megabyte files in a single pass.
        let many_poison = vec![f("p1", 900, false, 1, 2), f("p2", 950, false, 3, 4), f("p3", 800, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&many_poison, TARGET, 2, TARGET / 4, SEAL).len(), 1, "one repair per bin");
        // A converged file that IS a sorted run stays done — repairing it would
        // be a 1->1 rewrite forever.
        let healthy = vec![f("big_sorted", 900, true, 1, 2)];
        assert!(super::select_tail_bin(&healthy, TARGET, 2, TARGET / 4, SEAL).is_empty(), "a converged sorted run is never re-selected");

        // Sorted runs fold only while under the cap; an over-cap run is excluded.
        let runs = vec![f("run_small", 100, true, 1, 2), f("run_big", 300, true, 3, 4), f("a", 10, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&runs, TARGET, 2, TARGET / 4, SEAL), vec!["run_small", "a"], "only the sub-cap run folds");

        // Earliest contiguous slice up to cap, ordered by EVENT time (input
        // order is deliberately scrambled) — this is what makes runs disjoint.
        let pack = vec![f("third", 600, false, 30, 31), f("first", 600, false, 10, 11), f("second", 300, false, 20, 21)];
        assert_eq!(super::select_tail_bin(&pack, TARGET, 2, TARGET / 4, SEAL), vec!["first", "second"], "packs earliest slice, stops at cap");

        // min_files gate.
        assert_eq!(super::select_tail_bin(&[f("a", 10, false, 1, 2), f("b", 10, false, 3, 4)], TARGET, 3, TARGET / 4, SEAL), Vec::<String>::new());

        // A lone over-cap-adjacent file must not wedge the pass: selection skips
        // past it to the next slice rather than returning a 1-file bin.
        let lone = vec![f("lone", 800, false, 1, 2), f("a", 300, false, 10, 11), f("b", 300, false, 12, 13)];
        assert_eq!(super::select_tail_bin(&lone, TARGET, 2, TARGET / 4, SEAL), vec!["a", "b"]);

        // Files with no event-time stats can't be binned disjointly (the
        // 2026-07-20 silent no-op read them as None and selected NOTHING).
        let no_stats = vec![super::TailAdd { path: "x".into(), size: 10, is_sorted_run: false, event_range: None }, f("a", 10, false, 1, 2)];
        assert_eq!(super::select_tail_bin(&no_stats, TARGET, 2, TARGET / 4, SEAL), Vec::<String>::new());
    }

    /// Wave commit blast radius: ONE stale bin must lose only its own actions.
    /// Naive batching would fail the whole wave (11 bins for one conflict).
    #[test]
    fn wave_drops_only_the_stale_bin() {
        let live: std::collections::HashSet<String> = ["f1", "f2", "f4"].iter().map(|s| s.to_string()).collect();
        let bins = vec![
            ("alpha", vec!["f1".to_string()]),
            ("beta", vec!["f3".to_string()]), // f3 rewritten concurrently
            ("gamma", vec!["f2".to_string(), "f4".to_string()]),
        ];
        let (fresh, stale) = super::split_live_bins(bins, |(_, t)| t.as_slice(), &live);
        assert_eq!(fresh.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec!["alpha", "gamma"], "surviving bins still commit together");
        assert_eq!(stale.iter().map(|(n, _)| *n).collect::<Vec<_>>(), vec!["beta"]);
        // And the surviving bins' actions concatenate in removes-then-adds order
        // per bin, which is what the single CommitBuilder receives.
        let actions: Vec<&str> = fresh.iter().flat_map(|(n, _)| [*n, *n]).collect();
        assert_eq!(actions.len(), 4);
    }

    fn test_add(path: &str) -> deltalake::kernel::Add {
        deltalake::kernel::Add { path: path.to_string(), size: 1024, modification_time: 0, data_change: true, ..Default::default() }
    }

    /// A snapshot that lists one file twice must still map to ONE target, so
    /// the planners' `targets.len() != files.len()` check stays meaningful.
    /// Prod 2026-08-02: the incremental snapshot advance duplicated the file
    /// list across a checkpoint, every plan logged "mapped 2/1 files", and both
    /// dedup and hot-tail compaction stalled indefinitely.
    #[test]
    fn dedup_adds_by_path_collapses_a_duplicated_snapshot_entry() {
        let dup = vec![test_add("a.parquet"), test_add("b.parquet"), test_add("a.parquet")];
        let targets = super::dedup_adds_by_path(dup.into_iter(), "otel_metrics");
        assert_eq!(targets.len(), 2, "one Add per distinct path");
        let mut paths: Vec<&str> = targets.iter().map(|a| a.path.as_str()).collect();
        paths.sort();
        assert_eq!(paths, ["a.parquet", "b.parquet"]);
        // Order-preserving for the already-clean case: no needless churn in the
        // Remove tombstones a wave commits.
        let clean = vec![test_add("b.parquet"), test_add("a.parquet")];
        let targets = super::dedup_adds_by_path(clean.into_iter(), "otel_metrics");
        assert_eq!(targets.iter().map(|a| a.path.as_str()).collect::<Vec<_>>(), ["b.parquet", "a.parquet"]);
    }

    fn staged_unit(project: &str, paths: &[&str], dedup: Option<super::DedupUnit>) -> super::StagedBin {
        let targets: Vec<_> = paths.iter().map(|p| test_add(p)).collect();
        let staged = vec![deltalake::kernel::Action::Add(test_add(&format!("{project}-new.parquet")))];
        let (removes, adds) = super::staged_actions(&targets, staged, dedup.is_some());
        super::StagedBin {
            project_id: project.to_string(),
            wave_id: format!("wave-{project}"),
            target_paths: paths.iter().map(|p| p.to_string()).collect(),
            removes,
            adds,
            stage_store: Arc::new(object_store::memory::InMemory::new()),
            dedup,
        }
    }

    fn dedup_unit(date: &str, before: u64, after: u64) -> super::DedupUnit {
        super::DedupUnit { key: None, date: date.to_string(), label: "chunk".into(), before, after }
    }

    /// THE load-bearing subtlety of the shared wave commit: hot compaction
    /// preserves rows (data_change=false → snapshot-isolation downgrade →
    /// Optimize), dedup DROPS rows (data_change=true → honest OCC → Write).
    /// Flipping either direction is a correctness bug, not a tuning choice.
    #[test]
    fn staged_actions_carry_data_change_per_engine() {
        use deltalake::{kernel::Action, protocol::DeltaOperation};
        let flags = |bin: &super::StagedBin| -> Vec<bool> {
            bin.removes
                .iter()
                .chain(bin.adds.iter())
                .map(|a| match a {
                    Action::Remove(r) => r.data_change,
                    Action::Add(add) => add.data_change,
                    other => panic!("unexpected action {other:?}"),
                })
                .collect()
        };
        let hot = staged_unit("alpha", &["f1", "f2"], None);
        assert_eq!(flags(&hot), vec![false; 3], "compaction Removes AND Adds must be data-preserving");
        let dedup = staged_unit("beta", &["f3"], Some(dedup_unit("2026-07-28", 10, 7)));
        assert_eq!(flags(&dedup), vec![true; 2], "a row-dropping rewrite must not claim to preserve data");
        // The operation is derived from the same flag so the two can't drift.
        assert!(matches!(super::wave_operation(false, 256, None), DeltaOperation::Optimize { .. }));
        assert!(matches!(super::wave_operation(true, 256, Some(vec!["date".into()])), DeltaOperation::Write { .. }));
    }

    /// A dedup wave has the same blast radius as a hot wave: the unit whose
    /// target file was rewritten concurrently drops out alone.
    #[test]
    fn dedup_wave_drops_only_the_stale_unit() {
        let live: std::collections::HashSet<String> = ["f1", "f4"].iter().map(|s| s.to_string()).collect();
        let units = vec![
            staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6))),
            staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 5, 4))), // f2 gone
            staged_unit("gamma", &["f4"], Some(dedup_unit("2026-07-27", 8, 8))),
        ];
        let (fresh, stale) = super::split_live_bins(units, |b| &b.target_paths, &live);
        assert_eq!(fresh.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["alpha", "gamma"]);
        assert_eq!(stale.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["beta"]);
    }

    /// The self-landed split (prod 2026-07-30 follow-up). "Targets gone" has TWO
    /// causes and they need opposite handling: another writer rewrote them
    /// (staged parquet is garbage → delete), or our OWN earlier attempt landed
    /// and then reported an error (staged parquet is LIVE DATA → never delete,
    /// credit the bin). The bin's own Adds are what tells them apart.
    #[test]
    fn a_bin_whose_own_adds_are_live_is_self_landed_not_stale() {
        let alpha = staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6)));
        let beta = staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 5, 4)));
        // alpha's commit LANDED: its target is gone AND its staged file is now
        // active. beta's target was rewritten by someone else.
        let live: std::collections::HashSet<String> = ["alpha-new.parquet"].iter().map(|s| s.to_string()).collect();
        let (fresh, stale) = super::split_live_bins(vec![alpha, beta], |b| &b.target_paths, &live);
        assert!(fresh.is_empty(), "neither bin's targets survive");
        let (self_landed, stale): (Vec<_>, Vec<_>) = stale.into_iter().partition(|b| super::bin_adds_live(b, &live));
        assert_eq!(self_landed.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["alpha"], "a landed bin must never be discarded");
        assert_eq!(stale.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["beta"]);
        // Its rows really were dropped from the table, so they count.
        assert_eq!(super::wave_dropped_rows(&self_landed), 4);
        // And a bin whose targets ARE live is never mistaken for self-landed.
        let live_targets: std::collections::HashSet<String> = ["f1"].iter().map(|s| s.to_string()).collect();
        assert!(!super::bin_adds_live(&staged_unit("alpha", &["f1"], None), &live_targets));
    }

    /// A commit-lock holder must free the lock on a bounded schedule even when
    /// the object store never answers, and must route the abandoned commit to
    /// the UNCONFIRMED-landing branch (leave staged parquet, requeue the bins) —
    /// never to `NotLanded`, which authorizes deleting files a landed commit
    /// may reference.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_timed_out_commit_frees_the_lock_and_lands_unconfirmed() {
        let lock: Arc<tokio::sync::Mutex<()>> = Arc::default();
        let started = std::time::Instant::now();
        let failure = {
            let _guard = lock.lock().await;
            assert!(lock.try_lock().is_err(), "held while the commit is in flight");
            super::bounded_commit_await(
                std::time::Duration::from_millis(50),
                "wave_commit",
                "otel_logs_and_spans",
                // The hung R2 request of the 2026-07-30 incident.
                futures::future::pending::<std::result::Result<(), String>>(),
            )
            .await
            .expect_err("a never-answering commit must be abandoned")
        };
        assert!(failure.timed_out, "the failure must be marked as an abandoned await, not a plain error");
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "the bound, not the store, decides when the lock is freed");
        assert!(lock.try_lock().is_ok(), "the next committer must find the lock free");
        // The routing decision the wave/flush paths make with `timed_out`.
        assert!(matches!(super::probe_after_timeout(super::CommitProbe::NotLanded, failure.timed_out), super::CommitProbe::Inconclusive));
        assert!(matches!(super::probe_after_timeout(super::CommitProbe::Inconclusive, true), super::CommitProbe::Inconclusive));
        // Positive evidence still passes through — a landed commit is credited.
        assert!(matches!(super::probe_after_timeout(super::CommitProbe::Landed, true), super::CommitProbe::Landed));
        // Without a timeout, a probe's "did not land" is trusted (that IS how
        // orphaned staged parquet gets reclaimed).
        assert!(matches!(super::probe_after_timeout(super::CommitProbe::NotLanded, false), super::CommitProbe::NotLanded));
    }

    /// The backstop must be invisible on the happy path and must not swallow
    /// ordinary commit errors into the unconfirmed branch.
    #[tokio::test]
    async fn bounded_commit_await_passes_success_and_errors_through() {
        let ok: std::result::Result<u8, super::CommitFailure> =
            super::bounded_commit_await(std::time::Duration::from_secs(30), "flush_commit", "t", async { Ok::<_, String>(7u8) }).await;
        assert_eq!(ok.map_err(|e| e.message), Ok(7));
        let err = super::bounded_commit_await(std::time::Duration::from_secs(30), "flush_commit", "t", async { Err::<(), _>("version already exists") })
            .await
            .expect_err("errors propagate");
        assert!(!err.timed_out, "a real commit error must keep its normal (probe/OCC) classification");
        assert_eq!(err.message, "version already exists");
    }

    /// Dropped-row accounting must survive a PARTIAL wave: only landed units
    /// removed rows from the table. Counting a stale unit's `before - after`
    /// would report drops that never happened (and, via the dirty-bin
    /// bookkeeping, certify a bin that still holds duplicates).
    #[test]
    fn dropped_rows_count_only_landed_units() {
        let live: std::collections::HashSet<String> = ["f1"].iter().map(|s| s.to_string()).collect();
        let units = vec![
            staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6))), // drops 4
            staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 100, 1))), // never lands
        ];
        let (fresh, stale) = super::split_live_bins(units, |b| &b.target_paths, &live);
        assert_eq!(super::wave_dropped_rows(&fresh), 4);
        assert_eq!(super::wave_dropped_rows(&stale), 99, "the stale unit's rewrite is real but uncommitted — never added to the metric");
        // Hot bins carry no dedup accounting at all.
        assert_eq!(super::wave_dropped_rows(&[staged_unit("gamma", &["f9"], None)]), 0);
    }

    /// The staged-intent manifest is a cleanup aid, never a correctness input:
    /// a torn tail from an unclean shutdown must cost entries, not a boot.
    #[test]
    fn staged_intent_manifest_skips_garbage_lines() {
        let contents = concat!(
            r#"{"wave_id":"w1","project_id":"a","paths":["p1","p2"]}"#,
            "\n",
            "not json at all\n",
            "\n",
            r#"{"wave_id":"w2","project_id":"b","paths":["p3"]"#, // torn tail, no newline
        );
        let entries = super::parse_staged_intents(contents);
        assert_eq!(entries.len(), 1, "only the intact line survives: {entries:?}");
        assert_eq!(entries[0].paths, vec!["p1", "p2"]);
        assert!(super::parse_staged_intents("").is_empty());
        assert!(super::parse_staged_intents("garbage").is_empty());
    }

    /// Boot reconcile deletes ONLY what the Delta log doesn't reference — a
    /// referenced path belongs to a wave that committed, and deleting it would
    /// destroy live data.
    #[test]
    fn staged_orphan_deletions_spares_committed_files_and_foreign_tables() {
        let e = |wave: &str, table: &str, age_secs: u64, paths: &[&str]| super::StagedIntent {
            wave_id: wave.into(),
            table_name: table.into(),
            project_id: "p".into(),
            recorded_at: 100_000 - age_secs,
            paths: paths.iter().map(|s| s.to_string()).collect(),
        };
        let old = super::STAGED_INTENT_MIN_AGE_SECS + 1;
        let entries = vec![
            e("w1", "logs", old, &["committed", "orphan1"]),
            e("w2", "logs", old, &["orphan2"]),
            // Another table's entry: NOT this reconcile's to judge — its paths
            // never appear in this table's snapshot and must not be deleted.
            e("w3", "metrics", old, &["metrics_staged"]),
            // Young entry: may belong to a live instance on a shared volume
            // (rolling deploy) — left alone.
            e("w4", "logs", 10, &["young_staged"]),
        ];
        let referenced: std::collections::HashSet<String> = ["committed".to_string(), "unrelated".to_string()].into_iter().collect();
        assert_eq!(super::staged_orphan_deletions(&entries, "logs", 100_000, &referenced), vec!["orphan1", "orphan2"]);
        // Nothing to delete when every staged file landed.
        let all_live: std::collections::HashSet<String> = ["committed", "orphan1", "orphan2"].iter().map(|s| s.to_string()).collect();
        assert!(super::staged_orphan_deletions(&entries, "logs", 100_000, &all_live).is_empty());
    }

    /// Stats trimming (2026-07-29): the Add stats column list must be the narrow
    /// prune set, not the whole 90+ column schema (18.4% of CPU in stats JSON
    /// parsing), and must never include partition columns.
    #[test]
    fn stats_columns_are_the_prune_keys_only() {
        let schema = get_schema("otel_logs_and_spans").unwrap_or_else(get_default_schema);
        let stats_columns = super::stats_columns_for(schema);
        let cols: Vec<&str> = stats_columns.split(',').collect();
        assert!(cols.contains(&schema.time_column_name()), "the time column drives every query and the event-time binning");
        for key in &schema.dedup_keys {
            assert!(cols.contains(&key.as_str()), "dedup key {key} must keep stats");
        }
        for partition in &schema.partitions {
            assert!(!cols.contains(&partition.as_str()), "partition column {partition} is in the path, not the stats");
        }
        assert!(cols.len() < schema.fields.len(), "must be a strict subset of the schema, got {} of {}", cols.len(), schema.fields.len());
        assert_eq!(cols.len(), cols.iter().collect::<std::collections::HashSet<_>>().len(), "no duplicates");
    }

    /// Fairness guard for prod 2026-07-29: the old per-project drain gave the
    /// most-fragmented project all 12 bins before project #2 got its first, so
    /// on a 5-min cron most projects were never compacted. Every project must
    /// get its Nth bin before any project gets its (N+1)th.
    #[tokio::test(flavor = "multi_thread")]
    async fn round_robin_bins_gives_every_project_a_bin_before_anyone_gets_a_second() {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, usize)>::new()));
        let seen = calls.clone();
        // Every project has unbounded work, so only the round cap stops it.
        let failed = super::round_robin_bins(
            vec!["a".into(), "b".into(), "c".into()],
            3,
            1,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_, _| panic!("must not truncate: deadline is far away"),
            || None,
            |project_id, round| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push((project_id.clone(), round));
                    (project_id, Ok(super::BinOutcome::Staged(())))
                }
            },
            |_bins, _round| async { 0 },
        )
        .await;
        assert_eq!(failed, 0);
        let calls = calls.lock().unwrap().clone();
        assert_eq!(
            calls,
            vec![
                ("a".to_string(), 0),
                ("b".to_string(), 0),
                ("c".to_string(), 0),
                ("a".to_string(), 1),
                ("b".to_string(), 1),
                ("c".to_string(), 1),
                ("a".to_string(), 2),
                ("b".to_string(), 2),
                ("c".to_string(), 2),
            ],
            "expected round-robin, got a per-project drain"
        );
    }

    /// A dedup-raced bin (`Retry`) must NOT silence the project for the tick:
    /// it stays in the rotation and gets a fresh bin next round (2026-07-29
    /// review finding — `Ok(None)` overloading dropped it with the converged).
    #[tokio::test(flavor = "multi_thread")]
    async fn round_robin_bins_keeps_a_vanished_bin_project_in_rotation() {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, usize)>::new()));
        let seen = calls.clone();
        let failed = super::round_robin_bins(
            vec!["raced".into()],
            3,
            1,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_, _| panic!("must not truncate"),
            || None,
            |project_id, round| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push((project_id.clone(), round));
                    // Round 0: selection went stale under a concurrent rewrite.
                    let outcome = if round == 0 { super::BinOutcome::Retry } else { super::BinOutcome::Staged(()) };
                    (project_id, Ok(outcome))
                }
            },
            |bins: Vec<()>, _| async move {
                assert!(!bins.is_empty());
                0
            },
        )
        .await;
        assert_eq!(failed, 0);
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls.iter().filter(|(p, _)| p == "raced").count(), 3, "raced project must be retried every round, not dropped");
    }

    /// A project whose tail is converged (`Ok(false)`) must stop consuming
    /// rounds, so the remaining budget goes to projects that still have work.
    #[tokio::test(flavor = "multi_thread")]
    async fn round_robin_bins_drops_converged_and_failed_projects() {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, usize)>::new()));
        let seen = calls.clone();
        let failed = super::round_robin_bins(
            vec!["converged".into(), "busy".into(), "broken".into()],
            3,
            1,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_, _| panic!("must not truncate"),
            || None,
            |project_id, round| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push((project_id.clone(), round));
                    let outcome = match project_id.as_str() {
                        "converged" => Ok(super::BinOutcome::Converged),
                        "broken" => Err(anyhow::anyhow!("boom")),
                        _ => Ok(super::BinOutcome::Staged(())),
                    };
                    (project_id, outcome)
                }
            },
            |_bins, _round| async { 0 },
        )
        .await;
        assert_eq!(failed, 1, "the erroring project counts once, then drops out");
        let calls = calls.lock().unwrap().clone();
        assert_eq!(calls.iter().filter(|(p, _)| p == "converged").count(), 1, "converged project must not be retried");
        assert_eq!(calls.iter().filter(|(p, _)| p == "broken").count(), 1, "failed project must not be retried");
        assert_eq!(calls.iter().filter(|(p, _)| p == "busy").count(), 3, "busy project keeps its rounds");
    }

    /// The tick must stop starting rounds past its wall-clock budget rather than
    /// overrunning its own cron period (prod 2026-07-29: "still in progress
    /// after 600s" on a 300s schedule).
    #[tokio::test(flavor = "multi_thread")]
    async fn round_robin_bins_stops_at_the_tick_deadline() {
        let truncated = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(usize, usize)>::new()));
        let sink = truncated.clone();
        let failed = super::round_robin_bins(
            vec!["a".into(), "b".into()],
            12,
            1,
            std::time::Instant::now(), // already expired
            move |round, remaining: &[String]| sink.lock().unwrap().push((round, remaining.len())),
            || None,
            |project_id, _| async move { (project_id, Ok(super::BinOutcome::Staged(()))) },
            |_bins, _round| async { 0 },
        )
        .await;
        assert_eq!(failed, 0);
        assert_eq!(*truncated.lock().unwrap(), vec![(0, 2)], "must truncate on round 0 with both projects still pending");
    }

    /// Prod 2026-08-01: RSS went 50GB → 110GB (cgroup 128GB) INSIDE one round,
    /// and the brake — sampled only at the round boundary — recorded its first
    /// `Stop` after that round had already landed. A brake that can only fire
    /// between rounds cannot bound the memory a round allocates, so it must also
    /// gate ADMISSION of each bin within the round.
    ///
    /// A brake that engages after the round starts must therefore stop new bins
    /// from being admitted, leaving them pending for the boundary check to
    /// truncate — not run the whole round anyway.
    #[tokio::test(flavor = "multi_thread")]
    async fn round_robin_bins_stops_admitting_bins_when_the_brake_engages_mid_round() {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let truncated = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(usize, Vec<String>)>::new()));
        // Healthy at the round boundary; trips as soon as the first bin is
        // admitted — the shape of a wave whose own staging exhausts memory.
        let admitted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (seen, sink, gate) = (calls.clone(), truncated.clone(), admitted.clone());
        super::round_robin_bins(
            vec!["a".into(), "b".into(), "c".into()],
            3,
            1, // serial admission, so the trip point is deterministic
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            move |round, remaining: &[String]| sink.lock().unwrap().push((round, remaining.to_vec())),
            move || (gate.load(std::sync::atomic::Ordering::SeqCst) > 0).then_some(super::Brake::Stop("mem")),
            |project_id, _round| {
                let (seen, admitted) = (seen.clone(), admitted.clone());
                async move {
                    admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    seen.lock().unwrap().push(project_id.clone());
                    (project_id, Ok(super::BinOutcome::Staged(())))
                }
            },
            |_bins, _round| async { 0 },
        )
        .await;

        assert_eq!(*calls.lock().unwrap(), vec!["a".to_string()], "only the bin admitted before the brake tripped may run");
        assert_eq!(
            *truncated.lock().unwrap(),
            vec![(1, vec!["a".to_string(), "b".to_string(), "c".to_string()])],
            "the deferred bins stay pending and the NEXT round's boundary check truncates the tick"
        );
    }

    /// Prod 2026-07-29: a permanently-engaged WAL brake truncated every tick at
    /// round 0, so ZERO waves committed for hours. `Degrade` must keep a service
    /// floor — the debt-ordered head project keeps getting served every round —
    /// while `Stop` keeps the old full-truncation behaviour.
    #[tokio::test(flavor = "multi_thread")]
    async fn round_robin_bins_degrade_serves_all_projects_serially() {
        let run = |brake: Option<super::Brake>| async move {
            let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(String, usize)>::new()));
            let truncated = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(usize, Vec<String>)>::new()));
            let (seen, sink) = (calls.clone(), truncated.clone());
            super::round_robin_bins(
                vec!["head".into(), "b".into(), "c".into()],
                3,
                3,
                std::time::Instant::now() + std::time::Duration::from_secs(60),
                move |round, remaining: &[String]| sink.lock().unwrap().push((round, remaining.to_vec())),
                move || brake,
                |project_id, round| {
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push((project_id.clone(), round));
                        (project_id, Ok(super::BinOutcome::Staged(())))
                    }
                },
                |_bins, _round| async { 0 },
            )
            .await;
            (calls.lock().unwrap().clone(), truncated.lock().unwrap().clone())
        };

        let (calls, truncated) = run(Some(super::Brake::Degrade("wal"))).await;
        let serial_round = |r: usize| [("head".to_string(), r), ("b".to_string(), r), ("c".to_string(), r)];
        assert_eq!(
            calls,
            [serial_round(0), serial_round(1), serial_round(2)].concat(),
            "degrade serves every project serially — the deadline, not a cut, bounds the tick"
        );
        assert!(truncated.is_empty(), "degrade alone cuts nothing; only the deadline truncates");

        let (calls, truncated) = run(Some(super::Brake::Stop("mem"))).await;
        assert!(calls.is_empty(), "stop must start no work at all");
        assert_eq!(truncated, vec![(0, vec!["head".to_string(), "b".to_string(), "c".to_string()])]);
    }

    /// Cache keys are bucket-relative (inserts happen below the PrefixStore),
    /// so evict/contains must join the table's in-bucket path back onto the
    /// table-relative file path — probing with the bare relative path never
    /// matches (prod 2026-08-03: evictions were no-ops, confirms re-fetched).
    #[test]
    fn bucket_cache_key_restores_the_table_path_segment() {
        let prefix = super::table_cache_prefix("s3://bucket/timefusion/otel_logs_and_spans/proj-1?endpoint=x");
        assert_eq!(prefix, "s3://bucket/timefusion/otel_logs_and_spans/proj-1");
        let table_path = super::table_path_in_bucket(prefix);
        assert_eq!(table_path, "timefusion/otel_logs_and_spans/proj-1");
        let rel = super::relativize_to_prefix(prefix, "s3://bucket/timefusion/otel_logs_and_spans/proj-1/date=2026-08-03/f.parquet").unwrap();
        assert_eq!(super::bucket_cache_key(table_path, &rel), "timefusion/otel_logs_and_spans/proj-1/date=2026-08-03/f.parquet");
        assert_eq!(super::bucket_cache_key("", &rel), "date=2026-08-03/f.parquet", "bucket-rooted tables keep the bare relative key");
    }

    #[test]
    fn hot_project_ids_prioritize_the_most_fragmented_hot_partition() {
        let date = chrono::NaiveDate::from_ymd_opt(2026, 7, 16).unwrap();
        let uris = vec![
            "s3://b/t/project_id=alpha/date=2026-07-16/a.parquet".to_string(),
            "s3://b/t/project_id=beta/date=2026-07-16/b.parquet".to_string(),
            "s3://b/t/project_id=alpha/date=2026-07-16/c.parquet".to_string(),
            "s3://b/t/project_id=beta/date=2026-07-16/d.parquet".to_string(),
            "s3://b/t/project_id=beta/date=2026-07-16/e.parquet".to_string(),
            "s3://b/t/project_id=old/date=2026-07-15/f.parquet".to_string(),
            "s3://b/t/date=2026-07-16/g.parquet".to_string(),
        ];
        // beta has 3 files today, alpha 2 → beta first; the wrong-date and
        // missing-project_id URIs are excluded.
        assert_eq!(Database::hot_project_ids(&uris, date), vec!["beta", "alpha"]);
    }

    /// Two identical file sets compare equal (→ partition skipped); adding a
    /// file makes them differ (→ partition re-optimized). This is the core of
    /// the ZOrder idempotence guard.
    #[test]
    fn filesets_equal_only_when_unchanged() {
        let d = chrono::NaiveDate::from_ymd_opt(2026, 6, 6).unwrap();
        let base = vec!["s3://b/t/date=2026-06-06/a.parquet".to_string()];
        let plus = vec!["s3://b/t/date=2026-06-06/a.parquet".to_string(), "s3://b/t/date=2026-06-06/b.parquet".to_string()];
        let a = Database::filesets_for_dates(&base, &[d]);
        let b = Database::filesets_for_dates(&base, &[d]);
        let c = Database::filesets_for_dates(&plus, &[d]);
        assert_eq!(a[&d], b[&d]);
        assert_ne!(a[&d], c[&d]);
    }

    #[tokio::test]
    async fn maintenance_job_gate_serializes_full_and_light_jobs() {
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        let full = gate.clone().acquire_owned().await.unwrap();
        assert!(gate.clone().try_acquire_owned().is_err());
        drop(full);
        assert!(gate.try_acquire_owned().is_ok());
    }

    #[test]
    fn full_optimize_sorts_by_timestamp_when_enabled() {
        use deltalake::operations::optimize::OptimizeType;
        let schema = get_schema("otel_logs_and_spans").unwrap();
        let (optimize_type, declare_sorted) = full_optimize_type(schema, true);
        assert!(matches!(optimize_type, OptimizeType::SortBy(_)));
        assert!(declare_sorted);
    }

    #[test]
    fn consolidate_dedups_on_sorted_rewrite() {
        use deltalake::operations::optimize::OptimizeType;
        let schema = get_schema("otel_logs_and_spans").unwrap();
        let (optimize_type, declare_sorted) = consolidate_optimize_type(schema, true);
        let OptimizeType::SortByDedup(cols, dedup) = optimize_type else { panic!("expected SortByDedup") };
        assert_eq!(cols[0].column, "timestamp");
        assert_eq!(dedup.columns, vec!["timestamp", "id"]);
        let tb = dedup.tiebreak.expect("tiebreak from schema");
        // `updated_at` since the 2026-08-02 merge-on-read flip: the tiebreak had
        // to move off the client-supplied `observed_timestamp`, which
        // `stamp_version` would otherwise overwrite on every write.
        assert!(tb.column == "updated_at" && tb.descending);
        assert!(declare_sorted);
        // sort disabled → plain Compact, no dedup claim
        assert!(matches!(consolidate_optimize_type(schema, false), (OptimizeType::Compact, false)));
    }

    /// Helper function to extract string value from array column, handling different string array types
    fn get_str(array: &dyn Array, idx: usize) -> String {
        use datafusion::arrow::array::{LargeStringArray, StringArray, StringViewArray};
        if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            arr.value(idx).to_string()
        } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
            arr.value(idx).to_string()
        } else if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
            arr.value(idx).to_string()
        } else {
            panic!("Unsupported string array type: {:?}", array.data_type())
        }
    }

    fn create_test_config(test_id: &str) -> Arc<AppConfig> {
        let mut cfg = AppConfig::default();
        // S3/MinIO settings
        cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
        cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
        cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
        cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
        cfg.aws.aws_default_region = Some("us-east-1".to_string());
        cfg.aws.aws_allow_http = Some("true".to_string());
        // Core settings - unique per test
        cfg.core.timefusion_table_prefix = format!("test-{}", test_id);
        cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-db-{}", test_id));
        // Disable Foyer cache for tests
        cfg.cache.timefusion_foyer_disabled = true;
        Arc::new(cfg)
    }

    /// prod 2026-08-03 ~21:00Z: escalated flush sorts starved their own shared
    /// FairSpillPool and silently wrote UNSORTED files. Each plan fanned out to
    /// `MAINTENANCE_MAX_PARTITIONS` ExternalSorters (32 MB merge reservation
    /// each) plus an UNSPILLABLE SortPreservingMergeExec, so N gate permits
    /// meant ~3N pool consumers — not the 1-per-permit the gate's 512 MB math
    /// assumes. One unsorted file then cost the partition's footer ordering:
    /// reads needed a query-time SortExec (48 × ~1.5 GB blew the 30 GB query
    /// pool → Log Explorer XX000) and enrichment UPDATEs died on the 2 GiB
    /// unordered-dedup limit. The escalated sort must be ONE pool consumer:
    /// single-partition plan, no merge exec — so it survives even the 64 MB
    /// pool floor with data far larger than the pool.
    #[tokio::test]
    async fn escalated_flush_sort_is_one_pool_consumer_and_survives_a_minimum_pool() -> Result<()> {
        use arrow::array::{StringArray, TimestampMicrosecondArray};
        let mut cfg = (*create_test_config("flush-sort-floor")).clone();
        cfg.maintenance.timefusion_sort_skip_bytes = 0; // every group escalates
        cfg.maintenance.timefusion_flush_sort_pool_mb = 64; // the config floor
        let db = Database::with_config(Arc::new(cfg)).await?;

        let table = get_schema("otel_logs_and_spans").expect("registered");
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("timestamp", arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
            arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, false),
        ]));
        // ~128 MB across 16 batches — 2× the pool — with scrambled event time,
        // so an append-ordered pass-through cannot masquerade as a sort.
        let per_batch = 10_000i64;
        let batches: Vec<RecordBatch> = (0..16)
            .map(|b| {
                let ts: TimestampMicrosecondArray = (0..per_batch).map(|i| Some((b * per_batch + i).wrapping_mul(2_654_435_761) % 1_000_000_000)).collect();
                let ids = StringArray::from_iter_values((0..per_batch).map(|i| format!("{b:04}-{i:06}-{}", "x".repeat(800))));
                RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(ts), Arc::new(ids)]).unwrap()
            })
            .collect();

        let (out, escalated) = db.sort_flush_group(table, batches).await;
        assert!(escalated, "the spilling sort starved its own pool and fell back to writing unsorted");
        let FlushBatches::Ready(it) = out else { panic!("escalated path yields Ready batches") };
        let sorted: Vec<RecordBatch> = it.collect();
        let stamps: Vec<i64> = sorted
            .iter()
            .flat_map(|b| b.column_by_name("timestamp").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().values().to_vec())
            .collect();
        assert_eq!(stamps.len(), 160_000, "the sort must not lose or duplicate rows");
        assert!(stamps.windows(2).all(|w| w[0] >= w[1]), "output must honor the schema's timestamp DESC ordering");
        Ok(())
    }

    async fn setup_test_database() -> Result<(Database, SessionContext, String)> {
        let test_prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&test_prefix);
        let db = Database::with_config(cfg).await?;
        let db_arc = Arc::new(db.clone());
        let mut ctx = db_arc.create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db.setup_session_context(&mut ctx)?;
        Ok((db, ctx, test_prefix))
    }

    /// The logical-count fast path must cover the full merge-on-read lifecycle:
    /// build an exact snapshot base, resolve a newly appended tombstone as a
    /// narrow overlay, and replace DedupExec in the physical COUNT plan.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn logical_count_build_and_append_overlay_are_exact_end_to_end() -> Result<()> {
        let (db, ctx, prefix) = setup_test_database().await?;
        let project_id = format!("logical_count_{prefix}");
        let timestamp = chrono::Utc::now().timestamp_micros() - 60_000_000;
        let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(timestamp).unwrap().date_naive();
        let row = |id: &str, deleted: Option<bool>| {
            serde_json::json!({
                "timestamp": timestamp,
                "id": id,
                "name": id,
                "project_id": project_id,
                "date": date.to_string(),
                "deleted": deleted,
            })
        };

        let base = json_to_batch_for("mor_versioned", vec![row("live", None), row("gone", None)])?;
        db.insert_records_batch(&project_id, "mor_versioned", vec![base], true, None).await?;
        let key =
            crate::logical_count_index::CountPartition { project_id: project_id.clone(), table_name: "mor_versioned".to_string(), date: date.to_string() };
        db.build_logical_count_partition(&key, false).await?;

        let table_ref = db.resolve_table(&project_id, "mor_versioned").await?;
        let (index, added) = {
            let table = table_ref.read().await;
            let (_, files) = Database::logical_count_partition_snapshot(&table, &project_id, &date.to_string())?;
            db.logical_count_memory_for_files(&project_id, "mor_versioned", &date.to_string(), &files.into_iter().collect())
                .expect("built index must be memory-resident")
        };
        assert!(added.is_empty());
        assert_eq!(index.count(timestamp, timestamp + 1), 2);

        let tombstone = json_to_batch_for("mor_versioned", vec![row("gone", Some(true))])?;
        db.insert_records_batch(&project_id, "mor_versioned", vec![tombstone], true, None).await?;

        let sql = format!(
            "SELECT COUNT(*) FROM mor_versioned WHERE project_id = '{project_id}' AND timestamp >= to_timestamp_micros({timestamp}) AND timestamp < to_timestamp_micros({})",
            timestamp + 1
        );
        let frame = ctx.sql(&sql).await?;
        let plan = frame.create_physical_plan().await?;
        let rendered = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
        assert!(!rendered.contains("DedupExec"), "logical-count pushdown did not fire:\n{rendered}");
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
        let count = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().expect("COUNT returns Int64").value(0);
        assert_eq!(count, 1, "the appended tombstone must retire its base winner");
        Ok(())
    }

    /// Per-context RuntimeEnvs each granted the full memory budget, so N
    /// contexts oversubscribed the cgroup N× — the pool must be process-wide,
    /// including across `Database` clones (bootstrap clones the db).
    #[tokio::test]
    async fn session_contexts_share_one_memory_pool() -> Result<()> {
        let cfg = create_test_config("pool-share");
        let db = Database::with_config(cfg).await?;
        let ctx1 = Arc::new(db.clone()).create_session_context();
        let ctx2 = Arc::new(db.clone()).create_session_context();
        assert!(Arc::ptr_eq(&ctx1.runtime_env(), &ctx2.runtime_env()), "contexts must share one RuntimeEnv/memory pool");
        Ok(())
    }

    /// prod 2026-07-30: a dedup dirty-bin drain held every heavy rewrite permit
    /// for 25+ min and starved hot compaction (62 bins selected, 1 wave in 35
    /// min). The wave engine must hold its OWN permits, so exhausting the heavy
    /// semaphore leaves wave staging fully able to proceed.
    #[tokio::test]
    async fn wave_staging_permits_are_independent_of_heavy_rewrite_permits() -> Result<()> {
        let db = Database::with_config(create_test_config("rewrite-sem-split")).await?;
        assert!(!Arc::ptr_eq(&db.maintenance_rewrite_sem, &db.light_rewrite_sem), "wave staging must not share the heavy rewrite semaphore");
        assert_eq!(db.light_rewrite_sem.available_permits(), db.config.derived.max_light_optimize_k().max(1));
        // Dedup/optimize/recompress take every heavy permit…
        let heavy = db.maintenance_rewrite_sem.clone().acquire_many_owned(db.maintenance_rewrite_sem.available_permits() as u32).await?;
        assert_eq!(db.maintenance_rewrite_sem.available_permits(), 0);
        // …and a wave still stages immediately.
        assert!(db.light_rewrite_sem.try_acquire().is_ok(), "hot-compact waves must not wait on a dedup drain");
        drop(heavy);
        Ok(())
    }

    /// Regression for issue #83: the Delta commit lock must be per physical
    /// table, not process-wide, or flush commits to independent tables
    /// needlessly serialize and cap throughput. Two default projects share the
    /// unified table's single log → one lock; different tables → independent
    /// locks; commit and DML locks are distinct critical sections.
    #[tokio::test]
    async fn commit_lock_is_per_physical_table() -> Result<()> {
        let db = Database::with_config(create_test_config("commit-lock-key")).await?;
        let a = db.commit_lock("proj_a", "otel_logs_and_spans").await;
        let b = db.commit_lock("proj_b", "otel_logs_and_spans").await;
        let c = db.commit_lock("proj_a", "metrics").await;
        assert!(Arc::ptr_eq(&a, &b), "default projects on a unified table must share one commit lock");
        assert!(!Arc::ptr_eq(&a, &c), "different tables must get independent commit locks");
        assert!(!Arc::ptr_eq(&a, &db.dml_lock("proj_a", "otel_logs_and_spans").await), "commit and DML locks must be distinct");
        Ok(())
    }

    /// prod 2026-07-30: the commit lock is FIFO and every holder is bounded, but
    /// a backlogged tick queued several legally-minutes-long wave commits ahead
    /// of the flush path — flush waited >600s to ACQUIRE and its watchdog killed
    /// the attempt (`commit_lock_timeouts` stayed 0: nobody hung, flush starved).
    /// Durability outranks maintenance, so a wave with a flush already queued
    /// must NOT enqueue: it requeues its bins and counts the yield. With no
    /// waiter it commits exactly as before.
    #[tokio::test]
    async fn wave_commit_yields_to_a_waiting_flush() -> Result<()> {
        use datafusion::arrow::{
            array::{Int32Array, RecordBatch},
            datatypes::{DataType as ArrowDataType, Field, Schema},
        };
        use deltalake::{
            kernel::{DataType, PrimitiveType, StructField},
            protocol::SaveMode,
        };
        use std::sync::atomic::Ordering::Relaxed;

        let db = Database::with_config(create_test_config("wave-flush-priority")).await?;
        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///wave_flush_priority")?;
        let table = DeltaTableBuilder::from_url(url.clone())?
            .with_storage_backend(mem, url)
            .build()?
            .create()
            .with_columns(vec![StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true)])
            .await?;
        let arrow = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Int32, true)]));
        let table = table.write(vec![RecordBatch::try_new(arrow, vec![Arc::new(Int32Array::from(vec![1, 2])) as _])?]).with_save_mode(SaveMode::Append).await?;
        let target = table.snapshot()?.log_data().iter().map(|f| f.path().into_owned()).next().expect("the written file is live");
        let version = table.version();
        let table_ref = Arc::new(RwLock::new(table));
        // The wave keys on ("", table) — the same key every flush committer uses.
        let bin = || vec![staged_unit("alpha", &[target.as_str()], None)];

        // A flush queued on the lock ⇒ the wave stands down without committing.
        let waiter = FlushWaiter::register(&db.flush_waiters("", "otel_logs_and_spans").await);
        let yields = crate::metrics::maintenance_stats().wave_commits_yielded_to_flush.load(Relaxed);
        let deferred = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], false, bin(), 0).await;
        assert_eq!(crate::metrics::maintenance_stats().wave_commits_yielded_to_flush.load(Relaxed), yields + 1, "the yield is counted, not silent");
        assert!(deferred.landed.is_empty(), "nothing may land while a flush waits");
        assert_eq!(deferred.failed.len(), 1, "the bin is requeued (dedup's dirty bin must not be certified clean)");
        assert_eq!(table_ref.read().await.version(), version, "no commit was attempted");

        // Flush done ⇒ the very same wave commits.
        drop(waiter);
        let landed = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], false, bin(), 0).await;
        assert_eq!(crate::metrics::maintenance_stats().wave_commits_yielded_to_flush.load(Relaxed), yields + 1, "no yield without a waiter");
        assert_eq!(landed.landed.len(), 1);
        assert!(landed.failed.is_empty());
        assert_eq!(table_ref.read().await.version(), version.map(|v| v + 1), "the wave commits as before once no flush is queued");
        Ok(())
    }

    /// issue #82 follow-up: a slow/stuck coalescer drain (Delta commits on a
    /// dead backend) must not overrun the stop grace. Holding the drain lock
    /// makes `drain()` block on acquisition; `shutdown_by` must honor its
    /// deadline and return rather than hang (which would keep wal.lock held
    /// until the orchestrator SIGKILLs us).
    #[tokio::test]
    async fn shutdown_by_bounds_a_blocked_dml_drain() -> Result<()> {
        let db = Database::with_config(create_test_config("shutdown-drain-bound")).await?;
        let coalescer = Arc::new(crate::dml_coalescer::DmlCoalescer::new(600, true));
        let _ = db.dml_coalescer.set(coalescer.clone());
        let _held = coalescer.lock_drain_for_test().await; // drain() blocks on this
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
        let res = tokio::time::timeout(std::time::Duration::from_secs(5), db.shutdown_by(deadline)).await;
        assert!(res.is_ok(), "shutdown_by hung on a blocked drain instead of honoring the deadline");
        Ok(())
    }

    /// Regression guard for the 2026-06-11 prod planning-stall convoy: every
    /// query refreshes the unified table via `refresh_table_snapshot`, and the
    /// old implementation held the table WRITE lock across `update_state()`
    /// (full log replay + object-store IO — 1s+ per post-flush refresh on
    /// prod's 40k-action log), so all concurrent reads convoyed behind it for
    /// 50-110s during flush passes. Pin the fix: while a refresh runs against
    /// a deliberately slow object store, read-lock acquisition must stay fast.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_table_snapshot_does_not_block_readers() -> Result<()> {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};

        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///convoy_tbl")?;
        let fast = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()).build()?;
        let table = fast.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?;
        assert_eq!(table.version(), Some(0));

        // Same store, but every list/get pays a delay — makes update_state
        // slow the way prod's R2-backed log replay is.
        let wait = std::time::Duration::from_millis(100);
        let throttled = ThrottledStore::new(
            mem,
            ThrottleConfig { wait_get_per_call: wait, wait_list_per_call: wait, wait_list_with_delimiter_per_call: wait, ..Default::default() },
        );
        let slow = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(Arc::new(throttled), url).build()?;
        let shared = Arc::new(RwLock::new(slow));

        let refresher = {
            let shared = Arc::clone(&shared);
            tokio::spawn(async move { refresh_table_snapshot(&shared, true).await })
        };

        // Sample read-lock acquisition latency while the refresh is in flight.
        let mut max_wait = std::time::Duration::ZERO;
        let started = std::time::Instant::now();
        while !refresher.is_finished() && started.elapsed() < std::time::Duration::from_secs(30) {
            let t0 = std::time::Instant::now();
            drop(shared.read().await);
            max_wait = max_wait.max(t0.elapsed());
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let refresh_took = started.elapsed();
        let version = refresher.await?.map_err(|e| anyhow::anyhow!(e))?;

        assert_eq!(version, Some(0), "refresh resolved the table snapshot");
        assert!(refresh_took >= wait, "throttle must make the refresh measurably slow (took {refresh_took:?})");
        assert!(
            max_wait < wait / 2,
            "readers stalled {max_wait:?} behind an in-flight refresh (refresh took {refresh_took:?}) — write lock is being held across update_state"
        );
        Ok(())
    }

    /// Tier-C correctness guard: advancing a materialized snapshot incrementally
    /// across a `replace_where` (Add + Remove) must yield exactly the active
    /// file set a full re-materialize produces. This is the path the
    /// dedup/compaction sweeps take (`with_incremental_advance`) and that
    /// `refresh_table_snapshot(.., true)` → `advance_catchup` takes on catch-up.
    /// Drift here silently corrupts query results — by keeping a tombstoned file
    /// or dropping a live one — so it must be pinned in this repo, not only in
    /// the fork's EagerSnapshot tests.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_incremental_matches_full_across_removes() -> Result<()> {
        use datafusion::arrow::{
            array::{Int32Array, RecordBatch, StringArray},
            datatypes::{DataType as ArrowDataType, Field, Schema},
        };
        use deltalake::{
            kernel::{DataType, PrimitiveType, StructField},
            protocol::SaveMode,
        };

        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///tierc_removes")?;
        let backend = || DeltaTableBuilder::from_url(url.clone()).unwrap().with_storage_backend(mem.clone(), url.clone());

        // v0: partitioned table.
        let cols = vec![
            StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true),
            StructField::new("p", DataType::Primitive(PrimitiveType::String), true),
        ];
        let table = backend().build()?.create().with_columns(cols).with_partition_columns(["p".to_string()]).await?;

        let schema = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Int32, true), Field::new("p", ArrowDataType::Utf8, true)]));
        let batch = |ids: Vec<i32>, ps: Vec<&str>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids)) as _, Arc::new(StringArray::from(ps)) as _]).unwrap()
        };

        // v1: append p=a, v2: append p=b (two partition files). These plain writes
        // set no incremental flag, so the returned `table` is the authoritative
        // full re-materialize at every step.
        let table = table.write(vec![batch(vec![1, 2], vec!["a", "a"])]).with_save_mode(SaveMode::Append).await?;
        let table = table.write(vec![batch(vec![3], vec!["b"])]).with_save_mode(SaveMode::Append).await?;
        assert_eq!(table.version(), Some(2));

        // v3: replace_where p=a → tombstones v1's file, adds a new one (Add + Remove).
        let table = table.write(vec![batch(vec![10, 11], vec!["a", "a"])]).with_save_mode(SaveMode::Overwrite).with_replace_where("p = 'a'").await?;
        assert_eq!(table.version(), Some(3));

        let uris = |t: &DeltaTable| t.get_file_uris().map(|it| it.collect::<std::collections::HashSet<String>>()).unwrap_or_default();
        let truth = uris(&table); // authoritative v3 set (full re-materialize)
        assert_eq!(truth.len(), 2, "v3 active set = p=b file + replaced p=a file");

        // Stale handle pinned at v2. Drive the Tier-C catch-up directly (the
        // path refresh_table_snapshot takes) and assert it RETURNED TRUE — i.e.
        // actually took the incremental path across the replace_where, rather
        // than silently falling back to a full update_state (which would also
        // produce a correct set and so hide a broken incremental path).
        let mut stale = backend().with_version(2).load().await?;
        assert!(stale.state.as_ref().is_some_and(|s| s.has_materialized_files()), "stale handle must be materialized to exercise the fast path");
        let log_store = stale.log_store();
        let took_fast_path = stale.state.as_mut().unwrap().advance_catchup(log_store.as_ref(), REFRESH_APPEND_CATCHUP_MAX_GAP).await?;
        assert!(took_fast_path, "advance_catchup must take the incremental path over the replace_where, not fall back to a full update");
        assert_eq!(stale.version(), Some(3), "incremental catch-up reached the latest version");
        assert_eq!(uris(&stale), truth, "incremental advance across replace_where must equal the full re-materialize");
        Ok(())
    }

    /// Pins the checkpoint-tombstone fix: tables that predate the
    /// `delta.deletedFileRetentionDuration` property (prod sat at delta's
    /// 7-day default and accumulated 38.5k Remove tombstones per checkpoint)
    /// get the property set once at load, idempotently.
    #[tokio::test(flavor = "multi_thread")]
    async fn ensure_deleted_file_retention_sets_property_once() -> Result<()> {
        const KEY: &str = "delta.deletedFileRetentionDuration";
        const CP_KEY: &str = "delta.checkpointInterval";
        let props = |hours: u64| HashMap::from([(KEY.to_string(), format!("interval {hours} hours")), (CP_KEY.to_string(), "10".to_string())]);
        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///retention_tbl")?;
        let t = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem, url).build()?;
        let table = t.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?;
        assert!(!table.snapshot()?.metadata().configuration().contains_key(KEY), "fresh table has no retention property");

        let table = ensure_table_properties(table, props(24)).await;
        let config = table.snapshot()?.metadata().configuration().clone();
        assert_eq!(config.get(KEY).map(String::as_str), Some("interval 24 hours"));
        assert_eq!(config.get(CP_KEY).map(String::as_str), Some("10"), "checkpoint interval retrofitted alongside");
        assert_eq!(table.version(), Some(1), "properties set in one commit");

        let table = ensure_table_properties(table, props(24)).await;
        assert_eq!(table.version(), Some(1), "matching properties must not commit again");

        // Retention reconfiguration (e.g. env change) re-reconciles.
        let table = ensure_table_properties(table, props(48)).await;
        assert_eq!(table.snapshot()?.metadata().configuration().get(KEY).map(String::as_str), Some("interval 48 hours"));
        assert_eq!(table.version(), Some(2));
        Ok(())
    }

    /// `refresh_table_snapshot` on an already-current table must not pay a
    /// `_delta_log` LIST (LISTs bypass the Foyer cache, so this was per-query
    /// S3 metadata traffic): the immutable-commit probe (GET version+1 → 404)
    /// short-circuits the refresh. Pinned by making LIST prohibitively slow —
    /// a current-table refresh stays fast, and a genuinely stale one must
    /// still observe the new commit.
    #[tokio::test(flavor = "multi_thread")]
    async fn refresh_table_snapshot_probes_instead_of_listing() -> Result<()> {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};

        let mem = Arc::new(object_store::memory::InMemory::new());
        let url = Url::parse("memory:///probe_tbl")?;
        let fast = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(mem.clone(), url.clone()).build()?;
        let table = fast.create().with_columns(get_default_schema().columns().unwrap_or_default()).await?;

        let list_wait = std::time::Duration::from_secs(2);
        let throttled =
            ThrottledStore::new(mem, ThrottleConfig { wait_list_per_call: list_wait, wait_list_with_delimiter_per_call: list_wait, ..Default::default() });
        let mut slow = DeltaTableBuilder::from_url(url.clone())?.with_storage_backend(Arc::new(throttled), url).build()?;
        slow.update_state().await?; // initial load pays the LIST
        let shared = Arc::new(RwLock::new(slow));

        let t0 = std::time::Instant::now();
        assert_eq!(refresh_table_snapshot(&shared, true).await.map_err(|e| anyhow::anyhow!(e))?, Some(0));
        assert!(t0.elapsed() < list_wait, "current-table refresh paid a LIST ({:?})", t0.elapsed());

        // External commit → the probe finds {v+1}.json and the refresh must
        // run the full update to pick it up.
        let _ = ensure_table_properties(table, HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())])).await;
        assert_eq!(refresh_table_snapshot(&shared, true).await.map_err(|e| anyhow::anyhow!(e))?, Some(1));
        Ok(())
    }

    /// `scoped_file_uris` replaces `get_file_uris()` in every warm/evict diff,
    /// so the load-bearing invariant is that it produces the SAME URIs — an
    /// off-by-one in the `Path::parse`/`to_uri` reconstruction would silently
    /// make every file look both added and removed (mass re-warm + wrong
    /// evictions). Also pins the scoping: partition markers must select exactly
    /// the matching files, and a non-matching marker must select none.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn scoped_file_uris_matches_get_file_uris_and_filters_by_partition() -> Result<()> {
        let (db, _ctx, prefix) = setup_test_database().await?;
        let (p1, p2) = (format!("sfu_a_{prefix}"), format!("sfu_b_{prefix}"));
        for pid in [&p1, &p2] {
            let batch = json_to_batch(vec![test_span("sfu1", "span", pid)])?;
            db.insert_records_batch(pid, "otel_logs_and_spans", vec![batch], true, None).await?;
        }
        let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");
        let table = table_ref.read().await;

        let expected: Vec<String> = table.get_file_uris()?.collect();
        assert!(!expected.is_empty(), "expected active files");
        assert_eq!(scoped_file_uris(&table, &[]), expected, "unscoped walk must be byte-identical to get_file_uris()");

        let marker = format!("project_id={p1}/");
        let scoped = scoped_file_uris(&table, &[marker.as_str()]);
        assert!(!scoped.is_empty() && scoped.len() < expected.len(), "scope must select a proper non-empty subset");
        assert!(scoped.iter().all(|u| u.contains(&marker)), "every scoped URI is in the scoped partition");
        assert_eq!(scoped, expected.iter().filter(|u| u.contains(&marker)).cloned().collect::<Vec<_>>(), "scoped walk must equal the filtered full walk");
        assert!(scoped_file_uris(&table, &["project_id=no_such_project"]).is_empty(), "a non-matching scope selects nothing");
        Ok(())
    }

    /// End-to-end test of `recompress_partition`. Skip behavior is the
    /// load-bearing property: if the footer-tier probe breaks, the daily
    /// cron rewrites every partition every night. We assert via file-set
    /// comparison since the production code path itself reads the footer.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_recompress_partition_skip_idempotency() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("project_{}", prefix);
            let today = chrono::Utc::now().date_naive();

            // Two rows across two commits → the partition holds >1 file, so the
            // rewrite genuinely merges (not a trivial single-file no-op).
            for (id, name) in [("rc1", "span1"), ("rc2", "span2")] {
                let batch = json_to_batch(vec![test_span(id, name, &project_id)])?;
                db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
            }

            let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");

            // Data-integrity baseline: the replace_where rewrite must preserve
            // every row verbatim (guards data loss / corruption from the
            // streaming overwrite path, not just that *some* rewrite happened).
            let rows_sql = format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY id");
            let ids_before = ctx.sql(&rows_sql).await?.collect().await?;
            let count_before: usize = ids_before.iter().map(|b| b.num_rows()).sum();
            assert_eq!(count_before, 2, "baseline must have both rows");

            // First recompress at tier 9 — must rewrite files.
            let files_before: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert!(!files_before.is_empty(), "expected files in today's partition");
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9).await?;
            let files_after: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_ne!(files_before, files_after, "first recompress must rewrite files");

            // Rows survive the rewrite unchanged.
            let ids_after = ctx.sql(&rows_sql).await?.collect().await?;
            let count_after: usize = ids_after.iter().map(|b| b.num_rows()).sum();
            assert_eq!(count_after, 2, "recompress must preserve all rows");
            assert_eq!(format!("{ids_before:?}"), format!("{ids_after:?}"), "recompress must preserve row contents verbatim");

            // Re-run at the same tier — footer probe must detect tier=9 and skip,
            // so the file set is unchanged. If skip is broken, this assertion
            // fails because Optimize emits a fresh part file.
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9).await?;
            let files_after_rerun: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_eq!(files_after, files_after_rerun, "rerun at same tier must skip");

            // Downgrade target — also skip.
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 3).await?;
            let files_after_downgrade: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_eq!(files_after, files_after_downgrade, "downgrade target must skip");

            db.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
    }

    /// Anchors the Delta-empty short-circuit correctness invariant:
    /// `delta_scan_can_be_skipped` must return `false` (the conservative default
    /// that runs the full scan) until `mark_delta_has_files` is called, and
    /// the flip is monotonic and per-(project,table). This is the load-
    /// bearing predicate for the 45% latency win — a regression that
    /// flipped polarity would silently hide post-flush data.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_delta_has_files_sticky_bit() -> Result<()> {
        let (db, _ctx, prefix) = setup_test_database().await?;
        let t = "otel_logs_and_spans";
        let p1 = format!("proj-marked-{prefix}");
        let p2 = format!("proj-unmarked-{prefix}");

        // Fresh (project, table): unknown → false (must NOT skip Delta).
        assert!(!db.delta_scan_can_be_skipped(&p1, t), "unknown projects must default to false so callers don't skip Delta");
        assert!(!db.delta_scan_can_be_skipped(&p2, t), "second unknown project also defaults to false");

        // Mark p1 as having files. delta_scan_can_be_skipped for p1 stays false
        // because the table is no longer empty — short-circuit must NOT
        // fire (otherwise we'd hide the just-flushed data).
        db.mark_delta_has_files(&p1, t);
        assert!(!db.delta_scan_can_be_skipped(&p1, t), "after mark_delta_has_files, table has files → can't skip");

        // Unrelated project: bit per-(project, table), so p2 unaffected.
        // Still false (unknown), still must scan.
        assert!(!db.delta_scan_can_be_skipped(&p2, t), "marking p1 must not affect p2's bit");

        // Re-marking is idempotent.
        db.mark_delta_has_files(&p1, t);
        assert!(!db.delta_scan_can_be_skipped(&p1, t), "re-mark is idempotent — still has files");

        // Sticky-true invariant: the populate path inside resolve_table
        // (and helpers) must NEVER downgrade an already-set true to false,
        // even if it observes version == 0 on a stale snapshot. Simulate
        // the populate path's store(false) — must be a no-op when the
        // bit is true.
        // White-box test: reach into delta_has_files via the public API
        // by re-asserting; the populate helper is private but the
        // invariant matters at the field level.
        // (For a true round-trip we'd resolve the table; setup_test_database
        // doesn't yet have a Delta-empty table to test that branch, but the
        // populate_resolve_caches docstring documents the property and the
        // implementation only ever calls store(true).)
        assert!(!db.delta_scan_can_be_skipped(&p1, t), "sticky-true: bit stays set across subsequent resolves");
        Ok(())
    }

    /// End-to-end test of the sticky-bit's load-bearing property: after a
    /// project is marked as having files, NO subsequent code path may
    /// downgrade the bit and silently hide those files from queries.
    ///
    /// The scenario this pins: a flush callback marks `(p, t)` true; a
    /// concurrent reader's `resolve_table` then races against the same
    /// (p, t) and would observe `version() == 0` on its just-loaded
    /// snapshot (delta-rs caches per-handle, update_state is async).
    /// Pre-fix, `populate_resolve_caches` would unconditionally store the
    /// false from that observation, downgrade the bit, and every
    /// subsequent scan would skip Delta — losing the just-flushed rows
    /// until process restart. The fix only ever stores `true`. The test
    /// here forces the exact sequence (mark → resolve fresh table at
    /// version 0 → assert) without needing a real concurrency race.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_delta_has_files_resolve_doesnt_downgrade() -> Result<()> {
        let (db, _ctx, prefix) = setup_test_database().await?;
        let project_id = format!("proj-{prefix}");
        let table = "otel_logs_and_spans";

        // Simulate the flush callback marking files-present for this project.
        db.mark_delta_has_files(&project_id, table);
        assert!(!db.delta_scan_can_be_skipped(&project_id, table), "post-mark: bit is true → not known empty");

        // Force a resolve of the unified table. The fresh handle reports
        // version() == 0 because nothing has been written. Pre-fix this
        // would have downgraded the bit; post-fix the sticky-true
        // invariant holds.
        let _t = db.resolve_table(&project_id, table).await?;
        assert!(
            !db.delta_scan_can_be_skipped(&project_id, table),
            "STICKY-TRUE: resolve_table observing version==0 must NOT downgrade a previously-marked bit. \
             A regression here means post-flush rows get hidden from queries."
        );

        // Resolve via the alternative path used by SELECTs (try_fast_resolve
        // → fast_resolve_cache hit) — same invariant must hold.
        let _ = db.try_fast_resolve(&project_id, table);
        assert!(!db.delta_scan_can_be_skipped(&project_id, table), "STICKY-TRUE preserved across try_fast_resolve too");

        Ok(())
    }

    /// C3 — cross-project flush commit coalescing, end to end.
    ///
    /// One tick's units for N default-storage projects must produce EXACTLY ONE
    /// Delta commit (they all share one physical `_delta_log`, and
    /// `table_lock_key` already serialized them behind one mutex), carrying every
    /// project's files and every project's watermark. Each project's result must
    /// list only its own files (path-attributed) so the tantivy sidecar keeps
    /// indexing per project, and every project's rows must be queryable from that
    /// same commit.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalesced_commit_spans_projects_in_one_delta_version() -> Result<()> {
        use walrus_rust::WalPosition;
        let (db, _ctx, prefix) = setup_test_database().await?;
        let t = "otel_logs_and_spans";
        let projects: Vec<String> = (0..3).map(|i| format!("coal{i}-{prefix}")).collect();

        // Create the table first, so the version delta below measures the
        // coalesced commit alone rather than the table-create commit.
        db.insert_records_batch(&projects[0], t, vec![json_to_batch(vec![test_span("warm", "warm", &projects[0])])?], true, None).await?;
        let table_ref = get_unified_delta_table(db.unified_tables(), t).await.expect("table created");
        let before = table_ref.read().await.version().unwrap_or(0);

        let units: Vec<CoalescedWriteUnit> = projects
            .iter()
            .enumerate()
            .map(|(i, p)| CoalescedWriteUnit {
                project_id: p.clone(),
                table_name: t.to_string(),
                batches: vec![json_to_batch(vec![test_span(&format!("c{i}"), "span", p)]).unwrap()],
                watermark: vec![Some(WalPosition { block_id: 100 + i as u64, offset: i as u64 })],
            })
            .collect();
        let results = db.insert_records_batches_coalesced(units).await;

        assert_eq!(results.len(), projects.len(), "one result per unit, in input order");
        let added: Vec<Vec<String>> = results.into_iter().map(|r| r.expect("coalesced commit failed")).collect();

        // ONE commit for all three projects — the whole point of C3.
        let after = table_ref.read().await.version().unwrap_or(0);
        assert_eq!(after, before + 1, "N default-storage projects must land in ONE Delta commit, got {} commits", after - before);

        // Every project's files are in that commit, attributed to its own
        // partition path (tantivy/warming inputs stay per project).
        for (i, project) in projects.iter().enumerate() {
            assert!(!added[i].is_empty(), "project {project} contributed no files to the coalesced commit");
            assert!(added[i].iter().all(|u| u.contains(&format!("project_id={project}/"))), "project {project} was handed a co-tenant's files: {:?}", added[i]);
        }

        // The single commit carries EVERY project's watermark, and each project
        // derives its own resume position from it (crash-recovery invariant).
        let history: Vec<_> = table_ref.read().await.history(Some(1)).await?.collect();
        assert_eq!(history.len(), 1);
        let shards = 8;
        for (i, project) in projects.iter().enumerate() {
            let parsed = parse_watermark_from_json(&history[0].info, shards, project, t);
            assert_eq!(parsed[0], Some(WalPosition { block_id: 100 + i as u64, offset: i as u64 }), "project {project} lost/mixed up its watermark");
        }
        // A project not in the commit inherits nothing.
        assert!(parse_watermark_from_json(&history[0].info, shards, "outsider", t).iter().all(Option::is_none));

        // All rows are readable from the shared commit.
        let files = table_ref.read().await.get_file_uris().map(|it| it.collect::<Vec<_>>()).unwrap_or_default();
        for project in &projects {
            assert!(files.iter().any(|u| u.contains(&format!("project_id={project}/"))), "project {project} has no active file after the coalesced commit");
        }
        Ok(())
    }

    /// C3 requirement 5: if ONE project's batches need a schema merge, it must be
    /// split OUT of the coalesced group and committed on its own (locked
    /// WriteBuilder merge path) rather than dragging every co-tenant through the
    /// slow path. Two default projects + one evolving project ⇒ exactly TWO
    /// commits: one shared, one solo — and all three succeed.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn schema_evolution_project_splits_out_of_the_coalesced_group() -> Result<()> {
        use datafusion::arrow::{
            array::{Array, StringArray},
            datatypes::{DataType, Field, Schema},
        };
        use walrus_rust::WalPosition;
        let (db, _ctx, prefix) = setup_test_database().await?;
        let t = "otel_logs_and_spans";
        let (p1, p2, evolving) = (format!("se1-{prefix}"), format!("se2-{prefix}"), format!("se3-{prefix}"));

        db.insert_records_batch(&p1, t, vec![json_to_batch(vec![test_span("warm", "warm", &p1)])?], true, None).await?;
        let table_ref = get_unified_delta_table(db.unified_tables(), t).await.expect("table created");
        let before = table_ref.read().await.version().unwrap_or(0);

        // A batch carrying a column the table schema lacks — delta-rs' Default-mode
        // RecordBatchWriter can't evolve schema on a partitioned table, so this
        // unit has no staged writer and must take the solo merge path.
        let base = json_to_batch(vec![test_span("e1", "span", &evolving)])?;
        let mut fields: Vec<Field> = base.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
        fields.push(Field::new("c3_brand_new_column", DataType::Utf8, true));
        let mut columns: Vec<Arc<dyn Array>> = base.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec![Some("evolved")])));
        let evolved_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;

        let units = vec![
            CoalescedWriteUnit {
                project_id: p1.clone(),
                table_name: t.to_string(),
                batches: vec![json_to_batch(vec![test_span("s1", "span", &p1)])?],
                watermark: vec![Some(WalPosition { block_id: 1, offset: 0 })],
            },
            CoalescedWriteUnit {
                project_id: evolving.clone(),
                table_name: t.to_string(),
                batches: vec![evolved_batch],
                watermark: vec![Some(WalPosition { block_id: 2, offset: 0 })],
            },
            CoalescedWriteUnit {
                project_id: p2.clone(),
                table_name: t.to_string(),
                batches: vec![json_to_batch(vec![test_span("s2", "span", &p2)])?],
                watermark: vec![Some(WalPosition { block_id: 3, offset: 0 })],
            },
        ];
        let results = db.insert_records_batches_coalesced(units).await;
        for (i, r) in results.iter().enumerate() {
            assert!(r.is_ok(), "unit {i} failed: {:?}", r.as_ref().err());
        }
        // Results stay in INPUT order even though the evolving unit committed last.
        assert!(results[0].as_ref().unwrap().iter().all(|u| u.contains(&format!("project_id={p1}/"))));
        assert!(results[1].as_ref().unwrap().iter().all(|u| u.contains(&format!("project_id={evolving}/"))));
        assert!(results[2].as_ref().unwrap().iter().all(|u| u.contains(&format!("project_id={p2}/"))));

        // Two commits: the shared one for p1+p2, plus the evolving project's solo
        // merge commit. Three would mean no coalescing; one would mean the merge
        // path swallowed the co-tenants.
        let after = get_unified_delta_table(db.unified_tables(), t).await.expect("table").read().await.version().unwrap_or(0);
        assert_eq!(after, before + 2, "expected 1 coalesced + 1 solo schema-evolution commit");

        // The evolution actually applied.
        let table = get_unified_delta_table(db.unified_tables(), t).await.expect("table");
        let guard = table.read().await;
        let delta_schema = guard.snapshot()?.schema();
        assert!(delta_schema.fields().any(|f| f.name() == "c3_brand_new_column"), "schema merge never landed");
        Ok(())
    }

    /// A custom-storage project has its OWN `_delta_log` and must never be
    /// coalesced into the shared unified-table commit. The grouping key IS
    /// `table_lock_key`, so isolation is structural: same key ⇒ same physical
    /// log ⇒ safe to share a commit; different key ⇒ separate commit.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn custom_storage_project_is_not_coalesced_with_default_storage() -> Result<()> {
        let (db, _ctx, prefix) = setup_test_database().await?;
        let t = "otel_logs_and_spans";
        let custom = format!("cust-{prefix}");
        db.storage_configs.write().await.insert(
            (custom.clone(), t.to_string()),
            StorageConfig {
                project_id: custom.clone(),
                table_name: t.to_string(),
                s3_bucket: "timefusion-tests".to_string(),
                s3_prefix: format!("custom-{prefix}"),
                s3_region: "us-east-1".to_string(),
                s3_access_key_id: "minioadmin".to_string(),
                s3_secret_access_key: "minioadmin".to_string(),
                s3_endpoint: Some("http://127.0.0.1:9000".to_string()),
            },
        );

        let (a, b) = (db.table_lock_key("proj_a", t).await, db.table_lock_key("proj_b", t).await);
        assert_eq!(a, b, "default-storage projects share a physical log → one coalesced commit");
        assert_ne!(db.table_lock_key(&custom, t).await, a, "custom-storage project must group (and commit) separately");
        // Same project on a different table is also a different physical log.
        assert_ne!(db.table_lock_key("proj_a", "otel_metrics").await, a);
        Ok(())
    }

    /// Provider cache invalidation on snapshot version change.
    ///
    /// The cache keyed on `(project, table) → (version, Arc<OnceCell<Provider>>)`
    /// must replace the cell when `table.version()` advances. A regression in
    /// the `if entry.0 != current_version` branch would serve stale Delta
    /// files to queries (pre-flush state forever).
    ///
    /// Strategy: do two queries to the same table, with an insert between
    /// them that adds a commit (bumping version). The second query must see
    /// the new row — proving the cached provider was rebuilt.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_delta_provider_cache_invalidates_on_version_change() -> Result<()> {
        let (db, ctx, prefix) = setup_test_database().await?;
        let project_id = format!("proj-inv-{prefix}");
        let t = "otel_logs_and_spans";

        // First commit + query.
        let batch1 = json_to_batch(vec![test_span("v1", "span1", &project_id)])?;
        db.insert_records_batch(&project_id, t, vec![batch1], true, None).await?;
        let v1 = {
            let table_ref = get_unified_delta_table(db.unified_tables(), t).await.expect("table created");
            table_ref.read().await.version().unwrap_or(0)
        };
        assert!(v1 > 0, "first commit must bump version above zero");
        let count1 = ctx.sql(&format!("SELECT count(*) AS c FROM {} WHERE project_id = '{}'", t, project_id)).await?.collect().await?;
        let c1 = count1[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().expect("count column").value(0);
        assert_eq!(c1, 1, "first query sees the v=1 row");
        assert_eq!(db.delta_provider_cache.len(), 1, "provider cache must retain the resolved provider for the warm query");
        let stats = ctx.sql("SELECT value FROM timefusion_stats WHERE component = 'scan' AND key = 'provider_cache_entries'").await?.collect().await?;
        let entries = stats[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().expect("stats value").value(0);
        assert_eq!(entries, "1", "timefusion_stats must observe the live provider cache, not a cloned startup snapshot");

        // Second commit advances the snapshot version.
        let batch2 = json_to_batch(vec![test_span("v2", "span2", &project_id)])?;
        db.insert_records_batch(&project_id, t, vec![batch2], true, None).await?;
        let v2 = {
            let table_ref = get_unified_delta_table(db.unified_tables(), t).await.expect("table created");
            table_ref.read().await.version().unwrap_or(0)
        };
        assert!(v2 > v1, "second commit must advance version");

        // Second query: if the provider cache served the stale v=v1 cell,
        // the count would be 1 (just the first row). With invalidation, it
        // sees both rows.
        let count2 = ctx.sql(&format!("SELECT count(*) AS c FROM {} WHERE project_id = '{}'", t, project_id)).await?.collect().await?;
        let c2 = count2[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().expect("count column").value(0);
        assert_eq!(
            c2, 2,
            "STALE CACHE REGRESSION: second query must see the row added at v=v{v2}. \
             Got {c2}/2 — the delta_provider_cache version-mismatch branch is broken."
        );
        assert_eq!(db.delta_provider_cache.len(), 1, "version invalidation adds a version to the key's ring, it does not add a key");
        // Version retention: the v1 provider is still cached alongside v2, so
        // an in-flight query holding a v1 handle hits instead of replaying the
        // snapshot. `entries` counts providers, so it now reports 2.
        {
            let ring = db.delta_provider_cache.get(&(project_id.clone(), t.to_string())).expect("ring for the queried key");
            assert_eq!(ring.len(), 2, "both v{v1} and v{v2} providers must be retained");
            let ttl = db.config.cache.provider_cache_ttl();
            let old = ring.get(v1 as u64, ttl).expect("previous version still retrievable — no rebuild for in-flight queries");
            assert!(old.initialized(), "the retained v{v1} cell must still hold its built provider");
            assert!(ring.get(v2 as u64, ttl).is_some(), "latest version cached");
            assert!(ring.get(v2 as u64 + 99, ttl).is_none(), "lookup is exact-version: an unseen version must miss");
        }
        let stats2 = ctx.sql("SELECT value FROM timefusion_stats WHERE component = 'scan' AND key = 'provider_cache_entries'").await?.collect().await?;
        let entries2 = stats2[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().expect("stats value").value(0);
        assert_eq!(entries2, "2", "provider_cache_entries counts retained providers across the version ring");
        Ok(())
    }

    /// Retention semantics of the per-(project,table) version ring, without IO.
    #[test]
    fn provider_versions_retains_recent_and_expires() {
        let ttl = std::time::Duration::from_secs(300);
        let mut ring = ProviderVersions::default();
        let cells: Vec<_> = (1..=4).map(|v| ring.install(v, ttl)).collect();
        assert_eq!(ring.len(), PROVIDER_VERSION_RETENTION, "ring is bounded at the retention window");
        assert!(ring.get(1, ttl).is_none(), "the oldest version falls out once the window is full");
        for (i, v) in [4u64, 3, 2].iter().enumerate() {
            let got = ring.get(*v, ttl).expect("recent version retained");
            assert!(Arc::ptr_eq(&got, &cells[3 - i]), "the SAME cell Arc comes back — no rebuild");
        }
        // Re-installing an existing version replaces it in place (no duplicate).
        let fresh = ring.install(4, ttl);
        assert_eq!(ring.len(), PROVIDER_VERSION_RETENTION);
        assert!(Arc::ptr_eq(&ring.get(4, ttl).unwrap(), &fresh));
        // Zero TTL expires everything on lookup and on prune.
        let zero = std::time::Duration::ZERO;
        assert!(ring.get(4, zero).is_none(), "expired versions are not served");
        assert_eq!(ring.prune(zero), PROVIDER_VERSION_RETENTION);
        assert_eq!(ring.len(), 0);
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_insert_and_query() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("project_{}", prefix);

            // Test basic insert
            let batch = json_to_batch(vec![test_span("test1", "span1", &project_id)])?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;

            // Verify count
            let result = ctx.sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id)).await?.collect().await?;
            use datafusion::arrow::array::AsArray;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            assert_eq!(count, 1);

            // Test field selection
            let result = ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}'", project_id)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "test1");
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "span1");

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    /// C3 end-to-end through the real stack (bootstrap → WAL → MemBuffer →
    /// coalescing flush → Delta → SQL). With the flag on, two projects' rows
    /// flushed in the same tick land in ONE Delta commit and both are queryable
    /// immediately afterwards — same tick, same visibility as before coalescing.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn coalesced_flush_e2e_keeps_every_project_queryable() -> Result<()> {
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let mut cfg = (*create_test_config(&prefix)).clone();
        cfg.buffer.timefusion_flush_coalesce_commits = true;
        let cfg = Arc::new(cfg);
        tokio::time::timeout(std::time::Duration::from_secs(50), async {
            let b = crate::bootstrap::bootstrap(Arc::clone(&cfg)).await?;
            let t = "otel_logs_and_spans";
            let projects: Vec<String> = (0..3).map(|i| format!("e2e{i}_{prefix}")).collect();

            // Create the Delta table first so the version delta measures only
            // the coalesced flush commit.
            b.db.insert_records_batch(&projects[0], t, vec![json_to_batch(vec![test_span("warm", "warm", &projects[0])])?], true, None).await?;
            let table_ref = get_unified_delta_table(b.db.unified_tables(), t).await.expect("table created");
            let before = table_ref.read().await.version().unwrap_or(0);

            // skip_queue=false → WAL + MemBuffer, so the flush tick owns these rows.
            for (i, project) in projects.iter().enumerate() {
                let batch = json_to_batch(vec![test_span(&format!("row{i}"), "span", project)])?;
                b.db.insert_records_batch(project, t, vec![batch], false, None).await?;
            }
            let stats = b.buffered_layer.flush_all_now().await?;
            assert_eq!(stats.buckets_failed, 0, "coalesced e2e flush failed");
            assert_eq!(stats.buckets_flushed, projects.len() as u64);

            let after = get_unified_delta_table(b.db.unified_tables(), t).await.expect("table").read().await.version().unwrap_or(0);
            assert_eq!(after, before + 1, "three projects flushed in one tick must produce ONE Delta commit");

            use datafusion::arrow::array::AsArray;
            for project in &projects {
                let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project}'");
                let r = b.session_ctx.sql(&sql).await?.collect().await?;
                let n = r[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
                let expected = if project == &projects[0] { 2 } else { 1 }; // p0 also has its warm-up row
                assert_eq!(n, expected, "{project} rows are not queryable after the coalesced commit");
            }

            b.shutdown.cancel();
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 50 seconds"))?
    }

    /// Regression for the pressure_flush e2e undercount (8-of-150): when
    /// `force_flush_current_buckets` commits the open bucket's rows to Delta and
    /// inserts then repopulate the same bucket_id, the query path must still
    /// return the force-flushed rows. The old per-bucket exclusion masked the
    /// current bucket's whole range from the Delta scan, hiding everything that
    /// had been force-flushed. Drives the force-flush directly so it's
    /// deterministic (no need to actually exhaust the memory budget).
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn force_flushed_current_bucket_rows_stay_queryable() -> Result<()> {
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&prefix);
        tokio::time::timeout(std::time::Duration::from_secs(50), async {
            // Need the real buffered layer (force_flush path), so bootstrap the
            // full stack rather than the layer-less setup_test_database().
            let b = crate::bootstrap::bootstrap(Arc::clone(&cfg)).await?;
            let project_id = format!("ffq_{}", prefix);

            // 3 rows into the current (open) bucket, then force-flush them to
            // Delta — leaving the bucket drained but its range still "current".
            // skip_queue=false so the write flows through the buffered layer
            // (WAL → MemBuffer), not straight to Delta.
            for i in 0..3 {
                let batch = json_to_batch(vec![test_span(&format!("flushed_{i}"), "span", &project_id)])?;
                b.db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;
            }
            b.buffered_layer.force_flush_current_buckets().await?;

            // 2 more rows repopulate the same current bucket_id in MemBuffer.
            for i in 0..2 {
                let batch = json_to_batch(vec![test_span(&format!("buffered_{i}"), "span", &project_id)])?;
                b.db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;
            }

            // All 5 must be visible: 3 from Delta (force-flushed), 2 from MemBuffer.
            // Pre-fix this returned 2 (the current range was excluded from Delta).
            use datafusion::arrow::array::AsArray;
            let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
            let r = b.session_ctx.sql(&sql).await?.collect().await?;
            let n = r[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            assert_eq!(n, 5, "force-flushed rows must remain queryable alongside repopulated MemBuffer rows");

            b.shutdown.cancel();
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 50 seconds"))?
    }

    /// Regression for the 2026-06-11 prod visibility gap: rows force-flushed
    /// to Delta from an open bucket became invisible once that bucket
    /// *sealed* — the per-bucket exclusion masked the whole window from the
    /// Delta scan while the flush backlog kept the bucket in MemBuffer for
    /// hours. Force-flushed buckets must stay exempt from the exclusion for
    /// their whole lifetime, not just while current.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn force_flushed_bucket_rows_stay_queryable_after_seal() -> Result<()> {
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&prefix);
        let res = tokio::time::timeout(std::time::Duration::from_secs(50), async {
            let b = crate::bootstrap::bootstrap(Arc::clone(&cfg)).await?;
            let project_id = format!("ffs_{}", prefix);
            // Freeze the clock mid-window so all inserts land in one
            // deterministic bucket we can later seal by advancing time.
            let dur = crate::mem_buffer::bucket_duration_micros();
            let t0 = crate::clock::set_micros((crate::clock::now_micros() / dur) * dur + dur / 2);

            for i in 0..3 {
                let batch = json_to_batch(vec![test_span_ts(&format!("flushed_{i}"), "span", &project_id, t0)])?;
                b.db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;
            }
            b.buffered_layer.force_flush_current_buckets().await?;
            for i in 0..2 {
                let batch = json_to_batch(vec![test_span_ts(&format!("buffered_{i}"), "span", &project_id, t0 + 1_000_000)])?;
                b.db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;
            }
            // Roll past the bucket boundary: the bucket is now sealed but
            // unflushed (the periodic flush hasn't run) — exactly the
            // backed-up state from the incident.
            crate::clock::advance_micros(dur);

            use datafusion::arrow::array::AsArray;
            let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
            let r = b.session_ctx.sql(&sql).await?.collect().await?;
            let n = r[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            anyhow::ensure!(n == 5, "force-flushed rows must stay visible after their bucket seals; got {n} of 5");
            b.shutdown.cancel();
            Ok(())
        })
        .await;
        crate::clock::unfreeze();
        res.map_err(|_| anyhow::anyhow!("Test timed out after 50 seconds"))?
    }

    /// Regression for the skip-Delta fast path half of the 2026-06-11 gap:
    /// a late-arriving row can pull MemBuffer's oldest timestamp to/below
    /// the query's lower bound while newer rows live only in Delta
    /// (force-flush, or a newer bucket drained while an older one is stuck).
    /// The old `query_min >= mem_oldest` heuristic then skipped the Delta
    /// scan and hid those rows; the flushed-watermark rule must not.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn delta_skip_must_not_hide_force_flushed_rows_from_bounded_query() -> Result<()> {
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&prefix);
        let res = tokio::time::timeout(std::time::Duration::from_secs(50), async {
            let b = crate::bootstrap::bootstrap(Arc::clone(&cfg)).await?;
            let project_id = format!("ffw_{}", prefix);
            let dur = crate::mem_buffer::bucket_duration_micros();
            let t0 = crate::clock::set_micros((crate::clock::now_micros() / dur) * dur + dur / 2);

            // Newer row first → force-flushed, lives only in Delta.
            let batch = json_to_batch(vec![test_span_ts("newer", "span", &project_id, t0 + 2_000_000)])?;
            b.db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;
            b.buffered_layer.force_flush_current_buckets().await?;
            // Late arrival with an older timestamp lands in MemBuffer.
            let batch = json_to_batch(vec![test_span_ts("older", "span", &project_id, t0 + 1_000_000)])?;
            b.db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], false, None).await?;

            use datafusion::arrow::array::AsArray;
            let bound = chrono::DateTime::from_timestamp_micros(t0 + 1_000_000).unwrap().to_rfc3339();
            let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp >= TIMESTAMP '{}'", project_id, bound);
            let r = b.session_ctx.sql(&sql).await?.collect().await?;
            let n = r[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            anyhow::ensure!(n == 2, "Delta-only rows inside the bound must not be skipped; got {n} of 2");
            b.shutdown.cancel();
            Ok(())
        })
        .await;
        crate::clock::unfreeze();
        res.map_err(|_| anyhow::anyhow!("Test timed out after 50 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    // Regression for the row-0 routing bug against BYO-bucket (custom storage)
    // tenants — the one case where it actually corrupts. A single mixed-project
    // batch (what a multi-row pgwire INSERT produces) goes through the real
    // fast_insert_batch path. pb has an isolated custom bucket; pa uses the
    // default unified table. The old code routed the whole batch to row 0's
    // project (pa) → all rows landed in the unified table, so pb's row never
    // reached pb's bucket: silent data loss for pb AND a cross-tenant leak of
    // pb's row into the shared unified store. (For all-unified projects Delta's
    // project_id partitioning masks the bug, which is why it needs custom storage
    // to reproduce.)
    async fn test_fast_insert_mixed_custom_storage_routing() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            use datafusion::arrow::array::AsArray;
            let (db, ctx, prefix) = setup_test_database().await?;
            let (pa, pb, table) = (format!("csA_{prefix}"), format!("csB_{prefix}"), "otel_logs_and_spans".to_string());

            // pb is a BYO-bucket tenant: same MinIO, distinct prefix → its own Delta table.
            // config_pool is None under setup_test_database, so this injected config is
            // authoritative (no TTL reload overwrites it).
            db.storage_configs.write().await.insert(
                (pb.clone(), table.clone()),
                StorageConfig {
                    project_id: pb.clone(),
                    table_name: table.clone(),
                    s3_bucket: "timefusion-tests".to_string(),
                    s3_prefix: format!("custom-{prefix}"),
                    s3_region: "us-east-1".to_string(),
                    s3_access_key_id: "minioadmin".to_string(),
                    s3_secret_access_key: "minioadmin".to_string(),
                    s3_endpoint: Some("http://127.0.0.1:9000".to_string()),
                },
            );

            // One batch, interleaved A/B/A so row 0 (pa) is not the only project.
            let batch = json_to_batch(vec![test_span("a1", "n", &pa), test_span("b1", "n", &pb), test_span("a2", "n", &pa)])?;
            let provider = ctx.table_provider(table.as_str()).await?;
            // Upcast to &dyn Any (TableProvider: Any) — `use super::*` pulls arrow's
            // Array::as_any into scope, which would otherwise shadow the right method.
            let any: &dyn std::any::Any = provider.as_ref();
            let rt = any.downcast_ref::<ProjectRoutingTable>().ok_or_else(|| anyhow::anyhow!("otel_logs_and_spans is not a ProjectRoutingTable"))?;
            assert_eq!(rt.fast_insert_batch(batch).await?, 3);

            let count = |p: String| {
                let ctx = ctx.clone();
                async move {
                    let sql = format!("SELECT COUNT(*) c FROM otel_logs_and_spans WHERE project_id = '{p}'");
                    Result::<i64>::Ok(ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0))
                }
            };
            assert_eq!(count(pb.clone()).await?, 1, "pb's row must reach pb's BYO bucket, not leak into pa's unified table");
            assert_eq!(count(pa.clone()).await?, 2, "pa keeps exactly its 2 rows");

            db.shutdown().await?;
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 60 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_projects() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let projects: Vec<String> = (1..=3).map(|i| format!("proj{}_{}", i, prefix)).collect();

            // Insert data for multiple projects
            for project in &projects {
                let batch = json_to_batch(vec![test_span(&format!("id_{}", project), &format!("span_{}", project), project)])?;
                db.insert_records_batch(project, "otel_logs_and_spans", vec![batch], true, None).await?;
            }

            // Verify project isolation
            use datafusion::arrow::array::AsArray;
            for project in &projects {
                let sql = format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                assert_eq!(result[0].num_rows(), 1);
                assert_eq!(get_str(result[0].column(0).as_ref(), 0), format!("id_{}", project));
            }

            // Verify total count - need to check across all projects
            let mut total_count = 0;
            for project in &projects {
                let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
                total_count += count;
            }
            assert_eq!(total_count, 3);

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_filtering() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("filter_proj_{}", prefix);
            use chrono::Utc;
            use serde_json::json;

            let now = Utc::now();
            let records = vec![
                json!({
                    "timestamp": now.timestamp_micros(),
                    "id": "span1",
                    "name": "test_span_1",
                    "project_id": &project_id,
                    "level": "INFO",
                    "status_code": "OK",
                    "duration": 100_000_000,
                    "date": now.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Test span 1 - INFO level"]
                }),
                json!({
                    "timestamp": (now + chrono::Duration::minutes(10)).timestamp_micros(),
                    "id": "span2",
                    "name": "test_span_2",
                    "project_id": &project_id,
                    "level": "ERROR",
                    "status_code": "ERROR",
                    "status_message": "Error occurred",
                    "duration": 200_000_000,
                    "date": now.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Test span 2 - ERROR level"]
                }),
            ];

            let batch = json_to_batch(records)?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;

            // Test filtering by level
            let result =
                ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'", project_id)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "span2");

            // Test filtering by duration
            let result =
                ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND duration > 150000000", project_id)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "span2");

            // Test compound filtering
            let result = ctx
                .sql(&format!("SELECT id, status_message FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'", project_id))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "Error occurred");

            // Shutdown database to ensure proper cleanup
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_sql_insert() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let proj1 = format!("default_{}", prefix);
            let proj2 = format!("proj2_{}", prefix);
            use datafusion::arrow::array::AsArray;

            // Insert via API first
            let batch = json_to_batch(vec![test_span("id1", "name1", &proj1)])?;
            db.insert_records_batch(&proj1, "otel_logs_and_spans", vec![batch], true, None).await?;

            // Insert via SQL
            let sql = format!(
                "INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES (
                       '{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z',
                       'sql_id', ARRAY[], 'sql_name', 'INFO', 'OK', ARRAY['SQL inserted test span']
                     )",
                proj2
            );
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);

            // Verify both records exist - need to check both projects
            let mut total_count = 0;
            for project in [&proj1, &proj2] {
                let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
                total_count += count;
            }
            assert_eq!(total_count, 2);

            // Verify SQL-inserted record
            let result =
                ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'sql_id'", proj2)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "sql_name");

            db.shutdown().await?;
            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_multi_row_sql_insert() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("multirow_{}", prefix);
            use datafusion::arrow::array::AsArray;

            // Test multi-row INSERT
            let sql = format!("INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES
                     ('{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 'id1', ARRAY[], 'name1', 'INFO', 'OK', ARRAY['Multi-row insert test 1']),
                     ('{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T11:00:00Z', 'id2', ARRAY[], 'name2', 'INFO', 'OK', ARRAY['Multi-row insert test 2']),
                     ('{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T12:00:00Z', 'id3', ARRAY[], 'name3', 'ERROR', 'ERROR', ARRAY['Multi-row insert test 3 - ERROR'])",
                     project_id, project_id, project_id);

            // Multi-row INSERT returns a count of rows inserted
            let result = ctx.sql(&sql).await?.collect().await?;
            let inserted_count = result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0);
            assert_eq!(inserted_count, 3);

            // Verify all 3 records exist
            let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
            let result = ctx.sql(&sql).await?.collect().await?;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            assert_eq!(count, 3);

            // Verify individual records
            let result = ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}' ORDER BY id", project_id)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 3);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "id1");
            assert_eq!(get_str(result[0].column(0).as_ref(), 1), "id2");
            assert_eq!(get_str(result[0].column(0).as_ref(), 2), "id3");

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_timestamp_operations() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let project_id = format!("ts_test_{}", prefix);
            use chrono::Utc;
            use serde_json::json;

            let base_time = chrono::DateTime::parse_from_rfc3339("2023-01-01T10:00:00Z").unwrap().with_timezone(&Utc);
            let records = vec![
                json!({
                    "timestamp": base_time.timestamp_micros(),
                    "id": "early",
                    "name": "early_span",
                    "project_id": &project_id,
                    "date": base_time.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Early span for timestamp test"]
                }),
                json!({
                    "timestamp": (base_time + chrono::Duration::hours(2)).timestamp_micros(),
                    "id": "late",
                    "name": "late_span",
                    "project_id": &project_id,
                    "date": base_time.date_naive().to_string(),
                    "hashes": [],
                    "summary": ["Late span for timestamp test"]
                }),
            ];

            let batch = json_to_batch(records)?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;

            // First check if any records were inserted - need to specify project_id
            let all_records = ctx.sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id)).await?.collect().await?;
            assert!(!all_records.is_empty(), "No records found in table");

            // Test timestamp filtering - need to include project_id
            let result = ctx
                .sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp > '2023-01-01T11:00:00Z'", project_id))
                .await?
                .collect()
                .await?;
            assert!(!result.is_empty(), "Query returned no results");
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "late");

            // Test timestamp formatting - need to include project_id
            let result = ctx
                .sql(&format!(
                    "SELECT id, to_char(timestamp, 'YYYY-MM-DD HH24:MI') as ts FROM otel_logs_and_spans WHERE project_id = '{}' ORDER BY timestamp",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 2);
            assert_eq!(get_str(result[0].column(1).as_ref(), 0), "2023-01-01 10:00");
            assert_eq!(get_str(result[0].column(1).as_ref(), 1), "2023-01-01 12:00");

            // Shutdown database to ensure proper cleanup
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    // The three #[ignore]'d tests below stress real Delta-table concurrency against
    // S3 (MinIO). They run cleanly in isolated environments (`make test-all`) but
    // wedge in the shared GHA test process because `config::init_config()` uses a
    // OnceLock — so every test inherits the *first* test's TIMEFUSION_TABLE_PREFIX.
    // By the time a "concurrent" test runs, the table has accumulated versions
    // from earlier tests and 3-way commit contention retries past any
    // reasonable timeout. Run with `cargo test -- --ignored` locally.
    #[serial]
    #[ignore = "wedges under shared-state CI; see comment above. Run with cargo test -- --ignored"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_writes_same_project() -> Result<()> {
        // Locally <3s; CI's MinIO + fresh Delta-table create-on-write under 3-way
        // concurrent contention regularly exceeds 60s on the GHA runner. Headroom.
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Database::new().await?;
            let db = Arc::new(db);
            let project_id = format!("concurrent_test_{}", uuid::Uuid::new_v4());

            // Create 3 concurrent write tasks (reduced from 10 to minimize Delta conflicts)
            let tasks = (0..3).map(|i| {
                let db = Arc::clone(&db);
                let project = project_id.clone();

                tokio::spawn(async move {
                    let batch_id = format!("batch_{}", i);
                    let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project)])?;
                    db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], true, None).await.map(|_| batch_id)
                })
            });

            let results: Vec<Result<String, _>> =
                futures::future::join_all(tasks).await.into_iter().map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?).collect();

            let successful_writes: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;
            assert_eq!(successful_writes.len(), 3, "All 3 concurrent writes should succeed");

            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
    }

    #[serial]
    #[ignore = "wedges under shared-state CI; see test_concurrent_writes_same_project comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_table_creation() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Database::new().await?;
            let db = Arc::new(db);

            // Create multiple projects concurrently - each will try to create its own table
            let tasks = (0..5).map(|i| {
                let db = Arc::clone(&db);
                let project_id = format!("project_create_test_{}", i);

                tokio::spawn(async move {
                    let batch_id = format!("init_batch_{}", i);
                    let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project_id)])?;
                    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await.map(|_| project_id)
                })
            });

            // Wait for all tasks to complete
            let results: Vec<Result<String, _>> =
                futures::future::join_all(tasks).await.into_iter().map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?).collect();

            let created_projects: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;
            assert_eq!(created_projects.len(), 5, "All 5 projects should be created successfully");

            // Shutdown database
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
    }

    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_queue_under_load() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            use crate::batch_queue::BatchQueue;

            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Arc::new(Database::new().await?);
            let queue = BatchQueue::new(Arc::clone(&db), 100, 50); // 100ms interval, 50 rows max

            let project_id = format!("queue_test_{}", uuid::Uuid::new_v4());

            // Queue many batches rapidly
            for i in 0..100 {
                let batch_id = format!("queued_batch_{}", i);
                let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{}", batch_id), &project_id)])?;

                match queue.queue(batch) {
                    Ok(_) => {}
                    Err(e) if e.to_string().contains("Queue full") => break,
                    Err(e) => return Err(e),
                }
            }

            // Give queue time to process
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

            queue.shutdown().await;
            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 30 seconds"))?
    }

    #[serial]
    #[ignore = "wedges under shared-state CI; see test_concurrent_writes_same_project comment"]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_mixed_operations() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            dotenv::dotenv().ok();
            unsafe {
                std::env::set_var("AWS_S3_BUCKET", "timefusion-tests");
                std::env::set_var("TIMEFUSION_TABLE_PREFIX", format!("test-{}", uuid::Uuid::new_v4()));
            }

            let db = Database::new().await?;
            let db = Arc::new(db);

            // Test concurrent writes to DIFFERENT projects (no conflicts)
            let mut handles = Vec::new();
            for i in 0..3 {
                let db_clone = Arc::clone(&db);
                let project_id = format!("project_{}", i);
                handles.push(tokio::spawn(async move {
                    let batch = json_to_batch(vec![test_span(&format!("id_{}", i), &format!("span_{}", i), &project_id)])?;
                    db_clone.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
                    Ok::<_, anyhow::Error>(())
                }));
            }

            // Wait for all writes
            for handle in handles {
                handle.await??;
            }

            // Now test concurrent reads across all projects
            let mut read_handles = Vec::new();
            for i in 0..3 {
                let db_clone = Arc::clone(&db);
                let project_id = format!("project_{}", i);
                read_handles.push(tokio::spawn(async move {
                    let ctx = db_clone.clone().create_session_context();
                    let _ = ctx.sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id)).await;
                    Ok::<_, anyhow::Error>(())
                }));
            }

            for handle in read_handles {
                handle.await??;
            }

            db.shutdown().await?;

            Ok(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
    }

    #[serial]
    #[tokio::test]
    async fn dirty_dedup_bins_survive_restart() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-restart-{}", uuid::Uuid::new_v4().simple()));
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let old = (Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
        let db = Database::with_config(Arc::clone(&cfg)).await?;
        let batch = json_to_batch(vec![test_span_ts("restart", "first", &project, old)])?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1);
        drop(db);

        let restored = Database::with_config(cfg).await?;
        assert_eq!(restored.dedup_dirty_bins.len(), 1, "restart restores the sealed late-event bin");
        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn dirty_dedup_bins_enqueue_seal_and_requeue() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-bins-{}", uuid::Uuid::new_v4().simple()));
        assert!(!cfg.maintenance.timefusion_dedup_sweep_fallback, "the broad fallback sweep must default off");
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        // Well beyond the seal lag. Today's sealed bins now follow this same
        // targeted path; 26h keeps this test deterministic around midnight.
        let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();
        let row = |id: &str, observed: &str, timestamp| json_to_batch(vec![test_span_ts(id, observed, &project, timestamp)]);

        // The first duplicate shares a parquet file with a row from an adjacent
        // 10-minute bin. A targeted rewrite must carry that neighbour through;
        // using the dirty-bin predicate as the full-file re-read predicate
        // dropped it in production.
        let neighbour = old - 60 * 60 * 1_000_000;
        let first_file = json_to_batch(vec![test_span_ts("sealed", "first", &project, old), test_span_ts("neighbour", "keep", &project, neighbour)])?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![first_file], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("sealed", "second", old)?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 2, "successful commits enqueue both timestamp bins");
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();
        let selected = {
            let table = table.read().await;
            let snapshot = table.snapshot()?.snapshot();
            let old_date = chrono::DateTime::<Utc>::from_timestamp_micros(old).unwrap().date_naive().to_string();
            dedup_partition_paths(snapshot.log_data().iter().map(|f| f.path().to_string()), &project, &old_date)
        };
        assert_eq!(selected.len(), 2, "snapshot selection must retain both duplicate-bearing files: {selected:?}");
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 2, "sealed bin is deduplicated without dropping an adjacent-bin row from the same file");
        assert!(db.dedup_dirty_bins.is_empty(), "completed sealed bin is consumed");

        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("sealed", "later", old)?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1, "late retry requeues the previously consumed bin");
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 2, "later observed timestamp survives the requeue rewrite and the neighbour remains");

        let fresh = Utc::now().timestamp_micros();
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("unsealed", "a", fresh)?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("unsealed", "b", fresh)?], true, None).await?;
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1, "unsealed bin remains queued without rewrite");
        assert_eq!(delta_physical_row_count(&table).await?, 4, "unsealed copies remain for read-side dedup");
        Ok(())
    }

    /// One transient flush-unhealthy sample mid-pass must not discard the
    /// batch's staged work. The old latch requeued every remaining bin after a
    /// single bad sample — at boot that silently forfeited an entire 128-bin
    /// pass (~75 min of staging, prod 2026-08-05). A wave whose commit finds
    /// flush unhealthy waits for recovery instead.
    #[tokio::test]
    async fn dirty_dedup_drain_survives_transient_flush_unhealthy() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-transient-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();
        let row = |id: &str, observed: &str| json_to_batch(vec![test_span_ts(id, observed, &project, old)]);
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("sealed", "first")?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("sealed", "second")?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();

        // Healthy at pass start, unhealthy for exactly one mid-pass sample.
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let flaky = || calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) != 1;
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &flaky, std::time::Duration::MAX).await?;
        assert!(calls.load(std::sync::atomic::Ordering::Relaxed) >= 2, "the unhealthy sample must have been consumed");
        assert_eq!(delta_physical_row_count(&table).await?, 1, "a transient unhealthy flush sample must not forfeit the staged dedup work");
        assert!(db.dedup_dirty_bins.is_empty(), "bin is consumed, not requeued");
        Ok(())
    }

    /// One whole-date probe classifies every queued bin of a (project, date):
    /// probe-clean bins are consumed WITHOUT per-bin staging scans (every
    /// flushed bin is enqueued, so in prod ~97% of queued bins carry no
    /// duplicates), and dup-bearing bins still dedup through the per-bin path.
    #[tokio::test]
    async fn dirty_dedup_batch_probe_consumes_clean_bins() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-batchprobe-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        // Noon of the day 26h ago: ±20 min can never cross a date boundary,
        // and the bins are sealed far beyond the 2h lag.
        let day = (Utc::now() - chrono::Duration::hours(26)).date_naive();
        let base = day.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
        const TEN_MIN: i64 = 10 * 60 * 1_000_000;
        let row = |id: &str, observed: &str, ts: i64| json_to_batch(vec![test_span_ts(id, observed, &project, ts)]);
        // Three bins on one date: one duplicate pair, two clean singles.
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("dup", "first", base)?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("dup", "second", base)?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("clean1", "only", base - TEN_MIN)?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("clean2", "only", base - 2 * TEN_MIN)?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 3);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();

        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(crate::metrics::maintenance_stats().dirty_bin_batch_probe_clean.load(Relaxed), 2, "both clean bins are consumed by the batch probe alone");
        assert!(db.dedup_dirty_bins.is_empty(), "clean and dup bins are all consumed");
        assert_eq!(delta_physical_row_count(&table).await?, 3, "the duplicate collapsed; clean rows untouched");
        Ok(())
    }

    /// A hung staging read must not wedge the drain: the per-bin deadline
    /// converts the hang into an ordinary requeue and the pass moves on
    /// (prod 2026-08-05: one unbounded read held the 1-permit maintenance
    /// semaphore for 6.5h while the cron logged skips=77).
    #[tokio::test]
    async fn dirty_dedup_bin_staging_deadline_requeues_instead_of_wedging() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-deadline-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();
        let row = |observed: &str| json_to_batch(vec![test_span_ts("sealed", observed, &project, old)]);
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("second")?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();

        // A 1ms deadline fires before any real staging read can complete —
        // standing in for the hung GET.
        use std::sync::atomic::Ordering::Relaxed;
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::from_millis(1)).await?;
        assert!(crate::metrics::maintenance_stats().dedup_bin_stage_timeouts.load(Relaxed) >= 1, "the per-bin deadline must fire");
        assert_eq!(db.dedup_dirty_bins.len(), 1, "the timed-out bin is requeued, not lost");
        assert_eq!(delta_physical_row_count(&table).await?, 2, "no partial rewrite landed");

        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 1, "the requeued bin dedups under a sane deadline");
        assert!(db.dedup_dirty_bins.is_empty());
        Ok(())
    }

    /// Hot bins drain newest-first, and cold-owned dates sink below every hot
    /// bin instead of monopolising the batch (boot 2026-07-30 drained a 10-day
    /// backlog oldest-first and never reached the hot window).
    #[test]
    fn drain_bins_order_newest_first_and_sink_cold() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 7, 30).unwrap();
        let bin = |date: &str, bin: i64| ("p".to_string(), date.to_string(), bin);
        let candidates = vec![bin("2026-07-20", 5), bin("2026-07-29", 1), bin("2026-07-29", 7), bin("2026-07-21", 3), bin("2026-07-28", 2)];

        // after_days=3 ⇒ 07-20/07-21 are cold-owned, 07-28/07-29 are hot.
        let (ready, deferred) = Database::select_drain_bins(candidates.clone(), today, 3, 10);
        assert_eq!(
            ready,
            vec![bin("2026-07-29", 7), bin("2026-07-29", 1), bin("2026-07-28", 2), bin("2026-07-21", 3), bin("2026-07-20", 5)],
            "newest-first within each tier, cold tier last"
        );
        assert!(deferred.is_empty(), "a batch with room serves the cold tail too");

        // A batch smaller than the hot tier still RESERVES half for cold, so the
        // cold backlog drains monotonically instead of being deferred forever
        // behind continuous hot work (prod 2026-08-02: 20556 deferred, 0
        // processed, queue 22135). Hot keeps the priority — it takes the larger
        // share and the newest bins — but it no longer takes everything.
        let (ready, deferred) = Database::select_drain_bins(candidates.clone(), today, 3, 2);
        assert_eq!(ready, vec![bin("2026-07-29", 7), bin("2026-07-21", 3)], "hot keeps priority, cold gets its reserved slot");
        assert_eq!(deferred, vec![bin("2026-07-20", 5)], "cold bins are deferred, not dropped");
        assert_eq!(deferred.last().unwrap().1, "2026-07-20", "summary line reports the oldest deferred date");

        // With no cold work at all, hot still gets the WHOLE batch — the reserve
        // must never idle a slot.
        let hot_only: Vec<_> = candidates.iter().filter(|(_, d, _)| d.as_str() >= "2026-07-28").cloned().collect();
        let (ready, deferred) = Database::select_drain_bins(hot_only, today, 3, 2);
        assert_eq!(ready, vec![bin("2026-07-29", 7), bin("2026-07-29", 1)], "no cold work ⇒ hot uses the full batch");
        assert!(deferred.is_empty());
    }

    #[test]
    fn dedup_shards_bound_oversized_rewrites() {
        assert_eq!(dedup_shard_count(100, 100, 100, 100), 1);
        assert_eq!(dedup_shard_count(101, 100, 100, 100), 2);
        assert_eq!(dedup_shard_count(u64::MAX, 1, 1, 0), DEDUP_BUCKET_COUNT);
    }

    #[test]
    fn dedup_rewrite_rejects_the_production_partial_file_loss_shape() {
        assert!(dedup_rewrite_counts_match(5_795_641, 5_795_641, 2_100_000, 2_100_000));
        assert!(
            !dedup_rewrite_counts_match(63_786, 5_795_641, 63_786, 2_100_000),
            "a bin-scoped re-read must never remove files whose adjacent rows were omitted"
        );
        assert!(!dedup_rewrite_counts_match(5_795_641, 5_795_641, 2_100_001, 2_100_000), "winner-count drift must also fail closed");
    }

    /// The cold cutoff must never DROP bins: the nightly consolidate bin-packs
    /// those partitions but does not collapse duplicates, so this drain is
    /// their only physical dedup.
    #[serial]
    #[tokio::test]
    async fn dirty_dedup_cold_bins_are_deferred_not_dropped() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-cold-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let after_days = db.config.parquet.cold_optimize_after_days();
        let ancient = (Utc::now() - chrono::Duration::days(after_days as i64 + 2)).timestamp_micros();
        let row = |observed: &str| json_to_batch(vec![test_span_ts("cold", observed, &project, ancient)]);

        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("second")?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1);

        let deferred_before = crate::metrics::maintenance_stats().dedup_bins_deferred_cold.load(std::sync::atomic::Ordering::Relaxed);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();
        // Batch of 1 with a cold-only queue still serves it (lowest priority ≠
        // never), so the drain deduplicates rather than abandoning the rows.
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        assert_eq!(
            crate::metrics::maintenance_stats().dedup_bins_deferred_cold.load(std::sync::atomic::Ordering::Relaxed),
            deferred_before,
            "nothing to defer when the batch has room"
        );
        assert_eq!(delta_physical_row_count(&table).await?, 1, "cold duplicates are physically collapsed, never silently abandoned");
        assert!(db.dedup_dirty_bins.is_empty());
        Ok(())
    }

    /// Dedup yields to persistence: an unhealthy flush path skips the pass
    /// whole, leaving the queue intact for a later tick.
    #[serial]
    #[tokio::test]
    async fn dirty_dedup_drain_yields_to_unhealthy_flush() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-gate-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();
        let row = |observed: &str| json_to_batch(vec![test_span_ts("gated", observed, &project, old)]);

        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("second")?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1);

        let yields_before = crate::metrics::maintenance_stats().dedup_passes_flush_yields.load(std::sync::atomic::Ordering::Relaxed);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| false, std::time::Duration::MAX).await?;
        assert_eq!(
            crate::metrics::maintenance_stats().dedup_passes_flush_yields.load(std::sync::atomic::Ordering::Relaxed),
            yields_before + 1,
            "the skipped pass is counted, not silent"
        );
        assert_eq!(db.dedup_dirty_bins.len(), 1, "the bin stays queued for a healthier tick");
        assert_eq!(delta_physical_row_count(&table).await?, 2, "no rewrite happened; read-side dedup keeps results correct");

        // Healthy again ⇒ the same bin drains normally.
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 1);
        Ok(())
    }
}
