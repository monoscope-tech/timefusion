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
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, projection::ProjectionExec, union::UnionExec},
    scalar::ScalarValue,
};
use datafusion_datasource::{memory::MemorySourceConfig, source::DataSourceExec};
use datafusion_functions_json;
use deltalake::{
    DeltaTable, DeltaTableBuilder, PartitionFilter, datafusion::parquet::file::properties::WriterProperties, kernel::transaction::CommitProperties,
    logstore::LogStore, operations::create::CreateBuilder,
};
use futures::StreamExt;
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
type DeltaProviderCache = dashmap::DashMap<(String, String), (u64, Arc<DeltaProviderCell>)>;

/// Captured per-scan to feed `ScanMetrics::record_scan`. Cheap to copy.
#[derive(Debug, Default, Clone, Copy)]
struct ScanShape {
    skipped_delta:    bool,
    has_mem:          bool,
    has_delta:        bool,
    fast_resolve_hit: Option<bool>,
}

/// Counters surfaced via `timefusion_stats` for production debugging. Cheap to
/// update on the hot path (Relaxed atomics); read via `snapshot()`. Histogram
/// is fixed-bucket microsecond bins so percentile estimates are O(buckets) to
/// compute without sorting.
#[derive(Debug, Default)]
pub struct ScanMetrics {
    pub scans_total:              std::sync::atomic::AtomicU64,
    pub scans_skipped_delta:      std::sync::atomic::AtomicU64,
    pub scans_mem_only:           std::sync::atomic::AtomicU64,
    pub scans_delta_only:         std::sync::atomic::AtomicU64,
    pub scans_mem_plus_delta:     std::sync::atomic::AtomicU64,
    pub fast_resolve_hits:        std::sync::atomic::AtomicU64,
    pub fast_resolve_misses:      std::sync::atomic::AtomicU64,
    /// Delta TableProvider cache: hit = cached cell at the current snapshot
    /// version; miss = either no entry, or an entry at a stale version that
    /// had to be replaced. Operators tracking the cold-start vs steady-state
    /// cliff watch the hit ratio: after the first ~tens of seconds per
    /// (project, table), this should stay high; a low ratio in prod means
    /// version is churning faster than expected (e.g. very aggressive
    /// compaction) and the cache isn't paying for itself.
    pub provider_cache_hits:      std::sync::atomic::AtomicU64,
    pub provider_cache_misses:    std::sync::atomic::AtomicU64,
    /// Provider builds that started against a version that was already
    /// stale by the time the build finished — the DashMap entry got
    /// replaced under us (a flush bumped the version) and the rebuilt
    /// provider had to be dropped. Cheap-to-skip in the steady state
    /// (flush cadence is seconds apart); a non-zero rate here under
    /// sustained traffic flags either very frequent compaction or a
    /// pathological version-churn pattern worth investigating.
    pub provider_build_abandoned: std::sync::atomic::AtomicU64,
    /// Latency histogram of the full `ProjectRoutingTable::scan` call in
    /// microseconds. Buckets are powers of two so reads at any duration land
    /// in a single bucket via `usize::leading_zeros` math. Bucket i holds
    /// scans whose duration_us fits in `[1<<i, 1<<(i+1))`. 32 buckets covers
    /// 1us through ~1.2 hours.
    pub scan_latency_buckets:     [std::sync::atomic::AtomicU64; 32],
    /// End-to-end pgwire query latency histogram (same bucket scheme as
    /// `scan_latency_buckets`). Recorded by `LoggingSimpleHandler` and
    /// `LoggingExtendedQueryHandler` around the `DfSessionService::do_query`
    /// call — the FULL server-side path from "harness received our query"
    /// through "result encoded back to client". Compare to scan p95/p99 to
    /// see how much of the user-visible tail is outside the scan call.
    pub pgwire_total:             std::sync::atomic::AtomicU64,
    pub pgwire_latency_buckets:   [std::sync::atomic::AtomicU64; 32],
}

impl ScanMetrics {
    pub fn record_scan(&self, duration_us: u64, skipped_delta: bool, has_mem: bool, has_delta: bool, fast_resolve_hit: Option<bool>) {
        use std::sync::atomic::Ordering::Relaxed;
        self.scans_total.fetch_add(1, Relaxed);
        if skipped_delta {
            self.scans_skipped_delta.fetch_add(1, Relaxed);
        }
        match (has_mem, has_delta) {
            (true, false) => {
                self.scans_mem_only.fetch_add(1, Relaxed);
            }
            (false, true) => {
                self.scans_delta_only.fetch_add(1, Relaxed);
            }
            (true, true) => {
                self.scans_mem_plus_delta.fetch_add(1, Relaxed);
            }
            _ => {}
        }
        if let Some(hit) = fast_resolve_hit {
            if hit {
                self.fast_resolve_hits.fetch_add(1, Relaxed);
            } else {
                self.fast_resolve_misses.fetch_add(1, Relaxed);
            }
        }
        let bucket = if duration_us <= 1 { 0 } else { (64 - duration_us.leading_zeros() - 1).min(31) as usize };
        self.scan_latency_buckets[bucket].fetch_add(1, Relaxed);
    }

    /// Record a pgwire end-to-end query duration. Cheap on hot path —
    /// just a counter bump and one histogram bin increment.
    pub fn record_pgwire_query(&self, duration_us: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.pgwire_total.fetch_add(1, Relaxed);
        let bucket = if duration_us <= 1 { 0 } else { (64 - duration_us.leading_zeros() - 1).min(31) as usize };
        self.pgwire_latency_buckets[bucket].fetch_add(1, Relaxed);
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
        let total: u64 = buckets.iter().map(|b| b.load(Relaxed)).sum();
        if total == 0 {
            return 0;
        }
        let target = (total as f64 * p) as u64;
        let mut cum = 0u64;
        for (i, b) in buckets.iter().enumerate() {
            cum += b.load(Relaxed);
            if cum >= target {
                return 1u64 << (i + 1);
            }
        }
        1u64 << 32
    }
}

// Custom project tables: projects with their own S3 bucket get isolated tables
// Key: (project_id, table_name) -> DeltaTable
pub type CustomProjectTables = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;

// Per-table (keyed by storage URL), per-date set of live file URIs at the last
// successful z-order optimize. Backs the ZOrder idempotence guard.
type ZOrderFilesets = Arc<RwLock<HashMap<String, HashMap<chrono::NaiveDate, std::collections::HashSet<String>>>>>;

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

    batch.schema().fields().iter().position(|f| f.name() == "project_id").and_then(|idx| {
        let column = batch.column(idx);
        // Try Utf8View first (our preferred type), then fall back to Utf8
        if let Some(arr) = column.as_any().downcast_ref::<StringViewArray>() {
            (arr.len() > 0 && !arr.is_null(0)).then(|| arr.value(0).to_string())
        } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
            (arr.len() > 0 && !arr.is_null(0)).then(|| arr.value(0).to_string())
        } else {
            None
        }
    })
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

    // Group row indices by project. `get_mut`-or-`insert` so the owned key String
    // is allocated once per distinct project, not once per row.
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut push = |pid: &str, i: usize| match groups.get_mut(pid) {
        Some(v) => v.push(i as u32),
        None => drop(groups.insert(pid.to_string(), vec![i as u32])),
    };
    if let Some(arr) = column.as_any().downcast_ref::<StringViewArray>() {
        for i in 0..num_rows {
            push(if arr.is_null(i) { default_project } else { arr.value(i) }, i);
        }
    } else if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        for i in 0..num_rows {
            push(if arr.is_null(i) { default_project } else { arr.value(i) }, i);
        }
    } else {
        return Ok(vec![(default_project.to_string(), batch)]);
    }

    // Homogeneous batch: route the whole thing, skip the take/copy.
    if groups.len() == 1 {
        let pid = groups.into_keys().next().unwrap();
        return Ok(vec![(pid, batch)]);
    }

    groups.into_iter().map(|(pid, indices)| Ok((pid, take_record_batch(&batch, &UInt32Array::from(indices))?))).collect()
}

/// Convert Utf8/Utf8View/LargeUtf8 columns to Variant binary StructArrays where the target
/// schema expects Variant. Called from `DataSink::write_all` so that INSERT statements (where
/// the table provider presents Variant cols as Utf8View for the SQL planner's type check) can
/// land their JSON-string values in the underlying Delta storage which expects Variant structs.
/// Normalize incoming Timestamp columns whose timezone is a numeric UTC
/// offset (`"+00:00"` — what psycopg / pgwire emit for timestamptz) to the
/// IANA name `"UTC"`. Delta-rs's Arrow→Delta schema converter rejects
/// `Timestamp(µs, "+00:00")` even though it's semantically identical to
/// `"UTC"`; without normalization every flush errors out and MemBuffer
/// fills until eviction warnings, with no data ever reaching Delta.
///
/// We only retag — the underlying micros-since-epoch buffer is unchanged.
/// Build a minimal `SessionState` for delta-rs `OptimizeBuilder` to use.
///
/// delta-rs's default `DeltaSessionConfig` turns `schema_force_view_types`
/// ON, which makes the optimize-internal Parquet reader cast our Variant
/// columns' Binary buffers to BinaryView at read time. The kernel's
/// `unshredded_variant()` schema then mismatches and the rewrite errors
/// out ("Expected ... Binary, got ... BinaryView"). Passing this session
/// via `.with_session_state(...)` overrides the default and keeps the
/// read schema as declared.
fn build_optimize_session_state(target_partitions: usize) -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    let mut cfg = SessionConfig::new().set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    // Same CPU-quota cap as the query session (see autotune::apply); 0 = default.
    if target_partitions > 0 {
        cfg = cfg.with_target_partitions(target_partitions);
    }
    SessionStateBuilder::new().with_config(cfg).with_default_features().build()
}

/// Cast Variant struct columns (Struct{BinaryView,BinaryView}) to the
/// Binary-backed form delta-kernel's `unshredded_variant()` requires on
/// On-disk key for the WAL watermark stored in `commitInfo.info`. Constant so
/// the writer (this file) and reader (`derive_wal_cursor_for_table`) can't
/// drift, and the roundtrip test below pins the format.
const WAL_WATERMARK_KEY: &str = "timefusion.wal_watermark";

/// Serialize a per-shard watermark to the JSON map shape we store in
/// `commitInfo.info[WAL_WATERMARK_KEY]`. Only shards with a position are
/// included — absent shards mean "no constraint from this commit", which is
/// how the per-shard MAX aggregation across commits ignores them.
fn serialize_watermark_to_json(watermark: &crate::buffered_write_layer::DeltaWatermark) -> serde_json::Map<String, serde_json::Value> {
    watermark
        .iter()
        .enumerate()
        .filter_map(|(shard, pos)| pos.map(|p| (shard.to_string(), serde_json::json!({ "block_id": p.block_id, "offset": p.offset }))))
        .collect()
}

/// Inverse of `serialize_watermark_to_json`. Out-of-range or malformed shards
/// are dropped silently — schema-evolution-friendly: future writers can add
/// fields without breaking older readers.
fn parse_watermark_from_json(info: &std::collections::HashMap<String, serde_json::Value>, shards: usize) -> Vec<Option<walrus_rust::WalPosition>> {
    let mut out = vec![None; shards];
    let Some(wm) = info.get(WAL_WATERMARK_KEY).and_then(|v| v.as_object()) else {
        return out;
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
    commit_infos: impl IntoIterator<Item = &'a std::collections::HashMap<String, serde_json::Value>>, shards: usize,
) -> Vec<Option<walrus_rust::WalPosition>> {
    let mut acc = vec![None; shards];
    for info in commit_infos {
        for (shard, p) in parse_watermark_from_json(info, shards).into_iter().enumerate() {
            let Some(candidate) = p else { continue };
            acc[shard] = Some(acc[shard].map_or(candidate, |prev: walrus_rust::WalPosition| prev.max(candidate)));
        }
    }
    acc
}

/// Build [`CommitProperties`] carrying the watermark under [`WAL_WATERMARK_KEY`].
/// Empty when the watermark has no positions (e.g. WAL-replay-derived buckets);
/// delta-rs writes the commit without the key in that case, and recovery
/// silently skips that commit.
fn build_watermark_commit_properties(watermark: &crate::buffered_write_layer::DeltaWatermark) -> CommitProperties {
    let entries = serialize_watermark_to_json(watermark);
    if entries.is_empty() {
        return CommitProperties::default();
    }
    let mut meta = std::collections::HashMap::new();
    meta.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(entries));
    CommitProperties::default().with_metadata(meta)
}

/// `CommitProperties` for a compaction/dedup commit (Add + Remove): when
/// `enabled`, the post-commit hook advances the materialized snapshot
/// incrementally instead of re-materializing every active file. `false` is the
/// plain full-update behaviour.
fn incremental_commit_properties(enabled: bool) -> CommitProperties {
    CommitProperties::default().with_incremental_advance(enabled)
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
fn is_occ_conflict_err(msg: &str) -> bool {
    msg.contains("already exists")
        || msg.contains("Commit failed")
        || msg.contains("concurrent transaction")
        || msg.contains("Metadata changed")
        || msg.contains("Transaction failed")
}

/// write. No-op for any column that's not a Variant struct or already in
/// Binary form. Called from `insert_records_batch` right before the
/// Delta write so MemBuffer can keep its natural BinaryView layout
/// (matches what parquet reads produce → no per-row read-side cast).
fn cast_variant_columns_to_binary(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::{array::StructArray, compute::cast};
    use datafusion::arrow::datatypes::{DataType, Field};
    let schema = batch.schema();
    let mut new_cols = batch.columns().to_vec();
    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut changed = false;
    for (i, field) in schema.fields().iter().enumerate() {
        if !is_variant_type(field.data_type()) {
            continue;
        }
        let DataType::Struct(struct_fields) = field.data_type() else { continue };
        // Only act if any inner field is BinaryView.
        let needs = struct_fields.iter().any(|f| matches!(f.data_type(), DataType::BinaryView));
        if !needs {
            continue;
        }
        let Some(struct_arr) = batch.columns()[i].as_any().downcast_ref::<StructArray>() else {
            continue;
        };
        let casted_cols: Vec<arrow::array::ArrayRef> = struct_arr
            .columns()
            .iter()
            .zip(struct_fields.iter())
            .map(|(arr, f)| -> DFResult<arrow::array::ArrayRef> {
                if matches!(f.data_type(), DataType::BinaryView) {
                    cast(arr, &DataType::Binary).map_err(arrow_err)
                } else {
                    Ok(arr.clone())
                }
            })
            .collect::<DFResult<_>>()?;
        let casted_fields: arrow::datatypes::Fields = struct_fields
            .iter()
            .map(|f| {
                if matches!(f.data_type(), DataType::BinaryView) {
                    Arc::new(Field::new(f.name(), DataType::Binary, f.is_nullable()))
                } else {
                    f.clone()
                }
            })
            .collect::<Vec<_>>()
            .into();
        new_cols[i] = Arc::new(StructArray::new(casted_fields.clone(), casted_cols, struct_arr.nulls().cloned()));
        new_fields[i] = Arc::new(Field::new(field.name(), DataType::Struct(casted_fields), field.is_nullable()).with_metadata(field.metadata().clone()));
        changed = true;
    }
    if !changed {
        return Ok(batch);
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(new_fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, new_cols).map_err(arrow_err)
}

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
    let schema = batch.schema();
    let mut new_fields: Vec<Arc<Field>> = schema.fields().iter().cloned().collect();
    let mut new_cols = batch.columns().to_vec();
    let mut changed = false;
    for (i, field) in schema.fields().iter().enumerate() {
        if let DataType::Timestamp(unit, Some(tz)) = field.data_type()
            && is_utc_offset(tz.as_ref())
        {
            let col = &batch.columns()[i];
            // Downcasts are guarded by the outer `DataType::Timestamp(unit, ..)` match,
            // but Arrow's trait-object dispatch isn't an unsafe-level guarantee — return
            // an error rather than panic on the INSERT path if a future Arrow version
            // diverges.
            let bad = |w| DataFusionError::Execution(format!("timestamp downcast failed for field '{}' with width {w}", field.name()));
            let retagged: Arc<dyn arrow::array::Array> = match unit {
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
            new_cols[i] = retagged;
            new_fields[i] =
                Arc::new(Field::new(field.name(), DataType::Timestamp(*unit, Some("UTC".into())), field.is_nullable()).with_metadata(field.metadata().clone()));
            changed = true;
        }
    }
    if !changed {
        return Ok(batch);
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new_with_metadata(new_fields, schema.metadata().clone()));
    RecordBatch::try_new(new_schema, new_cols).map_err(arrow_err)
}

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

    let utf8_to_variant = |iter: Box<dyn Iterator<Item = Option<&str>> + '_>| -> DFResult<StructArray> {
        let items: Vec<_> = iter.collect();
        let mut builder = VariantArrayBuilder::new(items.len());
        for (idx, item) in items.into_iter().enumerate() {
            match item {
                Some(s) => builder
                    .append_json(s)
                    .map_err(|e| DataFusionError::Execution(format!("Invalid JSON at row {idx}: {e} (value: '{s}')")))?,
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
    };

    for (idx, target_field) in target_schema.fields().iter().enumerate() {
        if !is_variant_type(target_field.data_type()) || idx >= columns.len() {
            continue;
        }
        let col = &columns[idx];
        // Downcasts are guarded by the `DataType::*` match arm above. If Arrow ever
        // returns a different concrete array for the same logical type, surface as
        // a DataFusionError instead of panicking on the INSERT path.
        let name = target_field.name();
        let bad_downcast = |ty: &str| DataFusionError::Execution(format!("{ty} downcast failed for column {name}"));
        let converted: Option<ArrayRef> = match col.data_type() {
            DataType::Utf8View => Some(Arc::new(utf8_to_variant(Box::new(
                col.as_any().downcast_ref::<StringViewArray>().ok_or_else(|| bad_downcast("Utf8View"))?.iter(),
            ))?) as ArrayRef),
            DataType::Utf8 => Some(Arc::new(utf8_to_variant(Box::new(
                col.as_any().downcast_ref::<StringArray>().ok_or_else(|| bad_downcast("Utf8"))?.iter(),
            ))?) as ArrayRef),
            DataType::LargeUtf8 => Some(Arc::new(utf8_to_variant(Box::new(
                col.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| bad_downcast("LargeUtf8"))?.iter(),
            ))?) as ArrayRef),
            _ => None, // already Variant struct
        };
        if let Some(arr) = converted {
            columns[idx] = arr;
            new_fields[idx] = target_field.clone();
        }
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
    project_id:           String,
    table_name:           String,
    s3_bucket:            String,
    s3_prefix:            String,
    s3_region:            String,
    /// Skipped on serialize so credentials never leak through serde-based dumps
    /// (debug endpoints, metrics serialization, etc.). sqlx::FromRow bypasses
    /// serde so DB-row loading is unaffected. `#[debug("[redacted]")]` keeps
    /// them out of `{:?}` log lines.
    #[serde(serialize_with = "redact_str")]
    #[debug("[redacted]")]
    s3_access_key_id:     String,
    #[serde(serialize_with = "redact_str")]
    #[debug("[redacted]")]
    s3_secret_access_key: String,
    s3_endpoint:          Option<String>,
}

fn redact_str<S: serde::Serializer>(_: &str, ser: S) -> std::result::Result<S::Ok, S::Error> {
    ser.serialize_str("[redacted]")
}

#[derive(Debug, Clone)]
pub struct Database {
    config:                          Arc<AppConfig>,
    /// One RuntimeEnv (and thus one memory pool) shared by every session
    /// context, across `Database` clones. Per-context pools each granted the
    /// full `memory_limit × fraction` budget, so N contexts oversubscribed
    /// the cgroup N×; the pool only enforces a global cap if it's global.
    runtime_env:                     Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables:                  UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables:           CustomProjectTables,
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
    fast_resolve_cache:              dashmap::DashMap<(String, String), Arc<RwLock<DeltaTable>>>,
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
    delta_has_files:                 dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicBool>>,
    /// Per-(project,table) cached Delta-side `TableProvider` along with the
    /// snapshot version it was built against. Steady-state (post-flush)
    /// queries that have to UNION mem + delta were rebuilding the provider
    /// on every scan — measured as ~30 ms p95 of pure Delta-side overhead
    /// in the prior session. The provider is parameter-independent: every
    /// query for `(project, table)` at the same snapshot version uses the
    /// same provider, varying only filters/projection/limit on scan().
    /// Invalidation: compare table.version() against the cached version
    /// on lookup; mismatch → rebuild + replace.
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
    delta_provider_cache:            DeltaProviderCache,
    /// Per-process scan-path counters. Read by `timefusion_stats` so operators
    /// can see — in prod — whether the in-memory shortcut is being taken,
    /// what the resolve cache hit rate looks like, and how the latency
    /// distribution shifts under real load. Counters are cumulative since
    /// process start; deltas are useful for rate analysis.
    pub scan_metrics:                Arc<ScanMetrics>,
    batch_queue:                     Option<Arc<crate::batch_queue::BatchQueue>>,
    maintenance_shutdown:            Arc<CancellationToken>,
    /// One-shot guard for `preload_tables` — main.rs and bootstrap.rs are
    /// disjoint entry points today, but a second call must not double the
    /// boot-time S3 warm burst.
    preload_started:                 Arc<std::sync::atomic::AtomicBool>,
    config_pool:                     Option<PgPool>,
    storage_configs:                 Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    /// Monotonic deadline (nanos since process start) for when the next
    /// storage-configs refresh from the config DB is allowed. Capped at 30s
    /// so a hot SQL path doesn't hit PG on every statement.
    storage_configs_next_refresh_ns: Arc<std::sync::atomic::AtomicU64>,
    default_s3_bucket:               Option<String>,
    default_s3_prefix:               Option<String>,
    default_s3_endpoint:             Option<String>,
    object_store_cache:              Option<Arc<SharedFoyerCache>>,
    statistics_extractor:            Arc<DeltaStatisticsExtractor>,
    last_written_versions:           Arc<RwLock<HashMap<(String, String), u64>>>,
    /// Delta snapshot version at last dedup sweep, per scheduler key. Skips
    /// the sweep when the version hasn't moved (no commits → no new dupes).
    /// Same unbounded-growth caveat as `last_written_versions`.
    last_dedup_versions:             Arc<RwLock<HashMap<String, u64>>>,
    /// Serializes in-process Delta commits (flush appends vs dedup
    /// replace_where). delta-kernel's OCC checker cannot evaluate the
    /// bare-string timestamp predicate replace_where commits carry (errors
    /// "arrow_cast should have been simplified"), so a dedup commit racing
    /// any concurrent append aborts — every attempt, forever, on a busy
    /// table. With commits serialized the rebase sees no newer versions and
    /// skips the checker entirely.
    delta_commit_lock:               Arc<tokio::sync::Mutex<()>>,
    buffered_layer:                  Option<Arc<crate::buffered_write_layer::BufferedWriteLayer>>,
    tantivy_search:                  Option<Arc<crate::tantivy_index::search::TantivySearchService>>,
    tantivy_indexer:                 Option<Arc<crate::tantivy_index::service::TantivyIndexService>>,
    /// Per-table, per-date set of live file URIs as of the last successful full
    /// (z-order) optimize. delta-rs's ZOrder planner has no idempotence guard —
    /// it rewrites every file in the window on every run, even sealed days that
    /// didn't change, minting cold multipart objects that cold-start the
    /// object-store cache (which PR #39 then has to re-warm). This lets
    /// `optimize_table` skip a sealed partition whose file set is unchanged.
    /// Keyed by table storage URL (unique per physical table). In-memory only:
    /// a restart re-z-orders each partition once, which is harmless.
    zorder_filesets:                 ZOrderFilesets,
}

impl Database {
    /// Get the config for this database instance
    pub fn config(&self) -> &AppConfig {
        &self.config
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

                    debug!(
                        "Failed to update table for {}/{} (attempt {}/{}): {}, retrying...",
                        project_id, table_name, retries, MAX_RETRIES, e
                    );
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
                    error!(
                        "Skipping {}/{}: cannot decrypt s3_secret_access_key: {}",
                        config.project_id, config.table_name, e
                    );
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
        info!(
            "Loaded {} storage configs from timefusion_projects (encryption key: {})",
            map.len(),
            if key_set { "configured" } else { "NOT configured" }
        );
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
        crate::snapshot_cache::prune_stale(&Self::delta_snapshot_dir(&cfg), std::time::Duration::from_secs(7 * 24 * 3600));
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
                    warn!(
                        "Could not connect to config database, falling back to default mode (custom project routing disabled): {}",
                        e
                    );
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

        let db = Self {
            config: cfg,
            runtime_env: Arc::new(std::sync::OnceLock::new()),
            unified_tables: Arc::new(RwLock::new(HashMap::new())),
            custom_project_tables: Arc::new(RwLock::new(HashMap::new())),
            fast_resolve_cache: dashmap::DashMap::new(),
            delta_has_files: dashmap::DashMap::new(),
            delta_provider_cache: dashmap::DashMap::new(),
            scan_metrics: Arc::new(ScanMetrics::default()),
            batch_queue: None,
            maintenance_shutdown: Arc::new(CancellationToken::new()),
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
            delta_commit_lock: Arc::new(tokio::sync::Mutex::new(())),
            buffered_layer: None,
            tantivy_search: None,
            tantivy_indexer: None,
            zorder_filesets: Arc::new(RwLock::new(HashMap::new())),
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

    /// Set the buffered write layer for WAL + in-memory buffer
    pub fn with_buffered_layer(mut self, layer: Arc<crate::buffered_write_layer::BufferedWriteLayer>) -> Self {
        self.buffered_layer = Some(layer);
        self
    }

    /// Get the buffered write layer if configured
    pub fn buffered_layer(&self) -> Option<&Arc<crate::buffered_write_layer::BufferedWriteLayer>> {
        self.buffered_layer.as_ref()
    }

    /// Attach the tantivy search service used by the scan-side prefilter.
    pub fn with_tantivy_search(mut self, svc: Arc<crate::tantivy_index::search::TantivySearchService>) -> Self {
        self.tantivy_search = Some(svc);
        self
    }

    pub fn tantivy_search(&self) -> Option<&Arc<crate::tantivy_index::search::TantivySearchService>> {
        self.tantivy_search.as_ref()
    }

    /// Attach the write-side tantivy service. Used by the compaction-GC hook
    /// in `optimize_table` to clean up stale sidecar indexes after files are
    /// rewritten away.
    pub fn with_tantivy_indexer(mut self, svc: Arc<crate::tantivy_index::service::TantivyIndexService>) -> Self {
        self.tantivy_indexer = Some(svc);
        self
    }

    pub fn tantivy_indexer(&self) -> Option<&Arc<crate::tantivy_index::service::TantivyIndexService>> {
        self.tantivy_indexer.as_ref()
    }

    /// Query Delta tables directly, bypassing the in-memory buffer (for testing).
    pub async fn query_delta_only(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let mut db_clone = self.clone();
        db_clone.buffered_layer = None;
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
        use tokio_cron_scheduler::{Job, JobScheduler};

        let scheduler = JobScheduler::new().await?;
        let db = Arc::new(self.clone());

        // Light optimize job - every 5 minutes for small recent files
        let light_optimize_schedule = &self.config.maintenance.timefusion_light_optimize_schedule;

        if !light_optimize_schedule.is_empty() {
            info!("Light optimize job scheduled with cron expression: {}", light_optimize_schedule);

            let light_optimize_job = Job::new_async(light_optimize_schedule, {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled light optimize on recent small files");
                        // Optimize unified tables. Run dedup FIRST so the
                        // light compact bin-packs already-deduped files —
                        // otherwise compact would rewrite the duplicates into
                        // a single file that we'd then have to rewrite again.
                        for (table_name, table) in db.unified_tables.read().await.iter() {
                            if let Err(e) = db.dedup_today_partitions(table, table_name, table_name).await {
                                error!("Dedup sweep failed for unified table '{}': {}", table_name, e);
                            }
                            match db.optimize_table_light(table, table_name).await {
                                Ok(_) => info!("Light optimize completed for unified table '{}'", table_name),
                                Err(e) => error!("Light optimize failed for unified table '{}': {}", table_name, e),
                            }
                        }
                        // Optimize custom project tables
                        for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                            let key = format!("{}:{}", project_id, table_name);
                            if let Err(e) = db.dedup_today_partitions(table, table_name, &key).await {
                                error!("Dedup sweep failed for custom project '{}' table '{}': {}", project_id, table_name, e);
                            }
                            match db.optimize_table_light(table, table_name).await {
                                Ok(_) => info!("Light optimize completed for custom project '{}' table '{}'", project_id, table_name),
                                Err(e) => error!("Light optimize failed for custom project '{}' table '{}': {}", project_id, table_name, e),
                            }
                        }
                    })
                }
            })?;

            scheduler.add(light_optimize_job).await?;
        } else {
            info!("Light optimize job scheduling skipped - empty schedule");
        }

        // Optimize job - configurable schedule (default: every 30mins)
        let optimize_schedule = &self.config.maintenance.timefusion_optimize_schedule;

        if !optimize_schedule.is_empty() {
            info!(
                "Optimize job scheduled with cron expression: {} (processes last 28 hours only)",
                optimize_schedule
            );

            let optimize_job = Job::new_async(optimize_schedule, {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled optimize on all tables");
                        // Optimize unified tables
                        for (table_name, table) in db.unified_tables.read().await.iter() {
                            if let Err(e) = db.optimize_table(table, table_name, None).await {
                                error!("Optimize failed for unified table '{}': {}", table_name, e);
                            }
                        }
                        // Optimize custom project tables
                        for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                            if let Err(e) = db.optimize_table(table, table_name, None).await {
                                error!("Optimize failed for custom project '{}' table '{}': {}", project_id, table_name, e);
                            }
                        }
                    })
                }
            })?;

            scheduler.add(optimize_job).await?;
        } else {
            info!("Optimize job scheduling skipped - empty schedule");
        }

        // Recompress job - daily tier upgrade for cold (14d+).
        // Skips partitions whose probe file already advertises the target tier
        // via Parquet footer metadata, so re-runs are cheap on stable data.
        let recompress_schedule = self.config.maintenance.timefusion_recompress_schedule.clone();
        let cold_cutoff = self.config.parquet.timefusion_cold_cutoff_days;
        let zstd_cold = self.config.parquet.timefusion_zstd_level_cold;

        if !recompress_schedule.is_empty() {
            info!(
                "Recompress job scheduled: {} (warm→cold@{}d zstd={})",
                recompress_schedule, cold_cutoff, zstd_cold
            );
            // Cold sweep upper bound — partitions older than this fall under
            // vacuum; we don't need to keep extending the window indefinitely.
            let cold_upper = (self.config.maintenance.timefusion_vacuum_retention_hours / 24).max(cold_cutoff + 60);

            let recompress_job = Job::new_async(recompress_schedule.as_str(), {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled tier recompression");
                        // Flatten unified + custom tables into one (name, table) list.
                        let mut targets: Vec<(String, Arc<RwLock<DeltaTable>>)> =
                            db.unified_tables.read().await.iter().map(|(n, t)| (n.clone(), t.clone())).collect();
                        targets.extend(db.custom_project_tables.read().await.iter().map(|((_, n), t)| (n.clone(), t.clone())));
                        for (name, table) in &targets {
                            if let Err(e) = db.recompress_tier_window(table, name, cold_cutoff, cold_upper, zstd_cold).await {
                                error!("Recompress (cold tier) failed for '{}': {}", name, e);
                            }
                        }
                    })
                }
            })?;
            scheduler.add(recompress_job).await?;
        } else {
            info!("Recompress job scheduling skipped - empty schedule");
        }

        // Vacuum job - configurable schedule (default: daily at 2AM)
        let vacuum_schedule = &self.config.maintenance.timefusion_vacuum_schedule;
        let vacuum_retention = self.config.maintenance.timefusion_vacuum_retention_hours;

        if !vacuum_schedule.is_empty() {
            info!("Vacuum job scheduled with cron expression: {}", vacuum_schedule);

            let vacuum_job = Job::new_async(vacuum_schedule.as_str(), {
                let db = db.clone();
                move |_, _| {
                    let db = db.clone();
                    Box::pin(async move {
                        info!("Running scheduled vacuum on all tables");
                        let retention_hours = vacuum_retention;

                        // Vacuum unified tables
                        for (table_name, table) in db.unified_tables.read().await.iter() {
                            info!("Vacuuming unified table '{}' (retention: {}h)", table_name, retention_hours);
                            db.vacuum_table(table, retention_hours).await;
                        }
                        // Vacuum custom project tables
                        for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                            info!(
                                "Vacuuming custom project '{}' table '{}' (retention: {}h)",
                                project_id, table_name, retention_hours
                            );
                            db.vacuum_table(table, retention_hours).await;
                        }
                    })
                }
            })?;

            scheduler.add(vacuum_job).await?;
        } else {
            info!("Vacuum job scheduling skipped - empty schedule");
        }

        // Cache stats job - every 5 minutes
        let cache_stats_job = Job::new_async("0 */5 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    // Log Foyer cache stats if available
                    if let Some(ref cache) = db.object_store_cache {
                        cache.log_stats().await;
                    }

                    // Log statistics cache stats
                    let (used, capacity) = db.statistics_extractor.get_cache_stats().await;
                    info!("Statistics cache: {}/{} entries used", used, capacity);
                })
            }
        })?;

        scheduler.add(cache_stats_job).await?;

        // Statistics refresh job - every 15 minutes
        let stats_refresh_job = Job::new_async("0 */15 * * * *", {
            let db = db.clone();
            move |_, _| {
                let db = db.clone();
                Box::pin(async move {
                    info!("Refreshing Delta Lake statistics cache");
                    db.statistics_extractor.clear_cache().await;

                    // Pre-warm cache for unified tables
                    for (table_name, table) in db.unified_tables.read().await.iter() {
                        let table = table.read().await;
                        let current_version = table.version().unwrap_or(0);
                        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
                        let schema = schema_def.schema_ref();
                        // Use empty string for project_id since unified tables are shared
                        if let Err(e) = db.statistics_extractor.extract_statistics(&table, "", table_name, &schema).await {
                            error!("Failed to refresh statistics for unified table '{}': {}", table_name, e);
                        } else {
                            debug!("Refreshed statistics for unified table '{}' (version {})", table_name, current_version);
                        }
                    }
                    // Pre-warm cache for custom project tables
                    for ((project_id, table_name), table) in db.custom_project_tables.read().await.iter() {
                        let table = table.read().await;
                        let current_version = table.version().unwrap_or(0);
                        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
                        let schema = schema_def.schema_ref();
                        if let Err(e) = db.statistics_extractor.extract_statistics(&table, project_id, table_name, &schema).await {
                            error!("Failed to refresh statistics for {}:{}: {}", project_id, table_name, e);
                        } else {
                            debug!("Refreshed statistics for {}:{} (version {})", project_id, table_name, current_version);
                        }
                    }
                })
            }
        })?;

        scheduler.add(stats_refresh_job).await?;

        // Start the scheduler
        scheduler.start().await?;

        // Handle shutdown
        let shutdown = self.maintenance_shutdown.clone();
        tokio::spawn(async move {
            shutdown.cancelled().await;
            info!("Shutting down maintenance scheduler");
            // Note: scheduler will be dropped when this task ends
        });

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
        let _ = options.set(
            "datafusion.execution.parquet.metadata_size_hint",
            &self.config.cache.timefusion_parquet_metadata_size_hint.to_string(),
        );
        let _ = options.set("datafusion.explain.show_schema", "true");
        // NOTE: the decoded-metadata cache limit is NOT set here — a
        // `datafusion.runtime.*` SessionConfig string does not reconfigure an
        // already-built RuntimeEnv. It is applied on the RuntimeEnvBuilder
        // below via `build_query_runtime_env` instead.

        // Cap query parallelism at the container's CPU quota (derived in
        // autotune::apply; 0 = leave DataFusion's default). See MemoryConfig.
        if self.config.memory.timefusion_query_partitions > 0 {
            let _ = options.set(
                "datafusion.execution.target_partitions",
                &self.config.memory.timefusion_query_partitions.to_string(),
            );
        }

        // Enable general statistics collection for query optimization.
        // (DataFusion default is `true` — set explicitly so a future default flip
        // doesn't silently regress query plans.)
        let _ = options.set("datafusion.execution.collect_statistics", "true");

        // Enable bloom filter pruning if available in Parquet files
        let _ = options.set("datafusion.execution.parquet.bloom_filter_on_read", "true");

        // Time-series optimized settings
        // Larger batch size for better throughput with time-series data
        let _ = options.set("datafusion.execution.batch_size", "65536");

        // Optimize for sorted data (timestamps are typically sorted)
        let _ = options.set("datafusion.optimizer.prefer_existing_sort", "true");

        // Enable repartition for better parallel aggregations
        let _ = options.set("datafusion.optimizer.repartition_aggregations", "true");

        // Disable round-robin repartitioning to maintain sort order
        let _ = options.set("datafusion.optimizer.enable_round_robin_repartition", "false");

        // Enable filter and limit pushdown optimizations
        let _ = options.set("datafusion.optimizer.filter_null_join_keys", "true");
        let _ = options.set("datafusion.optimizer.skip_failed_rules", "false");

        // Enable proper limit handling across partitions
        let _ = options.set("datafusion.optimizer.enable_distinct_aggregation_soft_limit", "true");
        let _ = options.set("datafusion.optimizer.enable_topk_aggregation", "true");

        // Memory management for large time-series queries
        let _ = options.set("datafusion.execution.coalesce_batches", "true");
        let _ = options.set("datafusion.execution.coalesce_target_batch_size", "65536");

        // Enable all optimizer rules for maximum optimization
        let _ = options.set("datafusion.optimizer.max_passes", "5");

        // Configure memory limit for DataFusion operations
        let memory_limit_bytes = self.config.memory.memory_limit_bytes();
        let memory_fraction = self.config.memory.timefusion_memory_fraction;
        let sort_spill_reservation_bytes = self.config.memory.timefusion_sort_spill_reservation_bytes.unwrap_or(67_108_864);

        // Set memory-related configuration options
        let _ = options.set("datafusion.execution.memory_fraction", &memory_fraction.to_string());
        let _ = options.set("datafusion.execution.sort_spill_reservation_bytes", &sort_spill_reservation_bytes.to_string());

        // Memory pool: defaults to Greedy (single global cap, no per-consumer slicing)
        // for ingest-heavy workloads. Opt into FairSpill for ad-hoc multi-tenant
        // query workloads via TIMEFUSION_MEMORY_POOL=fair_spill.
        // Built once and shared by every session context (pgwire, internal SQL,
        // maintenance) so the cap is a real process-wide budget.
        let runtime_env = self
            .runtime_env
            .get_or_init(|| {
                let pool_size = (memory_limit_bytes as f64 * memory_fraction) as usize;
                let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = match self.config.memory.timefusion_memory_pool {
                    crate::config::MemoryPoolKind::Greedy => Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(pool_size)),
                    crate::config::MemoryPoolKind::FairSpill => Arc::new(datafusion::execution::memory_pool::FairSpillPool::new(pool_size)),
                };
                let meta_cache_bytes = self.config.cache.timefusion_df_metadata_cache_mb * 1024 * 1024;
                Arc::new(build_query_runtime_env(pool, meta_cache_bytes))
            })
            .clone();

        // Set up tracing options with configurable sampling
        let record_metrics = self.config.memory.timefusion_tracing_record_metrics;

        let tracing_options = InstrumentationOptions::builder().record_metrics(record_metrics).preview_limit(5).build();

        // Create instrumentation rule
        let instrument_rule = instrument_with_info_spans!(
            options: tracing_options,
        );

        // Create session state with tracing rule and DML support
        // Rule ordering: VariantInsertRewriter runs BEFORE TypeCoercion (rewrites string->json_to_variant)
        //                VariantSelectRewriter runs AFTER TypeCoercion (wraps Variant cols with variant_to_json)
        let analyzer_rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> = vec![
            Arc::new(datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction::new()),
            Arc::new(crate::optimizers::VariantInsertRewriter),
            // Tantivy predicate rewriter runs BEFORE TypeCoercion so the
            // injected `text_match(col, lit)` calls get coerced like any
            // other UDF args (Utf8 vs Utf8View etc).
            Arc::new(crate::optimizers::TantivyPredicateRewriter),
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
            .with_physical_optimizer_rule(instrument_rule)
            .with_query_planner(Arc::new({
                let planner = DmlQueryPlanner::new(self.clone());
                if let Some(layer) = self.buffered_layer.as_ref() {
                    planner.with_buffered_layer(Arc::clone(layer))
                } else {
                    planner
                }
            }))
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
                let routing_table = ProjectRoutingTable::new(
                    "default".to_string(),
                    Arc::new(self.clone()),
                    schema.schema_ref(),
                    batch_queue.clone(),
                    table_name.clone(),
                );
                ctx.register_table(&table_name, Arc::new(routing_table))?;
                info!("Registered ProjectRoutingTable for table '{}' with SessionContext", table_name);
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
        let cache_sizes: crate::stats_table::CacheSizeSnapshot = Arc::new(move || (fr_handle.len(), dp_handle.len()));
        ctx.register_table(
            "timefusion_stats",
            Arc::new(
                crate::stats_table::StatsTableProvider::new(self.buffered_layer.clone())
                    .with_scan_metrics(self.scan_metrics.clone())
                    .with_cache_sizes(cache_sizes),
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

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8View, false),
            Field::new("setting", DataType::Utf8View, false),
        ]));

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

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringViewArray::from(names)), Arc::new(StringViewArray::from(settings))],
        )?;

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

        let set_config_udf = create_udf(
            "set_config",
            vec![DataType::Utf8View, DataType::Utf8View, DataType::Boolean],
            DataType::Utf8View,
            Volatility::Volatile,
            set_config_fn,
        );

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
            .map(|f| !f.load(std::sync::atomic::Ordering::Acquire))
            .unwrap_or(false)
    }

    /// Mark a (project, table) as having Delta files. Called by the flush
    /// callback after a successful commit.
    pub fn mark_delta_has_files(&self, project_id: &str, table_name: &str) {
        let key = (project_id.to_string(), table_name.to_string());
        let flag = self.delta_has_files.entry(key).or_insert_with(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        flag.store(true, std::sync::atomic::Ordering::Release);
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
                && self
                    .storage_configs_next_refresh_ns
                    .compare_exchange(next, now_ns + STORAGE_CONFIGS_TTL_NS, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
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
        let has_files = t.read().await.version().map(|v| v > 0).unwrap_or(false);
        let entry = self.delta_has_files.entry(key).or_insert_with(|| Arc::new(std::sync::atomic::AtomicBool::new(false)));
        if has_files {
            // Release pairs with the Acquire load in delta_scan_can_be_skipped
            // (see comment there). Same rationale.
            entry.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Resolve a unified table (shared by all default projects, partitioned by project_id)
    async fn resolve_unified_table(&self, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // Check unified_tables cache first
        {
            let tables = self.unified_tables.read().await;
            if let Some(table) = tables.get(table_name) {
                debug!("Found unified table '{}' in cache", table_name);
                // For unified tables, we use table_name as the key for version tracking
                let last_written_version = {
                    let versions = self.last_written_versions.read().await;
                    // Use empty string for project_id since unified tables aren't project-specific
                    versions.get(&("".to_string(), table_name.to_string())).cloned()
                };

                let current_version = table.read().await.version();
                let should_update = should_refresh_table(current_version, last_written_version);

                if should_update {
                    self.update_table(table, "", table_name)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("Failed to update table: {}", e)))?;
                }

                return Ok(Arc::clone(table));
            }
        }

        // Not in cache, create/load it
        self.get_or_create_unified_table(table_name)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get or create unified table: {}", e)))
    }

    /// Resolve a custom project table (isolated table for projects with their own S3 bucket)
    async fn resolve_custom_table(&self, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        // Check custom_project_tables cache first
        {
            let tables = self.custom_project_tables.read().await;
            if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
                debug!("Found custom table for project '{}' table '{}' in cache", project_id, table_name);
                let last_written_version = {
                    let versions = self.last_written_versions.read().await;
                    versions.get(&(project_id.to_string(), table_name.to_string())).cloned()
                };

                let current_version = table.read().await.version();
                let should_update = should_refresh_table(current_version, last_written_version);

                if should_update {
                    self.update_table(table, project_id, table_name)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("Failed to update table: {}", e)))?;
                }

                return Ok(Arc::clone(table));
            }
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

        let prefix = self
            .default_s3_prefix
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No default S3 prefix configured for unified table '{}'", table_name))?;
        let endpoint = self
            .default_s3_endpoint
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("No default S3 endpoint configured for unified table '{}'", table_name))?;
        // Unified table path: s3://{bucket}/{prefix}/{table_name}/ (NO project_id subdirectory)
        let storage_uri = format!("s3://{}/{}/{}/?endpoint={}", bucket, prefix, table_name, endpoint);
        let storage_options = self.build_storage_options();

        info!("Creating or loading unified table '{}' at: {}", table_name, storage_uri);

        // Hold write lock during table creation
        let mut tables = self.unified_tables.write().await;

        // Double-check after acquiring write lock
        if let Some(table) = tables.get(table_name) {
            return Ok(Arc::clone(table));
        }

        let table = self.create_delta_table_internal(&storage_uri, &storage_options, table_name).await?;
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
            config
                .s3_endpoint
                .as_ref()
                .unwrap_or(&self.default_s3_endpoint.clone().unwrap_or_else(|| "https://s3.amazonaws.com".to_string()))
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

        info!(
            "Creating or loading custom table for project '{}' table '{}' at: {}",
            project_id, table_name, storage_uri
        );

        // Hold write lock during table creation
        let mut tables = self.custom_project_tables.write().await;

        // Double-check after acquiring write lock
        if let Some(table) = tables.get(&(project_id.to_string(), table_name.to_string())) {
            return Ok(Arc::clone(table));
        }

        let table = self.create_delta_table_internal(&storage_uri, &storage_options, table_name).await?;
        let table_arc = Arc::new(RwLock::new(table));
        tables.insert((project_id.to_string(), table_name.to_string()), Arc::clone(&table_arc));
        info!(
            "Cached custom table for project '{}' table '{}', cache now contains {} entries",
            project_id,
            table_name,
            tables.len()
        );

        Ok(table_arc)
    }

    /// Internal helper to create/load a Delta table with caching and retry logic
    async fn create_delta_table_internal(&self, storage_uri: &str, storage_options: &HashMap<String, String>, table_name: &str) -> Result<DeltaTable> {
        // Create the base S3 object store
        let base_store = self.create_object_store(storage_uri, storage_options).instrument(tracing::trace_span!("create_object_store")).await?;
        let instrumented_store = instrument_object_store(base_store, "s3");

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
                let desired = HashMap::from([
                    (
                        "delta.deletedFileRetentionDuration".to_string(),
                        format!("interval {} hours", self.config.maintenance.timefusion_vacuum_retention_hours),
                    ),
                    (
                        "delta.checkpointInterval".to_string(),
                        self.config.parquet.timefusion_checkpoint_interval.to_string(),
                    ),
                ]);
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
                    // Default of 32 leaf columns isn't enough for our wide schema (90+ fields);
                    // -1 = index all columns. Needed so kernel data-skipping can evaluate
                    // predicates on columns beyond the first 32 without "No such field" errors.
                    config.insert("delta.dataSkippingNumIndexedCols".to_string(), Some("-1".to_string()));

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
    /// Non-blocking and strictly best-effort: the whole job runs in a detached,
    /// concurrency-bounded task and never affects the commit. Files are filtered
    /// to partitions within `timefusion_warm_recency_days` so we don't spend S3
    /// GETs (and evict useful entries) warming cold partitions nobody reads.
    async fn warm_cache_for_uris(&self, object_store: Arc<dyn object_store::ObjectStore>, table_uri: String, uris: Vec<String>) {
        let maint = &self.config.maintenance;
        if !maint.timefusion_warm_after_compaction || uris.is_empty() {
            return;
        }
        let warm_full_files = maint.timefusion_warm_full_files;
        let warm_all_footers = maint.timefusion_warm_all_footers;
        let recency_days = maint.timefusion_warm_recency_days;
        let concurrency = maint.timefusion_warm_concurrency.max(1);
        let metadata_size_hint = self.config.cache.timefusion_parquet_metadata_size_hint as u64;
        let stats_cache = self.object_store_cache.clone();

        // Relativize absolute s3:// URIs against the table root: the cached
        // object store consumes bucket-relative paths.
        let prefix = table_cache_prefix(&table_uri);
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
        let baseline = match &stats_cache {
            Some(cache) => {
                let s = cache.get_stats().await.main;
                let rate = if s.hits + s.misses > 0 { (s.hits as f64 / (s.hits + s.misses) as f64) * 100.0 } else { 0.0 };
                Some(rate)
            }
            None => None,
        };

        // Labelled scope rather than `full=true/false` so warm logs are easy to
        // filter (e.g. in Loki) by what was actually primed.
        let scope = if warm_full_files { "full" } else { "footer-only" };
        // Surface the burst size up front so operators can see what a restart
        // is about to issue against S3 (the completion log alone can't —
        // a large warm set takes minutes to get there).
        info!("Cache warm start: {count} files (scope={scope}, concurrency={concurrency})");
        let t0 = std::time::Instant::now();
        // Progress heartbeat: a 10k-file boot warm runs minutes; without one
        // operators can't tell warming from a hang. The {count} denominator
        // is the selected warm set (footer warms); full-file warming covers
        // only the `recent` subset of it.
        const WARM_PROGRESS_INTERVAL: usize = 500;
        let done = std::sync::atomic::AtomicUsize::new(0);
        let done = &done;
        futures::stream::iter(paths)
            .for_each_concurrent(concurrency, |(path, recent)| {
                let store = object_store.clone();
                async move {
                    let _ = crate::object_store_cache::warm_footer(store.as_ref(), &path, metadata_size_hint).await;
                    if warm_full_files && recent {
                        let _ = crate::object_store_cache::warm_full(store.as_ref(), &path).await;
                    }
                    let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if n.is_multiple_of(WARM_PROGRESS_INTERVAL) {
                        // Elapsed on the heartbeat lets operators extrapolate
                        // time-remaining without waiting for completion.
                        info!("Cache warm progress: {n}/{count} files ({:.1}s elapsed)", t0.elapsed().as_secs_f64());
                    }
                }
            })
            .await;

        let elapsed_s = t0.elapsed().as_secs_f64();
        match baseline {
            Some(rate) => info!(
                "Cache warm complete: {} files warmed (scope={}) in {:.1}s; foyer main hit rate before warm was {:.2}% (next query benefits)",
                count, scope, elapsed_s, rate
            ),
            None => info!("Cache warm complete: {} files warmed (scope={}) in {:.1}s", count, scope, elapsed_s),
        }
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
        let mut evicted = 0usize;
        let mut dropped = 0usize;
        for u in removed {
            if let Some(path) = relativize_to_prefix(prefix, u) {
                cache.evict_data_entry(path.as_ref());
                evicted += 1;
            } else {
                // Prefix mismatch (trailing-slash or query-string drift between
                // table_url() and get_file_uris()) — we'd evict the wrong key, so
                // skip. Log like the warm path: a systematic mismatch here means
                // tombstoned files linger in cache until TTL/LRU, which is worth
                // diagnosing rather than silently swallowing.
                if dropped == 0 {
                    debug!("evict: URI {} does not start with table prefix {}; skipping (evict only)", u, prefix);
                }
                dropped += 1;
            }
        }
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
                db.warm_cache_for_uris(store, table_uri, uris).await;
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
                            info!(
                                "bootstrap.phase=table_preload table={table_name} files={} elapsed_ms={}",
                                uris.len(),
                                t.elapsed().as_millis()
                            );
                            db.warm_cache_for_uris(store, table_uri, uris).await;
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
    /// Both optimize paths — full Z-order and light — funnel through here so the
    /// warm/evict pair can't drift; the evict call was once missing from the
    /// light path, and a single helper keeps them in lockstep.
    async fn swap_and_refresh_cache(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, new_table: DeltaTable, pre_uris: &std::collections::HashSet<String>,
    ) -> Vec<String> {
        // Capture live URIs off `new_table` *before* the swap moves it in.
        let live_uris: Vec<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
        let live_set: std::collections::HashSet<&String> = live_uris.iter().collect();
        let added: Vec<String> = live_uris.iter().filter(|u| !pre_uris.contains(*u)).cloned().collect();
        let removed: Vec<String> = pre_uris.iter().filter(|u| !live_set.contains(u)).cloned().collect();
        let warm_store = new_table.log_store().object_store(None);
        let warm_table_uri = new_table.table_url().to_string();
        self.persist_snapshot(&new_table);
        {
            // Version-guarded swap so this is safe to call WITHOUT holding
            // `delta_commit_lock`: a concurrent committer may have already
            // advanced `table_ref` past our just-committed version (its refresh
            // picks our commit up from the log), and a bare `*table = new_table`
            // would regress the handle. None < Some(_), so an unloaded handle
            // always swaps. Same guard as `refresh_table_snapshot`.
            let mut table = table_ref.write().await;
            if new_table.version() > table.version() {
                *table = new_table;
            }
        }
        // Eviction is in-cache only (cheap), so run it inline. Warming issues S3
        // GETs, so detach it — the maintenance loop shouldn't block on priming
        // the cache (preserves the previous in-`warm_cache_for_uris` spawn).
        self.evict_cache_for_uris(&warm_table_uri, &removed);
        let db = self.clone();
        tokio::spawn(async move {
            db.warm_cache_for_uris(warm_store, warm_table_uri, added).await;
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
        use std::time::Duration;

        use object_store::{BackoffConfig, ClientOptions, RetryConfig, aws::AmazonS3Builder};

        // Parse the S3 URI to extract bucket and prefix
        let url = Url::parse(storage_uri)?;
        let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("Invalid S3 URI: missing bucket"))?;

        // Configure retry with exponential backoff for transient network errors
        let retry_config = RetryConfig {
            max_retries:   5,
            retry_timeout: Duration::from_secs(180),
            backoff:       BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff:  Duration::from_secs(15),
                base:         2.0,
            },
        };

        // Configure HTTP client with reasonable timeouts
        let client_options = ClientOptions::new().with_connect_timeout(Duration::from_secs(30)).with_timeout(Duration::from_secs(300));

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
        cfg.core.timefusion_data_dir.join(".timefusion_meta").join("delta_snapshots")
    }

    /// Persist `table`'s post-commit snapshot locally (detached) so the next
    /// boot restores it and replays only later commits (see `snapshot_cache`).
    /// Called from every commit path that swaps a fresh table state in.
    pub(crate) fn persist_snapshot(&self, table: &DeltaTable) {
        if let Some(state) = table.state.clone() {
            let (dir, url) = (Self::delta_snapshot_dir(&self.config), table.table_url().to_string());
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
                // e.g. the log tail past the snapshot was vacuumed away → full load.
                table
                    .update_state()
                    .await
                    .inspect_err(|e| warn!("Local snapshot catch-up failed for '{storage_uri}': {e}; falling back to full load"))
                    .ok()
                    .map(|()| {
                        info!(
                            "Restored '{storage_uri}' from local snapshot at v{restored_version}, caught up to {:?}",
                            table.version()
                        );
                        table
                    })
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
    /// Insert batches and return the URIs of files newly added by this commit
    /// (empty for the buffered-layer / batch-queue paths where the actual
    /// Delta write happens later). Callers use the returned list to drive
    /// cache warming and the tantivy sidecar without paying for a second
    /// `update_state()` log scan.
    pub async fn insert_records_batch(
        &self, project_id: &str, table_name: &str, batches: Vec<RecordBatch>, skip_queue: bool, watermark: Option<&crate::buffered_write_layer::DeltaWatermark>,
    ) -> Result<Vec<String>> {
        let span = tracing::Span::current();
        // Normalize timezone-as-offset (`+00:00`) timestamp columns to the
        // IANA `"UTC"` form. Delta-rs Arrow→Delta schema conversion only
        // accepts `"UTC"`; without this normalisation the flush callback
        // path (which feeds MemBuffer batches straight into Delta) errors
        // out and data piles up in MemBuffer.
        let batches: Vec<RecordBatch> = batches.into_iter().map(normalize_timestamp_tz).collect::<DFResult<Vec<_>>>()?;

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

        // If buffered layer is configured and not skipping, use it (WAL → MemBuffer flow).
        // No files are written synchronously on this path; an empty URI list is correct.
        if !skip_queue && let Some(ref layer) = self.buffered_layer {
            span.record("use_queue", "buffered_layer");
            layer.insert(&project_id, &table_name, batches).await?;
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

        // Delta-kernel's `unshredded_variant()` expects Struct{Binary,Binary}
        // on write, but our MemBuffer carries Struct{BinaryView,BinaryView}
        // (matches what the parquet reader natively produces — no per-row
        // casts on read). Cast just-before-write so the Delta commit
        // accepts the schema.
        let batches: Vec<RecordBatch> = batches.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;

        // Get or create the table
        let table_ref = self.get_or_create_table(&project_id, &table_name).await?;

        // Get the appropriate schema for this table
        let schema = get_schema(&table_name).unwrap_or_else(get_default_schema);

        // Cluster by the declared sort keys (timestamp-first) so the parquet
        // SortingColumn footer is honest and the page index localizes the lead
        // key. `sorted` is false when a schema-evolved bucket can't be combined
        // (we then write unsorted) — declare the footer only when it's true.
        let (batches, sorted) = sort_batches_by_schema(schema, batches);
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted);

        // Hoist out of the retry loop — the watermark is the same on every attempt.
        let commit_properties = watermark.map(build_watermark_commit_properties);
        // Let the post-commit hook advance the snapshot incrementally — carry
        // the materialized file list forward, append the committed files, drop
        // any removed ones — instead of re-materializing the whole active set.
        // Safe for the staged (pure-append) and schema-evolution merge paths
        // alike: the hook rebuilds the kernel snapshot from the log, so a
        // MetaData/schema change IS applied; only the file-list re-materialize
        // is skipped.
        let commit_properties = if self.config.maintenance.timefusion_incremental_snapshot {
            Some(commit_properties.unwrap_or_default().with_incremental_advance(true))
        } else {
            commit_properties
        };
        let max_retries = 5;
        // Exponential backoff between OCC conflict retries (shared by both paths).
        let retry_backoff = |n: u32| tokio::time::Duration::from_millis(100 * 2_u64.pow(n.min(5)));
        // STAGED COMMIT (fast path): encode parquet + upload to S3 OUTSIDE the
        // global `delta_commit_lock`, then serialize only the tiny commit-log
        // append. The old path held the lock across the whole `.write()`
        // (parquet encode + S3 upload + commit), serializing every tenant's
        // upload behind one mutex — the ~8-17 rows/s flush ceiling under heavy
        // backfill. A staged write parallelizes the uploads and pays the lock
        // only for a sub-second log append; OCC conflicts re-commit the already
        // uploaded parquet (no re-encode/re-upload).
        //
        // delta-rs' Default-mode RecordBatchWriter cannot evolve schema on a
        // partitioned table, so when a batch carries a column absent from the
        // table schema we fall back to the locked WriteBuilder merge path below.
        let staging_table = { table_ref.read().await.clone() };
        let staged_writer = match deltalake::writer::RecordBatchWriter::for_table(&staging_table) {
            Ok(w) => {
                let w = w.with_writer_properties(writer_properties.clone());
                let arrow_schema = w.arrow_schema();
                let table_fields: std::collections::HashSet<&str> = arrow_schema.fields().iter().map(|f| f.name().as_str()).collect();
                let evolves = batches.iter().any(|b| b.schema().fields().iter().any(|f| !table_fields.contains(f.name().as_str())));
                (!evolves).then_some(w)
            }
            Err(e) => {
                debug!("RecordBatchWriter::for_table failed, using merge path: {}", e);
                None
            }
        };

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
                for b in &batches {
                    let casted = deltalake::kernel::schema::cast_record_batch(b, target_schema.clone(), true, true)?;
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
            let op = DeltaOperation::Write {
                mode: deltalake::protocol::SaveMode::Append,
                partition_by,
                predicate: None,
            };
            // Store to clean up the staged parquet on a terminal commit failure —
            // those objects have no Add/Remove in the log, so Delta VACUUM won't
            // reclaim them; abandoning them leaks files on S3 forever.
            let stage_store = staging_table.log_store().object_store(None);

            let mut retry_count = 0;
            loop {
                // Refresh UNDER the lock (the merge path refreshes before locking).
                // delta_commit_lock serializes all in-process commits, so
                // refreshing here guarantees we build on the previous committer's
                // version and never self-conflict; refresh is probe-cheap (a single
                // GET that 404-short-circuits when already current), so the extra
                // lock-hold is sub-millisecond on the common path.
                let commit_guard = self.delta_commit_lock.lock().await;
                if let Err(e) = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                    debug!("pre-commit refresh failed (attempt {}): {}", retry_count + 1, e);
                }
                let mut new_table = { table_ref.read().await.clone() };
                let commit_res = deltalake::kernel::transaction::CommitBuilder::from(commit_properties.clone().unwrap_or_default())
                    .with_actions(adds.clone())
                    .build(Some(new_table.snapshot()? as &dyn TableReference), new_table.log_store(), op.clone())
                    .await;
                match commit_res {
                    Ok(finalized) => {
                        // Diff pre- vs post-commit file URIs for `added`. Capture
                        // pre-uris here (only on success) — before the state swap
                        // below makes `new_table` post-commit — so failed attempts
                        // don't pay the full-table file-URI walk.
                        let pre_uris: std::collections::HashSet<String> = new_table.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                        new_table.state = Some(finalized.snapshot());
                        drop(commit_guard);
                        return Ok(self.record_committed_write(&table_ref, &project_id, &table_name, new_table, &pre_uris, watermark.is_some()).await);
                    }
                    Err(e) => {
                        drop(commit_guard);
                        if !is_occ_conflict_err(&e.to_string()) {
                            Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                            return Err(anyhow::anyhow!("staged commit failed: {}", e));
                        }
                        retry_count += 1;
                        if retry_count >= max_retries {
                            Self::cleanup_orphaned_parquet(&stage_store, &adds).await;
                            return Err(anyhow::anyhow!("staged commit failed after {} retries: {}", max_retries, e));
                        }
                        debug!("staged commit conflict, retrying ({}/{}): {}", retry_count, max_retries, e);
                        tokio::time::sleep(retry_backoff(retry_count)).await;
                    }
                }
            }
        }

        // SCHEMA-EVOLUTION FALLBACK: locked WriteBuilder merge path. Holds the
        // commit lock across the whole write so the schema-metadata merge can't
        // race a concurrent commit. Rare (only when a batch adds a column).
        let mut retry_count = 0;
        let mut last_error = None;
        while retry_count < max_retries {
            if let Err(e) = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                debug!("Failed to update table state before write (attempt {}): {}", retry_count + 1, e);
            }
            let commit_guard = self.delta_commit_lock.lock().await;
            let (table, pre_uris) = {
                let guard = table_ref.read().await;
                let pre: std::collections::HashSet<String> = guard.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                (guard.clone(), pre)
            };

            let write_span = tracing::trace_span!(parent: &span, "delta.write_operation", retry_attempt = retry_count + 1);
            let write_result = async {
                let mut builder = table
                    .clone()
                    .write(batches.clone())
                    .with_partition_columns(schema.partitions.clone())
                    .with_writer_properties(writer_properties.clone())
                    .with_save_mode(deltalake::protocol::SaveMode::Append)
                    .with_schema_mode(deltalake::operations::write::SchemaMode::Merge);
                if let Some(cp) = commit_properties.clone() {
                    builder = builder.with_commit_properties(cp);
                }
                builder.await
            }
            .instrument(write_span)
            .await;

            match write_result {
                Ok(new_table) => {
                    let added = self.record_committed_write(&table_ref, &project_id, &table_name, new_table, &pre_uris, watermark.is_some()).await;
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
                        tokio::time::sleep(retry_backoff(retry_count)).await;
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
    async fn record_committed_write(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str, new_table: DeltaTable, pre_uris: &std::collections::HashSet<String>,
        warm: bool,
    ) -> Vec<String> {
        let committed_version = new_table.version();
        if let Some(version) = committed_version {
            self.last_written_versions.write().await.insert((project_id.to_string(), table_name.to_string()), version);
            debug!("Stored last written version for {}/{}: {}", project_id, table_name, version);
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
            let db = self.clone();
            let warm_added = added.clone();
            let shutdown = self.maintenance_shutdown.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = shutdown.cancelled() => {}
                    _ = db.warm_cache_for_uris(warm_store, warm_table_uri, warm_added) => {}
                }
            });
        }
        self.statistics_extractor.invalidate(project_id, table_name).await;
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
    /// the crash-mid-flush window where Delta committed but `advance_by_counts`
    /// didn't finish — without this, restart replays entries already in Delta
    /// and the next flush writes them a second time.
    ///
    /// Must run *before* `recover_from_wal`. Best-effort: any failure to read
    /// metadata is logged and skipped (walrus's locally-fsynced cursor wins),
    /// so this can't make recovery worse than today's at-least-once behaviour.
    pub async fn derive_wal_cursors_from_delta(&self, wal: &crate::wal::WalManager) -> anyhow::Result<usize> {
        use futures::stream::{self, StreamExt};
        let totals: Vec<usize> = stream::iter(wal.list_topic_pairs()?)
            .map(|(project_id, table_name)| async move { self.derive_wal_cursor_for_table(wal, &project_id, &table_name).await.unwrap_or(0) })
            .buffer_unordered(self.config.buffer.delta_scan_concurrency())
            .collect()
            .await;
        Ok(totals.into_iter().sum())
    }

    async fn derive_wal_cursor_for_table(&self, wal: &crate::wal::WalManager, project_id: &str, table_name: &str) -> anyhow::Result<usize> {
        // Scan recent commits; replay-derived commits without a watermark
        // contribute nothing so they can't reset the MAX backward.
        let Ok(table_ref) = self.resolve_table(project_id, table_name).await else {
            return Ok(0);
        };
        let table = table_ref.read().await;
        let commits: Vec<_> = match table.history(Some(self.config.buffer.delta_scan_depth())).await {
            Ok(it) => it.collect(),
            Err(e) => {
                debug!("derive_wal_cursor: history unavailable for {}/{}: {}", project_id, table_name, e);
                return Ok(0);
            }
        };
        drop(table);

        let delta_max = max_watermark_across_commits(commits.iter().map(|ci| &ci.info), wal.shards_per_topic());
        let advanced = wal.merge_persisted_positions(project_id, table_name, &delta_max)?;
        if advanced > 0 {
            info!(
                "Delta-derived cursor advance: project={}, table={}, shards_advanced={}",
                project_id, table_name, advanced
            );
        }
        Ok(advanced)
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
        let window_dates: Vec<chrono::NaiveDate> = (0..=num_days).map(|days_ago| (now - chrono::Duration::days(days_ago as i64)).date_naive()).collect();

        // Snapshot the current live file set once: drives both the ZOrder
        // idempotence guard (below) and PR #39's warm/evict (`pre_uris`).
        let all_uris: Vec<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();
        let table_url = table_clone.table_url().to_string();
        let current = Self::filesets_for_dates(&all_uris, &window_dates);

        // Pre-state file set, used to derive the files this optimize *adds*
        // (to warm) and *removes* (to evict) — see warm/evict_cache_for_uris.
        let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
        let pre_uris: std::collections::HashSet<String> = if track_files { all_uris.iter().cloned().collect() } else { Default::default() };

        let target_size = self.config.parquet.timefusion_optimize_target_size;

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
            info!(
                "optimize: table={} all {} window partitions unchanged since last run — skipping (cache churn avoided)",
                table_name,
                window_dates.len()
            );
            crate::metrics::record_optimize_partitions(0, skipped as u64);
            return Ok(());
        }

        info!(
            "Starting optimize (z-order): table={} rewriting {} of {} window partitions, skipping {} unchanged (last {}h)",
            table_name,
            kept_dates.len(),
            window_dates.len(),
            skipped,
            window_hours
        );

        let partition_filters: Vec<PartitionFilter> =
            kept_dates.iter().filter_map(|d| PartitionFilter::try_from(("date", "=", d.to_string().as_str())).ok()).collect();

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // Full Z-order optimize runs every 30 min over a 48h window — promote
        // these rewrites to the "warm" tier so day-old data lands smaller on
        // disk without slowing the hot flush path.
        // declare_sorted=false: Z-order/Compact reorders rows, so the footer must not claim the schema sort order.
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_level_warm, false);

        // Same trade-off as optimize_table_light: best-effort, don't pause
        // flushes (see comment there). Z-order full optimize is daily-ish,
        // so an occasional OCC failure is fine.
        let optimize_result = table_clone
            .optimize()
            .with_filters(&partition_filters)
            .with_type(if schema.z_order_columns.is_empty() {
                deltalake::operations::optimize::OptimizeType::Compact
            } else {
                deltalake::operations::optimize::OptimizeType::ZOrder(schema.z_order_columns.clone())
            })
            .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
            .with_max_concurrent_tasks(self.config.maintenance.timefusion_optimize_max_concurrent_tasks)
            .with_writer_properties(writer_properties)
            .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
            .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
            // Avoid the BinaryView read for Variant columns (same issue as
            // optimize_table_light); delta-rs's internal session defaults to
            // schema_force_view_types=true.
            .with_session_state(Arc::new(build_optimize_session_state(self.config.memory.timefusion_query_partitions)))
            .await;

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
                    debug!(
                        "Skipping optimization commit: {} files < min threshold {}",
                        metrics.total_considered_files, min_files
                    );
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
                let live_uris = self.swap_and_refresh_cache(table_ref, new_table, &pre_uris).await;
                // Tantivy compaction GC — drop sidecar indexes for files that
                // were rewritten away. Best-effort: errors are logged.
                if let Some(svc) = self.tantivy_indexer().cloned() {
                    let svc_table = table_name.to_string();
                    // Per-project: collect all (project_id, ...) values from
                    // manifests in this table prefix. Today only the unified
                    // "default" path is exercised in practice; iterate over
                    // known custom projects too.
                    let mut project_ids: Vec<String> =
                        self.custom_project_tables.read().await.keys().filter(|(_, t)| t == table_name).map(|(p, _)| p.clone()).collect();
                    project_ids.push("default".to_string());
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
            for (d, marker) in &markers {
                if uri.contains(marker) {
                    out.get_mut(d).expect("date pre-seeded").insert(uri.clone());
                    break;
                }
            }
        }
        out
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
                                kvs.iter()
                                    .find(|kv| kv.key == COMPRESSION_TIER_KEY)
                                    .and_then(|kv| kv.value.as_ref())
                                    .and_then(|v| v.parse::<i32>().ok())
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
                warn!(
                    "recompress probe: could not relativize {} against {}; rewriting anyway",
                    probe_uri, table_prefix
                );
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

        info!(
            "recompress: rewriting date={} table={} at zstd={} ({} files)",
            date_str,
            table_name,
            target_level,
            uris.len()
        );

        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // declare_sorted=false: recompress rewrites via Z-order/Compact.
        let writer_properties = self.create_writer_properties(schema, target_level, false);
        let partition_filters = vec![PartitionFilter::try_from(("date", "=", date_str.as_str()))?];
        let target_size = self.config.parquet.timefusion_optimize_target_size;

        let table_clone = table_ref.read().await.clone();
        let pre_uris: std::collections::HashSet<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();
        let optimize_result = table_clone
            .optimize()
            .with_filters(&partition_filters)
            // Z-order rewrites every file in the partition (Compact only
            // touches small files), which is exactly what we need to lift
            // the partition's tier.
            .with_type(if schema.z_order_columns.is_empty() {
                deltalake::operations::optimize::OptimizeType::Compact
            } else {
                deltalake::operations::optimize::OptimizeType::ZOrder(schema.z_order_columns.clone())
            })
            .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
            .with_max_concurrent_tasks(self.config.maintenance.timefusion_optimize_max_concurrent_tasks)
            .with_writer_properties(writer_properties)
            .with_min_commit_interval(tokio::time::Duration::from_secs(10 * 60))
            .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
            .with_session_state(Arc::new(build_optimize_session_state(self.config.memory.timefusion_query_partitions)))
            .await;

        match optimize_result {
            Ok((new_table, metrics)) => {
                info!(
                    "recompress: date={} table={} removed={} added={} considered={}",
                    date_str, table_name, metrics.num_files_removed, metrics.num_files_added, metrics.total_considered_files
                );
                // Swap + warm-added/evict-removed like the other optimize
                // paths. A bare swap left the rewritten cold-tier files
                // un-warmed and the tombstoned ones cached — the next query
                // on a recompressed partition paid full S3 reads (1.5 s
                // observed against OVH).
                self.swap_and_refresh_cache(table_ref, new_table, &pre_uris).await;
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

    /// Cross-flush dedup: collapse a `(project_id, date)` partition by the
    /// schema's `dedup_keys` (last-write-wins) and write back via
    /// `replace_where`. No-op on no dedup_keys / no duplicates (avoids
    /// gratuitous Foyer churn). Returns rows dropped.
    pub async fn dedup_partition(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, project_id: &str, date: chrono::NaiveDate) -> Result<u64> {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        if schema.dedup_keys.is_empty() {
            return Ok(0);
        }
        let date_str = date.to_string();

        // Bypass ProjectRoutingTable: its MemBuffer union would feed in-flight
        // rows to dedup, then `replace_where` would write them to Delta —
        // double-writing on the next real flush.
        use deltalake::delta_datafusion::TableProviderBuilder;
        let (snapshot, log_store) = {
            let table = table_ref.read().await;
            (Arc::new(table.snapshot()?.snapshot().clone()), table.log_store())
        };
        let provider = TableProviderBuilder::default()
            .with_log_store(log_store)
            .with_eager_snapshot(snapshot)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("delta table provider: {e}"))?;
        let ctx = datafusion::prelude::SessionContext::new_with_state(build_optimize_session_state(self.config.memory.timefusion_query_partitions));
        let scan_name = "__dedup_src";
        ctx.register_table(scan_name, Arc::new(provider))?;
        // project_id is currently always a UUID/controlled identifier, but defend in depth: escape single quotes
        // so a future caller can't inject SQL through the partition predicate. date_str comes from NaiveDate::to_string
        // and is already safe.
        let safe_pid = project_id.replace('\'', "''");
        let filter = format!("project_id = '{}' AND date = DATE '{}'", safe_pid, date_str);
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
        let chunks: Vec<(String, String)> = if schema.dedup_keys.iter().any(|k| k == "timestamp") {
            // 10-minute bins (not hours): one HOUR of the largest project is
            // >2.1GB of string data — past Arrow's i32 offset limit ("Offset
            // overflow error: 2222394106" in prod) and tens of GB materialized.
            // 10 minutes matches the flush-bucket granularity.
            let probe = format!(
                "SELECT CAST(date_bin(INTERVAL '10 minutes', \"timestamp\", TIMESTAMP '1970-01-01T00:00:00') AS VARCHAR) FROM \
                 (SELECT \"timestamp\", count(*) AS c FROM {scan_name} WHERE {filter} GROUP BY {keys_csv}) AS g \
                 WHERE c > 1 GROUP BY 1 ORDER BY 1"
            );
            let mut hours = Vec::new();
            for batch in ctx.sql(&probe).await?.collect().await? {
                let col = datafusion::arrow::compute::cast(batch.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
                let col = col.as_any().downcast_ref::<datafusion::arrow::array::StringArray>().expect("cast to Utf8");
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        hours.push(col.value(i).to_string());
                    }
                }
            }
            // Rewriting an hour that late data may still flush into races
            // replace_where against the append (the stale materialized chunk
            // would win and drop the fresh rows — same race the old
            // whole-partition rewrite had for the entire day). The buffer
            // holds up to ~70 min of data, so only hours sealed for 2h+ are
            // rewritten; newer dupes clear on a later sweep.
            let sealed_before = Utc::now().naive_utc() - chrono::Duration::hours(2);
            hours
                .into_iter()
                .filter_map(|h| {
                    // CAST .. AS VARCHAR may append fractional seconds or a
                    // timezone suffix; the leading 19 chars are the datetime.
                    let h19 = h.get(..19)?;
                    let start = chrono::NaiveDateTime::parse_from_str(h19, "%Y-%m-%dT%H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(h19, "%Y-%m-%d %H:%M:%S"))
                        .ok()?;
                    let end = start + chrono::Duration::minutes(10);
                    if end > sealed_before {
                        debug!("dedup: skipping unsealed chunk starting {start} (cleared on a later sweep)");
                        return None;
                    }
                    let (s, e) = (start.format("%Y-%m-%d %H:%M:%S"), end.format("%Y-%m-%d %H:%M:%S"));
                    Some((
                        format!("{filter} AND \"timestamp\" >= TIMESTAMP '{s}' AND \"timestamp\" < TIMESTAMP '{e}'"),
                        // Bare-string timestamp bounds: delta-rs can't stringify
                        // typed TIMESTAMP/CAST literals into the commit's predicate
                        // ("Unable to convert expression to string"), so this is the
                        // only commit-able form. Trade-off: when a concurrent commit
                        // lands mid-write, the OCC checker re-evaluates this with
                        // delta-kernel's engine and errors on the string→timestamp
                        // coercion ("arrow_cast should have been simplified") — the
                        // widened is_conflict below turns that into a rebase+retry,
                        // and a rebased attempt with no newer commits skips the
                        // checker entirely.
                        format!("project_id = '{safe_pid}' AND date = '{date_str}' AND timestamp >= '{s}' AND timestamp < '{e}'"),
                    ))
                })
                .collect()
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
            if dup_rows <= 0 {
                Vec::new()
            } else {
                vec![(filter.clone(), format!("project_id = '{safe_pid}' AND date = '{date_str}'"))]
            }
        };
        if chunks.is_empty() {
            return Ok(0);
        }

        let mut total_dropped = 0u64;
        for (chunk_filter, predicate) in chunks {
            let select = format!("SELECT * FROM {scan_name} WHERE {chunk_filter}");
            let batches = ctx.sql(&select).await?.collect().await?;
            let before: usize = batches.iter().map(|b| b.num_rows()).sum();
            if before == 0 {
                continue;
            }
            let deduped = crate::mem_buffer::dedup_batches(batches, &schema.dedup_keys)?;
            let after: usize = deduped.iter().map(|b| b.num_rows()).sum();
            if before == after {
                continue;
            }
            let dropped = (before - after) as u64;

            // Variant struct columns may still be BinaryView if the partition mixes
            // tiers — cast to Binary so the delta-kernel write accepts the schema.
            let deduped: Vec<RecordBatch> = deduped.into_iter().map(cast_variant_columns_to_binary).collect::<DFResult<Vec<_>>>()?;
            // Keep the rewritten chunk sorted (same as the flush path); declare
            // the footer only when the sort actually succeeded (per chunk, since
            // a schema-evolved chunk falls back to unsorted).
            let (deduped, sorted) = sort_batches_by_schema(schema, deduped);
            let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted);

            // OCC retry — same shape as optimize_table_light_inner. Best-effort;
            // exhaustion bubbles up as Err (the caller in `dedup_today_partitions`
            // logs and continues), so the partition only retries when the next
            // scheduler tick sees a file-count change.
            const MAX_RETRIES: usize = 4;
            let mut last_err: Option<deltalake::DeltaTableError> = None;
            let mut committed = false;
            for attempt in 0..MAX_RETRIES {
                // Refresh UNDER the lock before cloning, exactly like the flush
                // staged-commit path. Holding the lock alone is NOT enough: a
                // prior committer advances the Delta log but our in-memory
                // `table_ref` handle lags it, so building the replace_where on
                // the stale handle commits a base_version behind latest and the
                // OCC checker rebases — re-running the full file-matching scan
                // every attempt (the conflicts_checked>0 retry storm seen in
                // prod). Refreshing here makes base_version == latest, so the
                // commit lands first-try and the bare-string predicate's OCC
                // checker never runs. Refresh is probe-cheap (404-short-circuit
                // when already current).
                let commit_guard = self.delta_commit_lock.lock().await;
                if let Err(e) = refresh_table_snapshot(table_ref, self.config.maintenance.timefusion_incremental_snapshot).await {
                    debug!("dedup pre-commit refresh failed (attempt {}): {}", attempt + 1, e);
                }
                let table_clone = {
                    let table = table_ref.read().await;
                    table.clone()
                };
                let pre_uris: std::collections::HashSet<String> = table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default();
                // Apply this replace_where's Add+Remove to the materialized
                // snapshot incrementally (drop tombstoned paths, append new)
                // instead of the post-commit hook re-materializing all active
                // files — the 2-8s/commit full scan on the 26k-file unified
                // table that the dedup sweep otherwise pays under the commit lock.
                let result = table_clone
                    .write(deduped.clone())
                    .with_partition_columns(schema.partitions.clone())
                    .with_writer_properties(writer_properties.clone())
                    .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
                    .with_replace_where(predicate.clone())
                    .with_commit_properties(incremental_commit_properties(self.config.maintenance.timefusion_incremental_snapshot))
                    .await;
                match result {
                    Ok(new_table) => {
                        // Release the commit lock BEFORE post-commit work, matching
                        // the flush staged-commit path. The commit has landed; the
                        // version-guarded swap + cache refresh no longer need the
                        // lock, and holding it across get_file_uris (26k paths),
                        // persist_snapshot, and the table write-lock would serialize
                        // every concurrent flush append behind this window.
                        drop(commit_guard);
                        // Swap + warm-added/evict-removed exactly like the optimize
                        // paths; a bare swap left the replace_where outputs stone-cold
                        // (1.5 s first read observed against OVH) and the tombstoned
                        // files' cache entries alive until TTL.
                        self.swap_and_refresh_cache(table_ref, new_table, &pre_uris).await;
                        crate::metrics::record_compaction_dedup_dropped(dropped);
                        info!(
                            "dedup compaction: table={} project={} date={} chunk=[{}] dropped={} (before={} after={})",
                            table_name, project_id, date_str, predicate, dropped, before, after
                        );
                        total_dropped += dropped;
                        committed = true;
                        break;
                    }
                    Err(e) => {
                        // Drop BEFORE the backoff sleep below — do not remove.
                        // Holding the commit lock across the sleep would block
                        // every concurrent append behind this dedup retry.
                        drop(commit_guard);
                        let msg = e.to_string();
                        // "Transaction failed" covers the conflict checker erroring
                        // while evaluating our predicate against a concurrent commit
                        // (seen in prod as "Failed to commit transaction: Error
                        // evaluating predicate") — rebasing on retry resolves it.
                        let is_conflict = is_occ_conflict_err(&msg);
                        if !is_conflict || attempt + 1 == MAX_RETRIES {
                            return Err(anyhow::anyhow!("dedup_partition write failed: {}", e));
                        }
                        debug!(
                            "dedup_partition OCC conflict (attempt {}/{}): table={} project={} date={}",
                            attempt + 1,
                            MAX_RETRIES,
                            table_name,
                            project_id,
                            date_str
                        );
                        tokio::time::sleep(tokio::time::Duration::from_millis(150u64 << attempt)).await;
                        last_err = Some(e);
                    }
                }
            }
            if !committed {
                return Err(anyhow::anyhow!(
                    "dedup_partition exhausted retries: {}",
                    last_err.map(|e| e.to_string()).unwrap_or_default()
                ));
            }
        }
        Ok(total_dropped)
    }

    /// Sweep every `(project_id, today)` partition in this table via
    /// `dedup_partition`. Skips when Delta version is unchanged since the
    /// last sweep. Best-effort: per-partition errors are logged.
    pub async fn dedup_today_partitions(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, dedup_key: &str) -> Result<()> {
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        if schema.dedup_keys.is_empty() {
            return Ok(());
        }
        let today = Utc::now().date_naive();
        let date_marker = format!("date={}", today);

        let (pre_version, project_ids) = {
            let table = table_ref.read().await;
            let v = table.version().unwrap_or(0);
            let pids: std::collections::HashSet<String> = table
                .get_file_uris()?
                .filter(|u| u.contains(&date_marker))
                .filter_map(|uri| uri.split('/').find_map(|seg| seg.strip_prefix("project_id=").map(str::to_string)))
                .collect();
            (v, pids)
        };
        if self.last_dedup_versions.read().await.get(dedup_key).copied() == Some(pre_version) {
            debug!("dedup sweep: table={} version={} unchanged — skipping", table_name, pre_version);
            return Ok(());
        }
        // Custom-project tables don't embed project_id in the path; sweep "default".
        let project_ids = if project_ids.is_empty() { std::iter::once("default".to_string()).collect() } else { project_ids };

        let mut total_dropped = 0u64;
        let mut any_ok = false;
        for pid in &project_ids {
            match self.dedup_partition(table_ref, table_name, pid, today).await {
                Ok(d) => {
                    total_dropped += d;
                    any_ok = true;
                }
                Err(e) => warn!("dedup sweep: project={} date={} table={} failed: {}", pid, today, table_name, e),
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
            info!(
                "dedup sweep: table={} key={} projects={} total_dropped={}",
                table_name,
                dedup_key,
                project_ids.len(),
                total_dropped
            );
        }
        Ok(())
    }

    pub async fn optimize_table_light(&self, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str) -> Result<()> {
        let start_time = std::time::Instant::now();
        let today = Utc::now().date_naive();
        let partition_filters = vec![PartitionFilter::try_from(("date", "=", today.to_string().as_str()))?];
        let target_size = self.config.maintenance.timefusion_light_optimize_target_size;
        let schema = get_schema(table_name).unwrap_or_else(get_default_schema);
        // declare_sorted=false: light optimize Compacts (concatenates) files without re-sorting.
        let writer_properties = self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, false);

        // Best-effort optimize: retry on OCC conflict but DO NOT hold the
        // flush lock. Earlier we wrapped this in `with_flush_paused` to
        // ensure optimize won the race against flush commits, but the
        // retry+OCC time is 4–10s and flushes accumulate buckets during
        // that window — at 25h-bench scale we saw 46+ stuck MemBuffer
        // buckets and a 10× drop in ingest throughput. Better to let
        // optimize fail loudly during heavy ingest; the next scheduler
        // tick (5 min later) usually catches a quiet enough window.
        self.optimize_table_light_inner(table_ref, today, &partition_filters, target_size, &writer_properties, start_time).await
    }

    /// Inner optimize loop. Caller is expected to hold the flush lock when
    /// a `BufferedWriteLayer` is active; the retry loop here remains as a
    /// safety net against bursts from `flush_all_now` or shutdown flushes.
    async fn optimize_table_light_inner(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, today: chrono::NaiveDate, partition_filters: &[PartitionFilter], target_size: i64,
        writer_properties: &WriterProperties, start_time: std::time::Instant,
    ) -> Result<()> {
        const MAX_RETRIES: usize = 4;
        let mut last_err: Option<deltalake::DeltaTableError> = None;
        for attempt in 0..MAX_RETRIES {
            let table_clone = {
                let table = table_ref.read().await;
                table.clone()
            };
            // Pre-state file set for deriving the files this optimize adds (to
            // warm) and removes (to evict).
            let track_files = self.config.maintenance.timefusion_warm_after_compaction || self.config.maintenance.timefusion_evict_after_compaction;
            let pre_uris: std::collections::HashSet<String> = if track_files {
                table_clone.get_file_uris().map(|it| it.collect()).unwrap_or_default()
            } else {
                Default::default()
            };
            if attempt == 0 {
                info!("Light optimizing files from date: {}", today);
            } else {
                debug!("Light optimize retry {}/{} after OCC conflict", attempt + 1, MAX_RETRIES);
            }
            let optimize_result = table_clone
                .optimize()
                .with_filters(partition_filters)
                .with_type(deltalake::operations::optimize::OptimizeType::Compact)
                .with_target_size(std::num::NonZero::new(target_size as u64).unwrap_or(std::num::NonZero::new(1).unwrap()))
                .with_max_concurrent_tasks(self.config.maintenance.timefusion_optimize_max_concurrent_tasks)
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
                .with_session_state(Arc::new(build_optimize_session_state(self.config.memory.timefusion_query_partitions)))
                .await;
            match optimize_result {
                Ok((new_table, metrics)) => {
                    let min_files = self.config.maintenance.timefusion_compact_min_files;
                    if metrics.total_considered_files < min_files {
                        debug!(
                            "Skipping light optimization commit: {} files < min threshold {}",
                            metrics.total_considered_files, min_files
                        );
                        return Ok(());
                    }
                    let duration = start_time.elapsed();
                    info!(
                        "Light optimization completed in {:?} (attempt {}): {} files removed, {} files added",
                        duration,
                        attempt + 1,
                        metrics.num_files_removed,
                        metrics.num_files_added
                    );
                    // Swap the optimized table in and refresh the cache (warm
                    // freshly-compacted files, evict the small files just
                    // tombstoned) via the shared helper.
                    self.swap_and_refresh_cache(table_ref, new_table, &pre_uris).await;
                    return Ok(());
                }
                Err(e) => {
                    let msg = e.to_string();
                    let is_conflict = is_occ_conflict_err(&msg);
                    // "Found unmasked nulls for non-nullable StructArray" surfaces
                    // when delta-rs is mid-rewrite and the in-flight Add log lines
                    // for partition struct values aren't fully populated yet.
                    // It usually clears on a fresh re-scan, so treat as transient.
                    let is_transient_schema = msg.contains("Found unmasked nulls");
                    if (is_conflict || is_transient_schema) && attempt + 1 < MAX_RETRIES {
                        // Quick backoff scaled so we straddle multiple flush
                        // ticks (~2s each) — picks 150, 300, 600 ms.
                        let backoff_ms = 150u64 << attempt;
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        last_err = Some(e);
                        continue;
                    }
                    error!("Light optimization operation failed (attempt {}): {}", attempt + 1, e);
                    return Err(anyhow::anyhow!("Light table optimization failed: {}", e));
                }
            }
        }
        let err = last_err.map(|e| e.to_string()).unwrap_or_else(|| "exhausted retries".into());
        warn!("Light optimization gave up after {} OCC conflicts; will retry next tick: {}", MAX_RETRIES, err);
        Ok(())
    }

    /// Vacuum the Delta table to clean up old files that are no longer needed
    /// This reduces storage costs and improves query performance
    async fn vacuum_table(&self, table_ref: &Arc<RwLock<DeltaTable>>, retention_hours: u64) {
        // Log the start of the vacuum operation
        let start_time = std::time::Instant::now();
        info!("Starting vacuum operation with retention period of {} hours", retention_hours);

        // Get a clone of the table to avoid holding the lock during the operation
        let table_clone = {
            let table = table_ref.read().await;
            table.clone()
        };

        // Directly run vacuum without dry run to delete old files
        match table_clone
            .vacuum()
            .with_retention_period(chrono::Duration::hours(retention_hours as i64))
            .with_enforce_retention_duration(false) // Allow deletion of files newer than default retention
            // Full: also sweep files whose tombstones already left the
            // checkpoint (pruned at deletedFileRetentionDuration) — Lite can
            // no longer see those and they'd leak storage forever.
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
            }
            Err(e) => error!("Vacuum operation failed: {}", e),
        }
    }

    /// Get table statistics using the statistics extractor
    pub async fn get_table_statistics(&self, table: &DeltaTable, project_id: &str, table_name: &str) -> Result<Statistics> {
        // Get the schema for this table
        let schema_def = get_schema(table_name).unwrap_or_else(get_default_schema);
        let schema = schema_def.schema_ref();
        self.statistics_extractor.extract_statistics(table, project_id, table_name, &schema).await
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
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down TimeFusion database...");

        // Cancel maintenance tasks
        self.maintenance_shutdown.cancel();

        // Shutdown batch queue if present
        if let Some(ref queue) = self.batch_queue {
            info!("Flushing batch queue...");
            queue.shutdown().await;
        }

        // Log final cache stats and shutdown cache
        if let Some(ref cache) = self.object_store_cache {
            info!("Shutting down Foyer cache...");
            cache.log_stats().await;
            cache.shutdown().await?;
        }

        // Close PostgreSQL connection pool if present
        if let Some(ref pool) = self.config_pool {
            pool.close().await;
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
fn sort_batches_by_schema(schema: &crate::schema_loader::TableSchema, batches: Vec<RecordBatch>) -> (Vec<RecordBatch>, bool) {
    use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch};
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return (batches, false);
    }
    let arrow_schema = batches[0].schema();
    // Only sort a schema-homogeneous bucket. mem_buffer's `schemas_compatible`
    // admits batches with extra nullable fields into one bucket; concatenating
    // those would either abort the flush or silently drop the evolved column.
    // Write such a bucket unsorted (sorted=false) and let the writer's
    // SchemaMode::Merge union the columns losslessly, matching the old path.
    if batches.iter().any(|b| b.schema() != arrow_schema) {
        return (batches, false);
    }
    let sort_idx: Vec<(usize, &crate::schema_loader::SortingColumnDef)> =
        schema.sorting_columns.iter().filter_map(|sc| arrow_schema.index_of(&sc.name).ok().map(|i| (i, sc))).collect();
    if sort_idx.is_empty() {
        return (batches, false);
    }
    let combined = if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        match concat_batches(&arrow_schema, &batches) {
            Ok(c) => c,
            Err(e) => {
                warn!("sort_batches_by_schema: concat failed, writing unsorted: {e}");
                return (batches, false);
            }
        }
    };
    let sort_cols: Vec<SortColumn> = sort_idx
        .iter()
        .map(|(i, sc)| SortColumn {
            values:  combined.column(*i).clone(),
            options: Some(SortOptions {
                descending:  sc.descending,
                nulls_first: sc.nulls_first,
            }),
        })
        .collect();
    let indices = match lexsort_to_indices(&sort_cols, None) {
        Ok(i) => i,
        Err(e) => {
            warn!("sort_batches_by_schema: lexsort failed, writing unsorted: {e}");
            return (vec![combined], false);
        }
    };
    // Already ordered (common: append-ordered, ~monotonic timestamp) → skip the take copy.
    if indices.values().iter().enumerate().all(|(i, &v)| v as usize == i) {
        return (vec![combined], true);
    }
    match take_record_batch(&combined, &indices) {
        Ok(sorted) => (vec![sorted], true),
        Err(e) => {
            warn!("sort_batches_by_schema: take failed, writing unsorted: {e}");
            (vec![combined], false)
        }
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
/// any reader that trusts it (see docs/plans/2026-06-17-parquet-ordering-pushdown.md).
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
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(zstd_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap()),
        ))
        .set_max_row_group_row_count(Some(max_row_group_size))
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

    for field in &schema.fields {
        let dt = field.data_type.as_str();
        let col = ColumnPath::from(field.name.as_str());
        let is_sort_key = sort_key_names.contains(field.name.as_str());

        // Page-level stats only where they prune AND are cheap: the declared
        // sort keys, plus any timestamp/date column (8-byte min/max, common
        // range predicates like observed_timestamp/start_time/end_time). Wide
        // JSON/variant/string columns stay at the Chunk default so the
        // ColumnIndex doesn't balloon.
        if is_sort_key || dt.starts_with("Timestamp") || dt == "Date32" {
            builder = builder.set_column_statistics_enabled(col.clone(), EnabledStatistics::Page);
        }

        if dt.starts_with("Timestamp") || dt == "Date32" {
            builder = builder
                .set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED)
                .set_column_dictionary_enabled(col.clone(), false);
        } else if matches!(dt, "Int32" | "Int64" | "UInt32" | "UInt64") {
            builder = builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED);
        } else if dt == "Utf8" && is_sort_key {
            builder = builder.set_column_encoding(col.clone(), Encoding::DELTA_BYTE_ARRAY).set_column_dictionary_enabled(col.clone(), false);
        }

        // Explicit per-column dict opt-out (overrides defaults above only
        // when set to Some(false); Some(true)/None leaves defaults intact).
        if field.dictionary == Some(false) {
            builder = builder.set_column_dictionary_enabled(col.clone(), false);
        }

        if field.bloom_filter && !bloom_globally_disabled {
            builder = builder
                .set_column_bloom_filter_enabled(col.clone(), true)
                .set_column_bloom_filter_ndv(col.clone(), BLOOM_NDV)
                .set_column_bloom_filter_fpp(col, 0.01);
        }
    }

    builder.build()
}

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database:        Arc<Database>,
    schema:          SchemaRef,
    _batch_queue:    Option<Arc<crate::batch_queue::BatchQueue>>,
    table_name:      String,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::batch_queue::BatchQueue>>, table_name: String,
    ) -> Self {
        Self {
            default_project,
            database,
            schema,
            _batch_queue: batch_queue,
            table_name,
        }
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
        let mut writes = Vec::new();
        for (project_id, sub) in partition_batch_by_project(batch, &self.default_project)? {
            let converted = convert_variant_columns(sub, &target_schema)?;
            writes.push(async move {
                self.database
                    .insert_records_batch(&project_id, &self.table_name, vec![converted], false, None)
                    .await
                    .map_err(|e| DataFusionError::Execution(format!("fast_insert_batch for project {} table {}: {}", project_id, self.table_name, e)))
            });
        }
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
            Expr::BinaryExpr(BinaryExpr {
                left,
                op: Operator::And,
                right,
            }) => Self::is_exact_pushdown_filter(left) && Self::is_exact_pushdown_filter(right),
            // Simple column comparisons are exact
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let is_column_literal = matches!(
                    (left.as_ref(), right.as_ref()),
                    (Expr::Column(_), Expr::Literal(_, _)) | (Expr::Literal(_, _), Expr::Column(_))
                );

                let is_supported_op = matches!(
                    op,
                    Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                );

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
        let time_column = crate::schema_loader::get_schema(&self.table_name)
            .map(|s| s.time_column_name().to_string())
            .unwrap_or_else(|| "timestamp".to_string());

        let mut optimized_filters = Vec::new();
        let mut has_date_filter = false;

        // First, check if we already have a date filter to avoid duplicates
        for filter in filters {
            if Self::is_date_filter(filter) {
                has_date_filter = true;
            }
            optimized_filters.push(filter.clone());
        }

        // Only add date filters if we don't already have one
        if !has_date_filter {
            for filter in filters {
                // Check if this is a timestamp filter that needs a date filter added
                if let Some(date_filter) = time_range_partition_pruner::timestamp_to_date_filter(filter, &time_column) {
                    optimized_filters.push(date_filter);
                    debug!("Added date partition filter for {} on column {}", self.table_name, time_column);
                }
            }
        }

        // Check if project_id filter is present
        if !self.has_project_id_in_filters(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }

        Ok(optimized_filters)
    }

    /// Check if an expression is a date filter
    fn is_date_filter(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, .. }) => {
                matches!(left.as_ref(), Expr::Column(col) if col.name == "date")
            }
            _ => false,
        }
    }

    /// Check if filters contain a project_id filter
    fn has_project_id_in_filters(&self, filters: &[Expr]) -> bool {
        use crate::optimizers::ProjectIdPushdown;
        ProjectIdPushdown::has_project_id_filter(filters)
    }

    /// Create a MemorySourceConfig-based execution plan with multiple partitions
    fn create_memory_exec(&self, partitions: &[Vec<RecordBatch>], projection: Option<&Vec<usize>>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mem_source =
            MemorySourceConfig::try_new(partitions, self.schema.clone(), projection.cloned()).map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(DataSourceExec::new(Arc::new(mem_source))))
    }

    /// Scan a Delta table and coerce output schema to match our expected types.
    /// Handles object store registration, projection translation, and type coercion (e.g., Utf8 -> Utf8View).
    async fn scan_delta_table(
        &self, table: &DeltaTable, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Extract project_id from filters for the provider cache key.
        // Falls back to table_name-only key if absent (multi-project queries).
        let project_id = self.extract_project_id_from_filters(filters).unwrap_or_else(|| self.default_project.clone());
        let cache_key = (project_id, self.table_name.clone());

        table.update_datafusion_session(state).map_err(|e| DataFusionError::External(Box::new(e)))?;

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
        let read_hit = self
            .database
            .delta_provider_cache
            .get(&cache_key)
            .filter(|e| e.value().0 == current_version)
            .map(|e| Arc::clone(&e.value().1));
        let (cell, was_fresh_cell, brand_new_entry) = if let Some(c) = read_hit {
            (c, false, false)
        } else {
            // Miss / stale — take the write path. Re-check after acquiring
            // the entry lock since another thread may have populated it
            // between our get() and entry() (DashMap doesn't upgrade locks).
            let entry = self.database.delta_provider_cache.entry(cache_key.clone());
            let brand_new = matches!(entry, dashmap::Entry::Vacant(_));
            let mut e = entry.or_insert_with(|| (current_version, Arc::new(tokio::sync::OnceCell::new())));
            let stale = e.0 != current_version;
            if stale {
                *e = (current_version, Arc::new(tokio::sync::OnceCell::new()));
            }
            // "Hit" = the cell was already initialised at this version when
            // we found it. We approximate this by checking initialised state
            // BEFORE we touch get_or_try_init; close enough for an alerting
            // metric. Miss covers both "never seen" and "stale-replaced".
            (Arc::clone(&e.1), stale, brand_new)
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
                    provider_cache_entries = size,
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
                let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
                // Build the delta-rs table provider with our session so its scan
                // inherits `schema_force_view_types=false` (set in
                // `create_session_context`). delta-rs's default is `true` (BinaryView),
                // which mismatches our Binary-typed MemBuffer at the union and
                // panics in physical planning.
                if let Some(ss) = session_state {
                    table.table_provider().with_session(Arc::new(ss)).await
                } else {
                    table.table_provider().await
                }
                .map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .await?
            .clone();
        // Abandoned-build detection: if the DashMap entry for this key now
        // points to a different cell than the one we built into, a version
        // bump replaced our cell mid-build and our work is wasted. Non-zero
        // counts here under sustained traffic flag pathological version
        // churn (very frequent compaction, racy update_state).
        if let Some(current_entry) = self.database.delta_provider_cache.get(&cache_key)
            && !Arc::ptr_eq(&current_entry.value().1, &cell)
        {
            self.database.scan_metrics.provider_build_abandoned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Translate projection indices from our schema to delta table's schema.
        // DataFusion passes indices based on ProjectRoutingTable.schema, but the
        // delta table provider expects indices based on its own schema.
        let delta_schema = provider.schema();
        let translated_projection = projection.map(|proj| {
            let mut translated = Vec::with_capacity(proj.len());
            for &idx in proj {
                let col_name = self.schema.field(idx).name();
                if let Some(delta_idx) = delta_schema.fields().iter().position(|f| f.name() == col_name) {
                    translated.push(delta_idx);
                } else {
                    warn!(
                        "Column '{}' requested in projection but not found in Delta schema for table '{}'",
                        col_name, self.table_name
                    );
                }
            }
            translated
        });

        let delta_plan = provider.scan(state, translated_projection.as_ref(), filters, limit).await?;

        // Determine target schema based on projection
        let target_schema = match projection {
            Some(proj) => Arc::new(arrow_schema::Schema::new(
                proj.iter().map(|&idx| self.schema.field(idx).clone()).collect::<Vec<_>>(),
            )),
            None => self.schema.clone(),
        };

        Self::coerce_plan_to_schema(delta_plan, &target_schema)
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

        let needs_coercion = plan_schema
            .fields()
            .iter()
            .zip(target_schema.fields())
            .any(|(plan_field, target_field)| differs(plan_field, target_field));

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
                let expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> = if differs(plan_field, target_field) {
                    Arc::new(CastExpr::new(col_expr, target_field.data_type().clone(), None))
                } else {
                    col_expr
                };
                (expr, target_field.name().clone())
            })
            .collect();

        Ok(Arc::new(ProjectionExec::try_new(cast_exprs, plan)?))
    }

    /// Helper to scan Delta only (when no MemBuffer data)
    async fn scan_delta_only(
        &self, state: &dyn Session, project_id: &str, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let delta_table = self.database.resolve_table(project_id, &self.table_name).await?;
        let table = delta_table.read().await;
        self.scan_delta_table(&table, state, projection, filters, limit).await
    }

    /// Extract time range (min, max) from query filters.
    /// Returns None if no time constraints found.
    fn extract_time_range_from_filters(&self, filters: &[Expr]) -> Option<(i64, i64)> {
        let mut min_ts: Option<i64> = None;
        let mut max_ts: Option<i64> = None;

        for filter in filters {
            if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter {
                // Check if left side is timestamp column
                let is_timestamp_col = matches!(left.as_ref(), Expr::Column(c) if c.name == "timestamp");
                if !is_timestamp_col {
                    continue;
                }

                // Extract timestamp value from right side
                let ts_value = match right.as_ref() {
                    Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts), _), _) => Some(*ts),
                    Expr::Literal(ScalarValue::TimestampNanosecond(Some(ts), _), _) => Some(*ts / 1000),
                    Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts), _), _) => Some(*ts * 1000),
                    Expr::Literal(ScalarValue::TimestampSecond(Some(ts), _), _) => Some(*ts * 1_000_000),
                    _ => None,
                };

                if let Some(ts) = ts_value {
                    match op {
                        Operator::Gt | Operator::GtEq => {
                            min_ts = Some(min_ts.map_or(ts, |m| m.max(ts)));
                        }
                        Operator::Lt | Operator::LtEq => {
                            max_ts = Some(max_ts.map_or(ts, |m| m.min(ts)));
                        }
                        Operator::Eq => {
                            min_ts = Some(ts);
                            max_ts = Some(ts);
                        }
                        _ => {}
                    }
                }
            }
        }

        match (min_ts, max_ts) {
            (Some(min), Some(max)) => Some((min, max)),
            (Some(min), None) => Some((min, i64::MAX)),
            (None, Some(max)) => Some((i64::MIN, max)),
            (None, None) => None,
        }
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
            debug!(
                "write_all: inserting {} batches with {} total rows for project {}",
                batches.len(),
                row_count,
                project_id
            );
            let insert_span = tracing::trace_span!(parent: &span, "delta_table.insert", project_id = %project_id, rows = row_count);
            async move {
                self.database
                    .insert_records_batch(&project_id, &self.table_name, batches, false, None)
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
            .map(|s| {
                s.schema_ref()
                    .fields()
                    .iter()
                    .filter(|f| crate::schema_loader::is_variant_type(f.data_type()))
                    .map(|f| f.name().clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(filter
            .iter()
            .map(|f| {
                if !variant_cols.is_empty() && f.column_refs().iter().any(|c| variant_cols.contains(&c.name)) {
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
        )
    )]
    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        let span = tracing::Span::current();
        let scan_start = std::time::Instant::now();
        let scan_metrics = self.database.scan_metrics.clone();

        // Apply our custom optimizations to the filters
        let optimized_filters = self.apply_time_series_optimizations(filters)?;

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
        let text_match_preds = crate::tantivy_index::udf::collect_text_matches(&optimized_filters);
        let mut tantivy_id_filter: Option<Expr> = None;
        if !text_match_preds.is_empty()
            && let Some(svc) = self.database.tantivy_search()
        {
            use datafusion::logical_expr::{Expr, lit};
            let tcfg = &self.database.config().tantivy;
            let max_hits = tcfg.prefilter_max_hits();
            let min_sel_pct = tcfg.prefilter_min_selectivity_pct() as u64;
            crate::metrics::record_tantivy_prefilter_attempt();

            let mut delta_ids: Option<std::collections::HashSet<String>> = None;
            let mut delta_indexed_rows: u64 = 0;
            let mut delta_any_usable = false;
            let mut abort_reason: Option<&'static str> = None;
            for p in &text_match_preds {
                match svc.search_with_stats(&self.table_name, &project_id, &p.column, &p.query, max_hits).await {
                    Ok(Some(result)) => {
                        delta_any_usable = true;
                        delta_indexed_rows = delta_indexed_rows.saturating_add(result.indexed_rows);
                        let ids: std::collections::HashSet<String> = result.hits.into_iter().map(|h| h.id).collect();
                        // Intersect: this is sound only because predicates are AND-ed.
                        // `collect_text_matches` skips OR subtrees so disjunctive terms
                        // never reach here (else x_ids ∩ y_ids = ∅ would drop everything).
                        delta_ids = Some(match delta_ids.take() {
                            None => ids,
                            Some(prev) => prev.intersection(&ids).cloned().collect(),
                        });
                    }
                    Ok(None) => {
                        abort_reason = Some("delta_no_index_or_cap_exceeded");
                        delta_any_usable = false;
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "tantivy search failed for {}/{}: {} — falling back to full scan",
                            project_id, self.table_name, e
                        );
                        crate::metrics::record_tantivy_prefilter_error();
                        abort_reason = Some("delta_error");
                        delta_any_usable = false;
                        break;
                    }
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
                    } else {
                        crate::metrics::record_tantivy_prefilter_used();
                        tantivy_id_filter = Some(Expr::InList(datafusion::logical_expr::expr::InList {
                            expr:    Box::new(datafusion::logical_expr::col("id")),
                            list:    ids.into_iter().map(lit).collect(),
                            negated: false,
                        }));
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
        let scan_state = parking_lot::Mutex::new(ScanShape::default());
        let wrap_result = |plan: Arc<dyn ExecutionPlan>| -> DFResult<Arc<dyn ExecutionPlan>> {
            let shape = *scan_state.lock();
            let us = scan_start.elapsed().as_micros() as u64;
            scan_metrics.record_scan(us, shape.skipped_delta, shape.has_mem, shape.has_delta, shape.fast_resolve_hit);
            Ok(plan)
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
            let plan = self.scan_delta_only(state, &project_id, projection, &delta_only_filters, limit).await?;
            return wrap_result(plan);
        };

        span.record("scan.uses_mem_buffer", true);

        // Extract query time range from filters
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);

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
        let mem_partitions = match layer.query_partitioned_with_text_match(&project_id, &self.table_name, &optimized_filters, &text_match_preds) {
            Ok(partitions) => partitions,
            Err(e) => {
                warn!("Failed to query mem buffer: {}", e);
                vec![]
            }
        };

        // If no mem buffer data, query Delta only
        debug!("MemBuffer partitions count: {} for {}/{}", mem_partitions.len(), project_id, self.table_name);
        if mem_partitions.is_empty() {
            debug!("No MemBuffer data, querying Delta only for {}/{}", project_id, self.table_name);
            let mut delta_only_filters = optimized_filters.clone();
            if let Some(f) = tantivy_id_filter.clone() {
                delta_only_filters.push(f);
            }
            tag_shape(&|s| s.has_delta = true);
            let plan = self.scan_delta_only(state, &project_id, projection, &delta_only_filters, limit).await?;
            return wrap_result(plan);
        }

        // Create MemorySourceConfig with multiple partitions for parallel execution
        let mem_plan = self.create_memory_exec(&mem_partitions, projection)?;
        tag_shape(&|s| s.has_mem = true);

        // If we can skip Delta, return mem plan directly
        if skip_delta {
            span.record("scan.skipped_delta", true);
            debug!(
                "Skipping Delta scan - query time range entirely within MemBuffer for {}/{}",
                project_id, self.table_name
            );
            return wrap_result(mem_plan);
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
        let mem_ranges = layer.get_bucket_ranges(&project_id, &self.table_name);
        let mut delta_filters = optimized_filters.clone();
        let ts_col = || Box::new(col("timestamp"));
        let ts_lit = |t: i64| Box::new(lit(ScalarValue::TimestampMicrosecond(Some(t), Some("UTC".into()))));
        for (start, end) in &mem_ranges {
            // NOT (ts >= start AND ts < end)  ≡  (ts < start) OR (ts >= end)
            let below = Expr::BinaryExpr(BinaryExpr {
                left:  ts_col(),
                op:    Operator::Lt,
                right: ts_lit(*start),
            });
            let at_or_above = Expr::BinaryExpr(BinaryExpr {
                left:  ts_col(),
                op:    Operator::GtEq,
                right: ts_lit(*end),
            });
            delta_filters.push(Expr::BinaryExpr(BinaryExpr {
                left:  Box::new(below),
                op:    Operator::Or,
                right: Box::new(at_or_above),
            }));
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
        let delta_plan = self.scan_delta_table(&table, state, projection, &delta_filters, limit).await?;
        tag_shape(&|s| {
            s.has_mem = true;
            s.has_delta = true;
        });

        // Union both plans (mem data first for recency, then Delta for historical)
        wrap_result(UnionExec::try_new(vec![mem_plan, delta_plan])?)
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        // Cancel maintenance tasks immediately
        self.maintenance_shutdown.cancel();

        // Note: We can't do async cleanup in Drop, but cancelling the token
        // will cause background tasks to stop, preventing the panic
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
        FieldDef {
            name:         name.into(),
            data_type:    dt.into(),
            nullable:     true,
            tantivy:      None,
            dictionary:   None,
            bloom_filter: false,
        }
    }

    fn schema_with(fields: Vec<FieldDef>, sort: Vec<&str>) -> TableSchema {
        TableSchema {
            table_name: "t".into(),
            partitions: vec![],
            sorting_columns: sort
                .into_iter()
                .map(|n| SortingColumnDef {
                    name:        n.into(),
                    descending:  false,
                    nulls_first: false,
                })
                .collect(),
            z_order_columns: vec![],
            fields,
            time_column: None,
            dedup_keys: vec![],
        }
    }

    #[test]
    fn compression_level_drives_zstd() {
        for level in [3, 9, 15, 19] {
            let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), level, true);
            assert_eq!(
                p.compression(&ColumnPath::from("anything")),
                Compression::ZSTD(ZstdLevel::try_new(level).unwrap())
            );
        }
    }

    #[test]
    fn invalid_zstd_level_falls_back() {
        let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), 999, true);
        assert_eq!(
            p.compression(&ColumnPath::from("x")),
            Compression::ZSTD(ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap())
        );
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
        let p = build_writer_properties(
            &cfg(),
            &schema_with(vec![field("ts", "Timestamp(Nanosecond, None)"), field("n", "Int64")], vec![]),
            3,
            true,
        );
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
            &schema_with(
                vec![field("timestamp", "Timestamp(Microsecond, None)"), field("body", "Utf8")],
                vec!["timestamp"],
            ),
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
        let s = schema_with(
            vec![field("timestamp", "Timestamp(Microsecond, None)"), field("id", "Utf8")],
            vec!["timestamp", "id"],
        );
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

    // Fix #4: batches are globally sorted by the declared lead key before write.
    #[test]
    fn sort_batches_orders_by_declared_keys() {
        use arrow::array::{Array, Int64Array};
        use arrow_schema::{DataType, Field, Schema};
        let s = std::sync::Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]));
        let b1 = RecordBatch::try_new(s.clone(), vec![std::sync::Arc::new(Int64Array::from(vec![3, 1]))]).unwrap();
        let b2 = RecordBatch::try_new(s.clone(), vec![std::sync::Arc::new(Int64Array::from(vec![2, 0]))]).unwrap();
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), vec![b1, b2]);
        assert!(sorted);
        assert_eq!(out.len(), 1);
        let col = out[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(col.values(), &[0, 1, 2, 3]);
        // No declared sort columns → input returned untouched, sorted=false.
        let (passthrough, sorted) = sort_batches_by_schema(&schema_with(vec![], vec![]), vec![out[0].clone(), out[0].clone()]);
        assert!(!sorted);
        assert_eq!(passthrough.len(), 2);
    }

    // Regression for the review's headline finding: a bucket whose batches have
    // evolved schemas (an extra nullable column on the 2nd batch, which
    // mem_buffer's schemas_compatible admits) must NOT abort — concat fails, so
    // we write unsorted and report sorted=false (footer stays honest).
    #[test]
    fn sort_batches_tolerates_schema_evolution() {
        use arrow::array::{Int64Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let s1 = std::sync::Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]));
        let s2 = std::sync::Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, true),
        ]));
        let b1 = RecordBatch::try_new(s1, vec![std::sync::Arc::new(Int64Array::from(vec![2, 1]))]).unwrap();
        let b2 = RecordBatch::try_new(
            s2,
            vec![std::sync::Arc::new(Int64Array::from(vec![3])), std::sync::Arc::new(StringArray::from(vec![Some("x")]))],
        )
        .unwrap();
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), vec![b1, b2]);
        assert!(!sorted, "mixed-schema bucket must report unsorted, not panic/abort");
        assert_eq!(out.len(), 2, "original batches returned for the writer to merge");
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serial_test::serial;

    use super::*;
    use crate::{config::AppConfig, test_utils::test_helpers::*};

    /// The shared OCC classifier must treat every retryable delta-rs conflict as
    /// retryable — including `VersionAlreadyExists` ("already exists", which can
    /// hit the dedup path under multi-replica races), `MetadataChanged`, and the
    /// predicate re-evaluation failure ("Transaction failed") — while permanent
    /// errors (protocol version, auth/IO) fail fast. Guards the dedup/optimize
    /// loops, which previously omitted some of these substrings.
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
            let schema = Arc::new(Schema::new(vec![
                Field::new("project_id", pid_col.data_type().clone(), true),
                Field::new("id", DataType::Int64, false),
            ]));
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
        let json = serialize_watermark_to_json(&wm);
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(json));
        let parsed = parse_watermark_from_json(&info, wm.len());
        assert_eq!(parsed, wm);
    }

    /// All-None watermark serializes to an empty object, which
    /// `build_watermark_commit_properties` turns into a default
    /// `CommitProperties` (no metadata written). Recovery sees no key and
    /// silently skips the commit — same path as old commits from before
    /// this feature landed.
    #[test]
    fn watermark_all_none_omits_metadata() {
        let wm: crate::buffered_write_layer::DeltaWatermark = vec![None, None, None];
        assert!(serialize_watermark_to_json(&wm).is_empty());
        let mut info = std::collections::HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(serde_json::Map::new()));
        assert!(parse_watermark_from_json(&info, 3).iter().all(|p| p.is_none()));
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

        let max = max_watermark_across_commits([&a, &b, &c, &d], 3);
        assert_eq!(max[0], Some(WalPosition { block_id: 6, offset: 0 }));
        assert_eq!(max[1], Some(WalPosition { block_id: 5, offset: 50 }));
        assert_eq!(max[2], None, "shard 2 unwritten by all commits stays None");
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
                "0": {"block_id": 1, "offset": 10},
                "99": {"block_id": 1, "offset": 999},
                "garbage": {"block_id": 1, "offset": 0},
            }),
        );
        let parsed = parse_watermark_from_json(&info, 4);
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

    /// Per-context RuntimeEnvs each granted the full memory budget, so N
    /// contexts oversubscribed the cgroup N× — the pool must be process-wide,
    /// including across `Database` clones (bootstrap clones the db).
    #[tokio::test]
    async fn session_contexts_share_one_memory_pool() -> Result<()> {
        let cfg = create_test_config("pool-share");
        let db = Database::with_config(cfg).await?;
        let ctx1 = Arc::new(db.clone()).create_session_context();
        let ctx2 = Arc::new(db.clone()).create_session_context();
        assert!(
            Arc::ptr_eq(&ctx1.runtime_env(), &ctx2.runtime_env()),
            "contexts must share one RuntimeEnv/memory pool"
        );
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
            ThrottleConfig {
                wait_get_per_call: wait,
                wait_list_per_call: wait,
                wait_list_with_delimiter_per_call: wait,
                ..Default::default()
            },
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

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int32, true),
            Field::new("p", ArrowDataType::Utf8, true),
        ]));
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
        let table = table
            .write(vec![batch(vec![10, 11], vec!["a", "a"])])
            .with_save_mode(SaveMode::Overwrite)
            .with_replace_where("p = 'a'")
            .await?;
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
        assert!(
            stale.state.as_ref().is_some_and(|s| s.has_materialized_files()),
            "stale handle must be materialized to exercise the fast path"
        );
        let log_store = stale.log_store();
        let took_fast_path = stale.state.as_mut().unwrap().advance_catchup(log_store.as_ref(), REFRESH_APPEND_CATCHUP_MAX_GAP).await?;
        assert!(
            took_fast_path,
            "advance_catchup must take the incremental path over the replace_where, not fall back to a full update"
        );
        assert_eq!(stale.version(), Some(3), "incremental catch-up reached the latest version");
        assert_eq!(
            uris(&stale),
            truth,
            "incremental advance across replace_where must equal the full re-materialize"
        );
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
        assert!(
            !table.snapshot()?.metadata().configuration().contains_key(KEY),
            "fresh table has no retention property"
        );

        let table = ensure_table_properties(table, props(24)).await;
        let config = table.snapshot()?.metadata().configuration().clone();
        assert_eq!(config.get(KEY).map(String::as_str), Some("interval 24 hours"));
        assert_eq!(config.get(CP_KEY).map(String::as_str), Some("10"), "checkpoint interval retrofitted alongside");
        assert_eq!(table.version(), Some(1), "properties set in one commit");

        let table = ensure_table_properties(table, props(24)).await;
        assert_eq!(table.version(), Some(1), "matching properties must not commit again");

        // Retention reconfiguration (e.g. env change) re-reconciles.
        let table = ensure_table_properties(table, props(48)).await;
        assert_eq!(
            table.snapshot()?.metadata().configuration().get(KEY).map(String::as_str),
            Some("interval 48 hours")
        );
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
        let throttled = ThrottledStore::new(
            mem,
            ThrottleConfig {
                wait_list_per_call: list_wait,
                wait_list_with_delimiter_per_call: list_wait,
                ..Default::default()
            },
        );
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

    /// End-to-end test of `recompress_partition`. Skip behavior is the
    /// load-bearing property: if the footer-tier probe breaks, the daily
    /// cron rewrites every partition every night. We assert via file-set
    /// comparison since the production code path itself reads the footer.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_recompress_partition_skip_idempotency() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            let (db, _ctx, prefix) = setup_test_database().await?;
            let project_id = format!("project_{}", prefix);
            let today = chrono::Utc::now().date_naive();

            let batch = json_to_batch(vec![test_span("rc1", "span1", &project_id)])?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;

            let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");

            // First recompress at tier 9 — must rewrite files.
            let files_before: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert!(!files_before.is_empty(), "expected files in today's partition");
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9).await?;
            let files_after: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_ne!(files_before, files_after, "first recompress must rewrite files");

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
        assert!(
            !db.delta_scan_can_be_skipped(&p1, t),
            "unknown projects must default to false so callers don't skip Delta"
        );
        assert!(!db.delta_scan_can_be_skipped(&p2, t), "second unknown project also defaults to false");

        // Mark p1 as having files. delta_scan_can_be_skipped for p1 stays false
        // because the table is no longer empty — short-circuit must NOT
        // fire (otherwise we'd hide the just-flushed data).
        db.mark_delta_has_files(&p1, t);
        assert!(
            !db.delta_scan_can_be_skipped(&p1, t),
            "after mark_delta_has_files, table has files → can't skip"
        );

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
        assert!(
            !db.delta_scan_can_be_skipped(&project_id, table),
            "STICKY-TRUE preserved across try_fast_resolve too"
        );

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
        Ok(())
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
            let result = ctx
                .sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
                .await?
                .collect()
                .await?;
            use datafusion::arrow::array::AsArray;
            let count = result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            assert_eq!(count, 1);

            // Test field selection
            let result = ctx
                .sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
                .await?
                .collect()
                .await?;
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
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
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
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
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
        unsafe { std::env::set_var("WALRUS_DATA_DIR", cfg.core.wal_dir()) };
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
            let sql = format!(
                "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp >= TIMESTAMP '{}'",
                project_id, bound
            );
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
                    project_id:           pb.clone(),
                    table_name:           table.clone(),
                    s3_bucket:            "timefusion-tests".to_string(),
                    s3_prefix:            format!("custom-{prefix}"),
                    s3_region:            "us-east-1".to_string(),
                    s3_access_key_id:     "minioadmin".to_string(),
                    s3_secret_access_key: "minioadmin".to_string(),
                    s3_endpoint:          Some("http://127.0.0.1:9000".to_string()),
                },
            );

            // One batch, interleaved A/B/A so row 0 (pa) is not the only project.
            let batch = json_to_batch(vec![test_span("a1", "n", &pa), test_span("b1", "n", &pb), test_span("a2", "n", &pa)])?;
            let provider = ctx.table_provider(table.as_str()).await?;
            // Upcast to &dyn Any (TableProvider: Any) — `use super::*` pulls arrow's
            // Array::as_any into scope, which would otherwise shadow the right method.
            let any: &dyn std::any::Any = provider.as_ref();
            let rt = any
                .downcast_ref::<ProjectRoutingTable>()
                .ok_or_else(|| anyhow::anyhow!("otel_logs_and_spans is not a ProjectRoutingTable"))?;
            assert_eq!(rt.fast_insert_batch(batch).await?, 3);

            let count = |p: String| {
                let ctx = ctx.clone();
                async move {
                    let sql = format!("SELECT COUNT(*) c FROM otel_logs_and_spans WHERE project_id = '{p}'");
                    Result::<i64>::Ok(ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0))
                }
            };
            assert_eq!(
                count(pb.clone()).await?,
                1,
                "pb's row must reach pb's BYO bucket, not leak into pa's unified table"
            );
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
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "span2");

            // Test filtering by duration
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND duration > 150000000",
                    project_id
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(get_str(result[0].column(0).as_ref(), 0), "span2");

            // Test compound filtering
            let result = ctx
                .sql(&format!(
                    "SELECT id, status_message FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'",
                    project_id
                ))
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
            let result = ctx
                .sql(&format!(
                    "SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'sql_id'",
                    proj2
                ))
                .await?
                .collect()
                .await?;
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
            let all_records = ctx
                .sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id))
                .await?
                .collect()
                .await?;
            assert!(!all_records.is_empty(), "No records found in table");

            // Test timestamp filtering - need to include project_id
            let result = ctx
                .sql(&format!(
                    "SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp > '2023-01-01T11:00:00Z'",
                    project_id
                ))
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

            let results: Vec<Result<String, _>> = futures::future::join_all(tasks)
                .await
                .into_iter()
                .map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?)
                .collect();

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
            let results: Vec<Result<String, _>> = futures::future::join_all(tasks)
                .await
                .into_iter()
                .map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?)
                .collect();

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
}
