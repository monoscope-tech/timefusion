use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    path::PathBuf,
    sync::Arc,
};

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
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, error, field::Empty, info, instrument, warn};
use url::Url;

use crate::{
    config::{self, AppConfig},
    observability::arrow_err,
    read::DeltaStatisticsExtractor,
    schema::{create_insert_compatible_schema, get_schema, is_variant_type, schema_or_default},
    storage::{FoyerCacheConfig, FoyerObjectStoreCache, SharedFoyerCache},
};

mod compact;
mod maintain;
mod write;

/// Delta tables shared by default projects and partitioned by `project_id`.
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

#[derive(derive_more::Debug)]
#[debug("CachedDeltaProvider {{ version: {version}, age: {:?}, .. }}", created_at.elapsed())]
struct CachedDeltaProvider {
    version: u64,
    created_at: std::time::Instant,
    cell: Arc<DeltaProviderCell>,
}

/// Snapshot versions kept cached per `(project, table)`. A single slot thrashes
/// under flush cadence (every commit bumps the version; concurrent readers
/// observe non-monotonically); a short ring serves each query its exact version.
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

/// Why the swept-partition dedup skip was granted or refused, decided once per
/// scan. The split answers "would persisting certifications convert the
/// denial?": `NeverCertified` = yes, `FpMoved` = no (written since certified).
/// See `docs/plans/2026-08-11-certification-survival.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DedupSkipVerdict {
    Granted,
    /// `timefusion_read_dedup_skip_swept` is off, or the table declares no dedup keys.
    Disabled,
    /// No usable time bound on the query (no timestamp predicate, or a span so
    /// wide `window_dates` refuses it), so there is no partition set to certify.
    NoWindow,
    /// The table was not already resolved in this process. The skip is an
    /// optimisation, so it declines rather than pay a resolve to decide.
    Unresolved,
    /// Some in-window partition has no `dedup_clean_fp` entry at all.
    NeverCertified,
    /// Some in-window partition was certified and has been committed to since.
    FpMoved,
}

impl DedupSkipVerdict {
    pub fn granted(self) -> bool {
        self == Self::Granted
    }
}

/// Counter/gauge names for `ScanMetrics`, shared between the `metrics::counter!()`/
/// `gauge!()` call sites (below) and their `timefusion_stats` readback
/// (`server::pg_compat`) so the two can't drift apart into a silent typo. High-water-mark
/// fields (`decode_polls_inflight`/`_peak`, `decode_peak_batch_bytes`) stay hand-rolled
/// atomics below — `metrics::Gauge` has no `fetch_max`, so there's no clean equivalent.
pub mod scan_metric_names {
    pub const SCANS_TOTAL: &str = "timefusion.scan.scans_total";
    pub const SCANS_SKIPPED_DELTA: &str = "timefusion.scan.scans_skipped_delta";
    pub const SCANS_MEM_ONLY: &str = "timefusion.scan.scans_mem_only";
    pub const SCANS_DELTA_ONLY: &str = "timefusion.scan.scans_delta_only";
    pub const SCANS_MEM_PLUS_DELTA: &str = "timefusion.scan.scans_mem_plus_delta";
    pub const DEDUP_ELIGIBLE_SCANS: &str = "timefusion.scan.dedup_eligible_scans";
    pub const DEDUP_SKIPPED: &str = "timefusion.scan.dedup_skipped";
    pub const DEDUP_DENIED_UNCERTIFIED: &str = "timefusion.scan.dedup_denied_uncertified";
    pub const DEDUP_DENIED_BY_LEG: &str = "timefusion.scan.dedup_denied_by_leg";
    pub const DEDUP_DENIED_NEVER_CERTIFIED: &str = "timefusion.scan.dedup_denied_never_certified";
    pub const DEDUP_DENIED_FP_MOVED: &str = "timefusion.scan.dedup_denied_fp_moved";
    pub const DEDUP_DENIED_NO_WINDOW: &str = "timefusion.scan.dedup_denied_no_window";
    pub const DEDUP_DENIED_UNRESOLVED: &str = "timefusion.scan.dedup_denied_unresolved";
    pub const DEDUP_DENIED_DISABLED: &str = "timefusion.scan.dedup_denied_disabled";
    pub const CERT_GRANTED_TOTAL: &str = "timefusion.scan.cert_granted_total";
    /// Scans that skipped `DedupExec` over SOME (not all) in-window dates.
    pub const DEDUP_SKIPPED_PER_DATE: &str = "timefusion.scan.dedup_skipped_per_date";
    /// Scans where the per-FILE skip fired: some files of an UNCERTIFIED date
    /// were proved clean and isolated, so they bypassed DedupExec.
    pub const DEDUP_SKIPPED_PER_FILE: &str = "timefusion.scan.dedup_skipped_per_file";
    // Where the tantivy ROUTING TAX actually goes. Measured 2026-08-22: a routed
    // equality costs ~300-500ms more than the identical unrouted query while the
    // whole tantivy fan-out is 33-83ms of it, with zero IO — so ~85% is spent
    // here, building the scan, and nothing could see it. The single-provider
    // fast path needs `raw.is_empty() && !bloom_pruned && date_restrict.is_none()`;
    // these name which conjunct failed, because "drain coverage" and "make the
    // split cheap" are very different fixes and only the loser tells you which.
    pub const TANTIVY_SCAN_CALLS: &str = "timefusion.scan.tantivy_scan_calls";
    pub const TANTIVY_SCAN_US: &str = "timefusion.scan.tantivy_scan_us";
    pub const TANTIVY_URIS_US: &str = "timefusion.scan.tantivy_uris_us";
    pub const TANTIVY_FASTPATH: &str = "timefusion.scan.tantivy_fastpath";
    pub const TANTIVY_SPLIT_RAW: &str = "timefusion.scan.tantivy_split_raw";
    pub const TANTIVY_SPLIT_BLOOM: &str = "timefusion.scan.tantivy_split_bloom";
    pub const TANTIVY_SPLIT_DATE: &str = "timefusion.scan.tantivy_split_date";
    pub const TANTIVY_LIVE_FILES: &str = "timefusion.scan.tantivy_live_files";
    pub const TANTIVY_RAW_FILES: &str = "timefusion.scan.tantivy_raw_files";
    // Mirrors of the OTel-only prefilter recorders. `recorders!` adds to an OTel
    // meter, which `counter_value` (LOCAL_REGISTRY, fed by `metrics::counter!`)
    // cannot see — so reading them through timefusion_stats returned 0 for
    // everything and made "the prefilter never ran" look measured. Emit on the
    // same path as every other row here instead.
    pub const PREFILTER_ATTEMPTS: &str = "timefusion.scan.prefilter_attempts";
    pub const PREFILTER_USED: &str = "timefusion.scan.prefilter_used";
    pub const PREFILTER_SKIPPED: &str = "timefusion.scan.prefilter_skipped";
    /// Per-reason breakdown of `PREFILTER_SKIPPED`. SIX reasons, not three:
    /// `decide_prefilter`'s exits (`empty_index`, `low_selectivity`,
    /// `field_coverage_gap`) plus the search aborts, which increment the same
    /// total from a different call site. Without the split, "63% of attempts are
    /// skipped" cannot be attributed to a decision at all — and the fix for a
    /// declined decision is the opposite of the fix for a missing index.
    ///
    /// `counter_value` reads by `&'static str`, so the names are consts and this
    /// is the one place reason strings and metric names are tied together.
    pub const PREFILTER_SKIP_REASONS: [(&str, &str); 6] = [
        ("empty_index", "timefusion.scan.prefilter_skipped.empty_index"),
        ("low_selectivity", "timefusion.scan.prefilter_skipped.low_selectivity"),
        ("field_coverage_gap", "timefusion.scan.prefilter_skipped.field_coverage_gap"),
        ("delta_no_index_or_cap_exceeded", "timefusion.scan.prefilter_skipped.no_index_or_cap"),
        ("delta_no_hits_returned", "timefusion.scan.prefilter_skipped.no_hits_returned"),
        ("delta_error", "timefusion.scan.prefilter_skipped.delta_error"),
    ];

    /// Why a slice's coverage was refused, splitting `rollup_miss_stale_coverage`.
    /// Unverifiable (predates `TAG_SOURCE_ROWS`) and genuinely-moved are the same
    /// miss with opposite fixes: the first clears on republish, the second cannot.
    pub const ROLLUP_STALE_NO_WITNESS: &str = "timefusion.scan.rollup_stale_no_witness";
    pub const ROLLUP_STALE_MOVED: &str = "timefusion.scan.rollup_stale_moved";
    pub const ROLLUP_STALE_NO_SOURCE_ROWS: &str = "timefusion.scan.rollup_stale_no_source_rows";

    pub fn prefilter_skip_metric(reason: &str) -> Option<&'static str> {
        PREFILTER_SKIP_REASONS.iter().find(|(r, _)| *r == reason).map(|(_, m)| *m)
    }
    /// Indexes built by the reconcile BACKFILL specifically — not by flush,
    /// compaction or the wave reindex, all of which also publish.
    pub const TANTIVY_BACKFILL_BUILT: &str = "timefusion.scan.tantivy_backfill_built";
    // Inside the file-pruned scan: which of its three steps owns the ~430ms.
    // The provider cache comment above records ~30ms for a pure provider build,
    // so the cost is expected in selection construction or in scan() over a
    // multi-thousand-file selection — but that is a guess until measured, and
    // guesses about this path have been wrong three times.
    pub const PRUNED_SELECT_US: &str = "timefusion.scan.pruned_select_us";
    pub const PRUNED_BUILD_US: &str = "timefusion.scan.pruned_build_us";
    pub const PRUNED_SCAN_US: &str = "timefusion.scan.pruned_scan_us";
    pub const PRUNED_CALLS: &str = "timefusion.scan.pruned_calls";
    pub const PRUNED_FILES: &str = "timefusion.scan.pruned_files";
    /// Batched manifest commits made by the backfill, and entries they carried.
    /// `entries / commits` is the amortisation actually achieved.
    pub const TANTIVY_MANIFEST_COMMITS: &str = "timefusion.scan.tantivy_manifest_commits";
    pub const TANTIVY_MANIFEST_COMMIT_US: &str = "timefusion.scan.tantivy_manifest_commit_us";
    // Why a dedup pass did NOT end in a certification. `cert_granted_total` has
    // sat at 0 since 2026-08-20 across three attempted fixes, each of which
    // guessed at the exit; these name the exits so the next fix is measured.
    // Sum of the four ~= calls to `record_clean_slice`.
    pub const CERT_SLICE_OUTSIDE_DAY: &str = "timefusion.scan.cert_slice_outside_day";
    pub const CERT_SLICE_DIRTY: &str = "timefusion.scan.cert_slice_dirty";
    pub const CERT_SLICE_PARTIAL: &str = "timefusion.scan.cert_slice_partial";
    pub const CERT_SLICE_DAY_COVERED: &str = "timefusion.scan.cert_slice_day_covered";
    // Why `record_certification` refused, split by the failing conjunct.
    pub const CERT_REFUSED_DROPPED: &str = "timefusion.scan.cert_refused_dropped";
    pub const CERT_REFUSED_INCOMPLETE: &str = "timefusion.scan.cert_refused_incomplete";
    pub const CERT_REFUSED_EMPTY: &str = "timefusion.scan.cert_refused_empty";
    pub const CERT_REFUSED_FP_MOVED: &str = "timefusion.scan.cert_refused_fp_moved";
    pub const CERT_DWELL_TOTAL: &str = "timefusion.scan.cert_dwell_total";
    pub const CERT_DWELL_SECS_TOTAL: &str = "timefusion.scan.cert_dwell_secs_total";
    pub const FAST_RESOLVE_HITS: &str = "timefusion.scan.fast_resolve_hits";
    pub const FAST_RESOLVE_MISSES: &str = "timefusion.scan.fast_resolve_misses";
    pub const PROVIDER_CACHE_HITS: &str = "timefusion.scan.provider_cache_hits";
    pub const PROVIDER_CACHE_MISSES: &str = "timefusion.scan.provider_cache_misses";
    pub const PROVIDER_CACHE_EVICTIONS: &str = "timefusion.scan.provider_cache_evictions";
    pub const PROVIDER_BUILD_ABANDONED: &str = "timefusion.scan.provider_build_abandoned";
    pub const PROVIDER_BUILD_US_TOTAL: &str = "timefusion.scan.provider_build_us_total";
    pub const PROVIDER_BUILD_TOTAL: &str = "timefusion.scan.provider_build_total";
    pub const PROVIDER_SCAN_US_TOTAL: &str = "timefusion.scan.provider_scan_us_total";
    pub const PROVIDER_SCAN_TOTAL: &str = "timefusion.scan.provider_scan_total";
    pub const BOUNDED_OTEL_SCAN_CANDIDATES: &str = "timefusion.scan.bounded_otel_scan_candidates";
    pub const BOUNDED_OTEL_SCAN_REJECTIONS: &str = "timefusion.scan.bounded_otel_scan_rejections";
    pub const WIDE_SCAN_OVERSIZE_TOTAL: &str = "timefusion.scan.wide_scan_oversize_total";
    pub const MEM_PLAN_US_TOTAL: &str = "timefusion.scan.mem_plan_us_total";
    pub const MEM_PLAN_TOTAL: &str = "timefusion.scan.mem_plan_total";
    pub const PGWIRE_TOTAL: &str = "timefusion.scan.pgwire_total";
    pub const DECODE_BYTES_TOTAL: &str = "timefusion.scan.decode_bytes_total";
    pub const DECODE_PRESSURE_THROTTLED: &str = "timefusion.scan.decode_pressure_throttled";
}

/// High-water-mark decode gauges surfaced via `timefusion_stats`. Separate from the
/// `metrics`-backed counters above because `metrics::Gauge` has no `fetch_max` —
/// `decode_begin`/`decode_end` need the atomic read-modify-write these give directly.
#[derive(Debug, Default)]
pub struct DecodeGauges {
    pub decode_peak_batch_bytes: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight_peak: std::sync::atomic::AtomicU64,
}

/// Counters/gauges surfaced via `timefusion_stats` for production debugging.
/// Recorded through `metrics::counter!()`/`histogram!()` (see `scan_metric_names`)
/// and read back via `observability::{counter_value, histogram_quantile}` — fanned
/// out to OTel export and local readback from one `metrics::Recorder`; see that
/// module for why. `decode` high-water marks are the one exception (`DecodeGauges`).
#[derive(Debug, Default)]
pub struct ScanMetrics {
    pub decode: DecodeGauges,
}

impl ScanMetrics {
    /// One gated decode entered: bump the in-flight gauge and its high-water
    /// mark. Returns nothing — the caller pairs it with `decode_end`.
    fn decode_begin(&self) {
        use std::sync::atomic::Ordering::Relaxed;
        let n = self.decode.decode_polls_inflight.fetch_add(1, Relaxed) + 1;
        self.decode.decode_polls_inflight_peak.fetch_max(n, Relaxed);
    }

    /// One gated decode finished, having produced `bytes` of Arrow.
    fn decode_end(&self, bytes: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.decode.decode_polls_inflight.fetch_sub(1, Relaxed);
        metrics::counter!(scan_metric_names::DECODE_BYTES_TOTAL).increment(bytes);
        self.decode.decode_peak_batch_bytes.fetch_max(bytes, Relaxed);
    }

    /// Outcome of the swept-partition dedup skip. A non-`Granted` verdict means
    /// the window was never eligible; `Granted` with no skip means a leg
    /// refused one that was — different fixes, so the verdict arrives whole.
    #[allow(clippy::too_many_arguments)] // one flag per counter; a struct would just rename them
    pub fn record_scan(
        &self, duration_us: u64, skipped_delta: bool, has_mem: bool, has_delta: bool, fast_resolve_hit: Option<bool>, dedup_skip: bool,
        verdict: DedupSkipVerdict,
    ) {
        use scan_metric_names::*;
        metrics::counter!(SCANS_TOTAL).increment(1);
        // Counted only where a Delta leg was actually read: the skip's prize is the key columns
        // it stops decoding out of Parquet, and a mem-only scan has none to decode.
        if has_delta {
            metrics::counter!(DEDUP_ELIGIBLE_SCANS).increment(1);
            if dedup_skip {
                metrics::counter!(DEDUP_SKIPPED).increment(1);
            } else if verdict.granted() {
                metrics::counter!(DEDUP_DENIED_BY_LEG).increment(1);
            } else {
                metrics::counter!(DEDUP_DENIED_UNCERTIFIED).increment(1);
                let name = match verdict {
                    DedupSkipVerdict::NeverCertified => DEDUP_DENIED_NEVER_CERTIFIED,
                    DedupSkipVerdict::FpMoved => DEDUP_DENIED_FP_MOVED,
                    DedupSkipVerdict::NoWindow => DEDUP_DENIED_NO_WINDOW,
                    DedupSkipVerdict::Unresolved => DEDUP_DENIED_UNRESOLVED,
                    // `Granted` is excluded by the branch above; it shares an arm
                    // rather than introducing a panic path on the scan hot path.
                    DedupSkipVerdict::Disabled | DedupSkipVerdict::Granted => DEDUP_DENIED_DISABLED,
                };
                metrics::counter!(name).increment(1);
            }
        }
        let by_source = match (has_mem, has_delta) {
            (true, false) => Some(SCANS_MEM_ONLY),
            (false, true) => Some(SCANS_DELTA_ONLY),
            (true, true) => Some(SCANS_MEM_PLUS_DELTA),
            (false, false) => None,
        };
        let by_resolve = fast_resolve_hit.map(|hit| if hit { FAST_RESOLVE_HITS } else { FAST_RESOLVE_MISSES });
        for name in skipped_delta.then_some(SCANS_SKIPPED_DELTA).into_iter().chain(by_source).chain(by_resolve) {
            metrics::counter!(name).increment(1);
        }
        metrics::histogram!("timefusion.scan.latency_seconds").record(duration_us as f64 / 1_000_000.0);
    }

    /// A certification ended: record its survival in the dwell histogram. The
    /// end is observed (first read to notice), not caused, so the dwell is an
    /// upper bound — it can't make persistence look better than it is.
    pub fn record_cert_dwell(&self, since: std::time::Instant) {
        let secs = since.elapsed().as_secs();
        metrics::counter!(scan_metric_names::CERT_DWELL_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::CERT_DWELL_SECS_TOTAL).increment(secs);
        metrics::histogram!("timefusion.cert.dwell_seconds").record(secs as f64);
    }

    /// Dwell percentile, in seconds. `None` (surfaced as 0) if metrics weren't
    /// initialized or nothing has been recorded yet.
    pub fn cert_dwell_percentile_secs(&self, p: f64) -> u64 {
        crate::observability::histogram_quantile("timefusion.cert.dwell_seconds", p).unwrap_or(0.0) as u64
    }

    /// Record a pgwire end-to-end query duration. Cheap on hot path — a
    /// counter bump and one histogram record.
    pub fn record_pgwire_query(&self, duration_us: u64) {
        metrics::counter!(scan_metric_names::PGWIRE_TOTAL).increment(1);
        metrics::histogram!("timefusion.pgwire.query_latency_seconds").record(duration_us as f64 / 1_000_000.0);
    }

    /// Percentile of the full `ProjectRoutingTable::scan` call, in microseconds.
    pub fn latency_percentile_us(&self, p: f64) -> u64 {
        (crate::observability::histogram_quantile("timefusion.scan.latency_seconds", p).unwrap_or(0.0) * 1_000_000.0) as u64
    }
    pub fn pgwire_percentile_us(&self, p: f64) -> u64 {
        (crate::observability::histogram_quantile("timefusion.pgwire.query_latency_seconds", p).unwrap_or(0.0) * 1_000_000.0) as u64
    }
}

/// One partition proved duplicate-free: the file fingerprint it was proved over,
/// and when. `since` exists purely to measure how long certifications survive —
/// the evidence that decides whether persisting them could ever pay.
#[derive(Clone, Debug)]
struct Certification {
    fp: u64,
    since: std::time::Instant,
    /// The file paths the certifying pass proved clean. `fp` is their hash and
    /// answers "is the partition UNCHANGED"; this answers the weaker, additive
    /// question "which of the files still live were proved" — the one that
    /// survives a partition gaining a file. Empty for a certification restored
    /// from a store written before the field existed, which simply means the
    /// per-file skip cannot use it.
    files: Arc<[String]>,
    /// The partition has since MOVED, so this can never again grant the
    /// whole-partition skip — but its file list is still true of the files it
    /// names, which is exactly what the per-FILE skip needs.
    ///
    /// Before this, the read path DELETED a certification the moment its
    /// fingerprint moved. That is right for the all-or-nothing question and
    /// fatal for the per-file one: a single new file destroyed the evidence for
    /// every file it did not overlap, so the per-file skip could never fire on a
    /// churning partition — the only kind it exists for.
    stale: bool,
}

/// Clean dedup slices accumulated toward certifying one (project, table, date)
/// partition: disjoint sorted `[start, end)` intervals, all proved over `fp`.
/// A unit that drops rows or a fingerprint move resets the accumulation —
/// evidence over a moved file set is void. Memory-only: a restart just
/// re-accumulates; certification itself persists.
#[derive(Clone, Debug)]
struct SliceCoverage {
    fp: u64,
    intervals: Vec<(i64, i64)>,
}

/// Merge `[start, end)` into a sorted vec of disjoint half-open intervals.
fn merge_clean_interval(intervals: &mut Vec<(i64, i64)>, interval: (i64, i64)) {
    intervals.push(interval);
    intervals.sort_unstable();
    *intervals = intervals.iter().copied().coalesce(|a, b| if b.0 <= a.1 { Ok((a.0, a.1.max(b.1))) } else { Err((a, b)) }).collect();
}

// Custom project tables: projects with their own S3 bucket get isolated tables
// Key: (project_id, table_name) -> DeltaTable
pub type CustomProjectTables = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;

// Per-table (keyed by storage URL), per-date set of live file URIs at the last
// successful z-order optimize. Backs the ZOrder idempotence guard.
type ZOrderFilesets = Arc<RwLock<HashMap<String, HashMap<chrono::NaiveDate, HashSet<String>>>>>;
/// Per-(project_id, table_name) DML serialization mutexes — see `Database::dml_lock`.
type DmlLocks = Arc<dashmap::DashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>;

type RollupSourceKey = (String, String, String);
type RollupCoverageKey = (String, String, String, String);
type RollupSliceCoverageKey = (String, String, String, i64, i64);

#[derive(Debug, Clone)]
struct RollupCoverage {
    source_fp: u64,
    source_epoch: u64,
    generation: String,
    rows: u64,
    /// The source DATE partition's `num_records` sum when this slice was built.
    /// `None` for a slice written before `TAG_SOURCE_ROWS` existed, which the
    /// read path treats as unverifiable and refuses — see
    /// `rollup::slice_coverage_agrees`.
    source_rows: Option<u64>,
    /// Exclusive upper bound on the source timestamps this build actually
    /// aggregated. `day_start + DAY_MICROS` for a sealed day — the whole
    /// partition — and less than that only for a day still being written.
    ///
    /// Stored rather than recomputed because it is TIME-VARYING for today: the
    /// build, the read and the ticket re-check all have to agree on the same
    /// bound, and two of them computing it a minute apart would disagree and
    /// invalidate perfectly good coverage on every query.
    covered_through: i64,
}

#[derive(Debug)]
/// (coverage key, source fingerprint, source epoch, generation, the BOUND the
/// fingerprint was taken to). The bound travels with the ticket because it is
/// the build's, not something the re-check may recompute — see
/// `RollupCoverage::covered_through`.
pub(crate) struct RollupReadTicket {
    dates: Vec<(RollupCoverageKey, u64, u64, String, i64)>,
    slices: Vec<(RollupSliceCoverageKey, u64, String)>,
}

/// A matched query's rollup substitute: the SQL to plan, the `Aggregate` node it
/// is substituted for, and the coverage ticket to re-check before it is used.
#[derive(Debug)]
pub(crate) struct RollupRewrite {
    pub sql: String,
    pub grain: String,
    /// `"full"` when the rollup answered the whole window, `"hybrid"` when raw
    /// fringes or a live tail were unioned in. Reported on the hit metric: the
    /// two have very different cost profiles and conflating them hides which one
    /// production actually gets.
    pub mode: &'static str,
    /// The `Aggregate` this rewrite replaces, verbatim, so the caller can swap it
    /// in place instead of taking the plan apart and rebuilding it.
    pub matched: datafusion::logical_expr::LogicalPlan,
    pub ticket: RollupReadTicket,
}
/// Per-physical-table count of flush/ingest committers QUEUED on the commit lock
/// — see `Database::flush_waiters` and the priority check in `commit_wave`.
type FlushWaiterCounts = Arc<dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicUsize>>>;

/// RAII count of one flush/ingest committer waiting on a per-table commit lock.
/// Must decrement on lock acquisition AND on future cancellation — drop
/// covers both; a manual decrement leaks on cancellation and wedges maintenance.
fn flush_waiter(count: &Arc<std::sync::atomic::AtomicUsize>) -> impl Drop + use<> {
    count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    scopeguard::guard(Arc::clone(count), |count| {
        count.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    })
}

/// Get a Delta table from custom project tables by project_id and table_name
pub async fn get_custom_delta_table(custom_tables: &CustomProjectTables, project_id: &str, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    custom_tables.read().await.get(&table_key(project_id, table_name)).cloned()
}

/// Get a Delta table from unified tables by table_name
pub async fn get_unified_delta_table(unified_tables: &UnifiedTables, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    unified_tables.read().await.get(table_name).cloned()
}

/// Should `resolve_*_table` call `update_state()` on the cached snapshot?
/// Biased toward refreshing: skip only when this process's own writes prove the
/// snapshot current — a `(Some(_), None) => false` shortcut broke buffer→Delta
/// visibility (a background flusher may have committed).
fn should_refresh_table(current_version: Option<u64>, last_written_version: Option<u64>) -> bool {
    match (current_version, last_written_version) {
        (Some(current), Some(last)) => current < last,
        (None, None) => false,
        _ => true,
    }
}

/// Max commits behind for the append-only fast catch-up in `refresh_table_snapshot`:
/// each commit in the range costs one log read for the Remove check, so cap it
/// and let larger gaps take the single full re-materialize instead.
const REFRESH_APPEND_CATCHUP_MAX_GAP: u64 = 64;

/// Refresh `table`'s snapshot without holding the write lock across `update_state()`.
///
/// `update_state()` replays the Delta log and does object-store IO; every query refreshes after a
/// flush, so holding the write lock convoyed planning. Clone-update-swap instead: readers keep the
/// old snapshot while a clone refreshes, and the write lock is held only for the swap. The swap is
/// version-guarded against concurrent committers, so the shared handle never regresses.
pub(crate) async fn refresh_table_snapshot(table: &Arc<RwLock<DeltaTable>>, incremental: bool) -> std::result::Result<Option<u64>, deltalake::DeltaTableError> {
    // Staleness probe: versions are contiguous, so the snapshot is current iff
    // `{version+1}.json` doesn't exist — one GET/404 instead of update_state's
    // `_delta_log` LIST (LISTs bypass the Foyer cache). On probe *error* fall
    // through to the full refresh — never skip on uncertainty.
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
    // Fast path (caller's `incremental` flag): carry the materialized file list
    // forward over the catch-up range instead of the O(active files)
    // re-materialize `update_state` pays (2-8s on the 26k-file unified table).
    // Falls back to the full update when not applicable (gap too large, not
    // materialized, unreadable commit).
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

/// Reconcile table properties existing tables predate, idempotently and best-effort.
///
/// A failed property commit must never block table load. Currently retrofits:
/// - `delta.deletedFileRetentionDuration`: reduce tombstone replay on snapshot load/refresh.
/// - `delta.checkpointInterval`: new tables get the configured interval at creation.
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
/// classify). A `None` cutoff means "no recency limit". Single source of truth,
/// shared with the object-store cache admission window.
use crate::storage::date_partition_within as within_recency;

/// Whether `uri`'s `date=YYYY-MM-DD` Hive partition overlaps the `[lo, hi]`
/// microsecond window, at day granularity. Absent/unparseable date ⇒ `true`
/// (conservative: treat as in-window so the coverage gate still demands an
/// index for it). Open bounds (`i64::MIN`/`MAX`) match everything on that side.
fn uri_date_in_window(uri: &str, lo: i64, hi: i64) -> bool {
    let Some(d) = crate::storage::date_partition_of(uri) else {
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

/// The table's path within its bucket (`"s3://bucket/tf/tbl"` → `"tf/tbl"`).
/// Cache inserts happen below delta-rs's `PrefixStore`, so keys are
/// bucket-relative; joining this with `relativize_to_prefix` output yields the
/// key inserts actually used — table-relative keys made evictions silent no-ops.
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

/// Select and order files for `warm_cache_for_uris`.
///
/// Returns `(path, recent)` pairs. Footers warm for every returned file; full-file warming
/// additionally requires `recent`. Non-recent files are kept as footer-only by default or dropped
/// when disabled. Ordered newest date-partition first so a boot-time warm that gets cut short
/// does not leave the partitions dashboards query cold. Undated files sort last.
fn select_warm_paths(
    uris: Vec<String>, prefix: &str, warm_all_footers: bool, cutoff: Option<chrono::NaiveDate>,
) -> (Vec<(object_store::path::Path, bool)>, usize) {
    let (mut paths, dropped): (Vec<(object_store::path::Path, bool)>, Vec<()>) = uris
        .into_iter()
        .filter(|u| u.ends_with(".parquet"))
        .map(|u| {
            let recent = within_recency(&u, cutoff);
            (u, recent)
        })
        .filter(|(_, recent)| warm_all_footers || *recent)
        .partition_map(|(u, recent)| match relativize_to_prefix(prefix, &u) {
            Some(path) => itertools::Either::Left((path, recent)),
            // Prefix mismatch (e.g. trailing-slash or query-string drift
            // between table_url() and get_file_uris()). Warming this file
            // would address the wrong key, so skip it.
            None => itertools::Either::Right(()),
        });
    // Assumes 10-char ISO dates (date=YYYY-MM-DD, lexically sortable). A
    // missing or differently-shaped date= segment keys as "" — sorts last
    // under Reverse (treated as oldest), never a crash.
    let date_key = |p: &object_store::path::Path| {
        let s = p.as_ref();
        s.find("date=").and_then(|i| s.get(i + 5..i + 15)).unwrap_or("").to_string()
    };
    // cached_key: one allocation per path, not one per comparison.
    paths.sort_by_cached_key(|(p, _)| std::cmp::Reverse(date_key(p)));
    (paths, dropped.len())
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
    build_optimize_session_state_tuned(target_partitions, runtime_env, None, None)
}

/// A sort whose caller runs FEW enough concurrent bins that
/// `MAINTENANCE_MAX_PARTITIONS` — sized for the many-sibling-sorters case —
/// does not apply to it. Both hot-tail passes qualify: they hold their own
/// pools and are bounded by `light_rewrite_sem`, not by the heavy rewrite fan-out.
#[derive(Clone, Copy)]
struct UncappedSort {
    partitions: usize,
    /// Per-partition up-front reservation, where the 32 MB default is wrong.
    /// Repair raises it because its unspillable `SortPreservingMergeExec` is
    /// per-partition and it sorts whole multi-hundred-MB files; packing merges a
    /// handful of small files and keeps the default.
    reservation_bytes: Option<usize>,
}

/// `batch_override` shrinks the sort's indivisible admission unit. Merge memory
/// scales with fan-in x batch, so a whole-file repair — the one rewrite whose
/// input is a single ~1 GiB file and whose sort therefore spills into a wide
/// merge — needs a smaller batch than the packing bins it shares a pool with.
/// `uncapped` lifts `MAINTENANCE_MAX_PARTITIONS` and pins the sort's
/// parallelism to the given value — see [`UncappedSort`].
fn build_optimize_session_state_tuned(
    target_partitions: usize, runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>, batch_override: Option<&str>, uncapped: Option<UncappedSort>,
) -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    // batch_size 2048: merge memory ≈ fan-in × batch and otel rows are wide
    // (8192-row batches measured 145MB), so this bounds a 32-file bin merge
    // near ~1GB. The maintenance-CLI budget profile drops it to 256 — a batch
    // is the sort's indivisible admission unit and small pools must admit one.
    let batch_size = batch_override.unwrap_or_else(|| crate::config::try_config().map_or("2048", |c| c.derived.maintenance_batch_size()));
    let mut cfg = match uncapped {
        None => maintenance_session_config(SessionConfig::new(), batch_size, target_partitions),
        Some(u) => maintenance_session_config(SessionConfig::new(), batch_size, usize::MAX).with_target_partitions(u.partitions),
    };
    if let Some(reservation) = uncapped.and_then(|u| u.reservation_bytes) {
        let _ = cfg.options_mut().set("datafusion.execution.sort_spill_reservation_bytes", &reservation.to_string());
    }
    // `runtime_env` is the dedicated bounded maintenance pool (see
    // `maintenance_runtime_env`): allocations still fail as errors rather than
    // OOM-killing the process, but are isolated from query pressure and can spill.
    let mut state = SessionStateBuilder::new().with_config(cfg).with_runtime_env(runtime_env).with_default_features().build();
    // `hash_bucket` is ours, not a builtin, and sharded dedup / slice-builder
    // SQL uses it. These states skip `register_custom_functions`, so without
    // this every sharded rewrite fails to PLAN ("Invalid function
    // 'hash_bucket'"). Registered on the one builder all maintenance contexts share.
    datafusion::execution::FunctionRegistry::register_udf(&mut state, Arc::new(crate::read::functions::hash_bucket_udf())).ok();
    state
}

/// Days of rollup coverage counting back from yesterday with no hole, minimised over active
/// projects in `covered`.
///
/// A 30d panel needs 30 contiguous days; any gap sends the query to a raw scan. Minimised,
/// not averaged: one uncovered project means a slow dashboard. Counts from yesterday because
/// today is the live frontier's job. Active projects only, derived from the source table's
/// partitions, not the rollup tier — a broken rollup still has recent source partitions and
/// must stay counted.
const CONTIGUITY_HORIZON_DAYS: u64 = 30;

/// `(worst, worst_project, median)` contiguous answered days across projects.
fn min_contiguous_days<'a>(
    covered: &HashSet<(String, chrono::NaiveDate)>, source: &HashSet<(String, chrono::NaiveDate)>, today: chrono::NaiveDate, active_projects: &HashSet<&'a str>,
) -> (u64, Option<&'a str>, u64) {
    // A day the SOURCE never held counts as answered: a rollup can't exist for
    // rows that don't exist, and the query correctly returns nothing. Without
    // this the minimum is unreachable — one sparse tenant pins the fleet at 0.
    let answered = |project: &str, date: chrono::NaiveDate| {
        let key = (project.to_owned(), date);
        covered.contains(&key) || !source.contains(&key)
    };
    // Capped at the horizon (30 is the goal itself); otherwise a quiet tenant —
    // for whom every pre-first-row day is "answered" — scores the age of the epoch.
    let mut per_project = active_projects
        .iter()
        .map(|project| {
            let days = (1u64..=CONTIGUITY_HORIZON_DAYS)
                .take_while(|back| today.checked_sub_days(chrono::Days::new(*back)).is_some_and(|date| answered(project, date)))
                .count() as u64;
            (days, Some(*project))
        })
        .collect::<Vec<_>>();
    // Name tie-break keeps the reported laggard stable across sweeps.
    let (worst_days, worst) = per_project.iter().copied().min_by_key(|(days, project)| (*days, *project)).unwrap_or((0, None));
    // The minimum is the honest goal ("can EVERY tenant answer a 30d panel")
    // but a terrible control signal — one outlier pins it forever, holding the
    // fleet in coverage-short mode and starving the dedup that would end it.
    // The median is what `coverage_is_short` steers by.
    per_project.sort_unstable_by_key(|(days, _)| *days);
    let median = per_project.get(per_project.len() / 2).map_or(0, |(days, _)| *days);
    (worst_days, worst, median)
}

/// Does this file prove its partition holds no rows?
///
/// Only an explicit zero counts. Missing stats is unknown and must read as non-empty; otherwise
/// we would re-plan already-done work forever. The zero case is the one to act on; see
/// `partitions_of`.
fn partition_file_is_empty(num_records: Option<i64>) -> bool {
    num_records == Some(0)
}

/// Which declared tiers each candidate day is missing.
///
/// The old code subtracted the union, but the intent is the intersection (days not covered by every
/// tier). Returning which tiers are missing lets the caller enqueue only the absent tier: a derived
/// tier reads the base tier, so a day missing only its derived tier needs no raw source scan or
/// dedup.
fn tiers_missing_per_day(
    candidates: &[(String, chrono::NaiveDate)], covered_per_tier: &[(usize, HashSet<(String, chrono::NaiveDate)>)],
) -> HashMap<(String, chrono::NaiveDate), Vec<usize>> {
    covered_per_tier
        .iter()
        .flat_map(|(index, covered)| candidates.iter().filter(|key| !covered.contains(*key)).map(|key| (key.clone(), *index)))
        .into_group_map()
}

/// Parallelism cap for every maintenance session. Each partition's
/// `ExternalSorter` reserves `sort_spill_reservation_bytes` up-front from the bounded maintenance
/// pool, so a query-derived partition count exhausts it before the sort can start. Legacy and
/// high-file-count partitions still exceeded the reservation at 4 partitions, hence 2.
const MAINTENANCE_MAX_PARTITIONS: usize = 2;

/// The `date=` partition a repair bin sits in, for ordering by how recent its
/// poison is. Empty string sorts last, which is the right default for a path
/// that carries no date.
fn repair_bin_date(files: &[String]) -> &str {
    files.first().and_then(|p| path_partition_value(p, "date")).unwrap_or("")
}

/// The value of one Hive-style `key=value` path segment, or `None` if the
/// path carries no such segment.
fn path_partition_value<'a>(path: &'a str, key: &str) -> Option<&'a str> {
    path.split('/').find_map(|segment| segment.strip_prefix(key)?.strip_prefix('='))
}

/// Parallelism for the repair sort specifically.
///
/// `MAINTENANCE_MAX_PARTITIONS` pins many concurrent sorters at 2 to protect the bounded pool,
/// but repair runs exactly one bin at a time, so the premise does not hold. 16 partitions uses
/// ~512 MB of up-front reservation against the light pool, which is small enough to fit while
/// keeping rewrite latency inside the tick.
const REPAIR_SORT_PARTITIONS: usize = 16;

/// Up-front merge reservation per repair-sort partition.
///
/// The merge cannot spill, so reserving its share before the sort starts forces the
/// sorter to spill early instead of growing into an unspillable tail. 512 MB x 16
/// partitions reserves ~8 GB, calibrated to cover the measured merge peak. The total
/// is partitions x reservation; change one at a time.
const REPAIR_SORT_RESERVATION_BYTES: usize = 512 * 1024 * 1024;

/// How long after boot the one-shot footer repair waits. Long enough for WAL
/// replay and snapshot load to finish, short enough that a process which only
/// lives ~20 minutes between deploys still gets a repair pass in.
const STARTUP_REPAIR_DELAY: std::time::Duration = std::time::Duration::from_secs(180);
const COORDINATOR_OWNS_SLICE_MAINTENANCE: bool = true;

/// Maximum wall time for one ordinary coordinator unit.
///
/// Dedup and rollup source windows split recursively, so they retain the old
/// five-minute fairness bound even though file rewrites need a longer deadline.
const COORDINATOR_STANDARD_UNIT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(crate::maintenance_coordinator::operation_deadline_secs(crate::maintenance_coordinator::Operation::Dedup));

/// Maximum wall time for one coordinator file-rewrite unit.
///
/// Dedup and rollup units normally finish quickly because their source windows split recursively.
/// File packing must create runs near 256 MiB or the sealed-file-count invariant never converges.
/// A 15-minute ceiling lets the measured rewrite floor finish a 256 MiB run with commit margin,
/// while the spill pool remains the hard decoded-memory bound.
const COORDINATOR_FILE_REWRITE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(crate::maintenance_coordinator::operation_deadline_secs(crate::maintenance_coordinator::Operation::Repair));

/// Last-resort guard around planning plus one operation-specific unit. The
/// operation itself has the tighter bound above; this catches a wedged planner
/// or dispatcher without racing the file-rewrite timeout at the same instant.
const COORDINATOR_LOOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(16 * 60);

/// How often coverage recovery re-reads the tiers' Delta logs.
///
/// Long on purpose: it exists to notice slow-moving facts — a partition holding
/// untagged files, coverage that a restart lost — not to schedule work. Its cost
/// is one log read per tier and no parquet at all.
const COVERAGE_RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// How long before a unit whose source is still buffered could possibly run.
///
/// `has_rows_in_range` is not transient when the slice reaches into time that has not finished
/// yet. A flat five-second retry used to be an unbounded spin and let permanently-eligible
/// buffered-slice tasks monopolize `claim_next`, starving fresh work. The earliest the slice
/// can complete is its own end plus the write path's finalization delay, floored at five
/// seconds so a sealed slice waiting on a slow flush behaves as before.
fn buffered_source_retry_delay(slice: crate::maintenance_coordinator::TimeSlice, now_micros: i64) -> std::time::Duration {
    const FLOOR: std::time::Duration = std::time::Duration::from_secs(5);
    let earliest = slice.end_micros.saturating_add(crate::maintenance_coordinator::FINALIZATION_DELAY_MICROS);
    u64::try_from(earliest.saturating_sub(now_micros)).map_or(FLOOR, |micros| std::time::Duration::from_micros(micros).max(FLOOR))
}

fn coordinator_operation_timeout(operation: crate::maintenance_coordinator::Operation) -> std::time::Duration {
    use crate::maintenance_coordinator::Operation;
    match operation {
        Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair => COORDINATOR_FILE_REWRITE_TIMEOUT,
        // Rollup cost is set by INPUT FILE COUNT — every uncompacted file spans
        // the whole day, so narrowing the slice shrinks nothing and bisection
        // never converges. Safe to lengthen: rollup takes no rewrite permit and
        // its memory is bounded by group cardinality, not input size.
        Operation::BaseRollup | Operation::DerivedRollup => COORDINATOR_FILE_REWRITE_TIMEOUT,
        Operation::Dedup => COORDINATOR_STANDARD_UNIT_TIMEOUT,
    }
}

/// Physical run targets for coordinator packing. Sealed consolidation used to
/// target 512 MiB even though the coordinator's five-minute deadline could not
/// land that unit in production. Keeping both tiers at 256 MiB makes each
/// rewrite deadline-safe and still satisfies the final sealed-file bound.
const COORDINATOR_HOT_TARGET_BYTES: i64 = 256 * 1024 * 1024;
const COORDINATOR_SEALED_TARGET_BYTES: i64 = 256 * 1024 * 1024;

/// Rows per decode batch for any session that reads the wide OTel schema.
///
/// Parquet decode buffer is the largest untracked consumer in the process: `batch_size × row width`
/// is not pool-accounted. The value only works if every session reading these rows uses it; it
/// was previously missed on the DML merge session, which inherited DataFusion's default while doing
/// the most decode-expensive thing in the system.
pub(crate) const WIDE_ROW_DECODE_BATCH_SIZE: &str = "2048";

/// Config tuning shared by every delta-rs maintenance session.
///
/// `schema_force_view_types=false` keeps Variant columns as `Binary` (not `BinaryView`) so
/// delta_kernel's unshredded-variant schema check passes. The sort-spill floor lets a sort spill
/// instead of erroring under the bounded maintenance pool. `skip_physical_aggregate_schema_check`
/// mirrors `create_session_context` because maintenance reads the files carrying the widened nullability.
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
///
/// `batch_size` is the sort's indivisible admission unit, and merge memory
/// scales with fan-in x batch — the same relationship
/// `build_optimize_session_state_tuned`'s `batch_override` exists for. The
/// flush escalation sort passes 256 because it has the worst combination in
/// the system: the largest batches against the smallest pool (the 1 GB
/// `flush_sort_pool`, vs maintenance's 22.5 GB).
fn build_delta_write_session_state(
    target_partitions: usize, runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>, batch_size: &str,
) -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    let base: SessionConfig = deltalake::delta_datafusion::DeltaSessionConfig::default().into();
    let cfg = maintenance_session_config(base, batch_size, target_partitions);
    SessionStateBuilder::new()
        .with_config(cfg)
        .with_runtime_env(runtime_env)
        .with_default_features()
        .with_query_planner(deltalake::delta_datafusion::planner::DeltaPlanner::new())
        .build()
}

/// Spawn a background cron task that runs `job` at each wall-clock occurrence of `schedule`.
///
/// Fire times are computed from the system clock, so they are predictable and independent of
/// process start time. Each fire runs on a detached task; if it is still running at the next tick,
/// the tick is skipped. Slow but healthy runs are not aborted; only shutdown forces an in-flight
/// abort. Long-running runs are logged and metriced so wedged I/O is visible. Replaces an
/// external cron scheduler that silently stopped dispatching ticks.
fn spawn_cron_job<F, Fut>(name: &'static str, schedule: &str, cancel: Arc<CancellationToken>, job: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    spawn_cron_job_on(name, schedule, cancel, None, job);
}

/// Schedule like [`spawn_cron_job`], but execute each job body on `executor`.
/// The lightweight wall-clock timer remains on the caller's runtime so shutdown
/// and tick accounting retain one owner; CPU and I/O polling for the job itself
/// stay off the foreground PGWire runtime.
fn spawn_cron_job_on<F, Fut>(name: &'static str, schedule: &str, cancel: Arc<CancellationToken>, executor: Option<tokio::runtime::Handle>, job: F)
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
                            crate::observability::maintenance_stats().cron_ticks_skipped.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if running_since.is_some_and(|s| s.elapsed() >= LONG_RUNNING_WARN_THRESHOLD) {
                                warn!("{name} job run still in progress after {:?} — may be wedged or just slow (skips={skips})", LONG_RUNNING_WARN_THRESHOLD);
                                crate::observability::record_cron_long_running();
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
                    crate::observability::maintenance_stats().cron_ticks_fired.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    running_since = Some(std::time::Instant::now());
                    let future = job();
                    running = Some(match &executor {
                        Some(handle) => handle.spawn(future),
                        None => tokio::spawn(future),
                    });
                }
            }
        }
    });
}

/// `spawn_cron_job` for the common "clone `db`, run an async method on it" shape — owns
/// both the per-tick and per-run clone so call sites write `|db| async move { .. }` once
/// instead of `{ let db = db.clone(); move || { let db = db.clone(); async move { .. } } }`.
fn spawn_db_cron<F, Fut>(db: &Arc<Database>, name: &'static str, schedule: &str, cancel: Arc<CancellationToken>, job: F)
where
    F: Fn(Arc<Database>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let db = Arc::clone(db);
    spawn_cron_job(name, schedule, cancel, move || job(Arc::clone(&db)));
}

/// On-disk key for the WAL watermark stored in `commitInfo.info`. Constant so
/// the writer (this file) and reader (`derive_wal_cursor_for_table`) can't
/// drift, and the roundtrip test below pins the format.
const WAL_WATERMARK_KEY: &str = "timefusion.wal_watermark";

/// Serialize a per-shard watermark to the JSON map shape we store in
/// `commitInfo.info[WAL_WATERMARK_KEY]`. Only shards with a position are
/// included — absent shards mean "no constraint from this commit", which is
/// how the per-shard MAX aggregation across commits ignores them.
fn serialize_watermark_to_json(watermark: &crate::write::DeltaWatermark, project_id: &str, table_name: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut map: serde_json::Map<String, serde_json::Value> = watermark
        .iter()
        .enumerate()
        .filter_map(|(shard, pos)| pos.map(|p| (shard.to_string(), serde_json::json!({ "block_id": p.block_id, "offset": p.offset }))))
        .collect();
    if !map.is_empty() {
        // Topic scope: unified-table tenants share ONE Delta log and walrus
        // positions are per-topic, so an unscoped watermark read by another
        // tenant advances its cursor past unreplayed entries. Backward-compatible:
        // older readers skip non-numeric keys.
        map.insert(WATERMARK_TOPIC_KEY.to_string(), serde_json::Value::String(wal_topic(project_id, table_name)));
    }
    map
}

/// Key inside the watermark object naming the topic that produced it.
const WATERMARK_TOPIC_KEY: &str = "topic";

/// Key holding the MULTI-topic map of a cross-project coalesced commit:
/// `{ "<project>:<table>": <single-topic map> }`. Single-topic commits keep the
/// flat legacy shape byte-for-byte; an old binary reading a MULTI commit finds
/// no top-level `topic` key and contributes nothing (under-advance = replay +
/// dedup, never loss) — the safe rollback direction.
const WATERMARK_TOPICS_KEY: &str = "topics";

/// Serialize the watermarks of every (project, table) carried by ONE commit.
/// Topics whose watermark has no positions are dropped (same rule as the
/// single-topic form). Exactly one surviving topic ⇒ flat legacy shape.
fn serialize_watermarks_to_json(
    entries: impl IntoIterator<Item = (String, String, crate::write::DeltaWatermark)>,
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
/// DataFusion's `DiskManager` removes its temp dirs on `Drop`, which a SIGKILL skips, so every
/// OOM kill leaks spill files. A full disk means WAL appends fail, so this is a durability
/// concern. The orphan list is snapshotted *inline* before this env's `DiskManager` exists, so
/// nothing this process wrote can be in it; the WAL dir flock guarantees no second live
/// TimeFusion. Deletion runs on a detached thread because reaping a dead process's garbage is
/// never urgent.
fn reap_orphaned_spill_dirs(spill_dir: &std::path::Path) {
    // Snapshot the orphan list synchronously, BEFORE this env's DiskManager
    // exists: enumerating lazily on the detached thread raced dirs the new
    // DiskManager creates and yanked a live spill dir mid-sort. Only the
    // pre-existing snapshot is deleted off-thread.
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
    info: &HashMap<String, serde_json::Value>, shards: usize, project_id: &str, table_name: &str,
) -> Vec<Option<walrus_rust::WalPosition>> {
    let mut out = vec![None; shards];
    let Some(wm) = info.get(WAL_WATERMARK_KEY).and_then(|v| v.as_object()) else {
        return out;
    };
    let topic = wal_topic(project_id, table_name);
    // Only apply a watermark to the topic that wrote it: an unattributable
    // position may be another tenant's, and over-advancing a cursor loses acked
    // writes, whereas under-advancing only replays duplicates (dedup drops them).
    let wm = match wm.get(WATERMARK_TOPICS_KEY).and_then(|v| v.as_object()) {
        Some(topics) => match topics.get(&topic).and_then(|v| v.as_object()) {
            Some(mine) => mine,
            None => return out,
        },
        None if wm.get(WATERMARK_TOPIC_KEY).and_then(|v| v.as_str()) == Some(topic.as_str()) => wm,
        None => return out,
    };
    for (shard, pos_val) in wm.iter().filter_map(|(s, v)| s.parse::<usize>().ok().filter(|&s| s < shards).map(|s| (s, v))) {
        let field = |k| pos_val.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
        out[shard] = Some(walrus_rust::WalPosition { block_id: field("block_id"), offset: field("offset") });
    }
    out
}

/// Take the per-shard MAX position across a sequence of commit-info maps.
/// `None` for a shard means no commit observed had a position for it.
/// Used during startup to compute the cursor each shard should sit at to
/// be consistent with all recent Delta commits.
fn max_watermark_across_commits<'a>(
    commit_infos: impl IntoIterator<Item = &'a HashMap<String, serde_json::Value>>, shards: usize, project_id: &str, table_name: &str,
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

/// Base [`CommitProperties`] for every ingest/maintenance commit.
///
/// Disables the delta-rs post-commit checkpoint and expired-log-cleanup hooks. Those run after
/// `N.json` is durably written, but a hook failure is surfaced as a commit error, which the flush/
/// dedup error arms misread as "commit never landed" and then delete the parquet the landed commit
/// references. Both hooks now run out-of-band in the maintenance scheduler, tolerant of object-store
/// 500s. `Some(false)` overrides the `enableExpiredLogCleanup` table property per-commit.
fn base_commit_properties() -> CommitProperties {
    CommitProperties::default().with_create_checkpoint(false).with_cleanup_expired_logs(Some(false))
}

/// Build [`CommitProperties`] carrying the watermark under [`WAL_WATERMARK_KEY`].
/// Empty when the watermark has no positions (e.g. WAL-replay-derived buckets);
/// delta-rs writes the commit without the key in that case, and recovery
/// silently skips that commit.
/// Takes every (project, table, watermark) the commit carries — one entry on
/// the per-project path, N on a coalesced cross-project commit.
fn build_watermark_commit_properties(watermarks: impl IntoIterator<Item = (String, String, crate::write::DeltaWatermark)>) -> CommitProperties {
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

/// `table.get_file_uris()`, collected into `C` — empty on an unloaded snapshot,
/// matching every existing `unwrap_or_default()` call site.
pub fn file_uris<C: Default + FromIterator<String>>(table: &DeltaTable) -> C {
    table.get_file_uris().map(Iterator::collect).unwrap_or_default()
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

/// Cheap structural check that catches real-world checkpoint-corruption classes.
///
/// A Parquet file ends with `[footer_len: u32 LE][PAR1]`. This catches an object overwritten with
/// foreign bytes or a truncated write without reading the whole file. `tail` is the file's last 8 bytes.
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

/// (project_id, table_name) key shape shared by the table caches/maps below.
pub(crate) fn table_key(project_id: &str, table_name: &str) -> (String, String) {
    (project_id.to_string(), table_name.to_string())
}

/// Exponential backoff between OCC conflict retries — single policy for every
/// retry site (flush, optimize, dedup, DML): 150, 300, 600ms… capped so the
/// shift can't overflow if a caller raises its attempt limit.
pub(crate) fn occ_backoff(attempt: usize) -> tokio::time::Duration {
    tokio::time::Duration::from_millis(150 << attempt.min(6))
}

/// True for transient S3/network transport failures worth retrying at a higher level.
///
/// `object_store` retries individual requests, but a multipart part whose connection drops
/// mid-body can bubble up as terminal. delta-rs wraps these as "Failed to parse parquet", so we
/// match the transport phrases, not the wrapper. Auth and permanent errors (403, NoSuchBucket)
/// fail fast. Phrases are anchored to genuinely transient states — notably not a bare
/// "connection", which would match permanent "connection refused" and burn the retry budget.
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

/// A partition's identity hash plus the row timestamps its files span.
#[derive(Clone, Copy)]
pub(crate) struct PartitionStats {
    fingerprint: u64,
    min_ts: i64,
    max_ts: i64,
    /// The partition's `num_records` sum. The witness rollup slice coverage is
    /// checked against (`rollup::slice_coverage_agrees`), so it must stay THIS
    /// computation on both sides — it counts tombstones and superseded
    /// merge-on-read versions, which the builder's decoded input does not.
    rows: i64,
}

impl PartitionStats {
    /// Whether this partition may hold rows in `[lo, hi)`.
    ///
    /// A partition whose files carry no timestamp statistics reports the
    /// sentinel range (`min > max`) and is treated as OVERLAPPING. Excluding it
    /// would drop a project from the coverage requirement on the strength of a
    /// statistic that is not there — the direction that undercounts.
    fn overlaps(&self, lo: i64, hi: i64) -> bool {
        self.min_ts > self.max_ts || (self.min_ts < hi && self.max_ts >= lo)
    }
}

/// Days of contiguous rollup coverage below which the maintenance cycle favours
/// the rollup chain over independent file debt.
///
/// A 14d panel is the most common wide dashboard window and the cheapest useful
/// target; 30d follows once 14 holds. Set at the goal rather than at 1 so the
/// weighting does not flap the moment the first day lands. Pub for the journal
/// simulator (`maintenance_sim`), which drives the same switch from its own
/// contiguity model.
pub const COVERAGE_SHORT_DAYS: u64 = 14;

/// Is contiguous rollup coverage short enough to be worth reweighting for?
/// Reads the gauge the backfill sweep already publishes — one atomic load.
fn coverage_is_short() -> bool {
    // The MEDIAN, not the minimum: the minimum is the goal metric but one
    // negligible tenant pins it forever, holding the fleet in coverage-short
    // mode and starving the dedup certification that would end it.
    crate::observability::maintenance_stats().rollup_median_contiguous_days.load(std::sync::atomic::Ordering::Relaxed) < COVERAGE_SHORT_DAYS
}

/// The overlap of two coalesced range sets. Both inputs must already be sorted
/// and disjoint — `merge_ranges` guarantees that — so one linear sweep suffices.
fn intersect_ranges(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let (mut i, mut j, mut out) = (0, 0, Vec::new());
    while i < left.len() && j < right.len() {
        let (start, end) = (left[i].0.max(right[j].0), left[i].1.min(right[j].1));
        if start < end {
            out.push((start, end));
        }
        // Retire whichever range ends first; the other may still meet the next.
        if left[i].1 < right[j].1 { i += 1 } else { j += 1 }
    }
    out
}

/// UTC dates covered by a `[lo, hi]` microsecond window, or `None` when the
/// window is unbounded/invalid/wider than a year (such queries keep DedupExec).
fn window_dates(lo: i64, hi: i64) -> Option<Vec<chrono::NaiveDate>> {
    let lo_d = chrono::DateTime::from_timestamp_micros(lo)?.date_naive();
    let hi_d = chrono::DateTime::from_timestamp_micros(hi)?.date_naive();
    let span = (hi_d - lo_d).num_days();
    if !(0..=366).contains(&span) {
        return None;
    }
    Some((0..=span).map(|d| lo_d + chrono::Duration::days(d)).collect())
}

/// The partition dates a write batch's rows land in, or `None` when the batch does not say.
///
/// `None` costs the whole table its rollup coverage, so both `timestamp` and the `date` partition
/// column must fail. Distinct days are collected before formatting to allocate one `String` per day
/// rather than per row; it runs on every inbound write.
fn batch_hours(batch: &RecordBatch) -> Option<HashMap<String, u32>> {
    use datafusion::arrow::{
        array::AsArray,
        compute::cast,
        datatypes::{DataType, TimeUnit, TimestampMicrosecondType},
    };
    const DAY: i64 = 86_400_000_000;
    let micros = batch.column_by_name("timestamp").and_then(|column| {
        let wanted = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let column = if column.data_type() == &wanted { column.clone() } else { cast(column, &wanted).ok()? };
        // Distinct (day, hour) pairs — at most 24 per day however many rows.
        Some(
            column
                .as_primitive_opt::<TimestampMicrosecondType>()?
                .iter()
                .flatten()
                .map(|micros| (micros.div_euclid(DAY), micros.rem_euclid(DAY) / 3_600_000_000))
                .collect::<HashSet<_>>(),
        )
    });
    if let Some(hours) = micros {
        return Some(hours.into_iter().fold(HashMap::new(), |mut dates, (day, hour)| {
            if let Some(day_start) = chrono::DateTime::from_timestamp_micros(day * DAY) {
                *dates.entry(day_start.date_naive().to_string()).or_default() |= 1 << hour;
            }
            dates
        }));
    }
    // One cast covers Date32, Utf8, Utf8View and LargeUtf8 alike, and only runs
    // on the path where the timestamp was already unreadable. Without a
    // timestamp there is no hour to name, so the whole day is dirty.
    let text = cast(batch.column_by_name("date")?, &DataType::Utf8).ok()?;
    Some(text.as_string::<i32>().iter().flatten().map(|date| (date.to_string(), crate::rollup::ALL_HOURS)).collect())
}

const DAY_MICROS: i64 = 86_400_000_000;

/// The TF-owned stamp a table's writes overwrite, if it declares one.
fn tiebreak_of(source: &str) -> Option<&'static str> {
    get_schema(source).and_then(|schema| schema.dedup_tiebreak.as_deref())
}

/// Microseconds at 00:00:00 UTC on a `YYYY-MM-DD` partition value.
fn date_start_micros(date: &str) -> Option<i64> {
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()?.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros().into()
}

fn delta_stat_micros(value: &serde_json::Value) -> Option<i64> {
    match value {
        serde_json::Value::Number(number) => number.as_i64(),
        serde_json::Value::String(value) => chrono::DateTime::parse_from_rfc3339(value)
            .map(|time| time.timestamp_micros())
            .or_else(|_| chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f").map(|time| time.and_utc().timestamp_micros()))
            .ok(),
        _ => None,
    }
}

/// Does the UTC day named by `date` overlap the half-open `[start, end)`?
fn date_intersects(date: &str, (start, end): (i64, i64)) -> bool {
    date_start_micros(date).is_some_and(|day| day < end && day.saturating_add(86_400_000_000) > start)
}

fn window_hour_masks(lo: i64, hi: i64) -> Option<Vec<(String, u32)>> {
    if lo >= hi {
        return None;
    }
    let dates = window_dates(lo, hi.checked_sub(1)?)?;
    Some(
        dates
            .into_iter()
            .filter_map(|date| {
                let day = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
                let first = lo.saturating_sub(day).max(0).div_euclid(3_600_000_000).clamp(0, 23);
                let last = hi.checked_sub(1)?.saturating_sub(day).max(0).div_euclid(3_600_000_000).clamp(0, 23);
                let mask = (first..=last).fold(0u32, |mask, hour| mask | (1 << hour));
                Some((date.to_string(), mask))
            })
            .collect(),
    )
}

/// Whether a commit that returned an error actually landed.
///
/// delta-rs surfaces a post-commit hook or snapshot-refresh failure as a commit `Err` even though
/// `N.json` is already durably written. Deleting the staged parquet in that case orphans the Adds
/// the landed commit references, so the flush path must tell the cases apart. See
/// `Database::probe_commit_landed`.
#[derive(Debug)]
pub(crate) enum CommitProbe {
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
    pub watermark: crate::write::DeltaWatermark,
}

/// A unit whose parquet is uploaded and whose `Add` actions are waiting for the
/// shared commit.
struct StagedUnit {
    table_ref: Arc<RwLock<DeltaTable>>,
    schema: &'static crate::schema::TableSchema,
    dirty_bins: Vec<(String, i64)>,
    adds: Vec<deltalake::kernel::Action>,
    stage_store: Arc<dyn object_store::ObjectStore>,
}

/// Marks a commit error where landing could not be confirmed. The staged parquet must be left in
/// place; deleting files a landed commit references creates dangling Adds.
const INCONCLUSIVE_COMMIT_MARKER: &str = "landing-unconfirmed";

/// Last-resort circuit breaker on a network await taken while a per-table commit lock is held.
///
/// The lock serializes every committer for one physical table, so a hung request pins the
/// whole table at zero commit throughput. This is NOT the primary commit timeout: per-request
/// and retry-ladder bounds in the object-store client are strictly better, because timing out
/// here abandons a future mid-flight and manufactures an unconfirmed landing. This fires only
/// if a request escapes those bounds. 600s matches `CHECKPOINT_OP_TIMEOUT`.
const COMMIT_LOCK_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// A bounded in-guard commit await that did not succeed.
#[derive(Debug)]
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
            crate::observability::record_commit_timeout(op);
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

/// Split a coalesced commit's newly-added file URIs per project — the
/// `project_id=<id>/` path segment IS the attribution, so downstream consumers
/// receive only their own files. Single-project groups pass through unfiltered.
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
    schema: &'static crate::schema::TableSchema,
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

/// Collect matched Adds keeping at most one per path.
///
/// A path can appear twice in an in-memory snapshot when the incremental refresh crossed a
/// checkpoint and the kernel "delta" was silently a full file set. Rewrite planners compare
/// `targets.len()` against the distinct files they mean to rewrite, so a duplicate silently
/// mismatches the plan and stalls compaction.
fn dedup_adds_by_path(adds: impl Iterator<Item = deltalake::kernel::Add>, table_name: &str) -> Vec<deltalake::kernel::Add> {
    let mut seen = HashSet::new();
    let mut total = 0usize;
    let out: Vec<deltalake::kernel::Add> = adds.inspect(|_| total += 1).filter(|add| seen.insert(add.path.clone())).collect();
    let dropped = total - out.len();
    if dropped > 0 {
        warn!(table_name, dropped, event = "snapshot_duplicate_adds", "snapshot listed the same file more than once — reads over it double-count rows");
        crate::observability::maintenance_stats().snapshot_duplicate_adds.fetch_add(dropped as u64, std::sync::atomic::Ordering::Relaxed);
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
    remap_batch_columns(batch, |_, field, col| {
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
    batch: RecordBatch,
    remap: impl Fn(usize, &arrow_schema::FieldRef, &arrow::array::ArrayRef) -> DFResult<Option<(arrow_schema::FieldRef, arrow::array::ArrayRef)>>,
) -> DFResult<RecordBatch> {
    let schema = batch.schema();
    let remapped = schema.fields().iter().zip(batch.columns()).enumerate().map(|(i, (f, c))| remap(i, f, c)).collect::<DFResult<Vec<_>>>()?;
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
    remap_batch_columns(batch, |_, field, col| {
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
        array::{Array, LargeStringArray, StringArray, StringViewArray, StructArray},
        compute::cast,
        datatypes::{DataType, Field},
    };
    use parquet_variant_compute::VariantArrayBuilder;
    use parquet_variant_json::JsonToVariant;

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
            Arc::new(Field::new(crate::schema::VARIANT_METADATA_FIELD, DataType::Binary, false)),
            Arc::new(Field::new(crate::schema::VARIANT_VALUE_FIELD, DataType::Binary, false)),
        ];
        Ok(StructArray::new(fields.into(), vec![metadata, value], arr.nulls().cloned()))
    }

    remap_batch_columns(batch, |idx, _field, col| {
        let Some(target_field) = target_schema.fields().get(idx) else { return Ok(None) };
        if !is_variant_type(target_field.data_type()) {
            return Ok(None);
        }
        // Downcasts are guarded by the `DataType::*` match arm above. If Arrow ever
        // returns a different concrete array for the same logical type, surface as
        // a DataFusionError instead of panicking on the INSERT path.
        let name = target_field.name();
        let bad_downcast = |ty: &str| DataFusionError::Execution(format!("{ty} downcast failed for column {name}"));
        let converted = match col.data_type() {
            DataType::Utf8View => utf8_to_variant(col.as_any().downcast_ref::<StringViewArray>().ok_or_else(|| bad_downcast("Utf8View"))?.iter())?,
            DataType::Utf8 => utf8_to_variant(col.as_any().downcast_ref::<StringArray>().ok_or_else(|| bad_downcast("Utf8"))?.iter())?,
            DataType::LargeUtf8 => utf8_to_variant(col.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| bad_downcast("LargeUtf8"))?.iter())?,
            _ => return Ok(None), // already Variant struct
        };
        Ok(Some((target_field.clone(), Arc::new(converted) as arrow::array::ArrayRef)))
    })
}

// Fallback ZSTD level when a configured/tier level is rejected as out-of-range.
const ZSTD_COMPRESSION_LEVEL: i32 = 3;
// Parquet footer key-value metadata key recording the ZSTD level used to
// write the file. Read by `recompress_partition` to skip files already
// at-or-above the target tier without rewriting.
/// What a `recompress_partition` call actually did. The CLI used to print
/// "rewritten" unconditionally, including on the tier-probe skip — which made a poisoned partition
/// look repaired while nothing had changed. A skip must say so.
#[derive(Debug)]
pub enum RecompressOutcome {
    Rewritten { files: usize },
    Skipped(&'static str),
}

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
    /// One RuntimeEnv/memory pool shared by every session context and clone —
    /// the pool only enforces a global cap if it's global.
    runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    // The next five runtime envs are disjoint pool slices (constant total
    // budget) so one workload's long sorts can never starve another's.
    /// Heavy maintenance (optimize/dedup/recompress): bounded FairSpill pool +
    /// spill dir so a Z-order global sort can always reserve its merge floor
    /// instead of losing the race for the saturated query pool.
    maintenance_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Hot-tail packing: keeps today's compaction reserve while heavy rewrites
    /// hold the maintenance pool for minutes.
    light_optimize_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Footer repair: sorts whole multi-hundred-MB files; sharing packing's
    /// pool meant stopping packing outright for the duration.
    repair_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Coordinator execution units: one process-wide 512 MiB spill pool. Reuse
    /// stops a fresh runtime treating another worker's live spill dir as an
    /// orphan; the pool is the hard aggregate ceiling across slice workers.
    coordinator_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Flush-path sorts for oversized buckets: flush is on the INGEST path and
    /// must not queue behind maintenance. Bounded + spillable because the
    /// in-process sort allocates outside every pool.
    flush_sort_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Caps concurrent spilling flush sorts. FairSpill gives each ~pool/N;
    /// below a viable slice the sort fails, the group is written unsorted, and
    /// a footer with no `sorting_columns` disables ordering for every scan of
    /// that partition — starvation shows up as slow queries, not errors.
    flush_sort_gate: Arc<tokio::sync::Semaphore>,
    /// Memoized `build_optimize_session_state` per runtime env (building one
    /// re-registers every rule and UDF). A clone shares its `catalog_list`, so
    /// sites that register temporary tables must still build a fresh state.
    maintenance_session_state: Arc<std::sync::OnceLock<datafusion::execution::session_state::SessionState>>,
    light_optimize_session_state: Arc<std::sync::OnceLock<datafusion::execution::session_state::SessionState>>,
    /// Repair-only sessions: same pool, smaller batches, keyed by the sort
    /// parallelism they pin.
    repair_session_states: Arc<dashmap::DashMap<usize, datafusion::execution::session_state::SessionState>>,
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables: UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables: CustomProjectTables,
    /// Lock-free (project, table) → resolved Delta table cache. The inner
    /// `Arc<RwLock<DeltaTable>>` is the same object held in the table maps
    /// above, so slow-path `update_state` is seen by hot-path callers.
    /// No eviction: grows with unique pairs seen since process start.
    fast_resolve_cache: FastResolveCache,
    /// Per-(project, table) sticky "Delta may hold matching files" bit — never
    /// falsely `false`. While false, scans skip Delta and MemBuffer is
    /// authoritative. `Arc<AtomicBool>` because `Database` derives `Clone`.
    delta_has_files: dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicBool>>,
    /// Cached Delta-side `TableProvider` per (project, table) + snapshot
    /// version. Exact-version invalidation; concurrent misses single-flight
    /// via a per-key `OnceCell`. No drop eviction.
    delta_provider_cache: DeltaProviderCache,
    /// Cumulative scan-path counters, exported via `timefusion_stats`.
    pub scan_metrics: Arc<ScanMetrics>,
    batch_queue: Option<Arc<crate::write::BatchQueue>>,
    maintenance_shutdown: Arc<CancellationToken>,
    /// Cancels `maintenance_shutdown` when the last guard-holding clone drops.
    /// `None` in clones handed to long-lived background tasks — a task waiting
    /// on the token must not hold its own kill-switch alive.
    _maintenance_cancel_guard: Option<Arc<tokio_util::sync::DropGuard>>,
    /// One-shot guard so a second `preload_tables` call can't double the
    /// boot-time S3 warm burst.
    preload_started: Arc<std::sync::atomic::AtomicBool>,
    /// Barrier set after startup discovery + footer warming. Maintenance waits
    /// on it so it's never the first cold-loader of a table foreground needs.
    preload_complete: Arc<std::sync::atomic::AtomicBool>,
    preload_complete_notify: Arc<tokio::sync::Notify>,
    /// Runtime for CPU/IO-heavy startup and coordinator work; foreground
    /// PGWire tasks never execute here.
    maintenance_executor: Arc<std::sync::OnceLock<tokio::runtime::Handle>>,
    config_pool: Option<PgPool>,
    storage_configs: Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    /// Monotonic deadline (ns since process start) throttling storage-config
    /// refreshes to ≤ every 30s, so a hot SQL path doesn't hit PG per statement.
    storage_configs_next_refresh_ns: Arc<std::sync::atomic::AtomicU64>,
    default_s3_bucket: Option<String>,
    default_s3_prefix: Option<String>,
    default_s3_endpoint: Option<String>,
    object_store_cache: Option<Arc<SharedFoyerCache>>,
    statistics_extractor: Arc<DeltaStatisticsExtractor>,
    last_written_versions: Arc<RwLock<HashMap<(String, String), u64>>>,
    /// Delta version at last dedup sweep per scheduler key; unmoved version →
    /// skip (no commits, no new dupes). Unbounded like `last_written_versions`.
    last_dedup_versions: Arc<RwLock<HashMap<String, u64>>>,
    /// (project, table, date) → live-file-set fingerprint captured when a sweep
    /// found zero duplicates. A query whose window partitions all match the
    /// current snapshot provably reads no Delta duplicates, so `DedupExec` can
    /// be skipped. Any commit changes the file set → mismatch → dedup stays on.
    dedup_clean_fp: Arc<dashmap::DashMap<(String, String, String), Certification>>,
    /// Serializes `dedup_clean_fp` snapshots with their shared atomic temp path.
    dedup_certification_persist_lock: Arc<std::sync::Mutex<()>>,
    /// Clean-slice coverage accumulating toward a `dedup_clean_fp` entry.
    /// See `SliceCoverage` and `record_clean_slice`.
    dedup_slice_coverage: Arc<dashmap::DashMap<(String, String, String), SliceCoverage>>,
    /// Monotonic invalidation epoch for each source `(project, table, date)`.
    rollup_source_epochs: Arc<dashmap::DashMap<RollupSourceKey, u64>>,
    /// Certified rollup generations keyed by `(project, source, target, date)`.
    ///
    /// **Has no producer.** Every use of this field in the crate is a read
    /// (`mod.rs` routing lookup) or a removal (`maintain.rs` x2) — there is no
    /// `insert`, `entry` or `or_insert` anywhere, so on a private field it is
    /// permanently empty and the date-level routing lookup returns `None` for
    /// every date on every process. `maintenance_coordinator.rs` still claims
    /// "`recover_rollup_coverage` is the only producer"; that function writes
    /// only `rollup_slice_coverage`. The insert was lost in a refactor and two
    /// comments outlived it.
    ///
    /// Not a wrong-answer bug — the `None` branch `continue`s without setting a
    /// miss, so queries fall through to slice coverage and are correct, merely
    /// unrouted. Which is why it left no trace. See
    /// `rollup_coverage_entries` and
    /// `docs/plans/2026-08-22-query-latency-matrix.md`.
    rollup_coverage: Arc<dashmap::DashMap<RollupCoverageKey, RollupCoverage>>,
    rollup_slice_coverage: Arc<dashmap::DashMap<RollupSliceCoverageKey, RollupCoverage>>,
    /// Untagged live files per tier TABLE (tiers publish independently, so one
    /// shared slot would read clean while another tier still held damage).
    /// The exported gauge is the SUM over tiers.
    rollup_tier_untagged: Arc<dashmap::DashMap<String, u64>>,
    /// 24-bit changed-hours mask per `(project, source, date)`. PRESENCE claims
    /// every change since the last build was observed; absence means "unknown"
    /// and forces a full rebuild. Only a successful build inserts one.
    rollup_dirty: Arc<dashmap::DashMap<(String, String, String), u32>>,
    /// First durable invalidation time per dirty source partition; kept apart
    /// from the mask so a clean zero-mask entry has no age.
    rollup_invalidated_at: Arc<dashmap::DashMap<RollupSourceKey, u64>>,
    /// Serializes dirty-map mutation with journal snapshots, else two writers
    /// can persist out of order and lose the newer invalidation on restart.
    rollup_journal_lock: Arc<std::sync::Mutex<()>>,
    /// Durable slice work from the same pre-ack invalidation path as
    /// `rollup_dirty`; the finer-grained source of truth coordinator workers consume.
    maintenance_tasks: Arc<std::sync::Mutex<crate::maintenance_coordinator::TaskJournal>>,
    maintenance_admission: crate::maintenance_coordinator::AdmissionController,
    maintenance_debt_planned_at: Arc<std::sync::atomic::AtomicI64>,
    /// Last Tantivy coverage census. Metadata-only, so it is throttled by time
    /// rather than by admission.
    tantivy_census_at: Arc<std::sync::atomic::AtomicI64>,
    maintenance_schedule_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Bounded exponential retry state for failed source-partition rollup builds.
    rollup_backoff: Arc<dashmap::DashMap<RollupCoverageKey, (u32, std::time::Instant)>>,
    /// Exact merge-on-read count partitions. Query threads use only the
    /// process-local front; disk/Delta loads are single-flight and bounded in
    /// the background so a cold cache cannot amplify query load.
    logical_count_cache: Arc<crate::read::LogicalCountCache>,
    logical_count_building: Arc<dashmap::DashSet<crate::read::CountPartition>>,
    logical_count_build_sem: Arc<tokio::sync::Semaphore>,
    /// Dirty `(project, table, date, 10-minute bin)` keys recorded only after a
    /// Delta append commits. In-memory by design: after restart the read-side
    /// DedupExec remains the correctness backstop.
    dedup_dirty_bins: Arc<dashmap::DashMap<(String, String, String, i64), ()>>,
    /// Exponential failure backoff per dedup target, else a failing partition
    /// re-runs every sweep tick. In-memory only; a restart retries once.
    dedup_backoff: Arc<dashmap::DashMap<String, (u32, std::time::Instant)>>,
    /// Repair candidates whose footer proved sorted, excluded after one footer
    /// read per process. The `delta-rs.optimize.sort_by` tag ≠ a
    /// `sorting_columns` footer (flush sorts without the tag), so untagged
    /// means suspect, not unsorted — admission by tag would rewrite healthy files.
    repair_verified_sorted: Arc<dashmap::DashSet<String>>,
    /// Serializes appends to the persisted verified-sorted list.
    repair_verified_lock: Arc<std::sync::Mutex<()>>,
    /// Consecutive staging failures per repair candidate — an always-failing
    /// file starves every candidate behind it; see `REPAIR_QUARANTINE_AFTER`.
    repair_failures: Arc<dashmap::DashMap<String, u32>>,
    /// Index into [`REPAIR_SORT_PARTITION_LADDER`] for candidates that
    /// exhausted the pool at higher parallelism; cleared on success.
    repair_degradation: Arc<dashmap::DashMap<String, usize>>,
    /// Caps concurrent heavy rewrites (dedup staging, optimize, consolidate,
    /// recompress). Their Arrow footprint is invisible to the DataFusion memory
    /// pool, so aggregate concurrency, not the pool, is the real OOM bound.
    /// Hot-tail waves deliberately use [`Self::light_rewrite_sem`] instead.
    maintenance_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps hot-tail wave staging. Separate from `maintenance_rewrite_sem` so a
    /// long dedup drain can't starve hot compaction (disjoint partitions, so
    /// serializing them is pure loss). Sized to the light pool's own K.
    light_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps coordinator workers in debt work (dedup, packing, consolidation,
    /// repair), reserving the rest for rollup. Bounds occupancy, not memory:
    /// debt units hold a worker for minutes and would otherwise starve cheap
    /// rollup units regardless of attempt share.
    maintenance_debt_slots: Arc<tokio::sync::Semaphore>,
    /// Caps workers inside units already timed out under
    /// `TaskJournal::QUARANTINE_ATTEMPTS`: bounds the wall-clock cost of a
    /// doomed unit, not its frequency.
    maintenance_quarantine_slots: Arc<tokio::sync::Semaphore>,
    /// Reserves workers for `DerivedRollup` while coverage is short — slow
    /// sealed `BaseRollup` days would otherwise occupy every freed slot while
    /// cheap derived work (the actual coverage gap) starves.
    maintenance_derived_reserve: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent user DML MERGE-UPDATEs; each scans the time-windowed
    /// target and ungated bursts starve reads. Permits =
    /// `timefusion_dml_merge_concurrency`.
    dml_merge_sem: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent Parquet decodes for WIDE scans across all queries
    /// (`timefusion_max_concurrent_scan_readers`), so a burst of wide-window
    /// dashboards can't stack decode buffers into an OOM.
    heavy_scan_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes the outer full and light maintenance jobs; rewrite permits
    /// alone let a waiting light job exhaust its table timeout before starting.
    maintenance_job_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes in-process Delta commits per physical table (`table_lock_key`).
    /// delta-kernel's OCC checker can't evaluate `replace_where`'s timestamp
    /// predicate, so a dedup commit racing a concurrent append to the same log
    /// aborts; per-log serialization lets the rebase skip the checker.
    commit_locks: DmlLocks,
    /// Flush/ingest committers queued on each table's `commit_locks` entry.
    /// Durability outranks maintenance: `commit_wave` declines to enqueue while
    /// nonzero, bounding flush latency to one in-flight wave commit.
    flush_waiter_counts: FlushWaiterCounts,
    /// Per-table serialization for in-process DML (see `dml_lock`): concurrent
    /// merges would OCC-conflict and redo full rewrites. Queuing here leaves
    /// the table's RwLock free for readers and insert commits.
    dml_locks: DmlLocks,
    /// Last snapshot-persist time per table URL; throttles `persist_snapshot`.
    /// The on-disk snapshot is only a boot-recovery seed, so staleness just
    /// means boot replays a few more sub-second commits.
    snapshot_persist_gate: Arc<dashmap::DashMap<String, std::time::Instant>>,
    /// Late-binding shared cell: boot creates the pgwire SessionContext before
    /// the layer exists, so the layer is published through a OnceLock visible
    /// to clones captured earlier (e.g. DmlQueryPlanner). A plain Option left
    /// those clones without the mem leg, losing updates to unflushed rows.
    buffered_layer: Arc<std::sync::OnceLock<Arc<crate::write::BufferedWriteLayer>>>,
    /// Per-clone override for `query_delta_only`: hides the shared layer so
    /// scans bypass the in-memory buffer.
    bypass_buffer: bool,
    /// Internal aggregate builds must never read the rollup they are rebuilding.
    bypass_rollup: bool,
    /// Plan the scan with maintenance parallelism instead of query parallelism:
    /// Parquet decode buffers are untracked by the memory pool, so planning a
    /// background rewrite at the full CPU quota fanned out decode into OOMs.
    maintenance_scan: bool,
    /// Same late-binding pattern as `buffered_layer`: attached by `with_*`
    /// builders after boot has already cloned Database into sessions/planners.
    tantivy_search: Arc<std::sync::OnceLock<Arc<crate::tantivy::search::TantivySearchService>>>,
    tantivy_indexer: Arc<std::sync::OnceLock<Arc<crate::tantivy::search::TantivyIndexService>>>,
    bloom_prune: Arc<std::sync::OnceLock<Arc<crate::read::bloom_prune::BloomPruneRegistry>>>,
    /// Same late-binding pattern; populated by `start_dml_coalescer` when
    /// `TIMEFUSION_DML_COALESCE_SECS > 0`.
    dml_coalescer: Arc<std::sync::OnceLock<Arc<crate::dml::DmlCoalescer>>>,
    /// Live file URIs per (table URL, date) at the last full z-order optimize.
    /// delta-rs's ZOrder planner has no idempotence guard, so this lets
    /// `optimize_table` skip sealed partitions whose file set is unchanged.
    /// In-memory only; a restart re-z-orders each partition once, harmlessly.
    zorder_filesets: ZOrderFilesets,
    /// Last checkpointed version per table URL, letting the out-of-band
    /// checkpoint task (with `checkpoint_after_waves`, the only checkpoint
    /// drivers) skip idle tables. In-memory only; the first tick after restart
    /// checkpoints every table once, harmlessly.
    checkpoint_versions: Arc<dashmap::DashMap<String, u64>>,
    /// Serializes staged-intent manifest append/rewrite: orphan reconciliation's
    /// compacting rewrite racing a wave's append could drop the only
    /// targeted-cleanup record for a later-orphaned parquet file.
    staged_intent_manifest_lock: Arc<std::sync::Mutex<()>>,
    // Rotation cursors: each pass's budget is smaller than its work list, so
    // without rotating the start point the tail would never be reached.
    /// Index into the light-optimize tick's debt-ordered project list.
    light_optimize_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Count of (date, project) items served by the last truncated dedup sweep.
    dedup_sweep_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Which table a dedup tick starts with.
    dedup_table_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Which table a hot-compaction tick starts with.
    hot_compact_table_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// One repair pass process-wide: the light pool is shared by every table,
    /// and two tables repairing concurrently can exhaust it and kill a bin
    /// that still had headroom.
    repair_pass_permit: Arc<tokio::sync::Semaphore>,
}

fn sort_backfill_uris_newest_first(uris: &mut [String]) {
    // Partition paths contain `date=YYYY-MM-DD`, so reverse lexical order is
    // chronological newest-first.
    uris.sort_by(|a, b| b.cmp(a));
}

type TantivyBackfillWork = (String, String, String); // project, relative parquet, absolute URI

/// `fair_tantivy_backfill_work`, but reserving `tail_pct` of the pass for the
/// OLDEST uncovered files instead of spending all of it on the newest.
///
/// Newest-first plus a per-pass cap starves the tail outright, and prod proved
/// it rather than merely risking it (2026-08-22): the census breakdown held
/// `week` and `older` at 1723/3459 across three consecutive samples — frozen to
/// within one file — while `today` climbed 586 → 624 → 643. With a 150-file cap
/// and 624 uncovered files in today's partition alone, a pass can never reach
/// yesterday, and today's count is replenished continuously by flush and
/// hot-tail compaction. More throughput just chases churn faster; only reserving
/// part of the pass makes the 5,182-file backlog finite.
///
/// The tail is built by running the same fair round-robin over reversed queues,
/// so oldest-first is fair across projects too — one deep-history project cannot
/// own the whole reservation.
fn fair_tantivy_backfill_work_split(queues: Vec<(String, VecDeque<(String, String)>)>, cap: usize, tail_pct: u8) -> Vec<TantivyBackfillWork> {
    let tail_n = (cap > 0).then(|| cap * usize::from(tail_pct.min(100)) / 100).filter(|n| *n > 0);
    let Some(tail_n) = tail_n else {
        let mut work = fair_tantivy_backfill_work(queues);
        if cap > 0 {
            work.truncate(cap);
        }
        return work;
    };
    let oldest_first: Vec<_> = queues.iter().map(|(p, q)| (p.clone(), q.iter().rev().cloned().collect())).collect();
    let mut work = fair_tantivy_backfill_work(queues);
    // Head keeps the hot window converging; the reservation is carved out of the
    // cap, never added to it, so the pass cost is unchanged.
    let head_n = cap.saturating_sub(tail_n);
    let tail: Vec<_> = {
        let head_uris: HashSet<&str> = work.iter().take(head_n).map(|(_, _, uri)| uri.as_str()).collect();
        fair_tantivy_backfill_work(oldest_first).into_iter().filter(|(_, _, uri)| !head_uris.contains(uri.as_str())).take(tail_n).collect()
    };
    // INTERLEAVE, do not append. A pass is routinely killed long before it
    // finishes — prod restarts every 15-30 min against a ~1.5h pass — so work
    // placed at the END is the first thing lost. Appending the reservation
    // reproduced the very starvation it exists to prevent, one level down:
    // measured 2026-08-22 on a live pass, `older` had not moved off 3456 after
    // 33 minutes because execution was still working through the newest 100.
    // Interleaved, the tail is attempted from the first minute.
    work.truncate(head_n);
    let (mut head, mut tail): (VecDeque<_>, VecDeque<_>) = (work.into(), tail.into());
    let per_tail = head.len().div_ceil(tail.len().max(1)).max(1);
    let mut out = Vec::with_capacity(head.len() + tail.len());
    while !head.is_empty() || !tail.is_empty() {
        out.extend((0..per_tail).filter_map(|_| head.pop_front()));
        out.extend(tail.pop_front());
    }
    out
}

/// A file's decoded contribution to a slice, scaled by the share of its time span the slice covers.
///
/// Counting the whole file for any overlap made splitting a no-op. A narrow slice of a day-spanning
/// file decodes only the overlapping row groups. Files with no usable timestamp bounds keep their
/// full weight (unknown must never estimate cheap), and the fraction is clamped to at least one row
/// group's worth so a narrow slice is never estimated as free. The row-group count is deliberately
/// conservative: too few row groups makes the floor larger, which over-estimates, the safe direction.
fn estimated_row_groups(size_bytes: i64) -> u64 {
    const TARGET_ROW_GROUP_BYTES: i64 = 128 * 1024 * 1024;
    u64::try_from(size_bytes.max(0).div_euclid(TARGET_ROW_GROUP_BYTES).max(1)).unwrap_or(1)
}

fn slice_share_of_file(file_min: Option<i64>, file_max: Option<i64>, slice: crate::maintenance_coordinator::TimeSlice, row_groups: u64) -> (u64, u64) {
    let (Some(min), Some(max)) = (file_min, file_max) else { return (1, 1) };
    let span = max.saturating_sub(min).saturating_add(1);
    if span <= 0 {
        return (1, 1);
    }
    let overlap = slice.end_micros.min(max.saturating_add(1)).saturating_sub(slice.start_micros.max(min));
    if overlap <= 0 {
        return (0, 1);
    }
    let floor = span.div_euclid(i64::try_from(row_groups.max(1)).unwrap_or(1)).max(1);
    (u64::try_from(overlap.max(floor).min(span)).unwrap_or(1), u64::try_from(span).unwrap_or(1))
}

fn fair_tantivy_backfill_work(mut queues: Vec<(String, VecDeque<(String, String)>)>) -> Vec<TantivyBackfillWork> {
    // HashMap iteration is deliberately unstable. Pin project order so repair
    // is reproducible, then take one newest file from every project per round —
    // a stable sort by per-queue position keeps project order within a round.
    queues.sort_by(|a, b| a.0.cmp(&b.0));
    queues
        .into_iter()
        .flat_map(|(project, queue)| queue.into_iter().enumerate().map(move |(round, (rel, uri))| (round, (project.clone(), rel, uri))))
        .sorted_by_key(|(round, _)| *round)
        .map(|(_, work)| work)
        .collect()
}

/// What a single `run-unit` execution produced. The phase counters are deltas
/// of the global `maintenance_stats` atomics over the unit's run; the CLI owns
/// the process, so nothing else is moving them.
#[derive(derive_more::Display)]
#[display(
    "run-unit: {operation:?} {project_id} {date} | wall {wall_ms}ms | scan {scan_ms}ms staging {staging_ms}ms commit {commit_ms}ms e2e {end_to_end_ms}ms | cohorts {cohorts} | state {state:?}{}",
    retry_reason.as_deref().map_or_else(String::new, |reason| format!(" | retry_reason {reason}"))
)]
pub struct UnitRunReport {
    pub operation: crate::maintenance_coordinator::Operation,
    pub project_id: String,
    pub date: chrono::NaiveDate,
    pub wall_ms: u64,
    pub scan_ms: u64,
    pub staging_ms: u64,
    pub commit_ms: u64,
    pub end_to_end_ms: u64,
    pub cohorts: u64,
    pub state: Option<crate::maintenance_coordinator::TaskState>,
    pub retry_reason: Option<String>,
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

    /// Lock the maintenance task journal, recovering from mutex poisoning.
    fn journal(&self) -> std::sync::MutexGuard<'_, crate::maintenance_coordinator::TaskJournal> {
        self.maintenance_tasks.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// All maintenance targets (unified + custom project tables) as `(name, table)` pairs,
    /// for cron jobs that sweep every table.
    async fn all_maintenance_targets(&self) -> Vec<(String, Arc<RwLock<DeltaTable>>)> {
        let mut targets: Vec<(String, Arc<RwLock<DeltaTable>>)> = self.unified_tables.read().await.iter().map(|(n, t)| (n.clone(), t.clone())).collect();
        targets.extend(self.custom_project_tables.read().await.iter().map(|((_, n), t)| (n.clone(), t.clone())));
        targets
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

    /// Creates writer properties for a Delta write at a compression tier.
    ///
    /// Hot writes use zstd level 3 for fast ingest; cold rewrites use 9/15/19 for storage
    /// savings. The chosen level is stored in footer metadata so re-sweeps can skip
    /// already-target-tier files. Encoding is tuned per column: delta packing for
    /// timestamps/ints, delta byte arrays for sorted strings, dictionary by default for other
    /// strings with per-field opt-out, and bloom filters opt-in for point-lookup columns.
    /// `declare_sorted` is `true` only for paths that sort rows by the schema's sort keys
    /// (flush, dedup); optimize/compact pass `false`.
    fn create_writer_properties(&self, schema: &crate::schema::TableSchema, zstd_level: i32, declare_sorted: bool) -> WriterProperties {
        build_writer_properties(&self.config.parquet, schema, zstd_level, declare_sorted)
    }

    /// WriterProperties for DML rewrite paths.
    ///
    /// delta-rs defaults to SNAPPY, which inflates storage and forces daily recompress to zstd.
    /// `sorted` must match the caller's actual plan because the footer is trusted, not verified:
    /// `true` only for the DV-merge path (`perform_delta_merge_update` appends rows already
    /// sorted by the schema's sort order); `false` for `UpdateBuilder`/`DeleteBuilder` because
    /// their output is scan order and a lying footer breaks `DedupExec`'s bounded mode.
    pub(crate) fn dml_writer_properties(&self, table_name: &str, sorted: bool) -> WriterProperties {
        let schema = schema_or_default(table_name);
        self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted)
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
                        versions.insert(table_key(project_id, table_name), version);
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
                    tokio::time::sleep(occ_backoff(retries as usize)).await;
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

        let key_set = crate::config::key_configured();
        let mut map = HashMap::new();
        let mut plaintext_rows = 0usize;
        'rows: for mut config in configs {
            let all_encrypted = [&config.s3_access_key_id, &config.s3_secret_access_key].into_iter().all(|v| v.starts_with(crate::config::ENC_PREFIX));
            let (project_id, table_name) = (config.project_id.clone(), config.table_name.clone());
            for (name, field) in [("s3_access_key_id", &mut config.s3_access_key_id), ("s3_secret_access_key", &mut config.s3_secret_access_key)] {
                match crate::config::decrypt_or_passthrough(field) {
                    Ok(v) => *field = v,
                    Err(e) => {
                        error!("Skipping {project_id}/{table_name}: cannot decrypt {name}: {e}");
                        continue 'rows;
                    }
                }
            }
            plaintext_rows += usize::from(!all_encrypted);
            debug!("Loaded config: {project_id}/{table_name}");
            map.insert((project_id, table_name), config);
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
        crate::storage::prune_stale(&Self::delta_snapshot_dir(&cfg), crate::storage::SNAPSHOT_MAX_AGE);
        let dedup_dirty_bins = Arc::new(dashmap::DashMap::new());
        for bin in crate::storage::load_sidecar::<crate::storage::DirtyBin>(&cfg.core.timefusion_data_dir, crate::storage::DIRTY_BINS) {
            dedup_dirty_bins.insert((bin.project_id, bin.table_name, bin.date, bin.bin), ());
        }
        crate::observability::maintenance_stats().dirty_bin_queue_depth.store(dedup_dirty_bins.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let rollup_dirty = Arc::new(dashmap::DashMap::new());
        let rollup_source_epochs = Arc::new(dashmap::DashMap::new());
        let rollup_invalidated_at = Arc::new(dashmap::DashMap::new());
        let mut maintenance_tasks = crate::maintenance_coordinator::TaskJournal::load(&cfg.core.timefusion_data_dir)?;
        let requeued_tasks = maintenance_tasks.requeue_running(crate::support::now_micros());
        if requeued_tasks != 0 {
            maintenance_tasks.checkpoint()?;
            info!(requeued_tasks, event = "maintenance_tasks_requeued");
        }
        for entry in crate::rollup_journal::load(&cfg.core.timefusion_data_dir) {
            let key = (entry.project_id, entry.source, entry.date);
            rollup_source_epochs.insert(key.clone(), entry.epoch);
            let dirty = if entry.unknown { crate::rollup::ALL_HOURS } else { entry.dirty_hours };
            if dirty != 0 && entry.invalidated_unix_ms != 0 {
                rollup_invalidated_at.insert(key.clone(), entry.invalidated_unix_ms);
            }
            rollup_dirty.insert(key, dirty);
        }
        let rollup_stats = crate::observability::maintenance_stats();
        rollup_stats
            .rollup_dirty_partitions
            .store(rollup_dirty.iter().filter(|entry| *entry.value() != 0).count() as u64, std::sync::atomic::Ordering::Relaxed);
        let now_unix_ms = crate::storage::now_unix_ms();
        rollup_stats.rollup_oldest_invalidation_age_secs.store(
            rollup_invalidated_at.iter().map(|entry| now_unix_ms.saturating_sub(*entry.value()) / 1_000).max().unwrap_or(0),
            std::sync::atomic::Ordering::Relaxed,
        );
        // Reload sweep certifications so the read-side dedup skip does not start
        // from cold after every deploy. Loading cannot grant a skip on its own:
        // `dedup_window_clean` still checks each entry's fingerprint against the
        // live file list, so a record that has gone stale simply fails there.
        let dedup_clean_fp: Arc<dashmap::DashMap<(String, String, String), Certification>> = Arc::new(dashmap::DashMap::new());
        if cfg.maintenance.timefusion_dedup_certification_persist {
            let now = std::time::Instant::now();
            for entry in crate::storage::load_sidecar::<crate::storage::StoredCertification>(&cfg.core.timefusion_data_dir, crate::storage::CERTIFICATIONS) {
                // Rebuild the monotonic instant from the stored wall-clock age, so
                // dwell keeps measuring the certification's real lifetime instead
                // of restarting at this boot. A clock that moved backwards (or an
                // entry from the future) falls back to "granted now".
                let since = crate::storage::age_since(entry.granted_unix_ms).and_then(|age| now.checked_sub(age)).unwrap_or(now);
                dedup_clean_fp.insert(
                    (entry.project_id, entry.table_name, entry.date),
                    Certification { fp: entry.fp, since, files: Arc::from(entry.files), stale: false },
                );
            }
            info!(loaded = dedup_clean_fp.len(), event = "dedup_certifications_loaded");
        }
        // Reload accumulated slice coverage for the same reason — the journal
        // durably skips completed slices, so losing their evidence on restart
        // makes any day that straddles one permanently uncertifiable. Stale
        // entries are harmless: `record_clean_slice` recomputes the live fp on
        // every merge and the grant path re-checks it one final time.
        let dedup_slice_coverage: Arc<dashmap::DashMap<(String, String, String), SliceCoverage>> = Arc::new(dashmap::DashMap::new());
        if cfg.maintenance.timefusion_dedup_certification_persist {
            for e in crate::storage::load_sidecar::<crate::storage::StoredSliceCoverage>(&cfg.core.timefusion_data_dir, crate::storage::SLICE_COVERAGE) {
                dedup_slice_coverage.insert((e.project_id, e.table_name, e.date), SliceCoverage { fp: e.fp, intervals: e.intervals });
            }
            info!(loaded = dedup_slice_coverage.len(), event = "dedup_slice_coverage_loaded");
        }
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
        // In UNITS, not polls: one reader slot is `DECODE_UNITS_PER_READER`
        // units and a worst-case batch claims all of them, so the heap ceiling
        // is identical to the poll-counting version. See `decode_units`.
        let heavy_scan_permits = cfg.memory.timefusion_max_concurrent_scan_readers.max(1) * DECODE_UNITS_PER_READER as usize;
        let flush_sort_permits = flush_sort_permits(cfg.maintenance.flush_sort_pool_bytes());
        let maintenance_shutdown = CancellationToken::new();
        let maintenance_cancel_guard = Arc::new(maintenance_shutdown.clone().drop_guard());
        // Admission stays narrow (this is a single-node foreground DB — dedup,
        // rollup and compaction must not each open an object-store stream and
        // saturate PGWire) but NOT serial: one-unit-at-a-time left the queue at
        // 20 TB while dedup committed nothing. Sized from the box; each unit is
        // capped at MAX_DECODED_BYTES so the memory term is exact. Object I/O is
        // latency-bound, so it gets more slots than CPU.
        let coordinator_jobs = cfg.derived.coordinator_jobs();
        let coordinator_io_slots = u32::try_from(coordinator_jobs.saturating_mul(2)).unwrap_or(u32::MAX);
        let maintenance_admission = crate::maintenance_coordinator::AdmissionController::new(
            u32::try_from(coordinator_jobs).unwrap_or(1),
            u64::try_from(cfg.derived.memory_limit_bytes).unwrap_or(u64::MAX),
            coordinator_io_slots,
            coordinator_io_slots,
        );
        let logical_count_cache =
            Arc::new(crate::read::LogicalCountCache::new(cfg.core.timefusion_data_dir.join("logical_count"), cfg.derived.logical_count_memory_bytes()));
        let db = Self {
            config: cfg,
            runtime_env: Arc::new(std::sync::OnceLock::new()),
            maintenance_runtime_env: Arc::new(std::sync::OnceLock::new()),
            light_optimize_runtime_env: Arc::new(std::sync::OnceLock::new()),
            repair_runtime_env: Arc::new(std::sync::OnceLock::new()),
            coordinator_runtime_env: Arc::new(std::sync::OnceLock::new()),
            flush_sort_gate: Arc::new(tokio::sync::Semaphore::new(flush_sort_permits)),
            flush_sort_runtime_env: Arc::new(std::sync::OnceLock::new()),
            maintenance_session_state: Arc::new(std::sync::OnceLock::new()),
            light_optimize_session_state: Arc::new(std::sync::OnceLock::new()),
            repair_session_states: Arc::new(dashmap::DashMap::new()),
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
            preload_complete: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            preload_complete_notify: Arc::new(tokio::sync::Notify::new()),
            maintenance_executor: Arc::new(std::sync::OnceLock::new()),
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
            dedup_clean_fp,
            dedup_certification_persist_lock: Arc::new(std::sync::Mutex::new(())),
            dedup_slice_coverage,
            rollup_source_epochs,
            rollup_coverage: Arc::new(dashmap::DashMap::new()),
            rollup_slice_coverage: Arc::new(dashmap::DashMap::new()),
            rollup_tier_untagged: Arc::new(dashmap::DashMap::new()),
            rollup_dirty,
            rollup_invalidated_at,
            rollup_journal_lock: Arc::new(std::sync::Mutex::new(())),
            maintenance_tasks: Arc::new(std::sync::Mutex::new(maintenance_tasks)),
            maintenance_admission,
            maintenance_debt_planned_at: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            tantivy_census_at: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            maintenance_schedule_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            rollup_backoff: Arc::new(dashmap::DashMap::new()),
            logical_count_cache,
            logical_count_building: Arc::new(dashmap::DashSet::new()),
            // A build retains one winner per logical key. Serial construction
            // is deliberate at production cardinality; query execution and
            // the other cache tiers remain concurrent.
            logical_count_build_sem: Arc::new(tokio::sync::Semaphore::new(1)),
            dedup_dirty_bins,
            dedup_backoff: Arc::new(dashmap::DashMap::new()),
            repair_verified_sorted: Arc::new(dashmap::DashSet::new()),
            repair_verified_lock: Arc::new(std::sync::Mutex::new(())),
            repair_failures: Arc::new(dashmap::DashMap::new()),
            repair_degradation: Arc::new(dashmap::DashMap::new()),
            maintenance_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(maint_rewrite_permits)),
            light_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(light_rewrite_permits)),
            // Three quarters to debt: a quarter always free for the rollup
            // chain, but not less — ungoverned file counts are their own outage.
            maintenance_debt_slots: Arc::new(tokio::sync::Semaphore::new((coordinator_jobs * 3 / 4).max(1))),
            // An eighth, not zero: transiently timed-out units must still get
            // turns, and a genuinely expensive rollup is what the 30d goal needs.
            maintenance_quarantine_slots: Arc::new(tokio::sync::Semaphore::new((coordinator_jobs / 8).max(1))),
            // Two workers held open for derived work, expressed as the permits
            // everything else shares. Never zero: tiny boxes must still progress.
            maintenance_derived_reserve: Arc::new(tokio::sync::Semaphore::new(coordinator_jobs.saturating_sub(2).max(1))),
            dml_merge_sem: Arc::new(tokio::sync::Semaphore::new(dml_merge_permits)),
            heavy_scan_sem: Arc::new(tokio::sync::Semaphore::new(heavy_scan_permits)),
            maintenance_job_sem: Arc::new(tokio::sync::Semaphore::new(1)),
            commit_locks: Arc::new(dashmap::DashMap::new()),
            flush_waiter_counts: Arc::new(dashmap::DashMap::new()),
            dml_locks: Arc::new(dashmap::DashMap::new()),
            snapshot_persist_gate: Arc::new(dashmap::DashMap::new()),
            buffered_layer: Arc::new(std::sync::OnceLock::new()),
            bypass_buffer: false,
            bypass_rollup: false,
            maintenance_scan: false,
            tantivy_search: Arc::new(std::sync::OnceLock::new()),
            tantivy_indexer: Arc::new(std::sync::OnceLock::new()),
            bloom_prune: Arc::new(std::sync::OnceLock::new()),
            dml_coalescer: Arc::new(std::sync::OnceLock::new()),
            zorder_filesets: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_versions: Arc::new(dashmap::DashMap::new()),
            light_optimize_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            dedup_sweep_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            dedup_table_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            hot_compact_table_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            repair_pass_permit: Arc::new(tokio::sync::Semaphore::new(1)),
            staged_intent_manifest_lock: Arc::new(std::sync::Mutex::new(())),
        };

        Ok(db)
    }

    /// Create a new Database using global config (for production).
    /// For tests, prefer `with_config()` to pass config explicitly.
    pub async fn new() -> Result<Self> {
        let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {e}"))?;
        Self::with_config(Arc::new(cfg.clone())).await
    }

    /// Set the batch queue to use for insert operations
    pub fn with_batch_queue(mut self, batch_queue: Arc<crate::write::BatchQueue>) -> Self {
        self.batch_queue = Some(batch_queue);
        self
    }

    /// Set the buffered write layer for WAL + in-memory buffer. Publishes to
    /// every existing clone (shared OnceLock) — set-once; a second call is a
    /// no-op.
    pub fn with_buffered_layer(self, layer: Arc<crate::write::BufferedWriteLayer>) -> Self {
        let _ = self.buffered_layer.set(layer);
        self
    }

    /// Get the buffered write layer if configured
    pub fn buffered_layer(&self) -> Option<&Arc<crate::write::BufferedWriteLayer>> {
        if self.bypass_buffer { None } else { self.buffered_layer.get() }
    }

    /// The deferred-DML coalescer, when enabled (see `start_dml_coalescer`).
    pub fn dml_coalescer(&self) -> Option<&Arc<crate::dml::DmlCoalescer>> {
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
        let coalescer = Arc::new(crate::dml::DmlCoalescer::new(secs, self.config.buffer.dml_coalesce_fold()));
        if self.dml_coalescer.set(coalescer.clone()).is_ok() {
            tokio::spawn(coalescer.run(self.background_clone(), (*self.maintenance_shutdown).clone()));
        }
    }

    /// Attach the tantivy search service used by the scan-side prefilter.
    /// Publishes to every existing clone (shared OnceLock, set-once).
    pub fn with_tantivy_search(self, svc: Arc<crate::tantivy::search::TantivySearchService>) -> Self {
        let _ = self.tantivy_search.set(svc);
        self
    }

    pub fn tantivy_search(&self) -> Option<&Arc<crate::tantivy::search::TantivySearchService>> {
        self.tantivy_search.get()
    }

    /// Attach the write-side tantivy service. Used by the compaction-GC hook
    /// in `optimize_table` to clean up stale sidecar indexes after files are
    /// rewritten away. Publishes to every existing clone (shared OnceLock).
    pub fn with_tantivy_indexer(self, svc: Arc<crate::tantivy::search::TantivyIndexService>) -> Self {
        let _ = self.tantivy_indexer.set(svc);
        self
    }

    pub fn tantivy_indexer(&self) -> Option<&Arc<crate::tantivy::search::TantivyIndexService>> {
        self.tantivy_indexer.get()
    }

    pub fn with_bloom_prune(self, reg: Arc<crate::read::bloom_prune::BloomPruneRegistry>) -> Self {
        let _ = self.bloom_prune.set(reg);
        self
    }

    pub fn bloom_prune(&self) -> Option<&Arc<crate::read::bloom_prune::BloomPruneRegistry>> {
        self.bloom_prune.get().filter(|_| self.config.maintenance.timefusion_file_bloom_pruning)
    }

    /// Startup backfill (gated on `timefusion_tantivy_backfill`): build
    /// partition-mirrored indexes for live parquet files that no successful
    /// manifest entry covers, newest partition first. Every covered file
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
    pub fn spawn_deferred_tantivy_reindex(self: &Arc<Self>, layer: Arc<crate::write::BufferedWriteLayer>) {
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
                    let rel =
                        crate::tantivy::search::parquet_rel_of_uri(&file.uri).ok_or_else(|| anyhow::anyhow!("invalid deferred parquet URI {}", file.uri))?;
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

    /// Make the hot window resident: pull every index blob covering the last
    /// `prefetch_days` into the local extraction cache. Idempotent — blobs
    /// already on disk cost one stat each — so this is safe to run on a cron.
    ///
    /// A named method rather than an inline cron body on purpose: the closure
    /// form borrows the loop's table name across an await inside an HRTB-bound
    /// closure, which rustc cannot prove general enough.
    pub async fn tantivy_warm_hot_window(&self) {
        let days = self.config.tantivy.timefusion_tantivy_prefetch_days;
        let Some(svc) = self.tantivy_search().cloned() else { return };
        if days == 0 {
            return;
        }
        for table in self.config.tantivy.indexed_tables() {
            match svc.warm_recent(&table, days).await {
                Ok(n) => debug!("tantivy hot warm: table={table} days={days} blobs_resident={n}"),
                Err(e) => warn!("tantivy hot warm failed for {table}: {e}"),
            }
        }
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
        for pid in crate::tantivy::list_manifest_projects(svc.object_store.as_ref(), table_name).await? {
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

    /// For one resolved table (root), gather live parquet files, group by
    /// owning project (partition segment), drop files already covered by that
    /// project's tantivy manifest, and drop files over the configured backfill
    /// size cap. Returns per-project uncovered URIs, per-file sizes (for
    /// callers that need them again), and the table's object store; bumps
    /// `*oversized` by the count skipped for size and, when `warn_skipped`,
    /// logs one warning per project with a size-skip.
    async fn group_uncovered_files_by_project(
        &self, svc: &crate::tantivy::search::TantivyIndexService, table_ref: &Arc<RwLock<DeltaTable>>, table_name: &str, oversized: &mut u64,
        warn_skipped: bool,
    ) -> anyhow::Result<(HashMap<String, Vec<String>>, HashMap<String, u64>, Arc<dyn object_store::ObjectStore>)> {
        use crate::tantivy::search::project_id_of_uri;
        let (uris, sizes, delta_store) = {
            let t = table_ref.read().await;
            let sizes: HashMap<String, u64> = match t.snapshot() {
                Ok(s) => s.log_data().iter().map(|f| (f.path().into_owned(), f.size() as u64)).collect(),
                Err(_) => HashMap::new(),
            };
            (t.get_file_uris()?.collect::<Vec<String>>(), sizes, t.log_store().object_store(None))
        };
        let by_pid: HashMap<String, Vec<String>> =
            uris.into_iter().filter(|uri| uri.ends_with(".parquet")).filter_map(|uri| Some((project_id_of_uri(&uri)?.to_string(), uri))).into_group_map();
        let max_bytes = self.config.tantivy.timefusion_tantivy_backfill_max_file_mb * 1024 * 1024;
        let mut result: HashMap<String, Vec<String>> = HashMap::with_capacity(by_pid.len());
        for (pid, mut uris) in by_pid {
            let manifest = crate::tantivy::load_manifest(svc.object_store.as_ref(), table_name, &pid).await?;
            let covered: HashSet<&String> =
                manifest.entries.values().filter(|e| e.index.is_some() && e.error.is_none()).flat_map(|e| e.covered_files.iter()).collect();
            uris.retain(|uri| !covered.contains(uri));
            if max_bytes > 0 {
                let before = uris.len();
                uris.retain(|uri| crate::tantivy::search::parquet_rel_of_uri(uri).and_then(|rel| sizes.get(rel)).is_none_or(|size| *size <= max_bytes));
                let skipped = before - uris.len();
                *oversized = oversized.saturating_add(skipped as u64);
                if warn_skipped && skipped > 0 {
                    // No silent caps: oversized files stay uncovered until a
                    // bigger-memory runner reconciles without the limit.
                    warn!(table_name, project_id = %pid, skipped, "tantivy backfill skipped files over TIMEFUSION_TANTIVY_BACKFILL_MAX_FILE_MB");
                }
            }
            result.insert(pid, uris);
        }
        Ok((result, sizes, delta_store))
    }

    /// Commit one project's deferred manifest entries. Best-effort by design:
    /// the blobs are already uploaded, so a failed commit costs a rebuild next
    /// pass, and failing the whole backfill over one project's manifest write
    /// would throw away every other project's completed work.
    async fn commit_manifest_batch(
        svc: &crate::tantivy::search::TantivyIndexService, table_name: &str, project_id: &str,
        pending: &mut HashMap<String, Vec<(String, crate::tantivy::ManifestEntry)>>,
    ) {
        let Some(entries) = pending.remove(project_id).filter(|e| !e.is_empty()) else { return };
        let (n, started) = (entries.len(), std::time::Instant::now());
        match crate::tantivy::upsert_manifest_many(svc.object_store.as_ref(), table_name, project_id, entries).await {
            Ok(()) => {
                metrics::counter!(scan_metric_names::TANTIVY_MANIFEST_COMMITS).increment(1);
                metrics::counter!(scan_metric_names::TANTIVY_MANIFEST_COMMIT_US).increment(started.elapsed().as_micros() as u64);
            }
            Err(e) => warn!(table_name, project_id, entries = n, %e, "tantivy backfill manifest batch commit failed; files stay uncovered for the next pass"),
        }
    }

    /// Count live parquet the Tantivy manifest does not cover, without building anything.
    ///
    /// `backfill_table_indexes` only runs from the daily reconcile cron, so after a deploy the
    /// reindex has no reported remaining-work figure for hours. This is the same diff but
    /// metadata-only: the Delta log names live files and the manifest names covered ones. No
    /// parquet is read and no index is built, so it is safe to run far more often.
    /// Returns `(uncovered, oversized, by_age)` where `by_age` is
    /// `[today, 1-7d, older]` — see `CENSUS_AGE_BUCKETS`.
    pub async fn tantivy_coverage_census(&self) -> anyhow::Result<(u64, u64, [u64; 3])> {
        let Some(svc) = self.tantivy_indexer().cloned() else { return Ok((0, 0, [0; 3])) };
        let (mut uncovered, mut oversized) = (0u64, 0u64);
        // Age of the UNCOVERED files, not just how many. A total alone cannot
        // separate "a rewrite path is minting uncovered files right now" from
        // "an old backlog the backfill has not chewed through yet", and those
        // want opposite fixes — hook the rewriter vs. raise build throughput.
        let mut by_age = [0u64; 3];
        let today = crate::support::now_micros() / 86_400_000_000;
        for table_name in svc.config.indexed_tables() {
            if !svc.config.is_table_indexed(&table_name) {
                continue;
            }
            let mut roots: Vec<String> = vec!["default".into()];
            roots.extend(self.custom_project_tables.read().await.keys().filter(|(_, t)| *t == table_name).map(|(p, _)| p.clone()));
            for root in roots {
                let Ok(table_ref) = self.resolve_table(&root, &table_name).await else { continue };
                let (by_pid, ..) = self.group_uncovered_files_by_project(&svc, &table_ref, &table_name, &mut oversized, false).await?;
                for uris in by_pid.into_values() {
                    uncovered = uncovered.saturating_add(uris.len() as u64);
                    for uri in &uris {
                        // Partition date, not file mtime: a compaction rewrite
                        // of old data must count as OLD, or every rewrite would
                        // masquerade as fresh accrual and point at the wrong fix.
                        let age_days = crate::storage::date_partition_of(uri)
                            .and_then(|d| chrono::NaiveDate::parse_from_str(&d.to_string(), "%Y-%m-%d").ok())
                            .map_or(i64::MAX, |d| today - d.and_hms_opt(0, 0, 0).map_or(0, |t| t.and_utc().timestamp_micros() / 86_400_000_000));
                        by_age[usize::from(age_days > 0) + usize::from(age_days > 7)] += 1;
                    }
                }
            }
        }
        let stats = crate::observability::maintenance_stats();
        stats.tantivy_uncovered_files.store(uncovered, std::sync::atomic::Ordering::Relaxed);
        stats.tantivy_oversized_skipped.store(oversized, std::sync::atomic::Ordering::Relaxed);
        Ok((uncovered, oversized, by_age))
    }

    /// Bloom sidecar reconcile: for every table with bloom-enabled columns,
    /// diff live parquet against the per-(project,date) sidecars, lift the
    /// missing files' parquet blooms (footer + bloom ranges, no row decode)
    /// and GC entries for retired files. Newest dates first; bounded per
    /// pass. docs/plans/2026-08-22-file-level-needle-pruning.md
    pub async fn bloom_sidecar_reconcile(&self) -> anyhow::Result<(usize, usize)> {
        use crate::read::bloom_prune;
        let Some(reg) = self.bloom_prune().cloned() else { return Ok((0, 0)) };
        let mut budget = self.config.maintenance.timefusion_bloom_sidecar_files_per_pass;
        let (mut built, mut errors) = (0usize, 0usize);
        for table_name in crate::schema::registry().list_tables() {
            let Some(schema) = crate::schema::get_schema(&table_name) else { continue };
            let cols: Vec<String> = schema.fields.iter().filter(|f| f.bloom_filter).map(|f| f.name.clone()).collect();
            if cols.is_empty() {
                continue;
            }
            let mut roots: Vec<String> = vec!["default".into()];
            roots.extend(self.custom_project_tables.read().await.keys().filter(|(_, t)| *t == table_name).map(|(p, _)| p.clone()));
            for root in roots {
                if budget == 0 {
                    return Ok((built, errors));
                }
                let Ok(table_ref) = self.resolve_table(&root, &table_name).await else { continue };
                let (rels, delta_store) = {
                    let t = table_ref.read().await;
                    let rels: Vec<(String, u64)> = match t.snapshot() {
                        Ok(s) => s.log_data().iter().map(|f| (f.path().into_owned(), f.size() as u64)).collect(),
                        Err(_) => continue,
                    };
                    (rels, t.log_store().object_store(None))
                };
                // Group by (project, date); newest dates first so the hot
                // partitions a 24h point lookup touches converge first.
                let mut by_pd: HashMap<(String, String), Vec<(String, u64)>> = HashMap::new();
                for (rel, size) in rels.into_iter().filter(|(rel, _)| rel.ends_with(".parquet")) {
                    if let Some((pid, date)) = bloom_prune::project_date_of_rel(&rel) {
                        by_pd.entry((pid.to_string(), date.to_string())).or_default().push((rel, size));
                    }
                }
                let mut cells: Vec<_> = by_pd.into_iter().collect();
                cells.sort_by(|a, b| b.0.1.cmp(&a.0.1));
                for ((pid, date), files) in cells {
                    if budget == 0 {
                        return Ok((built, errors));
                    }
                    let existing = reg.load_sidecar_raw(&table_name, &pid, &date).await.unwrap_or_default();
                    let existing_count = existing.files.len();
                    let live: HashMap<&str, u64> = files.iter().map(|(rel, size)| (rel.as_str(), *size)).collect();
                    let mut kept: Vec<bloom_prune::FileBlooms> = existing.files.into_iter().filter(|f| live.contains_key(f.rel.as_str())).collect();
                    let known: HashSet<&str> = kept.iter().map(|f| f.rel.as_str()).collect();
                    let missing: Vec<(String, u64)> =
                        files.iter().filter(|(rel, _)| !known.contains(rel.as_str())).map(|(rel, size)| (rel.clone(), *size)).take(budget).collect();
                    if missing.is_empty() && kept.len() == existing_count {
                        continue; // converged: nothing new, nothing retired
                    }
                    budget = budget.saturating_sub(missing.len());
                    let mut results = futures::stream::iter(missing.into_iter().map(|(rel, size)| {
                        let store = delta_store.clone();
                        let cols = cols.clone();
                        async move { bloom_prune::build_file_blooms(store, &rel, size, &cols).await }
                    }))
                    .buffer_unordered(4);
                    while let Some(res) = results.next().await {
                        match res {
                            Ok(fb) => {
                                built += 1;
                                reg.stats.build_files.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                kept.push(fb);
                            }
                            Err(e) => {
                                errors += 1;
                                reg.stats.build_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                debug!(table_name, project_id = %pid, date, "bloom sidecar build failed: {e:#}");
                            }
                        }
                    }
                    if let Err(e) = reg.store_sidecar(&table_name, &pid, &date, bloom_prune::DateSidecar { files: kept }).await {
                        warn!(table_name, project_id = %pid, date, "bloom sidecar store failed: {e:#}");
                    }
                }
            }
        }
        Ok((built, errors))
    }

    async fn backfill_table_indexes(&self, svc: &Arc<crate::tantivy::search::TantivyIndexService>, table_name: &str) -> anyhow::Result<usize> {
        use crate::tantivy::search::parquet_rel_of_uri;
        // Unified table ("default") holds every default-routed project's
        // files; custom project tables are resolved separately.
        let mut roots: Vec<String> = vec!["default".into()];
        roots.extend(self.custom_project_tables.read().await.keys().filter(|(_, t)| t == table_name).map(|(p, _)| p.clone()));
        let mut built = 0usize;
        // Coverage gauges for this pass — see `tantivy_uncovered_files`.
        let (mut uncovered_total, mut oversized_total) = (0u64, 0u64);
        // Work this pass deliberately left for the next one. Reported, never
        // silent: a capped pass that logged only `built` would look identical
        // to a converged one.
        let mut deferred_total = 0u64;
        for root in roots {
            let Ok(table_ref) = self.resolve_table(&root, table_name).await else {
                continue;
            };
            let (by_pid, _sizes, delta_store) = self.group_uncovered_files_by_project(svc, &table_ref, table_name, &mut oversized_total, true).await?;
            let mut queues = Vec::with_capacity(by_pid.len());
            for (pid, mut uris) in by_pid {
                // Dashboard acceptance is governed by recent windows; a
                // terabyte of legacy history must not stand in front of the
                // last seven days after a migration.
                uncovered_total = uncovered_total.saturating_add(uris.len() as u64);
                sort_backfill_uris_newest_first(&mut uris);
                let queue = uris.into_iter().filter_map(|uri| Some((parquet_rel_of_uri(&uri)?.to_string(), uri))).collect::<VecDeque<_>>();
                queues.push((pid, queue));
            }
            let table_owned = table_name.to_string();
            // Bound the pass so the cron can run hourly. Fair round-robin
            // ordering means truncation drops the OLDEST round across every
            // project, never one project's whole queue.
            let cap = self.config.tantivy.timefusion_tantivy_backfill_max_files_per_pass;
            let available: usize = queues.iter().map(|(_, q)| q.len()).sum();
            let work = fair_tantivy_backfill_work_split(queues, cap, self.config.tantivy.timefusion_tantivy_backfill_tail_share_pct);
            deferred_total = deferred_total.saturating_add((available - work.len()) as u64);
            // Announce the pass BEFORE the first build, with what it will
            // attempt. `tantivy_backfill_pass` only prints at the very end, and
            // on this box a pass has never once reached it — so "planned" was
            // unobservable and the cap could not be shown to have applied.
            // (Equivalent to the unmerged fix/tantivy-reconcile-progress-logging;
            // rewritten here because that branch predates the bounded pass.)
            let planned = work.len();
            info!(table_name, root = %root, planned, cap, event = "tantivy_backfill_started");
            let mut done = 0usize;
            // Manifest writes are batched per project: each is a full
            // read-modify-write of that project's manifest (745 KB / 950 entries
            // for the busiest) under a per-project lock, so publishing per build
            // pays that per file. Worth ~6x fewer object-store writes per pass.
            //
            // It is NOT a throughput fix, and the measurement says so plainly:
            // builds ran 16/hr before and 13/hr after, because a build costs
            // ~4-5 MINUTES and the manifest write it saves costs ~1 second. The
            // ~60 builds/hr this was originally sized against was itself wrong —
            // the direct counter put it at ~16/hr. Kept for the write reduction.
            let mut pending: HashMap<String, Vec<(String, crate::tantivy::ManifestEntry)>> = HashMap::new();
            let mut pending_since: HashMap<String, std::time::Instant> = HashMap::new();
            let mut jobs = futures::stream::iter(work.into_iter().map(|(pid, rel, uri)| {
                let (svc, store, table) = (svc.clone(), delta_store.clone(), table_owned.clone());
                async move { (pid.clone(), svc.build_index_for_file_deferred(&table, &pid, &rel, &uri, store).await) }
            }))
            .buffer_unordered(self.config.tantivy.timefusion_tantivy_build_concurrency.max(1));
            while let Some((pid, result)) = jobs.next().await {
                match result {
                    // Counted live, not just at pass end: the `built=` pass line
                    // has NEVER been harvested in prod — passes outrun their tick
                    // or die to a deploy first — so backfill throughput has only
                    // ever been guessed at, most recently from `cache_seeded`,
                    // which also counts flush publishes and overstated it ~10x.
                    // A monotonic counter gives the true rate from deltas mid-pass.
                    Ok(entry) => {
                        built += 1;
                        metrics::counter!(scan_metric_names::TANTIVY_BACKFILL_BUILT).increment(1);
                        pending.entry(pid.clone()).or_default().push(entry);
                        pending_since.entry(pid.clone()).or_insert_with(std::time::Instant::now);
                    }
                    Err(e) => warn!("tantivy backfill build failed table={} project={}: {}", table_name, pid, e),
                }
                // Bounded by count AND age, so an interrupted pass loses at most
                // MANIFEST_MAX_AGE of durability: the blobs are already uploaded,
                // so what is lost is the record and the next pass rebuilds them —
                // wasted work, never wrong work.
                //
                // Swept over EVERY pending project, not just the one that just
                // built. Checking only `pid` made the age bound unreachable for
                // any project with one build per pass: nothing re-examines it,
                // so its entries wait for a pass end that on this box has never
                // arrived. Prod `b6d8c86` at 44 min uptime read
                // `tantivy_backfill_built=2, tantivy_manifest_commits=0` — the
                // bound was deployed and still could not fire. A build costs
                // ~4-5 minutes, so a whole-map sweep runs at most every few
                // minutes and its cost is noise against that.
                for project in due_manifest_flushes(&pending, &pending_since) {
                    pending_since.remove(&project);
                    Self::commit_manifest_batch(svc, table_name, &project, &mut pending).await;
                }
                done += 1;
                if done.is_multiple_of(25) {
                    info!(table_name, root = %root, done, planned, built, event = "tantivy_backfill_progress");
                }
            }
            for pid in pending.keys().cloned().collect::<Vec<_>>() {
                Self::commit_manifest_batch(svc, table_name, &pid, &mut pending).await;
            }
        }
        // Gauges, so each pass reports the CURRENT remaining work rather than a
        // running total. `uncovered` reaching 0 is what "the reindex is done"
        // means, and nothing reported it before — which is why the reindex was
        // being chased by hand from sibling containers.
        let stats = crate::observability::maintenance_stats();
        stats.tantivy_uncovered_files.store(uncovered_total.saturating_sub(built as u64), std::sync::atomic::Ordering::Relaxed);
        stats.tantivy_oversized_skipped.store(oversized_total, std::sync::atomic::Ordering::Relaxed);
        info!(
            table_name,
            built,
            uncovered_before = uncovered_total,
            oversized_skipped = oversized_total,
            deferred_to_next_pass = deferred_total,
            event = "tantivy_backfill_pass"
        );
        Ok(built)
    }

    pub(crate) async fn rollup_sql(
        &self, logical_plan: &datafusion::logical_expr::LogicalPlan, session: &datafusion::execution::context::SessionState,
    ) -> std::result::Result<Option<RollupRewrite>, crate::rollup::MissReason> {
        // Both switches are project-independent, so they are checked BEFORE the
        // matcher: `match_aggregate` plans one statement per filtered measure,
        // and with the feature off that cost must not be paid at all.
        if self.bypass_rollup || !self.config.maintenance.timefusion_rollup_enabled || !self.config.maintenance.timefusion_rollup_read_enabled {
            return Ok(None);
        }
        let routes = crate::rollup::match_aggregates(logical_plan, session).await?;
        if routes.is_empty() {
            return Ok(None);
        }
        // A cross-project route reads every project at once, so a per-project
        // allowlist cannot be honoured: it is enabled only when the rollout is on
        // for everyone.
        let enabled = match routes[0].project_id.as_deref() {
            Some(project) => self.config.maintenance.rollup_read_enabled_for(project),
            None => {
                let maintenance = &self.config.maintenance;
                maintenance.timefusion_rollup_enabled && maintenance.timefusion_rollup_read_enabled && maintenance.timefusion_rollup_read_projects.is_none()
            }
        };
        if !enabled {
            return Ok(None);
        }
        // Try every viable tier, best first, taking the first actually BUILT
        // across the window: the coarsest tier is cheapest but newest, so it's
        // the likeliest to have a hole the fine tier can still serve.
        let mut best_miss = None;
        for route in routes {
            match self.rollup_rewrite_for(route, session).await {
                Ok(Some(rewrite)) => return Ok(Some(rewrite)),
                Ok(None) => return Ok(None),
                Err(reason) => best_miss = best_miss.or(Some(reason)),
            }
        }
        Err(best_miss.unwrap_or(crate::rollup::MissReason::NotBuilt))
    }

    /// Resolve ONE candidate tier against its coverage, or say why it cannot serve.
    async fn rollup_rewrite_for(
        &self, route: crate::rollup::RoutedRollup, _session: &datafusion::execution::context::SessionState,
    ) -> std::result::Result<Option<RollupRewrite>, crate::rollup::MissReason> {
        let end = route.hi.checked_sub(1).ok_or(crate::rollup::MissReason::UnboundedTime)?;
        // A rollup partition is built from Delta files only (`query_delta_only`),
        // so any row still in the MemBuffer is absent from it. That is a bound on
        // how far the rollup may be trusted, not a reason to refuse: everything
        // at or above it is read raw.
        let dates = window_dates(route.lo, end).ok_or(crate::rollup::MissReason::IncompleteCoverage)?;
        // A cross-project route reads the unified table's rollup rows for every
        // tenant at once, and a project on custom storage lives in a table this
        // pass never sees — so its coverage would be assumed rather than proved,
        // and an uncovered day would be served as a silent undercount. Refuse
        // instead. Custom storage is rare, and the map is tiny.
        if route.project_id.is_none() && self.custom_storage_keys().await.iter().any(|(_, table)| table == &route.source) {
            return Err(crate::rollup::MissReason::IncompleteCoverage);
        }
        // ONE pass over the add actions for the whole window. Asking per date
        // rebuilds the entire add-actions batch each time, which on a table with
        // tens of thousands of files is the dominant cost of planning — a 4-day
        // window paid it four times.
        let lookup_project = route.project_id.clone().unwrap_or_else(|| "default".to_string());
        let fingerprints = {
            let table = self.resolve_table(&lookup_project, &route.source).await.map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?;
            let table = table.read().await;
            // Each partition is fingerprinted to the bound ITS coverage was built
            // to, so a day still being written is compared on the part the build
            // actually read. A partition with no coverage gets the whole-partition
            // question and simply fails to match below.
            Self::partition_stats_bounded(&table, tiebreak_of(&route.source), &|project, date| {
                self.rollup_coverage
                    .get(&(project.to_string(), route.source.clone(), route.target.clone(), date.to_string()))
                    .map_or(i64::MAX, |coverage| coverage.covered_through)
            })
            .map_err(|_| crate::rollup::MissReason::IncompleteCoverage)?
        };
        // Pinned: the one project. Grouped: every project the SOURCE holds rows
        // for IN THIS WINDOW — from the source, never the tier, so a project
        // with no rollup still counts against coverage. The window test (not
        // just the date) matters: a dormant tenant with rows at another hour
        // would otherwise void the intersected range for everyone, and dropping
        // it is safe — its rollup rows for the range are equally empty.
        let window_dates: HashSet<String> = dates.iter().map(chrono::NaiveDate::to_string).collect();
        let projects: Vec<String> = match &route.project_id {
            Some(project) => vec![project.clone()],
            None => fingerprints
                .iter()
                .filter(|((_, date), stats)| window_dates.contains(date) && stats.overlaps(route.lo, route.hi))
                .map(|((project, _), _)| project.clone())
                .sorted_unstable()
                .dedup()
                .collect(),
        };
        if projects.is_empty() {
            return Err(crate::rollup::MissReason::NotBuilt);
        }
        // Buffered rows are absent from every rollup partition; the EARLIEST
        // project's bound governs, and everything at or above it is read raw.
        let buffered = projects
            .iter()
            .filter_map(|project| self.buffered_layer().and_then(|layer| layer.min_buffered_micros(project, &route.source, route.lo, end)))
            .min();
        let mut generations = Vec::with_capacity(dates.len());
        let mut ticket = Vec::with_capacity(dates.len());
        let mut slice_ticket = Vec::new();
        // The certified SET, not a prefix: a mid-window gap used to discard
        // every later date too, so one stale day made a 7-day query scan 8.4M
        // raw rows while six days sat fully built.
        let mut miss = None;
        // One project's covered ranges. The rewrite may only read a range EVERY
        // project has covered, so these are intersected below — a project missing
        // a day drags that day into the raw leg instead of being silently omitted
        // from the answer.
        let mut per_project: Vec<Vec<(i64, i64)>> = Vec::with_capacity(projects.len());
        let mut first_uncovered: Option<String> = None;
        // Projects that proved coverage, and those that did not. Only the first
        // group is intersected — including a project with nothing would empty the
        // intersection and refuse the query for everyone, which is exactly what
        // prod did on 2026-08-18 with 9 of 10 projects covered.
        let (mut covered_projects, mut raw_only): (Vec<String>, Vec<String>) = (Vec::new(), Vec::new());
        for project in &projects {
            let mut covered: Vec<(i64, i64)> = Vec::new();
            for date in &dates {
                let date = date.to_string();
                let key = (project.clone(), route.source.clone(), route.target.clone(), date.clone());
                let day_start = date_start_micros(&date).ok_or(crate::rollup::MissReason::IncompleteCoverage)?;
                // `not_built` and `stale_coverage` are the same outcome but opposite
                // diagnoses: the first says the build never ran, the second says it
                // ran and the source moved under it. Collapsed into one reason, the
                // miss histogram cannot tell a missing cron from a churning
                // partition, which is the only question a rollout has to answer.
                let Some(coverage) = self.rollup_coverage.get(&key) else {
                    // Deliberately does NOT set `miss`: after a restart only SLICE
                    // coverage is recovered, so this lookup misses for every date
                    // while the slice loop below may still cover the window whole.
                    // Setting `miss` here hid every real refusal reason behind
                    // `not_built`. Genuine absence is caught after the slice loop.
                    continue;
                };
                let source_fp = fingerprints
                    .get(&(project.clone(), date.clone()))
                    .or_else(|| fingerprints.get(&("default".to_string(), date.clone())))
                    .map_or(0, |stats| stats.fingerprint);
                let source_epoch = self.rollup_source_epochs.get(&(project.clone(), route.source.clone(), date.clone())).map_or(0, |entry| *entry.value());
                if coverage.source_fp != source_fp || coverage.source_epoch != source_epoch {
                    miss = miss.or(Some(crate::rollup::MissReason::StaleCoverage));
                    continue;
                }
                debug!(project_id = %project, source = %route.source, target = %route.target, date, rows = coverage.rows, "rollup coverage selected");
                // Merge with the previous run when the dates are adjacent, so a
                // contiguous span costs one range predicate rather than one per day.
                // The build's OWN bound, not the day end. They are the same for a
                // sealed partition; for a day still being written the build stops
                // short, and reading past that point would serve buckets the rollup
                // never aggregated — the silent-wrong-number failure.
                let end = coverage.covered_through.min(day_start + DAY_MICROS);
                if end <= day_start {
                    miss = miss.or(Some(crate::rollup::MissReason::NotBuilt));
                    continue;
                }
                covered.push((day_start, end));
                generations.push((project.clone(), date.clone(), coverage.generation.clone()));
                ticket.push((key, source_fp, source_epoch, coverage.generation.clone(), coverage.covered_through));
            }
            // Slice coverage is the only live coverage, and until this guard it
            // was checked for project/source/target/overlap and nothing else —
            // no fingerprint, no epoch — so a slice went on claiming its range
            // after the source partition moved under it. Collect the candidates
            // per date first: a date is readable from the tier only if EVERY
            // slice covering it witnessed the partition as it stands now.
            let mut by_date: HashMap<String, Vec<(RollupSliceCoverageKey, RollupCoverage)>> = HashMap::new();
            for entry in self.rollup_slice_coverage.iter() {
                let ((slice_project, source, target, start, end), coverage) = (entry.key(), entry.value());
                if slice_project != project || source != &route.source || target != &route.target || *start >= route.hi || *end <= route.lo {
                    continue;
                }
                let Some(date) = chrono::DateTime::from_timestamp_micros(*start).map(|time| time.date_naive().to_string()) else { continue };
                by_date.entry(date).or_default().push((entry.key().clone(), coverage.clone()));
            }
            for (date, slices) in by_date {
                let current = fingerprints
                    .get(&(project.clone(), date.clone()))
                    .or_else(|| fingerprints.get(&("default".to_string(), date.clone())))
                    .and_then(|stats| u64::try_from(stats.rows).ok());
                // PER SLICE, not all-or-nothing over the date. A slice with no
                // witness is not evidence of DISAGREEMENT, it is an absence —
                // condemning the whole date for it meant one un-rebuilt slice
                // kept a fully rebuilt day on the raw path, and with thousands of
                // slices queued that is indefinite. Dropping just the
                // unverifiable slice sends its range to the raw fringe, which is
                // the same safe direction at a fraction of the cost.
                //
                // Sound because the witness is a statement about the WHOLE
                // partition ("it held N rows when I built"): two slices that both
                // still agree with N are both current, independently.
                // The live fingerprint, for the witness-less fallback below only.
                // Same field, same comparison the date-level path makes.
                let current_fp = fingerprints
                    .get(&(project.clone(), date.clone()))
                    .or_else(|| fingerprints.get(&("default".to_string(), date.clone())))
                    .map(|stats| stats.fingerprint);
                let fp_fallback = self.config.maintenance.timefusion_rollup_slice_fp_fallback;
                let (fresh, stale): (Vec<_>, Vec<_>) =
                    slices.into_iter().partition(|(_, coverage)| slice_is_current(coverage.source_rows, current, coverage.source_fp, current_fp, fp_fallback));
                if !stale.is_empty() {
                    miss = miss.or(Some(crate::rollup::MissReason::StaleCoverage));
                    // WHY it is stale, because the two answers demand opposite
                    // work. `no_witness` is a slice written before
                    // `TAG_SOURCE_ROWS` — it is not disagreeing, it is
                    // unverifiable, and only a republish clears it, so the fix is
                    // build throughput. `moved` is the partition genuinely
                    // changing under a verifiable slice, which no amount of
                    // rebuilding fixes on a churning day. `no_source_rows` is
                    // neither: the LIVE fingerprint is missing, so nothing can be
                    // compared. Prod 2026-08-22 had `stale_coverage` as the sole
                    // blocker on every bare dashboard shape, with no way to tell
                    // these apart.
                    for (_, coverage) in &stale {
                        metrics::counter!(stale_coverage_metric(coverage.source_rows, current)).increment(1);
                    }
                }
                for (key, coverage) in fresh {
                    covered.push((key.3, key.4));
                    generations.push((project.clone(), date.clone(), coverage.generation.clone()));
                    slice_ticket.push((key, coverage.source_fp, coverage.generation.clone()));
                }
            }
            let covered = crate::write::mem_buffer::merge_ranges(covered);
            // The honest NotBuilt: this project produced no covered range for the
            // window by EITHER route — date-level or slice. It no longer REFUSES
            // the query, though: the project is read raw across the whole window
            // while the covered projects still route, so one lagging tenant costs
            // its own rows a raw scan instead of everyone's.
            if covered.is_empty() {
                miss = miss.or(Some(crate::rollup::MissReason::NotBuilt));
                first_uncovered.get_or_insert_with(|| project.clone());
                raw_only.push(project.clone());
            } else {
                per_project.push(covered);
                covered_projects.push(project.clone());
            }
        }
        // The intersection over the COVERED projects only, so a range is read from
        // the rollup where each of them proved it. For the pinned case there is
        // one element and this is exactly the old behaviour.
        let covered = per_project.into_iter().reduce(|left, right| intersect_ranges(&left, &right)).unwrap_or_default();
        // `None` = every project the query reads, which keeps the pinned case and
        // the all-covered case emitting exactly the SQL they did before.
        let split = crate::rollup::ProjectSplit {
            covered: (route.project_id.is_none() && !raw_only.is_empty()).then(|| covered_projects.clone()),
            raw_only: if route.project_id.is_none() { raw_only.clone() } else { Vec::new() },
        };
        generations.sort_unstable();
        generations.dedup();
        // A buffered row is missing from EVERY rollup partition, whichever dates
        // are certified, so this caps the whole set rather than trimming a tail.
        let horizon = buffered.unwrap_or(route.hi);
        let realtime = self.config.maintenance.timefusion_rollup_realtime_tail;
        // Sampled on the same limiter as `rollup_miss_sampled`, so a
        // multiple-per-second miss rate cannot flood the log.
        if let Some(project) = &first_uncovered
            && crate::observability::sample_rollup_miss()
        {
            warn!(
                project_id = %project,
                source = %route.source,
                target = %route.target,
                projects_in_window = projects.len(),
                event = "rollup_uncovered_project",
                "a project in the window contributed NO covered range; with coverage intersected across the set this refuses the whole query"
            );
        }
        // With the tail off, the pre-fringe contract stands exactly: the rollup
        // answers the whole window or nothing at all.
        if !realtime && (miss.is_some() || horizon < route.hi || route.lo.rem_euclid(route.grain) != 0 || route.hi.rem_euclid(route.grain) != 0) {
            return Err(miss.unwrap_or(crate::rollup::MissReason::IncompleteCoverage));
        }
        let interiors = crate::rollup::interiors(route.lo, route.hi, route.grain, horizon, &covered);
        if interiors.is_empty() {
            // The most common refusal, and ambiguous from the counter alone: no
            // overlapping coverage vs the buffered horizon cutting it away vs a
            // survivor narrower than one grain — opposite fixes, so log the shape.
            if crate::observability::sample_rollup_miss() {
                warn!(
                    lo = route.lo,
                    hi = route.hi,
                    horizon,
                    // How far the buffered horizon pulled `hi` back. Large means
                    // the answer is the flush path, not the rollup builds.
                    horizon_shortfall_secs = (route.hi - horizon).max(0) / 1_000_000,
                    grain_secs = route.grain / 1_000_000,
                    covered_ranges = covered.len(),
                    covered_start = covered.first().map(|range| range.0).unwrap_or_default(),
                    covered_end = covered.last().map(|range| range.1).unwrap_or_default(),
                    projects_in_window = projects.len(),
                    target = %route.target,
                    event = "rollup_empty_interior",
                    "coverage produced no usable interior for this window"
                );
            }
            return Err(miss.unwrap_or(crate::rollup::MissReason::TinyInterior));
        }
        if crate::rollup::hybrid_branch_count(route.lo, route.hi, &interiors) > 32 {
            return Err(crate::rollup::MissReason::TooManyBranches);
        }
        // Drop dates no interval actually reads: keeping them in the ticket would
        // let an unrelated partition's churn invalidate a valid plan.
        let reads = |date: &str| interiors.iter().any(|interval| date_intersects(date, *interval));
        generations.retain(|(_, date, _)| reads(date));
        ticket.retain(|((_, _, _, date), ..)| reads(date));
        slice_ticket.retain(|((_, _, _, start, end), ..)| interiors.iter().any(|(covered_start, covered_end)| *start < *covered_end && *end > *covered_start));
        if generations.is_empty() {
            return Err(miss.unwrap_or(crate::rollup::MissReason::NotBuilt));
        }
        let mode = if interiors == [(route.lo, route.hi)] { "full" } else { "hybrid" };
        Ok(Some(RollupRewrite {
            sql: route.sql(&generations, &interiors, &split),
            grain: format!("{}us", route.grain),
            mode,
            matched: route.matched,
            ticket: RollupReadTicket { dates: ticket, slices: slice_ticket },
        }))
    }

    pub(crate) async fn rollup_ticket_current(&self, ticket: &RollupReadTicket) -> bool {
        for ((project_id, source, target, date), source_fp, source_epoch, generation, bound) in &ticket.dates {
            if self
                .rollup_coverage
                .get(&(project_id.clone(), source.clone(), target.clone(), date.clone()))
                .is_none_or(|coverage| coverage.source_fp != *source_fp || coverage.source_epoch != *source_epoch || coverage.generation != *generation)
            {
                return false;
            }
            if self.rollup_source_epochs.get(&(project_id.clone(), source.clone(), date.clone())).map_or(0, |epoch| *epoch.value()) != *source_epoch {
                return false;
            }
            if self.rollup_source_fingerprint(project_id, source, date, *bound).await.map_or(true, |fingerprint| fingerprint != *source_fp) {
                return false;
            }
        }
        for (key, source_fp, generation) in &ticket.slices {
            if self.rollup_slice_coverage.get(key).is_none_or(|coverage| coverage.source_fp != *source_fp || coverage.generation != *generation) {
                return false;
            }
        }
        true
    }

    /// Query Delta tables directly, bypassing the in-memory buffer (for testing).
    pub async fn query_delta_only(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let ctx = self.rollup_maintenance_context()?;
        Ok(ctx.sql(sql).await?.collect().await?)
    }

    fn rollup_maintenance_context(&self) -> Result<datafusion::prelude::SessionContext> {
        let mut db_clone = self.clone();
        db_clone.bypass_buffer = true;
        db_clone.bypass_rollup = true;
        // Background rewrite, not an interactive query: see `maintenance_scan`.
        db_clone.maintenance_scan = true;
        let db_arc = Arc::new(db_clone);
        let mut ctx = Arc::clone(&db_arc).create_session_context();
        datafusion_functions_json::register_all(&mut ctx)?;
        db_arc.setup_session_context(&mut ctx)?;
        Ok(ctx)
    }

    /// A coordinator rollup unit gets a private spillable pool so its physical
    /// operators cannot reserve more than the decoded-work ceiling. The unit
    /// registers its direct Delta provider after this returns, so it needs the
    /// aggregate-state UDFs but not the foreground routing/catalog tables.
    fn bounded_rollup_maintenance_context(&self) -> Result<datafusion::prelude::SessionContext> {
        let runtime = self.coordinator_runtime_env();
        let state = build_optimize_session_state_tuned(1, runtime, Some("256"), None);
        let mut ctx = datafusion::prelude::SessionContext::new_with_state(state);
        datafusion_functions_json::register_all(&mut ctx)?;
        self.setup_session_udfs(&mut ctx)?;
        Ok(ctx)
    }

    /// Start background maintenance schedulers for optimize and vacuum operations
    pub async fn start_maintenance_schedulers(self) -> Result<Self> {
        let db = Arc::new(self.background_clone());
        let cancel = self.maintenance_shutdown.clone();

        // Before any repair pass runs: re-adopt the footers already probed as
        // sorted in earlier processes, so this one does not spend its pass
        // re-clearing them. See `load_verified_sorted`.
        db.load_verified_sorted();

        // Coordinator planning and DataFusion execution must not share Tokio
        // workers with PGWire. In production the old arrangement accepted the
        // health-check TCP connection but starved its authentication future for
        // five consecutive 1.5s deadlines while maintenance was being polled.
        // Resource tokens bound memory and I/O; this runtime boundary reserves
        // executor progress for ingest, queries, and liveness.
        // Runtime threads keep timers, cancellation, and object-store I/O
        // progressing while admitted jobs are polled, so there must be headroom
        // beyond the jobs themselves — sizing the pool to exactly the job count
        // lets a blocking decode stall the timers that cancel it.
        let coordinator_job_workers = self.config.derived.coordinator_jobs();
        let coordinator_runtime_workers = (self.config.derived.cores / 8).clamp(2, 4).max(coordinator_job_workers + 2);
        db.maintenance_debt_planned_at.store(crate::support::now_micros(), std::sync::atomic::Ordering::Relaxed);
        {
            let db = Arc::clone(&db);
            let cancel = cancel.clone();
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(coordinator_runtime_workers)
                .thread_name("maintenance-worker")
                .enable_all()
                .build()
                .map_err(|error| anyhow::anyhow!("failed to build maintenance runtime: {error}"))?;
            db.maintenance_executor.set(runtime.handle().clone()).map_err(|_| anyhow::anyhow!("maintenance runtime already started"))?;
            std::thread::Builder::new()
                .name("maintenance-runtime".to_owned())
                .spawn(move || {
                    runtime.block_on(async move {
                        let journal = Arc::clone(&db.maintenance_tasks);
                        match tokio::task::spawn_blocking(move || -> Result<(usize, usize)> {
                            let mut journal = journal.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                            let discarded = journal.migrate_bootstrap_backlog();
                            let migrated = journal.migrate_derived_slices();
                            // One-shot: collapse the fine-grained sealed backfill
                            // so the coarse planner can re-derive it day-sized.
                            if let Some(cleared) = journal.clear_stale_estimates() {
                                // Rewrites every pending task in place; the WAL
                                // would carry it, but a full rewrite is cheaper
                                // than 85k append records and is what the
                                // following migrations need anyway.
                                journal.compact()?;
                                info!(cleared, event = "maintenance_stale_estimates_cleared");
                            }
                            let coarsened = journal.migrate_fine_grained_backfill(crate::support::now_micros()).unwrap_or_default();
                            if coarsened != 0 {
                                info!(coarsened, event = "maintenance_coarse_backfill_migrated");
                            }
                            if discarded.is_some_and(|count| count != 0) || coarsened != 0 {
                                journal.compact()?;
                            } else if discarded.is_some() || migrated != 0 {
                                journal.checkpoint()?;
                            }
                            Ok((discarded.unwrap_or_default(), migrated))
                        })
                        .await
                        {
                            Ok(Ok((discarded_bootstrap_tasks, migrated_tasks))) => {
                                // The cleanup removes only unpublished work.
                                // Recreate the small, authoritative set of
                                // invalidations persisted in rollup_journal so
                                // they still converge instead of remaining on
                                // the correctness-safe raw fallback forever.
                                let mut requeued_dirty_partitions = 0usize;
                                if discarded_bootstrap_tasks != 0 {
                                    for entry in db.rollup_dirty.iter() {
                                        let ((project, source, date), hours) = (entry.key(), *entry.value());
                                        if hours != 0 {
                                            match db.enqueue_maintenance_hours(project, source, date, hours) {
                                                Ok(()) => requeued_dirty_partitions = requeued_dirty_partitions.saturating_add(1),
                                                Err(error) => warn!(%error, project, source, date, event = "maintenance_dirty_partition_requeue_failed"),
                                            }
                                        }
                                    }
                                }
                                info!(
                                    discarded_bootstrap_tasks,
                                    requeued_dirty_partitions,
                                    migrated_tasks,
                                    runtime_workers = coordinator_runtime_workers,
                                    job_workers = coordinator_job_workers,
                                    event = "maintenance_runtime_started"
                                );
                            }
                            Ok(Err(error)) => {
                                warn!(%error, event = "maintenance_task_journal_migration_failed");
                                return;
                            }
                            Err(error) => {
                                warn!(%error, event = "maintenance_task_journal_migration_panicked");
                                return;
                            }
                        }

                        for worker in 0..coordinator_job_workers {
                            let db = Arc::clone(&db);
                            let cancel = cancel.clone();
                            tokio::spawn(async move {
                                if !db.wait_for_preload(&cancel).await {
                                    return;
                                }
                                loop {
                                    if cancel.is_cancelled() {
                                        return;
                                    }
                                    match tokio::time::timeout(COORDINATOR_LOOP_TIMEOUT, db.run_maintenance_coordinator_once()).await {
                                        Ok(Ok(true)) => tokio::task::yield_now().await,
                                        Ok(Ok(false)) => tokio::select! {
                                            _ = cancel.cancelled() => return,
                                            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                                        },
                                        Ok(Err(error)) => {
                                            warn!(worker, %error, event = "maintenance_coordinator_error");
                                            tokio::select! {
                                                _ = cancel.cancelled() => return,
                                                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                                            }
                                        }
                                        Err(_) => {
                                            // Dropping the future drops its TaskLease, which
                                            // durably requeues the claimed unit and releases
                                            // every admission token. A whale or wedged object
                                            // read therefore costs one bounded turn instead of
                                            // freezing maintenance for every project forever.
                                            warn!(
                                                worker,
                                                timeout_seconds = COORDINATOR_LOOP_TIMEOUT.as_secs(),
                                                event = "maintenance_coordinator_loop_timed_out"
                                            );
                                        }
                                    }
                                }
                            });
                        }

                        let reconcile_db = Arc::clone(&db);
                        let reconcile_cancel = cancel.clone();
                        tokio::spawn(async move {
                            if !reconcile_db.wait_for_preload(&reconcile_cancel).await {
                                return;
                            }
                            match reconcile_db.reconcile_maintenance_task_cursors().await {
                                Ok(reconciled_tasks) => info!(reconciled_tasks, event = "maintenance_task_reconcile_complete"),
                                Err(error) => {
                                    warn!(%error, event = "maintenance_task_reconcile_failed");
                                }
                            }
                            // Do not discover fresh file-rewrite debt in the
                            // boot burst. Durable journal work is already being
                            // drained above; the coordinator's normal 60-second
                            // planner tick discovers new debt after preload and
                            // PGWire handoff have settled. Planning here raced
                            // repair/resume staging and recreated the exact
                            // startup contention this runtime isolates.
                        });

                        let coverage_db = Arc::clone(&db);
                        let coverage_cancel = cancel.clone();
                        tokio::spawn(async move {
                            if !coverage_db.wait_for_preload(&coverage_cancel).await {
                                return;
                            }
                            // RECURRING, not once at startup. This pass is the
                            // only thing that sees an untagged tier file, so it
                            // is the only thing that can enqueue the republish
                            // that retires one — and a file becomes retirable
                            // when OTHER slices publish, which happens long
                            // after boot. Prod 2026-08-22, an hour after the
                            // ranking fix: 30 of the 85 untagged files satisfied
                            // a retirement proof and none had been retired,
                            // because the publish that would evaluate the proof
                            // was never queued. Startup-only made convergence
                            // run at the DEPLOY cadence.
                            //
                            // Metadata only — it reads each tier's Delta log and
                            // never touches parquet — so hourly is far cheaper
                            // than the 60s planner tick beside it.
                            loop {
                                for source in crate::schema::registry().list_tables() {
                                    if let Err(error) = coverage_db.recover_rollup_coverage(&source).await {
                                        warn!(source, %error, "rollup coverage recovery failed; those partitions stay on raw scans");
                                    }
                                }
                                tokio::select! {
                                    _ = coverage_cancel.cancelled() => return,
                                    _ = tokio::time::sleep(COVERAGE_RECOVERY_INTERVAL) => {}
                                }
                            }
                        });

                        cancel.cancelled().await;
                    });
                })
                .map_err(|error| anyhow::anyhow!("failed to start maintenance runtime: {error}"))?;
        }

        // Always-on pressure sampling: `scan_pressure_permits` samples lazily on
        // gated decode polls, so pressure driven by anything else (flush sorts,
        // replay, maintenance) reached the OOM kill unlogged. A 250ms heartbeat
        // keeps the tier fresh whoever is allocating.
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

        // Schedule settings remain accepted during migration, but these loops
        // no longer own packing or repair once the durable coordinator is on.
        if !COORDINATOR_OWNS_SLICE_MAINTENANCE {
            // Hot compact — bin-pack today's small files (every ~5 min). Runs WITHOUT
            // the maintenance_job_sem so it can't be starved behind the dedup backlog
            // or a long full-optimize: prod 2026-07-20 showed the busy project only
            // got compacted ~every 40 min because dedup churning an old-date backlog
            // wedged the shared serial pass >600s. Compaction touches only today; dedup
            // skips today — disjoint partitions, so decoupling is safe. Peak heap stays
            // bounded by the wave engine's OWN light_rewrite_sem (+ the light pool
            // slice it is sized from), never the heavy maintenance_rewrite_sem:
            // sharing that one re-couples the two engines (prod 2026-07-30).
            spawn_db_cron(&db, "Hot compact", &self.config.maintenance.timefusion_light_optimize_schedule, cancel.clone(), |db| async move {
                if !db.config.maintenance.timefusion_light_optimize_enabled {
                    return;
                }
                info!("Running scheduled hot-tail compaction on today's small files");
                db.run_hot_compact_sweep(TailPass::Pack).await;
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
            });

            // Footer repair — rewrite sealed-date files with no `sorting_columns`
            // on its OWN cron: one repair unit is a whole 700MB-1GiB file, so a
            // 5-minute tick budget discarded every attempt forever. Disjoint from
            // packing (sealed vs today). `repair_guard` is ONE try_lock for every
            // repair entry point — spawn_cron_job only skips ticks it started
            // itself, and two concurrent repair passes together starve packing.
            let repair_guard = Arc::new(tokio::sync::Mutex::new(()));
            spawn_cron_job("Footer repair", &self.config.maintenance.timefusion_footer_repair_schedule, cancel.clone(), {
                let db = db.clone();
                let repair_guard = repair_guard.clone();
                move || {
                    let db = db.clone();
                    let repair_guard = repair_guard.clone();
                    async move {
                        let Ok(_guard) = repair_guard.try_lock() else {
                            info!("Scheduled footer repair skipped — a repair pass is already running");
                            return;
                        };
                        if !db.config.maintenance.timefusion_light_optimize_enabled || db.config.maintenance.timefusion_light_optimize_repair_days == 0 {
                            return;
                        }
                        info!("Running scheduled footer repair on sealed partitions");
                        db.run_hot_compact_sweep(TailPass::Repair).await;
                    }
                }
            });

            // ...and once shortly after boot: a backlog that only drains between
            // restarts never drains when restarts outpace the schedule. The delay
            // lets boot settle; it takes `repair_guard`, so startup and cron
            // passes stand down for each other.
            {
                let db = db.clone();
                let cancel = cancel.clone();
                let repair_guard = repair_guard.clone();
                tokio::spawn(async move {
                    tokio::select! {
                        _ = cancel.cancelled() => return,
                        _ = tokio::time::sleep(STARTUP_REPAIR_DELAY) => {}
                    }
                    let Ok(_guard) = repair_guard.try_lock() else {
                        info!("Startup footer repair skipped — a repair pass is already running");
                        return;
                    };
                    if !db.config.maintenance.timefusion_light_optimize_enabled || db.config.maintenance.timefusion_light_optimize_repair_days == 0 {
                        return;
                    }
                    info!(delay_secs = STARTUP_REPAIR_DELAY.as_secs(), "Running startup footer repair (not waiting for the next tick)");
                    db.run_hot_compact_sweep(TailPass::Repair).await;
                });
            }
        }

        // Dedup — collapse duplicates in sealed (< today) partitions on its own
        // cron, decoupled from hot compaction above. Keeps the job_sem so it stays
        // serialized against the full optimize job (the other job_sem holder).
        // spawn_cron_job skips overlapping ticks.
        spawn_cron_job_on("Dedup", &self.config.maintenance.timefusion_dedup_schedule, cancel.clone(), self.maintenance_executor.get().cloned(), {
            let db = db.clone();
            move || {
                let db = db.clone();
                async move {
                    let Ok(_maintenance_job) = db.maintenance_job_sem.clone().acquire_owned().await else {
                        return;
                    };
                    info!("Running scheduled dedup on sealed partitions");
                    // ONE budget for the whole pass (overrunning skips every later
                    // tick via maintenance_job_sem), SPLIT between its two
                    // sequential halves so the drain can't spend it all before the
                    // sweep — which is what certifies partitions — ever runs.
                    let budget = db.config.derived.tick_budget(cron_period(&db.config.maintenance.timefusion_dedup_schedule));
                    let now = std::time::Instant::now();
                    // The drain gets the larger share: it physically removes
                    // duplicates, and the sweep resumes at a cursor while a
                    // half-drained bin does not.
                    let drain_deadline = now + budget.mul_f64(0.6);
                    let sweep_deadline = now + budget;
                    // ROTATE the table order — a fixed order lets the first table
                    // spend the shared budget and starve the rest forever.
                    let mut tables = db.all_tables().await;
                    let start = sweep_resume_offset(tables.len(), db.dedup_table_cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
                    tables.rotate_left(start);
                    for (project_id, table_name, table) in tables {
                        if db.maintenance_shutdown.is_cancelled() {
                            return;
                        }
                        // Rollup-declared sources are owned by durable coordinator
                        // tasks; this legacy cron serves only tables with no slice
                        // lifecycle. Do NOT re-enable the drain here (954d516,
                        // reverted d5688fd): drain_deadline bounds admission only,
                        // so one admitted bin can hold maintenance_job_sem for its
                        // 3600s stage deadline and wedge every other job. Staging
                        // must become resumable and the drain needs its own budget first.
                        if get_schema(&table_name).is_some_and(|schema| !schema.rollups.is_empty()) {
                            continue;
                        }
                        // Dedup key: bare table name for unified tables, tenant-scoped
                        // for custom-storage ones (they are separate Delta logs).
                        let key = if project_id.is_empty() { table_name.clone() } else { format!("{project_id}:{table_name}") };
                        db.run_dedup_for_table(&table, &table_name, &key, &Self::table_label(&project_id, &table_name), drain_deadline, sweep_deadline).await;
                    }
                }
            }
        });

        // Rollup backfill schedule settings remain accepted for migration, but
        // the durable coordinator above is the sole rollup owner.

        if !COORDINATOR_OWNS_SLICE_MAINTENANCE {
            // Kept only as a migration fallback. The coordinator plans the same
            // packing debt by snapshot and must be its sole writer once active.
            spawn_db_cron(&db, "Optimize", &self.config.maintenance.timefusion_optimize_schedule, cancel.clone(), |db| async move {
                let Ok(_maintenance_job) = db.maintenance_job_sem.clone().acquire_owned().await else {
                    return;
                };
                info!("Running scheduled optimize on all tables");
                for (project_id, table_name, table) in db.all_tables().await {
                    if let Err(e) = db.optimize_table(&table, &table_name, None).await {
                        error!("Optimize failed for {}: {}", Self::table_label(&project_id, &table_name), e);
                    }
                }
            });
        }

        if !COORDINATOR_OWNS_SLICE_MAINTENANCE {
            // Consolidate — daily cold sweep bin-packing sealed partitions (older than
            // cold_optimize_after_days) to the 512MB cold target, beyond the 48h warm window.
            spawn_db_cron(&db, "Consolidate", &self.config.maintenance.timefusion_consolidate_schedule, cancel.clone(), |db| async move {
                info!("Running scheduled cold consolidation on sealed partitions");
                let targets = db.all_maintenance_targets().await;
                for (name, table) in &targets {
                    if let Err(e) = db.consolidate_sealed_partitions(table, name).await {
                        error!("Consolidate (cold tier) failed for '{}': {}", name, e);
                    }
                }
            });
        }

        // Recompress — daily tier upgrade for cold (14d+). Skips partitions whose
        // probe file already advertises the target tier, so re-runs are cheap.
        let cold_cutoff = self.config.parquet.timefusion_cold_cutoff_days;
        let zstd_cold = self.config.parquet.timefusion_zstd_level_cold;
        // Cold sweep upper bound — older partitions fall under vacuum.
        let cold_upper = (self.config.maintenance.timefusion_vacuum_retention_hours / 24).max(cold_cutoff + 60);
        if !COORDINATOR_OWNS_SLICE_MAINTENANCE {
            spawn_db_cron(&db, "Recompress", &self.config.maintenance.timefusion_recompress_schedule, cancel.clone(), move |db| async move {
                info!("Running scheduled tier recompression (warm→cold@{}d zstd={})", cold_cutoff, zstd_cold);
                let targets = db.all_maintenance_targets().await;
                for (name, table) in &targets {
                    if let Err(e) = db.recompress_tier_window(table, name, cold_cutoff, cold_upper, zstd_cold).await {
                        error!("Recompress (cold tier) failed for '{}': {}", name, e);
                    }
                }
            });
        }

        // Vacuum — expired-file removal (default: daily at 2AM).
        let vacuum_retention = self.config.maintenance.timefusion_vacuum_retention_hours;
        spawn_db_cron(&db, "Vacuum", &self.config.maintenance.timefusion_vacuum_schedule, cancel.clone(), move |db| async move {
            info!("Running scheduled vacuum on all tables");
            for (project_id, table_name, table) in db.all_tables().await {
                info!("Vacuuming {} (retention: {}h)", Self::table_label(&project_id, &table_name), vacuum_retention);
                db.vacuum_table(&project_id, &table_name, &table, vacuum_retention).await;
            }
        });

        // Tantivy reconcile — nightly index consistency: backfill every live
        // parquet no manifest covers (wave commits carry no index hook; failed
        // builds; emergency-CLI compactions), then GC entries for dead files
        // across every per-uuid manifest. Gated at tick time: the indexer is
        // attached after construction, and may be absent entirely (no indexed
        // tables / no bucket).
        spawn_db_cron(&db, "Tantivy reconcile", &self.config.maintenance.timefusion_tantivy_reconcile_schedule, cancel.clone(), |db| async move {
            let Some(svc) = db.tantivy_indexer().cloned() else { return };
            for table_name in svc.config.indexed_tables() {
                match db.tantivy_reconcile_table(&table_name).await {
                    // Logged even when it did nothing: a silent no-op arm made
                    // "ran and found nothing" indistinguishable from "never
                    // ran", and the cron not firing at all was the actual bug.
                    Ok((built, removed, blobs)) => {
                        info!("tantivy reconcile: table={} built={} entries_removed={} blobs_deleted={}", table_name, built, removed, blobs);
                    }
                    Err(e) => warn!("tantivy reconcile failed for {}: {}", table_name, e),
                }
            }
        });

        spawn_db_cron(&db, "Bloom sidecar reconcile", &self.config.maintenance.timefusion_bloom_sidecar_schedule, cancel.clone(), |db| async move {
            if db.bloom_prune().is_none() {
                return;
            }
            match db.bloom_sidecar_reconcile().await {
                // Logged on no-op too: "ran and found nothing" must stay
                // distinguishable from "never ran" (the tantivy lesson).
                Ok((built, errors)) => info!("bloom sidecar reconcile: built={built} errors={errors}"),
                Err(e) => warn!("bloom sidecar reconcile failed: {e:#}"),
            }
        });

        // Tantivy cache reap — bound the extracted-index disk tree. The only
        // thing that deletes from it: `gc_after_compaction` reaps manifest
        // entries and object-store blobs, but never the local extraction, and
        // that tree shares a volume with the WAL. Uses the search service (the
        // reader, which owns the cache), not the indexer.
        spawn_db_cron(&db, "Tantivy cache reap", &self.config.tantivy.timefusion_tantivy_cache_reap_schedule, cancel.clone(), |db| async move {
            let Some(svc) = db.tantivy_search().cloned() else { return };
            let budget = db.config.tantivy.cache_disk_bytes();
            // Walks the whole cache tree and unlinks — never on the runtime's worker threads.
            let Ok(report) = tokio::task::spawn_blocking(move || svc.reap_disk_cache(budget)).await else { return };
            crate::observability::record_tantivy_cache_bytes(report.bytes_before - report.bytes_removed);
            if report.dirs_removed > 0 || report.errors > 0 {
                info!(
                    "tantivy cache reap: scanned={} before={}MB removed={} freed={}MB errors={} budget={}MB",
                    report.dirs_scanned,
                    report.bytes_before / 1024 / 1024,
                    report.dirs_removed,
                    report.bytes_removed / 1024 / 1024,
                    report.errors,
                    budget / 1024 / 1024
                );
            }
        });

        // Tantivy hot-window re-warm. Startup warming alone decays: the reaper
        // evicts, new indexes land from other writers, and one wide historical
        // query can pull enough cold blobs to push the hot window out. Re-warming
        // also re-stamps `last_used` on every hot dir, which is what keeps them
        // at the young end of the reaper's eviction order — so this is the
        // mechanism that makes "hot indexes stay local" true over time, not just
        // at boot. Blobs already on disk cost one `has_any_segment` stat each.
        spawn_db_cron(&db, "Tantivy hot warm", &self.config.tantivy.timefusion_tantivy_prefetch_schedule, cancel.clone(), |db| async move {
            db.tantivy_warm_hot_window().await;
        });

        // Checkpoint + expired-log cleanup — runs the post-commit hooks out-of-band
        // (see the 2026-07-09 incident) so R2 500s on the checkpoint PUT / bulk log
        // delete never fail a landed commit; faster cadence keeps the log bounded.
        spawn_db_cron(&db, "Checkpoint", &self.config.maintenance.timefusion_checkpoint_schedule, cancel.clone(), |db| async move {
            db.run_checkpoint_maintenance().await
        });

        // Reconcile — repair dangling Add entries (committed parquet deleted by a
        // past commit-path failure) by Remove'ing them via filesystem_check.
        spawn_db_cron(&db, "Reconcile", &self.config.maintenance.timefusion_reconcile_schedule, cancel.clone(), |db| async move {
            db.run_reconcile_maintenance().await
        });

        // Cache stats — every 5 minutes.
        spawn_db_cron(&db, "Cache stats", "0 */5 * * * *", cancel.clone(), |db| async move {
            if let Some(ref cache) = db.object_store_cache {
                cache.log_stats().await;
            }
            let (used, capacity) = db.statistics_extractor.get_cache_stats().await;
            info!("Statistics cache: {}/{} entries used", used, capacity);
        });

        // Statistics refresh — every 15 minutes.
        spawn_db_cron(&db, "Statistics refresh", "0 */15 * * * *", cancel.clone(), |db| async move {
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
        // Defaults set explicitly (even where they match DataFusion's) so a
        // future upstream default flip can't silently regress query plans.
        // NOTE: the decoded-metadata cache limit is NOT set here — a
        // `datafusion.runtime.*` SessionConfig string does not reconfigure an
        // already-built RuntimeEnv. It is applied on the RuntimeEnvBuilder
        // below via `build_query_runtime_env` instead.
        for (key, value) in [
            ("datafusion.catalog.information_schema", "true"),
            // INCIDENT 2026-07-31 (7d68f01): a briefly-live commit wrote permanent
            // parquet whose physical schema has `timestamp`/`id` NULLABLE while the
            // YAML declares NOT NULL, and DataFusion rejects aggregates grouped on
            // such columns (physical-vs-logical nullability mismatch) — which took
            // out every time_bucket dashboard. Widened nullability is always safe to
            // read; keep this set until those files age out or are rewritten.
            ("datafusion.execution.skip_physical_aggregate_schema_check", "true"),
            // Must be false: delta_kernel's unshredded_variant() schema uses Binary (not BinaryView).
            // Forcing view types causes UPDATE/DELETE rewrites to fail schema validation against variant columns.
            ("datafusion.execution.parquet.schema_force_view_types", "false"),
            ("datafusion.sql_parser.map_string_types_to_utf8view", "true"),
            // PostgreSQL dialect for ctx.sql() parsing. The default GenericDialect gives
            // the JSON `->`/`->>` operators precedence *below* `=` (PgOther 16 < Eq 20), so
            // `body->>'k'='v'` mis-parses as `body->>('k'='v')`. PostgreSQL binds them
            // *above* comparison (matching real Postgres + the pgwire fork's own parser),
            // so unparenthesized `col->>'k'='v'` works without the caller adding parens.
            ("datafusion.sql_parser.dialect", "postgresql"),
            // Parquet file/column statistics + bloom filters for pruning with Delta Lake.
            ("datafusion.execution.parquet.statistics_enabled", "page"),
            ("datafusion.execution.parquet.pushdown_filters", "true"),
            ("datafusion.execution.parquet.reorder_filters", "true"),
            ("datafusion.execution.parquet.enable_page_index", "true"),
            ("datafusion.execution.parquet.pruning", "true"),
            ("datafusion.execution.parquet.skip_metadata", "false"),
            ("datafusion.execution.parquet.bloom_filter_on_read", "true"),
            ("datafusion.execution.collect_statistics", "true"),
            ("datafusion.explain.show_schema", "true"),
            // Batch size 2048 (down from 65536): the wide otel schema makes every
            // decode buffer cost `batch_size × row width`, none of it pool-accounted
            // — heap profiling caught byte-view coalesce and parquet decoder buffers
            // at 10-29GB. Same value the maintenance sessions have run since 07-12.
            ("datafusion.execution.batch_size", WIDE_ROW_DECODE_BATCH_SIZE),
            // Optimize for sorted data (timestamps are typically sorted); disable
            // round-robin repartitioning to maintain sort order.
            ("datafusion.optimizer.prefer_existing_sort", "true"),
            ("datafusion.optimizer.repartition_aggregations", "true"),
            ("datafusion.optimizer.enable_round_robin_repartition", "false"),
            ("datafusion.optimizer.filter_null_join_keys", "true"),
            ("datafusion.optimizer.skip_failed_rules", "false"),
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
            ("datafusion.optimizer.enable_leaf_expression_pushdown", "false"),
            // Proper limit handling across partitions.
            ("datafusion.optimizer.enable_distinct_aggregation_soft_limit", "true"),
            ("datafusion.optimizer.enable_topk_aggregation", "true"),
            ("datafusion.execution.coalesce_batches", "true"),
            ("datafusion.optimizer.max_passes", "5"),
            // The per-query share of the (already tree-sized) pool — a fixed 0.9
            // replaces the deleted TIMEFUSION_MEMORY_FRACTION knob, whose prod
            // value was calibrated against the old hand-set limit.
            ("datafusion.execution.memory_fraction", "0.9"),
        ] {
            let _ = options.set(key, value);
        }
        // One-shot footer read sized to match `warm_footer`'s suffix range: the
        // Foyer metadata cache keys on (path, exact range), so the reader's
        // first fetch (size-hint..size) hits the entry the warm task populated.
        // Without this the reader does 8-byte-tail + metadata-range reads —
        // two sequential S3 RTTs on different keys that can never be pre-warmed
        // (measured 1.6 s of metadata_load_time on a cold OVH partition).
        let _ = options.set("datafusion.execution.parquet.metadata_size_hint", &self.config.cache.timefusion_parquet_metadata_size_hint.to_string());
        let _ = options.set(
            "datafusion.execution.sort_spill_reservation_bytes",
            &self.config.memory.timefusion_sort_spill_reservation_bytes.unwrap_or(67_108_864).to_string(),
        );
        // Cap query parallelism at the container's CPU quota (derived in
        // config::apply; 0 = leave DataFusion's default). See MemoryConfig.
        if self.maintenance_scan {
            let _ = options.set("datafusion.execution.target_partitions", &MAINTENANCE_MAX_PARTITIONS.to_string());
        } else if self.config.memory.timefusion_query_partitions > 0 {
            let _ = options.set("datafusion.execution.target_partitions", &self.config.memory.timefusion_query_partitions.to_string());
        }

        // A maintenance scan borrows the MAINTENANCE pool, not the query pool.
        // Two reasons, both about staying inside the configured budget rather
        // than growing RSS: that pool has a spill directory, so a whole-day
        // aggregate spills to disk instead of holding every group in memory;
        // and a background rewrite must not compete with interactive queries
        // for the query pool it was previously charging against. Certification
        // — the other half of a backfill unit — already used this env, so the
        // two halves were charged to different budgets.
        let runtime_env = if self.maintenance_scan { self.maintenance_runtime_env() } else { self.shared_runtime_env() };

        // Set up tracing options with configurable sampling
        let record_metrics = self.config.memory.timefusion_tracing_record_metrics;

        // Cell-capped preview formatter — the default renders whole cell values;
        // see `observability::capped_preview_fn` for the 2026-07-06 OOM it prevents.
        let tracing_options = InstrumentationOptions::builder()
            .record_metrics(record_metrics)
            .preview_limit(5)
            .preview_fn(Arc::new(crate::observability::capped_preview_fn))
            .build();

        let instrument_rule = instrument_with_info_spans!(options: tracing_options);

        // Create session state with tracing rule and DML support
        // Rule ordering: VariantInsertRewriter runs BEFORE TypeCoercion (rewrites string->json_to_variant)
        //                VariantSelectRewriter runs AFTER TypeCoercion (wraps Variant cols with variant_to_json)
        let analyzer_rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> = vec![
            Arc::new(datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction::new()),
            Arc::new(crate::read::optimizers::VariantInsertRewriter),
            // Tantivy predicate rewriter runs BEFORE TypeCoercion so the
            // injected `text_match(col, lit)` calls get coerced like any
            // other UDF args (Utf8 vs Utf8View etc).
            Arc::new(crate::read::optimizers::TantivyPredicateRewriter::new(self.config.tantivy.route_equality())),
            // Expands `f(qualifier.*)` into `f(qualifier.c1, …, qualifier.cN)`
            // before TypeCoercion rejects the typeless wildcard. Postgres parity.
            Arc::new(crate::read::optimizers::WildcardFnArgExpander),
            // PG parity: `COALESCE(list_col, '{}')` — re-type PG array string
            // literals as list literals before TypeCoercion fails the call.
            Arc::new(crate::read::optimizers::PgArrayLiteralRewriter),
            // PG clients put EXISTS in a SELECT list (pgAdmin's `is_catalog`);
            // DataFusion only decorrelates EXISTS in a filter, so rewrite it to
            // the equivalent correlated `count(1) > 0` scalar subquery, which it
            // does decorrelate. Before TypeCoercion so the comparison coerces.
            Arc::new(crate::read::optimizers::ExistsInProjection),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
            Arc::new(crate::read::optimizers::VariantSelectRewriter),
        ];

        let session_state = SessionStateBuilder::new()
            .with_config(options.into())
            .with_runtime_env(runtime_env)
            .with_default_features()
            .with_analyzer_rules(analyzer_rules)
            // Appended after DataFusion's defaults so push_down_limit has
            // already folded LIMIT into Sort.fetch — see the rule's docs.
            .with_optimizer_rule(Arc::new(crate::read::optimizers::DeferExpensiveProjection))
            // Must run LAST: re-restores Variant scan types that
            // optimize_projections reverts to Utf8View when it rebuilds each
            // TableScan from the lying provider schema — see the rule's docs
            // (fixes XX000 on `DISTINCT ON` over Variant columns).
            .with_optimizer_rule(Arc::new(crate::read::optimizers::VariantScanSchemaRestore))
            // Physical rules: start from DataFusion's defaults, splice our
            // mem∪delta union-ordering rule in *before* EnforceDistribution so
            // the built-in EnforceDistribution/EnforceSorting do the
            // SortPreservingMerge insertion + redundant-sort removal for us
            // (turns `ORDER BY timestamp DESC LIMIT n` into a streaming,
            // early-terminating TopK). Tracing instrument rule stays last.
            .with_physical_optimizer_rules({
                let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
                let pos = rules.iter().position(|r| r.name() == "EnforceDistribution").unwrap_or(0);
                rules.insert(pos, Arc::new(crate::read::optimizers::OrderedUnionForTopK));
                // After EnforceSorting/EnforceDistribution, never before: this rule exists to
                // undo their (locally correct) decision to discharge DedupExec's ordering as
                // trivially satisfied when a pushed equality pins the sort column to a
                // constant, which leaves the operator reading through an order-erasing
                // coalesce. See `DedupNeedsOrderedInput`.
                rules.push(Arc::new(crate::read::optimizers::DedupNeedsOrderedInput));
                rules.push(instrument_rule);
                rules
            })
            // The planner resolves the buffered layer at plan time (late-
            // binding): sessions are created during boot before the layer
            // exists.
            .with_query_planner(Arc::new(DmlQueryPlanner::new(self.clone())))
            // PostgreSQL custom casts, including jsonpath and regproc, become
            // text consistently for simple and extended protocol queries.
            .with_type_planner(Arc::new(crate::read::functions::PostgresTypePlanner))
            .build();

        SessionContext::new_with_state(session_state)
    }

    /// Register UDFs only — safe to call before `with_buffered_layer`.
    pub fn setup_session_udfs(&self, ctx: &mut SessionContext) -> DFResult<()> {
        self.register_set_config_udf(ctx);
        // CRITICAL: Register custom functions BEFORE JSON functions to ensure VariantAwareExprPlanner
        // intercepts -> and ->> operators on Variant columns before JsonExprPlanner handles them as strings
        crate::read::functions::register_custom_functions(ctx)
            .map_err(|e| DataFusionError::Execution(format!("Failed to register custom functions: {}", e)))?;
        self.register_json_functions(ctx);
        Ok(())
    }

    /// Register routing, stats, and PostgreSQL catalog tables. Depends on `self.buffered_layer`
    /// being set (stats table holds an Arc to it).
    pub fn setup_session_tables(&self, ctx: &mut SessionContext) -> DFResult<()> {
        use crate::schema::registry;

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
        let cache_sizes: crate::server::pg_compat::CacheSizeSnapshot = Arc::new(move || (fr_handle.len(), dp_handle.iter().map(|e| e.value().len()).sum()));
        let foyer = self.object_store_cache.clone();
        let foyer_stats: crate::server::pg_compat::FoyerStatsSnapshot =
            Arc::new(move || foyer.as_ref().map_or_else(crate::storage::FoyerRuntimeStats::default, |cache| cache.runtime_stats()));
        ctx.register_table(
            "timefusion_stats",
            Arc::new(
                crate::server::pg_compat::StatsTableProvider::new(self.buffered_layer().cloned())
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
                    })
                    .with_tantivy_search_opt(self.tantivy_search().cloned())
                    .with_bloom_prune_opt(self.bloom_prune().cloned()),
            ),
        )?;

        crate::server::pg_compat::setup_catalog(ctx, &self.config.core.pgwire_user, self.config.core.timefusion_pgwire_max_statement_secs)?;
        Ok(())
    }

    /// Setup the session context with both UDFs and tables. Preserves the legacy
    /// table-then-UDF ordering for existing callers that wire everything up at once.
    pub fn setup_session_context(&self, ctx: &mut SessionContext) -> DFResult<()> {
        self.setup_session_tables(ctx)?;
        self.setup_session_udfs(ctx)
    }

    /// `set_config(name, value, is_local)` — PostgreSQL returns the value that
    /// was set, which is all TF does: it holds no state for the settings
    /// clients set this way.
    ///
    /// Echoing the argument is also what makes both call shapes work. An
    /// all-literal call arrives as `ColumnarValue::Scalar`, and the previous
    /// Array-only implementation rejected precisely the call pgAdmin makes on
    /// connect (`set_config('bytea_output', 'escape', false)`).
    pub fn register_set_config_udf(&self, ctx: &SessionContext) {
        use datafusion::{
            arrow::datatypes::DataType,
            logical_expr::{ScalarFunctionImplementation, Volatility, create_udf},
        };

        let set_config_fn: ScalarFunctionImplementation = Arc::new(|args| Ok(args[1].clone()));
        ctx.register_udf(create_udf(
            "set_config",
            vec![DataType::Utf8View, DataType::Utf8View, DataType::Boolean],
            DataType::Utf8View,
            Volatility::Volatile,
            set_config_fn,
        ));
    }

    /// Register JSON functions from datafusion-functions-json
    pub fn register_json_functions(&self, ctx: &mut SessionContext) {
        datafusion_functions_json::register_all(ctx).expect("Failed to register JSON functions");
        info!("Registered JSON functions with SessionContext");
    }

    /// Check if a project has custom storage configuration (their own S3 bucket)
    async fn has_custom_storage(&self, project_id: &str, table_name: &str) -> bool {
        self.storage_configs.read().await.contains_key(&table_key(project_id, table_name))
    }

    /// Snapshot of the custom-storage (project, table) keys — one lock
    /// acquisition for callers that need many membership checks (the
    /// coalescer's fold pass). Custom storage is rare, so the clone is tiny.
    pub(crate) async fn custom_storage_keys(&self) -> HashSet<(String, String)> {
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
        // Two String allocations per call (~70 ns each, not the bottleneck);
        // a borrowed-key DashMap lookup needs an `Equivalent` wrapper type.
        self.fast_resolve_cache.get(&table_key(project_id, table_name)).map(|r| Arc::clone(r.value()))
    }

    /// True iff the scan path may skip the Delta side for `(project, table)`.
    ///
    /// Returns true only when we have resolved the table and have positive evidence it had no
    /// files; the `delta_has_files` bit is sticky-true, never sticky-false, so false means
    /// "unknown" and callers fall through to the full scan path. The internal bit is the positive
    /// `delta_has_files`; this method flips polarity once so call sites read naturally.
    pub fn delta_scan_can_be_skipped(&self, project_id: &str, table_name: &str) -> bool {
        self.delta_has_files
            .get(&table_key(project_id, table_name))
            // Acquire pairs with the Release stores; the DashMap shard lock
            // happens to order these today, but Relaxed would break the moment
            // the Arc<AtomicBool> is read outside the shard guard.
            .is_some_and(|f| !f.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Mark a (project, table) as having Delta files. Called by the flush
    /// callback after a successful commit.
    pub fn mark_delta_has_files(&self, project_id: &str, table_name: &str) {
        let key = table_key(project_id, table_name);
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
        metrics::counter!(scan_metric_names::PROVIDER_CACHE_EVICTIONS).increment(evicted as u64);
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

    /// Seed `fast_resolve_cache` and (sticky-up only) `delta_has_files` from a freshly-resolved
    /// Delta table handle.
    ///
    /// Sticky-true invariant: only flips `delta_has_files` false → true. Downgrading would skip
    /// Delta and hide rows; positive evidence (`version > 0` or `mark_delta_has_files`) is the
    /// only path to true. Cold-start with pre-existing S3 data relies on
    /// `create_or_load_delta_table` calling `DeltaTableBuilder::load()` synchronously, so the
    /// resolved handle already reflects S3 truth. A lazy loader would reopen that staleness
    /// window and break this seeding.
    async fn populate_resolve_caches(&self, project_id: &str, table_name: &str, t: &Arc<RwLock<DeltaTable>>) {
        let key = table_key(project_id, table_name);
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
        let last_written_version = self.last_written_versions.read().await.get(&table_key(project_id, table_name)).cloned();
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
        let cached = self.custom_project_tables.read().await.get(&table_key(project_id, table_name)).cloned();
        if let Some(table) = cached {
            debug!("Found custom table for project '{}' table '{}' in cache", project_id, table_name);
            return self.refresh_cached_table(table, project_id, table_name).await;
        }

        // Not in cache, create/load it
        self.get_or_create_custom_table(project_id, table_name)
            .await
            .map_err(|e| DataFusionError::Execution(format!("Failed to get or create custom table: {}", e)))
    }

    /// Load-outside-write-lock, double-check-then-insert cache shape shared by
    /// `get_or_create_unified_table`/`get_or_create_custom_table`.
    ///
    /// The load MUST happen outside the write lock: the Delta-log replay is
    /// network-bound and unbounded, and tokio's RwLock is write-preferring, so
    /// one stuck writer wedges every later reader (the 2026-07-29 total
    /// maintenance wedge). Racing first touches may both load; the double-check
    /// keeps the first insert. Returns the post-insert cache size when this
    /// call inserted, `None` on a hit, so callers log "cached" exactly once.
    async fn get_or_create_cached<K: Eq + std::hash::Hash + Clone>(
        &self, cache: &RwLock<HashMap<K, Arc<RwLock<DeltaTable>>>>, key: K, storage_uri: &str, storage_options: &HashMap<String, String>, table_name: &str,
    ) -> Result<(Arc<RwLock<DeltaTable>>, Option<usize>)> {
        if let Some(table) = cache.read().await.get(&key) {
            return Ok((Arc::clone(table), None));
        }
        let table = self.create_delta_table_internal(storage_uri, storage_options, table_name).await?;
        let mut tables = cache.write().await;
        if let Some(table) = tables.get(&key) {
            return Ok((Arc::clone(table), None));
        }
        let table_arc = Arc::new(RwLock::new(table));
        tables.insert(key, Arc::clone(&table_arc));
        Ok((table_arc, Some(tables.len())))
    }

    #[instrument(
        name = "database.get_or_create_unified_table",
        skip(self),
        fields(table.name = %table_name)
    )]
    pub async fn get_or_create_unified_table(&self, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
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

        let (table_arc, fresh_count) =
            self.get_or_create_cached(&self.unified_tables, table_name.to_string(), &storage_uri, &storage_options, table_name).await?;
        if let Some(count) = fresh_count {
            info!("Cached unified table '{}', cache now contains {} entries", table_name, count);
        }
        Ok(table_arc)
    }

    #[instrument(
        name = "database.get_or_create_custom_table",
        skip(self),
        fields(project_id = %project_id, table.name = %table_name)
    )]
    pub async fn get_or_create_custom_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
        // Get custom storage config for this project
        let configs = self.storage_configs.read().await;
        let config = configs
            .get(&table_key(project_id, table_name))
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

        // Load OUTSIDE the write lock — see `get_or_create_cached`. Worse here:
        // the load targets a TENANT's BYO bucket, so one unreachable endpoint or
        // stale credential set can pin this map's write guard indefinitely and
        // wedge every maintenance job that walks the map.
        let (table_arc, fresh_count) =
            self.get_or_create_cached(&self.custom_project_tables, table_key(project_id, table_name), &storage_uri, &storage_options, table_name).await?;
        if let Some(count) = fresh_count {
            info!("Cached custom table for project '{}' table '{}', cache now contains {} entries", project_id, table_name, count);
        }
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
        let routed = Arc::new(crate::storage::RequestClassRouter::new(log_store_client, base_store)) as Arc<dyn object_store::ObjectStore>;
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
                    ("delta.dataSkippingStatsColumns".to_string(), stats_columns_for(schema_or_default(table_name))),
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

                let schema = schema_or_default(table_name);
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

    #[instrument(
        name = "database.list_file_uris",
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

    /// Best-effort warm of the Foyer cache for parquet files just written by a flush or optimize.
    ///
    /// Reuses the read path so recent partitions don't cold-start after compaction: a ranged
    /// footer GET primes the metadata cache, and a full GET (when configured) primes the data
    /// cache. Runs in a detached, concurrency-bounded task and never affects the commit. Files
    /// are filtered to partitions within `timefusion_warm_recency_days`. `confirm` awaits the
    /// metadata pass under a deadline, leaving full bodies to the detached post-commit pass.
    /// `allow_full_files` is false during bootstrap so restart recovery does not saturate the
    /// object store.
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

        // warm_all_footers (default) warms footers for EVERY live file (tens of
        // KB each; bounded by `concurrency`); full-file warming is always
        // recency-bounded. Oldest partitions warm first so the newest land last
        // in LRU order and eviction drops the least-queried old partitions.
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
        // misses, so a post-warm hit rate would read artificially low.
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
        // Pace only the boot/background pass (confirm=None); per-flush confirm
        // warms are small and latency-relevant.
        let body_pace = (confirm.is_none() && self.config.maintenance.timefusion_warm_body_boot_files_per_sec > 0)
            .then(|| std::time::Duration::from_secs_f64(1.0 / f64::from(self.config.maintenance.timefusion_warm_body_boot_files_per_sec)));
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
                let _ = crate::storage::warm_parquet_metadata(store.as_ref(), &path, metadata_size_hint).await;
                if warm_full_files && recent {
                    let hit = match shared {
                        Some(shared) => crate::storage::warm_full_if_absent(store.as_ref(), shared, &path, &bucket_cache_key(table_path, &path)).await,
                        None => crate::storage::warm_full(store.as_ref(), &path).await,
                    };
                    fetched.fetch_add(hit as usize, std::sync::atomic::Ordering::Relaxed);
                    // Boot-path body warms are PACED (see the config comment: the
                    // unpaced variant saturated object-store bandwidth and was
                    // reverted). Only fetches sleep — cached files fly by — and
                    // post-commit confirm warms (small, bounded) stay unthrottled.
                    if hit && let Some(interval) = body_pace {
                        tokio::time::sleep(interval).await;
                    }
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
            crate::observability::record_cache_confirm_timeout();
            warn!("cache confirm exceeded {:?} for {} file(s) — proceeding uncached (commit unaffected)", deadline, count);
        }
        let fetched = fetched.load(std::sync::atomic::Ordering::Relaxed);
        crate::observability::record_cache_confirm(count as u64, fetched as u64);
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
        let preload = async move {
            let preload_all = futures::stream::iter(crate::schema::registry().list_tables()).for_each_concurrent(concurrency, |table_name| {
                let db = Arc::clone(&db);
                async move {
                    let t = std::time::Instant::now();
                    match db.resolve_table("default", &table_name).await {
                        Ok(table_ref) => {
                            // Warm via the already-resolved handle — warm_cache_for_table
                            // would redundantly resolve_table a second time.
                            let (uris, store, table_uri) = {
                                let table = table_ref.read().await;
                                let uris: Vec<String> = file_uris(&table);
                                (uris, table.log_store().object_store(None), table.table_url().to_string())
                            };
                            info!("bootstrap.phase=table_preload table={table_name} files={} elapsed_ms={}", uris.len(), t.elapsed().as_millis());
                            // Bodies warm too, but PACED and skip-if-cached
                            // (timefusion_warm_body_boot_files_per_sec; 0 restores
                            // footer-only). The unpaced variant saturated
                            // object-store bandwidth (13GB during three 1h
                            // queries) and was reverted; pacing is what makes
                            // body warmth at boot safe. Without it, windows
                            // older than the last flush burst stay cold until
                            // first query — the measured 23-31s band.
                            let bodies = db.config.maintenance.timefusion_warm_body_boot_files_per_sec > 0;
                            db.warm_cache_for_uris(store, table_uri, uris, None, bodies).await;
                        }
                        Err(e) => warn!("bootstrap.phase=table_preload table={table_name} skipped: {e}"),
                    }
                }
            });
            // Abandon warming on shutdown so in-flight S3 calls can't slow
            // a fast restart during initial boot.
            let completed = tokio::select! {
                _ = shutdown.cancelled() => false,
                _ = preload_all => true,
            };
            if completed {
                db.preload_complete.store(true, std::sync::atomic::Ordering::Release);
                db.preload_complete_notify.notify_waiters();
                info!(event = "table_preload_complete");
            }
        };
        if let Some(executor) = self.maintenance_executor.get() {
            executor.spawn(preload);
        } else {
            // Test/CLI callers can preload without starting schedulers. The
            // production server always has the isolated executor by this point.
            tokio::spawn(preload);
        }
    }

    /// Wait until preload has published all discoverable table handles. The
    /// atomic closes the `Notify` check/subscribe race and also makes late
    /// subscribers return immediately.
    async fn wait_for_preload(&self, cancel: &CancellationToken) -> bool {
        loop {
            if self.preload_complete.load(std::sync::atomic::Ordering::Acquire) {
                return true;
            }
            let notified = self.preload_complete_notify.notified();
            if self.preload_complete.load(std::sync::atomic::Ordering::Acquire) {
                return true;
            }
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = notified => {}
            }
        }
    }

    /// Atomically swap a freshly-optimized `new_table` in under the write lock, then refresh the
    /// cache for the file-set delta vs `pre_uris`: warm files added and evict files tombstoned.
    ///
    /// `pre_uris` is `None` when the caller isn't tracking the file set, so the diff is skipped.
    /// `scope` must match the partition markers used for `pre_uris`; otherwise a scoped pre-set
    /// diffed against an unscoped live set would warm every other partition. Both optimize paths
    /// — full Z-order and light — funnel through here so warm/evict cannot drift between them.
    async fn swap_and_refresh_cache(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, new_table: DeltaTable, pre_uris: Option<&HashSet<String>>, scope: &[&str],
    ) -> Vec<String> {
        // Capture live URIs off `new_table` *before* the swap moves it in.
        let live_uris: Vec<String> = scoped_file_uris(&new_table, scope);
        let (added, removed): (Vec<String>, Vec<String>) = match pre_uris {
            Some(pre) => {
                let live_set: HashSet<&str> = live_uris.iter().map(String::as_str).collect();
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
    /// [`crate::storage::RequestClassRouter`]). Every other setting
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

        // Apply storage options, falling back to config values
        if let Some(access_key) = storage_options.get("AWS_ACCESS_KEY_ID").or(self.config.aws.aws_access_key_id.as_ref()) {
            builder = builder.with_access_key_id(access_key);
        }
        if let Some(secret_key) = storage_options.get("AWS_SECRET_ACCESS_KEY").or(self.config.aws.aws_secret_access_key.as_ref()) {
            builder = builder.with_secret_access_key(secret_key);
        }
        if let Some(region) = storage_options.get("AWS_REGION").or(self.config.aws.aws_default_region.as_ref()) {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = storage_options.get("AWS_ENDPOINT_URL").or(Some(&self.config.aws.aws_s3_endpoint)) {
            builder = builder.with_endpoint(endpoint);
            // If endpoint is HTTP, allow HTTP connections
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }

        let store = builder.build()?;
        Ok(Arc::new(store))
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

/// Number of buckets the sharded dedup rewrite hashes into.
///
/// Shards partition `[0, DEDUP_BUCKET_COUNT)` into contiguous ranges, so more shards than buckets
/// would leave rows uncovered. Also acts as a runaway shard-count backstop.
const DEDUP_BUCKET_COUNT: u64 = 256;

/// One shard's staged parquet plus its (before, after) row counts — or the
/// error that stopped it. The adds come back either way, so a shard that fails
/// mid-flight still hands over what it wrote for cleanup.
type StagedShard = (Vec<deltalake::kernel::Action>, anyhow::Result<(usize, usize)>);

#[derive(Clone, Copy)]
pub(crate) struct DedupExecutionLimits {
    max_decoded_bytes: u64,
    max_concurrent_shards: usize,
    probe_hash_shards: usize,
}

#[derive(Clone)]
pub(crate) struct DedupRangeOptions {
    slice: Option<crate::maintenance_coordinator::TimeSlice>,
    dirty_key: Option<DirtyBinKey>,
    limits: Option<DedupExecutionLimits>,
}

#[derive(Clone)]
struct HotStageOptions {
    pass: TailPass,
    runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
}

struct CompactionDebtFile {
    size: i64,
    path: String,
}

/// Arrow ONE dedup bin may hold across all its concurrent shards.
///
/// Deliberately equal to the old `d_dedup_max_decoded_bytes`, because that is
/// what a bin already peaked at when it ran a single 2 GiB shard at a time.
/// Concurrency here is funded by SHRINKING the shard, not by raising the
/// ceiling: peak Arrow per bin is unchanged, so `maintenance_rewrite_sem` still
/// bounds the process the same way it did.
const DEDUP_BIN_ARROW_BUDGET: u64 = 2 * 1024 * 1024 * 1024;

/// How many shards of one dedup bin may rewrite at once.
///
/// Each shard materializes up to `decoded_budget` of Arrow outside any pool (which
/// `maintenance_rewrite_sem` exists to bound), so the count is how many fit in
/// `DEDUP_BIN_ARROW_BUDGET`. Capped at a quarter of the cores because several bins may be
/// in flight on their own permits.
fn dedup_shard_concurrency(decoded_budget: u64, cores: usize) -> usize {
    match decoded_budget {
        // No per-shard ceiling configured ⇒ one shard already holds everything.
        0 => 1,
        budget => (DEDUP_BIN_ARROW_BUDGET / budget).clamp(1, (cores / 4).max(1) as u64) as usize,
    }
}

/// Hash-shard count to keep one dedup rewrite within its byte budget. A zero budget disables
/// that ceiling.
///
/// The `streaming` branch uses a `ROW_NUMBER` window and writes straight through; it is not
/// bounded by the decoded-bytes budget because `Sorted` mode holds one partition-by group at a
/// time and `SortExec` spills under the memory pool. Sharding it would only repeat full file
/// rescans, and the coordinator runs shards serially, so it would multiply wall clock. The
/// collecting branch materialises the partition, so it keeps the bound.
fn dedup_shard_count(streaming: bool, decoded_bytes: u64, rewrite_bytes: u64, decoded_budget: u64, rewrite_budget: u64) -> u64 {
    if streaming {
        return 1;
    }
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

/// May this caller write a group UNSORTED when the sort can't be completed
/// within budget?
///
/// The asymmetry is the whole point. An unsorted file declares no
/// `sorting_columns` footer, and the reader's `derive_common_ordering` is
/// all-or-nothing — ONE of them voids the declared ordering for every scan
/// touching that partition, dropping `DedupExec` to its unbounded `full-set`
/// seen-set. Nothing ever re-sorts a converged file, so the cost is permanent.
/// Prod reached 1,263 of 2,290 active files unsorted (55%, 20 projects) this
/// way; the median one was 712 MB, i.e. ~12 GB of in-memory Arrow at prod's
/// ~17x zstd ratio — far past any in-process sort budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsortedFallback {
    /// INGEST only. The rows exist nowhere else yet, so refusing to write them
    /// would lose data; an unsorted file is the lesser evil (and is counted).
    Allow,
    /// REWRITE paths (dedup / consolidate / compact). Their inputs are already
    /// committed and already sorted, so failing the rewrite costs one cycle and
    /// the next tick retries — while replacing those inputs with a single
    /// unsorted output is an unrecoverable, partition-wide read regression.
    Forbid,
}

/// Batches a flush/rewrite writes, either eagerly held or produced on demand by
/// the streaming sort-merge. Iterating yields writer-sized chunks; the merge
/// frees each sorted run (and its encoded keys) as it is drained, so the whole
/// bucket is never materialized twice.
pub(crate) enum FlushBatches {
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
/// Equivalent to the old concat + `lexsort_to_indices` + `take` path: same multiset of rows and
/// same sort-key sequence (both are total orders by the same keys with the same `SortOptions`).
/// Rows that tie on every sort key may come out in a different relative order than the old path
/// did, but that is not a regression — `lexsort_to_indices` is unstable, so its tie order was
/// already arbitrary. The merge breaks ties by run index and nothing downstream reads tie order.
pub(crate) struct SortMergeStream {
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
fn sort_one_batch(batch: &RecordBatch, sort_idx: &[(usize, &crate::schema::SortingColumnDef)]) -> Result<RecordBatch, arrow_schema::ArrowError> {
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

/// Unify a schema-diverse batch of `RecordBatch`es to one superset schema: if
/// every batch already shares the first batch's schema this is a no-op;
/// otherwise `Schema::try_merge` computes a lossless superset (missing/nullable
/// fields unioned) and every batch is cast to it via `cast_record_batch`
/// (`add_missing=true`). `None` on merge or cast failure — the caller falls
/// back to writing the original batches unsorted.
fn unify_batch_schemas(batches: Vec<RecordBatch>) -> Option<(arrow_schema::SchemaRef, Vec<RecordBatch>)> {
    let first_schema = batches[0].schema();
    if batches.iter().all(|b| b.schema() == first_schema) {
        return Some((first_schema, batches));
    }
    let merged = arrow_schema::Schema::try_merge(batches.iter().map(|b| b.schema().as_ref().clone())).ok().map(Arc::new)?;
    let normalized = batches.iter().map(|b| deltalake::kernel::schema::cast_record_batch(b, merged.clone(), true, true)).collect::<Result<Vec<_>, _>>().ok()?;
    Some((merged, normalized))
}

fn sort_batches_by_schema(schema: &crate::schema::TableSchema, batches: Vec<RecordBatch>, skip_over_bytes: usize) -> (FlushBatches, bool) {
    use arrow::{
        compute::{SortOptions, concat_batches},
        row::{RowConverter, SortField},
    };
    let unsorted = |b: Vec<RecordBatch>| (FlushBatches::Ready(b.into_iter()), false);
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return unsorted(batches);
    }
    // `skip_over_bytes` budgets the IN-PROCESS sort in in-memory Arrow bytes —
    // NOT file bytes; they differ by the zstd ratio (~17x on prod otel data).
    // Oversize coalesced groups (bulk backfill) write unsorted rather than
    // materialize 2-3x on the flush path — correctness-safe (the footer just
    // doesn't advertise an order) and re-sorted by scheduled compaction.
    let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if total_bytes > skip_over_bytes {
        return unsorted(batches);
    }
    // Unify a schema-diverse bucket to a lossless superset schema so it still
    // flushes as ONE globally sorted file with an honest `sorting_columns`
    // footer — one unsorted file disables the reader's all-or-nothing ordering
    // pushdown for the whole scan. Incompatibility falls back to unsorted.
    let (arrow_schema, batches) = match unify_batch_schemas(batches.clone()) {
        Some(pair) => pair,
        None => {
            warn!("sort_batches_by_schema: schema unify failed, writing unsorted");
            return unsorted(batches);
        }
    };
    let sort_idx: Vec<(usize, &crate::schema::SortingColumnDef)> =
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
fn sort_batches_by_schema_reference(schema: &crate::schema::TableSchema, batches: Vec<RecordBatch>) -> (Vec<RecordBatch>, bool) {
    use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch};
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return (batches, false);
    }
    let (arrow_schema, batches) = match unify_batch_schemas(batches.clone()) {
        Some(pair) => pair,
        None => return (batches, false),
    };
    let sort_idx: Vec<(usize, &crate::schema::SortingColumnDef)> =
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
fn schema_optimize_sort_columns(schema: &crate::schema::TableSchema) -> Vec<deltalake::operations::optimize::SortColumn> {
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
/// tiebreak. Partition columns are excluded — they're encoded in the path, not the stats. Writing
/// stats for the whole wide schema instead cost a large share of process CPU in Add-stats
/// `parse_json_impl` on every log replay.
fn stats_columns_for(schema: &crate::schema::TableSchema) -> String {
    std::iter::once(schema.time_column_name())
        .chain(schema.sorting_columns.iter().map(|c| c.name.as_str()))
        .chain(schema.dedup_keys.iter().map(String::as_str))
        .chain(schema.dedup_tiebreak.as_deref())
        .filter(|c| !schema.partitions.iter().any(|p| p == c))
        // `unique` dedups preserving first-seen order; the list is a handful of columns.
        .unique()
        .join(",")
}

/// Input bytes one repair slice may cover.
///
/// A whole-file sort does not fit the light pool, so the unit must shrink. 256 MB is the only
/// value with an observed successful completion. A 64 MB target was tried and reverted: the
/// apparent throughput cliff was concurrent-bin contention, not size. If retuning, measure
/// completion time for one bin of a known size on an idle box.
const REPAIR_SLICE_TARGET_BYTES: i64 = 256 * 1024 * 1024;

/// Run the min/max/null probe for slice planning. `None` declines slicing —
/// on any error, a non-i64 sort column, or a single NULL, because a NULL would
/// fall outside every range and be dropped.
///
/// The probe is an aggregate with no ORDER BY, so it costs a scan and no sort
/// memory. Slicing then costs one scan per slice; for a repair that otherwise
/// never completes at all, re-reading is the cheap side of the trade.
async fn bin_time_range(ctx: &datafusion::prelude::SessionContext, probe: &str) -> Option<(i64, i64)> {
    use datafusion::arrow::array::Array;
    let batches = ctx.sql(probe).await.ok()?.collect().await.ok()?;
    let batch = batches.into_iter().find(|b| b.num_rows() > 0)?;
    // Timestamps arrive as Timestamp(µs); `as_primitive` over the concrete type
    // keeps this honest rather than casting blindly.
    let int_at = |i: usize| -> Option<i64> {
        let col = batch.column(i);
        if col.is_null(0) {
            return None;
        }
        arrow::compute::kernels::cast::cast(col, &arrow_schema::DataType::Int64)
            .ok()
            .map(|c| arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(&c).value(0))
    };
    let (lo, hi) = (int_at(0)?, int_at(1)?);
    // Any NULL in the sort column declines slicing outright.
    match int_at(2) {
        Some(0) | None => Some((lo, hi)),
        Some(_) => None,
    }
}

/// Split `[lo, hi]` into `slices` half-open ranges, last one inclusive of `hi`.
/// Returns `(lo, hi_exclusive_or_none)` pairs in ASCENDING order.
///
/// Slicing a repair rewrite by TIME (never by row count) is what keeps the
/// output honest: the slices are processed in the output's own sort direction
/// and appended to one writer, so the concatenation stays globally sorted and
/// every file's `sorting_columns` footer remains true. Row-count slices would
/// produce overlapping files, and two versions of one key could then land in
/// different files where keep-greatest can no longer see them together.
fn repair_slice_bounds(lo: i64, hi: i64, slices: usize) -> Vec<(i64, Option<i64>)> {
    if slices <= 1 || hi <= lo {
        return vec![(lo, None)];
    }
    let span = (hi - lo) as i128;
    (0..slices)
        .map(|i| {
            let start = lo + (span * i as i128 / slices as i128) as i64;
            let end = (i + 1 < slices).then(|| lo + (span * (i + 1) as i128 / slices as i128) as i64);
            (start, end)
        })
        // A degenerate span can produce empty ranges; they simply select nothing.
        .collect()
}

/// Interior cut points placed at equal-row quantiles of the sort column.
///
/// Equal time spans do not bound memory when rows clump by time; one slice can hold most of the
/// file and exhaust the pool. Quantiles keep the cuts monotone and contiguous in the sort
/// column's order, so concatenation stays globally sorted and each footer stays honest, while
/// flattening row count per slice. Approximate cuts are fine: the row-count guard still verifies
/// the rewrite before commit.
async fn repair_slice_cuts(ctx: &datafusion::prelude::SessionContext, bin_table: &str, col: &str, slices: usize) -> Vec<i64> {
    use datafusion::arrow::array::Array;
    if slices <= 1 {
        return Vec::new();
    }
    let exprs = (1..slices).map(|i| format!("approx_percentile_cont(arrow_cast(\"{col}\", 'Int64'), {})", i as f64 / slices as f64)).join(", ");
    let Ok(df) = ctx.sql(&format!("SELECT {exprs} FROM {bin_table}")).await else {
        return Vec::new();
    };
    let Ok(batches) = df.collect().await else {
        return Vec::new();
    };
    let Some(batch) = batches.into_iter().find(|b| b.num_rows() > 0) else {
        return Vec::new();
    };
    // Heavy ties collapse neighbouring quantiles onto one value. Deduping just
    // yields fewer, larger slices — an overlap would duplicate rows, so it is
    // the one outcome that must not survive.
    (0..batch.num_columns())
        .filter_map(|i| {
            let c = batch.column(i);
            (!c.is_null(0))
                .then(|| arrow::compute::kernels::cast::cast(c, &arrow_schema::DataType::Int64).ok())
                .flatten()
                .map(|c| arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(&c).value(0))
        })
        .sorted_unstable()
        .dedup()
        .collect()
}

/// Turn interior cut points into the same half-open, ascending tiling
/// `repair_slice_bounds` produces: `[lo, c1), [c1, c2), ... [cn, +inf)`.
fn repair_bounds_from_cuts(lo: i64, hi: i64, cuts: &[i64]) -> Vec<(i64, Option<i64>)> {
    // Deduped here rather than trusting the caller: repeated cuts are empty
    // ranges, which cost a full scan each and select nothing.
    let interior: Vec<i64> = cuts.iter().copied().filter(|&c| c > lo && c <= hi).sorted_unstable().dedup().collect();
    if interior.is_empty() {
        return vec![(lo, None)];
    }
    let starts = std::iter::once(lo).chain(interior.iter().copied());
    let ends = interior.iter().copied().map(Some).chain(std::iter::once(None));
    starts.zip(ends).collect()
}

fn schema_order_by_clause(schema: &crate::schema::TableSchema) -> String {
    if schema.sorting_columns.is_empty() {
        return String::new();
    }
    let cols = schema
        .sorting_columns
        .iter()
        .map(|c| {
            format!(
                "{} {}{}",
                crate::rollup::quoted(&c.name),
                if c.descending { "DESC" } else { "ASC" },
                if c.nulls_first { " NULLS FIRST" } else { " NULLS LAST" }
            )
        })
        .join(", ");
    format!(" ORDER BY {cols}")
}

/// Full compaction optionally sorts by the schema's timestamp-leading keys so
/// rewritten files retain tight timestamp statistics and an honest footer.
fn full_optimize_type(schema: &crate::schema::TableSchema, allow_sort: bool) -> (deltalake::operations::optimize::OptimizeType, bool) {
    choose_optimize_type(schema, false, allow_sort)
}

/// One staged bin's intent line in the staged-intent manifest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagedIntent {
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
    /// Input files this staged output replaces. Empty on pre-resume entries and
    /// on dedup entries, which are then cleanup-only. Without it an entry says
    /// only "these objects exist", never "and they may replace those" — which
    /// is enough to delete an orphan and not enough to commit one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    target_paths: Vec<String>,
    /// The staged Add actions verbatim, so a resume rebuilds the bin without
    /// re-reading a single footer. Tens of KB per entry (per-column stats over
    /// ~96 columns) against a handful of live entries.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    adds: Vec<deltalake::kernel::Add>,
}

/// Parse the append-only manifest, SKIPPING any line that doesn't decode. The
/// manifest is a cleanup aid, never a correctness input (staged parquet is
/// invisible to readers until the atomic wave commit, and VACUUM is the
/// backstop), so a torn tail from an unclean shutdown must degrade to "fewer
/// entries", never to a boot failure.
fn parse_staged_intents(contents: &str) -> Vec<StagedIntent> {
    contents.lines().filter(|l| !l.trim().is_empty()).filter_map(|line| serde_json::from_str::<StagedIntent>(line).ok()).collect()
}

/// Which staged paths to delete on boot: everything the Delta log does not reference.
///
/// A referenced path belongs to a wave that did commit, and deleting it would destroy live data.
/// The decision is pure and testable without an object store; it deliberately takes the referenced
/// set from the snapshot rather than a LIST. `now_secs` gates the rolling-deploy hazard: entries
/// younger than `STAGED_INTENT_MIN_AGE_SECS` may belong to a live instance sharing the volume and are
/// left for its own wave commit, the next boot, or VACUUM.
const STAGED_INTENT_MIN_AGE_SECS: u64 = 30 * 60;

fn staged_orphan_deletions(entries: &[StagedIntent], table_name: &str, now_secs: u64, referenced: &HashSet<String>) -> Vec<String> {
    entries
        .iter()
        .filter(|e| e.table_name == table_name)
        .filter(|e| now_secs.saturating_sub(e.recorded_at) >= STAGED_INTENT_MIN_AGE_SECS)
        .flat_map(|e| e.paths.iter())
        .filter(|p| !referenced.contains(p.as_str()))
        .cloned()
        .collect()
}

/// What boot-time resume should do with one staged-intent entry. Decided
/// WITHOUT IO so every branch is unit-testable; the caller does the one check
/// that needs the network (are the staged objects actually there, at the
/// recorded size) only for entries that reach [`ResumeVerdict::Commit`].
#[derive(Debug, PartialEq, Eq)]
enum ResumeVerdict {
    /// Another table's entry, a cleanup-only (pre-resume or dedup) entry, or one
    /// still inside the rolling-deploy age window.
    Skip,
    /// Our own Adds are already live: the commit landed before the crash. Clear
    /// the intent, never re-commit.
    AlreadyLanded,
    /// An input left the snapshot, so someone rewrote it underneath us and the
    /// staged output would resurrect removed rows or drop new ones.
    Stale,
    /// Output rows != input rows — a truncated staging. Never commit this.
    RowMismatch {
        target_rows: i64,
        staged_rows: i64,
    },
    Commit,
}

/// `live` maps every path in the current snapshot to its `numRecords`
/// (`None` = the Add carries no stats, which makes row preservation
/// unverifiable and is therefore never committable).
fn classify_resume(entry: &StagedIntent, table_name: &str, now_secs: u64, live: &HashMap<&str, Option<i64>>) -> ResumeVerdict {
    // Same age gate, same reason, as `staged_orphan_deletions`: on an
    // overlapping rolling deploy a still-running instance may be mid-staging,
    // and committing its half-written output is exactly the hazard.
    if entry.table_name != table_name
        || entry.target_paths.is_empty()
        || entry.adds.is_empty()
        || now_secs.saturating_sub(entry.recorded_at) < STAGED_INTENT_MIN_AGE_SECS
    {
        return ResumeVerdict::Skip;
    }
    // Staged parquet is uuid-named by the writer, so nobody else can produce
    // those paths — live ⇒ our commit landed (same argument as `bin_adds_live`).
    if entry.adds.iter().all(|a| live.contains_key(a.path.as_str())) {
        return ResumeVerdict::AlreadyLanded;
    }
    if !entry.target_paths.iter().all(|p| live.contains_key(p.as_str())) {
        return ResumeVerdict::Stale;
    }
    let rows = |sum: Option<i64>| sum.unwrap_or(-1);
    let target_rows: Option<i64> = entry.target_paths.iter().filter_map(|p| live.get(p.as_str())).copied().sum();
    let staged_rows: Option<i64> = entry.adds.iter().map(|a| a.get_stats().ok().flatten().map(|s| s.num_records)).sum();
    match (target_rows, staged_rows) {
        // A repair is data-preserving by construction, so this equality is the
        // ONLY thing standing between a killed-mid-staging bin and silent row loss.
        (Some(t), Some(s)) if t == s => ResumeVerdict::Commit,
        (t, s) => ResumeVerdict::RowMismatch { target_rows: rows(t), staged_rows: rows(s) },
    }
}

/// One bin rewritten to staged parquet but NOT yet committed. Uncommitted Adds
/// are invisible to Delta readers, so a wave can hold several of these while
/// other bins finish; the wave commit turns them all into one transaction.
pub(crate) struct StagedBin {
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
pub(crate) struct WaveResult {
    landed: Vec<StagedBin>,
    failed: Vec<StagedBin>,
}

/// Match a wave's committed Add actions to the absolute URIs from the exact
/// post-commit snapshot. Index manifests store absolute URIs for coverage,
/// while the Add action and Delta object store use table-relative paths; keep
/// that conversion in one pure, testable boundary.
fn wave_added_parquet(bins: &[StagedBin], live_uris: &[String]) -> Vec<(String, String, String)> {
    let by_rel: HashMap<&str, &str> =
        live_uris.iter().filter_map(|uri| crate::tantivy::search::parquet_rel_of_uri(uri).map(|rel| (rel, uri.as_str()))).collect();
    bins.iter()
        .flat_map(|bin| {
            bin.adds.iter().filter_map(|action| {
                let deltalake::kernel::Action::Add(add) = action else {
                    return None;
                };
                let rel = add.path.as_str();
                let uri = by_rel.get(rel)?;
                Some((bin.project_id.clone(), rel.to_owned(), (*uri).to_owned()))
            })
        })
        .collect()
}

/// Deferred manifest entries a backfill commits in one write.
const MANIFEST_BATCH: usize = 8;

/// A deferred entry is never left unwritten longer than this. DURABILITY, not
/// batching, is the binding constraint here: a build takes minutes and a pass on
/// this box is routinely killed by a deploy before it ends, so a count-only
/// bound of 25 against ~5 builds per pass deferred everything to a pass end that
/// never arrived — losing work the per-build write used to make durable.
const MANIFEST_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(60);

/// Which projects' deferred manifest entries are due to be committed.
///
/// Over EVERY pending project, not just the one whose build has just finished.
/// Checking only that project left the age bound unreachable for any project
/// with a single build per pass — nothing ever re-examined it, so its entries
/// waited on a pass end that this box has never reached.
fn due_manifest_flushes<T>(pending: &HashMap<String, Vec<T>>, since: &HashMap<String, std::time::Instant>) -> Vec<String> {
    pending
        .iter()
        .filter(|(project, entries)| entries.len() >= MANIFEST_BATCH || since.get(*project).is_some_and(|t| t.elapsed() >= MANIFEST_MAX_AGE))
        .map(|(project, _)| project.clone())
        .collect()
}

#[cfg(test)]
mod due_manifest_flush_tests {
    use super::*;

    /// The prod failure, as a unit: a project that built once and then went
    /// quiet must still flush on age. Prod `b6d8c86` read
    /// `tantivy_backfill_built=2, tantivy_manifest_commits=0` at 44 minutes.
    #[test]
    fn an_aged_project_flushes_even_though_another_one_is_building() {
        let pending = HashMap::from([("quiet".to_string(), vec![()]), ("busy".to_string(), vec![()])]);
        let since = HashMap::from([("quiet".to_string(), std::time::Instant::now() - MANIFEST_MAX_AGE * 2), ("busy".to_string(), std::time::Instant::now())]);
        assert_eq!(due_manifest_flushes(&pending, &since), vec!["quiet".to_string()]);
    }

    #[test]
    fn a_full_batch_flushes_regardless_of_age() {
        let pending = HashMap::from([("p".to_string(), vec![(); MANIFEST_BATCH])]);
        let since = HashMap::from([("p".to_string(), std::time::Instant::now())]);
        assert_eq!(due_manifest_flushes(&pending, &since), vec!["p".to_string()]);
        assert!(due_manifest_flushes(&HashMap::from([("p".to_string(), vec![(); MANIFEST_BATCH - 1])]), &since).is_empty());
    }
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
/// `data_change` distinguishes hot compaction from dedup. `false` (compaction) preserves every row,
/// so the fork's conflict checker downgrades to snapshot isolation and a wave can commit next to
/// concurrent ingest appends without the OCC ladder. `true` (dedup) drops rows, so those actions
/// must not claim data-preserving. `wave_operation` derives the `DeltaOperation` from the same flag
/// so the two cannot drift.
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

/// Charged memory for the wave-boundary brake: cgroup `memory.current` (what
/// memcg OOM-kills on), statm RSS as fallback. `None` on platforms with neither
/// (dev macOS) — the brake then never engages, safe for a non-prod box.
pub(crate) fn process_memory_bytes() -> Option<usize> {
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        && let Ok(v) = raw.trim().parse::<usize>()
    {
        // Discount ALL clean page cache, active included: the kernel reclaims
        // clean file pages before OOM-killing anything, and the hot tier is
        // DESIGNED to keep its pages active — charging them made the brake
        // starve the very compaction that keeps queries fast, and poisoned the
        // rate the scan valve differentiates (a streaming read looks like a
        // heap burst). Anon-dominated `used` is the only series whose growth
        // predicts a kill.
        return Some(v.saturating_sub(cgroup_reclaimable_file_bytes().unwrap_or(0)));
    }
    crate::observability::process_rss_bytes()
}

/// Clean page cache from cgroup v2 `memory.stat`. `None` when unreadable (the
/// caller then charges the full `memory.current`, the conservative direction).
fn cgroup_reclaimable_file_bytes() -> Option<usize> {
    reclaimable_file_bytes(&std::fs::read_to_string("/sys/fs/cgroup/memory.stat").ok()?)
}

/// `file` less the part that cannot be dropped without first writing it back.
/// Dirty and under-writeback pages stay charged: reclaim has to wait on IO for
/// them, so they are pressure in a way a clean page never is.
fn reclaimable_file_bytes(stat: &str) -> Option<usize> {
    let field = |name: &str| stat.lines().find_map(|line| line.strip_prefix(name)?.trim().parse::<usize>().ok());
    Some(field("file ")?.saturating_sub(field("file_dirty ").unwrap_or(0) + field("file_writeback ").unwrap_or(0)))
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

/// Concurrent footer verifications while clearing false repair suspects.
///
/// These are metadata-cache-backed ranged reads, not rewrites, so this is bounded by object-store
/// round trips. How many times one repair tick may re-select after clearing false suspects. Must
/// exceed a project's run of false suspects or the tick cannot reach real work. Every sealed day
/// adds a fresh batch of untagged suspects; newest-first walks them first. `repair_verified_sorted`
/// is in-process only, so a restart resets to the newest candidate.
const REPAIR_RESELECT_ROUNDS: usize = 64;

/// Wave cap for one tail pass. A wave serves each project at most one bin, so this is also
/// the per-project file ceiling.
///
/// Packing keeps 12: bins are minutes apart on `today` and run frequently; a never-ending
/// pack tick would hold the light pool against the next flush. Repair uses a much larger cap
/// because its 8640s budget is meant to chew through a backlog; capping it at 12 files per
/// pass would make a large backlog converge at only 12 files per hour and fail to outrun
/// deploy churn. The real bounds are the deadline and memory brake enforced inside
/// `round_robin_bins`; this constant only stops a runaway loop.
const fn max_waves(pass: TailPass) -> usize {
    match pass {
        TailPass::Pack => 12,
        TailPass::Repair => 512,
    }
}

const REPAIR_VERIFY_CONCURRENCY: usize = 16;

/// How many verified-sorted paths survive a restart. One path is ~150 bytes, so
/// 200k is ~30 MB on disk and bounds boot-time load. Newest wins, because the
/// walk is newest-first and the recent tail is what the next pass will re-probe.
const REPAIR_VERIFIED_PERSIST_CAP: usize = 200_000;

/// Compressed input admitted to one coordinator L0 sort. Production zstd
/// expansion is about 17x, so 16 MiB leaves room in the 512 MiB decoded pool
/// without turning every flush-file rewrite into a multi-gigabyte spill.
/// Sorted runs are merged separately toward the 256/512 MiB physical targets.
const COORDINATOR_L0_SORT_TARGET_BYTES: i64 = 16 * 1024 * 1024;

const SORTED_RUN_TAG: &str = "delta-rs.optimize.sort_by";

/// Coverage identity tags to carry from a rewrite's inputs onto its outputs.
///
/// Tags are only carried when every input agrees on every one of them. `tag_sorted` used to
/// copy only the sorted-run tag, which silently erased rollup tier coverage because
/// `recover_rollup_coverage` reads exactly these tags. A union of disagreeing slices would
/// claim coverage over gaps, and `rollup_slice_complete` requires an exact slice match anyway,
/// so identical-only is correct. The common case is one publication split by
/// `timefusion_writer_max_file_bytes`, where all inputs share the same tags and the merged
/// output keeps them.
fn carried_coverage_tags(targets: &[deltalake::kernel::Add]) -> HashMap<String, String> {
    use crate::maintenance_coordinator::{TAG_GENERATION, TAG_PROJECT, TAG_SLICE_END, TAG_SLICE_START, TAG_SOURCE, TAG_SOURCE_FINGERPRINT};
    const COVERAGE_TAGS: [&str; 6] = [TAG_SOURCE, TAG_PROJECT, TAG_SLICE_START, TAG_SLICE_END, TAG_SOURCE_FINGERPRINT, TAG_GENERATION];
    let Some(first) = targets.first() else { return HashMap::new() };
    let value = |add: &deltalake::kernel::Add, tag: &str| add.tags.as_ref().and_then(|tags| tags.get(tag).cloned().flatten());
    COVERAGE_TAGS
        .into_iter()
        .map(|tag| {
            let expected = value(first, tag)?;
            targets.iter().all(|add| value(add, tag).as_deref() == Some(expected.as_str())).then_some((tag.to_owned(), expected))
        })
        .collect::<Option<HashMap<_, _>>>()
        .unwrap_or_default()
}

fn is_sorted_run(tags: &HashMap<String, Option<String>>) -> bool {
    tags.get(SORTED_RUN_TAG).is_some_and(|v| v.as_deref() == Some("true"))
}

/// Files whose newest event is within `SEAL_LAG` of now may still receive appends or DV-merge
/// rewrites, so compacting them races concurrent commits and would need re-compaction. Only sealed
/// time slices are compacted. The 15-minute lag was chosen because a shorter lag churned the cache
/// too fast for 1h-window queries to warm the bodies.
fn seal_micros_now() -> i64 {
    const SEAL_LAG_MICROS: i64 = 15 * 60 * 1_000_000;
    crate::support::now_micros() - SEAL_LAG_MICROS
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
    /// Parse the raw Add stats JSON.
    ///
    /// The snapshot's parsed-stats column is not materialized on this path, so the kernel
    /// `stat_min_i64`/`stat_max_i64` accessors return `None` for every file. Event-time binning
    /// previously trusted those accessors and silently selected nothing, so it now parses the raw
    /// stats JSON instead.
    fn from_stats(path: String, size: i64, is_sorted_run: bool, stats: Option<&str>) -> Self {
        let range = stats.and_then(Database::event_time_range_from_stats);
        Self { path, size, is_sorted_run, event_range: range }
    }
}

/// Select one coordinator compaction unit in two levels: first turn unsorted
/// L0 files into small sorted runs, then merge only sorted runs toward the
/// physical target. Mixing the two levels forces a full sort of the entire
/// 256/512 MiB group; in production those units either exhausted the 512 MiB
/// pool or timed out after five minutes and made no durable progress.
fn select_coordinator_compaction_candidates(candidates: Vec<TailAdd>, target: i64) -> Vec<String> {
    let has_unsorted = candidates.iter().any(|add| !add.is_sorted_run);
    let limit = if has_unsorted { COORDINATOR_L0_SORT_TARGET_BYTES } else { target };
    let mut bytes = 0i64;
    let mut selected = Vec::new();
    for add in candidates {
        // A file at or above target is converged and never packing's work,
        // whatever its tags say — sortedness is Repair's job. This skip must be
        // unconditional: it once sat behind `!has_unsorted`, and since the
        // sorted-run tag is written only by OPTIMIZE, it effectively never ran
        // and units re-rewrote converged half-GB files past every deadline.
        if add.size >= target {
            continue;
        }
        if has_unsorted && add.is_sorted_run {
            continue;
        }
        if !selected.is_empty() && bytes.saturating_add(add.size) > limit {
            break;
        }
        bytes = bytes.saturating_add(add.size);
        selected.push(add);
    }
    if !has_unsorted && selected.len() < 2 {
        return Vec::new();
    }
    selected.into_iter().map(|add| add.path).collect()
}

fn coordinator_slice_target(pass: TailPass, input_files: usize, bytes_in: i64) -> Option<i64> {
    match pass {
        TailPass::Repair => Some(REPAIR_SLICE_TARGET_BYTES),
        TailPass::Pack if input_files == 1 && bytes_in > COORDINATOR_L0_SORT_TARGET_BYTES => Some(COORDINATOR_L0_SORT_TARGET_BYTES),
        TailPass::Pack => None,
    }
}

/// Pick the files one light-optimize bin should rewrite. Pure so the policy is testable without a
/// Delta table.
///
/// `sorted_run_cap` bounds which already-tagged sorted runs are re-admitted; the cold tier
/// passes `i64::MAX` and the hot tier passes `target/2`. Files >= 7/8 target are excluded as
/// converged. Binning is by event time so output runs are time-disjoint and range-pruning works.
///
/// Two scopes:
/// - `today`: full bin-packing; fold sub-cap sorted runs and skip converged files unless
///   converged and unsorted (repair candidate).
/// - sealed date in `repair_markers`: repair only; admit unsorted files.
///
/// Repair exists because an unsorted file that survives midnight stays unsorted forever,
/// forcing `DedupExec` into its full-set path for scans touching that date.
fn hot_bin_admits(path: &str, today_marker: &str, repair_markers: &[String], size: i64, sorted_run: bool, repairable: bool, policy: &HotBinPolicy<'_>) -> bool {
    if path.contains(today_marker) {
        // A repair pass owns sealed dates only. Admitting today's files too
        // would let packing work back into a budget reserved for repair.
        if policy.pass == TailPass::Repair {
            return false;
        }
        let converged_done = size >= policy.converged() && (sorted_run || !repairable);
        let over_cap_run = size >= policy.sorted_run_cap && sorted_run;
        return !(converged_done || over_cap_run);
    }
    // Sealed dates: every un-verified file is a suspect and the FOOTER decides
    // — the sorted-run tag records the optimizer's INTENT and can disagree with
    // the footer queries actually read, so admission must not key off it.
    // `verified_sorted` makes the footer check one ranged read per file per
    // process. `repair_max_bytes` keeps a tick bounded (multi-GB legacy files
    // belong to the off-box CLI), and quarantined candidates are skipped so the
    // queue behind them drains (`REPAIR_QUARANTINE_AFTER`).
    repairable
        && size <= policy.repair_max_bytes
        && !policy.verified_sorted.contains(path)
        && policy.failures.get(path).map_or(0, |n| *n.value()) < REPAIR_QUARANTINE_AFTER
        && repair_markers.iter().any(|m| path.contains(m.as_str()))
}

/// Pool bytes one escalated flush sort needs to finish without being refused.
///
/// 512 MB was too small: a single sort's merge phase exceeded it and a second sort was admitted
/// alongside, exhausting the whole pool. Every such refusal writes the group unsorted, and one
/// unsorted file disables the reader's all-or-nothing footer ordering for its entire partition.
const MIN_SPILL_SORT_BYTES: usize = 1 << 30;

/// How many escalated flush sorts may run at once on a pool of `pool_bytes`.
///
/// At the 1 GB default this is ONE: escalated sorts serialize. That is the trade
/// the gate was always meant to make — "queueing costs latency on an already
/// oversized group; losing the slice costs the partition's footer ordering on
/// every later scan" — and concurrency is bought back by raising
/// `TIMEFUSION_FLUSH_SORT_POOL_MB`, not by pretending a sort fits in half a
/// gigabyte.
pub(crate) fn flush_sort_permits(pool_bytes: usize) -> usize {
    (pool_bytes / MIN_SPILL_SORT_BYTES).max(1)
}

/// Largest file a repair pass will admit, derived from the time it actually has.
///
/// A repair pass may admit roughly `budget * INPUT_BYTES_PER_SEC` and still finish. Never smaller
/// than the configured value, so raising the knob still widens the reach.
pub(crate) fn repair_reach_bytes(configured: i64, budget: std::time::Duration) -> i64 {
    configured.max(budget_bytes(budget))
}

/// Conservative input bytes one hot-tail rewrite sustains per second.
///
/// Measured on an uncontended 1.19 GB repair rewrite. Packing bins ran faster under concurrency,
/// so this stays the conservative floor for both.
const INPUT_BYTES_PER_SEC: i64 = 460_000;

fn budget_bytes(budget: std::time::Duration) -> i64 {
    (budget.as_secs().min(i64::MAX as u64) as i64).saturating_mul(INPUT_BYTES_PER_SEC)
}

/// Fraction of the tick a packing bin may consume, leaving the rest as margin.
///
/// Sizing a bin to fill the whole budget leaves nothing for the commit, and any dip below the
/// assumed rate turns the tick's output to zero. A single global rate estimate cannot be tight
/// because rewrite rate varies widely by table; it can only be safe. Half the budget puts the
/// slowest table safely inside the tick while letting fast tables run several rounds.
const PACK_BUDGET_FRACTION: u32 = 2;

/// Largest bin a packing pass will assemble, derived from the time it has.
///
/// A bin that cannot be rewritten inside the tick is discarded at the deadline and the next
/// tick re-selects the same files, so an oversized target produces nothing while burning the
/// whole budget. Capping at [`PACK_BUDGET_FRACTION`] of the budget removes stragglers. The
/// whole policy shrinks together with this value, so a freshly packed run still lands above
/// `sorted_run_cap` and `converged`; otherwise smaller outputs would be re-selected forever.
pub(crate) fn pack_target_bytes(configured: i64, budget: std::time::Duration) -> i64 {
    configured.min(budget_bytes(budget / PACK_BUDGET_FRACTION)).max(1)
}

/// Consecutive staging failures after which a repair candidate stops being offered for the rest
/// of the process.
///
/// A repair bin is one whole-file sort. If its working set does not fit, the failure is
/// deterministic, not transient. Without quarantine that file would be re-selected identically
/// on every pass and consume the wave. Three attempts, not one, because staging can also fail for
/// transient reasons (OCC race, concurrent rewrite, restart mid-stage). A success clears the
/// count.
const REPAIR_QUARANTINE_AFTER: u32 = 3;

/// Sort parallelism ladder a repair bin is retried at before its pool exhaustion is believed.
///
/// `REPAIR_SORT_PARTITIONS` (16) is the fast setting, but exhaustion there used to be treated
/// as proof a file is impossible. The working set is a function of both file size and
/// parallelism: each partition has an unspillable merge operator, so 16 partitions means 16
/// unspillable merges competing for the pool. A single-partition sort has no merge and spills
/// within its fair share. Exhaustion at 16 says nothing about 1, so the ladder descends
/// through 16, 4, 1. Exhaustion at the floor is believed.
const REPAIR_SORT_PARTITION_LADDER: [usize; 3] = [REPAIR_SORT_PARTITIONS, 4, 1];

/// What a failed repair staging costs the candidate: the parallelism to retry
/// at next (`None` = nothing cheaper left) and the strike step to charge.
///
/// Split out from the failure arm so the escalation rule is testable without a
/// pool to exhaust. A pool exhaustion is only "deterministic" once it happens
/// at the bottom of [`REPAIR_SORT_PARTITION_LADDER`]; above the bottom it buys
/// a retry at lower parallelism and charges a single strike, so a file that
/// keeps failing for a genuinely unrelated reason still parks after
/// [`REPAIR_QUARANTINE_AFTER`].
fn repair_failure_action(exhausted: bool, level: usize) -> (Option<usize>, u32) {
    let next = level + 1;
    match (exhausted, REPAIR_SORT_PARTITION_LADDER.get(next)) {
        // Room left on the ladder: retry cheaper, charge one strike.
        (true, Some(&partitions)) => (Some(partitions), 1),
        // Bottom of the ladder, or a non-pool failure: existing semantics.
        (true, None) => (None, REPAIR_QUARANTINE_AFTER),
        (false, _) => (None, 1),
    }
}

/// travel together through planning, admission and binning.
pub(crate) struct HotBinPolicy<'a> {
    /// SEALED dates (yesterday backwards) scanned for footer repair only.
    /// Empty restores the old today-only pass.
    repair_dates: &'a [String],
    target_size: i64,
    min_files: usize,
    sorted_run_cap: i64,
    /// Largest file a hot tick will rewrite to repair its footer. Bigger ones
    /// are legacy (pre-`timefusion_writer_max_file_bytes`) and belong to the
    /// off-box `timefusion optimize` CLI, not a 5-minute tick.
    repair_max_bytes: i64,
    /// Which of the two disjoint jobs this pass is running. See [`TailPass`].
    pass: TailPass,
    /// Suspects a footer read already cleared — see `repair_verified_sorted`.
    verified_sorted: &'a dashmap::DashSet<String>,
    /// Consecutive staging failures per candidate — see `REPAIR_QUARANTINE_AFTER`.
    failures: &'a dashmap::DashMap<String, u32>,
}

/// The hot tail runs two jobs that were one, and merging them broke both.
///
/// Packing is continuous (today's small files, small units). Footer repair is finite (a fixed
/// backlog of large sealed files, each a whole-file global sort). Sharing the same short cron
/// gave repair a slice of a budget it cannot fit in, and `stage_hot_bin` discards a bin that
/// outruns the budget and re-selects the identical file next time. Splitting them lets each get
/// the budget its unit size actually needs.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TailPass {
    /// Today's partition only: bin-pack small files. Never touches sealed dates.
    Pack,
    /// Sealed dates only: rewrite ONE footer-less file per project so the
    /// reader's all-or-nothing ordering claim survives. Never touches today.
    Repair,
}

impl HotBinPolicy<'_> {
    /// A file at or past 7/8 of target is "converged" — re-selecting it alone
    /// would rewrite it 1→1 forever.
    fn converged(&self) -> i64 {
        let cap = self.target_size.max(1);
        cap - cap / 8
    }
}

pub(crate) fn select_tail_bin(adds: &[TailAdd], target_size: i64, min_files: usize, sorted_run_cap: i64, seal_micros: i64, pass: TailPass) -> Vec<String> {
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
    // A lone repair file is real work and needs no `min_files` company. On a
    // Repair pass EVERY candidate is repair work whatever its size —
    // `hot_bin_admits` already scoped it, and a small sealed footer-less file
    // poisons its date's scan exactly as thoroughly as a 1 GiB one.
    let is_candidate = |repair: bool| pass == TailPass::Repair || repair;
    let repairs_present = fresh.iter().any(|(_, _, _, repair)| is_candidate(*repair));
    if fresh.len() < min_files && !repairs_present {
        return vec![];
    }
    fresh.sort_unstable_by_key(|(_, min, _, _)| *min);
    // A repair pass rewrites exactly one file: NEWEST first (the most recent
    // footer-less date is the wall users hit; repairing it widens the window),
    // and among equals the SMALLEST (so one un-finishable file can't
    // head-of-line block its date).
    if pass == TailPass::Repair {
        return fresh.iter().max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.2.cmp(&a.2))).map(|(path, _, _, _)| vec![path.to_string()]).unwrap_or_default();
    }
    // Pack the earliest contiguous slice up to `cap` → one time-disjoint run
    // per tick. Small commit converges quickly and shrinks the conflict window;
    // later ticks pack the next (strictly later) slice.
    let mut bytes = 0i64;
    let mut files: Vec<String> = vec![];
    for (path, _, size, _) in fresh.iter().filter(|(_, _, _, r)| !*r) {
        let (path, size) = (*path, *size);
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
    // Gap rule, for TODAY only: once a project has no packable slice left, spend
    // the tick rewriting one oversized unsorted file instead. Today's partition
    // does converge, so the gap reliably appears — which is why this works here
    // and did NOT work for sealed dates, where a still-ingesting project always
    // has a packable slice and the gap never comes (prod 2026-08-07: the sealed
    // backlog fell 1,243 -> ~940 and stopped dead). Sealed dates are the
    // `TailPass::Repair` cron's job now, not this fallback's.
    if files.is_empty()
        && let Some((path, _, _, _)) = fresh.iter().find(|(_, _, _, repair)| *repair)
    {
        return vec![path.to_string()];
    }
    files
}

#[cfg(test)]
mod decode_tests {
    /// The gate bounds decode heap; it used to bound polls, which reserved the worst case for every
    /// batch. Dashboard batches are much smaller than the worst case, so most of the budget went
    /// unused while polls queued. Charging for produced heap must not move the ceiling.
    #[test]
    fn a_decode_claim_is_proportional_to_heap_and_never_exceeds_one_reader_slot() {
        let k = super::DECODE_UNITS_PER_READER;
        let nominal = super::NOMINAL_DECODE_BATCH_BYTES;

        // THE INVARIANT: a worst-case batch still claims a whole slot, so N
        // concurrent worst-case decodes are still capped at N readers.
        assert_eq!(super::decode_units(nominal), k, "a full-size batch must still cost a full slot");
        assert_eq!(super::decode_units(nominal * 4), k, "an oversized batch is clamped, never exceeding one slot");

        // Unknown size (a stream's first poll) is charged conservatively — the
        // pre-split behaviour — so nothing regresses before we have evidence.
        assert_eq!(super::decode_units(0), k, "unknown batch size claims a whole slot");

        // The actual prod-measured batch size: 1 unit of 16, i.e. 16x the
        // concurrency at the same ceiling.
        assert_eq!(super::decode_units(210 * 1024), 1, "a 0.21 MB prod batch costs one unit, not a whole slot");

        // Monotonic, and never zero (zero would break progress guarantees).
        let mut prev = 0;
        for mb in [0.2_f64, 1.0, 10.0, 50.0, 100.0, 145.0] {
            let u = super::decode_units((mb * 1024.0 * 1024.0) as u64);
            assert!(u >= 1 && u <= k, "{mb} MB -> {u} units, out of range 1..={k}");
            assert!(u >= prev, "claim must not decrease as batches grow: {mb} MB -> {u} after {prev}");
            prev = u;
        }
    }

    /// `None` here wipes EVERY date's rollup coverage for the table, so the bar
    /// for reaching it has to be "the batch genuinely cannot say". A hard
    /// downcast to microseconds meant a millisecond column — or any other
    /// precision — hit that path silently.
    #[test]
    fn a_write_batch_names_its_partitions_at_any_timestamp_precision() {
        use arrow::{
            array::{ArrayRef, Date32Array, RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray},
            datatypes::{Field, Schema},
        };
        use std::sync::Arc;
        let day = 86_400_000_000i64;
        let batch = |name: &str, column: ArrayRef| {
            RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new(name, column.data_type().clone(), true)])), vec![column]).expect("batch")
        };
        let expect = |batch: RecordBatch| {
            let mut dates = super::batch_hours(&batch).expect("the batch names its partitions").into_keys().collect::<Vec<_>>();
            dates.sort();
            dates
        };

        // Two rows a day apart, written at microsecond and millisecond precision.
        let micros = TimestampMicrosecondArray::from(vec![0i64, day]).with_timezone("UTC");
        assert_eq!(expect(batch("timestamp", Arc::new(micros))), ["1970-01-01", "1970-01-02"]);
        let millis = TimestampMillisecondArray::from(vec![0i64, day / 1_000]).with_timezone("UTC");
        assert_eq!(expect(batch("timestamp", Arc::new(millis))), ["1970-01-01", "1970-01-02"], "a millisecond column used to wipe the whole table");

        // No timestamp at all: the `date` partition column is what Delta
        // partitions on, and it answers for both its physical encodings.
        assert_eq!(expect(batch("date", Arc::new(Date32Array::from(vec![0, 1])))), ["1970-01-01", "1970-01-02"]);
        assert_eq!(expect(batch("date", Arc::new(StringArray::from(vec!["2026-08-01"])))), ["2026-08-01"]);

        // Only a batch that carries neither may force the source-wide wipe.
        let opaque = batch("name", Arc::new(StringArray::from(vec!["x"])));
        assert!(super::batch_hours(&opaque).is_none(), "a batch with no date and no timestamp must still fall back");

        // The HOURS are what make a repair cheap: a batch confined to 14:00 must
        // mark one hour, not the day. A batch with only a `date` cannot name an
        // hour and must mark all of them.
        let at = |hours: i64| TimestampMicrosecondArray::from(vec![hours * 3_600_000_000]).with_timezone("UTC");
        let hours = |batch: RecordBatch| super::batch_hours(&batch).expect("hours").into_values().fold(0u32, |mask, hour| mask | hour);
        assert_eq!(hours(batch("timestamp", Arc::new(at(14)))), 1 << 14, "one hour of enrichment must mark one hour");
        assert_eq!(hours(batch("date", Arc::new(Date32Array::from(vec![0])))), crate::rollup::ALL_HOURS, "no timestamp means no hour to name");
    }

    #[test]
    fn bounded_dml_windows_mark_only_overlapping_hours() {
        const HOUR: i64 = 3_600_000_000;
        let masks = super::window_hour_masks(14 * HOUR + 30, 16 * HOUR).expect("bounded window");
        assert_eq!(masks, vec![("1970-01-01".into(), (1 << 14) | (1 << 15))]);

        let masks = super::window_hour_masks(23 * HOUR, 25 * HOUR).expect("cross-day window");
        assert_eq!(masks, vec![("1970-01-01".into(), 1 << 23), ("1970-01-02".into(), 1)]);
        assert!(super::window_hour_masks(HOUR, HOUR).is_none());
    }

    /// A pool exhaustion must be retried at a parallelism that can fit before the candidate is
    /// quarantined.
    ///
    /// A single `Resources exhausted` at the top parallelism forces `mode=full` dedup and breaks
    /// large-window dashboards for that tenant, so the ladder retries cheaper parallelism first.
    #[test]
    fn a_pool_exhaustion_walks_down_the_parallelism_ladder_before_it_is_believed() {
        // Top of the ladder: buy a retry at lower parallelism, charge ONE strike
        // (not the whole threshold) so this alone cannot park the file.
        assert_eq!(super::repair_failure_action(true, 0), (Some(4), 1));
        assert_eq!(super::repair_failure_action(true, 1), (Some(1), 1));

        // Bottom of the ladder: nothing cheaper left, so the exhaustion IS
        // deterministic and keeps the original whole-threshold semantics.
        let (next, step) = super::repair_failure_action(true, 2);
        assert_eq!(next, None, "single-partition sort is the floor");
        assert_eq!(step, super::REPAIR_QUARANTINE_AFTER, "believed at the floor: park it");

        // A non-pool failure is transient and keeps the 3-strike rule at every level.
        for level in 0..super::REPAIR_SORT_PARTITION_LADDER.len() {
            assert_eq!(super::repair_failure_action(false, level), (None, 1), "transient failures never escalate or park early");
        }
    }

    /// The ladder must end at 1: a single-partition sort is the only setting
    /// with no `SortPreservingMergeExec` at all, and that exec cannot spill.
    #[test]
    fn the_parallelism_ladder_descends_to_a_single_partition() {
        let ladder = super::REPAIR_SORT_PARTITION_LADDER;
        assert_eq!(*ladder.last().unwrap(), 1, "the floor must have no merge exec");
        assert_eq!(ladder[0], super::REPAIR_SORT_PARTITIONS, "the ladder starts at the fast setting");
        assert!(ladder.windows(2).all(|w| w[0] > w[1]), "strictly descending: {ladder:?}");
    }

    #[test]
    fn repair_bin_date_extracts_the_partition_and_orders_recent_first() {
        let bin = |d: &str| vec![format!("project_id=abc/date={d}/part-00000-x.zstd.parquet")];
        assert_eq!(super::repair_bin_date(&bin("2026-07-30")), "2026-07-30");
        assert_eq!(super::repair_bin_date(&[]), "", "no file sorts last");
        assert_eq!(super::repair_bin_date(&["no-date-here.parquet".to_string()]), "", "no date sorts last");

        let mut planned: Vec<(String, Vec<String>)> =
            vec![("old".into(), bin("2026-05-30")), ("blocking".into(), bin("2026-07-30")), ("older".into(), bin("2026-06-09"))];
        planned.sort_by(|a, b| super::repair_bin_date(&b.1).cmp(super::repair_bin_date(&a.1)));
        assert_eq!(planned.iter().map(|(p, _)| p.as_str()).collect::<Vec<_>>(), vec!["blocking", "older", "old"]);
    }
}

#[cfg(test)]
mod repair_batch_tests {
    /// A repair bin is ONE whole file — prod's worst is 1,088,634,971 bytes —
    /// so its sort spills many runs and `SortPreservingMergeExec` allocates per
    /// run per batch. A packing bin (a handful of small files, shallow merge)
    /// does not. Sharing packing's 2048 is what made the merge ask 64.3 MB
    /// against 25.3 MB of headroom and fail the whole rewrite.
    #[test]
    fn repair_sorts_with_smaller_batches_than_packing() {
        use datafusion::execution::runtime_env::RuntimeEnv;
        let batch = |state: &datafusion::execution::session_state::SessionState| state.config().options().execution.batch_size;
        let pack = super::build_optimize_session_state(0, std::sync::Arc::new(RuntimeEnv::default()));
        let repair = super::build_optimize_session_state_tuned(
            0,
            std::sync::Arc::new(RuntimeEnv::default()),
            Some("256"),
            Some(super::UncappedSort { partitions: super::REPAIR_SORT_PARTITIONS, reservation_bytes: Some(super::REPAIR_SORT_RESERVATION_BYTES) }),
        );
        assert_eq!(batch(&repair), 256, "repair shrinks the sort's indivisible admission unit");
        // Repair runs ONE bin at a time, so the many-concurrent-sorters premise
        // behind `MAINTENANCE_MAX_PARTITIONS` does not apply to it. Paying the
        // cap anyway sorted ~1 GiB with two threads on a 48-core box — ~40
        // minutes, against an 18-21 minute process lifetime.
        assert_eq!(repair.config().options().execution.target_partitions, super::REPAIR_SORT_PARTITIONS, "repair sorts wide");
        assert!(
            repair.config().options().execution.target_partitions > pack.config().options().execution.target_partitions,
            "packing keeps the cap because it runs many bins at once"
        );
        assert!(batch(&repair) < batch(&pack), "packing keeps the wider batch: {} vs {}", batch(&pack), batch(&repair));
    }
}

/// Split staged bins into (committable, stale) against the live file set of the
/// refreshed snapshot. One conflicting file used to fail one commit of one bin;
/// with a batched wave commit, dropping only the stale bin's actions keeps the
/// blast radius identical to the per-bin path (the dropped bin's files are just
/// re-selected next tick). Pure + generic so the wave-assembly test needs no
/// object store.
fn split_live_bins<T>(bins: Vec<T>, targets: impl Fn(&T) -> &[String], live: &HashSet<String>) -> (Vec<T>, Vec<T>) {
    bins.into_iter().partition(|bin| targets(bin).iter().all(|t| live.contains(t)))
}

/// Whether a bin's OWN staged Adds are active in the snapshot — i.e. its commit
/// landed. Distinguishes "another writer rewrote my targets" (staged parquet is
/// garbage) from "my own earlier attempt landed and then errored" (staged
/// parquet is LIVE DATA). See the self-landed split in `commit_wave`.
/// A bin with no Adds can't have landed anything.
fn bin_adds_live(bin: &StagedBin, live: &HashSet<String>) -> bool {
    let mut adds = bin.adds.iter().filter_map(|a| match a {
        deltalake::kernel::Action::Add(add) => Some(add.path.as_str()),
        _ => None,
    });
    adds.next().is_some_and(|first| live.contains(first) && adds.all(|p| live.contains(p)))
}

/// Per-project outcome of one round's staging. `Result<Option<T>>` overloaded `None`: "converged"
/// (drop for the tick) vs "bin vanished under a concurrent rewrite" (project still has work —
/// dropping it would silence its compaction for the rest of the tick).
enum BinOutcome<T> {
    /// Bin staged; project stays in the round-robin.
    Staged(T),
    /// Tail converged — nothing left this tick; drop from the rotation.
    Converged,
    /// Selection went stale (concurrent rewrite); keep the project pending so
    /// the next round's re-plan serves it a fresh bin.
    Retry,
}

/// Parallelism for one packing sort, bound by the tighter of two independent limits.
///
/// * Memory: `k` concurrent bins may spend at most a quarter of the pack pool on
///   unspillable up-front reservations.
/// * CPU: `k` bins together may use at most half the box, leaving the rest for reads.
///
/// Never below `MAINTENANCE_MAX_PARTITIONS`; this only lifts that cap. Derived from the box
/// profile rather than pinned, because a fixed width that fits a large box would consume most
/// of a small box's pool before sorting a single row.
fn pack_sort_partitions(pack_pool_bytes: usize, k: usize, cores: usize) -> usize {
    /// `maintenance_session_config`'s `sort_spill_reservation_bytes`.
    const DEFAULT_RESERVATION_BYTES: usize = 33_554_432;
    let k = k.max(1);
    let memory_bound = pack_pool_bytes / (4 * k * DEFAULT_RESERVATION_BYTES);
    let cpu_bound = cores / 2 / k;
    memory_bound.min(cpu_bound).max(MAINTENANCE_MAX_PARTITIONS)
}

/// Where a deadline-truncated dedup sweep resumes: the served-item cursor
/// wrapped into the current work list. The cursor only ever grows (each tick
/// adds what it served), so the modulo is what makes successive short ticks walk
/// the whole list instead of re-serving its head.
fn sweep_resume_offset(len: usize, cursor: usize) -> usize {
    if len == 0 { 0 } else { cursor % len }
}

/// Rotate only the sealed tail of a sweep work list, leaving the first
/// `sealed_from` items (today) pinned at the front.
///
/// Rotation exists so a deadline-truncated tick resumes into sealed work it has
/// not seen. Today must be exempt: it is re-dirtied by every flush and is what
/// the hot queries read, so it has to be swept on every tick rather than once
/// per full rotation. Harmless while the window was today+1; load-bearing at a
/// 35-day certification window, where first-time passes over never-certified
/// days truncate ticks often — precisely when today would rotate out.
fn rotate_sealed_tail<T>(work: &mut Vec<T>, sealed_from: usize, cursor: usize) {
    let sealed_from = sealed_from.min(work.len());
    let mut sealed = work.split_off(sealed_from);
    let offset = sweep_resume_offset(sealed.len(), cursor);
    sealed.rotate_left(offset);
    work.append(&mut sealed);
}

/// Decrement-on-drop gauge, so a bin that times out or panics still clears its
/// slot rather than reading as permanently busy.
fn in_flight_guard(counter: &std::sync::atomic::AtomicU64) -> impl Drop + use<'_> {
    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    scopeguard::guard(counter, |counter| {
        counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    })
}

/// A wave-boundary safety brake. Two levels: `Degrade` keeps a SERVICE FLOOR
/// (one project, concurrency 1, for the rest of the tick) so a chronic overload
/// signal throttles compaction instead of starving it; `Stop` ends the tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Brake {
    Degrade(&'static str),
    Stop(&'static str),
}

#[allow(clippy::too_many_arguments)] // scheduling params are positional by design; a struct would just rename them
async fn round_robin_bins<F, Fut, T, C, CFut>(
    projects: Vec<String>, max_rounds: usize, concurrency: usize, deadline: std::time::Instant, on_truncate: impl Fn(usize, &[String]),
    should_pause: impl Fn() -> Option<Brake>, commit_each_bin: bool, op: F, commit_wave: C,
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
                crate::observability::maintenance_stats().light_optimize_ticks_degraded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(round, served = pending.len(), reason, event = "light_optimize_wave_degraded");
            }
            None => {}
        }
        // Admission control INSIDE the round: the boundary check alone let RSS
        // grow 60GB within one round before it could fire. Sampling as each bin
        // is admitted stops NEW work the moment the line is crossed; in-flight
        // bins finish (aborting wastes the sort holding the memory), and
        // deferred projects stay `pending` for the next boundary check.
        let mut outcomes = std::pin::pin!(
            futures::stream::iter(std::mem::take(&mut pending))
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
        );
        // Carry forward projects that still have work; converged or failed
        // projects drop out for the rest of the tick.
        let (mut staged, mut staged_seen) = (Vec::new(), 0usize);
        while let Some((project_id, outcome)) = outcomes.next().await {
            match outcome {
                Ok(BinOutcome::Staged(bin)) => {
                    staged_seen += 1;
                    pending.push(project_id);
                    // Per-bin commits must also be per-bin in TIME. Collecting
                    // the round first made a finished bin wait on its slowest
                    // sibling, and a repair round's siblings are minutes apart —
                    // so a restart mid-round threw away rewrites that were
                    // already complete on S3. Prod 2026-08-10/11: EVERY tier
                    // drop arrived via `staged_intent_resumed` on a LATER pass,
                    // never from a bin that staged and committed in one.
                    match commit_each_bin {
                        true => failed += commit_wave(vec![bin], round).await,
                        false => staged.push(bin),
                    }
                }
                Ok(BinOutcome::Retry) => pending.push(project_id),
                Ok(BinOutcome::Converged) => {}
                // A dropped bin is the one outcome that always wants a reason —
                // the aggregate "failed for N bins" names no project or cause.
                Err(error) => {
                    warn!(project_id, round, %error, event = "light_optimize_bin_failed");
                    failed += 1;
                }
            }
        }
        // Did the round SURVIVE to its commit, and with how many bins? This
        // line bisects the two failure stories: `staged=0` means the outcome
        // was never `Staged`; a resume that logged but not this means the
        // round's future was dropped before the stream drained.
        info!(round, staged = staged_seen, pending = pending.len(), failed, event = "round_staged");
        // Packing commits ONCE per wave, which is what made wave commits ~40x
        // rarer than the per-bin commits they replaced (2026-07-29: ~130 commits
        // a tick drove OCC ladders to attempt 9-20). Right for packing, whose
        // bins are many and quick; repair committed each bin as it arrived above.
        if !staged.is_empty() {
            failed += commit_wave(staged, round).await;
        }
    }
    failed
}

fn choose_optimize_type(schema: &crate::schema::TableSchema, allow_zorder: bool, allow_sort: bool) -> (deltalake::operations::optimize::OptimizeType, bool) {
    use deltalake::operations::optimize::OptimizeType;
    if allow_zorder && !schema.z_order_columns.is_empty() {
        return (OptimizeType::ZOrder(schema.z_order_columns.clone()), false);
    }
    let sort_cols = schema_optimize_sort_columns(schema);
    if allow_sort && !sort_cols.is_empty() { (OptimizeType::SortBy(sort_cols), true) } else { (OptimizeType::Compact, false) }
}

/// Consolidation opportunistically upgrades SortBy to SortByDedup. Removing
/// physically redundant versions preserves TimeFusion's logical DedupExec view,
/// so the fork intentionally retains Optimize's `data_change=false` contract:
/// concurrent appends remain outside the selected-file rewrite and visible,
/// while concurrent removal of a source file still conflicts.
///
/// This is only a space/read-amplification optimization, not a convergence
/// proof. Bin packing may strand versions of one key in different terminal
/// output runs; authoritative physical collapse remains owned by the dedup
/// engine, whose input scope contains every version of each rewritten key.
fn consolidate_optimize_type(schema: &crate::schema::TableSchema, allow_sort: bool) -> (deltalake::operations::optimize::OptimizeType, bool) {
    use deltalake::operations::optimize::{DedupConfig, OptimizeType, SortColumn};
    match choose_optimize_type(schema, false, allow_sort) {
        (OptimizeType::SortBy(cols), true) if !schema.dedup_keys.is_empty() => {
            let tiebreak = schema.dedup_tiebreak.as_ref().map(|tb| SortColumn { column: tb.clone(), descending: true, nulls_first: false });
            (OptimizeType::SortByDedup(cols, DedupConfig { columns: schema.dedup_keys.clone(), tiebreak }), true)
        }
        other => other,
    }
}

/// Pure builder for parquet `WriterProperties` at a compression tier.
///
/// Lives outside `impl Database` so unit tests can exercise tier/encoding/bloom decisions without
/// S3/MinIO. `declare_sorted` may be `true` only for paths that actually sort rows in schema order;
/// optimize/compact/recompress rewrite rows into Z-order or concatenation and must pass `false`.
///
/// Rows per row group are derived from the configured byte target and the schema's estimated row
/// width. Parquet's byte cap under-reports the finished row group, so the row cap is the binding
/// lever.
fn row_group_row_count(schema: &crate::schema::TableSchema, target_bytes: usize) -> usize {
    /// Encoded-but-uncompressed bytes a value of this type costs. Variable-width
    /// types are the telemetry payload (body/attributes/resource), so they
    /// dominate; the constants are deliberately rough and only need to put the
    /// row width in the right order of magnitude.
    fn width(data_type: &str) -> usize {
        match data_type.split(',').next().unwrap_or(data_type).trim() {
            "Boolean" => 1,
            "Int32" | "Date32" | "Float32" => 4,
            "Int64" | "Float64" | "UInt64" => 8,
            t if t.starts_with("Timestamp") => 8,
            "Variant" => 256,
            t if t.starts_with("List") => 64,
            // Utf8 and anything unrecognised: a telemetry string column.
            _ => 48,
        }
    }
    let row_bytes: usize = schema.fields.iter().map(|f| width(&f.data_type)).sum::<usize>().max(1);
    // Floor keeps footer/dictionary overhead from dominating on narrow tables;
    // ceiling is parquet's own default, so this can only ever make row groups
    // SMALLER than the behaviour it replaces.
    (target_bytes / row_bytes).clamp(32_768, 1_048_576)
}

fn build_writer_properties(
    parquet_cfg: &crate::config::ParquetConfig, schema: &crate::schema::TableSchema, zstd_level: i32, declare_sorted: bool,
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
    let sort_key_names: HashSet<&str> = schema.sorting_columns.iter().map(|c| c.name.as_str()).collect();

    // Note: do NOT call `set_bloom_filter_fpp` at the global level — parquet-rs
    // treats any global bloom setter (other than `set_bloom_filter_enabled`)
    // as implicit enable, which then uses the default NDV (~1M) and triggers
    // massive bloom buffer allocations on every column. We set fpp per-column
    // only, for the columns we actually want blooms on.
    let builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(zstd_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap())))
        .set_max_row_group_bytes(Some(max_row_group_size))
        // Exact companion to the byte cap above, which does not bind — see
        // `row_group_row_count`.
        .set_max_row_group_row_count(Some(row_group_row_count(schema, max_row_group_size)))
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

/// The table's declared ordering, expressed against a leg's OUTPUT schema.
///
/// Only the LEADING run of sorting columns that survived projection is claimed —
/// a query that projects `timestamp` away gets no claim rather than a false one.
/// `sorted` is the leg's own attestation that its rows are actually in that
/// order; `None` means claim nothing, which is always safe (the plan sorts).
///
/// Shared by both legs that can make the claim: the in-memory source via
/// `declare_ordering`, and the hot tier's `ArrowSource` via `with_output_ordering`.
pub(crate) fn table_ordering(table_name: &str, out: &SchemaRef, sorted: bool) -> Option<datafusion::physical_expr::LexOrdering> {
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    let table = crate::schema::get_schema(table_name).filter(|_| sorted)?;
    LexOrdering::new(
        table
            .sorting_columns
            .iter()
            .map_while(|sc| {
                let idx = out.index_of(&sc.name).ok()?;
                Some(PhysicalSortExpr::new(
                    Arc::new(PhysicalColumn::new(&sc.name, idx)),
                    arrow::compute::SortOptions { descending: sc.descending, nulls_first: sc.nulls_first },
                ))
            })
            .collect::<Vec<_>>(),
    )
}

#[derive(Debug, Clone)]
pub struct ProjectRoutingTable {
    default_project: String,
    database: Arc<Database>,
    schema: SchemaRef,
    _batch_queue: Option<Arc<crate::write::BatchQueue>>,
    table_name: String,
    /// When true, INSERTs commit straight to Delta (`skip_queue=true`),
    /// bypassing the BufferedWriteLayer (WAL + MemBuffer). Backs the
    /// `{table}__bulk` alias for backfills / DLQ drains that must not pressure
    /// the live MemBuffer. Reads route to the same underlying Delta table.
    skip_queue: bool,
}

impl ProjectRoutingTable {
    pub fn new(
        default_project: String, database: Arc<Database>, schema: SchemaRef, batch_queue: Option<Arc<crate::write::BatchQueue>>, table_name: String,
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
        filters.iter().find_map(crate::read::optimizers::extract_project_id_from_expr)
    }

    fn bounded_otel_scan_reason(&self, filters: &[Expr], limit: Option<usize>) -> Option<&'static str> {
        let mut conjuncts = Vec::new();
        fn collect_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
            if let Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) = expr {
                collect_conjuncts(left, out);
                collect_conjuncts(right, out);
            } else {
                out.push(expr);
            }
        }
        for filter in filters {
            collect_conjuncts(filter, &mut conjuncts);
        }
        let bounded = conjuncts.iter().any(|expr| Self::is_bounding_predicate(expr))
            || self.extract_time_range_from_filters(&conjuncts.into_iter().cloned().collect::<Vec<_>>()).is_some_and(|(lower, _)| lower != i64::MIN);
        Self::raw_otel_scan_reason(&self.table_name, filters, limit, bounded)
    }

    /// Predicates that bound a scan but that `extract_time_range_from_filters`
    /// does not decompose: `date = '…'` restricts it to one partition, and
    /// `BETWEEN` is a timestamp range it does not see. Both are shapes
    /// `rollup::match_aggregate` accepts, so the guard must accept them too —
    /// otherwise enforce mode rejects TimeFusion's own rollup build query.
    fn is_bounding_predicate(expr: &Expr) -> bool {
        matches!(expr, Expr::Between(between) if !between.negated && matches!(between.expr.as_ref(), Expr::Column(c) if c.name == "timestamp"))
            || matches!(expr, Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, .. }) if matches!(left.as_ref(), Expr::Column(c) if c.name == "date"))
    }

    fn raw_otel_scan_reason(table_name: &str, filters: &[Expr], limit: Option<usize>, lower_timestamp_bound: bool) -> Option<&'static str> {
        if !matches!(table_name, "otel_logs_and_spans" | "otel_metrics") {
            return None;
        }
        if !filters.iter().any(|filter| crate::read::optimizers::extract_project_id_from_expr(filter).is_some()) {
            return Some("missing exact project_id filter");
        }
        (limit.is_none() && !lower_timestamp_bound).then_some("missing timestamp lower bound or scan limit")
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

    /// True for columns that support exact pushdown — the table provider fully applies the
    /// filter and DataFusion can drop the `FilterExec`.
    ///
    /// Only true partition columns qualify: Delta's partition pruning is exact, and partition values
    /// are compared exactly inside MemBuffer. Other columns were previously listed, but MemBuffer's
    /// best-effort physical-expr compilation can silently fall back to "no filter"; with exact
    /// pushdown the filter is gone and rows leak through.
    fn is_pushdown_column(column_name: &str) -> bool {
        matches!(column_name, "project_id" | "date")
    }

    /// Apply time-series specific optimizations to filters
    fn apply_time_series_optimizations(&self, filters: &[Expr]) -> DFResult<Vec<Expr>> {
        use crate::read::optimizers::time_range_partition_pruner;

        // Resolve the schema-declared time column for this table; falls back to
        // "timestamp" when the schema isn't registered (custom/dynamic tables).
        let time_column = crate::schema::get_schema(&self.table_name).map(|s| s.time_column_name().to_string()).unwrap_or_else(|| "timestamp".to_string());

        let optimized_filters: Vec<Expr> = filters
            .iter()
            .cloned()
            .chain(filters.iter().flat_map(|filter| {
                let date_filters = time_range_partition_pruner::timestamp_to_date_filters(filter, &time_column);
                if !date_filters.is_empty() {
                    debug!("Added {} date partition filter(s) for {} on column {}", date_filters.len(), self.table_name, time_column);
                }
                date_filters
            }))
            .collect();

        // Check if project_id filter is present
        if !crate::read::optimizers::ProjectIdPushdown::has_project_id_filter(&optimized_filters) {
            debug!("Query missing project_id filter - may scan all partitions");
        }

        Ok(optimized_filters)
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

    /// Attach the table's declared ordering to an in-memory source. Failure to
    /// attach is never fatal: an undeclared source is merely slower.
    fn declare_ordering(source: MemorySourceConfig, sorted: bool, table_name: &str, out: &SchemaRef) -> MemorySourceConfig {
        let Some(ordering) = table_ordering(table_name, out, sorted) else { return source };
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
        include_files: Option<&HashSet<String>>, exclude_files: Option<&HashSet<String>>, row_selections: Option<&HashMap<String, Vec<u64>>>,
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
        let select_started = std::time::Instant::now();
        let file_selection: Option<Vec<String>> = include_files.or_else(|| exclude_files.filter(|e| !e.is_empty())).and_then(|_selection| {
            // Scoped so the (non-Send) file-view iterator drops before any await.
            // `collect::<Option<_>>` reproduces the bail-the-whole-selection
            // semantics: one unmappable URI aborts the restriction entirely.
            table
                .get_file_uris()
                .ok()?
                .filter(|u| u.ends_with(".parquet") && include_files.map_or_else(|| !exclude_files.is_some_and(|e| e.contains(u)), |files| files.contains(u)))
                .map(|u| crate::tantivy::search::parquet_rel_of_uri(&u).map(str::to_string))
                .collect::<Option<Vec<String>>>()
        });
        // Row-selection pushdown: per-file matching ordinals keyed by rel path.
        // Purely narrowing — files without an entry scan normally — so unlike
        // `file_selection` an unmappable URI just drops that file's selection.
        let ordinal_selections: HashMap<String, Vec<u64>> = row_selections
            .into_iter()
            .flatten()
            .filter_map(|(uri, ords)| Some((crate::tantivy::search::parquet_rel_of_uri(uri)?.to_string(), ords.clone())))
            .collect();
        if file_selection.is_some() || !ordinal_selections.is_empty() {
            use deltalake::delta_datafusion::{FileSelection, MissingSelectedFilePolicy};
            if let Some(sel) = &file_selection {
                metrics::counter!(scan_metric_names::PRUNED_FILES).increment(sel.len() as u64);
                debug!(
                    "tantivy file pruning: {}/{} scanning {} files (excluded {})",
                    cache_key.0,
                    self.table_name,
                    sel.len(),
                    exclude_files.map_or(0, |e| e.len())
                );
            }
            let session_state = state.as_any().downcast_ref::<datafusion::execution::context::SessionState>().cloned();
            // Same DV opt-in as the cached-provider path below: without it,
            // `parquet_pushdown_enabled` is false on any DeletionVectors-feature
            // table, so every tantivy-split leg scanned the whole window with NO
            // parquet predicate (prod 2026-08-20: a 4h delta leg emitted 2.11M
            // rows for a trace_id equality matching 0; 24h SELECT * died at the
            // full-set dedup 2 GiB cap). Actual DV-bearing FILES still disable
            // the predicate per-file inside the fork — this only lifts the
            // blanket feature-level gate, exactly like the cached path.
            let mut builder = table.table_provider().with_pushdown_with_deletion_vectors(true);
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
            metrics::counter!(scan_metric_names::PRUNED_SELECT_US).increment(select_started.elapsed().as_micros() as u64);
            let build_started = std::time::Instant::now();
            let provider: Arc<dyn TableProvider> = Arc::new(builder.build().await.map_err(|e| DataFusionError::External(Box::new(e)))?);
            metrics::counter!(scan_metric_names::PRUNED_BUILD_US).increment(build_started.elapsed().as_micros() as u64);
            let scan_started = std::time::Instant::now();
            let plan = self.scan_via_provider(provider, state, projection, filters, limit).await;
            metrics::counter!(scan_metric_names::PRUNED_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
            metrics::counter!(scan_metric_names::PRUNED_CALLS).increment(1);
            return plan;
        }

        // Per-(project,table) provider cache: only rebuild when the Delta
        // snapshot version changes. Provider construction is parameter-
        // independent so the cached value is correct for every query at
        // the same version. Measured: ~30 ms p95 of pure provider-build
        // overhead per query under load before this cache. Cache hits
        // skip the whole `table.table_provider().with_session(...).await`
        // chain.
        let current_version = table.version().unwrap_or(0);
        // Resolve or install a OnceCell for this (key, version). Optimistic
        // read path: `get()` takes only a per-shard READ lock so concurrent
        // hits don't serialize (the old entry()-per-call write lock did, under
        // 300+ readers); the write path runs only on miss/version change,
        // which is seconds apart per project. The expensive provider build runs
        // OUTSIDE any lock — concurrent tasks await the same cell's single init.
        let ttl = self.database.config.cache.provider_cache_ttl();
        // Lookup is by EXACT version against the key's small recent-version
        // ring (`PROVIDER_VERSION_RETENTION`): a query always gets the provider
        // for its own resolved handle's version, never an older retained one.
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
            metrics::counter!(scan_metric_names::PROVIDER_CACHE_MISSES).increment(1);
        } else {
            metrics::counter!(scan_metric_names::PROVIDER_CACHE_HITS).increment(1);
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
                metrics::counter!(scan_metric_names::PROVIDER_BUILD_TOTAL).increment(1);
                metrics::counter!(scan_metric_names::PROVIDER_BUILD_US_TOTAL).increment(started.elapsed().as_micros() as u64);
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
            metrics::counter!(scan_metric_names::PROVIDER_BUILD_ABANDONED).increment(1);
        }

        self.scan_via_provider(provider, state, projection, filters, limit).await
    }

    /// Build one Delta leg for complete/no Tantivy coverage, or two disjoint
    /// legs when sidecar coverage is partial. The covered leg is narrowed by
    /// the Tantivy id set; the uncovered leg evaluates the original filters.
    /// Both selections come from the exact snapshot held by `table`, so a
    /// concurrent compaction can at worst make the query use the older
    /// snapshot—it cannot move a file between the two legs or drop it.
    #[allow(clippy::too_many_arguments)]
    async fn scan_delta_with_tantivy(
        &self, table: &DeltaTable, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>, id_filter: Option<&Expr>,
        covered_files: Option<&HashSet<String>>, zero_hit_files: Option<&HashSet<String>>, row_selections: Option<&HashMap<String, Vec<u64>>>,
        query_time_range: Option<(i64, i64)>, bloom_rejected: Option<&HashSet<String>>, date_restrict: Option<&HashSet<String>>,
        file_restrict: Option<&HashSet<String>>,
    ) -> DFResult<Vec<Arc<dyn ExecutionPlan>>> {
        let narrow = |filters: &[Expr]| filters.iter().cloned().chain(id_filter.cloned()).collect::<Vec<_>>();
        // Per-date dedup skip: restrict this call's whole file universe to one
        // side of the certified/uncertified split, so two calls partition the
        // in-window files exactly once between them. A file whose URI carries
        // no `date=` partition cannot be attributed, so it fails this test and
        // lands on the uncertified (still-deduped) side — the safe default.
        let in_dates = |uri: &String| date_restrict.is_none_or(|dates| crate::storage::date_partition_of(uri).is_some_and(|d| dates.contains(&d.to_string())));
        // Per-FILE dedup skip: the same one-side-of-the-split restriction as
        // `date_restrict`, one level finer. A URI that yields no relative path
        // cannot be attributed to either side, so it fails this test and lands
        // on the uncertified (still-deduped) side — the safe default.
        let in_files = |uri: &String| file_restrict.is_none_or(|files| crate::tantivy::search::parquet_rel_of_uri(uri).is_some_and(|rel| files.contains(rel)));
        let in_leg = |uri: &String| in_dates(uri) && in_files(uri);
        // Bloom-rejected rels apply to EVERY branch here — unlike
        // `zero_hit_files` they must reach the raw (uncovered) leg, which is
        // where the un-prunable file mass lives. `exclude_files` is ignored
        // by `scan_delta_table` whenever an include set exists, so the split
        // path filters the live universe instead of passing excludes down.
        let is_rejected = |uri: &String| bloom_rejected.is_some_and(|r| crate::tantivy::search::parquet_rel_of_uri(uri).is_some_and(|rel| r.contains(rel)));
        // Full-URI exclude set for the single-provider paths (include=None,
        // so exclude semantics apply), merged with any zero-hit excludes.
        let merged_exclude = |table: &DeltaTable| -> Option<HashSet<String>> {
            let rejected: HashSet<String> = bloom_rejected
                .filter(|r| !r.is_empty())
                .map(|_| table.get_file_uris().ok().map(|uris| uris.filter(|u| u.ends_with(".parquet") && is_rejected(u)).collect()).unwrap_or_default())
                .unwrap_or_default();
            match (rejected.is_empty(), zero_hit_files) {
                (true, _) => None, // caller falls back to zero_hit_files alone
                (false, Some(zero)) => Some(rejected.union(zero).cloned().collect()),
                (false, None) => Some(rejected),
            }
        };
        let (lo, hi) = query_time_range.unwrap_or((i64::MIN, i64::MAX));
        let Some(covered) = covered_files else {
            let narrowed = narrow(filters);
            let merged = merged_exclude(table);
            let exclude = merged.as_ref().or(zero_hit_files);
            // Under a split `exclude` is not enough — `scan_delta_table`
            // ignores excludes whenever an include exists, and we need an
            // include to bound this call to its side. Build it explicitly.
            //
            // Keyed on EITHER restriction. Keyed on `date_restrict` alone, a
            // file-only split built no include at all, so both legs scanned the
            // whole table and every certified row was counted twice.
            let include: Option<HashSet<String>> = (date_restrict.is_some() || file_restrict.is_some())
                .then(|| {
                    Ok::<_, DataFusionError>(
                        table
                            .get_file_uris()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .filter(|u| u.ends_with(".parquet") && uri_date_in_window(u, lo, hi) && in_leg(u) && !is_rejected(u))
                            .filter(|u| !exclude.is_some_and(|e| e.contains(u)))
                            .collect::<HashSet<String>>(),
                    )
                })
                .transpose()?;
            if include.as_ref().is_some_and(HashSet::is_empty) {
                return Ok(Vec::new()); // nothing on this side of the split
            }
            return Ok(vec![self.scan_delta_table(table, state, projection, &narrowed, limit, include.as_ref(), exclude, row_selections).await?]);
        };

        // Everything from here to the `plans` return is the routed-only work the
        // attribution could not see. Timed as a whole, with the file-listing
        // step timed separately: materializing every URI is the one step whose
        // cost scales with the table rather than with the query.
        let scan_started = std::time::Instant::now();
        let mut bloom_pruned_any = false;
        let uris_started = std::time::Instant::now();
        let all_uris = table.get_file_uris().map_err(|e| DataFusionError::External(Box::new(e)))?.collect::<Vec<_>>();
        metrics::counter!(scan_metric_names::TANTIVY_URIS_US).increment(uris_started.elapsed().as_micros() as u64);
        metrics::counter!(scan_metric_names::TANTIVY_LIVE_FILES).increment(all_uris.len() as u64);
        let live = all_uris
            .into_iter()
            .filter(|uri| uri.ends_with(".parquet") && uri_date_in_window(uri, lo, hi))
            .filter(|uri| {
                let rejected = is_rejected(uri);
                bloom_pruned_any |= rejected;
                !rejected
            })
            .filter(in_leg);
        let (mut indexed, raw): (HashSet<String>, HashSet<String>) = live.partition(|uri| covered.contains(uri));
        // Charge the fast path's LOSER, one counter per failing conjunct (they
        // are not exclusive — a scan can be defeated by two at once, and which
        // ones co-occur is the whole question).
        metrics::counter!(scan_metric_names::TANTIVY_SCAN_CALLS).increment(1);
        metrics::counter!(scan_metric_names::TANTIVY_RAW_FILES).increment(raw.len() as u64);
        for (defeated, name) in [
            (!raw.is_empty(), scan_metric_names::TANTIVY_SPLIT_RAW),
            (bloom_pruned_any, scan_metric_names::TANTIVY_SPLIT_BLOOM),
            (date_restrict.is_some() || file_restrict.is_some(), scan_metric_names::TANTIVY_SPLIT_DATE),
        ] {
            if defeated {
                metrics::counter!(name).increment(1);
            }
        }
        // Complete coverage keeps the established single-provider fast path.
        // The split is only needed when the exact snapshot has raw debt.
        if raw.is_empty() && !indexed.is_empty() && !bloom_pruned_any && date_restrict.is_none() {
            let narrowed = narrow(filters);
            metrics::counter!(scan_metric_names::TANTIVY_FASTPATH).increment(1);
            let plan = self.scan_delta_table(table, state, projection, &narrowed, limit, None, zero_hit_files, row_selections).await?;
            metrics::counter!(scan_metric_names::TANTIVY_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
            return Ok(vec![plan]);
        }
        if let Some(zero) = zero_hit_files {
            indexed.retain(|uri| !zero.contains(uri));
        }

        let mut plans = Vec::with_capacity(2);
        if !indexed.is_empty() {
            let narrowed = narrow(filters);
            plans.push(self.scan_delta_table(table, state, projection, &narrowed, limit, Some(&indexed), None, row_selections).await?);
        }
        if !raw.is_empty() {
            plans.push(self.scan_delta_table(table, state, projection, filters, limit, Some(&raw), None, None).await?);
        }
        // Under a date split an empty side is a real empty side; the
        // unrestricted fallback below would read the OTHER side's files.
        if plans.is_empty() && date_restrict.is_some() {
            metrics::counter!(scan_metric_names::TANTIVY_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
            return Ok(Vec::new());
        }
        if plans.is_empty() {
            if bloom_pruned_any {
                // Every in-window file was bloom-rejected: the needle provably
                // matches nothing in Delta. An EMPTY include selection yields a
                // schema-correct zero-file scan (the fork deselects all files),
                // NOT the unrestricted fallback below — falling through would
                // undo the pruning exactly when it worked best.
                let none: HashSet<String> = HashSet::new();
                plans.push(self.scan_delta_table(table, state, projection, filters, limit, Some(&none), None, None).await?);
            } else {
                // A time window can be wholly outside the snapshot's partition
                // dates. Keep the provider path available as a conservative
                // fallback rather than manufacturing an empty plan with a
                // subtly different schema.
                plans.push(self.scan_delta_table(table, state, projection, filters, limit, None, None, None).await?);
            }
        }
        metrics::counter!(scan_metric_names::TANTIVY_SCAN_US).increment(scan_started.elapsed().as_micros() as u64);
        Ok(plans)
    }

    /// Delta-only scan shared by `scan()`'s two Delta-alone callers.
    ///
    /// Re-derives `skip_dedup` against the resolved table so the fingerprint verdict applies to
    /// the exact snapshot being read. When granted, restores the pushed `limit`; this is sound
    /// because nothing above a Delta-only scan drops rows except the tombstone filter. The old
    /// `output_projection.is_none()` check silently revoked every skip because the tombstone marker
    /// alone makes `output_projection` `Some`. `readmit_mutable_filters` decides whether a granted
    /// skip also re-admits version-mutable-column predicates; safe because a granted skip means no
    /// MemBuffer leg is in play.
    #[allow(clippy::too_many_arguments)]
    async fn scan_delta_only(
        &self, state: &dyn Session, projection: Option<&Vec<usize>>, optimized_filters: &[Expr], unstripped_filters: &[Expr], project_id: &str,
        query_time_range: Option<(i64, i64)>, dedup_keys: &[String], pre_skip_dedup: bool, tombstone: &Option<String>, orig_limit: Option<usize>,
        limit: Option<usize>, readmit_mutable_filters: bool, tantivy_id_filter: Option<&Expr>, tantivy_covered_files: Option<&HashSet<String>>,
        tantivy_exclude: Option<&HashSet<String>>, tantivy_row_selections: Option<&HashMap<String, Vec<u64>>>, bloom_rejected: Option<&HashSet<String>>,
    ) -> DFResult<(bool, Vec<Arc<dyn ExecutionPlan>>, Vec<Arc<dyn ExecutionPlan>>)> {
        let mut delta_only_filters = optimized_filters.to_vec();
        let delta_table = self.database.resolve_table(project_id, &self.table_name).await?;
        let table = delta_table.read().await;
        let (verdict, certified_dates) = match dedup_keys.is_empty() || !self.database.config.maintenance.timefusion_read_dedup_skip_swept {
            true => (DedupSkipVerdict::Disabled, HashSet::new()),
            false => match query_time_range {
                None => (DedupSkipVerdict::NoWindow, HashSet::new()),
                Some(w) => self.database.dedup_window_certified(&table, project_id, &self.table_name, w),
            },
        };
        let skip_dedup = pre_skip_dedup && verdict.granted();
        // Partial certification → the certified dates still skip. Only when the
        // window is NOT wholly certified (the plain skip above is cheaper) and
        // only here on the Delta-only path, where no MemBuffer leg can hold an
        // uncertified newer version.
        let per_date_dates: HashSet<String> = match !skip_dedup
            && self.database.config.maintenance.timefusion_read_dedup_skip_per_date
            && !certified_dates.is_empty()
            && !dedup_keys.is_empty()
        {
            true => certified_dates,
            false => HashSet::new(),
        };
        // Complement within the window: what the DedupExec leg must still read.
        // Derived from the same `window_dates` enumeration certification used,
        // so the two sides provably partition the window's dates.
        let uncertified_dates: HashSet<String> = match per_date_dates.is_empty() {
            true => HashSet::new(),
            false => query_time_range
                .and_then(|(lo, hi)| window_dates(lo, hi))
                .into_iter()
                .flatten()
                .map(|d| d.to_string())
                .filter(|d| !per_date_dates.contains(d))
                .collect(),
        };
        // Per-FILE split, tried only where the per-DATE one did not already
        // claim the window: within an uncertified date, the FILES a sweep proved
        // clean can still skip when no uncertified file overlaps them. Empty
        // unless `timefusion_read_dedup_skip_per_file` is on.
        let (certified_files, uncertified_files) = match !skip_dedup && per_date_dates.is_empty() && !dedup_keys.is_empty() {
            true => query_time_range.map_or_else(Default::default, |window| self.database.certified_file_split(&table, project_id, &self.table_name, window)),
            false => Default::default(),
        };
        if skip_dedup && readmit_mutable_filters {
            let mutable = Self::version_mutable_columns(&self.table_name);
            let leg_safe = |f: &Expr| {
                !Self::references_tombstone(&self.table_name, f) && !mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name)))
            };
            delta_only_filters.extend(unstripped_filters.iter().filter(|f| !leg_safe(f) && !Self::references_tombstone(&self.table_name, f)).cloned());
        }
        // Restoring the pushed limit is only sound when nothing above the
        // scan drops rows — the tombstone filter does, regardless of dedup.
        let eff_limit = if skip_dedup && tombstone.is_none() { orig_limit } else { limit };
        let plans = self
            .scan_delta_with_tantivy(
                &table,
                state,
                projection,
                &delta_only_filters,
                eff_limit,
                tantivy_id_filter,
                tantivy_covered_files,
                tantivy_exclude,
                tantivy_row_selections,
                query_time_range,
                bloom_rejected,
                (!per_date_dates.is_empty()).then_some(&uncertified_dates),
                (!certified_files.is_empty()).then_some(&uncertified_files),
            )
            .await?;
        // The certified side: the same scan restricted to certified date
        // partitions, returned separately so the caller can union it ABOVE
        // DedupExec instead of feeding it through.
        let certified_plans = match (per_date_dates.is_empty(), certified_files.is_empty()) {
            (true, true) => Vec::new(),
            (by_date_empty, _) => {
                self.scan_delta_with_tantivy(
                    &table,
                    state,
                    projection,
                    &delta_only_filters,
                    eff_limit,
                    tantivy_id_filter,
                    tantivy_covered_files,
                    tantivy_exclude,
                    tantivy_row_selections,
                    query_time_range,
                    bloom_rejected,
                    (!by_date_empty).then_some(&per_date_dates),
                    (!certified_files.is_empty()).then_some(&certified_files),
                )
                .await?
            }
        };
        if !certified_plans.is_empty() {
            let metric = if certified_files.is_empty() { scan_metric_names::DEDUP_SKIPPED_PER_DATE } else { scan_metric_names::DEDUP_SKIPPED_PER_FILE };
            metrics::counter!(metric).increment(1);
        }
        Ok((skip_dedup, plans, certified_plans))
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
        metrics::counter!(scan_metric_names::PROVIDER_SCAN_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::PROVIDER_SCAN_US_TOTAL).increment(started.elapsed().as_micros() as u64);
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
            Some((min, _)) if min != i64::MIN => Some(crate::support::now_micros().saturating_sub(min)),
            _ => None,
        }
    }

    /// Selected file bytes above which one scan is recorded as a process-risk candidate. Not a
    /// limit: nothing is refused at this size.
    ///
    /// Sized above the release threshold so it names genuinely large scans rather than every
    /// dashboard that misses the fast path.
    const WIDE_SCAN_OVERSIZE_BYTES: u64 = 1024 * 1024 * 1024;

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
        let selected = selected_file_work(&plan);
        if let Some((files, bytes)) = selected
            && files <= mem.timefusion_wide_scan_max_files
            && bytes <= mem.timefusion_wide_scan_max_mb.saturating_mul(1 << 20)
        {
            return plan;
        }
        // Observation only, nothing is refused — see `wide_scan_oversize_total`.
        // The gate below bounds how MANY wide scans decode at once, never how
        // much any one of them decodes, so this is the only place that can see
        // a single query large enough to take the process down.
        if let Some((files, bytes)) = selected
            && bytes > Self::WIDE_SCAN_OVERSIZE_BYTES
        {
            metrics::counter!(scan_metric_names::WIDE_SCAN_OVERSIZE_TOTAL).increment(1);
            warn!(
                event = "wide_scan_oversize",
                table.name = %self.table_name,
                selected_files = files,
                selected_mb = bytes / (1 << 20),
                threshold_mb = Self::WIDE_SCAN_OVERSIZE_BYTES / (1 << 20),
                "wide scan selected more than the oversize threshold; admitted anyway"
            );
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
            mem.timefusion_max_concurrent_scan_readers.max(1) as u32 * DECODE_UNITS_PER_READER,
        ))
    }

    /// Lead sort key that makes `DedupExec`'s keep-greatest engage.
    ///
    /// The table's first declared sorting column, but only when the table declares a `dedup_tiebreak`
    /// and that column is itself a dedup key of an i64-backed type. Equal dedup keys then share the
    /// bound value, so all versions of a row live in one contiguous run and the operator can emit
    /// without buffering the scan. One column, not the whole sort order, keeps the injected sort
    /// cheap. `None` leaves the plan as it was before merge-on-read.
    fn keep_greatest_ordering(table: &crate::schema::TableSchema, leg_schema: &SchemaRef) -> Option<datafusion::physical_expr::LexOrdering> {
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

    /// Columns whose value can differ between versions of one row (`None` for
    /// tables that append no versions).
    ///
    /// A filter on a column NOT in this set is safe below the merge-on-read
    /// `DedupExec` (all versions agree, so dedup-then-filter equals
    /// filter-then-dedup); a filter on a column IN it must stay above, or a
    /// stale version could match a predicate the winner no longer satisfies.
    ///
    /// **Immutable is the default; mutable is declared** — the old
    /// everything-mutable default blocked pushdown on ~120 columns to protect
    /// the one genuinely UPDATEd, and a trace_id point lookup materialising 24h
    /// of spans above the dedup OOM-killed prod (2026-08-20, exit 137). The
    /// tiebreak and tombstone are mutable by construction; `extract_dml_info`
    /// refuses UPDATEs assigning undeclared columns, so this is a checked contract.
    fn version_mutable_columns(table_name: &str) -> Option<HashSet<String>> {
        let schema = crate::schema::get_schema(table_name).filter(|s| s.version_append)?;
        Some(
            schema
                .fields
                .iter()
                .filter(|field| field.mutable)
                .map(|field| field.name.clone())
                .chain(schema.dedup_tiebreak.clone())
                .chain(schema.tombstone_column.clone())
                .collect(),
        )
    }

    /// Does `f` mention the table's tombstone marker? Such a predicate must never reach a scan leg.
    ///
    /// Applied at the source it would drop the tombstone row before the dedup, leaving the older
    /// live version to win keep-greatest and a deleted row silently resurrects. Reported
    /// `Unsupported` so DataFusion keeps its own `FilterExec` above the whole scan, and stripped again
    /// in `scan` so a filter arriving by any other route still cannot be pushed into a leg.
    pub(crate) fn references_tombstone(table_name: &str, f: &Expr) -> bool {
        crate::schema::get_schema(table_name).and_then(|s| s.tombstone_column.as_deref()).is_some_and(|t| f.column_refs().iter().any(|c| c.name == t))
    }

    /// `(column, name)` pairs for an identity projection over `idxs` — each index
    /// kept as-is, by position, with its own field name. Shared by `project_indices`
    /// and `filter_tombstones`, which both restore a plain column subset.
    fn identity_projection_exprs(schema: &SchemaRef, idxs: impl Iterator<Item = usize>) -> Vec<(Arc<dyn datafusion::physical_expr::PhysicalExpr>, String)> {
        idxs.map(|i| {
            (Arc::new(PhysicalColumn::new(schema.field(i).name(), i)) as Arc<dyn datafusion::physical_expr::PhysicalExpr>, schema.field(i).name().clone())
        })
        .collect()
    }

    /// Restore a requested column set from an augmented scan, by index into the input.
    /// Mirrors what `DedupExec`'s `output_projection` does for paths that skip it.
    fn project_indices(plan: Arc<dyn ExecutionPlan>, idxs: &[usize]) -> DFResult<Arc<dyn ExecutionPlan>> {
        let schema = plan.schema();
        if idxs.len() == schema.fields().len() && idxs.iter().enumerate().all(|(i, &j)| i == j) {
            return Ok(plan);
        }
        let exprs = Self::identity_projection_exprs(&schema, idxs.iter().copied());
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    fn filter_tombstones(plan: Arc<dyn ExecutionPlan>, marker: &str, keep: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        use datafusion::{physical_expr::expressions::binary, physical_plan::filter::FilterExec};
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
        let exprs = Self::identity_projection_exprs(&schema, 0..k);
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
            !crate::schema::is_variant_type(target_field.data_type())
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

    /// True iff every `(project, date)` partition in the query window carries a clean fingerprint
    /// that still matches the live file set.
    ///
    /// Only consulted on Delta-only paths; mem∪delta overlap still needs `DedupExec`. Never granted
    /// on `version_append` tables. There, `DedupExec` is not an optional optimization — it resolves
    /// versions. The sweep's "duplicate-free" verdict is about duplicate keys, which merge-on-read
    /// creates on purpose; skipping `DedupExec` would serve superseded versions and tombstoned
    /// rows.
    fn dedup_skip_allowed(&self, table: &DeltaTable, project_id: &str, window: Option<(i64, i64)>, dedup_keys: &[String]) -> DedupSkipVerdict {
        if dedup_keys.is_empty() || !self.database.config.maintenance.timefusion_read_dedup_skip_swept {
            return DedupSkipVerdict::Disabled;
        }
        // `version_append` (merge-on-read) is NOT a reason to refuse. It looks
        // like one — MOR appends a new version per UPDATE, so a skip would serve
        // the superseded row — but that is only true BEFORE the sweep. On a
        // partition the sweep certified duplicate-free with a still-matching
        // fingerprint there is exactly one row per key, and it is the winner:
        //
        //  * the sweep collapses versions keep-greatest, calling
        //    `mem_buffer::dedup_batches(.., schema.dedup_tiebreak, None)`;
        //  * `filter_tombstones` runs OUTSIDE `match dedup_on`, so a deleted row
        //    is removed whether or not DedupExec ran — a skip cannot resurrect it.
        //
        // Asserted by execution, not inspection, in
        // `dedup_compaction_test::a_swept_mor_partition_holds_one_winning_row_per_key`.
        let Some(window) = window else { return DedupSkipVerdict::NoWindow };
        self.database.dedup_window_clean(table, project_id, &self.table_name, window)
    }

    /// Extract time range (min, max) from query filters.
    /// Returns None if no time constraints found.
    fn extract_time_range_from_filters(&self, filters: &[Expr]) -> Option<(i64, i64)> {
        use crate::read::optimizers::{is_col_through_cast, swap_comparison};
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

        if min_ts.is_some() || max_ts.is_some() {
            return Some((min_ts.unwrap_or(i64::MIN), max_ts.unwrap_or(i64::MAX)));
        }
        // No timestamp bound — fall back to a `date = X` partition equality
        // (exactly one day). This is what makes a rollup build skippable: its
        // SQL never mentions `timestamp`, and a `None` window forced an
        // UNBOUNDED merge-on-read dedup that OOM'd every large backfill. Sound
        // because certification is itself keyed by (project, date).
        date_partition_window(filters)
    }
}

/// The micros span of a `date = <Date32>` equality, if the filters carry one.
///
/// Free-standing so the sourness of getting it wrong — a window that reaches
/// into a neighbouring date whose certification was never asked about — is
/// directly testable.
fn date_partition_window(filters: &[Expr]) -> Option<(i64, i64)> {
    use crate::read::optimizers::is_col_through_cast;
    const DAY_MICROS: i64 = 86_400_000_000;
    filters.iter().find_map(|filter| {
        let Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) = filter else { return None };
        let literal = match (is_col_through_cast(left, "date"), is_col_through_cast(right, "date")) {
            (true, _) => right.as_ref(),
            (_, true) => left.as_ref(),
            _ => return None,
        };
        let Expr::Literal(ScalarValue::Date32(Some(days)), _) = literal else { return None };
        let start = i64::from(*days).checked_mul(DAY_MICROS)?;
        // Inclusive end, one microsecond short of the next day.
        Some((start, start.checked_add(DAY_MICROS - 1)?))
    })
}

/// What [`Database::migrate_add_columns`] did.
pub struct ColumnMigrationReport {
    pub stored_before: usize,
    pub stored_after: usize,
    /// Columns actually added; empty when the stored schema already had them.
    pub added: Vec<String>,
}

impl Database {
    /// Add new nullable columns to a live table's stored Delta schema without editing YAML.
    ///
    /// The YAML schema and the Delta transaction log schema are separate. Widening only the YAML
    /// makes the write path build wider batches than storage declares. Storage must be widened
    /// first, then YAML may follow. Mechanism: commit a zero-row batch carrying only the new
    /// columns under `SchemaMode::Merge`. That unions them without writing data or rewriting
    /// rows. Columns already present are skipped, so a half-finished run can simply be re-run.
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

/// Decode-admission pressure valve: how many of the wide-scan semaphore's `total` permits one decode
/// poll must claim. 1 normally; a quarter of the pool at tier 1; the whole pool at tier 2.
///
/// Decode heap is untracked by any DataFusion pool, so the only OOM lever is concurrency.
/// Fixed-percentage thresholds failed in both directions, so tiers fire on projected seconds to
/// the limit; a box parked at 70% has near-zero consumption rate and never throttles. Absolute
/// backstops remain so the valve is never later than the old behaviour. Pressure is memcg usage
/// minus reclaimable cache, sampled at most every 250ms.
fn scan_pressure_permits(total: u32) -> u32 {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    static SAMPLED_AT_MS: AtomicU64 = AtomicU64::new(u64::MAX);
    static USAGE_PCT: AtomicU64 = AtomicU64::new(0);
    static PREV_USED: AtomicU64 = AtomicU64::new(0);
    /// Smoothed growth rate in bytes/sec. Saturating at 0 on a fall: a
    /// *shrinking* process is never heading for the wall, and letting negative
    /// rates into the average would mask a burst that follows a big free.
    static RATE_BPS: AtomicU64 = AtomicU64::new(0);
    /// Projected seconds until usage reaches the limit at the current rate.
    static ETA_SECS: AtomicU64 = AtomicU64::new(u64::MAX);
    let now_ms = EPOCH.get_or_init(std::time::Instant::now).elapsed().as_millis() as u64;
    let last = SAMPLED_AT_MS.load(Relaxed);
    if last == u64::MAX || now_ms.saturating_sub(last) >= 250 {
        SAMPLED_AT_MS.store(now_ms, Relaxed);
        let limit = crate::config::try_config().map_or(0, |c| c.derived.memory_limit_bytes);
        let used = process_memory_bytes().unwrap_or(0);
        let pct = if limit > 0 { (used * 100 / limit) as u64 } else { 0 };
        let prev_used = PREV_USED.swap(used as u64, Relaxed);
        let dt_ms = now_ms.saturating_sub(last);
        // First sample (no `last`) has no interval to differentiate over.
        if last != u64::MAX && dt_ms > 0 && prev_used > 0 {
            let gained = (used as u64).saturating_sub(prev_used);
            let sample_bps = gained.saturating_mul(1000) / dt_ms;
            // EWMA over ~4 samples (~1s). Enough to reject a single noisy read
            // without blunting a sustained burst — at 450 MB/s the estimate is
            // within ~15% of truth inside 2 seconds.
            let prev_rate = RATE_BPS.load(Relaxed);
            RATE_BPS.store((prev_rate * 3 + sample_bps) / 4, Relaxed);
        }
        let rate = RATE_BPS.load(Relaxed);
        let headroom = (limit as u64).saturating_sub(used as u64);
        ETA_SECS.store(if rate > 0 { headroom / rate } else { u64::MAX }, Relaxed);
        let prev_pct = USAGE_PCT.swap(pct, Relaxed);
        // Tier transitions are rare and load-bearing for OOM post-mortems:
        // counters die with the process, the log survives it.
        let was = pressure_permit_claim_at(prev_pct, u64::MAX, total);
        let now = pressure_permit_claim_at(pct, ETA_SECS.load(Relaxed), total);
        if was != now {
            warn!(
                "scan pressure valve: {prev_pct}% -> {pct}% of cgroup limit, growth {} MB/s, projected {} to limit, decode permit claim {was} -> {now} (of {total})",
                rate / (1024 * 1024),
                match ETA_SECS.load(Relaxed) {
                    u64::MAX => "never".to_string(),
                    s => format!("{s}s"),
                }
            );
        }
    }
    pressure_permit_claim_at(USAGE_PCT.load(Relaxed), ETA_SECS.load(Relaxed), total)
}

/// First tier: the valve needs this long to drain in-flight decodes and have
/// the throttle mean anything. Sized against the measured ~450 MB/s burst,
/// which crosses 40 GB of headroom in ~90s.
const VALVE_ETA_TIER1_SECS: u64 = 90;
/// Second tier: fully serialize. At this projection nothing else will do.
const VALVE_ETA_TIER2_SECS: u64 = 30;
/// The projected tiers apply only from this share of the limit. Below it there
/// is so much absolute headroom that a short projection means "filling fast
/// from cold", not "about to hit the wall". At half of a 96 GiB limit there are
/// still ~48 GiB free, which the measured ~450 MB/s burst takes ~107s to cross
/// — so the 90s tier still engages with real lead time.
const VALVE_RATE_FLOOR_PCT: u64 = 50;

/// Sub-divisions of one reader slot in the wide-scan gate.
///
/// The gate bounds Parquet decode heap, which is untracked by the DataFusion memory pool.
/// Counting raw polls sized each permit for the worst-case batch while real dashboard batches
/// are orders of magnitude smaller, so most of the decode budget sat unused while polls queued.
/// Splitting each reader slot into `K` units and charging a poll for the heap it actually
/// produced keeps the same ceiling — a full-size batch still claims all `K` units, while a tiny
/// batch claims one. This is why `timefusion_max_concurrent_scan_readers` is not raised: doing
/// so would raise the heap ceiling, which is the one thing the guard exists to hold down.
const DECODE_UNITS_PER_READER: u32 = 16;

/// Worst-case decoded batch size one reader slot is sized for.
///
/// 8192-row decode batches of wide OTel rows measured up to 145 MB; this is the number the
/// poll-counting gate was implicitly built around.
const NOMINAL_DECODE_BATCH_BYTES: u64 = 145 * 1024 * 1024;

/// Units a poll must claim to cover `last_batch_bytes` of decoded Arrow.
///
/// `0` means "not yet known" (a stream's first poll) and claims a whole reader
/// slot, i.e. exactly the pre-split behaviour; the claim adapts down once the
/// stream has shown what its batches actually weigh. Never exceeds one slot, so
/// the heap ceiling is unchanged, and never zero, so progress is guaranteed.
fn decode_units(last_batch_bytes: u64) -> u32 {
    let k = u64::from(DECODE_UNITS_PER_READER);
    match last_batch_bytes {
        0 => DECODE_UNITS_PER_READER,
        b => (b.saturating_mul(k).div_ceil(NOMINAL_DECODE_BATCH_BYTES)).clamp(1, k) as u32,
    }
}

/// Tier math for `scan_pressure_permits`, separated for testability.
///
/// Permits one decode poll must claim, given both current usage and its rate. Whichever tier is
/// reached first wins: absolute percentages backstop fast bursts, projections catch bursts early.
/// The projection is gated on [`VALVE_RATE_FLOOR_PCT`] of the limit so a process filling from cold
/// — which grows fast but plateaus far from the limit — is not throttled at a small fraction of
/// the limit.
fn pressure_permit_claim_at(usage_pct: u64, eta_secs: u64, total: u32) -> u32 {
    let projected = usage_pct >= VALVE_RATE_FLOOR_PCT;
    if usage_pct >= 95 || (projected && eta_secs <= VALVE_ETA_TIER2_SECS) {
        total
    } else if usage_pct >= 88 || (projected && eta_secs <= VALVE_ETA_TIER1_SECS) {
        (total / 4).max(1)
    } else {
        1
    }
}

/// Concurrency-gate a wide read scan.
///
/// Each output partition acquires a permit around every batch decode, bounding the number of
/// Parquet row groups decoded at once across all wide queries. Parquet decode heap is untracked by
/// the DataFusion memory pool, so unbounded parallelism can OOM the process. Acquisition is
/// per-batch, not per-stream: holding a permit for a partition's whole lifetime would deadlock
/// `SortPreservingMergeExec`, which needs one batch from every input before it can emit.
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
        // `last_bytes` is this stream's most recent decoded batch size: the
        // claim adapts to what this scan actually produces instead of every
        // poll reserving the 145 MB worst case.
        let gated = futures::stream::unfold((inner, 0u64), move |(mut inner, last_bytes)| {
            let sem = sem.clone();
            let metrics = metrics.clone();
            async move {
                // Near the OOM line each poll claims more of the pool,
                // shrinking effective decode concurrency (see
                // `scan_pressure_permits`). `acquire_many` never exceeds the
                // pool size, so progress is guaranteed.
                // Pressure valve wins when it is engaged (it claims a quarter
                // or all of the pool); otherwise the heap-proportional claim does.
                let want = scan_pressure_permits(pool_size).max(decode_units(last_bytes)).min(pool_size);
                let _permit = sem.acquire_many_owned(want).await.ok()?;
                if let Some(m) = &metrics {
                    m.decode_begin();
                    if want > DECODE_UNITS_PER_READER {
                        metrics::counter!(scan_metric_names::DECODE_PRESSURE_THROTTLED).increment(1);
                    }
                }
                // The object-store fetches for this batch happen inside the
                // poll, so the bypass scope covers exactly them. Only paid for
                // when it's actually suppressing.
                let next = match bypass {
                    true => crate::storage::scan_bypass_scope(true, futures::StreamExt::next(&mut inner)).await,
                    false => futures::StreamExt::next(&mut inner).await,
                };
                let produced = next.as_ref().and_then(|r| r.as_ref().ok()).map_or(0, |b: &RecordBatch| b.get_array_memory_size() as u64);
                if let Some(m) = &metrics {
                    // Size the decoded Arrow, not the compressed parquet.
                    m.decode_end(produced);
                }
                next.map(|item| (item, (inner, produced)))
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

/// Outcome of [`decide_prefilter`]: either why the tantivy prefilter was
/// skipped, or the narrowing it proved sound to apply.
enum PrefilterDecision {
    Skipped(&'static str),
    Used { ids: HashSet<String>, covered_files: HashSet<String>, exclude_files: Option<HashSet<String>>, row_selections: Option<HashMap<String, Vec<u64>>> },
}

/// Pure decision over an already-completed tantivy search's stats — no IO, mirrors the branches
/// in `ProjectRoutingTable::scan`'s prefilter block exactly (skip reasons in the same order).
///
/// `is_mutable`: whether the ROUTED PREDICATE touches a version-mutable column —
/// not merely whether the table has one. Narrowed 2026-08-22: the table-wide
/// form was true for `otel_logs_and_spans`, which silently disabled both
/// optimisations below on the busiest table in production. On such a
/// table a "hitless" file may hold the NEWEST version of a key whose match lives only in an
/// older version, and dropping it below `DedupExec` would serve the stale row — so file
/// exclusion/row-selection narrowing NEVER applies there; only the id-set does (sound at
/// whole-key granularity).
/// Does the routed predicate reference a version-mutable column?
///
/// The prefilter's file pruning is only unsound when it does. `None` mutable set
/// means the table is not merge-on-read at all; `None` tree means nothing was
/// routed, so there is no predicate to be unsound about.
fn routed_touches_mutable(mutable: Option<&HashSet<String>>, tree: Option<&crate::tantivy::udf::PredNode>) -> bool {
    match (mutable, tree) {
        (Some(m), Some(t)) => t.columns().iter().any(|c| m.contains(*c)),
        _ => false,
    }
}

/// May this slice's coverage still be read?
///
/// The row witness decides. `fp_fallback` adds a second question BENEATH it,
/// never instead of it: a slice the witness could not JUDGE (absent witness — an
/// absence, not a disagreement) may instead prove itself with the same
/// fingerprint test the date-level path already applies. A slice whose witness
/// DISAGREES can never be rescued this way, which is the property that keeps the
/// fallback from weakening the rule.
fn slice_is_current(witness: Option<u64>, current_rows: Option<u64>, slice_fp: u64, current_fp: Option<u64>, fp_fallback: bool) -> bool {
    crate::rollup::slice_coverage_agrees(&[witness], current_rows) || (fp_fallback && witness.is_none() && current_fp.is_some_and(|fp| fp == slice_fp))
}

#[cfg(test)]
mod slice_is_current_tests {
    use super::*;

    /// The fallback must widen ONLY the unverifiable case. Prod 2026-08-22 put
    /// 95.2% of `stale_coverage` there (1,567 witness-less slices) against 4.7%
    /// genuinely moved, so this is the whole value of the flag — and rescuing a
    /// moved slice instead would serve rows the rollup never aggregated.
    #[test]
    fn the_fallback_rescues_only_the_unjudgeable_never_the_disagreeing() {
        let (fp, other_fp) = (7u64, 8u64);
        // (witness, current_rows, slice_fp, current_fp, off, on)
        for (witness, rows, slice_fp, cur_fp, off, on) in [
            // Witness agrees: current either way, fallback irrelevant.
            (Some(10), Some(10), fp, Some(fp), true, true),
            // Witness DISAGREES: stale either way — the fallback must not rescue
            // it even though the fingerprint matches.
            (Some(9), Some(10), fp, Some(fp), false, false),
            // No witness, fingerprint matches: the case the flag exists for.
            (None, Some(10), fp, Some(fp), false, true),
            // No witness, fingerprint moved: still stale.
            (None, Some(10), fp, Some(other_fp), false, false),
            // No witness, no live fingerprint: nothing to compare, still stale.
            (None, Some(10), fp, None, false, false),
        ] {
            assert_eq!(slice_is_current(witness, rows, slice_fp, cur_fp, false), off, "flag OFF: witness={witness:?} fp={slice_fp} current_fp={cur_fp:?}");
            assert_eq!(slice_is_current(witness, rows, slice_fp, cur_fp, true), on, "flag ON: witness={witness:?} fp={slice_fp} current_fp={cur_fp:?}");
        }
    }

    /// Default-off means the read path is bit-identical to `slice_coverage_agrees`.
    #[test]
    fn with_the_flag_off_nothing_changes() {
        for witness in [None, Some(0), Some(10), Some(11)] {
            for rows in [None, Some(10)] {
                assert_eq!(
                    slice_is_current(witness, rows, 7, Some(7), false),
                    crate::rollup::slice_coverage_agrees(&[witness], rows),
                    "flag OFF must not alter the witness verdict"
                );
            }
        }
    }
}

/// Why one slice's coverage was refused. Mirrors `slice_coverage_agrees`'s
/// FALSE branches, which is the only thing that reaches here.
///
/// The split exists because the three demand opposite work: an absent witness
/// clears itself on republish (build throughput), a moved partition never will,
/// and an absent live row count means nothing could be compared at all.
fn stale_coverage_metric(witness: Option<u64>, current: Option<u64>) -> &'static str {
    match (witness, current) {
        (None, _) => scan_metric_names::ROLLUP_STALE_NO_WITNESS,
        (_, None) => scan_metric_names::ROLLUP_STALE_NO_SOURCE_ROWS,
        _ => scan_metric_names::ROLLUP_STALE_MOVED,
    }
}

#[cfg(test)]
mod stale_coverage_metric_tests {
    use super::*;

    /// Every case that reaches the classifier is one `slice_coverage_agrees`
    /// rejected, so the two must not drift: anything this names must be a slice
    /// the read path actually refused.
    #[test]
    fn each_refusal_gets_its_own_name_and_only_refusals_reach_it() {
        for (witness, current, expected) in [
            (None, Some(10), scan_metric_names::ROLLUP_STALE_NO_WITNESS),
            (None, None, scan_metric_names::ROLLUP_STALE_NO_WITNESS),
            (Some(10), None, scan_metric_names::ROLLUP_STALE_NO_SOURCE_ROWS),
            (Some(9), Some(10), scan_metric_names::ROLLUP_STALE_MOVED),
            (Some(11), Some(10), scan_metric_names::ROLLUP_STALE_MOVED),
        ] {
            assert!(!crate::rollup::slice_coverage_agrees(&[witness], current), "{witness:?}/{current:?} must be a refusal to be classified");
            assert_eq!(stale_coverage_metric(witness, current), expected, "for witness={witness:?} current={current:?}");
        }
        // The one agreeing case never reaches the classifier.
        assert!(crate::rollup::slice_coverage_agrees(&[Some(10)], Some(10)));
    }
}

/// Charge one prefilter skip to the total AND to its reason.
///
/// The total alone is not attributable: it is incremented from two call sites —
/// `decide_prefilter`'s three exits and the search-abort branch — so a high skip
/// rate could be a decision or a missing index, and the fix differs.
fn record_prefilter_skip(reason: &'static str) {
    crate::observability::record_tantivy_prefilter_skipped();
    metrics::counter!(scan_metric_names::PREFILTER_SKIPPED).increment(1);
    let named = scan_metric_names::prefilter_skip_metric(reason);
    debug_assert!(named.is_some(), "unregistered prefilter skip reason {reason:?} — it would vanish from the breakdown");
    if let Some(name) = named {
        metrics::counter!(name).increment(1);
    }
}

#[allow(clippy::too_many_arguments)]
fn decide_prefilter(
    ids: HashSet<String>, indexed_rows: u64, min_selectivity_pct: u64, field_gap: bool, covered_files: HashSet<String>, zero_hit_files: HashSet<String>,
    row_selections: HashMap<String, Vec<u64>>, is_mutable: bool, file_pruning_enabled: bool, row_selection_enabled: bool,
) -> PrefilterDecision {
    // No indexed rows = no useful prefilter. Without this guard we'd emit an
    // empty IN(...) list that zeros the Delta scan even when matching rows
    // exist there (e.g. data written directly without triggering an index build).
    if indexed_rows == 0 {
        return PrefilterDecision::Skipped("empty_index");
    }
    // Selectivity cutoff: if the hit set covers most of the indexed rows, the
    // IN-list won't prune enough to be worth its planning cost. Bail; the
    // original predicate re-runs as the correctness backstop.
    if (ids.len() as u64) * 100 >= indexed_rows * min_selectivity_pct {
        return PrefilterDecision::Skipped("low_selectivity");
    }
    // An in-window index lacked one of the queried fields (schema evolution
    // added a tantivy column after it was built). It can't answer that
    // predicate yet appears "covered", so the IN-list would drop its rows.
    if field_gap {
        return PrefilterDecision::Skipped("field_coverage_gap");
    }
    // A zero-hit covering index proves its own files hold no matches. Complete
    // coverage excludes those files from the single Delta leg; partial
    // coverage excludes them only from the indexed leg while uncovered files
    // scan raw.
    let (exclude_files, row_selections) = if is_mutable {
        (None, None)
    } else {
        (
            (file_pruning_enabled && !zero_hit_files.is_empty()).then_some(zero_hit_files),
            (row_selection_enabled && !row_selections.is_empty()).then_some(row_selections),
        )
    };
    PrefilterDecision::Used { ids, covered_files, exclude_files, row_selections }
}

#[cfg(test)]
mod decide_prefilter_tests {
    use super::*;

    fn decide(ids: &[&str], indexed_rows: u64, is_mutable: bool) -> PrefilterDecision {
        decide_prefilter(
            ids.iter().map(|s| s.to_string()).collect(),
            indexed_rows,
            50, // min_selectivity_pct
            false,
            HashSet::from(["covered.parquet".to_string()]),
            HashSet::from(["zero_hit.parquet".to_string()]),
            HashMap::from([("row_sel.parquet".to_string(), vec![1, 2])]),
            is_mutable,
            true,
            true,
        )
    }

    /// The narrowing itself: which predicates count as touching a mutable
    /// column. Getting this wrong in the permissive direction is a correctness
    /// bug on a merge-on-read table, so pin every arm.
    #[test]
    fn routed_touches_mutable_only_when_a_predicate_column_is_mutable() {
        use crate::tantivy::udf::{PredNode, TextMatchPred};
        let leaf = |c: &str| PredNode::Leaf(TextMatchPred { column: c.into(), query: "x".into() });
        let mutable: HashSet<String> = ["status".to_string(), "_version".to_string()].into_iter().collect();

        assert!(!routed_touches_mutable(Some(&mutable), Some(&leaf("trace_id"))), "immutable column");
        assert!(routed_touches_mutable(Some(&mutable), Some(&leaf("status"))), "mutable column");
        // A conjunction is only as safe as its least safe branch.
        assert!(routed_touches_mutable(Some(&mutable), Some(&PredNode::And(vec![leaf("trace_id"), leaf("status")]))), "AND with a mutable branch");
        assert!(routed_touches_mutable(Some(&mutable), Some(&PredNode::Or(vec![leaf("trace_id"), leaf("_version")]))), "OR with a mutable branch");
        // Not merge-on-read, or nothing routed: nothing to be unsound about.
        assert!(!routed_touches_mutable(None, Some(&leaf("status"))), "non-MOR table");
        assert!(!routed_touches_mutable(Some(&mutable), None), "nothing routed");
    }

    /// Against the REAL schema, not a fixture: the narrowing is only sound while
    /// the columns queries actually route on are declared immutable. This is the
    /// guard that fires if someone later marks an indexed column `mutable: true`
    /// — the unit tests above would still pass, and the pruning would silently
    /// become unsound on a merge-on-read table.
    #[test]
    fn otel_routes_on_immutable_columns_so_pruning_stays_sound() {
        use crate::tantivy::udf::{PredNode, TextMatchPred};
        let mutable = ProjectRoutingTable::version_mutable_columns("otel_logs_and_spans").expect("otel is version_append, so it has a mutable set");
        let leaf = |c: &str| PredNode::Leaf(TextMatchPred { column: c.into(), query: "x".into() });
        for column in ["context___trace_id", "id", "name"] {
            assert!(
                !routed_touches_mutable(Some(&mutable), Some(&leaf(column))),
                "`{column}` is routed by real queries and must stay immutable, or zero-hit file pruning is unsound"
            );
        }
        // ...and the set is not vacuously empty, which would make the assertions above meaningless.
        assert!(!mutable.is_empty(), "otel declares mutable columns; an empty set would make this test prove nothing");
    }

    /// The mutable gate is what keeps merge-on-read correct, and narrowing it
    /// from table-wide to predicate-aware is the whole point of the change — so
    /// pin BOTH directions. A predicate on a mutable column must still refuse
    /// to prune (a newer version could match where the indexed one did not);
    /// an immutable-only predicate must prune, because every version of a
    /// matching row carries the same values and a zero-hit file therefore holds
    /// no version of one.
    #[test]
    fn only_a_mutable_predicate_blocks_file_pruning() {
        let PrefilterDecision::Used { exclude_files, row_selections, .. } = decide(&["a"], 100, false) else {
            panic!("immutable predicate must use the prefilter");
        };
        assert!(exclude_files.is_some(), "an immutable-only predicate must prune zero-hit files");
        assert!(row_selections.is_some(), "...and push row selections");

        let PrefilterDecision::Used { exclude_files, row_selections, .. } = decide(&["a"], 100, true) else {
            panic!("mutable predicate still uses the prefilter, just without pruning");
        };
        assert!(exclude_files.is_none(), "a mutable predicate must NOT prune: a newer version may match");
        assert!(row_selections.is_none(), "...nor push row selections");
    }

    /// Each skip reason fires independently and in the documented priority order —
    /// pins the exact branch order `scan()`'s prefilter block depends on.
    #[test]
    fn skip_reasons_fire_in_priority_order() {
        assert!(matches!(decide(&["a"], 0, false), PrefilterDecision::Skipped("empty_index")));
        assert!(matches!(decide(&["a", "b"], 2, false), PrefilterDecision::Skipped("low_selectivity")), "2 of 2 indexed rows hit == 100% >= 50%");
        let field_gap = decide_prefilter(
            HashSet::from(["a".to_string()]),
            10,
            50,
            true, // field_gap
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
            false,
            true,
            true,
        );
        assert!(matches!(field_gap, PrefilterDecision::Skipped("field_coverage_gap")));
    }

    /// A non-mutable (non-merge-on-read) table gets file exclusion + row selection.
    #[test]
    fn non_mutable_table_gets_file_exclusion_and_row_selection() {
        let PrefilterDecision::Used { exclude_files, row_selections, .. } = decide(&["a"], 10, false) else { panic!("expected Used") };
        assert_eq!(exclude_files, Some(HashSet::from(["zero_hit.parquet".to_string()])));
        assert_eq!(row_selections, Some(HashMap::from([("row_sel.parquet".to_string(), vec![1, 2])])));
    }

    /// A merge-on-read (`version_append`) table's Used decision NEVER carries file
    /// exclusion or row selection — a "hitless" file may hold the newest version of a
    /// key whose match lives only in an older version, and either narrowing would
    /// serve the stale row instead of the current one. Only the id-set narrows, and
    /// only at whole-key granularity.
    #[test]
    fn mutable_table_never_gets_file_exclusion_or_row_selection() {
        let PrefilterDecision::Used { exclude_files, row_selections, ids, .. } = decide(&["a"], 10, true) else { panic!("expected Used") };
        assert!(exclude_files.is_none());
        assert!(row_selections.is_none());
        assert_eq!(ids, HashSet::from(["a".to_string()]));
    }

    /// Disabled config knobs suppress narrowing even when the data would otherwise justify it.
    #[test]
    fn narrowing_respects_its_own_config_toggle() {
        let decision = decide_prefilter(
            HashSet::from(["a".to_string()]),
            10,
            50,
            false,
            HashSet::new(),
            HashSet::from(["zero_hit.parquet".to_string()]),
            HashMap::from([("row_sel.parquet".to_string(), vec![1])]),
            false,
            false, // file_pruning_enabled = false
            false, // row_selection_enabled = false
        );
        let PrefilterDecision::Used { exclude_files, row_selections, .. } = decision else { panic!("expected Used") };
        assert!(exclude_files.is_none(), "file pruning disabled must suppress exclude_files even though zero_hit_files is non-empty");
        assert!(row_selections.is_none(), "row selection disabled must suppress row_selections even though the map is non-empty");
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
        let variant_cols: HashSet<String> = crate::schema::registry()
            .get(&self.table_name)
            .map(|s| s.schema_ref().fields().iter().filter(|f| crate::schema::is_variant_type(f.data_type())).map(|f| f.name().clone()).collect())
            .unwrap_or_default();
        let mutable = Self::version_mutable_columns(&self.table_name);
        Ok(filter
            .iter()
            .map(|f| {
                if Self::references_tombstone(&self.table_name, f)
                    || (!variant_cols.is_empty() && f.column_refs().iter().any(|c| variant_cols.contains(&c.name)))
                {
                    TableProviderFilterPushDown::Unsupported
                } else if mutable.as_ref().is_some_and(|m| f.column_refs().iter().any(|c| m.contains(&c.name))) {
                    // `Inexact`, not `Unsupported`: DataFusion keeps its own
                    // FilterExec above the scan either way, but `Unsupported`
                    // also withholds the predicate from `scan()`, and `scan()`
                    // is the only place that knows whether this window is
                    // sweep-certified. `scan()` re-strips it for every path that
                    // is not (see `leg_safe`), so this widens what `scan()` can
                    // SEE without widening what any leg is given. NEVER `Exact`:
                    // the above-dedup FilterExec is what makes a stale-version
                    // match impossible, and `Exact` would delete it.
                    TableProviderFilterPushDown::Inexact
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

        // Internal Delta-only reads (rollup builds, maintenance) are not the
        // unbounded client scans this guard exists to reject.
        if !self.database.bypass_rollup
            && let Some(reason) = self.bounded_otel_scan_reason(filters, limit)
        {
            match self.database.config.core.timefusion_otel_scan_guard {
                config::OtelScanGuard::Off => {}
                config::OtelScanGuard::Observe => {
                    metrics::counter!(scan_metric_names::BOUNDED_OTEL_SCAN_CANDIDATES).increment(1);
                    warn!(event = "otel_scan_guard_candidate", table.name = %self.table_name, reason, "raw OTel scan would be rejected");
                }
                config::OtelScanGuard::Enforce => {
                    metrics::counter!(scan_metric_names::BOUNDED_OTEL_SCAN_REJECTIONS).increment(1);
                    return Err(DataFusionError::Plan("raw OTel queries require project_id = <value> and a timestamp lower bound or LIMIT".to_string()));
                }
            }
        }

        // Second line of defence behind `supports_filters_pushdown`: predicates
        // on the tombstone marker or any version-mutable column must not reach
        // a scan leg however they arrived (silent resurrection / stale-version
        // serving). For the tantivy prefilter the invariant is: leaf pruning
        // below DedupExec commutes with keep-greatest only when the predicate
        // evaluates identically on every version of a key — so file exclusion
        // and row selections stay OFF on mutable columns, while the id-set half
        // stays sound (`id` is a dedup key: `id IN (hits)` admits whole keys
        // atomically and the above-dedup filter rejects stale-only matches).
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

        // Tantivy prefilter, two independent paths: the Delta side builds
        // `id IN (delta_ids)` for the Delta scan only (MemBuffer rows are never
        // in the index, so applying it there would drop valid rows); the
        // MemBuffer side prefilters atomically under its own bucket lock. On a
        // MOR table collect the tree from the UNSTRIPPED filters — the id-set
        // (the only output allowed below on such tables) is sound for them.
        let text_match_tree = match mutable.is_some() {
            false => crate::tantivy::udf::collect_text_match_tree(&optimized_filters),
            true => crate::tantivy::udf::collect_text_match_tree(&self.apply_time_series_optimizations(unstripped_filters)?),
        };
        // Query [lo,hi] timestamp window, shared by the tantivy prefilter (time-
        // prunes the sidecar search + scopes the coverage gate to a needle's
        // window, not every index the project built) and the skip-delta
        // watermark check below.
        let query_time_range = self.extract_time_range_from_filters(&optimized_filters);
        let mut tantivy_id_filter: Option<Expr> = None;
        // When index coverage is partial, the indexed and raw files are read
        // as separate Delta legs. Only the indexed leg receives the narrowing
        // id-set; uncovered files retain the original predicate and therefore
        // cannot lose rows. This turns coverage lag into bounded raw debt
        // instead of making one missing sidecar poison the whole query window.
        let mut tantivy_covered_files: Option<HashSet<String>> = None;
        // Files the prefilter proved hold no matches (zero-hit covering
        // index) — excluded from the Delta scan when file pruning is on.
        let mut tantivy_exclude: Option<HashSet<String>> = None;
        // Per-file matching row ordinals (row-selection pushdown), for files
        // whose covering index was built in parquet row order.
        let mut tantivy_row_selections: Option<HashMap<String, Vec<u64>>> = None;
        // File-level needle pruning: table-relative paths the resident bloom
        // registry proves cannot contain the query's equality/IN needles.
        // Memory-only consult — a cold registry spawns background loads and
        // prunes nothing this query. Applied where the delta legs partition
        // live files (unlike zero_hit_files it must reach the RAW leg too).
        let bloom_rejected: Option<HashSet<String>> = (|| {
            let reg = self.database.bloom_prune()?;
            let dates = crate::read::bloom_prune::dates_in_range(query_time_range?)?;
            let schema = crate::schema::get_schema(&self.table_name)?;
            let needles = crate::read::bloom_prune::extract_needles(&optimized_filters, schema, Self::version_mutable_columns(&self.table_name).as_ref());
            if needles.is_empty() {
                return None;
            }
            let rejected = reg.rejected_rels(&self.table_name, &project_id, &dates, &needles);
            (!rejected.is_empty()).then_some(rejected)
        })();
        if let Some(tree) = text_match_tree.as_ref()
            && let Some(svc) = self.database.tantivy_search()
        {
            use datafusion::logical_expr::{Expr, lit};
            let tcfg = &self.database.config().tantivy;
            let max_hits = tcfg.prefilter_max_hits();
            let min_sel_pct = tcfg.prefilter_min_selectivity_pct() as u64;
            crate::observability::record_tantivy_prefilter_attempt();
            metrics::counter!(scan_metric_names::PREFILTER_ATTEMPTS).increment(1);

            let mut delta_ids: Option<HashSet<String>> = None;
            let mut delta_indexed_rows: u64 = 0;
            let mut delta_covered: HashSet<String> = HashSet::new();
            let mut delta_zero_hit: HashSet<String> = HashSet::new();
            let mut delta_row_sel: HashMap<String, Vec<u64>> = HashMap::new();
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
                    crate::observability::record_tantivy_prefilter_error();
                    abort_reason = Some("delta_error");
                }
            }

            if delta_any_usable {
                if let Some(ids) = delta_ids {
                    let routed_touches_mutable = routed_touches_mutable(mutable.as_ref(), text_match_tree.as_ref());
                    let decision = decide_prefilter(
                        ids,
                        delta_indexed_rows,
                        min_sel_pct,
                        delta_field_gap,
                        delta_covered,
                        delta_zero_hit,
                        delta_row_sel,
                        // Predicate-aware, not table-wide. `mutable.is_some()`
                        // asked "does this TABLE have a mutable column", which is
                        // true for otel_logs_and_spans (version_append + a mutable
                        // field) — so on the busiest table both zero-hit file
                        // pruning and row-selection pushdown were dead, and the
                        // indexed leg carried ~2,880 unpruned files into a
                        // scan() that costs ~14.5us per selected file.
                        //
                        // Sound to narrow: if every ROUTED predicate column is
                        // immutable, every version of a matching row carries the
                        // same values, so a file whose index found no match cannot
                        // hold any version of one — tombstone versions included,
                        // since they carry the same keys. A predicate touching a
                        // mutable column (or the tiebreak/tombstone, both of which
                        // are in that set) still takes the old conservative path.
                        routed_touches_mutable,
                        tcfg.timefusion_tantivy_file_pruning,
                        tcfg.timefusion_tantivy_row_selection,
                    );
                    match decision {
                        PrefilterDecision::Skipped(reason) => {
                            record_prefilter_skip(reason);
                            debug!("Tantivy prefilter skipped for {}/{}: {}", project_id, self.table_name, reason);
                        }
                        PrefilterDecision::Used { ids, covered_files, exclude_files, row_selections } => {
                            crate::observability::record_tantivy_prefilter_used();
                            metrics::counter!(scan_metric_names::PREFILTER_USED).increment(1);
                            tantivy_id_filter = Some(Expr::InList(datafusion::logical_expr::expr::InList {
                                expr: Box::new(datafusion::logical_expr::col("id")),
                                list: ids.into_iter().map(lit).collect(),
                                negated: false,
                            }));
                            // Partition the exact snapshot at scan construction,
                            // not a separately resolved snapshot here. A flush or
                            // compaction can commit between index search and table
                            // resolution; carrying the coverage set forward makes
                            // the later split race-free.
                            tantivy_covered_files = Some(covered_files);
                            tantivy_exclude = exclude_files;
                            tantivy_row_selections = row_selections;
                        }
                    }
                }
            } else {
                let reason = abort_reason.unwrap_or("delta_no_hits_returned");
                record_prefilter_skip(reason);
                debug!("Tantivy prefilter skipped for {}/{}: {}", project_id, self.table_name, reason);
            }
        }

        // Read-side dedup setup: collapse physical duplicates of dedup-key rows
        // over the routed/pruned union at query time, so COUNT(*) is correct
        // regardless of sweep timing. The pushed projection is augmented with
        // any dedup-key columns the query projected away; `output_projection`
        // restores the requested set. No-op without declared dedup_keys.
        let table_schema = crate::schema::get_schema(&self.table_name);
        let dedup_keys: Vec<String> = table_schema.as_ref().map(|s| s.dedup_keys.clone()).unwrap_or_default();
        // The tiebreak rides in with the keys ONLY for merge-on-read tables
        // (DedupExec keeps the greatest version per key and must see the
        // column). Gated on `version_append`: elsewhere keep-greatest can't
        // engage, so the column would be read and never used — measured at
        // 2x-6x slower counts when pulled in unconditionally.
        let dedup_tiebreak: Option<String> = table_schema.as_ref().filter(|s| s.version_append).and_then(|s| s.dedup_tiebreak.clone());
        // Merge-on-read DELETE: a tombstone version must reach the filter ABOVE
        // the dedup, so its marker column rides in with the keys and is stripped
        // again afterwards. `None` on every table that declares none.
        let tombstone: Option<String> = table_schema.and_then(|s| s.tombstone_column.clone());
        // `tombstone_keep` is the requested width when the marker rode in
        // purely for the filter (one trailing column the post-filter projection
        // removes). The dedup skip is decided BEFORE the projection is built:
        // deciding after, gated on `output_projection.is_none()`, meant
        // augmenting with the keys was exactly what disabled the skip — and
        // `id` is ~43% of an otel file's bytes, read purely to feed a DedupExec
        // this path then removes. A certified window now skips BOTH the dedup
        // and the widening; a fast-resolve miss simply declines (the skip is an
        // optimisation, never a correctness input).
        let skip_verdict = match dedup_keys.is_empty() {
            true => DedupSkipVerdict::Disabled,
            false => self
                .database
                .try_fast_resolve(&project_id, &self.table_name)
                .and_then(|t| t.try_read().ok().map(|table| self.dedup_skip_allowed(&table, &project_id, query_time_range, &dedup_keys)))
                // A resolve miss is its own denial reason, not an uncertified
                // partition: it says the provider cache was cold, which the
                // certification-survival question must not be charged for.
                .unwrap_or(DedupSkipVerdict::Unresolved),
        };
        let pre_skip_dedup = skip_verdict.granted();
        let (scan_projection, output_projection, tombstone_keep): (Option<Vec<usize>>, Option<Vec<usize>>, Option<usize>) = match projection {
            Some(p) if !dedup_keys.is_empty() || tombstone.is_some() => {
                let full_schema = self.schema();
                // The dedup keys ALWAYS ride in, even when `pre_skip_dedup` says
                // the window is certified: the skip is granted PER LEG below and
                // the mem ∪ delta union path never grants it, so dropping the
                // keys here built DedupExecs over scans that couldn't feed them
                // ("DedupExec key `id` not in input schema", 08-09..08-11).
                // Keeping them costs bytes; removing the DedupExec is the larger win.
                let augment: Vec<&String> = dedup_keys.iter().chain(dedup_tiebreak.iter()).collect();
                let missing: Vec<usize> =
                    augment.into_iter().chain(tombstone.iter()).filter_map(|k| full_schema.index_of(k).ok()).filter(|i| !p.contains(i)).collect();
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
        // (plan, leg) pairs rather than a plan vec plus a parallel sortable
        // mask: the mask has to stay index-aligned with a list built by
        // flattening three Options, and `LegKind::sortable()` derives the same
        // bit from the identity that can't drift out of step with it.
        // `skip_legs` are Delta legs over date partitions certified
        // duplicate-free: they are unioned ABOVE DedupExec rather than fed
        // through it. Sound because `date` derives from `timestamp` and DML
        // re-appends preserve it, so no dedup key spans a date boundary —
        // dedup over the union equals dedup applied per date.
        let wrap_result_split =
            |mut legs: Vec<(Arc<dyn ExecutionPlan>, crate::read::LegKind)>, skip_legs: Vec<Arc<dyn ExecutionPlan>>| -> DFResult<Arc<dyn ExecutionPlan>> {
                // A leg pruned to nothing (tantivy split with zero surviving files,
                // a date window outside the snapshot) bottoms out in an EmptyExec,
                // which declares no output ordering — and Delta legs are unsortable,
                // so ONE empty leg vetoed `merge_req` below: no SPM, DedupExec fell
                // to full-set over a coalesce, and the most selective point lookups
                // inherited the 2 GiB full-set ceiling (prod 2026-08-20). An empty
                // leg contributes no rows; drop it before the union. Keep one leg if
                // all are empty so the single-plan path stays valid.
                fn provably_empty(plan: &dyn ExecutionPlan) -> bool {
                    plan.is::<datafusion::physical_plan::empty::EmptyExec>() || matches!(plan.children().as_slice(), [child] if provably_empty(child.as_ref()))
                }
                if legs.len() > 1 && legs.iter().any(|(p, _)| provably_empty(p.as_ref())) {
                    match legs.iter().any(|(p, _)| !provably_empty(p.as_ref())) {
                        true => legs.retain(|(p, _)| !provably_empty(p.as_ref())),
                        false => legs.truncate(1),
                    }
                }
                // Under a per-date split the deduped side can end up with NO legs
                // while the certified side carries every row. Everything below
                // indexes `plans[0]` and unions a non-empty vec, so the scan panics
                // with `index out of bounds: the len is 0 but the index is 0`.
                //
                // Observed, not theorised: `a_certification_survives_a_restart_and
                // _still_grants_the_skip` took this panic under full-suite load on
                // 2026-08-22, where the sweep had time to reach the state. It is
                // NOT date composition that empties the leg — a window date with no
                // files for this project leaves the verdict `Granted`, which takes
                // the whole-window skip instead. It is file-level pruning: the
                // uncertified dates' files are all removed by the tantivy/bloom
                // prefilter, which is the ordinary case for a selective point
                // lookup. That is why there is no unit regression test here — a
                // deterministic repro needs the prefilter to empty exactly one side
                // of the split, and a test that passes with and without the guard
                // would be false confidence.
                //
                // The certified legs need no dedup, only the projection debt that
                // the `dedup_on == false` branch below pays.
                if legs.is_empty() && !skip_legs.is_empty() {
                    let projected = skip_legs
                        .into_iter()
                        .map(|leg| match &output_projection {
                            Some(idxs) => Self::project_indices(leg, idxs),
                            None => Ok(leg),
                        })
                        .collect::<DFResult<Vec<_>>>()?;
                    let plan = match projected.len() {
                        1 => projected.into_iter().next().expect("len checked"),
                        _ => UnionExec::try_new(projected)? as Arc<dyn ExecutionPlan>,
                    };
                    return match &tombstone {
                        Some(marker) => Self::filter_tombstones(plan, marker, tombstone_keep),
                        None => Ok(plan),
                    };
                }
                let leg_sortable: Vec<bool> = legs.iter().map(|(_, k)| k.sortable()).collect();
                let legs: Vec<Arc<dyn ExecutionPlan>> = legs
                    .into_iter()
                    .map(|(plan, kind)| match crate::read::ordering_probe_enabled() {
                        true => Arc::new(crate::read::OrderingProbeExec::new(plan, kind)) as Arc<dyn ExecutionPlan>,
                        false => plan,
                    })
                    .collect();
                let shape = *scan_state.lock();
                let us = scan_start.elapsed().as_micros() as u64;
                scan_metrics.record_scan(us, shape.skipped_delta, shape.has_mem, shape.has_delta, shape.fast_resolve_hit, shape.skip_dedup, skip_verdict);
                let dedup_on = !dedup_keys.is_empty() && !shape.skip_dedup;
                let mut plans = legs;
                // Merge-on-read prerequisite: keep-greatest only engages while the
                // input still declares an ordering on the leading dedup key, so the
                // in-memory legs are sorted up to the Delta leg's footer ordering
                // and merged explicitly. The SPM is built HERE, not left to
                // EnforceDistribution — DedupExec declares no required input
                // ordering, so EnforceSorting would delete the injected sorts.
                // Gated on `version_append`: ungated, it charged every scan a
                // blocking SortExec + k-way merge for a dormant feature (the
                // 2026-07-20 OOM shape).
                let mut merge_req = None;
                if dedup_on
                    && table_schema.is_some_and(|t| t.version_append)
                    && let Some(req) = plans.first().and_then(|p| table_schema.and_then(|t| Self::keep_greatest_ordering(t, &p.schema())))
                {
                    // Per-leg sortability: the DELTA leg is NEVER sortable — MOR
                    // UPDATEs make files overlap, and a read-time SortExec over
                    // them exhausted the query pool twice (2026-08-02, and again
                    // via the 08-07 sort-only-the-unordered-branch attempt, whose
                    // unspillable per-partition merges saturated 24 GB; reverted —
                    // footer-less files need REPAIR, not read-time sorting).
                    // `ordered_children` bails whenever an unsortable leg misses
                    // `req`, so a Delta sort is structurally impossible here. The
                    // in-memory legs ARE sortable: bounded, cheap, and exactly
                    // where a fresh version append lives.
                    match crate::read::optimizers::ordered_children(&plans, &req, None, &leg_sortable, false)? {
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
                // `plans` is non-empty on every known path — the guard above takes
                // the one-sided split, and a scan with no legs at all has not been
                // observed. Belt-and-braces because the failure mode of being wrong
                // is a PANICKED QUERY, not a failed one: index no element, and turn
                // an impossible state into an error a caller can see.
                let plan = match plans.len() {
                    0 => return Err(datafusion::error::DataFusionError::Execution(format!("scan produced no legs to union (project_id={project_id})"))),
                    1 => plans.remove(0),
                    _ => UnionExec::try_new(plans)?,
                };
                let plan = match merge_req.clone() {
                    Some(req) => Arc::new(datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec::new(req, plan)),
                    None => plan,
                };
                let plan = match dedup_on {
                    true => Arc::new(
                        crate::read::DedupExec::with_tiebreak(plan, dedup_keys.clone(), dedup_tiebreak.clone(), output_projection.clone())?
                            // Declaring it REQUIRED is what stops EnforceSorting from
                            // deleting the merge above as unused — see the field docs.
                            .requiring(merge_req.clone()),
                    ) as Arc<dyn ExecutionPlan>,
                    // DedupExec is what restores the requested columns when it runs. Skipped,
                    // that debt is still owed: the scan is carrying augmented key columns the
                    // caller never asked for, and without this they leak into the result.
                    false => match &output_projection {
                        Some(idxs) => Self::project_indices(plan, idxs)?,
                        None => plan,
                    },
                };
                // Union the certified-date legs on top. They owe the same
                // projection debt as the `dedup_on == false` branch above: without
                // it the augmented key columns leak into the result and the two
                // sides of the union disagree on schema.
                let plan = match skip_legs.is_empty() {
                    true => plan,
                    false => {
                        let mut all = Vec::with_capacity(skip_legs.len() + 1);
                        all.push(plan);
                        for leg in skip_legs {
                            all.push(match &output_projection {
                                Some(idxs) => Self::project_indices(leg, idxs)?,
                                None => leg,
                            });
                        }
                        UnionExec::try_new(all)? as Arc<dyn ExecutionPlan>
                    }
                };
                match &tombstone {
                    Some(marker) => Self::filter_tombstones(plan, marker, tombstone_keep),
                    None => Ok(plan),
                }
            };
        let wrap_result = |legs: Vec<(Arc<dyn ExecutionPlan>, crate::read::LegKind)>| wrap_result_split(legs, Vec::new());
        // Check if buffered layer is configured
        let has_layer = self.database.buffered_layer().is_some();
        debug!("ProjectRoutingTable::scan - buffered_layer present: {}, project_id: {}", has_layer, project_id);
        let Some(layer) = self.database.buffered_layer() else {
            // No buffered layer, query Delta directly
            debug!("No buffered layer, querying Delta only");
            // A sweep-certified window holds exactly one (winning) row per key,
            // so the stale-version hazard that keeps mutable predicates above
            // DedupExec has no instance — a scan that needs no dedup needs no
            // protection from pruning below it. Pushing those predicates down
            // is the half that matters for dashboards (445x row amplification
            // measured when only `timestamp >=` reached Parquet). Delta-only by
            // construction: the skip is never granted while the MemBuffer leg —
            // where uncertified versions live — is in play.
            let (skip_dedup, plans, certified_plans) = self
                .scan_delta_only(
                    state,
                    projection,
                    &optimized_filters,
                    unstripped_filters,
                    &project_id,
                    query_time_range,
                    &dedup_keys,
                    pre_skip_dedup,
                    &tombstone,
                    orig_limit,
                    limit,
                    true,
                    tantivy_id_filter.as_ref(),
                    tantivy_covered_files.as_ref(),
                    tantivy_exclude.as_ref(),
                    tantivy_row_selections.as_ref(),
                    bloom_rejected.as_ref(),
                )
                .await?;
            if skip_dedup {
                scan_state.lock().skip_dedup = true;
            }
            // This leg IS the Delta read, and until now it said so to nobody:
            // `record_scan` gates every dedup counter on `has_delta`, so a
            // deployment without a buffered layer reported zero eligible scans
            // however many it served. Prod runs with the buffer, which is why the
            // 2026-08-11 measurement was unaffected — but `query_delta_only`
            // (bypass_buffer) takes this path, so tests measured nothing at all.
            scan_state.lock().has_delta = true;
            return wrap_result_split(plans.into_iter().map(|plan| (plan, crate::read::LegKind::Delta)).collect(), certified_plans);
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
        scan_state.lock().skipped_delta = skip_delta;

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
        metrics::counter!(scan_metric_names::MEM_PLAN_TOTAL).increment(1);
        metrics::counter!(scan_metric_names::MEM_PLAN_US_TOTAL).increment(mem_plan_started.elapsed().as_micros() as u64);
        let mem_partitions = mem_leg.partitions;

        let mem_ranges = layer.get_bucket_ranges(&project_id, &self.table_name);

        // Nothing above Delta to union with: query Delta alone.
        debug!("MemBuffer partitions count: {} for {}/{}", mem_partitions.len(), project_id, self.table_name);
        if mem_partitions.is_empty() {
            debug!("No MemBuffer data, querying Delta only for {}/{}", project_id, self.table_name);
            scan_state.lock().has_delta = true;
            // Same guard for the dedup gate and the scan (see branch above, and
            // `scan_delta_only`'s doc comment). `readmit_mutable_filters: false`
            // here matches this branch's existing behavior of leaving
            // `delta_only_filters` unextended on a skip.
            let (skip_dedup, plans, certified_plans) = self
                .scan_delta_only(
                    state,
                    projection,
                    &optimized_filters,
                    unstripped_filters,
                    &project_id,
                    query_time_range,
                    &dedup_keys,
                    pre_skip_dedup,
                    &tombstone,
                    orig_limit,
                    limit,
                    false,
                    tantivy_id_filter.as_ref(),
                    tantivy_covered_files.as_ref(),
                    tantivy_exclude.as_ref(),
                    tantivy_row_selections.as_ref(),
                    bloom_rejected.as_ref(),
                )
                .await?;
            if skip_dedup {
                scan_state.lock().skip_dedup = true;
            }
            return wrap_result_split(plans.into_iter().map(|plan| (plan, crate::read::LegKind::Delta)).collect(), certified_plans);
        }

        // Create MemorySourceConfig with multiple partitions for parallel execution
        let mem_plan = match mem_partitions.is_empty() {
            true => None,
            false => {
                scan_state.lock().has_mem = true;
                Some(self.create_memory_exec(&mem_partitions, projection, mem_leg.sorted)?)
            }
        };

        // If we can skip Delta, return mem plan directly (the hot leg is empty
        // by construction on this path — see above).
        if let Some(mem_plan) = mem_plan.clone().filter(|_| skip_delta) {
            span.record("scan.skipped_delta", true);
            debug!("Skipping Delta scan - query time range entirely within MemBuffer for {}/{}", project_id, self.table_name);
            return wrap_result(vec![(mem_plan, crate::read::LegKind::Mem)]);
        }

        // Build Delta filters with per-bucket exclusion so the union doesn't
        // double-count: Delta excludes the merged (mem ∪ hot) row ranges where
        // those legs are authoritative (`get_bucket_ranges` skips open and
        // force-flushed buckets, whose windows legitimately straddle stores).
        // MERGE-ON-READ: an UPDATE appends a new version at the row's ORIGINAL
        // timestamp — inside an excluded range — so each conjunct is weakened
        // with `OR stamp > gate`. Weakening is safe in one direction only: an
        // over-admitted row is a duplicate DedupExec collapses, an
        // under-admitted one is a stale read; the union path never grants skip_dedup.
        let mut delta_filters = optimized_filters.clone();
        let ts_col = || Box::new(col("timestamp"));
        let ts_lit = |t: i64| Box::new(lit(ScalarValue::TimestampMicrosecond(Some(t), Some("UTC".into()))));
        for (start, end) in crate::write::mem_buffer::merge_ranges(mem_ranges) {
            // NOT (ts >= start AND ts < end)  ≡  (ts < start) OR (ts >= end)
            let below = Expr::BinaryExpr(BinaryExpr { left: ts_col(), op: Operator::Lt, right: ts_lit(start) });
            let at_or_above = Expr::BinaryExpr(BinaryExpr { left: ts_col(), op: Operator::GtEq, right: ts_lit(end) });
            delta_filters.push(Expr::BinaryExpr(BinaryExpr { left: Box::new(below), op: Operator::Or, right: Box::new(at_or_above) }));
        }
        // Execute Delta query — fast path skips the 3 tokio RwLock `.await`s
        // when we've already resolved this (project, table) pair before.
        let resolve_span = tracing::trace_span!(parent: &span, "resolve_delta_table");
        let delta_table = match self.database.try_fast_resolve(&project_id, &self.table_name) {
            Some(t) => {
                scan_state.lock().fast_resolve_hit = Some(true);
                t
            }
            None => {
                scan_state.lock().fast_resolve_hit = Some(false);
                self.database.resolve_table(&project_id, &self.table_name).instrument(resolve_span).await?
            }
        };
        let table = delta_table.read().await;
        let delta_plans = self
            .scan_delta_with_tantivy(
                &table,
                state,
                projection,
                &delta_filters,
                limit,
                tantivy_id_filter.as_ref(),
                tantivy_covered_files.as_ref(),
                tantivy_exclude.as_ref(),
                tantivy_row_selections.as_ref(),
                query_time_range,
                bloom_rejected.as_ref(),
                None,
                None,
            )
            .await?;
        scan_state.lock().has_delta = true;

        // Union the legs in recency order — mem, then Delta — so DedupExec's
        // keep-first favours the freshest copy of a row.
        //
        // Identity travels WITH the plan, so the flatten cannot desynchronise it
        // from the sortability it implies (see `wrap_result`).
        use crate::read::LegKind;
        let mut legs: Vec<(Arc<dyn ExecutionPlan>, LegKind)> = mem_plan.map(|p| (p, LegKind::Mem)).into_iter().collect();
        legs.extend(delta_plans.into_iter().map(|p| (p, LegKind::Delta)));
        wrap_result(legs)
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
    use crate::schema::{FieldDef, SortingColumnDef, TableSchema};

    fn cfg() -> crate::config::ParquetConfig {
        serde_json::from_str("{}").unwrap()
    }

    fn field(name: &str, dt: &str) -> FieldDef {
        FieldDef { name: name.into(), data_type: dt.into(), nullable: true, tantivy: None, dictionary: None, bloom_filter: false, mutable: false }
    }

    fn schema_with(fields: Vec<FieldDef>, sort: Vec<&str>) -> TableSchema {
        TableSchema {
            rollups: vec![],
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

    /// Equivalence check for the streaming k-way merge against the old concat + global-lexsort +
    /// take path.
    ///
    /// Batches are adversarial: individually unsorted, overlapping key ranges, tiny key domain
    /// with observable tie payload, interleaved empty batches, and mixed physical encodings. We
    /// do not assert byte-for-byte row order because the reference path uses unstable sort, so
    /// tie order is already arbitrary. We assert (1) the same multiset of rows, (2) the same
    /// sort-key sequence, and (3) the same `sorted` outcome.
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

    /// An exhausted maintenance pool must name its holders, not just its victim.
    ///
    /// The error message only names the consumer asking for memory, not the holders of the rest
    /// of the pool. This pins the DataFusion contract the fix depends on: a bare pool reports the
    /// reservation and total and stops there, so callers must record who holds what.
    #[test]
    fn an_exhausted_maintenance_pool_names_the_consumers_holding_it() {
        use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool, TrackConsumersPool};
        let top = std::num::NonZeroUsize::new(5).expect("5 is non-zero");
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(FairSpillPool::new(4 * 1024 * 1024), top));

        // A fat holder takes most of the pool, then a small consumer is starved
        // — exactly the shape of the prod failure.
        let hog = MemoryConsumer::new("TheBinThatAteThePool").register(&pool);
        hog.grow(3 * 1024 * 1024);
        let starved = MemoryConsumer::new("SortPreservingMergeExec").register(&pool);
        let error = starved.try_grow(3 * 1024 * 1024).expect_err("pool is too small for both");

        let text = error.to_string();
        assert!(text.contains("TheBinThatAteThePool"), "the HOLDER must be named, that is the whole point: {text}");
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

    // Slice-coverage merge: disjoint stays disjoint, touching/overlapping fuse,
    // and out-of-order inserts still converge to one day-spanning interval.
    #[test]
    fn clean_interval_merge() {
        let merged = |pairs: &[(i64, i64)]| {
            let mut v = Vec::new();
            pairs.iter().for_each(|&p| merge_clean_interval(&mut v, p));
            v
        };
        assert_eq!(merged(&[(0, 10), (20, 30)]), vec![(0, 10), (20, 30)], "a gap must survive");
        assert_eq!(merged(&[(0, 10), (10, 20)]), vec![(0, 20)], "half-open adjacency fuses");
        assert_eq!(merged(&[(0, 15), (10, 20)]), vec![(0, 20)], "overlap fuses");
        assert_eq!(merged(&[(12, 24), (0, 6), (6, 12)]), vec![(0, 24)], "order of arrival must not matter");
        assert_eq!(merged(&[(0, 24), (5, 10)]), vec![(0, 24)], "a contained slice changes nothing");
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
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serial_test::serial;
    use test_case::test_case;

    use super::*;
    use crate::{config::AppConfig, schema::get_default_schema, support::test_helpers::*};

    /// A pass deadline no test will reach, for the drain's bounding parameter.
    /// `Instant` has no MAX, and adding `Duration::MAX` overflows.
    fn far_future() -> std::time::Instant {
        std::time::Instant::now() + std::time::Duration::from_secs(86_400)
    }

    /// Midnight UTC of `date`, in micros.
    fn midnight_micros(date: chrono::NaiveDate) -> i64 {
        date.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp_micros()
    }

    /// Insert one `otel_logs_and_spans` span at `ts` micros, op name "op".
    async fn insert_a_span(db: &Database, project: &str, id: &str, ts: i64) -> Result<()> {
        db.insert_records_batch(project, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts(id, "op", project, ts)])?], true, None).await?;
        Ok(())
    }

    /// Register a BYO-bucket (custom-storage) tenant on local MinIO, same bucket, distinct prefix.
    async fn register_custom_storage(db: &Database, project_id: &str, table_name: &str, s3_prefix: &str) {
        db.storage_configs.write().await.insert(
            (project_id.to_string(), table_name.to_string()),
            StorageConfig {
                project_id: project_id.to_string(),
                table_name: table_name.to_string(),
                s3_bucket: "timefusion-tests".to_string(),
                s3_prefix: s3_prefix.to_string(),
                s3_region: "us-east-1".to_string(),
                s3_access_key_id: "minioadmin".to_string(),
                s3_secret_access_key: "minioadmin".to_string(),
                s3_endpoint: Some("http://127.0.0.1:9000".to_string()),
            },
        );
    }

    /// A unit whose slice has not finished yet must back off past the slice, not spin.
    ///
    /// A flat retry interval left a day-wide unit permanently eligible until midnight, monopolising
    /// `claim_next` ahead of tens of thousands of pending units that had never run. Backing off
    /// past the slice's end lets other work run.
    #[test]
    fn a_slice_that_has_not_finished_backs_off_past_its_own_end() {
        use crate::maintenance_coordinator::{FINALIZATION_DELAY_MICROS, TimeSlice};
        const HOUR: i64 = 3_600 * 1_000_000;
        let now = 20 * HOUR;
        let day = TimeSlice::new(0, 24 * HOUR).expect("day slice");

        // Today's day-wide unit cannot succeed before midnight plus finalization.
        let delay = super::buffered_source_retry_delay(day, now);
        let expected = (24 * HOUR - now + FINALIZATION_DELAY_MICROS) as u64;
        assert_eq!(delay.as_micros() as u64, expected, "a unit must wait out the rest of its own slice, not 5 seconds");
        assert!(delay.as_secs() > 4 * 3_600, "the old 5s retry is what made this an unbounded spin");

        // A sealed slice merely waiting on a slow flush is unchanged: the floor
        // applies, so ordinary frontier work keeps retrying promptly.
        let sealed = TimeSlice::new(0, HOUR).expect("sealed slice");
        assert_eq!(super::buffered_source_retry_delay(sealed, now).as_secs(), 5, "an already-sealed slice keeps the fast retry");
    }

    /// A pending unit for one rollup tier must not veto planning another tier of the same day.
    ///
    /// A day has one unit per tier plus a dedup, and they are independent: the 1h tier reads the 1m
    /// tier, so a pending 1m unit says nothing about whether 1h can be planned. Keyed on the day
    /// alone, a pending frontier base unit vetoed every tier of that day, leaving no historical
    /// derived tasks ever created.
    #[tokio::test]
    async fn a_queued_unit_for_one_tier_does_not_veto_planning_another() -> Result<()> {
        use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
        let mut cfg = (*create_test_config("per-tier-veto")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        let project = format!("veto_{}", uuid::Uuid::new_v4().simple());
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "a", day.timestamp_micros()).await?;

        // Exactly the prod shape: the day carries a pending frontier slice for
        // ONE tier, and nothing at all for the others.
        let blocking_table = {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            let keys: Vec<_> = journal.tasks().map(|task| task.key.clone()).collect();
            for key in &keys {
                journal.complete(key);
            }
            let blocking = keys.iter().find(|key| key.operation == Operation::BaseRollup).cloned().expect("the write path queues a base rollup for the day");
            let start = midnight_micros(day.date_naive());
            journal.enqueue(TaskKey { slice: TimeSlice::new(start, start + 600_000_000)?, ..blocking.clone() }, 0, 0, 0);
            blocking.physical_table
        };

        db.plan_rollup_backfill().await?;

        let (blocked, others) = {
            let journal = db.maintenance_tasks.lock().unwrap();
            let day_tasks: Vec<_> = journal
                .tasks()
                .filter(|task| task.key.project_id == project && task.state != crate::maintenance_coordinator::TaskState::Complete)
                .map(|task| task.key.physical_table.clone())
                .collect();
            (day_tasks.iter().filter(|t| **t == blocking_table).count(), day_tasks.iter().filter(|t| **t != blocking_table).count())
        };
        assert_eq!(blocked, 1, "the tier that already has work queued must not be enqueued twice");
        assert!(others > 0, "a sibling tier of the same day must still be planned, got {others}");
        Ok(())
    }

    /// A partition of empty files must not read as coverage.
    ///
    /// A rollup unit that aggregated nothing still commits a file. A zero-row partition made the
    /// day look covered, so it was never rebuilt and stayed empty. Empty partitions must read as
    /// uncovered so the coordinator re-queues them.
    #[test]
    fn an_empty_partition_file_is_not_coverage() {
        assert!(super::partition_file_is_empty(Some(0)), "an explicit zero-row file proves nothing was built");
        assert!(!super::partition_file_is_empty(Some(1)), "a file with rows is coverage");
        // Unknown must read as covered: the other direction re-plans finished
        // work forever for every partition written before stats existed.
        assert!(!super::partition_file_is_empty(None), "missing stats are unknown, never empty");
    }

    /// While coverage is short, the goal outranks the ceiling.
    ///
    /// The live frontier alone can hold the journal above the ceiling and is replenished by
    /// ingest, so waiting for it to fall means waiting forever. Self-limiting in both directions:
    /// a pass admits at most `BACKFILL_PARTITIONS_PER_PASS` cells, and coverage reaching
    /// `COVERAGE_SHORT_DAYS` restores the ceiling.
    #[tokio::test]
    async fn coverage_short_backfill_outranks_the_pending_ceiling() -> Result<()> {
        use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
        use std::sync::atomic::Ordering::Relaxed;
        let mut cfg = (*create_test_config("backfill-ceiling-bypass")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        let project = format!("ceil_{}", uuid::Uuid::new_v4().simple());
        let ts = (Utc::now() - chrono::Duration::days(3)).timestamp_micros();
        insert_a_span(&db, &project, "a", ts).await?;
        {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            // The write path already queued this day; complete it so the planner
            // re-derives the day from the (missing) tier coverage.
            let keys: Vec<_> = journal.tasks().map(|task| task.key.clone()).collect();
            for key in keys {
                journal.complete(&key);
            }
            // Stuff the journal past the ceiling with unrelated debt.
            for i in 0..25_100i64 {
                journal.enqueue(
                    TaskKey {
                        physical_table: "debt".to_owned(),
                        source: "debt".to_owned(),
                        project_id: format!("dummy{i}"),
                        slice: TimeSlice::new(i * 60_000_000, (i + 1) * 60_000_000)?,
                        operation: Operation::Repair,
                    },
                    0,
                    0,
                    0,
                );
            }
        }
        let gauge = &crate::observability::maintenance_stats().rollup_median_contiguous_days;
        let restore = gauge.load(Relaxed);
        gauge.store(30, Relaxed);
        let healthy = db.plan_rollup_backfill().await?;
        gauge.store(0, Relaxed);
        let short = db.plan_rollup_backfill().await?;
        gauge.store(restore, Relaxed);

        assert_eq!(healthy, 0, "at the ceiling with healthy coverage, the backfill still defers");
        assert!(short > 0, "coverage-short goal work outranks the ceiling, got {short}");
        Ok(())
    }

    /// The backfill ceiling must defer only the enqueue, never the whole pass.
    ///
    /// The live frontier alone can keep pending above the ceiling indefinitely, so the planner
    /// would return at its first statement and take several other outputs with it: the
    /// `rollup_median_contiguous_days` gauge that `coverage_is_short` actually reads, the
    /// per-project coverage contiguity log, and the base-tier proof for already-queued derived units.
    #[tokio::test]
    async fn the_backfill_ceiling_defers_enqueueing_without_stopping_the_pass() -> Result<()> {
        use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
        use std::sync::atomic::Ordering::Relaxed;
        let mut cfg = (*create_test_config("backfill-ceiling-pass")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        let project = format!("ceil_{}", uuid::Uuid::new_v4().simple());
        let ts = (Utc::now() - chrono::Duration::days(3)).timestamp_micros();
        insert_a_span(&db, &project, "a", ts).await?;

        // Stuff the journal past the ceiling with work unrelated to rollup.
        {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            for i in 0..25_100i64 {
                journal.enqueue(
                    TaskKey {
                        physical_table: "debt".to_owned(),
                        source: "debt".to_owned(),
                        project_id: format!("dummy{i}"),
                        slice: TimeSlice::new(i * 60_000_000, (i + 1) * 60_000_000)?,
                        operation: Operation::Repair,
                    },
                    0,
                    0,
                    0,
                );
            }
        }
        let gauge = &crate::observability::maintenance_stats().rollup_median_contiguous_days;
        let restore = gauge.load(Relaxed);
        // A value the pass cannot legitimately produce for this fixture, so only
        // a pass that actually RAN can overwrite it.
        gauge.store(u64::MAX, Relaxed);
        let queued = db.plan_rollup_backfill().await?;
        let recomputed = gauge.load(Relaxed);
        gauge.store(restore, Relaxed);

        assert_eq!(queued, 0, "past the ceiling nothing new is enqueued");
        assert_ne!(recomputed, u64::MAX, "but the pass still ran and recomputed the goal gauge");
        Ok(())
    }

    /// Coverage published for one source must survive planning the next.
    ///
    /// `set_base_tier_ready` / `set_tier_holes` replace wholesale, which is right because coverage can
    /// go backwards, but calling them per source inside the planner's loop meant the last table
    /// processed wiped every earlier one. The ready set and hole ranking must be accumulated across
    /// sources before they are written.
    #[tokio::test]
    async fn coverage_published_for_one_source_survives_planning_the_next() -> Result<()> {
        let mut cfg = (*create_test_config("multi-source-coverage")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        let project = format!("multi_{}", uuid::Uuid::new_v4().simple());
        let ts = (Utc::now() - chrono::Duration::days(3)).timestamp_micros();
        // `all_tables` yields several unified tables, so the planner loop runs
        // more than once whatever we write to. Only this source has coverage.
        insert_a_span(&db, &project, "a", ts).await?;

        db.plan_rollup_backfill().await?;

        // `tier_holes` is the same accumulate-across-sources path and is
        // non-empty immediately: the source has a partition, the tier does not.
        // Before the fix this held only whichever table sorted LAST, so the one
        // source that actually has cells was wiped by an empty sibling.
        let sources = {
            let journal = db.maintenance_tasks.lock().unwrap();
            journal.tier_hole_sources()
        };
        assert!(sources.contains("otel_logs_and_spans"), "cells published for a source must survive planning the others, got {sources:?}");
        Ok(())
    }

    /// A rewrite must carry coverage identity forward and must never invent it.
    ///
    /// Dropping the `timefusion.*` identity during compaction destroys the identity that
    /// `recover_rollup_coverage` derives coverage from. Carry it forward identically only; unioning
    /// slices would claim coverage of any gap between inputs, and `rollup_slice_complete` matches
    /// `task.key.slice` exactly, so a unioned slice would be discarded by recovery regardless.
    #[test]
    fn a_rewrite_carries_coverage_identity_only_when_every_input_agrees() {
        use crate::maintenance_coordinator::{TAG_GENERATION, TAG_PROJECT, TAG_SLICE_END, TAG_SLICE_START, TAG_SOURCE, TAG_SOURCE_FINGERPRINT};
        let add = |slice_start: &str, generation: &str| {
            let mut a = deltalake::kernel::Add {
                path: format!("f{slice_start}.parquet"),
                partition_values: Default::default(),
                size: 1,
                modification_time: 0,
                data_change: true,
                ..Default::default()
            };
            a.tags = Some(HashMap::from([
                (TAG_SOURCE.to_owned(), Some("otel_logs_and_spans".to_owned())),
                (TAG_PROJECT.to_owned(), Some("p".to_owned())),
                (TAG_SLICE_START.to_owned(), Some(slice_start.to_owned())),
                (TAG_SLICE_END.to_owned(), Some("86400000000".to_owned())),
                (TAG_SOURCE_FINGERPRINT.to_owned(), Some("77".to_owned())),
                (TAG_GENERATION.to_owned(), Some(generation.to_owned())),
            ]));
            a
        };

        // One publication cut into two files by the size limit: identical tags.
        let carried = super::carried_coverage_tags(&[add("0", "g1"), add("0", "g1")]);
        assert_eq!(carried.get(TAG_SLICE_START).map(String::as_str), Some("0"), "identical inputs carry their identity forward");
        assert_eq!(carried.len(), 6, "all six coverage tags travel together or not at all");

        // Different slices: the union would claim the gap between them.
        assert!(super::carried_coverage_tags(&[add("0", "g1"), add("3600000000", "g1")]).is_empty(), "differing slices must NOT be merged into a span");
        // Different generations: different source state, not one publication.
        assert!(super::carried_coverage_tags(&[add("0", "g1"), add("0", "g2")]).is_empty(), "differing generations must not be conflated");
        // An untagged input means the output's coverage is unknown.
        let mut untagged = add("0", "g1");
        untagged.tags = None;
        assert!(super::carried_coverage_tags(&[add("0", "g1"), untagged]).is_empty(), "an untagged input makes the merged coverage unknowable");
        // And nothing is invented from nothing.
        assert!(super::carried_coverage_tags(&[]).is_empty());
    }

    /// Consolidation admits on size. An untagged-but-large file is not debt.
    ///
    /// The sorted-run tag is intent, not fact: the flush path can stamp a sorted footer without the
    /// tag, so untagged means suspect, not unsorted. Repair footer-checks before rewriting; consolidation
    /// did not, so it treated suspicion as proof and re-admitted every partition.
    #[tokio::test]
    async fn consolidation_admits_on_size_not_on_a_missing_sort_tag() -> Result<()> {
        use crate::maintenance_coordinator::{Operation, TaskState};
        let db = Database::with_config(create_test_config("consolidation-size")).await?;
        let project = format!("size_{}", uuid::Uuid::new_v4().simple());
        // One sealed day, flushed once: a single untagged file, well under the
        // 256 MB target. One file is not two, so it is not consolidation debt
        // however untagged it is.
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "a", day.timestamp_micros()).await?;

        db.plan_compaction_debt().await?;

        let sealed_tasks = {
            let journal = db.maintenance_tasks.lock().unwrap();
            journal
                .tasks()
                .filter(|task| {
                    task.key.project_id == project
                        && task.key.operation == Operation::SealedConsolidation
                        && matches!(task.state, TaskState::Pending | TaskState::Retry)
                })
                .count()
        };
        assert_eq!(sealed_tasks, 0, "a single untagged file is a Repair suspect, not consolidation debt");
        Ok(())
    }

    /// A clean day-wide coordinator dedup unit must certify the partition.
    ///
    /// Certification removes `DedupExec` from a plan, and `dedup_sweep` used to be the only thing
    /// that granted it. The dedup cron skips rollup-declared tables, so for tables like
    /// `otel_logs_and_spans` the sweep stopped running and this path did not take over. This test
    /// pins that coordinator dedup must certify.
    #[test_case(true ; "persistence enabled")]
    #[test_case(false ; "persistence disabled")]
    #[serial]
    #[tokio::test]
    async fn a_clean_day_wide_coordinator_dedup_certifies_the_partition(persist: bool) -> Result<()> {
        use crate::maintenance_coordinator::{DAY_MICROS, Operation, TaskKey, TimeSlice};
        let mut config = create_test_config("coord-dedup-certify");
        Arc::make_mut(&mut config).maintenance.timefusion_dedup_certification_persist = persist;
        let db = Database::with_config(config).await?;
        let project = format!("cert_{}", uuid::Uuid::new_v4().simple());
        // A sealed day with NO duplicates: the pass must drop nothing and leave
        // the file set where it found it, which is exactly what certification
        // requires.
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "only", day.timestamp_micros()).await?;

        let date = day.date_naive();
        let day_start = midnight_micros(date);
        let slice = TimeSlice::new(day_start, day_start + DAY_MICROS)?;
        {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            journal.enqueue(
                TaskKey {
                    physical_table: "otel_logs_and_spans".to_owned(),
                    source: "otel_logs_and_spans".to_owned(),
                    project_id: project.clone(),
                    slice,
                    operation: Operation::Dedup,
                },
                0,
                1024,
                0,
            );
        }
        assert!(db.run_coordinator_dedup_once().await?, "the day-wide dedup unit must be claimed and run");

        let key = (project.clone(), "otel_logs_and_spans".to_owned(), date.to_string());
        assert!(db.dedup_clean_fp.contains_key(&key), "a clean day-wide unit must certify the partition; without it DedupExec survives in every 30d plan");
        let stored = crate::storage::load_sidecar::<crate::storage::StoredCertification>(&db.config.core.timefusion_data_dir, crate::storage::CERTIFICATIONS);
        let persisted = stored.iter().any(|entry| entry.project_id == project && entry.table_name == "otel_logs_and_spans" && entry.date == date.to_string());
        assert_eq!(
            persisted, persist,
            "a coordinator-owned table never reaches the legacy sweep's persistence site, so its grant must honor the persistence flag here"
        );
        Ok(())
    }

    /// Enqueue one Dedup unit over `[start, end)` and run the coordinator once.
    async fn run_dedup_slice(db: &Database, project: &str, start: i64, end: i64) -> Result<bool> {
        use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
        let key = TaskKey {
            physical_table: "otel_logs_and_spans".to_owned(),
            source: "otel_logs_and_spans".to_owned(),
            project_id: project.to_owned(),
            slice: TimeSlice::new(start, end)?,
            operation: Operation::Dedup,
        };
        db.maintenance_tasks.lock().unwrap().enqueue(key, 0, 1024, 0);
        db.run_coordinator_dedup_once().await
    }

    /// Clean NARROW dedup units must accumulate into certification.
    ///
    /// Prod never produces surviving day-wide units: `coarsen_to_width` caps units at
    /// `MAX_DECODED_BYTES` (≈6h for otel_logs_and_spans) and true day-wide units die at the
    /// coordinator's Dedup deadline — so `cert_granted_total` stayed 0 forever (2026-08-20).
    /// Certification is a property of the partition, not the unit shape: clean slices whose
    /// union covers the UTC day over an unmoved file set prove what one day-wide pass does.
    #[tokio::test]
    #[serial]
    async fn clean_slice_units_accumulate_to_certify_the_partition() -> Result<()> {
        use crate::maintenance_coordinator::DAY_MICROS;
        let db = Database::with_config(create_test_config("slice-cov-certify")).await?;
        let project = format!("cert_{}", uuid::Uuid::new_v4().simple());
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "only", day.timestamp_micros()).await?;
        let date = day.date_naive();
        let day_start = midnight_micros(date);
        let half = day_start + DAY_MICROS / 2;
        let key = (project.clone(), "otel_logs_and_spans".to_owned(), date.to_string());

        assert!(run_dedup_slice(&db, &project, day_start, half).await?, "first half-day unit must run");
        assert!(!db.dedup_clean_fp.contains_key(&key), "half a day proves nothing on its own");
        assert!(run_dedup_slice(&db, &project, half, day_start + DAY_MICROS).await?, "second half-day unit must run");
        assert!(db.dedup_clean_fp.contains_key(&key), "two clean halves cover the day and must certify the partition");
        Ok(())
    }

    /// Accumulated slice coverage must survive a restart.
    ///
    /// Prod restarts every 1-2h and a day needs ~8 clean 3h slices; the journal
    /// durably marks completed slices Complete (never re-run) while the coverage
    /// evidence lived only in memory — so a day straddling any restart could
    /// NEVER certify, and cert_granted_total stayed 0 (diagnosed 2026-08-21).
    #[tokio::test]
    #[serial]
    async fn slice_coverage_survives_a_restart() -> Result<()> {
        use crate::maintenance_coordinator::DAY_MICROS;
        let cfg = create_test_config("slice-cov-restart");
        let db = Database::with_config(cfg.clone()).await?;
        let project = format!("cert_{}", uuid::Uuid::new_v4().simple());
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "only", day.timestamp_micros()).await?;
        let date = day.date_naive();
        let day_start = midnight_micros(date);
        let half = day_start + DAY_MICROS / 2;
        let key = (project.clone(), "otel_logs_and_spans".to_owned(), date.to_string());

        assert!(run_dedup_slice(&db, &project, day_start, half).await?, "first half-day unit must run");
        assert!(!db.dedup_clean_fp.contains_key(&key));
        drop(db); // the restart: coverage evidence must not die with the process

        let db = Database::with_config(cfg).await?;
        assert!(run_dedup_slice(&db, &project, half, day_start + DAY_MICROS).await?, "second half-day unit must run post-restart");
        assert!(
            db.dedup_clean_fp.contains_key(&key),
            "coverage accumulated before the restart plus the post-restart half must certify; \
             losing it on restart is why prod (restarting every 1-2h) never granted"
        );
        Ok(())
    }

    /// A write between clean slices moves the fingerprint and voids accumulated coverage.
    ///
    /// The evidence is per-file-set: a slice proved clean over yesterday's files says nothing
    /// about today's. Without the fp reset, a partition written mid-accumulation would certify
    /// while duplicates could sit in the never-re-swept first half.
    #[tokio::test]
    #[serial]
    async fn a_write_between_clean_slices_voids_accumulated_coverage() -> Result<()> {
        use crate::maintenance_coordinator::DAY_MICROS;
        let db = Database::with_config(create_test_config("slice-cov-voided")).await?;
        let project = format!("cert_{}", uuid::Uuid::new_v4().simple());
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "only", day.timestamp_micros()).await?;
        let date = day.date_naive();
        let day_start = midnight_micros(date);
        let half = day_start + DAY_MICROS / 2;
        let key = (project.clone(), "otel_logs_and_spans".to_owned(), date.to_string());

        assert!(run_dedup_slice(&db, &project, day_start, half).await?, "first half-day unit must run");
        // A new file lands in the partition: the fp the first slice was proved under is dead.
        insert_a_span(&db, &project, "late", day.timestamp_micros() + 1).await?;
        assert!(run_dedup_slice(&db, &project, half, day_start + DAY_MICROS).await?, "second half-day unit must run");
        assert!(!db.dedup_clean_fp.contains_key(&key), "a moved file set voids the first slice's evidence — no certification");
        Ok(())
    }

    /// One negligible tenant must not pin the control signal.
    ///
    /// `min_contiguous_days` returns the worst project — the honest GOAL metric,
    /// "can every tenant answer a 30d panel" — and the median, which is what
    /// `coverage_is_short` steers by. Prod 2026-08-19: `5ce1c976` holds one file
    /// per day at 0.00 GB and has zero rows in the 1h tier, so it scored 2 and
    /// pinned the fleet while shipbubble was at 12/14 and otel_metrics' 1m tier
    /// had reached the full 30. That held `CYCLE_COVERAGE_SHORT` selected
    /// indefinitely, giving Dedup 1 of 10 slots instead of 3 — and Dedup is what
    /// certifies partitions, without which every query unions a raw scan.
    #[test]
    fn a_single_lagging_tenant_pins_the_goal_but_not_the_control_signal() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 8, 20).expect("date");
        let day = |back: u64| today.checked_sub_days(chrono::Days::new(back)).expect("date");
        // Four healthy tenants covered for 10 days back, one laggard covered for 2.
        let healthy = ["a", "b", "c", "d"];
        let mut covered = HashSet::new();
        let mut source = HashSet::new();
        // Source rows on every day in the horizon, so a project's score is
        // decided by COVERAGE rather than running off the end of its data — a
        // day the source never held counts as answered.
        for back in 1..=CONTIGUITY_HORIZON_DAYS {
            for project in healthy.into_iter().chain(std::iter::once("laggard")) {
                source.insert((project.to_owned(), day(back)));
            }
        }
        for back in 1..=10 {
            for project in healthy {
                covered.insert((project.to_owned(), day(back)));
            }
        }
        for back in 1..=2 {
            covered.insert(("laggard".to_owned(), day(back)));
        }
        let active: HashSet<&str> = healthy.into_iter().chain(std::iter::once("laggard")).collect();

        let (worst, worst_project, median) = min_contiguous_days(&covered, &source, today, &active);
        assert_eq!(worst, 2, "the goal metric still reports the worst tenant");
        assert_eq!(worst_project, Some("laggard"), "and still names it");
        assert_eq!(median, 10, "the control signal must reflect the fleet, not its worst member");
    }

    /// Age a long-sealed fragmented partition from its seal time, not its scheduling time.
    ///
    /// Hygiene work is re-derived on every restart, so `created_unix_ms` would reset to `now` and
    /// the starvation threshold became unreachable. The seal time is a property of the data, so it
    /// survives restarts and tells the scheduler how long the partition has been out of policy.
    #[tokio::test]
    async fn a_long_sealed_partition_is_aged_from_when_it_sealed_not_when_rescanned() -> Result<()> {
        use crate::maintenance_coordinator::Operation;
        let db = Database::with_config(create_test_config("hygiene-seal-age")).await?;
        let project = format!("age_{}", uuid::Uuid::new_v4().simple());
        // Two flushes on a long-sealed day => two small files => consolidation
        // debt, planned fresh by a scan running right now.
        let day = Utc::now() - chrono::Duration::days(6);
        for id in ["a", "b"] {
            insert_a_span(&db, &project, id, day.timestamp_micros()).await?;
        }
        db.plan_compaction_debt().await?;

        let created = {
            let journal = db.maintenance_tasks.lock().unwrap();
            journal.tasks().find(|task| task.key.project_id == project && task.key.operation == Operation::SealedConsolidation).map(|task| task.created_unix_ms)
        };
        let created = created.expect("a six-day-old partition with two small files is consolidation debt");
        let now_ms = u64::try_from(crate::support::now_micros().div_euclid(1_000)).unwrap_or_default();
        let waited_hours = (now_ms.saturating_sub(created)) / 3_600_000;
        assert!(
            waited_hours >= 24,
            "a partition sealed six days ago must read as waiting >24h so it can escalate; got {waited_hours}h — \
             aged from the rescan it would read as 0 and starve forever"
        );
        Ok(())
    }

    /// File hygiene must not pack a rollup TIER.
    ///
    /// A tier file carries the identity tags `recover_rollup_coverage` reads,
    /// and that function skips any file missing ANY of them. Packing merges
    /// files from different slices, so `carried_coverage_tags` finds the inputs
    /// disagree and emits nothing — the packed file proves no coverage, and the
    /// slices it absorbed lose their only representation.
    ///
    /// Prod 2026-08-19 spent half of file hygiene doing this: 58 of 119
    /// HotPacking/SealedConsolidation claims targeted tier tables, the 1m tier
    /// carried 79 untagged live files on 08-19 against zero on every older day,
    /// and a 10-day historical query reported `rollup_miss_not_built_total +45`
    /// as its only miss reason.
    #[tokio::test]
    async fn compaction_debt_skips_rollup_tier_tables() -> Result<()> {
        use crate::maintenance_coordinator::Operation;
        let mut cfg = (*create_test_config("debt-skips-tiers")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        let project = format!("tier_{}", uuid::Uuid::new_v4().simple());
        let day = Utc::now() - chrono::Duration::days(4);
        for id in ["a", "b"] {
            insert_a_span(&db, &project, id, day.timestamp_micros()).await?;
        }
        db.plan_compaction_debt().await?;

        let (on_source, on_tier) = {
            let journal = db.maintenance_tasks.lock().unwrap();
            let hygiene =
                |task: &crate::maintenance_coordinator::MaintenanceTask| matches!(task.key.operation, Operation::HotPacking | Operation::SealedConsolidation);
            (
                journal.tasks().filter(|t| hygiene(t) && t.key.physical_table == "otel_logs_and_spans").count(),
                journal.tasks().filter(|t| hygiene(t) && t.key.physical_table.contains("_rollup_")).count(),
            )
        };
        assert!(on_source > 0, "the scan must still plan hygiene for the source table");
        assert_eq!(on_tier, 0, "a rollup tier must never be packed — packing it erases the coverage it proves");
        Ok(())
    }

    /// `run-unit` must run the unit it was ASKED for.
    ///
    /// It enqueues its key and then calls the ordinary coordinator runner, which claims whatever
    /// ranks FIRST — and the journal it claims from is not scratch: a Database built against real
    /// storage plans that storage's whole outstanding queue into it. So the CLI ran other
    /// projects' slices while the requested one stayed Pending, and reported success either way
    /// because the report echoes the requested key. A 100-unit repair pass on 2026-08-20 spent 28
    /// units that way and left every partition it named untouched.
    #[tokio::test]
    async fn run_unit_runs_the_requested_project_and_not_another() -> Result<()> {
        use crate::maintenance_coordinator::Operation;
        let mut cfg = (*create_test_config("run-unit-targeting")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        // The background coordinator would roll up BOTH projects on its own,
        // which is exactly what this test must not mistake for the CLI's doing.
        db.cancel_maintenance();
        // The decoy sits in the LIVE FRONTIER, which `scheduling_class` ranks
        // ahead of every sealed unit unconditionally — 30 minutes back, so it is
        // past FINALIZATION_DELAY and genuinely claimable. Equal-priority
        // projects would make which one runs a coin flip; the point here is that
        // the CLI ignores ranking entirely.
        let day = (Utc::now() - chrono::Duration::days(12)).date_naive();
        let (wanted, other) = (format!("want_{}", uuid::Uuid::new_v4().simple()), format!("other_{}", uuid::Uuid::new_v4().simple()));
        for (project, at) in [
            (&wanted, day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros()),
            (&other, (Utc::now() - chrono::Duration::minutes(30)).timestamp_micros()),
        ] {
            insert_a_span(&db, project, "a", at).await?;
        }

        // Make the decoy ELIGIBLE. The write path stamps a future deadline
        // (finalization delay), so in a fresh journal the CLI's own key is the
        // only claimable one and the bug cannot show. Prod's journal is the
        // opposite: thousands of units already past their deadline, ranked ahead
        // of anything the CLI adds.
        {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            let decoy = journal
                .tasks()
                .find(|task| task.key.project_id == other && task.key.operation == Operation::BaseRollup)
                .map(|task| task.key.clone())
                .expect("the write path queues a base rollup for the decoy");
            journal.enqueue(decoy, 0, crate::maintenance_coordinator::MAX_DECODED_BYTES, 0);
        }

        let report = db.run_unit_once("otel_logs_and_spans", &wanted, day, Operation::BaseRollup, 24, 0).await?;
        // THE assertion. Under the bug the runner claimed the decoy and the
        // requested key was left untouched at Pending — which is exactly what
        // prod reported for 28 of 100 targeted repair units, while the CLI's own
        // summary line echoed the requested key and read as success.
        assert_eq!(report.state, Some(crate::maintenance_coordinator::TaskState::Complete), "the REQUESTED unit must be the one that ran");

        // And the decoy must not have been published on the CLI's behalf. It is
        // retired so it cannot be claimed, so `state` alone cannot tell the two
        // apart — a publication is what proves work was actually done for it.
        let decoy_published = {
            let journal = db.maintenance_tasks.lock().unwrap();
            journal.tasks().filter(|task| task.key.project_id == other).any(|task| task.publication.is_some())
        };
        assert!(!decoy_published, "a CLI unit must not run another project's work");
        Ok(())
    }

    /// The safety net for the slice-coverage source-row guard: a freshly built
    /// rollup must STILL route, and a source that gains rows afterwards must
    /// STOP routing and answer from raw.
    ///
    /// Both halves matter. Prod 2026-08-22 served a 3-day throughput chart that
    /// returned 4.43M of 7.29M rows because nothing re-checked a slice against
    /// its source; the guard that fixes it fails the opposite way, silently
    /// refusing every rollup, and that failure is invisible in a correctness
    /// assertion alone.
    #[tokio::test]
    #[serial]
    async fn a_built_rollup_routes_and_stops_routing_once_its_source_grows() -> Result<()> {
        use crate::maintenance_coordinator::Operation;
        let mut cfg = (*create_test_config("slice-rows-guard")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        db.cancel_maintenance();
        let project = format!("guard_{}", uuid::Uuid::new_v4().simple());
        let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
        let at = day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros();
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("a", "op", &project, at)])?], true, None).await?;
        db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;

        let covered = |db: &Database| {
            db.rollup_slice_coverage
                .iter()
                .filter(|entry| entry.key().0 == project && entry.key().1 == "otel_logs_and_spans")
                .map(|entry| entry.value().source_rows)
                .collect::<Vec<_>>()
        };
        let witnesses = covered(&db);
        assert!(!witnesses.is_empty(), "the build must publish slice coverage");
        assert!(witnesses.iter().all(Option::is_some), "every published slice must carry its source-row witness: {witnesses:?}");

        // The partition as the build saw it — the same computation the read
        // path compares against, so this is what the guard must accept.
        let source = db.resolve_table(&project, "otel_logs_and_spans").await?;
        let now = {
            let table = source.read().await;
            Database::partition_stats_bounded(&table, tiebreak_of("otel_logs_and_spans"), &|_, _| i64::MAX)?
                .remove(&(project.clone(), day.to_string()))
                .map(|stats| stats.rows)
        };
        assert_eq!(
            witnesses.first().copied().flatten(),
            now.and_then(|rows| u64::try_from(rows).ok()),
            "a freshly built slice must agree with its source, or the guard refuses every rollup"
        );
        assert!(crate::rollup::slice_coverage_agrees(&witnesses, now.and_then(|rows| u64::try_from(rows).ok())), "a fresh build must be trusted");

        // Rows arrive after the build. The witness is now stale, and the guard
        // must refuse rather than serve an aggregate short of the partition.
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("b", "op", &project, at + 1)])?], true, None).await?;
        let grown = {
            let table = source.read().await;
            Database::partition_stats_bounded(&table, tiebreak_of("otel_logs_and_spans"), &|_, _| i64::MAX)?
                .remove(&(project.clone(), day.to_string()))
                .map(|stats| stats.rows)
        };
        assert_ne!(now, grown, "the write must move the partition's row count, or this proves nothing");
        assert!(
            !crate::rollup::slice_coverage_agrees(&witnesses, grown.and_then(|rows| u64::try_from(rows).ok())),
            "a slice built before the write must not be served afterwards"
        );
        Ok(())
    }

    /// A partition still holding an untagged tier file must be QUEUED for rebuild.
    ///
    /// `slice_retires` can retire such a file, but only when something publishes that partition,
    /// and the coordinator publishes for the live frontier and for coverage gaps — neither of which
    /// describes a sealed day that already has coverage. Recovery already sees these files and
    /// skipped them silently, which is how 352 of them accumulated across 26 days unnoticed.
    #[tokio::test]
    async fn recovery_queues_a_rebuild_for_a_partition_holding_untagged_tier_files() -> Result<()> {
        use crate::maintenance_coordinator::{Operation, TaskState};
        use deltalake::kernel::{
            Action,
            transaction::{CommitBuilder, TableReference},
        };
        use object_store::ObjectStoreExt as _;
        let mut cfg = (*create_test_config("untagged-selfheal")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        db.cancel_maintenance();
        let project = format!("heal_{}", uuid::Uuid::new_v4().simple());
        let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
        let at = day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros();
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts("a", "op", &project, at)])?], true, None).await?;
        let key = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await.map(|report| report.date)?;
        assert_eq!(key, day);

        // Strip the identity from the published file, exactly as an OPTIMIZE does.
        let tier = get_schema("otel_logs_and_spans")
            .and_then(|schema| schema.rollups.iter().find(|spec| spec.derive_from.is_none()).map(|spec| spec.table_name("otel_logs_and_spans")))
            .expect("a base tier");
        let tier_ref = db.get_or_create_table(&project, &tier).await?;
        {
            let mut table = tier_ref.read().await.clone();
            let live: Vec<_> = table
                .snapshot()?
                .log_data()
                .iter()
                .map(|file| {
                    #[allow(deprecated)]
                    file.add_action()
                })
                .collect();
            let store = table.log_store().object_store(None);
            let mut actions = Vec::new();
            for add in live {
                let path = format!("{}-stripped.parquet", add.path.trim_end_matches(".parquet"));
                store.copy(&deltalake::Path::from(add.path.clone()), &deltalake::Path::from(path.clone())).await?;
                actions.push(Action::Add(deltalake::kernel::Add { path, tags: None, ..add }));
            }
            let op = deltalake::protocol::DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by: None, predicate: None };
            let finalized = CommitBuilder::default().with_actions(actions).build(Some(table.snapshot()? as &dyn TableReference), table.log_store(), op).await?;
            table.state = Some(finalized.snapshot());
            *tier_ref.write().await = table;
        }

        // Retire everything so only work THIS recovery creates is visible.
        {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            let keys: Vec<_> = journal.tasks().map(|task| task.key.clone()).collect();
            for key in &keys {
                journal.complete(key);
            }
        }
        db.recover_rollup_coverage("otel_logs_and_spans").await?;

        let queued: Vec<_> = {
            let journal = db.maintenance_tasks.lock().unwrap();
            journal
                .tasks()
                .filter(|task| task.key.project_id == project && task.state == TaskState::Pending && task.key.physical_table == tier)
                .map(|task| task.key.slice)
                .collect()
        };
        assert!(!queued.is_empty(), "a partition holding an untagged tier file must be queued for rebuild");
        // Bounded by the FILE, not the day. A day-wide unit is over
        // `MAX_DECODED_BYTES` for any real tenant, so the preflight shreds it
        // down the bisection ladder to one-minute slices — prod 2026-08-22 held
        // 3,455 units for a single (project, tier, day), 1,423 still pending,
        // and that cell had not converged in days.
        assert!(
            queued.iter().all(|slice| slice.width() < crate::maintenance_coordinator::DAY_MICROS),
            "the rebuild must target the untagged file's own span, got {queued:?}"
        );
        // And it must be the cell the untagged file is IN, not merely the same
        // project on some other day.
        assert!(
            queued.iter().all(|slice| chrono::DateTime::from_timestamp_micros(slice.start_micros).is_some_and(|time| time.date_naive() == day)),
            "the rebuild must land on the damaged partition's date"
        );

        // The retired COUNTER must equal what actually left the table.
        //
        // It used to be incremented where the replace-set is computed, ~80 lines
        // before the commit, so it counted INTENDED retirements. Prod 2026-08-22
        // read 21 retired within an hour while a fresh Delta-log replay said the
        // live count had not moved at all — two deploys had killed the units in
        // between. The same block also cleared the partition's hole rank, which
        // would have switched the prioritisation off on the cells that still
        // needed it.
        //
        // HONEST LIMIT: this asserts the happy path, where the commit lands, so
        // it would also pass against the old placement. The divergence needs a
        // unit abandoned between the replace-set and the commit (`still_running`
        // false, or `slice_occ_stale`), which there is no cheap way to induce
        // from here. What it does guard is double-counting and counting files
        // the replace-set selected but the commit did not remove.
        let untagged_live = |db: &Database| {
            let tier_ref = futures::executor::block_on(db.get_or_create_table(&project, &tier)).expect("tier");
            let table = futures::executor::block_on(tier_ref.read());
            table
                .snapshot()
                .expect("snapshot")
                .log_data()
                .iter()
                .filter(|file| {
                    #[allow(deprecated)]
                    let add = file.add_action();
                    add.tags.as_ref().is_none_or(|tags| !tags.contains_key(crate::maintenance_coordinator::TAG_SLICE_START))
                })
                .count()
        };
        let before = untagged_live(&db);
        assert!(before > 0, "the stripped copies must be live for this to prove anything");
        let counter = || crate::observability::maintenance_stats().rollup_tier_untagged_retired.load(std::sync::atomic::Ordering::Relaxed);
        let counted_before = counter();
        db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;
        assert_eq!(
            u64::try_from(before - untagged_live(&db)).unwrap_or_default(),
            counter() - counted_before,
            "the retired counter must equal the untagged files the commit actually removed"
        );
        Ok(())
    }

    /// Columns are IMMUTABLE by default; only declared ones are version-mutable.
    ///
    /// This decides whether a point lookup reaches the scan or is stranded above the merge-on-read
    /// dedup. Stranded, the engine materialises the ENTIRE window keep-greatest before applying the
    /// predicate: prod 2026-08-20, one `context___trace_id` lookup over 24h pulled 623 hot-tier
    /// files plus the Delta legs into a single partition at ~4.5 GB/s, took the instance from 84%
    /// to 93% of its cgroup limit in ~1.5s, and killed it (exit 137). Treating every non-key column
    /// as mutable withheld the pushdown from ~120 columns to protect the one that is UPDATEd.
    #[test]
    fn only_declared_and_version_bearing_columns_are_mutable() {
        let mutable = super::ProjectRoutingTable::version_mutable_columns("otel_logs_and_spans").expect("a version_append table");

        // Declared: monoscope's enrichment appends to it.
        // The columns production and clients actually assign.
        // The ONLY column production rewrites after write.
        assert!(mutable.contains("hashes"), "hashes is declared mutable and must stay above the dedup");
        // Mutable by construction, no declaration needed — `stamp_version`
        // rewrites the tiebreak on every append, and a delete appends a row
        // differing only in the tombstone.
        assert!(mutable.contains("updated_at"), "the version tiebreak differs across versions");
        assert!(mutable.contains("deleted"), "the tombstone differs across versions");

        // Everything else is immutable and therefore leg-safe — that is the win.
        for column in [
            "context___trace_id",
            "context___span_id",
            "kind",
            "parent_id",
            "timestamp",
            "id",
            "project_id",
            "date",
            "name",
            "level",
            "duration",
            "status_code",
            "status_message",
        ] {
            assert!(!mutable.contains(column), "{column} is immutable and its filter must be pushable below the dedup");
        }
    }

    /// An untagged tier file must not be immortal.
    ///
    /// The replace-set matched on slice tags alone, so a file that had lost them — a delta-rs
    /// OPTIMIZE keeps only its own `sort_by` tag — could never be removed by anything. Every
    /// rebuild then stacked another version of every `id` beside it and the tier's measures grew
    /// without bound; prod 2026-08-20 held 352 such files at 7.17 versions per id, and a repair
    /// rebuild published a correct 8,892-row file while the 22,505-row untagged file survived
    /// untouched. A day-wide publish reproduces the whole partition, so it must retire them.
    #[tokio::test]
    async fn a_day_wide_publish_retires_an_untagged_tier_file() -> Result<()> {
        use crate::maintenance_coordinator::{DAY_MICROS, MAX_DECODED_BYTES, Operation, TaskKey, TimeSlice};
        use deltalake::kernel::{
            Action,
            transaction::{CommitBuilder, TableReference},
        };
        use object_store::ObjectStoreExt as _;
        let mut cfg = (*create_test_config("untagged-immortal")).clone();
        cfg.maintenance.timefusion_rollup_enabled = true;
        cfg.maintenance.timefusion_rollup_backfill_days = 35;
        let db = Database::with_config(std::sync::Arc::new(cfg)).await?;
        let project = format!("untag_{}", uuid::Uuid::new_v4().simple());
        let day_start = midnight_micros((Utc::now() - chrono::Duration::days(3)).date_naive());
        let noon = day_start + DAY_MICROS / 2;
        for i in 0..3 {
            let span = test_span_ts(&format!("s{i}"), "op", &project, noon + i);
            db.insert_records_batch(&project, "otel_logs_and_spans", vec![json_to_batch(vec![span])?], true, None).await?;
        }

        // Publish the day at DAY width — the width this fix is scoped to, and
        // what `coarsen_sealed_slices` produces for a sealed day in prod.
        let day_unit = |db: &Database| -> Result<TaskKey> {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            let keys: Vec<_> = journal.tasks().map(|task| task.key.clone()).collect();
            for key in &keys {
                journal.complete(key);
            }
            let base = keys.into_iter().find(|key| key.operation == Operation::BaseRollup).expect("the write path queues a base rollup for the day");
            let key = TaskKey { slice: TimeSlice::new(day_start, day_start + DAY_MICROS)?, ..base };
            journal.enqueue(key.clone(), 0, MAX_DECODED_BYTES, 0);
            Ok(key)
        };
        let key = day_unit(&db)?;
        assert!(db.run_maintenance_units(1024).await? > 0, "the day-wide base unit must run");
        let tier_total = async |db: &Database| -> Result<i64> {
            let sql = format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM {} WHERE project_id = '{project}'", key.physical_table);
            Ok(db
                .query_delta_only(&sql)
                .await?
                .iter()
                .filter(|batch| batch.num_rows() > 0)
                .filter_map(|batch| batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().map(|column| column.value(0)))
                .next()
                .unwrap_or(0))
        };
        assert_eq!(tier_total(&db).await?, 3, "the first publish must count each span once");

        // Seed the damage exactly as OPTIMIZE produced it: the same bytes, live
        // in the same partition, carrying no identity tags.
        let tier_ref = db.get_or_create_table(&project, &key.physical_table).await?;
        {
            let mut table = tier_ref.read().await.clone();
            let live: Vec<_> = table
                .snapshot()?
                .log_data()
                .iter()
                .map(|file| {
                    #[allow(deprecated)]
                    file.add_action()
                })
                .collect();
            assert!(!live.is_empty(), "the publish must have written a file to strip");
            let store = table.log_store().object_store(None);
            let mut actions = Vec::new();
            for add in live {
                let path = format!("{}-untagged.parquet", add.path.trim_end_matches(".parquet"));
                store.copy(&deltalake::Path::from(add.path.clone()), &deltalake::Path::from(path.clone())).await?;
                actions.push(Action::Add(deltalake::kernel::Add { path, tags: None, ..add }));
            }
            let op = deltalake::protocol::DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by: None, predicate: None };
            let finalized = CommitBuilder::default().with_actions(actions).build(Some(table.snapshot()? as &dyn TableReference), table.log_store(), op).await?;
            table.state = Some(finalized.snapshot());
            *tier_ref.write().await = table;
        }
        // Count FILES, not summed measures. Reads collapse versions now that a
        // tier declares its identity, so a read-side sum would report the right
        // answer over a partition that is still carrying the damage — which is
        // exactly the confusion this whole area produced in the first place.
        let live_files = async |db: &Database| -> Result<usize> {
            let table = db.get_or_create_table(&project, &key.physical_table).await?;
            let table = table.read().await.clone();
            Ok(table.snapshot()?.log_data().iter().count())
        };
        assert_eq!(live_files(&db).await?, 2, "precondition: the untagged copy is live alongside the tagged one");
        assert_eq!(tier_total(&db).await?, 3, "read-time dedup must already hide the duplicate from queries");

        // A late row makes the day genuinely re-eligible, which is how a rebuild
        // reaches a damaged partition in production.
        insert_a_span(&db, &project, "s3", noon + 3).await?;
        day_unit(&db)?;
        assert!(db.run_maintenance_units(1024).await? > 0, "the rebuild must run");
        assert_eq!(live_files(&db).await?, 1, "a day-wide rebuild must RETIRE the untagged file, not stack a version beside it");
        assert_eq!(tier_total(&db).await?, 4, "and the rebuilt partition counts each span once");
        Ok(())
    }

    /// A hygiene task for a partition the scan proved compliant must be retired, and a partition
    /// the scan never saw must not be.
    ///
    /// File hygiene is stateless work stated as a durable queue. A partition absent from the snapshot
    /// is unknown, not clean; retiring its work would silently drop compaction.
    #[tokio::test]
    async fn compliant_partitions_retire_their_hygiene_tasks_but_unseen_ones_do_not() -> Result<()> {
        use crate::maintenance_coordinator::{Operation, TaskKey, TaskState, TimeSlice};
        let db = Database::with_config(create_test_config("retire-compliant")).await?;
        let project = format!("retire_{}", uuid::Uuid::new_v4().simple());
        // One well-formed partition: a single flushed day, hence compliant.
        let day = Utc::now() - chrono::Duration::days(3);
        insert_a_span(&db, &project, "a", day.timestamp_micros()).await?;

        let slice_for = |d: chrono::DateTime<Utc>| -> Result<TimeSlice> {
            let start = midnight_micros(d.date_naive());
            TimeSlice::new(start, start + DAY_MICROS)
        };
        let task_for = |d: chrono::DateTime<Utc>, operation| -> Result<TaskKey> {
            Ok(TaskKey {
                physical_table: "otel_logs_and_spans".to_owned(),
                source: "otel_logs_and_spans".to_owned(),
                project_id: project.clone(),
                slice: slice_for(d)?,
                operation,
            })
        };
        // The real staleness shape: a HotPacking task minted while the day was
        // TODAY, on a day that has since sealed. The scan sees that partition
        // and plans SealedConsolidation for it, never HotPacking — so the
        // HotPacking task can never be right again.
        let seen_key = task_for(day, Operation::HotPacking)?;
        // And a day with no data at all, which the scan never sees.
        let unseen_key = task_for(Utc::now() - chrono::Duration::days(400), Operation::SealedConsolidation)?;
        {
            let mut journal = db.maintenance_tasks.lock().unwrap();
            journal.enqueue(seen_key.clone(), 0, 1, 0);
            journal.enqueue(unseen_key.clone(), 0, 1, 0);
        }

        db.plan_compaction_debt().await?;

        let (seen_state, unseen_state) = {
            let journal = db.maintenance_tasks.lock().unwrap();
            (journal.state(&seen_key), journal.state(&unseen_key))
        };
        assert_eq!(seen_state, Some(TaskState::Complete), "a partition the scan proved compliant retires its stale task");
        assert_ne!(unseen_state, Some(TaskState::Complete), "a partition the scan never saw is unknown, not clean, and keeps its task");
        Ok(())
    }

    /// The reweighting must move slots to the rollup chain without starving anything. Each
    /// operation keeps a slot so file debt cannot drop to zero and let counts run away.
    #[test]
    fn the_coverage_short_cycle_favours_rollup_without_starving_file_work() {
        use crate::maintenance_coordinator::{CYCLE_BALANCED, CYCLE_COVERAGE_SHORT, Operation};
        let count = |cycle: &[Operation; 10], op: Operation| cycle.iter().filter(|candidate| **candidate == op).count();
        let chain = |cycle: &[Operation; 10]| count(cycle, Operation::BaseRollup) + count(cycle, Operation::DerivedRollup);

        assert_eq!(chain(&CYCLE_BALANCED), 4, "precondition: the balanced cycle gives the rollup chain 4 of 10");
        assert_eq!(chain(&CYCLE_COVERAGE_SHORT), 6, "the coverage-short cycle must give the rollup chain 6 of 10");
        // Nothing starves. Each of these has its own incident behind it.
        for op in [Operation::Dedup, Operation::HotPacking, Operation::SealedConsolidation, Operation::Repair] {
            assert!(count(&CYCLE_COVERAGE_SHORT, op) >= 1, "{op:?} must keep at least one slot");
        }
        assert_eq!(CYCLE_BALANCED.len(), CYCLE_COVERAGE_SHORT.len(), "same length, so the rotating cursor behaves identically");
    }

    /// The reweighting is self-limiting: it must switch off once coverage reaches
    /// the target, or it becomes a permanent tax on file hygiene.
    #[test]
    fn the_reweighting_switches_off_once_coverage_is_healthy() {
        use std::sync::atomic::Ordering::Relaxed;
        let gauge = &crate::observability::maintenance_stats().rollup_median_contiguous_days;
        let restore = gauge.load(Relaxed);
        gauge.store(0, Relaxed);
        assert!(coverage_is_short(), "zero contiguous days is short");
        gauge.store(COVERAGE_SHORT_DAYS - 1, Relaxed);
        assert!(coverage_is_short(), "one day below target is still short");
        gauge.store(COVERAGE_SHORT_DAYS, Relaxed);
        assert!(!coverage_is_short(), "at the target the balanced cycle returns");
        gauge.store(restore, Relaxed);
    }

    /// Which projects the coverage must hold for is decided by this test, and
    /// Coverage is intersected across projects, so a dormant tenant that wrote nothing in the window
    /// refuses the query for everyone. A single project with a tiny partition can dominate the miss
    /// profile once `missing_project` is fixed.
    #[test]
    fn a_partition_outside_the_window_does_not_join_the_coverage_requirement() {
        let stats = |min_ts, max_ts| PartitionStats { fingerprint: 0, min_ts, max_ts, rows: 0 };
        // Wholly before, wholly after: not in the window.
        assert!(!stats(0, 50).overlaps(100, 200));
        assert!(!stats(300, 400).overlaps(100, 200));
        // Half-open: a row exactly at `hi` is outside, one exactly at `lo` is in.
        assert!(!stats(200, 250).overlaps(100, 200));
        assert!(stats(100, 100).overlaps(100, 200));
        // Straddling either edge, and strictly containing, all overlap.
        assert!(stats(50, 150).overlaps(100, 200));
        assert!(stats(150, 250).overlaps(100, 200));
        assert!(stats(0, 1000).overlaps(100, 200));
        // No timestamp statistics: the sentinel range must be treated as
        // OVERLAPPING. Dropping a project on the strength of a statistic that is
        // not there is the direction that undercounts.
        assert!(stats(i64::MAX, i64::MIN).overlaps(100, 200));
    }

    /// The intersection is the whole safety argument for cross-project routing:
    /// a range one project has not covered must NOT be read from the rollup, or
    /// that project's rows are silently absent from the answer. An undercount is
    /// worse than a slow query, and nothing downstream can detect it.
    #[test]
    fn a_range_only_one_project_covers_is_not_in_the_intersection() {
        // Project A covered the whole day; project B only its first half.
        assert_eq!(intersect_ranges(&[(0, 100)], &[(0, 50)]), vec![(0, 50)]);
        // Disjoint coverage yields nothing at all — everything goes to the raw leg.
        assert_eq!(intersect_ranges(&[(0, 40)], &[(60, 100)]), Vec::new());
        // Touching but not overlapping is still nothing: the ranges are half-open.
        assert_eq!(intersect_ranges(&[(0, 50)], &[(50, 100)]), Vec::new());
        // A hole in one side splits the other's single range in two.
        assert_eq!(intersect_ranges(&[(0, 100)], &[(0, 30), (70, 100)]), vec![(0, 30), (70, 100)]);
        // One project is the identity case: coverage passes through untouched.
        assert_eq!(intersect_ranges(&[(10, 20), (30, 40)], &[(10, 20), (30, 40)]), vec![(10, 20), (30, 40)]);
        // Neither side advances past a range the other still overlaps.
        assert_eq!(intersect_ranges(&[(0, 100)], &[(10, 20), (30, 40), (90, 200)]), vec![(10, 20), (30, 40), (90, 100)]);
    }

    #[test]
    fn tantivy_backfill_prioritizes_recent_partitions() {
        let mut uris = vec![
            "project_id=p/date=2024-01-01/old.parquet".to_string(),
            "project_id=p/date=2026-08-16/new-b.parquet".to_string(),
            "project_id=p/date=2025-06-10/middle.parquet".to_string(),
            "project_id=p/date=2026-08-16/new-a.parquet".to_string(),
        ];
        super::sort_backfill_uris_newest_first(&mut uris);

        assert!(uris[0].contains("date=2026-08-16") && uris[1].contains("date=2026-08-16"));
        assert!(uris[2].contains("date=2025-06-10"));
        assert!(uris[3].contains("date=2024-01-01"));
    }

    /// The tail must be REACHABLE, which pure newest-first plus a cap makes
    /// impossible: prod held `week`/`older` frozen at 1723/3459 across three
    /// census samples while `today` climbed, because today's 624 uncovered files
    /// exceeded the 150-file cap on their own. Here the hot partition is 10 files
    /// against a cap of 4 — newest-first alone can never emit the old one.
    #[test]
    fn tantivy_backfill_reserves_a_share_of_the_pass_for_the_oldest() {
        let hot = (0..30).map(|i| (format!("rel/2026-08-22-{i:02}"), format!("uri/2026-08-22-{i:02}")));
        let cold = std::iter::once(("rel/2026-01-01".to_string(), "uri/2026-01-01".to_string()));
        let queues = || vec![("p".to_string(), hot.clone().chain(cold.clone()).collect::<VecDeque<_>>())];
        let uris = |w: Vec<super::TantivyBackfillWork>| w.into_iter().map(|(_, _, uri)| uri).collect::<Vec<_>>();

        let starved = uris(super::fair_tantivy_backfill_work_split(queues(), 12, 0));
        assert!(!starved.contains(&"uri/2026-01-01".to_string()), "pure newest-first must starve the tail — otherwise this test proves nothing");

        let split = uris(super::fair_tantivy_backfill_work_split(queues(), 12, 33));
        assert_eq!(split.len(), 12, "the reservation is carved OUT of the cap, so pass cost is unchanged");
        assert!(split.contains(&"uri/2026-01-01".to_string()), "the oldest uncovered file must be reachable, got {split:?}");
        assert!(split.contains(&"uri/2026-08-22-29".to_string()), "the hot window must still converge");
        // Reaching the tail LAST is the same starvation one level down: a pass is
        // routinely killed part-way, so a reservation at the end never executes.
        // Measured in prod 2026-08-22 — 33 min into a live pass, `older` had not
        // moved because the run was still on the newest files.
        let oldest_at = split.iter().position(|u| u == "uri/2026-01-01").expect("present");
        assert!(oldest_at < split.len() / 2, "the reserved tail must be interleaved, not appended — it was at {oldest_at} of {}", split.len());
        assert_eq!(split.iter().collect::<HashSet<_>>().len(), split.len(), "a file must not be scheduled twice in one pass: {split:?}");
    }

    #[test]
    fn tantivy_backfill_rotates_projects_before_their_history() {
        let queue = |items: &[&str]| items.iter().map(|item| (format!("rel/{item}"), format!("uri/{item}"))).collect::<VecDeque<_>>();
        let work = super::fair_tantivy_backfill_work(vec![
            ("whale".to_string(), queue(&["whale-new", "whale-mid", "whale-old"])),
            ("small".to_string(), queue(&["small-new"])),
        ]);
        let scheduled: Vec<_> = work.iter().map(|(project, _, uri)| (project.as_str(), uri.as_str())).collect();

        assert_eq!(scheduled, vec![("small", "uri/small-new"), ("whale", "uri/whale-new"), ("whale", "uri/whale-mid"), ("whale", "uri/whale-old"),]);
    }

    /// Truncating a bounded pass must cut the OLDEST round, never one project's
    /// whole queue — that is the property that lets the reconcile run hourly
    /// without a big tenant's history starving everyone else's recent files.
    #[test]
    fn a_bounded_backfill_pass_truncates_the_oldest_round_not_one_project() {
        let queue = |items: &[&str]| items.iter().map(|item| (format!("rel/{item}"), format!("uri/{item}"))).collect::<VecDeque<_>>();
        let mut work = super::fair_tantivy_backfill_work(vec![
            ("whale".to_string(), queue(&["whale-new", "whale-mid", "whale-old"])),
            ("small".to_string(), queue(&["small-new", "small-old"])),
        ]);
        let deferred = work.len().saturating_sub(3);
        work.truncate(3);
        let scheduled: Vec<_> = work.iter().map(|(project, _, uri)| (project.as_str(), uri.as_str())).collect();

        assert_eq!(deferred, 2, "the cap must report what it left behind, not swallow it");
        // Round 0 is both projects' newest; round 1 begins before anything older.
        assert_eq!(scheduled, vec![("small", "uri/small-new"), ("whale", "uri/whale-new"), ("small", "uri/small-old")]);
        assert!(scheduled.iter().any(|(p, _)| *p == "small"), "the smaller project keeps its slot under a cap");
    }

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

    /// Query-session sizing: decode buffers cost `batch_size × row width` per partition per
    /// concurrent query and are invisible to the memory pool, so the bound has to be the config
    /// (large batch sizes for wide rows caused multi-gigabyte decoder allocations); and a rollup
    /// build is a background rewrite and must be planned like one — it goes through
    /// `query_delta_only`, which built its session like an interactive query's, so
    /// `target_partitions` was the CPU quota rather than `MAINTENANCE_MAX_PARTITIONS`.
    #[tokio::test]
    async fn query_session_sizing() -> Result<()> {
        let config = create_test_config("query-session-sizing");
        let query_partitions = config.memory.timefusion_query_partitions;
        let db = Database::with_config(config).await?;
        let exec = &Arc::new(db.clone()).create_session_context().state().config().options().execution.clone();
        assert_eq!(exec.batch_size, 2048, "wide otel rows: one 8192-row batch measured 63MB");

        let partitions = |db: Database| Arc::new(db).create_session_context().state().config().options().execution.target_partitions;
        let query = partitions(db.clone());
        let mut maintenance = db.clone();
        maintenance.maintenance_scan = true;
        let scan = partitions(maintenance);
        assert_eq!(scan, super::MAINTENANCE_MAX_PARTITIONS, "a background rewrite must plan with maintenance parallelism");
        // In prod `timefusion_query_partitions` is the autotuned CPU quota (48);
        // a test config may leave it 0, meaning DataFusion's own core-count
        // default. Either way the maintenance scan must be strictly smaller.
        assert!(scan < query, "the whole point is fewer concurrent decoders: {scan} vs {query} (configured {query_partitions})");
        Ok(())
    }

    /// Production accepted the health-check socket but missed five auth-read
    /// deadlines because coordinator futures and PGWire shared Tokio workers.
    /// A non-yielding maintenance poll must no longer prevent foreground work
    /// from being scheduled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn maintenance_cpu_work_cannot_starve_the_foreground_runtime() -> Result<()> {
        let db = Database::with_config(create_test_config("maintenance-runtime-isolation")).await?.start_maintenance_schedulers().await?;
        let executor = db.maintenance_executor.get().expect("scheduler installs the isolated runtime").clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        executor.spawn(async move {
            let _ = started_tx.send(());
            let deadline = std::time::Instant::now() + std::time::Duration::from_millis(400);
            let mut value = 1_u64;
            while std::time::Instant::now() < deadline {
                value = std::hint::black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
            }
        });
        started_rx.await.expect("maintenance hog started");

        tokio::time::timeout(std::time::Duration::from_millis(150), async {
            for _ in 0..5 {
                tokio::task::yield_now().await;
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("foreground executor was starved by maintenance");
        db.cancel_maintenance();
        Ok(())
    }

    /// Reconciliation runs after preload and must be metadata-only over the
    /// handles preload published. If it cold-resolves a declared source, it can
    /// replay a large Delta log ahead of foreground ingest and recreate the
    /// production startup stall while health checks remain green.
    #[tokio::test]
    async fn maintenance_reconciliation_never_cold_loads_a_source() -> Result<()> {
        let db = Database::with_config(create_test_config("reconcile-cached-tables-only")).await?;

        assert!(db.unified_tables.read().await.is_empty());
        assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0);
        assert!(db.unified_tables.read().await.is_empty(), "reconciliation must not resolve a source table");
        Ok(())
    }

    /// The first cursor is a baseline, not a request to turn the full retained
    /// history into urgent startup work. Historical debt is discovered by the
    /// fair planner after foreground service has settled.
    #[tokio::test]
    async fn maintenance_reconciliation_baselines_a_new_source_cursor() -> Result<()> {
        let db = Database::with_config(create_test_config("reconcile-first-cursor-baseline")).await?;
        let table = db.get_or_create_unified_table("otel_logs_and_spans").await?;
        let version = table.read().await.version().unwrap_or_default();
        let cursor_key = ":otel_logs_and_spans";
        assert_eq!(db.maintenance_tasks.lock().unwrap().source_cursor(cursor_key), None);

        assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0);
        assert_eq!(db.maintenance_tasks.lock().unwrap().source_cursor(cursor_key), Some(version));
        Ok(())
    }

    /// Reconciling a missed commit must invalidate only the hours the commit's
    /// files actually span, never the whole partition-day. The old form
    /// enqueued `ALL_HOURS` per changed partition, and unified-table flush
    /// Reconcile must enqueue only the hours a missed commit touched.
    ///
    /// Previously the reconcile re-enqueued durable tasks for every active project and reset the
    /// day's completed frontier on every restart, inflating the queue by thousands of tasks. This
    /// test's commit touches one hour, so the reconcile should add only the write path's own
    /// one-hour invalidation.
    #[tokio::test]
    async fn reconcile_enqueues_only_the_hours_a_missed_commit_touched() -> Result<()> {
        let db = Database::with_config(create_test_config("reconcile-precise-hours")).await?;
        // The reconcile inspects cached handles only, so the table must exist
        // before the cursor baselines.
        db.get_or_create_unified_table("otel_logs_and_spans").await?;
        assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0, "first reconcile baselines the cursor");

        let project = format!("recon_{}", uuid::Uuid::new_v4().simple());
        let ts = (Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
        let day = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive();
        let day_start = midnight_micros(day);
        let hour_start = ts.div_euclid(3_600_000_000) * 3_600_000_000;
        for id in ["a", "b"] {
            insert_a_span(&db, &project, id, ts).await?;
        }
        let queued = db.reconcile_maintenance_task_cursors().await?;

        let journal = db.maintenance_tasks.lock().unwrap();
        let tasks = journal.tasks().filter(|task| task.key.project_id == project).collect::<Vec<_>>();
        assert_eq!(tasks.len(), 13, "one touched hour: 6 dedup + 6 base + 1 derived, not 312 for the whole day");
        assert!(
            tasks.iter().all(|task| task.key.slice.start_micros >= hour_start && task.key.slice.end_micros <= hour_start + 3_600_000_000),
            "every reconciled task must lie inside the touched hour; day {day} starts {day_start}"
        );
        assert_eq!(queued, 2, "one dirty hour x two rollup specs");
        Ok(())
    }

    /// A tenant's first write must not manufacture a full day of empty and future maintenance debt.
    ///
    /// File hygiene is planned by debt in `plan_compaction_debt` (one day-wide unit per project, only
    /// when the partition really has small or unsorted files), never per slice. Minting it per slice
    /// made the journal track ingest rather than fragmentation.
    #[tokio::test]
    async fn first_rollup_invalidation_enqueues_only_the_touched_hour() -> Result<()> {
        use crate::maintenance_coordinator::Operation;

        let db = Database::with_config(create_test_config("first-rollup-invalidation-is-sparse")).await?;
        db.invalidate_rollup_hours("customer-a", "otel_logs_and_spans", "2026-08-16", 1 << 7)?;

        let journal = db.maintenance_tasks.lock().unwrap();
        let counts = journal.tasks().fold(HashMap::<Operation, usize>::new(), |mut counts, task| {
            *counts.entry(task.key.operation).or_default() += 1;
            counts
        });
        assert_eq!(counts.get(&Operation::Dedup), Some(&6));
        assert_eq!(counts.get(&Operation::BaseRollup), Some(&6));
        assert_eq!(counts.get(&Operation::DerivedRollup), Some(&1));
        assert_eq!(counts.get(&Operation::HotPacking), None, "ingest must not mint file-hygiene work; the debt planner owns it");
        assert_eq!(journal.tasks().count(), 13, "one touched hour, not 456 full-day tasks");
        Ok(())
    }

    #[test]
    fn maintenance_reconciliation_extracts_only_the_changed_partition() {
        let values = HashMap::from([("project_id".to_owned(), Some("customer-a".to_owned())), ("date".to_owned(), Some("2026-08-16".to_owned()))]);
        assert_eq!(
            Database::maintenance_partition_from_action("ignored.parquet", Some(&values), "default"),
            Some(("customer-a".to_owned(), "2026-08-16".to_owned()))
        );

        // Older writers may omit partitionValues on Remove. The file path is
        // still sufficient, so a delete cannot advance the cursor without
        // invalidating the affected slice.
        assert_eq!(
            Database::maintenance_partition_from_action("project_id=customer-b/date=2026-08-15/part-000.parquet", None, "default"),
            Some(("customer-b".to_owned(), "2026-08-15".to_owned()))
        );

        assert_eq!(
            Database::maintenance_partition_from_action("date=2026-08-14/part-000.parquet", None, "default"),
            Some(("default".to_owned(), "2026-08-14".to_owned()))
        );
        assert_eq!(Database::maintenance_partition_from_action("part-000.parquet", None, "default"), None);

        // `default_project` is the fallback for a custom-project table's storage
        // project, not always the literal string "default".
        assert_eq!(
            Database::maintenance_partition_from_action("date=2026-08-14/part-000.parquet", None, "customer-c"),
            Some(("customer-c".to_owned(), "2026-08-14".to_owned()))
        );
    }

    /// Today is swept on every tick; only the sealed tail rotates.
    ///
    /// The 35-day certification window makes the work list ~36 dates x projects,
    /// and first-time passes over never-certified days truncate ticks often. A
    /// whole-list rotation would then visit today once per full rotation, which
    /// silently stops certifying the partition the hot queries actually read —
    /// with nothing failing anywhere near the change.
    #[test]
    fn sweep_rotation_never_rotates_today_out() {
        // [today_a, today_b, sealed0, sealed1, sealed2, sealed3]
        let sealed_from = 2;
        for cursor in 0..12 {
            let mut work = vec![100, 101, 0, 1, 2, 3];
            super::rotate_sealed_tail(&mut work, sealed_from, cursor);
            assert_eq!(&work[..2], &[100, 101], "today must stay at the front at cursor {cursor}");
            let mut tail = work[2..].to_vec();
            tail.sort_unstable();
            assert_eq!(tail, vec![0, 1, 2, 3], "rotation must preserve the sealed set at cursor {cursor}");
            assert_eq!(work[2], cursor % 4, "the sealed tail must resume at the cursor");
        }
        // Degenerate shapes must not panic.
        let mut only_today = vec![1, 2];
        super::rotate_sealed_tail(&mut only_today, 5, 3);
        assert_eq!(only_today, vec![1, 2]);
        let mut empty: Vec<i32> = vec![];
        super::rotate_sealed_tail(&mut empty, 0, 7);
        assert!(empty.is_empty());
    }

    /// Maintenance admission is bounded by the configured job count, and every token is returned
    /// when a job finishes.
    ///
    /// This used to assert exactly one serial job, which prevented concurrent object-store streams
    /// but also left the queue deep and uncertified. The invariant worth pinning is bounded and
    /// returned admission, not serial admission.
    #[tokio::test]
    async fn database_bounds_concurrent_maintenance_jobs() -> Result<()> {
        use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Resources};

        let db = Database::with_config(create_test_config("bounded-maintenance-jobs")).await?;
        let jobs = db.config.derived.coordinator_jobs();
        let request = Resources { cpu: 1, decoded_bytes: MAX_DECODED_BYTES, object_reads: 1, object_writes: 1 };

        let permits: Vec<_> =
            (0..jobs).map(|i| db.maintenance_admission.try_acquire(request).unwrap_or_else(|| panic!("job {i} of {jobs} must be admitted"))).collect();
        assert!(db.maintenance_admission.try_acquire(request).is_none(), "admission must stop at the configured job count, not run unbounded");
        drop(permits);
        assert!(db.maintenance_admission.try_acquire(request).is_some(), "dropping the jobs returns every admission token");
        Ok(())
    }

    /// A maintenance scan must also be CHARGED to the maintenance pool.
    ///
    /// Parallelism bounds the decode buffers; the pool is what bounds the
    /// aggregate. The maintenance env carries a spill directory, so a whole-day
    /// GROUP BY spills to disk instead of holding every group in memory, and a
    /// background rewrite stops competing with interactive queries for the query
    /// pool. Certification — the other half of a backfill unit — already used
    /// this env, so the two halves were being charged to different budgets.
    #[tokio::test]
    async fn a_maintenance_scan_is_charged_to_the_maintenance_pool() -> Result<()> {
        let db = Database::with_config(create_test_config("maintenance-scan-pool")).await?;
        let pool_of = |db: Database| Arc::new(db).create_session_context().task_ctx().runtime_env().memory_pool.clone();

        let mut maintenance = db.clone();
        maintenance.maintenance_scan = true;
        let (query_pool, maintenance_pool) = (pool_of(db.clone()), pool_of(maintenance));
        assert!(!Arc::ptr_eq(&query_pool, &maintenance_pool), "a background rewrite must not draw on the query pool");
        assert!(Arc::ptr_eq(&maintenance_pool, &db.maintenance_runtime_env().memory_pool), "it must draw on the maintenance pool");
        Ok(())
    }

    /// The optimize/dedup session must carry a bounded batch size and a sort spill reservation so
    /// the Z-order external sort spills instead of failing with "Resources exhausted".
    ///
    /// The byte cap does not bind: parquet checks it against `get_estimated_total_bytes()`, which
    /// under-reports the finished row group. The derived row cap is what actually bounds row groups,
    /// and it must land far below parquet's own default.
    #[test]
    fn row_group_row_count_binds_far_below_the_parquet_default() {
        const PARQUET_DEFAULT: usize = 1024 * 1024;
        let otel = crate::schema::get_schema("otel_logs_and_spans").expect("otel schema");
        let rows = super::row_group_row_count(otel, 128 * 1024 * 1024);
        assert!(rows < PARQUET_DEFAULT / 4, "otel row groups must shrink at least 4x, got {rows}");
        assert!(rows >= 32_768, "floor keeps footer overhead from dominating, got {rows}");

        // Monotonic in the operator-facing byte target, so raising the target
        // raises the value that actually binds.
        assert!(super::row_group_row_count(otel, 256 * 1024 * 1024) > rows);
        // ...and it can never exceed the default it replaces, whatever the
        // target — a huge target must not silently restore 1M-row row groups.
        assert_eq!(super::row_group_row_count(otel, usize::MAX / 2), PARQUET_DEFAULT);
    }

    /// The repair sort runs 16 partitions, so its unspillable merge is 8x the
    /// 2-partition maintenance default. Reserving that share up-front is what
    /// forces the sorter to spill instead of consuming the pool and stranding the merge — the
    /// exact failure mode that quarantined a blocking repair file.
    #[test]
    fn repair_session_reserves_merge_memory_up_front() {
        let env = Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default());
        let repair = build_optimize_session_state_tuned(
            0,
            Arc::clone(&env),
            Some("256"),
            Some(UncappedSort { partitions: REPAIR_SORT_PARTITIONS, reservation_bytes: Some(REPAIR_SORT_RESERVATION_BYTES) }),
        );
        let exec = &repair.config().options().execution;
        assert_eq!(exec.sort_spill_reservation_bytes, REPAIR_SORT_RESERVATION_BYTES);
        assert_eq!(exec.target_partitions, REPAIR_SORT_PARTITIONS, "repair runs one bin at a time, so it is not capped at 2");

        // The reservation must scale WITH the partition count, or raising
        // partitions silently re-inflates the unspillable merge share.
        assert!(
            exec.sort_spill_reservation_bytes * exec.target_partitions >= 4 * 1024 * 1024 * 1024,
            "16 partitions need >=4 GB reserved; measured merge was 9.5 GB"
        );

        // Ordinary maintenance is untouched: it runs many concurrent bins, where
        // a large per-partition reservation is what exhausts the bounded pool.
        let plain = build_optimize_session_state_tuned(0, env, None, None);
        assert_eq!(plain.config().options().execution.sort_spill_reservation_bytes, 33_554_432);
    }

    /// Packing sorts at more than `MAINTENANCE_MAX_PARTITIONS`, but must NOT
    /// take repair's inflated per-partition reservation with it.
    ///
    /// The cap is sized for the heavy fan-out (8 sibling sorters on one pool);
    /// packing runs at most `k` bins in a pool nothing else touches, and paying
    /// the cap anyway sorted 2-way on a 48-core box. The reservation is the
    /// safety rail on that: it is UNSPILLABLE and per-partition, so pairing
    /// raised partitions with repair's 256 MB would reserve more of packing's
    /// pool than the bins themselves use.
    #[tokio::test]
    async fn packing_sorts_wider_than_the_heavy_cap_but_keeps_the_default_reservation() -> Result<()> {
        let db = Database::with_config(create_test_config("pack-partitions")).await?;
        let exec = db.light_optimize_session_state().config().options().execution.clone();
        assert_eq!(exec.sort_spill_reservation_bytes, 33_554_432, "repair's reservation must not leak onto the packing path");
        assert_eq!(exec.target_partitions, db.pack_sort_partitions(), "the session must use the derived width, not the shared cap");
        // The rail that keeps the split honest on ANY box: reservations stay a
        // small fraction of the pool the bins themselves have to fit in.
        let reserved = exec.sort_spill_reservation_bytes * exec.target_partitions * db.config.derived.max_light_optimize_k().max(1);
        assert!(reserved * 4 <= db.pack_pool_bytes().max(4), "reservations claim {reserved} of a {} byte pack pool", db.pack_pool_bytes());
        Ok(())
    }

    /// Prod's box (48 cores, a 7.5 GB pack pool at k=3) must actually LIFT the
    /// heavy cap — that cap is sized for 8 sibling sorters on one shared pool,
    /// and paying it anyway sorted packing 2-way on 48 cores. A small box must
    /// fall back to it instead, because the per-partition reservation is
    /// unspillable and would eat the pool before any row was sorted.
    #[test]
    fn pack_sort_width_follows_the_pool_and_the_box_not_a_pinned_constant() {
        const GIB: usize = 1024 * 1024 * 1024;
        // Prod: memory affords 20, CPU (48/2/3) binds at 8.
        assert_eq!(super::pack_sort_partitions(7 * GIB + GIB / 2, 3, 48), 8);
        assert_eq!(super::pack_sort_partitions(341 * 1024 * 1024, 1, 8), MAINTENANCE_MAX_PARTITIONS, "a small pool must fall back to the cap, never below it");
        assert_eq!(super::pack_sort_partitions(64 * GIB, 1, 8), 4, "cores bound the width even when memory is abundant");

        // Neither budget may be exceeded, across two orders of magnitude of box.
        for (pool_gib, k, cores) in [(1usize, 1usize, 4usize), (2, 2, 8), (8, 3, 16), (16, 3, 48), (64, 4, 96)] {
            let parts = super::pack_sort_partitions(pool_gib * GIB, k, cores);
            assert!(parts >= MAINTENANCE_MAX_PARTITIONS, "the derivation must only ever lift the cap");
            let at_cap = parts == MAINTENANCE_MAX_PARTITIONS;
            assert!(at_cap || parts * k * 2 <= cores, "packing may take at most half the box across its {k} bins");
            assert!(at_cap || parts * k * 4 * 33_554_432 <= pool_gib * GIB, "reservations may take at most a quarter of the pack pool");
        }
    }

    /// A day the FINE tier already has but the COARSE tier does not must still
    /// be queued — and queued for the coarse tier ALONE.
    ///
    /// The single set this replaced subtracted each tier in turn with
    /// The old code used set union, so a day missing only the coarse tier was removed by the fine
    /// tier's pass and never enqueued. A day missing only a derived tier must be queued for that tier
    /// alone, because the derived tier reads the base tier and needs no raw source scan.
    #[test]
    fn a_day_missing_only_the_coarse_tier_is_queued_for_that_tier_alone() {
        let day = |n: u32| chrono::NaiveDate::from_ymd_opt(2026, 8, n).expect("date");
        let candidates: Vec<(String, chrono::NaiveDate)> = (14..=16).map(|d| ("p".to_owned(), day(d))).collect();
        let set = |days: &[u32]| days.iter().map(|d| ("p".to_owned(), day(*d))).collect::<HashSet<_>>();
        // Tier 0 (fine) has all three days; tier 1 (coarse) has only 08-16.
        let covered = vec![(0usize, set(&[14, 15, 16])), (1usize, set(&[16]))];

        let missing = tiers_missing_per_day(&candidates, &covered);
        assert_eq!(missing.get(&("p".to_owned(), day(16))), None, "a day both tiers have needs nothing");
        assert_eq!(
            missing.get(&("p".to_owned(), day(15))).map(Vec::as_slice),
            Some([1usize].as_slice()),
            "a day the fine tier has but the coarse tier lacks must be queued for the COARSE tier only — \
             queueing the fine tier too re-reads the whole raw partition to rebuild a rollup that exists"
        );
        assert_eq!(missing.get(&("p".to_owned(), day(14))).map(Vec::as_slice), Some([1usize].as_slice()));
        assert_eq!(missing.len(), 2, "only the days actually missing a tier");
    }

    /// The metric exists because the two we had both read as progress while
    /// 14d/30d queries were unroutable. It must report the CONTIGUOUS run, and
    /// must be dragged down by the worst project — but a day the SOURCE never
    /// held must still count as answered (else the gauge is unreachable by
    /// construction: the goal is "30 contiguous days a 30d panel can answer
    /// from the tier", not "30 days every tenant happened to send traffic").
    #[test]
    fn contiguous_coverage_ignores_days_stranded_behind_a_hole() {
        let today = chrono::NaiveDate::from_ymd_opt(2026, 8, 17).expect("date");
        let day = |n: u32| chrono::NaiveDate::from_ymd_opt(2026, 8, n).expect("date");
        let covered = |pairs: &[(&str, u32)]| pairs.iter().map(|(p, d)| ((*p).to_owned(), day(*d))).collect::<HashSet<_>>();
        let active = |names: &[&'static str]| names.iter().copied().collect::<HashSet<&str>>();
        // Every day in the window held source rows, so an absent day in `covered`
        // is a genuine hole rather than a day with nothing to roll up.
        let dense = |names: &[&str]| names.iter().flat_map(|p| (1u32..=31).map(move |d| ((*p).to_owned(), day(d)))).collect::<HashSet<_>>();

        // 08-16, 08-15, 08-14 unbroken back from yesterday.
        assert_eq!(min_contiguous_days(&covered(&[("a", 16), ("a", 15), ("a", 14)]), &dense(&["a"]), today, &active(&["a"])).0, 3);

        // Prod's actual shape: plenty of days, but a hole right after yesterday.
        // Everything behind it is unreachable to a 30d panel, so it must not count.
        assert_eq!(min_contiguous_days(&covered(&[("a", 16), ("a", 13), ("a", 12), ("a", 11), ("a", 10)]), &dense(&["a"]), today, &active(&["a"])).0, 1);

        // Today is the frontier's job — covering it must not extend the run.
        assert_eq!(
            min_contiguous_days(&covered(&[("a", 17)]), &dense(&["a"]), today, &active(&["a"])).0,
            0,
            "yesterday missing is zero coverage however complete today is"
        );

        // One starved project drags the fleet number down; averaging would hide it.
        assert_eq!(
            min_contiguous_days(&covered(&[("a", 16), ("a", 15), ("b", 16)]), &dense(&["a", "b"]), today, &active(&["a", "b"])).0,
            1,
            "the worst project is the number that matters"
        );
        assert_eq!(min_contiguous_days(&HashSet::new(), &dense(&["a"]), today, &active(&["a"])).0, 0);

        // A project that stopped ingesting can never have yesterday. Counting it
        // pinned the gauge at 0 forever on prod (`edb04135`, last wrote 08-03)
        // while every project anyone queries was fully covered.
        assert_eq!(
            min_contiguous_days(&covered(&[("a", 16), ("a", 15), ("dormant", 3)]), &dense(&["a", "dormant"]), today, &active(&["a"])).0,
            2,
            "a project that no longer ingests must not pin the fleet number at zero"
        );

        // A day the source never held must count as answered, or the gauge is
        // unreachable by construction: the goal is "30 contiguous days a 30d
        // panel can answer from the tier", not "30 days on which every tenant
        // happened to send traffic".
        let today2 = chrono::NaiveDate::from_ymd_opt(2026, 8, 18).expect("date");
        let day2 = |n: u32| chrono::NaiveDate::from_ymd_opt(2026, 8, n).expect("date");
        let set = |pairs: &[(&str, u32)]| pairs.iter().map(|(p, d)| ((*p).to_owned(), day2(*d))).collect::<HashSet<_>>();
        // 4f020cf8's real shape: source on 14/16/18 only, tier matching exactly.
        let source = set(&[("q", 14), ("q", 16), ("q", 18)]);
        let covered_q = set(&[("q", 14), ("q", 16), ("q", 18)]);
        assert_eq!(
            min_contiguous_days(&covered_q, &source, today2, &active(&["q"])).0,
            CONTIGUITY_HORIZON_DAYS,
            "17 and 15 had no rows to roll up, and neither did anything older — fully answered"
        );
        // A day the source DID hold and the tier does not is still a real hole.
        let source = set(&[("q", 17), ("q", 16), ("q", 15)]);
        let covered_q = set(&[("q", 17), ("q", 15)]);
        assert_eq!(min_contiguous_days(&covered_q, &source, today2, &active(&["q"])).0, 1, "16 has rows and no tier — a genuine gap");
        // A project that emitted nothing at all in the window is fully answered
        // rather than scoring zero.
        assert_eq!(min_contiguous_days(&HashSet::new(), &HashSet::new(), today2, &active(&["silent"])).0, CONTIGUITY_HORIZON_DAYS);
    }

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

    /// Parquet decode is outside every DataFusion pool, so its heap was never measured. A single
    /// wide scan's decode can exceed the box's whole slack. `GatedScanExec`'s permit window is the
    /// decode window, so it can account decoded Arrow bytes there. Accounting only: it must never
    /// refuse a batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn gated_scan_exec_accounts_decoded_bytes() {
        use arrow::array::Int32Array;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use std::sync::atomic::Ordering::Relaxed;
        crate::observability::init_local_metrics_for_test();
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
        assert_eq!(crate::observability::counter_value(scan_metric_names::DECODE_BYTES_TOTAL), want * 2, "both decoded batches must be accounted");
        assert_eq!(metrics.decode.decode_peak_batch_bytes.load(Relaxed), want);
        assert_eq!(metrics.decode.decode_polls_inflight.load(Relaxed), 0, "in-flight gauge must return to zero");
        assert_eq!(metrics.decode.decode_polls_inflight_peak.load(Relaxed), 1, "one partition polled serially = peak 1");
    }

    /// The decode pressure valve. ABSOLUTE backstop tiers: full concurrency until 88% of the
    /// cgroup limit, quarter pool to 95%, fully serialized past that — claims never exceed the
    /// pool (progress guaranteed) and never drop to zero. RATE-based tiers are the whole point of
    /// the rework: a box PARKED at the old first tier's percentage must not throttle, and a box
    /// RUSHING at the wall from well below it must — 70%/82% could not express that, and flapped
    /// 52 times in 25 minutes on a working set that simply sat at 69-70% (135648b).
    const CALM: u64 = u64::MAX; // not projected to reach the limit at all
    #[test_case(0, CALM, 16 => 1 ; "well under the backstop")]
    #[test_case(87, CALM, 16 => 1 ; "just under the 88% backstop")]
    #[test_case(88, CALM, 16 => 4 ; "88% backstop trips to quarter pool")]
    #[test_case(94, CALM, 16 => 4 ; "still quarter pool just under 95%")]
    #[test_case(95, CALM, 16 => 16 ; "95% backstop fully serializes")]
    #[test_case(200, CALM, 16 => 16 ; "over 100% still fully serializes")]
    #[test_case(88, CALM, 2 => 1 ; "tiny pools floor at 1")]
    #[test_case(95, CALM, 1 => 1 ; "single-permit pool floors at 1")]
    #[test_case(70, u64::MAX, 16 => 1 ; "a steady working set parked at 70% must never be throttled")]
    #[test_case(18, 65, 16 => 1 ; "a cold process filling fast from near-empty must not engage the valve")]
    #[test_case(49, 10, 16 => 1 ; "below the floor, even a tiny projection is not evidence")]
    #[test_case(70, 60, 16 => 4 ; "a burst projected to hit the limit within a minute engages the valve well before 88%")]
    #[test_case(70, 10, 16 => 16 ; "seconds left at only 70% used still serializes")]
    #[test_case(96, u64::MAX, 16 => 16 ; "a burst too fast to project still trips on level alone")]
    fn pressure_permit_claim_tiers(usage_pct: u64, eta_secs: u64, total: u32) -> u32 {
        pressure_permit_claim_at(usage_pct, eta_secs, total)
    }

    /// The hot tier's own reads must not read as memory pressure.
    ///
    /// Charging `active_file` as pressure put the brake near the budget and starved compaction. Only
    /// `anon` + kernel is unreclaimable; file cache is the tier being read, and the kernel drops it
    /// rather than kill the process.
    #[test]
    fn clean_page_cache_is_not_charged_as_memory_pressure() {
        const PROD: &str = "anon 54533181440\nfile 35626905600\nkernel 1233899520\nfile_mapped 526012416\n\
                            file_dirty 99794944\nfile_writeback 0\ninactive_file 14998753280\nactive_file 20628152320\n";
        let current: usize = 91_423_989_760;
        let limit: usize = 85_899_345_920;

        let reclaimable = super::reclaimable_file_bytes(PROD).expect("`file` is present");
        assert_eq!(reclaimable, 35_626_905_600 - 99_794_944, "dirty pages stay charged; every other file page does not");

        let charged = current - reclaimable;
        assert_eq!(charged * 100 / limit, 65, "prod read 88% while only 65% of the budget was unreclaimable");
        // The anon+kernel figure this is standing in for, within rounding.
        assert!(charged.abs_diff(54_533_181_440 + 1_233_899_520) < 200_000_000, "charged must track anon + kernel, not the page cache");

        assert_eq!(super::reclaimable_file_bytes("anon 1\n"), None, "no `file` line means charge everything");
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

    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cron_job_on_runs_body_on_the_selected_runtime() {
        let isolated =
            tokio::runtime::Builder::new_multi_thread().worker_threads(1).thread_name("cron-isolated").enable_all().build().expect("isolated runtime");
        let cancel = Arc::new(CancellationToken::new());
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_cron_job_on("isolated-test", "* * * * * *", cancel.clone(), Some(isolated.handle().clone()), move || {
            let tx = tx.clone();
            async move {
                let name = std::thread::current().name().unwrap_or("unnamed").to_owned();
                let _ = tx.send(name);
            }
        });

        let thread_name = tokio::time::timeout(std::time::Duration::from_millis(2500), rx.recv()).await.expect("cron fired").expect("sender alive");
        cancel.cancel();
        assert!(thread_name.starts_with("cron-isolated"), "job ran on {thread_name}, not the isolated executor");
        isolated.shutdown_background();
    }

    /// A wedged job body must not freeze the cron loop: later ticks are skipped
    /// (not queued) and the skip counter grows, so the schedule survives and the
    /// wedge is visible in `timefusion_stats`.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_cron_job_skips_ticks_while_previous_run_hangs() {
        use std::sync::atomic::Ordering::Relaxed;
        let cancel = Arc::new(CancellationToken::new());
        let skipped_before = crate::observability::maintenance_stats().cron_ticks_skipped.load(Relaxed);
        spawn_cron_job("hung-test", "* * * * * *", cancel.clone(), move || async move {
            std::future::pending::<()>().await; // never returns
        });
        // Generous window: needs >=2 wall-clock second boundaries even on a
        // loaded CI runner where task startup and sleep wakeups slip.
        tokio::time::sleep(std::time::Duration::from_millis(5200)).await;
        cancel.cancel();
        let skipped = crate::observability::maintenance_stats().cron_ticks_skipped.load(Relaxed) - skipped_before;
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
        let mut info = HashMap::new();
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

    /// Watermark JSON format invariants, all sharing one build-info/parse harness.
    /// Positions are per-`topic:shard` walrus offsets and NOT comparable across
    /// topics — applying a busy tenant's high block_id to a quiet tenant's cursor
    /// skips that tenant's unreplayed WAL entries (acked-write loss on every
    /// Delta-scan boot). Each block below pins one distinct edge case that a
    /// production incident or a format-compat requirement forced.
    #[test]
    fn watermark_json_format_invariants() {
        use walrus_rust::WalPosition;

        // topic scoping: only the topic that wrote a watermark may apply it — not
        // a co-tenant sharing the same physical log, not a different table, and a
        // legacy (pre-topic) commit contributes nothing rather than being applied
        // to everyone (unattributable is indistinguishable from another tenant's,
        // and duplicates read-side dedup removes are preferable to loss).
        let busy = serialize_watermark_to_json(&vec![Some(WalPosition { block_id: 9_000, offset: 0 })], "busy_proj", "otel_logs_and_spans");
        let mut info = HashMap::new();
        info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(busy));
        assert_eq!(parse_watermark_from_json(&info, 1, "busy_proj", "otel_logs_and_spans"), vec![Some(WalPosition { block_id: 9_000, offset: 0 })]);
        assert_eq!(parse_watermark_from_json(&info, 1, "quiet_proj", "otel_logs_and_spans"), vec![None], "co-tenant on the same log gets nothing");
        assert_eq!(parse_watermark_from_json(&info, 1, "busy_proj", "otel_metrics"), vec![None], "different table is a different topic");
        let legacy: serde_json::Map<String, serde_json::Value> =
            [("0".to_string(), serde_json::json!({ "block_id": 9_000, "offset": 0 }))].into_iter().collect();
        let mut old = HashMap::new();
        old.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(legacy));
        assert_eq!(parse_watermark_from_json(&old, 1, "any_proj", "otel_logs_and_spans"), vec![None], "topic-less legacy commit applies to no one");

        // all-None serializes to an empty object, which `build_watermark_commit_properties`
        // turns into a default `CommitProperties` (no metadata written) — recovery sees no
        // key and silently skips the commit, same path as pre-feature commits.
        let wm: crate::write::DeltaWatermark = vec![None, None, None];
        assert!(serialize_watermark_to_json(&wm, "p", "t").is_empty());
        let mut empty_info = HashMap::new();
        empty_info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(serde_json::Map::new()));
        assert!(parse_watermark_from_json(&empty_info, 3, "p", "t").iter().all(|p| p.is_none()));

        // per-shard MAX across commits: a shard's position is whichever commit observed the
        // furthest; a commit missing a shard (including a replay-derived one with no
        // watermark key at all) contributes nothing and must not reset the MAX.
        let mk_info = |entries: &[(usize, u64, u64)]| {
            let map: serde_json::Map<String, serde_json::Value> =
                entries.iter().map(|(s, b, o)| (s.to_string(), serde_json::json!({ "block_id": b, "offset": o }))).collect();
            let mut map = map;
            map.insert(WATERMARK_TOPIC_KEY.to_string(), serde_json::Value::String(wal_topic("p", "t")));
            let mut info = HashMap::new();
            info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(map));
            info
        };
        let a = mk_info(&[(0, 5, 100), (1, 5, 50)]);
        let b = mk_info(&[(0, 6, 0)]); // past A on shard 0; nothing for shard 1
        let c: HashMap<String, serde_json::Value> = HashMap::new(); // replay-derived, no watermark key
        let d = mk_info(&[(1, 5, 30)]); // behind A on shard 1; must lose to A
        let max = max_watermark_across_commits([&a, &b, &c, &d], 3, "p", "t");
        assert_eq!(max[0], Some(WalPosition { block_id: 6, offset: 0 }));
        assert_eq!(max[1], Some(WalPosition { block_id: 5, offset: 50 }));
        assert_eq!(max[2], None, "shard 2 unwritten by all commits stays None");

        // coalesced commits (C3 crash-recovery invariant): ONE commit carries every
        // included project's watermark, and each project resumes from ITS OWN position —
        // never a co-tenant's, and a project absent from the commit gets nothing.
        let t = "otel_logs_and_spans";
        let ca = vec![Some(WalPosition { block_id: 900, offset: 10 }), None];
        let cb = vec![None, Some(WalPosition { block_id: 4, offset: 7 })];
        let cc = vec![Some(WalPosition { block_id: 1, offset: 1 }), Some(WalPosition { block_id: 2, offset: 2 })];
        let json = serialize_watermarks_to_json([
            ("proj_a".to_string(), t.to_string(), ca.clone()),
            ("proj_b".to_string(), t.to_string(), cb.clone()),
            ("proj_c".to_string(), t.to_string(), cc.clone()),
        ]);
        let mut coalesced = HashMap::new();
        coalesced.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(json));
        assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_a", t), ca, "proj_a resumes from its own position");
        assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_b", t), cb, "proj_b resumes from its own position");
        assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_c", t), cc, "proj_c resumes from its own position");
        assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_d", t), vec![None, None], "project absent from the commit gets nothing");
        assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_a", "otel_metrics"), vec![None, None]);
        let mut solo = HashMap::new();
        solo.insert(
            WAL_WATERMARK_KEY.to_string(),
            serde_json::Value::Object(serialize_watermark_to_json(&vec![Some(WalPosition { block_id: 901, offset: 0 }), None], "proj_a", t)),
        );
        // MAX across a coalesced + a per-project commit still resolves per project.
        assert_eq!(max_watermark_across_commits([&coalesced, &solo], 2, "proj_a", t), vec![Some(WalPosition { block_id: 901, offset: 0 }), None]);
        assert_eq!(max_watermark_across_commits([&coalesced, &solo], 2, "proj_b", t), cb, "proj_b unaffected by proj_a's later solo commit");

        // a one-project "coalesced" commit must serialize to the EXACT legacy flat shape —
        // the non-coalesced path and older binaries reading these commits see no format change.
        let single_wm = vec![Some(WalPosition { block_id: 3, offset: 4 }), None];
        assert_eq!(
            serialize_watermarks_to_json([("p".to_string(), "t".to_string(), single_wm.clone())]),
            serialize_watermark_to_json(&single_wm, "p", "t"),
            "single-topic coalesced commits must keep the flat legacy shape byte-for-byte"
        );
        assert!(serialize_watermarks_to_json([("p".to_string(), "t".to_string(), vec![None, None])]).is_empty());
        let one = serialize_watermarks_to_json([("p".to_string(), "t".to_string(), single_wm.clone()), ("q".to_string(), "t".to_string(), vec![None, None])]);
        assert_eq!(one, serialize_watermark_to_json(&single_wm, "p", "t"), "one populated topic of two collapses to the flat form");

        // two units for the SAME topic in one commit (shouldn't happen — the flush layer
        // coalesces per (project, table) first — but must never silently drop one): the
        // survivor takes the per-shard MAX.
        let dup_json = serialize_watermarks_to_json([
            ("p".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 5, offset: 100 }), Some(WalPosition { block_id: 1, offset: 0 })]),
            ("p".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 5, offset: 40 }), Some(WalPosition { block_id: 9, offset: 0 })]),
            ("q".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 2, offset: 0 })]),
        ]);
        let mut dup_info = HashMap::new();
        dup_info.insert(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(dup_json));
        assert_eq!(
            parse_watermark_from_json(&dup_info, 2, "p", "t"),
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
        let mut info = HashMap::new();
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
        use HashSet;
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
        assert_eq!(super::select_tail_bin(&unsealed, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack), Vec::<String>::new(), "unsealed files leave < min_files");

        // Converged (>= 7/8 target) files are never re-selected: rewriting one
        // alone is a 1→1 rewrite forever.
        let converged = vec![f("big", 900, false, 1, 2), f("a", 10, false, 3, 4), f("b", 10, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&converged, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack), vec!["a", "b"]);

        // REPAIR: an oversized file that is NOT a sorted run declares no
        // `sorting_columns`, and one of those disables the reader's
        // all-or-nothing footer ordering for every scan touching the partition.
        // Nothing else rewrites it — hot-tail used to skip it as converged, and
        // the daily crons rarely survive this process's restart cadence. So it
        // is repaired, but only in the GAPS: while a project still has a
        // packable slice, that slice wins.
        let poisoned = vec![f("big_unsorted", 900, false, 1, 2), f("a", 10, false, 3, 4), f("b", 10, false, 5, 6)];
        assert_eq!(
            super::select_tail_bin(&poisoned, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack),
            vec!["a", "b"],
            "normal packing must not be starved by a pending repair"
        );
        let only_poison = vec![f("big_unsorted", 900, false, 1, 2)];
        assert_eq!(
            super::select_tail_bin(&only_poison, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack),
            vec!["big_unsorted"],
            "with no packable slice left, the tick repairs one oversized unsorted file — alone, and below min_files (the gap rule: today's poisoned file still gets repaired even though `TailPass::Repair` excludes today)"
        );
        // ...and exactly one per bin, so a backlog drains across ticks instead
        // of rewriting several hundred-megabyte files in a single pass.
        let many_poison = vec![f("p1", 900, false, 1, 2), f("p2", 950, false, 3, 4), f("p3", 800, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&many_poison, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack).len(), 1, "one repair per bin");
        // A converged file that IS a sorted run stays done — repairing it would
        // be a 1->1 rewrite forever.
        let healthy = vec![f("big_sorted", 900, true, 1, 2)];
        assert!(super::select_tail_bin(&healthy, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack).is_empty(), "a converged sorted run is never re-selected");

        // Sorted runs fold only while under the cap; an over-cap run is excluded.
        let runs = vec![f("run_small", 100, true, 1, 2), f("run_big", 300, true, 3, 4), f("a", 10, false, 5, 6)];
        assert_eq!(super::select_tail_bin(&runs, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack), vec!["run_small", "a"], "only the sub-cap run folds");

        // Earliest contiguous slice up to cap, ordered by EVENT time (input
        // order is deliberately scrambled) — this is what makes runs disjoint.
        let pack = vec![f("third", 600, false, 30, 31), f("first", 600, false, 10, 11), f("second", 300, false, 20, 21)];
        assert_eq!(super::select_tail_bin(&pack, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack), vec!["first", "second"], "packs earliest slice, stops at cap");

        // min_files gate.
        assert_eq!(
            super::select_tail_bin(&[f("a", 10, false, 1, 2), f("b", 10, false, 3, 4)], TARGET, 3, TARGET / 4, SEAL, TailPass::Pack),
            Vec::<String>::new()
        );

        // A lone over-cap-adjacent file must not wedge the pass: selection skips
        // past it to the next slice rather than returning a 1-file bin.
        let lone = vec![f("lone", 800, false, 1, 2), f("a", 300, false, 10, 11), f("b", 300, false, 12, 13)];
        assert_eq!(super::select_tail_bin(&lone, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack), vec!["a", "b"]);

        // Files with no event-time stats can't be binned disjointly (the
        // 2026-07-20 silent no-op read them as None and selected NOTHING).
        let no_stats = vec![super::TailAdd { path: "x".into(), size: 10, is_sorted_run: false, event_range: None }, f("a", 10, false, 1, 2)];
        assert_eq!(super::select_tail_bin(&no_stats, TARGET, 2, TARGET / 4, SEAL, TailPass::Pack), Vec::<String>::new());
    }

    #[test]
    fn coordinator_compaction_sorts_small_l0_units_before_merging_sorted_runs() {
        const MB: i64 = 1024 * 1024;
        let f = |path: &str, size_mb: i64, sorted: bool| super::TailAdd { path: path.into(), size: size_mb * MB, is_sorted_run: sorted, event_range: None };

        let mixed = vec![f("l0-a", 20, false), f("sorted", 10, true), f("l0-b", 20, false)];
        assert_eq!(super::select_coordinator_compaction_candidates(mixed, 256 * MB), vec!["l0-a"]);

        let small_l0 = vec![f("a", 8, false), f("b", 8, false), f("c", 8, false), f("d", 8, false)];
        assert_eq!(super::select_coordinator_compaction_candidates(small_l0, 256 * MB), vec!["a", "b"]);

        let runs = vec![f("a", 100, true), f("b", 100, true), f("c", 100, true)];
        assert_eq!(super::select_coordinator_compaction_candidates(runs, 256 * MB), vec!["a", "b"]);
        assert!(super::select_coordinator_compaction_candidates(vec![f("done", 100, true)], 256 * MB).is_empty());

        assert_eq!(
            super::select_coordinator_compaction_candidates(vec![f("legacy", 40, false)], 256 * MB),
            vec!["legacy"],
            "an individually oversized L0 file remains progressable and is time-sliced by staging"
        );

        assert_eq!(super::coordinator_slice_target(TailPass::Pack, 1, 40 * MB), Some(16 * MB));
        assert_eq!(super::coordinator_slice_target(TailPass::Pack, 2, 40 * MB), None);
        assert_eq!(super::coordinator_slice_target(TailPass::Repair, 1, 40 * MB), Some(super::REPAIR_SLICE_TARGET_BYTES));
    }

    /// A packing pass must never rewrite a file that is already at or above target.
    ///
    /// The converged-file skip used to sit behind `!has_unsorted`, and `is_sorted_run` reads a tag
    /// only the OPTIMIZE path writes — 1,593 of 1,648 prod adds carry no tags at all. So
    /// `has_unsorted` was true for effectively every partition and the skip never ran: a partition
    /// admitted for holding two small files went on to select its half-gigabyte converged ones.
    ///
    /// Prod 2026-08-19: whale/2026-07-30 (103 files, 53.5 GB, already 0.52 GB/file) took 11 of 20
    /// SealedConsolidation starts in 25 minutes at 4.44 GB estimated per unit, exceeded its 900 s
    /// deadline every time, and was re-claimed with attempts climbing 1 → 7 — while 2026-08-13 sat
    /// at 167 files for six days and got zero starts. Sortedness belongs to Repair; this pass is
    /// size-only, exactly as `plan_compaction_debt`'s admission test already is.
    #[test]
    fn coordinator_packing_never_rewrites_a_converged_file() {
        const MB: i64 = 1024 * 1024;
        let f = |path: &str, size_mb: i64, sorted: bool| super::TailAdd { path: path.into(), size: size_mb * MB, is_sorted_run: sorted, event_range: None };

        // The whale/07-30 shape: converged files carrying no sorted-run tag, beside real small debt.
        let whale = vec![f("converged-a", 520, false), f("converged-b", 512, false), f("small-a", 6, false), f("small-b", 6, false)];
        assert_eq!(
            super::select_coordinator_compaction_candidates(whale, 256 * MB),
            vec!["small-a", "small-b"],
            "a packing unit takes the small files and leaves the converged ones alone"
        );

        // Nothing but converged untagged files is nothing to pack — the task retires as compliant.
        let done = vec![f("converged-a", 520, false), f("converged-b", 512, false)];
        assert!(
            super::select_coordinator_compaction_candidates(done, 256 * MB).is_empty(),
            "an already-packed partition must produce no work, whatever its tags say"
        );
    }

    /// Coordinator sealed units must fit their deadline and file-count contract.
    ///
    /// A target too large was repeatedly cancelled mid-rewrite and made zero durable progress. A
    /// physical run must still be at least 256 MiB to meet the sealed-file-count contract, so the unit
    /// is reduced to give the conservative rewrite floor enough time to commit it.
    #[test]
    fn coordinator_sealed_units_fit_their_deadline_and_file_count_contract() {
        const MIB: i64 = 1024 * 1024;
        assert_eq!(super::COORDINATOR_SEALED_TARGET_BYTES, 256 * MIB);
        assert_eq!(super::COORDINATOR_HOT_TARGET_BYTES, 256 * MIB);

        let estimated_rewrite_seconds = (super::COORDINATOR_SEALED_TARGET_BYTES + super::INPUT_BYTES_PER_SEC - 1) / super::INPUT_BYTES_PER_SEC;
        assert!(
            estimated_rewrite_seconds + 120 <= super::COORDINATOR_FILE_REWRITE_TIMEOUT.as_secs() as i64,
            "one sealed run needs {estimated_rewrite_seconds}s at the measured floor plus commit margin"
        );
        assert_eq!(
            super::coordinator_operation_timeout(crate::maintenance_coordinator::Operation::Dedup),
            super::COORDINATOR_STANDARD_UNIT_TIMEOUT,
            "a longer packing deadline must not widen dedup's fairness bound"
        );
        // Rollup builds DO get the longer deadline: their cost tracks the input
        // file count, which narrowing the slice cannot reduce while a sealed
        // day's files are still unsorted and each spans it (prod 2026-08-17 —
        // 828 files for a one-minute whale window). Dedup stays short because
        // it is the one that rewrites.
        for operation in [crate::maintenance_coordinator::Operation::BaseRollup, crate::maintenance_coordinator::Operation::DerivedRollup] {
            assert_eq!(
                super::coordinator_operation_timeout(operation),
                super::COORDINATOR_FILE_REWRITE_TIMEOUT,
                "{operation:?} must outlive the 300s bound a whale day cannot meet"
            );
        }
        assert_eq!(
            super::coordinator_operation_timeout(crate::maintenance_coordinator::Operation::SealedConsolidation),
            super::COORDINATOR_FILE_REWRITE_TIMEOUT
        );

        let run = |name: &str, mib: i64| super::TailAdd { path: name.into(), size: mib * MIB, is_sorted_run: true, event_range: None };
        assert_eq!(
            super::select_coordinator_compaction_candidates(vec![run("a", 128), run("b", 128), run("c", 128)], super::COORDINATOR_SEALED_TARGET_BYTES),
            vec!["a", "b"],
            "selection must stop at one independently committable physical run"
        );
    }

    /// A repair pass rewrites the newest poisoned file, not the oldest.
    ///
    /// The poison is all-or-nothing over a query window: the most recent footer-less date is the
    /// wall a user's time predicate hits, so repairing it is the only thing that widens the window.
    /// The old path took `fresh.first()` after an ascending sort — i.e. the oldest — which no query
    /// window reaches first and which is most likely to outrun the budget.
    #[test]
    fn repair_pass_takes_the_newest_poisoned_file_then_the_smallest() {
        const TARGET: i64 = 1000;
        const SEAL: i64 = 10_000;
        let f = |path: &str, size: i64, min: i64| super::TailAdd { path: path.into(), size, is_sorted_run: false, event_range: Some((min, min + 1)) };
        let backlog = vec![f("may", 900, 10), f("july", 900, 500), f("june", 950, 100)];
        assert_eq!(super::select_tail_bin(&backlog, TARGET, 2, TARGET / 4, SEAL, TailPass::Repair), vec!["july"], "newest poisoned file first");

        // Same date: take the smallest, so one un-finishable giant cannot
        // head-of-line block everything behind it.
        let same_day = vec![f("huge", 999, 500), f("small", 880, 500)];
        assert_eq!(super::select_tail_bin(&same_day, TARGET, 2, TARGET / 4, SEAL, TailPass::Repair), vec!["small"], "smallest first within a date");

        // A repair pass NEVER packs — it returns ONE file, never a bin, even
        // when several candidates could be packed together. Packing is the other
        // cron's job and a multi-file bin would spend the repair budget on it.
        let mixed = vec![f("poison", 900, 500), f("a", 10, 600), f("b", 10, 700)];
        assert_eq!(super::select_tail_bin(&mixed, TARGET, 2, TARGET / 4, SEAL, TailPass::Repair), vec!["b"], "one file, the newest");
        // A SMALL sealed footer-less file is repair work too. `hot_bin_admits`
        // has already established these are sealed and untagged, and one small
        // unsorted file voids its date's scan ordering exactly like a 1 GiB one
        // — `derive_common_ordering` is all-or-nothing and size-blind. Requiring
        // `size >= converged` here stranded them: Pack has no sealed dates in
        // scope, so nothing would ever have rewritten them.
        let small_only = vec![f("tiny_old", 10, 100), f("tiny_new", 10, 900)];
        assert_eq!(
            super::select_tail_bin(&small_only, TARGET, 2, TARGET / 4, SEAL, TailPass::Repair),
            vec!["tiny_new"],
            "a small sealed footer-less file is still repair work, newest first"
        );
        assert!(super::select_tail_bin(&[], TARGET, 2, TARGET / 4, SEAL, TailPass::Repair).is_empty(), "nothing admitted => no work");
    }

    /// Scope admission for the hot tail. Sealed-date repair exists because an unsorted file that
    /// survived midnight used to be unreachable forever, forcing full-set dedup on every query whose
    /// window crossed that date.
    #[test]
    fn hot_bin_admits_repairs_sealed_dates_without_rebinning_them() {
        const TARGET: i64 = 1000; // → converged at 7/8 = 875
        const REPAIR_MAX: i64 = 2000; // hot ticks repair up to here; larger is off-box CLI work
        let today = "date=2026-08-06/";
        let repair: Vec<String> = vec!["date=2026-08-05/".into()];
        let p = |d: &str| format!("timefusion/otel_logs_and_spans/project_id=abc/{d}part-x.parquet");
        let cleared: dashmap::DashSet<String> = dashmap::DashSet::new();
        let failures: dashmap::DashMap<String, u32> = dashmap::DashMap::new();
        let policy = |repair_dates: &'static [String]| super::HotBinPolicy {
            repair_dates,
            target_size: TARGET,
            min_files: 2,
            sorted_run_cap: TARGET / 2,
            repair_max_bytes: REPAIR_MAX,
            pass: TailPass::Pack,
            verified_sorted: &cleared,
            failures: &failures,
        };
        let with_repair = super::HotBinPolicy { repair_dates: &repair, ..policy(&[]) };
        let admits = |path: &str, size, sorted, repairable| super::hot_bin_admits(path, today, &repair, size, sorted, repairable, &with_repair);

        // TODAY — unchanged behaviour.
        assert!(admits(&p(today), 10, false, true), "small file today packs");
        assert!(!admits(&p(today), 900, true, true), "converged sorted run today is done");
        assert!(admits(&p(today), 900, false, true), "converged UNSORTED today is a repair candidate");

        // A candidate that keeps failing to stage is PARKED, so the queue behind
        // it drains. Prod 2026-08-09: `__HIVE_DEFAULT_PARTITION__`'s ~1.16 GB
        // file failed the pool on every pass and shipbubble's own blocking file
        // was never staged once in six hours.
        let sealed = p("date=2026-08-05/");
        assert!(admits(&sealed, 900, false, true), "sealed unsorted file is repair work");
        for n in 1..super::REPAIR_QUARANTINE_AFTER {
            failures.insert(sealed.clone(), n);
            assert!(admits(&sealed, 900, false, true), "still eligible after {n} failure(s) — transient failures must not cost eligibility");
        }
        failures.insert(sealed.clone(), super::REPAIR_QUARANTINE_AFTER);
        assert!(!admits(&sealed, 900, false, true), "parked once it has failed REPAIR_QUARANTINE_AFTER times");
        // Parking is per FILE, not per project or date — its neighbours stay eligible.
        assert!(admits(&p("date=2026-08-05/other-"), 900, false, true), "a different file on the same sealed date is unaffected");
        // A DETERMINISTIC failure (pool exhaustion) is worth the whole threshold:
        // the working set of a whole-file sort is a function of file size vs
        // pool, so it recurs every attempt. Measured 2026-08-09: at one repair
        // pass per process a 3-strike counter never reached 3, so the mechanism
        // could not fire at all.
        failures.clear();
        failures.insert(sealed.clone(), super::REPAIR_QUARANTINE_AFTER);
        assert!(!admits(&sealed, 900, false, true), "one deterministic failure parks it — a 3-strike rule never fires at the observed pass rate");
        failures.clear(); // the assertions below share these paths

        // SEALED, in the lookback window — repair only.
        let sealed = "date=2026-08-05/";
        assert!(admits(&p(sealed), 900, false, true), "the immortal file: converged + unsorted on a sealed date IS repaired");
        assert!(admits(&p(sealed), 10, false, true), "any unsorted file on a sealed date is repairable regardless of size");
        // The sort TAG no longer grants immunity. It records the optimizer's
        // INTENT and can disagree with the footer queries actually read — prod
        // 2026-08-08 had tagged files with `sorting_columns=()` pinning both
        // complaining tenants' 30-day queries, permanently invisible to repair.
        assert!(admits(&p(sealed), 10, true, true), "a tagged file is still only a SUSPECT; the footer decides");
        assert!(admits(&p(sealed), 900, true, true), "including a converged one");
        // Settled history still stays put — enforced by the footer read, which
        // records genuinely-sorted files so they are never offered again.
        cleared.insert(p(sealed));
        assert!(!admits(&p(sealed), 10, true, true), "a VERIFIED-sorted file is not re-binned");
        assert!(!admits(&p(sealed), 900, false, true), "verification outranks even an absent tag");
        cleared.remove(&p(sealed));
        assert!(!admits(&p(sealed), 900, false, false), "a table declaring no sort order has no footer to repair");
        // A legacy multi-GB file must NOT be dragged into a 5-minute tick; the
        // off-box CLI owns those. (Prod holds actives up to 2.34 GB.)
        assert!(!admits(&p(sealed), REPAIR_MAX + 1, false, true), "an oversized legacy file is left to the off-box optimize CLI");
        assert!(admits(&p(sealed), REPAIR_MAX, false, true), "exactly at the ceiling is still hot-repairable");

        // OUTSIDE the lookback window — untouched at any size or tag.
        let old = "date=2026-07-01/";
        assert!(!admits(&p(old), 10, false, true), "an unsorted file outside the lookback is out of scope");
        assert!(!admits(&p(old), 900, false, true));

        // Lookback 0 restores today-only behaviour.
        assert!(!super::hot_bin_admits(&p(sealed), today, &[], 900, false, true, &policy(&[])), "empty repair window == the old today-only pass");

        // A REPAIR pass owns sealed dates and nothing else. Letting today's
        // files in would spend a budget sized for one whole-file rewrite on
        // packing work the 5-minute cron already does.
        let repair_pass = super::HotBinPolicy { pass: TailPass::Repair, ..with_repair };
        let admits_repair = |path: &str, size, sorted| super::hot_bin_admits(path, today, &repair, size, sorted, true, &repair_pass);
        assert!(!admits_repair(&p(today), 10, false), "a repair pass never packs today");
        assert!(!admits_repair(&p(today), 900, false), "not even today's poisoned file — the Pack gap rule owns that");
        assert!(admits_repair(&p(sealed), 900, false), "sealed poisoned file is exactly its job");

        // The sort TAG is not the FOOTER. The flush path writes a correct
        // `sorting_columns` footer without stamping the tag, so an untagged file
        // is only a SUSPECT — prod 2026-08-08, project 6297304f: `2026-07-09` is
        // untagged with a sorted footer (716 MB), while `2026-07-30` and
        // `2026-07-08` are untagged and genuinely unsorted. Admitting on the tag
        // alone rewrites healthy files. Once a footer read clears a suspect it
        // must never be offered again.
        cleared.insert(p(sealed));
        assert!(!admits_repair(&p(sealed), 900, false), "a suspect whose footer read came back SORTED is out of the candidate set");
    }

    /// Wave commit blast radius: ONE stale bin must lose only its own actions.
    /// Naive batching would fail the whole wave (11 bins for one conflict).
    #[test]
    fn wave_drops_only_the_stale_bin() {
        let live: HashSet<String> = ["f1", "f2", "f4"].iter().map(|s| s.to_string()).collect();
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

    /// A snapshot that lists one file twice must still map to one target, so the planners'
    /// `targets.len() != files.len()` check stays meaningful. The incremental snapshot advance used to
    /// duplicate the file list across a checkpoint, making every plan mismatch and stalling compaction.
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

    #[test]
    fn committed_wave_adds_map_to_exact_live_tantivy_uris() {
        let mut alpha = staged_unit("alpha", &["old-a.parquet"], None);
        let mut beta = staged_unit("beta", &["old-b.parquet"], None);
        let alpha_rel = "project_id=alpha/date=2026-08-16/alpha-new.parquet";
        let beta_rel = "project_id=beta/date=2026-08-16/beta-new.parquet";
        if let deltalake::kernel::Action::Add(add) = &mut alpha.adds[0] {
            add.path = alpha_rel.into();
        }
        if let deltalake::kernel::Action::Add(add) = &mut beta.adds[0] {
            add.path = beta_rel.into();
        }
        let alpha_uri = format!("s3://bucket/table/{alpha_rel}");
        let beta_uri = format!("s3://bucket/table/{beta_rel}");
        let unrelated = "s3://bucket/table/project_id=gamma/date=2026-08-16/other.parquet".to_owned();

        assert_eq!(
            super::wave_added_parquet(&[alpha, beta], &[unrelated, beta_uri.clone(), alpha_uri.clone()]),
            vec![("alpha".into(), alpha_rel.into(), alpha_uri), ("beta".into(), beta_rel.into(), beta_uri)],
            "only this wave's live Adds become committed-file index jobs"
        );
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
        let live: HashSet<String> = ["f1", "f4"].iter().map(|s| s.to_string()).collect();
        let units = vec![
            staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6))),
            staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 5, 4))), // f2 gone
            staged_unit("gamma", &["f4"], Some(dedup_unit("2026-07-27", 8, 8))),
        ];
        let (fresh, stale) = super::split_live_bins(units, |b| &b.target_paths, &live);
        assert_eq!(fresh.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["alpha", "gamma"]);
        assert_eq!(stale.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["beta"]);
    }

    /// The self-landed split: "targets gone" has two causes that need opposite handling.
    ///
    /// Another writer may have rewritten the targets (staged parquet is garbage → delete), or our own
    /// earlier attempt landed and then errored (staged parquet is live data → never delete, credit the
    /// bin). The bin's own Adds are what tells them apart.
    #[test]
    fn a_bin_whose_own_adds_are_live_is_self_landed_not_stale() {
        let alpha = staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6)));
        let beta = staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 5, 4)));
        // alpha's commit LANDED: its target is gone AND its staged file is now
        // active. beta's target was rewritten by someone else.
        let live: HashSet<String> = ["alpha-new.parquet"].iter().map(|s| s.to_string()).collect();
        let (fresh, stale) = super::split_live_bins(vec![alpha, beta], |b| &b.target_paths, &live);
        assert!(fresh.is_empty(), "neither bin's targets survive");
        let (self_landed, stale): (Vec<_>, Vec<_>) = stale.into_iter().partition(|b| super::bin_adds_live(b, &live));
        assert_eq!(self_landed.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["alpha"], "a landed bin must never be discarded");
        assert_eq!(stale.iter().map(|b| b.project_id.as_str()).collect::<Vec<_>>(), vec!["beta"]);
        // Its rows really were dropped from the table, so they count.
        assert_eq!(super::wave_dropped_rows(&self_landed), 4);
        // And a bin whose targets ARE live is never mistaken for self-landed.
        let live_targets: HashSet<String> = ["f1"].iter().map(|s| s.to_string()).collect();
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
        let live: HashSet<String> = ["f1"].iter().map(|s| s.to_string()).collect();
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
        // Pre-resume lines carry neither resume field and stay cleanup-only.
        assert!(entries[0].target_paths.is_empty() && entries[0].adds.is_empty());
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
            target_paths: Vec::new(),
            adds: Vec::new(),
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
        let referenced: HashSet<String> = ["committed".to_string(), "unrelated".to_string()].into_iter().collect();
        assert_eq!(super::staged_orphan_deletions(&entries, "logs", 100_000, &referenced), vec!["orphan1", "orphan2"]);
        // Nothing to delete when every staged file landed.
        let all_live: HashSet<String> = ["committed", "orphan1", "orphan2"].iter().map(|s| s.to_string()).collect();
        assert!(super::staged_orphan_deletions(&entries, "logs", 100_000, &all_live).is_empty());
    }

    fn resume_add(path: &str, rows: i64) -> deltalake::kernel::Add {
        deltalake::kernel::Add { path: path.into(), size: 10, stats: Some(format!(r#"{{"numRecords":{rows}}}"#)), ..Default::default() }
    }

    fn resume_intent(targets: &[&str], adds: Vec<deltalake::kernel::Add>) -> super::StagedIntent {
        super::StagedIntent {
            wave_id: "w1".into(),
            table_name: "logs".into(),
            project_id: "p".into(),
            recorded_at: 100_000 - (super::STAGED_INTENT_MIN_AGE_SECS + 1),
            paths: adds.iter().map(|a| a.path.clone()).collect(),
            target_paths: targets.iter().map(|s| s.to_string()).collect(),
            adds,
        }
    }

    fn resume_live<'a>(files: &[(&'a str, Option<i64>)]) -> HashMap<&'a str, Option<i64>> {
        files.iter().copied().collect()
    }

    /// A staged repair only commits when EVERY input is still live and the rows
    /// add up. Everything else declines and leaves the work for reconcile —
    /// declining costs a re-stage, committing wrongly costs rows.
    #[test]
    fn resume_commits_only_a_row_preserving_rewrite_of_still_live_inputs() {
        use super::ResumeVerdict::*;
        let live = resume_live(&[("in1", Some(30)), ("in2", Some(70))]);
        let ok = resume_intent(&["in1", "in2"], vec![resume_add("out1", 100)]);
        assert_eq!(super::classify_resume(&ok, "logs", 100_000, &live), Commit);
        // Another table's entry is not this reconcile's to judge.
        assert_eq!(super::classify_resume(&ok, "metrics", 100_000, &live), Skip);
        // Young: a still-running instance on a shared volume may be mid-staging.
        let young = super::StagedIntent { recorded_at: 100_000 - 10, ..ok.clone() };
        assert_eq!(super::classify_resume(&young, "logs", 100_000, &live), Skip);
        // Pre-resume (and dedup) entries carry neither field: cleanup-only.
        let legacy = super::StagedIntent { target_paths: Vec::new(), adds: Vec::new(), ..ok.clone() };
        assert_eq!(super::classify_resume(&legacy, "logs", 100_000, &live), Skip);
        // An input was rewritten underneath us — the output is garbage.
        assert_eq!(super::classify_resume(&ok, "logs", 100_000, &resume_live(&[("in1", Some(30))])), Stale);
        // Truncated staging: the case that would silently drop 30 rows.
        let short = resume_intent(&["in1", "in2"], vec![resume_add("out1", 70)]);
        assert_eq!(super::classify_resume(&short, "logs", 100_000, &live), RowMismatch { target_rows: 100, staged_rows: 70 });
        // Unverifiable is not the same as verified: no stats ⇒ never commit.
        let no_stats = resume_intent(&["in1", "in2"], vec![deltalake::kernel::Add { path: "out1".into(), size: 10, ..Default::default() }]);
        assert_eq!(super::classify_resume(&no_stats, "logs", 100_000, &live), RowMismatch { target_rows: 100, staged_rows: -1 });
        assert_eq!(
            super::classify_resume(&ok, "logs", 100_000, &resume_live(&[("in1", None), ("in2", Some(70))])),
            RowMismatch { target_rows: -1, staged_rows: 100 }
        );
        // Staged parquet is uuid-named, so its presence in the snapshot can only
        // mean OUR commit landed before the crash — clear the intent, never
        // re-commit (re-committing would remove files the landed commit added).
        let landed = resume_intent(&["in1"], vec![resume_add("out1", 100)]);
        assert_eq!(super::classify_resume(&landed, "logs", 100_000, &resume_live(&[("out1", Some(100))])), AlreadyLanded);
    }

    /// The Add stats column list must be the narrow prune set, not the whole schema, and must never
    /// include partition columns.
    #[test]
    fn stats_columns_are_the_prune_keys_only() {
        let schema = schema_or_default("otel_logs_and_spans");
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
        assert_eq!(cols.len(), cols.iter().collect::<HashSet<_>>().len(), "no duplicates");
    }

    /// A repair pass must be bounded by its budget, not by a wave count.
    ///
    /// A wave serves one bin per project, so a flat wave cap could limit a pass to a handful of
    /// files per project regardless of how much long budget remained.
    #[test]
    fn repair_waves_are_bounded_by_budget_not_by_the_pack_cap() {
        assert_eq!(super::max_waves(super::TailPass::Pack), 12, "packing's short-cron cap must not change");
        // The whale's worst single date (07-24) holds 120 poisoned files; one
        // pass must be able to clear a date rather than 12 files of it.
        assert!(
            super::max_waves(super::TailPass::Repair) >= 120,
            "a repair pass capped below a single date's backlog can never clear that date, whatever its budget"
        );
    }

    /// Round-robin fairness: every project must get its Nth bin before any project gets its
    /// (N+1)th.
    ///
    /// The old per-project drain gave the most-fragmented project all bins before project #2 got
    /// its first, so on a short cron most projects were never compacted.
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
            false,
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

    /// A dedup sweep cut by its deadline must still reach every `(date, project)` item across
    /// ticks.
    ///
    /// Bounding it is only safe if the remainder is served next tick rather than dropped. A cursor
    /// that resets, or one applied without wrap, would re-serve the head forever and starve the tail.
    #[test]
    fn a_truncated_dedup_sweep_walks_the_whole_work_list_across_ticks() {
        let (total, per_tick) = (386usize, 40usize); // prod's ~193 projects x 2 dates
        let mut seen = vec![false; total];
        let mut cursor = 0usize;
        for _ in 0..total.div_ceil(per_tick) {
            let offset = super::sweep_resume_offset(total, cursor);
            for i in 0..per_tick {
                seen[(offset + i) % total] = true;
            }
            cursor += per_tick; // the cursor only ever grows, as `fetch_add(swept)` does
        }
        assert!(seen.iter().all(|s| *s), "a bounded sweep must still cover every partition; the tail was never served");
        // An empty list must not panic on the modulo.
        assert_eq!(super::sweep_resume_offset(0, 7), 0);
    }

    /// The window and its CONSUMER must agree: `dedup_window_clean` turns the
    /// window into dates via `window_dates` and asks certification about each
    /// one. A date-equality window must therefore resolve to EXACTLY one date.
    ///
    /// This is what the inclusive end buys. An exclusive-style end
    /// (`day_start + DAY`) would land on the next day's midnight, `window_dates`
    /// would return two dates, and the skip would be denied whenever the
    /// following date happened to be uncertified — silently, and only for some
    /// partitions, which is the worst way for it to be wrong.
    #[test]
    fn a_date_window_resolves_to_exactly_the_date_it_came_from() {
        use datafusion::prelude::{col, lit};
        for days in [0_i32, 1, 20_666, 25_000] {
            let (lo, hi) = super::date_partition_window(&[col("date").eq(lit(ScalarValue::Date32(Some(days))))]).expect("a window");
            let dates = super::window_dates(lo, hi).expect("resolvable");
            let expected = chrono::DateTime::from_timestamp_micros(lo).expect("in range").date_naive();
            assert_eq!(dates, vec![expected], "days={days} must ask about one date, got {dates:?}");
        }
    }

    /// The build SQL writes `date = '2026-08-01'` — a string literal against a `Date32` column.
    ///
    /// The predicate handling only works if DataFusion's type coercion turns the literal into a
    /// `Date32` value rather than casting the column to `Utf8`. Asserted against a real optimized plan.
    #[tokio::test]
    async fn the_builds_string_date_literal_survives_as_a_date32_window() {
        use datafusion::{
            arrow::datatypes::{DataType, Field, Schema},
            datasource::MemTable,
            logical_expr::LogicalPlan,
            prelude::SessionContext,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("project_id", DataType::Utf8, true), Field::new("date", DataType::Date32, false)]));
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())).unwrap();

        // Byte-for-byte the predicate `build_partition_sql` emits.
        let state = ctx.state();
        let plan = state.optimize(&state.create_logical_plan("SELECT * FROM t WHERE project_id = 'p' AND date = '2026-08-01'").await.unwrap()).unwrap();

        let mut filters = Vec::new();
        fn collect(plan: &LogicalPlan, out: &mut Vec<Expr>) {
            if let LogicalPlan::Filter(f) = plan {
                out.push(f.predicate.clone());
            }
            if let LogicalPlan::TableScan(scan) = plan {
                out.extend(scan.filters.iter().cloned());
            }
            plan.inputs().iter().for_each(|input| collect(input, out));
        }
        collect(&plan, &mut filters);
        // Split conjunctions, as the scan path does before extracting a window.
        let conjuncts: Vec<Expr> = filters.iter().flat_map(|f| datafusion::logical_expr::utils::split_conjunction(f).into_iter().cloned()).collect();

        let window = super::date_partition_window(&conjuncts);
        assert!(window.is_some(), "the optimized plan must still expose a Date32 equality; got {conjuncts:?}");
        // 2026-08-01 is 20666 days after the epoch.
        let days = chrono::NaiveDate::from_ymd_opt(2026, 8, 1).unwrap().signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
        assert_eq!(window, Some((days * 86_400_000_000, (days + 1) * 86_400_000_000 - 1)));
    }

    /// A rollup build filters on `project_id` and `date`, so the read-dedup skip must treat a date
    /// equality as a one-day window. Otherwise the skip is denied and the scan runs an unbounded
    /// merge-on-read dedup.
    ///
    /// The window must cover its day exactly. Reaching one microsecond into the next date would ask
    /// `dedup_window_clean` about an uncertified date and silently cost the skip.
    #[test]
    fn a_date_partition_equality_is_a_one_day_window() {
        use datafusion::prelude::{col, lit};
        const DAY: i64 = 86_400_000_000;
        let date = |days: i32| lit(ScalarValue::Date32(Some(days)));

        let (start, end) = super::date_partition_window(&[col("date").eq(date(20_000))]).expect("a date equality is a window");
        assert_eq!(start, 20_000 * DAY);
        assert_eq!(end, 20_001 * DAY - 1, "the window must stop one micro short of the next date");

        // Operands either way round, since the optimizer may swap them.
        assert_eq!(super::date_partition_window(&[date(20_000).eq(col("date"))]), Some((20_000 * DAY, 20_001 * DAY - 1)));
        // Beside the project filter a real rollup build carries.
        assert_eq!(super::date_partition_window(&[col("project_id").eq(lit("p")), col("date").eq(date(1))]), Some((DAY, 2 * DAY - 1)));
        // Anything that is not a date equality yields nothing, so the skip stays
        // denied rather than being granted over an unproven window.
        assert_eq!(super::date_partition_window(&[col("project_id").eq(lit("p"))]), None);
        assert_eq!(super::date_partition_window(&[col("date").gt(date(1))]), None);
        assert_eq!(super::date_partition_window(&[col("timestamp").eq(lit(1_i64))]), None);
    }

    /// A source whose partitions all fail expensively must not starve the sources behind it.
    ///
    /// Each source has its own tick budget, so bounding the budget cannot fix this; only rotation can.
    /// A failing source that consumes the whole run would otherwise leave tables behind it unserved.
    #[test]
    fn a_failing_source_does_not_starve_the_sources_behind_it() {
        let sources = ["otel_metrics", "otel_logs_and_spans", "otel_traces"];
        // One source per run is all a process lifetime affords when the first
        // one spends 695s of a 600s budget.
        let served: Vec<&str> = (0..sources.len())
            .map(|cursor| {
                let mut rotated = sources;
                let offset = super::sweep_resume_offset(rotated.len(), cursor);
                rotated.rotate_left(offset);
                rotated[0]
            })
            .collect();
        for source in sources {
            assert!(served.contains(&source), "`{source}` never got to run first across {} runs: {served:?}", sources.len());
        }
    }

    /// A repair bin that times out or panics must still clear its slot, or
    /// `repair_bins_in_flight` reads "busy" forever and the gauge stops being
    /// able to tell a grinding repair from a wedged one.
    #[test]
    fn an_in_flight_repair_slot_clears_even_when_the_bin_unwinds() {
        use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
        let counter = AtomicU64::new(0);
        {
            let _guard = super::in_flight_guard(&counter);
            assert_eq!(counter.load(Relaxed), 1, "the gauge must SHOW the sort while it runs");
        }
        assert_eq!(counter.load(Relaxed), 0);
        let _ = std::panic::catch_unwind(|| {
            let _guard = super::in_flight_guard(&counter);
            panic!("bin exploded");
        });
        assert_eq!(counter.load(Relaxed), 0, "a panicking bin left the slot held");
    }

    /// A finished repair bin must commit without waiting for its slowest sibling.
    ///
    /// Repair bins are minutes apart, so a round that collected every outcome before committing held
    /// finished rewrites hostage to the slowest sibling — and a restart mid-round discarded them all.
    /// The slow bin only finishes once the fast bin's commit has run, so a collect-then-commit
    /// implementation cannot finish this test at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_finished_repair_bin_commits_without_waiting_for_its_slow_sibling() {
        let (committed, gate) = (std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new())), std::sync::Arc::new(tokio::sync::Notify::new()));
        let (seen, release) = (committed.clone(), gate.clone());
        let run = super::round_robin_bins(
            vec!["fast".into(), "slow".into()],
            1,
            2,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_, _| {},
            || None,
            true,
            |project_id, _round| {
                let gate = gate.clone();
                async move {
                    if project_id == "slow" {
                        gate.notified().await;
                    }
                    (project_id.clone(), Ok(super::BinOutcome::Staged(project_id)))
                }
            },
            |bins: Vec<String>, _round| {
                let (seen, release) = (seen.clone(), release.clone());
                async move {
                    seen.lock().unwrap().push(bins);
                    release.notify_one();
                    0
                }
            },
        );
        let failed =
            tokio::time::timeout(std::time::Duration::from_secs(10), run).await.expect("a finished bin must commit while its sibling is still staging");
        assert_eq!(failed, 0);
        assert_eq!(*committed.lock().unwrap(), vec![vec!["fast".to_string()], vec!["slow".to_string()]], "one commit per bin, in completion order");
    }

    /// Packing bins are many and quick, and one commit per bin used to drive long OCC ladders.
    /// Packing must still land the whole wave in a single call.
    #[tokio::test(flavor = "multi_thread")]
    async fn packing_still_commits_the_whole_wave_in_one_call() {
        let committed = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
        let seen = committed.clone();
        let failed = super::round_robin_bins(
            vec!["a".into(), "b".into(), "c".into()],
            1,
            3,
            std::time::Instant::now() + std::time::Duration::from_secs(60),
            |_, _| {},
            || None,
            false,
            |project_id, _round| async move { (project_id.clone(), Ok(super::BinOutcome::Staged(project_id))) },
            |bins: Vec<String>, _round| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(bins);
                    0
                }
            },
        )
        .await;
        assert_eq!(failed, 0);
        let committed = committed.lock().unwrap();
        assert_eq!(committed.len(), 1, "packing commits ONCE per wave, got {} calls", committed.len());
        assert_eq!(committed[0].len(), 3, "all three bins in the one commit");
    }

    /// A dedup-raced bin (`Retry`) must not silence the project for the tick; it stays in rotation and
    /// gets a fresh bin next round.
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
            false,
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
            false,
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

    /// A hot-compaction sweep spends one tick budget across all tables and starts at a different
    /// table each tick.
    ///
    /// Each table used to derive its own full budget, so a sweep over many tables could run
    /// multiples of the schedule. The two properties are inseparable: a shared budget without rotation
    /// just moves the starvation from the tail of the tick to the tail of the table list.
    #[tokio::test]
    async fn a_hot_compact_sweep_shares_one_budget_and_rotates_its_starting_table() -> Result<()> {
        let db = Database::with_config(create_test_config(&format!("hotcompact-sweep-{}", uuid::Uuid::new_v4().simple()))).await?;
        for name in ["otel_logs_and_spans", "otel_metrics"] {
            db.get_or_create_unified_table(name).await?;
        }
        let tables = db.all_tables().await.len();
        assert!(tables > 1, "the sweep's whole point needs more than one table");

        // The budget is shared, so the sweep cannot take a multiple of it. This
        // is a no-op sweep (nothing to compact), so the real assertion is the
        // ROTATION below; this one guards the shape.
        let started = std::time::Instant::now();
        db.run_hot_compact_sweep(TailPass::Pack).await;
        assert!(started.elapsed() < db.tail_pass_tick_budget(TailPass::Pack), "the sweep must fit inside one tick budget, not one per table");

        // Consecutive ticks must start at different tables, or the tail of the
        // list is never compacted once the budget is genuinely contended.
        use std::sync::atomic::Ordering::Relaxed;
        let offset = || sweep_resume_offset(tables, db.hot_compact_table_cursor.load(Relaxed));
        let first = offset();
        db.run_hot_compact_sweep(TailPass::Pack).await;
        assert_ne!(first, offset(), "the sweep must rotate its starting table between ticks");
        Ok(())
    }

    /// The tick must stop starting rounds past its wall-clock budget rather than overrunning its
    /// own cron period.
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
            false,
            |project_id, _| async move { (project_id, Ok(super::BinOutcome::Staged(()))) },
            |_bins, _round| async { 0 },
        )
        .await;
        assert_eq!(failed, 0);
        assert_eq!(*truncated.lock().unwrap(), vec![(0, 2)], "must truncate on round 0 with both projects still pending");
    }

    /// A brake sampled only at round boundaries cannot bound memory allocated inside a round, so it
    /// must also gate admission of each bin within the round.
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
            false,
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

    /// A permanently engaged WAL brake must degrade rather than truncate.
    ///
    /// Truncating every tick at round 0 commits zero waves for hours. `Degrade` keeps a service floor
    /// by serving the head project serially; `Stop` keeps the old full-truncation behaviour.
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
                false,
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

    /// Cache keys are bucket-relative (inserts happen below the PrefixStore), so evict/contains must
    /// join the table's in-bucket path back onto the table-relative file path. Probing with the bare
    /// relative path never matches, so evictions become no-ops and confirms re-fetch.
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

    #[test]
    fn full_optimize_sorts_by_timestamp_when_enabled() {
        use deltalake::operations::optimize::OptimizeType;
        let schema = get_schema("otel_logs_and_spans").unwrap();
        let (optimize_type, declare_sorted) = full_optimize_type(schema, true);
        assert!(matches!(optimize_type, OptimizeType::SortBy(_)));
        assert!(declare_sorted);
    }

    #[test]
    fn consolidate_opportunistically_dedups_on_sorted_rewrite() {
        use deltalake::operations::optimize::OptimizeType;
        let schema = get_schema("otel_logs_and_spans").unwrap();
        let (optimize_type, declare_sorted) = consolidate_optimize_type(schema, true);
        let OptimizeType::SortByDedup(cols, dedup) = optimize_type else { panic!("expected SortByDedup") };
        assert_eq!(cols[0].column, "timestamp");
        assert_eq!(dedup.columns, vec!["timestamp", "id"]);
        let tb = dedup.tiebreak.expect("tiebreak from schema");
        assert!(tb.column == "updated_at" && tb.descending);
        assert!(declare_sorted);
        // Sort disabled → plain Compact, no opportunistic dedup.
        assert!(matches!(consolidate_optimize_type(schema, false), (OptimizeType::Compact, false)));
    }

    /// Opportunistic per-bin keep-greatest cannot certify convergence. Two
    /// versions can already live in separate target-sized sorted runs; neither
    /// run is ever selected again, so no number of normal consolidation passes
    /// brings those versions into the same dedup stream.
    #[test]
    fn converged_sorted_runs_are_a_counterexample_to_compaction_dedup_convergence() {
        const TARGET: i64 = 1000;
        let run = |path: &str, min: i64| super::TailAdd { path: path.into(), size: 900, is_sorted_run: true, event_range: Some((min, min + 1)) };
        let versions_in_different_runs = vec![run("older-version", 1), run("newer-version", 1)];

        assert!(
            super::select_tail_bin(&versions_in_different_runs, TARGET, 2, i64::MAX, 10_000, TailPass::Pack).is_empty(),
            "target-sized runs are terminal, even when their key/time domains overlap"
        );
    }

    /// A narrow slice of a day-spanning file must estimate a narrow share.
    ///
    /// The whole splitting mechanism assumes bisecting a unit halves its cost.
    /// Counting every overlapping file whole broke that assumption: a ten-minute
    /// slice of a day-spanning file estimated exactly what the day-wide slice
    /// did, so `split_time_task` bisected to `MIN_SLICE_MICROS` and produced
    /// ~144 units per (project, date) that each still claimed the whole day.
    /// Prod 2026-08-19: `pending_base_rollup = 85,503` flat, BaseRollup
    /// completing ~1.1 units/min, coarsening reporting `fused=0`.
    #[test]
    fn a_narrow_slice_estimates_a_narrow_share_of_a_day_spanning_file() {
        use crate::maintenance_coordinator::TimeSlice;
        const DAY: i64 = 86_400_000_000;
        const TEN_MIN: i64 = 600_000_000;
        let (file_min, file_max) = (0, DAY - 1);

        let day = TimeSlice::new(0, DAY).expect("day slice");
        assert_eq!(slice_share_of_file(Some(file_min), Some(file_max), day, 1), (DAY as u64, DAY as u64), "a day-wide slice takes the whole file");

        // One row group, so the pruning floor is the whole file and the share
        // cannot be finer — over-estimating is the safe direction.
        let narrow = TimeSlice::new(0, TEN_MIN).expect("ten-minute slice");
        assert_eq!(slice_share_of_file(Some(file_min), Some(file_max), narrow, 1), (DAY as u64, DAY as u64), "one row group cannot be pruned below");

        // A realistically-chunked file prunes: the share collapses to roughly
        // the slice's fraction of the day, which is what makes splitting work.
        let (share, whole) = slice_share_of_file(Some(file_min), Some(file_max), narrow, 144);
        assert!(share * 100 < whole, "a ten-minute slice must estimate well under 1% of a day-spanning file, got {share}/{whole}");

        // Unknown bounds keep full weight — unknown must never estimate cheap.
        assert_eq!(slice_share_of_file(None, None, narrow, 144), (1, 1));
        // A disjoint file contributes nothing.
        let after = TimeSlice::new(2 * DAY, 2 * DAY + TEN_MIN).expect("later slice");
        assert_eq!(slice_share_of_file(Some(file_min), Some(file_max), after, 144).0, 0);
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
        // Unique per RUN, not merely per test name. Keying storage on the name
        // alone means every run of a test reads the objects its previous runs
        // left behind, so results depend on history: on 2026-08-19 two tests
        // failed in a full suite, passed in isolation, and passed again in the
        // suite once `s3://timefusion-tests` was emptied. That is a false alarm
        // in the good case and a hidden regression in the bad one — and it cost
        // an hour chasing a subsume "regression" that did not exist.
        let unique = format!("{test_id}-{}", &uuid::Uuid::new_v4().to_string()[..8]);
        cfg.core.timefusion_table_prefix = format!("test-{unique}");
        cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-db-{unique}"));
        // Disable Foyer cache for tests
        cfg.cache.timefusion_foyer_disabled = true;
        Arc::new(cfg)
    }

    /// A repair rewrite must not buy a transient sort failure with a permanent read regression.
    ///
    /// Inputs are already committed and sorted, so replacing them with unsorted output voids the
    /// partition's declared ordering forever. Ingest keeps its fallback because its rows exist
    /// nowhere else. The slice target must split large files but leave small files unsliced, because
    /// each slice costs a scan of the input.
    #[test]
    fn a_repair_slice_splits_big_files_but_leaves_small_ones_alone() {
        const MB: i64 = 1024 * 1024;
        let slices = |input: i64| (input / super::REPAIR_SLICE_TARGET_BYTES).max(0) as usize + 1;
        // shipbubble's blocker must be split; it is the file this exists for.
        assert!(slices(1_088_634_971) >= 4, "the 1.04 GiB blocker must be sliced, got {}", slices(1_088_634_971));
        // ...but not shredded: every extra slice is another full scan.
        assert!(slices(1_088_634_971) <= 8, "too many slices turns the rewrite into re-scans, got {}", slices(1_088_634_971));
        assert_eq!(slices(30 * MB), 1, "a small file is sorted whole");
    }

    /// Slice bounds must TILE `[lo, hi]` — no gaps (a gap drops rows) and no
    /// overlaps (an overlap duplicates them, and puts two versions of one key in
    /// different files where keep-greatest can no longer see them together).
    /// A slice bound is the column's raw i64 micros, but the column is
    /// `Timestamp(Microsecond, Some("UTC"))`. DataFusion does NOT coerce a bare
    /// integer to a timestamp, so the untyped predicate failed the whole bin in
    /// `type_coercion` before reading a row — the sliced repair ran, logged
    /// `slices=5`, and died in 2 ms (prod 2026-08-09). The literal must carry the
    /// column's type, and `{:?}` on the Arrow type is `arrow_cast`'s syntax.
    #[tokio::test]
    async fn repair_slice_predicate_plans_against_a_tz_aware_timestamp() {
        use datafusion::{datasource::MemTable, prelude::SessionContext};
        let ty = arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new("timestamp", ty.clone(), false)]));
        let ctx = SessionContext::new();
        ctx.register_table("bin", Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("memtable"))).expect("register");

        // The type string the fix derives from the provider's schema.
        let rendered = format!("{ty:?}");
        assert_eq!(rendered, r#"Timestamp(Microsecond, Some("UTC"))"#, "arrow_cast parses this exact syntax");

        // `ctx.sql()` only builds the unanalyzed plan — coercion runs on the way
        // to a physical plan, which is where `execute_stream` takes it, so the
        // check has to go that far or it proves nothing.
        let plan = |sql: String| {
            let ctx = ctx.clone();
            async move { ctx.sql(&sql).await.expect("parses").create_physical_plan().await }
        };

        // The typed literal plans...
        plan(format!("SELECT * FROM bin WHERE \"timestamp\" >= arrow_cast(1753833600000000, '{rendered}')"))
            .await
            .expect("typed literal must plan against a tz-aware timestamp");

        // ...and the bare integer that shipped is exactly what did not.
        let Err(err) = plan("SELECT * FROM bin WHERE \"timestamp\" >= 1753833600000000".into()).await else {
            panic!("a bare integer bound must NOT silently coerce");
        };
        assert!(format!("{err}").contains("type_coercion"), "expected the prod failure mode, got: {err}");
    }

    /// The quantile probe must actually PLAN and return monotone cuts. It falls
    /// back to the uniform split on any error, silently — so if
    /// `approx_percentile_cont` over a cast timestamp does not plan in this
    /// DataFusion version, slicing quietly reverts to the behaviour that pinned
    /// 22 GB and nothing looks wrong.
    #[tokio::test]
    async fn repair_slice_cuts_are_monotone_and_row_balanced_under_skew() {
        use datafusion::{datasource::MemTable, prelude::SessionContext};
        // 1000 rows crammed into the last 1% of the span — the shape that defeats
        // an even TIME split.
        let ts: Vec<i64> = (0..1000).map(|i| 990_000 + i * 10).chain(std::iter::once(0)).collect();
        let ty = arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
        let arr = arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC");
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new("timestamp", ty, false)]));
        let batch = arrow::array::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).expect("batch");
        let ctx = SessionContext::new();
        ctx.register_table("bin", Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable"))).expect("register");

        let cuts = super::repair_slice_cuts(&ctx, "bin", "timestamp", 4).await;
        assert_eq!(cuts.len(), 3, "4 slices need 3 interior cuts; empty means the probe did not plan");
        assert!(cuts.windows(2).all(|w| w[0] < w[1]), "cuts must be strictly increasing to tile: {cuts:?}");

        // The whole point: cuts follow the ROWS into the dense tail, not the span.
        // An even TIME split would have put its first cut near 250_000.
        assert!(cuts[0] > 500_000, "cuts must track row density, not the time span: {cuts:?}");

        let bounds = super::repair_bounds_from_cuts(0, 999_990, &cuts);
        assert_eq!(bounds.len(), 4);
        assert_eq!(bounds[0].0, 0);
        assert!(bounds.last().expect("non-empty").1.is_none());
    }

    /// Shared tiling invariant for `repair_bounds_from_cuts` / `repair_slice_bounds`:
    /// the first slice starts at `lo`, the last is open-ended so `hi` is never
    /// dropped, adjacent slices abut exactly, and every value in `samples` lands
    /// in exactly one slice.
    fn assert_tiles(bounds: &[(i64, Option<i64>)], lo: i64, hi: i64, samples: &[i64]) {
        let last = *bounds.last().expect("non-empty");
        assert_eq!(bounds[0].0, lo, "first slice starts at lo");
        assert!(last.1.is_none(), "last slice is open so hi is never dropped");
        // The doc's claim about `hi`, actually checked. Without this the
        // parameter was unused, which is how it read as dead rather than as an
        // unasserted invariant.
        assert!(last.0 <= hi, "the open last slice must start at or before hi ({:?} vs {hi})", last.0);
        for w in bounds.windows(2) {
            assert_eq!(w[0].1.expect("only the last is open"), w[1].0, "{:?} must abut {:?}", w[0], w[1]);
        }
        for &v in samples {
            let hits = bounds.iter().filter(|(s, e)| v >= *s && e.is_none_or(|e| v < e)).count();
            assert_eq!(hits, 1, "value {v} must fall in exactly one slice of {bounds:?}");
        }
    }

    /// Quantile cuts must tile exactly like the uniform split — a gap drops rows
    /// and an overlap duplicates them, putting two versions of one key in
    /// different files where keep-greatest can no longer see them together.
    #[test]
    fn repair_cut_bounds_tile_the_range_and_survive_skew() {
        // Skewed cuts — the whole point: they may bunch anywhere in the range.
        let bounds = super::repair_bounds_from_cuts(0, 1000, &[997, 998, 999]);
        assert_eq!(bounds.len(), 4);
        assert_tiles(&bounds, 0, 1000, &[0, 1, 500, 999, 1000]);

        // Ties collapse to FEWER slices, never to an overlap.
        assert_eq!(super::repair_bounds_from_cuts(0, 100, &[50, 50, 50]), vec![(0, Some(50)), (50, None)]);

        // Cuts outside (lo, hi] are ignored; all-degenerate falls back to one pass.
        assert_eq!(super::repair_bounds_from_cuts(10, 20, &[5, 10, 99]), vec![(10, None)]);
        assert_eq!(super::repair_bounds_from_cuts(0, 100, &[]), vec![(0, None)]);

        // A cut exactly at hi is legal: it opens a final slice holding only hi.
        assert_tiles(&super::repair_bounds_from_cuts(0, 100, &[100]), 0, 100, &[0, 1, 50, 99, 100]);
    }

    #[test]
    fn repair_slices_tile_the_range_without_gaps_or_overlaps() {
        for (lo, hi, n) in [(0i64, 100i64, 4usize), (-50, 50, 3), (1_700_000_000_000_000, 1_700_000_086_400_000, 8), (0, 1, 4), (5, 5, 4)] {
            let bounds = super::repair_slice_bounds(lo, hi, n);
            assert_tiles(&bounds, lo, hi, &[lo, hi, lo + (hi - lo) / 2]);
        }
        // Degenerate inputs decline slicing rather than producing nonsense.
        assert_eq!(super::repair_slice_bounds(10, 5, 4), vec![(10, None)], "hi <= lo is a single unbounded pass");
        assert_eq!(super::repair_slice_bounds(0, 100, 1), vec![(0, None)], "one slice is the un-sliced path");
    }

    /// A footer probed as sorted must survive a RESTART.
    ///
    /// The set was in-process only, so every boot re-probed thousands of
    /// correctly-sorted files before the walk could reach a poisoned one: prod
    /// Persisted verified-sorted paths must survive a restart and prevent re-probing.
    ///
    /// Without persistence, every restart re-probes thousands of correctly sorted files before
    /// reaching the blocking one. Sound to persist because a Delta object path is immutable.
    #[tokio::test]
    async fn a_verified_sorted_footer_survives_a_restart() -> Result<()> {
        let cfg = create_test_config("verified-sorted-persist");
        let db = Database::with_config(cfg.clone()).await?;
        let paths: Vec<String> = (0..3).map(|i| format!("timefusion/t/project_id=p/date=2026-07-30/part-{i}.parquet")).collect();
        db.persist_verified_sorted(&paths);

        // A fresh Database over the SAME data dir is the restart.
        let restarted = Database::with_config(cfg).await?;
        assert!(restarted.repair_verified_sorted.is_empty(), "nothing is adopted until the load runs");
        restarted.load_verified_sorted();
        for path in &paths {
            assert!(restarted.repair_verified_sorted.contains(path), "re-adopted after restart: {path}");
        }

        // Loading twice must not duplicate or drop anything.
        restarted.load_verified_sorted();
        assert_eq!(restarted.repair_verified_sorted.len(), paths.len(), "idempotent");
        Ok(())
    }

    /// Degradation is forced here by an unmergeable schema (`id` Utf8 vs Int64),
    /// which is one of the several ways `sort_batches_by_schema` gives up — so
    /// the guard is exercised on the in-process path, not just on escalation.
    #[tokio::test]
    async fn rewrite_aborts_rather_than_writing_an_unsorted_file() -> Result<()> {
        use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
        let db = Database::with_config(create_test_config("rewrite-no-unsorted")).await?;
        let table = get_schema("otel_logs_and_spans").expect("registered");
        let batch = |id: ArrayRef| {
            let ts: TimestampMicrosecondArray = (0..4i64).map(Some).collect();
            let schema = arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("timestamp", arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
                arrow_schema::Field::new("id", id.data_type().clone(), false),
            ]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ts) as ArrayRef, id]).unwrap()
        };
        let conflicting = || {
            vec![batch(Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef), batch(Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef)]
        };

        // `FlushBatches` is a lazy iterator, not `Debug` — match rather than `expect_err`.
        let err = match db.sort_flush_group(table, conflicting(), UnsortedFallback::Forbid).await {
            Ok(_) => panic!("a rewrite must fail, not silently degrade to an unsorted file"),
            Err(e) => e,
        };
        assert!(format!("{err}").contains("keeping the committed inputs"), "the error must say the inputs were kept, got: {err}");

        // Ingest still degrades rather than losing rows — and says so honestly.
        let (_, sorted) = db.sort_flush_group(table, conflicting(), UnsortedFallback::Allow).await?;
        assert!(!sorted, "ingest writes the group unsorted and must NOT claim a sorted footer");

        // An empty group has no footer to lose: not a degradation, never an abort.
        let (_, sorted) = db.sort_flush_group(table, vec![], UnsortedFallback::Forbid).await?;
        assert!(!sorted, "empty group is trivially unsorted but must not abort a rewrite");
        Ok(())
    }

    /// Scrambled-event-time batches of `batches_n × rows_per_batch` wide rows (`id_width` bytes
    /// each), so an append-ordered pass-through cannot masquerade as a sort.
    fn wide_row_batches(rows_per_batch: i64, batches_n: i64, id_width: usize) -> Vec<RecordBatch> {
        use arrow::array::{StringArray, TimestampMicrosecondArray};
        let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("timestamp", arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
            arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, false),
        ]));
        (0..batches_n)
            .map(|b| {
                let ts: TimestampMicrosecondArray =
                    (0..rows_per_batch).map(|i| Some((b * rows_per_batch + i).wrapping_mul(2_654_435_761) % 1_000_000_000)).collect();
                let ids = StringArray::from_iter_values((0..rows_per_batch).map(|i| format!("{b:04}-{i:06}-{}", "x".repeat(id_width))));
                RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(ts), Arc::new(ids)]).unwrap()
            })
            .collect()
    }

    /// Escalated flush sorts, config floor (64 MB pool, sort_skip_bytes=0 so every group
    /// escalates). Each plan used to fan out to many `ExternalSorters` plus an unspillable merge
    /// exec, so N gate permits meant far more than N pool consumers and the sort starved its own
    /// pool, writing unsorted files — the escalated sort must be a single-partition plan with no
    /// merge exec so it survives a minimum pool floor with data far larger than the pool. And a
    /// batch is the sort's indivisible admission unit, so a pool that cannot hold one batch cannot
    /// spill — it just fails, poisoning the partition's footer ordering; wide rows can make one
    /// default-sized batch larger than a small floor pool.
    #[test_case(10_000, 16, 800, 160_000 ; "128MB across 16 batches, 2x the pool, starves the sort into escalation")]
    #[test_case(1_000, 16, 8_000, 16_000 ; "8KB rows: one old-sized batch would be 65MB, over the 64MB pool floor")]
    #[tokio::test]
    async fn escalated_flush_sort_is_one_pool_consumer_and_admits_wide_row_batches(
        rows_per_batch: i64, batches_n: i64, id_width: usize, expected_rows: i64,
    ) -> Result<()> {
        use arrow::array::TimestampMicrosecondArray;
        let mut cfg = (*create_test_config("flush-sort-floor")).clone();
        cfg.maintenance.timefusion_sort_skip_bytes = 0; // every group escalates
        cfg.maintenance.timefusion_flush_sort_pool_mb = 64; // the config floor
        let db = Database::with_config(Arc::new(cfg)).await?;
        let table = get_schema("otel_logs_and_spans").expect("registered");
        let batches = wide_row_batches(rows_per_batch, batches_n, id_width);

        let (out, escalated) = db.sort_flush_group(table, batches, UnsortedFallback::Allow).await?;
        assert!(escalated, "the pool could not sustain the sort and fell back to writing unsorted");
        let FlushBatches::Ready(it) = out else { panic!("escalated path yields Ready batches") };
        let stamps: Vec<i64> =
            it.flat_map(|b| b.column_by_name("timestamp").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().values().to_vec()).collect();
        assert_eq!(stamps.len() as i64, expected_rows, "the sort must not lose or duplicate rows");
        assert!(stamps.windows(2).all(|w| w[0] >= w[1]), "output must honor the schema's timestamp DESC ordering");
        Ok(())
    }

    /// The DML footer declaration must track the path that actually sorts.
    ///
    /// `dml_writer_properties` used to pass `declare_sorted=true` unconditionally, but only
    /// `perform_delta_merge_update` sorts. `UpdateBuilder`/`DeleteBuilder` write scan order, so a
    /// false footer makes `DedupExec`'s bounded mode advance its bound over unordered rows. A lying
    /// footer is worse than a missing one: a missing footer only costs the ordering claim, while a
    /// false one corrupts the bound.
    #[tokio::test]
    async fn dml_declares_a_sorted_footer_only_on_the_path_that_sorts() -> Result<()> {
        let db = Database::with_config(create_test_config("dml-footer-honesty")).await?;
        let schema = get_schema("otel_logs_and_spans").expect("registered");
        assert!(!schema.sorting_columns.is_empty(), "fixture needs a table with sort keys or this asserts nothing");

        // The merge/DV path sorts its appended rows, so it may declare them.
        let sorted = db.dml_writer_properties("otel_logs_and_spans", true);
        assert!(sorted.sorting_columns().is_some_and(|c| !c.is_empty()), "the DV-merge path sorts (append_sort_by) and must declare its footer");

        // Update/Delete do not sort. Their footer must claim nothing.
        let unsorted = db.dml_writer_properties("otel_logs_and_spans", false);
        assert!(
            unsorted.sorting_columns().is_none_or(|c| c.is_empty()),
            "UpdateBuilder/DeleteBuilder write in SCAN order — declaring the schema's sort order is a footer that lies"
        );
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
        let key = crate::read::CountPartition { project_id: project_id.clone(), table_name: "mor_versioned".to_string(), date: date.to_string() };
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

    /// Hot-tail wave staging must hold its own permits, not share the heavy maintenance rewrite
    /// semaphore.
    ///
    /// A long dedup drain once held every heavy rewrite permit and starved hot compaction for tens of
    /// minutes. The wave engine must have its own permits so exhausting the heavy semaphore leaves
    /// wave staging fully able to proceed.
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

    /// Packing and footer repair must use disjoint memory pools.
    ///
    /// When they shared one pool, the only way to protect a small packing bin from a large
    /// repair sort was to pause packing whenever repair was in flight. Repair runs frequently, so
    /// packing was effectively stopped. The split must be memory-neutral — the two slices partition
    /// the light share, not add to it — or it trades a compaction stall for an OOM.
    #[tokio::test]
    async fn packing_and_repair_hold_disjoint_pools_that_partition_the_light_share() -> Result<()> {
        let db = Database::with_config(create_test_config("pool-split")).await?;
        assert_eq!(db.pack_pool_bytes() + db.repair_pool_bytes(), db.light_optimize_pool_bytes(), "the split must not grow the maintenance budget");
        assert!(db.pack_pool_bytes() > 0 && db.repair_pool_bytes() > 0, "neither pass may be sized to zero");
        // The coordinator, light and heavy pools must TILE the maintenance
        // budget. They had independent definitions that agreed only while
        // HEAVY_MIN_SHARE was 0.25, so moving that share silently desynced
        // pools from `light_optimize_k`. Heavy was later still derived as "pool
        // minus light", which ABSORBED the coordinator's new share instead of
        // shrinking for it — 24 GiB of pools against a 16 GiB budget, the exact
        // over-commit shape that OOM-killed prod on 2026-08-17.
        assert_eq!(
            db.config.derived.coordinator_share_bytes() + db.light_optimize_pool_bytes() + db.heavy_pool_bytes(),
            db.config.derived.maintenance_pool_bytes(),
            "coordinator + light + heavy must tile the pool, never over-commit it"
        );
        assert_eq!(db.heavy_pool_bytes(), db.config.derived.heavy_share_bytes(), "the heavy pool must be the budget tree's heavy share, not a second opinion");
        assert!(!Arc::ptr_eq(&db.light_optimize_runtime_env(), &db.repair_runtime_env()), "sharing one RuntimeEnv is sharing one pool");

        // The brake must be blind to repair: an in-flight repair bin is exactly
        // the state that used to zero packing's throughput.
        let _repair_bin = in_flight_guard(&crate::observability::maintenance_stats().repair_bins_in_flight);
        assert!(db.light_optimize_brake().is_none(), "a repair bin in flight must no longer stop packing");
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

    /// Flush/ingest committers queued ahead of a wave must make the wave yield.
    ///
    /// The commit lock is FIFO, and a backlogged tick can queue several long wave commits ahead of a
    /// flush. The flush waits out the queue and gets killed by its watchdog even though no holder
    /// hangs (`commit_lock_timeouts` stays 0). Durability outranks maintenance, so a wave with a flush
    /// already queued must requeue its bins and count the yield. With no
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
        let waiter = flush_waiter(&db.flush_waiters("", "otel_logs_and_spans").await);
        let yields = crate::observability::maintenance_stats().wave_commits_yielded_to_flush.load(Relaxed);
        let deferred = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], false, bin(), 0).await;
        assert_eq!(crate::observability::maintenance_stats().wave_commits_yielded_to_flush.load(Relaxed), yields + 1, "the yield is counted, not silent");
        assert!(deferred.landed.is_empty(), "nothing may land while a flush waits");
        assert_eq!(deferred.failed.len(), 1, "the bin is requeued (dedup's dirty bin must not be certified clean)");
        assert_eq!(table_ref.read().await.version(), version, "no commit was attempted");

        // Flush done ⇒ the very same wave commits.
        drop(waiter);
        let landed = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], false, bin(), 0).await;
        assert_eq!(crate::observability::maintenance_stats().wave_commits_yielded_to_flush.load(Relaxed), yields + 1, "no yield without a waiter");
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
        let coalescer = Arc::new(crate::dml::DmlCoalescer::new(600, true));
        let _ = db.dml_coalescer.set(coalescer.clone());
        let _held = coalescer.lock_drain_for_test().await; // drain() blocks on this
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
        let res = tokio::time::timeout(std::time::Duration::from_secs(5), db.shutdown_by(deadline)).await;
        assert!(res.is_ok(), "shutdown_by hung on a blocked drain instead of honoring the deadline");
        Ok(())
    }

    /// Regression guard: snapshot refresh must not convoy readers behind a write lock.
    ///
    /// The old implementation held the table write lock across `update_state()` (full log replay +
    /// object-store IO), so concurrent reads convoyed behind it for minutes during flush passes. With a
    /// deliberately slow object store, read-lock acquisition must stay fast.
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

        let uris = |t: &DeltaTable| t.get_file_uris().map(|it| it.collect::<HashSet<String>>()).unwrap_or_default();
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

    /// `--project` scoping is refused while its deadlock is unexplained.
    ///
    /// Refusing beats both alternatives: hanging (what it does) and silently ignoring the flag (which
    /// would rewrite every tenant on the date without the caller asking). Re-enable only with a test
    /// that the scoped write actually completes.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn recompress_refuses_project_scope_and_leaves_data_intact() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(180), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let (target, other) = (format!("target_{prefix}"), format!("other_{prefix}"));
            let today = chrono::Utc::now().date_naive();
            for (pid, ids) in [(&target, ["t1", "t2"]), (&other, ["o1", "o2"])] {
                for id in ids {
                    let batch = json_to_batch(vec![test_span(id, "span", pid)])?;
                    db.insert_records_batch(pid, "otel_logs_and_spans", vec![batch], true, None).await?;
                }
            }
            let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");
            let files =
                |m: String| -> Result<Vec<String>> { Ok(futures::executor::block_on(table_ref.read()).get_file_uris()?.filter(|u| u.contains(&m)).collect()) };
            let (t_before, o_before) = (files(format!("project_id={target}/"))?, files(format!("project_id={other}/"))?);
            assert!(t_before.len() > 1 && o_before.len() > 1, "both tenants need >1 file for the rewrite to be non-trivial");

            // The scope is REFUSED, not silently ignored: it deadlocks the
            // write (see `recompress_partition`), and a repair tool that hangs
            // is worse than one that declines. Silently ignoring it would
            // rewrite every tenant on the date behind the caller's back.
            let err = db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, Some(target.as_str())).await.unwrap_err();
            assert!(err.to_string().contains("--project is disabled"), "must refuse, got: {err}");
            assert_eq!(files(format!("project_id={target}/"))?, t_before, "a refused scope must not have written anything");
            assert_eq!(files(format!("project_id={other}/"))?, o_before, "every other project's files must be untouched");

            // Unscoped still works and preserves every tenant's rows.
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, None).await?;
            let rows = ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{other}'")).await?.collect().await?;
            assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "other project's rows must survive the rewrite");

            db.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(|_| anyhow::anyhow!("Test timed out after 180 seconds"))?
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
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, None).await?;
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
            let rerun = db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, None).await?;
            assert!(matches!(rerun, RecompressOutcome::Skipped(_)), "rerun at same tier must report a SKIP, not a rewrite");
            let files_after_rerun: Vec<String> = table_ref.read().await.get_file_uris()?.collect();
            assert_eq!(files_after, files_after_rerun, "rerun at same tier must skip");

            // Downgrade target — also skip.
            db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 3, None).await?;
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
    /// the flip is monotonic and per-(project,table), never downgraded once set.
    ///
    /// A flush callback marks `(p, t)` true; a concurrent reader's `resolve_table` may observe
    /// `version() == 0` on its just-loaded snapshot. `populate_resolve_caches` used to store that
    /// false observation, downgrade the bit, and subsequent scans would skip Delta until restart
    /// — the load-bearing predicate for the 45% latency win. The fix only ever stores `true`.
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

        // Force a resolve of the unified table. The fresh handle reports
        // version() == 0 because nothing has been written. Pre-fix this
        // would have downgraded the bit; post-fix the sticky-true
        // invariant holds.
        let _t = db.resolve_table(&p1, t).await?;
        assert!(
            !db.delta_scan_can_be_skipped(&p1, t),
            "STICKY-TRUE: resolve_table observing version==0 must NOT downgrade a previously-marked bit. \
             A regression here means post-flush rows get hidden from queries."
        );

        // Resolve via the alternative path used by SELECTs (try_fast_resolve
        // → fast_resolve_cache hit) — same invariant must hold.
        let _ = db.try_fast_resolve(&p1, t);
        assert!(!db.delta_scan_can_be_skipped(&p1, t), "STICKY-TRUE preserved across try_fast_resolve too");
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
        register_custom_storage(&db, &custom, t, &format!("custom-{prefix}")).await;

        let (a, b) = (db.table_lock_key("proj_a", t).await, db.table_lock_key("proj_b", t).await);
        assert_eq!(a, b, "default-storage projects share a physical log → one coalesced commit");
        assert_ne!(db.table_lock_key(&custom, t).await, a, "custom-storage project must group (and commit) separately");
        // Same project on a different table is also a different physical log.
        assert_ne!(db.table_lock_key("proj_a", "otel_metrics").await, a);
        Ok(())
    }

    /// Provider cache invalidation on snapshot version change.
    ///
    /// The cache keyed on `(project, table) → (version, Arc<OnceCell<Provider>>)` must replace the
    /// cell when `table.version()` advances. Strategy: query the table, insert a commit to bump
    /// version, then query again; the second query must see the new row, proving the cached provider
    /// was rebuilt.
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
            let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
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
            let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
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

    /// Regression: rows force-flushed to Delta from an open bucket must stay visible after the bucket
    /// seals.
    ///
    /// The per-bucket exclusion used to mask the whole window from the Delta scan while the flush
    /// backlog kept the bucket in MemBuffer. Force-flushed buckets must stay exempt from the
    /// exclusion for their whole lifetime, not just while current.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn force_flushed_bucket_rows_stay_queryable_after_seal() -> Result<()> {
        // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
        let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let cfg = create_test_config(&prefix);
        let res = tokio::time::timeout(std::time::Duration::from_secs(50), async {
            let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
            let project_id = format!("ffs_{}", prefix);
            // Freeze the clock mid-window so all inserts land in one
            // deterministic bucket we can later seal by advancing time.
            let dur = crate::write::mem_buffer::bucket_duration_micros();
            let t0 = crate::support::set_micros((crate::support::now_micros() / dur) * dur + dur / 2);

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
            crate::support::advance_micros(dur);

            use datafusion::arrow::array::AsArray;
            let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}'", project_id);
            let r = b.session_ctx.sql(&sql).await?.collect().await?;
            let n = r[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            anyhow::ensure!(n == 5, "force-flushed rows must stay visible after their bucket seals; got {n} of 5");
            b.shutdown.cancel();
            Ok(())
        })
        .await;
        crate::support::unfreeze();
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
            let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
            let project_id = format!("ffw_{}", prefix);
            let dur = crate::write::mem_buffer::bucket_duration_micros();
            let t0 = crate::support::set_micros((crate::support::now_micros() / dur) * dur + dur / 2);

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
        crate::support::unfreeze();
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
            register_custom_storage(&db, &pb, &table, &format!("custom-{prefix}")).await;

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

    /// SQL surface smoke test, one shared DB/session: insert+count+select+project isolation,
    /// level/duration/compound filtering, literal SQL `INSERT` (single- and multi-row) alongside
    /// an API insert, and timestamp filtering + `to_char` formatting.
    #[serial]
    #[tokio::test(flavor = "multi_thread")]
    async fn sql_surface_smoke() -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (db, ctx, prefix) = setup_test_database().await?;
            use datafusion::arrow::array::AsArray;

            // Basic insert, count and field selection on a single project.
            let solo = format!("project_{}", prefix);
            let batch = json_to_batch(vec![test_span("test1", "span1", &solo)])?;
            db.insert_records_batch(&solo, "otel_logs_and_spans", vec![batch], true, None).await?;
            let result = ctx.sql(&format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", solo)).await?.collect().await?;
            assert_eq!(result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0), 1);
            let result = ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}'", solo)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 0), "test1");
            assert_eq!(array_get_str(result[0].column(1).as_ref(), 0), "span1");

            // Multiple projects: isolation per project, and a total across them.
            let projects: Vec<String> = (1..=3).map(|i| format!("proj{}_{}", i, prefix)).collect();
            for project in &projects {
                let batch = json_to_batch(vec![test_span(&format!("id_{}", project), &format!("span_{}", project), project)])?;
                db.insert_records_batch(project, "otel_logs_and_spans", vec![batch], true, None).await?;
            }
            for project in &projects {
                let sql = format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                assert_eq!(result[0].num_rows(), 1);
                assert_eq!(array_get_str(result[0].column(0).as_ref(), 0), format!("id_{}", project));
            }
            let mut total_count = 0;
            for project in &projects {
                let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                total_count += result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            }
            assert_eq!(total_count, 3, "each project keeps exactly its own row, unseen by the others");

            // Level / duration / compound filtering.
            let filter_project = format!("filter_proj_{}", prefix);
            use chrono::Utc;
            use serde_json::json;
            let now = Utc::now();
            let records = vec![
                json!({
                    "timestamp": now.timestamp_micros(), "id": "span1", "name": "test_span_1", "project_id": &filter_project,
                    "level": "INFO", "status_code": "OK", "duration": 100_000_000, "date": now.date_naive().to_string(),
                    "hashes": [], "summary": ["Test span 1 - INFO level"]
                }),
                json!({
                    "timestamp": (now + chrono::Duration::minutes(10)).timestamp_micros(), "id": "span2", "name": "test_span_2",
                    "project_id": &filter_project, "level": "ERROR", "status_code": "ERROR", "status_message": "Error occurred",
                    "duration": 200_000_000, "date": now.date_naive().to_string(), "hashes": [], "summary": ["Test span 2 - ERROR level"]
                }),
            ];
            let batch = json_to_batch(records)?;
            db.insert_records_batch(&filter_project, "otel_logs_and_spans", vec![batch], true, None).await?;
            let result = ctx
                .sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'", filter_project))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 0), "span2");
            let result = ctx
                .sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND duration > 150000000", filter_project))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 0), "span2");
            let result = ctx
                .sql(&format!("SELECT id, status_message FROM otel_logs_and_spans WHERE project_id = '{}' AND level = 'ERROR'", filter_project))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(array_get_str(result[0].column(1).as_ref(), 0), "Error occurred");

            // Literal SQL INSERT, single- and multi-row, alongside a prior API insert.
            let sql_proj1 = format!("default_{}", prefix);
            let sql_proj2 = format!("sqlins_proj2_{}", prefix);
            let batch = json_to_batch(vec![test_span("id1", "name1", &sql_proj1)])?;
            db.insert_records_batch(&sql_proj1, "otel_logs_and_spans", vec![batch], true, None).await?;
            let sql = format!(
                "INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES (
                       '{}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z',
                       'sql_id', ARRAY[], 'sql_name', 'INFO', 'OK', ARRAY['SQL inserted test span']
                     )",
                sql_proj2
            );
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            let mut sql_total = 0;
            for project in [&sql_proj1, &sql_proj2] {
                let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{}'", project);
                let result = ctx.sql(&sql).await?.collect().await?;
                sql_total += result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0);
            }
            assert_eq!(sql_total, 2);
            let result =
                ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{}' AND id = 'sql_id'", sql_proj2)).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(array_get_str(result[0].column(1).as_ref(), 0), "sql_name");
            // Multi-row INSERT, in one statement, returns a count of rows inserted
            // and preserves per-row field values and (default) insertion order.
            let multirow_id = format!("multirow_{}", prefix);
            let sql = format!("INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES
                     ('{multirow_id}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 'id1', ARRAY[], 'name1', 'INFO', 'OK', ARRAY['Multi-row insert test 1']),
                     ('{multirow_id}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T11:00:00Z', 'id2', ARRAY[], 'name2', 'INFO', 'OK', ARRAY['Multi-row insert test 2']),
                     ('{multirow_id}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T12:00:00Z', 'id3', ARRAY[], 'name3', 'ERROR', 'ERROR', ARRAY['Multi-row insert test 3 - ERROR'])");
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 3);
            let sql = format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{multirow_id}'");
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].column(0).as_primitive::<arrow::datatypes::Int64Type>().value(0), 3);
            let result =
                ctx.sql(&format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{multirow_id}' ORDER BY id")).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 3);
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 0), "id1");
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 1), "id2");
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 2), "id3");

            // Timestamp filtering and `to_char` formatting.
            let ts_project = format!("ts_test_{}", prefix);
            let base_time = chrono::DateTime::parse_from_rfc3339("2023-01-01T10:00:00Z").unwrap().with_timezone(&Utc);
            let records = vec![
                json!({
                    "timestamp": base_time.timestamp_micros(), "id": "early", "name": "early_span", "project_id": &ts_project,
                    "date": base_time.date_naive().to_string(), "hashes": [], "summary": ["Early span for timestamp test"]
                }),
                json!({
                    "timestamp": (base_time + chrono::Duration::hours(2)).timestamp_micros(), "id": "late", "name": "late_span",
                    "project_id": &ts_project, "date": base_time.date_naive().to_string(), "hashes": [], "summary": ["Late span for timestamp test"]
                }),
            ];
            let batch = json_to_batch(records)?;
            db.insert_records_batch(&ts_project, "otel_logs_and_spans", vec![batch], true, None).await?;
            let result = ctx
                .sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp > '2023-01-01T11:00:00Z'", ts_project))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(array_get_str(result[0].column(0).as_ref(), 0), "late");
            let result = ctx
                .sql(&format!(
                    "SELECT id, to_char(timestamp, 'YYYY-MM-DD HH24:MI') as ts FROM otel_logs_and_spans WHERE project_id = '{}' ORDER BY timestamp",
                    ts_project
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(result[0].num_rows(), 2);
            assert_eq!(array_get_str(result[0].column(1).as_ref(), 0), "2023-01-01 10:00");
            assert_eq!(array_get_str(result[0].column(1).as_ref(), 1), "2023-01-01 12:00");

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
            let db = Database::with_config(create_test_config("concurrent-writes-same-project")).await?;
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
            let db = Database::with_config(create_test_config("concurrent-table-creation")).await?;
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
            use crate::write::BatchQueue;

            let db = Arc::new(Database::with_config(create_test_config("batch-queue-under-load")).await?);
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
            let db = Database::with_config(create_test_config("concurrent-mixed-operations")).await?;
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
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 2, "sealed bin is deduplicated without dropping an adjacent-bin row from the same file");
        assert!(db.dedup_dirty_bins.is_empty(), "completed sealed bin is consumed");

        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("sealed", "later", old)?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1, "late retry requeues the previously consumed bin");
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 2, "later observed timestamp survives the requeue rewrite and the neighbour remains");

        let fresh = Utc::now().timestamp_micros();
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("unsealed", "a", fresh)?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("unsealed", "b", fresh)?], true, None).await?;
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
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
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &flaky, std::time::Duration::MAX, far_future()).await?;
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

        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(
            crate::observability::maintenance_stats().dirty_bin_batch_probe_clean.load(Relaxed),
            2,
            "both clean bins are consumed by the batch probe alone"
        );
        assert!(db.dedup_dirty_bins.is_empty(), "clean and dup bins are all consumed");
        assert_eq!(delta_physical_row_count(&table).await?, 3, "the duplicate collapsed; clean rows untouched");
        Ok(())
    }

    /// The probe phase's deadline is shared by every group, not re-granted per group.
    ///
    /// A per-probe `Duration` ran in waves of `rewrite_permits`, so many groups could take
    /// multiples of the budget the caller intended. An `Instant` makes over-granting unrepresentable.
    /// A group reached with nothing left must leave its bins queued: dequeuing for a probe that
    /// cannot run would spend the cheap classification path's entry ticket on nothing.
    #[tokio::test]
    async fn dedup_batch_probe_leaves_bins_queued_when_the_phase_budget_is_gone() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-probedeadline-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let day = (Utc::now() - chrono::Duration::hours(26)).date_naive();
        let base = day.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
        const TEN_MIN: i64 = 10 * 60 * 1_000_000;
        let row = |id: &str, ts: i64| json_to_batch(vec![test_span_ts(id, "only", &project, ts)]);
        // Two bins on one date, so the group clears the `bins.len() >= 2` filter.
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("a", base)?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("b", base - TEN_MIN)?], true, None).await?;
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();

        let ready: Vec<(String, String, i64)> = db.dedup_dirty_bins.iter().map(|e| (e.key().0.clone(), e.key().2.clone(), e.key().3)).collect();
        let queued_before = db.dedup_dirty_bins.len();
        assert_eq!(queued_before, 2, "both bins are queued to begin with");

        let out = db.batch_probe_classify(&table, "otel_logs_and_spans", ready, std::time::Instant::now()).await;

        assert_eq!(db.dedup_dirty_bins.len(), queued_before, "an exhausted phase budget must not dequeue bins it never probed");
        assert_eq!(out.len(), queued_before, "and every bin still fails OPEN to the per-bin staging path");
        Ok(())
    }

    /// A drain pass out of budget must stop admitting bins and leave them queued.
    ///
    /// Staging used to ignore the pass deadline: up to 64 bins, each with a long per-bin ceiling,
    /// inside a tick whose entire budget was short. An over-running pass holds `maintenance_job_sem`
    /// and drops overlapping ticks, costing every tick behind it and every other maintenance job.
    /// Bins that cannot be admitted must stay queued so the next tick can serve them.
    #[tokio::test]
    async fn dedup_drain_out_of_budget_leaves_its_bins_queued() -> Result<()> {
        let cfg = create_test_config(&format!("dirty-dedup-passbudget-{}", uuid::Uuid::new_v4().simple()));
        let db = Database::with_config(cfg).await?;
        let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
        let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();
        let row = |observed: &str| json_to_batch(vec![test_span_ts("sealed", observed, &project, old)]);
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("first")?], true, None).await?;
        db.insert_records_batch(&project, "otel_logs_and_spans", vec![row("second")?], true, None).await?;
        assert_eq!(db.dedup_dirty_bins.len(), 1);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();

        // An already-elapsed pass deadline with a generous per-bin ceiling: only
        // the pass budget can stop this, and it must.
        let started = std::time::Instant::now();
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::from_secs(3600), std::time::Instant::now()).await?;
        assert!(started.elapsed() < std::time::Duration::from_secs(30), "the pass must return on its budget, not run the per-bin ceiling");
        assert_eq!(db.dedup_dirty_bins.len(), 1, "an unadmitted bin stays queued for the next tick");
        assert_eq!(delta_physical_row_count(&table).await?, 2, "no partial rewrite landed");

        // And the bin is still perfectly drainable once a pass has budget.
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 1, "the deferred bin dedups on a pass that has budget");
        assert!(db.dedup_dirty_bins.is_empty());
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
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::from_millis(1), far_future()).await?;
        assert!(crate::observability::maintenance_stats().dedup_bin_stage_timeouts.load(Relaxed) >= 1, "the per-bin deadline must fire");
        assert_eq!(db.dedup_dirty_bins.len(), 1, "the timed-out bin is requeued, not lost");
        assert_eq!(delta_physical_row_count(&table).await?, 2, "no partial rewrite landed");

        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
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
        // Shard concurrency is funded by SHRINKING the shard, so the product
        // `shards_in_flight x per-shard budget` must not exceed what one
        // sequential 2 GiB shard already held. A change that raises the budget
        // without lowering the concurrency (or vice versa) breaks that.
        const GIB: u64 = 1024 * 1024 * 1024;
        for budget in [GIB / 8, GIB / 4, GIB / 2, GIB, 2 * GIB] {
            let k = dedup_shard_concurrency(budget, 48) as u64;
            assert!(k >= 1, "a bin must always make progress");
            assert!(k * budget <= super::DEDUP_BIN_ARROW_BUDGET, "{k} shards x {budget} B exceeds the per-bin Arrow budget");
        }
        assert_eq!(dedup_shard_concurrency(GIB / 2, 48), 4, "prod: 512 MiB shards, four in flight, same 2 GiB peak as one old shard");
        assert_eq!(dedup_shard_concurrency(GIB / 2, 8), 2, "cores/4 bounds it on a small box");
        assert_eq!(dedup_shard_concurrency(0, 48), 1, "no configured ceiling means one shard already holds everything");

        assert_eq!(dedup_shard_count(false, 100, 100, 100, 100), 1);
        assert_eq!(dedup_shard_count(false, 101, 100, 100, 100), 2);
        assert_eq!(dedup_shard_count(false, u64::MAX, 1, 1, 0), DEDUP_BUCKET_COUNT);
        // The streaming branch never collects, so no budget can shard it.
        assert_eq!(dedup_shard_count(true, u64::MAX, u64::MAX, 1, 1), 1, "a ROW_NUMBER dedup is bounded by its execution shape, not by the decoded budget");
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

        let deferred_before = crate::observability::maintenance_stats().dedup_bins_deferred_cold.load(std::sync::atomic::Ordering::Relaxed);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();
        // Batch of 1 with a cold-only queue still serves it (lowest priority ≠
        // never), so the drain deduplicates rather than abandoning the rows.
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
        assert_eq!(
            crate::observability::maintenance_stats().dedup_bins_deferred_cold.load(std::sync::atomic::Ordering::Relaxed),
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

        let yields_before = crate::observability::maintenance_stats().dedup_passes_flush_yields.load(std::sync::atomic::Ordering::Relaxed);
        let table = db.unified_tables().read().await.get("otel_logs_and_spans").unwrap().clone();
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| false, std::time::Duration::MAX, far_future()).await?;
        assert_eq!(
            crate::observability::maintenance_stats().dedup_passes_flush_yields.load(std::sync::atomic::Ordering::Relaxed),
            yields_before + 1,
            "the skipped pass is counted, not silent"
        );
        assert_eq!(db.dedup_dirty_bins.len(), 1, "the bin stays queued for a healthier tick");
        assert_eq!(delta_physical_row_count(&table).await?, 2, "no rewrite happened; read-side dedup keeps results correct");

        // Healthy again ⇒ the same bin drains normally.
        db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await?;
        assert_eq!(delta_physical_row_count(&table).await?, 1);
        Ok(())
    }

    /// `date = '…'` and `BETWEEN` bound a scan but are invisible to
    /// `extract_time_range_from_filters`. Enforce mode used to reject both —
    /// including the rollup builder's own `project_id = … AND date = …`
    /// aggregate, which has no timestamp predicate and no LIMIT.
    #[test]
    fn a_date_partition_or_between_predicate_counts_as_bounded() {
        assert!(ProjectRoutingTable::is_bounding_predicate(&col("date").eq(lit("2026-08-10"))));
        assert!(ProjectRoutingTable::is_bounding_predicate(&col("timestamp").between(lit(1_i64), lit(2_i64))));
        assert!(!ProjectRoutingTable::is_bounding_predicate(&col("timestamp").not_between(lit(1_i64), lit(2_i64))));
        assert!(!ProjectRoutingTable::is_bounding_predicate(&col("name").between(lit(1_i64), lit(2_i64))));
        assert!(!ProjectRoutingTable::is_bounding_predicate(&col("project_id").eq(lit("project"))));
    }

    #[test]
    fn bounded_otel_scan_requires_a_project_and_bound() {
        let project = col("project_id").eq(lit("project"));
        assert_eq!(ProjectRoutingTable::raw_otel_scan_reason("otel_logs_and_spans", &[], None, false), Some("missing exact project_id filter"));
        assert_eq!(
            ProjectRoutingTable::raw_otel_scan_reason("otel_logs_and_spans", std::slice::from_ref(&project), None, false),
            Some("missing timestamp lower bound or scan limit")
        );
        assert_eq!(ProjectRoutingTable::raw_otel_scan_reason("otel_logs_and_spans", std::slice::from_ref(&project), Some(1), false), None);
        assert_eq!(ProjectRoutingTable::raw_otel_scan_reason("otel_logs_and_spans", std::slice::from_ref(&project), None, true), None);
        assert_eq!(
            ProjectRoutingTable::raw_otel_scan_reason(
                "otel_logs_and_spans",
                &[col("project_id").eq(lit("one")).or(col("project_id").eq(lit("two")))],
                None,
                true,
            ),
            Some("missing exact project_id filter")
        );
        assert_eq!(ProjectRoutingTable::raw_otel_scan_reason("timefusion_stats", &[], None, false), None);
    }
}

#[cfg(test)]
mod footer_repair_schedule_tests {
    /// A repair pass must admit what its BUDGET can finish, not what a
    /// five-minute tick could. Prod 2026-08-08: shipbubble's `date=2026-07-30`
    /// was pinned by a 1,088,634,971-byte file — 14 MB over the 1 GiB prod
    /// setting — so it was never a candidate, and no budget, concurrency or
    /// cadence change could ever have repaired that tenant.
    #[test]
    fn repair_reach_follows_the_budget_and_admits_the_file_that_pinned_shipbubble() {
        const GIB: i64 = 1073741824;
        let budget = std::time::Duration::from_secs(8640); // the 144-minute default
        let reach = super::repair_reach_bytes(GIB, budget);
        assert!(reach > 1_088_634_971, "the file that pinned shipbubble must be admitted, got {reach}");
        // Never narrower than what the operator asked for.
        assert_eq!(super::repair_reach_bytes(8 * GIB, budget), 8 * GIB, "a larger configured cap still wins");
        // A short budget must not pretend it can finish a huge file.
        assert!(super::repair_reach_bytes(0, std::time::Duration::from_secs(240)) < GIB / 4, "240s reaches only a small file");
    }

    /// Prod 2026-08-08: an escalated flush sort's merge phase reached 675 MB and
    /// was refused 96.9 MB more with only "18.4 MB remain available for the
    /// total memory pool: fair(pool_size: 1024.0 MB)" — the 512 MB divisor had
    /// admitted a second sort into a pool that cannot feed two. A refusal writes
    /// the group UNSORTED, and one unsorted file kills the reader's
    /// all-or-nothing footer ordering for the whole partition, so the flush path
    /// was manufacturing the very poison the repair pass cleans up.
    #[test]
    fn a_flush_sort_slice_covers_the_largest_measured_sort() {
        const MB: usize = 1 << 20;
        // The measured peak, plus room for the allocation that was refused.
        const MEASURED_PEAK: usize = 675 * MB + 97 * MB;
        for pool in [1024 * MB, 2048 * MB, 4096 * MB, 64 * MB] {
            let slice = pool / super::flush_sort_permits(pool);
            assert!(
                slice >= MEASURED_PEAK || pool < MEASURED_PEAK,
                "a {}MB pool admits {} sorts, giving each {}MB — below the {}MB a real sort was measured needing",
                pool / MB,
                super::flush_sort_permits(pool),
                slice / MB,
                MEASURED_PEAK / MB
            );
        }
        assert_eq!(super::flush_sort_permits(1024 * MB), 1, "two 675MB sorts do not fit in the 1GB default pool");
        // Concurrency is bought by raising the pool, not by shrinking the slice.
        assert!(super::flush_sort_permits(4096 * MB) > 1, "a pool with real room must still allow concurrent sorts");
        // The 64MB config floor cannot feed even one sort, but must never yield
        // zero permits — that would deadlock every escalation.
        assert_eq!(super::flush_sort_permits(64 * MB), 1, "the floor must still admit one sort");
    }

    /// Prod 2026-08-08: `otel_metrics` logged `planned=9 completed=0 bins=0` on
    /// EVERY 10-minute tick for hours — five bins, each timing out at exactly
    /// the 240s deadline, discarded, then re-selected identically next tick.
    /// A bin the tick cannot finish does not run late, it produces NOTHING
    /// forever, so the packing unit has to be sized by the time available.
    #[test]
    fn a_packing_bin_is_sized_to_what_the_tick_can_actually_finish() {
        const MB: i64 = 1024 * 1024;
        let tick = std::time::Duration::from_secs(240);
        let target = super::pack_target_bytes(256 * MB, tick);
        // The SLOWEST rate measured across tables, not the average: prod
        // 2026-08-08 saw a 6x spread on identically sized bins —
        // otel_logs_and_spans 107MB/37.7s (2.8 MB/s) vs otel_metrics
        // 106MB/225s (0.47 MB/s). Sizing for the average is what left the
        // metrics bins at 198s and 225s of a 240s tick, one dip from producing
        // nothing, so the bin must fit at the slow end with margin to spare.
        const SLOWEST_BYTES_PER_SEC: i64 = 470_000;
        let secs_at_slowest = target / SLOWEST_BYTES_PER_SEC;
        assert!(secs_at_slowest * 2 <= 240, "a bin must fit the tick at the SLOWEST measured rate with margin, got {secs_at_slowest}s of a 240s tick");
        assert!(target < 256 * MB, "the 256 MB target is exactly what could not finish");
        // A freshly packed run must land ABOVE the sorted-run cap the same
        // target derives (`target / 2`), or every output is re-selected next
        // tick and packing becomes a rewrite loop.
        assert!(target > target / 2, "a full bin must exceed the sorted-run cap it derives");
        // Never widens: a generous budget leaves the operator's target alone.
        assert_eq!(super::pack_target_bytes(64 * MB, std::time::Duration::from_secs(3600)), 64 * MB, "a budget with room keeps the configured target");
        // Bigger budget, bigger unit — the sizing must track the budget, not be
        // a second hardcoded constant.
        assert!(
            super::pack_target_bytes(256 * MB, std::time::Duration::from_secs(480)) > super::pack_target_bytes(256 * MB, tick),
            "doubling the tick must admit a larger bin"
        );
        assert!(super::pack_target_bytes(256 * MB, std::time::Duration::from_secs(0)) >= 1, "a zero budget must not produce a zero-size cap");
    }

    /// The repair budget is derived from the schedule string, so a 5-vs-6-field
    /// cron mix-up would silently hand repair the wrong budget — the exact
    /// failure the split exists to remove.
    #[test]
    fn default_footer_repair_budget_clears_a_measured_whole_file_rewrite() {
        let cfg = crate::config::AppConfig::default();
        let period = super::cron_period(&cfg.maintenance.timefusion_footer_repair_schedule);
        assert_eq!(period, std::time::Duration::from_secs(3600), "tries hourly, so a restart repairs something soon");
        // The run length is INDEPENDENT of that period. The measured
        // contention-free rewrite of prod's blocking file was 43 minutes; at
        // `concurrency = k/2` expect 2-3x, so the budget must clear ~130 with
        // margin. Overlapping ticks are skipped, so budget > period is sound.
        let budget = std::time::Duration::from_secs(cfg.maintenance.timefusion_footer_repair_budget_secs);
        assert_eq!(budget, std::time::Duration::from_secs(8640), "144-minute budget");
        assert!(budget >= std::time::Duration::from_secs(43 * 60 * 3), "must clear 3x the measured solo rewrite");
        assert!(budget > period, "a long run must not be capped by a short cadence");
    }
}
