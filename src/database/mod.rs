use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
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
    common::not_impl_err,
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
use deltalake::{
    DeltaTable, DeltaTableBuilder, FilterLiteral, FilterOp, FilterValue, datafusion::parquet::file::properties::WriterProperties,
    kernel::transaction::CommitProperties, logstore::LogStore, operations::create::CreateBuilder,
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
mod histogram;
mod index;
/// Single definition of the sidecar bin width: a bin marked at one width and
/// looked up at another is never found.
pub(crate) use compact::DEFAULT_BIN_MINUTES;
pub use histogram::CapturedHistogram;
pub(crate) use histogram::{HistogramDmlGuard, HistogramDmlScope};
pub(crate) mod maintain;
mod rollup;
mod scan;
mod write;

pub(crate) use index::TANTIVY_RECONCILE_CONCURRENCY;
pub use index::truncate_to_byte_budget;
#[cfg(test)]
pub(crate) use index::{TantivyBackfillWork, fair_tantivy_backfill_work, fair_tantivy_backfill_work_split, sort_backfill_uris_newest_first};
pub use scan::ProjectRoutingTable;
pub(crate) use scan::{DECODE_UNITS_PER_READER, scan_pressure_permits, selected_file_work, stale_coverage_metric};
#[cfg(test)]
pub(crate) use scan::{GatedScanExec, NOMINAL_DECODE_BATCH_BYTES, date_partition_window, pressure_permit_claim_at};

/// The decode ratio every sort budget is denominated in; `config` derives the
/// repair budget from it.
pub(crate) use maintain::DECODED_BYTES_PER_COMPRESSED;
pub use maintain::{file_content_hash, note_probe_cost_into, probe_groups_for_budget};
pub use write::spill_disk_builder;

/// Delta tables shared by default projects and partitioned by `project_id`.
pub type UnifiedTables = Arc<RwLock<HashMap<String, Arc<RwLock<DeltaTable>>>>>;

/// Soft size at which the no-eviction table caches log a warning: 10× the
/// design target of "thousands of tenants".
const CACHE_SOFT_LIMIT_WARN: usize = 10_000;

/// Build de-duplicator for the cached Delta `TableProvider`: initialised once per
/// `(project, table, version)`; concurrent misses await the same build.
type DeltaProviderCell = tokio::sync::OnceCell<Arc<dyn datafusion::datasource::TableProvider>>;

#[derive(derive_more::Debug)]
#[debug("CachedDeltaProvider {{ version: {version}, age: {:?}, .. }}", created_at.elapsed())]
struct CachedDeltaProvider {
    version: u64,
    created_at: std::time::Instant,
    cell: Arc<DeltaProviderCell>,
}

/// Snapshot versions kept cached per `(project, table)`. A single slot thrashes
/// under flush cadence; a short ring serves each query its exact version.
const PROVIDER_VERSION_RETENTION: usize = 3;

/// The recent-version ring for one `(project, table)`, newest first.
#[derive(Debug, Default)]
struct ProviderVersions {
    versions: Vec<CachedDeltaProvider>,
}

impl ProviderVersions {
    /// Cell for `version`, if cached and within TTL. Exact-version match only —
    /// an older retained version must never serve a newer snapshot's query.
    fn get(&self, version: u64, ttl: std::time::Duration) -> Option<Arc<DeltaProviderCell>> {
        self.versions.iter().find(|e| e.version == version && e.created_at.elapsed() <= ttl).map(|e| Arc::clone(&e.cell))
    }

    /// Install a fresh cell for `version` at the head, dropping expired or
    /// same-version entries and keeping only `PROVIDER_VERSION_RETENTION`.
    fn install(&mut self, version: u64, ttl: std::time::Duration) -> Arc<DeltaProviderCell> {
        let cell = Arc::new(DeltaProviderCell::new());
        self.versions.retain(|e| e.version != version && e.created_at.elapsed() <= ttl);
        self.versions.insert(0, CachedDeltaProvider { version, created_at: std::time::Instant::now(), cell: Arc::clone(&cell) });
        self.versions.truncate(PROVIDER_VERSION_RETENTION);
        cell
    }

    /// Drop expired versions; returns how many were removed.
    fn prune(&mut self, ttl: std::time::Duration) -> usize {
        self.versions.extract_if(.., |e| e.created_at.elapsed() > ttl).count()
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
pub(crate) struct ScanShape {
    skipped_delta: bool,
    has_mem: bool,
    has_delta: bool,
    fast_resolve_hit: Option<bool>,
    /// Read-side dedup skip engaged (all window partitions sweep-verified clean).
    skip_dedup: bool,
}

/// Why the swept-partition dedup skip was granted or refused, decided once per scan.
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

/// One declaration per scan counter: its metric name AND the `timefusion_stats`
/// row that exposes it, so a counter cannot be recorded and stay invisible.
///
/// * `rows { C = "metric" as component.row_key; }` — exposed verbatim.
/// * `reasons { … when "label"; }` — same, plus a `PREFILTER_SKIP_REASONS` entry.
/// * `derived { C = "metric" via component.row_key; }` — reaches stats only via a
///   row `pg_compat` computes by hand; that row is asserted to exist.
macro_rules! scan_metrics {
    (
        rows { $($(#[$rm:meta])* $rid:ident = $rmetric:literal as $rcomp:ident.$rkey:ident;)+ }
        reasons { $($(#[$xm:meta])* $xid:ident = $xmetric:literal as $xcomp:ident.$xkey:ident when $reason:literal;)+ }
        derived { $($(#[$dm:meta])* $did:ident = $dmetric:literal via $dcomp:ident.$dkey:ident;)+ }
    ) => {
        $($(#[$rm])* pub const $rid: &str = $rmetric;)+
        $($(#[$xm])* pub const $xid: &str = $xmetric;)+
        $($(#[$dm])* pub const $did: &str = $dmetric;)+

        /// `(component, row key, metric)` — rendered verbatim by `timefusion_stats`.
        pub const SCAN_ROWS: &[(&str, &str, &str)] =
            &[$((stringify!($rcomp), stringify!($rkey), $rmetric),)+ $((stringify!($xcomp), stringify!($xkey), $xmetric),)+];

        /// `(component, row key)` of the hand-computed rows the `derived` counters
        /// reach stats through. Asserted present, never rendered from here.
        pub const SCAN_DERIVED_ROWS: &[(&str, &str)] = &[$((stringify!($dcomp), stringify!($dkey)),)+];

        /// Skip label -> metric name, as passed by `record_prefilter_skip`.
        pub const PREFILTER_SKIP_REASONS: &[(&str, &str)] = &[$(($reason, $xmetric),)+];
    };
}

pub mod scan_metric_names {
    scan_metrics! {
    rows {
        SCANS_TOTAL = "timefusion.scan.scans_total" as scan.total;
        SCANS_SKIPPED_DELTA = "timefusion.scan.scans_skipped_delta" as scan.skipped_delta;
        SCANS_MEM_ONLY = "timefusion.scan.scans_mem_only" as scan.mem_only;
        SCANS_DELTA_ONLY = "timefusion.scan.scans_delta_only" as scan.delta_only;
        SCANS_MEM_PLUS_DELTA = "timefusion.scan.scans_mem_plus_delta" as scan.mem_plus_delta;
        DEDUP_ELIGIBLE_SCANS = "timefusion.scan.dedup_eligible_scans" as scan.dedup_eligible;
        DEDUP_SKIPPED = "timefusion.scan.dedup_skipped" as scan.dedup_skipped;
        DEDUP_DENIED_UNCERTIFIED = "timefusion.scan.dedup_denied_uncertified" as scan.dedup_denied_uncertified;
        DEDUP_DENIED_BY_LEG = "timefusion.scan.dedup_denied_by_leg" as scan.dedup_denied_by_leg;
        DEDUP_DENIED_NEVER_CERTIFIED = "timefusion.scan.dedup_denied_never_certified" as scan.dedup_denied_never_certified;
        DEDUP_DENIED_FP_MOVED = "timefusion.scan.dedup_denied_fp_moved" as scan.dedup_denied_fp_moved;
        DEDUP_DENIED_NO_WINDOW = "timefusion.scan.dedup_denied_no_window" as scan.dedup_denied_no_window;
        DEDUP_DENIED_UNRESOLVED = "timefusion.scan.dedup_denied_unresolved" as scan.dedup_denied_unresolved;
        DEDUP_DENIED_DISABLED = "timefusion.scan.dedup_denied_disabled" as scan.dedup_denied_disabled;
        CERT_GRANTED_TOTAL = "timefusion.scan.cert_granted_total" as scan.cert_granted_total;
        /// Scans that skipped `DedupExec` over SOME (not all) in-window dates.
        /// Fires only after the whole-window verdict is denied, so it never
        /// appears in `dedup_skipped`.
        DEDUP_SKIPPED_PER_DATE = "timefusion.scan.dedup_skipped_per_date" as scan.dedup_skipped_per_date;
        /// Scans where the per-FILE skip fired: some files of an UNCERTIFIED date
        /// were proved clean and isolated, so they bypassed DedupExec.
        DEDUP_SKIPPED_PER_FILE = "timefusion.scan.dedup_skipped_per_file" as scan.dedup_skipped_per_file;
        // The single-provider fast path needs `raw.is_empty() && !bloom_pruned &&
        // date_restrict.is_none()`; the three `split_*` counters say which
        // conjunct refused it.
        TANTIVY_SCAN_CALLS = "timefusion.scan.tantivy_scan_calls" as scan.tantivy_scan_calls;
        TANTIVY_SCAN_US = "timefusion.scan.tantivy_scan_us" as scan.tantivy_scan_us_total;
        TANTIVY_URIS_US = "timefusion.scan.tantivy_uris_us" as scan.tantivy_uris_us_total;
        TANTIVY_FASTPATH = "timefusion.scan.tantivy_fastpath" as scan.tantivy_fastpath;
        TANTIVY_SPLIT_RAW = "timefusion.scan.tantivy_split_raw" as scan.tantivy_split_raw;
        TANTIVY_SPLIT_BLOOM = "timefusion.scan.tantivy_split_bloom" as scan.tantivy_split_bloom;
        TANTIVY_SPLIT_DATE = "timefusion.scan.tantivy_split_date" as scan.tantivy_split_date;
        TANTIVY_LIVE_FILES = "timefusion.scan.tantivy_live_files" as scan.tantivy_live_files_total;
        TANTIVY_RAW_FILES = "timefusion.scan.tantivy_raw_files" as scan.tantivy_raw_files_total;
        // Mirrors of the OTel-only prefilter recorders: `counter_value` reads
        // LOCAL_REGISTRY and cannot see an OTel meter, so these must also be
        // emitted here to be readable via timefusion_stats.
        PREFILTER_ATTEMPTS = "timefusion.scan.prefilter_attempts" as scan.prefilter_attempts;
        PREFILTER_USED = "timefusion.scan.prefilter_used" as scan.prefilter_used;
        PREFILTER_SKIPPED = "timefusion.scan.prefilter_skipped" as scan.prefilter_skipped;
        /// Why a slice's coverage was refused, splitting `rollup_miss_stale_coverage`.
        /// GREW = rows arrived (genuinely stale); SHRANK = rows collapsed by
        /// dedup/compaction/vacuum. Both are refused.
        ROLLUP_STALE_NO_WITNESS = "timefusion.scan.rollup_stale_no_witness" as scan.rollup_stale_no_witness;
        ROLLUP_STALE_SHRANK = "timefusion.scan.rollup_stale_shrank" as scan.rollup_stale_shrank;
        ROLLUP_STALE_GREW = "timefusion.scan.rollup_stale_grew" as scan.rollup_stale_grew;
        ROLLUP_STALE_NO_SOURCE_ROWS = "timefusion.scan.rollup_stale_no_source_rows" as scan.rollup_stale_no_source_rows;
        /// Indexes built by the reconcile BACKFILL specifically — flush,
        /// compaction and the wave reindex publish separately.
        TANTIVY_BACKFILL_BUILT = "timefusion.scan.tantivy_backfill_built" as scan.tantivy_backfill_built;
        /// Output files covered by extending an existing entry across a compaction
        /// instead of re-indexing them.
        TANTIVY_CARRIED_FORWARD = "timefusion.scan.tantivy_carried_forward" as scan.tantivy_carried_forward;
        // Inside the file-pruned scan: which of its three steps owns the time.
        PRUNED_SELECT_US = "timefusion.scan.pruned_select_us" as scan.pruned_select_us_total;
        PRUNED_BUILD_US = "timefusion.scan.pruned_build_us" as scan.pruned_build_us_total;
        PRUNED_SCAN_US = "timefusion.scan.pruned_scan_us" as scan.pruned_scan_us_total;
        PRUNED_CALLS = "timefusion.scan.pruned_calls" as scan.pruned_calls;
        PRUNED_FILES = "timefusion.scan.pruned_files" as scan.pruned_files_total;
        /// Batched manifest commits made by the backfill.
        TANTIVY_MANIFEST_COMMITS = "timefusion.scan.tantivy_manifest_commits" as scan.tantivy_manifest_commits;
        TANTIVY_MANIFEST_COMMIT_US = "timefusion.scan.tantivy_manifest_commit_us" as scan.tantivy_manifest_commit_us_total;
        // Why a dedup pass did NOT end in a certification. `cert_slice_*` are the
        // exits of `record_clean_slice` and should sum to its call count.
        CERT_SLICE_OUTSIDE_DAY = "timefusion.scan.cert_slice_outside_day" as scan.cert_slice_outside_day;
        CERT_SLICE_DIRTY = "timefusion.scan.cert_slice_dirty" as scan.cert_slice_dirty;
        // DV-visibility guard: a foreign same-path DV commit, which the URI
        // fingerprint alone cannot see.
        CERT_SLICE_DV_MOVED = "timefusion.scan.cert_slice_dv_moved" as scan.cert_slice_dv_moved;
        CERT_SLICE_PARTIAL = "timefusion.scan.cert_slice_partial" as scan.cert_slice_partial;
        CERT_SLICE_DAY_COVERED = "timefusion.scan.cert_slice_day_covered" as scan.cert_slice_day_covered;
        CERT_SLICE_FILES_PROVED = "timefusion.scan.cert_slice_files_proved" as scan.cert_slice_files_proved;
        // A certification probe that found duplicates, memoised against the file
        // set so it is not re-probed until a commit moves the fingerprint.
        CERT_PROBE_DECLINED = "timefusion.scan.cert_probe_declined" as scan.cert_probe_declined;
        // Summed dirty bins across declined dates, out of 144 per date.
        CERT_DECLINED_DIRTY_BINS = "timefusion.scan.cert_declined_dirty_bins" as scan.cert_declined_dirty_bins;
        CERT_SLICE_FILES_UNPROVEN = "timefusion.scan.cert_slice_files_unproven" as scan.cert_slice_files_unproven;
        // A whole-day proof that survived a fingerprint move because every file
        // added since sits outside the query window. Read against
        // dedup_denied_fp_moved, which was 60% of all eligible scans.
        CERT_WINDOW_SURVIVED_FP_MOVE = "timefusion.scan.cert_window_survived_fp_move" as scan.cert_window_survived_fp_move;
        // A skip granted straight from accumulated slice coverage, with no
        // whole-day certification involved. The consumer #290's retained
        // intervals never had: the per-FILE path they fed measured 27,125
        // blocked and 0 granted.
        CERT_WINDOW_FROM_SLICE_COVERAGE = "timefusion.scan.cert_window_from_slice_coverage" as scan.cert_window_from_slice_coverage;
        // Recovered rollup slices carrying #298's bounded witness, against those
        // without. Read BEFORE building the read side on it: present==0 would mean
        // the bounded witness never reaches a recovered slice and the flip would be
        // inert, which is how three read-path changes went tonight.
        ROLLUP_WITNESS_BOUNDED_PRESENT = "timefusion.scan.rollup_witness_bounded_present" as scan.rollup_witness_bounded_present;
        ROLLUP_WITNESS_BOUNDED_ABSENT = "timefusion.scan.rollup_witness_bounded_absent" as scan.rollup_witness_bounded_absent;
        // The rescue itself: a slice that failed the whole-partition compare and
        // was re-proved (or not) against its bounded witness. RESCUED climbing is
        // the fix working; STALE_TOO means the below-bound content genuinely
        // changed (dedup, compaction across the bound) and a rebuild is right.
        ROLLUP_WITNESS_BOUNDED_RESCUED = "timefusion.scan.rollup_witness_bounded_rescued" as scan.rollup_witness_bounded_rescued;
        ROLLUP_WITNESS_BOUNDED_STALE_TOO = "timefusion.scan.rollup_witness_bounded_stale_too" as scan.rollup_witness_bounded_stale_too;
        // WHY a day's rollup coverage was not usable, split the way the cert-side
        // split paid off: `stale_coverage` and `not_built` each conflate a
        // structural cause with a churn cause, and the fix differs per cause.
        // - fp_moved / epoch_moved: coverage exists but the source moved under it
        //   (fp: file set changed; epoch: `apply_rollup_hours` bumped the date).
        // - absent_invalidated: coverage was REMOVED by `apply_rollup_hours` —
        //   it existed and a write destroyed it, however far the write sat from
        //   `covered_through`. Reads as `not_built` without this counter.
        // - absent_never_built: no coverage and no invalidation record — the
        //   build lane genuinely has not gotten there.
        ROLLUP_STALE_FP_MOVED = "timefusion.scan.rollup_stale_fp_moved" as scan.rollup_stale_fp_moved;
        ROLLUP_STALE_EPOCH_MOVED = "timefusion.scan.rollup_stale_epoch_moved" as scan.rollup_stale_epoch_moved;
        ROLLUP_COVERAGE_ABSENT_INVALIDATED = "timefusion.scan.rollup_coverage_absent_invalidated" as scan.rollup_coverage_absent_invalidated;
        ROLLUP_COVERAGE_ABSENT_NEVER_BUILT = "timefusion.scan.rollup_coverage_absent_never_built" as scan.rollup_coverage_absent_never_built;
        // The post-plan ticket recheck failing (dml.rs): coverage moved DURING
        // planning — a race, not a structural gap; the two need different fixes.
        ROLLUP_TICKET_RECHECK_FAILED = "timefusion.scan.rollup_ticket_recheck_failed" as scan.rollup_ticket_recheck_failed;
        // A fingerprint move that KEPT span-disjoint coverage instead of discarding
        // the day. Read against `cert_coverage_reset`: coverage that only ever
        // resets is coverage that never accumulates, which is what held
        // `cert_slice_files_proved` at 9 against 2,194 unproven.
        CERT_COVERAGE_RETAINED = "timefusion.scan.cert_coverage_retained" as scan.cert_coverage_retained;
        CERT_COVERAGE_RESET = "timefusion.scan.cert_coverage_reset" as scan.cert_coverage_reset;
        // `declined`: the isolated non-conforming leg exceeded
        // `timefusion_read_sort_unordered_leg_max_mb`, so the union advertises no
        // ordering and `ORDER BY ts DESC LIMIT n` becomes a blocking sort.
        // `no_claim`: not one file declares an ordering.
        ORDERING_REPAIR_APPLIED = "timefusion.scan.ordering_repair_applied" as scan.ordering_repair_applied;
        ORDERING_REPAIR_DECLINED = "timefusion.scan.ordering_repair_declined" as scan.ordering_repair_declined;
        ORDERING_REPAIR_NO_CLAIM = "timefusion.scan.ordering_repair_no_claim" as scan.ordering_repair_no_claim;
        // Memory leg. `unsorted`: the caller could not claim ordered partitions;
        // `rejected`: it claimed and `try_with_sort_information` refused.
        MEM_ORDERING_DECLARED = "timefusion.scan.mem_ordering_declared" as scan.mem_ordering_declared;
        MEM_ORDERING_UNSORTED = "timefusion.scan.mem_ordering_unsorted" as scan.mem_ordering_unsorted;
        MEM_ORDERING_REJECTED = "timefusion.scan.mem_ordering_rejected" as scan.mem_ordering_rejected;
        // `insert_batch` accepts nullable field additions, so one new optional field
        // makes a bucket `schema_diverse` and retracts ordering for the whole query.
        MEM_SORT_RETRACTED = "timefusion.scan.mem_sort_retracted" as scan.mem_sort_retracted;
        MEM_SORT_RETRACTED_SCHEMA_DIVERSE = "timefusion.scan.mem_sort_retracted_schema_diverse" as scan.mem_sort_retracted_schema_diverse;
        // Why a CERTIFIED file still could not skip: one uncertified file with no
        // statistics blocks the whole scan; overlap blocks only the files it touches.
        CERT_SKIP_BLOCKED_NO_STATS = "timefusion.scan.cert_skip_blocked_no_stats" as scan.cert_skip_blocked_no_stats;
        CERT_SKIP_BLOCKED_OVERLAP = "timefusion.scan.cert_skip_blocked_overlap" as scan.cert_skip_blocked_overlap;
        CERT_SKIP_FILES = "timefusion.scan.cert_skip_files" as scan.cert_skip_files;
        // Why `record_certification` refused, split by the failing conjunct.
        CERT_REFUSED_DROPPED = "timefusion.scan.cert_refused_dropped" as scan.cert_refused_dropped;
        CERT_REFUSED_INCOMPLETE = "timefusion.scan.cert_refused_incomplete" as scan.cert_refused_incomplete;
        CERT_REFUSED_EMPTY = "timefusion.scan.cert_refused_empty" as scan.cert_refused_empty;
        CERT_REFUSED_FP_MOVED = "timefusion.scan.cert_refused_fp_moved" as scan.cert_refused_fp_moved;
        CERT_DWELL_TOTAL = "timefusion.scan.cert_dwell_total" as scan.cert_dwell_total;
        FAST_RESOLVE_HITS = "timefusion.scan.fast_resolve_hits" as scan.fast_resolve_hits;
        FAST_RESOLVE_MISSES = "timefusion.scan.fast_resolve_misses" as scan.fast_resolve_misses;
        PROVIDER_CACHE_HITS = "timefusion.scan.provider_cache_hits" as scan.provider_cache_hits;
        PROVIDER_CACHE_MISSES = "timefusion.scan.provider_cache_misses" as scan.provider_cache_misses;
        PROVIDER_CACHE_EVICTIONS = "timefusion.scan.provider_cache_evictions" as scan.provider_cache_evictions;
        PROVIDER_BUILD_ABANDONED = "timefusion.scan.provider_build_abandoned" as scan.provider_build_abandoned;
        PROVIDER_BUILD_TOTAL = "timefusion.scan.provider_build_total" as scan.provider_build_total;
        PROVIDER_SCAN_TOTAL = "timefusion.scan.provider_scan_total" as scan.provider_scan_total;
        BOUNDED_OTEL_SCAN_CANDIDATES = "timefusion.scan.bounded_otel_scan_candidates" as scan.bounded_otel_scan_candidates;
        BOUNDED_OTEL_SCAN_REJECTIONS = "timefusion.scan.bounded_otel_scan_rejections" as scan.bounded_otel_scan_rejections;
        WIDE_SCAN_OVERSIZE_TOTAL = "timefusion.scan.wide_scan_oversize_total" as scan.wide_scan_oversize_total;
        // full-set has no LIMIT early termination and charges the 2 GiB per-query budget.
        DEDUP_BOUNDED_TOTAL = "timefusion.scan.dedup_bounded_total" as scan.dedup_bounded_total;
        DEDUP_FULL_SET_TOTAL = "timefusion.scan.dedup_full_set_total" as scan.dedup_full_set_total;
        DEDUP_WINNER_COMPACTIONS_TOTAL = "timefusion.scan.dedup_winner_compactions_total" as scan.dedup_winner_compactions_total;
        DEDUP_WINNER_COMPACTION_ROWS_DROPPED = "timefusion.scan.dedup_winner_compaction_rows_dropped" as scan.dedup_winner_compaction_rows_dropped;
        MEM_PLAN_TOTAL = "timefusion.scan.mem_plan_total" as scan.mem_plan_total;
        PGWIRE_TOTAL = "timefusion.scan.pgwire_total" as pgwire.queries_total;
        DECODE_BYTES_TOTAL = "timefusion.scan.decode_bytes_total" as scan_decode.bytes_total;
        DECODE_PRESSURE_THROTTLED = "timefusion.scan.decode_pressure_throttled" as scan_decode.pressure_throttled_total;
        // Heavy-query admission: a spilling-sort query admitted to the pool, one
        // that had to WAIT for a slot, and one that waited out the timeout. queued
        // climbing with a healthy admitted rate is orderly backpressure; timeouts
        // climbing means K is too low or queries too slow.
        HEAVY_QUERY_ADMITTED = "timefusion.scan.heavy_query_admitted" as scan.heavy_query_admitted;
        HEAVY_QUERY_ORDERED_MOR_ADMITTED = "timefusion.scan.heavy_query_ordered_mor_admitted" as scan.heavy_query_ordered_mor_admitted;
        HEAVY_QUERY_QUEUED = "timefusion.scan.heavy_query_queued" as scan.heavy_query_queued;
        HEAVY_QUERY_QUEUE_TIMEOUT = "timefusion.scan.heavy_query_queue_timeout" as scan.heavy_query_queue_timeout;
    }
    // Per-reason breakdown of `PREFILTER_SKIPPED`.
    reasons {
        PREFILTER_SKIP_EMPTY_INDEX = "timefusion.scan.prefilter_skipped.empty_index" as scan.prefilter_skipped_empty_index when "empty_index";
        PREFILTER_SKIP_LOW_SELECTIVITY = "timefusion.scan.prefilter_skipped.low_selectivity" as scan.prefilter_skipped_low_selectivity when "low_selectivity";
        PREFILTER_SKIP_FIELD_COVERAGE_GAP = "timefusion.scan.prefilter_skipped.field_coverage_gap" as scan.prefilter_skipped_field_coverage_gap when "field_coverage_gap";
        PREFILTER_SKIP_FIELD_REPRESENTATION = "timefusion.scan.prefilter_skipped.field_representation" as scan.prefilter_skipped_field_representation when "field_representation_mismatch";
        PREFILTER_SKIP_MUTABLE_VISIBILITY = "timefusion.scan.prefilter_skipped.mutable_visibility" as scan.prefilter_skipped_mutable_visibility when "mutable_visibility";
        PREFILTER_SKIP_NO_INDEX = "timefusion.scan.prefilter_skipped.no_index" as scan.prefilter_skipped_no_index when "delta_no_index";
        PREFILTER_SKIP_NO_USABLE_INDEX = "timefusion.scan.prefilter_skipped.no_usable_index" as scan.prefilter_skipped_no_usable_index when "delta_no_usable_index";
        PREFILTER_SKIP_CAP_EXCEEDED_ONE_INDEX = "timefusion.scan.prefilter_skipped.cap_exceeded_one_index" as scan.prefilter_skipped_cap_exceeded_one_index when "delta_cap_exceeded_one_index";
        PREFILTER_SKIP_CAP_EXCEEDED_COMBINED = "timefusion.scan.prefilter_skipped.cap_exceeded_combined" as scan.prefilter_skipped_cap_exceeded_combined when "delta_cap_exceeded_combined";
        PREFILTER_SKIP_NO_HITS_RETURNED = "timefusion.scan.prefilter_skipped.no_hits_returned" as scan.prefilter_skipped_no_hits_returned when "delta_no_hits_returned";
        PREFILTER_SKIP_DELTA_ERROR = "timefusion.scan.prefilter_skipped.delta_error" as scan.prefilter_skipped_delta_error when "delta_error";
    }
    derived {
        CERT_DWELL_SECS_TOTAL = "timefusion.scan.cert_dwell_secs_total" via scan.cert_dwell_secs_avg;
        PROVIDER_BUILD_US_TOTAL = "timefusion.scan.provider_build_us_total" via scan.provider_build_us_avg;
        PROVIDER_SCAN_US_TOTAL = "timefusion.scan.provider_scan_us_total" via scan.provider_scan_us_avg;
        MEM_PLAN_US_TOTAL = "timefusion.scan.mem_plan_us_total" via scan.mem_plan_us_avg;
        WIDE_SCAN_SELECTED_MB = "timefusion.scan.wide_scan_selected_mb" via scan.wide_scan_selected_mb_p99;
    }
    }

    pub fn prefilter_skip_metric(reason: &str) -> Option<&'static str> {
        PREFILTER_SKIP_REASONS.iter().find(|(r, _)| *r == reason).map(|(_, m)| *m)
    }
}

/// Why a recovered rollup slice cannot be verified. Two dimensions over the SAME
/// slices, each summing to the aggregate: `UnverifiableReason` (why the row
/// witness is missing) and `UnverifiableFate` (what recovery did with it). Gauge
/// names are derived from these lists, so there is no third list to maintain.
pub mod rollup_unverifiable {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

    use itertools::Itertools;

    /// Declares a bucket enum together with its gauge array, so a variant cannot
    /// be added without a gauge and a row appearing for it.
    macro_rules! buckets {
        ($(#[$m:meta])* $name:ident => $gauges:ident { $($(#[$vm:meta])* $variant:ident => $label:literal),+ $(,)? }) => {
            $(#[$m])*
            #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
            pub enum $name { $($(#[$vm])* $variant,)+ }

            impl $name {
                /// Declaration order is PRECEDENCE order (lowest matching variant
                /// wins) and the row order in `timefusion_stats`.
                pub const ALL: &'static [Self] = &[$(Self::$variant,)+];

                pub const fn as_str(self) -> &'static str {
                    match self { $(Self::$variant => $label,)+ }
                }

                /// Count one slice into a per-pass tally.
                pub fn bump(self, tally: &mut Tally) {
                    tally.resize(Self::ALL.len(), 0);
                    tally[self as usize] += 1;
                }

                /// `label=count,…` for the recovery log line.
                pub fn render(tally: &Tally) -> String {
                    Self::ALL.iter().enumerate().map(|(index, bucket)| format!("{}={}", bucket.as_str(), tally.get(index).copied().unwrap_or_default())).join(",")
                }
            }

            static $gauges: [AtomicU64; $name::ALL.len()] = [const { AtomicU64::new(0) }; $name::ALL.len()];
        };
    }

    buckets! {
        /// Every route to `source_rows == None` in the tag replay. Exhaustive by
        /// construction: `classify_witness` is the ONLY producer of `source_rows`.
        UnverifiableReason => BY_REASON {
            /// The same (project, slice, generation, fingerprint) ALSO appears
            /// with a witness — a rewrite stripped the tag off some files only.
            /// Outranks the parse reasons: the fix is a repair, not a republish.
            MixedWitness => "mixed_witness",
            /// `TAG_SOURCE_ROWS` present but not an `i64`.
            TagUnparsable => "witness_tag_unparsable",
            /// Parses, but negative — `-1` is the sentinel a build writes when
            /// the source reported no count.
            TagNegative => "witness_negative",
            /// No `TAG_SOURCE_ROWS` at all (present-with-null-value lands here
            /// too, since Delta tags are `HashMap<String, Option<String>>`).
            TagAbsent => "witness_tag_absent",
            /// MUST STAY 0: unreachable by construction. Nonzero means the two
            /// passes disagree about the population and the split can no longer
            /// be trusted to sum.
            Unattributed => "unattributed",
        }
    }

    buckets! {
        /// What the recovery pass did with the slice, in code order. Only the
        /// last arm is ever queued for republish.
        UnverifiableFate => BY_FATE {
            /// `TimeSlice::new` or the date derivation refused the tags.
            InvalidSlice => "fate_invalid_slice",
            /// The journal does not call the slice complete, so recovery skips it
            /// before the witness-less list is built.
            JournalIncomplete => "fate_journal_incomplete",
            /// Stored generation no longer matches the current spec — the
            /// witness-less subset of the pass-wide `stale_generation` field.
            StaleGeneration => "fate_stale_generation",
            /// Passed every filter and was handed to the republish queue.
            QueuedForRepublish => "fate_queued_for_republish",
        }
    }

    /// The witness a slice's `TAG_SOURCE_ROWS` carries, or why it has none. The
    /// single producer of `source_rows`.
    ///
    /// ```
    /// use timefusion::database::rollup_unverifiable::{classify_witness, UnverifiableReason::*};
    /// assert_eq!(classify_witness(Some("42")), Ok(42));
    /// assert_eq!(classify_witness(None), Err(TagAbsent));
    /// assert_eq!(classify_witness(Some("")), Err(TagUnparsable));
    /// assert_eq!(classify_witness(Some("12.5")), Err(TagUnparsable));
    /// assert_eq!(classify_witness(Some("-1")), Err(TagNegative));
    /// ```
    pub fn classify_witness(raw: Option<&str>) -> Result<u64, UnverifiableReason> {
        match raw.map(str::parse::<i64>) {
            None => Err(UnverifiableReason::TagAbsent),
            Some(Err(_)) => Err(UnverifiableReason::TagUnparsable),
            Some(Ok(rows)) => u64::try_from(rows).map_err(|_| UnverifiableReason::TagNegative),
        }
    }

    /// Files whose IDENTITY tags are incomplete (missing/unparsable project,
    /// generation, fingerprint or slice bounds); such files are dropped from the
    /// tagged set entirely. Monotonic across passes — read its RATE, not its value.
    pub static IDENTITY_TAG_INCOMPLETE: AtomicU64 = AtomicU64::new(0);

    /// A per-pass tally, indexed by variant.
    pub type Tally = Vec<u64>;

    /// Publish one source's tallies, replacing that source's previous numbers.
    /// Kept per SOURCE because a plain `store` would make the exported number
    /// last-source-wins; the published rows are fleet totals.
    pub fn publish(source: &str, reasons: &Tally, fates: &Tally) {
        use std::sync::{LazyLock, Mutex};
        type PerSource = std::collections::HashMap<String, (Tally, Tally)>;
        static BY_SOURCE: LazyLock<Mutex<PerSource>> = LazyLock::new(Default::default);

        let Ok(mut by_source) = BY_SOURCE.lock() else { return };
        by_source.insert(source.to_owned(), (reasons.clone(), fates.clone()));
        let sum = |pick: fn(&(Tally, Tally)) -> &Tally, gauges: &[AtomicU64]| {
            for (index, gauge) in gauges.iter().enumerate() {
                gauge.store(by_source.values().map(|tallies| pick(tallies).get(index).copied().unwrap_or_default()).sum(), Relaxed);
            }
        };
        sum(|(reasons, _)| reasons, &BY_REASON);
        sum(|(_, fates)| fates, &BY_FATE);
    }

    /// Every gauge this module exposes, as `timefusion_stats` rows — the one
    /// source of truth for the key list; `pg_compat` iterates it rather than
    /// naming keys. `total` is the fleet aggregate, the sum of either dimension.
    pub fn gauge_rows() -> impl Iterator<Item = (String, u64)> {
        let reasons = UnverifiableReason::ALL.iter().map(|reason| reason.as_str()).zip(BY_REASON.iter());
        let fates = UnverifiableFate::ALL.iter().map(|fate| fate.as_str()).zip(BY_FATE.iter());
        let total = BY_REASON.iter().map(|gauge| gauge.load(Relaxed)).sum::<u64>();
        [
            ("rollup_unverifiable_total".to_owned(), total),
            ("rollup_unverifiable_identity_tag_incomplete_total".to_owned(), IDENTITY_TAG_INCOMPLETE.load(Relaxed)),
        ]
        .into_iter()
        .chain(reasons.chain(fates).map(|(label, gauge)| (format!("rollup_unverifiable_{label}"), gauge.load(Relaxed))))
    }
}

/// High-water-mark decode gauges surfaced via `timefusion_stats`. Hand-rolled
/// atomics because `metrics::Gauge` has no `fetch_max`.
#[derive(Debug, Default)]
pub struct DecodeGauges {
    pub decode_peak_batch_bytes: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight: std::sync::atomic::AtomicU64,
    pub decode_polls_inflight_peak: std::sync::atomic::AtomicU64,
}

/// Counters/gauges surfaced via `timefusion_stats`, recorded through
/// `metrics::counter!()`/`histogram!()` (see `scan_metric_names`); `decode`
/// high-water marks are the one exception (`DecodeGauges`).
#[derive(Debug, Default)]
pub struct ScanMetrics {
    pub decode: DecodeGauges,
}

impl ScanMetrics {
    /// One gated decode entered; the caller must pair it with `decode_end`.
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
    /// the window was never eligible; `Granted` with no skip means a leg refused
    /// one that was.
    pub(crate) fn record_scan(&self, duration_us: u64, shape: ScanShape, verdict: DedupSkipVerdict) {
        use scan_metric_names::*;
        let ScanShape { skipped_delta, has_mem, has_delta, fast_resolve_hit, skip_dedup: dedup_skip } = shape;
        metrics::counter!(SCANS_TOTAL).increment(1);
        // Counted only where a Delta leg was actually read.
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
                    // rather than adding a panic path on the scan hot path.
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

    /// A certification ended: record its survival in the dwell histogram. The end
    /// is observed, not caused, so the dwell is an upper bound.
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

    /// Record a pgwire end-to-end query duration.
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
/// and when.
#[derive(Clone, Debug)]
struct Certification {
    fp: u64,
    since: std::time::Instant,
    /// Exact file visibility proved clean, using table-relative paths.
    files: Arc<crate::read::CountFiles>,
    /// The partition has MOVED, so this can never again grant the whole-partition
    /// skip — but its file list is still true of the files it names, which the
    /// per-FILE skip needs. Do not delete a certification on fingerprint move.
    stale: bool,
}

/// Clean dedup slices accumulated toward certifying one (project, table, date)
/// partition: disjoint sorted `[start, end)` intervals, all proved over `fp`.
/// A unit that drops rows or a fingerprint move resets the accumulation —
/// evidence over a moved file set is void. Memory-only.
#[derive(Clone, Debug)]
struct SliceCoverage {
    fp: u64,
    intervals: Vec<(i64, i64)>,
    /// Exact file visibility when these intervals were proved.
    files: crate::read::CountFiles,
}

/// Which clean intervals survive a set of newly-arrived files.
///
/// Sound only where `timestamp` is a dedup key: then a duplicate group shares one
/// timestamp, so a file spanning `[min, max]` can only mint duplicates inside
/// `[min, max]` and an interval disjoint from every new span stays proved.
///
/// `None` means "cannot decide, reset": a file with no span statistics overlaps
/// everything, which is the same rule `partition_file_spans` states for readers.
/// `column = value` over one partition column, borrowing both.
fn eq_filter<'a>(column: &'a str, value: &'a str) -> FilterLiteral<'a> {
    (column, FilterOp::Eq, FilterValue::Scalar(value))
}

fn retain_clean_intervals(intervals: &[(i64, i64)], new_spans: &[Option<(i64, i64)>]) -> Option<Vec<(i64, i64)>> {
    if new_spans.iter().any(Option::is_none) {
        return None;
    }
    let spans: Vec<(i64, i64)> = new_spans.iter().flatten().copied().collect();
    // Half-open interval vs inclusive span, matching `certify_files_within_slice`.
    Some(intervals.iter().copied().filter(|&(s, e)| !spans.iter().any(|&(min, max)| s <= max && min < e)).collect())
}

/// Merge `[start, end)` into a sorted vec of disjoint half-open intervals.
fn merge_clean_interval(intervals: &mut Vec<(i64, i64)>, interval: (i64, i64)) {
    intervals.push(interval);
    intervals.sort_unstable();
    *intervals = intervals.iter().copied().coalesce(|a, b| if b.0 <= a.1 { Ok((a.0, a.1.max(b.1))) } else { Err((a, b)) }).collect();
}

// Isolated tables for projects with their own S3 bucket.
pub type CustomProjectTables = Arc<RwLock<HashMap<(String, String), Arc<RwLock<DeltaTable>>>>>;

// Per-table (keyed by storage URL), per-date set of live file URIs at the last
// successful z-order optimize. Backs the ZOrder idempotence guard.
type ZOrderFilesets = Arc<RwLock<HashMap<String, HashMap<chrono::NaiveDate, HashSet<String>>>>>;
/// Per-(project_id, table_name) DML serialization mutexes — see `Database::dml_lock`.
type DmlLocks = Arc<dashmap::DashMap<(String, String), Arc<tokio::sync::Mutex<()>>>>;

/// Last durable state of the rollup journal. Both `None` until the first
/// successful store, so a fresh process always writes once.
#[derive(Default, Debug)]
struct PersistedRollupJournal {
    digest: Option<u64>,
    at: Option<std::time::Instant>,
}

type RollupSourceKey = (String, String, String);
type RollupCoverageKey = (String, String, String, String);
type RollupSliceCoverageKey = (String, String, String, i64, i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollupOutputEvidence {
    /// No publication count. Date summaries still require independent slice output proof.
    Unknown,
    /// Verified zero-row output, subject to source and publication evidence checks.
    Empty,
    Files(std::num::NonZeroU32),
}

impl RollupOutputEvidence {
    fn from_file_count(files: impl TryInto<u32>) -> Self {
        files.try_into().ok().and_then(std::num::NonZeroU32::new).map_or(Self::Unknown, Self::Files)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RollupCoverage {
    source_fp: u64,
    /// The source partition's invalidation epoch when this was built; `None` for
    /// SLICE coverage, which has none and proves freshness with the row witness
    /// instead. `None` rather than `0` so "never invalidated" stays
    /// distinguishable from "not measured".
    source_epoch: Option<u64>,
    generation: String,
    /// The source DATE partition's `num_records` sum when this slice was built.
    /// `None` is unverifiable and the read path refuses it.
    source_rows: Option<u64>,
    /// The same sum over only the files lying wholly below `covered_through`,
    /// mirroring `TAG_SOURCE_ROWS_BELOW`. Ingest past the bound cannot move it,
    /// so a slice that fails the whole-partition compare can still be re-proved
    /// against this — the rescue that stops a tail append from staling the
    /// morning. `None` on pre-#298 slices, which simply keep today's behaviour.
    source_rows_below: Option<u64>,
    /// Exclusive upper bound on the source timestamps this build aggregated.
    /// `day_start + DAY_MICROS` for a sealed day, less for a day still being
    /// written. Stored, never recomputed: it is time-varying for today, and the
    /// build, the read and the ticket re-check must all use the same bound.
    covered_through: i64,
    /// The measure columns this cell's files actually MATERIALIZED, from
    /// `TAG_MEASURES`. `None` is a legacy cell carrying no such evidence.
    measures: Option<HashSet<String>>,
    /// The input file set this cell was AGGREGATED from, deletion vectors
    /// included. Deliberately NOT `source_fp`, which hashes paths alone and so
    /// cannot see a deletion vector superseding an `Add` under an unchanged path.
    /// `None` only ever DECLINES the no-op-rebuild skip.
    content_fp: Option<u64>,
    /// Output evidence is independent of input freshness; zero files alone prove nothing.
    output: RollupOutputEvidence,
}

impl RollupCoverage {
    fn matches_day(&self, fingerprint: u64, epoch: u64) -> bool {
        self.source_fp == fingerprint && self.source_epoch == Some(epoch)
    }

    fn matches_slice(&self, rows: Option<u64>, rows_below: Option<u64>) -> bool {
        crate::rollup::slice_coverage_agrees(&[self.source_rows], rows)
            || self.source_rows_below.zip(rows_below).is_some_and(|(built, current)| built == current)
    }

    fn empty_publication(&self) -> Option<crate::maintenance_coordinator::Publication> {
        (self.output == RollupOutputEvidence::Empty).then_some(())?;
        Some(crate::maintenance_coordinator::Publication {
            source_fingerprint: self.source_fp,
            generation: self.generation.clone(),
            rows: 0,
            source_rows: self.source_rows,
            source_rows_below: self.source_rows_below,
            evidence: Some(crate::maintenance_coordinator::PublicationEvidence {
                content_fp: self.content_fp?,
                measures: self.measures.as_ref()?.iter().cloned().sorted_unstable().collect(),
            }),
        })
    }
}

#[derive(Debug)]
/// Coverage to re-check before a rollup substitute is used. The bound travels
/// with the ticket because it is the build's, not something the re-check may
/// recompute — see `RollupCoverage::covered_through`.
pub(crate) struct RollupReadTicket {
    /// `(coverage key, source fingerprint, source epoch, generation)`. No bound:
    /// the date fingerprint is re-checked over the WHOLE partition.
    dates: Vec<(RollupCoverageKey, u64, u64, String)>,
    slices: Vec<(RollupSliceCoverageKey, u64, String)>,
    output: RollupOutputTicket,
}

#[derive(Debug)]
struct RollupOutputTicket {
    source: String,
    target: String,
    lookup_project: String,
    accepted: maintain::RollupOutputCoverage,
}

/// A matched query's rollup substitute: the SQL to plan, the `Aggregate` node it
/// is substituted for, and the coverage ticket to re-check before it is used.
#[derive(Debug)]
pub(crate) struct RollupRewrite {
    pub sql: String,
    pub grain: String,
    /// `"full"` when the rollup answered the whole window, `"hybrid"` when raw
    /// fringes or a live tail were unioned in.
    pub mode: &'static str,
    /// The `Aggregate` this rewrite replaces, verbatim, so the caller can swap it
    /// in place.
    pub matched: datafusion::logical_expr::LogicalPlan,
    pub ticket: RollupReadTicket,
}
/// Per-physical-table count of flush/ingest committers QUEUED on the commit lock.
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

/// Get a Delta table from unified tables by table_name
pub async fn get_unified_delta_table(unified_tables: &UnifiedTables, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
    unified_tables.read().await.get(table_name).cloned()
}

/// Should `resolve_*_table` call `update_state()` on the cached snapshot?
/// Biased toward refreshing: skip only when this process's own writes prove the
/// snapshot current, since a background flusher may have committed.
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

/// Refresh `table`'s snapshot. Never hold the write lock across `update_state()` —
/// it does object-store IO and convoys planning. Clone-update-swap instead; the
/// swap is version-guarded so the shared handle never regresses.
pub(crate) async fn refresh_table_snapshot(table: &Arc<RwLock<DeltaTable>>, incremental: bool) -> std::result::Result<Option<u64>, deltalake::DeltaTableError> {
    let _timed = crate::observability::TimedSection::new("delta_snapshot_refresh");
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
    // Carry the materialized file list forward over the catch-up range instead of
    // the O(active files) re-materialize `update_state` pays; falls back to the
    // full update when not applicable.
    // Bound before `state.as_mut()`: an Arc clone, so it cannot borrow `fresh`.
    let log_store = fresh.log_store();
    let advanced = if incremental && let Some(state) = fresh.state.as_mut() {
        // Non-fatal: the full update_state below re-attempts the same IO.
        state.advance_catchup(log_store.as_ref(), REFRESH_APPEND_CATCHUP_MAX_GAP).await.unwrap_or_else(|e| {
            debug!("incremental catch-up failed, falling back to full update_state: {e}");
            false
        })
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

/// Reconcile table properties existing tables predate, idempotently and
/// best-effort: a failed property commit must never block table load.
pub(crate) async fn ensure_table_properties(table: DeltaTable, desired: HashMap<String, String>) -> DeltaTable {
    let current = table.snapshot().ok().map(|s| s.metadata().configuration().clone()).unwrap_or_default();
    if desired.iter().all(|(k, v)| current.get(k) == Some(v)) {
        return table;
    }
    match table.clone().set_tbl_properties().with_properties(desired.clone()).with_commit_properties(base_commit_properties()).await {
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
/// Absent/unparseable `date=` segment ⇒ `true`; `None` cutoff means no limit.
use crate::storage::date_partition_within as within_recency;

/// Whether `uri`'s `date=YYYY-MM-DD` Hive partition overlaps the `[lo, hi]`
/// microsecond window, at day granularity. Absent/unparseable date ⇒ `true`
/// (conservative). Open bounds (`i64::MIN`/`MAX`) match everything on that side.
fn uri_date_in_window(uri: &str, lo: i64, hi: i64) -> bool {
    let Some(d) = crate::storage::date_partition_of(uri) else {
        return true;
    };
    let to_date = |ts: i64, open: i64| (ts != open).then(|| chrono::DateTime::from_timestamp_micros(ts)).flatten().map(|dt| dt.date_naive());
    to_date(lo, i64::MIN).is_none_or(|l| d >= l) && to_date(hi, i64::MAX).is_none_or(|h| d <= h)
}

/// The cache-key prefix for a table: its URI minus any `?endpoint=...` query
/// string and trailing slash. File URIs are relativized against this.
fn table_cache_prefix(table_uri: &str) -> &str {
    table_uri.split('?').next().unwrap_or(table_uri).trim_end_matches('/')
}

/// Relativize an absolute file URI against a `table_cache_prefix`. `None` on
/// prefix mismatch. Shared by the warm and evict paths so the two cannot desync.
fn relativize_to_prefix(prefix: &str, uri: &str) -> Option<object_store::path::Path> {
    uri.strip_prefix(prefix).map(|rel| object_store::path::Path::from(rel.trim_start_matches('/')))
}

/// The table's path within its bucket (`"s3://bucket/tf/tbl"` → `"tf/tbl"`).
/// Cache inserts happen below delta-rs's `PrefixStore`, so keys must be
/// bucket-relative; a table-relative key makes evictions silent no-ops.
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

/// Select and order files for `warm_cache_for_uris`, as `(path, recent)` pairs.
/// Footers warm for every returned file; full-file warming additionally requires
/// `recent`. Ordered newest date-partition first so a truncated boot-time warm
/// still covers the hot partitions; undated files sort last.
fn select_warm_paths(
    uris: Vec<String>, prefix: &str, warm_all_footers: bool, cutoff: Option<chrono::NaiveDate>,
) -> (Vec<(object_store::path::Path, bool)>, usize) {
    let (mut paths, dropped): (Vec<(object_store::path::Path, bool)>, Vec<()>) = uris
        .into_iter()
        .filter(|u| u.ends_with(".parquet"))
        .filter_map(|u| {
            let recent = within_recency(&u, cutoff);
            (warm_all_footers || recent).then_some((u, recent))
        })
        .partition_map(|(u, recent)| match relativize_to_prefix(prefix, &u) {
            Some(path) => itertools::Either::Left((path, recent)),
            // Prefix mismatch: warming this file would address the wrong key.
            None => itertools::Either::Right(()),
        });
    // Assumes 10-char ISO dates (date=YYYY-MM-DD, lexically sortable); a missing
    // segment keys as "" and sorts last under Reverse.
    let date_key = |p: &object_store::path::Path| {
        let s = p.as_ref();
        s.find("date=").and_then(|i| s.get(i + 5..i + 15)).unwrap_or("").to_string()
    };
    paths.sort_by_cached_key(|(p, _)| std::cmp::Reverse(date_key(p)));
    (paths, dropped.len())
}

/// Row values of a Utf8View/Utf8 column, or `None` if it is neither.
fn str_col_rows(column: &datafusion::arrow::array::ArrayRef) -> Option<Box<dyn Iterator<Item = Option<&str>> + '_>> {
    use datafusion::arrow::array::{StringArray, StringViewArray};

    let any = column.as_any();
    any.downcast_ref::<StringViewArray>()
        .map(|arr| Box::new(arr.iter()) as Box<dyn Iterator<Item = Option<&str>> + '_>)
        .or_else(|| any.downcast_ref::<StringArray>().map(|arr| Box::new(arr.iter()) as _))
}

/// First row's `project_id`, if the batch carries the column.
pub fn extract_project_id(batch: &RecordBatch) -> Option<String> {
    let idx = batch.schema().fields().iter().position(|f| f.name() == "project_id")?;
    str_col_rows(batch.column(idx))?.next().flatten().map(str::to_string)
}

/// Split a batch row-wise by its `project_id` column into per-project sub-batches.
/// Routing must follow each row's own `project_id` — reading only row 0 (as
/// [`extract_project_id`] does) misroutes every other row. Null/absent falls back
/// to `default_project`; a homogeneous batch is returned as-is (no copy). Groups
/// come back in sorted key order for deterministic table writes.
pub fn partition_batch_by_project(batch: RecordBatch, default_project: &str) -> DFResult<Vec<(String, RecordBatch)>> {
    use std::collections::BTreeMap;

    use datafusion::arrow::{array::UInt32Array, compute::take_record_batch};

    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(vec![]);
    }
    let Some(col_idx) = batch.schema().fields().iter().position(|f| f.name() == "project_id") else {
        return Ok(vec![(default_project.to_string(), batch)]);
    };
    let column = batch.column(col_idx);

    // The block scopes the iterator's borrow of `batch`.
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    {
        let Some(rows) = str_col_rows(column) else { return Ok(vec![(default_project.to_string(), batch)]) };
        for (i, pid) in rows.enumerate() {
            let pid = pid.unwrap_or(default_project);
            match groups.get_mut(pid) {
                Some(v) => v.push(i as u32),
                None => drop(groups.insert(pid.to_string(), vec![i as u32])),
            }
        }
    }

    if groups.len() == 1 {
        let pid = groups.into_keys().next().unwrap();
        return Ok(vec![(pid, batch)]);
    }

    groups.into_iter().map(|(pid, indices)| Ok((pid, take_record_batch(&batch, &UInt32Array::from(indices))?))).collect()
}

/// Minimal `SessionState` for delta-rs `OptimizeBuilder`. Must be passed via
/// `.with_session_state(...)`: delta-rs's default `DeltaSessionConfig` turns
/// `schema_force_view_types` ON, which makes the kernel's `unshredded_variant()`
/// schema mismatch on Variant columns.
fn build_optimize_session_state(
    target_partitions: usize, runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> datafusion::execution::session_state::SessionState {
    build_optimize_session_state_tuned(target_partitions, runtime_env, None, None)
}

/// A sort whose caller runs few enough concurrent bins that
/// `MAINTENANCE_MAX_PARTITIONS` — sized for the many-sibling-sorters case — does
/// not apply to it.
#[derive(Clone, Copy)]
struct UncappedSort {
    partitions: usize,
    /// Per-partition up-front reservation, where the 32 MB default is wrong
    /// (repair's unspillable `SortPreservingMergeExec` is per-partition).
    reservation_bytes: Option<usize>,
}

/// `batch_override` shrinks the sort's indivisible admission unit; merge memory
/// scales with fan-in x batch, so callers sorting one huge file need a smaller
/// `ConfigOptions::set` fails only on an unknown key — i.e. when a DataFusion upgrade renames one of
/// the load-bearing memory settings below. Silently reverting to the default is how that goes
/// unnoticed, so say it out loud.
pub(crate) fn set_or_warn(options: &mut datafusion::config::ConfigOptions, key: &str, value: &str) {
    if let Err(e) = options.set(key, value) {
        warn!(%key, %value, error = %e, "DataFusion rejected a config key; it keeps its default");
    }
}

/// batch than the packing bins they share a pool with. `uncapped` lifts
/// `MAINTENANCE_MAX_PARTITIONS` and pins parallelism — see [`UncappedSort`].
fn build_optimize_session_state_tuned(
    target_partitions: usize, runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>, batch_override: Option<&str>, uncapped: Option<UncappedSort>,
) -> datafusion::execution::session_state::SessionState {
    use datafusion::{execution::SessionStateBuilder, prelude::SessionConfig};
    // A batch is the sort's indivisible admission unit, so small pools must be
    // able to admit one.
    let batch_size = batch_override.unwrap_or_else(|| crate::config::try_config().map_or("2048", |c| c.derived.maintenance_batch_size()));
    let mut cfg = match uncapped {
        None => maintenance_session_config(SessionConfig::new(), batch_size, target_partitions),
        Some(u) => maintenance_session_config(SessionConfig::new(), batch_size, usize::MAX).with_target_partitions(u.partitions),
    };
    if let Some(reservation) = uncapped.and_then(|u| u.reservation_bytes) {
        set_or_warn(cfg.options_mut(), "datafusion.execution.sort_spill_reservation_bytes", &reservation.to_string());
    }
    let mut state = SessionStateBuilder::new().with_config(cfg).with_runtime_env(runtime_env).with_default_features().build();
    // `hash_bucket` is ours, not a builtin, and these states skip
    // `register_custom_functions`; without it every sharded rewrite fails to plan.
    datafusion::execution::FunctionRegistry::register_udf(&mut state, Arc::new(crate::read::functions::hash_bucket_udf())).ok();
    state
}

/// Days of rollup coverage counting back from yesterday with no hole, minimised
/// (not averaged) over active projects.
const CONTIGUITY_HORIZON_DAYS: u64 = 30;

/// `(worst, worst_project, median)` contiguous answered days across projects.
pub(crate) fn min_contiguous_days<'a>(
    covered: &HashSet<(String, chrono::NaiveDate)>, source: &HashSet<(String, chrono::NaiveDate)>, today: chrono::NaiveDate, active_projects: &HashSet<&'a str>,
) -> (u64, Option<&'a str>, u64) {
    // A day the SOURCE never held counts as answered; otherwise one sparse tenant
    // pins the fleet minimum at 0 forever.
    let answered = |project: &str, date: chrono::NaiveDate| {
        let key = (project.to_owned(), date);
        covered.contains(&key) || !source.contains(&key)
    };
    // Capped at the horizon; otherwise a quiet tenant, for whom every
    // pre-first-row day is "answered", scores the age of the epoch.
    let per_project = active_projects
        .iter()
        .map(|project| {
            let days = (1u64..=CONTIGUITY_HORIZON_DAYS)
                .take_while(|back| today.checked_sub_days(chrono::Days::new(*back)).is_some_and(|date| answered(project, date)))
                .count() as u64;
            (days, *project)
        })
        .collect_vec();
    // Name tie-break keeps the reported laggard stable across sweeps.
    let worst = per_project.iter().copied().min();
    let mut days = per_project.iter().map(|(days, _)| *days).collect_vec();
    (worst.map_or(0, |(days, _)| days), worst.map(|(_, project)| project), median_contiguous_days(&mut days))
}

/// Does this file prove its partition holds no rows? Only an explicit zero counts;
/// missing stats is unknown and must read as non-empty.
fn partition_file_is_empty(num_records: Option<i64>) -> bool {
    num_records == Some(0)
}

/// Are client queries starving in the heavy-admission queue RIGHT NOW?
///
/// True when the queue-timeout counter moved since `seen` (swapped to the
/// current reading as a side effect). A timeout there is a customer query that
/// waited its whole budget and got nothing — the one signal that says
/// background work must stand aside, whatever memory and flush think. Prod
/// 2026-09-24: the drain's first REAL rebuild wave (the no-op-skip fix made it
/// real) pushed a 24-hour count from 1.7s to 84s and timed out a third of all
/// heavy queries, while every other health signal stayed green.
pub fn queries_starving(seen: &std::sync::atomic::AtomicU64) -> bool {
    let now = crate::observability::counter_value("timefusion.scan.heavy_query_queue_timeout");
    now > seen.swap(now, std::sync::atomic::Ordering::Relaxed)
}

/// Why the rollup backfill must not enqueue this pass, or `None` to proceed.
///
/// The queue ceiling was the only brake when the 2026-09-22 drain ran: it
/// bounded how much work QUEUED while nothing bounded what the running drain did
/// to the box. Its S3 commits starved flush commits past the watchdog and the
/// MemBuffer hit its hard limit twice in one day. Backfill is the one lane whose
/// work is entirely deferrable, so it yields to every ingest-health signal:
///
/// - `flush_stalls_delta`: a flush commit stalled since the LAST pass. The
///   passes run about a minute apart, so this is a one-minute-old signal that
///   the store is contended at the exact point that rejects customer data.
/// - `buffer_pressure_pct`: the same yield line the hygiene lane uses — flush
///   needs the box before maintenance does.
/// - `replay_complete`: before the tag replay finishes, coverage maps are
///   partial and every partition reads as a hole; enqueueing then would rebuild
///   the fleet to fix nothing.
///
/// Pure so the policy is testable without a box under pressure.
///
/// ```
/// use timefusion::database::backfill_enqueue_deferred as deferred;
/// // Healthy: proceed.
/// assert_eq!(deferred(100, false, 0, 10, true, false), None);
/// // One stalled flush since the last pass parks the drain.
/// assert_eq!(deferred(100, false, 1, 10, true, false), Some("flush_stalled"));
/// // Ingest filling: flush outranks backfill.
/// assert_eq!(deferred(100, false, 0, 70, true, false), Some("buffer_pressure"));
/// // A heavy-queue timeout since the last pass parks the drain for a customer.
/// assert_eq!(deferred(100, false, 0, 10, true, true), Some("queries_starving"));
/// // Partial coverage maps: a hole is not evidence yet.
/// assert_eq!(deferred(100, false, 0, 10, false, false), Some("replay_incomplete"));
/// // The queue ceiling still binds, unless coverage is short of the window.
/// assert_eq!(deferred(25_000, false, 0, 10, true, false), Some("queue_ceiling"));
/// assert_eq!(deferred(25_000, true, 0, 10, true, false), None);
/// ```
pub fn backfill_enqueue_deferred(
    pending: usize, coverage_short: bool, flush_stalls_delta: u64, buffer_pressure_pct: u32, replay_complete: bool, queries_starving: bool,
) -> Option<&'static str> {
    const BACKFILL_PENDING_CEILING: usize = 25_000;
    if flush_stalls_delta > 0 {
        return Some("flush_stalled");
    }
    // Client queries outrank backfill the same way flush does: a heavy-queue
    // timeout is a customer waiting a full budget for nothing.
    if queries_starving {
        return Some("queries_starving");
    }
    if buffer_pressure_pct >= crate::config::HYGIENE_BUFFER_YIELD_PCT {
        return Some("buffer_pressure");
    }
    if !replay_complete {
        return Some("replay_incomplete");
    }
    (pending >= BACKFILL_PENDING_CEILING && !coverage_short).then_some("queue_ceiling")
}

/// Which declared tiers each candidate day is missing, so the caller can enqueue
/// only the absent tier.
fn tiers_missing_per_day(
    candidates: &[(String, chrono::NaiveDate)], covered_per_tier: &[(usize, HashSet<(String, chrono::NaiveDate)>)],
) -> HashMap<(String, chrono::NaiveDate), Vec<usize>> {
    covered_per_tier
        .iter()
        .flat_map(|(index, covered)| candidates.iter().filter(|key| !covered.contains(*key)).map(|key| (key.clone(), *index)))
        .into_group_map()
}

/// Parallelism cap for every maintenance session: each partition's `ExternalSorter` reserves
/// `sort_spill_reservation_bytes` up-front from the bounded maintenance pool, which a
/// query-derived partition count exhausts before the sort can start.
const MAINTENANCE_MAX_PARTITIONS: usize = 2;

/// The `date=` partition a repair bin sits in. Empty string (no date in the path) sorts last.
fn repair_bin_date(files: &[String]) -> &str {
    files.first().and_then(|p| path_partition_value(p, "date")).unwrap_or("")
}

/// The value of one Hive-style `key=value` path segment, or `None` if the
/// path carries no such segment.
fn path_partition_value<'a>(path: &'a str, key: &str) -> Option<&'a str> {
    path.split('/').find_map(|segment| segment.strip_prefix(key)?.strip_prefix('='))
}

/// Parallelism for the repair sort specifically: repair runs exactly one bin at a time, so the
/// concurrency premise behind `MAINTENANCE_MAX_PARTITIONS` does not apply.
const REPAIR_SORT_PARTITIONS: usize = 16;

/// Up-front merge reservation per repair-sort partition. The merge cannot spill, so
/// reserving its share first forces the sorter to spill early instead of growing an
/// unspillable tail. Total reserved is partitions x reservation.
const REPAIR_SORT_RESERVATION_BYTES: usize = 512 * 1024 * 1024;

/// Maximum wall time for one ordinary coordinator unit.
const COORDINATOR_STANDARD_UNIT_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(crate::maintenance_coordinator::operation_deadline_secs(crate::maintenance_coordinator::Operation::Dedup));

/// Maximum wall time for one coordinator file-rewrite unit; sized so a 256 MiB run
/// finishes with commit margin.
const COORDINATOR_FILE_REWRITE_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(crate::maintenance_coordinator::operation_deadline_secs(crate::maintenance_coordinator::Operation::Repair));

/// Last-resort guard catching a hang OUTSIDE a unit (wedged planning scan or
/// dispatcher). Must stay WELL above the longest per-unit clock or it becomes the
/// real deadline.
const COORDINATOR_LOOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2 * crate::maintenance_coordinator::MAX_OPERATION_DEADLINE_SECS);

/// How often coverage recovery re-reads the tiers' Delta logs. Long on purpose: it
/// notices slow-moving facts, it does not schedule work.
const COVERAGE_RECOVERY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3600);

/// How long before a unit whose source is still buffered could possibly run: the
/// slice's end plus the write path's finalization delay, floored at five seconds
/// and backed off for a sealed slice whose flush remains stuck. A flat five-second
/// retry let unavailable inputs consume thousands of claims and journal commits
/// per ten minutes while doing no work.
fn buffered_source_retry_delay(slice: crate::maintenance_coordinator::TimeSlice, now_micros: i64, attempts: u32) -> std::time::Duration {
    const FLOOR: std::time::Duration = std::time::Duration::from_secs(5);
    let earliest = slice.end_micros.saturating_add(crate::maintenance_coordinator::FINALIZATION_DELAY_MICROS);
    let until_finalized = u64::try_from(earliest.saturating_sub(now_micros)).map_or(FLOOR, std::time::Duration::from_micros);
    until_finalized.max(FLOOR).max(crate::database::maintain::transient_retry_backoff(attempts))
}

/// Absolute wall-clock ceiling for the lanes that hold a rewrite permit others are
/// waiting on; `None` means the idle window alone governs, which is right wherever
/// a unit costs only its worker.
fn coordinator_operation_lifetime_cap(operation: crate::maintenance_coordinator::Operation) -> Option<std::time::Duration> {
    use crate::maintenance_coordinator::Operation;
    match operation {
        Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair => Some(coordinator_operation_timeout(operation) * 4),
        Operation::BaseRollup | Operation::DerivedRollup | Operation::Dedup => None,
    }
}

fn coordinator_operation_timeout(operation: crate::maintenance_coordinator::Operation) -> std::time::Duration {
    use crate::maintenance_coordinator::Operation;
    match operation {
        Operation::HotPacking | Operation::SealedConsolidation | Operation::Repair => COORDINATOR_FILE_REWRITE_TIMEOUT,
        // Rollup cost is set by INPUT FILE COUNT, so narrowing the slice shrinks
        // nothing and bisection never converges.
        Operation::BaseRollup | Operation::DerivedRollup => COORDINATOR_FILE_REWRITE_TIMEOUT,
        Operation::Dedup => COORDINATOR_STANDARD_UNIT_TIMEOUT,
    }
}

/// Physical run targets for coordinator packing. Both tiers sit at 256 MiB: large enough to
/// satisfy the sealed-file-count bound, small enough that one rewrite fits its deadline.
pub(crate) const COORDINATOR_HOT_TARGET_BYTES: i64 = 256 * 1024 * 1024;
const COORDINATOR_SEALED_TARGET_BYTES: i64 = 256 * 1024 * 1024;

/// Current-day packing's share of the light rewrite pool while sealed debt is
/// pending. The guard is bypassed when sealed debt is zero, so this is a
/// catch-up allocation rather than a permanent loss of concurrency.
pub(crate) fn hot_packing_permits(light_rewrite_permits: usize) -> usize {
    (light_rewrite_permits / 3).max(1).min(light_rewrite_permits.max(1))
}

/// Rows per decode batch for any session that reads the wide OTel schema. The
/// parquet decode buffer is not pool-accounted, so EVERY such session must set
/// this or it inherits DataFusion's much larger default.
pub(crate) const WIDE_ROW_DECODE_BATCH_SIZE: &str = "2048";

/// Config shared by every delta-rs maintenance session. `schema_force_view_types=false`
/// keeps Variant columns as `Binary` so delta_kernel's unshredded-variant check passes;
/// the sort-spill floor lets a sort spill instead of erroring under the bounded pool.
fn maintenance_session_config(base: datafusion::prelude::SessionConfig, batch_size: &str, target_partitions: usize) -> datafusion::prelude::SessionConfig {
    let mut cfg = base.set_bool("datafusion.execution.parquet.schema_force_view_types", false);
    for (k, v) in [
        ("datafusion.execution.batch_size", batch_size),
        ("datafusion.execution.sort_spill_reservation_bytes", "33554432"),
        ("datafusion.execution.skip_physical_aggregate_schema_check", "true"),
    ] {
        set_or_warn(cfg.options_mut(), k, v);
    }
    let parts = if target_partitions == 0 { MAINTENANCE_MAX_PARTITIONS } else { target_partitions.min(MAINTENANCE_MAX_PARTITIONS) };
    cfg.with_target_partitions(parts)
}

/// Session for delta-rs *write* execution. Must carry delta-rs's `DeltaPlanner`: the
/// write path wraps its input in a `MetricObserver` node only that planner can plan.
/// `batch_size` is the sort's indivisible admission unit, so callers with a small
/// memory pool must pass a small value.
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

/// Spawn a background cron task running `job` at each wall-clock occurrence of
/// `schedule`. A tick whose predecessor is still running is skipped, not queued;
/// slow but healthy runs are never aborted — only shutdown forces an abort.
fn spawn_cron_job<F, Fut>(name: &'static str, schedule: &str, cancel: Arc<CancellationToken>, job: F)
where
    F: Fn() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    spawn_cron_job_on(name, schedule, cancel, None, job);
}

/// Schedule like [`spawn_cron_job`], but execute each job body on `executor` so its CPU and I/O
/// stay off the foreground runtime. The wall-clock timer stays on the caller's runtime.
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
    // Observability only — slow-but-progressing work is allowed to finish.
    const LONG_RUNNING_WARN_THRESHOLD: std::time::Duration = std::time::Duration::from_secs(600);
    info!("{name} job scheduled with cron expression: {schedule}");
    tokio::spawn(async move {
        let mut running: Option<tokio::task::JoinHandle<()>> = None;
        let mut running_since: Option<std::time::Instant> = None;
        let mut skips = 0u32;
        loop {
            let now = chrono::Utc::now();
            let dur = match cron.find_next_occurrence(&now, false) {
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
                    // Detached so a wedged run can never freeze this loop; overlapping
                    // runs are skipped rather than piled up (jobs are idempotent).
                    if running.as_ref().is_some_and(|h| !h.is_finished()) {
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
                    skips = 0;
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

/// `spawn_cron_job` for the "clone `db`, run an async method on it" shape.
fn spawn_db_cron<F, Fut>(db: &Arc<Database>, name: &'static str, schedule: &str, cancel: Arc<CancellationToken>, job: F)
where
    F: Fn(Arc<Database>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let db = Arc::clone(db);
    spawn_cron_job(name, schedule, cancel, move || job(Arc::clone(&db)));
}

/// `commitInfo.info` key for the WAL watermark.
const WAL_WATERMARK_KEY: &str = "timefusion.wal_watermark";

/// `commitInfo.info` marker on a wave commit composed ENTIRELY of in-place DV-dedup bins: it adds
/// no rows, so reconcile skips re-minting Dedup. A commit WITHOUT the marker fails toward minting.
pub(crate) const DV_DEDUP_COMMIT_KEY: &str = "timefusion.dv_dedup";

/// `commitInfo.info` marker naming which maintenance lane authored a rewrite
/// commit. Delta history is the one durable record of write volume (counters
/// reset per deploy), and without the lane every OPTIMIZE reads the same —
/// which is how 300M rewritten rows/day went unattributed.
pub(crate) const LANE_COMMIT_KEY: &str = "timefusion.lane";

/// Serialize a per-shard watermark to the JSON map stored in `commitInfo.info[WAL_WATERMARK_KEY]`.
/// Only shards with a position are included — an absent shard means "no constraint from this
/// commit".
fn serialize_watermark_to_json(watermark: &crate::write::DeltaWatermark, project_id: &str, table_name: &str) -> serde_json::Map<String, serde_json::Value> {
    let mut map: serde_json::Map<String, serde_json::Value> = watermark
        .iter()
        .enumerate()
        .filter_map(|(shard, pos)| pos.map(|p| (shard.to_string(), serde_json::json!({ "block_id": p.block_id, "offset": p.offset }))))
        .collect();
    if !map.is_empty() {
        // Unified-table tenants share ONE Delta log and walrus positions are per-topic, so an
        // unscoped watermark would advance another tenant's cursor past unreplayed entries.
        map.insert(WATERMARK_TOPIC_KEY.to_string(), serde_json::Value::String(wal_topic(project_id, table_name)));
    }
    map
}

/// Key inside the watermark object naming the topic that produced it.
const WATERMARK_TOPIC_KEY: &str = "topic";

/// Key holding the MULTI-topic map of a cross-project coalesced commit:
/// `{ "<project>:<table>": <single-topic map> }`. Single-topic commits keep the flat shape.
const WATERMARK_TOPICS_KEY: &str = "topics";

/// Serialize the watermarks of every (project, table) carried by ONE commit. Topics with no
/// positions are dropped; exactly one surviving topic ⇒ flat single-topic shape.
fn serialize_watermarks_to_json(
    entries: impl IntoIterator<Item = (String, String, crate::write::DeltaWatermark)>,
) -> serde_json::Map<String, serde_json::Value> {
    let per_topic: Vec<(String, serde_json::Map<String, serde_json::Value>)> = entries
        .into_iter()
        .filter_map(|(project_id, table_name, wm)| {
            let map = serialize_watermark_to_json(&wm, &project_id, &table_name);
            (!map.is_empty()).then(|| (wal_topic(&project_id, &table_name), map))
        })
        .collect();
    if per_topic.len() <= 1 {
        return per_topic.into_iter().next().map(|(_, map)| map).unwrap_or_default();
    }
    // Two units for the same topic in one commit must not silently drop one; per-shard MAX keeps
    // the survivor no further behind than either contributor (behind replays, ahead loses rows).
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
    // Non-numeric keys are the "topic" key — already present and identical.
    for (shard, value) in from.into_iter().filter(|(shard, _)| shard.parse::<usize>().is_ok()) {
        if into.get(&shard).is_none_or(|existing| pos(existing) < pos(&value)) {
            into.insert(shard, value);
        }
    }
}

/// Delete `datafusion-*` spill directories left behind by a previous process — `DiskManager` cleans
/// up on `Drop`, which a SIGKILL skips. Safe only because the WAL dir flock rules out a second live
/// TimeFusion.
fn reap_orphaned_spill_dirs(spill_dir: &std::path::Path) {
    // Snapshot the orphan list synchronously, BEFORE this env's DiskManager exists:
    // enumerating lazily on the detached thread can yank a live spill dir mid-sort.
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

fn reap_orphaned_spill_dirs_blocking(dir: &std::path::Path, orphans: Vec<std::path::PathBuf>) {
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
        info!("spill reap: removed {dirs} orphaned spill dir(s), {} MB freed from {dir:?}", bytes / (1024 * 1024));
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

/// Logical WAL topic for a (project, table) — must match `wal.rs`'s topic naming.
fn wal_topic(project_id: &str, table_name: &str) -> String {
    format!("{project_id}:{table_name}")
}

/// Identities of the batch sets a commit contains, so a later boot can decline to write rows it
/// can prove are already durable.
///
/// **This record must NEVER advance a WAL cursor** — it may only decline a write. A digest is an
/// IDENTITY, not a range, so advancing a cursor from one loses interleaved writes.
const LANDED_DIGESTS_KEY: &str = "timefusion.landed_digests";

/// `{ "<project>:<table>": ["<hex digest>", …] }` — topic-scoped like the watermark, since
/// unified-table tenants share one Delta log.
fn serialize_landed_digests_to_json(
    entries: impl IntoIterator<Item = (String, String, crate::write::LandedDigest)>,
) -> serde_json::Map<String, serde_json::Value> {
    let by_topic = entries.into_iter().map(|(p, t, digest)| (wal_topic(&p, &t), serde_json::Value::String(hex::encode(digest)))).into_group_map();
    by_topic.into_iter().map(|(topic, digests)| (topic, serde_json::Value::Array(digests))).collect()
}

/// Inverse of [`serialize_landed_digests_to_json`], for one topic. Malformed entries are skipped:
/// an unreadable digest is an identity we will not match, which costs a duplicate, never a loss.
fn parse_landed_digests_from_json(info: &HashMap<String, serde_json::Value>, project_id: &str, table_name: &str) -> Vec<crate::write::LandedDigest> {
    let Some(list) =
        info.get(LANDED_DIGESTS_KEY).and_then(|v| v.as_object()).and_then(|m| m.get(&wal_topic(project_id, table_name))).and_then(|v| v.as_array())
    else {
        return Vec::new();
    };
    list.iter().filter_map(|v| hex::decode(v.as_str()?).ok()?.try_into().ok()).collect()
}

/// Inverse of `serialize_watermark_to_json`. Out-of-range or malformed shards are dropped silently
/// so future writers can add fields without breaking older readers.
fn parse_watermark_from_json(
    info: &HashMap<String, serde_json::Value>, shards: usize, project_id: &str, table_name: &str,
) -> Vec<Option<walrus_rust::WalPosition>> {
    let mut out = vec![None; shards];
    let Some(wm) = info.get(WAL_WATERMARK_KEY).and_then(|v| v.as_object()) else {
        return out;
    };
    let topic = wal_topic(project_id, table_name);
    // Only apply a watermark to the topic that wrote it: over-advancing a cursor loses acked
    // writes, while under-advancing only replays duplicates.
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

/// Per-shard MAX position across a sequence of commit-info maps; `None` means no observed commit
/// carried a position for that shard. Used at startup to place each shard's cursor.
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

/// Base [`CommitProperties`] for every ingest/maintenance commit. Disables the delta-rs post-commit
/// checkpoint and log-cleanup hooks: they run after `N.json` is durable, so a hook failure would
/// surface as a commit error and callers would delete parquet the landed commit references. Both
/// run out-of-band in the maintenance scheduler instead.
fn base_commit_properties() -> CommitProperties {
    CommitProperties::default().with_create_checkpoint(false).with_cleanup_expired_logs(Some(false))
}

/// Build [`CommitProperties`] carrying the watermark under [`WAL_WATERMARK_KEY`] and the landed
/// identities under [`LANDED_DIGESTS_KEY`]. `watermarks` takes every (project, table, watermark)
/// the commit carries; one with no positions is omitted and recovery skips that commit.
///
/// Both keys MUST be written in one `with_metadata` call: it REPLACES the map rather than extending
/// it, so a chained second call silently drops the watermark.
fn build_watermark_commit_properties(
    watermarks: impl IntoIterator<Item = (String, String, crate::write::DeltaWatermark)>,
    digests: impl IntoIterator<Item = (String, String, crate::write::LandedDigest)>,
) -> CommitProperties {
    match flush_commit_metadata(watermarks, digests) {
        metadata if metadata.is_empty() => base_commit_properties(),
        metadata => base_commit_properties().with_metadata(metadata),
    }
}

/// The `commitInfo.info` entries a flush commit carries. Split out of
/// [`build_watermark_commit_properties`] because `CommitProperties::app_metadata` is private.
fn flush_commit_metadata(
    watermarks: impl IntoIterator<Item = (String, String, crate::write::DeltaWatermark)>,
    digests: impl IntoIterator<Item = (String, String, crate::write::LandedDigest)>,
) -> Vec<(String, serde_json::Value)> {
    let entries = serialize_watermarks_to_json(watermarks);
    let landed = serialize_landed_digests_to_json(digests);
    [
        (!entries.is_empty()).then(|| (WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(entries))),
        (!landed.is_empty()).then(|| (LANDED_DIGESTS_KEY.to_string(), serde_json::Value::Object(landed))),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// `CommitProperties` for a compaction/dedup commit (Add + Remove); `enabled` advances the
/// materialized snapshot incrementally instead of re-materializing every active file. `lane`
/// lands in `commitInfo.info[LANE_COMMIT_KEY]`; a caller that adds its own metadata must
/// re-include the lane entry there — `with_metadata` REPLACES the map.
fn incremental_commit_properties(enabled: bool, lane: &'static str) -> CommitProperties {
    base_commit_properties().with_incremental_advance(enabled).with_metadata(lane_metadata(lane))
}

/// The `[LANE_COMMIT_KEY]` entry, for callers composing it with their own metadata.
fn lane_metadata(lane: &'static str) -> [(String, serde_json::Value); 1] {
    [(LANE_COMMIT_KEY.to_string(), serde_json::Value::String(lane.to_string()))]
}

/// Active-file URIs of `table`, restricted to files whose log path contains every marker in
/// `scope` (`partition=value` path segments); empty `scope` = whole table, unloaded snapshot =
/// empty set.
fn scoped_file_uris(table: &DeltaTable, scope: &[&str]) -> Vec<String> {
    let Ok(state) = table.snapshot() else { return Vec::new() };
    let log_store = table.log_store();
    state
        .log_data()
        .into_iter()
        .filter_map(|f| {
            let path = f.path();
            // Mirrors the fork's `object_store_path()` so these URIs stay byte-identical to
            // `get_file_uris()`.
            scope.iter().all(|m| path.contains(m)).then(|| {
                let p = object_store::path::Path::parse(path.as_ref()).unwrap_or_else(|_| object_store::path::Path::from(path.as_ref()));
                log_store.to_uri(&p)
            })
        })
        .collect()
}

/// `table.get_file_uris()`, collected into `C` — empty on an unloaded snapshot.
pub fn file_uris<C: Default + FromIterator<String>>(table: &DeltaTable) -> C {
    table.get_file_uris().map(Iterator::collect).unwrap_or_default()
}

/// True for the retryable Delta OCC conflicts, matched by substring on delta-rs Display strings.
/// Deliberately NOT a bare "version": that also matches the permanent
/// Unsupported{Reader,Writer}Version errors, which must fail fast.
pub(crate) fn is_occ_conflict_err(msg: &str) -> bool {
    ["already exists", "Commit failed", "concurrent transaction", "Metadata changed", "Transaction failed"].into_iter().any(|needle| msg.contains(needle))
}

/// Structural check for checkpoint corruption: a Parquet file ends with `[footer_len: u32 LE][PAR1]`,
/// so an overwritten or truncated object is detectable from its last 8 bytes (`tail`) alone.
fn parquet_tail_ok(tail: &[u8], file_len: u64) -> bool {
    tail.len() == 8 && &tail[4..] == b"PAR1" && {
        let footer_len = u32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]) as u64;
        footer_len > 0 && footer_len + 8 <= file_len
    }
}

/// Verify the checkpoint `_last_checkpoint` points to is readable Parquet before pruning the JSON
/// commit log behind it. `Ok(true)` = every part has a sane footer; `Ok(false)` = at least one part
/// is definitively corrupt; `Err` = couldn't determine. A missing `_last_checkpoint` is `Ok(true)`.
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

/// Exponential backoff between OCC conflict retries (150, 300, 600ms…), capped so the shift cannot
/// overflow if a caller raises its attempt limit.
pub(crate) fn occ_backoff(attempt: usize) -> tokio::time::Duration {
    tokio::time::Duration::from_millis(150 << attempt.min(6))
}

/// True for transient S3/network transport failures worth retrying at a higher level. Matched on
/// the transport phrase, not delta-rs's wrapper; notably not a bare "connection", which would also
/// match the permanent "connection refused".
fn is_transient_s3_err(msg: &str) -> bool {
    ["error sending request", "connection reset", "connection closed", "broken pipe", "reset by peer", "timed out", "timeout"]
        .into_iter()
        .any(|needle| msg.contains(needle))
}

/// Synthetic per-row source-file column on the dedup sweep's table provider, so the rewrite can
/// commit exact Remove+Add actions instead of a predicate-evaluated replace_where.
const DEDUP_FILE_COL: &str = "__tf_dedup_file";
const DEDUP_SCAN_NAME: &str = "__dedup_src";

/// Order-insensitive fingerprint of a partition's live file set, so any add/remove/rewrite changes
/// it.
///
/// FROZEN HASH: persisted in the certification sidecar. Changing the hasher silently invalidates
/// every certification.
fn partition_file_fp(files: &[String]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut files = files.to_vec();
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
    /// The partition's `num_records` sum. Counts tombstones and superseded merge-on-read versions,
    /// which a decoded row count does not — both sides of `rollup::slice_coverage_agrees` must use
    /// THIS computation.
    rows: i64,
    /// Sum of the partition's file sizes: the CEILING on what any unit over this partition can
    /// decode.
    pub(crate) bytes: u64,
}

impl PartitionStats {
    /// Whether this partition may hold rows in `[lo, hi)`. A partition with no timestamp statistics
    /// reports the sentinel range (`min > max`) and must count as OVERLAPPING.
    fn overlaps(&self, lo: i64, hi: i64) -> bool {
        self.min_ts > self.max_ts || (self.min_ts < hi && self.max_ts >= lo)
    }
}

/// Days of contiguous rollup coverage below which the maintenance cycle favours the rollup chain
/// over independent file debt. Read only through `coverage_is_short_for`.
pub const COVERAGE_SHORT_DAYS: u64 = 14;

/// The statistic the coverage-short switch steers by: the median of the per-project contiguous-day
/// counts (upper median on an even count, 0 when empty). Sorts in place. Deliberately not the
/// minimum — one outlier would pin the fleet in coverage-short mode forever.
pub(crate) fn median_contiguous_days(per_project: &mut [u64]) -> u64 {
    per_project.sort_unstable();
    per_project.get(per_project.len() / 2).copied().unwrap_or(0)
}

/// THE coverage-short predicate, shared by the server and `maintenance_sim` so the simulated and
/// served cycle selections cannot disagree on threshold or statistic.
pub(crate) fn coverage_is_short_for(median_days: u64) -> bool {
    median_days < COVERAGE_SHORT_DAYS
}

/// Is contiguous rollup coverage short enough to be worth reweighting for?
fn coverage_is_short() -> bool {
    coverage_is_short_for(crate::observability::maintenance_stats().rollup_median_contiguous_days.load(std::sync::atomic::Ordering::Relaxed))
}

/// The overlap of two coalesced range sets. Both inputs must already be sorted and disjoint.
fn intersect_ranges(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let (mut i, mut j, mut out) = (0, 0, Vec::new());
    while i < left.len() && j < right.len() {
        let (start, end) = (left[i].0.max(right[j].0), left[i].1.min(right[j].1));
        if start < end {
            out.push((start, end));
        }
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
    (0..=366).contains(&span).then(|| (0..=span).map(|d| lo_d + chrono::Duration::days(d)).collect())
}

/// The partition dates (and a 24-bit hour mask) a write batch's rows land in. `None` costs the
/// whole table its rollup coverage, so it is returned only when BOTH `timestamp` and the `date`
/// partition column are unreadable.
fn batch_hours(batch: &RecordBatch) -> Option<HashMap<String, u32>> {
    use datafusion::arrow::{
        array::AsArray,
        compute::cast,
        datatypes::{DataType, TimeUnit, TimestampMicrosecondType},
    };
    let micros = batch.column_by_name("timestamp").and_then(|column| {
        let wanted = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        let column = if column.data_type() == &wanted { column.clone() } else { cast(column, &wanted).ok()? };
        Some(
            column
                .as_primitive_opt::<TimestampMicrosecondType>()?
                .iter()
                .flatten()
                .map(|micros| (micros.div_euclid(DAY_MICROS), micros.rem_euclid(DAY_MICROS) / 3_600_000_000))
                .collect::<HashSet<_>>(),
        )
    });
    if let Some(hours) = micros {
        return Some(hours.into_iter().fold(HashMap::new(), |mut dates, (day, hour)| {
            if let Some(day_start) = chrono::DateTime::from_timestamp_micros(day * DAY_MICROS) {
                *dates.entry(day_start.date_naive().to_string()).or_default() |= 1 << hour;
            }
            dates
        }));
    }
    // Without a timestamp there is no hour to name, so the whole day is dirty.
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
    date_start_micros(date).is_some_and(|day| day < end && day.saturating_add(DAY_MICROS) > start)
}

pub(crate) fn window_hour_masks(lo: i64, hi: i64) -> Option<Vec<(String, u32)>> {
    if lo >= hi {
        return None;
    }
    let last = hi.checked_sub(1)?;
    let masks = window_dates(lo, last)?
        .into_iter()
        .filter_map(|date| {
            let day = date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros();
            let hour_of = |t: i64| t.saturating_sub(day).max(0).div_euclid(3_600_000_000).clamp(0, 23);
            Some((date.to_string(), (hour_of(lo)..=hour_of(last)).fold(0u32, |mask, hour| mask | (1 << hour))))
        })
        .collect();
    Some(masks)
}

/// Whether a commit that returned an error actually landed: delta-rs surfaces a post-commit hook or
/// snapshot-refresh failure as `Err` even though `N.json` is already durable.
#[derive(Debug)]
pub(crate) enum CommitProbe {
    /// `N.json` landed; every staged Add is active. Treat as success + drain.
    Landed,
    /// Confirmed the commit did not land; the staged parquet is safe to delete.
    NotLanded,
    /// Could not confirm. Preserve staged parquet because it may be live.
    Inconclusive,
}

/// One (project, table) flush unit handed to [`Database::insert_records_batches_coalesced`].
pub struct CoalescedWriteUnit {
    pub project_id: String,
    pub table_name: String,
    pub batches: Vec<RecordBatch>,
    pub watermark: crate::write::DeltaWatermark,
}

/// A unit whose parquet is uploaded and whose `Add` actions await the shared commit.
struct StagedUnit {
    table_ref: Arc<RwLock<DeltaTable>>,
    schema: &'static crate::schema::TableSchema,
    dirty_bins: Vec<(String, i64)>,
    adds: Vec<deltalake::kernel::Action>,
    stage_store: Arc<dyn object_store::ObjectStore>,
    /// Lets the coalesced commit mark its output verified-sorted.
    sorted: bool,
}

/// Attached to a commit error where landing could not be confirmed. The staged parquet must be left
/// in place; deleting files a landed commit references creates dangling Adds. A typed marker, not a
/// message substring: callers test it with `err.chain().any(|c| c.is::<InconclusiveCommit>())`, so a
/// `.context()` re-wrap upstream cannot silently disarm the guard.
#[derive(Debug, thiserror::Error)]
#[error("landing-unconfirmed")]
pub(crate) struct InconclusiveCommit;

impl InconclusiveCommit {
    /// Whether `e` carries the marker anywhere in its cause chain.
    pub(crate) fn marks(e: &anyhow::Error) -> bool {
        e.chain().any(|c| c.is::<Self>())
    }
}

/// Last-resort circuit breaker on a network await taken while a per-table commit lock is held. NOT
/// the primary commit timeout: firing here abandons a future mid-flight and manufactures an
/// unconfirmed landing, so the object-store client's own bounds should fire first.
const COMMIT_LOCK_OP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// A bounded in-guard commit await that did not succeed.
#[derive(Debug)]
struct CommitFailure {
    message: String,
    /// The await was ABANDONED mid-flight, so the commit may still land and no probe may be trusted
    /// to say "no" — see [`probe_after_timeout`].
    timed_out: bool,
}

/// Run one commit-path future under `bound` so a hung object-store request cannot pin a commit
/// lock. A timeout is a failure whose landing is UNKNOWN, never "did not commit": callers MUST
/// route it through `probe_after_timeout` + `CommitProbe::Inconclusive`, which leaves staged
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

/// A commit whose await TIMED OUT can never be classified `NotLanded`: the request was abandoned in
/// flight, so "I don't see our Adds" is not evidence of absence. `Landed` still passes through.
fn probe_after_timeout(probe: CommitProbe, timed_out: bool) -> CommitProbe {
    match (probe, timed_out) {
        (CommitProbe::NotLanded, true) => CommitProbe::Inconclusive,
        (probe, _) => probe,
    }
}

/// Split a coalesced commit's newly-added file URIs per project — the `project_id=<id>/` path
/// segment IS the attribution. Single-project groups pass through unfiltered.
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
    /// Lazy: the sort-merge runs when the staging writer drains it, so a batch-prepare of N units
    /// doesn't hold N sorted buckets at once.
    batches: FlushBatches,
    writer_properties: WriterProperties,
    /// Whether `batches` were actually sorted into schema order — exactly the `declare_sorted`
    /// argument `writer_properties` was built with.
    sorted: bool,
    /// Store the staged parquet lands in — used to clean it up on a terminal commit failure, since
    /// those objects have no Add/Remove and VACUUM never reclaims them.
    stage_store: Arc<dyn object_store::ObjectStore>,
    staged_writer: Option<deltalake::writer::RecordBatchWriter>,
}

/// `data_change`: true when the rewrite drops rows (dedup), false for a data-preserving compaction.
/// The conflict checker only counts `data_change: true` removals as conflicts, so a compaction
/// Remove marked true loses every OCC race to concurrent appends.
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

/// Collect matched Adds keeping at most one per path. A path can appear twice when an incremental
/// snapshot refresh crosses a checkpoint, and a duplicate silently mismatches a rewrite plan.
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

/// Drop `name` from `batch` (no-op when absent).
fn drop_batch_column(mut batch: RecordBatch, name: &str) -> RecordBatch {
    if let Ok(idx) = batch.schema().index_of(name) {
        batch.remove_column(idx);
    }
    batch
}

/// Cast Variant struct columns (Struct{BinaryView,BinaryView}) to the Binary-backed form
/// delta-kernel's `unshredded_variant()` requires on write; no-op for anything else. Applied only
/// at the Delta write so MemBuffer keeps its BinaryView layout.
fn cast_variant_columns_to_binary(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::{array::StructArray, compute::cast};
    use datafusion::arrow::datatypes::{DataType, Field};
    remap_batch_columns(batch, |_, field, col| {
        let DataType::Struct(struct_fields) = field.data_type() else { return Ok(None) };
        if !is_variant_type(field.data_type()) || !struct_fields.iter().any(|f| matches!(f.data_type(), DataType::BinaryView)) {
            return Ok(None);
        }
        let Some(struct_arr) = col.as_any().downcast_ref::<StructArray>() else { return Ok(None) };
        let (fields, casted_cols): (Vec<_>, Vec<arrow::array::ArrayRef>) = struct_arr
            .columns()
            .iter()
            .zip(struct_fields)
            .map(|(arr, f)| match f.data_type() {
                DataType::BinaryView => {
                    Ok((Arc::new(Field::new(f.name(), DataType::Binary, f.is_nullable())), cast(arr, &DataType::Binary).map_err(arrow_err)?))
                }
                _ => Ok((f.clone(), arr.clone())),
            })
            .collect::<DFResult<Vec<_>>>()?
            .into_iter()
            .unzip();
        let casted_fields: arrow::datatypes::Fields = fields.into();
        let new_field =
            Arc::new(Field::new(field.name(), DataType::Struct(casted_fields.clone()), field.is_nullable()).with_metadata(field.metadata().clone()));
        Ok(Some((new_field, Arc::new(StructArray::new(casted_fields, casted_cols, struct_arr.nulls().cloned())) as arrow::array::ArrayRef)))
    })
}

/// Rebuild `batch` with the columns for which `remap` yields a replacement `(field, array)`;
/// `None` leaves a column untouched, and an all-`None` pass returns `batch` itself (no copy).
/// Schema-level metadata is preserved.
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

/// Normalize incoming Timestamp columns whose timezone is a numeric UTC offset (`"+00:00"`, what
/// psycopg/pgwire emit for timestamptz) to the IANA name `"UTC"`, which is the only form delta-rs's
/// Arrow→Delta schema converter accepts. Retag only — the micros-since-epoch buffer is unchanged.
fn normalize_timestamp_tz(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::array::PrimitiveArray;
    use datafusion::arrow::datatypes::{
        ArrowTimestampType, DataType, Field, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    };
    // Anything that semantically means UTC; delta-rs only accepts the IANA "UTC" string.
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
        fn cast_tz<T: ArrowTimestampType>(col: &arrow::array::ArrayRef) -> Option<arrow::array::ArrayRef> {
            Some(Arc::new(col.as_any().downcast_ref::<PrimitiveArray<T>>()?.clone().with_timezone("UTC")))
        }
        let retagged = match unit {
            TimeUnit::Microsecond => cast_tz::<TimestampMicrosecondType>(col),
            TimeUnit::Millisecond => cast_tz::<TimestampMillisecondType>(col),
            TimeUnit::Nanosecond => cast_tz::<TimestampNanosecondType>(col),
            TimeUnit::Second => cast_tz::<TimestampSecondType>(col),
        }
        .ok_or_else(|| DataFusionError::Execution(format!("timestamp downcast failed for field '{}' with width {unit:?}", field.name())))?;
        let new_field =
            Arc::new(Field::new(field.name(), DataType::Timestamp(*unit, Some("UTC".into())), field.is_nullable()).with_metadata(field.metadata().clone()));
        Ok(Some((new_field, retagged)))
    })
}

/// `date` is a physical UTC partition key, never caller-owned data: rebuild it from `timestamp`
/// before every shared write path, or timestamp pruning can hide rows whose client-provided date
/// was stale or malformed.
fn derive_date_partition(batch: RecordBatch) -> DFResult<RecordBatch> {
    use arrow::array::{Date32Array, PrimitiveArray};
    use datafusion::arrow::datatypes::{
        ArrowTimestampType, DataType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    };

    let schema = batch.schema();
    let (Ok(date_idx), Ok(timestamp_idx)) = (schema.index_of("date"), schema.index_of("timestamp")) else { return Ok(batch) };
    if !matches!(schema.field(date_idx).data_type(), DataType::Date32) {
        return Err(DataFusionError::Execution("date partition column must be Date32".to_string()));
    }
    let timestamp = batch.column(timestamp_idx);
    let fail = |message: &str| DataFusionError::Execution(format!("timestamp-to-date partition conversion failed: {message}"));
    // Micros per row, nulls preserved. `scale` converts the column's unit to micros (negative
    // divides); an overflowing widen is an error, never a null.
    fn micros_of<T: ArrowTimestampType>(
        col: &arrow::array::ArrayRef, unit: &str, scale: i64, fail: impl Fn(&str) -> DataFusionError,
    ) -> DFResult<Vec<Option<i64>>> {
        col.as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| fail(&format!("{unit} downcast")))?
            .iter()
            .map(|v| {
                v.map(|v| if scale < 0 { Ok(v.div_euclid(-scale)) } else { v.checked_mul(scale).ok_or_else(|| fail(&format!("{unit} overflow"))) }).transpose()
            })
            .collect()
    }
    // All-null yields all-null dates WITHOUT dispatching on type: an entirely-null non-timestamp
    // `timestamp` column must still succeed on the flush path.
    let micros = if arrow::array::Array::null_count(timestamp.as_ref()) == timestamp.len() {
        vec![None; timestamp.len()]
    } else {
        match schema.field(timestamp_idx).data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => micros_of::<TimestampNanosecondType>(timestamp, "nanosecond", -1_000, fail)?,
            DataType::Timestamp(TimeUnit::Microsecond, _) => micros_of::<TimestampMicrosecondType>(timestamp, "microsecond", 1, fail)?,
            DataType::Timestamp(TimeUnit::Millisecond, _) => micros_of::<TimestampMillisecondType>(timestamp, "millisecond", 1_000, fail)?,
            DataType::Timestamp(TimeUnit::Second, _) => micros_of::<TimestampSecondType>(timestamp, "second", 1_000_000, fail)?,
            _ => return Err(fail("timestamp column is not a timestamp")),
        }
    };
    let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let dates = micros
        .into_iter()
        .map(|micros| {
            micros
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

/// Convert Utf8/Utf8View/LargeUtf8 columns to Variant binary StructArrays where the target schema
/// expects Variant — the table provider presents Variant columns as Utf8View for the SQL planner's
/// type check, while Delta storage expects Variant structs.
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
        // Cast BinaryView to Binary so the batch matches `delta_kernel::unshredded_variant()`.
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
        let Some(target_field) = target_schema.fields().get(idx).filter(|f| is_variant_type(f.data_type())) else { return Ok(None) };
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

/// What a `recompress_partition` call actually did: a rewrite, or a skip with its reason.
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
    /// Redacted on serialize and in `{:?}`; sqlx::FromRow bypasses serde, so loading is unaffected.
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
    /// One RuntimeEnv/memory pool shared by every session context and clone — the pool only
    /// enforces a global cap if it is global.
    runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    // The next five runtime envs are disjoint pool slices (constant total budget) so one
    // workload's long sorts can never starve another's.
    /// Heavy maintenance (optimize/dedup/recompress): bounded FairSpill pool + spill dir.
    maintenance_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Hot-tail packing, so today's compaction reserve survives long heavy rewrites.
    light_optimize_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Footer repair: sorts whole multi-hundred-MB files, kept off packing's pool.
    repair_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Coordinator execution units: one process-wide 512 MiB spill pool. Reuse stops a fresh
    /// runtime treating another worker's live spill dir as an orphan.
    coordinator_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Flush-path sorts for oversized buckets: flush is on the INGEST path and must not queue
    /// behind maintenance.
    flush_sort_runtime_env: Arc<std::sync::OnceLock<Arc<datafusion::execution::runtime_env::RuntimeEnv>>>,
    /// Caps concurrent spilling flush sorts. FairSpill gives each ~pool/N; below a viable slice the
    /// sort fails and the group is written unsorted, which disables ordering for every scan of that
    /// partition — starvation shows up as slow queries, not errors.
    flush_sort_gate: Arc<tokio::sync::Semaphore>,
    /// Memoized `build_optimize_session_state` per runtime env. A clone shares its `catalog_list`,
    /// so sites that register temporary tables must still build a fresh state.
    maintenance_session_state: Arc<std::sync::OnceLock<datafusion::execution::session_state::SessionState>>,
    light_optimize_session_state: Arc<std::sync::OnceLock<datafusion::execution::session_state::SessionState>>,
    /// Repair-only sessions, keyed by the sort parallelism they pin.
    repair_session_states: Arc<dashmap::DashMap<usize, datafusion::execution::session_state::SessionState>>,
    /// Unified tables: one Delta table per schema, partitioned by [project_id, date]
    unified_tables: UnifiedTables,
    /// Custom project tables: isolated tables for projects with their own S3 bucket
    custom_project_tables: CustomProjectTables,
    /// Lock-free (project, table) → resolved Delta table cache. The inner `Arc<RwLock<DeltaTable>>`
    /// is the same object held in the table maps above, so slow-path `update_state` is seen by
    /// hot-path callers. No eviction.
    fast_resolve_cache: FastResolveCache,
    /// Per-(project, table) sticky "Delta may hold matching files" bit — must never be falsely
    /// `false`. While false, scans skip Delta and MemBuffer is authoritative.
    delta_has_files: dashmap::DashMap<(String, String), Arc<std::sync::atomic::AtomicBool>>,
    /// Cached Delta-side `TableProvider` per (project, table) + snapshot version. Exact-version
    /// invalidation; concurrent misses single-flight via a per-key `OnceCell`. No drop eviction.
    delta_provider_cache: DeltaProviderCache,
    /// Cumulative scan-path counters, exported via `timefusion_stats`.
    pub scan_metrics: Arc<ScanMetrics>,
    batch_queue: Option<Arc<crate::write::BatchQueue>>,
    maintenance_shutdown: Arc<CancellationToken>,
    /// Every background maintenance task, so shutdown can WAIT for them rather
    /// than only signalling. `cancel_maintenance` sets a flag; without this the
    /// handles were dropped on spawn, so foyer and the delta-kernel executor were
    /// torn down underneath tasks still using them — which is what produced three
    /// separate teardown panics on 2026-09-14, one of them a SIGABRT that killed
    /// the process (a panic inside `Drop` during unwind does not unwind, it aborts).
    maintenance_tasks_tracker: tokio_util::task::TaskTracker,
    /// Cancels `maintenance_shutdown` when the last guard-holding clone drops. `None` in clones
    /// handed to long-lived background tasks — a task waiting on the token must not hold its own
    /// kill-switch alive.
    _maintenance_cancel_guard: Option<Arc<tokio_util::sync::DropGuard>>,
    /// One-shot guard so a second `preload_tables` call can't double the boot-time S3 warm burst.
    preload_started: Arc<std::sync::atomic::AtomicBool>,
    /// Barrier set once every registry table's Delta log has been REPLAYED — not when the paced
    /// body warm behind it finishes.
    preload_replay_complete: Arc<std::sync::atomic::AtomicBool>,
    preload_replay_notify: Arc<tokio::sync::Notify>,
    preload_tables_done: Arc<std::sync::atomic::AtomicU64>,
    preload_tables_total: Arc<std::sync::atomic::AtomicU64>,
    /// Runtime for CPU/IO-heavy startup and coordinator work; foreground PGWire tasks never
    /// execute here.
    maintenance_executor: Arc<std::sync::OnceLock<tokio::runtime::Handle>>,
    config_pool: Option<PgPool>,
    storage_configs: Arc<RwLock<HashMap<(String, String), StorageConfig>>>,
    /// Monotonic deadline (ns since process start) throttling storage-config refreshes to at most
    /// once every 30s, so a hot SQL path doesn't hit PG per statement.
    storage_configs_next_refresh_ns: Arc<std::sync::atomic::AtomicU64>,
    default_s3_bucket: Option<String>,
    default_s3_prefix: Option<String>,
    default_s3_endpoint: Option<String>,
    object_store_cache: Option<Arc<SharedFoyerCache>>,
    statistics_extractor: Arc<DeltaStatisticsExtractor>,
    last_written_versions: Arc<RwLock<HashMap<(String, String), u64>>>,
    /// Delta version at last dedup sweep per scheduler key; unmoved version → skip. Unbounded.
    last_dedup_versions: Arc<RwLock<HashMap<String, u64>>>,
    /// (project, table, date) → live-file-set fingerprint captured when a sweep found zero
    /// duplicates, letting a query whose window partitions all still match skip `DedupExec`. Any
    /// commit changes the file set → mismatch → dedup stays on.
    dedup_clean_fp: Arc<dashmap::DashMap<(String, String, String), Certification>>,
    /// The negative of `dedup_clean_fp`: fingerprint at which a probe found duplicates, so a
    /// dup-bearing date is not re-probed every pass. Not persisted.
    dedup_probe_declined: Arc<dashmap::DashMap<(String, String, String), u64>>,
    /// Serializes `dedup_clean_fp` snapshots with their shared atomic temp path.
    dedup_certification_persist_lock: Arc<std::sync::Mutex<()>>,
    /// Last time slice-derived certification evidence was written (`now_micros`). Debounced because
    /// `store_sidecar` re-serializes the whole ledger; whole-day grants persist immediately.
    dedup_certification_persist_at: Arc<std::sync::atomic::AtomicI64>,
    /// Clean-slice coverage accumulating toward a `dedup_clean_fp` entry.
    dedup_slice_coverage: Arc<dashmap::DashMap<(String, String, String), SliceCoverage>>,
    /// Monotonic invalidation epoch for each source `(project, table, date)`.
    rollup_source_epochs: Arc<dashmap::DashMap<RollupSourceKey, u64>>,
    /// Certified rollup generations keyed by `(project, source, target, date)`. **Has no
    /// producer**: permanently empty, so date-level routing falls through to
    /// `rollup_slice_coverage`.
    rollup_coverage: Arc<dashmap::DashMap<RollupCoverageKey, RollupCoverage>>,
    rollup_slice_coverage: Arc<dashmap::DashMap<RollupSliceCoverageKey, RollupCoverage>>,
    /// `partition_file_rows` memoized per source table, keyed by the Delta
    /// version it was computed from — a new commit invalidates it naturally.
    /// Without this the bounded-witness rescue re-materialized the add-actions
    /// batch on EVERY route call that met a stale-looking slice; after a restart
    /// that is every call until recovery catches up, and the planning drag keeps
    /// queries in flight longer, which is pool pressure by another name.
    rollup_file_rows_cache: dashmap::DashMap<String, (u64, std::sync::Arc<crate::database::maintain::PartitionFileRows>)>,
    /// Untagged live files per tier TABLE (tiers publish independently, so one shared slot would
    /// read clean while another tier still held damage). The exported gauge is the SUM over tiers.
    rollup_tier_untagged: Arc<dashmap::DashMap<String, u64>>,
    /// Rollup coverage as an explicit record. Write-only for now: the file tags remain the
    /// authority.
    coverage_ledger: Arc<crate::storage::JsonCoverageLedger>,
    /// 24-bit changed-hours mask per `(project, source, date)`. PRESENCE claims every change since
    /// the last build was observed; absence means "unknown" and forces a full rebuild.
    rollup_dirty: Arc<dashmap::DashMap<(String, String, String), u32>>,
    /// First durable invalidation time per dirty source partition; kept apart from the mask so a
    /// clean zero-mask entry has no age.
    rollup_invalidated_at: Arc<dashmap::DashMap<RollupSourceKey, u64>>,
    /// Serializes dirty-map mutation with journal snapshots, else two writers can persist out of
    /// order and lose the newer invalidation on restart.
    rollup_journal_lock: Arc<std::sync::Mutex<()>>,
    /// What was last persisted of the rollup journal, so an unmoved or not-yet-due commit can skip
    /// two `fsync`s.
    rollup_journal_persisted: Arc<std::sync::Mutex<PersistedRollupJournal>>,
    /// Coalesces the durable half of that path (see [`Self::commit_journal`]); the lock above
    /// covers only the in-memory mutation.
    journal_group_commit: Arc<crate::support::GroupCommit>,
    /// Task checkpoints shared by invalidations and rollup publications, without
    /// making a publication serialize the separate dirty-range map.
    task_journal_group_commit: Arc<crate::support::GroupCommit>,
    /// Durable slice work from the same pre-ack invalidation path as `rollup_dirty`; the
    /// finer-grained source of truth coordinator workers consume.
    maintenance_tasks: Arc<std::sync::Mutex<crate::maintenance_coordinator::TaskJournal>>,
    /// Raised whenever work is enqueued, so idle coordinator workers wake on the
    /// event instead of rediscovering it on the next poll. `Notify` is tokio's
    /// primitive for exactly this; the loop kept a one-second sleep as its only
    /// wakeup, which is a second of idle cores every time a backlog reappears.
    maintenance_work: Arc<tokio::sync::Notify>,
    maintenance_admission: crate::maintenance_coordinator::AdmissionController,
    maintenance_debt_planned_at: Arc<std::sync::atomic::AtomicI64>,
    /// Last Tantivy coverage census. Metadata-only, so it is throttled by time, not by admission.
    tantivy_census_at: Arc<std::sync::atomic::AtomicI64>,
    /// EMA of one dedup batch probe's duration, in ms, PER TABLE — probe cost varies by orders of
    /// magnitude between tables, so a shared figure would mis-size admission.
    dedup_probe_cost_ms: Arc<dashmap::DashMap<String, u64>>,
    maintenance_schedule_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Bounded exponential retry state for failed source-partition rollup builds.
    rollup_backoff: Arc<dashmap::DashMap<RollupCoverageKey, (u32, std::time::Instant)>>,
    /// Exact merge-on-read count partitions. Query threads use only the process-local front;
    /// disk/Delta loads are single-flight and bounded in the background.
    logical_count_cache: Arc<crate::read::LogicalCountCache>,
    logical_count_building: Arc<dashmap::DashSet<crate::read::CountPartition>>,
    logical_count_build_sem: Arc<tokio::sync::Semaphore>,
    /// Dirty `(project, table, date, 10-minute bin)` keys recorded only after a Delta append
    /// commits. In-memory by design: after restart the read-side DedupExec is the backstop.
    dedup_dirty_bins: Arc<dashmap::DashMap<(String, String, String, i64), ()>>,
    /// Exponential failure backoff per dedup target, else a failing partition re-runs every sweep
    /// tick. In-memory only; a restart retries once.
    dedup_backoff: Arc<dashmap::DashMap<String, (u32, std::time::Instant)>>,
    /// Paths known to carry an honest `sorting_columns` footer, and therefore excluded from repair
    /// admission; persisted to `repair_verified_sorted.txt`.
    ///
    /// The `delta-rs.optimize.sort_by` tag is NOT a `sorting_columns` footer, so untagged means
    /// suspect, not unsorted — admitting by tag would rewrite healthy files.
    repair_verified_sorted: Arc<dashmap::DashSet<String>>,
    /// Serializes appends to the persisted verified-sorted list.
    repair_verified_lock: Arc<std::sync::Mutex<()>>,
    /// Paths appended since the list was last truncated, so compaction amortizes to one rewrite per
    /// `REPAIR_VERIFIED_PERSIST_CAP` appends.
    repair_verified_appends: Arc<std::sync::atomic::AtomicUsize>,
    /// Consecutive staging failures per repair candidate — an always-failing file starves every
    /// candidate behind it; see `REPAIR_QUARANTINE_AFTER`.
    repair_failures: Arc<dashmap::DashMap<String, u32>>,
    /// Index into [`REPAIR_SORT_PARTITION_LADDER`] for candidates that exhausted the pool at higher
    /// parallelism; cleared on success.
    repair_degradation: Arc<dashmap::DashMap<String, usize>>,
    /// Caps concurrent heavy rewrites (dedup staging, optimize, consolidate, recompress). Their
    /// Arrow footprint is invisible to the DataFusion memory pool, so aggregate concurrency, not
    /// the pool, is the real OOM bound. Hot-tail waves use [`Self::light_rewrite_sem`] instead.
    maintenance_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps hot-tail wave staging. Separate from `maintenance_rewrite_sem` so a long dedup drain
    /// can't starve hot compaction. Sized to the light pool's own K.
    light_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Live-memory admission for the hygiene lane — see [`HygieneGate`].
    hygiene_gate: Arc<HygieneGate>,
    /// Set while a deploy handoff drains the write fence — see
    /// [`Database::quiesce_maintenance`].
    maintenance_quiesced: Arc<std::sync::atomic::AtomicBool>,
    /// `flush_stalled` as of the backfill census's last pass, so the pass can
    /// tell "a flush stalled in the last minute" from the counter's history.
    /// Arc for the struct's Clone; the census is the only reader.
    backfill_flush_stalls_seen: Arc<std::sync::atomic::AtomicU64>,
    /// `heavy_query_queue_timeout` as of the last starvation check, per consumer:
    /// the census and the heavy claim path each track their own last-seen so one
    /// does not eat the other's delta.
    census_query_timeouts_seen: Arc<std::sync::atomic::AtomicU64>,
    claims_query_timeouts_seen: Arc<std::sync::atomic::AtomicU64>,
    /// Caps current-day packing while sealed consolidation is pending. It is
    /// consulted only while sealed debt exists, so the full light pool remains
    /// available to hot packing after catch-up.
    hot_packing_sem: Arc<tokio::sync::Semaphore>,
    /// Light permits currently lent out of the repair lane's reservation. Only
    /// ever 0 or `repair_holdback_permits()`; see `rebalance_repair_holdback`.
    repair_holdback_lent: Arc<std::sync::atomic::AtomicU64>,
    /// Repair's decoded-BYTE budget, one permit per MiB — not a count of rewrites. Requests are
    /// clamped, so a bin larger than the budget takes all of it and runs alone.
    repair_rewrite_sem: Arc<tokio::sync::Semaphore>,
    /// Caps coordinator workers in debt work (dedup, packing, consolidation, repair), reserving the
    /// rest for rollup. Bounds occupancy, not memory: debt units hold a worker for minutes.
    maintenance_debt_slots: Arc<tokio::sync::Semaphore>,
    /// Caps workers inside units already timed out under `TaskJournal::QUARANTINE_ATTEMPTS`: bounds
    /// the wall-clock cost of a doomed unit, not its frequency.
    maintenance_quarantine_slots: Arc<tokio::sync::Semaphore>,
    /// Reserves workers for `DerivedRollup` while coverage is short — slow sealed `BaseRollup` days
    /// would otherwise occupy every freed slot.
    maintenance_derived_reserve: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent user DML MERGE-UPDATEs; each scans the time-windowed target and ungated
    /// bursts starve reads. Permits = `timefusion_dml_merge_concurrency`.
    dml_merge_sem: Arc<tokio::sync::Semaphore>,
    /// Caps concurrent Parquet decodes for WIDE scans across all queries
    /// (`timefusion_max_concurrent_scan_readers`), so a burst of wide-window dashboards can't stack
    /// decode buffers into an OOM.
    heavy_scan_sem: Arc<tokio::sync::Semaphore>,
    /// Maintenance's OWN, smaller decode gate — the reservation that keeps a
    /// full-tilt drain from occupying the whole scan/S3 path. Queries and flush
    /// keep `heavy_scan_sem`; a background rewrite can saturate at most this
    /// pool, so ingest headroom exists by construction rather than by the census
    /// reacting to stalls after the fact. The 2026-09-22 outage was maintenance
    /// scan traffic starving flush commits; the feedback throttle bounds NEW
    /// work, this bounds the work already running.
    maintenance_scan_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes the outer full and light maintenance jobs; rewrite permits alone let a waiting
    /// light job exhaust its table timeout before starting.
    maintenance_job_sem: Arc<tokio::sync::Semaphore>,
    /// Serializes in-process Delta commits per physical table (`table_lock_key`). delta-kernel's
    /// OCC checker can't evaluate `replace_where`'s timestamp predicate, so a dedup commit racing a
    /// concurrent append to the same log aborts; per-log serialization lets the rebase skip it.
    commit_locks: DmlLocks,
    /// Flush/ingest committers queued on each table's `commit_locks` entry. Durability outranks
    /// maintenance: `commit_wave` declines to enqueue while nonzero.
    flush_waiter_counts: FlushWaiterCounts,
    /// Per-table serialization for in-process DML (see `dml_lock`): concurrent merges would
    /// OCC-conflict and redo full rewrites. Queuing here leaves the table's RwLock free for readers
    /// and insert commits.
    dml_locks: DmlLocks,
    histogram_dml: Arc<dashmap::DashMap<(String, String), Arc<histogram::HistogramDmlState>>>,
    histogram_delta: Arc<histogram::HistogramDeltaCache>,
    /// At most one query-triggered proof build, including time waiting for the shared count-builder
    /// budget. Admission never queues query requests.
    histogram_proof_build: Arc<histogram::HistogramProofBuilds>,
    /// Startup and scheduled backfill share one pass per indexed table.
    tantivy_backfill_slots: Arc<dashmap::DashMap<String, Arc<tokio::sync::Semaphore>>>,
    /// Last snapshot-persist time per table URL; throttles `persist_snapshot`. The on-disk snapshot
    /// is only a boot-recovery seed, so staleness just means boot replays a few more commits.
    snapshot_persist_gate: Arc<dashmap::DashMap<String, std::time::Instant>>,
    /// Late-binding shared cell: boot creates the pgwire SessionContext before the layer exists, so
    /// it must publish through a OnceLock visible to clones captured earlier. A plain Option would
    /// leave those clones without the mem leg.
    buffered_layer: Arc<std::sync::OnceLock<Arc<crate::write::BufferedWriteLayer>>>,
    /// Per-clone override for `query_delta_only`: hides the shared layer so scans bypass the
    /// in-memory buffer.
    bypass_buffer: bool,
    /// Internal aggregate builds must never read the rollup they are rebuilding.
    bypass_rollup: bool,
    /// Plan the scan with maintenance parallelism instead of query parallelism: Parquet decode
    /// buffers are untracked by the memory pool, so a background rewrite planned at the full CPU
    /// quota fans decode out into an OOM.
    maintenance_scan: bool,
    /// Same late-binding pattern as `buffered_layer`: attached by `with_*` builders after boot has
    /// already cloned Database into sessions/planners.
    tantivy_search: Arc<std::sync::OnceLock<Arc<crate::tantivy::search::TantivySearchService>>>,
    tantivy_indexer: Arc<std::sync::OnceLock<Arc<crate::tantivy::search::TantivyIndexService>>>,
    bloom_prune: Arc<std::sync::OnceLock<Arc<crate::read::bloom_prune::BloomPruneRegistry>>>,
    /// Same late-binding pattern; populated by `start_dml_coalescer` when
    /// `TIMEFUSION_DML_COALESCE_SECS > 0`.
    dml_coalescer: Arc<std::sync::OnceLock<Arc<crate::dml::DmlCoalescer>>>,
    /// Live file URIs per (table URL, date) at the last full z-order optimize. delta-rs's ZOrder
    /// planner has no idempotence guard, so this lets `optimize_table` skip sealed partitions whose
    /// file set is unchanged. In-memory only; a restart re-z-orders each partition once.
    zorder_filesets: ZOrderFilesets,
    /// Last checkpointed version per table URL, letting the out-of-band checkpoint task skip idle
    /// tables. In-memory only; the first tick after restart checkpoints every table once.
    checkpoint_versions: Arc<dashmap::DashMap<String, u64>>,
    /// Serializes staged-intent manifest append/rewrite: orphan reconciliation's compacting rewrite
    /// racing a wave's append could drop the only targeted-cleanup record for an orphaned file.
    staged_intent_manifest_lock: Arc<std::sync::Mutex<()>>,
    // Rotation cursors: each pass's budget is smaller than its work list, so without rotating the
    // start point the tail would never be reached.
    /// Index into the light-optimize tick's debt-ordered project list.
    light_optimize_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Count of (date, project) items served by the last truncated dedup sweep.
    dedup_sweep_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// Which table a dedup tick starts with.
    dedup_table_cursor: Arc<std::sync::atomic::AtomicUsize>,
    /// One repair pass process-wide: the light pool is shared by every table, and two tables
    /// repairing concurrently can exhaust it and kill a bin that still had headroom.
    repair_pass_permit: Arc<tokio::sync::Semaphore>,
}

/// Drop the hot (today) partition from a backfill queue, returning how many URIs were removed.
/// `marker` is `None` when the skip is disabled.
fn drop_hot_partition(uris: &mut Vec<String>, marker: Option<&str>) -> u64 {
    let Some(marker) = marker else { return 0 };
    let before = uris.len();
    uris.retain(|uri| !uri.contains(marker));
    (before - uris.len()) as u64
}

/// Row groups a file of this size is assumed to hold. Conservative on purpose: under-counting
/// enlarges the per-slice floor below, which over-estimates cost — the safe direction.
fn estimated_row_groups(size_bytes: i64) -> u64 {
    const TARGET_ROW_GROUP_BYTES: i64 = 128 * 1024 * 1024;
    u64::try_from(size_bytes.max(0).div_euclid(TARGET_ROW_GROUP_BYTES).max(1)).unwrap_or(1)
}

/// A file's (overlap, span) share of `slice`. Files with no usable timestamp bounds keep their
/// full weight — unknown must never estimate cheap — and overlap is floored at one row group.
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

/// What a single `run-unit` execution produced. Phase counters are deltas of the global
/// `maintenance_stats` atomics, valid only because the CLI owns the process.
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
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Concurrency gate for user DML MERGE-UPDATEs.
    pub(crate) fn dml_merge_sem(&self) -> &Arc<tokio::sync::Semaphore> {
        &self.dml_merge_sem
    }

    pub fn unified_tables(&self) -> &UnifiedTables {
        &self.unified_tables
    }

    pub fn custom_project_tables(&self) -> &CustomProjectTables {
        &self.custom_project_tables
    }

    /// Lock the maintenance task journal, recovering from mutex poisoning.
    ///
    /// A blocking mutex that can occupy a runtime worker for a whole checkpoint.
    /// The read path must never take it.
    fn journal(&self) -> crate::observability::Watched<std::sync::MutexGuard<'_, crate::maintenance_coordinator::TaskJournal>> {
        let wait = crate::observability::BlockWatch::new("journal_lock_wait");
        let guard = crate::support::lock(&self.maintenance_tasks);
        drop(wait);
        crate::observability::Watched::new("journal_hold", guard)
    }

    /// Change admission through the live journal, never a second journal writer.
    pub(crate) fn set_rollup_build_policy(&self, source: &str, table: &str, policy: crate::maintenance_coordinator::RollupBuildPolicy) -> Result<()> {
        self.journal().set_rollup_build_policy(source, table, policy)
    }

    pub(crate) fn rollup_build_policies(&self, source: &str) -> Result<Vec<crate::maintenance_coordinator::RollupPolicyView>> {
        let schema = get_schema(source).ok_or_else(|| anyhow::anyhow!("unknown rollup source {source}"))?;
        let journal = self.journal();
        Ok(schema
            .rollups
            .iter()
            .map(|spec| {
                let table = spec.table_name(source);
                let status = journal.rollup_policy_status(&table);
                crate::maintenance_coordinator::RollupPolicyView { table, parent: spec.derive_from.clone(), status }
            })
            .collect())
    }

    pub async fn perform_delta_update(
        &self, table_name: &str, project_id: &str, predicate: Option<datafusion::logical_expr::Expr>,
        assignments: Vec<(String, datafusion::logical_expr::Expr)>, session: Arc<dyn datafusion::catalog::Session>,
    ) -> Result<u64, DataFusionError> {
        crate::dml::perform_delta_update(self, table_name, project_id, predicate, assignments, session).await
    }

    pub async fn perform_delta_delete(
        &self, table_name: &str, project_id: &str, predicate: Option<datafusion::logical_expr::Expr>, session: Arc<dyn datafusion::catalog::Session>,
    ) -> Result<u64, DataFusionError> {
        crate::dml::perform_delta_delete(self, table_name, project_id, predicate, session).await
    }

    fn build_storage_options(&self) -> HashMap<String, String> {
        let storage_options = self.config.aws.build_storage_options(self.default_s3_endpoint.as_deref());

        let safe_options: HashMap<_, _> = storage_options.iter().filter(|(k, _)| !k.contains("secret") && !k.contains("password")).collect();
        debug!("Storage options configured: {:?}", safe_options);
        storage_options
    }

    /// Writer properties for a Delta write at compression tier `zstd_level` (also recorded in
    /// footer metadata so re-sweeps can skip already-target-tier files).
    ///
    /// `declare_sorted` must be `true` only for paths that really sort rows by the schema's
    /// sort keys — the footer is trusted, not verified.
    fn create_writer_properties(&self, schema: &crate::schema::TableSchema, zstd_level: i32, declare_sorted: bool) -> WriterProperties {
        build_writer_properties(&self.config.parquet, schema, zstd_level, declare_sorted, None)
    }

    /// Writer properties for a rewrite that has measured its input's row width, so row groups
    /// are capped by decoded bytes rather than by an estimate.
    pub(crate) fn create_writer_properties_measured(
        &self, schema: &crate::schema::TableSchema, zstd_level: i32, declare_sorted: bool, bytes_per_row: Option<u64>,
    ) -> WriterProperties {
        build_writer_properties(&self.config.parquet, schema, zstd_level, declare_sorted, bytes_per_row)
    }

    /// WriterProperties for DML rewrite paths (delta-rs would otherwise default to SNAPPY).
    ///
    /// `sorted` must match the caller's actual plan: `true` only for the DV-merge path, `false`
    /// for `UpdateBuilder`/`DeleteBuilder`. A lying footer breaks `DedupExec`.
    pub(crate) fn dml_writer_properties(&self, table_name: &str, sorted: bool) -> WriterProperties {
        let schema = schema_or_default(table_name);
        self.create_writer_properties(schema, self.config.parquet.timefusion_zstd_compression_level, sorted)
    }

    /// Updates a DeltaTable, retrying for eventual consistency.
    async fn update_table(&self, table: &Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) -> Result<()> {
        const MAX_RETRIES: u32 = 5;
        let mut retries = 0;
        loop {
            match refresh_table_snapshot(table, self.config.maintenance.timefusion_incremental_snapshot).await {
                Ok(version) => {
                    if let Some(version) = version {
                        debug!("Updated table for {}/{} to version {}", project_id, table_name, version);
                        self.last_written_versions.write().await.insert(table_key(project_id, table_name), version);
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

    /// One-time DDL for the config schema. Call during construction only, never on reload.
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

    /// Load storage configurations from PostgreSQL. Credential columns prefixed with `enc:v1:`
    /// are decrypted in place; plaintext rows pass through with a warning.
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

    /// Create a new Database with explicit config. Prefer this over `new()` in tests.
    pub async fn with_config(cfg: Arc<AppConfig>) -> Result<Self> {
        crate::storage::prune_stale(&Self::delta_snapshot_dir(&cfg), crate::storage::SNAPSHOT_MAX_AGE);
        let dedup_dirty_bins = Arc::new(dashmap::DashMap::new());
        // Re-key any bin recorded at a different width. The queue is the ONLY record that a bin
        // needs dedup, so bins are remapped (over-approximating) rather than discarded.
        let width_micros = crate::database::compact::bin_micros();
        let mut remapped = 0usize;
        for bin in crate::storage::load_sidecar::<crate::storage::DirtyBin>(&cfg.core.timefusion_data_dir, crate::storage::DIRTY_BINS) {
            let was = bin.width_minutes.max(1) * 60 * 1_000_000;
            if was != width_micros {
                remapped += 1;
            }
            for id in crate::storage::remap_bin(bin.bin, was, width_micros) {
                dedup_dirty_bins.insert((bin.project_id.clone(), bin.table_name.clone(), bin.date.clone(), id), ());
            }
        }
        if remapped > 0 {
            warn!(
                remapped,
                width_minutes = width_micros / 60_000_000,
                event = "dirty_bins_rewidened",
                "dedup bin width changed; the dirty-bin queue was re-keyed"
            );
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
        // Repair units survive a restart in the journal above, but their priority does not:
        // `claim_next` ranks damage from a runtime set filled long after boot, so the ranking
        // is restored from a sidecar here.
        {
            let restored = crate::storage::load_sidecar::<crate::storage::StoredUntaggedCell>(&cfg.core.timefusion_data_dir, crate::storage::UNTAGGED_CELLS);
            if !restored.is_empty() {
                info!(cells = restored.len(), event = "rollup_untagged_cells_restored");
                maintenance_tasks.restore_untagged_cells(restored.into_iter().map(|cell| (cell.source, cell.project_id, cell.table_name, cell.date)));
            }
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
        // Reload sweep certifications so the read-side dedup skip is not cold after a deploy.
        // `dedup_window_clean` re-checks each entry's fingerprint, so stale records fail there.
        let dedup_clean_fp: Arc<dashmap::DashMap<(String, String, String), Certification>> = Arc::new(dashmap::DashMap::new());
        let dedup_slice_coverage: Arc<dashmap::DashMap<(String, String, String), SliceCoverage>> = Arc::new(dashmap::DashMap::new());
        if cfg.maintenance.timefusion_dedup_certification_persist {
            let now = std::time::Instant::now();
            for entry in crate::storage::load_sidecar::<crate::storage::StoredCertification>(&cfg.core.timefusion_data_dir, crate::storage::CERTIFICATIONS) {
                // Rebuild the monotonic instant from the stored wall-clock age so dwell keeps
                // measuring real lifetime across boots; a backwards clock falls back to "now".
                let since = crate::storage::age_since(entry.granted_unix_ms).and_then(|age| now.checked_sub(age)).unwrap_or(now);
                dedup_clean_fp.insert(
                    (entry.project_id, entry.table_name, entry.date),
                    Certification { fp: entry.fp, since, files: Arc::new(entry.files), stale: entry.stale },
                );
            }
            info!(loaded = dedup_clean_fp.len(), event = "dedup_certifications_loaded");
            // Slice coverage too: the journal durably skips completed slices, so losing their
            // evidence makes any day straddling one permanently uncertifiable.
            for e in crate::storage::load_sidecar::<crate::storage::StoredSliceCoverage>(&cfg.core.timefusion_data_dir, crate::storage::SLICE_COVERAGE) {
                dedup_slice_coverage.insert((e.project_id, e.table_name, e.date), SliceCoverage { fp: e.fp, intervals: e.intervals, files: e.files });
            }
            info!(loaded = dedup_slice_coverage.len(), event = "dedup_slice_coverage_loaded");
        }
        let aws_endpoint = &cfg.aws.aws_s3_endpoint;
        let aws_url = Url::parse(aws_endpoint).expect("AWS endpoint must be a valid URL");
        deltalake::aws::register_handlers(Some(aws_url));
        info!("AWS handlers registered");

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

        // Must precede table creation so every table gets the cache.
        let object_store_cache = Self::initialize_cache_with_retry(&cfg).await;

        let statistics_extractor =
            Arc::new(DeltaStatisticsExtractor::new(cfg.parquet.timefusion_stats_cache_size, 300, cfg.parquet.timefusion_page_row_count_limit));

        let light_rewrite_permits = cfg.derived.max_light_optimize_k().max(1);
        // During catch-up, give sealed debt the majority of the scarce sort
        // lanes. One third (floored at one) keeps today's tail moving; the cap
        // is bypassed entirely when no sealed unit is pending.
        let hot_packing_permits = hot_packing_permits(light_rewrite_permits);
        crate::observability::maintenance_stats().light_rewrite_permits_total.store(light_rewrite_permits as u64, std::sync::atomic::Ordering::Relaxed);
        // In units, not readers: one reader slot is `DECODE_UNITS_PER_READER` units.
        let heavy_scan_permits = cfg.memory.timefusion_max_concurrent_scan_readers.max(1) * DECODE_UNITS_PER_READER as usize;
        let maintenance_shutdown = CancellationToken::new();
        let maintenance_tasks_tracker = tokio_util::task::TaskTracker::new();
        let maintenance_cancel_guard = Arc::new(maintenance_shutdown.clone().drop_guard());
        // Every unit holds its I/O tokens across object-store waits. Their capacity
        // must reach the worker-slot ceiling or it silently becomes the real
        // concurrency cap (prod: 66 slots and CPU tokens, but only 28 I/O tokens).
        let coordinator_jobs = cfg.derived.coordinator_jobs();
        let coordinator_slots = cfg.derived.coordinator_job_slots();
        let coordinator_io_slots = u32::try_from(cfg.derived.coordinator_io_slots()).unwrap_or(u32::MAX);
        // The static `coordinator_jobs` (cores/3) is the floor; an unstarved
        // runtime may exceed the thread count because a unit retains its token
        // while parked on I/O. Prod sat pinned at the floor with ~2,500 units
        // eligible while the box ran at half its CPU limit, so the reservation
        // was costing throughput it did not need to.
        let cpu_base = u32::try_from(coordinator_jobs).unwrap_or(1);
        // The ceiling must be REACHABLE: it is what the slots are sized to, or
        // admission silently re-imposes the old cap the slots were raised past.
        let cpu_max = u32::try_from(coordinator_slots).unwrap_or(cpu_base).max(cpu_base);
        let maintenance_admission = crate::maintenance_coordinator::AdmissionController::with_decoded_capacity(
            cpu_base,
            cpu_max,
            cfg.derived.coordinator_decoded_capacity_bytes(),
            coordinator_io_slots,
            coordinator_io_slots,
        );
        let logical_count_cache =
            Arc::new(crate::read::LogicalCountCache::new(cfg.core.timefusion_data_dir.join("logical_count"), cfg.derived.logical_count_memory_bytes()));
        let db = Self {
            config: cfg.clone(),
            runtime_env: Arc::new(std::sync::OnceLock::new()),
            maintenance_runtime_env: Arc::new(std::sync::OnceLock::new()),
            light_optimize_runtime_env: Arc::new(std::sync::OnceLock::new()),
            repair_runtime_env: Arc::new(std::sync::OnceLock::new()),
            coordinator_runtime_env: Arc::new(std::sync::OnceLock::new()),
            flush_sort_gate: Arc::new(tokio::sync::Semaphore::new(flush_sort_permits(cfg.maintenance.flush_sort_pool_bytes()))),
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
            maintenance_tasks_tracker,
            _maintenance_cancel_guard: Some(maintenance_cancel_guard),
            preload_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            preload_replay_complete: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            preload_replay_notify: Arc::new(tokio::sync::Notify::new()),
            preload_tables_done: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            preload_tables_total: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            maintenance_executor: Arc::new(std::sync::OnceLock::new()),
            config_pool,
            storage_configs: Arc::new(RwLock::new(storage_configs)),
            storage_configs_next_refresh_ns: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            default_s3_bucket: cfg.aws.aws_s3_bucket.clone(),
            default_s3_prefix: Some(cfg.core.timefusion_table_prefix.clone()),
            default_s3_endpoint: Some(aws_endpoint.clone()),
            object_store_cache,
            statistics_extractor,
            last_written_versions: Arc::new(RwLock::new(HashMap::new())),
            last_dedup_versions: Arc::new(RwLock::new(HashMap::new())),
            dedup_clean_fp,
            dedup_probe_declined: Arc::new(dashmap::DashMap::new()),
            dedup_certification_persist_lock: Arc::new(std::sync::Mutex::new(())),
            dedup_certification_persist_at: Arc::new(std::sync::atomic::AtomicI64::new(0)),
            dedup_slice_coverage,
            rollup_source_epochs,
            rollup_coverage: Arc::new(dashmap::DashMap::new()),
            rollup_slice_coverage: Arc::new(dashmap::DashMap::new()),
            rollup_file_rows_cache: dashmap::DashMap::new(),
            rollup_tier_untagged: Arc::new(dashmap::DashMap::new()),
            coverage_ledger: Arc::new(crate::storage::JsonCoverageLedger::load(&cfg.core.timefusion_data_dir)),
            rollup_dirty,
            rollup_invalidated_at,
            rollup_journal_lock: Arc::new(std::sync::Mutex::new(())),
            rollup_journal_persisted: Arc::new(std::sync::Mutex::new(PersistedRollupJournal::default())),
            journal_group_commit: Arc::new(crate::support::GroupCommit::default()),
            task_journal_group_commit: Arc::new(crate::support::GroupCommit::default()),
            maintenance_tasks: Arc::new(std::sync::Mutex::new(maintenance_tasks)),
            maintenance_work: Arc::new(tokio::sync::Notify::new()),
            maintenance_admission,
            maintenance_debt_planned_at: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            tantivy_census_at: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            dedup_probe_cost_ms: Arc::new(dashmap::DashMap::new()),
            maintenance_schedule_cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            rollup_backoff: Arc::new(dashmap::DashMap::new()),
            logical_count_cache,
            logical_count_building: Arc::new(dashmap::DashSet::new()),
            // Serial by design: a build retains one winner per logical key.
            logical_count_build_sem: Arc::new(tokio::sync::Semaphore::new(1)),
            dedup_dirty_bins,
            dedup_backoff: Arc::new(dashmap::DashMap::new()),
            repair_verified_sorted: Arc::new(dashmap::DashSet::new()),
            repair_verified_lock: Arc::new(std::sync::Mutex::new(())),
            repair_verified_appends: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            repair_failures: Arc::new(dashmap::DashMap::new()),
            repair_degradation: Arc::new(dashmap::DashMap::new()),
            maintenance_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(cfg.derived.rewrite_permits().max(1))),
            light_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(light_rewrite_permits)),
            hygiene_gate: Arc::new(HygieneGate::default()),
            maintenance_quiesced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            backfill_flush_stalls_seen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            census_query_timeouts_seen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            claims_query_timeouts_seen: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            hot_packing_sem: Arc::new(tokio::sync::Semaphore::new(hot_packing_permits)),
            repair_holdback_lent: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            repair_rewrite_sem: Arc::new(tokio::sync::Semaphore::new(cfg.derived.repair_rewrite_budget_mib())),
            // Three quarters to debt, leaving a quarter always free for the rollup chain.
            maintenance_debt_slots: Arc::new(tokio::sync::Semaphore::new((coordinator_jobs * 3 / 4).max(1))),
            // An eighth, not zero: transiently timed-out units must still get turns.
            maintenance_quarantine_slots: Arc::new(tokio::sync::Semaphore::new((coordinator_jobs / 8).max(1))),
            // Two workers held open for derived work; never zero, so tiny boxes still progress.
            maintenance_derived_reserve: Arc::new(tokio::sync::Semaphore::new(coordinator_jobs.saturating_sub(2).max(1))),
            dml_merge_sem: Arc::new(tokio::sync::Semaphore::new(cfg.maintenance.timefusion_dml_merge_concurrency.max(1))),
            heavy_scan_sem: Arc::new(tokio::sync::Semaphore::new(heavy_scan_permits)),
            maintenance_scan_sem: Arc::new(tokio::sync::Semaphore::new(crate::config::maintenance_scan_permits(heavy_scan_permits))),
            maintenance_job_sem: Arc::new(tokio::sync::Semaphore::new(1)),
            commit_locks: Arc::new(dashmap::DashMap::new()),
            flush_waiter_counts: Arc::new(dashmap::DashMap::new()),
            dml_locks: Arc::new(dashmap::DashMap::new()),
            histogram_dml: Arc::new(dashmap::DashMap::new()),
            histogram_delta: Arc::new(histogram::HistogramDeltaCache::default()),
            histogram_proof_build: Arc::new(histogram::HistogramProofBuilds::default()),
            tantivy_backfill_slots: Default::default(),
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
            repair_pass_permit: Arc::new(tokio::sync::Semaphore::new(1)),
            staged_intent_manifest_lock: Arc::new(std::sync::Mutex::new(())),
        };

        db.seed_routing_from_ledger();

        Ok(db)
    }

    /// Create a new Database using the global config. Tests should use `with_config()`.
    pub async fn new() -> Result<Self> {
        let cfg = config::init_config().map_err(|e| anyhow::anyhow!("Failed to load config: {e}"))?;
        Self::with_config(Arc::new(cfg.clone())).await
    }

    pub fn with_batch_queue(mut self, batch_queue: Arc<crate::write::BatchQueue>) -> Self {
        self.batch_queue = Some(batch_queue);
        self
    }

    /// Set the buffered write layer. Shared OnceLock: publishes to every existing clone, and a
    /// second call is a no-op.
    pub fn with_buffered_layer(self, layer: Arc<crate::write::BufferedWriteLayer>) -> Self {
        let _ = self.buffered_layer.set(layer);
        self
    }

    pub fn buffered_layer(&self) -> Option<&Arc<crate::write::BufferedWriteLayer>> {
        if self.bypass_buffer { None } else { self.buffered_layer.get() }
    }

    /// The deferred-DML coalescer, when enabled.
    pub fn dml_coalescer(&self) -> Option<&Arc<crate::dml::DmlCoalescer>> {
        self.dml_coalescer.get()
    }

    /// Start the DML coalescer and its drain task when `TIMEFUSION_DML_COALESCE_SECS > 0`.
    /// Idempotent. The drain loop stops, after one final drain, on the maintenance token.
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

    /// Attach the tantivy search service used by the scan-side prefilter (set-once).
    pub fn with_tantivy_search(self, svc: Arc<crate::tantivy::search::TantivySearchService>) -> Self {
        let _ = self.tantivy_search.set(svc);
        self
    }

    pub fn tantivy_search(&self) -> Option<&Arc<crate::tantivy::search::TantivySearchService>> {
        self.tantivy_search.get()
    }

    /// Attach the write-side tantivy service, used by the compaction-GC hook (set-once).
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

    /// Create and configure a SessionContext with DataFusion settings
    pub fn create_session_context(self: Arc<Self>) -> SessionContext {
        self.create_session_context_for(false)
    }

    /// As `create_session_context`, but `for_pgwire` gates the client-facing-only
    /// rules — heavy-query admission must never touch internal/maintenance
    /// contexts, which use their own pool.
    pub fn create_session_context_for(self: Arc<Self>, for_pgwire: bool) -> SessionContext {
        use datafusion::{config::ConfigOptions, execution::SessionStateBuilder};
        use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};

        use crate::dml::DmlQueryPlanner;

        let query_batch_size = self.config.memory.timefusion_query_batch_size.to_string();
        let mut options = ConfigOptions::new();
        // Defaults set explicitly (even where they match DataFusion's) so a
        // future upstream default flip can't silently regress query plans.
        // `datafusion.runtime.*` keys do NOT belong here — a SessionConfig string
        // cannot reconfigure an already-built RuntimeEnv; see `build_query_runtime_env`.
        for (key, value) in [
            ("datafusion.catalog.information_schema", "true"),
            // Some permanent parquet has `timestamp`/`id` nullable while the YAML
            // declares NOT NULL, and DataFusion otherwise rejects aggregates grouped
            // on such columns. Widened nullability is always safe to read.
            ("datafusion.execution.skip_physical_aggregate_schema_check", "true"),
            // Must be false: delta_kernel's unshredded_variant() schema uses Binary (not BinaryView).
            // Forcing view types causes UPDATE/DELETE rewrites to fail schema validation against variant columns.
            ("datafusion.execution.parquet.schema_force_view_types", "false"),
            ("datafusion.sql_parser.map_string_types_to_utf8view", "true"),
            // Required: the default GenericDialect binds `->`/`->>` BELOW `=`, so
            // `body->>'k'='v'` mis-parses as `body->>('k'='v')`.
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
            // Small batches: the wide otel schema makes every decode buffer cost
            // `batch_size × row width`, none of it pool-accounted.
            // Query sessions read the knob; maintenance sessions stay on the
            // constant (their sorts admit per batch against fixed budgets).
            ("datafusion.execution.batch_size", query_batch_size.as_str()),
            // Timestamps are typically sorted; round-robin repartitioning off to
            // maintain sort order.
            ("datafusion.optimizer.prefer_existing_sort", "true"),
            ("datafusion.optimizer.repartition_aggregations", "true"),
            ("datafusion.optimizer.enable_round_robin_repartition", "false"),
            ("datafusion.optimizer.filter_null_join_keys", "true"),
            ("datafusion.optimizer.skip_failed_rules", "false"),
            // Upstream DataFusion bug: leaf-expression pushdown panics ("Assertion
            // failed: expr.is_empty()") on any multi-column UNNEST whose plan carries
            // a get_field. TF's Variant access uses `variant_get`, so disabling these
            // rules does not affect Variant plans.
            ("datafusion.optimizer.enable_leaf_expression_pushdown", "false"),
            // Proper limit handling across partitions.
            ("datafusion.optimizer.enable_distinct_aggregation_soft_limit", "true"),
            ("datafusion.optimizer.enable_topk_aggregation", "true"),
            ("datafusion.execution.coalesce_batches", "true"),
            ("datafusion.optimizer.max_passes", "5"),
            // The per-query share of the (already tree-sized) pool.
            ("datafusion.execution.memory_fraction", "0.9"),
        ] {
            set_or_warn(&mut options, key, value);
        }
        // Must match `warm_footer`'s suffix range: the Foyer metadata cache keys on
        // (path, exact range), so the reader's first fetch hits the warmed entry.
        set_or_warn(&mut options, "datafusion.execution.parquet.metadata_size_hint", &self.config.cache.timefusion_parquet_metadata_size_hint.to_string());
        // Partition count and pool together price the sort reservation, which is
        // taken per partition and cannot spill. `pinned` distinguishes a count this
        // session imposes from one merely used to price the reservation.
        // Concurrency is per POOL: maintenance is shared by its own workers, the
        // query pool by monoscope's client connections. Using the client count for
        // maintenance starves the spill reservation.
        let (partitions, pool_bytes, pinned, concurrency) = match (self.maintenance_scan, self.config.memory.timefusion_query_partitions) {
            (true, _) => (MAINTENANCE_MAX_PARTITIONS, self.config.derived.maintenance_pool_bytes(), true, self.config.derived.coordinator_jobs()),
            (false, 0) => (self.config.derived.cores(), self.config.derived.query_pool_bytes(), false, crate::config::client_sort_concurrency()),
            (false, n) => (n, self.config.derived.query_pool_bytes(), true, crate::config::client_sort_concurrency()),
        };
        set_or_warn(
            &mut options,
            "datafusion.execution.sort_spill_reservation_bytes",
            &crate::config::sort_spill_reservation_bytes(self.config.memory.timefusion_sort_spill_reservation_bytes, partitions, pool_bytes, concurrency)
                .to_string(),
        );
        // Cap query parallelism at the container's CPU quota (0 = DataFusion default).
        if pinned {
            set_or_warn(&mut options, "datafusion.execution.target_partitions", &partitions.to_string());
        }

        // A maintenance scan borrows the maintenance pool, not the query pool: that
        // pool has a spill directory, and background rewrites must not compete with
        // interactive queries.
        let runtime_env = if self.maintenance_scan { self.maintenance_runtime_env() } else { self.shared_runtime_env() };

        let record_metrics = self.config.memory.timefusion_tracing_record_metrics;

        // Cell-capped preview formatter — the default renders whole cell values,
        // which can be arbitrarily large.
        let tracing_options = InstrumentationOptions::builder()
            .record_metrics(record_metrics)
            .preview_limit(5)
            .preview_fn(Arc::new(crate::observability::capped_preview_fn))
            .build();

        let instrument_rule = instrument_with_info_spans!(options: tracing_options);

        // Rule ordering: VariantInsertRewriter runs BEFORE TypeCoercion (rewrites string->json_to_variant)
        //                VariantSelectRewriter runs AFTER TypeCoercion (wraps Variant cols with variant_to_json)
        let analyzer_rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> = vec![
            Arc::new(datafusion::optimizer::analyzer::resolve_grouping_function::ResolveGroupingFunction::new()),
            Arc::new(crate::read::optimizers::VariantInsertRewriter),
            // Before TypeCoercion, so the injected `text_match(col, lit)` calls
            // get coerced like any other UDF args.
            Arc::new(crate::read::optimizers::TantivyPredicateRewriter::new(self.config.tantivy.route_equality())),
            // Expands `f(qualifier.*)` before TypeCoercion rejects the typeless
            // wildcard. Postgres parity.
            Arc::new(crate::read::optimizers::WildcardFnArgExpander),
            // PG parity: re-type PG array string literals as list literals before
            // TypeCoercion fails the call.
            Arc::new(crate::read::optimizers::PgArrayLiteralRewriter),
            // DataFusion only decorrelates EXISTS in a filter, so an EXISTS in a
            // SELECT list is rewritten to a correlated `count(1) > 0` subquery.
            // Before TypeCoercion so the comparison coerces.
            Arc::new(crate::read::optimizers::ExistsInProjection),
            // Reads a field off a Variant natively instead of round-tripping the
            // whole struct through JSON text. Before TypeCoercion so the calls it
            // splices in get coerced.
            Arc::new(crate::read::optimizers::VariantJsonAccessorPeephole),
            Arc::new(datafusion::optimizer::analyzer::type_coercion::TypeCoercion::new()),
            Arc::new(crate::read::optimizers::VariantSelectRewriter),
        ];

        let session_state = SessionStateBuilder::new()
            .with_config(options.into())
            .with_runtime_env(runtime_env)
            .with_default_features()
            // Ours FIRST, ahead of DataFusion's defaults: CoreFunctionPlanner claims
            // e.g. every `SUBSTRING` unconditionally, so appending would hide the PG
            // regex form from our planner.
            .with_expr_planners(
                std::iter::once(Arc::new(crate::read::functions::VariantAwareExprPlanner) as Arc<dyn datafusion::logical_expr::planner::ExprPlanner>)
                    .chain(datafusion::execution::SessionStateDefaults::default_expr_planners())
                    .collect(),
            )
            .with_analyzer_rules(analyzer_rules)
            // Appended after DataFusion's defaults so push_down_limit has
            // already folded LIMIT into Sort.fetch — see the rule's docs.
            .with_optimizer_rule(Arc::new(crate::read::optimizers::DeferExpensiveProjection))
            // After the defaults so predicate pushdown has already placed the
            // timestamp bounds this reads, and before the Variant restore below
            // so the branches it clones are re-typed like any other scan.
            .with_optimizer_rule(Arc::new(crate::read::optimizers::RangeParallelDedup))
            // Must run LAST: re-restores Variant scan types that
            // optimize_projections reverts to Utf8View when it rebuilds each
            // TableScan from the provider's un-typed schema.
            .with_optimizer_rule(Arc::new(crate::read::optimizers::VariantScanSchemaRestore))
            // Splice the mem∪delta union-ordering rule in BEFORE EnforceDistribution
            // so the built-in EnforceDistribution/EnforceSorting do the
            // SortPreservingMerge insertion and redundant-sort removal. The tracing
            // instrument rule stays last.
            .with_physical_optimizer_rules({
                let mut rules = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new().rules;
                let pos = rules.iter().position(|r| r.name() == "EnforceDistribution").unwrap_or(0);
                rules.insert(pos, Arc::new(crate::read::optimizers::OrderedUnionForTopK));
                rules.insert(pos, Arc::new(crate::read::optimizers::AggregateInputOrdering));
                // After EnforceSorting/EnforceDistribution, never before: it undoes
                // their discharge of DedupExec's ordering, which otherwise leaves the
                // operator reading through an order-erasing coalesce.
                rules.push(Arc::new(crate::read::optimizers::DedupNeedsOrderedInput));
                // LAST, on the pgwire session only, so it wraps the absolute root
                // (executed once) of a heavy plan. Internal SQL contexts (maintenance,
                // rollup) have their own pool and are never gated.
                if for_pgwire {
                    rules.push(Arc::new(crate::read::admission::HeavyQueryAdmission));
                }
                rules.push(instrument_rule);
                rules
            })
            // Late-binding: sessions are created during boot, before the buffered
            // layer exists, so the planner resolves it at plan time.
            .with_query_planner(Arc::new(DmlQueryPlanner::new(self.clone())))
            // PostgreSQL custom casts (jsonpath, regproc) become text consistently
            // across the simple and extended protocols.
            .with_type_planner(Arc::new(crate::read::functions::PostgresTypePlanner))
            .build();

        SessionContext::new_with_state(session_state)
    }

    /// Register UDFs only — safe to call before `with_buffered_layer`.
    pub fn setup_session_udfs(&self, ctx: &mut SessionContext) -> DFResult<()> {
        self.register_set_config_udf(ctx);
        // Must precede the JSON functions, so VariantAwareExprPlanner intercepts
        // `->`/`->>` on Variant columns before JsonExprPlanner treats them as strings.
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
        for (table_name, schema) in registry.list_tables().into_iter().filter_map(|name| registry.get(&name).map(|schema| (name, schema))) {
            let provider = |skip_queue| -> Arc<dyn TableProvider> {
                Arc::new(
                    ProjectRoutingTable::new("default".to_string(), Arc::new(self.clone()), schema.schema_ref(), batch_queue.clone(), table_name.clone())
                        .with_skip_queue(skip_queue),
                )
            };
            ctx.register_table(&table_name, provider(false))?;
            info!("Registered ProjectRoutingTable for table '{}' with SessionContext", table_name);

            // Bulk-write alias: `INSERT INTO {table}__bulk ...` commits straight to
            // Delta, bypassing WAL + MemBuffer. A dedicated table name (not a GUC)
            // is how a client opts in, because the session context is shared across
            // connections. Internal `table_name` stays the real table.
            ctx.register_table(format!("{table_name}__bulk"), provider(true))?;
        }

        // Introspection table: `SELECT * FROM timefusion_stats` returns a flat
        // (component, key, value) snapshot of MemBuffer / WAL / BufferedWriteLayer
        // counters. The DashMap clones share live state with `self`, so the closures
        // observe inserts made after registration.
        let fr_handle = self.fast_resolve_cache.clone();
        let dp_handle = self.delta_provider_cache.clone();
        // Provider count, not key count: a key holds a version ring, and the provider
        // total is what tracks retained heap.
        let cache_sizes: crate::server::pg_compat::CacheSizeSnapshot = Arc::new(move || (fr_handle.len(), dp_handle.iter().map(|e| e.value().len()).sum()));
        let foyer = self.object_store_cache.clone();
        let foyer_stats: crate::server::pg_compat::FoyerStatsSnapshot =
            Arc::new(move || foyer.as_ref().map_or_else(crate::storage::FoyerRuntimeStats::default, |cache| cache.runtime_stats()));
        // Live (reserved, size) of one memory pool — same shape for all three.
        let pool_snapshot = |env: Arc<datafusion::execution::runtime_env::RuntimeEnv>, size: usize| -> crate::server::pg_compat::PoolSnapshot {
            Arc::new(move || (env.memory_pool.reserved(), size))
        };
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
                    .with_query_pool(pool_snapshot(self.shared_runtime_env(), self.config.derived.query_pool_bytes()))
                    // `heavy_pool_bytes`, NOT `maintenance_pool_bytes`: the maintenance
                    // env's pool is built from the heavy share, and the denominator
                    // must be the pool the numerator is reserved from or a saturated
                    // pool reads as idle.
                    .with_maintenance_pools(
                        pool_snapshot(self.maintenance_runtime_env(), self.heavy_pool_bytes()),
                        pool_snapshot(self.coordinator_runtime_env(), self.config.derived.coordinator_share_bytes()),
                    )
                    .with_tantivy_search_opt(self.tantivy_search().cloned())
                    .with_bloom_prune_opt(self.bloom_prune().cloned()),
            ),
        )?;

        crate::server::pg_compat::setup_catalog(ctx, &self.config.core.pgwire_user, self.config.core.timefusion_pgwire_max_statement_secs)?;
        Ok(())
    }

    /// Set up the session context with both tables and UDFs (in that order).
    pub fn setup_session_context(&self, ctx: &mut SessionContext) -> DFResult<()> {
        self.setup_session_tables(ctx)?;
        self.setup_session_udfs(ctx)
    }

    /// `set_config(name, value, is_local)` — echoes `value` back, as PostgreSQL
    /// does. TF holds no state for settings clients set this way. Echoing the
    /// argument unchanged also handles both the Scalar and Array call shapes.
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

    /// Snapshot of the custom-storage (project, table) keys — one lock acquisition
    /// for callers needing many membership checks. Custom storage is rare.
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
    /// Lock-free hot-path resolve: returns the cached handle without any `.await`,
    /// skipping the version-refresh check `resolve_table` does. Only for reads that
    /// tolerate a snapshot a few seconds behind a flush.
    pub fn try_fast_resolve(&self, project_id: &str, table_name: &str) -> Option<Arc<RwLock<DeltaTable>>> {
        self.fast_resolve_cache.get(&table_key(project_id, table_name)).map(|r| Arc::clone(r.value()))
    }

    /// True iff the scan path may skip the Delta side for `(project, table)`.
    ///
    /// True only with positive evidence the resolved table had no files; the
    /// `delta_has_files` bit is sticky-true, so false means "unknown" and callers
    /// must fall through to the full scan path.
    pub fn delta_scan_can_be_skipped(&self, project_id: &str, table_name: &str) -> bool {
        self.delta_has_files
            .get(&table_key(project_id, table_name))
            // Acquire pairs with the Release stores; Relaxed would break the moment
            // the Arc<AtomicBool> is read outside the DashMap shard guard.
            .is_some_and(|f| !f.load(std::sync::atomic::Ordering::Acquire))
    }

    /// Mark a (project, table) as having Delta files. Called by the flush
    /// callback after a successful commit.
    pub fn mark_delta_has_files(&self, project_id: &str, table_name: &str) {
        self.delta_has_files.entry(table_key(project_id, table_name)).or_default().store(true, std::sync::atomic::Ordering::Release);
    }

    /// Total cached providers across every key (a key holds up to
    /// `PROVIDER_VERSION_RETENTION` versions). This, not the key count, tracks the
    /// cache's heap footprint.
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
        // The caller is about to insert one provider, so leave one slot free.
        // Capacity is counted in providers, not keys.
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

        // Lazy reload of storage configs from PG, at most once per
        // STORAGE_CONFIGS_TTL_NS — otherwise every statement costs a PG roundtrip.
        if let Some(ref pool) = self.config_pool {
            const STORAGE_CONFIGS_TTL_NS: u64 = 30 * 1_000_000_000; // 30s
            use std::{sync::atomic::Ordering, time::Instant};
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

        // Custom storage config → isolated table; otherwise the unified table
        // (all projects share it, partitioned by project_id).
        let is_custom = self.has_custom_storage(project_id, table_name).await;
        span.record("is_custom", is_custom);
        // Clone the handle and DROP the map guard before refreshing: `update_table`
        // replays the Delta log, and tokio's RwLock is write-preferring, so a read
        // guard held across it wedges every later reader.
        let cached = if is_custom {
            self.custom_project_tables.read().await.get(&table_key(project_id, table_name)).cloned()
        } else {
            self.unified_tables.read().await.get(table_name).cloned()
        };
        let t = match cached {
            Some(table) => {
                if is_custom {
                    debug!("Found custom table for project '{}' table '{}' in cache", project_id, table_name);
                } else {
                    debug!("Found unified table '{}' in cache", table_name);
                }
                // Version tracking keys unified tables under an empty project_id.
                self.refresh_cached_table(table, if is_custom { project_id } else { "" }, table_name).await?
            }
            None if is_custom => self
                .get_or_create_custom_table(project_id, table_name)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to get or create custom table: {}", e)))?,
            None => self
                .get_or_create_unified_table(table_name)
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to get or create unified table: {}", e)))?,
        };
        self.populate_resolve_caches(project_id, table_name, &t).await;
        Ok(t)
    }

    /// Seed `fast_resolve_cache` and `delta_has_files` from a freshly-resolved handle.
    ///
    /// Sticky-true invariant: only flips `delta_has_files` false → true, because
    /// downgrading would skip Delta and hide rows. This relies on the resolved handle
    /// already reflecting S3 truth (`DeltaTableBuilder::load()` runs synchronously);
    /// a lazy loader would break the seeding.
    async fn populate_resolve_caches(&self, project_id: &str, table_name: &str, t: &Arc<RwLock<DeltaTable>>) {
        let key = table_key(project_id, table_name);
        let was_new = self.fast_resolve_cache.insert(key.clone(), Arc::clone(t)).is_none();
        // Warn on threshold-multiple crossings only, so log volume tracks tenant
        // growth rather than per-query traffic. The cache is unbounded by design.
        let size = if was_new { self.fast_resolve_cache.len() } else { 0 };
        if size >= CACHE_SOFT_LIMIT_WARN && size.is_multiple_of(CACHE_SOFT_LIMIT_WARN) {
            tracing::warn!(
                target = "table_caches",
                fast_resolve_cache_entries = size,
                threshold = CACHE_SOFT_LIMIT_WARN,
                "fast_resolve_cache crossed soft limit (no eviction by design). If your steady-state tenant count is below the threshold, dropped or transient project_ids are accumulating. Watch scan.fast_resolve_cache_entries in timefusion_stats."
            );
        }
        let has_files = t.read().await.version().is_some_and(|v| v > 0);
        let entry = self.delta_has_files.entry(key).or_default();
        if has_files {
            // Release pairs with the Acquire load in delta_scan_can_be_skipped.
            entry.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    /// Refresh a cache-hit handle when this process's view may be behind.
    async fn refresh_cached_table(&self, table: Arc<RwLock<DeltaTable>>, project_id: &str, table_name: &str) -> DFResult<Arc<RwLock<DeltaTable>>> {
        let last_written_version = self.last_written_versions.read().await.get(&table_key(project_id, table_name)).cloned();
        let current_version = table.read().await.version();
        if should_refresh_table(current_version, last_written_version) {
            self.update_table(&table, project_id, table_name).await.map_err(|e| DataFusionError::Execution(format!("Failed to update table: {e}")))?;
        }
        Ok(table)
    }

    /// Load-outside-write-lock, double-check-then-insert cache shape shared by
    /// `get_or_create_unified_table`/`get_or_create_custom_table`.
    ///
    /// The load MUST happen outside the write lock: the Delta-log replay is
    /// network-bound and unbounded, and tokio's RwLock is write-preferring, so one
    /// stuck writer wedges every later reader. Racing first touches may both load;
    /// the double-check keeps the first insert. Returns the post-insert cache size
    /// when this call inserted, `None` on a cache hit.
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
        let missing = |what: &str| anyhow::anyhow!("No default S3 {} configured for unified table '{}'", what, table_name);
        let bucket = self.default_s3_bucket.as_ref().ok_or_else(|| missing("bucket"))?;
        let prefix = self.default_s3_prefix.as_ref().ok_or_else(|| missing("prefix"))?;
        let endpoint = self.default_s3_endpoint.as_ref().ok_or_else(|| missing("endpoint"))?;
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
        let configs = self.storage_configs.read().await;
        let config = configs
            .get(&table_key(project_id, table_name))
            .ok_or_else(|| anyhow::anyhow!("No storage config found for project '{}' table '{}'", project_id, table_name))?
            .clone();
        drop(configs);

        let endpoint = config.s3_endpoint.as_deref().or(self.default_s3_endpoint.as_deref()).unwrap_or("https://s3.amazonaws.com");
        let storage_uri = format!("s3://{}/{}/?endpoint={endpoint}", config.s3_bucket, config.s3_prefix);

        // Inherit the shared base options (AWS_ALLOW_HTTP, connect_timeout), then
        // override with this tenant's credentials. The endpoint stays tenant-scoped:
        // a BYO bucket with no custom endpoint must resolve against real AWS S3, so
        // the inherited default is dropped rather than kept.
        let mut storage_options = self.build_storage_options();
        storage_options.insert("AWS_ACCESS_KEY_ID".to_string(), config.s3_access_key_id.clone());
        storage_options.insert("AWS_SECRET_ACCESS_KEY".to_string(), config.s3_secret_access_key.clone());
        storage_options.insert("AWS_REGION".to_string(), config.s3_region.clone());
        match config.s3_endpoint.as_ref() {
            Some(endpoint) => storage_options.insert("AWS_ENDPOINT_URL".to_string(), endpoint.clone()),
            None => storage_options.remove("AWS_ENDPOINT_URL"),
        };

        info!("Creating or loading custom table for project '{}' table '{}' at: {}", project_id, table_name, storage_uri);

        // Load OUTSIDE the write lock — see `get_or_create_cached`. Worse here: the
        // load targets a tenant's BYO bucket, so one unreachable endpoint can pin
        // this map's write guard indefinitely.
        let (table_arc, fresh_count) =
            self.get_or_create_cached(&self.custom_project_tables, table_key(project_id, table_name), &storage_uri, &storage_options, table_name).await?;
        if let Some(count) = fresh_count {
            info!("Cached custom table for project '{}' table '{}', cache now contains {} entries", project_id, table_name, count);
        }
        Ok(table_arc)
    }

    /// Table properties applied at CREATE and reconciled on tables loaded from
    /// storage, so a config change also reaches tables that baked in an older value.
    fn delta_table_properties(&self, table_name: &str) -> HashMap<String, String> {
        let mut props = HashMap::from([
            // Aligned with vacuum retention so checkpoints prune Remove tombstones
            // as soon as vacuum has had its shot at the files.
            ("delta.deletedFileRetentionDuration".to_string(), format!("interval {} hours", self.config.maintenance.timefusion_vacuum_retention_hours)),
            ("delta.checkpointInterval".to_string(), self.config.parquet.timefusion_checkpoint_interval.to_string()),
            // Bound the _delta_log so per-commit version-discovery LISTs stay cheap;
            // Delta's 30-day default lets it grow to tens of thousands of objects.
            ("delta.logRetentionDuration".to_string(), format!("interval {} hours", self.config.maintenance.timefusion_log_retention_hours)),
            // Stats for an explicit column list, not all 90+ leaf columns: whole-schema
            // stats make every Add carry min/max/nullCount for each wide JSON/variant
            // column, and parsing them dominates log replay. The listed columns are the
            // only ones data-skipping and compaction prune on. Takes precedence over a
            // legacy `dataSkippingNumIndexedCols=-1` (delta-rs reads stats_columns first),
            // so that key is left alone rather than removed.
            ("delta.dataSkippingStatsColumns".to_string(), stats_columns_for(schema_or_default(table_name))),
        ]);
        // Merge-on-read deletion vectors. Opt-in only; `ensure_table_properties` is
        // idempotent, so an already-upgraded table commits nothing.
        if self.config.maintenance.timefusion_use_deletion_vectors {
            props.insert("delta.enableDeletionVectors".to_string(), "true".to_string());
        }
        props
    }

    /// Internal helper to create/load a Delta table with caching and retry logic
    async fn create_delta_table_internal(&self, storage_uri: &str, storage_options: &HashMap<String, String>, table_name: &str) -> Result<DeltaTable> {
        // Two S3 clients, one per request class: the data client keeps the generous
        // `request_timeout` a multi-MB parquet part needs, while `_delta_log` traffic
        // gets a short timeout so a hung commit PUT cannot pin the commit lock for
        // the data bound. The router sits BELOW instrumentation and the foyer cache,
        // so cache keys and metrics are unchanged.
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

        match self.create_or_load_delta_table(storage_uri, storage_options.clone(), cached_store.clone()).await {
            Ok(table) => {
                info!("Loaded existing table '{}'", table_name);
                Ok(ensure_table_properties(table, self.delta_table_properties(table_name)).await)
            }
            Err(load_err) => {
                info!("Table '{}' doesn't exist, creating new table. err: {:?}", table_name, load_err);

                let schema = schema_or_default(table_name);
                let mut create_attempts = 0;

                loop {
                    create_attempts += 1;
                    // The reconciled set plus the one create-only key.
                    let config: HashMap<String, Option<String>> = self
                        .delta_table_properties(table_name)
                        .into_iter()
                        .chain([("delta.enableExpiredLogCleanup".to_string(), "true".to_string())])
                        .map(|(key, value)| (key, Some(value)))
                        .collect();

                    match CreateBuilder::new()
                        .with_location(storage_uri)
                        .with_columns(schema.columns().unwrap_or_default())
                        .with_partition_columns(schema.partitions.clone())
                        .with_storage_options(storage_options.clone())
                        .with_commit_properties(base_commit_properties())
                        .with_configuration(config)
                        .await
                    {
                        Ok(table) => break Ok(table),
                        Err(create_err) => {
                            let err_str = create_err.to_string();
                            let conflict = ["already exists", "version 0", "ConditionalCheckFailedException"].iter().any(|m| err_str.contains(m));
                            if !(conflict && create_attempts < 3) {
                                break Err(anyhow::anyhow!("Failed to create table: {}", create_err));
                            }
                            debug!("Table creation conflict, attempting to load existing table (attempt {})", create_attempts);
                            let backoff_ms = 100 * (2_u64.pow(create_attempts.min(5)));
                            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;

                            match self.create_or_load_delta_table(storage_uri, storage_options.clone(), cached_store.clone()).await {
                                Ok(table) => break Ok(table),
                                Err(reload_err) => debug!("Failed to load table after creation conflict: {:?}", reload_err),
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
    /// Live parquet file URIs of a Delta table, after refreshing its state. Empty
    /// if the table does not exist yet.
    pub async fn list_file_uris(&self, project_id: &str, table_name: &str) -> Result<Vec<String>> {
        let Ok(table_ref) = self.resolve_table(project_id, table_name).await else {
            return Ok(Vec::new());
        };
        let _ = refresh_table_snapshot(&table_ref, self.config.maintenance.timefusion_incremental_snapshot).await;
        let uris = table_ref.read().await.get_file_uris()?.collect();
        Ok(uris)
    }

    /// Best-effort warm of the Foyer cache for parquet just written by a flush or
    /// optimize: a ranged footer GET primes the metadata cache, a full GET (when
    /// configured) the data cache. Never affects the commit. Files are filtered to
    /// partitions within `timefusion_warm_recency_days`. `confirm` awaits the
    /// metadata pass under a deadline; `allow_full_files` is false during bootstrap
    /// so restart recovery does not saturate the object store.
    async fn warm_cache_for_uris(
        &self, object_store: Arc<dyn object_store::ObjectStore>, table_uri: String, uris: Vec<String>, confirm: Option<std::time::Duration>,
        allow_full_files: bool,
    ) {
        let maint = &self.config.maintenance;
        if !maint.timefusion_warm_after_compaction || uris.is_empty() {
            return;
        }
        // Confirm runs ON the flush path, so it only warms metadata and uses its
        // own much lower concurrency bound — never the detached compaction knob.
        let (warm_full_files, warm_all_footers, concurrency) = match confirm {
            Some(_) => (false, false, crate::config::CACHE_CONFIRM_CONCURRENCY),
            None => (allow_full_files && maint.timefusion_warm_full_files, maint.timefusion_warm_all_footers, maint.timefusion_warm_concurrency),
        };
        let recency_days = maint.timefusion_warm_recency_days;
        let concurrency = concurrency.max(1);
        let metadata_size_hint = self.config.cache.timefusion_parquet_metadata_size_hint as u64;
        let stats_cache = self.object_store_cache.clone();

        // Relativize absolute s3:// URIs against the table root: the cached
        // object store consumes bucket-relative paths.
        let prefix = table_cache_prefix(&table_uri);
        let table_path = table_path_in_bucket(prefix);
        // Cap the day count before the i64 cast so a misconfiguration can't wrap.
        let cutoff = (recency_days > 0).then(|| Utc::now().date_naive() - chrono::Duration::days(recency_days.min(3650) as i64));

        // Oldest partitions warm first so the newest land last in LRU order and
        // eviction drops the least-queried old partitions.
        let (paths, dropped) = select_warm_paths(uris, prefix, warm_all_footers, cutoff);
        if dropped > 0 {
            // warn, not debug: a systematic prefix mismatch silently no-ops the
            // whole warm pass.
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
                let s = cache.get_stats().main;
                Some(if s.hits + s.misses > 0 { (s.hits as f64 / (s.hits + s.misses) as f64) * 100.0 } else { 0.0 })
            }
            _ => None,
        };

        let scope = if warm_full_files { "full" } else { "footer-only" };
        // Pace only the boot/background pass (confirm=None); per-flush confirm
        // warms are small and latency-relevant.
        let body_pace = (confirm.is_none() && self.config.maintenance.timefusion_warm_body_boot_files_per_sec > 0)
            .then(|| std::time::Duration::from_secs_f64(1.0 / f64::from(self.config.maintenance.timefusion_warm_body_boot_files_per_sec)));
        if confirm.is_none() {
            info!("Cache warm start: {count} files (scope={scope}, concurrency={concurrency})");
        }
        let t0 = std::time::Instant::now();
        const WARM_PROGRESS_INTERVAL: usize = 500;
        let (done, fetched) = (std::sync::atomic::AtomicUsize::new(0), std::sync::atomic::AtomicUsize::new(0));
        let (done, fetched, shared) = (&done, &fetched, stats_cache.as_ref());
        let pass = futures::stream::iter(paths).for_each_concurrent(concurrency, |(path, recent)| {
            let store = object_store.clone();
            async move {
                // Always warm metadata: a cold footer probe must never trigger a
                // whole-object GET.
                let _ = crate::storage::warm_parquet_metadata(store.as_ref(), &path, metadata_size_hint).await;
                if warm_full_files && recent {
                    let hit = match shared {
                        Some(shared) => crate::storage::warm_full_if_absent(store.as_ref(), shared, &path, &bucket_cache_key(table_path, &path)).await,
                        None => crate::storage::warm_full(store.as_ref(), &path).await,
                    };
                    fetched.fetch_add(hit as usize, std::sync::atomic::Ordering::Relaxed);
                    // Boot-path body warms are paced or they saturate object-store
                    // bandwidth. Only actual fetches sleep.
                    if hit && let Some(interval) = body_pace {
                        tokio::time::sleep(interval).await;
                    }
                }
                let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if n.is_multiple_of(WARM_PROGRESS_INTERVAL) {
                    info!("Cache warm progress: {n}/{count} files ({:.1}s elapsed)", t0.elapsed().as_secs_f64());
                }
            }
        });

        let Some(deadline) = confirm else {
            pass.await;
            let elapsed_s = t0.elapsed().as_secs_f64();
            let before = baseline.map_or_else(String::new, |rate| format!("; foyer main hit rate before warm was {rate:.2}% (next query benefits)"));
            info!("Cache warm complete: {} files warmed (scope={}) in {:.1}s{}", count, scope, elapsed_s, before);
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

    /// Evict the cached bytes of files a compaction tombstoned. Cache-only (no
    /// S3), so it runs inline; a straggler query on the old snapshot just takes
    /// a cache miss, never a wrong result.
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
            // Prefix mismatch: evicting would hit the wrong key, so skip and log.
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

    /// Warm the cache for files added by a just-committed flush/optimize.
    /// Fire-and-forget: table resolution and the read lock happen in a spawned
    /// task, so the caller (notably the flush callback) never blocks.
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
                db.warm_cache_for_uris(store, table_uri, uris, None, true).await;
            }
        });
    }

    /// Resolve every registry table and warm parquet footers in the background
    /// (ALL live files by default; recency-bounded when
    /// `TIMEFUSION_WARM_ALL_FOOTERS=false`), so the first query after a deploy
    /// doesn't pay Delta log replay + footer reads inline.
    pub fn preload_tables(self: &Arc<Self>) {
        // Idempotent: a second call must not double the boot-time S3 warm burst.
        if self.preload_started.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Concurrent, but capped at the per-file warm bound: each table preload
        // is a Delta log replay, so unbounded spawn-per-table spikes S3 at boot.
        let db = Arc::clone(self);
        let shutdown = self.maintenance_shutdown.clone();
        let concurrency = self.config.maintenance.timefusion_warm_concurrency.max(1);
        let preload = async move {
            let tables = crate::schema::registry().list_tables();
            db.preload_tables_total.store(tables.len() as u64, std::sync::atomic::Ordering::Relaxed);
            let preload_all = futures::stream::iter(tables).for_each_concurrent(concurrency, |table_name| {
                let db = Arc::clone(&db);
                async move {
                    let t = std::time::Instant::now();
                    let resolved = db.resolve_table("default", &table_name).await;
                    // Counted even on failure: one unresolvable table must not
                    // hold the maintenance gate shut for the whole budget.
                    db.mark_table_replayed();
                    match resolved {
                        Ok(table_ref) => {
                            let (uris, store, table_uri) = {
                                let table = table_ref.read().await;
                                let uris: Vec<String> = file_uris(&table);
                                (uris, table.log_store().object_store(None), table.table_url().to_string())
                            };
                            info!("bootstrap.phase=table_preload table={table_name} files={} elapsed_ms={}", uris.len(), t.elapsed().as_millis());
                            // Bodies warm too, but paced and skip-if-cached; 0
                            // restores footer-only.
                            let bodies = db.config.maintenance.timefusion_warm_body_boot_files_per_sec > 0;
                            db.warm_cache_for_uris(store, table_uri, uris, None, bodies).await;
                        }
                        Err(e) => warn!("bootstrap.phase=table_preload table={table_name} skipped: {e}"),
                    }
                }
            });
            // Abandon warming on shutdown so in-flight S3 calls can't slow a restart.
            let completed = tokio::select! {
                _ = shutdown.cancelled() => false,
                _ = preload_all => true,
            };
            if completed {
                // The only thing that releases the gate when the registry is empty.
                db.mark_replay_complete();
                info!(event = "table_preload_complete");
            }
        };
        if let Some(executor) = self.maintenance_executor.get() {
            executor.spawn(preload);
        } else {
            // Test/CLI callers can preload without starting schedulers.
            tokio::spawn(preload);
        }
    }

    /// One table's Delta log replay is done — the warm behind it may still be
    /// running. Counted in BOTH outcomes, so an unresolvable table can't hold
    /// the gate.
    fn mark_table_replayed(&self) {
        let done = self.preload_tables_done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        if done >= self.preload_tables_total.load(std::sync::atomic::Ordering::Relaxed) {
            self.mark_replay_complete();
        }
    }

    fn mark_replay_complete(&self) {
        if !self.preload_replay_complete.swap(true, std::sync::atomic::Ordering::Release) {
            self.preload_replay_notify.notify_waiters();
            info!(event = "table_preload_replay_complete");
        }
    }

    /// Wait until every registry table's Delta log has been replayed, so
    /// maintenance is never the first cold loader of a table the foreground
    /// needs. The atomic closes the `Notify` check/subscribe race and makes late
    /// subscribers return immediately.
    ///
    /// Deliberately does NOT wait for the paced body warm behind the replay —
    /// that warm is paced to be safe beside other work. Bounded by
    /// `timefusion_coordinator_preload_wait_secs`; `false` means "give up on
    /// this worker" and is returned ONLY for cancellation, so a merely-slow
    /// replay still returns `true`.
    async fn wait_for_preload(&self, cancel: &CancellationToken) -> bool {
        let budget = self.config.maintenance.timefusion_coordinator_preload_wait_secs;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(budget);
        loop {
            if self.preload_replay_complete.load(std::sync::atomic::Ordering::Acquire) {
                return true;
            }
            if budget == 0 || tokio::time::Instant::now() >= deadline {
                warn!(
                    event = "maintenance_coordinator_preload_wait_expired",
                    waited_secs = budget,
                    tables_done = self.preload_tables_done.load(std::sync::atomic::Ordering::Relaxed),
                    tables_total = self.preload_tables_total.load(std::sync::atomic::Ordering::Relaxed),
                    "starting maintenance before the table replay finished; it would otherwise never start on this container"
                );
                return true;
            }
            let notified = self.preload_replay_notify.notified();
            if self.preload_replay_complete.load(std::sync::atomic::Ordering::Acquire) {
                return true;
            }
            tokio::select! {
                _ = cancel.cancelled() => return false,
                _ = notified => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// Atomically swap a freshly-optimized `new_table` in under the write lock, then refresh the
    /// cache for the file-set delta vs `pre_uris`: warm files added and evict files tombstoned.
    ///
    /// `pre_uris` is `None` when the caller isn't tracking the file set, so the diff is skipped.
    /// `scope` MUST match the partition markers used for `pre_uris`; otherwise a scoped pre-set
    /// diffed against an unscoped live set would warm every other partition.
    async fn swap_and_refresh_cache(
        &self, table_ref: &Arc<RwLock<DeltaTable>>, new_table: DeltaTable, pre_uris: Option<&HashSet<String>>, scope: &[&str],
    ) -> Vec<String> {
        // Capture live URIs off `new_table` *before* the swap moves it in.
        let live_uris: Vec<String> = scoped_file_uris(&new_table, scope);
        let (added, removed): (Vec<String>, Vec<String>) = pre_uris.map_or_else(
            || (Vec::new(), Vec::new()),
            |pre| {
                let live_set: HashSet<&str> = live_uris.iter().map(String::as_str).collect();
                (live_uris.iter().filter(|u| !pre.contains(*u)).cloned().collect(), pre.iter().filter(|u| !live_set.contains(u.as_str())).cloned().collect())
            },
        );
        let warm_store = new_table.log_store().object_store(None);
        let warm_table_uri = new_table.table_url().to_string();
        self.persist_snapshot(&new_table);
        {
            // Version-guarded so this is safe WITHOUT the commit lock: a
            // concurrent committer may already have advanced `table_ref` past
            // our version, and a bare assignment would regress the handle.
            let mut table = table_ref.write().await;
            if new_table.version() > table.version() {
                *table = new_table;
            }
        }
        // WARM BEFORE EVICT, always: evicting first cold-starts the hottest
        // query window. Detached (warming issues S3 GETs), with eviction behind
        // the warm in the same task so the ordering holds without blocking.
        let db = self.clone();
        tokio::spawn(async move {
            let uri = warm_table_uri;
            db.warm_cache_for_uris(warm_store, uri.clone(), added, None, true).await;
            db.evict_cache_for_uris(&uri, &removed);
        });
        live_uris
    }

    pub async fn get_or_create_table(&self, project_id: &str, table_name: &str) -> Result<Arc<RwLock<DeltaTable>>> {
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
    /// commit-log request class can get its own client. Every other setting is
    /// identical by construction.
    pub async fn create_object_store_with_timeout(
        &self, storage_uri: &str, storage_options: &HashMap<String, String>, request_timeout: String,
    ) -> Result<Arc<dyn object_store::ObjectStore>> {
        use std::time::Duration;

        use object_store::{BackoffConfig, ClientConfigKey, ClientOptions, RetryConfig, aws::AmazonS3Builder};

        let url = Url::parse(storage_uri)?;
        let bucket = url.host_str().ok_or_else(|| anyhow::anyhow!("Invalid S3 URI: missing bucket"))?;

        let retry_config = RetryConfig {
            max_retries: 5,
            retry_timeout: Duration::from_secs(180),
            backoff: BackoffConfig { init_backoff: Duration::from_millis(100), max_backoff: Duration::from_secs(15), base: 2.0 },
        };

        // Timeouts come from config and must match `build_storage_options` rather
        // than being hardcoded. PoolMaxIdlePerHost keeps connections warm so
        // concurrent uploads reuse sockets instead of re-establishing TLS.
        let client_options = ClientOptions::new()
            .with_config(ClientConfigKey::ConnectTimeout, self.config.aws.connect_timeout())
            .with_config(ClientConfigKey::Timeout, request_timeout)
            .with_config(ClientConfigKey::PoolMaxIdlePerHost, crate::config::S3_POOL_MAX_IDLE_PER_HOST.to_string());

        let builder = AmazonS3Builder::new().with_bucket_name(bucket).with_retry(retry_config).with_client_options(client_options);
        let pick = |key: &str, fallback: Option<&String>| storage_options.get(key).or(fallback).cloned();
        let endpoint = storage_options.get("AWS_ENDPOINT_URL").unwrap_or(&self.config.aws.aws_s3_endpoint);
        let builder = pick("AWS_ACCESS_KEY_ID", self.config.aws.aws_access_key_id.as_ref()).into_iter().fold(builder, AmazonS3Builder::with_access_key_id);
        let builder =
            pick("AWS_SECRET_ACCESS_KEY", self.config.aws.aws_secret_access_key.as_ref()).into_iter().fold(builder, AmazonS3Builder::with_secret_access_key);
        let builder = pick("AWS_REGION", self.config.aws.aws_default_region.as_ref()).into_iter().fold(builder, AmazonS3Builder::with_region);
        // An http:// endpoint must also allow plaintext connections.
        let builder = builder.with_endpoint(endpoint).with_allow_http(endpoint.starts_with("http://"));

        Ok(Arc::new(builder.build()?))
    }
}

/// Build the shared query `RuntimeEnv`: the global memory pool plus the
/// decoded-parquet-metadata cache limit. The limit MUST be set on the builder
/// here — `datafusion.runtime.metadata_cache_limit` on the SessionConfig does
/// NOT reconfigure an already-built RuntimeEnv and silently falls back to the
/// 50MB default, making every scan re-decode the footer + page index.
fn build_query_runtime_env(
    pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool>, metadata_cache_bytes: usize, disk: datafusion::execution::disk_manager::DiskManagerBuilder,
) -> datafusion::execution::runtime_env::RuntimeEnv {
    datafusion::execution::runtime_env::RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_metadata_cache_limit(metadata_cache_bytes)
        .with_disk_manager_builder(disk)
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
    /// Sort parallelism for this attempt — see [`dedup_sort_partitions`].
    sort_partitions: usize,
    /// Rows per scan batch, derived from this unit's measured row width — see
    /// [`crate::database::batch_rows_for`].
    batch_rows: usize,
}

#[derive(Clone)]
pub(crate) struct DedupRangeOptions {
    slice: Option<crate::maintenance_coordinator::TimeSlice>,
    dirty_key: Option<DirtyBinKey>,
    limits: Option<DedupExecutionLimits>,
}

pub(crate) struct HotStageOptions {
    pass: TailPass,
    /// Coordinator lane that owns this rewrite. The cron-driven wave engine has
    /// no coordinator operation, so it leaves this as `None` and `pass` remains
    /// the useful label. Keeping both on the staging events is what lets us tell
    /// whether a scarce light permit is draining sealed debt or today's tail.
    operation: Option<crate::maintenance_coordinator::Operation>,
    runtime_env: Option<Arc<datafusion::execution::runtime_env::RuntimeEnv>>,
    /// A `light_rewrite_sem` permit the CALLER already holds. `None` means
    /// `stage_hot_bin` blocks for one itself — right for the wave engine, wrong
    /// for a coordinator unit whose deadline is already running. Ownership is
    /// moved in so "the caller holds it" cannot be asserted falsely.
    light_permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

struct CompactionDebtFile {
    size: i64,
    path: String,
}

/// Arrow ONE dedup bin may hold across all its concurrent shards. Shard
/// concurrency is funded by shrinking the shard, never by raising this, so peak
/// Arrow per bin stays what `maintenance_rewrite_sem` was sized against.
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
/// The `streaming` branch writes straight through and spills under the memory pool, so it is
/// exempt from the decoded-bytes budget; the collecting branch materialises the partition and
/// keeps the bound.
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
/// row groups itself, so this only bounds our transient copy.
const MERGE_CHUNK_BYTES: usize = 8 * 1024 * 1024;
const MERGE_CHUNK_ROWS_MIN: usize = 1_024;
const MERGE_CHUNK_ROWS_MAX: usize = 65_536;

/// May this caller write a group UNSORTED when the sort can't be completed
/// within budget?
///
/// The asymmetry is the point. An unsorted file declares no `sorting_columns`
/// footer, and the reader's `derive_common_ordering` is all-or-nothing — ONE
/// such file voids the declared ordering for every scan touching that
/// partition, and nothing ever re-sorts a converged file, so the cost is
/// permanent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsortedFallback {
    /// INGEST only: the rows exist nowhere else yet, so refusing would lose data.
    Allow,
    /// REWRITE paths (dedup / consolidate / compact). Their inputs are already
    /// committed and sorted, so failing costs one retry cycle while an unsorted
    /// output is an unrecoverable partition-wide read regression.
    Forbid,
}

/// Batches a flush/rewrite writes, either eagerly held or produced on demand by
/// the streaming sort-merge. The merge frees each sorted run as it drains, so
/// the bucket is never materialized twice.
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

/// K-way merge of individually sorted runs, emitting chunk-sized batches. Ties
/// on every sort key break by run index; nothing downstream reads tie order.
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

/// `a`'s head sorts before `b`'s. Ties break by run index, which is what makes
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
        let min = [2 * i + 1, 2 * i + 2].into_iter().filter(|&c| c < heap.len()).fold(i, |m, c| if head_less(heap[c], heap[m], keys, pos) { c } else { m });
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
        // Free drained runs and their encoded keys, or the merge holds the whole
        // bucket to the end.
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
    // Already ordered (common: ~monotonic timestamp) → skip the take copy.
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

/// Unify schema-diverse `RecordBatch`es to one lossless superset schema.
/// `None` on merge or cast failure — the caller then writes the originals
/// unsorted.
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
    let bail = |reason: String, b: Vec<RecordBatch>| {
        warn!("sort_batches_by_schema: {reason}");
        unsorted(b)
    };
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return unsorted(batches);
    }
    // `skip_over_bytes` budgets the in-process sort in in-memory ARROW bytes,
    // not file bytes (they differ by ~17x on zstd'd otel data). Oversize groups
    // write unsorted; scheduled compaction re-sorts them later.
    let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    if total_bytes > skip_over_bytes {
        return unsorted(batches);
    }
    // Unify to one superset schema so the bucket still flushes as ONE globally
    // sorted file with an honest `sorting_columns` footer.
    let Some((arrow_schema, batches)) = unify_batch_schemas(batches.clone()) else {
        return bail("schema unify failed, writing unsorted".into(), batches);
    };
    let sort_idx: Vec<(usize, &crate::schema::SortingColumnDef)> =
        schema.sorting_columns.iter().filter_map(|sc| arrow_schema.index_of(&sc.name).ok().map(|i| (i, sc))).collect();
    if sort_idx.is_empty() {
        return unsorted(batches);
    }
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // Small buckets: one concat + sort beats encoding rows and running a heap.
    if batches.len() == 1 || total_rows <= MERGE_MIN_ROWS {
        let combined = if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            match concat_batches(&arrow_schema, &batches) {
                Ok(c) => c,
                Err(e) => return bail(format!("concat failed, writing unsorted: {e}"), batches),
            }
        };
        return match sort_one_batch(&combined, &sort_idx) {
            Ok(sorted) => (FlushBatches::Ready(vec![sorted].into_iter()), true),
            Err(e) => bail(format!("sort failed, writing unsorted: {e}"), vec![combined]),
        };
    }
    // Streaming path: sort each run, then k-way merge into writer-sized chunks.
    // Any setup failure BEFORE the first chunk downgrades to unsorted, because
    // `sorted` is decided here, up front. Once the merge starts the writer is
    // already configured with a sorted footer, so a mid-merge error must
    // propagate and fail the flush rather than write a dishonest file.
    let fields: Vec<SortField> = sort_idx
        .iter()
        .map(|(i, sc)| {
            SortField::new_with_options(arrow_schema.field(*i).data_type().clone(), SortOptions { descending: sc.descending, nulls_first: sc.nulls_first })
        })
        .collect();
    let converter = match RowConverter::new(fields) {
        Ok(c) => c,
        Err(e) => return bail(format!("row converter unavailable, writing unsorted: {e}"), batches),
    };
    let mut merge = SortMergeStream {
        schema: arrow_schema.clone(),
        converter,
        runs: Vec::with_capacity(batches.len()),
        keys: Vec::with_capacity(batches.len()),
        pos: Vec::with_capacity(batches.len()),
        heap: Vec::with_capacity(batches.len()),
        chunk_rows: (MERGE_CHUNK_BYTES / (total_bytes / total_rows.max(1)).max(1)).clamp(MERGE_CHUNK_ROWS_MIN, MERGE_CHUNK_ROWS_MAX),
    };
    // Consume the input as we go, so the bucket is never resident twice.
    let mut rest = batches.into_iter();
    let failed = rest.by_ref().find_map(|batch| {
        if batch.num_rows() == 0 {
            return None;
        }
        match sort_one_batch(&batch, &sort_idx).and_then(|run| {
            let key_cols: Vec<_> = sort_idx.iter().map(|(i, _)| run.column(*i).clone()).collect();
            merge.converter.convert_columns(&key_cols).map(|keys| (run, keys))
        }) {
            Ok((run, keys)) => {
                merge.runs.push(run);
                merge.keys.push(keys);
                merge.pos.push(0);
                None
            }
            Err(e) => Some((batch, e)),
        }
    });
    if let Some((batch, e)) = failed {
        // Keep every row: sorted-so-far runs plus this batch and the rest go out
        // unsorted (same rows, no order claim).
        let leftover = std::mem::take(&mut merge.runs).into_iter().chain(std::iter::once(batch)).chain(rest).collect();
        return bail(format!("run sort/encode failed, writing unsorted: {e}"), leftover);
    }
    if merge.runs.is_empty() {
        return unsorted(Vec::new());
    }
    for run in 0..merge.runs.len() {
        merge.heap.push(run);
        sift_up(&mut merge.heap, run, &merge.keys, &merge.pos);
    }
    (FlushBatches::Merge(merge), true)
}

/// Reference implementation of [`sort_batches_by_schema`]: concat, one global
/// `lexsort_to_indices`, one `take`. The equivalence oracle the property test
/// compares against — the streaming path must reproduce this row order exactly.
#[cfg(test)]
fn sort_batches_by_schema_reference(schema: &crate::schema::TableSchema, batches: Vec<RecordBatch>) -> (Vec<RecordBatch>, bool) {
    use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch};
    if batches.is_empty() || schema.sorting_columns.is_empty() {
        return (batches, false);
    }
    let Some((arrow_schema, batches)) = unify_batch_schemas(batches.clone()) else {
        return (batches, false);
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
/// `sorting_columns`; empty when the table declares none (caller falls back to
/// `Compact`).
fn schema_optimize_sort_columns(schema: &crate::schema::TableSchema) -> Vec<deltalake::operations::optimize::SortColumn> {
    schema
        .sorting_columns
        .iter()
        .map(|c| deltalake::operations::optimize::SortColumn { column: c.name.clone(), descending: c.descending, nulls_first: c.nulls_first })
        .collect()
}

/// Columns that get per-file min/max/nullCount stats in the Delta log
/// (`delta.dataSkippingStatsColumns`). Deliberately narrow — stats over the
/// whole wide schema cost a large share of process CPU parsing Add stats on
/// every log replay. Partition columns are excluded: they're in the path.
fn stats_columns_for(schema: &crate::schema::TableSchema) -> String {
    std::iter::once(schema.time_column_name())
        .chain(schema.sorting_columns.iter().map(|c| c.name.as_str()))
        .chain(schema.dedup_keys.iter().map(String::as_str))
        .chain(schema.dedup_tiebreak.as_deref())
        .filter(|c| !schema.partitions.iter().any(|p| p == c))
        .unique()
        .join(",")
}

/// **DECODED** bytes one repair slice may cover — the unit the sort allocates,
/// NOT compressed input bytes. Sized from the pool invariant: `target x
/// TIMEFUSION_MAINTENANCE_REWRITE_CONCURRENCY` must stay under about half the
/// query pool, since the sort merge cannot spill.
///
/// Test-only: slicing is currently off (see `coordinator_slice_target`), so this
/// is retained as the value the sizing invariants are asserted against.
#[cfg(test)]
const REPAIR_SLICE_DECODED_TARGET_BYTES: i64 = 1024 * 1024 * 1024;

/// Row 0 of `col` as an i64, `None` when it is NULL or doesn't cast. Casts
/// rather than reinterpreting, so Timestamp(µs) columns come through honestly.
fn first_i64(col: &dyn arrow::array::Array) -> Option<i64> {
    if col.is_null(0) {
        return None;
    }
    arrow::compute::kernels::cast::cast(col, &arrow_schema::DataType::Int64)
        .ok()
        .map(|c| arrow::array::AsArray::as_primitive::<arrow::datatypes::Int64Type>(&c).value(0))
}

/// Run a single-row aggregate probe, yielding the first non-empty batch. `None`
/// on any planning/execution failure — every caller declines rather than fails.
async fn probe_row(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Option<RecordBatch> {
    ctx.sql(sql).await.ok()?.collect().await.ok()?.into_iter().find(|b| b.num_rows() > 0)
}

async fn bin_time_range(ctx: &datafusion::prelude::SessionContext, probe: &str) -> Option<(i64, i64)> {
    let batch = probe_row(ctx, probe).await?;
    let int_at = |i: usize| first_i64(batch.column(i));
    let (lo, hi) = (int_at(0)?, int_at(1)?);
    // Any NULL in the sort column declines slicing outright: it would fall
    // outside every range and be dropped.
    int_at(2).is_none_or(|nulls| nulls == 0).then_some((lo, hi))
}

/// Split `[lo, hi]` into `slices` half-open ranges, last one inclusive of `hi`.
/// Returns `(lo, hi_exclusive_or_none)` pairs in ASCENDING order.
///
/// Slice by TIME, never by row count: slices are appended to one writer in the
/// output's sort direction, so the concatenation stays globally sorted and every
/// `sorting_columns` footer remains true. Row-count slices would overlap, and
/// two versions of one key could land in files keep-greatest can't compare.
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
/// Equal time spans do not bound memory when rows clump by time. Quantile cuts stay monotone and
/// contiguous in the sort column's order, so concatenation stays globally sorted, while flattening
/// rows per slice. Approximate cuts are fine — the row-count guard verifies the rewrite anyway.
async fn repair_slice_cuts(ctx: &datafusion::prelude::SessionContext, bin_table: &str, col: &str, slices: usize) -> Vec<i64> {
    if slices <= 1 {
        return Vec::new();
    }
    let exprs = (1..slices).map(|i| format!("approx_percentile_cont(arrow_cast(\"{col}\", 'Int64'), {})", i as f64 / slices as f64)).join(", ");
    let Some(batch) = probe_row(ctx, &format!("SELECT {exprs} FROM {bin_table}")).await else {
        return Vec::new();
    };
    // Heavy ties collapse neighbouring quantiles onto one value; deduping yields
    // fewer, larger slices, whereas an overlap would duplicate rows.
    (0..batch.num_columns()).filter_map(|i| first_i64(batch.column(i))).sorted_unstable().dedup().collect()
}

/// Turn interior cut points into the same half-open, ascending tiling
/// `repair_slice_bounds` produces: `[lo, c1), [c1, c2), ... [cn, +inf)`.
fn repair_bounds_from_cuts(lo: i64, hi: i64, cuts: &[i64]) -> Vec<(i64, Option<i64>)> {
    // Deduped here rather than trusting the caller: a repeated cut is an empty
    // range that still costs a full scan.
    let interior: Vec<i64> = cuts.iter().copied().filter(|&c| c > lo && c <= hi).sorted_unstable().dedup().collect();
    if interior.is_empty() {
        return vec![(lo, None)];
    }
    let starts = std::iter::once(lo).chain(interior.iter().copied());
    let ends = interior.iter().copied().map(Some).chain(std::iter::once(None));
    starts.zip(ends).collect()
}

fn schema_order_by_clause(schema: &crate::schema::TableSchema) -> String {
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
    if cols.is_empty() { String::new() } else { format!(" ORDER BY {cols}") }
}

/// One staged bin's intent line in the staged-intent manifest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct StagedIntent {
    wave_id: String,
    /// Owning table — reconcile/clear MUST act only on their own table's
    /// entries, or another table's entries are judged "orphan" against the
    /// wrong snapshot and cleared.
    #[serde(default)]
    table_name: String,
    project_id: String,
    /// Unix seconds at staging. Reconcile skips young entries: during an
    /// overlapping rolling deploy a booting instance must not delete parquet
    /// another live instance staged but hasn't committed.
    #[serde(default)]
    recorded_at: u64,
    paths: Vec<String>,
    /// Input files this staged output replaces. Empty on pre-resume and dedup
    /// entries, which are then cleanup-only: enough to delete an orphan, not
    /// enough to commit one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    target_paths: Vec<String>,
    /// The staged Add actions verbatim, so a resume rebuilds the bin without
    /// re-reading a single footer.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    adds: Vec<deltalake::kernel::Add>,
    /// Present ONLY on a rollup unit's staged output. Absence is structural, not
    /// a sentinel: repair, dedup and pre-upgrade entries all decode to `None`,
    /// which is never committable as a rollup.
    #[serde(default, skip_serializing_if = "Option::is_none", serialize_with = "serialize_rollup_resume", deserialize_with = "deserialize_rollup_resume")]
    rollup: Option<RollupResume>,
    /// WHICH instance staged this (`crate::observability::instance_id`). A
    /// different id proves some other process staged it and this one is not
    /// mid-staging it; `None` predates the field. See [`resume_guarded`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    instance: Option<String>,
}

/// What a killed rollup unit needs in order to be COMMITTED on the next boot
/// rather than redone.
///
/// `classify_resume`'s row-preservation test cannot be reused: a rollup
/// AGGREGATES, so it would be refused by construction. A rollup carries its own
/// two-sided evidence instead:
///
/// - the TARGET side asks "would committing this double-count?", answered by
///   requiring the live files overlapping the slice to be exactly the replace
///   set recorded at staging;
/// - the SOURCE side asks "is this output still what a read would be served?",
///   answered by the source row witness and the selected input's content fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct RollupResume {
    /// The journal unit this output belongs to. A resumed Delta commit MUST also
    /// be published: coverage recovery needs `rollup_slice_complete`, or the
    /// planner sees a hole, re-enqueues the slice and retires the resumed files.
    key: crate::maintenance_coordinator::TaskKey,
    publication: crate::maintenance_coordinator::Publication,
    /// The source DATE partition's `num_records` when this was built. `None` is
    /// an unverifiable build and must never resume — same rule the read path
    /// applies to a witness-less slice.
    source_rows: Option<u64>,
    /// Exact target-partition metadata around the atomic publication. Legacy
    /// replacement intents without this proof cannot safely resume a removal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    target: Option<RollupTargetProof>,
    date: String,
}

// Older readers must reject new evidence, not silently resume without checking it.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
enum RollupResumeWire<T> {
    V2 { v2: T },
    Legacy(T),
}

fn serialize_rollup_resume<S: serde::Serializer>(value: &Option<RollupResume>, serializer: S) -> std::result::Result<S::Ok, S::Error> {
    serde::Serialize::serialize(&value.as_ref().map(|v2| RollupResumeWire::V2 { v2 }), serializer)
}

fn deserialize_rollup_resume<'de, D: serde::Deserializer<'de>>(deserializer: D) -> std::result::Result<Option<RollupResume>, D::Error> {
    let value: Option<RollupResumeWire<RollupResume>> = serde::Deserialize::deserialize(deserializer)?;
    Ok(value.map(|wire| match wire {
        RollupResumeWire::V2 { v2 } => v2,
        RollupResumeWire::Legacy(body) => body,
    }))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RollupTargetProof {
    before: u64,
    after: u64,
}

/// Frozen persisted digest; ordering of files and map entries carries no meaning.
fn rollup_target_fingerprint<'a>(files: impl IntoIterator<Item = &'a deltalake::kernel::Add>) -> u64 {
    use itertools::Itertools;
    let summary = files.into_iter().fold((0u64, 0u64), |(count, fingerprint), add| {
        let identity = (
            maintain::file_content_hash(&add.path, add.deletion_vector.as_ref()),
            add.deletion_vector.as_ref().map(|dv| dv.storage_type.as_ref()),
            add.size,
            add.modification_time,
            &add.stats,
            add.partition_values.iter().sorted_unstable().collect::<Vec<_>>(),
            add.tags.as_ref().into_iter().flat_map(|tags| tags.iter()).sorted_unstable().collect::<Vec<_>>(),
        );
        (count.saturating_add(1), fingerprint ^ maintain::digest_of(identity))
    });
    maintain::digest_of(summary)
}

/// Parse the append-only manifest, SKIPPING any line that doesn't decode. The
/// manifest is a cleanup aid, never a correctness input, so a torn tail from an
/// unclean shutdown must degrade to "fewer entries", never to a boot failure.
fn parse_staged_intents(contents: &str) -> Vec<StagedIntent> {
    contents.lines().filter(|l| !l.trim().is_empty()).filter_map(|line| serde_json::from_str::<StagedIntent>(line).ok()).collect()
}

/// Entries younger than this may belong to a live instance sharing the volume
/// (rolling deploy), so boot-time cleanup leaves them for their own wave commit,
/// the next boot, or VACUUM.
const STAGED_INTENT_MIN_AGE_SECS: u64 = 30 * 60;

/// Must this entry be left alone for now, i.e. might we ourselves still be
/// staging it?
///
/// - a DIFFERENT id is another process's staging, and entries are only appended
///   after the last parquet flush returns, so it is eligible immediately.
/// - OUR id may still be in flight here, so it keeps the wall-clock gate.
/// - NO id predates this field, so it keeps the gate too.
///
/// Deliberately NOT used by `staged_orphan_deletions`: identity proves "not
/// ours", never "its owner is dead", and deleting another instance's parquet
/// before its commit destroys the work.
fn resume_guarded(entry: &StagedIntent, now_secs: u64) -> bool {
    entry.instance.as_deref().is_none_or(|id| id == crate::observability::instance_id())
        && now_secs.saturating_sub(entry.recorded_at) < STAGED_INTENT_MIN_AGE_SECS
}

fn staged_orphan_deletions(entries: &[StagedIntent], table_name: &str, now_secs: u64, referenced: &HashSet<String>) -> Vec<String> {
    entries
        .iter()
        .filter(|e| e.table_name == table_name && now_secs.saturating_sub(e.recorded_at) >= STAGED_INTENT_MIN_AGE_SECS)
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
    /// ROLLUP only: the source partition moved under the staged output, so the
    /// read path would refuse this slice the moment it landed. Discard it.
    SourceMoved,
    /// ROLLUP only: the tier already holds a file overlapping this slice that
    /// the staged output does not replace. Committing would leave both live and
    /// a reader would SUM them.
    WouldDoubleCount,
    Commit,
}

/// The guard both resume classifiers apply first; `None` means neither branch
/// fires. On an overlapping rolling deploy the staging instance may still be
/// alive and about to commit its own output — `resume_guarded` answers that by
/// ownership. Staged parquet is uuid-named by the writer, so nobody else can
/// produce those paths: live ⇒ our commit landed.
fn resume_precheck<V>(entry: &StagedIntent, table_name: &str, now_secs: u64, live: &HashMap<&str, V>) -> Option<ResumeVerdict> {
    if entry.table_name != table_name || entry.adds.is_empty() || resume_guarded(entry, now_secs) {
        return Some(ResumeVerdict::Skip);
    }
    entry.adds.iter().all(|a| live.contains_key(a.path.as_str())).then_some(ResumeVerdict::AlreadyLanded)
}

/// Should a killed rollup unit's staged output be committed on the next boot?
///
/// IO-free, like [`classify_resume`], so every branch is unit-testable; the
/// caller supplies the target tier's live files and the source partition's
/// CURRENT row count and selected input content fingerprint.
///
/// A rollup aggregates, so the repair path's row-preservation test does not
/// apply. Two checks stand in for it, both refusals by default:
///
/// 1. **Source witness.** The build recorded the source partition's
///    `num_records` and input content fingerprint must both match. File paths
///    alone miss deletion-vector changes; row counts miss same-count mutations.
///    Missing fingerprint evidence declines legacy intents conservatively.
/// 2. **No double-count.** Every live file whose tagged range overlaps this
///    slice must be one the staged output replaces, or both stay live and their
///    rows get summed.
///
/// `live` maps every path in the target partition to the slice range its tags
/// claim, `None` for an untagged file — which claims no range and therefore
/// cannot double-count.
fn classify_rollup_resume(
    entry: &StagedIntent, table_name: &str, now_secs: u64, live: &HashMap<&str, Option<(i64, i64)>>, current_source_rows: Option<u64>, current_content_fp: u64,
) -> ResumeVerdict {
    let Some(rollup) = entry.rollup.as_ref() else { return ResumeVerdict::Skip };
    let precheck = resume_precheck(entry, table_name, now_secs, live);
    match precheck {
        Some(ResumeVerdict::AlreadyLanded) | None => {}
        Some(verdict) => return verdict,
    }
    // No witness ⇒ unverifiable; a witness that no longer matches ⇒ the source moved.
    if !matches!((rollup.source_rows, current_source_rows), (Some(built_from), Some(current)) if built_from == current)
        || rollup.publication.evidence.as_ref().is_none_or(|evidence| evidence.content_fp != current_content_fp)
    {
        return ResumeVerdict::SourceMoved;
    }
    // A landed artifact is not permission to reactivate stale coverage.
    if let Some(verdict) = precheck {
        return verdict;
    }
    let replaced: HashSet<&str> = entry.target_paths.iter().map(String::as_str).collect();
    let (start, end) = (rollup.key.slice.start_micros, rollup.key.slice.end_micros);
    let overlaps = |(lo, hi): (i64, i64)| lo < end && hi > start;
    if live.iter().any(|(path, slice)| slice.is_some_and(overlaps) && !replaced.contains(path)) {
        return ResumeVerdict::WouldDoubleCount;
    }
    // Checked LAST so an input that left the snapshot is reported as staleness
    // rather than as a double-count.
    if !replaced.iter().all(|path| live.contains_key(path)) {
        return ResumeVerdict::Stale;
    }
    ResumeVerdict::Commit
}

/// `live` maps every path in the current snapshot to its `numRecords`
/// (`None` = the Add carries no stats, which makes row preservation
/// unverifiable and is therefore never committable).
fn classify_resume(entry: &StagedIntent, table_name: &str, now_secs: u64, live: &HashMap<&str, Option<i64>>) -> ResumeVerdict {
    if entry.target_paths.is_empty() {
        return ResumeVerdict::Skip;
    }
    if let Some(verdict) = resume_precheck(entry, table_name, now_secs, live) {
        return verdict;
    }
    if !entry.target_paths.iter().all(|p| live.contains_key(p.as_str())) {
        return ResumeVerdict::Stale;
    }
    let target_rows: Option<i64> = entry.target_paths.iter().filter_map(|p| live.get(p.as_str())).copied().sum();
    let staged_rows: Option<i64> = entry.adds.iter().map(|a| a.get_stats().ok().flatten().map(|s| s.num_records)).sum();
    match (target_rows, staged_rows) {
        // A repair is data-preserving by construction; this equality is what stands
        // between a killed-mid-staging bin and silent row loss.
        (Some(t), Some(s)) if t == s => ResumeVerdict::Commit,
        (t, s) => ResumeVerdict::RowMismatch { target_rows: t.unwrap_or(-1), staged_rows: s.unwrap_or(-1) },
    }
}

/// One bin rewritten to staged parquet but NOT yet committed. Uncommitted Adds
/// are invisible to Delta readers, so a wave can hold several of these while
/// other bins finish; the wave commit turns them all into one transaction.
pub(crate) struct StagedBin {
    project_id: String,
    /// Manifest key for this bin's staged files (see `record_staged_intent`).
    wave_id: String,
    /// All files read while staging, including DV dedup survivors that need no
    /// Remove. Re-verify their exact logical identities under the commit lock.
    targets: Vec<deltalake::kernel::Add>,
    removes: Vec<deltalake::kernel::Action>,
    adds: Vec<deltalake::kernel::Action>,
    /// Store the staged parquet was written to, for `cleanup_orphaned_parquet`.
    stage_store: Arc<dyn object_store::ObjectStore>,
    /// Paths (relative to `stage_store`) to delete when this bin is DISCARDED,
    /// beyond the live-checked `adds` cleanup. Empty for copy-on-write bins. A
    /// DV-dedup bin's `adds` are SAME-PATH live files that must never be deleted;
    /// its discardable set is instead the fresh-UUID `.bin` deletion-vector
    /// sidecars `write_deletion_vectors` wrote. Cleanup preserves any sidecar
    /// already referenced by a landed part of the wave.
    discardable_paths: Vec<String>,
    /// Whether this bin's `adds` were written with a declared `sorting_columns`
    /// footer. False is always SAFE: it costs a footer probe later, never a wrong
    /// exoneration.
    sorted: bool,
    /// Dedup-only accounting; `None` for hot compaction bins. Also THE source of
    /// truth for `data_change` — a dedup unit drops rows by definition, a
    /// compaction unit cannot.
    dedup: Option<DedupUnit>,
}

impl StagedBin {
    /// Derived, never stored: a second `data_change` field could disagree with
    /// `dedup`, which would mean committing a row-dropping rewrite under snapshot
    /// isolation.
    fn data_change(&self) -> bool {
        self.dedup.is_some()
    }

    /// A DV-dedup bin masks its losers IN PLACE: every Add re-adds a live path
    /// with a deletion vector attached. A CoW rewrite's adds never carry a DV.
    /// Derived, never stored — a second field could disagree.
    fn masked_in_place(&self) -> bool {
        self.dedup.is_some()
            && !self.adds.is_empty()
            && self.adds.iter().all(|a| matches!(a, deltalake::kernel::Action::Add(add) if add.deletion_vector.is_some()))
    }
}

/// One live file-action identity: `(path, dv_unique_id)` — the pair Delta log
/// replay keys file actions on. The certification fingerprint hashes URIs only,
/// so it is blind to a same-path DV commit; the DV-visibility guard compares
/// sets of these instead. Computed live, never persisted.
pub(crate) type DvEntry = (String, Option<String>);

/// Identity of one DV attachment, per the protocol's uniqueId derivation
/// (storageType + pathOrInlineDv + offset). Two distinct DV writes always
/// differ: the DV blob is a fresh UUID-named file.
fn dv_identity(d: &deltalake::kernel::DeletionVectorDescriptor) -> String {
    format!("{}{}@{}", d.storage_type, d.path_or_inline_dv, d.offset.unwrap_or(0))
}

/// Per-unit outcome of one wave commit. Per-unit (not a count) because dedup
/// has to requeue exactly the dirty bins that did NOT land.
pub(crate) struct WaveResult {
    landed: Vec<StagedBin>,
    failed: Vec<StagedBin>,
}

/// Match a wave's committed Add actions to the absolute URIs from the exact
/// post-commit snapshot. Index manifests store absolute URIs; Add actions use
/// table-relative paths.
fn wave_added_parquet(bins: &[StagedBin], live_uris: &[String]) -> Vec<(String, String, String)> {
    let by_rel: HashMap<&str, &str> =
        live_uris.iter().filter_map(|uri| crate::tantivy::search::parquet_rel_of_uri(uri).map(|rel| (rel, uri.as_str()))).collect();
    bins.iter()
        .flat_map(|bin| {
            bin.adds.iter().filter_map(|action| {
                let deltalake::kernel::Action::Add(add) = action else { return None };
                let uri = by_rel.get(add.path.as_str())?;
                Some((bin.project_id.clone(), add.path.clone(), (*uri).to_owned()))
            })
        })
        .collect()
}

/// Deferred manifest entries a backfill commits in one write.
const MANIFEST_BATCH: usize = 8;

/// A deferred entry is never left unwritten longer than this: durability, not
/// batching, is the binding constraint — a pass may be killed before it ends.
const MANIFEST_MAX_AGE: std::time::Duration = std::time::Duration::from_secs(60);

/// Which projects' deferred manifest entries are due to be committed. Must
/// consider EVERY pending project, not just the one that just built, or the age
/// bound is unreachable for a project with a single build per pass.
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

    /// A project that built once and then went quiet must still flush on age.
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

/// Which table a rotating maintenance pass should start with, derived from the
/// clock so a restart cannot reset it to the same starving order.
///
/// One 15-minute slot per position: with N tables every table leads a pass once
/// every `15 * N` minutes, so a pass that dies part-way still serves a different
/// prefix next time.
///
/// ```
/// # use timefusion::database::rotation_offset;
/// const SLOT: i64 = 15 * 60 * 1_000_000;
/// assert_eq!(rotation_offset(0, 3), 0);
/// assert_eq!(rotation_offset(SLOT, 3), 1, "the next slot leads with the next table");
/// assert_eq!(rotation_offset(3 * SLOT, 3), 0, "and wraps");
/// assert_eq!(rotation_offset(SLOT, 0), 0, "no tables: no panic on %0");
/// ```
pub fn rotation_offset(now_micros: i64, len: usize) -> usize {
    (now_micros / (15 * 60 * 1_000_000)).unsigned_abs() as usize % len.max(1)
}

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

/// What a dedup unit carries through a wave: the rows it drops (reported once
/// the unit LANDS, never at staging time) and the dirty-bin it came from, so a
/// unit that loses the liveness check or the commit is requeued instead of
/// certifying a partition that still holds duplicates.
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
/// landed contribute nothing — no commit references their staged copy.
fn wave_dropped_rows(bins: &[StagedBin]) -> u64 {
    bins.iter().filter_map(|b| b.dedup.as_ref()).map(DedupUnit::dropped).sum()
}

/// Assemble one staged unit's Remove+Add actions.
///
/// `data_change` distinguishes hot compaction from dedup. `false` (compaction) preserves every row,
/// so the conflict checker downgrades to snapshot isolation and a wave can commit next to
/// concurrent ingest appends. `true` (dedup) drops rows, so those actions must not claim
/// data-preserving. `wave_operation` derives the `DeltaOperation` from the same flag.
fn staged_actions(
    targets: &[deltalake::kernel::Add], staged: Vec<deltalake::kernel::Action>, data_change: bool,
) -> (Vec<deltalake::kernel::Action>, Vec<deltalake::kernel::Action>) {
    use deltalake::kernel::Action;
    let removes: Vec<Action> = targets.iter().map(|a| Action::Remove(remove_for_add(a, data_change))).collect();
    let adds: Vec<Action> = staged
        .into_iter()
        .update(|a| {
            if let Action::Add(add) = a {
                add.data_change = data_change
            }
        })
        .collect();
    (removes, adds)
}

/// The operation a wave commits under, derived from [`staged_actions`]'s
/// `data_change`. An Optimize over data-preserving actions downgrades to
/// snapshot isolation; a Write/Overwrite would re-inherit the OCC ladder.
/// Conversely a row-dropping dedup MUST stay a Write — an Optimize that silently
/// changed the logical data would let a concurrent transaction keep a read set
/// the commit invalidated.
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
/// Decides whether another hygiene sort may start, against LIVE memory.
///
/// The policy is [`crate::config::hygiene_admits`], a pure function; this holds
/// only the state that policy cannot: the hysteresis bit, a cached RSS reading,
/// and the refusal backoff.
///
/// Why a cache: `process_memory_bytes` reads two files under `/sys/fs/cgroup`,
/// and the coordinator fleet asks this question ~100 times a second. A 500 ms
/// sample is far fresher than memory moves under a multi-second sort, and costs
/// two reads a second instead of two hundred.
#[derive(Debug, Default)]
pub(crate) struct HygieneGate {
    state: std::sync::Mutex<HygieneGateState>,
}

#[derive(Debug)]
struct HygieneGateState {
    /// Hysteresis for the MEMORY verdict only — never the backstop. Starts open;
    /// a cold process with an empty lane has nothing to back off from.
    open: bool,
    sampled: Option<Reading>,
    /// Decaying high-water mark. Lending is decided against this rather than the
    /// live reading — see `MemorySnapshot::peak_rss_bytes`.
    peak: Reading,
}

/// An RSS reading and when it was taken.
#[derive(Debug, Clone, Copy)]
struct Reading {
    at: std::time::Instant,
    bytes: usize,
}

impl Reading {
    fn age(&self, now: std::time::Instant) -> std::time::Duration {
        now.duration_since(self.at)
    }
}

impl Default for HygieneGateState {
    fn default() -> Self {
        // Opens with no history: a cold process has nothing to back off from,
        // and the first sample sets the watermark.
        Self { open: true, sampled: None, peak: Reading { at: std::time::Instant::now(), bytes: 0 } }
    }
}

impl HygieneGate {
    const SAMPLE_TTL: std::time::Duration = std::time::Duration::from_millis(500);
    /// How long a high-water mark suppresses lending. Longer than one
    /// oscillation of the prod cycle (minutes), so the trough cannot look like
    /// spare capacity before the next peak arrives.
    const PEAK_DECAY: std::time::Duration = std::time::Duration::from_secs(600);

    /// One cached reading, plus the decaying high-water mark lending uses.
    ///
    /// Admission sizes its decoded-bytes ceiling from the same sample the gate
    /// decides on, so the two cannot disagree about the state of the box.
    ///
    /// The peak decays by expiry rather than by a moving average: the question
    /// is "has this box been near its limit RECENTLY", and an average of a
    /// 45%-to-99% cycle answers neither end of it.
    pub(crate) fn snapshot(
        &self, cfg: &crate::config::DerivedBudget, pool_reserved: usize, pool_size: usize, buffer_pressure: u32,
    ) -> crate::config::MemorySnapshot {
        let mut state = crate::support::lock(&self.state);
        let now = std::time::Instant::now();
        let rss = match state.sampled {
            Some(prev) if prev.age(now) < Self::SAMPLE_TTL => prev.bytes,
            _ => {
                let bytes = process_memory_bytes().unwrap_or(0);
                state.sampled = Some(Reading { at: now, bytes });
                bytes
            }
        };
        if rss >= state.peak.bytes || state.peak.age(now) > Self::PEAK_DECAY {
            state.peak = Reading { at: now, bytes: rss };
        }
        crate::config::MemorySnapshot {
            rss_bytes: rss,
            peak_rss_bytes: state.peak.bytes,
            limit_bytes: cfg.memory_limit_bytes,
            pool_reserved_bytes: pool_reserved,
            pool_size_bytes: pool_size,
            buffer_pressure_pct: buffer_pressure,
        }
    }

    /// Whether another hygiene sort may start. A refusal is not a failure — the
    /// unit was never claimed and stays there for whoever can run it.
    ///
    /// Deliberately NOT rate-limited beyond the sample cache. An earlier cut
    /// silenced the lane for a second after each refusal, which also silenced it
    /// across the moment capacity freed: a test that frees every permit and
    /// re-asks immediately got refused. The cache already bounds the syscalls,
    /// which is all the backoff was really buying.
    ///
    /// Takes the sample rather than the four readings it is built from: the
    /// caller already holds one (admission sizes its ceiling from it), and two
    /// views of the same box that disagree are worse than one that is stale.
    /// Sampling therefore happens under a SEPARATE acquisition of this lock, so
    /// the latch below is not updated atomically with the reading — harmless,
    /// because the latch is recomputed from whatever sample arrives and the
    /// sample itself is a 500 ms cache every caller shares.
    fn admits(&self, sample: crate::config::MemorySnapshot, in_flight: usize, ceiling: usize, floor: usize) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let mut state = crate::support::lock(&self.state);
        // The memory latch is updated from memory alone; the floor and backstop
        // are then composed on top for this caller's answer.
        let was_open = state.open;
        state.open = crate::config::hygiene_memory_open(sample, was_open);
        let stats = crate::observability::maintenance_stats();
        stats.hygiene_gate_open.store(u64::from(state.open), Relaxed);
        // Once per EPISODE — the open->shut edge — not once per asking worker.
        // Counting attempts made this read 99/sec on a lane that was merely full,
        // a number that says nothing about how long it stayed that way.
        if was_open && !state.open {
            stats.compaction_permits_unavailable.fetch_add(1, Relaxed);
        }
        crate::config::hygiene_admits(sample, in_flight, ceiling, floor, was_open)
    }
}

/// Suspends maintenance claiming until dropped — see
/// [`Database::quiesce_maintenance`].
#[must_use = "the lane resumes the moment this is dropped"]
pub(crate) struct MaintenanceQuiesce(Arc<std::sync::atomic::AtomicBool>);

impl MaintenanceQuiesce {
    /// Keep the lane suspended for the rest of the process's life.
    ///
    /// Only correct once the handoff has SUCCEEDED: the replacement owns the
    /// writes from that point and this process is waiting to be replaced, so
    /// resuming compaction here would dirty partitions the successor has already
    /// taken responsibility for.
    pub(crate) fn hold_until_exit(self) {
        std::mem::forget(self);
    }
}

impl Drop for MaintenanceQuiesce {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

impl Database {
    /// Stop claiming new maintenance units until the returned guard is dropped.
    ///
    /// A deploy handoff fences write admission and then waits up to four minutes
    /// for in-flight writers to drain. Under a saturated maintenance lane that
    /// wait does not finish: prod 2026-09-21 failed three consecutive rollouts
    /// with 59 units running and memory at 96%, because ordinary ingest writes
    /// take minutes under that pressure. Maintenance does not hold write
    /// admission itself — the coupling is through memory and IO — so the fix is
    /// to stop ADDING load for the duration of the drain rather than to keep
    /// shaving the lane's steady-state concurrency, which trades throughput
    /// against deployability forever.
    ///
    /// In-flight units are deliberately left alone. Cancelling them would requeue
    /// work and dirty partitions on every deploy; this only stops the lane
    /// growing while the fence drains.
    ///
    /// RAII because the failure path matters most: a handoff that times out must
    /// resume maintenance, exactly as it reopens write admission.
    pub(crate) fn quiesce_maintenance(&self) -> MaintenanceQuiesce {
        self.maintenance_quiesced.store(true, std::sync::atomic::Ordering::Release);
        MaintenanceQuiesce(Arc::clone(&self.maintenance_quiesced))
    }

    /// The memory reading admission sizes its decoded-bytes ceiling from.
    ///
    /// Shares the hygiene gate's cached sample deliberately: two views of the
    /// same box that disagree are worse than one that is 500 ms stale.
    pub(crate) fn admission_memory(&self) -> crate::config::MemorySnapshot {
        self.hygiene_gate.snapshot(
            &self.config.derived,
            self.coordinator_runtime_env().memory_pool.reserved(),
            self.config.derived.coordinator_share_bytes(),
            self.buffer_pressure_pct(),
        )
    }

    /// MemBuffer fill, or 0 when no buffered layer is wired. Zero is the
    /// permissive direction, which is correct: an absent ingest path cannot be
    /// under pressure.
    pub(crate) fn buffer_pressure_pct(&self) -> u32 {
        self.buffered_layer().map_or(0, |layer| layer.pressure_pct())
    }
}

pub(crate) fn process_memory_bytes() -> Option<usize> {
    if let Ok(raw) = std::fs::read_to_string("/sys/fs/cgroup/memory.current")
        && let Ok(v) = raw.trim().parse::<usize>()
    {
        // Discount ALL clean page cache, active included: the kernel reclaims
        // clean file pages before OOM-killing anything, so only anon-dominated
        // usage predicts a kill.
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

/// Host free memory: `MemAvailable` from /proc/meminfo, which inside a container
/// is the HOST's — the figure the kernel's global OOM killer races against.
/// `None` on parse failure (the host brake then never engages).
fn host_mem_available_bytes() -> Option<u64> {
    let raw = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = raw.lines().find(|l| l.starts_with("MemAvailable:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

/// How many times one repair tick may re-select after clearing false suspects.
/// Must exceed a project's run of false suspects or the tick cannot reach real
/// work.
const REPAIR_RESELECT_ROUNDS: usize = 64;

/// Wave cap for one tail pass. A wave serves each project at most one bin, so this is also
/// the per-project file ceiling. The real bounds are the deadline and memory brake enforced
/// inside `round_robin_bins`; this constant only stops a runaway loop.
const fn max_waves(pass: TailPass) -> usize {
    match pass {
        TailPass::Pack => 12,
        TailPass::Repair => 512,
    }
}

const REPAIR_VERIFY_CONCURRENCY: usize = 16;

/// Files one seeding sweep will probe — a ceiling that keeps the sweep bounded
/// for a large tenant, not an exhaustive count.
const REPAIR_VERIFY_SEED_LIMIT: usize = 5_000;

/// How many verified-sorted paths survive a restart. One path is ~150 bytes, so
/// 200k is ~30 MB on disk and bounds boot-time load. Newest wins, because the
/// walk is newest-first.
const REPAIR_VERIFIED_PERSIST_CAP: usize = 200_000;

/// Compressed input admitted to one coordinator L0 sort. zstd expansion is about
/// 17x, so 16 MiB leaves room in the 512 MiB decoded pool. Sorted runs are merged
/// separately toward the 256/512 MiB physical targets.
const COORDINATOR_L0_SORT_TARGET_BYTES: i64 = 16 * 1024 * 1024;

const SORTED_RUN_TAG: &str = "delta-rs.optimize.sort_by";

/// Coverage identity tags to carry from a rewrite's inputs onto its outputs.
///
/// Tags are only carried when every input agrees on every one of them: a union of
/// disagreeing slices would claim coverage over gaps, and `rollup_slice_complete`
/// requires an exact slice match. Dropping them silently erases rollup tier
/// coverage, which `recover_rollup_coverage` reads from exactly these tags.
fn carried_coverage_tags(targets: &[deltalake::kernel::Add]) -> HashMap<String, String> {
    use crate::maintenance_coordinator::{
        TAG_CONTENT_FINGERPRINT, TAG_GENERATION, TAG_MEASURES, TAG_OUTPUT_ROWS, TAG_PROJECT, TAG_SLICE_END, TAG_SLICE_START, TAG_SOURCE,
        TAG_SOURCE_FINGERPRINT, TAG_SOURCE_ROWS, TAG_SOURCE_ROWS_BELOW,
    };
    const COVERAGE_TAGS: [&str; 6] = [TAG_SOURCE, TAG_PROJECT, TAG_SLICE_START, TAG_SLICE_END, TAG_SOURCE_FINGERPRINT, TAG_GENERATION];
    const PROOF_TAGS: [&str; 5] = [TAG_SOURCE_ROWS, TAG_SOURCE_ROWS_BELOW, TAG_CONTENT_FINGERPRINT, TAG_MEASURES, TAG_OUTPUT_ROWS];
    let Some(first) = targets.first() else { return HashMap::new() };
    let value = |add: &deltalake::kernel::Add, tag: &str| add.tags.as_ref().and_then(|tags| tags.get(tag).cloned().flatten());
    let agreed = |tag| {
        let expected = value(first, tag)?;
        targets.iter().all(|add| value(add, tag).as_deref() == Some(expected.as_str())).then_some((tag.to_owned(), expected))
    };
    let Some(mut tags) = COVERAGE_TAGS.into_iter().map(agreed).collect::<Option<HashMap<_, _>>>() else { return HashMap::new() };
    tags.extend(PROOF_TAGS.into_iter().filter_map(agreed));
    tags
}

fn is_sorted_run(tags: &HashMap<String, Option<String>>) -> bool {
    tags.get(SORTED_RUN_TAG).is_some_and(|v| v.as_deref() == Some("true"))
}

/// Files whose newest event is within `SEAL_LAG` of now may still receive appends or DV-merge
/// rewrites, so compacting them races concurrent commits. Only sealed time slices are compacted.
/// A shorter lag churns the cache too fast for 1h-window queries to warm the bodies.
fn seal_micros_now() -> i64 {
    const SEAL_LAG_MICROS: i64 = 15 * 60 * 1_000_000;
    crate::support::now_micros() - SEAL_LAG_MICROS
}

/// The metadata one planner walk collects per candidate file, decoupled from the
/// snapshot API so each file's stats are parsed exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailAdd {
    pub path: String,
    pub size: i64,
    pub is_sorted_run: bool,
    /// (min, max) event time from Add stats; None when stats are absent —
    /// one field so a half-present range is unrepresentable.
    pub event_range: Option<(i64, i64)>,
    /// `numRecords` from the same Add stats; rows are what a staging rewrite
    /// costs. `None` (absent stats) is treated as "unknown, do not count".
    pub rows: Option<u64>,
    /// The file carries a deletion vector. A DV-bearing file is NEVER converged
    /// even at target size: reading it disables per-file parquet predicate
    /// pushdown, so it must be rewritten DV-free to restore the read fast path.
    pub has_dv: bool,
}

impl TailAdd {
    /// Parse the raw Add stats JSON. The snapshot's parsed-stats column is not
    /// materialized on this path, so the kernel `stat_min_i64`/`stat_max_i64`
    /// accessors return `None` for every file and cannot be used here.
    fn from_stats(path: String, size: i64, is_sorted_run: bool, has_dv: bool, stats: Option<&str>) -> Self {
        let stats = stats.and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
        let event_range = stats.as_ref().and_then(Database::event_time_range_from_stats);
        let rows = stats.as_ref().and_then(|v| v.get("numRecords").and_then(serde_json::Value::as_u64));
        Self { path, size, is_sorted_run, event_range, rows, has_dv }
    }
}

/// DECODED working set one bin-of-unsorted-files may sort.
///
/// Compaction runs in two levels: first turn unsorted L0 files into small sorted
/// runs, then merge only sorted runs toward the physical target. Mixing the two
/// forces a full sort of the entire 256/512 MiB group, which exhausts the pool.
///
/// Sized from the same invariant as [`REPAIR_SLICE_DECODED_TARGET_BYTES`]:
/// `budget x concurrent sorts <= ~half the pool`. Kept at or below the repair
/// slice budget so a bin can never out-reserve the heavier lane.
const UNSORTED_BIN_DECODED_BUDGET_BYTES: i64 = 768 * 1024 * 1024;

/// The unsorted-bin budget in COMPRESSED bytes, which is what `Add.size` is.
fn unsorted_bin_budget_bytes() -> i64 {
    UNSORTED_BIN_DECODED_BUDGET_BYTES / crate::database::maintain::DECODED_BYTES_PER_COMPRESSED
}

/// Pack a cell's candidates into ONE bin, smallest first.
///
/// How a bin orders its candidates before packing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BinOrder {
    /// Smallest first. Levels an L0 tail: the loop takes its first candidate
    /// unconditionally, so in event-time order a single large file can fill the
    /// budget alone and the unit selects ONE file — a 1:1 rewrite retiring none.
    SmallestFirst,
    /// Earliest event time first, packing a contiguous slice, so output runs are
    /// time-disjoint and range pruning keeps working.
    EventTime,
}

/// What ONE bin may take. Shared by both compaction paths so their budgets
/// cannot drift apart — see [`select_bin`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct BinPolicy {
    pub target_size: i64,
    pub max_rows: u64,
    pub order: BinOrder,
    /// Exclude sorted runs while any unsorted file is present, so L0 arrivals are
    /// sorted into runs before runs are merged with each other.
    pub level_unsorted_first: bool,
}

/// THE bin packer. One implementation, both callers.
///
/// INVARIANT, and it is the whole contract: given two or more PACKABLE
/// candidates this returns two or more paths. Every rule here is a *stopping*
/// rule — it bounds how much one bin takes — and none may veto a bin outright,
/// because a veto has no next attempt to fall through to: the cell is simply
/// re-claimed forever.
///
/// Prod 2026-09-15 is why the invariant is written down. A value floor vetoed
/// bins under five files while the packing cap pinned the byte budget to the two
/// smallest files, so a bin could never hold more than two. The two rules were
/// mutually unsatisfiable, sealed consolidation committed nothing for three
/// days, and single cells were re-claimed 1,663 times while CPU sat at 99% of
/// its limit.
///
/// It is ONE function because it used to be two. The coordinator packed
/// smallest-first under a cap that collapsed onto a pair; the off-box CLI packed
/// by event time to its full target. Same intent, different budgets, and the
/// difference was worth a full rewrite pass per doubling — plus a value floor
/// that wedged one lane and not the other because they disagreed on `min_files`.
/// Ordering still differs by design; the BUDGETS may not.
pub(crate) fn select_bin(candidates: &[TailAdd], policy: BinPolicy) -> Vec<String> {
    let has_unsorted = policy.level_unsorted_first && candidates.iter().any(|add| !add.is_sorted_run);
    // The pair floor applies to the limit the loop actually uses, not only to
    // the target: whichever budget wins, a bin that cannot hold two files
    // retires nothing and its cell re-enqueues forever.
    let pair_floor = candidates.iter().map(|add| add.size).k_smallest(2).sum::<i64>();
    let limit = if has_unsorted { unsorted_bin_budget_bytes() } else { policy.target_size }.max(pair_floor);

    let mut ordered: Vec<&TailAdd> = candidates
        .iter()
        // A file at or above target is converged and never packing's work,
        // whatever its tags say — sortedness is Repair's job. EXCEPTION: a
        // DV-bearing file is not converged at any size, it must be rewritten
        // DV-free to restore parquet pushdown.
        .filter(|add| add.size < policy.target_size || add.has_dv)
        .filter(|add| !(has_unsorted && add.is_sorted_run))
        .collect();
    match policy.order {
        BinOrder::SmallestFirst => ordered.sort_by_key(|add| add.size),
        BinOrder::EventTime => ordered.sort_by_key(|add| add.event_range.map_or(i64::MIN, |range| range.0)),
    }

    let (mut bytes, mut rows) = (0i64, 0u64);
    let mut selected: Vec<&TailAdd> = Vec::new();
    for add in ordered {
        // ROWS, not just bytes: a rewrite costs what it must sort and write.
        // Unknown row counts (absent stats) do not accumulate, so this can never
        // be stricter than the byte budget alone.
        let next_rows = rows.saturating_add(add.rows.unwrap_or(0));
        // Neither budget may reduce a bin below a PAIR — a one-file bin retires
        // nothing and its cell is re-claimed forever. `limit` carries
        // `pair_floor` for bytes; the second file is UNCONDITIONALLY exempt from
        // the row cap for the same reason. It was once exempt only up to
        // `2 * max_rows`, which is still a veto: two 5M-row files could never
        // pair, so their cell spun forever.
        let pair_exemption = selected.len() == 1;
        let over_bytes = bytes.saturating_add(add.size) > limit;
        let over_rows = next_rows > policy.max_rows && !pair_exemption;
        if !selected.is_empty() && (over_bytes || over_rows) {
            if selected.len() >= 2 {
                break;
            }
            // RESTART, don't stop: a lone file is already a run and rewriting it
            // 1:1 retires nothing, so the pass moves on rather than wedging
            // behind it. Reachable in event-time order, where one early large
            // file would otherwise block every later one.
            (bytes, rows, selected) = (0, 0, Vec::new());
        }
        bytes = bytes.saturating_add(add.size);
        rows = rows.saturating_add(add.rows.unwrap_or(0));
        selected.push(add);
    }
    // A lone UNSORTED file is real work: sorting it into a run is the L0 pass.
    // Everywhere else a one-file bin is a 1:1 rewrite that retires nothing.
    if selected.len() < 2 && !has_unsorted {
        return Vec::new();
    }
    selected.into_iter().map(|add| add.path.clone()).collect()
}

/// Per-slice budget, in **DECODED** bytes — the unit the sort actually allocates.
///
/// Returns a decoded budget; feed it to [`repair_slice_want`], never divide
/// compressed `bytes_in` by it.
fn coordinator_slice_target(pass: TailPass, input_files: usize, bytes_in: i64) -> Option<i64> {
    match pass {
        // Slicing a repair is NOT a cheaper piece of the rewrite — it is a whole
        // extra pass over the same file: the predicate cannot prune (the file is
        // a repair candidate precisely for being unsorted) and the file is sealed,
        // so each pass re-downloads it. Off by default; the knob re-enables
        // slicing at a given size.
        TailPass::Repair => crate::config::try_config()
            .map_or(0, |cfg| cfg.maintenance.timefusion_repair_slice_decoded_target_bytes)
            .try_into()
            .ok()
            .filter(|target: &i64| *target > 0),
        // A LONE oversized L0 file is cut far finer than the sort budget: it is
        // unsorted, so the whole file must pass through one sort, and the 16 MB
        // compressed target keeps that pass small.
        TailPass::Pack if input_files == 1 && bytes_in > COORDINATOR_L0_SORT_TARGET_BYTES => {
            Some(crate::database::maintain::estimated_decoded_bytes(COORDINATOR_L0_SORT_TARGET_BYTES) as i64)
        }
        // MULTI-FILE bins slice to the sort budget. This was `None` until
        // 2026-09-19, which is why a bin that could not fit its sort had to be
        // SHRUNK instead — the packing cap collapsed onto the two smallest files
        // and a day converged one doubling per pass. Slicing bounds the sort
        // directly, so the packer can fill its target in one pass.
        //
        // Cheap here in a way it is not for Repair: these inputs are sorted runs
        // with real event ranges, so each slice's time predicate prunes row
        // groups instead of re-reading the whole bin. `repair_slice_want`
        // returns 1 for a bin that already fits, and the caller only slices when
        // it wants more than one, so small bins are untouched.
        // 3/5 of the budget, not all of it: `DECODED_BYTES_PER_COMPRESSED` is an
        // optimistic fixed ratio, so a slice priced at exactly one sort budget
        // can still overrun it. The same margin the old bin-level cap carried,
        // moved to where it belongs.
        TailPass::Pack => Some(crate::config::coordinator_per_sort_decoded_bytes() * 3 / 5),
    }
}

/// How many event-time slices a bin must be cut into so no single sort exceeds
/// `decoded_slice_target` of decoded Arrow.
///
/// The `+ 1` matches the caller's gate: `want > 1` is what enables slicing at
/// all, so a bin that fits whole returns 1 and is sorted in one pass.
fn repair_slice_want(bytes_in: i64, decoded_slice_target: i64) -> usize {
    let decoded = crate::database::maintain::estimated_decoded_bytes(bytes_in);
    (decoded / (decoded_slice_target.max(1) as u64)) as usize + 1
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
        // A repair pass owns sealed dates only; admitting today's files would let
        // packing work back into a budget reserved for repair.
        if policy.pass == TailPass::Repair {
            return false;
        }
        let converged_done = size >= policy.converged() && (sorted_run || !repairable);
        let over_cap_run = size >= policy.sorted_run_cap && sorted_run;
        return !(converged_done || over_cap_run);
    }
    // Sealed dates: every un-verified file is a suspect and the FOOTER decides —
    // the sorted-run tag records the optimizer's INTENT and can disagree with the
    // footer queries actually read, so admission must not key off it.
    // `repair_max_bytes` keeps a tick bounded; quarantined candidates are skipped
    // so the queue behind them drains.
    repairable
        && size <= policy.repair_max_bytes
        && !policy.verified_sorted.contains(path)
        && policy.failures.get(path).map_or(0, |n| *n.value()) < REPAIR_QUARANTINE_AFTER
        && repair_markers.iter().any(|m| path.contains(m.as_str()))
}

/// Pool bytes one escalated flush sort needs to finish without being refused.
/// A refused sort writes its group unsorted, and one unsorted file disables the
/// reader's all-or-nothing footer ordering for its entire partition.
const MIN_SPILL_SORT_BYTES: usize = 1 << 30;

/// How many escalated flush sorts may run at once on a pool of `pool_bytes`.
/// At the 1 GB default this is ONE: escalated sorts serialize. Buy concurrency
/// back by raising `TIMEFUSION_FLUSH_SORT_POOL_MB`.
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
const INPUT_BYTES_PER_SEC: i64 = 460_000;

fn budget_bytes(budget: std::time::Duration) -> i64 {
    (budget.as_secs().min(i64::MAX as u64) as i64).saturating_mul(INPUT_BYTES_PER_SEC)
}

/// Fraction of the tick a packing bin may consume, leaving the rest as margin for
/// the commit and for rewrite rates slower than the global estimate.
const PACK_BUDGET_FRACTION: u32 = 2;

/// Largest bin a packing pass will assemble, derived from the time it has.
///
/// A bin that cannot be rewritten inside the tick is discarded at the deadline and the next
/// tick re-selects the same files, so an oversized target produces nothing while burning the
/// whole budget. The whole policy shrinks with this value, so a freshly packed run still lands
/// above `sorted_run_cap` and `converged`; otherwise smaller outputs are re-selected forever.
pub(crate) fn pack_target_bytes(configured: i64, budget: std::time::Duration) -> i64 {
    configured.min(budget_bytes(budget / PACK_BUDGET_FRACTION)).max(1)
}

/// Consecutive staging failures after which a repair candidate stops being offered for the rest
/// of the process. A repair bin is one whole-file sort, so a working set that does not fit fails
/// deterministically and would otherwise be re-selected every pass. Three, not one, because
/// staging also fails transiently (OCC race, restart mid-stage). A success clears the count.
const REPAIR_QUARANTINE_AFTER: u32 = 3;

/// Sort parallelism ladder a repair bin is retried at before its pool exhaustion is believed.
///
/// Each partition has an unspillable merge operator, so 16 partitions means 16 unspillable
/// merges competing for the pool while a single-partition sort has no merge and spills within
/// its fair share. Exhaustion at 16 therefore says nothing about 1; only the floor is believed.
const REPAIR_SORT_PARTITION_LADDER: [usize; 3] = [REPAIR_SORT_PARTITIONS, 4, 1];

/// Sort parallelism ladder a dedup rewrite is retried at, same doctrine as
/// [`REPAIR_SORT_PARTITION_LADDER`] but starting from the cap dedup already runs
/// under. Sharding bounds decoded bytes but not sort parallelism, and the merge
/// operator is unspillable and per-partition.
const DEDUP_SORT_PARTITION_LADDER: [usize; 2] = [MAINTENANCE_MAX_PARTITIONS, 1];

/// Sort parallelism for a dedup rewrite, by the claiming task's `attempts`.
///
/// `attempts` is POST-CLAIM — `claim_next` increments before returning the task, so a
/// first-ever run arrives with `attempts == 1`, hence the `saturating_sub(1)`. Indexing the
/// ladder directly would put every first-ever dedup on the single-partition floor.
///
/// Level comes from the persisted count, not an in-process map: the process is replaced often
/// enough that a map would lose the degradation and keep retrying at the width that just failed.
/// Narrowing on ANY retry (not just an exhaustion) is deliberate — the failure reason is not
/// plumbed here and a narrower sort costs only parallelism.
fn dedup_sort_partitions(attempts: u32) -> usize {
    DEDUP_SORT_PARTITION_LADDER[(attempts.saturating_sub(1) as usize).min(DEDUP_SORT_PARTITION_LADDER.len() - 1)]
}

/// What a failed repair staging costs the candidate: the parallelism to retry
/// at next (`None` = nothing cheaper left) and the strike step to charge.
///
/// A pool exhaustion is only "deterministic" once it happens at the bottom of
/// [`REPAIR_SORT_PARTITION_LADDER`]; above the bottom it buys a retry at lower
/// parallelism and charges a single strike, so a file failing for an unrelated
/// reason still parks after [`REPAIR_QUARANTINE_AFTER`].
fn repair_failure_action(exhausted: bool, level: usize) -> (Option<usize>, u32) {
    let next = level + 1;
    match (exhausted, REPAIR_SORT_PARTITION_LADDER.get(next)) {
        // Room left on the ladder: retry cheaper, charge one strike.
        (true, Some(&partitions)) => (Some(partitions), 1),
        // Bottom of the ladder: the exhaustion is believed.
        (true, None) => (None, REPAIR_QUARANTINE_AFTER),
        (false, _) => (None, 1),
    }
}

/// The knobs that travel together through planning, admission and binning.
pub(crate) struct HotBinPolicy<'a> {
    /// SEALED dates (yesterday backwards) scanned for footer repair only.
    /// Empty restores the old today-only pass.
    repair_dates: &'a [String],
    target_size: i64,
    min_files: usize,
    sorted_run_cap: i64,
    /// Largest file a hot tick will rewrite to repair its footer. Bigger ones
    /// belong to the off-box `timefusion optimize` CLI, not a 5-minute tick.
    repair_max_bytes: i64,
    /// Which of the two disjoint jobs this pass is running. See [`TailPass`].
    pass: TailPass,
    /// Suspects a footer read already cleared — see `repair_verified_sorted`.
    verified_sorted: &'a dashmap::DashSet<String>,
    /// Consecutive staging failures per candidate — see `REPAIR_QUARANTINE_AFTER`.
    failures: &'a dashmap::DashMap<String, u32>,
}

/// The two disjoint jobs the hot tail runs. They must stay separate: packing is
/// continuous (today's small files, small units) while footer repair is a finite
/// backlog of large sealed files, each a whole-file global sort, so one shared
/// budget starves repair and it never finishes a bin.
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

/// Pack the earliest sealed slice of `adds` into one time-disjoint bin.
///
/// Carries the same INVARIANT as [`select_coordinator_compaction_candidates`]:
/// no rule may veto a bin outright, only bound how much one takes. The value
/// floor and the size-ratio guard that used to sit here were removable knobs
/// whose default combination wedged the coordinator lane for three days
/// (2026-09-15); a packer that can decline all work has no safe default.
pub(crate) fn select_tail_bin(adds: &[TailAdd], target_size: i64, min_files: usize, sorted_run_cap: i64, seal_micros: i64, pass: TailPass) -> Vec<String> {
    let cap = target_size.max(1);
    let converged = cap - cap / 8;
    // An oversized file needs a SOLO rewrite (one per bin) when it is either an
    // unsorted run or carries a deletion vector: an unsorted file disables the
    // reader's all-or-nothing footer ordering for the whole scan, and a DV
    // disables per-file parquet pushdown until it is rewritten DV-free. A SMALL
    // DV'd file packs normally (it is already under `converged`).
    let is_repair = |add: &TailAdd| add.size >= converged && (!add.is_sorted_run || add.has_dv);
    let mut fresh: Vec<(&str, i64, i64, bool)> = adds
        .iter()
        .filter(|add| add.size < sorted_run_cap || !add.is_sorted_run || add.has_dv)
        .filter(|add| add.size < converged || is_repair(add))
        .filter_map(|add| match add.event_range {
            Some((min, max)) if max <= seal_micros => Some((add.path.as_str(), min, add.size, is_repair(add))),
            _ => None,
        })
        .collect();
    // A lone repair file is real work and needs no `min_files` company. On a
    // Repair pass EVERY candidate is repair work whatever its size: a small
    // sealed footer-less file poisons its date's scan just as thoroughly.
    let is_candidate = |repair: bool| pass == TailPass::Repair || repair;
    let repairs_present = fresh.iter().any(|(_, _, _, repair)| is_candidate(*repair));
    if fresh.len() < min_files && !repairs_present {
        return vec![];
    }
    fresh.sort_unstable_by_key(|(_, min, _, _)| *min);
    // A repair pass ranks NEWEST first (the most recent footer-less date is the
    // wall users hit) and SMALLEST among equals (so one un-finishable file cannot
    // head-of-line block its date). The pass's own `budget` bounds wall clock, so
    // taking more files per pass cannot overrun the tick.
    if pass == TailPass::Repair {
        let take = crate::config::try_config().map_or(1, |c| c.maintenance.timefusion_footer_repair_files_per_pass).max(1);
        let ranked = fresh.iter().filter(|(_, _, _, repair)| is_candidate(*repair));
        return ranked.sorted_unstable_by(|a, b| b.1.cmp(&a.1).then_with(|| a.2.cmp(&b.2))).take(take).map(|(path, ..)| path.to_string()).collect();
    }
    // Pack the earliest contiguous slice up to `cap` → one time-disjoint run per
    // tick; later ticks pack the next (strictly later) slice. THE SAME packer the
    // coordinator uses — the budgets live in exactly one place now.
    // From `fresh`, NOT from `adds`: fresh already applied the seal gate, the
    // converged threshold and `sorted_run_cap`. Rebuilding from `adds` would
    // silently readmit every file those filters excluded.
    let packable: std::collections::HashSet<&str> = fresh.iter().filter(|(_, _, _, r)| !*r).map(|(path, ..)| *path).collect();
    let nonrepair: Vec<TailAdd> = adds.iter().filter(|add| packable.contains(add.path.as_str())).cloned().collect();
    let files = select_bin(
        &nonrepair,
        BinPolicy {
            target_size: cap,
            // NO row cap here: the tail's bins are bounded by bytes, and the sort
            // they feed is sliced (`coordinator_slice_target`), so rows cannot
            // blow the sort budget.
            max_rows: u64::MAX,
            order: BinOrder::EventTime,
            // Sortedness is handled by `sorted_run_cap` above, not by levelling.
            level_unsorted_first: false,
        },
    );
    // Gap rule, for TODAY only: once a project has no packable slice left, spend
    // the tick rewriting one oversized unsorted file instead. Today's partition
    // converges so the gap reliably appears; on sealed dates it never does, which
    // is why they belong to the `TailPass::Repair` cron instead.
    if files.is_empty()
        && let Some((path, _, _, _)) = fresh.iter().find(|(_, _, _, repair)| *repair)
    {
        return vec![path.to_string()];
    }
    files
}

#[cfg(test)]
mod repair_batch_tests {
    /// A repair bin is one whole file, so its sort spills many runs and the
    /// merge allocates per run per batch; packing's wider batch would exhaust
    /// the pool.
    #[test]
    fn repair_sorts_with_smaller_batches_than_packing() {
        use datafusion::execution::runtime_env::RuntimeEnv;
        let batch = |state: &datafusion::execution::session_state::SessionState| state.config().options().execution.batch_size.get();
        let pack = super::build_optimize_session_state(0, std::sync::Arc::new(RuntimeEnv::default()));
        let repair = super::build_optimize_session_state_tuned(
            0,
            std::sync::Arc::new(RuntimeEnv::default()),
            Some("256"),
            Some(super::UncappedSort { partitions: super::REPAIR_SORT_PARTITIONS, reservation_bytes: Some(super::REPAIR_SORT_RESERVATION_BYTES) }),
        );
        assert_eq!(batch(&repair), 256, "repair shrinks the sort's indivisible admission unit");
        // Repair runs ONE bin at a time, so `MAINTENANCE_MAX_PARTITIONS`
        // (sized for many concurrent sorters) does not apply to it.
        assert_eq!(repair.config().options().execution.target_partitions, super::REPAIR_SORT_PARTITIONS, "repair sorts wide");
        assert!(
            repair.config().options().execution.target_partitions > pack.config().options().execution.target_partitions,
            "packing keeps the cap because it runs many bins at once"
        );
        assert!(batch(&repair) < batch(&pack), "packing keeps the wider batch: {} vs {}", batch(&pack), batch(&repair));
    }
}

/// Logical file identities in a snapshot. A DV update preserves the parquet path;
/// a path alone therefore proves neither target freshness nor commit landing.
/// Keep all entries so an already-ambiguous path cannot authorize another rewrite.
struct ActiveFiles(HashMap<String, Vec<Option<deltalake::kernel::DeletionVectorDescriptor>>>);

impl ActiveFiles {
    fn from_snapshot(snapshot: &deltalake::table::state::DeltaTableState) -> Self {
        Self(snapshot.log_data().iter().map(|file| (file.path().into_owned(), file.deletion_vector_descriptor())).into_group_map())
    }

    fn matches(&self, path: &str, dv: &Option<deltalake::kernel::DeletionVectorDescriptor>) -> bool {
        self.0.get(path).is_some_and(|entries| matches!(entries.as_slice(), [entry] if entry == dv))
    }

    fn adds_live(&self, actions: &[deltalake::kernel::Action]) -> bool {
        let mut adds = actions.iter().filter_map(|a| if let deltalake::kernel::Action::Add(add) = a { Some(add) } else { None }).peekable();
        adds.peek().is_some() && adds.all(|add| self.matches(&add.path, &add.deletion_vector))
    }

    /// Physical objects referenced by any active entry, including every DV when
    /// a damaged snapshot contains several logical entries for the same parquet.
    fn referenced_paths(&self) -> HashSet<String> {
        self.0
            .keys()
            .cloned()
            .chain(self.0.values().flatten().filter_map(|dv| dv.as_ref().and_then(deltalake::operations::deletion_vectors::dv_object_store_relative_path)))
            .collect()
    }
}

/// Reject only the bin whose planned input has changed. DV dedup can read a
/// survivor without removing its file, so verify every input, not only Removes.
fn split_live_bins(bins: Vec<StagedBin>, live: &ActiveFiles) -> (Vec<StagedBin>, Vec<StagedBin>) {
    bins.into_iter().partition(|bin| bin.targets.iter().all(|add| live.matches(&add.path, &add.deletion_vector)))
}

/// Exact staged Adds distinguish a prior successful commit from another writer
/// changing our targets. An empty set cannot prove that a commit landed.
fn bin_adds_live(bin: &StagedBin, live: &ActiveFiles) -> bool {
    live.adds_live(&bin.adds)
}

/// Delete the staged parquet of bins leaving a wave uncommitted — MINUS anything
/// the snapshot references.
///
/// Filters per ADD, not per bin: a resuming instance commits a wave bin by bin,
/// so a wave can be part-live and deleting a live object is data loss. Not a
/// lock — another instance can still commit between the caller's snapshot read
/// and these deletes.
async fn discard_bin_parquet(bins: &[StagedBin], live: &HashSet<String>) {
    use object_store::ObjectStoreExt; // dyn-safe `delete`
    for bin in bins {
        let orphans: Vec<deltalake::kernel::Action> =
            bin.adds.iter().filter(|a| !matches!(a, deltalake::kernel::Action::Add(add) if live.contains(add.path.as_str()))).cloned().collect();
        Database::cleanup_orphaned_parquet(&bin.stage_store, &orphans).await;
        // A partial wave can contain an already-committed DV; preserve its sidecar.
        for rel in bin.discardable_paths.iter().filter(|path| !live.contains(*path)) {
            if let Err(e) = bin.stage_store.delete(&object_store::path::Path::from(rel.as_str())).await {
                warn!("orphaned DV sidecar (manual cleanup needed): {rel} — delete failed: {e}");
            }
        }
    }
}

/// Per-project outcome of one round's staging.
pub(crate) enum BinOutcome<T> {
    /// Bin staged; project stays in the round-robin.
    Staged(T),
    /// Tail converged — nothing left this tick; drop from the rotation.
    Converged,
    /// Selection went stale (concurrent rewrite); keep the project pending so
    /// the next round's re-plan serves it a fresh bin.
    Retry,
    /// The repair byte budget is held by another rewrite. Distinct from `Retry`:
    /// a stale selection is re-servable immediately, but the budget holder runs
    /// for tens of minutes, so re-queueing it just spins.
    BudgetBusy,
}

/// Parallelism for one packing sort, bound by the tighter of two independent limits.
///
/// * Memory: `k` concurrent bins may spend at most a quarter of the pack pool on
///   unspillable up-front reservations.
/// * CPU: `k` bins together may use at most half the box, leaving the rest for reads.
///
/// Never below `MAINTENANCE_MAX_PARTITIONS`; this only lifts that cap.
///
/// The CPU term divides the half-box among `k` bins and FLOORS, so aggregate
/// width falls slightly as `k` rises: at 44 cores, k=5 gives 4 each (20 total)
/// and k=8 gives 2 each (16). That is deliberate, and rounding up was tried and
/// rejected — `k * parts * 2 <= cores` is a promise to READS, and breaking it to
/// recover a few partitions starves the queries this lane exists to serve.
///
/// It is also not the binding term. Prod staged 431 MB in 236 s at four
/// partitions — 1.8 MB/s, with the box nowhere near CPU-bound — so these sorts
/// wait on S3, not on cores. More concurrent bins beat wider ones here; if that
/// ever inverts, the evidence will be CPU saturation during staging, and the
/// fix is the half-box fraction, not the rounding.
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
/// the hot queries read, so it has to be swept on every tick.
fn rotate_sealed_tail<T>(work: &mut Vec<T>, sealed_from: usize, cursor: usize) {
    let mut sealed = work.split_off(sealed_from.min(work.len()));
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
    projects: Vec<String>, max_rounds: usize, mut concurrency: usize, deadline: std::time::Instant, on_truncate: impl Fn(usize, &[String]),
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
                // A pause is a truncation for fairness: without rotating, a
                // chronically-engaged brake restarts every tick at the
                // debt-ordered head and starves the tail.
                on_truncate(round, &pending);
                break;
            }
            // Service floor: drop to SERIAL, not to one project. One bin in
            // flight bounds the instantaneous heap, while every project the
            // deadline admits still gets served.
            Some(Brake::Degrade(reason)) => {
                concurrency = 1;
                crate::observability::maintenance_stats().light_optimize_ticks_degraded.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                info!(round, served = pending.len(), reason, event = "light_optimize_wave_degraded");
            }
            None => {}
        }
        // Admission control INSIDE the round, not only at its boundary: sampling
        // as each bin is admitted stops NEW work the moment the line is crossed.
        // In-flight bins finish; deferred projects stay `pending`.
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
                    // Per-bin commits must also be per-bin in TIME: collecting
                    // the round first makes a finished bin wait on its slowest
                    // sibling, so a restart mid-round discards complete rewrites.
                    match commit_each_bin {
                        true => failed += commit_wave(vec![bin], round).await,
                        false => staged.push(bin),
                    }
                }
                Ok(BinOutcome::Retry) => pending.push(project_id),
                // Keeping the project in rotation would spin every remaining
                // round against the same held budget; the next tick re-plans it.
                Ok(BinOutcome::BudgetBusy) => {}
                Ok(BinOutcome::Converged) => {}
                Err(error) => {
                    warn!(project_id, round, %error, event = "light_optimize_bin_failed");
                    failed += 1;
                }
            }
        }
        info!(round, staged = staged_seen, pending = pending.len(), failed, event = "round_staged");
        // Packing commits ONCE per wave — its bins are many and quick, and
        // per-bin commits drive OCC retry ladders. Repair committed per bin above.
        if !staged.is_empty() {
            failed += commit_wave(staged, round).await;
        }
    }
    failed
}

/// Sorting by the schema's timestamp-leading keys is what keeps rewritten files'
/// timestamp statistics tight and their footers honest.
fn choose_optimize_type(schema: &crate::schema::TableSchema, allow_zorder: bool, allow_sort: bool) -> (deltalake::operations::optimize::OptimizeType, bool) {
    use deltalake::operations::optimize::OptimizeType;
    if allow_zorder && !schema.z_order_columns.is_empty() {
        return (OptimizeType::ZOrder(schema.z_order_columns.clone()), false);
    }
    let sort_cols = schema_optimize_sort_columns(schema);
    if allow_sort && !sort_cols.is_empty() { (OptimizeType::SortBy(sort_cols), true) } else { (OptimizeType::Compact, false) }
}

/// Opportunistically upgrade SortBy to SortByDedup. Only a space/read-
/// amplification optimization, not a convergence proof: bin packing may strand
/// versions of one key in different output runs, so authoritative physical
/// collapse stays with the dedup engine.
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

/// Rows per row group, derived from the configured byte target and the row
/// width. Parquet's byte cap does not bind, so the row cap is the real lever.
fn row_group_row_count(schema: &crate::schema::TableSchema, target_bytes: usize, measured_bytes_per_row: Option<u64>) -> usize {
    /// Rough encoded-but-uncompressed bytes a value of this type costs; only
    /// needs to put the row width in the right order of magnitude.
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
    // MEASURED width wins over the modelled one: the per-type table can be off
    // by an order of magnitude on wide telemetry rows, and a row group is the
    // indivisible unit of a scan.
    let row_bytes: usize = measured_bytes_per_row
        .and_then(|width| usize::try_from(width).ok())
        .filter(|width| *width > 0)
        .unwrap_or_else(|| schema.fields.iter().map(|f| width(&f.data_type)).sum::<usize>())
        .max(1);
    // The floor keeps footer/dictionary overhead from dominating on NARROW
    // tables, but must not survive when honouring it would blow the byte target.
    const MIN_ROWS: usize = 32_768;
    const MAX_ROWS: usize = 1_048_576;
    let by_bytes = (target_bytes / row_bytes).max(1);
    by_bytes.clamp(MIN_ROWS.min(by_bytes), MAX_ROWS)
}

/// `declare_sorted` may be `true` only for paths that actually sort rows in schema order;
/// optimize/compact/recompress rewrite rows into Z-order or concatenation and must pass `false`.
fn build_writer_properties(
    parquet_cfg: &crate::config::ParquetConfig, schema: &crate::schema::TableSchema, zstd_level: i32, declare_sorted: bool, measured_bytes_per_row: Option<u64>,
) -> WriterProperties {
    use deltalake::datafusion::parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::{metadata::KeyValue, properties::EnabledStatistics},
        schema::types::ColumnPath,
    };

    let max_row_group_size = parquet_cfg.timefusion_max_row_group_size;

    // Per-column bloom NDV sized to a typical row-group row count (~1.7MB bloom
    // per column at fpp=0.01). Do not scale it by the byte-sized row-group cap.
    const BLOOM_NDV: u64 = 1_000_000;

    let sorting_columns_pq = schema.sorting_columns();
    let sort_key_names: HashSet<&str> = schema.sorting_columns.iter().map(|c| c.name.as_str()).collect();

    // Do NOT call `set_bloom_filter_fpp` globally — parquet-rs treats any global
    // bloom setter other than `set_bloom_filter_enabled` as an implicit enable,
    // allocating a bloom buffer on every column. Set fpp per-column only.
    let builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(zstd_level).unwrap_or_else(|_| ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap())))
        .set_max_row_group_bytes(Some(max_row_group_size))
        // Exact companion to the byte cap above, which does not bind — see
        // `row_group_row_count`.
        .set_max_row_group_row_count(Some(row_group_row_count(schema, max_row_group_size, measured_bytes_per_row)))
        .set_dictionary_enabled(true)
        .set_dictionary_page_size_limit(8388608)
        // Chunk by default; page stats are opted into per-column below. Page
        // stats on wide JSON/variant columns bloat the ColumnIndex with a
        // min/max per page, re-decoded on every scan.
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(false)
        .set_data_page_row_count_limit(parquet_cfg.timefusion_page_row_count_limit)
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

            // Page stats only where they prune AND are cheap: sort keys plus
            // timestamp/date columns (8-byte min/max).
            let builder = if is_sort_key || time_like { builder.set_column_statistics_enabled(col.clone(), EnabledStatistics::Page) } else { builder };

            // `time_like` FIRST: Timestamp/Date32 must not fall into the integer arm.
            let builder = match dt {
                _ if time_like => builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED).set_column_dictionary_enabled(col.clone(), false),
                "Int32" | "Int64" | "UInt32" | "UInt64" => builder.set_column_encoding(col.clone(), Encoding::DELTA_BINARY_PACKED),
                "Utf8" if is_sort_key => builder.set_column_encoding(col.clone(), Encoding::DELTA_BYTE_ARRAY).set_column_dictionary_enabled(col.clone(), false),
                _ => builder,
            };

            // Explicit per-column dict opt-out; Some(true)/None leaves defaults intact.
            let builder = if field.dictionary == Some(false) { builder.set_column_dictionary_enabled(col.clone(), false) } else { builder };

            if field.bloom_filter && !parquet_cfg.timefusion_bloom_filter_disabled {
                builder
                    .set_column_bloom_filter_enabled(col.clone(), true)
                    .set_column_bloom_filter_max_ndv(col.clone(), BLOOM_NDV)
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

/// Widen a live table's STORED Delta schema to hold every field of `want` it
/// lacks, by committing a ZERO-ROW batch of only the missing fields under
/// `SchemaMode::Merge`. Metadata-only: it unions the columns without writing
/// data or rewriting rows, and it is a no-op once they exist — so a caller may
/// run it on every pass rather than remembering whether it has.
///
/// Added fields are forced NULLABLE: `Merge` cannot add a NOT NULL column to a
/// table that already has rows, and readers null-fill a column a file lacks.
pub(crate) async fn evolve_table_columns(table_ref: &Arc<RwLock<DeltaTable>>, want: &arrow_schema::Fields) -> Result<Vec<String>> {
    let stored: HashSet<String> = { table_ref.read().await.snapshot()?.schema().fields().map(|f| f.name().to_string()).collect() };
    let missing: Vec<arrow_schema::FieldRef> =
        want.iter().filter(|f| !stored.contains(f.name())).map(|f| Arc::new(f.as_ref().clone().with_nullable(true))).collect();
    if missing.is_empty() {
        return Ok(Vec::new());
    }
    let columns = missing.iter().map(|f| arrow::array::new_empty_array(f.data_type())).collect();
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(missing.clone())), columns)?;
    let table = { table_ref.read().await.clone() };
    table
        .write(vec![batch])
        .with_save_mode(deltalake::protocol::SaveMode::Append)
        .with_schema_mode(deltalake::operations::write::SchemaMode::Merge)
        .await
        .map_err(|e| anyhow::anyhow!("schema-merge commit failed: {e}"))?;

    // Re-read from the log rather than trusting the write.
    let after: HashSet<String> = {
        let mut guard = table_ref.write().await;
        guard.load().await?;
        guard.snapshot()?.schema().fields().map(|f| f.name().to_string()).collect()
    };
    let added: Vec<String> = missing.iter().map(|f| f.name().clone()).collect();
    let still: Vec<&String> = added.iter().filter(|name| !after.contains(*name)).collect();
    anyhow::ensure!(still.is_empty(), "schema merge committed but columns are still absent from the stored schema: {still:?}");
    Ok(added)
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
    /// The YAML schema and the Delta transaction log schema are separate: storage must be widened
    /// first, then YAML may follow. Columns already present are skipped, so this is re-runnable.
    /// [`evolve_table_columns`] is the mechanism; this is the type-string front end for the CLI.
    pub async fn migrate_add_columns(&self, table_name: &str, adds: &[(String, String)], dry_run: bool) -> Result<ColumnMigrationReport> {
        use arrow_schema::{DataType, Field, TimeUnit};

        let table_ref = self.get_or_create_unified_table(table_name).await?;
        let stored: Vec<String> = {
            let t = table_ref.read().await;
            t.snapshot()?.schema().fields().map(|f| f.name().to_string()).collect()
        };
        let missing: Vec<&(String, String)> = adds.iter().filter(|(n, _)| !stored.contains(n)).collect();
        let report = |after: usize, added: Vec<String>| ColumnMigrationReport { stored_before: stored.len(), stored_after: after, added };
        if missing.is_empty() {
            return Ok(report(stored.len(), Vec::new()));
        }

        let fields: Vec<Field> = missing
            .iter()
            .map(|(n, t)| {
                let data_type = match t.as_str() {
                    "timestamp" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                    "boolean" => DataType::Boolean,
                    // The types a rollup measure is made of: counts Int64, sums/min/max
                    // Int64 or Float64, `tdigest`/`hll` states Binary.
                    "bigint" => DataType::Int64,
                    "double" => DataType::Float64,
                    "binary" => DataType::Binary,
                    "text" | "string" => DataType::Utf8,
                    other => anyhow::bail!("unsupported column type '{other}' (expected timestamp|boolean|bigint|double|binary|text)"),
                };
                Ok(Field::new(n, data_type, true))
            })
            .collect::<Result<Vec<_>>>()?;
        // AFTER the type mapping, so a dry run rejects a typo instead of
        // reporting work it would then fail to do.
        if dry_run {
            return Ok(report(stored.len(), missing.iter().map(|(n, _)| n.clone()).collect()));
        }
        let added = evolve_table_columns(&table_ref, &fields.into()).await?;
        Ok(report(stored.len() + added.len(), added))
    }
}

#[cfg(test)]
mod writer_properties_tests {
    use arrow::array::{DictionaryArray, Int64Array, StringArray, StringViewArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use deltalake::datafusion::parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::properties::EnabledStatistics,
        schema::types::ColumnPath,
    };
    use test_case::test_case;

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

    /// A batch of one non-null Int64 `timestamp` column — the minimal input for the sort paths.
    fn ts_batch(vals: Vec<i64>) -> RecordBatch {
        let s = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]));
        RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    /// The `timestamp` values of a `ts_batch` list, concatenated in emit order.
    fn ts_values(batches: &[RecordBatch]) -> Vec<i64> {
        batches.iter().flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().to_vec()).collect()
    }

    /// Row-wise rendering of a batch list, independent of chunking and physical
    /// encoding. `cols = None` renders whole rows; a subset renders just those.
    fn render(batches: &[RecordBatch], cols: Option<&[&str]>) -> Vec<String> {
        use arrow::util::display::{ArrayFormatter, FormatOptions};
        let opts = FormatOptions::default().with_null("<NULL>");
        batches
            .iter()
            .flat_map(|b| {
                let schema = b.schema();
                let picked: Vec<(&str, _)> = schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| cols.is_none_or(|c| c.contains(&f.name().as_str())))
                    .map(|(i, f)| (f.name().as_str(), ArrayFormatter::try_new(b.column(i).as_ref(), &opts).unwrap()))
                    .collect();
                (0..b.num_rows()).map(|row| picked.iter().map(|(n, f)| format!("{n}={}", f.value(row))).collect::<Vec<_>>().join("|")).collect::<Vec<_>>()
            })
            .collect()
    }

    /// Equivalence check for the streaming k-way merge against the reference
    /// concat + global-lexsort + take path, over adversarial inputs. Tie order is
    /// arbitrary (the reference sort is unstable), so we assert the same row
    /// multiset, the same sort-key sequence, and the same `sorted` outcome.
    #[test]
    fn streaming_merge_matches_reference_sort() {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, true),
            Field::new("id", DataType::Utf8, true),
            // Payload columns: never sort keys, so any order difference shows up here.
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

    /// The merge emits bounded chunks rather than one whole-bucket batch.
    #[test]
    fn streaming_merge_emits_bounded_chunks() {
        // Two interleaved runs, 200k rows total — past the row cap.
        let mk = |off: i64| ts_batch((0..100_000).map(|i| i * 2 + off).rev().collect());
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), vec![mk(0), mk(1)], DEFAULT_SORT_SKIP_BYTES);
        assert!(sorted);
        let chunks = drain(out);
        assert!(chunks.len() > 1, "output must be chunked, not one 200k-row batch");
        assert!(chunks.iter().all(|c| c.num_rows() <= MERGE_CHUNK_ROWS_MAX), "no chunk may exceed the row cap");
        let all = ts_values(&chunks);
        assert_eq!(all.len(), 200_000);
        assert!(all.windows(2).all(|w| w[0] <= w[1]), "merge of sorted runs is globally sorted");
    }

    // A bucket whose batches differ by an evolved nullable column must still flush
    // as a globally sorted file with an honest `sorting_columns` footer: one
    // unsorted file disables the reader's all-or-nothing footer-ordering pushdown
    // for the whole scan.
    #[test]
    fn heterogeneous_bucket_still_sorts_with_honest_footer() {
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

    fn day(y: i32, m: u32, d: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, m, d).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros()
    }

    const DATED_URI: &str = "s3://b/timefusion/default/otel/project_id=p/date=2026-06-15/f.parquet";

    #[test_case(DATED_URI, day(2026, 6, 1), day(2026, 6, 30) => true ; "window squarely contains the partition day")]
    #[test_case(DATED_URI, day(2026, 6, 16), day(2026, 6, 20) => false ; "window entirely after the partition day")]
    #[test_case(DATED_URI, day(2026, 5, 1), day(2026, 6, 14) => false ; "window entirely before the partition day")]
    #[test_case(DATED_URI, day(2026, 6, 15), day(2026, 6, 15) => true ; "boundary days are inclusive")]
    #[test_case(DATED_URI, i64::MIN, day(2026, 6, 30) => true ; "open lower bound matches that side")]
    #[test_case(DATED_URI, day(2026, 6, 1), i64::MAX => true ; "open upper bound matches that side")]
    #[test_case(DATED_URI, i64::MIN, i64::MAX => true ; "fully open window matches")]
    #[test_case("s3://b/no-partition/f.parquet", day(2026, 6, 16), day(2026, 6, 20) => true ; "missing or unparseable date is conservatively in-window")]
    fn uri_date_in_window_gates_on_partition_day(uri: &str, from: i64, to: i64) -> bool {
        uri_date_in_window(uri, from, to)
    }

    /// The requested level drives ZSTD compression and is recorded verbatim in the
    /// footer tier key, including when it is out of range and compression falls back.
    #[test_case(3 => (Compression::ZSTD(ZstdLevel::try_new(3).unwrap()), "3".to_string()) ; "level 3")]
    #[test_case(9 => (Compression::ZSTD(ZstdLevel::try_new(9).unwrap()), "9".to_string()) ; "level 9")]
    #[test_case(15 => (Compression::ZSTD(ZstdLevel::try_new(15).unwrap()), "15".to_string()) ; "level 15")]
    #[test_case(19 => (Compression::ZSTD(ZstdLevel::try_new(19).unwrap()), "19".to_string()) ; "level 19")]
    #[test_case(999 => (Compression::ZSTD(ZstdLevel::try_new(ZSTD_COMPRESSION_LEVEL).unwrap()), "999".to_string()) ; "invalid zstd level falls back")]
    fn compression_level_drives_zstd_and_footer_tier(level: i32) -> (Compression, String) {
        let p = build_writer_properties(&cfg(), &schema_with(vec![], vec![]), level, true, None);
        let kv = p.key_value_metadata().expect("KV metadata present");
        let tier = kv.iter().find(|k| k.key == COMPRESSION_TIER_KEY).expect("tier key present");
        (p.compression(&ColumnPath::from("anything")), tier.value.clone().expect("tier value present"))
    }

    #[test]
    fn row_group_size_is_a_byte_limit() {
        let mut c = cfg();
        c.timefusion_max_row_group_size = 128 * 1024 * 1024;
        let p = build_writer_properties(&c, &schema_with(vec![], vec![]), 3, true, None);
        assert_eq!(p.max_row_group_bytes(), Some(c.timefusion_max_row_group_size));
    }

    // The footer check must reject foreign/truncated objects written over a
    // checkpoint, so log cleanup is withheld and the JSON stays recoverable.
    #[test_case(b"\x10\x00\x00\x00PAR1", 1024 => true ; "footer_len=16 and magic ok")]
    #[test_case(b"quest>\x00\x00", 299 => false ; "the real clobber: an XML body's last 8 bytes, no PAR1 magic")]
    #[test_case(b"Result>\n", 299 => false ; "another XML body tail")]
    #[test_case(b"\xff\xff\xff\x7fPAR1", 64 => false ; "valid magic but a footer length that can't fit in the file")]
    #[test_case(b"\x00\x00\x00\x00PAR1", 1024 => false ; "footer_len == 0 is impossible for a real file")]
    #[test_case(b"PAR1", 8 => false ; "wrong length input")]
    fn parquet_tail_ok_rejects_foreign_and_truncated_objects(tail: &[u8], file_len: u64) -> bool {
        super::parquet_tail_ok(tail, file_len)
    }

    // warm_all_footers default: non-recent files stay in the warm set as
    // footer-only, NEWEST partition first so a process dying mid-warm has still
    // warmed what dashboards query; with the flag off they are dropped entirely.
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

    /// Bloom filters are opt-in per column, and the global kill switch wins over the opt-in.
    #[test_case("id", false => true ; "flagged column has bloom")]
    #[test_case("body", false => false ; "unflagged column has no bloom")]
    #[test_case("id", true => false ; "global kill switch overrides opt-in")]
    fn bloom_opt_in_only_for_flagged_columns(col: &str, globally_disabled: bool) -> bool {
        let mut flagged = field("id", "Utf8");
        flagged.bloom_filter = true;
        let mut c = cfg();
        c.timefusion_bloom_filter_disabled = globally_disabled;
        build_writer_properties(&c, &schema_with(vec![flagged, field("body", "Utf8")], vec![]), 3, true, None)
            .bloom_filter_properties(&ColumnPath::from(col))
            .is_some()
    }

    /// Per-column encoding and dictionary selection: `(encoding, dictionary_enabled)`.
    /// `id` is the declared sort key; `stacktrace` opts out of dictionary encoding.
    #[test_case("id" => (Some(Encoding::DELTA_BYTE_ARRAY), false) ; "sort key utf8 uses delta byte array and no dict")]
    #[test_case("ts" => (Some(Encoding::DELTA_BINARY_PACKED), false) ; "timestamp uses delta binary packed and no dict")]
    #[test_case("n" => matches (Some(Encoding::DELTA_BINARY_PACKED), _) ; "int uses delta binary packed")]
    #[test_case("stacktrace" => matches (_, false) ; "dictionary opt-out disables dict")]
    fn column_encoding_and_dictionary(col: &str) -> (Option<Encoding>, bool) {
        let mut no_dict = field("stacktrace", "Utf8");
        no_dict.dictionary = Some(false);
        let sch = schema_with(vec![field("id", "Utf8"), field("ts", "Timestamp(Nanosecond, None)"), field("n", "Int64"), no_dict], vec!["id"]);
        let p = build_writer_properties(&cfg(), &sch, 3, true, None);
        (p.encoding(&ColumnPath::from(col)), p.dictionary_enabled(&ColumnPath::from(col)))
    }

    // Page-level stats only on declared sort keys; wide columns get chunk-level
    // stats to keep the ColumnIndex small.
    #[test_case("timestamp" => EnabledStatistics::Page ; "declared sort key gets page stats")]
    #[test_case("body" => EnabledStatistics::Chunk ; "wide column gets chunk stats")]
    fn page_stats_only_for_sort_keys(col: &str) -> EnabledStatistics {
        let sch = schema_with(vec![field("timestamp", "Timestamp(Microsecond, None)"), field("body", "Utf8")], vec!["timestamp"]);
        build_writer_properties(&cfg(), &sch, 3, true, None).statistics_enabled(&ColumnPath::from(col))
    }

    /// Pins the DataFusion contract the pool wrapping depends on: an exhausted
    /// pool's error must name the HOLDERS, not just the starved consumer.
    #[test]
    fn an_exhausted_maintenance_pool_names_the_consumers_holding_it() {
        use datafusion::execution::memory_pool::{FairSpillPool, MemoryConsumer, MemoryPool, TrackConsumersPool};
        let top = std::num::NonZeroUsize::new(5).expect("5 is non-zero");
        let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(FairSpillPool::new(4 * 1024 * 1024), top));

        let hog = MemoryConsumer::new("TheBinThatAteThePool").register(&pool);
        hog.grow(3 * 1024 * 1024);
        let starved = MemoryConsumer::new("SortPreservingMergeExec").register(&pool);
        let error = starved.try_grow(3 * 1024 * 1024).expect_err("pool is too small for both");

        let text = error.to_string();
        assert!(text.contains("TheBinThatAteThePool"), "the HOLDER must be named, that is the whole point: {text}");
    }

    // Only declare the parquet SortingColumn footer when the writer actually
    // sorted the rows; claiming an order that was not written breaks
    // order-trusting readers.
    #[test_case(true => true ; "flush/dedup path declares the sort order")]
    #[test_case(false => false ; "optimize/compact path declares no order")]
    fn sorting_columns_declared_only_when_sorted(declare_sorted: bool) -> bool {
        let s = schema_with(vec![field("timestamp", "Timestamp(Microsecond, None)"), field("id", "Utf8")], vec!["timestamp", "id"]);
        build_writer_properties(&cfg(), &s, 3, declare_sorted, None).sorting_columns().is_some()
    }

    /// The decoded-metadata cache limit must reach the RuntimeEnv (a SessionConfig
    /// `datafusion.runtime.*` string would not), and query spill must be bounded
    /// and land on the configured volume — an unconfigured DiskManager still
    /// answers every query correctly, so only the cap can catch it.
    #[test]
    fn query_runtime_applies_the_metadata_limit_and_bounds_spill_off_the_process_temp_dir() {
        let pool = std::sync::Arc::new(datafusion::execution::memory_pool::GreedyMemoryPool::new(1024 * 1024));
        let bytes = 321 * 1024 * 1024;
        let dir = tempfile::tempdir().expect("spill dir");
        let rt = build_query_runtime_env(pool, bytes, crate::database::spill_disk_builder(dir.path().to_path_buf(), 7));

        assert_eq!(rt.cache_manager.get_metadata_cache_limit(), bytes);
        assert_eq!(rt.disk_manager.max_temp_directory_size(), 7 * 1024 * 1024 * 1024, "query spill must honour the configured cap, not DataFusion's default");
        // DataFusion's default cap is far larger; matching it means no builder was applied.
        let unconfigured = datafusion::execution::disk_manager::DiskManagerBuilder::default().build().unwrap().max_temp_directory_size();
        assert_ne!(rt.disk_manager.max_temp_directory_size(), unconfigured, "an unconfigured DiskManager spills to the process temp dir");
    }

    // Read-side dedup skip: fingerprint is order-insensitive but content-
    // sensitive, and the window→dates expansion bounds itself.
    #[test]
    fn dedup_skip_fingerprint_and_window_dates() {
        let a = vec!["p/date=2026-07-01/f1.parquet".to_string(), "p/date=2026-07-01/f2.parquet".to_string()];
        let mut b = a.clone();
        b.reverse();
        assert_eq!(partition_file_fp(&a), partition_file_fp(&b), "order must not matter");
        let c = vec![a[0].clone()];
        assert_ne!(partition_file_fp(&a), partition_file_fp(&c), "content must matter");

        let day = 86_400_000_000i64;
        assert_eq!(window_dates(0, 0).map(|d| d.len()), Some(1));
        assert_eq!(window_dates(0, 2 * day).map(|d| d.len()), Some(3));
        assert_eq!(window_dates(2 * day, 0), None, "inverted window");
        assert_eq!(window_dates(0, 400 * day), None, "wider than a year → keep DedupExec");
    }

    // Slice-coverage merge: disjoint stays disjoint, touching/overlapping fuse,
    // and out-of-order inserts still converge to one day-spanning interval.
    #[test_case(&[(0, 10), (20, 30)] => vec![(0, 10), (20, 30)] ; "a gap must survive")]
    #[test_case(&[(0, 10), (10, 20)] => vec![(0, 20)] ; "half-open adjacency fuses")]
    #[test_case(&[(0, 15), (10, 20)] => vec![(0, 20)] ; "overlap fuses")]
    #[test_case(&[(12, 24), (0, 6), (6, 12)] => vec![(0, 24)] ; "order of arrival must not matter")]
    #[test_case(&[(0, 24), (5, 10)] => vec![(0, 24)] ; "a contained slice changes nothing")]
    fn clean_interval_merge(pairs: &[(i64, i64)]) -> Vec<(i64, i64)> {
        let mut v = Vec::new();
        pairs.iter().for_each(|&p| merge_clean_interval(&mut v, p));
        v
    }

    const UNIFIED_PATHS: &[&str] =
        &["project_id=p1/date=2026-08-03/a.parquet", "project_id=p2/date=2026-08-03/b.parquet", "project_id=p1/date=2026-08-02/c.parquet"];
    const CUSTOM_PATHS: &[&str] = &["date=2026-08-03/a.parquet", "date=2026-08-02/b.parquet"];

    /// A unified table carries a `project_id=` segment and must be filtered on it;
    /// a custom project table has none, so the date filter alone is exact.
    #[test_case(UNIFIED_PATHS, "p1" => vec!["project_id=p1/date=2026-08-03/a.parquet"] ; "unified table filters project and date")]
    #[test_case(CUSTOM_PATHS, "physical-owner" => vec!["date=2026-08-03/a.parquet"] ; "custom table keyed by its physical owner")]
    fn dedup_file_selection_is_exact_for_unified_and_custom_tables(paths: &[&str], project_id: &str) -> Vec<String> {
        dedup_partition_paths(paths.iter().map(|p| p.to_string()), project_id, "2026-08-03")
    }

    // Batches are globally sorted by the declared lead key before write.
    #[test]
    fn sort_batches_orders_by_declared_keys() {
        let batches = vec![ts_batch(vec![3, 1]), ts_batch(vec![2, 0])];
        let (out, sorted) = sort_batches_by_schema(&schema_with(vec![], vec!["timestamp"]), batches, DEFAULT_SORT_SKIP_BYTES);
        assert!(sorted);
        let out = drain(out);
        assert_eq!(out.len(), 1);
        assert_eq!(ts_values(&out), vec![0, 1, 2, 3]);
        // No declared sort columns → input returned untouched, sorted=false.
        let (passthrough, sorted) = sort_batches_by_schema(&schema_with(vec![], vec![]), vec![out[0].clone(), out[0].clone()], DEFAULT_SORT_SKIP_BYTES);
        assert!(!sorted);
        assert_eq!(drain(passthrough).len(), 2);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod footer_repair_schedule_tests {
    /// A repair pass must admit what its BUDGET can finish, not what one tick could:
    /// a file a hair over the configured cap is otherwise never a candidate at all.
    #[test]
    fn repair_reach_follows_the_budget_and_admits_the_file_that_pinned_shipbubble() {
        const GIB: i64 = 1073741824;
        let budget = std::time::Duration::from_secs(8640); // the 144-minute default
        let reach = super::repair_reach_bytes(GIB, budget);
        assert!(reach > 1_088_634_971, "the file that pinned shipbubble must be admitted, got {reach}");
        assert_eq!(super::repair_reach_bytes(8 * GIB, budget), 8 * GIB, "a larger configured cap still wins");
        assert!(super::repair_reach_bytes(0, std::time::Duration::from_secs(240)) < GIB / 4, "240s reaches only a small file");
    }

    /// Each escalated flush sort's pool slice must cover the largest sort measured
    /// (~772 MB): a refused allocation writes the group UNSORTED, and one unsorted file
    /// kills the reader's all-or-nothing footer ordering for the whole partition.
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
        // The 64MB floor cannot feed one sort, but zero permits would deadlock escalation.
        assert_eq!(super::flush_sort_permits(64 * MB), 1, "the floor must still admit one sort");
    }

    /// A packing bin the tick cannot finish does not run late, it produces NOTHING
    /// forever (timeout → discard → identical re-selection), so the unit must be sized
    /// by the time available.
    #[test]
    fn a_packing_bin_is_sized_to_what_the_tick_can_actually_finish() {
        const MB: i64 = 1024 * 1024;
        let tick = std::time::Duration::from_secs(240);
        let target = super::pack_target_bytes(256 * MB, tick);
        // The SLOWEST rate measured across tables, not the average — there is a ~6x
        // spread between tables on identically sized bins.
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
        // The run length is INDEPENDENT of that period: a contention-free whole-file
        // rewrite was measured at 43 min, so the budget must clear ~3x that. Overlapping
        // ticks are skipped, so budget > period is sound.
        let budget = std::time::Duration::from_secs(cfg.maintenance.timefusion_footer_repair_budget_secs);
        assert_eq!(budget, std::time::Duration::from_secs(8640), "144-minute budget");
        assert!(budget >= std::time::Duration::from_secs(43 * 60 * 3), "must clear 3x the measured solo rewrite");
        assert!(budget > period, "a long run must not be capped by a short cadence");
    }
}

#[cfg(test)]
mod clean_coverage_retention_tests {
    use super::retain_clean_intervals;

    /// The defect this replaces: coverage was discarded on ANY fingerprint move,
    /// and on a live partition that is every flush. Prod 2026-09-14 measured the
    /// consequence — `cert_slice_files_proved` 9 against `cert_slice_files_unproven`
    /// 2,194, because accumulated coverage never outlived one slice while files
    /// span hours.
    ///
    /// Ingest appends at the TAIL, so the morning's proved intervals are disjoint
    /// from every file arriving now and must survive.
    #[test]
    fn a_tail_append_keeps_the_morning_proved() {
        let day = [(0i64, 100), (100, 200), (200, 300)];
        let kept = retain_clean_intervals(&day, &[Some((320, 400))]).expect("spans known");
        assert_eq!(kept, day.to_vec(), "a file that lands after every interval invalidates none of them");
    }

    /// Only the overlapped interval is lost — the point of the change.
    #[test]
    fn an_overlapping_file_drops_only_what_it_touches() {
        let day = [(0i64, 100), (100, 200), (200, 300)];
        let kept = retain_clean_intervals(&day, &[Some((150, 160))]).expect("spans known");
        assert_eq!(kept, vec![(0, 100), (200, 300)], "the 100-200 interval is the only one the new rows could duplicate into");
    }

    /// Fails CLOSED. A file with no statistics overlaps everything, the same rule
    /// `partition_file_spans` states for readers, so the caller must reset.
    #[test]
    fn a_file_without_span_statistics_forces_a_reset() {
        assert!(retain_clean_intervals(&[(0, 100)], &[Some((500, 600)), None]).is_none(), "unknown span must not be read as disjoint");
    }

    /// Half-open interval against an inclusive span, matching
    /// `certify_files_within_slice`. A file whose max lands exactly on `start`
    /// DOES touch the interval; one whose min equals `end` does not.
    #[test]
    fn boundaries_match_the_certification_test() {
        assert_eq!(retain_clean_intervals(&[(100, 200)], &[Some((50, 100))]).unwrap(), Vec::<(i64, i64)>::new(), "max == start overlaps");
        assert_eq!(retain_clean_intervals(&[(100, 200)], &[Some((200, 250))]).unwrap(), vec![(100, 200)], "min == end does not");
    }
}
