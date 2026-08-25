//! OpenTelemetry metrics export.
//!
//! Sits next to `telemetry.rs` (which owns traces). On `init_metrics()` we
//! create a `SdkMeterProvider` with the OTLP exporter, register a few
//! observable gauges that read from the `BufferedWriteLayer` once per export
//! cycle, and install it as the global meter provider.
//!
//! Why observables (not synchronous counters): the stats we care about
//! (memory pressure, oldest bucket age, WAL bytes) live inside the
//! `BufferedWriteLayer` and are already computed by `snapshot_stats()` for
//! the SQL `timefusion.stats()` view. Polling on each export keeps the hot
//! path untouched.
//!
//! Counters (insert success/failure, corruption events) are exposed through
//! `MetricsRegistry::record_*` so they can be incremented inline. They live
//! in a process-global `OnceLock`; if init isn't called (tests, embedded
//! use), the helpers no-op.

use std::{
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

static MAINTENANCE_RETRY_REASON: OnceLock<Mutex<String>> = OnceLock::new();

pub fn set_maintenance_retry_reason(reason: &str) {
    let mut current = MAINTENANCE_RETRY_REASON.get_or_init(|| Mutex::new(String::new())).lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    current.clear();
    current.push_str(reason);
}

pub fn maintenance_retry_reason() -> String {
    MAINTENANCE_RETRY_REASON.get_or_init(|| Mutex::new(String::new())).lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
}

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Meter},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::{PeriodicReader, SdkMeterProvider},
};
use tracing::{info, warn};

use crate::{config::TelemetryConfig, tantivy::search::TantivyIndexService, write::BufferedWriteLayer};

static METRICS: OnceLock<MetricsRegistry> = OnceLock::new();

/// Declares the counter registry struct and its `new()` builder from a single
/// list of `field => "metric.id": "description"` entries, so adding a counter
/// is a one-line change with no risk of the field and registration drifting.
macro_rules! counter_registry {
    ($($field:ident => $id:literal : $desc:literal),+ $(,)?) => {
        /// Holds counters that need to be incremented from the hot path. Gauges
        /// are observed by callback and don't need to live here.
        pub struct MetricsRegistry {
            $(pub $field: Counter<u64>,)+
        }

        impl MetricsRegistry {
            fn new(meter: &Meter) -> Self {
                Self {
                    $($field: meter.u64_counter($id).with_description($desc).build(),)+
                }
            }
        }
    };
}

counter_registry! {
    ingest_inserts             => "timefusion.ingest.inserts": "Ingest insert calls accepted",
    ingest_rows                => "timefusion.ingest.rows": "Rows accepted into MemBuffer",
    ingest_errors              => "timefusion.ingest.errors": "Ingest call failures",
    ingest_event_time_bounded  => "timefusion.ingest.event_time_bounded_rows": "Rows dropped at admission for event timestamps outside the sanity bounds (pre-2000 or >48h future) — a client unit error otherwise mints garbage date partitions",
    wal_corruption             => "timefusion.wal.corruption_events": "WAL entries that failed to deserialize or replay",
    quarantine_redriven        => "timefusion.wal.quarantine_redriven": "Quarantined WAL payloads successfully re-ingested through the durable insert path at boot",
    quarantine_backlog         => "timefusion.wal.quarantine_backlog_events": "Boot-time detections of a non-empty quarantine after re-drive — acked data is NOT in the store. PAGE if > 0",
    wal_gc_deleted_files       => "timefusion.wal.gc_deleted_files": "Stale WAL files reclaimed by the mtime reaper (walrus leaks files across restarts)",
    flush_completed            => "timefusion.flush.completed": "Flush cycles that committed to Delta",
    flush_failed               => "timefusion.flush.failed": "Flush cycles that errored",
    flush_stalled              => "timefusion.flush.stalled": "Flush bucket commits that exceeded the flush-bucket watchdog timeout (Delta/S3 commit hung, holding flush_lock). A stalled flush frees no MemBuffer memory → inserts wedge at the hard limit. PAGE if > 0",
    flush_sort_unsorted_fallbacks => "timefusion.flush.sort_unsorted_fallbacks": "Escalated flush sorts that failed and wrote the group UNSORTED. One such file disables the reader's footer ordering for its whole partition (query-time SortExec, unordered MOR dedup) — a read-path incident in the making. PAGE if > 0",
    query_executions           => "timefusion.query.executions": "SQL query plans executed",
    tantivy_prefilter_attempts => "timefusion.tantivy.prefilter_attempts": "Queries where at least one text_match predicate triggered a tantivy lookup",
    tantivy_prefilter_used     => "timefusion.tantivy.prefilter_used": "Queries where the tantivy id-set prefilter was applied to the Delta scan",
    count_pushdown_used        => "timefusion.query.count_pushdown_used": "COUNT(*) queries answered from Delta add-action stats without scanning",
    logical_count_pushdown_used => "timefusion.query.logical_count_pushdown_used": "Merge-on-read COUNT(*) queries answered from an exact snapshot-bound logical index plus append/MemBuffer overlays",
    tantivy_prefilter_skipped  => "timefusion.tantivy.prefilter_skipped": "Queries where tantivy lookup was attempted but pushdown was skipped (no index, hit cap, or low selectivity)",
    tantivy_prefilter_errors   => "timefusion.tantivy.prefilter_errors": "Tantivy lookups that errored (S3 down, parse failure, etc.)",
    tantivy_build_failures     => "timefusion.tantivy.build_failures": "Post-flush tantivy index builds that errored — accumulating drift means queries silently fall back to UDF scan",
    tantivy_recovery_deferred  => "timefusion.tantivy.recovery_deferred": "Parquet files whose Tantivy builds were deferred until WAL replay completed",
    tantivy_merges_deferred    => "timefusion.tantivy.merges_deferred": "Multi-segment Tantivy builds that shipped unmerged (merge kept off the ingest window; the post-optimize/backfill rebuild collapses them). Segment count stays bounded by MAX_DEFERRED_SEGMENTS",
    tantivy_merges_executed    => "timefusion.tantivy.merges_executed": "Tantivy builds that ran a segment merge — maintenance rebuilds (expected), or an ingest build that blew past MAX_DEFERRED_SEGMENTS (its warn log names the count). Sustained growth with no optimize running means merge CPU is back in the flush window",
    dedup_dropped_rows         => "timefusion.flush.dedup_dropped_rows": "Rows collapsed by per-table dedup_keys (last-write-wins) before Delta commit",
    optimize_partitions_rewritten => "timefusion.optimize.partitions_rewritten": "Date partitions rewritten by full (z-order) optimize",
    optimize_partitions_skipped   => "timefusion.optimize.partitions_skipped": "Date partitions skipped by full optimize because their file set was unchanged since the last run (cache churn avoided)",
    compaction_dedup_dropped_rows => "timefusion.compaction.dedup_dropped_rows": "Rows collapsed by Delta-vs-Delta dedup compaction (cross-flush duplicates)",
    backpressure_engaged       => "timefusion.ingest.backpressure_engaged": "Inserts that hit the memory hard limit and triggered synchronous flush-to-Delta instead of rejecting (alert if sustained > 0)",
    backpressure_rejected      => "timefusion.ingest.backpressure_rejected": "Inserts rejected after the backpressure window expired without freeing memory — means Delta flush is not keeping up (page: data still in WAL but ingest is dropping)",
    backpressure_force_flush   => "timefusion.ingest.backpressure_force_flush": "Current open-bucket force-flushes triggered by sustained backpressure (escalation tier)",
    optimize_conflict          => "timefusion.optimize.conflict": "Optimize/compaction commits that hit an OCC conflict (a concurrent txn touched a file the merge read). Retried — but a sustained nonzero rate means optimize is losing commit races to dedup/flush. WARN if rate() stays > 0 across several ticks",
    optimize_failed            => "timefusion.optimize.failed": "Optimize/compaction runs that ultimately errored or gave up after exhausting retries. The partition stays fragmented until a later run succeeds, so small files pile up silently. PAGE if > 0 sustained",
    dml_conflict               => "timefusion.dml.conflict": "DML (UPDATE/DELETE) Delta operations that lost an OCC race to a concurrent commit and were retried on a fresh snapshot. Sustained rate > 0 means UPDATE churn is racing flush commits",
    dml_retry_success           => "timefusion.dml.retry_success": "DML Delta operations that succeeded after at least one OCC retry",
    dml_retry_exhausted         => "timefusion.dml.retry_exhausted": "DML Delta operations that exhausted the OCC retry budget and failed",
    dml_delta_leg_skipped      => "timefusion.dml.delta_leg_skipped": "DML Delta legs skipped because the predicate's time window lies entirely above the flush watermark — the matched rows are buffer-only, so the flush persists their post-DML values and the Delta merge would scan+commit for nothing",
    dml_coalesce_enqueued      => "timefusion.dml.coalesce_enqueued": "UPDATE ... FROM statements whose Delta leg was deferred into the coalescer queue",
    dml_coalesce_merges        => "timefusion.dml.coalesce_merges": "Delta merges executed by coalescer drains (each replaces N deferred statement-merges; compare with coalesce_enqueued for the batching ratio)",
    dml_coalesce_dropped       => "timefusion.dml.coalesce_dropped": "Coalesced DML groups whose rows could NOT even be quarantined — deferred Delta updates were LOST for rows already in Delta (buffer-resident rows are unaffected). PAGE if > 0",
    dml_coalesce_quarantined   => "timefusion.dml.coalesce_quarantined": "Coalesced DML groups parked to <wal_dir>/quarantine/dml after exhausting drain retries. Rows are recoverable (Arrow IPC + .meta sidecar) but the Delta leg has NOT applied — investigate and re-drive. ALERT if > 0",
    write_capture_skipped      => "timefusion.cache.write_capture_skipped": "Multipart uploads whose cache write-tee was skipped or abandoned (over the per-upload cap or the process-wide capture budget). Purely a cache miss later — the upload itself is unaffected. Sustained high values on flush-sized files mean the caps are too tight",
    cache_confirm_attempts     => "timefusion.cache.confirm_attempts": "Files probed by the pre-drain cache confirm on the flush path (Influx oracle ordering). Files captured during upload cost only this probe",
    cache_confirm_warmed       => "timefusion.cache.confirm_warmed": "Files the pre-drain confirm had to fetch because write-capture skipped them. Sustained ~= confirm_attempts means the write-capture caps are too tight — every flush output is being re-read from S3",
    cache_confirm_timeouts     => "timefusion.cache.confirm_timeouts": "Pre-drain cache confirms that hit their bound and gave up. Best-effort — the commit and the drain proceed; the next query on those files just pays an S3 round-trip",
    rollup_hits                => "timefusion.rollup.hits": "Dashboard aggregates served from the pre-aggregated rollup instead of raw spans",
    rollup_misses              => "timefusion.rollup.misses": "Dashboard aggregates that fell through to a raw scan, labelled by REASON. Without the reason breakdown there is no feedback loop telling us which dimension to add next — a rollup silently serving 20% of traffic looks identical to one serving 90%",
    rollup_scan_cohorts        => "timefusion.rollup.maintenance.scan_cohorts": "Bounded rollup scan cohorts executed",
    rollup_scan_projects       => "timefusion.rollup.maintenance.scan_projects": "Projects included in bounded rollup scan cohorts",
    rollup_scan_estimated_bytes => "timefusion.rollup.maintenance.scan_estimated_bytes": "Estimated decoded input bytes admitted to rollup cohort scans",
    rollup_cohort_splits       => "timefusion.rollup.maintenance.cohort_splits": "Resource-exhausted rollup cohorts split and retried",
    rollup_singleton_failures  => "timefusion.rollup.maintenance.singleton_failures": "Rollup projects isolated into backoff after singleton failure",
    rollup_staged_projects     => "timefusion.rollup.maintenance.staged_projects": "Project replacements staged outside the Delta commit lock",
    rollup_shared_commits      => "timefusion.rollup.maintenance.shared_commits": "Shared rollup replacement transactions committed",
    rollup_commit_actions      => "timefusion.rollup.maintenance.commit_actions": "Delta actions included in shared rollup commits",
    rollup_occ_retries         => "timefusion.rollup.maintenance.occ_retries": "Shared rollup commits retried after OCC conflict",
    rollup_ambiguous_landings  => "timefusion.rollup.maintenance.ambiguous_landings": "Errored rollup commits confirmed landed by probing",
    rollup_scan_duration_ms    => "timefusion.rollup.maintenance.scan_duration_ms": "Cumulative rollup scan-wave duration in milliseconds",
    rollup_staging_duration_ms => "timefusion.rollup.maintenance.staging_duration_ms": "Cumulative rollup file-staging duration in milliseconds",
    rollup_commit_duration_ms  => "timefusion.rollup.maintenance.commit_duration_ms": "Cumulative shared rollup commit duration in milliseconds",
    rollup_end_to_end_duration_ms => "timefusion.rollup.maintenance.end_to_end_duration_ms": "Cumulative rollup cohort end-to-end duration in milliseconds",
    rollup_output_rows         => "timefusion.rollup.maintenance.output_rows": "Rows written by rollup maintenance",
    rollup_output_files        => "timefusion.rollup.maintenance.output_files": "Parquet files staged by rollup maintenance",
    rollup_full_hours_rebuilt  => "timefusion.rollup.maintenance.full_hours_rebuilt": "Rollup project-hours rebuilt by full scans",
    rollup_incremental_hours_rebuilt => "timefusion.rollup.maintenance.incremental_hours_rebuilt": "Rollup project-hours rebuilt from durable dirty masks",
    cache_insert_bypassed      => "timefusion.cache.insert_bypassed": "Cache populations suppressed because the read ran inside a large-scan bypass scope (scan-resistant admission — a wide historical scan must not evict the hot tail)",
    dedup_chunk_skipped        => "timefusion.dedup.chunk_skipped": "Dedup chunk rewrites skipped (over the rewrite-byte budget, or partition in failure backoff). Duplicates persist in Delta — read-side dedup keeps queries correct — until a later sweep or manual compaction clears them. WARN if sustained",
    maintenance_checkpoint_failed => "timefusion.maintenance.checkpoint_failed": "Out-of-band checkpoint attempts that errored (e.g. R2 500 on the checkpoint PUT). Retried next tick; ingest is unaffected. WARN if sustained — checkpoints falling behind slows boot replay and blocks log cleanup",
    maintenance_log_cleanup_failed => "timefusion.maintenance.log_cleanup_failed": "Out-of-band expired-log-cleanup attempts that errored. Retried next tick; the _delta_log grows until it succeeds. WARN if sustained (a growing log slows every commit's version LIST)",
    maintenance_cron_long_running => "timefusion.maintenance.cron_long_running": "Cron maintenance runs that exceeded the long-running warning threshold while still in progress. Slow-but-healthy runs are allowed to finish; sustained nonzero with no completion means a job is wedged.",
    reconcile_dangling_removed => "timefusion.maintenance.reconcile_dangling_removed": "Active Add entries whose parquet object was missing from the store and got Remove'd by the reconcile task. NONZERO means committed data was destroyed elsewhere (commit-path parquet deletion bug) — PAGE and investigate",
    commit_lock_timeouts       => "timefusion.commit.lock_timeouts": "Commit-path operations abandoned by their bound while holding a per-table commit lock (attribute `op`: wave_commit, flush_commit, coalesced_commit, *_refresh, landing_probe). Each one is a hung object-store request that WOULD have stalled every committer for that table (prod 2026-07-30). The work is requeued and its staged parquet preserved (landing unconfirmed), so this is not data loss — but sustained nonzero means R2 latency is pathological and commit throughput is degraded. PAGE if sustained",
    maintenance_checkpoint_corrupt => "timefusion.maintenance.checkpoint_corrupt": "Checkpoints that failed post-write footer verification — the object _last_checkpoint points to is not a readable Parquet file (foreign/corrupt bytes, e.g. an S3 error or SelectObjectContent body written over it, 2026-07-17). Log cleanup is withheld so the JSON commit log — the only recovery source — is never pruned behind an unreadable checkpoint. PAGE if > 0",
}

pub fn registry() -> Option<&'static MetricsRegistry> {
    METRICS.get()
}

/// Local, in-process side of `metrics::histogram!()` calls, for readback (e.g.
/// `timefusion_stats` percentiles). The OTel bridge (`metrics_exporter_opentelemetry`)
/// is push-only — no snapshot API — so the two are fanned out from one global
/// `metrics::Recorder` via `metrics_util::layers::Fanout`; see `init_metrics()`.
///
/// One `Summary` (DDSketch, relative-error quantiles) per metric name, keyed
/// lazily on first `record()` — replaces hand-rolled power-of-two bucket arrays.
struct LocalHistograms(dashmap::DashMap<String, Mutex<metrics_util::storage::Summary>>);

impl LocalHistograms {
    fn quantile(&self, name: &str, p: f64) -> Option<f64> {
        self.0.get(name)?.lock().unwrap_or_else(std::sync::PoisonError::into_inner).quantile(p)
    }
}

struct LocalHistogramHandle {
    histograms: Arc<LocalHistograms>,
    name: String,
}

impl metrics::HistogramFn for LocalHistogramHandle {
    fn record(&self, value: f64) {
        self.histograms
            .0
            .entry(self.name.clone())
            .or_insert_with(|| Mutex::new(metrics_util::storage::Summary::with_defaults()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .add(value);
    }
}

type CounterGaugeRegistry = metrics_util::registry::Registry<metrics::Key, metrics_util::registry::AtomicStorage>;

/// Local recorder: histograms go through `LocalHistograms` (Summary/DDSketch,
/// above); counters and gauges go through `metrics_util`'s own `Registry` +
/// `AtomicStorage` — ready-made `Arc<AtomicU64>`-backed storage, so this needs
/// no bespoke counter/gauge type of its own. Wrapping both in one newtype
/// (rather than implementing `Recorder` on `Arc<LocalHistograms>` directly) is
/// required by the orphan rule — `Arc` isn't a fundamental type.
#[derive(Clone)]
struct LocalRecorder {
    histograms: Arc<LocalHistograms>,
    registry: Arc<CounterGaugeRegistry>,
}

impl metrics::Recorder for LocalRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(self.registry.get_or_create_counter(key, Clone::clone))
    }
    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(self.registry.get_or_create_gauge(key, Clone::clone))
    }
    fn register_histogram(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::from_arc(Arc::new(LocalHistogramHandle { histograms: self.histograms.clone(), name: key.name().to_owned() }))
    }
}

static LOCAL_HISTOGRAMS: OnceLock<Arc<LocalHistograms>> = OnceLock::new();
static LOCAL_REGISTRY: OnceLock<Arc<CounterGaugeRegistry>> = OnceLock::new();

/// Read back a quantile (0.0-1.0) for a name recorded via `metrics::histogram!()`.
/// `None` if metrics weren't initialized or the name has never recorded a value.
pub fn histogram_quantile(name: &str, p: f64) -> Option<f64> {
    LOCAL_HISTOGRAMS.get()?.quantile(name, p)
}

/// Read back the current value of a name recorded via `metrics::counter!()`.
/// 0 if metrics weren't initialized or the name has never recorded a value —
/// matches how these were read before migration (`AtomicU64::load` on an
/// unused field is also 0), so callers don't need an `Option`.
pub fn counter_value(name: &'static str) -> u64 {
    LOCAL_REGISTRY.get().and_then(|r| r.get_counter(&metrics::Key::from_name(name))).map_or(0, |c| c.load(Relaxed))
}

/// Read back the current value of a name recorded via `metrics::gauge!()`. See `counter_value`.
pub fn gauge_value(name: &'static str) -> f64 {
    LOCAL_REGISTRY.get().and_then(|r| r.get_gauge(&metrics::Key::from_name(name))).map_or(0.0, |g| f64::from_bits(g.load(Relaxed)))
}

/// Test helper: installs just the local (non-OTel) side of `init_metrics()`'s recorder, so a
/// unit/integration test can assert on `metrics::counter!()`/`histogram!()` values via
/// `counter_value()`/`gauge_value()`/`histogram_quantile()` without a running OTLP collector.
/// A no-op if a recorder is already installed (e.g. the test runs against a fully bootstrapped
/// server) — matches `init_metrics()`'s own idempotence.
pub fn init_local_metrics_for_test() {
    let local_histograms = Arc::new(LocalHistograms(dashmap::DashMap::new()));
    let local_registry = Arc::new(CounterGaugeRegistry::atomic());
    if metrics::set_global_recorder(LocalRecorder { histograms: local_histograms.clone(), registry: local_registry.clone() }).is_ok() {
        let _ = LOCAL_HISTOGRAMS.set(local_histograms);
        let _ = LOCAL_REGISTRY.set(local_registry);
    }
}

/// Initialize OTel metrics. Idempotent (subsequent calls are no-ops).
///
/// `buffered_layer` is a Weak so the metrics callback doesn't extend its
/// lifetime — the layer owns its shutdown order, not us.
pub fn init_metrics(
    config: &TelemetryConfig, buffered_layer: Weak<BufferedWriteLayer>, tantivy_indexer: Option<Weak<TantivyIndexService>>,
) -> anyhow::Result<()> {
    if METRICS.get().is_some() {
        return Ok(());
    }

    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", config.otel_service_name.clone()),
            KeyValue::new("service.version", config.otel_service_version.clone()),
        ])
        .build();

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otel_exporter_otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    let reader = PeriodicReader::builder(exporter).with_interval(Duration::from_secs(30)).build();
    opentelemetry::global::set_meter_provider(SdkMeterProvider::builder().with_reader(reader).with_resource(resource).build());

    let meter = opentelemetry::global::meter("timefusion");

    // Bridges the `metrics` facade (counter!/histogram!/gauge! macros) onto this
    // same Meter/provider, so ad-hoc call-site metrics (e.g. database::ScanMetrics's
    // ~40 counters) don't need a hand-rolled Counter/instrument wired through a
    // struct field. Fanned out to two recorders because the OTel bridge is
    // push-only (no snapshot API): `local_histograms`/`local_registry` also get
    // every call so `timefusion_stats` can read values back in-process via
    // `histogram_quantile()`/`counter_value()`/`gauge_value()`.
    // Idempotent-guarded by the METRICS OnceLock above; ignore "already installed"
    // from a second init_metrics() call (tests, embedded use).
    let local_histograms = Arc::new(LocalHistograms(dashmap::DashMap::new()));
    let local_registry = Arc::new(CounterGaugeRegistry::atomic());
    let fanout = metrics_util::layers::FanoutBuilder::default()
        .add_recorder(LocalRecorder { histograms: local_histograms.clone(), registry: local_registry.clone() })
        .add_recorder(metrics_exporter_opentelemetry::Recorder::with_meter(meter.clone()))
        .build();
    if metrics::set_global_recorder(fanout).is_ok() {
        let _ = LOCAL_HISTOGRAMS.set(local_histograms);
        let _ = LOCAL_REGISTRY.set(local_registry);
    }

    // Observable gauges polled from snapshot_stats() each export cycle. We
    // build one shared snapshot per export by stashing the Weak; if the
    // upgrade fails (layer dropped during shutdown), each gauge records 0.
    let bl_for_buckets = buffered_layer.clone();
    meter
        .u64_observable_gauge("timefusion.mem_buffer.oldest_bucket_age_seconds")
        .with_description("Age of oldest MemBuffer bucket; alert if > 2x flush_interval_secs")
        .with_callback(move |obs| {
            if let Some(age) = bl_for_buckets.upgrade().and_then(|l| l.snapshot_stats().oldest_bucket_age_secs) {
                obs.observe(age, &[]);
            }
        })
        .build();

    // Each simple metric upgrades the Weak, snapshots stats, and observes one
    // derived value; the macro captures that shape so each is a single line.
    // Metrics with conditional/Option logic (oldest bucket age, index lag) stay
    // spelled out. `counter` registers a monotonic observable counter (OTel Sum
    // → Prometheus Counter) so PromQL rate() applies reset detection and
    // survives restarts (values snap to 0 on boot); use it for cumulative
    // totals, `gauge` for point-in-time levels.
    macro_rules! layer_metric {
        (@build $method:ident, $id:literal, $desc:literal, |$s:ident| $value:expr) => {{
            let weak = buffered_layer.clone();
            meter
                .$method($id)
                .with_description($desc)
                .with_callback(move |obs| {
                    if let Some(layer) = weak.upgrade() {
                        let $s = layer.snapshot_stats();
                        obs.observe($value, &[]);
                    }
                })
                .build();
        }};
        (gauge $($rest:tt)+) => { layer_metric!(@build u64_observable_gauge, $($rest)+) };
        (counter $($rest:tt)+) => { layer_metric!(@build u64_observable_counter, $($rest)+) };
    }

    layer_metric!(gauge "timefusion.mem_buffer.pressure_pct", "MemBuffer memory pressure as percentage of max", |s| s.pressure_pct as u64);
    layer_metric!(gauge "timefusion.mem_buffer.estimated_bytes", "MemBuffer estimated heap residency in bytes", |s| s.mem_estimated_bytes as u64);
    layer_metric!(gauge "timefusion.mem_buffer.rows", "Total rows in MemBuffer across all projects/tables", |s| s.mem_total_rows as u64);
    // Ingest vs drain: rate() these two and compare. Ingested climbing faster
    // than flushed (while pressure_pct=100, flush_failed flat) = ingest
    // outpacing a working drain, not a stuck flush. Counters (not gauges) so
    // rate() handles the restart-to-0 reset. `ingested` includes WAL-recovered
    // rows so the pair stays comparable after a restart (see snapshot_stats).
    layer_metric!(counter "timefusion.mem_buffer.rows_ingested_total", "Cumulative rows accepted into MemBuffer (incl. WAL recovery)", |s| s
        .rows_ingested_total);
    layer_metric!(counter "timefusion.mem_buffer.rows_flushed_total", "Cumulative rows drained from MemBuffer to Delta", |s| s.rows_flushed_total);
    layer_metric!(gauge "timefusion.wal.disk_bytes", "Disk bytes occupied by WAL shards", |s| s.wal_disk_bytes);
    layer_metric!(gauge "timefusion.wal.files", "Number of WAL segment files on disk", |s| s.wal_files as u64);
    layer_metric!(gauge "timefusion.tantivy.recovery_pending_files", "Committed Parquet files awaiting post-WAL-replay Tantivy indexing", |s| s
        .tantivy_recovery_pending_files
        as u64);

    // Extracted-index disk cache, as of the last reap. Shares a volume with
    // the WAL, so a plateau at the configured budget is healthy and a climb
    // past it means the reap cron has stopped running.
    meter
        .u64_observable_gauge("timefusion.tantivy.cache_disk_bytes")
        .with_description("Bytes under <data_dir>/tantivy_cache as of the most recent reap; 0 until the first one runs")
        .with_callback(|obs| obs.observe(TANTIVY_CACHE_BYTES.load(std::sync::atomic::Ordering::Relaxed), &[]))
        .build();

    // Runtime scheduling lag — see `spawn_runtime_lag_sampler`. `last` is the
    // current state; `max` is the high-water mark for the process lifetime, so
    // a spike that has since recovered is still visible after the fact.
    meter
        .u64_observable_gauge("timefusion.runtime.scheduling_lag_ms")
        .with_description(
            "How late a 500ms timer task actually woke — nonzero means workers are starved, which is what a missed health probe looks like from inside",
        )
        .with_callback(|obs| obs.observe(RUNTIME_LAG_LAST_MS.load(Relaxed), &[]))
        .build();
    meter
        .u64_observable_gauge("timefusion.runtime.scheduling_lag_max_ms")
        .with_description("Worst scheduling lag this process lifetime; survives the spike so a post-mortem can still see it")
        .with_callback(|obs| obs.observe(RUNTIME_LAG_MAX_MS.load(Relaxed), &[]))
        .build();

    meter
        .u64_observable_gauge("timefusion.rollup.maintenance.pending_dirty_partitions")
        .with_description("Source partitions with durable rollup invalidations awaiting maintenance")
        .with_callback(|obs| obs.observe(maintenance_stats().rollup_dirty_partitions.load(Relaxed), &[]))
        .build();
    meter
        .u64_observable_gauge("timefusion.rollup.maintenance.oldest_invalidation_age_seconds")
        .with_description("Age of the oldest durable rollup invalidation")
        .with_callback(|obs| obs.observe(maintenance_stats().rollup_oldest_invalidation_age_secs.load(Relaxed), &[]))
        .build();

    // Index lag: how far behind ingest the newest published tantivy index is.
    // Computed as max(0, now - newest_max_timestamp). Surfaces the post-flush
    // indexing lag that the rewriter / search service can't shortcut around.
    if let Some(indexer_weak) = tantivy_indexer {
        meter
            .u64_observable_gauge("timefusion.tantivy.index_lag_seconds")
            .with_description("now() minus newest indexed timestamp; quantifies post-flush index lag")
            .with_callback(move |obs| {
                let Some(svc) = indexer_weak.upgrade() else { return };
                let Some(newest_idx) = svc.newest_indexed_micros() else { return };
                obs.observe(((crate::support::now_micros() - newest_idx).max(0) / 1_000_000) as u64, &[]);
            })
            .build();
    }

    if METRICS.set(MetricsRegistry::new(&meter)).is_err() {
        warn!("MetricsRegistry was already set; metric counters from this call will be discarded");
    }

    info!("OpenTelemetry metrics initialized (OTLP -> {}, interval=30s)", config.otel_exporter_otlp_endpoint);
    Ok(())
}

/// Build the standard (project_id, table_name) attribute pair.
/// Cardinality math at typical multi-tenant scale: ~100 projects × ~20
/// tables = 2k series per counter, which OTel handles cleanly. If a
/// deployment has thousands of projects, switch to label-only on
/// table_name (or drop project_id) — but that's an upstream knob, not
/// something to gate at this layer.
fn ingest_attrs(project_id: &str, table_name: &str) -> [KeyValue; 2] {
    [KeyValue::new("project_id", project_id.to_string()), KeyValue::new("table_name", table_name.to_string())]
}

/// Convenience helpers for hot-path counter increments. No-op if metrics
/// weren't initialized (tests, embedded use).
pub fn record_insert(project_id: &str, table_name: &str, rows: u64) {
    if let Some(m) = METRICS.get() {
        let attrs = ingest_attrs(project_id, table_name);
        m.ingest_inserts.add(1, &attrs);
        m.ingest_rows.add(rows, &attrs);
    }
}

pub fn record_ingest_error(project_id: &str, table_name: &str) {
    if let Some(m) = METRICS.get() {
        m.ingest_errors.add(1, &ingest_attrs(project_id, table_name));
    }
}

pub fn record_event_time_bounded(project_id: &str, table_name: &str, rows: u64) {
    if let Some(m) = METRICS.get() {
        m.ingest_event_time_bounded.add(rows, &ingest_attrs(project_id, table_name));
    }
}

pub fn record_flush(success: bool) {
    if let Some(m) = METRICS.get() {
        (if success { &m.flush_completed } else { &m.flush_failed }).add(1, &[]);
    }
}

/// Last observed size of the tantivy extracted-index disk cache. Written by
/// the reap cron, read by the `cache_disk_bytes` gauge callback — the walk it
/// comes from is far too expensive to run per scrape.
static TANTIVY_CACHE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn record_tantivy_cache_bytes(bytes: u64) {
    TANTIVY_CACHE_BYTES.store(bytes, std::sync::atomic::Ordering::Relaxed);
}

/// When this process started, as seen from inside it.
///
/// Every accrual and latency number this process reports is only readable
/// against its own age — a counter reads 0 because it was never exercised or
/// because the process is four minutes old, and those imply opposite work
/// (2026-08-23: a tantivy accrual "fix" was credited twice to what was purely
/// process age). `docker service ps` answers this from outside, but nothing
/// pairs it with the numbers themselves; `timefusion_stats` now does.
static PROCESS_START: std::sync::LazyLock<std::time::Instant> = std::sync::LazyLock::new(std::time::Instant::now);

/// Pin the process-start instant. Idempotent; call as early as possible in
/// every entry point (`main`, `bootstrap`) — the first force wins, so a late
/// first call would under-report uptime for the whole process lifetime.
pub fn mark_process_start() {
    std::sync::LazyLock::force(&PROCESS_START);
}

pub fn process_uptime_secs() -> u64 {
    PROCESS_START.elapsed().as_secs()
}

/// `(last, max)` runtime scheduling lag in ms — see `spawn_runtime_lag_sampler`.
pub fn runtime_lag_ms() -> (u64, u64) {
    (RUNTIME_LAG_LAST_MS.load(Relaxed), RUNTIME_LAG_MAX_MS.load(Relaxed))
}

/// Worst scheduling delay seen by the runtime-lag sampler this process, in ms.
static RUNTIME_LAG_MAX_MS: AtomicU64 = AtomicU64::new(0);
/// Most recent sample, so a gauge shows the CURRENT state rather than a
/// high-water mark that never comes back down.
static RUNTIME_LAG_LAST_MS: AtomicU64 = AtomicU64::new(0);

/// How late a task that asked to wake in exactly `SAMPLE_EVERY` actually woke.
///
/// This is the direct test of the "the health probe lost its runtime slice"
/// hypothesis behind the 2026-08-11 `exit 137` (probe deadline missed at CPU
/// 805%/4800% and 17.8 of 96 GiB — neither saturation nor OOM, so the cause was
/// never visible in resource metrics). Every maintenance sort, repair rewrite
/// and flush escalation runs on the same multi-thread runtime as the pgwire
/// handshake; CPU-bound work that does not yield starves it, and this is what
/// that looks like from inside.
///
/// Read it against the probe's `auth_ms` stage: lag spiking into the seconds at
/// the moment a probe misses confirms starvation and points at a dedicated
/// runtime for the listener. Lag flat while probes still miss rules it out, and
/// the search moves to the accept path.
///
/// Cost is one timer wakeup per interval — deliberately cheap enough to leave
/// on in prod, since the failure it explains only happens in prod.
pub fn spawn_runtime_lag_sampler(cancel: tokio_util::sync::CancellationToken) {
    const SAMPLE_EVERY: Duration = Duration::from_millis(500);
    // Below this, a sample is ordinary timer slack and not worth a log line.
    const NOTEWORTHY: u128 = 250;
    tokio::spawn(async move {
        loop {
            let deadline = tokio::time::Instant::now() + SAMPLE_EVERY;
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep_until(deadline) => {}
            }
            // `sleep_until` fires no EARLIER than the deadline, so the excess is
            // entirely scheduling delay: the timer expired and no worker picked
            // this task up.
            let lag_ms = tokio::time::Instant::now().saturating_duration_since(deadline).as_millis();
            RUNTIME_LAG_LAST_MS.store(lag_ms as u64, Relaxed);
            RUNTIME_LAG_MAX_MS.fetch_max(lag_ms as u64, Relaxed);
            if lag_ms >= NOTEWORTHY {
                warn!(lag_ms, "tokio runtime scheduling lag — a task that asked for {SAMPLE_EVERY:?} woke this late; the pgwire handshake shares this runtime");
            }
        }
    });
}

/// Cumulative time in a named section: `(count, total_us, max_us)`.
#[derive(Default)]
struct SectionStat {
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
}

/// Keyed by `(component, name)`. The component keeps the two kinds apart and is
/// load-bearing: a `block` row claims a worker was OCCUPIED for that long, a
/// `section` row only claims wall time, which for an `async` body includes
/// awaits that gave the worker back. Reading one as the other is how "blocked"
/// and "slow" get confused, which is the exact confusion this plan exists to
/// resolve.
static SECTION_STATS: std::sync::LazyLock<dashmap::DashMap<(&'static str, &'static str), SectionStat>> = std::sync::LazyLock::new(dashmap::DashMap::new);

fn record_section(component: &'static str, name: &'static str, elapsed: Duration) {
    let entry = SECTION_STATS.entry((component, name)).or_default();
    entry.count.fetch_add(1, Relaxed);
    entry.total_us.fetch_add(elapsed.as_micros() as u64, Relaxed);
    entry.max_us.fetch_max(elapsed.as_micros() as u64, Relaxed);
}

/// Times a section that occupies a runtime worker without yielding — a
/// `std::sync::Mutex` hold, a synchronous rebuild. **Not for `async` bodies**:
/// use [`TimedSection`] there, which makes no occupancy claim.
///
/// This is the instrument `scheduling_lag_ms` asks for and cannot supply: lag
/// says workers woke late while the host was half idle, which means blocked,
/// not busy — but not *where*. A CPU sampler is the wrong tool for the same
/// reason (a blocked worker samples as idle), and prod has none anyway since
/// the 2026-08-11 SIGSEGV crashloop. Wrap the suspects instead and let
/// `max_ms` name them.
///
/// Cost is two `Instant::now()` and one atomic triple per section entry.
pub struct BlockWatch(&'static str, std::time::Instant);

/// Wall time of a named section, awaits included. Answers "does this get slower
/// as the process ages" — hypothesis 3 — without claiming a worker was held.
pub struct TimedSection(&'static str, std::time::Instant);

macro_rules! section_timer {
    ($ty:ident, $component:literal $(, warn_ms = $warn:literal, $msg:literal)?) => {
        impl $ty {
            pub fn new(name: &'static str) -> Self {
                Self(name, std::time::Instant::now())
            }
        }
        impl Drop for $ty {
            fn drop(&mut self) {
                let elapsed = self.1.elapsed();
                record_section($component, self.0, elapsed);
                $(if elapsed.as_millis() >= $warn {
                    warn!(section = self.0, elapsed_ms = elapsed.as_millis() as u64, $msg);
                })?
            }
        }
    };
}

section_timer!(BlockWatch, "block", warn_ms = 250, "blocking section held a runtime worker — queries scheduled onto this worker waited behind it");
section_timer!(TimedSection, "section");

/// jemalloc's own arena accounting, in bytes: `(allocated, active, resident, mapped, retained)`.
///
/// `resident - allocated` is fragmentation — memory the kernel has given this
/// process that no live allocation is using. It is the one quantity left that
/// could explain the 2026-08-24 finding: query cost flat for an hour, then
/// climbing past ~1.5 h of uptime, while `journal_hold` froze,
/// `delta_snapshot_refresh` FELL, buffer pressure stayed at 0–7 % and
/// `scheduling_lag_ms` never left 0–1. None of those grow; fragmentation does,
/// and this process churns 13–26 GB every few minutes.
///
/// `None` off Linux or without `--features profiling`, where jemalloc is not
/// the allocator (prod builds with it — see the Dockerfile). Same posture as
/// the tantivy rows: absent, never faked zeros.
#[cfg(all(feature = "profiling", target_os = "linux"))]
pub fn jemalloc_bytes() -> Option<(u64, u64, u64, u64, u64)> {
    use tikv_jemalloc_ctl::{epoch, stats};
    // jemalloc's stats are cached; advancing the epoch is what refreshes them.
    // Without this every sample returns the values from process start.
    epoch::advance().ok()?;
    Some((
        stats::allocated::read().ok()? as u64,
        stats::active::read().ok()? as u64,
        stats::resident::read().ok()? as u64,
        stats::mapped::read().ok()? as u64,
        stats::retained::read().ok()? as u64,
    ))
}

#[cfg(not(all(feature = "profiling", target_os = "linux")))]
pub fn jemalloc_bytes() -> Option<(u64, u64, u64, u64, u64)> {
    None
}

/// `((component, section), count, total_us, max_us)` for every timed section
/// entered this process. Unsorted; the caller orders it.
pub fn section_stats() -> Vec<((&'static str, &'static str), u64, u64, u64)> {
    SECTION_STATS.iter().map(|e| (*e.key(), e.count.load(Relaxed), e.total_us.load(Relaxed), e.max_us.load(Relaxed))).collect()
}

/// Holds `inner` while a [`BlockWatch`] times it. Derefs to `inner`, so a
/// `MutexGuard` wrapped in one is used exactly like the guard.
pub struct Watched<T> {
    inner: T,
    _watch: BlockWatch,
}

impl<T> Watched<T> {
    pub fn new(name: &'static str, inner: T) -> Self {
        Self { inner, _watch: BlockWatch::new(name) }
    }
}

impl<T> std::ops::Deref for Watched<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for Watched<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

/// Generates the no-attribute "increment by one" recorders. Each no-ops on the
/// OTel side if metrics weren't initialized; `mirror STATIC.field` additionally
/// bumps a process-global atomic, which — unlike an OTel counter — is readable
/// back in-process by the `timefusion_stats` view and by tests.
macro_rules! recorders {
    ($($(#[$doc:meta])* $fn_name:ident => $field:ident $(mirror $stats:ident . $mirrored:ident)?),+ $(,)?) => {
        $(
            $(#[$doc])*
            pub fn $fn_name() {
                $($stats.$mirrored.fetch_add(1, Relaxed);)?
                if let Some(m) = METRICS.get() {
                    m.$field.add(1, &[]);
                }
            }
        )+
    };
}

/// Same, for recorders that add caller-supplied counts to one or more counters.
macro_rules! sum_recorders {
    ($($(#[$doc:meta])* $fn_name:ident ( $($arg:ident => $field:ident $(mirror $stats:ident . $mirrored:ident)?),+ $(,)? ));+ $(;)?) => {
        $(
            $(#[$doc])*
            pub fn $fn_name($($arg: u64),+) {
                $($($stats.$mirrored.fetch_add($arg, Relaxed);)?)+
                if let Some(m) = METRICS.get() {
                    $(m.$field.add($arg, &[]);)+
                }
            }
        )+
    };
}

recorders! {
    record_wal_corruption => wal_corruption,
    record_quarantine_redriven => quarantine_redriven,
    record_quarantine_backlog => quarantine_backlog,
    record_query => query_executions,
    record_tantivy_prefilter_attempt => tantivy_prefilter_attempts,
    record_tantivy_prefilter_used => tantivy_prefilter_used,
    record_tantivy_prefilter_skipped => tantivy_prefilter_skipped,
    record_tantivy_prefilter_error => tantivy_prefilter_errors,
    record_tantivy_build_failure => tantivy_build_failures,
    record_tantivy_recovery_deferred => tantivy_recovery_deferred,
    record_tantivy_merge_deferred => tantivy_merges_deferred,
    record_tantivy_merge_executed => tantivy_merges_executed,
    record_count_pushdown_used => count_pushdown_used,
    record_logical_count_pushdown_used => logical_count_pushdown_used,
    record_backpressure_engaged => backpressure_engaged,
    record_backpressure_rejected => backpressure_rejected,
    record_backpressure_force_flush => backpressure_force_flush,
    record_flush_stalled => flush_stalled,
    record_flush_sort_unsorted_fallback => flush_sort_unsorted_fallbacks mirror MAINTENANCE_STATS.flush_sort_unsorted_fallbacks,
    record_write_capture_skipped => write_capture_skipped,
    record_cache_confirm_timeout => cache_confirm_timeouts,
    record_cache_insert_bypassed => cache_insert_bypassed,
    /// One optimize/compaction OCC conflict (retryable). A sustained rate means the
    /// optimizer is repeatedly losing commit races to concurrent dedup/flush.
    record_optimize_conflict => optimize_conflict,
    /// One optimize/compaction run that errored or gave up after retries — that
    /// partition stays fragmented until a later run succeeds.
    record_optimize_failed => optimize_failed,
    /// One DML Delta operation OCC conflict (retried on a fresh snapshot).
    record_dml_conflict => dml_conflict mirror DML_STATS.occ_conflicts,
    record_dml_retry_success => dml_retry_success mirror DML_STATS.retry_successes,
    record_dml_retry_exhausted => dml_retry_exhausted mirror DML_STATS.retry_exhausted,
    /// One DML Delta leg skipped because its time window is entirely unflushed.
    record_dml_delta_leg_skipped => dml_delta_leg_skipped,
    /// One `UPDATE ... FROM` Delta leg deferred into the coalescer.
    record_dml_coalesce_enqueued => dml_coalesce_enqueued,
    /// One Delta merge executed by a coalescer drain.
    record_dml_coalesce_merge => dml_coalesce_merges mirror DML_STATS.coalesce_merges,
    /// One coalesced DML group parked to the quarantine dir after exhausting
    /// drain retries. Recoverable — unlike `record_dml_coalesce_dropped`.
    record_dml_coalesce_quarantined => dml_coalesce_quarantined mirror DML_STATS.coalesce_quarantined,
    /// One coalesced DML group whose rows could not be quarantined — real loss.
    record_dml_coalesce_dropped => dml_coalesce_dropped,
    /// One dedup chunk rewrite skipped (over budget or in failure backoff).
    record_dedup_chunk_skipped => dedup_chunk_skipped,
    /// One cron maintenance run that exceeded the long-running warning threshold.
    record_cron_long_running => maintenance_cron_long_running mirror MAINTENANCE_STATS.cron_long_running,
    /// One out-of-band checkpoint failure (also mirrors to OTel for alerting).
    record_checkpoint_failed => maintenance_checkpoint_failed mirror MAINTENANCE_STATS.checkpoint_failed,
    /// One checkpoint that failed post-write footer verification (mirrors to OTel).
    record_checkpoint_corrupt => maintenance_checkpoint_corrupt mirror MAINTENANCE_STATS.checkpoint_corrupt,
    /// One out-of-band log-cleanup failure (mirrors to OTel).
    record_log_cleanup_failed => maintenance_log_cleanup_failed mirror MAINTENANCE_STATS.log_cleanup_failed,
}

sum_recorders! {
    /// One pre-drain confirm pass: `attempted` files probed, `warmed` of them
    /// missing and fetched (the write-capture gap).
    record_cache_confirm(attempted => cache_confirm_attempts, warmed => cache_confirm_warmed);
    record_dedup_dropped(rows => dedup_dropped_rows);
    record_compaction_dedup_dropped(rows => compaction_dedup_dropped_rows);
    /// Record one full-optimize run's idempotence split: how many window partitions
    /// were rewritten vs skipped as unchanged (the cache-churn-avoided signal).
    record_optimize_partitions(rewritten => optimize_partitions_rewritten, skipped => optimize_partitions_skipped);
    /// `n` dangling Add entries Remove'd by the reconcile task (mirrors to OTel).
    /// Nonzero ⇒ committed data was destroyed elsewhere.
    record_dangling_removed(n => reconcile_dangling_removed mirror MAINTENANCE_STATS.dangling_removed);
}

/// One commit-path operation abandoned by its bound. `op` is a fixed set of
/// static labels (bounded cardinality by construction — never a table or
/// project id, which belong on the accompanying warn's span attributes).
/// One dashboard aggregate answered from a configured rollup.
pub fn record_rollup_hit(mode: &'static str, grain: &str) {
    let stats = maintenance_stats();
    if mode == "hybrid" { &stats.rollup_hits_hybrid } else { &stats.rollup_hits_full }.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(m) = METRICS.get() {
        m.rollup_hits.add(1, &[KeyValue::new("mode", mode), KeyValue::new("grain", grain.to_string())]);
    }
}

/// One dashboard aggregate that fell through to a raw scan.
///
/// Takes the REASON, not its label: the match below is exhaustive, so a new
/// variant fails the build instead of landing in a catch-all bucket. It
/// previously matched on the string and four of the thirteen reasons had no
/// arm, so `missing_project`, `unbounded_time`, `non_decomposable` and
/// `rewrite_schema_mismatch` were indistinguishable in prod — 29 misses that
/// could not be diagnosed without a deploy.
/// True once every `ROLLUP_MISS_SAMPLE` misses, for logging one refused plan
/// with context. Misses run at several per second on a busy node, so the
/// counter is what you alert on and this is what you diagnose with.
pub fn sample_rollup_miss() -> bool {
    const ROLLUP_MISS_SAMPLE: u64 = 512;
    static SEEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    SEEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed).is_multiple_of(ROLLUP_MISS_SAMPLE)
}

pub fn record_rollup_miss(reason: crate::rollup::MissReason) {
    use crate::rollup::MissReason as R;
    let stats = maintenance_stats();
    stats.rollup_misses_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match reason {
        R::NotBuilt => &stats.rollup_miss_not_built,
        R::StaleCoverage => &stats.rollup_miss_stale_coverage,
        R::TinyInterior => &stats.rollup_miss_tiny_interior,
        R::TooManyBranches => &stats.rollup_miss_too_many_branches,
        R::UnsupportedShape => &stats.rollup_miss_unsupported,
        R::IncompleteCoverage => &stats.rollup_miss_incomplete_coverage,
        R::UnknownFilter => &stats.rollup_miss_unknown_filter,
        R::FilterNotEligible => &stats.rollup_miss_filter_not_eligible,
        R::MissingMeasure => &stats.rollup_miss_missing_measure,
        R::PartialBucket => &stats.rollup_miss_unaligned_bucket,
        R::UnknownGroupBy => &stats.rollup_miss_unknown_group_by,
        R::MissingProject => &stats.rollup_miss_missing_project,
        R::UnboundedTime => &stats.rollup_miss_unbounded_time,
        R::NonDecomposableAggregate => &stats.rollup_miss_non_decomposable,
        R::RewriteSchemaMismatch => &stats.rollup_miss_rewrite_schema_mismatch,
        R::UnwalkableSource => &stats.rollup_miss_unwalkable_source,
    }
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(m) = METRICS.get() {
        m.rollup_misses.add(1, &[KeyValue::new("reason", reason.label())]);
    }
}

pub fn record_commit_timeout(op: &'static str) {
    if let Some(m) = METRICS.get() {
        m.commit_lock_timeouts.add(1, &[KeyValue::new("op", op)]);
    }
}

/// Declares a process-global atomic-counter struct together with its all-zero
/// static AND its `timefusion_stats` rows, so a new counter can drift from
/// neither its initializer nor the readout. On 2026-08-25 two counters were
/// incremented with no arm in `pg_compat`, so no `SELECT` would ever have shown
/// them; there is deliberately no "declared but not exposed" arm here.
/// The row key defaults to the field name — `field as "key"` pins the
/// historical key where it differs (usually a `_total` suffix).
macro_rules! atomic_stats {
    ($(#[$sm:meta])* $name:ident => $global:ident as $component:literal { $($(#[$fm:meta])* $field:ident $(as $key:literal)?),+ $(,)? }) => {
        $(#[$sm])*
        pub struct $name {
            $($(#[$fm])* pub $field: AtomicU64,)+
        }
        static $global: $name = $name { $($field: AtomicU64::new(0),)+ };
        impl $name {
            /// `(component, key, value)` for every counter, for `timefusion_stats`.
            pub fn stats_rows(&self) -> Vec<(&'static str, &'static str, u64)> {
                vec![$((
                    $component,
                    atomic_stats!(@key $field $(, $key)?),
                    self.$field.load(std::sync::atomic::Ordering::Relaxed),
                ),)+]
            }
        }
    };
    (@key $field:ident) => { stringify!($field) };
    (@key $field:ident, $key:literal) => { $key };
}

atomic_stats! {
    DmlStats => DML_STATS as "dml" {
        occ_conflicts as "occ_conflicts_total",
        retry_successes as "retry_successes_total",
        retry_exhausted as "retry_exhausted_total",
        /// Delta merges executed by coalescer drains — readable in-process (the
        /// OTel counter isn't); tests assert on deltas of this to pin folding.
        coalesce_merges,
        /// Groups parked to `<wal_dir>/quarantine/dml` — in-process readable so
        /// tests can assert the terminal branch parks instead of dropping.
        coalesce_quarantined,
    }
}

pub fn dml_stats() -> &'static DmlStats {
    &DML_STATS
}

atomic_stats! {
    /// Readable maintenance counters for the `timefusion_stats` view — the OTel
    /// counters above can't be read back in-process. Process-global const atomics
    /// (no init needed), incremented by the out-of-band checkpoint + reconcile
    /// tasks. `checkpoint_lag_versions` is the last observed max lag (a gauge), the
    /// rest are monotonic.
    #[derive(Default)]
    MaintenanceStats => MAINTENANCE_STATS as "maintenance" {
        checkpoints_created,
        checkpoint_failed,
        /// Checkpoints that wrote OK but failed post-write footer verification
        /// (the referenced object isn't a readable Parquet). Log cleanup is
        /// withheld so the JSON log stays recoverable. PAGE if > 0.
        checkpoint_corrupt,
        log_files_cleaned,
        log_cleanup_failed,
        // Max version lag (current - last checkpointed) seen at the last
        // checkpoint tick. Should stay near checkpoint_interval; a large,
        // growing value means the checkpoint task is failing or wedged.
        checkpoint_lag_versions,
        // NONZERO = committed parquet was destroyed elsewhere (2026-07-09
        // commit-path deletion bug). PAGE and investigate.
        dangling_removed,
        reconcile_failed,
        dedup_timed_out as "dedup_timed_out_total",
        dedup_failed as "dedup_failed_total",
        /// Adds a rewrite planner had to drop because the in-memory snapshot listed
        /// the same file twice. Nonzero means reads over that table double-count
        /// rows — the file list diverged from the log, which no amount of dedup or
        /// compaction repairs on its own. PAGE if > 0.
        snapshot_duplicate_adds,
        light_optimize_timed_out as "light_optimize_timed_out_total",
        light_optimize_failed as "light_optimize_failed_total",
        /// Ticks that hit the wall-clock budget with hot projects still pending.
        light_optimize_tick_truncated as "light_optimize_tick_truncated_total",
        /// Wave-engine per-tick accounting. `planned` counts projects the tick's
        /// single metadata walk found work for; `completed` counts bins that landed.
        /// ALERT when completed lags planned for N consecutive ticks — that's the
        /// "8 of 11 hot projects never reached" shape (prod 2026-07-29).
        // planned vs completed is the per-tick coverage check: a persistent
        // gap means hot projects are going uncompacted (prod 2026-07-29).
        light_optimize_projects_planned as "light_optimize_projects_planned_total",
        light_optimize_projects_completed as "light_optimize_projects_completed_total",
        light_optimize_bins_committed as "light_optimize_bins_committed_total",
        light_optimize_waves_committed as "light_optimize_waves_committed_total",
        /// GAUGE: repair bins sorting right now. A repair pass runs for up to
        /// `timefusion_footer_repair_budget_secs` and logs nothing between its
        /// per-bin events, so this is the only cheap way to tell "repair is
        /// grinding" from "repair is wedged" without SSH.
        repair_bins_in_flight,
        /// Dedup-engine waves (data_change: true) — counted separately so the
        /// light_optimize_* counters mean pure compaction only.
        dedup_bins_committed as "dedup_bins_committed_total",
        dedup_waves_committed as "dedup_waves_committed_total",
        /// Waves not STARTED because the WAL was over its emergency-flush threshold
        /// (durability outranks compaction) or memory was near the cgroup limit.
        /// Chronic nonzero = compaction is being starved, not protected.
        light_optimize_wal_yields as "light_optimize_wal_yields_total",
        /// Ticks/waves stopped because at least one MemBuffer bucket exceeded its
        /// retention target without landing. Unlike the byte-based WAL brake this
        /// catches small but old persistence debt.
        light_optimize_flush_debt_yields as "light_optimize_flush_debt_yields_total",
        light_optimize_memory_brakes as "light_optimize_memory_brakes_total",
        /// Scans on a `version_append` table where the Delta leg did NOT already
        /// satisfy keep-greatest's ordering, so a `SortExec` was injected over it.
        ///
        /// Zero is the healthy state and the PRECONDITION for turning
        /// `version_append` on for a busy table: the sort is per-partition and
        /// spillable, but prod's `otel_logs_and_spans` scans read 48 file groups
        /// (2026-08-01), and 48 concurrent sorts over a measured 145MB peak batch is
        /// the 2026-07-20 wide-scan OOM shape. Nonzero here means the partition
        /// carries files without an honest sorted footer — today that is the DML
        /// rewrite path (`dml_writer_properties` passes `declare_sorted=false`),
        /// which merge-on-read removes by construction.
        mor_delta_leg_sorts as "mor_delta_leg_sorts_total",
        /// Escalated flush sorts that FAILED and wrote their group unsorted. One
        /// unsorted file disables the reader's footer ordering for every scan
        /// touching its partition (query-time SortExec, unordered MOR dedup), so
        /// any nonzero here is a read-path incident in the making (2026-08-03).
        flush_sort_unsorted_fallbacks as "flush_sort_unsorted_fallbacks_total",
        /// Rounds where the WAL-backlog brake DEGRADED the wave to the one-project
        /// service floor (instead of stopping the tick). Chronic nonzero = ingest is
        /// outrunning flush often enough that compaction is running at the floor.
        light_optimize_ticks_degraded as "light_optimize_ticks_degraded_total",
        /// Repair ticks skipped because ANOTHER table's repair pass already held the
        /// process-wide permit. The light pool is shared across tables while the
        /// wave engine's concurrency cap is per-table, so before this guard two
        /// repair sorts could co-exist and starve each other (prod 2026-08-11 11:30,
        /// `otel_metrics` vs `otel_logs_and_spans`, a 981 MB bin lost with 2.1 MB
        /// left of 15.4 GB). Chronic nonzero = repair is contended, not broken; a
        /// permanently-zero repair backlog on one table while this climbs means the
        /// other table is monopolising the permit.
        repair_ticks_yielded,
        /// Dashboard aggregates served from a rollup, split by how much of the
        /// window the rollup owned. The OTel counters carry the same numbers but
        /// cannot be read back in-process, and these two are the only signal that
        /// says whether read routing is actually firing — `rollup_hits_hybrid`
        /// specifically is the one that proves the raw-fringe union works, since a
        /// full-window hit needs no union at all.
        rollup_hits_full as "rollup_hits_full_total",
        rollup_hits_hybrid as "rollup_hits_hybrid_total",
        /// Partitions rebuilt from only the hours that changed, vs from scratch.
        /// The ratio is the whole point of the dirty-hour tracking: a fall to zero
        /// means something is widening the dirty set to the whole day and every
        /// enrichment is paying for 24 hours of re-aggregation again.
        rollup_rebuilds_incremental as "rollup_rebuilds_incremental_total",
        rollup_rebuilds_full as "rollup_rebuilds_full_total",
        rollup_dirty_partitions,
        /// Derived slices completed WITHOUT publishing because a strictly wider live
        /// file already covered them. Expected to be rare; if it is not, a late row
        /// inside an already-published day may be going stale in the coarse tier.
        rollup_skipped_covered_by_wider,
        /// Splits refused because the unit measured nearly what its parent measured:
        /// bisection has hit the row-group floor and halving the width again buys
        /// nothing but journal units.
        ///
        /// This is the instrument for the 2026-08-22 shred — 3,455 units for a
        /// single (project, tier, day). Read it against `pending_base_rollup`: this
        /// rising while pending stops growing is the fix working. This rising while
        /// pending ALSO rises means units are being declined and then failing to
        /// run, which is worse than the shred, not better.
        split_declined_at_floor,
        /// Dedup keys whose versions DISAGREE on a column declared immutable.
        ///
        /// Immutability is enforced for UPDATE only, so an INSERT can append a
        /// disagreeing version; read filters on immutable columns are pushed below
        /// the dedup on the strength of that declaration. Non-zero means the read
        /// path's premise is false in production and a pushed predicate can match a
        /// version the winner does not satisfy.
        ///
        /// The audit runs unconditionally, so zero here means CLEAN rather than
        /// "not measured" — the one reading a flag-gated version of this counter
        /// could never give.
        immutable_column_disagreement_total,
        /// Partitions where the coverage ledger and the Delta tags disagree.
        ///
        /// The ledger is destined to be the authority, and an authority can DRIFT
        /// where self-describing files cannot — that is the one risk the design
        /// adds. This is the standing alarm against it, and it is why the tag replay
        /// stays after reads move onto the ledger rather than being deleted with the
        /// tags it reads.
        ///
        /// Must be zero before any read path trusts the ledger. Non-zero afterwards
        /// means queries may be answered from coverage that is not there.
        coverage_ledger_disagreements,
        /// Ledger writes that did not reach disk.
        ///
        /// `store_sidecar` warns and continues, which is right for a hint and wrong
        /// for an authority: the in-memory ledger goes on serving what it holds while
        /// the durable copy falls behind, and nothing else would say so. Understating
        /// coverage is the safe direction — it costs a rebuild, not a wrong answer —
        /// so this is not fatal while the Delta tags remain. It must read ZERO
        /// alongside `coverage_ledger_disagreements` before the tags can go.
        coverage_ledger_persist_failures,
        /// Base rollup files carrying no parseable slice tags — history written
        /// before tagging existed.
        ///
        /// Until #169 such a file was DROPPED, so it was invisible to the coarse
        /// tier forever while the unit published rows=0 and completed,
        /// indistinguishable from a genuinely empty slice. That is what this counter
        /// was added to expose, and it did: 15 hits in 20 minutes against 16 of 16
        /// derived publications at rows=0.
        ///
        /// It now counts files SELECTED by the fallback — pruned on their own
        /// timestamp statistics instead of discarded — so it measures how much of the
        /// base tier predates tagging, not how much is unreachable. Expect it to
        /// shrink as those partitions are rewritten; a rise means older history is
        /// being reached, which is the point.
        rollup_untagged_inputs,
        /// Untagged files found LIVE IN A TIER at publish time (gauge, overwritten
        /// per unit), and the running total this publish path has retired.
        ///
        /// A tier file with no identity tags used to be immortal — the replace-set
        /// skipped it, so every rebuild stacked another version of every `id` beside
        /// it. That ran for a MONTH unnoticed (352 files, 26 days, 7.17 versions per
        /// id) purely because nothing counted it; it was found by hand while chasing
        /// an inflated dashboard. After `slice_retires` the steady state is genuinely
        /// zero, so `found` is alarmable at > 0: nonzero means some path is writing
        /// or stripping tier tags again and should be named before it costs another
        /// archaeology session.
        ///
        /// `retired` is how a repair is watched draining — the only proof a rebuild
        /// REMOVED the old file rather than publishing a correct one beside it,
        /// which is exactly what the first repair attempt did.
        rollup_tier_untagged_found,
        rollup_tier_untagged_retired as "rollup_tier_untagged_retired_total",
        /// Recovered slices carrying NO row witness — published before
        /// `TAG_SOURCE_ROWS` existed. Every read refuses them `stale_coverage` and no
        /// rule can ever rescue them, so this is the size of the backlog that has to
        /// republish before wide dashboards route. Set from the whole recovery pass,
        /// hourly, so it reads 0 only when there genuinely are none.
        // The republish backlog that gates wide-window routing. Watch it fall;
        // `rollup_stale_no_witness` per query falls with it.
        rollup_witnessless_slices,
        /// Contiguous sealed days of rollup coverage, counting back from yesterday,
        /// minimised over every (project, declared tier).
        ///
        /// This is the number that governs long-window query latency, and no
        /// existing metric tracked it. `MIN(date)` reads as progress while the
        /// middle stays holey — it advanced 08-01 -> 07-30 on 2026-08-17 while the
        /// coarse tier held only 3 days — and a 30d panel needs 30 CONTIGUOUS days
        /// in the tier it reads, so one hole anywhere in the window sends it to a
        /// raw scan. Minimised, not averaged: a single uncovered project is a
        /// customer whose dashboard is slow.
        rollup_min_contiguous_days,
        rollup_median_contiguous_days,
        rollup_oldest_invalidation_age_secs as "rollup_oldest_invalidation_age_seconds",
        rollup_scan_cohorts as "rollup_scan_cohorts_total",
        rollup_scan_projects as "rollup_scan_projects_total",
        rollup_scan_estimated_bytes as "rollup_scan_estimated_bytes_total",
        rollup_cohort_splits as "rollup_cohort_splits_total",
        rollup_singleton_failures as "rollup_singleton_failures_total",
        rollup_staged_projects as "rollup_staged_projects_total",
        rollup_shared_commits as "rollup_shared_commits_total",
        rollup_commit_actions as "rollup_commit_actions_total",
        rollup_occ_retries as "rollup_occ_retries_total",
        rollup_ambiguous_landings as "rollup_ambiguous_landings_total",
        rollup_scan_duration_ms as "rollup_scan_duration_ms_total",
        rollup_staging_duration_ms as "rollup_staging_duration_ms_total",
        rollup_commit_duration_ms as "rollup_commit_duration_ms_total",
        rollup_end_to_end_duration_ms as "rollup_end_to_end_duration_ms_total",
        rollup_output_rows as "rollup_output_rows_total",
        rollup_output_files as "rollup_output_files_total",
        /// Live parquet files the Tantivy manifest does NOT cover, as of the last
        /// reconcile pass, plus the ones skipped for exceeding
        /// TIMEFUSION_TANTIVY_BACKFILL_MAX_FILE_MB. Gauges, not counters: each pass
        /// overwrites them.
        ///
        /// Without these there is no way to tell whether a reindex is converging or
        /// how far it has left to run, which is precisely why the reindex was being
        /// driven by hand from sibling containers — three of which were OOM-killed
        /// on 2026-08-16. `uncovered` trending to 0 IS the definition of done.
        tantivy_uncovered_files,
        tantivy_oversized_skipped,
        /// Pending (non-Complete) tasks split by operation, and the subset that is
        /// ELIGIBLE right now (deadline passed). Gauges, republished each checkpoint.
        ///
        /// `tasks_pending` alone cannot answer the only question that matters when
        /// coverage stalls: is the rollup work absent, present-but-not-eligible, or
        /// present-and-eligible but out-competed? Prod 2026-08-17 sat at ~128k
        /// pending with rollup coverage frozen for hours, and there was no way to
        /// tell which of those three it was without guessing.
        pending_dedup,
        pending_base_rollup,
        pending_derived_rollup,
        pending_hot_packing,
        pending_sealed_consolidation,
        pending_repair,
        eligible_base_rollup,
        eligible_sealed_total,
        rollup_full_hours_rebuilt as "rollup_full_hours_rebuilt_total",
        rollup_incremental_hours_rebuilt as "rollup_incremental_hours_rebuilt_total",
        maintenance_tasks_pending as "tasks_pending",
        maintenance_tasks_running as "tasks_running",
        maintenance_tasks_retry as "tasks_retry",
        maintenance_tasks_complete as "tasks_complete",
        maintenance_backlog_bytes as "backlog_bytes",
        /// Oldest age over work the scheduler still INTENDS to do — tasks whose
        /// slice ended within `STARVATION_HORIZON_MICROS` — so it is bounded by 31
        /// days and a reading near the bound is a real stall inside the goal window.
        ///
        /// `beyond_horizon_tasks` is the deliberately-abandoned remainder. It is not
        /// optional company: without it, narrowing the age gauge is
        /// indistinguishable from hiding the debt.
        maintenance_oldest_task_age_secs as "oldest_task_age_seconds",
        maintenance_beyond_horizon_tasks as "beyond_horizon_tasks",
        maintenance_eligible_watermark_lag_secs as "eligible_watermark_lag_seconds",
        maintenance_processed_bytes as "processed_bytes_total",
        maintenance_processed_bytes_per_sec as "processed_bytes_per_second",
        maintenance_raw_tail_duration_secs as "raw_tail_duration_seconds",
        sealed_compaction_debt_bytes,
        maintenance_cpu_tokens_used as "cpu_tokens_used",
        maintenance_decoded_bytes_used as "decoded_bytes_used",
        maintenance_object_read_tokens_used as "object_read_tokens_used",
        maintenance_object_write_tokens_used as "object_write_tokens_used",
        /// Aggregates that fell through to a raw scan, plus the breakdown by reason.
        ///
        /// The OTel counter carries the same labels but cannot be read back
        /// in-process, and the reason is the ONLY thing that distinguishes "the
        /// build never ran" from "it ran and the source moved under it" from "the
        /// shape is unsupported" — which is the entire diagnosis of a rollout that
        /// is building rollups but not serving them. Without it the answer is
        /// guesswork over a 19k-line log.
        rollup_misses_total,
        rollup_miss_not_built as "rollup_miss_not_built_total",
        rollup_miss_stale_coverage as "rollup_miss_stale_coverage_total",
        rollup_miss_tiny_interior as "rollup_miss_tiny_interior_total",
        rollup_miss_too_many_branches as "rollup_miss_too_many_branches_total",
        rollup_miss_unsupported as "rollup_miss_unsupported_total",
        rollup_miss_incomplete_coverage as "rollup_miss_incomplete_coverage_total",
        rollup_miss_unknown_filter as "rollup_miss_unknown_filter_total",
        rollup_miss_filter_not_eligible as "rollup_miss_filter_not_eligible_total",
        rollup_miss_missing_measure as "rollup_miss_missing_measure_total",
        rollup_miss_unaligned_bucket as "rollup_miss_unaligned_bucket_total",
        rollup_miss_unknown_group_by as "rollup_miss_unknown_group_by_total",
        rollup_miss_missing_project as "rollup_miss_missing_project_total",
        rollup_miss_unbounded_time as "rollup_miss_unbounded_time_total",
        rollup_miss_non_decomposable as "rollup_miss_non_decomposable_total",
        rollup_miss_rewrite_schema_mismatch as "rollup_miss_rewrite_schema_mismatch_total",
        rollup_miss_unwalkable_source as "rollup_miss_unwalkable_source_total",
        dirty_bin_queue_depth,
        dirty_bin_enqueued as "dirty_bin_enqueued_total",
        dirty_bin_eligible as "dirty_bin_eligible_total",
        dirty_bin_processed as "dirty_bin_processed_total",
        dirty_bin_requeued as "dirty_bin_requeued_total",
        /// Queued bins consumed by the whole-date BATCH probe without per-bin
        /// staging (every flushed bin is enqueued, so in prod ~97% of queued bins
        /// carry no duplicates at all). Also counted in `dirty_bin_processed`.
        dirty_bin_batch_probe_clean as "dirty_bin_batch_probe_clean_total",
        dirty_bin_dropped_rows as "dirty_bin_dropped_rows_total",
        dirty_bin_rewrite_duration_ms as "dirty_bin_rewrite_duration_ms_total",
        /// Cold-owned dirty bins (date old enough that the nightly consolidate owns
        /// the partition) DEPRIORITIZED to the tail of a drain pass and left on the
        /// queue. They are NOT dropped: consolidate bin-packs but does not collapse
        /// duplicates, so the dirty-bin drain stays their only physical dedup.
        /// NOTE `cold_optimize_after_days` defaults to 1 and the drain already skips
        /// today, so in the default configuration EVERY drainable bin is cold-owned
        /// — this then reads as "queued bins this pass had no batch slot for".
        /// Chronic growth = a backlog the batch size can't keep up with.
        dedup_bins_deferred_cold as "dedup_bins_deferred_cold_total",
        /// Drain passes skipped (or cut short between chunks) because the flush path
        /// was behind. Dedup is an optimization — read-side DedupExec keeps results
        /// correct — so it yields to persistence (2026-07-30: a boot drain over a
        /// 10-day backlog pinned the commit path and starved flush for a whole
        /// container life). Chronic nonzero = flush is unhealthy, not dedup.
        dedup_passes_flush_yields as "dedup_passes_flush_yields_total",
        /// Per-bin STAGING attempts killed at the deadline and requeued. The
        /// deadline exists so one hung object-store read can't wedge the drain
        /// for hours behind the 1-permit maintenance semaphore (prod 2026-08-05:
        /// 6.5h stall, skips=77). Repeated hits are the same bin retrying —
        /// an oversized bin that can't finish inside the deadline, not noise.
        dedup_bin_stage_timeouts as "dedup_bin_stage_timeouts_total",
        /// Wave (dedup / light-optimize) commits that STOOD DOWN rather than queue
        /// on a per-table commit lock a flush was already waiting for — the flush
        /// starvation of prod 2026-07-30, where durability waited >600s behind
        /// legally-slow maintenance commits. The wave's bins are requeued and
        /// re-staged later, so this is deferred work, not lost work. Chronic nonzero
        /// = flush is saturating the commit path and compaction is being crowded out.
        wave_commits_yielded_to_flush as "wave_commits_yielded_to_flush_total",
        /// Boot-time resume of a staged-but-uncommitted footer-repair bin: the
        /// rewrite survived the restart that killed its process, so the next pass
        /// doesn't redo the (40+ minute) work. See `resume_staged_intents`.
        repair_resumed as "repair_resumed_total",
        /// Rollup units COMMITTED at claim time from output a previous process
        /// staged, instead of re-running their ~21-minute scan. Read against
        /// `rollup_resume_declined`: the ratio is what says whether resume is
        /// rescuing work or whether the source keeps moving underneath it.
        rollup_resumed as "rollup_resumed_total",
        /// Staged rollup outputs refused — the source moved, an input left the
        /// snapshot, another live file already covers the slice, or the parquet is
        /// short. Every one of these is a correct refusal; a rising count means the
        /// staging window and the churn window overlap, not that resume is broken.
        rollup_resume_declined as "rollup_resume_declined_total",
        /// Resume declined: an input file was rewritten underneath the staged
        /// output, so committing it would resurrect removed rows.
        repair_resume_declined_stale as "repair_resume_declined_stale_total",
        /// Resume declined: a staged output object is missing or the wrong size —
        /// the process died mid-PUT.
        repair_resume_declined_incomplete as "repair_resume_declined_incomplete_total",
        /// Resume declined because output rows != input rows. A repair is
        /// row-preserving by construction, so this must be ZERO forever; nonzero
        /// means a truncated staging that would have DROPPED rows, or a broken
        /// assumption. PAGE if > 0.
        // MUST stay 0 — nonzero = a staged repair whose rows didn't add up.
        repair_resume_row_mismatch as "repair_resume_row_mismatch_total",
        /// Cron ticks skipped because the previous run of the same job was still
        /// in flight. A steadily growing value = a wedged/overlong job body.
        cron_ticks_skipped,
        /// Cron fires actually dispatched (all jobs). Frozen while uptime grows =
        /// the scheduler is dead (2026-07-14 outage signature).
        // Fired frozen while uptime grows = scheduler dead (2026-07-14
        // outage); skipped growing = a job body is wedged or overlong.
        cron_ticks_fired,
        /// Cron runs that exceeded the long-running warning threshold. Slow but
        /// progressing work is allowed to finish; this is for observability.
        // Runs exceeding the long-running warning threshold. Slow progress
        // is allowed; sustained nonzero with no completion = wedged.
        cron_long_running as "cron_long_running_total",
    }
}

pub fn maintenance_stats() -> &'static MaintenanceStats {
    &MAINTENANCE_STATS
}

/// Resident set size of this process in bytes from `/proc/self/statm`
/// (Linux only; None elsewhere). Compare against the MemBuffer's
/// `estimate_batch_size` charge: a large RSS-below-estimate gap means the
/// per-bucket estimate (`get_array_memory_size` on wide Utf8View / replayed
/// batches) is over-counting, so backpressure is tripping on phantom bytes
/// rather than real memory.
pub fn process_rss_bytes() -> Option<usize> {
    // statm fields are in pages; resident is field 2. 4 KiB pages on every
    // Linux target TF deploys to (x86_64) — avoids a libc dependency.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    statm.split_whitespace().nth(1)?.parse::<usize>().ok().map(|pages| pages * 4096)
}

#[cfg(test)]
mod runtime_lag_tests {
    use std::sync::atomic::Ordering::Relaxed;

    use super::{RUNTIME_LAG_LAST_MS, RUNTIME_LAG_MAX_MS, spawn_runtime_lag_sampler};

    /// The sampler must report near-zero on an idle runtime and must stop on
    /// cancel. A sampler that reported lag when nothing was competing would
    /// make the starvation signal unreadable — which is the whole point of it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_runtime_reports_no_meaningful_lag_and_stops_on_cancel() {
        RUNTIME_LAG_MAX_MS.store(0, Relaxed);
        let cancel = tokio_util::sync::CancellationToken::new();
        spawn_runtime_lag_sampler(cancel.clone());
        tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
        let idle_max = RUNTIME_LAG_MAX_MS.load(Relaxed);
        assert!(idle_max < 250, "idle runtime must not look starved, got {idle_max}ms");

        cancel.cancel();
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        RUNTIME_LAG_LAST_MS.store(u64::MAX, Relaxed);
        tokio::time::sleep(std::time::Duration::from_millis(700)).await;
        assert_eq!(RUNTIME_LAG_LAST_MS.load(Relaxed), u64::MAX, "a cancelled sampler must stop writing samples");
    }
}

// ===== telemetry =====
use anyhow::Context;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::{
    logs::SdkLoggerProvider,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler},
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::SubscriberExt, util::SubscriberInitExt};

/// Kept for `shutdown_telemetry` to flush buffered log batches at exit.
static LOGGER_PROVIDER: OnceLock<SdkLoggerProvider> = OnceLock::new();

/// Max spans/logs per OTLP export message. TF's spans/logs embed full query
/// text, so the SDK default (512) overflowed the collector's 4MB gRPC limit
/// (messages up to 39MB → every export failed). 32 keeps a typical message
/// ~2-3MB. See init_telemetry.
const EXPORT_BATCH: usize = 32;
const EXPORT_TIMEOUT: Duration = Duration::from_secs(10);

pub fn init_telemetry(config: &TelemetryConfig) -> anyhow::Result<()> {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_endpoint = &config.otel_exporter_otlp_endpoint;
    info!("Initializing OpenTelemetry with OTLP endpoint: {}", otlp_endpoint);

    let service_name = &config.otel_service_name;
    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", service_name.clone()), KeyValue::new("service.version", config.otel_service_version.clone())])
        .build();

    // Span export honors the standard OTEL_TRACES_EXPORTER=none switch. When on
    // (prod has OTEL_TRACES_EXPORTER=otlp), TF's spans carry full query text +
    // attributes, so the DEFAULT batch of 512 produced export messages up to
    // 39MB — far over the collector's 4MB gRPC receive limit — and every export
    // failed ("resource exhausted"), silently losing TF's self-observability.
    // opentelemetry-otlp 0.31 can't raise the message-size limit via the public
    // API, so we cap the batch instead: EXPORT_BATCH keeps a typical message
    // well under 4MB (≈76KB/span observed → ~2.4MB at 32). A single span larger
    // than 4MB still can't be sent, but those are rare vs the batch-size overflow.
    let telemetry_layer = if config.otel_traces_exporter.as_deref() == Some("none") {
        None
    } else {
        use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor};
        let span_exporter = opentelemetry_otlp::SpanExporter::builder().with_tonic().with_endpoint(otlp_endpoint).with_timeout(EXPORT_TIMEOUT).build()?;
        let span_processor = BatchSpanProcessor::builder(span_exporter)
            .with_batch_config(BatchConfigBuilder::default().with_max_export_batch_size(EXPORT_BATCH).build())
            .build();
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_span_processor(span_processor)
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        Some(OpenTelemetryLayer::new(tracer_provider.tracer("timefusion")))
    };

    // OTLP logs: bridge tracing events to the collector so TF shows up as a
    // service in monoscope (the 2026-06-11 OOM loop was diagnosed entirely
    // from client-side error strings because TF only logged to stdout).
    // The bridge must not observe the exporter's own tracing output —
    // tonic/hyper events inside an export would recurse into another export.
    let log_exporter = opentelemetry_otlp::LogExporter::builder().with_tonic().with_endpoint(otlp_endpoint).with_timeout(EXPORT_TIMEOUT).build()?;
    // Slow-statement logs also carry full SQL text, so cap the log batch too
    // (same 4MB-limit reasoning as spans above).
    let log_processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter)
        .with_batch_config(opentelemetry_sdk::logs::BatchConfigBuilder::default().with_max_export_batch_size(EXPORT_BATCH).build())
        .build();
    let logger_provider = SdkLoggerProvider::builder().with_log_processor(log_processor).with_resource(resource).build();
    let log_bridge = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider)
        .with_filter(tracing_subscriber::filter::filter_fn(|meta| !["opentelemetry", "tonic", "h2", "hyper"].iter().any(|p| meta.target().starts_with(p))));
    let _ = LOGGER_PROVIDER.set(logger_provider);

    // Tantivy emits an INFO event for every segment operation; recovery bursts
    // otherwise flood stdout and OTLP with merge/GC internals.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tantivy=warn"));

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true).with_thread_ids(true).with_thread_names(true);
    let fmt_layer = if config.is_json_logging() { fmt_layer.json().boxed() } else { fmt_layer.boxed() };

    Registry::default().with(env_filter).with(telemetry_layer).with(log_bridge).with(fmt_layer).try_init().context("failed to set tracing subscriber")?;

    info!("OpenTelemetry initialized successfully with service name: {}", service_name);

    Ok(())
}

pub fn shutdown_telemetry() {
    info!("Shutting down OpenTelemetry");
    // Tracer/meter providers shut down when dropped; flush buffered logs
    // explicitly so the final shutdown lines reach the collector.
    if let Some(p) = LOGGER_PROVIDER.get() {
        let _ = p.shutdown();
    }
}

/// Cell-capped preview formatter for datafusion-tracing spans, replacing the
/// crate's `default_preview_fn` (comfy_table over WHOLE cell values). Cells
/// here are unbounded — Variant/JSON bodies on SELECTs, and on an
/// INSERT…unnest input node ONE cell holds an entire bind array — so the
/// default burned 86–93% CPU and drove the 85GiB OOM loop of 2026-07-06.
/// The capped writer aborts each cell's `Display` after `PREVIEW_CELL_CAP`
/// bytes, so oversized values are never materialized, only their prefix.
pub fn capped_preview_fn(batch: &arrow::record_batch::RecordBatch) -> Result<String, arrow::error::ArrowError> {
    use std::fmt::Write;

    use arrow::util::display::{ArrayFormatter, FormatOptions};

    const PREVIEW_CELL_CAP: usize = 256;

    /// `fmt::Write` that stops accepting bytes after `left` is exhausted; the
    /// resulting `fmt::Error` aborts the value's `Display` mid-render.
    struct Capped<'a> {
        buf: &'a mut String,
        left: usize,
    }
    impl std::fmt::Write for Capped<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let take = (0..=s.len().min(self.left)).rev().find(|&i| s.is_char_boundary(i)).unwrap_or(0);
            self.buf.push_str(&s[..take]);
            self.left -= take;
            if take < s.len() { Err(std::fmt::Error) } else { Ok(()) }
        }
    }

    let opts = FormatOptions::default();
    let schema = batch.schema();
    let formatters = batch.columns().iter().map(|c| ArrayFormatter::try_new(c.as_ref(), &opts)).collect::<Result<Vec<_>, _>>()?;
    // Imperative: every cell writes through `Capped`, which borrows `out` mutably.
    let mut out = String::new();
    for row in 0..batch.num_rows() {
        for (formatter, field) in formatters.iter().zip(schema.fields()) {
            out.push_str(field.name());
            out.push('=');
            let mut w = Capped { buf: &mut out, left: PREVIEW_CELL_CAP };
            if write!(w, "{}", formatter.value(row)).is_err() {
                out.push('…');
            }
            out.push_str("  ");
        }
        out.push('\n');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ListBuilder, StringArray, StringBuilder},
        record_batch::RecordBatch,
    };

    use super::*;

    /// Regression guard for the 2026-07-06 OOM: a cell holding a huge value
    /// (like an INSERT…unnest bind array) must preview as a bounded prefix,
    /// not render in full.
    #[test]
    fn capped_preview_bounds_giant_cells() {
        let mut list = ListBuilder::new(StringBuilder::new());
        let cell = "x".repeat(100);
        (0..10_000).for_each(|_| list.values().append_value(&cell));
        list.append(true);
        let names = StringArray::from(vec!["row1"]);
        let batch = RecordBatch::try_from_iter([
            ("name", Arc::new(names) as arrow::array::ArrayRef),
            ("bind_array", Arc::new(list.finish()) as arrow::array::ArrayRef),
        ])
        .unwrap();

        let out = capped_preview_fn(&batch).unwrap();
        assert!(out.len() < 1024, "1MB cell must not render in full, got {} bytes", out.len());
        assert!(out.contains('…'), "oversized cell must be marked truncated");
        assert!(out.contains("name=row1"), "small cells render whole");
    }
}

// ===== profiling =====
// Production heap + CPU profiling, compiled only under `--features profiling`
// (Linux-only deps; a default or macOS build sees an empty module).
//
// Why baked-in rather than attached at runtime: the CapRover host is
// strictly read-only for us (no `perf`, no `MALLOC_CONF` env change, no
// `exec`/signal into the container). So the binary self-instruments and
// writes artifacts into the data-dir volume, which we CAN read off the host
// (`/var/lib/docker/volumes/…/_data/timefusion/profiles`).
//
// Heap: jemalloc's own profiler (`prof:true`), configured via the baked
// `malloc_conf` symbol in `main.rs`, auto-dumps a `.heap` every
// `lg_prof_interval` bytes allocated — so as RSS climbs toward the 89GB
// cgroup kill, the last dumps before each OOM show the allocation call
// stacks. Analyze off-host with `jeprof --svg <binary> jeprof.*.heap`.
//
// CPU: a `pprof` sampling profiler writes a rolling flamegraph SVG every
// `interval`, capturing what's hot while memory grows.

#[cfg(all(feature = "profiling", target_os = "linux"))]
mod imp {
    use std::{path::PathBuf, time::Duration};

    use tracing::{info, warn};

    /// Start background profiling. Heap profiling is already active via the
    /// baked `malloc_conf`; here we (1) ensure the artifact dir exists and
    /// (2) spawn the rolling CPU flamegraph sampler. Safe to call once at boot.
    pub fn start(data_dir: PathBuf) {
        // MUST equal the parent of the baked jemalloc `prof_prefix` in main.rs
        // so the heap dumps land in a dir we create — jemalloc does NOT mkdir
        // its prefix, and the earlier doubled `timefusion/timefusion/profiles`
        // meant the prefix dir never existed and every .heap silently failed.
        let dir = data_dir.join("profiles");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("profiling: cannot create {dir:?}: {e} — CPU flamegraphs disabled, heap dumps still land at malloc_conf prof_prefix");
        }
        archive_prekill_dumps(&dir);
        // The CPU sampler is the one part of boot that can only be removed by a
        // REBUILD, and it is signal-handler + libunwind code — the classic shape
        // for a SIGSEGV with no Rust panic. On 2026-08-11 prod crashlooped
        // (exit 139) with `starting cpu profiler` as the last line of every
        // attempt, and there was no way to test the hypothesis without shipping
        // a new image into an outage. Off by env, not by rebuild.
        //
        // Heap profiling is unaffected: it is jemalloc's own, configured by the
        // baked `malloc_conf`, so the dumps that attribute an OOM still land.
        // Heap-dump pruning must NOT ride on the CPU sampler. jemalloc dumps a
        // .heap every ~8GiB allocated and never prunes them — left alone they
        // reached 95GB / 42k files in prod — and the pruning used to live inside
        // the sampler loop, which the next line can skip entirely. Prod runs
        // exactly that way (`TIMEFUSION_CPU_PROFILE=false` since the 2026-08-11
        // crashloop), so the guard was absent in the one configuration where it
        // matters: `prof_active` is flipped on to attribute an OOM, and the
        // dumps then grow unpruned on a volume that is already the WAL's.
        spawn_heap_pruner(dir.clone());
        if std::env::var("TIMEFUSION_CPU_PROFILE").is_ok_and(|v| v.eq_ignore_ascii_case("false") || v == "0") {
            info!("profiling: jemalloc heap auto-dump only — CPU sampler disabled by TIMEFUSION_CPU_PROFILE → {dir:?}");
            return;
        }
        info!("profiling: enabled (jemalloc heap auto-dump + rolling CPU flamegraph) → {dir:?}");
        spawn_cpu_sampler(dir);
    }

    /// Cap the jemalloc heap dumps, independently of whether the CPU sampler
    /// runs.
    ///
    /// jemalloc writes a `.heap` every ~8GiB allocated (`lg_prof_interval`) once
    /// `prof_active` is on, and never removes one. This is deliberately its own
    /// thread rather than a step in the CPU sampler: the sampler is disabled in
    /// prod, so pruning attached to it does nothing in the exact configuration
    /// where heap dumps are being produced on purpose.
    fn spawn_heap_pruner(dir: PathBuf) {
        const KEEP_HEAP: usize = 50;
        const EVERY: Duration = Duration::from_secs(60);
        std::thread::Builder::new()
            .name("heap-pruner".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(EVERY);
                    prune_old(&dir, "jeprof", KEEP_HEAP);
                }
            })
            .expect("spawn heap-pruner thread");
    }

    /// One CPU profile window at a time on a dedicated OS thread: build a
    /// guard, sample for `WINDOW`, write a flamegraph, drop, repeat. A fresh
    /// guard per window keeps each SVG scoped to a recent interval (so the
    /// window overlapping an OOM isn't diluted by minutes of prior samples).
    fn spawn_cpu_sampler(dir: PathBuf) {
        const HZ: i32 = 99; // 99Hz: cheap, avoids lock-step with periodic timers
        const WINDOW: Duration = Duration::from_secs(60);
        const KEEP_CPU: usize = 10;
        std::thread::Builder::new()
            .name("cpu-profiler".into())
            .spawn(move || {
                let mut seq: u64 = 0; // `mut`: advances per completed window only, so it can't be an iterator counter
                loop {
                    let Ok(guard) = pprof::ProfilerGuardBuilder::default()
                        .frequency(HZ)
                        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                        .build()
                        .inspect_err(|e| warn!("profiling: cpu guard build failed: {e} — retrying in {WINDOW:?}"))
                    else {
                        std::thread::sleep(WINDOW);
                        continue;
                    };
                    std::thread::sleep(WINDOW);
                    let Ok(report) = guard.report().build().inspect_err(|e| warn!("profiling: cpu report build failed: {e}")) else {
                        continue;
                    };
                    let path = dir.join(format!("cpu-{seq:06}.svg"));
                    if let Err(e) = write_flamegraph(&path, &report) {
                        warn!("profiling: writing cpu flamegraph {path:?} failed: {e}");
                    }
                    // Rolling windows: keep the ones straddling an OOM without
                    // unbounded growth. jemalloc auto-dumps a .heap every ~8GiB
                    // allocated (lg_prof_interval:33) and never prunes them —
                    // left alone they grow unbounded (95GB / 42k files in prod).
                    prune_old(&dir, "cpu-", KEEP_CPU);
                    seq += 1;
                }
            })
            .expect("spawn cpu-profiler thread");
    }

    /// Preserve the PREVIOUS process's final heap dumps before this process's
    /// pruner evicts them. At prod churn (~4 dumps/min) the rolling KEEP_HEAP
    /// window is ~12 minutes, so an OOM-killed process's last dumps — the only
    /// attribution evidence for the kill — were gone before anyone could look
    /// (2026-08-03, twice). Boot moves the newest few into `prekill-<pid-seq>/`;
    /// only the 3 newest archives are kept.
    fn archive_prekill_dumps(dir: &std::path::Path) {
        let mut dumps: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with("jeprof") && n.ends_with(".heap")))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        if dumps.is_empty() {
            return;
        }
        dumps.sort_unstable_by_key(|(mtime, _)| std::cmp::Reverse(*mtime));
        let stamp = dumps[0].0.duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let arch = dir.join(format!("prekill-{stamp}"));
        if std::fs::create_dir_all(&arch).is_err() {
            return;
        }
        for (_, p) in dumps.iter().take(5) {
            if let Some(name) = p.file_name() {
                let _ = std::fs::rename(p, arch.join(name));
            }
        }
        // The rest of the dead process's dumps are noise — drop them now so the
        // rolling pruner starts clean for this process.
        dumps.into_iter().skip(5).for_each(|(_, old)| {
            let _ = std::fs::remove_file(old);
        });
        let mut archives: Vec<PathBuf> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with("prekill-")))
            .map(|e| e.path())
            .collect();
        archives.sort();
        let n = archives.len();
        archives.into_iter().take(n.saturating_sub(3)).for_each(|old| {
            let _ = std::fs::remove_dir_all(&old);
        });
        info!("profiling: archived previous process's final heap dumps → {arch:?}");
    }

    fn write_flamegraph(path: &std::path::Path, report: &pprof::Report) -> anyhow::Result<()> {
        report.flamegraph(std::fs::File::create(path)?)?;
        Ok(())
    }

    /// Keep only the newest `keep` files whose name starts with `prefix`,
    /// ordered by mtime — NOT filename. The CPU seq counter resets to 0 on every
    /// process restart, so a dead process's high-seq files (`cpu-000902`) would
    /// outsort a live process's fresh low-seq files (`cpu-000003`) by name and
    /// survive pruning forever, leaving us blind to the current run's CPU. mtime
    /// is monotonic across restarts, so newest-by-mtime always keeps the live
    /// process's files and evicts the stale ones.
    fn prune_old(dir: &std::path::Path, prefix: &str, keep: usize) {
        let mut files: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(prefix)))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        files.sort_unstable_by_key(|(mtime, _)| std::cmp::Reverse(*mtime)); // newest first; `mut`: std sorts in place
        files.into_iter().skip(keep).for_each(|(_, old)| {
            let _ = std::fs::remove_file(old);
        });
    }
}

#[cfg(all(feature = "profiling", target_os = "linux"))]
pub use imp::start;

/// No-op without the `profiling` feature (Linux) — callers wire it unconditionally at boot.
#[cfg(not(all(feature = "profiling", target_os = "linux")))]
pub fn start(_data_dir: std::path::PathBuf) {}

// ===== errors =====
// Shared error-wrapping helpers to collapse the repetitive `.map_err(|e| ...)`
// closures scattered across the write/query paths. Each preserves the original
// DataFusionError variant and message text.

use std::fmt::Display;

use datafusion::{arrow::error::ArrowError, error::DataFusionError};
use datafusion_postgres::pgwire::error::PgWireError;

/// Wrap an Arrow error as `DataFusionError::ArrowError`. Unlike
/// `DataFusionError::from`, skips backtrace capture — these fire on hot paths.
pub fn arrow_err(e: ArrowError) -> DataFusionError {
    DataFusionError::ArrowError(Box::new(e), None)
}

/// `.map_err(exec_err("context"))` → `Execution("context: {e}")`.
pub fn exec_err<E: Display>(ctx: &'static str) -> impl Fn(E) -> DataFusionError {
    move |e| DataFusionError::Execution(format!("{ctx}: {e}"))
}

/// `.map_err(wal_err("op"))` → `External("WAL op failed: {e}")`.
pub fn wal_err<E: Display>(op: &'static str) -> impl Fn(E) -> DataFusionError {
    move |e| DataFusionError::External(format!("WAL {op} failed: {e}").into())
}

/// Wrap any std error as `PgWireError::ApiError` — the pgwire escape hatch for
/// errors that carry no SQLSTATE of their own.
pub fn api_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> PgWireError {
    PgWireError::ApiError(Box::new(e))
}
