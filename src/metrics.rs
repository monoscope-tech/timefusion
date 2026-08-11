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
        OnceLock, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

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

use crate::{buffered_write_layer::BufferedWriteLayer, config::TelemetryConfig, tantivy_index::service::TantivyIndexService};

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
    cache_insert_bypassed      => "timefusion.cache.insert_bypassed": "Cache populations suppressed because the read ran inside a large-scan bypass scope (scan-resistant admission — a wide historical scan must not evict the hot tail)",
    hot_tier_demote_skipped    => "timefusion.hot_tier.demote_skipped": "Flush groups NOT demoted to the local hot tier because a demotion was already running (the bound that keeps drained batches from piling up off-ledger). Purely a latency miss — those windows are served from Delta. WARN if sustained: the local IPC write is falling behind the flush rate",
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

    // 30s export interval is the OTLP/Prometheus convention.
    let reader = PeriodicReader::builder(exporter).with_interval(Duration::from_secs(30)).build();

    // The global registry owns the provider for the rest of the process.
    opentelemetry::global::set_meter_provider(SdkMeterProvider::builder().with_reader(reader).with_resource(resource).build());

    let meter = opentelemetry::global::meter("timefusion");

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
    // Local hot tier. The two "the tier is broken" signals are write_failures
    // (demotion failing — reads stay correct, just slower) and read_misses
    // (torn/absent files falling back to Delta); schema_drift is the silent
    // one, where a file looks like a healthy hit but contributes zero rows.
    layer_metric!(gauge "timefusion.hot_tier.bytes", "Bytes of demoted Arrow IPC held by the local hot tier", |s| s.hot_tier.bytes);
    layer_metric!(gauge "timefusion.hot_tier.files", "Demoted bucket files in the local hot tier", |s| s.hot_tier.files as u64);
    layer_metric!(counter "timefusion.hot_tier.writes_total", "Buckets demoted to the local hot tier", |s| s.hot_tier.writes);
    layer_metric!(counter "timefusion.hot_tier.write_failures_total", "Demotions that errored. ALERT if sustained", |s| s.hot_tier.write_failures);
    layer_metric!(counter "timefusion.hot_tier.read_hits_total", "Hot-tier files served to a scan", |s| s.hot_tier.read_hits);
    layer_metric!(counter "timefusion.hot_tier.read_misses_total", "Hot-tier files that read as torn/absent and fell through to Delta. ALERT if sustained", |s| s
        .hot_tier
        .read_misses);
    layer_metric!(counter "timefusion.hot_tier.schema_drift_total", "Hot-tier files skipped because their schema no longer matches the table's", |s| s
        .hot_tier
        .schema_drift);
    layer_metric!(counter "timefusion.hot_tier.mem_skipped_total", "Hot-tier files skipped because MemBuffer still owned their window (expected)", |s| s
        .hot_tier
        .mem_skipped);
    layer_metric!(gauge
        "timefusion.hot_tier.suppressed_tables",
        "Tables currently not demoting because a DML kept invalidating their files before any query read them",
        |s| s.hot_tier.suppressed_tables as u64
    );
    layer_metric!(counter
        "timefusion.hot_tier.suppressions_total",
        "Times a table's demotions were judged not to pay off and were suspended for a cooldown. Sustained growth on a table you expect to be read means the hot tier is losing a race with continuous enrichment — expected for whole-table UPDATE workloads, otherwise investigate",
        |s| s.hot_tier.suppressions
    );
    layer_metric!(gauge "timefusion.wal.disk_bytes", "Disk bytes occupied by WAL shards", |s| s.wal_disk_bytes);
    layer_metric!(gauge "timefusion.wal.files", "Number of WAL segment files on disk", |s| s.wal_files as u64);
    layer_metric!(gauge "timefusion.tantivy.recovery_pending_files", "Committed Parquet files awaiting post-WAL-replay Tantivy indexing", |s| s
        .tantivy_recovery_pending_files
        as u64);

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
                obs.observe(((crate::clock::now_micros() - newest_idx).max(0) / 1_000_000) as u64, &[]);
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
    record_hot_tier_demote_skipped => hot_tier_demote_skipped,
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

/// One dashboard aggregate that fell through to a raw scan. `reason` is a
/// `MissReason::label` — a closed, bounded set, never a table or project id.
pub fn record_rollup_miss(reason: &'static str) {
    let stats = maintenance_stats();
    stats.rollup_misses_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match reason {
        "not_built" => &stats.rollup_miss_not_built,
        "stale_coverage" => &stats.rollup_miss_stale_coverage,
        "tiny_interior" => &stats.rollup_miss_tiny_interior,
        "unsupported_shape" => &stats.rollup_miss_unsupported,
        "incomplete_coverage" => &stats.rollup_miss_incomplete_coverage,
        "unknown_filter" => &stats.rollup_miss_unknown_filter,
        "missing_measure" => &stats.rollup_miss_missing_measure,
        "unaligned_bucket_width" => &stats.rollup_miss_unaligned_bucket,
        "unknown_group_by" => &stats.rollup_miss_unknown_group_by,
        _ => &stats.rollup_miss_other,
    }
    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(m) = METRICS.get() {
        m.rollup_misses.add(1, &[KeyValue::new("reason", reason)]);
    }
}

pub fn record_commit_timeout(op: &'static str) {
    if let Some(m) = METRICS.get() {
        m.commit_lock_timeouts.add(1, &[KeyValue::new("op", op)]);
    }
}

/// Declares a process-global atomic-counter struct together with its all-zero
/// static, so a new counter can't drift from its initializer.
macro_rules! atomic_stats {
    ($(#[$sm:meta])* $name:ident => $global:ident { $($(#[$fm:meta])* $field:ident),+ $(,)? }) => {
        $(#[$sm])*
        pub struct $name {
            $($(#[$fm])* pub $field: AtomicU64,)+
        }
        static $global: $name = $name { $($field: AtomicU64::new(0),)+ };
    };
}

atomic_stats! {
    DmlStats => DML_STATS {
        occ_conflicts,
        retry_successes,
        retry_exhausted,
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
    MaintenanceStats => MAINTENANCE_STATS {
    checkpoints_created,
    checkpoint_failed,
    /// Checkpoints that wrote OK but failed post-write footer verification
    /// (the referenced object isn't a readable Parquet). Log cleanup is
    /// withheld so the JSON log stays recoverable. PAGE if > 0.
    checkpoint_corrupt,
    log_files_cleaned,
    log_cleanup_failed,
    checkpoint_lag_versions,
    dangling_removed,
    reconcile_failed,
    dedup_timed_out,
    dedup_failed,
    /// Adds a rewrite planner had to drop because the in-memory snapshot listed
    /// the same file twice. Nonzero means reads over that table double-count
    /// rows — the file list diverged from the log, which no amount of dedup or
    /// compaction repairs on its own. PAGE if > 0.
    snapshot_duplicate_adds,
    light_optimize_timed_out,
    light_optimize_failed,
    /// Ticks that hit the wall-clock budget with hot projects still pending.
    light_optimize_tick_truncated,
    /// Wave-engine per-tick accounting. `planned` counts projects the tick's
    /// single metadata walk found work for; `completed` counts bins that landed.
    /// ALERT when completed lags planned for N consecutive ticks — that's the
    /// "8 of 11 hot projects never reached" shape (prod 2026-07-29).
    light_optimize_projects_planned,
    light_optimize_projects_completed,
    light_optimize_bins_committed,
    light_optimize_waves_committed,
    /// Dedup-engine waves (data_change: true) — counted separately so the
    /// light_optimize_* counters mean pure compaction only.
    dedup_bins_committed,
    dedup_waves_committed,
    /// Waves not STARTED because the WAL was over its emergency-flush threshold
    /// (durability outranks compaction) or memory was near the cgroup limit.
    /// Chronic nonzero = compaction is being starved, not protected.
    light_optimize_wal_yields,
    /// Ticks/waves stopped because at least one MemBuffer bucket exceeded its
    /// retention target without landing. Unlike the byte-based WAL brake this
    /// catches small but old persistence debt.
    light_optimize_flush_debt_yields,
    light_optimize_memory_brakes,
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
    mor_delta_leg_sorts,
    /// Escalated flush sorts that FAILED and wrote their group unsorted. One
    /// unsorted file disables the reader's footer ordering for every scan
    /// touching its partition (query-time SortExec, unordered MOR dedup), so
    /// any nonzero here is a read-path incident in the making (2026-08-03).
    flush_sort_unsorted_fallbacks,
    /// Rounds where the WAL-backlog brake DEGRADED the wave to the one-project
    /// service floor (instead of stopping the tick). Chronic nonzero = ingest is
    /// outrunning flush often enough that compaction is running at the floor.
    light_optimize_ticks_degraded,
    /// Dashboard aggregates served from a rollup, split by how much of the
    /// window the rollup owned. The OTel counters carry the same numbers but
    /// cannot be read back in-process, and these two are the only signal that
    /// says whether read routing is actually firing — `rollup_hits_hybrid`
    /// specifically is the one that proves the raw-fringe union works, since a
    /// full-window hit needs no union at all.
    rollup_hits_full,
    rollup_hits_hybrid,
    /// Aggregates that fell through to a raw scan, plus the breakdown by reason.
    ///
    /// The OTel counter carries the same labels but cannot be read back
    /// in-process, and the reason is the ONLY thing that distinguishes "the
    /// build never ran" from "it ran and the source moved under it" from "the
    /// shape is unsupported" — which is the entire diagnosis of a rollout that
    /// is building rollups but not serving them. Without it the answer is
    /// guesswork over a 19k-line log.
    rollup_misses_total,
    rollup_miss_not_built,
    rollup_miss_stale_coverage,
    rollup_miss_tiny_interior,
    rollup_miss_unsupported,
    rollup_miss_incomplete_coverage,
    rollup_miss_unknown_filter,
    rollup_miss_missing_measure,
    rollup_miss_unaligned_bucket,
    rollup_miss_unknown_group_by,
    rollup_miss_other,
    dirty_bin_queue_depth,
    dirty_bin_enqueued,
    dirty_bin_eligible,
    dirty_bin_processed,
    dirty_bin_requeued,
    /// Queued bins consumed by the whole-date BATCH probe without per-bin
    /// staging (every flushed bin is enqueued, so in prod ~97% of queued bins
    /// carry no duplicates at all). Also counted in `dirty_bin_processed`.
    dirty_bin_batch_probe_clean,
    dirty_bin_dropped_rows,
    dirty_bin_rewrite_duration_ms,
    /// Cold-owned dirty bins (date old enough that the nightly consolidate owns
    /// the partition) DEPRIORITIZED to the tail of a drain pass and left on the
    /// queue. They are NOT dropped: consolidate bin-packs but does not collapse
    /// duplicates, so the dirty-bin drain stays their only physical dedup.
    /// NOTE `cold_optimize_after_days` defaults to 1 and the drain already skips
    /// today, so in the default configuration EVERY drainable bin is cold-owned
    /// — this then reads as "queued bins this pass had no batch slot for".
    /// Chronic growth = a backlog the batch size can't keep up with.
    dedup_bins_deferred_cold,
    /// Drain passes skipped (or cut short between chunks) because the flush path
    /// was behind. Dedup is an optimization — read-side DedupExec keeps results
    /// correct — so it yields to persistence (2026-07-30: a boot drain over a
    /// 10-day backlog pinned the commit path and starved flush for a whole
    /// container life). Chronic nonzero = flush is unhealthy, not dedup.
    dedup_passes_flush_yields,
    /// Per-bin STAGING attempts killed at the deadline and requeued. The
    /// deadline exists so one hung object-store read can't wedge the drain
    /// for hours behind the 1-permit maintenance semaphore (prod 2026-08-05:
    /// 6.5h stall, skips=77). Repeated hits are the same bin retrying —
    /// an oversized bin that can't finish inside the deadline, not noise.
    dedup_bin_stage_timeouts,
    /// Wave (dedup / light-optimize) commits that STOOD DOWN rather than queue
    /// on a per-table commit lock a flush was already waiting for — the flush
    /// starvation of prod 2026-07-30, where durability waited >600s behind
    /// legally-slow maintenance commits. The wave's bins are requeued and
    /// re-staged later, so this is deferred work, not lost work. Chronic nonzero
    /// = flush is saturating the commit path and compaction is being crowded out.
    wave_commits_yielded_to_flush,
    /// Boot-time resume of a staged-but-uncommitted footer-repair bin: the
    /// rewrite survived the restart that killed its process, so the next pass
    /// doesn't redo the (40+ minute) work. See `resume_staged_intents`.
    repair_resumed,
    /// Resume declined: an input file was rewritten underneath the staged
    /// output, so committing it would resurrect removed rows.
    repair_resume_declined_stale,
    /// Resume declined: a staged output object is missing or the wrong size —
    /// the process died mid-PUT.
    repair_resume_declined_incomplete,
    /// Resume declined because output rows != input rows. A repair is
    /// row-preserving by construction, so this must be ZERO forever; nonzero
    /// means a truncated staging that would have DROPPED rows, or a broken
    /// assumption. PAGE if > 0.
    repair_resume_row_mismatch,
    /// Cron ticks skipped because the previous run of the same job was still
    /// in flight. A steadily growing value = a wedged/overlong job body.
    cron_ticks_skipped,
    /// Cron fires actually dispatched (all jobs). Frozen while uptime grows =
    /// the scheduler is dead (2026-07-14 outage signature).
    cron_ticks_fired,
    /// Cron runs that exceeded the long-running warning threshold. Slow but
    /// progressing work is allowed to finish; this is for observability.
    cron_long_running,
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
