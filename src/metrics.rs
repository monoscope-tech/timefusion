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
    sync::{Arc, OnceLock, Weak},
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
    wal_corruption             => "timefusion.wal.corruption_events": "WAL entries that failed to deserialize or replay",
    wal_gc_deleted_files       => "timefusion.wal.gc_deleted_files": "Stale WAL files reclaimed by the mtime reaper (walrus leaks files across restarts)",
    flush_completed            => "timefusion.flush.completed": "Flush cycles that committed to Delta",
    flush_failed               => "timefusion.flush.failed": "Flush cycles that errored",
    flush_stalled              => "timefusion.flush.stalled": "Flush bucket commits that exceeded the flush-bucket watchdog timeout (Delta/S3 commit hung, holding flush_lock). A stalled flush frees no MemBuffer memory → inserts wedge at the hard limit. PAGE if > 0",
    query_executions           => "timefusion.query.executions": "SQL query plans executed",
    tantivy_prefilter_attempts => "timefusion.tantivy.prefilter_attempts": "Queries where at least one text_match predicate triggered a tantivy lookup",
    tantivy_prefilter_used     => "timefusion.tantivy.prefilter_used": "Queries where the tantivy id-set prefilter was applied to the Delta scan",
    count_pushdown_used        => "timefusion.query.count_pushdown_used": "COUNT(*) queries answered from Delta add-action stats without scanning",
    tantivy_prefilter_skipped  => "timefusion.tantivy.prefilter_skipped": "Queries where tantivy lookup was attempted but pushdown was skipped (no index, hit cap, or low selectivity)",
    tantivy_prefilter_errors   => "timefusion.tantivy.prefilter_errors": "Tantivy lookups that errored (S3 down, parse failure, etc.)",
    tantivy_build_failures     => "timefusion.tantivy.build_failures": "Post-flush tantivy index builds that errored — accumulating drift means queries silently fall back to UDF scan",
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
    dml_coalesce_dropped       => "timefusion.dml.coalesce_dropped": "Coalesced DML groups dropped after exhausting drain retries — deferred Delta updates were LOST for rows already in Delta (buffer-resident rows are unaffected). PAGE if > 0",
    dedup_chunk_skipped        => "timefusion.dedup.chunk_skipped": "Dedup chunk rewrites skipped (over the rewrite-byte budget, or partition in failure backoff). Duplicates persist in Delta — read-side dedup keeps queries correct — until a later sweep or manual compaction clears them. WARN if sustained",
    maintenance_checkpoint_failed => "timefusion.maintenance.checkpoint_failed": "Out-of-band checkpoint attempts that errored (e.g. R2 500 on the checkpoint PUT). Retried next tick; ingest is unaffected. WARN if sustained — checkpoints falling behind slows boot replay and blocks log cleanup",
    maintenance_log_cleanup_failed => "timefusion.maintenance.log_cleanup_failed": "Out-of-band expired-log-cleanup attempts that errored. Retried next tick; the _delta_log grows until it succeeds. WARN if sustained (a growing log slows every commit's version LIST)",
    reconcile_dangling_removed => "timefusion.maintenance.reconcile_dangling_removed": "Active Add entries whose parquet object was missing from the store and got Remove'd by the reconcile task. NONZERO means committed data was destroyed elsewhere (commit-path parquet deletion bug) — PAGE and investigate",
}

pub fn registry() -> Option<&'static MetricsRegistry> {
    METRICS.get()
}

/// Initialize OTel metrics. Idempotent (subsequent calls are no-ops). Returns
/// the meter provider so the caller can keep a handle for shutdown if needed.
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

    // 30s export interval is the OTLP/Prometheus convention. Memory cost is
    // negligible since we have ~7 series.
    let reader = PeriodicReader::builder(exporter).with_interval(Duration::from_secs(30)).build();

    let provider = SdkMeterProvider::builder().with_reader(reader).with_resource(resource).build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let meter = opentelemetry::global::meter("timefusion");

    // Observable gauges polled from snapshot_stats() each export cycle. We
    // build one shared snapshot per export by stashing the Weak; if the
    // upgrade fails (layer dropped during shutdown), each gauge records 0.
    let bl_for_buckets = buffered_layer.clone();
    meter
        .u64_observable_gauge("timefusion.mem_buffer.oldest_bucket_age_seconds")
        .with_description("Age of oldest MemBuffer bucket; alert if > 2x flush_interval_secs")
        .with_callback(move |obs| {
            if let Some(layer) = bl_for_buckets.upgrade()
                && let Some(age) = layer.snapshot_stats().oldest_bucket_age_secs
            {
                obs.observe(age, &[]);
            }
        })
        .build();

    // Each simple gauge upgrades the Weak, snapshots stats, and observes one
    // derived value. The macro captures that shape so each metric is a single
    // line; gauges with conditional/Option logic (oldest bucket age, index lag)
    // stay spelled out below.
    macro_rules! layer_gauge {
        ($id:literal, $desc:literal, |$s:ident| $value:expr) => {{
            let weak = buffered_layer.clone();
            meter
                .u64_observable_gauge($id)
                .with_description($desc)
                .with_callback(move |obs| {
                    if let Some(layer) = weak.upgrade() {
                        let $s = layer.snapshot_stats();
                        obs.observe($value, &[]);
                    }
                })
                .build();
        }};
    }

    // Same shape as layer_gauge! but registers a monotonic observable counter
    // (OTel Sum → Prometheus Counter) so PromQL rate() applies reset detection
    // and survives process restarts (the values snap to 0 on boot). Use for
    // cumulative totals; layer_gauge! for point-in-time levels.
    macro_rules! layer_counter {
        ($id:literal, $desc:literal, |$s:ident| $value:expr) => {{
            let weak = buffered_layer.clone();
            meter
                .u64_observable_counter($id)
                .with_description($desc)
                .with_callback(move |obs| {
                    if let Some(layer) = weak.upgrade() {
                        let $s = layer.snapshot_stats();
                        obs.observe($value, &[]);
                    }
                })
                .build();
        }};
    }

    layer_gauge!("timefusion.mem_buffer.pressure_pct", "MemBuffer memory pressure as percentage of max", |s| s.pressure_pct as u64);
    layer_gauge!("timefusion.mem_buffer.estimated_bytes", "MemBuffer estimated heap residency in bytes", |s| s.mem_estimated_bytes as u64);
    layer_gauge!("timefusion.mem_buffer.rows", "Total rows in MemBuffer across all projects/tables", |s| s.mem_total_rows as u64);
    // Ingest vs drain: rate() these two and compare. Ingested climbing faster
    // than flushed (while pressure_pct=100, flush_failed flat) = ingest
    // outpacing a working drain, not a stuck flush. Counters (not gauges) so
    // rate() handles the restart-to-0 reset. `ingested` includes WAL-recovered
    // rows so the pair stays comparable after a restart (see snapshot_stats).
    layer_counter!("timefusion.mem_buffer.rows_ingested_total", "Cumulative rows accepted into MemBuffer (incl. WAL recovery)", |s| s.rows_ingested_total);
    layer_counter!("timefusion.mem_buffer.rows_flushed_total", "Cumulative rows drained from MemBuffer to Delta", |s| s.rows_flushed_total);
    layer_gauge!("timefusion.wal.disk_bytes", "Disk bytes occupied by WAL shards", |s| s.wal_disk_bytes);
    layer_gauge!("timefusion.wal.files", "Number of WAL segment files on disk", |s| s.wal_files as u64);

    // Index lag: how far behind ingest the newest published tantivy index is.
    // Computed as max(0, now - newest_max_timestamp). Surfaces the post-flush
    // indexing lag that the rewriter / search service can't shortcut around.
    if let Some(indexer_weak) = tantivy_indexer {
        let bl_for_lag = buffered_layer;
        meter
            .u64_observable_gauge("timefusion.tantivy.index_lag_seconds")
            .with_description("now() minus newest indexed timestamp; quantifies post-flush index lag")
            .with_callback(move |obs| {
                let Some(svc) = indexer_weak.upgrade() else { return };
                let Some(newest_idx) = svc.newest_indexed_micros() else { return };
                // Compare against newest MemBuffer timestamp if available
                // (more meaningful than wall clock — ingest may have stopped).
                // Fall back to wall clock if no MemBuffer reference exists.
                let now_micros = if let Some(layer) = bl_for_lag.upgrade() {
                    layer.snapshot_stats().oldest_bucket_age_secs.map(|_| crate::clock::now_micros()).unwrap_or_else(crate::clock::now_micros)
                } else {
                    crate::clock::now_micros()
                };
                let lag_secs = ((now_micros - newest_idx).max(0) / 1_000_000) as u64;
                obs.observe(lag_secs, &[]);
            })
            .build();
    }

    let registry = MetricsRegistry::new(&meter);
    if METRICS.set(registry).is_err() {
        warn!("MetricsRegistry was already set; metric counters from this call will be discarded");
    }

    // Keep provider alive by leaking the Arc — it's process-global and lives
    // until shutdown anyway. Avoids stashing a handle the caller must own.
    let _ = Arc::new(provider);

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

pub fn record_flush(success: bool) {
    if let Some(m) = METRICS.get() {
        if success {
            m.flush_completed.add(1, &[]);
        } else {
            m.flush_failed.add(1, &[]);
        }
    }
}

/// Generates the no-attribute "increment by one" recorders. Each no-ops if
/// metrics weren't initialized.
macro_rules! simple_recorders {
    ($($fn_name:ident => $field:ident),+ $(,)?) => {
        $(
            pub fn $fn_name() {
                if let Some(m) = METRICS.get() {
                    m.$field.add(1, &[]);
                }
            }
        )+
    };
}

simple_recorders! {
    record_wal_corruption => wal_corruption,
    record_query => query_executions,
    record_tantivy_prefilter_attempt => tantivy_prefilter_attempts,
    record_tantivy_prefilter_used => tantivy_prefilter_used,
    record_tantivy_prefilter_skipped => tantivy_prefilter_skipped,
    record_tantivy_prefilter_error => tantivy_prefilter_errors,
    record_tantivy_build_failure => tantivy_build_failures,
    record_count_pushdown_used => count_pushdown_used,
    record_backpressure_engaged => backpressure_engaged,
    record_backpressure_rejected => backpressure_rejected,
    record_backpressure_force_flush => backpressure_force_flush,
    record_flush_stalled => flush_stalled,
}

pub fn record_dedup_dropped(rows: u64) {
    if let Some(m) = METRICS.get() {
        m.dedup_dropped_rows.add(rows, &[]);
    }
}

pub fn record_compaction_dedup_dropped(rows: u64) {
    if let Some(m) = METRICS.get() {
        m.compaction_dedup_dropped_rows.add(rows, &[]);
    }
}

/// Record one full-optimize run's idempotence split: how many window partitions
/// were rewritten vs skipped as unchanged (the cache-churn-avoided signal).
pub fn record_optimize_partitions(rewritten: u64, skipped: u64) {
    if let Some(m) = METRICS.get() {
        m.optimize_partitions_rewritten.add(rewritten, &[]);
        m.optimize_partitions_skipped.add(skipped, &[]);
    }
}

/// One optimize/compaction OCC conflict (retryable). A sustained rate means the
/// optimizer is repeatedly losing commit races to concurrent dedup/flush.
pub fn record_optimize_conflict() {
    if let Some(m) = METRICS.get() {
        m.optimize_conflict.add(1, &[]);
    }
}

/// One optimize/compaction run that errored or gave up after retries — that
/// partition stays fragmented until a later run succeeds.
pub fn record_optimize_failed() {
    if let Some(m) = METRICS.get() {
        m.optimize_failed.add(1, &[]);
    }
}

/// One DML Delta operation OCC conflict (retried on a fresh snapshot).
pub fn record_dml_conflict() {
    DML_STATS.occ_conflicts.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.dml_conflict.add(1, &[]);
    }
}

pub fn record_dml_retry_success() {
    DML_STATS.retry_successes.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.dml_retry_success.add(1, &[]);
    }
}

pub fn record_dml_retry_exhausted() {
    DML_STATS.retry_exhausted.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.dml_retry_exhausted.add(1, &[]);
    }
}

/// One DML Delta leg skipped because its time window is entirely unflushed.
pub fn record_dml_delta_leg_skipped() {
    if let Some(m) = METRICS.get() {
        m.dml_delta_leg_skipped.add(1, &[]);
    }
}

/// One `UPDATE ... FROM` Delta leg deferred into the coalescer.
pub fn record_dml_coalesce_enqueued() {
    if let Some(m) = METRICS.get() {
        m.dml_coalesce_enqueued.add(1, &[]);
    }
}

/// One Delta merge executed by a coalescer drain.
pub fn record_dml_coalesce_merge() {
    if let Some(m) = METRICS.get() {
        m.dml_coalesce_merges.add(1, &[]);
    }
}

/// One coalesced DML group dropped after exhausting drain retries.
pub fn record_dml_coalesce_dropped() {
    if let Some(m) = METRICS.get() {
        m.dml_coalesce_dropped.add(1, &[]);
    }
}

/// One dedup chunk rewrite skipped (over budget or in failure backoff).
pub fn record_dedup_chunk_skipped() {
    if let Some(m) = METRICS.get() {
        m.dedup_chunk_skipped.add(1, &[]);
    }
}

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub struct DmlStats {
    pub occ_conflicts: AtomicU64,
    pub retry_successes: AtomicU64,
    pub retry_exhausted: AtomicU64,
}

static DML_STATS: DmlStats = DmlStats { occ_conflicts: AtomicU64::new(0), retry_successes: AtomicU64::new(0), retry_exhausted: AtomicU64::new(0) };

pub fn dml_stats() -> &'static DmlStats {
    &DML_STATS
}

/// Readable maintenance counters for the `timefusion_stats` view — the OTel
/// counters above can't be read back in-process. Process-global const atomics
/// (no init needed), incremented by the out-of-band checkpoint + reconcile
/// tasks. `checkpoint_lag_versions` is the last observed max lag (a gauge), the
/// rest are monotonic.
#[derive(Default)]
pub struct MaintenanceStats {
    pub checkpoints_created: AtomicU64,
    pub checkpoint_failed: AtomicU64,
    pub log_files_cleaned: AtomicU64,
    pub log_cleanup_failed: AtomicU64,
    pub checkpoint_lag_versions: AtomicU64,
    pub dangling_removed: AtomicU64,
    pub reconcile_failed: AtomicU64,
    pub dedup_timed_out: AtomicU64,
    pub dedup_failed: AtomicU64,
    pub light_optimize_timed_out: AtomicU64,
    pub light_optimize_failed: AtomicU64,
    /// Cron ticks skipped because the previous run of the same job was still
    /// in flight. A steadily growing value = a wedged/overlong job body.
    pub cron_ticks_skipped: AtomicU64,
    /// Cron fires actually dispatched (all jobs). Frozen while uptime grows =
    /// the scheduler is dead (2026-07-14 outage signature).
    pub cron_ticks_fired: AtomicU64,
}

static MAINTENANCE_STATS: MaintenanceStats = MaintenanceStats {
    checkpoints_created: AtomicU64::new(0),
    checkpoint_failed: AtomicU64::new(0),
    log_files_cleaned: AtomicU64::new(0),
    log_cleanup_failed: AtomicU64::new(0),
    checkpoint_lag_versions: AtomicU64::new(0),
    dangling_removed: AtomicU64::new(0),
    reconcile_failed: AtomicU64::new(0),
    dedup_timed_out: AtomicU64::new(0),
    dedup_failed: AtomicU64::new(0),
    light_optimize_timed_out: AtomicU64::new(0),
    light_optimize_failed: AtomicU64::new(0),
    cron_ticks_skipped: AtomicU64::new(0),
    cron_ticks_fired: AtomicU64::new(0),
};

pub fn maintenance_stats() -> &'static MaintenanceStats {
    &MAINTENANCE_STATS
}

/// One out-of-band checkpoint failure (also mirrors to OTel for alerting).
pub fn record_checkpoint_failed() {
    MAINTENANCE_STATS.checkpoint_failed.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.maintenance_checkpoint_failed.add(1, &[]);
    }
}

/// One out-of-band log-cleanup failure (mirrors to OTel).
pub fn record_log_cleanup_failed() {
    MAINTENANCE_STATS.log_cleanup_failed.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.maintenance_log_cleanup_failed.add(1, &[]);
    }
}

/// `n` dangling Add entries Remove'd by the reconcile task (mirrors to OTel).
/// Nonzero ⇒ committed data was destroyed elsewhere.
pub fn record_dangling_removed(n: u64) {
    MAINTENANCE_STATS.dangling_removed.fetch_add(n, Relaxed);
    if let Some(m) = METRICS.get() {
        m.reconcile_dangling_removed.add(n, &[]);
    }
}
