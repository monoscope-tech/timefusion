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
    quarantine_redriven        => "timefusion.wal.quarantine_redriven": "Quarantined WAL payloads successfully re-ingested through the durable insert path at boot",
    quarantine_backlog         => "timefusion.wal.quarantine_backlog_events": "Boot-time detections of a non-empty quarantine after re-drive — acked data is NOT in the store. PAGE if > 0",
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
    // Local hot tier. The two "the tier is broken" signals are write_failures
    // (demotion failing — reads stay correct, just slower) and read_misses
    // (torn/absent files falling back to Delta); schema_drift is the silent
    // one, where a file looks like a healthy hit but contributes zero rows.
    layer_gauge!("timefusion.hot_tier.bytes", "Bytes of demoted Arrow IPC held by the local hot tier", |s| s.hot_tier.bytes);
    layer_gauge!("timefusion.hot_tier.files", "Demoted bucket files in the local hot tier", |s| s.hot_tier.files as u64);
    layer_counter!("timefusion.hot_tier.writes_total", "Buckets demoted to the local hot tier", |s| s.hot_tier.writes);
    layer_counter!("timefusion.hot_tier.write_failures_total", "Demotions that errored. ALERT if sustained", |s| s.hot_tier.write_failures);
    layer_counter!("timefusion.hot_tier.read_hits_total", "Hot-tier files served to a scan", |s| s.hot_tier.read_hits);
    layer_counter!("timefusion.hot_tier.read_misses_total", "Hot-tier files that read as torn/absent and fell through to Delta. ALERT if sustained", |s| s
        .hot_tier
        .read_misses);
    layer_counter!("timefusion.hot_tier.schema_drift_total", "Hot-tier files skipped because their schema no longer matches the table's", |s| s
        .hot_tier
        .schema_drift);
    layer_counter!("timefusion.hot_tier.mem_skipped_total", "Hot-tier files skipped because MemBuffer still owned their window (expected)", |s| s
        .hot_tier
        .mem_skipped);
    layer_gauge!(
        "timefusion.hot_tier.suppressed_tables",
        "Tables currently not demoting because a DML kept invalidating their files before any query read them",
        |s| s.hot_tier.suppressed_tables as u64
    );
    layer_counter!(
        "timefusion.hot_tier.suppressions_total",
        "Times a table's demotions were judged not to pay off and were suspended for a cooldown. Sustained growth on a table you expect to be read means the hot tier is losing a race with continuous enrichment — expected for whole-table UPDATE workloads, otherwise investigate",
        |s| s.hot_tier.suppressions
    );
    layer_gauge!("timefusion.wal.disk_bytes", "Disk bytes occupied by WAL shards", |s| s.wal_disk_bytes);
    layer_gauge!("timefusion.wal.files", "Number of WAL segment files on disk", |s| s.wal_files as u64);
    layer_gauge!("timefusion.tantivy.recovery_pending_files", "Committed Parquet files awaiting post-WAL-replay Tantivy indexing", |s| s
        .tantivy_recovery_pending_files
        as u64);

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
    record_backpressure_engaged => backpressure_engaged,
    record_backpressure_rejected => backpressure_rejected,
    record_backpressure_force_flush => backpressure_force_flush,
    record_flush_stalled => flush_stalled,
    record_write_capture_skipped => write_capture_skipped,
    record_cache_confirm_timeout => cache_confirm_timeouts,
    record_cache_insert_bypassed => cache_insert_bypassed,
    record_hot_tier_demote_skipped => hot_tier_demote_skipped,
}

/// One pre-drain confirm pass: `attempted` files probed, `warmed` of them
/// missing and fetched (the write-capture gap).
pub fn record_cache_confirm(attempted: u64, warmed: u64) {
    if let Some(m) = METRICS.get() {
        m.cache_confirm_attempts.add(attempted, &[]);
        m.cache_confirm_warmed.add(warmed, &[]);
    }
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

/// One commit-path operation abandoned by its bound. `op` is a fixed set of
/// static labels (bounded cardinality by construction — never a table or
/// project id, which belong on the accompanying warn's span attributes).
pub fn record_commit_timeout(op: &'static str) {
    if let Some(m) = METRICS.get() {
        m.commit_lock_timeouts.add(1, &[KeyValue::new("op", op)]);
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
    DML_STATS.coalesce_merges.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(m) = METRICS.get() {
        m.dml_coalesce_merges.add(1, &[]);
    }
}

/// One coalesced DML group parked to the quarantine dir after exhausting
/// drain retries. Recoverable — unlike `record_dml_coalesce_dropped`.
pub fn record_dml_coalesce_quarantined() {
    DML_STATS.coalesce_quarantined.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if let Some(m) = METRICS.get() {
        m.dml_coalesce_quarantined.add(1, &[]);
    }
}

/// One coalesced DML group whose rows could not be quarantined — real loss.
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

/// One cron maintenance run that exceeded the long-running warning threshold.
pub fn record_cron_long_running() {
    MAINTENANCE_STATS.cron_long_running.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.maintenance_cron_long_running.add(1, &[]);
    }
}

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

pub struct DmlStats {
    pub occ_conflicts: AtomicU64,
    pub retry_successes: AtomicU64,
    pub retry_exhausted: AtomicU64,
    /// Delta merges executed by coalescer drains — readable in-process (the
    /// OTel counter isn't); tests assert on deltas of this to pin folding.
    pub coalesce_merges: AtomicU64,
    /// Groups parked to `<wal_dir>/quarantine/dml` — in-process readable so
    /// tests can assert the terminal branch parks instead of dropping.
    pub coalesce_quarantined: AtomicU64,
}

static DML_STATS: DmlStats = DmlStats {
    occ_conflicts: AtomicU64::new(0),
    retry_successes: AtomicU64::new(0),
    retry_exhausted: AtomicU64::new(0),
    coalesce_merges: AtomicU64::new(0),
    coalesce_quarantined: AtomicU64::new(0),
};

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
    /// Checkpoints that wrote OK but failed post-write footer verification
    /// (the referenced object isn't a readable Parquet). Log cleanup is
    /// withheld so the JSON log stays recoverable. PAGE if > 0.
    pub checkpoint_corrupt: AtomicU64,
    pub log_files_cleaned: AtomicU64,
    pub log_cleanup_failed: AtomicU64,
    pub checkpoint_lag_versions: AtomicU64,
    pub dangling_removed: AtomicU64,
    pub reconcile_failed: AtomicU64,
    pub dedup_timed_out: AtomicU64,
    pub dedup_failed: AtomicU64,
    pub light_optimize_timed_out: AtomicU64,
    pub light_optimize_failed: AtomicU64,
    /// Ticks that hit the wall-clock budget with hot projects still pending.
    pub light_optimize_tick_truncated: AtomicU64,
    /// Wave-engine per-tick accounting. `planned` counts projects the tick's
    /// single metadata walk found work for; `completed` counts bins that landed.
    /// ALERT when completed lags planned for N consecutive ticks — that's the
    /// "8 of 11 hot projects never reached" shape (prod 2026-07-29).
    pub light_optimize_projects_planned: AtomicU64,
    pub light_optimize_projects_completed: AtomicU64,
    pub light_optimize_bins_committed: AtomicU64,
    pub light_optimize_waves_committed: AtomicU64,
    /// Dedup-engine waves (data_change: true) — counted separately so the
    /// light_optimize_* counters mean pure compaction only.
    pub dedup_bins_committed: AtomicU64,
    pub dedup_waves_committed: AtomicU64,
    /// Waves not STARTED because the WAL was over its emergency-flush threshold
    /// (durability outranks compaction) or memory was near the cgroup limit.
    /// Chronic nonzero = compaction is being starved, not protected.
    pub light_optimize_wal_yields: AtomicU64,
    pub light_optimize_memory_brakes: AtomicU64,
    /// Rounds where the WAL-backlog brake DEGRADED the wave to the one-project
    /// service floor (instead of stopping the tick). Chronic nonzero = ingest is
    /// outrunning flush often enough that compaction is running at the floor.
    pub light_optimize_ticks_degraded: AtomicU64,
    pub dirty_bin_queue_depth: AtomicU64,
    pub dirty_bin_enqueued: AtomicU64,
    pub dirty_bin_eligible: AtomicU64,
    pub dirty_bin_processed: AtomicU64,
    pub dirty_bin_requeued: AtomicU64,
    pub dirty_bin_dropped_rows: AtomicU64,
    pub dirty_bin_rewrite_duration_ms: AtomicU64,
    /// Cold-owned dirty bins (date old enough that the nightly consolidate owns
    /// the partition) DEPRIORITIZED to the tail of a drain pass and left on the
    /// queue. They are NOT dropped: consolidate bin-packs but does not collapse
    /// duplicates, so the dirty-bin drain stays their only physical dedup.
    /// NOTE `cold_optimize_after_days` defaults to 1 and the drain already skips
    /// today, so in the default configuration EVERY drainable bin is cold-owned
    /// — this then reads as "queued bins this pass had no batch slot for".
    /// Chronic growth = a backlog the batch size can't keep up with.
    pub dedup_bins_deferred_cold: AtomicU64,
    /// Drain passes skipped (or cut short between chunks) because the flush path
    /// was behind. Dedup is an optimization — read-side DedupExec keeps results
    /// correct — so it yields to persistence (2026-07-30: a boot drain over a
    /// 10-day backlog pinned the commit path and starved flush for a whole
    /// container life). Chronic nonzero = flush is unhealthy, not dedup.
    pub dedup_passes_flush_yields: AtomicU64,
    /// Wave (dedup / light-optimize) commits that STOOD DOWN rather than queue
    /// on a per-table commit lock a flush was already waiting for — the flush
    /// starvation of prod 2026-07-30, where durability waited >600s behind
    /// legally-slow maintenance commits. The wave's bins are requeued and
    /// re-staged later, so this is deferred work, not lost work. Chronic nonzero
    /// = flush is saturating the commit path and compaction is being crowded out.
    pub wave_commits_yielded_to_flush: AtomicU64,
    /// Cron ticks skipped because the previous run of the same job was still
    /// in flight. A steadily growing value = a wedged/overlong job body.
    pub cron_ticks_skipped: AtomicU64,
    /// Cron fires actually dispatched (all jobs). Frozen while uptime grows =
    /// the scheduler is dead (2026-07-14 outage signature).
    pub cron_ticks_fired: AtomicU64,
    /// Cron runs that exceeded the long-running warning threshold. Slow but
    /// progressing work is allowed to finish; this is for observability.
    pub cron_long_running: AtomicU64,
}

static MAINTENANCE_STATS: MaintenanceStats = MaintenanceStats {
    checkpoints_created: AtomicU64::new(0),
    checkpoint_failed: AtomicU64::new(0),
    checkpoint_corrupt: AtomicU64::new(0),
    log_files_cleaned: AtomicU64::new(0),
    log_cleanup_failed: AtomicU64::new(0),
    checkpoint_lag_versions: AtomicU64::new(0),
    dangling_removed: AtomicU64::new(0),
    reconcile_failed: AtomicU64::new(0),
    dedup_timed_out: AtomicU64::new(0),
    dedup_failed: AtomicU64::new(0),
    light_optimize_timed_out: AtomicU64::new(0),
    light_optimize_failed: AtomicU64::new(0),
    light_optimize_tick_truncated: AtomicU64::new(0),
    light_optimize_projects_planned: AtomicU64::new(0),
    light_optimize_projects_completed: AtomicU64::new(0),
    light_optimize_bins_committed: AtomicU64::new(0),
    light_optimize_waves_committed: AtomicU64::new(0),
    dedup_bins_committed: AtomicU64::new(0),
    dedup_waves_committed: AtomicU64::new(0),
    light_optimize_wal_yields: AtomicU64::new(0),
    light_optimize_memory_brakes: AtomicU64::new(0),
    light_optimize_ticks_degraded: AtomicU64::new(0),
    dirty_bin_queue_depth: AtomicU64::new(0),
    dirty_bin_enqueued: AtomicU64::new(0),
    dirty_bin_eligible: AtomicU64::new(0),
    dirty_bin_processed: AtomicU64::new(0),
    dirty_bin_requeued: AtomicU64::new(0),
    dirty_bin_dropped_rows: AtomicU64::new(0),
    dirty_bin_rewrite_duration_ms: AtomicU64::new(0),
    dedup_bins_deferred_cold: AtomicU64::new(0),
    dedup_passes_flush_yields: AtomicU64::new(0),
    wave_commits_yielded_to_flush: AtomicU64::new(0),
    cron_ticks_skipped: AtomicU64::new(0),
    cron_ticks_fired: AtomicU64::new(0),
    cron_long_running: AtomicU64::new(0),
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

/// One checkpoint that failed post-write footer verification (mirrors to OTel).
pub fn record_checkpoint_corrupt() {
    MAINTENANCE_STATS.checkpoint_corrupt.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.maintenance_checkpoint_corrupt.add(1, &[]);
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
