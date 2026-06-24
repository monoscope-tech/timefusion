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
    query_executions           => "timefusion.query.executions": "SQL query plans executed",
    tantivy_prefilter_attempts => "timefusion.tantivy.prefilter_attempts": "Queries where at least one text_match predicate triggered a tantivy lookup",
    tantivy_prefilter_used     => "timefusion.tantivy.prefilter_used": "Queries where the tantivy id-set prefilter was applied to the Delta scan",
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

    layer_gauge!(
        "timefusion.mem_buffer.pressure_pct",
        "MemBuffer memory pressure as percentage of max",
        |s| s.pressure_pct as u64
    );
    layer_gauge!(
        "timefusion.mem_buffer.estimated_bytes",
        "MemBuffer estimated heap residency in bytes",
        |s| s.mem_estimated_bytes as u64
    );
    layer_gauge!(
        "timefusion.mem_buffer.rows",
        "Total rows in MemBuffer across all projects/tables",
        |s| s.mem_total_rows as u64
    );
    // Ingest vs drain: rate() these two and compare. Ingested climbing faster
    // than flushed (while pressure_pct=100, flush_failed flat) = ingest
    // outpacing a working drain, not a stuck flush.
    layer_gauge!("timefusion.buffer.rows_ingested_total", "Cumulative rows accepted into MemBuffer", |s| s
        .rows_ingested_total);
    layer_gauge!("timefusion.buffer.rows_flushed_total", "Cumulative rows drained from MemBuffer to Delta", |s| s
        .rows_flushed_total);
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
                    layer
                        .snapshot_stats()
                        .oldest_bucket_age_secs
                        .map(|_| crate::clock::now_micros())
                        .unwrap_or_else(crate::clock::now_micros)
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

    info!(
        "OpenTelemetry metrics initialized (OTLP -> {}, interval=30s)",
        config.otel_exporter_otlp_endpoint
    );
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
    record_backpressure_engaged => backpressure_engaged,
    record_backpressure_rejected => backpressure_rejected,
    record_backpressure_force_flush => backpressure_force_flush,
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
