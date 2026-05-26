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

use crate::buffered_write_layer::BufferedWriteLayer;
use crate::config::TelemetryConfig;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;
use tracing::{info, warn};

static METRICS: OnceLock<MetricsRegistry> = OnceLock::new();

/// Holds counters that need to be incremented from the hot path. Gauges are
/// observed by callback and don't need to live here.
pub struct MetricsRegistry {
    pub ingest_inserts: Counter<u64>,
    pub ingest_rows: Counter<u64>,
    pub ingest_errors: Counter<u64>,
    pub wal_corruption: Counter<u64>,
    pub flush_completed: Counter<u64>,
    pub flush_failed: Counter<u64>,
    pub query_executions: Counter<u64>,
}

impl MetricsRegistry {
    fn new(meter: &Meter) -> Self {
        Self {
            ingest_inserts: meter.u64_counter("timefusion.ingest.inserts").with_description("Ingest insert calls accepted").build(),
            ingest_rows: meter.u64_counter("timefusion.ingest.rows").with_description("Rows accepted into MemBuffer").build(),
            ingest_errors: meter.u64_counter("timefusion.ingest.errors").with_description("Ingest call failures").build(),
            wal_corruption: meter
                .u64_counter("timefusion.wal.corruption_events")
                .with_description("WAL entries that failed to deserialize or replay")
                .build(),
            flush_completed: meter.u64_counter("timefusion.flush.completed").with_description("Flush cycles that committed to Delta").build(),
            flush_failed: meter.u64_counter("timefusion.flush.failed").with_description("Flush cycles that errored").build(),
            query_executions: meter.u64_counter("timefusion.query.executions").with_description("SQL query plans executed").build(),
        }
    }
}

pub fn registry() -> Option<&'static MetricsRegistry> {
    METRICS.get()
}

/// Initialize OTel metrics. Idempotent (subsequent calls are no-ops). Returns
/// the meter provider so the caller can keep a handle for shutdown if needed.
///
/// `buffered_layer` is a Weak so the metrics callback doesn't extend its
/// lifetime — the layer owns its shutdown order, not us.
pub fn init_metrics(config: &TelemetryConfig, buffered_layer: Weak<BufferedWriteLayer>) -> anyhow::Result<()> {
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
            if let Some(layer) = bl_for_buckets.upgrade() {
                if let Some(age) = layer.snapshot_stats().oldest_bucket_age_secs {
                    obs.observe(age, &[]);
                }
            }
        })
        .build();

    let bl_for_pressure = buffered_layer.clone();
    meter
        .u64_observable_gauge("timefusion.mem_buffer.pressure_pct")
        .with_description("MemBuffer memory pressure as percentage of max")
        .with_callback(move |obs| {
            if let Some(layer) = bl_for_pressure.upgrade() {
                obs.observe(layer.snapshot_stats().pressure_pct as u64, &[]);
            }
        })
        .build();

    let bl_for_bytes = buffered_layer.clone();
    meter
        .u64_observable_gauge("timefusion.mem_buffer.estimated_bytes")
        .with_description("MemBuffer estimated heap residency in bytes")
        .with_callback(move |obs| {
            if let Some(layer) = bl_for_bytes.upgrade() {
                obs.observe(layer.snapshot_stats().mem_estimated_bytes as u64, &[]);
            }
        })
        .build();

    let bl_for_rows = buffered_layer.clone();
    meter
        .u64_observable_gauge("timefusion.mem_buffer.rows")
        .with_description("Total rows in MemBuffer across all projects/tables")
        .with_callback(move |obs| {
            if let Some(layer) = bl_for_rows.upgrade() {
                obs.observe(layer.snapshot_stats().mem_total_rows as u64, &[]);
            }
        })
        .build();

    let bl_for_wal = buffered_layer.clone();
    meter
        .u64_observable_gauge("timefusion.wal.disk_bytes")
        .with_description("Disk bytes occupied by WAL shards")
        .with_callback(move |obs| {
            if let Some(layer) = bl_for_wal.upgrade() {
                obs.observe(layer.snapshot_stats().wal_disk_bytes, &[]);
            }
        })
        .build();

    let bl_for_wal_files = buffered_layer;
    meter
        .u64_observable_gauge("timefusion.wal.files")
        .with_description("Number of WAL segment files on disk")
        .with_callback(move |obs| {
            if let Some(layer) = bl_for_wal_files.upgrade() {
                obs.observe(layer.snapshot_stats().wal_files as u64, &[]);
            }
        })
        .build();

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

/// Convenience helpers for hot-path counter increments. No-op if metrics
/// weren't initialized (tests, embedded use).
pub fn record_insert(rows: u64) {
    if let Some(m) = METRICS.get() {
        m.ingest_inserts.add(1, &[]);
        m.ingest_rows.add(rows, &[]);
    }
}

pub fn record_ingest_error() {
    if let Some(m) = METRICS.get() {
        m.ingest_errors.add(1, &[]);
    }
}

pub fn record_wal_corruption() {
    if let Some(m) = METRICS.get() {
        m.wal_corruption.add(1, &[]);
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

pub fn record_query() {
    if let Some(m) = METRICS.get() {
        m.query_executions.add(1, &[]);
    }
}
