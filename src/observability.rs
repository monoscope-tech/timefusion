//! OpenTelemetry metrics export: gauges observed from `BufferedWriteLayer`
//! snapshots each export cycle, plus counters incremented inline via
//! `record_*`. All helpers no-op if `init_metrics()` was never called.

use std::{
    sync::{
        Arc, LazyLock, OnceLock, Weak,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use parking_lot::Mutex;

static MAINTENANCE_RETRY_REASON: LazyLock<Mutex<String>> = LazyLock::new(Mutex::default);

pub fn set_maintenance_retry_reason(reason: &str) {
    *MAINTENANCE_RETRY_REASON.lock() = reason.to_owned();
}

pub fn maintenance_retry_reason() -> String {
    MAINTENANCE_RETRY_REASON.lock().clone()
}

/// Retries counted per `(operation, reason)`.
static MAINTENANCE_RETRIES: LazyLock<dashmap::DashMap<String, AtomicU64>> = LazyLock::new(dashmap::DashMap::new);

/// Work actually done per `(operation, metric)` — numerator (`rows_dropped`)
/// and denominator (`worker_secs`) for a work rate, since unit counts measure
/// churn as readily as work.
static MAINTENANCE_WORK: LazyLock<dashmap::DashMap<String, AtomicU64>> = LazyLock::new(dashmap::DashMap::new);

/// Bounded on purpose: a retry reason can carry error text, so the map stops
/// accepting NEW keys once full rather than growing with distinct error strings.
fn add_bounded(map: &dashmap::DashMap<String, AtomicU64>, key: String, amount: u64) {
    const MAX_KEYS: usize = 128;
    if let Some(count) = map.get(&key) {
        count.fetch_add(amount, Relaxed);
    } else if map.len() < MAX_KEYS {
        map.entry(key).or_default().fetch_add(amount, Relaxed);
    }
}

fn counter_rows(map: &dashmap::DashMap<String, AtomicU64>, prefix: &str) -> Vec<(String, u64)> {
    map.iter().map(|entry| (format!("{prefix}.{}", entry.key()), entry.value().load(Relaxed))).collect()
}

pub fn count_maintenance_retry(operation: &str, reason: &str) {
    add_bounded(&MAINTENANCE_RETRIES, format!("{operation}.{}", reason.split([':', '(']).next().unwrap_or(reason).trim()), 1);
}

/// `rows_dropped` is exact and post-commit; `worker_secs`/`killed_secs` are wall
/// time a worker held for this operation. `progress_rows` is the liveness
/// counter's tally — summed over every operator in the plan tree, so it is a
/// same-shape trend proxy, NOT a count of rows of work.
pub fn count_maintenance_work(operation: &str, metric: &str, amount: u64) {
    add_bounded(&MAINTENANCE_WORK, format!("{operation}.{metric}"), amount);
}

pub fn maintenance_retry_rows() -> Vec<(String, u64)> {
    counter_rows(&MAINTENANCE_RETRIES, "retry")
}

pub fn maintenance_work_rows() -> Vec<(String, u64)> {
    counter_rows(&MAINTENANCE_WORK, "work")
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

/// Declares the counter registry struct and its `new()` builder from one list
/// of `field => "metric.id": "description"` entries.
macro_rules! counter_registry {
    ($($field:ident => $id:literal : $desc:literal),+ $(,)?) => {
        /// Counters incremented from the hot path. Gauges are observed by
        /// callback and don't live here.
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
    cache_insert_bypassed    => "timefusion.cache.insert_bypassed": "Cache populations suppressed because the read ran inside a large-scan bypass scope (scan-resistant admission — a wide historical scan must not evict the hot tail)",
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

/// In-process side of `metrics::histogram!()` calls, for readback — the OTel
/// bridge is push-only, so both are fanned out from one global `Recorder`.
/// One DDSketch `Summary` per metric name, created lazily on first `record()`.
struct LocalHistograms(dashmap::DashMap<String, Mutex<metrics_util::storage::Summary>>);

struct LocalHistogramHandle {
    histograms: Arc<LocalHistograms>,
    name: String,
}

impl metrics::HistogramFn for LocalHistogramHandle {
    fn record(&self, value: f64) {
        self.histograms.0.entry(self.name.clone()).or_insert_with(|| Mutex::new(metrics_util::storage::Summary::with_defaults())).lock().add(value);
    }
}

type CounterGaugeRegistry = metrics_util::registry::Registry<metrics::Key, metrics_util::registry::AtomicStorage>;

/// Local recorder: histograms via `LocalHistograms`, counters/gauges via
/// `metrics_util`'s `Registry` + `AtomicStorage`. Must be its own newtype —
/// the orphan rule forbids implementing `Recorder` on `Arc<LocalHistograms>`.
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

impl LocalRecorder {
    fn new() -> Self {
        Self { histograms: Arc::new(LocalHistograms(dashmap::DashMap::new())), registry: Arc::new(CounterGaugeRegistry::atomic()) }
    }
}

static LOCAL_HISTOGRAMS: OnceLock<Arc<LocalHistograms>> = OnceLock::new();
static LOCAL_REGISTRY: OnceLock<Arc<CounterGaugeRegistry>> = OnceLock::new();

/// Installs `recorder` globally (no-op if one is already installed) and on
/// success publishes `local`'s handles for in-process readback. `local` must be
/// `recorder` itself or one of its fanout arms, so readback sees every write.
fn publish_local(local: LocalRecorder, recorder: impl metrics::Recorder + Sync + 'static) {
    if metrics::set_global_recorder(recorder).is_ok() {
        let _ = LOCAL_HISTOGRAMS.set(local.histograms);
        let _ = LOCAL_REGISTRY.set(local.registry);
    }
}

/// Read back a quantile (0.0-1.0) for a name recorded via `metrics::histogram!()`.
/// `None` if metrics weren't initialized or the name has never recorded a value.
pub fn histogram_quantile(name: &str, p: f64) -> Option<f64> {
    LOCAL_HISTOGRAMS.get()?.0.get(name)?.lock().quantile(p)
}

/// Read back the current value of a name recorded via `metrics::counter!()`.
/// 0 if metrics weren't initialized or the name has never recorded a value.
pub fn counter_value(name: &'static str) -> u64 {
    LOCAL_REGISTRY.get().and_then(|r| r.get_counter(&metrics::Key::from_name(name))).map_or(0, |c| c.load(Relaxed))
}

/// Read back the current value of a name recorded via `metrics::gauge!()`. See `counter_value`.
pub fn gauge_value(name: &'static str) -> f64 {
    LOCAL_REGISTRY.get().and_then(|r| r.get_gauge(&metrics::Key::from_name(name))).map_or(0.0, |g| f64::from_bits(g.load(Relaxed)))
}

/// Test helper: installs only the local (non-OTel) recorder, so tests can read
/// back `counter!`/`histogram!` values without an OTLP collector. Idempotent.
pub fn init_local_metrics_for_test() {
    let local = LocalRecorder::new();
    publish_local(local.clone(), local);
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

    // Bridges the `metrics` facade onto this Meter. Fanned out to two recorders
    // because the OTel bridge is push-only: the local arm is what makes values
    // readable back in-process for `timefusion_stats`.
    let local = LocalRecorder::new();
    let fanout = metrics_util::layers::FanoutBuilder::default()
        .add_recorder(local.clone())
        .add_recorder(metrics_exporter_opentelemetry::Recorder::with_meter(meter.clone()))
        .build();
    publish_local(local, fanout);

    // Observable gauges polled from snapshot_stats() each export cycle; if the
    // Weak upgrade fails (layer dropped during shutdown) the gauge observes nothing.
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

    // Upgrade the Weak, snapshot stats, observe one derived value. Use `counter`
    // for cumulative totals (OTel Sum, so rate() survives the restart-to-0) and
    // `gauge` for point-in-time levels.
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

    /// Gauges sourced from a process-global atomic; `$read` is evaluated once
    /// per export cycle.
    macro_rules! atomic_gauge {
        ($id:literal, $desc:literal, $read:expr) => {
            meter.u64_observable_gauge($id).with_description($desc).with_callback(|obs| obs.observe($read, &[])).build();
        };
    }

    /// Cumulative totals from a process-global atomic. A counter, not a gauge, so
    /// `rate()` still reads correctly across the reset to zero that every deploy
    /// causes — and this service redeploys on any non-docs push.
    macro_rules! atomic_counter {
        ($id:literal, $desc:literal, $read:expr) => {
            meter.u64_observable_counter($id).with_description($desc).with_callback(|obs| obs.observe($read, &[])).build();
        };
    }

    layer_metric!(gauge "timefusion.mem_buffer.pressure_pct", "MemBuffer memory pressure as percentage of max", |s| s.pressure_pct as u64);
    layer_metric!(gauge "timefusion.mem_buffer.estimated_bytes", "MemBuffer estimated heap residency in bytes", |s| s.mem_estimated_bytes as u64);
    layer_metric!(gauge "timefusion.mem_buffer.rows", "Total rows in MemBuffer across all projects/tables", |s| s.mem_total_rows as u64);
    // Ingest vs drain: rate() these two and compare. `ingested` includes
    // WAL-recovered rows so the pair stays comparable after a restart.
    layer_metric!(counter "timefusion.mem_buffer.rows_ingested_total", "Cumulative rows accepted into MemBuffer (incl. WAL recovery)", |s| s
        .rows_ingested_total);
    layer_metric!(counter "timefusion.mem_buffer.rows_flushed_total", "Cumulative rows drained from MemBuffer to Delta", |s| s.rows_flushed_total);
    layer_metric!(gauge "timefusion.wal.disk_bytes", "Disk bytes occupied by WAL shards", |s| s.wal_disk_bytes);
    layer_metric!(gauge "timefusion.wal.files", "Number of WAL segment files on disk", |s| s.wal_files as u64);
    layer_metric!(gauge "timefusion.tantivy.recovery_pending_files", "Committed Parquet files awaiting post-WAL-replay Tantivy indexing", |s| s
        .tantivy_recovery_pending_files
        as u64);

    atomic_gauge!(
        "timefusion.tantivy.cache_disk_bytes",
        "Bytes under <data_dir>/tantivy_cache as of the most recent reap; 0 until the first one runs",
        TANTIVY_CACHE_BYTES.load(Relaxed)
    );

    atomic_gauge!(
        "timefusion.runtime.scheduling_lag_ms",
        "How late a 500ms timer task actually woke — nonzero means workers are starved, which is what a missed health probe looks like from inside",
        RUNTIME_LAG_LAST_MS.load(Relaxed)
    );
    atomic_gauge!(
        "timefusion.runtime.scheduling_lag_max_ms",
        "Worst scheduling lag this process lifetime; survives the spike so a post-mortem can still see it",
        RUNTIME_LAG_MAX_MS.load(Relaxed)
    );

    atomic_gauge!(
        "timefusion.rollup.maintenance.pending_dirty_partitions",
        "Source partitions with durable rollup invalidations awaiting maintenance",
        maintenance_stats().rollup_dirty_partitions.load(Relaxed)
    );
    atomic_gauge!(
        "timefusion.rollup.maintenance.oldest_invalidation_age_seconds",
        "Age of the oldest durable rollup invalidation",
        maintenance_stats().rollup_oldest_invalidation_age_secs.load(Relaxed)
    );

    // THE MAINTENANCE LANE. None of this was exported before, so all of it lived
    // only in `timefusion_stats` — a process-scoped table that resets on every
    // deploy, and prod redeploys on any non-docs push. A full day of 2026-09-13
    // went into distinguishing a lane that WEDGES (both permits held, zero claims,
    // every throughput counter frozen) from one that is merely slow, and it could
    // not be settled, because a point sample has nothing to be compared against
    // and each restart erased the evidence. Meanwhile the metrics that DO have
    // history told the story immediately: over the same seven days
    // `pending_dirty_partitions` rose 340 -> 588 and `cron_long_running` tripled.
    atomic_gauge!(
        "timefusion.maintenance.permit_held_without_staging_seconds",
        "Longest a hygiene permit has been held without reaching a sort. THE wedge signal: a wedged lane stops counting rather than counting badly, so every throughput metric goes quiet and quiet reads as healthy",
        maintenance_stats().permit_held_without_staging_secs.load(Relaxed)
    );
    atomic_gauge!(
        "timefusion.maintenance.permits_available",
        "Free hygiene rewrite permits. Pinned at 0 while the lane is wedged",
        maintenance_stats().light_rewrite_permits_available.load(Relaxed)
    );
    atomic_counter!(
        "timefusion.maintenance.permits_acquired",
        "Hygiene permits taken. Frozen against a climbing permits_unavailable is the wedge; both climbing is healthy contention",
        maintenance_stats().compaction_permits_acquired.load(Relaxed)
    );
    atomic_counter!(
        "timefusion.maintenance.permits_unavailable",
        "Hygiene turns that found no free permit and gave up before claiming",
        maintenance_stats().compaction_permits_unavailable.load(Relaxed)
    );
    // Committed bins per lane — the drain rate. Quoting one without history is how
    // a working-window burst right after a restart gets reported as a sustained rate.
    atomic_counter!(
        "timefusion.maintenance.light_optimize_bins_committed",
        "Hot-pack and sealed-consolidation bins committed",
        maintenance_stats().light_optimize_bins_committed.load(Relaxed)
    );
    atomic_counter!(
        "timefusion.maintenance.dedup_bins_committed",
        "Dedup bins committed",
        maintenance_stats().dedup_bins_committed.load(Relaxed)
    );
    // Queue depth per lane. Depth alone cannot tell slow from never-claimed —
    // pair it with the permit counters above, which is what settles starvation.
    atomic_gauge!("timefusion.maintenance.pending_base_rollup", "BaseRollup units queued", maintenance_stats().pending_base_rollup.load(Relaxed));
    atomic_gauge!("timefusion.maintenance.pending_dedup", "Dedup units queued", maintenance_stats().pending_dedup.load(Relaxed));
    atomic_gauge!("timefusion.maintenance.pending_repair", "Repair units queued", maintenance_stats().pending_repair.load(Relaxed));
    atomic_gauge!(
        "timefusion.maintenance.sealed_compaction_debt_bytes",
        "Sealed compaction debt in DECODED bytes, not bytes on disk — roughly 12x the compressed footprint, and it is a stock of estimates that never decrements, so read it as a shape and never as a drain rate",
        maintenance_stats().sealed_compaction_debt_bytes.load(Relaxed)
    );

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

/// Build the standard (project_id, table_name) attribute pair. Series
/// cardinality is projects × tables — drop `project_id` if a deployment has
/// thousands of projects.
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

/// Last observed size of the tantivy extracted-index disk cache. Written by the
/// reap cron — the directory walk is too expensive to run per scrape.
static TANTIVY_CACHE_BYTES: AtomicU64 = AtomicU64::new(0);

pub fn record_tantivy_cache_bytes(bytes: u64) {
    TANTIVY_CACHE_BYTES.store(bytes, Relaxed);
}

/// When this process started, as seen from inside it — every accrual counter is
/// only interpretable against process age.
static PROCESS_START: LazyLock<std::time::Instant> = LazyLock::new(std::time::Instant::now);

/// Pin the process-start instant. Idempotent; call as early as possible in
/// every entry point (`main`, `bootstrap`) — the first force wins, so a late
/// first call would under-report uptime for the whole process lifetime.
pub fn mark_process_start() {
    LazyLock::force(&PROCESS_START);
}

pub fn process_uptime_secs() -> u64 {
    PROCESS_START.elapsed().as_secs()
}

/// WHICH instance this is — minted on first use, never reused, and stamped on
/// everything a restart may have to disown. The PID cannot serve: the process
/// runs as PID 1 in a container, so every restart would look like the same one.
pub fn instance_id() -> &'static str {
    static INSTANCE_ID: LazyLock<String> = LazyLock::new(|| uuid::Uuid::new_v4().to_string());
    &INSTANCE_ID
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

/// Samples how late a task that asked to wake in exactly `SAMPLE_EVERY` really
/// woke — i.e. whether CPU-bound work is starving the runtime the pgwire
/// handshake shares. Cost is one timer wakeup per interval.
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
            // entirely scheduling delay.
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

/// Keyed by `(component, name)`. The component is load-bearing: a `block` row
/// claims a worker was OCCUPIED that long, a `section` row only claims wall
/// time, which for an `async` body includes awaits that gave the worker back.
static SECTION_STATS: LazyLock<dashmap::DashMap<(&'static str, &'static str), SectionStat>> = LazyLock::new(dashmap::DashMap::new);

fn record_section(component: &'static str, name: &'static str, elapsed: Duration) {
    let us = elapsed.as_micros() as u64;
    let entry = SECTION_STATS.entry((component, name)).or_default();
    entry.count.fetch_add(1, Relaxed);
    entry.total_us.fetch_add(us, Relaxed);
    entry.max_us.fetch_max(us, Relaxed);
}

/// Times a section that occupies a runtime worker without yielding — a
/// `std::sync::Mutex` hold, a synchronous rebuild. **Not for `async` bodies**:
/// use [`TimedSection`] there, which makes no occupancy claim.
pub struct BlockWatch(&'static str, std::time::Instant);

/// Wall time of a named section, awaits included — no claim that a worker was held.
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

/// jemalloc's arena accounting, in bytes: `(allocated, active, resident, mapped,
/// retained)`; `resident - allocated` is fragmentation. `None` off Linux or
/// without `--features profiling`, where jemalloc is not the allocator.
#[cfg(all(feature = "profiling", target_os = "linux"))]
pub fn jemalloc_bytes() -> Option<(u64, u64, u64, u64, u64)> {
    use tikv_jemalloc_ctl::{epoch, stats};
    // jemalloc's stats are cached; without advancing the epoch every sample
    // returns the values from process start.
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
#[derive(derive_more::Deref, derive_more::DerefMut)]
pub struct Watched<T> {
    #[deref]
    #[deref_mut]
    inner: T,
    _watch: BlockWatch,
}

impl<T> Watched<T> {
    pub fn new(name: &'static str, inner: T) -> Self {
        Self { inner, _watch: BlockWatch::new(name) }
    }
}

/// Generates the no-attribute "increment by one" recorders. `mirror
/// STATIC.field` additionally bumps a process-global atomic, which — unlike an
/// OTel counter — is readable back in-process by `timefusion_stats` and tests.
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

/// One dashboard aggregate answered from a configured rollup.
pub fn record_rollup_hit(mode: &'static str, grain: &str) {
    let stats = maintenance_stats();
    if mode == "hybrid" { &stats.rollup_hits_hybrid } else { &stats.rollup_hits_full }.fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.rollup_hits.add(1, &[KeyValue::new("mode", mode), KeyValue::new("grain", grain.to_string())]);
    }
}

/// True on the first, and then every `ROLLUP_MISS_SAMPLE`th, miss under `key` —
/// for logging one refused plan with context. Budgeted PER KEY, so a rare miss
/// reason stays diagnosable regardless of how noisy the other reasons are.
pub fn sample_rollup_miss(key: &'static str) -> bool {
    const ROLLUP_MISS_SAMPLE: u64 = 64;
    static SEEN: LazyLock<dashmap::DashMap<&'static str, AtomicU64>> = LazyLock::new(dashmap::DashMap::new);
    // `fetch_add` returns the count BEFORE the increment, so a key's first miss always samples.
    SEEN.entry(key).or_default().fetch_add(1, Relaxed).is_multiple_of(ROLLUP_MISS_SAMPLE)
}

/// One dashboard aggregate that fell through to a raw scan. Takes the REASON,
/// not its label, so the exhaustive match below fails the build on a new variant
/// instead of silently bucketing it.
pub fn record_rollup_miss(reason: crate::rollup::MissReason) {
    use crate::rollup::MissReason as R;
    let stats = maintenance_stats();
    stats.rollup_misses_total.fetch_add(1, Relaxed);
    match reason {
        R::NotBuilt => &stats.rollup_miss_not_built,
        R::StaleCoverage => &stats.rollup_miss_stale_coverage,
        R::TinyInterior => &stats.rollup_miss_tiny_interior,
        R::TooManyBranches => &stats.rollup_miss_too_many_branches,
        R::UnsupportedShape => &stats.rollup_miss_unsupported,
        R::IncompleteCoverage => &stats.rollup_miss_incomplete_coverage,
        R::UnknownFilter => &stats.rollup_miss_unknown_filter,
        R::FilterNotEligible => &stats.rollup_miss_filter_not_eligible,
        R::FilterMultipleNullGuards => &stats.rollup_miss_filter_multiple_null_guards,
        R::FilterNullGuardMismatch => &stats.rollup_miss_filter_null_guard_mismatch,
        R::MissingMeasure => &stats.rollup_miss_missing_measure,
        R::PartialBucket => &stats.rollup_miss_unaligned_bucket,
        R::UnknownGroupBy => &stats.rollup_miss_unknown_group_by,
        R::MissingProject => &stats.rollup_miss_missing_project,
        R::UnboundedTime => &stats.rollup_miss_unbounded_time,
        R::NonDecomposableAggregate => &stats.rollup_miss_non_decomposable,
        R::RewriteSchemaMismatch => &stats.rollup_miss_rewrite_schema_mismatch,
        R::UnwalkableSource => &stats.rollup_miss_unwalkable_source,
        R::MeasureNotStored => &stats.rollup_miss_measure_not_stored,
    }
    .fetch_add(1, Relaxed);
    if let Some(m) = METRICS.get() {
        m.rollup_misses.add(1, &[KeyValue::new("reason", reason.label())]);
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

/// Declares a process-global atomic-counter struct together with its all-zero
/// static AND its `timefusion_stats` rows, so a counter cannot be declared
/// without being exposed. The row key defaults to the field name; `field as
/// "key"` pins a different one (usually a `_total` suffix).
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
                    self.$field.load(Relaxed),
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
        /// Delta merges executed by coalescer drains.
        coalesce_merges,
        /// Groups parked to `<wal_dir>/quarantine/dml`.
        coalesce_quarantined,
    }
}

pub fn dml_stats() -> &'static DmlStats {
    &DML_STATS
}

atomic_stats! {
    /// Maintenance counters for the `timefusion_stats` view — the OTel counters
    /// above can't be read back in-process. Monotonic unless noted as a gauge.
    #[derive(Default)]
    MaintenanceStats => MAINTENANCE_STATS as "maintenance" {
        checkpoints_created,
        /// Durable journal commits performed, and the callers that rode someone
        /// else's `fsync` instead (see `support::GroupCommit`). Both gauges.
        /// `coalesced / (performed + coalesced)` is the share of the durability
        /// barrier that costs no IO.
        journal_commits,
        journal_commits_coalesced,
        checkpoint_failed,
        /// Checkpoints that wrote OK but failed post-write footer verification.
        /// Log cleanup is withheld so the JSON log stays recoverable. PAGE if > 0.
        checkpoint_corrupt,
        log_files_cleaned,
        log_cleanup_failed,
        // Gauge: max version lag (current - last checkpointed) at the last tick.
        // Large and growing means the checkpoint task is failing or wedged.
        checkpoint_lag_versions,
        // NONZERO = committed parquet was destroyed elsewhere. PAGE and investigate.
        dangling_removed,
        reconcile_failed,
        dedup_timed_out as "dedup_timed_out_total",
        dedup_failed as "dedup_failed_total",
        /// Adds a rewrite planner had to drop because the in-memory snapshot listed
        /// the same file twice. Nonzero means reads over that table double-count
        /// rows — the file list diverged from the log. PAGE if > 0.
        snapshot_duplicate_adds,
        light_optimize_timed_out as "light_optimize_timed_out_total",
        /// Units PARKED on a deterministic plan error (a spec naming a column its
        /// source files lack) rather than bisected — retrying cannot fix these.
        maintenance_schema_parked as "maintenance_schema_parked_total",
        light_optimize_failed as "light_optimize_failed_total",
        /// Ticks that hit the wall-clock budget with hot projects still pending.
        light_optimize_tick_truncated as "light_optimize_tick_truncated_total",
        /// Wave-engine per-tick accounting. `planned` counts projects the tick's
        /// single metadata walk found work for; `completed` counts bins that landed.
        /// ALERT when completed lags planned for N consecutive ticks.
        light_optimize_projects_planned as "light_optimize_projects_planned_total",
        light_optimize_projects_completed as "light_optimize_projects_completed_total",
        light_optimize_bins_committed as "light_optimize_bins_committed_total",
        light_optimize_waves_committed as "light_optimize_waves_committed_total",
        /// GAUGE: repair bins sorting right now — tells "repair is grinding" from
        /// "repair is wedged" between the sparse per-bin log events.
        repair_bins_in_flight,
        /// Dedup-engine waves (data_change: true) — counted separately so the
        /// light_optimize_* counters mean pure compaction only.
        dedup_bins_committed as "dedup_bins_committed_total",
        dedup_waves_committed as "dedup_waves_committed_total",
        /// Dedup bins STAGED via the deletion-vector path (marks losers with a
        /// DV bitmap instead of rewriting whole files).
        dv_dedup_bins_staged as "dv_dedup_bins_staged_total",
        /// Dedup slices NOT re-pended at reconcile because their commit was a
        /// self-authored DV-dedup wave (`timefusion.dv_dedup`).
        dedup_remint_skipped as "dedup_remint_skipped_total",
        /// Rollup slices NOT re-minted because the only commits touching the hour
        /// were self-authored DV-dedup waves: such a wave moves neither the
        /// partition stats fingerprint nor `rollup_source_epochs`, and the base
        /// build already reads its raw input deduped, so a rebuild would be
        /// byte-identical.
        rollup_remint_skipped as "rollup_remint_skipped_total",
        /// Rollup units COMPLETED without rebuilding because their input file set
        /// (deletion vectors included) was unchanged since the live slice coverage
        /// was published. `rollup_remint_skipped` prevents a task being created;
        /// this catches the ones created anyway and proves the rebuild redundant at
        /// claim time. Read against `rollup_staged_projects_total`: the two sum to
        /// the units claimed.
        rollup_noop_rebuild_skipped as "rollup_noop_rebuild_skipped_total",
        /// Rollup slice witnesses repaired in place across a landed dedup,
        /// rather than invalidated.
        ///
        /// A dedup drops exactly the rows the rollup's deduplicated read never
        /// counted, so its numbers are unchanged and only the physical
        /// `num_records` witness moved — by `dropped`, which the commit site
        /// knows exactly. Read against `rollup_stale_shrank`: this is the class
        /// that no longer has to be rebuilt. Prod 2026-09-13 sized it at 18,418
        /// shrank against 229,361 grew, so a large `grew` with this counter
        /// climbing is the EXPECTED shape, not a failure of the carry.
        rollup_witness_carried as "rollup_witness_carried_total",
        /// Times the repair lane's reserved light permits were lent to hygiene
        /// while repair had no pending work, and reclaimed when it did. Read
        /// together: a lend with no matching return while `pending_repair` is
        /// non-zero means the reclaim is not firing.
        repair_holdback_lends as "repair_holdback_lends_total",
        repair_holdback_returns as "repair_holdback_returns_total",
        /// How many times the maintenance gauges were actually recomputed.
        /// `publish_statistics` is a full linear scan of the journal under the one
        /// global `Mutex<TaskJournal>`, so it is throttled: this should sit near
        /// one per second however busy maintenance gets. Tracking the checkpoint
        /// rate instead means the throttle is not firing.
        journal_stats_publishes as "journal_stats_publishes_total",
        /// Finished tasks dropped from the journal because their slice is past the
        /// abandonment horizon. If this climbs while `tasks_complete` does not
        /// fall, something is re-creating the keys being pruned.
        journal_retired_tasks_pruned as "journal_retired_tasks_pruned_total",
        /// Base files a DERIVED unit refused for an obsolete generation, split by
        /// whether refusing them cost anything. `reproduced`: the current-generation
        /// files already cover the refused file's whole span, so no rebuild is
        /// demanded. `unreproduced`: a genuine hole, so the unit mints the base
        /// rebuild and retries. Rising `reproduced` with a flat
        /// `rollup_tier_untagged_found` means obsolete tier files are accumulating.
        rollup_base_refusal_reproduced as "rollup_base_refusal_reproduced_total",
        rollup_base_refusal_unreproduced as "rollup_base_refusal_unreproduced_total",
        /// Rollup-journal writes performed, and the ones skipped because the
        /// encoded content had not moved since the last successful store. `store`
        /// costs two `fsync`s inside every group commit, so a healthy system —
        /// where steady ingest re-invalidates the same hours idempotently — skips
        /// most of them; read `skipped / (skipped + persists)`.
        rollup_journal_persists as "rollup_journal_persists_total",
        rollup_journal_persist_skipped as "rollup_journal_persist_skipped_total",
        /// Writes DEFERRED because the journal had changed but was written less
        /// than `ROLLUP_JOURNAL_MAX_STALENESS` ago. Deferred, never dropped: the
        /// next commit past the window writes it, and shutdown forces one.
        rollup_journal_persist_deferred as "rollup_journal_persist_deferred_total",
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
        /// Zero is the healthy state and the PRECONDITION for enabling
        /// `version_append` on a busy table — one sort per file group over a wide
        /// scan is an OOM shape. Nonzero means the partition carries files without
        /// an honest sorted footer.
        mor_delta_leg_sorts as "mor_delta_leg_sorts_total",
        /// Escalated flush sorts that FAILED and wrote their group unsorted. One
        /// unsorted file disables the reader's footer ordering for every scan
        /// touching its partition (query-time SortExec, unordered MOR dedup).
        flush_sort_unsorted_fallbacks as "flush_sort_unsorted_fallbacks_total",
        /// Files marked verified-sorted BY THE WRITE that produced them, rather than
        /// by reading their footer back — each is a ranged read footer repair does
        /// not pay. 0 on a busy process means the marking is not reaching the commit
        /// path, which looks identical to a working feature with nothing to do.
        repair_sorted_at_write as "repair_sorted_at_write_total",
        /// Rounds where the WAL-backlog brake DEGRADED the wave to the one-project
        /// service floor (instead of stopping the tick). Chronic nonzero = ingest is
        /// outrunning flush often enough that compaction is running at the floor.
        light_optimize_ticks_degraded as "light_optimize_ticks_degraded_total",
        /// Repair ticks skipped because ANOTHER table's repair pass held the
        /// process-wide permit. The light pool is shared across tables while the
        /// wave engine's concurrency cap is per-table, so without this guard two
        /// repair sorts co-exist and starve each other.
        repair_ticks_yielded,
        /// Packing/consolidation turns that declined to CLAIM because no
        /// `light_rewrite_sem` permit was free. A saturation gauge, not a fault:
        /// the alternative is claiming anyway and blocking inside `stage_hot_bin`
        /// until the deadline, committing nothing.
        compaction_permits_unavailable,
        /// Packing/consolidation turns that DID take a `light_rewrite_sem` permit —
        /// the denominator a refusal count needs. Read
        /// `acquired / (acquired + unavailable)`.
        compaction_permits_acquired,
        /// Units killed by the absolute lifetime cap rather than by going idle —
        /// see `coordinator_operation_lifetime_cap`. These were making progress and
        /// still not converging; if this tracks the claim rate, units are being
        /// bisected too slowly.
        maintenance_unit_lifetime_capped,
        /// `light_rewrite_sem` permits free, sampled whenever a hygiene turn asks
        /// for one, and the TOTAL the semaphore was built with. Refusal and success
        /// counts alone cannot say whether permits are HELD or simply do not exist,
        /// and those need opposite fixes. `light_rewrite_permits_total` is derived
        /// at boot from `coordinator_share / COORDINATOR_PER_SORT_BUDGET -
        /// repair_holdback`, floored at 1 — never derive it by hand.
        light_rewrite_permits_available,
        /// Longest a hygiene permit has been held WITHOUT reaching the sort, this
        /// process. The wedge detector: the lane holds both permits and claims
        /// nothing, so every throughput counter simply stops rather than reading
        /// as bad, and a stopped counter is indistinguishable from a quiet one.
        permit_held_without_staging_secs,
        light_rewrite_permits_total,
        /// Dashboard aggregates served from a rollup, split by how much of the
        /// window the rollup owned. `rollup_hits_hybrid` is what proves the
        /// raw-fringe union works, since a full-window hit needs no union.
        rollup_hits_full as "rollup_hits_full_total",
        rollup_hits_hybrid as "rollup_hits_hybrid_total",
        /// Partitions rebuilt from only the hours that changed, vs from scratch.
        /// Incremental falling to zero means something is widening the dirty set to
        /// the whole day.
        rollup_rebuilds_incremental as "rollup_rebuilds_incremental_total",
        rollup_rebuilds_full as "rollup_rebuilds_full_total",
        rollup_dirty_partitions,
        /// Derived slices completed WITHOUT publishing because a strictly wider live
        /// file already covered them. Expected to be rare; if it is not, a late row
        /// inside an already-published day may be going stale in the coarse tier.
        rollup_skipped_covered_by_wider,
        /// Units that published ZERO rows while the tier they aggregate had published
        /// rows over the SAME slice — and were then marked `complete`, so nothing
        /// revisits them. Empty propagates: `rollup_slice_coverage` records an empty
        /// publication as covered, so the next tier up sees no hole and freezes too.
        rollup_published_empty_over_full_base,
        /// Base files a DERIVED unit refused although they carry slice tags, split by
        /// which test refused them. `rollup_untagged_inputs` counts only files with
        /// NO tags, so without these two a unit can skip every input it has while
        /// that counter reads 0.
        rollup_base_file_skipped_tag_project,
        rollup_base_file_skipped_tag_range,
        /// Relevant base files whose materialization generation could not be proved.
        /// Each refusal schedules base rebuilding and retries the derived unit.
        rollup_base_file_skipped_generation,
        /// Persisted coverage entries rejected during restart generation validation.
        rollup_ledger_seed_rejected_generation,
        /// Splits refused because the unit measured nearly what its parent measured:
        /// bisection has hit the row-group floor and halving again buys nothing but
        /// journal units. Rising while `pending_base_rollup` also rises means units
        /// are being declined and then failing to run.
        split_declined_at_floor,
        /// Splits refused for the OTHER reason: bisection produced one child, or a
        /// child that would need hash sharding, so there is no width to split to.
        /// Together with `split_declined_at_floor` this is the complete set of
        /// reasons a unit that cannot finish also cannot be made smaller.
        split_declined_no_width,
        /// Dedup keys whose versions DISAGREE on a column declared immutable.
        /// Immutability is enforced for UPDATE only, so an INSERT can append a
        /// disagreeing version — and read filters on immutable columns are pushed
        /// BELOW the dedup on the strength of that declaration. Non-zero means a
        /// pushed predicate can match a version the winner does not satisfy. Read
        /// with `immutable_audit_shards_total`, which says whether it ran at all.
        immutable_column_disagreement_total,
        /// Dedup shards whose collapse ran with the immutable audit ARMED — the
        /// denominator for `immutable_column_disagreement_total`.
        /// `RunCollapse::with_immutable_audit` resolves columns by name and disarms
        /// itself when none resolve, so zero here means the audit is not running.
        immutable_audit_shards_total,
        /// Partitions where the coverage ledger and the Delta tags disagree. Must be
        /// zero before any read path trusts the ledger; non-zero afterwards means
        /// queries may be answered from coverage that is not there.
        coverage_ledger_disagreements,
        /// Ledger writes that did not reach disk. `store_sidecar` warns and
        /// continues, so the in-memory ledger keeps serving while the durable copy
        /// falls behind. Must read ZERO alongside `coverage_ledger_disagreements`
        /// before the Delta tags can be removed.
        coverage_ledger_persist_failures,
        /// Base rollup files carrying no parseable slice tags — history written
        /// before tagging existed. Counts files SELECTED by the fallback (pruned on
        /// their own timestamp statistics), so it measures how much of the base tier
        /// predates tagging, not how much is unreachable.
        rollup_untagged_inputs,
        /// Untagged files found LIVE IN A TIER at publish time (gauge, overwritten
        /// per unit), and the running total this publish path has retired. A tier
        /// file with no identity tags is skipped by the replace-set, so every
        /// rebuild stacks another version of every `id` beside it: `found` is
        /// alarmable at > 0, and `retired` is the proof a rebuild REMOVED the old
        /// file rather than publishing a correct one beside it.
        rollup_tier_untagged_found,
        rollup_tier_untagged_retired as "rollup_tier_untagged_retired_total",
        /// Recovered slices carrying NO row witness — published before
        /// `TAG_SOURCE_ROWS` existed. Every read refuses them `stale_coverage` and no
        /// rule can rescue them, so this is the size of the backlog that must
        /// republish before wide dashboards route. Set from the whole hourly
        /// recovery pass, so 0 means there genuinely are none.
        rollup_witnessless_slices,
        /// Contiguous sealed days of rollup coverage, counting back from yesterday,
        /// minimised over every (project, declared tier). A 30d panel needs 30
        /// CONTIGUOUS days in the tier it reads, so one hole anywhere sends it to a
        /// raw scan — `MIN(date)` would read as progress while the middle stays
        /// holey. Minimised, not averaged: one uncovered project is a slow dashboard.
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
        /// TIMEFUSION_TANTIVY_BACKFILL_MAX_FILE_MB. Gauges: each pass overwrites
        /// them. `uncovered` trending to 0 IS the definition of a converged reindex.
        tantivy_uncovered_files,
        tantivy_oversized_skipped,
        /// Pending (non-Complete) tasks split by operation, and the subset that is
        /// ELIGIBLE right now (deadline passed). Gauges, republished each checkpoint.
        /// `tasks_pending` alone cannot distinguish work that is absent from work
        /// that is present-but-not-eligible from work that is out-competed.
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
        /// slice ended within `STARVATION_HORIZON_MICROS` — so a reading near that
        /// bound is a real stall inside the goal window. `beyond_horizon_tasks` is
        /// the deliberately-abandoned remainder; without it, narrowing the age gauge
        /// is indistinguishable from hiding the debt.
        maintenance_oldest_task_age_secs as "oldest_task_age_seconds",
        maintenance_beyond_horizon_tasks as "beyond_horizon_tasks",
        maintenance_eligible_watermark_lag_secs as "eligible_watermark_lag_seconds",
        maintenance_processed_bytes as "processed_bytes_total",
        maintenance_processed_bytes_per_sec as "processed_bytes_per_second",
        maintenance_raw_tail_duration_secs as "raw_tail_duration_seconds",
        sealed_compaction_debt_bytes,
        /// How often a unit target WOULD be (or was) shrunk because its lane's pool
        /// was over half full, and how many bytes that withheld. Emitted even when
        /// `timefusion_maintenance_pressure_scaling` is off, so the flag can be
        /// decided from data.
        pressure_scale_engaged as "pressure_scale_engaged",
        pressure_scale_bytes_withheld as "pressure_scale_bytes_withheld",
        /// Packing bins whose bytes-per-file-eliminated exceeded the value floor
        /// (counted even when the floor is 0/off, so it can be chosen from data).
        pack_value_refused as "pack_value_refused",
        pack_value_refused_rows as "pack_value_refused_rows",
        maintenance_cpu_tokens_used as "cpu_tokens_used",
        maintenance_decoded_bytes_used as "decoded_bytes_used",
        maintenance_object_read_tokens_used as "object_read_tokens_used",
        maintenance_object_write_tokens_used as "object_write_tokens_used",
        /// Aggregates that fell through to a raw scan, plus the breakdown by reason —
        /// the reason is the only thing that distinguishes "never built" from "the
        /// source moved under it" from "unsupported shape".
        rollup_misses_total,
        rollup_miss_not_built as "rollup_miss_not_built_total",
        rollup_miss_stale_coverage as "rollup_miss_stale_coverage_total",
        rollup_miss_tiny_interior as "rollup_miss_tiny_interior_total",
        rollup_miss_too_many_branches as "rollup_miss_too_many_branches_total",
        rollup_miss_unsupported as "rollup_miss_unsupported_total",
        rollup_miss_incomplete_coverage as "rollup_miss_incomplete_coverage_total",
        rollup_miss_unknown_filter as "rollup_miss_unknown_filter_total",
        rollup_miss_measure_not_stored as "rollup_miss_measure_not_stored_total",
        rollup_miss_filter_not_eligible as "rollup_miss_filter_not_eligible_total",
        rollup_miss_filter_multiple_null_guards as "rollup_miss_filter_multiple_null_guards_total",
        rollup_miss_filter_null_guard_mismatch as "rollup_miss_filter_null_guard_mismatch_total",
        rollup_miss_missing_measure as "rollup_miss_missing_measure_total",
        rollup_miss_unaligned_bucket as "rollup_miss_unaligned_bucket_total",
        rollup_miss_unknown_group_by as "rollup_miss_unknown_group_by_total",
        rollup_miss_missing_project as "rollup_miss_missing_project_total",
        rollup_miss_unbounded_time as "rollup_miss_unbounded_time_total",
        rollup_miss_non_decomposable as "rollup_miss_non_decomposable_total",
        rollup_miss_rewrite_schema_mismatch as "rollup_miss_rewrite_schema_mismatch_total",
        rollup_miss_unwalkable_source as "rollup_miss_unwalkable_source_total",
        /// Derived units retried because their BASE tier does not cover the slice
        /// they were asked to build. Publishing anyway would trust a short cell
        /// permanently, since the witness is the RAW partition. Read as a RATE:
        /// rising while the base tier publishes nothing means the base is stuck.
        rollup_derived_base_incomplete as "rollup_derived_base_incomplete_total",
        dirty_bin_queue_depth,
        dirty_bin_enqueued as "dirty_bin_enqueued_total",
        dirty_bin_eligible as "dirty_bin_eligible_total",
        dirty_bin_processed as "dirty_bin_processed_total",
        dirty_bin_requeued as "dirty_bin_requeued_total",
        /// Queued bins consumed by the whole-date BATCH probe without per-bin
        /// staging — every flushed bin is enqueued, so most carry no duplicates.
        /// Also counted in `dirty_bin_processed`.
        dirty_bin_batch_probe_clean as "dirty_bin_batch_probe_clean_total",
        dirty_bin_dropped_rows as "dirty_bin_dropped_rows_total",
        dirty_bin_rewrite_duration_ms as "dirty_bin_rewrite_duration_ms_total",
        /// Cold-owned dirty bins (date old enough that the nightly consolidate owns
        /// the partition) DEPRIORITIZED to the tail of a drain pass and left on the
        /// queue. NOT dropped: consolidate bin-packs but does not collapse
        /// duplicates, so the dirty-bin drain stays their only physical dedup.
        /// With the default `cold_optimize_after_days = 1` every drainable bin is
        /// cold-owned, so this reads as "queued bins this pass had no batch slot for".
        dedup_bins_deferred_cold as "dedup_bins_deferred_cold_total",
        /// Drain passes skipped (or cut short between chunks) because the flush path
        /// was behind. Dedup is an optimization — read-side DedupExec keeps results
        /// correct — so it yields to persistence. Chronic nonzero = flush is
        /// unhealthy, not dedup.
        dedup_passes_flush_yields as "dedup_passes_flush_yields_total",
        /// Per-bin STAGING attempts killed at the deadline and requeued, so one hung
        /// object-store read cannot wedge the drain behind the maintenance semaphore.
        /// Repeated hits are the same oversized bin retrying, not noise.
        dedup_bin_stage_timeouts as "dedup_bin_stage_timeouts_total",
        /// Batch PROBES that did not get a slice of the phase budget — a different
        /// event from the staging deadline above, and counted apart from it because
        /// conflating the two makes both uninterpretable. The probe is the much
        /// cheaper proof that a partition is already duplicate-free; a timeout only
        /// means its bins take the expensive path this tick. Expected nonzero while
        /// a group backlog drains, zero once it has.
        dedup_probe_timeouts as "dedup_probe_timeouts_total",
        /// Wave (dedup / light-optimize) commits that STOOD DOWN rather than queue on
        /// a per-table commit lock a flush was already waiting for. The bins are
        /// requeued and re-staged, so this is deferred work, not lost work. Chronic
        /// nonzero = flush is saturating the commit path and crowding out compaction.
        wave_commits_yielded_to_flush as "wave_commits_yielded_to_flush_total",
        /// Boot-time resume of a staged-but-uncommitted footer-repair bin, so the
        /// next pass doesn't redo the rewrite. See `resume_staged_intents`.
        repair_resumed as "repair_resumed_total",
        /// Rollup units COMMITTED at claim time from output a previous process
        /// staged, instead of re-running the scan. Read against
        /// `rollup_resume_declined` to tell rescued work from a moving source.
        rollup_resumed as "rollup_resumed_total",
        /// Staged rollup outputs refused — the source moved, an input left the
        /// snapshot, another live file already covers the slice, or the parquet is
        /// short. All correct refusals; a rising count means the staging window and
        /// the churn window overlap, not that resume is broken.
        rollup_resume_declined as "rollup_resume_declined_total",
        /// Claims that found NO staged intent for the unit at all (no manifest, or no
        /// entry naming this task) — the denominator that tells a zero
        /// `rollup_resumed` apart from "resume was never offered a candidate".
        rollup_resume_no_intent as "rollup_resume_no_intent_total",
        /// Candidates held back by the ownership guard (see `resume_guarded`) or
        /// belonging to another table. Nonzero with `rollup_resumed` at 0 means the
        /// guard, not the evidence, is what forfeits the work.
        rollup_resume_skipped as "rollup_resume_skipped_total",
        /// Staged rollup output whose Delta commit had already landed — only the
        /// journal publication was lost. Counted apart from `rollup_resumed` because
        /// it rescues bookkeeping, not the scan.
        rollup_resume_already_landed as "rollup_resume_already_landed_total",
        /// Repair equivalents of the two above; same reading.
        repair_resume_skipped as "repair_resume_skipped_total",
        repair_resume_already_landed as "repair_resume_already_landed_total",
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
        repair_resume_row_mismatch as "repair_resume_row_mismatch_total",
        /// Cron ticks skipped because the previous run of the same job was still
        /// in flight. A steadily growing value = a wedged/overlong job body.
        cron_ticks_skipped,
        /// Cron fires actually dispatched (all jobs). Frozen while uptime grows =
        /// the scheduler is dead.
        cron_ticks_fired,
        /// Cron runs that exceeded the long-running warning threshold. Slow but
        /// progressing work is allowed to finish; sustained nonzero with no
        /// completion = wedged.
        cron_long_running as "cron_long_running_total",
        /// Ingest-time client-retry dedup: rows DROPPED because their exact
        /// client-visible content was provably already committed. A far-too-high
        /// dropped/rows_ingested ratio means it is misfiring on legitimate version
        /// traffic; a permanent zero means the hash point has drifted inert.
        ingest_dedup_dropped_rows as "ingest_dedup_dropped_rows_total",
        /// Probes whose dedup KEY matched a flushed row (content match or not).
        /// key_hits >> dropped_rows = version traffic, not retries.
        ingest_dedup_key_hits as "ingest_dedup_key_hits_total",
        /// Gauge: live identity entries across every per-table index (both epochs).
        ingest_dedup_index_entries,
        ingest_dedup_epoch_rotations as "ingest_dedup_epoch_rotations_total",
    }
}

pub fn maintenance_stats() -> &'static MaintenanceStats {
    &MAINTENANCE_STATS
}

/// Resident set size of this process in bytes from `/proc/self/statm`
/// (Linux only; `None` elsewhere).
pub fn process_rss_bytes() -> Option<usize> {
    // statm fields are in pages; resident is field 2. 4 KiB pages assumed.
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    statm.split_whitespace().nth(1)?.parse::<usize>().ok().map(|pages| pages * 4096)
}

#[cfg(test)]
mod runtime_lag_tests {
    use std::sync::atomic::Ordering::Relaxed;

    use super::{RUNTIME_LAG_LAST_MS, RUNTIME_LAG_MAX_MS, spawn_runtime_lag_sampler};

    /// The sampler reports near-zero on an idle runtime and stops on cancel.
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

/// Max spans/logs per OTLP export message. Spans/logs embed full query text, so
/// the SDK default (512) overflows the collector's 4MB gRPC limit.
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

    // Span export honors the standard OTEL_TRACES_EXPORTER=none switch. The batch
    // is capped at EXPORT_BATCH because opentelemetry-otlp 0.31 cannot raise the
    // gRPC message-size limit through the public API.
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

    // OTLP logs: bridge tracing events to the collector. The bridge must not
    // observe the exporter's own tracing output — tonic/hyper events emitted
    // inside an export would recurse into another export.
    let log_exporter = opentelemetry_otlp::LogExporter::builder().with_tonic().with_endpoint(otlp_endpoint).with_timeout(EXPORT_TIMEOUT).build()?;
    // Logs carry full SQL text, so cap the log batch too.
    let log_processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter)
        .with_batch_config(opentelemetry_sdk::logs::BatchConfigBuilder::default().with_max_export_batch_size(EXPORT_BATCH).build())
        .build();
    let logger_provider = SdkLoggerProvider::builder().with_log_processor(log_processor).with_resource(resource).build();
    let log_bridge = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider)
        .with_filter(tracing_subscriber::filter::filter_fn(|meta| !["opentelemetry", "tonic", "h2", "hyper"].iter().any(|p| meta.target().starts_with(p))));
    let _ = LOGGER_PROVIDER.set(logger_provider);

    // Tantivy emits an INFO event per segment operation, which floods stdout/OTLP.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tantivy=warn"));

    let fmt_layer = tracing_subscriber::fmt::layer().with_target(true).with_thread_ids(true).with_thread_names(true);
    let fmt_layer = if config.is_json_logging() { fmt_layer.json().boxed() } else { fmt_layer.boxed() };

    Registry::default().with(env_filter).with(telemetry_layer).with(log_bridge).with(fmt_layer).try_init().context("failed to set tracing subscriber")?;

    info!("OpenTelemetry initialized successfully with service name: {}", service_name);

    Ok(())
}

pub fn shutdown_telemetry() {
    info!("Shutting down OpenTelemetry");
    // Tracer/meter providers shut down on drop; buffered logs need an explicit flush.
    let _ = LOGGER_PROVIDER.get().map(SdkLoggerProvider::shutdown);
}

/// Cell-capped preview formatter for datafusion-tracing spans. Cell values here
/// are unbounded (Variant/JSON bodies, whole bind arrays), so each cell's
/// `Display` is aborted after `PREVIEW_CELL_CAP` bytes and never materialized in
/// full — unlike the crate's `default_preview_fn`.
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

    /// The two maps share one bounded adder; a key collision would mix retry
    /// counts into work counts.
    #[test]
    fn work_and_retry_counters_accumulate_under_their_own_prefixes() {
        count_maintenance_work("Dedup", "rows_dropped", 7);
        count_maintenance_work("Dedup", "rows_dropped", 5);
        count_maintenance_retry("Dedup", "worker_error: out of memory (pool)");
        let work = maintenance_work_rows();
        assert!(work.contains(&("work.Dedup.rows_dropped".to_owned(), 12)), "amounts must SUM, not count events: {work:?}");
        // The retry key is the reason's head, and lives in the other map.
        assert!(maintenance_retry_rows().contains(&("retry.Dedup.worker_error".to_owned(), 1)));
        assert!(work.iter().all(|(key, _)| !key.starts_with("work.Dedup.worker_error")), "a retry must not land in the work map: {work:?}");
    }

    /// A cell holding a huge value must preview as a bounded prefix.
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

    /// One rare miss class must not have its sample budget spent by a common one.
    #[test]
    fn the_miss_sampler_budgets_each_reason_separately() {
        let rate = |key| (0..256).filter(|_| sample_rollup_miss(key)).count();
        let quiet = rate("test.quiet");
        // A noisy neighbour: with one shared counter it would shift the next key's phase.
        (0..1000).for_each(|_| {
            sample_rollup_miss("test.noisy");
        });
        assert_eq!(rate("test.rare"), quiet, "each reason must get its own budget, whatever the other reasons did");
        assert!(quiet >= 4, "a divisor that renders <4 plans in 256 misses is the ~5-hours-per-line rate measured on prod");
    }
}

// ===== profiling =====
// Self-instrumented heap + CPU profiling, compiled only under
// `--features profiling` (Linux-only deps). Heap dumps come from jemalloc's own
// profiler (configured by the baked `malloc_conf` in `main.rs`); CPU comes from a
// `pprof` sampler. Both write into `<data_dir>/profiles`.

#[cfg(all(feature = "profiling", target_os = "linux"))]
mod imp {
    use std::{path::PathBuf, time::Duration};

    use tracing::{info, warn};

    /// Ensure the artifact dir exists and spawn the pruner + CPU sampler. Call
    /// once at boot; heap profiling is already active via the baked `malloc_conf`.
    pub fn start(data_dir: PathBuf) {
        // MUST equal the parent of the baked jemalloc `prof_prefix` in main.rs:
        // jemalloc does not mkdir its prefix, and silently drops every dump if
        // the directory is missing.
        let dir = data_dir.join("profiles");
        if let Err(e) = std::fs::create_dir_all(&dir) {
            warn!("profiling: cannot create {dir:?}: {e} — CPU flamegraphs disabled, heap dumps still land at malloc_conf prof_prefix");
        }
        archive_prekill_dumps(&dir);
        // The CPU sampler is signal-handler + libunwind code, so it is disabled by
        // env rather than by a rebuild. Heap-dump pruning must stay OUTSIDE it:
        // the sampler can be off while jemalloc is still dumping.
        spawn_heap_pruner(dir.clone());
        if std::env::var("TIMEFUSION_CPU_PROFILE").is_ok_and(|v| v.eq_ignore_ascii_case("false") || v == "0") {
            info!("profiling: jemalloc heap auto-dump only — CPU sampler disabled by TIMEFUSION_CPU_PROFILE → {dir:?}");
            return;
        }
        info!("profiling: enabled (jemalloc heap auto-dump + rolling CPU flamegraph) → {dir:?}");
        spawn_cpu_sampler(dir);
    }

    /// Cap the jemalloc heap dumps (it writes one every `lg_prof_interval` bytes
    /// and never removes any). Its own thread on purpose: the CPU sampler, whose
    /// loop would otherwise host this, can be disabled.
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

    /// One CPU profile window at a time on a dedicated OS thread: build a guard,
    /// sample for `WINDOW`, write a flamegraph, drop, repeat. A fresh guard per
    /// window keeps each SVG scoped to a recent interval.
    fn spawn_cpu_sampler(dir: PathBuf) {
        const HZ: i32 = 99; // 99Hz: cheap, avoids lock-step with periodic timers
        const WINDOW: Duration = Duration::from_secs(60);
        const KEEP_CPU: usize = 10;
        std::thread::Builder::new()
            .name("cpu-profiler".into())
            .spawn(move || {
                let mut seq: u64 = 0;
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
                    prune_old(&dir, "cpu-", KEEP_CPU);
                    seq += 1;
                }
            })
            .expect("spawn cpu-profiler thread");
    }

    /// Move the previous process's newest heap dumps into `prekill-<stamp>/` so
    /// this process's pruner cannot evict the evidence of an OOM kill. Keeps the
    /// 3 newest archives.
    fn archive_prekill_dumps(dir: &std::path::Path) {
        let dumps = newest_first(dir, |n| n.starts_with("jeprof") && n.ends_with(".heap"));
        let Some((newest, _)) = dumps.first() else { return };
        let stamp = newest.duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let arch = dir.join(format!("prekill-{stamp}"));
        if std::fs::create_dir_all(&arch).is_err() {
            return;
        }
        dumps.iter().take(5).for_each(|(_, p)| {
            if let Some(name) = p.file_name() {
                let _ = std::fs::rename(p, arch.join(name));
            }
        });
        // Drop the rest so the rolling pruner starts clean for this process.
        dumps.into_iter().skip(5).for_each(|(_, old)| {
            let _ = std::fs::remove_file(old);
        });
        newest_first(dir, |n| n.starts_with("prekill-")).into_iter().skip(3).for_each(|(_, old)| {
            let _ = std::fs::remove_dir_all(&old);
        });
        info!("profiling: archived previous process's final heap dumps → {arch:?}");
    }

    fn write_flamegraph(path: &std::path::Path, report: &pprof::Report) -> anyhow::Result<()> {
        report.flamegraph(std::fs::File::create(path)?)?;
        Ok(())
    }

    /// Keep only the newest `keep` files whose name starts with `prefix`, ordered
    /// by mtime — NOT filename: the CPU seq counter restarts at 0 each process, so
    /// a dead process's high-seq files would outsort live ones by name forever.
    fn prune_old(dir: &std::path::Path, prefix: &str, keep: usize) {
        newest_first(dir, |n| n.starts_with(prefix)).into_iter().skip(keep).for_each(|(_, old)| {
            let _ = std::fs::remove_file(old);
        });
    }

    /// `(mtime, path)` for every file in `dir` whose name satisfies `matches`, newest first.
    fn newest_first(dir: &std::path::Path, matches: impl Fn(&str) -> bool) -> Vec<(std::time::SystemTime, PathBuf)> {
        let mut files: Vec<_> = std::fs::read_dir(dir)
            .into_iter()
            .flatten()
            .flatten()
            .filter(|e| e.file_name().to_str().is_some_and(&matches))
            .filter_map(|e| Some((e.metadata().ok()?.modified().ok()?, e.path())))
            .collect();
        files.sort_unstable_by_key(|(mtime, _)| std::cmp::Reverse(*mtime));
        files
    }
}

#[cfg(all(feature = "profiling", target_os = "linux"))]
pub use imp::start;

/// No-op without the `profiling` feature (Linux) — callers wire it unconditionally at boot.
#[cfg(not(all(feature = "profiling", target_os = "linux")))]
pub fn start(_data_dir: std::path::PathBuf) {}

// ===== errors =====
// Shared `.map_err` helpers; each preserves the original variant and message.

use std::fmt::Display;

use datafusion::{arrow::error::ArrowError, error::DataFusionError};
use datafusion_postgres::pgwire::error::PgWireError;

/// Wrap an Arrow error as `DataFusionError::ArrowError`, skipping the backtrace
/// capture `DataFusionError::from` would do (these fire on hot paths).
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
