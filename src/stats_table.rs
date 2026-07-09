//! `timefusion.stats` — operator-visible introspection table.
//!
//! Exposes a flat (component, key, value) view of `BufferedWriteLayer` /
//! `MemBuffer` / `WalManager` internals so monitoring and bench harnesses
//! don't have to scrape `ps -o rss=` and guess what walrus is up to.
//!
//! Usage:
//!     SELECT * FROM timefusion_stats;
//!     SELECT key, value FROM timefusion_stats WHERE component='mem_buffer';

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::Result as DFResult,
    datasource::{MemTable, TableProvider, TableType},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};

use crate::{buffered_write_layer::BufferedWriteLayer, database::ScanMetrics, errors::arrow_err};

/// Snapshot of the size of the resolve/provider caches at scan time.
/// Reported as `scan.fast_resolve_cache_entries` and
/// `scan.provider_cache_entries` so operators can spot the unbounded
/// growth (documented on each cache's field) before it shows up as
/// memory pressure in long-running processes.
pub type CacheSizeSnapshot = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;

pub struct StatsTableProvider {
    layer:        Option<Arc<BufferedWriteLayer>>,
    scan_metrics: Option<Arc<ScanMetrics>>,
    cache_sizes:  Option<CacheSizeSnapshot>,
    schema:       SchemaRef,
}

impl std::fmt::Debug for StatsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsTableProvider")
            .field("layer", &self.layer)
            .field("scan_metrics", &self.scan_metrics)
            .finish_non_exhaustive()
    }
}

impl StatsTableProvider {
    pub fn new(layer: Option<Arc<BufferedWriteLayer>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        Self {
            layer,
            scan_metrics: None,
            cache_sizes: None,
            schema,
        }
    }

    pub fn with_scan_metrics(mut self, m: Arc<ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn with_cache_sizes(mut self, f: CacheSizeSnapshot) -> Self {
        self.cache_sizes = Some(f);
        self
    }

    fn snapshot_batch(&self) -> DFResult<RecordBatch> {
        let mut rows: Vec<(&'static str, String, String)> = Vec::with_capacity(16);

        if let Some(layer) = &self.layer {
            let s = layer.snapshot_stats();
            rows.push(("mem_buffer", "project_count".into(), s.mem_project_count.to_string()));
            rows.push(("mem_buffer", "total_buckets".into(), s.mem_total_buckets.to_string()));
            rows.push(("mem_buffer", "total_rows".into(), s.mem_total_rows.to_string()));
            rows.push(("mem_buffer", "total_batches".into(), s.mem_total_batches.to_string()));
            // Replay DML consumed without applying (table already flushed) —
            // the quarantine dir no longer captures this loss class; monitor
            // this like a quarantine count (growth ⇒ check logs + re-drive).
            rows.push(("mem_buffer", "replay_dml_noops_total".into(), s.mem_replay_dml_noops.to_string()));
            // Suffix `_approx` because the in-bucket coalesce path overwrites
            // `memory_bytes` to the post-concat size, but the MemBuffer-level
            // running total only adds the pre-concat new_size at insert time
            // (no subtraction on coalesce). Drift is at most a few percent
            // during coalesce-heavy bursts; the value is for capacity
            // alerting, not for billing.
            rows.push(("mem_buffer", "estimated_bytes_approx".into(), s.mem_estimated_bytes.to_string()));
            rows.push((
                "mem_buffer",
                "estimated_mb_approx".into(),
                format!("{:.1}", s.mem_estimated_bytes as f64 / (1024.0 * 1024.0)),
            ));
            rows.push(("mem_buffer", "bucket_duration_micros".into(), s.bucket_duration_micros.to_string()));
            rows.push((
                "mem_buffer",
                "oldest_bucket_age_secs".into(),
                s.oldest_bucket_age_secs.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
            ));
            rows.push(("buffered_layer", "reserved_bytes".into(), s.reserved_bytes.to_string()));
            rows.push(("buffered_layer", "max_memory_bytes".into(), s.max_memory_bytes.to_string()));
            rows.push((
                "buffered_layer",
                "max_memory_mb".into(),
                format!("{:.1}", s.max_memory_bytes as f64 / (1024.0 * 1024.0)),
            ));
            rows.push(("buffered_layer", "pressure_pct".into(), s.pressure_pct.to_string()));
            rows.push(("buffered_layer", "backpressure_engaged_total".into(), s.backpressure_engaged_total.to_string()));
            rows.push((
                "buffered_layer",
                "backpressure_rejected_total".into(),
                s.backpressure_rejected_total.to_string(),
            ));
            rows.push((
                "buffered_layer",
                "backpressure_force_flush_total".into(),
                s.backpressure_force_flush_total.to_string(),
            ));
            rows.push(("buffered_layer", "flush_completed_total".into(), s.flush_completed_total.to_string()));
            rows.push(("buffered_layer", "flush_failed_total".into(), s.flush_failed_total.to_string()));
            // Ingest-vs-drain: both climb in steady state. If ingested pulls
            // ahead of flushed while pressure_pct=100 and flush_failed_total is
            // flat, ingest is outpacing a working drain (throughput wedge) —
            // not a stuck flush. `rows_in_buffer_lag` ≈ rows currently buffered
            // (ingested includes WAL-recovered rows, so the pair stays
            // comparable after a restart).
            rows.push(("buffered_layer", "rows_ingested_total".into(), s.rows_ingested_total.to_string()));
            rows.push(("buffered_layer", "rows_flushed_total".into(), s.rows_flushed_total.to_string()));
            rows.push((
                "buffered_layer",
                "rows_in_buffer_lag".into(),
                s.rows_ingested_total.saturating_sub(s.rows_flushed_total).to_string(),
            ));
            // Drain effectiveness: flat while pressure_pct=100 and flushes
            // commit ⇒ drained buckets are empty (memory is in buckets the
            // flush path isn't reaching, e.g. an open window needing force-flush).
            rows.push(("buffered_layer", "flush_freed_bytes_total".into(), s.flush_freed_bytes_total.to_string()));
            // Real RSS vs the estimate_batch_size charge. RSS far below
            // estimated_bytes_approx ⇒ per-bucket estimate is over-counting and
            // backpressure is tripping on phantom bytes, not real memory.
            rows.push((
                "buffered_layer",
                "process_rss_bytes".into(),
                s.process_rss_bytes.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
            ));
            rows.push((
                "buffered_layer",
                "process_rss_mb".into(),
                s.process_rss_bytes.map(|v| format!("{:.1}", v as f64 / (1024.0 * 1024.0))).unwrap_or_else(|| "null".into()),
            ));
            // Orphaned topics = failed-commit rows living ONLY in the WAL,
            // each pinning the WAL GC floor. PAGE on >0; remedy = restart.
            rows.push(("buffered_layer", "orphaned_topics".into(), s.orphaned_topics.to_string()));
            rows.push((
                "buffered_layer",
                "orphan_pin_age_secs".into(),
                s.orphan_pin_age_secs.map(|v| v.to_string()).unwrap_or_else(|| "null".into()),
            ));
            rows.push(("wal", "files".into(), s.wal_files.to_string()));
            rows.push(("wal", "disk_bytes".into(), s.wal_disk_bytes.to_string()));
            rows.push(("wal", "disk_mb".into(), format!("{:.1}", s.wal_disk_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("wal", "shards_per_topic".into(), s.wal_shards_per_topic.to_string()));
            rows.push(("wal", "known_topics".into(), s.wal_known_topics.to_string()));
        } else {
            rows.push(("buffered_layer", "status".into(), "disabled".into()));
        }

        if let Some(pc) = crate::plan_cache::global() {
            let (hits, misses) = pc.counters();
            let total = hits + misses;
            let hit_pct = if total > 0 { hits as f64 * 100.0 / total as f64 } else { 0.0 };
            rows.push(("plan_cache", "hits".into(), hits.to_string()));
            rows.push(("plan_cache", "misses".into(), misses.to_string()));
            rows.push(("plan_cache", "hit_pct".into(), format!("{:.1}", hit_pct)));
        }

        if let Some(m) = &self.scan_metrics {
            use std::sync::atomic::Ordering::Relaxed;
            let total = m.scans_total.load(Relaxed);
            let skipped = m.scans_skipped_delta.load(Relaxed);
            let mem_only = m.scans_mem_only.load(Relaxed);
            let delta_only = m.scans_delta_only.load(Relaxed);
            let both = m.scans_mem_plus_delta.load(Relaxed);
            let fr_hits = m.fast_resolve_hits.load(Relaxed);
            let fr_misses = m.fast_resolve_misses.load(Relaxed);
            let pct = |n: u64, d: u64| if d > 0 { n as f64 * 100.0 / d as f64 } else { 0.0 };
            rows.push(("scan", "total".into(), total.to_string()));
            rows.push(("scan", "skipped_delta".into(), skipped.to_string()));
            rows.push(("scan", "skipped_delta_pct".into(), format!("{:.1}", pct(skipped, total))));
            rows.push(("scan", "mem_only".into(), mem_only.to_string()));
            rows.push(("scan", "delta_only".into(), delta_only.to_string()));
            rows.push(("scan", "mem_plus_delta".into(), both.to_string()));
            rows.push(("scan", "fast_resolve_hits".into(), fr_hits.to_string()));
            rows.push(("scan", "fast_resolve_misses".into(), fr_misses.to_string()));
            rows.push(("scan", "fast_resolve_hit_pct".into(), format!("{:.1}", pct(fr_hits, fr_hits + fr_misses))));
            let pc_hits = m.provider_cache_hits.load(Relaxed);
            let pc_misses = m.provider_cache_misses.load(Relaxed);
            rows.push(("scan", "provider_cache_hits".into(), pc_hits.to_string()));
            rows.push(("scan", "provider_cache_misses".into(), pc_misses.to_string()));
            rows.push(("scan", "provider_cache_hit_pct".into(), format!("{:.1}", pct(pc_hits, pc_hits + pc_misses))));
            rows.push(("scan", "provider_build_abandoned".into(), m.provider_build_abandoned.load(Relaxed).to_string()));
            rows.push(("scan", "lat_p50_us_approx".into(), m.latency_percentile_us(0.50).to_string()));
            rows.push(("scan", "lat_p95_us_approx".into(), m.latency_percentile_us(0.95).to_string()));
            rows.push(("scan", "lat_p99_us_approx".into(), m.latency_percentile_us(0.99).to_string()));
            rows.push(("scan", "lat_p999_us_approx".into(), m.latency_percentile_us(0.999).to_string()));
            let pgt = m.pgwire_total.load(Relaxed);
            rows.push(("pgwire", "queries_total".into(), pgt.to_string()));
            rows.push(("pgwire", "lat_p50_us_approx".into(), m.pgwire_percentile_us(0.50).to_string()));
            rows.push(("pgwire", "lat_p95_us_approx".into(), m.pgwire_percentile_us(0.95).to_string()));
            rows.push(("pgwire", "lat_p99_us_approx".into(), m.pgwire_percentile_us(0.99).to_string()));
            rows.push(("pgwire", "lat_p999_us_approx".into(), m.pgwire_percentile_us(0.999).to_string()));
        }

        if let Some(snap) = &self.cache_sizes {
            // Mirror the field-level doc: these caches don't evict; size
            // tracks unique (project, table) pairs since process start.
            let (fast_resolve, provider) = snap();
            rows.push(("scan", "fast_resolve_cache_entries".into(), fast_resolve.to_string()));
            rows.push(("scan", "provider_cache_entries".into(), provider.to_string()));
        }

        let components: Vec<&str> = rows.iter().map(|r| r.0).collect();
        let keys: Vec<&str> = rows.iter().map(|r| r.1.as_str()).collect();
        let values: Vec<&str> = rows.iter().map(|r| r.2.as_str()).collect();

        let cols: Vec<ArrayRef> = vec![Arc::new(StringArray::from(components)), Arc::new(StringArray::from(keys)), Arc::new(StringArray::from(values))];
        RecordBatch::try_new(Arc::clone(&self.schema), cols).map_err(arrow_err)
    }
}

#[async_trait]
impl TableProvider for StatsTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(&self, state: &dyn Session, projection: Option<&Vec<usize>>, filters: &[Expr], limit: Option<usize>) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Build a fresh batch on every scan — counters move, we want point-in-time.
        let batch = self.snapshot_batch()?;
        let mem = MemTable::try_new(Arc::clone(&self.schema), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}
