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

use crate::{buffered_write_layer::BufferedWriteLayer, database::ScanMetrics, errors::arrow_err, object_store_cache::FoyerRuntimeStats};

/// Snapshot of the size of the resolve/provider caches at scan time.
/// Reported as `scan.fast_resolve_cache_entries` and
/// `scan.provider_cache_entries` so operators can spot the unbounded
/// growth (documented on each cache's field) before it shows up as
/// memory pressure in long-running processes.
pub type CacheSizeSnapshot = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;
pub type FoyerStatsSnapshot = Arc<dyn Fn() -> FoyerRuntimeStats + Send + Sync>;

pub struct StatsTableProvider {
    layer: Option<Arc<BufferedWriteLayer>>,
    scan_metrics: Option<Arc<ScanMetrics>>,
    cache_sizes: Option<CacheSizeSnapshot>,
    foyer_stats: Option<FoyerStatsSnapshot>,
    schema: SchemaRef,
}

impl std::fmt::Debug for StatsTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatsTableProvider").field("layer", &self.layer).field("scan_metrics", &self.scan_metrics).finish_non_exhaustive()
    }
}

impl StatsTableProvider {
    pub fn new(layer: Option<Arc<BufferedWriteLayer>>) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("component", DataType::Utf8, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        Self { layer, scan_metrics: None, cache_sizes: None, foyer_stats: None, schema }
    }

    pub fn with_scan_metrics(mut self, m: Arc<ScanMetrics>) -> Self {
        self.scan_metrics = Some(m);
        self
    }

    pub fn with_cache_sizes(mut self, f: CacheSizeSnapshot) -> Self {
        self.cache_sizes = Some(f);
        self
    }

    pub fn with_foyer_stats(mut self, f: FoyerStatsSnapshot) -> Self {
        self.foyer_stats = Some(f);
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
            rows.push(("mem_buffer", "estimated_mb_approx".into(), format!("{:.1}", s.mem_estimated_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("mem_buffer", "bucket_duration_micros".into(), s.bucket_duration_micros.to_string()));
            rows.push(("mem_buffer", "oldest_bucket_age_secs".into(), s.oldest_bucket_age_secs.map(|v| v.to_string()).unwrap_or_else(|| "null".into())));
            rows.push(("buffered_layer", "reserved_bytes".into(), s.reserved_bytes.to_string()));
            rows.push(("buffered_layer", "max_memory_bytes".into(), s.max_memory_bytes.to_string()));
            rows.push(("buffered_layer", "max_memory_mb".into(), format!("{:.1}", s.max_memory_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("buffered_layer", "pressure_pct".into(), s.pressure_pct.to_string()));
            rows.push(("buffered_layer", "backpressure_engaged_total".into(), s.backpressure_engaged_total.to_string()));
            rows.push(("buffered_layer", "backpressure_rejected_total".into(), s.backpressure_rejected_total.to_string()));
            rows.push(("buffered_layer", "backpressure_force_flush_total".into(), s.backpressure_force_flush_total.to_string()));
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
            rows.push(("buffered_layer", "rows_in_buffer_lag".into(), s.rows_ingested_total.saturating_sub(s.rows_flushed_total).to_string()));
            // Drain effectiveness: flat while pressure_pct=100 and flushes
            // commit ⇒ drained buckets are empty (memory is in buckets the
            // flush path isn't reaching, e.g. an open window needing force-flush).
            rows.push(("buffered_layer", "flush_freed_bytes_total".into(), s.flush_freed_bytes_total.to_string()));
            // Real RSS vs the estimate_batch_size charge. RSS far below
            // estimated_bytes_approx ⇒ per-bucket estimate is over-counting and
            // backpressure is tripping on phantom bytes, not real memory.
            rows.push(("buffered_layer", "process_rss_bytes".into(), s.process_rss_bytes.map(|v| v.to_string()).unwrap_or_else(|| "null".into())));
            rows.push((
                "buffered_layer",
                "process_rss_mb".into(),
                s.process_rss_bytes.map(|v| format!("{:.1}", v as f64 / (1024.0 * 1024.0))).unwrap_or_else(|| "null".into()),
            ));
            // Orphaned topics = failed-commit rows living ONLY in the WAL,
            // each pinning the WAL GC floor. PAGE on >0; remedy = restart.
            rows.push(("buffered_layer", "orphaned_topics".into(), s.orphaned_topics.to_string()));
            rows.push(("buffered_layer", "orphan_pin_age_secs".into(), s.orphan_pin_age_secs.map(|v| v.to_string()).unwrap_or_else(|| "null".into())));
            rows.push(("buffered_layer", "drained".into(), s.drained.to_string()));
            rows.push(("buffered_layer", "boot_micros".into(), s.boot_micros.to_string()));
            rows.push(("wal", "recovery_complete".into(), s.wal_recovery_complete.to_string()));
            rows.push(("wal", "recovery_duration_ms".into(), s.wal_recovery_duration_ms.to_string()));
            rows.push(("wal", "files".into(), s.wal_files.to_string()));
            rows.push(("wal", "disk_bytes".into(), s.wal_disk_bytes.to_string()));
            rows.push(("wal", "disk_mb".into(), format!("{:.1}", s.wal_disk_bytes as f64 / (1024.0 * 1024.0))));
            rows.push(("wal", "shards_per_topic".into(), s.wal_shards_per_topic.to_string()));
            rows.push(("wal", "known_topics".into(), s.wal_known_topics.to_string()));
        } else {
            rows.push(("buffered_layer", "status".into(), "disabled".into()));
        }

        {
            use std::sync::atomic::Ordering::Relaxed;
            let d = crate::metrics::dml_stats();
            rows.push(("dml", "occ_conflicts_total".into(), d.occ_conflicts.load(Relaxed).to_string()));
            rows.push(("dml", "retry_successes_total".into(), d.retry_successes.load(Relaxed).to_string()));
            rows.push(("dml", "retry_exhausted_total".into(), d.retry_exhausted.load(Relaxed).to_string()));

            let m = crate::metrics::maintenance_stats();
            rows.push(("maintenance", "checkpoints_created".into(), m.checkpoints_created.load(Relaxed).to_string()));
            rows.push(("maintenance", "checkpoint_failed".into(), m.checkpoint_failed.load(Relaxed).to_string()));
            rows.push(("maintenance", "checkpoint_corrupt".into(), m.checkpoint_corrupt.load(Relaxed).to_string()));
            rows.push(("maintenance", "log_files_cleaned".into(), m.log_files_cleaned.load(Relaxed).to_string()));
            rows.push(("maintenance", "log_cleanup_failed".into(), m.log_cleanup_failed.load(Relaxed).to_string()));
            // Max version lag (current - last checkpointed) seen at the last
            // checkpoint tick. Should stay near checkpoint_interval; a large,
            // growing value means the checkpoint task is failing or wedged.
            rows.push(("maintenance", "checkpoint_lag_versions".into(), m.checkpoint_lag_versions.load(Relaxed).to_string()));
            // NONZERO = committed parquet was destroyed elsewhere (2026-07-09
            // commit-path deletion bug). PAGE and investigate.
            rows.push(("maintenance", "dangling_removed".into(), m.dangling_removed.load(Relaxed).to_string()));
            rows.push(("maintenance", "reconcile_failed".into(), m.reconcile_failed.load(Relaxed).to_string()));
            rows.push(("maintenance", "dedup_timed_out_total".into(), m.dedup_timed_out.load(Relaxed).to_string()));
            rows.push(("maintenance", "dedup_failed_total".into(), m.dedup_failed.load(Relaxed).to_string()));
            rows.push(("maintenance", "light_optimize_timed_out_total".into(), m.light_optimize_timed_out.load(Relaxed).to_string()));
            rows.push(("maintenance", "light_optimize_failed_total".into(), m.light_optimize_failed.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_queue_depth".into(), m.dirty_bin_queue_depth.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_enqueued_total".into(), m.dirty_bin_enqueued.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_eligible_total".into(), m.dirty_bin_eligible.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_processed_total".into(), m.dirty_bin_processed.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_requeued_total".into(), m.dirty_bin_requeued.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_dropped_rows_total".into(), m.dirty_bin_dropped_rows.load(Relaxed).to_string()));
            rows.push(("maintenance", "dirty_bin_rewrite_duration_ms_total".into(), m.dirty_bin_rewrite_duration_ms.load(Relaxed).to_string()));
            // Runs exceeding the long-running warning threshold. Slow progress
            // is allowed; sustained nonzero with no completion = wedged.
            rows.push(("maintenance", "cron_long_running_total".into(), m.cron_long_running.load(Relaxed).to_string()));
            // Fired frozen while uptime grows = scheduler dead (2026-07-14
            // outage); skipped growing = a job body is wedged or overlong.
            rows.push(("maintenance", "cron_ticks_fired".into(), m.cron_ticks_fired.load(Relaxed).to_string()));
            rows.push(("maintenance", "cron_ticks_skipped".into(), m.cron_ticks_skipped.load(Relaxed).to_string()));
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

        if let Some(snap) = &self.foyer_stats {
            let s = snap();
            for (component, stats) in [("foyer", s.stats.main), ("foyer_metadata", s.stats.metadata)] {
                rows.push((component, "hits".into(), stats.hits.to_string()));
                rows.push((component, "misses".into(), stats.misses.to_string()));
                rows.push((component, "bytes_served".into(), stats.bytes_served.to_string()));
                rows.push((component, "inner_bytes_read".into(), stats.inner_bytes_read.to_string()));
                rows.push((component, "ttl_expirations".into(), stats.ttl_expirations.to_string()));
                rows.push((component, "inner_gets".into(), stats.inner_gets.to_string()));
            }
            rows.push(("foyer", "memory_mb".into(), (s.memory_size_bytes / 1024 / 1024).to_string()));
            rows.push(("foyer", "disk_gb".into(), (s.disk_size_bytes / 1024 / 1024 / 1024).to_string()));
            rows.push(("foyer", "ttl_seconds".into(), s.ttl_seconds.to_string()));
            rows.push(("foyer", "l1_max_entry_mb".into(), (s.l1_max_entry_bytes / 1024 / 1024).to_string()));
            rows.push(("foyer", "block_size_mb".into(), (s.block_size_bytes / 1024 / 1024).to_string()));
            rows.push(("foyer", "cache_recent_days".into(), s.cache_recent_days.to_string()));
            rows.push(("foyer", "cache_dir".into(), s.cache_dir.display().to_string()));
            rows.push(("foyer", "metadata_memory_mb".into(), (s.metadata_memory_size_bytes / 1024 / 1024).to_string()));
            rows.push(("foyer", "metadata_disk_gb".into(), (s.metadata_disk_size_bytes / 1024 / 1024 / 1024).to_string()));
            rows.push(("foyer", "l1_used_bytes".into(), s.l1_used_bytes.to_string()));
            rows.push(("foyer", "l2_used_bytes".into(), s.l2_used_bytes.to_string()));
            rows.push(("foyer", "entry_count".into(), s.entry_count.to_string()));
            rows.push(("foyer", "evictions".into(), s.evictions.to_string()));
        }

        {
            let s = deltalake::delta_datafusion::parquet_metrics::snapshot();
            rows.push(("parquet", "metadata_cache_hits".into(), s.metadata_cache_hits.to_string()));
            rows.push(("parquet", "metadata_cache_misses".into(), s.metadata_cache_misses.to_string()));
            rows.push(("parquet", "bytes_read".into(), s.bytes_read.to_string()));
            rows.push(("parquet", "read_time_us".into(), s.read_time_us.to_string()));
            rows.push(("parquet", "scans".into(), s.scans.to_string()));
            rows.push(("parquet", "files_planned".into(), s.files_planned.to_string()));
            rows.push(("parquet", "bytes_planned".into(), s.bytes_planned.to_string()));
            rows.push(("parquet", "selected_row_groups".into(), s.selected_row_groups.to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    #[test]
    fn exposes_foyer_runtime_configuration_and_occupancy() {
        let snapshot = FoyerRuntimeStats {
            memory_size_bytes: 4 * 1024 * 1024,
            disk_size_bytes: 128 * 1024 * 1024 * 1024,
            ttl_seconds: 3600,
            l1_max_entry_bytes: 64 * 1024 * 1024,
            block_size_bytes: 256 * 1024 * 1024,
            cache_recent_days: 1,
            cache_dir: "/cache".into(),
            metadata_memory_size_bytes: 512 * 1024 * 1024,
            metadata_disk_size_bytes: 5 * 1024 * 1024 * 1024,
            l1_used_bytes: 123,
            l2_used_bytes: 456,
            entry_count: 7,
            evictions: 8,
            ..Default::default()
        };
        let batch = StatsTableProvider::new(None).with_foyer_stats(Arc::new(move || snapshot.clone())).snapshot_batch().unwrap();
        let components = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let keys = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let values = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let rows: Vec<_> = (0..batch.num_rows()).map(|i| (components.value(i), keys.value(i), values.value(i))).collect();

        for key in [
            "memory_mb",
            "disk_gb",
            "ttl_seconds",
            "l1_max_entry_mb",
            "block_size_mb",
            "cache_recent_days",
            "cache_dir",
            "metadata_memory_mb",
            "metadata_disk_gb",
            "l1_used_bytes",
            "l2_used_bytes",
            "entry_count",
            "evictions",
        ] {
            assert!(rows.iter().any(|(component, actual, _)| *component == "foyer" && *actual == key), "missing foyer.{key}");
        }
        assert!(rows.contains(&("foyer", "cache_dir", "/cache")));
    }

    #[test]
    fn exposes_dml_retry_outcomes() {
        let batch = StatsTableProvider::new(None).snapshot_batch().unwrap();
        let components = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let keys = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let rows: Vec<_> = (0..batch.num_rows()).map(|i| (components.value(i), keys.value(i))).collect();

        assert!(rows.contains(&("dml", "occ_conflicts_total")));
        assert!(rows.contains(&("dml", "retry_successes_total")));
        assert!(rows.contains(&("dml", "retry_exhausted_total")));
        assert!(rows.contains(&("maintenance", "dedup_timed_out_total")));
        assert!(rows.contains(&("maintenance", "light_optimize_timed_out_total")));
        assert!(rows.contains(&("maintenance", "cron_long_running_total")));
        assert!(rows.contains(&("parquet", "metadata_cache_hits")));
        assert!(rows.contains(&("parquet", "bytes_read")));
    }
}
