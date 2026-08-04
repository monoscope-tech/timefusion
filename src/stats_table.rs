//! `timefusion.stats` — operator-visible introspection table.
//!
//! Exposes a flat (component, key, value) view of `BufferedWriteLayer` /
//! `MemBuffer` / `WalManager` internals so monitoring and bench harnesses
//! don't have to scrape `ps -o rss=` and guess what walrus is up to.
//!
//! Usage:
//!     SELECT * FROM timefusion_stats;
//!     SELECT key, value FROM timefusion_stats WHERE component='mem_buffer';

use std::sync::{Arc, atomic::Ordering::Relaxed};

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
/// (used_bytes, pool_size) of the shared query memory pool, live.
pub type PoolSnapshot = Arc<dyn Fn() -> (usize, usize) + Send + Sync>;
/// (resident partitions, estimated bytes, byte limit, active builders).
pub type LogicalCountSnapshot = Arc<dyn Fn() -> (usize, usize, usize, usize) + Send + Sync>;

type Row = (&'static str, String, String);

/// `rows![component; "key" => value, …]` — one component's rows. Values only
/// need `ToString`, so the mixed usize/u64/bool/`&str` metric types need no
/// conversion at the call site. `rows![@atomic component; …]` takes the
/// counters themselves and loads each one `Relaxed`.
macro_rules! rows {
    (@atomic $component:expr; $($key:literal => $counter:expr),* $(,)?) => {
        rows![$component; $($key => $counter.load(Relaxed)),*]
    };
    ($component:expr; $($key:literal => $val:expr),* $(,)?) => {
        vec![$(($component, $key.to_string(), $val.to_string())),*]
    };
}

fn mb(bytes: f64) -> String {
    format!("{:.1}", bytes / (1024.0 * 1024.0))
}

fn mib(bytes: usize) -> usize {
    bytes / 1024 / 1024
}

fn gib(bytes: usize) -> usize {
    mib(bytes) / 1024
}

fn pct(n: u64, d: u64) -> String {
    format!("{:.1}", if d > 0 { n as f64 * 100.0 / d as f64 } else { 0.0 })
}

fn avg(total: u64, samples: u64) -> u64 {
    total.checked_div(samples).unwrap_or(0)
}

fn or_null<T: ToString>(v: Option<T>) -> String {
    v.map_or_else(|| "null".to_string(), |v| v.to_string())
}

pub struct StatsTableProvider {
    layer: Option<Arc<BufferedWriteLayer>>,
    scan_metrics: Option<Arc<ScanMetrics>>,
    cache_sizes: Option<CacheSizeSnapshot>,
    foyer_stats: Option<FoyerStatsSnapshot>,
    query_pool: Option<PoolSnapshot>,
    logical_count: Option<LogicalCountSnapshot>,
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
        Self { layer, scan_metrics: None, cache_sizes: None, foyer_stats: None, query_pool: None, logical_count: None, schema }
    }

    pub fn with_scan_metrics(self, m: Arc<ScanMetrics>) -> Self {
        Self { scan_metrics: Some(m), ..self }
    }

    pub fn with_cache_sizes(self, f: CacheSizeSnapshot) -> Self {
        Self { cache_sizes: Some(f), ..self }
    }

    pub fn with_foyer_stats(self, f: FoyerStatsSnapshot) -> Self {
        Self { foyer_stats: Some(f), ..self }
    }

    pub fn with_query_pool(self, f: PoolSnapshot) -> Self {
        Self { query_pool: Some(f), ..self }
    }

    pub fn with_logical_count(self, f: LogicalCountSnapshot) -> Self {
        Self { logical_count: Some(f), ..self }
    }

    fn snapshot_batch(&self) -> DFResult<RecordBatch> {
        // Boot memory budget. `slack_mb` is the figure that matters: it is what
        // absorbs allocation no budget tracks (parquet decode, walrus mmaps,
        // tantivy, allocator overhead), and a small slack is how the box gets
        // OOM-killed while every individual budget reads healthy.
        let budget = crate::autotune::boot_budget_audit().map_or_else(Vec::new, |a| {
            rows!["budget";
                "committed_mb" => a.committed_mb,
                "warn_at_mb" => a.warn_at_mb,
                "slack_mb" => a.slack_mb(),
                "query_pool_mb" => a.query_pool_mb,
                "mem_buffer_hard_mb" => a.mem_buffer_hard_mb,
                "maintenance_pool_mb" => a.maintenance_pool_mb,
                "foyer_mb" => a.foyer_mb,
                "tantivy_peak_mb" => a.tantivy_peak_mb,
                "df_metadata_cache_mb" => a.df_metadata_cache_mb,
                "oversubscribed" => a.oversubscribed(),
            ]
        });

        let layer = self.layer.as_ref().map_or_else(
            || rows!["buffered_layer"; "status" => "disabled"],
            |layer| {
                let s = layer.snapshot_stats();
                // Local hot tier (demoted sealed buckets served by mmap'd Arrow
                // IPC). `read_misses` growing = torn/absent files falling through
                // to Delta; `write_failures` growing = demotion is broken (reads
                // stay correct, just slower).
                let h = &s.hot_tier;
                [
                    rows!["mem_buffer";
                        "project_count" => s.mem_project_count,
                        "total_buckets" => s.mem_total_buckets,
                        "total_rows" => s.mem_total_rows,
                        "total_batches" => s.mem_total_batches,
                        // Replay DML consumed without applying (table already flushed) —
                        // the quarantine dir no longer captures this loss class; monitor
                        // this like a quarantine count (growth ⇒ check logs + re-drive).
                        "replay_dml_noops_total" => s.mem_replay_dml_noops,
                        // Suffix `_approx` because the in-bucket coalesce path overwrites
                        // `memory_bytes` to the post-concat size, but the MemBuffer-level
                        // running total only adds the pre-concat new_size at insert time
                        // (no subtraction on coalesce). Drift is at most a few percent
                        // during coalesce-heavy bursts; the value is for capacity
                        // alerting, not for billing.
                        "estimated_bytes_approx" => s.mem_estimated_bytes,
                        "estimated_mb_approx" => mb(s.mem_estimated_bytes as f64),
                        "bucket_duration_micros" => s.bucket_duration_micros,
                        "oldest_bucket_age_secs" => or_null(s.oldest_bucket_age_secs),
                    ],
                    rows!["buffered_layer";
                        "reserved_bytes" => s.reserved_bytes,
                        "max_memory_bytes" => s.max_memory_bytes,
                        "max_memory_mb" => mb(s.max_memory_bytes as f64),
                        "pressure_pct" => s.pressure_pct,
                        "backpressure_engaged_total" => s.backpressure_engaged_total,
                        "backpressure_rejected_total" => s.backpressure_rejected_total,
                        "backpressure_force_flush_total" => s.backpressure_force_flush_total,
                        "flush_completed_total" => s.flush_completed_total,
                        "flush_failed_total" => s.flush_failed_total,
                        // Ingest-vs-drain: both climb in steady state. If ingested pulls
                        // ahead of flushed while pressure_pct=100 and flush_failed_total is
                        // flat, ingest is outpacing a working drain (throughput wedge) —
                        // not a stuck flush. `rows_in_buffer_lag` ≈ rows currently buffered
                        // (ingested includes WAL-recovered rows, so the pair stays
                        // comparable after a restart).
                        "rows_ingested_total" => s.rows_ingested_total,
                        "rows_flushed_total" => s.rows_flushed_total,
                        "rows_in_buffer_lag" => s.rows_ingested_total.saturating_sub(s.rows_flushed_total),
                        // Drain effectiveness: flat while pressure_pct=100 and flushes
                        // commit ⇒ drained buckets are empty (memory is in buckets the
                        // flush path isn't reaching, e.g. an open window needing force-flush).
                        "flush_freed_bytes_total" => s.flush_freed_bytes_total,
                        // Real RSS vs the estimate_batch_size charge. RSS far below
                        // estimated_bytes_approx ⇒ per-bucket estimate is over-counting and
                        // backpressure is tripping on phantom bytes, not real memory.
                        "process_rss_bytes" => or_null(s.process_rss_bytes),
                        "process_rss_mb" => or_null(s.process_rss_bytes.map(|v| mb(v as f64))),
                        // Orphaned topics = failed-commit rows living ONLY in the WAL,
                        // each pinning the WAL GC floor. PAGE on >0; remedy = restart.
                        "orphaned_topics" => s.orphaned_topics,
                        "orphan_pin_age_secs" => or_null(s.orphan_pin_age_secs),
                        "drained" => s.drained,
                        "boot_micros" => s.boot_micros,
                    ],
                    rows!["wal"; "recovery_complete" => s.wal_recovery_complete, "recovery_duration_ms" => s.wal_recovery_duration_ms],
                    rows!["tantivy"; "recovery_pending_files" => s.tantivy_recovery_pending_files],
                    rows!["wal";
                        "files" => s.wal_files,
                        "disk_bytes" => s.wal_disk_bytes,
                        "disk_mb" => mb(s.wal_disk_bytes as f64),
                        // Parked payloads: invisible to wal_disk_bytes (flat walk), which is
                        // how gc_wal_files deleted them unnoticed. ALERT if files > 0.
                        "quarantine_files" => s.quarantine_files,
                        "quarantine_mb" => mb(s.quarantine_bytes as f64),
                    ],
                    rows!["hot_tier";
                        "tables" => h.tables,
                        "files" => h.files,
                        "bytes" => h.bytes,
                        "writes_total" => h.writes,
                        "write_failures_total" => h.write_failures,
                        "read_hits_total" => h.read_hits,
                        "read_misses_total" => h.read_misses,
                        "mem_skipped_total" => h.mem_skipped,
                        "schema_drift_total" => h.schema_drift,
                        "gc_deleted_total" => h.gc_deleted,
                        "gc_bytes_freed_total" => h.gc_bytes_freed,
                        "invalidated_total" => h.invalidated,
                        // DML statements scoped to their own time window vs. those
                        // that had to drop the whole table. `invalidations_full_total`
                        // dominating means the range derivation isn't matching the
                        // workload's predicate shapes — the tier is back to paying for
                        // files every enrichment statement throws away.
                        "invalidations_ranged_total" => h.invalidations_ranged,
                        "invalidations_full_total" => h.invalidations_full,
                        // Tables that stopped demoting because a DML kept dropping
                        // their files before any query read them (continuous
                        // whole-table enrichment). Non-zero is the tier working as
                        // intended; it used to be an invisible silent waste.
                        "suppressed_tables" => h.suppressed_tables,
                        "suppressions_total" => h.suppressions,
                        // The two heap bounds. `memo_bytes` is the mapping the decode
                        // memo pins (LRU-capped); `leg_budget_stops_total` counts scans
                        // that hit the per-query hot-leg byte budget and served the
                        // remaining windows from Delta instead — sustained non-zero
                        // means the budget, not retention, is the tier's real size.
                        "memo_files" => h.memo_files,
                        "memo_bytes" => h.memo_bytes,
                        "memo_evicted_total" => h.memo_evicted,
                        "leg_budget_stops_total" => h.leg_budget_stops,
                    ],
                    h.suppressed
                        .iter()
                        .map(|((project, table), secs)| ("hot_tier", format!("suppressed.{project}.{table}"), format!("cooldown_secs_remaining={secs}")))
                        .collect(),
                    rows!["wal"; "shards_per_topic" => s.wal_shards_per_topic, "known_topics" => s.wal_known_topics],
                ]
                .into_iter()
                .flatten()
                .collect()
            },
        );

        let d = crate::metrics::dml_stats();
        let dml = rows![@atomic "dml";
            "occ_conflicts_total" => d.occ_conflicts,
            "retry_successes_total" => d.retry_successes,
            "retry_exhausted_total" => d.retry_exhausted,
        ];

        let m = crate::metrics::maintenance_stats();
        let maintenance = rows![@atomic "maintenance";
            "checkpoints_created" => m.checkpoints_created,
            "checkpoint_failed" => m.checkpoint_failed,
            "checkpoint_corrupt" => m.checkpoint_corrupt,
            "log_files_cleaned" => m.log_files_cleaned,
            "log_cleanup_failed" => m.log_cleanup_failed,
            // Max version lag (current - last checkpointed) seen at the last
            // checkpoint tick. Should stay near checkpoint_interval; a large,
            // growing value means the checkpoint task is failing or wedged.
            "checkpoint_lag_versions" => m.checkpoint_lag_versions,
            // NONZERO = committed parquet was destroyed elsewhere (2026-07-09
            // commit-path deletion bug). PAGE and investigate.
            "dangling_removed" => m.dangling_removed,
            "reconcile_failed" => m.reconcile_failed,
            "dedup_timed_out_total" => m.dedup_timed_out,
            "dedup_failed_total" => m.dedup_failed,
            "light_optimize_timed_out_total" => m.light_optimize_timed_out,
            "light_optimize_failed_total" => m.light_optimize_failed,
            "light_optimize_tick_truncated_total" => m.light_optimize_tick_truncated,
            // planned vs completed is the per-tick coverage check: a persistent
            // gap means hot projects are going uncompacted (prod 2026-07-29).
            "light_optimize_projects_planned_total" => m.light_optimize_projects_planned,
            "light_optimize_projects_completed_total" => m.light_optimize_projects_completed,
            "light_optimize_bins_committed_total" => m.light_optimize_bins_committed,
            "light_optimize_waves_committed_total" => m.light_optimize_waves_committed,
            "dedup_bins_committed_total" => m.dedup_bins_committed,
            "dedup_waves_committed_total" => m.dedup_waves_committed,
            "light_optimize_wal_yields_total" => m.light_optimize_wal_yields,
            "light_optimize_memory_brakes_total" => m.light_optimize_memory_brakes,
            "mor_delta_leg_sorts_total" => m.mor_delta_leg_sorts,
            "flush_sort_unsorted_fallbacks_total" => m.flush_sort_unsorted_fallbacks,
            "light_optimize_ticks_degraded_total" => m.light_optimize_ticks_degraded,
            "dirty_bin_queue_depth" => m.dirty_bin_queue_depth,
            "dirty_bin_enqueued_total" => m.dirty_bin_enqueued,
            "dirty_bin_eligible_total" => m.dirty_bin_eligible,
            "dirty_bin_processed_total" => m.dirty_bin_processed,
            "dirty_bin_requeued_total" => m.dirty_bin_requeued,
            "dirty_bin_dropped_rows_total" => m.dirty_bin_dropped_rows,
            "dirty_bin_rewrite_duration_ms_total" => m.dirty_bin_rewrite_duration_ms,
            "dedup_bins_deferred_cold_total" => m.dedup_bins_deferred_cold,
            "dedup_passes_flush_yields_total" => m.dedup_passes_flush_yields,
            "wave_commits_yielded_to_flush_total" => m.wave_commits_yielded_to_flush,
            // Runs exceeding the long-running warning threshold. Slow progress
            // is allowed; sustained nonzero with no completion = wedged.
            "cron_long_running_total" => m.cron_long_running,
            // Fired frozen while uptime grows = scheduler dead (2026-07-14
            // outage); skipped growing = a job body is wedged or overlong.
            "cron_ticks_fired" => m.cron_ticks_fired,
            "cron_ticks_skipped" => m.cron_ticks_skipped,
        ];

        let plan_cache = crate::plan_cache::global().map_or_else(Vec::new, |pc| {
            let (hits, misses) = pc.counters();
            // Shape path = literal-bearing and now()-bearing SELECTs (the dashboard
            // hot path). Separate from the placeholder-`$N` counters above, and
            // previously unexposed — so the now()-shape caching (the bulk of the
            // dashboard work) was entirely invisible in stats. shape_hits = a plan
            // was served via the shape path (built or reused); shape_skips = a shape
            // couldn't be parameterized and fell back to a fresh plan.
            let (shape_hits, shape_skips) = pc.shape_counters();
            rows!["plan_cache";
                "hits" => hits,
                "misses" => misses,
                "hit_pct" => pct(hits, hits + misses),
                "shape_hits" => shape_hits,
                "shape_skips" => shape_skips,
            ]
        });

        let scan = self.scan_metrics.as_ref().map_or_else(Vec::new, |m| {
            let (total, skipped) = (m.scans_total.load(Relaxed), m.scans_skipped_delta.load(Relaxed));
            let (fr_hits, fr_misses) = (m.fast_resolve_hits.load(Relaxed), m.fast_resolve_misses.load(Relaxed));
            let (pc_hits, pc_misses) = (m.provider_cache_hits.load(Relaxed), m.provider_cache_misses.load(Relaxed));
            let provider_builds = m.provider_build_total.load(Relaxed);
            let provider_scans = m.provider_scan_total.load(Relaxed);
            let mem_plans = m.mem_plan_total.load(Relaxed);
            let hot_plans = m.hot_plan_total.load(Relaxed);
            // Parquet decode heap — the largest consumer outside every budget.
            // `peak_batch_bytes x polls_inflight_peak` bounds the worst-case
            // concurrent decode heap, which is what a Transient budget must cover.
            let (dpeak, dinflight_peak) = (m.decode_peak_batch_bytes.load(Relaxed), m.decode_polls_inflight_peak.load(Relaxed));
            // The number the OOM killer acts on, live: without this the only
            // record of a memory climb is the kernel's post-mortem kill line.
            let (used, limit) =
                (crate::database::process_memory_bytes().unwrap_or(0) as u64, crate::config::try_config().map_or(0, |c| c.derived.memory_limit_bytes as u64));
            let (pool_used, pool_size) = self.query_pool.as_ref().map_or((0, 0), |f| f());
            [
                rows!["memory";
                    "charged_bytes" => used,
                    "limit_bytes" => limit,
                    "charged_pct" => if limit > 0 { used * 100 / limit } else { 0 },
                    // Saturation here surfaces as "Resources exhausted" query
                    // errors (2026-08-03 07:06: enrichment UPDATEs failing at
                    // 30.0/30.0 GB), invisible before this row.
                    "query_pool_used_bytes" => pool_used,
                    "query_pool_pct" => if pool_size > 0 { pool_used * 100 / pool_size } else { 0 },
                ],
                rows!["scan_decode";
                    "bytes_total" => m.decode_bytes_total.load(Relaxed),
                    "peak_batch_bytes" => dpeak,
                    "polls_inflight" => m.decode_polls_inflight.load(Relaxed),
                    "polls_inflight_peak" => dinflight_peak,
                    "pressure_throttled_total" => m.decode_pressure_throttled.load(Relaxed),
                    "worst_case_heap_mb" => mb(dpeak.saturating_mul(dinflight_peak) as f64),
                ],
                rows!["scan";
                    "total" => total,
                    "skipped_delta" => skipped,
                    "skipped_delta_pct" => pct(skipped, total),
                    "mem_only" => m.scans_mem_only.load(Relaxed),
                    "delta_only" => m.scans_delta_only.load(Relaxed),
                    "mem_plus_delta" => m.scans_mem_plus_delta.load(Relaxed),
                    "fast_resolve_hits" => fr_hits,
                    "fast_resolve_misses" => fr_misses,
                    "fast_resolve_hit_pct" => pct(fr_hits, fr_hits + fr_misses),
                    "provider_cache_hits" => pc_hits,
                    "provider_cache_misses" => pc_misses,
                    "provider_cache_evictions" => m.provider_cache_evictions.load(Relaxed),
                    "provider_cache_hit_pct" => pct(pc_hits, pc_hits + pc_misses),
                    "provider_build_abandoned" => m.provider_build_abandoned.load(Relaxed),
                    "provider_build_us_avg" => avg(m.provider_build_us_total.load(Relaxed), provider_builds),
                    "provider_build_total" => provider_builds,
                    "provider_scan_us_avg" => avg(m.provider_scan_us_total.load(Relaxed), provider_scans),
                    "provider_scan_total" => provider_scans,
                    "mem_plan_us_avg" => avg(m.mem_plan_us_total.load(Relaxed), mem_plans),
                    "mem_plan_total" => mem_plans,
                    "hot_plan_us_avg" => avg(m.hot_plan_us_total.load(Relaxed), hot_plans),
                    "hot_plan_total" => hot_plans,
                    "lat_p50_us_approx" => m.latency_percentile_us(0.50),
                    "lat_p95_us_approx" => m.latency_percentile_us(0.95),
                    "lat_p99_us_approx" => m.latency_percentile_us(0.99),
                    "lat_p999_us_approx" => m.latency_percentile_us(0.999),
                ],
                rows!["pgwire";
                    "queries_total" => m.pgwire_total.load(Relaxed),
                    "lat_p50_us_approx" => m.pgwire_percentile_us(0.50),
                    "lat_p95_us_approx" => m.pgwire_percentile_us(0.95),
                    "lat_p99_us_approx" => m.pgwire_percentile_us(0.99),
                    "lat_p999_us_approx" => m.pgwire_percentile_us(0.999),
                ],
            ]
            .into_iter()
            .flatten()
            .collect()
        });

        let foyer = self.foyer_stats.as_ref().map_or_else(Vec::new, |snap| {
            let s = snap();
            [("foyer", s.stats.main), ("foyer_metadata", s.stats.metadata)]
                .into_iter()
                .flat_map(|(component, st)| {
                    rows![component;
                        "hits" => st.hits,
                        "misses" => st.misses,
                        "range_hits" => st.range_hits,
                        "range_misses" => st.range_misses,
                        "bytes_served" => st.bytes_served,
                        "inner_bytes_read" => st.inner_bytes_read,
                        "range_bytes_read" => st.range_bytes_read,
                        "ttl_expirations" => st.ttl_expirations,
                        "inner_gets" => st.inner_gets,
                    ]
                })
                .chain(rows!["foyer";
                    "memory_mb" => mib(s.memory_size_bytes),
                    "disk_gb" => gib(s.disk_size_bytes),
                    "ttl_seconds" => s.ttl_seconds,
                    "l1_max_entry_mb" => mib(s.l1_max_entry_bytes),
                    "block_size_mb" => mib(s.block_size_bytes),
                    "cache_recent_days" => s.cache_recent_days,
                    "cache_dir" => s.cache_dir.display(),
                    "metadata_memory_mb" => mib(s.metadata_memory_size_bytes),
                    "metadata_disk_gb" => gib(s.metadata_disk_size_bytes),
                    "l1_used_bytes" => s.l1_used_bytes,
                    "l2_used_bytes" => s.l2_used_bytes,
                    "entry_count" => s.entry_count,
                    "evictions" => s.evictions,
                ])
                .collect()
        });

        let p = deltalake::delta_datafusion::parquet_metrics::snapshot();
        let parquet = rows!["parquet";
            "metadata_cache_hits" => p.metadata_cache_hits,
            "metadata_cache_misses" => p.metadata_cache_misses,
            "bytes_read" => p.bytes_read,
            "read_time_us" => p.read_time_us,
            "scans" => p.scans,
            "files_planned" => p.files_planned,
            "bytes_planned" => p.bytes_planned,
            "selected_row_groups" => p.selected_row_groups,
        ];

        let cache_sizes = self.cache_sizes.as_ref().map_or_else(Vec::new, |snap| {
            // Mirror the field-level doc: these caches don't evict; size
            // tracks unique (project, table) pairs since process start.
            let (fast_resolve, provider) = snap();
            rows!["scan"; "fast_resolve_cache_entries" => fast_resolve, "provider_cache_entries" => provider]
        });

        let logical_count = self.logical_count.as_ref().map_or_else(Vec::new, |snap| {
            let (entries, resident, limit, building) = snap();
            rows!["logical_count";
                "resident_partitions" => entries,
                "resident_bytes_estimated" => resident,
                "resident_mb_estimated" => mib(resident),
                "resident_limit_bytes" => limit,
                "resident_limit_mb" => mib(limit),
                "active_builds" => building,
            ]
        });

        let rows: Vec<Row> = [budget, layer, dml, maintenance, plan_cache, scan, foyer, logical_count, parquet, cache_sizes].into_iter().flatten().collect();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(rows.iter().map(|r| Some(r.0)).collect::<StringArray>()),
            Arc::new(rows.iter().map(|r| Some(r.1.as_str())).collect::<StringArray>()),
            Arc::new(rows.iter().map(|r| Some(r.2.as_str())).collect::<StringArray>()),
        ];
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

    type OwnedRow = (String, String, String);

    fn snapshot_rows(p: &StatsTableProvider) -> Vec<OwnedRow> {
        let batch = p.snapshot_batch().unwrap();
        let col = |i: usize| batch.column(i).as_any().downcast_ref::<StringArray>().cloned().unwrap();
        let (components, keys, values) = (col(0), col(1), col(2));
        (0..batch.num_rows()).map(|i| (components.value(i).to_string(), keys.value(i).to_string(), values.value(i).to_string())).collect()
    }

    fn assert_has(rows: &[OwnedRow], component: &str, key: &str) {
        assert!(rows.iter().any(|(c, k, _)| c == component && k == key), "missing {component}.{key}");
    }

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
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_foyer_stats(Arc::new(move || snapshot.clone())));

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
            assert_has(&rows, "foyer", key);
        }
        assert!(rows.contains(&("foyer".into(), "cache_dir".into(), "/cache".into())));
    }

    #[test]
    fn exposes_logical_count_residency_and_build_activity() {
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_logical_count(Arc::new(|| (3, 42, 100, 1))));
        for key in ["resident_partitions", "resident_bytes_estimated", "resident_mb_estimated", "resident_limit_bytes", "resident_limit_mb", "active_builds"] {
            assert_has(&rows, "logical_count", key);
        }
        assert!(rows.contains(&("logical_count".into(), "active_builds".into(), "1".into())));
    }

    #[test]
    fn exposes_dml_retry_outcomes() {
        let rows = snapshot_rows(&StatsTableProvider::new(None).with_scan_metrics(Arc::new(ScanMetrics::default())));

        for (component, key) in [
            ("dml", "occ_conflicts_total"),
            ("dml", "retry_successes_total"),
            ("dml", "retry_exhausted_total"),
            ("maintenance", "dedup_timed_out_total"),
            ("maintenance", "light_optimize_timed_out_total"),
            ("maintenance", "cron_long_running_total"),
            ("parquet", "metadata_cache_hits"),
            ("parquet", "bytes_read"),
            ("scan", "provider_build_us_avg"),
            ("scan", "provider_scan_us_avg"),
            ("scan", "mem_plan_us_avg"),
            ("scan", "hot_plan_us_avg"),
        ] {
            assert_has(&rows, component, key);
        }
    }
}
