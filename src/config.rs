use std::{collections::HashMap, path::PathBuf, sync::OnceLock, time::Duration};

use serde::Deserialize;

static CONFIG: OnceLock<AppConfig> = OnceLock::new();

/// Bytes per MiB / GiB — used by the `*_bytes()` size accessors below so the
/// `* 1024 * 1024` chains don't repeat (and read as the unit they mean).
const MIB: usize = 1024 * 1024;
const GIB: usize = 1024 * 1024 * 1024;

/// Load config from environment variables.
pub fn load_config_from_env() -> Result<AppConfig, envy::Error> {
    // Load each sub-config separately to avoid #[serde(flatten)] issues with envy
    // See: https://github.com/softprops/envy/issues/26
    Ok(AppConfig {
        aws: envy::from_env()?,
        core: envy::from_env()?,
        buffer: envy::from_env()?,
        cache: envy::from_env()?,
        parquet: envy::from_env()?,
        maintenance: envy::from_env()?,
        memory: envy::from_env()?,
        telemetry: envy::from_env()?,
        tantivy: envy::from_env()?,
    })
}

/// Initialize global config from environment (for production use).
pub fn init_config() -> Result<&'static AppConfig, envy::Error> {
    if let Some(cfg) = CONFIG.get() {
        return Ok(cfg);
    }
    let mut config = load_config_from_env()?;
    crate::autotune::apply(&mut config);
    let _ = CONFIG.set(config);
    Ok(CONFIG.get().unwrap())
}

/// Get global config. Panics if not initialized.
pub fn config() -> &'static AppConfig {
    CONFIG.get().expect("Config not initialized. Call init_config() first.")
}

/// Whether the operator has opted into open auth for local dev via
/// `TIMEFUSION_ALLOW_INSECURE_AUTH=true`. Both the pgwire and gRPC auth
/// paths gate their fail-secure defaults on this flag.
pub fn is_insecure_auth_allowed() -> bool {
    std::env::var("TIMEFUSION_ALLOW_INSECURE_AUTH").map(|v| v.eq_ignore_ascii_case("true")).unwrap_or(false)
}

// Macro to generate const default functions for serde
macro_rules! const_default {
    ($name:ident: bool = $val:expr) => {
        fn $name() -> bool {
            $val
        }
    };
    ($name:ident: u64 = $val:expr) => {
        fn $name() -> u64 {
            $val
        }
    };
    ($name:ident: u16 = $val:expr) => {
        fn $name() -> u16 {
            $val
        }
    };
    ($name:ident: u32 = $val:expr) => {
        fn $name() -> u32 {
            $val
        }
    };
    ($name:ident: i32 = $val:expr) => {
        fn $name() -> i32 {
            $val
        }
    };
    ($name:ident: i64 = $val:expr) => {
        fn $name() -> i64 {
            $val
        }
    };
    ($name:ident: usize = $val:expr) => {
        fn $name() -> usize {
            $val
        }
    };
    ($name:ident: f64 = $val:expr) => {
        fn $name() -> f64 {
            $val
        }
    };
    ($name:ident: String = $val:expr) => {
        fn $name() -> String {
            $val.into()
        }
    };
    ($name:ident: PathBuf = $val:expr) => {
        fn $name() -> PathBuf {
            PathBuf::from($val)
        }
    };
}

// All default value functions using the macro
const_default!(d_true: bool = true);
const_default!(d_s3_endpoint: String = "https://s3.amazonaws.com");
const_default!(d_data_dir: PathBuf = "./data");
const_default!(d_pgwire_port: u16 = 5432);
const_default!(d_grpc_port: u16 = 50051);
const_default!(d_table_prefix: String = "timefusion");
const_default!(d_batch_queue_capacity: usize = 100_000_000);
const_default!(d_pgwire_user: String = "postgres");
// 60s (was 300s): a shorter flush interval bounds how much un-flushed WAL a
// restart must replay — startup/redeploy downtime is dominated by WAL replay
// (95.6s for 46,692 entries at 300s on 2026-07-13), and it scales ~linearly
// with this interval. Trade-off: ~5x more Delta commits / small files (handled
// by compaction/OPTIMIZE).
const_default!(d_flush_interval: u64 = 60);
const_default!(d_retention_mins: u64 = 70);
const_default!(d_eviction_interval: u64 = 60);
const_default!(d_buffer_max_memory: usize = 4096);
const_default!(d_wal_shards_per_topic: usize = 4);
// Total graceful-shutdown budget shared by ALL serial shutdown phases
// (PGWire drain → gRPC drain → buffered-layer flush + cursor snapshot).
// Set to ~80% of the orchestrator's SIGTERM→SIGKILL grace (Docker/CapRover
// `StopGracePeriod`; prod is 60s) so the clean cursor snapshot always lands
// before SIGKILL — the previous per-phase 180s ceilings assumed 540s of
// grace nobody configured, and PGWire drain alone could eat the real grace
// before the flush or snapshot ever started (2026-06-11 deploy). Anything
// unflushed at the deadline is durable in the WAL and replays on next boot.
const_default!(d_stop_grace: u64 = 50);
const_default!(d_wal_corruption_threshold: usize = 10);
// Concurrent staged flush commits. Parquet encode + S3 upload happen outside
// the per-table commit lock (see insert_records_batch staged path), so this scales
// upload throughput directly — the dominant steady-state drain lever under
// backfill. 8 doubles concurrency over the old 4 while bounding in-flight
// encode memory; raise further (env) if CPU/R2 headroom allows.
const_default!(d_flush_parallelism: usize = 8);
// Cold-boot Delta cursor reconciliation. R2 happily takes 64+ concurrent
// gets per bucket; the original 8 left ~8× headroom. Depth 8 is half the
// original 16 (the snapshot replaces the bulk of the scan) but keeps a
// safety margin: if a few snapshot writes failed silently before reboot,
// depth-2 could miss the legitimate cursor advance. Tune via env if the
// fallback Delta scan is the bottleneck.
const_default!(d_delta_scan_concurrency: usize = 64);
const_default!(d_delta_scan_depth: usize = 8);
const_default!(d_wal_fsync_ms: u64 = 200);
// MemBuffer bucket window (seconds). Smaller windows free RAM sooner because
// the previous bucket becomes flushable sooner; larger windows amortize into
// fewer/larger Delta commits. Default 300s (5 min) — halved from the historical
// 600s to cut peak MemBuffer footprint (the current bucket is excluded from
// flushing, so this is the floor on how long a row accumulates in RAM). The
// trade-off is ~2× Delta commits / small files; high-throughput tenants can go
// lower still (60–120s), memory-relaxed deployments can raise it back.
const_default!(d_bucket_duration_secs: u64 = 300);
// Memory pressure threshold (0–100) at which the flush task is woken
// independently of the periodic flush timer. Triggers an early
// `flush_completed_buckets` so MemBuffer drains before reservation reaches
// the hard limit. 0 disables pressure-triggered flushes.
const_default!(d_pressure_flush_pct: u32 = 75);
// Max seconds an insert applies backpressure (synchronously flushing
// MemBuffer → Delta to free RAM) before failing, when the memory hard limit
// is hit. The rows are already durable in the WAL, so this trades a slow
// write for a rejected one — the right call for a TS DB whose producers DLQ
// on rejection. 0 restores the old fail-fast behavior. 60s is long enough to
// ride out a flush cycle / drain a replayed backlog, finite so a genuinely
// down Delta can't pile blocked writers up without bound.
const_default!(d_write_backpressure_secs: u64 = 60);
// DML coalescing (0 = disabled). When > 0, the Delta leg of `UPDATE ... FROM`
// statements is deferred and batched: sources accumulate per (project, table,
// statement shape) and a background task merges them every N seconds, cutting
// one-Delta-commit-per-statement churn (which starves OPTIMIZE via OCC and
// piles up small files) down to a few commits per interval. The in-memory leg
// still applies synchronously, so reads that overlay the buffer stay
// read-your-writes. CONTRACT: statements must be idempotent under
// re-application (e.g. guard appends with `NOT (col @> val)`), because a row
// flushed between the mem leg and the drain sees the assignment applied
// twice, and a failed drain retries whole groups. Timestamp-range conjuncts
// are widened to the union across coalesced statements.
const_default!(d_dml_coalesce_secs: u64 = 0);
// Watchdog for a single bucket's Delta commit inside `flush_bucket`. A hung S3
// commit / commit-lock wait otherwise pins `flush_lock` forever with no log:
// flushes freeing zero memory while inserts wedge at the hard limit (prod
// 2026-07-01 — 0 flushes, 0 errors, 1300+ rejects). On timeout the flush errors
// (counted in flush_failed + flush_stalled), releasing the lock so relief
// retries; rows stay in MemBuffer + WAL, so it's safe. Must exceed a normal
// backfill commit but stay well under retention.
//
// 600s, NOT 120s: the timeout covers the whole callback — parquet encode +
// S3 upload + commit — and a post-restart WAL-replay backlog produces
// multi-GB coalesced commits that legitimately take minutes. At 120s the
// watchdog aborted every big drain, wasting the work and retrying forever
// while MemBuffer grew to the memcg limit (2026-07-02 OOM loop: stalled=42
// vs flush-ok=22, RSS 89GB). The watchdog exists for the *infinitely* hung
// commit, so a generous ceiling loses nothing.
const_default!(d_flush_bucket_timeout_secs: u64 = 600);
// Durability mode for the WAL. One of:
//   "ms"        — async fsync every `wal_fsync_ms` (default; ~200ms loss window)
//   "sync_each" — fsync after every entry (zero data-loss window, ~1ms per write)
//   "none"      — never fsync (test/throwaway data only)
const_default!(d_wal_fsync_mode: String = "ms");
const_default!(d_wal_max_files: usize = 200);
const_default!(d_foyer_memory_mb: usize = 1024);
// Local disk is cheap and fast relative to S3 GETs, so default the cache large
// — servers run 500GB–1TB cache volumes. foyer creates the backing file sparse
// (no upfront allocation), but this is the logical ceiling at which it starts
// evicting, so it MUST stay <= the cache volume's free space or writes hit
// ENOSPC before eviction kicks in. Lower it on smaller disks.
const_default!(d_foyer_disk_gb: usize = 500);
const_default!(d_foyer_ttl: u64 = 604_800); // 7 days
const_default!(d_foyer_shards: usize = 8);
const_default!(d_foyer_file_size_mb: usize = 32);
const_default!(d_foyer_stats: String = "true");
const_default!(d_metadata_size_hint: usize = 1_048_576);
// DataFusion's in-process decoded-parquet-metadata cache (footer + page index).
// Distinct from the Foyer footer-BYTES cache: this holds the decoded
// ParquetMetaData so repeat scans skip re-parsing. Entries larger than the
// limit are silently dropped, so it must comfortably exceed a single file's
// metadata; the DataFusion default is only 50MB.
const_default!(d_df_metadata_cache_mb: usize = 512);
const_default!(d_metadata_memory_mb: usize = 512);
const_default!(d_metadata_disk_gb: usize = 5);
const_default!(d_metadata_shards: usize = 4);
const_default!(d_warm_inline_max_mb: usize = 0);
const_default!(d_foyer_block_size_mb: usize = 256);
const_default!(d_l1_max_entry_mb: usize = 16);
const_default!(d_cache_recent_days: usize = 8);
const_default!(d_page_rows: usize = 20_000);
const_default!(d_zstd_level: i32 = 3);
// Tiered compression by partition age. Hot writes prioritize ingest latency;
// older data is rewritten at progressively higher levels by `recompress_tier`.
const_default!(d_zstd_level_warm: i32 = 9);
const_default!(d_zstd_level_cold: i32 = 19);
const_default!(d_cold_cutoff_days: u64 = 14);
const_default!(d_recompress_schedule: String = "0 0 3 * * *");
const_default!(d_row_group_size: usize = 134_217_728); // 128MB
const_default!(d_checkpoint_interval: u64 = 10);
// 256MB compacted-file target: fewer, larger files cut Delta metadata, S3
// object count, and the per-commit get_file_uris() walk on the flush append
// path; sorted + page-indexed files still prune time-range queries within a
// file, so the query downside is minimal for this (project_id,date)-partitioned
// workload. Light/today optimize keeps its own 16MB target.
const_default!(d_optimize_target: i64 = 256 * 1024 * 1024);
// Cold tier: sealed partitions (older than `cold_optimize_after_days`) bin-pack
// to 1GB. File size grows with partition age — recent days stay at 256MB (less
// rewrite while the day still fills), sealed days consolidate to 1GB so the
// Delta checkpoint (≈ live file count) shrinks, which is the dominant driver of
// commit latency. Compression is per-row-group, so 1GB files don't change bytes
// stored — the win is fewer files (smaller checkpoint, fewer S3 objects, cheaper
// query planning). Re-runs are cheap: Compact skips files already ≥ target.
const_default!(d_cold_optimize_target: i64 = 1024 * 1024 * 1024);
// 1 day = everything past the current (day-partitioned) partition. Only today
// still takes writes, so every sealed day consolidates to 1GB. The warm
// optimize is clamped to dates newer than this boundary (see `optimize_table`)
// so the 30-min Z-order never fragments these 1GB files back to 256MB.
const_default!(d_cold_optimize_after_days: u64 = 1);
const_default!(d_stats_cache_size: usize = 50);
// Observability data is high-churn and rarely time-traveled; the only hard
// floor is that retention must outlive any in-flight query (which holds a Delta
// snapshot referencing files vacuum would delete). With no query running beyond
// ~1h, 24h is still a 24x safety margin. This value also drives
// `delta.deletedFileRetentionDuration` (set at create + reconciled at load):
// Remove tombstones stay in every checkpoint for this long, and prod's 5-min
// compaction churn at the 7-day delta default accumulated 38.5k tombstones
// (93% of checkpoint actions) replayed on every snapshot refresh.
const_default!(d_vacuum_retention: u64 = 24);
// Delta _delta_log (transaction-log) retention. Keeps the log directory small
// (~commit-rate × retention files) so every commit's version-discovery LIST stays
// cheap. Delta's default is 30 DAYS — which let the log grow to 68k objects and
// made each commit's list take ~35s (2026-06-25 DLQ-drain incident). Even 1 day
// regrew to ~6.7k objects (~3-5s/commit) under the multi-tenant per-project commit
// rate (2026-06-26), so hold a tighter 6h window. enableExpiredLogCleanup (default
// true) prunes during checkpoints; cross-project flush coalescing cuts the commit
// rate that drives growth.
const_default!(d_log_retention: u64 = 6);
const_default!(d_optimize_window_hours: u64 = 48);
const_default!(d_compact_min_files: usize = 5);
// Hot/today target stays small (32MB): the light job runs every 5 min on the
// current partition, which takes constant writes — a large target would rewrite
// the same growing files repeatedly (write amplification) and add in-process
// merge memory on the hot path. Consolidation to 256MB/1GB happens later, once
// the partition is sealed (warm optimize → daily cold consolidate).
const_default!(d_light_optimize_target: i64 = 32 * 1024 * 1024);
const_default!(d_optimize_concurrency: usize = 4);
const_default!(d_light_schedule: String = "0 */5 * * * *");
const_default!(d_optimize_schedule: String = "0 */30 * * * *");
// Daily cold consolidation sweep (02:30): bin-pack sealed partitions to the 1GB
// cold target. Calendar-age driven; idempotent (skips ≥-target files).
const_default!(d_consolidate_schedule: String = "0 30 2 * * *");
// Every 6h (not daily): tombstones leave checkpoints once older than the
// retention property, so vacuum must visit often enough to delete the files
// before their tombstones are pruned — a daily cadence against a 24h
// retention orphans most of a day's churn (VacuumMode::Full backstops any
// that slip through).
const_default!(d_vacuum_schedule: String = "0 15 */6 * * *");
// Out-of-band checkpoint + expired-log cleanup. These run in the delta-rs
// post-commit hook by default, but a hook failure (R2 500 on the checkpoint PUT
// or the bulk log ?delete) surfaces as a commit error AFTER the commit landed,
// which the flush path misreads as a failed commit and then deletes the
// committed parquet (2026-07-09 incident). Disabled on the commit path
// (base_commit_properties) and driven here instead, every 2 min, tolerant of
// R2 500s. Faster than the ~1.5s commit cadence so the log stays bounded.
const_default!(d_checkpoint_schedule: String = "0 */2 * * * *");
// Reconcile active Add entries against object-store truth: HEAD every live file
// and commit Remove actions for any that are missing. Repairs dangling Adds
// left by past commit-path parquet deletions (the 2026-07-09 incident left 14);
// a nonzero removal count means committed data was destroyed elsewhere.
const_default!(d_reconcile_schedule: String = "0 0 * * * *");
const_default!(d_warm_recency_days: u64 = 2);
// 16: at concurrency 4 prod's 3.1k-file boot warm ran >55 min and was cut
// short by a restart every time; 16 finishes it in ~1-3 min. Footer GETs are
// ~64KB suffix ranges, well within R2/S3 burst limits.
const_default!(d_warm_concurrency: usize = 16);
const_default!(d_snapshot_reconcile: u64 = 500);
// Hard byte ceiling on the file set a single dedup chunk rewrite may
// materialize (sum of target parquet file sizes). Chunks over budget are
// SKIPPED loudly instead of rewritten — read-side dedup keeps queries
// correct, and the skip metric (timefusion.dedup.chunk_skipped) surfaces the
// debt. Guards against e.g. a z-ordered whole-day file (1GB+ on disk, several
// GB decompressed × copies) dragging the whole day into one rewrite.
const_default!(d_dedup_max_rewrite_bytes: u64 = 2 * 1024 * 1024 * 1024);
// 4 GiB estimated decoded footprint — a single chunk this large already
// dwarfs the DataFusion pool; larger chunks skip rather than risk the cgroup.
const_default!(d_dedup_max_decoded_bytes: u64 = 4 * 1024 * 1024 * 1024);
// 12× compressed→decoded: zstd on wide Variant/JSON otel rows routinely
// decodes 10-20×; 12 is a deliberately conservative floor.
const_default!(d_dedup_decode_inflation: u64 = 12);
// 4 KiB/row decoded estimate for otel spans (wide Variant/JSON bodies).
const_default!(d_dedup_bytes_per_row: u64 = 4096);
// Serial by default: one heavy maintenance rewrite in flight at a time.
const_default!(d_maintenance_rewrite_concurrency: usize = 1);
// How many days back (in addition to today) the dedup sweep covers. today-only
// left cross-flush dupes that landed in a prior-day partition (a late DLQ replay
// crossing midnight UTC) uncollapsed forever; 1 catches the day-boundary case.
// Arbitrarily-late replays still need read-side dedup — see the parity plan.
const_default!(d_dedup_lookback_days: u64 = 1);
const_default!(d_mem_gb: usize = 8);
const_default!(d_mem_fraction: f64 = 0.9);
const_default!(d_query_partitions: usize = 0);
const_default!(d_otlp_endpoint: String = "http://localhost:4317");
const_default!(d_service_name: String = "timefusion");
fn d_service_version() -> String {
    env!("CARGO_PKG_VERSION").into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(flatten)]
    pub aws: AwsConfig,
    #[serde(flatten)]
    pub core: CoreConfig,
    #[serde(flatten)]
    pub buffer: BufferConfig,
    #[serde(flatten)]
    pub cache: CacheConfig,
    #[serde(flatten)]
    pub parquet: ParquetConfig,
    #[serde(flatten)]
    pub maintenance: MaintenanceConfig,
    #[serde(flatten)]
    pub memory: MemoryConfig,
    #[serde(flatten)]
    pub telemetry: TelemetryConfig,
    #[serde(flatten)]
    pub tantivy: TantivyConfig,
}

const_default!(d_tantivy_max_index_mb: u64 = 64);
const_default!(d_tantivy_cache_disk_gb: u64 = 4);
// Level 3: index packing is on the flush hot path; level 19 cost ~88% of a CPU
// window per flush for ~10-15% smaller output (profiled 2026-07-05).
const_default!(d_tantivy_zstd_level: i32 = 3);
const_default!(d_tantivy_min_files: usize = 2);
const_default!(d_tantivy_prefilter_max_hits: usize = 100_000);
const_default!(d_tantivy_prefilter_min_selectivity_pct: u32 = 50);

/// Tantivy sidecar-index configuration. Indexing is always-on for any
/// table whose YAML schema declares `tantivy.indexed: true` on at least
/// one field. There is no override knob — schema is the single source of
/// truth.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct TantivyConfig {
    #[serde(default = "d_tantivy_max_index_mb")]
    pub timefusion_tantivy_max_index_size_mb: u64,
    #[serde(default = "d_tantivy_cache_disk_gb")]
    pub timefusion_tantivy_cache_disk_gb: u64,
    #[serde(default = "d_tantivy_zstd_level")]
    pub timefusion_tantivy_compression_level: i32,
    #[serde(default = "d_tantivy_min_files")]
    pub timefusion_tantivy_min_files_for_pushdown: usize,
    /// If a tantivy prefilter would produce more than this many hits, skip
    /// the `id IN (...)` pushdown entirely — the IN-list itself becomes the
    /// bottleneck above this point. Default 100k.
    #[serde(default = "d_tantivy_prefilter_max_hits")]
    pub timefusion_tantivy_prefilter_max_hits: usize,
    /// If a tantivy prefilter selects more than this percentage of the
    /// indexed rows, the pushdown isn't worth the round-trip; skip it and
    /// let Delta scan with the original predicate. Default 50 (%).
    #[serde(default = "d_tantivy_prefilter_min_selectivity_pct")]
    pub timefusion_tantivy_prefilter_min_selectivity_pct: u32,
    /// Route exact `col = 'lit'` on raw-tokenized high-cardinality columns
    /// (trace_id/span_id/id/parent_id) through the tantivy id-prefilter, not
    /// just LIKE (and `IN`-lists as OR-of-terms). Correctness-safe under OR:
    /// `collect_text_match_tree` only routes a disjunction when every branch
    /// is completely covered by a text_match, and the original predicate is
    /// always preserved as the post-filter backstop. Targets the ~6× trace/
    /// span lookup gap vs the indexed PG path.
    /// Default ON; set false to revert to bloom/stats-only equality pruning.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_route_equality: bool,
    /// Startup backfill: build partition-mirrored indexes for live parquet
    /// files no successful manifest entry covers (pre-tantivy history, failed
    /// builds, files landed while the feature was off). Oldest-first, bounded
    /// concurrency; each covered file widens the windows where the prefilter
    /// can engage. Off by default — reads every uncovered file back from S3.
    #[serde(default)]
    pub timefusion_tantivy_backfill: bool,
    /// File-level scan pruning: when the prefilter engages, files whose
    /// covering index returned zero hits are excluded from the Delta scan
    /// entirely (needle queries read only the files that can match). Off
    /// switch for instant rollback to id-IN-list-only pruning.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_file_pruning: bool,
    /// Warm the local index cache with blobs whose data is at most this many
    /// days old, at startup (0 = off). Turns the cold-window download cliff
    /// into a background cost after restarts.
    #[serde(default)]
    pub timefusion_tantivy_prefetch_days: u32,
    /// Row-selection pushdown: when the prefilter engages, files whose index
    /// was built in parquet row order get a per-file ParquetAccessPlan so the
    /// reader decodes only matching row groups/rows. Off switch for instant
    /// rollback to id-IN-list-only filtering inside surviving files.
    #[serde(default = "d_true")]
    pub timefusion_tantivy_row_selection: bool,
}

impl TantivyConfig {
    /// Tables to index: schemas with `tantivy.indexed: true` on any field.
    /// Computed once from the static registry on first access (the registry
    /// is compiled-in YAML, so there's nothing to invalidate).
    fn indexed_set() -> &'static std::collections::HashSet<String> {
        static SET: std::sync::OnceLock<std::collections::HashSet<String>> = std::sync::OnceLock::new();
        SET.get_or_init(|| {
            crate::schema_loader::registry()
                .list_tables()
                .into_iter()
                .filter(|name| {
                    crate::schema_loader::registry().get(name).is_some_and(|s| s.fields.iter().any(|f| f.tantivy.as_ref().is_some_and(|t| t.indexed)))
                })
                .collect()
        })
    }
    pub fn indexed_tables(&self) -> Vec<String> {
        let mut v: Vec<String> = Self::indexed_set().iter().cloned().collect();
        v.sort();
        v
    }
    pub fn is_table_indexed(&self, table: &str) -> bool {
        Self::indexed_set().contains(table)
    }
    pub fn compression_level(&self) -> i32 {
        self.timefusion_tantivy_compression_level
    }
    pub fn prefilter_max_hits(&self) -> usize {
        self.timefusion_tantivy_prefilter_max_hits.max(1)
    }
    pub fn prefilter_min_selectivity_pct(&self) -> u32 {
        self.timefusion_tantivy_prefilter_min_selectivity_pct.min(100)
    }
    pub fn route_equality(&self) -> bool {
        self.timefusion_tantivy_route_equality
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AwsConfig {
    #[serde(default)]
    pub aws_access_key_id: Option<String>,
    #[serde(default)]
    pub aws_secret_access_key: Option<String>,
    #[serde(default)]
    pub aws_default_region: Option<String>,
    #[serde(default = "d_s3_endpoint")]
    pub aws_s3_endpoint: String,
    #[serde(default)]
    pub aws_s3_bucket: Option<String>,
    #[serde(default)]
    pub aws_allow_http: Option<String>,
    /// TCP/TLS connection-establishment bound passed to the object_store S3
    /// client (humantime format, e.g. "15s"). `Option` so the derived
    /// `AwsConfig::default()` stays valid (an empty string wouldn't parse);
    /// see `connect_timeout`/`request_timeout` for the effective defaults.
    #[serde(default)]
    pub timefusion_s3_connect_timeout: Option<String>,
    /// Total per-request bound (humantime, e.g. "900s"). Must comfortably
    /// exceed the time to PUT one large multipart part to R2 under load —
    /// the 2026-06-24 compaction failed when 300s + concurrent big PUTs
    /// starved R2 connections. Tunable via TIMEFUSION_S3_REQUEST_TIMEOUT.
    #[serde(default)]
    pub timefusion_s3_request_timeout: Option<String>,
}

/// Coerce a bare-number timeout (e.g. `"150"`) to humantime seconds (`"150s"`).
/// object_store's `ClientConfigKey::{ConnectTimeout,Timeout}` parse strictly via
/// humantime and PANIC the process at boot on a unitless value — a prod
/// `TIMEFUSION_S3_CONNECT_TIMEOUT=150` crash-looped TF on 2026-06-24. Treat an
/// all-digit string as seconds; pass anything with a unit through untouched.
fn normalize_duration(s: String) -> String {
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) { format!("{s}s") } else { s }
}

impl AwsConfig {
    /// Effective connect timeout. R2 establishes healthy connections in <1s;
    /// a generous bound only matters when something is wrong, where it trades
    /// slower failure for surviving transient R2 connection refusals.
    pub fn connect_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_connect_timeout.clone().unwrap_or_else(|| "60s".into()))
    }

    pub fn request_timeout(&self) -> String {
        normalize_duration(self.timefusion_s3_request_timeout.clone().unwrap_or_else(|| "900s".into()))
    }

    pub fn build_storage_options(&self, endpoint_override: Option<&str>) -> HashMap<String, String> {
        macro_rules! insert_opt {
            ($opts:expr, $key:expr, $val:expr) => {
                if let Some(ref v) = $val {
                    $opts.insert($key.into(), v.clone());
                }
            };
        }

        let mut opts = HashMap::new();
        insert_opt!(opts, "AWS_ACCESS_KEY_ID", self.aws_access_key_id);
        insert_opt!(opts, "AWS_SECRET_ACCESS_KEY", self.aws_secret_access_key);
        insert_opt!(opts, "AWS_REGION", self.aws_default_region);
        insert_opt!(opts, "AWS_ALLOW_HTTP", self.aws_allow_http);
        opts.insert("AWS_ENDPOINT_URL".into(), endpoint_override.unwrap_or(&self.aws_s3_endpoint).to_string());
        // Bound connection establishment + total request time. Kept in sync
        // with create_object_store (which builds its own client) so both the
        // delta-rs register path and the direct AmazonS3Builder agree.
        opts.insert("connect_timeout".into(), self.connect_timeout());
        opts.insert("timeout".into(), self.request_timeout());
        opts
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CoreConfig {
    #[serde(default = "d_data_dir")]
    pub timefusion_data_dir: PathBuf,
    #[serde(default = "d_pgwire_port")]
    pub pgwire_port: u16,
    #[serde(default = "d_table_prefix")]
    pub timefusion_table_prefix: String,
    #[serde(default)]
    pub timefusion_config_database_url: Option<String>,
    #[serde(default = "d_true")]
    pub enable_batch_queue: bool,
    #[serde(default = "d_batch_queue_capacity")]
    pub timefusion_batch_queue_capacity: usize,
    #[serde(default = "d_pgwire_user")]
    pub pgwire_user: String,
    #[serde(default)]
    pub pgwire_password: Option<String>,
    #[serde(default = "d_grpc_port")]
    pub grpc_port: u16,
    #[serde(default)]
    pub grpc_token: Option<String>,
}

impl CoreConfig {
    pub fn wal_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("wal")
    }
    pub fn cache_dir(&self) -> PathBuf {
        self.timefusion_data_dir.join("cache")
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BufferConfig {
    #[serde(default = "d_flush_interval")]
    pub timefusion_flush_interval_secs: u64,
    #[serde(default = "d_retention_mins")]
    pub timefusion_buffer_retention_mins: u64,
    #[serde(default = "d_eviction_interval")]
    pub timefusion_eviction_interval_secs: u64,
    #[serde(default = "d_buffer_max_memory")]
    pub timefusion_buffer_max_memory_mb: usize,
    #[serde(default = "d_stop_grace")]
    pub timefusion_stop_grace_secs: u64,
    #[serde(default = "d_wal_corruption_threshold")]
    pub timefusion_wal_corruption_threshold: usize,
    #[serde(default = "d_flush_parallelism")]
    pub timefusion_flush_parallelism: usize,
    #[serde(default)]
    pub timefusion_flush_immediately: bool,
    /// EXPERIMENTAL (default OFF), parity plan Defect 1: when set, `insert()`
    /// admits over the memory hard limit instead of *rejecting* a write whose
    /// backpressure budget is exhausted — the WAL append is the durability
    /// boundary, so a slow/over-budget write is preferable to a dropped one.
    /// Closes the drop-before-durability loss seam. Requires a soak (watch RSS /
    /// flush throughput) before prod enable — over-budget admission trades a
    /// reject for unbounded growth if Delta flush can't keep up.
    #[serde(default)]
    pub timefusion_wal_admit_decouple: bool,
    #[serde(default = "d_wal_fsync_ms")]
    pub timefusion_wal_fsync_ms: u64,
    #[serde(default = "d_wal_fsync_mode")]
    pub timefusion_wal_fsync_mode: String,
    /// Fsync the WAL shard before acking DML appends (machine-crash
    /// durability). Batched INSERT appends are always flushed before ack by
    /// walrus's `batch_write`; only single-entry DML appends defer to the
    /// background fsync thread — this closes that window. Default off: an
    /// OOM/SIGKILL never loses mmap'd writes, only power loss does.
    #[serde(default)]
    pub timefusion_wal_ack_fsync: bool,
    #[serde(default = "d_wal_max_files")]
    pub timefusion_wal_max_file_count: usize,
    /// Force-flush backstop on total on-disk (unflushed) WAL bytes. Guards the
    /// cursor-lag case the memory-pressure valve misses: a stuck /
    /// persistently-retrying commit pins the WAL GC floor while buffer memory
    /// frees post-commit, so the WAL bloats without tripping memory pressure —
    /// inflating restart replay (issue #83). 0 = derive from the buffer budget.
    #[serde(default)]
    pub timefusion_wal_max_unflushed_mb: usize,
    #[serde(default = "d_bucket_duration_secs")]
    pub timefusion_bucket_duration_secs: u64,
    #[serde(default = "d_pressure_flush_pct")]
    pub timefusion_pressure_flush_pct: u32,
    #[serde(default = "d_write_backpressure_secs")]
    pub timefusion_write_backpressure_secs: u64,
    /// See `d_dml_coalesce_secs` — drain interval for deferred UPDATE ... FROM
    /// Delta merges; 0 keeps the synchronous per-statement path.
    #[serde(default = "d_dml_coalesce_secs")]
    pub timefusion_dml_coalesce_secs: u64,
    #[serde(default = "d_flush_bucket_timeout_secs")]
    pub timefusion_flush_bucket_timeout_secs: u64,
    /// WAL shards per (project, table) topic. Higher = more append parallelism
    /// at the cost of O(shards) recovery memory and more file handles.
    #[serde(default = "d_wal_shards_per_topic")]
    pub timefusion_wal_shards_per_topic: usize,
    /// Max concurrent S3/R2 reads when reconciling per-table Delta watermarks
    /// at boot. Only used when the cursor snapshot is missing or stale.
    #[serde(default = "d_delta_scan_concurrency")]
    pub timefusion_delta_scan_concurrency: usize,
    /// Per-table Delta commit history depth scanned at boot. The cursor
    /// snapshot covers the bulk of the watermark; this only needs to catch
    /// a writer that committed after the last snapshot was written.
    #[serde(default = "d_delta_scan_depth")]
    pub timefusion_delta_scan_depth: usize,
}

/// WAL durability mode. See `d_wal_fsync_mode` for the env-var encoding.
#[derive(Debug, Clone, Copy)]
pub enum WalFsyncMode {
    Milliseconds(u64),
    SyncEach,
    None,
}

impl BufferConfig {
    pub fn flush_interval_secs(&self) -> u64 {
        self.timefusion_flush_interval_secs.max(1)
    }
    pub fn retention_mins(&self) -> u64 {
        self.timefusion_buffer_retention_mins.max(1)
    }
    /// mtime age past which a WAL file is PRESUMED dead weight. This is a
    /// heuristic, not a soundness bound: replay is cursor-bounded (no age
    /// cutoff — see `recover_from_wal`), so GC soundness comes from the
    /// un-flushed floor (`gc_wal_files`'s `unflushed_floor_micros`) and the
    /// drained-gated boot sweep, NEVER from age alone. Do not tighten or
    /// bypass the floor on the strength of this age.
    pub fn wal_gc_max_age(&self) -> Duration {
        Duration::from_secs((self.retention_mins() + 20) * 60)
    }
    pub fn eviction_interval_secs(&self) -> u64 {
        self.timefusion_eviction_interval_secs.max(1)
    }
    pub fn max_memory_mb(&self) -> usize {
        self.timefusion_buffer_max_memory_mb.max(64)
    }
    pub fn wal_shards_per_topic(&self) -> usize {
        self.timefusion_wal_shards_per_topic.max(1)
    }
    pub fn wal_corruption_threshold(&self) -> usize {
        self.timefusion_wal_corruption_threshold
    }
    pub fn flush_parallelism(&self) -> usize {
        self.timefusion_flush_parallelism.max(1)
    }
    pub fn dml_coalesce_secs(&self) -> u64 {
        self.timefusion_dml_coalesce_secs
    }
    pub fn delta_scan_concurrency(&self) -> usize {
        self.timefusion_delta_scan_concurrency.max(1)
    }
    pub fn delta_scan_depth(&self) -> usize {
        self.timefusion_delta_scan_depth.max(1)
    }
    pub fn flush_immediately(&self) -> bool {
        self.timefusion_flush_immediately
    }
    pub fn wal_admit_decouple(&self) -> bool {
        self.timefusion_wal_admit_decouple
    }
    pub fn wal_fsync_ms(&self) -> u64 {
        self.timefusion_wal_fsync_ms.max(1)
    }
    pub fn wal_ack_fsync(&self) -> bool {
        self.timefusion_wal_ack_fsync
    }
    pub fn wal_fsync_mode(&self) -> WalFsyncMode {
        match self.timefusion_wal_fsync_mode.to_ascii_lowercase().as_str() {
            "sync_each" | "synceach" | "each" => WalFsyncMode::SyncEach,
            "none" | "off" | "disabled" => WalFsyncMode::None,
            _ => WalFsyncMode::Milliseconds(self.wal_fsync_ms()),
        }
    }
    pub fn wal_max_file_count(&self) -> usize {
        self.timefusion_wal_max_file_count
    }
    /// Byte ceiling for the unflushed-WAL force-flush backstop, or `None` when
    /// disabled (`TIMEFUSION_WAL_MAX_UNFLUSHED_MB` unset/0 — the default). Opt-in
    /// because the always-on memory-pressure valve already drains the common
    /// lagging-flush case; this guards the rarer stuck-cursor backlog. Set it to
    /// bound restart replay by on-disk WAL size (e.g. 12000 for the 30GB-buffer
    /// prod instance).
    pub fn wal_max_unflushed_bytes(&self) -> Option<u64> {
        (self.timefusion_wal_max_unflushed_mb > 0).then(|| (self.timefusion_wal_max_unflushed_mb as u64).saturating_mul(1024 * 1024))
    }
    pub fn bucket_duration_secs(&self) -> u64 {
        self.timefusion_bucket_duration_secs.max(1)
    }
    pub fn pressure_flush_pct(&self) -> u32 {
        self.timefusion_pressure_flush_pct.min(100)
    }
    pub fn write_backpressure_timeout(&self) -> Duration {
        Duration::from_secs(self.timefusion_write_backpressure_secs)
    }
    /// Per-bucket Delta-commit watchdog inside `flush_bucket`. 0 disables it
    /// (unbounded wait — the pre-2026-07-01 behavior).
    pub fn flush_bucket_timeout(&self) -> Duration {
        Duration::from_secs(self.timefusion_flush_bucket_timeout_secs)
    }

    /// Total graceful-shutdown budget — see `d_stop_grace`.
    pub fn stop_grace(&self) -> Duration {
        Duration::from_secs(self.timefusion_stop_grace_secs.max(1))
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "d_foyer_memory_mb")]
    pub timefusion_foyer_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disk_mb: Option<usize>,
    #[serde(default = "d_foyer_disk_gb")]
    pub timefusion_foyer_disk_gb: usize,
    #[serde(default = "d_foyer_ttl")]
    pub timefusion_foyer_ttl_seconds: u64,
    #[serde(default = "d_foyer_shards")]
    pub timefusion_foyer_shards: usize,
    #[serde(default = "d_foyer_file_size_mb")]
    pub timefusion_foyer_file_size_mb: usize,
    #[serde(default = "d_foyer_stats")]
    pub timefusion_foyer_stats: String,
    #[serde(default = "d_metadata_size_hint")]
    pub timefusion_parquet_metadata_size_hint: usize,
    /// Memory limit (MB) for DataFusion's decoded parquet-metadata cache
    /// (`datafusion.runtime.metadata_cache_limit`). See `d_df_metadata_cache_mb`.
    #[serde(default = "d_df_metadata_cache_mb")]
    pub timefusion_df_metadata_cache_mb: usize,
    #[serde(default = "d_metadata_memory_mb")]
    pub timefusion_foyer_metadata_memory_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_metadata_disk_mb: Option<usize>,
    #[serde(default = "d_metadata_disk_gb")]
    pub timefusion_foyer_metadata_disk_gb: usize,
    #[serde(default = "d_metadata_shards")]
    pub timefusion_foyer_metadata_shards: usize,
    /// Disk block size (MB) for the main data cache. The block is foyer's
    /// minimal eviction unit AND its size caps the largest entry that can land
    /// on disk — so it must be >= the largest file we want cached locally. This
    /// acts as a *floor*: `from_app_config` automatically raises the effective
    /// block size to 2x the compaction target size, so the two can't drift out
    /// of sync if an operator bumps the target. Default 256MB.
    ///
    /// Memory note: this also bounds the transient buffer each multipart-write
    /// warm holds in heap (see `timefusion_warm_inline_max_mb`). Up to
    /// `timefusion_warm_concurrency` compactions can run at once, so the worst
    /// case is `block_size_mb * warm_concurrency` of transient heap during a
    /// busy maintenance window (e.g. 256MB x 4 = 1GB). On smaller-memory
    /// instances set `timefusion_warm_inline_max_mb` to cap this independently
    /// of the on-disk block size.
    #[serde(default = "d_foyer_block_size_mb")]
    pub timefusion_foyer_block_size_mb: usize,
    /// Entries larger than this (MB) are inserted disk-only (foyer
    /// `Location::OnDisk`) so warming a big compaction output doesn't evict the
    /// hot small-entry working set from L1 memory. Small entries keep the
    /// default L1+disk placement for fastest repeat reads. 0 = always use L1.
    #[serde(default = "d_l1_max_entry_mb")]
    pub timefusion_foyer_l1_max_entry_mb: usize,
    /// Don't admit writes whose `date=` partition is older than this many days
    /// (e.g. cold-tier recompress rewrites) — recent data stays local, old data
    /// is served from S3. 0 = no age limit. Pairs with the cache TTL, which
    /// governs how long an admitted entry survives before falling back to S3.
    #[serde(default = "d_cache_recent_days")]
    pub timefusion_cache_recent_days: usize,
    /// Optional extra cap (MB) on the in-flight buffer used to warm the cache
    /// directly from a multipart write (so we don't re-download a file we just
    /// streamed to S3). Always bounded by the disk block size; 0 = bound only
    /// by the block size.
    #[serde(default = "d_warm_inline_max_mb")]
    pub timefusion_warm_inline_max_mb: usize,
    #[serde(default)]
    pub timefusion_foyer_disabled: bool,
}

impl CacheConfig {
    pub fn is_disabled(&self) -> bool {
        self.timefusion_foyer_disabled
    }
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.timefusion_foyer_ttl_seconds)
    }
    pub fn stats_enabled(&self) -> bool {
        self.timefusion_foyer_stats.eq_ignore_ascii_case("true")
    }
    pub fn memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_memory_mb * MIB
    }
    pub fn disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_disk_mb.map_or(self.timefusion_foyer_disk_gb * GIB, |mb| mb * MIB)
    }
    pub fn file_size_bytes(&self) -> usize {
        self.timefusion_foyer_file_size_mb * MIB
    }
    pub fn metadata_memory_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_memory_mb * MIB
    }
    pub fn warm_inline_max_bytes(&self) -> usize {
        self.timefusion_warm_inline_max_mb * MIB
    }
    pub fn block_size_bytes(&self) -> usize {
        self.timefusion_foyer_block_size_mb * MIB
    }
    pub fn l1_max_entry_bytes(&self) -> usize {
        self.timefusion_foyer_l1_max_entry_mb * MIB
    }
    pub fn metadata_disk_size_bytes(&self) -> usize {
        self.timefusion_foyer_metadata_disk_mb.map_or(self.timefusion_foyer_metadata_disk_gb * GIB, |mb| mb * MIB)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParquetConfig {
    #[serde(default = "d_page_rows")]
    pub timefusion_page_row_count_limit: usize,
    /// ZSTD level for hot writes (flush + today's light optimize). Default 3.
    /// Aliased by the legacy env name; lower = faster ingest.
    #[serde(default = "d_zstd_level", alias = "timefusion_zstd_level_hot")]
    pub timefusion_zstd_compression_level: i32,
    #[serde(default = "d_zstd_level_warm")]
    pub timefusion_zstd_level_warm: i32,
    #[serde(default = "d_zstd_level_cold")]
    pub timefusion_zstd_level_cold: i32,
    #[serde(default = "d_cold_cutoff_days")]
    pub timefusion_cold_cutoff_days: u64,
    #[serde(default = "d_row_group_size")]
    pub timefusion_max_row_group_size: usize,
    #[serde(default = "d_checkpoint_interval")]
    pub timefusion_checkpoint_interval: u64,
    #[serde(default = "d_optimize_target")]
    pub timefusion_optimize_target_size: i64,
    #[serde(default = "d_cold_optimize_target")]
    pub timefusion_cold_optimize_target_size: i64,
    #[serde(default = "d_cold_optimize_after_days")]
    pub timefusion_cold_optimize_after_days: u64,
    #[serde(default = "d_stats_cache_size")]
    pub timefusion_stats_cache_size: usize,
    #[serde(default)]
    pub timefusion_bloom_filter_disabled: bool,
}

impl ParquetConfig {
    /// Warm/cold boundary in days, floored at 1: the current (day-partitioned)
    /// partition must always stay warm — it's still taking writes — so 0 is
    /// never valid (it would consolidate today to 1GB mid-write).
    pub fn cold_optimize_after_days(&self) -> u64 {
        self.timefusion_cold_optimize_after_days.max(1)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceConfig {
    #[serde(default = "d_vacuum_retention")]
    pub timefusion_vacuum_retention_hours: u64,
    #[serde(default = "d_log_retention")]
    pub timefusion_log_retention_hours: u64,
    #[serde(default = "d_optimize_window_hours")]
    pub timefusion_optimize_window_hours: u64,
    /// Use Z-order clustering for the periodic full OPTIMIZE (the 30-min,
    /// window-wide job). Default OFF: Z-order runs a memory-heavy global sort
    /// that exhausts the pool on large windows (prod 2026-07-12), and its
    /// space-filling curve actually loosens timestamp locality. Plain Compact
    /// bin-packs the flush's already-timestamp-sorted, time-bucketed files,
    /// preserving tight per-row-group timestamp ranges — the dominant query
    /// predicate — with no global sort. (The cold-tier recompress path keeps
    /// Z-order independently: it relies on the full-file rewrite to recompress.)
    /// Re-enable only once Z-order's memory footprint is bounded.
    #[serde(default)]
    pub timefusion_optimize_use_zorder: bool,
    /// Rewrite optimize/compact/recompress output sorted by the schema's
    /// `sorting_columns` (delta-rs `OptimizeType::SortBy`) with an honest DESC
    /// footer, so the timestamp-ordering/LIMIT pushdown keeps firing on
    /// optimized/compacted partitions (not just fresh flush files). Default ON:
    /// without it every rewrite path concatenates (`declare_sorted=false`),
    /// strips the footer, and the reader's all-or-nothing ordering rule disables
    /// the top-N pushdown for the whole partition within one compaction cycle.
    ///
    /// Memory: `SortBy` reads each partition through the ordering-advertising
    /// `DeltaScanNext` and runs `df.sort()`. Once every file in the partition
    /// carries an honest sorted footer (guaranteed by the flush path's
    /// `sort_batches_by_schema` + prior SortBy rewrites), DataFusion elides the
    /// blocking sort into a streaming `SortPreservingMergeExec` — a k-way merge
    /// holding ~one batch per file, bounded regardless of partition size. The
    /// 2026-07-14 OOM ("Not enough memory to continue external sort") was
    /// `df.sort()` over *unsorted* inputs (the heterogeneous-flush-file bug,
    /// since fixed) forcing a full-partition blocking sort. TRANSITION CAVEAT:
    /// the first compaction of a partition still holding legacy unsorted files
    /// is a one-time blocking sort (bounded by the maintenance FairSpillPool +
    /// 64 MB reservation + disk spill, partitions capped at 4); after it the
    /// partition is sorted and every later compaction is the streaming merge.
    /// Set to `false` (plain Compact) only if a deployment can't afford even
    /// that one-time transition sort.
    #[serde(default = "d_true")]
    pub timefusion_optimize_sort_by: bool,
    #[serde(default = "d_compact_min_files")]
    pub timefusion_compact_min_files: usize,
    #[serde(default = "d_light_optimize_target")]
    pub timefusion_light_optimize_target_size: i64,
    /// Concurrent merge tasks per optimize run. delta-rs defaults to
    /// num_cpus (48 on prod), where each task holds decompressed batches
    /// plus a zstd writer buffer — 2026-06-11 this OOM-killed the process
    /// every optimize tick once small files accumulated.
    #[serde(default = "d_optimize_concurrency")]
    pub timefusion_optimize_max_concurrent_tasks: usize,
    #[serde(default = "d_light_schedule")]
    pub timefusion_light_optimize_schedule: String,
    #[serde(default = "d_optimize_schedule")]
    pub timefusion_optimize_schedule: String,
    #[serde(default = "d_consolidate_schedule")]
    pub timefusion_consolidate_schedule: String,
    #[serde(default = "d_vacuum_schedule")]
    pub timefusion_vacuum_schedule: String,
    #[serde(default = "d_recompress_schedule")]
    pub timefusion_recompress_schedule: String,
    /// Out-of-band checkpoint + expired-log-cleanup schedule. See d_checkpoint_schedule.
    #[serde(default = "d_checkpoint_schedule")]
    pub timefusion_checkpoint_schedule: String,
    /// Dangling-Add reconcile schedule. See d_reconcile_schedule.
    #[serde(default = "d_reconcile_schedule")]
    pub timefusion_reconcile_schedule: String,
    /// Proactively warm the Foyer cache for files written by a flush/optimize
    /// commit, so recent partitions dashboards read don't cold-start after
    /// every compaction. Footers are always warmed when enabled.
    #[serde(default = "d_true")]
    pub timefusion_warm_after_compaction: bool,
    /// In addition to footers, warm the full file contents into the main
    /// (full-file) cache. Off by default — footers carry most of the
    /// planning-latency win at a fraction of the bytes; enable for data-read
    /// warmth on the hottest partitions.
    #[serde(default)]
    pub timefusion_warm_full_files: bool,
    /// Only warm files whose `date=` partition is within this many days of
    /// today. Bounds warming to the partitions dashboards actually query.
    /// 0 = no recency limit.
    #[serde(default = "d_warm_recency_days")]
    pub timefusion_warm_recency_days: u64,
    /// Warm parquet footers for EVERY live file (not just recency-window
    /// ones). Footers are tens of KB each, but on tables with thousands of
    /// files the boot-time GET burst may matter on small instances — disable
    /// to fall back to recency-bounded footer warming.
    #[serde(default = "d_true")]
    pub timefusion_warm_all_footers: bool,
    /// Max concurrent warm fetches per commit. Bounds the S3 GET burst a
    /// warm job adds right after a compaction.
    #[serde(default = "d_warm_concurrency")]
    pub timefusion_warm_concurrency: usize,
    /// After a compaction commit, proactively evict the cached full-file bytes
    /// of the files it tombstoned (no longer in the live set), instead of
    /// waiting for VACUUM / TTL / LRU to reclaim them. Cheap (in-cache only, no
    /// S3) and keeps the cache from filling with dead compaction outputs.
    #[serde(default = "d_true")]
    pub timefusion_evict_after_compaction: bool,
    /// Advance the post-commit snapshot by appending only the files the commit
    /// added, instead of re-materializing the whole active file set (2-8s over
    /// 26k files every flush in prod). Produces an identical file set — a
    /// faster, equivalent replay, safe regardless of writer count. Off reverts
    /// to the full re-materialize per commit.
    #[serde(default = "d_true")]
    pub timefusion_incremental_snapshot: bool,
    /// Belt-and-suspenders for the above: every Nth commit per table, drop the
    /// materialized files and re-materialize from S3 truth, bounding any drift
    /// from an incremental-replay bug. 0 disables reconciliation.
    #[serde(default = "d_snapshot_reconcile")]
    pub timefusion_snapshot_reconcile_commits: u64,
    /// Days back (plus today) the dedup sweep scans. See `d_dedup_lookback_days`.
    #[serde(default = "d_dedup_lookback_days")]
    pub timefusion_dedup_lookback_days: u64,
    /// Skip the read-side DedupExec (and restore per-scan LIMIT pushdown) for
    /// Delta-only queries whose every in-window (project, date) partition was
    /// verified duplicate-free by a sweep pass AND whose file set is unchanged
    /// since (fingerprint match). Off by default until COUNT parity is
    /// validated on prod-shaped data.
    #[serde(default)]
    pub timefusion_read_dedup_skip_swept: bool,
    /// Answer gate-eligible `SELECT COUNT(*) ... WHERE project_id AND
    /// timestamp range` from Delta add-action stats (zero parquet IO). Only
    /// fires when the window is fully flushed, dedup-provably-clean, and
    /// every overlapping file lies entirely inside the window — otherwise
    /// the normal scan runs. See src/count_pushdown.rs.
    #[serde(default = "d_true")]
    pub timefusion_count_pushdown: bool,
    /// Per-shard COMPRESSED-bytes target for a dedup chunk rewrite (`sum(add.size)`).
    /// The rewrite is split into `ceil(compressed_bytes / this)` md5-bucketed passes
    /// so each pass reads ~this much. 0 disables this ceiling's contribution to the
    /// shard count. See `d_dedup_max_rewrite_bytes`.
    #[serde(default = "d_dedup_max_rewrite_bytes")]
    pub timefusion_dedup_max_rewrite_bytes: u64,
    /// Per-shard target on the ESTIMATED DECODED (in-memory Arrow) footprint of a
    /// dedup chunk rewrite. Compressed bytes under-count by 5-20× for wide
    /// Variant/JSON columns, and the `SELECT * … collect()` Arrow buffers are NOT
    /// accounted by DataFusion's memory pool — so a compressed-under-budget chunk
    /// once decoded to tens of GB and OOM-killed the process (prod 2026-07-04, 89GB
    /// cgroup kill). The rewrite now SHARDS by an md5 hash of the dedup keys into
    /// `ceil(est_decoded / this)` passes so each pass materializes ~this much; a
    /// single key group that alone exceeds this is unshardable and skipped (read-side
    /// dedup keeps queries correct). 0 → one shard for this ceiling. See
    /// `d_dedup_max_decoded_bytes`.
    #[serde(default = "d_dedup_max_decoded_bytes")]
    pub timefusion_dedup_max_decoded_bytes: u64,
    /// Compressed→decoded inflation factor used to estimate a dedup chunk's
    /// in-memory footprint when per-file `num_records` stats are unavailable.
    /// See `d_dedup_decode_inflation`.
    #[serde(default = "d_dedup_decode_inflation")]
    pub timefusion_dedup_decode_inflation: u64,
    /// Estimated decoded Arrow bytes per row, used with per-file `num_records`
    /// to size a dedup chunk's in-memory footprint. otel spans carry wide
    /// Variant/JSON bodies; 4 KiB is a conservative average. See
    /// `d_dedup_bytes_per_row`.
    #[serde(default = "d_dedup_bytes_per_row")]
    pub timefusion_dedup_bytes_per_row: u64,
    /// Max concurrent heavy maintenance rewrites (dedup / optimize / recompress)
    /// that may materialize Arrow at once. 1 = strictly serial. Backstop for
    /// decoded-footprint mis-estimates: their Arrow memory is invisible to the
    /// DataFusion pool, so aggregate concurrency is the real bound. See
    /// `d_maintenance_rewrite_concurrency`.
    #[serde(default = "d_maintenance_rewrite_concurrency")]
    pub timefusion_maintenance_rewrite_concurrency: usize,
}

/// Which DataFusion `MemoryPool` to back the runtime with.
///
/// - `Greedy` (default): all consumers share the full pool; first-come,
///   first-served. Right for write-heavy workloads where INSERTs dominate
///   and per-statement memory needs vary widely (e.g. one batch is 50 MB
///   of Arrow, another is 5 MB). FairSpillPool would slice the pool into
///   per-consumer quotas (`pool / num_consumers`) and reject any consumer
///   whose batch exceeded its slot — bit us in prod on 2026-05-28 when
///   ~30 concurrent INSERTs each got a ~76 MB slot and every 700-row
///   batch hit `Memory limit exceeded`.
/// - `FairSpill`: slot-per-consumer fairness. Better for ad-hoc query
///   workloads with many concurrent users where one large query
///   shouldn't starve the others. Not the right default for ingest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPoolKind {
    Greedy,
    FairSpill,
}

fn d_memory_pool() -> MemoryPoolKind {
    MemoryPoolKind::Greedy
}

#[derive(Debug, Clone, Deserialize)]
pub struct MemoryConfig {
    #[serde(default = "d_mem_gb")]
    pub timefusion_memory_limit_gb: usize,
    #[serde(default = "d_mem_fraction")]
    pub timefusion_memory_fraction: f64,
    #[serde(default)]
    pub timefusion_sort_spill_reservation_bytes: Option<usize>,
    #[serde(default = "d_memory_pool")]
    pub timefusion_memory_pool: MemoryPoolKind,
    #[serde(default = "d_true")]
    pub timefusion_tracing_record_metrics: bool,
    /// DataFusion `target_partitions` for query + maintenance sessions. 0 =
    /// auto: `autotune::apply()` derives it from the container's CPU quota
    /// (num_cpus ignores the CFS quota, oversubscribing throttled containers).
    /// A non-zero env (`TIMEFUSION_QUERY_PARTITIONS`) wins. 0 also when unset in
    /// tests → sessions keep DataFusion's default.
    #[serde(default = "d_query_partitions")]
    pub timefusion_query_partitions: usize,
}

impl MemoryConfig {
    pub fn memory_limit_bytes(&self) -> usize {
        self.timefusion_memory_limit_gb * GIB
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelemetryConfig {
    #[serde(default = "d_otlp_endpoint")]
    pub otel_exporter_otlp_endpoint: String,
    #[serde(default = "d_service_name")]
    pub otel_service_name: String,
    #[serde(default = "d_service_version")]
    pub otel_service_version: String,
    #[serde(default)]
    pub log_format: Option<String>,
    /// Standard OTel var; `none` disables span export (logs/metrics unaffected).
    #[serde(default)]
    pub otel_traces_exporter: Option<String>,
}

impl TelemetryConfig {
    pub fn is_json_logging(&self) -> bool {
        self.log_format.as_deref() == Some("json")
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        envy::from_iter::<_, Self>(std::iter::empty::<(String, String)>()).expect("Default config should always succeed with serde defaults")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for the 2026-06-24 crash loop: TIMEFUSION_S3_CONNECT_TIMEOUT=150
    // (unitless) panicked object_store's Duration parse at boot. Bare numbers must
    // coerce to seconds; values with a unit pass through untouched.
    #[test]
    fn normalize_duration_coerces_bare_numbers_to_seconds() {
        assert_eq!(normalize_duration("150".into()), "150s");
        assert_eq!(normalize_duration("150s".into()), "150s");
        assert_eq!(normalize_duration("3m".into()), "3m");
        assert_eq!(normalize_duration("".into()), "");
        let aws = AwsConfig { timefusion_s3_connect_timeout: Some("150".into()), ..Default::default() };
        assert_eq!(aws.connect_timeout(), "150s");
    }

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.core.pgwire_port, 5432);
        assert_eq!(config.buffer.timefusion_flush_interval_secs, 60);
        assert_eq!(config.buffer.timefusion_bucket_duration_secs, 300);
        assert_eq!(config.cache.timefusion_foyer_memory_mb, 1024);
        assert_eq!(config.cache.timefusion_foyer_disk_gb, 500);
        assert_eq!(config.cache.disk_size_bytes(), 500 * 1024 * 1024 * 1024);
        assert_eq!(config.cache.timefusion_warm_inline_max_mb, 0);
        assert_eq!(config.cache.timefusion_foyer_block_size_mb, 256);
        assert_eq!(config.cache.block_size_bytes(), 256 * 1024 * 1024);
        assert_eq!(config.cache.timefusion_foyer_l1_max_entry_mb, 16);
        assert_eq!(config.cache.timefusion_cache_recent_days, 8);
        assert!(config.maintenance.timefusion_warm_after_compaction);
        assert!(config.maintenance.timefusion_evict_after_compaction);
        assert!(!config.maintenance.timefusion_warm_full_files);
        assert_eq!(config.maintenance.timefusion_warm_recency_days, 2);
        assert_eq!(config.maintenance.timefusion_warm_concurrency, 16);
    }

    #[test]
    fn test_buffer_min_enforcement() {
        let mut config = AppConfig::default();
        config.buffer.timefusion_buffer_max_memory_mb = 10;
        assert_eq!(config.buffer.max_memory_mb(), 64);
    }

    #[test]
    fn test_cache_size_calculations() {
        let mut config = AppConfig::default();
        config.cache.timefusion_foyer_memory_mb = 256;
        config.cache.timefusion_foyer_disk_mb = Some(1024);
        assert_eq!(config.cache.memory_size_bytes(), 256 * 1024 * 1024);
        assert_eq!(config.cache.disk_size_bytes(), 1024 * 1024 * 1024);
    }

    // issue #83: the unflushed-WAL force-flush backstop is opt-in — disabled
    // (None) unless an explicit MB ceiling is set.
    #[test]
    fn test_wal_max_unflushed_bytes_optin() {
        let mut config = AppConfig::default();
        config.buffer.timefusion_wal_max_unflushed_mb = 0;
        assert_eq!(config.buffer.wal_max_unflushed_bytes(), None, "unset → disabled");
        config.buffer.timefusion_wal_max_unflushed_mb = 8000;
        assert_eq!(config.buffer.wal_max_unflushed_bytes(), Some(8000u64 * 1024 * 1024), "explicit ceiling");
    }
}
