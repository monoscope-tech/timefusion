# Configuration reference

Every setting is an environment variable, parsed at startup. Defaults below are
the authoritative values from [`src/config.rs`](src/config.rs); the [README](README.md#configuration)
covers the common subset. Sizes are MB/GB unless noted; schedules are 6-field
cron (`sec min hour dom mon dow`).

## Object storage (S3 / MinIO / R2)

| Variable                        | Default                    | Description |
| ------------------------------- | -------------------------- | ----------- |
| `AWS_S3_BUCKET`                 | _(required)_               | Bucket holding the Delta tables. |
| `AWS_ACCESS_KEY_ID`             | —                          | Access key. |
| `AWS_SECRET_ACCESS_KEY`         | —                          | Secret key. |
| `AWS_DEFAULT_REGION`            | —                          | Region (passed through to the S3 client as `AWS_REGION`). |
| `AWS_S3_ENDPOINT`               | `https://s3.amazonaws.com` | Endpoint override for MinIO/R2/etc. |
| `AWS_ALLOW_HTTP`                | —                          | Set `true` to allow plain HTTP (local MinIO). |
| `TIMEFUSION_S3_CONNECT_TIMEOUT` | `60s`                      | TCP/TLS connect bound (humantime; bare numbers are read as seconds). |
| `TIMEFUSION_S3_REQUEST_TIMEOUT` | `900s`                     | Total per-request bound; must exceed the time to PUT one large multipart part under load. |

## Server

| Variable                          | Default      | Description |
| --------------------------------- | ------------ | ----------- |
| `PGWIRE_PORT`                     | `5432`       | PostgreSQL wire protocol port. |
| `PGWIRE_USER`                     | `postgres`   | pgwire username. |
| `PGWIRE_PASSWORD`                 | —            | pgwire password (required unless insecure auth is allowed). |
| `GRPC_PORT`                       | `50051`      | gRPC ingestion port (Arrow IPC). |
| `GRPC_TOKEN`                      | —            | Bearer token for gRPC; ingest is open if unset. |
| `TIMEFUSION_ALLOW_INSECURE_AUTH`  | `false`      | Opt into open auth for local dev (both pgwire and gRPC). |
| `TIMEFUSION_DATA_DIR`             | `./data`     | Base dir for the WAL (`/wal`) and cache (`/cache`). |
| `TIMEFUSION_TABLE_PREFIX`         | `timefusion` | Path prefix for tables within the bucket. |
| `TIMEFUSION_STOP_GRACE_SECS`      | `70`         | Total graceful-shutdown budget; keep below the 90s CapRover SIGTERM→SIGKILL grace. |
| `RUST_LOG`                        | —            | Standard `tracing` filter (e.g. `info`, `debug`). |
| `LOG_FORMAT`                      | —            | Set `json` for structured logs. |

## Write buffer & WAL

| Variable                              | Default | Description |
| ------------------------------------- | ------- | ----------- |
| `TIMEFUSION_BUFFER_MAX_MEMORY_MB`     | `4096`  | In-memory buffer ceiling (min 64). |
| `TIMEFUSION_FLUSH_INTERVAL_SECS`      | `300`   | Background flush cadence. |
| `TIMEFUSION_BUFFER_RETENTION_MINS`    | `70`    | How long buckets stay buffered before eviction. |
| `TIMEFUSION_EVICTION_INTERVAL_SECS`   | `60`    | Eviction task cadence. |
| `TIMEFUSION_BUCKET_DURATION_SECS`     | `300`   | MemBuffer time-bucket window (floor on per-row RAM dwell). |
| `TIMEFUSION_FLUSH_PARALLELISM`        | `8`     | Concurrent staged flush commits (Parquet encode + S3 upload). |
| `TIMEFUSION_PRESSURE_FLUSH_PCT`       | `75`    | Memory-pressure % that wakes an early flush; `0` disables. |
| `TIMEFUSION_WRITE_BACKPRESSURE_SECS`  | `60`    | Max secs an insert flushes-to-free-RAM before failing; `0` = fail fast. |
| `TIMEFUSION_WAL_FSYNC_MODE`           | `ms`    | `ms` (async every `wal_fsync_ms`), `sync_each`, or `none`. |
| `TIMEFUSION_WAL_FSYNC_MS`             | `200`   | Async fsync interval for `ms` mode. |
| `TIMEFUSION_WAL_MAX_FILE_COUNT`       | `200`   | WAL file retention cap. |
| `TIMEFUSION_WAL_SHARDS_PER_TOPIC`     | `4`     | Append parallelism per `(project, table)` topic. |
| `TIMEFUSION_WAL_CORRUPTION_THRESHOLD` | `10`    | Corrupt-entry tolerance during WAL replay. |
| `TIMEFUSION_DELTA_SCAN_CONCURRENCY`   | `64`    | Concurrent S3 reads when reconciling Delta watermarks at boot. |
| `TIMEFUSION_DELTA_SCAN_DEPTH`         | `8`     | Per-table commit history depth scanned at boot. |
| `TIMEFUSION_FLUSH_IMMEDIATELY`        | `false` | Debug: flush every bucket immediately. |

## Cache (Foyer, memory + disk)

| Variable                              | Default  | Description |
| ------------------------------------- | -------- | ----------- |
| `TIMEFUSION_FOYER_MEMORY_MB`          | `1024`   | L1 memory cache size. |
| `TIMEFUSION_FOYER_DISK_GB`            | `500`    | L2 disk cache size (must stay ≤ free space on the cache volume). |
| `TIMEFUSION_FOYER_TTL_SECONDS`        | `604800` | Entry TTL (7 days). |
| `TIMEFUSION_FOYER_SHARDS`             | `8`      | Cache shard count. |
| `TIMEFUSION_FOYER_FILE_SIZE_MB`       | `32`     | On-disk cache file size. |
| `TIMEFUSION_FOYER_BLOCK_SIZE_MB`      | `256`    | Disk block / max on-disk entry size (auto-raised to 2× compaction target). |
| `TIMEFUSION_FOYER_L1_MAX_ENTRY_MB`    | `16`     | Entries larger than this go disk-only; `0` = always use L1. |
| `TIMEFUSION_CACHE_RECENT_DAYS`        | `8`      | Don't admit writes whose `date=` partition is older than this; `0` = no limit. |
| `TIMEFUSION_DF_METADATA_CACHE_MB`     | `512`    | DataFusion decoded-parquet-metadata cache. |
| `TIMEFUSION_PARQUET_METADATA_SIZE_HINT`| `1048576`| Footer prefetch size hint (bytes). |
| `TIMEFUSION_FOYER_METADATA_MEMORY_MB` | `512`    | Memory cache for the metadata tier. |
| `TIMEFUSION_FOYER_METADATA_DISK_GB`   | `5`      | Disk cache for the metadata tier. |
| `TIMEFUSION_FOYER_METADATA_SHARDS`    | `4`      | Metadata-tier shard count. |
| `TIMEFUSION_FOYER_STATS`              | `true`   | Emit cache statistics. |
| `TIMEFUSION_FOYER_DISABLED`           | `false`  | Disable the cache entirely. |

## Parquet & compression

| Variable                            | Default     | Description |
| ----------------------------------- | ----------- | ----------- |
| `TIMEFUSION_PAGE_ROW_COUNT_LIMIT`   | `20000`     | Rows per Parquet data page. |
| `TIMEFUSION_MAX_ROW_GROUP_SIZE`     | `134217728` | Max row-group size (128 MB). |
| `TIMEFUSION_ZSTD_COMPRESSION_LEVEL` | `3`         | ZSTD level for hot writes (alias `TIMEFUSION_ZSTD_LEVEL_HOT`). |
| `TIMEFUSION_ZSTD_LEVEL_WARM`        | `9`         | ZSTD level for warm-tier rewrites. |
| `TIMEFUSION_ZSTD_LEVEL_COLD`        | `19`        | ZSTD level for cold-tier rewrites. |
| `TIMEFUSION_COLD_CUTOFF_DAYS`       | `14`        | Age past which data is recompressed to the cold level. |
| `TIMEFUSION_CHECKPOINT_INTERVAL`    | `10`        | Commits between Delta checkpoints. |
| `TIMEFUSION_STATS_CACHE_SIZE`       | `50`        | Cached table-statistics entries. |
| `TIMEFUSION_BLOOM_FILTER_DISABLED`  | `false`     | Disable Parquet bloom filters. |

## Compaction & maintenance

| Variable                                | Default         | Description |
| --------------------------------------- | --------------- | ----------- |
| `TIMEFUSION_OPTIMIZE_TARGET_SIZE`       | `268435456`     | Warm compaction target file size (256 MB). |
| `TIMEFUSION_COLD_OPTIMIZE_TARGET_SIZE`  | `1073741824`    | Cold (sealed-partition) target file size (1 GB). |
| `TIMEFUSION_COLD_OPTIMIZE_AFTER_DAYS`   | `1`             | Partitions older than this consolidate to the cold target (min 1). |
| `TIMEFUSION_LIGHT_OPTIMIZE_TARGET_SIZE` | `33554432`      | Hot/today compaction target (32 MB). |
| `TIMEFUSION_COMPACT_MIN_FILES`          | `5`             | Min small files before a compaction runs. |
| `TIMEFUSION_OPTIMIZE_MAX_CONCURRENT_TASKS`| `4`           | Concurrent merge tasks per optimize run. |
| `TIMEFUSION_OPTIMIZE_WINDOW_HOURS`      | `48`            | Lookback window for the warm optimize. |
| `TIMEFUSION_VACUUM_RETENTION_HOURS`     | `24`            | File retention for VACUUM (also drives Delta `deletedFileRetentionDuration`). |
| `TIMEFUSION_LOG_RETENTION_HOURS`        | `6`             | `_delta_log` (transaction-log) retention window. |
| `TIMEFUSION_LIGHT_OPTIMIZE_SCHEDULE`    | `0 */5 * * * *` | Hot/today compaction (every 5 min). |
| `TIMEFUSION_OPTIMIZE_SCHEDULE`          | `0 */30 * * * *`| Full Compact every 30 min. |
| `TIMEFUSION_CONSOLIDATE_SCHEDULE`       | `0 30 2 * * *`  | Daily cold consolidation sweep (02:30). |
| `TIMEFUSION_VACUUM_SCHEDULE`            | `0 15 */6 * * *`| VACUUM sweep (every 6h). |
| `TIMEFUSION_RECOMPRESS_SCHEDULE`        | `0 0 3 * * *`   | Cold recompression (03:00). |
| `TIMEFUSION_WARM_AFTER_COMPACTION`      | `true`          | Warm the cache for files a flush/optimize commit wrote. |
| `TIMEFUSION_WARM_FULL_FILES`            | `false`         | Warm full file contents (not just footers). |
| `TIMEFUSION_WARM_ALL_FOOTERS`           | `true`          | Warm footers for every live file at boot. |
| `TIMEFUSION_WARM_RECENCY_DAYS`          | `2`             | Only warm partitions within this many days; `0` = no limit. |
| `TIMEFUSION_WARM_CONCURRENCY`           | `16`            | Max concurrent warm fetches per commit. |
| `TIMEFUSION_EVICT_AFTER_COMPACTION`     | `true`          | Evict cached bytes of files a compaction tombstoned. |
| `TIMEFUSION_INCREMENTAL_SNAPSHOT`       | `true`          | Advance the post-commit snapshot incrementally instead of re-materializing. |
| `TIMEFUSION_SNAPSHOT_RECONCILE_COMMITS` | `500`           | Every Nth commit, re-materialize from S3 truth; `0` disables. |

## Memory & query

| Variable                          | Default  | Description |
| --------------------------------- | -------- | ----------- |
| `TIMEFUSION_MEMORY_LIMIT_GB`      | `8`      | DataFusion memory-pool ceiling. |
| `TIMEFUSION_MEMORY_FRACTION`      | `0.9`    | Fraction of the limit the pool may use. |
| `TIMEFUSION_MEMORY_POOL`          | `greedy` | `greedy` (write-heavy default) or `fair_spill` (many concurrent queries). |
| `TIMEFUSION_QUERY_PARTITIONS`     | `0`      | DataFusion `target_partitions`; `0` = auto-derive from the CPU quota. |
| `TIMEFUSION_TRACING_RECORD_METRICS`| `true`  | Record per-query metrics in tracing spans. |

## Telemetry

| Variable                       | Default                  | Description |
| ------------------------------ | ------------------------ | ----------- |
| `OTEL_EXPORTER_OTLP_ENDPOINT`  | `http://localhost:4317`  | OTLP collector endpoint. |
| `OTEL_SERVICE_NAME`            | `timefusion`             | Service name in telemetry. |
| `OTEL_SERVICE_VERSION`         | _(crate version)_        | Service version in telemetry. |
| `OTEL_TRACES_EXPORTER`         | —                        | `none` disables span export (logs/metrics unaffected). |

## Full-text index (Tantivy)

Indexing is always on for any table whose YAML schema declares
`tantivy.indexed: true` on a field — there is no enable knob, the schema is the
source of truth. These tune the index itself:

| Variable                                          | Default  | Description |
| ------------------------------------------------- | -------- | ----------- |
| `TIMEFUSION_TANTIVY_MAX_INDEX_SIZE_MB`            | `64`     | Max in-memory index segment size. |
| `TIMEFUSION_TANTIVY_CACHE_DISK_GB`               | `4`      | Disk cache for index segments. |
| `TIMEFUSION_TANTIVY_COMPRESSION_LEVEL`           | `19`     | ZSTD level for index storage. |
| `TIMEFUSION_TANTIVY_MIN_FILES_FOR_PUSHDOWN`      | `2`      | Min files before the index prefilter is used. |
| `TIMEFUSION_TANTIVY_PREFILTER_MAX_HITS`          | `100000` | Skip the `id IN (...)` pushdown above this many hits. |
| `TIMEFUSION_TANTIVY_PREFILTER_MIN_SELECTIVITY_PCT`| `50`    | Skip the pushdown if it selects more than this % of indexed rows. |
