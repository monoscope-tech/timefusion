# TimeFusion

[![CI](https://github.com/monoscope-tech/timefusion/actions/workflows/ci.yml/badge.svg)](https://github.com/monoscope-tech/timefusion/actions/workflows/ci.yml)
[![Rust edition 2024](https://img.shields.io/badge/rust-edition%202024-orange.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

TimeFusion stores observability data — logs, traces, metrics, and events — as
[Delta Lake](https://delta.io/) tables on your own S3-compatible object storage,
and lets you query it over the **PostgreSQL wire protocol**. It uses
[Apache DataFusion](https://datafusion.apache.org/) for query execution and a
write-ahead log + in-memory buffer for sub-second write durability.

If you already have a Postgres client, you already have a TimeFusion client.

> **Status:** Pre-1.0. TimeFusion runs in production at
> [APIToolkit](https://apitoolkit.io), but the schema, wire behavior, and
> configuration surface can still change between releases — pin a version
> before depending on it. (The version a client sees via `SHOW server_version`
> reflects the embedded PostgreSQL-protocol layer, not TimeFusion's own.)

## Contents

- [Why TimeFusion](#why-timefusion)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Querying](#querying)
- [Configuration](#configuration)
- [Performance](#performance)
- [Development](#development)
- [License](#license)

## Why TimeFusion

- **Your data stays in your bucket.** Storage is plain Parquet under a Delta
  Lake transaction log in your own S3 / MinIO / R2 bucket. No proprietary
  format, no vendor storage to pay for, nothing to migrate out of.
- **Postgres wire protocol.** Connect with `psql`, any Postgres driver, or
  existing BI/observability tooling. No new client to learn.
- **Columnar analytics.** DataFusion executes vectorized SQL with predicate
  pushdown and partition pruning over Parquet.
- **Durable, low-latency writes.** Inserts hit a write-ahead log and an
  in-memory buffer before being flushed to Delta in the background, so writes
  are acknowledged quickly without losing durability.
- **Multi-tenant by design.** Data is partitioned and isolated by
  `project_id`, with shared tables for default projects and dedicated tables
  for projects that need isolation.

## How it works

```
 Postgres client ──▶ pgwire ──▶ DataFusion query engine
 (psql, drivers)                      │
                                      ▼
              ┌───────────── write path ─────────────┐
              │  WAL (durable) ─▶ in-memory buffer    │
              │            │ flush every ~10 min       │
              │            ▼                            │
              │      Delta Lake (Parquet) on S3        │
              └────────────────────────────────────────┘
                                      ▲
                          Foyer cache (memory + disk)
```

- **Write:** `INSERT` → WAL append (durable) → in-memory buffer (fast ack).
  Completed time buckets flush to Delta in the background.
- **Read:** queries union the in-memory buffer with Delta on S3, so recently
  written rows are visible immediately.
- **Ingest:** besides pgwire `INSERT`, TimeFusion accepts gRPC streaming
  ingestion of Arrow IPC payloads (`GRPC_PORT`, default `50051`).

**Storage layout:** events are written to a Delta table such as
`otel_logs_and_spans`, partitioned by `[project_id, date]`. Table schemas live
in [`schemas/`](schemas/) and are loaded at startup.

> Queries **must** filter on `project_id` — it is the tenancy and partition
> key. Queries without it scan across tenants and are far slower.

## Quick start

### Try it locally (zero config)

The bundled Compose stack runs TimeFusion against a local MinIO — no AWS account
or credentials needed. It builds the image, starts MinIO, creates the bucket,
and launches TimeFusion on port 5432:

```bash
git clone https://github.com/monoscope-tech/timefusion.git
cd timefusion
docker compose up
```

Then connect from another terminal — skip to [Connect](#connect).

### Run against your own S3

Images are published per commit to GitHub Container Registry, tagged with the
git short SHA (see the
[packages page](https://github.com/monoscope-tech/timefusion/pkgs/container/timefusion)
for available tags):

```bash
docker run -d -p 5432:5432 \
  -e AWS_S3_BUCKET=your-bucket \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  ghcr.io/monoscope-tech/timefusion:<tag>
```

### Build from source

Requires a recent stable Rust toolchain (edition 2024 → **Rust 1.85+**).

```bash
git clone https://github.com/monoscope-tech/timefusion.git
cd timefusion
cargo build --release
AWS_S3_BUCKET=your-bucket ./target/release/timefusion
```

### Connect

```bash
psql "postgresql://postgres:postgres@localhost:5432/postgres"
```

### Insert and query

```sql
INSERT INTO otel_logs_and_spans (name, id, project_id, timestamp, date, hashes)
VALUES ('api.request', '550e8400-e29b-41d4-a716-446655440000',
        'prod-api-001', '2025-01-17 14:25:00', '2025-01-17', ARRAY[]::text[]);

SELECT name, COUNT(*) AS count
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001' AND date = '2025-01-17'
GROUP BY name
ORDER BY count DESC;
```

## Querying

TimeFusion speaks analytical SQL: aggregations, window functions,
`PERCENTILE_CONT`, `FILTER`, CTEs, and a TimescaleDB-compatible `time_bucket`.
Here's finding slow API endpoints from trace data:

```sql
SELECT name AS endpoint,
       COUNT(*)                       AS request_count,
       AVG(duration / 1000000)::INT   AS avg_duration_ms,
       MAX(duration / 1000000)::INT   AS max_duration_ms
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
  AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00'
  AND duration > 1000000000          -- 1 second, in nanoseconds
GROUP BY name
ORDER BY avg_duration_ms DESC;
```
```
         endpoint        | request_count | avg_duration_ms | max_duration_ms
-------------------------+---------------+-----------------+-----------------
 POST /api/v1/payments   |             1 |            3421 |            3421
 GET /api/v1/users/:id   |             1 |            2100 |            2100
 POST /api/v1/users      |             1 |            1523 |            1523
```

<details>
<summary><b>More query examples</b> — error rates, trace lookup, time-bucketed aggregation, moving averages</summary>

```sql
-- Error rate per endpoint, per hour
SELECT name AS endpoint,
       date_trunc('hour', timestamp) AS hour,
       COUNT(*) AS total_requests,
       COUNT(*) FILTER (WHERE attributes___http___response___status_code >= 400) AS errors,
       ROUND(100.0 * COUNT(*) FILTER (WHERE attributes___http___response___status_code >= 400)
             / COUNT(*), 2) AS error_rate
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
  AND timestamp >= '2025-01-16 15:00:00' AND timestamp < '2025-01-17 15:00:00'
GROUP BY name, date_trunc('hour', timestamp)
ORDER BY hour DESC, error_rate DESC;

-- Find a trace by hash
SELECT id AS trace_id, name AS endpoint, timestamp,
       (duration / 1000000)::INT AS duration_ms,
       attributes___error___type AS error_type
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
  AND 'trace_124' = ANY(hashes)
  AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00';

-- Requests and p95 latency, bucketed into 5-minute windows
SELECT time_bucket(INTERVAL '5 minutes', timestamp) AS bucket,
       COUNT(*) AS requests,
       AVG(duration / 1000000)::INT AS avg_duration_ms,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration / 1000000) AS p95_duration_ms
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
  AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00'
GROUP BY bucket
ORDER BY bucket DESC;

-- Per-endpoint 3-window moving average over 1-minute buckets
WITH ts AS (
  SELECT time_bucket(INTERVAL '1 minute', timestamp) AS minute,
         name AS endpoint,
         AVG(duration / 1000000) AS avg_duration_ms
  FROM otel_logs_and_spans
  WHERE project_id = 'prod-api-001'
    AND timestamp >= '2025-01-17 14:30:00' AND timestamp < '2025-01-17 15:00:00'
  GROUP BY minute, endpoint
)
SELECT minute, endpoint, avg_duration_ms::INT,
       AVG(avg_duration_ms) OVER (
         PARTITION BY endpoint ORDER BY minute
         ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       )::INT AS moving_avg
FROM ts
ORDER BY endpoint, minute DESC;
```

</details>

## Configuration

TimeFusion is configured entirely through environment variables.

**Required — object storage**

| Variable                | Description                       |
| ----------------------- | --------------------------------- |
| `AWS_S3_BUCKET`         | Bucket for Delta tables           |
| `AWS_ACCESS_KEY_ID`     | Access key                        |
| `AWS_SECRET_ACCESS_KEY` | Secret key                        |
| `AWS_S3_ENDPOINT`       | Custom endpoint (MinIO, R2, etc.) |

**Server**

| Variable      | Description                          | Default |
| ------------- | ------------------------------------ | ------- |
| `PGWIRE_PORT` | PostgreSQL wire protocol port        | `5432`  |
| `GRPC_PORT`   | gRPC ingestion port                  | `50051` |
| `GRPC_TOKEN`  | Bearer token for gRPC (open if unset)| —       |

**Write buffer & flushing**

| Variable                            | Description                      | Default |
| ----------------------------------- | -------------------------------- | ------- |
| `TIMEFUSION_BUFFER_MAX_MEMORY_MB`   | In-memory buffer size            | `4096`  |
| `TIMEFUSION_FLUSH_INTERVAL_SECS`    | Background flush interval (secs) | `300`   |
| `TIMEFUSION_BUFFER_RETENTION_MINS`  | Buffer retention before eviction | `70`    |

**Cache (Foyer, memory + disk)**

| Variable                       | Description       | Default           |
| ------------------------------ | ----------------- | ----------------- |
| `TIMEFUSION_FOYER_MEMORY_MB`   | Memory cache size | `1024`            |
| `TIMEFUSION_FOYER_DISK_GB`     | Disk cache size   | `500`             |
| `TIMEFUSION_FOYER_TTL_SECONDS` | Cache entry TTL   | `604800` (7 days) |

See [DELTA_CONFIG.md](DELTA_CONFIG.md) for the full configuration reference,
including Parquet sizing, compaction targets, and connection limiting.

## Performance

TimeFusion is built for high-throughput ingestion and low-latency analytical
queries: columnar Parquet, predicate pushdown, partition pruning by
`project_id`/`date`, a two-tier (memory + disk) cache, and background
compaction of small files.

We don't publish a single headline throughput number — real numbers depend
heavily on hardware, payload shape, batch size, and S3 round-trip latency, and
a number measured on one setup misleads on another. Instead, the
[`bench/`](bench/) directory contains the harnesses we use to measure it
ourselves, so you can reproduce results on your own infrastructure:

| Script                        | Measures                              |
| ----------------------------- | ------------------------------------- |
| `bench/run_insert_bench.sh`   | Insert throughput and tail latency    |
| `bench/run_select_bench.sh`   | Query latency                         |
| `bench/query_under_ingest.py` | Query latency during concurrent writes|
| `bench/timeseries_lifecycle.py`| End-to-end ingest → flush → query    |

**Getting good performance:**

1. **Always filter on `project_id`** — it's the tenant routing key. A
   `timestamp` range filter is automatically turned into a `date` partition
   filter, so you don't need to add an explicit `date` predicate to get
   partition pruning.
2. **Batch your inserts** — larger batches amortize WAL and Delta commit cost.
3. **Size the buffer and cache** (`TIMEFUSION_BUFFER_MAX_MEMORY_MB`,
   `TIMEFUSION_FOYER_*`) to your working set.

## Development

```bash
git clone https://github.com/monoscope-tech/timefusion.git
cd timefusion

# Start MinIO only, then run TimeFusion from source against it
docker compose up -d minio createbucket
export AWS_S3_BUCKET=timefusion \
       AWS_S3_ENDPOINT=http://localhost:9000 \
       AWS_ALLOW_HTTP=true \
       AWS_ACCESS_KEY_ID=minioadmin \
       AWS_SECRET_ACCESS_KEY=minioadmin
cargo run
```

**Tests:**

```bash
cargo test                          # unit tests (fast)
cargo test --test sqllogictest      # SQL logic tests
cargo test --test integration_test  # write-path integration
cargo test --test e2e               # end-to-end (Docker required)
RUST_LOG=debug cargo test           # with debug logging
```

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT — see [LICENSE](LICENSE).

## Built with

[Apache DataFusion](https://datafusion.apache.org/) ·
[Delta Lake](https://delta.io/) ·
[Apache Arrow](https://arrow.apache.org/) ·
[pgwire](https://github.com/sunng87/pgwire) ·
[Foyer](https://github.com/foyer-rs/foyer)
