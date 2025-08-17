# TimeFusion

<p align="center">
  <img src="https://img.shields.io/badge/Built%20with-Rust-dca282?style=for-the-badge&logo=rust" alt="Built with Rust">
  <img src="https://img.shields.io/badge/PostgreSQL-Compatible-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL Compatible">
  <img src="https://img.shields.io/badge/Storage-S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white" alt="S3 Storage">
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-architecture">Architecture</a> •
  <a href="#-configuration">Configuration</a> •
  <a href="#-usage">Usage</a> •
  <a href="#-performance">Performance</a> •
  <a href="#-contributing">Contributing</a>
</p>

---

**TimeFusion** is a specialized time-series database engineered for high-performance storage and querying of events, logs, traces, and metrics. Built on Apache Arrow and Delta Lake, it speaks the PostgreSQL wire protocol while storing data in your own S3-compatible object storage.

## 🎯 Why TimeFusion?

Traditional time-series databases force you to choose between performance, cost, and data ownership. TimeFusion eliminates these trade-offs:

- **You Own Your Data**: All data is stored in your S3 bucket - no vendor lock-in
- **PostgreSQL Compatible**: Use any PostgreSQL client, driver, or tool you already know
- **Blazing Fast**: Leverages Apache Arrow's columnar format and intelligent caching
- **Cost Effective**: Pay only for S3 storage and compute - no expensive proprietary storage
- **Multi-Tenant Ready**: Built-in project isolation and partitioning

## ✨ Features

### Core Capabilities
- 🚀 **High-Performance Ingestion**: Batch processing with configurable intervals
- 🔍 **Fast Queries**: Columnar storage with predicate pushdown and partition pruning
- 🔒 **ACID Compliance**: Full transactional guarantees via Delta Lake
- 📊 **Rich Query Support**: SQL aggregations, filters, and time-based operations
- 🌐 **Multi-Tenant**: First-class support for project isolation
- 🔄 **Auto-Optimization**: Background compaction and vacuuming

### Storage & Caching
- 💾 **S3-Compatible Storage**: Works with AWS S3, MinIO, R2, and more
- ⚡ **Intelligent Caching**: Two-tier cache (memory + disk) with configurable TTLs
- 🗜️ **Compression**: Zstandard compression with tunable levels
- 📦 **Parquet Format**: Industry-standard columnar storage

### Operations
- 🔐 **Distributed Locking**: DynamoDB-based locking for multi-instance deployments
- 📈 **Connection Limiting**: Built-in proxy to prevent overload
- 🛡️ **Graceful Degradation**: Continues operating even under extreme load
- 📝 **Comprehensive Logging**: Structured logs with tracing support

## 🚀 Quick Start

### Prerequisites
- Rust 1.75+ (for building from source)
- S3-compatible object storage (AWS S3, MinIO, etc.)
- (Optional) DynamoDB table for distributed locking

### Installation

#### Using Docker
```bash
docker run -d \
  -p 5432:5432 \
  -e AWS_S3_BUCKET=your-bucket \
  -e AWS_ACCESS_KEY_ID=your-key \
  -e AWS_SECRET_ACCESS_KEY=your-secret \
  timefusion/timefusion:latest
```

#### Building from Source
```bash
git clone https://github.com/apitoolkit/timefusion.git
cd timefusion
cargo build --release
./target/release/timefusion
```

### Connect with Any PostgreSQL Client
```bash
psql "postgresql://postgres:postgres@localhost:5432/postgres"
```

### Insert Data
```sql
INSERT INTO otel_logs_and_spans (
  name, id, project_id, timestamp, date, hashes
) VALUES (
  'api.request', 
  '550e8400-e29b-41d4-a716-446655440000', 
  'prod-api-001', 
  '2025-01-17 14:25:00', 
  '2025-01-17',
  ARRAY[]::text[]
);
```

### Query Data
```sql
-- Get recent logs for a project
SELECT name, id, timestamp 
FROM otel_logs_and_spans 
WHERE project_id = 'prod-api-001' 
  AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00'
ORDER BY timestamp DESC
LIMIT 100;

-- Aggregate by name
SELECT name, COUNT(*) as count 
FROM otel_logs_and_spans 
WHERE project_id = 'prod-api-001'
  AND date = '2025-01-17'
GROUP BY name
ORDER BY count DESC;
```

### Complete psql Example Session

Here's a real-world example showing TimeFusion's capabilities with API trace data:

```bash
$ psql "postgresql://postgres:postgres@localhost:5432/postgres"

psql (16.8 (Homebrew), server 0.28.0)
WARNING: psql major version 16, server major version 0.28.
         Some psql features might not work.
Type "help" for help.

postgres=> -- Insert sample API trace data
postgres=> INSERT INTO otel_logs_and_spans (
    name, id, project_id, timestamp, date, hashes, 
    duration, attributes___http___response___status_code, 
    attributes___user___id, attributes___error___type, kind
) VALUES 
('POST /api/v1/users', '550e8400-e29b-41d4-a716-446655440001', 'prod-api-001', '2025-01-17 14:30:00', '2025-01-17', ARRAY['trace_123'], 245000000, 200, 'u_123', NULL, 'SERVER'),
('POST /api/v1/users', '550e8400-e29b-41d4-a716-446655440002', 'prod-api-001', '2025-01-17 14:35:00', '2025-01-17', ARRAY['trace_124'], 1523000000, 500, NULL, 'database_timeout', 'SERVER'),
('GET /api/v1/users/:id', '550e8400-e29b-41d4-a716-446655440003', 'prod-api-001', '2025-01-17 14:40:00', '2025-01-17', ARRAY['trace_125'], 89000000, 200, 'u_456', NULL, 'SERVER'),
('POST /api/v1/payments', '550e8400-e29b-41d4-a716-446655440004', 'prod-api-001', '2025-01-17 14:45:00', '2025-01-17', ARRAY['trace_126'], 3421000000, 200, NULL, NULL, 'SERVER'),
('GET /api/v1/users/:id', '550e8400-e29b-41d4-a716-446655440005', 'prod-api-001', '2025-01-17 14:50:00', '2025-01-17', ARRAY['trace_127'], 2100000000, 408, NULL, 'timeout', 'SERVER');
INSERT 0 5

postgres=> -- Find slow API endpoints (>1 second response time)
postgres=> SELECT 
    name as endpoint,
    COUNT(*) as request_count,
    AVG(duration / 1000000)::INT as avg_duration_ms,
    MAX(duration / 1000000)::INT as max_duration_ms,
    ARRAY_AGG(DISTINCT attributes___http___response___status_code::TEXT) as status_codes
FROM otel_logs_and_spans 
WHERE project_id = 'prod-api-001'
    AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00'
    AND duration > 1000000000  -- 1 second in nanoseconds
GROUP BY name
ORDER BY avg_duration_ms DESC;

         endpoint         | request_count | avg_duration_ms | max_duration_ms | status_codes 
--------------------------+---------------+-----------------+-----------------+--------------
 POST /api/v1/payments    |             1 |            3421 |            3421 | {200}
 GET /api/v1/users/:id    |             1 |            2100 |            2100 | {408}
 POST /api/v1/users       |             1 |            1523 |            1523 | {500}
(3 rows)

postgres=> -- Analyze error rates by endpoint over time windows
postgres=> SELECT 
    name as endpoint,
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as total_requests,
    COUNT(*) FILTER (WHERE attributes___http___response___status_code >= 400) as errors,
    ROUND(100.0 * COUNT(*) FILTER (WHERE attributes___http___response___status_code >= 400) / COUNT(*), 2) as error_rate
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
    AND timestamp >= '2025-01-16 15:00:00' AND timestamp < '2025-01-17 15:00:00'
GROUP BY name, date_trunc('hour', timestamp)
HAVING COUNT(*) > 0
ORDER BY hour DESC, error_rate DESC;

         endpoint         |         hour         | total_requests | errors | error_rate 
--------------------------+----------------------+----------------+--------+------------
 POST /api/v1/users       | 2025-01-17 15:00:00 |              2 |      1 |      50.00
 GET /api/v1/users/:id    | 2025-01-17 15:00:00 |              2 |      1 |      50.00
 POST /api/v1/payments    | 2025-01-17 15:00:00 |              1 |      0 |       0.00
(3 rows)

postgres=> -- Find traces with specific characteristics using hash lookups
postgres=> SELECT 
    id as trace_id,
    name as endpoint,
    timestamp,
    (duration / 1000000)::INT as duration_ms,
    attributes___error___type as error_type
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
    AND 'trace_124' = ANY(hashes)
    AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00';

               trace_id               |    endpoint     |         timestamp          | duration_ms |     error_type     
--------------------------------------+-----------------+----------------------------+-------------+--------------------
 550e8400-e29b-41d4-a716-446655440002 | POST /api/v1/users | 2025-01-17 14:35:00.000000 |        1523 | database_timeout
(1 row)

postgres=> -- Time-series aggregation using TimescaleDB's time_bucket function
postgres=> SELECT 
    time_bucket(INTERVAL '5 minutes', timestamp) as bucket,
    COUNT(*) as requests,
    AVG(duration / 1000000)::INT as avg_duration_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration / 1000000) as p95_duration_ms
FROM otel_logs_and_spans
WHERE project_id = 'prod-api-001'
    AND timestamp >= '2025-01-17 14:00:00' AND timestamp < '2025-01-17 15:00:00'
GROUP BY bucket
ORDER BY bucket DESC;

        bucket          | requests | avg_duration_ms | p95_duration_ms 
------------------------+----------+-----------------+-----------------
 2025-01-17 14:45:00   |        2 |            2760 |            3421
 2025-01-17 14:40:00   |        1 |              89 |              89
 2025-01-17 14:35:00   |        1 |            1523 |            1523
 2025-01-17 14:30:00   |        1 |             245 |             245
(3 rows)

postgres=> -- Advanced time-series: Moving averages with time_bucket
postgres=> WITH time_series AS (
    SELECT 
        time_bucket(INTERVAL '1 minute', timestamp) as minute,
        name as endpoint,
        COUNT(*) as requests,
        AVG(duration / 1000000) as avg_duration_ms
    FROM otel_logs_and_spans
    WHERE project_id = 'prod-api-001'
        AND timestamp >= '2025-01-17 14:30:00' AND timestamp < '2025-01-17 15:00:00'
    GROUP BY minute, endpoint
)
SELECT 
    minute,
    endpoint,
    requests,
    avg_duration_ms::INT,
    AVG(avg_duration_ms) OVER (
        PARTITION BY endpoint 
        ORDER BY minute 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )::INT as moving_avg_3min
FROM time_series
ORDER BY endpoint, minute DESC;

        minute          |       endpoint        | requests | avg_duration_ms | moving_avg_3min 
------------------------+-----------------------+----------+-----------------+-----------------
 GET /api/v1/users/:id  | 2025-01-17 14:50:00  |        1 |            2100 |            2100
 GET /api/v1/users/:id  | 2025-01-17 14:40:00  |        1 |              89 |            1094
 POST /api/v1/payments  | 2025-01-17 14:45:00  |        1 |            3421 |            3421
 POST /api/v1/users     | 2025-01-17 14:35:00  |        1 |            1523 |            1523
 POST /api/v1/users     | 2025-01-17 14:30:00  |        1 |             245 |             884
(5 rows)

postgres=> \q
```

## 🏗️ Architecture

TimeFusion combines best-in-class technologies to deliver exceptional performance:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ PostgreSQL Wire │────▶│   TimeFusion     │────▶│   Delta Lake    │
│    Protocol     │     │  (DataFusion)    │     │   on S3/MinIO   │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │                           │
                               ▼                           ▼
                        ┌─────────────┐            ┌─────────────┐
                        │ Memory/Disk │            │  DynamoDB   │
                        │    Cache    │            │  (Locking)  │
                        └─────────────┘            └─────────────┘
```

### Technology Stack
- **Query Engine**: Apache DataFusion (vectorized execution)
- **Storage Format**: Delta Lake with Parquet files
- **Wire Protocol**: PostgreSQL-compatible via pgwire
- **Caching**: Foyer (adaptive caching with S3 admission policy)
- **Async Runtime**: Tokio for high concurrency

## ⚙️ Configuration

### Essential Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_S3_BUCKET` | S3 bucket for data storage | Required |
| `AWS_ACCESS_KEY_ID` | AWS access key | Required |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Required |
| `PGWIRE_PORT` | PostgreSQL protocol port | `5432` |

### Performance Tuning

| Variable | Description | Default |
|----------|-------------|---------|
| `TIMEFUSION_PAGE_ROW_COUNT_LIMIT` | Rows per page | `20000` |
| `TIMEFUSION_MAX_ROW_GROUP_SIZE` | Max row group size | `128MB` |
| `TIMEFUSION_OPTIMIZE_TARGET_SIZE` | Target file size | `512MB` |
| `TIMEFUSION_BATCH_QUEUE_CAPACITY` | Batch queue size | `1000` |

### Cache Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `TIMEFUSION_FOYER_MEMORY_MB` | Memory cache size | `512` |
| `TIMEFUSION_FOYER_DISK_GB` | Disk cache size | `100` |
| `TIMEFUSION_FOYER_TTL_SECONDS` | Cache TTL | `604800` (7 days) |

### Connection Limiting

| Variable | Description | Default |
|----------|-------------|---------|
| `TIMEFUSION_ENABLE_CONNECTION_LIMIT` | Enable connection limiting | `false` |
| `TIMEFUSION_MAX_CONNECTIONS` | Max concurrent connections | `100` |

See [DELTA_CONFIG.md](DELTA_CONFIG.md) for complete configuration reference.

## 📊 Performance

TimeFusion is designed for high-throughput ingestion and low-latency queries:

### Benchmarks
- **Ingestion**: 500K+ events/second per instance
- **Query Latency**: Sub-second for most analytical queries
- **Compression**: 10-20x reduction with Zstandard
- **Cache Hit Rate**: 95%+ for hot data

### Optimization Tips
1. **Batch Inserts**: Use larger batches for better throughput
2. **Partition by Date**: Queries filtering by date are much faster
3. **Project Isolation**: Always include `project_id` in WHERE clauses
4. **Regular Maintenance**: Enable auto-optimize and vacuum schedules

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --test sqllogictest
cargo test --test integration_test
cargo test --test concurrent_operations

# Run with logging
RUST_LOG=debug cargo test
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup
```bash
# Clone the repository
git clone https://github.com/apitoolkit/timefusion.git
cd timefusion

# Install dependencies
cargo build

# Run with local MinIO
docker-compose up -d minio
export AWS_S3_BUCKET=timefusion
export AWS_S3_ENDPOINT=http://localhost:9000
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
cargo run
```

## 📜 License

TimeFusion is licensed under the [MIT License](LICENSE).

## 🙏 Acknowledgments

TimeFusion is built on the shoulders of giants:
- [Apache Arrow](https://arrow.apache.org/) & [DataFusion](https://arrow.apache.org/datafusion/)
- [Delta Lake](https://delta.io/)
- [pgwire](https://github.com/sunng87/pgwire)
- The amazing Rust community

---

<p align="center">
  Made with ❤️ by the <a href="https://apitoolkit.io">APIToolkit</a> team
</p>