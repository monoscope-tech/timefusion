# TimeFusion Architecture Review: Path to Trillions of Events/Day

## Executive Summary

This document reviews TimeFusion's architecture against ClickHouse and InfluxDB 3.0 (IOx), evaluating readiness to serve as the storage/query backend for a Datadog alternative handling trillions of events per day with ClickHouse-class query speeds.

**Bottom line:** TimeFusion shares its foundational stack (Arrow, DataFusion, Parquet, object storage) with InfluxDB 3.0 — the right foundation for a cloud-native time-series system. However, reaching ClickHouse-level query performance and trillion-event-per-day scale requires significant architectural investment in six areas: vectorized execution, distributed query coordination, ingestion parallelism, storage-level indexing, pre-aggregation, and tiered caching.

---

## 1. Architecture Comparison Matrix

| Dimension | TimeFusion | ClickHouse | InfluxDB 3.0 (IOx) |
|-----------|-----------|------------|---------------------|
| **Language** | Rust | C++ | Rust |
| **Storage format** | Parquet on S3 (Delta Lake) | MergeTree on local SSD (+ S3 tiered) | Parquet on S3 |
| **In-memory format** | Apache Arrow | Custom columnar | Apache Arrow |
| **Query engine** | DataFusion 52.1 | Custom vectorized + SIMD + JIT | DataFusion |
| **Wire protocol** | PostgreSQL (pgwire) | Native TCP + HTTP | Flight SQL / gRPC |
| **Compression** | ZSTD (Parquet) | LZ4/ZSTD + per-column codecs (Delta, DoubleDelta, Gorilla) | ZSTD/Snappy (Parquet) |
| **Write model** | WAL → MemBuffer → Delta Lake flush | Direct part creation (LSM-like) | WAL → Buffer → Parquet flush |
| **Indexing** | Parquet statistics + bloom filters | Sparse primary index + data-skipping indexes + marks | Parquet statistics + partition pruning |
| **Distributed** | Multi-writer via DynamoDB locks | Sharded + replicated (ZooKeeper/Keeper) | Disaggregated (Router/Ingester/Querier/Compactor) |
| **Pre-aggregation** | None | Materialized views + AggregatingMergeTree | None (continuous queries external) |
| **Batch size** | 8,192 rows | 65,505 rows | Varies |

---

## 2. What TimeFusion Does Well

### 2.1 Correct Foundational Stack

TimeFusion is built on the same FDAP stack as InfluxDB 3.0 (Flight/DataFusion/Arrow/Parquet). This is the right foundation:

- **Arrow** provides zero-copy columnar memory layout that enables vectorized processing
- **DataFusion** delivers a production-grade SQL query engine with optimizer passes, predicate pushdown, and parallel execution
- **Parquet** on S3 gives virtually unlimited, cheap storage with columnar compression
- **Delta Lake** adds ACID transactions, schema evolution, and time travel that raw Parquet doesn't provide

### 2.2 Smart Write Path

The BufferedWriteLayer design is sound:

- **WAL-first** for durability (200ms fsync)
- **Time-bucketed MemBuffer** (10-minute buckets using DashMap) for concurrent writes
- **Background flush** to Delta Lake with bounded parallelism
- **Memory backpressure** via atomic CAS reservation with hard limits

This mirrors what both ClickHouse (async inserts) and InfluxDB (ingester buffer → WAL → Parquet) do.

### 2.3 Cost-Effective Storage

S3-backed storage via Delta Lake is significantly cheaper than ClickHouse's SSD-attached model at scale. For a Datadog alternative where customers generate massive volumes of observability data, storage cost is a major competitive factor. ClickHouse Cloud's SharedMergeTree has moved toward this model for the same reason.

### 2.4 Multi-Tenant Isolation

Project-level isolation with per-project Delta tables, separate S3 credentials, and ProjectRoutingTable enforcement is a strong foundation for a multi-tenant SaaS product.

### 2.5 PostgreSQL Compatibility

The pgwire protocol gives immediate compatibility with existing tooling (psql, Grafana PostgreSQL datasource, any PostgreSQL driver). This is a practical advantage over ClickHouse's custom protocol and InfluxDB's Flight SQL.

---

## 3. Critical Gaps vs. ClickHouse

### 3.1 Query Execution Performance

**The gap:** ClickHouse processes 100-200 million rows/second per core for analytical scans. DataFusion, while good, is not in the same performance class for several reasons:

| Factor | ClickHouse | TimeFusion (DataFusion) |
|--------|-----------|------------------------|
| SIMD utilization | Hand-tuned AVX2/AVX-512 intrinsics for hashing, filtering, string ops | Relies on Rust compiler auto-vectorization |
| Block size | 65,505 rows (cache-line optimized) | 8,192 rows |
| Hash tables | Specialized per-key-type hash tables (different impls for UInt8, UInt16, String, etc.) | Generic hash tables |
| JIT compilation | LLVM JIT for expression evaluation | None |
| String processing | Custom SIMD string search, comparison | Standard Rust string ops |
| Memory allocation | Custom arena allocators, pooled memory | Standard Rust allocator |

**Recommendations:**

1. **Increase batch size to 32,768-65,536 rows.** The current 8,192 is conservative. Larger batches amortize per-batch overhead and improve CPU cache utilization. This is a config change:
   ```
   datafusion.execution.batch_size = 65536
   ```

2. **Investigate DataFusion's evolving SIMD support.** The DataFusion community is actively adding vectorized kernels. Track and adopt these as they land.

3. **Profile hot query paths.** Use `perf` or `samply` to identify where time is spent in analytical queries. The likely bottlenecks are hash aggregation, string comparison, and decompression.

4. **Consider Arrow's compute kernels.** Arrow's Rust compute kernels use SIMD where possible. Ensure TimeFusion is using the latest Arrow version and that SIMD feature flags are enabled at compile time.

### 3.2 Indexing and Data Skipping

**The gap:** ClickHouse's query pruning cascade is multi-layered and extremely effective:

```
ClickHouse pruning cascade:
  1. Partition pruning (min/max of partition key)
  2. Part-level pruning (min/max of primary key per part)
  3. Granule-level pruning via sparse primary index (1 entry per 8,192 rows, loaded in RAM)
  4. Data-skipping indexes (bloom, minmax, set, ngram per granule)

TimeFusion pruning:
  1. Date partition pruning
  2. Parquet row group statistics (min/max)
  3. Parquet page-level statistics
  4. Bloom filters (Parquet-native)
```

The critical missing piece is **a sparse primary index loaded in memory**. ClickHouse's primary.idx for 1 billion rows is ~122,000 entries — trivially small in RAM but enables binary search to find the exact granules needed. TimeFusion relies on Parquet metadata, which requires reading file footers from S3 (even if cached).

**Recommendations:**

1. **Build an in-memory sparse index on (timestamp, service_name) per partition.** Store one entry per Parquet row group with min/max timestamp and service name. This avoids reading Parquet footers for the common case of time-range + service queries.

2. **Implement a marks-style index for hot partitions.** For the current day's data (where most queries land), maintain a granule-level index in memory that maps time ranges to specific Parquet file offsets.

3. **Expand Parquet statistics caching.** Currently `DeltaStatisticsExtractor` only caches row count and byte size. Cache per-column min/max from Parquet metadata to enable file-level pruning without reading footers.

4. **Add data-skipping indexes for high-cardinality columns.** Bloom filters on `id`, `trace_id`, `span_id` columns would dramatically speed up point lookups. Consider building these as sidecar files alongside Parquet data.

### 3.3 Compression Specialization

**The gap:** ClickHouse uses codec stacking — a domain-specific encoding (Delta, DoubleDelta, Gorilla) applied before general compression (LZ4/ZSTD). This is critical for time-series data:

| Column type | ClickHouse codec stack | TimeFusion | Improvement potential |
|-------------|----------------------|------------|----------------------|
| Timestamps | DoubleDelta + LZ4 | ZSTD only | 2-5x better compression |
| Float metrics | Gorilla + LZ4 | ZSTD only | 3-10x better compression |
| Integer counters | Delta + LZ4 | ZSTD only | 2-5x better compression |
| String tags | Dictionary + LZ4 | Dictionary + ZSTD | Similar (ZSTD may be slightly better) |

**Recommendations:**

1. **Use Parquet's native encoding options.** Parquet supports DELTA_BINARY_PACKED for integers and timestamps, BYTE_STREAM_SPLIT for floats. These are equivalent to ClickHouse's Delta and Gorilla codecs. Configure the Parquet writer to use these per-column:
   - Timestamp columns: `DELTA_BINARY_PACKED` encoding + ZSTD compression
   - Float columns: `BYTE_STREAM_SPLIT` encoding + ZSTD compression
   - Integer columns: `DELTA_BINARY_PACKED` encoding + ZSTD compression
   - String columns: Dictionary encoding (already enabled) + ZSTD

2. **Tune ZSTD level per data temperature.** Use level 1-3 for hot data (faster decompression), level 6-9 for compacted cold data (better compression).

### 3.4 Pre-Aggregation (Materialized Views)

**The gap:** ClickHouse's materialized views are insert triggers that pre-aggregate data in real-time. This is how Datadog, Cloudflare, and every large observability platform achieve sub-second dashboard queries over billions of events. Without pre-aggregation, every dashboard query scans raw events.

Example: A "request rate by service, 1-minute buckets" dashboard panel over 24 hours:
- **Without pre-aggregation:** Scan ~86 billion raw events, aggregate in query
- **With pre-aggregation:** Scan ~1.4 million pre-aggregated rows

ClickHouse's `AggregatingMergeTree` stores intermediate aggregation states (HyperLogLog for uniq, t-digest for percentiles) that merge incrementally. This is the single most impactful feature for Datadog-class query speeds.

**Recommendations:**

1. **Implement real-time materialized views in the ingestion pipeline.** When data arrives in the BufferedWriteLayer, apply aggregation functions and write results to separate "rollup" Delta tables. Key rollup dimensions:
   - 1-minute buckets by (service, operation, status)
   - 5-minute buckets by (service, host)
   - 1-hour buckets by (service)

2. **Store intermediate aggregation states, not final values.** Use HyperLogLog for distinct counts, t-digest for percentiles, sum/count pairs for averages. This allows re-aggregation across time windows.

3. **Route queries to rollup tables when possible.** Add a query rewriter that detects `time_bucket()` + `GROUP BY service` patterns and redirects to the appropriate rollup table. This is how ClickHouse materialized views work transparently.

4. **Start with the 80/20 rule.** Pre-aggregate the queries that Datadog dashboards actually run: error rates, latency percentiles, request counts, top-N services. These cover 80%+ of dashboard queries.

### 3.5 No Distributed Query Execution

**The gap:** ClickHouse distributes queries across shards, with each shard scanning its local data in parallel. The initiator node merges results. TimeFusion is currently single-instance with optional DynamoDB-based multi-writer locking.

For trillions of events/day:
- At 500K events/sec per instance, you need ~23 instances just for ingestion (1 trillion / 86400 seconds ≈ 11.6M events/sec)
- Query parallelism is limited to one machine's CPU cores
- The MemBuffer and Foyer cache are not shared across instances

**Recommendations:**

1. **Adopt InfluxDB 3.0's disaggregated model.** Separate concerns into:
   - **Ingest nodes:** Accept writes, manage WAL + MemBuffer, flush to S3
   - **Query nodes:** Execute queries against Parquet on S3, with local SSD cache
   - **Compaction nodes:** Background optimization and Z-ordering of Parquet files
   - **Catalog:** PostgreSQL for metadata coordination (you already have this)

2. **Implement scatter-gather for distributed queries.** Query nodes should:
   - Parse the query and identify relevant partitions
   - Scatter partial queries to nodes that have relevant cached data
   - Gather and merge results

3. **Add a shared catalog for data placement.** Track which Parquet files exist, which query nodes have them cached, and which ingest nodes hold un-flushed data for leading-edge queries.

4. **Shared-nothing ingest, shared-everything query.** Ingest nodes can be sharded by project_id or time range. Query nodes should all be able to read from S3 — add local SSD caching (like Foyer, which you already have) for hot data.

---

## 4. Critical Gaps vs. InfluxDB 3.0

### 4.1 Ingestion Protocol

**The gap:** InfluxDB supports line protocol (simple, efficient, widely adopted in the monitoring ecosystem), OTLP, and Prometheus remote write. TimeFusion only accepts SQL INSERTs via pgwire.

For a Datadog alternative, you need:
- **OTLP gRPC/HTTP** (OpenTelemetry standard — this is the future)
- **Prometheus remote write** (for Prometheus-compatible metrics)
- **A high-throughput binary ingest protocol** (line protocol, Arrow Flight, or custom)

SQL INSERT is too slow for high-throughput ingestion — the overhead of SQL parsing per batch is significant.

**Recommendations:**

1. **Add an OTLP gRPC endpoint** as a first-class ingestion path. This bypasses SQL parsing entirely and maps directly to Arrow RecordBatches.

2. **Add an Arrow Flight endpoint** for bulk data loading and inter-service communication. This is zero-copy columnar transfer.

3. **Keep pgwire for ad-hoc queries** but not as the primary ingestion path.

### 4.2 Leading-Edge Query Architecture

**The gap:** InfluxDB 3.0 has a direct gRPC connection from Querier → Ingester to serve queries over un-persisted data. TimeFusion's approach of checking MemBuffer time ranges and using UnionExec is correct for a single instance but won't work when ingest and query are on separate nodes.

**Recommendation:** When splitting into ingest/query nodes, implement a Flight-based RPC from query nodes to ingest nodes for leading-edge data, similar to InfluxDB's architecture.

### 4.3 Automatic Sort Order Selection

**The gap:** InfluxDB 3.0 automatically selects the optimal sort order for Parquet files by choosing the least-cardinality columns first. This maximizes compression (similar values grouped together). TimeFusion currently Z-orders on (timestamp, service_name) which is good for queries but may not be optimal for compression.

**Recommendation:** During compaction, analyze column cardinality statistics and order sort columns from lowest to highest cardinality. For the otel_logs_and_spans table, this might be: `severity → service_name → timestamp` rather than `timestamp → service_name`.

---

## 5. Scaling to Trillions of Events/Day

### 5.1 The Math

| Metric | Value |
|--------|-------|
| Target throughput | 1 trillion events/day |
| Events per second | ~11.6 million |
| Avg event size (compressed) | ~200 bytes |
| Daily data volume (compressed) | ~200 TB/day |
| Annual data volume (compressed) | ~73 PB/year |
| Required ingest nodes (at 500K/sec each) | ~24 nodes |
| Required ingest nodes (at 2M/sec each, optimized) | ~6 nodes |

### 5.2 Architecture at Trillion-Scale

```
                        ┌─────────────────────────┐
                        │   Load Balancer          │
                        │   (OTLP / Flight / HTTP) │
                        └────────┬────────────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              ▼                  ▼                   ▼
     ┌────────────────┐ ┌────────────────┐ ┌────────────────┐
     │  Ingest Node 1 │ │  Ingest Node 2 │ │  Ingest Node N │
     │  - OTLP recv   │ │  - OTLP recv   │ │  - OTLP recv   │
     │  - WAL         │ │  - WAL         │ │  - WAL         │
     │  - MemBuffer   │ │  - MemBuffer   │ │  - MemBuffer   │
     │  - MV pipeline │ │  - MV pipeline │ │  - MV pipeline │
     └───────┬────────┘ └───────┬────────┘ └───────┬────────┘
             │                  │                   │
             ▼                  ▼                   ▼
     ┌────────────────────────────────────────────────────┐
     │              S3 Object Storage                      │
     │  - Raw Parquet files (partitioned by date)          │
     │  - Rollup Parquet files (1m, 5m, 1h buckets)       │
     │  - WAL files                                        │
     │  - Delta transaction logs                           │
     └────────────────────────┬───────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
     ┌────────────────┐ ┌──────────────┐ ┌──────────────────┐
     │  Query Node 1  │ │ Query Node 2 │ │  Query Node N    │
     │  - DataFusion  │ │ - DataFusion │ │  - DataFusion    │
     │  - SSD cache   │ │ - SSD cache  │ │  - SSD cache     │
     │  - Sparse idx  │ │ - Sparse idx │ │  - Sparse idx    │
     └────────────────┘ └──────────────┘ └──────────────────┘
              │                                    │
              ▼                                    ▼
     ┌────────────────┐                   ┌────────────────┐
     │ Compactor Node │                   │  Catalog (PG)  │
     │ - Z-order opt  │                   │  - Table meta  │
     │ - File merge   │                   │  - File index  │
     │ - Rollup build │                   │  - Placement   │
     └────────────────┘                   └────────────────┘
```

### 5.3 Priority Roadmap

The following represents the high-impact changes ordered by their contribution to closing the gap with ClickHouse performance at scale:

**Phase 1: Query Performance (Highest Impact)**
- Implement materialized views / rollup tables for common dashboard queries
- Increase DataFusion batch size to 65,536
- Cache Parquet column statistics in memory for file-level pruning
- Enable Parquet page index and bloom filter pruning
- Use per-column Parquet encodings (DELTA_BINARY_PACKED for timestamps, BYTE_STREAM_SPLIT for floats)

**Phase 2: Ingestion Throughput**
- Add OTLP gRPC ingest endpoint (bypass SQL parsing)
- Add Arrow Flight ingest endpoint for bulk loading
- Increase MemBuffer bucket parallelism (adaptive bucket sizing instead of fixed 10-min)
- Implement ingestion-time materialized view pipeline

**Phase 3: Distributed Architecture**
- Separate ingest and query node roles
- Add shared Parquet file catalog (extend existing PostgreSQL config DB)
- Implement scatter-gather distributed query execution
- Add leading-edge query RPC (query node → ingest node)

**Phase 4: Advanced Optimizations**
- Build in-memory sparse index for hot partitions
- Add data-skipping bloom filter sidecar files for high-cardinality columns
- Implement query result caching (identical query deduplication)
- Add cost-based query optimizer that routes to rollup tables when possible
- Consider JIT compilation for expression evaluation (DataFusion is exploring this)

---

## 6. Detailed Component Comparison

### 6.1 Storage Engine

| Feature | TimeFusion | ClickHouse | InfluxDB 3.0 | Gap Assessment |
|---------|-----------|------------|--------------|----------------|
| Format | Parquet (Delta Lake) | Custom columnar (MergeTree) | Parquet | TimeFusion is on par with InfluxDB |
| Partition strategy | Daily date | Configurable (monthly recommended) | Daily timestamp | Adequate for time-series |
| Sort order | Z-order (timestamp, service) | Primary key sort | Auto-selected by cardinality | Consider cardinality-based auto-selection |
| Compression ratio | ~10-20x (ZSTD) | ~5-20x (codec-stacked) | ~25x (encoding + ZSTD) | Improve with per-column encodings |
| ACID transactions | Yes (Delta Lake) | Atomic part creation | Yes (catalog + object store) | Strong advantage over ClickHouse |
| Tiered storage | S3 only | Local SSD + S3 | S3 only | Consider local SSD cache tier |
| Compaction | Light (5m) + Full (30m) | Continuous background merge | Separate compactor process | Adequate, consider dedicated compactor |

### 6.2 Query Engine

| Feature | TimeFusion | ClickHouse | InfluxDB 3.0 | Gap Assessment |
|---------|-----------|------------|--------------|----------------|
| Engine | DataFusion 52.1 | Custom C++ | DataFusion | On par with InfluxDB, behind ClickHouse |
| SIMD | Compiler auto-vectorization | Hand-tuned intrinsics | Compiler auto-vectorization | Significant gap vs ClickHouse |
| JIT | None | LLVM JIT | None | Gap, but lower priority |
| Block/batch size | 8,192 rows | 65,505 rows | Varies | Increase to 65,536 |
| Parallel execution | Multi-core via DataFusion | Multi-core + multi-node | Multi-core via DataFusion | Need distributed execution |
| Join support | Via DataFusion | Full SQL joins | Via DataFusion | On par with InfluxDB |
| Pre-aggregation | None | Materialized views | None | Critical gap |
| Query caching | None | Query result cache | None | Add for dashboard workloads |

### 6.3 Ingestion Pipeline

| Feature | TimeFusion | ClickHouse | InfluxDB 3.0 | Gap Assessment |
|---------|-----------|------------|--------------|----------------|
| Throughput (single node) | ~500K events/sec | ~2-6M rows/sec | ~320K-1.3M rows/sec | Behind ClickHouse, competitive with InfluxDB |
| Protocol | SQL INSERT (pgwire) | HTTP + native TCP | Line protocol + HTTP | Need OTLP + Flight |
| WAL | walrus-rust (local disk) | No true WAL (async inserts) | Object store WAL | Adequate |
| Buffer strategy | 10-min time buckets | Async insert buffer | Mutable batches + WAL | Adequate |
| Backpressure | Memory-based CAS | Part count limits | WAL buffer limits | Adequate |
| Deduplication | None | Block-level (replicated) | At compaction time | Add if needed for at-least-once delivery |

### 6.4 Caching

| Feature | TimeFusion | ClickHouse | InfluxDB 3.0 | Gap Assessment |
|---------|-----------|------------|--------------|----------------|
| L1 (Memory) | 512MB Foyer | Mark cache (5GB) + page cache | DataFusion memory pool | Increase Foyer memory significantly |
| L2 (SSD) | 100GB Foyer disk | OS page cache + uncompressed cache | Local SSD cache | Adequate |
| Metadata cache | 512MB Foyer | primary.idx always in RAM | Catalog cache | Cache column stats in RAM |
| Query result cache | None | Yes (configurable TTL) | None | Add for dashboard workloads |
| Uncompressed cache | None | Optional (disabled by default) | None | Consider for hot data |

---

## 7. Summary of Recommendations

### Must-Have for Datadog Alternative

1. **Materialized views / rollup tables** — without this, every dashboard query scans raw events. This is the single highest-impact feature for query performance. ClickHouse users report 100-1000x speedups from materialized views.

2. **OTLP ingestion endpoint** — SQL INSERT cannot sustain 11.6M events/sec. OTLP is the industry standard for observability data. Arrow Flight for bulk ingest.

3. **Distributed architecture** — separate ingest and query nodes scaling independently. Shared S3 storage, shared catalog, independent compute.

4. **In-memory sparse indexes** — for the hot path (today's data), maintain a memory-resident index that maps time ranges to Parquet file locations. Avoid round-trips to S3 for metadata.

5. **Per-column Parquet encodings** — DELTA_BINARY_PACKED for timestamps, BYTE_STREAM_SPLIT for floats. Immediate compression improvement with no query-side changes.

### Important for Scale

6. **Larger DataFusion batch sizes** (65,536 rows)
7. **Parquet column statistics cache** in memory
8. **Query result caching** for dashboard workloads (same query repeated every 30 seconds)
9. **Automatic sort order selection** based on column cardinality
10. **Bloom filter sidecar files** for trace_id, span_id lookups

### Nice-to-Have

11. JIT compilation for expressions (wait for DataFusion to implement)
12. Custom SIMD kernels for hot code paths
13. Uncompressed data cache for frequently-accessed columns
14. Adaptive time bucket sizing based on ingestion rate
