# Buffered Write Layer Architecture

TimeFusion implements an InfluxDB-inspired in-memory buffer with Write-Ahead Logging (WAL) for sub-second query latency on recent data while maintaining durability through Delta Lake.

## Overview

```
                                    ┌─────────────────┐
                                    │   SQL Query     │
                                    └────────┬────────┘
                                             │
                                             ▼
                              ┌──────────────────────────────┐
                              │    ProjectRoutingTable       │
                              │       (TableProvider)        │
                              └──────────────┬───────────────┘
                                             │
                         ┌───────────────────┼───────────────────┐
                         │                   │                   │
                         ▼                   ▼                   ▼
              ┌──────────────────┐  ┌───────────────┐  ┌─────────────────┐
              │ Query entirely   │  │ Query spans   │  │ No MemBuffer    │
              │ in MemBuffer     │  │ both ranges   │  │ data            │
              │ time range       │  │               │  │                 │
              └────────┬─────────┘  └───────┬───────┘  └────────┬────────┘
                       │                    │                   │
                       ▼                    ▼                   ▼
              ┌──────────────┐     ┌────────────────┐   ┌──────────────┐
              │ MemBuffer    │     │   UnionExec    │   │  Delta Lake  │
              │ Only         │     │ (Mem + Delta)  │   │  Only        │
              └──────────────┘     └────────────────┘   └──────────────┘
```

## Components

### 1. Write-Ahead Log (WAL) - `src/wal.rs`

Uses [walrus-rust](https://github.com/nubskr/walrus/) for durable, topic-based logging.

```rust
pub struct WalManager {
    wal: Walrus,
    data_dir: PathBuf,
}
```

**Key features:**
- Topic-based partitioning: `{project_id}:{table_name}`
- Arrow IPC serialization for RecordBatch data
- Configurable fsync schedule (default: 200ms)
- Supports batch append for efficiency

**Data flow:**
```
INSERT → WAL.append() → MemBuffer.insert() → Response to client
              │
              └─────────────────────────────────────────┐
                                                        ▼
                                              (async, every 10 min)
                                                        │
                                              Delta Lake write
                                                        │
                                              WAL.checkpoint()
```

### 2. In-Memory Buffer - `src/mem_buffer.rs`

Flattened, time-bucketed storage for recent data optimized for high insert throughput.

```rust
/// Composite key using Arc<str> for efficient cloning
pub type TableKey = (Arc<str>, Arc<str>);  // (project_id, table_name)

pub struct MemBuffer {
    tables: DashMap<TableKey, Arc<TableBuffer>>,  // Flattened: 1 lookup instead of 2
    estimated_bytes: AtomicUsize,
}

pub struct TableBuffer {
    buckets: DashMap<i64, TimeBucket>,  // bucket_id → TimeBucket
    schema: RwLock<SchemaRef>,
    project_id: Arc<str>,
    table_name: Arc<str>,
}

pub struct TimeBucket {
    batches: RwLock<Vec<RecordBatch>>,
    row_count: AtomicUsize,
    memory_bytes: AtomicUsize,
    min_timestamp: AtomicI64,
    max_timestamp: AtomicI64,
}
```

**Design rationale:**
- Flattened from 3-level hierarchy (project → table → bucket) to 2-level (table → bucket)
- `Arc<str>` keys avoid string cloning on every lookup
- `Arc<TableBuffer>` enables handle caching for batch operations

**Time bucketing:**
- Bucket duration: 10 minutes
- `bucket_id = timestamp_micros / (10 * 60 * 1_000_000)`
- Mirrors Delta Lake's date partitioning for efficient queries

**Insert methods:**
- `get_or_create_table()` - Returns `Arc<TableBuffer>` for caching across batch operations
- `TableBuffer::insert_batch()` - Direct bucket insertion, bypasses table lookup
- `insert_batches()` - Caches table handle internally for the batch loop

**Query methods:**
- `query()` - Returns all batches as a flat `Vec<RecordBatch>`
- `query_partitioned()` - Returns `Vec<Vec<RecordBatch>>` with one partition per time bucket (enables parallel execution)

### 3. Buffered Write Layer - `src/buffered_write_layer.rs`

Orchestrates WAL, MemBuffer, and Delta Lake writes.

```rust
pub struct BufferedWriteLayer {
    wal: Arc<WalManager>,
    mem_buffer: Arc<MemBuffer>,
    config: BufferConfig,
    shutdown: CancellationToken,
    delta_write_callback: Option<DeltaWriteCallback>,
}
```

**Background tasks:**
1. **Flush Task** (every 10 min): Writes completed time buckets to Delta Lake
2. **Eviction Task** (every 1 min): Removes data older than retention period from MemBuffer and WAL

## Query Execution

### Time-Based Exclusion Strategy

The system uses time-based exclusion to prevent duplicate data between MemBuffer and Delta:

```rust
// In ProjectRoutingTable::scan()

// 1. Get MemBuffer's time range
let mem_time_range = layer.get_time_range(&project_id, &table_name);

// 2. Extract query's time range from filters
let query_time_range = self.extract_time_range_from_filters(&filters);

// 3. Determine if Delta can be skipped
let skip_delta = match (mem_time_range, query_time_range) {
    (Some((mem_oldest, _)), Some((query_min, query_max))) => {
        // Query entirely within MemBuffer's range
        query_min >= mem_oldest && query_max >= mem_oldest
    }
    _ => false,
};

// 4. If not skipping Delta, add exclusion filter
let delta_filters = if let Some(cutoff) = oldest_mem_ts {
    // Delta only sees: timestamp < mem_oldest
    filters.push(Expr::lt(col("timestamp"), lit(cutoff)));
    filters
} else {
    filters
};
```

**Result:** No duplicate scans - MemBuffer handles `timestamp >= oldest_mem_ts`, Delta handles `timestamp < oldest_mem_ts`.

### Parallel Execution with MemorySourceConfig

Instead of using `MemTable` (which creates a single partition), we use `MemorySourceConfig` directly with multiple partitions:

```rust
fn create_memory_exec(&self, partitions: &[Vec<RecordBatch>], projection: Option<&Vec<usize>>) -> DFResult<Arc<dyn ExecutionPlan>> {
    let mem_source = MemorySourceConfig::try_new(
        partitions,  // One partition per time bucket
        self.schema.clone(),
        projection.cloned(),
    )?;
    Ok(Arc::new(DataSourceExec::new(Arc::new(mem_source))))
}
```

**Partition structure:**
```
MemBuffer Query
     │
     ▼
┌─────────────────────────────────────────────┐
│           MemorySourceConfig                │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │
│  │Bucket 0 │ │Bucket 1 │ │Bucket 2 │ ...   │
│  │10:00-10 │ │10:10-20 │ │10:20-30 │       │
│  └────┬────┘ └────┬────┘ └────┬────┘       │
│       │          │           │              │
│       ▼          ▼           ▼              │
│    Core 0     Core 1      Core 2            │
└─────────────────────────────────────────────┘
```

### UnionExec vs InterleaveExec

We use `UnionExec` instead of `InterleaveExec` because:

| Aspect | UnionExec | InterleaveExec |
|--------|-----------|----------------|
| Partition requirement | None | Requires identical hash partitioning |
| Our partitioning | Time buckets (MemBuffer) + Files (Delta) | Not compatible |
| Output partitions | M + N (concatenated) | Same as input |
| Parallel execution | Yes (each partition independent) | Yes |

`InterleaveExec` requires `can_interleave()` check to pass:
```rust
pub fn can_interleave(inputs: impl Iterator<Item = &Arc<dyn ExecutionPlan>>) -> bool {
    // Requires all inputs to have identical Hash partitioning
    matches!(reference, Partitioning::Hash(_, _))
        && inputs.all(|plan| plan.output_partitioning() == *reference)
}
```

Since MemBuffer uses `UnknownPartitioning` (time buckets) and Delta uses file-based partitioning, `InterleaveExec` cannot be used.

## Performance Characteristics

### Optimizations Implemented

| Optimization | Impact |
|-------------|--------|
| Flattened MemBuffer structure | Reduced from 3 hash lookups to 1-2 per insert |
| `Arc<str>` composite keys | Avoids string cloning on every table lookup |
| `Arc<TableBuffer>` handle caching | Amortizes lookup cost across batch operations |
| Partitioned MemBuffer queries | Multi-core parallel execution for in-memory data |
| Time-range filter extraction | Skip Delta entirely for recent-data queries |
| Direct MemorySourceConfig | Avoids extra data copying through MemTable |
| Time-based exclusion | No duplicate scans between sources |
| DashMap for concurrent access | Lock-free reads, minimal write contention |

### Data Copying Analysis

| Operation | Copies | Notes |
|-----------|--------|-------|
| `query_partitioned()` | 1 | Clones batches from RwLock |
| `MemorySourceConfig` | 0 | Stores reference to partitions |
| `MemoryStream::poll_next()` | 0-1 | None if no projection, clone if projecting |

### Locking Strategy

| Component | Lock Type | Contention |
|-----------|-----------|------------|
| `MemBuffer.tables` | DashMap (lock-free reads) | Very low |
| `TableBuffer.buckets` | DashMap (lock-free reads) | Very low |
| `TableBuffer.schema` | RwLock | Very low (rarely changes) |
| `TimeBucket.batches` | RwLock | Low (read-heavy workload) |

**Key insight:** Query path uses read locks only. Write path acquires write lock briefly per bucket. Handle caching (`Arc<TableBuffer>`) further reduces contention by avoiding repeated table lookups.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `WALRUS_DATA_DIR` | `/var/lib/timefusion/wal` | WAL storage directory |
| `TIMEFUSION_FLUSH_INTERVAL_SECS` | `600` | Flush to Delta interval (10 min) |
| `TIMEFUSION_BUFFER_RETENTION_MINS` | `90` | Data retention in buffer |
| `TIMEFUSION_EVICTION_INTERVAL_SECS` | `60` | Eviction check interval |
| `TIMEFUSION_BUFFER_MAX_MEMORY_MB` | `4096` | Memory limit for buffer |

## Recovery

On startup, the system recovers from WAL:

```rust
pub async fn recover_from_wal(&self) -> anyhow::Result<RecoveryStats> {
    let cutoff = now() - retention_duration;
    // checkpoint=false: WAL entries are only removed after successful Delta flush
    let entries = self.wal.read_all_entries(Some(cutoff), false)?;

    for (entry, batch) in entries {
        self.mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros)?;
    }
}
```

Only entries within the retention window are replayed.

## Graceful Shutdown

```rust
pub async fn shutdown(&self) -> anyhow::Result<()> {
    // 1. Signal background tasks to stop
    self.shutdown.cancel();

    // 2. Wait for tasks to notice
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 3. Force flush all remaining buckets to Delta
    for bucket in self.mem_buffer.get_all_buckets() {
        self.flush_bucket(&bucket).await?;
        self.mem_buffer.drain_bucket(...);
        self.wal.checkpoint(...)?;
    }
}
```

## Tradeoffs

### Chosen Approach: Flattened 2-Level Hierarchy

**Pros:**
- Single hash lookup for table access (was 2 lookups with project → table)
- `Arc<str>` keys are cheap to clone and compare
- `Arc<TableBuffer>` enables handle caching for batch operations
- Simpler iteration for flush/eviction (no nested loops)

**Cons:**
- Can't efficiently iterate "all tables for project X" without scanning all entries
- Composite key tuple slightly larger than single string

**Alternative considered:** 3-level hierarchy (project → table → bucket)
- Rejected: Extra hash lookup on every insert not worth the organizational benefit

### Chosen Approach: Time-Based Exclusion

**Pros:**
- No duplicate data between sources
- Simple mental model
- Efficient partition pruning in Delta

**Cons:**
- Queries spanning both ranges require union
- Slightly more complex filter manipulation

**Alternative considered:** Deduplication at query time using row IDs
- Rejected: Would require tracking row IDs and dedup logic, more expensive

### Chosen Approach: 10-Minute Time Buckets

**Pros:**
- Natural parallelism (one partition per bucket)
- Matches typical flush interval
- Good balance of granularity vs overhead

**Cons:**
- Fixed granularity (not adaptive to workload)
- Very short queries might not benefit from parallelism

### Chosen Approach: Clone-on-Query

**Pros:**
- Simple implementation
- Releases locks quickly
- Predictable memory behavior

**Cons:**
- Memory overhead during query
- Extra copying for large result sets

**Alternative considered:** Zero-copy with Arc<RecordBatch>
- Rejected: Would complicate lifetime management and eviction

## Files

| File | Purpose |
|------|---------|
| `src/wal.rs` | WAL manager using walrus-rust |
| `src/mem_buffer.rs` | In-memory buffer with time buckets |
| `src/buffered_write_layer.rs` | Orchestration layer |
| `src/database.rs` | Modified `ProjectRoutingTable::scan()` for unified queries |

## Future Improvements

1. **Adaptive bucket sizing** - Adjust bucket duration based on write rate
2. **Predicate pushdown to MemBuffer** - Apply filters during query, not after
3. **Compression in MemBuffer** - Reduce memory footprint for string-heavy data
4. **Metrics and observability** - Expose buffer stats, flush latency, skip rates
5. **Ring buffer for ultra-high throughput** - Lock-free writes if >100K inserts/sec needed
