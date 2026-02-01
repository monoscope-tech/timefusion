# TimeFusion Architecture

This document provides a comprehensive overview of TimeFusion's architecture, covering all major subsystems and their interactions.

## System Overview

TimeFusion is a time-series database that combines:
- **Apache DataFusion**: Vectorized SQL query engine
- **Delta Lake**: ACID transactional storage on S3
- **PostgreSQL Wire Protocol**: Client compatibility via `datafusion-postgres`
- **Buffered Write Layer**: Sub-second write latency with WAL + MemBuffer

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          PostgreSQL Clients                                  │
│                    (psql, pgAdmin, any PostgreSQL driver)                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PGWire Protocol Layer                                │
│                      (datafusion-postgres crate)                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      DataFusion Query Engine                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐  │
│  │AnalyzerRules    │  │PhysicalPlanner  │  │ExpressionPlanner            │  │
│  │• VariantInsert  │  │• DmlQueryPlanner│  │• VariantAwareExprPlanner    │  │
│  │• VariantSelect  │  │                 │  │  (-> and ->> operators)     │  │
│  └─────────────────┘  └─────────────────┘  └─────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
          ┌──────────────────────────┼──────────────────────────┐
          ▼                          ▼                          ▼
┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────────┐
│ Buffered Write Layer│  │  Object Store Cache │  │      Delta Lake         │
│  ┌───────────────┐  │  │     (Foyer)         │  │       on S3             │
│  │     WAL       │  │  │  ┌─────────────┐    │  │  ┌─────────────────┐    │
│  │  (walrus-     │  │  │  │ L1: Memory  │    │  │  │ Parquet Files   │    │
│  │   rust)       │  │  │  │ (512MB)     │    │  │  │ + Delta Log     │    │
│  └───────────────┘  │  │  └─────────────┘    │  │  └─────────────────┘    │
│  ┌───────────────┐  │  │  ┌─────────────┐    │  │                         │
│  │  MemBuffer    │  │  │  │ L2: Disk    │    │  │  Partitioned by:        │
│  │ (10-min       │  │  │  │ (100GB)     │    │  │  • project_id           │
│  │  buckets)     │  │  │  └─────────────┘    │  │  • date                 │
│  └───────────────┘  │  │                     │  │                         │
└─────────────────────┘  └─────────────────────┘  └─────────────────────────┘
```

## Module Structure

```
src/
├── main.rs                        # Entry point, server startup
├── lib.rs                         # Module exports
├── config.rs                      # OnceLock<AppConfig> singleton
├── database.rs                    # Core DB engine (~2600 lines)
├── buffered_write_layer.rs        # Orchestrates WAL + MemBuffer
├── mem_buffer.rs                  # In-memory storage with time buckets
├── wal.rs                         # Write-ahead log (walrus-rust)
├── object_store_cache.rs          # Foyer L1/L2 hybrid cache
├── dml.rs                         # UPDATE/DELETE interception
├── functions.rs                   # Custom SQL functions + VariantAwareExprPlanner
├── schema_loader.rs               # YAML schema registry (compile-time embedded)
├── pgwire_handlers.rs             # PostgreSQL protocol handlers
├── batch_queue.rs                 # Queue for batch insert operations
├── statistics.rs                  # Delta statistics extraction
├── telemetry.rs                   # OpenTelemetry integration
├── test_utils.rs                  # Testing utilities
└── optimizers/
    ├── mod.rs                     # Optimizer utilities + partition pruning
    ├── variant_insert_rewriter.rs # INSERT: Utf8 → json_to_variant()
    └── variant_select_rewriter.rs # SELECT: Variant → variant_to_json()
```

## Data Flow

### Insert Path

```
Client INSERT
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. PGWire parses SQL                                            │
│ 2. DataFusion analyzes query                                    │
│    └── VariantInsertRewriter wraps Utf8→json_to_variant()       │
│ 3. Execute INSERT                                               │
└─────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│ BufferedWriteLayer.insert()                                     │
│ 1. Check memory pressure → early flush if needed                │
│ 2. try_reserve_memory() → atomic CAS with backoff               │
│ 3. WAL.append_batch() → durable write (fsync every 200ms)       │
│ 4. MemBuffer.insert() → fast in-memory write                    │
│ 5. release_reservation()                                        │
└─────────────────────────────────────────────────────────────────┘
     │
     ▼
Response to client (sub-second latency)
```

### Select Path

```
Client SELECT
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. PGWire parses SQL                                            │
│ 2. DataFusion analyzes query                                    │
│    └── VariantSelectRewriter wraps Variant→variant_to_json()    │
│ 3. VariantAwareExprPlanner handles -> and ->> operators         │
│ 4. Physical planning                                            │
└─────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│ ProjectRoutingTable.scan()                                      │
│ 1. Extract project_id from WHERE clause (mandatory)             │
│ 2. Get MemBuffer time range                                     │
│ 3. Determine data sources:                                      │
│    • Query entirely in MemBuffer? → MemBuffer only              │
│    • Query spans both? → UnionExec(MemBuffer + Delta)           │
│    • No MemBuffer data? → Delta only                            │
│ 4. Add time-range exclusion filter for Delta                    │
└─────────────────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│ Execution                                                       │
│ • MemBuffer: query_partitioned() → parallel by time bucket      │
│ • Delta: Parquet scan with partition pruning                    │
│ • Object Store Cache: L1/L2 caching of parquet files            │
└─────────────────────────────────────────────────────────────────┘
     │
     ▼
Result stream → PGWire encoding → Client
```

### Flush Path (Background)

```
Every 10 minutes (flush_interval_secs)
     │
     ▼
┌─────────────────────────────────────────────────────────────────┐
│ BufferedWriteLayer.flush_completed_buckets()                    │
│ 1. Acquire flush lock                                           │
│ 2. Get flushable buckets (bucket_id < current_bucket)           │
│ 3. For each bucket (parallel with bounded concurrency):         │
│    a. DeltaWriteCallback → write to Delta Lake                  │
│    b. WAL.checkpoint() → mark entries as consumed               │
│    c. MemBuffer.drain_bucket() → free memory                    │
└─────────────────────────────────────────────────────────────────┘
```

## Multi-Tenant Storage Model

### Two Table Types

1. **Unified Tables**: Default projects share one Delta table per schema
   - Partitioned by `[project_id, date]`
   - Path: `s3://bucket/timefusion/default/{table_name}/`

2. **Custom Project Tables**: Isolated tables for specific projects
   - Own S3 bucket/path configuration
   - Path: `s3://bucket/timefusion/projects/{project_id}/{table_name}/`

### Routing

- `WHERE project_id = 'xxx'` is **mandatory** in all queries
- `ProjectIdPushdown` utility validates filters contain project_id
- MemBuffer uses composite key: `(Arc<str>, Arc<str>)` for (project_id, table_name)

## Key Data Structures

### MemBuffer Hierarchy

```
MemBuffer
  └── tables: DashMap<TableKey, Arc<TableBuffer>>
        │
        └── TableBuffer
              ├── schema: SchemaRef (immutable)
              ├── project_id: Arc<str>
              ├── table_name: Arc<str>
              └── buckets: DashMap<i64, TimeBucket>
                    │
                    └── TimeBucket
                          ├── batches: RwLock<Vec<RecordBatch>>
                          ├── row_count: AtomicUsize
                          ├── memory_bytes: AtomicUsize
                          ├── min_timestamp: AtomicI64
                          └── max_timestamp: AtomicI64
```

**Key type:** `TableKey = (Arc<str>, Arc<str>)` - (project_id, table_name)

**Bucket ID calculation:** `bucket_id = timestamp_micros / (10 * 60 * 1_000_000)`

### WAL Entry Format

```
┌─────────────────────────────────────────────────────────────────┐
│ WAL_MAGIC: 4 bytes [0x57, 0x41, 0x4C, 0x32] ("WAL2")            │
│ VERSION: 1 byte (128)                                           │
│ OPERATION: 1 byte (0=Insert, 1=Delete, 2=Update)                │
│ BINCODE_PAYLOAD: WalEntry                                       │
│   ├── timestamp_micros: i64                                     │
│   ├── project_id: String                                        │
│   ├── table_name: String                                        │
│   ├── operation: WalOperation                                   │
│   └── data: Vec<u8>                                             │
│       ├── Insert: CompactBatch (Arrow data without schema)      │
│       ├── Delete: DeletePayload { predicate_sql }               │
│       └── Update: UpdatePayload { predicate_sql, assignments }  │
└─────────────────────────────────────────────────────────────────┘
```

### Configuration (AppConfig)

```rust
AppConfig {
    aws: AwsConfig,              // S3/DynamoDB credentials and endpoints
    core: CoreConfig,            // Data directory, PGWire port, table prefix
    buffer: BufferConfig,        // Flush intervals, memory limits, WAL settings
    cache: CacheConfig,          // Foyer cache sizes, TTL
    parquet: ParquetConfig,      // Compression, row groups, page limits
    maintenance: MaintenanceConfig, // Optimize, vacuum schedules
    memory: MemoryConfig,        // Memory limits and spill settings
    telemetry: TelemetryConfig,  // OTLP endpoint, service name/version
}
```

## Query Transformation Pipeline

### Analyzer Rules (Before Type Checking)

1. **VariantInsertRewriter** (`src/optimizers/variant_insert_rewriter.rs`)
   - Intercepts `LogicalPlan::Dml` with `WriteOp::Insert`
   - Finds columns where target schema has Variant type
   - Wraps Utf8/Utf8View literals with `json_to_variant()` UDF
   - Applies recursively to Values and Projection nodes

2. **VariantSelectRewriter** (`src/optimizers/variant_select_rewriter.rs`)
   - Intercepts `LogicalPlan::Projection`
   - Checks if expression result type is Variant (via `is_variant_type()`)
   - Wraps with `variant_to_json()` for PostgreSQL wire protocol
   - Preserves column aliases

### Physical Planner

- **DmlQueryPlanner** (`src/dml.rs`)
  - Intercepts UPDATE/DELETE logical plans
  - Extracts table_name, project_id, predicate, assignments
  - Creates `DmlExec` physical plan
  - Logs to WAL and applies to MemBuffer

### Expression Planner

- **VariantAwareExprPlanner** (`src/functions.rs`)
  - Handles `->` (get JSON object) and `->>` (get JSON as text) operators
  - Converts to `variant_get(col, "path.to.field")` calls
  - Builds dot-path strings from nested access patterns

## Caching Architecture

### Foyer Hybrid Cache

```
┌─────────────────────────────────────────────────────────────────┐
│                     FoyerObjectStoreCache                        │
├─────────────────────────────────────────────────────────────────┤
│  Main Cache (Parquet data files)                                │
│  ├── L1: Memory (512MB default)                                 │
│  └── L2: Disk (100GB default)                                   │
├─────────────────────────────────────────────────────────────────┤
│  Metadata Cache (Parquet footers)                               │
│  ├── L1: Memory (512MB)                                         │
│  └── L2: Disk (5GB)                                             │
├─────────────────────────────────────────────────────────────────┤
│  Features:                                                       │
│  • TTL-based expiration (7 days default)                        │
│  • Implements ObjectStore trait transparently                   │
│  • Statistics tracking (hits, misses, expirations)              │
└─────────────────────────────────────────────────────────────────┘
```

## Safety and Durability

### Memory Management

- **Reservation system**: Atomic CAS prevents race conditions
- **20% overhead multiplier**: Accounts for Arrow alignment/metadata
- **Hard limit**: `max_bytes + max_bytes/5 = 120%` headroom
- **Exponential backoff**: Reduces CPU thrashing under contention

### WAL Durability

- **Fsync schedule**: Every 200ms (configurable)
- **Size limits**: `MAX_BATCH_SIZE = 100MB` prevents unbounded allocation
- **Version detection**: Byte 4 > 2 distinguishes from legacy format
- **Recovery**: On startup, replays entries within retention window

### Crash Recovery

```
Startup
  │
  ▼
┌─────────────────────────────────────────────────────────────────┐
│ BufferedWriteLayer.recover_from_wal()                           │
│ 1. Calculate cutoff = now - retention_mins                      │
│ 2. Read all WAL entries (sorted by timestamp)                   │
│ 3. For each entry within retention:                             │
│    • Insert: Replay to MemBuffer                                │
│    • Delete: Apply delete to MemBuffer                          │
│    • Update: Apply update to MemBuffer                          │
│ 4. Report recovery stats                                        │
└─────────────────────────────────────────────────────────────────┘
```

## Key Constants

```rust
// MemBuffer
BUCKET_DURATION_MICROS = 10 * 60 * 1_000_000  // 10 minutes

// BufferedWriteLayer
MEMORY_OVERHEAD_MULTIPLIER = 1.2  // 20% overhead
HARD_LIMIT_MULTIPLIER = 5         // max + max/5 = 120%
MAX_CAS_RETRIES = 100
CAS_BACKOFF_BASE_MICROS = 1

// WAL
WAL_MAGIC = [0x57, 0x41, 0x4C, 0x32]  // "WAL2"
WAL_VERSION = 128
MAX_BATCH_SIZE = 100 * 1024 * 1024    // 100MB
FSYNC_SCHEDULE_MS = 200
```

## Related Documentation

- [Buffered Write Layer](buffered-write-layer.md) - Detailed WAL and MemBuffer internals
- [Multi-Table Architecture](MULTI_TABLE_ARCHITECTURE.md) - Multi-tenant table organization
- [Caching](CACHING.md) - Foyer cache configuration
- [Tracing](TRACING.md) - OpenTelemetry integration
- [Delta Checkpoint Handling](DELTA_CHECKPOINT_HANDLING.md) - Delta Lake internals
