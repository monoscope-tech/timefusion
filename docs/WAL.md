# Write-Ahead Log (WAL)

TimeFusion uses a Write-Ahead Log for durability, ensuring data is never lost even if the server crashes before flushing to Delta Lake.

## Overview

The WAL is implemented using [walrus-rust](https://github.com/nubskr/walrus/), a topic-based logging library. Every write operation is logged before being applied to the in-memory buffer.

```
Client INSERT
     │
     ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ WAL.append()    │───▶│ MemBuffer.insert│───▶│ Response        │
│ (durable)       │    │ (fast)          │    │ to client       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
     │
     │ (async, every 10 min)
     ▼
┌─────────────────┐    ┌─────────────────┐
│ Delta Lake      │───▶│ WAL.checkpoint()│
│ write           │    │ (mark consumed) │
└─────────────────┘    └─────────────────┘
```

## Entry Format

### Wire Format

```
┌──────────────────────────────────────────────────────────────┐
│ Byte 0-3:  WAL_MAGIC [0x57, 0x41, 0x4C, 0x32] ("WAL2")       │
│ Byte 4:    VERSION (130)                                     │
│ Byte 5:    OPERATION (0=Insert, 1=Delete, 2=Update)          │
│ Byte 6+:   BINCODE_PAYLOAD (WalEntry)                        │
└──────────────────────────────────────────────────────────────┘
```

### WalEntry Structure

```rust
#[derive(Debug, Encode, Decode)]
pub struct WalEntry {
    pub timestamp_micros: i64,
    pub project_id: String,
    pub table_name: String,
    pub operation: WalOperation,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub enum WalOperation {
    Insert = 0,
    Delete = 1,
    Update = 2,
}
```

### Data Payloads

**Insert**: `CompactBatch` (Arrow data without schema)
```rust
struct CompactBatch {
    num_rows: usize,
    columns: Vec<CompactColumn>,
}

struct CompactColumn {
    null_bitmap: Option<Vec<u8>>,
    buffers: Vec<Vec<u8>>,
    children: Vec<CompactColumn>,
    null_count: usize,
    child_lens: Vec<usize>,
}
```

**Delete**:
```rust
struct DeletePayload {
    predicate_sql: Option<String>,
}
```

**Update**:
```rust
struct UpdatePayload {
    predicate_sql: Option<String>,
    assignments: Vec<(String, String)>,  // (column, value_sql)
}
```

## Topic Partitioning

Each (project_id, table_name) combination gets its own WAL topic:

- **Human-readable topic**: `{project_id}:{table_name}`
- **Walrus key**: 16-character hex hash (walrus has 62-byte metadata limit)

```rust
fn walrus_topic_key(project_id: &str, table_name: &str) -> String {
    let mut hasher = AHasher::default();
    project_id.hash(&mut hasher);
    table_name.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}
```

Topics are persisted to `.timefusion_meta/topics` for discovery on startup.

## Operations

### Append

```rust
// Single batch
wal.append(project_id, table_name, &batch)?;

// Multiple batches (more efficient)
wal.append_batch(project_id, table_name, &batches)?;

// DML operations
wal.append_delete(project_id, table_name, predicate_sql)?;
wal.append_update(project_id, table_name, predicate_sql, &assignments)?;
```

### Read

```rust
// Read entries for a specific table
let (entries, error_count) = wal.read_entries_raw(
    project_id,
    table_name,
    Some(cutoff_timestamp),  // Filter old entries
    checkpoint,              // Mark as consumed?
)?;

// Read all entries across all tables
let (entries, error_count) = wal.read_all_entries_raw(
    Some(cutoff_timestamp),
    checkpoint,
)?;
```

### Checkpoint

After successful Delta Lake flush, mark WAL entries as consumed:

```rust
wal.checkpoint(project_id, table_name)?;
```

This removes the entries from the WAL, preventing replay on next startup.

## Recovery

On startup, the system replays WAL entries within the retention window:

```rust
pub async fn recover_from_wal(&self) -> anyhow::Result<RecoveryStats> {
    let retention_micros = (retention_mins as i64) * 60 * 1_000_000;
    let cutoff = now() - retention_micros;

    let (entries, error_count) = self.wal.read_all_entries_raw(Some(cutoff), true)?;

    // Fail if corruption exceeds threshold
    if corruption_threshold > 0 && error_count >= corruption_threshold {
        anyhow::bail!("WAL corruption threshold exceeded");
    }

    for entry in entries {
        match entry.operation {
            WalOperation::Insert => {
                let batch = WalManager::deserialize_batch(&entry.data, &entry.table_name)?;
                self.mem_buffer.insert(&entry.project_id, &entry.table_name, batch, entry.timestamp_micros)?;
            }
            WalOperation::Delete => {
                let payload = deserialize_delete_payload(&entry.data)?;
                self.mem_buffer.delete_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref())?;
            }
            WalOperation::Update => {
                let payload = deserialize_update_payload(&entry.data)?;
                self.mem_buffer.update_by_sql(&entry.project_id, &entry.table_name, payload.predicate_sql.as_deref(), &payload.assignments)?;
            }
        }
    }

    Ok(RecoveryStats { ... })
}
```

## Safety Features

### Size Limits

```rust
const MAX_BATCH_SIZE: usize = 100 * 1024 * 1024;  // 100MB
```

Prevents unbounded memory allocation from corrupted or malicious WAL data.

### Version Detection

The version byte (currently 130, ≥ 128) is greater than any valid operation byte (0-2), allowing safe format detection. When the on-disk version is older than the build, recovery emits a `warn!` and rejects the entry as `UnsupportedVersion`; operators should wipe `${TIMEFUSION_DATA_DIR}/wal` or roll back the binary.

```rust
fn deserialize_wal_entry(data: &[u8]) -> Result<WalEntry, WalError> {
    if data[0..4] == WAL_MAGIC {
        if data[4] > 2 {
            // New format: version byte + operation byte
            let version = data[4];
            let operation = data[5];
            // ...
        } else {
            // Legacy v0: magic + operation byte only
            let operation = data[4];
            // ...
        }
    } else {
        // Ancient format: no magic header
        // ...
    }
}
```

### Fsync Schedule

```rust
const FSYNC_SCHEDULE_MS: u64 = 200;

Walrus::with_consistency_and_schedule(
    ReadConsistency::StrictlyAtOnce,
    FsyncSchedule::Milliseconds(FSYNC_SCHEDULE_MS)
)?;
```

Balances durability (200ms max data loss window) with performance.

### Corruption Threshold

The `wal_corruption_threshold` config controls failure behavior:
- `0`: Disabled (continue despite corruption)
- `>0`: Fail if error_count >= threshold

### Single-Writer Invariant & Directory Lock

The WAL is **single-writer**. walrus tracks per-topic block/offset state and the read cursor *in-process only* — there is **no cross-process coordination** — and startup recovery is a **one-shot** read of the WAL tail as it stands when recovery begins (`recover_from_wal` runs once; the topic list is frozen when `replay_iter()` is constructed, and replay thereafter resumes from the persisted cursor *forward*, never re-scanning older-than-cursor entries).

If two processes run against the same WAL directory at once — e.g. an old and a new container during an overlapping (start-first) redeploy that share one persistent WAL volume — the WAL **forks**: the newer process recovers only the prefix present at its start, while the older process's concurrent appends *after* that point are never replayed and are then trimmed by the cursor watermark / boot GC. Because those writes were already acked to the client, nothing errors and nothing reaches a dead-letter path — the result is **silent data loss**. (This is the failure mode observed on 2026-07-12.)

To enforce the invariant at the OS level, startup acquires an exclusive advisory `flock` on `${TIMEFUSION_DATA_DIR}/wal/.timefusion_meta/wal.lock` (`WalDirLock`, `src/wal.rs`) **before any WAL access** — boot GC, recovery, or writes — and holds it for the whole process lifetime. A second process blocks (with backoff, logging `WAL dir ... is locked by another TimeFusion process`) until the first exits.

Properties:

- **No stale locks.** The lock lives on the open file descriptor, so the kernel releases it on *any* process death, including SIGKILL/OOM. A force-killed instance leaves nothing to clean up; the next process acquires immediately.
- **Never steals, never times out.** A held lock means a *live* holder; barging in would recreate the two-writer fork. The wait is bounded in practice by the orchestrator's stop-grace SIGKILL, which guarantees a wedged predecessor is eventually killed and the lock freed.
- **GC-safe.** The lock file lives in `.timefusion_meta/`, which `gc_wal_files` skips, so it is never deleted out from under a holder.
- **Single-host only.** `flock` coordinates writers on one host; it does *not* coordinate across hosts on shared network storage. TimeFusion's WAL is a local/host path, which already pins the writer to one host.

**Deploy implications:**

- Keep readiness a **TCP check on the pgwire port**. The early-bind 57P03 responder binds the port *before* the lock is acquired, so readiness is decoupled from lock acquisition. A *start-first* rolling deploy then self-resolves: the new container passes readiness immediately (serving 57P03), the orchestrator stops the old one, the old releases the lock, and the new recovers the **complete** WAL. **Do not** add a deep readiness check that gates on WAL recovery — it would deadlock a start-first deploy (new can't become ready until it holds the lock; the orchestrator won't stop old until new is ready).
- **stop-first, single-replica** deploys are recommended as *hardening* (not required for correctness): they avoid the brief window where both containers are bound and let the old instance finish flushing to Delta before the new one starts, minimizing replay work.

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `TIMEFUSION_DATA_DIR` | `./data` | Base directory containing WAL |
| `TIMEFUSION_BUFFER_RETENTION_MINS` | `70` | Entries older than this are skipped on recovery |
| `TIMEFUSION_WAL_CORRUPTION_THRESHOLD` | `0` | Max errors before failing recovery |

WAL directory: `{TIMEFUSION_DATA_DIR}/wal`

## File Structure

```
data/
└── wal/
    ├── {walrus_topic_key_1}/
    │   └── ... (walrus internal files)
    ├── {walrus_topic_key_2}/
    │   └── ...
    └── .timefusion_meta/
        └── topics  # Line-separated topic names
```

## Performance Characteristics

| Operation | Latency | Notes |
|-----------|---------|-------|
| `append()` | ~1ms | Includes fsync if schedule triggers |
| `append_batch()` | ~1ms total | Amortizes fsync across batches |
| `read_entries_raw()` | O(n) | Reads all entries for topic |
| `checkpoint()` | O(n) | Marks all entries as consumed |

## Best Practices

1. **Use batch append**: Reduces fsync overhead
2. **Set appropriate retention**: Balance recovery time vs. disk usage
3. **Monitor corruption**: Set threshold > 0 in production
4. **Regular checkpointing**: Happens automatically after Delta flush

## Tradeoffs

### Chosen: Topic-per-table

**Pros:**
- Parallel read/write per table
- Independent checkpointing
- Smaller recovery scope per table

**Cons:**
- More files on disk
- Topic discovery overhead on startup

### Chosen: 200ms Fsync Schedule

**Pros:**
- Good balance of durability and performance
- Max 200ms data loss on crash
- Batches multiple writes into single fsync

**Cons:**
- Not immediately durable (fsync not per-write)
- Some data loss possible on crash

### Chosen: CompactBatch (No Schema)

**Pros:**
- Smaller WAL entries
- Schema reconstructed from registry

**Cons:**
- Requires schema registry at recovery time
- Schema changes need careful handling

## Files

| File | Purpose |
|------|---------|
| `src/wal.rs` | WalManager implementation |
| `src/buffered_write_layer.rs` | WAL integration with buffer |
