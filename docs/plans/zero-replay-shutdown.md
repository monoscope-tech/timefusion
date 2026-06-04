# Zero-replay shutdown + exact-once WAL recovery

## Goal

Two related invariants we don't hold today:

1. **Every clean redeploy ends with `wal_unflushed_blocks ≈ 0`**, so
   the next container starts in <5s instead of a 20-30 min WAL replay.
2. **The WAL cursor only advances past entries Delta actually has**,
   so a crash doesn't drop unflushed entries (today's bug) or
   re-flush already-flushed ones as duplicates.

## Why this matters

**Observed 2026-06-03 deploy**: 50-min commit gap (17:55→18:45 UTC),
~3 GiB of WAL replayed on one column on cold start. During replay the
pgwire listener doesn't bind (`main.rs:125` awaits `recover_from_wal()`
before spawning `pg_task` at `:149`), so **all writes get
connection-refused for the duration**. Monoscope's dual-write path
counts that as data loss on the TF side.

**Latent data-loss bug**: `wal.checkpoint(project, table)` drains each
shard's walrus cursor to the *tail* of the column, not to the
flushed-bucket's tail. The tail may contain entries for the open
(still-accumulating) bucket B′. Walrus' `StrictlyAtOnce` mode fsyncs
the cursor on every read, so by the time `checkpoint` returns the
cursor has been advanced past B′ — even though B′ hasn't been flushed
to Delta. A crash anywhere from now until B′'s eventual flush loses
those rows: cursor says "consumed", Delta doesn't have them,
MemBuffer was holding them in volatile memory. The exposure window is
small (between any flush and the next bucket-roll) and dual-write to
Postgres has masked it. It's still real, and worsens with
per-column throughput.

## Root causes

### A1: pgwire keeps accepting writes during shutdown

`pg_task` (`main.rs:149`) runs `serve_with_handlers`, which is an
infinite `loop { listener.accept() }` (`vendor/datafusion-postgres/src/lib.rs:160`).
The `tokio::select!` at `:221` waits for SIGTERM but **never tells
pgwire to stop accepting**. New INSERTs keep landing in MemBuffer +
WAL throughout the gRPC drain and the BufferedWriteLayer flush — so
"flush everything" is a moving target and the WAL keeps growing.

### A2: shutdown timeout default is 5s

`timefusion_shutdown_timeout_secs` (`config.rs:108`) defaults to 5s
and is overloaded as both the gRPC drain deadline (`main.rs:238`) and
the BufferedWriteLayer background-task wait (`config.rs:447`). 5s is
below realistic flush time for any non-trivial MemBuffer.

### A3: BufferedWriteLayer's `compute_shutdown_timeout` adds a stale
heuristic (`memory_mb/100`) that hasn't been calibrated against real
flush throughput and is capped at 300s, often below realistic flush
time for 5 GiB+ buffers.

### A4: Docker `StopGracePeriod` likely below the actual flush time,
so Docker sends SIGKILL before shutdown completes. (Inferred — needs
verification on the captain host.)

### B1: `wal.checkpoint` advances cursor to walrus tail

(`wal.rs:533`) — instead of advancing exactly to the flushed bucket's
tail. Drops unflushed entries for the open bucket on crash. This is
the silent data-loss path.

### B2: Crash mid-flush produces duplicates in Delta

The flush sequence is `delta_callback.await → cursor.advance`. If we
crash between those two, Delta has the rows but walrus' cursor is
stale. Restart replays them → next flush writes them again →
duplicates. Today's design is "at-least-once" — exact-once needs the
watermark to live in Delta itself.

## Design

### Step 1 — Stop pgwire from accepting writes on shutdown

Plumb a `CancellationToken` through `serve_with_logging` and the vendor
`serve_with_handlers`. Replace the `loop { accept() }` with:

```rust
loop {
    tokio::select! {
        _ = shutdown.cancelled() => {
            info!("PGWire: shutdown signal, stopping accept loop");
            break;
        }
        accept_result = listener.accept() => { /* existing handler */ }
    }
}
```

In-flight connections keep going on their spawned tasks; the listener
just stops minting new ones. Existing tasks drain naturally (queries
finish, clients disconnect on the next read).

Wire it up in `main.rs`:

```rust
let pgwire_shutdown = CancellationToken::new();
let pgwire_shutdown_for_task = pgwire_shutdown.clone();
let pg_task = tokio::spawn(async move {
    serve_with_logging(..., pgwire_shutdown_for_task).await
});

// In the drain phase, BEFORE gRPC cancel:
pgwire_shutdown.cancel();
let _ = tokio::time::timeout(
    Duration::from_secs(cfg.buffer.timefusion_shutdown_timeout_secs),
    &mut pg_task,
).await;
```

Vendor change goes in `vendor/datafusion-postgres/src/lib.rs` — same
file we patched for the DML response fix. New parameter on
`serve_with_handlers(handlers, opts, shutdown: CancellationToken)`.
The two other `serve*` wrappers in that file take the same shutdown
arg; pass `CancellationToken::new()` (never fired) from anywhere that
doesn't care.

### Step 2 — One shutdown budget, raised default

Keep the existing `TIMEFUSION_SHUTDOWN_TIMEOUT_SECS` but treat it as
the *per-phase* ceiling for each serial shutdown phase (pgwire drain,
gRPC drain, flush). Raise the default from **5s → 180s**.

Drop the `memory_mb/100` adjustment in `compute_shutdown_timeout`
(`config.rs:447`); a single number an operator can reason about beats
a hidden formula.

The phases are serial, so worst-case total is `3 × budget = 540s`.
That's fine — Docker's `StopGracePeriod` (Step 3) caps it externally,
and 99% of clean shutdowns finish well before any single phase hits
its ceiling.

### Step 3 — Verify and bump Docker / CapRover stop grace period

On the captain host:

```bash
docker service inspect timefusion --format '{{.Spec.TaskTemplate.ContainerSpec.StopGracePeriod}}'
```

If it's <`3 × TIMEFUSION_SHUTDOWN_TIMEOUT_SECS` (= 540s at the new
default), bump it to that or higher. Per
[[infra_caprover_appdefinitions_full_replace]] this must go through
the CapRover API (not `docker service update`) to avoid wiping the
envVars/volumes config.

### Step 4 — Cursor advances only to bucket's recorded WAL position

(Fixes B1.)

When a bucket B is sealed for flush, snapshot the *current* walrus
position per shard:

```rust
let positions: Vec<(usize, BlockPos)> = (0..shards_per_topic)
    .map(|s| (s, walrus.current_position(&shard_key(s))))
    .collect();
bucket.wal_positions = positions;
```

After `flush_bucket(B)` succeeds, advance the cursor to *exactly*
those positions (not the tail):

```rust
for (shard, pos) in &bucket.wal_positions {
    walrus.set_persisted_read_position(&shard_key(*shard), *pos);
}
```

Walrus' `StrictlyAtOnce` still fsyncs the offset before returning —
the difference is *where* it lands. `wal.checkpoint`'s drain-to-tail
becomes obsolete; delete it.

Walrus API needed:
- `current_position(col) -> BlockPos` (reads `tail_block_id`, `tail_offset`).
- `set_persisted_read_position(col, pos) -> io::Result<()>` (writes
  the offset index entry directly, no `read_next` walk).

Both are private state today (`walrus_read.rs`); upstream a small API
addition, or vendor walrus short-term.

### Step 5 — Watermark in Delta commit metadata (Delta-derived cursor)

(Fixes B2.)

The flush sequence is `delta_callback.await → walrus.advance_cursor()`.
Crash between those two: Delta has commit N, walrus cursor is stale,
restart replays N's rows → duplicates.

Fix: write the upstream watermark *into* the downstream durability
record. Each Delta commit includes per-shard `(block_id, offset)`
snapshotted at flush time, recorded in commit-level metadata via
delta-rs' `commit_info`:

```rust
let metadata = json!({
    "timefusion": {
        "wal_watermark": bucket.wal_positions
            .iter().map(|(s, p)| (s, p.block_id, p.offset)).collect::<Vec<_>>(),
        "bucket_id": bucket.bucket_id,
    }
});
delta.commit_with_metadata(added_files, metadata).await?;
```

On startup, derive the cursor from Delta first:

```rust
for (project, table) in known_tables() {
    let latest = read_latest_delta_commit_metadata(project, table).await?;
    let delta_watermark = latest.timefusion.wal_watermark;
    for (shard, block_id, offset) in delta_watermark {
        let local = walrus.persisted_position(&shard_key(shard));
        let cursor = max(local, BlockPos { block_id, offset });
        walrus.set_persisted_read_position(&shard_key(shard), cursor);
    }
}
```

This makes Delta the source of truth, the way Postgres' `pg_control.redo`
is sourced from the same fsync that recorded the last applied data.

Cost: one S3 GET per table at startup (already paid for schema), one
extra JSON map per Delta commit (negligible).

### Step 6 — Consumption-based WAL retention

With Step 4 in place, sealed walrus blocks whose `block.end_offset <
min(cursor)` are reclaimable. RocksDB-style: WAL self-sizes to the
in-flight window. `timefusion_buffer_retention_mins` becomes a safety
floor (keep ≥ N min regardless), not the primary reclamation lever.
Kills the "853 retained blocks" overhang we saw on 2026-06-03.

## Recovery semantics under Steps 4 + 5

| Scenario | What's durable | Cursor after restart | Replay re-injects | Result |
|---|---|---|---|---|
| Clean shutdown | All buckets in Delta | At each bucket's recorded tail | nothing | Fast cold start |
| Crash between flushes | Last flushed bucket only | At last bucket's recorded tail | Open bucket B′'s entries | Reflushed cleanly. No loss. No dupes. |
| Crash mid-flush, Delta commit landed | B's rows in Delta, metadata has B's watermark | Derived from Delta = B's watermark | Nothing past B | No loss. No dupes. |
| Crash mid-flush, Delta commit didn't land | B's rows not in Delta, metadata has last successful commit's watermark | At pre-B watermark | B's entries | Reflushed cleanly. No loss. No dupes. |
| Host loss + walrus directory wipe | Whatever's in Delta | Reset to whatever Delta says | Nothing | RPO = last successful Delta commit. |

**Invariant**: the cursor is the max of *(locally fsynced walrus
cursor, latest Delta commit metadata's watermark)*. Either is safe to
advance from; taking the max means a host that lost walrus state
still recovers correctly from Delta, and a Delta commit that landed
but didn't fsync locally still gets honoured.

## How real DBs do this

| System | Watermark | Where it lives | Recovery reads it from |
|---|---|---|---|
| Postgres | `redo` LSN | `pg_control` (fsynced atomically) | `pg_control` |
| RocksDB | min(seq across CFs) | MANIFEST | MANIFEST |
| Kafka | committed offset | `__consumer_offsets` topic | broker |
| **TF (Step 5)** | (block_id, offset) per shard | Delta commit metadata | latest `_delta_log/N.json` |

The pattern they all share: the watermark lives next to the data it
bounds, and only advances when the data is durable on the downstream
side. TF today violates both halves; Steps 4 + 5 fix both.

## Migration

Existing deployments don't have the Delta commit metadata field.
First restart after Step 5 lands sees `wal_watermark = None` for all
tables — fall back to walrus' persisted cursor (today's behaviour).
The first new flush after the upgrade writes a watermark; from then
on every restart uses Delta-derived cursors.

No backfill needed. No data migration. Old commits stay valid.

## Tests

Shutdown:

- `shutdown_drains_inflight_pgwire`: connect a client, START INSERT,
  cancel shutdown token, assert the insert completes successfully
  (no abort/disconnect mid-statement) and no new connections accepted.
- `shutdown_flushes_walmembuffer_to_delta`: write N rows via gRPC,
  trigger shutdown, restart with a fresh `Walrus`, assert
  `recover_from_wal()` returns `entries_replayed == 0` and
  Delta `count(*) == N`.

Watermark:

- `bucket_seal_snapshots_walrus_position`: insert N rows, seal the
  bucket, assert recorded positions == walrus tail at that instant
  (not the tail after later inserts).
- `cursor_advances_to_bucket_position_not_tail`: insert into bucket B,
  insert into open bucket B′, flush B, assert cursor sits at B's
  position (B′'s entries still unconsumed).
- `recovery_from_delta_watermark`: flush a bucket, wipe walrus
  directory, restart. Cursor derived from Delta; replay re-injects
  nothing.
- `recovery_replays_inflight_after_crash`: write rows to MemBuffer +
  walrus without flushing, simulate crash, restart. Replay re-injects;
  next flush commits them.
- `no_duplicates_on_crash_mid_flush`: stub Delta callback to succeed
  but inject panic before cursor advance; restart; assert Delta has no
  duplicate rows.

## Metrics

Per [[infra_otel_metrics_pattern]]:

- `timefusion.wal.chain_len{column}` — total sealed blocks retained.
- `timefusion.wal.cursor_lag_blocks{shard}` — `tail_block_id - cursor.block_id`.
- `timefusion.shutdown.flush_duration_seconds` (histogram).
- `timefusion.recovery.delta_derived_cursor_used` (counter) — should be
  0 on clean shutdown, >0 on crash recovery.
- `timefusion.recovery.entries_replayed_already_in_delta` (counter) —
  should be 0 after Steps 4+5. If nonzero, the watermark logic
  regressed.

## Sequencing

| Step | Lands | Reason |
|---|---|---|
| 1 — pgwire stop-accept | First PR | Highest impact, smallest blast radius. Closes the deploy-day gap. |
| 2 — timeout default + cleanup | Same PR as 1 | Config-only, low risk. |
| 3 — Docker StopGracePeriod | Ops change | No code; verify after PR 1 deploys. |
| 4 — cursor-to-bucket-position | Second PR | Touches durability contract; deserves its own review. Fixes the silent data-loss path. |
| 5 — Delta-derived cursor | Third PR | Builds on 4. Closes the duplicate-on-mid-flush-crash. |
| 6 — consumption-based retention | Fourth PR | Disk savings + simplification. Last because least risky. |

## Validation per step

After each deploy:

1. Trigger a redeploy.
2. Check Delta `_delta_log/` — pre- and post-redeploy commit
   timestamps should be within `TIMEFUSION_SHUTDOWN_TIMEOUT_SECS` of
   each other.
3. New container's startup log: `WAL recovery complete: <N> entries
   replayed in <ms>ms`. Target `<N> == 0` and `<ms> < 5000` after
   Step 1; `<N> == 0` guaranteed even on crash after Steps 4+5.
4. `timefusion.recovery.entries_replayed_already_in_delta` (Step 5) ==
   0.

## Out of scope

- `--no-replay` / read-only mode against Delta only. Useful for
  inspection during deep replay, but separate; this plan removes the
  need.
- Idempotent Delta MERGE / per-batch IDs. Steps 4+5 give exact-once
  without changing the Delta write path; MERGE is a bigger change with
  no remaining benefit.
- WAL compaction beyond Step 6.
- Replicated WAL / multi-region durability.
