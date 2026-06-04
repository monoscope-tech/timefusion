# Steps 5 + 6 — Delta-derived cursor & consumption-based WAL retention

Follow-on to `zero-replay-shutdown.md`. Step 4 (per-shard count snapshot
+ `advance_by_counts`) is already on the working tree; this plan
assumes it's landed.

## Step 5 — Delta-derived cursor (exact-once across crash-mid-flush)

### What the plan needs to prove

Today's flush sequence is:

```
seal bucket → snapshot wal_shard_counts → delta_callback.await
  → advance_by_counts (fsync cursor forward)
```

If we crash between `delta_callback.await` (Delta has the rows) and
`advance_by_counts` (cursor hasn't moved), restart replays the same
WAL entries → next flush writes them to Delta a second time. At-least-
once, not exact-once.

Fix: write the *post-flush cursor target* into the Delta commit
itself, atomic with the data. On restart, derive the cursor from the
latest Delta commit metadata and fast-forward walrus to whichever is
ahead (local fsynced cursor or Delta-recorded watermark).

### Design choices to nail before coding

1. **Watermark representation.** Per-shard `(block_id, offset)` —
   walrus's native cursor. Map `shard -> (block_id, offset)`. JSON in
   commit-info `userMetadata`. Bounded size (~`shards_per_topic`
   entries per commit).

2. **Snapshot timing.** At seal time, snapshot
   `walrus.current_position(walrus_key_for_shard)` per shard alongside
   `wal_shard_counts`. Two coupled invariants:

   - `wal_shard_counts[s]` drives the advance (already there).
   - `wal_positions[s]` is the absolute position the cursor will sit
     at after `advance_by_counts` returns.

   Per-`(project, table)` topics never interleave across shards
   (walrus key includes both), so snapshot-at-seal-time =
   pre-flush-cursor + count exactly. No virtual arithmetic.

3. **Write-order invariant.** The Delta commit *must* contain the
   watermark for the rows in that commit. If we wrote the watermark in
   a separate commit after `advance_by_counts`, the crash window
   reopens. Single commit, single metadata blob.

4. **Read-time invariant.** On startup, for each known
   `(project, table)`:

   ```
   for shard in 0..shards_per_topic:
       local  = walrus.persisted_position(key_for_shard)
       delta  = latest_delta_watermark.get(shard)
       cursor = max(local, delta)
       walrus.set_persisted_read_position(key_for_shard, cursor)
   ```

   Max-of-two means hosts that lost walrus state (host loss, disk
   wipe) recover from Delta, and hosts whose Delta commit didn't land
   still honour their locally-fsynced cursor.

### Walrus API gaps

Step 4 used the existing `read_next(key, persist=true)` loop. Step 5
needs two new calls walrus doesn't expose today:

- `current_position(key) -> BlockPos` — reads `tail_block_id`,
  `tail_offset` without consuming.
- `set_persisted_read_position(key, BlockPos) -> io::Result<()>` —
  writes the persisted-read offset index directly, no `read_next`
  walk.

Decision needed: upstream a small API addition vs. vendor walrus
short-term (we already vendor `datafusion-postgres`; precedent
exists). **Recommend: vendor short-term**, mirror the upstream PR.
Walrus's internal state is in `walrus_read.rs` — the two functions
are 5-10 lines each over the existing `OffsetIndex` machinery.

If vendoring is unacceptable, fall back to:

- `current_position`: append a sentinel? No — pollutes the WAL.
  Better: track positions in TimeFusion by counting our own appends
  per shard from a known reset point. Already half-implemented (the
  `wal_shard_counts` accumulator). Add a per-shard
  `cumulative_position` that updates on each append (block-id +
  offset returned by `append_for_topic`). Walrus already returns
  offsets internally; expose via a thin trait impl on
  `WalManager`. Pragmatically cleaner than vendoring.
- `set_persisted_read_position`: drop, keep `advance_by_counts`-only.
  This means we can't *fast-forward* from Delta watermark on a
  cold-walrus host. We can still detect the gap and refuse to start
  (loud failure beats silent loss). Reduced functionality, smaller
  blast radius.

**Recommend path A** (vendor + true `set_persisted_read_position`):
the host-loss recovery case is the highest-value scenario and
deserves a proper fast-forward.

### Code surface

| File | Change |
|---|---|
| `vendor/walrus/...` (new) | Vendor walrus; add `current_position` + `set_persisted_read_position` on its main reader/writer types. Patch is small (~30 lines + tests). |
| `src/wal.rs` | New `WalManager::current_position(project, table) -> Vec<BlockPos>` (per-shard) and `set_persisted_positions(project, table, &[BlockPos])`. Thin pass-through. |
| `src/mem_buffer.rs` | `TimeBucket` grows `wal_positions: Mutex<Vec<Option<BlockPos>>>`. `record_wal_append` takes `BlockPos` and stores it (first-write-wins per shard at seal time — actually: capture pre-append position, and the *post-flush position* = post-append position of the last batch in the bucket on that shard). Sealed `FlushableBucket` carries `wal_positions: Vec<BlockPos>`. |
| `src/buffered_write_layer.rs` | `insert` queries `wal.current_position` per shard *after* `append_batch` returns, calls `mem_buffer.record_wal_append` with the new position. Flush callback signature gains `wal_watermark: &[(usize, BlockPos)]`. `checkpoint_and_drain` is unchanged (still uses counts). |
| `src/database.rs` (delta callback owner) | Delta-rs commit uses `commit_with_metadata` (delta-rs API: `CommitBuilder::with_metadata` or equivalent). Serialize watermark as JSON: `{"timefusion": {"wal_watermark": [[shard, block, offset], ...], "bucket_id": N}}`. |
| `src/main.rs` / startup path | New `derive_cursor_from_delta(project, table)` runs *before* `recover_from_wal`. Reads latest commit metadata per known table (one S3 GET each — schema load already pays this), reconciles via max, calls `wal.set_persisted_positions`. |
| `src/config.rs` | None expected. |

### Migration

First boot after Step 5 lands sees `wal_watermark = None` in every
existing Delta commit. Behaviour: fall through to walrus persisted
cursor (today's path). First new flush after upgrade writes the
field; every subsequent restart uses Delta-derived cursors. No
backfill, no data migration, old commits stay valid.

### Tests

In rough priority order:

1. **`bucket_seal_snapshots_walrus_position`** — insert N rows on
   shard `s`, seal, assert `bucket.wal_positions[s] ==
   walrus.current_position(key_for(s))` at the instant of sealing
   (not after later inserts).
2. **`delta_commit_carries_watermark`** — stub Delta callback,
   capture the metadata blob, assert it contains expected
   per-shard positions. Doesn't need real S3.
3. **`recovery_uses_delta_watermark_when_ahead`** — flush a bucket,
   wipe walrus directory, restart. Cursor derived from Delta;
   `recover_from_wal()` returns `entries_replayed == 0`.
4. **`recovery_uses_local_when_ahead`** — flush succeeds locally
   (cursor advanced) but Delta metadata is stubbed older. Restart;
   cursor stays at local position; no replay.
5. **`no_duplicates_on_crash_mid_flush`** — stub Delta callback to
   succeed-then-inject-panic before `advance_by_counts`. Restart;
   `derive_cursor_from_delta` advances cursor to Delta's recorded
   watermark; replay is empty; final `count(*)` in Delta has no
   duplicates.
6. **`migration_no_watermark_falls_back`** — Delta commit without
   the `timefusion` metadata field; restart uses walrus persisted
   cursor unchanged.

### Risk register

- **delta-rs commit_info API surface**: needs to be a per-commit
  user-metadata blob, not table properties. Verify
  `CommitBuilder::with_metadata` lands in the same atomic write as
  the data files. If the metadata write is a separate transaction,
  the whole design collapses — re-check before coding.
- **Walrus state for tables not in known_tables on startup**: the
  topic-discovery file (`.timefusion_meta/topics`) is the union of
  all topics ever appended. If a custom-project table is gone but
  its WAL topic remains, derive_cursor_from_delta has no Delta to
  read. Skip silently and keep walrus state.
- **Watermark JSON growth**: with `shards_per_topic=4` (default),
  ~50 bytes per commit. Trivial. Re-evaluate if shards goes to 64+.

---

## Step 6 — Consumption-based WAL retention

### What changes

Today `timefusion_buffer_retention_mins` (default 70min) is the
primary reclamation lever. Step 4 lets walrus know exactly how far
back the consumed cursor is per shard. Step 6 makes walrus's
block-reclaim driven by *consumption*, with retention as a safety
floor.

### Design

- Walrus already segments its log into blocks. A block is fully
  consumed when `block.end_offset <= min_persisted_read_cursor` for
  that topic. (Single-consumer model — TimeFusion is the only
  reader.)
- Add walrus API: `reclaim_consumed_blocks(key, min_age: Duration)
  -> Result<usize>`. Drops blocks whose entire offset range is
  behind the persisted cursor *and* whose sealed-at timestamp is
  older than `min_age`. Returns count reclaimed.
- Background task in `BufferedWriteLayer` (alongside flush and
  eviction tasks): every 60s, walk known topics × shards, call
  `reclaim_consumed_blocks(key, retention_mins)`.
- `timefusion_buffer_retention_mins` keeps its current name; its
  meaning shifts from "evict from MemBuffer after N min" to
  "evict MemBuffer + retain WAL blocks ≥ N min even if consumed".

### Why retention stays as a floor, not zero

Two reasons:

1. **Manual replay / debugging.** Operators sometimes need to
   re-ingest the last hour from WAL after a Delta-side mistake
   (bad schema migration, wrong project-routing rule). Retention
   floor preserves that window.
2. **Recovery from corrupted Delta commit.** If a Delta commit is
   later discovered corrupt and rolled back, the WAL entries it
   referenced are the only source. Floor must exceed the corruption
   detection lag (typically minutes, sometimes hours).

Default unchanged (70min). Operators can lower for disk-bound
deployments.

### Code surface

| File | Change |
|---|---|
| `vendor/walrus/...` | Add `reclaim_consumed_blocks`. Inspects the offset index, finds first block whose `start_offset > persisted_cursor.offset`, deletes blocks strictly before it that are also older than `min_age`. |
| `src/wal.rs` | `WalManager::reclaim_consumed(project, table, min_age)` — iterate shards, pass through. |
| `src/buffered_write_layer.rs` | New `run_retention_task(retention_mins)` loop alongside the existing flush/eviction tasks. Hooked up in `start_background_tasks`. |
| `src/config.rs` | None; reuse `timefusion_buffer_retention_mins`. |

### Tests

1. **`reclaim_drops_blocks_behind_cursor`** — append M batches
   across 3 walrus blocks, advance cursor past block 1's end,
   call `reclaim_consumed`. Assert block 1's file is gone, blocks
   2 and 3 remain.
2. **`reclaim_respects_min_age_floor`** — same setup but block 1
   was sealed 10s ago and `min_age = 1h`. Assert nothing reclaimed.
3. **`reclaim_with_zero_cursor_advances_is_noop`** — fresh topic,
   nothing flushed yet, cursor at origin. Reclaim returns 0.
4. **`retention_task_runs_periodically`** — set retention to 60s,
   sleep > 60s, assert block count dropped (or use a faked clock).

### Metrics

Per `[[infra_otel_metrics_pattern]]`:

- `timefusion.wal.reclaimed_blocks_total{project, table}` —
  counter, incremented per reclaim.
- `timefusion.wal.retained_blocks{project, table}` — gauge,
  current block count after reclaim. Should hover near
  `retention_mins / block_seal_interval` in steady state.

### Risk register

- **Walrus block-deletion ordering**: must fsync the offset-index
  update *before* unlinking block files. Otherwise crash mid-reclaim
  leaves dangling index pointing to missing blocks → next read
  fails. Confirm walrus does this; add a test if not.
- **Multi-tenant cursor coupling**: `min_persisted_read_cursor` is
  per-(project, table)-per-shard, not global. Verify walrus keys
  scope correctly so reclaim on table A's shard doesn't affect
  table B sharing nothing.

---

## Sequencing recommendation

5 first, alone, behind a "watermark-write enabled but watermark-read
gated by env var" feature flag for one deploy cycle. Verify
`timefusion.recovery.delta_derived_cursor_used` == 0 on clean
shutdown and goes positive only when we deliberately wipe walrus state
in staging. Then flip the read path on in prod.

6 after, independently. No coupling. Worth its own deploy because
disk reclamation has historically surprised us (the 853-block
overhang on 2026-06-03 was the symptom that motivated this).

## Open questions (need answers before coding)

1. Does delta-rs `CommitBuilder::with_metadata` write atomically with
   the data files? (If no, Step 5 collapses.)
2. Vendor walrus or upstream the two new APIs? Vendor is faster but
   we'll need to rebase walrus updates by hand.
3. Should `derive_cursor_from_delta` be gated by an env var for the
   first one or two deploys, or just ship it?
