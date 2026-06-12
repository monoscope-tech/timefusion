# MemBuffer/Delta read-visibility gap — implementation plan (2026-06-12)

## Incident summary

2026-06-11: project 87576849, window 12:23–12:55 UTC showed 2 rows at 13:35,
then 178k rows ~2h later with no backfill. Rows were durably written the whole
time (WAL + Delta) but masked from every query. Trigger chain: deploy SIGTERM →
15s grace abandons 38GB buffer → dirty shutdown → 303s replay → post-replay
drain over budget → flush pipeline backed up ~2h under memory pressure.

## Root cause (from code)

The read path unions MemBuffer + Delta, excluding Delta rows for time ranges
MemBuffer "claims" (src/database.rs:4513-4560). Three facts combine:

1. **Exclusions are whole-bucket.** `get_bucket_ranges()` (src/mem_buffer.rs:1057)
   returns `(bucket_id*dur, (bucket_id+1)*dur)` for every bucket present —
   ignoring the bucket's actual min/max timestamps and actual contents.
2. **Force-flush legitimately puts open-bucket rows in Delta.** Under pressure,
   `force_flush_current_buckets` commits the open bucket's rows to Delta and
   removes them from MemBuffer first; inserts keep repopulating the same
   bucket_id. The *current* bucket is therefore exempt from exclusion
   (comment at database.rs:4525, the 8-of-150 undercount fix).
3. **The exemption ends at seal.** Once that bucket_id is no longer current,
   the full-range exclusion turns on — masking the force-flushed rows in Delta
   while MemBuffer holds only the rows inserted after the force-flush. The
   masked rows stay invisible until the bucket drains. With the flush backlog
   stuck for 2h, that was 2h of invisibility for exactly the highest-volume
   project during the storm.

The "brief overlap" comment assumes bucket-in-MemBuffer ⇒ MemBuffer holds all
rows for that window. Force-flush breaks that invariant by construction; the
backlog just stretched the exposure.

Note: `checkpoint_and_drain` (src/buffered_write_layer.rs:1289) drains
unconditionally even when `advance_by_counts` short-reads, so the WAL-cursor
warning does **not** pin exclusions — it is a separate bug (Phase 3).

## Phase 0 — failing tests first (mandatory bug-fix workflow)

- `force_flushed_bucket_stays_visible_after_seal` (tests/e2e/):
  1. Insert N rows; inject memory pressure so the open bucket force-flushes to
     Delta (rows removed from MemBuffer pre-commit, bucket repopulates).
  2. `env.advance()` past the bucket boundary so the bucket seals.
  3. Stall the flush pipeline (injectable delta-write callback that blocks).
  4. Query the window via pgwire; assert all N rows visible.
  Must FAIL on master: sealed exclusion masks the force-flushed Delta rows.
- `exclusion_never_outlives_drain`: stall flush > threshold of virtual time,
  assert exclusion-age metric (Phase 2) exceeds alarm threshold.

## Phase 1 — surgical fix

- Add `force_flushed: AtomicBool` to `TimeBucket` (src/mem_buffer.rs); set in
  `force_flush_current_buckets` when it commits an open bucket's rows.
- `get_bucket_ranges()` skips flagged buckets — extends the current-bucket
  exemption for the bucket's lifetime. Safe by the existing argument:
  force-flush removes rows from MemBuffer *before* commit, so MemBuffer
  residual and Delta rows are disjoint; the union cannot double-count.
  Remaining double-count window = the normal commit-then-drain seal flush,
  identical to today.
- Tighten remaining exclusions from bucket boundaries to actual
  `[min_timestamp, max_timestamp]` (atomics already maintained, incl. by
  `restore_taken_bucket`'s monotonic widen) — reduces collateral masking for
  sparse buckets.

## Phase 2 — observability / invariant alarm

- Gauge: active exclusion-range count + oldest exclusion age (now − seal
  time). Warn 5 min, page 15 min (slot into existing alerting recipe).
- Counter: `advance_by_counts` short reads per topic (currently warn-only at
  src/wal.rs:675).

## Phase 3 — WAL cursor short read (expected=30, got=0)

- Cause family: count-based (relative) checkpointing drifts — replayed buckets
  carry empty `wal_shard_counts` (replay doesn't call `record_wal_append`),
  and a crash after advance-but-before-snapshot double-counts on next boot →
  over-advance → `got=0` forever after.
- Fix: switch checkpointing to absolute positions. `bucket.wal_positions` is
  already captured per shard and passed to the Delta commit callback as the
  crash watermark — add `advance_to_position` in src/wal.rs and use it in
  `checkpoint_and_drain`; on short read, resync from persisted walrus reader
  position instead of warn-and-abandon.

## Phase 4 — deploy hygiene (ops)

- CapRover stop-grace ≥ shutdown deadline from 5b1ccad (e.g. 90s, not 15s).
- Pre-stop hook hitting a flush/drain admin endpoint so draining starts before
  SIGTERM lands.

## Phase 5 — retire range-exclusion (longer term)

1. **Flushed-file registry (preferred):** `flush_bucket` already receives
   `added_files` from the Delta commit callback. Maintain
   `{bucket_id → committed files}`; read path masks those *files* (not time
   ranges) while the bucket lives; entry removed transactionally in
   `drain_bucket`. Exact by construction — an unflushed bucket masks nothing.
   Persist the map in the shutdown snapshot to cover crash between commit and
   drain.
2. **Read-time dedup fallback:** last-write-wins anti-join on `dedup_keys` in
   the union plan. Fully general but per-query cost; only if (1) insufficient.

## Residual

TF still ~19.8k rows (10%) short of PG for the window — consistent with
buckets still in the backlog tail or the genuinely-failed share. Re-check
before reconciling from PG; the Phase 2 metric answers this directly once live.
