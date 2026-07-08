# WAL durability fixes: stop losing acked writes (2026-07-08)

## Incident

After a Kafka re-drive, TF's 13:10–14:29 window held only 75.9% of PG's rows
(~384k missing) and the re-drive did not backfill the hole. Investigation
found a self-sustaining loss loop, live in prod today:

1. `recover_from_wal` replays with `cutoff = now − retention_mins (70min)`
   and `checkpoint=true`. Pre-cutoff entries are **read, checkpoint-consumed,
   and dropped** (`wal.rs::next_eligible_from_shard`, the `Ok(_) => continue`
   arm). The cursor advances past them — permanent loss. The comment at
   `recover_from_wal`'s parking step documents this as deliberate ("lets
   bloated WAL backlogs GC after replay"), on the assumption *older than
   70min ⇒ already flushed*. That assumption is false whenever flush falls
   behind (crash loop, S3 wedge, backlog drain).
2. Replay loads the whole un-flushed backlog into MemBuffer **bypassing the
   memory budget** (prod 2026-07-08 14:30 boot: 15.4GB loaded vs 7.3GB
   budget), the process OOMs (exit 137), reboots, replays again. Each loop
   the oldest un-flushed data ages past 70min and is discarded by (1).
3. `gc_wal_files` deletes WAL files purely by mtime (`> retention+20min`),
   ignoring cursors — the same unsound "old = flushed" assumption at the
   file level, including a pre-walrus boot GC pass.

Note: walrus is mmap-backed, so an OOM SIGKILL does **not** lose appended
bytes (page cache survives process death). The 200ms deferred fsync only
matters for machine/power crashes — it is NOT the incident mechanism.

## Fixes

### A — replay boundary = persisted cursor (loss-stopper)

The persisted per-shard read cursor already marks the Delta-durable boundary
precisely: `derive_wal_cursors_from_delta` advances it to the committed Delta
watermark at boot, and `advance_wal_watermark` maintains it after every
flush. Everything at/after the cursor is un-flushed (or in the crash-mid-
flush re-replay overlap, which dedup collapses). The 70-min wall-clock
cutoff is therefore redundant where it's sound and lossy where it isn't.

- Remove the age cutoff from replay: `recover_from_wal` consumes ALL entries
  from the cursor. Delete the `since_timestamp_micros` filtering from the
  replay path (`for_each_entry` has exactly one caller).
- `reap_expired_empty_buckets` stays: it releases holds of DML-emptied
  shells, which is hold-based (consistent with the cursor model), and
  skipping an insert+delete pair that nets to zero rows is semantically
  exact (a crash before the DML's Delta leg re-replays both).
- WAL-bloat trade: an un-flushable topic now pins its cursor and the WAL
  grows instead of data being dropped at 70min. That is the correct
  direction; `flush_failed_total` alerting covers the operational case, and
  D keeps GC from making it worse.

### B — budget-bounded replay (breaks the OOM loop)

- Convert `for_each_entry`'s per-topic k-way merge into a pull-based
  `WalReplayIter` (same order, same checkpoint semantics, error counter on
  the iterator). `for_each_entry` is deleted (single caller).
- `recover_from_wal` loops the iterator; whenever `is_memory_pressure()` it
  awaits `relieve_memory_pressure()` (flush completed buckets, then
  force-flush open ones) before continuing. Boot takes longer on a big
  backlog; memory never exceeds the budget.
- **Relief backoff:** if a relief attempt leaves pressure standing (S3 down
  ⇒ flushes failing), don't re-attempt per entry — wait ~200 entries before
  the next attempt. Worst case degrades to today's behavior (replay loads
  over budget) instead of spinning the boot forever on a dead S3.
- **P0 replay guard (soundness):** a mid-replay flush can empty a topic's
  MemBuffer holds; `compute_wal_watermark` would then fall back to the WAL
  write tail — and (worse than a benign cursor advance) the Delta commit
  records that tail as its watermark metadata, so a LATER boot's
  `derive_wal_cursors_from_delta` would skip entries the commit never
  contained → loss. Guard: at replay start, register each topic's
  pre-recovery position P0 as an inflight hold (`register_inflight_holds`);
  release after the post-replay parking. All mid-replay watermarks are then
  floored at P0 — conservative (re-replay + dedup), never lossy.
- Crash mid-replay remains covered by the existing rewind marker (rewinds
  to P0; rows flushed mid-replay are re-replayed and deduped).

### C — opt-in fsync-before-ack (machine-crash durability)

Code reading found the gap is smaller than assumed: walrus's `batch_write`
(TF's INSERT path via `append_batch`) already `mmap.flush()`es every touched
file **before returning** — batched inserts are durable before ack even in
the default 200ms mode. Only single-entry `write()` (TF's `locked_append`:
DELETE/UPDATE/UPDATE...FROM entries) defers to the background fsync thread,
and sealing flushes prior blocks, so the un-synced surface is exactly the
current block of a DML append.

- Vendored walrus: `Writer::sync()` (flush the active block; sealed blocks
  were flushed at seal) + `Walrus::sync_topic(col)`.
- TF: `TIMEFUSION_WAL_ACK_FSYNC=true` (default **false**) makes
  `locked_append` sync its shard before returning — DML acks become
  machine-crash durable. `FsyncSchedule::SyncEach` remains the blunt
  every-write alternative.
- Default stays deferred: the incident is OOM (process death), which mmap
  already survives; power-loss durability is opt-in until soaked.

### D — durability-aware WAL GC

Never delete a file that may hold un-consumed entries:

- Track the oldest un-flushed WAL **append** time: `TimeBucket` gains
  `min_wal_append_micros`, stamped on every insert as
  `min(bucketing_ts_param, chrono::Utc::now())` — no signature churn. On
  replay the param IS the append time (exact); on live inserts the param is
  event time ≤ append time (conservative) and the `Utc::now()` min-clamp
  covers future-skewed client timestamps. Real-clock domain throughout,
  matching file mtimes. Inflight/orphaned flush holds carry the same min
  alongside their `ShardHolds` (merged in `CombinedBucket::absorb`).
  `oldest_unflushed_wal_append_micros()` = min over all.
- `gc_wal_files(dir, max_age, floor)`: delete only files with
  `mtime < min(now − max_age, floor − SLACK)` (SLACK = 10min, covers the
  append→bucket-record window; entries in that window have mtime ≈ now
  anyway). No un-flushed data ⇒ floor = ∞ (pure mtime, current behavior).
- Boot (pre-walrus) GC runs **only when `cursor_snapshot.json` says
  `clean_shutdown=true`** (graceful shutdown drained everything ⇒ mtime GC
  sound). Dirty boot: skip; the runtime sweep (immediate first pass, now
  floor-aware, running after replay parks the cursors) reclaims instead.
- The mtime heuristic direction is safe against shared block files: a file
  is touched by any topic writing into it, so recency keeps it alive.

### E — memory budget coherence guard

`autotune::apply` already derives query pool / buffer / foyer from the
cgroup limit with a documented ≈72% invariant, and
`sort_spill_reservation_bytes` is plumbed. The remaining gap is user-pinned
envs oversubscribing RAM (prod ran a hand-set combination for months).
Add a boot-time check in autotune: compute the final (post-override) sum of
query pool + buffer + foyer(mem+meta) and WARN loudly when it exceeds 85%
of detected RAM, naming the knobs. No behavior change — operators keep
authority; the failure mode becomes visible instead of an OOM surprise.

## Failing-tests-first (mandatory workflow)

1. **A / e2e**: un-ignore `unflushed_rows_replayed_from_wal` — its recorded
   symptom ("second-pass read returns zero entries though the WAL holds
   them") is this exact bug: the frozen virtual clock (~2030) puts the
   cutoff ahead of the real-clock WAL stamps (2026), so replay
   drops-and-consumes everything. Verify it fails on master, passes with A.
2. **A / unit** `wal_replay_restores_entries_older_than_retention`: freeze
   `crate::clock` 2h ahead of real time, insert via layer, recover in a new
   layer, assert rows restored (fails on master).
3. **B / unit** `wal_replay_drains_to_budget`: tiny budget, >budget of WAL
   entries, recover with a capturing Delta callback; assert flushes happened
   during replay, post-replay memory ≤ budget, and every commit's watermark
   metadata ≤ P0 (guards the derive-skip bug).
4. **C / unit**: ack-fsync mode appends + replays correctly; walrus
   `sync_now` drains dirty paths.
5. **D / unit**: `gc_wal_files` floor keeps an old-mtime file alive when
   un-flushed data predates it; boot GC no-ops on a dirty snapshot.
6. **E / unit**: oversubscribed pinned envs produce the warning path
   (assert on the computed decision, not log text).

## Risks / accepted trade-offs

- First boot after deploy replays the true backlog with no age cap. Replay
  reads only from the cursor (walrus skips consumed blocks), so size =
  actual un-flushed backlog, not the 59GB dir. B's draining makes it long
  but bounded. `FATAL: starting up` window may lengthen on a big backlog.
- Mid-replay flushes commit to Delta with replay pinned at P0 → next boot
  re-replays those entries → duplicates → write-side dedup_keys + read-side
  DedupExec collapse them (existing, tested behavior).
- Un-flushable data now pins WAL forever (alerting on flush_failed /
  wal.disk_bytes is the escape hatch, plus existing quarantine for
  poison entries).
- **These fixes do not resurrect the already-lost rows**: past boots
  checkpoint-consumed them, so the cursor is beyond them. The 13:10–14:29
  hole (and the 455k `rows_in_buffer_lag` drift) must be repaired by a
  monoscope re-drive run AFTER this deploys — with A+B in place the re-driven
  rows will actually survive to Delta.
