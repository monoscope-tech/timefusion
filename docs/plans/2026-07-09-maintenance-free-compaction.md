# Maintenance-free compaction — event-driven design

**Goal:** a partition that fragments (incident relief-drains, backfill, DLQ replay)
compacts itself within seconds, with no human running `optimize` and no reliance on
the calendar. Fragmentation must never again pile up until dedup blows its budget.

## Root cause (unchanged — see git history / incident 2026-07-08)

`dedup_rewrite_chunk` (database.rs:4368) re-reads the **full row sets of every file
touching a 10-min chunk** and materializes them into Arrow memory unbounded by the
DataFusion pool. When a partition has thousands of tiny files (9,486 on 2026-07-08),
one chunk's rows scatter across ~all of them → the materialization exceeds the
2 GB/4 GB dedup budget → dedup skips → duplicates persist. Compaction (streaming
bin-pack, no budget) shrinks the file set so dedup fits. **Bounded file counts are
the whole game.** Today's four compaction jobs are all calendar-scoped (light=today,
optimize=48 h, consolidate=daily, recompress=daily) — none reacts to *fragmentation*,
so a burst on a past-day partition waits up to 24 h.

## Design: event-driven fragmentation compaction

The flush path already knows exactly what it wrote. `DeltaWriteCallback`
(buffered_write_layer.rs:213) returns the **URIs of files added by each flush
commit**, and the buffered layer consumes that `Vec<String>` in one place. That is
the signal — no polling, no re-scan.

```
flush commit ──returns added URIs──▶ [count parse]  DashMap<(proj,table,date), u32>
                                          │ counter += added-in-partition
                                          │ if counter > THRESHOLD and not debounced:
                                          ▼
                                    enqueue (proj,table,date) ─notify─▶ ┌───────────────┐
 dedup over-budget (Fix 2) ─────────enqueue same key────────────────▶ │ ONE worker    │
                                                                        │ drains queue: │
                                                                        │  compact_date │
                                                                        │  _with(…,1)   │
                                                                        │  reset counter│
                                                                        └───────────────┘
```

**Components:**

1. **Signal (hot path, cheap).** Where the buffered layer receives the callback's
   returned URIs, parse `date=` + `project_id=` from each and bump
   `frag_counter: DashMap<(Arc<str>,Arc<str>,NaiveDate), u32>`. This is the single
   choke point covering both `bootstrap.rs` and `main.rs` callbacks. Compaction's own
   Adds do **not** flow here (compaction uses delta-rs `optimize`, not this callback),
   so there is no self-trigger loop.

2. **Trigger.** When a partition's counter crosses `compact_fragment_threshold`
   (default 256) and it isn't already queued or within its debounce window, insert
   `(proj,table,date)` into a dedup `DashSet` and `notify_one()` the worker. Enqueue
   is O(1); it never blocks the flush.

3. **Worker (single owner).** One background task (mirrors `DmlCoalescer`'s
   `Notify` + `drain_lock`). Drains the set, and for each partition:
   `compact_date_with(table, date, concurrency=1)` — the same memory-bounded primitive
   the daily consolidate already trusts — then **resets the counter** to the
   post-compaction live file count. Single owner ⇒ no compactor races itself.

4. **Over-budget dedup → same queue (Fix 2).** When `dedup_rewrite_chunk` skips over
   budget it enqueues `(proj,table,date)` and returns incomplete (not certified
   clean). The worker compacts; the next dedup sweep fits. Covers the few-but-huge
   files case that a file *count* threshold misses.

5. **Debounce.** Per-partition min interval (`compact_fragment_min_interval_secs`,
   default 300) so a hot partition still receiving flushes isn't recompacted every
   few seconds. `compact_date`'s Compact skips files already ≥ target, so a wasted
   trigger costs a plan, not a rewrite — the debounce is about churn, not correctness.

**Config (all in MaintenanceConfig):**
- `timefusion_compact_fragment_threshold: usize = 256` (0 disables the feature).
- `timefusion_compact_fragment_min_interval_secs: u64 = 300`.
- No new schedule string — it's event-driven, not cron.

## Interaction with the existing jobs (must get right)

The existing light/optimize/consolidate jobs *also* compact. Running them **and** the
event worker on the same partition is the dual-compactor OCC contention the incident
notes blame for stretching a ~1 h job to ~3 h. Plan:

- **Phase 1 (ship):** event worker + keep the daily consolidate as a backstop. Accept
  occasional OCC retries (they're safe, just wasteful). Gate behind
  `threshold=0`-disables so it can be turned off instantly.
- **Phase 2 (after soak):** the event worker becomes the single compaction owner;
  **disable** the daily consolidate (empty schedule) and shrink `optimize` to z-order
  only. This is where the OCC contention actually goes away.

## Deeper fixes (separate, higher leverage — do not block Phase 1)

- **Write-side minimum file size.** Coalesce flush/relief batches so files are *born*
  near target size. Fewer tiny files created ⇒ compaction becomes rare tidy, not
  firefight. This is the real root fix; pairs with resumable replay (already merged),
  which killed the re-replay small-file storms.
- **Fix 3 — incremental dedup.** Stream the dedup key-scan + filtered rewrite so a
  single fat file never materializes 63 GB, retiring the decode budget entirely. Once
  dedup can't blow a budget, fragmentation degrades only *query speed*, never
  correctness — dropping this whole area from "P0 driver" to "perf hygiene."

## Tests (mandatory — reproduce first)

1. `frag_counter_triggers_compaction` — feed N>threshold flush-added URIs for one
   partition; assert the worker compacts it and the counter resets. Assert a
   *different* partition under threshold is untouched.
2. `compaction_adds_do_not_self_trigger` — run a compaction; assert the frag counter
   for that partition does not increase (proves the callback-only signal).
3. `dedup_over_budget_enqueues_and_self_heals` — tiny decoded budget + dupes across
   many files → over-budget skip enqueues the partition → after the worker runs, a
   re-dedup collapses the dupes. Regression guard for 2026-07-08.
4. `debounce_suppresses_recompaction` — two threshold crossings inside the debounce
   window trigger exactly one compaction.

## Metrics
- `timefusion.compact.fragment_triggered` (counter), `…_files_before`/`…_after`
  (histogram), `timefusion.compact.queue_depth` (gauge),
  `timefusion.dedup.compaction_requested` (counter). Alert if `queue_depth` stays
  high (worker not keeping up) — the early warning the incident lacked.

## Rollout
- Ship Phase 1 behind `threshold=0`-disabled default **or** a conservative 256, after
  resumable-replay soaks. Turn on for one project first if possible.
- Keep the off-box `optimize` CLI as the manual escape hatch.

---

# Risk review (read before implementing)

Ranked by how much they'd worry me.

1. **New work on the durable flush path.** The counter bump sits on the commit path
   that must complete before WAL checkpoint. It MUST be non-blocking and panic-free:
   a `DashMap` increment + URI string parse, no lock held across `.await`, no enqueue
   that can back-pressure the flush. If the frag subsystem ever panics or deadlocks,
   it must not take flush down. *Mitigation:* wrap the signal in
   `catch_unwind`/`Result` and treat any error as "skip the bump"; the worker is
   fully detached from the flush future.

2. **Self-trigger / thrash loop.** Verified the flush callback is flush-only, but a
   subtle path (e.g. a future refactor routing optimize through the callback, or the
   worker's post-compaction counter reset being wrong) could make compaction re-arm
   itself → infinite compaction. *Mitigation:* test #2 above is a hard guard;
   reset-to-actual-live-count (not 0) so a partition still genuinely fragmented after
   a partial compaction re-triggers legitimately but a clean one does not.

3. **We are adding automation days after an incident caused by automation.** The
   7 h outage was a calendar job + deploy interaction; more moving parts is exactly
   what a "maintenance-free" pitch risks. *Mitigation:* `threshold=0` kill switch,
   Phase-1 keeps the proven consolidate as backstop, single-project canary, and the
   worker only ever calls the *already-in-prod* `compact_date_with` — no new
   compaction primitive, just a new *trigger*.

4. **Counter drift / unbounded map.** The DashMap grows one entry per (proj,date)
   ever flushed; counters can drift from reality (missed URIs, restarts lose the
   map). *Mitigation:* the map is an approximate trigger, not truth — compaction
   re-reads the real file set. Evict entries older than retention; on restart the map
   starts empty (the daily consolidate backstop covers the cold gap until it warms).

5. **Threshold is workload-dependent.** 256 is a guess; wrong values churn or miss.
   *Mitigation:* make it config, watch `fragment_triggered`/`queue_depth`, tune. Fix 2
   (over-budget → enqueue) is the safety net independent of the threshold.

6. **Interaction with dedup ordering.** Light optimize runs dedup *then* compact so it
   doesn't bin-pack un-deduped dupes into one file. The event worker compacts without
   a preceding dedup. *Risk:* compacting duplicate-laden files merges dupes into one
   file, which is fine for the file count but the dupes still need dedup afterward.
   *Mitigation:* worker enqueues a dedup pass after compaction (or the next light
   sweep catches it) — but confirm this ordering doesn't fight the read-side dedup
   fingerprint. **This needs a test before Phase 2.**

7. **OCC contention in Phase 1** (event worker vs consolidate/light on one partition).
   Safe (retries) but wasteful; if it's bad, accelerate Phase 2. *Mitigation:* the
   debounce + single-owner worker keeps the event side to one compaction at a time.

**Honest bottom line:** the *trigger* is the new/risky part; the *action*
(`compact_date_with`) is already in prod and safe. The plan is sound if (a) the hot-path
signal is bulletproofed and detached, (b) the self-trigger guard test passes, and
(c) it ships behind a kill switch with the consolidate backstop intact. The write-side
min-file-size fix (deeper section) would reduce the need for this entire subsystem and
is worth doing regardless — arguably *before* investing in the trigger.
