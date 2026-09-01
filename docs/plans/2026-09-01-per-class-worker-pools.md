# The coordinator should have per-class worker pools, not gates

Raised by the user on 2026-09-01, after the self-starvation incident: *"isn't
this solved via a queue or other architecture?"*

Yes. Everything done to that bug class so far — including the fix shipped in
`0876b07f` — is a patch on a shape that produces the bug.

## The shape

16 identical coordinator workers. Each loops: rotate the operation cycle → claim
a task of that operation → run it. Concurrency limits live in **semaphores taken
after the claim**, so a worker can be holding a claim and blocked on a resource
it should never have claimed against.

Every guard in `maintain.rs` exists to paper over that:

```
maintenance_quarantine_slots     try_acquire
maintenance_admission            try_acquire   (3 call sites)
light_rewrite_sem                try_acquire   (HotPacking | SealedConsolidation)
maintenance_debt_slots           try_acquire
maintenance_derived_reserve      try_acquire
repair_pass_permit               try_acquire
repair_rewrite_sem               try_acquire   <- added 2026-09-01
light_rewrite_sem                acquire().await   <- reachable only from dead code
```

(That last one is at `maintain.rs:6360`. Its only callers pass
`runtime_env: None`, which is `optimize_table_light` and the legacy wave engine
— and `optimize_table_light` has **no callers**. So it does not park a
coordinator worker today. It is left alone deliberately: changing inert code is
churn, and a pool design removes it anyway.)

Eight pre-claim gates, one remaining blocking wait. Each was added after a
different production incident, and each is the same lesson relearned: *check
before you claim.* The 2026-09-01 starvation — `tasks_running=16/16`,
`tasks_pending=3,364`, **zero** completions in 25 minutes, `pending_*` gauges
frozen byte-identical — happened at the one place that had not yet learned it.

**It is a recurring stall, not a deadlock.** The fleet recovered on its own once
the repair rewrite holding the permit finished: 20 completions in the following
six minutes and `pending_repair` resumed falling. So the failure mode is a
~25-minute fleet-wide stall every time a long repair rewrite overlaps other
claimed repair units — invisible in the gauges while it lasts, which is what
makes it worse than its duration suggests.

The deeper cost is that a blocked worker is invisible. It is not idle (so no
"idle workers" signal fires), not failing (so no error counter moves), and not
finishing (so the planning pass that publishes every gauge never runs). **The
monitoring said "flat and converging" while the fleet did nothing.**

## The architecture that removes the class

Dedicated worker pools per operation class, sized by the resource each class
needs. **The pool size IS the permit**, so a worker never waits for one:

| pool | workers | sized by |
|---|---|---|
| repair | 1 | one whole-file rewrite's memory (the current `repair_rewrite_sem`) |
| hygiene (HotPacking + SealedConsolidation) | 5 | `coordinator_share / COORDINATOR_PER_SORT_BUDGET_BYTES - 1`, the measured optimum |
| dedup | N | its own share |
| rollup (base + derived) | M | its own share |

A repair worker only ever claims repair. It cannot be blocked by hygiene and
cannot starve rollup, because it was never eligible to take a rollup slot. The
seven pre-claim gates collapse into pool sizing, which is one number per class
that a benchmark can set.

This also fixes the *other* half of tonight's incident for free: with a rollup
pool that repair cannot enter, the planning pass and gauge publication keep
running no matter how long a repair rewrite takes.

## The prior art already said this

From `2026-08-31-how-other-systems-schedule-maintenance.md`, rule 7 — written
before the incident, and not applied:

- **RocksDB**: `Env::Priority::{BOTTOM, LOW, HIGH, USER}`. Flushes go to HIGH,
  compactions to LOW, and *bottommost* compactions — the long ones — to BOTTOM.
  Stated reason: "stalling memtable flush can stall writes, increasing p99."
  That is precisely our long-repair-starves-everything failure.
- **ClickHouse**: `background_pool_size`, `background_fetches_pool_size`,
  `background_common_pool_size`, `background_schedule_pool_size` — separate
  pools per class, plus `number_of_free_entries_in_pool_to_lower_max_size_of_merge`
  so a busy pool admits only small work.
- **SILK**: a reserved bandwidth floor (50 of 200 MB/s) for flush and L0→L1 that
  deeper compactions can never take.
- **Druid**: `compactionTaskSlotRatio` caps compaction at a fraction of cluster
  capacity, *with a guaranteed floor of one task*.

Four systems, one answer: **partition the workers, don't gate them.**

## Why it was not done tonight

It is a rewrite of the coordinator's worker loop and its claim path, at the end
of a night that already shipped ten deploys. The patch in `0876b07f` stops the
bleeding — a repair unit that cannot get its permit now hands the worker back
instead of parking on it — and is worth having either way, because a pool design
still needs the claim to be non-blocking.

## Doing it properly

1. **Keep the claim path non-blocking.** As of `0876b07f` it is: the only
   remaining `acquire().await` is unreachable from the coordinator. The rule to
   hold is that no new one may be added — a pool design depends on it.
2. **Add a per-class pool to the worker spawn** in `mod.rs:4731`: instead of
   `coordinator_job_workers` identical tasks each rotating `operation_cycle()`,
   spawn per class with its own count. `claim_coordinator_task(operation)` is
   already per-operation, so the claim path needs no change.

   **The cycle is already a weighted round-robin, so the weights map directly.**
   `CYCLE_BALANCED` is 10 slots: Dedup 3, BaseRollup 3, DerivedRollup 1,
   HotPacking 1, SealedConsolidation 1, Repair 1. Over 16 workers that is:

   | pool | workers | from |
   |---|---|---|
   | Dedup | 5 | 3/10 of 16 |
   | BaseRollup | 5 | 3/10 of 16 |
   | DerivedRollup | 2 | 1/10, rounded up |
   | HotPacking | 1 | 1/10 |
   | SealedConsolidation | 1 | 1/10 |
   | Repair | 1 | 1/10, and it matches `repair_rewrite_sem`'s single permit exactly |

   Note the last row: repair's pool size and its rewrite permit become the same
   number, which is the whole point — the permit disappears.

   **The one real design question is `CYCLE_COVERAGE_SHORT`.** The cycle is not
   static: when `rollup_median_contiguous_days` is below goal the fleet
   reweights toward the rollup chain, because six of ten balanced slots go to
   work that cannot move the metric governing 14d/30d query latency (measured
   2026-08-18). Fixed pools cannot do that. Three options, in preference order:

   1. **Resizable pools** — workers hold a per-class token from a `Semaphore`
      whose permits are adjusted when coverage state flips. Keeps the dynamic
      behaviour; the token is taken BEFORE the claim, so the invariant holds.
   2. **Two static shapes** — a balanced set and a coverage-short set, switched
      by draining and respawning. Simple, but a respawn mid-unit is exactly the
      abandonment this whole night was about.
   3. **Give up the reweighting** — measure first whether it still earns its
      keep now that rollup completes at ~480/h instead of 121/h. It was added
      when the rollup chain was starved; that may no longer be true.

   Option 1 is the same mechanism as today's gates, but applied to a *class*
   rather than a *resource* — and a class token can always be honoured by
   simply not claiming, which is why it does not reintroduce parking.
3. **Delete the gates as their pools land**, one at a time, each with the
   incident it was added for named in the commit — `maintenance_debt_slots` and
   `maintenance_derived_reserve` are reserve mechanisms that a rollup pool makes
   redundant by construction.
4. **Size each pool from `benches/rewrite_throughput.rs`** (`TF_BENCH_FLEET=1`),
   which already measures aggregate throughput against worker count and finds
   the cliff. That turns pool sizing into a measurement instead of an incident.

The invariant to hold onto, and the one the current design cannot state:
**a claimed unit is always runnable.**
