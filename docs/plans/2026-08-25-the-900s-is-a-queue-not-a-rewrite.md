# The 900 s timeout is a permit QUEUE, not a rewrite

**Owner:** unassigned. **Status:** measured, decided, implemented.
**Last reviewed:** 2026-08-25. **Closes:** `tasks/01`'s P0-1 framing.

## Verdict

Hygiene units do not time out doing work. They time out **waiting in line**.
Measured on prod `e67a149` (container up 5 h), 90-minute window, from the
`wave_bin_staged` line that has carried both numbers since 2026-08-08:

| | min | median | max |
|---|---|---|---|
| `permit_wait_ms` | 349,962 | ~535,000 | 750,561 |
| `staging_ms` | 254 | ~2,400 | 15,303 |

**46 of 46 staged bins.** Not a tail, not a shape — every single one. The
rewrite the 900 s deadline was sized for takes **2.4 s**; the unit spends
**350–750 s** of its deadline blocked in `light_rewrite_sem.acquire().await`,
which sits ~46 lines inside `stage_hot_bin` (`database/maintain.rs:5438`),
long after `claim_next` has stamped the unit `Running` and started its clock.
The bins are 0.24–16 MB of input. Bytes were never the constraint.

The rest of the window agrees. 88 `wave_bin_staging_started` (logged *after*
the permit is granted) against 47 `wave_bin_staged`: 41 bins held a permit and
were cancelled at their deadline. Zero `light_optimize_bin_vanished`, zero
`light_optimize_bin_no_rows`, two staging failures — so nothing errored, they
simply ran out of clock. `light_rewrite_permits = max_light_optimize_k()`,
which is `min(light_share/PER_SORT_BUDGET, cores/4)` and prices out at ~2 on
this box. Arithmetic closes: 41 holds × ~250 s + 47 × ~4 s ≈ 10,400
permit-seconds over 5,400 s ≈ **1.9 permits busy — a ~2-permit pool at ~100%
saturation**, fed by 16 coordinator jobs plus the light-optimize cron's waves.

Cost of the queue, same window, from `maintenance_task_finished`:

```
45 Dedup                300 s     8 Dedup                299 s
23 SealedConsolidation  900 s     8 SealedConsolidation  901 s
 3 SealedConsolidation  902 s     2 SealedConsolidation  903 s
 1 SealedConsolidation  907 s
```

53 × ~300 + 38 × ~900 = **50,100 worker-seconds committing nothing**, against
16 × 5,400 = 86,400 of capacity. **58% of the maintenance pool**, burned in a
queue.

## Decision: take the concurrency permit BEFORE the claim, or don't claim

`run_coordinator_compaction_once` acquires the light-rewrite permit with
`try_acquire_owned` **before** `claim_coordinator_task`, and hands it to
`stage_hot_bin` through `HotStageOptions`. No permit means no claim: the
function returns `Ok(false)` and the operation cycle in
`run_maintenance_coordinator_once` moves the worker on to rollup or dedup —
the identical `try_acquire_owned … else continue` shape that
`maintenance_debt_slots` and `maintenance_derived_reserve` already use twenty
lines above it.

This is the wall-time bound the item asked for, stated as an invariant rather
than a number: **a unit's deadline clock may only run while the unit is able to
run.** It needs no throughput constant, no calibration, and no new tuning knob.
Concurrency stays exactly where it was — the permit pool is unchanged, so the
memory argument that sized it is untouched.

Scope is the two operations the measurement indicts, `HotPacking` and
`SealedConsolidation`. `Repair` routes through the same function and is
deliberately left alone: 41 of its 42 units in the window ran **0 s**, returning
at `repair_bin_already_sorted`/`take(1)` without ever reaching `stage_hot_bin`,
so pre-gating it would invent a starvation the evidence does not show. The
cron wave engine keeps its internal blocking acquire — it has a tick budget, not
a journal claim, and nothing is burned when it waits.

### Rejected: mid-flight checkpoint / partial commit

This was the item's nominated strongest prize, and the measurement kills it.
**There is nothing to checkpoint.** 99.5% of the elapsed time is spent before a
single byte is rewritten; the work that a partial commit would save is the
2.4 s of staging. Building a resume protocol — sub-bin transactions, remove/add
consistency across a partial output, re-enqueue of the remainder — to preserve
2.4 s of a 900 s unit is a large correctness surface bought with nothing. Note
also that a Delta bin rewrite cannot commit half its output anyway: the Remove
tombstones for the inputs are only sound once every input row has been
rewritten, so partial progress would have to be re-cut as sub-bins of *files*,
not a checkpoint of one.

### Rejected: split the unit by wall time on abandonment

`abandon_running` already bisects at `attempts >= 2`, and its own comment
(`maintenance_coordinator.rs:2364`) correctly says byte-splitting cannot reach a
wall-time overrun. What the comment could not know is that the overrun is a
queue. **Splitting a unit that is stuck in a line produces two units in the same
line** — more waiters, more claims, more attempts, strictly worse. No bisection
rule of any shape reaches this.

### Rejected: raise the deadline, or raise the permit count

Raising the deadline bounds nothing; the queue simply grows to fill it, which
the code at `maintain.rs:2632` already argues in place. Raising
`max_light_optimize_k` is the one change the incident history explicitly forbids
— it is a memory bound (`2026-07-23`: two concurrent sorts in a 6 GiB slice),
and even a larger pool re-creates the convoy under a burst. The defect is *where
the waiting happens*, not how many may proceed.

## What this fix does and does not buy

**Does:** returns ~50,100 worker-seconds per 90 minutes to rollup and dedup, and
stops `attempts` inflating on units whose only fault was queueing — which today
feeds them into `abandon_running`'s bisect and its ≥900 s backoff floor,
manufacturing exactly the shredded queue that `next/floor` was written to
prevent.

**Does not:** raise the rate at which hygiene retires files. That rate is
`permits / hold_time` and is unchanged by construction — ~47 bins per 90 minutes,
which is what prod already achieves. Anyone reading
`maintenance_hygiene_tasks_retired` after this lands should expect the pool to
stop burning, not the backlog to suddenly drain. The next question — *why does a
bin hold a permit for ~250 s when the median hold is 4 s* — is a different one,
and this document does not answer it.

**`next/floor` can deploy alongside this.** Its concern was that a 660 s run has
thin margin against a 900 s deadline. Rollup is exempt from the permit entirely
(`maintain.rs:2600`, "rollup takes no rewrite permit"), so its runs face only
their own work time — the queue defect never touched them — and they will now
run in a pool that is 58% less occupied by units burning deadlines. The
660-vs-900 margin is its own risk, unchanged in either direction by this change.

## The `attempts` revival path, checked and formally excluded

`enqueue_inner`'s `attempts = 0` reset (`maintenance_coordinator.rs:1821`) fires
only on the `TaskState::Superseded` arm. A unit that `abandon_running` handed
back is `Retry` with `retry_reason == WORKER_FAILURE_REASON`, and the very next
branch (`:1837`) returns before touching anything, precisely so the planner's
60-second tick cannot erase that verdict. So a timed-out unit's `attempts`
**cannot** be reset by the planner, and the prod trace's monotone 1 → 2 is one
surviving record. The sibling's hypothesis was disfavoured; it is now excluded.

## Status of the code: UNVERIFIED — nothing was compiled

The machine was at load ~200-240 and a cold `cargo check --lib` did not reach
this crate before it was killed, so **nothing here has been compiled, linted or
run.** The change is four edits and one test; what must pass before it merges:

- `cargo lint` (0 warnings) and `cargo fmt` — comment reflow is the likeliest
  complaint, and none of the new code is fmt-stable by inspection.
- `cargo nextest run --lib a_packing_unit_never_claims_a_slot_it_cannot_start`
  — **1 selected, 1 passed**. Needs the `timefusion-tests` MinIO bucket.
- `cargo nextest run --lib wave_staging_permits_are_independent_of_heavy_rewrite_permits`
  — the neighbouring test that pins the permit pool's identity and size.

Named risks, in the order I would check them:

1. `run_coordinator_compaction_once` was private and `mod maintain` is a child
   module, so the test could not have called it; it is now `pub(crate)`, the
   same visibility its `run_coordinator_dedup_once` sibling already has.
2. `HotStageOptions` lost its `#[derive(Clone)]` because an
   `OwnedSemaphorePermit` is not `Clone`. No call site clones it (three
   constructions, one destructure), but the compiler is the authority.
3. The test's **last** assertion — that the same turn claims once a permit is
   free — is the one most likely to need adjusting: it depends on
   `claim_next` admitting a 3-day-old `SealedConsolidation` unit in a scratch
   journal. The three assertions before it are the discriminating ones and do
   not depend on it.

## Done when

`maintenance_coordinator_unit_timed_out` for `SealedConsolidation`/`HotPacking`
falls to ~zero over a quiet hour, and no `wave_bin_staged` line reports a
`permit_wait_ms` above a few seconds — because a unit that waited never claimed.
