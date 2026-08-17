# Maintenance unit economics, and why the queue diverges at 10x projects

Status: **#134 fixes the immediate arithmetic. The deeper questions below are
open and are the next exploration.**

The roadmap target is "10x current number of projects on the same server". This
document is about the one property that decides whether that is possible: the
number of durable maintenance UNITS the system creates per day, versus the
number it can retire per day. Everything else — pool sizes, concurrency,
per-unit speed — is a constant factor on top of it.

## The arithmetic

The live path mints one unit per **ten-minute slice** (`NORMAL_SLICE_MICROS`),
per project, per operation, per declared tier. The codebase already states the
per-partition figure, in `plan_rollup_backfill`:

> ~144 slices x Dedup/BaseRollup/HotPacking x each tier, about **450 durable
> tasks per (project, date)**

So, per day:

```
   450 units per (project, source)
 x  13 projects
 x   2 rollup-declared sources   (otel_logs_and_spans, otel_metrics)
 = ~11,700 units/day created
```

Measured drain, 2026-08-17 (16 coordinator jobs, after the day's fixes):
~500 units/hour = **~12,000 units/day retired**.

That is break-even, and it is break-even by coincidence rather than design. The
observed queue confirms it — flat-to-rising all day:

```
16:14Z   tasks_pending 17,993   tasks_complete 7,970   tasks_running 14
         dedup 4,997 · base_rollup 4,857 · hot_packing 4,955
         derived_rollup 1,207 · sealed_consolidation 1,659 · repair 381
         oldest_task_age 106,836s (29.7h)   backlog 18.1 TB
```

**Creation scales linearly with projects; drain does not scale at all** — it is
bounded by cores and object-store round trips on one box. At 130 projects the
creation rate is ~117,000 units/day against the same ~12,000 retired. The queue
does not lag, it diverges, and every `claim_next` scans the task set, so the
scheduler gets slower exactly as the backlog grows.

## Why the units are so numerous

Ten minutes is the right granularity for the **live frontier**: it bounds how
much work a crash loses and how quickly a just-written slice becomes queryable.
It is the wrong granularity for a **sealed day**, where the same 144 units each
pay the same fixed cost — Delta log scan, admission, lease, checkpoint — for a
144th of the data.

The fixed cost per unit dominates: measured earlier in the day, units cost
50-80s of object-store work "regardless of how little data it covers".

## What #134 does

Collapses each sealed day's leftover sub-day units into one day-sized unit, on
the existing 60s planner tick. `migrate_fine_grained_backfill` did this once for
the historical backlog; #134 is the recurring form, because every midnight
creates another day of it.

Expected effect: creation drops from ~450 to ~3-6 units per (project, date)
**after the day seals**, i.e. from ~11,700/day to roughly 11,700 transient +
~150 durable. The frontier still mints its ten-minute units during the day;
they simply stop outliving their purpose.

The anti-loop guard is the load-bearing half: `split_time_task` leaves an
oversized parent `Superseded`, so a day whose day-unit exists in ANY state is
skipped. Without it, a whale's day splits into children, coarsens back into a
day, and splits again forever.

## Open questions — the actual next exploration

1. **Does the frontier need durable per-slice units at all?**
   They exist so a crash cannot lose a slice. But the WAL already provides that
   guarantee for the DATA. If the frontier's units were derived from a cursor
   rather than enumerated, creation would stop scaling with projects entirely.
   This is the single biggest lever and the least explored.

2. **`claim_next` is O(n) over the whole task set, per claim, per worker.**
   Already rewritten once today from O(n log n) plus two allocations. At 16
   workers and a six-operation cycle it is ~100 scans/second over `n`. It needs
   an index keyed by `(class, operation)` before `n` grows another order of
   magnitude.

3. **HotPacking is ~28% of the queue (4,955) and produces no coverage.**
   It is file hygiene. Its units should probably be planned by DEBT (bytes of
   unpacked files) rather than by time slice, so their count tracks fragmentation
   instead of tracking the calendar.

4. **Per-unit fixed cost of 50-80s is mostly Delta metadata.**
   ~6 metadata scans per task at 47-90ms was measured, which is only ~0.3s — so
   the rest is object-store latency on the data path. Worth attributing properly
   before assuming compaction fixes it.

5. **Dedup is not a dependency of BaseRollup** (`dependencies_complete` returns
   `None` for it), yet it holds a third of the queue and, before #127, was the
   largest single consumer of worker time. Its scheduling share should be
   justified against what actually blocks coverage.

6. **The unit count is a function of `NORMAL_SLICE_MICROS`.**
   Nothing has re-derived that 10-minute constant against the current write
   rates; it predates the coordinator. A larger frontier slice would cut
   creation proportionally, at the cost of coarser crash granularity.

## How to tell whether this is fixed

Not by `tasks_pending` alone — it moves for many reasons. The test is:

- `tasks_pending` **falls** across a midnight boundary rather than stepping up
- `maintenance_sealed_slices_coarsened` reports a large collapse each day
- `oldest_task_age_seconds` stops climbing (it was 29.7h)
- creation-per-project-day, derived from the above, is O(tiers) not O(144 x tiers)
