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

---

## Measured, 2026-08-17 evening

The night's fixes made the pipeline CORRECT (#136, #139, #142, #148) and
OBSERVABLE (#140, #143, #144, #145, #147, #149). None of them made it BIGGER,
and the numbers below are why that is now the binding constraint.

### The frontier is not a tail — it is a day

`LIVE_FRONTIER_WINDOW_MICROS` is **24 hours**. The scheduler comment describing
the frontier as "small in volume, so one claim in four still clears it" is
wrong at current scale: class 0 covers a full day of ten-minute slices across
every stream.

```
streams        ~26   (13 projects x 2 rollup-declared sources)
per stream     1 slice / 10 min x {Dedup, BaseRollup, HotPacking}
creation       ~7.8 units/min
claimed        ~3.8 units/min   (measured: 38 frontier starts in 10 min)
total drain    ~11.6 units/min
```

So the frontier alone wants **67% of the entire system's drain rate**, leaving
33% for a sealed backlog of 24,000. `eligible_watermark_lag_seconds` climbed
monotonically all evening — 1419 -> 5895 — at close to 1:1 with wall clock.

**This is a prerequisite, not a competing concern.** `rollup_min_contiguous_days`
counts back from YESTERDAY, so a frontier that never finishes today guarantees
tomorrow's yesterday is holed and the coverage metric can never leave zero.
#146 halves the sealed reservation while the frontier is behind, which buys
headroom but does not change the arithmetic.

### The queue is at its ceiling

```
tasks_pending          24,629     BACKFILL_PENDING_CEILING = 25,000
pending_base_rollup     9,364     (4,857 at 16:14 — nearly doubled)
pending_dedup           6,422
pending_hot_packing     5,367     produces no coverage
eligible_sealed_total  14,488
net drain when backfill backs off   ~230 units/hour
```

At ~230/hour net, the standing backlog is ~100 hours of work. The ceiling
behaves as designed (fill, back off, drain, refill) — it is the drain rate, not
the ceiling, that bounds convergence.

### Query cost is fragmentation, not window width

`wide_scan_oversize_total` reached 300 within an hour of shipping. Every one was
`otel_logs_and_spans` selecting **~1,301 files / 1.8 GB** — and the same shape
was measured earlier for a **one-minute** window. These are not wide queries;
they are narrow queries against unpacked partitions. HotPacking is 22% of the
queue, produces no coverage, and is exactly what would fix them.

### What this changes about the open questions

Question 1 (does the frontier need durable per-slice units?) is no longer the
most speculative item on the list — it is the only lever that changes the 7.8
units/min. Reducing the frontier's per-slice unit count is worth more than any
further scheduling or memory tuning, because scheduling can only redistribute a
drain rate that is already fully committed.
