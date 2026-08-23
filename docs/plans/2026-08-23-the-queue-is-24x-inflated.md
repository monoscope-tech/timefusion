# The drain cannot converge because the queue is 24x inflated

2026-08-23, read from prod's journal
(`/home/ubuntu/timefusion-data/.timefusion_meta/maintenance_tasks.json`,
95,655 entries) after the coordinator was unblocked and started building for the
first time (`319c5a8`).

## The measurement

```
journal states     superseded 62,140 | pending 21,811 | complete 11,475 | retry 221 | running 8
pending by op      base_rollup 12,207 | dedup 5,848 | derived_rollup 3,415 | repair 341

pending base_rollup                       12,207
  on SEALED days (>= 2 days old)          11,174   (92%)
  of those, sub-day slices                10,520
  distinct (project, date) cells             458
  inflation                                24.4x
```

Slice widths across pending base_rollup:

```
10 min 2,374 | 1 min 2,043 | 6 min 1,652 | 3 min 1,545 | 5 min 1,487 | 1 day 670
```

Worst individual cells:

```
00000000  2026-08-13   1,474 units for ONE day
87576849  2026-07-25     679
87576849  2026-07-29     655
87576849  2026-07-23     570
87576849  2026-07-24     422
```

## Why this is the convergence blocker

Unit cost is dominated by the COMMIT, not the scan — measured previously at
3.74 s of a 5.21 s unit — so a day split 679 ways pays the fixed cost 679 times
for one day's data. The coordinator now runs at 16 concurrent units and produces
~170 full rebuilds/hr. Against that rate:

- **11,174 sealed units ≈ 65 hours.**
- **458 day-wide units ≈ 2.7 hours.**

That is the whole difference between the backlog draining tonight and never
draining. It also explains why raising `coordinator_jobs` is the wrong first
lever: 24x more units than cells means concurrency is being spent on fixed cost,
not on work.

These are SEALED days — 2026-07-23..29 is a month old and immutable. There is no
reason for a sealed day to be anything other than one day-wide unit.

## Why it bears directly on 14d/30d

The two projects whose wide windows still fail are `87576849` and `28f62f01`,
and `87576849`'s late-July days are exactly the ones shredded into 400-679 units
each. Those are the sealed days a 30-day window needs. The tier cannot reach them
at 25 units per cell.

## Not fixed here

Coarsening exists and a concurrent session is actively working drain (their
`2026-08-23` docs pre-register what four drain fixes must show). This page is the
number that workstream needs — 458 cells behind 11,174 units — rather than a
competing change to the same planner. The obvious question for whoever takes it:
why does a sealed, immutable day keep sub-day slices at all, when 670 day-wide
units in the same queue prove the planner can emit them.

## Measurement trap: the JSON is a CHECKPOINT, not live state

`maintenance_tasks.json` is rewritten only on checkpoint. Caught while reading
it twice, 30 minutes apart, and getting byte-identical numbers:

```
maintenance_tasks.json   mtime 09:53:30   46.7 MB
maintenance_tasks.wal    mtime 13:09:09   57.9 MB   <- the live one
now                            13:09:09
```

The checkpoint was **3h15m old**. So:

- The 24.4x inflation above is real, but it is a snapshot **as of 09:53**, not a
  current reading. Quote it with that timestamp.
- "The sealed backlog is not draining" does **not** follow from the two identical
  readings — they were the same file. That claim is withdrawn; it may still be
  true, but this is not evidence for it.

Live pending counts do exist and should be preferred for trend work:
`pending_base_rollup`, `pending_dedup`, `pending_derived_rollup` in
`timefusion_stats` are process-live. Across this session they read 12,663 →
12,544 → 12,602 → 12,552 — hovering rather than clearly draining, which is a
weaker statement than the one the stale file appeared to support.

This is the same shape as the session's earlier traps: a number that will not
move is first a claim about the ruler.
