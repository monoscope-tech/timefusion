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

## The drain rate, measured live — and what it implies

Live counters (not the stale checkpoint), 6 minutes, `tasks_running` = 16 the
whole time:

```
15:11  pending_base_rollup=12514  rebuilds_full=32
15:13  pending_base_rollup=12504  rebuilds_full=32
15:15  pending_base_rollup=12495  rebuilds_full=41
15:17  pending_base_rollup=12511  rebuilds_full=41
```

- **9 rebuilds in 6 minutes at 16 concurrent = ~90 units/hr**, i.e. ~11 minutes
  of worker time per unit (consistent with the concurrent session's "units
  average ~21 min").
- `pending_base_rollup` oscillates 12,495–12,518. **Flat: drain ~= accrual.**

The arithmetic that follows is the whole story:

| | at 90 units/hr |
|---|---|
| 12,500 pending units | ~139 hours |
| 458 sealed CELLS behind them (24.4x) | ~3.7 cells/hr -> ~124 hours |

So the backlog does not converge at this granularity, and it is not close. This
is the quantitative case that **coarsening is the lever and concurrency is not**:
doubling workers halves 139 hours to 70, while collapsing 24.4 units per cell
into one attacks the same number by an order of magnitude — and does it by
removing fixed commit cost rather than by buying more of it.

## Pass condition, re-measured after the coordinator was unblocked

Unchanged at **9 of 12**:

```
87576849 14d  61.0s TIMEOUT     87576849 30d  2.5s REFUSED (460 GB selected)
edb04135 14d   0.8s ok          edb04135 30d 23.7s ok
00000000 14d  14.1s ok          00000000 30d 60.1s ok
dcad860a 14d  10.9s ok          dcad860a 30d 32.0s ok
be87ebc1 14d   9.0s ok          be87ebc1 30d 24.7s ok
28f62f01 14d  42.3s ok          28f62f01 30d  1.1s REFUSED
```

Getting the coordinator running was necessary and is not sufficient: the three
failing cells need their SEALED days built, and at 3.7 cells/hr against 458 they
are days away. Nothing on the query side moves them.

## CORRECTION: the drain rate is ~490/hr, not ~90/hr

The "~90 units/hr and flat" figure above is **wrong**, and the error was the
measurement window, not the system. It was taken on a container still finishing
its cache warm, where the coordinator was competing with warm tasks for the same
small pool. Re-measured on a container PAST the warm, 6.5 minutes, `tasks_running`
= 16 throughout:

```
15:33:27  rebuilds_full=44  pending_base_rollup=12153  output_rows=27704
15:34:43  rebuilds_full=67  pending_base_rollup=12128  output_rows=50360
15:36:00  rebuilds_full=73  pending_base_rollup=12118  output_rows=52552
15:39:59  rebuilds_full=97  pending_base_rollup=12099  output_rows=76724
```

- **53 rebuilds in 6.5 min = ~490/hr.**
- `pending_base_rollup` fell 12,153 -> 12,099, i.e. **~500/hr NET drain** — it is
  draining, not holding level. Across the wider window it went 12,460 -> 12,099
  in ~15 minutes.

The earlier "flat" reading was the same artifact: sampled while the coordinator
was starved, so accrual matched a suppressed drain.

### What this does to the estimate

| | at 90/hr (wrong) | at 490/hr (measured) |
|---|---|---|
| 12,100 pending base_rollup | ~139 h | **~25 h** |
| duty-cycled for restarts (~50%) | — | **~50 h** |

The duty cycle matters and is the remaining tax: each container spends its first
5 minutes in the preload bound, and deploys land every ~20-40 minutes, so a
container gets roughly 15-35 productive minutes before it is replaced. On a quiet
prod the figure is the ~25 h one.

So the conclusion changes in degree, not in kind: the backlog now genuinely
converges, on the order of a day or two rather than a week, and coarsening the
24.4x inflation would still collapse that by an order of magnitude. But "it does
not converge" — stated earlier on this page — is retracted.

## The number that actually decides the goal: sealed cells drain at ~2.7/hr

The checkpoint went stale for 3h and then refreshed, which accidentally produced
a clean before/after 3h44m apart:

```
            09:53      13:37     delta        rate
sealed units   11,174     11,020    -154      ~41/hr
sealed CELLS      458        448     -10      ~2.7 cells/hr
pending base   12,207     12,091    -116
inflation       24.4x      24.6x
```

Both prior estimates on this page were measuring the wrong thing:

- ~490 rebuilds/hr is real, but **only ~41/hr of it is sealed work**. The rest is
  today's frontier, which ingest replenishes continuously.
- so "12,100 pending / 490 per hour = ~25 h" does not describe the goal. The wide
  windows need SEALED days, and those clear at **2.7 cells/hr**.

**448 sealed cells at 2.7 cells/hr is ~166 hours — about a week.**

### This is the quantified case for coarsening

The inflation is doing the damage in exactly the place that matters:

| | units | rate | time |
|---|---|---|---|
| today, 24.6 units per sealed cell | 11,020 | ~41 units/hr | ~166 h |
| coarsened to one unit per cell | 448 | ~41 units/hr | **~11 h** |

Same worker pool, same per-unit cost, 15x less wall clock — because the fixed
commit cost stops being paid 24.6 times per day. Raising `coordinator_jobs`
cannot touch this: it buys more units per hour, and 24.6 of every 25 units are
redundant fixed cost.

### Caveats on this measurement

- The two endpoints come from checkpoint rewrites, not a controlled sample, so
  the rate is an average over 3h44m that includes several restarts. It is the
  right order of magnitude, not a precise figure.
- `complete` base_rollup is 3,000 of which 889 are sealed, consistent with the
  sealed share of drain being small but non-zero.

## Lever 1 shipped and verified: the queue collapsed 85%

`ac962ba` (partition-byte ceiling on the fusion price) reached prod at 17:2x.
Measured immediately after:

```
                      before      after     change
pending_base_rollup   12,460      1,861      -85%
tasks_pending         22,499      5,086      -77%
```

From the journal, the part that matters:

```
sealed units          11,174        637
sealed cells             458        424
inflation              24.4x       1.5x
sealed slice widths   1-10 min   {1440 min: 637}
```

**Every remaining sealed unit is exactly one day wide** — nothing narrower
survives, which is the goal stated as "one unit per (project, date)". The 1.5x
residual is cells carrying more than one tier/table, not shredding.

### Why it took a ceiling rather than a smarter sum

The concurrent session's `GroupPrice` was already the better mechanism: it
charges members sharing an `InputFootprint` once, because they re-read the same
row groups. But it prices only members that HAVE a footprint, and prod's entire
backlog predates footprints — those members keep the old summed price and stay
stuck. The ceiling covers exactly that legacy set: no unit over one partition can
decode more than the partition holds, whatever produced its estimate. The two
compose — footprint de-duplication for new work, the partition ceiling as the
backstop for everything already queued.

### What this does to the drain estimate

637 sealed units instead of 11,174, and each now does a day in ONE scan instead
of ~24.6 redundant ones. The earlier projection of ~124 hours for the sealed
cells no longer applies; the remaining work is a few hundred units, not five
figures.
