# Nothing is being built, and that is what blocks 14d/30d

2026-08-23, prod, read-only. The goal is "14d and 30d complete for all six
projects". After the routing work the last measure put **9 of 12 cells
completing**, and every remaining miss is `not_built`. This page is about why
`not_built` never clears.

## The observation

Sampled every 50 s for 3.5 minutes on prod, one container:

```
tasks_running                     = 0      (every sample)
tasks_pending                     = 22218  (every sample, not one unit claimed)
rollup_rebuilds_full_total        = 0
rollup_rebuilds_incremental_total = 0
rollup_scan_projects_total        = 0
rollup_output_rows_total          = 0
```

`tasks_pending` did not move by one across the window, and `eligible_base_rollup`
is **12,329** — these are units nothing is blocking. The backlog is also
*growing*: `pending_base_rollup` read 12,417 → 12,443 → 12,490 over the session.

15 minutes of service logs contain **zero** rollup unit executions — no
`BaseRollup`, no `DerivedRollup`, no claim. So the tier is not being built
slowly; on this container it is not being built at all.

## The leading mechanism: the maintenance pool is doing cache warm

The same 15 minutes contain **15 completed cache-warm tasks plus 6 still
running**, totalling **11,374 files warmed**, with the slowest completed task at
**210 s** and one in-flight task past **573 s** at 3,000/4,394 files.

Cache warm and the rollup coordinator share ONE runtime — `maintenance-worker`,
sized `(cores / 8).clamp(2, 4).max(coordinator_jobs + 2)`. That is a small pool,
and a handful of multi-minute warm tasks can occupy it.

Combined with the deploy cadence this closes:

1. a deploy restarts prod (8 times last night by the concurrent session's count;
   ~15–25 min apart through this session),
2. on boot the pool spends minutes warming ~11k files,
3. prod redeploys at or before the point the warm drains,
4. the coordinator never gets a turn, so nothing is built,
5. `not_built` never clears, so 14d/30d cannot route,
6. and the backlog grows, which makes the next boot's job larger.

## What would confirm or refute it

Stated because the evidence is occupancy plus correlation, not a stack:

- **Confirm:** on a container left alone past the warm, rollup units start
  claiming and `tasks_running` goes non-zero. That is one quiet hour.
- **Refute:** units stay at zero after warm completes — then the coordinator is
  gated by something of its own and cache warm is a bystander. The counters to
  read are `tasks_running` and `rollup_scan_projects_total`.

I could not run either: the longest quiet window in this session was ~49 min and
prod restarted mid-measurement more than once.

## Why this outranks more query-side work

Every routing RULE is now fixed and measured: `filter_not_eligible` is 0 (the
null guard), `stale_coverage` is 0 (the three-site witness/bound fix). Routing
demonstrably works — `rollup_hits_hybrid_total` climbed 1 → 3 while being
driven. The remaining misses are 135 of ~147 `not_built`.

So no further query-side change can move the widest windows. Either the tier
gets built, or 14d/30d keeps missing. That makes coordinator occupancy — not
routing, not the read path — the top item for this goal.

Two adjacent facts worth carrying:

- **One project selects 1,447 files / 460 GB for a 30-day `count(*)`** and is
  refused by the 16 GiB scan guard in 2.9 s. That is a compaction signal. Even
  with a perfect coordinator, a 460 GB partition set wants fewer, larger files.
- **`dirty_bin_queue_depth` is 19,411** with `dirty_bin_processed_total = 0` on
  this container — the same "queued but nothing draining" shape, from the
  compaction side.

## Verified in prod — maintenance started for the first time

`319c5a8` reached prod as image `2321c47`. The container booted at ~14:36 and the
preload-wait bound is 300 s, so the prediction was "nothing until ~14:41, then
work". Sampled once a minute:

```
14:36:15  tasks_running=0   rebuilds_full=0   output_rows=0      pending=22445
14:37:15  tasks_running=0   rebuilds_full=0   output_rows=0      pending=22445
14:38:16  tasks_running=0   rebuilds_full=0   output_rows=0      pending=22445
14:39:16  tasks_running=0   rebuilds_full=0   output_rows=0      pending=22445
14:40:16  tasks_running=0   rebuilds_full=0   output_rows=0      pending=22445
14:41:17  tasks_running=15  rebuilds_full=18  output_rows=6970   pending=22370
```

It fires at the bound, to the minute. Sustained over the following eight
minutes:

```
14:42  running=16  rebuilds=30  output_rows=21782
14:46  running=16  rebuilds=40  output_rows=22647
14:49  running=16  rebuilds=41  output_rows=24807
```

`tasks_running` sits at 16 — the pool is saturated, i.e. work is queued and
being taken, not trickling. ~23 full rebuilds in 8 minutes is roughly 170/hr
against a previous rate of **zero**; the concurrent session's "~4 builds/hr"
ceiling was measured on containers where the coordinator had never started.

`tasks_pending` drifts down slowly (22,445 → 22,326) because ingest keeps
enqueueing while the backlog drains — the relevant fact is that it is moving at
all, and `pending_base_rollup` is falling (12,663 → 12,544).

### What this does and does not settle

It settles the mechanism: an unbounded wait on a preload that never completes was
disabling all maintenance, and bounding it starts the work. It does NOT yet show
14d/30d completing — 12.5k base-rollup units remain, and at this rate that is
hours of drain, during which the deploy train will restart prod repeatedly. Each
restart now costs the 5-minute wait rather than the whole container's life, which
is the difference between a backlog that drains and one that only grows.

The honest next check is whether the tier reaches the sealed days the widest
windows need — `rollup_miss_not_built_total` falling, and `14d`/`30d` cells
turning from `not_built` into hits.
