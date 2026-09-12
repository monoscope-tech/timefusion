# The hygiene lanes are dead, and what that says about 10x writes

2026-09-12, measured against production.

## Part 1 — HotPacking and SealedConsolidation do no work at all

`timefusion_stats`, on a 55-minute process:

| lane | worker-seconds | rate |
|---|---:|---|
| BaseRollup | 7,740 | ~2.5 cores |
| Dedup | 801 | ~0.26 cores |
| DerivedRollup | 118 | ~0.04 cores |
| **HotPacking** | **0** | — |
| **SealedConsolidation** | **0** | — |
| **Repair** | **0** | — |

Over 60 minutes, 623 units started: **425 BaseRollup, 130 Dedup, 67
DerivedRollup, 1 HotPacking, 1 SealedConsolidation.** Against that:
`sealed_compaction_debt_bytes` = **1.1 TB**, 196 hygiene cells planned every
60-second pass, and `maintenance_hygiene_debt_unclaimed` firing 14 times per 15
minutes.

The cycle is not the cause — `CYCLE_BALANCED` gives each hygiene operation one
slot in ten, coverage is not short (median 30 days against a threshold of 14) so
neither the debt-slot nor the derived-reserve cap is engaged, and the
`most_indebted_unclaimed` diagnostic reports `outranked_by`, meaning a claimable
task exists.

### The mechanism

Three facts compose into a dead lane:

1. **`light_rewrite_sem` prices 3 permits** —
   `coordinator_share(8 GiB) / COORDINATOR_PER_SORT_BUDGET(1.25 GiB) −
   repair_holdback(3)`, floored against `cores/4`.
2. **The permit is taken BEFORE the claim**
   (`run_coordinator_compaction_selected`). That is deliberate and correct: the
   alternative spends the unit's whole deadline queueing inside `stage_hot_bin`,
   which prod measured on 2026-08-25 as 350-750 s of every 900 s deadline.
   Refusing to claim is work-conserving — the worker goes and does rollup.
3. **Nothing bounds how long a unit may hold that permit.** `run_until_idle` is
   an IDLE window, not a budget: it fires only after a full deadline with zero
   progress, so a unit that emits one progress tick per window runs forever.

So one non-converging unit removes a third of the lane's capacity for as long as
it survives. Production had exactly that:

```
operation=SealedConsolidation  outcome=Some(Running)  ran_secs=7634
```

**7,634 seconds — 8.5x its 900 s deadline — completing nothing**, on a day-wide
consolidation of the shared project. `maintenance_coordinator_unit_timed_out`
fired **zero** times in two hours. `compaction_permits_unavailable` reached
1,812 in 55 minutes, which is the refusals being counted while the acquisitions
behind them were 2 per hour.

### Fixed here

`coordinator_operation_lifetime_cap` gives the permit-holding lanes an absolute
wall-clock ceiling of 4x their idle deadline. Killing such a unit loses nothing:
the `TaskLease` requeues it on drop and `abandon_running` bisects it, which is
what a whale day-wide unit needs — children that fit. Rollup and dedup keep
idle-only semantics, because they hold no permit and rollup cost is set by input
FILE COUNT, so bisecting it converges on nothing.

Two counters close the instrument gap that made this take forty minutes to find
rather than one: `compaction_permits_acquired` (the denominator
`compaction_permits_unavailable` never had) and
`maintenance_unit_lifetime_capped`.

## Part 2 — can we take 10x the writes?

**No. Not today, and CPU is the least of the reasons.**

Current load, same 55-minute process: **1,448 rows/s** ingested, buffer pressure
**9%**, `backpressure_engaged_total` and `backpressure_rejected_total` both
**0**, zero flush failures. At 1x the write path is comfortable.

### What scales, and what breaks first

| resource | at 1x | at 10x | verdict |
|---|---|---|---|
| CPU | ~1,800-2,500% of 4,800% | BaseRollup ~25 cores of 48, plus dedup and hygiene | **tight, not fatal** |
| MemBuffer | pressure 9% | ~90% of the hard limit | **no margin** |
| Disk write bandwidth | 284-341 MB/s at 43-54% util | ~3 GB/s asked of an array that saturates near 600-700 MB/s | **~5x short — the wall** |
| Hygiene lanes | 0 worker-seconds, 1.1 TB behind | 10x the file production | **diverges without bound** |

### The disk is the wall, and amplification is why

The array is **much healthier than on 09-11** — md3 now runs 43-54% utilisation
with `w_await` **1.6-1.8 ms** and queue depth 12-23, against 99-100%,
63-620 ms and 212-620 then. The tantivy-scratch move and the journal `fsync`
reductions bought that.

But the ratio has not moved. Durable ingest contributes ~4.8 MB/s
(`flush_freed_bytes_total` 15.7 GB / 55 min, and that is DECODED bytes, so the
true on-disk figure is smaller and the ratio larger). The device writes ~300 MB/s
sustained. **The box writes on the order of 60 bytes for every byte of ingest
that lands.**

Ten times the ingest at the same amplification asks for ~3 GB/s from a device
that saturates around 600-700 MB/s. **Faster disks do not fix this; a smaller
multiplier does.**

The largest single channel is now measurable, and it is not maintenance rewrites:

```
foyer.admit_read_miss_bytes: 158.5 GB -> 232.6 GB over 90 s = 824 MB/s
```

Foyer admitting read-miss bytes into the L2 disk cache, at **824 MB/s** on a
warming process — more than the whole device does in steady state. And
`l2_used_bytes` reads **638 GB against a 600 GB configured cap**, which is the
over-cap condition the 09-11 doc already flagged as a misconfiguration. An L2
that is over its cap admits, evicts, misses and re-admits; that loop is
self-sustaining and is paid entirely in disk writes.

### So the order of work for 10x

1. **Hygiene throughput** — fixed here, needs verification. At 10x this is not
   optional: file production is 10x and the lane is currently at zero.
2. **Foyer L2 admission policy.** Measure `admit_read_miss_bytes` against
   `evictions` on a mature process. If the cache is thrashing over its cap, this
   is the single biggest write channel and the cheapest to cut — a cache that
   cannot hold its working set should decline admission, not churn.
3. **MemBuffer headroom.** 9% at 1x leaves nothing at 10x. Either the budget
   grows or the flush interval shortens — and shortening it produces more files,
   which lands back on (1).
4. Only then, per-unit maintenance cost. The 09-11 fix order
   (`verify_blob` double-unpack, unattributed DataFusion spill) still applies and
   is not re-derived here.

CPU headroom is real: the box runs at roughly half of 48 cores with BaseRollup
down to ~2.5 cores after today's rollup work. The constraint is bytes to disk,
not instructions.
