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

**No — not today. The wall is CPU and the hygiene lane, not the disk.**

Current load, same 55-minute process: **1,448 rows/s** ingested, buffer pressure
**9%**, `backpressure_engaged_total` and `backpressure_rejected_total` both
**0**, zero flush failures. At 1x the write path is comfortable.

### A correction I made on myself, first

My first pass said the disk was the wall and was **wrong**, because I measured a
process that had just restarted. Recorded because it is the standing trap in
this codebase and I walked into it anyway:

| measured on | read-miss admission | md3 writes | md3 util |
|---|---:|---:|---:|
| 3-minute process | 824 MB/s | — | — |
| 10-minute process (average since boot) | 482 MB/s | ~300 MB/s | 43-54% |
| **14-minute process, 120 s delta** | **1.9 MB/s** | **8-10 MB/s** | **3.7-4.9%** |

The first two are **cold-start cache warming**, not steady state. A lifetime
average on a young process is dominated by its boot. Taking a delta on a warm
one gives 1.9 MB/s of admission and a device at ~4% utilisation.

This also settles the open question in
[`2026-09-12-cache-admission-plan.md`](2026-09-12-cache-admission-plan.md), and
not in favour of its leading candidate. The admission-source counters shipped
today read `admit_write_capture_bytes` at **0.002 MB/s** against
`admit_read_miss_bytes` — so write-capture is exonerated by its own instrument,
matching the 474 KB/s / 80%-hit-before-evict verdict already recorded. The
~500 MB/s that hunt was chasing is **cache re-warming after a restart**, which is
a restart-frequency cost, not a steady-state throughput ceiling.

### What scales, and what breaks first

| resource | at 1x | at 10x | verdict |
|---|---|---|---|
| CPU | 1,432-2,149% of 4,800% | BaseRollup alone 25-39 of 48 cores | **the wall** |
| Hygiene lanes | 0 worker-seconds, 1.1 TB behind | 10x the file production | **the other wall** |
| MemBuffer | pressure 9-13% | ~90-100% of the hard limit | **no margin** |
| Disk write bandwidth | 8-10 MB/s at ~4% util (warm) | ~100 MB/s, ~40% util | fine |

Steady state on a 14-minute process: 1,470 rows/s, CPU **1,432-2,149% of 4,800%
(30-45% of the box)**, memory 15-16 GB of 120 GB, buffer pressure 13%.
BaseRollup costs **2.5-3.9 cores** (the lower figure on a mature process, the
higher on one still burning its restart backlog).

At 10x ingest, BaseRollup alone wants **25-39 of 48 cores**, before dedup, before
the hygiene lanes that today do nothing and must then do ten times nothing, and
before query. That does not fit.

The disk, by contrast, would go from ~4% to ~40% utilisation — uncomfortable but
not a wall. The 09-11 measurement of 484 MB/s sustained on a **19-hour** process
was real at the time; the tantivy-scratch move, the journal `fsync` reductions
and write-capture gating have since taken steady-state writes down by roughly
two orders of magnitude.

### So the order of work for 10x

1. **Hygiene throughput** — fixed here, needs verification. Not optional at 10x:
   file production is 10x and the lane is currently at zero.
2. **BaseRollup CPU per unit.** It is the single largest consumer and the one
   that scales with data volume. Today's no-op skip and `#262` roughly halved it;
   the remaining cost is real aggregation over 1,490x more rows than are
   ingested, and that ratio is the lever.
3. **MemBuffer headroom.** 9-13% at 1x leaves nothing at 10x. Either the budget
   grows or the flush interval shortens — and shortening it produces more files,
   which lands back on (1).
4. **Restart frequency**, which is now a first-class cost rather than an
   annoyance: each restart re-warms the cache at ~500-800 MB/s for minutes, and
   the no-op rollup skip cannot fire until slices republish. Prod restarted
   three times in two and a half hours today.

Disk is no longer on this list, and that is a change from 09-11 worth noticing
rather than assuming.
