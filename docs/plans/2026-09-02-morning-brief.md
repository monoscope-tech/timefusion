# Morning brief — 2026-09-02 night

Everything below is pushed and green (suite **1323/1323**, e2e **61/61**).
Prod is running my build and healthy. **One decision is left for you.**

## The one decision

**Turn on `TIMEFUSION_LANDED_SKIP_ENABLED`?** It is shipped, tested, and OFF.

- **What it does:** stops TimeFusion re-writing rows it already has. 58% of the
  duplicate groups in a real prod bin are byte-identical rows our own WAL replay
  re-inserted after an unclean exit, and dedup is ~96% of maintenance time — so
  a majority of our maintenance budget is spent removing our own output.
- **Failure direction:** a wrong answer produces a duplicate, never a lost row.
  Every uncertainty returns "flush it"; the only path to a skip is a full
  128-bit match against an identity a landed commit recorded.
- **Validated by:** an end-to-end test on real Delta + object storage that
  reproduces the duplicate the way prod makes it (a commit that lands while its
  cursor advance is lost). Staging would have been the place to prove it, but
  there is no `timefusion-staging` service on the host.
- **Why I did not flip it:** it is the durability path, the area with the worst
  incident history in the repo, and I said in the plan that this call is yours.
  Nothing I learned since changed that.
- **Cost when on:** the identity digest is ~0.37x the parquet encode it avoids,
  and in steady state (no dirty boot) it is a map lookup.

Detail: `docs/plans/2026-09-02-stop-manufacturing-duplicates.md`.

## THE HEADLINE (found last, matters most)

**The maintenance queue is not slow, backlogged, or overloaded. It is STARVING,
and one mis-calibrated constant explains it.**

From the real prod journal: 2,369 queued tasks, **median age 34.8 hours, 49%
older than 48 hours**, and a cohort queued for the journal's entire 17-day span
that never ran. **328 of 328 repair tasks are starved.** At the real arrival
rate (241/h) that queue is under 10 hours of work — capacity was never the
constraint.

The stuck cohort is defined by **admission refusal** (`resource_admission` 272,
`admission_busy` 169, 355 never claimed at all). Admission's ceiling is
`MAX_DECODED_BYTES x (available/capacity)` clamped to **[32 MiB, 512 MiB]** —
against a **median unit of 316 MiB**. The entire scaling band sits below the
working set:

| `MAX_DECODED_BYTES` | 100% free | 50% free | **25% free** | never-admissible |
| --- | --- | --- | --- | --- |
| **512 MiB (today)** | 86% | **33%** | **7%** | 14.1% |
| **2 GiB** | 90% | 88% | **86%** | 10.5% |

At half occupancy only a third of the queue can be admitted; at 25% free, 7%.
A self-sustaining stall.

**And it is over-guarded, not under-guarded.** Admission capacity is 60 GiB of
decoded bytes. Today a single unit may use **under 1%** of the pool it is being
admitted into, and all 10 permits at maximum size use **8%** of capacity. At
2 GiB the worst case is 33% — still a 3x margin.

> **Recommendation: raise the admission ceiling AND the per-sort slice
> together — or neither.** Admission governs what may ENTER; the sort slice
> governs what can FINISH. Today they disagree: admission permits 512 MiB while
> every sized sort failure in the journal is at or below that, median 256 MiB.
> Raising admission alone would convert starved units into sort-failing units on
> the OOM path, while the landed-skip flag is off. Two constants, real
> interaction, wants a canary and someone awake.

**There is a SECOND, separate defect, and it is the lowest-risk fix of the
night.** The largest stuck population is not admission at all: all **197
`compaction_incomplete` tasks are REPAIR**, median age **412 hours (17.2 days)**,
attempts median 14 and **max 875**, **80% whale**. They are *small* (median
0.25 GiB), so they fit the current ceiling — they are admitted, they run, and
they never finish. Repair selects candidates with **`.take(1)`**: one file per
attempt, each attempt clearing one false "unsorted" suspicion. The code comment
predicts exactly this ("could take days… while looking busy the whole time") —
it is taking weeks, and `repair_verified_sorted.txt` is 12.76 MB (~100k paths)
and still growing. Verifying the whole candidate set per attempt touches **no
memory constant**, which makes it the safest of the three candidates.

**Confirmed against live code, not just a snapshot.** I re-fetched the journal
4.4 hours later: of the 1,168 tasks stuck >48h, **1,144 (98%) were still
queued**, only 24 drained, and **359 had their attempts incremented** — they are
being picked up and refused right now, on the binary running in prod.

Caveat I want on the record: the sim **cannot** test this (*"Memory admission
... outside the model"*), so an earlier experiment of mine measured only the
fusion effect and wrongly looked like a dead end. Detail and full evidence chain
in `2026-09-02-scale-readiness-10x-100x.md`.

## CORRECTION you should read first

Late in the night I got the **real prod journal** into the sim (the synthetic
queue could not stand in for it), and it overturns the headline I had written
from synthetic runs.

`synth:whale` forces `mint_frontier = false` — it models a backlog with **no
ongoing ingest at all**. On the real journal, 24 virtual hours, 10 workers:

| run | pending after 24h | executions |
| --- | --- | --- |
| no new arrivals (`--no-mint`) | 21,544 → **18** | 1,594 |
| ongoing ingest modelled | 21,544 → **16,431** | 21,088 |

**The standing backlog is not our problem** — it drains to nothing in 1,594
executions. **The arrival rate is.** With ingest modelled, 13x more work done
still ends further behind than it started.

So: **"10x keeps up" was too generous.** It means "a 10x-costlier *backlog*
still drains", not "we keep up with 10x the traffic". The honest answer to *are
we breaking a sweat at today's load* is, on the real queue with ingest
modelled, **yes**.

The scale-vs-concurrency findings below remain valid for what they measure —
how unit cost and worker count interact on a backlog — and the 100x diagnosis
(units too big, ~51% timeout rate invariant to concurrency) is unaffected.

## Scale readiness — measured, not asserted

`timefusion sim synth:whale`, 24h virtual, prod's pinned 10 workers, 4 seeds:

| load | result |
| --- | --- |
| **10x** | **keeps up** — queue fully drains, zero timeouts, lag 0 → 1h |
| 30x | keeps up, straining (138 timeouts) |
| 50x | **does not drain** at 10 workers; drains at 20 |
| 100x | does not drain at ANY worker count tried (up to 160) |

**The constraint changes character with scale, and that is the finding:**

- **50x is concurrency-bound** — doubling workers fixes it outright (4/4 seeds).
  But `HEAVY_REWRITE_PERMITS` is pinned at 10 *specifically* to bound rewrite
  OOM, and peak heap ≈ `block_size × permits`. **So 50x is a memory problem, not
  a scheduler problem** — and it is the same ceiling whose OOM restarts
  manufacture the duplicates above. The memory work and the throughput work are
  one problem; this is the measurement that shows it.
- **100x is deadline-bound** — 8x the workers moves pending only ~210 → ~60 and
  never reaches zero, while timeouts *rise* (2036 → 2758). Units must be **sized
  up front** to fit their budget rather than split after they fail. I verified
  this is not the split guard: turning the guard off doubles the residue.

Detail: `docs/plans/2026-09-02-scale-readiness-10x-100x.md`.

## The four subsystems you named, measured

| subsystem | verdict tonight |
| --- | --- |
| **rollups** | sim: drains at 10x untouched; knee 30–50x (above) |
| **dedup** | expensive path **healthy** — prod logs show ZERO staging timeouts over an hour; the 114 "timeouts" were the cheap probe, and its backlog is draining 61 → 7 groups/phase |
| **hot-tail packing** | not starved: the memory brake fired **5 times in 24h**, not chronically. It trips at **64 GiB**, which is 80% of the **`TIMEFUSION_MEMORY_BUDGET_GB=80`** prod explicitly sets inside a 120 GiB cgroup. The 40 GiB gap is a deliberate margin for memory the budget does not track, and the OOM history (kills at ~100 GiB anon) says it is **not** free headroom — which is exactly why the 50x lever below is a re-slice of the existing pool rather than a bigger budget |
| **sorting** | the flush-path sort was measured directly: the landed digest is 0.37x a parquet encode, and tonight's earlier work removed the rewrite's remaining `SortExec` |

**Caveat I want to be explicit about:** prod redeployed onto my build partway
through, so the `timefusion_stats` counters are from a **10-minute-old
process**. Their zeros are *consistent with* health but are not evidence of it —
a young process reads as fixed. Every claim above rests on **event counts over a
window** (restart-insensitive), not on cumulative counters. For the same reason
I did not read `pending_dedup` 1647 → 1538 as draining: those are two different
processes.

## The strongest number of the night, from the real prod journal

I fetched the actual maintenance journal (78,741 tasks) read-only from the
running container, which the synthetic runs could not stand in for.

> **6.4% of units exceed the 2 GiB per-sort budget, and those units carry
> 67.1% of all queued bytes.** (repair: 95.3% of its bytes; dedup: 76.9%.)

Unit size is heavy-tailed to an extreme: a dedup unit's median is 0.30 GiB and
its maximum is **1.1 TiB**. Each concurrent heavy sort gets a 510 MB slice.

**This reframes the 100x work as a TODAY problem.** Two thirds of the current
maintenance workload sits in units that individually do not fit the budget they
run against — which explains why adding workers cannot help (the ~51% timeout
rate is invariant to concurrency), and plausibly the 24% superseded rate too.
The fix is the prior art above: bound unit size at SELECTION time using
`estimated_decoded_bytes`, which the journal already carries and which nothing
currently uses as a bound.

## What shipped tonight

| area | change |
| --- | --- |
| duplicates | the landed-batch skip (off by default) + `wal.landed_skips` counters |
| hashing | XXH3-128 for the digest (**17x** the hash vs blake3); XXH3 for in-process hashing incl. `query_fingerprint`, which was running SHA-256 on **every query** |
| hashing safety | six persisted hashes marked `FROZEN HASH` — a blanket sweep would have orphaned rollup history and reset certification coverage |
| observability | split `dedup_probe_timeouts_total` out of `dedup_bin_stage_timeouts_total` |

## Three things I got wrong, and corrected

Recorded because the corrections are more useful than the conclusions.

1. **"80% of dedup work is timing out."** Wrong. One counter was incremented
   from two sites meaning opposite things. Prod logs: **114 cheap probe
   timeouts, ZERO real staging timeouts.** Dedup's expensive path is healthy,
   and its probe backlog is draining (61 → 7 groups per phase over an hour).
2. **"The digest cost is irrelevant next to the parquet encode."** Asserted, not
   measured; it was 3x the encode. Decomposing showed the hash was 83% of it —
   which is what led to blake3 and then to XXH3-128 at your suggestion, and the
   claim is now true by measurement rather than by assertion.
3. **"The canary fails on baseline."** It does not — a fully green 1323/1323 run
   settled it. It is load-flaky, not broken. Three different tests failed once
   each across runs and passed in isolation; one SIGSEGV I chased was a release
   bench compile starving the machine, not a code fault.

## Where I would go next

1. **Your call on the flag.** Everything else about duplicates is inert until it.
2. **50x = a POOL RE-SLICE, and it needs no extra memory.** Worked out
   tonight: heavy sorts live in `heavy_share_bytes()` (~4.98 GiB), not the
   cgroup, and each concurrent sort needs ≥182 MB or it fails instead of
   spilling. That caps permits at ~28 — so doubling 10 → 20 (exactly what the
   50x sim needs) fits inside the pool we already have, if
   `PER_SORT_BUDGET_BYTES` is halved 2 GiB → 1 GiB to keep the guarded 20 GiB
   envelope unchanged. Full arithmetic in the scale-readiness doc. Left undone
   deliberately: OOM path, deliberately guarded, and nothing below 50x needs it.
3. **100x = predictive unit sizing.** A byte preflight already exists (the sim
   exercises it); the design question is whether it can *size* a unit rather
   than only refuse one.
4. **Get a real prod journal into the sim.** Tonight's runs are rollup-shaped
   (`synth:whale`), so they measure the rollup path only. The journal lives in
   object storage, not on the host's disk, so it needs credentials I did not use
   under the read-only constraint.
