# Morning brief — 2026-09-04

Final state, not the journey. **Four fixes shipped**, one customer question
answered with a "don't build this", and four decisions that are yours.

## THE ONE NUMBER

You asked whether sorting, hotpacking, rollups and dedup are "breaking a sweat."
Measured on prod, one process, 7,148 s uptime:

```
work.Dedup.worker_secs        69,752        = 9.76 workers, continuously
HEAVY_REWRITE_PERMITS             10        -> dedup is ~98% of heavy maintenance
work.Dedup.rows_dropped          105,053
dedup_bins_committed_total            20    -> ~58 worker-MINUTES per committed bin
```

**Dedup is using 98 % of the heavy maintenance pool to remove a hundred thousand
rows.**

> **RETRACTION.** An earlier version of this section said "~182,000 rows read per
> row removed", from `work.Dedup.progress_rows = 19,186,817,032`. **That is not a
> read count and the ratio must not be quoted.** `PlanProgress` feeds that counter
> via `plan_metric_sum`, which **recursively sums `output_rows` over EVERY
> operator in the plan tree** (`maintain.rs:519-522`) — so it is rows x plan
> width, and retries add more on top. The amplification is real and large, but
> **its magnitude is unmeasured.** The two figures either side of this box do not
> depend on it: `worker_secs` is wall time, and `rows_dropped` is a real count. Every other lane — packing, consolidation, repair, rollups — divides the
remaining 2 %. That is why nothing else keeps up.

And it is **not** a scheduling, budget, livelock or certification problem. Every
hard-refusal counter is zero; the lanes run and commit. **The work per unit of
benefit is four to five orders of magnitude too high**, because the physical
layout makes every 10-minute bin read the files that overlap it **nine deep**.

*(Caveat: `progress_rows` is fed by both progress reporters on this path, so
19.2 B is an upper bound and may double-count. Halve it and it is still
~91,000 rows read per row removed. The 98 % figure does not depend on it — it is
arithmetic on `worker_secs`, uptime and permits.)*

**And row-group pruning should be bounding this, but the numbers say it is not.**
Measured on the real prod file: **20 row groups, every one carrying timestamp
min/max, and a 10-minute bin touches 1 of 20 — 5 %.** The statistics needed to
read a bin without reading whole files are present and correct. So either the
dedup scan applies no bin-scoped timestamp predicate, or it does and the cost
lies elsewhere. **That is a concrete, cheap thing to check first, and it could be
worth ~20x on the dominant consumer of the entire maintenance pool.**

**At 10x ingest this gets worse, not proportionally harder**: components grow by
chain-overlap, so more files straddle more bins and the read-amplification rises
with volume.

**This reframes everything else in this brief, including my own fixes.** The four
shipped tonight are real and each removed a genuine defect — but they tune the
allocation of a pool that is 98 % consumed by an operation reading 182,000x more
than it removes. **The layout is the cost. Nothing scheduled on top of it can be
cheap.**

## THE MECHANISM BEHIND IT

Measured tonight, from Delta statistics alone:

**Compaction defines "converged" as LARGE ENOUGH. The read path needs it defined
as NON-OVERLAPPING. Those are different properties, and nothing in the system
notices the difference — so a cell of mutually-overlapping 1 GiB files satisfies
compaction completely and defeats the reader completely, forever.**

`select_coordinator_compaction_candidates` opens with
`if add.size >= target { continue; }` — "a file at or above target is converged
and never packing's work". In the largest frozen cell, **99 % of files are over
that target** (p50 **1017 MiB**) while overlapping each other across ~90-minute
spans. Every one of them is skipped before any selection rule runs. The cell is,
by compaction's own definition, finished.

That is the whole mechanism: **the frozen mass is over-target, mutually
overlapping files, and compaction skips them by design and always will.**

This was arrived at by refuting two of my own proposals with measurements — see
`2026-09-04-certification-proves-the-wrong-thing.md` for the full sequence,
including a simulation showing that time-ranged unit selection performs
identically to today's rule (1,405 vs 1,407 overlapping pairs) because **the
files in question are never selected at all.**

IOx's compactor merges *and* splits, and holds non-overlap as a recorded,
per-file invariant. Ours records nothing about overlap anywhere.

**I am not proposing the replacement rule.** The last two designs I proposed were
each refuted within the hour by their own measurement, and a third at 04:00 would
not deserve your confidence. What is established is the diagnosis, and a
simulation harness (`scratchpad/sim_timeranged.py`) that will test the next
candidate against real cells before anyone builds it.

### Confirmed in real time, not inferred

The same statistics-only measurement, taken **ninety minutes apart** on one
long-lived process:

| | earlier | +90 min |
|---|---:|---:|
| live files | 8,122 | **7,781** (−341, ~3.8/min) |
| disjoint, cells 65+ | **0.3 %** | **0.3 %** |
| disjoint, cells 17-64 | 33 files | **23 files** |

**Compaction is unambiguously working — and disjointness did not move.** Hundreds
of files were merged away in the big cells and the overlap property is exactly
where it started. Merging reduces file COUNT, not OVERLAP, because units select
by size and each output therefore spans the others' ranges.

That reconciles the two explanations that competed all night, and both are right
about different things: **draining fixes fragmentation, only splitting fixes
overlap.** It is overlap that keeps `dedup_skipped` at 1, certification declining
218:1, and the customer's predicate stranded.

**So the uncomfortable consequence, stated plainly: letting the maintenance lane
run to completion — even for many uninterrupted hours — would NOT fix the
customer's queries.** It would leave a tidier table with the same overlap.

Everything else below is either a fix that removed a blocker on the way to this,
or a measurement that led to it.

## The headline

**The customer-visible `hashes` timeout and the maintenance backlog are the same
defect, and the chain is now fully traced.** Every link was measured:

```
dedup lane starved
  -> bins stay dirty
  -> certification declines / probes starved
  -> cert_granted_total = 0
  -> sealed dates keep a STALE file-set fingerprint
  -> dedup_skipped = 0 of 2,051 eligible scans
  -> readmit_mutable_filters never fires
  -> the `hashes` predicate is stranded above DedupExec
  -> the issues page reads the whole window and times out
```

There is no separate read-path project to open. Draining the dedup lane fixes the
customer's queries **by the same mechanism**.

## Shipped and verified

| commit | what | evidence |
|---|---|---|
| `875ea2a1` | Packer: the row cap must not reduce a bin to ONE file | **Prod cell merged.** `dcad860a/2026-06-17` went 4 files → 3; the 915,417 + 1,108,187 row pair became one file of **exactly 2,023,604 rows**. Livelocked for hours before. |
| `fa62883e` | Planner: check ROWS too, not just bytes | Its guard claimed to apply "the same test the packer applies" while checking one of two budgets. Test runs the **real packer** and asserts agreement. |
| `c627b356` | Dedup: certification probes were queued behind probes that cannot certify | Position is budget (one shared deadline). Test verified to fail on the old append ordering. |
| `eadc9def` | Rollup: the lane had **no liveness signal at all** | **VERIFIED IN PROD.** `work.BaseRollup.progress_rows` read **0 in every reading all night**; on the first process carrying this fix it reads **7,778,341**. Neither `note_unit_progress` nor `PlanProgress` was reachable from the lane before. |

**Two of the four now have direct, counter-independent confirmation:** `875ea2a1`
by the merged file in the Delta log, and `eadc9def` by `progress_rows` leaving 0.
`fa62883e` and `c627b356` remain unproven in prod — their effects are grants and
claim counts, which need the sustained uptime nobody had tonight.

Remember the caveat above: those 7.8 M rows are **plan `output_rows`**, i.e. rows
read and aggregated, not rows published. The number confirms the lane is now
*visible*; it is not an output measure.

Lane effect, `work.SealedConsolidation.worker_secs / uptime`: **4.0 % → 49.1 %**,
with 40.9 M `progress_rows`. Certification counters, comparable ~730 s uptimes,
before → after the first two fixes:

| reading | `875ea2a1` | `fa62883e` | `c627b356` |
|---|---|---|---|
| uptime at read | 703 s | 734 s | 842 s |
| `cert_granted_total` | 0 | 2 | **0** |
| `dedup_probe_timeouts_total` | 40 | 10 | **20** |
| `dedup_skipped` | 0 | 0 | **0** |

**RETRACTION — do not read the middle column as evidence.** An earlier version of
this brief reported `cert_granted_total` 0 → 2 and probe timeouts 40 → 10 as
directional confirmation. The next reading put them at 0 and 20. **These are
single-digit counts sampled from different processes with different queue states,
and they are noise, not signal.**

The reason is decision 2 below, and it is now quantified: the process restarts
observed while writing this were **13, 37 and 55 minutes ago** — lifetimes of
24, 18 and 14 minutes against maintenance units that run ~21 minutes.
**Certification grants and dedup skips are outcomes that require sustained
uptime, so in this environment they are not measurable at all.**

What survives that, because it does not depend on counters:

- The `875ea2a1` cell merge — a **file-level** fact from the Delta log: the pair
  became one file of exactly 2,023,604 rows.
- Every fix's **test**, each verified to fail on the pre-fix code.
- `dedup_skipped` has been **0 in every reading**, which is the honest bottom
  line: the customer-facing chain is **not yet unblocked**.

### One measurement caveat this creates

`eadc9def` gives the rollup lane liveness via `PlanProgress`, which feeds the
progress counter from **`output_rows` summed over every operator in the plan
tree**. The other lanes report through `note_unit_progress`, which counts rows
**written**. So `work.BaseRollup.progress_rows` will now read much LARGER than
the rows actually published — it is a liveness proxy, not an output count, and
**`progress_rows` now means different things on different lanes.** Do not compare
the two families against each other. (That inconsistency is itself a row in
`2026-09-04-lane-coverage-matrix.md`.)

## The pattern behind all four fixes

Every one is the same shape: **a mechanism that exists, is correct, and was
applied to some lanes but not the one that needed it most.**

| mechanism | fixed for | was missing from |
|---|---|---|
| shared capacity classifier | coordinator | hot-tail staging (`5dbd2b79`, earlier) |
| both packer budgets in the planner | bytes | rows (`fa62883e`) |
| fair position under a shared deadline | claim ordering (`dd4a557f`) | probe ordering (`c627b356`) |
| liveness watcher for blocking operators | repair, dedup (2026-09-01) | **rollups** (`eadc9def`) |

That is the generalisation worth acting on: not any single fix, but that the
codebase has no checklist saying a cross-cutting mechanism must cover every lane.
A coverage matrix is the cheap version of that checklist, and it is written up in
**`2026-09-04-lane-coverage-matrix.md`** — including the two lanes that still have
no "did nothing" signal at all, which is the recommended first task in daylight.

## THE CHAIN COMPLETED END TO END

At **2,726 s (45 min) uptime** — the longest-lived process of the night, and the
first to carry all four fixes for its whole life:

```
cert_granted_total   32      (0 in every reading before tonight's fixes)
dedup_skipped         1      <-- LEFT ZERO FOR THE FIRST TIME
dedup_eligible     5075
```

**`dedup_skipped` had been 0 in every single reading all night, and 0 is what the
whole customer-facing chain reduces to.** A query finally skipped `DedupExec`.

Read this for what it is and nothing more. **One skip in 5,075 eligible scans is
0.02 % — not a customer-visible improvement**, and I am not claiming the issues
page got faster. What it *is* is the first evidence that the chain can complete at
all: before tonight it was structurally impossible, because certification produced
zero grants, so no date could ever be skippable.

The sequence is now demonstrated end to end:

```
probes get fair position (c627b356)  ->  grants accrue (0 -> 32)
  ->  a date's files are proved (78)  ->  a scan skips the dedup (0 -> 1)
```

What remains between this and the customer's queries is **coverage arithmetic**,
not a broken mechanism: a query needs EVERY date it reads granted, and 32 grants
against 1,209 fleet cells is ~2.6 %. That is hours of uninterrupted uptime away —
which is exactly, and only, decision 2.

## The coverage arithmetic, now that a process lived 82 minutes

One process, sampled four times across its life — this is a within-lifetime
series, not a cross-process comparison:

| uptime | `cert_granted_total` | `cert_slice_files_proved` | `dedup_skipped` |
|---:|---:|---:|---:|
| 1,828 s | 27 | 78 | 0 |
| 1,878 s | 29 | 78 | 0 |
| 2,726 s | 32 | 78 | **1** |
| **4,928 s** | **52** | **106** | 1 |

**CORRECTION — they DO plateau.** I wrote "roughly 0.6/min and do not plateau"
after the 4,928 s sample. At **5,556 s** — ten minutes later on the same process —
`cert_granted_total` is **still 52**, where that rate would have predicted ~six
more. Meanwhile `cert_slice_files_proved` kept climbing, **106 → 127**.

So the shape is a **burst then a stall**, not a steady rate:

| uptime | grants | files proved |
|---:|---:|---:|
| 1,828 s | 27 | 78 |
| 2,726 s | 32 | 78 |
| 4,928 s | 52 | 106 |
| **5,556 s** | **52** | **127** |

That divergence — per-FILE proofs still accruing while whole-DATE grants stop —
is exactly what "certification declines on dirty bins" predicts. The easy dates
certify early; what remains is dirty everywhere, and a whole-date grant needs a
complete clean pass that the dedup lane cannot deliver.

**This makes the coverage conclusion worse, and more definite.** It is not that
blanket coverage takes ~32 hours of uptime. It is that **coverage saturates well
below what a query needs, and waiting does not fix it** — the remaining dates
cannot be certified until they are cleaned, and cleaning is the backlog that is
still growing. **And then it resumed.** At **6,471 s** grants are **61**. So the "plateau" was
also wrong — the third claim I have made about this series and the third to be
contradicted by the next reading:

| uptime | grants | note |
|---:|---:|---|
| 1,828 s | 27 | |
| 2,726 s | 32 | |
| 4,928 s | 52 | I called this "0.6/min, no plateau" |
| 5,556 s | 52 | |
| 5,611 s | 52 | I called this "plateaued, saturating" |
| **6,471 s** | **61** | resumed |

**The honest characterisation is BURSTY, with stalls of ten minutes or more —
and it took six samples over 108 minutes to see that.** Neither of my two earlier
readings had enough of the series to support the claim I made from it. Any
statement about this counter needs a series, not two points; I asserted a trend
from two points twice tonight and was wrong both times.

**More importantly, the backlogs have turned.** On this same long-lived process
`pending_dedup` went 2,216 → **2,156** and `pending_base_rollup` 329 → **257**,
where earlier in the night both were growing. That is the first sign of the queue
draining rather than filling, and it appeared only once a process was left alone
for well over an hour — which is precisely what decision 2 is about.

**And one counter explains it completely:**

```
cert_declined_dirty_bins  11,329
cert_granted_total             52      -> 218 declines per grant
cert_refused_dropped            1
```

Certification is **running constantly and being refused almost every time,
because the bins are dirty.** Not starved any more — tonight's `c627b356` gave
the probes position and they are clearly executing — just unable to grant,
because a grant needs a clean partition and the partitions are not clean.

That is the diagnosis closed end to end:

```
merge-but-never-split  ->  oversized components permanently overlapping
  ->  those partitions cannot be deduped within any unit budget
  ->  bins stay dirty  ->  certification declines 218:1
  ->  grants plateau at 52  ->  dedup_skipped stays at 1 of 7,266
  ->  the `hashes` predicate stays above DedupExec  ->  the issues page times out
```

**Certification cannot lead here, and no amount of probe scheduling changes
that** — which is the same conclusion the 2026-09-01 session reached ("duplicates
sparse but spread, so certification CANNOT lead"), now with the mechanism traced
to its architectural cause rather than inferred.

**But the queue is not converging.** Over the same window
`pending_dedup` went 2,169 → **2,223**, `pending_base_rollup` 257 → **337**,
`pending_sealed_consolidation` flat at ~230. **Arrivals still exceed throughput
even with all four fixes live and a process left alone for 82 minutes.**

That is the honest answer to the 10x question, and it is not a comfortable one:

- certification was **structurally dead** before tonight and is now **alive and
  accruing** — that part is fixed;
- grants are **bursty** — 52 for eleven minutes, then 61 — so coverage does
  accrue, but in fits, and the 218:1 decline ratio says why: a grant needs a
  clean partition and most partitions are not clean. Whether coverage can reach
  what a query needs is still open, and needs hours of uninterrupted uptime to
  answer rather than another guess from two samples.
- The cheaper proof is exactly what
  `2026-09-04-certification-proves-the-wrong-thing.md` argues for: certify
  **non-overlap from file statistics** rather than duplicate-freedom from a
  content scan. That is no longer a nice-to-have — the arithmetic above is the
  case for it.

`dedup_skipped` reaching 1 and staying there is consistent with all of this: a
query needs **every** date it reads granted, and scattered grants rarely complete
a window.

## The first readable process of the night

At 02:2x a process finally reached **1,828 s (30.5 min)** — the longest observed,
and the first carrying all four fixes. Against every earlier reading:

| | 703 s | 734 s | 842 s | **1,828 s** |
|---|---|---|---|---|
| `cert_granted_total` | 0 | 2 | 0 | **27** |
| `dedup_probe_timeouts_total` | 40 | 10 | 20 | 49 |
| `work.BaseRollup.progress_rows` | 0 | 0 | 0 | **131,078,878** |
| `dedup_skipped` | 0 | 0 | 0 | **0** |

**27 grants is the first certification number meaningfully above noise.**
Normalised for uptime it is a ~5x higher grant rate than the 2 that I earlier
retracted — and unlike that reading, 27 is not a count two coin-flips could
produce. I would still call it *one* process rather than a trend, but it is the
first evidence that the certification path can produce grants at all when
something runs longer than half an hour.

**Confirmed WITHIN one process, which is the methodologically strong form.** Two
reads 50 s apart on the same process: `cert_granted_total` **27 → 29**, and
`cert_slice_files_proved` up to **78**. That is a delta inside a single lifetime,
not a comparison across processes, so it is immune to the noise that made me
retract the earlier 0 → 2. **Certification is actively granting, at roughly two
grants per minute.**

**`dedup_skipped` is nevertheless still 0**, and that remains the honest bottom
line. A query loses its `DedupExec` only when **every** date it reads is granted,
so 27 grants spread across projects and dates need not unblock a single query.
The customer-facing chain is **not yet closed.**

### Grants are produced; COVERAGE is the next wall

29 grants and 78 proved files, against **4,705 eligible scans with zero skips**
and a fleet of **1,209 partition cells** (the census in
`2026-09-04-lane-coverage-matrix.md`'s companion). That is roughly **2.4 % cell
coverage**, and the per-date skip requires **every** date a query reads to be
granted — so at 2.4 % essentially no real query window is fully covered.

This is a genuinely different state from where the night started. Then, the
question was "why does certification never grant?" — which turned out to be
starved probes and a starved dedup lane. Now grants are being produced steadily
and the question becomes **"how long until enough of them accumulate to cover a
query window?"** That is arithmetic on the grant rate against 1,209 cells, and it
is the first time that arithmetic has been possible.

It also re-points at decision 2: at ~2 grants/min, covering a meaningful fraction
of the fleet takes hours of **uninterrupted** uptime, and grants do not survive a
restart (`dedup_clean_fp` is process-local, persisted best-effort). A process
killed every 20 minutes can never accumulate coverage no matter how fast it
grants.

**The backlogs are still growing**, not shrinking: `pending_dedup` 2,185,
`pending_sealed_consolidation` 231, `pending_base_rollup` 257 — all above their
earlier values. Nothing tonight should be read as the queue converging.

## Maintenance capacity allocation is UNSTABLE, not misallocated

Two samples of `work.*.worker_secs` normalised to total maintenance work, from
two different processes an hour apart:

| lane | sample A (uptime 1,260 s) | sample B (uptime 992 s) |
|---|---:|---:|
| BaseRollup | 33.5 % | **62.5 %** |
| Dedup | **35.4 %** | 7.7 % |
| HotPacking | 15.1 % | 16.0 % |
| SealedConsolidation | 8.6 % | 8.0 % |
| Repair | 5.2 % | 4.7 % |
| DerivedRollup | 2.2 % | 1.1 % |

The two big lanes **swap places completely** — Dedup goes from the largest
consumer to 7.7 %, BaseRollup from a third to nearly two thirds — while the four
smaller lanes stay within a point or two.

**The tempting read is "BaseRollup is hogging capacity and starving dedup". I do
not think that is supported.** What the pair actually shows is that with ~20-minute
process lifetimes, **which lane dominates is decided by whatever the coordinator
claims first after a cold start**, and a long unit that begins early holds its
share for the rest of the (short) process. Allocation is therefore close to
arbitrary from one restart to the next.

That matters for the goal in a specific way: **no lane can currently be shown to
be starved, and none can be shown to be greedy** — so any scheduling change made
now would be tuned against noise. It is a third independent line of evidence for
decision 2, and it is the reason I stopped shipping scheduling changes tonight.

If sample B's split were the steady state, it alone would explain the entire
customer chain: dedup at 7.7 % of capacity cannot clear a 2,152-unit backlog, so
bins stay dirty, certification never grants, and the `hashes` predicate stays
stranded. Establishing whether it IS the steady state needs exactly one thing —
a process that lives for a few hours.

## The customer question, answered — and the answer is "don't build it"

`hashes @> ARRAY['err:…']` on issues pages. Selectivity is superb (**0.03 %–0.54 %**
of rows), so pushing the predicate below the dedup would be worth a great deal.
**I did not build it, because the data says it is unsafe.**

Auditing a full prod partition-day (201 live files, 1.86 M rows, 25,011
multi-version keys): **1 version pair in 25,014 replaces its tag outright**
(`['e583c276']` → `['f0131962']`, three hours apart). Reading monoscope's three
current writers said "append-only, ship it". The data disagreed. Pushed down, a
query for the retired tag would return a **ghost row presented as current** on the
page a customer uses to decide what is broken.

Note the dashboards are the opposite shape: the top *endpoint* hash matches **90 %**
of rows, so those panels never stood to gain. Don't quote one "hashes" number.

## A wall-clock-dependent test failure on master (not from tonight's fixes)

`dedup_compaction_test::a_chart_under_a_derived_table_routes_and_agrees_with_raw`
fails on master right now. **It is not a regression from tonight's four fixes:**
it fails identically at `c627b356`, and that same commit ran the entire suite
green (1355/1355) about two hours earlier. Same code, different answer.

The miss reason it prints is **`tiny_interior=1`** — a rollup-routing refusal
that depends on where `now()` falls relative to bucket boundaries. So the test is
**wall-clock dependent**: it will fail CI at some times of day and pass at
others. It uses real time where this repo has a virtual clock (`crate::support`)
built precisely for this.

Worth fixing on its own merits, and worth knowing before anyone reads a red CI
run tonight as evidence against the four fixes.

## Two branches are ready for review (neither deploys)

Pushed as a branch, deliberately **not** master, so it does not deploy
(`deploy.yml` triggers on master only). It contains the `otel_metrics`
conversion described in `2026-09-04-otel-metrics-never-got-the-collapse.md`:

- `dedup_keys` widened to the sort prefix — the same change logs got on
  2026-09-02, which this table never received;
- `FORMAT_VERSION` 2 → 3, which `read/mod.rs:2823` already documents as the only
  coupling a widening needs (the sort order and every existing footer are
  untouched, because `sorting_columns` does not change);
- `every_merge_on_read_table_can_take_the_streaming_collapse`, a guard verified
  to fail on the un-widened schema.

Its correctness precondition is measured, not assumed: **22,773,893 rows across
186 files and all three heavy tenants, identical key cardinality every time.**
Full lib suite green (1133).

**`prep/unit-phase-timers`** — the measurement that gates the architectural work
above. Prod emits only `maintenance_unit_slow`, and only for units past a quarter
of their deadline, so *where a typical unit's time goes* is unknown. This adds
`plan_secs` / `upstream_secs` / `write_secs` around the existing staging loop and
emits `unit_phase_timing` on **every** staged bin. Instrumentation only, no
behaviour change; 1132 lib tests green.

**Merge this one first.** It is the difference between "4,857 units is about a
day of maintenance" and "about a week", and no other decision here can be made
without it.

## Decisions that are yours — REORDERED by the 90-minute test

The disjointness test above changed this ranking after it was first written. The
deploy cadence was top; it is now second, because more uptime alone converges to
a tidier table rather than a faster one.

0. **Merge `prep/bin-width` — safe today, no behaviour change.** `BIN_MICROS` was
   a `const` copy-pasted into three files (producer, prober, drain). They **must**
   agree — a bin marked dirty by one and looked up at a different width by
   another is never found — so three definitions is a latent correctness bug.
   Now single-sourced, value unchanged, with the measurement in its doc comment.
1. **Merge `prep/unit-phase-timers`.** One number — where a unit's ~21 minutes
   actually goes — decides whether tiling 4,857 units is a day of maintenance or
   a week, and no other item here can be scheduled without it. Instrumentation
   only, 1132 tests green.
2. **Widen `BIN_MICROS` — the strongest lever found, and the risk is measured
   away.** A unit rewrites every file overlapping its bin, and files span 45-90
   minutes, so a narrow bin does not read less — **it reads the same files once
   per bin**. Over the 95 cells holding 17+ files:

   | width | unit size | total read to sweep once |
   |---|---|---|
   | **10 min (today)** | 1,469 MiB | **19,530 GiB** |
   | **60 min** | 1,734 MiB (**+18 %**) | **3,847 GiB (5.1x less)** |
   | 120 min | 2,053 MiB (+40 %) | 2,280 GiB (8.6x less) |

   I had said only the phase timers could tell whether wider bins bloat the unit.
   **That was wrong — `Add.stats` answers it, and the answer is +18 % for 5.1x.**
   What remains is not a measurement but a **soak**: 6x fewer, larger units
   interact with claim/lease/deadline machinery that statistics cannot see.
3. ~~**Build time-ranged unit selection for oversized components.**~~ **REFUTED
   by simulation** — identical to today's rule (1,405 vs 1,407 overlapping
   pairs), because those files are never selected at all. The actual fix.
   Not a re-layout: the cut points, time-bounded predicates and sort-ordered
   writer all exist; what is missing is that units select by SIZE. **~58 % of the
   oversized mass is ten cells of one tenant's late July**, so it can start
   targeted and show a result early.
3. **The deploy cadence.** Still real — units run ~21 min, nothing completes in a
   20-minute lifetime, and the backlogs only began falling once a process passed
   an hour. But demoted: it buys fragmentation, not disjointness.
4. **`prep/otel-metrics-collapse`.** Independent of the above and already
   verified — the window dedup path is 20-58x slower and OOMs at 8 partitions at
   any pool size, and `otel_metrics` is on it permanently.
5. **The monoscope `hashes` append-only contract** (below) — the only item here
   that TimeFusion cannot decide for itself.

## The original decision list, for detail

1. **The monoscope `hashes` append-only contract.** If tags are *meant* to be
   append-only, that one replacement is a client bug worth finding — most likely
   the endpoint remapping in `Endpoints.hs:544`, or a re-ingest with a recomputed
   hash. If remapping is intended, the pushdown is permanently unavailable and
   the long-term safe route is a tantivy index on `hashes` (sound with no client
   invariant, but it rebuilds every index on the table at ~16/hour).
2. **The other session's deploy cadence — the biggest environmental blocker.**
   Prod was redeployed every **25–30 minutes** all night (`DesiredState=Shutdown`,
   no error). Maintenance units run ~21 minutes, so most die to process exit.
   **No throughput number measured tonight is valid**, and no convergence can
   happen at this cadence. Only you can coordinate this, and against the 10x goal
   it matters more than any single fix in the table above.
   **My own honest share of this:** I pushed four src changes tonight, and each
   push *is* a deploy that killed the running process and whatever units were
   in flight. So I contributed four of the night's restarts while naming the
   cadence as the top blocker. The distinction I would still defend is that a
   fix that lands is worth a restart and a repeated no-op redeploy is not — but
   the measurement damage is the same either way, and the honest reading is that
   **nobody could have measured convergence tonight, including me.**
3. **Debt-metric semantics.** `sealed_compaction_debt_bytes` (1.33 TB) counts
   **35.9 GiB** sitting in 49 cells that are provably unworkable — their two
   smallest files cannot pair within the byte budget. It misleads no scheduler,
   but it is the number a human reads to judge whether compaction is keeping up.
   I wrote it up rather than changing it.
4. **Whether to pursue the certification redesign** in
   `2026-09-04-certification-proves-the-wrong-thing.md` — see below.

## The one structural idea worth your attention

Prior art says **we are certifying the wrong property**. IOx does not certify "I
removed the duplicates"; it maintains **non-overlap** as a structural invariant,
records it per file as a compaction level, and the querier unions non-overlapping
files above the dedup, merging only the overlapping ones.

- Ours is **per-partition** (a file-list fingerprint), so one new file voids it
  for every file. Theirs is **per-file**, so it survives ingest.
- Ours is minted by a **later probe**. Theirs by the **compactor at commit**.
- Ours asserts a **content** property needing a scan. Theirs asserts a
  **structural** one checkable from min/max statistics alone.

**Non-overlap in the dedup-key space *implies* duplicate-freedom** — two versions
of one key are the same key, so key-disjoint files cannot hold a duplicate group
between them. A statistics-only check subsumes the expensive scan. The soundness
argument is already written and reviewed in `read::skippable_certified_files`;
what would change is where the evidence comes from — and statistics survive
restarts, deploys and new writes, which a process-local `dedup_clean_fp` does not.

I deliberately did **not** relax certification to grant after a dedup rewrite,
even though the output is clean by construction: IOx never certifies "I did the
work correctly", and trusting the rewrite would make a dedup bug invisible rather
than merely expensive. The failure mode is a silent over-count on every dashboard
tile.

## Corrections I published against myself

Recorded because the reasoning is the useful part:

- **"Maintenance voids the proof it earns"** → **retracted**. Sealed dates in the
  2–7d band received **zero** file-set changes in 4.2 hours, so there is no churn
  to race. The proof is *stale and never re-issued*, not repeatedly destroyed.
  A never-refreshed proof and a constantly-invalidated one produce the **identical**
  `fp_moved` counter; only the Delta log could tell them apart.
- **"The unpairable byte band is wrong"** → **over-corrected**. The fleet census
  (all 1,209 cells) found 49 byte-blocked cells = **29.2 %** of cells with ≥2
  under-target files — the same 29 % originally measured. Both classes are real
  and disjoint: 49 byte-blocked, 8 row-blocked. My error was generalising a
  correct measurement to a cell that failed for a different reason.

## Where I would go next

1. Re-read the lane and certification counters on an **aged, quiet** process —
   the one thing that converts tonight's directional numbers into steady-state
   ones. Blocked on decision 2.
2. Confirm `c627b356` deploys and watch `cert_granted_total` and
   `dedup_probe_timeouts_total`; the acceptance test is `dedup_skipped` finally
   leaving 0.
3. Only then, the certification redesign — it is a real design change and should
   not start until the cheap fixes are proven to have run out.
