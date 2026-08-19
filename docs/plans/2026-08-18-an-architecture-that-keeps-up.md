# An architecture that keeps up: maintenance throughput and 1s 30d dashboards

**Status:** plan. Written 2026-08-18, superseding the approach in
`2026-08-18-plan-close-the-latency-goal.md`, whose failure is documented in
`2026-08-18-handover-maintenance-and-rollup.md`. Every number below is either
measured on prod (dated) or labelled an estimate.

Second pass, same day: added Phase 0 (iteration infrastructure). The overnight
session's real cost was not CI — it was the deploy → restart → 2h-quiet →
ambiguous-counter loop, ~half a day per hypothesis. No design decision in this
plan should require a prod deploy to evaluate; Phase 0 builds the tools that
make that true.

---

## 1. The goal, measurably

| # | Goal | Metric | Target | Where read |
|---|------|--------|--------|-----------|
| G1 | 30d dashboards fast, all projects | `rollup_min_contiguous_days` | **30** | `timefusion_stats` |
| G2 | 30d dashboard latency | real query wall time, per active project | **p50 < 1s, no raw-scan fallback** | timed `psql`, `rollup_hits_full` / `rollup_misses` |
| G3 | Maintenance keeps up | frontier: `eligible_watermark_lag_seconds`; sealed: goal-relevant backlog age | **< 600s** (`FRONTIER_LAG_BUDGET_SECS`); backlog → 0 | `timefusion_stats` |
| G4 | No OOM query death | `exit 137` count | **0 for 7 consecutive days** | `docker service ps` |
| G5 | Tantivy reindex concludes | `tantivy_uncovered_files` | **0** | `timefusion_stats` |
| G6 | 10x projects, same server | creation rate vs drain rate at 130 projects (arithmetic, not prod) | **drain >= creation by design** | section 3 |

G1–G4 are the user-visible goal. G5 is a standing debt item gated on the same
capacity. G6 is a design constraint checked by arithmetic and the simulator
(Phase 0.1), not by prod.

**A metric is not the goal.** The 2026-08-18 session raised hit rates and tier
depths while no dashboard got faster. Every phase below has an exit criterion
stated in terms of G1–G4, and a phase is not done until its criterion is
measured with a real query against real coverage.

---

## 2. What the last session established (do not re-litigate)

Confirmed root causes, all fixed or understood — see the handover for evidence:

- Rollup routing had four stacked silent refusals (#155, #159, #166, #169);
  routing now works. `rollup_output_rows_total` 6 → 5,180.
- Repair units burned 44% of maintenance capacity on a ~78% retry duty cycle
  (#168 fixed the backoff floor).
- The miss taxonomy lied (`not_built` was a recovery artifact, #163/#164).
- The dedup byte estimate is **accurate** (#157) — do not "fix" it; lowering it
  causes OOMs.
- Slot-mix reweighting (#167) changed the mix exactly as designed and moved
  throughput not at all. **Per-unit cost and enqueue volume are the only
  levers. Do not tune the cycle again without first changing one of those.**
- Removing dedup sharding from the streaming branch (#161) without raising its
  300s deadline made dedup timeouts 2.5x worse. The deadline fix exists,
  measured, on branch `fix/dedup-deadline` (30d7b45): `maintenance_unit_slow`
  shows SealedConsolidation at 440s/363s/335s and HotPacking at 320s against a
  900s deadline, while dedup gets 300s.

Process lessons that constrain HOW this plan executes (from the handover, §8):

1. **No deploy churn during measurement.** Each deploy kills in-flight units
   (dedup commits: 0 on a 30-min-old process vs 7 on a 2-hour-old one) and
   resets the date-level coverage map, so a restarted process reads as all-miss.
   Minimum 2 hours between deploys before trusting any throughput number. One
   change per measurement window. Phase 0 exists to make this affordable: most
   questions should never need a prod deploy at all.
2. **Depth-first, not breadth-first.** 60 project-days spread across 13
   projects × 21 days left every project at 0 contiguous days. One project
   fully covered end-to-end teaches more than the fleet partially covered.
3. **Reconcile the arithmetic before believing a rate.** The handover's pending
   delta (+12,906) and its measured net rate (+1.9/min × 206 min = +391) are
   33x apart. The divergence was dominated by the backfill planner enqueueing
   the horizon — **the queue growth was substantially self-inflicted**, which
   means enqueue policy is a primary lever, not an afterthought.

---

## 3. The arithmetic that constrains everything

Unit costs, measured (2026-08-17/18):

- Small frontier unit (10-min slice): ~50–80s, dominated by object-store fixed
  cost "regardless of how little data it covers".
- Heavy day-sized unit: 300–450s (#172's `maintenance_unit_slow`).
- Concurrency: 16 coordinator jobs, each capped at 512 MiB decoded
  (`MAX_DECODED_BYTES`).

### Steady state at 13 projects (26 rollup-declared streams)

Frontier creation (`invalidate()`, `maintenance_coordinator.rs:597`):

```
per stream per day:  144 x Dedup(10m) + 144 x BaseRollup(10m) + 24 x DerivedRollup(1h)
                   = 312 units
fleet:               26 streams x 312 = ~8,100 units/day  (~5.6/min)
```

Capacity for small units: 16 jobs / ~65s ≈ **14.8 units/min**. The frontier
fits with ~9 units/min to spare — *if* the spare actually reaches sealed work
(the sealed reservation in `claim_next` exists because it did not).

Sealed backlog — the goal-relevant set is bounded and enumerable:

```
worst case:   13 projects x 2 sources x 35 days x <=3 units  = ~2,700 units
goal-relevant: 30 contiguous days x 13 x 2 (1h tier + the base it derives from)
```

At 9 spare units/min of small-unit capacity — or far fewer heavy 400s slots —
the sealed backlog is a **days-long convergence process, not a queue that
drains**. That is acceptable IF the convergence is aimed at the goal
(Phase 2) and the frontier stays healthy (G3). It is not acceptable to pretend
break-even is keep-up.

### At 130 projects (10x, G6) — the model breaks here

```
frontier creation: 260 streams x 312 = ~81,000 units/day  (~56/min)
small-unit capacity: still ~14.8/min                     -> diverges ~3.8x
claim_next:          O(n) scan per claim per worker over ~300k+ tasks
```

**The enumerated 10-minute durable frontier unit does not scale to 10x.** This
is the single most important architectural finding in this plan: the frontier's
per-slice units exist so a crash cannot lose work, but the WAL already
guarantees the data — the unit is only a trigger. Deriving frontier work from
a per-stream cursor (or minting hour-wide frontier units) makes creation
independent of project count. This is open question #1 from
`2026-08-17-maintenance-unit-economics-at-10x.md`, promoted here from
"exploration" to **required for G6** (Phase 4.1). The simulator (Phase 0.1) is
where the cursor design gets validated — at 130 synthetic projects, in
seconds, before a line of it ships.

### Why latency has two independent halves

A 30d query = rollup leg + live tail. Measured on a shipbubble 1-day query
(warm): **139ms rollup leg + ~6s live tail**
(`2026-08-17-live-tail-read-amplification.md`). Building 30 days of coverage
(G1) does nothing for the tail: a rolling window always has an uncovered tail,
and today the Delta leg re-reads from S3 what the hot tier already holds on
local disk (~72% of served hot files fail to exclude their window). G2
requires BOTH the coverage work (Phases 2–4) AND the read-path work
(Phase 5). Either alone leaves dashboards slow.

---

## 4. Phase 0 — iteration infrastructure (build these first)

The overnight session's iteration loop was build (~15 min release) → deploy →
restart (kills in-flight units, resets the coverage map) → 2h quiet → discover
the counters can't answer the question → repeat. Call it half a day per
hypothesis. This phase compresses **time-to-knowledge** to ~an hour. What it
cannot compress is **time-to-convergence**: draining ~2,700 heavy units is
~19h of slot time whatever we do — but convergence runs unattended once the
design is proven, so the only thing worth buying is knowledge speed.

Everything here is tooling; nothing here touches prod behavior.

1. **Journal-replay simulator.** ✅ **Landed** (`src/maintenance_sim.rs`, CLI
   `timefusion sim <journal.json|data-dir> [--hours N] [--workers N] [--streams N]
   [--scale F] [--seed N] [--no-mint] [--json]`). The coordinator is
   deliberately IO-free (its module contract is "no scan implementation") and
   its journal is a plain file — `maintenance_tasks.json`, loadable via
   `Coordinator::load` (`maintenance_coordinator.rs:298`), fetchable from prod
   over the read-only SSH access we already have. The sim replays the journal
   on virtual time through the REAL scheduler: `claim_next`, `abandon_running`
   (bisect-on-repeat, deadline-floored backoff), the shared
   `operation_cycle()` (one definition with the server — they cannot drift),
   and frontier minting via the real `invalidate()`. Durations are sampled
   from the measured ranges per operation and width class (`--scale` to
   stress). 48 virtual hours over ~3k tasks replays in ~15s wall.
   - Verified: unit tests pin 13-projects-hold vs 10x-diverges (the §3
     arithmetic, executable), sealed-backlog contiguity, timeout bisection,
     and per-seed determinism.
   - First use is a **backtest**: replay tonight's real journal with today's
     policy and check the sim reproduces the observed lag/queue shape. A sim
     that cannot reproduce the present is not trusted with the future.
   - Then: every scheduler-policy question in this plan (sealed reservation
     share, contiguity-targeted ordering, cursor frontier, 130 synthetic
     projects) is a local run, not a prod experiment.
   → verify: backtest matches the observed 2026-08-18 queue shape within a
   stated tolerance; a 130-project synthetic journal answers
   "diverges or not" in seconds.
2. **`run-unit` CLI.** ✅ **Landed** (`timefusion run-unit --project ID
   [--source T] [--date D] [--op base|derived|dedup|hot|sealed|repair]
   [--slice-hours N]`). Same one-shot pattern as `optimize` / `redrive-dml` /
   `migrate-columns` under the MaintenanceCli budget profile (`main.rs:143`):
   enqueues exactly one unit into a scratch journal and runs it through the
   real coordinator path, printing the scan/stage/commit/e2e counter deltas
   plus wall time. This is handover §7.2 — "nobody has ever profiled where the
   8 minutes go" — turned into a five-minute command runnable from a laptop
   against a scratch R2 prefix. Point TIMEFUSION_DATA_DIR at a scratch dir so
   the journal holds no other claimable work.
   → verify: one BaseRollup day unit's time budget printed end-to-end; the
   Phase 4 ordering gets re-derived from its output.
3. **Staging that can actually carry throughput experiments.** MinIO tests
   validate correctness, not cost: the 50–80s per-unit fixed cost is
   object-store round trips, so experiments need real R2 latency and
   whale-shaped partitions (1.8 GB one-minute windows, 77x ZSTD). A
   `timefusion-staging` CapRover service pointed at a scratch prefix on the
   same R2, seeded with a copied whale day plus a few shipbubble days, makes
   deploys and restarts free — no coverage-map resets on the serving process,
   no customer OOM risk, no 2h-quiet rule. The vertical slice (Phase 3) runs
   here first.
   → verify: staging ingests, builds rollups, and serves queries against the
   scratch prefix; a staging restart is a non-event.
4. **Flags per deploy, batch the experiments.** The repo already ships
   kill-switch env vars (`TIMEFUSION_REPAIR_RESUME_ENABLED`,
   `TIMEFUSION_PLAN_CACHE_TIME_FNS`). Scheduler-policy variants go behind flags
   in ONE image, so a single prod deploy (when one is finally needed) carries
   the whole experiment matrix instead of N deploy-restart-measure cycles.
   → verify: policy switchable by env without a rebuild.
5. **Standing `timefusion_stats` snapshots.** Half the lost nights were
   *re-measurements* — a gauge lied, a counter was unwired, no before/after was
   captured. Cron a periodic dump (pgwire `SELECT` into a file/otel) so every
   experiment has a baseline without anyone remembering to take one.
   → verify: snapshots exist covering every future experiment window.

**The target loop once these exist:**

```
0:00  edit scheduler policy / unit-cost change
0:05  cargo nextest run <targeted>          (correctness)
0:10  sim replay vs tonight's prod journal  (divergence / lag / contiguity)
0:15  run-unit or staging deploy            (real-storage cost)
0:45  decide from printed numbers
```

Prod is touched only after 1–3 agree, under the deploy rules in §12.

---

## 5. Phase 1 — honest baseline, defuse landmines (before any optimization)

Everything in this phase is measurement or one-line risk removals. No tuning.
Items 1–3 use the Phase 0 tools; they should cost hours, not nights.

1. **Decompose one heavy unit end-to-end.** ✅ **Answered 2026-08-18** (prod
   counters + #174 phase timing): rollup units are NOT slow — 174 starts, not
   one over 60s; e2e counters (~15.0M ms over ~4.8k rebuilds) put a rollup
   unit at **~3.1s, split scan ~44% / staging ~12% / commit ~44%**. The slow
   units are debt: HotPacking 320-895s, SealedConsolidation 294-767s, dedup
   bimodal (quick or >300s). The "~8 minutes per rollup unit" that drove the
   handover's arithmetic was an averaging artifact across operations (see
   #176's commit message). Consequence: the rollup chain's cost is commit- and
   scan-bound in roughly equal halves — fewer, larger units and shared commits
   attack BOTH halves; decode/sort is not the story.
2. **Split enqueue accounting by source.** ✅ **Root cause found 2026-08-18,
   via the sim backtest** (below): the dominant enqueue source is
   **restart-triggered whole-day re-invalidation**. On boot,
   `reconcile_maintenance_task_cursors` enqueues `ALL_HOURS` per partition
   with commits since the durable cursor — and unified-table flush commits
   touch every project at once, so even a 5-minute OOM restart re-enqueues
   ~312 tasks x every active stream AND resets the day's COMPLETE frontier
   tasks to Pending (`invalidate` upsert semantics). That is the handover's
   +12,906 (5 restarts that night) and the +5,876 base-rollup jump measured
   across the 14:05 OOM restart. The per-source enqueue counters are still
   worth adding, but the fix is structural: **the boot reconcile must not
   re-enqueue partitions whose dirty state is already journaled** — the write
   path records precise dirty hours durably (`rollup_invalidations.json`), so
   re-invalidating a whole day for partitions the journal already covers is
   pure rework. Only partitions with commits during downtime AND no journaled
   invalidation need the conservative ALL_HOURS enqueue.
   **IMPLEMENTED 2026-08-18** (more precise than sketched): instead of
   skipping journaled partitions, the reconcile now derives the touched hours
   from the missed commits' Add-action timestamp stats
   (`rollup::hours_from_stats_json`; a partition seen only via Removes, or a
   file without stats, still gets ALL_HOURS). A restart now enqueues ~13
   tasks per touched stream-hour instead of ~312 tasks per stream-day, and no
   longer resets completed frontier work outside the touched hours.
   Regression guard: `reconcile_enqueues_only_the_hours_a_missed_commit_touched`.
3. **Verify the goal metric against ground truth.** This codebase has shipped
   three lying gauges (miss taxonomy, unpopulated counters, `MIN(date)`
   "progress"). Check `rollup_min_contiguous_days` against a direct Delta-log
   partition listing for two projects.
   → verify: gauge == hand-computed contiguous days.
4. **Time one real 30d query** on a mid-size project (NOT the whale — a heavy
   ad-hoc query on the busiest tenant OOM'd the box once already; CLAUDE.md's
   prod-read rules apply). Establishes the G2 baseline and proves the raw-scan
   fallback is what we think it is.
   → verify: a dated wall-time number + `rollup_miss` reason recorded here.
5. **Defuse the restart landmine.** Rollup reads work only because
   `TIMEFUSION_ROLLUP_REALTIME_TAIL` happens to be on:
   `recover_rollup_coverage` repopulates slice coverage but never the
   date-level map (`database.rs:3820` comment). One flag flip + one restart =
   every window refused. Either rebuild date-level coverage in recovery or pin
   the flag on in config with a comment naming the dependency.
   → verify: restart with the flag off routes a covered window, or the flag is
   pinned and the dependency is documented at the definition site.
6. **~~Merge `fix/dedup-deadline`~~ SUPERSEDED by events.** #175 merged it,
   an OOM followed (~15 min later; correlation not proof), and #177 reverted
   it. The current understanding (#177's comment): dedup units are bimodal and
   the right fix is to make oversized units FIT — teach `split_time_task` to
   accept hash-sharded children below `MIN_SLICE_MICROS` — not to let them run
   longer. Do not re-raise the deadline without that.
   → verify: dedup day units split instead of timing out; `dedup` timeout rate
   falls without a deadline change.

### Sim backtest result (2026-08-18, real prod journal)

T0 = prod journal fetched 14:02 UTC (65,261 non-complete tasks); one restart
modeled at +3 min (the 14:05 OOM boot); replayed 66 min forward against the
15:08 stats snapshot:

| op | sim @+66min | actual @15:08 |
|---|---|---|
| total pending | 84,310 | 60,077 |
| base_rollup | 45,870 | 37,047 |
| dedup | 29,875 | 14,472 |
| derived / hot / sealed / repair | 427 / 5,345 / 2,255 / 538 | 483 / 5,376 / 2,275 / 534 |

The pre-restart model MISSED the growth entirely (pending shrank); with restart
modeling the shape matches — pending grows, base balloons — with magnitude
overshoot ~2.5x on base/dedup, attributable to known simplifications (the sim
re-invalidates EVERY stream; the real reconcile touches only projects with
commits during downtime, and unified-table commits usually touch all of them).
Conclusion: the sim is trusted for POLICY COMPARISONS (relative ordering), not
absolute prediction. File-hygiene ops (hot/sealed/repair) match within 1%,
which cross-validates the drain side of the model.

---

## 5b. Measured on prod 2026-08-18 ~21:00 UTC — timeout waste is the binding constraint

Deployed image `82d62a8` (#178), process 3h old, two `exit 137` kills in the
preceding 7h. Every number below is from `timefusion_stats` deltas over a
180s window, `docker service logs --since 60m`, and timed `psql`.

### G2 baseline: queries are failing outright, not merely slow

| query (shipbubble `28f62f01`) | wall time |
|---|---|
| `count(*)`, 5-minute window | 279 ms |
| `count(*)`, 1-hour window | **4.7 s** |
| hourly `time_bucket` + `count(*)`, 1 day | **timeout at 60 s** |
| hourly `time_bucket` + `count(*)`, 30 days | **timeout at 60 s** |

Prod logged **127 `57014` statement timeouts in 25 minutes**. The 30d goal is
not a tuning target right now — the 1d query does not complete either.

`rollup_min_contiguous_days` = **0**, so nothing routes and every window is a
raw scan. G1, G2 and G4 are all red simultaneously.

### The box is idle while everything starves

Host: load 21.9 on **48 cores**; the TF container at **772% CPU (7.7 cores)**
and **25.4 GiB of a 120 GiB** cgroup. Neither CPU nor memory nor the object
store (2×429, 1×503 in 25 min) is the constraint. **Maintenance capacity is
being consumed by work that produces nothing.**

### Where the 16 workers actually go (60 min of prod logs)

```
maintenance_task_started   239   BaseRollup 132, HotPacking 42, Dedup 21,
                                 SealedConsolidation 20, Repair 18, Derived 6
maintenance_coordinator_unit_timed_out  47   BaseRollup 24, HotPacking 12,
                                             SealedConsolidation 11
maintenance_unit_slow      26   HotPacking 289-846s, Sealed 271-314s
```

All five timing-out operations carry a **900s** deadline
(`operation_deadline_secs`). So:

```
timeout burn      47 x 900s            = 42,300 worker-seconds/hour
slow completions  ~23 x ~600s          = ~13,800
capacity          16 workers x 3,600s  = 57,600
```

**~73% of all maintenance capacity is spent on units that time out and commit
nothing**, and ~97% is spent before any cheap unit gets a slot. Observed
throughput over 180s: `tasks_complete` **+2**, `tasks_running` pinned at
**16**, `pending_base_rollup` 40,833 → 40,834 (*rising*),
`eligible_watermark_lag_seconds` +181s in 181s — the frontier is not slow, it
is stalled. The single rollup that did complete took **812 ms**.

That is the whole story: **~40,000 sub-second units are starving behind ~47
units/hour that each hold a worker for a quarter of an hour and produce
nothing.**

### Why the units do not fit, and why retrying makes it worse

`maintenance_task_started` decomposes by project and slice width:

```
94c5dc1f width=1440min est_mb=0   x22      87576849 width=11min est_mb=376  x17
98fdd4f3 width=1440min est_mb=0   x20      be87ebc1 width=720min est_mb=256 x13
87576849 width=1440min est_mb=0   x10      28f62f01 width=1440min est_mb=0  x7
```

Two facts, both bugs:

1. **Day units carry `estimated_decoded_bytes = 0`.** `invalidate()` mints every
   frontier slice with a literal `0` (`maintenance_coordinator.rs:765`), and
   `coarsen_sealed_slices` builds the day unit by *summing its children's*
   estimates (`:603`) — 144 zeros. So byte-based admission and byte-bounded
   splitting are both blind to exactly the units that overrun. (The
   `plan_rollup_backfill` path is fine; it passes `MAX_DECODED_BYTES`.)
2. **Bisecting a wall-time timeout amplifies the waste.** `abandon_running`
   splits on the 2nd timeout and gives each child `attempts = 0`
   (`split_time_task`), so a day that cannot fit becomes 2×12h, then 4×6h, each
   child paying two full 900s timeouts before it splits again. The `be87ebc1`
   row above is that ladder caught at the 12h rung, 13 attempts in.
   `abandon_running`'s own doc-comment already states the reason this cannot
   work — *"a day-sized slice with modest bytes still pays an object-store round
   trip per file"* — and then bisects anyway. Halving the slice does not halve a
   per-file cost; it doubles the number of units paying it.

### The per-file cost is the shared root of slow queries AND unaffordable rollups

`EXPLAIN ANALYZE` of the shipbubble **1-hour** query, Delta leg:

```
files_opened=25   bytes_scanned=3.88 MB   metadata_load_time=4.17ms
time_elapsed_opening=36.66ms
time_elapsed_scanning_total=80.85s    time_elapsed_scanning_until_data=34.40s
```

**~3.2 s per small parquet file**, with opening and metadata cheap — so the cost
is per-file byte-range fetching, not planning and not volume. The same constant
sets the rollup unit's cost: a sealed day with several hundred files cannot fit
900 s at 3 s/file, which is precisely why BaseRollup times out.

That closes the loop into a **deadlock**: rollup day units are unaffordable
because their partitions are fragmented; the consolidation that would defragment
them never runs because rollup timeouts eat the capacity.

Also visible in the same plan, and *not* the binding constraint today (Phase 5
still owns them): the hot and Delta legs both return the same hour
(`DedupExec` 405.4 K in → 142.0 K out), and the hot leg materialises **1.5 GB**
for 228 K rows.

### Consequences for this plan

- Phase 1.1's "rollup units cost ~3.1 s" is **half true and dangerously
  incomplete**: rollup units are *bimodal*. `maintenance_unit_slow` logs no
  BaseRollup at all, because a rollup unit either finishes well inside a quarter
  of its deadline or blows through it entirely. Averages over this distribution
  have now produced two wrong throughput models in two days. **Never quote a
  mean unit cost again — quote the completion rate and the timeout rate.**
- #176's occupancy cap is the right *kind* of lever and does not go far enough.
  It caps *debt* occupancy while coverage is short; it does not cap occupancy by
  **units that have already proven they cannot fit**, which is where the 73%
  actually goes, and which includes BaseRollup — an operation the debt cap
  deliberately exempts.
- Raising deadlines is now conclusively the wrong direction (it multiplies waste
  per timeout), and so is time-bisection for per-file-bound units. §12 gains an
  entry.

### Fix landed for this: quarantine slots (#179)

Cap how many workers may sit inside a unit that has already timed out twice, the
same occupancy lever #176 proved, applied to the class that actually burns the
capacity. Proven-unfittable units keep a small, fixed share of the pool and can
still make progress; everything else always has somewhere to run. No deadline
changes, so slow-but-productive HotPacking/SealedConsolidation units (289–846 s)
are untouched.

→ verify: `tasks_complete` per minute rises by more than an order of magnitude;
`pending_base_rollup` turns over; `eligible_watermark_lag_seconds` stops
tracking wall clock 1:1.

### The loop this sits in, and why the same fix is the entry point

Aggregate read counters over the same 180s window:

```
parquet.files_planned   +73,535   =  408 file-opens per second
parquet.read_time_us    +28,640s  =  ~389 ms per file, ~159 readers' worth
parquet.bytes_read      +8.09 GB  =  45 MB/s for all that effort
parquet.scans           +218      =  ~337 files planned PER SCAN
```

A `ScanMetadataCompleted` line for one window reads `active_add_files=186,
active_add_files_bytes=186,419,280` — **~1 MB per file against the 256 MB
`COORDINATOR_SEALED_TARGET_BYTES`**. Partitions are fragmented by two orders of
magnitude, so every query and every rollup unit pays hundreds of ~389 ms
object-store round trips to read a few megabytes.

That closes a self-sustaining loop:

```
fragmented partitions -> rollup day units exceed 900s -> timeouts burn 73%
of capacity -> HotPacking/SealedConsolidation never run -> partitions stay
fragmented
```

Queries and maintenance are not two problems; they are the same file count seen
from two directions. The quarantine cap is the entry point precisely because it
is the only one of these arrows that can be cut without first fixing the others
— it reclaims the capacity that consolidation needs, and consolidation is what
makes both the rollup units and the queries cheap.

**Sequencing that follows:** measure the reclaimed capacity first, and only then
decide whether consolidation throughput is itself sufficient. If partitions
defragment, rollup day units start fitting and coverage builds without any
further change. If they do not, the next lever is consolidation's own unit cost,
not the scheduler.

### Two secondary defects found while measuring (neither is today's blocker)

1. **`decode_polls_inflight` leaks.** `decode_begin`/`decode_end` bracket an
   `await` on the object store (`database.rs:19193/19208`), so a cancelled query
   — and prod cancels 127 per 25 min — drops the future between them and never
   decrements. The gauge rises monotonically with `peak == current` (10,408 and
   climbing ~200/min). Harmless to the semaphore, which returns owned permits on
   drop, but it makes `decode_peak_batch_bytes × decode_polls_inflight_peak` —
   the figure the comment says to size a Transient budget from — meaningless.
2. **`DataSourceExec.output_bytes` overstates by ~15x** when batches are split
   (1,567 MB reported vs 106 MB at the FilterExec directly above it, same
   228.1 K rows), because a sliced batch's `get_array_memory_size` still counts
   the whole parent buffer. Do not read that field as I/O; the plan's earlier
   "hot leg materialises 1.5 GB" reading came from this and is wrong. The hot
   leg's real cost is `time_elapsed_opening = 2.88s` over 48 local files.

### #180's coarsening guard cannot fire for the population it targets

`coarsen_sealed_slices` now does `groups.retain(|_, bytes| *bytes <=
MAX_DECODED_BYTES)`, where `bytes` is the SUM of the fused children's
`estimated_decoded_bytes`. But `invalidate` mints every frontier slice with a
literal `estimated_decoded_bytes: 0` (`maintenance_coordinator.rs:765`), so a
day fused from 144 frontier slices sums to **0** and always passes the guard.
Prod confirms it: every day-wide `maintenance_task_started` logs `est_mb=0`,
across seven distinct projects. Only `plan_rollup_backfill`-originated units
carry a real figure (it passes `MAX_DECODED_BYTES`), which is why the one unit
seen with an estimate — `be87ebc1 width=720min est_mb=256` — is a bisected
backfill day, not a coarsened one.

#180's test constructs tasks with explicit non-zero byte counts, so it passes
while the production population is entirely zeros. **Do not read a post-#180
improvement as evidence the guard worked.**

The gap is not closed by guessing a better default. `0` here means *unknown*,
and treating unknown as zero makes every admission and split decision maximally
optimistic — exactly backwards for a guard — while treating unknown as "too big"
would refuse to fuse anything and undo #178's 12,763-slice collapse entirely.
The honest resolution is the one already shipping: for a KNOWN-oversized day,
#180 keeps the slices; for an UNKNOWN one, the system learns by running it, and
the quarantine cap bounds what that lesson costs to two workers instead of
sixteen. The two changes cover the two cases; neither covers both.

### Measured after #181 shipped (2026-08-18 22:08 UTC, image `c8d3c6b`)

240s window on the new process. The stall is gone:

| metric | before | after |
|---|---|---|
| `tasks_complete` | **+0.67/min** | **+13.94/min** |
| `tasks_pending` | rising | **−1,868/min** |
| `pending_base_rollup` | +0.33/min (*rising*) | **−1,127/min** |
| `rollup_output_rows_total` | +5.67/min | **+123,205/min** |
| `rollup_rebuilds_full_total` | ~0 | +19.17/min |
| `rollup_hits_full_total` | 0 | 66 (first ever non-zero) |
| `rollup_hits_hybrid_total` | +9.33/min | +32.61/min |

Caveat, stated because this plan's §12 demands it: the process was 5 minutes old,
so part of the `pending` drop is boot-time coarsening rather than completion, and
the ≥2h rule has not been met. `tasks_complete` (+21x) and `rollup_output_rows`
are the honest throughput numbers; both are unambiguous.

**G2 is NOT yet met.** Immediately after the deploy, shipbubble's 1d / 7d / 30d
queries all still time out at 60s. Two causes visible in `EXPLAIN`, both
independent of maintenance throughput:

1. **The query does not route to a rollup at all** — the plan is a raw
   mem+hot+delta union with no tier leg. Consistent with `rollup_miss_not_built`
   dominating (+10.5/min).
2. **`GatedScanExec: permits=0`** — the wide-scan gate is saturated, the exact
   signature documented at `config.rs:2558` for 2026-08-01 (a query reading ONE
   file and 8.24 KB paid 40-57s purely queued). A 1-day window is deeper than
   `timefusion_wide_scan_lookback_hours`, so it is gated, and nothing is
   available. The hot leg for that query also carries **430 file groups** for a
   single day — the fragmentation above, seen from the read side.

This confirms §3's claim that latency has two independent halves, and locates
the second half precisely: it is the gate plus file count, not coverage depth.

### Where the coverage actually is (ground truth, Phase 1.3 — done)

Read directly from the tier tables rather than the gauge:

| project | 1m tier days | 1h tier days |
|---|---|---|
| 94c5dc1f | 33 | 17 |
| 98fdd4f3 | 33 | 10 |
| be87ebc1 | 33 | 9 |
| 8100121c | 33 | — |
| shipbubble `28f62f01` | 14 | **6** (08-15..18, plus stale 07-25/26) |

Two things follow, and both correct this plan:

- **`rollup_min_contiguous_days` = 0 is an artifact**, again. The minimum is
  dragged to zero by dormant tenants (`d828e6d5` has one day; `edb04135` stops
  at 2026-08-03). It is a fleet MINIMUM, so one abandoned project pins
  `coverage_is_short()` true forever — which currently biases the scheduler in a
  direction we happen to want, but is not a signal anyone should trust or tune
  against. **G1 must be restated per-project, not as a fleet minimum.**
- **The gap is the DERIVED (1h) tier, not the base.** The base 1m tier is 33 days
  deep on most projects while 1h sits at 9-17. Derived units need no raw scan —
  they aggregate the base tier — so this is the cheapest work in the system and
  the closest to the 30d goal. Yet `pending_derived_rollup` barely moves
  (780 → 757, −5.7/min) while base drains at −1,127/min.

**That makes derived-rollup throughput the next thing to attribute.** The
candidate mechanism is `dependencies_complete`: a `DerivedRollup` is claimable
only when COMPLETE `BaseRollup` *journal tasks* contiguously cover its slice —
journal bookkeeping, not the base tier's actual coverage. A historical day whose
1m tier exists but whose base journal records do not would be permanently
unclaimable, and `claim_next` skips it silently, with no counter. That would be
the sixth silent refusal in this family (#155, #159, #166, #169, and #180's
guard above). **Not yet proven** — starvation alone could explain the old
numbers. The discriminator is now available and cheap: with workers free, if
`pending_derived_rollup` stays flat, it is the dependency gate.

### The discriminator ran, and it is the dependency gate (#184)

Two further 240s windows on the post-#181 process, workers free and zero
timeouts:

- `pending_derived_rollup` did not move by **one task**.
- All **35** derived units claimed in 20 minutes were *frontier* slices whose
  base had completed minutes earlier.
- No project's 1h day count moved at all (94c5dc1f 17, 98fdd4f3 10, be87ebc1 9,
  shipbubble 6 — unchanged across the whole window).

Confirmed the sixth silent refusal. `dependencies_complete` requires COMPLETE
`BaseRollup` *journal tasks* contiguously covering a derived unit's slice, and
`claim_next` evaluates it inside a filter — so a historical day whose 1m tier was
built weeks ago, by an older code path or with its journal records since
collapsed, is unclaimable forever with no counter and no log.

Fixed by making the evidence match the question: `plan_rollup_backfill` computes
`missing` tiers from ACTUAL rollup coverage, so a day missing its derived tier
while missing no base tier proves the base data is present. That fact is
recorded on the task (`MaintenanceTask::base_tier_present`) and the gate honours
it; frontier units, where the planner can prove nothing, keep the strict check.

**This is the shortest path to G1/G2 that exists**, because derived units read no
raw data at all — the 1h tier can backfill from the 1m tier that is already 33
days deep.

### Standing correction to this plan's method

Three of the four defects found tonight were *silent refusals in a filter
predicate* — work that is queued, eligible-looking on every gauge, and never
claimed. Gauges cannot see them: `pending_derived_rollup` reads identically
whether 757 tasks are waiting their turn or can never be claimed at all.

What found all three was the same two-step: (1) diff two `timefusion_stats`
snapshots to find what is NOT moving, then (2) go to the logs and histogram
`maintenance_task_started` by `operation` and by `attempts`. **Add that histogram
to the standing snapshots (Phase 0.5).** Every future scheduler question should
start there, and no scheduling change should be evaluated without it.

### #184 did not reach the tasks it was written for (#186)

Measured after #184 deployed: `pending_derived_rollup` went **746 → 759** over
300s. Still not draining, and now growing.

#184 carried the proof through `enqueue`, but `plan_rollup_backfill` **skips
every day that already has rollup work queued**
(`want.retain(|key| !queued.contains(key))`). A derived unit blocked by
`dependencies_complete` stays queued forever → its day is permanently ineligible
for that admission → the one path that could have carried the proof never runs
for it. **The tasks that most needed the fix were exactly the ones it could not
reach**, and every one of the 759 predated it.

#186 proves it directly on the existing task, over all candidate days rather
than the 24 admitted per pass — touching queued tasks only, minting nothing,
moving no deadline.

The general lesson, worth more than the fix: **a change that only takes effect
at enqueue time cannot repair a queue that is already stuck**, and this journal
is durable, so "already stuck" is the normal case after any incident. Any future
scheduler change needs an explicit answer to "what happens to the tasks that
predate it?" — the deploy alone will not clear them.

### The backfill planner had not run at all (#188) — and this explains G1

`plan_rollup_backfill` begins with

```rust
if pending >= BACKFILL_PENDING_CEILING { return Ok(0); }   // 25,000
```

Prod sat at **61,306 pending**, and the live frontier alone holds the journal
above 25,000 indefinitely. So the pass returned at its first statement every 60
seconds, for hours. **This was not a valve that had closed; it was one that could
never reopen.**

The ceiling's stated argument — a deep journal taxes every `claim_next` — is an
argument about minting MORE work. The early return also took three things that
mint nothing:

- **`rollup_min_contiguous_days`**, the goal gauge, was not being *computed*. It
  read a stale 0 all night, which is why it never moved no matter what was
  fixed — and `coverage_is_short()` reads it to weight the entire scheduling
  cycle.
- **`rollup_coverage_contiguity`**, the only per-project coverage view.
- **#186's base-tier proof**, which is why that fix appeared to do nothing after
  it shipped.

#188 gates the enqueue rather than the pass.

**This retro-explains several earlier conclusions in this document.** Any
statement of the form "the gauge did not move, therefore X did not work" made
before #188 is unsupported — the gauge could not move. The contiguity-targeted
ordering (§6) and the deferral-at-goal have likewise never executed on prod.
They are still unevaluated, not disproven.

### What the fixes actually bought, honestly (as of 2026-08-19 00:00 UTC)

Maintenance no longer wastes capacity — that is measured and solid:

- **Zero timeouts** in the last 20 minutes of logs, against 47/hour before.
- Claim mix went from 18% fresh work to 61%; the 100+-attempt population
  collapsed from 134 starts to 7.
- `tasks_complete` peaked at **+56.75/min** against +0.67/min before.

But steady-state throughput settled back to **~2 units/min**, and the reason is
now different and honest: units *complete* rather than timing out, and they cost
456–801s each (BaseRollup 801s, HotPacking 578s, SealedConsolidation 456s).
16 workers ÷ ~600s ≈ 1.6/min, which matches the observation exactly. **The waste
is gone; what remains is genuine per-unit cost**, and that cost is the ~1 MB file
problem — hundreds of ~389 ms object-store round trips per unit.

**G2 is not met.** shipbubble 1d/7d/30d still time out at 60s. A project that
DOES have coverage returns rather than timing out — `94c5dc1f` (17 days of 1h
tier): 14d in **9.8s**, 7d in **23.1s** — note 14d is *faster* than 7d, because
14d routes to the tier while 7d pays more of the uncovered recent tail. So
routing works and helps by ~2.5x, and is still 10x short of the 1s target.

**Do not raise maintenance scan parallelism to fix the unit cost.**
`MAINTENANCE_MAX_PARTITIONS = 2` and the `maintenance_scan` flag exist because
rollup builds planned at 48 partitions OOM-killed prod four times in ninety
minutes on 2026-08-13. That lever is closed; the file count is the one that is
open.

### After the ceiling opened: the frontier took every derived claim (#189)

#188 landed and the planner ran for the first time:
`rollup_derived_base_tier_proven` fired with **proven=175** and **proven=212** —
387 historical derived units unblocked. Throughput went to `tasks_complete`
**+84.9/min** and `rollup_output_rows_total` **+259,702/min**.

And the 1h tier day count still did not move for any project. Every derived unit
claimed in the following twelve minutes was a **one-hour frontier slice for today
or yesterday**:

```
19  8100121c  2026-08-18  width=60min
 6  87576849  2026-08-17  width=60min
 5  6297304f  2026-08-17  width=60min
```

Class is strict priority and ingest regenerates the frontier continuously, so a
sealed unit runs only on a reserved turn — and that reservation *halves* to
one-in-four exactly when the frontier is behind, which it is. So the units that
had just been unblocked could still never be claimed.

#189 gives `DerivedRollup` a standing sealed reservation. The reservation exists
to stop sealed work starving the frontier, and for derived that premise does not
hold: the frontier mints derived work at `DERIVED_SLICE_MICROS` — one unit per
stream per HOUR, ~24/day — against 144 Dedup + 144 BaseRollup for the same
stream-day. Derived is ~3% of frontier creation, so preferring sealed for it
cannot meaningfully starve the frontier, while it is the only operation whose
sealed backlog directly builds the tier the goal is stated in.

**Four fixes deep, the same shape each time:** the work was queued, every gauge
said it was pending and eligible, and a predicate decided it would never run.
Capacity (#181), backoff (#182), dependency (#184–#186), admission (#188), and
now priority (#189). Each one was invisible until the previous was removed —
which is the real lesson: **these do not show up in parallel, they queue behind
each other**, so "measure, fix one, re-measure" is not merely good practice here,
it is the only method that terminates.

### …and then the expensive rollup units took the derived workers (#190)

#189 worked — sealed day-wide derived units started winning claims for the first
time (`2a39bd83`, 2026-08-17, width 1440). But the rate was **8 derived claims
in 25 minutes, 2 of them sealed**, which is 145 hours for the ~700-unit backlog.

Wall clock again, not attempts. The cycle already gives `DerivedRollup` 2 of 10
slots, but a worker that picks HotPacking is gone for 578s and one that picks a
sealed BaseRollup for 801s. **This is #176's finding one level below where #176
applied it:** that change freed workers from *debt* for "the rollup chain", but
the chain is not uniform — BaseRollup's sealed day units are as expensive as the
debt they replaced, so they take the freed workers and derived starves behind
them precisely as it starved behind debt.

#190 holds 2 of 16 workers reachable only by derived work while coverage is
short. Derived is the cheap half: it aggregates the base tier and reads no raw
data.

### The one-line summary of the whole night

**Six defects, one shape.** Work was queued, every gauge said pending and
eligible, and something refused to run it:

| # | refusal | fix |
|---|---|---|
| 1 | capacity burned by units that time out | #181 |
| 2 | 5s backoff spinning forever on unsatisfiable slices | #182 |
| 3 | dependency gate demanding journal records history lacks | #184/#185/#186 |
| 4 | admission ceiling permanently closed, disabling the whole planner | #188 |
| 5 | frontier outranking sealed derived work on every claim | #189 |
| 6 | expensive rollup units taking the workers freed for cheap ones | #190 |

Each was invisible until the previous was removed. They queue behind each other
rather than appearing in parallel, so **measure → fix exactly one → re-measure**
is not just good practice here, it is the only procedure that terminates. Every
one was found the same way: diff two `timefusion_stats` snapshots for what is
NOT moving, then histogram `maintenance_task_started` by `operation`, by
`attempts`, and by slice width.

### Final state, 2026-08-19 ~01:00 UTC

**Maintenance (G3): fixed.** Stalled → functioning.

| metric | before | after |
|---|---|---|
| `tasks_complete` | +0.67/min | **+84.9/min** |
| `rollup_output_rows_total` | +5.7/min | **+259,702/min** |
| unit timeouts | 47/hour | **0** |
| claims to fresh work (attempts 0-1) | 18% | **61%** |
| starts at 100+ attempts | 134 per 90min | **7 per 20min** |

**Memory/OOM (G4): trending right.** Container RSS 25.4 GiB → **15.2 GiB**, and
**no `exit 137` since these changes began** — every shutdown in the window is a
clean deploy restart, against two OOM kills in the seven hours before.

**Queries (G2): improved where coverage exists, NOT met for shipbubble.**

| query | before | after |
|---|---|---|
| `94c5dc1f` 14d (17 days of 1h tier) | 9.8s | **6.0s** |
| `94c5dc1f` 30d | — | **13.1s** (completes) |
| shipbubble 1d / 7d / 30d | timeout | **still timeout** |

So a project WITH coverage now answers a 30-day dashboard query in 13s instead
of failing. shipbubble does not, and the reason is specific and known: its base
1m tier holds only **14 of 30 days**, so there is nothing for the cheap derived
path to aggregate. Closing it needs ~16 sealed-day BaseRollup units — the
expensive raw-scan kind, ~800s each — which is hours of drain now that capacity
exists, and is queued.

**The honest headline: the goal is not met.** What was achieved is that the
machine that builds coverage went from producing nothing to producing 260k
rollup rows a minute, and every defect between here and the goal is now named,
measured, and either fixed or written down below.

### The seventh: the missing days were never enqueued (#191)

With every other defect fixed, the derived backlog drained (634 → 601) and
**every one of 132 derived claims in the window was a frontier HOUR. Zero sealed
days.** Not because they lost the claim — #189/#190 fixed that — but because
there was no historical work left in the journal at all.

#188 stopped the ceiling disabling the whole pass, but it still deferred the
ENQUEUE, and the live frontier alone holds the journal at ~43,000 against the
25,000 ceiling, *replenished by ingest*. Waiting for it to fall is waiting
forever. The days shipbubble is missing had never been enqueued and could not be.

#191 makes the ceiling bind only when coverage is healthy. Bounded and
self-limiting in both directions: at most 24 cells per pass, contiguity ordering
aims them at the worst project's earliest hole, and coverage reaching
`COVERAGE_SHORT_DAYS` restores it.

**Seven now, same shape, still queueing behind one another.** Capacity → backoff
→ dependency → admission-of-the-pass → priority → occupancy → admission-of-the-
work. The list in §"one-line summary" should be read as evidence for the method,
not as a claim that it is finished.

### Routing measured properly (2026-08-19 01:30 UTC) — partial coverage buys NOTHING

All seven fixes live (`60fc0b0`). #191 confirmed working: `backfilled=5`,
`queued=2 remaining=0` — the planner is enqueueing past the ceiling for the
first time. Frontier healthy (`eligible_watermark_lag_seconds` = 0).

**Do not test routing with `EXPLAIN`.** It does not exercise the router — every
window width on a covered project showed zero tier references in the plan while
`rollup_hits_full_total` was climbing +84/min from real traffic. Test by
snapshotting `rollup_hits_*` / `rollup_miss_*`, running ONE query, and diffing.

Done that way, on `94c5dc1f` (17 days of 1h tier) with a 30d query:

```
rollup_miss_not_built_total   13 -> 143   (+130)
rollup_hits_full / hybrid      0 ->   0
                                            ...and the query timed out at 60s
```

`covered.is_empty()` for that project, i.e. it produced no covered range from
EITHER the date-level map or the slice map. Boot recovery is not the culprit —
`rollup_coverage_recovered` re-adopted **8,589** slices for `otel_logs_and_spans`.
The 1h tier holds 17 days of *data* whose *coverage* is not registered, which is
consistent with those files predating tagging (`maintenance_rollup_untagged_input`
fires continuously against the 1m tier).

**The consequence is a planning rule, and it is the most useful thing learned
tonight:** a 30d query needs the whole window; 17 of 30 days yields a full raw
scan, not a 17/30 speed-up. **Coverage must be built DEPTH-FIRST per project.**
Breadth-first spreads days across projects and buys exactly nothing until some
project crosses 30 — which retroactively justifies the contiguity-targeted
ordering (§6) that has still never actually run on prod.

It also means the untagged-legacy interaction deserves a check: `missing_tiers`
is computed from partition PRESENCE, so a day holding untagged (hence
unrecoverable) tier data counts as covered and is never rebuilt — which would
make those days permanently unroutable. Not yet confirmed; it is the first thing
to test next.

### The eighth: one tier's queued unit vetoed every other tier (#192)

02:00 UTC. All of #181-#191 live, and the derived tier STILL could not be
planned:

- backfill planner: `queued=2 remaining=0`
- `94c5dc1f`: **34 days of 1m tier against 17 of 1h**
- **zero** sealed-day derived claims in 25 minutes; all **84** were frontier hours

No historical derived task had ever been *created* — which is why #186's proof,
#189's reservation and #190's worker reserve all appeared to do nothing. They
were competing over a population that did not exist.

`blocks_rollup_backfill` is keyed on the DAY. But a day is not one unit of
rollup work — it is one per tier plus a dedup, and they are independent: the 1h
tier's unit reads the 1m TIER. Keyed that way, one pending ten-minute frontier
BaseRollup slice vetoed every tier of that whole day, and with ~47,000 pending
BaseRollup tasks that vetoed essentially every day.

The code comment above it already records this exact bug being fixed once —
unrelated file debt disqualifying a day forever. Narrowing from "any task" to
"any rollup task" was not narrow enough. Now keyed on
`(project, date, physical_table)`.

**Pattern note, and it is the useful one:** three consecutive fixes (#186, #189,
#190) targeted a queue that was empty. Each was individually correct and none
could have worked. **Before fixing how a queue is served, verify the queue has
the items you think it has** — `pending_derived_rollup` was ~900 the whole time
and every one of them was frontier work.

### The ninth, and the one that was actually blocking G1: empty partitions read as coverage (#193)

02:30 UTC, #181-#192 all live:

```
94c5dc1f  1m tier  34 CONTIGUOUS days   2026-07-17 .. 08-19
94c5dc1f  1h tier  18 days, missing     2026-08-01 .. 08-13
backfill planner   queued=1 remaining=0
```

The planner could not see a single one of those days as missing.

`partitions_of` lists FILES, and a rollup unit that aggregated nothing still
commits one. Pre-#169 that was the normal outcome for history — a derived unit
dropped base files whose slice tags it could not read and published rows=0,
which that path's own comment describes as reading *"exactly like this slice is
genuinely empty"*. The zero-row partition then made the day look **covered**, so
it was never rebuilt, so it stayed empty. Permanently. A closed loop that could
not be entered from outside.

**This retro-explains the previous four fixes.** #186 (proof), #189 (sealed
reservation), #190 (worker reserve) and #192 (per-tier veto) were each correct
and each entirely inert, because they all govern how queued work is SERVED and
the work was never QUEUED — the holes were invisible to the planner.

**The generalised lesson, which is worth more than the fix:** *presence is not
completeness*. This codebase has now been bitten by that exact substitution three
times in one night — `partitions_of` counting an empty file as coverage,
`missing_tiers` treating one file as a built day, and `blocks_rollup_backfill`
treating one queued task as a planned day. Any predicate that answers "is X
done?" by asking "does something for X exist?" should be treated as suspect on
sight.

**Diagnostic rule earned here:** when a fix that should obviously work does
nothing, stop fixing and go count the population it acts on. Four fixes in a row
were validated against `pending_derived_rollup` ≈ 900 without once checking that
those 900 were all frontier work.

### The census paid for itself in one pass (#194 -> #195)

03:00 UTC, first census reading:

```
source=otel_logs_and_spans  cells_missing=264  cells_wanted=0  defer_enqueue=false
source=otel_metrics         cells_missing=215  cells_wanted=0  defer_enqueue=false
```

The planner sees **every** hole. **Every** one is vetoed as already-queued. So
the tasks exist and are pending, and the question was never "why can't the
planner see the work" — it was "why is queued work never claimed". That is the
opposite of what four earlier fixes assumed, and no measurement available before
#194 could distinguish the two.

The answer followed immediately: #186's proof is keyed on one exact **day-wide**
`TaskKey`, and prod's queued work is hour-wide. `invalidate` mints derived units
at `DERIVED_SLICE_MICROS` (one hour), and `coarsen_sealed_slices` will not fuse
a day whose day-wide unit already exists in ANY state — including `Complete`,
which is precisely what a legacy rows=0 publication leaves behind. So such a day
holds hour-wide PENDING tasks under a COMPLETED day-wide one, and the proof
landed on the completed task, which is never claimed. #195 makes the proof a
property of the day at any width.

**This is the argument for buying observability mid-investigation.** #194 changed
no behaviour and cost one deploy; it then resolved in a single pass a question
that four behavioural fixes had each answered wrongly. When consecutive correct
fixes produce no effect, the next change should be a measurement, not a fifth fix.

### State at 04:30 UTC — frontier caught up, historical backfill still blocked

**Frontier: DONE.** `eligible_watermark_lag_seconds` = 0 and stable for hours.
New data is processed in real time; that half of "keeps up" is achieved.

**Historical backfill: still blocked, cause now narrowed to one question.**
#195 (day-scoped proof) did not unblock it either: over 20 minutes, zero
`derived_base_tier_proven` events and every derived claim was a frontier hour
for today. The census pair now in place answers the remaining question directly —
#194 said the planner SEES all 475 missing cells and all are already queued;
#196 reports, of the pending derived tasks, how many are unproven vs quarantined
vs not-yet-due vs sealed. One of those four is the answer.

**Scope, correctly stated.** The work left for the 30d goal is **475
(project, date) cells** — 260 `otel_logs_and_spans` + 215 `otel_metrics` — NOT
the 80,098 `tasks_pending`. That queue is dominated by self-replenishing frontier
work and has gone 47k -> 55k -> 61k while the system got healthier; reading it as
a backlog to drain is what produced two wrong throughput models earlier in this
plan. **Never quote `tasks_pending` as remaining work.**

**Estimate, labelled as such:** derived cells are cheap (they read the built 1m
tier, no raw scan), so ~2-4h once claiming is fixed; shipbubble needs ~15 base
day-units at ~800s on top, so 4-8h and it lands last. These are projections from
unit cost — historical derived units have never once run, so there is no measured
rate to quote yet.

### Still open at hand-off, in priority order

1. **The frontier queue starves the sealed backfill.** `pending_base_rollup` is
   ~42,000 against roughly 420 day-wide sealed units (14 projects × 30 days), so
   the overwhelming majority are 10-minute frontier slices. `claim_next`'s sealed
   reservation drops to **one claim in four** while
   `eligible_watermark_lag_seconds` exceeds its budget — and it is at 2,000s and
   rising — so exactly when coverage needs sealed work most, it gets the least.
   This is the next thing to attribute.
2. **`candidates` is not filtered to `active_projects`.** The backfill takes
   every source partition in the horizon, while `active_projects` is computed
   right beside it and used only for the gauge. Since
   `backfill_cells_by_contiguity` orders by run length ASC, dormant tenants sort
   first. The same artifact that makes `rollup_min_contiguous_days` read 0 also
   steers the work queue.
3. **The read path** (Phase 5) is untouched and is half of G2: `GatedScanExec`
   at `permits=0`, and 430 hot file groups for a one-day window.

---

## 6. Phase 2 — aim the backfill at the goal (enqueue control)

**IMPLEMENTED 2026-08-18** as `backfill_cells_by_contiguity` (`database.rs`,
regression guard `backfill_ordering_fills_the_worst_projects_earliest_hole_first`):

1. **Contiguity-targeted ordering.** Each 60s pass now admits the worst
   project's earliest holes first — (run length ASC, hole distance ASC) —
   instead of uniformly newest-first. This orders ADMISSION; execution order
   in the journal remains `claim_next`'s. With rollup units measured at ~3s
   and the restart-driven growth fixed, admission cadence (24 cells/pass) was
   the binding constraint, so admission order is where the leverage is.
2. **Deferral at the goal.** While `coverage_is_short()`, cells for projects
   already at 30 contiguous days are not enqueued at all — the triage half of
   the horizon split. The full certification/routing horizon split is NOT
   done: the deferral achieves the triage without a new knob (§12: no
   flexibility knobs without a measurement).

Original sketch, for the record:

1. ~~Contiguity-targeted ordering~~ and 2. ~~triage~~ — as above.
   → verify: unit test pins hole-filling before depth-extending; on prod,
   `rollup_min_contiguous_days` rises within hours of the deploy, not days.

---

## 7. Phase 3 — the vertical slice (prove the chain on ONE project)

Before scaling anything, prove the whole path on one project — on staging
(Phase 0.3) first, prod second:

- **Subject:** shipbubble (named in the goal; mid-size; not the whale).
- **Work:** burn its 30 contiguous 1h-tier days for `otel_logs_and_spans`
  (base tier where missing, then derived). This is ~30–90 units — hours of
  drain, not days.
- **Then measure, in order:**
  1. `rollup_min_contiguous_days` contribution for shipbubble == 30
     (ground-truthed per Phase 1.3).
  2. A real shipbubble 30d dashboard query, timed, with `rollup_hits_full` /
     `hits_hybrid` / `misses` recorded.
  3. If >= 1s: the gap decomposition is already known — rollup leg vs live
     tail (§3). A slow rollup leg means coverage or routing is still broken;
     a slow tail means Phase 5 is the whole game. Either way the slice tells
     us WHERE the remaining time is before we spend fleet-wide effort.

→ **Exit criterion: a dated table — project, contiguous days, 30d query wall
time, hit/miss reason — for shipbubble.** This is the first time anyone will
have timed a 30d query against built coverage. If it is < 1s, the architecture
is validated and Phases 4/5 are scaling work. If not, this plan's later phases
get re-ordered by where the time actually is.

---

## 8. Phase 4 — unit cost and the frontier model (the binding constraints)

Ordered by the Phase 1.1 decomposition. Expected candidates, with the evidence
already pointing at them:

1. **Frontier work derived from a cursor, not enumerated units** (REQUIRED for
   G6; the §3 arithmetic shows 10x diverges 3.8x otherwise). Per stream, keep a
   durable high-water cursor (`rollup_done_through_micros`); the coordinator
   materializes at most one in-flight unit per stream per operation, covering
   `[cursor, now - FINALIZATION_DELAY)`. Crash recovery = cursor + WAL replay,
   which already guarantees the data. Creation becomes O(streams) per tick
   instead of O(streams × slices × ops). Design validated in the sim at 130
   synthetic projects before implementation starts.
   → verify: sim at 130 projects shows creation <= drain; on prod, frontier
   lag gauge unchanged after the switch.
2. **Whatever Phase 1.1 says the 400s is.** If object-store fixed cost: widen
   sealed units beyond a day where certification allows (fewer, larger units —
   the direction day-sizing already proved). If decode/sort: the memory-pool
   and spill paths are already the tuned ones; measure before touching. If
   commit: the shared-wave machinery (`commit_rollup_wave`,
   `database.rs:11337`) exists — check heavy units actually use it.
3. **`claim_next` index.** O(n) per claim per worker over the task set
   (`maintenance_coordinator.rs:697`) was already rewritten once under
   pressure; at 10x (n ~ 300k) it needs an index keyed by
   `(class, operation, eligibility)`. Do this WITH the cursor change, since the
   cursor shrinks n for the frontier and the index serves the sealed journal.
   → verify: claim latency p99 in `timefusion_stats`; no starvation regression
   (sealed reservation tests stay green).

**Not in this phase:** raising `MAX_DECODED_BYTES` or the dedup decoded budget.
That decode is explicitly outside the memory pool; raising it trades CPU for
untracked heap — the exact shape of the August OOM series.

---

## 9. Phase 5 — the read path (the other half of G2)

1. **Resolve the hot-tier exclusion soundness question, then relax the gate.**
   `plan_leg` only excludes a bucket from the Delta leg when one file both
   covers the window and is the bucket's ONLY file — because `demote` can be
   skipped under queue pressure (`demote_skipped_total` = 20 in prod), a
   double-drained bucket can hold one file claiming `covers_window` over a span
   it half holds. Step 1 is answering, from the code: *can a bucket drain twice
   with a demote skip between?* If yes, `covers_window` must carry the drain's
   own completeness (a generation/drain-id in the file metadata), not the
   caller's constant. Only then relax to "all files of the bucket served and
   all complete". A slow dashboard is visible; an under-counted one is not —
   this ordering is not negotiable.
   → verify: the shipbubble 1-day tail goes from ~6s toward the rollup leg's
   ~139ms; `hot_tier.unproven_windows_total` / `read_hits_total` ratio falls
   from ~72%; **zero parity failures** in the rollup-vs-raw parity suite.
2. **Group-by-covered-set routing legs.** Coverage is intersected across
   projects, so one thin project voids a cross-project window for everyone
   (#166 split covered/raw legs; the follow-up — one rollup leg per distinct
   covered-set with `project_id IN (...)`, bounded by the existing 32-branch
   check — was never built). Without it, G2 for cross-project dashboards
   depends on the WEAKEST project, forever.
   → verify: a cross-project 30d query with one uncovered project still routes
   the covered projects (`rollup_hits_hybrid` rises, no corresponding
   `misses`); parity tests for the split legs.
3. **Kill the two known OOM shapes** (G4):
   - *Service-map self-join* (handover §6.4): monoscope's
     `dispatchServiceMapRollups` fans out a 6-reference CTE (hash self-join +
     two anti-joins) per project per 5-min bucket, concurrently; DataFusion
     inlines the CTE, and the scan guard structurally cannot see it. Two
     independent fixes, either sufficient: bound the job's concurrency in
     monoscope (one line, safe — it is a background job), and/or materialize
     `sp` once. TF-side hardening: per-query peak-memory attribution in
     `timefusion_stats` so the NEXT join-shaped OOM is visible before the
     oom-killer fires.
   - *The 32.8 GB `SELECT metric_name, count(*) ... GROUP BY metric_name`*
     full-history scan: answer distinct metric names from metadata/statistics,
     or require a time bound. This query shape should be refused or
     rewritten, not survived.
   → verify: no `exit 137` for 7 days (G4); the two query shapes above are
   measurably cheap or measurably refused.
4. **Realtime tail, deliberately.** After Phase 1.5 the realtime-tail path is
   either load-bearing-by-design (documented, tested on restart) or replaced.
   → verify: restart-then-route test in the suite.

---

## 10. Phase 6 — tantivy to zero (G5)

Gated on the same maintenance capacity; do not start until G3 holds
(frontier lag < 600s sustained) or it will steal from coverage. 3,031 uncovered
files at handover. The oversized-file unblock (#142) already landed; what
remains is scheduling throughput.

→ verify: `tantivy_uncovered_files` = 0 and stays 0 for 48h.

---

## 11. Phase 7 — 10x readiness check (G6)

Arithmetic and simulation, not prod. At 130 projects × 2 sources, recompute §3
with the cursor-based frontier and the indexed `claim_next`, and confirm the
sim agrees:

- creation <= drain by construction (not by coincidence, as 2026-08-17's
  ~11,700 created vs ~12,000 retired was);
- journal size bounded by sealed work only; `BACKFILL_PENDING_CEILING` still
  meaningful;
- the maintenance memory pool still fits `coordinator_jobs × 512 MiB`
  (`config.rs` DerivedBudget asserts this per box; re-run the derivation at
  prod's actual limit).

If the box cannot hold both the query pool and 16 × 512 MiB of maintenance at
10x ingest, the honest lever is the existing `BudgetProfile::MaintenanceCli`
split — a maintenance-only process on the same server, memory-partitioned —
which the codebase already budgets for. That is a decision to take WITH the
Phase 1 numbers, not now.

---

## 12. Explicitly not doing

- **No slot-mix reweighting** (#167: proved non-binding).
- **No lowering the dedup byte estimate** (#157: it is accurate; lowering it
  OOMs).
- **No unsharding the non-streaming dedup branch** without a deadline that
  matches its measured cost (#161/#172 lesson, generalized: never change a
  unit's time structure without re-deriving its deadline from measurement).
- **No ingestion-time partial aggregation** (excluded in the 08-15 redesign:
  not idempotent under WAL replay, not invertible under merge-on-read).
- **No prod-deploy-per-hypothesis.** If a question can be answered by the sim,
  `run-unit`, or staging, it must be (Phase 0).
- **No "flexibility" knobs.** Every constant above was earned by a measurement;
  new ones need one too.

---

## 13. How to tell this is working

Per-phase exit criteria are inline. Globally, in order:

1. `rollup_min_contiguous_days` climbs and reaches 30 (G1) — ground-truthed
   once (Phase 1.3), then trusted.
2. A timed 30d dashboard query per active project, p50 < 1s, with
   `rollup_hits_full`/`hits_hybrid` and no raw fallback (G2) — the shipbubble
   slice first (Phase 3), the fleet after Phase 4.
3. `eligible_watermark_lag_seconds` < 600 sustained (G3); enqueue-by-source
   counters show the planner is no longer the dominant source.
4. Zero `exit 137` for 7 days (G4).
5. `tantivy_uncovered_files` = 0 (G5).
6. The 10x arithmetic closes by construction, confirmed in the sim (G6).

**Operating rules while executing this plan:** prod is the last resort, not the
loop — sim, `run-unit`, and staging first (Phase 0); one change per deploy;
>= 2h quiet before believing a throughput number; never run an unbounded
ad-hoc query against the largest tenant; every PR states which goal metric it
moves and how that will be measured — a PR that cannot name one does not merge.

---

# PART II — Catching every historical backlog up with today

**Written 2026-08-19 ~07:00 UTC**, after thirteen changes shipped overnight
(#181–#197). Part I above is the record of getting maintenance to *run at all*.
This part is the plan to get every class *caught up*, and it starts by
correcting two framings in Part I that the measurements do not support.

## 14. Where every class actually is

Measured on prod over a 450s rate window at 04:45 UTC, image `9d7ac74`:

| class | pending | rate | ETA |
|---|---|---|---|
| **Hot packing** (today's compaction) | 2,654 | **−17.5/min** | **~2.5h** ✅ |
| **Sealed consolidation** (historical compaction) | 2,218 | −0.27/min | ~5.7 days ⚠️ |
| **Dedup** | 12,955 | −0.27/min | ~33 days ❌ |
| **Repair** (sorting / footers) | 584 | **+0.13/min** | not converging ❌ |
| **Derived rollup** | 619 | −0.27/min | blocked (mid-fix) |
| **Tantivy index** | 4,761 uncovered | growing (3,031 at handover) | ❌ |

Byte-level debt, **growing**:

```
backlog_bytes                 38.4 TB   +291 MB/min
sealed_compaction_debt_bytes   8.18 TB   +34 MB/min
dirty_bin_queue_depth          8,105     +1.2/min
oldest_task_age_seconds      241,526     (2.8 days)
```

Frontier, by contrast, is **caught up**: `eligible_watermark_lag_seconds` = 0,
stable for hours. Live data is processed in real time. That half of "keeps up"
is achieved and is not at risk.

### The claim mix explains all of it at once

60 minutes of `maintenance_task_started`, by operation and slice width:

```
299  BaseRollup           10-min frontier
 34  HotPacking           10-min frontier
 23  DerivedRollup        hour frontier
 21  SealedConsolidation  day  (historical)
 18  Repair               day  (historical)
  7  Dedup                10-min frontier
  1  BaseRollup           day  (historical)
```

**~87% of every claim is frontier work.** The historical classes are not broken —
sealed consolidation and repair each run ~20/hour — they are simply outnumbered,
and new debt arrives about as fast as they retire it. This is §3's arithmetic
arriving on schedule: each stream mints 144 Dedup + 144 BaseRollup units per
day, so at 26 streams ~8,100 frontier units/day must be claimed before anything
historical gets a turn.

## 15. Two corrections to Part I — the problem is much smaller than stated

### 15.1 Fragmentation is in the last ~3 days, NOT across history

Part I asserted "~1 MB files against a 256 MB target, two orders of magnitude
off", generalised from scan-time file counts. Listing the object store directly
says otherwise:

| partition | objects | size | avg file |
|---|---|---|---|
| `00000000` 2026-08-05 | **1** | 251 MB | ✅ |
| `98fdd4f3` 2026-08-05 | **1** | 190 MB | ✅ |
| `be87ebc1` 2026-08-10 | **1** | 137 MB | ✅ |
| `87576849` 2026-08-05 | **1** | 244 MB | ✅ |
| `28f62f01` 2026-08-05 | **2** | 997 MB | ✅ |
| `28f62f01` 2026-08-16 | **602** | 5.47 GB | ❌ |
| `28f62f01` 2026-08-17 | **971** | 5.74 GB | ❌ |
| `28f62f01` 2026-08-18 | **722** | 2.23 GB | ❌ |
| `00000000` 2026-08-18 | **705** | 872 MB | ❌ |
| `87576849` 2026-08-18 | **454** | 1.69 GB | ❌ |

**Every sealed day older than ~3 days is already a single file.** Historical
compaction is, for these projects, *done*. The fragmentation is a rolling
~3-day tail.

The consequence for G2 is large and was previously mis-stated: a 30-day query
reads ~27 compacted days (~27 files) plus ~3 fragmented days (~2,300 files).
**The recent tail is ~98% of the file count.** So *hot packing* — the class that
is already converging at −17.5/min — is the query-latency lever, and sealed
consolidation is not. The earlier "337 files per scan" figure was measured on
recent windows and generalised to history it does not describe.

**Therefore `pending_sealed_consolidation` = 2,218 and
`sealed_compaction_debt_bytes` = 8.18 TB need auditing before being worked:**
if the days they name are already single-file, they are stale tasks, and
draining them is a no-op that costs capacity the other classes need. Audit
first, drain second.

### 15.2 `tasks_pending` is not a backlog

`tasks_pending` went 47k → 55k → 61k → 80k while the system got measurably
healthier. It is dominated by self-replenishing frontier work. **Never quote it
as remaining work**; doing so produced two wrong throughput models in Part I.
The bounded, goal-relevant number is **475 (project, date) cells** — 260
`otel_logs_and_spans` + 215 `otel_metrics` — from #194's census.

## 16. Is the implementation faulty, or just behind?

Both, in separable ways.

### 16.1 Faulty: the frontier mints durable work proportional to time × streams

`invalidate()` creates one durable task per 10-minute slice per operation per
stream. That is 312 tasks per stream-day, ~8,100/day at current scale, each
requiring a claim, a journal write, and an object-store round trip whose cost is
**independent of how little data the slice covers**. The unit exists so a crash
cannot lose work — but the WAL already guarantees the data. The unit is only a
trigger, and a trigger does not need to be durable, enumerated, or per-slice.

This is the single structural fault. Everything in §14 is downstream of it.

### 16.2 Faulty: `dependencies_complete` asks the journal a question about data

It requires COMPLETE `BaseRollup` *tasks* to prove a base tier exists. Journal
bookkeeping is not the authority on what is in the tier; coverage is. Four
changes (#184, #186, #195, #197) were needed to route around this, and only the
last — keying the fact on the DAY and reading it from coverage — is robust.
**Generalise it: no scheduler predicate should answer a question about data by
consulting bookkeeping.**

### 16.3 Faulty: presence substituted for completeness, three times

`partitions_of` counted an empty file as coverage; `missing_tiers` treated one
file as a built day; `blocks_rollup_backfill` treated one queued task as a
planned day. Any predicate answering "is X done?" with "does something for X
exist?" is suspect on sight.

### 16.4 Not faulty, just outnumbered

Sealed consolidation, repair and dedup all work. They run ~20/hour each and
complete. They do not converge because 87% of claims go to the frontier. Fixing
them is a *scheduling share* problem, not a correctness problem — and the share
cannot be fixed by reweighting the cycle, because #167 proved attempt share and
slot-time share differ by two orders of magnitude when unit costs differ by two
orders of magnitude.

## 17. What everyone else does (and where we differ)

| system | frontier trigger | compaction | dedup / upsert | rollups |
|---|---|---|---|---|
| **Druid** | segment handoff per time chunk; no per-slice durable task | background **auto-compaction** picks intervals by a *policy scan* over segment metadata | none at read; dedup at ingest via rollup key | ingestion-time rollup + optional reindex |
| **ClickHouse** | `MergeTree` background merges triggered by *part count*, not by time | continuous, level/size-tiered; merge selector scans parts | `ReplacingMergeTree` collapses on merge | materialized views maintained on insert |
| **Iceberg / Delta** | no frontier concept; `OPTIMIZE`/`rewrite_data_files` is an explicit job | operator-scheduled or auto, selects by *file size histogram* | `MERGE`/deletion vectors at write | downstream jobs (Spark) on a schedule |
| **BigQuery / Snowflake** | opaque; storage engine merges micro-partitions continuously | continuous, size-driven | primary-key merge at write | materialized views with incremental refresh |
| **TimeFusion (today)** | **one durable task per 10-min slice per op per stream** | day-sized units planned per (project, date) | dedup as a scheduled unit + read-side `DedupExec` | rollup tiers as scheduled units with a dependency graph |

**The pattern everywhere else: compaction is driven by a cheap scan over
metadata that selects work by file-size/count, and it is *stateless* — there is
no durable queue of pending compaction tasks.** ClickHouse's merge selector and
Druid's auto-compaction both re-derive what to do from current state on every
tick. Nothing accumulates; nothing can be stale; a restart loses nothing.

TimeFusion instead maintains a durable journal of enumerated future work. That
is what produced 80k pending tasks, tasks 2.8 days old, stale tasks for days
that are already compacted, and nine separate ways for work to be queued but
never claimed. **The industry-standard design would have made most of Part I's
bugs unrepresentable.**

Where TimeFusion is *right* to differ: rollup tiers with a dependency graph is a
real requirement (Druid's ingestion-time rollup is not invertible and this plan
already rejects that at §12), and read-side dedup is needed because we accept
late/duplicate writes. Those stay.

**The recommendation is therefore not "become ClickHouse", it is: keep the
durable journal ONLY for work that is genuinely stateful (rollup tiers, whose
dependency and generation identity must survive restarts), and make file hygiene
— HotPacking, SealedConsolidation, Repair — stateless and re-derived per tick
from the file list, exactly like `plan_compaction_debt` already does for
enqueueing.** It already scans the real file list every 60s; it just then writes
the answer into a durable queue that can go stale. Delete the queue, keep the
scan.

## 18. Phase A — local replication and profiling (do this first)

Nothing below should be evaluated on prod. The whole of Part I was diagnosed at
~20 minutes per deploy cycle; the measurements in §15 took minutes because they
read object storage directly. Build the local loop before changing behaviour.

**A.1 — Corpus.** Copy a debt-shaped slice from prod to local MinIO. Sizing is
now known, so this is cheap and targeted:

```bash
# the fragmented tail (the real problem): ~5.5 GB, 971 files
aws s3 sync s3://$BUCKET/timefusion/otel_logs_and_spans/project_id=28f62f01…/date=2026-08-17/ \
            s3://local/…  --endpoint-url $OVH
# a compacted control: 1 file, 244 MB
aws s3 sync …/project_id=87576849…/date=2026-08-05/ …
# plus the _delta_log for both tables
```
Total corpus ≈ 15 GB, which fits locally and reproduces both the fragmented and
the healthy shape.

→ verify: local TF serves a 1-day query over the copied partition.

**A.2 — BUILD `sim` and `run-unit`. They do not exist.**

§4 marks both "✅ **Landed**" with file paths and CLI flags. They are not on
master, and they are not on any branch: a search of every remote ref for a `sim`
or `run-unit` subcommand in `main.rs` returns **nothing**. `src/maintenance_sim.rs`
resolves on a handful of stale June `claude/*` branches but is empty.

This is the most consequential inaccuracy in Part I, and it is self-explaining:
**the entire overnight session ran on ~20-minute prod deploy cycles precisely
because the tooling that was supposed to make that unnecessary had never been
written.** Nine of the thirteen changes were diagnosed by deploying to
production and reading logs. §4's own opening line — "No design decision in this
plan should require a prod deploy to evaluate" — was never true.

Treat every other "✅ Landed" claim in §4 as unverified until checked against
master. Build `run-unit` first: it answers "where do the 800 seconds go" in
minutes, and no class heavier than a rollup has EVER been profiled.

→ verify: `timefusion run-unit --op sealed --project 28f62f01 --date 2026-08-17`
prints a scan/stage/commit decomposition.

**A.3 — Profile the three heavy classes on the local corpus.** SealedConsolidation
(271–767s), HotPacking (289–895s) and Repair are all unprofiled — nobody has
ever decomposed where their time goes. Do it with the pprof sampler already in
the binary.

→ verify: a flamegraph per class, and a stated per-unit cost model
(fixed cost vs per-file vs per-byte).

**A.4 — Replay harness.** With A.1–A.3, a policy change is evaluated in ~10
minutes locally instead of ~40 on prod (build 18 + deploy + boot + a quiet
window). That is the number that decides how many iterations remain possible.

## 19. Phase B — make the frontier stop crowding everything out

The claim mix (§14) says this is the whole game. Two options, in increasing
order of change:

**B.1 — Widen frontier units from 10 minutes to 1 hour.** Cuts frontier unit
creation ~6x immediately. Cost: the live tail a hybrid rollup query must scan
grows from ~10 min to ~1 h, and `raw_tail_duration_secs` is paid by every such
query — so this trades query freshness for maintenance capacity, and must be
measured, not assumed. **Validate in the sim (A.2) before shipping.**

**B.2 — Cursor-based frontier (§8.1's Phase 4.1).** Per stream keep a durable
high-water cursor; materialise at most one in-flight unit per stream per
operation covering `[cursor, now − FINALIZATION_DELAY)`. Creation becomes
O(streams) rather than O(streams × slices × ops), and the 10-minute granularity
is preserved because the unit's *width* is decoupled from its *frequency*.
This is the correct answer and matches how every system in §17 works.

→ verify (both): claim mix inverts — historical classes reach ≥50% of claims;
`eligible_watermark_lag_seconds` stays < 600.

## 20. Phase C — make file hygiene stateless

Per §17. `plan_compaction_debt` already scans the real file list every 60s and
knows exactly which (project, date) have small or unsorted files. Stop writing
that answer into a durable queue:

- Compute the work set per tick; claim directly from it.
- No `pending_hot_packing` / `pending_sealed_consolidation` / `pending_repair`
  to go stale, to be 2.8 days old, or to name days that are already one file.
- A restart re-derives everything; nothing is lost, nothing is duplicated.

This deletes an entire class of the bugs in Part I by construction, and it is
the smaller half of the work — the scan already exists.

→ verify: the three pending gauges are replaced by "partitions currently out of
policy", which by definition cannot exceed the number of partitions.

## 21. Phase D — audit before draining

Before spending capacity on the 2,218 sealed-consolidation and 12,955 dedup
tasks, establish how many name partitions that are *already* compliant (§15.1
suggests most). A stale task drained is capacity stolen from a real one.

→ verify: a count of pending tasks whose partition already meets policy. If it
is the majority, Phase C subsumes this entirely.

## 21b. Phase D executed — the results, and the treadmill it exposed

**#198 retired 4,290 stale hygiene tasks in one pass** (2,646 + 1,644).
`pending_hot_packing` collapsed **2,654 → 70**: today's compaction is done.

Sealed consolidation barely moved (2,218 → 2,130), which was the interesting
part. It is **not a backlog — it is a treadmill**, and the Delta log says why.
Over 381 commits of the prod log:

```
add actions: 1,648    untagged: 1,593 (96.7%)    tagged: 55, all delta-rs.optimize.sort_by
```

Only the OPTIMIZE path tags its output. `plan_compaction_debt` admitted a
partition when any file was `!is_sorted_run()`, so **every partition holding a
flush-written file — which is every partition — was permanently out of policy**,
and ingest recreated the condition faster than consolidation cleared it.

The codebase already knew. From `repair_verified_sorted`'s own comment: *"the
flush path sorts and stamps a correct footer WITHOUT the tag, so an untagged
file is only a suspect… admission by tag alone would rewrite a healthy 716 MB
file for nothing."* Repair honours that and footer-checks. Consolidation did
not, and treated suspicion as proof.

**#200** gives consolidation file SIZE — unambiguous, and what the word means —
and leaves sortedness to Repair, which verifies it. Expected effect: sealed
consolidation pending falls from ~2,130 toward the **108** the object-storage
audit found.

**Generalises the §16.3 lesson:** three cases of *presence* substituted for
*completeness*, and now a fourth of *absence-of-a-tag* substituted for
*absence-of-the-property*. Both are the same error — reading a cheap proxy as
the fact — and every one of them created work that could never be finished.

### Coverage machinery: all of it now live and confirmed

```
base_tier_ready = 374   dependencies satisfied from real coverage (#197)
tier_holes      = 318   holes ranked ahead of re-derives          (#199)
derived_sealed  = 485   the historical work itself
derived_not_due = 504   post-restart backoff; decays
```

Every known blocker on the derived tier is now addressed: dependency (#197),
priority (#189), occupancy (#190), admission (#191/#192), visibility
(#194/#196), and ordering (#199). What remains is throughput once the deadlines
mature — which needs a QUIET WINDOW to measure, not another change.

**Stopping deploys here.** Seventeen changes have shipped in one session and
every deploy resets the counters that would show whether they worked; §12's
"≥2h quiet before believing a throughput number" has been violated repeatedly
out of necessity and should now be honoured.

## 21c. What else is missing from the Delta log, and whether it matters

Audited 2026-08-19 by reading the logs directly rather than trusting counters.

| metadata | source table | 1m tier | 1h tier | verdict |
|---|---|---|---|---|
| `numRecords`, `minValues`, `maxValues`, `nullCount` | **1,648/1,648** | present | present | ✅ fine |
| `min/max` on `timestamp`, `id`, `updated_at` | **1,648/1,648** | present | present | ✅ fine |
| `min/max` on `date`, `project_id` | absent | absent | absent | ✅ **correct** — partition columns live in the path; Delta omits them by convention |
| `delta-rs.optimize.sort_by` | 55/1,648 (3.3%) | 57/1,055 | 41/512 | ⚠️ only OPTIMIZE tags output — fixed by #200 |
| `timefusion.*` coverage tags | n/a | **998/1,055** | **471/512** | ❌ **see below** |

Two things are healthy and worth stating, because both had been suspected:

- **Statistics are complete.** Every add carries `numRecords` and timestamp
  min/max. So #193's `partition_file_is_empty` has the input it needs, and
  timestamp-based file pruning is not silently degraded. The earlier worry that
  missing stats might be defeating pruning is disproved.
- **The rollup tiers ARE tagged** — which is why `recover_rollup_coverage`
  re-adopted 8,589 slices. Coverage identity is being written correctly by the
  rollup path.

### The one that is NOT ok: consolidation strips coverage identity

The tagged counts are **disjoint**. In the 1m tier, 998 files carry the six
`timefusion.*` tags and 57 carry only `delta-rs.optimize.sort_by`; 998 + 57 =
1,055. Same shape in the 1h tier (471 + 41 = 512).

Cause, in `tag_sorted` on the rewrite output path:

```rust
let tag_sorted = |mut add: Add| {
    if sorted { add.tags…insert(SORTED_RUN_TAG, "true"); }
    Action::Add(add)
};
```

It writes the sort tag and **carries nothing forward**. So consolidating a
rollup-tier partition destroys that partition's `timefusion.source` /
`project` / `slice_start` / `slice_end` / `source_fingerprint` / `generation`.

**Why that is bad, and why it is worse than it looks:** `recover_rollup_coverage`
re-derives coverage from exactly those tags. A consolidated tier partition is
therefore no longer recoverable from storage — it survives only as long as the
journal's `published_rollups` does. And the interaction is perverse: **the more
successful compaction is, the more coverage identity it erases.** This session
has been pushing consolidation hard, which actively erodes the thing G1 is
trying to build. It is also the source of the constant
`maintenance_rollup_untagged_input` warnings, since derived units reading a
consolidated base partition fall back to timestamp-stats pruning (#169).

Currently **5.4% of 1m-tier files and 8.0% of 1h-tier files** are already
coverage-blind.

**The fix, and why it is not a one-liner.** The output of a consolidation spans
several input slices, so it cannot simply inherit one input's tags — claiming a
wider slice than was actually aggregated is the silent-wrong-number failure this
plan refuses elsewhere. The safe form: carry the tags forward only when every
input of the bin agrees on `TAG_GENERATION` and `TAG_SOURCE_FINGERPRINT` (which
they will, within one (project, date) partition of one tier), and set the
output's slice to the union of the inputs'. Where the inputs disagree, omit the
tags exactly as today. That is correct, conservative, and testable — but it is a
correctness change to coverage identity and belongs in a measured window, not
appended to a run of eighteen deploys.

**Generalised finding, now the fifth of its kind:** every one of these bugs is a
cheap proxy read as the fact it stands for — presence for completeness, an
absent tag for an absent property, and now a rewrite that preserves the proxy
(`sorted`) while dropping the fact (coverage identity). When a file is rewritten,
ask what the OLD file was evidence FOR, not just what it looked like.

## 21d. Result: file hygiene is caught up

Measured 2026-08-19 ~10:30 UTC with #198 and #200 both live:

| class | at 04:45 | now | note |
|---|---|---|---|
| `pending_hot_packing` | 2,654 | **66** | today's compaction done |
| `pending_sealed_consolidation` | 2,218 | **283** | matches the ~108-per-source audit |
| `pending_repair` | 584 | 575 | Repair now solely owns sortedness, as designed |
| `eligible_watermark_lag_seconds` | 0 | **0** | frontier still caught up |

**Sealed consolidation fell 87%** and landed where reading object storage said it
should. The two changes were complementary and neither alone was enough: #198
retired what was already done (4,290 tasks in its first pass), #200 stopped the
policy re-creating it (the sort-tag treadmill). Together they turned an
apparently-8.18 TB, never-converging class into a small finite backlog.

**What this validates about the method**, and it is the whole argument of Part II:
both fixes came from reading the storage layer directly — `aws s3 ls` over 24,637
objects, and 381 commits of the Delta log — not from any counter the process
exposes. The counters said 2,218 pending and −0.27/min forever. The storage said
877 of 1,033 partitions were already compliant. **When a queue and the world
disagree, the world is right.**

Remaining queues are `pending_dedup` (14,816) and `pending_base_rollup` (65,934),
both dominated by self-replenishing frontier work — see §19, which is the
frontier unit model and the last structural item.

## 21e. The derived tier is not blocked — it is correctly waiting

#205's probe ended a four-fix guessing streak in one reading:

```
derived_pending=621  derived_sealed=401  derived_quarantined=0  derived_not_due=480
derived_refusal="dependencies:87576849:2026-08-10"
```

`dependencies_complete` is refusing the whale's 1h unit for 2026-08-10 — **and it
is right to**. The whale's 1m BASE tier genuinely lacks that day:

```
whale 1m tier:  08-05 08-06 08-09 08-11 08-12 08-13 …   (no 08-07, 08-08, 08-10)
whale 1h tier:  08-13 08-14 08-15 …
```

You cannot derive an hour tier from a minute tier that does not exist. **The
derived tier is not blocked by a defect; it is waiting on the base tier**, which
is the correct behaviour and which four consecutive fixes (#197, #199, #202,
#204) were each trying to force past. Every one of them was a real bug —
`derived_quarantined` is now 0, `base_tier_ready` accumulates correctly, holes
outrank re-derives — but none could have produced a sealed derived claim,
because the thing they were unblocking was legitimately not ready.

**Restating the remaining work honestly:** it is BASE rollup backfill, the
expensive raw-scan kind, for the ~260 cells the census counts. The derived tier
then follows almost for free, since it reads the built base tier.

And that work has started. Base rollup claims over 12 minutes:

```
12 × DAY-wide  2026-08-18     sealed backfill
 6 × 10-minute 2026-08-10     history, reached for the first time
```

The 08-10 claims are #203's aging working — old days are finally being served.
But note the **granularity**: 10-minute slices, not day units. A day rebuilt at
frontier granularity is ~144 units instead of one, which is the enumerated-unit
problem of §16.1 showing up in the backfill path. `coarsen_sealed_slices` exists
for exactly this and is collapsing thousands of slices per pass, so these are
most likely freshly re-invalidated rather than un-coarsened — worth confirming
before treating it as a defect.

**What this changes about the goal.** "Every historical backlog caught up" now
decomposes cleanly:

| class | state |
|---|---|
| live frontier | **caught up** (lag 0) |
| hot packing | **caught up** (2,654 → 73) |
| sealed consolidation | **caught up** (2,218 → 266) |
| base rollup (history) | genuine compute, now running and correctly ordered |
| derived rollup | waiting on base, correctly |
| dedup / repair / tantivy | still outnumbered by frontier — §19 |

Nothing in the first three rows is a queue any more. What remains is arithmetic:
raw scans over ~260 partition-days, plus the frontier unit model (§19) which is
what keeps dedup and repair outnumbered.

## 22. Ordering, and the one-line rationale for it

1. **A** (local loop) — because every estimate below is currently a projection.
2. **D** (audit) — because it may delete most of the remaining work for free.
3. **C** (stateless hygiene) — because it makes three classes converge by
   construction and removes the staleness that D just measured.
4. **B** (frontier) — the largest and riskiest; it is the only one that needs a
   freshness trade-off decided, so it should be decided with A's numbers in hand.

**Explicitly NOT first:** more scheduler tweaks. Part I shipped nine of them;
the last four were each individually correct and jointly inert. The lesson,
stated once more because it is the expensive one: **when consecutive correct
fixes produce no effect, the next change should be a measurement, not a fifth
fix.**

---

# Part III — the per-unit constant (2026-08-19)

§22 said the next change should be a measurement, not a fifth fix. It was, and
the measurement invalidates §19's framing.

## 23. 72% of a rollup unit is the commit

Prod, 43 min uptime, 1,122 published base-rollup units. The four phase counters
added in #174 decompose exactly (unit count =
`rollup_rebuilds_full_total + rollup_rebuilds_incremental_total`):

| phase | total ms | per unit | share |
|---|---|---|---|
| scan | 1,247,675 | 1.11 s | 21% |
| stage | 393,498 | 0.35 s | 7% |
| **commit** | **4,200,744** | **3.74 s** | **72%** |
| end-to-end | 5,842,883 | 5.21 s | ✓ sums |

`rollup_commit_actions_total ≈ unit count` — **one Add action per commit**. A
3.74 s commit that writes one action is not the write. It is lock wait on the
per-`(project, table)` `commit_lock`, plus `refresh_table_snapshot` and
`swap_and_refresh_cache`.

That cost is **fixed per unit and independent of the span the unit covers**.

This reframes everything Part I did. Nine scheduler changes shipped; the last
four were correct and jointly inert. They could not have worked: scheduling
decides *who runs when*, and the constraint is a *per-unit constant*. The only
lever that touches a constant-per-unit cost is **units per unit of work** —
i.e. width.

`rollup_shared_commits_total = 0` looks like a wave-commit path that never
fires. It is not: `stage_rollup_wave` / `commit_rollup_wave` are documented dead
code from the retired cohort path. Don't re-chase it.

## 24. Why the width lever was jammed shut

Base slices are `NORMAL_SLICE_MICROS` = 10 min, so a sealed day mints 144 units
and pays 144 commits where one would do. `coarsen_sealed_slices` exists to undo
exactly this — and had dead-ended.

It was day-or-nothing: fuse a day's leftovers into one day unit, but only if the
summed estimate fits `MAX_DECODED_BYTES` (512 MiB). Over budget → the group was
dropped and the day kept all 144 slices.

That fallback is **inverted, not merely slower**. On an uncompacted sealed
partition every file spans the whole day, so timestamp-stat pruning skips
nothing and a ten-minute slice reads exactly the files a day unit would — prod
logged `scan_ms=481682 stage_ms=30 commit_ms=961 rows=142`. The rule answered
"too expensive to scan once" with "scan it 144 times".

**The signature to recognise it by:** `maintenance_sealed_slices_coarsened` logs
a short decreasing run (12, 6, 3) and then nothing for the rest of the process's
life, while `pending_base_rollup` sits at 84,834. A coarsener that looks
*converged* with an enormous queue behind it is this bug. That is the seventh
instance of this codebase's recurring failure shape — a cheap proxy read as the
fact it stands for. Here: "collapsed nothing this tick" read as "nothing left to
collapse".

**Shipped (01caa46).** Fusion is a cascade over `COARSEN_WIDTHS` = [24 h, 6 h,
1 h]; a span lands at the widest width that fits. The anti-loop guard became
per-width *and* state-aware, and that combination is what makes the cascade
terminate **usefully** rather than just terminate:

| state | blocks fusion at width W when |
|---|---|
| `Running` | always — never race claimed work |
| `Pending` / `Retry` | its width ≥ W (already queued at least this wide) |
| `Superseded` | its width ≤ W (proven too big at a width no larger) |
| `Complete` | never — built once ≠ too big |

`split_time_task` supersedes what did not fit, so a superseded day frees 6 h, a
superseded 6 h frees 1 h, and **each supersede strictly lowers the ceiling** —
the split/fuse loop the guard exists to prevent still cannot run. Without the
width comparison, a superseded day blocks its children at every width and they
sit at ten minutes forever, which is precisely where prod was.

Applies to Dedup, BaseRollup, DerivedRollup and HotPacking alike, so it should
move `pending_dedup` (14,484) as well as `pending_base_rollup` (84,834).

## 25. Where shipbubble actually is

Measured from storage, not counters, via the new `scripts/maintenance_state.py`
(§26). Sealed days 08-05 … 08-18:

| dimension | state |
|---|---|
| compacted (≤2 active files) | 8 / 14 |
| **deduped (no `DedupExec` in plan)** | **0 / 14** |
| 1 m rollup present | 6 / 14 (08-13 onward) |
| 1 h rollup present | 4 / 14 (08-15 onward) |

Fleet-wide the certification picture matches: `dedup_denied_never_certified_pct
= 99.9`. Nothing is certified, so `DedupExec` survives in every 30-day plan —
which is the single biggest remaining term in the 30 d latency goal, ahead of
rollup coverage. Routing itself is now healthy (4,248 hits vs 866 misses, 83%);
the dominant miss is `rollup_miss_not_built_total = 746`, i.e. coverage, not
matching. `rollup_min_contiguous_days = 2` against a target of 30.

**Certification is produced only by `dedup_sweep`, and only 80 certifications
exist, all loaded from disk at boot — the sweep has certified nothing new.** In
43 min it logged no `dedup_sweep_truncated` at all. Next measurement: whether
the sweep is reaching sealed days or being consumed by the drain half before
`sweep_deadline`. Do not "fix" this before measuring it — that is the §22 rule.

## 26. `scripts/maintenance_state.py`

The compaction-chart work, generalised into a terminal tool, because every
question in §25 was previously a hand-rolled one-off.

```
scripts/maintenance_state.py                       # fleet, per day
scripts/maintenance_state.py --project 28f62f01    # all four dimensions
scripts/maintenance_state.py --project 28f62f01 --footers
```

Each dimension comes from the cheapest source that is actually authoritative:
active Adds in the Delta snapshot (not `aws s3 ls`, which counts unvacuumed
tombstones and overstated by 24× on 2026-08-19); the parquet footer's
`sorting_columns` (not the `delta-rs.optimize.sort_by` tag — only OPTIMIZE
writes it, 1,593 of 1,648 adds carry no tags, and the flush path sorts and
stamps a correct footer *without* the tag); the plan shape for dedup; the tier
tables' own partitions for rollups.

**Trap embedded in the tool, worth stating here too:** the project filter must
resolve to a full UUID and use equality. `project_id LIKE 'prefix%'` pins
nothing the router recognises and silently changes the plan shape — it reported
**0** `DedupExec` for every shipbubble day where equality reports **1** for every
day. That inversion is exactly the class of error §24 is about.

## 27. Revised ordering

§22's order stands with one change: **B (frontier width) is no longer last, and
is no longer about scheduling.** §23 shows width is the only lever on the
per-unit constant, and 01caa46 is the sealed half of it. What remains of B is
the live-frontier half — whether the 10-minute mint width should widen, which is
the freshness trade-off, and is the only part that needs a decision rather than
a measurement.

Still explicitly not next: more scheduler tweaks.
