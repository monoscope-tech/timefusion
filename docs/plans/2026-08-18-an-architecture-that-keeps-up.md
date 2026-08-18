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
