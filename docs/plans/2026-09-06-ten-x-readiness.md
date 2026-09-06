# 10x readiness, re-measured on the FIXED system

2026-09-06 ~09:20Z. The standing "no headroom" result — *"across 10x load,
completions move UNDER 6% while backlog grows 11.6x"* — is from 2026-09-04 and
predates EVERY fix of the last 48h (probe admission, per-table EMA, contiguity
rank, spill cap, repair pool, landed-skip). This re-runs the question against
today's code.

## Pre-registered (written BEFORE results)

Arms: merged prod journal, 24h horizon, seed 7, release build —
(a) 1x = 17 ingesting streams (journal truth), (b) 10x = `--streams 170`.

- **Primary**: backlog trajectory at 10x — bounded band vs unbounded growth.
  Numbers: `pending_end`, completions by op, `frontier_lag_secs_max`.
- **Comparator**: the 09-04 signature (completions ~flat, backlog ~11.6x).
- **Mixed cells enumerated**: (i) backlog grows but completions ALSO scale ≥5x
  → capacity improved but still short — measure the multiple; (ii) bounded at
  10x with frontier lag blown → keeping up on volume, losing on freshness;
  (iii) bounded and lag fine → 10x-ready in the sim's IO-free model, and the
  remaining question is real-IO (staging/envelope), stated as such.
- The sim is IO-FREE: a pass here is necessary, not sufficient, for prod 10x —
  per-unit cost there is object-store round trips.

## CPU profile of the expensive dedup shape (partial, method noted)

`run-unit --op dedup` on the whale's freshly sealed day ran **258 s** of real
work (then correctly SPLIT itself — byte preflight working), sampled for 45 s
mid-phase via macOS `sample`. Two run-unit traps hit and documented on the way:
a quiet date's dedup completes in 4 s (probe-clean — nothing to profile), and
the `maintenance-cli` profile gives dedup a **0-byte pool** (`fair(pool_size:
0.0 B)`) because dedup draws the coordinator share that profile zeroes — repair
only escapes it now because repair has its own pool.

Result is PARTIAL: 65% of frames unsymbolicated (release built with
`debuginfo=0` — my own build-speed flag). Through the stripping, the named
frames are dominated by ALLOCATOR traffic (macOS xzone malloc/realloc/free +
memmove/memset/memcmp ≈ 9% at ~35% visibility → plausibly ~25% of CPU),
plus `read` syscalls and condvar waits — consistent with Arrow batch churn as
the CPU story, and with the jemalloc dirty-decay finding from 08-18 (~15% CPU
in allocator behavior) on the prod side. **Method fix for the real rerun:**
build release with `debug = "line-tables-only"`, re-sample, and aggregate by
Rust frame; do it when the cores are not running the 10x arms.

## VERDICT (pre-registered cells) — the fixes bought 1x-cycling, NOT 10x headroom

| metric (24 h, seed 7) | 1x (17 streams) | 10x (170 streams) |
|---|---:|---:|
| executions | 4,373 | **4,320 — FLAT** |
| pending_end | 3,503 | **50,980 (+14x)** |
| frontier_lag_secs_max | 83,739 | 83,739 (identical) |
| completions.DerivedRollup | 208 | **48 — collapses** |
| completions (other ops) | — | within ±5% |

Cell verdict: the 2026-09-04 signature (completions flat, backlog ~11.6x)
REPRODUCES EXACTLY on the fully-fixed system — none of the week's six fixes
moved the 10x ceiling, because they fixed CORRECTNESS and WASTE, and the
ceiling is CAPACITY: per-unit cost x slots. Derived rollups are the first
casualty (the freshness product), while the frontier lane itself keeps up.
A `--workers 32` arm is running to separate "scales with slots" from "capped
elsewhere"; its answer prices the envelope raise vs the DV writer.

## Envelope raise (item 4) — it is a STAGING ENV VAR, not a code change

`coordinator_jobs()` already honors `TIMEFUSION_COORDINATOR_JOB_WORKERS` (env
override, bypasses the cores/3-capped-at-16 default). So "envelope 6→8" needs no
deploy of new code — a staging env var does it. Two cautions the code states:

1. **The `coordinator_jobs` doc already warns against it.** Slots past the inner
   `HEAVY_REWRITE_PERMITS` (10) "just convert coordinator slots into queueing —
   at a 6:1 job:permit ratio, completions collapsed ~0.6/s → 0.035/s". Prod is
   already at 16 jobs : 10 permits (1.6:1); pushing to 20 (to reach the
   `pool×3/5` = 10,178 MiB coordinator ceiling) is 2:1, deeper into that regime.
2. **The pools TILE, so the raise SHRINKS repair.** `coordinator_share` up
   ~2 GB means `maintenance_split` down ~2 GB, and pack+repair split the loss.
   Repair is the pool that unfroze the whale lane yesterday; its post-raise size
   must still clear the ~690 MB first-allocation floor with margin. State it
   explicitly before any staging run; the tiling test holds the arithmetic.

The `--workers 32` sim arm (w16 vs w32 at 10x) is the discriminator: if
completions stay flat, the raise is refuted in-model and it becomes a
repair/pack drain-quality change, not a 10x lever — matching the doc's own
queueing warning. [result pending]

## DV writer (item 2) — GO, as a bounded 2-3 day Phase 0, because it is the ONLY lever the 10x sim leaves

Today's 10x result (executions flat, backlog +14x, DerivedRollup completions
208→48) proves the ceiling is per-unit cost × slots — and the envelope arm tests
"more slots" while DV is the only "cheaper unit" on the table. Dedup rewrites
whole files to drop 0.0008% of rows; a DV write emits a bitmap instead.

Phase 0 deliverable = design doc + measured numbers, then go/kill:
1. **Read-path proof FIRST (the correctness trap).** DV-deleted duplicates are
   masked by `DedupExec` today — but certification exists to DROP `DedupExec`,
   so a scan that ignores DVs would resurface deleted rows on exactly the
   certified windows. Write a DV'd file into a staging table, read it through
   OUR scan path with dedup skipped, assert the deleted row is absent. This seam
   is verified first, not last.
2. **Stats-consumer audit.** add-action `numRecords` ignores DVs, so it
   over-counts on DV'd files — enumerate every reader (`estimated_decoded_bytes`,
   repair reach, witness, count paths) and note tolerance per site.
3. **Cost spike.** Prototype the commit shape (new `add` with DV descriptor
   superseding the old) in the delta-rs fork; `run-unit --op dedup` on a whale
   date, DV-write vs rewrite. That number is the 100x-customer business case.

Note: DV changes the COST of removal, not certification logic — a DV dedup still
has `dropped > 0`, still refused a grant, so the one-pass-delay loop persists.

Carried finding: **DerivedRollup completions collapse first under 10x load**
(208→48 while every other lane holds) — the freshness product dies first, and
derived rollups read exactly what dedup gates, which is the DV motivation too.

## Envelope verdict (item 4, CLOSED) — a real but bounded multiplier, not a 10x lever

Matched 6h pair, 10x load (170 streams), seed 7:

| metric (6h, 10x) | w16 | w32 |
|---|---:|---:|
| completions TOTAL | 638 | **1317 (2.06x)** |
| executions | 882 | 1784 |
| pending_end | 13,619 | **13,014 (−4%)** |
| frontier_lag_secs_max | 67,750 | 67,854 |

**Overturns my prior.** I expected the `coordinator_jobs` doc's queueing
collapse; instead completions scale ~linearly to 32 workers. BUT two facts bound
it:

1. **The sim has NO rewrite-permit semaphore** (grepped: no `HEAVY_REWRITE_PERMITS`,
   no `rewrite_sem`). Its `--workers` models the claim/schedule path only, so
   2.06x is what the SCHEDULER permits — an upper bound. Prod's real cap is the
   10-permit inner pool + real sort CPU/IO per unit, which the sim cannot see.
   The doc's "6:1 job:permit collapse" lives entirely in that unmodeled layer.
2. **Even at 2x completions the backlog barely moves** (13,619→13,014) — at 10x
   arrivals, doubling maintenance throughput dents pending by 4%. Arrivals
   dominate; a throughput multiplier cannot close a 10x arrival gap.

**Decision:** the envelope raise is worth a STAGING test (free via the existing
`TIMEFUSION_COORDINATOR_JOB_WORKERS` env var) as a general maintenance-throughput
win where the box has CPU headroom — but it must raise `HEAVY_REWRITE_PERMITS`
in step (else jobs queue on permits) and it is NOT a path to 10x. Staging watch:
`light_optimize_memory_brakes_total`, the 08-17 OOM shape, and the tiling-shrunk
repair pool clearing the ~690 MB floor.

**The only structural 10x/100x lever remains the cheaper unit (DV writer).**
Item 4's throughput multiplier and item 2's cheaper unit are complementary:
2x from slots × ~Nx from DV (dedup stops rewriting files) is how the arithmetic
reaches 10x, and only DV supplies the large factor.
