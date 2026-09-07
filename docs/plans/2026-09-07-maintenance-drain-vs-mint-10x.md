# Maintenance drain vs mint: why we don't keep up, and the 10x lever

2026-09-07, prod image `bf0ade5` (master + DV-dedup + certify-on-completion + re-mint-skip).
Analysis only; nothing here is deployed.

## Premise check, up front

"We mint faster than we drain" is **half right**. Steady-state ingest mint is ~5.2 units/min
against a measured whole-fleet drain of ~11.6 units/min (the code's own measurement,
`src/maintenance_coordinator.rs:2458`) — ingest alone does NOT outrun the lanes. What outran
them is (a) a **livelocked dedup lane burning ~90% of maintenance CPU without converging**, and
(b) #214's 60s reconcile loop re-minting work from our own commits (mostly closed same-day by
`71524a51`+`21c4179b`; counters `dedup_remint_skipped_total=19,932`,
`rollup_remint_skipped_total=11,627` in ~1h show the closed leak's magnitude). The ~23d
`oldest_task_age` is #214's one-time dredge — bounded and idempotent
(`src/database/mod.rs:15441`), not steady-state mint.

## The numbers (one process, ~50 min, then re-sampled 18:23Z)

- worker_secs: **Dedup 35,695 (90.3%)**, BaseRollup 3,439, HotPacking 317, DerivedRollup 96.
  Total ≈ 39.5k over ~50 min ≈ 13.2 of 16 workers busy — capacity is NOT idle, it is spent.
- **Dedup killed_secs 17,212 = 48.2% of dedup time** (killed ⊂ worker_secs,
  `maintain.rs:4004-4008`) **produced nothing**; ~43% of ALL maintenance capacity.
- `retry.Dedup.dedup_incomplete` 45 → 69 in ~35 min: units respinning on a flat 30s requeue
  with **no backoff, no attempt cap, no quarantine** (`dedup_incomplete` matches neither
  `is_capacity_failure` nor quarantine reasons — `maintenance_coordinator.rs:3080`, `:2595`).
- Backlog 74% rollups (base 1,547 + derived 859) vs dedup 873 — the starved lanes are the
  ones the backlog is made of.

## Root cause 1 — the dv-dedup oracle is CORRUPT, not the scan; retry can never converge

`stage_dedup_chunk_dv` runs a same-snapshot `COUNT(*)` oracle (`compact.rs:2121`) and rejects
certification when the projected scan disagrees (`compact.rs:2186`). Prod logs show the
mismatch firing constantly on `otel_logs_and_spans`, **in both directions** (scan 17,829 vs
oracle 12,243; scan 19,228 vs 32,664; near-ties off by 1–8). The comment's theory ("truncated
re-read — retry") is inverted. Mechanism, in the delta-rs fork (rev `a1737985`):

- The DV keep-mask is a **shared, destructively-drained** `DashMap` — each stream
  `drain(0..batch_rows)` from the front per file
  (`crates/core/src/delta_datafusion/table_provider/next/scan/exec.rs:120,:593,:68-93`).
  Correct only if ONE stream sees the file's rows in physical order.
- The SinglePartition guard keys on **`retained_row_index_field()`**, not on DV presence
  (`exec.rs:329-336,:361-370`). The shard scan projects `__tf_dv_row_index` → guarded,
  correct. The oracle `COUNT(*)` doesn't → DataFusion **byte-range-splits DV'd files across
  2 partitions** (`repartition_file_scans=true` default, min 10 MB;
  `MAINTENANCE_MAX_PARTITIONS=2`, `src/database/mod.rs:1324`), two streams drain one mask out
  of order. Total file count is preserved but **survivor identity is scrambled**; the
  timestamp window filter (applied above the scan — parquet pushdown is off for both shapes,
  fork `table_provider.rs:463`) then counts a scrambled set. Sign depends on where the 10-min
  window sits in each byte range — hence bidirectional, hence the near-ties.
- **Occurrence is deterministic**: any DV'd chunk ≥10 MB fails every attempt. The `hashes`
  DML-DV UPDATE flood guarantees DVs everywhere, so dedup on the hot table is a permanent
  30s-respin loop: full preflight snapshot walk + 235s-class GROUP BY probe + permit-gated
  narrow scan per attempt, zero certifications. `files_sql` (`compact.rs:2085`) is scrambled
  the same way (under-deletion only — safe, but source of `mapped N/M files` replans).
- Same-cause counter: `scan.cert_granted_total=0`. It was already 0 pre-DV for contiguity
  reasons (2026-08-20); the oracle bug now guarantees it STAYS 0 — a DV'd chunk can never
  record a clean slice, so day coverage can never complete, and `DedupExec` stays in every
  read plan on this table.

## Root cause 2 — kills are permit-queue deaths, and one kill loses a whole wave

- ~12 concurrent dedup workers (35,695s/3,000s window) contend for
  `maintenance_rewrite_sem` = **10 permits** shared with 20–50 min heavy rewrites; the
  acquire at `compact.rs:2160` is an **unwatched await**, so `run_until_idle`'s 900s idle
  deadline (`maintenance_coordinator.rs:132`) most likely kills units parked in the permit
  queue.
  worker_error=6 vs 17,212 killed_secs ⇒ ~6 kills averaging ~2,870s — units that worked
  30–40 min, then starved 900s.
- All bins of a unit commit in ONE terminal `commit_wave` (`compact.rs:976`) — **a kill
  discards every staged bin**, and the staged `.bin` DV sidecars are orphaned until the next
  restart's `reconcile_staged_intents` (spawned once at boot, `mod.rs:5083`).
- No dedup lane cap exists: with `rollup_median_contiguous_days=30 ≥ COVERAGE_SHORT_DAYS=14`,
  the BALANCED cycle runs and BOTH debt gates are `None` (`maintain.rs:3917-3943`) — all 16
  workers may sit in Dedup. Slots don't cap lane TIME, only claimability and unit duration do.

## Why the rollup backlog (74%) doesn't drain

- **DerivedRollup is claim-gated, not slow**: `dependencies_complete` requires Complete
  BaseRollup **task records** contiguously covering the slice
  (`maintenance_coordinator.rs:2887,:2911-2934`); historical days whose journal records were
  coarsened away are unclaimable forever, silently (prod in-code: `derived_unproven=674/674`,
  `:736`). 96 worker_secs against 859 pending.
- BaseRollup drains when claimed (window 2 below: −60 in 5.5 min) but lost ground in window 1
  (+89) against the dredge refill — it competes for workers against a dedup lane that never
  finishes its units.
- Residual mint leak: mixed/CoW dedup waves stay untagged (`maintain.rs:7971`) and are
  re-minted by the 60s reconcile loop — structural, still open, but second-order next to the
  dedup livelock.

## Same-process drain deltas (verified no restart: `pgwire.queries_total` monotonic)

- First window (~35 min → 18:23:32Z): dedup 873→828 (−45), base 1,547→1,636 (+89), derived
  859→934 (+75), tasks_pending 3,254→3,376 (+122) — rollup lanes GROWING while dedup burned
  90% of CPU to move its own queue −45.
- Second window (18:23:32→18:29:04Z, 332s): Dedup worker_secs +8,772, **killed_secs +7,279**
  (per-kill `killed_secs` is the unit's FULL elapsed, so 2–8 kills — the waste is ongoing,
  not a boot artifact),
  `dedup_incomplete` +13 (~142/hr), tasks_pending −105, base −60. Drain oscillates around
  mint window-to-window; the margin dedup wastes is the difference.
- Re-mint skip counters grew +2,964 dedup / +1,729 rollup in those 332s ≈ **51 skips/min**
  vs ~5.2/min frontier mint — the reconcile loop generates ~10x the frontier's mint pressure
  and the skip is what absorbs it; the untagged-CoW share leaks through.
- `cert_granted_total` still 0 across all samples.

Verdict: drain<mint as observed is the dedup lane eating the fleet (plus #214's bounded
dredge refilling coarse units), not the frontier out-minting the lanes.

## The 10x lever, ranked

**(1) Fix the oracle — flood-tolerance is the wrong frame; the oracle is simply wrong.**
Two independent fixes, do both:
  - TF-side, no fork bump: give the oracle the scan's plan shape so it gets the
    SinglePartition guard — e.g. count over a subquery projecting `__tf_dv_row_index` at
    `compact.rs:2121` (and the same for `files_sql:2085`). CAUTION: optimize-projections may
    strip the unreferenced inner column and silently reproduce the corrupt shape — the test
    must EXPLAIN-assert the plan is SinglePartition, not just that counts agree once. The
    fork-side guard is the robust fix.
  - Fork-side, one line ×3: extend the guard to `retained_row_index_field().is_some() ||
    !self.selection_vectors.is_empty()` at `exec.rs:329/:366/:389` — the clause `a1737985`
    already added for filter pushdown. Fixes every DV'd multi-partition scan, not just dedup.

Expected: dedup units complete and certify instead of respinning; the ~48% killed share and
the infinite 30s loops collapse; certification unblocks (`cert_granted_total` 0 → >0), which
removes `DedupExec` from read plans AND enables the dedup re-mint skip economy. Reclaims most
of the ~90% CPU share for the rollup backlog — that is the 10x drain headroom, because the
fleet's ~11.6 units/min capacity already exceeds ~5.2 units/min mint once workers do work
that completes. Risk: low — the scan side (the side that writes DVs) is already correct;
the change makes the guard agree with it. Deferring dedup on hot tables (option c) is worse:
`maintain.rs:2241-2258` — certification is precisely what the hot table needs.

**(2) Stop killing permit-queued units** (secondary, ~17k killed_secs): either claim-gate
Dedup to `HEAVY_REWRITE_PERMITS` (don't claim what can't get a permit) or count permit-wait
as progress-exempt; and commit landed bins incrementally so a kill stops costing the wave.
**(3) Unblock DerivedRollup** via `base_tier_ready` covering coarsened history (859 units).
**(4) Close the CoW-wave re-mint tag gap** (`maintain.rs:7971`).
**(5) Give `dedup_incomplete` a backoff/attempt cap** so any future non-converging condition
degrades instead of livelocking.

## Validation before prod

1. Failing test first: sqllogictest/e2e over a DV'd file >10 MB with `target_partitions=2` —
   assert `COUNT(*) WHERE window` == projected-scan row count (this reproduces red today).
2. `timefusion run-unit --op Dedup` against a real DV'd prod date (e.g. `dcad860a… 2026-08-14`)
   with phase timers: before = oracle-mismatch retry; after = staged+certified, and record
   the per-unit cost.
3. `timefusion sim` on a fresh prod journal: with dedup unit-duration set to post-fix
   measured cost, confirm rollup lanes' claim share and `day_covered` rise.
4. Staging soak (real S3 latency), then ONE deploy; gates: `dv-dedup: scan saw` warn rate → 0,
   `cert_granted_total` > 0, `killed_secs` flat, `pending_base+derived` slope turns negative
   over a ≥2h quiet window.

## Stale docs to fix while here

`compact.rs:2184` comment (mismatch ≠ truncated re-read), `maintain.rs:4037` (packing units
no longer calendar-minted), `maintain.rs:7965` ("rollups still re-mint" — falsified by
`21c4179b`).
