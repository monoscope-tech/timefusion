# Morning brief — 2026-09-03

Three behaviour changes shipped, all from the same root cause, all measured
before shipping. Two of the four maintenance lanes turned out to need nothing.

## The one sentence

**Every stuck lane was a unit whose size the gate in front of it could not
grant — and in two of the three cases the gate could not grant the size the
system itself was designed to produce.**

## What shipped

| # | change | lane | evidence it was needed |
|---|---|---|---|
| 1 | repair budget sized by the target file (`2 x 256 MiB x 12`), in its own constant | repair | budget was **0.42x** one target-sized file, so every unit clamped to the whole semaphore; ~1.2 rewrites/hr, 310 queued, ~11 days to drain — **but see below: measured concurrency gain so far is ZERO** |
| 2 | maintenance/coordinator pool USAGE exposed, and its denominator corrected | all | pool sizes were visible, usage was not, so no memory decision could be verified after shipping |
| 3 | admission grants the full cap at **half-free**, not only at a perfectly idle pool | dedup + base_rollup | 365 dedup units at a median of **exactly 512.0 MiB**, median age **14.6 days** |

## Lane survey — two lanes are already fine

| lane | live units | age median | verdict |
|---|---|---|---|
| **HotPacking** | **0** | — | **keeping up completely, no backlog at all** |
| derived_rollup | 52 | 1.1 h | healthy |
| dedup | 1,552 | 22.3 h (p90 386 h) | change 3 targets the stuck tail |
| base_rollup | 320 | 169 h (7 d) | change 3 targets it |
| repair | 314 | 412 h (17 d) | change 1 |

## Change 1 UNDER-DELIVERED — the measured gain is zero, and here is why

**This is the top decision for the morning.** The change is live and behaving
exactly as written (`budget_mib=6144`, and units now report their real size
instead of every one clamping to 1280). But `repair_rewrite_permit_busy` is
**1.35/min — identical to the pre-fix baseline** (243 in 3 h before, 88 in 60 min
after), because no two real units can share the new budget.

The `want_mib` distribution from 60 minutes of live logs (88 events):

| want_mib | count |
|---|---|
| 3,176 | 1 |
| 3,391 | 2 |
| 3,574 | 10 |
| 3,725 | 2 |
| 4,544 | 7 |
| 5,203 | 1 |
| **6,144 (clamped, i.e. ≥6,144)** | **65 — 74%** |

**The smallest real unit is 3,176 MiB; two need 6,352 > 6,144.** I sized the
budget as `2 x COORDINATOR_HOT_TARGET_BYTES x 12` on the reasoning that a repair
unit is one file and compaction targets 256 MiB files. That is true of files
compaction *produces*; the files repair is *queued on* are legacy/whale files far
above the target. So "2 target-sized files" is not two real units — it is barely
one.

**Do not simply raise `REPAIR_REWRITE_TARGET_FILES` 2 → 4.** Two reasons:

1. **It is not validated at that magnitude.** Two clamped-class units are
   ≥12.3 GB decoded through the 8.4 GiB coordinator pool *with light sorts
   alongside*. The measured boundary from the fleet ladder is **1.79x
   decoded-to-pool passes, 2.39x fails**; that proposal lands in the untested gap
   where the cliff is. (Tonight's 2-worker PASS was at 2,756 MB per worker — the
   wrong magnitude to justify it.)
2. **The constant does double duty.** It is also `light_optimize_k`'s holdback,
   so 2 → 4 silently takes light K from 4 to 2. K collapsing is the 2026-09-01
   HotPacking outage class. Zero HotPacking backlog says K=2 is probably fine —
   but "probably fine, via a side effect of a constant that should not have that
   job" is the drift-class bug this codebase keeps re-documenting.

**The right fix is to decouple them**, and it is a design change for a waking
human: the semaphore stays denominated in **decoded bytes** (what admission
prices), while the pool holdback is priced in **pool bytes** at the measured
ratio. Tonight's benches give that ratio — a spilling sort needs ~0.16–0.21x its
decoded size, so the semaphore over-states repair's memory need several-fold.
This is the recurring theme of the whole investigation: *a budget must be
denominated in the unit that actually costs.*

Caveat on the table above: 60 minutes of permit-busy events is a **bounce log,
not the queue** — a held semaphore bounces small units too, so "nothing below
3,176 in 88 events" is real signal but wants a longer window before it is quoted
as the queue's distribution.

## Change 3 in detail — the boundary

`byte_bounded_units` splits until a unit **fits** `MAX_DECODED_BYTES`, so its
output piles up **at** that constant. Both admission sites then clamp their
request to it — deliberately: `split_time_task` declines to hash-shard in the
journal so an oversized unit can reserve the maximum and shard *itself*
internally, which bounds memory without minting units that each re-pay the
partition scan.

But `MAX * available / capacity` is strictly below MAX the moment one byte is
reserved. **So the request the design intends as "reserve the maximum and
self-shard" was the one request the gate could never grant.** Median size of the
stuck class is exactly the constant, minimum too — a boundary, not a
distribution.

ClickHouse grants its full merge cap at **8 free pool entries of 16 — half free,
not idle**. Ours granted it only at 100% free. That is the whole fix, one
divisor.

## Two local benchmarks, which is what changed the design

I was about to ship this paired with a bigger per-job sort slice. **Both halves
of that reasoning were wrong, and the benches are what showed it.**

1. `TF_BENCH_SLICE` — a **512 MB pool sorts 2,451 MB decoded**, 4.8x its own
   size. Floor is ~0.16–0.21x decoded; 384 MB is where it breaks. So
   "512 MiB per job = zero headroom" was wrong about what a spilling sort needs.
2. `TF_BENCH_PRODSHAPE` — **16 workers x ~490 MB decoded through one 8 GiB pool,
   0 of 16 failed.** The old fleet ladder suggested otherwise only because it
   hands every worker the *whole* file — 4.8x what a coordinator job is ever
   admitted for.

Both are committed and re-runnable.

## What to check this morning

- **Dedup/rollup (change 3):** re-fetch the journal. The exactly-512-MiB cohort
  (298 units) and the base_rollup 7.5 GiB cohort (94, pinned at the 1-minute
  minimum width) should be shrinking, and their max age falling.
- **Abort signal:** `Not enough memory to continue external sort` in journal
  retry reasons **rising above tonight's 247** → revert the one divisor in
  `occupancy_scaled_ceiling`.
- **`admission_busy` hot-loops** (attempts in the hundreds — one unit reached
  715) should stop accumulating.
- **Repair (change 1):** expect **no throughput change** — see the section above.
  Judge from the **Delta log**, not counters (restarts re-zeroed them). Large
  single-file rewrites per hour vs the **1.17/hr** baseline; the filter trap is
  that a large repair commits **1 remove → 2 adds**, so a 1-add-1-remove filter
  hides it. The change is still correct and worth keeping — units now express
  their true size instead of every one taking the whole semaphore — it just does
  not buy concurrency until the budget clears two REAL units.
- **`scan.dedup_full_set_pct`** — a file property, restart-proof. Was 8.7% at
  the start of the night.

## Honest uncertainties

- **Pool peak is 70%, not 28%.** Sampling `coordinator_pool_pct` 45 times over
  15 min: median 10%, **max 70%**. My first single reading said 28% and was not
  representative. Under the 80% line I set as the abort threshold, but it means
  the pool does see real pressure at peak.
- **`maintenance_admission` is denominated by the whole box** (80 GiB limit →
  60 GiB capacity), not by the coordinator share it actually protects. That is
  why the occupancy curve is nearly inert. Whether it *should* be denominated by
  the coordinator share is a real question and deliberately **not** touched
  tonight.
- **Repair's gain depends on the queue's size distribution.** The two
  repair-class bins in 24h of logs were 584 MB and 899 MB compressed — still
  over the new budget, so they keep running alone. The win is on the ordinary
  target-sized case.
- **One measurement was thrown away, not reported:** an extended fleet run showed
  8/10/12 workers failing, then died with `StorageFull`. Local disk had hit 100%.
  The 10- and 12-worker rungs "failed" in 8.7 s and 0.9 s — disk exhaustion, not
  memory. Discarded; disk reclaimed to 49 GiB.

## Not done

- The 100x question (`tf_unrunnable_fraction_grows_with_tenant`) is untouched:
  unit size scales with tenant size in every lane, so a bigger tenant puts a
  larger *share* of its work outside fixed byte budgets. Tonight's three fixes
  raise the budgets; they do not make them adaptive.
- No profiling this session (CPU profiling is off in prod since the 08-11
  SIGSEGV).
