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
| 1 | repair budget sized by the target file (`2 x 256 MiB x 12`), in its own constant | repair | budget was **0.42x** one target-sized file, so every unit clamped to the whole semaphore; ~1.2 rewrites/hr, 310 queued, ~11 days to drain |
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
- **Repair (change 1):** judge from the **Delta log**, not counters — there were
  three restarts tonight so cumulative counters re-zeroed. Large single-file
  rewrites per hour vs the **1.17/hr** baseline. Remember the filter trap: a
  large repair commits **1 remove → 2 adds**, so a 1-add-1-remove filter hides it.
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
