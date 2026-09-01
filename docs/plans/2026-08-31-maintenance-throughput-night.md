# 2026-08-31/09-01 night — maintenance must stop breaking a sweat

Goal (from the user): every maintenance lane — sorting/repair, hot packing,
rollups, dedup — must keep up with the live stream with room for 10x, ideally
100x. Prefer local benchmarks; prod only to confirm.

## Baseline measured tonight (process `6c37e29`, 5.8h, ended 21:07 UTC by a deploy)

Unit outcomes over 5h of logs:

| Lane | Complete | Retry | Timed out (`outcome=Running`) |
|---|---|---|---|
| SealedConsolidation | 448 | 0 | 0 |
| BaseRollup | 605 | 0 | 32 |
| Hot-tail/light optimize | 533 bins | — | 0 |
| DerivedRollup | 107 | 181 | 0 |
| Dedup | 189 | 496 | 618 |
| HotPacking | 14 | 472 | — |
| Repair | 0 | 1 | 6 |

Persisted gauges (survive restart), flat or growing over 10 min:
backlog 3.54 TB, sealed debt 1.07 TB, `pending_dedup` 4929→4933,
`dirty_bin_queue_depth` 40744→40751, `beyond_horizon_tasks` 2808→2814,
`oldest_task_age_seconds` 1.458M (16.9 d) rising 1:1 with wall clock,
`rollup_oldest_invalidation_age_seconds` 16.3 d.

Read side: `read_dedup.ordering_violations_delta` = 2.2M this process.

### The three anomalies, ranked by how absurd the cost is

1. **Repair: 900s for a 2-file, 268 MB-decoded slice, `attempts=100`.** The lane
   completed nothing in 5.8h and is cycling the same two 12h slices
   (project 87576849…, 2026-05-30 and 05-31). Write-time sorting is healthy
   (`flush_sort_unsorted_fallbacks` 0, `repair_sorted_at_write` 2416), so the
   entire sortedness debt sits behind this one wedged lane.
2. **HotPacking: 14 complete vs 472 retry.** 18 units cycling to `attempts=21`.
   One logged refusal: `maintenance_hygiene_debt_unclaimed
   refusal="outranked_by:…files=89"`.
3. **Dedup: 13% completion.** Units run 75–254s against a 300s deadline, so 618
   died mid-flight. Queue grows.

Rollups split in two:
- Building is fine (605 base publications, coverage contiguity 30 d).
- Routing is ~2% (138 hits / 6774 misses). Dominant miss is
  `unaligned_bucket` 3604 = the query asked for a bucket narrower than the
  rollup grain. That is a tier-granularity/shape problem, not throughput.

Dead counters (no producer in `src/`, do not read their zeros as facts):
`rollup_full_hours_rebuilt`, `rollup_incremental_hours_rebuilt`.
`tasks_complete` is a gauge.

### Gauge deltas over 30 min (23:04 → 23:34 CEST, spans one deploy restart)

| gauge | t0 | t+30m |
|---|---|---|
| `backlog_bytes` | 3,536,560,609,552 | 3,552,888,809,998 (+16.3 GB) |
| `dirty_bin_queue_depth` | 40,744 | 40,792 |
| `beyond_horizon_tasks` | 2,808 | 2,825 |
| `eligible_sealed_total` | 3,592 | 3,607 |
| `oldest_task_age_seconds` | 1,458,254 | 1,460,056 (+1,802 s in 1,800 s — nothing retires) |
| `pending_repair` | 432 | 432 |
| `pending_hot_packing` | 17 | 18 |
| `pending_dedup` | 4,929 | 4,883 |
| `pending_base_rollup` | 412 | 333 |

Only base rollups are draining. Everything else is flat or growing.

## The table is SMALL. The per-unit cost is what is absurd.

`otel_logs_and_spans` at Delta version 513,504: **6,615 active files,
1,063 GB, 1,176 (project,date) cells, 3.66 B rows.** So none of this is a
"too much data to walk" problem — snapshot walks are cheap. Row width is the
story: p50 file 5 MB, p90 725 MB, max 2.3 GB. The whale's worst file is
**431 MB for 81,491 rows = 5.3 KB/row compressed ≈ 63 KB/row decoded.**

Observed staging throughput from `wave_bin_staged` (prod, 21:02-21:07 UTC):

| bin | bytes_in | staging_ms | MB/s compressed |
|---|---|---|---|
| otel_logs 87576849 | 2.97 MB | 2,514 | 1.2 |
| otel_logs 94c5dc1f | 0.15 MB | 628 | 0.24 |
| otel_logs dcad860a | 6.74 MB | 14,252 | 0.47 |
| otel_metrics 8100121c | 93.8 MB | 322,325 | **0.29** |

**The whole coordinator rewrite fleet runs at roughly 0.3-1.7 MB/s
compressed.** At 12x decode that is 4-20 MB/s decoded on a 48-core box. That
single number explains every lane's backlog.

## Log

### 1. Why does a 2-file repair unit take 900s? — ANSWERED

Not the permit queue. `permit_wait_ms=0` on every staged bin in the window,
and `compaction_permits_unavailable` is 9 over 5.8h — the pre-claim gate added
after the 2026-08-25 investigation did its job. The 08-25 note in
`maintain.rs:5964-5975` is now history, not a live diagnosis.

The actual chain, from the logs:

```
selected_files=1 bytes_in=1148230580 permit_wait_ms=0   wave_bin_staging_started
bytes_in=1148230580 slices=13                           repair_bin_sliced
(no wave_bin_staged — the 900s deadline fires first)
```

`repair_slice_want` = `estimated_decoded_bytes(1.148 GB) / 1 GiB + 1` = 13.
Each slice is a **separate `ctx.sql("SELECT * … WHERE ts >= a AND ts < b ORDER
BY …")` pass over the same file**. The file is a repair candidate precisely
because it is unsorted, so its row-group timestamp stats overlap and no slice
can prune: **each of the 13 passes decodes the whole 13.8 GB.** ~180 GB
decoded, single-partition, to rewrite one 1.1 GB file.

And it is not even cached: `foyer.cache_recent_days = 35`, the file is dated
2026-05-30 (93 days old), so `is_within_recent_window` is false and **every
slice re-downloads 1.1 GB from OVH**. 15 GB of egress per attempt × 100
attempts.

`attempts=100` is therefore arithmetic, not bad luck. The unit cannot finish
inside 900s and never records a `StagedIntent`, so resume can never rescue it
either.

### 2. The whole fleet sorts single-partition with 256-row batches

`stage_hot_bin` on the coordinator path (`maintain.rs:6025-6031`) and the
limited dedup path (`compact.rs:998-1003`) both build:

```rust
build_optimize_session_state_tuned(query_partitions, coordinator_runtime_env(),
    Some("256"), Some(UncappedSort { partitions: 1, reservation_bytes: 32 MiB }))
```

So **Repair, HotPacking, SealedConsolidation and Dedup all rewrite with
`batch_size = 256` rows and `target_partitions = 1`.** On otel rows this is a
~4.5 MB batch and a single-threaded sort. The 256 is scar tissue with a real
reason (`write.rs:284-292`: `SortPreservingMergeExec` allocates per spill-run
per batch, and the coordinator pool is 4 GB) — but it is applied uniformly,
including to the 0.15-7 MB bins where fan-in is tiny and the per-batch
overhead is the entire cost.

One fix here lifts four lanes at once. That is the lever.

### 3. The file has ONE row group of 7 GB

`big1148.parquet`: 1,035,264 rows, **1 row group**, 94 columns, 7,017 MB
uncompressed. Three consequences, all bad:

- **No scan parallelism is possible.** DataFusion partitions a parquet scan by
  row group. One row group = one partition, whatever `target_partitions` says.
- **No pruning is possible.** One set of stats covers the whole file, so the
  13 event-time slice predicates each select the entire row group.
- Every read of any part of it decodes 7 GB.

Current writer properties (`build_writer_properties`, mod.rs:8567) do bound
row groups — `set_max_row_group_bytes(128 MB)` plus
`set_max_row_group_row_count(row_group_row_count(...))`, which clamps to
[32,768, 1,048,576] rows. This file has 1,035,264 rows in one group, i.e. it
predates that or hit the ceiling. Note the row-count model estimates otel at
~4.5 KB/row while the whale's May files are ~63 KB/row decoded — **14x off**,
so even today's cap can emit a 2 GB row group for that tenant.

### 4. The journal says every repair unit dies the same death

`maintenance_tasks.json` (52 MB, 73,005 tasks), states by operation:

| operation | complete | superseded | pending | retry | running |
|---|---|---|---|---|---|
| base_rollup | 31,630 | 7,773 | 131 | 200 | 3 |
| dedup | 13,051 | 3,602 | 2,847 | 1,988 | 13 |
| derived_rollup | 3,546 | 5,351 | 21 | 18 | 0 |
| repair | 2,064 | 335 | 0 | 432 | 0 |

Retry reasons on everything not complete:

```
7773  base_rollup      split_into_smaller_slices
3602  dedup            split_into_smaller_slices
2790  derived_rollup   migrated_to_aligned_hour_slice
2561  derived_rollup   split_into_smaller_slices
1693  dedup            worker_error            <- timed out
 432  repair           worker_error            <- ALL of them
 243  dedup            "Not enough memory to continue external sort"
 183  base_rollup      worker_error
```

- **All 432 active repair units are `worker_error`.** The lane is not slow, it
  is 0%.
- **Dedup's failures are memory and time**, in that order of interest: 243
  units died with `Not enough memory to continue external sort` and 1 with
  `Additional allocation failed for SortPreservingMerge`.
- Repair units all carry `estimated_decoded_bytes = 268,435,457`
  (= `MAX_DECODED_BYTES/2 + 1`, stamped by `byte_bounded_units`) while the real
  file is 13.8 GB decoded — **the admission and slicing decisions are made on a
  number that is 50x wrong.**
- Repair units have been bisected to widths of 12h/0.75h/0.38h. Bisecting a
  repair unit sheds nothing: `coordinator_compaction_files` hands Repair
  `take(1)` of a *whole file* regardless of slice width, so all the children
  fight over the same file.
- Dedup's active units: 1,653 at 10-min width, 1,436 at **1-min** width — the
  bisect debris of the same failures. Each still pays the full fixed cost
  (claim, resolve, snapshot walks, provider, plan, ≥1 row group per overlapping
  file, commit).
- Dedup backlog age: 1,750 units are stuck on 2026-07-23/24/25 while 2,051 are
  from today — a five-week-old head-of-line blockage plus the live stream.

### 5. The coordinator pool is 4 GB shared by 16 workers

`coordinator_share_bytes()` = `min(jobs × MAX_DECODED_BYTES, maintenance_pool/4)`
= min(8 GB, 4.24 GB) = **4.24 GB**, and `tasks_running` is 16. A `FairSpillPool`
splits that ~16 ways: **~265 MB per concurrent rewrite**, against a per-unit
admission estimate of 512 MB. That is the direct cause of the 243
`Not enough memory to continue external sort` failures.

Meanwhile `maintenance_pool_mb` is 16,964 and the remaining ~12.7 GB sits in
the heavy/light/repair pools, whose only callers are the legacy
`optimize_table_light` path that `COORDINATOR_OWNS_SLICE_MAINTENANCE` disabled.
The comment on `coordinator_share_bytes` still says "DEDUP units sort on the
heavy share" — they do not: `compact.rs:998` hands coordinator dedup
`coordinator_runtime_env()`.

Which of the four maintenance pools are actually live (every `*_runtime_env()`
call site classified):

| pool | share | live callers |
|---|---|---|
| coordinator | 4,241 MB | **all coordinator rewrites** (maintain.rs:2884, compact.rs:1000, mod.rs:4551) |
| heavy | 5,089 MB | dedup cron for non-coordinator tables, delta-write session, maintenance scans |
| light: pack | 3,817 MB | `light_optimize_session_state` — only from `stage_hot_bin`'s `runtime_env: None` arm and `optimize_table_light`, **which has no callers** |
| light: repair | 3,817 MB | `repair_session_state` — same dead arm |

**~7.6 GB of the maintenance budget is reserved for two code paths that cannot
run**, while the path that runs everything gets 4.2 GB split 16 ways.

### 6. Row groups are capped by ROWS, and rows vary 22x in width

A *recent* compacted whale file (`date=2026-08-30`, 204 MB, 1.64 M rows) is
healthy in shape — **51 row groups, every one exactly 32,768 rows** (the floor
of `row_group_row_count`), with honest `sorting_columns`. So the write path
fixed the 1-row-group problem for new files.

But the decoded size of those equal-row groups ranges from **37 MB to 819 MB**:

```
rg uncompressed MB: 45.6, 39.2, 41.2, 45.5, 46.8, 47.5, 629.4, 819.0, 44.9, ...
rg rows:            32768 (every one)
```

`set_max_row_group_bytes(128 MB)` is set but does not bind (parquet-rs), so the
row-count cap is the only real control and it is blind to payload width. A
reader or a rewrite that touches row group #8 must decode 819 MB — against a
coordinator share of ~265 MB per worker. **This is a second, independent cause
of the `Not enough memory to continue external sort` failures**, and it is a
write-time defect: the fix belongs where the file is produced, not where it is
read.

### 7. Bench: the real file, `benches/rewrite_throughput.rs`

Prod's worst repair input (`big1148.parquet`, 1.148 GB compressed, 1,035,264
rows, 7.0 GB decoded, ONE row group), release build, 4 GB pool, local disk —
so no OVH latency, which makes these numbers a *lower bound* on prod's:

| variant | secs | MB/s in |
|---|---|---|
| scan only (no sort) | **7.3** | 156.7 |
| sort b256 p1 — **what prod does per slice** | 39.4 | 29.1 |
| sort b256 p8 | 30.5 | 37.6 |
| sort b2048 p1 | 23.4 | 49.0 |
| sort b2048 p8 | 20.2 | 57.0 |
| sort b8192 p1 | 20.3 | 56.5 |
| sort b8192 p8 | 20.2 | 56.8 |
| **PROD: b256 p1 x13 slices** | **94.3** | 12.2 |

Three conclusions, none of which needed a deploy:

1. **Decode is not the problem.** 7.3 s to decode 7 GB is ~960 MB/s. The whole
   file's data movement is cheap; everything above the 7.3 s floor is the sort.
2. **`batch_size = 256` costs ~2x.** 39.4 s → 20.3 s just by moving to 8192, on
   identical hardware, identical pool, identical plan. Partitions are worth
   almost nothing (30.5 vs 39.4) because ONE row group means one scan
   partition no matter what `target_partitions` says.
3. **Slicing costs 13 decodes.** 94.3 s ≈ 13 x 7.3 s of scan — the sort is
   cheaper per slice, but the rescans dominate. Locally that is only 2.4x
   worse than one pass; on prod each slice ALSO re-downloads 1.1 GB from OVH
   (the file is 93 days old, past `cache_recent_days = 35`), which is what
   turns 40 s of work into a 900 s timeout.

So the repair wedge is: **~40 s of compute, done 13 times, over a network.**

#### The same file at a 256 MB pool — prod's per-worker share today

`4.24 GB FairSpillPool / 16 concurrent jobs ≈ 265 MB`. Re-run at 256 MB:

At 512 MB — the per-worker share AFTER the pool change — `scan only` is 7.0 s
and `sort b256 p1` 31.2 s; `b256 p8` still fails on the merge reservation, which
is why the coordinator path sorts with `partitions: 1`. The larger-batch rows
did not complete (see open items).

| variant | secs |
|---|---|
| scan only | 6.7 |
| sort b256 p1 | 39.2 |
| sort b256 p8 | **FAILED** — `Additional allocation failed for SortPreservingMergeExec` |
| sort b2048 p1 | 47.1 |
| sort b2048 p8 | 50.0 |
| sort b8192 p1 | **FAILED** — `Not enough memory to continue external sort` |
| sort b8192 p8 | **FAILED** — same |
| PROD: b256 p1 x13 | 82.8 |

This is the prod journal's error text, reproduced on a laptop: those are the
verbatim messages behind 243 dedup units and the one
`SortPreservingMerge` failure. Two things follow, and they are the whole
argument for pricing a batch in BYTES:

- **A row count cannot be safe.** 8192 rows is 109 MB per batch on this file and
  fails; on `otel_metrics` (47 B/row) the same 8192 is 385 KB and is
  ~30x too *small*. `batch_rows_for` caps the BYTES, so it produces 630 rows
  here and 8192 there — both ~8 MB.
- **The optimum inverts with pool size.** At 4 GB, bigger batches are faster
  (39.4 → 20.3 s); at 256 MB they are slower (39.2 → 47.1 s) and then fail,
  because spilling dominates. That is why the pool increase and the batch
  change belong in the same deploy: the batch change alone, at today's pool,
  would have been neutral-to-worse.

## What shipped — deploy 1 of 2 (`fb2e4528`)

| # | change | knob | why |
|---|---|---|---|
| A | Repair rewrites in **one pass**; event-time slicing is off | `TIMEFUSION_REPAIR_SLICE_DECODED_TARGET_BYTES` (0 = off, the default) | 13 rescans of an unprunable, uncached 1-row-group file is the wedge |
| A2 | The unit deadline became an **idle** clock (`run_until_idle`), and Repair's is 3600s | — | IOx's `timeout_with_progress_checking`: made progress, keep going; made none, quarantine. Killing a working unit discards uncommitted work and re-queues the same slice |
| B | Coordinator pool cap `pool/4` → `pool*3/5` (4.2 → 8 GB, i.e. `jobs x MAX_DECODED_BYTES`) | — | 265 MB per worker against a 512 MB admission ceiling is why 243 dedup units died on external-sort memory |
| C | Batch size from **measured row width** (`batch_rows_for`, target 8 MB, clamp [256, 8192]) instead of the constant 256 | `TIMEFUSION_MAINTENANCE_BATCH_TARGET_BYTES` | measured 39.4s → 20.3s on the same file, same pool, same plan |
| D | `split_time_task` declines for Repair | — | bisecting a whole-file unit sheds nothing and mints duplicates that all fight over the same file |
| E | Retries counted per `(operation, reason)` | — | the missing instrument; `retry_reason` was a single "last reason" `String` |

Note when reading E: `compaction_debt_remaining` is a **success** requeue — the
partition still has debt after a bin landed — not a failure. HotPacking's 472
"retries" are largely that.

### There were always TWO clocks, and the outer one was one minute away

`COORDINATOR_LOOP_TIMEOUT` (16 min) wraps planning **plus** one unit; the
per-operation deadline (15 min) wraps the unit. A one-minute margin, which held
only while every operation shared the 15-minute bound. Raising Repair's window
to an hour would have changed nothing on its own — the outer guard would have
killed the unit at 16 minutes, dropped its `TaskLease` and requeued it, which is
the treadmill exactly. `COORDINATOR_LOOP_TIMEOUT` is now derived from
`MAX_OPERATION_DEADLINE_SECS`, and a test pins the ordering.

The two clocks answer different questions and should not be conflated again:
`run_until_idle` asks *is this unit working?* (and a working unit may
legitimately outlive any fixed window); the loop guard asks *is something hung
outside a unit?* — planning scans and the claim path, which the per-unit
counter cannot see.

### One regression the suite caught, worth remembering

`run_until_idle` is an extra async layer that holds the coordinator's **whole
dispatch future** for the life of the unit. Pinning it on the stack
(`std::pin::pin!`) overflowed the worker stack in a debug build —
`dedup_compaction_test::a_partly_covered_window_unions_the_rollup_with_raw…`
SIGABRT'd on the branch and passed on master. `Box::pin` fixes it. The previous
shape avoided this by not adding a layer at all: `tokio::time::timeout` took the
future by value at the call site.

Discriminating it took a worktree on master and two env-knob runs (batch target
forced to the floor, slicing restored) to rule out the two config changes first
— which is the argument for putting every behavior change behind a knob.

The `Box::pin` is load-bearing and looks like a needless allocation, so it will
be "simplified" eventually. The structural fix is upstream: the dispatch future
in `run_maintenance_coordinator_once` inlines all six operation paths, so
anything that holds it is stack-hostile. Boxing per arm at the dispatch site
would shrink it for every holder, not just this one. Follow-up, not tonight.

### Gate result

`make prepush`: lint clean, **1286/1286** integration tests pass. `make test-e2e`:
**59/60**, the one failure being `smoke::count_star_returns_correct_value` — the
already-documented count-pushdown defect (memory
`tf_count_star_is_wrong_not_flaky_2026-08-28`), which passes in isolation here
and is not touched by this branch. `ordering_pushdown::one_unsorted_file_…`
failed once under full-suite load and passed 4/4 in isolation, and passes with
either new knob reverted — a load flake, not a regression.

## Deployed: `fb2e4528`, and the wedged file is gone

First evidence from the new build (image `fb2e452`, container `v5sf7cbcoy5n`):

```
maintenance_repair_attempts_reset reset=767              <- the migration ran
budget tree ... coordinator_share_gb=8 heavy_share_gb=4 light_share_gb=3
repair_bin_sliced occurrences: 0                          <- slicing is off
wave_bin_staged  selected_files=1 bytes_in=1148230580 staging_ms=410023
retry.Repair.compaction_debt_remaining = 1                <- a SUCCESS requeue
retry.Repair.worker_error              = 0
```

**`bytes_in=1148230580` is `big1148.parquet`** — the file that never once
reached `wave_bin_staged` in 100 attempts. It staged in **410 s** and committed.
410 s against the 39 s measured locally is the OVH download of an uncached
1.1 GB file plus a loaded box; it is comfortably inside the new idle window and
would have been killed at 900 s under the old one.

First ~11 minutes of unit outcomes: BaseRollup 12 Complete, Dedup 7 Complete /
8 timed out / 3 Superseded, DerivedRollup 1 Complete / 3 Retry
(`base_tier_incomplete`), Repair 1 Retry (`compaction_debt_remaining`).
`processed_bytes_total` is ~19 MB/s against the old build's ~5.8 MB/s — early,
noisy, and the number to re-read after a quiet window.

### Ground truth, ~45 minutes in: both wedged files are GONE

Compared against the pre-deploy `get_add_actions` snapshot, the live Delta table
has **retired 122 files / 3.60 GB**, and the two largest are:

```
2337.8 MB  2026-05-31  __HIVE_DEFAULT_PARTITION__   <- the table's LARGEST file
1148.2 MB  2026-05-30  87576849                     <- the attempts=100 file
```

The 2.3 GB one staged in **1,150 s**. Under the old build it had two ways to
fail and took both: the 900 s deadline killed it outright, and slicing would
have cut its 28 GB decoded into ~28 full re-reads of an unprunable, uncached
file. It is now rewritten, sorted, and committed.

Every `maintenance_coordinator_unit_timed_out` in the window is **Dedup**. Zero
for Repair.

And the liveness clock keeps earning it. `maintenance_unit_slow` over the first
hour reports three Repair units at **3031 s, 2679 s and 1562 s — all of which
COMPLETED.** Every one would have been killed by the old 900 s deadline, and
each kill would have discarded uncommitted work and re-queued the identical
slice. (These are *elapsed* times; the rule is idleness, so a unit writing rows
continuously never approaches the 3600 s window.) The same log shows completing
Dedup units at 91 s and 149 s — the dedup timeouts are a different, heavier
population, not a general slowness.

Dedup timeout rate: 13 in the first ~58 minutes ≈ 13/hour, against the old
build's 618 over 5 hours ≈ 124/hour. **~9x fewer**, from the batch and pool
changes alone — but `pending_dedup` still grows, so arrivals still exceed
completions.

**Dedup still times out at 300 s** (`retry.Dedup.worker_error = 6`). Expected:
its deadline was not raised and it does not report progress, so the liveness
clock does not cover it. That is the next lane.

### Repair is draining months of debt: 10.11 GB retired in ~2h

Delta-snapshot diff against the pre-deploy baseline, ~2 hours in — **127 files /
10.11 GB retired, of which 10.00 GB is seven files over 100 MB**:

```
2337.8 MB  2026-05-31      1183.6 MB  2026-06-08      1148.2 MB  2026-05-30
2251.2 MB  2026-05-31      1163.6 MB  2026-06-09      1145.5 MB  2026-05-30
```

These are the biggest and oldest files in the table. Every one was
unrewritable under the old build — too large for the 900 s deadline, and
slicing would have turned each into a dozen-plus full re-reads of an unprunable,
uncached file. The lane went from **zero completions in 5.8 hours** to **10 GB of
legacy sortedness debt retired in two.**

### Deploy 1's quiet-window readout (1h53m, `uptime_seconds = 6771`)

| metric | before (old build, 5.8h) | deploy 1 (1h53m) |
|---|---|---|
| `processed_bytes_total` rate | 5.8 MB/s | **16.1 MB/s** (2.8x) |
| Repair `worker_error` | every unit (432/432) | 7 |
| Dedup `worker_error` rate | ~124/hour | ~15/hour (**8x fewer**) |
| `pending_repair` | 432, flat all night | **426** — first decrease |
| files retired vs the baseline | — | 122 files / 3.60 GB, incl. both wedged giants |

What is NOT better, and is the thing to watch: `pending_base_rollup` went
314 → 596 and `pending_derived_rollup` 36 → 159 over the same window. Repair
units that now run for 25-50 minutes instead of dying at 15 hold a worker for
that whole time, and the rollup lanes are what gave up the slots. That is a
reasonable trade while a finite repair backlog drains, and it is NOT reasonable
as a steady state — if rollup pending is still climbing after the repair backlog
clears, the answer is a reserved slot for the lanes that gate dashboards (SILK's
floor for flush + L0→L1, prior-art rule 7), not a shorter repair deadline.

## Deploy 2 of 2: the liveness signal was in the wrong place

Deploy 1 fixed repair's *cost* and then hit its own limit: seven
`operation=Repair timeout_seconds=3600` kills against units that were **working**.
The clock was right; the signal was not. Progress was reported only from the
write loop, and `ORDER BY` is a blocking operator — a repair unit emits its first
row only after the whole input is downloaded, decoded and spilled, and on the
fleet's largest files that silent stretch exceeds an hour. Same treadmill, one
order of magnitude further out.

The dedup rewrite has the identical shape and worse: its SQL is a `ROW_NUMBER()`
window **plus** a final `ORDER BY`, two blocking operators between the scan and
the write loop. That is the likely reason dedup units time out at 300 s while
working — the population the write-loop bump can never see.

| # | change | why |
|---|---|---|
| F | `PlanProgress` polls the physical plan's own `output_rows` every 15 s while held | the scan feeding a blocking sort now counts as progress, which is the window the write loop cannot see |
| G | The repair/packing rewrite and the dedup rewrite both watch their plans | both sit behind blocking operators |
| H | Progress reporting moved to a **task-local** | the reporter is four calls below the measurer on three paths; this also deleted the explicit threading deploy 1 carried, so there is one mechanism |

### Reading deploy 2's boot, so the expected does not look broken

- **`maintenance_repair_attempts_reset` will be ABSENT.** The one-shot cursor was
  spent on deploy 1 (`reset=767`). Correct, not a missing migration.
- The ~7 repair units killed at the 3600 s wall have re-accumulated `attempts`
  and some are re-quarantined behind hour-scale backoff floors. **No v2
  migration**: under `PlanProgress` they survive their next claim, and 7 units
  through ~2 quarantine slots is hours, not days. It self-heals.
- **Dedup completions ABOVE 300 s are the success signal**, not a regression:
  `maintenance_unit_slow` reporting `operation=Dedup elapsed_secs > 300` means a
  working unit was allowed to finish instead of being killed and re-run.
- The night now has three measurement windows, split by the two deploys. Do not
  compare across them without saying which is which.

### Two corrections to numbers I quoted earlier tonight

**1. `processed_bytes_total` is NOT fleet throughput.** It is credited at three
sites (`maintain.rs:1841, 2951, 3214`) — dedup, rollup, compaction — but with
each unit's `estimated_decoded_bytes`, and **derived rollups carry a zero
estimate** (the journal's `est_decoded MB p50` for `derived_rollup` is 0). On
deploy 2 the counter sat frozen at 11,500,437,856 while **185 units completed**.
So the "5.8 → 16.1 MB/s" figure is a like-for-like comparison of the same
counter across builds — real, but it measures the compaction/dedup path, not
maintenance as a whole. **Use completions per hour for fleet throughput.**

**2. Completions per hour, per lane** (old build over 5h vs deploy 2 over 45m):

| lane | old build | deploy 2 |
|---|---|---|
| DerivedRollup | 21/h | **155/h** (7.4x) |
| BaseRollup | 121/h | 69/h |
| Dedup | 38/h | 17/h complete, 15/h still timing out |
| Repair | **0** | 5/h **Complete** (not just requeued) |
| HotPacking | 3/h | **0 — not claimed at all** |

Repair units now reach `Complete`, which means the partition has no debt left —
not merely that a bin landed.

### HotPacking, finally attributed

The open item from the baseline (14 completions vs 472 retries) is answered by
the new retry histogram plus the funnel log, and the answer is that **HotPacking
is not failing — it is not being claimed.** Zero HotPacking units ran in 45
minutes with `pending_hot_packing = 17`, and there are no `retry.HotPacking.*`
rows at all. The funnel says why:

```
3 operation=HotPacking      refusal="outranked_by:00000000:2026-09-01:00000000:2026-09-01:files=66"
12 operation=SealedConsolidation refusal="outranked_by:__HIVE_D:2026-05-31:28f62f01:2026-08-29:files=305"
```

Note the HotPacking line: the most-indebted unclaimed cell is outranked **by
itself** (same project, same date on both sides). That is a ranking bug, not a
priority decision, and it is the next thing to chase. SealedConsolidation's
refusal is honest by contrast — it loses to a May repair cell, which is the
slot competition that should resolve as the repair backlog drains.

### Open items — evidence gathered tonight, work not done

- **The dirty-bin queue is dead, and still being written to.** ANSWERED, not
  fixed. `dirty_bin_eligible_total = 0` against a depth of 40,792 is not a
  missing caller: `mod.rs:4887` skips `run_dedup_for_table` for every table that
  declares rollups, deliberately (`954d516`, reverting `d5688fd`) because one
  admitted bin can hold `maintenance_job_sem` for its 3600s stage deadline. Both
  producing tables — `otel_logs_and_spans` and `otel_metrics` — declare rollups,
  so **nothing can ever drain this queue**, while `enqueue_dirty_bin` keeps
  adding to it at ~100/hour and `persist_dirty_bins` rewrites the entire
  40k-entry sidecar on **every** enqueue. The coordinator's Dedup units do the
  real work; this queue is vestigial for those tables. The fix is to stop
  enqueuing for coordinator-owned tables. **Attempted and reverted: the producer
  is the wrong seam.** Gating `enqueue_dirty_bin` on the same predicate the cron
  uses broke six tests that correctly assert the write path's behaviour — the
  queue is the flush's honest record of what changed, and the write path should
  keep producing it. The defect is that nothing CONSUMES it for
  coordinator-owned tables, so the fix belongs at the consumer end: either drain
  it (the cron's comment explains why that is dangerous — one admitted bin holds
  `maintenance_job_sem` for its 3600s stage deadline), or retire those bins
  explicitly, the way `retire_undeclared_tiers` retires work for a deleted tier.
- **HotPacking's 14 completions vs 472 retries is still unattributed.** Note
  that `retry("compaction_debt_remaining")` is a *success* requeue, not a
  failure, so some large share of those 472 is progress. The new
  per-`(operation, reason)` retry histogram answers this directly after the
  deploy — read it before assuming HotPacking is broken.
- **`rollup_oldest_invalidation_age_seconds` is 16.3 days** (an invalidation
  from ~2026-08-15 still unserved). Not chased.
- **Write-side: cap row groups by BYTES, not rows** (finding 6). The 819 MB
  row group is a memory hazard for every reader and rewrite, and it is the
  cause the read path shares with maintenance. `set_max_row_group_bytes` does
  not bind in parquet-rs, so this needs the writer to cut on accumulated
  decoded bytes.
- **The scheduling tuple ranks age above benefit.** RocksDB made
  benefit-per-byte the default in 6.0 and demoted age to an escalation floor;
  see the prior-art doc. Worth revisiting once throughput is fixed — IOx gets
  by with **no ranking at all**.
- **Unconfirmed: `b2048 p1` at a 512 MB pool may WEDGE rather than error.** The
  same variant completes in 47.1 s at 256 MB and 23.4 s at 4 GB, but at 512 MB
  it produced nothing in 15 minutes with the process at 0% CPU. Not chased and
  not confirmed (the process also showed an implausible 3 MB RSS, so it may
  simply have been a stuck leftover). It does not gate the deploy —
  `batch_rows_for` caps a batch at ~8 MB and this variant is 27 MB — but if
  post-deploy units start dying to zero-progress kills rather than errors, this
  is the first thing to reproduce.
- **Admission is a flat `MAX_DECODED_BYTES`.** ClickHouse scales the cap by free
  pool slots so a busy pool only admits small work, which subsumes a deadline.
  The single most transferable idea in the survey, and not done tonight.

### Pre-deploy baseline (2026-09-01, captured before the push)

```
backlog_bytes            3,576,339,029,140
dirty_bin_queue_depth               40,904
oldest_task_age_seconds          1,464,125   (16.95 d)
pending_base_rollup                    314
pending_dedup                        4,905
pending_derived_rollup                  36
pending_hot_packing                     16
pending_repair                         432
pending_sealed_consolidation           169
tasks_pending / tasks_retry     3,140 / 2,717
```

Re-read shortly before the push (image `54e5152`):

```
backlog_bytes            3,580,107,474,211   (+43.5 GB since the first sample)
dirty_bin_queue_depth               40,948
oldest_task_age_seconds          1,466,072
pending_base_rollup                    408
pending_dedup                        4,975
pending_repair                         432   (unchanged all night)
pending_sealed_consolidation           171
sealed_compaction_debt_bytes 1,074,884,551,884
tasks_pending / tasks_retry     3,306 / 2,723
```

Nothing moved except backlog, which grew.

### Success metrics to read in the morning (≥2h after the deploy)

**The repair queue is un-quarantined on boot, once.** Without that the fix
would have been invisible: all 432 units carry `attempts >= 2` with
`worker_error`, which makes them claimable only through
`maintenance_quarantine_slots` (`coordinator_jobs / 8` ≈ 2 of 16) and floors
their retry backoff at `operation_deadline_secs` — now an hour. 432 units
through 2 slots at an hour each is **over a week** before the fix is attempted
once. `reset_repair_attempts` (cursor `__maintenance_repair_single_pass_v1`)
zeroes `attempts`, `retry_reason` and the stamped deadline for non-complete
Repair units on the first boot after this deploy. Look for
`event="maintenance_repair_attempts_reset" reset=432` in the startup log — **if
that line is missing, nothing below will move.**

1. journal: `repair` tasks in state `complete` grows; the 2026-05-30/31
   87576849 slices disappear from `retry/worker_error`.
2. journal: dedup `worker_error` and `Not enough memory to continue external
   sort` stop accruing.
3. `wave_bin_staged.staging_ms` for similar `bytes_in` drops by ~an order.
4. `pending_dedup` and `backlog_bytes` slope negative over a quiet window.
5. `maintenance_processed_bytes_total / uptime` up from the current ~5.8 MB/s.
