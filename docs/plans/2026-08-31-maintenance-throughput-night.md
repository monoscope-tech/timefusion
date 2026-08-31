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

## Planned changes (one deploy, each behind an env knob)

| # | change | knob | why |
|---|---|---|---|
| A | Repair rewrites in ONE pass (spilling sort) instead of N event-time slices | `TIMEFUSION_REPAIR_SINGLE_PASS` | 13 rescans of an unprunable 1-row-group file is the wedge |
| A2 | Raise the Repair unit deadline (900s → 3600s) or scale it by real bytes | `TIMEFUSION_REPAIR_DEADLINE_SECS` | staging records its `StagedIntent` only at the END, so a timeout mid-staging loses everything and resume can never engage |
| B | Coordinator pool cap `pool/4` → `pool*3/5` (≈4.2 → 8 GB, i.e. jobs × `MAX_DECODED_BYTES`) | `TIMEFUSION_COORDINATOR_POOL_FRACTION` | 265 MB per worker vs a 512 MB admission ceiling is why 243 dedup units died on external-sort memory |
| C | Batch size derived from measured row width (target ~8-16 MB/batch, clamp [256, 8192]) instead of the constant 256 | `TIMEFUSION_MAINTENANCE_BATCH_TARGET_BYTES` | 256 rows is right for 63 KB whale rows and ~30x too small for ordinary ones; the fleet runs at 0.3-1.7 MB/s |
| D | `split_time_task` declines for Repair | — | bisecting a whole-file unit sheds nothing and mints duplicates |
| E | Retry reasons counted per (operation, reason) instead of one "last reason" string | — | the missing instrument: today `retry_reason` is a single `String` (observability.rs:35) |

### Success metrics to read in the morning (≥2h after the deploy)

1. journal: `repair` tasks in state `complete` grows; the 2026-05-30/31
   87576849 slices disappear from `retry/worker_error`.
2. journal: dedup `worker_error` and `Not enough memory to continue external
   sort` stop accruing.
3. `wave_bin_staged.staging_ms` for similar `bytes_in` drops by ~an order.
4. `pending_dedup` and `backlog_bytes` slope negative over a quiet window.
5. `maintenance_processed_bytes_total / uptime` up from the current ~5.8 MB/s.
