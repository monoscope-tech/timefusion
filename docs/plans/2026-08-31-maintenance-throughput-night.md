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

### 3. Next: attribute the cost on the real file
Downloading the two wedged parquet files to bench against locally, rather than
synthetic rows — the shape (63 KB rows, variant blobs, ~200 columns) is
probably what makes it slow.
