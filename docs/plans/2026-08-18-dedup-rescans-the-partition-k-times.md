# The dedup rewrite decodes each partition ~K times

Status: **root-caused from a live CPU profile. Amplification now logged
(`dedup_rewrite_sharded`). The redesign below is NOT implemented.**

This is the answer to "why is maintenance slow — 80 hours on such a big box?".
It is not a big backlog moving at a fair rate. It is a modest backlog moving
through a loop that reads the same bytes tens of times.

## The measurement

`perf record -F 99 -g` on the live prod process, 83,399 samples, 2026-08-18,
with 23 of 48 cores busy and RSS at 12.6 GB of 120 GB. Top symbols, all on
`maintenance-wor` threads:

```
5.71%  md5::compress::compress            <- larger than the decompression
5.41%  native_queued_spin_lock_slowpath
3.97%  clear_page_erms
3.60%  smp_call_function_many_cond
3.20%  ZSTD_decompressSequences_bmi2      <- the actual work
1.96%  flush_tlb_func
1.58%  arrow ... GenericByteViewArray::from_iter
```

The call graph puts the MD5 under `FilterExecStream` ->
`datafusion_functions::crypto::md5::Md5Func` — DataFusion's **SQL `md5()`**,
evaluated per row inside a filter.

## Why SQL md5() is in a maintenance filter

`dedup_partition_range_limited` splits a rewrite into K passes bucketed by

```sql
substr(md5(concat_ws(chr(31), <dedup keys>)), 1, 2)   -- 256 buckets
```

and runs **one query per bucket**, each scanning the same target files. The
code says so:

> N shards paid N scans + sorts + writes back to back

So a partition is decoded K times and every row is MD5-hashed K times, to keep
1/K of the rows each pass.

## How large K gets

```rust
shards = ceil(est_decoded_bytes / decoded_budget)          // clamp 1..=256
est_decoded_bytes = max(rows x 4096, compressed x 12) x 2  // and it is ACCURATE — see below
decoded_budget    = 512 MiB                                // MAX_DECODED_BYTES
```

For a 1.8 GB partition (measured on shipbubble, and that was a ONE-MINUTE
window): `1.8 GB x 12 x 2 / 512 MiB` ~= **84 shards**. By the row term, with
10M rows, ~160. K is clamped at 256.

One consequence worth stating separately:

- **Raising `decoded_budget` divides K directly**, but that Arrow decode is
  explicitly OUTSIDE the memory pool, so it trades CPU for untracked heap —
  which is the exact shape of the 2026-08 OOM series. Not a free knob.

This also explains the ~18% of CPU in kernel memory management
(`clear_page_erms`, TLB shootdowns, `mmap_lock` contention): K passes allocate,
touch and free the same buffers K times. Jemalloc `dirty_decay_ms:0` made each
of those frees an immediate `madvise` — addressed separately in #153, which took
that ~15% down to ~3.8% measured before/after on prod.

## The redesign

The memory bound is real: a whole partition does not fit, so it must be
processed in pieces. The defect is that the pieces are produced by **re-reading
the source once per piece**.

Two candidate shapes, both O(1) passes over the source:

**A. External hash partition.** Scan once, route each row to one of K spill
files by hash, then dedup each spill file independently. Cost: 1 source read +
1 spill write + 1 spill read, versus K source reads. Keeps the existing
per-shard memory bound exactly, since a shard is still processed alone.

**B. Sort-based dedup.** Sort the partition by `(dedup keys, tiebreak)` with
DataFusion's `SortExec`, which already spills, then take the winner per key in
one streaming pass. Cost: 1 read + external sort. Removes bucketing — and the
MD5 — entirely. Also produces sorted output, which the writer wants anyway.

B is the smaller change in concept and reuses machinery that already exists and
already spills. A preserves the current per-shard commit structure more
directly.

## Constraints any implementation must keep

These are the guards that make the current path safe, and they are not
negotiable:

- `dedup_rewrite_counts_match(reread, expected_live, output, expected_logical)`
  — the two independent conservation checks. The first proves every target
  file's live rows were re-read; the second proves exactly one row per distinct
  key was emitted. The 2026-08-03 data loss (63k rows re-read against 5.8M live)
  is what these exist to stop.
- Deletion-vector cardinality is subtracted from `num_records` when computing
  expected live rows.
- Tombstones are retained, not dropped.
- Per-shard staging with an all-or-nothing commit; a partial failure must stage
  nothing.

## The estimate is ACCURATE — measured, and it kills the obvious fix

> **Correction.** An earlier version of this document called the estimate
> "deliberately pessimistic" and proposed tuning `bytes_per_row` and `inflation`
> down as a cheap win. That was inference from the compression ratio looking
> implausible. It was wrong, and acting on it would have caused OOMs rather than
> speedups.

`dedup_shard_decoded` (#157), prod 2026-08-18:

```
shards=8  rows=63046  actual_decoded_mb=515  predicted_decoded_mb=491
```

Actual **515 MB** against a predicted 491 MB. The estimate slightly UNDER-states,
and this is the streamed figure, which counts POST-dedup rows — so the true input
ratio is higher still. 63,046 rows at 515 MB is ~8.5 KB per row decoded, above
the `bytes_per_row = 4096` the estimate uses.

That also settles the earlier `shards=18, est_decoded_mb=9191, compressed_mb=130,
files=1` line, which looked absurd: a 77x compression ratio is simply real for
ZSTD over repetitive Variant/JSON columns. One 130 MB file genuinely decodes to
~10 GB, and reading it 18 times is ~180 GB of decode work.

**So there is no constant to tune.** K is large because the data really is that
large decoded. Only two things reduce it:

- the redesign below — one pass instead of K
- raising `MAX_DECODED_BYTES`, which trades CPU for heap OUTSIDE the memory pool.
  That is the shape of the August OOM series and remains refused.

## The cheapest remaining lead: the streaming branch may not need sharding at all

Not yet verified — this is the next experiment, not a conclusion.

The `limits.is_some()` branch does not `collect()`. It runs

```sql
SELECT … FROM (SELECT …, ROW_NUMBER() OVER (PARTITION BY keys ORDER BY tiebreak) …) WHERE __tf_rn = 1
```

and streams the result straight into the writer. A window function over a sorted
input is exactly shape B (sort-based dedup) — the machinery is ALREADY there and
already spills under the memory pool. If that is true, the sharding on top of it
is redundant for this branch, and K can go to 1 by not sharding rather than by
rewriting anything.

The question that decides it: does DataFusion plan this as a `BoundedWindowAggExec`
that streams per partition, or as a `WindowAggExec` that buffers the whole input?
The first is bounded; the second is not, and the sharding is load-bearing.

**Test it, do not reason about it**: force `shards = 1` against a partition known
to decode past the budget, under a deliberately small pool, and see whether it
spills or fails. The collecting branch is separate and genuinely does need the
bound.

## Cheaper wins

1. **Done (#156):** replaced `md5` with `hash_bucket`. It was 5.87% of all CPU and
   the largest single symbol on the box; it is now absent from the profile.
2. **Done (#154, #157):** log K, its inputs, and what a shard actually decodes to
   — which is what turned the tuning idea above from plausible into refuted.

## How to tell it worked

- `dedup_rewrite_sharded` shards falls to single digits, or the event stops
- ~~`md5::compress` leaves the profile~~ — done, #156
- maintenance completes more units per core-hour, not merely more units
