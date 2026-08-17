# Live-tail read amplification — analysis, and why the obvious fix is unsafe

Status: **analysis only, nothing shipped beyond the correctness precondition (#129).**

## The measurement

A one-hour window over the whale (`28f62f01…`), `EXPLAIN ANALYZE`:

```
AggregateExec                       output_rows=1
  FilterExec                        output_rows=159.8K   elapsed_compute=2.27ms
    DedupExec keys=[timestamp…]     output_rows=159.8K   elapsed_compute=78.64ms
      SortPreservingMergeExec       output_rows=410.4K   elapsed_compute=59.36ms
        UnionExec                   output_rows=410.4K   elapsed_compute=39.27ms
          OrderingProbeExec leg=mem
          OrderingProbeExec leg=hot     output_rows=236.4K   (local 54 MB .arrow)
          OrderingProbeExec leg=delta   output_rows=202.5K   (48 file groups, OVH S3)
```

**410K rows read to answer 160K.** Total `elapsed_compute` across every operator
is ~320 ms, against 5–11 s of wall clock — so this is not CPU. The hot tier and
Delta hold the same recent rows, the Delta leg re-fetches from object storage
what is already on local disk, and `DedupExec` throws the duplicates away.

Supporting counters: `hot_tier.unproven_windows_total` 2286 against
`read_hits_total` 3163 — roughly 72% of served hot files fail to exclude their
window from Delta.

Cost breakdown of a shipbubble 1-day dashboard query (warm): 139 ms rollup leg
+ ~6 s live tail. The tail is the whole gap to the 1 s target; coverage depth
does nothing for it, because a *rolling* window always has an uncovered tail.

## Why the exclusion fails

`plan_leg` only lets a file exclude its window from the Delta leg when

```rust
m.covers_window && files_per_bucket.get(&m.bucket_id) == Some(&1)
```

`covers_window` means "this file holds every row the bucket carried", which is
true per drain. A bucket drained twice has two files, each holding only part of
the span, so neither may claim it — hence the count gate.

## The obvious relaxation, and why it is NOT safe yet

The tempting generalisation: allow the claim when **all** of a bucket's files
are served and all are `covers_window`, since their union is the whole bucket.

Two hazards, one fixed and one open:

1. **FIXED in #129.** `gc` unlinks by `end_ts` and a bucket's files carry
   different `end_ts`, so a cutoff could fall between them; `invalidate_range`
   likewise drops only overlapping files. Either left a survivor looking like a
   single-file bucket, reclaiming a span it half held — a *silent
   under-report*, present in the code before any relaxation. Deletion is now
   bucket-atomic, so "all files present" == "all files ever written".

2. **OPEN.** `demote` is skipped wholesale under queue pressure
   (`demote_skipped_total` = 20 in prod; the log calls the result "a permanent
   coverage hole served from Delta"). If drain 1 demoted and drain 2 was
   skipped, the bucket holds ONE file that claims `covers_window` — and the
   union is genuinely incomplete. That breaks the relaxation, and note it also
   breaks the CURRENT single-file gate. Whether it is reachable depends on
   whether a bucket can be drained twice with a skip in between.

Until (2) is resolved, relaxing the gate converts a slow query into a wrong
one. That is a strictly worse failure: a slow dashboard is visible, an
under-counted one is not.

## Next steps, in order

1. Determine whether a bucket can be drained twice with a demote skip between
   the drains. If it can, `covers_window` is not a sound claim even today and
   needs to carry the drain's own completeness, not the caller's constant.
2. Only then relax the gate to "all files of the bucket served and all
   `covers_window`".
3. Re-measure the union row count; the target is the delta leg contributing
   ~0 rows for windows the hot tier fully holds.

## What is NOT the problem

- Not small files: the 1-hour window's metadata scan saw 1–2 active files,
  ~2.5 MB, in 70 ms.
- Not MemBuffer size: 824 MB across 11 projects at the time of measurement.
- Not planning overhead: a 4-row project answers the same shape in 162 ms warm.
