# `log_list` at 30d fails on the footer-repair backlog, not on the read path

2026-08-23. Closes the diagnosis half of "bound `log_list` so TopK never
deduplicates the whole window". The conclusion moves the fix to a different
workstream, so the reasoning is recorded rather than the patch.

## The failure

```
Resources exhausted: unordered merge-on-read dedup exceeded its 2048 MiB
per-query limit; narrow the time window or compact unsorted files
```

p1 (`87576849-…`) fails this way on `SELECT … ORDER BY timestamp DESC LIMIT
251` at 30 days. p4 answers the identical shape at 30 days in 879 ms. The
window is the same, so window width is not the variable.

## What the two plans differ in

Read from prod (`11cd17a`), `EXPLAIN` only — no execution.

The whale (`dcad860a-…`), 30d, works:

```
FilterExec: … fetch=251
  DedupExec: keys=[timestamp, id], mode=bounded[timestamp]/greatest
    SortPreservingMergeExec: [timestamp@0 DESC]
      UnionExec
```

p1, 30d, fails:

```
SortExec: TopK(fetch=251), expr=[timestamp@1 DESC]
  FilterExec: … AND deleted@2 IS DISTINCT FROM true
    DedupExec: keys=[timestamp, id], mode=full-set/greatest
      CoalescePartitionsExec
        UnionExec
          OrderingProbeExec: leg=mem   → output_ordering=timestamp@0 DESC
          OrderingProbeExec: leg=delta
            …
              DeltaScanExec
                UnionExec
                  DataSourceExec: file_groups={48 groups}, output_ordering=[timestamp@0 DESC]
                  DataSourceExec: file_groups={38 groups}      ← no output_ordering
```

**38 of p1's 86 file groups carry no footer `sorting_columns`.** They land in
their own branch of the `DeltaScanExec` union, that union therefore declares no
ordering, and the delta leg as a whole declares none. `ordered_children` then
bails (the Delta leg is deliberately marked unsortable), no
`SortPreservingMergeExec` is built, `detect_bound` returns `None`, and
`DedupExec` runs `full-set`.

Only `full-set` charges `check_unbounded_growth`, and only `full-set` has no
`LIMIT` early termination. Everything downstream of that one missing footer
property follows mechanically. `SortExec: TopK` appears in p1's plan for the
same reason — dedup's output is no longer ordered, so the `ORDER BY` needs a
sort that the whale's plan does not.

## Why the obvious read-path fixes are wrong

**Sort the unordered branch.** Already tried and reverted twice — 2026-08-02,
and the 2026-08-07 sort-only-the-unordered-branch attempt whose unspillable
per-partition merges saturated 24 GB. The code comment at the `ordered_children`
call site records the conclusion: footer-less files need REPAIR, not read-time
sorting.

**A top-K watermark inside `DedupExec`.** Attractive and nearly sound: the dedup
key is `(timestamp, id)`, so every version of a row shares its full key, and
once K distinct keys with `ts ≥ W` are retained no row below `W` can place in
the top K or be a version of anything retained. It fails on the plan above. A
`FilterExec` carrying `deleted IS DISTINCT FROM true` sits BETWEEN the sort and
the dedup, so some of those K retained rows are dropped after dedup — and then a
row below `W` *is* needed. Making the watermark aware of that predicate means
evaluating it inside the operator. Not worth it for a problem whose real cause is
one workstream over.

**Raising the 2 GiB budget.** It converts a query error into a box-wide OOM. The
budget is doing its job here.

## What shipped instead

`dedup_bounded_total`, `dedup_full_set_total` and `dedup_full_set_pct` in
`timefusion_stats`. The mode was visible only in `EXPLAIN`, which means the
footer-repair backlog could only ever be discovered as a user-facing failure on
one project. It is now a number, and it is the number to watch while footer
repair drains.

## What actually fixes it

Footer repair / compaction on p1's 38 unordered file groups. Tracked as 2.3 in
`2026-08-22-make-14d-30d-complete.md`. One repaired branch flips the whole leg
back to `bounded[timestamp]`, which restores `LIMIT` early termination and takes
the 2 GiB budget out of the path entirely.
