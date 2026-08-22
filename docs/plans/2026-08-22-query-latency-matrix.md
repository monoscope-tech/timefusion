# Query latency across 3d/7d/30d — measured, and what actually blocks each shape

Prod `b6d8c86`, 2026-08-22, read-only. Goal: fast queries across 3d/7d/30d for
different projects and different filter types. This is the baseline that
decision-making should start from, plus the attribution of every slow cell to a
named blocker.

## The matrix

Wall-clock ms, `count`-shaped outputs, `rep2` (warm) unless noted. Projects:
`whale` = `dcad860a…`, `mid` = `28f62f01…`, `small` = `d062e010…`.

| shape | whale 3d | whale 7d | mid 3d | mid 7d | small 3d | small 7d |
|---|---|---|---|---|---|---|
| `count(*)` | 402 | 6,121 | 1,405 | 17,044 | 212 | 1,651 |
| `trace_id =` (needle) | 170 | 78 | 2,153 | 79 | 227 | 90 |
| `level = 'ERROR'` | 1,427 | 12,539 | 2,423 | — | 239 | 308 |
| `service_name =` | 131 | 682 | 1,735 | — | 410 | 1,058 |
| `name LIKE 'GET%'` | 2,387 | 4,021 | 4,298 | — | 470 | 173 |
| `time_bucket(1h)` group-by | 2,512 | 10,570 | 5,085 | — | 698 | 355 |
| `ORDER BY ts DESC LIMIT 100` | 333 | 255 | 221 | — | 226 | 168 |
| `kind IN ('server','client')` | — | **22,753** | — | — | — | — |

## 30 days, which is where it breaks

`fail` = `ERROR: canceling statement due to statement timeout` at ~60 s — with
the client's `statement_timeout` set to 90 s and then 120 s. That is **not a
bug**: `DEFAULT_MAX_STATEMENT_SECS = 60` (`timefusion_pgwire_max_statement_secs`)
and `effective_statement_timeout` takes `min(client, server)`, so a client cannot
raise its own ceiling past the server's. The cap is deliberate and tested. What
it means for this table is simply that these queries genuinely exceed 60 s — the
server is reporting the failure, not causing it.

| shape | whale 30d | mid 30d | small 30d |
|---|---|---|---|
| `count(*)` | **fail** | **fail** | 5,814 |
| `trace_id =` (needle) | 95 | 200 | 75 |
| `level = 'ERROR'` | **fail** | **fail** | 221 (0 rows) |
| `service_name =` | 176 (0 rows) | 486 (0 rows) | 9,206 (307k rows) |
| `name LIKE 'GET%'` | **fail** | **fail** | 512 (0 rows) |
| `time_bucket(1h)` group-by | 36,942 | **fail** | 3,148 |
| `ORDER BY ts DESC LIMIT 100` | 431 | 931 | 152 |

The split is not 3d-versus-30d. It is **aggregate versus needle, and it holds at
every window**: the needle and TopK rows stay double-digit-to-few-hundred ms
across 3d, 7d and 30d, while every aggregate degrades until it stops returning.
`small` is the control that rules out "the whale is just big" — 1.8M rows over
30d still costs 5.8 s to count and 9.2 s to filter by service name.

Two things the 3d/7d table says immediately:

- **Point lookups and TopK are solved** (78–330 ms at every window). The tantivy
  + bloom + sorted-footer work landed; nothing here needs more of it.
- **Everything that AGGREGATES or filters on a non-leading column is slow**, and
  it degrades superlinearly with the window: whale `count(*)` goes 402 ms → 6.1 s
  from 3d to 7d; the 1h group-by 2.5 s → 10.6 s.

Cold (`rep1`) is 3–10x worse than warm on the big cells — whale 7d `level=`
measured 39.4 s cold against 12.5 s warm. Any number quoted from a single rep is
a cache measurement, not a query measurement.

## Where the time goes on the raw path

`EXPLAIN ANALYZE`, whale 3d 1h group-by (~2.4 s wall):

```
AggregateExec  partial          elapsed_compute=160ms   output_rows=73
FilterExec                      elapsed_compute=53ms    7.34M → 7.34M (100%)
DedupExec                       elapsed_compute=822ms   input_rows=9.03M → 7.34M
SortPreservingMergeExec         elapsed_compute=502ms   output_bytes=930MB
DeltaScanExec                   elapsed_compute=238ms   9.02M rows, 60 files
DataSourceExec                  row_groups 329 → 309 matched
```

**DedupExec + its SortPreservingMerge are 1.32 s of the ~2.4 s — 55%.** They are
there because the window holds **1.69M physical duplicates in 9.03M rows (23%)**:
merge-on-read versions from monoscope's enrichment UPDATEs. This is not waste
that can be deleted — it is version resolution, and it is correct.

Note `DedupExec`'s required input ordering is deliberate, not incidental: without
it `EnforceSorting` deletes the merge, keep-greatest degrades to keep-FIRST, and
a merge-on-read table answers with the PRE-update row. Removing the SPM to
reclaim its 502 ms would reintroduce a known correctness bug. Not a lever.

## Rollups are the lever, and they are routing NOTHING

```
rollup_hits_full_total    = 0
rollup_hits_hybrid_total  = 0
rollup_misses_total       = 155
rollup_min_contiguous_days = 30      <- coverage is NOT the blocker any more
```

Coverage reached 30 contiguous days. The blocker moved to routing, and it splits
four ways. Each of the four slow shapes was run against prod with the miss
counters diffed around it, so the mapping is measured, not inferred:

| shape | miss reason | count | owner |
|---|---|---|---|
| `kind IN ('server','client')` | `unknown_filter` | 33 | **fixed here** |
| `level = 'ERROR'` | `filter_not_eligible` | 31 | no rollup carries `level` |
| plain `count(*)` | `stale_coverage` | 49 | parallel session (per-slice witness) |
| `time_bucket` group-by | `unknown_filter` / `stale_coverage` | — | both of the above |

Caveat on the per-shape attribution: prod carries live monoscope traffic, so a
single-increment counter diff can be contaminated. The `unknown_filter` mapping
below is not — it is confirmed by the sampled plan text in the logs, matched to
the query text.

## Fix shipped: an OR-of-`text_match` is a hint, and the stripper could not see it

`rollup_promotion_unmatched`, prod, verbatim:

```
promoted = ((kind Eq "client") OR (kind Eq "server"))
           AND (text_match(kind,"client") OR text_match(kind,"server"))
```

and, for `select distinct project_id … kind in (?,?,?,?)` whose `IN` was already
consumed as a dimension filter:

```
promoted = (text_match(kind,"client") OR text_match(kind,"consumer")
            OR text_match(kind,"producer") OR text_match(kind,"server"))
```

`optimizers::tantivy_rewriter` ADDITIVELY ANDs `text_match` hints beside a
predicate it can accelerate, and by its own stated invariant never removes the
original comparison — so a hint is always semantically redundant.
`strip_index_hints` already dropped them, but only when the conjunct's TOP node
was a `text_match` call. The `IN`-list spelling expands to an **OR of per-item
`text_match` calls**, which is a `BinaryExpr(Or)` at the top and therefore
invisible to it. The hint survived into the promoted filter, matched no declared
measure, and the whole query declined `unknown_filter`.

The fix recurses `hint_column` through `Or`, accepting the subtree only when
EVERY leaf hints the SAME column — a mixed OR is a real predicate and dropping it
would widen the filter. The existing guard still applies on top: a hint is only
dropped when this AND level already compares that column directly, so a
`text_match` the user wrote against some other column is preserved and the filter
correctly fails to match.

Regression test `an_in_list_hint_or_tree_does_not_become_a_residual`, which fails
before the change with `hinted=Err(UnknownFilter)` against `plain=Ok(routed)` —
and the plain control routes through `row_filters: ["kind = 'server' OR kind =
'client'"]`, so this shape genuinely serves from the 1h tier once the hint is
gone. The blocked query measured **22.8 s** at 7d.

## Also shipped: the manifest age bound could not fire

Last session's 60-second durability bound is not merely unobserved — it is
**broken, and prod says so**: `tantivy_backfill_built = 2` next to
`tantivy_manifest_commits = 0` at 44 minutes of uptime, well past the bound.

Both `full` and `stale` were evaluated only for the project whose build had just
completed (`pending.get(&pid)`, `pending_since.get(&pid)`). A project with a
single build per pass therefore set its `pending_since` once and was never
re-examined — nothing else could flush it, so it waited for a pass end that on
this box has never arrived. That is the same failure the bound was added to fix,
reintroduced per project.

`due_manifest_flushes` now sweeps every pending project on each build completion.
It is a pure function, so the case is a unit test rather than another prod
observation. A build costs 4-5 minutes, so a whole-map sweep runs at most every
few minutes and its cost is noise against that.

## Also shipped: the prefilter skip breakdown

`prefilter_skipped` was incremented from two call sites — `decide_prefilter`'s
three exits and the search-abort branch — so the standing "63% of prefilter
attempts are skipped and thrown away" could not be attributed to a decision at
all. Six named counters now split it; the first three are decisions (the index
answered, the rule declined it), the rest are the index failing to answer, and
the fixes are opposite.

## What is NOT worth doing, with the measurement that says so

- **Removing DedupExec's SortPreservingMerge** (502 ms, 930 MB). Its required
  ordering is what keeps keep-greatest from degrading to keep-first under
  merge-on-read. Correctness, not overhead.
- **A provider cache for file-pruned scans** — `pruned_build_us` is 0.10 ms,
  0.1% of the pruned-scan path. Already recorded; restated because it keeps
  looking attractive.
- **Adding `level` as a rollup dimension** to fix `filter_not_eligible`. It would
  work, but it is a spec change requiring a full rebuild across 30 days, which is
  wall-clock physics and not an overnight change.

## Post-deploy verdict (`ebfa7e0`)

**The hint strip works, confirmed directly.** `rollup_promotion_unmatched` now
logs a clean promoted filter — `((kind Eq "client") OR (kind Eq "server"))`, the
`text_match` OR gone — and the `select distinct project_id … kind in (?,?,?,?)`
query that monoscope fires every 5 minutes went from one unmatched decline per
spec per run to **zero**.

**The prefilter breakdown works, and it refutes the standing hypothesis.**
`prefilter_attempts=65, used=27, skipped=38`, and the 38 splits as
`no_index_or_cap` **29 (76%)**, `low_selectivity` 9 (24%), `field_coverage_gap`
**0**, everything else 0. They sum exactly. `field_coverage_gap` was written into
`pg_compat.rs` as the most plausible cause and it is flatly zero — the wasted
fan-out is the index not being there, not a rule declining a usable one. That
sends the fix back to coverage, not to `min_selectivity_pct`.

**Everything I measured about ROUTING after the deploy is invalid**, and this is
worth more than the measurements were:
`rollup_min_contiguous_days` read **0** on the five-minute-old container against
**30** before the restart. The gauge is process-scoped and rebuilds after boot.
With coverage empty the router does not attempt routing at all — so a novel
group-by shape (fresh literals, fresh bucket width, past the plan cache) produced
**neither a hit nor a miss**, and `rollup_misses_total` sat flat at 3 across
several distinct queries. A flat miss counter after a deploy is an artifact of
the restart. **Absence of a miss is not evidence of a hit.** Read
`rollup_min_contiguous_days` before quoting any routing number.

So "does anything route now" is still open, and needs a re-measure once coverage
is back at 30 days. Note also that `kind IN ('server','client')` has a second,
independent blocker even with the hint gone: no declared measure matches that
filter.

`tantivy_manifest_commits` is likewise unverifiable on a young container — the
reconcile cron fires at minute 20 and `tantivy_backfill_built` was still 0.

## Why the routing re-measure has not happened

It needs a quiet window and there has not been one. Prod restarted twice during
this session — `ebfa7e0` at 18:41, then `5062a7d` at ~18:55 — and each restart
zeroes `rollup_min_contiguous_days`, which gates whether the router attempts
anything at all. Fifty minutes after the second deploy it still reads 0, along
with `tantivy_uncovered_files` and `tantivy_backfill_built`.

This is the deploy-train problem, and it is worth stating as a constraint on the
whole approach rather than as an excuse: **on this box the gauges that answer
"did the fix work" take longer to rebuild than the interval between deploys.**
Any routing conclusion drawn inside that interval is an artifact. The compaction
chart shows the same constraint from the write side — six sealed days frozen
because units are re-claimed rather than finished.

## Open, in priority order

1. `stale_coverage` (49 misses, and it owns plain `count(*)`) — the per-slice
   witness change in flight.
2. `filter_not_eligible` (31) — `level` / `status_code` are not rollup
   dimensions. Needs a spec decision, then a rebuild.
3. Read the new `prefilter_skipped_*` rows once prod has an hour on them.
