# Seven windows, six projects, monoscope's real SQL — where sub-second breaks

2026-08-22, prod, read-only. Goal: **sub-second at every window (1h/3h/24h/3d/
7d/14d/30d) for six named projects.** This page is the measured baseline plus
the attribution of every slow cell to a named blocker.

This is deliberately not the same experiment as
`2026-08-22-query-latency-matrix.md`. That page measured **hand-picked filter
shapes** at 3d/7d/30d on three projects. This one measures **the SQL monoscope
actually emits** (`docs/monoscope-query-shapes.md`, verbatim — the
`extract(epoch from time_bucket(...))` projection, the `COALESCE(dim,'null')`
wrapper, `COUNT(*) FILTER`, `percentile_agg`) across **seven** windows and
**six** projects. Two of the three findings in the 2026-08-22 dashboard profile
existed only in monoscope's exact syntax, so hand-written equivalents measure a
system nobody runs.

## Method, and the two things that limit it

- Serial, one connection. A single wide scan has selected 32 GB before; two
  concurrent is a self-inflicted OOM.
- Two reps per cell; **warm (rep2) is the quoted number**. A cell over 15 s
  skips rep2 rather than paying twice — those are marked `*` and are COLD.
- `fail` = the 60 s server cap (`DEFAULT_MAX_STATEMENT_SECS`, which
  `min(client, server)` makes un-raisable from the client). `fail'` = implied:
  once a shape times out at some window, wider windows are skipped rather than
  paying another 60 s for a known-monotone failure.
- Bucket width follows what `bin_auto` would pick: 1m/5m at 1h–24h, 1h at
  3d–7d, 6h at 14d–30d.

**Limit 1 — a deploy landed mid-sweep.** Prod went `5062a7d` → `ba87ed3` at
19:32. Cells after that line ran on a different build with cold caches and
`rollup_min_contiguous_days` reset to 0. The boundary is marked in
`matrix.csv`. It does not change any conclusion below, all of which are
order-of-magnitude, but no single cell should be quoted to three digits across
it.

**Limit 2 — cold/warm is worth more than one rep.** p3's 1h row is *slower*
than its 3h row (1,684 ms vs 422 ms) purely because 1h was the first touch on
that project. Any number from a single rep is a cache measurement.

## The matrix (warm ms)

### p1 `87576849-4941-49d3-a15d-680fef88a1a8`

| shape | 1h | 3h | 24h | 3d | 7d | 14d | 30d |
|---|---|---|---|---|---|---|---|
| throughput | 199 | 312 | 1,136 | 3,297 | 9,857 | fail | fail' |
| group_by_service | 347 | 324 | 2,546 | 14,412 | 20,582* | fail | fail' |
| p95_latency | 472 | 262 | 5,930 | 8,744 | 5,875 | fail | fail' |
| error_rate | 287 | 316 | 1,349 | 5,108 | 9,045 | fail | fail' |
| log_list (TopK) | 459 | 503 | 758 | 424 | 416 | **467** | **OOM** |
| dcount_service | — | — | 1,277 | — | 8,896 | — | fail |
| topk_services | — | — | 221 | — | 509 | — | fail |
| facet_service | — | — | 3,564 | — | 15,650 | — | fail |

### p2 `edb04135-1ee1-435e-8b01-2f969eb01c2b` — the planning-floor control

p2 holds 17.7M rows at 30d but **zero rows in every window up to 14d**: all its
data is older than 14 days. Its 1h–14d row is therefore the cost of asking,
with nothing to read.

| shape | 1h | 3h | 24h | 3d | 7d | 14d | 30d |
|---|---|---|---|---|---|---|---|
| throughput | 285 | 146 | 180 | 300 | 276 | 332 | fail |
| group_by_service | 142 | 263 | 180 | 227 | 241 | 304 | 52,620* |
| p95_latency | 172 | 121 | 149 | 159 | 268 | 292 | fail |
| error_rate | 388 | 209 | 140 | 207 | 265 | 547 | 53,826* |
| log_list | 279 | 293 | 143 | 345 | 302 | 483 | 1,588 |
| dcount_service | — | — | 130 | — | 277 | — | 23,618* |
| topk_services | — | — | 295 | — | 275 | — | fail |
| facet_service | — | — | 168 | — | 237 | — | fail |

### p3 `00000000-0000-0000-0000-000000000000` — the shared unified-default table

| shape | 1h | 3h | 24h | 3d | 7d | 14d | 30d |
|---|---|---|---|---|---|---|---|
| throughput | 1,684 | 422 | 848 | 1,954 | 4,806 | 17,040* | fail |
| group_by_service | 1,745 | 443 | 3,674 | 5,284 | 33,896* | err | fail |
| p95_latency | 2,806 | 249 | 1,041 | 2,420 | 6,046 | fail | fail' |
| error_rate | 1,030 | 295 | 1,171 | 3,581 | 39,224* | 60,004* | fail |
| log_list | 1,229 | 218 | 694 | 200 | 292 | 1,232 | 1,114 |
| needle_trace_id | 458 | 260 | 272 | 395 | **43,401\*** | 1,155 | — |
| dcount_service | — | — | 1,093 | — | 6,660 | — | fail |
| topk_services | — | — | 5,828 | — | 37,857* | — | — |
| facet_service | — | — | 1,309 | — | 31,903* | — | — |

### p4 `dcad860a-9a98-4c9e-9e69-20d52dcf90e2` — the whale

| shape | 1h | 3h | 24h | 3d | 7d | 14d | 30d |
|---|---|---|---|---|---|---|---|
| throughput | 1,542 | 562 | 1,367 | 2,848 | 6,734 | 15,190 | 30,901* |
| group_by_service | 1,032 | 570 | 2,874 | 4,951 | 7,437 | 16,956* | fail |
| p95_latency | 653 | 648 | 4,774 | 7,306 | 11,875 | fail | fail' |
| error_rate | 380 | 527 | 1,811 | 4,905 | 9,140 | 23,128* | fail |
| log_list | 623 | 402 | 414 | 303 | 693 | 659 | **879** |
| needle_trace_id | 246 | 518 | 260 | 258 | **35,106\*** | 1,032 | 6,012 |
| dcount_service | — | — | 1,501 | — | 8,411 | — | 44,148* |
| topk_services | — | — | 4,372 | — | 29,144* | — | fail |
| facet_service | — | — | 1,218 | — | 29,673* | — | fail |

### p5 and p6 — measured on a later build, do not merge with the above

p5 `be87ebc1` and p6 `28f62f01` were re-measured after the session lost its
scratchpad, on prod `a7a4eb0`+ with cold caches — a **different build** from
p1–p4's `5062a7d`. Per this page's own boundary rule they are not comparable
cell-for-cell, so they are reported as completion status rather than latency,
in the pass/fail table of
`2026-08-22-make-14d-30d-complete.md`. Headline: p5 completes **5 of 5** shapes
at 14d and 3 of 5 at 30d — the best result of the six — while p6 completes 3 of
5 and 1 of 5. The same aggregate-versus-needle split holds: p6's `log_list`
returns in 2,151 ms at 30d while every one of its aggregates fails.

The cold-cache penalty on the later build is itself worth noting: p5's 1h
throughput read 2,054 ms there against 189 ms on the earlier warm run of the
same shape. Any cross-build comparison on this page is a cache measurement.

## What the matrix says

**1. Nothing is sub-second anywhere, including 1 hour.** The best cells in the
whole table are ~150–350 ms, and they are the ones returning *no rows*. The
target is missed at every window, not just the wide ones — so "make 30d fast"
is the wrong framing of the goal. There are two separate problems with two
separate fixes.

**2. The split is aggregate-versus-needle, and it holds at every window.**
`log_list` (the log-explorer TopK, `ORDER BY timestamp DESC LIMIT 251`) is
**flat**: 459 → 503 → 758 → 424 → 416 → 467 ms from 1h to 14d on p1. It does
not care how wide the window is. Every aggregate shape degrades
superlinearly over the same range and stops returning at 14d. This reproduces
the earlier page's finding on four more windows and four more projects.

**3. The floor is planning, and p2 proves it independently.** p2 answers in
140–550 ms with **zero rows** across 1h–14d. Nothing is read, decoded or
deduped, and it still cannot reach 300 ms reliably. That is the same
window-scaled planning cost the ghost-project control in
`2026-08-21-planning-floor-attributed.md` (deleted 2026-08-24) found,
reproduced here on a real
project by accident. **No scan-side or rollup work moves this number**, and it
alone is ~30–50% of the sub-second budget at every window.

**4. The rollup tier answered nothing.** Across ~216 routing decisions spanning
every monoscope dashboard shape, six projects and seven windows,
`rollup_hits_hybrid_total` and `rollup_hits_full_total` were **0** while
`rollup_misses_total` went 37 → 253. Coverage was *not* the blocker for most of
that: `rollup_min_contiguous_days` read 30. The reason breakdown, diffed across
the sweep, put `stale_coverage` first (+25), then `filter_not_eligible` (+10)
and `not_built` (+8). A concurrent session reached the same conclusion by a
different route (`82873a6`) and shipped the sub-attribution counters in
`ba87ed3`.

**5. `log_list` at 30d fails on MEMORY, not time** — a distinct bottleneck:

```
Resources exhausted: unordered merge-on-read dedup exceeded its 2048 MiB
per-query [budget]
```

A `LIMIT 251` over `ORDER BY timestamp DESC` should never need to dedup a
30-day window. This is the one cell in the table whose fix is neither planning
nor rollups.

**6. The needle path is not uniformly solved.** p3's `trace_id` lookup is
260–460 ms at 1h/3h/24h/3d and then **43.4 s at 7d** (cold). The earlier page's
"point lookups and TopK are solved" holds for dedicated projects but not for
the shared unified-default table.

> **RESOLVED 2026-08-23 — the 7d cliff is a cold first touch, not a band.**
> Re-measured three reps per window, both projects:
>
> | | 3d | 7d | 14d |
> |---|---|---|---|
> | p3 | 3239 / 2678 / 2201 | **33489** / 2600 / 1679 | 4610 / 3942 / 1850 |
> | p4 | 2569 / 2434 / 1575 | **46127** / 4477 / 1102 | 3914 / 1714 / 1799 |
>
> Warm, 7d is *faster* than both neighbours on both projects, and the counter
> diffs are the same shape at every window — `prefilter_used=3`,
> `tantivy_scan_calls=3`, one bloom/raw split, with `tantivy_raw_files_total`
> rising smoothly (3615 → 6417 → 7246). There is no 7d-specific plan and no
> coverage boundary. 14d looked fine only because the sweep ran windows
> narrowest-first, so the 7d rep paid the warm-in that 14d then reused.
>
> What survives is not an anomaly but a level: a point lookup costs **1.1–3.2 s
> warm and 15–40× that cold**, so the real item is cold-start cost. Any future
> sweep must run ≥2 reps per cell or it measures cache state.

**7. A single 30-day aggregate makes the box unreachable.** During the sweep, a
trivial `timefusion_stats` query could not open a connection —
`Operation timed out` — and the same condition killed the harness's first run.
That is an availability symptom, not merely a latency one.

## Where the budget goes, and what each fix can buy

| term | size | fixable by |
|---|---|---|
| planning | ~150–550 ms, scales with window, independent of data | not rollups, not scan work — see below |
| DedupExec + SortPreservingMerge | ~55% of the raw path (prior page, 23% real duplicates) | certification, not deletion — the ordering is correctness |
| scan/decode | the remainder, scales with files in window | compaction, bloom/tantivy pruning |
| rollup routing | would remove scan+dedup entirely for aggregates | `stale_coverage` (in flight) |

`dedup_denied_never_certified_pct` read **100.0** for the whole sweep with
`cert_granted_total = 0`, so every cell above paid full merge-on-read dedup.

## Open questions this page does not answer

- The planning floor's internal split. Needs `wall − EXPLAIN ANALYZE execution`
  per window; EXPLAIN ANALYZE times execution only.
- ~~Whether monoscope's `AND duration IS NOT NULL` is a routing blocker.~~
  **Answered: yes.** A counter-diff A/B isolating exactly that predicate flips
  the miss from `not_built` to `filter_not_eligible`, and the mechanism is
  confirmed at `src/rollup.rs:1741`. The latency charts cannot route at any
  window regardless of coverage. Written up in
  `2026-08-22-make-14d-30d-complete.md` §"The p95 finding", including the
  all-measures condition the fix must respect to avoid changing `count(*)`.
