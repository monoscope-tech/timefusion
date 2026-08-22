# Dashboard query profile — where the time actually goes

2026-08-22. Prod `d327df8`, ~6h uptime at start, **no restart during the run**
(verified via `docker service ps`, so the process-scoped counters below are
continuous). Read-only throughout. Raw cells: scratchpad
`bench_20260822/{cells.jsonl,ab.jsonl,run.log}`.

## Method (matches the 08-20 / 08-22 matrices so cells are comparable)

Serial, one query at a time. `SELECT 1` control before every cell (35–240 ms,
median ~60 ms — no load skew). Two reps per cell, **warm rep reported**.
`statement_timeout` 12 s at 1h/24h, 30 s at 3d/7d/14d. One attempt, never
retried; a shape that times out at a narrower window is skipped wider.
`timefusion_stats` diffed around every cell — that diff is what makes the
attribution below possible, and it is the whole reason this run says more
than "some queries are slow".

**The SQL is monoscope's own generated dashboard SQL**, lifted verbatim from
`shared/src/Pkg/Parser/Stats.hs` + `test/unit/Pkg/ParserSpec.hs` — not shapes
I invented. That turns out to be the single most important methodological
choice in this document: two of the three findings exist *only* in the exact
syntax monoscope emits and would have been invisible to hand-written SQL.

| shape | what the user sees | the generated SQL, in one line |
|---|---|---|
| `thrpt` | throughput chart | `count(*)`, `GROUP BY time_bucket(w, timestamp)` |
| `lat` | latency breakdown p50/p90/p95 | `percentile_agg(duration)` CTE, `GROUP BY extract(epoch from time_bucket(w,ts))` |
| `errs` | error-rate chart | `COUNT(*) FILTER (WHERE status_code='ERROR')` |
| `svc` | throughput by service | `GROUP BY time_bucket(...), COALESCE(resource___service___name,'null')` |
| `dcount` | unique users | `distinct_count(approx_count_distinct(attributes___user___id))` |
| `topn` | top endpoints | `GROUP BY name ORDER BY count DESC LIMIT 20` |
| `count` | total events | `count(1)` over a subquery (count-pushdown-proof form) |
| `list` | log explorer page | `ORDER BY timestamp DESC LIMIT 251` |
| `trace` | trace point lookup | `context___trace_id = '<fresh real hex>' LIMIT 100` |

Projects chosen by **measured** 1h volume, not by assumption:

| tag | project_id | rows/1h | note |
|---|---|---|---|
| TOP | `28f62f01-…` | 143,696 | highest-volume tenant today |
| W | `87576849-…` | 89,808 | the documented whale |
| P1 | `dcad860a-…` | 17,104 | P1 of the 08-20 / 08-22 matrices |
| P2 | `6297304f-…` | 8,045 | P2 of the 08-20 / 08-22 matrices |

## Results — warm rep, ms

### P1 (full grid)

| shape | 1h | 24h | 3d | 7d | 14d | rollup routing (3d/7d) |
|---|---|---|---|---|---|---|
| thrpt | 204 | 230 | 242 | 924 | 678 | **HIT (hybrid)** |
| list | 181 | 193 | 184 | 157 | 203 | n/a (TopK) |
| trace | 145 | 1,757 | 258 | 332 | 15,356 | n/a |
| count | 162 | 939 | 2,329 | 5,621 | TIMEOUT | no attempt |
| dcount | 208 | 1,054 | 2,865 | 6,700 | TIMEOUT | miss: missing_measure |
| topn | 161 | 1,174 | 3,099 | 7,314 | 20,090 | miss: unknown_group_by |
| errs | 173 | 2,031 | 4,484 | 7,912 | TIMEOUT | miss: missing_measure |
| lat | 168 | 2,826 | 13,150 | 14,679 | TIMEOUT | miss: unsupported_shape |
| svc | 180 | 1,715 | 8,980 | 8,835 | TIMEOUT | miss: unsupported_shape |

### TOP (highest-volume tenant; 7d/14d not completed — see Coverage)

| shape | 1h | 24h | 3d |
|---|---|---|---|
| thrpt | 635 | 1,112 | 505 |
| list | 358 | 579 | 268 |
| trace | 764 | 858 | 1,105 |
| errs | 392 | 2,021 | 5,540 |
| count | 401 | 1,696 | 4,651 |
| dcount | 521 | 2,939 | 5,555 |
| topn | 516 | 1,935 | 5,477 |
| lat | 488 | 2,669 | 7,161 |
| svc | 444 | 5,612 | 11,228 |

The shape of the table is the finding: **one chart is fast at every width
(`thrpt`, 204–924 ms) and every other chart degrades roughly linearly with
the window.** `thrpt` is the only one that routes to the rollup tier. The
spread is not about data volume — at 3d, P1's `thrpt` is 242 ms and P1's
`svc` is 8,980 ms over the same rows.

`list` (160–580 ms flat to 14 days) and `trace` (145–1,105 ms, apart from two
outliers) are the two genuinely healthy paths. `list` is flat because TopK
terminates early; `trace` is the payoff of the bloom sidecar shipped in
576e1c3 — see the historical comparison.

## Finding 1 — the latency and per-service charts are unroutable **for
syntactic reasons**, and fixing that is worth ~9–13x

Both charts decline at the same line: the group-expression matcher in
`src/rollup.rs` accepts a bare `Expr::Column` naming a declared dimension, or
a bare `time_bucket(w, timestamp)`, and everything else falls through to
`_ => return Err(MissReason::UnsupportedShape)`.

monoscope never emits either form:

- the percentile chart groups by `extract(epoch from time_bucket(w, ts))`
- every grouped chart groups by `COALESCE(<dimension>, 'null')`

A/B on P1, same method, `ab.jsonl`:

| arm | 3d | 7d | routing counter |
|---|---|---|---|
| `lat` as monoscope emits it | 3,525 | 7,926 | miss: unsupported_shape |
| `lat`, `extract(epoch …)` lifted above the GROUP BY | 3,420 | 8,119 | miss: **missing_measure** |
| `lat`, lifted **and** under the declared `server` filter | **278** | **1,035** | **HIT (hybrid)** |
| `svc` as monoscope emits it | 3,237 | 6,826 | miss: unsupported_shape |
| `svc`, bare dimension column | **276** | **754** | **HIT (hybrid)** |
| `thrpt` (control, already routes) | 206 | 749 | HIT (hybrid) |

Read that carefully, because it is two different defects:

- **`svc` is purely the COALESCE wrapper.** Unwrap it and the query routes:
  **3,237 → 276 ms at 3d (11.7x), 6,826 → 754 ms at 7d (9.1x)**.
- **`lat` needs two fixes.** Lifting `extract(epoch …)` moves the decline from
  `unsupported_shape` to `missing_measure` — proving the group-by wrapper is
  *a* blocker — but the query still scans raw, because the only `tdigest`
  measure declared in `schemas/otel_logs_and_spans.yaml` carries the
  `kind='server' OR name IN (…)` filter. There is no unfiltered
  `duration_digest`, even though `duration_sum/min/max/count` are all declared
  unfiltered. Supply both and it routes: **3,525 → 278 ms (12.7x), 7,926 →
  1,035 ms (7.7x)**.

So the work is:

1. **Unwrap `extract(epoch from time_bucket(…))` in the group-expr matcher.**
   Trivially sound — epoch-of-bucket is 1:1 with bucket, so the grouping is
   identical and the conversion lifts above the aggregate unchanged.
2. **Add an unfiltered `duration_digest` (`agg: tdigest, column: duration`) to
   both rollup grains.** One line each; it is the only unfiltered measure
   missing from an otherwise complete set.
3. **Unwrap `COALESCE(<dimension>, <literal>)` in the group-expr matcher — but
   not blindly.** COALESCE folds NULL and the literal `'null'` into one group.
   Grouping the rollup by the raw dimension and coalescing above therefore
   needs a re-aggregation step, or two groups leak out where the raw path
   produced one. The A/B arms bear this out: `svc_bare` returned 165 groups
   where `svc_asis` returned 219, and I did **not** establish that the
   difference is only the NULL-folding. **Do not ship the COALESCE unwrap on
   the strength of the latency number alone** — it needs a row-level
   equivalence test first.
4. Cheaper, lower-priority: `errs` declines as `missing_measure` because
   monoscope filters on `status_code='ERROR'` while the declared `error_count`
   measure is `status_code='ERROR' OR COALESCE(http_status,0) >= 500`. The two
   definitions of "error" simply disagree. This is a product decision (align
   monoscope to the rollup, or declare a second measure), not a matcher bug.
5. `dcount` declines as `missing_measure` — no `hll` measure on
   `attributes___user___id`. `topn` declines as `unknown_group_by` — `name` is
   not a declared dimension, and adding it multiplies rollup cardinality, so
   it deserves its own sizing rather than a reflex.

## Finding 2 (BLOCKING) — the rollup-routed throughput chart **silently
under-reports**, so Finding 1 must not ship first

While checking why routed `thrpt` returned 55 buckets at 3d where raw
returned 73, I compared the two bucket-by-bucket on P1:

```
routed  (GROUP BY time_bucket)                     55 buckets, 4,433,648 rows
raw     (GROUP BY extract(epoch from time_bucket)) 73 buckets, 7,289,079 rows
buckets present in raw and absent from routed:     18
buckets present in both with different values:     20
```

The routed chart shows **61% of the tenant's actual traffic**, with 18
one-hour holes.

Both standard explanations were tested and **both are refuted**. On one
dropped bucket (`1787266800`) and one mis-valued bucket (`1787256000`):

| bucket | `count(*)` | `count(1)` over subquery | `count DISTINCT id` | routed chart |
|---|---|---|---|---|
| dropped | 69,225 | 69,225 | 69,094 | **absent** |
| mis-valued | 266,170 | 266,170 | 265,988 | 83,151 |

`count(*)` equals the count-pushdown-proof form, so this is **not** the
count-pushdown over-report. Distinct-id differs from the raw count by 0.2%,
so it is **not** duplicate amplification (dedup is denied on 100% of eligible
scans — `dedup_denied_never_certified_pct = 100.0` — so the raw arm does carry
duplicates, but 0.2% of them, not 39%). The dropped bucket really holds
~69k rows and the chart really shows nothing there.

I have the symptom, not the mechanism. The shape of it — whole buckets
missing rather than uniformly scaled values — points at coverage bookkeeping
(a rollup built against a partition that later moved, where `StaleCoverage` /
`NotBuilt` should have forced a raw fringe and did not) rather than at the
aggregation itself. That is the next thing to read, in
`rollup_coverage`/hybrid-branch construction.

**This inverts the priority order.** Finding 1's whole content is "route more
chart shapes to the rollup tier". Doing that while the tier under-reports by
39% would take a correctness bug that currently affects one chart and spread
it across the dashboard. Fix Finding 2 first; Finding 1 is the payoff that
follows it, not a parallel track.

## Finding 3 — everything not routed is bounded by scan width, and the
existing caches are already doing their job

For the non-routed shapes the per-cell `foyer` diffs show the cost is scan
volume, not cache misses. P1 at 7d: `lat` 3,680 foyer hits against 144 misses
(96% hit rate) and still 14.7 s; `dcount` 2,659 hits and **zero** misses and
still 6.7 s. At 14d the misses grow (`lat` 475, `svc` 276) but never approach
the hit counts. `foyer` fleet-wide sits at 1.73 M hits / 43.5 K misses
(97.5%), l2 at 285 GB of a 600 GB budget, and `ttl_expirations = 0` — the
35-day warm depth shipped on 08-22 is holding.

The corollary matters for planning: **there is no cache win left on these
shapes.** They are slow because they decode every row in the window, and the
only two structural fixes are routing them to a pre-aggregate (Finding 1) or
reducing files per project-day. Tuning Foyer further will not move them.

Two anomalies worth naming rather than smoothing over:

- **`trace` at P1/24h is 1,757 ms but 258 ms at 3d and 332 ms at 7d** — a
  narrower window costing 5–7x more than wider ones. The 24h cell is the only
  `trace` cell with meaningful foyer misses (30). Consistent with today's
  partition being fragmented by live ingest while sealed days are compacted;
  the bloom sidecar prunes sealed files well and the hot tail not at all.
- **`trace` at P1/14d is 15,356 ms**, an order of magnitude off the 3d/7d
  cells, and `topn` at 14d is 20,090 ms. Both sit beyond where the sealed
  compaction backlog has reached.

## Finding 4 — `statement_timeout` is enforced, but a cancelled wide query can
wedge its connection for tens of minutes

The harness twice hung for 40+ minutes on a cell whose `statement_timeout` was
30 s. Follow-up isolated it:

- `statement_timeout` **is** honoured — the same P1/14d `svc` query cancelled
  at exactly 20.2 s and 20.0 s on two consecutive reps, and a `SELECT 1` and a
  `timefusion_stats` read on the *same connection* immediately afterwards both
  returned in 0.1 s. So cancellation is clean at 20 s.
- At 30 s on the wide cells it is not: the client blocked indefinitely, and a
  watchdog closing the connection from another thread did not unblock it.

Prod stayed healthy throughout (no restart, host at 60 GB of 188 GB), so this
is not an availability finding — but a pooled dashboard client that cancels a
wide query can lose the connection rather than reuse it, and it is the reason
the TOP/W/P2 grids below are incomplete. Worth a bounded repro before it is
theorised about further.

## Comparison with the historical numbers

Against the **08-20 baseline matrix** (deployed 493bb1b, same P1/P2, same
serial method). Shapes are not all identical — 08-20 measured log-explorer
shapes, this run measures dashboard chart shapes — so the honest comparison is
limited to the two that recur:

| cell | 08-20 | 08-22 (this run) |
|---|---|---|
| P1 trace lookup, 1h | 4,696 | **145** |
| P1 trace lookup, 24h | TIMEOUT (12 s) | **1,757** |
| P1 trace lookup, 7d | TIMEOUT (30 s) | **332** |
| P1 plain count, 1h | 1,248 | **162** |
| P1 plain count, 24h | 12,235 | **939** |
| P1 plain count, 7d | TIMEOUT (30 s) | **5,621** |

The trace-lookup row is the file-level bloom sidecar (576e1c3) verified on
real traffic and a real needle: **32x at 1h, and two former timeouts now at
1.8 s and 0.33 s**. The count row is 13x at 24h with the 7d timeout cleared.
Both hold up.

Against the **08-22 final-cycle scorecard** (525f6ec: 24h<1s in 13/25 cells,
30d<10s in 8/25): on this run's dashboard shapes, 24h<1s holds in 4 of 9 P1
cells (`thrpt`, `list`, `trace` misses at 1,757, `count` at 939) and 3 of 9
TOP cells. The scorecards are not directly comparable — different shape sets
— and I am not going to force them into one number. What is comparable is the
direction: the point-lookup and list paths are fixed, and the remaining misses
have moved from "IO and dedup amplification" to "this chart cannot use the
pre-aggregate", which is a different and more tractable problem.

Against `2026-08-21-post-hot-tier-speed.md`, which recorded a 30d rollup count
at **845 ms / 60 files** and concluded "the ~300ms-class dashboard path exists
via rollups now": confirmed, and now bounded. It exists for exactly one chart
shape. The 278 ms and 276 ms A/B arms show the same class is reachable for the
latency and per-service charts, and Finding 2 shows what has to be true first.

## Ranked next steps

1. **Diagnose the rollup under-count (Finding 2).** Blocking. Start at hybrid
   branch construction and the coverage predicates: 18 whole buckets absent is
   a coverage-bookkeeping signature, not an aggregation one. Nothing else in
   this document should ship before it.
2. **Unwrap `extract(epoch from time_bucket(…))` in the group-expr matcher**
   (`src/rollup.rs`) + **declare an unfiltered `duration_digest`** in both
   rollup grains. Measured 12.7x at 3d, 7.7x at 7d on the latency chart. Both
   changes are small and the soundness argument is clean.
3. **Unwrap `COALESCE(dimension, literal)`** — measured 11.7x / 9.1x on the
   per-service chart, but gated on a row-level equivalence test for the
   NULL-folding described above.
4. **Reconcile the two definitions of "error"** between monoscope's chart and
   the declared `error_count` measure. Product decision, then one line.
5. **Size `name` as a rollup dimension** for the top-endpoints chart, and an
   `hll` measure on `attributes___user___id` for unique-users. Both are
   `missing_measure`/`unknown_group_by` declines with real cardinality cost —
   measure before declaring.
6. **Bounded repro of the 30 s cancellation wedge** (Finding 4).

Explicitly **not** on this list: Foyer tuning. Finding 3 shows the cache is at
97.5% fleet-wide with zero TTL expirations and the slow shapes miss it barely
at all.

## Coverage — what this run did not measure

Stated plainly rather than left as a gap in the tables:

- **TOP was completed to 3d only; W and P2 were not run at all.** The harness
  stalled on TOP's 7d/14d cells (Finding 4) and I stopped it in favour of the
  A/B verification, which was worth more. P1 is the only full 1h→14d grid.
- **W has no `trace` cell** — the whale had no `context___trace_id` in the last
  90 minutes to use as a needle. It is a logs tenant, not a spans tenant
  (201 of 39,225 rows in 30 min carry a `duration`).
- **`EXPLAIN ANALYZE` per cell was lost.** The harness issued
  `EXPLAIN (ANALYZE) …`; TimeFusion's parser accepts only `EXPLAIN ANALYZE …`
  without parentheses, so every capture failed with a parse error. The
  operator-level attribution in this document therefore comes from
  `timefusion_stats` diffs and the rollup miss-reason counters, which turned
  out to be the more decisive instrument anyway — the miss reasons are what
  identified Findings 1 and 2. A re-run with the corrected syntax
  (`ea.py` in the scratchpad, already fixed) would add per-operator timings
  but is not needed to act on anything above.
- **`dcount` reads 0 for P1 and W** because those tenants do not populate
  `attributes___user___id`. The scan still happens, so the timing is valid;
  the returned value is not interesting for them.
