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

### W (whale) and P2 — 1h/24h only

| shape | W 1h | W 24h | P2 1h | P2 24h |
|---|---|---|---|---|
| thrpt | 259 | 3,624 | 153 | 1,288 |
| lat | 241 | 7,106 | 159 | 1,340 |
| errs | 251 | 1,487 | 433 | 1,626 |
| svc | 287 | TIMEOUT | 147 | 1,325 |
| dcount | 235 | 6,689 | 255 | 1,162 |
| topn | 232 | 1,476 | 366 | 1,110 |
| count | 242 | 6,678 | 129 | 1,182 |
| list | 255 | 385 | 242 | 207 |
| trace | n/a | n/a | 135 | 380 |

W is the one tenant where **`thrpt` is also slow** (3,624 ms at 24h against
P1's 230 ms) — its 24h `svc` is the only 12 s timeout in the reduced grid.
W has no `trace` cell: it had no `context___trace_id` in the last 90 minutes
to use as a needle. It is a logs tenant, not a spans tenant (201 of 39,225
rows in 30 minutes carry a `duration`).

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

> **SUPERSEDED 2026-08-22 (same day).** The mechanism paragraph below is
> WRONG — it is not the interior/fringe split, which is correct and
> property-tested. The under-count comes from STALE SLICE CONTENT: all live
> coverage is `rollup_slice_coverage`, whose plan-time loop has no staleness
> guard at all, while the per-date map that does have one is dead code. The
> measurements in this finding stand; only the attribution changes. See
> `2026-08-22-rollup-correctness-and-routing.md` §1.

**The mechanism is now identified.** Query either bucket *on its own*, as a
one-hour window, and the correct value comes back:

| probe | result | routing counter |
|---|---|---|
| dropped bucket alone (`2026-08-20 23:00 UTC`) | **69,225** — correct | miss: **`not_built`** → raw |
| mis-valued bucket alone (`2026-08-20 20:00 UTC`) | **266,170** — correct | miss: **`not_built`** → raw |

So no rollup has been built for P1 on 2026-08-20. When that date is the
*whole* window, `NotBuilt` correctly declines and the query falls back to raw.
When the same date sits *inside* a 3-day window, the query takes the hybrid
path (`rollup_hits_hybrid_total`) and the not-built dates are **neither
covered by the rollup nor added to the raw fringe** — their rows simply
disappear from the result.

That is a precise, testable defect: **the hybrid split's interior must exclude
dates with no rollup and hand them to the raw leg.** It is not an aggregation
bug and not a staleness bug; it is the interior/fringe partition treating
"no rollup for this date" as "nothing to add".

**This inverts the priority order.** Finding 1's whole content is "route more
chart shapes to the rollup tier". Doing that while the tier under-reports by
39% would take a correctness bug that currently affects one chart and spread
it across the dashboard. Fix Finding 2 first; Finding 1 is the payoff that
follows it, not a parallel track.

## Finding 3 — for everything not routed, **half the compute is the dedup
stack**, not the scan — and it is dedup that is denied on every scan

68 `EXPLAIN ANALYZE` plans (`ea.jsonl`; the first pass emitted
`EXPLAIN (ANALYZE)`, which TimeFusion's parser rejects — re-run with the
correct syntax). Summing `elapsed_compute` per operator, P1:

| win | shape | DedupExec | SortPreservingMergeExec | DeltaScanExec / Aggregate | wall |
|---|---|---|---|---|---|
| 24h | lat | 355 ms | 202 ms | Aggregate 321 ms | 2,826 |
| 3d | lat | 1,030 ms | 666 ms | Aggregate 953 ms | 13,150 |
| 3d | count | 917 ms | 531 ms | Scan 253 ms | 2,329 |
| 7d | errs | 2,130 ms | 2,020 ms | Scan 615 ms | 7,912 |
| 7d | topn | 2,370 ms | 2,120 ms | Scan 640 ms | 7,314 |
| 7d | dcount | 1,970 ms | 1,950 ms | Scan 595 ms | 6,700 |
| 14d | thrpt | 3,630 ms | 3,120 ms | Scan 1,070 ms | 678¹ |

¹ the 14d `thrpt` wall is the routed hybrid query; the EA above is the plan's
own compute, which includes the raw fringe.

**`DedupExec` is the single largest operator in every non-routed cell, and
`SortPreservingMergeExec` — the sort that feeds it — is the second.** At 7d
they are ~4 s of a ~7–8 s query: roughly half the compute, before any
aggregation. `DeltaScanExec` is consistently a third of DedupExec.

This corrects the intuition that these shapes are scan-bound. They are
*dedup-bound*, and the dedup is doing nothing: `dedup_denied_uncertified`
equals `dedup_eligible` (16,935 of 16,935) and
`dedup_denied_never_certified_pct = 100.0`, so **every one of these scans pays
for the dedup machinery and none of them is allowed to skip it.** The
per-date dedup skip is aimed at exactly this cost; **Finding 5 shows it is
already on by default, already working, and denied 100% of the time for want
of certification coverage on recent dates.**

The two shapes that escape are the two fast ones: `list` and `trace` show
`DedupExec` at 1–2 ms, because TopK and a point lookup never materialise the
window.

The caches, by contrast, are healthy and have nothing left to give. P1 at 7d:
`lat` 3,680 foyer hits against 144 misses; `dcount` 2,659 hits and **zero**
misses, and still 6.7 s. Fleet-wide foyer is 1.73 M hits / 43.5 K misses
(97.5%), l2 at 285 GB of a 600 GB budget, `ttl_expirations = 0` — the 35-day
warm depth shipped on 08-22 is holding. **Tuning Foyer further will not move
any of these shapes.**

Two anomalies the plans explain:

- **`trace` at P1/24h costs 1,757 ms against 258 ms at 3d and 332 ms at 7d.**
  The plans show why: at 24h the trace lookup prunes to 9 files and 101 row
  groups, at 3d to 11 files / 113 row groups, at 7d to 56 / 135 — the pruning
  is *better* at 3d than at 24h relative to what remains, and the 24h cell is
  the only `trace` cell with meaningful foyer misses (30). Consistent with
  today's partition being fragmented by live ingest while sealed days are
  compacted: the bloom sidecar prunes sealed files well and the hot tail
  hardly at all.
- **`trace` at P1/14d is 15,356 ms** and `topn` at 14d is 20,090 ms — both
  beyond where the sealed compaction backlog has reached.

## Finding 4 — `statement_timeout` is enforced, but a cancelled wide query can
wedge its connection for tens of minutes

The harness twice hung for 40+ minutes on a cell whose `statement_timeout` was
30 s. Follow-up isolated it:

- `statement_timeout` **is** honoured — the same P1/14d `svc` query cancelled
  at exactly 20.2 s and 20.0 s on two consecutive reps, and a `SELECT 1` and a
  `timefusion_stats` read on the *same connection* immediately afterwards both
  returned in 0.1 s. So cancellation is clean at 20 s.
- **Both hangs occurred on cells configured at 30 s, and neither was
  reproduced under control.** A watchdog closing the connection from another
  thread did not unblock the client. That 30 s is the trigger is an inference
  from two harness incidents, not a tested claim — the only controlled
  cancellation test I ran was the 20 s one above, and it passed.

Prod stayed healthy throughout (no restart, host at 60 GB of 188 GB), so this
is not an availability finding — but a pooled dashboard client that cancels a
wide query can lose the connection rather than reuse it, and it is the reason
the TOP/W/P2 grids below are incomplete. Worth a bounded repro before it is
theorised about further.

## Finding 5 — what to do about the dedup and the sort

Follow-up investigation, after the profile above. Prod had by then moved to
`c4843b5` (deployed 12 min before these reads), so the counters are from a
fresh process.

**The sort is not a second problem.** `DedupExec::required_input_distribution`
is `SinglePartition` and `required_input_ordering` is the dedup key ordering
(`src/read/mod.rs:481-486`). The `SortPreservingMergeExec` that costs as much
as the dedup itself exists *only* to satisfy those two requirements. Remove
the dedup for a partition and the merge goes with it, and the scan stays
multi-partition. That is why the pair is ~4 s of a ~7.9 s 7-day query rather
than ~2 s, and why these shapes do not scale with cores: every file group is
funnelled through one thread before aggregation.

**The skip is already on, and it works.** Both
`timefusion_read_dedup_skip_swept` and `timefusion_read_dedup_skip_per_date`
default to `true` (`src/config.rs:2008,2033`) — my earlier note that the
per-date skip shipped default-off was wrong. Measured on P1, a full-day
count per date, warm rep:

| date | certified? | wall | rows | dedup counters |
|---|---|---|---|---|
| 2026-08-12 | yes | **189 ms** | 2,431,232 | `dedup_skipped=2` |
| 2026-08-13 | no | 789 ms | 2,273,057 | denied, never_certified |
| 2026-08-14 | no | 1,603 ms | 2,580,617 | denied, never_certified |
| 2026-08-08 | partly (2 of 9) | 2,364 ms | 2,174,059 | `dedup_skipped=2`, 7 denied |

**4.2x and 8.5x on comparable row counts.** File counts per date are not
controlled, so treat the multiple as indicative rather than exact — but the
mechanism demonstrably fires and demonstrably pays.

**The blocker is certification coverage, and it is concentrated in the wrong
place.** The durable store
(`/home/ubuntu/timefusion-data/.timefusion_meta/dedup_certifications.json`,
last written 00:54, ~9 h before these reads) holds **97 certifications across
13 projects** — and P1's are exactly **two dates: 2026-08-08 and 2026-08-12**.
The busiest tenant has 9, spanning 08-04..08-21. Dashboards query the last
1–14 days. So:

- a 1h / 24h / 3d / 7d window contains **zero** certified dates → 100% denial,
  which is exactly what `dedup_denied_never_certified_pct = 100.0` reports;
- even the 14d window catches only 08-08 and 08-12 out of 15 dates, so the
  skip fires on two old, cold days and the other 13 pay in full.

Certification is not broken — it is being spent on dates nobody queries.

**Why recent dates never certify.** A certification is keyed on the
partition's whole file-set fingerprint, and any new file voids it
(`maintain.rs`, the `entry.fp != fp` reset). Recent partitions are rewritten
continuously by ingest, hot-tail compaction and the sealed/repair backlog, so
they churn faster than sweeps can certify them. Today's partition legitimately
can never certify; the last 7 days are the contested band.

### Ranked, and the structural one first

1. **Make certification additive over a FILE SET rather than exact over a
   partition fingerprint.** Certify the set of files a sweep proved clean; a
   file added afterwards is simply not in that set, and dedup runs over the
   uncertified remainder while the certified files union in above it. This is
   the same decomposition the per-date skip already performs one level up, and
   it is the only option that survives ingest churn instead of racing it.
   Soundness needs the same argument the per-date skip needed — that no dedup
   key spans the split — which is *not* free here, because merge-on-read
   versions of one row can land in different files. **That is the thing to
   establish before building it**, and it is why this is a design task, not a
   patch.
2. **Bias sweep ordering to the newest ~7 days.** Cheap scheduling change,
   immediate partial payoff, and it aligns the spend with what dashboards
   query. But it fights churn rather than escaping it, and it competes with
   the sealed backlog for the same workers — so treat it as relief, not a fix.
3. **Accept that today's partition keeps full dedup.** With the per-date skip
   already shipped, a window's today-slice paying while its older dates skip
   is the correct end state, not a gap.

Not recommended: turning the skip off, widening the fingerprint check, or
touching `timefusion_read_dedup_bounded` — the mechanism is sound and the
measurements above show it paying whenever it is allowed to.

One thread left hanging: `src/rollup.rs`'s module doc states "a rollup is
built only after its source partition is duplicate-free", while
`maintenance_coordinator.rs:161` states "BaseRollup depend on NOTHING". If the
first is really enforced somewhere, then certification throughput gates
Finding 2's `not_built` dates as well and the two findings share one root
cause. I did not establish that either way — worth ten minutes before planning
Finding 2's fix.

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

1. **Fix the hybrid interior to hand `not_built` dates to the raw leg
   (Finding 2).** Blocking, and now a specific change rather than an
   investigation: the same date returns correct rows when it is the whole
   window (declines as `not_built` → raw) and vanishes when it is inside a
   hybrid split. Add a regression test that asserts a routed window
   containing a not-built date sums to the raw total. Nothing else in this
   document should ship before it.
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
6. **Raise certification coverage on RECENT dates — see Finding 5.** This, not
   a flag flip, is what unlocks the dedup/sort cost for every shape that will
   still not route after steps 2–5.
7. **Bounded repro of the 30 s cancellation wedge** (Finding 4).

Explicitly **not** on this list: Foyer tuning. Finding 3 shows the cache is at
97.5% fleet-wide with zero TTL expirations and the slow shapes miss it barely
at all.

## Coverage — what this run did not measure

Stated plainly rather than left as a gap in the tables:

- **TOP was completed to 3d; W and P2 to 24h.** P1 is the only full 1h→14d
  grid. The harness stalled on TOP's 7d/14d cells (Finding 4), so those and
  W/P2's 3d+ cells were dropped in favour of the A/B verification and the
  EXPLAIN ANALYZE pass, which were worth more.
- **W has no `trace` cell** — no `context___trace_id` in the last 90 minutes
  to use as a needle.
- **`dcount` reads 0 for P1 and W** because those tenants do not populate
  `attributes___user___id`. The scan still happens, so the timing is valid;
  the returned value is not.
- **The 08-20 and this run's `trace` needles differ** (fresh real trace ids
  were fetched per project per run, as the method requires); P1's grid also
  spans two needles, because the run was resumed after the first stall. Both
  were real ≤100-row traces from the last 90 minutes.
- **`errs`/`svc`/`dcount`/`topn` were not A/B'd** — only `lat` and `svc` were.
  Their miss reasons come from the counters, and the remedies in steps 4–5 are
  therefore reasoned from the schema, not measured. Size them before building.
