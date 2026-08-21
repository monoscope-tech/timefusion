# Independent controls for the P0 route_equality finding

2026-08-21 evening. A parallel session reached the same conclusion in
`2026-08-21-point-lookup-file-open-wall.md` (P0: flip
`TIMEFUSION_TANTIVY_ROUTE_EQUALITY=false`). **This doc does not restate it.**
It adds four controls that doc lacks, corrects one claim in it, and tightens
the expected size of the win. Measured against prod `823746a`, read-only, one
psql session with `\timing`, interleaved reps, `SELECT 1` control in every
round, minimums reported (contention only ever adds).

## 1. A better proxy for the flag flip — and it is an upper bound

The companion doc's A/B used `col||'' = '…'` to defeat the rewrite, and
correctly caveats that this also defeats predicate pushdown, so its "B" arm is
inflated (~888ms at 7d).

`col >= 'lit' AND col <= 'lit'` is a cleaner proxy. Verified on the emitted
plans — `text_match` occurrences at 7d:

| form | `text_match` in plan |
|---|---|
| `trace_id = 'hex'` | **5** |
| `trace_id >= 'hex' AND <= 'hex'` | **0** |
| `substr(trace_id,1,32) = 'hex'` | **0** |

and unlike `||''`, the range form **keeps pushdown** — both the scan predicate
and a stats pruning predicate reach the parquet layer:

```
predicate=… AND CAST(context___trace_id@2 AS Utf8View) >= 9b4b… AND … <= 9b4b…
predicate=… AND context___trace_id_max@4 >= 9b4b… AND context___trace_id_min@6 <= 9b4b…
```

So the range arm differs from a real `route_equality=false` in exactly one
way: a range predicate cannot use a **bloom** (blooms serve equality only).
A real flip keeps `=`, so it keeps the bloom. **Every number below is
therefore an upper bound on what the flip delivers, not an optimistic one.**

Planning, minimums, `SELECT 1` control at 78 ms:

| shape | 7d | 30d |
|---|---|---|
| `=` (routed) | 2245 ms | 2552 ms |
| range (not routed) | **166 ms** | **312 ms** |
| `substr()` (not routed) | — | 313 ms |
| no trace predicate at all | — | 292 ms |

The tax is ~2.1–2.2s and everything else in planning totals ~300ms at 30 days.

## 2. Correction — planning scales with WINDOW WIDTH, not file count

The companion doc states there is no fixed floor and that "planning scales
with window width, **i.e. with the number of files in range**." The second
half is wrong, and the distinction matters for what to build next.

Control: the same 30-day query against a **project id that does not exist** —
zero files, nothing to prune.

| shape | plan (min) |
|---|---|
| `SELECT 1` | 63 ms |
| `EXPLAIN SELECT 1` | 84 ms |
| whale, 10 min | 448 ms |
| whale, 1 day | 1281 ms |
| whale, 7 days | 1845 ms |
| whale, 30 days | 2043 ms |
| **nonexistent project, 30 days** | **2153 ms** |

A project with **no data at all** plans as expensively as the whale's 30 days.
The cost tracks the *time range asked for*, not the files found — consistent
with the companion doc's own mechanism (wider window → more sidecar indexes to
fetch and search), but it rules out the file-list reading of the same
sentence.

This also independently kills the "mirror the Delta log into a second
database so planning is fast" idea: delta-rs already keeps an `EagerSnapshot`
resident, incrementally advanced (`src/database/mod.rs:481`) and persisted
across restarts (`src/storage.rs:3256+`). The log is already parsed and
cached, and the ghost-project control shows the cost is not there anyway.

## 3. The routed form never warms up

Full execution at 7d, 4 interleaved rounds, **both forms return the same 17
rows every time**:

| run | `=` (routed) | range (not routed) |
|---|---|---|
| 1 (cold) | 2918 ms | 2577 ms |
| 2 | 2534 ms | **673 ms** |
| 3 | 3169 ms | **288 ms** |
| 4 | 3880 ms | **300 ms** |

The unrouted form warms by ~9x; the routed form never does. That is the
signature of per-query recomputation, and it explains the companion doc's
"run 2 ≈ run 1, therefore not a cache miss" observation mechanically.

End-to-end this is a **~5–10x win at 7d**, with identical results.

## 4. Blast radius, measured rather than reasoned

The companion doc argues from code (§3b) that the flip's low-cardinality
victims — `kind`, `status_code`, `level` — lose nothing, because neither a
bloom nor a term index prunes them. Measured at 7d, that holds, and they carry
the same tax:

| column | `=` (routed) | range (not routed) |
|---|---|---|
| `kind = 'span'` | 2548 ms | **203 ms** |
| `level = 'ERROR'` | 2290 ms | **160 ms** |

So on low-cardinality columns the route costs ~2.3s and buys pruning that
cannot exist. Combined with the high-cardinality columns falling back to
blooms, **no affected column regresses.** The global flip is clean.

## 5. What P0 does NOT fix — the 30-day wall stands

Full execution, same 17 rows where they completed:

| window | `=` (routed) | range (not routed) |
|---|---|---|
| 7d | 2.5–3.9 s | **0.29–0.67 s** |
| 30d | 35–60 s (1 timeout) | 38–60 s (2 timeouts) |

At 30 days, removing the route changes nothing measurable — both forms land at
35–60s and both intermittently hit the statement timeout.

**Caveat, stated because it cuts against the claim:** the range arm forgoes
blooms, so it is not a clean stand-in for the flip at 30d specifically. A real
flip keeps bloom pruning. But blooms save *decode*, not *opens*, and the
companion doc's `EXPLAIN ANALYZE` puts `time_elapsed_opening` at 4.39s against
26ms of scanning — so the file-open wall is not something blooms can move.
The honest statement is: **P0 is not shown to rescue 30d, and the file-open
argument for compaction + a pre-open resident index survives P0 intact.**

That ordering is the actionable part. P0 is a config flip worth ~5–10x at ≤7d.
The resident pre-open index is still the only mechanism that reaches 30d — but
had it been built first, it would have sat behind a 2.2s planning tax that no
index can remove.
