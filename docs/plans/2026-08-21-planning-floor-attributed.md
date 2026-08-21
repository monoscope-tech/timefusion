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

---

## Review addendum (parallel session, later the same evening)

Re-measured on the same prod image `823746a`, **6 interleaved reps per cell**
at 30d rather than 3, minimums reported, sorted ascending:

| cell | reps (ms, sorted) | min |
|---|---|---|
| whale `=` (routed) | 2656 2781 2937 2997 3276 5302 | **2656** |
| whale range (not routed) | 569 572 637 648 813 813 | **569** |
| ghost `=` (routed) | 700 795 797 799 1564 2766 | **700** |
| ghost range (not routed) | 616 663 704 724 759 783 | **616** |

**§1 is confirmed and strengthened.** The routing tax on the whale at 30d is
2656 − 569 ≈ **2.1s**, matching §1's independent estimate by a different
route. The range-form proxy is the right instrument; the companion doc's
`||''` arm was inflated, and its "~0.9–1.5s flat residual" should be read as
**~570–620ms** — the cleaner number here.

**§2 is half right, and the half that fails is the load-bearing half.** The
*residual* is indeed window-driven and not file-driven: ghost range (616ms)
≈ whale range (569ms), despite one having no data at all. That kills the
"mirror the Delta log" idea exactly as §2 argues, and it is a good control.

But §2 generalises it to the whole cost — "planning tracks the time range
asked for, not the files found". The dominant term does **not** behave that
way: ghost `=` pays only ~85ms of routing tax (700 vs 616), while whale `=`
pays ~2.1s (2656 vs 569), a **~25x difference driven purely by how much
indexed data sits in the window**. §2's single ghost figure (2153ms) does not
replicate at n=6, where the minimum is 700ms; the routed form is heavy-tailed
(outliers at 2766ms and 5302ms here), so n=3 minimums are under-powered
precisely on the arm that varies most.

The corrected mechanism is the companion doc's original one, which §2 believed
it had ruled out: **the prefilter's cost scales with the number of sidecar
indexes actually present in the window.** Ghost has none, so it pays nothing.

This *strengthens* P0 rather than weakening it — the tax is largest exactly
where the goal lives, on popular projects with the most indexed data in the
widest windows.

**§3, §4, §5 stand.** The heavy tail here independently reproduces "the routed
form never warms". §5's conclusion that P0 does not rescue 30d also survives
this data: 30d planning is ~2.7s routed against a 35–60s full query, so at 30d
planning is a minor term and execution dominates — the reverse of 7d.

**One citation to retire.** §5 leans on `time_elapsed_opening=4.39s` versus
26ms of scanning to argue a file-open wall. That figure is **summed across the
24 partition groups, not wall clock** (the companion doc retracts a
"19ms/open" number derived the same way). The compaction and resident-index
conclusions may well hold, but this number cannot carry them — the file-open
wall needs a wall-clock measurement it does not yet have.

### Addendum note on the paired foyer change (c8e03ac)

`ttl 7d→35d` and `cache_recent_days 8→35` are live in prod now
(`timefusion_stats` reports `ttl_seconds=3024000`, `cache_recent_days=35`),
but **`disk_gb` is still 600** — and it is env-pinned on CapRover, so the code
default is not the lever.

Sizing concern: l2 sat at ~131GB steady-state under an 8-day admission window
and is at ~136GB and climbing. Scaling the admission window 8d→35d scales the
resident set roughly 4.4x, i.e. toward **~575GB against a 600GB cap** — ~95%
full, where foyer is evicting oldest-first continuously and the 30-day tail
(the exact band this change exists to warm) is the first thing evicted.

The companion doc's item 1 called for `disk_gb` 600→~900 of the 1.1TB free,
and this change shipped the horizon half without the capacity half. They are
one change: without the disk raise, the expansion partly self-defeats. Watch
`foyer.evictions` (9,812 at the time of writing) and `l2_used_bytes` over the
next day — a rising eviction rate with l2 pinned near 600GB is the signature.
