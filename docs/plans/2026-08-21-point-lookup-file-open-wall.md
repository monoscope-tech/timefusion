# The point-lookup file-open wall — why 30d is 60s, and what actually fixes it

2026-08-21 afternoon. Attribution for the reported-slow log_explorer trace
lookup. This SHARPENS `2026-08-21-post-hot-tier-speed.md` rather than
replacing it: item 3 (compaction) is confirmed necessary but **proven
insufficient for the 30-day goal**, and the user's "compact always-resident
index" intuition is confirmed as the missing mechanism.

All numbers measured against prod between image rolls (another session was
deploying; tags rolled 41ca4c6 → 5559ac8 → 7c70789 → 823746a during the
session — timestamps below are from the 823746a window unless noted).

## What the user actually runs

The complaint URL is a `log_explorer` link, not a `count(*)`. Monoscope
lowers it (`src/Pkg/Parser.hs:391-395`, `shared/src/Pkg/Parser/Expr.hs:846-853`)
to a **plain column equality** — no `text_match`, no variant accessor, no OR
fan-out:

```sql
SELECT jsonb_build_array(id, to_char(timestamp ...), context___trace_id, name,
       duration, resource___service___name, parent_id, ..., to_jsonb(summary),
       context___span_id, kind)
FROM otel_logs_and_spans
WHERE project_id='6297304f-…' AND timestamp BETWEEN '<iso>' AND '<iso>'
  AND (context___trace_id = '9b4b64dad7af955fbad42fa8d43c90da')
ORDER BY timestamp desc limit 501
```

Plus two client-side chart queries on the same predicate (`/chart_data`).
So the earlier `text_match`-poisoning P0 does **not** apply to this shape, and
branch `0a2842a` (the 08-20 fix set) **is already merged into master** — there
is no pending merge that fixes this.

## Measured latency curve (warm, real query shape)

| window | wall | rows | note |
|---|---|---|---|
| 24h | 1.4–1.6s | **0** | the trace is not in 24h — user waited, got nothing |
| 72h | 1.6–2.5s | 0 | |
| 7d | **8.9–11.5s** | 17 | run 2 ≈ run 1 ⇒ **not a cache miss; repeated work** |
| 30d | **60s (statement timeout)** | — | the stated goal, unmet |

A first-order UX note independent of TF: the user's default window was 24H and
the trace lives at ~96h, so the page was both slow *and* empty.

## Attribution — `EXPLAIN ANALYZE`, 7d leg

```
count_files_scanned=434
files_ranges_pruned_statistics = 434 total → 434 matched   ← ZERO files pruned
row_groups_pruned_statistics   = 1.67K   → 653 matched
row_groups_pruned_bloom_filter = 653     → 1 matched       ← blooms are excellent
time_elapsed_opening           = 4.39s                     ← THE COST
time_elapsed_scanning_total    = 26.40ms
metadata_load_time             = 58.11ms
statistics_eval_time           = 93.56ms
bloom_filter_eval_time         = 4.17ms
bytes_scanned                  = 23.59 MB   → output_rows = 17
```

A second delta leg opens a further 34 files for 0 rows (468 files total).

Read that top to bottom and the shape is unambiguous:

1. **File-level statistics prune literally nothing** (434 → 434). They cannot:
   `context___trace_id` is random hex, so every file's `[min,max]` spans
   essentially the entire hex space. Min/max is the wrong index for a
   high-cardinality random key. This is structural, not a tuning miss.
2. **Blooms are superb but arrive one step too late** (653 row groups → 1).
   The bloom lives *inside the file footer*, so to consult it you must already
   have opened the file. Pruning 99.8% of row groups saves decode — it does not
   save the open.
3. **So the query pays ~468 file opens to return 17 rows**, and
   `time_elapsed_opening` (4.39s) dwarfs actual scanning (26ms) by ~170x.
   Warm cost is ≈19ms/open (8.9s / 468); the earlier cold measurement in the
   companion doc (26s / ~200 opens ≈ 130ms) is the same curve, cold.

**Cost is a function of `files_opened`, essentially nothing else.** Bytes,
decode and row groups are already solved problems here.

## Why compaction alone cannot reach the 30-day goal

Current fragmentation is ~62 files/project-day (434 over 7d), consistent with
the companion doc's 197–217 files/project-day fleet-wide figure. Plan item 3
targets <20 files/project-day. Apply that to the goal:

| | files/day | 30d files | @19ms warm open |
|---|---|---|---|
| today | ~62 | ~1,860 | ~35s (measured: 60s timeout) |
| after compaction to 20 | 20 | 600 | **~11s** |
| after compaction to 10 | 10 | 300 | ~6s |

Compaction is a 3–6x win and is worth doing — but a linear win against a
linear cost cannot turn 1,860 opens into an interactive query. **At 30 days,
any design that opens one object per file-per-day is already lost.** The
window is the multiplier, and compaction does not change the exponent.

## The mechanism that does fix it — and it is the user's idea

To make a 30-day point lookup interactive, the pruning decision must happen
**before** any file is opened, from state already resident in the process.
That is precisely "compact in-memory indexes that are always resident."

Concretely: a per-file trace-key sketch, held in memory (spilling to the foyer
metadata tier), consulted at plan time to reduce the file list from ~1,860 to
the 1–2 files that can contain the key. The blooms we already write prove the
selectivity is there (653 → 1); the only thing missing is *where they live*.
This is exactly Tempo's design — per-block sharded blooms that gate **block
selection before planning** — and the companion doc already cites it under
"separate INDEX from DATA FILE and cache the index locally."

Sizing, order of magnitude: at ~9.6 bits/entry for 1% FPP, a file holding
~100k distinct trace ids costs ~120KB. 1,860 files ≈ 220MB for one project's
30 days — too large to pin for every project in RAM, but a natural fit for the
existing foyer metadata tier (today only **512MB memory / 5GB disk**, running
a 97% hit rate — it is small and effective, and is the right thing to grow).
Popular projects (the explicit goal) can be pinned resident. A local sketch
read is ~0.1ms against a ~19ms remote open: the ~100x that the file-count
math needs.

Cheaper variants worth costing before building the full thing: raise the FPP
(file-level pruning tolerates far more false positives than row-group
pruning, since a false positive costs one open we already pay today), or
index at day+shard granularity rather than per file.

## Revised ordering

1. **Foyer coverage** (companion item 1) — unchanged, still the cheapest win,
   and note l2 is 131GB of a 600GB budget with 1.1TB host free. Fixes *cold*
   (130ms/open → 19ms/open). Does **not** fix warm 30d.
2. **Compaction to <20 files/project-day** (companion item 3) — promoted to
   the top of the code-work list: file count multiplies planning, opens and
   decode alike, so it is the only lever that attacks all three. 3–6x. Still
   insufficient alone for 30d.
3. **Pre-open file pruning index (NEW, and the load-bearing one for 30d)** —
   resident per-file trace-key sketches, consulted **at plan time so the
   file-action list itself shrinks** (otherwise it removes the opens and
   leaves the ~3s planning). Without this, the 30-day goal is unreachable by
   tuning; with it, window width stops being a cost multiplier for point
   lookups.
4. Wide *aggregates* over 30d are already solved by rollups (845ms measured) —
   this document is about point lookups, which rollups cannot serve.

## Second cost centre: planning is ~3s before a single byte is read

Timing `EXPLAIN` (planning only, no execution) against `EXPLAIN ANALYZE` at 7d:

| statement | wall |
|---|---|
| `EXPLAIN` (plan only, 0 rows read) | **2.6–3.1s** |
| `EXPLAIN ANALYZE`, 2 projected columns | 3.2s |
| full query, 12 projected columns incl. `to_jsonb(summary)` | 8.9s |

So the 7d budget decomposes roughly as **~3s planning + ~0.2s to reach the
17 rows + ~5.7s decoding the wide projection**. Planning is a third of the
cost and produces no data at all: it is delta snapshot handling and pruning
across the file-action list, and it scales with the same file count as
everything else. Note also that widening the projection from 2 to 12 columns
costs ~5.7s for the *same 17 rows* — worth its own attribution pass, since
`bytes_scanned` was only 23.6MB.

This **raises the value of compaction** relative to the companion doc's
ordering: file count is the multiplier on planning *and* opens *and* decode,
so it is the only lever that attacks all three. It also means a pre-open
pruning index must be consulted early enough to shorten the planning file
list, not just the execution one — otherwise it removes the opens and leaves
the 3s.

## Not in scope, deliberately

Prod OOM-killed 3x this morning at ~125GB anon (09:45, 09:52, 10:03) and has
been stable since; the restarts after that are deploys, not kills. That is the
separate standing OOM item, and none of the queries here reproduced it.
