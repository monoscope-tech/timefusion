# Slow point lookups: full attribution and plan

2026-08-21. Attribution for the reported-slow `log_explorer` trace lookup,
and the plan that follows from it. Supersedes the earlier drafts of this file
(retractions listed at the end — several early numbers were wrong).

Companion: `2026-08-21-post-hot-tier-speed.md`. This doc keeps that doc's
foyer/compaction/rollup verdicts and adds the planning-side attribution it
was missing.

## Measurement methodology (read first — I got this wrong twice)

- **One `psql` session, `\timing on`, server-side timings.** A fresh `psql`
  connection costs **620–655ms** on this host. Every per-invocation number in
  the first drafts of this doc silently included it.
- **Interleaved reps, report the spread**, not a single run. Prod runs at
  load average ~44 and another session was deploying throughout (image tags
  rolled 41ca4c6 → 5559ac8 → 7c70789 → 823746a); single runs vary 2–4x.
- **Never attribute by differencing two separate runs.** That error produced
  a retracted "5.7s wide-projection decode" claim.
- Counter diffs from `timefusion_stats` are **contaminated on a live box** —
  a 20-query burst showed 110 new scans, i.e. mostly prod's own traffic.

## What the user actually runs

Monoscope lowers `trace_id == "…"` to a **plain column equality** — no
`text_match` in the SQL, no variant accessor (`shared/src/Pkg/Parser/Expr.hs:846-853`,
`src/Pkg/Parser.hs:391-395`): 12-column projection, `ORDER BY timestamp desc
limit 501`, literal ISO timestamp bounds. So the 08-20 `text_match` P0 does
not apply to this shape, and `0a2842a` is already merged into master.

Measured (warm, single session): 24h **1.4–2.5s** (0 rows), 7d **4.9–8.9s**
(17 rows), 30d **60s statement timeout**. The reported trace was at ~96h
while the UI defaults to 24H, so that page was slow *and* empty.

## Cost decomposition

`EXPLAIN` (planning only, zero rows read), best of 3 interleaved reps:

| window | planning |
|---|---|
| 1 min | **147ms** |
| 1 h | 305ms |
| 6 h | 1,181ms |
| 24 h | 1,133ms |
| 7 d | **4,789ms** |

**Planning is the whole cost.** At 7d it is 4.8s first-touch (2.2–2.9s warm,
repeated) against a full-query time of 4.9–8.9s. Execution is nearly free:
on the real 12-column shape, `bytes_scanned=24.67MB`,
`time_elapsed_scanning_total=196ms`, `time_elapsed_processing=644ms`, for 460
files and 17 output rows.

There is **no fixed planning floor** — 147ms at a 1-minute window. Planning
scales with window width, i.e. with the number of files in range.

## Attribution inside planning

### 1. The plan-time tantivy prefilter — the single biggest item

`TIMEFUSION_TANTIVY_ROUTE_EQUALITY` **defaults to true**
(`src/config.rs:703`), so `TantivyPredicateRewriter` (`src/read/optimizers.rs:1018-1043`)
rewrites a plain `context___trace_id = '…'` into `text_match(…)`. `scan()`
then runs a real tantivy search **at plan time** (`src/database/mod.rs:8598`
→ `src/tantivy/search.rs:122-215`): an S3 manifest GET behind a **5-second
TTL**, plus per-index blob download/untar and a synchronous search.

A/B at 7d, 4 interleaved reps, verified by counting `text_match` occurrences
in each emitted plan:

| variant | plan contains `text_match` | planning |
|---|---|---|
| A: `context___trace_id = '…'` | yes (5x, every rep) | 3305 / 6207 / 8091 / 2714 ms — median **~4.7s** |
| B: `context___trace_id\|\|'' = '…'` (defeats the rewrite) | **no (0, every rep)** | 2884 / 1745 / 1816 / 1079 ms — median **~1.8s** |

**~2.5–3s of 7d planning is the plan-time tantivy search.** Caveat: B's
`||''` also defeats predicate pushdown, so B is a *proxy* for
`route_equality=false`, not identical to it — a real flag flip should be at
least this good (it keeps pushdown and blooms). Treat ~2.5–3s as the
measured magnitude, not a promised delta.

### 2. That index cannot even answer the question

`maintenance.tantivy_uncovered_files = 5497` and diverging (~85/hr; the only
drain is a 03:30 cron that has not been firing — see
`tf_tantivy_reconcile_never_drains_2026-08-21`). So we pay seconds of
plan-time S3 IO to consult an index that does not cover 5,497 files, and the
scan must fall back to those files regardless. **Worst of both.**

### 3. Parquet blooms already do this job, better and later

From `EXPLAIN ANALYZE`:

```
files_ranges_pruned_statistics = 434 → 434   ← file-level stats prune NOTHING
row_groups_pruned_statistics   = 1.67K → 653
row_groups_pruned_bloom_filter = 653 → 1     ← blooms are excellent
bloom_filter_eval_time         = 4.17ms      ← for ~4ms, not ~3s
```

**Cite leg 2, not leg 1, for "blooms suffice on their own."** Leg 1's
653→1 is *confounded*: that plan carried
`required_guarantees=[id in (…17 UUIDs…)]`, and those ids came **from** the
plan-time tantivy search we propose to remove — so leg 1 cannot prove blooms
stand alone. Leg 2 can: it had `required_guarantees=[]` (no id-set) and still
bloom-pruned **102 row groups → 0 on the `context___trace_id` bloom alone**.
That is the un-confounded evidence, and it is what P0 rests on.

File-level min/max prunes nothing and *structurally cannot*: `trace_id` is
random hex, so every file's `[min,max]` spans the hex space. Min/max is the
wrong index for a high-cardinality random key. Blooms fix that — but they
live **inside the footer**, so consulting one requires already having opened
the file. They save decode; they can never save the open.

### 3b. Exactly what P0 changes — and what it does not

Equality routing is **gated on the RAW tokenizer**
(`src/read/optimizers.rs:1126-1145`: `if indexed_columns.get(&c.name)?.0 !=
RAW_TOKENIZER { return None }`). Cross-referencing the 12 tantivy-indexed
columns against the 7 bloomed ones in `schemas/otel_logs_and_spans.yaml`:

| raw-tokenized (so affected by the flip) | bloom? | consequence of flipping |
|---|---|---|
| `id`, `parent_id`, `context___trace_id`, `context___span_id` | **yes** | falls back to blooms — the measured 102→0 path |
| `kind`, `status_code`, `level` | no | **no loss**: low-cardinality (`log`/`span`, `ERROR`/`OK`, `error`/`info`), so neither a bloom nor a term index prunes — essentially every file holds every value |

`name`, `status_message`, `body`, `attributes`, `summary` are ngram3, so
equality never routed to them in the first place. **`attributes___user___email`
is not tantivy-indexed at all**, so the flip does not touch it — the
companion doc's "add the email bloom" item stands on its own merit,
independent of P0.

Critically, **LIKE / regex / `contains` routing is unaffected** — those match
in the `Expr::Like` and regex arms, which are not gated by `allow_eq`. The
DSL's substring search keeps its tantivy acceleration. P0 removes the
plan-time index lookup *only* for exact `=`, which is precisely the case
blooms already serve.

### 4. Healthy — ruled out, do not chase

`fast_resolve_hit_pct=100.0` (so no slow `resolve_table`, no `update_state`
herd), `provider_cache_hit_pct=95.4`, `provider_build_us_avg=138µs`,
`mem_plan_us_avg=3.0ms`. Fleet-wide `provider_scan_us_avg=144.6ms` — the
delta-rs O(files) replay is real but second-order next to item 1.

Also tested and **not** supported: monoscope's literal (non-parameterized)
timestamps defeating the plan cache. Identical vs varying literals time the
same. (Consistent with the code: the shape cache lifts literals to `$N`, and
a hit skips only parse/analyze/optimize — never physical planning, which is
where the cost is.)

### 4b. The residual planning cost does NOT scale with window

Running the same ladder on variant B (rewrite defeated, so no tantivy),
3 interleaved reps, min per window:

| window | A (tantivy-routed) | B (no tantivy) |
|---|---|---|
| 1 min | 147ms | ~905ms |
| 1 h | 305ms | ~1,352ms |
| 24 h | 1,133ms | ~900ms |
| 7 d | **4,789ms** | **~888ms** |

**B is flat across windows; A scales.** So the window-scaling of planning is
the tantivy prefilter (wider window → more sidecar indexes to fetch and
search), *not* the delta-rs `provider.scan()` replay or `dedup_window_clean`.
The non-tantivy residual is a roughly constant ~0.9–1.5s.

Two honesty caveats: this ladder ran while prod was heavily loaded (`SELECT 1`
in that same session cost **3,376ms**, versus 84ms earlier), so B's absolute
floor is inflated and one rep hit a 28s outlier — the *flatness* is the robust
claim, not the exact floor. And B's `||''` defeats pushdown, so B selects more
files than a real `route_equality=false` would, which makes the flatness
finding conservative rather than optimistic.

This **re-sizes P3/P5**: after P0, planning stops being window-proportional,
so compaction's value shifts away from planning and toward the cold-path
opens it always addressed. It also means the ~0.9–1.5s residual — not file
count — becomes the next ceiling for point lookups, and it is unattributed.

### 5. Dedup is never skipped

`cert_granted_total=0`, `dedup_denied_never_certified_pct=100.0`,
`dedup_eligible=10019`, `dedup_skipped_pct=0.0`. Every eligible scan still
carries `DedupExec`. Slice-coverage certification is deployed and grants
nothing — unchanged since the companion doc flagged it.

## Plan, ordered by measured value per unit of risk

**P0 — flip `TIMEFUSION_TANTIVY_ROUTE_EQUALITY=false`. Config only, zero
code, documented in-code as the instant-rollback lever.** Removes ~2.5–3s of
plan-time S3 IO from every string point lookup; parquet blooms already
deliver the pruning (653→1) at 4ms. This is the largest measured win
available and it is a one-line env change. Validate on staging first, then
one deploy carrying only this, and compare 7d planning before/after.

**P1 — decide tantivy's future.** It is currently a net cost: 5,497 uncovered
files and diverging, consulted on the plan path at seconds per query. Either
fix the reconcile drain so coverage converges, or stop routing to it. Do not
leave it half-alive. (P0 makes this non-urgent, which is why it is second.)

**P2 — certification → dedup skip.** 10,019 eligible scans, 0 grants, 100%
`never_certified`. Diagnose why `record_clean_slice` never grants; removes
`DedupExec` from every eligible scan.

**P2b — attribute the ~0.9–1.5s non-tantivy planning residual.** After P0 this
*is* the point-lookup cost, and it does not scale with window (§4b), so
neither compaction nor a pruning index will move it. Nothing below should be
sized until this is understood. Prime suspects from the code map:
`provider.scan()`'s O(files) replay with plan-time deletion-vector loads
(`scan/replay.rs:150-163`), and `dedup_window_clean`'s two full
`get_file_uris()` string scans per query (`src/database/maintain.rs:2655`),
which are O(*total* files, ~26k) rather than O(window). Measure on a quiet
box — the counters are contaminated under prod load.

**P3 — compaction to <20 files/project-day** (from ~62 measured here, ~200
fleet-wide). **Re-scoped by §4b**: since planning is flat once tantivy is out
of the path, compaction's value is the cold-path opens and decode, not
planning. Sized against measured anchors rather than a synthesized per-file
cost:

| | files/day | 30d files | equivalent to |
|---|---|---|---|
| today | ~62 | ~1,860 | 30d **times out at 60s** |
| compacted to 20 | 20 | ~600 | today's 7d file count → 4.9–8.9s |
| compacted to 10 | 10 | ~300 | today's ~3–4d file count → ~1.6–2.5s |

Necessary, and **not sufficient**: at 20 files/day, 30d only reaches what 7d
costs today.

**P4 — foyer coverage** (companion item 1: `disk_gb` 600→~900 of 1.1TB free,
`ttl_seconds` past 7d, `cache_recent_days` 8→~35). Fixes the *cold* curve.
Orthogonal to everything above, still cheap, still worth doing.

**P5 — resident plan-time pruning index. The 30-day endgame.** The user's
"compact always-resident index" intuition is right, and the measurements say
exactly what it must be: consulted **during planning so the file-action list
shrinks** (otherwise it removes opens and leaves the planning cost, which is
the cost), and **resident, not fetched** (the tantivy prefilter is the
cautionary tale — a plan-time index that does S3 IO is worse than no index).
Per-file key sketches in the foyer metadata tier (today only 512MB memory /
5GB disk at a 97% hit rate — obvious room), pinned for popular projects.
This is Tempo's block-level bloom design. Rollups already serve 30d
*aggregates* at 845ms; this is specifically for point lookups, which rollups
cannot serve.

## Retractions from earlier drafts of this file

1. **"~19ms per warm file open."** Wrong: `time_elapsed_opening` is summed
   across the 24 partition groups, not wall clock. Do not divide wall by opens.
2. **"~5.7s decoding the wide 12-column projection."** Wrong: derived by
   differencing two separate runs. Measuring the real 12-column shape gives
   4.9s total with <1s of decode. Projection width is **not** a cost centre;
   `to_jsonb(summary)` is fine.
3. **"~2.25s fixed planning floor, present even at 1 day."** Wrong twice
   over: it included ~620ms of per-connection setup, and it was noise. Clean
   single-session timing gives 147ms at a 1-minute window. **There is no
   floor; planning scales with window.**
4. **"Cost is a function of `files_opened`, essentially nothing else."**
   Too strong. Cost is dominated by *planning*, which scales with files in
   window; opens matter mainly on the cold path.

## Not in scope

Prod OOM-killed 3x this morning at ~125GB anon (09:45, 09:52, 10:03), stable
since; later restarts were deploys. Separate standing item — none of the
queries here reproduced it.
