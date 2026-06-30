# TimeFusion query speed vs TimescaleDB — benchmark + plan

Date: 2026-06-30. Goal: make TF queries consistently competitive with (ideally
faster than) the TimescaleDB path monoscope dual-reads.

## Method

- Same prod data on both stores (monoscope dual-writes). Busy project
  `87576849-…a1a8`, ~117k rows in a 1-hour window (`05:58–06:58Z 2026-06-30`).
- Query shapes lifted from `monoscope/src/Models/Telemetry/Telemetry.hs`
  (`selectOtelSpans`, trace/point lookups, count, `Log.hs` page + chart widgets).
- Read-only. Tightly time-bounded to protect the memory-tight TF instance.
- Server-side times = single psql session, `\timing`, min of repeats (excludes
  reconnect). Wall times include ~150–320ms connect/RTT.

## Results (server-side ms, 1h window, ~117k rows)

| query | TS | TF | TF/TS | why TF is slower |
|---|---|---|---|---|
| count_window | 113–320 | ~520 | ~2.5× | scans `timestamp` (now also `id`, see §dedup-tax) over the whole day-file; no metadata count for a partial window |
| recent_page (ORDER BY ts DESC LIMIT 100) | 320 | 550 | 1.7× | timestamp sort-order helps, but wide projection still decoded |
| filter_error (`status_code='ERROR'`, 7k/117k) | 330 | 580–5700 | 1.7–17× (high variance) | no pruning on `status_code`; decodes the wide projection for all 117k then filters |
| timeseries (`time_bucket('5 min')` count) | 320 | 650 | 2× | full-window scan + hash agg |
| topN_name (GROUP BY name) | 360 | 1140 | 3.1× | decodes `name` for all rows |
| **trace_lookup (`context___trace_id=…`)** | **930** | **5560–6700** | **~6×** | **no index → full-window decode of 6 cols, filter to a handful** |
| point_lookup (`timestamp=… AND id=…`) | 150 | 125–450 | ~1× server-side | partition + timestamp prune well; TF is fine here |

## Root cause (confirmed from EXPLAIN)

TF has exactly **two** data-skipping mechanisms:
1. Hive partition pruning on `(project_id, date)` — works.
2. Parquet row-group min/max stats — effective only for the **timestamp**
   sort-leading column.

Every selective predicate on a high-cardinality, non-sorted column
(`context___trace_id`, `context___span_id`, `id` alone, `name`,
`resource___service___name`, `attributes___session___id`, `…user___id`,
`http.method`) defeats min/max pruning (random/low-cardinality min–max spans the
whole value range), so TF **decodes the projected columns for the entire window
and filters in memory**.

TimescaleDB serves all of these from **11 btree indexes**, notably
`(project_id, left(context___trace_id,800))`, `(project_id, name)`,
`(project_id, context___span_id)`, `(project_id, resource___service___name)`,
plus partial indexes on session/user/http_method. That index set *is* the gap.

EXPLAIN evidence (trace_lookup): one ~40MB parquet file split into 48 scan
ranges; `pruning_predicate` includes `context___trace_id_min/max` but cannot
prune (random hex); filter predicate is **emitted twice** (full_filters +
partial_filters both evaluated); `CAST(context___trace_id AS Utf8View)` per row.

## Plan — ranked by impact / effort

### P0 — Term-index prefilter via the existing tantivy sidecar (biggest win)
TF *already* maintains a tantivy index (segment merges visible in prod logs;
prior "tantivy id-prefilter" work). Wire equality / `IN` / `= ANY` predicates on
high-cardinality indexed columns (`context___trace_id`, `context___span_id`,
`id`, `parent_id`, `session_id`, `user_id`) through tantivy to produce a
matching row-id / file-and-row-group set, and use it to **skip parquet row
groups / restrict the row selection** before decode. Directly kills the 6×
trace_lookup and the span/id lookups — the worst and most common trace-view
paths. (Reuse/extend the existing prefilter; mind the past OR-prefilter bug —
only route exact `=`/`IN`, skip OR subtrees.)

### P1 — Parquet late materialization (filter-column-first) for un-indexable predicates
For `status_code`, `name`, free-text body: ensure the scan decodes only the
filter column(s) first, builds a selection mask, then decodes the wide
projection (`body`, `attributes`, `resource`) **only for surviving rows**.
Verify DataFusion's parquet `row_filter` pushdown + page-index + `reorder_filters`
are enabled on the Delta scan. Attacks filter_error (17× tail) and shrinks the
wide-projection cost on every page query.

### P1 — count(*) / aggregate pushdown from stats
Answer `count(*)` (and min/max/sum where stats suffice) from Delta `num_records`
+ row-group stats instead of scanning. For a partial timestamp window: count
row groups fully *inside* the window from stats, scan only the boundary groups.
Attacks count_window (2.5×) and the monoscope ingest-counters path.

### P1 — Undo the read-dedup tax I just shipped (commit 1d773bf)
The new `DedupExec` (a) augments **every** projection on `otel_logs_and_spans`
with the dedup keys (`id`) — visible: `count(*)` now scans `[timestamp, id]` —
and (b) forces `SinglePartition` (a `CoalescePartitionsExec`), serializing
downstream parallelism and holding an unbounded un-pooled `seen` HashSet. Gate
DedupExec to activate only when the routed MemBuffer∪Delta union can actually
contain cross-flush dupes for the query (e.g. window overlaps the un-swept
region), and avoid forcing single-partition for pure aggregates. Removes a tax
on the hot path. (See the code-review findings on 1d773bf.)

### P2 — Parquet bloom filters on equality columns
Write bloom filters for `trace_id`, `span_id`, `id`, `parent_id` so the reader
skips row groups that definitely lack the value — complements tantivy and helps
even when the tantivy path isn't taken. Write-path + reader-config change.

### P2 — Cold-start / tail-latency ("consistently")
Boot-warm after a deploy is a ~70s read blackout, and the cold Foyer cache makes
the first filtered scan spike (status_code 1.8s→5.7s observed). Pre-warm Foyer
for hot `(project_id, recent-date)` partitions on boot and after flush. This is
the lever for the *consistency* half of the goal — tail, not median. NB: a
`git push` to master auto-deploys via CapRover, so each push pays this blackout.

### P3 — Cleanup
Eliminate the duplicated scan filter predicate (full_filters + partial_filters
both evaluated → predicate twice); drop the per-row `CAST(trace_id AS Utf8View)`
by comparing in the stored type.

## Expected effect
P0 alone should bring trace/span/id lookups from ~6× slower to at/below TS
(index-vs-index). P1 (late-materialization + count pushdown + dedup-tax removal)
should pull count/filter/page queries to ~1–1.5×. P2/P3 close the tail and the
"consistent" requirement. Aggregations over a full window (timeseries, topN)
remain scan-bound and roughly at parity — acceptable, since that's where a
columnar engine is supposed to be competitive without indexes.

## Implementation status (2026-06-30)

Exploration found much of the plan **already shipped**; the table records what
was actually changed vs. pre-existing.

| Item | Status | Notes |
|---|---|---|
| **P0 tantivy `=` routing** | **DONE this session** | Schema: `context___trace_id`/`span_id`/`id`/`parent_id` now `tantivy: {indexed, tokenizer: raw}` (kept their blooms). Rewriter routes exact `col='lit'` on raw cols → `text_match` (gated by `tantivy.route_equality`, default on, struct-field not global). Correctness-safe under OR (collector already skips OR subtrees; original `=` preserved). `!=`/`IN` deliberately NOT routed (no term form / intersect-only prefilter can't OR). Tests: 5 unit (`tantivy_rewriter`) + e2e `flushed_eq_prefilter_matches_baseline_and_or_is_safe` (on==off + OR returns correct union). |
| P1 late-materialization | **pre-existing** | `pushdown_filters`/`reorder_filters`/`enable_page_index`/`bloom_filter_on_read` all already `true` in `create_session_context`. No change needed. |
| P1 count(*)/agg pushdown from stats | **deferred — has a correctness landmine** | Real gap, but: with read-side `DedupExec` active, `COUNT(*)` returns the *distinct* count, while Delta `num_records` stats give the *physical* row count (incl. un-swept dupes) → a naive stats-pushdown would over-count exactly when dupes exist. Safe only when the scanned set is provably dupe-free (e.g. fully swept partitions with no MemBuffer overlap). Needs its own focused PR + tests; do NOT bolt on quickly. |
| P1 "undo DedupExec tax" | **reclassified — mostly intrinsic** | The id-augmentation + SinglePartition are *required* for the `COUNT==DISTINCT` correctness guarantee DedupExec exists to provide; removing them would reintroduce wrong counts. The real way to drop the tax is to make writes dupe-free so DedupExec can be removed (the broader parity effort), not to gate it per-query. |
| **P2 blooms on eq columns** | **pre-existing** | `bloom_filter: true` already on id/parent_id/trace_id/span_id/session_id/user_id/name (NDV 1M, fpp 0.01); `bloom_filter_on_read=true`. P0's tantivy index complements these (blooms prune row groups; tantivy prunes to exact ids). |
| P2 Foyer boot pre-warm | **pre-existing** | `preload_tables()` → `warm_cache_for_uris` (footer-only, newest-first) already runs on boot. Tuning (full-file warm for hottest partitions) is a config/perf follow-up, not missing code. |
| P3 doubled filter predicate / per-row `CAST` | **fork-side, deferred** | Both originate in the `delta-rs-timefusion` fork's scan construction (full_filters + partial_filters; Utf8→Utf8View coercion), not this repo. Needs a fork change; low value vs P0. |

**Operational caveat for P0:** the new tantivy fields only index data flushed
*after* this deploys; existing Delta files index trace_id/span_id once
re-indexed (compaction/GC rebuild). Until then those rows fall back to scan
(correct, just not accelerated) — so the speedup ramps in, it isn't instant.
Also watch `timefusion_tantivy_max_index_size_mb` (64): indexing 4 more
high-card raw columns grows the index; if it exceeds the cap the prefilter
silently no-ops (still correct). Tune if prod index size warrants.

## Validation harness
`/tmp/tfbench/run.sh` (this session) — parameterized query set, min-of-3 on both
DURLs. Re-run after each lever; assert TF/TS ratio drops for the targeted row.
```
