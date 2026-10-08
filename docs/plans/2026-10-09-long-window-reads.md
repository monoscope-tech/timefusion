# Long-window reads (shipbubble first)

Started 2026-10-08 22:00 UTC. Owner goal: fast queries in monoscope at 14d, 30d and other long windows. This covers the log explorer, endpoint analytics and ad-hoc filters, starting with shipbubble (`28f62f01`).
This file is the working plan. Update it in place, with evidence (commit, numbers) on every item.

## 1. How progress is judged

All measurements are taken on prod, on a process at least 10 minutes old, one query at a time, with 3 or more runs and the arms alternated.
`bench/latest_bench.py --windows 7d,14d,30d --projects shipbubble` covers the overview widgets and the log explorer list, status chart and percentiles.
`filt.py` (scratchpad, to be folded into the bench) covers filtered list and chart queries.

| Shape | Target p50 | Target max |
| --- | --- | --- |
| Overview widgets, 7d to 30d | < 2 s | < 5 s |
| Log explorer list, unfiltered, any window | < 1 s | < 3 s |
| Log explorer list with a filter of 0.5% or more of rows, 7d to 30d | < 3 s | < 8 s |
| Log explorer chart with a filter, 7d to 30d | < 3 s | < 10 s |
| Needle search (few or no matches), 30d | completes (< 90 s) | — |

## 2. Baseline (2026-10-08, image `75455c49`, process 36 h old)

Overview widgets, shipbubble, 1 round:

- `error_rate` and `http_by_status` hit the 90 s timeout at 7d, 14d and 30d.
- At 30d: `top_resources` 67 s, `var_service` 12 s, `le_percentiles` 23 s.
- The rest ran in 1.5–6.5 s.

Log explorer, unfiltered:

- List: 0.5–5.8 s. The status chart and percentiles took 2.4–5.4 s.
- One real user page load at 21:03 UTC took 29 s for the status chart and 65 s for the percentiles. I could not reproduce this.

Filtered queries, 7d, on a mature process (about 50 min old):

| Filter | List | Chart |
| --- | --- | --- |
| `status_code = 'ERROR'` | 52 s | 5.7 s |
| `http.status = 500` | 28 s | 13 s |
| route equality | 17 s | 7.6 s |
| `name ilike` | 20 s | 1.1 s |

At 30d, most filtered queries time out at 90 s.

## 3. Findings

- **The overview widgets missed rollups for matcher reasons, not because data was missing.** Fixed in `1c548150` (PR #330, see Done).
- **A filtered list query reads the whole window.** EXPLAIN ANALYZE of the 7d errors list shows 24 file groups, 41 files, 24.7M rows decoded and 1.62 GB scanned to return 501 rows.
  - A plain run serves 1.7–2.1 GB from the foyer cache. When about 400–500 MB of that misses the cache, the query takes 9–15 s. When nearly all of it is cached, it takes 1.3 s.
  - Cause: the LIMIT sits above `DedupExec` (keep-greatest), so it cannot reach the scan.
  - `SortPreservingMergeExec` needs a head batch from every group. With a sparse filter, filling a head batch decodes most of the group's head file, so every head file is read: about 24 files for 500 rows.
- **Cold single-day sealed scans are cheap:** 1–5 s for about 4–30 GETs. The 39–68 s numbers were taken on a young process. Measure only after 10 minutes of uptime.
- `EXPLAIN ANALYZE` of the 24h list took 22 s against 1.3 s plain, and ANALYZE skips routing. Use counter diffs (`foyer.*`, `rollup_*`) around plain runs.

## 4. Workstreams

### Done

- [x] **WAL ingest wedge** (not a read issue, found during this work): `4fee4fd7`, PR #329.
  - Every INSERT for project `2a39bd83` failed with ENOENT from 2026-10-08 00:25 until the deploy at 22:13, about 86k statements.
  - Cause: the age-based WAL GC deleted a segment that held an idle writer's open block.
  - Verified: 0 errors after the deploy, and the project is ingesting again.
- [x] **Overview widgets route** (`1c548150`, PR #330, deployed about 23:00 08-10):
  - Error rate: the Int32 versus Int64 typing of the bound literal made the filter strings differ, so the measure never matched.
  - Requests by status: a `text_match` hint nested inside the HTTP-scope OR blocked dimension matching.
  - `endpoints_1m` and `hashes_30m` now backfill 31 days instead of 14, without changing their generation.
  - The pgwire e2e guard goes red with either fix reverted.

### R1 — Latest-N with a filter: stop reading every group's head file

Prior art (research notes in section 5): ClickHouse read-in-order with a limit, the InfluxDB IOx `ProgressiveEvalExec`, and DataFusion's TopK dynamic filters and statistics-based file ordering.

Design: a stats-aware merge in place of `SortPreservingMergeExec` above the scan union.

- Each input partition has an upper bound: the maximum `timestamp` from the statistics of its files. The bound is `None` for the in-memory legs, which are always active.
- A partition is opened only when the merge frontier (the head value about to be emitted) is at or below its upper bound.
- A row is emitted only when it is strictly greater than every inactive bound, so ties stay together for `DedupExec`.
- When every open partition is empty or pending, more partitions are activated, doubling each time. A needle query therefore regains full parallelism quickly, while a dense query never opens the old groups.
- Correctness: the output order on the leading key is the same as `SortPreservingMergeExec`, and no row can be skipped, because an inactive partition holds nothing above its bound.
- Guards: an e2e test asserting cost, namely that a dense filtered latest-501 over many files opens fewer than K files (bytes served), plus correctness on the same answer as before.
- Metric: `foyer.bytes_served` per list query. Target is under 100 MB for the 7d errors list.

### R2 — Filtered charts and needles over 7–30 days

- Filter on dimension columns (status, service, kind, level): route through the tiers. Verify that each one routes.
- Filter on non-dimension columns (route, `http.status`, text): this is a raw scan.
  - Measure cold and warm cost per day, and whether the cost is I/O misses or decode.
  - Candidates: the tantivy prefilter for equality on indexed columns, bloom sidecars, and wider cache retention for the active tenants' last 30 days.

### R3 — Unfiltered log explorer at 30d

- Percentiles at 30d were 23 s in one sample and 3.7 s in another. Classify each run as cold or warm and routed or raw, then fix what is named.

### R4 — Monitoring (continuous)

- TimeFusion dashboard: monoscope project `87576849`, dashboard `8e2deff2`.
- Per hour: `WAL append_batch failed`, OOM or restart history (`docker service ps`), `pgwire.slow_statement` for shipbubble, and `base_generation_unverified` after the routing deploy.

## 5. Prior art notes

All of these systems read sources in time order, newest first, keep a few reads in flight, and stop once nothing left can beat the current Nth row.

- **ClickHouse.** `optimize_read_in_order` reads parts in key order and stops at the LIMIT. The docs warn that it is slow for sparse WHERE clauses with a large LIMIT, because it gives up parallelism.
  - `read_in_order_use_virtual_row` (default on since 26.8; DESC support in #99198) makes each part announce its next key from the sparse index. The merge then reads only the parts that can contribute, plus a read-ahead window of up to `max_threads` parts.
  - The per-block variant once serialized reads and was up to 16x slower (#62125, #117330), so keep reads in flight.
  - Virtual rows combined with FINAL (dedup) is still a draft (#124468).
- **InfluxDB IOx `ProgressiveEvalExec`.** When time ranges do not overlap, it concatenates inputs newest-first instead of merging them, prefetches the next one or two, and stops at the fetch. Only overlapping file sets go through merge plus dedup.
- **Upstream DataFusion.** ProgressiveEval is not upstream (#10316 is open). Sort pushdown reorders files by statistics and can stop early. TopK dynamic filters need a TopK `SortExec`, which our plan does not have, and they had a duplicate-rows bug with `pushdown_filters` (#24352).
- **Log engines.** Loki splits newest-first and cancels the remaining splits once it reaches the limit. Quickwit and Elasticsearch skip splits or shards whose max timestamp cannot beat the current Kth hit.
  - Quickwit #6865 is a tie bug: ranges rounded to seconds skipped equal-timestamp splits and returned wrong top-K results. **A bound must be compared strictly, at full precision.**

**Chosen: virtual-row bounds in our own merge (R1).** It keeps today's file groups, dedup placement and filter placement. Only the merge decides when to open a group, from file statistics:
- an open group's rows must beat every closed group's bound;
- a read-ahead window keeps a few groups in flight;
- when the open groups run dry with no rows, the window doubles.

Preconditions, checked:
- The dedup key `[timestamp, service, id]` contains the sort prefix, so all versions of a key share a timestamp, and the ties rule keeps them adjacent.
- A file with missing timestamp stats, or with nulls, is unbounded and always open.
