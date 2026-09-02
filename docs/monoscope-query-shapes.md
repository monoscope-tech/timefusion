# What monoscope actually asks TimeFusion

> **This document is now executable: `tests/slt/monoscope_query_shapes.slt`.**
> Prose drifts — two claims below were stale for months (the `Utf8View` OR
> workaround, and `EXPLAIN (ANALYZE)`), so monoscope kept paying for defects
> that no longer existed. Add a shape to the slt file when you add it here, and
> CI will tell you when a claim stops being true.

2026-08-22. Sourced from the monoscope tree, not from guesswork: the KQL→SQL
lowering in `shared/src/Pkg/Parser/Stats.hs`, the query builder in
`src/Pkg/Parser.hs`, the shipped dashboard definitions in
`static/public/dashboards/*.yaml`, and the hand-written SQL in
`src/Models/Telemetry/{Telemetry,ServiceGraph}.hs` and
`src/Web/FacetsFallback.hs`.

Why it matters: two of the three findings in the 2026-08-22 dashboard profile
existed **only** in the exact syntax monoscope emits and were invisible to
hand-written equivalents. Anything benchmarked against invented SQL measures a
system nobody runs.

## 0. The lowering rules — how a KQL widget becomes SQL

Every dashboard widget is either `query:` (KQL) or `sql:` (raw). The KQL side
lowers through these, and the spellings are load-bearing:

| KQL | SQL emitted | source |
|---|---|---|
| `count()` / `count(*)` | `count(*)::float` | `Stats.hs` |
| `countif(cond)` | `COUNT(*) FILTER (WHERE cond)::float` | `Stats.hs` |
| `dcount(x)` | `distinct_count(approx_count_distinct(x))::float` | `dcountSQL` |
| `p50/p75/p90/p95/p99(x)` | `COALESCE(approx_percentile(0.95, percentile_agg(CAST(x AS DOUBLE PRECISION))), 0)::float` | `percentileSQL` |
| `percentiles(x, 50, 75, 90)` | same, one call per quantile, CROSS JOIN'd | `Stats.hs` |
| `avg/sum/min/max/median/stdev` | the plain aggregate, `::float` | `simpleAggSQL` |
| `bin_auto(timestamp)` | `time_bucket('5 minutes', timestamp)` (`defaultBinSize`) | `Stats.hs:82` |
| `bin(timestamp, 60)` | `time_bucket('60 seconds', timestamp)` | `Stats.hs` |
| `coalesce(a, "x")` | `COALESCE(a, 'x')` | `Stats.hs` |
| `case(...)` / `iff(...)` | `CASE WHEN … THEN … ELSE … END` | `Stats.hs` |

Three structural facts about every generated chart query:

1. **The bucket is projected as an epoch integer**:
   `extract(epoch from time_bucket(w, timestamp))::integer`.
2. **It is grouped by the bare bucket**: `GROUP BY time_bucket(w, timestamp)` —
   so the SELECT and the GROUP BY spell the same bucket two different ways.
3. **Grouped dimensions are wrapped**: `COALESCE(<dim>, 'null')`, and DataFusion
   then rewrites that to `CASE WHEN dim IS NOT NULL THEN dim ELSE 'null' END`
   before any optimizer sees it.
4. Default `LIMIT 500` on non-summarize queries; summarize queries get **no
   limit** unless the KQL says `| limit N` (dashboards commonly say 2,000 or
   10,000).

## 1. Throughput — the most common shape by far

```sql
SELECT extract(epoch from time_bucket('5 minutes', timestamp))::integer, 'value',
       count(*)::float AS count_
FROM otel_logs_and_spans
WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3
GROUP BY time_bucket('5 minutes', timestamp)
ORDER BY time_bucket('5 minutes', timestamp) DESC
```

Widgets: *Requests*, *Total Spans*, *Total Logs*, *Traffic*, *Total Queries*.
KQL: `summarize count() by bin_auto(timestamp)`.

## 2. Throughput split by a dimension — the grouped chart

```sql
SELECT extract(epoch from time_bucket('5 minutes', timestamp))::integer,
       COALESCE(resource___service___name, 'null'),
       count(*)::float
FROM otel_logs_and_spans
WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3
GROUP BY time_bucket('5 minutes', timestamp), COALESCE(resource___service___name, 'null')
ORDER BY time_bucket('5 minutes', timestamp) DESC
LIMIT 10000
```

Dimensions actually used across the shipped dashboards:
`resource___service___name`, `status_code`, `kind`, `level`,
`attributes___http___request___method`,
`coalesce(tostring(attributes.http.response.status_code), "unknown")`,
`attributes___db___system___name`, `name` (endpoint/operation), and
`method, url_path` together (two dimensions plus the bucket).

Widgets: *All requests*, *Events by Service*, *Spans by Service*,
*HTTP Requests by Status Code*, *Log Volume by Level*, *Span Kind Distribution*,
*Requests by Endpoint*.

## 3. Latency percentiles — two distinct spellings

**3a. One percentile per bucket** (`summarize p95(duration) / 1000000 by bin_auto(timestamp)`):

```sql
SELECT extract(epoch from time_bucket('5 minutes', timestamp))::integer, 'value',
       (COALESCE(approx_percentile(0.95, percentile_agg(CAST(duration AS DOUBLE PRECISION))), 0)::float / 1000000)
FROM otel_logs_and_spans
WHERE project_id = $1 AND duration IS NOT NULL AND timestamp BETWEEN $2 AND $3
GROUP BY time_bucket('5 minutes', timestamp)
```

**3b. Multi-quantile, digest built once and read N times** — the
`percentiles(duration, 50, 75, 90, 95, 99)` form, which is a CTE plus a
CROSS JOIN over a VALUES list:

```sql
WITH bucket_digests AS (
  SELECT extract(epoch from time_bucket('1 hours', timestamp))::integer AS timeB,
         percentile_agg(CAST(duration AS DOUBLE PRECISION)) AS digest
  FROM otel_logs_and_spans
  WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3
  GROUP BY timeB HAVING COUNT(*) > 0)
SELECT b.timeB, q.quantile, COALESCE(approx_percentile(q.percentile, b.digest), 0)::float AS value
FROM bucket_digests b
CROSS JOIN (VALUES (0.5,'p50'),(0.9,'p90')) AS q(percentile, quantile)
ORDER BY b.timeB DESC, q.quantile
```

A third variant appears in `http-stats.yaml` as raw SQL, building an `ARRAY[...]`
of four `approx_percentile` calls and `LATERAL unnest`-ing it.

Widgets: *Latency percentiles (ms)*, *Latency Percentiles*, *P50/P75/P95/P99
Latency*, *Request Latency Percentiles*, *Query Latency Percentiles*.

## 4. Error rate — `countif` over a ratio

```sql
SELECT extract(epoch from time_bucket('5 minutes', timestamp))::integer, 'value',
       ROUND((COUNT(*) FILTER (WHERE status_code = 'ERROR'
              OR COALESCE(attributes___http___response___status_code, 0) >= 500)::float
              * 100.0 / count(*)::float)::numeric, 2)::float
FROM otel_logs_and_spans
WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3
GROUP BY time_bucket('5 minutes', timestamp)
```

Note the two definitions of "error" in use: `status_code = 'ERROR'` alone, and
`status_code = 'ERROR' OR http.response.status_code >= 500`. They disagree, and
the rollup tier declares only the second.

## 5. Distinct counts — HLL sketches

```sql
SELECT distinct_count(approx_count_distinct(resource___service___name))::float
FROM otel_logs_and_spans WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3
```

Over `resource___service___name` (*Active Services*, *Log Sources*),
`context___trace_id` (*Traces*), `name` (*Unique Endpoints*),
`attributes___db___system___name` (*DB Systems*), and
`attributes___user___id`. Both bucketed and as a single scalar.

## 6. Top-K tables — `ORDER BY COUNT(*) DESC LIMIT N`

The tables under most dashboards. Representative (*Services Health*, `_overview.yaml`):

```sql
SELECT resource___service___name AS service_name,
       ROUND(SUM(CASE WHEN duration <= 500000000 THEN 1.0
                      WHEN duration <= 2000000000 THEN 0.5 ELSE 0 END)
             / GREATEST(1, COUNT(*))::numeric, 2)::text AS apdex,
       ROUND(COUNT(*)::numeric / GREATEST(1, EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60), 2)::text AS throughput,
       ROUND((COUNT(*) FILTER (WHERE status_code = 'ERROR'
              OR COALESCE(attributes___http___response___status_code, 0) >= 500) * 100.0
              / GREATEST(1, COUNT(*))::numeric), 2)::text AS error_rate,
       ROUND(approx_percentile(0.95, percentile_agg(duration))::numeric / 1000000, 2)::text AS p95_latency
FROM otel_logs_and_spans
WHERE project_id = $1 AND resource___service___name IS NOT NULL
  AND kind = 'server' AND duration IS NOT NULL AND timestamp BETWEEN $2 AND $3
GROUP BY resource___service___name
ORDER BY COUNT(*)::numeric / GREATEST(1, EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))) / 60) DESC
LIMIT 50
```

Same shape keyed on `name` (operations), the `method || ' ' || url_path`
concatenation (endpoints), `attributes___db___system___name`, and a
`LEFT(COALESCE(db.query.summary, db.query.text, …), 2000)` query-pattern key.

**Two-stage CTE variant** — top-N first, then aggregate, explicitly to bound
GROUP BY cardinality:

```sql
WITH top_endpoints AS (
  SELECT attributes___http___request___method || ' ' || attributes___url___path AS endpoint,
         COUNT(*) AS total_count
  FROM otel_logs_and_spans WHERE … GROUP BY 1 ORDER BY 2 DESC LIMIT 10)
SELECT … FROM otel_logs_and_spans JOIN top_endpoints USING (endpoint) …
```

## 7. Facets — top-50 values per field, one query per field

`src/Web/FacetsFallback.hs`, run for the filter sidebar:

```sql
SELECT ("resource___service___name")::text, COUNT(*)::bigint
FROM public.otel_logs_and_spans
WHERE project_id = $1::uuid AND timestamp >= $2::timestamptz AND timestamp < $3::timestamptz
  AND ("resource___service___name") IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 50
```

## 8. Log-explorer list page — TopK, no aggregation

```sql
SELECT <~88 columns> FROM otel_logs_and_spans
WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3
ORDER BY timestamp DESC LIMIT 251
```

Cursor pagination rewrites the window to `timestamp <= <cursor>` (older) or
`timestamp >= <cursor>` (newer) — see `buildDateRange`.

## 9. Point lookups — trace and span detail

```sql
SELECT <cols> FROM otel_logs_and_spans
WHERE timestamp = $1 AND project_id = $2 AND id = $3 LIMIT 1;

SELECT context___trace_id, start_time FROM ( … ) …   -- orphan resolution, ±24h window

SELECT <cols> FROM otel_logs_and_spans
WHERE project_id = $1 AND timestamp BETWEEN $2 AND $3 AND context___trace_id = $4
```

The needle columns are `id`, `context___trace_id`, `context___span_id`,
`parent_id`, `attributes___session___id`, `attributes___user___id` — the seven
with parquet bloom filters.

## 10. Variable pickers — `SELECT DISTINCT … LIMIT`

Run on every dashboard load, one per template variable:

```sql
SELECT DISTINCT resource___service___name FROM otel_logs_and_spans
WHERE project_id = $1 AND resource___service___name IS NOT NULL AND timestamp … LIMIT 100;

SELECT DISTINCT name AS value, name AS label FROM otel_logs_and_spans
WHERE project_id = $1 AND resource___service___name = $2 AND name IS NOT NULL … LIMIT 500;

SELECT name FROM otel_logs_and_spans WHERE … GROUP BY name ORDER BY COUNT(*) DESC LIMIT 20;
```

Also `SELECT DISTINCT project_id FROM otel_logs_and_spans WHERE timestamp >= $1
AND timestamp < $2 AND kind IN ('server','client','producer','consumer')` —
cross-project, no `project_id` filter, run by the service-graph rollup.

## 11. Service graph — the self-join

`ServiceGraph.hs::rollupServiceEdges`, the heaviest shape in the product. A CTE
projecting ~14 columns, self-joined on `c.tid = p.tid AND c.par = p.sid`, with
`UNION ALL` branches for service→service, service→database and messaging hops.
Notes from its own comments that are worth respecting when benchmarking:

- `kind IN (…)` rather than an OR-of-equalities, because TimeFusion's
  `Utf8View` OR predicate returned wrong rows. **HISTORICAL — fixed by
  `e0bf291`.** The cause was not Utf8View: the tantivy id-prefilter intersected
  per-term id sets, which is sound for `AND` and empty for `OR`, so the scan
  skipped every file. `IN` may still be the better spelling on speed alone,
  since `kind` is raw-indexed — but it is no longer a correctness requirement.
  §11b of `tests/slt/monoscope_query_shapes.slt` asserts the two forms agree.
- Every projected column is aliased (`knd`, `stc`, `dur`, `nm`) because
  DataFusion cannot resolve `c.kind`/`p.kind` when both sides carry the
  unqualified base-table name.
- `ORDER BY COUNT(*)` rather than the ordinal, because DataFusion rejects an
  ordinal pointing at an aggregate.

## 12. Scalar stats — single value, no bucket

```sql
SELECT ROUND(SUM(CASE WHEN duration <= 500000000 THEN 1.0
                      WHEN duration <= 2000000000 THEN 0.5 ELSE 0 END)
             / GREATEST(1, COUNT(*))::numeric, 2)::float
FROM otel_logs_and_spans WHERE project_id = $1
  AND (kind = 'server' OR name = 'apitoolkit-http-span' OR name = 'monoscope.http')
  AND duration IS NOT NULL AND timestamp BETWEEN $2 AND $3
```

Plus `SELECT count(*)::bigint …` and `SELECT count(*), COALESCE(SUM(message_size_bytes),0) …`
for usage metering.

## 13. Window functions

`SUM(COUNT(*)) OVER ()` for "% of total" columns, and `count(*) OVER ()` as a
total-rows companion in chart queries (`jsonb_build_array(…, count(*) OVER ())`).

## Which of these route to the rollup tier

As of 2026-08-22, after the routing work:

| shape | routes? | why |
|---|---|---|
| §1 throughput | **yes** | `request_count` measure, bucket group |
| §2 grouped by service / kind / status_code | **yes** (since `8f29584`) | those three are declared dimensions; the `COALESCE` wrapper is now unwrapped |
| §2 grouped by `name`, `url_path`, `http.method`, `db.system` | no | not declared dimensions — `unknown_group_by` |
| §3 percentiles, unfiltered | **yes** (since `0462eed`) | needed an unfiltered `duration_digest` |
| §3 percentiles under a `server` filter | yes | `server_duration_digest` |
| §4 error rate | only when the predicate matches the declared `error_count` exactly | otherwise `missing_measure` |
| §5 dcount over `resource___service___name` | **yes** (since 2026-08-26) | `service_name_hll`; the column is a dimension, so `IS NOT NULL` is a row filter |
| §5 dcount over `name` / `body` / `context___trace_id` | no | each guards on a non-dimension and needs a `count` guard measure too |
| §6 top-K tables | no | `name`/endpoint not a dimension; `MAX(timestamp)-MIN(timestamp)` is not decomposable |
| §7 facets | no | arbitrary field |
| §8–§11 | n/a | not aggregates |

## Benchmarking notes

- Take a **warm** repetition. Cold first-touches on these shapes read 12s/36s/
  timeout where the warm figure is 335ms/1.2s/2.9s.
- Use `count(1) FROM (SELECT id …) t`, not bare `count(*)`, when the intent is
  to exercise the scan — a bare count is answered from Delta statistics without
  building one.
- The routing decision is only visible in `timefusion_stats`
  (`rollup_hits_hybrid_total` / `rollup_miss_*`). `EXPLAIN` cannot see it: the
  substitution happens in `DmlQueryPlanner`, and EXPLAIN renders the inner plan
  with the default planner.
- ~~`EXPLAIN ANALYZE` must be spelled without parentheses; `EXPLAIN (ANALYZE) …`
  is a parse error in TimeFusion.~~ **No longer true** — the parenthesised form
  plans today (pinned in §15 of the slt suite).
