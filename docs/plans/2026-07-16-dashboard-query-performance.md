# Dashboard query performance improvement plan

## Goal

Make the common Overview/Service dashboard workloads responsive for high-volume projects without increasing write-path pressure:

- throughput: bucketed server-span counts;
- latency: bucketed P50/P95/P99 duration;
- latency breakdown: root server span to downstream-child-span aggregation.

Success is measured against production-like data using the dashboard's actual adaptive rollup intervals:

| Range | Target source | Cold target | Warm p95 target |
|---|---|---:|---:|
| 1 hour | raw spans, 1-minute buckets | < 2 s | < 750 ms |
| 3 hours | raw spans, 5-minute buckets | < 3 s | < 1 s |
| 3 days | aggregate rollups, 15-minute/hour buckets | < 3 s | < 1 s |

Latency breakdown is a drill-down query, not a default broad-range panel. Its initial target is < 3 seconds for a selected service over 1 hour.

## Production investigation

Investigation date: 2026-07-16. Deployment observed: `b43945d`.

All queries used direct `project_id = '<literal>'` predicates and read-only production connections. Tests were serial and bounded; a five-project, three-day service-discovery query did not complete in two minutes, so broad concurrent scans and three-day `EXPLAIN ANALYZE` were intentionally not run.

### Current health

- Buffer pressure: 4%.
- Backpressure/rejected writes: 0.
- Provider-cache hit rate: 78.8%, but `provider_cache_entries` reports 0.
- Approximate scan p99: 524 ms, improved from about 2.1 s in the earlier check.
- Approximate pgwire p95/p99: 262 ms / 4.2 s. These are aggregate, workload-sensitive counters, not a benchmark replacement.

### Direct one-hour service discovery

This intentionally simple query is representative of dashboard filtering before percentile work:

```sql
SELECT resource___service___name, COUNT(*)
FROM otel_logs_and_spans
WHERE project_id = '<project>'
  AND timestamp >= now() - INTERVAL '1 hour'
  AND kind = 'server'
GROUP BY resource___service___name
ORDER BY COUNT(*) DESC
LIMIT 1;
```

| Project | Previous | Current |
|---|---:|---:|
| `87576849-…` | 12.1 s | >20 s timeout |
| `edb04135-…` | 13.9 s | 14.6 s |
| `00000000-…` | 7.4 s | 9.7 s |
| `dcad860a-…` | 3.4 s | 2.4 s, no server spans returned |
| `be87ebc1-…` | 1.1 s | 1.1 s, no server spans returned |

The active-project results show that lightweight direct-filter queries still have unacceptable latency.

### Representative high-volume project results

Project `edb04135-…` (`blockradar-production`) had roughly 35k server spans during the one-hour probe.

| Query | 1h | 3h | 3d |
|---|---:|---:|---:|
| Throughput: `COUNT(*)` by minute | 10.8 s | 15.5 s | >30 s timeout |
| Latency: P50/P95/P99 by minute | 8.8 s | 10.8 s | >30 s timeout |

The three-day test deliberately used minute buckets, which is more demanding than the dashboard's adaptive rollup. It still proves that raw-span scanning is not viable for this range.

### Plan evidence

For the high-volume project's throughput query:

| Range | `EXPLAIN` time | Delta scan groups | Shape |
|---|---:|---:|---|
| 1h | 3.34 s | 48 | Delta scan + buffered/Delta union |
| 3h | 5.83 s | 48 | Delta scan + buffered/Delta union |
| 3d | 6.57 s | 48 | Delta scan + buffered/Delta union |

The one-hour throughput-plan time improved from 4.14 s to 3.34 s (~19%), but planning alone remains far beyond the dashboard budget. The direct project predicate is recognized, yet all ranges retain 48 scan groups and pay significant Delta/provider setup cost before aggregation.

## Diagnosis

1. **Planning and provider/snapshot resolution are expensive.** A non-executing one-hour `EXPLAIN` takes 3.34 seconds.
2. **Time pruning does not reduce setup enough.** Narrow and broad ranges still form 48 Delta scan groups.
3. **Dashboard widgets duplicate scans.** Traffic, error rate, Apdex, percentile charts, and tables share most of the same project/time/server-span filter.
4. **Raw spans are the wrong source for multi-day panels.** Increasing bucket width reduces result rows but does not eliminate raw-file discovery and scanning.
5. **Latency breakdown is intrinsically costly.** It self-joins root spans to child spans by `(trace_id, parent_id)` and is unsuitable for default broad-range loading.

## Work plan

### 1. Add phase-level query diagnostics

**Outcome:** identify whether time is spent on Delta snapshot resolution, object-store metadata/listing, file selection, Parquet reads, or execution.

Record per query:

- logical and physical planning time;
- Delta snapshot/provider resolution time;
- number of files and row groups considered/pruned;
- bytes read;
- object-store metadata and data-read time;
- execution time by scan, join, aggregate, and sort;
- buffered versus Delta rows scanned.

Expose the data in structured logs and `timefusion_stats`; attach the query shape/range, not SQL values or customer payloads.

**Verify:** a benchmark report can account for nearly all wall time by phase and can show file/row-group pruning for 1h, 3h, and 3d.

### 2. Fix Delta/provider metadata reuse

**Outcome:** reduce planning and setup cost shared by all dashboard queries.

- Keep a bounded cache of resolved Delta snapshots and file lists per table/project/version.
- Reuse provider/file-group state for identical table, project, and relevant date range predicates.
- Investigate why `provider_cache_entries` is zero despite a nonzero hit rate.
- Invalidate only on a Delta version change, flush, or configured TTL—not for every widget request.

**Verify:** 1-hour `EXPLAIN` p95 is below 500 ms warm and substantially lower cold; cache entries/hits/misses are observable.

### 3. Reduce scan work for short raw-span ranges

**Outcome:** let timestamp filters eliminate most files/row groups for 1h and 3h queries.

- Inspect actual Parquet file count, file sizes, row groups, and timestamp min/max statistics by `(project_id, date)`.
- Compact active partitions to avoid fragmented small files.
- Preserve or establish timestamp-local ordering during compaction so Parquet statistics prune effectively.
- Do not add highly granular timestamp partitions without proving they reduce total file count and metadata work.

**Verify:** the high-volume 1h query scans materially fewer files/row groups/bytes than its 3h version and stays within the raw-span targets.

### 4. Coalesce Overview golden-signal scans

**Outcome:** avoid multiple scans of the same raw spans on initial dashboard load.

Produce a single per-bucket aggregate containing request count, error count, duration sum, and one percentile sketch:

```sql
SELECT
  time_bucket('<interval>', timestamp) AS time,
  COUNT(*) AS requests,
  COUNT(*) FILTER (
    WHERE status_code = 'ERROR'
       OR COALESCE(attributes___http___response___status_code, 0) >= 500
  ) AS errors,
  SUM(duration) AS duration_sum,
  percentile_agg(CAST(duration AS DOUBLE)) AS duration_sketch
FROM otel_logs_and_spans
WHERE project_id = '<project>'
  AND kind = 'server'
  AND timestamp >= '<from>' AND timestamp < '<to>'
GROUP BY time;
```

Derive P50/P95/P99 from that single sketch in the final projection or query layer. Let traffic, error rate, and latency widgets consume this shared result.

**Verify:** initial Overview loading performs one raw-span aggregate scan for golden signals rather than independent near-identical scans.

### 5. Create dashboard rollups

**Outcome:** route three-day-and-larger dashboard ranges away from raw spans.

Create a rollup keyed by:

```text
project_id, bucket, service, endpoint,
request_count, error_count,
duration_sum, duration_min, duration_max,
mergeable_percentile_sketch
```

Use:

- raw spans / 1-minute buckets for 1h;
- raw spans / 5-minute buckets for 3h;
- 15-minute or hourly rollups for 3d;
- hourly or daily rollups for longer ranges.

**Verify:** 3-day throughput and percentile panels read rollup records only and meet the 3-second cold / 1-second warm targets.

### 6. Make latency breakdown an explicit drill-down or rollup

**Outcome:** prevent the root-to-child span self-join from blocking dashboard first paint.

- Require a service and default to 1h/3h for downstream-operation breakdown.
- Do not load it automatically for a 3-day range.
- If broad-range breakdown is required, materialize `(project, bucket, service, parent operation, downstream operation)` totals during flush/compaction.

**Verify:** default dashboard load never runs the self-join; selected-service one-hour breakdown meets its target.

## Benchmark protocol

Run against the supplied high-volume projects after each change:

1. Use the exact production dashboard query generator and adaptive bucket intervals.
2. Measure cold once and warm at least five times on a reused connection.
3. Capture wall time, plan time, execution time, scanned files/row groups/bytes, and returned rows.
4. Test throughput, latency percentiles, and latency breakdown independently at 1h, 3h, and 3d.
5. Run serially against production with a statement/client timeout; use a production-shaped local dataset for exhaustive `EXPLAIN ANALYZE` and regression runs.
6. Record p50, p95, and max, rather than a single timing.

## Priority order

1. Phase-level diagnostics and provider-cache investigation.
2. Delta metadata/file-list reuse.
3. Compaction and timestamp-local Parquet layout.
4. Coalesced golden-signal query path.
5. Multi-range rollups.
6. Materialized or drill-down-only latency breakdown.
