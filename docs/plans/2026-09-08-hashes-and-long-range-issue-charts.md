# Hashes and long-range issue charts

> The recommendation below is superseded by
> [the deeper benchmark and implementation plan](2026-09-08-indexed-hash-histogram-plan.md).
> Its production measurements remain valid. Later tests support native Tantivy
> histograms before a separate hourly-count system. They also expose a partial-coverage
> hazard that the earlier discussion of ID prefilter reuse did not resolve.

Research date: 2026-09-08. TimeFusion checkout: `35d9a702`. Monoscope checkout: `39155e4a6`.

The recommendation is to serve long-range charts from durable event-time counts, with a separate membership index for event drill-down.
The current chart scans versioned telemetry before it can apply the hash filter.
Reducing the selected time range does not meet the business requirement for 3-day, 7-day, and 30-day charts.

## Production evidence

The [reported issue](https://app.monoscope.tech/p/00000000-0000-0000-0000-000000000000/issues/7e6d04e5-0f43-4e16-ad90-e480f9ab1f1d?since=7D)
belongs to the all-zero project, with service `checkout` and target hash `e03848c6`.
Its type is `runtime_exception`, so its telemetry tag is `err:e03848c6`.
The API reports one occurrence on September 2.

The investigation used these read-only commands:

```sh
monoscope auth status
monoscope -p 00000000-0000-0000-0000-000000000000 issues get 7e6d04e5-0f43-4e16-ad90-e480f9ab1f1d
```

Python `psycopg` probes used the existing application database URLs without printing credentials.
All windows ended at `2026-09-08T15:53:24.566813Z`.
Each raw query used a one-hour chart bucket and a 10-second statement timeout.
The bucket width is a controlled benchmark choice, not a captured browser request.

| Window | Raw containment | Current-source JSONPath | Existing hourly counts | Hourly result |
|---|---:|---:|---:|---|
| 3 days | timeout, 10.023 s | timeout, 10.029 s | 117.8 ms | no buckets |
| 7 days | timeout, 10.103 s | timeout, 10.073 s | 30.2 ms | one bucket, one occurrence |
| 30 days | timeout, 10.027 s | timeout, 10.026 s | 25.8 ms | one bucket, one occurrence |

These are individual client measurements, including network time but excluding connection setup.
The timeouts establish a lower bound, not the full query duration.
The three-day hourly result is consistent with the September 2 occurrence falling outside that window.
The raw queries did not finish, so these results do not establish agreement between raw and hourly counts.

[Measurements and exact raw SQL](evidence/2026-09-08-hashes/measurements.json) include physical-plan excerpts.
All six plans contain this structure:

```text
Aggregate
  Filter: hash membership AND deleted IS DISTINCT FROM true
    DedupExec: keys=[timestamp, resource___service___name, id], greatest
      SortPreservingMerge
        Union
          memory
          Sort: timestamp DESC
            Delta scan, with time filtering
```

The hash cannot narrow this scan before deduplication.
The Delta-side sort also adds work before aggregation.
The plans establish these mechanisms, but do not quantify their separate CPU or I/O costs.
No browser trace, production build SHA, or fleet-wide latency distribution was captured.

## Current query and storage

`../monoscope/src/Pages/Anomalies.hs:964` builds this chart query:

```kql
hashes[*]=="err:e03848c6" | summarize count(*) by bin_auto(timestamp)
```

The current parser renders wildcard equality through `jsonb_path_exists` in
`../monoscope/shared/src/Pkg/Parser/Expr.hs:1464`:

```sql
jsonb_path_exists(to_jsonb(hashes), '$[*] ? (@ == "err:e03848c6")'::jsonpath)
```

The older research assumed `hashes @> ARRAY[...]`.
Both forms timed out in this investigation, and neither reached the scan as a hash filter.
Native array membership can avoid JSON conversion, but that change alone does not resolve the measured scan structure.

`schemas/otel_logs_and_spans.yaml:95` declares `hashes` as nullable `List(Utf8)` and `mutable: true`.
It declares neither a Bloom filter nor a Tantivy index for this column.
The existing dashboard rollup dimensions are service, kind, and status code.
They cannot answer arbitrary per-hash counts.

Mutable values require selection of the newest row version before membership evaluation.
An old version can contain a tag that the newest version no longer contains.
A filter before deduplication can exclude the newest version and return the old version as a false match.

The September 3 audit records a real tag replacement in historical data.
That audit was not repeated today, and its raw scratchpad is absent from this checkout.
Current schema and DML behavior still permit replacement, so append-only behavior remains an invalid engine assumption.
See [the earlier safety investigation](2026-09-03-the-hashes-pushdown-is-not-safe-and-already-exists.md).

`src/database/mod.rs:9983` already restores mutable filters when a certified scan can safely skip deduplication.
Certification remains useful maintenance work.
It does not provide a predictable latency bound for hot data or high-volume, long-range charts.

## Why a schema-only index change is insufficient

The earlier recommendation to add a raw Tantivy index omitted two implementation gaps:

1. `src/tantivy/mod.rs:228` joins list elements with spaces before indexing.
   With a raw tokenizer, that represents the whole joined string, not independent tag terms.
2. `src/read/optimizers.rs:1340` routes scalar predicates, not array membership or this JSONPath expression.
   An index declaration alone does not make the chart use the index.

Tantivy supports separate values for a multivalued field.
Its [official example](https://tantivy-search.github.io/examples/index_with_json.html) shows this representation.
A hash index needs one exact term per distinct tag and a matching query route.

The safe route must retain candidate event identities through version resolution and evaluate the original predicate on the winner.
`src/database/mod.rs:11011` already disables file exclusion and row selection for mutable predicates.
The existing ID prefilter is a useful foundation, subject to coverage and version tests.
Missing or outdated indexes must retain a raw fallback.
The rollout also needs a measured rebuild budget and an index-format version change.
The earlier rebuild-rate estimate is historical evidence, not a current capacity estimate.

PostgreSQL [GIN array indexes](https://www.postgresql.org/docs/current/gin.html) illustrate the same element-to-row lookup model.
A PostgreSQL index on the mirror does not accelerate TimeFusion's Parquet scans.

## Existing hourly counts: fast, but different semantics

`apis.error_hourly_stats` has a primary key on `(project_id, error_id, hour_bucket)`.
The production lookup for this issue returned its September 2 bucket quickly:

```sql
SELECT s.hour_bucket, SUM(s.event_count)
FROM apis.error_hourly_stats s
JOIN apis.error_patterns e
  ON e.project_id = s.project_id AND e.id = s.error_id
WHERE e.project_id = '00000000-0000-0000-0000-000000000000'
  AND e.hash = 'e03848c6'
  AND s.hour_bucket >= TIMESTAMPTZ '2026-09-08 15:53:24.566813+00' - INTERVAL '30 days'
  AND s.hour_bucket <= TIMESTAMPTZ '2026-09-08 15:53:24.566813+00'
GROUP BY s.hour_bucket
ORDER BY s.hour_bucket;
```

The writer in `../monoscope/src/BackgroundJobs.hs:2121` groups incoming error records by hash.
`../monoscope/src/Models/Apis/ErrorPatterns.hs:560` assigns them to `truncateHour now` and increments existing counts.
It does not derive the bucket from each event timestamp.
This path also does not establish idempotency for repeated batches or count distinct telemetry rows.

Consequently, delayed events, retries, or several matching errors on one event can disagree with the chart's `count(*)`.
An hourly table also cannot reproduce arbitrary partial-hour boundaries without additional data.
Missing coverage must not become a zero count.

Log patterns have another constraint:
`../monoscope/src/BackgroundJobs.hs:5218` prunes their hourly stats after `baselineWindowHours + 24`, currently 72 hours.
Those stats cannot cover seven or thirty days without retention changes and historical backfill.
The existing error statistics prove that the aggregate approach is practical, but do not prove a ready replacement for every issue chart.

## Recommended implementation

### 1. Establish an authoritative membership source

Represent each current event-to-tag membership with project, event timestamp, complete event identity, and tag.
Use `(timestamp, resource___service___name, id)` as the existing event identity within a project.
Treat duplicate tags on one event as one membership.
Preserve tag removals, deletes, replays, and canonical-hash changes.

A narrow membership table or index separates tag lookup from the wide telemetry row.
The first implementation can reuse the existing error and pattern pipelines only after their counting semantics match this contract.
Otherwise, build the memberships from canonical telemetry versions.

### 2. Materialize counts for chart reads

Maintain counts by `(project_id, tag, event_time_bucket)` with explicit coverage and freshness metadata.
One-minute buckets support multiple chart widths.
Hourly counts can serve aligned long-range views if the interface explicitly selects that resolution.

For tag updates and deletes, revise affected buckets from canonical memberships or apply idempotent corrections.
Blind increment-only writes are insufficient.
Persist at least thirty days of coverage plus the supported late-arrival and repair window.
Backfill historical buckets before switching the chart route.

For total-event versus anomaly overlays, maintain a separate total-event series using the same event population and time boundaries.
Summing all tag counts overcounts events that carry several tags.
For an OR across issue hashes, count the union of event identities rather than summing overlapping hash counts.

Combine complete aggregate buckets with exact membership reads at partial boundaries and in the uncovered recent tail.
Use disjoint intervals to prevent double counting.
Keep arbitrary KQL filters on the raw path unless the aggregate stores all dimensions needed for those filters.

### 3. Add the exact membership index for drill-down

Index individual tags and route native array membership to that index.
Normalize only equivalent JSONPath forms to native membership.
Preserve exact semantics for nulls, empty arrays, multiple tags, and negation.
Retain complete version groups and the final membership predicate.

This index benefits sparse issue drill-down and exact boundary reads.
Frequently used endpoint tags can match most events, so durable aggregates remain necessary for their long-range charts.

### 4. Measure acceptance before switching traffic

Use the linked issue plus busy projects, common endpoint tags, rare error tags, and log patterns.
Measure 3-day, 7-day, and 30-day charts, including empty results and overlapping issue hashes.
Compare every bucket against canonical raw results on a stable snapshot.
Include late enrichment, tag replacement, deletion, duplicate delivery, partial boundaries, and incomplete index or aggregate coverage.

A proposed target is chart API p95 under one second across these windows, with no raw scan of the complete historical window.
This is an acceptance target, not a measured result or an agreed SLA.
Measure cold and warm reads under normal ingestion, including freshness, ingestion cost, and background repair throughput.
Keep the old route available until count parity and coverage pass.

## Validation and scope

This change records research and production read-only measurements.
It does not change the schema, deploy an index, or switch the chart data source.
The performance repair remains implementation work.

The evidence JSON parses successfully, all six plans contain deduplication after the scan, and all six raw probes reached the timeout.
`git diff --check` passed.
No Rust source changed, so Rust CI checks were not run and no CI attestations were published.
No PR or push was requested.
An implementation PR must run the relevant `make ci-signoff` checks and record any checks left for GitHub.
