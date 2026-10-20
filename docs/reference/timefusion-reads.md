# Reads, routing, and indexes

Reviewed: 2026-10-20. This reference summarizes the August–October plan history through source revision `ad1ee10b`.
Historical measurements describe their original builds and workloads. They are not current acceptance results.
The [current backlog](../plans/2026-10-19-next-work.md) owns implementation priorities.
The [review inventory](../reviews/2026-10-19-timefusion-triage.md#plan-inventory) maps the source notes to these topic summaries.

## Query correctness comes before pruning

TimeFusion reads MemBuffer and Delta together, then resolves versions under the table's dedup contract.
Merge-on-read updates can append a new version at the original event timestamp.
A filter on a mutable field must not discard the newer winner before version resolution.
Otherwise, an older matching version can become visible again.

This affects hash membership, session enrichment, text prefilters, and any optimization that narrows only one source leg.
A useful index result is not automatically a proof of complete logical visibility.
The safe fallback keeps the full candidate population and the original predicate.
Unknown coverage or ordering can cost performance. It must not change results.

The MemBuffer's routing span differs from its true row span.
Routing timestamps drive the merge-on-read mask. True row timestamps support read pruning.
Widening the routing span to the row span can mask unrelated Delta rows.

## Ordering and bounded dedup

Keep-greatest dedup needs the declared ordering and dedup keys to agree.
When every dedup key forms an ordering prefix, all versions of a key are adjacent.
The operator can collapse each run without retaining the whole window.
Timestamp-led inputs retain their numeric fast path. Other key types use tuple ordering.

A lying ordering is a correctness risk, not merely a slow query.
After a streaming operator emits a run, it cannot retract that result when the same key appears again.
Footer checks must identify the actual ordered fields, including nested Parquet leaf indices and direction/null options.
A footer's existence does not prove it matches the configured schema.

One unordered file formerly removed the common ordering for an entire scan.
Later work preserved ordered subsets and isolated repairs instead of forcing every file through a blocking sort.
A large plan-time file selection does not prove those files were decoded at runtime.
Likewise, a removed scan guard cannot explain a later query refusal.

The unbounded seen-set remains a bounded-resource fallback with its own memory ceiling.
Raising that ceiling does not repair a lost ordering or prove a query will finish.
A LIMIT above dedup cannot justify reading only the first physical versions.

## Metric-first layout

The implemented `otel_metrics` order is `(metric_name, timestamp DESC, series_id)`.
Its dedup keys use the same prefix, with the configured version tiebreak unchanged.
Project/date partitioning remains intact.

The workload selects a metric and then a time range. It does not generally pin `series_id`.
Placing series before time spreads the selected metric's time window across every series run.
Partitioning by metric instead creates many small partitions and files.
The chosen layout lets ordinary Parquet statistics and page indexes prune inside the selected metric.
It does not require a proprietary clustering feature.

Migration required four supporting mechanisms:

- Collapse complete key tuples rather than assuming a numeric leading key.
- Repair files whose declared footer order differs from the schema.
- Dedup the dates of each layout separately during mixed-layout reads.
- Bound maintenance slices and permits when the leading key is a string.

The October 5 migration record reports 44.7M to 368K decoded rows for demo web vitals.
Scanned bytes fell from 459 MB to 2.3 MB in that recorded window.
A measured migration window is not a census of every historical file.
Time-only maintenance still needs its own decode-cost gate after a layout change.
Attribute filters remain a separate potential cost because Variant attributes lack ordinary column statistics.

## Latest-N reads

Opening a head batch from every file group can dominate a sparse filtered list.
The old merge decoded many head files before returning the newest 501 rows.
A useful bound comes from file timestamp statistics or an index's newest matching timestamps.

Keep these conditions when changing the merge or TopK path:

- Compare bounds at full timestamp precision and preserve equal-timestamp ties.
- Treat missing statistics and null uncertainty as unbounded.
- Keep a small read-ahead window so serialized I/O does not replace excess I/O.
- Open more groups when the current groups cannot fill the page.
- Apply the same timestamp restriction to every version-bearing leg.
- Preserve deletion-vector selections when constructing an ascending or descending provider.

`622346ac` narrowed newest-first pages using indexed match bounds.
`5b2a3dec` replaced the earlier ascending implementation after its deletion-vector planning failure.
A local plan-shape assertion alone does not prove the production PGWire path takes the optimization.

## Hash membership and exact histograms

Array-element indexes represent exact membership. Duplicate elements within one row count once.
OR overlap also counts a matching row once, not once per matching term.
NULL and empty arrays need explicit fixtures.

Native histograms must count visible logical winners, not physical indexed documents.
Capture pins the file snapshot, deletion vectors, metadata, and current Arrow batches used for visibility.
Physical row ordinals come from the committed Parquet file's order.
Pre-sort ingestion batches cannot claim those ordinals.

Partial coverage requires winners across indexed files, uncovered files, and MemBuffer.
An old indexed version can lose to an unindexed update or tombstone.
Conversely, pruning the indexed winner can resurrect an old uncovered match.
A count proof therefore binds source identity, dedup keys, tiebreak, tombstone semantics, and clipped query bounds.
A histogram must decline when it cannot establish that contract.

The visibility cache retains Delta masks only when current memory rows cannot affect that cached day.
Its key includes ordered file metadata, deletion vectors, root, schema, version semantics, and query bounds.
Current-memory masks are recomputed. Cache eviction does not release reservations still held by a query.
Prepared sources and masks share bounded resident/query budgets.

The September optimized benchmark checked 240 exact-bucket and route cases over three million rows.
It eliminated redundant preparations but did not establish a controlled latency speedup.
The common-term native case remained slower than ordinary SQL.
A complete, freshly cached index can be fast while a partially indexed day is slower than an ordinary scan.
Coverage and cold-fetch state must accompany every histogram timing.

## Mutable membership narrowing

`b35f7acd` narrows a mutable-hash list with the union of matching timestamps from all relevant sources.
Indexed matches alone are insufficient.
The candidate set also includes matches from uncovered in-window files and MemBuffer.
Every version of a dedup key shares its timestamp, so all legs retain possible winners at those timestamps.

The path falls back on errors, excess candidates, or excessive uncovered bytes.
`651f7c4c` bounded raw bytes read during planning. `ad1ee10b` memoized over-cap membership.
Do not reopen the old “cap planning bytes” task as unimplemented.
Do not replace this exact candidate union with a probabilistic membership decision that can drop unique rows.

## DML capture fences

A project-wide active-DML fence unnecessarily blocked historical histogram capture during unrelated recent updates.
Range-aware fences allow non-overlapping capture only when the mutation range is proved.
An overlapping writer permanently invalidates an existing capture ticket, even after the writer finishes.

Unknown predicates, ambiguous source timestamps, timestamp assignments, precision loss, overflow, and uncertain recovery retain a table-wide fence.
Every queued statement retains its guard through folding, splitting, retry, and cancellation.
The coalescer must also fence its final widened predicate for every folded project before mutation.
Original statement intervals do not cover gaps introduced by predicate folding.
No registry lock spans I/O, and narrower ranges do not weaken atomic ordering.

## Search coverage and body predicates

Fresh indexes use the committed-file builder and remain detached from flush commit under bounded admission.
Startup and periodic backfill share a per-table semaphore and the same bounded builder.
Skipping a duplicate pass does not claim coverage.
File count, compressed bytes, build concurrency, and fair project selection remain separate bounds.

Index carry-forward can preserve useful coverage through compaction.
Do not infer that every rewritten file requires a complete rebuild.
Manifest caches, per-index fan-out, low-selectivity memoization, and empty-window avoidance address different read costs.
A counter with one combined refusal label cannot distinguish missing index, mutable visibility, budget, and selectivity.

`9707b2ae` and `f0537c13` route eligible JSON body searches through the body index while retaining the original predicate.
The indexed serializer and SQL rendering must agree. Regex and LIKE rewrites need explicit semantic checks.
An unsafe Variant predicate cannot be readmitted to a source leg merely to enable pruning.

Today's backfill exclusion avoids indexing files that packing will immediately replace.
It also leaves body searches scanning today's uncovered files with JSON rendering.
The October 15 record reports roughly 270 uncovered today files, despite only two uncovered sealed files.
The routed 1d search took 7.9 seconds versus 54 seconds bypassed; 7d and 30d still timed out at 90 seconds.
A normal sealed-index monitor does not close this gap.
The current backlog requires a bounded indexing experiment with a measured CPU price.

## Rollup query contracts

Coverage and matcher eligibility are separate questions.
A fully built tier cannot answer a query whose filters, dimensions, bucket grain, or measures do not match.
Every selected aggregate needs a correct stored or derivable measure.
Serving one measure from a tier while silently dropping another changes the result.

Retain fixtures for these previously expensive shapes:

- Nested common-expression projections, aliases, casts, and supported `coalesce` wrappers.
- HTTP-scoped counts, error-rate measures, cumulative apdex bands, and status breakdowns.
- An unnesting tier queried with one pinned element, with original array predicates on raw legs.
- Route-filtered charts and per-tier version-only witnesses.
- Sessions with browser scope, latest-page semantics, deterministic ties, URL/UA fallbacks, and legacy-cell refusal.
- Exact billing/count queries under updates, tombstones, and replay.

A session-ID lookup also needs the recorded re-delivery/enrichment lost-update race reproduced before claiming pruning parity.
A tier generation must remain stable when unrelated schema representation changes.
Changing a Debug representation once caused an unintended historical rebuild wave.

`EXPLAIN ANALYZE` can bypass the application rollup route.
Use executed route evidence and bounded counter differences around plain statements.
A raw event/trace query is not an aggregate-routing opportunity.
An inclusive `BETWEEN` upper bound can create a one-microsecond raw fringe even for a closed aggregate window.

## Cache, planning, and cold starts

The Foyer disk tier survives process restarts, but manifests, index readers, and plans have in-process state.
A warm data cache therefore does not imply a warm first query.
The October disk-tier recovery fix `f91b6dfa` repaired a zero-clean-block reclaim deadlock.
Repeated-query latency fell from about 20 seconds to 1.4 seconds in its recorded comparison.
Later fixed-size block caching reduced repeated range duplication.

Other independent reductions included project-pinned Delta replay seeds, avoiding a second replay, and projected MemBuffer batches.
`6c446306` listed the certification snapshot once per call rather than once per date.
`ef36c8ed` memoized partition statistics per snapshot version.
`04fb9f28` scoped output coverage to the routed project.
These improvements do not prove the residual cold-reader cost is resolved.

## Retained query targets and tools

The October long-window plan defined these targets for sequential, mature-process measurements:

| Shape | Median target | Maximum target |
| --- | ---: | ---: |
| Overview, 7d–30d | under 2 s | under 5 s |
| Unfiltered log list | under 1 s | under 3 s |
| Filtered list with at least 0.5% matches | under 3 s | under 8 s |
| Filtered chart, 7d–30d | under 3 s | under 10 s |
| Sparse needle, 30d | completes below 90 s | — |

Latest-window goals were median under 300 ms/p95 under 1 s for at most 1h.
The 24h goals were median under 1 s/p95 under 2 s, with no overview statement above 5 s.
These are retained acceptance targets, not a claim that every project meets them.

Use [the client shapes](../monoscope-query-shapes.md), [contract fixtures](../../tests/slt/monoscope_query_shapes.slt), and [latest-query benchmark](../../bench/latest_bench.py).
The query-routing harness records query/result digests, source identity, timings, ambient counters, and optional independent raw-oracle results.
Its EXPLAIN hint remains separate from executed routing evidence.
Run the current multi-project matrix before declaring fleet-wide completion.
