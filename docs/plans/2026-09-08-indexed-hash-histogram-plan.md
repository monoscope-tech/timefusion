# Build hash histograms inside the index

Date: 2026-09-08. TimeFusion source: `35d9a702`. Monoscope source: `39155e4a6`.

**Decision: implement exact array-element indexing and native Tantivy histograms first.**
The chart must return bucket counts directly from the index, without an ID list followed by a telemetry scan.
Snapshot-consistent version visibility is part of this implementation, not a later optimization.
Do not start with a new hourly-count database or a new Roaring index format.

This replaces the aggregate-first recommendation in [the initial investigation](2026-09-08-hashes-and-long-range-issue-charts.md).
The original measurements proved that the current scan was slow. They did not prove that precomputed counts were necessary.
The new experiments distinguish those claims.

## What the experiments established

All local datasets contain arrays with one endpoint tag and optional error tags.
Events span thirty days, with 0.1% rare-hash membership, 1% medium membership, and 90% common membership.
The overlap query combines a 1% tag with a 0.5% subset, so its correct count remains 1%.
The PostgreSQL and Tantivy tests compare every returned bucket against an independent expected result.

### Thirty-day hourly charts: ten million events

| Method | Rare: 10,000 matches | Medium: 100,000 matches | Common: 9,000,000 matches |
|---|---:|---:|---:|
| PostgreSQL without array index | 1,395 ms | 1,415 ms | 4,203 ms |
| PostgreSQL GIN on array | 11.1 ms | 313 ms | 4,201 ms |
| PostgreSQL covering membership table | 4.1 ms | 36.1 ms | 4,675 ms |
| PostgreSQL hourly counts | 0.95 ms | 0.98 ms | 1.43 ms |
| Native Tantivy histogram | **7.7 ms** | **50.7 ms** | **208 ms** |

These are local median client times, not a production latency forecast.
PostgreSQL samples repeat three times. Tantivy samples repeat five times.
PostgreSQL uses one query worker, 64 MB work memory, disabled JIT, and parameter-aware custom plans.
Tantivy uses a local index with the library's search execution defaults.
The engines have different storage and execution settings, so this table compares mechanisms rather than overall database quality.
No cold object-store reads, ingestion contention, network fan-out, or TimeFusion version resolution occur in these local measurements.

At one million events, GIN answered the rare case in 1.1 ms and the common case in 372 ms.
Tantivy answered those cases in 1.1 ms and 21.2 ms.
The ten-million-event narrow Tantivy index took 19.2 seconds to build and occupied 130 MB.
It contains only exact tag terms and indexed/fast timestamps, so this size excludes the production event-key and visibility metadata.

The existing hourly table wins on pure lookup cost, but loses information.
Adding its two overlapping tag counts returned 150,000 instead of the correct 100,000.
The GIN, membership, and Tantivy queries returned the correct union count.

Tantivy also passed every bucket comparison with a 17-minute width.
For thirty days, that query took 8.7 ms for the rare hash and 210 ms for the common hash.
The index therefore supports flexible fixed-width charts without precomputing each chart resolution.

### Three-, seven-, and thirty-day Tantivy measurements

| Window | Rare | Medium | Common |
|---|---:|---:|---:|
| 3 days | 16.9 ms | 21.0 ms | 37.4 ms |
| 7 days | 15.6 ms | 25.4 ms | 62.8 ms |
| 30 days | 7.7 ms | 50.7 ms | 208.4 ms |

Shorter windows are not always faster because their timestamp range query adds filtering work.
An absent tag took 0.58 ms across thirty days but 16.3 ms across three days.
This is another reason to measure actual query shapes rather than predict latency from selectivity alone.

### Production evidence changes the integration plan

The initial raw chart probes still establish the failure: all three windows exceeded a ten-second statement timeout.
The current JSONPath expression and native array containment both failed.

A new ten-minute production sample contained 82,799 events in project `87576849-4941-49d3-a15d-680fef88a1a8`.
There were 25,297 single-tag arrays, 57,493 two-tag arrays, and nine empty arrays.
One endpoint hash appeared 80,277 times, approximately 97% of events.
Both distribution queries finished in about 0.5 seconds.
This sample establishes that common hashes are a real workload, not only a synthetic stress case.
It does not establish a fleet-wide distribution.

The linked issue's trace contains an event with `err:e03848c6` at `2026-09-02T08:04:30.107802Z`.
The issue creation timestamp is several seconds later, so the first one-second lookup missed the event.
A fifteen-second trace lookup returned 55 rows in 0.75 seconds and located the matching event.

Supplying that known event ID to the long-range chart still timed out:

| Window | Known-ID chart elapsed time |
|---|---:|
| 3 days | 10.0 s, canceled |
| 7 days | 20.4 s, canceled |
| 30 days | 10.5 s, canceled |

Even a seven-day `EXPLAIN` with the known ID timed out after 17.1 seconds under an eight-second statement limit.
Cancellation can exceed the configured limit, so these are observed elapsed times rather than a strict timeout bound.
The known-ID probe excludes hash discovery and does not establish the issue's complete count.
It shows that an ID lookup followed by the existing read path is not yet sufficient.
The failed EXPLAIN prevents precise attribution of this additional delay.

Source supports two concrete concerns with that route:

- `src/config.rs:891` limits the existing prefilter to 2,000 hits by default.
  It also rejects queries above 50% selectivity.
- `src/database/mod.rs:11442` constructs a SQL `id IN (...)` expression from search hits.
  `src/tantivy/search.rs` searches sidecars and materializes hits before the downstream scan.

The new chart route must bypass both the ID materialization and the 2,000-hit prefilter cap.
It needs its own bucket-count and resource limits, not an unlimited event-ID list.

## Prior art and the parts to reuse

The proposed query path most closely matches Elasticsearch and Quickwit.
It combines an inverted index for membership with columnar timestamps for aggregation.
It computes counts at query time, rather than maintaining a separate total for every hour.
Quickwit's [aggregation API](https://quickwit.io/docs/reference/aggregation) documents this fast-field approach and supports requests that return aggregations without event hits.

The main distinction from a complete database is responsibility for consistency.
TimeFusion remains authoritative for event versions in Delta and memory.
Tantivy's own document visibility cannot automatically identify superseded rows across those independent stores.
The proposed integration must supply that proof before an index-only count becomes authoritative.

| Approach | Chart work | Update behavior | Main tradeoff for this workload |
|---|---|---|---|
| PostgreSQL array GIN | Find candidate rows, fetch visible tuples and timestamps, aggregate | PostgreSQL manages MVCC and index maintenance | Strong sparse lookup, but common hashes still require substantial row processing |
| Elasticsearch / Quickwit style | Filter indexed terms, aggregate indexed columnar timestamps | Consistency depends on the engine's document and ingestion model | Flexible buckets and filters, with query cost that still grows with matches and segments |
| Pinot upsert style | Filter postings, restrict to valid documents, aggregate | Winner masks track upserts, with explicit snapshot consistency options | Closest visibility model, but maintaining masks has memory and lifecycle costs |
| ClickHouse raw query | Prune blocks, scan relevant columns, aggregate | Replacing tables need correct version resolution | Efficient broad scans, but a skipping index is not an exact per-event membership index |
| Timescale continuous aggregates | Read stored bucket summaries | Refresh policies reconcile changed buckets | Very cheap repeated charts, but resolution, dimensions, retention, and refresh coverage constrain queries |
| Proposed TimeFusion integration | Filter exact tags and count indexed timestamps through a visibility gate | Delta/memory snapshot determines which index documents count | Reuses Tantivy, but requires integration work for masks, coverage, and sidecar fan-out |

The benchmarks establish potential for the proposed computation, not superiority over a deployed Elasticsearch, Pinot, or ClickHouse cluster.
Those databases were not benchmarked here.
The PostgreSQL comparison also used different storage and execution settings from Tantivy.
Its strongest conclusion is that avoiding payload retrieval can matter substantially for this chart shape.

The initial fast path must explicitly support fixed-duration buckets.
Calendar months, local-time days across daylight-saving changes, and unsupported filters remain on the canonical path until separately implemented and verified.

| System | Relevant mechanism | Concrete implication |
|---|---|---|
| PostgreSQL | GIN maps array elements to candidate row references. MVCC determines visible rows. | Exact element indexing and row visibility are separate requirements. |
| Elasticsearch / Tantivy | Inverted term filtering plus histogram aggregation over columnar fast fields. | Return buckets directly from the search engine. |
| Apache Pinot | Multivalue inverted indexes plus valid-document bitmaps for upserts. | Intersect matches with a consistent set of winning event versions. |
| ClickHouse | Columnar scans and materialized views, with explicit version resolution for replacing tables. | Preaggregation does not eliminate update/delete correctness work. |
| TimescaleDB | Materialized time buckets with invalidation and refresh. | Use preaggregation later if indexed histograms miss the target. |

PostgreSQL documents GIN's [array operators](https://www.postgresql.org/docs/current/gin.html)
and [MVCC visibility](https://www.postgresql.org/docs/current/mvcc-intro.html).
Tantivy 0.22.1 already supplies [histogram and distributed aggregation collectors](https://docs.rs/tantivy/0.22.1/tantivy/aggregation/index.html).
The existing TimeFusion `_timestamp` field is already `FAST | INDEXED` in `src/tantivy/mod.rs:494`.

Pinot explicitly supports [multivalue bitmap indexes](https://docs.pinot.apache.org/build-with-pinot/indexing/inverted-index).
Its [upsert documentation](https://docs.pinot.apache.org/build-with-pinot/ingestion/upsert-dedup/upsert)
explains why queries need a consistent snapshot of valid-document masks across segments.
Independent masks can double-count or omit rows during concurrent updates.

ClickHouse's [ReplacingMergeTree documentation](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree)
requires query-time version resolution when background merges do not establish current rows.
Its [incremental views](https://clickhouse.com/docs/materialized-view/incremental-materialized-view)
process inserted blocks, so ordinary increment-only counts do not resolve replacement semantics by themselves.
Timescale's [refresh policies](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/refresh-policies/)
provide the relevant alternative when durable bucket summaries are required.

## Correctness findings from executable tests

### An ID prefilter is not automatically safe with partial coverage

The following two versions share an event key:

```text
raw file or memory: version 1, hashes=[a]
indexed file:      version 2, hashes=[b]
```

A search for `a` finds no indexed candidate ID.
Filtering only indexed files by that ID set excludes version 2.
The raw or memory leg retains version 1, which then wins and produces a false match.

This shape follows `scan_delta_with_tantivy`: indexed files receive `narrow(filters)`, while uncovered files receive the original filters.
The mutable-column guard disables file pruning, but it does not close this coverage gap.
An executable SQL model reproduced `partial_index=1` versus `canonical=0`.
A separate exhaustive model found 24 failures across 450 combinations of tags, deletes, and storage legs.

An integration test now reproduces this against TimeFusion, Delta, and local MinIO.
It uses the mutable `name` field on the `mor_versioned` fixture.
The old matching version occupies an uncovered file; the newer nonmatching version occupies an indexed file.
Restoring the old prefilter decision makes the test return the superseded match and fail.
Restoring the mutable-visibility guard makes the same test pass.
The production schema does not yet route hashes to Tantivy, so this is not a demonstrated production hashes-result bug.

Two models passed all 450 cases:

1. Discover matching keys from every leg, fetch all versions of those keys, then select winners and recheck membership.
2. Intersect matching physical documents with a correct, snapshot-consistent winner bitmap.

The first provides a correctness reference. The second supports direct index aggregation without fetching event payloads.

### Other checks

- Ten PostgreSQL cases established native-array versus JSONPath equivalence in `WHERE`, including null arrays, null elements, empty arrays, and duplicate elements.
  This does not establish equivalence for every JSONPath expression or projected nullable boolean.
- Native Tantivy tests passed duplicate-element counting, tag removal, tag addition, event deletion, and old-searcher snapshot behavior.
  Those tests cover one index commit, not a transaction across Delta, memory, and many indexes.
- The local Roaring prototype passed the same overlap totals at one and ten million events.
  Scattering document IDs increased the ten-million-event hour-bitmap storage from 12 KB to 20.9 MB.
  Common-hash histogram time increased from 0.21 ms to 10.7 ms.
  This confirms the mechanism but does not justify another index implementation while Tantivy already supplies the required collector.

## Concrete implementation sequence

### Change 1: exact membership representation and query normalization

Add an explicit element-indexing mode for list fields in `src/schema.rs` and `src/tantivy/mod.rs`.
Store one exact raw term per distinct non-null list element.
Preserve the existing joined-text behavior for other list fields unless their schema requests element indexing.
Do not silently change `summary` indexing.

Add native array-membership routing in `src/read/optimizers.rs` and `src/tantivy/udf.rs`.
Normalize the application's literal equality JSONPath only when the input is a compatible string-list column.
Retain the original predicate as the result check on fallback paths.
Keep `hashes` mutable.

Version the changed field representation and manifest compatibility explicitly.
Old joined-string indexes cannot claim element coverage.
Prefer per-field capability metadata so enabling hashes does not unnecessarily rebuild unrelated indexes.
Give rebuilt blobs and local cache entries distinct generation identities.
Otherwise a warm reader can use the old representation with a new manifest.
Retire replaced generations through bounded garbage collection before rollout; retain them while active snapshots can need them.

Required tests: single and multiple tags, duplicate terms, nulls, empty arrays, punctuation, Unicode, AND/OR, prepared parameters, and unsupported-expression fallback.

### Change 2: exact visibility contract for index reads

Define a query snapshot containing the Delta file identities, deletion-vector state, and memory/WAL boundary.
Bind each index document to its source file and physical row ordinal.
Include complete event identity and version metadata wherever winner resolution needs them.
The identity is `(project_id, timestamp, resource___service___name, id)`, not `id` alone.

Reuse Delta deletion vectors and existing certification where they prove current visible rows.
Do not treat a deletion vector as proof that all duplicate versions are resolved.
Existing `ordinals_valid` metadata must remain mandatory for physical-mask mapping.

For unresolved groups, maintain or compute winner masks from narrow key/version/tombstone columns across all relevant legs.
Cache masks by snapshot identity, including DV changes and the memory boundary.
Invalidate them on updates, deletes, replay, flush, and compaction.
Index publication must not publish a mask from a different snapshot.

Begin with fully covered, certified slices and exact fallback for other slices.
A slice boundary must not split a deduplication key group.
Time boundaries can partition groups because event timestamp is part of the immutable key.
File boundaries alone do not prove that property.

Required integration tests include the partial-coverage counterexample, out-of-order versions, equal-version winner rules, deletes, partial index rebuilds, and concurrent snapshot publication.
Also test restarts, repeated DVs, compaction with changed ordinals, and events sharing an ID across services.

### Change 3: native histogram execution

Add a histogram search API alongside `search_detailed` in `src/tantivy/search.rs`.
Use Tantivy's aggregation collector over `_timestamp`, with exact membership and timestamp bounds.
The implementation uses a fixed-width integer collector through Tantivy's collector API.
It reads the existing timestamp fast field and avoids floating-point bucket boundaries.
The local benchmark above measured Tantivy's built-in histogram, so benchmark the integer collector separately before rollout.
Apply the visible-document mask before counting.
Merge per-index bucket results through the distributed aggregation interface or an equivalent exact integer-count merge.
Do not use `TopDocs`, fetch stored event IDs, or generate SQL `IN` lists for this path.

Recognize only supported query shapes in the TimeFusion aggregate planner:
project equality, supported tag predicates, a bounded event-time interval, and `count(*)` by a fixed-width timestamp bucket.
Support overlapping OR predicates as a document union, so each event contributes once.
Preserve bucket origin, timestamp units, boundary inclusivity, and the SQL output schema.
Use exact timestamp filtering for partial buckets rather than rounded hourly bounds.

For unsupported filters or missing visibility proof, retain the canonical query path.
For mixed covered and uncovered slices, combine counts only when both slices use the same query snapshot and disjoint event groups.
Expose the fallback reason and coverage in query diagnostics.

The issue-volume chart uses this route without an application-maintained count table.
A total-event comparison series needs its own count over the same visible event population.
Never derive total events by adding all hash counts.

### Change 4: measured backfill and rollout

Build new indexes first for the linked project and one busy project, within the existing maintenance budget.
Measure source bytes, index bytes, rows per second, resident memory, and coverage by age.
Do not extrapolate rebuild duration from the synthetic 19-second build or the earlier historical rebuild-rate estimate.

Run shadow comparisons against canonical raw results on stable snapshots.
Include rare errors, common endpoints, log patterns, absent hashes, and overlapping issue hashes.
Cover 3, 7, and 30 days, with hourly and non-hourly buckets.
Measure first-read, warm-read, and concurrent-ingestion latency through the actual chart API.
Measure cold sidecar download and manifest fan-out separately.

Proposed release target: p95 below one second and p99 below two seconds, with exact bucket parity and freshness matching the canonical query snapshot.
These are proposed targets, not previously agreed service guarantees.
Also require no material regression in ingestion or maintenance progress under the agreed workload.

Do not call the feature complete if only old certified dates are fast while the recent tail still forces a slow full-window scan.
If winner-mask construction or sidecar fan-out dominates, fix those costs before changing the chart to approximate counts.
If the complete indexed path still misses the target for common hashes, add snapshot-derived aggregates for those shapes as the next measured step.

## Reproduction, evidence, and limits

Implementation checkpoint:

- Exact element mode, manifest capabilities, integer histogram collection, and immutable index generations are implemented.
- Replaced generations remain tracked for garbage collection after a one-day grace period.
- The existing ID prefilter declines mutable predicates until global visibility is available.
- The winner-mask resolver uses canonical `DedupExec` over narrow key, version, and tombstone columns.
  It retains physical ordinals across deletion-vector filtering and distinguishes the complete event key.
- The file reader reconstructs partition columns from captured Delta file metadata and enforces a decoded-memory limit.
  It uses the same pinned kernel as Delta to read deletion vectors.
  Metadata capture uses the public logical-file API and rejects missing partition constants.
- The sidecar histogram API accepts a pinned manifest entry and a physical winner mask.
  It checks one-file coverage, ordinal validity, representation, and row counts before returning buckets.
- Manifest selection preserves captured file order and leaves absent coverage explicit.
  It rejects duplicate source paths and overlapping valid ordinal entries, and ignores obsolete flush-order entries.
  The selector and histogram reader resolve absolute manifest URIs against the captured table root.
  Store identity and path-component boundaries prevent sibling tables or buckets from claiming coverage.
  Absolute URI paths are decoded once; relative object paths retain literal percent sequences.
- Memory merge snapshots now capture batches and Delta exclusions under each bucket's batch lock.
  The logical-count path uses this capture instead of reading rows and exclusions separately.
  Empty buckets retain delete exclusions, and later mutations cannot change captured batches or ranges.
  This captures each bucket consistently; it does not establish a transaction across buckets and Delta.
  Eighteen logical-count and snapshot tests pass, including deletion during an in-flight flush.
- The combined resolver applies captured memory exclusions to Delta before version selection.
  It preserves deletion-vector exclusions and physical ordinals, and retains captured batches for fallback.
  Timestamp ranges cannot partition a table unless timestamp belongs to its immutable key.
- Captured-row histogram counting uses DataFusion array membership and the pinned winner masks.
  The snapshot histogram service combines indexed files, uncovered files, and memory with checked bucket sums.
  Failed sidecar reads fall back to captured rows and retain their errors for query diagnostics.
  The database entry point captures memory before Delta and pins project/date file metadata and the manifest.
  Execution reads narrow columns and feeds daily captured rows into the combined histogram service.
  Capture follows the existing per-bucket read contract and still needs concurrent-DML validation before production use.
- The database now returns an owned captured histogram before execution.
  Its query bounds, predicate, file metadata, manifest, and memory batches remain bound together.
  Daily rows and winner masks are resolved during execution using those pinned inputs.
  A retained-view test preserves counts across a flush and later DELETE; a fresh capture observes that DELETE.
  This proves stability after capture, not atomicity while capturing a multi-step DML operation.
- SQL DML now holds an activity guard from before memory mutation through completion.
  Histogram capture checks the activity generation before memory capture and after pinning Delta files.
  Active or overlapping SQL DML declines indexed capture; cancellation releases the guard and invalidates earlier stamps.
  Deferred coalescer groups retain shared activity guards through time-window splitting, cross-project folding, and retries.
  A lifecycle test enqueues two projects, folds and splits their groups, requeues them, and checks that all guards survive until final release.
  Quarantine recovery holds guards for every affected project across the complete replay.
  Pending updates therefore decline indexed capture even after the initiating SQL statement returns.
  These guards do not make multi-commit replay transactional. Forced concurrent capture tests remain outstanding.
- Delta histogram sources now have a process-local cache keyed by exact root URL, ordered file metadata (including deletion vectors and partition values), projected schema, keys, and tiebreak.
  Cached liveness retains winning tombstones; each query independently applies current memory authority and resolves its memory overlay.
  A 36-case comparison checks cached versus full resolution across versions, tombstones, and memory authority ranges.
  Resident decoded arrays and masks are limited to 64 MiB and 32 entries, with memory-pool reservations retained by captured queries after eviction.
  Cache admission evicts unpinned resident ownership when the pool rejects a reservation.
  These limits exclude metadata overhead; concurrent cold builders are not yet coalesced.
  The database test now checks that inserting a memory row reuses unchanged Delta data and updates the count.
  Cache hits and histogram executions are exposed in `timefusion_stats`.
  Execution now resolves and counts one UTC date at a time under the decoded-source budget, then combines buckets with checked addition.
  File timestamps must agree with the captured partition date. Memory is projected before daily filtering.
  The complete file set and memory view are captured once; later dates never refresh their snapshot.
  A three-day test exceeds the combined decoded budget, fits each daily budget, combines 17-hour buckets across midnight, rejects an undersized budget, and preserves counts after a later commit.
  The SQL default remains 64 MiB per day. A single busy day can still exceed it; large-partition visibility and measured long-range performance remain outstanding.
- An index-only daily route now uses exact logical-count evidence for the captured file and deletion-vector set.
  With no memory rows or authority exclusions in that day, equality between logical count and DV-adjusted physical count proves that all live rows are unique and non-deleted.
  The route requires complete usable index coverage and timestamp bounds inside the partition, then counts with pinned DV masks without decoding event keys or hashes.
  A real Delta/Tantivy test declines coverage without proof, counts under a 64-byte bitmap budget after proof, rejects added versions and same-path DV changes, and accepts a refreshed proof after the superseded physical row is deleted.
  The original captured view retains its pre-DV count.
  Existing path-only dedup certificates do not supply this proof. The logical-count cache's limited resident history still constrains availability; durable compact proofs and production seeding remain outstanding.
- Initial SQL routing recognizes `count(*)` by `time_bucket` with bounded timestamps, project equality, and exact `array_has` predicates.
  String and SQL INTERVAL widths share the existing UDF parser. Aggregate replacement preserves parent projections and ordering.
  Only schema-declared element fields qualify; the `mor_versioned` fixture now has such a hashes field, while production schemas remain unchanged.
  Native containment and overlap route through `array_has_all` and `array_has_any` for constant, nonempty, non-null string lists.
  Empty and null-containing array literals remain on the ordinary path. SQL tests check routing and counts for these cases.
  The chart's `jsonb_path_exists(to_jsonb(hashes), '$[*] ? (@ == "value")')` form now maps to exact membership.
  Its whole path must parse with the existing UDF parser; other JSONPath forms remain on the ordinary path.
  Literal Unicode and supported string escapes are preserved. Unicode escape sequences remain rejected by the existing parser.
  Additional optimized query shapes still need matching and parity checks.
- Backfill and its coverage census now treat obsolete element representations and invalid physical ordinals as uncovered.
- One hundred seventy selected Tantivy, DML, visibility, logical-count, memory snapshot, and versioned-table tests pass.
  Command: `SSL_CERT_FILE=/etc/ssl/cert.pem TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 cargo nextest run --no-default-features -E 'test(tantivy) | test(delta_cache) | test(histogram_capture_detects) | test(dml::tests) | test(decide_prefilter_tests) | test(element_index_configuration) | test(merge_snapshot_preserves) | test(logical_count) | test(mor_) | test(versioned) | test(version_append)'`.
  Run ID: `d125d2f9-60f8-4183-93ca-d817c2de3f95` (9.182 seconds), before integrating upstream positional scan ordering fix `ed6e56b4`. `cargo lint`, formatting, and diff checks passed on this base; the combined tree will be checked again before publication.
  JSONPath tests verify chart-form routing, ordinary execution for inequality, escaped strings, and agreement with the existing evaluator.
  SQL tests verify service execution for string/INTERVAL widths and OR membership, and verify ordinary planning for an unsupported filter.
  The database integration test counts competing Delta versions plus memory and verifies decoded-budget rejection.
  Histogram parity covers indexed, uncovered, and missing-blob sources combined with memory, plus exact element fallback for nulls and duplicates.
  The committed-DV test reads original captured file metadata after two later deletes and retains the original visibility.
- The stale-version integration test fails with the old decision and passes with the guard restored.
- `make ci-signoff CHECKS="fmt clippy"` could not start Docker and published no attestations.
  Storage integration tests used native MinIO on localhost instead.
- Concurrent snapshot validation, mask caching, SQL histogram routing, measured backfill, and production validation remain incomplete.
  Publication failures can also leave unreferenced generation blobs; general orphan collection remains to be addressed.

The benchmark scripts create local synthetic data and preserve results in JSON:

```sh
python3 bench/hash_index_research.py --rows 10000000 --dsn "$LOCAL_HASH_BENCH_DSN" --output postgres-10m.json
python3 bench/hash_bitmap_research.py --rows 10000000 --output bitmap-10m.json
python3 bench/hash_tantivy_research.py --rows 10000000 --output tantivy-10m.json
python3 bench/hash_tantivy_research.py --semantics-only --output tantivy-visibility.json
```

Repeat the first three commands with `--rows 1000000` for the smaller dataset.
The PostgreSQL script creates a unique schema and refuses non-local hosts.
The local server used PostgreSQL 16 through a private Unix socket.
The scripts used `psycopg`, `pyroaring`, and Python binding `tantivy==0.22.2`, which reports engine `tantivy v0.22.0`.
TimeFusion pins the 0.22 release family, but these are library-level experiments rather than a build of TimeFusion itself.
Python 3.13 could not build that older binding. Python 3.11 installed and ran it successfully.

[The evidence directory](evidence/2026-09-08-hashes/) contains all timing samples, PostgreSQL plans, sizes, bucket checks, production queries, and correctness counterexamples.
The scripts do not preserve raw production event payloads.
The production-known-ID evidence stores only the selected event identity and aggregate results.

Validation: both PostgreSQL scales completed, both bitmap scales completed, both Tantivy scales completed, and the native visibility checks passed.
Python syntax checks, JSON parsing, local links, and whitespace checks also passed.
The research phase did not change engine code, schema, or production configuration.
The implementation checkpoint above records the subsequent local engine changes and targeted Rust checks.
No PR, push, production change, or CI attestation has been published.
Local `make ci-signoff` remains required before a push; its Docker prerequisite is currently unavailable.

The remaining uncertainty is integration cost, not whether an index can compute these charts.
The native-library tests demonstrate the computation. The snapshot and coverage gates define what TimeFusion must prove before deployment.
