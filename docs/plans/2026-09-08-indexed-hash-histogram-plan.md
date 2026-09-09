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
| ClickHouse | Scan relevant columns or use array-capable text-index postings, then aggregate | Replacing tables need correct version resolution | Current text indexes offer a closer comparison than block skipping alone; exact tokenization and query eligibility matter |
| Timescale continuous aggregates | Read stored bucket summaries | Refresh policies reconcile changed buckets | Very cheap repeated charts, but resolution, dimensions, retention, and refresh coverage constrain queries |
| Proposed TimeFusion integration | Filter exact tags and count indexed timestamps through a visibility gate | Delta/memory snapshot determines which index documents count | Reuses Tantivy, but requires integration work for masks, coverage, and sidecar fan-out |

The benchmarks establish potential for the proposed computation, not superiority over a deployed Elasticsearch, Pinot, or ClickHouse cluster.
Those databases were not benchmarked here.
The PostgreSQL comparison also used different storage and execution settings from Tantivy.
Its strongest conclusion is that avoiding payload retrieval can matter substantially for this chart shape.

ClickHouse's current [text-index documentation](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/textindexes)
also describes string-array support and posting lists stored as Roaring bitmaps.
The earlier block-skipping comparison did not cover this mechanism.
We have not benchmarked that path against TimeFusion.

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
  Existing path-only dedup certificates do not supply this proof.
- Fresh logical-count builds now publish compact daily proofs to the Tantivy manifest for tables with element indexes.
  Each proof binds the table root, complete file/DV set, key order, tiebreak, tombstone column, and projected visibility schema.
  Queries can recover the count after winner-cache eviction or database restart. They still require complete usable ordinal indexes and physical/logical count equality.
  The manifest retains at most 32 daily proofs per project/table, covering the 31 UTC partitions a thirty-day window can intersect.
  A real integration test checks cache eviction, manifest reload, a cold database and index-reader restart, stale file/DV rejection, and retained old captures.
  A persistence test checks identity changes, serialization, and the retention limit.
  The two focused tests passed in 1.539 seconds (`9a9c16f1-438c-4ec5-972f-545eb93ec358`).
  All 135 selected Tantivy, proof, logical-count, cache, capture, and positional tests passed in 9.095 seconds (`fa247551-9636-4a81-8290-fdad3942221c`).
  Command: `SSL_CERT_FILE=/etc/ssl/cert.pem TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 cargo nextest run --locked --no-default-features -E 'test(tantivy) | test(count_proofs_bind) | test(delta_cache) | test(histogram_capture_detects) | test(logical_count) | test(positional)'`.
  Production seeding and large-partition build limits remain outstanding. Loading an older Arrow cache alone does not publish a new proof.
  The proof currently publishes only after a successful fresh count build and cache install; publication failure retains ordinary fallback behavior.
  `make ci-signoff CHECKS="fmt clippy test e2e"` passed and attested all four checks for `7609cc46`, including all 8 doctests.
  The final gate leaves only canonical pgwire smoke for GitHub because of the local Docker host-network limitation.
  The preceding revision's complete GitHub CI run also passed.
- SQL histogram capture now requests one missing proof for a completed UTC day in the background.
  One admission slot covers both queued and executing query-triggered work. Busy requests do not add a queue.
  Proof builds share the count-build semaphore and use the maintenance memory pool, spill-disk limit, and host memory brake.
  A bounded history of 256 attempts defers retries for 60 seconds while each entry remains resident, allowing other requested days to advance.
  Tests verify busy-slot rejection, duplicate admission rejection, completion, retry delay, and persisted proof recovery.
  All 133 selected Tantivy, proof, and logical-count tests passed in 8.854 seconds (`a414da47-0837-40bb-a2de-7481ea24214c`).
  The change was tested in an isolated checkout while the preceding revision completed CI.
- Query-triggered proof construction now sorts only complete keys and the optional tombstone column, then checks strict key ordering across batches.
  A duplicate key or physical tombstone declines publication. Unexpected key ordering is an error.
  A unique, tombstone-free partition's physical count is also its exact logical count, so no winner cache is needed.
  The checker retains one encoded-key batch and the preceding batch's final key, charged to the maintenance memory pool.
  The external sort uses the existing maintenance spill runtime. This removes the complete winner-index admission limit from proof seeding.
  The builder checks that its exact file/DV set remains current before publishing.
  Tests verify publication with the winner cache disabled, duplicate and tombstone rejection, and acceptance after a real deletion-vector update.
  The first focused test passed in 1.681 seconds (`e457fe53-ca82-4f38-9d49-dfc7b8fcb590`).
  All 135 selected regression tests then passed in 8.843 seconds (`41e92162-dbc5-4c3d-9f1a-581ecc9eeb84`).
  An added multi-batch case checks unique keys over several batches and a duplicate at a sorted batch boundary.
  That case exposed a fixture truncation: `json_to_batch_for` returned only the first 1,024 supplied records.
  The helper now sizes its reader batch to include all supplied records. The corrected case inserts 4,097 rows under the default maintenance batch size.
  The corrected 135-test run passed in 12.262 seconds (`a35fded1-d92e-4e83-9cae-0fa6aefe9120`), with one nextest leak notification.
  A repeat with full final-status output is running to identify any recurring leak.
  Full local signoff (`fmt clippy test e2e`) is running because the shared helper change also affects other fixtures.
  Large-file spill performance and dirty/hot-day histogram execution remain unverified. The ordinary captured-row path still has its 64 MiB daily decoded limit.
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
- One hundred eighty-six selected Tantivy, DML, visibility, logical-count, memory snapshot, and versioned-table tests pass.
  Command: `SSL_CERT_FILE=/etc/ssl/cert.pem TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000 cargo nextest run --no-default-features -E 'test(tantivy) | test(delta_cache) | test(histogram_capture_detects) | test(dml::tests) | test(decide_prefilter_tests) | test(element_index_configuration) | test(merge_snapshot_preserves) | test(logical_count) | test(mor_) | test(versioned) | test(version_append)'`.
  Run ID: `27487bb1-68ad-4a68-b1ec-2cc8c2c492cd` (9.501 seconds), after integrating upstream positional scan ordering fix `ed6e56b4`. The command also includes `test(dv_scan) | test(footer) | test(positional)`. The full default-feature test suite and all 8 doctests passed and were attested.
  JSONPath tests verify chart-form routing, ordinary execution for inequality, escaped strings, and agreement with the existing evaluator.
  SQL tests verify service execution for string/INTERVAL widths and OR membership, and verify ordinary planning for an unsupported filter.
  The database integration test counts competing Delta versions plus memory and verifies decoded-budget rejection.
  Histogram parity covers indexed, uncovered, and missing-blob sources combined with memory, plus exact element fallback for nulls and duplicates.
  The committed-DV test reads original captured file metadata after two later deletes and retains the original visibility.
- The stale-version integration test fails with the old decision and passes with the guard restored.
- Earlier signoff attempts could not start Docker; those attempts published no attestations.
  Docker is now available. The rebased tests use CI’s pinned MinIO release `2025-04-22T22-12-26Z`; the temporary native service was stopped.
  `make ci-signoff CHECKS="fmt clippy"` passed and attested both checks.
  `make ci-signoff CHECKS="test pg-smoke e2e"` attested the full test check.
  The canonical pgwire smoke check cannot reach the native server through Docker's `--network host` on this laptop.
  A repeat with `CI_KEEP_GOING=true make ci-signoff CHECKS="pg-smoke e2e"` confirmed this limitation and started e2e.
  The server listened on port 12345. The same PostgreSQL 18.4 container returned `SELECT 1` through `host.docker.internal`.
  All five catalog commands (`\dt`, `\d otel_logs_and_spans`, `\l`, `\du`, `\dn`) passed through that address with `ON_ERROR_STOP=1`.
  This alternate-address check has no attestation; GitHub must run canonical pgwire smoke.
  E2e subsequently passed and published an attestation for the source in `a426b1e9`, before the persisted-proof change.
- A read-only repeat of the recorded seven-day known-ID production chart timed out after 10.043 seconds.
  [Recorded result](evidence/2026-09-08-hashes/production-known-candidate-recheck.json), taken at `2026-09-08T21:05:56Z`.
  This branch was not deployed, and the probe did not establish the server revision.
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

### Continued execution mandate — 2026-09-08 22:10 UTC

The user requests a goal audit every 30 minutes during active work. Next audit:
2026-09-08 22:40 UTC. Each audit must check progress toward fast hash-column
queries, measured end to end, including the busy current day. Passing isolated
collector tests does not establish that outcome.

Before deployment, run rs-distill and rs-evasion-review at least twice, apply
findings, and repeat affected checks. Review the full feature diff in bounded
sections; a clean section does not establish a clean full branch. The user has
authorized implementation of review suggestions without further confirmation.

After deployment, verify hash correctness and latency in production and fix
observed issues. Then collect CPU and memory profiles for popular queries on
different columns and time ranges. Use those measurements to select further
optimizations, deploy, and validate continuously until the user asks to stop.
This profiling phase follows production verification of the hash work.

The full-status rerun of the 135 selected tests passed in 10.038 seconds with
no leak notification (`/tmp/timefusion-stream-proof-leak-check.log`). The earlier
notification did not recur; its source remains unidentified. Full local CI for
`bdb49995` is still running.

Validation checkpoint — 2026-09-08 22:16 UTC: `make ci-signoff
CHECKS="fmt clippy test e2e"` completed successfully for the `bdb49995`
source. All four checks were attested. Final status leaves only canonical
`pg-smoke` to GitHub because of the local Docker host-network limitation.
The direct-provider review cleanup is isolated at `5445827b`; its focused
regression is still running and is not covered by these attestations.

The direct-provider cleanup passed its focused integration test (nextest run
`14dc8b09-ce49-4654-9ab7-2fbdaf4a1ebe`) and was integrated as `d05f161a`.
The preceding full-CI source and validation are pushed through `212f2466`.

Busy-day implementation now extracts a captured Parquet stream from
`read_file_rows`. The collecting API retains its existing decoded budget.
The stream preserves all physical rows, reconstructs captured partition
constants, checks object size and required columns, and returns its pinned DV
mask separately. Both end-of-stream and excess-row checks protect ordinal
lineage. This extraction is a foundation, not completion of busy-day execution.

The next consumer must stream complete keys plus version and physical lineage
through ordered canonical deduplication, accumulate only winner masks, and
count indexed timestamps. Preserve first-source/ordinal ties explicitly when
sorting. Missing index sources must stream membership columns after winner
resolution. Charge masks and batch/sort working memory; do not silently bypass
the existing daily decoded-budget contract. A sorted key stream should use the
query runtime's spill limits. No full-day hash-array collection is required by
that design. This consumer and its large-day parity test are not yet implemented.

Stream extraction `1edf7519`: all 135 selected tests passed in 9.099 seconds
(nextest `6ccf21b2-704d-4261-8a00-759a9153c9f1`). Command:
`SSL_CERT_FILE=/etc/ssl/cert.pem TIMEFUSION_TEST_S3_ENDPOINT=http://127.0.0.1:9000
cargo nextest run --locked --no-default-features -E 'test(tantivy) |
test(count_proofs_bind) | test(logical_count) | test(delta_cache) |
test(histogram_capture_detects)'`. `make ci-signoff CHECKS="fmt clippy"`
passed formatting and is running lint. No push of this extraction yet.
The completed temporary review and proof-seeding worktrees were removed; their
changes are integrated on the feature branch. Next 30-minute audit remains
2026-09-08 22:40 UTC.

The extraction's `make ci-signoff CHECKS="fmt clippy"` completed successfully;
both checks were attested. Final status leaves full test, pg-smoke, and e2e
for GitHub. The next local change sorts complete keys and lineage before
canonical deduplication and consumes its winner output incrementally. Review
identified the canonical 64 MiB timestamp-run early-emission case; a focused
large-run regression is running before the version-order fix.

Streaming winner masks: the first 118 selected tests passed in 9.637 seconds
(nextest `9fc081cb-27aa-414a-90a9-6a5c2bcdef12`). A new real-size regression
then crossed the canonical 64 MiB timestamp-run ceiling and failed with 8,192
old winners instead of 8,191. The implementation now sorts complete keys,
version descending/nulls last, then physical source/ordinal. This keeps early
run emission exact and preserves equal-version source priority. The expanded
regressions are running. The source-plan consumer uses canonical deduplication
and `execute_stream`; it charges mask buffers during execution.

The collecting source adapter now uses that consumer. Wiring captured Parquet
streams into it without retaining all decoded source batches remains pending.
Returning masks also requires caller ownership accounting; an execution-time
reservation alone is not a lifetime-wide memory guarantee.

The version-order fix passed all 136 selected tests in 11.479 seconds
(nextest `ae1fc38e-fce0-4379-9946-e94153b4fd94`), including the previously
failing 64 MiB run regression. `make ci-signoff CHECKS="fmt clippy"` is
running against this source; no new-source attestation is inferred from the
preceding extraction. Full-feature reviews remain open.

For the next source adapter, Parquet's installed async reader exposes
`ArrowReaderMetadata` and `ParquetRecordBatchStreamBuilder::new_with_metadata`.
Inspect the pinned version and use immutable prepared metadata to produce
reusable physical streams and exact source lengths, rather than a one-shot
stream hidden behind a mutex. Metadata and DV masks need reservation ownership.

`76a958f7` passed `make ci-signoff CHECKS="fmt clippy"`; both checks were
attested. Final status leaves full test, pg-smoke, and e2e to GitHub. The
current selected regression set passed all 136 tests as recorded above.

Prepared Parquet sources now retain immutable `ArrowReaderMetadata` and the
pinned DV mask, creating fresh readers with `new_with_metadata` for each scan.
The collecting and streaming entry points share the same decoder. Extended
the real Delta DV test with repeated prepared scans, alternate projections,
and old-mask preservation after later commits. All 136 selected tests passed
in 9.317 seconds (`be958064-0dd1-4562-9701-53c98848a3d8`). Final fmt/clippy
signoff is running.

Next integration: construct reusable physical source partitions from prepared
files, attach source/ordinal before eligibility filtering, and feed them to
`stream_winner_masks`. Retain prepared sources for missing-index fallback that
streams membership columns after winner resolution. Add an explicit streaming
count entry point with a working-memory budget; preserve the existing public
collecting API's total-decoded-budget contract. Test a dirty indexed day whose
hash arrays exceed that old daily limit, with parity against the same capture.
The metadata and DV owner must keep its reservation alive through source
execution and fallback. This is the next implementation step, not another
standalone reader refactor.

### 30-minute goal audit — 2026-09-08 22:40 UTC

Verified current source, rather than treating passing component tests as the
performance outcome. SQL still calls `capture_histogram(..., 64 MiB, ...)`
and `captured.count()`. The non-unique daily path still calls collecting
`read_file_rows`. Production `hashes` Elements remains disabled. No deployment
or production speed improvement is established.

Progress since the preceding audit: full streamed-proof CI passed; captured
reader and sorted winner-mask changes are pushed. The new large-run regression
exposed and fixed version loss at the canonical 64 MiB run ceiling. Prepared
repeatable readers now pass all 136 selected regressions and final fmt/clippy
signoff (`113a4d91`). Scoped Rust reviews found and addressed issues; full
feature reviews remain open.

Immediate priority: connect prepared sources to winner masks and indexed
counting, with streamed missing-index fallback and explicit working-memory
ownership. Verify a dirty day above the old decoded limit, then benchmark
complete 3/7/30-day queries before production activation. Component cleanup
alone does not meet the goal. CPU/memory profiling of other popular columns
follows production hash validation. Next goal audit: 2026-09-08 23:10 UTC.

For `113a4d91`, `make ci-signoff CHECKS="fmt clippy"` passed and attested both
checks. Final status leaves full test, pg-smoke, and e2e to GitHub.


### Goal audit — 2026-09-09 05:41 UTC

SQL histogram execution now calls `count_streaming`. Prepared Parquet sources
stream visibility columns into sorted winner masks. Missing-index fallback
reads membership columns from the same capture. Files execute sequentially;
metadata, deletion vectors, masks, and batches retain query-pool reservations.
The collecting API preserves its total decoded-data limit. Streaming uses a
per-batch limit; this is not a whole-process RSS guarantee.

The new dirty-day fixture contains more than 64 MiB of hash arrays. It checks
indexed counting, an unindexed replacement, and old-capture stability. The
initial integration passed 136 selected regressions. A later 8 MiB query-pool
trial failed because DataFusion reserves 10 MiB for external-sort merging.
The final test uses 32 MiB, still below the fixture size, and checks reservation
release. Full local signoff is running; no passing result is claimed yet.

The user explicitly requested committing and pushing to master. This advances
integration deployment; production `hashes` Elements activation remains off.
Warm/cold/ingest benchmarks, full-feature reviews, activation, production
validation, and subsequent CPU/memory profiling remain open. No audits were
recorded between the prior checkpoint and this audit. Next audit: 06:11 UTC.


Final integration validation: `make ci-signoff CHECKS="fmt clippy test e2e"`
exited 0. Formatting, Clippy, the full nextest suite, all eight doctests, and
end-to-end tests passed. All four requested checks were attested for the final
source, including the 32 MiB streaming regression. Final gate status requires
only canonical `pg-smoke` on GitHub due to the documented macOS networking
limitation. No failed check was attested.


### Goal audit — 2026-09-09 06:19 UTC

Integration is merged to master at `97235f16`. Local fmt, Clippy, full tests,
doctests, and e2e passed. GitHub reused those attestations and passed canonical
PostgreSQL smoke. Build/deploy run `34317126834` is still building the image.
The live service remains on `ed6e56b`; production deployment is not yet proven.

The user rejected manual hash-index activation and reiterated local signoff.
The production schema now declares exact hash elements by default, without a
new environment variable or runtime switch. Existing coverage checks reject
legacy manifests that lack these elements, so maintenance backfills them.
Correct captured-row fallback remains available during incomplete coverage.
Full local signoff for this schema change is running. This instruction
supersedes the earlier plan to leave activation off until a separate rollout.
Reviews, complete-path benchmarks, production validation, and subsequent
CPU/memory profiles remain required. Next goal audit: 06:49 UTC.


Local validation follow-up: the first full run failed, but a pre-existing TCP
probe redirected subsequent stderr to `/dev/null`. A before/after probe
against local MinIO reproduced the loss and verified the fix. Keep the socket
open/close inside the subshell so caller stderr remains intact.

The visible rerun executed 1,474 tests: 1,457 passed and 17 search-service tests
failed because their shared fixture omitted `hashes`. The index builder's
missing-element-column guard remains unchanged. The fixture now includes a
nullable string-list column, matching the newly indexed production schema.
Targeted search-service tests and canonical local pg-smoke are running.
No failed check was attested as passing; fmt and Clippy passed independently.


The corrected shared fixture passed all 20 search-service tests in 3.357s
(nextest `e2d0160e-0117-4a39-94d8-7f1d41aedd91`). Canonical `make ci-signoff
CHECKS="pg-smoke"` passed on macOS and published its attestation, using the
same SELECT 1 and five PostgreSQL 18 catalog assertions as Linux. No remote
smoke fallback is needed for the current source. Full signoff is still running.

Production service inspection now reports image `97235f1` with 1/1 replicas.
A read-only pgwire `SELECT 1` returned 1 in 27.18ms after that observation.
This confirms basic connectivity for the streaming integration deployment;
it does not prove indexed histogram latency. The always-on production schema
change is still local pending final signoff.


### 30-minute goal audit — 2026-09-09 06:49 UTC

The streaming integration deployed successfully at `97235f1`; service state,
pgwire connectivity, and the deployment readiness soak passed. Production
hash indexing still awaits the local schema change. That change requires no
user switch and automatically invalidates legacy element coverage.

All 1,474 main tests and eight doctests passed. Canonical PostgreSQL smoke
passed locally after fixing Docker Desktop networking. All 63 e2e tests are
running. The first failed suite exposed a fixture that lacked the now-indexed
hashes column; its correction passed all 20 search-service tests. The runner's
stderr redirection bug was also reproduced and fixed without suppressing any
failure or weakening the index builder's missing-column guard.

Master advanced to `90d314f4` with a separate Rust refactor and attestation GC
workflow. Incorporate those changes, then validate the combined source locally
before pushing. Production histogram correctness and latency, complete-path
benchmarks, remaining full-feature reviews, and subsequent CPU/memory profiles
remain open. Next goal audit: 07:19 UTC.


Before rebasing onto the concurrent master refactor, full local validation
passed: 1,474 tests, eight doctests, 63 e2e tests, fmt, Clippy, and canonical
PostgreSQL smoke. Both signoff commands exited 0. The final gate marked all
five checks proven locally, with none left for GitHub. These attestations
cover this source only; combined-source validation follows the rebase.


Final signoff after pulling `74295465`: `make ci-signoff` exited 0 for
`14eaddea`. All 1,474 main tests, eight doctests, 63 e2e tests, fmt, Clippy,
and canonical PostgreSQL smoke passed locally. Every check is attested;
the final gate leaves none for GitHub. Main nextest run:
`d9c584c8-f237-470e-8cb8-aa9849e86b3b` (147.551s). E2E run:
`a11bd581-cfbd-4ebe-872e-c2bce35ed2ae` (212.474s).

The pull retained master's shared host-gateway smoke implementation and our
TCP-probe stderr fix. The existing production baseline is saved in
`evidence/2026-09-08-hashes/production-integration-bounded-probes.json`:
image `7429546`, one matching event in a one-second hash histogram (754.13ms),
confirmed by ID lookup (1246.52ms). These timings precede automatic indexing.
Push, merge, and production verification of the schema change follow.


### 30-minute goal audit — 2026-09-09 07:19 UTC

Automatic production hash-element indexing is merged in PR #231 at
`413f1ef3`. Every required check passed locally, including canonical
PostgreSQL smoke and 63 e2e tests; the remote CI run completed successfully.
Build/deploy run `34323175414` is currently building its image. Production
still serves `7429546`, so automatic indexing is not yet verified live.

Saved pre-deployment counters in
`evidence/2026-09-08-hashes/production-before-automatic-index-stats.json`.
Histogram snapshots are zero. Uncovered and oversized files are both zero
under the old schema; that does not prove hash-element coverage. After the
schema deploy, verify new coverage convergence and histogram routing, then
repeat the saved bounded correctness/latency probe before widening ranges.
The remaining reviews, complete-path benchmarks, and CPU/memory profiling
remain open. Next audit: 07:49 UTC.


### First live automatic-index validation — 2026-09-09 07:42 UTC

Production now serves `413f1ef`. The saved one-second histogram probe timed
out at 3113.41ms; its ID control returned the expected event in 1091.47ms.
Two repeats returned the correct count in 367.20ms, then timed out at
3119.62ms. Histogram execution counters remained zero. Coverage reported
2,391 uncovered files, zero oversized skips, and zero backfill builds.

Logs show the first request triggered a uniqueness proof for 2026-09-02,
which completed in about five seconds with 3,031,107 rows. The source also
resolves daily visibility before fallback. Investigate that work for narrow
queries without usable element indexes; do not treat a successful repeat
or the running service as production performance success. The probe JSON
files record the exact SQL and results. Count-merging cleanup PR #232 is
merged at `21a458c5` after complete local signoff.


### Goal audit — 2026-09-09 07:49 UTC

The previous turn verified the requested remote pull; it made no feature
change. This turn reproduced unnecessary histogram execution locally: a
one-microsecond SQL query against an unindexed Delta file returned the
correct count but incremented the histogram execution counter. The new
regression assertion failed with 1 versus the expected 0. The planner now
checks captured physical-ordinal element coverage before executing the
histogram or seeding a proof. Targeted verification is running. The same
test subsequently exercises partial indexed coverage and newer versions.

Deployment run 34323175414 completed successfully. Production logs show
automatic backfill started at 07:45:11 with 52 files (1,959 MiB) selected,
and index builds completed at 07:45:35. A follow-up statistics connection
timed out; this does not establish a service failure or coverage convergence.
The narrow-query fix, repeated reviews, local signoff, deployment validation,
full-range benchmarks, and CPU/memory profiles remain open. Next audit:
08:19 UTC.


### Local no-coverage regression — 2026-09-09 07:55 UTC

The regression passed in 3.015s after adding automatic SQL admission and
correcting the integration fixture. The fixture now checks three states:
no index, a flush index without physical ordinals, and a Parquet-backed
index for only the newer file. Existing version, memory, and membership
assertions still pass. The first two states use ordinary SQL; the third
uses the histogram service automatically. Full `make ci-signoff` is running.

The next performance investigation must retain the original scope: partial
coverage still sorts daily visibility. Timestamp belongs to the immutable
key, so rows outside the requested timestamp interval cannot compete with
rows inside it. Test narrowing visibility before the sort while preserving
physical ordinals, DV exclusions, tombstones, and memory authority. Do not
claim that the no-coverage admission fix resolves partial-coverage latency.


### Production routing comparison — 2026-09-09 07:58 UTC

The production task changed at 07:54:49 without changing image `413f1ef`.
The old task exited 0; startup reported a clean cursor snapshot. The old
container has already been removed, so the restart cause is unverified.
The statistics query now succeeds in 265ms: 2,380 uncovered files, zero
oversized skips, four completed histogram snapshots, and no unique-partition
counts. The restart reset process counters, so they cannot establish build
throughput across the restart.

On the same connection, ordinary SQL using `count(timestamp)` returned
the known count 1 in 314.46ms. The otherwise identical `count(*)` histogram
query timed out in 3140.29ms. Both include explicit timestamp bounds, so
null timestamps cannot change the count. This strengthens the evidence
that histogram routing adds work for this narrow query. Exact SQL and
results are saved in `production-histogram-ordinary-comparison.json`.


### Combined narrow-query fixes — 2026-09-09 08:15 UTC

No-coverage admission passed complete local signoff and was pushed as
`7a112910` in PR #234. Before merge, master advanced to `a56f0be8` with
a substantial read-path refactor. That upstream change was merged locally.

A separate checkout reproduced the remaining daily-sort problem: 20,000
out-of-window keys exhausted the 32 MiB query pool with spill disabled.
Filtering the captured visibility input before the sort made the same test
pass, including the expected count 63 and zero retained reservations. This
filter preserves physical lineage and relies on timestamp being an immutable
key. The two fixes are now combined for a fresh local signoff and one release.
Production serves `21a458c`; these fixes are not deployed yet.


### Goal audit — 2026-09-09 08:19 UTC

The previous goal turn made concrete progress: admission was pushed in PR
#234 after full local signoff, and a resource regression proved the need to
filter narrow windows before sorting. The filter passed that regression.
Both fixes and upstream `a56f0be8` are now under a fresh local signoff.
Formatting and Clippy have passed; the full test build is running. No
production performance claim is justified yet.

The next release must pass the combined local checks, merge, deploy, and
repeat the saved ordinary-SQL/histogram comparison. Coverage convergence,
partial-coverage latency, full SQL 3/7/30-day benchmarks, repeated broader
reviews, and CPU/memory profiles remain open. Benchmark preparation now
targets real SQL planning and Delta visibility, with independent expected
bucket counts. Next audit: 08:49 UTC.


### Deployment and further validation — 2026-09-09 08:48 UTC

PR #234 merged to master at `e44e08cd` after complete combined local
signoff. Deployment run `34329798739` is building its image. Production
currently serves `a56f0be`; the saved immediate baseline returned count 1
in 343.51ms through ordinary SQL and timed out in 3115.47ms through the
histogram form.

The global coverage gauge is unreliable before the follow-up fix: an
empty table's backfill overwrites the full census. Production logs and
a failing-then-passing local regression confirm this. The follow-up makes
the census its sole writer and still needs full local signoff.

A real SQL benchmark harness in a separate checkout compiles and validates
buckets against arithmetic expectations. Its first run rejected a 30-day
query that returned correct buckets through ordinary fallback. Diagnose
that route before treating any timing as indexed performance. Wider
benchmarks, production validation, and CPU/memory profiling remain open.


### Goal audit — 2026-09-09 08:53 UTC

This audit was recorded four minutes after its scheduled time.
The narrow-window fixes are merged at `e44e08cd`; deployment run
`34329798739` is confirmed active in image build. The immediate production
baseline still reproduces the timeout. Production validation of the new
image remains pending.

Further work produced two concrete findings. First, an empty-table backfill
overwrote a 2,282-file global census; the regression and sole-census-writer
fix pass locally. Second, the full SQL benchmark rejects 30-day histogram
measurements because the range-parallel optimizer introduces a Union that
the histogram matcher cannot traverse. The saved optimized plan proves
this routing gap. Implement exact handling of disjoint, contiguous branches
with identical source/project/membership; do not disable range splitting
or accept overlapping unions.

The overall goal remains incomplete: wide native routing, production
performance, coverage convergence, full benchmark measurements, broader
reviews, and CPU/memory profiles remain open. Next audit: 09:19 UTC.

### Production validation — 2026-09-09 09:01 UTC

Deployment 34329798739 completed successfully with readiness soak; production
serves e44e08c. The saved one-second query returned count 1 in all three
histogram-form samples: 674.85, 676.03, and 551.39 ms. The ordinary control
returned count 1 in 1357.46 ms. Histogram counters stayed zero: these samples
prove the automatic ordinary fallback removed the observed timeout, not
indexed production speed. Evidence: production-narrow-window-first-validation.json.

The contiguous-Union matcher regression passed in 2.915s, nextest
23c1ef5e-e170-4258-9cfe-316828aaa48f. The separate 30-day SQL benchmark
is being rerun with this matcher and retains strict bucket and routing checks.

The fixed matcher passed the independent full SQL benchmark: 192 samples
across 3/7/30 days, four predicates, ordinary/native routes, complete/partial
coverage, and four repetitions. Every bucket matched arithmetic expectations;
native and uniqueness counters matched each case. Newer unindexed versions
were included in the partial phase. This 30,000-row debug run is correctness
evidence only, especially while local compilation shared the machine. Saved
results: evidence/2026-09-08-hashes/local-histogram-sql-debug-validation.json.

### Fresh-ingest coverage follow-up

The production flush callback builds from pre-Parquet batches and publishes
IndexSource::Flush with untrusted physical ordinals. Single-file manifest
paths do not prove row order. Preserve that guard; investigate building
from committed files or carrying exact writer lineage instead. Both
src/server/mod.rs and src/main.rs wire the callback, and the write layer
already runs it after commit under a bounded semaphore. Avoid a new flag
or a synchronous index build on the Delta commit path. This is open work,
not covered by the current matcher/gauge signoff.

At 09:00 production scheduled 37 otel files (1,950 MiB) within the existing
2,048 MiB backfill budget. The 08:55 census reported 2,296 uncovered files.
Production coverage has not converged. Upstream PR #235 also touches Tantivy
and bootstrap modules; re-fetch before integrating follow-up changes.

### Local signoff failure — 2026-09-09

The first follow-up signoff stopped: 1,482 tests passed, the coverage-gauge
regression failed twice with 0 versus 2, and the batch-queue load test
crashed once before passing its automatic retry. No test attestation was
published; pgwire/e2e were not reached. The suite took 143.424s, nextest
a3903d37-be84-4357-9276-117f69bf2c57. Full failure output is saved beside
the other evidence.

Current source has only the census writer. The benchmark checkout still
has the old per-table writer and shared this target directory. Rebuild the
current database module before deciding whether this is stale artifact reuse
or another writer path. Do not weaken the regression. Separately, BatchQueue
shutdown only cancels its worker and does not await completion; investigate
that lifetime against the Delta executor RecvError/segmentation-fault trace.
The fixes remain unpushed.

The unchanged gauge regression passed in 1.993s after forcing recompilation
of the current database module. This supports stale shared-worktree artifacts
as the cause; a new full signoff is running without competing worktree builds.
A separate shutdown regression now checks that BatchQueue releases its
database worker before shutdown returns. It has not been run yet.

### Goal audit — 2026-09-09 09:19 UTC

The preceding goal turn made progress: the failed gauge check passed after
recompilation, fixes were committed locally as ad925175, and fresh-ingest
and shutdown regressions were prepared in isolated checkouts. The renewed
local signoff is confirmed live (session 17832). It has not passed yet.

Upstream PRs #235 and #236 merged at 3e37227e. Their integration with the
fixes merged cleanly at df2f9655 in the integrated checkout; the combined
tree still needs local signoff. No competing worktree builds may use the
shared target until the current checks finish.

Production e44e08c returned the narrow query correctly through ordinary
fallback. The 09:10 census reported 2,238 uncovered files. Local benchmark
validation passed all 192 cases, including 30-day native routing and partial
coverage. Optimized timings, fresh-ingest physical indices, production
validation of the follow-up, shutdown-crash investigation, and broader CPU
and memory profiles remain open. The full goal is not achieved.
Next audit: 09:49 UTC.

### Combined signoff — 2026-09-09 09:22 UTC

The rebuilt pre-integration tree passed all 1,483 tests in 120.785s and
ten doctests without retries (nextest 7712865a-dc20-4217-b34b-e539c952a082).
The remaining checks were deliberately stopped during the pgwire binary
build because upstream integration changed the deployable tree. No pgwire
or e2e result is claimed for that run.

The main checkout fast-forwarded to 567664f3, containing both hash fixes and
upstream 3e37227e. A full make ci-signoff is now running there, session
86509, log /tmp/timefusion-histogram-integrated-signoff.log. This is the
required combined-tree signoff before push.

### Wider production baselines — 2026-09-09 09:24 UTC

On e44e08c, the saved hash predicate returned count 1 over one minute
(426.64/479.52 ms), one hour (483.72/471.91 ms), and one day
(766.77/712.59 ms), ordinary count(timestamp) followed by count(*).
Both seven-day forms hit the three-second timeout (3028.24/3113.22 ms);
histogram completion and unique-partition counters stayed zero.

Daily count(*) probes then returned no matching rows on September 3, 4, 5,
and 7 in 545.95, 1369.70, 1545.64, and 1673.93 ms. September 6 and 8
timed out in 3087.80 and 3045.72 ms. These isolate further investigation;
they do not prove indexed execution or overall production acceptance.
Evidence: production-wider-window-validation.json, production-day-week-validation.json,
and production-daily-window-validation.json. Production was observed on
e44e08c immediately after the daily probes. Backfill unit logs continued
through 09:23:56, so the pass was progressing rather than known stopped.

The combined-tree make ci-signoff completed successfully: formatting,
Clippy, 1,506 tests, ten doctests, PostgreSQL smoke, and all 63 e2e tests.
All five checks are locally attested; none remains for GitHub. E2e nextest
5128033f-d790-43ec-b0c7-a96f0c311d46. The committed tree is ready to push.

### Matcher deployment and fresh-ingest follow-up — 2026-09-09 09:39 UTC

PR #237 merged at 03cc1ae68cd88aa3efca4984e7679453760cdd5b after all
five local checks passed. Deployment run 34335914742 is pending behind
the active Build and Deploy run 34333464243 for 04d70eaf. The previously
watched Push on master run 34333837490 was CodeQL, not deployment; its
status cannot prove deployment progress. Production was last observed at e44e08c.

In the isolated fresh-ingest checkout, both new regressions failed before
implementation and passed afterward (2.308s, nextest
3f5f2726-8486-48b8-87d3-1ec1395602e8). Server flushes now use a shared
post-commit callback that builds physical Parquet indexes. Queue shutdown
waits for its tracked worker; that separate fix is committed locally as
a17593ec. Neither follow-up is pushed or deployed.

The broader Tantivy/search/bloom test selection passed 35 tests in 10.675s
(nextest 1425be4c-ba39-4a18-b950-278f24f04668), with one leaky-process
classification. A rerun with per-test output and an added multi-file callback
case is running. Full signoff for this follow-up remains required.

### Goal audit — 2026-09-09 09:49 UTC

The preceding goal turn made concrete progress: fresh physical indexing
and worker shutdown passed their regressions, broader tests, and multi-file
coverage; the matcher fixes merged to master at 03cc1ae. Deployment
34335914742 remains pending behind the confirmed live image build
34333464243. Production acceptance is still incomplete.

Fresh-ingest signoff caught a missed callback rename in the buffered-write
benchmark. The benchmark now uses the production physical-file factory
and paired reader. Clippy failed, so no later checks were attested; a full
rerun is starting. The private cache isolates these builds from root work.

Saved EXPLAIN plans for September 6 and 8 show scans of hashes and version
keys, deduplication, then membership filtering. Printed file groups are
truncated, so their visible file names are not a full file count. A new
absent-hash regression is compiling at root: complete indexes must return
zero without decoding visibility rows; missing coverage must decline.

The overall goal remains open: deploy and test both follow-ups, resolve
slow historical days and incomplete coverage, obtain optimized timings,
then profile popular columns and time ranges. Next audit: 10:19 UTC.

EXPLAIN qualification: the histogram matcher accepts aggregate/projection/
sort/limit roots, not an Explain wrapper. The saved plans describe ordinary
execution and are not proof of the route entered by a cancelled query.
Zero completion counters show no completed histogram, not necessarily no
attempt. Keep this distinction when diagnosing the slow production days.

Production 04d70ea was confirmed before and after the 10:00 probe. The
narrow query returned count 1 in 432.81ms; September 6 still timed out in
3024.53ms. Snapshot and uniqueness completion counters were zero. Evidence:
production-upstream-04d70ea-validation.json. Matcher deployment 34335914742
is now actively building; the preceding 04d70ea deployment succeeded.

2026-09-09 10:17 UTC production check: image 03cc1ae is running and the
deployment readiness soak is active. Saved narrow count(timestamp)/count(*)
queries both return count 1 in 365.61/278.50ms. September 6 and the seven-day
range still time out at three seconds in both formulations. Histogram
completion and uniqueness counters remain zero before and after the probe.
This does not identify the route of canceled queries. Evidence:
`evidence/2026-09-08-hashes/production-03cc1ae-validation.json`.

The empty-index SQL optimization now avoids scheduling daily uniqueness
maintenance for certified-empty partitions. Its regression failed before
that scheduling change and passes afterward. All five histogram unit tests
pass in 12.782s (nextest 73fa6cb5-2aff-4118-9625-8f9aa7fdab30).
Fresh physical flush indexing remains local: its signoff exposed a macOS
LLVM 15 unwind crash, reproduced with backtraces and resolved in the targeted
Apple-linked binary. Default-linker full signoff is rebuilding. Neither
pending change is claimed deployed. Wide-range acceptance, optimized SQL
benchmarking, ingestion contention, and subsequent CPU/memory profiling of
popular queries across other columns and time ranges remain open.

Progress audit 2026-09-09 10:19:08 UTC: the active goal remains fast hash
queries through Tantivy, deployed and verified, followed by ongoing CPU and
memory profiling across popular columns and time ranges. This interval made
concrete progress: reproduced and fixed redundant empty-query proof seeding,
passed targeted SQL/visibility tests, diagnosed the local E2E unwind failure,
and verified production image 03cc1ae. Deployment 34335914742 now completed
successfully. Wider production queries still fail the three-second target;
no completion is claimed. Continue fresh-ingest signoff session 72595
(`/tmp/timefusion-fresh-index-apple-signoff.log`), integrate local commit
65f6bd7a after the fresh branch is ready, sign off the combined source,
push/deploy, and verify index use and latency. Broader optimized benchmarks,
ingestion contention, and post-acceptance profiling remain required.
Next progress audit is due by 10:49:08 UTC.

Progress audit 2026-09-09 10:48:29 UTC: this interval reproduced the
project-wide DML capture bottleneck seen in production and implemented
range-aware capture protection. Review found and fixed widened coalescer
execution gaps, with observed red and green regressions. Six targeted tests
pass with no leak classification in the final run. The earlier transient
classification remains unidentified, not claimed fixed.

Fresh-ingest indexing and the default macOS linker passed all five local
checks, including 1,506 tests, ten doctests, PostgreSQL smoke, and 63 E2E
tests. Root commits 65f6bd7a, da95b526, and 200134e1 are merged with that
work as f2368db2 in /tmp/timefusion-hash-flush. Full combined signoff is
running in session 98582, log /tmp/timefusion-hash-integrated-final-signoff.log.
Keep source inputs frozen. Only passing final-tree attestations may authorize
push. No combined change is deployed yet.

Production 03cc1ae's census at 10:46:29 UTC reports 2,203 uncovered files,
zero oversized, 125 today, 1,348 in the last week, and 730 older. The 10:30:14
census was 2,217. These totals include changing live files and are not a pure
index-build throughput measure. Wide query acceptance still fails the latest
three-second probes. Continue through combined signoff, push/merge/deploy,
and verify actual routing and latency under ongoing DML. Optimized workload
benchmarks, ingestion contention, and the requested subsequent CPU/memory
profiles across columns and time ranges remain open.
Next progress audit is due by 11:18:29 UTC.

Combined local signoff completed successfully on 2026-09-09. Command:
`CARGO_TARGET_DIR=/tmp/timefusion-isolated-target make ci-signoff` in
/tmp/timefusion-hash-flush. Formatting, Clippy, 1,507 tests in 150.385s,
ten doctests, PostgreSQL smoke, and 63 E2E tests in 231.137s passed.
Test run: 3176000b-3a28-4a8c-af8c-1bab4d2758d5. E2E run:
de88eb4f-233d-4518-bf34-765528b3de56. The final E2E run reports three
slow tests, no retries, and no leak classification. All five passing
attestations are published. The final gate requires no checks from GitHub.
This documentation update changes no check input. Push, merge, deployment,
and production acceptance follow; no production improvement is claimed yet.

Progress audit 2026-09-09 11:09:48 UTC: the combined source passed all
five local checks and merged through PR 238 as dd5647f218cbdaf676ed8ccda220bd92bc7bbb24.
Deployment 34343939580 is live at its image-build step:
https://github.com/monoscope-tech/timefusion/actions/runs/34343939580
Production acceptance is pending. Root fast-forwarded to the merged source.

The SQL benchmark harness is now prepared in root benches/hash_histogram_sql.rs
with its Cargo registration. An optimized build is live in session 23306:
`cargo bench --bench hash_histogram_sql --no-run --locked`, log
/tmp/timefusion-hash-sql-optimized-build.log. It uses root's own target cache,
not the private signoff worktree cache. No optimized timing is claimed yet.
The harness checks exact buckets, duplicate array elements, overlap unions,
version replacements, and routing for 3/7/30-day complete/partial coverage.
Its warm uniqueness proofs are an explicit measurement condition; cold readers
and ingestion contention still need separate measurements.

Next: verify deployment and running task image dd5647f, then run
/tmp/timefusion-production-hash-probe.py dd5647f with a JSON evidence path.
The probe now checks actual running tasks before and after, not just the
service's desired image. Finish the optimized build and execute the workload
against local MinIO. Wide-query acceptance and subsequent cross-column
CPU/memory profiling remain open. Next progress audit is due by 11:39:48 UTC.

At 11:13:01 UTC, production still runs task q28k1pv1ndxgzxzi5d3odh76i
with image 03cc1ae. Saved pre-deployment counters in
evidence/2026-09-08-hashes/production-pre-dd5647f-counters.json.
The cached uncovered-file gauge is 2,217; histogram completions and unique
partitions are zero. This is a stats snapshot, not a fresh file census.

The reusable production probe is now bench/hash_histogram_production.py;
prefer it over the temporary script. It exits nonzero for incomplete
comparisons, a mismatch, an image change, or a native query over three seconds.
Its wrong-image guard was exercised through actual SSH before any SQL.
The benchmark review added awaited database shutdown before releasing
temporary storage. Both review passes are recorded in
2026-09-09-hash-sql-benchmark-review.md. The optimized build remains live
in session 23306; the deployment remains in its image build step.

The optimized SQL harness now includes one fresh-application-cache sample
per arm/case, in addition to four warm repetitions (240 queries total).
It creates a new database and search service for each cold sample and
recovers persisted proofs, following the existing real restart fixture.
MinIO and OS caches remain warm. This is not a cold-storage benchmark.
The current build session 23306 remains live with these source changes.
Run the finished harness before interpreting timings or publishing signoff.

PR 238's GitHub gate passed in nine seconds and reused the local checks:
format, Clippy, test shards, and E2E were skipped remotely. The image build
is separate and remains in progress in deployment 34343939580. No PR review
comments were returned by either the inline or issue-comment endpoint.

Release-scope correction: recent progress notes calling three seconds an
acceptance target refer only to the initial saved-query smoke probe.
The proposed release targets above remain p95 below one second and p99
below two seconds, with the full recorded workload and freshness checks.
The script now names its result probe_passed to avoid conflating these gates.
Two smoke repetitions cannot establish tail latency. The actual chart API,
common endpoints, log patterns, overlap predicates, non-hourly buckets,
3/7/30-day ranges, recent tail, ingestion contention, and maintenance health
remain required. Do not close the goal on a passing three-second probe.

Application-path inspection: sibling Monoscope Pages/Anomalies.hs builds
`hashes[*]=="<prefix><hash>" | summarize count(*) by bin_auto(timestamp)`.
Web/Routes.hs exposes /chart_data and /chart_data/stream, with pid, query,
from/to, and chart_type parameters. Web/Auth.hs accepts an existing CLI
Bearer session for these browser routes. The TimeFusion matcher supports
jsonb_path_exists(to_jsonb(column), exact-element JSONPath) as well as array
containment, but the initial SQL probe exercises only containment. Verify
the real chart API separately. Monoscope's default-range comments still
describe the old array-scan limitations; do not infer current performance
from those comments or widen defaults before production measurements.

Chart-handler access is verified through the existing authenticated
Monoscope CLI. `monoscope auth status` reports token authentication for
the busy project; overriding the project to the saved issue also succeeds.
The CLI chart command calls /api/v1/metrics, whose handler delegates to
the same Charts.queryMetrics function as /chart_data. The exact issue KQL
for 2026-09-02 08:04:30 through 08:04:31 returned one event with no error.
Saved production-chart-handler-baseline.json records the response and
03cc1ae observed afterward. Elapsed time and cache state were not measured;
this is access/result evidence only. No credential was printed or saved.

The chart handler caches data and refreshes partial buckets. A changed
time bound can therefore still reuse most historical data. Do not label
API samples cold merely because bounds differ. Combine actual chart-query
measurements with direct SQL timing/routing evidence; record application
cache uncertainty where it cannot be established. Both live builds remain
pending; the optimized compiler is working on the benchmark and server
binaries without a reported error.

Progress audit 2026-09-09 11:38:39 UTC: the previous goal interval made
concrete progress by merging the locally signed-off implementation, adding
reviewed warm/cold SQL measurements, and verifying authenticated chart-handler
access with the known count. The current interval is a verified wait: local
build session 23306 remains live, compiler processes 98817/98818 are active,
and deployment 34343939580 is live at Build and Push Docker Image. Neither
build has reported failure. Disk availability is about 25 GiB. Do not restart
either job merely because its output is quiet.

The goal remains aligned with exact, fast hash-column queries. The next
actions are optimized benchmark execution, running-image verification, SQL
smoke comparisons, chart-handler measurements, and investigation of any
failure or latency gap. Three-second smoke success is not release acceptance.
The proposed p95/p99 targets, common/overlap/log-pattern workloads, non-hourly
buckets, 3/7/30-day windows, recent tail, ingestion and maintenance health,
and later cross-column CPU/memory profiles remain open. No completion or
production speedup is claimed. Next audit is due by 12:08:39 UTC.

Progress audit 2026-09-09 12:15:38 UTC: the last pull verified that remote
master remains current and preserved local benchmark work. Since the previous
audit, dd5647f deployed successfully, but production day/week smoke queries
still timed out and native completion counters stayed zero. A real startup
regression exposed the remaining backfill opt-in; removal now passes that
regression in 0.663 seconds. Full local signoff is running in the isolated
automatic-backfill worktree, session 6625, before any source push.

Optimized 300k and 3M fixtures passed all 240 bucket/routing comparisons
each. Complete warm rare-hash counting is fast; incomplete coverage and
cold index preparation remain measured bottlenecks. The diagnostic 3M run
shows cold download/unpack costs and zero index fetches on slow warm partial
queries. A five-second CPU sample captured visibility deduplication and
Arrow row conversion, but spans ordinary/native queries and does not assign
per-query CPU percentages. Its whole-process footprint is not a heap profile.
Evidence is saved alongside the benchmark summaries. The profiled process
has ended and emitted 240 samples; its missing session does not establish
an exit code. No profiler timings replace unprofiled latency evidence.

The goal remains open: deploy automatic startup coverage, verify production
coverage and exact chart-query behavior, fix measured visibility/cold-fetch
costs, meet the full workload and latency scope, then profile other columns.
Next progress audit is due by 12:45:38 UTC.

Actual chart-handler follow-up on dd5647f: the saved narrow issue window
returned one event with no error in 665.50 ms, including CLI overhead.
The Sep 6 day and Sep 2–9 week each exceeded the 12-second client deadline.
The running image/task matched before and after. Cache state is unknown.
These are three observations, not tail-latency estimates. The noon cron
selected 34 otel files within 1,938 MiB; logged units confirm that scheduled
backfill is active despite the missing automatic startup pass.

The automatic-backfill signoff exposed one first-attempt failure in
shutdown_writes_clean_snapshot_under_deadline: no cursor snapshot was found
after a one-second shutdown budget. The configured retry passed in 1.063 s.
This is an unresolved timing observation, not proof of an introduced backfill
regression or a clean test run. Inspect and rerun it after the suite finishes.

Progress audit 2026-09-09 12:43:56 UTC: this interval made concrete progress.
The automatic startup-backfill fix passed all five local checks and merged
through PR 239 as a31c7b5. The suite retained one shutdown-test retry; ten
independent retry-disabled stress iterations passed. Deployment 34351293065
remains active; do not assume the new image is serving or cancel its build.

The user added local production-image build and deployment reuse to remove
the repeated CI compile delay. In /tmp/timefusion-hash-flush, branch
build/local-production-image, native ARM64 Rust and C cross-toolchains have
produced and executed Linux x86-64 probes, including OpenSSL/libunwind.
Full production cross-build session 25358 remains live. The image helper
archives stable inputs, requires matching signoff, smoke-tests before push,
and publishes a content-tagged candidate. The workflow can reuse that image
and pins its digest before deployment. Git-based fingerprint tests and
actionlint pass. Full image smoke, publication, reuse, and direct local
deployment remain unproven or unimplemented. Changes are not pushed yet.

This supports the shipping loop; it does not replace the original goal.
Still required: verify automatic backfill in production, exact fast SQL and
chart queries across the full planned workload, fix partial-visibility and
cold-index costs, confirm ingestion/maintenance health, then profile CPU
and memory across other columns and time ranges. No completion is claimed.
Next progress audit is due by 13:13:56 UTC.

Production a31c7b5 follow-up: deployment 34351293065 completed successfully,
including readiness soak. Running task lpeid8zwffglvatr39u290u83 uses the
expected image. At 12:51:26 indexing became active; at 12:51:29 startup
backfill selected 34 otel files within 1,910 MiB. A first completed unit for
the saved-issue project was logged at 12:51:53. This verifies the removed
startup opt-in in production. It does not establish complete coverage.
The image-guarded SQL smoke probe is running in session 73563; its result
will determine whether any latency gap has improved.

The a31c7b5 post-deploy smoke completed with exit 1. Eight of sixteen
comparison pairs completed without a mismatch; the slow Sep 6 day and
Sep 2–9 week timed out in both ordinary and native forms. Image verification
matched before/after. All three histogram counters remained zero. Evidence:
production-a31c7b5-smoke.json. The startup fix is verified, but latency has
not improved for the failing windows. Next hash investigation must establish
physical element-index coverage for this project and these dates; broad
index-build logs cannot prove coverage of the actual queried snapshots.

Physical coverage audit after a31c7b5: the saved-project manifest has
11,382 entries, only 153 of which declare hash elements. Reading Delta
version 556758 found 2,294 live project files. The Sep 6 partition has
166 live files and zero physical hash-index candidates. Sep 2–8 has 1,406
live files and only 14 candidates, all on Sep 8. Matching used exact
one-file URIs, schema version, hash element fields, and physical ordinal
flags. The manifest and Delta snapshot were loaded independently; no
Tantivy contents were decoded. The report is saved as
production-a31c7b5-physical-hash-coverage.json.

This explains the Sep 6 no-index fallback. The week has sparse usable
coverage, so zero completed histogram counters must not be read as proof
that the week never entered native planning. Visibility/scanning can time
out before the completion counter increments. Next implementation must
make physical hash coverage converge faster as well as reducing partial
visibility cost; startup admission alone cannot resolve this backlog.

Audit access note: production non-secret service settings match .env.prod
(OVH, timefusion-eu, default timefusion table prefix). The index manifest is
at bucket-root index_manifests/otel_logs_and_spans/<project>/manifest.json.
create_object_store builds a bucket-scoped store and does not apply the
URI path as a prefix. The first prefixed GET returned NoSuchKey; the
bucket-root GET succeeded. No credential values were printed or recorded.

Progress audit 2026-09-09 13:13:50 UTC: the interval made concrete progress.
Automatic backfill is verified in production a31c7b5, but post-deploy day/week
SQL probes still fail. An independent Delta/manifest audit established zero
physical hash candidates on Sep 6 (166 files) and 14 among the week’s 1,406
files. This directs the next query work toward coverage convergence and
partial visibility cost; do not reinterpret zero completion counters as a
global indexing disablement.

Local release work now includes native ARM64-to-Linux-amd64 compilation,
content-based image publication/reuse, shared deployment scripts, and a
local/CI deployment runner. Git-based tests cover source snapshots and
rollout exclusion, including retaining a lease when a submitted rollout is
unresolved. The local client image built, but actual local rollout remains
unverified because CapRover credentials are absent from checked local
environments/config paths. The command rejects that state before mutation.
The full production cross-build is still live, session 25358. A VM process
inspection found rustc PID 85022, CPU ticks 184893 and about 1,794 MiB RSS.
No finished image, successful smoke, or publication is claimed.

The full hash workload/latency acceptance, ingestion and maintenance health,
and later CPU/memory profiles across other columns remain open. Build and
deployment changes are not pushed until relevant validation is complete.
Next progress audit is due by 13:43:50 UTC.

Progress audit 2026-09-09 13:42:51 UTC: native ARM64 cross-compilation produced
a tested Linux amd64 production image. QEMU guest address placement caused
local crashes with both the candidate and known-good production images;
a canonical guest-base offset fixed the harness without changing runtime
CPU or profiling settings. Full local signoff passed and published the image.
PR 240 merged; deployment 34358189411 skipped compilation, passed native smoke,
and completed recovery verification and 46 readiness probes without failures.

A review during local preflight caught promotion wrapping the image in an
index, which gave identical runtime bytes different outer digests. The local
lease waiter was stopped before mutation. A real registry comparison failed
before the normalization fix and passed afterward. PR 241 merged as 95bdacb1
with passing local signoff and a new tested candidate. The local deployment
command is now being exercised with the existing authorized app token read
into memory; no secret values were printed. Its redundant new CI deployment
is being cancelled before production work. Local completion is not yet claimed.

The measured hash gaps remain unchanged: Sep 6 had zero physical candidates
among 166 live files, and Sep 2–8 had only 14 among 1,406. Complete warm queries
are fast, but cold index loading and repeated streamed visibility resolution
remain expensive. After local rollout verification, resume coverage convergence
and partial-visibility optimization with exact parity and bounded memory.
No hash acceptance or cross-column profiling completion is claimed.
Next progress audit is due by 14:12:51 UTC.

Next bounded query experiment: cache completed-day streamed Delta winner masks,
not memory-overlay winners. The profile already attributes slow warm partial
queries to visibility sorting with zero index downloads. A cache must use exact
ordered file metadata (including deletion vectors), root, projected visibility
schema, dedup keys, tie-break, tombstone semantics, and query bounds. Bucket size
and hash membership do not change visibility. Reject reuse when current memory
rows or covered ranges intersect the day/window; preserve current memory masks
and row counts separately. Use the shared query memory pool and bounded resident
LRU ownership, with reservations retained by pinned queries after eviction.

First prove the streaming cache miss/hit and invalidation behavior in the real
MinIO histogram regression, including new Delta files, deletion vectors,
changed bounds, tombstones, and a fresh memory overlay. Keep exact ordinary/native
bucket parity in the 240-query benchmark. Do not claim this alone fixes production
coverage: most historical files still require physical hash indexing. Evaluate
hash-priority indexing only with honest per-field coverage metadata so missing
text fields never become false negative proof. Both experiments remain proposed,
not implemented or measured.

Local release follow-through: PR 242 fixed the foreground/background rollout
observer race without relaxing budgets. The local deployment then passed with
2,423 ms unready, 2,525 ms old-to-new query handoff, zero WAL recovery, and 55
passing readiness probes. A second invocation recognized the same image/boot,
ran only record-boot plus soak, passed 55 probes, and kept the same task ID.
Its lease was released. The failed observer and separate 56-probe recovery
soak remain recorded in local-rollout-observer-race.json.

The latest exact-digest production hash smoke still completes only 8/16 pairs,
with no mismatch and a stable image. Sep 6 day/week queries hit the three-second
statement limit; completed histogram counters remain zero. Resume query work
from this evidence. The local release work is verified; the main hash-query
performance goal remains active.

Progress audit 2026-09-09 14:15:09 UTC: streamed visibility-cache work is active
in /tmp/timefusion-hash-flush on perf/streamed-histogram-cache. The real MinIO
regression failed before implementation and passed afterward, alongside the
cache ownership test. First Rust reviews fixed a large enum layout, wildcard
matches, paired file/reservation ownership, and bitmap/key memory accounting.
Expanded memory-overlay cases are compiling; the test must move MemSnapshot
with mem::take instead of adding Clone. No cache change is signed off or deployed.
Production remains at the verified local release, and exact day/week hash probes
still time out. Keep physical hash coverage and cold first-query latency in scope.
Next progress audit is due by 14:45:09 UTC.


### Historical backfill measurement — 2026-09-09 14:35 UTC

The current healthy production container 6722ae2dc2e1 started a 24-file,
1,971 MiB historical otel pass at 13:49:38 UTC. Direct container logs show
15 completed historical units, two for the saved-issue project. The last
completion is 14:11:49 UTC. Fresh-file index logs continue, but are not
historical throughput. A single Docker statistics sample reports about
26.7 CPU cores and 41.74 GiB resident usage; it cannot attribute that load
to backfill. Evidence: production-current-backfill-progress.json.

The 14:27 physical audit found 17 of 305 Sep 8 files covered and zero of
166 Sep 6 files. No completion ETA is established. Full text fields still
share the committed-file builder with hash elements, so hash-priority
indexing must avoid those costs while preserving existing text coverage.
A second index cannot simply be added under another manifest key: current
histogram selection correctly rejects overlapping physical coverage, and
text-search coverage must not treat a hash-only blob as a complete index.
Any separate element artifact requires explicit reader, maintenance, and
GC support, plus mixed-generation correctness tests.

The streamed-visibility cache and benchmark sources match in both working
trees. Targeted tests and lint passed; the optimized benchmark compiler
remains active. No cache deployment or speedup is claimed yet.


### Coverage audit correction — 2026-09-09 14:41 UTC

The earlier physical audits overstated the saved project's file denominator.
The Python Delta reader returned all table URIs despite the supplied project
partition filter with skip_stats enabled. Comparing explicit add-action
partition values exposed the mismatch: 2,360 table files, 367 for the target
project. The corrected audit filters exact URI partition segments explicitly.
This supersedes prior claims of roughly 1,400 missing files for this project
and 166 Sep 6 files; those denominators included other projects.

At Delta version 557113 the project has 367 live files. Sep 2–8 has 225 live
files, 17 with usable physical hash coverage, leaving 208 missing files.
Sep 6 has 33 files and zero covered. Sep 8 has 73 files and 17 covered.
The corrected report replaces production-8524b316-physical-hash-coverage.json.
The earlier production-a31c7b5 report is retained as superseded evidence, not
a valid per-project census. The separate add-action volume check found
7,389,972,212 compressed bytes in the 208 missing Sep 2–8 files. Row counts
are unavailable with the chosen metadata-only load and must not be read as
zero rows. Historical completion logs and query timeout results are unaffected.
The user received an explicit correction. No completion ETA is established.


### Hash-only build input measurement — 2026-09-09 14:43 UTC

Read metadata only from the smallest, median-sized, and largest unindexed
Sep 6 files for the saved project. Hashes, timestamp, and identity account
for 10.8%, 16.3%, and 15.4% of compressed column bytes, respectively. The
largest file contains 1,972,423 rows: 258,019,995 compressed column bytes
versus 39,627,193 for the selected columns. This supports projected element
indexing as a way to avoid most column I/O and unrelated text tokenization.
It is not a measured build-time, heap, or network-latency improvement.
Evidence: production-hash-column-footer-sample.json. The first PyArrow S3
read failed because OVH rejected its checksum-mode header; bounded boto3
footer range reads succeeded, with footer length and magic checked. No data
pages or event payloads were read for this measurement.


### Goal audit — 2026-09-09 14:44 UTC

The goal remains active. Production is healthy on the verified local image,
but saved day/week hash queries have not passed latency acceptance. Historical
backfill is active and slow. The corrected per-project census leaves 208
Sep 2–8 files unindexed, totaling about 7.4 GB compressed. Three actual file
footers show selected hash/identity columns occupy 11–16% of compressed
column data, supporting a separately covered element-index implementation.
This requires reader, manifest, and GC changes; it is not implemented yet.

The visibility cache passed its red/green regression, memory-overlay and
identity invalidation checks, and two source reviews. Full local fmt and
Clippy now passed and are attested. Full tests are still compiling in session
23512; the optimized benchmark is compiling in session 75825. Both handles
were confirmed live. Sources remain frozen. Complete the tests and measure
the exact-query benchmark before image signoff and production deployment.
All required production correctness/performance checks, coverage acceleration,
and subsequent other-column CPU/heap profiling remain open.
Next goal audit is due by 15:14 UTC.


### Live indexing CPU investigation — 2026-09-09 14:53 UTC

A ten-second perf cpu-clock sample at 49 Hz captured 10,169 samples from
the current database process, with zero lost samples. Inclusive stacks
show 52.37% under dedup_partition_range_limited, 10.54% under coordinator
compaction, 10.18% under coordinator rollup, and 8.90% under Tantivy
IndexMerger::write. These are overlapping whole-process stack observations,
not additive costs or per-backfill-file attribution. The sample confirms
substantial competing maintenance CPU and ongoing Tantivy merge work.
It does not establish why any specific historical file has not completed.
Raw data, self/inclusive reports, and limitations are saved in the
production-backfill-cpu artifacts. The first attempt sampled docker-init
and had zero CPU samples; it was corrected to the verified child database
PID 1079501. No sampler configuration or production image was changed.

Local validation now includes passing fmt, Clippy, all 1,508 main tests, ten
doctests, and PostgreSQL smoke. End-to-end checks remain active. Benchmark
compilation succeeded; timed execution still awaits idle validation work.


### Optimized cache benchmark — 2026-09-09 15:02 UTC

The 3-million-row SQL benchmark exited 0 with all 240 exact hourly bucket,
route, and cache-hit assertions passing. Every repeated warm partial query
reused all queried daily masks. Thirty-day partial warm medians were
208.1 ms (rare), 213.0 ms (medium), 595.0 ms (common), and 259.6 ms (overlap).
The prior uncached run measured 730.1, 757.4, 980.7, and 767.1 ms. These
are historical-run comparisons under different host load, not isolation of
every latency change. Within the new run, ordinary scans were still faster
for those partial cases (173.1, 166.5, 305.3, and 177.1 ms).

Complete warm histograms stayed at 9.3–147.3 ms across these predicates.
Partial application-cold queries remained at 1.33–2.08 seconds. Warm partial
queries had zero blob fetches and index opens, confirming the remaining
cost is elsewhere. Whole-process peak footprint was 594,500,544 bytes,
including setup and all queries; it is not a per-query heap profile.
The benchmark supports deploying the cache as an improvement, while
physical coverage, cold I/O, and residual partial counting costs remain open.

Full local checks passed under the configured retry policy, but e2e reported
one first-attempt checkpoint failure. Investigation found table creation
explicitly enables post-commit hooks and property reconciliation uses defaults,
contrary to the out-of-band checkpoint rule. A strengthened existing in-memory
property test is compiling against unchanged implementation for a red run.
New changes will need renewed checks before image signoff and deployment.


### Follow-up fixes — 2026-09-09 15:09 UTC

The deterministic property-reconciliation regression failed on the original
implementation: nextest 69723f0b-902c-4077-8a50-d47692dc3860 exited 100 in
0.367 seconds because checkpoint files existed after property updates with
checkpoint interval 1. Both table creation and property reconciliation now
reuse base_commit_properties, keeping checkpoint and expired-log hooks out
of the foreground operation. Assertions and scheduler behavior are unchanged.

The first cache benchmark still reopened every Parquet footer before cache
lookup. Preparation now happens after a miss, or lazily for a source that
must scan because its index is absent or fails. A cache hit retains the exact
file/DV/window identity already validated by its captured masks. Prepared
metadata and its memory reservation remain paired. A new operation counter
exposes histogram_parquet_prepares in timefusion_stats and benchmark output.
The existing real partial-index regression now requires only the unindexed
replacement to prepare Parquet on a repeat. The SQL benchmark asserts the
same per-day behavior over all warm partial cases.

These changes are not validated yet. Targeted green tests are running in
session 40708; the next optimized build is running in session 82809 in the primary worktree. Source
is frozen in both worktrees. The earlier full-check attestations predate
these changes and cannot sign off this final source. Re-run the affected
checks and repeated source reviews before publishing or deploying an image.


### Goal audit — 2026-09-09 15:13 UTC

The goal remains active and production performance acceptance is incomplete.
The first optimized cache run passed 240 exact SQL cases and reduced measured
warm partial medians, but still trailed ordinary scans and did not solve cold
latency. Source review identified redundant Parquet metadata reads on hits;
these are now deferred until a source needs fallback. A public statistics
counter and existing integration assertions verify the number of prepares.

A real e2e failure exposed foreground table-creation/property checkpoint
hooks. The strengthened property test failed before the fix (nextest
69723f0b-902c-4077-8a50-d47692dc3860). After both paths reused the existing
base commit settings, targeted nextest ed7196c6-826d-469e-ac36-555c1d61687b
passed all three tests in 9.650 seconds, including lazy metadata reads,
visibility invalidation, and reservation ownership. The new full local gate
is running; the optimized build remains live in session 82809. Both source
trees are frozen. The first cache executable is preserved for comparison.

Still required: final full checks and repeated reviews, optimized comparison,
image signoff, deployment, and production correctness/latency monitoring.
Dedicated element indexes, historical coverage convergence, and subsequent
other-column CPU/heap optimization remain in scope. The corrected project
backlog and actual footer/CPU evidence supersede earlier broad estimates.
Next goal audit is due by 15:43 UTC.

### 2026-09-09 15:45 UTC goal audit

The goal remains fast exact hash queries in production. Revised cache checks
have passed formatting, clippy, and all 1508 nextest tests; the remaining
local gate and optimized benchmark build are still in progress. No cache
release has been deployed. Dedicated element-index implementation remains
isolated and uncompiled; publication, GC, and scheduling integration must
precede enablement. The measured 11–16% projected column-byte fraction is
not a measured indexing speedup. Production acceptance and subsequent
other-column CPU/memory profiling remain open.

## Maintenance constraint — user correction, 2026-09-09 15:52 UTC

Do not introduce a second permanent per-file indexing obligation or an
independent background worker pool merely to reduce backfill latency. The
previous separate-obligations/scheduling proposal is superseded pending a
measured total-cost design. Continue the cache release independently: it
reuses query work within an existing bounded memory cache and adds no
maintenance sweep.

A narrow artifact may repair missing physical hash coverage, but an existing
usable full artifact must satisfy the same requirement. New files must not
receive duplicate hash and full builds by default. Reuse existing admission
limits, and measure steady-state CPU, memory, I/O, artifact count, and backlog
under ingestion, dedup, rollups, and compaction, not just a one-file build.

Source inspection identifies a critical cycle to resolve: compaction can
carry text coverage forward without rebuilding, but forfeits physical row
ordinals. The backfill coverage gate then rejects that artifact for element
histograms. A narrow rebuild is still recurring work after such rewrites;
calling it a one-time historical repair would be misleading. Investigate
reuse across rewrites and indexing in existing write work before adding
another maintenance obligation. Dedicated-artifact source remains isolated,
uncompiled, and undeployed while this design is evaluated.

16:09 UTC progress: dedicated historical indexes are abandoned at the user's
request. Optimize the ordinary full-index builder; do not add another
maintenance obligation. Source now projects only full-index columns and
prepares variants once per batch in the isolated perf/full-index-rebuild
branch. Microbenchmarks and footer estimates are evidence of component
cost only; full rebuild validation is in progress.

Cache commit 0ec937e8 plus evidence commit 6f13e65e passed all local checks
and 240 revised SQL cases. Local ci-signoff is building its candidate image;
no cache deployment has occurred. Fresh 16:07:55 production metadata shows
Sep6 0/33 files covered, Sep8 19/73 (previously17), and Sep9 33/131.
Historical unit events are present through 15:53:46 across projects.
Live metadata includes dates older than 30 days; retention enforcement and
backfill priority require checking, without assuming live files may be
deleted. Production hash latency and full rebuild acceleration remain open.

### Goal audit — 2026-09-09 16:11:48 UTC

The full goal remains active and unproven. User constraints now explicitly
exclude a special historical hash index and new maintenance obligations.
The active rebuild source changes only full-index projection and per-batch
variant preparation. First Rust review and cargo lint passed (7m20s);
subsequently extended the existing test for three batches including all-null
variants. Full local tests are compiling; rerun lint for final test source
before signoff. The complete build benchmark baseline is compiling.

Cache release local checks and 240 SQL cases passed; candidate Linux image
is building through make ci-signoff. No new image is deployed, so production
query latency acceptance remains open. Fresh metadata confirms historical
backfill progress but Sep6 remains 0/33 covered in the saved project.
Full build-time speedup, production maintenance cost, query latency, and
later other-column CPU/memory profiles are still outstanding.

### Goal audit — 2026-09-09 16:41:57 UTC

Goal remains active. Cache release passed local correctness checks and 240
SQL benchmark cases. Its Linux dependency cook finished; the final profiling
application image build is now running. Image smoke, publication, push/merge,
local rollout, production exact-result comparisons, and latency acceptance
remain unproven. No new release has been deployed.

Full-index rebuild changes passed 1508 tests, 10 doctests, final formatting,
Clippy, and PGWire smoke. E2e is compiling. Both requested Rust reviews
are recorded. Baseline benchmark compiled successfully; candidate compilation
and a separate merge/query probe are active. Neither an end-to-end rebuild
speedup nor acceptable multi-segment query cost has been measured yet.

No special historical artifact or extra maintenance pool remains in active
source. Source changes preserve the normal full-index format and physical
row validity. Full production historical coverage and fast day/week queries
remain incomplete. Subsequent popular-column CPU and memory profiling and
optimization are still outstanding.

### Coverage convergence measured, not a gate rejection — 2026-09-09 17:25 UTC

Two audits of the same unified project, 76 minutes apart, answer why saved
day and week hash queries still miss the index. Sep 1 through Sep 7 held
exactly zero physical hash candidates in both readings, across 166 live files.
Sep 8 moved 19 to 21. Sep 9 fell from 33 candidates over 131 live files to
1 over 78, because compaction rewrote those files and a rewritten file
forfeits its physical row ordinals. Evidence: the 16:07:55 and 17:23:41
production-*-physical-hash-coverage.json artifacts.

The cause is throughput and ordering, not the coverage gate. Backfill units
did run for the unified project during this window, and the backfill path
publishes ordinals_valid = true, so its entries can satisfy
covers_current_elements. The bound is the pass budget. The 16:58:51 pass
recorded built = 33, uncovered_before = 2187, oversized_skipped = 0, and
deferred_to_next_pass = 1962. The 17:00:12 pass planned 31 files and 1,977 MB
against cap = 320 and budget_mb = 2048, and skipped 251 files in today's
partition. The pass is byte-bounded, not count-bounded.

At roughly 31 files and 2 GB per hourly pass against 2,187 uncovered files,
an undisturbed queue needs about 70 hours. The queue is disturbed: files are
ordered newest-first with a reserved oldest tail share, and compaction keeps
invalidating the recent partition the newest-first order returns to. That is
why the Sep 2 to Sep 9 week has not converged while the tail sits untouched.

This measurement does not establish which lever is correct. Raising the byte
budget spends more of a maintenance pool that a 14:53 UTC perf sample already
showed at 52.37% inclusive under dedup. Accelerating the ordinary full-index
builder, the sanctioned direction from the 15:52 correction, multiplies the
same budget instead of enlarging it. Neither is measured end-to-end yet.
No knob was changed and no production behavior was modified for this reading.

### Streamed histogram cache released — 2026-09-09 17:30 UTC

The cache release merged as 73de5c6f (#244) and production now runs
ghcr.io/monoscope-tech/timefusion@sha256:f007af8f064b3a87c2a4828cc0889de119b2153cd09b1dd8b8894a8cce369556,
the same digest that local signoff built, smoke-tested and pushed. The master
CI run resolved that published candidate instead of recompiling, and the
Build and Deploy workflow completed successfully.

Acceptance for this release was fixed before the probe ran: no correctness
mismatch, no regression on the eight pairs that already completed, and
movement in the histogram counters. The Sep 6 day and the Sep 2 to Sep 9 week
pairs are expected to keep exceeding the three-second probe bound, because
those dates hold zero physical hash candidates. That is the coverage
condition measured above, and it is not evidence about the cache.

### Production smoke on the released cache — 2026-09-09 17:45 UTC

The probe ran against the deployed digest after ten minutes of uptime and
reproduced the previous result exactly: eight of sixteen pairs completed,
no mismatch, same image throughout. Narrow queries took 281-532 ms and the
Sep 2 day took 586-1,022 ms. Sep 6 and the Sep 2 to Sep 9 week were
cancelled at the three-second statement timeout in every arm, both
aggregates, both repetitions. Evidence: production-f007af8f-hash-smoke.json.

The release is therefore live and non-regressive, and also DORMANT. Its own
counters say so: histogram_snapshots and histogram_delta_cache_hits are both
still 0 after the probe, so the native histogram route has not executed once
in production and the new cache has never been consulted. Only
histogram_parquet_prepares moved, to 104, which is preparation performed for
sources that must scan. No production latency improvement is claimed from
this release. The cache can only pay once index coverage exists.

The failing windows are explained by bytes alone. Sep 2 holds about 257 MB
of uncovered compressed columns and answers in under a second; Sep 6 holds
about 3,461 MB, roughly thirteen times as much, and exceeds three seconds.
Scan cost scales with the bytes that have no index.

### Correction to the convergence estimate

The 70-hour figure recorded at 17:25 UTC is wrong and should not be quoted.
It assumed the whole 2,187-file queue drains toward the dashboard window.
The two audits measure otherwise: manifest hash entries rose by 39 fleet-wide
in 76 minutes, consistent with built = 33 per pass, while the unified
project's September dates gained exactly 2. The observed unified-project unit
carried from_reserved_tail = true, so it served mid-August.

The order explains it. Work is split fairly across projects, each project is
sorted newest-first, and 33% of every pass is reserved for the oldest
uncovered files. Today's partition is skipped. The newest share is therefore
spread across every project's recent files, and the reserved share lands in
August. Sep 2 through Sep 7 is neither newest nor oldest, so it receives
close to zero files per pass. The dashboard band converges in weeks at these
settings, not hours.

Coverage rate is min(build rate, per-pass budget x cadence), and the two are
currently balanced: a 2,048 MB pass takes about 58 minutes of an hourly cron.
Commit 548920d1, which projects only indexed columns and prepares variants
once per batch, therefore buys headroom rather than coverage. Halving build
time would finish the pass in about 29 minutes and then idle until the next
tick. Raising the budget alone would overrun the hour and the pass semaphore
would no-op the following crons. Moving coverage requires moving both, or
draining the sealed dates outside the pass.
