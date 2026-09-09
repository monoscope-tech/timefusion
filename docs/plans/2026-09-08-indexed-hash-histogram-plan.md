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
