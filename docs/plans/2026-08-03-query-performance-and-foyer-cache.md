# Query performance and Foyer cache plan

## Evidence

Production, tenant `98fdd4f3-3544-4087-ad91-1e7ca95aba29`:

| Range | Rows | Wall time |
| --- | ---: | ---: |
| 1 hour | 33,029 | 47.53 s |
| 3 hours | 114,747 | 82.35 s |
| 3 days | not executed | unsafe before cache fix |

The 1-hour `EXPLAIN ANALYZE` opened 54 Delta files and needed 2.24 MB of selected Parquet data, but reported 82.80 s until first data and 86.55 s aggregate scan time. CPU work was small: filter 125 ms, merge 8.7 ms, aggregate <1 ms. There was no `SortExec`.

The plan also materialized a 428.9 MB hot-tier leg for a count query.

During the investigation, Foyer `inner_bytes_read` rose from zero to about 1.48 TB. A 3-day structural `EXPLAIN` alone planned 1,618 files and added about 126 GB of Foyer inner reads. The 3-day plan intentionally skips the hot tier.

A later warm 1-hour count (83k rows) still caused 185 main-cache misses and
increased `inner_bytes_read` by 1.33 GB while `EXPLAIN ANALYZE` reported only
4.99 MB of selected Parquet bytes. Query-triggered detached full-file warming
therefore amplified storage traffic by roughly 266x and landed only one new
full-file entry during the sample. This path competes with foreground range
reads and is not a viable convergence mechanism at production fan-out.

Current prod settings include 120 GB Foyer disk, 4 GB L1, and `TIMEFUSION_WARM_FULL_FILES=true`. Foyer metadata cache was partially effective (2,302 hits / 2,619 misses at the first snapshot), while main-cache coverage was incomplete.

## Root causes

### Full-file warming is not guaranteed

Recent Parquet files are intended to be full-file cached. That is the right steady-state policy for the dashboard hot path. However, the write path cannot guarantee it:

- Multipart capture is capped at 32 MB. Larger optimize outputs skip direct cache population.
- The flush-path cache confirm downloads uncaptured full objects with only four workers and a 10-second total deadline.
- On timeout, the warm future is dropped. It has no detached continuation.
- Confirm mode skips `warm_footer`, assuming the full warm will succeed. A timed-out full warm therefore leaves neither a main-cache entry nor a footer entry.

### A cold Parquet range read downloads the whole object

`FoyerObjectStoreCache::get_range_cached` treats a non-tail Parquet range as data. When the main full-file entry is absent, it calls `get_cached`, which calls `inner.get(location)` and downloads the whole object before slicing the requested range.

A warmed footer cannot answer leading Parquet header/probe reads. Those reads are classified as data, so an incompletely warmed file can trigger a full object fetch merely to read a small range.

### Hot tier eagerly materializes full-width rows

`HotTier::read_leg` runs at planning time. With `projection=None`, `plan_columns` keeps all columns, so narrow queries can decode/filter/copy large `body` and `attributes` values before downstream pruning.

### Fragmentation is a multiplier

The dirty-bin queue was about 21,720 entries, while only 11 light-optimization bins had committed. More live files amplify cache misses, metadata work, and Delta scan fan-out.

## Goals

| Horizon | Primary source | Required behavior |
| --- | --- | --- |
| <=5–10 min | MemBuffer | No object-store reads |
| 10 min–24 h | Hot Arrow tier + Foyer full files | Local narrow reads |
| 1–3 days | Foyer metadata plus selected full files | No full-object fetch from a range probe |
| >3 days | Delta plus metadata | Efficient object-store ranges; no cache pollution |

Provisional targets for the representative count query:

- 1h p95: <500 ms
- 3h p95: <2 s
- 3d: set after the cache fix; no TB-scale object reads or multi-minute scans

## Phase 0: observability

Expose the following through `timefusion_stats`:

- Foyer full-file hits/misses, range hits/misses, and range miss fallback (`get_range` versus full GET).
- Object-store bytes split by query range, query full fallback, full warm, and metadata warm.
- Recent live-file coverage: files/bytes cached versus eligible files/bytes.
- Warm worker queue depth, age, retries, deduplications, and failures.
- Existing counters currently absent from pgwire diagnostics: write-capture skips, confirm attempts/warmed/timeouts, and cache-insert bypasses.
- Per-query leg rows/bytes, files opened, Foyer bytes, hot-tier materialized bytes, first-batch latency, and wall latency.

Success: a `timefusion_stats` snapshot identifies cold Foyer data, cold metadata, hot-tier allocation, or file fan-out as the dominant cause.

## Phase 1: always warm Parquet metadata

On every successful flush or optimize commit:

1. Cache immutable `ObjectMeta`.
2. Cache the final footer/metadata range.
3. Cache a small leading Parquet header/probe range.
4. Do this independently of full-file warming and before MemBuffer ownership is handed off.

Keep footer containment lookup. Make header/probe caching explicit rather than classifying it as full data.

Success: a warmed file’s planning path performs no remote HEAD, footer GET, or leading header GET.

## Phase 2: detached, convergent full-file warming

Keep direct write-capture warming for files within the existing capture limit. Do not synchronously download uncaptured full objects on the flush critical path.

For an uncaptured cacheable recent file:

1. Commit the file.
2. Complete Phase 1 metadata warming synchronously.
3. Enqueue a deduplicated full warm keyed by immutable path and ETag/version.
4. Run bounded workers with byte and concurrency limits.
5. Retry transient failures with bounded backoff.
6. Reconstruct recent pending warm work on restart from live Delta files.

The current 10-second confirm may stop waiting for full warming, but it must not cancel the work. Full warming continues detached.

For a query range miss:

1. Serve a main-cache full-file hit when present.
2. Serve a main-cache exact-range hit when present.
3. Otherwise use `inner.get_range` for exactly the requested range and admit
   that range for repeat dashboard queries.
4. Never start a full-file warm from a query. Full-file convergence belongs to
   upload capture and bounded post-commit/restart workers.
5. Never synchronously fetch a whole object merely to satisfy a small range probe.

Full warming applies only to files inside the full-cache working-set window and below Foyer’s cacheable object limit. Metadata warming can cover all live files.

Success: range-miss object-store bytes approximate requested bytes; no query downloads a full Parquet file due only to header/footer/page probing.

## Phase 3: define Foyer capacity and working set

Inventory live physical Parquet bytes by table, project, and date; cacheable versus oversized files; query windows; write rate; eviction rate; and main-cache coverage.

Define two independent policies:

- Metadata window: all live files, capacity sized for footer/header data.
- Full-data window: dashboard hot working set only.

Set data-cache capacity to at least:

```text
full-data working-set bytes * 1.3
```

If the desired full-data window does not fit, reduce its window rather than treating all recent days as warm. Do not use a broad `cache_recent_days` value as a proxy for actual full-cache coverage.

Success: dashboard files have near-100% main-cache coverage with low eviction churn; cold files use metadata-plus-range reads safely.

## Phase 4: reduce hot-tier allocation

First, derive the hot leg’s internal projection from:

- requested output columns;
- dedup keys (`timestamp`, `id`);
- version/tombstone columns;
- predicate columns.

A count query must not decode full event payload columns.

Then, if needed, defer hot-file decode/filter materialization from planning to physical execution while preserving current query-pool charging and leg-byte bounds.

Success: hot-leg allocated bytes scale with required columns and returned rows, not full event payload width.

## Phase 5: reduce Delta file fan-out

1. Drain the dirty-bin backlog with bounded work per bin.
2. Keep compaction concurrency within memory limits.
3. Track files written per flush versus files removed by optimization.
4. Prioritize project/date partitions with the largest file count and dashboard traffic.
5. Do not relax scan gates before file counts decline.

Success: representative 1h/3h scans open materially fewer files and the dirty-bin queue falls over sustained maintenance cycles.

## Tests

### Unit

- Footer and header warm serve later reads without remote HEAD/GET.
- Full-file cache range hit is zero-copy and makes no object-store call.
- Cold Parquet range uses `get_range`, never `get`.
- A repeated cold-file data range hits Foyer without another inner request.
- A query range never enqueues a full-file warm.
- Confirm timeout leaves metadata cached and warm work queued.
- Multipart capture, queued warm, and range lookup use identical cache keys.

### Integration

Use a counting object store to verify:

- small range requests cannot download full files on cache miss;
- captured flush files are immediately full-cache hits;
- uncaptured optimize outputs become metadata-hot immediately and full-hot after the worker completes;
- restart rebuilds recent warming work.

### Production benchmark

For the representative query and two known tenants, collect cold and warm samples at 1h, 3h, and 3d. Capture `EXPLAIN ANALYZE` and `timefusion_stats` before/after each sample. Record wall time, first-batch latency, file count, Foyer source bytes, hot-leg bytes, and Delta bytes.

Do not run 3-day execution benchmarks until Phase 2 is deployed.

### Production finding: restart warm storm

After exact range admission was deployed, three repeated 1h queries produced
roughly 1,748 range hits and 77 range misses, but total inner-store reads still
grew by about 14.1GB while range bytes grew by only 0.95GB. The bootstrap full-
file warmer accounted for the remaining approximately 13GB and competed with
queries for object-store bandwidth.

Bootstrap therefore rebuilds table state and warms Parquet metadata only. New
flush and optimize outputs retain detached full-body warming. Existing files
populate exact range entries on demand, avoiding a deployment-wide body fetch.

### Production finding: sliding ranges defeat exact keys

After removing the restart body storm, serialized counts measured 7.15s (1h),
9.29s (3h), 14.74s (24h), and 25.29s (3d). A warm 1h `EXPLAIN ANALYZE` improved
to 3.16s, but still opened 83 files for 48 row groups. Although only 2.94MB of
projected Parquet data was scanned, the requested benchmark matrix accumulated
about 14GB of inner-store range reads.

Exact `(path,start,end)` keys are unstable for a sliding `now()-window`
predicate because page-index selection and coalescing move the byte boundaries.
Large Parquet data ranges therefore align to 1MiB edges before fetch/admission.
This permits nearby refreshes to reuse an entry while bounding extra remote
bytes to less than 2MiB per request; tiny files retain whole-file caching.

### Production finding: hot IPC projection happens too late

With aligned range caching deployed, three 1h queries took 13.99s, 12.91s, and
12.08s while inner-store bytes grew by only 18.4MB and range misses by four.
Foyer was no longer the bottleneck. A concurrent `EXPLAIN ANALYZE` took 19.2s:
Delta opened 73 files but reported only ~138ms opening and ~191ms processing,
while the hot leg supplied ~55k rows from 12 Arrow files.

The hot tier previously decoded the complete wide IPC batches from mmap before
projecting to the four columns required by `COUNT(*)`. That faults unused body
and attribute buffers from a 135GB tier and defeats the narrow logical plan.
Projection now runs inside Arrow's `FileDecoder`; unused columns remain cold.

## Rollout

1. Add observability and benchmark harness.
2. Deploy independent metadata/header warming.
3. Deploy range-read fallback and detached full warmer.
4. Verify cache coverage and byte amplification in production.
5. Deploy hot-tier narrow projection.
6. Drain fragmentation backlog and repeat the complete benchmark matrix.
