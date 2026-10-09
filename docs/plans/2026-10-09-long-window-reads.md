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

- [x] **Measured after the routing deploy** (`1c548150`, process more than 20 minutes old, 1 round). No query hits the 90 s timeout any more.

  | Widget | 7d | 14d | 30d |
  | --- | --- | --- | --- |
  | `error_rate` | 12 s (cold first run) | 3.6 s | 4.7 s |
  | `http_by_status` | 4.8 s | 2.7 s | 29.7 s |
  | everything else | — | ≤ 4.2 s | — |

  - 30d `top_resources` (25 s) and `http_by_status` stay raw until `endpoints_1m` backfills days 15–31.
  - The routed floor is about 3 s, of which planning is 0.7–1 s at 7d/14d and 1.6–2.7 s at 30d (`pgwire.slow_statement planned_us`). That is R5.

- [x] **Endpoint analytics routes** (`5c8bbe85`, PR #333, deployed 01:21 10-09):
  - The request chart went to the indexed histogram, which counts at plan time and ran before rollups. Rollups now go first.
  - Requests-by-status had stacked CSE projections, and the matcher peeled only one. It now peels all of them.
  - Prod, shipbubble endpoint `f7d8a198`, all 6 widgets route at every window:

    | Window | Range |
    | --- | --- |
    | 7d | 0.7–2.1 s |
    | 14d | 0.9–2.1 s |
    | 30d | 1.2–2.4 s; requests 4.7 s on the first run |

    By-status was 22 s and an error. The 30d request chart was 6.8 s.
  - The e2e guard sends the six widgets verbatim and asserts the exact answer for each.

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

Status:
- Built: `src/read/bounded_merge.rs`.
- Property test against `SortPreservingMergeExec`, with fetch and ties. A non-strict tie rule fails it with a minimal counterexample.
- e2e `a_latest_n_opens_only_the_file_groups_that_can_contribute`:
  - opens 7 of 19 inputs;
  - goes red with the rule unregistered.
- The output batch is capped at the limit above. Without the cap, the merge filled a session batch (8192 rows) and opened every input before the limit could stop it.

**Shipped** in `f27ff309` (PR #331, deployed 00:09 UTC 10-09). Prod at 15 min uptime, 7d, two runs each:

| Filter | Served before | Served after | Time after |
| --- | --- | --- | --- |
| errors list | 1.7–2.1 GB | 108–239 MB | 4.8–5.8 s (was 9–52 s) |
| route list | — | 54–456 MB | 4.6–15 s |
| `http.status = 500` list | — | 488–624 MB | 19–30 s |

The `http.status = 500` list stays slow because the filter is sparse, so the merge widens across most files. That is R2 territory (index prefilter or bloom).

### R7 — Needle searches (owner decision 10-09: partial prefilter, then a hotcache)

**Prior art:**
- **Quickwit's hotcache:** stores the bytes needed to open an index (under 0.1% of a split) in the split footer, keeps them resident, then reads term-dictionary blocks and postings by byte range. A cold split costs about 3 round trips.
- **ClickHouse and Loki n-gram blooms:** a poor fit. A per-file trigram set saturates at about 1M rows, Loki 3.3 dropped n-gram blooms for build cost, and ClickHouse deprecated `ngrambf` for an inverted text index.

**Our state:**
- A tantivy index is one `tar.zst` blob per parquet file, installed whole before any search. A cold blob fetch averages 1.14 s; a manifest load averages 544 ms with a 49% hit rate.
- Any cold in-window index refused the whole prefilter (`search.rs`), so one cold day of a 30d window full-scanned all 30.

**Steps:**
- [x] **Partial prefilter** (branch `reads/partial-prefilter`):
  - Cold indexes are left out and warmed in the background, and their files are scanned with the predicate.
  - The warm indexes still prune.
  - Counter `tantivy.cold_indexes_left_raw`.
  - Guard `a_cold_index_leaves_only_its_own_files_unpruned`, red with the old refusal.
- [x] **Per-date index manifest** (branch `tantivy/manifest-split`):
  - The manifest is now `index_manifests/{table}/{pid}/shards/{date|undated}.json` plus a `_root.json` listing the shards. Shipbubble has 67 shards of about 0.35 MB each in compact JSON; the single file was 24.5 MB of pretty-printed JSON.
  - A query loads only its window's shards plus `undated`. The cache holds shards, so a 7d query reuses the shards a 30d query loaded.
  - A publish rewrites only the shards whose bytes changed (normally today's), and the root only when the set of shards changes. An emptied shard is deleted after the root drops it.
  - Shard GETs are issued 128 at a time, so a cold load is the root GET plus one round of shard GETs, at most two round trips. The store is round-trip-bound (p50 350–600 ms whatever the size), so a cold load costs about the same as before; the gains are cache reuse across windows and roughly 70x smaller publish writes.
  - Coverage of files outside the window's dates is no longer loaded. That is sound: an uncovered file is scanned raw with the predicate, and date partitions are pruned by the scan anyway.
  - `tantivy.manifest_hits/loads` now count per window load, not per whole manifest, so do not compare them with the 49% baseline.
  - **Migration is one-way.** The first write after deploy splits the legacy `manifest.json` into shards and leaves the legacy file untouched. A rollback to an older image reads that stale `manifest.json`, so entries published since are invisible. Their files are scanned raw until the index backfill re-covers them, and no blobs are lost: orphan reconcile works off live parquet, not the manifest. Entries an old image publishes after a rollback are likewise invisible after the next roll-forward.
  - Guard `a_manifest_publish_rewrites_only_its_date_and_a_window_reads_only_its_dates` (ETags of untouched shards, GET counts per window). It went red with "rewrite every shard" and with "load every shard".
- [x] **Stop indexing `id` twice** (branch `tantivy/id-alias`):
  - A raw-indexed `id` column holds exactly `_id`'s terms. New builds drop the user `id` field, and an `id` predicate resolves to `_id`, which saves about 78 MB of terms per 2M-row file. Old indexes still carry `id` and resolve to it directly.
  - Guard `a_raw_id_column_is_served_by_the_id_field_not_indexed_twice`, red with the duplicate field back.
  - **Not trimmed:** `context___trace_id`, `context___span_id` and `parent_id`. An equality filter on them is rewritten to `text_match`, and tantivy answers it with row-level selections. Blooms only prune whole files, and row-group stats on random IDs prune nothing, so dropping these fields would turn a trace lookup into decoding the whole file.
- [ ] **Range-readable indexes with a hotcache, behind a flag** (owner: start it; estimate 3–4 engineer-weeks plus A/B). Design notes:
  - Do NOT enable tantivy's `quickwit` feature: it switches the term dictionary to SSTable, so every existing index stops opening, and it cannot be A/B'd at runtime. Warm-up is hand-built:
    1. Term lookups are sync against the resident term dictionary.
    2. Postings ranges are fetched in one batched async read.
    3. The search runs sync.
  - Layout: uncompressed bundle with a file table and a hotcache footer. Manifest gains `format` and `hotcache` fields; `SCHEMA_VERSION` must not be bumped (an `==` check would orphan every existing entry).
  - Read side: a strict `BundleDirectory` whose misses are errors, so a missing range never blocks a query; a foyer range store; flag `timefusion_tantivy_range_reads`.
  - Step 1, measured 10-09 on one prod index (shipbubble 09-30, 2.06M rows, 1.38 GB tar.zst, 2.17 GB unpacked):
    - Term dictionaries total 251 MB. Text fields are small: `body` 1.1, `attributes` 0.8, `summary` 0.7 MB, `name` tiny (≈2.7 MB). ID fields dominate: `id` 78 MB **and** `_id` 78 MB (indexed twice), `context___span_id` 36, `context___trace_id` 33, `parent_id` 13.5, `_timestamp` 9.7 MB.
    - Postings total 1.69 GB: `attributes` 656, `summary` 626, `body` 349 MB.
    - Store 94 MB, fast fields 56 MB (`_id` 44 MB), positions 50 MB.
  - Step 1b, measured 10-09 with `hotcache::RecordingDirectory` on the same index (test `measure_reads_of_a_needle_search`, ignored, run on a local copy). Bytes a search actually reads:

    | Phase | Reads | Bytes |
    |---|---|---|
    | open + reader | 28 | ~54 KB (meta 9 KB, store footer 44 KB) |
    | `body`="timeout" | 12 | term 1.14 MB (the whole field FST) + postings 0.54 MB |
    | `attributes`="shipment" | 13 | term 0.84 MB + postings 2.75 MB |
    | `summary`="connection refused" | 20 | term 0.72 MB + postings 7.8 MB |
    | `name`="GET" | 8 | 7 KB + 0.48 MB |
    | `level`="ERROR" | 8 | <11 KB |

    ⇒ A cold needle search needs about 2–9 MB of a 1.38 GB blob (about 0.15–0.6%). Term dictionaries are read whole, so they are the hotcache; postings are a handful of ranges, one batched round trip. The catch: the blob is `tar.zst`, which cannot be range-read. The bundle must store files uncompressed (2.17 GB vs 1.38 GB, +57% storage) or compress per file.
  - Format decision (10-09): prod holds about 3.2 TB of indexes across the top eight projects (shipbubble 466 GB), so an uncompressed bundle (+57%) costs about 1.9 TB. Per-file zstd ratios on the sample: `.idx` 1691→1097 MB, `.term` 251→158, `.store` 94→58, `.fast` incompressible. ⇒ Block-compress the files (zstd, 64–128 KB blocks, offset table). Postings ranges average about 90 KB, so read amplification stays under 2x.
  - Hotcache = the ~54 KB open set (meta, composite file tables, store footer) as one contiguous footer, resident in foyer: 12k indexes for a 30d window ≈ 650 MB. Term dictionaries and postings are NOT resident; they go through the foyer range store like any cached range. Text-field FSTs (~2.7 MB per 2M-row file) times 12k files does not fit in memory.
  - A cold text search is two batched GETs (footer + FSTs, then postings), about 1 s: latency parity with today's 1.14 s blob fetch. The win is bytes moved and resident (the 09-30 OOM was whole-blob installs). Metric: `tantivy.blob_fetch_bytes` (added) against range-read bytes.
  - **Bundle format** (branch `tantivy/bundle`, readers only, nothing writes it yet): `[TFB1][u32 table len][table json][hot bytes][zstd 64 KB blocks…]`. The leading magic lets the existing whole-install path stream-unpack a bundle like a `tar.zst`, so the rollout is: (1) every binary can read bundles, (2) flip the writer, (3) range reads behind the flag. `verify_blob` opens a bundle through `BundleDirectory` and checks its length against the table.
  - Measured on the prod sample (shipbubble 09-30, 2.06M rows): bundle **1326 MB vs tar.zst 1380 MB**, packed in 11.4 s (debug build, zstd-3). Head 49 KB; opening a reader makes **0** reads past it. Hit counts match the unpacked index exactly.

    | Query | Reads | Compressed bytes |
    |---|---|---|
    | `body`="timeout" | 12 | 1.6 MB |
    | `summary`="connection refused" | 20 | 5.8 MB |
    | `attributes`="shipment" | 13 | 2.8 MB |
    | `name`="GET" | 8 | 0.7 MB |
    | `level`="ERROR" | 8 | 0.36 MB |

  - Next: those reads are sequential (8–20 round trips ≈ 3–8 s cold per index). Before any flag flips, add a block cache (repeated term-dictionary reads) and a batched warm-up so a cold search is two round trips.
  - **Range-read path** (branch `tantivy/range-reads`; both settings ON by default per owner 10-09, "I don't like flags", and kept only as kill switches):
    - `timefusion_tantivy_bundle_writes` (config) makes new builds write bundles and record `bundle_head` in the manifest entry.
    - `timefusion_tantivy_range_reads` (config + `FLAG SET`) makes search and the indexed histogram open a bundle with one GET of its head. Reads then go through a shared block cache (`timefusion_tantivy_block_cache_mb`, default 1024) and a store source that fetches each missing run of blocks in one ranged GET.
    - `hotcache::warm` prefetches each queried field's term dictionary, then every term's postings, in two parallel rounds.
    - A ranged bundle is never "cold", and the prefetch cron skips it. An index already installed locally (seeded at publish) is still read from disk.
    - Reads may run on a thread driving a runtime (current-thread runtimes, or code outside `block_in_place`), where `block_on` panics; such a read fetches from a scoped thread of its own.
    - Counters: `tantivy.bundle_opens`, `range_reads`, `range_read_bytes`, against `blob_fetch_bytes`.
  - Per-phase reads on the prod sample, block cache on (3 h window):
    - `body`="timeout": warm 9 reads / 1.4 MB, window count 2 / 6.8 MB (`_timestamp` column), term count 0, hits (595) 3 / 47 MB, mostly the `_id` fast column.
    - Every other query is ≤3 MB after warm-up.
    - ⇒ A cold `body` search moves about 57 MB instead of a 1.38 GB download plus 2.17 GB unpack, in about 4 round trips.
    - Follow-up: hit materialization reads most of `_id`. With valid ordinals, row selections need only `_row_ordinal`.
  - **Rollout:** on by default. New builds write bundles from this deploy on, and old `tar.zst` indexes keep installing whole until they are rebuilt. Rolling back below `dde040e9` cannot install bundles. To verify after deploy: `tantivy.bundle_opens`/`range_read_bytes` grow while `blob_fetch_bytes` flattens, cold 7d/30d needle latency holds, and memory stays flat. Kill switch: `FLAG SET timefusion_tantivy_range_reads OFF`.
  - **Converting old indexes** (branch `tantivy/convert-bundles`). Measured 13:20 10-09 on prod, a 7d `name ilike '%webhook%'` search:
    - 1,679 indexes searched; **1,218 cold `tar.zst` indexes left raw**, with their 379 files scanned with the predicate.
    - 1,154 background warms dropped (queue full), and the next query repeats it. About 42% of a 7d window never prunes.
    - Fix: the 15-minute hot-warm cron now also repacks `tar.zst` entries within `timefusion_cache_recent_days` (35) as bundles, newest first, 2 at a time, for up to 10 minutes per tick.
    - Each conversion stream-unpacks to scratch, repacks, uploads a new generation, and swaps the entry only if it still points at the old blob (`swap_to_bundle`). The old blob retires via the GC grace period.
    - Guard: `converted_tar_zst_indexes_are_searched_by_range_reads`, red with the swap disabled.
    - Expect about 50 GB/h, and shipbubble's 35 days range-readable within hours. Watch the `tantivy_bundles_converted` log, `tantivy.cold_indexes_left_raw` falling, and `bundle_opens` rising.
  - **Regression found 14:10 10-09:**
    - The first conversion tick converted 604 indexes in 10 minutes. Newest first, so the hottest ones, already installed locally by the 3-day prefetch.
    - Their new blob paths no longer matched the local installs, and the prefetch skipped bundles, so searches range-read hundreds of indexes at about 4 round trips each.
    - The 7d needle went from 3.8 s to 49–67 s (global counters were mixed with other traffic, but the mechanism held).
    - Fix (branch `tantivy/keep-hot-installs`): a conversion moves the already-unpacked index into the cache under the new path whenever the old blob was installed, and the prefetch installs bundles again. Range reads serve only what is not installed locally.
    - Guard: the conversion test's hot arm (0 bundle opens, 0 downloads), red without the move.
  - **Demand-driven conversion** (branch `tantivy/convert-on-demand`):
    - The cron converts newest-first across every project at about 650 per tick, so shipbubble's 30d needle (about 9k cold indexes per query) would wait many hours.
    - The cold-index background warm now repacks a searched `tar.zst` as a bundle (under the same 2-slot warm limit), installs it under the new path, and folds the entry into the cached manifest. What users search converts first.
    - Guard: `a_searched_cold_tar_zst_index_converts_in_the_background`, red with conversion disabled in the warm path.
  - Known limitation: tantivy 0.22 reads a field's term dictionary whole, so a trace/span id lookup reads that field's 33–36 MB FST per file. That is 40x less than today's blob, but not cheap. Out of scope for this arc.
  - ⇒ A hotcache of text-field term dictionaries is about 3 MB per 2M-row file and fits a resident budget. The postings are what range reads must avoid fetching whole.
  - ⇒ Side win: ID term dictionaries (about 250 MB per file) duplicate the bloom sidecars, and `id` is indexed twice. Trimming them from new builds shrinks every blob, which speeds today's cold installs too.
  - Also measured: shipbubble's index manifest is ONE 24.5 MB JSON with 22,688 entries. Manifest loads average 544 ms at a 49% hit rate, a planning cost worth splitting per date (done above).
  - Store each index as range-readable files with a hotcache footer, and implement a tantivy `Directory` over object-store ranges, as `quickwit-directories` does.
  - Keep the footers resident under a byte budget.
  - A/B the change within one process before turning it on.

### R5 — Planning cost of routed widgets

Measured 10-09 00:25 with the per-phase counters, single 30d widgets, second run:

| Phase | Time |
| --- | --- |
| Routing decision | 50–75 ms |
| Output coverage (inside the routing decision) | 40–60 ms |
| Source stats (inside the routing decision) | 4–8 ms |
| Rewrite planning | **870–920 ms** (2.0–2.6 s on first runs) |
| Whole statement | 1.2 s |

- Rewrite planning is about 75% of a routed widget.
- Each hybrid rewrite is one tier leg plus one raw source scan per uncovered fringe. A plain raw scan plans in about 0.25–0.3 s.
- Measured with `adef5b59`:
  - Logical rewrite planning is about 15 ms. The rest is physical: the legs' table scans.
  - Each `table_scan` costs about 150–220 ms, and a routed 30d widget plans about 3 of them.
  - `provider_scan_us_avg` is about 70 ms and `mem_plan_us_avg` about 15 ms, which leaves 70–110 ms per scan unattributed.
- `7550ecb3` adds the `scan_certification`, `scan_tantivy`, `scan_provider` and `scan_mem_leg` sub-phases.
- Leading hypothesis (from the code map): per-date certification walks over the whole unified snapshot. Fix candidate: one pass per snapshot version, grouped by (project, date), shared by every scan in a statement.


- `timefusion_stats` component `planning` adds per-phase totals: route, match, source stats, output coverage, and rewrite planning.
- Read before and after a bench to get each phase's average cost per statement, then fix the largest.
- Suspects:
  - `partition_stats_bounded` scans every add action of the unified source table, per candidate tier, per statement.
  - `rollup_output_coverage` scans the tier's snapshot.

**Update 10-09 15:00 (endpoint widget, 7d, warm, about 1 s total):**

| Phase | Time |
| --- | --- |
| `scan_provider` | 4 calls × ~95 ms ≈ 0.4 s, the largest bucket |
| `rollup_rewrite_plan` | ~0.35 s (contains the leg scans) |
| `scan_certification` | 40–70 ms |
| Mem leg | 50–370 ms |

- The provider cost is the fork's `DeltaScan::scan`: `scan_metadata_seeded` (`kernel/snapshot/scan.rs`) → `scan_metadata_from`. It re-runs kernel data skipping and the scan-row transform over every materialized file of the snapshot on every scan. Cost scales with the table's total file count, not the query's window.
- Fix candidates:
  - Memoize the replayed `ScanMetadata` per (snapshot version, predicate) inside the fork.
  - Or seed the replay with only the project partition's files.
- Both are fork changes; measure `scan_provider` per call before and after.

### R2 — Filtered charts and needles over 7–30 days

**Diagnosed 10-09 02:00.** The shipbubble 7d chart grouped by `status_code` with an `http.status = 500` filter took 21–90 s. Grouped by bucket only, it took 0.3–1.3 s.

| Measurement | Result |
| --- | --- |
| Sealed days only, status-grouped | No `DedupExec`; certified days skip it |
| One sealed day | Filter pushed into parquet, 3.38M rows pruned, 65 MB, 0.8 s |
| Window including today | Whole window goes through one `DedupExec` |

- Count-only charts are served by the logical-count index without dedup, which is why grouping by bucket alone was fast.
- Cause: the per-date split (certified dates skip dedup) existed only on the Delta-only path. Any window reaching into the MemBuffer deduped every sealed day.
- Fix (branch `reads/per-date-split`):
  - Extend the split to the mem ∪ delta path.
  - A certified date skips only if every buffered row is later than the date's end. The floor is the minimum of the bucket metadata (key start, routing min, row min) and the leg's own rows, because a late merge-on-read version keeps its old timestamp.
  - Guard: `per_date_skip_with_a_mem_leg_never_skips_a_buffered_version`. It goes red both ways: an unsafe skip returns the superseded version, and with the split disabled the dedup reads the sealed rows.

**Shipped** in `82af1ddd` (PR #335, deployed 02:37 10-09). On prod, `dedup_skipped_per_date` reached 134 within 30 minutes. 7d status-grouped charts with filters, `http.status = 500` and route:

| State | Before | After |
| --- | --- | --- |
| Warm | 21–90 s | 0.8 s |
| Cold | — | 26–38 s |

The cold cost is round trips, below.

**Cold raw scans are bound by round trips (measured 03:00 10-09).**
- Object-store GET latency below the cache (`timefusion.store.request_ms`, op `get_range`, 6 h): p50 350–600 ms, p95 0.8–1.25 s, p99 1.1–1.7 s.
- In-flight `get_range` averaged only 1–8, so the pipe is not saturated: latency is per request.
- The 7d cold chart made 2,174 GETs and read 3.6 GB from the store; warm, the same query is served 969 MB from cache.
  - 1 MB range alignment therefore amplifies about 3.6× (not 40×; `bytes_served` counts hits only).
  - The real cost is about 90 sequential GETs per stream.
- Cause: rows are written in row groups of about 36k rows (the 128 MB decoded cap on wide rows), so a file has about 17 row groups. The parquet reader fetches each row group's column chunks separately, one round after another.
- Cache churn is low. A quiet 6-minute window read 2.6 GB from the store but admitted only 0.17 GB, with 118 evictions. Warm state survives, and restarts keep the disk cache (158 blocks recovered).

**Tried and reverted 10-09:**
- Built: fork `f0795e64` (also `02a1115d` on `timefusion-upgrade-55-dv`), deployed as `4c96c0c4` at 03:39, reverted as `d6c20f00`.
- Fork test: identical rows, at most 5 round trips against 20 on a 20-row-group file.
- Prod, cold single recent days, filter on a column not read before:

  | Build | Time | GETs | Fetched |
  | --- | --- | --- | --- |
  | Before | 2.3–2.6 s | 38–42 | 88 MB |
  | After | 3.4–4.1 s | 326–392 | 490–540 MB |

- **Correction (04:15):** the same probe after the revert, on fresh cold day 10-02, also read 317 GETs / 491 MB in 7.1 s. The comparison was confounded: global `foyer.*` counters on a young process include post-restart background reads, and each arm used different days. **The read-ahead's effect is unknown, not shown to be harmful.**
- The reasoning below still holds as a risk, but it was not demonstrated: the reader wrapper cannot see the access plan, so it can fetch chunks of row groups that statistics, page index or bloom pruning would skip.
- A correct version needs the opener's `ParquetAccessPlan`: read ahead only row groups the plan will read, ideally only the page ranges it selects.
- Also: put any such change behind a `FLAG SET` runtime switch, and A/B it within one process on the same cold-day population. Measure per-query bytes, not global counters.

**Re-shipped dark for an on-prod A/B:**
- Fork `e87db4dd` adds a `set_read_ahead` switch, off by default; integration branch `1ecfc1cb`.
- TF `RuntimeFlag::TimefusionParquetReadAhead` (config `timefusion_parquet_read_ahead`, default off) is applied at boot and on `FLAG SET`.
- A/B protocol: alternate `FLAG SET timefusion_parquet_read_ahead ON/OFF` within one process. Each arm runs on fresh cold (date, column) pairs from the same population. Compare time and `parquet.bytes_read` deltas.

**A/B result (10-09 05:15–05:30, process 36 min old):**
- Method: 24 day×method pairs on shipbubble days 09-26..10-01, alternating `FLAG SET timefusion_parquet_read_ahead` ON/OFF.
- Cold pairs (DELETE, PUT, OPTIONS; HEAD pairs were already cached and excluded):

  | Arm | Pairs | Average | Range | Parquet `bytes_read` per query |
  | --- | --- | --- | --- | --- |
  | ON | 9 | 3.4 s | 1.9–5.6 s | higher |
  | OFF | 9 | 2.9 s | 2.2–3.7 s | lower |

- **No benefit; the flag stays OFF.** Under filter pushdown the reader's round trips are mostly sparse page reads of projected columns, which read-ahead does not cover, while read-ahead adds whole-chunk bytes for row groups pruning would skip.
- A real fix needs the opener's access plan, or larger row groups for sealed rewrites.
- Side finding: `FLAG` fails over the extended protocol, i.e. a prepared statement (psycopg auto-prepares after 5 repeats). Only the simple protocol intercepts it.

**Original proposal — column-chunk read-ahead.**
- Design: in the fork's `InstrumentedParquetFileReader::get_byte_ranges`, map the requested ranges to (row group, column) chunks using the file's metadata.
- Add the same columns for the next row groups to the SAME batched `get_byte_ranges` call, under a per-reader byte budget of about 16 MB, and serve later requests that fall inside a buffered chunk.
- No spawned tasks, so the scan's cache-bypass scope and permits still apply.
- Expected: about 90 sequential rounds per stream become about 20, i.e. cold 7d charts of about 30 s fall to 5–8 s.
- Process: fork commit on the branch carrying pin `ec8319c` (`tf-read-skipping-predicate`), land it on the integration branch `timefusion-upgrade-55-dv` too, pin bump, full signoff.
- Alternative without a fork change: larger row groups for sealed rewrites only. This trades against the decode-memory bound the 128 MB cap exists for.

Still open for R2: the `http.status = 500` *list* (sparse, no index) and needle text search at 30d (cold tantivy indexes are skipped by design, so it is a full scan).

- Filter on dimension columns (status, service, kind, level): route through the tiers. Verify that each one routes.
- Filter on non-dimension columns (route, `http.status`, text): this is a raw scan.
  - Measure cold and warm cost per day, and whether the cost is I/O misses or decode.
  - Candidates: the tantivy prefilter for equality on indexed columns, bloom sidecars, and wider cache retention for the active tenants' last 30 days.

### R3 — Unfiltered log explorer at 30d

- Percentiles at 30d were 23 s in one sample and 3.7 s in another. Classify each run as cold or warm and routed or raw, then fix what is named.

### R6 — WAL segments pinned by idle writers (follow-up to `4fee4fd7`)

- GC now keeps any segment that holds an unsealed writer block. An idle topic can therefore pin a 1 GB segment until it writes 10 MB.
- The bound is about one segment per writer: 30 topics × 4 shards, about 124 GB, against 793 GB free.
- Watch `wal.files` and `wal.disk_mb`. Fix properly by having GC seal and roll a writer whose segment aged out.

### Scorecard 10-09 02:00 (build `7550ecb3`, shipbubble, 2 rounds, warm)

| Window | p50 | Slowest widget |
| --- | --- | --- |
| 7d | 0.50 s | 2.4 s |
| 14d | 0.54 s | 1.6 s |
| 30d | 0.65 s | 2.2 s |

- These are all 15 overview and log-explorer shapes. The 10-08 baseline had 90 s timeouts on `error_rate` and `http_by_status`, and 67 s for `top_resources`.
- Endpoint analytics: all 6 widgets 0.7–2.4 s at 7d, 14d and 30d.

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
